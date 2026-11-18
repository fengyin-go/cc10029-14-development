"""归档批次接口：分批生成归档批次数据集、校验归档文件、补做失败分区、落地与清理。

既有 /api/archive 的归档策略接口保持不变；归档批次相关能力挂在 /api/archive/batches 下，
同时以 /api/archive-batch 作为别名，方便调用方按数据集维度直接接入。
"""
from __future__ import annotations

import csv
import io
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ArchiveBatchBulkPayload, ArchiveBatchPayload
from app.services.archive_batch import ArchiveBatchService
from app.services.storage import StorageService

service = ArchiveBatchService()
storage_service = StorageService()

BATCH_FIELDS = [
    "批次编号", "策略编号", "归档周期", "归档格式", "保留年限", "数据起始", "数据截止",
    "分区数", "记录数", "批次状态", "校验状态", "校验摘要", "写入状态", "清理状态",
    "落地状态", "回执状态", "生成时间",
]

routers = [
    APIRouter(prefix="/api/archive/batches", tags=["历史归档"]),
    APIRouter(prefix="/api/archive-batch", tags=["历史归档"]),
]


def register(router: APIRouter) -> None:
    @router.get("/summary")
    def get_summary() -> dict[str, Any]:
        """归档批次指标卡：总数、待校验、部分失败与可清理数量。"""
        return {"metrics": [{"label": label, "value": value} for label, value in service.summary().items()]}

    @router.get("")
    def list_batches(
        keyword: str | None = Query(default=None, description="按批次编号或策略编号检索"),
        status: str | None = Query(default=None, description="待校验、部分失败、已校验、已落地、已清理"),
        cleanup_status: str | None = Query(default=None, description="不可清理、保留期内、可清理、已清理"),
        strategy: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        size: int = Query(default=20, ge=1, le=200),
    ) -> dict[str, Any]:
        """列出归档批次数据集，按新到旧排序。"""
        items, total = service.list_batches(
            keyword=keyword, status=status, cleanup_status=cleanup_status,
            strategy=strategy, page=page, size=size)
        return {"items": items, "total": total, "page": page, "size": size}

    @router.get("/export")
    def export_batches(
        status: str | None = Query(default=None),
        cleanup_status: str | None = Query(default=None),
        strategy: str | None = Query(default=None),
    ) -> StreamingResponse:
        """导出当前筛选条件下的归档批次清单。"""
        items, _ = service.list_batches(
            status=status, cleanup_status=cleanup_status, strategy=strategy, page=1, size=1_000_000)
        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["id", *BATCH_FIELDS])
        for item in items:
            row = []
            for value in [item.get("id"), *(item.get(field, "") for field in BATCH_FIELDS)]:
                text = "" if value is None else str(value)
                row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
            writer.writerow(row)
        return StreamingResponse(
            iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="archive-batches.csv"'})

    @router.get("/recoverable")
    def get_recoverable() -> dict[str, Any]:
        """列出完成顺序冲突下仍可恢复的完整归档批次，并标记被作废的冲突副本。"""
        recoverable, conflicts = service.recoverable_view()
        return {
            "items": recoverable,
            "conflicts": conflicts,
            "total": len(recoverable),
            "message": "完成顺序冲突已处理：同一数据集仅保留一份可恢复的完整归档批次",
        }

    @router.post("/conflicts/resolve")
    def resolve_conflicts() -> dict[str, Any]:
        """把完成顺序冲突的副本标记为冲突归档并作废，确保只保留一份可恢复批次。"""
        recoverable, conflicts = service.resolve_conflicts()
        return {
            "ok": True,
            "message": f"已处理完成顺序冲突：保留 {len(recoverable)} 份可恢复批次，作废 {len(conflicts)} 份冲突副本",
            "items": recoverable,
            "conflicts": conflicts,
        }

    @router.post("/{batch_id}/conflict-copy")
    def create_conflict_copy(batch_id: int) -> dict[str, Any]:
        """复现完成顺序冲突：为同一数据集再生成一份已落地、待回执的副本。"""
        clone, message = service.simulate_completion_conflict(batch_id)
        return {"ok": clone is not None, "message": message, "entry": clone}

    @router.get("/{batch_id}/expired-scope")
    def get_expired_scope(batch_id: int) -> dict[str, Any]:
        """校验批次数据集是否落在保留年限之外，并列出可清理分区。"""
        batch = service._find(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"归档批次 {batch_id} 不存在")
        scope = service.cleanup_scope(batch)
        scope["已写入"] = bool(batch.get("校验摘要")) and batch.get("写入状态") == "已写入"
        scope["清理状态"] = batch.get("清理状态")
        return scope

    @router.get("/{batch_id}")
    def get_batch(batch_id: int) -> dict[str, Any]:
        """读取批次明细，含分区列表、落地清单和可清理范围。"""
        detail = service.get_detail(batch_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"归档批次 {batch_id} 不存在")
        return detail

    @router.post("")
    def create_batch(payload: ArchiveBatchPayload) -> dict[str, Any]:
        """按归档策略生成一批归档批次数据集。"""
        batch, message, duplicated = service.generate(
            payload.strategy,
            data_end=payload.data_end,
            partition_count=payload.partition_count,
            fail_count=payload.fail_count,
            fail_partitions=payload.fail_partitions,
            submit_key=payload.submit_key,
        )
        if batch is None:
            return {"ok": False, "message": message, "duplicated": False, "entry": None}
        return {"ok": True, "message": message, "duplicated": duplicated, "entry": batch}

    @router.post("/bulk")
    def bulk_action(
        payload: ArchiveBatchBulkPayload,
        simulate_commit_failure: bool | None = Query(default=None, description="复现整组事务提交失败"),
    ) -> dict[str, Any]:
        """对归档批次数据集执行批量动作；整组提交，逐条返回成功或失败。"""
        ok, message, results, rolled_back = service.bulk(
            [int(item) for item in payload.ids],
            payload.action.strip(),
            checksum=payload.checksum,
            receipt_no=payload.receipt_no,
            simulate_commit_failure=bool(
                simulate_commit_failure if simulate_commit_failure is not None
                else payload.simulate_commit_failure),
        )
        return {
            "ok": ok,
            "message": message,
            "results": results,
            "rolled_back": rolled_back,
        }

    @router.post("/{batch_id}/actions")
    def run_batch_action(batch_id: int, payload: ActionPayload) -> dict[str, Any]:
        """对单个归档批次数据集执行动作，返回与批量操作一致的逐条结果。"""
        ok, message, results, rolled_back = service.bulk(
            [batch_id], payload.action.strip())
        item = results[0] if results else {"id": batch_id, "ok": False, "message": message}
        entry = service.get_detail(batch_id) if item.get("ok") else None
        return {
            "ok": bool(ok and item.get("ok")),
            "message": str(item.get("message") or message),
            "entry": entry,
            "result": item,
            "rolled_back": rolled_back,
        }

    @router.get("/{batch_id}/landing")
    def get_landing(batch_id: int) -> dict[str, Any]:
        """查询归档落地清单，落地路径与校验摘要必须与批次一一对应。"""
        batch = service._find(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"归档批次 {batch_id} 不存在")
        landing = storage_service.find_landing(str(batch.get("批次编号")))
        if landing is None:
            raise HTTPException(status_code=404, detail=f"归档批次 {batch_id} 尚未生成归档落地清单")
        return landing


for _router in routers:
    register(_router)

# 与既有模块保持同一结构：对外暴露一个 router 属性；含 /api/archive/batches 与别名两个前缀。
ROUTERS = [SimpleNamespace(router=_router) for _router in routers]
