"""存储落地业务规则：状态流转、字段校验与筛选口径都收在这里。

除了维护存储配置，本模块还承接历史归档批次的下游消费：归档批次数据集完成校验后，
由这里的 ``land_archive_batch`` 生成归档落地清单，落地路径与校验摘要同批次一一对应。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "storage"
ARCHIVE_LANDING_MODULE = "archive_landing"
LIST_FIELDS = ["配置编号", "存储类型", "连接信息", "分区策略", "压缩格式", "保存天数", "备份副本", "清理策略"]
REQUIRED_FIELDS = ["配置编号", "存储类型", "连接信息"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用配置": "已启用", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


class StorageService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("配置编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"存储配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于存储落地可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"存储配置已{action}"

    # ------------------------------------------------------------------ 归档落地
    def landing_rows(self) -> list[dict[str, Any]]:
        return store.rows(ARCHIVE_LANDING_MODULE)

    def find_landing(self, batch_no: str) -> dict[str, Any] | None:
        for row in self.landing_rows():
            if row.get("批次编号") == batch_no:
                return row
        return None

    def land_archive_batch(self, batch: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        """消费归档批次数据集生成归档落地清单；同一批次只保留一条落地记录。"""
        batch_no = str(batch.get("批次编号") or "")
        if not batch_no:
            return None, "归档批次编号为空，无法落地", False
        checksum = str(batch.get("校验摘要") or "").strip()
        if not checksum:
            return None, "归档批次缺少校验摘要，不能生成归档落地清单", False

        location = f"/warehouse/archive/{batch.get('策略编号')}/{batch_no}"
        existing = self.find_landing(batch_no)
        if existing is not None:
            existing["落地路径"] = location
            existing["校验摘要"] = checksum
            return existing, f"归档落地清单 {existing.get('落地编号')} 已存在，保持一一对应", True

        rows = self.landing_rows()
        entry = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "落地编号": f"LAND-{max((int(row.get('id', 0)) for row in rows), default=0) + 1:04d}",
            "批次编号": batch_no,
            "策略编号": batch.get("策略编号"),
            "归档格式": batch.get("归档格式"),
            "落地路径": location,
            "校验摘要": checksum,
            "分区数": batch.get("分区数", 0),
            "记录数": batch.get("记录数", 0),
            "落地状态": "已落地",
            "回执状态": "待回执",
            "落地时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        rows.append(entry)
        return entry, f"归档落地清单 {entry['落地编号']} 已生成", False

