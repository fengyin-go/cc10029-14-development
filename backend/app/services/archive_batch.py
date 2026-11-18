"""历史归档批次业务规则：分批生成、归档文件校验、失败分区补做、落地与清理。

一条归档策略可以反复生成归档批次（``归档批次数据集``）。每次生成按归档周期切分
分区、按归档格式决定文件后缀、按保留年限计算可清理截止日期，三者共同决定这一
批次的可清理范围。批次只有在校验摘要写入之后才算完成归档，也才可能进入可清理范围。
"""
from __future__ import annotations

import copy
import hashlib
import hmac
from datetime import date, datetime, timedelta
from typing import Any

from app.store import store
from app.services.storage import StorageService

BATCH_MODULE = "archive_batch"
PARTITION_MODULE = "archive_partition"
LANDING_MODULE = "archive_landing"
STRATEGY_MODULE = "archive"

# 批次状态
PENDING = "待校验"
PARTIAL_FAILED = "部分失败"
VERIFIED = "已校验"
LANDED = "已落地"
CLEANED = "已清理"
CONFLICT = "冲突归档"

# 分区状态
WAIT_ARCHIVE = "待归档"
ARCHIVED = "已归档"
ARCHIVE_FAILED = "归档失败"

# 校验 / 写入状态
UNVERIFIED = "未校验"
VERIFY_PASSED = "校验通过"
VERIFY_FAILED = "校验失败"
UNWRITTEN = "未写入"
WRITTEN = "已写入"

# 清理状态
NOT_CLEANABLE = "不可清理"
IN_RETENTION = "保留期内"
CLEANABLE = "可清理"
CLEANED_STATUS = "已清理"
VOID = "作废"

# 落地 / 回执状态
NOT_LANDED = "未落地"
LANDED_STATUS = "已落地"
WAIT_RECEIPT = "待回执"
RECEIVED = "已回执"

CYCLE_DAYS = {"按日": 1, "按周": 7, "按月": 30, "按季度": 90, "按年": 365}
CYCLE_PARTITIONS = {"按日": 4, "按周": 3, "按月": 3, "按季度": 4, "按年": 4}
FORMAT_SUFFIX = {"parquet": ".parquet", "jsonl.gz": ".jsonl.gz", "csv.gz": ".csv.gz", "orc": ".orc", "avro": ".avro"}

BULK_ACTIONS = {
    "校验归档文件": "verify",
    "补做失败分区": "retry",
    "落地存储": "landing",
    "进入可清理": "mark_cleanable",
    "清理回执": "receipt",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _first_int(value: Any, default: int) -> int:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return int(digits) if digits else default


def _normalize_cycle(value: Any) -> str:
    text = str(value or "")
    for keyword, cycle in (("季", "按季度"), ("周", "按周"), ("月", "按月"), ("年", "按年"), ("日", "按日")):
        if keyword in text:
            return cycle
    return "按日"


def _normalize_format(value: Any) -> str:
    text = str(value or "").lower()
    if "json" in text:
        return "jsonl.gz"
    if "csv" in text:
        return "csv.gz"
    if "orc" in text:
        return "orc"
    if "avro" in text:
        return "avro"
    return "parquet"


def _parse_date(value: Any, default: date) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return default


def _date_range(text: str) -> tuple[date | None, date | None]:
    parts = str(text or "").split("~")
    if len(parts) != 2:
        return None, None
    try:
        return date.fromisoformat(parts[0].strip()), date.fromisoformat(parts[1].strip())
    except ValueError:
        return None, None


class ArchiveBatchService:
    def __init__(self) -> None:
        self.storage = StorageService()
        self._snapshots: dict[str, list[dict[str, Any]]] = {}
        self._seed_demo_batches()

    # ------------------------------------------------------------------ 基础工具
    def _rows(self) -> list[dict[str, Any]]:
        return store.rows(BATCH_MODULE)

    def _partitions(self) -> list[dict[str, Any]]:
        return store.rows(PARTITION_MODULE)

    def _find(self, batch_id: int) -> dict[str, Any] | None:
        return store.find(BATCH_MODULE, batch_id)

    def _next_id(self, rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def _next_batch_no(self) -> str:
        numbers = []
        for row in self._rows():
            text = str(row.get("批次编号") or "")
            if text.startswith("BATCH-") and text[6:].isdigit():
                numbers.append(int(text[6:]))
        return f"BATCH-{max(numbers, default=0) + 1:04d}"

    @staticmethod
    def _digest(seed: str) -> str:
        return "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def batch_digest(self, batch_no: str) -> str:
        """批次校验摘要由各分区校验摘要汇总，缺少任一摘要就不完整。"""
        checksums = [str(p.get("校验摘要") or "") for p in self.partitions_of(batch_no)]
        checksums.sort()
        return self._digest(batch_no + "|" + "|".join(checksums))

    def partitions_of(self, batch_no: str) -> list[dict[str, Any]]:
        return [row for row in self._partitions() if row.get("批次编号") == batch_no]

    def _all_archived(self, batch: dict[str, Any]) -> bool:
        parts = self.partitions_of(str(batch.get("批次编号") or ""))
        return bool(parts) and all(p.get("归档状态") == ARCHIVED and p.get("校验摘要") for p in parts)

    def _find_strategy(self, strategy_ref: Any) -> dict[str, Any] | None:
        rows = store.rows(STRATEGY_MODULE)
        text = str(strategy_ref or "").strip()
        if not text:
            return None
        for row in rows:
            if str(row.get("id")) == text:
                return row
        for row in rows:
            if str(row.get("策略编号")) == text:
                return row
        return None

    # ------------------------------------------------------------------ 可清理范围
    def cleanup_scope(self, batch: dict[str, Any]) -> dict[str, Any]:
        """归档周期、归档格式和保留年限共同决定可清理范围。"""
        retention = max(1, _first_int(batch.get("保留年限"), 1))
        cutoff = date.today() - timedelta(days=365 * retention)
        parts = self.partitions_of(str(batch.get("批次编号") or ""))
        cleanable = []
        for part in parts:
            start, end = _date_range(str(part.get("分区范围") or ""))
            if end is not None and end < cutoff:
                cleanable.append(str(part.get("分区编号")))
        start_text = str(batch.get("数据起始") or "")
        return {
            "归档周期": batch.get("归档周期"),
            "归档格式": batch.get("归档格式"),
            "保留年限": retention,
            "保留截止日期": cutoff.isoformat(),
            "可清理范围": f"{start_text} ~ {cutoff.isoformat()}" if cleanable else "无（全部处于保留期）",
            "可清理分区数": len(cleanable),
            "保留分区数": len(parts) - len(cleanable),
            "可清理分区": cleanable,
            "文件后缀": FORMAT_SUFFIX.get(str(batch.get("归档格式")), ".parquet"),
        }

    def _refresh_cleanup_status(self, batch: dict[str, Any]) -> None:
        if batch.get("批次状态") in (CONFLICT, CLEANED) or batch.get("清理状态") == CLEANED_STATUS:
            return
        if not batch.get("校验摘要") or batch.get("写入状态") != WRITTEN:
            batch["清理状态"] = NOT_CLEANABLE
            return
        scope = self.cleanup_scope(batch)
        batch["清理状态"] = CLEANABLE if scope["可清理分区数"] > 0 else IN_RETENTION

    # ------------------------------------------------------------------ 生成
    def generate(
        self,
        strategy_ref: Any,
        *,
        data_end: Any = None,
        partition_count: int | None = None,
        fail_count: int = 0,
        fail_partitions: list[int] | None = None,
        submit_key: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, bool]:
        strategy = self._find_strategy(strategy_ref)
        if strategy is None:
            return None, f"归档策略 {strategy_ref} 不存在或已停用", False

        strategy_no = str(strategy.get("策略编号") or f"ARCH-{strategy.get('id')}")
        cycle = _normalize_cycle(strategy.get("归档周期"))
        fmt = _normalize_format(strategy.get("归档格式"))
        retention = max(1, _first_int(strategy.get("保留年限"), 1))
        end = _parse_date(data_end, date.today() - timedelta(days=1))
        start = end - timedelta(days=CYCLE_DAYS.get(cycle, 1) - 1)
        key = str(submit_key or "").strip() or f"{strategy_no}:{cycle}:{end.isoformat()}"

        # 同一批次数据集重复送交时沿用既有编号，不生成第二条记录。
        for row in self._rows():
            if row.get("批次状态") == CONFLICT or row.get("清理状态") == VOID:
                continue
            same_dataset = (
                row.get("策略编号") == strategy_no
                and row.get("归档周期") == cycle
                and row.get("数据截止") == end.isoformat()
            )
            if row.get("提交键") == key or same_dataset:
                return row, f"重复送交的归档批次数据集，沿用批次编号 {row.get('批次编号')}", True

        count = int(partition_count or CYCLE_PARTITIONS.get(cycle, 4))
        count = max(1, min(12, count))
        batch_no = self._next_batch_no()
        suffix = FORMAT_SUFFIX.get(fmt, ".parquet")
        failed_indexes = {int(i) for i in (fail_partitions or []) if str(i).isdigit()}
        failed_indexes |= {count - i for i in range(max(0, int(fail_count))) if count - i >= 1}

        rows = self._partitions()
        next_partition_id = self._next_id(rows)
        partitions: list[dict[str, Any]] = []
        span = (end - start).days + 1
        for index in range(count):
            part_start = start + timedelta(days=span * index // count)
            part_end = start + timedelta(days=max(0, span * (index + 1) // count - 1))
            code = f"{batch_no}-P{index + 1:02d}"
            record_count = 1200 + (index * 137) % 900
            expected = self._digest(f"{batch_no}|{code}|{part_start}|{part_end}|{record_count}")
            failed = (index + 1) in failed_indexes
            partition = {
                "id": next_partition_id + index,
                "批次编号": batch_no,
                "分区编号": code,
                "分区范围": f"{part_start.isoformat()}~{part_end.isoformat()}",
                "记录数": record_count,
                "归档路径": f"/archive/{strategy_no}/{batch_no}/{code}{suffix}",
                "预期校验摘要": expected,
                "校验摘要": "" if failed else expected,
                "归档状态": ARCHIVE_FAILED if failed else ARCHIVED,
                "归档次数": 0 if failed else 1,
                "归档时间": "" if failed else _now(),
                "失败原因": "归档文件写入失败，等待补做" if failed else "",
            }
            partitions.append(partition)

        ends = [end for _ in partitions]
        total_records = sum(int(p["记录数"]) for p in partitions)
        batch = {
            "id": self._next_id(self._rows()),
            "批次编号": batch_no,
            "策略编号": strategy_no,
            "归档周期": cycle,
            "归档格式": fmt,
            "保留年限": retention,
            "数据起始": start.isoformat(),
            "数据截止": ends[0].isoformat(),
            "分区数": count,
            "记录数": total_records,
            "批次状态": PARTIAL_FAILED if failed_indexes else PENDING,
            "校验状态": UNVERIFIED,
            "校验摘要": "",
            "写入状态": UNWRITTEN,
            "清理状态": NOT_CLEANABLE,
            "落地状态": NOT_LANDED,
            "回执状态": "未回执",
            "提交键": key,
            "版本号": 1,
            "生成时间": _now(),
            "上一版本": "",
            "备注": "部分分区归档失败，只能补做失败分区" if failed_indexes else "",
        }
        rows.extend(partitions)
        self._rows().append(batch)
        message = (
            f"归档批次 {batch_no} 已生成，其中 {len(failed_indexes)} 个分区归档失败，仅可补做失败分区"
            if failed_indexes else f"归档批次 {batch_no} 已生成，等待归档文件校验"
        )
        return batch, message, False

    # ------------------------------------------------------------------ 校验
    def verify(self, batch_id: int, checksum: str | None = None) -> tuple[dict[str, Any] | None, str]:
        batch = self._find(batch_id)
        if batch is None:
            return None, f"归档批次 {batch_id} 不存在"
        if batch.get("批次状态") == CONFLICT:
            return None, "冲突归档批次不可恢复，不能写入归档批次数据集"
        batch_no = str(batch.get("批次编号"))
        if not self._all_archived(batch):
            return None, "仍有分区未完成归档，请先补做失败分区再校验归档文件"

        expected = self.batch_digest(batch_no)
        provided = str(checksum or "").strip()
        if provided and not hmac.compare_digest(provided, expected):
            batch["校验状态"] = VERIFY_FAILED
            batch["清理状态"] = NOT_CLEANABLE
            batch["版本号"] = int(batch.get("版本号", 1)) + 1
            return None, "校验摘要与归档文件不一致，归档批次数据集未写入"

        batch["校验摘要"] = expected
        batch["校验状态"] = VERIFY_PASSED
        batch["写入状态"] = WRITTEN
        batch["批次状态"] = VERIFIED
        batch["备注"] = "归档文件校验通过，归档批次数据集已写入"
        batch["版本号"] = int(batch.get("版本号", 1)) + 1
        self._refresh_cleanup_status(batch)
        scope = self.cleanup_scope(batch)
        message = f"归档文件校验通过，批次 {batch_no} 已写入"
        if batch["清理状态"] == CLEANABLE:
            message += f"，可清理 {scope['可清理分区数']} 个分区"
        else:
            message += "，数据仍在保留年限内，暂不可清理"
        return batch, message

    # ------------------------------------------------------------------ 补做失败分区
    def retry(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        batch = self._find(batch_id)
        if batch is None:
            return None, f"归档批次 {batch_id} 不存在"
        if batch.get("批次状态") in (CLEANED, CONFLICT):
            return None, f"归档批次 {batch.get('批次编号')} 已{CLEANED if batch.get('批次状态') == CLEANED else '冲突归档'}，不能补做"
        failed = [p for p in self.partitions_of(str(batch.get("批次编号"))) if p.get("归档状态") != ARCHIVED]
        if not failed:
            return batch, "没有失败分区，无需补做"
        for part in failed:
            part["校验摘要"] = part.get("预期校验摘要") or self._digest(
                f"{part.get('批次编号')}|{part.get('分区编号')}|{part.get('分区范围')}")
            part["归档状态"] = ARCHIVED
            part["归档时间"] = _now()
            part["归档次数"] = int(part.get("归档次数", 0)) + 1
            part["失败原因"] = ""

        batch["批次状态"] = PENDING
        batch["校验状态"] = UNVERIFIED
        batch["校验摘要"] = ""
        batch["写入状态"] = UNWRITTEN
        batch["清理状态"] = NOT_CLEANABLE
        batch["备注"] = f"已补做 {len(failed)} 个失败分区，成功分区未重复归档，需重新校验"
        batch["版本号"] = int(batch.get("版本号", 1)) + 1
        return batch, f"已补做 {len(failed)} 个失败分区，成功分区未重复归档"

    # ------------------------------------------------------------------ 落地
    def landing(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        batch = self._find(batch_id)
        if batch is None:
            return None, f"归档批次 {batch_id} 不存在"
        if batch.get("批次状态") == CONFLICT:
            return None, f"归档批次 {batch.get('批次编号')} 处于冲突归档状态，不能落地"
        if not batch.get("校验摘要") or batch.get("写入状态") != WRITTEN:
            return None, "归档批次尚未校验通过，缺少校验摘要，不能生成归档落地清单"

        record, message, duplicated = self.storage.land_archive_batch(batch)
        if record is None:
            return None, message
        if batch.get("落地状态") != LANDED_STATUS:
            batch["落地状态"] = LANDED_STATUS
            batch["回执状态"] = WAIT_RECEIPT
            if batch.get("批次状态") not in (CLEANED,):
                batch["批次状态"] = LANDED
            batch["备注"] = f"已由存储落地生成归档落地清单 {record.get('落地编号')}"
            batch["版本号"] = int(batch.get("版本号", 1)) + 1
        if duplicated:
            message = f"归档批次已存在落地清单 {record.get('落地编号')}，保持一一对应"
        return batch, message

    # ------------------------------------------------------------------ 清理
    def mark_cleanable(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        batch = self._find(batch_id)
        if batch is None:
            return None, f"归档批次 {batch_id} 不存在"
        if batch.get("批次状态") == CONFLICT:
            return None, "冲突归档批次不能进入可清理范围"
        if not batch.get("校验摘要"):
            return None, "缺少校验摘要的批次不能进入可清理范围"
        scope = self.cleanup_scope(batch)
        if scope["可清理分区数"] == 0:
            batch["清理状态"] = IN_RETENTION
            return None, f"批次 {batch.get('批次编号')} 的数据仍在 {scope['保留年限']} 年保留期内，无可清理分区"
        batch["清理状态"] = CLEANABLE
        batch["版本号"] = int(batch.get("版本号", 1)) + 1
        return batch, f"批次 {batch.get('批次编号')} 已进入可清理范围，共 {scope['可清理分区数']} 个分区"

    def _conflict_group_of(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        for group in self._conflict_groups():
            if any(int(row.get("id", 0)) == int(batch.get("id", -1)) for row in group):
                return group
        return []

    def authoritative(self, batch: dict[str, Any]) -> dict[str, Any] | None:
        """同一完成顺序冲突组里可恢复的那一份；不在冲突组里时返回自身。"""
        group = self._conflict_group_of(batch)
        if not group:
            return batch
        complete = [row for row in group if self._complete(row)]
        return complete[0] if complete else None

    def receipt(self, batch_id: int, receipt_no: str | None = None) -> tuple[dict[str, Any] | None, str]:
        batch = self._find(batch_id)
        if batch is None:
            return None, f"归档批次 {batch_id} 不存在"
        if batch.get("批次状态") == CONFLICT:
            return None, "冲突归档批次不能登记清理回执"
        keeper = self.authoritative(batch)
        if keeper is not None and int(keeper.get("id", 0)) != int(batch.get("id", -1)):
            return None, (
                f"完成顺序冲突，仅可对可恢复批次 {keeper.get('批次编号')} 登记清理回执，"
                f"批次 {batch.get('批次编号')} 不能重复清理"
            )
        if keeper is None:
            return None, "完成顺序冲突且没有完整归档批次，不能登记清理回执"
        if batch.get("落地状态") != LANDED_STATUS:
            return None, "归档批次尚未落地，不能登记清理回执"
        if not str(receipt_no or "").strip():
            return None, "缺少清理回执编号"
        if batch.get("回执状态") == RECEIVED:
            return batch, f"清理回执已登记，批次 {batch.get('批次编号')} 保持已清理状态"
        group = self._conflict_group_of(batch)
        now_text = _now()
        batch["回执状态"] = RECEIVED
        batch["清理状态"] = CLEANED_STATUS
        batch["批次状态"] = CLEANED
        batch["回执编号"] = str(receipt_no).strip()
        batch["回执时间"] = now_text
        batch["备注"] = "清理回执已到达，归档文件完成清理"
        batch["版本号"] = int(batch.get("版本号", 1)) + 1
        # 冲突组内其余副本同步标记为冲突归档，保证只有一份完整批次可读取。
        for row in group:
            if int(row.get("id", 0)) == int(batch.get("id", -1)):
                continue
            row["批次状态"] = CONFLICT
            row["清理状态"] = VOID
            row["回执状态"] = RECEIVED
            row["回执编号"] = batch["回执编号"]
            row["回执时间"] = now_text
            row["备注"] = f"完成顺序冲突，回执已由可恢复批次 {batch.get('批次编号')} 登记"
            row["版本号"] = int(row.get("版本号", 1)) + 1
        return batch, f"批次 {batch.get('批次编号')} 清理回执已登记"

    # ------------------------------------------------------------------ 完成顺序冲突恢复
    def _conflict_groups(self) -> list[list[dict[str, Any]]]:
        """归档文件已写入、清理回执尚未到达、且同一数据集出现多份落地批次的冲突组。"""
        pending = [
            row for row in self._rows()
            if row.get("回执状态") == WAIT_RECEIPT
            and row.get("落地状态") == LANDED_STATUS
            and row.get("批次状态") not in (CONFLICT, CLEANED)
        ]
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in pending:
            groups.setdefault((str(row.get("策略编号")), str(row.get("数据截止"))), []).append(row)
        return [sorted(group, key=lambda row: int(row.get("id", 0))) for group in groups.values() if len(group) > 1]

    def _complete(self, row: dict[str, Any]) -> bool:
        return bool(self._all_archived(row) and row.get("校验摘要"))

    def resolve_conflicts(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """完成顺序冲突只保留一份可恢复的完整归档批次，其余副本作废。"""
        recoverable: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        for group in self._conflict_groups():
            complete = [row for row in group if self._complete(row)]
            chosen = complete[0] if complete else None
            if chosen is not None:
                recoverable.append(chosen)
            for row in group:
                if row is chosen:
                    continue
                row["批次状态"] = CONFLICT
                row["清理状态"] = VOID
                row["备注"] = (
                    f"完成顺序冲突，仅保留可恢复批次 {chosen.get('批次编号')}"
                    if chosen is not None else "完成顺序冲突，回执未到达且无完整归档，全部标记冲突"
                )
                row["版本号"] = int(row.get("版本号", 1)) + 1
                conflicts.append(row)
        return recoverable, conflicts

    def recoverable_view(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """只读视图：保证同一完成顺序冲突组只暴露一份可恢复的完整归档批次。"""
        readable: dict[int, dict[str, Any]] = {}
        conflict_rows: list[dict[str, Any]] = []
        disputed: set[int] = set()

        for group in self._conflict_groups():
            complete = [row for row in group if self._complete(row)]
            chosen = complete[0] if complete else None
            for row in group:
                if row is chosen:
                    readable[int(row["id"])] = row
                else:
                    disputed.add(int(row["id"]))
                    conflict_rows.append({
                        **row,
                        "可恢复": False,
                        "冲突说明": (
                            f"完成顺序冲突，仅保留可恢复批次 {chosen.get('批次编号')}"
                            if chosen is not None else "回执未到达且无完整归档"
                        ),
                    })

        for row in self._rows():
            row_id = int(row["id"])
            if row_id in disputed or row_id in readable:
                continue
            if row.get("批次状态") == CONFLICT or row.get("清理状态") == VOID:
                continue
            if (
                row.get("落地状态") == LANDED_STATUS
                and row.get("回执状态") == WAIT_RECEIPT
                and self._complete(row)
            ):
                readable[row_id] = row

        recoverable = sorted(readable.values(), key=lambda row: int(row.get("id", 0)))
        return recoverable, conflict_rows

    def simulate_completion_conflict(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        """复现“归档已写入、回执未到达、完成顺序错乱”的场景：生成一份同数据集副本。"""
        source = self._find(batch_id)
        if source is None:
            return None, f"归档批次 {batch_id} 不存在"
        if not self._complete(source):
            return None, "只有校验通过的完整归档批次才能复现完成顺序冲突"
        batch_no = self._next_batch_no()
        clone = copy.deepcopy(source)
        clone["id"] = self._next_id(self._rows())
        clone["批次编号"] = batch_no
        clone["提交键"] = f"{source.get('提交键')}#副本"
        clone["生成时间"] = _now()
        clone["备注"] = "完成顺序冲突副本：归档文件已写入，清理回执未到达"
        clone["批次状态"] = VERIFIED
        clone["回执状态"] = WAIT_RECEIPT
        clone["落地状态"] = LANDED_STATUS
        clone["清理状态"] = source.get("清理状态")
        clone["版本号"] = 1
        rows = self._partitions()
        next_id = self._next_id(rows)
        for offset, part in enumerate(self.partitions_of(str(source.get("批次编号") or ""))):
            copy_part = copy.deepcopy(part)
            copy_part["id"] = next_id + offset
            copy_part["批次编号"] = batch_no
            copy_part["分区编号"] = str(part.get("分区编号", "")).replace(str(source.get("批次编号")), batch_no)
            copy_part["归档路径"] = str(part.get("归档路径", "")).replace(str(source.get("批次编号")), batch_no)
            rows.append(copy_part)
        self._rows().append(clone)
        return clone, f"已生成完成顺序冲突副本 {batch_no}，等待冲突恢复"

    # ------------------------------------------------------------------ 批量操作
    def _apply_action(self, batch_id: int, action: str, *, checksum: str | None, receipt_no: str | None) -> tuple[dict[str, Any] | None, str]:
        if action == "verify":
            return self.verify(batch_id, checksum)
        if action == "retry":
            return self.retry(batch_id)
        if action == "landing":
            return self.landing(batch_id)
        if action == "mark_cleanable":
            return self.mark_cleanable(batch_id)
        if action == "receipt":
            return self.receipt(batch_id, receipt_no)
        return None, f"动作「{action}」不属于归档批次数据集可执行范围"

    def bulk(
        self,
        ids: list[int],
        action_name: str,
        *,
        checksum: str | None = None,
        receipt_no: str | None = None,
        simulate_commit_failure: bool = False,
    ) -> tuple[bool, str, list[dict[str, Any]], bool]:
        action = BULK_ACTIONS.get(action_name)
        if action is None:
            return False, f"动作「{action_name}」不属于历史归档批量操作范围", [], False
        if not ids:
            return False, "请选择至少一个归档批次", [], False

        snapshot = {
            BATCH_MODULE: copy.deepcopy(self._rows()),
            PARTITION_MODULE: copy.deepcopy(self._partitions()),
            LANDING_MODULE: copy.deepcopy(store.rows(LANDING_MODULE)),
        }
        results: list[dict[str, Any]] = []
        for batch_id in ids:
            batch = self._find(int(batch_id))
            if batch is None:
                results.append({"id": int(batch_id), "批次编号": "", "ok": False, "message": f"归档批次 {batch_id} 不存在"})
                continue
            batch_no = batch.get("批次编号")
            entry, message = self._apply_action(int(batch_id), action, checksum=checksum, receipt_no=receipt_no)
            results.append({"id": int(batch_id), "批次编号": batch_no, "ok": entry is not None, "message": message})

        if simulate_commit_failure:
            self._restore(snapshot)
            rolled_back = [
                {**item, "ok": False, "message": f"整组事务提交失败已回滚：{item['message']}"}
                for item in results
            ]
            return False, "归档批次数据集整组提交失败，历史归档已回滚并保留上一版本", rolled_back, True

        ok = all(item["ok"] for item in results)
        succeeded = sum(1 for item in results if item["ok"])
        message = f"归档批次数据集整组提交完成：成功 {succeeded} 条，失败 {len(results) - succeeded} 条"
        return ok, message, results, False

    def _restore(self, snapshot: dict[str, list[dict[str, Any]]]) -> None:
        for module, rows in snapshot.items():
            current = store.rows(module)
            current.clear()
            current.extend(rows)

    # ------------------------------------------------------------------ 查询
    def list_batches(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        cleanup_status: str | None = None,
        strategy: str | None = None,
        include_conflicts: bool = False,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(self._rows())
        if not include_conflicts:
            # 完成顺序冲突的副本不再作为可读取的归档批次数据集出现。
            rows = [row for row in rows if row.get("批次状态") != CONFLICT and row.get("清理状态") != VOID]
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if needle in str(row.get("批次编号", "")).casefold()
                or needle in str(row.get("策略编号", "")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("批次状态") == status]
        if cleanup_status:
            rows = [row for row in rows if row.get("清理状态") == cleanup_status]
        if strategy:
            rows = [row for row in rows if row.get("策略编号") == strategy]
        rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_detail(self, batch_id: int) -> dict[str, Any] | None:
        batch = self._find(batch_id)
        if batch is None:
            return None
        detail = dict(batch)
        detail["partitions"] = self.partitions_of(str(batch.get("批次编号")))
        detail["landing"] = self.storage.find_landing(str(batch.get("批次编号")))
        detail["cleanup_scope"] = self.cleanup_scope(batch)
        return detail

    def summary(self) -> dict[str, int]:
        rows = [row for row in self._rows() if row.get("批次状态") != CONFLICT]
        return {
            "归档批次总数": len(rows),
            "待校验批次": sum(1 for row in rows if row.get("批次状态") == PENDING),
            "部分失败批次": sum(1 for row in rows if row.get("批次状态") == PARTIAL_FAILED),
            "可清理批次": sum(1 for row in rows if row.get("清理状态") == CLEANABLE),
            "已落地批次": sum(1 for row in rows if row.get("落地状态") == LANDED_STATUS),
        }

    # ------------------------------------------------------------------ 演示数据
    def _seed_demo_batches(self) -> None:
        if self._rows():
            return
        today = date.today()
        batch, _, _ = self.generate(1, data_end=today - timedelta(days=400))
        if batch is not None:
            self.verify(int(batch["id"]))
        self.generate(2, data_end=today - timedelta(days=200), fail_count=1)
        landed, _, _ = self.generate(3, data_end=today - timedelta(days=1200))
        if landed is not None:
            self.verify(int(landed["id"]))
            self.landing(int(landed["id"]))
            self.receipt(int(landed["id"]), "CLR-SEED-0001")


service = ArchiveBatchService()
