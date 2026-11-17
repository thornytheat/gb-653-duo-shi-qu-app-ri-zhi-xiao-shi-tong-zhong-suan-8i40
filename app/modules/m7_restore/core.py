"""M7 备份恢复的纯规则函数。

这里只放与存储无关的规则：日志归一、小时键、桶累计与稳定摘要。
真正的事务编排在 :mod:`app.modules.m7_restore.service`。
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Iterable

from app.modules.m2_time.core import normalize as normalize_time
from app.modules.m4_buckets.core import hour_key

#: 恢复记录的状态。RECOVERING 是唯一允许继续写入暂存区的状态。
RECOVERING = "RECOVERING"
EFFECTIVE = "EFFECTIVE"
FAILED_SWITCH = "FAILED_SWITCH"
STALE_VERSION = "STALE_VERSION"
ABORTED = "ABORTED"

#: 回读范围固定覆盖三类记录，避免恢复中、已生效、待采纳之间出现盲区。
READBACK_SCOPE = (RECOVERING, EFFECTIVE, "PENDING")

PENDING = "PENDING"


class InvalidRecord(ValueError):
    """单条日志无法入库；由调用方转成隔离记录而不是中断整批。"""

    def __init__(self, reason: str, message: str = ""):
        self.reason = reason
        self.message = message or reason
        super().__init__(self.message)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return sha256(canonical(value).encode("utf-8")).hexdigest()


def _first(item: dict, *keys: str, default=None):
    for key in keys:
        if key in item:
            return item[key]
    return default


def _reason_for_time_error(exc: ValueError) -> str:
    text = str(exc)
    if text in {"TIMEZONE_REQUIRED", "DST_GAP"}:
        return text
    return "INVALID_VALUE"


def normalize_event(item: dict, index: int = 0, receipt_id: str = "") -> dict:
    """把一条实际入库日志变成可入桶的稳定记录。

    时间只用 M2 已有规则归一为 UTC，不增加任何时区校正。无法归一的记录
    以 :class:`InvalidRecord` 返回稳定原因码，由服务层隔离。
    """
    if not isinstance(item, dict):
        raise InvalidRecord("INVALID_VALUE", "日志必须是对象")

    raw_ts = _first(item, "timestamp", "ts", "time", "datetime")
    if raw_ts is None or str(raw_ts).strip() == "":
        raise InvalidRecord("EMPTY_VALUE", "timestamp 为空")
    zone = _first(item, "zone", "timezone")
    fold = _first(item, "fold", default=0)
    try:
        timestamp = normalize_time(str(raw_ts), zone, fold)
    except ValueError as exc:
        raise InvalidRecord(_reason_for_time_error(exc), str(exc)) from exc

    service = _first(item, "service", "svc")
    if service is None or str(service).strip() == "":
        raise InvalidRecord("EMPTY_VALUE", "service 为空")
    service = str(service)

    raw_status = _first(item, "status", "code", default=0)
    try:
        status = int(raw_status)
    except (TypeError, ValueError) as exc:
        raise InvalidRecord("INVALID_VALUE", "status 不是整数") from exc

    request_id = str(_first(item, "request_id", "id", default="") or "")
    # 空 request_id 仍可入库，但每行需要一个稳定且不冲突的内部编号。
    log_id = request_id or f"auto-{receipt_id or 'receipt'}-{index}"
    return {
        "log_id": log_id,
        "request_id": request_id,
        "timestamp": timestamp,
        "hour": hour_key(timestamp),
        "service": service,
        "status": status,
        "errors": int(status >= 500),
        "payload": canonical(item),
    }


def add_to_bucket(buckets: dict[tuple[str, str], dict], hour: str, service: str,
                  errors: int, count: int = 1) -> None:
    key = (hour, service)
    row = buckets.setdefault(key, {"hour": hour, "service": service, "count": 0, "errors": 0})
    row["count"] += int(count)
    row["errors"] += int(errors)


def sorted_buckets(buckets: Iterable[dict]) -> list[dict]:
    return sorted(buckets, key=lambda row: (row["hour"], row["service"]))


def totals(buckets: Iterable[dict]) -> dict:
    rows = list(buckets)
    return {
        "count": sum(int(row["count"]) for row in rows),
        "errors": sum(int(row["errors"]) for row in rows),
        "buckets": len(rows),
    }


def parse_jsonl(text: str) -> tuple[list[dict], list[dict]]:
    """把 M1 风格的 JSONL 拆成对象和隔离行；空行不产生记录。"""
    rows: list[dict] = []
    quarantine: list[dict] = []
    for line_no, line in enumerate(str(text).splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            quarantine.append({"source_line": line_no, "reason": "TRUNCATED", "raw": line})
            continue
        if not isinstance(value, dict):
            quarantine.append({"source_line": line_no, "reason": "INVALID_VALUE", "raw": line})
            continue
        value = dict(value)
        value.setdefault("source_line", line_no)
        rows.append(value)
    return rows, quarantine
