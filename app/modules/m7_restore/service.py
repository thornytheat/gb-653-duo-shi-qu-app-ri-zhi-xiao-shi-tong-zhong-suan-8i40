"""M7 备份恢复的服务编排。

存储层负责事务与游标；这里负责把请求翻译成明确的业务动作，并把可预期的
失败映射成稳定的错误码：

- ``STALE_VERSION``：恢复所基于的版本已被其它恢复越过，拒绝切换且不动当前桶。
- ``RESTORE_INCOMPLETE``：尚未摊完备份，不能切换。
- ``SWITCH_FAILED``：切换阶段失败，事务回滚后旧版小时桶仍在。
- ``RECEIPT_CONFLICT``：同一回执编号提交了不同内容。
"""
from __future__ import annotations

from app.errors import DomainError, NotFoundError, ReceiptConflictError, StaleVersionError
from app.manifest import canonical_digest
from app.store import (ReceiptConflict, RestoreInProgress, RestoreIncomplete, StaleVersion,
                       SwitchFailed)
from . import core


def _payload_dict(payload: dict | None) -> dict:
    return dict(payload or {})


def _events_from(payload: dict) -> tuple[list, list]:
    """既接受结构化 events，也接受 M1 风格的 JSONL text。"""
    if "events" in payload and payload["events"] is not None:
        events = payload["events"]
        if not isinstance(events, list):
            raise DomainError("INVALID_VALUE", "events 必须是数组")
        return list(events), []
    if "text" in payload and payload["text"] is not None:
        return core.parse_jsonl(str(payload["text"]))
    return [], []


def _normalize(rows: list, receipt_id: str) -> tuple[list[dict], list[dict]]:
    normalized: list[dict] = []
    quarantine: list[dict] = []
    for index, item in enumerate(rows, 1):
        if not isinstance(item, dict):
            quarantine.append({"source_line": index, "reason": "INVALID_VALUE",
                               "raw": str(item)})
            continue
        try:
            row = core.normalize_event(item, index=index, receipt_id=receipt_id)
        except core.InvalidRecord as exc:
            quarantine.append({"source_line": item.get("source_line", index),
                               "reason": exc.reason, "raw": core.canonical(item)})
            continue
        row["line_no"] = index
        row["raw"] = core.canonical(item)
        normalized.append(row)
    return normalized, quarantine


def ingest(store, payload: dict | None) -> dict:
    """接收日志并写回执。

    - 回执编号可由调用方显式给出；未给出时由内容摘要派生，重试可复现。
    - 摘要基于原始证据（events 或 JSONL 文本），重放同一内容可稳定命中。
    - 同一回执再次提交直接返回原结果（``replayed=True``），不新增日志。
    - 恢复进行中接收的日志为 DEFERRED，不进入当前桶。
    """
    data = _payload_dict(payload)
    if "events" in data and data["events"] is not None:
        basis = {"events": data["events"]}
    else:
        basis = {"text": data.get("text")}
    supplied = data.get("receipt_id")
    receipt_id = str(supplied) if supplied not in (None, "") else None
    digest = canonical_digest(basis)
    if receipt_id is None:
        receipt_id = "rcpt-" + digest[:12]
    rows, parsed_quarantine = _events_from(data)
    normalized, quarantine = _normalize(rows, receipt_id)
    quarantine.extend(parsed_quarantine)
    try:
        result = store.ingest(receipt_id, digest, basis, normalized, quarantine)
    except ReceiptConflict as exc:
        raise ReceiptConflictError(f"回执 {receipt_id} 已存在且内容不同") from exc
    return {"status": "READY", **result}


def create_backup(store, payload: dict | None = None) -> dict:
    return {"status": "READY", **store.create_backup()}


def list_backups(store) -> dict:
    return {"backups": store.list_backups()}


def get_backup(store, backup_id: str) -> dict:
    backup = store.get_backup(backup_id)
    if not backup:
        raise NotFoundError(f"备份 {backup_id} 不存在")
    return {"status": "READY", **backup}


def start_restore(store, payload: dict | None) -> dict:
    """创建恢复并可选地立刻推进若干 chunk。

    ``expected_version`` 是并发栅栏：不传则取创建时的当前版本；传了但已过期
    则直接拒绝，不创建恢复记录，也不触碰当前桶。
    """
    data = _payload_dict(payload)
    backup_id = data.get("backup_id") or data.get("backup") or data.get("backup_point")
    point_at = data.get("at") or data.get("backup_at") or data.get("point_at")
    if not backup_id and point_at in (None, ""):
        raise DomainError("EMPTY_VALUE", "backup_id 或备份时点不能为空")
    if not backup_id:
        try:
            resolved = store.resolve_backup_at(str(point_at))
        except ValueError as exc:
            raise DomainError("INVALID_VALUE", f"备份时点无效: {exc}") from exc
        if not resolved:
            raise NotFoundError(f"时点 {point_at} 之前没有可用备份")
        backup_id = resolved["backup_id"]
    expected = data.get("expected_version", data.get("base_version"))
    if expected in ("",):
        expected = None
    try:
        chunk_size = int(data.get("chunk_size") or 100)
    except (TypeError, ValueError) as exc:
        raise DomainError("INVALID_VALUE", "chunk_size 必须是整数") from exc
    if chunk_size <= 0:
        raise DomainError("INVALID_VALUE", "chunk_size 必须为正数")
    try:
        restore = store.create_restore(
            str(backup_id),
            expected_version=None if expected is None else int(expected),
            chunk_size=chunk_size,
            restore_id=data.get("restore_id"))
    except KeyError as exc:
        raise NotFoundError(f"备份 {backup_id} 不存在") from exc
    except StaleVersion as exc:
        raise StaleVersionError(str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise DomainError("INVALID_VALUE", "expected_version 必须是整数") from exc

    chunks = data.get("chunks")
    if chunks is not None:
        restore = store.process_restore(restore["restore_id"], int(chunks))
    return {"status": "READY", "restore": restore,
            "readback": store.readback(restore["restore_id"])}


def resume_restore(store, restore_id: str, payload: dict | None) -> dict:
    """从已恢复位置（checkpoint）继续把备份摊入暂存区。"""
    data = _payload_dict(payload)
    if not store.get_restore(restore_id):
        raise NotFoundError(f"恢复 {restore_id} 不存在")
    chunks = data.get("chunks")
    restore = store.process_restore(restore_id, None if chunks is None else int(chunks))
    return {"status": "READY", "restore": restore, "readback": store.readback(restore_id)}


def switch_restore(store, restore_id: str, payload: dict | None = None) -> dict:
    """把暂存桶切换为当前桶；失败回滚并保留旧版。

    过期版本标记为终态 STALE_VERSION；其它失败标记为 FAILED_SWITCH，可再次
    调用重试。两种情况当前小时桶都保持切换前的版本。
    """
    data = _payload_dict(payload)
    current = store.get_restore(restore_id)
    if not current:
        raise NotFoundError(f"恢复 {restore_id} 不存在")
    # 终态不再改写：STALE_VERSION 已明确被淘汰，ABORTED 已明确放弃。
    if current["status"] == core.STALE_VERSION:
        raise StaleVersionError(f"恢复 {restore_id} 的版本已过期")
    if current["status"] == core.ABORTED:
        raise DomainError("RESTORE_ABORTED", f"恢复 {restore_id} 已放弃")
    try:
        switched = store.switch_restore(restore_id, fail=bool(data.get("fail")))
    except StaleVersion as exc:
        store.mark_restore(restore_id, core.STALE_VERSION, str(exc))
        raise StaleVersionError(str(exc)) from exc
    except RestoreIncomplete as exc:
        raise DomainError("RESTORE_INCOMPLETE", str(exc)) from exc
    except SwitchFailed as exc:
        store.mark_restore(restore_id, core.FAILED_SWITCH, str(exc))
        raise DomainError("SWITCH_FAILED", str(exc)) from exc
    except DomainError:
        raise
    except Exception as exc:  # 切换阶段任何异常都不能污染当前桶
        store.mark_restore(restore_id, core.FAILED_SWITCH, str(exc))
        raise DomainError("SWITCH_FAILED", str(exc)) from exc
    return {"status": "READY", "restore": switched,
            "readback": store.readback(restore_id)}


def readback(store, restore_id: str, payload: dict | None = None) -> dict:
    data = _payload_dict(payload)
    try:
        limit = int(data.get("limit") or 500)
    except (TypeError, ValueError) as exc:
        raise DomainError("INVALID_VALUE", "limit 必须是整数") from exc
    view = store.readback(restore_id, limit=limit)
    if not view:
        raise NotFoundError(f"恢复 {restore_id} 不存在")
    return {"status": "READY", **view}


def current(store) -> dict:
    return {"status": "READY", "version": store.version(), **store.readback_current()}


def adopt_pending(store, payload: dict | None = None) -> dict:
    """把待采纳日志并入当前桶；恢复进行中拒绝。"""
    data = _payload_dict(payload)
    receipt_ids = data.get("receipt_ids") or data.get("receipt_id")
    if isinstance(receipt_ids, str):
        receipt_ids = [receipt_ids]
    try:
        adopted = store.adopt_pending(receipt_ids)
    except RestoreInProgress as exc:
        raise DomainError("RESTORE_IN_PROGRESS", "恢复进行中，暂不能采纳较新日志") from exc
    return {"status": "READY", "version": store.version(), **adopted}


def abort_restore(store, restore_id: str, payload: dict | None = None) -> dict:
    """放弃未生效的恢复；当前桶保持旧版。"""
    data = _payload_dict(payload)
    try:
        restore = store.abort_restore(restore_id, data.get("reason"))
    except KeyError as exc:
        raise NotFoundError(f"恢复 {restore_id} 不存在") from exc
    return {"status": "READY", "restore": restore}


def get_receipt(store, receipt_id: str) -> dict:
    receipt = store.get_receipt(receipt_id)
    if not receipt:
        raise NotFoundError(f"回执 {receipt_id} 不存在")
    return {"status": "READY", **receipt}


def list_receipts(store) -> dict:
    return {"receipts": store.list_receipts()}
