from __future__ import annotations
from contextlib import contextmanager
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator
from uuid import uuid4


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (digest TEXT PRIMARY KEY, run_id TEXT NOT NULL, result TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT OR IGNORE INTO meta(key, value) VALUES ('version', '0');

-- 实际入库日志。seq 是单调写入顺序，也是恢复时的“已恢复位置”游标。
CREATE TABLE IF NOT EXISTS logs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    log_id TEXT NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    timestamp TEXT NOT NULL,
    hour TEXT NOT NULL,
    service TEXT NOT NULL,
    status INTEGER NOT NULL,
    errors INTEGER NOT NULL,
    payload TEXT NOT NULL,
    raw TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'CURRENT',
    ingested_at TEXT NOT NULL,
    UNIQUE(receipt_id, line_no)
);
CREATE INDEX IF NOT EXISTS logs_state ON logs(state);
CREATE INDEX IF NOT EXISTS logs_seq ON logs(seq);
CREATE INDEX IF NOT EXISTS logs_receipt ON logs(receipt_id);

-- 当前小时桶。恢复的最终切换才替换这里，失败时保留旧版。
CREATE TABLE IF NOT EXISTS buckets (
    hour TEXT NOT NULL,
    service TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(hour, service)
);

-- 回执独立持久化，整库恢复不触碰它。
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    payload TEXT NOT NULL,
    result TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    backup_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    version INTEGER NOT NULL,
    backup_seq INTEGER NOT NULL,
    log_count INTEGER NOT NULL,
    totals TEXT NOT NULL,
    snapshot TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_members (
    backup_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (backup_id, seq)
);

CREATE TABLE IF NOT EXISTS restores (
    restore_id TEXT PRIMARY KEY,
    backup_id TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    checkpoint INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    chunk_size INTEGER NOT NULL DEFAULT 100,
    error TEXT,
    result TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS restores_status ON restores(status);

-- 暂存区：恢复先把备份日志摊到暂存桶，切换成功才替换当前桶。
CREATE TABLE IF NOT EXISTS restore_stage (
    restore_id TEXT NOT NULL,
    hour TEXT NOT NULL,
    service TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (restore_id, hour, service)
);

-- 备份成员标记。切换时据此把“备份内”日志设为 CURRENT，其余隔离为 PENDING。
CREATE TABLE IF NOT EXISTS restore_members (
    restore_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (restore_id, seq)
);
"""


class StaleVersion(Exception):
    """恢复所基于的版本已被其它恢复越过；必须拒绝且不动当前桶。"""


class SwitchFailed(Exception):
    """切换阶段失败；事务回滚，旧版小时桶保留。"""


class RestoreIncomplete(Exception):
    """备份尚未全部摊到暂存区，不能切换。"""


class RestoreInProgress(Exception):
    """恢复进行中，待采纳日志此时不能并入当前桶。"""


class ReceiptConflict(Exception):
    """同一回执编号提交了不同内容。"""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return sha256(_canonical(value).encode("utf-8")).hexdigest()


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Store:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)

    def connect(self) -> sqlite3.Connection:
        # isolation_level=None: 由调用方用 BEGIN IMMEDIATE/COMMIT 管理事务。
        return sqlite3.connect(self.path, timeout=30, isolation_level=None)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.execute("COMMIT")
        except BaseException:
            try:
                db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            db.close()

    # ---- M6 运行记录（保持原有行为）----

    def record(self, digest: str, run_id: str, result: dict) -> dict:
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?, ?)",
                       (digest, run_id, json.dumps(result, ensure_ascii=False, sort_keys=True)))
            row = db.execute("SELECT run_id, result FROM runs WHERE digest=?", (digest,)).fetchone()
        return {"run_id": row[0], "result": json.loads(row[1])}

    def count(self) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]

    def all(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT run_id, digest, result FROM runs ORDER BY run_id").fetchall()
        return [{"run_id": r[0], "digest": r[1], "result": json.loads(r[2])} for r in rows]

    # ---- 版本 ----

    @staticmethod
    def _version(db: sqlite3.Connection) -> int:
        row = db.execute("SELECT value FROM meta WHERE key='version'").fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _set_version(db: sqlite3.Connection, version: int) -> None:
        db.execute("INSERT INTO meta(key, value) VALUES ('version', ?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(version),))

    def version(self) -> int:
        with self.connect() as db:
            return self._version(db)

    # ---- 实际入库日志与回执 ----

    @staticmethod
    def _buckets(db: sqlite3.Connection, where: str = "", params: tuple = ()) -> list[dict]:
        rows = db.execute(
            "SELECT hour, service, COUNT(*), COALESCE(SUM(errors), 0) FROM logs "
            + (where or "") + " GROUP BY hour, service ORDER BY hour, service", params).fetchall()
        return [{"hour": r[0], "service": r[1], "count": int(r[2]), "errors": int(r[3])} for r in rows]

    def has_active_restore(self) -> bool:
        with self.connect() as db:
            return db.execute(
                "SELECT 1 FROM restores WHERE status='RECOVERING' LIMIT 1").fetchone() is not None

    def ingest(self, receipt_id: str, digest: str, payload: dict,
               normalized: list[dict], quarantine: list[dict]) -> dict:
        """写入一条回执对应的实际日志。

        同一 receipt_id 重试时直接返回原结果，不新增日志也不重复计数；若内容
        摘要不同则拒绝，避免用旧回执覆盖新内容。恢复进行中写入的日志标记为
        DEFERRED，不进入当前桶。
        """
        with self.transaction() as db:
            existing = db.execute(
                "SELECT digest, result FROM receipts WHERE receipt_id=?", (receipt_id,)).fetchone()
            if existing:
                if existing[0] != digest:
                    raise ReceiptConflict(receipt_id)
                result = json.loads(existing[1])
                result["replayed"] = True
                result["version"] = self._version(db)
                states = [r[0] for r in db.execute(
                    "SELECT DISTINCT state FROM logs WHERE receipt_id=? ORDER BY state",
                    (receipt_id,)).fetchall()]
                if len(states) == 1:
                    result["state"] = states[0]
                elif states:
                    result["state"] = "MIXED"
                return result

            state = "DEFERRED" if db.execute(
                "SELECT 1 FROM restores WHERE status='RECOVERING' LIMIT 1").fetchone() else "CURRENT"
            now = _now()
            inserted = 0
            for index, row in enumerate(normalized, 1):
                cursor = db.execute(
                    "INSERT OR IGNORE INTO logs(receipt_id, line_no, log_id, request_id, timestamp, "
                    "hour, service, status, errors, payload, raw, state, ingested_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (receipt_id, index, row["log_id"], row["request_id"], row["timestamp"],
                     row["hour"], row["service"], row["status"], row["errors"],
                     row["payload"], row.get("raw", ""), state, now))
                if cursor.rowcount:
                    inserted += 1
                    if state == "CURRENT":
                        db.execute(
                            "INSERT INTO buckets(hour, service, count, errors) VALUES (?,?,1,?) "
                            "ON CONFLICT(hour, service) DO UPDATE SET "
                            "count=count+1, errors=errors+excluded.errors",
                            (row["hour"], row["service"], row["errors"]))

            result = {
                "receipt_id": receipt_id,
                "digest": digest,
                "input_count": len(normalized) + len(quarantine),
                "valid_count": inserted,
                "quarantine_count": len(quarantine),
                "quarantine": quarantine,
                "state": state,
                "version": self._version(db),
                "replayed": False,
            }
            db.execute("INSERT INTO receipts(receipt_id, digest, payload, result, created_at) "
                       "VALUES (?,?,?,?,?)",
                       (receipt_id, digest, _canonical(payload), _canonical(result), now))
            return result

    def get_receipt(self, receipt_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT receipt_id, digest, payload, result, created_at FROM receipts "
                "WHERE receipt_id=?", (receipt_id,)).fetchone()
        if not row:
            return None
        return {"receipt_id": row[0], "digest": row[1], "payload": json.loads(row[2]),
                "result": json.loads(row[3]), "created_at": row[4]}

    def list_receipts(self, limit: int = 200) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT receipt_id, digest, created_at FROM receipts ORDER BY created_at, receipt_id "
                "LIMIT ?", (int(limit),)).fetchall()
        return [{"receipt_id": r[0], "digest": r[1], "created_at": r[2]} for r in rows]

    # ---- 备份（取自实际入库且属于当前桶的日志）----

    def create_backup(self) -> dict:
        with self.transaction() as db:
            version = self._version(db)
            members = [r[0] for r in db.execute(
                "SELECT seq FROM logs WHERE state='CURRENT' ORDER BY seq").fetchall()]
            buckets = self._buckets(db, "WHERE state='CURRENT'")
            backup_seq = max(members) if members else 0
            snapshot = {"version": version, "backup_seq": backup_seq,
                        "log_count": len(members), "buckets": buckets}
            digest = _digest({"members": members, **snapshot})
            backup_id = "bak-" + digest[:12]
            existing = db.execute(
                "SELECT backup_id FROM backups WHERE backup_id=?", (backup_id,)).fetchone()
            if existing:
                return self._backup_row(db, backup_id)
            totals = {"count": sum(r["count"] for r in buckets),
                      "errors": sum(r["errors"] for r in buckets),
                      "buckets": len(buckets)}
            db.execute(
                "INSERT INTO backups(backup_id, created_at, version, backup_seq, log_count, "
                "totals, snapshot) VALUES (?,?,?,?,?,?,?)",
                (backup_id, _now(), version, backup_seq, len(members),
                 _canonical(totals), _canonical(snapshot)))
            db.executemany("INSERT INTO backup_members(backup_id, seq) VALUES (?,?)",
                           [(backup_id, seq) for seq in members])
            return self._backup_row(db, backup_id)

    @staticmethod
    def _backup_row(db: sqlite3.Connection, backup_id: str) -> dict:
        row = db.execute(
            "SELECT backup_id, created_at, version, backup_seq, log_count, totals, snapshot "
            "FROM backups WHERE backup_id=?", (backup_id,)).fetchone()
        return {"backup_id": row[0], "created_at": row[1], "version": row[2],
                "backup_seq": row[3], "log_count": row[4],
                "totals": json.loads(row[5]), "snapshot": json.loads(row[6])}

    def get_backup(self, backup_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT backup_id FROM backups WHERE backup_id=?",
                             (backup_id,)).fetchone()
            return self._backup_row(db, backup_id) if row else None

    def list_backups(self, limit: int = 200) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT backup_id, created_at, version, backup_seq, log_count, totals "
                "FROM backups ORDER BY created_at, backup_id LIMIT ?", (int(limit),)).fetchall()
        return [{"backup_id": r[0], "created_at": r[1], "version": r[2], "backup_seq": r[3],
                 "log_count": r[4], "totals": json.loads(r[5])} for r in rows]

    def resolve_backup_at(self, at: str) -> dict | None:
        """按选定备份时点取“该时刻及之前最近”的备份。

        时间按解析后的 UTC 时刻比较，避免 ISO 字符串不同精度造成的字典序误差。
        """
        from datetime import datetime, timezone

        raw = str(at).replace("Z", "+00:00")
        moment = datetime.fromisoformat(raw)
        if moment.tzinfo is None:
            # 与 M2 一致：裸时间不猜时区。
            raise ValueError("TIMEZONE_REQUIRED")
        moment = moment.astimezone(timezone.utc)
        with self.connect() as db:
            rows = db.execute(
                "SELECT backup_id, created_at FROM backups ORDER BY created_at, backup_id").fetchall()
            chosen = None
            for backup_id, created_at in rows:
                try:
                    stamp = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
                except ValueError:
                    continue
                if stamp.astimezone(timezone.utc) <= moment:
                    chosen = backup_id
            return self._backup_row(db, chosen) if chosen else None

    # ---- 恢复 ----

    def create_restore(self, backup_id: str, expected_version: int | None = None,
                       chunk_size: int = 100, restore_id: str | None = None) -> dict:
        with self.transaction() as db:
            backup = db.execute("SELECT backup_id FROM backups WHERE backup_id=?",
                                (backup_id,)).fetchone()
            if not backup:
                raise KeyError(backup_id)
            version = self._version(db)
            if expected_version is not None and int(expected_version) != version:
                raise StaleVersion(f"expected {expected_version}, current {version}")
            # 自动编号必须每次不同，否则并发发起会命中同一条恢复记录而绕过
            # 版本栅栏；显式传入 restore_id 时才按幂等处理。
            restore_id = restore_id or "rst-" + uuid4().hex[:12]
            existing = db.execute("SELECT status FROM restores WHERE restore_id=?",
                                  (restore_id,)).fetchone()
            if existing:
                return self._restore_row(db, restore_id)
            now = _now()
            db.execute(
                "INSERT INTO restores(restore_id, backup_id, base_version, status, checkpoint, "
                "processed, total, chunk_size, created_at, updated_at) "
                "VALUES (?,?,?,?,0,0,0,?,?,?)",
                (restore_id, backup_id, version, "RECOVERING",
                 int(chunk_size or 100), now, now))
            db.execute("INSERT OR IGNORE INTO restore_members(restore_id, seq) "
                       "SELECT ?, seq FROM backup_members WHERE backup_id=?", (restore_id, backup_id))
            total = db.execute("SELECT COUNT(*) FROM restore_members WHERE restore_id=?",
                               (restore_id,)).fetchone()[0]
            db.execute("UPDATE restores SET total=? WHERE restore_id=?", (int(total), restore_id))
            return self._restore_row(db, restore_id)

    @staticmethod
    def _restore_row(db: sqlite3.Connection, restore_id: str) -> dict:
        row = db.execute(
            "SELECT restore_id, backup_id, base_version, status, checkpoint, processed, total, "
            "chunk_size, error, result, created_at, updated_at FROM restores WHERE restore_id=?",
            (restore_id,)).fetchone()
        if not row:
            return None
        return {"restore_id": row[0], "backup_id": row[1], "base_version": row[2],
                "status": row[3], "checkpoint": row[4], "processed": row[5],
                "total": row[6], "chunk_size": row[7], "error": row[8],
                "result": json.loads(row[9]) if row[9] else None,
                "created_at": row[10], "updated_at": row[11],
                "complete": row[5] >= row[6]}

    def get_restore(self, restore_id: str) -> dict | None:
        with self.connect() as db:
            return self._restore_row(db, restore_id)

    def list_restores(self, limit: int = 200) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT restore_id FROM restores ORDER BY created_at, restore_id "
                              "LIMIT ?", (int(limit),)).fetchall()
            return [self._restore_row(db, r[0]) for r in rows]

    def process_restore(self, restore_id: str, chunks: int | None = None) -> dict:
        """按 chunk 把备份日志摊到暂存区，并持久化 checkpoint。

        每个 chunk 单独提交，因此中途异常只会丢掉未提交的 chunk，下次从
        checkpoint 续跑，不会重复累计已恢复部分。
        """
        with self.connect() as db:
            restore = self._restore_row(db, restore_id)
        if not restore:
            raise KeyError(restore_id)
        if restore["status"] != "RECOVERING":
            return restore

        remaining = None if chunks is None else max(0, int(chunks))
        if remaining == 0:
            return restore
        guard = 0
        while True:
            with self.transaction() as db:
                current = self._restore_row(db, restore_id)
                if current["status"] != "RECOVERING" or current["processed"] >= current["total"]:
                    restore = current
                    break
                rows = db.execute(
                    "SELECT l.seq, l.hour, l.service, l.errors FROM restore_members rm "
                    "JOIN logs l ON l.seq=rm.seq WHERE rm.restore_id=? AND l.seq>? "
                    "ORDER BY l.seq LIMIT ?",
                    (restore_id, current["checkpoint"], int(current["chunk_size"]))).fetchall()
                if not rows:
                    # 成员没有对应日志属于数据不一致；保持 RECOVERING 以便人工续跑。
                    restore = current
                    break
                for seq, hour, service, errors in rows:
                    db.execute(
                        "INSERT INTO restore_stage(restore_id, hour, service, count, errors) "
                        "VALUES (?,?,?,1,?) ON CONFLICT(restore_id, hour, service) DO UPDATE SET "
                        "count=count+1, errors=errors+excluded.errors",
                        (restore_id, hour, service, int(errors)))
                db.execute("UPDATE restores SET checkpoint=?, processed=?, updated_at=? "
                           "WHERE restore_id=?",
                           (rows[-1][0], current["processed"] + len(rows), _now(), restore_id))
                restore = self._restore_row(db, restore_id)
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    break
            guard += 1
            if guard > 1_000_000:
                break
        return restore

    def resume_restore(self, restore_id: str, chunks: int | None = None) -> dict:
        return self.process_restore(restore_id, chunks)

    def mark_restore(self, restore_id: str, status: str, error: str | None = None) -> None:
        with self.transaction() as db:
            db.execute("UPDATE restores SET status=?, error=?, updated_at=? WHERE restore_id=?",
                       (status, error, _now(), restore_id))
            self._reconcile_deferred(db)

    def abort_restore(self, restore_id: str, reason: str | None = None) -> dict:
        """放弃未生效的恢复：暂存区丢弃，当前桶不动，DEFERRED 转为待采纳。"""
        with self.transaction() as db:
            restore = self._restore_row(db, restore_id)
            if not restore:
                raise KeyError(restore_id)
            if restore["status"] == "RECOVERING":
                db.execute("DELETE FROM restore_stage WHERE restore_id=?", (restore_id,))
                db.execute("UPDATE restores SET status='ABORTED', error=?, updated_at=? "
                           "WHERE restore_id=?", (reason, _now(), restore_id))
            self._reconcile_deferred(db)
            return self._restore_row(db, restore_id)

    def switch_restore(self, restore_id: str, fail: bool = False) -> dict:
        """把暂存桶一次性切换为当前桶。

        版本不匹配、尚未摊完或显式失败都在事务内抛出，回滚后当前桶保持旧版。
        """
        with self.transaction() as db:
            restore = self._restore_row(db, restore_id)
            if not restore:
                raise KeyError(restore_id)
            if restore["status"] == "EFFECTIVE":
                return restore
            if restore["status"] == "STALE_VERSION":
                raise StaleVersion(restore.get("error") or "版本已过期")
            # FAILED_SWITCH 保留暂存区，允许原样重试；其它状态不可切换。
            if restore["status"] not in ("RECOVERING", "FAILED_SWITCH"):
                raise SwitchFailed(f"restore status {restore['status']}")
            if restore["processed"] < restore["total"]:
                raise RestoreIncomplete(
                    f"processed {restore['processed']} of {restore['total']}")
            version = self._version(db)
            if restore["base_version"] != version:
                raise StaleVersion(
                    f"base {restore['base_version']}, current {version}")
            if fail:
                raise SwitchFailed("switch failure requested")

            staged = db.execute(
                "SELECT hour, service, count, errors FROM restore_stage WHERE restore_id=? "
                "ORDER BY hour, service", (restore_id,)).fetchall()
            backup = self._backup_row(db, restore["backup_id"])
            totals = {"count": sum(int(r[2]) for r in staged),
                      "errors": sum(int(r[3]) for r in staged),
                      "buckets": len(staged)}
            expected = {"count": int(backup["totals"]["count"]),
                        "errors": int(backup["totals"]["errors"]),
                        "buckets": int(backup["totals"]["buckets"])}
            if totals != expected:
                raise SwitchFailed(
                    f"staged totals {totals} != backup totals {expected}")
            db.execute("DELETE FROM buckets")
            db.executemany(
                "INSERT INTO buckets(hour, service, count, errors) VALUES (?,?,?,?)",
                [(r[0], r[1], int(r[2]), int(r[3])) for r in staged])
            db.execute("UPDATE logs SET state='PENDING'")
            db.execute("UPDATE logs SET state='CURRENT' WHERE seq IN "
                       "(SELECT seq FROM restore_members WHERE restore_id=?)", (restore_id,))
            new_version = version + 1
            self._set_version(db, new_version)
            # 同一时点发起的其它恢复已被这次切换越过，立即判为过期，避免
            # 僵尸恢复继续阻塞接收与采纳。
            db.execute("UPDATE restores SET status=?, error=?, updated_at=? "
                       "WHERE status=? AND restore_id<>? AND base_version<?",
                       ("STALE_VERSION", "已被并发恢复越过", _now(),
                        "RECOVERING", restore_id, new_version))
            current = db.execute("SELECT COUNT(*) FROM logs WHERE state='CURRENT'").fetchone()[0]
            pending = db.execute("SELECT COUNT(*) FROM logs WHERE state='PENDING'").fetchone()[0]
            result = {"version": new_version, "current_count": int(current),
                      "pending_count": int(pending), "totals": totals,
                      "buckets": [{"hour": r[0], "service": r[1], "count": int(r[2]),
                                   "errors": int(r[3])} for r in staged]}
            db.execute("UPDATE restores SET status='EFFECTIVE', result=?, error=NULL, "
                       "updated_at=? WHERE restore_id=?",
                       (_canonical(result), _now(), restore_id))
            self._reconcile_deferred(db)
            return self._restore_row(db, restore_id)

    # ---- 回读 ----

    @staticmethod
    def _log_rows(db: sqlite3.Connection, where: str, params: tuple, limit: int) -> list[dict]:
        rows = db.execute(
            "SELECT seq, receipt_id, log_id, request_id, timestamp, hour, service, status, "
            "errors, state FROM logs " + where + " ORDER BY seq LIMIT ?",
            params + (int(limit),)).fetchall()
        return [{"seq": r[0], "receipt_id": r[1], "log_id": r[2], "request_id": r[3],
                 "timestamp": r[4], "hour": r[5], "service": r[6], "status": r[7],
                 "errors": r[8], "state": r[9]} for r in rows]

    def readback(self, restore_id: str, limit: int = 500) -> dict | None:
        """回读一次恢复涉及的三段范围：恢复中、已生效、待采纳。

        - ``recovering``：本恢复的成员日志及其已摊入暂存区的桶；``processed``
          标记该成员是否已越过 checkpoint。
        - ``effective``：当前仍在生效的小时桶与日志。
        - ``pending``：不属于本恢复的较新日志（恢复期间接收的为 DEFERRED），
          它们未进入当前桶，等待采纳。
        """
        with self.connect() as db:
            restore = self._restore_row(db, restore_id)
            if not restore:
                return None
            staged = db.execute(
                "SELECT hour, service, count, errors FROM restore_stage WHERE restore_id=? "
                "ORDER BY hour, service", (restore_id,)).fetchall()
            members = db.execute(
                "SELECT l.seq, l.receipt_id, l.log_id, l.request_id, l.timestamp, l.hour, "
                "l.service, l.status, l.errors, l.state FROM restore_members rm "
                "JOIN logs l ON l.seq=rm.seq WHERE rm.restore_id=? ORDER BY l.seq LIMIT ?",
                (restore_id, int(limit))).fetchall()
            current = self._buckets(db, "WHERE state='CURRENT'")
            effective_logs = self._log_rows(db, "WHERE state='CURRENT'", (), limit)
            pending_where = ("WHERE seq NOT IN (SELECT seq FROM restore_members WHERE restore_id=?)"
                             if restore["status"] != "EFFECTIVE" else "WHERE state='PENDING'")
            pending_params: tuple = () if restore["status"] == "EFFECTIVE" else (restore_id,)
            pending_buckets = self._buckets(db, pending_where, pending_params)
            pending_logs = self._log_rows(db, pending_where, pending_params, limit)
            if restore["status"] == "EFFECTIVE":
                receipts = db.execute(
                    "SELECT r.receipt_id, COUNT(l.seq), "
                    "COALESCE(SUM(CASE WHEN l.state='CURRENT' THEN 1 ELSE 0 END), 0), "
                    "COALESCE(SUM(CASE WHEN l.state='PENDING' THEN 1 ELSE 0 END), 0), "
                    "COALESCE(SUM(CASE WHEN l.state='DEFERRED' THEN 1 ELSE 0 END), 0) "
                    "FROM receipts r LEFT JOIN logs l ON l.receipt_id=r.receipt_id "
                    "GROUP BY r.receipt_id ORDER BY r.receipt_id LIMIT ?", (int(limit),)).fetchall()
            else:
                # 恢复尚未生效时，不属于备份的日志都算待采纳（含恢复期间的
                # DEFERRED），不能因为它们状态仍为 CURRENT 就报成已生效。
                receipts = db.execute(
                    "SELECT r.receipt_id, COUNT(l.seq), "
                    "COALESCE(SUM(CASE WHEN l.seq IS NOT NULL AND l.seq IN "
                    "(SELECT seq FROM restore_members WHERE restore_id=?) THEN 1 ELSE 0 END), 0), "
                    "COALESCE(SUM(CASE WHEN l.seq IS NOT NULL AND l.seq NOT IN "
                    "(SELECT seq FROM restore_members WHERE restore_id=?) THEN 1 ELSE 0 END), 0), "
                    "COALESCE(SUM(CASE WHEN l.state='DEFERRED' THEN 1 ELSE 0 END), 0) "
                    "FROM receipts r LEFT JOIN logs l ON l.receipt_id=r.receipt_id "
                    "GROUP BY r.receipt_id ORDER BY r.receipt_id LIMIT ?",
                    (restore_id, restore_id, int(limit))).fetchall()

        epoch = restore["checkpoint"]
        member_rows = [{"seq": r[0], "receipt_id": r[1], "log_id": r[2], "request_id": r[3],
                        "timestamp": r[4], "hour": r[5], "service": r[6], "status": r[7],
                        "errors": r[8], "state": r[9],
                        "processed": bool(r[0] <= epoch)} for r in members]
        return {
            "restore": restore,
            "scope": ["RECOVERING", "EFFECTIVE", "PENDING"],
            "recovering": {
                "state": "RECOVERING" if restore["status"] == "RECOVERING" else "CLOSED",
                "checkpoint": restore["checkpoint"],
                "processed": restore["processed"],
                "total": restore["total"],
                "buckets": [{"hour": r[0], "service": r[1], "count": int(r[2]),
                             "errors": int(r[3])} for r in staged],
                "logs": member_rows,
            },
            "effective": {
                "state": "EFFECTIVE",
                "version": self.version(),
                "buckets": current,
                "logs": effective_logs,
            },
            "pending": {
                "state": "PENDING",
                "count": sum(r["count"] for r in pending_buckets),
                "buckets": pending_buckets,
                "logs": pending_logs,
            },
            "receipts": [{"receipt_id": r[0], "log_count": int(r[1]),
                          "current": int(r[2]), "pending": int(r[3]),
                          "deferred": int(r[4])} for r in receipts],
        }

    @staticmethod
    def _reconcile_deferred(db: sqlite3.Connection) -> None:
        """没有进行中的恢复时，遗留的 DEFERRED 日志转为待采纳，避免永久不可见。"""
        active = db.execute(
            "SELECT 1 FROM restores WHERE status='RECOVERING' LIMIT 1").fetchone()
        if not active:
            db.execute("UPDATE logs SET state='PENDING' WHERE state='DEFERRED'")

    def readback_current(self) -> dict:
        """不依赖某次恢复，直接看当前桶、待采纳与恢复期间暂存的日志。"""
        with self.transaction() as db:
            self._reconcile_deferred(db)
            buckets = self._buckets(db, "WHERE state='CURRENT'")
            pending = self._buckets(db, "WHERE state='PENDING'")
            deferred = self._buckets(db, "WHERE state='DEFERRED'")
        return {"buckets": buckets, "pending": pending, "deferred": deferred,
                "current_count": sum(r["count"] for r in buckets),
                "pending_count": sum(r["count"] for r in pending),
                "deferred_count": sum(r["count"] for r in deferred)}

    def adopt_pending(self, receipt_ids: list[str] | None = None) -> dict:
        """把待采纳日志并入当前桶。

        只接受 PENDING；DEFERRED（恢复尚未切换）不会被采纳，避免越过恢复
        时点。按 receipt_id 限定可选，缺省采纳全部待采纳日志。
        """
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM restores WHERE status='RECOVERING' LIMIT 1").fetchone():
                raise RestoreInProgress()
            where = "state='PENDING'"
            params: tuple = ()
            if receipt_ids:
                placeholders = ",".join("?" for _ in receipt_ids)
                where += f" AND receipt_id IN ({placeholders})"
                params = tuple(receipt_ids)
            rows = db.execute(
                "SELECT seq, hour, service, errors FROM logs WHERE " + where + " ORDER BY seq",
                params).fetchall()
            for _, hour, service, errors in rows:
                db.execute(
                    "INSERT INTO buckets(hour, service, count, errors) VALUES (?,?,1,?) "
                    "ON CONFLICT(hour, service) DO UPDATE SET "
                    "count=count+1, errors=errors+excluded.errors",
                    (hour, service, int(errors)))
            db.executemany("UPDATE logs SET state='CURRENT' WHERE seq=?",
                           [(r[0],) for r in rows])
            adopted = len(rows)
            buckets = self._buckets(db, "WHERE state='CURRENT'")
        return {"adopted_count": adopted, "buckets": buckets,
                "current_count": sum(r["count"] for r in buckets)}
