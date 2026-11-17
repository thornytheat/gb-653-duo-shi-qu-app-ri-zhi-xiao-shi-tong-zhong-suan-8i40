
import pytest

from app.store import Store


def post(client, path, body=None):
    return client.post(path, json=body or {})


def get(client, path):
    return client.get(path)


def events(*rows):
    return [{"request_id": r[0], "timestamp": r[1], "service": r[2], "status": r[3]}
            for r in rows]


SAMPLE = events(
    ("a", "2026-11-15T01:05:00Z", "pay", 200),
    ("b", "2026-11-15T01:10:00Z", "pay", 500),
)


def ingest(client, receipt_id, rows):
    response = post(client, "/api/intake", {"receipt_id": receipt_id, "events": rows})
    assert response.status_code == 200, response.text
    return response.json()


def backup(client):
    response = post(client, "/api/backups")
    assert response.status_code == 200
    return response.json()


def start(client, backup_id, **kw):
    response = post(client, "/api/restores", {"backup_id": backup_id, **kw})
    assert response.status_code == 200, response.text
    return response.json()


def buckets(client):
    return get(client, "/api/buckets/current").json()["buckets"]


def test_backup_taken_from_ingested_logs_only(client):
    ingest(client, "R1", SAMPLE)
    # 一条无效日志被隔离，不应进入备份。
    ingest(client, "R2", [{"request_id": "bad", "timestamp": "2026-01-01T00:00:00",
                           "service": "pay", "status": 200}])
    data = backup(client)
    assert data["log_count"] == 2
    assert data["totals"] == {"count": 2, "errors": 1, "buckets": 1}
    assert data["backup_seq"] == 2
    assert data["snapshot"]["buckets"][0]["hour"] == "2026-11-15T01:00:00Z"


def test_restore_returns_current_hour_bucket_to_backup_point(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    assert len(buckets(client)) == 2

    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    switched = post(client, f"/api/restores/{restore['restore_id']}/switch").json()
    assert switched["restore"]["status"] == "EFFECTIVE"
    assert switched["restore"]["result"]["totals"]["count"] == 2
    assert buckets(client) == [{"hour": "2026-11-15T01:00:00Z", "service": "pay",
                                "count": 2, "errors": 1}]


def test_newer_logs_are_isolated_as_pending_after_restore(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/switch")

    current = get(client, "/api/buckets/current").json()
    assert current["current_count"] == 2
    assert current["pending"] == [{"hour": "2026-11-15T02:00:00Z", "service": "pay",
                                   "count": 1, "errors": 0}]


def test_original_receipt_readable_after_restore(client):
    first = ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/switch")

    receipt = get(client, "/api/intake/receipts/R1").json()
    assert receipt["receipt_id"] == "R1"
    assert receipt["digest"] == first["digest"]
    assert receipt["payload"]["events"] == SAMPLE


def test_replaying_old_receipt_does_not_double_count(client):
    ingest(client, "R1", SAMPLE)
    before = get(client, "/api/buckets/current").json()["current_count"]
    replay = ingest(client, "R1", SAMPLE)
    assert replay["replayed"] is True
    after = get(client, "/api/buckets/current").json()["current_count"]
    assert before == after == 2


def test_receipt_conflict_rejected(client):
    ingest(client, "R1", SAMPLE)
    response = post(client, "/api/intake", {"receipt_id": "R1", "events": []})
    assert response.status_code == 409
    assert response.json()["error"] == "RECEIPT_CONFLICT"


def test_ingest_during_recovery_not_in_current_bucket(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/resume", {"chunks": 0})

    accepted = ingest(client, "R3", events(("d", "2026-11-15T03:05:00Z", "pay", 200)))
    assert accepted["state"] == "DEFERRED"
    current = get(client, "/api/buckets/current").json()
    assert current["current_count"] == 2  # 旧版仍在，恢复中日志未入桶
    assert current["pending_count"] == 0

    post(client, f"/api/restores/{restore['restore_id']}/resume", {"chunks": 1000})
    post(client, f"/api/restores/{restore['restore_id']}/switch")
    current = get(client, "/api/buckets/current").json()
    assert current["current_count"] == 2
    assert current["pending"] == [{"hour": "2026-11-15T03:00:00Z", "service": "pay",
                                   "count": 1, "errors": 0}]


def test_stale_restore_rejected_and_current_kept(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    first = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{first['restore_id']}/switch")  # version 0 -> 1

    before = buckets(client)
    response = post(client, "/api/restores",
                    {"backup_id": point["backup_id"], "expected_version": 0})
    assert response.status_code == 409
    assert response.json()["error"] == "STALE_VERSION"
    assert buckets(client) == before

    second = start(client, point["backup_id"], chunks=1000)["restore"]
    assert second["base_version"] == 1
    stale = post(client, f"/api/restores/{second['restore_id']}/switch")
    # 版本未被别的恢复越过，这次切换应成功。
    assert stale.status_code == 200


def test_second_restore_switch_becomes_stale_after_first_wins(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    a = start(client, point["backup_id"], chunks=1000)["restore"]
    b = start(client, point["backup_id"], chunks=1000)["restore"]
    assert a["base_version"] == b["base_version"] == 0
    assert post(client, f"/api/restores/{a['restore_id']}/switch").status_code == 200
    before = buckets(client)
    response = post(client, f"/api/restores/{b['restore_id']}/switch")
    assert response.status_code == 409
    assert response.json()["error"] == "STALE_VERSION"
    assert buckets(client) == before
    assert get(client, f"/api/restores/{b['restore_id']}").json()["restore"]["status"] == "STALE_VERSION"


def test_interrupted_restore_resumes_from_checkpoint(tmp_path):
    path = str(tmp_path / "restore.db")
    store = Store(path)
    for index in range(5):
        store.ingest(f"R{index}", f"digest-{index}", {},
                     [{"log_id": f"l{index}", "request_id": f"l{index}",
                       "timestamp": "2026-11-15T01:05:00Z", "hour": "2026-11-15T01:00:00Z",
                       "service": "pay", "status": 200, "errors": 0,
                       "payload": "{}", "raw": "{}", "line_no": 1}], [])
    point = store.create_backup()
    restore = store.create_restore(point["backup_id"], chunk_size=2)
    store.process_restore(restore["restore_id"], chunks=1)
    partial = store.get_restore(restore["restore_id"])
    assert partial["processed"] == 2
    assert partial["checkpoint"] == 2

    # 模拟进程中断后重启：新实例从已恢复位置续跑，不重复累计。
    reopened = Store(path)
    resumed = reopened.process_restore(restore["restore_id"])
    assert resumed["processed"] == resumed["total"] == 5
    with reopened.connect() as db:
        staged = db.execute("SELECT SUM(count) FROM restore_stage WHERE restore_id=?",
                            (restore["restore_id"],)).fetchone()[0]
    assert staged == 5
    switched = reopened.switch_restore(restore["restore_id"])
    assert switched["status"] == "EFFECTIVE"
    assert switched["result"]["totals"]["count"] == 5


def test_switch_failure_keeps_old_version_and_can_retry(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    old = buckets(client)
    restore = start(client, point["backup_id"], chunks=1000)["restore"]

    response = post(client, f"/api/restores/{restore['restore_id']}/switch", {"fail": True})
    assert response.status_code == 422
    assert response.json()["error"] == "SWITCH_FAILED"
    assert buckets(client) == old
    assert get(client, f"/api/restores/{restore['restore_id']}").json()["restore"]["status"] \
        == "FAILED_SWITCH"

    retry = post(client, f"/api/restores/{restore['restore_id']}/switch").json()
    assert retry["restore"]["status"] == "EFFECTIVE"
    assert retry["restore"]["result"]["totals"]["count"] == 2


def test_switch_before_fully_staged_rejected(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    response = post(client, f"/api/restores/{restore['restore_id']}/switch")
    assert response.status_code == 422
    assert response.json()["error"] == "RESTORE_INCOMPLETE"


def test_readback_covers_recovering_effective_and_pending(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    rid = restore["restore_id"]
    post(client, f"/api/restores/{rid}/resume", {"chunks": 1})

    during = get(client, f"/api/restores/{rid}/readback").json()
    assert during["scope"] == ["RECOVERING", "EFFECTIVE", "PENDING"]
    assert during["recovering"]["state"] == "RECOVERING"
    assert during["recovering"]["processed"] == 1
    assert [row["processed"] for row in during["recovering"]["logs"]] == [True, False]
    assert during["effective"]["buckets"]
    assert during["pending"]["count"] == 1
    assert during["receipts"][1]["pending"] == 1

    post(client, f"/api/restores/{rid}/resume", {"chunks": 1000})
    post(client, f"/api/restores/{rid}/switch")
    after = get(client, f"/api/restores/{rid}/readback").json()
    assert after["recovering"]["state"] == "CLOSED"
    assert after["effective"]["buckets"] == [
        {"hour": "2026-11-15T01:00:00Z", "service": "pay", "count": 2, "errors": 1}]
    assert after["pending"]["count"] == 1


def test_adopt_pending_merges_isolated_logs(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/switch")

    adopted = post(client, "/api/pending/adopt").json()
    assert adopted["adopted_count"] == 1
    assert adopted["current_count"] == 3
    assert get(client, "/api/buckets/current").json()["pending"] == []


def test_adopt_rejected_during_recovery(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    start(client, point["backup_id"], chunk_size=1)
    response = post(client, "/api/pending/adopt")
    assert response.status_code == 422
    assert response.json()["error"] == "RESTORE_IN_PROGRESS"


def test_m1_import_feeds_actual_ingest_ledger(client):
    response = post(client, "/api/logs/import")
    data = response.json()
    assert data["valid_count"] == 2
    assert data["receipt_id"].startswith("rcpt-")
    assert get(client, "/api/buckets/current").json()["current_count"] == 2
    # 同一输入重跑复用原回执，不增加桶计数。
    again = post(client, "/api/logs/import").json()
    assert again["receipt_id"] == data["receipt_id"]
    assert again["replayed"] is True
    assert get(client, "/api/buckets/current").json()["current_count"] == 2


def test_restore_of_empty_backup_empties_current_bucket(client):
    point = backup(client)
    ingest(client, "R1", SAMPLE)
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/switch")
    current = get(client, "/api/buckets/current").json()
    assert current["buckets"] == []
    assert current["pending_count"] == 2


def test_abort_discards_stage_and_keeps_current(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    old = buckets(client)
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/resume", {"chunks": 1})
    aborted = post(client, f"/api/restores/{restore['restore_id']}/abort").json()
    assert aborted["restore"]["status"] == "ABORTED"
    assert buckets(client) == old
    response = post(client, f"/api/restores/{restore['restore_id']}/switch")
    assert response.status_code == 422
    assert response.json()["error"] == "RESTORE_ABORTED"


def test_deferred_logs_visible_and_reconciled_after_failed_switch(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    ingest(client, "R9", events(("e", "2026-11-15T05:05:00Z", "pay", 200)))
    assert get(client, "/api/buckets/current").json()["deferred_count"] == 1
    # 切换成功但恢复期间的日志变为待采纳。
    post(client, f"/api/restores/{restore['restore_id']}/switch")
    current = get(client, "/api/buckets/current").json()
    assert current["deferred_count"] == 0
    assert current["pending_count"] == 1


def test_pending_endpoint_during_recovery(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    ingest(client, "R9", events(("e", "2026-11-15T05:05:00Z", "pay", 200)))
    data = get(client, "/api/pending").json()
    assert data["pending_count"] == 0
    assert data["deferred_count"] == 1
    # 恢复进行中暂存区里也是空的，说明未越过恢复时点。
    assert get(client, f"/api/restores/{restore['restore_id']}/readback").json()[
        "pending"]["count"] == 1


def test_quarantine_reason_codes_stable():
    from app.modules.m7_restore.core import InvalidRecord, normalize_event
    with pytest.raises(InvalidRecord) as error:
        normalize_event({"service": "pay", "status": 200})
    assert error.value.reason == "EMPTY_VALUE"
    with pytest.raises(InvalidRecord) as error:
        normalize_event({"timestamp": "2026-01-01T00:00:00", "service": "pay"})
    assert error.value.reason == "TIMEZONE_REQUIRED"
    with pytest.raises(InvalidRecord) as error:
        normalize_event({"timestamp": "2026-03-08T02:30:00", "zone": "America/New_York",
                         "service": "pay"})
    assert error.value.reason == "DST_GAP"
    row = normalize_event({"request_id": "9007199254740993",
                           "timestamp": "2026-11-15T09:10:00+08:00",
                           "service": "pay", "status": 500})
    assert row["request_id"] == "9007199254740993"
    assert row["timestamp"] == "2026-11-15T01:10:00Z"
    assert row["hour"] == "2026-11-15T01:00:00Z"
    assert row["errors"] == 1


def test_m1_import_accepts_events_and_backs_up_them(client):
    response = post(client, "/api/logs/import", {"events": SAMPLE, "receipt_id": "R1"})
    assert response.status_code == 200
    data = response.json()
    assert data["valid_count"] == 2
    assert data["ingest_state"] == "CURRENT"
    point = backup(client)
    assert point["log_count"] == 2


def test_concurrent_restores_only_one_switch_wins(client):
    from concurrent.futures import ThreadPoolExecutor

    ingest(client, "R1", SAMPLE)
    point = backup(client)
    ids = [start(client, point["backup_id"], chunks=1000)["restore"]["restore_id"]
           for _ in range(2)]

    def attempt(restore_id):
        return post(client, f"/api/restores/{restore_id}/switch").status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = sorted(pool.map(attempt, ids))
    assert statuses == [200, 409]


def test_concurrent_resume_does_not_double_stage(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    path = str(tmp_path / "concurrent.db")
    store = Store(path)
    rows = [{"log_id": f"l{i}", "request_id": f"l{i}",
             "timestamp": "2026-11-15T01:05:00Z", "hour": "2026-11-15T01:00:00Z",
             "service": "pay", "status": 200, "errors": 0,
             "payload": "{}", "raw": "{}", "line_no": 1} for i in range(20)]
    store.ingest("R", "digest", {}, rows, [])
    point = store.create_backup()
    restore = store.create_restore(point["backup_id"], chunk_size=3)

    def work(_):
        return Store(path).process_restore(restore["restore_id"])["processed"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        pool.map(work, range(4))
    final = store.get_restore(restore["restore_id"])
    assert final["processed"] == final["total"] == 20
    with store.connect() as db:
        staged = db.execute("SELECT SUM(count) FROM restore_stage WHERE restore_id=?",
                            (restore["restore_id"],)).fetchone()[0]
    assert staged == 20


def test_restore_can_select_backup_by_point_in_time(client):
    ingest(client, "R1", SAMPLE)
    first = backup(client)
    ingest(client, "R2", events(("c", "2026-11-15T02:05:00Z", "pay", 200)))
    second = backup(client)
    assert first["backup_id"] != second["backup_id"]

    # 以第二份备份的创建时刻为“选定备份时点”，应解析到它自己。
    response = post(client, "/api/restores",
                    {"at": second["created_at"], "chunks": 1000})
    assert response.status_code == 200, response.text
    restore = response.json()["restore"]
    assert restore["backup_id"] == second["backup_id"]

    naive = post(client, "/api/restores", {"at": "2026-11-15T00:00:00"})
    assert naive.status_code == 422
    early = post(client, "/api/restores", {"at": "2000-01-01T00:00:00Z"})
    assert early.status_code == 404


def test_backup_is_repeatable_for_same_log_state(client):
    ingest(client, "R1", SAMPLE)
    first = backup(client)
    second = backup(client)
    assert first["backup_id"] == second["backup_id"]


def test_late_log_after_backup_is_pending_not_current(client):
    ingest(client, "R1", SAMPLE)
    point = backup(client)
    # 备份后追加一条更早小时但仍晚于备份时点写入的日志。
    ingest(client, "R2", events(("old", "2026-11-15T00:10:00Z", "pay", 200)))
    restore = start(client, point["backup_id"], chunks=1000)["restore"]
    post(client, f"/api/restores/{restore['restore_id']}/switch")
    current = get(client, "/api/buckets/current").json()
    assert [row["hour"] for row in current["buckets"]] == ["2026-11-15T01:00:00Z"]
    assert [row["hour"] for row in current["pending"]] == ["2026-11-15T00:00:00Z"]


def test_m1_import_during_recovery_defers_to_ledger(client):
    post(client, "/api/logs/import")
    point = backup(client)
    restore = start(client, point["backup_id"], chunk_size=1)["restore"]
    before = get(client, "/api/buckets/current").json()

    text = '{"request_id":"new","timestamp":"2026-11-15T05:05:00Z","service":"pay","status":200}\n'
    response = post(client, "/api/logs/import", {"text": text, "receipt_id": "RN"})
    assert response.status_code == 200
    assert response.json()["ingest_state"] == "DEFERRED"
    during = get(client, "/api/buckets/current").json()
    assert during["buckets"] == before["buckets"]
    assert during["deferred_count"] == 1

    post(client, f"/api/restores/{restore['restore_id']}/resume", {"chunks": 1000})
    post(client, f"/api/restores/{restore['restore_id']}/switch")
    after = get(client, "/api/buckets/current").json()
    assert after["deferred_count"] == 0
    assert after["pending_count"] == 1
    # 回执科的证据仍在，可回读原内容。
    receipt = get(client, "/api/intake/receipts/RN").json()
    assert receipt["payload"]["text"] == text
