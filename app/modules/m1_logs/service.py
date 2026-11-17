from . import core


def execute(payload: dict | None = None, store=None) -> dict:
    """运行 M1 采信；有存储时把有效日志写入实际入库台账。

    - ``text``：沿用 M1 的 JSONL 解析与行号证据。
    - ``events``：结构化日志，等价于 M7 的接收接口，便于直接复核对账库。
    """
    data = payload or core.SAMPLE
    if store is None:
        return core.run(data)

    from app.modules.m7_restore import service as restore_service
    if "events" in data and data["events"] is not None:
        receipt = restore_service.ingest(store, {
            "events": data["events"],
            "receipt_id": data.get("receipt_id"),
        })
        first = data["events"][0] if data["events"] else {}
        reason = receipt["quarantine"][0]["reason"] if receipt["quarantine"] else "OK"
        return {"status": "READY",
                "input_count": receipt["input_count"],
                "valid_count": receipt["valid_count"],
                "quarantine_count": receipt["quarantine_count"],
                "first_id": str(first.get("request_id", "")) if isinstance(first, dict) else "",
                "reason": reason,
                "receipt_id": receipt["receipt_id"],
                "ingested_count": receipt["valid_count"],
                "ingest_state": receipt["state"],
                "replayed": receipt["replayed"]}

    result = core.run(data)
    receipt = restore_service.ingest(store, {
        "text": data.get("text", core.SAMPLE["text"]),
        "receipt_id": data.get("receipt_id"),
    })
    result = dict(result)
    result["receipt_id"] = receipt["receipt_id"]
    result["ingested_count"] = receipt["valid_count"]
    result["ingest_state"] = receipt["state"]
    result["replayed"] = receipt["replayed"]
    return result
