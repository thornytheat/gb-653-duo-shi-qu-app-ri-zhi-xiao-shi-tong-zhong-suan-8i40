from fastapi import APIRouter, Body, Request
from . import service

router = APIRouter(tags=["M7-备份恢复"])


@router.get("/restore")
def page(request: Request):
    store = request.app.state.store
    restores = store.list_restores(limit=20)
    latest = restores[-1]["restore_id"] if restores else None
    view = store.readback(latest) if latest else None
    data = {
        "status": "READY",
        "version": store.version(),
        "backups": store.list_backups(limit=20),
        "restores": restores,
        "latest": view,
        **store.readback_current(),
    }
    return request.app.state.templates.TemplateResponse(
        request, "m7_restore.html", {"title": "备份恢复", "data": data})


@router.post("/api/intake")
def intake(request: Request, payload: dict = Body(default={})):
    """接收实际入库日志并返回回执；恢复期间暂不入当前桶。"""
    return service.ingest(request.app.state.store, payload)


@router.get("/api/intake/receipts")
def receipts(request: Request):
    return service.list_receipts(request.app.state.store)


@router.get("/api/intake/receipts/{receipt_id}")
def receipt(request: Request, receipt_id: str):
    """原回执可回读；备份恢复不会触碰回执表。"""
    return service.get_receipt(request.app.state.store, receipt_id)


@router.post("/api/backups")
def create_backup(request: Request, payload: dict = Body(default={})):
    """从实际入库日志生成备份时点。"""
    return service.create_backup(request.app.state.store, payload)


@router.get("/api/backups")
def list_backups(request: Request):
    return service.list_backups(request.app.state.store)


@router.get("/api/backups/{backup_id}")
def get_backup(request: Request, backup_id: str):
    return service.get_backup(request.app.state.store, backup_id)


@router.post("/api/restores")
def start_restore(request: Request, payload: dict = Body(default={})):
    return service.start_restore(request.app.state.store, payload)


@router.get("/api/restores")
def list_restores(request: Request):
    return {"restores": request.app.state.store.list_restores()}


@router.get("/api/restores/{restore_id}")
def get_restore(request: Request, restore_id: str):
    from app.errors import NotFoundError
    restore = request.app.state.store.get_restore(restore_id)
    if not restore:
        raise NotFoundError(f"恢复 {restore_id} 不存在")
    return {"status": "READY", "restore": restore}


@router.post("/api/restores/{restore_id}/resume")
def resume(request: Request, restore_id: str, payload: dict = Body(default={})):
    """中断后从已恢复位置续跑。"""
    return service.resume_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restores/{restore_id}/switch")
def switch(request: Request, restore_id: str, payload: dict = Body(default={})):
    """切换暂存桶为当前桶；失败保留旧版。"""
    return service.switch_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restores/{restore_id}/abort")
def abort(request: Request, restore_id: str, payload: dict = Body(default={})):
    """放弃未生效的恢复，当前桶保持旧版。"""
    return service.abort_restore(request.app.state.store, restore_id, payload)


@router.get("/api/restores/{restore_id}/readback")
def readback(request: Request, restore_id: str, limit: int = 500):
    """回读恢复中、已生效与待采纳范围。"""
    return service.readback(request.app.state.store, restore_id, {"limit": limit})


@router.get("/api/buckets/current")
def current(request: Request):
    return service.current(request.app.state.store)


@router.get("/api/buckets")
def current_alias(request: Request):
    return service.current(request.app.state.store)


@router.post("/api/pending/adopt")
def adopt(request: Request, payload: dict = Body(default={})):
    """把较新、待采纳日志并入当前桶。"""
    return service.adopt_pending(request.app.state.store, payload)


@router.get("/api/pending")
def pending(request: Request):
    """只读查看待采纳日志（恢复期间也可见）。"""
    data = service.current(request.app.state.store)
    return {"status": "READY", "pending": data["pending"],
            "pending_count": data["pending_count"],
            "deferred_count": data["deferred_count"]}


# ---- 兼容别名：备份 / 恢复的常见叫法 ----

@router.post("/api/backup")
def alias_backup(request: Request, payload: dict = Body(default={})):
    return service.create_backup(request.app.state.store, payload)


@router.post("/api/logs/backup")
def alias_logs_backup(request: Request, payload: dict = Body(default={})):
    return service.create_backup(request.app.state.store, payload)


@router.post("/api/backups/{backup_id}/restore")
def alias_backup_restore(request: Request, backup_id: str, payload: dict = Body(default={})):
    body = dict(payload or {})
    body.setdefault("backup_id", backup_id)
    return service.start_restore(request.app.state.store, body)


@router.post("/api/restore")
def alias_restore(request: Request, payload: dict = Body(default={})):
    return service.start_restore(request.app.state.store, payload)


@router.post("/api/logs/restore")
def alias_logs_restore(request: Request, payload: dict = Body(default={})):
    return service.start_restore(request.app.state.store, payload)


@router.get("/api/restores/{restore_id}/status")
def restore_status(request: Request, restore_id: str):
    return get_restore(request, restore_id)


@router.post("/api/restore/{restore_id}/continue")
def alias_continue(request: Request, restore_id: str, payload: dict = Body(default={})):
    return service.resume_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restore/{restore_id}/commit")
def alias_commit(request: Request, restore_id: str, payload: dict = Body(default={})):
    return service.switch_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restore/{restore_id}/apply")
def alias_apply(request: Request, restore_id: str, payload: dict = Body(default={})):
    return service.switch_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restore/{restore_id}/resume")
def alias_resume(request: Request, restore_id: str, payload: dict = Body(default={})):
    return service.resume_restore(request.app.state.store, restore_id, payload)


@router.post("/api/restore/{restore_id}/switch")
def alias_switch(request: Request, restore_id: str, payload: dict = Body(default={})):
    return service.switch_restore(request.app.state.store, restore_id, payload)


@router.get("/api/restore/{restore_id}/readback")
def alias_readback(request: Request, restore_id: str, limit: int = 500):
    return service.readback(request.app.state.store, restore_id, {"limit": limit})
