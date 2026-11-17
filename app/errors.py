from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

#: 领域错误的默认 HTTP 状态；可用 DomainError(status=...) 覆盖。
DEFAULT_STATUS = 422


class DomainError(ValueError):
    def __init__(self, code: str, message: str, status: int | None = None):
        self.code, self.message = code, message
        self.status = status or DEFAULT_STATUS
        super().__init__(message)


class StaleVersionError(DomainError):
    def __init__(self, message: str = "恢复所基于的版本已过期"):
        super().__init__("STALE_VERSION", message, status=409)


class ReceiptConflictError(DomainError):
    def __init__(self, message: str = "同一回执编号提交了不同内容"):
        super().__init__("RECEIPT_CONFLICT", message, status=409)


class NotFoundError(DomainError):
    def __init__(self, message: str = "目标不存在"):
        super().__init__("NOT_FOUND", message, status=404)


def install(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        return JSONResponse(status_code=exc.status,
                            content={"error": exc.code, "message": exc.message})
