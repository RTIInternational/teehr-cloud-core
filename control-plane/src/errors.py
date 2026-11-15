"""Errors that carry a machine-readable ``code`` (see ``ErrorOut``)."""

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


class APIError(HTTPException):
    def __init__(self, status_code: int, detail: str, code: str | None = None):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


async def api_error_handler(_request: Request, exc: APIError) -> JSONResponse:
    return JSONResponse(
        {"detail": exc.detail, "code": exc.code},
        status_code=exc.status_code,
        headers=exc.headers,
    )
