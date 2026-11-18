"""Errors as RFC 9457 problem details (``application/problem+json``).

Every error response is ``{type, title, status, detail}``, plus ``code`` where a
client can act on it (``token_refresh_required``) and ``errors`` for request
validation (each problem's location, message and type). Same shape as VeriRace's
API, so clients of either handle errors the same way.
"""

from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

PROBLEM_JSON = "application/problem+json"

# RFC 9110 names, pinned: older Pythons still use the RFC 7231 phrases for these,
# which would make titles depend on the Python version.
_TITLES = {
    413: "Content Too Large",
    414: "URI Too Long",
    416: "Range Not Satisfiable",
    422: "Unprocessable Content",
}


class APIError(HTTPException):
    """An HTTP error with a machine-readable ``code``."""

    def __init__(self, status_code: int, detail: str, code: str | None = None):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


def problem(
    status: int,
    detail: str | None = None,
    *,
    headers: dict[str, str] | None = None,
    **extra: Any,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": "about:blank",
        "title": _TITLES.get(status) or HTTPStatus(status).phrase,
        "status": status,
    }
    if detail is not None:
        body["detail"] = detail
    body.update({k: v for k, v in extra.items() if v is not None})
    return JSONResponse(
        body, status_code=status, headers=headers, media_type=PROBLEM_JSON
    )


async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return problem(
        exc.status_code,
        detail,
        headers=getattr(exc, "headers", None),
        code=getattr(exc, "code", None),
    )


async def _validation_error(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    errors = [
        {"loc": [str(p) for p in e["loc"]], "msg": e["msg"], "type": e["type"]}
        for e in exc.errors()
    ]
    first = errors[0] if errors else None
    detail = f"{'.'.join(first['loc'])}: {first['msg']}" if first else "Invalid request"
    return problem(422, detail, errors=errors)


def install(app: FastAPI) -> None:
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(RequestValidationError, _validation_error)


def document(schema: dict[str, Any]) -> dict[str, Any]:
    """Document every error response as ``application/problem+json``.

    Routes declare their errors with the ``ErrorOut`` model; 422s point at
    ``ValidationErrorOut`` instead of FastAPI's default validation schema.
    """
    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            for code, response in operation.get("responses", {}).items():
                if not code.startswith(("4", "5")):
                    continue
                content = response.get("content", {})
                if code == "422":
                    ref = "#/components/schemas/ValidationErrorOut"
                else:
                    ref = (
                        content.get("application/json", {})
                        .get("schema", {})
                        .get("$ref", "#/components/schemas/ErrorOut")
                    )
                response["content"] = {PROBLEM_JSON: {"schema": {"$ref": ref}}}
    schemas = schema.setdefault("components", {}).setdefault("schemas", {})
    for unused in ("HTTPValidationError", "ValidationError"):
        schemas.pop(unused, None)
    return schema
