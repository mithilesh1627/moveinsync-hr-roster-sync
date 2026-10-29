"""Global Exception Handling & Error Envelope Module

Ensures uniform, non-leaking JSON error responses across all endpoints.
Strictly redacts sensitive PII from exception details.
"""

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.core.logging import logger, sanitize_value


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle standard FastAPI/Starlette HTTP exceptions."""
    sanitized_detail = sanitize_value("detail", exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": f"HTTP_{exc.status_code}",
                "message": sanitized_detail,
                "path": str(request.url.path),
            }
        },
        headers=exc.headers,
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle boundary schema validation errors without leaking PII."""
    sanitized_errors = []
    for err in exc.errors():
        loc = ".".join(str(item) for item in err.get("loc", []))
        msg = err.get("msg", "Validation error")
        sanitized_errors.append({
            "field": loc,
            "message": sanitize_value("message", msg),
        })

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid request payload at API boundary",
                "path": str(request.url.path),
                "details": sanitized_errors,
            }
        },
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected internal server errors safely without leaking stack traces to clients."""
    logger.exception(f"Unhandled system exception processing {request.method} {request.url.path}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred. Incident has been recorded.",
                "path": str(request.url.path),
            }
        },
    )
