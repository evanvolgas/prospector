"""API exception handlers."""

import logging
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from models import ErrorResponse

logger = logging.getLogger(__name__)


async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with consistent error format."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            detail=str(exc)
        ).model_dump(mode='json')
    )


async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions with error logging."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc)
        ).model_dump(mode='json')
    )