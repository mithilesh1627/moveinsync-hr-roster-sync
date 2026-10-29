"""MoveInSync HR Roster Sync - Main FastAPI Application Entrypoint

Configures application lifecycle, routes, background outbox workers,
global exception handlers, and OpenAPI documentation.
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.logging import logger
from app.core.error_handlers import (
    http_exception_handler,
    validation_exception_handler,
    generic_exception_handler,
)
from app.api.v1.router import api_v1_router
from app.api.v1.health import router as health_router
from app.api.v1.metrics import router as metrics_router
from app.workers.outbox_publisher import OutboxPublisher


# Global publisher instance
outbox_publisher = OutboxPublisher(
    batch_size=settings.outbox_batch_size,
    max_retries=settings.outbox_max_retries,
    base_backoff_seconds=settings.outbox_base_backoff_seconds,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager: Starts background workers on startup and stops on shutdown."""
    publisher_task = None
    if settings.app_env != "testing":
        logger.info("Starting MoveInSync HR Roster Sync service background worker...")
        publisher_task = asyncio.create_task(
            outbox_publisher.start(poll_interval=settings.outbox_poll_interval_seconds)
        )

    yield

    # Graceful shutdown
    if publisher_task:
        logger.info("Stopping MoveInSync HR Roster Sync service background worker...")
        outbox_publisher.stop()
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass
        logger.info("Background workers stopped successfully.")


app = FastAPI(
    title="MoveInSync HR Roster Sync API",
    description=(
        "Production-grade, multi-tenant employee roster synchronization backend. "
        "Engineered for idempotent, out-of-order, self-healing event ingestion, "
        "streaming 200k reconciliation diffs with a 30% safety brake, and transactional outbox publishing."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception Handlers
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, generic_exception_handler)

# Top-level Health & Observability routes
app.include_router(health_router)
app.include_router(metrics_router)

# Versioned API Router
app.include_router(api_v1_router)
