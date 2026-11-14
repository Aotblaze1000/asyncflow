"""FastAPI application factory."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import jobs, uploads
from app.core.config import settings
from app.database.connection import init_db

logger = logging.getLogger(__name__)


def _redis_reachable() -> bool:
    """Best-effort broker check used by the health endpoint."""

    try:
        import redis

        client = redis.Redis.from_url(
            settings.redis_url, socket_connect_timeout=0.5, socket_timeout=0.5
        )
        try:
            return bool(client.ping())
        finally:
            client.close()
    except Exception:  # noqa: BLE001 - health must never raise
        return False


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings.ensure_storage_dir()
    init_db()
    logger.info("AsyncFlow started (environment=%s)", settings.environment)
    yield
    logger.info("AsyncFlow stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description=(
            "Asynchronous file and attachment processing. Uploads create a job, "
            "a worker processes the attachment, and the job records the outcome."
        ),
        lifespan=lifespan,
    )

    app.include_router(uploads.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")

    @app.get("/health", tags=["health"])
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "environment": settings.environment,
            "broker": "up" if _redis_reachable() else "unavailable",
        }

    return app


app = create_app()
