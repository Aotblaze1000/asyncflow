"""FastAPI application factory."""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

from app import __version__
from app.api import jobs, uploads
from app.core.config import settings
from app.core.logging import configure_logging, request_id_var
from app.database.connection import init_db_with_retry

logger = logging.getLogger(__name__)

MAX_REQUEST_ID_LENGTH = 64


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
    configure_logging()
    settings.ensure_storage_dir()
    init_db_with_retry()
    logger.info("AsyncFlow started (environment=%s)", settings.environment)
    yield
    logger.info("AsyncFlow stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=__version__,
        description=(
            "Asynchronous file and attachment processing. Uploads create a job, "
            "a worker processes the attachment, and the job records the outcome."
        ),
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next) -> Response:  # noqa: ANN001
        incoming = request.headers.get("x-request-id", "")
        request_id = (
            incoming[:MAX_REQUEST_ID_LENGTH]
            if incoming and incoming.isprintable()
            else uuid.uuid4().hex[:16]
        )
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - started) * 1000
            response.headers["X-Request-ID"] = request_id
            # Logged before the context is reset, so the line can be traced to
            # the request that produced it.
            logger.info(
                "%s %s -> %s in %.0fms",
                request.method,
                request.url.path,
                response.status_code,
                elapsed_ms,
            )
            return response
        except Exception:
            logger.exception(
                "%s %s failed after %.0fms",
                request.method,
                request.url.path,
                (time.perf_counter() - started) * 1000,
            )
            raise
        finally:
            request_id_var.reset(token)

    app.include_router(uploads.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")

    @app.get("/health", tags=["health"])
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "version": __version__,
            "environment": settings.environment,
            "broker": "up" if _redis_reachable() else "unavailable",
        }

    return app


app = create_app()
