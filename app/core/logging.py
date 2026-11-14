"""Logging configuration and request correlation.

Every request gets an id that appears in its log lines and in the
``X-Request-ID`` response header, so an upload can be traced from the API log
into the worker log for the same job.
"""

from __future__ import annotations

import contextvars
import logging
import sys

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


class RequestIdFilter(logging.Filter):
    """Attach the current request id to every record."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.request_id = request_id_var.get()
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """Configure a single stdout handler with a stable, greppable format."""

    root = logging.getLogger()
    if getattr(root, "_asyncflow_configured", False):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s [%(request_id)s] %(name)s: %(message)s"
        )
    )
    handler.addFilter(RequestIdFilter())

    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root._asyncflow_configured = True  # type: ignore[attr-defined]

    # Uvicorn installs its own handlers; route them through ours instead.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
