"""Database engine, session factory and declarative base."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Declarative base shared by every ORM model."""


def _build_engine(url: str) -> Engine:
    connect_args: dict[str, object] = {}
    if url.startswith("sqlite"):
        # FastAPI serves requests from a thread pool and Celery workers use
        # their own threads, so connections must be usable across threads.
        connect_args["check_same_thread"] = False

    engine = create_engine(url, connect_args=connect_args, future=True)

    if url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, _record) -> None:  # noqa: ANN001
            cursor = dbapi_connection.cursor()
            try:
                # WAL lets readers proceed while a worker writes, and the
                # busy timeout avoids spurious "database is locked" errors
                # when an API request and a worker touch the same file.
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=5000")
                cursor.execute("PRAGMA foreign_keys=ON")
            finally:
                cursor.close()

    return engine


engine = _build_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def get_db() -> Iterator[Session]:
    """FastAPI dependency yielding a request-scoped session."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional scope for workers and scripts."""

    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    """Create tables for the configured database."""

    from app import models  # noqa: F401  (register mappers)

    Base.metadata.create_all(bind=engine)


def init_db_with_retry(
    attempts: int | None = None, delay_seconds: float | None = None
) -> None:
    """Create tables, tolerating a database that is still coming up.

    A container can be healthy a moment before it accepts connections, and a
    managed database can briefly refuse traffic during a failover. Exiting on
    the first refused connection turns a few seconds of unavailability into a
    crash loop, so the startup path retries with a bounded budget instead.
    """

    remaining = attempts or settings.database_connect_attempts
    delay = (
        settings.database_connect_delay_seconds if delay_seconds is None else delay_seconds
    )

    for attempt in range(1, remaining + 1):
        try:
            init_db()
            return
        except OperationalError:
            if attempt == remaining:
                logger.error(
                    "Database still unreachable after %s attempt(s); giving up", attempt
                )
                raise
            logger.warning(
                "Database not ready (attempt %s/%s); retrying in %.1fs",
                attempt,
                remaining,
                delay,
            )
            time.sleep(delay)
