"""Shared test fixtures.

Each test gets its own SQLite database file and storage directory, so tests are
independent and never touch the development database.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings
from app.database.connection import Base, get_db
from app.main import app


@pytest.fixture()
def session_factory(tmp_path: Path) -> Iterator[sessionmaker]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'test.db'}",
        connect_args={"check_same_thread": False},
        future=True,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture()
def db_session(session_factory: sessionmaker) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "storage"
    directory.mkdir()
    monkeypatch.setattr(settings, "storage_dir", directory)
    return directory


@pytest.fixture()
def queued_jobs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the broker with an in-memory recorder."""

    recorded: list[str] = []

    def _fake_enqueue(job_id: str) -> bool:
        recorded.append(job_id)
        return True

    monkeypatch.setattr("app.services.jobs.enqueue", _fake_enqueue)
    return recorded


@pytest.fixture()
def client(
    session_factory: sessionmaker,
    storage: Path,
    queued_jobs: list[str],
) -> Iterator[TestClient]:
    def _override_get_db() -> Iterator[Session]:
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _override_get_db
    # The lifespan is intentionally not started: it would create tables on the
    # development engine. Every request uses the overridden test session.
    test_client = TestClient(app)
    try:
        yield test_client
    finally:
        app.dependency_overrides.clear()


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
