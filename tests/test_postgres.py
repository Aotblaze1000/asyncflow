"""PostgreSQL integration checks.

These run only when a PostgreSQL URL is provided, so the default suite stays
fast and dependency-free:

    $env:ASYNCFFLOW_TEST_DATABASE_URL="postgresql+psycopg://asyncflow:asyncflow@localhost:5432/asyncflow"
    pytest tests/test_postgres.py -v

The point is to prove the claiming and state transitions are not SQLite
specific: the same conditional UPDATE must arbitrate correctly on Postgres.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.database.connection import Base
from app.models.job import Job, JobStatus
from app.services import jobs as job_service
from app.workers.tasks import JobOutcome, run_job

DATABASE_URL = os.environ.get("ASYNCFFLOW_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not DATABASE_URL, reason="ASYNCFFLOW_TEST_DATABASE_URL is not set"
    ),
]


@pytest.fixture()
def pg_factory():
    engine = create_engine(DATABASE_URL, future=True)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS jobs"))
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )
    try:
        yield factory
    finally:
        engine.dispose()


def _job(factory, storage: Path) -> Job:
    path = storage / "pg.txt"
    path.write_bytes(b"postgres\nline two\n")
    session = factory()
    try:
        job = Job(
            original_filename="pg.txt",
            stored_path=str(path),
            content_type="text/plain",
            size_bytes=18,
            status=JobStatus.QUEUED.value,
            max_retries=3,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        return job
    finally:
        session.close()


def test_postgres_claim_race_has_one_winner(pg_factory, storage: Path) -> None:
    job = _job(pg_factory, storage)
    barrier = threading.Barrier(8)
    results: list[bool] = []
    lock = threading.Lock()

    def attempt() -> None:
        session = pg_factory()
        try:
            barrier.wait(timeout=10)
            claimed = job_service.claim_job(session, job.id) is not None
        finally:
            session.close()
        with lock:
            results.append(claimed)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert results.count(True) == 1


def test_postgres_processing_and_result(pg_factory, storage: Path) -> None:
    job = _job(pg_factory, storage)

    outcome = run_job(job.id, pg_factory)

    assert outcome.outcome is JobOutcome.COMPLETED

    session = pg_factory()
    try:
        stored = session.get(Job, job.id)
        assert stored is not None
        assert stored.status == JobStatus.COMPLETED.value
        assert stored.attempts == 1
        assert '"word_count": 3' in (stored.result_json or "")
    finally:
        session.close()
