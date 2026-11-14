"""Worker behaviour: claiming, processing and result persistence."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from app.models.job import Job, JobStatus
from app.services import jobs as job_service
from app.workers.tasks import JobOutcome, run_job


def make_job(
    db: Session,
    storage: Path,
    name: str = "notes.txt",
    content: bytes = b"one two three\nfour five\n",
    status: str = JobStatus.QUEUED.value,
) -> Job:
    path = storage / name
    path.write_bytes(content)
    job = Job(
        original_filename=name,
        stored_path=str(path),
        content_type="text/plain",
        size_bytes=len(content),
        status=status,
        max_retries=3,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def test_worker_processes_text_attachment(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.COMPLETED
    assert result.attempt == 1

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.COMPLETED.value
    assert stored.attempts == 1
    assert stored.finished_at is not None
    assert stored.last_error is None

    payload = json.loads(stored.result_json or "{}")
    assert payload["kind"] == "text"
    assert payload["word_count"] == 5
    assert payload["line_count"] == 2
    assert payload["preview"].startswith("one two three")


def test_second_worker_skips_already_completed_job(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)

    assert run_job(job.id, session_factory).outcome is JobOutcome.COMPLETED
    second = run_job(job.id, session_factory)

    assert second.outcome is JobOutcome.SKIPPED

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.attempts == 1
    assert stored.status == JobStatus.COMPLETED.value


def test_claim_is_exclusive(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)

    first = job_service.claim_job(db_session, job.id)
    second = job_service.claim_job(db_session, job.id)

    assert first is not None
    assert first.status == JobStatus.PROCESSING.value
    assert second is None

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.attempts == 1


def test_binary_attachment_result(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(
        db_session,
        storage,
        name="image.png",
        content=b"\x89PNG\r\n\x1a\n" + b"0" * 64,
    )

    outcome = run_job(job.id, session_factory)

    assert outcome.outcome is JobOutcome.COMPLETED
    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    payload = json.loads(stored.result_json or "{}")
    assert payload["kind"] == "binary"
    assert payload["size_bytes"] == len(b"\x89PNG\r\n\x1a\n" + b"0" * 64)
    assert "word_count" not in payload


def test_result_is_not_rewritten_by_a_late_writer(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)
    assert run_job(job.id, session_factory).outcome is JobOutcome.COMPLETED

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    original = stored.result_json

    # A retried delivery that still believes it holds attempt 1 must not be
    # able to publish a second result.
    assert job_service.mark_completed(db_session, job.id, attempt=1, payload={"kind": "dup"}) is False

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.result_json == original
