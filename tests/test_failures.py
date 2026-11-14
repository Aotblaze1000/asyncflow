"""Failure handling: retries, permanent failures, abandoned workers."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy.orm import Session, sessionmaker

from app.models.job import Job, JobStatus, utcnow
from app.services import jobs as job_service
from app.workers.tasks import JobOutcome, run_job
from tests.test_workers import make_job


def test_missing_attachment_is_retried(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)
    Path(job.stored_path).unlink()

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.RETRY
    assert result.attempt == 1

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.RETRYING.value
    assert stored.last_error is not None
    assert "CorruptAttachmentError" in stored.last_error
    assert stored.attempts == 1


def test_retries_are_exhausted_into_a_permanent_failure(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)
    job.max_retries = 1
    db_session.commit()
    Path(job.stored_path).unlink()

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.FAILED

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED.value
    assert stored.finished_at is not None
    assert "Attempt 1/1 failed" in (stored.last_error or "")


def test_unexpected_exception_marks_job_failed(
    db_session: Session,
    session_factory: sessionmaker,
    storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = make_job(db_session, storage)

    def _explode(*_args, **_kwargs):
        raise ValueError("processor bug")

    monkeypatch.setattr("app.workers.tasks.process_attachment", _explode)

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.FAILED

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED.value
    assert "ValueError: processor bug" in (stored.last_error or "")
    assert stored.finished_at is not None


def test_soft_time_limit_is_retryable(
    db_session: Session,
    session_factory: sessionmaker,
    storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow attempt must not permanently fail a job that had started."""

    from celery.exceptions import SoftTimeLimitExceeded

    job = make_job(db_session, storage)

    def _too_slow(*_args, **_kwargs):
        raise SoftTimeLimitExceeded("300s exceeded")

    monkeypatch.setattr("app.workers.tasks.process_attachment", _too_slow)

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.RETRY

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.RETRYING.value
    assert "SoftTimeLimitExceeded" in (stored.last_error or "")


def test_a_failed_job_is_never_claimable_again(    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = make_job(db_session, storage)
    job.status = JobStatus.FAILED.value
    job.attempts = 3
    db_session.commit()

    result = run_job(job.id, session_factory)

    assert result.outcome is JobOutcome.SKIPPED


def test_stale_processing_job_is_requeued(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.PROCESSING.value)
    job.attempts = 1
    job.started_at = utcnow() - timedelta(seconds=600)
    db_session.commit()

    recovered = job_service.reset_stale_jobs(db_session, older_than_seconds=300)

    assert recovered == [job.id]

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED.value
    assert "re-queued" in (stored.last_error or "")


def test_abandoned_job_with_no_attempts_left_is_failed(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.PROCESSING.value)
    job.attempts = 3
    job.max_retries = 3
    job.started_at = utcnow() - timedelta(seconds=600)
    db_session.commit()

    recovered = job_service.reset_stale_jobs(db_session, older_than_seconds=300)

    # Re-queueing it would only produce a job that can never be claimed.
    assert recovered == []

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED.value
    assert "Abandoned after 3 attempt(s)" in (stored.last_error or "")
    assert stored.finished_at is not None


def test_queued_job_that_never_reached_a_worker_is_requeued(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.QUEUED.value)
    job.updated_at = utcnow() - timedelta(seconds=900)
    db_session.commit()

    recovered = job_service.reset_stale_jobs(db_session, older_than_seconds=300)

    assert recovered == [job.id]

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.QUEUED.value
    assert "stale-job sweep" in (stored.last_error or "")


def test_a_freshly_queued_job_is_not_disturbed(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.QUEUED.value)

    recovered = job_service.reset_stale_jobs(db_session, older_than_seconds=300)

    assert recovered == []
    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.last_error is None



def test_recent_processing_job_is_left_alone(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.PROCESSING.value)
    job.started_at = utcnow()
    db_session.commit()

    recovered = job_service.reset_stale_jobs(db_session, older_than_seconds=300)

    assert recovered == []


def test_guarded_write_rejects_a_lost_claim(
    db_session: Session, storage: Path
) -> None:
    job = make_job(db_session, storage, status=JobStatus.PROCESSING.value)
    job.attempts = 2
    db_session.commit()

    assert job_service.mark_failed(db_session, job.id, attempt=1, error="late") is False

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.status == JobStatus.PROCESSING.value
    assert stored.last_error is None
