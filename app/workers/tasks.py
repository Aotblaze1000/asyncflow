"""Background execution of processing jobs.

The work is split in two layers on purpose:

* :func:`run_job` contains the whole state machine and is plain Python, so it
  can be tested without a broker or a Celery worker.
* :func:`process_job` is the thin Celery task that turns a "retry" outcome into
  an actual scheduled retry.

Ownership is claimed in the database before any file is touched, so two
workers that receive the same job id cannot both process it.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.connection import SessionLocal
from app.models.job import Job
from app.services import jobs as job_service
from app.services.attachment import CorruptAttachmentError, process_attachment
from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]


class JobOutcome(str, Enum):
    COMPLETED = "completed"
    SKIPPED = "skipped"
    RETRY = "retry"
    FAILED = "failed"


@dataclass(frozen=True)
class JobRunResult:
    outcome: JobOutcome
    attempt: int = 0
    error: str | None = None


#: Errors that a later attempt may resolve on its own (a half-written file, a
#: transient IO problem). Anything else is treated as a logic error.
RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    CorruptAttachmentError,
    OSError,
    TimeoutError,
)


def _retry_countdown(attempt: int) -> int:
    """Exponential backoff, capped so a broken job cannot stall a queue."""

    return min(settings.job_retry_backoff_seconds * (2 ** max(attempt - 1, 0)), 60)


def _handle_retryable_failure(
    db: Session, job: Job, attempt: int, exc: BaseException
) -> JobRunResult:
    message = f"Attempt {attempt}/{job.max_retries} failed: {type(exc).__name__}: {exc}"

    if attempt >= job.max_retries:
        job_service.mark_failed(db, job.id, attempt, message)
        logger.warning("Job %s failed permanently: %s", job.id, message)
        return JobRunResult(JobOutcome.FAILED, attempt, message)

    job_service.mark_retrying(db, job.id, attempt, message)
    logger.info("Job %s will be retried: %s", job.id, message)
    return JobRunResult(JobOutcome.RETRY, attempt, message)


def run_job(
    job_id: str,
    session_factory: SessionFactory = SessionLocal,
) -> JobRunResult:
    """Execute one attempt of ``job_id`` and record the resulting state."""

    db = session_factory()
    try:
        job = job_service.claim_job(db, job_id)
        if job is None:
            logger.info("Job %s was not claimable; leaving it untouched", job_id)
            return JobRunResult(JobOutcome.SKIPPED)

        attempt = job.attempts

        try:
            result = process_attachment(Path(job.stored_path), job.content_type)
        except RETRYABLE_ERRORS as exc:
            return _handle_retryable_failure(db, job, attempt, exc)
        except Exception as exc:  # noqa: BLE001 - a job must never vanish
            message = (
                f"Unexpected {type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc(limit=5)}"
            )
            job_service.mark_failed(db, job.id, attempt, message)
            logger.exception("Job %s raised an unexpected error", job.id)
            return JobRunResult(JobOutcome.FAILED, attempt, message)

        if not job_service.mark_completed(db, job_id, attempt, result.to_dict()):
            # Another writer moved the job on (for example the stale sweeper).
            # Its result wins; nothing is overwritten.
            logger.warning("Job %s finished but its claim was already released", job_id)
            return JobRunResult(JobOutcome.SKIPPED, attempt)

        logger.info("Job %s completed on attempt %s", job_id, attempt)
        return JobRunResult(JobOutcome.COMPLETED, attempt)

    finally:
        db.close()


@celery_app.task(bind=True, name="asyncflow.process_job")
def process_job(self, job_id: str) -> str:  # noqa: ANN001
    """Celery entry point: run one attempt, scheduling a retry when needed."""

    result = run_job(job_id)

    if result.outcome is JobOutcome.RETRY:
        raise self.retry(countdown=_retry_countdown(result.attempt), exc=None)

    return result.outcome.value


@celery_app.task(name="asyncflow.recover_stale_jobs")
def recover_stale_jobs() -> int:
    """Periodic safety net for jobs whose worker disappeared."""

    db = SessionLocal()
    try:
        recovered = job_service.reset_stale_jobs(db)
    finally:
        db.close()

    for job_id in recovered:
        if job_service.enqueue(job_id):
            job_service.mark_queued(db, job_id)
    return len(recovered)
