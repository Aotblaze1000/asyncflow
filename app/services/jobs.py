"""Job lifecycle operations.

Every state transition is written with a conditional ``UPDATE`` that includes
the expected current status (and attempt number for terminal writes). A worker
can therefore never overwrite a job that another worker already took over, and
a late retry cannot publish a duplicate result.
"""

from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.job import CLAIMABLE_STATUSES, Job, JobStatus, utcnow
from app.services.attachment import StoredUpload


def create_job(db: Session, upload: StoredUpload, content_type: str) -> Job:
    """Persist a new job in ``pending`` state and return it."""

    job = Job(
        original_filename=upload.filename,
        stored_path=str(upload.path),
        content_type=content_type,
        size_bytes=upload.size_bytes,
        checksum_sha256=upload.checksum_sha256,
        status=JobStatus.PENDING.value,
        max_retries=settings.job_max_retries,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_job(db: Session, job_id: str) -> Job | None:
    return db.get(Job, job_id)


def list_jobs(
    db: Session,
    *,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[Job], int]:
    """Return a page of jobs, newest first, plus the total count."""

    filters = []
    if status:
        filters.append(Job.status == status)

    total = db.scalar(select(func.count()).select_from(Job).where(*filters)) or 0
    stmt = (
        select(Job)
        .where(*filters)
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(db.scalars(stmt)), total


def mark_queued(db: Session, job_id: str) -> bool:
    result = db.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PENDING.value)
        .values(status=JobStatus.QUEUED.value, updated_at=utcnow())
    )
    db.commit()
    return result.rowcount == 1


def claim_job(db: Session, job_id: str) -> Job | None:
    """Atomically take ownership of a claimable job.

    Returns the refreshed job when this caller won the race, or ``None`` when
    the job is missing, already terminal, already claimed by another worker, or
    out of attempts.
    """

    claimable = tuple(status.value for status in CLAIMABLE_STATUSES)
    result = db.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_(claimable),
            Job.attempts < Job.max_retries,
        )
        .values(
            status=JobStatus.PROCESSING.value,
            attempts=Job.attempts + 1,
            started_at=utcnow(),
            updated_at=utcnow(),
            last_error=None,
        )
    )
    db.commit()

    if result.rowcount != 1:
        return None

    db.expire_all()
    return db.get(Job, job_id)


def _guarded_update(db: Session, job_id: str, attempt: int, **values) -> bool:
    """Update a job only while ``attempt`` still owns it."""

    values.setdefault("updated_at", utcnow())
    result = db.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.PROCESSING.value,
            Job.attempts == attempt,
        )
        .values(**values)
    )
    db.commit()
    return result.rowcount == 1


def mark_completed(
    db: Session, job_id: str, attempt: int, payload: dict[str, object]
) -> bool:
    return _guarded_update(
        db,
        job_id,
        attempt,
        status=JobStatus.COMPLETED.value,
        result_json=json.dumps(payload, sort_keys=True),
        last_error=None,
        finished_at=utcnow(),
    )


def mark_retrying(
    db: Session, job_id: str, attempt: int, error: str
) -> bool:
    """Release the claim and record why the attempt failed.

    The job becomes claimable again (``retrying``) before the retry is
    scheduled, so a crash between these two steps leaves a job that the stale
    sweeper can recover instead of an invisible one.
    """

    return _guarded_update(
        db,
        job_id,
        attempt,
        status=JobStatus.RETRYING.value,
        last_error=error[:2000],
    )


def mark_failed(db: Session, job_id: str, attempt: int, error: str) -> bool:
    return _guarded_update(
        db,
        job_id,
        attempt,
        status=JobStatus.FAILED.value,
        last_error=error[:2000],
        finished_at=utcnow(),
    )


def enqueue(job_id: str) -> bool:
    """Hand a job to Celery. Returns ``False`` when the broker is unreachable.

    Import is local so that the API (and the tests) can use the job service
    without a running broker.
    """

    from app.workers.tasks import process_job

    try:
        process_job.delay(job_id)
    except Exception:  # noqa: BLE001 - broker errors must not break the request
        return False
    return True


def reset_stale_jobs(db: Session, older_than_seconds: int | None = None) -> list[str]:
    """Recover jobs that no worker is going to finish.

    Two situations are handled:

    * a job left in ``processing`` because its worker died (exhausted jobs are
      closed as failed instead of being re-queued forever);
    * a job left in ``queued``/``retrying`` because the broker call never
      reached a worker, for example during a short Redis outage.

    Returns the ids that were made claimable again.
    """

    timeout = older_than_seconds or settings.job_stale_after_seconds
    cutoff = utcnow() - timedelta(seconds=timeout)

    stale = list(
        db.scalars(
            select(Job).where(
                or_(
                    and_(
                        Job.status == JobStatus.PROCESSING.value,
                        Job.started_at.is_not(None),
                        Job.started_at < cutoff,
                    ),
                    and_(
                        Job.status.in_(
                            [JobStatus.QUEUED.value, JobStatus.RETRYING.value]
                        ),
                        Job.updated_at < cutoff,
                    ),
                )
            )
        )
    )

    requeued: list[str] = []
    for job in stale:
        if (
            job.status == JobStatus.PROCESSING.value
            and job.attempts >= job.max_retries
        ):
            db.execute(
                update(Job)
                .where(Job.id == job.id, Job.status == JobStatus.PROCESSING.value)
                .values(
                    status=JobStatus.FAILED.value,
                    last_error=(
                        f"Abandoned after {job.attempts} attempt(s): the worker "
                        "stopped before the job finished"
                    ),
                    finished_at=utcnow(),
                    updated_at=utcnow(),
                )
            )
            continue

        result = db.execute(
            update(Job)
            .where(Job.id == job.id, Job.status == job.status)
            .values(
                status=JobStatus.QUEUED.value,
                last_error="Recovered by the stale-job sweep; re-queued",
                updated_at=utcnow(),
            )
        )
        if result.rowcount == 1:
            requeued.append(job.id)

    db.commit()
    return requeued
