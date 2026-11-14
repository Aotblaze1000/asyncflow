"""Job inspection and recovery endpoints."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.api.schemas import JobList, JobRead, JobResult, JobStatusLiteral
from app.database.connection import get_db
from app.models.job import Job, JobStatus, utcnow
from app.services import jobs as job_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _get_or_404(db: Session, job_id: str) -> Job:
    job = job_service.get_job(db, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Job {job_id} not found")
    return job


@router.get("", response_model=JobList, summary="List jobs")
def list_jobs(
    status_filter: JobStatusLiteral | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> JobList:
    items, total = job_service.list_jobs(
        db, status=status_filter, limit=limit, offset=offset
    )
    return JobList(
        items=[JobRead.model_validate(job) for job in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{job_id}", response_model=JobRead, summary="Get one job")
def get_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
    return JobRead.model_validate(_get_or_404(db, job_id))


@router.get("/{job_id}/result", response_model=JobResult, summary="Get a job result")
def get_job_result(job_id: str, db: Session = Depends(get_db)) -> JobResult:
    job = _get_or_404(db, job_id)

    payload: dict[str, Any] | None = None
    if job.result_json:
        try:
            payload = json.loads(job.result_json)
        except json.JSONDecodeError:  # pragma: no cover - defensive
            payload = {"raw": job.result_json}

    return JobResult(
        id=job.id,
        status=job.status,
        attempts=job.attempts,
        result=payload,
        error=job.last_error,
    )


@router.post(
    "/{job_id}/retry",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Re-queue a failed job",
)
def retry_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
    job = _get_or_404(db, job_id)

    if job.status != JobStatus.FAILED.value:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Only failed jobs can be retried (job is {job.status})",
        )

    result = db.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.FAILED.value)
        .values(
            status=JobStatus.QUEUED.value,
            attempts=0,
            last_error=None,
            result_json=None,
            started_at=None,
            finished_at=None,
            updated_at=utcnow(),
        )
    )
    db.commit()
    if result.rowcount != 1:
        raise HTTPException(status.HTTP_409_CONFLICT, "Job changed while retrying")

    db.expire_all()
    job = _get_or_404(db, job_id)

    if not job_service.enqueue(job_id):
        db.execute(
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.QUEUED.value)
            .values(status=JobStatus.PENDING.value, updated_at=utcnow())
        )
        db.commit()
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "The job queue is unavailable; the job stays pending",
        )

    db.refresh(job)
    return JobRead.model_validate(job)


@router.post(
    "/recovery/stale",
    summary="Re-queue jobs abandoned by a stopped worker",
)
def recover_stale_jobs(
    older_than_seconds: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> dict[str, object]:
    recovered = job_service.reset_stale_jobs(
        db, older_than_seconds or None
    )
    queued = [job_id for job_id in recovered if job_service.enqueue(job_id)]
    return {"recovered": recovered, "queued": queued}
