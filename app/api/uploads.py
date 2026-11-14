"""Upload endpoint: validate, store, create a job, hand it to the broker."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.api.schemas import UploadAccepted
from app.database.connection import get_db
from app.services import jobs as job_service
from app.services.attachment import (
    AttachmentTooLargeError,
    EmptyAttachmentError,
    UnsupportedAttachmentError,
    store_upload,
    validate_metadata,
)

router = APIRouter(tags=["uploads"])


@router.post(
    "/uploads",
    response_model=UploadAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload an attachment and create a processing job",
)
def create_upload(
    file: UploadFile = File(..., description="Attachment to process"),
    db: Session = Depends(get_db),
) -> UploadAccepted:
    """Store an upload and queue a background job for it.

    The endpoint is synchronous on purpose: Starlette runs it in a worker
    thread, so streamed reads and ``fsync`` do not block the event loop.
    """

    try:
        safe_name, media_type = validate_metadata(file.filename or "", file.content_type)
    except UnsupportedAttachmentError as exc:
        raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, str(exc)) from exc

    try:
        stored = store_upload(file.file, safe_name)
    except EmptyAttachmentError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except AttachmentTooLargeError as exc:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, str(exc)
        ) from exc
    finally:
        # The spooled upload is always released, including on the error paths.
        file.file.close()

    job = job_service.create_job(db, stored, media_type)

    if job_service.enqueue(job.id):
        job_service.mark_queued(db, job.id)
        db.refresh(job)
        queued = True
    else:
        # The broker is down. The job stays ``pending`` so it is not lost, and
        # the caller can retry or trigger the recovery sweep later.
        queued = False

    return UploadAccepted(job=job, queued=queued)
