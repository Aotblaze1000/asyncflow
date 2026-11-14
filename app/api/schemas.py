"""Pydantic schemas describing the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JobStatusLiteral = Literal[
    "pending", "queued", "processing", "retrying", "completed", "failed"
]


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    original_filename: str
    content_type: str
    size_bytes: int
    checksum_sha256: str | None
    status: JobStatusLiteral
    attempts: int
    max_retries: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: JobStatusLiteral
    attempts: int
    result: dict[str, Any] | None = None
    error: str | None = None


class JobList(BaseModel):
    items: list[JobRead]
    total: int
    limit: int
    offset: int


class UploadAccepted(BaseModel):
    job: JobRead
    queued: bool
