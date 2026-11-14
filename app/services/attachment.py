"""Attachment inspection and processing.

This module owns the "real work" of a job: validating an upload before it is
stored, writing it to disk safely, and extracting a result once a worker picks
the job up.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO

from app.core.config import settings


class AttachmentError(Exception):
    """Base class for attachment problems."""


class UnsupportedAttachmentError(AttachmentError):
    """The file type is not accepted."""


class EmptyAttachmentError(AttachmentError):
    """The upload contained no data."""


class AttachmentTooLargeError(AttachmentError):
    """The upload exceeded the configured size limit."""


class CorruptAttachmentError(AttachmentError):
    """Stored data cannot be interpreted as the declared type (retryable)."""


TEXT_EXTENSIONS = frozenset({".txt", ".md", ".csv", ".log", ".json"})


@dataclass(frozen=True)
class StoredUpload:
    filename: str
    path: Path
    size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class ProcessingResult:
    kind: str
    size_bytes: int
    checksum_sha256: str
    line_count: int | None = None
    word_count: int | None = None
    character_count: int | None = None
    preview: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {k: v for k, v in asdict(self).items() if v is not None}


def _safe_filename(filename: str) -> str:
    """Reduce a client-supplied name to a harmless basename."""

    name = os.path.basename(filename or "").strip()
    # Strip control characters and path separators that survived basename.
    name = "".join(ch for ch in name if ch.isprintable() and ch not in '\\/:*?"<>|')
    if not name or name in {".", ".."}:
        raise UnsupportedAttachmentError("Uploaded file has no usable name")
    return name[:255]


def _extension(filename: str) -> str:
    return Path(filename).suffix.lower()


def validate_metadata(filename: str, content_type: str | None) -> tuple[str, str]:
    """Validate a declared upload and return ``(filename, content_type)``.

    Validation happens before a single byte is written to disk so that a bad
    request never consumes storage.
    """

    safe_name = _safe_filename(filename)
    if _extension(safe_name) not in settings.allowed_extensions:
        allowed = ", ".join(settings.allowed_extensions)
        raise UnsupportedAttachmentError(
            f"Unsupported file extension {_extension(safe_name) or '(none)'!r}; allowed: {allowed}"
        )

    media_type = (content_type or "").split(";")[0].strip().lower()
    if not media_type:
        media_type = "application/octet-stream"
    return safe_name, media_type


def store_upload(stream: BinaryIO, safe_name: str) -> StoredUpload:
    """Stream an upload to a temporary file, enforcing the size limit.

    The data is written to a temporary file first and only moved into place
    once the checksum is known, so a failed write never leaves a half written
    attachment that a worker could pick up.
    """

    storage = settings.ensure_storage_dir()
    target = storage / f"{uuid.uuid4().hex}{_extension(safe_name)}"
    digest = hashlib.sha256()
    total = 0

    fd, temp_name = tempfile.mkstemp(dir=storage, prefix=".upload-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as temp_file:
            while True:
                chunk = stream.read(1024 * 256)
                if not chunk:
                    break
                total += len(chunk)
                if total > settings.max_upload_bytes:
                    raise AttachmentTooLargeError(
                        f"Upload exceeds the {settings.max_upload_bytes} byte limit"
                    )
                digest.update(chunk)
                temp_file.write(chunk)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        if total == 0:
            raise EmptyAttachmentError("Uploaded file is empty")

        os.replace(temp_name, target)
    except BaseException:
        # Any failure path removes the partial file; the caller never has to
        # remember cleanup, which is what prevents temporary-file leaks.
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

    return StoredUpload(
        filename=safe_name,
        path=target,
        size_bytes=total,
        checksum_sha256=digest.hexdigest(),
    )


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptAttachmentError(f"{path.name} is not valid UTF-8 text") from exc


def process_attachment(path: Path, content_type: str) -> ProcessingResult:
    """Inspect a stored attachment and produce a JSON-serialisable result.

    Reading is done inside a context manager so the handle is released even
    when inspection fails part way through.
    """

    if not path.is_file():
        raise CorruptAttachmentError(f"Attachment {path.name} is missing from storage")

    with path.open("rb") as handle:
        digest = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: handle.read(1024 * 256), b""):
            digest.update(chunk)
            size += len(chunk)

    if path.suffix.lower() in TEXT_EXTENSIONS:
        text = _read_text(path)
        stripped = text.strip()
        return ProcessingResult(
            kind="text",
            size_bytes=size,
            checksum_sha256=digest.hexdigest(),
            line_count=len(text.splitlines()),
            word_count=len(stripped.split()),
            character_count=len(text),
            preview=stripped[:280],
        )

    kind = "document" if path.suffix.lower() == ".pdf" else "binary"
    return ProcessingResult(
        kind=kind,
        size_bytes=size,
        checksum_sha256=digest.hexdigest(),
    )
