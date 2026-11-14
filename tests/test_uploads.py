"""Upload endpoint behaviour: validation, storage and job creation."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.core.config import settings
from tests.conftest import sha256_of


def test_upload_creates_job_and_stores_file(
    client: TestClient, storage: Path, queued_jobs: list[str]
) -> None:
    payload = b"alpha beta\ngamma delta\n"

    response = client.post(
        "/api/uploads",
        files={"file": ("notes.txt", payload, "text/plain")},
    )

    assert response.status_code == 202, response.text
    body = response.json()
    job = body["job"]

    assert body["queued"] is True
    assert job["status"] == "queued"
    assert job["original_filename"] == "notes.txt"
    assert job["size_bytes"] == len(payload)
    assert job["checksum_sha256"] == sha256_of(payload)
    assert job["attempts"] == 0
    assert queued_jobs == [job["id"]]

    stored = list(storage.iterdir())
    assert len(stored) == 1
    assert stored[0].read_bytes() == payload


def test_upload_rejects_unsupported_extension(
    client: TestClient, storage: Path, queued_jobs: list[str]
) -> None:
    response = client.post(
        "/api/uploads",
        files={"file": ("payload.exe", b"MZ", "application/octet-stream")},
    )

    assert response.status_code == 415
    assert "Unsupported file extension" in response.json()["detail"]
    assert list(storage.iterdir()) == []
    assert queued_jobs == []


def test_upload_rejects_empty_file(
    client: TestClient, storage: Path, queued_jobs: list[str]
) -> None:
    response = client.post(
        "/api/uploads",
        files={"file": ("empty.txt", b"", "text/plain")},
    )

    assert response.status_code == 400
    assert list(storage.iterdir()) == []
    assert queued_jobs == []


def test_upload_rejects_file_over_limit(
    client: TestClient,
    storage: Path,
    queued_jobs: list[str],
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "max_upload_bytes", 32)

    response = client.post(
        "/api/uploads",
        files={"file": ("big.txt", b"x" * 200, "text/plain")},
    )

    assert response.status_code == 413
    # The rejected upload leaves nothing behind, not even a partial file.
    assert list(storage.iterdir()) == []
    assert queued_jobs == []


def test_upload_sanitises_filename(
    client: TestClient, storage: Path, queued_jobs: list[str]
) -> None:
    response = client.post(
        "/api/uploads",
        files={"file": ("..\\..\\secret.txt", b"data", "text/plain")},
    )

    assert response.status_code == 202
    assert response.json()["job"]["original_filename"] == "secret.txt"
    assert all(path.parent == storage for path in storage.iterdir())


def test_upload_survives_unavailable_broker(
    client: TestClient, monkeypatch, queued_jobs: list[str]
) -> None:
    monkeypatch.setattr("app.services.jobs.enqueue", lambda _job_id: False)

    response = client.post(
        "/api/uploads",
        files={"file": ("notes.txt", b"hello", "text/plain")},
    )

    assert response.status_code == 202
    assert response.json()["queued"] is False
    assert response.json()["job"]["status"] == "pending"
