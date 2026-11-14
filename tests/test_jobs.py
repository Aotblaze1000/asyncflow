"""Job listing, lookup, result and manual retry endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.job import Job, JobStatus


def _make_job(db: Session, storage: Path, name: str = "a.txt", status: str = "queued") -> Job:
    path = storage / f"{name}.bin"
    path.write_bytes(b"content")
    job = Job(
        original_filename=name,
        stored_path=str(path),
        content_type="text/plain",
        size_bytes=len(b"content"),
        status=status,
        max_retries=3,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def test_list_jobs_filters_and_paginates(
    client: TestClient, db_session: Session, storage: Path
) -> None:
    for index in range(3):
        _make_job(db_session, storage, f"done-{index}.txt", JobStatus.COMPLETED.value)
    _make_job(db_session, storage, "pending.txt", JobStatus.PENDING.value)

    response = client.get("/api/jobs")

    assert response.status_code == 200
    assert response.json()["total"] == 4
    assert len(response.json()["items"]) == 4

    filtered = client.get("/api/jobs", params={"status": "completed", "limit": 2})
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 3
    assert len(filtered.json()["items"]) == 2
    assert filtered.json()["limit"] == 2


def test_list_jobs_rejects_unknown_status(client: TestClient) -> None:
    response = client.get("/api/jobs", params={"status": "exploded"})
    assert response.status_code == 422


def test_get_job_and_missing_job(
    client: TestClient, db_session: Session, storage: Path
) -> None:
    job = _make_job(db_session, storage)

    found = client.get(f"/api/jobs/{job.id}")
    assert found.status_code == 200
    assert found.json()["id"] == job.id

    missing = client.get("/api/jobs/does-not-exist")
    assert missing.status_code == 404


def test_get_result_for_completed_job(
    client: TestClient, db_session: Session, storage: Path
) -> None:
    job = _make_job(db_session, storage, status=JobStatus.COMPLETED.value)
    job.result_json = '{"kind": "text", "word_count": 2}'
    db_session.commit()

    response = client.get(f"/api/jobs/{job.id}/result")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"] == {"kind": "text", "word_count": 2}
    assert body["error"] is None


def test_retry_only_failed_jobs(
    client: TestClient, db_session: Session, storage: Path, queued_jobs: list[str]
) -> None:
    completed = _make_job(db_session, storage, "done.txt", JobStatus.COMPLETED.value)

    conflict = client.post(f"/api/jobs/{completed.id}/retry")
    assert conflict.status_code == 409
    assert queued_jobs == []


def test_retry_requeues_failed_job(
    client: TestClient, db_session: Session, storage: Path, queued_jobs: list[str]
) -> None:
    job = _make_job(db_session, storage, "broken.txt", JobStatus.FAILED.value)
    job.attempts = 3
    job.last_error = "boom"
    db_session.commit()

    response = client.post(f"/api/jobs/{job.id}/retry")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["last_error"] is None
    assert queued_jobs == [job.id]


def test_retry_reports_broker_outage(
    client: TestClient, db_session: Session, storage: Path, monkeypatch
) -> None:
    job = _make_job(db_session, storage, "broken.txt", JobStatus.FAILED.value)
    monkeypatch.setattr("app.services.jobs.enqueue", lambda _job_id: False)

    response = client.post(f"/api/jobs/{job.id}/retry")

    assert response.status_code == 503
    assert client.get(f"/api/jobs/{job.id}").json()["status"] == "pending"


def test_health_endpoint(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr("app.main._redis_reachable", lambda: False)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["broker"] == "unavailable"
