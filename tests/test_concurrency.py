"""Concurrency: the database, not luck, decides who owns a job."""

from __future__ import annotations

import threading
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from app.models.job import Job, JobStatus
from app.services import jobs as job_service


def _claim_many(
    factory: sessionmaker, job_id: str, workers: int
) -> list[bool]:
    results: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(workers)

    def attempt() -> None:
        session = factory()
        try:
            barrier.wait(timeout=10)
            claimed = job_service.claim_job(session, job_id) is not None
        finally:
            session.close()
        with lock:
            results.append(claimed)

    threads = [threading.Thread(target=attempt) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    return results


def _make_job(db: Session, storage: Path) -> Job:
    path = storage / "race.txt"
    path.write_bytes(b"race")
    job = Job(
        original_filename="race.txt",
        stored_path=str(path),
        content_type="text/plain",
        size_bytes=4,
        status=JobStatus.QUEUED.value,
        max_retries=3,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def test_only_one_of_eight_workers_claims_a_job(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    job = _make_job(db_session, storage)

    results = _claim_many(session_factory, job.id, workers=8)

    assert len(results) == 8
    assert results.count(True) == 1, f"expected exactly one winner, got {results}"

    db_session.expire_all()
    stored = db_session.get(Job, job.id)
    assert stored is not None
    assert stored.attempts == 1
    assert stored.status == JobStatus.PROCESSING.value


def test_repeated_races_never_produce_two_winners(
    db_session: Session, session_factory: sessionmaker, storage: Path
) -> None:
    for round_number in range(5):
        job = _make_job(db_session, storage)
        path = storage / "race.txt"
        job.stored_path = str(path)
        db_session.commit()

        results = _claim_many(session_factory, job.id, workers=4)

        assert results.count(True) == 1, (
            f"round {round_number} produced {results.count(True)} winners"
        )
