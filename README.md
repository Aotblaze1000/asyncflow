# AsyncFlow

AsyncFlow is a small but complete backend service for asynchronous file and
attachment processing. A client uploads a file, the API validates and stores it,
a Celery worker processes it in the background, and the job record records every
state change.

It is deliberately built around the failure modes that matter in a queue-based
system: duplicate delivery, worker crashes, retries, unreclaimable jobs and
resource leaks.

## How it works

```
POST /api/uploads
        |
        v
  validate request  --415/413/400--> reject, nothing written
        |
        v
  stream to storage (temporary file -> atomic rename, SHA-256)
        |
        v
  jobs row (pending) --enqueue--> queue (queued)
        |
        v
  Celery worker: claim_job()  [atomic UPDATE ... WHERE status IN claimable]
        |
        v
  process_attachment()  -> completed(result_json)
        |
        +-- retryable error -> retrying + scheduled retry (bounded)
        +-- unexpected error -> failed(error, traceback)
```

## Features

- **Uploads** with extension allow-list, streaming size limit and empty-file checks.
- **Job lifecycle** with explicit `pending` / `queued` / `processing` / `retrying` /
  `completed` / `failed` states.
- **Atomic claiming** so two workers that receive the same message cannot both
  process the same job.
- **Bounded retries** with exponential backoff for transient failures; permanent
  failures store the error instead of disappearing.
- **Idempotent results**: a late or duplicated delivery cannot overwrite a job
  that has already finished.
- **Crash recovery**: `acks_late` + `task_reject_on_worker_lost`, plus a stale-job
  sweep for jobs left in `processing` by a dead worker.
- **Resource safety**: uploads stream through a temporary file that is removed on
  every failure path, and processing files are always closed.

## Stack

| Component | Purpose |
| --- | --- |
| Python 3.12 | Runtime |
| FastAPI | HTTP API |
| Celery | Background workers |
| Redis | Broker and result backend |
| SQLAlchemy 2.0 | Persistence |
| SQLite | Default database |
| pytest | Test suite |

## Layout

```
app/
  main.py                 application factory, health endpoint
  api/uploads.py          upload endpoint
  api/jobs.py             job lookup, results, retry, recovery
  api/schemas.py          response models
  core/config.py          settings from environment
  database/connection.py  engine, sessions, declarative base
  models/job.py           job model and states
  services/attachment.py  validation, storage, inspection
  services/jobs.py        state transitions
  workers/celery_app.py   Celery configuration
  workers/tasks.py        run_job() state machine + Celery task
tests/
  test_uploads.py         upload validation and storage
  test_jobs.py            API behaviour
  test_workers.py         claiming, processing, idempotency
  test_failures.py        retries, permanent failures, recovery
```

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start Redis (Docker Desktop must be running):

```powershell
docker run -d --name asyncflow-redis -p 6379:6379 redis:7-alpine
```

Configuration is read from environment variables prefixed with `ASYNCFFLOW_`
(see `.env.example`).

## Running

API:

```powershell
uvicorn app.main:app --reload
```

Interactive documentation: <http://127.0.0.1:8000/docs>

Worker (separate terminal):

```powershell
celery -A app.workers.celery_app worker --loglevel=info --pool=solo
```

The `solo` pool is convenient on Windows; production deployments normally use
`prefork` with an explicit concurrency.

## API

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/api/uploads` | Upload a file; returns `202` with the created job |
| `GET` | `/api/jobs` | List jobs (`status`, `limit`, `offset`) |
| `GET` | `/api/jobs/{id}` | Fetch a job |
| `GET` | `/api/jobs/{id}/result` | Fetch the processing result or failure |
| `POST` | `/api/jobs/{id}/retry` | Re-queue a failed job |
| `POST` | `/api/jobs/recovery/stale` | Re-queue jobs abandoned by a dead worker |
| `GET` | `/health` | Liveness plus broker reachability |

Example:

```powershell
curl.exe -F "file=@README.md;type=text/plain" http://127.0.0.1:8000/api/uploads
curl.exe http://127.0.0.1:8000/api/jobs/<id>/result
```

## Tests

```powershell
pytest -v
pytest tests/test_workers.py -v
pytest tests/test_failures.py -v
```

The suite uses a temporary SQLite file per test and a recording stub for the
broker, so it runs without Redis. Tests cover the concurrency-relevant paths:
exclusive claiming, retry exhaustion, late-writer rejection and stale-job
recovery.

## Design notes

**Claiming.** `claim_job()` performs a single `UPDATE ... WHERE id = ? AND
status IN (...) AND attempts < max_retries` and checks `rowcount`. The database
is the arbiter, so no lock table or distributed lock is required.

**Retry state.** A retryable failure marks the job `retrying` *before* the retry
is scheduled. If the process dies between those two steps, the job is still
claimable and the stale sweep can pick it up.

**Terminal writes are guarded.** `mark_completed()` / `mark_failed()` include
`status = 'processing' AND attempts = <claimed attempt>` in the `WHERE` clause.
A delivery that lost its claim updates nothing.

**Broker outages.** An upload whose enqueue fails returns `202` with
`queued: false` and the job stays `pending`; the record is never dropped.

## Limitations

- Processing is intentionally format-light: text statistics for text-like files,
  checksum and size for binary/PDF. It is not a document converter.
- SQLite is the default; the schema uses portable SQLAlchemy types so PostgreSQL
  works by changing `ASYNCFFLOW_DATABASE_URL`.
- Authentication is out of scope; put the API behind an authenticating gateway.

## License

MIT
