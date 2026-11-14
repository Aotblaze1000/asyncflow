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
- **Request correlation**: every response carries an `X-Request-ID` header (client
  supplied or generated) that also appears in the log line for that request.
- **Structured startup**: container entrypoints wait for a database that is still
  starting instead of crash-looping on the first refused connection.

## Stack

| Component | Purpose |
| --- | --- |
| Python 3.12 | Runtime |
| FastAPI | HTTP API |
| Celery | Background workers |
| Redis | Broker and result backend |
| SQLAlchemy 2.0 | Persistence |
| SQLite | Default database for local runs |
| PostgreSQL | Database in the Compose stack |
| pytest | Test suite |

## Layout

```
app/
  main.py                 application factory, health endpoint
  bootstrap.py            schema + storage setup for containers
  api/uploads.py          upload endpoint
  api/jobs.py             job lookup, results, retry, recovery
  api/schemas.py          response models
  core/config.py          settings from environment
  core/logging.py         log format, request-id filter
  database/connection.py  engine, sessions, declarative base
  models/job.py           job model and states
  services/attachment.py  validation, storage, inspection
  services/jobs.py        state transitions
  workers/celery_app.py   Celery configuration and beat schedule
  workers/tasks.py        run_job() state machine + Celery tasks
tests/
  test_uploads.py         upload validation and storage
  test_jobs.py            API behaviour
  test_workers.py         claiming, processing, idempotency
  test_failures.py        retries, permanent failures, recovery
  test_concurrency.py     eight threads race for one job
  test_postgres.py        same guarantees against PostgreSQL
  test_observability.py   request ids, log format, health
  test_startup.py         startup retries when the database is late
docker-compose.yml        api + worker + beat + redis + postgres
```

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

`requirements.txt` holds runtime dependencies; `requirements-dev.txt` adds the
test tools.

Configuration is read from environment variables prefixed with `ASYNCFFLOW_`
(see `.env.example`).

## Running

### Docker Compose (recommended)

One command starts Redis, PostgreSQL, the API, a Celery worker and the beat
scheduler:

```powershell
docker compose up -d --build
docker compose ps
```

- API and docs: <http://127.0.0.1:8000/docs>
- Logs: `docker compose logs -f worker`
- Stop: `docker compose down` (add `-v` to drop the database volume)

### Locally, without Docker

Start Redis:

```powershell
docker run -d --name asyncflow-redis -p 6379:6379 redis:7-alpine
```

API:

```powershell
uvicorn app.main:app --reload
```

Interactive documentation: <http://127.0.0.1:8000/docs>

Worker (separate terminal):

```powershell
celery -A app.workers.celery_app worker --loglevel=info --pool=solo
```

Beat scheduler (separate terminal), which runs the stale-job sweep every 60s:

```powershell
celery -A app.workers.celery_app beat --loglevel=info
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
pytest tests/test_concurrency.py -v
```

The default suite uses a temporary SQLite file per test and a recording stub for
the broker, so it runs without Redis or Docker. Tests cover the
concurrency-relevant paths: exclusive claiming under eight threads, retry
exhaustion, late-writer rejection and stale-job recovery.

PostgreSQL integration checks are skipped unless a database URL is provided:

```powershell
docker compose up -d db
$env:ASYNCFFLOW_TEST_DATABASE_URL="postgresql+psycopg://asyncflow:asyncflow@localhost:5432/asyncflow"
pytest tests/test_postgres.py -v
```

That file recreates the `jobs` table, then repeats the eight-thread claim race
and a full process-and-persist cycle on PostgreSQL, proving the transitions do
not depend on SQLite behaviour.

## Observability

Every request gets a correlation id:

- a client may send `X-Request-ID`; otherwise the API generates a 16-character
  hex id;
- the id is always returned in the `X-Request-ID` response header, including on
  `404` and `422` responses;
- the access log line carries the same id, so a response can be matched to its
  log entry even under concurrent traffic.

```
2026-11-14 02:23:05,601 INFO [trace-final-99] app.main: POST /api/uploads -> 202 in 202ms
2026-11-14 02:23:05,685 INFO/ForkPoolWorker-2 Job 7524814d-... completed on attempt 1
```

Both lines carry the job id, so an upload can be followed from the API into the
worker. Uvicorn's own loggers are routed through the same handler instead of
keeping a second, differently formatted stream.

`GET /health` reports the application version, the environment and broker
reachability. Startup retries a refused database connection
(`ASYNCFFLOW_DATABASE_CONNECT_ATTEMPTS`, default 10, with a 1.5s delay), which
covers a database container that is healthy a moment before it accepts
connections.

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

**Crash recovery.** `acks_late` plus `task_reject_on_worker_lost` gets a message
redelivered when a worker dies. If the job was mid-`processing`, the beat
scheduler's sweep (every 60s) re-queues it once it has been stale for
`ASYNCFFLOW_JOB_STALE_AFTER_SECONDS`. A job that died on its final attempt is
closed as `failed` instead of being re-queued forever. The sweep also retries
jobs stuck in `queued`, which covers a broker outage during `delay()`.

**Duplicate deliveries are harmless.** Every path that can hand the same job to
two workers ends in `claim_job()`, and the loser gets `None`. Re-queueing a job
that is already being processed cannot corrupt it.

## Limitations

- Processing is intentionally format-light: text statistics for text-like files,
  checksum and size for binary/PDF. It is not a document converter.
- SQLite is the default for local runs and tests; the Compose stack uses
  PostgreSQL with the same code paths.
- Authentication is out of scope; put the API behind an authenticating gateway.

## License

MIT
