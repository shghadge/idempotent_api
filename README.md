# Idempotent Job Processing API

A small FastAPI service for submitting background jobs with an `Idempotency-Key`.

The project is meant to show the usual pieces behind reliable job intake:

- duplicate `POST /jobs` requests return the same job
- the same key with a different payload is rejected
- a worker leases queued jobs from Postgres
- failed jobs retry with exponential backoff
- jobs that run out of attempts move to a dead-letter state
- dead jobs can be replayed

It uses Postgres as the source of truth. The idempotency guarantee comes from a unique constraint on `jobs.idempotency_key`, not from an in-memory cache.

## Run it with Docker

From this directory:

```bash
docker compose up --build
```

This starts three containers:

- `api` on `http://localhost:8000`
- `worker` for background job execution
- `postgres` on port `5432`

The API creates its table on startup. The worker also checks that the table exists before it starts polling.

## Submit a job

```bash
curl -i -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"payload":{"task":"echo","value":"hello"}}'
```

The first request returns `201 Created`.

Send the same request again with the same key and payload:

```bash
curl -i -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"payload":{"task":"echo","value":"hello"}}'
```

That returns `200 OK` with the original job.

If the key is reused with a different payload, the API returns `409 Conflict`.

## Check jobs

List all jobs:

```bash
curl http://localhost:8000/jobs
```

Filter by status:

```bash
curl 'http://localhost:8000/jobs?status=succeeded'
```

Get one job:

```bash
curl http://localhost:8000/jobs/<job-id>
```

List dead-lettered jobs:

```bash
curl http://localhost:8000/jobs/dead
```

Replay a dead job:

```bash
curl -X POST http://localhost:8000/jobs/<job-id>/replay
```

## Demo worker payloads

The worker has a tiny built-in demo task runner:

```json
{"task":"echo","value":"hello"}
```

returns:

```json
{"echo":"hello"}
```

```json
{"task":"fail","error":"boom"}
```

fails, retries, and eventually moves to `dead` after the job reaches its max attempts.

Any other payload is accepted and marked as processed.

## Dead jobs and retries

A failed job does not go straight to `dead`.

New jobs get `max_attempts` set to `5`. Each time the worker runs a failing job, it adds one attempt. If the job still has attempts left, it goes back to `queued` and waits before trying again.

With the current backoff, the wait times are about:

- 2 seconds after the first failure
- 4 seconds after the second failure
- 8 seconds after the third failure
- 16 seconds after the fourth failure

On the fifth failed run, the job moves to `dead`.

Dead jobs stay in the database so they can be inspected or replayed later.

## Local tests

Install dependencies with `uv`, then run:

```bash
uv run --extra dev pytest
```

The Postgres integration tests are skipped unless `POSTGRES_TEST_DATABASE_URL` is set.

Example:

```bash
POSTGRES_TEST_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/idempotent_api \
  uv run --extra dev pytest tests/test_postgres_integration.py
```

## Important note about idempotency

This API deduplicates job submission. It does not promise that the external side effect performed by a worker can happen exactly once.

The worker is designed around at-least-once execution:

- one worker leases a job with a database row lock
- stale running jobs are returned to the queue
- failed jobs are retried later
- exhausted jobs are kept in the dead-letter queue

For real side effects, the worker action should also be idempotent or protected by its own unique key.
