from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import SessionLocal, create_schema
from .jobs import Job, JobStatus

BACKOFF_BASE_SECONDS = 2
STALE_LOCK_AFTER = timedelta(minutes=5)


class JobExecutionError(Exception):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def backoff_delay(attempts: int) -> timedelta:
    return timedelta(seconds=BACKOFF_BASE_SECONDS * (2 ** max(attempts - 1, 0)))


def lease_next_job(
    session: Session, worker_id: str, now: datetime | None = None
) -> Job | None:
    lease_time = now or utc_now()
    job = session.scalar(
        select(Job)
        .where(Job.status == JobStatus.queued.value, Job.next_run_at <= lease_time)
        .order_by(Job.created_at)
        .with_for_update(skip_locked=True)
    )
    if job is None:
        return None

    job.status = JobStatus.running.value
    job.locked_at = lease_time
    job.locked_by = worker_id
    session.commit()
    session.refresh(job)
    return job


def execute_job(job: Job) -> dict[str, Any]:
    task = job.payload.get("task")
    if task == "fail":
        error = job.payload.get("error", "job failed")
        raise JobExecutionError(str(error))
    if task == "echo":
        return {"echo": job.payload.get("value")}
    return {"processed": True, "payload": job.payload}


def finish_job(
    session: Session, job: Job, result: dict[str, Any], now: datetime | None = None
) -> Job:
    finished_at = now or utc_now()
    job.status = JobStatus.succeeded.value
    job.attempts += 1
    job.result = result
    job.error = None
    job.locked_at = None
    job.locked_by = None
    job.updated_at = finished_at
    session.commit()
    session.refresh(job)
    return job


def fail_job(
    session: Session, job: Job, error: str, now: datetime | None = None
) -> Job:
    failed_at = now or utc_now()
    job.attempts += 1
    job.result = None
    job.error = error
    job.locked_at = None
    job.locked_by = None

    if job.attempts >= job.max_attempts:
        job.status = JobStatus.dead.value
    else:
        job.status = JobStatus.queued.value
        job.next_run_at = failed_at + backoff_delay(job.attempts)

    job.updated_at = failed_at
    session.commit()
    session.refresh(job)
    return job


def run_job_once(session: Session, worker_id: str) -> Job | None:
    job = lease_next_job(session, worker_id)
    if job is None:
        return None

    try:
        result = execute_job(job)
    except JobExecutionError as exc:
        return fail_job(session, job, str(exc))

    return finish_job(session, job, result)


def recover_stale_jobs(
    session: Session,
    now: datetime | None = None,
    stale_after: timedelta = STALE_LOCK_AFTER,
) -> int:
    cutoff = (now or utc_now()) - stale_after
    stale_jobs = session.scalars(
        select(Job)
        .where(Job.status == JobStatus.running.value, Job.locked_at <= cutoff)
        .with_for_update(skip_locked=True)
    ).all()

    for job in stale_jobs:
        job.status = JobStatus.queued.value
        job.locked_at = None
        job.locked_by = None

    session.commit()
    return len(stale_jobs)


def run_worker_loop(worker_id: str | None = None, poll_seconds: float = 1.0) -> None:
    create_schema()
    resolved_worker_id = worker_id or f"worker-{os.getpid()}"
    while True:
        with SessionLocal() as session:
            recover_stale_jobs(session)
            job = run_job_once(session, resolved_worker_id)
        if job is None:
            time.sleep(poll_seconds)


if __name__ == "__main__":
    run_worker_loop()
