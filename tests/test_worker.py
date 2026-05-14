from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from app.database import Base
from app.jobs import Job, JobStatus, payload_fingerprint
from app.worker import lease_next_job, recover_stale_jobs, run_job_once
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture()
def session(tmp_path: Path) -> Iterator[Session]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'worker.db'}",
        connect_args={"check_same_thread": False},
    )
    testing_session = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )
    Base.metadata.create_all(bind=engine)

    with testing_session() as database_session:
        yield database_session


def add_job(session: Session, payload: dict[str, object], max_attempts: int = 5) -> Job:
    job = Job(
        idempotency_key=f"key-{uuid4()}",
        payload=payload,
        payload_sha256=payload_fingerprint(payload),
        max_attempts=max_attempts,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def test_worker_completes_ready_job(session: Session) -> None:
    job = add_job(session, {"task": "echo", "value": "done"})

    processed_job = run_job_once(session, "worker-1")

    assert processed_job is not None
    assert processed_job.id == job.id
    assert processed_job.status == JobStatus.succeeded.value
    assert processed_job.attempts == 1
    assert processed_job.result == {"echo": "done"}
    assert processed_job.error is None
    assert processed_job.locked_at is None
    assert processed_job.locked_by is None


def test_worker_schedules_retry_after_failure(session: Session) -> None:
    add_job(session, {"task": "fail", "error": "temporary outage"})

    processed_job = run_job_once(session, "worker-1")

    assert processed_job is not None
    assert processed_job.status == JobStatus.queued.value
    assert processed_job.attempts == 1
    assert processed_job.error == "temporary outage"
    assert processed_job.result is None
    assert processed_job.locked_at is None
    assert processed_job.locked_by is None
    assert processed_job.next_run_at - processed_job.updated_at >= timedelta(seconds=2)


def test_worker_moves_exhausted_job_to_dead_letter(session: Session) -> None:
    add_job(session, {"task": "fail", "error": "permanent failure"}, max_attempts=1)

    processed_job = run_job_once(session, "worker-1")

    assert processed_job is not None
    assert processed_job.status == JobStatus.dead.value
    assert processed_job.attempts == 1
    assert processed_job.error == "permanent failure"
    assert processed_job.result is None
    assert processed_job.locked_at is None
    assert processed_job.locked_by is None


def test_leasing_marks_job_running_before_another_worker_can_take_it(
    session: Session,
) -> None:
    job = add_job(session, {"task": "echo", "value": "leased"})

    first_lease = lease_next_job(session, "worker-1")
    second_lease = lease_next_job(session, "worker-2")

    assert first_lease is not None
    assert first_lease.id == job.id
    assert first_lease.status == JobStatus.running.value
    assert first_lease.locked_by == "worker-1"
    assert second_lease is None


def test_stale_running_jobs_are_requeued(session: Session) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    job = add_job(session, {"task": "echo", "value": "stale"})
    job.status = JobStatus.running.value
    job.locked_by = "dead-worker"
    job.locked_at = now - timedelta(minutes=10)
    session.commit()

    recovered_count = recover_stale_jobs(
        session, now=now, stale_after=timedelta(minutes=5)
    )
    session.refresh(job)

    assert recovered_count == 1
    assert job.status == JobStatus.queued.value
    assert job.locked_at is None
    assert job.locked_by is None
