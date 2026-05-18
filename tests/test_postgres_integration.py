import os
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base, get_session
from app.jobs import Job, JobStatus, payload_fingerprint
from app.main import create_app
from app.worker import run_job_once

POSTGRES_TEST_DATABASE_URL = os.environ.get("POSTGRES_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not POSTGRES_TEST_DATABASE_URL,
    reason="POSTGRES_TEST_DATABASE_URL is required for Postgres integration tests",
)


@pytest.fixture()
def postgres_session() -> Iterator[Session]:
    engine = create_engine(POSTGRES_TEST_DATABASE_URL, pool_pre_ping=True)
    testing_session = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    with testing_session() as session:
        yield session

    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture()
def postgres_client(postgres_session: Session) -> Iterator[TestClient]:
    def override_session() -> Iterator[Session]:
        yield postgres_session

    app = create_app(init_schema=False)
    app.dependency_overrides[get_session] = override_session

    with TestClient(app) as test_client:
        yield test_client


def test_postgres_api_and_worker_round_trip(
    postgres_client: TestClient, postgres_session: Session
) -> None:
    create_response = postgres_client.post(
        "/jobs",
        headers={"Idempotency-Key": "postgres-round-trip"},
        json={"payload": {"task": "echo", "value": "postgres"}},
    )
    duplicate_response = postgres_client.post(
        "/jobs",
        headers={"Idempotency-Key": "postgres-round-trip"},
        json={"payload": {"value": "postgres", "task": "echo"}},
    )

    assert create_response.status_code == 201
    assert duplicate_response.status_code == 200
    assert duplicate_response.json()["id"] == create_response.json()["id"]

    processed_job = run_job_once(postgres_session, "postgres-worker")

    assert processed_job is not None
    assert processed_job.id == create_response.json()["id"]
    assert processed_job.status == JobStatus.succeeded.value
    assert processed_job.result == {"echo": "postgres"}

    status_response = postgres_client.get(f"/jobs/{processed_job.id}")

    assert status_response.status_code == 200
    assert status_response.json()["status"] == JobStatus.succeeded.value
    assert status_response.json()["result"] == {"echo": "postgres"}


def test_postgres_skip_locked_hides_locked_jobs(postgres_session: Session) -> None:
    job = Job(
        idempotency_key="postgres-lock-test",
        payload={"task": "echo", "value": "lock"},
        payload_sha256=payload_fingerprint({"task": "echo", "value": "lock"}),
    )
    postgres_session.add(job)
    postgres_session.commit()
    expected_job_id = job.id

    engine = postgres_session.get_bind()
    testing_session = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )
    statement = (
        select(Job)
        .where(Job.status == JobStatus.queued.value)
        .order_by(Job.created_at)
        .with_for_update(skip_locked=True)
    )

    with testing_session() as first_session, testing_session() as second_session:
        first_transaction = first_session.begin()
        second_transaction = second_session.begin()
        locked_job = None
        skipped_job = None
        try:
            locked_job = first_session.scalar(statement)
            locked_job_id = locked_job.id if locked_job is not None else None
            skipped_job = second_session.scalar(statement)
        finally:
            second_transaction.rollback()
            first_transaction.rollback()

    assert locked_job is not None
    assert locked_job_id == expected_job_id
    assert skipped_job is None
