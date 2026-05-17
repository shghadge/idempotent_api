from collections.abc import Iterator
from pathlib import Path

import pytest
from app.database import Base
from app.jobs import Job, JobStatus, get_session, payload_fingerprint
from app.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture()
def session(tmp_path: Path) -> Iterator[Session]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'jobs.db'}",
        connect_args={"check_same_thread": False},
    )
    testing_session = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )
    Base.metadata.create_all(bind=engine)

    with testing_session() as database_session:
        yield database_session


@pytest.fixture()
def client(session: Session) -> Iterator[TestClient]:
    def override_session() -> Iterator[Session]:
        yield session

    app = create_app(init_schema=False)
    app.dependency_overrides[get_session] = override_session

    with TestClient(app) as test_client:
        yield test_client


def test_create_job_and_fetch_status(client: TestClient) -> None:
    response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "job-create-1"},
        json={"payload": {"task": "send_email", "to": "user@example.com"}},
    )

    assert response.status_code == 201
    created_job = response.json()
    assert created_job["status"] == "queued"
    assert created_job["attempts"] == 0
    assert created_job["payload"] == {"task": "send_email", "to": "user@example.com"}

    status_response = client.get(f"/jobs/{created_job['id']}")

    assert status_response.status_code == 200
    assert status_response.json() == created_job


def test_repeated_submission_returns_existing_job(client: TestClient) -> None:
    first_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "same-job"},
        json={"payload": {"a": 1, "b": 2}},
    )
    second_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "same-job"},
        json={"payload": {"b": 2, "a": 1}},
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 200
    assert second_response.json()["id"] == first_response.json()["id"]
    assert second_response.json()["payload"] == first_response.json()["payload"]


def test_same_idempotency_key_with_different_payload_conflicts(
    client: TestClient,
) -> None:
    first_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "conflicting-job"},
        json={"payload": {"task": "send_email"}},
    )
    conflict_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "conflicting-job"},
        json={"payload": {"task": "charge_card"}},
    )

    assert first_response.status_code == 201
    assert conflict_response.status_code == 409
    assert conflict_response.json() == {
        "detail": "Idempotency key was used with a different payload"
    }


def test_unknown_job_returns_not_found(client: TestClient) -> None:
    response = client.get("/jobs/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


def test_list_jobs_filters_by_status(client: TestClient, session: Session) -> None:
    queued_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "queued-job"},
        json={"payload": {"task": "echo", "value": "queued"}},
    )
    dead_job = Job(
        idempotency_key="dead-job",
        payload={"task": "fail"},
        payload_sha256=payload_fingerprint({"task": "fail"}),
        status=JobStatus.dead.value,
        attempts=5,
    )
    session.add(dead_job)
    session.commit()

    queued_jobs = client.get("/jobs", params={"status": "queued"})

    assert queued_response.status_code == 201
    assert queued_jobs.status_code == 200
    assert [job["id"] for job in queued_jobs.json()] == [queued_response.json()["id"]]


def test_dead_letter_endpoint_lists_dead_jobs(
    client: TestClient, session: Session
) -> None:
    dead_job = Job(
        idempotency_key="dead-letter-job",
        payload={"task": "fail", "error": "exhausted"},
        payload_sha256=payload_fingerprint({"task": "fail", "error": "exhausted"}),
        status=JobStatus.dead.value,
        attempts=5,
        error="exhausted",
    )
    queued_job = Job(
        idempotency_key="queued-letter-job",
        payload={"task": "echo"},
        payload_sha256=payload_fingerprint({"task": "echo"}),
    )
    session.add_all([dead_job, queued_job])
    session.commit()

    response = client.get("/jobs/dead")

    assert response.status_code == 200
    dead_jobs = response.json()
    assert len(dead_jobs) == 1
    assert dead_jobs[0]["id"] == dead_job.id
    assert dead_jobs[0]["status"] == JobStatus.dead.value
    assert dead_jobs[0]["locked_at"] is None
    assert dead_jobs[0]["locked_by"] is None


def test_replay_dead_job_requeues_it(client: TestClient, session: Session) -> None:
    dead_job = Job(
        idempotency_key="replay-dead-job",
        payload={"task": "fail", "error": "temporary"},
        payload_sha256=payload_fingerprint({"task": "fail", "error": "temporary"}),
        status=JobStatus.dead.value,
        attempts=5,
        result={"partial": True},
        error="temporary",
        locked_by="old-worker",
    )
    session.add(dead_job)
    session.commit()

    response = client.post(f"/jobs/{dead_job.id}/replay")

    assert response.status_code == 200
    replayed_job = response.json()
    assert replayed_job["id"] == dead_job.id
    assert replayed_job["status"] == JobStatus.queued.value
    assert replayed_job["attempts"] == 0
    assert replayed_job["result"] is None
    assert replayed_job["error"] is None
    assert replayed_job["locked_at"] is None
    assert replayed_job["locked_by"] is None


def test_replay_non_dead_job_conflicts(client: TestClient) -> None:
    create_response = client.post(
        "/jobs",
        headers={"Idempotency-Key": "replay-queued-job"},
        json={"payload": {"task": "echo", "value": "queued"}},
    )

    response = client.post(f"/jobs/{create_response.json()['id']}/replay")

    assert create_response.status_code == 201
    assert response.status_code == 409
    assert response.json() == {"detail": "Only dead jobs can be replayed"}


def test_replay_unknown_job_returns_not_found(client: TestClient) -> None:
    response = client.post("/jobs/missing/replay")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}
