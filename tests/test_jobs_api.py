from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.jobs import get_session
from app.main import create_app


@pytest.fixture()
def client(tmp_path: Path) -> Iterator[TestClient]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'jobs.db'}",
        connect_args={"check_same_thread": False},
    )
    testing_session = sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    def override_session() -> Iterator[Session]:
        with testing_session() as session:
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


def test_same_idempotency_key_with_different_payload_conflicts(client: TestClient) -> None:
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
    assert conflict_response.json() == {"detail": "Idempotency key was used with a different payload"}


def test_unknown_job_returns_not_found(client: TestClient) -> None:
    response = client.get("/jobs/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}
