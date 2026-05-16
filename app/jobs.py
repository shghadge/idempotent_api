from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from json import dumps
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column
from sqlalchemy.types import JSON

from .database import Base, get_session

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    dead = "dead"


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=JobStatus.queued.value
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class JobCreate(BaseModel):
    payload: dict[str, Any] = Field(..., min_length=1)


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: JobStatus
    attempts: int
    max_attempts: int
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    next_run_at: datetime
    locked_at: datetime | None
    locked_by: str | None
    created_at: datetime
    updated_at: datetime


def payload_fingerprint(payload: dict[str, Any]) -> str:
    canonical_payload = dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return sha256(canonical_payload.encode("utf-8")).hexdigest()


@router.post("", response_model=JobRead, status_code=status.HTTP_201_CREATED)
def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1),
    session: Session = Depends(get_session),
) -> Job:
    fingerprint = payload_fingerprint(body.payload)
    job = Job(
        idempotency_key=idempotency_key,
        payload=body.payload,
        payload_sha256=fingerprint,
    )
    session.add(job)

    try:
        session.commit()
        session.refresh(job)
        return job
    except IntegrityError:
        session.rollback()

    existing_job = session.scalar(
        select(Job).where(Job.idempotency_key == idempotency_key)
    )
    if existing_job is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key is already in use",
        )
    if existing_job.payload_sha256 != fingerprint:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was used with a different payload",
        )

    response.status_code = status.HTTP_200_OK
    return existing_job


@router.get("", response_model=list[JobRead])
def list_jobs(
    job_status: JobStatus | None = Query(default=None, alias="status"),
    session: Session = Depends(get_session),
) -> list[Job]:
    statement = select(Job).order_by(Job.created_at)
    if job_status is not None:
        statement = statement.where(Job.status == job_status.value)
    return list(session.scalars(statement))


@router.get("/dead", response_model=list[JobRead])
def list_dead_jobs(session: Session = Depends(get_session)) -> list[Job]:
    statement = (
        select(Job).where(Job.status == JobStatus.dead.value).order_by(Job.created_at)
    )
    return list(session.scalars(statement))


@router.get("/{job_id}", response_model=JobRead)
def get_job(job_id: str, session: Session = Depends(get_session)) -> Job:
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job
