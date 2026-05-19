from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import get_session
from .jobs import Job, JobCreate, JobRead, JobStatus, payload_fingerprint

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobRead, status_code=status.HTTP_201_CREATED)
def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1),
    session: Session = Depends(get_session),
) -> Job:
    """Create a job or return the existing job for this key."""
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
    """List jobs, optionally filtered by status."""
    statement = select(Job).order_by(Job.created_at)
    if job_status is not None:
        statement = statement.where(Job.status == job_status.value)
    return list(session.scalars(statement))


@router.get("/dead", response_model=list[JobRead])
def list_dead_jobs(session: Session = Depends(get_session)) -> list[Job]:
    """List jobs that exhausted their retries."""
    statement = (
        select(Job).where(Job.status == JobStatus.dead.value).order_by(Job.created_at)
    )
    return list(session.scalars(statement))


@router.post("/{job_id}/replay", response_model=JobRead)
def replay_dead_job(job_id: str, session: Session = Depends(get_session)) -> Job:
    """Move a dead job back to the queue."""
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    if job.status != JobStatus.dead.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only dead jobs can be replayed",
        )

    now = datetime.now(UTC)
    job.status = JobStatus.queued.value
    job.attempts = 0
    job.result = None
    job.error = None
    job.next_run_at = now
    job.locked_at = None
    job.locked_by = None
    job.updated_at = now
    session.commit()
    session.refresh(job)
    return job


@router.get("/{job_id}", response_model=JobRead)
def get_job(job_id: str, session: Session = Depends(get_session)) -> Job:
    """Return one job by id."""
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job
