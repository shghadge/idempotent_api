import os
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/idempotent_api",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(
    bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
)


class Base(DeclarativeBase):
    """Base class for database models."""
    pass


def create_schema() -> None:
    """Create missing database tables."""
    Base.metadata.create_all(bind=engine)


def get_session() -> Iterator[Session]:
    """Provide one database session for a request."""
    with SessionLocal() as session:
        yield session
