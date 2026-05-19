from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .database import create_schema
from .routes import router as jobs_router


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Create tables when the API starts."""
    create_schema()
    yield


def create_app(*, init_schema: bool = True) -> FastAPI:
    """Build the FastAPI app."""
    app = FastAPI(
        title="Idempotent Job Processing API",
        lifespan=lifespan if init_schema else None,
    )
    app.include_router(jobs_router)
    return app


app = create_app()
