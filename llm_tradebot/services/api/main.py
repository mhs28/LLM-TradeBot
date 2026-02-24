"""FastAPI smoke service exposing health and version endpoints for deployment checks."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from llm_tradebot.core.config import get_settings
from llm_tradebot.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Log startup metadata for operational visibility."""

    logger.info(
        "api_startup",
        extra={"service": "api", "env": settings.ENV, "version": settings.VERSION},
    )
    yield


app = FastAPI(title=settings.APP_NAME, version=settings.VERSION, lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    """Return process liveness status."""

    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    """Return application metadata from shared settings."""

    return {
        "name": settings.APP_NAME,
        "version": settings.VERSION,
        "env": settings.ENV,
    }
