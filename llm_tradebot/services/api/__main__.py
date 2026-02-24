"""Module entrypoint for running the API service with shared settings."""

import uvicorn

from llm_tradebot.core.config import get_settings


def main() -> int:
    """Run the API service using configured host and port."""

    settings = get_settings()
    uvicorn.run(
        "llm_tradebot.services.api.main:app",
        host=settings.HOST,
        port=settings.PORT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
