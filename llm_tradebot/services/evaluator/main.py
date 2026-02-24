"""Runnable placeholder for the evaluator service contract."""

import logging

from llm_tradebot.core.config import get_settings
from llm_tradebot.core.logging import configure_logging


def main() -> int:
    """Log placeholder state and exit successfully."""

    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    logger.info("service_not_implemented", extra={"service": "evaluator"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
