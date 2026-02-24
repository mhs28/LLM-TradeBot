"""Time helpers for consistent UTC timestamps across services."""

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return current UTC datetime with timezone attached."""

    return datetime.now(timezone.utc)
