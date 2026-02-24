"""Shared lightweight types to keep module interfaces explicit and typed."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ServiceMeta:
    """Metadata describing a running service instance."""

    name: str
    version: str
    env: str
