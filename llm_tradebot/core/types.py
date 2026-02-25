"""Shared lightweight types to keep module interfaces explicit and typed."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ServiceMeta:
    """Metadata describing a running service instance."""

    name: str
    version: str
    env: str


@dataclass(frozen=True, slots=True)
class BarCloseEvent:
    """Normalized closed-candle event emitted by the ingestor."""

    type: str
    exchange: str
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    o: str
    h: str
    l: str
    c: str
    v: str
    event_time_ms: int
