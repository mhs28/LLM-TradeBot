"""Environment-driven settings shared by all services to keep runtime behavior deterministic."""

from functools import lru_cache
from typing import Callable

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Simple application settings loaded from environment variables or a local .env file."""

    APP_NAME: str = "LLM Tradebot"
    ENV: str = "dev"
    LOG_LEVEL: str = "INFO"
    VERSION: str = "0.1.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    INGEST_SYMBOLS: str = "ETHUSDT"
    INGEST_INTERVALS: str = "5m,15m,1h"
    INGEST_WINDOW: int = 200
    BINANCE_FUTURES_WS_URL: str = "wss://fstream.binance.com/ws"
    FEATURE_SYMBOLS: str = ""
    FEATURE_INTERVALS: str = "5m,15m,1h"
    FEATURE_WINDOW: int = 200
    REGIME_TREND_THRESHOLD: float = 0.002
    FEATURE_SNAPSHOT_PATH: str = "/app/data/feature_snapshots.jsonl"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    def ingest_symbols(self) -> tuple[str, ...]:
        """Return normalized symbol list from INGEST_SYMBOLS."""

        return self._split_csv(self.INGEST_SYMBOLS, transform=str.upper)

    def ingest_intervals(self) -> tuple[str, ...]:
        """Return normalized interval list from INGEST_INTERVALS."""

        return self._split_csv(self.INGEST_INTERVALS, transform=str.lower)

    def feature_symbols(self) -> tuple[str, ...]:
        """Return normalized symbols for feature generation."""

        symbols = self._split_csv(self.FEATURE_SYMBOLS, transform=str.upper)
        if symbols:
            return symbols

        ingest_symbols = self.ingest_symbols()
        if ingest_symbols:
            return ingest_symbols

        return ("ETHUSDT",)

    def feature_intervals(self) -> tuple[str, ...]:
        """Return normalized interval list from FEATURE_INTERVALS."""

        return self._split_csv(self.FEATURE_INTERVALS, transform=str.lower)

    @staticmethod
    def _split_csv(value: str, transform: Callable[[str], str]) -> tuple[str, ...]:
        """Split comma-separated values while removing empty entries and duplicates."""

        items: list[str] = []
        seen: set[str] = set()

        for raw in value.split(","):
            item = transform(raw.strip())
            if not item or item in seen:
                continue
            seen.add(item)
            items.append(item)

        return tuple(items)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings to avoid repeated environment parsing."""

    return Settings()
