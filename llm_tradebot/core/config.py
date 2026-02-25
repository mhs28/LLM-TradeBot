"""Environment-driven settings shared by all services to keep runtime behavior deterministic."""

from functools import lru_cache
from typing import Callable

from pydantic_settings import BaseSettings, SettingsConfigDict

_DEFAULT_TRADER_DECISION_PATH = "/app/data/router_decisions.jsonl"
_DEFAULT_TRADER_SNAPSHOT_PATH = "/app/data/feature_snapshots.jsonl"


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
    ROUTER_SNAPSHOT_PATH: str = "/app/data/feature_snapshots.jsonl"
    ROUTER_DECISION_PATH: str = "/app/data/router_decisions.jsonl"
    ROUTER_MIN_HOLD_BARS: int = 3
    ROUTER_DEV_ENTRY_THRESHOLD: float = 0.002
    ROUTER_LOG_EVERY_N: int = 1
    ROUTER_TEST_FORCE_TARGET: str = ""
    ROUTER_SYMBOLS: str = ""
    TRADER_DECISION_PATH: str = _DEFAULT_TRADER_DECISION_PATH
    TRADER_SNAPSHOT_PATH: str = _DEFAULT_TRADER_SNAPSHOT_PATH
    PAPER_TRADES_PATH: str = "/app/data/paper_trades.jsonl"
    PAPER_START_BALANCE: float = 10000.0
    PAPER_FEE_RATE: float = 0.0
    PAPER_SYMBOLS: str = ""

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

    def router_symbols(self) -> tuple[str, ...]:
        """Return normalized symbols for router decisions."""

        symbols = self._split_csv(self.ROUTER_SYMBOLS, transform=str.upper)
        if symbols:
            return symbols

        feature_symbols = self._split_csv(self.FEATURE_SYMBOLS, transform=str.upper)
        if feature_symbols:
            return feature_symbols

        ingest_symbols = self.ingest_symbols()
        if ingest_symbols:
            return ingest_symbols

        return ("ETHUSDT",)

    def paper_symbols(self) -> tuple[str, ...]:
        """Return normalized symbols for paper trader decisions."""

        symbols = self._split_csv(self.PAPER_SYMBOLS, transform=str.upper)
        if symbols:
            return symbols

        return self.router_symbols()

    def trader_decision_path(self) -> str:
        """Return decision path while preserving legacy router path overrides."""

        trader_path = self.TRADER_DECISION_PATH.strip()
        router_path = self.ROUTER_DECISION_PATH.strip()
        if trader_path and trader_path != _DEFAULT_TRADER_DECISION_PATH:
            return trader_path
        if router_path:
            return router_path
        if trader_path:
            return trader_path
        return _DEFAULT_TRADER_DECISION_PATH

    def trader_snapshot_path(self) -> str:
        """Return snapshot path while preserving legacy feature/router path overrides."""

        trader_path = self.TRADER_SNAPSHOT_PATH.strip()
        router_path = self.ROUTER_SNAPSHOT_PATH.strip()
        feature_path = self.FEATURE_SNAPSHOT_PATH.strip()
        if trader_path and trader_path != _DEFAULT_TRADER_SNAPSHOT_PATH:
            return trader_path
        if router_path:
            return router_path
        if feature_path:
            return feature_path
        if trader_path:
            return trader_path
        return _DEFAULT_TRADER_SNAPSHOT_PATH

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
