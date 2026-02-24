"""Environment-driven settings shared by all services to keep runtime behavior deterministic."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Simple application settings loaded from environment variables or a local .env file."""

    APP_NAME: str = "LLM Tradebot"
    ENV: str = "dev"
    LOG_LEVEL: str = "INFO"
    VERSION: str = "0.1.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings to avoid repeated environment parsing."""

    return Settings()
