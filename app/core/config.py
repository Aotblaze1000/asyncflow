"""Application configuration.

Settings are read from environment variables (or an optional ``.env`` file),
so the same image can run in development and production without code changes.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for AsyncFlow."""

    model_config = SettingsConfigDict(
        env_prefix="ASYNCFFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "AsyncFlow"
    environment: str = "development"
    debug: bool = False

    # Persistence
    database_url: str = "sqlite:///./asyncflow.db"
    storage_dir: Path = Field(default=Path("storage"))

    # Broker
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None

    # Upload limits
    max_upload_bytes: int = 10 * 1024 * 1024
    allowed_extensions: tuple[str, ...] = (
        ".txt",
        ".md",
        ".csv",
        ".log",
        ".json",
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
    )

    # Retry behaviour
    job_max_retries: int = 3
    job_retry_backoff_seconds: int = 2
    # A job stuck in ``processing`` longer than this is considered abandoned
    # (for example because a worker was killed) and may be re-queued.
    job_stale_after_seconds: int = 300

    @property
    def broker_url(self) -> str:
        return self.celery_broker_url or self.redis_url

    @property
    def result_backend(self) -> str:
        return self.celery_result_backend or self.redis_url

    def ensure_storage_dir(self) -> Path:
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        return self.storage_dir


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
