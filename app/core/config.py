"""Application Configuration Module

Uses Pydantic Settings v2 to load, validate, and manage environment configuration.
"""

from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    app_env: Literal["development", "testing", "production"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # PostgreSQL Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/moveinsync_roster",
        description="Async SQLAlchemy database connection string",
    )
    database_sync_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/moveinsync_roster",
        description="Sync SQLAlchemy database connection string for Alembic/batch tasks",
    )
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800

    # Security & RBAC
    jwt_secret_key: str = "dev-secret-key-change-in-production-min-32-chars-long"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440  # 24 hours

    # Idempotency
    idempotency_retention_days: int = 7

    # Nightly Reconciliation & Safety Brake
    safety_brake_threshold_percent: float = 30.0  # Halt if >= 30% missing
    reconciliation_chunk_size: int = 2000

    # Transactional Outbox Publisher
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 50
    outbox_max_retries: int = 5
    outbox_base_backoff_seconds: float = 1.0

    # Downstream Cab Service
    downstream_booking_service_url: str = "http://localhost:8000/api/v1/mock-downstream/cancel-booking"
    downstream_timeout_seconds: float = 5.0

    # Caching
    cache_ttl_seconds: int = 300
    cache_max_entries: int = 1000


settings = Settings()
