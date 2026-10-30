"""Application Configuration Module

Uses Pydantic Settings v2 to load, validate, and manage environment configuration.
"""

from typing import Literal, List
from pydantic import Field, model_validator
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

    # CORS
    cors_allowed_origins: List[str] = [
        "http://localhost:3000",
        "http://localhost:8000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8000",
    ]

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

    @model_validator(mode="after")
    def validate_production_security(self) -> "Settings":
        """Enforce strict production security policies at startup."""
        if self.app_env == "production":
            insecure_defaults = {
                "dev-secret-key-change-in-production-min-32-chars-long",
                "enterprise-production-secret-key-min-32-chars",
                "secret",
                "change-me",
                "password",
            }
            if not self.jwt_secret_key or len(self.jwt_secret_key) < 32:
                raise ValueError(
                    "Production startup failed: JWT_SECRET_KEY must be at least 32 characters long."
                )
            if self.jwt_secret_key in insecure_defaults:
                raise ValueError(
                    "Production startup failed: Insecure default JWT_SECRET_KEY detected. Provide a secure, unique secret."
                )
            if "*" in self.cors_allowed_origins:
                raise ValueError(
                    "Production startup failed: Wildcard CORS origin '*' is not permitted in production."
                )
        return self


settings = Settings()
