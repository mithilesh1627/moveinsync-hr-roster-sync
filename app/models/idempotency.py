"""Idempotency Key Persistence Model

Enforces per-tenant idempotency scope and prevents duplicate side-effects.
"""

import uuid
from datetime import datetime
from typing import Optional, Any
from sqlalchemy import (
    String,
    Integer,
    DateTime,
    Uuid,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_keys"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )
    payload_hash: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )
    response_code: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    response_body: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_tenant_idempotency_key"),
        Index("idx_idempotency_expires", "expires_at"),
    )
