"""Transactional Outbox Event Model

Stores domain change events committed atomically in the same database transaction as employee updates.
"""

import uuid
from datetime import datetime
from typing import Optional, Any
from sqlalchemy import (
    String,
    Integer,
    DateTime,
    Uuid,
    Text,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )  # EMPLOYEE_CREATED, EMPLOYEE_UPDATED, EMPLOYEE_DEACTIVATED, OFFICE_TRANSFERRED
    aggregate_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )  # employee_id
    payload: Mapped[Any] = mapped_column(
        JSONB,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        default="PENDING",
        nullable=False,
    )  # PENDING, IN_PROGRESS, PUBLISHED, FAILED, DEAD_LETTER
    retry_count: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
    )
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )

    __table_args__ = (
        Index("idx_outbox_poll", "status", "next_retry_at"),
    )
