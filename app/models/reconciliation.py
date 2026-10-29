"""Reconciliation Run & Audit Model

Tracks nightly reconciliation job execution, diff statistics, and safety brake triggers.
"""

import uuid
from datetime import datetime
from typing import Optional, Any
from sqlalchemy import (
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    Uuid,
    Text,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now


class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    run_id: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        nullable=False,
        index=True,
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    filename: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        default="PENDING",
        nullable=False,
    )  # PENDING, STREAMING, SAFETY_BRAKE_TRIGGERED, APPROVED, ABORTED, COMPLETED, FAILED

    total_records_received: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_unchanged: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_stale: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_deactivated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    missing_active_records: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    missing_percentage: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    operator_review_required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    approved_by_operator: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    operator_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    operator_action_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    anomaly_report: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_recon_tenant_status", "tenant_id", "status"),
    )
