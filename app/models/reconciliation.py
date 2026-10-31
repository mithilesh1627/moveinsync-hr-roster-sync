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
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now


class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"

    run_id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
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


class ReconciliationStaging(Base):
    __tablename__ = "reconciliation_staging"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    run_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    employee_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    first_name: Mapped[str] = mapped_column(String(128), nullable=False)
    last_name: Mapped[str] = mapped_column(String(128), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    office_code: Mapped[str] = mapped_column(String(64), nullable=False)
    department: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    manager_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_version: Mapped[int] = mapped_column(Integer, nullable=False)
    record_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    __table_args__ = (
        UniqueConstraint("run_id", "tenant_id", "employee_id", name="uq_staging_run_tenant_emp"),
        Index("idx_staging_run_tenant_emp", "run_id", "tenant_id", "employee_id"),
        Index("idx_staging_emp_tenant_run", "tenant_id", "employee_id", "run_id"),
    )
