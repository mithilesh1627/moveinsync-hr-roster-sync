"""Employee Model with Field Ownership & Multi-Tenant Partitioning"""

import uuid
from datetime import datetime
from typing import Optional, Any
from sqlalchemy import (
    String,
    BigInteger,
    Boolean,
    DateTime,
    Uuid,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, utc_now


class Employee(Base, TimestampMixin):
    __tablename__ = "employees"

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
    employee_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    # =========================================================================
    # HR-OWNED FIELDS (HR System has exclusive authority)
    # =========================================================================
    first_name: Mapped[str] = mapped_column(String(128), nullable=False)
    last_name: Mapped[str] = mapped_column(String(128), nullable=False)
    email: Mapped[str] = mapped_column(String(256), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    office_code: Mapped[str] = mapped_column(String(64), nullable=False)
    department: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    manager_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="ACTIVE",
    )  # ACTIVE, INACTIVE

    # =========================================================================
    # TRANSPORT-ADMIN-OWNED FIELDS (Transport Operations has exclusive authority)
    # HR updates must NEVER overwrite these fields!
    # =========================================================================
    preferred_pickup_point_id: Mapped[Optional[str]] = mapped_column(
        String(128),
        nullable=True,
    )
    pickup_geofence: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
    )
    special_assistance_required: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )
    special_assistance_notes: Mapped[Optional[str]] = mapped_column(
        String(512),
        nullable=True,
    )
    shift_timing: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
    )

    # =========================================================================
    # VERSIONING, DIFFING & CROSS-DOMAIN RECONCILIATION FLAGS
    # =========================================================================
    source_version: Mapped[int] = mapped_column(
        BigInteger,
        default=0,
        nullable=False,
    )
    source_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    record_hash: Mapped[Optional[str]] = mapped_column(
        String(128),
        nullable=True,
        index=True,
    )
    transport_review_required: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "employee_id", name="uq_tenant_employee_id"),
        Index("idx_employees_tenant_status", "tenant_id", "status"),
        Index("idx_employees_tenant_version", "tenant_id", "employee_id", "source_version"),
        Index("idx_employees_tenant_office", "tenant_id", "office_code"),
    )
