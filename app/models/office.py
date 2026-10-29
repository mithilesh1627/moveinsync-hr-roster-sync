"""Office Master Reference Model

Maintains valid office locations and timezones per corporate tenant.
"""

import uuid
from datetime import datetime
from sqlalchemy import (
    String,
    Boolean,
    DateTime,
    Uuid,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now


class Office(Base):
    __tablename__ = "offices"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    office_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    office_name: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )
    timezone: Mapped[str] = mapped_column(
        String(64),
        default="Asia/Kolkata",
        nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "office_code", name="uq_tenant_office_code"),
    )
