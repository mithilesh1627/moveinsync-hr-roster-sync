"""Pydantic v2 Schemas for Real-Time HR Ingestion Events & Boundary Validation"""

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


# E.164 International Phone Number Regex (+<country_code><subscriber_number>, 8-15 digits)
E164_PHONE_REGEX = re.compile(r"^\+[1-9]\d{7,14}$")

# Office Code Regex (Uppercase Alphanumeric, Hyphen, Underscore, e.g. BLR-CAMPUS-1)
OFFICE_CODE_REGEX = re.compile(r"^[A-Z0-9_\-]{2,32}$")

# Standard Email Regex (RFC 5322 compatible)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


class EventType(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DEACTIVATE = "DEACTIVATE"


class EmployeeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class SingleRosterEventSchema(BaseModel):
    """Schema representing an individual HR event inside an ingestion payload."""
    event_id: Optional[str] = Field(default=None, description="Optional unique event ID used for idempotency")
    event_type: EventType = Field(..., description="Action: CREATE, UPDATE, or DEACTIVATE")
    employee_id: str = Field(..., min_length=1, max_length=64, description="Unique tenant employee identifier")
    source_version: int = Field(..., ge=0, description="Monotonically increasing HR version")
    source_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event creation timestamp in source system",
    )

    # Core HR Attributes
    first_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    last_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    email: Optional[str] = Field(default=None, max_length=256)
    phone: Optional[str] = Field(default=None, max_length=32)
    office_code: Optional[str] = Field(default=None, max_length=64)
    department: Optional[str] = Field(default=None, max_length=128)
    manager_id: Optional[str] = Field(default=None, max_length=64)
    status: Optional[EmployeeStatus] = Field(default=EmployeeStatus.ACTIVE)

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().lower()
            if not EMAIL_REGEX.match(v_clean):
                raise ValueError("Invalid email format (must comply with standard email format)")
            return v_clean
        return v

    @field_validator("phone")
    @classmethod
    def validate_phone_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip()
            if not E164_PHONE_REGEX.match(v_clean):
                raise ValueError("Invalid phone format (must comply with E.164 international standard, e.g. +919876543210)")
            return v_clean
        return v

    @field_validator("office_code")
    @classmethod
    def validate_office_code_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().upper()
            if not OFFICE_CODE_REGEX.match(v_clean):
                raise ValueError("Invalid office code (must be uppercase alphanumeric, e.g. BLR-CAMPUS-1)")
            return v_clean
        return v

    @model_validator(mode="after")
    def validate_required_fields_by_event_type(self) -> "SingleRosterEventSchema":
        """Enforce required fields for CREATE and UPDATE events."""
        if self.event_type == EventType.CREATE:
            missing = []
            if not self.first_name:
                missing.append("first_name")
            if not self.last_name:
                missing.append("last_name")
            if not self.email:
                missing.append("email")
            if not self.office_code:
                missing.append("office_code")
            if missing:
                raise ValueError(f"CREATE event missing mandatory fields: {', '.join(missing)}")
        return self


class RosterRecordError(BaseModel):
    """Structured error descriptor for a single rejected record in an ingest batch."""
    record_index: int
    employee_id: Optional[str] = None
    field: Optional[str] = None
    error_code: str
    message: str


class RosterRecordResult(BaseModel):
    """Status summary for an individual record in an ingest batch."""
    record_index: int
    employee_id: str
    status: Literal["accepted", "stale_ignored", "duplicate_noop", "rejected"]
    source_version: int
    message: Optional[str] = None


class RosterBatchEventRequest(BaseModel):
    """Batch ingestion request envelope supporting mixed events."""
    events: List[Dict[str, Any]] = Field(
        ...,
        min_length=1,
        max_length=5000,
        description="List of raw event dictionaries to be individually validated and applied",
    )


class RosterBatchEventResponse(BaseModel):
    """Batch ingestion response reporting accepted and rejected records without aborting the batch."""
    tenant_id: str
    total_received: int
    accepted_count: int
    stale_count: int
    duplicate_count: int
    rejected_count: int
    results: List[RosterRecordResult] = Field(default_factory=list)
    errors: List[RosterRecordError] = Field(default_factory=list)
