"""Employee Schemas for Transport Admin Operations & Roster Queries"""

from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field


class TransportEmployeeUpdateRequest(BaseModel):
    """Schema for transport operations updating transport-owned fields."""
    preferred_pickup_point_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Assigned pickup point/stop ID",
    )
    pickup_geofence: Optional[Any] = Field(
        default=None,
        description="GeoJSON or coordinate bounds for pickup boundary",
    )
    special_assistance_required: Optional[bool] = Field(
        default=None,
        description="Wheelchair or medical commute assistance flag",
    )
    special_assistance_notes: Optional[str] = Field(
        default=None,
        max_length=512,
        description="Detailed operator notes for special assistance",
    )
    shift_timing: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Assigned shift schedule (e.g. 09:00-18:00)",
    )
    resolve_transport_review: Optional[bool] = Field(
        default=False,
        description="Clear transport_review_required flag after reviewing office transfer",
    )


class EmployeeResponse(BaseModel):
    """Safe employee profile response without exposing private secrets."""
    tenant_id: str
    employee_id: str
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    office_code: str
    department: Optional[str] = None
    manager_id: Optional[str] = None
    status: str

    # Transport-owned fields
    preferred_pickup_point_id: Optional[str] = None
    pickup_geofence: Optional[Any] = None
    special_assistance_required: bool = False
    special_assistance_notes: Optional[str] = None
    shift_timing: Optional[str] = None

    # Versioning & review flags
    source_version: int
    source_timestamp: datetime
    transport_review_required: bool
    created_at: datetime
    updated_at: datetime
