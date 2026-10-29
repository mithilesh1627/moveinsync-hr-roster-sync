"""Schemas for Outbox Events and Dead-Letter Queue (DLQ) Operations"""

import uuid
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel


class OutboxEventResponse(BaseModel):
    id: uuid.UUID
    tenant_id: str
    event_type: str
    aggregate_id: str
    payload: Any
    status: str
    retry_count: int
    next_retry_at: datetime
    last_error: Optional[str] = None
    published_at: Optional[datetime] = None
    created_at: datetime


class OutboxRetryResponse(BaseModel):
    event_id: str
    status: str
    message: str
