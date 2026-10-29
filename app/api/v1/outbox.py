"""Transactional Outbox DLQ Management Router"""

import uuid
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.auth import require_roles, AuthContext
from app.repositories.outbox_repo import OutboxRepository
from app.schemas.outbox import OutboxEventResponse, OutboxRetryResponse

router = APIRouter(prefix="/outbox", tags=["Outbox & Dead-Letter Queue"])


@router.get("/dlq", response_model=List[OutboxEventResponse])
async def list_dlq_events(
    limit: int = 50,
    auth: AuthContext = Depends(require_roles(["operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Retrieve dead-letter outbox events for operator troubleshooting."""
    repo = OutboxRepository(session)
    events = await repo.get_dlq_events(tenant_id=auth.tenant_id, limit=limit)
    return [
        OutboxEventResponse(
            id=e.id,
            tenant_id=e.tenant_id,
            event_type=e.event_type,
            aggregate_id=e.aggregate_id,
            payload=e.payload,
            status=e.status,
            retry_count=e.retry_count,
            next_retry_at=e.next_retry_at,
            last_error=e.last_error,
            published_at=e.published_at,
            created_at=e.created_at,
        )
        for e in events
    ]


@router.post("/dlq/{event_id}/retry", response_model=OutboxRetryResponse)
async def retry_dlq_event(
    event_id: uuid.UUID,
    auth: AuthContext = Depends(require_roles(["operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Re-enqueue a dead-letter event for immediate delivery retry."""
    repo = OutboxRepository(session)
    event = await repo.re_enqueue_event(event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Outbox event '{event_id}' not found",
        )
    await session.commit()

    return OutboxRetryResponse(
        event_id=str(event.id),
        status=event.status,
        message="Event re-enqueued for immediate delivery",
    )
