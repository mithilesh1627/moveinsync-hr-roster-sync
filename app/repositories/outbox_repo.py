"""Transactional Outbox Repository

Enables atomic event enqueue within the business transaction and high-throughput
non-blocking polling via PostgreSQL 'FOR UPDATE SKIP LOCKED'.
"""

import uuid
from datetime import datetime, timezone
from typing import List, Optional, Any
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxEvent
from app.models.base import utc_now


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def enqueue(
        self,
        tenant_id: str,
        event_type: str,
        aggregate_id: str,
        payload: Any,
    ) -> OutboxEvent:
        """Insert an outbox event within the active database transaction."""
        event = OutboxEvent(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            event_type=event_type,
            aggregate_id=aggregate_id,
            payload=payload,
            status="PENDING",
            retry_count=0,
            next_retry_at=utc_now(),
        )
        self.session.add(event)
        await self.session.flush()
        return event

    async def poll_pending_events(self, batch_size: int = 50) -> List[OutboxEvent]:
        """Fetch pending or ready-for-retry events using 'FOR UPDATE SKIP LOCKED'.
        
        Guarantees concurrency safety across multiple publisher workers without contention.
        """
        now = utc_now()
        query = (
            select(OutboxEvent)
            .where(
                OutboxEvent.status.in_(["PENDING", "FAILED"]),
                OutboxEvent.next_retry_at <= now,
            )
            .order_by(OutboxEvent.created_at.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def mark_published(self, event_id: uuid.UUID) -> None:
        """Mark event as successfully published."""
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                status="PUBLISHED",
                published_at=utc_now(),
                last_error=None,
            )
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def record_retry(
        self,
        event_id: uuid.UUID,
        retry_count: int,
        next_retry_at: datetime,
        error_message: str,
    ) -> None:
        """Reschedule event for exponential backoff retry."""
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                status="FAILED",
                retry_count=retry_count,
                next_retry_at=next_retry_at,
                last_error=error_message,
            )
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def move_to_dlq(self, event_id: uuid.UUID, error_message: str) -> None:
        """Move permanently failed event to Dead-Letter Queue (DLQ)."""
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                status="DEAD_LETTER",
                last_error=error_message,
            )
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def get_dlq_events(self, tenant_id: str, limit: int = 100) -> List[OutboxEvent]:
        """Fetch dead-letter events for operator inspection."""
        query = (
            select(OutboxEvent)
            .where(
                OutboxEvent.tenant_id == tenant_id,
                OutboxEvent.status == "DEAD_LETTER",
            )
            .order_by(OutboxEvent.created_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def re_enqueue_event(self, event_id: uuid.UUID) -> Optional[OutboxEvent]:
        """Reset a DLQ event back to PENDING for immediate redelivery."""
        query = select(OutboxEvent).where(OutboxEvent.id == event_id).with_for_update()
        result = await self.session.execute(query)
        event = result.scalars().first()
        if event:
            event.status = "PENDING"
            event.retry_count = 0
            event.next_retry_at = utc_now()
            event.last_error = "Re-enqueued by operator"
            await self.session.flush()
        return event

    async def get_pending_count(self, tenant_id: Optional[str] = None) -> int:
        """Count pending/failed events in the outbox."""
        query = select(func.count(OutboxEvent.id)).where(
            OutboxEvent.status.in_(["PENDING", "FAILED"])
        )
        if tenant_id:
            query = query.where(OutboxEvent.tenant_id == tenant_id)
        result = await self.session.execute(query)
        return result.scalar_one() or 0
