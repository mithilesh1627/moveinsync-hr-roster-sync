"""Idempotency Repository

Manages scoped persistence and atomic locking of idempotency records.
"""

from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.idempotency import IdempotencyRecord


class IdempotencyRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(
        self,
        tenant_id: str,
        idempotency_key: str,
        for_update: bool = False,
    ) -> Optional[IdempotencyRecord]:
        """Fetch idempotency record scoped to tenant and key with optional lock."""
        query = select(IdempotencyRecord).where(
            IdempotencyRecord.tenant_id == tenant_id,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        if for_update:
            query = query.with_for_update()
        result = await self.session.execute(query)
        return result.scalars().first()

    async def save(self, record: IdempotencyRecord) -> IdempotencyRecord:
        """Persist or merge an idempotency record."""
        self.session.add(record)
        await self.session.flush()
        return record
