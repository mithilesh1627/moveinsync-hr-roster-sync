"""Idempotency Service

Enforces exactly-once semantics per (tenant_id, idempotency_key) scope.
Replays return cached responses; payload mismatches return HTTP 409 Conflict.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Any
from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.idempotency import IdempotencyRecord
from app.models.base import utc_now
from app.repositories.idempotency_repo import IdempotencyRepository
from app.utils.hashing import compute_payload_hash


class IdempotencyService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = IdempotencyRepository(session)

    async def acquire_transaction_lock(self, tenant_id: str, idempotency_key: str) -> None:
        """Acquire a transaction-level advisory lock scoped to (tenant_id, idempotency_key).
        
        Serializes concurrent requests using the same idempotency key.
        PostgreSQL automatically releases this lock on transaction commit or rollback.
        """
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:t), hashtext(:k))"),
            {"t": tenant_id, "k": idempotency_key},
        )

    async def check_idempotency(
        self,
        tenant_id: str,
        idempotency_key: str,
        payload: Any,
        for_update: bool = True,
    ) -> Optional[Tuple[int, Any]]:
        """Check if an idempotency key has already been executed for this tenant.
        
        Returns:
            (response_code, response_body) if an exact matching replay is found.
        Raises:
            HTTPException(409): If the key exists but was submitted with a different payload.
        """
        existing = await self.repo.get(tenant_id, idempotency_key, for_update=for_update)
        if not existing:
            return None

        # Check expiration
        if existing.expires_at < utc_now():
            return None

        current_hash = compute_payload_hash(payload)
        if existing.payload_hash != current_hash:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key collision: key previously used with different request payload",
            )

        # Exact match: Return cached response
        return existing.response_code, existing.response_body

    async def record_execution(
        self,
        tenant_id: str,
        idempotency_key: str,
        payload: Any,
        response_code: int,
        response_body: Any,
    ) -> IdempotencyRecord:
        """Persist execution result under tenant idempotency key."""
        payload_hash = compute_payload_hash(payload)
        expires_at = utc_now() + timedelta(days=settings.idempotency_retention_days)

        record = IdempotencyRecord(
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            response_code=response_code,
            response_body=response_body,
            expires_at=expires_at,
        )
        return await self.repo.save(record)
