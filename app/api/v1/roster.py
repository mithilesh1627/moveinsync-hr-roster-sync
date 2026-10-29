"""Real-Time HR Roster Event Ingestion Router"""

from typing import Optional
from fastapi import APIRouter, Depends, Header, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.auth import require_roles, AuthContext
from app.schemas.roster_events import (
    RosterBatchEventRequest,
    RosterBatchEventResponse,
)
from app.services.idempotency_service import IdempotencyService
from app.services.roster_service import RosterService

router = APIRouter(prefix="/roster", tags=["HR Roster Ingestion"])


@router.post(
    "/events",
    response_model=RosterBatchEventResponse,
    status_code=status.HTTP_200_OK,
)
async def ingest_roster_events(
    request: RosterBatchEventRequest,
    response: Response,
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    auth: AuthContext = Depends(require_roles(["hr_integration", "operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Ingest a batch of HR employee change events.
    
    Guarantees:
    - Scoped per-tenant idempotency: replay with same key returns cached result.
    - Replay with same key but different payload returns HTTP 409 Conflict.
    - One bad record never fails the entire batch.
    - Stale versions are detected and safely ignored.
    - Preserves transport-admin fields and flags office transfers.
    """
    tenant_id = auth.tenant_id
    idempotency_service = IdempotencyService(session)

    # 1. Check Idempotency Key if supplied
    if x_idempotency_key:
        cached_result = await idempotency_service.check_idempotency(
            tenant_id=tenant_id,
            idempotency_key=x_idempotency_key,
            payload=request.model_dump(),
        )
        if cached_result:
            code, body = cached_result
            response.status_code = code
            return body

    # 2. Process batch within database transaction
    roster_service = RosterService(session)
    batch_response = await roster_service.process_batch_events(
        tenant_id=tenant_id,
        request=request,
    )

    # 3. Commit state changes & outbox events atomically
    await session.commit()

    # 4. Save Idempotency execution record if key provided
    if x_idempotency_key:
        await idempotency_service.record_execution(
            tenant_id=tenant_id,
            idempotency_key=x_idempotency_key,
            payload=request.model_dump(),
            response_code=status.HTTP_200_OK,
            response_body=batch_response.model_dump(),
        )
        await session.commit()

    return batch_response
