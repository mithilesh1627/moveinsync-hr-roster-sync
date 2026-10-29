"""Reconciliation Run Repository"""

from typing import Optional, List
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.reconciliation import ReconciliationRun


class ReconciliationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_run(self, run: ReconciliationRun) -> ReconciliationRun:
        self.session.add(run)
        await self.session.flush()
        return run

    async def get_by_run_id(
        self,
        tenant_id: str,
        run_id: str,
        for_update: bool = False,
    ) -> Optional[ReconciliationRun]:
        query = select(ReconciliationRun).where(
            ReconciliationRun.tenant_id == tenant_id,
            ReconciliationRun.run_id == run_id,
        )
        if for_update:
            query = query.with_for_update()
        result = await self.session.execute(query)
        return result.scalars().first()

    async def list_runs(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[ReconciliationRun]:
        query = (
            select(ReconciliationRun)
            .where(ReconciliationRun.tenant_id == tenant_id)
            .order_by(ReconciliationRun.started_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
