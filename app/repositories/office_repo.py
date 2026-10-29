"""Office Master Repository with Bounded TTL Caching"""

from typing import Set, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.office import Office
from app.core.cache import office_cache


class OfficeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_valid_office_codes(self, tenant_id: str) -> Set[str]:
        """Fetch active office codes for tenant, utilizing bounded in-memory cache."""
        cached_codes = office_cache.get(tenant_id)
        if cached_codes is not None:
            return cached_codes

        query = select(Office.office_code).where(
            Office.tenant_id == tenant_id,
            Office.is_active.is_(True),
        )
        result = await self.session.execute(query)
        codes = {row[0].upper() for row in result.all()}
        
        # Cache for tenant
        office_cache.set(tenant_id, codes)
        return codes

    async def create_office(
        self,
        tenant_id: str,
        office_code: str,
        office_name: str,
        timezone: str = "Asia/Kolkata",
    ) -> Office:
        """Create or register a tenant office, automatically invalidating the cache."""
        office = Office(
            tenant_id=tenant_id,
            office_code=office_code.upper(),
            office_name=office_name,
            timezone=timezone,
            is_active=True,
        )
        self.session.add(office)
        await self.session.flush()

        # Invalidate cache
        office_cache.invalidate(tenant_id)
        return office
