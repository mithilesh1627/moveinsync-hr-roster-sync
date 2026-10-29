"""Employee Repository

Encapsulates data access and locking strategies for multi-tenant employee records.
"""

from typing import Optional, List, Dict, Tuple
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.employee import Employee


class EmployeeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(
        self,
        tenant_id: str,
        employee_id: str,
        for_update: bool = False,
    ) -> Optional[Employee]:
        """Fetch employee by tenant and employee ID with optional row-level exclusive lock."""
        query = select(Employee).where(
            Employee.tenant_id == tenant_id,
            Employee.employee_id == employee_id,
        )
        if for_update:
            # Row-level lock to prevent concurrent update races on the same employee
            query = query.with_for_update()
        result = await self.session.execute(query)
        return result.scalars().first()

    async def create(self, employee: Employee) -> Employee:
        """Persist a new employee record."""
        self.session.add(employee)
        await self.session.flush()
        return employee

    async def count_active(self, tenant_id: str) -> int:
        """Count total ACTIVE employees for a given tenant."""
        query = select(func.count(Employee.id)).where(
            Employee.tenant_id == tenant_id,
            Employee.status == "ACTIVE",
        )
        result = await self.session.execute(query)
        return result.scalar_one() or 0

    async def get_active_ids_and_hashes(self, tenant_id: str) -> Dict[str, str]:
        """Retrieve dictionary of {employee_id: record_hash} for all active employees of a tenant.
        
        Optimized projection returning only employee_id and record_hash.
        """
        query = select(Employee.employee_id, Employee.record_hash).where(
            Employee.tenant_id == tenant_id,
            Employee.status == "ACTIVE",
        )
        result = await self.session.execute(query)
        return {row[0]: (row[1] or "") for row in result.all()}

    async def deactivate_batch(self, tenant_id: str, employee_ids: List[str]) -> int:
        """Bulk mark a set of employees as INACTIVE."""
        if not employee_ids:
            return 0
        stmt = (
            update(Employee)
            .where(
                Employee.tenant_id == tenant_id,
                Employee.employee_id.in_(employee_ids),
                Employee.status == "ACTIVE",
            )
            .values(status="INACTIVE")
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount
