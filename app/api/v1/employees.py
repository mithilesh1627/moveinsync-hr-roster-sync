"""Employee Profile Query Router with Strict Multi-Tenant Scoping"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.auth import require_roles, AuthContext
from app.repositories.employee_repo import EmployeeRepository
from app.schemas.employee import EmployeeResponse

router = APIRouter(prefix="/employees", tags=["Employees"])


@router.get("/{employee_id}", response_model=EmployeeResponse)
async def get_employee_profile(
    employee_id: str,
    auth: AuthContext = Depends(require_roles(["employee", "transport_admin", "operator", "hr_integration"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch employee profile scoped strictly to caller's tenant."""
    # If role is pure employee, they can only view their own profile
    if auth.roles == ["employee"] and auth.user_id != employee_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Employees can only access their own profile",
        )

    repo = EmployeeRepository(session)
    emp = await repo.get_by_id(tenant_id=auth.tenant_id, employee_id=employee_id)
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{employee_id}' not found",
        )

    return EmployeeResponse(
        tenant_id=emp.tenant_id,
        employee_id=emp.employee_id,
        first_name=emp.first_name,
        last_name=emp.last_name,
        email=emp.email,
        phone=emp.phone,
        office_code=emp.office_code,
        department=emp.department,
        manager_id=emp.manager_id,
        status=emp.status,
        preferred_pickup_point_id=emp.preferred_pickup_point_id,
        pickup_geofence=emp.pickup_geofence,
        special_assistance_required=emp.special_assistance_required,
        special_assistance_notes=emp.special_assistance_notes,
        shift_timing=emp.shift_timing,
        source_version=emp.source_version,
        source_timestamp=emp.source_timestamp,
        transport_review_required=emp.transport_review_required,
        created_at=emp.created_at,
        updated_at=emp.updated_at,
    )
