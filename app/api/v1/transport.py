"""Transport Operations Router

Allows Transport Administrators to manage transport-owned attributes
(preferred pickup points, geofences, special assistance, shift timings)
and resolve office transfer review flags.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.auth import require_roles, AuthContext
from app.repositories.employee_repo import EmployeeRepository
from app.schemas.employee import TransportEmployeeUpdateRequest, EmployeeResponse

router = APIRouter(prefix="/transport", tags=["Transport Operations"])


@router.put("/employees/{employee_id}", response_model=EmployeeResponse)
async def update_transport_attributes(
    employee_id: str,
    request: TransportEmployeeUpdateRequest,
    auth: AuthContext = Depends(require_roles(["transport_admin", "operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Update transport-owned employee attributes."""
    tenant_id = auth.tenant_id
    repo = EmployeeRepository(session)

    emp = await repo.get_by_id(tenant_id=tenant_id, employee_id=employee_id, for_update=True)
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{employee_id}' not found in tenant '{tenant_id}'",
        )

    # Apply transport-owned updates
    if request.preferred_pickup_point_id is not None:
        emp.preferred_pickup_point_id = request.preferred_pickup_point_id
    if request.pickup_geofence is not None:
        emp.pickup_geofence = request.pickup_geofence
    if request.special_assistance_required is not None:
        emp.special_assistance_required = request.special_assistance_required
    if request.special_assistance_notes is not None:
        emp.special_assistance_notes = request.special_assistance_notes
    if request.shift_timing is not None:
        emp.shift_timing = request.shift_timing

    if request.resolve_transport_review:
        emp.transport_review_required = False

    await session.commit()
    await session.refresh(emp)

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
