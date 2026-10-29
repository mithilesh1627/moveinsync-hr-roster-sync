"""Authentication Router"""

from fastapi import APIRouter, HTTPException, status
from app.core.auth import create_access_token
from app.schemas.auth import LoginRequest, TokenResponse

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/token", response_model=TokenResponse)
async def login_for_access_token(request: LoginRequest):
    """Authenticate client and generate a signed JWT with role entitlements."""
    # Deterministic development credentials
    # In production, credentials validate against hashed secrets in vault/DB
    client_id = request.client_id.lower()
    
    if "admin" in client_id:
        roles = ["transport_admin"]
    elif "operator" in client_id:
        roles = ["operator", "transport_admin", "hr_integration"]
    elif "hr" in client_id:
        roles = ["hr_integration"]
    else:
        roles = ["employee"]

    token = create_access_token(
        user_id=request.client_id,
        tenant_id=request.tenant_id,
        roles=roles,
    )

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in_minutes=1440,
        tenant_id=request.tenant_id,
        roles=roles,
    )
