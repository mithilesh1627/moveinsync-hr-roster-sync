"""Authentication Router"""

from fastapi import APIRouter, HTTPException, status
from app.core.config import settings
from app.core.auth import create_access_token, authenticate_client
from app.schemas.auth import LoginRequest, TokenResponse

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/token", response_model=TokenResponse)
async def login_for_access_token(request: LoginRequest):
    """Authenticate client credentials and generate a signed JWT with verified role entitlements."""
    roles = authenticate_client(
        tenant_id=request.tenant_id,
        client_id=request.client_id,
        client_secret=request.client_secret,
    )
    if not roles:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid client credentials or tenant not registered",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token(
        user_id=request.client_id,
        tenant_id=request.tenant_id,
        roles=roles,
    )

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in_minutes=settings.access_token_expire_minutes,
        tenant_id=request.tenant_id,
        roles=roles,
    )
