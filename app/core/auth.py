"""Authentication & Role-Based Access Control (RBAC) Module

Provides JWT token issuance, verification, API-Key authentication,
and FastAPI dependency guards enforcing role and tenant isolation.
"""

import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Tuple
import jwt
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader

from app.core.config import settings
from app.schemas.auth import AuthContext, TokenResponse

bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _hash_credential(secret: str) -> str:
    """Compute constant-length SHA256 digest of secret."""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


# Verified client credentials store: (tenant_id, client_id) -> {"secret_hash": str, "roles": List[str]}
_REGISTERED_CLIENTS: Dict[Tuple[str, str], Dict[str, object]] = {
    ("tenant-alpha", "hr-service"): {
        "secret_hash": _hash_credential("hr-client-secret-alpha-2026"),
        "roles": ["hr_integration"],
    },
    ("tenant-alpha", "transport-admin"): {
        "secret_hash": _hash_credential("transport-admin-secret-alpha-2026"),
        "roles": ["transport_admin"],
    },
    ("tenant-alpha", "operator-service"): {
        "secret_hash": _hash_credential("operator-secret-alpha-2026"),
        "roles": ["operator", "transport_admin", "hr_integration"],
    },
    ("tenant-beta", "hr-service"): {
        "secret_hash": _hash_credential("hr-client-secret-beta-2026"),
        "roles": ["hr_integration"],
    },
    ("tenant-beta", "operator-service"): {
        "secret_hash": _hash_credential("operator-secret-beta-2026"),
        "roles": ["operator", "transport_admin", "hr_integration"],
    },
}

# Verified API Key Store: key_hash -> AuthContext
_REGISTERED_API_KEYS: Dict[str, AuthContext] = {
    _hash_credential("alpha-hr-api-key-secure-999"): AuthContext(
        user_id="hr-api-key-alpha",
        tenant_id="tenant-alpha",
        roles=["hr_integration"],
    ),
    _hash_credential("alpha-operator-api-key-secure-888"): AuthContext(
        user_id="operator-api-key-alpha",
        tenant_id="tenant-alpha",
        roles=["operator", "transport_admin", "hr_integration"],
    ),
}


def register_client_credentials(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    roles: List[str],
) -> None:
    """Register or update verified client credentials."""
    _REGISTERED_CLIENTS[(tenant_id, client_id)] = {
        "secret_hash": _hash_credential(client_secret),
        "roles": list(roles),
    }


def register_api_key(api_key: str, tenant_id: str, roles: List[str]) -> None:
    """Register a verified API key for programmatic service callers."""
    _REGISTERED_API_KEYS[_hash_credential(api_key)] = AuthContext(
        user_id=f"api-key-{tenant_id}",
        tenant_id=tenant_id,
        roles=list(roles),
    )


def authenticate_client(
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> Optional[List[str]]:
    """Verify client credentials using constant-time hash comparison and return assigned roles."""
    record = _REGISTERED_CLIENTS.get((tenant_id, client_id))
    if not record:
        return None
    incoming_hash = _hash_credential(client_secret)
    expected_hash = record["secret_hash"]
    if isinstance(expected_hash, str) and hmac.compare_digest(expected_hash, incoming_hash):
        roles = record.get("roles")
        if isinstance(roles, list):
            return [str(r) for r in roles]
    return None


def authenticate_api_key(api_key: str) -> Optional[AuthContext]:
    """Verify API key against registered keys using constant-time digest comparison."""
    incoming_hash = _hash_credential(api_key)
    for stored_hash, context in _REGISTERED_API_KEYS.items():
        if hmac.compare_digest(stored_hash, incoming_hash):
            return context
    return None


def create_access_token(
    user_id: str,
    tenant_id: str,
    roles: List[str],
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Generate a signed HS256 JWT access token."""
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    )
    payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "roles": roles,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> AuthContext:
    """Decode and validate a JWT access token."""
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
        )
        sub: str = payload.get("sub")
        tenant_id: str = payload.get("tenant_id")
        roles: List[str] = payload.get("roles", [])

        if not sub or not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token claims: missing subject or tenant",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return AuthContext(user_id=sub, tenant_id=tenant_id, roles=roles)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_auth(
    bearer: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
    api_key: Optional[str] = Security(api_key_header),
) -> AuthContext:
    """FastAPI dependency extracting and validating authenticated client context.
    
    Requires either a valid signed Bearer JWT or a registered, verified X-API-Key.
    Unauthenticated or unverified requests are rejected with HTTP 401.
    """
    if bearer and bearer.credentials:
        return decode_access_token(bearer.credentials)

    if api_key:
        context = authenticate_api_key(api_key)
        if context:
            return context
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or unverified API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing authentication credentials (provide Bearer JWT or valid X-API-Key)",
        headers={"WWW-Authenticate": "Bearer"},
    )


def require_roles(allowed_roles: List[str]):
    """Factory dependency enforcing that the caller possesses at least one required role."""
    async def role_checker(auth: AuthContext = Depends(get_current_auth)) -> AuthContext:
        if not any(auth.has_role(r) for r in allowed_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access forbidden: requires one of roles {allowed_roles}",
            )
        return auth

    return role_checker
