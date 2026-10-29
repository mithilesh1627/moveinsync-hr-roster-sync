"""Authentication & Role-Based Authorization Schemas"""

from typing import Optional, List
from pydantic import BaseModel, Field


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int
    tenant_id: str
    roles: List[str]


class AuthContext(BaseModel):
    """Internal security context extracted from validated JWT token or API Key."""
    user_id: str
    tenant_id: str
    roles: List[str]

    def has_role(self, role: str) -> bool:
        return role in self.roles or "operator" in self.roles


class LoginRequest(BaseModel):
    tenant_id: str = Field(..., min_length=2, max_length=64)
    client_id: str = Field(..., min_length=2, max_length=64)
    client_secret: str = Field(..., min_length=6, max_length=128)
