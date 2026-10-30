"""Authentication Security & Production Hardening Tests (P0)

Verifies:
1. Unauthenticated privileged JWT issuance is blocked (invalid/unregistered credentials return 401).
2. Valid credentials return signed JWT with verified roles.
3. Unverified API keys return 401 (no default-operator fallback).
4. Verified API keys authenticate successfully.
5. Production startup fails if JWT_SECRET_KEY is weak or default.
6. Production startup fails if wildcard CORS origin '*' is present.
7. Mock downstream routes are excluded in production mode.
"""

import pytest
from httpx import AsyncClient, ASGITransport
from app.core.config import Settings
from app.core.auth import register_api_key, register_client_credentials
from app.main import app


@pytest.mark.asyncio
async def test_unauthenticated_jwt_issuance_blocked(client: AsyncClient):
    """Attempting to obtain a token with invalid secret or unregistered client returns 401."""
    # 1. Unregistered client
    res1 = await client.post(
        "/api/v1/auth/token",
        json={
            "tenant_id": "tenant-alpha",
            "client_id": "malicious-attacker",
            "client_secret": "random-password-123",
        },
    )
    assert res1.status_code == 401
    assert "Invalid client credentials" in res1.json()["error"]["message"]

    # 2. Registered client with wrong password
    res2 = await client.post(
        "/api/v1/auth/token",
        json={
            "tenant_id": "tenant-alpha",
            "client_id": "hr-service",
            "client_secret": "wrong-secret-token",
        },
    )
    assert res2.status_code == 401
    assert "Invalid client credentials" in res2.json()["error"]["message"]


@pytest.mark.asyncio
async def test_verified_credential_jwt_issuance(client: AsyncClient):
    """Valid registered credentials return signed token with exact authorized roles."""
    res = await client.post(
        "/api/v1/auth/token",
        json={
            "tenant_id": "tenant-alpha",
            "client_id": "hr-service",
            "client_secret": "hr-client-secret-alpha-2026",
        },
    )
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["roles"] == ["hr_integration"]


@pytest.mark.asyncio
async def test_unverified_api_key_rejected(client: AsyncClient):
    """Unverified API-key or arbitrary header fails with 401 without defaulting to operator."""
    res = await client.get(
        "/api/v1/employees/ANY-EMP-001",
        headers={"X-API-Key": "tenant-alpha:operator:arbitrary-secret"},
    )
    assert res.status_code == 401
    assert "Invalid or unverified API key" in res.json()["error"]["message"]

    # Simple arbitrary string also fails
    res_simple = await client.get(
        "/api/v1/employees/ANY-EMP-001",
        headers={"X-API-Key": "unregistered-key"},
    )
    assert res_simple.status_code == 401


@pytest.mark.asyncio
async def test_verified_api_key_accepted(client: AsyncClient):
    """Registered API key grants authenticated access."""
    # alpha-operator-api-key-secure-888 is registered in _REGISTERED_API_KEYS
    res = await client.get(
        "/healthz",
        headers={"X-API-Key": "alpha-operator-api-key-secure-888"},
    )
    assert res.status_code == 200


def test_production_fails_on_insecure_jwt_secret():
    """Settings validation fails in production if secret is too short or default."""
    with pytest.raises(ValueError, match="at least 32 characters"):
        Settings(
            app_env="production",
            jwt_secret_key="short",
        )

    with pytest.raises(ValueError, match="Insecure default JWT_SECRET_KEY"):
        Settings(
            app_env="production",
            jwt_secret_key="enterprise-production-secret-key-min-32-chars",
        )


def test_production_fails_on_wildcard_cors():
    """Settings validation fails in production if wildcard '*' CORS origin is used."""
    with pytest.raises(ValueError, match="Wildcard CORS origin"):
        Settings(
            app_env="production",
            jwt_secret_key="my-super-secure-production-random-jwt-key-9999",
            cors_allowed_origins=["*"],
        )


def test_mock_downstream_routes_disabled_in_production():
    """When app_env is production, mock downstream routes are omitted from API router."""
    from fastapi import FastAPI
    from unittest.mock import patch

    with patch("app.core.config.settings.app_env", "production"):
        import importlib
        import app.api.v1.router as router_module
        importlib.reload(router_module)

        prod_app = FastAPI()
        prod_app.include_router(router_module.api_v1_router)

        routes = [route.path for route in prod_app.routes]
        assert not any("mock-downstream" in r for r in routes)

        # Restore
        with patch("app.core.config.settings.app_env", "testing"):
            importlib.reload(router_module)
