"""Pytest Fixtures & Test Setup Configuration

Provides database connections against real PostgreSQL,
mock downstream adapters, JWT authorization headers, and test clients.
"""

import asyncio
import os
import sys
from typing import AsyncGenerator
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["APP_ENV"] = "testing"

from app.core.config import settings
settings.app_env = "testing"
from app.core.database import async_engine, async_session_factory, sync_engine
from app.core.auth import create_access_token
from app.models import Base
from app.main import app
from app.api.v1.mock_downstream import reset_mock_downstream
from app.core.cache import office_cache


@pytest.fixture(scope="session")
def event_loop():
    """Create a single session-scoped event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
def setup_database_schema():
    """Ensure all tables exist in PostgreSQL before running the test suite."""
    Base.metadata.create_all(bind=sync_engine)
    # Seed common test office
    with sync_engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO offices (id, tenant_id, office_code, office_name, timezone, is_active, created_at)
                VALUES 
                    (gen_random_uuid(), 'tenant-alpha', 'BLR-CAMPUS-1', 'Bangalore Campus 1', 'Asia/Kolkata', true, NOW()),
                    (gen_random_uuid(), 'tenant-alpha', 'HYD-CAMPUS-2', 'Hyderabad Campus 2', 'Asia/Kolkata', true, NOW()),
                    (gen_random_uuid(), 'tenant-beta', 'MUM-CAMPUS-1', 'Mumbai Campus 1', 'Asia/Kolkata', true, NOW())
                ON CONFLICT (tenant_id, office_code) DO NOTHING;
                """
            )
        )
        conn.commit()
    yield


@pytest.fixture(autouse=True)
def clean_state():
    """Synchronously clean test tables and mock downstream between test runs.
    
    Uses sync_engine to avoid asyncpg connection pool / event loop closing conflicts.
    """
    reset_mock_downstream()
    office_cache.clear()
    with sync_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE outbox_events, idempotency_keys, reconciliation_runs, reconciliation_staging CASCADE;"))
        conn.execute(text("DELETE FROM employees WHERE tenant_id IN ('tenant-alpha', 'tenant-beta', 'tenant-isolated');"))
        conn.commit()
    yield


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an independent async session for testing."""
    async with async_session_factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Yield an AsyncClient targeting the FastAPI application."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


@pytest.fixture
def hr_headers() -> dict:
    """Authorization headers for an HR integration caller."""
    token = create_access_token(
        user_id="hr-bot",
        tenant_id="tenant-alpha",
        roles=["hr_integration"],
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def transport_admin_headers() -> dict:
    """Authorization headers for a Transport Admin caller."""
    token = create_access_token(
        user_id="transport-admin-1",
        tenant_id="tenant-alpha",
        roles=["transport_admin"],
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def operator_headers() -> dict:
    """Authorization headers for an Operator caller."""
    token = create_access_token(
        user_id="operator-1",
        tenant_id="tenant-alpha",
        roles=["operator", "transport_admin", "hr_integration"],
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def tenant_beta_hr_headers() -> dict:
    """Authorization headers for a different tenant's HR integration."""
    token = create_access_token(
        user_id="hr-bot-beta",
        tenant_id="tenant-beta",
        roles=["hr_integration"],
    )
    return {"Authorization": f"Bearer {token}"}
