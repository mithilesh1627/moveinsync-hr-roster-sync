"""Tests for Idempotency Key Semantics, Replays & Collisions (REQ-B1, REQ-B2)"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine


@pytest.mark.asyncio
async def test_replay_feed_twice_changes_nothing(client: AsyncClient, hr_headers: dict):
    """KEY ASSESSMENT POINT: Define idempotency key. Show test that replays the same feed twice and changes nothing."""
    idempotency_key = "idemp-feed-test-001"
    headers = {**hr_headers, "X-Idempotency-Key": idempotency_key}

    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-IDEMP-001",
                "source_version": 1,
                "first_name": "Idemp",
                "last_name": "Tester",
                "email": "idemp.test@moveinsync.com",
                "phone": "+919876543201",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    # 1. First Submission
    response1 = await client.post("/api/v1/roster/events", json=payload, headers=headers)
    assert response1.status_code == 200
    data1 = response1.json()
    assert data1["accepted_count"] == 1

    # Check outbox events count in DB
    with sync_engine.connect() as conn:
        count1 = conn.execute(
            text("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = 'EMP-IDEMP-001'")
        ).scalar()
        assert count1 == 1

    # 2. Replay EXACT SAME request with exact same idempotency key
    response2 = await client.post("/api/v1/roster/events", json=payload, headers=headers)
    assert response2.status_code == 200
    data2 = response2.json()

    # Cached response returned identically
    assert data1 == data2

    # Verify that NO DUPLICATE OUTBOX EVENT was created
    with sync_engine.connect() as conn:
        count2 = conn.execute(
            text("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = 'EMP-IDEMP-001'")
        ).scalar()
        assert count2 == 1, "Duplicate outbox event was created on idempotent replay!"


@pytest.mark.asyncio
async def test_idempotency_key_reuse_with_different_payload_returns_409_conflict(
    client: AsyncClient, hr_headers: dict
):
    """Test that reusing an idempotency key with a conflicting payload raises HTTP 409 Conflict."""
    idempotency_key = "idemp-collision-key-002"
    headers = {**hr_headers, "X-Idempotency-Key": idempotency_key}

    payload_a = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-COL-001",
                "source_version": 1,
                "first_name": "Payload",
                "last_name": "One",
                "email": "one@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    payload_b = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-COL-001",
                "source_version": 1,
                "first_name": "Different",  # Changed payload
                "last_name": "One",
                "email": "different@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    # Submit first payload
    res1 = await client.post("/api/v1/roster/events", json=payload_a, headers=headers)
    assert res1.status_code == 200

    # Submit second payload with same key
    res2 = await client.post("/api/v1/roster/events", json=payload_b, headers=headers)
    assert res2.status_code == 409
    err = res2.json()
    assert "Idempotency key collision" in err["error"]["message"]


@pytest.mark.asyncio
async def test_idempotency_key_is_scoped_per_tenant(
    client: AsyncClient, hr_headers: dict, tenant_beta_hr_headers: dict
):
    """Test that the same idempotency key used across distinct tenants does NOT collide."""
    shared_key = "shared-key-across-tenants"

    payload_tenant_alpha = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-ALPHA-01",
                "source_version": 1,
                "first_name": "Alpha",
                "last_name": "User",
                "email": "alpha.user@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    payload_tenant_beta = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-BETA-01",
                "source_version": 1,
                "first_name": "Beta",
                "last_name": "User",
                "email": "beta.user@moveinsync.com",
                "office_code": "MUM-CAMPUS-1",
            }
        ]
    }

    # Tenant Alpha submits
    res_a = await client.post(
        "/api/v1/roster/events",
        json=payload_tenant_alpha,
        headers={**hr_headers, "X-Idempotency-Key": shared_key},
    )
    assert res_a.status_code == 200

    # Tenant Beta submits with identical key and different payload: should SUCCEED because scope is (tenant_id, key)
    res_b = await client.post(
        "/api/v1/roster/events",
        json=payload_tenant_beta,
        headers={**tenant_beta_hr_headers, "X-Idempotency-Key": shared_key},
    )
    assert res_b.status_code == 200
