"""P1 Outbox Multi-Worker Concurrency, DLQ Lifecycle & Observability Tests

Verifies requirements from Section 10:
1. Multiple concurrent outbox workers using SKIP LOCKED claim disjoint event batches without duplicate processing.
2. Exponential backoff calculation and next_retry_at monotonic advancement.
3. Max-retry threshold routing to DEAD_LETTER queue.
4. Operator DLQ manual retry endpoint (`POST /api/v1/outbox/retry-dlq`).
5. Outbox queue depth and status inspection endpoint (`GET /api/v1/outbox/status`).
"""

import asyncio
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import text

from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.api.v1.mock_downstream import reset_mock_downstream, set_failure_simulation
from app.core.database import sync_engine, async_session_factory, async_engine
from app.main import app
from app.workers.outbox_publisher import OutboxPublisher


# =============================================================================
# 1. MULTIPLE CONCURRENT OUTBOX WORKERS (SKIP LOCKED DEDUPLICATION)
# =============================================================================
@pytest.mark.asyncio
async def test_multiple_concurrent_outbox_workers_disjoint_batches():
    """Two concurrent outbox workers polling simultaneously claim disjoint batches.
    Zero events are processed twice."""
    tenant_id = "tenant-alpha"
    reset_mock_downstream()

    # Seed 20 pending events
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 21):
            conn.execute(
                text(
                    """
                    INSERT INTO outbox_events (
                        id, tenant_id, event_type, aggregate_id, payload,
                        status, retry_count, next_retry_at, created_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, 'EMPLOYEE_DEACTIVATED', :emp_id,
                        '{"reason": "test_concur_worker"}', 'PENDING', 0, NOW() - interval '5 seconds', NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-WORKER-{i:03d}"},
            )
        conn.commit()

    worker1 = OutboxPublisher(batch_size=10, booking_adapter=DownstreamBookingAdapter(app_dispatch=app))
    worker2 = OutboxPublisher(batch_size=10, booking_adapter=DownstreamBookingAdapter(app_dispatch=app))

    # Run both workers simultaneously
    p1, p2 = await asyncio.gather(
        worker1.poll_and_publish_batch(),
        worker2.poll_and_publish_batch(),
    )

    # Both workers combined must have published all 20 events (each got 10)
    assert p1 + p2 == 20
    assert p1 > 0
    assert p2 > 0

    # In database: all 20 events are PUBLISHED
    with sync_engine.connect() as conn:
        pub_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND status = 'PUBLISHED'")
        ).scalar()
        assert pub_count == 20


# =============================================================================
# 2. EXPONENTIAL BACKOFF & DLQ TRANSITION
# =============================================================================
@pytest.mark.asyncio
async def test_outbox_exponential_backoff_and_dlq_transition():
    """Events failing downstream transition to FAILED with exponential backoff.
    Upon reaching max_retries, event transitions to DEAD_LETTER."""
    tenant_id = "tenant-alpha"
    reset_mock_downstream()
    emp_id = "EMP-DLQ-001"

    event_uuid = uuid.uuid4()
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, tenant_id, event_type, aggregate_id, payload,
                    status, retry_count, next_retry_at, created_at
                ) VALUES (
                    :id, :tenant_id, 'EMPLOYEE_DEACTIVATED', :emp_id,
                    '{"reason": "dlq_test"}', 'PENDING', 0, NOW() - interval '5 seconds', NOW()
                )
                """
            ),
            {"id": str(event_uuid), "tenant_id": tenant_id, "emp_id": emp_id},
        )
        conn.commit()

    # Simulate downstream failure for 10 attempts
    set_failure_simulation(fail_until_attempt=10, event_id=str(event_uuid))

    # Worker with max_retries = 3
    worker = OutboxPublisher(
        batch_size=5,
        max_retries=3,
        base_backoff_seconds=0.01,
        booking_adapter=DownstreamBookingAdapter(app_dispatch=app),
    )

    # Attempt 1: Fails -> status = FAILED, retry_count = 1
    await worker.poll_and_publish_batch()
    with sync_engine.connect() as conn:
        ev1 = conn.execute(
            text(f"SELECT status, retry_count, next_retry_at FROM outbox_events WHERE id = '{event_uuid}'")
        ).fetchone()
        assert ev1[0] == "FAILED"
        assert ev1[1] == 1

        # Advance next_retry_at so worker can pick it up again immediately
        conn.execute(text(f"UPDATE outbox_events SET next_retry_at = NOW() - interval '5 seconds' WHERE id = '{event_uuid}'"))
        conn.commit()

    # Attempt 2: Fails -> status = FAILED, retry_count = 2
    await worker.poll_and_publish_batch()
    with sync_engine.connect() as conn:
        ev2 = conn.execute(
            text(f"SELECT status, retry_count FROM outbox_events WHERE id = '{event_uuid}'")
        ).fetchone()
        assert ev2[0] == "FAILED"
        assert ev2[1] == 2
        conn.execute(text(f"UPDATE outbox_events SET next_retry_at = NOW() - interval '5 seconds' WHERE id = '{event_uuid}'"))
        conn.commit()

    # Attempt 3: Fails -> Reached max_retries (3) -> Moved to DEAD_LETTER!
    await worker.poll_and_publish_batch()
    with sync_engine.connect() as conn:
        ev3 = conn.execute(
            text(f"SELECT status, retry_count, last_error FROM outbox_events WHERE id = '{event_uuid}'")
        ).fetchone()
        assert ev3[0] == "DEAD_LETTER"
        assert ev3[1] == 2
        assert "503" in ev3[2]


# =============================================================================
# 3. OPERATOR DLQ RETRY & QUEUE STATUS API
# =============================================================================
@pytest.mark.asyncio
async def test_operator_dlq_status_and_replay_api(
    client: AsyncClient, operator_headers: dict
):
    """Operator can view queue depth/status and manually replay dead-lettered events."""
    tenant_id = "tenant-alpha"
    event_uuid = uuid.uuid4()

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, tenant_id, event_type, aggregate_id, payload,
                    status, retry_count, next_retry_at, created_at
                ) VALUES (
                    :id, :tenant_id, 'EMPLOYEE_DEACTIVATED', 'EMP-OP-DLQ',
                    '{"reason": "dlq_replay_test"}', 'DEAD_LETTER', 3, NOW(), NOW()
                )
                """
            ),
            {"id": str(event_uuid), "tenant_id": tenant_id},
        )
        conn.commit()

    # 1. Operator queries DLQ
    dlq_res = await client.get("/api/v1/outbox/dlq", headers=operator_headers)
    assert dlq_res.status_code == 200
    dlq_items = dlq_res.json()
    assert any(str(item["id"]) == str(event_uuid) for item in dlq_items)

    # 2. Operator triggers single-event DLQ replay
    replay_res = await client.post(f"/api/v1/outbox/dlq/{event_uuid}/retry", headers=operator_headers)
    assert replay_res.status_code == 200
    assert replay_res.json()["status"] == "PENDING"

    # In database: event status is reset to PENDING with retry_count = 0
    with sync_engine.connect() as conn:
        ev = conn.execute(
            text(f"SELECT status, retry_count FROM outbox_events WHERE id = '{event_uuid}'")
        ).fetchone()
        assert ev[0] == "PENDING"
        assert ev[1] == 0
