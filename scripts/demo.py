"""MoveInSync HR Roster Sync - End-to-End Live Demonstration Script

Demonstrates the core system guarantees:
1. Health & Readiness verification
2. Boundary Validation (Mixed valid/invalid batch processing)
3. Idempotent Replays & Conflict Detection
4. Out-of-Order Version Handling (v7 transfer vs v6 stale shift change)
5. Field Ownership Invariant & Office Transfer Conflict Protocol
6. Nightly Reconciliation & Safety Brake Trigger (40% missing -> Halt & Approval)
7. Transactional Outbox Pattern & Crash Recovery Demonstration
8. Multi-Tenant Isolation Protection
9. Prometheus 3 AM Operational Metrics
"""

import asyncio
import io
import os
import sys
import time
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.main import app
from app.core.config import settings
from app.core.database import sync_engine
from app.core.auth import create_access_token
from app.workers.outbox_publisher import OutboxPublisher
from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.api.v1.mock_downstream import reset_mock_downstream, _CANCELLED_BOOKINGS
from tests.test_safety_brake import generate_roster_csv


def print_header(title: str):
    print("\n" + "=" * 78)
    print(f" {title.upper()}")
    print("=" * 78)


def print_step(step: str, detail: str = ""):
    print(f"\n[STEP] {step}")
    if detail:
        print(f"       -> {detail}")


async def run_demo():
    print_header("MoveInSync HR Roster Sync: Live System Demonstration")
    print("Architecture: Python 3.13, FastAPI, SQLAlchemy 2 Async, PostgreSQL 16")
    print(f"Environment : {settings.app_env.upper()} | DB: PostgreSQL 16 on port 5432")

    # Clean demo records for reproducible execution
    with sync_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE outbox_events, idempotency_keys, reconciliation_runs CASCADE;"))
        conn.execute(text("DELETE FROM employees WHERE employee_id LIKE 'DEMO-%';"))
        conn.commit()

    # Tokens
    hr_token = create_access_token("hr-svc", "tenant-alpha", ["hr_integration"])
    transport_token = create_access_token("transport-admin-1", "tenant-alpha", ["transport_admin"])
    operator_token = create_access_token("operator-1", "tenant-alpha", ["operator", "transport_admin", "hr_integration"])
    tenant_beta_token = create_access_token("hr-svc-beta", "tenant-beta", ["hr_integration"])

    hr_headers = {"Authorization": f"Bearer {hr_token}"}
    trans_headers = {"Authorization": f"Bearer {transport_token}"}
    op_headers = {"Authorization": f"Bearer {operator_token}"}
    beta_headers = {"Authorization": f"Bearer {tenant_beta_token}"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:

        # ---------------------------------------------------------------------
        # 1. Health & Readiness
        # ---------------------------------------------------------------------
        print_header("1. Health & Readiness Verification")
        res_health = await client.get("/healthz")
        res_ready = await client.get("/readyz")
        print_step("Liveness Probe (/healthz)", f"Status {res_health.status_code}: {res_health.json()}")
        print_step("Readiness Probe (/readyz)", f"Status {res_ready.status_code}: {res_ready.json()}")

        # ---------------------------------------------------------------------
        # 2. Boundary Validation (Mixed Batch)
        # ---------------------------------------------------------------------
        print_header("2. Boundary Validation: Mixed Valid & Invalid Batch")
        print("Constraint: One invalid record must never abort an otherwise valid batch.")

        batch_payload = {
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": "DEMO-EMP-001",
                    "source_version": 1,
                    "first_name": "Vikram",
                    "last_name": "Malhotra",
                    "email": "vikram.m@moveinsync.com",
                    "phone": "+919876500001",
                    "office_code": "BLR-CAMPUS-1",
                },
                {
                    "event_type": "CREATE",
                    "employee_id": "DEMO-EMP-BAD",
                    "source_version": 1,
                    "first_name": "Malformed",
                    "last_name": "Record",
                    "email": "invalid-email-no-domain",  # Violation!
                    "phone": "9876500002",               # Non-E.164!
                    "office_code": "UNKNOWN-OFFICE",     # Unregistered office!
                },
                {
                    "event_type": "CREATE",
                    "employee_id": "DEMO-EMP-002",
                    "source_version": 1,
                    "first_name": "Ananya",
                    "last_name": "Pandey",
                    "email": "ananya.p@moveinsync.com",
                    "phone": "+919876500003",
                    "office_code": "BLR-CAMPUS-1",
                },
            ]
        }

        res_batch = await client.post("/api/v1/roster/events", json=batch_payload, headers=hr_headers)
        data = res_batch.json()
        print_step("Batch Ingestion Result", f"Accepted: {data['accepted_count']} | Rejected: {data['rejected_count']}")
        print(f"       Per-Record Errors Reported: {data['errors']}")
        assert data["accepted_count"] == 2
        assert data["rejected_count"] == 1

        # ---------------------------------------------------------------------
        # 3. Idempotent Replays & Conflict Detection
        # ---------------------------------------------------------------------
        print_header("3. Idempotency Key Semantics & Conflict Detection")
        idemp_key = "DEMO-KEY-IDEMP-001"
        idemp_headers = {**hr_headers, "X-Idempotency-Key": idemp_key}

        idemp_payload = {
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": "DEMO-EMP-003",
                    "source_version": 1,
                    "first_name": "Sunil",
                    "last_name": "Gavaskar",
                    "email": "sunil.g@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        }

        # First request
        res_idemp1 = await client.post("/api/v1/roster/events", json=idemp_payload, headers=idemp_headers)
        print_step("Initial Execution with Idempotency Key", f"HTTP {res_idemp1.status_code}: Accepted {res_idemp1.json()['accepted_count']}")

        # Exact replay
        res_idemp2 = await client.post("/api/v1/roster/events", json=idemp_payload, headers=idemp_headers)
        print_step("Exact Replay with Same Key", f"HTTP {res_idemp2.status_code}: Returned cached response identically.")
        assert res_idemp1.json() == res_idemp2.json()

        # Replay with differing payload
        conflicting_payload = {
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": "DEMO-EMP-003",
                    "source_version": 1,
                    "first_name": "Sunil-Modified",
                    "last_name": "Gavaskar",
                    "email": "different@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        }
        res_conflict = await client.post("/api/v1/roster/events", json=conflicting_payload, headers=idemp_headers)
        print_step("Conflicting Replay Attempt (Same Key, Different Payload)", f"HTTP {res_conflict.status_code} Conflict: {res_conflict.json()['error']['message']}")
        assert res_conflict.status_code == 409

        # ---------------------------------------------------------------------
        # 4. Out-of-Order Version Ingestion
        # ---------------------------------------------------------------------
        print_header("4. Out-of-Order Version Handling (Official PDF Example)")
        print("Scenario: Transfer (v7) arrives BEFORE Shift Change (v6).")

        # Ingest v7 first
        v7_event = {
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": "DEMO-EMP-001",
                    "source_version": 7,
                    "first_name": "Vikram",
                    "last_name": "Malhotra",
                    "email": "vikram.m@moveinsync.com",
                    "office_code": "HYD-CAMPUS-2",
                }
            ]
        }
        res_v7 = await client.post("/api/v1/roster/events", json=v7_event, headers=hr_headers)
        print_step("Version 7 Arrives (Office transfer to HYD-CAMPUS-2)", f"HTTP {res_v7.status_code}: Accepted. Version updated to 7.")

        # Ingest v6 later
        v6_event = {
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": "DEMO-EMP-001",
                    "source_version": 6,
                    "first_name": "Vikram",
                    "last_name": "Malhotra",
                    "email": "vikram.m@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        }
        res_v6 = await client.post("/api/v1/roster/events", json=v6_event, headers=hr_headers)
        print_step("Version 6 Arrives LATER (Old shift in BLR-CAMPUS-1)", f"HTTP {res_v6.status_code}: Stale Ignored! Outcome: {res_v6.json()['results'][0]['message']}")
        assert res_v6.json()["stale_count"] == 1

        profile = (await client.get("/api/v1/employees/DEMO-EMP-001", headers=hr_headers)).json()
        print_step("Verified Employee Final State", f"Office: {profile['office_code']} (HYD) | Version: {profile['source_version']} (v7)")
        assert profile["office_code"] == "HYD-CAMPUS-2"
        assert profile["source_version"] == 7

        # ---------------------------------------------------------------------
        # 5. Field Ownership & Transfer Conflict Protocol
        # ---------------------------------------------------------------------
        print_header("5. Field Ownership Invariants & Transport Review Flagging")
        # Transport Admin assigns pickup point and special assistance
        await client.put(
            "/api/v1/transport/employees/DEMO-EMP-002",
            json={
                "preferred_pickup_point_id": "STOP-BLR-INDIRANAGAR-07",
                "special_assistance_required": True,
                "special_assistance_notes": "Visually impaired, assist to cab door",
            },
            headers=trans_headers,
        )
        print_step("Transport Admin Configures Transport-Owned Attributes", "Assigned: STOP-BLR-INDIRANAGAR-07, Special Assistance: True")

        # HR transfers employee to Hyderabad
        await client.post(
            "/api/v1/roster/events",
            json={
                "events": [
                    {
                        "event_type": "UPDATE",
                        "employee_id": "DEMO-EMP-002",
                        "source_version": 2,
                        "first_name": "Ananya",
                        "last_name": "Pandey-Kapoor",
                        "email": "ananya.kapoor@moveinsync.com",
                        "office_code": "HYD-CAMPUS-2",
                    }
                ]
            },
            headers=hr_headers,
        )
        print_step("HR Updates Core Fields & Transfers to HYD-CAMPUS-2", "HR modified name, email, and office.")

        profile_p2 = (await client.get("/api/v1/employees/DEMO-EMP-002", headers=hr_headers)).json()
        print_step("Field Ownership Verification", (
            f"HR Fields Updated -> Name: {profile_p2['last_name']}, Email: {profile_p2['email']}\n"
            f"       Transport Fields Intact -> Pickup: {profile_p2['preferred_pickup_point_id']}, Assistance: {profile_p2['special_assistance_required']}\n"
            f"       Transfer Conflict Review -> transport_review_required: {profile_p2['transport_review_required']}"
        ))
        assert profile_p2["preferred_pickup_point_id"] == "STOP-BLR-INDIRANAGAR-07"
        assert profile_p2["special_assistance_required"] is True
        assert profile_p2["transport_review_required"] is True

        # ---------------------------------------------------------------------
        # 6. Nightly Reconciliation & Safety Brake
        # ---------------------------------------------------------------------
        print_header("6. Nightly Reconciliation & Safety Brake Quarantine")
        # Seed 20 active employees
        seed_20 = [
            {
                "event_type": "CREATE",
                "employee_id": f"DEMO-RECON-{i:02d}",
                "source_version": 1,
                "first_name": f"User{i}",
                "last_name": "Recon",
                "email": f"recon{i}@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
            for i in range(1, 21)
        ]
        await client.post("/api/v1/roster/events", json={"events": seed_20}, headers=hr_headers)

        # Upload corrupted export missing 40% (only 12 of 20 active present)
        recon_rows = [
            {
                "employee_id": f"DEMO-RECON-{i:02d}",
                "first_name": f"User{i}",
                "last_name": "Recon",
                "email": f"recon{i}@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 2,
            }
            for i in range(1, 13)
        ]
        csv_file = generate_roster_csv(recon_rows)
        upload_res = await client.post(
            "/api/v1/reconciliation/upload",
            files={"file": ("nightly_corrupted.csv", csv_file, "text/csv")},
            headers=hr_headers,
        )
        recon_data = upload_res.json()
        print_step("Nightly Snapshot Uploaded (40% missing)", f"Status: {recon_data['status']} | Safety Brake: TRIGGERED")
        print(f"       Missing Active Records: {recon_data['missing_active_records']} ({recon_data['missing_percentage']}%) | Deactivated: {recon_data['records_deactivated']}")
        assert recon_data["status"] == "SAFETY_BRAKE_TRIGGERED"
        assert recon_data["records_deactivated"] == 0

        # Operator inspects report and overrides
        run_id = recon_data["run_id"]
        report = (await client.get(f"/api/v1/reconciliation/{run_id}/report", headers=op_headers)).json()
        print_step("Operator Inspection Report", f"Recommendation: {report['recommendation']}")

        approve_res = await client.post(
            f"/api/v1/reconciliation/{run_id}/approve",
            json={"operator_id": "operator-sec-01", "reason": "Restructuring verified with HR VP"},
            headers=op_headers,
        )
        print_step("Operator Approves Override", f"Status: {approve_res.json()['status']} | Deactivated: {approve_res.json()['records_deactivated']} employees.")

        # ---------------------------------------------------------------------
        # 7. Outbox Pattern & Crash Recovery
        # ---------------------------------------------------------------------
        print_header("7. Transactional Outbox Pattern & Crash Recovery Simulation")
        reset_mock_downstream()

        # Deactivate DEMO-EMP-001
        await client.post(
            "/api/v1/roster/events",
            json={
                "events": [
                    {
                        "event_type": "DEACTIVATE",
                        "employee_id": "DEMO-EMP-001",
                        "source_version": 8,
                        "first_name": "Vikram",
                        "last_name": "Malhotra",
                        "email": "vikram.m@moveinsync.com",
                        "office_code": "HYD-CAMPUS-2",
                    }
                ]
            },
            headers=hr_headers,
        )
        print_step("Employee Deactivated & Outbox Committed in Same Transaction", "Database updated; Outbox event committed as PENDING.")

        # Simulate Crash: Publisher dies immediately.
        print_step("SIMULATING PROCESS CRASH", "Worker killed before event publication. Downstream cab service has not received event.")
        assert len(_CANCELLED_BOOKINGS) == 0

        # Simulate Restart: New publisher initializes and resumes
        print_step("SIMULATING SERVICE RESTART", "Fresh publisher instance started on reboot. Polling outbox table with SKIP LOCKED...")
        publisher = OutboxPublisher(batch_size=50, booking_adapter=DownstreamBookingAdapter(app_dispatch=app))
        published_count = await publisher.poll_and_publish_batch()
        print_step("Crash Recovery Succeeded", f"Events Published: {published_count} | Downstream Cab Service: {list(_CANCELLED_BOOKINGS.keys())}")
        assert len(_CANCELLED_BOOKINGS) > 0

        # ---------------------------------------------------------------------
        # 8. Multi-Tenant Isolation
        # ---------------------------------------------------------------------
        print_header("8. Multi-Tenant Data Isolation Guard")
        cross_res = await client.get("/api/v1/employees/DEMO-EMP-002", headers=beta_headers)
        print_step("Tenant Beta Attempts Access to Tenant Alpha's Employee", f"HTTP {cross_res.status_code} ({'BLOCKED - Access Denied' if cross_res.status_code == 404 else 'LEAKED'})")
        assert cross_res.status_code == 404

        # ---------------------------------------------------------------------
        # 9. Prometheus Metrics Verification
        # ---------------------------------------------------------------------
        print_header("9. Prometheus 3 AM Operational Observability")
        metrics_res = await client.get("/metrics")
        print_step("Prometheus /metrics Endpoint", f"HTTP {metrics_res.status_code} | Bytes: {len(metrics_res.content)}")
        for line in metrics_res.text.splitlines():
            if line.startswith("roster_") and not line.startswith("roster_reconciliation_duration_seconds_bucket"):
                print(f"       {line}")

    print_header("Live Demonstration Completed Successfully")
    print("All functional requirements, safety brakes, idempotency invariants, and crash recovery verified.\n")


if __name__ == "__main__":
    asyncio.run(run_demo())
