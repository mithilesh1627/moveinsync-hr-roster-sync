"""Unit & Integration Tests for Boundary Ingestion Validation (REQ-A1 to REQ-A5)"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_valid_phone_and_email_accepted(client: AsyncClient, hr_headers: dict):
    """Test that records with valid E.164 phone and RFC 5322 email are accepted."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-VAL-001",
                "source_version": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@moveinsync.com",
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
            }
        ]
    }
    response = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["accepted_count"] == 1
    assert data["rejected_count"] == 0
    assert len(data["errors"]) == 0


@pytest.mark.asyncio
async def test_invalid_phone_format_rejected(client: AsyncClient, hr_headers: dict):
    """Test that malformed phone number (non-E.164) is rejected with structured error."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-VAL-002",
                "source_version": 1,
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane.doe@moveinsync.com",
                "phone": "9876543210",  # Missing leading '+'
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    response = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["accepted_count"] == 0
    assert data["rejected_count"] == 1
    assert len(data["errors"]) == 1
    assert data["errors"][0]["employee_id"] == "EMP-VAL-002"
    assert "phone" in data["errors"][0]["field"]


@pytest.mark.asyncio
async def test_unknown_office_code_rejected(client: AsyncClient, hr_headers: dict):
    """Test that unregistered office code is rejected."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-VAL-003",
                "source_version": 1,
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice.smith@moveinsync.com",
                "phone": "+919876543211",
                "office_code": "UNKNOWN-OFFICE-99",
            }
        ]
    }
    response = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["accepted_count"] == 0
    assert data["rejected_count"] == 1
    assert data["errors"][0]["error_code"] == "UNKNOWN_OFFICE_CODE"


@pytest.mark.asyncio
async def test_mixed_batch_one_bad_record_does_not_fail_valid_records(client: AsyncClient, hr_headers: dict):
    """CRITICAL REQUIREMENT: One invalid record in a batch must not abort valid records."""
    payload = {
        "events": [
            {
                # Valid record 1
                "event_type": "CREATE",
                "employee_id": "EMP-MIX-001",
                "source_version": 1,
                "first_name": "Valid",
                "last_name": "One",
                "email": "valid.one@moveinsync.com",
                "phone": "+919111111111",
                "office_code": "BLR-CAMPUS-1",
            },
            {
                # Bad record (invalid email format)
                "event_type": "CREATE",
                "employee_id": "EMP-MIX-BAD",
                "source_version": 1,
                "first_name": "Bad",
                "last_name": "Record",
                "email": "not-an-email",
                "phone": "+919222222222",
                "office_code": "BLR-CAMPUS-1",
            },
            {
                # Valid record 2
                "event_type": "CREATE",
                "employee_id": "EMP-MIX-002",
                "source_version": 1,
                "first_name": "Valid",
                "last_name": "Two",
                "email": "valid.two@moveinsync.com",
                "phone": "+919333333333",
                "office_code": "BLR-CAMPUS-1",
            },
        ]
    }
    response = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert response.status_code == 200
    data = response.json()

    assert data["total_received"] == 3
    assert data["accepted_count"] == 2
    assert data["rejected_count"] == 1

    # Verify error report
    assert len(data["errors"]) == 1
    assert data["errors"][0]["record_index"] == 1
    assert data["errors"][0]["employee_id"] == "EMP-MIX-BAD"

    # Verify that the two valid employees actually exist in the database
    emp1_res = await client.get("/api/v1/employees/EMP-MIX-001", headers=hr_headers)
    assert emp1_res.status_code == 200
    assert emp1_res.json()["employee_id"] == "EMP-MIX-001"

    emp2_res = await client.get("/api/v1/employees/EMP-MIX-002", headers=hr_headers)
    assert emp2_res.status_code == 200
    assert emp2_res.json()["employee_id"] == "EMP-MIX-002"

    # Verify that the bad employee does NOT exist
    bad_res = await client.get("/api/v1/employees/EMP-MIX-BAD", headers=hr_headers)
    assert bad_res.status_code == 404
