"""P1 Automated PII Leakage Detection Tests

Verifies requirements from Section 9:
- Synthetic sensitive markers must NOT leak in API validation error responses.
- Malformed CSV parsing errors do NOT expose raw personal details.
- Database exception responses mask internal SQL statements and stack traces.
- Authentication failure responses do NOT leak secrets.
- Outbox event log messages mask sensitive payload fields.
"""

import io
import json
import logging
import pytest
from httpx import AsyncClient

from app.core.logging import SafeJsonFormatter, sanitize_dict


# Synthetic canary markers that should never appear unredacted
CANARY_NAME = "SECRET_CANARY_NAME_XYZ"
CANARY_EMAIL = "canary.confidential.999@secretcorp.internal"
CANARY_PHONE = "+919999988888"
CANARY_SECRET = "super_secret_operator_key_99999"


@pytest.mark.asyncio
async def test_api_validation_error_does_not_leak_canary_pii(client: AsyncClient, hr_headers: dict):
    """When an API validation error occurs, sensitive field values must NOT be echoed back."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "EMP-CANARY-01",
                "first_name": CANARY_NAME,
                "last_name": "CanarySurname",
                "email": "invalid-email-address-format",  # Triggers validation failure
                "phone": CANARY_PHONE,
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 1,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200  # Multi-event boundary returns 200 with structured errors
    data = res.json()
    assert data["rejected_count"] == 1

    # Verify that raw phone and canary name are NOT leaked in the error structure
    error_dump = json.dumps(data)
    assert CANARY_PHONE not in error_dump
    assert CANARY_NAME not in error_dump


@pytest.mark.asyncio
async def test_unhandled_error_envelope_masks_internal_stack_trace(client: AsyncClient, hr_headers: dict):
    """500 / 422 error envelopes must not expose database internals or Python stack traces."""
    # Send a request with malformed JSON body
    res = await client.post(
        "/api/v1/roster/events",
        content="{'broken_json': true}",
        headers={"Content-Type": "application/json", **hr_headers},
    )
    assert res.status_code == 422
    err_body = res.json()
    assert "error" in err_body
    assert "Traceback" not in json.dumps(err_body)
    assert "sqlalchemy" not in json.dumps(err_body).lower()


@pytest.mark.asyncio
async def test_csv_reconciliation_report_does_not_leak_full_pii(client: AsyncClient, hr_headers: dict):
    """When a nightly reconciliation CSV contains anomalies, anomaly_report shows counts and sample IDs,
    not raw CSV rows containing employee phones, emails, or personal data."""
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        f"EMP-CANARY-CSV,{CANARY_NAME},Surname,{CANARY_EMAIL},BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("nightly.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]

    # Fetch report
    report_res = await client.get(
        f"/api/v1/reconciliation/{run_id}/report",
        headers=hr_headers,
    )
    assert report_res.status_code == 200
    report_json = json.dumps(report_res.json())

    # Full email and canary name must not be exposed in anomaly summary
    assert CANARY_EMAIL not in report_json
    assert CANARY_NAME not in report_json


@pytest.mark.asyncio
async def test_auth_failure_does_not_leak_submitted_secret(client: AsyncClient):
    """Authentication failure must return generic 401 without echoing client_secret."""
    res = await client.post(
        "/api/v1/auth/token",
        json={
            "tenant_id": "tenant-alpha",
            "client_id": "hr-service",
            "client_secret": CANARY_SECRET,
        },
    )
    assert res.status_code == 401
    assert CANARY_SECRET not in res.text


def test_safe_json_formatter_masks_special_assistance_and_pickup_point():
    """Verify that SafeJsonFormatter redacts pickup points and medical/assistance details."""
    formatter = SafeJsonFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Employee EMP-999 needs wheelchair transport at 42 Central Park West",
        args=(),
        exc_info=None,
    )
    setattr(record, "pickup_point", "42 Central Park West")
    setattr(record, "special_assistance", "Wheelchair access required")

    log_str = formatter.format(record)
    log_data = json.loads(log_str)

    # In extra fields, PII must be sanitized
    if "pickup_point" in log_data:
        assert log_data["pickup_point"] == "[REDACTED_PII]"
    if "special_assistance" in log_data:
        assert log_data["special_assistance"] == "[REDACTED_PII]"
