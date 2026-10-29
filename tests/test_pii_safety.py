"""Privacy-Safe Logging & PII Redaction Verification Tests (REQ-A4)"""

import json
import logging
from app.core.logging import SafeJsonFormatter, sanitize_dict, sanitize_value


def test_sanitize_dict_redacts_sensitive_fields():
    """Verify that sensitive PII dictionary keys are strictly masked."""
    raw_payload = {
        "employee_id": "EMP-PII-01",
        "first_name": "SensitiveName",
        "last_name": "PrivateSurname",
        "email": "sensitive.person@moveinsync.com",
        "phone": "+919876543210",
        "office_code": "BLR-CAMPUS-1",
        "pickup_point": "Home Street 42",
        "coordinates": {"lat": 12.9716, "lon": 77.5946},
    }

    sanitized = sanitize_dict(raw_payload)

    # Identifiers and non-PII preserved
    assert sanitized["employee_id"] == "EMP-PII-01"
    assert sanitized["office_code"] == "BLR-CAMPUS-1"

    # PII strictly redacted
    assert sanitized["first_name"] == "[REDACTED_PII]"
    assert sanitized["last_name"] == "[REDACTED_PII]"
    assert sanitized["email"] == "[REDACTED_PII]"
    assert sanitized["phone"] == "[REDACTED_PII]"
    assert sanitized["pickup_point"] == "[REDACTED_PII]"
    assert sanitized["coordinates"] == "[REDACTED_PII]"


def test_safe_json_formatter_masks_inline_pii_in_log_messages():
    """Verify that unstructured strings containing phone numbers or emails are masked by the formatter."""
    formatter = SafeJsonFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Error contacting driver at +91-9876543210 or ops@moveinsync.com for employee EMP-123",
        args=(),
        exc_info=None,
    )
    setattr(record, "tenant_id", "tenant-alpha")
    setattr(record, "employee_id", "EMP-123")

    formatted = formatter.format(record)
    log_obj = json.loads(formatted)

    assert log_obj["tenant_id"] == "tenant-alpha"
    assert log_obj["employee_id"] == "EMP-123"

    # Raw email and phone must NOT appear in output message!
    assert "ops@moveinsync.com" not in log_obj["message"]
    assert "[REDACTED_EMAIL]" in log_obj["message"]
    assert "+91-9876543210" not in log_obj["message"]
    assert "[REDACTED_PHONE]" in log_obj["message"]
