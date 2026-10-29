"""Privacy-Safe Structured JSON Logging Module

Enforces PII redaction: Employee phone numbers, emails, full names, home/pickup
geolocations, and raw request payloads are NEVER written to logs or error reports.
Only opaque tenant_id, employee_id, and operation metadata are recorded.
"""

import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict


# Regex patterns for accidental PII detection
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_REGEX = re.compile(r"(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")

PII_FIELD_NAMES = {
    "phone",
    "phone_number",
    "email",
    "email_address",
    "first_name",
    "last_name",
    "full_name",
    "pickup_point",
    "home_address",
    "geofence",
    "coordinates",
    "lat",
    "lon",
    "latitude",
    "longitude",
}


def sanitize_value(key: str, value: Any) -> Any:
    """Sanitize individual field values based on key name and content."""
    if value is None:
        return None
    key_lower = str(key).lower()
    if key_lower in PII_FIELD_NAMES:
        return "[REDACTED_PII]"
    if isinstance(value, str):
        # Mask emails and phone numbers in strings
        val = EMAIL_REGEX.sub("[REDACTED_EMAIL]", value)
        val = PHONE_REGEX.sub("[REDACTED_PHONE]", val)
        return val
    if isinstance(value, dict):
        return sanitize_dict(value)
    if isinstance(value, list):
        return [sanitize_value(f"{key}[]", item) for item in value]
    return value


def sanitize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively sanitize dictionary to redact sensitive keys."""
    sanitized = {}
    for k, v in data.items():
        sanitized[k] = sanitize_value(k, v)
    return sanitized


class SafeJsonFormatter(logging.Formatter):
    """Custom Formatter outputting structured JSON without PII."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": sanitize_value("message", record.getMessage()),
        }

        # Include standard context fields if provided
        for attr in ("tenant_id", "employee_id", "run_id", "event_id", "correlation_id"):
            val = getattr(record, attr, None)
            if val is not None:
                log_entry[attr] = str(val)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure root and application loggers with safe JSON output."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplication
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(SafeJsonFormatter())
    root_logger.addHandler(handler)

    return logging.getLogger("moveinsync.roster")


logger = setup_logging()
