"""Canonical Hashing Utility

Computes fast, deterministic record hashes for diffing and idempotency payloads.
Uses xxHash64 for high-throughput reconciliation (millions of records/sec) with SHA-256 fallback.
"""

import hashlib
import json
from typing import Any, Dict

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False


def compute_canonical_record_hash(
    employee_id: str,
    first_name: str,
    last_name: str,
    email: str,
    phone: str | None,
    office_code: str,
    department: str | None,
    manager_id: str | None,
    status: str,
) -> str:
    """Compute deterministic canonical hash for HR-owned employee fields.
    
    Normalizes string values (stripping whitespace, lowercase emails)
    so semantically equivalent records yield identical hashes.
    """
    normalized_parts = [
        str(employee_id).strip(),
        str(first_name).strip().lower(),
        str(last_name).strip().lower(),
        str(email).strip().lower(),
        (str(phone).strip() if phone else ""),
        str(office_code).strip().upper(),
        (str(department).strip().lower() if department else ""),
        (str(manager_id).strip() if manager_id else ""),
        str(status).strip().upper(),
    ]
    raw_key = "|".join(normalized_parts)
    
    if HAS_XXHASH:
        return xxhash.xxh64_hexdigest(raw_key.encode("utf-8"))
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def compute_payload_hash(payload: Any) -> str:
    """Compute deterministic SHA-256 hash for idempotency payloads."""
    if isinstance(payload, bytes):
        raw_bytes = payload
    elif isinstance(payload, str):
        raw_bytes = payload.encode("utf-8")
    else:
        # Canonical JSON string with sorted keys and no whitespace
        raw_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        
    return hashlib.sha256(raw_bytes).hexdigest()
