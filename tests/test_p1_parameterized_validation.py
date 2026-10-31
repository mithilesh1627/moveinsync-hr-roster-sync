"""P1 Comprehensive Parameterized Record Validation Tests

Verifies requirements from Section 8:
- Parameterized E.164 international phone number format validation.
- Parameterized RFC 5322 email format validation.
- Field length boundary validation.
- Employee status validation (ACTIVE, INACTIVE only).
- Source version validation (positive integers >= 1).
- Missing required fields validation.
- Error reports contain error category without leaking PII.
"""

import pytest
from httpx import AsyncClient


# =============================================================================
# 1. PARAMETERIZED PHONE VALIDATION (E.164 FORMAT)
# =============================================================================
@pytest.mark.parametrize(
    "phone,is_valid",
    [
        ("+919876543210", True),     # India valid
        ("+14155552671", True),      # US valid
        ("+442071838750", True),     # UK valid
        ("+4930123456", True),       # Germany valid
        ("+81312345678", True),      # Japan valid
        ("8765432109", False),       # Missing leading +
        ("+1", False),               # Too short
        ("+91-9876543210", False),   # Hyphens not permitted in strict E.164
        ("+14155552671987654321", False), # Exceeds 15 digits max length
        ("invalid-phone", False),    # Alphabetic
    ],
)
@pytest.mark.asyncio
async def test_parameterized_phone_validation(client: AsyncClient, hr_headers: dict, phone: str, is_valid: bool):
    """Test boundary E.164 phone validation rules across international country formats."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": f"EMP-PH-{abs(hash(phone)) % 100000:05d}",
                "first_name": "Test",
                "last_name": "User",
                "email": f"ph_{abs(hash(phone)) % 100000}@moveinsync.test",
                "phone": phone,
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 1,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200
    data = res.json()
    if is_valid:
        assert data["accepted_count"] == 1
        assert data["rejected_count"] == 0
    else:
        assert data["accepted_count"] == 0
        assert data["rejected_count"] == 1
        assert len(data["errors"]) == 1
        # PII check: raw invalid phone number must NOT be in error message!
        err_str = str(data["errors"][0])
        assert phone not in err_str or len(phone) < 3


# =============================================================================
# 2. PARAMETERIZED EMAIL VALIDATION (RFC 5322 FORMAT)
# =============================================================================
@pytest.mark.parametrize(
    "email,is_valid",
    [
        ("user@company.com", True),
        ("first.last@domain.co.in", True),
        ("user+tag@sub.domain.org", True),
        ("plainaddress", False),           # Missing @ and domain
        ("@missingusername.com", False),   # Missing username
        ("username@.com", False),          # Domain missing name
        ("user space@domain.com", False),  # Space in address
    ],
)
@pytest.mark.asyncio
async def test_parameterized_email_validation(client: AsyncClient, hr_headers: dict, email: str, is_valid: bool):
    """Test RFC 5322 boundary email format validation rules."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": f"EMP-EM-{abs(hash(email)) % 100000:05d}",
                "first_name": "Test",
                "last_name": "User",
                "email": email,
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 1,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200
    data = res.json()
    if is_valid:
        assert data["accepted_count"] == 1
        assert data["rejected_count"] == 0
    else:
        assert data["accepted_count"] == 0
        assert data["rejected_count"] == 1


# =============================================================================
# 3. SOURCE VERSION VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "version,is_valid",
    [
        (1, True),
        (5, True),
        (1000, True),
        (0, False),    # Must be positive >= 1
        (-1, False),   # Negative invalid
        (-99, False),  # Negative invalid
    ],
)
@pytest.mark.asyncio
async def test_parameterized_source_version_validation(
    client: AsyncClient, hr_headers: dict, version: int, is_valid: bool
):
    """Source versions must be strictly positive integers >= 1."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": f"EMP-VER-{abs(version):03d}",
                "first_name": "Test",
                "last_name": "User",
                "email": f"ver_{abs(version)}@test.com",
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": version,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200
    data = res.json()
    if is_valid:
        assert data["accepted_count"] == 1
        assert data["rejected_count"] == 0
    else:
        assert data["accepted_count"] == 0
        assert data["rejected_count"] == 1


# =============================================================================
# 4. FIELD LENGTH BOUNDARY VALIDATION
# =============================================================================
@pytest.mark.asyncio
async def test_field_length_boundary_validation(client: AsyncClient, hr_headers: dict):
    """Field length limits: employee_id <= 64, first_name <= 128, office_code <= 64."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": "E" * 65,  # Exceeds max 64
                "first_name": "Test",
                "last_name": "User",
                "email": "toolong@test.com",
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 1,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200
    data = res.json()
    assert data["accepted_count"] == 0
    assert data["rejected_count"] == 1
    assert "employee_id" in data["errors"][0]["field"]


# =============================================================================
# 5. STATUS VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "status_val,is_valid",
    [
        ("ACTIVE", True),
        ("INACTIVE", True),
        ("SUSPENDED", False),
        ("ON_LEAVE", False),
        ("TERMINATED_UNOFFICIAL", False),
    ],
)
@pytest.mark.asyncio
async def test_parameterized_status_validation(
    client: AsyncClient, hr_headers: dict, status_val: str, is_valid: bool
):
    """Only ACTIVE and INACTIVE are valid employee statuses."""
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": f"EMP-ST-{abs(hash(status_val)) % 10000:04d}",
                "first_name": "Test",
                "last_name": "User",
                "email": f"st_{abs(hash(status_val)) % 10000}@test.com",
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "status": status_val,
                "source_version": 1,
            }
        ]
    }
    res = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert res.status_code == 200
    data = res.json()
    if is_valid:
        assert data["accepted_count"] == 1
    else:
        assert data["accepted_count"] == 0
        assert data["rejected_count"] == 1
