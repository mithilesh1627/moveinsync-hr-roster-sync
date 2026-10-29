"""Mock Downstream Cab Booking Service for Testing & End-to-End Simulation"""

from typing import Dict, Any, Optional
from fastapi import APIRouter, Header, Response, status
from pydantic import BaseModel

router = APIRouter(prefix="/mock-downstream", tags=["Simulated Downstream"])

# In-memory storage for cancelled bookings to verify downstream idempotency
_CANCELLED_BOOKINGS: Dict[str, Dict[str, Any]] = {}
_SIMULATE_FAILURES: bool = False
_FAIL_UNTIL_ATTEMPT: Dict[str, int] = {}
_ATTEMPT_COUNTERS: Dict[str, int] = {}


class BookingCancellationRequest(BaseModel):
    tenant_id: str
    employee_id: str
    cancellation_event_id: str
    reason: str


@router.post("/cancel-booking")
async def mock_cancel_cab_booking(
    request: BookingCancellationRequest,
    response: Response,
    x_simulate_failure: Optional[str] = Header(None, alias="X-Simulate-Failure"),
):
    """Simulate downstream cab booking cancellation and seat freeing.
    
    Idempotent: Re-calling with same cancellation_event_id is a safe no-op.
    """
    event_id = request.cancellation_event_id
    _ATTEMPT_COUNTERS[event_id] = _ATTEMPT_COUNTERS.get(event_id, 0) + 1
    current_attempt = _ATTEMPT_COUNTERS[event_id]

    # Simulation hook: Fail until attempt N
    target_fail_attempt = _FAIL_UNTIL_ATTEMPT.get(event_id, 0)
    if _SIMULATE_FAILURES or (x_simulate_failure and x_simulate_failure.lower() == "true") or current_attempt <= target_fail_attempt:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"error": "Downstream cab booking service temporarily unavailable", "attempt": current_attempt}

    # Idempotent check
    if event_id in _CANCELLED_BOOKINGS:
        return {
            "status": "success",
            "already_cancelled": True,
            "message": "Cab booking already cancelled for this deactivation event (idempotent no-op)",
            "details": _CANCELLED_BOOKINGS[event_id],
        }

    # Record cancellation
    record = {
        "tenant_id": request.tenant_id,
        "employee_id": request.employee_id,
        "cancelled_rides_count": 3,
        "freed_seats_count": 3,
        "event_id": event_id,
    }
    _CANCELLED_BOOKINGS[event_id] = record

    return {
        "status": "success",
        "already_cancelled": False,
        "message": f"Successfully cancelled future rides and freed seats for employee {request.employee_id}",
        "details": record,
    }


def reset_mock_downstream():
    """Helper to reset mock downstream state between tests."""
    global _SIMULATE_FAILURES
    _CANCELLED_BOOKINGS.clear()
    _FAIL_UNTIL_ATTEMPT.clear()
    _ATTEMPT_COUNTERS.clear()
    _SIMULATE_FAILURES = False


def set_failure_simulation(fail_until_attempt: int, event_id: str):
    _FAIL_UNTIL_ATTEMPT[event_id] = fail_until_attempt
