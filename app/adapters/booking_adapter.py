"""Downstream Cab Booking Adapter

Integrates with MoveInSync's Cab Booking & Seat Allocation microservices.
When an employee is deactivated, this adapter cancels all scheduled cab rides
and frees reserved vehicle seats.
Ensures consumer-side idempotency: repeated cancellations are safe no-ops.
"""

from typing import Dict, Any, Optional
import httpx

from app.core.config import settings
from app.core.metrics import DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL
from app.core.logging import logger


class DownstreamBookingAdapter:
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 5.0,
        app_dispatch: Optional[Any] = None,
    ):
        self.base_url = base_url or settings.downstream_booking_service_url
        self.timeout = timeout
        self.app_dispatch = app_dispatch

    async def cancel_future_bookings(
        self,
        tenant_id: str,
        employee_id: str,
        event_id: str,
    ) -> Dict[str, Any]:
        """Cancel future cab bookings and free seats for a deactivated employee.
        
        Idempotent: passing the same event_id produces identical success without side-effects.
        """
        payload = {
            "tenant_id": tenant_id,
            "employee_id": employee_id,
            "cancellation_event_id": event_id,
            "reason": "EMPLOYEE_DEACTIVATED",
        }

        # If app_dispatch is provided (e.g. in test environment), dispatch via ASGI transport
        if self.app_dispatch:
            transport = httpx.ASGITransport(app=self.app_dispatch)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post("/api/v1/mock-downstream/cancel-booking", json=payload)
                if response.status_code in (200, 201, 204):
                    DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL.labels(status="success").inc()
                    return response.json() if response.content else {"status": "success"}
                DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL.labels(status="failure").inc()
                raise RuntimeError(
                    f"Downstream booking cancellation failed with status {response.status_code}: {response.text}"
                )

        # Production external HTTP call
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(self.base_url, json=payload)
                if response.status_code in (200, 201, 204):
                    DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL.labels(status="success").inc()
                    return response.json() if response.content else {"status": "success"}

                DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL.labels(status="failure").inc()
                raise RuntimeError(
                    f"Downstream booking cancellation failed with status {response.status_code}: {response.text}"
                )
        except httpx.RequestError as exc:
            DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL.labels(status="failure").inc()
            raise RuntimeError(f"Downstream booking service network error: {str(exc)}") from exc
