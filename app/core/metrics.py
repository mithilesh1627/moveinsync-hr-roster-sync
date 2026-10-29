"""Prometheus Operational Metrics Module

Exposes the critical counters, histograms, and gauges an on-call engineer needs at 3 AM:
- Ingestion rates, rejections, stale drops, duplicate no-ops
- Reconciliation elapsed time and streaming throughput
- Safety brake activations
- Outbox queue depth, retries, and dead-letter count
- Downstream cab cancellation results
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Ingestion Events Counter
ROSTER_EVENTS_TOTAL = Counter(
    "roster_events_total",
    "Total HR roster events processed, labeled by tenant and outcome",
    ["tenant", "status"],  # status: accepted, rejected, stale_ignored, duplicate_noop
)

# Reconciliation Duration & Outcome Histogram
RECONCILIATION_DURATION_SECONDS = Histogram(
    "roster_reconciliation_duration_seconds",
    "Time taken for nightly snapshot reconciliation run in seconds",
    ["tenant", "status"],  # status: completed, safety_brake_triggered, aborted, failed
    buckets=(1.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0),
)

# Safety Brake Activations Counter
SAFETY_BRAKE_TRIGGERS_TOTAL = Counter(
    "roster_safety_brake_triggers_total",
    "Total times the safety brake halted reconciliation due to >= 30% missing active employees",
    ["tenant"],
)

# Outbox Pending Queue Depth Gauge
OUTBOX_QUEUE_DEPTH = Gauge(
    "roster_outbox_queue_depth",
    "Current number of pending or retrying events in the transactional outbox",
    ["tenant"],
)

# Outbox Publisher Retries Counter
OUTBOX_PUBLISH_RETRIES_TOTAL = Counter(
    "roster_outbox_publish_retries_total",
    "Total outbox event delivery attempts that failed and were rescheduled for retry",
    ["tenant"],
)

# Outbox Dead-Letter Queue (DLQ) Counter
OUTBOX_DLQ_TOTAL = Counter(
    "roster_outbox_dlq_total",
    "Total outbox events moved to Dead-Letter state after exhausting all retries",
    ["tenant"],
)

# Downstream Cab Cancellation Side-Effects Counter
DOWNSTREAM_BOOKING_CANCELLATIONS_TOTAL = Counter(
    "roster_booking_cancellations_total",
    "Total downstream cab booking cancellations triggered on employee deactivation",
    ["status"],  # success, failure
)


def get_prometheus_metrics() -> bytes:
    """Render latest metrics in Prometheus text exposition format."""
    return generate_latest()
