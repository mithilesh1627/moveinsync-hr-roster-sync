# MoveInSync HR Roster Sync — 3 AM Operations Runbook & Incident Response

## 1. Quick Reference & System Health Endpoints

| Resource | Endpoint | Expected Response | Description |
| :--- | :--- | :--- | :--- |
| **Liveness Probe** | `GET /api/v1/health/live` | `{"status": "alive"}` (200 OK) | Confirms FastAPI event loop is responsive. |
| **Readiness Probe** | `GET /api/v1/health/ready` | `{"status": "ready", "database": "connected"}` (200 OK) | Verifies PostgreSQL connection pool is healthy. |
| **Prometheus Metrics**| `GET /api/v1/health/metrics` | Prometheus Exposition Format | Exposes operational counters, queue depths, latencies. |

---

## 2. Incident Playbooks

### Incident 1: Reconciliation Tripped Safety Brake (`NEEDS_APPROVAL`)

#### Alert Condition
Prometheus alert: `RECONCILIATION_SAFETY_BRAKE_TRIPPED_TOTAL > 0`  
Run status in `reconciliation_runs` is `NEEDS_APPROVAL`.

#### Symptoms & Business Impact
- Nightly reconciliation completed staging and set-diff, but detected that the percentage of missing active employees exceeded the configured safety threshold (default: $30.00\%$).
- **Destructive deactivations are automatically paused**. Cab bookings have **NOT** been cancelled yet.

#### Diagnostic Steps
1. Retrieve run details using operator credentials:
   ```bash
   curl -X GET "http://localhost:8000/api/v1/reconciliation/runs/<RUN_ID>" \
     -H "Authorization: Bearer <OPERATOR_JWT>"
   ```
2. Inspect the returned audit metrics:
   ```json
   {
     "run_id": "recon-tenant-alpha-12345",
     "status": "NEEDS_APPROVAL",
     "total_records_received": 140000,
     "missing_active_records": 60000,
     "missing_percentage": 30.01,
     "threshold_percent": 30.0
   }
   ```
3. Contact the tenant HR team to verify whether a mass layoff, organizational restructuring, or faulty export filter occurred:
   - **Case A: Faulty HR Export**: HR accidentally exported only the "Engineering" department, omitting 60,000 other employees.
   - **Case B: Legitimate Mass Departure**: Enterprise tenant executed an intended organizational transition.

#### Remediation
- **If Faulty HR Export (Case A)**:
  Reject the run to abort all deactivations:
  ```bash
  curl -X POST "http://localhost:8000/api/v1/reconciliation/runs/<RUN_ID>/reject" \
    -H "Authorization: Bearer <OPERATOR_JWT>"
  ```
  Ask HR to re-export the complete snapshot file.

- **If Legitimate Organizational Transition (Case B)**:
  Explicitly approve the run to resume chunked deactivations:
  ```bash
  curl -X POST "http://localhost:8000/api/v1/reconciliation/runs/<RUN_ID>/approve" \
    -H "Authorization: Bearer <OPERATOR_JWT>"
  ```

---

### Incident 2: Snapshot Quarantined (`QUARANTINED`)

#### Alert Condition
Run status is `QUARANTINED`.

#### Symptoms & Business Impact
- CSV snapshot contained malformed rows (unparseable email/phone) or duplicate employee IDs across chunk boundaries.
- Automatic processing is blocked to prevent corrupting database state.

#### Diagnostic Steps
1. Inspect the quarantine reason:
   ```bash
   curl -X GET "http://localhost:8000/api/v1/reconciliation/runs/<RUN_ID>" \
     -H "Authorization: Bearer <OPERATOR_JWT>"
   ```
   Check field `quarantine_reason`:
   ```json
   {
     "quarantine_flag": true,
     "quarantine_reason": "Snapshot contains 4 duplicate employee IDs and 2 malformed rows: [Row 45: Duplicate ID EMP-001; Row 89: Invalid RFC 5322 email]"
   }
   ```

#### Remediation
- Reject the run and request a cleansed CSV from the tenant administrator.
- If the operator determines that anomalies are non-critical, the operator can explicitly approve the run, which will synchronize valid rows while recording anomalies in audit logs.

---

### Incident 3: Outbox Queue Depth Spike & DLQ Accumulation

#### Alert Condition
- `OUTBOX_QUEUE_DEPTH > 1000` for $> 15\text{ minutes}$
- `OUTBOX_DLQ_TOTAL > 0`

#### Symptoms & Business Impact
- Downstream transport systems (cab allocation, route scheduling) are not receiving deactivation events. Employees deactivated in HR may still hold active cab seats.

#### Diagnostic Steps
1. Inspect the Dead-Letter Queue:
   ```bash
   curl -X GET "http://localhost:8000/api/v1/outbox/dlq?limit=20" \
     -H "Authorization: Bearer <OPERATOR_JWT>"
   ```
2. Inspect `last_error` on the returned items:
   - `HTTP 503: Fleet service undergoing maintenance`
   - `HTTP 504: Gateway timeout on cab-booking-service.internal`
3. Verify downstream fleet service health and network connectivity.

#### Remediation
1. Once the downstream fleet service has recovered:
2. Replay dead-lettered events individually:
   ```bash
   curl -X POST "http://localhost:8000/api/v1/outbox/dlq/<EVENT_ID>/retry" \
     -H "Authorization: Bearer <OPERATOR_JWT>"
   ```
3. Verify the event status resets to `PENDING` and is immediately published by background workers.

---

### Incident 4: Reconciliation Stuck in `APPROVING` Post-Crash

#### Alert Condition
Reconciliation run has been in `APPROVING` status for $> 30\text{ minutes}$ with no CPU activity.

#### Symptoms & Business Impact
- A background worker died mid-way through chunked deactivations (e.g., Kubernetes OOM kill, node reboot).
- Some batches of 1,000 were committed; remaining batches are pending.

#### Diagnostic Steps
1. Verify no active PostgreSQL advisory lock is held:
   ```sql
   SELECT pid, locktype, mode, granted 
   FROM pg_locks 
   WHERE locktype = 'advisory';
   ```
   If empty, the previous connection died and released the lock.

#### Remediation
1. Issue the approval request again:
   ```bash
   curl -X POST "http://localhost:8000/api/v1/reconciliation/runs/<RUN_ID>/approve" \
     -H "Authorization: Bearer <OPERATOR_JWT>"
   ```
2. The engine will acquire the advisory lock, identify remaining active missing employees, and resume deactivations from the durable checkpoint without duplicating already committed batches.

---

## 3. Recommended Prometheus Alerting Rules

```yaml
groups:
  - name: moveinsync_roster_alerts
    rules:
      - alert: ReconciliationSafetyBrakeTripped
        expr: increase(RECONCILIATION_SAFETY_BRAKE_TRIPPED_TOTAL[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "HR Reconciliation Safety Brake Tripped for tenant {{ $labels.tenant }}"
          description: "Nightly snapshot exceeded the 30% missing employee threshold. Automatic deactivation has halted."

      - alert: OutboxDeadLetterQueueAccumulation
        expr: increase(OUTBOX_DLQ_TOTAL[15m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Events moved to Outbox Dead-Letter Queue"
          description: "Outbox publisher exhausted retries for tenant {{ $labels.tenant }}. Downstream cab cancellation may be delayed."

      - alert: OutboxQueueLag
        expr: OUTBOX_QUEUE_DEPTH > 1000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Outbox publishing backlog exceeding 1,000 events"
          description: "Publishers are lagging behind event ingestion. Check downstream service latency."
```
