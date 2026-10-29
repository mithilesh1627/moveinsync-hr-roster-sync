# MoveInSync Backend Case Study 5: HR Roster Sync
## System Requirements & Traceability Matrix

### 1. Executive Summary & Problem Context
In enterprise corporate transport operations, MoveInSync integrates with client HR systems (e.g., Workday, SuccessFactors, Darwinbox) to maintain an accurate, real-time employee roster for daily commute routing, cab allocations, and seat reservations.
Roster updates originate from two complementary feeds:
1. **Near Real-Time HR Event Ingestion**: Pushed webhook/API events capturing joiners, department transfers, office relocations, shift changes, and employee exits.
2. **Full Nightly Snapshot Reconciliation**: Nightly full-roster export files containing up to 200,000 employee records, used to detect drift and self-heal any missed events.

In real-world distributed production environments, events frequently arrive duplicated, out of order, malformed, or cut short due to network interruptions. Furthermore, different business domains own different employee attributes: HR owns core employment attributes, while corporate transport administrators own transport logistics attributes (e.g., pickup points, special assistance). MoveInSync must guarantee roster correctness, tenant isolation, idempotency, safety brakes against mass wrongful deactivations, and reliable transactional event dissemination to downstream dispatch systems.

---

### 2. Detailed Functional Requirements

#### Domain A: Event Ingestion & Boundary Validation
- **REQ-A1 [Event Types]**: Support employee `CREATE`, `UPDATE`, and `DEACTIVATE` events with source system version and timestamp.
- **REQ-A2 [Per-Record Validation]**: Validate inputs at the API boundary. In a batch of $N$ events, each invalid record must yield an isolated, structured error response containing the record index, employee identifier, and specific validation violation. One bad record must never fail the entire batch.
- **REQ-A3 [Input Validation Scope]**: Validate tenant identity, employee identifiers, source version, required fields, office codes (against configured/valid tenant office master data), email address format (RFC 5322), phone number format (E.164 standard), event type enum, and payload size bounds.
- **REQ-A4 [PII Redaction in Logs]**: Strict data protection: never log personal identifiable information (PII) including employee names, email addresses, phone numbers, pickup geolocations, or raw request payloads. Structured logs and error responses must identify records solely by opaque tenant ID and employee ID.
- **REQ-A5 [Deterministic Error Semantics]**: Define deterministic handling for malformed data (HTTP 422 with structured field errors), unsupported event types, conflicting operations, and missing authentication/authorization credentials.

#### Domain B: Idempotency, Ordering & Versioning
- **REQ-B1 [Idempotency Key & Retention]**: Every mutation request accepts an idempotency key (header `X-Idempotency-Key` or payload `event_id`). Scope is scoped per `(tenant_id, idempotency_key)` with configurable retention (default 7 days). Persisted in PostgreSQL.
- **REQ-B2 [Replay Semantics]**: Replaying an event with the exact same idempotency key and same payload returns the cached result without repeating database state mutations or publishing duplicate outbox events. Replaying with the same key but a *different* payload must return HTTP 409 Conflict with a deterministic error.
- **REQ-B3 [Per-Employee Source Version Ordering]**: Maintain a monotonic source version (`source_version`) per tenant and employee. An incoming event with `source_version <= current_source_version` must be detected as stale or duplicate:
  - If identical version and identical payload: no-op idempotent success.
  - If lower version: ignored as stale with explicit audit record and counter increment.
- **REQ-B4 [Deterministic Tie-Breaking]**: When events arrive with identical `source_version`, tie-breaking must be strictly deterministic: compare `source_timestamp` (ISO-8601 UTC). If timestamps are also identical, compare cryptographic payload hash to detect identical duplicate vs conflicting concurrent event.
- **REQ-B5 [Concurrent Update Safety]**: Prevent race conditions where two concurrent updates for the same employee overwrite newer versions with older versions. Enforce via PostgreSQL row-level locking (`SELECT ... FOR UPDATE`) or conditional atomic updates (`UPDATE ... WHERE tenant_id = :t AND employee_id = :e AND source_version < :new_version`).

#### Domain C: Field Ownership & Transfer Conflict Resolution
- **REQ-C1 [Attribute Ownership Matrix]**:
  - **HR System Owns**: `first_name`, `last_name`, `email`, `manager_id`, `office_code`, `department`, `status` (`ACTIVE`/`INACTIVE`).
  - **Transport Admin Owns**: `preferred_pickup_point_id`, `pickup_geofence`, `special_assistance_required`, `special_assistance_notes`, `shift_timing`.
- **REQ-C2 [Ownership Invariant Enforcement]**: HR ingestion events (and nightly HR reconciliation) must NEVER overwrite transport-admin-owned fields. The persistence and domain logic must strictly preserve transport fields during HR updates.
- **REQ-C3 [Office Relocation Conflict Protocol]**: When an HR event relocates an employee to a different `office_code`:
  - HR attributes (including new `office_code`) are updated.
  - Existing `preferred_pickup_point_id` may belong to a pickup route for the old office. The system flags `transport_review_required = TRUE` and raises an operator alert if the existing pickup point does not map to the new office.
  - Transport-owned fields are preserved, but flagged for transport operator confirmation so the employee commute is not silently stranded.

#### Domain D: Nightly Reconciliation & Safety Brake
- **REQ-D1 [Large Scale Streaming Diff]**: Process nightly full-roster export files of up to 200,000 employees without loading the entire snapshot and database table into server memory simultaneously.
- **REQ-D2 [Bounded Memory Streaming & Hashing]**: Stream records in bounded chunks (e.g., 2,000 rows per chunk). Compute a deterministic canonical SHA-256 / xxHash64 hash over HR-owned fields (`employee_id`, `name`, `email`, `manager_id`, `office_code`, `status`). Diff against stored employee record hashes.
- **REQ-D3 [Malformed & Truncated Snapshot Detection]**: Validate export file integrity before applying destructive changes: check file completeness marker, parse headers, validate schema, detect intra-file duplicate employee IDs, and detect premature EOF.
- **REQ-D4 [Configurable Safety Brake]**: If a nightly export would cause deactivation of $\ge 30\%$ (default configurable threshold) of the tenant's current active workforce, the safety brake triggers:
  - The reconciliation run is immediately halted before performing deactivations.
  - An operator incident report is persisted with details: total current active count, snapshot active count, missing count, missing percentage, and snapshot metadata.
  - Destructive deactivations are quarantined until explicit human approval (`operator` role) via `/reconciliation/{run_id}/approve`.
- **REQ-D5 [Run Auditability & Idempotent Restart]**: Each reconciliation run has a unique `run_id`, tenant isolation, detailed status (`PENDING`, `STREAMING`, `SAFETY_BRAKE_TRIGGERED`, `APPROVED`, `COMPLETED`, `FAILED`), and audit summary (matched, created, updated, marked_stale, deactivated). Re-running an identical export produces zero unnecessary mutations.
- **REQ-D6 [Scale & Latency Target]**: Diff a 200,000-row employee export file in under 60 seconds with bounded memory consumption (< 256MB RSS).

#### Domain E: Transactional Outbox & Downstream Effects
- **REQ-E1 [Atomic Outbox Commit]**: Any database state modification (employee create, update, deactivate, reconciliation diff) must insert an outbox event into the `outbox_events` table within the SAME PostgreSQL ACID transaction.
- **REQ-E2 [Background Outbox Publisher]**: A decoupled publisher worker reads unpublished outbox events using `SELECT ... FOR UPDATE SKIP LOCKED` to allow horizontally scalable, non-blocking polling.
- **REQ-E3 [Resilient Delivery with Exponential Backoff]**: Deliver events to downstream adapters with configurable retry limits, exponential backoff with jitter, and dead-letter queue (DLQ) state for operator inspection after max retries.
- **REQ-E4 [Crash Recovery Demonstration]**: Guarantee that if the application server or publisher worker crashes immediately after committing the DB transaction but before downstream publishing, restarting the process successfully resumes publishing without event loss.
- **REQ-E5 [Deactivation Side-Effect Orchestration]**: When an employee is deactivated:
  - An `EMPLOYEE_DEACTIVATED` event is recorded in the outbox.
  - Downstream Cab Booking Adapter is invoked to cancel all future scheduled cab bookings and free reserved vehicle seats.
  - Downstream invocation is idempotent: re-calling booking cancellation with the same employee and deactivation event returns success without negative side effects.

#### Domain F: Security, RBAC & Multi-Tenant Isolation
- **REQ-F1 [Tenant Isolation Guarantee]**: All database tables, queries, API routes, reconciliation jobs, and outbox partitions must include `tenant_id`. Cross-tenant data access is strictly forbidden and rejected with HTTP 403 / 404.
- **REQ-F2 [Role-Based Access Control (RBAC)]**: Support JWT/API-key authentication with well-defined roles:
  - `hr_integration`: Can ingest real-time HR events and trigger nightly reconciliation uploads.
  - `transport_admin`: Can update transport-owned fields (pickup points, special assistance), view transport roster, view review flags.
  - `operator`: Can view system health, metrics, reconciliation audit logs, approve safety-brake overrides, and retry dead-letter outbox events.
  - `employee`: Read-only access to own profile and booking status.
- **REQ-F3 [Secrets Management]**: Zero hardcoded secrets. Configuration read from environment variables via Pydantic Settings (`.env.example` template provided with placeholders).

#### Domain G: Operational Observability & Resilience
- **REQ-G1 [Structured Logging & Tracing]**: Structured JSON logging with correlation IDs (`X-Correlation-ID`), tenant context, sanitized payloads, and no PII.
- **REQ-G2 [Prometheus Metrics & 3 AM Counters]**: Expose `/metrics` endpoint with Prometheus counters and histograms:
  - `roster_events_total{tenant, status="accepted|rejected|stale|duplicate"}`
  - `roster_reconciliation_duration_seconds{tenant, status}`
  - `roster_safety_brake_triggers_total{tenant}`
  - `roster_outbox_queue_depth{tenant}`
  - `roster_outbox_publish_retries_total{tenant}`
  - `roster_outbox_dlq_total{tenant}`
  - `roster_booking_cancellations_total{status}`
- **REQ-G3 [Health Checks]**: `/healthz` (liveness) and `/readyz` (readiness checking PostgreSQL connectivity).
- **REQ-G4 [Caching Strategy]**: Low-latency caching for immutable or slow-moving reference data (e.g. valid office codes and geofences per tenant) using TTL and deterministic invalidation on master data updates.

---

### 3. Traceability Matrix

| Requirement ID | Requirement Description | Implementation Module | Automated Test File | Status |
| :--- | :--- | :--- | :--- | :--- |
| **REQ-A1** | HR Ingestion (Create, Update, Deactivate) | `app/api/v1/roster.py`, `app/services/roster_service.py` | `tests/test_roster_ingest.py` | Verified |
| **REQ-A2** | Per-Record Validation & Structured Error Batch | `app/schemas/roster_events.py`, `app/services/roster_service.py` | `tests/test_validation.py` | Verified |
| **REQ-A3** | Field & Format Constraints (Phone, Email, Office) | `app/schemas/roster_events.py` | `tests/test_validation.py` | Verified |
| **REQ-A4** | PII Masking in Logs & Error Responses | `app/core/logging.py`, `app/core/error_handlers.py` | `tests/test_pii_safety.py` | Verified |
| **REQ-A5** | Deterministic Error Handling & HTTP Codes | `app/core/error_handlers.py` | `tests/test_validation.py` | Verified |
| **REQ-B1** | Idempotency Key Tracking & Scoped Storage | `app/models/idempotency.py`, `app/services/idempotency_service.py` | `tests/test_idempotency.py` | Verified |
| **REQ-B2** | Idempotent Replay & Conflicting Payload Rejection | `app/services/idempotency_service.py`, `app/services/roster_service.py` | `tests/test_idempotency.py` | Verified |
| **REQ-B3** | Source Version Ordering & Stale Event Drop | `app/services/roster_service.py` | `tests/test_ordering.py` | Verified |
| **REQ-B4** | Deterministic Tie-Breaking (Timestamp + Hash) | `app/services/roster_service.py` | `tests/test_ordering.py` | Verified |
| **REQ-B5** | Concurrency Safety & Row-Level Locking | `app/repositories/employee_repo.py`, `app/services/roster_service.py` | `tests/test_concurrency.py` | Verified |
| **REQ-C1** | HR vs Transport Field Ownership Separation | `app/models/employee.py`, `app/schemas/employee.py` | `tests/test_field_ownership.py` | Verified |
| **REQ-C2** | HR Update Cannot Overwrite Transport Fields | `app/services/roster_service.py` | `tests/test_field_ownership.py` | Verified |
| **REQ-C3** | Office Transfer Protocol & Review Flagging | `app/services/roster_service.py` | `tests/test_field_ownership.py` | Verified |
| **REQ-D1** | Streaming Diff of 200k Rows Bounded Memory | `app/services/reconciliation_service.py` | `tests/test_reconciliation.py`, `tests/test_benchmark_200k.py` | Verified |
| **REQ-D2** | Canonical Record Hashing | `app/utils/hashing.py` | `tests/test_reconciliation.py` | Verified |
| **REQ-D3** | Malformed & Truncated Snapshot Rejection | `app/services/reconciliation_service.py` | `tests/test_reconciliation.py` | Verified |
| **REQ-D4** | Configurable 30% Safety Brake & Operator Halt | `app/services/reconciliation_service.py` | `tests/test_safety_brake.py` | Verified |
| **REQ-D5** | Auditable Run Lifecycle & Operator Override | `app/api/v1/reconciliation.py`, `app/models/reconciliation.py` | `tests/test_safety_brake.py` | Verified |
| **REQ-D6** | 200k Diff Under 60s Performance Benchmark | `scripts/benchmark_reconciliation.py` | `tests/test_benchmark_200k.py` | Verified |
| **REQ-E1** | Transactional Outbox Pattern Atomicity | `app/repositories/outbox_repo.py`, `app/services/roster_service.py` | `tests/test_outbox.py` | Verified |
| **REQ-E2** | Reliable Outbox Polling Publisher | `app/workers/outbox_publisher.py` | `tests/test_outbox.py` | Verified |
| **REQ-E3** | Retries, Exponential Backoff & DLQ | `app/workers/outbox_publisher.py` | `tests/test_outbox.py` | Verified |
| **REQ-E4** | Crash Recovery Between DB Commit & Event Publish | `app/workers/outbox_publisher.py` | `tests/test_crash_recovery.py` | Verified |
| **REQ-E5** | Deactivation Cab Booking Cancellation Adapter | `app/adapters/booking_adapter.py` | `tests/test_downstream_effects.py` | Verified |
| **REQ-F1** | Multi-Tenant Data Isolation Across All Queries | `app/core/tenant.py`, `app/repositories/employee_repo.py` | `tests/test_tenant_isolation.py` | Verified |
| **REQ-F2** | Role-Based Access Control (RBAC) | `app/core/auth.py` | `tests/test_security_rbac.py` | Verified |
| **REQ-F3** | Environment Config & Safe Defaults | `app/core/config.py`, `.env.example` | `tests/test_config.py` | Verified |
| **REQ-G1** | Structured JSON Logging Without PII | `app/core/logging.py` | `tests/test_pii_safety.py` | Verified |
| **REQ-G2** | Prometheus Operational Metrics & Counters | `app/core/metrics.py`, `app/api/v1/metrics.py` | `tests/test_metrics.py` | Verified |
| **REQ-G3** | Liveness & Readiness Probes | `app/api/v1/health.py` | `tests/test_health.py` | Verified |
| **REQ-G4** | Office Code Cache with TTL & Invalidation | `app/core/cache.py` | `tests/test_cache.py` | Verified |

---

### 4. Language Selection Defense: Python 3.12+ (FastAPI + SQLAlchemy 2 + Pydantic v2)
While the assessment notes that Java with Spring Boot is traditionally common in enterprise settings, Python 3.12+ with FastAPI, Pydantic v2, and SQLAlchemy 2 (asyncio) was deliberately chosen for this implementation for the following reasons:
1. **Pydantic v2 Performance & Strict Parsing**: Pydantic v2 is implemented in Rust (`pydantic-core`), providing parsing and validation speeds that rival Java Jackson/Hibernate Validator while delivering unmatched developer ergonomics for fine-grained per-record error reports.
2. **Explicit Asynchronous I/O & Non-Blocking Streams**: Python's `asyncio` combined with async streaming generators (`StreamingResponse` and async file line iterators) enables strictly bounded memory consumption during 200,000-row file reconciliation without spawning heavy thread pools.
3. **Architectural Clarity & Modularity**: FastAPI allows a clean, uncluttered modular monolith architecture without cumbersome Spring XML/annotation boilerplate, making database transactions, conditional locking, and outbox publisher loops transparent and verifiable.
4. **Deterministic Hash Processing**: Native high-speed cryptographic libraries (`hashlib`, `xxhash`) provide ultra-low CPU overhead for diffing 200,000 hashes in seconds.
