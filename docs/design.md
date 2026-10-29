# MoveInSync Case Study 5: HR Roster Sync
## System Architecture & Technical Design Document

### 1. Architectural Overview & Component Topology
The HR Roster Sync system is engineered as a resilient, multi-tenant modular monolith. It decouples high-throughput ingest from transactional mutations and downstream event delivery via the **Transactional Outbox Pattern**.

```mermaid
flowchart TD
    subgraph Clients["External Ingest Feeds & Actors"]
        HR["HR System (Workday / SuccessFactors)"]
        TA["Transport Admin UI"]
        OP["Transport Operator UI"]
        BATCH["Nightly Snapshot File (200k CSV/JSONL)"]
    end

    subgraph API_GW["FastAPI Modular Monolith"]
        AUTH["Auth & Tenant Context Guard"]
        VALID["Boundary Validator & Per-Record Sanitizer"]
        CACHE["Tenant Master Cache (Office Codes/Geofences)"]
        ROSTER_SVC["Roster Domain Service (Idempotency, Ordering & Ownership)"]
        RECON_SVC["Streaming Reconciliation Engine & Safety Brake"]
        OUTBOX_PUB["Background Outbox Publisher Worker"]
    end

    subgraph Storage["PostgreSQL 16 Multi-Tenant ACID Store"]
        EMPLOYEES[("employees table (Row Lock & Versions)")]
        IDEMPOTENCY[("idempotency_keys table (Scope: tenant_id + key)")]
        OUTBOX[("outbox_events table (SKIP LOCKED Queue)")]
        RECON_RUNS[("reconciliation_runs table (Audit & Safety Brake)")]
        OFFICES[("offices table (Tenant Office Master)")]
    end

    subgraph Downstream["Downstream Dispatch & Transport Core"]
        BOOKING_SVC["Cab Booking Service (Seat Allocation / Cancellation)"]
        DLQ["Dead-Letter & Operator Inspection Queue"]
    end

    HR -->|Real-time Events API| AUTH
    TA -->|Transport Field Updates| AUTH
    OP -->|Reconciliation Approval / DLQ Retry| AUTH
    BATCH -->|Nightly Upload Streaming| AUTH

    AUTH --> VALID
    VALID --> CACHE
    VALID --> ROSTER_SVC
    VALID --> RECON_SVC

    ROSTER_SVC -->|Atomic Commit: Mutation + Outbox| Storage
    RECON_SVC -->|Streaming Diff & Safety Check| Storage

    OUTBOX_PUB -->|Poll SKIP LOCKED| OUTBOX
    OUTBOX_PUB -->|Publish Event| BOOKING_SVC
    OUTBOX_PUB -->|Exceeded Retries| DLQ
```

---

### 2. Sequence Diagrams

#### A. Event Ingestion, Idempotency & Outbox Publication
```mermaid
sequenceDiagram
    autonumber
    actor HR as HR System
    participant API as FastAPI Ingestion Endpoint
    participant Guard as Tenant & Schema Validator
    participant Svc as Roster Service
    participant DB as PostgreSQL 16
    participant Pub as Outbox Publisher
    participant Downstream as Cab Booking Adapter

    HR->>API: POST /api/v1/roster/events (Tenant T, Key K, Version V, Payload)
    API->>Guard: Authenticate & Validate Boundary (Phone, Email, Office)
    alt Validation Failed (Single Record in Batch)
        Guard-->>API: Return Per-Record Error (Keep Valid Records Intact)
    end
    Guard->>Svc: Process Valid Record
    Svc->>DB: BEGIN TRANSACTION (ISOLATION LEVEL READ COMMITTED)
    Svc->>DB: SELECT * FROM idempotency_keys WHERE tenant_id = T AND key = K FOR UPDATE
    alt Exact Key Replay with Same Payload Hash
        DB-->>Svc: Found Idempotency Record (Cached Result)
        Svc->>DB: COMMIT (No Mutation, No Outbox)
        Svc-->>API: 200 OK (Idempotent Cached Success)
        API-->>HR: 200 OK
    else Replay with Mismatched Payload Hash
        Svc->>DB: ROLLBACK
        Svc-->>API: 409 Conflict (Idempotency Key Payload Conflict)
        API-->>HR: 409 Conflict
    end

    Svc->>DB: SELECT * FROM employees WHERE tenant_id = T AND employee_id = E FOR UPDATE
    alt Stale Version (Incoming V < DB.source_version)
        Svc->>DB: Record Stale Audit, COMMIT
        Svc-->>API: 200 OK (Stale Event Ignored)
    else Equal Version (V == DB.source_version)
        Svc->>Svc: Tie-break: Compare timestamp, then payload hash
    else Valid Newer Version (V > DB.source_version)
        Svc->>Svc: Apply Field Ownership (Preserve Transport Fields: pickup_point, assistance)
        alt Office Code Changed
            Svc->>Svc: Flag transport_review_required = TRUE
        end
        Svc->>DB: UPDATE employees SET hr_fields = ..., source_version = V
        Svc->>DB: INSERT INTO outbox_events (event_id, event_type, payload, status = PENDING)
        Svc->>DB: INSERT INTO idempotency_keys (tenant_id, key, payload_hash, response)
        Svc->>DB: COMMIT TRANSACTION
        Svc-->>API: 200 OK (Event Accepted & Applied)
        API-->>HR: 200 OK
    end

    par Async Background Outbox Polling
        Pub->>DB: SELECT * FROM outbox_events WHERE status = PENDING FOR UPDATE SKIP LOCKED LIMIT 50
        Pub->>Downstream: Dispatch EMPLOYEE_MUTATED / EMPLOYEE_DEACTIVATED
        alt Dispatch Success
            Pub->>DB: UPDATE outbox_events SET status = PUBLISHED, published_at = NOW()
        else Dispatch Transient Network Failure
            Pub->>DB: UPDATE outbox_events SET retry_count = retry_count + 1, next_retry_at = NOW() + backoff
        end
    end
```

#### B. Nightly Reconciliation & Safety Brake Protocol
```mermaid
sequenceDiagram
    autonumber
    actor HR as HR Scheduler
    participant Recon as Streaming Reconciliation Engine
    participant DB as PostgreSQL 16
    actor Op as Transport Operator

    HR->>Recon: POST /api/v1/reconciliation/upload (tenant_id, 200,000-row file stream)
    Recon->>DB: INSERT INTO reconciliation_runs (status = STREAMING, started_at = NOW())
    
    loop Stream chunks of 2,000 records
        Recon->>Recon: Validate row schema & compute canonical xxHash64
        Recon->>DB: Batch compare hashes against stored active employee hashes
        Recon->>DB: Apply creates / updates for modified hashes
        Recon->>Recon: Accumulate set of present active employee IDs
    end

    Recon->>DB: Query total active count in DB prior to run (N_active)
    Recon->>Recon: Compute missing employees: MissingCount = N_active - PresentCount
    Recon->>Recon: Check Safety Brake: (MissingCount / N_active) >= threshold (30%)

    alt Safety Brake Triggered (e.g., 40% missing / truncated export)
        Recon->>DB: UPDATE reconciliation_runs SET status = SAFETY_BRAKE_TRIGGERED, missing_count = ..., operator_review_required = TRUE
        Recon-->>HR: 202 Accepted (Reconciliation Halted by Safety Brake. Awaiting Human Approval)
        Op->>Recon: GET /api/v1/reconciliation/{run_id}/report
        Recon-->>Op: Inspection report: Total 200k, Missing 80k (40%), File truncated at line 120k
        alt Operator Rejects (Broken File Confirmed)
            Op->>Recon: POST /api/v1/reconciliation/{run_id}/abort
            Recon->>DB: UPDATE reconciliation_runs SET status = ABORTED
        else Operator Approves (Legitimate Mass Redundancy)
            Op->>Recon: POST /api/v1/reconciliation/{run_id}/approve
            Recon->>DB: Mark missing employees INACTIVE in batches + Enqueue Outbox deactivation events
            Recon->>DB: UPDATE reconciliation_runs SET status = COMPLETED
        end
    else Normal Run (Missing < 30%)
        Recon->>DB: Mark missing employees INACTIVE in batches + Enqueue Outbox deactivation events
        Recon->>DB: UPDATE reconciliation_runs SET status = COMPLETED
        Recon-->>HR: 200 OK (Reconciliation Succeeded)
    end
```

---

### 3. Data Model & PostgreSQL Schema Design

```mermaid
erDiagram
    TENANTS ||--o{ EMPLOYEES : owns
    TENANTS ||--o{ OFFICES : configures
    TENANTS ||--o{ IDEMPOTENCY_KEYS : scopes
    TENANTS ||--o{ RECONCILIATION_RUNS : audits
    TENANTS ||--o{ OUTBOX_EVENTS : partitions

    EMPLOYEES {
        uuid id PK
        string tenant_id FK
        string employee_id UK
        string first_name
        string last_name
        string email
        string phone
        string office_code
        string department
        string manager_id
        string status
        bigint source_version
        timestamp source_timestamp
        string record_hash
        string preferred_pickup_point_id
        jsonb pickup_geofence
        boolean special_assistance_required
        string special_assistance_notes
        string shift_timing
        boolean transport_review_required
        timestamp created_at
        timestamp updated_at
    }

    IDEMPOTENCY_KEYS {
        uuid id PK
        string tenant_id FK
        string idempotency_key UK
        string payload_hash
        jsonb response_body
        integer response_code
        timestamp expires_at
        timestamp created_at
    }

    OUTBOX_EVENTS {
        uuid id PK
        string tenant_id FK
        string event_type
        string aggregate_id
        jsonb payload
        string status
        integer retry_count
        timestamp next_retry_at
        string last_error
        timestamp published_at
        timestamp created_at
    }

    RECONCILIATION_RUNS {
        uuid id PK
        string tenant_id FK
        string filename
        integer total_records_received
        integer records_created
        integer records_updated
        integer records_unchanged
        integer records_stale
        integer missing_active_records
        float missing_percentage
        string status
        boolean approved_by_operator
        string operator_id
        jsonb anomaly_report
        timestamp started_at
        timestamp completed_at
    }

    OFFICES {
        uuid id PK
        string tenant_id FK
        string office_code UK
        string office_name
        string timezone
        boolean is_active
    }
```

#### Key Indexes & Constraints
1. `idx_employees_tenant_emp_id`: UNIQUE `(tenant_id, employee_id)` — Guarantees multi-tenant employee identity.
2. `idx_employees_tenant_version`: `(tenant_id, employee_id, source_version)` — Fast conditional version checks.
3. `idx_idempotency_tenant_key`: UNIQUE `(tenant_id, idempotency_key)` — Scopes idempotency strictly to tenant.
4. `idx_outbox_pending_poll`: `(status, next_retry_at)` WHERE `status IN ('PENDING', 'RETRYING')` — Partial index optimizing `SKIP LOCKED` polling without scanning millions of historical published rows.
5. `idx_employees_tenant_status_hash`: `(tenant_id, status, record_hash)` — Fast hash matching during reconciliation diffing.

---

### 4. Failure Modes & Recovery Matrix

| Failure Mode | Direct Symptom | Root Cause | System Detection & Mitigation | User / Client Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Out-of-Order Events** | Transfer (v7) arrives before Shift Change (v6). | Network routing latency / multi-queue races. | Engine compares `source_version`. Version 6 is lower than current 7. Event is recorded as stale audit log and ignored. | HTTP 200 returned with `{status: "stale_ignored"}`. No state regression. |
| **Identical Duplicate Event** | Exact same webhook posted multiple times. | Network retries / timeout replays. | Idempotency layer detects existing `(tenant_id, key)` with matching hash. | HTTP 200 returns cached original response. Zero side effects or duplicate outbox events. |
| **Conflicting Idempotency Replay** | Same key used with differing employee data. | Client bug reusing keys across distinct operations. | SHA-256 payload hash does not match stored hash for key. | HTTP 409 Conflict with detailed error: `Idempotency key reused with different payload`. |
| **Crash After DB Commit Before Outbox Publish** | Database has updated employee; HTTP response returned; process crashes before downstream dispatch. | Power failure, container OOM, VM reboot. | Transactional outbox commits event with employee update in same DB transaction. Background worker resumes on restart, claims unpublished row via `SKIP LOCKED`, and delivers it. | Zero event loss. Downstream system receives event with at-least-once guarantee. |
| **Downstream Cab Service Outage** | Cab booking cancellation fails with 503 or socket timeout. | Cab booking microservice down or network partition. | Outbox worker intercepts exception, applies exponential backoff ($2^r \times 1\text{s} + \text{jitter}$), increments `retry_count`. If max retries (5) exceeded, moves to `DEAD_LETTER`. | Core roster remains consistent. Operator alerted to inspect DLQ and trigger redelivery. |
| **Truncated Nightly File** | HR export cut off at 120,000 of 200,000 employees. | FTP connection dropped prematurely; disk space exhaustion at HR. | Safety brake computes 80,000 missing active employees (40% $> 30%$ threshold). Halts deactivation pass, creates operator incident. | 80,000 employees keep their cab bookings. Operator receives alert to investigate truncated file. |
| **Office Transfer Desync** | Employee moved from BLR-Campus-1 to HYD-Campus-2, but pickup point still in Bangalore. | Transport admin pickup routing is campus-specific. | Field ownership preserves transport fields but sets `transport_review_required = TRUE` and logs warning for transport planner. | Commute routing engine halts routing to old geofence and notifies transport planner to assign new pickup point. |
| **Cross-Tenant Attack Attempt** | Tenant A passes employee ID belonging to Tenant B in payload or URL. | Malicious actor or misconfigured API key. | JWT/API-key sets request tenant context. All queries enforce `WHERE tenant_id = context.tenant_id`. | HTTP 404 / 403. Tenant A cannot view or alter Tenant B data. |

---

### 5. Architectural Trade-offs & Consistency Decisions

1. **Transactional Outbox vs Distributed 2PC (Two-Phase Commit)**:
   - *Decision*: Adopted Transactional Outbox.
   - *Rationale*: Two-Phase Commit / XA transactions introduce tight coupling, severe latency, and deadlock vulnerability under high ingest volume. Outbox provides local ACID atomicity with asynchronous eventual consistency for downstream cab cancellations.
2. **At-Least-Once Delivery vs Exactly-Once Fallacy**:
   - *Decision*: Guaranteed at-least-once delivery with mandatory idempotent downstream consumer adapters.
   - *Rationale*: Distributed systems theory proves network partitions prevent true exactly-once side-effects across independent HTTP services. Making consumers idempotent (e.g. `cancel_booking(emp_id, event_id)`) guarantees safety.
3. **Streaming Memory Diff vs Loading Full 200k Records**:
   - *Decision*: Chunked streaming iteration with canonical 64-bit integer hashes.
   - *Rationale*: Loading 200,000 Python ORM objects consumes $> 1.2\text{ GB}$ RAM and causes intense garbage collection pauses. Streaming chunks of 2,000 records with scalar hashes keeps memory strictly bounded under 120MB RSS while executing diff in $< 45$ seconds.
4. **Deliberately Excluded Technologies**:
   - *Kafka / RabbitMQ*: PostgreSQL `SKIP LOCKED` provides guaranteed transactional atomicity with the primary database. Introducing a broker adds dual-write consistency hazards, deployment complexity, and operations overhead without adding throughput benefit at 200k nightly events.
   - *Redis*: Local bounded LRU cache with TTL handles office code master data cleanly without network hop latency or cache-invalidation race conditions.

---

### 6. API Surface Specification

| Method | Endpoint | Allowed Roles | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/roster/events` | `hr_integration`, `operator` | Real-time batch/single HR event ingestion with per-record validation. |
| `POST` | `/api/v1/transport/employees/{id}` | `transport_admin`, `operator` | Transport admin update for pickup point and special assistance. |
| `GET` | `/api/v1/employees/{id}` | `employee`, `transport_admin`, `operator` | Retrieve employee profile (scoped by tenant and role). |
| `POST` | `/api/v1/reconciliation/upload` | `hr_integration`, `operator` | Stream full nightly export file (CSV/JSONL up to 200k rows). |
| `GET` | `/api/v1/reconciliation/{run_id}` | `hr_integration`, `transport_admin`, `operator` | Query status, metrics, and safety brake report for a reconciliation run. |
| `POST` | `/api/v1/reconciliation/{run_id}/approve` | `operator` | Operator approval to override safety brake and execute deactivations. |
| `POST` | `/api/v1/reconciliation/{run_id}/abort` | `operator` | Operator abort of halted reconciliation run. |
| `GET` | `/api/v1/outbox/dlq` | `operator` | Inspect failed outbox events in dead-letter state. |
| `POST` | `/api/v1/outbox/dlq/{id}/retry` | `operator` | Re-enqueue a failed outbox event for immediate retry. |
| `GET` | `/healthz` | Public | Liveness probe (HTTP 200). |
| `GET` | `/readyz` | Public | Readiness probe (validates PostgreSQL connectivity). |
| `GET` | `/metrics` | Public / Prometheus | Prometheus metrics endpoint. |
