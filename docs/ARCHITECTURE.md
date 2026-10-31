# MoveInSync HR Roster Sync — System Architecture

## 1. Executive Architecture Overview

MoveInSync HR Roster Sync is an enterprise-grade distributed synchronization engine engineered for **Case Study 5: Idempotent, Out-of-Order, Self-Healing HR Roster Sync**.

The platform reconciles human resource employee rosters with enterprise commute, cab-routing, and security infrastructure across two synchronized ingress vectors:
1. **Live Event Stream**: Real-time webhook and event updates (`CREATE`, `UPDATE`, `DEACTIVATE`, `OFFICE_TRANSFER`) arriving out-of-order, concurrently, or duplicated from upstream HRIS systems (Workday, SAP SuccessFactors, BambooHR).
2. **Nightly Full Snapshot Reconciliation**: Bulk CSV export snapshots (up to 200,000+ employees) uploaded nightly to detect missing joiners, stale departures, departmental shifts, and office transfers.

### Architectural Philosophy: Zero External Infrastructure Beyond PostgreSQL
Rather than introducing operational fragility and distributed transaction overhead through Kafka, Redis, RabbitMQ, or Celery, the system utilizes **PostgreSQL 16 as the single, authoritative state engine, transactional coordinator, and streaming buffer**:
- **Streaming Staging**: Chunked ingestion directly into unlogged staging tables avoiding resident memory inflation ($< 256$ MB RSS peak).
- **Set-Based Differential Calculus**: Pure relational SQL diffs (`EXCEPT`, `LEFT JOIN`) replacing $O(N)$ Python memory materialization.
- **Physical Session Pinned Advisory Locks**: `pg_try_advisory_lock` guaranteeing strictly mutually exclusive reconciliation approval workflows.
- **Transactional Outbox with `FOR UPDATE SKIP LOCKED`**: Zero-latency distributed publishing with queue depth tracking, exponential backoff, jitter, and dead-letter queue (DLQ) containment.

---

## 2. System Component Topology

```mermaid
flowchart TD
    subgraph Clients["Clients & Upstream Systems"]
        HRIS["HRIS Systems / Webhooks"]
        Admin["Commute Operators & Admins"]
    end

    subgraph Security["Security & Ingress Layer"]
        CORS["CORS & Timing Protection"]
        JWT["JWT Auth & RBAC (hr_integration, operator, admin)"]
        RateLimit["Tenant Scoping & Rate Limiter"]
    end

    subgraph AppServer["FastAPI Application Server (Python 3.13)"]
        RouterEvents["/api/v1/roster/events"]
        RouterRecon["/api/v1/reconciliation/*"]
        RouterOutbox["/api/v1/outbox/*"]
        
        ServiceRecon["Reconciliation Service"]
        ServiceRoster["Roster Event Processor"]
        Cache["BoundedTTLCache (Office Codes, Monotonic TTL)"]
    end

    subgraph Workers["Background Asynchronous Workers"]
        Publisher["Transactional Outbox Publisher (FOR UPDATE SKIP LOCKED)"]
    end

    subgraph Database["PostgreSQL 16 Storage Engine"]
        T_Emp[("employees")]
        T_Stage[("reconciliation_staging")]
        T_Runs[("reconciliation_runs")]
        T_Outbox[("outbox_events (Status, DLQ)")]
        T_Offices[("offices (Active Reference)")]
        T_Idemp[("idempotency_keys")]
        AdvisoryLock{{"pg_try_advisory_lock(run_id)"}}
    end

    subgraph Downstream["Downstream Transport & Fleet Systems"]
        Fleet["Cab Booking & Route Allocator"]
        Badge["Campus Turnstile & Access Control"]
        DLQ["Operator Dead-Letter Escalation"]
    end

    HRIS --> CORS --> JWT --> RouterEvents
    Admin --> CORS --> JWT --> RouterRecon
    Admin --> CORS --> JWT --> RouterOutbox

    RouterEvents --> ServiceRoster
    RouterRecon --> ServiceRecon
    ServiceRoster --> Cache
    ServiceRecon --> Cache

    ServiceRoster --> T_Emp
    ServiceRoster --> T_Outbox
    ServiceRoster --> T_Idemp

    ServiceRecon --> T_Stage
    ServiceRecon --> T_Runs
    ServiceRecon --> AdvisoryLock
    ServiceRecon --> T_Emp
    ServiceRecon --> T_Outbox

    Publisher --> T_Outbox
    Publisher --> Fleet
    Publisher --> Badge
    Publisher -. Failure Exceeded .-> DLQ
```

---

## 3. End-to-End Data Flows

### A. Live Event Ingestion Data Flow
Real-time events arrive via `POST /api/v1/roster/events`. Each event passes through deterministic validation, version fencing, and transactional outbox emission:

```mermaid
sequenceDiagram
    autonumber
    actor HR as HR System
    participant API as FastAPI Ingress
    participant Val as Schema & PII Validator
    participant Cache as BoundedTTLCache
    participant DB as PostgreSQL Transaction
    participant Outbox as Outbox Queue

    HR->>API: POST /api/v1/roster/events (Payload, Idempotency-Key)
    API->>DB: Check idempotency_keys table
    alt Key already committed
        DB-->>API: Return cached JSON response
        API-->>HR: 200 OK (Idempotent replay)
    else New Request
        API->>Val: Validate E.164 Phone, RFC 5322 Email, source_version >= 1
        API->>Cache: Lookup office_code for tenant_id
        Cache-->>API: Active office codes set
        API->>DB: BEGIN TRANSACTION
        DB->>DB: SELECT FOR UPDATE employee row
        alt Out-of-order event (source_version < current_version)
            DB->>DB: Record as STALE (no-op)
        else Identical version (source_version == current_version)
            DB->>DB: Compare SHA-256 canonical hash
            alt Hash matches
                DB->>DB: Record as DUPLICATE / UNCHANGED
            else Deterministic tie-breaker
                DB->>DB: Compare source_timestamp / field priority
            end
        else Valid newer version (source_version > current_version)
            DB->>DB: UPDATE employees SET ...
            DB->>Outbox: INSERT INTO outbox_events (EMPLOYEE_UPDATED / DEACTIVATED)
        end
        DB->>DB: INSERT INTO idempotency_keys
        DB->>DB: COMMIT TRANSACTION
        API-->>HR: 200 OK (status breakdown)
    end
```

### B. Nightly Snapshot Reconciliation Data Flow
Nightly CSV snapshots process up to 200,000+ employees in bounded memory chunks:

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Operator / Cron
    participant API as Reconciliation Ingress
    participant Stage as Staging Table
    participant Diff as SQL Diff Engine
    participant Brake as Safety Brake
    participant Approval as Approval Coordinator
    participant Outbox as Outbox Publisher

    Admin->>API: POST /api/v1/reconciliation/upload (CSV stream)
    API->>Stage: Stream CSV in 5,000-row chunks
    Stage-->>API: Staging complete (total_records, malformed_count)
    API->>Diff: Execute pure SQL diff (EXCEPT / LEFT JOIN)
    Diff-->>API: Diff summary: created, updated, unchanged, missing
    API->>Brake: Calculate missing_percentage = missing / baseline_active
    alt malformed_rows > 0 or duplicate_keys > 0
        API->>Approval: Transition to QUARANTINED (Operator Review Required)
    else missing_percentage > threshold_percent (default 30%)
        API->>Approval: Transition to NEEDS_APPROVAL (Safety Brake Tripped)
    else Normal Run (missing <= threshold)
        API->>Approval: Auto-approve: Transition to APPROVING
        Approval->>Stage: Chunked deactivations in 1,000-row batches
        Stage->>Outbox: Commit EMPLOYEE_DEACTIVATED outbox events atomically
        Approval->>API: Transition to COMPLETED
    end
    API-->>Admin: 200 OK (Run status & diff report)
```

---

## 4. Reconciliation State Machine

The reconciliation lifecycle is governed by an explicit, durable, crash-resilient finite state machine:

```mermaid
stateDiagram-v2
    [*] --> PENDING: CSV Upload Initiated
    PENDING --> STAGING: Background Task Picked
    STAGING --> STAGED: All Chunks Streamed to DB
    STAGING --> FAILED: Parse Crash / Abrupt Failure
    
    STAGED --> VALIDATING: Set-Diff Executing
    VALIDATING --> READY: Set-Diff Clean & Below Brake
    VALIDATING --> NEEDS_APPROVAL: Missing % > Brake Threshold
    VALIDATING --> QUARANTINED: Malformed / Duplicate Rows Detected
    VALIDATING --> FAILED: Engine Error
    
    NEEDS_APPROVAL --> REJECTED: Operator Explicit Rejection
    NEEDS_APPROVAL --> APPROVING: Operator Approval Issued
    QUARANTINED --> REJECTED: Operator Rejects Anomalous Snapshot
    QUARANTINED --> APPROVING: Operator Approves Quarantined Run

    READY --> APPROVING: Automatic Execution Triggered
    
    APPROVING --> APPROVING: Resumed Post-Crash (Idempotent Batches)
    APPROVING --> COMPLETED: All 1,000-Row Batches & Outbox Committed
    APPROVING --> FAILED: Unrecoverable Abort

    REJECTED --> [*]
    COMPLETED --> [*]
    FAILED --> [*]
```

---

## 5. Concurrency & Isolation Model

### 1. Tenant Partitioning
All operational tables (`employees`, `reconciliation_runs`, `reconciliation_staging`, `outbox_events`, `offices`, `idempotency_keys`) enforce strict `tenant_id` column partitioning. Foreign queries and cross-tenant mutations are impossible:
- All database queries filter explicitly by `WHERE tenant_id = :tenant_id`.
- Tenant context is authenticated and extracted cryptographically from verified JWT tokens.

### 2. Session-Level Pinned PostgreSQL Advisory Locks
To eliminate race conditions during approval of large reconciliations without long-lived blocking table locks:
```python
# Acquire advisory lock bound to dedicated physical PostgreSQL connection:
lock_id = hash(run_id) & 0x7FFFFFFF
acquired = await conn.execute(text(f"SELECT pg_try_advisory_lock({lock_id})"))
```
- **Connection Pinning**: The lock is tied to the physical asyncpg backend process (`bind=conn`).
- **Transaction Independence**: Individual 1,000-row deactivation batches can execute `COMMIT` and `ROLLBACK` repeatedly without releasing the advisory lock.
- **Fail-Safe Release**: The lock is released in a mandatory `finally` block or automatically discarded when the PostgreSQL backend connection terminates.

### 3. Worker Queue De-duplication (`FOR UPDATE SKIP LOCKED`)
Multiple concurrent Outbox Publisher workers poll the `outbox_events` table simultaneously:
```sql
SELECT * FROM outbox_events
WHERE status IN ('PENDING', 'FAILED') AND next_retry_at <= NOW()
ORDER BY created_at ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
```
- Workers acquire row-level locks only on unallocated events.
- Concurrently active workers instantly skip locked rows and process completely disjoint subsets.
- Lock contention and deadlocks are mathematically eliminated.

---

## 6. Transaction Boundary Contracts

| Workflow | Boundary Scope | Commit Guarantees | Rollback Guarantees |
| :--- | :--- | :--- | :--- |
| **Live Event Ingestion** | Single Event Batch (up to 500 records) | Employee status + Outbox events + Idempotency key committed in single transaction. | Any unhandled exception rolls back all employee mutations and outbox events together. |
| **Snapshot Staging** | 5,000 CSV rows per chunk | Staged rows flushed to `reconciliation_staging` with run progress counter. | Chunk-level failure rolls back chunk; prior committed chunks remain durable. |
| **Approval Deactivation** | 1,000 missing employees per batch | Status update (`INACTIVE`), `deactivated_at`, and `EMPLOYEE_DEACTIVATED` outbox records committed in SAME transaction. | If outbox insert fails, employee status rolls back to `ACTIVE`. No orphaned deactivations. |
| **Outbox Dispatch** | Single Outbox Record | Downstream delivery acknowledged before marking `PUBLISHED` or scheduling retry. | Delivery failure records incremented `retry_count`, calculates exponential backoff + jitter, or escalates to `DEAD_LETTER`. |
