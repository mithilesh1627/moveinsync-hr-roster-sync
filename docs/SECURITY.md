# MoveInSync HR Roster Sync — Security Architecture & Threat Mitigation

## 1. Multi-Tenant Data Isolation Model

The platform operates as a secure multi-tenant software-as-a-service (SaaS) backend. Data isolation across enterprise clients is non-negotiable:

### A. Tenant Claim Cryptographic Binding
Every authenticated request requires a signed JSON Web Token (JWT). The token's payload contains the cryptographically verified `tenant_id`:
```json
{
  "sub": "operator-corp-alpha",
  "tenant_id": "tenant-alpha",
  "roles": ["operator"],
  "exp": 1793463600
}
```
- Ingress middleware extracts and validates `AuthContext(user_id, tenant_id, roles)`.
- Client applications cannot override or manipulate `tenant_id` via request body, query parameters, or URL path.

### B. Relational Row-Level Scoping
All operational queries and mutations enforce explicit tenant constraints:
```sql
SELECT * FROM employees 
WHERE tenant_id = :tenant_id AND employee_id = :employee_id;
```
- There are **no global un-scoped queries** in any repository.
- Cross-tenant data leakage is structurally impossible at the SQL translation layer.

---

## 2. Authentication & Role-Based Access Control (RBAC)

### A. Role Hierarchy & Endpoint Permissions

| Role | Intended Consumer | Permitted Endpoints | Prohibited Operations |
| :--- | :--- | :--- | :--- |
| **`hr_integration`** | Upstream HR Systems (Workday, BambooHR webhooks) | `POST /api/v1/roster/events` | Cannot access reconciliation reports, cannot approve runs, cannot view DLQ. |
| **`operator`** | Commute Operations Team | `POST /api/v1/reconciliation/upload`<br>`GET /api/v1/reconciliation/runs/{id}`<br>`POST /api/v1/reconciliation/runs/{id}/approve`<br>`GET /api/v1/outbox/dlq`<br>`POST /api/v1/outbox/dlq/{id}/retry` | Cannot issue live webhooks directly, cannot alter tenant configuration. |
| **`admin`** | System & Security Administrators | All endpoints + tenant configuration + office master data creation. | Full access within authenticated tenant context. |

### B. Credential Verification & Timing Attack Mitigation
API keys and authentication tokens are verified using constant-time string comparison (`hmac.compare_digest`):
```python
import hmac

def verify_api_key(provided_key: str, expected_key: str) -> bool:
    return hmac.compare_digest(provided_key.encode("utf-8"), expected_key.encode("utf-8"))
```
This guarantees immunity against remote side-channel timing analysis attacks.

---

## 3. PII Safety & Data Leakage Prevention

Employee rosters contain Personally Identifiable Information (PII), including legal names, personal phone numbers, and corporate email addresses. The system enforces strict PII minimization across all external touchpoints:

### A. Canary Token Testing & Masked Error Responses
Validation exceptions (e.g., malformed phone numbers or email addresses) never echo the raw user input back in the response body.
- **Vulnerable Pattern (Disallowed)**:
  ```json
  {"detail": "Invalid phone number: '+919876543210' does not conform to E.164"}
  ```
- **Hardened Pattern (Implemented)**:
  ```json
  {"detail": "Validation error: Field 'phone' does not conform to E.164 specification (+[country code][digits])"}
  ```

### B. Structured Logging Redaction
All application logging uses structured JSON format. Logging filters strip sensitive keys (`phone`, `email`, `first_name`, `last_name`) before log serialization:
```json
{
  "timestamp": "2026-10-30T16:21:21.805865+00:00",
  "level": "INFO",
  "logger": "moveinsync.roster",
  "message": "Processed employee deactivation",
  "tenant_id": "tenant-alpha",
  "employee_id": "EMP-00123",
  "office_code": "BLR-CAMPUS-1"
}
```

### C. Downloadable Reconciliation Reports
Reconciliation summary reports expose strictly operational audit fields (`run_id`, `status`, `total_records`, `records_created`, `records_deactivated`, `missing_percentage`, `quarantine_reason`). Individual employee personal details are never exported in plain text audit summaries.

---

## 4. Production Hardening & Security Safeguards

### A. Secret Configuration Enforcement
When `settings.app_env == "production"`:
- The application fails to boot if `jwt_secret_key` matches the default insecure placeholder (`"change-me-in-production-moveinsync-secret"`).
- CORS origins cannot be configured to wildcard `*`.
- Development mock routes (`/api/v1/mock-downstream/*`) are automatically disabled and return `404 Not Found`.

### B. SQL Injection Immunity
All database interactions use SQLAlchemy 2.0 type-safe constructs or parameterized `text()` expressions with bound parameter dicts:
```python
# Safe Parameterized Query
await session.execute(
    text("SELECT * FROM employees WHERE tenant_id = :tenant AND employee_id = :emp"),
    {"tenant": tenant_id, "emp": emp_id}
)
```
Direct string formatting or raw SQL concatenation (`f"SELECT * WHERE id = '{id}'"`) is strictly forbidden across the codebase.
