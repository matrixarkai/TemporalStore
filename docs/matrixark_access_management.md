# MatrixArk Access Management

MatrixArk separates caller authentication from context ownership:

```text
API key   -> which customer app/service is calling MatrixArk
account   -> customer or billing organization
tenant    -> isolated workspace/deployment inside an account
user      -> human or agent identity whose context is being read/written
session   -> conversation, workflow, or agent run
```

This mirrors the practical shape used by agent-context systems: callers send a
simple envelope, while the context service owns tenant isolation, auditability,
key lifecycle, and internal storage hashes.

## Identity Model

External callers can send:

```json
{
  "api_key": "mk_test_...",
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_eng",
    "user_id": "alice",
    "session_id": "cursor-thread-123",
    "team": "infra",
    "project": "project_1"
  }
}
```

MatrixArk enriches the scope before writing or retrieving:

```text
tenant_hash  = hash(account_id + tenant_id)
user_hash    = hash(tenant_hash + user_id)
session_hash = hash(tenant_hash + session_id)
```

`session_hash` is tenant-scoped rather than user-scoped so a session-only query
can still find records from the same thread. Adding `user_id` makes the filter
stricter.

## API Keys

MatrixArk API keys are issued and stored as hashes. The raw key is returned only
once from `matrixark_admin_create_api_key`.

Default context scopes:

```text
context:ingest
context:retrieve
context:feedback
context:replay
resource:ingest
```

Admin scopes:

```text
admin:account
admin:tenant
admin:api_key
admin:sso
admin:audit
```

Example:

```json
{
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "scopes": ["context:ingest", "context:retrieve", "context:feedback"],
  "role": "agent_service"
}
```

The returned `api_key` should be stored by the customer app. MatrixArk persists
only `api_key_hash`, `api_key_id`, account, tenant, scopes, role, and status.

## Lifecycle

Implemented MCP tools:

| Tool | Purpose |
|---|---|
| `matrixark_admin_create_account` | Create account and default tenant records. |
| `matrixark_admin_create_api_key` | Issue a scoped MatrixArk API key. |
| `matrixark_admin_rotate_api_key` | Revoke an active key and issue a replacement. |
| `matrixark_admin_revoke_api_key` | Revoke an active key. |
| `matrixark_admin_map_sso_user` | Map Okta/Google/Azure AD user ids to MatrixArk user ids. |
| `matrixark_admin_audit` | Read access-management audit logs. |

Context tools now accept `api_key`:

```text
matrixark_ingest
matrixark_batch_extract
matrixark_retrieve
matrixark_feedback
matrixark_replay
```

## Access Modes

`MATRIXARK_ACCESS_MODE=dev`

- Default for local testing.
- Missing API keys are allowed.
- MatrixArk uses `acct_dev` and `tenant_dev` unless the caller sends account and
  tenant ids.

`MATRIXARK_ACCESS_MODE=enforced`

- Scoped MatrixArk API keys are required for context and admin operations.
- Revoked keys stop working immediately because the newest append-only key state
  is authoritative.

## Audit Records

Every context and admin operation appends a `matrixark_audit_log` record:

```json
{
  "record_type": "matrixark_audit_log",
  "action": "context.retrieve",
  "status": "ok",
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "api_key_id": "key_...",
  "role": "agent_service",
  "details": {"context_pack_id": "123"},
  "created_at_ms": 1782110000000
}
```

These records use the same adapter boundary as context data, so they work with
local JSONL and C++ TemporalStore-backed runs.

## SSO Mapping

SSO is optional in the MVP. MatrixArk can map external identities into stable
internal user ids:

```json
{
  "provider": "okta",
  "external_user_id": "alice@acme.com",
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng"
}
```

Result:

```json
{
  "matrixark_user_id": "mu_..."
}
```

Enterprise apps can keep sending their own user ids at first. SSO mapping lets
larger deployments later connect Okta, Google Workspace, or Azure AD without
changing TemporalStore data models.
