# MatrixArk Access Management

MatrixArk separates caller authentication from context ownership:

```text
API key   -> which customer app, hook, MCP server, or service is calling MatrixArk
account   -> customer or billing organization
tenant    -> isolated workspace/deployment inside an account
user      -> human or agent identity whose context is being read/written
session   -> conversation, workflow, task, or agent run
```

The customer-facing API stays simple: callers send messages, a query, and a
small `scope`. MatrixArk owns tenant isolation, user/session hashing, API-key
policy, audit logs, and TemporalStore data placement.

## Required Identity Shape

For production, every request should carry a MatrixArk API key. A request may
carry either `user_id`, `session_id`, or both. Sending both is best because it
lets MatrixArk isolate a human/agent and also group turns from the same thread.

```json
{
  "api_key": "mk_test_...",
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_eng",
    "user_id": "alice",
    "session_id": "codex-thread-123"
  }
}
```

MatrixArk enriches the scope before writing or retrieving:

```text
tenant_hash  = hash(account_id + tenant_id)
user_hash    = hash(tenant_hash + user_id)
session_hash = hash(tenant_hash + session_id)
```

`session_hash` is tenant-scoped rather than user-scoped so a session-only hook
can still find records from the same thread. Adding `user_id` makes the filter
stricter and is strongly recommended for enterprise deployments.

## Account, Tenant, User, Session

- `account_id`: assigned by MatrixArk or the enterprise control plane. It is the
  commercial and governance boundary. The newest account record controls whether
  context APIs are allowed.
- `tenant_id`: assigned by MatrixArk or the enterprise control plane. It is the
  deployment/workspace boundary under an account. The newest tenant record can
  disable one workspace without disabling the whole account.
- `user_id`: usually comes from the customer app, Codex/Cursor-like hook, or SSO
  mapping. MatrixArk does not invent it during normal context ingestion.
- `session_id`: usually comes from the agent thread/run. If it is missing,
  MatrixArk can still isolate by user, but confirmation detection, batch
  extraction, and replay quality are weaker.

## API Key Fields

`matrixark_admin_create_api_key` returns the raw key once and stores only the
hash. A key can now be constrained to specific users or sessions.

```json
{
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "role": "agent_service",
  "display_name": "alice codex hook",
  "scopes": ["context:ingest", "context:retrieve", "context:feedback"],
  "allowed_user_ids": ["alice"],
  "allowed_session_ids": ["codex-thread-123"],
  "expires_at_ms": 4102444800000
}
```

Policy:

- Empty `allowed_user_ids` means any user inside the key account/tenant.
- Empty `allowed_session_ids` means any session inside the key account/tenant.
- If an allow-list is present, the caller must send the matching `user_id` or
  `session_id`; omitted or different values are rejected before TemporalStore is
  touched.
- `expires_at_ms` must be a future Unix timestamp in milliseconds.
- API key scopes are validated against MatrixArk's known context/admin scopes;
  unknown scopes are rejected at creation time.
- Rotated keys preserve scopes, role, display name, user allow-list, session
  allow-list, and expiry.
- Disabled users are blocked for API-key-authenticated context calls even if the
  API key is otherwise valid.

## Scopes

Default context scopes:

```text
context:ingest
context:batch_extract
context:session_commit
context:refresh_summaries
context:retrieve
context:feedback
context:replay
resource:ingest
```

Admin scopes:

```text
admin:account
admin:tenant
admin:user
admin:api_key
admin:sso
admin:audit
```

A tenant admin API key can manage users and keys only inside its own
`account_id`/`tenant_id`. Cross-account admin writes are rejected.

## Lifecycle Tools

| Tool | Purpose |
|---|---|
| `matrixark_admin_create_account` | Create account and default tenant records. |
| `matrixark_admin_update_account` | Update account/tenant metadata or active/disabled status. |
| `matrixark_admin_list_accounts` | List visible account/tenant metadata. |
| `matrixark_admin_create_user` | Register a MatrixArk user under an account/tenant. |
| `matrixark_admin_update_user` | Update user metadata or disable/enable a user. |
| `matrixark_admin_list_users` | List user metadata for an account/tenant. |
| `matrixark_admin_create_api_key` | Issue a scoped MatrixArk API key. |
| `matrixark_admin_list_api_keys` | List redacted API key metadata; raw keys and hashes are never returned. |
| `matrixark_admin_rotate_api_key` | Revoke an active key and issue a replacement. |
| `matrixark_admin_revoke_api_key` | Revoke an active key. |
| `matrixark_admin_map_sso_user` | Map Okta/Google/Azure AD user ids to MatrixArk user ids. |
| `matrixark_admin_audit` | Read admin audit logs and API-key usage logs. |

Context tools accept `api_key` and `scope`:

```text
matrixark_ingest
matrixark_batch_extract
matrixark_session_commit
matrixark_refresh_summaries
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
- Revoked or expired keys stop working immediately because the newest
  append-only key state is authoritative.

## Account And Tenant Status Enforcement

Accounts and tenants are append-only records. The newest record is authoritative.
A disabled account or tenant blocks API-key-authenticated context operations,
but admin operations can still inspect or repair metadata.

```json
{
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "account_status": "active",
  "tenant_status": "disabled"
}
```

This gives enterprises a coarse off switch for a customer, environment,
workspace, department, or regulated tenant without deleting historical context.

## User Status Enforcement

Users are append-only records keyed by `account_id`, `tenant_id`, and `user_id`.
`matrixark_admin_update_user` writes a new user version. The newest version is
authoritative.

```json
{
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "user_id": "alice",
  "status": "disabled"
}
```

When an API-key-authenticated request includes `scope.user_id`, MatrixArk checks
the newest user record. If the user is disabled, ingest/retrieve/feedback/replay
are rejected. This gives enterprises a simple offboarding path without rewriting
old context data.

## Safe Key Inventory

`matrixark_admin_list_api_keys` returns only operational metadata:

```json
{
  "api_key_id": "key_...",
  "status": "active",
  "role": "agent_service",
  "display_name": "alice codex hook",
  "scopes": ["context:ingest", "context:retrieve"],
  "allowed_user_ids": ["alice"],
  "allowed_session_ids": ["codex-thread-123"],
  "expires_at_ms": 4102444800000
}
```

It never returns raw API keys or API key hashes.

## Audit And Usage Logs

Every admin/context operation appends an audit record. Every API-key-authenticated
call also appends a usage record.

```json
{
  "record_type": "matrixark_api_key_usage",
  "action": "matrixark_retrieve",
  "api_key_id": "key_...",
  "account_id": "acct_acme",
  "tenant_id": "tenant_eng",
  "user_id": "alice",
  "session_id": "codex-thread-123",
  "tenant_hash": 123,
  "user_hash": 456,
  "session_hash": 789,
  "used_at_ms": 1782110000000
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

Enterprise apps can keep sending their own user ids first. SSO mapping lets
larger deployments later connect Okta, Google Workspace, or Azure AD without
changing TemporalStore data models.

## Registration And Console Pages

The public website includes `website/matrixark-site/registration.html` for
account registration and API-key request planning. The local operations UI
includes `tools/temporalstore-monitoring-ui/access-management.html` for admins
to see the account, tenant, user, session, API-key, and audit tool map.

Both pages are intentionally static-friendly: they generate request payloads and
explain governance controls without requiring a live billing backend. A hosted
MatrixArk Cloud control plane can wire the same fields directly into the MCP
admin tools.

## Recommended Enterprise Setup

1. Create one account per customer or business unit.
2. Create one tenant per environment/workspace, such as `prod`, `staging`, or a
   regulated department. Disable a tenant for isolation incidents; disable an
   account for full customer suspension.
3. Register users from the customer app or SSO provider.
4. Issue separate API keys for hooks, MCP servers, batch ingestion, and admin
   automation.
5. Use user/session allow-lists for high-risk hooks or test deployments.
6. Disable users immediately during offboarding; existing context stays
   replayable for admins, but user-scoped API calls are blocked.
7. Rotate keys on a fixed schedule and revoke old hooks immediately when a user
   or integration is offboarded.
