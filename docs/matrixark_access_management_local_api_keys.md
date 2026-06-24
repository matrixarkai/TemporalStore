# MatrixArk Access Management: Local Agent Accounts And API Keys

MatrixArk supports enterprise account and tenant isolation, but local agent usage should be nearly zero-config. In local/dev mode, MatrixArk now derives the missing scope this way:

- `account_id`: defaults to `acct_local`.
- `tenant_id`: defaults to `tenant_<agent_name>`.
- `user_id`: defaults to the local OS account, or `MATRIXARK_LOCAL_USER_ID`.
- `session_id`: stays caller-provided when available; if omitted, the default context path uses the local user as the session fallback.

This means a Codex, Claude, Cursor, or local MCP agent can send a small envelope and still get isolated context:

```json
{
  "messages": [
    {"role": "user", "content": "Alice approved the GPU purchase."}
  ],
  "scope": {"session_id": "thread-123"},
  "agent_hook": {
    "source": "codex",
    "hook_type": "before_llm",
    "hook_id": "before-123",
    "observed_at_ms": 1781500000000,
    "auto_captured": true
  }
}
```

With that input, local mode derives:

```text
account_id = acct_local
tenant_id  = tenant_codex
user_id    = <local OS account>
node_path  = tenant:tenant_codex / user:<local OS account> / session:thread-123
```

## One-call API key application

Use `matrixark_admin_apply_api_key` to create or reuse the local account, agent tenant, local user, and API key in one call:

```json
{
  "agent_name": "codex",
  "scope": {"session_id": "thread-123"},
  "scopes": [
    "context:ingest",
    "context:retrieve",
    "context:feedback",
    "context:replay",
    "resource:read",
    "skill:read"
  ]
}
```

The response returns the raw key once, plus the derived `local_scope` and `default_node_path`.

```json
{
  "status": "applied",
  "account_id": "acct_local",
  "tenant_id": "tenant_codex",
  "local_scope": {
    "account_id": "acct_local",
    "tenant_id": "tenant_codex",
    "user_id": "deeproute",
    "session_id": "thread-123",
    "agent_name": "codex"
  },
  "default_node_path": [
    "tenant:tenant_codex",
    "user:deeproute",
    "session:thread-123"
  ],
  "api_key": "mk_local_..."
}
```

Store the returned `api_key` in `MATRIXARK_API_KEY`. MatrixArk stores only the key hash.

## API key behavior

- Keys are tenant-bound.
- Keys can be restricted by `allowed_user_ids` and `allowed_session_ids`.
- If a key is restricted to exactly one user, MatrixArk can infer that user when the caller only sends a session id.
- Raw keys are never returned by list APIs.
- Rotation revokes the old key and returns one replacement key.
- Audit and usage records are written for key creation, usage, rotation, and revocation.

## Enterprise mode

In enforced mode, admin operations require an admin API key. Local derivation still helps hook integrations build a scope, but account/tenant/user creation and key issuance remain governed by roles and scopes.
