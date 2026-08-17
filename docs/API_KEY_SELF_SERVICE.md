<!-- SPDX-License-Identifier: Apache-2.0 -->
# API-Key Self-Service & Per-Key Usage Metering

Self-service lifecycle management for MatrixArk API keys (create / list / rotate / revoke) plus a
lightweight per-key request meter at the `/v1` edge. This is the product-UX "#5" increment: the API
and metering surface. A customer-facing HTML portal UI is a **future follow-up** (see
[Deferred](#deferred)).

## 1. Self-service key endpoints

All key lifecycle operations already exist as MatrixArk admin tools and are reachable over HTTP. They
reuse the audited key-crypto primitives in `tools/matrixark_access_apikey.py`
(`create_api_key` / `rotate_api_key` / `revoke_api_key` / `list_api_keys`) — no crypto is
reimplemented. Each is gated by the `admin:api_key` scope (`MATRIXARK_TOOL_SCOPES` in
`tools/matrixark_mcp_core.py`), so a caller needs an **admin-scoped** key.

### Portal HTTP facade (`tools/matrixark_http.py`)

| Method | Route | Tool | Scope |
| ------ | ----- | ---- | ----- |
| POST | `/api/admin/create_api_key` | `matrixark_admin_create_api_key` | `admin:api_key` |
| GET/POST | `/api/admin/list_api_keys` | `matrixark_admin_list_api_keys` | `admin:api_key` |
| POST | `/api/admin/rotate_api_key` | `matrixark_admin_rotate_api_key` | `admin:api_key` |
| POST | `/api/admin/revoke_api_key` | `matrixark_admin_revoke_api_key` | `admin:api_key` |

Pass the admin key as `Authorization: Bearer <key>` (or `X-MatrixArk-API-Key`). In enforced mode a
missing/invalid/revoked key is rejected; a valid but non-admin (data-only) key is denied for lack of
`admin:api_key`.

### Cloud `/v1` gateway (`tools/matrixark_v1_gateway.py`)

The same admin tools are reachable through the MCP-over-HTTP route `POST /v1/mcp` as a JSON-RPC
`tools/call`. The edge gates that route **per tool** against the same `MATRIXARK_TOOL_SCOPES` map the
backend enforces, so a data-only key cannot reach `matrixark_admin_*` through `/v1/mcp` — it needs
`admin:api_key`. Example body:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call",
 "params":{"name":"matrixark_admin_create_api_key","arguments":{"tenant_id":"t","scopes":["context:ingest","context:retrieve"]}}}
```

`create` returns the plaintext key **once** (`api_key`) plus its `api_key_id`; MatrixArk stores only
the hash. `rotate` revokes the old key and mints a replacement carrying the same scopes/identity.
`revoke` marks a key inactive — the next request that key makes is rejected (401) at the edge.

## 2. Per-key usage metering

### Durable per-key usage (pre-existing, backend)

When the backend resolves an **API-key** identity it appends a `matrixark_api_key_usage` record
(`MatrixArkAccessManager.append_api_key_usage`), and `matrixark_admin_list_api_keys` aggregates those
into per-key `usage_count`, `last_used_at_ms`, and `last_used_action`. That is the durable,
cross-process usage read and is unchanged by this increment.

### Live edge meter (new, gateway)

The `/v1` gateway keeps a cheap in-process counter (`_UsageMeter`) that is bumped once per
**authenticated** request, immediately after the bearer key is validated. It is hot-path safe:

- **O(1) per request.** Each hit does a single dict update under a short lock — **no disk I/O on the
  request path.**
- **Amortized flush.** In-memory counters are snapshotted to `MATRIXARK_API_KEY_USAGE_FILE` at most
  every `MATRIXARK_API_KEY_USAGE_FLUSH_EVERY` recorded requests **or**
  `MATRIXARK_API_KEY_USAGE_FLUSH_INTERVAL_S` seconds, whichever comes first (atomic temp-file +
  `os.replace`). An empty file path keeps the counter purely in memory (still readable via the API).
- **Best-effort.** Every meter call is wrapped so a metering failure (bad path, disk full, internal
  error) is swallowed and **never** breaks or delays the request.
- **Privacy.** Keys are stored **hashed** (sha256), never in plaintext.
- **Enforced-mode only.** Metering runs only when a real key authenticated the request
  (`enforced` mode). Dev / anonymous (unenforced) traffic is **never** metered — the dev default
  posture is byte-identical.

Per key it tracks: `total`, `ingest`, `retrieve`, `other`, `bytes` (request-body bytes for the JSON
data routes), `first_used_at_ms`, `last_used_at_ms`, plus `tenant_id` / `account_id`.

### Read endpoint

```
GET /v1/admin/api_key_usage
Authorization: Bearer <admin-scoped key>
```

Returns the live in-process snapshot, gated behind `admin:api_key` **or** `admin:audit` (a scoped
enforced key without either is denied 403; a legacy/unrestricted keystore key reads it unchanged,
consistent with the rest of the edge). Response:

```json
{"status":"ok","count":1,
 "usage":[{"api_key_hash":"<sha256>","tenant_id":"t","account_id":"acct",
           "total":5,"ingest":3,"retrieve":2,"other":0,"bytes":128,
           "first_used_at_ms":1,"last_used_at_ms":9}]}
```

## 3. Environment variables

| Var | Default | Meaning |
| --- | ------- | ------- |
| `MATRIXARK_API_KEY_USAGE_FILE` | `""` (in-memory only) | Path the edge meter snapshots to. |
| `MATRIXARK_API_KEY_USAGE_FLUSH_EVERY` | `50` | Flush after this many recorded requests. |
| `MATRIXARK_API_KEY_USAGE_FLUSH_INTERVAL_S` | `5.0` | Or flush after this many seconds. |
| `MATRIXARK_AUTH_ENFORCED` | `0` | Enforced mode; required for metering to run. |

## Deferred

- **Customer-facing HTML portal UI** for self-service key management and a usage dashboard — this
  pass ships only the API + metering surface.
- **Billing / quota enforcement** off the usage counters (metering is observe-only here).
