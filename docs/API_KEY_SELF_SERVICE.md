<!-- SPDX-License-Identifier: Apache-2.0 -->
# API-Key Self-Service & Per-Key Usage Metering

Self-service lifecycle management for MatrixArk API keys (create / list / rotate / revoke), a
customer-facing **HTML portal UI**, a lightweight per-key request **meter** at the `/v1` edge, and
per-key request **quota enforcement** off that meter. Key crypto and the usage meter are pre-existing
primitives — the portal and quota are built **on top** of them, additively. **Billing / payment
integration is a future follow-up** (see [Deferred](#deferred)); the quota here is observe-and-limit,
never payment processing.

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

## 3. Portal UI (HTML)

A self-contained, single-file HTML portal (inline CSS/JS, no external dependencies) lets an operator
or customer-admin manage keys and watch usage from a browser. It is committed at
[`tools/portal/api_key_portal.html`](../tools/portal/api_key_portal.html) and served by the gateway:

```
GET /v1/admin/portal        ->  200 text/html  (the static page; NO auth to fetch)
```

Fetching the static page needs no auth, but the page is **inert without a valid admin key**: every
action button calls one of the admin-gated JSON endpoints above with the admin bearer key the
operator pastes into the **Connection** panel. That key is held in `sessionStorage` for the tab only
(never hardcoded, never persisted to disk) and sent as `Authorization: Bearer <key>`.

Features:

- **Create key** — tenant / account / scopes / role / display-name / key-prefix / allowed user IDs,
  plus **request quota** and **quota window** inputs. Shows the plaintext key **once**.
- **List keys** — `api_key_id`, tenant, scopes, quota, status, created, last-used, usage-count; an
  *include revoked* toggle.
- **Rotate** / **Revoke** — per-row, calling `rotate_api_key` / `revoke_api_key`.
- **Live edge usage** — the `GET /v1/admin/api_key_usage` snapshot (keys shown hashed).

Endpoint origins are configurable in the Connection panel (blank = same origin): a **key-management
API base** for the `/api/admin/*` routes (served by the management-portal HTTP facade) and a
**gateway base** for `/v1/admin/api_key_usage`. When the portal is served from the gateway, leave the
gateway base blank (same-origin) and point the key-management base at the management-portal server;
its CORS policy allows the cross-origin admin calls.

## 4. Per-key request quota enforcement

Per-key request quotas are enforced at the `/v1` edge **off the usage counter the meter already
tracks** — this is observe-and-**limit**, not billing.

### Key record field

`create_api_key` and the provisioner mint an optional `request_quota` (int, max requests in the
window) and optional `quota_window` (seconds). The gateway carries them to the edge exactly like
`scopes`, via `_normalize_key_record`:

| Field | Meaning |
| ----- | ------- |
| `request_quota` | Max requests per window. Absent / `null` / `0` → **UNLIMITED** (backward compatible). |
| `quota_window` | Rolling window in seconds. Absent / `0` → a per-process **lifetime** window (never resets). |

A record with no `request_quota` is byte-identical to the pre-quota shape, so legacy keystores and
un-quota'd keys behave exactly as before.

### Edge check + 429

After metering a request, the gateway compares the key's request count in the current window against
its `request_quota` (an O(1) read of the in-memory meter counter). The `request_quota`-th request in
a window is the last allowed; the next one is rejected:

```
HTTP 429
Retry-After: <seconds until the window resets>
X-RateLimit-Quota-Limit: <N>
X-RateLimit-Quota-Remaining: 0
X-RateLimit-Quota-Reset: <seconds>

{"error":"quota_exceeded","limit":N,"used":M}
```

Properties:

- **Enforced-mode only.** Metering (and therefore quota) runs only when a real key authenticated the
  request. **Dev / anonymous traffic is never limited** — the dev default posture is byte-identical.
- **Hot-path cheap.** Reuses the meter's in-memory per-key counter; the check is an O(1) compare.
- **Best-effort.** The whole meter+quota step is wrapped in `try/except` returning "allow": a
  quota-check bug can **never** wrongly block a legitimate request or crash the hot path.
- Enforced uniformly across the data routes (`/v1/ingest`, `/v1/retrieve`, `/v1/session/commit`,
  `/v1/mcp`), the blob proxy, and `/v1/ingest_file`.

### Provisioner flags

```
python3 matrixark_provision_api_key.py --tenant-id tenantA --account-id acct_a \
    --scope context:ingest,context:retrieve \
    --request-quota 1000 --quota-window 60 \
    --store /opt/temporalstore/gw/keys/api_keys_hashed.jsonl
```

`--request-quota N` (omit or `0` → unlimited) and `--quota-window S` (omit → lifetime window). The
fields are written to the hashed record only when a positive quota is given, keeping un-quota'd
records byte-identical to the previous shape.

## 5. Environment variables

| Var | Default | Meaning |
| --- | ------- | ------- |
| `MATRIXARK_API_KEY_USAGE_FILE` | `""` (in-memory only) | Path the edge meter snapshots to. |
| `MATRIXARK_API_KEY_USAGE_FLUSH_EVERY` | `50` | Flush after this many recorded requests. |
| `MATRIXARK_API_KEY_USAGE_FLUSH_INTERVAL_S` | `5.0` | Or flush after this many seconds. |
| `MATRIXARK_AUTH_ENFORCED` | `0` | Enforced mode; required for metering to run. |

## Deferred

- **Billing / payment integration.** The quota above is observe-and-**limit** only — it counts
  requests and returns `429` when a key is over its allowance. Turning metered usage into invoices,
  metered pricing, plan tiers, or a payment processor (e.g. usage-based billing, hard credit caps,
  proration) is a **future follow-up** and is intentionally out of scope here.
- **Per-tenant (aggregate) quotas.** Enforcement today is per **key**; rolling a tenant's keys up to
  a shared tenant-level allowance is a natural next increment on top of the same counter.
