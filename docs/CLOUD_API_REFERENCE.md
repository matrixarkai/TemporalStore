<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 MatrixArkAI
-->
# TemporalStore Cloud API — `/v1` Reference

The `/v1` gateway (`tools/matrixark_v1_gateway.py`) is the multi-tenant HTTP surface enterprise
customers use to ingest resources/skills and retrieve managed context. All routes are authenticated
with a per-tenant API key over TLS. Base URL (production): `https://api.temporalstore.ai/v1`
(**coming soon** — self-host today by pointing your client at the gateway).

---

## Authentication

Every data route requires a per-tenant bearer key (an `mk_live_…` token issued by MatrixArk). The
gateway's dev default is anonymous (no key needed); production runs in enforced mode where the key is
resolved by hash and its authorization is checked on every request.

| Header | Value | Notes |
|---|---|---|
| `Authorization` | `Bearer <api_key>` | preferred |
| `X-API-Key` | `<api_key>` | alternative |
| `Idempotency-Key` | `<uuid>` | optional — safe retries; dedup is backend-side |

**Identity.** The `tenant_id` and `account_id` are pinned **from your key**, never from the request
body — a tenant can never read another tenant's data, and two tenants cannot collide on a shared
`scope` string. `user_id`/`session_id` are taken from your `scope` (your own end-user axis).

**Scopes (per route).** Each key carries a set of `scopes`; a route requires one:

| Route | Required scope |
|---|---|
| `POST /v1/ingest`, `/v1/session/commit`, `/v1/ingest_file`, `PUT\|POST /v1/blob/<key>` | `context:ingest` |
| `POST /v1/retrieve`, `GET /v1/blob/<key>` | `context:retrieve` |
| `POST /v1/mcp` | **per-tool** (see below) |

**`/v1/mcp` is gated per-tool**, not by a single route scope. The scope a `tools/call` needs is the
one the *called tool* needs — the same `MATRIXARK_TOOL_SCOPES` map the backend enforces — so data
tools (`matrixark_ingest`, `matrixark_retrieve`, …) require the matching `context:*` scope while admin
tools (`matrixark_admin_*`) require the matching `admin:*` scope. A data-only key therefore cannot
reach an admin tool through the MCP route. Non-`tools/call` methods (`initialize`, `tools/list`,
`ping`, notifications) need no tool scope but still require a valid key; an unmapped tool needs no
scope (matching the backend).

A key may also be restricted to specific `allowed_user_ids` / `allowed_session_ids`; a request whose
`scope.user_id` / `scope.session_id` is outside the allow-list is rejected (for `/v1/mcp` this is
checked against the `tools/call` `params.arguments.scope`).

**401 vs 403.**

| Status | Body | Meaning |
|---|---|---|
| `401` | `{"error":"unauthorized"}` | missing / invalid / revoked / **expired** key |
| `403` | `{"error":"insufficient_scope","required":"<scope>"}` | valid key, but it lacks the route's scope |
| `403` | `{"error":"user_not_allowed"}` | valid key, but `scope.user_id` is not in the key's allow-list |
| `403` | `{"error":"session_not_allowed"}` | valid key, but `scope.session_id` is not in the key's allow-list |

Health routes (`/v1/healthz`, `/v1/readyz`) need no auth. Operators mint keys with
`tools/matrixark_provision_api_key.py` into a hash-only keystore (`MATRIXARK_API_KEYS_HASHED_FILE`);
see the deploy guide.

---

## Endpoints

### `POST /v1/ingest` — write resources, skills, events (async `202`)
Appends the raw event durably and returns immediately. Extraction/embedding is **deferred** (models are
NOT called on this path) unless the request is finalized (see flags).

**Request body**
```json
{
  "scope": "agent-7",                     // string OR object (see Scope below)
  "messages": [                            // a conversation turn(s) …
    {"role": "user", "content": "roll staging to build 42"},
    {"role": "assistant", "content": "promoted build 42"}
  ],
  "records": [ {"type": "resource_chunk", "uri": "repo://…", "text": "…"} ],  // …or typed records
  "finalize": false                        // flag — see below
}
```
Provide **either** `messages` (conversation) **or** `records` (typed items).

**Ingest flags**
| Flag | Type | Effect |
|---|---|---|
| `finalize` | bool | `true` → this is a **complete conversation**: trigger batched extraction **now** (ingest + `session/commit` in one call). |
| `commit` | bool | alias of `finalize`. |
| `kind` | string | `"conversation"` behaves like `finalize:true`; `"message"` (default) buffers. |

A plain **message** buffers and extracts later (on `session/commit` or a server timeout). A **complete
conversation** extracts immediately — but always as **one batched pass** over the whole conversation,
never per-message. The caller declares granularity; the gateway never guesses.

**Response `202`**
```json
{ "accepted": 2, "scope": {"tenant_id":"acme","namespace":"acme/agent-7"},
  "result": {…},
  "finalized": true, "extraction": {…} }     // finalized/extraction only when finalize requested
```
On extraction failure the durable ingest is **not** lost — you get `"finalized": false` +
`"extraction_error"`. Quotas: > `max_batch` records or > `max_body_bytes` → **`413`**.

**PurchaseMemory fields (optional, additive)** — per-record TTL and keyed-upsert:
| Field | Type | Effect |
|---|---|---|
| `expires_at` | number (unix **seconds**) | Absolute expiry. The whole ingest closure is stamped ephemeral: it stops surfacing from retrieve / `get_all` once `now >= expires_at`, is **excluded from summaries/rollups**, and is lazily purged (durable, crash-safe). Wins over `ttl_seconds`. |
| `ttl_seconds` | number | Relative TTL → `expires_at = ingestion_time + ttl_seconds`. |
| `retention_cutoff_ts` | number (unix seconds) | Scope-level cutoff: records in the subject scope older than this are hidden and reclaimed. |
| `identity_key` | string | Logical identity of a fact (e.g. `user.email`). A later ingest with the same key + a **>=** `truth_class` rank supersedes the prior value (closure-tombstoned); a **lower** rank is rejected (`rank_guarded`, no write). |
| `truth_class` | string | Confidence class → rank (`asserted=3, reported=2, inferred=1, unknown=0`; override via `MATRIXARK_TRUTH_RANK`). |

`expires_at` / `ttl_seconds` may also be sent as the headers `X-Expires-At` / `X-Ttl-Seconds`
(the JSON body wins when both are present). The `202` `result` carries `upsert_outcome`
(`add` / `update` / `rank_guarded`) when `identity_key` was used.

> NOTE: TTL is enforced at the application layer (read-time filter + lazy tombstone-purge), which is
> correct for every backend. Native engine-level key-expiry (`ttl_ms`) wiring on the Rust temporal
> backend is a documented TODO; records DO carry `expires_at_ms`, which the temporal read path
> already honors via its retention filter.

### `GET /v1/memory/by-key?identity_key=…` — recall the current keyed value (`context:retrieve`)
Returns the single **current live** value for an `identity_key` in a scope (highest `truth_rank`
surviving record). Optional `user_id` / `agent_id` / `session_id` query params scope it (tenant is
pinned from the API key). `404` with `{"found": false}` when no live keyed value exists.
```json
// GET /v1/memory/by-key?identity_key=user.email&user_id=u1  →  200
{ "found": true, "identity_key": "user.email", "id": "…", "memory": "…", "text": "…",
  "truth_rank": 3, "truth_class": "asserted" }
```

---

### `POST /v1/session/commit` — close a window, extract now
Triggers the batched extraction over the accumulated (buffered) messages for a scope.
**Body** `{ "scope": … }` → **`200`** with the extraction result.

---

### `POST /v1/retrieve` — ranked, token-budgeted ContextPack (sync `200`)
Synchronous: waits and returns the actual pack. Does **one** query-embedding per call (the latency floor).
```json
// request
{ "query": "current staging build?", "scope": "agent-7", "token_budget": 1800 }
// response 200
{ "pack": [ {"text":"staging = 1.9.2", "source":"…"} ], "tokens": 214 }
```

---

### `PUT` / `POST` / `GET /v1/blob/<key>` — large attachments (streamed)
- `PUT`/`POST` streams the request body straight to the datanode's blob store (bounded memory, no full
  buffering) → receipt `{ "key": "<key>", "bytes": N }`. > `max_blob_bytes` → **`413`**; too many
  concurrent transfers → **`429`**.
- `GET` streams the object back with `Content-Length`.

---

### `POST /v1/mcp` — Model Context Protocol over HTTP
JSON-RPC MCP message in the body → `200` with the MCP result. For MCP-native clients. In enforced
mode this route is authorized **per-tool** via `MATRIXARK_TOOL_SCOPES` (the same map the backend
uses): a `tools/call` needs the called tool's scope — data tools require `context:*`, admin tools
require `admin:*` — so a data-only key is `403 insufficient_scope` on `matrixark_admin_*`, while
`initialize` / `tools/list` / `ping` need only a valid key. `params.arguments.scope.user_id` /
`session_id` are also checked against the key's `allowed_user_ids` / `allowed_session_ids`.

### `GET /v1/healthz` · `GET /v1/readyz` — probes (no auth)
`/healthz` → `200 {"status":"ok"}` whenever the process is alive; it does not consult the
datanode, because a liveness probe that fails on a dependency gets the container restarted and
that fixes nothing. `/readyz` → `{"ready":<bool>,"datanode":"ok|erroring|unreachable"}`, with
**200 only when the datanode can serve** and **503 otherwise** -- a gateway whose backend is
down takes itself out of rotation rather than accepting requests it cannot fulfil.

> **Back-compat:** any non-`/v1` path is delegated to the legacy front (`/api/ingest`, `/api/retrieve`,
> `/api/session_commit`, `/mcp`, `/healthz`).

---

## Scope (tenant isolation)
`scope` may be a **string** (`"agent-7"`) or an **object**. The gateway always normalizes it to an
object and injects the caller's tenant, so backends receive:
```json
{ "tenant_id": "acme", "namespace": "acme/agent-7", "user_id": "…", "session_id": "…" }
```
You cannot address another tenant's namespace; `tenant_id` is set from your API key, not the request.

---

## Status codes
| Code | Meaning |
|---|---|
| `202` | Ingest accepted (durably stored; extraction async unless finalized) |
| `200` | Retrieve / commit / mcp / blob OK |
| `400` | Malformed JSON body |
| `401` | Missing / invalid / revoked / expired API key |
| `403` | Valid key, not authorized: `insufficient_scope` (wrong scope) · `user_not_allowed` / `session_not_allowed` |
| `404` | Unknown path / missing blob key |
| `405` | Wrong method for the route |
| `413` | Payload too large (body, batch, or blob) |
| `429` | Rate limited — includes `Retry-After` + `X-RateLimit-*` |
| `500` | Backend error |
| `504` | Backend did not respond within the gateway timeout |
| `507` | Storage quota exceeded |

**Rate-limit headers** on data responses: `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Reset` (seconds).

---

## Deployment configuration (environment flags)

### Auth
| Env | Default | Purpose |
|---|---|---|
| `MATRIXARK_REQUIRE_AUTH` | `0` | **dev default: anonymous allowed.** Set `1` in production to require a valid key (missing/bad → `401`). A one-time startup warning is logged while off |
| `MATRIXARK_AUTH_ENFORCED` | `0` | Set `1` for the edge hashed-keystore gate: identity + per-key `scopes` + `allowed_user_ids`/`allowed_session_ids` enforced (403 on failure) |
| `MATRIXARK_API_KEYS_HASHED_FILE` | — | hash-only keystore (JSONL records or `{"<sha256>":{"tenant_id","account_id"}}`); loaded when enforced. Mint with `tools/matrixark_provision_api_key.py` |
| `MATRIXARK_API_KEYS` | — | **legacy fallback** (unenforced): `key:tenant,key:tenant` plaintext keys |
| `MATRIXARK_API_KEYS_FILE` | — | **legacy fallback**: path to JSON `{"key":"tenant"}` |
| `MATRIXARK_ACCESS_MODE` | `dev` | separate **backend** knob; set `enforced` for backend-side isolation (defense in depth) |
| `MATRIXARK_GATEWAY_FORWARD_API_KEY` | `1` | forward the edge key to the backend as its credential |

### Backend (see the deploy guide)
| Env | Default | Purpose |
|---|---|---|
| `MATRIXARK_MCP_BACKEND` | `local` | `temporalstore-rust` (prod, Rust proxy — **no `.so`**) · `temporalstore-direct` (in-process Rust FFI, needs the cdylib) · `local`/`temporalstore-local` (dev) |
| `MATRIXARK_TEMPORALSTORE_RUST_CLI` | — | path to `matrixark_rust_proxy` (for `temporalstore-rust`) |
| `MATRIXARK_TEMPORALSTORE_METASERVER` | — | metaserver address |
| `MATRIXARK_DATANODE_BLOB_URL` | `http://…:17102` | datanode target for streamed `/v1/blob` |

### Ingestion architecture — ingest without a model, batch the extraction
| Env | Purpose |
|---|---|
| `MATRIXARK_HOOK_FAST_ASYNC_INGEST=1` | ingest stores raw and returns — **no inline model** |
| `MATRIXARK_HOOK_AUTO_BATCH_EXTRACT=1` | extraction runs **batched**, on commit/timeout |
| `MATRIXARK_DIRECT_RAW_INGESTION_QUEUE=1` | raw-write fast path |
| `MATRIXARK_RUST_PROXY_ASYNC_STORAGE=1` | async storage |
| `MATRIXARK_BULK_INGEST=1` | group-commit durability |

### Rate limits & quotas
| Env | Default |
|---|---|
| `MATRIXARK_RL_INGEST_RPS` / `_BURST` | `5000` / `10000` |
| `MATRIXARK_RL_RETRIEVE_RPS` / `_BURST` | `6000` / `12000` |
| `MATRIXARK_RL_BLOB_STREAMS` | `500` (concurrent transfers) |
| `MATRIXARK_QUOTA_MAX_BODY_BYTES` | `16777216` (16 MiB) |
| `MATRIXARK_QUOTA_MAX_BATCH` | `1000` records |
| `MATRIXARK_QUOTA_MAX_BLOB_BYTES` | `5368709120` (5 GiB) |

### Performance & runtime
| Env | Default | Purpose |
|---|---|---|
| `MATRIXARK_GATEWAY_THREADS` | asyncio default (~`min(32,cpu+4)`) | size the backend threadpool per worker |
| `MATRIXARK_GATEWAY_BACKEND_TIMEOUT_MS` | `30000` | a hung backend returns `504`, never stalls the caller |
| `MATRIXARK_HTTP_HOST` / `MATRIXARK_HTTP_PORT` | `0.0.0.0` / `8080` | gateway bind |
| `TS_BLOB_CHUNK_BYTES` | 1 MiB | blob streaming chunk size |

---

## curl quickstart
```bash
BASE=http://localhost:8080/v1; KEY=mk_live_...   # plaintext printed once at mint time
# ingest a streaming message (buffers)
curl -sS $BASE/ingest -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"scope":"agent-7","messages":[{"role":"user","content":"hi"}]}'
# ingest a complete conversation (extract now)
curl -sS $BASE/ingest -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"scope":"agent-7","messages":[{"role":"user","content":"…"}],"finalize":true}'
# retrieve
curl -sS $BASE/retrieve -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"query":"staging build?","scope":"agent-7","token_budget":1800}'
# large attachment
curl -sS -X PUT --data-binary @report.pdf $BASE/blob/acme/report.pdf -H "authorization: Bearer $KEY"
```

See also: `docs/DEPLOY_CLOUD_API.md` (deployment + AWS), `docker/docker-compose.cloud-api.yml`.
