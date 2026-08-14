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

Every data route requires a per-tenant bearer key.

| Header | Value | Notes |
|---|---|---|
| `Authorization` | `Bearer <api_key>` | preferred |
| `X-API-Key` | `<api_key>` | alternative |
| `Idempotency-Key` | `<uuid>` | optional — safe retries; dedup is backend-side |

Missing/unknown key on a `/v1` data route → **`401`** `{"error":"unauthorized"}`. Health routes need no
auth. The tenant is derived from the key and injected as `scope.tenant_id`; a tenant can never read
another tenant's data. Keys are configured via `MATRIXARK_API_KEYS` / `MATRIXARK_API_KEYS_FILE`.

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
JSON-RPC MCP message in the body → `200` with the MCP result. For MCP-native clients.

### `GET /v1/healthz` · `GET /v1/readyz` — probes (no auth)
`/healthz` → `{"status":"ok"}`. `/readyz` → `{"ready":true,"datanode":"ok|unknown|unreachable"}`.

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
| `401` | Missing/unknown API key |
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
| `MATRIXARK_API_KEYS` | — | `key:tenant,key:tenant` per-tenant keys |
| `MATRIXARK_API_KEYS_FILE` | — | path to JSON `{"key":"tenant"}` (use a secret store) |
| `MATRIXARK_REQUIRE_AUTH` | `1` | `0` allows anonymous (local/dev only) |
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
BASE=http://localhost:8080/v1; KEY=sk_live_demo
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

See also: `docs/DEPLOY_CLOUD_API.md` (deployment + AWS), `docker-compose.cloud-api.yml`.
