<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# Deploying the TemporalStore Enterprise Cloud API (`/v1/*`)

The Cloud API gateway (`tools/matrixark_v1_gateway.py`) is the managed, multi-tenant HTTPS front for
teams that ingest resources/skills and pull managed context through **APIs** rather than agent hooks.
It is framework-free, stdlib-only Python (uvicorn is the only runtime dependency) and wraps the same
MatrixArk backend as the rest of `tools/`. It adds the four things an enterprise edge needs on top of
the internal API: **per-tenant bearer auth, token-bucket rate limiting, request/blob quotas, and a
streamed `/v1/blob/<key>` proxy** to the datanode.

## Enterprise onboarding

After you deploy the Cloud API, hand customers the **[Enterprise Onboarding](./enterprise_onboarding.html)** page. It is the single self-contained walkthrough of the ingest/retrieve API, the tenant/user/session **scope model**, **large-file ingestion** (the `/v1/blob/<key>` stream), **auth & API-keys**, and **mem0 migration** — everything a new team needs to go from a fresh deployment to live context.

## Endpoints

| Method | Path | Purpose | Success |
|---|---|---|---|
| `GET` | `/v1/healthz` | Liveness | `200 {"status":"ok"}` |
| `GET` | `/v1/readyz` | Readiness (shallow datanode probe) | `200 {"ready":true,"datanode":"ok"}`, or `503 {"ready":false,"datanode":"erroring\|unreachable"}` when the datanode cannot serve |
| `POST` | `/v1/ingest` | Write resources/skills/session events (async, fast-ack) | `202 {"accepted":n,"scope":...}` |
| `POST` | `/v1/session/commit` | Close a window; extract entities & summaries | `200` |
| `POST` | `/v1/retrieve` | Ranked, token-budgeted ContextPack | `200` |
| `POST` | `/v1/mcp` | Model Context Protocol over HTTP | `200` (JSON-RPC) |
| `PUT`/`POST` | `/v1/blob/<key>` | Stream a large attachment to shared storage | `200` receipt |
| `GET` | `/v1/blob/<key>` | Stream an attachment back | `200` (streamed) |

Every non-`/v1` path (`/api/*`, `/mcp`, `/healthz`) falls through to the legacy ASGI front unchanged,
so the gateway is a drop-in superset.

## Configuration (environment variables)

### Auth

The gateway has two auth postures. The **dev default** is anonymous (zero config, a one-time no-auth
warning is logged). **Production** enables the enforced hashed-keystore model below.

| Variable | Default | Meaning |
|---|---|---|
| `MATRIXARK_REQUIRE_AUTH` | `0` | Set `1` to require a valid bearer key on every `/v1` request (rejects anonymous). |
| `MATRIXARK_AUTH_ENFORCED` | `0` | Set `1` to turn on the **edge** hashed-keystore gate: identity, per-key **scopes**, and **allowed_user_ids/allowed_session_ids** are all enforced at the gateway. Production sets `MATRIXARK_REQUIRE_AUTH=1` **and** `MATRIXARK_AUTH_ENFORCED=1`. |
| `MATRIXARK_API_KEYS_HASHED_FILE` | – | Path to the hashed keystore (only sha256 **hashes** live here — plaintext keys are never present at the edge). JSONL of `matrixark_api_key` records, or a plain `{"<sha256hex>":{"tenant_id","account_id"}}` object. Loaded when `MATRIXARK_AUTH_ENFORCED=1`. |
| `MATRIXARK_API_KEYS` | – | **Legacy/back-compat fallback** (unenforced mode only): `"key1:tenantA,key2:tenantB"` plaintext per-tenant keys. |
| `MATRIXARK_API_KEYS_FILE` | – | **Legacy/back-compat fallback**: JSON `{"key":"tenant"}` (takes precedence over the CSV form). |

> `MATRIXARK_ACCESS_MODE` (below) is a **separate backend** knob, not the edge gate. The gateway's
> scope/user enforcement is driven by `MATRIXARK_AUTH_ENFORCED`; set `MATRIXARK_ACCESS_MODE=enforced`
> as well so the backend applies its own tenant isolation defense-in-depth.

A request presents its key as `Authorization: Bearer <key>` or `X-API-Key: <key>`. In enforced mode
the key's sha256 hash is resolved in the keystore; the `tenant_id`/`account_id` are pinned **from the
stored record** (never from client-supplied text), so two tenants cannot collide on a shared `scope`
string. `user_id`/`session_id` are preserved from the client `scope` (the tenant's own end-user axis).

**Minting keys.** Use the provisioner — the plaintext key is printed **once** (only its hash is
stored):

```bash
python3 tools/matrixark_provision_api_key.py \
  --tenant-id tenantA --account-id acct_a --prefix mk_live \
  --store /opt/temporalstore/gw/keys/api_keys_hashed.jsonl
# restricted keys:
#   --scope context:retrieve                 # retrieve-only key (repeatable and/or comma-separated)
#   --allowed-user-id alice,bob              # lock the key to these scope.user_id values
#   --allowed-session-id sess-1              # lock the key to these scope.session_id values
```

Default scopes (when `--scope` is omitted) are all four:
`context:ingest,context:retrieve,context:feedback,context:replay`. Revoke by re-appending a record
with the same `api_key_hash` and `--status revoked` (the gateway keeps the last active record per
hash); expired records (`expires_at_ms` in the past) are ignored.

**401 vs 403.** `401 {"error":"unauthorized"}` = missing / invalid / revoked / **expired** key.
`403` = a **valid** key that is not authorized for this request:

- `403 {"error":"insufficient_scope","required":"<scope>"}` — the route needs a scope the key lacks.
- `403 {"error":"user_not_allowed"}` — key has `allowed_user_ids` and the request `scope.user_id` is not in it.
- `403 {"error":"session_not_allowed"}` — same for `allowed_session_ids` vs `scope.session_id`.

A key with `scopes=None` (a legacy plain keystore entry) or empty allow-lists imposes **no**
scope/user restriction — backward compatible.

**Route → required scope** (health/readyz need none):

| Route | Required scope |
|---|---|
| `POST /v1/ingest` | `context:ingest` |
| `POST /v1/session/commit` | `context:ingest` |
| `POST /v1/ingest_file` | `context:ingest` |
| `PUT\|POST /v1/blob/<key>` | `context:ingest` |
| `POST /v1/retrieve` | `context:retrieve` |
| `GET /v1/blob/<key>` | `context:retrieve` |
| `POST /v1/mcp` | **per-tool** via `MATRIXARK_TOOL_SCOPES` (data tools → `context:*`, admin tools → `admin:*`); non-`tools/call` methods need only a valid key |

### Rate limits (token bucket, per key + route-class)
| Variable | Default | Meaning |
|---|---|---|
| `MATRIXARK_RL_INGEST_RPS` | `5000` | Sustained ingest/commit req/s per key. |
| `MATRIXARK_RL_INGEST_BURST` | `10000` | Ingest burst capacity. |
| `MATRIXARK_RL_RETRIEVE_RPS` | `6000` | Sustained retrieve/mcp req/s per key. |
| `MATRIXARK_RL_RETRIEVE_BURST` | `12000` | Retrieve burst capacity. |
| `MATRIXARK_RL_BLOB_STREAMS` | `500` | Max concurrent in-flight blob streams. |

Data responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` (seconds).
Over-limit returns `429 {"error":"rate_limited"}` with `Retry-After`.

### Quotas
| Variable | Default | Meaning |
|---|---|---|
| `MATRIXARK_QUOTA_MAX_BODY_BYTES` | `16777216` (16 MiB) | Max JSON request body → `413`. |
| `MATRIXARK_QUOTA_MAX_BATCH` | `1000` | Max records/messages per ingest → `413`. |
| `MATRIXARK_QUOTA_MAX_BLOB_BYTES` | `5368709120` (5 GiB) | Max attachment (by `Content-Length`) → `413`. |

A storage-quota signal from the backend surfaces as `507 {"error":"storage_quota_exceeded"}`.

### Datanode / backend
| Variable | Default | Meaning |
|---|---|---|
| `MATRIXARK_DATANODE_BLOB_URL` | `http://127.0.0.1:17102` | Datanode base for the streamed `/blob` proxy + `/readyz` probe. |
| `MATRIXARK_HTTP_HOST` | `0.0.0.0` | Gateway bind host. |
| `MATRIXARK_HTTP_PORT` | `8080` | Gateway bind port. |
| `MATRIXARK_ACCESS_MODE` | `dev` | Backend access model. **Dev default allows anonymous;** set `enforced` in production for per-tenant hashed keys + isolation. |

## Local: `docker compose up`

```bash
docker compose -f docker/docker-compose.cloud-api.yml up --build
# gateway on :8080, datanode on :17102, metaserver on :17101, proxy on :17100

curl -s http://127.0.0.1:8080/v1/healthz
# $MK is the plaintext key the provisioner printed once (enforced mode); dev mode needs no key.
curl -s http://127.0.0.1:8080/v1/ingest \
  -H "authorization: Bearer $MK" -H 'content-type: application/json' \
  -d '{"scope":"agent-7","records":[{"type":"resource_chunk","text":"hello"}]}'   # -> 202
curl -s -X PUT --data-binary @report.pdf \
  -H "authorization: Bearer $MK" http://127.0.0.1:8080/v1/blob/report.pdf
```

Run the tests (pure Python, seconds):

```bash
cd tools && python3 -m unittest test_matrixark_v1_gateway -v
```

## AWS deployment

```
Route53  api.temporalstore.ai ──► ALB (:443, ACM TLS) ──► Target Group (:8080) ──► ECS/EC2 gateway tasks
                                                                                        │
                                                             cloud-api ──► datanode (:17102, /blob) ──► MatrixObject
                                                                       └──► metaserver (:17101) / proxy (:17100)
```

1. **DNS + TLS.** Create a Route53 A/ALIAS record `api.temporalstore.ai` → the ALB. Issue an ACM
   certificate for `api.temporalstore.ai` (and regional `api.us.` / `api.eu.` names) and bind it to
   the ALB HTTPS listener (`:443`). TLS terminates at the ALB; the gateway speaks plain HTTP on 8080.
2. **ALB → gateway.** One HTTPS listener forwarding to a target group on port `8080`. Health check
   path `GET /v1/healthz` (200). Enable sticky-free round-robin; the gateway is stateless, so any
   task serves any request. Idle timeout ≥ 60s to allow large blob streams.
3. **Gateway service (ECS Fargate or EC2).** Run the `docker/Dockerfile.cloud-api` image with
   `uvicorn matrixark_v1_gateway:application --workers 4`. Inject auth/limit/quota env from SSM
   Parameter Store / Secrets Manager (never bake keys into the image). Point
   `MATRIXARK_DATANODE_BLOB_URL` at the datanode's internal DNS / NLB.
4. **Sizing (measured ~5,000 mixed ops/s per 8-core node, linear scale-out).** Start each task at
   **8 vCPU / 16 GB**, `--workers 4` (≈ 2 workers per core with async I/O). Budget one task per
   ~5,000 sustained ops/s and set ECS target-tracking autoscaling on CPU ~60% (or ALB
   `RequestCountPerTarget`). Two tasks across AZs is the HA floor; scale out horizontally from there.
   The storage tier (datanode/metaserver/proxy) scales independently.
5. **API-key management.** Mint keys with `tools/matrixark_provision_api_key.py` (hash-only keystore;
   plaintext printed once) and deliver the JSONL keystore to the gateway as
   `MATRIXARK_API_KEYS_HASHED_FILE` mounted from Secrets Manager, with `MATRIXARK_REQUIRE_AUTH=1` +
   `MATRIXARK_AUTH_ENFORCED=1`. Per-key `scopes` / `allowed_user_ids` / `allowed_session_ids` are set
   at mint time (`--scope` / `--allowed-user-id` / `--allowed-session-id`) and enforced at the edge.
   Rotate by appending a new record and revoke by appending a `--status revoked` record for the same
   `api_key_hash`, then roll the service (workers reload config on restart). The legacy plaintext
   `MATRIXARK_API_KEYS_FILE` / `MATRIXARK_API_KEYS` remain as an unenforced-mode fallback.

## 5-line deploy checklist
1. Build/push the gateway image from `docker/Dockerfile.cloud-api`.
2. Mint keys (`matrixark_provision_api_key.py`) into the hashed keystore in Secrets Manager; wire `MATRIXARK_REQUIRE_AUTH=1` + `MATRIXARK_AUTH_ENFORCED=1` + `MATRIXARK_API_KEYS_HASHED_FILE` + `MATRIXARK_DATANODE_BLOB_URL`.
3. ACM cert + Route53 `api.temporalstore.ai` → ALB HTTPS listener → target group `:8080`.
4. ECS service: 8 vCPU/16 GB tasks, `--workers 4`, health check `GET /v1/healthz`, CPU autoscaling.
5. Smoke-test `POST /v1/ingest` (expect `202`) and `GET /v1/readyz` before shifting traffic.


## Production backend & ingestion architecture (read this)

**Backend: use a Rust backend — `temporalstore-rust` or `temporalstore-direct`.** Both are Rust:
`temporalstore-rust` talks to the `matrixark_rust_proxy` binary + metaserver (the standard **cluster** path);
`temporalstore-direct` is **in-process Rust FFI** via the `libtemporalstore.so` cdylib built from
`sdk/rust/temporalstore` (fastest for a **single-node embedded** deploy — no proxy subprocess — but it needs
that cdylib built). The `local` backend is the single-user hook/dev path (in-process Python with
**synchronous embedding**) and must never front the multi-tenant API. For a cluster deployment, configure:

```bash
MATRIXARK_MCP_BACKEND=temporalstore-rust
MATRIXARK_TEMPORALSTORE_RUST_CLI=/usr/local/bin/matrixark_rust_proxy   # the built Rust proxy binary
MATRIXARK_TEMPORALSTORE_METASERVER=metaserver:17101
```

**Ingest must NOT call the model; extraction is deferred and batched.**
`/v1/ingest` should append the raw event durably and return `202` with **zero model calls**. Extraction and
embedding — the only model-calling step — run **batched**, triggered by `/v1/session/commit` or a
timeout/size threshold. Enable the fast-ack path:

```bash
MATRIXARK_HOOK_FAST_ASYNC_INGEST=1      # ingest stores raw and returns, no inline model
MATRIXARK_HOOK_AUTO_BATCH_EXTRACT=1     # extraction batched on commit/timeout
MATRIXARK_DIRECT_RAW_INGESTION_QUEUE=1  # raw-write fast path
MATRIXARK_RUST_PROXY_ASYNC_STORAGE=1
MATRIXARK_BULK_INGEST=1                 # group-commit durability
```

Without these, the gateway runs **synchronous per-message extraction** (an embedding call on every ingest),
which collapses throughput to a few QPS — a local scale test confirmed exactly this failure mode (the
gateway saturated 4–7 CPU cores while the datanode sat idle, requests never reaching the engine). With them,
ingest is engine-bound and models fire only on batched extraction.

**Retrieve needs a scalable embedding service.** Each retrieve performs one query-embedding — a hard latency
floor (~57 ms deterministic vs ~212 ms on a CPU sentence-transformer). Run a batched/GPU embedding service,
not a single CPU model, for high retrieve QPS.

**New gateway knobs:**

```bash
MATRIXARK_GATEWAY_THREADS=64             # lift the ~32/worker asyncio.to_thread cap
MATRIXARK_GATEWAY_BACKEND_TIMEOUT_MS=30000   # a hung backend returns 504, never stalls the caller
```
