# Enterprise serving & ingestion architecture

For verticals that own their LLM loop (no CLI hooks). **Only TemporalStore (the storage engine) is
Rust; the entire MatrixArk front — MCP, HTTP, ingest FE, extraction orchestration — is Python**, served
by a production **ASGI** app (uvicorn/gunicorn). **Durability lives in TemporalStore's async ingestion,
which already *is* the queue — there is no queue in MatrixArk, and no external broker (no Kafka).**

```
LOCAL (CLI):    agent ──stdio MCP──► MatrixArk (Python, simple, per-process)

ENTERPRISE (front = Python ASGI; only TS = Rust):
  reads   agent ──POST /mcp──────► MatrixArk MCP-HTTP (Python ASGI, stateless, LB) ─► TemporalStore (Rust)
          app   ──POST /api/retrieve──► same
  writes  backend ─POST /api/ingest─► thin stateless FE (Python ASGI) ─► TemporalStore async ingest (Rust)
                                       (202 fast-ack; no buffer here)     store raw ContextEvent FAST
                                                                          (WAL/raft) → extract ASYNC = the queue
  blobs   MatrixObject ── resource files + durable COLD storage only (NOT the ingest queue)
```

## 1. The Python server — ASGI, not the stdlib toy

`matrixark_http.py`'s `ThreadingHTTPServer` is fine for pilots, not scale. The Python-native fix is a
production **ASGI** app — *implemented* in `tools/matrixark_asgi.py`:

- **Framework-free raw ASGI** — imports with no server lib installed; the ASGI server (uvicorn/gunicorn)
  is a runtime dependency only when you serve: `uvicorn matrixark_asgi:application --workers 4`.
- **Async concurrency over a sync backend** — `server.call_tool` / `mcp_http_dispatch` run in a
  threadpool (`asyncio.to_thread`), so the event loop stays free for high connection counts.
- **Routes:** `POST /api/ingest` (async fast-ack), `/api/retrieve`, `/api/session_commit`,
  `POST /mcp` (MCP-over-HTTP), `GET /healthz|/readyz`. Multi-tenant via the header API key.
- **Stays 100% Python** — only TemporalStore is Rust. No rust-proxy frontend.

## 2. Reads — distributed MCP-over-HTTP (`/mcp`)

stdio MCP is per-process/single-client — keep it for **local mode**. For enterprise, `POST /mcp`
(Streamable HTTP transport) pipes a JSON-RPC message (`initialize`/`tools/list`/`tools/call`) through
the **same `server.handle()`** as stdio:

- **Stateless** → any ASGI worker/replica serves any request; load-balance + scale horizontally.
- **Multi-tenant** → header API key injected into `tools/call` args; access model unchanged.
- `POST /api/retrieve` remains for non-MCP callers. *(Implemented: `mcp_http_dispatch` + `/mcp`.)*

## 3. Writes — TemporalStore async ingestion IS the queue (no MatrixArk queue, no Kafka)

The pipeline:

```
FE (Py) → raw msg → TemporalStore async ingest (Rust): store raw ContextEvent FAST → extract ASYNC
```

- **TemporalStore already provides the durable queue.** `async_processing` writes the raw event
  durably first (WAL; **raft** = durable on quorum replication), then runs extraction / summaries /
  embeddings **async** off the stored events. So TS is *both* the pre-extraction buffer *and* the write
  queue. **This is the whole ingestion path — no broker in front.**
- **No queue inside MatrixArk** — an in-process Python buffer isn't durable (lost on restart), is a
  SPOF, and re-implements badly what TS already does. The FE stays **thin + stateless**: authenticate →
  hand off to TS async → return **202**.
- **No Kafka.** TS-async covers durability, spike absorption (raw event lands fast, extraction drains
  async), and replay (WAL). A broker would only sit *before* extraction, and TS already occupies that
  role — so it's redundant here.
- **MatrixObject** = resource **blobs** + durable **cold** storage — never the hot ingest queue
  (object-store latency is wrong for millions of small appends/sec).

## 4. Message lifecycle (provisional vs. final)

Reuses the `finality` field: **provisional** (user turn, tool calls/results, reasoning) accumulate in
the per-session buffer; **final** (or `session_commit` / `final_session_boundary=true`) commits the
window → one-pass extraction. `session_buffer_threshold` + `idle_commit_timeout_ms` force interim
commits. Ingest *completed* messages, not partial streaming tokens.

## 5. Delivery, backpressure, robustness

- **At-least-once + idempotent:** client sends a content-derived `Idempotency-Key`; TS dedups
  (`idempotency_key` + `supersedes_chunk_hashes`). Re-delivery is a no-op.
- **Ordering** is per-session (sufficient).
- **Backpressure:** under overload the FE returns **429 + retry hint** (not 500); the client SDK backs
  off. *(Implemented in both the ASGI app and `_call_tool_route`: `MatrixArkBackpressureError` → 429.)*
- **Robust enough now?** Yes for production **when the FE is the ASGI app and writes land in TS-async
  (durable via raft)** — durability is in TemporalStore, not the FE.

## 6. Failure handling

- **Extraction failure** (async, off stored events) → retried by TS's async paths; the raw event is
  already durable, so nothing is lost.
- **FE crash** → stateless; a load balancer routes to another ASGI worker; in-flight requests retried
  by the client (idempotent).
- **Store unavailable** → FE applies backpressure (429) until TS recovers.
- **Replay** → TS WAL allows reprocessing a time range after an extraction fix, without the app re-sending.

## 7. Rollout (incremental, non-breaking — HTTP + SDK contracts unchanged)

1. **Now (shipped):** ingest/retrieve/commit routes, `/mcp` distributed reads, 429 backpressure, the
   **ASGI app** (`matrixark_asgi.py`), and the client SDK (buffer→flush, retry, idempotency). Writes
   land in TS-async (durable via raft). Serve under uvicorn/gunicorn.
2. **Scale the FE:** more uvicorn workers / replicas behind a LB; per-tenant quotas.
3. **Blobs:** MatrixObject via the rust-proxy blob RPC (separate item), cold storage only.

## 8. SLOs

Ingest **ack p99** (hand-off only) · **commit latency** (final → retrievable) · **retrieve p99** ·
`/mcp` p99 · backpressure (429) rate · extraction backlog depth.

## Verdict

- **Front = Python ASGI** (uvicorn/gunicorn); only **TemporalStore = Rust**.
- **Writes → TemporalStore async ingestion, which *is* the queue** (store raw fast → extract async);
  **no MatrixArk queue, no Kafka.**
- **Reads → distributed MCP-over-HTTP** (`/mcp`, stateless); stdio for local.
- **MatrixObject** = blobs + cold storage.
