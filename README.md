# TemporalStore

**Open-source temporal infrastructure for LLM memory.**
The universal, Rust-native store that context-management platforms build their agent memory on —
ingest, retrieval, features, and control in one durable, time-aware engine.
Apache-2.0 · self-hostable · no vector DB · [temporalstore.ai](https://temporalstore.ai)

📊 **Benchmarks & docs — [temporalstore.ai/benchmarks.html](https://temporalstore.ai/benchmarks.html)**

TemporalStore is the durable, time-aware foundation for **LLM memory at any scale** — the
universal infrastructure context-management platforms build on. Bring your own data models
and business logic; let one engine handle real-time ingest, entity/summary extraction, ranked
token-budgeted **ContextPack** retrieval, exact serving-time **feature aggregates**, and O(1)
**control state** (caps, quotas, pacing) — all from one temporal index. It runs locally with
one Docker command and scales out to a replicated, shared-storage cluster.

---

## What you get

| | |
|---|---|
| 🧠 **Agent memory** | Ingest → extract → retrieve a ranked ContextPack. Cross-session, cross-device, cross-agent, with a long-term profile. **No vector database.** |
| 📊 **Aggregated features** | Exact count/sum/min/max/avg over high-cardinality keys, on read — **no Flink/Spark pre-aggregation pipeline.** |
| 🎛️ **Control state** | Frequency caps, quotas, pacing, suppression — single-key **O(1)** atomic updates at serving time. |
| ⚡ **Rust-native** | Append-structured page store, no GC pauses, crash-safe reload from its own persistence. |
| 🔌 **Speaks RESP** | A Redis-compatible surface (strings, hashes, sets, control verbs) — existing Redis clients connect today. |

## Why it matters

Grounding an agent usually means running five systems — a vector DB, a feature store, a
Redis-style counter tier, a stream/log pipeline, and a bespoke memory service. TemporalStore
collapses that into **one time-aware engine**. Concretely, for enterprises that own their
model loop and pay per token, replaying a growing local context every turn is the dominant
cost; a bounded managed pack cuts it dramatically.

## Proof it works

Measured with open-source reader models (Ollama + Qwen) and independent ground truth — full
methodology and per-dataset numbers in [docs/benchmarks](docs/benchmarks/README.md), with the
live report at **[temporalstore.ai/benchmarks.html](https://temporalstore.ai/benchmarks.html)**:

- **~89–99% fewer prompt tokens** at equal-or-better answer quality on deep sessions —
  a real-transcript **median of 484k tokens/turn** of replayed context collapses to a
  **~4k** working pack. Up to **97%** on single-turn replays.
- **Retrieval hit@k 0.98–0.995** on LoCoMo & LongMemEval_s.
- **~0.23 ms p50** exact feature-aggregate read (~3.9k QPS/core, **0 mismatches**).
- **~17 ms p95** ContextPack retrieval at a 1.2k-token budget.

---

## Quick start (single node, Docker)

You need only [Docker](docs/INSTALL.md#step-0-install-docker-if-you-dont-have-it) and a clone
— no Rust toolchain on your host (it lives inside the build stage).

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build
```

The node listens on:
- `http://127.0.0.1:17101` — metaserver: cluster metadata + health
- `http://127.0.0.1:17102` — datanode: health, plus writes/reads via `POST /execute`

Health-check and do a write/read round trip:

```bash
curl http://127.0.0.1:17102/health

# write: key "hello" = bytes for "world"
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_set","key":"hello","value":[119,111,114,108,100]}}'

# read it back
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_get","key":"hello"}}'
```

Data persists in the `temporalstore-data` volume across restarts. Stop with `Ctrl-C`; remove
node + data with `docker compose -f docker-compose.single-node.yml down -v`. macOS / Windows /
native (non-Docker) builds are covered step by step in the [Install Guide](docs/INSTALL.md).

---

## Use it with your agent

TemporalStore installs as a memory layer for coding agents — automatic ingest/inject on every
turn, plus recall/remember tools.

### Claude Code (marketplace plugin)

```text
/plugin marketplace add matrixarkai/TemporalStore
/plugin install matrixark-memory@temporalstore
```

This wires the lifecycle hooks (ingest each turn, inject a ContextPack on prompt) and the MCP
`recall` / `remember` tools, backed by the Rust engine. Plugin manifest:
[`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json).

### Codex (MCP + hooks)

Codex integrates over MCP with the same tool surface. The one-time setup (config.toml MCP entry
+ notify hook) is in the
[Codex MCP/hook installation manual](docs/matrixark_codex_mcp_hook_installation_manual.md). The
underlying scripts are [`tools/matrixark_claude_hook.sh`](tools/matrixark_claude_hook.sh) and
[`tools/run_matrixark_mcp_server.sh`](tools/run_matrixark_mcp_server.sh) — usable from any
MCP-capable client.

---

## Enterprise Cloud API (ingest & retrieve at scale)

For teams that ingest their own resources and skills programmatically — through **APIs rather than
agent hooks** — TemporalStore Cloud exposes a managed, multi-tenant HTTPS endpoint. Every route is
authenticated with a per-tenant API key over TLS; the ingest path is **asynchronous**, so high-QPS
producers never block on durability.

> **Managed endpoint coming soon.** `api.temporalstore.ai` is being stood up; the same API runs today
> on a self-hosted cluster (point your client at the proxy). Deploy it yourself with the
> [Cloud API deployment guide](docs/DEPLOY_CLOUD_API.md) — Docker Compose or AWS ECS.

| Endpoint | Purpose | Shape |
|---|---|---|
| `POST /v1/ingest` | Write resources, skills, session events | async `202`; batch up to 1,000 records |
| `POST /v1/session/commit` | Close a window; extract entities & summaries | one pass over the session |
| `POST /v1/retrieve` | Ranked, token-budgeted **ContextPack** | read path; p50 < 2 ms |
| `PUT`/`GET /v1/blob/<key>` | Large attachments | streamed to shared storage |
| `POST /v1/mcp` | Model Context Protocol over HTTP | for MCP-native clients |
| `GET /v1/healthz` · `/readyz` | Liveness / readiness | probes |

```bash
# ingest — async, fast-ack (202)
curl -sS https://api.temporalstore.ai/v1/ingest \
  -H 'authorization: Bearer sk_live_...' -H 'content-type: application/json' \
  -d '{"scope":"acme/agent-7/session-42","records":[
        {"type":"resource_chunk","uri":"repo://api/handler.rs","text":"pub async fn handle(...)"},
        {"type":"skill_section","name":"deploy-runbook","text":"1. drain 2. roll 3. verify"}]}'
# -> 202 {"accepted": 2}

# retrieve — a ranked, token-budgeted ContextPack
curl -sS https://api.temporalstore.ai/v1/retrieve \
  -H 'authorization: Bearer sk_live_...' -H 'content-type: application/json' \
  -d '{"query":"current staging build and how to roll it","scope":"acme/agent-7","token_budget":1800}'
# -> 200 {"pack":[{"text":"staging = 1.9.2","source":"session-42#evt-8"}, ...],"tokens":214}

# large attachments stream straight to shared storage
curl -sS -X PUT --data-binary @report-q3.pdf \
  https://api.temporalstore.ai/v1/blob/acme/report-q3.pdf
```

**Auth, rate limits & quotas.** Per-tenant bearer keys, scoped to namespaces and rotated from the
portal; regional endpoints (`api.us.temporalstore.ai`, `api.eu.temporalstore.ai`) keep data in-region.
Limits are enforced per key with a token bucket and reported in `X-RateLimit-*` headers (`429` +
`Retry-After` when exceeded). Enterprise defaults, raised per contract:

| Limit / quota | Default |
|---|---|
| `POST /v1/ingest` | 5,000 req/s sustained · 10,000 burst |
| `POST /v1/retrieve` | 6,000 req/s sustained · 12,000 burst |
| Mixed ingest + retrieve | ~5,000 ops/s per 8-core node · linear scale-out |
| Max attachment (`/v1/blob`) | 5 GB, streamed |
| Max ingest batch | 1,000 records / 16 MB body |
| Storage per tenant | 1 TB, expandable |

Large files land in **MatrixObject** shared storage via `append_blob`; tenant metadata (accounts, keys,
scopes) is stored as KV in TemporalStore itself by default, with **MatrixKV** as an optional
transactional metadata plane. Self-hosted clusters expose the same operations through the proxy — see
the [Deployment guide](https://temporalstore.ai/deployment.html#cloud-api).

---

## Run with open-source models (local-first, no API key)

**Retrieval itself needs no model** — the ContextPack is ranked by term + temporal + entity
signal, with no embeddings round-trip. Models are only used where you want an LLM: the
benchmark reader/judge, and optional extraction/summarization. All of it runs on **open-source
models via [Ollama](https://ollama.com)** with no API key.

```bash
# install a local OSS model for the reader/judge and optional extraction
ollama pull qwen2.5:7b        # or qwen2.5:1.5b for a smaller/faster reader
ollama serve                  # 127.0.0.1:11434

# reproduce the token/quality benchmark end-to-end with the OSS reader
python3 tools/run_local_context_token_quality_sweep.py \
  --reader ollama --reader-model qwen2.5:7b --judge ollama
```

- **Reader/judge:** any Ollama model (`qwen2.5:1.5b`, `qwen2.5:7b`, …) via the OpenAI-compatible
  endpoint; an Anthropic reader is available too (`--reader anthropic`).
- **Embeddings:** MiniLM-class local embeddings; no hosted embedding service required.
- **Extraction/summarization providers** are pluggable (`understanding` / `extraction` /
  `segment` providers) — swap in a local model or disable for pure deterministic extraction.

See [docs/context_benchmarks_docker_open_model.md](docs/context_benchmarks_docker_open_model.md)
for the fully containerized OSS-model benchmark.

---

## Configuration (common env vars)

Every knob is environment-overridable; defaults are tuned for large-window serving.

| Env var | Default | What it does |
|---|---|---|
| `MATRIXARK_CONTEXT_SOURCE_MODE` | `auto` | `remote_only` (managed pack reconstructs context) or `local_and_remote` (augment local with cross-session memory) |
| `MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS` | `500000` | retrieval context window (budget ceiling) |
| `MATRIXARK_SKILL_DISCOVERY` | `0` | mine reusable skills from sessions on commit (discover → capture → learn) |
| `TS_STORAGE_BACKEND` / `TS_SHARED_STORE_DIR` | auto | distributed storage backend: object store → shared filesystem → replicated local |
| `MATRIXARK_OBJECT_RPC_URL` / `MATRIXARK_OBJECT_STORE_DIR` | — | store resource/skill raw blobs in **MatrixObject** (object storage) in distributed mode |
| `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD` | on | promote disk → memory on restart for a warm start |

---

## Deploy: laptop → replicated cluster

- **Local single node** — one Docker command (above); durable memory in a local volume, no metaserver dependency.
- **Distributed** — replicate through **MatrixRaft** consensus; the storage backend auto-resolves
  **object store → shared filesystem → replicated local disk**. Resource/skill raw content
  offloads to **MatrixObject** (content-addressed, deduped) while metadata stays in the store.
- **Context modes & budgets, startup/recovery, storage resolution** are documented in
  [docs/benchmarks](docs/benchmarks/README.md) and the deploy manuals below.

## Architecture (open core)

```
Agents ── Codex hook · Claude Code plugin · Redis (RESP) / SDK / proxy
   │
Engine (OSS) ─ TemporalStore: temporal engine · context pipeline · append-structured page store
   │
Foundation (OSS) ─ MatrixCache (multi-layer cache) · MatrixRaft (Rust Raft consensus)
   │
Storage backend (auto) ─ MatrixObject → shared filesystem → local + Raft
```

Three Apache-2.0 repositories: [TemporalStore](https://github.com/matrixarkai/TemporalStore) ·
[MatrixCache](https://github.com/matrixarkai/MatrixCache) ·
[MatrixRaft](https://github.com/matrixarkai/MatrixRaft).

---

## Build & test (from source)

```bash
cargo check -p temporalstore-rust --all-targets
cargo test  -p temporalstore-rust --lib --tests -- --test-threads=1
```

Focused harnesses:

```bash
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports
cargo run -p temporalstore-rust --bin context_workflow_harness
cargo run -p temporalstore-rust --bin storage_modes_harness
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

Fast repository checks:

```bash
cargo fmt --all -- --check
python3 tools/validate_open_source_readiness.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

## Status & evidence

Apache-2.0. The project's production-readiness claims should be read from passing readiness reports, not this
README alone:

- [Benchmarks: token & quality vs full local replay](docs/benchmarks/README.md)
- [Context Management on TemporalStore](docs/context_management_on_temporalstore.md) ·
  [technical blog](docs/blog_context_management_temporalstore.md)
- Deploy: [Windows Docker](docs/windows_docker_install.md) · [Linux](docs/linux_deploy.md) · [macOS](docs/macos_deploy.md)

Out of scope unless separately re-added: alternate wire-protocol compatibility (no brpc/thrift wire-compatible clone) and live external object-store (S3) integration.

## Contributing

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) and [SECURITY.md](docs/SECURITY.md). Contributions target the `rust-main` branch. New product-behavior tests
should reference a shared corpus case with `shared-corpus: <case_id>`; Rust-only implementation
tests should be marked `rust-internal: <reason>`.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](LICENSE), [`NOTICE`](NOTICE)).
Third-party dependency licenses and attributions are listed in
[`THIRD-PARTY-NOTICES.md`](docs/THIRD-PARTY-NOTICES.md). Product and crate names are trademarks
of MatrixArkAI; see [`TRADEMARKS.md`](docs/TRADEMARKS.md).
