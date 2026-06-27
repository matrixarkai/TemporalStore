# MatrixArk Installation, Operations, And Monitoring

MatrixArk should be as easy to run and inspect as an OpenViking-style context database, while using TemporalStore as the serving engine. The product surface should include:

- one local workspace directory
- Docker or native service startup
- MCP/API-key onboarding
- health and diagnostics
- monitoring UI
- Prometheus/Grafana metrics
- replayable ContextPack audits
- resource/skill ingestion inspection

## Install Shape

Recommended local layout:

```text
~/.matrixark/
├── matrixark.conf
├── data/
├── logs/
├── models/
├── resources/
├── skills/
└── health.json
```

`matrixark.conf` should carry the product-level settings:

```ini
[server]
host = 127.0.0.1
port = 8088
access_mode = dev

[identity.local]
account_id = acct_local
agent_name = codex
user_id = deeproute

[temporalstore]
backend = cpp        # cpp | rust
mode = local-single-node
storage = async
raft = false

[models]
embedding = sentence-transformers/all-MiniLM-L6-v2
reader = deterministic
vlm = disabled

[observability]
metrics_port = 9090
health_file = ~/.matrixark/health.json
log_level = info
```

Local mode derives:

```text
account_id = acct_local
tenant_id  = tenant_<agent_name>
user_id    = local OS account or MATRIXARK_LOCAL_USER_ID
node_path  = tenant:<tenant_id> / user:<user_id> / session:<session_id>
```

## Quickstart

Production C++/Rust MCP/server path:

```bash
export MATRIXARK_ACCESS_MODE=dev
export MATRIXARK_LOCAL_AGENT_NAME=codex
export MATRIXARK_LOCAL_USER_ID="$(whoami)"
export MATRIXARK_MCP_PROFILE=production
export MATRIXARK_MCP_BACKEND=temporalstore-rust     # or temporalstore-direct
export MATRIXARK_DIRECT_WRITE_QUEUE=1
export MATRIXARK_DIRECT_WRITE_QUEUE_MODE=temporalstore

python3 tools/matrixark_mcp_server.py \
  --backend "$MATRIXARK_MCP_BACKEND"
```

The local JSONL `record_log` adapter is now a debug/CI path. Production should
write context data directly to TemporalStore, using TemporalStore oplog,
MatrixArk audit records, and replay records for durability and inspection.

Apply a local API key:

```json
{
  "name": "matrixark_admin_apply_api_key",
  "arguments": {
    "agent_name": "codex",
    "scope": {"session_id": "local-thread-1"}
  }
}
```

The response returns `api_key`, `local_scope`, and `default_node_path`. Store the key in `MATRIXARK_API_KEY`.

## Docker Shape

The local Docker distribution should expose the same pieces:

```text
matrixark-server        MCP/API + context runtime
temporalstore-data      local C++ TemporalStore backend
temporalstore-rust      optional Rust TemporalStore backend
matrixark-models        optional OSS embedding/reader/VLM model cache
matrixark-monitoring    static console + health.json
prometheus              metrics scrape and alert rules
grafana                 optional dashboards
```

The current repo already includes:

- `tools/run_matrixark_cpp_docker_oss_scale.sh`
- `tools/temporalstore-monitoring-ui/`
- `tools/temporalstore-prometheus/docker-compose.yml`
- `tools/temporalstore-prometheus/vars-exporter/`

The installation target is to wrap those into one command:

```bash
matrixark-server init --home ~/.matrixark
matrixark-server doctor
matrixark-server start --backend cpp --local
matrixark-server start --backend rust --local
matrixark-server apply-key --agent codex
```

## C++ And Rust Store Parity

MatrixArk should operate against either TemporalStore implementation through the same product contract. The caller should not change API payloads, access scopes, ContextPack shape, monitoring views, or benchmark commands when switching backends.

| Area | C++ TemporalStore | Rust TemporalStore | Required parity |
| --- | --- | --- | --- |
| Local developer mode | Native process, direct SDK, or proxy/gateway. | Long-lived Rust proxy or binding; CLI-per-operation is debug only. | Same `backend=cpp|rust` switch in config and Docker. |
| Serving data | ContextNode, ContextSummary, ContextEmbedding, ContextEvent, ContextEntity, ContextIndex, ResourceChunk, SkillManifest, ContextPackAudit. | Same logical records and wire shape. | Same record keys, timestamps, ids, and replay output. |
| Ingestion | API, MCP, hook, batch/session commit, streaming, resource, skill, feedback. | Same ingestion APIs. | Same idempotency behavior and audit refs. |
| Retrieval | Tree-first traversal, secondary-index filtering, event/entity/resource/skill selection, token-budget packing. | Same retrieval semantics. | Same selected refs and dropped-ref reasons for parity tests. |
| Storage mode | Local single node, multi data node, async oplog, future Raft HA. | Local single node first, then gateway-backed async writes and future HA mode. | Same health state and metrics labels. |
| Benchmarking | LOCOMO, LongMemEval, scale tests, resource/skill tests. | Same unified tests. | Same artifacts: result JSON, report JSON/MD, hypotheses JSONL, ContextPack JSONL, judge JSONL. |

Backend selection should be explicit:

```bash
export MATRIXARK_TEMPORALSTORE_BACKEND=cpp
python3 tools/matrixark_mcp_server.py --event-log "$MATRIXARK_MCP_EVENT_LOG"

export MATRIXARK_TEMPORALSTORE_BACKEND=rust
python3 tools/matrixark_mcp_server.py --event-log "$MATRIXARK_MCP_EVENT_LOG"
```

Production Rust must not spawn a Rust CLI once per storage operation. That path is useful for feature parity tests, but product runs need a long-lived process, gateway, or in-process binding so latency and concurrency are comparable with C++.

Parity gates:

1. Same message/resource/skill input produces the same logical records.
2. Same query produces equivalent ContextPack sections, selected refs, dropped refs, and audit reasons.
3. Same LOCOMO/LongMemEval subset writes the same artifact types for both backends.
4. Same monitoring UI can inspect either backend using `temporalstore.backend`.
5. Same access-management rules apply before record reads and writes.

## Operations Console

The monitoring UI should be served at:

```text
/monitoring/
/observation/
/studio/        optional alias for OpenViking-style expectations
```

Operator views should cover:

- cluster and backend health
- ContextNode topology
- L0/L1 summary and embedding status
- event/entity/segment counts
- resource chunks and source refs
- skill manifests, triggers, and selected instructions
- query workbench
- retrieval trace and layer-by-layer traversal
- ContextPack replay
- API-key usage and audit logs
- dirty summary lag
- compression windows
- benchmark run artifacts

## Health And Diagnostics

Minimum health payload:

```json
{
  "status": "ready",
  "server": {"uptime_s": 120, "version": "dev"},
  "temporalstore": {
    "backend": "cpp",
    "mode": "local-single-node",
    "status": "ready",
    "storage": "async",
    "raft": false,
    "gateway": "ready"
  },
  "models": {"embedding": "ready", "reader": "deterministic"},
  "context_ops": {
    "ingest_ok": true,
    "retrieve_ok": true,
    "summary_dirty_count": 0,
    "last_context_pack_id": "..."
  },
  "access": {"mode": "dev", "active_api_keys": 1}
}
```

Diagnostic commands:

```bash
matrixark-server doctor
matrixark-server doctor --backend cpp
matrixark-server doctor --backend rust
matrixark-server list-keys
matrixark-server inspect-node tenant:tenant_codex/user:deeproute/session:thread-1
matrixark-server replay <context_pack_id>
matrixark-server refresh-summaries --limit 64
```

## Metrics

Expose Prometheus-compatible metrics for:

- request counts and errors by operation
- ingest, batch extract, retrieve, feedback, replay latency
- TemporalStore read/write latency and retry counts
- summary dirty backlog and refresh latency
- resource parse throughput and failure count
- skill parse throughput and selected-skill count
- context pack token use and dropped-token categories
- stale blocker count
- ContextPack replay/audit writes
- model calls, token usage, timeout, fallback, and cache hit rate
- access-denied, key-rotation, key-revocation, and expired-key events

Example metric names:

```text
matrixark_requests_total{operation,status}
matrixark_request_latency_ms_bucket{operation}
matrixark_context_pack_tokens{kind}
matrixark_summary_dirty_total{tenant}
matrixark_resource_parse_total{type,status}
matrixark_skill_selected_total{tenant}
matrixark_access_denied_total{reason}
matrixark_temporalstore_write_latency_ms_bucket{backend}
matrixark_temporalstore_read_latency_ms_bucket{backend}
matrixark_temporalstore_backend_status{backend,mode}
matrixark_temporalstore_gateway_restarts_total{backend}
```

## Runbooks

First checks:

1. Open `/monitoring/`.
2. Check `/health.json`.
3. Check `/metrics`.
4. Run `matrixark-server doctor`.
5. Replay the failing `context_pack_id`.
6. Inspect selected refs, dropped refs, stale blockers, and summary freshness.
7. Verify API key scope and account/tenant/user/session path.

Common fixes:

- Missing context: refresh summaries, inspect session buffer, check scope path.
- Bad stale answer: inspect entity supersession and stale blockers.
- Slow retrieval: check model encode latency, TemporalStore write/audit backlog, and tree traversal fallback.
- Resource miss: inspect parser output, chunk hashes, L0 summary, chunk embeddings, and resource access scope.
- Skill miss: inspect skill triggers, owner scope, status, precedence, and selected-skill audit.
- Backend mismatch: run the same parity fixture with `backend=cpp` and `backend=rust`; compare ContextPack JSONL, selected refs, dropped refs, and audit rows.
- Rust slow path: verify the Rust backend is a Rust proxy/binding, not CLI-per-operation.
- C++ slow path: verify async oplog, batch append, audit buffering, and data-node count before raising retrieval worker concurrency.

## Product Parity Target

OpenViking-style operation teaches the right expectation: context systems need an install path, a control surface, and diagnostics. MatrixArk should match that operator experience while keeping the serving architecture TemporalStore-native: local or distributed, C++ or Rust backend, one context store first, and optional MatrixDB/MatrixKV for offline analysis or transactional metadata.
