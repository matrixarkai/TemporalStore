# TemporalStore Monitoring UI

Standalone console for TemporalStore operations and observability.

Open `index.html` directly in a browser, or deploy the folder behind Nginx on the
metaserver node. The UI reads `/health.json`; when live health data is unavailable
it renders a safe pending-state sample.

On the MatrixArk test server, both public paths should serve this same console:

- `/monitoring/`
- `/observation/`
- `/studio/` as an optional OpenViking-style operations alias

Covered views:

- Cluster overview
- Node level monitoring
- Partition and replica topology
- LLM context extraction, ingestion, retrieval, feedback, and replay operations
- Query workbench for intent, filters, traversal controls, token budget, and pack output
- Context tree traversal, token-budgeted pack preview, and resource chunk citations
- Context runtime configuration for extraction, traversal, resources, and summaries
- Open-source model registry for embeddings, reranking, extraction, summaries, and VLM/document parsing
- Operator console for query extraction, tree traversal, resource ingestion, pack replay, feedback, and summary refresh
- Agent Context Envelope page for the minimal message/hook payload MatrixArk expects from AI agents
- Access management for local agent accounts, API-key application, key rotation,
  key revocation, user/session isolation, and audit logs
- Diagnostics and trace samples
- Workload testing status
- Dynamic runtime config values
- Scale-test summaries for primary, secondary, and TemporalAggregate workloads
- Data module testing for TemporalAggregate, Feature, IPS, and STRING

Expected optional fields in `health.json`:

- `cluster`: name, status, environment, metaserver count, data-node count
- `health`: metaserver, proxy, exporter, data nodes, shared storage, blockcache
- `runtime_config`: zone/blob sizing, replay knobs, blockcache capacities
- `nodes`: node-level status, role, endpoint, CPU, memory, storage, replay state
- `replication`: replay mode, source, secondary lag, visibility
- `scale_tests`: workload, QPS, p50, p99, secondary lag
- `module_tests`: module coverage for direct/proxy paths and latency
- `context_ops`: status, KPIs, backend parity, pipeline stages, test cards, request builder entries,
  query workbench, config groups, tree nodes, context-pack events/chunks/filters,
  open-source model registry, operator rows, safeguards, alerts, audit rows, and runbook steps

Recommended local install shape:

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

Recommended first-run commands once packaged:

```bash
matrixark-server init --home ~/.matrixark
matrixark-server doctor
matrixark-server doctor --backend cpp
matrixark-server doctor --backend rust
matrixark-server start --backend cpp --local
matrixark-server start --backend rust --local
matrixark-server apply-key --agent codex
```

Backend parity expectations:

- The same monitoring UI must work for `backend=cpp` and `backend=rust`.
- Health payloads should expose `temporalstore.backend`, `mode`, `storage`, `raft`, and `gateway`.
- C++ and Rust runs should render the same ContextNode topology, summaries, embeddings, events, entities, resource chunks, skills, ContextPacks, and audit rows.
- Rust CLI-per-operation is acceptable for debug parity only; production Rust should use the Rust proxy or Rust direct SDK.
- C++ and Rust benchmark runs should emit the same artifact set so LOCOMO, LongMemEval, resource, skill, and scale reports can be compared side by side.

Today, the same pieces are available through the MCP server, Docker OSS scale
script, this monitoring UI folder, and the Prometheus compose files.

Local context UI smoke test:

```bash
python3 -m http.server 8080 -d tools/temporalstore-monitoring-ui
```

Then open `http://127.0.0.1:8080/`. The same page also works as a static
`file://` page; in that mode it renders the built-in offline context sample.

The AI agent context envelope is available at
`http://127.0.0.1:8080/agent-context-envelope.html`.

Generate `health.json` from a result directory:

```bash
python3 tools/temporalstore-monitoring-ui/render_health_from_results.py \
  --result-dir /tmp/temporalstore-shared-file-3node-scale-YYYYMMDD_HHMMSS \
  --template tools/temporalstore-monitoring-ui/health.json \
  --secondary-lag-ms 12 \
  --temporalaggregate-qps 150000 \
  --temporalaggregate-p99-ms 4.8 \
  --release-build ok \
  --output tools/temporalstore-monitoring-ui/health.json
```
