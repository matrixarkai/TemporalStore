# MatrixArk MCP Production Defaults

These assumptions define the v1 MatrixArk MCP and orchestrator boundary.

## Defaults

- Python remains the MCP/HTTP/control-plane layer for now. Native C++ or Rust MCP servers are future optimizations, not a v1 requirement.
- C++ and Rust TemporalStore remain the serving engines. They own hot-path
  append, scan, index prefilter, retrieve, pack, storage, and topology behavior.
- Default production retrieval is compact and audit-light.
- Full replay/debug audit is opt-in policy, not the default hot path.
- Cloud mode requires an API key or trusted SSO gateway identity before scoped
  data leaves the server.
- Local/dev mode may use generated local scope defaults such as `acct_local`,
  an agent-derived tenant, the local OS user, and an optional session id.

## Agent Envelope

Codex, Claude, Cursor, OpenClaw, OpenCode, Aider, Continue, Cline/Roo, and
generic agents are clients of one MatrixArk envelope. The generated source of
truth is `tools/matrixark_agent_config.py`.

The envelope carries:

- visible local context only;
- query text;
- scope hints;
- local context token estimate;
- max context token budget;
- lifecycle event type;
- optional file/resource references.

Agents do not need to understand ContextEvent, ContextEntity, ContextSummary,
ContextEmbedding, ContextIndex, ResourceChunk, or SkillSection internals.

Lifecycle policy:

- before LLM: `matrixark_retrieve`;
- after answer/tool: `matrixark_ingest` for durable outcomes;
- resource added: import resource or skill with `matrixark_ingest`;
- feedback: `matrixark_feedback` with accepted/rejected refs;
- session boundary: `matrixark_session_commit` for commit/batch extraction.

## Observability And Portal

Metrics include backend identity and storage mode. Prometheus-compatible output
must expose:

- ingest/retrieve QPS;
- p50/p95/p99 latency;
- timeout count;
- partial ContextPack count;
- queue depth;
- audit write failures;
- dirty summary lag;
- resource import lag;
- model fallback flags;
- backend readiness.

The management portal shows scoped, redacted, paged tables for:

- messages;
- resources;
- skills;
- events/entities;
- ContextPacks;
- users;
- API keys;
- audit logs.

Portal responses enforce scope before data leaves the server and redact raw API
keys and sensitive identity metadata by default.

## Open-Source Readiness

Open-source readiness means:

- no private checkout paths;
- no local credentials or secrets;
- no vendored build outputs or generated dependency caches;
- clear license, notice, security, contribution, and code-of-conduct files;
- reproducible local validation commands.

Use:

```bash
python3 tools/validate_open_source_readiness.py
PYTHONPATH=tools:. python3 -m unittest \
  tools.test_matrixark_access_governance \
  tools.test_matrixark_python_module_boundaries \
  tools.test_matrixark_popular_agent_hooks \
  tools.test_matrixark_mcp_backend_policy
```

Production gates include `validate_open_source_readiness.py`, module-boundary
tests, access-governance tests, popular-agent hook tests, backend-policy tests,
and the C++/Rust scale matrix gate.

The production posture is intentionally conservative: Python is the protocol and
orchestration control plane; C++/Rust own serving-critical work; debug/replay is
policy-enabled rather than automatically written on every hot retrieval.
