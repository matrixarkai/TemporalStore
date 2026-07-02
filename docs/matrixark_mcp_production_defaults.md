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

The production posture is intentionally conservative: Python is the protocol and
orchestration control plane; C++/Rust own serving-critical work; debug/replay is
policy-enabled rather than automatically written on every hot retrieval.
