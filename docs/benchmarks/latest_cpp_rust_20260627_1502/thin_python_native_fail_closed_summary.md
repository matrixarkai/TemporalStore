# MatrixArk Thin Python Native Hot Path Proof

Run ID: `thin_python_native_fail_closed_20260627`

## What changed

- MatrixArk retrieval now dispatches to backend-native `matrixark_retrieve_context_pack` before computing Python query embeddings.
- Production/benchmark native pack failures fail closed instead of silently falling back to Python reference packing.
- Production/benchmark native candidate prefilter failures fail closed instead of falling back to Python `read_all` scan/prefilter.
- Metrics gauge refresh skips Python `read_all` in thin native profiles.
- Retrieval timeout fallback avoids full backend scans when native ContextPack assembly is required.

## Production contract

Python remains MCP/API/auth/model orchestration and request shaping. C++/Rust TemporalStore owns the hot serving path:

```text
query -> request shaping -> native retrieve_context_pack
      -> prefix scan -> secondary-index prefilter -> score/rank -> pack
      -> finished ContextPack returned to Python
```

Python must not receive thousands of raw records in production/benchmark serving. Debug/local tests can still use the reference packer by explicitly disabling native requirements.

## Validation

- `python3 -m py_compile tools/matrixark_mcp_server.py tools/matrixark_mcp_local_adapter.py tools/matrixark_mcp_temporal_adapters.py tools/test_matrixark_mcp_backend_policy.py`
- `PYTHONPATH=tools python3 -m unittest tools.test_matrixark_mcp_backend_policy -v`
- `python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_required_pipeline_parity.py --backends cpp rust --run-id thin_python_native_fail_closed_20260627`

Result: C++ and Rust both passed the required pipeline parity gate.
