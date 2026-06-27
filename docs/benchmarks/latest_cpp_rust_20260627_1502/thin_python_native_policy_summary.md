# MatrixArk Thin Python Native Policy Summary

Date: 2026-06-27

## Production Policy

Python MCP is now treated as API, auth, request shaping, and model orchestration glue. Production and benchmark profiles require native C++/Rust serving paths for hot retrieval.

Default production behavior:

- Python hot record cache is disabled for non-local backends.
- Python ContextPack cache is disabled for non-local production profiles.
- Backend-native ContextPack assembly is required.
- Backend-native candidate prefilter is required.
- Python read_all scan, secondary-index filtering, and reference packing are debug/dev fallback paths only.

Debug overrides remain explicit:

- MATRIXARK_ALLOW_PYTHON_HOT_CACHE=1 allows Python caches for diagnostics.
- MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK=0 allows Python reference packing for dev-only fallback.
- MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER=0 allows Python candidate filtering for dev-only fallback.

## Native Responsibilities

C++/Rust should own the hot path:

- prefix scan
- secondary-index prefilter
- candidate fetch
- scoring
- token-budget pack assembly
- audit buffering and metrics

Python should receive a finished ContextPack instead of thousands of raw records.

## Validation

C++ and Rust required MatrixArk pipeline parity passed after the policy change:

- ingest
- extraction
- async summary refresh
- L0/L1 traversal
- secondary-index prefilter
- events/entities/resources/skills retrieval
- ContextPack
- audit
- replay

Artifacts:

- required_pipeline_thin_python_native.json
- required_pipeline_thin_python_native.md
- required_pipeline_thin_python_native.log
