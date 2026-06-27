# MatrixArk Native Scan And Pack Symmetry Summary

Date: 2026-06-27

## What Changed

MatrixArk now has native hot-path coverage on both Rust and C++ for retrieval stages that were previously too Python-heavy.

- Rust proxy native path now performs prefix scan, secondary-index prefilter, scoring, selected-ref accounting, and ContextPack assembly in matrixark_record_log --serve.
- C++ direct SDK now exposes a standalone native candidate-prefilter API named temporalstore_matrixark_scan_candidates, matching the Rust proxy scan and prefilter boundary.
- C++ direct SDK already exposes the full native ContextPack API named temporalstore_matrixark_retrieve_context_pack.
- Python MCP remains the protocol, auth, request-shaping, and model glue layer. It no longer needs to materialize thousands of raw records for the native retrieval path.

## Native Retrieval Contract

The native retrieval path now reports native_prefix_scan, native_secondary_index_prefilter, native_pack_assembly, pack_assembly_location, selected_ref_counts, tree_traversal, secondary_index_filters, selected refs, dropped refs, token counts, and audit metadata.

## C++ API Added

The new C ABI is temporalstore_matrixark_scan_candidates(client, count_key, record_hash_key, shard_size, request_json, candidates_json, error_message).

This API scans MatrixArk record bundles natively, applies scope and secondary-index filters, and returns candidate records plus scan telemetry. It is useful for scan-only callers and parity testing. The existing C++ full pack API remains the production hot path when the caller wants a ready ContextPack.

## Validation

Required MatrixArk pipeline parity was rerun against both C++ and Rust:

- ingest
- extraction
- async summaries
- L0/L1 traversal
- secondary-index prefilter
- events/entities/resources/skills retrieval
- ContextPack
- audit
- replay

Result: both C++ and Rust passed.

Artifacts:

- required_pipeline_native_scan_pack_symmetry.json
- required_pipeline_native_scan_pack_symmetry.md
- required_pipeline_native_scan_pack_symmetry.log

## Remaining Work

The remaining performance work is not schema correctness. It is deeper native optimization and operational proof:

- run larger C++/Rust scale sweeps with the same live topology and model config;
- push more routing and batch append below hash-write compatibility paths;
- keep Rust proxy and future Rust direct SDK behavior aligned with the C++ direct SDK contract.
