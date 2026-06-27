# MatrixArk Native Hot Path Fix Summary

Date: 2026-06-27

## What changed

- C++ direct SDK now uses native `matrixark_retrieve_context_pack` semantics for candidate scan, scope filtering, secondary-index filtering, scoring, and ContextPack assembly.
- C++ native retrieval now understands canonical `scope_key` values (`t=...|u=...|s=...|`) the same way as Python/Rust: tenant is enforced, while user/session are enforced only when explicitly supplied.
- C++ native retrieval now scans `context_index` records alongside serving records so secondary-index prefiltering works before candidate scoring.
- C++ native pack output now includes selected ref counts, remote refs, tree traversal telemetry, secondary-index telemetry, and native scan stats.
- MCP Python remains the glue layer for request shaping, auth/scope resolution, model glue, and audit envelope writing; hot retrieval work stays in C++/Rust.

## Validation

### Required pipeline parity

Artifact: `docs/benchmarks/latest_cpp_rust_20260627_1502/required_pipeline_native_contract.md`

Result: C++ and Rust both pass the required MatrixArk pipeline:

```text
ingest -> extraction -> async summary refresh -> tree traversal using node_l0/node_l1
-> secondary-index prefilter -> retrieve events/entities/resources/skills
-> ContextPack -> audit/replay
```

### Focused LOCOMO C++ native pack slice

Artifact directory: `docs/benchmarks/latest_cpp_rust_20260627_1502/locomo_cpp_native_scopekey_fix/`

Result after C++ scope/index fix:

- context_recall: `100%`
- final_judge_score: `70%` deterministic judge slice
- avg_prompt_tokens: `5259.6`
- p95 retrieval latency: `116.251 ms`
- ingestion throughput: `730.306 turns/sec`

### 1K C++ vs Rust scale comparison

Artifact: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_native_contract/comparison.md`

Result: `passed`, zero errors and zero timeouts.

- C++ message QPS: `689.342`
- Rust message QPS: `571.662`
- C++ retrieve p95: `119.785 ms`
- Rust retrieve p95: `658.344 ms`

## Remaining difference

The required pipeline passes for both backends, but the comparison report still marks C++ vs Rust as `warning` because selected ref counts and record-type telemetry are not byte-identical. This is now a ranking/telemetry parity issue, not a missing C++ native hot-path issue.
