# MatrixArk LOCOMO C++ vs Rust TemporalStore Parity Metrics - 2026-06-23

## Result

The same LOCOMO-style MatrixArk extraction -> ingestion -> session commit -> async summary refresh -> layer-by-layer retrieval flow now runs through both TemporalStore backends:

- C++ direct SDK backend: `temporalstore-direct`
- Rust direct SDK backend: `temporalstore-rust`

The completed parity flow shows logical feature parity. Both backends produced identical model counts, identical per-query token counts, identical selected-ref counts, and no flat-recall fallback.

## Completed LOCOMO-Style Parity Flow

| Metric | C++ TemporalStore | Rust TemporalStore |
| --- | ---: | ---: |
| elapsed seconds | 2.44 | 2.16 |
| total records | 192 | 192 |
| ContextNode | 6 | 6 |
| ContextChildRef | 3 | 3 |
| ContextEvent | 12 | 12 |
| SessionBufferEvent | 12 | 12 |
| ContextBatchCommit | 3 | 3 |
| ContextSegment | 6 | 6 |
| ContextEntity | 9 | 9 |
| ContextIndex | 24 | 24 |
| ContextSummary | 27 | 27 |
| ContextEmbedding | 54 | 54 |
| ContextPackAudit | 9 | 9 |
| retrieval queries | 9 | 9 |
| selected refs total | 45 | 45 |
| used context tokens total | 441 | 441 |
| tree traversal fallback | none | none |

Per-query token counts were identical:

```json
[51, 50, 70, 59, 59, 44, 35, 38, 35]
```

Per-query selected node counts were identical:

```json
[2, 2, 2, 2, 2, 2, 2, 2, 2]
```

## Data Models Validated

Both backends wrote and read the same data-model families:

- `context_node`: materialized filesystem-like user/session nodes.
- `context_child_ref`: parent -> child edges for traversal.
- `context_event`: online event records from individual messages.
- `session_buffer_event`: pending online events for later batch extraction.
- `context_batch_commit`: forced session-window commit records.
- `context_segment`: batch-extracted topic segments.
- `context_entity`: evolving entity state.
- `context_index`: secondary keyword/index records.
- `context_summary`: L0/L1/node/session summaries.
- `context_embedding`: event/entity/summary embeddings stored in TemporalStore.
- `context_pack_audit`: retrieval replay/audit records.

## Commands Used

C++ direct SDK:

```bash
export PYTHONPATH=.
export TEMPORALSTORE_LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
export MATRIXARK_EMBEDDING_PROVIDER=hash
export MATRIXARK_UNDERSTANDING_PROVIDER=rules
python3 tools/run_matrixark_locomo_debug_flow.py \
  --backend temporalstore-direct \
  --storage-prefix matrixark:locomo:parity:cpp:20260623b \
  --artifact-dir docs/benchmarks/locomo_cpp_rust_parity_20260623/cpp
```

Rust direct SDK:

```bash
export PYTHONPATH=.
export TEMPORALSTORE_LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
export LD_LIBRARY_PATH=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib:$LD_LIBRARY_PATH
export MATRIXARK_TEMPORALSTORE_RUST_CLI=/root/src/github-services/TemporalStore/sdk/rust/temporalstore/target/release/matrixark_record_log
export MATRIXARK_EMBEDDING_PROVIDER=hash
export MATRIXARK_UNDERSTANDING_PROVIDER=rules
python3 tools/run_matrixark_locomo_debug_flow.py \
  --backend temporalstore-rust \
  --storage-prefix matrixark:locomo:parity:rust:20260623c \
  --artifact-dir docs/benchmarks/locomo_cpp_rust_parity_20260623/rust
```

## Official LOCOMO Dataset Status

The official local LOCOMO file is present and validates:

- path: `/root/matrixark_benchmarks/data/locomo10.json`
- conversations: `10`
- sessions: `272`
- turns: `5882`
- questions: `1986`

The full dataset benchmark runner was updated to accept both `temporalstore-direct` and `temporalstore-rust`. However, larger official LOCOMO batch runs are not clean yet on this local service. The first attempt with all conversations and 100 questions timed out during C++ ingestion after about `178.1s` with a 60s request timeout. A narrowed 2-conversation / 100-question official-file run still timed out during batch extraction:

| Attempt | Backend | Scope | Timeout | Elapsed before failure | Failure |
| --- | --- | --- | ---: | ---: | --- |
| official q100 | C++ | all conversations ingested | 60s | 178.1s | C++ server request timeout during `hset` |
| official conv2 q100 | C++ | first 2 conversations | 120s | 270.99s | C++ server request timeout during `hset` |
| official conv2 q100 | Rust | first 2 conversations | 120s | 140.87s | Rust CLI returned the same server-side `hset` timeout |

Follow-up fix: `docs/matrixark_locomo_hset_gap_fix_20260623.md` adds bounded direct-record bundles and reruns the official LOCOMO 2-conversation / 100-question slice successfully on both C++ and Rust. Full all-conversation LOCOMO still needs native batch writes and benchmark load shaping before full official-score parity is claimable on this single-node local deployment.

## What Was Fixed

- `tools/run_matrixark_locomo_debug_flow.py` now supports `--backend local|temporalstore-direct|temporalstore-rust`.
- `tools/run_matrixark_dataset_benchmark.py` now supports `--backend temporalstore-rust` and `--rust-cli`.
- `MatrixArkTemporalStoreRustAdapter` now initializes the shared record-lock field used by the direct record-log path.
- Rust runs require `LD_LIBRARY_PATH` to include the C++ SDK lib directory so `matrixark_record_log` can load `libbcache2.so`.

## Next Gap

For full official LOCOMO C++/Rust benchmark parity, the next fix is not extraction schema parity. It is the write path under larger batch extraction:

1. Push `matrixark_batch_extract` record writes into fewer native C++/Rust batch operations.
2. Avoid one `hset` RPC per logical MatrixArk record during large session extraction.
3. Add checkpointed official LOCOMO runs after each ingestion chunk.
4. Rerun full official LOCOMO on both backends and compare `context_recall`, `final_judge_score`, token density, p50/p95 retrieval latency, and failure buckets.

## Artifacts

- C++ LOCOMO-style JSON: `docs/benchmarks/locomo_cpp_rust_parity_20260623/cpp/matrixark_locomo_debug_data_flow.json`
- C++ LOCOMO-style Markdown: `docs/benchmarks/locomo_cpp_rust_parity_20260623/cpp/matrixark_locomo_debug_data_flow.md`
- Rust LOCOMO-style JSON: `docs/benchmarks/locomo_cpp_rust_parity_20260623/rust/matrixark_locomo_debug_data_flow.json`
- Rust LOCOMO-style Markdown: `docs/benchmarks/locomo_cpp_rust_parity_20260623/rust/matrixark_locomo_debug_data_flow.md`
- Comparison summary JSON: `docs/benchmarks/locomo_cpp_rust_parity_20260623/comparison_summary.json`
