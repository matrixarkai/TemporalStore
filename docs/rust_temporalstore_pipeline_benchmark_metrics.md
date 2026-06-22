# Rust TemporalStore Pipeline And Benchmark Metrics

## Answer

Yes. Accepted context ingestion, extraction-style context event storage, retrieval, and benchmark
evidence now requires the Rust TemporalStore path. Python is still used as orchestration, dataset
conversion, reader scoring, and report emission glue, but accepted reports must prove that Rust
TemporalStore was used for the backend pipeline.

Current evidence gates require:

- `all_pipelines_use_rust_temporalstore=true`
- `rust_temporalstore_backend_ready=true`
- `rust_temporalstore_context_event_ingest_ready=true`
- `rust_temporalstore_direct_source_scoring=false`
- positive `rust_temporalstore_ingested_source_sets`
- positive `rust_temporalstore_retrieved_source_sets`
- `python_only_diagnostic=false`

The endpoint benchmark runner also now forces `--require-rust-temporalstore`; it no longer exposes a
Python-only benchmark evidence mode.

Repeat benchmark runs now also keep the default Rust harness build cache under the repository
target tree (`target/temporalstore-context-benchmark`) instead of `/tmp`. This avoids losing the
release harness between long LOCOMO/LongMemEval_s replays on machines where `/tmp` is periodically
cleaned or reset. Callers can still override this with `CARGO_TARGET_DIR` when they intentionally
want a different cache location.

## Fresh Local Rust Pipeline Validation

Command:

```bash
cargo run -p temporalstore-rust --bin context_workflow_harness \
  > /tmp/temporalstore-context-workflow-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/temporalstore-context-workflow-validation.log
```

Result:

| Field | Value |
| --- | --- |
| `context_pipeline_ready` | `true` |
| `benchmark_ready` | `true` |
| `benchmark_sweep_ready` | `true` |
| `external_benchmark_ready` | `true` |
| `shared_store_sync_ready` | `true` |
| `shared_store_async_ready` | `true` |
| `raft_read_ready` | `true` |
| `benchmark_hit_at_k` | `1.0` |
| `benchmark_mean_reciprocal_rank` | `1.0` |
| `benchmark_token_reduction_percent` | `83.67876` |
| `benchmark_threshold_passed` | `true` |

This validates the Rust-native context workflow gate: ingestion/extraction, retrieval, injection,
management APIs, shared-store sync/async replay, Raft read path, benchmark sweep, and external
benchmark fixture.

The same gate was also run from a Ubuntu-native checkout at
`/home/vj/temporalstore-rust-native`, copied from the Windows worktree to avoid the mounted-folder
build penalty. Validation passed with:

```bash
cd /home/vj/temporalstore-rust-native
cargo check -p temporalstore-rust --bin context_workflow_harness
cargo run -p temporalstore-rust --bin context_workflow_harness \
  > /tmp/temporalstore-context-workflow-validation-native.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/temporalstore-context-workflow-validation-native.log
```

## Fresh Strict Rust Benchmark Fixture

Command shape:

```bash
TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING=1 \
python3 tools/run_longmemeval_s_full_path.py \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --threshold-profile fixture \
  --require-full-rust-temporalstore-replay \
  --rust-temporalstore-batch-size 2 \
  --rust-temporalstore-timeout-seconds 180 \
  --rust-temporalstore-score-tolerance 0
```

The hostile `TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING=1` environment value is
intentionally ignored by the runner for accepted evidence. The Rust backend run forces direct source
scoring off.

| Field | Value |
| --- | --- |
| `case_count` | `4` |
| `conversation_count` | `2` |
| `source_count` | `12` |
| `benchmark_hit_at_k` | `1.0` |
| `reader_hit_rate` | `1.0` |
| `benchmark_mean_reciprocal_rank` | `1.0` |
| `benchmark_retrieval_p50_ms` | `0.9757269999823848` |
| `benchmark_retrieval_p95_ms` | `8.604417499974204` |
| `benchmark_reader_p50_ms` | `4.728014499988831` |
| `benchmark_reader_p95_ms` | `9.917182149996506` |
| `benchmark_threshold_passed` | `true` |
| `all_pipelines_use_rust_temporalstore` | `true` |
| `rust_temporalstore_backend_ready` | `true` |
| `rust_temporalstore_context_event_ingest_ready` | `true` |
| `rust_temporalstore_direct_source_scoring` | `false` |
| `rust_temporalstore_full_replay_ready` | `true` |
| `rust_temporalstore_ingested_source_sets` | `2` |
| `rust_temporalstore_retrieved_source_sets` | `2` |

This fixture was rerun from the Ubuntu-native checkout with explicit Rust TemporalStore full replay.
The native run passed with `Hit@K=1.0`, `reader_hit_rate=1.0`,
`all_pipelines_use_rust_temporalstore=true`, `rust_temporalstore_backend_ready=true`, and
`rust_temporalstore_full_replay_ready=true`.

## Archived Full Dataset Metrics

These are deterministic-reader engineering gates, not live GPT-4o-mini paper-comparable runs.

### LOCOMO

Source: `docs/benchmark_archives/locomo_full_rust_replay_latest.json`

| Field | Value |
| --- | --- |
| Dataset | `LOCOMO` |
| Cases | `1542` |
| `benchmark_hit_at_k` | `0.953307392996109` |
| `reader_hit_rate` | `0.8839169909208819` |
| `benchmark_token_reduction_percent` | `83.98441915737831` |
| `benchmark_retrieval_p95_ms` | `46.16009804303758` |
| `benchmark_threshold_passed` | `true` |
| `all_pipelines_use_rust_temporalstore` | `true` |
| `rust_temporalstore_backend_ready` | `true` |
| `rust_temporalstore_full_replay_ready` | `true` |
| `python_only_diagnostic` | `false` |
| Batch replay | `true`, `10` batches |
| Source packing | enabled, pack size `32`, preserves all source text |
| Original source count | `1,475,584` |
| Packed source count | `46,782` |

### LongMemEval_s

Source: `docs/benchmark_archives/longmemeval_s_full_rust_replay_latest.json`

| Field | Value |
| --- | --- |
| Dataset | `LongMemEval_s` |
| Cases | `500` |
| Conversations | `500` |
| Sources | `10,960` |
| `benchmark_hit_at_k` | `1.0` |
| `reader_hit_rate` | `0.974` |
| `reader_answer_coverage` | `0.8695106649937264` |
| `answer_term_coverage` | `0.6398996235884568` |
| `benchmark_token_reduction_percent` | `81.40165238606907` |
| `benchmark_retrieval_p95_ms` | `28.978386276867248` |
| `benchmark_threshold_passed` | `true` |
| `all_pipelines_use_rust_temporalstore` | `true` |
| `rust_temporalstore_backend_ready` | `true` |
| `rust_temporalstore_full_replay_ready` | `true` |
| `python_only_diagnostic` | `false` |
| Rust/Python Hit@K delta | `0.0` |
| Rust/Python MRR delta | `0.0` |

## What Is Still Not Claimed

The full-dataset archived metrics above are deterministic-reader evidence. A VikingMem/OpenViking
paper-comparable claim still requires a live reader run with the configured GPT-4o-mini or compatible
OpenAI-style endpoint, real model calls, and archived output showing:

- `reader_mode_effective=open-source`
- `reader_open_source_calls > 0`
- full dataset thresholds passed
- full Rust TemporalStore replay ready
- no Python-only diagnostic mode

`OPENAI_API_KEY` or an OpenAI-compatible `TEMPORALSTORE_READER_BASE_URL` is intentionally not stored
in this repository. Without one of those configured, live GPT-4o-mini/OpenViking-compatible reader
runs must fail closed; deterministic-reader metrics remain engineering evidence rather than a
paper-comparable VikingMem score.
