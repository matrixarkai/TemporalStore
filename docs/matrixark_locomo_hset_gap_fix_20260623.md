# MatrixArk LOCOMO HSET Gap Fix - 2026-06-23

## What Was Fixed

The previous LOCOMO parity blocker was large `matrixark_batch_extract` workloads timing out on server-side `hset` for both C++ and Rust TemporalStore backends.

The fix reduces write pressure in two ways:

- `MatrixArkLocalAdapter` now exposes `append_many(records)`.
- `MatrixArkTemporalStoreDirectAdapter` and the Rust adapter path now persist logical record batches as bounded record bundles instead of doing one `hset` plus one `record_count` update per MatrixArk record.
- Bundle payloads are capped by `MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES`, default `65536`, so we avoid both extremes: too many tiny `hset`s and one oversized hash value.
- `matrixark_batch_extract` now gathers ContextEvent, ContextEntity, ContextSegment, ContextSummary, ContextEmbedding, ContextIndex, and audit records and flushes them through `append_many()`.

This keeps `read_all()` behavior unchanged for callers: bundled entries are expanded back into normal logical records.

## Validation

Scripts compile:

```bash
python3 -m py_compile \
  tools/matrixark_mcp_server.py \
  tools/run_matrixark_dataset_benchmark.py \
  tools/run_matrixark_locomo_debug_flow.py
```

The official LOCOMO source file validates:

```text
/root/matrixark_benchmarks/data/locomo10.json
conversations: 10
sessions: 272
turns: 5882
questions: 1986
```

## Official LOCOMO Slice: C++ vs Rust

The previously failing official-file slice now completes on both backends:

```text
dataset: LOCOMO official local file, locomo10.json
conversation_limit: 2
turns_ingested: 788
sessions: 38
questions_run: 100
batch_size: 40
max_context_tokens: 1200
embedding/understanding mode: deterministic hash/rules for backend parity
```

| Metric | C++ TemporalStore | Rust TemporalStore |
| --- | ---: | ---: |
| status | completed | completed |
| elapsed seconds | 14.02 | 15.64 |
| turns ingested | 788 | 788 |
| sessions | 38 | 38 |
| questions | 100 | 100 |
| ingestion elapsed ms | 3103 | 2748 |
| ingestion throughput turns/sec | 253.948 | 286.754 |
| context recall | 1.0000 | 1.0000 |
| final debug judge score | 0.6200 | 0.6200 |
| answer support hit | 0.6200 | 0.6200 |
| answer hit | 0.1000 | 0.1000 |
| evidence session recall | 0.4000 | 0.4000 |
| avg prompt tokens | 1199.1 | 1199.1 |
| answer-bearing token density | 0.012601 | 0.012593 |
| p50 retrieval ms | 85.443 | 83.262 |
| p95 retrieval ms | 191.453 | 206.703 |
| compression hidden answers | 0 | 0 |

## Remaining Boundary

This patch fixes the original larger-batch `hset` timeout enough for the official 2-conversation / 100-question LOCOMO slice to pass on both backends.

## Rust Timestamped Page Perf Follow-Up

The Rust engine now removes two avoidable packed timestamped-page costs that affected the same
Feature/Sequence/Context-style ingestion and retrieval paths used by LOCOMO and LongMemEval replay:

- page chunk planning no longer serializes the whole growing candidate page after every point; it
  uses incremental JSON size accounting and serializes only final page chunks;
- already-sorted timestamped batches skip the `BTreeMap` sort/dedup path and append directly;
- `FeatureQuery` and `FeatureQueryFiltered` decode each unique packed page once per scan instead of
  rereading/redecoding the same page for every timestamp mapped to that page.

`ContextQueryEvents` and `ContextCompressEvents` now use the same per-scan packed-page cache when
they encounter packed/migrated context pages. Ordinary `ContextWriteEvent` calls still append one
event page per command today; server-side context write coalescing remains the next QPS target for
LOCOMO-style ingestion.

Focused validation:

```text
cargo test -p temporalstore-rust feature_ --lib -- --test-threads=1 --nocapture
```

Result: 19 focused feature/storage tests passed, including
`feature_query_decodes_each_packed_page_once_per_scan`, which cold-invalidates the cache and verifies
one page-store read for a 64-timestamp packed page query.

Quick release scale validation:

```text
cargo run --release -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 --string-ops 20 --hash-ops 5 --sequence-keys 1 --sequence-len 30 \
  --scale-events 1 --failover-every 10 --read-sample-every 5 --compare-shared-store false
```

Captured result: replication healthy, max replica lag `0`, one failover, one scale event,
write QPS `18.41`, p95 write latency `39.8 ms`, p95 replica-read latency `5.154 ms`.

Follow-up async-oplog validation now shows the all-conversation / 100-question C++ run completes on a single local data node when storage async mode is enabled:

- `TEMPORALSTORE_STORAGE_ASYNC=true`
- `TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH=10000`
- `MATRIXARK_DIRECT_AUDIT_MODE=deferred`
- 10 conversations, 272 sessions, 5882 turns ingested
- 100 questions completed
- ingestion throughput: 710.73 turns/sec
- p95 retrieval latency: 117.87 ms

See `docs/matrixark_async_oplog_locomo_benchmark_20260623.md`.

So the original sync-storage bottleneck is now isolated: full official LOCOMO at this shape needs async storage or native batch append/load shaping on this single-node local deployment.

Follow-up load-shaping fix:

- direct writes now retry transient `hset` / `put_string` failures with exponential backoff;
- optional `MATRIXARK_DIRECT_WRITE_THROTTLE_MS` can pace sustained local benchmark writes;
- retrieval audit writes can run in `buffered`, `deferred`, `drop`, or `sync` mode through `MATRIXARK_DIRECT_AUDIT_MODE`;
- `deferred` audit mode is recommended for full-dataset benchmark ingestion/retrieval runs, with explicit flush at checkpoints/end.

## Next Engineering Step

For full official LOCOMO completion on one local node:

1. Add native batch hash write APIs or a MatrixArk-specific append-log API so the SDK can write multiple fields with one server-side operation.
2. Add runner-side ingestion checkpointing after every conversation so full runs can resume without rewriting all prior records.
3. Use `MATRIXARK_DIRECT_AUDIT_MODE=deferred` for full benchmark runs so retrieval audits are checkpointed outside the hot path.
4. Run a full matrix: sync vs async storage, buffered vs deferred audit, one data node vs multiple data nodes.
5. Rerun full LOCOMO on C++ and Rust separately with equivalent async/storage policy, then run parity comparison over the full artifact set.

## Artifact Pointers

Small report artifacts are under:

```text
docs/benchmarks/locomo_cpp_rust_parity_20260623_fixed/official_cpp_conv2/
docs/benchmarks/locomo_cpp_rust_parity_20260623_fixed/official_rust_conv2/
```

The large `context_packs.jsonl` and `result.json` files are local debugging artifacts and should not be committed unless explicitly needed.
