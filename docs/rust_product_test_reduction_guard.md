# Rust Product Test Reduction Guard

## Current Split

The current guard output is:

```text
rust_attributed_tests=555
grandfathered_tests=540
shared_corpus_marked_tests=27
rust_internal_marked_tests=0
```

The grandfathered baseline is classified in:

```text
tools/rust_product_test_migration_ledger.json
```

Those 540 grandfathered tests are split into explicit dispositions:

| Group | Count | Disposition |
| --- | ---: | --- |
| Product behavior to move into shared corpus | 533 | Must progressively move to `compat/unified_temporalstore_cases.json` or a sibling shared corpus consumed by both Rust and C++. |
| Rust-only internals that can remain local | 7 | May stay Rust-local because they protect parser/helper/test-runner mechanics rather than TemporalStore product behavior. |
| C++ out of scope | 0 | No Rust grandfathered tests are currently classified as C++-only transport/build/internal behavior. |
| Duplicate/remove | 0 | No explicit duplicate removal targets are identified in the first ledger pass; future migrations should fold redundant tests into shared cases when found. |

The migration ledger classifies every grandfathered test by family and disposition. Tests with
`shared-corpus:` markers still count in the 540 grandfathered baseline until they are removed,
collapsed into the shared runner, or deleted from `tools/rust_product_test_baseline.json`.

## Product Behavior Backlog

| Bucket | Count | Main files | Shared-corpus target |
| --- | ---: | --- | --- |
| Storage/cache/local durability | 176 | `engine.rs`, `page_store.rs`, `cache.rs`, `shared_store.rs`, `oplog.rs`, `index_log.rs`, storage crash and migration tests | Dump/load, recovery, corruption, follower-safe GC, cache pressure/refill, shared-store replay, storage migration. |
| Raft/distributed behavior | 140 | `raft.rs`, `bin/raft_node.rs` | ByteRaft-derived read safety, metrics/admin, snapshot lifecycle, backpressure, election controls, fault harnesses, membership, failover, secondary reads. |
| Control plane/service behavior | 91 | `client.rs`, `proxy.rs`, `data_node.rs`, `meta.rs`, `rebalance.rs`, `bin/server.rs`, `bin/metaserver.rs`, `e2e.rs` | Topology changes, stale routes, admission, route quarantine, load/reload/unload, scheduler tokens, service convergence. |
| Redis/admin/API behavior | 45 | `engine.rs`, `redis.rs`, `sdk.rs`, server/admin alias tests | Redis/API command-response corpus, admin aliases, common string/hash/set lifecycle, SDK contracts. |
| Ops/scale/fault behavior | 22 | `readiness.rs`, `bin/readiness_gate.rs`, `bin/external_chaos_gate.rs`, `replica_replay.rs` | Readiness blockers, chaos/fault evidence, rolling restart, replay safety, scale/SLO reports. |
| Feature model behavior | 18 | `engine.rs`, `temporalstore_compat.rs` | Packed timestamped pages, nested point/proto semantics, policy/filter/aggregate lifecycle. |
| Ingestion behavior | 10 | `ingestion.rs`, server ingestion routes | Kafka offsets, rebalance/backpressure, Flink checkpoints, dead letters, lag metrics, restart idempotence. |
| Risk model behavior | 10 | `engine.rs`, `temporalstore_compat.rs` | CPC/list/manager/debug/window semantics. |
| Sequence model behavior | 8 | `engine.rs`, `temporalstore_compat.rs` | Ordering, bounds, batch/filter groups, C++ feature-row shape. |
| Context model and pipeline behavior | 7 | `context_workflow.rs`, Context model tests | Event/segment/entity/index/embedding/summary/compression, query debug flow, prompt-pack ordering. |
| IPS model behavior | 6 | `engine.rs`, `temporalstore_compat.rs` | Snapshot/stat/filter metadata, batch-last grouping, action/table/request metadata. |

The next migration target is **Raft ByteRaft-derived process/fault/readiness cases**, followed by
storage/cache recovery cases and Context pipeline model cases.

## Rust-Only Internals

| Bucket | Count | Main files | Why local is acceptable |
| --- | ---: | --- | --- |
| Corpus runner/shape tests | 2 | `tests/unified_temporalstore_corpus.rs` | Validates the Rust-owned corpus loader and duplicate checks. |
| Helper/parser mechanics | 5 | `partition_id.rs`, `http.rs`, `types.rs` | Protects Rust helper parsing and type formatting, not a cross-language product workflow. |

## Guard Rule

The current 540 tests are grandfathered in:

```text
tools/rust_product_test_baseline.json
```

Any new Rust attributed test must add exactly one marker immediately above the test:

```rust
// shared-corpus: case_name
#[test]
fn product_behavior_test() {}
```

or:

```rust
// rust-internal: validates local helper parsing only
#[test]
fn implementation_helper_test() {}
```

`shared-corpus:` case names are checked against the canonical shared corpus. This makes new product
behavior fail validation unless it is tied to a shared Rust/C++ test case.

Run the guard directly:

```bash
python3 tools/validate_rust_product_test_guard.py
```

It is also run by:

```bash
python3 tools/validate_no_duplicate_tests.py
```

## Per-Family Migration Checks

Use the per-family runner when moving a test family from grandfathered
Rust-local coverage into shared Rust/C++ coverage:

```bash
python3 tools/run_per_family_migration_tests.py --family all
python3 tools/run_per_family_migration_tests.py --family "control plane" --run-rust
```

Each family declares:

- shared corpus case IDs
- representative Rust tests that must carry `shared-corpus: <case_id>`
- the focused Rust test command for that family
- C++ adapter suites or temporary static-surface gates
- optional Rust/C++ report comparison through
  `tools/compare_unified_cpp_rust_case_reports.py`

When native C++ execution is not available, the shared corpus keeps that family
as a temporary static surface gate with an explicit blocker and expected runner
command.

Current guard output includes both the grandfathered baseline and migration marker count:

```text
rust_attributed_tests=555
grandfathered_tests=540
shared_corpus_marked_tests=27
rust_internal_marked_tests=0
```
