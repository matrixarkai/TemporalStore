# Rust Product Test Reduction Guard

## Current Split

The current Rust attributed-test count is still:

```text
rust_attributed_tests=540
```

Those tests are now split into two explicit groups:

| Group | Count | Disposition |
| --- | ---: | --- |
| Product behavior to move into shared corpus | 533 | Must progressively move to `compat/unified_temporalstore_cases.json` or a sibling shared corpus consumed by both Rust and C++. |
| Rust-only internals that can remain local | 7 | May stay Rust-local because they protect parser/helper/test-runner mechanics rather than TemporalStore product behavior. |

The first migration slice now marks **11** existing `temporalstore_compat.rs` product tests with
explicit `shared-corpus:` references. These tests still count in the 540 Rust-attributed baseline
until they are removed or collapsed into the shared runner, but their product contract is no longer
ambiguous. The remaining unmarked compatibility tests are:

- `production_readiness_service_summary_is_public_api`
- `cxx_stream_random_size_reopen_and_scan_matches_stream_test`
- `cxx_stream_cross_block_large_values_survive_reopen`

Those need dedicated readiness/stream corpus cases before they can be marked or removed.

## Product Behavior Backlog

| Bucket | Count | Main files | Shared-corpus target |
| --- | ---: | --- | --- |
| Storage/cache/local durability | 163 | `engine.rs`, `page_store.rs`, `cache.rs`, `shared_store.rs`, `oplog.rs`, `index_log.rs`, storage crash and migration tests | Recovery, dump/load, cache refill, corruption, shared-store replay, storage migration. |
| Control plane/service behavior | 174 | `client.rs`, `proxy.rs`, `data_node.rs`, `meta.rs`, `rebalance.rs`, `bin/server.rs`, `bin/metaserver.rs`, `e2e.rs` | Topology, stale routes, admission, load/reload/unload, scheduler, service convergence. |
| Raft/distributed behavior | 140 | `raft.rs`, `bin/raft_node.rs` | Log, snapshot, membership, failover, catch-up, secondary reads, OpenRaft rollout evidence. |
| API/model/ingestion/context/SDK behavior | 36 | `temporalstore_compat.rs`, `redis.rs`, `context_workflow.rs`, `ingestion.rs`, `sdk.rs` | Redis/API, Feature/Sequence/IPS/Risk/Context, ingestion offsets/checkpoints/dead letters, SDK contracts. |
| Readiness/ops/fault behavior | 20 | `readiness.rs`, `bin/readiness_gate.rs`, `bin/external_chaos_gate.rs`, `replica_replay.rs` | Readiness blockers, chaos/fault evidence, replay safety, scale/fault reports. |

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

Current guard output includes both the grandfathered baseline and migration marker count:

```text
rust_attributed_tests=540
grandfathered_tests=540
shared_corpus_marked_tests=11
rust_internal_marked_tests=0
```
