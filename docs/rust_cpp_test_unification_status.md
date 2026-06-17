# Rust/C++ Test Unification Status

## Snapshot

Date: 2026-06-16

Rust attributed tests counted with `#[test]` and `#[tokio::test]` under
`crates/temporalstore-rust`: **484**.

Already tied directly to shared/C++ parity harnesses: **17 Rust test functions**.

Still Rust-specific: **467 Rust test functions**.

Duplicate test status:

```text
python3 tools/validate_no_duplicate_tests.py
rust_attributed_tests: no duplicate function names
shared corpus: no duplicate case names, per-case step names, or per-case command payloads
C++ existing-test surfaces: no repeated required_paths
```

Shared C++/Rust corpus:

```text
compat/unified_temporalstore_cases.json
cases: 34
steps: 80
executable behavior cases: 16
executable behavior steps: 62
C++ existing-test parity surface cases: 18
C++ existing-test parity surface steps: 18
```

## What Is Unified With C++ Now

| Area | Count | Location | Notes |
| --- | ---: | --- | --- |
| Shared corpus runner tests | 2 | `crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs` | Runs the same shared corpus through direct engine and HTTP client paths. |
| C++-like API/integration tests | 14 | `crates/temporalstore-rust/tests/temporalstore_compat.rs` | Rust-local tests named against C++ behavior. These should be moved into the shared corpus when command/response shape is stable. |
| C++ migration corpus test | 1 | `crates/temporalstore-rust/tests/storage_migration_corpus.rs` | Rust consumes converted C++ storage artifacts and validates storage lifecycle paths. |

The shared corpus currently covers common/string/hash/set, Redis-compatible set, Feature,
Sequence, IPS, Risk, Context, restart reads, missing-key semantics, timestamp bounds, and current
C++ storage/Raft surface gates.

## Rust-Specific Tests Remaining

| Bucket | Rust-specific tests | Main files |
| --- | ---: | --- |
| Storage/cache/local durability | 157 | `engine.rs`, `page_store.rs`, `cache.rs`, `shared_store.rs`, `oplog.rs`, `index_log.rs` |
| Control plane/service behavior | 157 | `client.rs`, `proxy.rs`, `data_node.rs`, `meta.rs`, `rebalance.rs`, `bin/server.rs`, `bin/metaserver.rs` |
| Raft/local consensus model | 110 | `raft.rs`, `bin/raft_node.rs` |
| API/model/ingestion/context/SDK | 17 | `redis.rs`, `context_workflow.rs`, `ingestion.rs`, `sdk.rs`, `types.rs` |
| Rust storage crash harness | 2 | `tests/storage_crash_harness.rs` |
| Other local tests | 24 | readiness, e2e, partition id, external chaos, HTTP, replica replay |

Total remaining Rust-specific tests: **467**.

## Highest-Value Tests To Share Next

1. Promote `temporalstore_compat.rs` cases into `compat/unified_temporalstore_cases.json`.
   These are already C++-named and mostly command/response oriented, so they are the cheapest to
   convert into shared cases.

2. Add shared storage lifecycle corpus cases for the large storage bucket:
   packed page recovery, slot dump/load, compaction, GC retention, tiny-cache refill, shared-store
   sync/async replay, and corrupt-page/missing-ref negative cases.

3. Add shared Raft harness cases for the large Raft bucket:
   command/log codec, follower catch-up, snapshot install, leader transfer, read-index, membership
   changes, stale-read rejection, and WAL recovery. C++ can initially validate these as
   `existing_test` harness surfaces, then move to executable corpus replay when a native C++
   runner exists.

4. Add shared control-plane cases:
   client retry budgets, proxy route invalidation/quarantine, metaserver scheduler task replay,
   data-node lifecycle transitions, and service readiness reports.

5. Add shared API/model cases:
   Redis command parity, context event/index/audit workflows, ingestion offset/checkpoint
   lifecycle, tonic SDK adapters, and C++ wire-model round trips.

## Current Limitation

The C++ side still validates the shared corpus mostly through its hook and context contract plus
`existing_test` surface gates. True same-test parity requires a native C++ executor that applies
every executable corpus command and compares every expected response.

Until that exists, the honest status is:

- Rust executes all 62 executable shared behavior steps.
- C++ validates the 34-case corpus shape, current context subset, and C++ storage/Raft required
  surfaces.
- 467 Rust-specific tests remain local and should be progressively converted or mirrored into the
  shared corpus/harness contract.
