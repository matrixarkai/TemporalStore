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

Raft/storage parity evidence status:

```text
python3 tools/validate_raft_storage_parity_evidence.py
raft_storage_parity_areas: 8
corpus_required_cpp_paths: 45
rust_evidence_snippets: 45
```

The eight checked areas are storage object/page/slot lifecycle, slot dump/load recovery,
compaction/GC delayed destroy, shared-store sync/async replication and cursor-safe GC, Raft
command/log/WAL codec, Raft snapshot/membership scale, Raft failover secondary replication, and
local scale/fault readiness gates.

Control-plane parity evidence status:

```text
python3 tools/validate_control_plane_parity_evidence.py
control_plane_parity_areas: 4
corpus_required_cpp_paths: 38
rust_evidence_snippets: 32
```

The four checked areas are client meta-sync/route retry, proxy serving/admission/topology,
metaserver scheduler repair/snapshot, and data-node lifecycle/server surfaces.

API/model parity evidence status:

```text
python3 tools/validate_api_model_parity_evidence.py
api_model_parity_areas: 4
required_command_kinds: 35
required_response_kinds: 16
rust_evidence_snippets: 35
```

The four checked areas are common/Redis string-hash-set behavior, Feature/Sequence timestamped
pages including policy/filter/aggregate/batch behavior, IPS/Risk models, and Context/SDK
wire-model behavior.

Ingestion/ops parity evidence status:

```text
python3 tools/validate_ingestion_ops_parity_evidence.py
ingestion_ops_parity_areas: 4
rust_evidence_snippets: 48
```

The four checked areas are Kafka/Flink ingestion durability, ingestion Prometheus/readiness
signals, production readiness workflow evidence, and scale/fault/chaos validation gates.

Shared C++/Rust corpus:

```text
compat/unified_temporalstore_cases.json
cases: 40
steps: 100
executable behavior cases: 18
executable behavior steps: 78
C++ existing-test parity surface cases: 22
C++ existing-test parity surface steps: 22
C++ existing-test required paths: 83
```

## What Is Unified With C++ Now

| Area | Count | Location | Notes |
| --- | ---: | --- | --- |
| Shared corpus runner tests | 2 | `crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs` | Runs the same shared corpus through direct engine and HTTP client paths. |
| C++-like API/integration tests | 14 | `crates/temporalstore-rust/tests/temporalstore_compat.rs` | Rust-local tests named against C++ behavior. These should be moved into the shared corpus when command/response shape is stable. |
| C++ migration corpus test | 1 | `crates/temporalstore-rust/tests/storage_migration_corpus.rs` | Rust consumes converted C++ storage artifacts and validates storage lifecycle paths. |

The shared corpus currently covers common/string/hash/set, Redis-compatible set, Feature,
Sequence, advanced Feature policy/filter/aggregate flows, Sequence batch/filter groups, IPS, Risk,
Context, restart reads, missing-key semantics, timestamp bounds, current C++ storage/Raft surface
gates, and C++ client/proxy/metaserver/data-node control-plane surface gates. Ingestion/ops parity
is currently enforced as Rust evidence plus local harness gates; the next same-test step is to
promote Kafka offset, Flink checkpoint, dead-letter, and lag scenarios into executable corpus cases
for both repos.

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
   The Feature policy/filter/aggregate cases and Sequence batch/filter cases have now moved into
   the shared corpus. Remaining candidates are Redis RESP-specific command parsing, stream/page
   read APIs, shared-store replication, and distributed workflow tests.

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
   Redis command parity, context event/index/audit workflows, tonic SDK adapters, and C++ wire-model
   round trips.

6. Add shared ingestion/ops cases:
   Kafka offset idempotency, Flink checkpoint precommit/commit/abort, dead-letter reporting,
   ingestion lag metrics, readiness blockers, and scale/fault workflow log assertions.

## Current Limitation

The C++ side still validates the shared corpus mostly through its hook and context contract plus
`existing_test` surface gates. True same-test parity requires a native C++ executor that applies
every executable corpus command and compares every expected response.

Until that exists, the honest status is:

- Rust executes all 78 executable shared behavior steps.
- C++ validates the 40-case corpus shape, current context subset, C++ storage/Raft required
  surfaces, C++ client/proxy/metaserver/data-node control-plane required surfaces, and the Rust
  ingestion/ops evidence gate in unified validation.
- 467 Rust-specific tests remain local and should be progressively converted or mirrored into the
  shared corpus/harness contract.
