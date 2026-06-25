# Rust/C++ Test Unification Status

## Snapshot

Date: 2026-06-25

Rust attributed tests counted with `#[test]` and `#[tokio::test]` under
`crates/temporalstore-rust`: **585**.

Current grandfathered migration baseline: **519 Rust test functions**.

Product behavior still to move into shared corpus: **512 Rust test functions**.

Rust-only internals that can remain local: **7 Rust test functions**.

Unification target:

- All externally observable TemporalStore product behavior should move into Rust-owned shared
  corpus files first, then be consumed by both Rust and C++ runners.
- Rust-specific and C++-specific tests should remain only for language/runtime internals that are
  not a product contract: Rust helper units, serde-only details, borrow/async mechanics, C++
  object ownership, legacy C++ wire glue, CMake/linking, and other implementation-only checks.
- No new duplicate behavioral tests should be added separately in Rust and C++; add a shared corpus
  case instead, then adapt each runner.

Duplicate test status:

```text
python3 tools/validate_no_duplicate_tests.py
rust_attributed_tests: no duplicate function names
Rust product-test guard: new tests require shared-corpus or rust-internal markers
shared corpus: no duplicate case names, per-case step names, or per-case command payloads
C++ existing-test surfaces: no repeated required_paths
```

The external `TemporalStoreTestCorpus` checkout now has a refreshed full draft migration inventory:

```text
canonical shared cases: 150
canonical shared steps: 312
draft translated existing tests: 2700
draft Rust candidates: 554
draft C++ candidates: 2146
Rust marked local tests remaining: 41
Rust marked local tests removable now: 0
```

Nine Rust-local product tests have been removed because their behavior is now covered by
self-executing shared cases:

- `feature_nested_proto_aggregate_semantics`
- `redis_operational_admin_commands`
- `redis_slot_hash_cpp_crc64`
- `storage_shared_store_oplog_cursor_retention`
- `storage_shared_store_checkpoint_cursor_retention`
- `sequence_cpp_feature_rows` / `sequence_batch_filter_groups`
- `ips_options_range` / `ips_snapshot_stat_filter_batch`
- `risk_counter_window` / `risk_family_query_and_delete` / `risk_manager_debug_fol`
- `storage_shared_store_sync_replay` / `storage_shared_store_async_replay`

The duplicate-test guard derives its C++ Raft alias exemptions from
`coverage.required_raft_case_names`, so the duplicate path rules and the unified Raft schema cannot
drift apart.

The shared corpus validator also requires every runtime/stress `cpp_data_raft_parity` step to
declare a Rust runner/validator, and requires the data-node and production Raft cases to point at
the combined data-node plus metaserver parity gate.
The corpus coverage block has a dedicated `required_raft_case_names` list for the exact unified
C++/Rust Raft cases: data-Raft replication, metaserver membership/failover/snapshot plus
post-failover replacement/scale-down, data-node scale/failover/snapshot plus post-snapshot
rescale, data-node mixed read/write plus membership, and the production Raft gate.

When `TS_CPP_REPO` is set, the unified, parity, and storage/Raft gates pass it through to
`validate_raft_storage_parity_evidence.py --cpp-repo`, so the Raft parity evidence is checked
against the current C++ checkout instead of Rust evidence alone.

For a focused C++ Raft-case-driven Rust run, use:

```bash
python3 tools/run_cpp_raft_cases_on_rust.py \
  --cpp-repo /path/to/cpp/TemporalStore \
  --artifact-dir /tmp/temporalstore-cpp-raft-cases-on-rust
```

The report maps each C++ Raft case and runner to the Rust runner/validator before the Rust
distributed Raft parity gate runs.

Raft/storage parity evidence status:

```text
python3 tools/validate_raft_storage_parity_evidence.py
raft_storage_parity_areas: 12
corpus_required_cpp_paths: 50
rust_evidence_snippets: 134
```

The twelve checked areas are storage object/page/slot lifecycle, slot dump/load recovery,
compaction/GC delayed destroy, shared-store sync/async replication and cursor-safe GC, Raft
command/log/WAL codec, Raft snapshot/membership scale, data-node Raft consensus contract, Raft
metaserver distributed fault contract, Raft failover secondary replication, exact C++ Raft case
names, local scale/fault readiness gates, and RustRaft-derived readiness contracts.
`tools/run_raft_shared_cases.py` now adds a focused shared-case bridge for the Raft rows: it
validates C++ required paths and Rust process/harness runners, and its Rust combined mode runs
`tools/run_raft_distributed_parity.sh` once to produce data-node plus metaserver evidence.
The combined parity summary exposes metaserver scheduler execution coverage, OpenRaft metaserver
process rollout, and metaserver-owned data-Raft membership as first-class fields. This is still
local multi-process evidence, not AWS/external SLO proof.

Control-plane parity evidence status:

```text
python3 tools/validate_control_plane_parity_evidence.py
control_plane_parity_areas: 11
corpus_required_cpp_paths: 60
rust_evidence_snippets: 86
```

The eleven checked areas are client meta-sync/route retry, proxy serving/admission/topology,
metaserver scheduler repair/snapshot, data-node lifecycle/server surfaces, topology-version change,
stale route invalidation, proxy admission policy, readonly/write-disabled table policy, route
quarantine/recovery, data-node load/reload/unload lifecycle, and metaserver scheduler lifecycle.
Client and proxy readiness now also expose typed local-vs-production splits:
`ClientRoutingReadinessReport` covers typed client routing, route refresh, background meta sync,
retry classification, topology preflight, Neptune routing hooks, and deployment placement hooks
while keeping C++ wire migration blocked; `ProxyServingReadinessReport` covers HTTP execute routes,
heartbeat/config application, topology refresh, admission policy, Rust-native discovery, and tonic
streaming/callback shape while keeping legacy C++ wire C++ wire proxy transport out of scope.
Metaserver and data-node readiness now expose the same split. `MetaServerControlPlaneReadinessReport`
covers inventory heartbeat, namespace/table topology, C++ partition-set/member/version topology,
local Raft mutation, placement, local snapshots, scheduler admin/snapshot, scheduler retry/task
Raft persistence, and preflight while keeping networked metaserver Raft, real-process scheduler
execution, and durable data-Raft membership blocked. `DataNodeServiceReadinessReport` covers
execute runtime, async jobs, lifecycle admin, shard-affine workers, local admission, and
crash-recovery reports, tonic/gRPC streaming callbacks, distributed admission, and multi-process
load/reload/unload/restart lifecycle validation.

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
ingestion_ops_parity_areas: 10
corpus_required_cpp_paths: 12
rust_evidence_snippets: 75
```

The ten checked areas are Kafka/Flink ingestion durability, ingestion Prometheus/readiness
signals, production readiness workflow evidence, scale/fault/chaos validation gates, Kafka offset
ledger, Kafka rebalance/backpressure, Flink checkpoint lifecycle, dead-letter export, lag metrics,
and restart/failover idempotence.

Shared C++/Rust corpus:

```text
compat/unified_temporalstore_cases.json
cases: 128
steps: 227
executable behavior cases: 128
executable behavior steps: 227
C++ existing-test parity surface cases: 0
C++ existing-test parity surface steps: 0
C++ existing-test required paths: 170 unique paths
```

Detailed inventory: `docs/unified_test_case_inventory.md`.

## What Is Unified With C++ Now

| Area | Count | Location | Notes |
| --- | ---: | --- | --- |
| Shared corpus runner tests | 2 | `crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs` | Runs the same shared corpus through direct engine and HTTP client paths. |
| C++-like API/integration tests | 14 | `crates/temporalstore-rust/tests/temporalstore_compat.rs` | Rust-local tests named against C++ behavior. 11 now carry explicit `shared-corpus:` references; the three remaining unmarked tests need readiness/stream corpus cases. |
| C++ migration corpus test | 1 | `crates/temporalstore-rust/tests/storage_migration_corpus.rs` | Rust consumes converted C++ storage artifacts and validates storage lifecycle paths. |

The shared corpus currently covers common/string/hash/set, Redis-compatible set, Feature,
Sequence, advanced Feature policy/filter/aggregate flows, Sequence batch/filter groups, IPS
snapshot/filter/stat/batch metadata flows, Risk manager/debug/FOL flows,
storage dump/load, fault matrix, follower-safe GC, cache refill, sync shared-store replay, async
shared-store replay,
explicit data-node Raft leader/failover, snapshot/restart/follower-lag, membership/secondary-read
cases, explicit metaserver Raft leader/snapshot/restart and membership add/promote/remove cases,
OpenRaft process-rollout evidence,
shared control-plane cases for topology-version changes, stale route invalidation, proxy admission,
readonly/write-disabled policy, route quarantine/recovery, data-node load/reload/unload lifecycle,
and metaserver scheduler lifecycle. Those control-plane cases are Rust-executable through
`tools/run_control_plane_shared_cases.py` and still C++-static until a native C++ control-plane
runner is configured,
shared ingestion cases for Kafka offsets, rebalance/backpressure, Flink checkpoint lifecycle, dead
letters, lag metrics, and restart/failover idempotence. Those ingestion cases are Rust-executable
through `tools/run_ingestion_shared_cases.py` and still C++-static until a native C++ ingestion
runner is configured,
Context, restart reads, missing-key semantics, timestamp bounds, current C++ storage/Raft surface
gates, and C++ client/proxy/metaserver/data-node control-plane surface gates. Ingestion/ops parity
now has shared corpus case names backed by Rust evidence and current C++ queue/proxy ingestion
surfaces. The next same-test step is native C++ execution of those ingestion workflows.

## Rust-Specific Tests Remaining

These counts are a migration backlog, not the desired end state. The detailed split and new-test
guard are documented in `docs/rust_product_test_reduction_guard.md`. The current split is 512
product-behavior tests to move into shared corpus coverage and 7 Rust-only internal tests that can
remain local.

| Bucket | Rust-specific tests | Main files |
| --- | ---: | --- |
| Storage/cache/local durability | 174 | `engine.rs`, `page_store.rs`, `cache.rs`, `shared_store.rs`, `oplog.rs`, `index_log.rs`, storage crash/migration tests |
| Raft/distributed behavior | 140 | `raft.rs`, `bin/raft_node.rs` |
| Control plane/service behavior | 91 | `client.rs`, `proxy.rs`, `data_node.rs`, `meta.rs`, `rebalance.rs`, `bin/server.rs`, `bin/metaserver.rs`, `e2e.rs` |
| Redis/admin/API behavior | 36 | `engine.rs`, `redis.rs`, `sdk.rs`, server/admin alias tests |
| Ops/scale/fault behavior | 22 | `readiness.rs`, `bin/readiness_gate.rs`, `bin/external_chaos_gate.rs`, `replica_replay.rs` |
| Feature model behavior | 13 | `engine.rs`, `temporalstore_compat.rs` |
| Ingestion behavior | 10 | `ingestion.rs`, server ingestion routes |
| Risk model behavior | 9 | `engine.rs`, `temporalstore_compat.rs` |
| Context model and pipeline behavior | 7 | `context_workflow.rs`, Context model tests |
| Sequence model behavior | 5 | `engine.rs`, `temporalstore_compat.rs` |
| IPS model behavior | 5 | `engine.rs`, `temporalstore_compat.rs` |
| Rust-only internals that can remain local | 7 | `tests/unified_temporalstore_corpus.rs`, `partition_id.rs`, `http.rs`, `types.rs` |

The duplicate-test validator currently reports `rust_attributed_tests=585`,
`rust_test_guard_shared_corpus_marked_tests=89`, `shared_corpus_cases=179`,
`shared_corpus_steps=341`, and `cpp_existing_test_surfaces=194`.
It now also checks `tools/rust_product_test_baseline.json` so new Rust tests must declare either
`shared-corpus: <case>` or `rust-internal: <reason>`.

Target disposition:

| Current bucket | Share into corpus | Keep implementation-specific |
| --- | --- | --- |
| Storage/cache/local durability | Recovery behavior, dump/load manifests, cache refill, shared-store replay, corruption outcomes | Rust page-store helpers, serializer unit checks, local cache data-structure mechanics |
| Control plane/service behavior | Client/proxy/meta/data-node topology, lifecycle, admission, retry, convergence workflows | Rust runtime worker handles, local mock plumbing, HTTP adapter unit details |
| Raft/local consensus model | Log codec, snapshot install, membership, failover, read-index, catch-up semantics | Temporary local consensus scaffolding until replaced by production Raft implementation |
| API/model/ingestion/context/SDK | Redis/API commands, Feature/Sequence/IPS/Risk/Context, ingestion offsets/checkpoints/dead letters | Rust SDK conversion helpers and provider mocks without cross-language behavior |
| Rust storage crash harness | Crash/restart/corrupt artifact outcomes | Process harness wiring that is only needed to drive Rust-local faults |
| Other local tests | Readiness output, external chaos scenarios, replica replay, scale/fault logs | Thin binary/CLI argument parsing and local fixture setup |

## Highest-Value Tests To Share Next

1. Promote `temporalstore_compat.rs` cases into `compat/unified_temporalstore_cases.json`.
   The Feature policy/filter/aggregate, Sequence batch/filter, Redis feature, Raft consistency,
   and shared-store replication compatibility tests now have explicit shared-corpus references.
   Remaining unmarked candidates are readiness service-summary API and stream/page read APIs.

2. Add shared storage lifecycle corpus cases for the large storage bucket:
   packed page recovery, slot dump/load, compaction, GC retention, tiny-cache refill, shared-store
   sync/async replay, and corrupt-page/missing-ref negative cases.

3. Add shared Raft harness cases for the large Raft bucket:
   command/log codec, follower catch-up, snapshot install, leader transfer, read-index, membership
   changes, stale-read rejection, and WAL recovery. C++ can initially validate these as
   `existing_test` harness surfaces, then move to executable corpus replay when a native C++
   runner exists.

4. Continue shared control-plane promotion:
   topology-version changes, stale route invalidation, proxy admission, readonly/write-disabled
   policy, route quarantine/recovery, data-node lifecycle transitions, and metaserver scheduler
   lifecycle now have shared corpus cases. The next step is native C++ execution of those workflows
   instead of static source/harness validation.

5. Continue shared ingestion promotion:
   Kafka offsets, rebalance/backpressure, Flink checkpoints, dead letters, lag metrics, and
   restart idempotence now have shared corpus cases. The next step is native C++ execution of those
   workflows instead of static queue/proxy ingestion surface validation.

6. Add shared API/model cases:
   Redis command parity, context event/index/audit workflows, tonic SDK adapters, and C++ wire-model
   round trips.

7. Add shared ops/scale cases:
   readiness blockers and scale/fault workflow log assertions.

8. Add a guard for new tests:
   Done for Rust with `tools/validate_rust_product_test_guard.py`. Any new product behavior test is
   rejected unless it is backed by a shared corpus case; language-specific tests must name the
   internal Rust mechanic they protect.

## Current Limitation

The C++ side still validates the shared corpus mostly through its hook and context contract plus
`existing_test` surface gates. True same-test parity requires a native C++ executor that applies
every executable corpus command and compares every expected response.

Until that exists, the honest status is:

- Rust executes all 341 executable shared behavior steps.
- C++ validates the 179-case corpus shape, current context subset, exact C++ Raft case names,
  C++ storage/Raft required surfaces, the shared `raft_production_gate` metadata points at both
  `run_storage_raft_production_readiness.sh` and `run_raft_distributed_parity.sh`, and C++
  client/proxy/metaserver/data-node control-plane required surfaces.
- The control-plane and ingestion cases in the shared corpus are now Rust-executable
  `existing_test` gates and still static surface/evidence gates on the C++ side, not native C++
  workflow execution.
- 519 grandfathered Rust tests remain local or partially local. The product-behavior portion should be progressively
  converted into Rust-owned shared corpus cases; only implementation-internal tests should remain
  Rust-specific.
