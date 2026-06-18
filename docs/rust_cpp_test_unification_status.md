# Rust/C++ Test Unification Status

## Snapshot

Date: 2026-06-16

Rust attributed tests counted with `#[test]` and `#[tokio::test]` under
`crates/temporalstore-rust`: **488**.

Already tied directly to shared/C++ parity harnesses: **20 Rust test functions**.

Still Rust-specific: **468 Rust test functions**.

Unification target:

- All externally observable TemporalStore product behavior should move into Rust-owned shared
  corpus files first, then be consumed by both Rust and C++ runners.
- Rust-specific and C++-specific tests should remain only for language/runtime internals that are
  not a product contract: Rust helper units, serde-only details, borrow/async mechanics, C++
  object ownership, brpc/thrift glue, CMake/linking, and other implementation-only checks.
- No new duplicate behavioral tests should be added separately in Rust and C++; add a shared corpus
  case instead, then adapt each runner.

Duplicate test status:

```text
python3 tools/validate_no_duplicate_tests.py
rust_attributed_tests: no duplicate function names
shared corpus: no duplicate case names, per-case step names, or per-case command payloads
C++ existing-test surfaces: no repeated required_paths
```

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
raft_storage_parity_areas: 11
corpus_required_cpp_paths: 50
rust_evidence_snippets: 118
```

The eleven checked areas are storage object/page/slot lifecycle, slot dump/load recovery,
compaction/GC delayed destroy, shared-store sync/async replication and cursor-safe GC, Raft
command/log/WAL codec, Raft snapshot/membership scale, data-node Raft consensus contract, Raft
metaserver distributed fault contract, Raft failover secondary replication, exact C++ Raft case
names, and local scale/fault readiness gates.

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
cases: 45
steps: 110
executable behavior cases: 18
executable behavior steps: 78
C++ existing-test parity surface cases: 27
C++ existing-test parity surface steps: 32
C++ existing-test required paths: 83 unique paths plus 30 exact Raft alias references
```

Detailed inventory: `docs/unified_test_case_inventory.md`.

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

These counts are a migration backlog, not the desired end state. Each bucket should be split into
shared product/parity cases versus true language-internal tests. The shared portion moves into
`compat/unified_temporalstore_cases.json` or a sibling shared corpus stored in this Rust repo first.

| Bucket | Rust-specific tests | Main files |
| --- | ---: | --- |
| Storage/cache/local durability | 157 | `engine.rs`, `page_store.rs`, `cache.rs`, `shared_store.rs`, `oplog.rs`, `index_log.rs` |
| Control plane/service behavior | 157 | `client.rs`, `proxy.rs`, `data_node.rs`, `meta.rs`, `rebalance.rs`, `bin/server.rs`, `bin/metaserver.rs` |
| Raft/local consensus model | 110 | `raft.rs`, `bin/raft_node.rs` |
| API/model/ingestion/context/SDK | 17 | `redis.rs`, `context_workflow.rs`, `ingestion.rs`, `sdk.rs`, `types.rs` |
| Rust storage crash harness | 2 | `tests/storage_crash_harness.rs` |
| Other local tests | 24 | readiness, e2e, partition id, external chaos, HTTP, replica replay |

Total remaining Rust-specific tests: **467**.

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

7. Add a guard for new tests:
   any new product behavior test should be rejected or called out unless it is backed by a shared
   corpus case. Language-specific tests should name the internal Rust or C++ mechanic they protect.

## Current Limitation

The C++ side still validates the shared corpus mostly through its hook and context contract plus
`existing_test` surface gates. True same-test parity requires a native C++ executor that applies
every executable corpus command and compares every expected response.

Until that exists, the honest status is:

- Rust executes all 78 executable shared behavior steps.
- C++ validates the 45-case corpus shape, current context subset, exact C++ Raft case names,
  C++ storage/Raft required surfaces, the shared `raft_production_gate` metadata points at both
  `run_storage_raft_production_readiness.sh` and `run_raft_distributed_parity.sh`, C++
  client/proxy/metaserver/data-node control-plane required surfaces, the combined data-node plus
  metaserver Raft distributed parity gate, and the Rust ingestion/ops evidence gate in unified
  validation.
- 467 Rust-specific tests remain local. The product-behavior portion should be progressively
  converted into Rust-owned shared corpus cases; only implementation-internal tests should remain
  Rust-specific.
