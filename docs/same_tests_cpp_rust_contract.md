# Same Tests For C++ And Rust TemporalStore

## What "Same Tests" Means

The same-test contract is not "Rust has similar tests" or "C++ has a separate smoke test with the
same idea." It means both codebases execute the same ordered test corpus file and compare against
the same expected logical responses.

The desired end state is no duplicate Rust-only and C++-only tests for product behavior. The Rust
repo owns the canonical shared cases first, and both implementations consume them. Tests may remain
language-specific only when they protect implementation mechanics that are not a cross-language
TemporalStore contract, such as Rust helper structs/serde internals, C++ object ownership,
legacy C++ wire glue, build/linking behavior, or temporary harness plumbing.

The current shared corpus is:

```text
compat/unified_temporalstore_cases.json
```

That file is the source of truth for cross-codebase behavior. It contains serialized command JSON,
expected response JSON, restart markers, shard ids, case names, step names, and a required coverage
manifest. The Rust and C++ runners must not hard-code different expected values.

## Current Shared Test Coverage

Current corpus:

```text
schema_version: 1
name: temporalstore-unified-cpp-rust-corpus
cases: 79
steps: 166
executable behavior cases: 26
executable behavior steps: 106
required command kinds: 59
required response kinds: 19
C++ existing-test parity surfaces: 133 unique required paths plus 60 Raft path references
```

The shared cases are:

- `common_string_hash_core`: string set/get plus hash multi-set/multi-get.
- `common_lifecycle_delete_ttl`: persistent TTL, immediate expire, delete, and exists semantics.
- `hash_single_field_and_delete`: hash set/get/increment/read-all/len/delete behavior.
- `redis_compatible_set_core`: Redis-compatible set add/members command-response behavior.
- `feature_packed_timestamped_pages`: packed timestamped Feature points and restart query.
- `sequence_cpp_feature_rows`: Sequence rows encoded in the C++ feature-row shape.
- `ips_options_range`: IPS add/query range with action/table/request metadata.
- `ips_snapshot_stat_filter_batch`: IPS load, batch-last grouping, snapshot, metadata filter,
  stats, and snapshot-report behavior.
- `risk_counter_window`: Risk increment/count over a time window.
- `risk_family_query_and_delete`: C++ risk-family set/query plus common delete cleanup.
- `risk_manager_debug_fol`: Risk set-and-get, first/last FOL selection, manager summary, and
  debug window report behavior.
- `context_node_roundtrip`: Context node upsert/read.
- `context_event_index_audit_dirty_models`: Context event, secondary index, prompt-pack audit, and
  dirty-summary models with restart-read persistence.
- `common_restart_persistence`: string/hash restart-read persistence.
- `mixed_model_restart_persistence`: Feature plus Context restart-read persistence in one case.
- `common_not_found_and_empty_reads`: missing string/hash/exists reads plus C++ `CommonExpire`
  not-found status.
- `timestamped_query_bounds`: Feature and Sequence count limits and empty timestamp windows.
- `feature_policy_filter_aggregate_lifecycle`: Feature append policy, aggregate query, replace,
  delete, C++ feature-row filter, and scan-bound count semantics.
- `feature_nested_proto_aggregate_semantics`: Feature nested/proto-shaped payload roundtrip,
  C++ row filtering, and sum/avg/min/max/count aggregate semantics.
- `sequence_batch_filter_groups`: Sequence unsorted insert, filtered ordered query, scan-bound
  count semantics, batch query groups, and missing sequence groups.
- `context_missing_node_semantics`: missing Context node returns a stable object key with `null`
  node.
- `storage_dump_load_recovery`: Rust executes the C++ migration storage corpus through slot
  dump/load, restart, recovery, and logical reads.
- `storage_fault_matrix`: Rust validates checksum mismatch, partial manifest, missing segment,
  stale manifest, and corrupt page-segment rejection.
- `storage_follower_safe_gc`: Rust runs storage lifecycle with a lagging follower cursor and
  verifies recovery stays clean.
- `storage_cache_refill`: Rust invalidates cache, warms from page-store refs, and verifies memory
  refill stats.
- `storage_shared_store_sync_replay` and `storage_shared_store_async_replay`: Rust replays the C++
  migration storage corpus through sync and async local shared-store replication.
- C++ storage/Raft parity surfaces are split into narrow `existing_test` cases so missing C++
  coverage fails by exact gap:
  `cpp_storage_object_page_slot_parity_surfaces`,
  `cpp_storage_manager_compaction_gc_parity_surfaces`,
  `cpp_storage_oplog_index_replay_parity_surfaces`,
  `cpp_storage_slot_context_test_parity_surfaces`,
  `cpp_data_raft_consensus_parity_surfaces`,
  `cpp_data_raft_replication_parity_surfaces`,
  `cpp_data_raft_unit_test_parity_surfaces`,
  `cpp_data_raft_failover_harness_parity_surfaces`,
  `cpp_data_raft_snapshot_restore_harness_parity_surfaces`, and
  `cpp_data_raft_scale_transition_harness_parity_surfaces`.
- Current C++ storage/Raft gap-fill parity adds eight more exact gates:
  `cpp_storage_object_zone_evicter_expirer_parity_surfaces`,
  `cpp_storage_replicator_guardrail_parity_surfaces`,
  `cpp_data_raft_mixed_rw_harness_parity_surfaces`,
  `cpp_data_raft_multinode_scale_harness_parity_surfaces`,
  `cpp_raft_production_stress_gate_parity_surfaces`,
  `cpp_metaserver_raft_harness_parity_surfaces`,
  `cpp_redis_live_storage_smoke_parity_surfaces`, and
  `cpp_local_docker_replication_matrix_parity_surfaces`.
- Current C++ Raft test unification adds the exact C++ Raft corpus case names with paired Rust
  harness metadata:
  `storage_data_raft_replication_gtest`,
  `raft_metaserver_membership_failover_snapshot`,
  `raft_data_node_scale_failover_snapshot`,
  `raft_data_node_mixed_rw_and_membership`,
  `raft_data_node_leader_election_failover`,
  `raft_data_node_snapshot_restart_follower_lag`,
  `raft_data_node_membership_secondary_reads`,
  `raft_metaserver_leader_snapshot_restart`,
  `raft_metaserver_membership_add_promote_remove`,
  `raft_openraft_process_rollout_evidence`, and
  `raft_production_gate`.
- Current C++ control-plane test unification adds shared Client/Proxy/DataNode/Meta workflow case
  names with Rust evidence metadata:
  `control_topology_version_change`,
  `control_stale_route_invalidation`,
  `control_proxy_admission_policy`,
  `control_readonly_write_disabled_tables`,
  `control_route_quarantine_recovery`,
  `control_data_node_load_reload_unload_lifecycle`, and
  `control_metaserver_scheduler_lifecycle_workflow`.
- Current C++ ingestion test unification adds shared queue/Kafka/Flink workflow case names with
  Rust evidence metadata:
  `ingestion_kafka_offset_ledger`,
  `ingestion_kafka_rebalance_backpressure`,
  `ingestion_flink_checkpoint_lifecycle`,
  `ingestion_dead_letter_export`,
  `ingestion_lag_metrics`, and
  `ingestion_restart_idempotence`.
- Current Context and benchmark unification adds shared pipeline and MatrixArk/VikingMem-style
  benchmark report contracts:
  `context_management_ingest_retrieve_pipeline`,
  `context_retrieval_qa_synonym_ranking`,
  `context_openviking_reasoning_vlm_parity`,
  `context_openviking_blocks_provider_switches`,
  `context_injection_prompt_pack_ordering`,
  `context_benchmark_fixture_gates`, and
  `context_benchmark_full_dataset_gates`.

## Unified Benchmark Contract

The benchmark cases map LOCOMO and LongMemEval_s reports into the same corpus inventory used for
API, storage, Raft, control-plane, and ingestion cases. They use `existing_test` command entries
with `suite=cpp_context_benchmark_parity` because each side executes a benchmark harness and emits a
report, rather than executing a single in-engine command.

Both C++ and Rust must consume these shared benchmark fields from
`compat/unified_temporalstore_cases.json`:

- `datasets`: benchmark artifact names, artifact kind, default paths, and threshold profile.
- `threshold_profiles`: one or more of `fixture`, `locomo_full`, `longmemeval_full`, or
  `oss_reader_full`.
- `report_contract.format`: `matrixark_vikingmem_context_benchmark_report_v1`.
- `report_contract.required_fields`: the MatrixArk/VikingMem report fields that both sides must
  emit.
- `archive_contract`: one `manifest.json`, one report JSON, and one misses JSONL per executed
  dataset. Missing real datasets must be recorded as skipped or blocked, never as passing fixture
  evidence.

Required benchmark report fields:

```text
benchmark_family
benchmark_hit_at_k
benchmark_recall_at_k
benchmark_mean_reciprocal_rank
benchmark_token_reduction_percent
benchmark_retrieval_p50_ms
benchmark_retrieval_p95_ms
benchmark_reader_p50_ms
benchmark_reader_p95_ms
benchmark_quality_ready
benchmark_threshold_passed
benchmark_threshold_violation_count
benchmark_threshold_violations
benchmark_thresholds
benchmark_per_query_count
case_count
hit_rate
reader_hit_rate
reader_mode_requested
reader_mode_effective
reader_provider_name
reader_model
paper_comparable_claim_ready
rust_temporalstore_full_replay_ready
```

Required `benchmark_per_query` fields now include the query ID, category, retrieval hit/rank,
reader hit, reader answer, expected answer terms, expected source refs, retrieved source IDs,
retrieval/reader latency, retrieved blocks, source tokens, retrieved tokens, and token reduction.

Rust currently emits this shape through:

```bash
python3 tools/run_locomo_90_hit_rate.py --threshold-profile locomo_full
python3 tools/run_longmemeval_s_full_path.py --threshold-profile fixture
bash tools/run_context_benchmarks_docker_open_model.sh
```

C++ should emit the same JSON shape from its native MatrixArk/VikingMem benchmark path and wire the
result into the same corpus case names.

The installable C++ adapter template is
`compat/cpp_context_benchmark_report_adapter.h`, and the adapter workflow is documented in
[cpp_context_benchmark_report_adapter.md](cpp_context_benchmark_report_adapter.md). Cross-repo
benchmark output comparison uses:

```bash
python3 tools/compare_context_benchmark_reports.py \
  --rust-report /tmp/rust_locomo_report.json \
  --cpp-report /tmp/cpp_locomo_report.json \
  --case-name context_benchmark_full_dataset_gates \
  --dataset locomo
```

## Rust Runner

Rust executes the corpus in:

```text
crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs
```

Run it with:

```bash
tools/run_temporalstore_unified_tests.sh
```

That script runs:

```bash
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
```

Rust currently runs the same corpus through two paths:

- direct `TemporalEngine`
- `TemporalStoreClient` plus `TemporalStoreTable::execute` over the local HTTP API

Both Rust paths reuse the same page/index directories for restart-read steps.

## Required C++ Runner

C++ must provide a runner that accepts the corpus path and executes every case and step in order.
Install the shared wrapper into the C++ checkout from this Rust checkout:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
  python3 tools/run_temporalstore_unified_tests.py --install-cpp-runner
```

That creates:

```text
/path/to/cpp/TemporalStore/tools/run_temporalstore_unified_tests.sh
```

The installed C++ wrapper has the same entry point as the Rust wrapper, but delegates to the native
C++ executor through `TS_CPP_UNIFIED_NATIVE_CMD`.

The native C++ executor contract is:

```bash
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh --corpus /absolute/path/to/compat/unified_temporalstore_cases.json
```

Or, when launched from the Rust repo:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_TEST_CMD='{cpp_repo}/tools/run_temporalstore_unified_tests.sh --corpus {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

The native C++ executor must:

- parse `schema_version`, `coverage`, `cases`, `steps`, `command`, `expect`, and `restart_before`
- fail if any `coverage.required_case_names`, `coverage.required_command_kinds`, or
  `coverage.required_response_kinds` entry is absent from the corpus
- fail on duplicate case names, duplicate step names within a case, or duplicate exact command
  payloads within a case
- validate `cpp_context_benchmark_parity` entries by consuming the dataset list, threshold profiles,
  report contract, and archive contract
- execute each command against C++ TemporalStore
- compare the actual logical response to `expect`
- restart or reload the local C++ engine when `restart_before=true`
- fail closed on unknown command fields, missing expected fields, unsupported data models, or
  response mismatches

## Running Both Codebases

From the Rust checkout:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_TEST_CMD='{cpp_repo}/tools/run_temporalstore_unified_tests.sh --corpus {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If the C++ repo already contains `tools/run_temporalstore_unified_tests.sh`, this shorter form is
valid:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If the C++ wrapper is installed, set only the native executor command:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

For strict same-test enforcement, require native C++ corpus execution:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp-native
```

The shell wrapper also supports the strict mode:

```bash
TS_RUN_CPP_UNIFIED_TESTS=1 \
TS_REQUIRE_CPP_NATIVE=1 \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

`--require-cpp` means a C++ hook must run. `--require-cpp-native` means a C++ native executor must
be configured through `TS_CPP_UNIFIED_TEST_CMD` or `TS_CPP_UNIFIED_NATIVE_CMD`, so the C++ side
actually applies every corpus command and compares every expected response.

For CI, require C++ execution:

```bash
TS_RUN_CPP_UNIFIED_TESTS=1 \
TS_CPP_UNIFIED_TEST_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

## What Is Not Yet The Same

These are not yet true same tests:

- Rust-only product behavior tests under `crates/temporalstore-rust/src/**` or
  `crates/temporalstore-rust/tests/**` that do not yet have a shared corpus case.
- C++ local smoke tests that do not consume `compat/unified_temporalstore_cases.json`.
- Rust storage migration tests using `compat/storage_migration_corpus.json`; the main dump/load,
  fault-matrix, cache-refill, and shared-store replay paths are now referenced from the unified
  corpus, while adapter-only migration details can remain Rust-local.
- Rust SDK contract validation in `tools/validate_sdk_contract.py`; that check protects the
  versioned open-source API schema, but behavior should still be represented in shared cases.
- C++ p99/performance gates; those compare thresholds and workload classes, but should eventually
  consume the exact same operation trace for workload shape.
- Proxy, metaserver, data-node lifecycle, ingestion, RESP wire protocol, storage recovery, Raft
  failover, and context provider tests outside the unified corpus.
- The new storage and data-Raft unified cases are C++ parity surface gates today. They prove the
  shared manifest names the concrete C++ source/test/harness surfaces, but they are not yet full
  Rust/C++ native command replay for dump/load, page corruption, Raft leader failover, or snapshot
  install.

Allowed language-specific tests after unification:

- Rust-only internals: page-store helper units, cache data structures, serde/codec edge units,
  async worker lifecycle helpers, CLI argument parsing, and Rust-only test fixture plumbing.
- C++-only internals: object lifetime/ownership units, legacy C++ wire service glue, CMake/linking,
  allocator or memory-layout tests, and C++-only fixture plumbing.
- Temporary scaffolding tests while a product behavior is being moved into a shared corpus. These
  should name the target shared case or parity area they are waiting on.

## Gap Fill Plan For Real Same-Test Parity

1. Add the C++ corpus runner if it does not exist in the C++ repo.
2. Make C++ CI call the runner with `compat/unified_temporalstore_cases.json`.
3. Expand the Rust-owned corpus into separate cases for context extract/inject, storage
   dump/load/restart, proxy/client routing, metaserver topology, data-node lifecycle, ingestion,
   RESP wire protocol, and Raft failover.
4. Add negative cases: not found, stale route, readonly table, bad lifecycle state, corrupt storage
   artifact, invalid timestamped page payload, and retry-safe write failure.
5. Require `--both --require-cpp` in the parity gate before claiming C++ parity.
6. Generate tonic/prost SDK bindings from `proto/temporalstore/v1/temporalstore.proto` and route
   the generated service through the same corpus-backed execution path.

Until C++ consumes the shared corpus in CI, the honest status is: Rust has a same-test contract and
runner, but cross-codebase same-test enforcement is only complete when the C++ runner executes the
same corpus and fails on the same expected-response mismatches.

New product behavior tests should follow this path:

1. Add or update a shared case under `compat/` in the Rust repo.
2. Teach the Rust runner to execute it.
3. Teach the C++ hook/native runner to validate or execute the same case.
4. Only add language-specific unit coverage for implementation details that cannot be expressed as
   a shared product behavior.

Rust now enforces this for new attributed tests. Existing Rust tests are grandfathered in
`tools/rust_product_test_baseline.json`; any new test must include either:

```rust
// shared-corpus: case_name
```

or:

```rust
// rust-internal: validates local helper parsing only
```

The guard is `python3 tools/validate_rust_product_test_guard.py`, and it is also run by
`python3 tools/validate_no_duplicate_tests.py`.

## Local Test Run: 2026-06-16

Rust checkout:

```text
C:\Users\Deeproute\Documents\Codex\2026-06-10\pull-rust-temporalstore-code-from-matrixarkai\work\TemporalStore
```

C++ checkout:

```text
C:\Users\Deeproute\Documents\Codex\2026-06-07\what-s-the-topology-for-all\temporalstore-service-fix
```

Shared corpus command:

```bash
TS_CPP_REPO=/mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix \
python3 tools/run_temporalstore_unified_tests.py \
  --both \
  --require-cpp
```

Result:

- Rust unified corpus runner passed.
- Rust direct engine path passed.
- Rust `TemporalStoreClient` plus local HTTP path passed.
- C++ unified hook passed against the then-current 45-case `compat/unified_temporalstore_cases.json`.
- C++ hook validated the shared `coverage` manifest, duplicate-test rejection rules, and current
  unified command names.
- C++ hook confirmed the required local C++ parity surfaces are present.

C++ fast local CI guard command:

```bash
env \
  ITERATIONS=1 \
  RUN_FULL_GATE=0 \
  DEPENDENCY_CACHE_RUN_BUILD_SMOKE=0 \
  RESULT_DIR=/tmp/temporalstore-cpp-ci-guard-unified-1781642126 \
  tools/run_ci_guard_ubuntu22.sh
```

Result:

- `syntax`: pass
- `dependency_cache`: pass
- `prometheus_unit`: pass
- `raft_summary`: pass
- `monitoring_health`: pass
- total passed cases: 5
- total failed cases: 0

Important caveat: this local run did not execute a full native C++ command-by-command corpus
executor because `TS_CPP_UNIFIED_NATIVE_CMD` was not configured. The C++ side validated the shared
corpus and required parity surfaces through its hook, then the C++ fast CI guard passed. Full
same-test enforcement still requires wiring `TS_CPP_UNIFIED_NATIVE_CMD` to a native C++ executor
that applies every corpus command and compares every expected response.

Strict native enforcement check:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --cpp \
  --require-cpp-native \
  --cpp-repo /mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
```

Expected current result: fail closed until `TS_CPP_UNIFIED_NATIVE_CMD` or `TS_CPP_UNIFIED_TEST_CMD`
is configured. This is intentional; it prevents a C++ hook-only run from being mistaken for true
same-test C++ execution.

## Local Repeat Run: 2026-06-16

After the C++ hook was updated to accept the current shared context command shapes, the shared
Rust/C++ command was repeated eight times:

```bash
TS_CPP_REPO=/mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix \
CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

Result for all 8 iterations:

- Rust direct engine corpus path passed.
- Rust `TemporalStoreClient` plus local HTTP corpus path passed.
- C++ unified corpus hook passed against the same 16-case corpus.
- C++ native context contract passed with `cases=4` and `context_steps=13`.

## Storage And Raft Repeat Run: 2026-06-16

The shared corpus now includes narrow storage and data-Raft parity surface cases:

```text
cpp_storage_object_page_slot_parity_surfaces
cpp_storage_manager_compaction_gc_parity_surfaces
cpp_storage_oplog_index_replay_parity_surfaces
cpp_storage_slot_context_test_parity_surfaces
cpp_data_raft_consensus_parity_surfaces
cpp_data_raft_replication_parity_surfaces
cpp_data_raft_unit_test_parity_surfaces
cpp_data_raft_failover_harness_parity_surfaces
cpp_data_raft_snapshot_restore_harness_parity_surfaces
cpp_data_raft_scale_transition_harness_parity_surfaces
```

The storage cases require the C++ object/page/slot lifecycle, storage manager, compaction, GC,
oplog, and slot-context test files as separate gap-fill gates. The data-Raft cases require the C++
data-Raft consensus, replication, unit test, failover, snapshot-restore, and scale-transition
harness files as separate gap-fill gates.

Repeated command:

```bash
TS_CPP_REPO=/mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix \
CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

Repeat result: all 9 iterations passed.

Validation in each iteration:

- Rust direct engine corpus path executes all executable shared command-response cases.
- Rust `TemporalStoreClient` plus local HTTP corpus path executes all executable shared
  command-response cases.
- Rust validates the storage/Raft `existing_test` case names and command kind in the same shared
  coverage manifest.
- C++ hook validates the then-current 45-case corpus, including required storage/Raft source and harness
  paths.
- C++ native context contract validates the shared context subset.

## Storage And Raft Gap-Fill Run: 2026-06-16

The broad storage/Raft surface checks were split into 10 narrow C++ parity gates, raising the
shared corpus to 26 cases. This makes missing C++ storage/Raft coverage fail by exact surface:
object/page/slot ownership, storage manager compaction/GC, oplog replay, slot-context tests,
data-Raft consensus, replication, unit tests, failover, snapshot restore, and scale transitions.

The expanded Rust+C++ command was repeated 9 times:

```bash
TS_CPP_REPO=/mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix \
CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

Result: all 9 iterations passed against the 26-case shared corpus.

## Current C++ Storage And Raft Gap-Fill Run: 2026-06-16

The shared corpus was compared against the current local C++ checkout and expanded by eight more
storage/Raft gap-fill gates:

```text
cpp_storage_object_zone_evicter_expirer_parity_surfaces
cpp_storage_replicator_guardrail_parity_surfaces
cpp_data_raft_mixed_rw_harness_parity_surfaces
cpp_data_raft_multinode_scale_harness_parity_surfaces
cpp_raft_production_stress_gate_parity_surfaces
cpp_metaserver_raft_harness_parity_surfaces
cpp_redis_live_storage_smoke_parity_surfaces
cpp_local_docker_replication_matrix_parity_surfaces
```

These gates make current C++ storage/Raft parity fail precisely when eviction/expiry/zone/object
surfaces, storage replication guardrails, mixed read/write Raft, multi-node scale, production/stress
Raft tooling, metaserver Raft harnesses, Redis live storage smoke, or Docker replication matrix
coverage disappears from the C++ codebase.

The expanded Rust+C++ command was repeated 8 times against the current local C++ checkout:

```bash
TS_CPP_REPO=/mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix \
CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

Result: all 8 iterations passed against the 34-case shared corpus.

## Historical Unified API/Model Expansion: 2026-06-16

At this stage, the shared corpus had 59 cases and 146 steps after exact C++ Raft case-name
unification. Ten C++-named Rust-local behavior groups were promoted into executable shared cases:

```text
feature_policy_filter_aggregate_lifecycle
sequence_batch_filter_groups
ips_snapshot_stat_filter_batch
risk_manager_debug_fol
storage_dump_load_recovery
storage_fault_matrix
storage_follower_safe_gc
storage_cache_refill
storage_shared_store_sync_replay
storage_shared_store_async_replay
```

These cases cover advanced Feature append policy, aggregate query, replace/delete lifecycle,
filtered C++ feature-row payloads, Sequence filtered queries, scan-bound count semantics, batch
query groups, missing sequence groups, IPS snapshot/filter/stat/batch metadata behavior, and Risk
manager/debug/FOL behavior. The storage cases cover slot dump/load restart recovery, manifest
fault rejection, follower-cursor lifecycle protection, cache refill from page-store refs, and sync
plus async local shared-store replay.

At this stage, the Rust runner executed 100 product behavior steps through both direct engine and
local HTTP client paths, plus 6 storage parity steps through direct engine/admin storage paths. The
C++ hook validated the same 59-case corpus, current context contract, coverage manifest,
duplicate-test rules, exact C++ Raft case names, and required C++ parity surfaces.
