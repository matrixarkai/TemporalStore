# Unified Test Case Inventory

See [`rust_vs_cpp_temporalstore_parity_report.md`](rust_vs_cpp_temporalstore_parity_report.md) for
the current subsystem-level Rust-vs-C++ evidence map and the readiness evidence fields that these
shared cases feed.

## Summary

The canonical shared test corpus is stored in the Rust repo first:

```text
compat/unified_temporalstore_cases.json
```

Current inventory:

```text
total cases: 79
total steps: 167
executable shared behavior cases: 26
executable shared behavior steps: 107
C++ existing-test parity surface cases: 50
C++ existing-test parity surface steps: 57
C++ required source/test/harness paths: 133 unique paths plus 60 Raft path references
required command kinds: 59
required response kinds: 19
```

The target is no duplicated Rust-only and C++-only tests for product behavior. Product behavior
should be represented as shared corpus cases. Rust-specific and C++-specific tests should remain
only for implementation internals that are not cross-language TemporalStore contracts.

Focused C++ Raft-to-Rust validation uses the same corpus entries:

```bash
python3 tools/run_cpp_raft_cases_on_rust.py \
  --cpp-repo /path/to/cpp/TemporalStore \
  --artifact-dir /tmp/temporalstore-cpp-raft-cases-on-rust
```

That command reads `coverage.required_raft_case_names`, checks the referenced C++ Raft test or
harness paths when `--cpp-repo` is provided, emits `cpp-raft-cases-on-rust.json`, and runs the Rust
combined data-node plus metaserver Raft parity gate.

## Executable Shared Behavior Cases

These cases are executable command/response tests. Rust runs them through both the direct engine
path and the local HTTP client path. The C++ hook validates the same corpus shape today, and native
C++ execution should progressively cover every executable case.

| Case | Coverage |
| --- | --- |
| `common_string_hash_core` | String set/get plus hash multi-set/multi-get. |
| `common_lifecycle_delete_ttl` | TTL, immediate expire, delete, exists, and missing-read lifecycle behavior. |
| `hash_single_field_and_delete` | Hash set/get/increment/get-all/len/delete behavior. |
| `redis_compatible_set_core` | Set add and sorted members response behavior. |
| `feature_packed_timestamped_pages` | Packed timestamped Feature points plus restart query. |
| `sequence_cpp_feature_rows` | Sequence rows in the C++ feature-row shape. |
| `ips_options_range` | IPS add/query with action/table/request metadata. |
| `ips_snapshot_stat_filter_batch` | IPS load, batch-last grouping, snapshot, metadata filter, stats, and snapshot-report behavior. |
| `risk_counter_window` | Risk increment/count over a timestamp window. |
| `risk_family_query_and_delete` | Risk family set/query plus common delete cleanup. |
| `risk_manager_debug_fol` | Risk set-and-get, first/last FOL selection, manager summary, and debug window report behavior. |
| `context_node_roundtrip` | Context node upsert/read. |
| `context_event_index_audit_dirty_models` | Context event, secondary index, prompt-pack audit, dirty-summary models, C++ model IDs 9-13, timeline fanout, and validation limits. |
| `common_restart_persistence` | String/hash restart-read persistence. |
| `mixed_model_restart_persistence` | Feature plus Context restart-read persistence in one case. |
| `common_not_found_and_empty_reads` | Missing string/hash/exists reads and C++ `CommonExpire` not-found status. |
| `timestamped_query_bounds` | Feature and Sequence count limits and empty timestamp windows. |
| `feature_policy_filter_aggregate_lifecycle` | Feature append policy, aggregate, replace/delete, C++ row filtering, and scan-bound count behavior. |
| `sequence_batch_filter_groups` | Sequence unsorted writes, filtered ordered reads, batch groups, and missing sequence groups. |
| `context_missing_node_semantics` | Missing Context node returns a stable object key and `null` node. |
| `storage_dump_load_recovery` | Rust executes the C++ migration storage corpus through slot dump/load, restart, recovery, and logical reads. |
| `storage_fault_matrix` | Rust validates checksum mismatch, partial manifest, missing segment, stale manifest, and corrupt page-segment rejection. |
| `storage_follower_safe_gc` | Rust runs storage lifecycle with a lagging follower cursor and verifies recovery stays clean. |
| `storage_cache_refill` | Rust invalidates cache, warms from page-store refs, and verifies memory refill stats. |
| `storage_shared_store_sync_replay` | Rust replays the C++ migration storage corpus through sync local shared-store replication. |
| `storage_shared_store_async_replay` | Rust replays the C++ migration storage corpus through async local shared-store replication. |

## C++ Existing-Test Parity Surface Cases

These are shared corpus gates, but not full native C++ command replay yet. They make the shared
corpus fail if expected C++ source/test/harness surfaces disappear while Rust parity evidence still
refers to them. The seven `control_*` rows and six `ingestion_*` rows are now
`rust_executable_cxx_static`: Rust executes the named shared case runners through
`tools/run_control_plane_shared_cases.py` and `tools/run_ingestion_shared_cases.py`, while C++
remains a static source/harness surface gate until native C++ workflow runners are configured.

| Case | Coverage |
| --- | --- |
| `cpp_storage_object_page_slot_parity_surfaces` | C++ object/page/slot ownership sources. |
| `cpp_storage_manager_compaction_gc_parity_surfaces` | Storage manager, compaction, GC, and delayed-destroy surfaces. |
| `cpp_storage_oplog_index_replay_parity_surfaces` | Oplog, index-log, checkpoint/replay surfaces. |
| `cpp_storage_slot_context_test_parity_surfaces` | Slot/page/object and context storage test surfaces. |
| `cpp_data_raft_consensus_parity_surfaces` | Data-Raft consensus implementation surfaces. |
| `cpp_data_raft_replication_parity_surfaces` | Data-Raft replication payload/log surfaces. |
| `cpp_data_raft_unit_test_parity_surfaces` | Data-Raft unit test surfaces. |
| `cpp_data_raft_failover_harness_parity_surfaces` | Failover harness surfaces. |
| `cpp_data_raft_snapshot_restore_harness_parity_surfaces` | Snapshot/restore harness surfaces. |
| `cpp_data_raft_scale_transition_harness_parity_surfaces` | Scale-transition harness surfaces. |
| `cpp_storage_object_zone_evicter_expirer_parity_surfaces` | Object/zone/evicter/expirer surfaces. |
| `cpp_storage_replicator_guardrail_parity_surfaces` | Storage replication guardrail surfaces. |
| `cpp_data_raft_mixed_rw_harness_parity_surfaces` | Mixed read/write Raft harness surfaces. |
| `cpp_data_raft_multinode_scale_harness_parity_surfaces` | Multi-node Raft scale harness surfaces. |
| `cpp_raft_production_stress_gate_parity_surfaces` | Production/stress Raft gate surfaces. |
| `cpp_metaserver_raft_harness_parity_surfaces` | Metaserver Raft harness surfaces. |
| `storage_data_raft_replication_gtest` | Exact C++ data-Raft replication unit case, paired with the Rust distributed Raft harness. |
| `raft_metaserver_membership_failover_snapshot` | Exact C++ metaserver Raft membership/failover/snapshot case, now including post-failover replacement plus scale-down, paired with the Rust `metaserver_raft_harness` JSON gate. |
| `raft_data_node_scale_failover_snapshot` | Exact C++ data-node Raft scale/failover/snapshot case, now including post-snapshot rescale down/up, paired with Rust distributed and secondary-replication harnesses plus the combined data-node/metaserver Raft parity gate. |
| `raft_data_node_mixed_rw_and_membership` | Exact C++ data-node mixed read/write plus membership case, paired with Rust distributed and secondary-replication harnesses plus the combined data-node/metaserver Raft parity gate. |
| `raft_data_node_leader_election_failover` | Data-node leader election and failover as an explicit shared harness case, paired with the Rust process secondary-replication harness. |
| `raft_data_node_snapshot_restart_follower_lag` | Data-node snapshot install, restart recovery, follower lag, and catch-up as an explicit shared harness case. |
| `raft_data_node_membership_secondary_reads` | Data-node membership add/promote/remove and secondary-read visibility as an explicit shared harness case. |
| `raft_metaserver_leader_snapshot_restart` | Metaserver leader/failover, snapshot install, and restart recovery as an explicit shared harness case. |
| `raft_metaserver_membership_add_promote_remove` | Metaserver learner add, catch-up, promote, leader transfer, and voter remove as an explicit shared harness case. |
| `raft_openraft_process_rollout_evidence` | Production-readiness evidence case requiring LocalModel rejection and OpenRaft process-rollout/log-store evidence. |
| `raft_production_gate` | Exact C++ Raft production gate case, paired with the Rust storage/Raft production-readiness local gate and the combined data-node plus metaserver Raft distributed parity gate. `tools/run_raft_shared_cases.py` validates these shared Raft cases and can run the combined Rust parity gate once. |
| `cpp_redis_live_storage_smoke_parity_surfaces` | Redis live storage smoke surfaces. |
| `cpp_local_docker_replication_matrix_parity_surfaces` | Local Docker replication matrix surfaces. |
| `cpp_client_meta_sync_route_parity_surfaces` | Client meta-sync, route, pipeline, and request surfaces. |
| `cpp_proxy_serving_admission_parity_surfaces` | Proxy serving, heartbeat, config, HA calibration, and smoke surfaces. |
| `cpp_metaserver_scheduler_repair_parity_surfaces` | Metaserver scheduler, repair, placement, heartbeat, and retry surfaces. |
| `cpp_data_node_lifecycle_server_parity_surfaces` | Data-node lifecycle, heartbeat, server, and metaserver client surfaces. |
| `control_topology_version_change` | Rust-executable/C++-static gate for the shared client/proxy/meta topology-version change workflow. |
| `control_stale_route_invalidation` | Rust-executable/C++-static gate for stale route invalidation and one-refresh retry behavior. |
| `control_proxy_admission_policy` | Rust-executable/C++-static gate for proxy admission, drop-percent, and degraded preflight behavior. |
| `control_readonly_write_disabled_tables` | Rust-executable/C++-static gate for readonly/write-disabled/not-serving table policy behavior. |
| `control_route_quarantine_recovery` | Rust-executable/C++-static gate for backend quarantine, recovery probing, and degraded preflight behavior. |
| `control_data_node_load_reload_unload_lifecycle` | Rust-executable/C++-static gate for data-node load/reload/readonly/unload lifecycle behavior. |
| `control_metaserver_scheduler_lifecycle_workflow` | Rust-executable/C++-static gate for metaserver scheduler-issued load/reload/unload token behavior. |
| `ingestion_kafka_offset_ledger` | Rust-executable/C++-static gate for Kafka offset ledger, duplicate rejection, and valid-record continuation behavior. |
| `ingestion_kafka_rebalance_backpressure` | Rust-executable/C++-static gate for Kafka consumer-group rebalance and backpressure behavior. |
| `ingestion_flink_checkpoint_lifecycle` | Rust-executable/C++-static gate for Flink checkpoint precommit/commit/abort behavior. |
| `ingestion_dead_letter_export` | Rust-executable/C++-static gate for dead-letter capture/export and non-blocking ingestion behavior. |
| `ingestion_lag_metrics` | Rust-executable/C++-static gate for Kafka lag, committed offset, and ingestion metric behavior. |
| `ingestion_restart_idempotence` | Rust-executable/C++-static gate for restart/failover idempotence behavior for offsets and checkpoints. |
| `context_management_ingest_retrieve_pipeline` | Rust-executable/C++-static gate for Context management, ingest/extract, retrieval handoff, provider routing, and OpenViking-style block construction. |
| `context_retrieval_qa_synonym_ranking` | Rust-executable/C++-static gate for Context QA retrieval synonym and adjacent-phrase ranking. |
| `context_openviking_reasoning_vlm_parity` | Rust-executable/C++-static gate for OpenViking/VikingMem-style multi-hop, temporal, update, stale-memory, open-domain, and VLM context evidence. |
| `context_openviking_blocks_provider_switches` | Rust-executable/C++-static gate for OpenViking-style context blocks and open-source text/VLM provider switching. |
| `context_injection_prompt_pack_ordering` | Rust-executable/C++-static gate for prompt-pack ordering and selected-ref audit ordering. |
| `context_benchmark_injection_entity_segment_index` | Rust-executable/C++-static gate for ContextEntity/ContextSegment benchmark injection, source secondary-index lookup, L0/L1/L2 prompt blocks, and selected-ref audit coverage. |
| `context_benchmark_fixture_gates` | Shared C++/Rust benchmark contract for LOCOMO-style and LongMemEval_s fixture gates using MatrixArk/VikingMem report fields. |
| `context_benchmark_full_dataset_gates` | Shared C++/Rust benchmark contract for LOCOMO, LongMemEval_s, and Docker/open-model full-dataset gates with explicit threshold profiles and archive reports. |

## Unified Benchmark Report Contract

The benchmark cases are `existing_test` entries because full LOCOMO/LongMemEval_s scoring is an
external corpus/harness contract, not an in-engine command/response step. They still live in
`compat/unified_temporalstore_cases.json` so C++ and Rust consume the same case names, threshold
profiles, dataset artifacts, archive layout, and report fields.

Required report fields for both codebases:

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

Each `benchmark_per_query` row carries query/category, hit/rank, reader hit and answer,
expected answer terms, expected source refs, retrieved source IDs, latency, token counts, and token
reduction so C++/Rust comparisons can isolate retrieval, evidence-ordering, and reader-only misses.

Shared threshold profiles:

```text
fixture
locomo_full
longmemeval_full
oss_reader_full
```

Rust emits these fields through `tools/run_locomo_90_hit_rate.py`,
`tools/run_longmemeval_s_full_path.py`, and
`tools/run_context_benchmarks_docker_open_model.sh`. C++ should emit the same
`matrixark_vikingmem_context_benchmark_report_v1` JSON report shape and archive one manifest plus
one report JSON and misses JSONL per executed dataset.

C++ can use `compat/cpp_context_benchmark_report_adapter.h` as the native emitter template. Rust and
C++ benchmark outputs are compared with `tools/compare_context_benchmark_reports.py`, which validates
the shared report contract and compares summary plus per-query rows by `query_id`.
Full Docker/open-model benchmark archives are compared with
`tools/compare_context_benchmark_archives.py`, which validates both `manifest.json` files, matches
dataset execution statuses, delegates passed report pairs to the per-report comparator, and treats
skipped real LOCOMO/LongMemEval_s artifacts as explicit blockers unless the caller intentionally
allows skipped evidence.

## Are There Still Rust-Specific Tests?

Yes. Current Rust-local attributed test count is:

```text
Rust attributed tests: 546
shared-corpus marked Rust tests: 17
shared corpus cases: 79
shared corpus steps: 167
C++ existing-test surfaces: 133
```

The detailed reduction split and new-test guard live in
`docs/rust_product_test_reduction_guard.md`. The current split is:

```text
product behavior to move into shared corpus: 536
Rust-only internals that can remain local: 7
existing Rust tests already marked with shared-corpus references: 14
```

The Rust-attributed tests are a migration backlog, not the desired final state. They should be
split into:

| Rust-local bucket | Move into shared corpus | Keep Rust-specific |
| --- | --- | --- |
| Storage/cache/local durability | Recovery, dump/load, cache refill, corruption outcomes, shared-store replay. | Page-store helper units, cache data-structure mechanics, serializer internals. |
| Control plane/service behavior | Client/proxy/meta/data-node topology, lifecycle, admission, retry, convergence workflows. | Runtime worker handle units, local mock plumbing, adapter-only details. |
| Raft/local consensus model | Log codec, snapshot, membership, failover, read-index, catch-up semantics. | Temporary Rust-local consensus scaffolding until production Raft lands. |
| API/model/ingestion/context/SDK | Redis/API behavior, Feature/Sequence/IPS/Risk/Context, ingestion offsets/checkpoints/dead letters. | Rust SDK conversion helpers and provider mocks without cross-language behavior. |
| Storage crash harness | Crash/restart/corrupt artifact outcomes. | Harness plumbing needed only to drive Rust-local faults. |
| Other local tests | Readiness output, external chaos, replica replay, scale/fault logs. | CLI parsing and local fixture setup. |

## Are There Still C++-Specific Tests?

Yes. C++ still has local tests and smoke/performance gates that do not consume the shared corpus.
Those should follow the same rule:

| C++-local bucket | Move into shared corpus | Keep C++-specific |
| --- | --- | --- |
| Product/API smoke tests | Redis/API command behavior, Feature/Sequence/IPS/Risk/Context behavior, lifecycle workflows. | legacy C++ wire service glue and C++ fixture setup. |
| Storage tests | Logical recovery, dump/load, compaction, GC, corruption, shared-store replay. | C++ object lifetime, allocator, and storage class ownership units. |
| Raft tests | Log/snapshot/membership/failover behavior and durability outcomes. | byteraft integration wiring and C++ transport internals. |
| Scale/performance gates | Shared workload traces and SLO result formats. | Platform-specific packaging or benchmark harness mechanics. |
| Build/deployment checks | Runtime behavior and readiness output. | CMake/linking, dependency discovery, and binary packaging details. |

## Next Unification Work

1. Promote remaining `temporalstore_compat.rs` product behavior into shared corpus cases:
   readiness service-summary API and stream/page read APIs. Redis/feature/sequence/Raft/shared-store
   compatibility tests now have explicit shared-corpus references and can be removed or shrunk once
   the shared runner fully replaces their extra assertions.
2. Add sibling shared corpora for storage, Raft, control-plane, ingestion, and scale/fault
   scenarios when a single command/response JSON file becomes too large.
3. Move the new control-plane shared cases from Rust-executable/C++-static validation to native
   C++ execution: topology-version changes, stale route invalidation, proxy admission,
   readonly/write-disabled policy, route quarantine/recovery, data-node lifecycle, and metaserver
   scheduler lifecycle.
4. Move the new ingestion shared cases from Rust-executable/C++-static validation to native C++
   execution: Kafka offsets, rebalance/backpressure, Flink checkpoints, dead letters, lag metrics,
   and restart idempotence.
5. Teach the C++ native runner to execute every executable shared behavior case, not only validate
   the corpus shape and context subset.
6. Keep the new Rust product-test guard enabled:
   `python3 tools/validate_no_duplicate_tests.py` now runs
   `tools/validate_rust_product_test_guard.py`, which requires new Rust tests to declare either
   `shared-corpus: <case>` or `rust-internal: <reason>`.

## Validation Commands

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_no_duplicate_tests.py
python3 tools/validate_api_model_parity_evidence.py
TS_CPP_REPO=/path/to/cpp/TemporalStore python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```
