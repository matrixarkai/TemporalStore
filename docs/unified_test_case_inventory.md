# Unified Test Case Inventory

## Summary

The canonical shared test corpus is stored in the Rust repo first:

```text
compat/unified_temporalstore_cases.json
```

Current inventory:

```text
total cases: 40
total steps: 100
executable shared behavior cases: 18
executable shared behavior steps: 78
C++ existing-test parity surface cases: 22
C++ existing-test parity surface steps: 22
C++ required source/test/harness paths: 83
required command kinds: 43
required response kinds: 16
```

The target is no duplicated Rust-only and C++-only tests for product behavior. Product behavior
should be represented as shared corpus cases. Rust-specific and C++-specific tests should remain
only for implementation internals that are not cross-language TemporalStore contracts.

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
| `risk_counter_window` | Risk increment/count over a timestamp window. |
| `risk_family_query_and_delete` | Risk family set/query plus common delete cleanup. |
| `context_node_roundtrip` | Context node upsert/read. |
| `context_event_index_audit_dirty_models` | Context event, secondary index, prompt-pack audit, and dirty-summary models. |
| `common_restart_persistence` | String/hash restart-read persistence. |
| `mixed_model_restart_persistence` | Feature plus Context restart-read persistence in one case. |
| `common_not_found_and_empty_reads` | Missing string/hash/exists reads and C++ `CommonExpire` not-found status. |
| `timestamped_query_bounds` | Feature and Sequence count limits and empty timestamp windows. |
| `feature_policy_filter_aggregate_lifecycle` | Feature append policy, aggregate, replace/delete, C++ row filtering, and scan-bound count behavior. |
| `sequence_batch_filter_groups` | Sequence unsorted writes, filtered ordered reads, batch groups, and missing sequence groups. |
| `context_missing_node_semantics` | Missing Context node returns a stable object key and `null` node. |

## C++ Existing-Test Parity Surface Cases

These are shared corpus gates, but not full native command replay yet. They make the shared corpus
fail if expected C++ source/test/harness surfaces disappear while Rust parity evidence still refers
to them.

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
| `cpp_redis_live_storage_smoke_parity_surfaces` | Redis live storage smoke surfaces. |
| `cpp_local_docker_replication_matrix_parity_surfaces` | Local Docker replication matrix surfaces. |
| `cpp_client_meta_sync_route_parity_surfaces` | Client meta-sync, route, pipeline, and request surfaces. |
| `cpp_proxy_serving_admission_parity_surfaces` | Proxy serving, heartbeat, config, HA calibration, and smoke surfaces. |
| `cpp_metaserver_scheduler_repair_parity_surfaces` | Metaserver scheduler, repair, placement, heartbeat, and retry surfaces. |
| `cpp_data_node_lifecycle_server_parity_surfaces` | Data-node lifecycle, heartbeat, server, and metaserver client surfaces. |

## Are There Still Rust-Specific Tests?

Yes. Current Rust-local attributed test count is:

```text
Rust attributed tests: 484
directly tied to shared/C++ parity harnesses: 17
still Rust-specific: 467
```

The `467` Rust-specific tests are a migration backlog, not the desired final state. They should be
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
| Product/API smoke tests | Redis/API command behavior, Feature/Sequence/IPS/Risk/Context behavior, lifecycle workflows. | brpc/thrift service glue and C++ fixture setup. |
| Storage tests | Logical recovery, dump/load, compaction, GC, corruption, shared-store replay. | C++ object lifetime, allocator, and storage class ownership units. |
| Raft tests | Log/snapshot/membership/failover behavior and durability outcomes. | byteraft integration wiring and C++ transport internals. |
| Scale/performance gates | Shared workload traces and SLO result formats. | Platform-specific packaging or benchmark harness mechanics. |
| Build/deployment checks | Runtime behavior and readiness output. | CMake/linking, dependency discovery, and binary packaging details. |

## Next Unification Work

1. Promote remaining `temporalstore_compat.rs` product behavior into shared corpus cases:
   Redis RESP parsing, stream/page read APIs, shared-store replication, and distributed workflow
   tests.
2. Add sibling shared corpora for storage, Raft, control-plane, ingestion, and scale/fault
   scenarios when a single command/response JSON file becomes too large.
3. Teach the C++ native runner to execute every executable shared behavior case, not only validate
   the corpus shape and context subset.
4. Add a guard so new product behavior tests must reference a shared corpus case. Rust-specific or
   C++-specific tests should state the implementation-only mechanic they protect.

## Validation Commands

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_no_duplicate_tests.py
python3 tools/validate_api_model_parity_evidence.py
TS_CPP_REPO=/path/to/cpp/TemporalStore python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```
