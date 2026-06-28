# Unified Test Case Inventory

See [`rust_vs_cpp_temporalstore_parity_report.md`](rust_vs_cpp_temporalstore_parity_report.md) for
the current subsystem-level Rust-vs-C++ evidence map and the readiness evidence fields that these
shared cases feed.

## Summary

The canonical shared test corpus is being externalized into the standalone
`TemporalStoreTestCorpus` repository so C++ and Rust can consume the same case
files, schemas, result contract, and comparator. During the transition, the Rust
repo keeps a local fallback copy:

```text
compat/unified_temporalstore_cases.json
```

The Rust runner resolves `TEMPORALSTORE_TEST_CORPUS`, then
`third_party/TemporalStoreTestCorpus`, then the sibling `../TemporalStoreTestCorpus`,
then the local fallback. When the external repository is checked out, run the
existing Rust validator against it with either:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --validate-only \
  --corpus ../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

or:

```bash
TEMPORALSTORE_TEST_CORPUS=../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json \
  python3 tools/run_temporalstore_unified_tests.py --validate-only
```

And enforce dependency wiring with:

```bash
python3 tools/validate_temporalstore_test_corpus_dependency.py --require-external
```

The repo-level Rust/C++ parity runner is:

```bash
python3 tools/run_unified_cpp_rust_parity.py \
  --cpp-repo wsl:/root/src/github-services/TemporalStore \
  --output /tmp/temporalstore-unified-cpp-rust-parity.json
```

That command validates the shared corpus, checks Rust-owned evidence paths, checks C++ static
source/test/harness paths, and emits a `temporalstore_unified_cpp_rust_parity_report_v1` report
whose `cases` array also follows the comparator-friendly `temporalstore_unified_case_report_v1`
shape. When the C++ repo has a native corpus executor, run it through the same entry point:

```bash
TS_CPP_UNIFIED_TEST_CMD='/path/to/cpp_runner --corpus {corpus}' \
  python3 tools/run_unified_cpp_rust_parity.py \
    --cpp-repo wsl:/root/src/github-services/TemporalStore \
    --run-rust \
    --output /tmp/temporalstore-unified-cpp-rust-parity.json
```

Use `--run-rust` only for focused or scheduled runs; the default mode is intentionally a fast
contract/evidence-path validation so both repositories can share the API/test inventory without
running every expensive storage, Raft, context, ingestion, and benchmark harness on every edit.
The report fails closed when a referenced Rust/C++ evidence path is missing, and its
`missing_required_paths` array is the migration backlog for the next C++ adapter or shared-case
cleanup pass. The runner also records `alias_path` when an older shared-case path maps to a
current C++ file that owns the same product surface, for example `src/proxy/proxy_server.cc` to
`src/proxy/proxy.cc`; aliases are only layout compatibility, not readiness evidence by themselves.
As of the current WSL C++ checkout, the joint runner resolves four moved C++ surfaces through
aliases and leaves three concrete missing surfaces: `docker-compose.context-benchmarks.yml`,
`src/client/example/kafka_consumer_group_runtime.cc`, and
`src/client/example/flink_checkpoint_replay_example.cc`.

Current inventory:

```text
total cases: 172
total steps: 334
executable shared behavior cases: 172
executable shared behavior steps: 334
C++ existing-test/static parity surfaces: 194
C++ adapter coverage families: 9
C++ required source/test/harness paths: 198 unique paths
required command kinds: 64
required response kinds: 20
```

The target is no duplicated Rust-only and C++-only tests for product behavior. Product behavior
should be represented as shared corpus cases. Rust-specific and C++-specific tests should remain
only for implementation internals that are not cross-language TemporalStore contracts.

The Rust grandfathered-test migration ledger is tracked in:

```text
tools/rust_product_test_migration_ledger.json
```

Current grandfathered Rust test dispositions:

| Disposition | Count |
| --- | ---: |
| `move_to_shared` | 512 |
| `rust_internal` | 7 |
| `cpp_out_of_scope` | 0 |
| `duplicate/remove` | 0 |

The next migration target is the Raft ByteRaft-derived process/fault/readiness family, followed by
storage/cache recovery cases and Context pipeline model cases. The storage family now includes
`storage_slot_first_physical_index`, which validates Rust's C++-style
`Index -> SlotNode -> PageIndex` authority for mixed page-backed product-model writes,
including Risk, and checks the C++ 17-byte `PageIndex` / 24-byte `SlotNode` packed-size
evidence while keeping C++ execution as a shared-corpus adapter target. The
`storage_object_manager_slotstore_runtime_authority` case now verifies the named
ObjectManager/SlotStore runtime authority modules against the same physical index. It also includes
`storage_slot_layout_transitions`, which covers native SlotStore-style layout transitions
across single-page object, multi-object, multi-page object, delete, compaction, slot
dump/load, and restart. `storage_model_layout_compaction_policies` covers
model-layout-aware compaction policy evidence for string, hash, set, timestamped,
context, and Risk model families, including object-page packing, cold-page rewrite,
stale-page density, tombstone-density, and index rewrite fields.
`storage_merged_dump_load_lifecycle` covers the merged dump/load policy evidence:
validated multi-slot source manifests, source-slot coverage rejection, rollback marker evidence,
load-version handoff, and stale object/page conflict preflight reports. The storage family also now includes focused
C++/Rust shared cases for object-manager cold/hot reload, PageAddress disk/cache
fallback, tombstone compaction, stale-page density compaction, merged dump/load restart
interruption, GC plus eviction under cold reads, continuous StorageManager background
runtime with jitter/backoff/pause/resume/per-phase flags/bounded work, and Risk/Context
page-backed restart parity.

Recent shared-case additions moved seven Rust data-node Raft API tests into the common contract:
`server_raft_status_admin_routes`, `server_raft_apply_health_route`,
`server_raft_membership_apply_route`, `server_raft_control_scale_up_down`,
`server_raft_control_accept_leadership`, `server_raft_admin_wait_applied`, and
`server_raft_byteraft_runtime_admin_route`. C++ currently contributes static server/Raft source and
test surfaces for those cases until a native C++ shared runner executes the same case IDs. The new
ByteRaft runtime-admin case requires a shared JSON shape with a capability matrix plus per-peer
match/next index, inflight bytes, append request/accept/reject counters, append/reorder queues,
reorder accept/release/reject counters, snapshot sender/downloader
lifecycle, WAL segments with bytes/record counts/sequence bounds, read-index/lease evidence, stale
follower rejection, and matching Prometheus metrics for scrape-based operator
parity. Snapshot lifecycle fields include send attempt/complete/failure counters,
install start/complete/reject/rollback counters, received/total chunks, retry count, and
backpressure rejection counters, including rejection of concurrent snapshot send attempts for the
same peer. Read-safety fields include read-index, lease-read, and pre-vote request/accept/reject
counters. Rust now also validates that configured in-flight append entry/byte limits reject
saturated peer pipelines, and that the per-peer pipeline and read-safety state is persisted through
WAL restore.

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
| `feature_nested_proto_aggregate_semantics` | Feature nested/proto-shaped payload roundtrip, C++ row filtering, and sum/avg/min/max/count aggregate semantics. |
| `sequence_batch_filter_groups` | Sequence unsorted writes, filtered ordered reads, batch groups, and missing sequence groups. |
| `context_missing_node_semantics` | Missing Context node returns a stable object key and `null` node. |
| `storage_dump_load_recovery` | Rust executes the C++ migration storage corpus through slot dump/load, restart, recovery, and logical reads. |
| `storage_fault_matrix` | Rust validates checksum mismatch, partial manifest, missing segment, stale manifest, and corrupt page-segment rejection. |
| `storage_follower_safe_gc` | Rust runs storage lifecycle with a lagging follower cursor and verifies recovery stays clean. |
| `storage_shared_store_oplog_cursor_retention` | Shared-store WAL GC refuses to reclaim WAL objects still needed by a saved follower replay cursor. |
| `storage_shared_store_checkpoint_cursor_retention` | Shared-store checkpoint GC retains the checkpoint generation anchoring a saved follower replay cursor. |
| `codex_mcp_multi_agent_context_hook_parity` | Rust-executable/C++-static gate for Codex/Claude/Cursor/generic agent context hook payload extraction, profile routing, session indexing, source-kind mapping, and role mapping. |
| `storage_cache_refill` | Rust invalidates cache, warms from page-store refs, and verifies memory refill stats. |
| `storage_cache_replacement_policy_soak` | Rust-native cache soak verifies access-refreshed hot blocks and pinned blocks survive capacity churn, cold blocks refill from disk, and latency samples are recorded. |
| `storage_shared_store_sync_replay` | Rust replays the C++ migration storage corpus through sync local shared-store replication. |
| `storage_shared_store_async_replay` | Rust replays the C++ migration storage corpus through async local shared-store replication. |
| `storage_object_manager_cold_hot_reload` | Rust verifies cold/hot object reload through the native slot/object/page index after memory eviction and restart. |
| `storage_object_manager_slotstore_runtime_authority` | Rust verifies named ObjectManager/SlotStore runtime authority modules over the native slot/object/page index. |
| `storage_page_address_disk_cache_shared_store_fallback` | Rust verifies PageAddress-driven disk cache and persistent page-store fallback after memory eviction. |
| `storage_tombstone_compaction` | Rust verifies tombstoned object reporting plus model-layout tombstone-density and rewrite-action evidence. |
| `storage_stale_page_density_compaction` | Rust verifies stale page estimate, density evidence, cold-page rewrite, object-page packing, and rewritten index refs for compaction decisions. |
| `storage_merged_dump_load_restart_interruption` | Rust verifies merged dump/load restart interruption markers, merged-installer roll-forward, load-version handoff after retry, and incomplete-commit reporting. |
| `storage_gc_eviction_cold_reads` | Rust verifies StorageManager GC plus eviction preserves cold reads through page-store fallback. |
| `storage_manager_continuous_background_runtime` | Rust verifies stoppable continuous StorageManager runtime with jitter, backoff, pause/resume, per-phase enable flags, and bounded work per round. |
| `storage_manager_real_pressure_signals` | Rust verifies StorageManagerPressureSnapshot is captured and cycle pressure is driven by actual dirty slots, WAL/index-log bytes, stale page density, cache pressure, expiry scan debt, delayed-destroy backlog, follower retention blockers, and model-layout compaction debt. |
| `storage_manager_wal_reclaim_slot_generation_retention` | Rust verifies WAL/index-log reclaim is slot-generation based and waits for durable dumps plus follower cursor and Raft snapshot retention frontiers. |
| `storage_manager_expire_cursor_scan_limits` | Rust verifies expire scanning uses hot/cold cursors, per-round limits, load-on-expire only when needed, and scanned/expired/skipped/loaded metrics. |
| `storage_manager_active_eviction_runtime` | Rust verifies active weighted slot/object eviction with pressure gates, batch limits, dump-before-evict, delete/drop mode, and cooldown reporting. |
| `storage_manager_page_gc_dependency_refusal` | Rust verifies page GC refuses reclaim when live refs, slot dump manifests, shared-store replay cursors, checkpoint/snapshot floors, Raft install floors, or delayed-destroy grace still retain a page segment. |
| `storage_manager_index_gc_thresholds_recovery` | Rust verifies Index GC uses index-log byte thresholds, usage-ratio triggers, max entries per round, dirty-slot commit before truncation, and restart recovery after bounded truncation. |
| `storage_manager_metrics_admin_phase_reports` | Rust verifies the reusable StorageManager phase executor populates per-phase admin reports and Prometheus metrics for last run time, duration, selected slots, skipped reason, errors, reclaimed bytes, compacted pages, WAL/index-log floors, retention blockers, pressure before/after, and the last pressure snapshot. |
| `storage_risk_context_page_backed_parity` | Rust verifies Risk and Context writes are page-backed in the slot-first index and survive restart. |
| `storage_byteraft_dump_load_atomicity` | Storage dump/load atomicity, manifest install, restart, logical read verification, and bounded data-node StorageManager cycle execution with prepare/reclaim/expire/evict/page-reclaim/index-GC/compact pressure evidence. |
| `storage_byteraft_corruption_recovery_matrix` | Storage corruption/recovery matrix for page/index/WAL/manifest faults, checksum mismatch, partial manifests, missing segments, and stale sequence rejection. |
| `storage_byteraft_follower_cursor_gc` | Follower-cursor-aware GC blocks unsafe reclaim and keeps recovery clean. |
| `storage_byteraft_cache_refill_pressure` | Tiny-cache refill pressure validates page-store reads, memory refill, admission/eviction stats, refill failures, pinned handles, DRAM/PMEM/SSD placement, async writeback/backpressure gauges, latency buckets, per-stage StorageManager pressure reports, live-read safety after eviction plus page GC, and periodic StorageManager scheduler queue safety. |
| `storage_byteraft_shared_store_sync_replay` | Sync local shared-store replay preserves converted pages and WAL/index-log ordering. |
| `storage_byteraft_shared_store_async_replay` | Async local shared-store replay preserves converted pages and WAL/index-log ordering under delayed follower catch-up. |

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
| `cpp_storage_oplog_index_replay_parity_surfaces` | WAL, index-log, checkpoint/replay surfaces. |
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
| `raft_byteraft_read_safety_policy` | ByteRaft-derived read-index, lease-read, bounded-stale, and secondary-read eligibility behavior. |
| `raft_byteraft_metrics_admin_pipeline_status` | ByteRaft-derived status/local-status/Prometheus capability matrix, peer pipeline, apply health, read-index, leader-transfer request/accept/reject/complete/elapsed/timeout counters, snapshot send elapsed/timeout counters, offline timeout state/rejection counters, and `/raft/control/byteraft_runtime_admin` evidence. |
| `server_raft_byteraft_runtime_admin_route` | Shared route and metrics contract for the ByteRaft-style runtime admin report, including capability matrix rows, on both standalone `raft_node` and raft-enabled `server`. |
| `raft_byteraft_snapshot_lifecycle_depth` | ByteRaft-derived snapshot trigger policy, sender timeout/retry, chunked install, stale/corrupt rejection, progress, restart recovery, and rollback reporting. |
| `raft_byteraft_replication_backpressure` | ByteRaft-derived oversized-log, in-flight append, backpressure, reorder, and apply-batch behavior. |
| `raft_byteraft_election_controls` | ByteRaft-derived pre-vote, election prohibition, transfer timeout, and offline peer controls. |
| `raft_byteraft_packet_loss_fault_harness` | Packet-loss/partition-heal fault scenario: majority continues and healed followers catch up. |
| `raft_byteraft_slow_wal_fsync_fault_harness` | Slow WAL fsync/backpressure scenario: committed writes survive and pressure is reported. |
| `raft_byteraft_snapshot_during_membership_fault_harness` | Snapshot during membership change preserves snapshot floor, membership generation, and restart recovery. |
| `raft_byteraft_leader_transfer_high_write_fault_harness` | Leader transfer under high write load has no lost or duplicate committed writes. |
| `raft_byteraft_follower_rejoin_compacted_logs_fault_harness` | Follower rejoin after compaction installs snapshot, replays retained tail, and becomes read-eligible after catch-up. |
| `raft_byteraft_rolling_restart_joint_consensus_fault_harness` | Rolling restart with pending joint consensus completes or rolls back safely. |
| `raft_byteraft_shared_fault_gate` | Combined ByteRaft-derived data-node and metaserver Raft fault gate. |
| `cpp_redis_live_storage_smoke_parity_surfaces` | Redis live storage smoke surfaces. |
| `cpp_local_docker_replication_matrix_parity_surfaces` | Local Docker replication matrix surfaces. |
| `cpp_client_meta_sync_route_parity_surfaces` | Client meta-sync, route, pipeline, and request surfaces. |
| `cpp_proxy_serving_admission_parity_surfaces` | Proxy serving, heartbeat, config, HA calibration, and smoke surfaces. |
| `cpp_metaserver_scheduler_repair_parity_surfaces` | Metaserver scheduler, repair, placement, heartbeat, and retry surfaces. |
| `cpp_data_node_lifecycle_server_parity_surfaces` | Data-node lifecycle, heartbeat, server, and metaserver client surfaces. |
| `control_topology_version_change` | Rust-executable/C++-static gate for the shared client/proxy/meta topology-version change workflow. |
| `control_stale_route_invalidation` | Rust-executable/C++-static gate for stale route invalidation and one-refresh retry behavior. |
| `control_proxy_admission_policy` | Rust-executable/C++-static gate for proxy admission, drop-percent, and degraded preflight behavior. |
| `control_proxy_operational_surface_aliases` | Rust-executable/C++-static gate for C++ proxy admin/config/heartbeat/status operational aliases over Rust-native routes. |
| `control_readonly_write_disabled_tables` | Rust-executable/C++-static gate for readonly/write-disabled/not-serving table policy behavior. |
| `control_route_quarantine_recovery` | Rust-executable/C++-static gate for backend quarantine, recovery probing, and degraded preflight behavior. |
| `control_data_node_load_reload_unload_lifecycle` | Rust-executable/C++-static gate for data-node load/reload/readonly/unload lifecycle behavior. |
| `control_cpp_server_service_alias_surface` | Rust-executable/C++-static gate for data-node C++ `ServerService` alias routes over the Rust-native migration surface. |
| `control_metaserver_scheduler_lifecycle_workflow` | Rust-executable/C++-static gate for metaserver scheduler-issued load/reload/unload token behavior. |
| `control_multi_proxy_topology_churn_scale` | Rust-executable/C++-static scale gate for two proxies converging after metaserver topology churn and stale cached route recovery. |
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
| `context_extracted_event_default_index_fanout` | Rust-executable/C++-static gate translated from C++ `WRITE_EXTRACTED_EVENT` debug tests; default internal indexes fan out and disabled indexes do not return refs. |
| `context_tree_embedding_summary_compression` | Rust-executable/C++-static gate translated from C++ tree/embedding/summary/compression round-trip behavior. |
| `context_temporal_compression_replayable_summary` | Rust-executable/C++-static gate translated from C++ temporal compression behavior; compression must not delete source events. |
| `context_events_segments_entities_child_refs` | Rust-executable/C++-static gate for ContextEvent, ContextSegment, ContextEntity, child refs, extracted-event fanout, node-context query behavior, timestamp-keyed context events under node/segment parents, compact embedding model hashes, and compact scope hot-record parity. |
| `context_cpp_wire_model_descriptor_roundtrip` | Rust-executable/C++-static gate for Context model IDs, key families, aliases, and C++ JSON wire payload round trips across Context models. |
| `context_embeddings_summaries_l0_l1_pipeline` | Rust-executable/C++-static gate for embeddings, L0/L1 summaries, summary dirty tracking, provider/model selection, and prompt block construction. |
| `context_compression_secondary_index_query_debug_flow` | Rust-executable/C++-static gate for temporal compression, secondary-index filter groups, C++-style query debug flow, retrieved evidence ordering, and audit refs. |
| `context_resource_skill_parser_openviking_parity` | Rust-executable/C++-static gate for OpenViking-style resource chunking, stable source refs, heading paths, line ranges, linked refs, code-language metadata, `SKILL.md` front matter, version/owner scope, allowed tools, triggers, model refs, tag/capability/tool/instruction/resource/example refs, chunk embeddings, and Rust TemporalStore ingestion/retrieval of parsed chunks. |
| `context_resource_lifecycle_openviking_parity` | Rust-executable/C++-static gate for OpenViking-style resource add/watch/refresh/delete lifecycle behavior across URL, Git, PDF/document, and Feishu-style imports, including parser provenance, owner scope, version invalidation, watch scheduling, and delete markers. |
| `context_resource_skill_registry_openviking_parity` | Rust-executable/C++-static gate for OpenViking-style skill registry behavior: enable/disable, precedence, owner scope, triggers, allowed tools, version updates, and retrieval-time skill selection. |
| `context_resource_skill_live_embedding_summary_retrieval` | Rust-executable/C++-static gate for live OpenAI-compatible embedding generation without mock fallback and summary-embedding-driven retrieval expansion into resource evidence. |
| `context_benchmark_fixture_gates` | Shared C++/Rust benchmark contract for LOCOMO-style and LongMemEval_s fixture gates using MatrixArk/VikingMem report fields. |
| `context_benchmark_full_dataset_gates` | Shared C++/Rust benchmark contract for LOCOMO, LongMemEval_s, and Docker/open-model full-dataset gates with explicit threshold profiles and archive reports. |
| `control_multi_proxy_convergence_and_quarantine` | Rust-executable/C++-static gate for multi-proxy convergence, backend quarantine, recovery probing, and stale-cache comparison. |
| `control_scheduler_token_stale_rejection` | Rust-executable/C++-static gate for metaserver scheduler-issued lifecycle tokens and stale generation rejection. |
| `control_datanode_lifecycle_restart_recovery` | Rust-executable/C++-static gate for data-node lifecycle, snapshot restore, and restart diagnostics. |
| `control_client_cpp_partition_set_route_cache` | Rust-executable/C++-static gate for direct SDK C++ partition-set/member/version route-cache hierarchy. |
| `control_client_retry_budget_topology_refresh` | Rust-executable/C++-static gate for client retry budgets, topology refresh, stale route invalidation, and no duplicate unsafe writes. |
| `control_client_metasync_outage_churn_stress` | Rust-executable/C++-static gate for MetaSyncer jitter/backoff/deadline behavior under metaserver outage and topology churn. |
| `control_client_pipeline_batch_partial_timeout_contract` | Rust-executable/C++-static gate for ordered batching, partial failures, retry-safe versus unsafe writes, and timeout budget propagation. |
| `control_client_deployment_placement_routing_hooks` | Rust-executable/C++-static gate for deployment placement hooks, location-affine secondary reads, and primary-only write routing. |
| `cross_storage_control_agent_parity` | Rust-executable/C++-static gate tying storage dump/load/cache recovery, client/proxy topology/admission, data-node lifecycle, metaserver scheduler tokens, and Context agent resource/skill parser workflow evidence into one cross-subsystem contract. |
| `ingestion_kafka_consumer_group_runtime_rebalance` | Rust-executable/C++-static gate for Kafka consumer-group runtime assignment, rebalance-required detection, and backpressure. |
| `ingestion_flink_checkpoint_restart_failover` | Rust-executable/C++-static gate for Flink checkpoint lifecycle across restart/failover idempotence. |
| `ingestion_dead_letter_lag_report_contract` | Rust-executable/C++-static gate for dead-letter export, lag metrics, committed offsets, and valid-record continuation. |
| `ops_scale_readiness_slo_gate` | Rust-executable/C++-static gate for readiness filtering, external chaos plan, rolling restart, Docker/AWS SLO evidence, and scale workload replay. |
| `ops_grafana_metrics_cpp_parity` | Rust-executable/C++-static gate for Grafana dashboard, alert, and Prometheus metric-family parity across readiness, Raft, metaserver scheduler, proxy/client, storage/cache, data-node, ingestion, secondary replication, and scale SLO evidence. |
| `benchmark_locomo_rust_full_replay_contract` | Shared benchmark contract for LOCOMO full Rust TemporalStore replay, deterministic/OSS reader modes, and VikingMem-style archive fields. |
| `benchmark_longmemeval_rust_full_replay_contract` | Shared benchmark contract for LongMemEval_s full Rust TemporalStore replay, deterministic/OSS reader modes, and VikingMem-style archive fields. |
| `benchmark_cpp_rust_vikingmem_report_comparator` | Shared benchmark contract for comparing C++ and Rust `matrixark_vikingmem_context_benchmark_report_v1` archives case-by-case. |

### ByteRaft Fault Acceptance Criteria

The ByteRaft-derived fault cases carry machine-validated
`acceptance_criteria` in the shared corpus:

| Scenario | Acceptance |
| --- | --- |
| Packet loss / partition | Majority continues committing; minority rejects stale reads/writes; healed peer catches up before read eligibility. |
| Slow WAL fsync | Backpressure activates; no committed write is lost; lag and latency counters show WAL pressure. |
| Snapshot during membership change | Snapshot floor and membership generation remain consistent; restart preserves both. |
| Leader transfer under high write load | Writes commit exactly once or fail safely; no committed write is lost or duplicated; final leader has all committed entries. |
| Follower rejoin with compacted logs | Follower installs snapshot, replays retained tail, and becomes read-eligible only after catch-up. |
| Rolling restart with pending joint consensus | Joint state survives restart and either completes safely or rolls back safely without losing membership state. |

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

## C++ Adapter Coverage And Comparison Output

The shared corpus now carries `coverage.cpp_adapter_coverage` so every migrated
family has a C++ execution story:

| Family | Status |
| --- | --- |
| Storage/cache | Temporary static surface gate with a native runner blocker. |
| Raft | Mixed native runner plus static surface gate for legacy ByteRaft/C++ surfaces. |
| Context | Temporary static surface gate with a native runner blocker. |
| Client/proxy/control-plane | Temporary static surface gate with a native runner blocker. |
| Ingestion | Temporary static surface gate with a native runner blocker. |
| Benchmarks | Native adapter contract through `cpp_context_benchmark_report_adapter.h`. |
| Codex/MCP | Temporary static surface gate with a native runner blocker. |

When a native C++ runner is not available, the corpus keeps the case as a
temporary static surface gate and records the blocker plus the expected runner
command. The validator fails if a new C++ suite is added without adapter
coverage, or if a static gate lacks a blocker.

Generic shared-case report comparisons use
`tools/compare_unified_cpp_rust_case_reports.py`. Its JSON output contains
`rust_only_misses`, `cpp_only_misses`, `shared_hard_failures`, `output_diffs`,
and `latency_deltas`; benchmark-specific archives continue to use the dedicated
MatrixArk/VikingMem comparators.

## Are There Still Rust-Specific Tests?

Yes. Current Rust-local attributed test count is:

```text
Rust attributed tests: 565
shared-corpus marked Rust tests: 78
shared corpus cases: 169
shared corpus steps: 331
C++ existing-test surfaces: 185
```

The detailed reduction split and new-test guard live in
`docs/rust_product_test_reduction_guard.md`. The current split is:

```text
product behavior to move into shared corpus: 533
Rust-only internals that can remain local: 7
existing Rust tests already marked with shared-corpus references: 41
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
