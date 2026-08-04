# Rust Test Suite — Known Failures & Root-Cause Triage

Last updated: 2026-08-04

Context: for a long period the `temporalstore-rust` workspace did **not compile**
under `cargo build --workspace --all-targets` (test/bin/example code had drifted
from current types). Because the suite could not build, its runtime failures
were invisible and a backlog of real regressions accumulated undetected.

Two fixes have restored visibility and cleared the largest cluster:

- **Build fix** — `cargo build --workspace --all-targets` compiles again
  (drifted test/bin/example code repaired; no library behavior changed).
  Result: **495 tests pass**.
- **`slot_storage_summaries` page-segment fix** — the function declared
  `page_segments_by_slot` but never populated it, so every slot's
  `page_segment_ids` was empty and slot-dump manifests failed validation with
  `slot_dump_page_segment_mismatch`. Populating it from live entries fixed
  **10 tests**. Result: **505 pass / 26 fail, no regressions**.

The remaining 26 are **genuine behavioral failures**, not identifier drift.
Editing test expectations to accept current output would mask real bugs, so they
are documented here for the owning subsystems to fix. Run to reproduce:

```bash
cargo test --workspace -- --test-threads=1
```

## Remaining failures by root-cause cluster

### 1. Memory-cache disk→memory promotion (~5)

Tests: `engine::tests::cache_replacement_policy_soak`,
`cold_index_page_address_reads_from_disk_cache_or_block_store_and_refills_memory`,
`restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache`,
`tiny_memory_cache_eviction_refills_from_persistence_then_block_cache`,
`slot_page_ownership_is_first_class_and_survives_reload`.

Symptom: after a disk-cache hit, `cache.get_memory(key)` is `None` — the block
is not promoted back into the memory tier.

Root cause (established by instrumenting `tiny_memory_cache_eviction`): on an
SSD/disk hit, matrixcache promotes via `get_with_tier` → `refill_from_ssd` →
`put_memory`, and `put_memory` **rejects any block whose `value.len()` exceeds
`memory_capacity_bytes`** (`memory_admission_rejected` / `eviction_oversize`).
These tests use a deliberately tiny cache (e.g. `with_local_dirs(32, ..)` = 32
bytes), but the target **page block** (page structure + overhead, not the
~23-byte string) is larger than 32 bytes, so the refill is rejected and the
block is never promoted. The `get_memory(..) == None` "already evicted"
precondition passes *vacuously* — that page block never fit in 32 bytes to
begin with. Instrumented stats at the failing point:
`disk_hits: 1, memory_admission_rejected: 4, eviction_oversize: 4,
refill_failures: 4, memory_bytes: 23, disk_bytes: 155`.

Ruled out (empirically): promotion is **not** gated by `memory_hotness_threshold`
— setting it to 1 in the engine's tiering policy did not fix any of the 5 tests.

Fix requires an owner decision, not a blind patch: either the test capacities are
too small for a page block (raise them — but that changes the eviction dynamics
the tests exercise), or the page-block cache representation grew and that size
growth is the regression to chase. Do **not** simply bump capacities to force
green.

### 2. Slot-dump merged-install source-manifest coverage (~1)

Test: `engine::tests::storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc`.

Symptom: merged-install preflight reports `missing_source_manifests` /
`source_manifest_slot_coverage` for a source manifest that was just created.

Diagnosis: `slot_dump_source_manifest_coverage` (engine.rs) re-reads the source
via `slot_dump_manifest_at` and accepts it only if
`slot_dump_manifest_checksum(&source) == source.checksum`. The just-created
manifest is being rejected — investigate whether the persisted checksum matches a
recomputation over the current manifest shape (checksum drift / persistence).

### 3. Slot-dump object-lifecycle rejection not firing (~1)

Test: `engine::tests::slot_dump_manifest_rejects_object_lifecycle_mismatch`.

Symptom: `unwrap_err()` on an `Ok` — validation that should reject an
object-lifecycle-mismatched manifest now accepts it. Object-lifecycle mismatch
detection in `validate_slot_dump_manifest` is not triggering for the mutated
manifest.

### 4. Packed-timestamped-page counting (~1)

Test: `engine::tests::object_manager_runtime_report_tracks_residency_layout_and_tombstones_cpp_parity`.

Symptom: `report.packed_timestamped_page_count >= 1` fails (count is 0). The
counter is incremented conditionally in `product_model.rs`; the packing-detection
condition is never satisfied for timestamped pages.

### 5. Storage metrics / GC / recovery (~4)

Tests: `prometheus_metrics_include_records_cache_page_and_wal`
(sealed `block_store_extent_count` is 0, expected 1),
`recovery_validates_all_timestamped_kv_page_families`,
`slot_storage_summaries_track_live_refs_dirty_slots_and_manifest_sequence`,
`storage_page_gc_blocks_all_retention_dependencies_before_reclaim`.

Diagnosis: likely tied to the page-segment / extent sealing lifecycle; audit
extent-sealing and live-ref accounting after the page-segment fix.

### 6. Raft (5)

Tests: `matrixraft_read_safety_fault_matrix_records_partition_and_catchup_evidence`
(`stale_leader_lease_rejected > 0`),
`metaserver_raft_apply_health_reports_commit_to_apply_lag` and
`raft_apply_health_reports_commit_to_apply_lag` (apply-lag Prometheus strings),
`raft_election_controls_record_prohibition_offline_and_transfer_timeouts`,
`raft_leader_lease_expiry_blocks_linearizable_reads_and_writes_until_heartbeat`.

Mixed: some assert exact Prometheus metric strings (behavioral/format drift);
the lease/election ones may be **timing-sensitive** and should be checked for
flakiness under load before treating as hard failures.

### 7. Context workflow / C++ model parity (5)

Tests: `context_workflow::tests::context_benchmark_injection_uses_entity_segment_l0_l1_and_secondary_index`,
`context_get_nodes_batches_summary_lookup_for_retrieval`,
`context_workflow_extracts_retrieves_and_injects_mock_context`,
`engine::tests::context_models_match_cpp_keys_timeline_pages_and_filters`,
`types::tests::context_models_round_trip_cpp_wire_payloads_and_type_alias`.

Retrieval tiering / node-ref / cpp wire round-trip mismatches.

### 8. Misc (block_store / readiness / client / data_node)

Tests: `block_store::tests::page_address_matches_compact_segment_metadata_contract_and_checksum_alias`,
`readiness::tests::production_readiness_report_summarizes_requested_service_readiness`,
`readiness::tests::remaining_blockers_map_to_concrete_evidence_fields`,
`client::tests::table_typed_methods_and_pipeline_match_cpp_client_shape`,
`data_node::tests::storage_manager_runtime_supports_stop_pause_resume_jitter_backoff_and_phase_flags`.

## Recommendation

The `slot_storage_summaries` bug (a declared-but-unpopulated field consumed
downstream) is a useful pattern to audit for elsewhere — several of the storage
metric/recovery failures may share that shape. Now that the suite compiles,
add a CI gate on `cargo build --workspace --all-targets` (and, once green, on
`cargo test`) so this class of regression cannot silently reaccumulate.
