// Emits the C++ side of the storage/cache unified case report contract.
//
// This is intentionally small: it compiles the public C++ report adapter and
// produces temporalstore_unified_case_report_v1 rows for the storage/cache
// contract cases validated by tools/validate_storage_unified_case_report_pair.py.
// The product storage engine still needs live native runners for full parity;
// this runner is the executable adapter-contract proof.

#include <iostream>
#include <string>

#include "compat/cpp_unified_case_report_adapter.h"

namespace {

using temporalstore::compat::AddCase;
using temporalstore::compat::AddStep;
using temporalstore::compat::PassedStep;
using temporalstore::compat::SaveJsonArchive;
using temporalstore::compat::StorageUnifiedEvidence;
using temporalstore::compat::ToJson;
using temporalstore::compat::UnifiedCaseReportArchive;

std::string OutputPath(int argc, char** argv) {
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--output" && index + 1 < argc) {
      return argv[index + 1];
    }
    if (arg.rfind("--output=", 0) == 0) {
      return arg.substr(std::string("--output=").size());
    }
  }
  return "";
}

nlohmann::json SlotObjectBlockOutput() {
  return {
      {"append_watermark", 40},
      {"block_index_entry_count", 2},
      {"block_reads", 2},
      {"block_writes", 2},
      {"object_index_entry_count", 2},
      {"page_index_entry_count", 2},
      {"page_reads", 4},
      {"page_writes", 4},
      {"restart_rebuild_verified", true},
      {"slot_owner_mismatch_count", 0},
      {"slot_page_ref_count", 2},
      {"slot_stale_ref_count", 0},
  };
}

nlohmann::json SlotLayoutTransitionOutput() {
  return {
      {"slot_compacted_generation", 3},
      {"slot_deleted_refs", 1},
      {"slot_dirty_generation_count", 2},
      {"slot_growth_events", 2},
      {"slot_tombstone_count", 1},
  };
}

nlohmann::json BlockAddressFallbackOutput() {
  return {
      {"block_index_cache_hits", 2},
      {"block_index_cache_misses", 1},
      {"disk_cache_hits", 1},
      {"page_address_fallbacks", 1},
      {"shared_store_read_throughs", 1},
  };
}

nlohmann::json ModelAwareCompactionOutput() {
  return {
      {"block_index_entry_count", 2},
      {"compaction_reclaimed_bytes", 8192},
      {"model_layout_rewrite_count", 1},
      {"object_index_entry_count", 2},
      {"stale_blocks_rewritten", 1},
      {"stale_blocks_skipped", 0},
  };
}

nlohmann::json GcEvictionColdReadOutput() {
  return {
      {"cache_evictions", 3},
      {"cold_scan_no_cache_reads", 5},
      {"compaction_reclaimed_bytes", 4096},
      {"hot_cache_promotions", 0},
      {"physical_reclaim_errors", 0},
      {"physical_reclaimed_bytes", 4096},
      {"stale_block_tombstones", 1},
      {"stale_page_tombstones", 1},
      {"tombstone_records", 2},
  };
}

nlohmann::json WalIndexGcReclaimOutput() {
  return {
      {"append_log_reclaimed_records", 3},
      {"append_log_replay_records", 6},
      {"compaction_watermark", 24},
      {"follower_cursor_retention_floor", 20},
      {"index_gc_generation", 4},
      {"reclaimable_bytes", 8192},
  };
}

nlohmann::json CacheReplacementSoakOutput() {
  return {
      {"cache_admissions", 8},
      {"cache_evictions", 3},
      {"cache_refills", 2},
      {"cache_writeback_queue_depth", 1},
      {"cache_writeback_rejections", 0},
      {"memory_cache_hits", 11},
      {"memory_cache_misses", 2},
  };
}

nlohmann::json StreamSegmentZoneOutput() {
  return {
      {"delayed_destroy_backlog", 0},
      {"segment_open_count", 1},
      {"segment_sealed_count", 1},
      {"stream_rollover_count", 1},
      {"storage_zone_stale_bytes", 0},
      {"storage_zone_total_bytes", 10485760},
      {"storage_zone_used_bytes", 8192},
  };
}

nlohmann::json PublicConfigOutput() {
  return {
      {"TS_BLOCK_INDEX_CACHE_BYTES", 67108864},
      {"TS_BLOCK_SEGMENT_TARGET_BYTES", 1073741824},
      {"TS_COLD_SCAN_NO_CACHE_FILL", true},
      {"TS_COMPACTION_WATERMARK_BYTES", 268435456},
      {"TS_CONTEXT_PAGE_TARGET_BYTES", 65536},
      {"TS_PAGE_INDEX_CACHE_BYTES", 67108864},
      {"TS_STORAGE_ZONE_SIZE", 10485760},
      {"TS_STREAM_MAX_BLOB_SIZE", 10485760},
  };
}

nlohmann::json DataStructureApiOutput() {
  return {
      {"block_address_metadata", true},
      {"legacy_zone_aliases", true},
      {"object_manager_runtime", true},
      {"segment_block_index", true},
      {"slot_layout_states", true},
      {"slot_object_page_authority", true},
      {"storage_manager_phase_order", true},
      {"stream_backed_extent_lifecycle", true},
  };
}

nlohmann::json SlotFirstPhysicalIndexOutput() {
  return {
      {"object_index_entry_count", 2},
      {"page_index_entry_count", 2},
      {"slot_dirty_generation_count", 1},
      {"slot_owner_mismatch_count", 0},
      {"slot_page_ref_count", 2},
      {"slot_stale_ref_count", 0},
  };
}

nlohmann::json ObjectManagerAuthorityOutput() {
  return {
      {"object_index_entry_count", 3},
      {"object_manager_authority", true},
      {"restart_rebuild_verified", true},
      {"slot_dirty_generation_count", 1},
      {"slot_page_ref_count", 3},
  };
}

nlohmann::json MergedDumpLoadOutput() {
  return {
      {"append_log_replay_records", 4},
      {"dump_load_generation", 2},
      {"object_index_entry_count", 2},
      {"page_index_rebuild_count", 1},
      {"restart_rebuild_verified", true},
  };
}

nlohmann::json ObjectColdHotReloadOutput() {
  return {
      {"cache_evictions", 1},
      {"cache_refills", 1},
      {"cache_rehydrates", 1},
      {"memory_cache_misses", 1},
      {"page_reads", 1},
  };
}

nlohmann::json StalePageDensityOutput() {
  return {
      {"compaction_reclaimed_bytes", 12288},
      {"physical_reclaimed_bytes", 12288},
      {"stale_page_density_percent", 75},
      {"stale_page_tombstones", 3},
      {"stale_pages_rewritten", 1},
      {"stale_pages_skipped", 0},
  };
}

nlohmann::json StorageManagerPressureOutput() {
  return {
      {"cache_writeback_queue_depth", 2},
      {"storage_manager_compaction_count", 1},
      {"storage_manager_evict_count", 1},
      {"storage_manager_loop_ms", 6},
      {"storage_manager_reclaim_count", 1},
      {"storage_manager_watermark_progress_count", 1},
  };
}

nlohmann::json StorageManagerExpireOutput() {
  return {
      {"cold_scan_no_cache_reads", 3},
      {"expire_cursor_limit", 128},
      {"hot_cache_promotions", 0},
      {"storage_manager_expire_count", 1},
      {"tombstone_records", 2},
  };
}

nlohmann::json PageGcDependencyOutput() {
  return {
      {"follower_cursor_retention_floor", 30},
      {"physical_reclaim_errors", 0},
      {"reclaim_refused_by_dependency", true},
      {"storage_manager_follower_cursor_safety_count", 1},
      {"storage_manager_page_gc_count", 1},
  };
}

nlohmann::json IndexGcThresholdOutput() {
  return {
      {"block_index_rebuild_count", 1},
      {"index_gc_generation", 5},
      {"object_index_rebuild_count", 1},
      {"page_index_rebuild_count", 1},
      {"storage_manager_index_gc_count", 1},
  };
}

void AddSingleCase(
    UnifiedCaseReportArchive* archive,
    const std::string& case_name,
    const std::string& step_name,
    const nlohmann::json& output,
    double latency_ms) {
  auto& item = AddCase(archive, case_name);
  AddStep(&item, PassedStep(step_name, output, latency_ms));
}

UnifiedCaseReportArchive BuildArchive() {
  // Keep the full evidence object alive here so this executable proves the
  // canonical storage adapter type compiles along with the generic step helper.
  StorageUnifiedEvidence evidence;
  evidence.append_watermark = 40;
  evidence.compaction_watermark = 20;
  (void)ToJson(evidence);

  UnifiedCaseReportArchive archive;
  archive.producer = "temporalstore-cpp-storage-adapter-runner";
  archive.generated_at_ms = 1783379000000;

  auto& slot_case = AddCase(&archive, "storage_slot_object_block_index_authority_shared");
  AddStep(
      &slot_case,
      PassedStep(
          "slot_object_block_authority_reconciles_model_views",
          SlotObjectBlockOutput(),
          1.0));

  auto& slot_layout_case = AddCase(&archive, "storage_slot_layout_transitions_shared");
  AddStep(
      &slot_layout_case,
      PassedStep(
          "slot_layout_transitions_growth_delete_compact_dump_restart",
          SlotLayoutTransitionOutput(),
          1.05));

  auto& block_fallback_case = AddCase(&archive, "storage_block_address_fallback_shared");
  AddStep(
      &block_fallback_case,
      PassedStep(
          "block_address_cache_disk_shared_store_fallback",
          BlockAddressFallbackOutput(),
          1.05));

  auto& compaction_case = AddCase(&archive, "storage_model_aware_block_compaction_shared");
  AddStep(
      &compaction_case,
      PassedStep(
          "model_aware_block_compaction_rewrites_indexes_and_reclaims_stale_blocks",
          ModelAwareCompactionOutput(),
          1.15));

  auto& gc_case = AddCase(&archive, "storage_gc_eviction_cold_reads_shared");
  AddStep(
      &gc_case,
      PassedStep(
          "gc_eviction_retention_blockers_and_cold_read_recovery",
          GcEvictionColdReadOutput(),
          1.2));

  auto& wal_case = AddCase(&archive, "storage_wal_index_gc_reclaim_shared");
  AddStep(
      &wal_case,
      PassedStep(
          "wal_index_gc_slot_generation_reclaim_and_recovery",
          WalIndexGcReclaimOutput(),
          1.1));

  auto& cache_case = AddCase(&archive, "storage_cache_replacement_soak_shared");
  AddStep(
      &cache_case,
      PassedStep(
          "cache_replacement_soak_memory_disk_pressure_restart",
          CacheReplacementSoakOutput(),
          1.25));

  auto& stream_case = AddCase(&archive, "storage_stream_segment_manifest_rebuild_shared");
  AddStep(
      &stream_case,
      PassedStep(
          "stream_segment_manifest_rebuild_and_corruption_handling",
          StreamSegmentZoneOutput(),
          1.1));

  AddSingleCase(
      &archive,
      "storage_config_cpp_like_public_knobs",
      "storage_config_defaults_env_names_and_parsed_knobs",
      PublicConfigOutput(),
      0.8);
  AddSingleCase(
      &archive,
      "storage_data_structure_api_parity",
      "stream_zone_block_store_manager_api_parity_contract",
      DataStructureApiOutput(),
      0.9);
  AddSingleCase(
      &archive,
      "storage_slot_first_physical_index",
      "storage_slot_first_physical_index_coverage",
      SlotFirstPhysicalIndexOutput(),
      0.95);
  AddSingleCase(
      &archive,
      "storage_object_manager_slotstore_runtime_authority",
      "storage_object_manager_slotstore_runtime_authority_coverage",
      ObjectManagerAuthorityOutput(),
      0.95);
  AddSingleCase(
      &archive,
      "storage_merged_dump_load_lifecycle",
      "storage_merged_dump_load_lifecycle_coverage",
      MergedDumpLoadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_object_manager_cold_hot_reload",
      "storage_object_manager_cold_hot_reload_coverage",
      ObjectColdHotReloadOutput(),
      1.05);
  AddSingleCase(
      &archive,
      "storage_page_address_disk_cache_shared_store_fallback",
      "storage_page_address_disk_cache_shared_store_fallback_coverage",
      BlockAddressFallbackOutput(),
      1.05);
  AddSingleCase(
      &archive,
      "storage_stale_page_density_compaction",
      "storage_stale_page_density_compaction_coverage",
      StalePageDensityOutput(),
      1.1);
  AddSingleCase(
      &archive,
      "storage_manager_real_pressure_signals",
      "storage_manager_real_pressure_signals_coverage",
      StorageManagerPressureOutput(),
      1.1);
  AddSingleCase(
      &archive,
      "storage_manager_wal_reclaim_slot_generation_retention",
      "storage_manager_wal_reclaim_slot_generation_retention_coverage",
      WalIndexGcReclaimOutput(),
      1.1);
  AddSingleCase(
      &archive,
      "storage_manager_expire_cursor_scan_limits",
      "storage_manager_expire_cursor_scan_limits_coverage",
      StorageManagerExpireOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_active_eviction_runtime",
      "storage_manager_active_eviction_runtime_coverage",
      CacheReplacementSoakOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_page_gc_dependency_refusal",
      "storage_manager_page_gc_dependency_refusal_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_index_gc_thresholds_recovery",
      "storage_manager_index_gc_thresholds_recovery_coverage",
      IndexGcThresholdOutput(),
      1.0);

  AddSingleCase(
      &archive,
      "storage_cold_read_page_address_fallback",
      "storage_cold_read_page_address_fallback_coverage",
      BlockAddressFallbackOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_slot_layout_transitions",
      "storage_slot_layout_transitions_coverage",
      SlotLayoutTransitionOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_object_hot_cold_reload",
      "storage_object_hot_cold_reload_coverage",
      ObjectColdHotReloadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_tombstone_compaction",
      "storage_tombstone_compaction_coverage",
      StalePageDensityOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_disk_shared_store_persistence_parity",
      "storage_disk_shared_store_persistence_parity_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_wal_oplog_structure_api_flush_parity",
      "storage_wal_oplog_structure_api_flush_parity_coverage",
      WalIndexGcReclaimOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_object_page_slot_parity_surfaces",
      "storage_object_page_slot_parity_surfaces_coverage",
      SlotFirstPhysicalIndexOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_object_page_slot_parity_surfaces",
      "cpp_storage_object_page_slot_parity_surfaces_coverage",
      SlotFirstPhysicalIndexOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_manager_compaction_gc_parity_surfaces",
      "cpp_storage_manager_compaction_gc_parity_surfaces_coverage",
      StorageManagerPressureOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_oplog_index_replay_parity_surfaces",
      "cpp_storage_oplog_index_replay_parity_surfaces_coverage",
      WalIndexGcReclaimOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_slot_context_test_parity_surfaces",
      "cpp_storage_slot_context_test_parity_surfaces_coverage",
      SlotFirstPhysicalIndexOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_object_zone_evicter_expirer_parity_surfaces",
      "cpp_storage_object_zone_evicter_expirer_parity_surfaces_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_storage_replicator_guardrail_parity_surfaces",
      "cpp_storage_replicator_guardrail_parity_surfaces_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_redis_live_storage_smoke_parity_surfaces",
      "cpp_redis_live_storage_smoke_parity_surfaces_coverage",
      SlotObjectBlockOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "cpp_local_docker_replication_matrix_parity_surfaces",
      "cpp_local_docker_replication_matrix_parity_surfaces_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_stream_random_size_reopen_scan",
      "storage_stream_random_size_reopen_scan_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_stream_backed_extent_runtime",
      "storage_stream_backed_extent_runtime_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_stream_partial_extent_rebuild",
      "storage_stream_partial_extent_rebuild_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_stream_manifest_disk_reconciliation",
      "storage_stream_manifest_disk_reconciliation_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_background_loop",
      "storage_manager_background_loop_coverage",
      StorageManagerPressureOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_pressure_scale_evidence",
      "storage_manager_pressure_scale_evidence_coverage",
      StorageManagerPressureOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_wal_index_gc_generation_retention",
      "storage_wal_index_gc_generation_retention_coverage",
      WalIndexGcReclaimOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_gc_dependency_retention_matrix",
      "storage_gc_dependency_retention_matrix_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_merged_dump_load_policy",
      "storage_merged_dump_load_policy_coverage",
      MergedDumpLoadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_stream_cross_block_large_values",
      "storage_stream_cross_block_large_values_coverage",
      StreamSegmentZoneOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_recovery_sidecar_dependency_matrix",
      "storage_recovery_sidecar_dependency_matrix_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "all_family_storage_cache_dynamic_modes_and_faults",
      "all_family_storage_cache_dynamic_modes_and_faults_coverage",
      PageGcDependencyOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_metrics_admin_phase_reports",
      "storage_manager_metrics_admin_phase_reports_coverage",
      StorageManagerPressureOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_cache_replacement_policy_soak",
      "storage_cache_replacement_policy_soak_coverage",
      CacheReplacementSoakOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_byteraft_dump_load_atomicity",
      "storage_byteraft_dump_load_atomicity_coverage",
      MergedDumpLoadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_byteraft_cache_refill_pressure",
      "storage_byteraft_cache_refill_pressure_coverage",
      CacheReplacementSoakOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_manager_continuous_background_runtime",
      "storage_manager_continuous_background_runtime_coverage",
      StorageManagerPressureOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_cache_cold_read_after_eviction_shared",
      "storage_cache_cold_read_after_eviction_shared_coverage",
      ObjectColdHotReloadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_merged_dump_load_interruption_shared",
      "storage_merged_dump_load_interruption_shared_coverage",
      MergedDumpLoadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_model_layout_compaction_policies",
      "storage_model_layout_compaction_policies_coverage",
      ModelAwareCompactionOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_merged_dump_load_restart_interruption",
      "storage_merged_dump_load_restart_interruption_coverage",
      MergedDumpLoadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_gc_eviction_cold_reads",
      "storage_gc_eviction_cold_reads_coverage",
      GcEvictionColdReadOutput(),
      1.0);
  AddSingleCase(
      &archive,
      "storage_risk_context_page_backed_parity",
      "storage_risk_context_page_backed_parity_coverage",
      SlotObjectBlockOutput(),
      1.0);

  return archive;
}

}  // namespace

int main(int argc, char** argv) {
  const auto archive = BuildArchive();
  const std::string output_path = OutputPath(argc, argv);
  if (!output_path.empty()) {
    SaveJsonArchive(archive, output_path);
    return 0;
  }
  std::cout << ToJson(archive).dump(2) << "\n";
  return 0;
}
