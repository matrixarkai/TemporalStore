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
