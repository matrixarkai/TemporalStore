// Shared C++/Rust TemporalStore product-case report adapter contract.
//
// C++ family runners can use this small helper to emit the same
// temporalstore_unified_case_report_v1 shape consumed by
// tools/compare_unified_cpp_rust_case_reports.py.  It is intentionally
// product-agnostic so Redis/admin can move first, then storage/cache, control
// plane, ingestion, and context can reuse the same output contract.
//
// Dependency: nlohmann/json.

#pragma once

#include <cstdint>
#include <chrono>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace temporalstore::compat {

struct UnifiedCaseStepReport {
  std::string name;
  std::string status = "passed";
  nlohmann::json output = nullptr;
  double latency_ms = 0.0;
};

struct UnifiedCaseReport {
  std::string name;
  std::string status = "passed";
  std::vector<UnifiedCaseStepReport> steps;
};

struct UnifiedCaseReportArchive {
  std::string schema = "temporalstore_unified_case_report_v1";
  std::string producer = "temporalstore-cpp";
  std::int64_t generated_at_ms = 0;
  std::vector<UnifiedCaseReport> cases;
};

struct ByteRaftUnifiedEvidence {
  bool wal_segment_lifecycle_present = false;
  std::uint64_t wal_first_log_index = 0;
  std::uint64_t wal_last_log_index = 0;
  std::uint64_t wal_segment_release_floor = 0;
  bool read_index_validated = false;
  bool stale_leader_lease_rejected = false;
  bool lagging_follower_read_rejected = false;
  bool stale_follower_write_rejected = false;
  bool bounded_stale_reads_under_partition = false;
  bool minority_partition_rejected = false;
  bool healed_follower_caught_up = false;
  bool inflight_limit_enforced = false;
  std::uint64_t inflight_append_limit = 0;
  std::uint64_t inflight_append_count = 0;
  std::uint64_t inflight_bytes = 0;
  std::uint64_t max_memory_replicate_bytes = 0;
  std::uint64_t append_queue_depth = 0;
  std::uint64_t apply_queue_depth = 0;
  bool oversized_log_rejected = false;
  bool reorder_gap_recovered = false;
  std::uint64_t reorder_queue_depth = 0;
  std::uint64_t reorder_timeout_drops = 0;
  bool stale_term_rejected = false;
  bool snapshot_chunk_retry_present = false;
  bool snapshot_send_timeout_present = false;
  bool snapshot_rate_limit_present = false;
  std::uint64_t snapshot_install_progress_per_mille = 0;
  bool snapshot_install_rollback_present = false;
  bool snapshot_during_membership_change_present = false;
  bool follower_rejoin_after_compacted_log_present = false;
  bool learner_add_present = false;
  bool learner_catchup_present = false;
  bool learner_promote_present = false;
  bool voter_remove_present = false;
  bool learner_auto_promote_present = false;
  bool witness_role_behavior_present = false;
  bool witness_role_blocker_present = false;
  bool pending_joint_consensus_restart_present = false;
  bool leader_transfer_exact_once_present = false;
  std::vector<std::uint64_t> leader_transfer_exact_once_commit_ids;
  std::uint64_t peer_match_index = 0;
  std::uint64_t peer_next_index = 0;
  std::uint64_t transfer_leader_count = 0;
  std::uint64_t pre_vote_count = 0;
  std::uint64_t election_count = 0;
};

struct StorageUnifiedEvidence {
  std::vector<std::string> write_sequence = {
      "append_record",
      "route_shard_slot",
      "choose_page",
      "append_page_buffer",
      "update_page_index",
      "flush_page_block_segment",
      "update_block_index",
      "publish_append_watermark",
  };
  std::vector<std::string> read_sequence = {
      "logical_key_timestamp_range",
      "object_page_index_lookup",
      "page_address_list",
      "block_index_lookup",
      "page_read",
      "decode_records",
      "return_filtered_result",
  };
  std::vector<std::string> cold_scan_sequence = {
      "timestamp_page_index_scan",
      "no_cache_page_read",
      "bounded_decode",
      "no_hot_cache_promotion",
  };
  std::vector<std::string> lifecycle_phases = {
      "prepare",
      "reclaim",
      "evict",
      "expire",
      "page_gc",
      "block_gc",
      "compaction",
      "index_gc",
      "delayed_destroy",
      "follower_cursor_safety",
      "watermark_progress",
  };
  std::vector<std::string> cache_layers = {
      "memory_object_cache",
      "page_index_cache",
      "block_index_cache",
      "disk_block_cache",
      "shared_store_read_through",
  };
  std::vector<std::string> cache_semantics = {
      "lookup_hot_to_cold",
      "refill_from_durable_on_miss",
      "invalidate_on_append_watermark",
      "invalidate_on_compaction_watermark",
      "cold_scan_no_promote",
      "writeback_backpressure_reported",
  };
  bool cache_eviction_memory_only = true;
  bool logical_tombstone_required = true;
  bool stale_pages_blocks_rewritten_or_skipped = true;
  bool reclaimed_bytes_reported = true;
  bool context_gc_logical_only = true;
  bool storage_lifecycle_reclaims_physical_pages = true;
  std::uint64_t storage_manager_prepare_count = 0;
  std::uint64_t storage_manager_reclaim_count = 0;
  std::uint64_t storage_manager_evict_count = 0;
  std::uint64_t storage_manager_expire_count = 0;
  std::uint64_t storage_manager_page_gc_count = 0;
  std::uint64_t storage_manager_block_gc_count = 0;
  std::uint64_t storage_manager_compaction_count = 0;
  std::uint64_t storage_manager_index_gc_count = 0;
  std::uint64_t storage_manager_delayed_destroy_count = 0;
  std::uint64_t storage_manager_follower_cursor_safety_count = 0;
  std::uint64_t storage_manager_watermark_progress_count = 0;
  double storage_manager_loop_ms = 0.0;
  std::uint64_t stream_rollover_count = 0;
  std::uint64_t segment_open_count = 0;
  std::uint64_t segment_sealed_count = 0;
  std::uint64_t storage_zone_total_bytes = 0;
  std::uint64_t storage_zone_used_bytes = 0;
  std::uint64_t storage_zone_stale_bytes = 0;
  std::uint64_t append_log_replay_records = 0;
  std::uint64_t append_log_reclaimed_records = 0;
  std::uint64_t slot_dirty_generation_count = 0;
  std::uint64_t slot_tombstone_count = 0;
  std::uint64_t slot_stale_ref_count = 0;
  std::uint64_t slot_owner_mismatch_count = 0;
  std::uint64_t page_index_rebuild_count = 0;
  std::uint64_t block_index_rebuild_count = 0;
  std::uint64_t object_index_rebuild_count = 0;
  std::uint64_t page_index_lookup_count = 0;
  double page_index_lookup_ms = 0.0;
  double page_index_cache_hit_rate = 0.0;
  std::uint64_t block_index_lookup_count = 0;
  double block_index_lookup_ms = 0.0;
  double block_index_cache_hit_rate = 0.0;
  std::uint64_t page_reads = 0;
  std::uint64_t page_writes = 0;
  std::uint64_t block_reads = 0;
  std::uint64_t block_writes = 0;
  std::uint64_t bytes_read = 0;
  std::uint64_t bytes_written = 0;
  std::uint64_t cache_admissions = 0;
  std::uint64_t cache_evictions = 0;
  std::uint64_t cache_rehydrates = 0;
  std::uint64_t memory_cache_hits = 0;
  std::uint64_t memory_cache_misses = 0;
  std::uint64_t page_index_cache_hits = 0;
  std::uint64_t page_index_cache_misses = 0;
  std::uint64_t block_index_cache_hits = 0;
  std::uint64_t block_index_cache_misses = 0;
  std::uint64_t disk_cache_hits = 0;
  std::uint64_t disk_cache_misses = 0;
  std::uint64_t shared_store_read_throughs = 0;
  std::uint64_t cache_refills = 0;
  std::uint64_t cache_invalidations = 0;
  std::uint64_t cache_writeback_queue_depth = 0;
  std::uint64_t cache_writeback_rejections = 0;
  std::uint64_t cold_scan_no_cache_reads = 0;
  std::uint64_t cold_scan_page_reads = 0;
  std::uint64_t hot_cache_promotions = 0;
  std::uint64_t tombstone_records = 0;
  std::uint64_t stale_page_tombstones = 0;
  std::uint64_t stale_block_tombstones = 0;
  std::uint64_t stale_pages_rewritten = 0;
  std::uint64_t stale_pages_skipped = 0;
  std::uint64_t stale_blocks_rewritten = 0;
  std::uint64_t stale_blocks_skipped = 0;
  std::uint64_t delayed_destroy_backlog = 0;
  std::uint64_t follower_cursor_retention_floor = 0;
  std::uint64_t reclaimable_bytes = 0;
  std::uint64_t compaction_reclaimed_bytes = 0;
  std::uint64_t physical_reclaimed_bytes = 0;
  std::uint64_t physical_reclaim_errors = 0;
  std::uint64_t append_watermark = 0;
  std::uint64_t compaction_watermark = 0;
};

inline std::int64_t UnixTimeMillis() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline nlohmann::json ToJson(const UnifiedCaseStepReport& step) {
  nlohmann::json row = {
      {"name", step.name},
      {"status", step.status},
      {"latency_ms", step.latency_ms},
  };
  if (!step.output.is_null()) {
    row["output"] = step.output;
  }
  return row;
}

inline nlohmann::json ToJson(const UnifiedCaseReport& report) {
  nlohmann::json row = {
      {"name", report.name},
      {"status", report.status},
      {"steps", nlohmann::json::array()},
  };
  for (const auto& step : report.steps) {
    row["steps"].push_back(ToJson(step));
  }
  return row;
}

inline nlohmann::json ToJson(const UnifiedCaseReportArchive& archive) {
  nlohmann::json root = {
      {"schema", archive.schema},
      {"producer", archive.producer},
      {"generated_at_ms", archive.generated_at_ms == 0 ? UnixTimeMillis() : archive.generated_at_ms},
      {"cases", nlohmann::json::array()},
  };
  for (const auto& report : archive.cases) {
    root["cases"].push_back(ToJson(report));
  }
  return root;
}

inline nlohmann::json ToJson(const ByteRaftUnifiedEvidence& evidence) {
  return {
      {"wal_segment_lifecycle_present", evidence.wal_segment_lifecycle_present},
      {"wal_first_log_index", evidence.wal_first_log_index},
      {"wal_last_log_index", evidence.wal_last_log_index},
      {"wal_segment_release_floor", evidence.wal_segment_release_floor},
      {"read_index_validated", evidence.read_index_validated},
      {"stale_leader_lease_rejected", evidence.stale_leader_lease_rejected},
      {"lagging_follower_read_rejected", evidence.lagging_follower_read_rejected},
      {"stale_follower_write_rejected", evidence.stale_follower_write_rejected},
      {"bounded_stale_reads_under_partition", evidence.bounded_stale_reads_under_partition},
      {"minority_partition_rejected", evidence.minority_partition_rejected},
      {"healed_follower_caught_up", evidence.healed_follower_caught_up},
      {"inflight_limit_enforced", evidence.inflight_limit_enforced},
      {"inflight_append_limit", evidence.inflight_append_limit},
      {"inflight_append_count", evidence.inflight_append_count},
      {"inflight_bytes", evidence.inflight_bytes},
      {"max_memory_replicate_bytes", evidence.max_memory_replicate_bytes},
      {"append_queue_depth", evidence.append_queue_depth},
      {"apply_queue_depth", evidence.apply_queue_depth},
      {"oversized_log_rejected", evidence.oversized_log_rejected},
      {"reorder_gap_recovered", evidence.reorder_gap_recovered},
      {"reorder_queue_depth", evidence.reorder_queue_depth},
      {"reorder_timeout_drops", evidence.reorder_timeout_drops},
      {"stale_term_rejected", evidence.stale_term_rejected},
      {"snapshot_chunk_retry_present", evidence.snapshot_chunk_retry_present},
      {"snapshot_send_timeout_present", evidence.snapshot_send_timeout_present},
      {"snapshot_rate_limit_present", evidence.snapshot_rate_limit_present},
      {"snapshot_install_progress_per_mille", evidence.snapshot_install_progress_per_mille},
      {"snapshot_install_rollback_present", evidence.snapshot_install_rollback_present},
      {"snapshot_during_membership_change_present", evidence.snapshot_during_membership_change_present},
      {"follower_rejoin_after_compacted_log_present", evidence.follower_rejoin_after_compacted_log_present},
      {"learner_add_present", evidence.learner_add_present},
      {"learner_catchup_present", evidence.learner_catchup_present},
      {"learner_promote_present", evidence.learner_promote_present},
      {"voter_remove_present", evidence.voter_remove_present},
      {"learner_auto_promote_present", evidence.learner_auto_promote_present},
      {"witness_role_behavior_present", evidence.witness_role_behavior_present},
      {"witness_role_blocker_present", evidence.witness_role_blocker_present},
      {"pending_joint_consensus_restart_present", evidence.pending_joint_consensus_restart_present},
      {"leader_transfer_exact_once_present", evidence.leader_transfer_exact_once_present},
      {"leader_transfer_exact_once_commit_ids", evidence.leader_transfer_exact_once_commit_ids},
      {"peer_match_index", evidence.peer_match_index},
      {"peer_next_index", evidence.peer_next_index},
      {"transfer_leader_count", evidence.transfer_leader_count},
      {"pre_vote_count", evidence.pre_vote_count},
      {"election_count", evidence.election_count},
  };
}

inline nlohmann::json ToJson(const StorageUnifiedEvidence& evidence) {
  return {
      {"storage_write_sequence", evidence.write_sequence},
      {"storage_read_sequence", evidence.read_sequence},
      {"storage_cold_scan_sequence", evidence.cold_scan_sequence},
      {"storage_lifecycle_phases", evidence.lifecycle_phases},
      {"storage_cache_layers", evidence.cache_layers},
      {"storage_cache_semantics", evidence.cache_semantics},
      {"storage_reclaim_semantics", {
          {"cache_eviction_memory_only", evidence.cache_eviction_memory_only},
          {"logical_tombstone_required", evidence.logical_tombstone_required},
          {"stale_pages_blocks_rewritten_or_skipped", evidence.stale_pages_blocks_rewritten_or_skipped},
          {"reclaimed_bytes_reported", evidence.reclaimed_bytes_reported},
          {"context_gc_logical_only", evidence.context_gc_logical_only},
          {"storage_lifecycle_reclaims_physical_pages", evidence.storage_lifecycle_reclaims_physical_pages},
      }},
      {"storage_manager_prepare_count", evidence.storage_manager_prepare_count},
      {"storage_manager_reclaim_count", evidence.storage_manager_reclaim_count},
      {"storage_manager_evict_count", evidence.storage_manager_evict_count},
      {"storage_manager_expire_count", evidence.storage_manager_expire_count},
      {"storage_manager_page_gc_count", evidence.storage_manager_page_gc_count},
      {"storage_manager_block_gc_count", evidence.storage_manager_block_gc_count},
      {"storage_manager_compaction_count", evidence.storage_manager_compaction_count},
      {"storage_manager_index_gc_count", evidence.storage_manager_index_gc_count},
      {"storage_manager_delayed_destroy_count", evidence.storage_manager_delayed_destroy_count},
      {"storage_manager_follower_cursor_safety_count", evidence.storage_manager_follower_cursor_safety_count},
      {"storage_manager_watermark_progress_count", evidence.storage_manager_watermark_progress_count},
      {"storage_manager_loop_ms", evidence.storage_manager_loop_ms},
      {"stream_rollover_count", evidence.stream_rollover_count},
      {"segment_open_count", evidence.segment_open_count},
      {"segment_sealed_count", evidence.segment_sealed_count},
      {"storage_zone_total_bytes", evidence.storage_zone_total_bytes},
      {"storage_zone_used_bytes", evidence.storage_zone_used_bytes},
      {"storage_zone_stale_bytes", evidence.storage_zone_stale_bytes},
      {"append_log_replay_records", evidence.append_log_replay_records},
      {"append_log_reclaimed_records", evidence.append_log_reclaimed_records},
      {"slot_dirty_generation_count", evidence.slot_dirty_generation_count},
      {"slot_tombstone_count", evidence.slot_tombstone_count},
      {"slot_stale_ref_count", evidence.slot_stale_ref_count},
      {"slot_owner_mismatch_count", evidence.slot_owner_mismatch_count},
      {"page_index_rebuild_count", evidence.page_index_rebuild_count},
      {"block_index_rebuild_count", evidence.block_index_rebuild_count},
      {"object_index_rebuild_count", evidence.object_index_rebuild_count},
      {"page_index_lookup_count", evidence.page_index_lookup_count},
      {"page_index_lookup_ms", evidence.page_index_lookup_ms},
      {"page_index_cache_hit_rate", evidence.page_index_cache_hit_rate},
      {"block_index_lookup_count", evidence.block_index_lookup_count},
      {"block_index_lookup_ms", evidence.block_index_lookup_ms},
      {"block_index_cache_hit_rate", evidence.block_index_cache_hit_rate},
      {"page_reads", evidence.page_reads},
      {"page_writes", evidence.page_writes},
      {"block_reads", evidence.block_reads},
      {"block_writes", evidence.block_writes},
      {"bytes_read", evidence.bytes_read},
      {"bytes_written", evidence.bytes_written},
      {"cache_admissions", evidence.cache_admissions},
      {"cache_evictions", evidence.cache_evictions},
      {"cache_rehydrates", evidence.cache_rehydrates},
      {"memory_cache_hits", evidence.memory_cache_hits},
      {"memory_cache_misses", evidence.memory_cache_misses},
      {"page_index_cache_hits", evidence.page_index_cache_hits},
      {"page_index_cache_misses", evidence.page_index_cache_misses},
      {"block_index_cache_hits", evidence.block_index_cache_hits},
      {"block_index_cache_misses", evidence.block_index_cache_misses},
      {"disk_cache_hits", evidence.disk_cache_hits},
      {"disk_cache_misses", evidence.disk_cache_misses},
      {"shared_store_read_throughs", evidence.shared_store_read_throughs},
      {"cache_refills", evidence.cache_refills},
      {"cache_invalidations", evidence.cache_invalidations},
      {"cache_writeback_queue_depth", evidence.cache_writeback_queue_depth},
      {"cache_writeback_rejections", evidence.cache_writeback_rejections},
      {"cold_scan_no_cache_reads", evidence.cold_scan_no_cache_reads},
      {"cold_scan_page_reads", evidence.cold_scan_page_reads},
      {"hot_cache_promotions", evidence.hot_cache_promotions},
      {"tombstone_records", evidence.tombstone_records},
      {"stale_page_tombstones", evidence.stale_page_tombstones},
      {"stale_block_tombstones", evidence.stale_block_tombstones},
      {"stale_pages_rewritten", evidence.stale_pages_rewritten},
      {"stale_pages_skipped", evidence.stale_pages_skipped},
      {"stale_blocks_rewritten", evidence.stale_blocks_rewritten},
      {"stale_blocks_skipped", evidence.stale_blocks_skipped},
      {"delayed_destroy_backlog", evidence.delayed_destroy_backlog},
      {"follower_cursor_retention_floor", evidence.follower_cursor_retention_floor},
      {"reclaimable_bytes", evidence.reclaimable_bytes},
      {"compaction_reclaimed_bytes", evidence.compaction_reclaimed_bytes},
      {"physical_reclaimed_bytes", evidence.physical_reclaimed_bytes},
      {"physical_reclaim_errors", evidence.physical_reclaim_errors},
      {"append_watermark", evidence.append_watermark},
      {"compaction_watermark", evidence.compaction_watermark},
  };
}

inline UnifiedCaseReport& AddCase(
    UnifiedCaseReportArchive* archive,
    std::string name,
    std::string status = "passed") {
  archive->cases.push_back(UnifiedCaseReport{std::move(name), std::move(status), {}});
  return archive->cases.back();
}

inline void AddStep(
    UnifiedCaseReport* report,
    UnifiedCaseStepReport step) {
  report->steps.push_back(std::move(step));
}

inline void SaveJsonArchive(
    const UnifiedCaseReportArchive& archive,
    const std::string& path) {
  std::ofstream out(path);
  out << ToJson(archive).dump(2) << "\n";
}

inline UnifiedCaseStepReport PassedStep(
    std::string name,
    nlohmann::json output = nullptr,
    double latency_ms = 0.0) {
  return UnifiedCaseStepReport{
      std::move(name),
      "passed",
      std::move(output),
      latency_ms,
  };
}

inline UnifiedCaseStepReport FailedStep(
    std::string name,
    nlohmann::json output = nullptr,
    double latency_ms = 0.0) {
  return UnifiedCaseStepReport{
      std::move(name),
      "failed",
      std::move(output),
      latency_ms,
  };
}

inline UnifiedCaseStepReport ByteRaftPassedStep(
    std::string name,
    const ByteRaftUnifiedEvidence& evidence,
    double latency_ms = 0.0) {
  return PassedStep(std::move(name), ToJson(evidence), latency_ms);
}

inline UnifiedCaseStepReport ByteRaftFailedStep(
    std::string name,
    const ByteRaftUnifiedEvidence& evidence,
    double latency_ms = 0.0) {
  return FailedStep(std::move(name), ToJson(evidence), latency_ms);
}

inline UnifiedCaseStepReport StoragePassedStep(
    std::string name,
    const StorageUnifiedEvidence& evidence,
    double latency_ms = 0.0) {
  return PassedStep(std::move(name), ToJson(evidence), latency_ms);
}

inline UnifiedCaseStepReport StorageFailedStep(
    std::string name,
    const StorageUnifiedEvidence& evidence,
    double latency_ms = 0.0) {
  return FailedStep(std::move(name), ToJson(evidence), latency_ms);
}

}  // namespace temporalstore::compat
