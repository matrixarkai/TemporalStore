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

}  // namespace temporalstore::compat
