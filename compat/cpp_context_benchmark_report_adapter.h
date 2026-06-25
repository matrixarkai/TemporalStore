// Shared C++/Rust Context benchmark report adapter contract.
//
// This header is intended to be copied or vendored into the C++ TemporalStore
// benchmark path. It maps MatrixArk/VikingMem benchmark results into the shared
// matrixark_vikingmem_context_benchmark_report_v1 JSON shape consumed by Rust
// tools/compare_context_benchmark_reports.py.
//
// Dependency: nlohmann/json.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace temporalstore::compat {

struct ContextBenchmarkThresholds {
  int64_t min_case_count = 0;
  double min_hit_at_k = 0.0;
  double min_reader_hit_rate = 0.0;
  double min_token_reduction_percent = 0.0;
  double max_retrieval_p95_ms = 0.0;
  double max_reader_p95_ms = 0.0;
  bool require_open_source_reader = false;
};

struct ContextBenchmarkPerQueryRow {
  std::string query_id;
  std::string category;
  std::string reader_answer;
  bool hit = false;
  bool reader_hit = false;
  int64_t rank = 0;  // Use 0 when no rank exists; JSON emits null.
  int64_t answer_terms = 0;
  int64_t matched_answer_terms = 0;
  int64_t matched_retrieval_answer_terms = 0;
  int64_t expected_source_refs = 0;
  int64_t matched_source_refs = 0;
  int64_t retrieved_blocks = 0;
  int64_t retrieved_source_groups = 0;
  int64_t source_tokens = 0;
  int64_t retrieved_tokens = 0;
  double token_reduction_percent = 0.0;
  double retrieval_ms = 0.0;
  double reader_ms = 0.0;
  std::vector<std::string> expected_answer_terms;
  std::vector<std::string> expected_source_ref_ids;
  std::vector<std::string> retrieved_source_ids;
  std::vector<std::string> retrieved_source_group_ids;
};

struct ContextBenchmarkReport {
  std::string schema = "matrixark_vikingmem_context_benchmark_report_v1";
  std::string benchmark_family = "vikingmem_long_memory";
  std::string dataset;
  std::string mode = "conversation_load_once_query_many";
  std::string input;
  std::string input_sha256;
  std::string reader_mode_requested;
  std::string reader_mode_effective;
  std::string reader_provider_name;
  std::string reader_model;
  std::string reader_last_error;

  int64_t case_count = 0;
  int64_t conversation_count = 0;
  int64_t source_count = 0;
  int64_t input_bytes = 0;
  int64_t benchmark_per_query_count = 0;
  int64_t reader_open_source_calls = 0;
  int64_t reader_fallback_count = 0;
  int64_t reader_error_count = 0;
  int64_t zero_hit_queries = 0;
  int64_t reader_zero_hit_queries = 0;

  double hit_rate = 0.0;
  double benchmark_hit_at_k = 0.0;
  double benchmark_recall_at_k = 0.0;
  double mean_reciprocal_rank = 0.0;
  double benchmark_mean_reciprocal_rank = 0.0;
  double answer_term_coverage = 0.0;
  double evidence_ref_coverage = 0.0;
  double reader_hit_rate = 0.0;
  double reader_answer_coverage = 0.0;
  double benchmark_token_reduction_percent = 0.0;
  double benchmark_retrieval_p50_ms = 0.0;
  double benchmark_retrieval_p95_ms = 0.0;
  double benchmark_reader_p50_ms = 0.0;
  double benchmark_reader_p95_ms = 0.0;
  double benchmark_avg_retrieved_blocks_per_query = 0.0;
  double benchmark_avg_retrieved_source_groups_per_query = 0.0;
  double benchmark_multi_source_group_query_rate = 0.0;
  double benchmark_avg_source_tokens_per_query = 0.0;
  double benchmark_avg_retrieved_tokens_per_query = 0.0;
  double benchmark_max_retrieved_tokens_per_query = 0.0;

  bool benchmark_quality_ready = false;
  bool benchmark_threshold_passed = false;
  bool paper_comparable_claim_ready = false;
  bool rust_temporalstore_full_replay_ready = false;
  int64_t benchmark_threshold_violation_count = 0;
  std::vector<std::string> benchmark_threshold_violations;
  ContextBenchmarkThresholds benchmark_thresholds;
  nlohmann::json category_breakdown = nlohmann::json::object();
  int64_t weak_category_count = 0;
  nlohmann::json weak_categories = nlohmann::json::array();
  nlohmann::json weak_category_policy = nlohmann::json::object();
  std::vector<ContextBenchmarkPerQueryRow> benchmark_per_query;
};

inline double SafeRate(double numerator, double denominator) {
  return denominator > 0.0 ? numerator / denominator : 0.0;
}

inline double Percentile(std::vector<double> values, double pct) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  if (values.size() == 1) {
    return values.front();
  }
  const double rank = (static_cast<double>(values.size() - 1) * pct) / 100.0;
  const auto lower = static_cast<std::size_t>(rank);
  const auto upper = static_cast<std::size_t>(std::ceil(rank));
  if (lower == upper) {
    return values[lower];
  }
  return values[lower] + (values[upper] - values[lower]) * (rank - static_cast<double>(lower));
}

inline nlohmann::json CategoryStatsToJson(
    int64_t case_count,
    int64_t hits,
    double reciprocal_rank_sum,
    int64_t answer_terms,
    int64_t matched_answer_terms,
    int64_t retrieval_answer_terms,
    int64_t source_refs,
    int64_t matched_source_refs,
    int64_t reader_hits,
    int64_t reader_answer_terms,
    int64_t zero_hit_queries) {
  return nlohmann::json{
      {"case_count", case_count},
      {"hit_rate", SafeRate(static_cast<double>(hits), static_cast<double>(case_count))},
      {"mean_reciprocal_rank", SafeRate(reciprocal_rank_sum, static_cast<double>(case_count))},
      {"answer_term_coverage", SafeRate(static_cast<double>(matched_answer_terms), static_cast<double>(answer_terms))},
      {"retrieval_answer_term_coverage", SafeRate(static_cast<double>(retrieval_answer_terms), static_cast<double>(answer_terms))},
      {"evidence_ref_coverage", SafeRate(static_cast<double>(matched_source_refs), static_cast<double>(source_refs))},
      {"reader_hit_rate", SafeRate(static_cast<double>(reader_hits), static_cast<double>(case_count))},
      {"reader_answer_coverage", SafeRate(static_cast<double>(reader_answer_terms), static_cast<double>(answer_terms))},
      {"zero_hit_queries", zero_hit_queries},
  };
}

inline void FinalizeReport(ContextBenchmarkReport* report) {
  if (report == nullptr) {
    throw std::invalid_argument("report must not be null");
  }
  const int64_t row_count = static_cast<int64_t>(report->benchmark_per_query.size());
  report->benchmark_per_query_count = row_count;
  if (report->case_count == 0) {
    report->case_count = row_count;
  }
  report->zero_hit_queries = 0;
  report->reader_zero_hit_queries = 0;

  int64_t hits = 0;
  int64_t reader_hits = 0;
  int64_t answer_terms = 0;
  int64_t matched_answer_terms = 0;
  int64_t retrieval_answer_terms = 0;
  int64_t source_refs = 0;
  int64_t matched_source_refs = 0;
  int64_t source_tokens = 0;
  int64_t retrieved_tokens = 0;
  int64_t retrieved_blocks = 0;
  int64_t retrieved_source_groups = 0;
  int64_t multi_source_group_queries = 0;
  double reciprocal_rank_sum = 0.0;
  std::vector<double> retrieval_latencies;
  std::vector<double> reader_latencies;
  std::map<std::string, std::vector<const ContextBenchmarkPerQueryRow*>> by_category;
  std::unordered_set<std::string> query_ids;

  for (const auto& row : report->benchmark_per_query) {
    if (row.query_id.empty()) {
      throw std::invalid_argument("benchmark_per_query row has empty query_id");
    }
    if (!query_ids.insert(row.query_id).second) {
      throw std::invalid_argument("duplicate benchmark query_id: " + row.query_id);
    }
    by_category[row.category.empty() ? "unknown" : row.category].push_back(&row);
    if (row.hit) {
      ++hits;
    } else {
      ++report->zero_hit_queries;
    }
    if (row.reader_hit) {
      ++reader_hits;
    } else {
      ++report->reader_zero_hit_queries;
    }
    if (row.rank > 0) {
      reciprocal_rank_sum += 1.0 / static_cast<double>(row.rank);
    }
    answer_terms += row.answer_terms;
    matched_answer_terms += row.matched_answer_terms;
    retrieval_answer_terms += row.matched_retrieval_answer_terms;
    source_refs += row.expected_source_refs;
    matched_source_refs += row.matched_source_refs;
    source_tokens += row.source_tokens;
    retrieved_tokens += row.retrieved_tokens;
    retrieved_blocks += row.retrieved_blocks;
    retrieved_source_groups += row.retrieved_source_groups;
    if (row.retrieved_source_groups > 1) {
      ++multi_source_group_queries;
    }
    retrieval_latencies.push_back(row.retrieval_ms);
    reader_latencies.push_back(row.reader_ms);
  }

  report->hit_rate = SafeRate(static_cast<double>(hits), static_cast<double>(report->case_count));
  report->benchmark_hit_at_k = report->hit_rate;
  if (report->benchmark_recall_at_k == 0.0) {
    report->benchmark_recall_at_k = report->hit_rate;
  }
  report->mean_reciprocal_rank = SafeRate(reciprocal_rank_sum, static_cast<double>(report->case_count));
  report->benchmark_mean_reciprocal_rank = report->mean_reciprocal_rank;
  report->answer_term_coverage = SafeRate(static_cast<double>(matched_answer_terms), static_cast<double>(answer_terms));
  report->evidence_ref_coverage = SafeRate(static_cast<double>(matched_source_refs), static_cast<double>(source_refs));
  report->reader_hit_rate = SafeRate(static_cast<double>(reader_hits), static_cast<double>(report->case_count));
  report->reader_answer_coverage = SafeRate(static_cast<double>(matched_answer_terms), static_cast<double>(answer_terms));
  report->benchmark_token_reduction_percent =
      100.0 * (1.0 - SafeRate(static_cast<double>(retrieved_tokens), static_cast<double>(source_tokens)));
  report->benchmark_retrieval_p50_ms = Percentile(retrieval_latencies, 50.0);
  report->benchmark_retrieval_p95_ms = Percentile(retrieval_latencies, 95.0);
  report->benchmark_reader_p50_ms = Percentile(reader_latencies, 50.0);
  report->benchmark_reader_p95_ms = Percentile(reader_latencies, 95.0);
  report->benchmark_avg_retrieved_blocks_per_query =
      SafeRate(static_cast<double>(retrieved_blocks), static_cast<double>(report->case_count));
  report->benchmark_avg_retrieved_source_groups_per_query =
      SafeRate(static_cast<double>(retrieved_source_groups), static_cast<double>(report->case_count));
  report->benchmark_multi_source_group_query_rate =
      SafeRate(static_cast<double>(multi_source_group_queries), static_cast<double>(report->case_count));
  report->benchmark_avg_source_tokens_per_query =
      SafeRate(static_cast<double>(source_tokens), static_cast<double>(report->case_count));
  report->benchmark_avg_retrieved_tokens_per_query =
      SafeRate(static_cast<double>(retrieved_tokens), static_cast<double>(report->case_count));
  for (const auto& row : report->benchmark_per_query) {
    report->benchmark_max_retrieved_tokens_per_query =
        std::max(report->benchmark_max_retrieved_tokens_per_query, static_cast<double>(row.retrieved_tokens));
  }

  nlohmann::json categories = nlohmann::json::object();
  for (const auto& [category, rows] : by_category) {
    int64_t category_hits = 0;
    int64_t category_reader_hits = 0;
    int64_t category_answer_terms = 0;
    int64_t category_matched_answer_terms = 0;
    int64_t category_retrieval_answer_terms = 0;
    int64_t category_source_refs = 0;
    int64_t category_matched_source_refs = 0;
    int64_t category_zero_hit_queries = 0;
    double category_reciprocal_rank_sum = 0.0;
    for (const auto* row : rows) {
      if (row->hit) {
        ++category_hits;
      } else {
        ++category_zero_hit_queries;
      }
      if (row->reader_hit) {
        ++category_reader_hits;
      }
      if (row->rank > 0) {
        category_reciprocal_rank_sum += 1.0 / static_cast<double>(row->rank);
      }
      category_answer_terms += row->answer_terms;
      category_matched_answer_terms += row->matched_answer_terms;
      category_retrieval_answer_terms += row->matched_retrieval_answer_terms;
      category_source_refs += row->expected_source_refs;
      category_matched_source_refs += row->matched_source_refs;
    }
    categories[category] = CategoryStatsToJson(
        static_cast<int64_t>(rows.size()),
        category_hits,
        category_reciprocal_rank_sum,
        category_answer_terms,
        category_matched_answer_terms,
        category_retrieval_answer_terms,
        category_source_refs,
        category_matched_source_refs,
        category_reader_hits,
        category_matched_answer_terms,
        category_zero_hit_queries);
  }
  report->category_breakdown = categories;

  std::vector<std::string> violations;
  if (report->case_count < report->benchmark_thresholds.min_case_count) {
    violations.push_back("case_count below min_case_count");
  }
  if (report->benchmark_hit_at_k < report->benchmark_thresholds.min_hit_at_k) {
    violations.push_back("benchmark_hit_at_k below min_hit_at_k");
  }
  if (report->reader_hit_rate < report->benchmark_thresholds.min_reader_hit_rate) {
    violations.push_back("reader_hit_rate below min_reader_hit_rate");
  }
  if (report->benchmark_token_reduction_percent < report->benchmark_thresholds.min_token_reduction_percent) {
    violations.push_back("benchmark_token_reduction_percent below min_token_reduction_percent");
  }
  if (report->benchmark_thresholds.max_retrieval_p95_ms > 0.0 &&
      report->benchmark_retrieval_p95_ms > report->benchmark_thresholds.max_retrieval_p95_ms) {
    violations.push_back("benchmark_retrieval_p95_ms above max_retrieval_p95_ms");
  }
  if (report->benchmark_thresholds.max_reader_p95_ms > 0.0 &&
      report->benchmark_reader_p95_ms > report->benchmark_thresholds.max_reader_p95_ms) {
    violations.push_back("benchmark_reader_p95_ms above max_reader_p95_ms");
  }
  if (report->benchmark_thresholds.require_open_source_reader && report->reader_open_source_calls <= 0) {
    violations.push_back("reader_open_source_calls required");
  }
  report->benchmark_threshold_violations = violations;
  report->benchmark_threshold_violation_count = static_cast<int64_t>(violations.size());
  report->benchmark_threshold_passed = violations.empty();
  report->benchmark_quality_ready = report->benchmark_threshold_passed && report->case_count > 0;
}

inline nlohmann::json ToJson(const ContextBenchmarkThresholds& thresholds) {
  return nlohmann::json{
      {"min_case_count", thresholds.min_case_count},
      {"min_hit_at_k", thresholds.min_hit_at_k},
      {"min_reader_hit_rate", thresholds.min_reader_hit_rate},
      {"min_token_reduction_percent", thresholds.min_token_reduction_percent},
      {"max_retrieval_p95_ms", thresholds.max_retrieval_p95_ms},
      {"max_reader_p95_ms", thresholds.max_reader_p95_ms},
      {"require_open_source_reader", thresholds.require_open_source_reader},
  };
}

inline nlohmann::json ToJson(const ContextBenchmarkPerQueryRow& row) {
  nlohmann::json rank = nullptr;
  if (row.rank > 0) {
    rank = row.rank;
  }
  return nlohmann::json{
      {"query_id", row.query_id},
      {"category", row.category},
      {"hit", row.hit},
      {"rank", std::move(rank)},
      {"reader_hit", row.reader_hit},
      {"reader_answer", row.reader_answer},
      {"matched_answer_terms", row.matched_answer_terms},
      {"answer_terms", row.answer_terms},
      {"expected_answer_terms", row.expected_answer_terms},
      {"matched_retrieval_answer_terms", row.matched_retrieval_answer_terms},
      {"expected_source_refs", row.expected_source_refs},
      {"expected_source_ref_ids", row.expected_source_ref_ids},
      {"matched_source_refs", row.matched_source_refs},
      {"retrieved_blocks", row.retrieved_blocks},
      {"retrieved_source_ids", row.retrieved_source_ids},
      {"retrieved_source_groups", row.retrieved_source_groups},
      {"retrieved_source_group_ids", row.retrieved_source_group_ids},
      {"source_tokens", row.source_tokens},
      {"retrieved_tokens", row.retrieved_tokens},
      {"token_reduction_percent", row.token_reduction_percent},
      {"retrieval_ms", row.retrieval_ms},
      {"reader_ms", row.reader_ms},
  };
}

inline nlohmann::json ToJson(const ContextBenchmarkReport& report) {
  nlohmann::json rows = nlohmann::json::array();
  for (const auto& row : report.benchmark_per_query) {
    rows.push_back(ToJson(row));
  }
  return nlohmann::json{
      {"schema", report.schema},
      {"mode", report.mode},
      {"benchmark_family", report.benchmark_family},
      {"dataset", report.dataset},
      {"input", report.input},
      {"input_sha256", report.input_sha256},
      {"input_bytes", report.input_bytes},
      {"case_count", report.case_count},
      {"conversation_count", report.conversation_count},
      {"source_count", report.source_count},
      {"hit_rate", report.hit_rate},
      {"benchmark_hit_at_k", report.benchmark_hit_at_k},
      {"benchmark_recall_at_k", report.benchmark_recall_at_k},
      {"mean_reciprocal_rank", report.mean_reciprocal_rank},
      {"benchmark_mean_reciprocal_rank", report.benchmark_mean_reciprocal_rank},
      {"answer_term_coverage", report.answer_term_coverage},
      {"evidence_ref_coverage", report.evidence_ref_coverage},
      {"reader_hit_rate", report.reader_hit_rate},
      {"reader_answer_coverage", report.reader_answer_coverage},
      {"reader_mode_requested", report.reader_mode_requested},
      {"reader_mode_effective", report.reader_mode_effective},
      {"reader_provider_name", report.reader_provider_name},
      {"reader_model", report.reader_model},
      {"reader_open_source_calls", report.reader_open_source_calls},
      {"reader_fallback_count", report.reader_fallback_count},
      {"reader_error_count", report.reader_error_count},
      {"reader_last_error", report.reader_last_error},
      {"zero_hit_queries", report.zero_hit_queries},
      {"reader_zero_hit_queries", report.reader_zero_hit_queries},
      {"benchmark_quality_ready", report.benchmark_quality_ready},
      {"benchmark_threshold_passed", report.benchmark_threshold_passed},
      {"paper_comparable_claim_ready", report.paper_comparable_claim_ready},
      {"rust_temporalstore_full_replay_ready", report.rust_temporalstore_full_replay_ready},
      {"benchmark_threshold_violation_count", report.benchmark_threshold_violation_count},
      {"benchmark_threshold_violations", report.benchmark_threshold_violations},
      {"benchmark_thresholds", ToJson(report.benchmark_thresholds)},
      {"category_breakdown", report.category_breakdown},
      {"weak_category_count", report.weak_category_count},
      {"weak_categories", report.weak_categories},
      {"weak_category_policy", report.weak_category_policy},
      {"benchmark_per_query_count", report.benchmark_per_query_count},
      {"benchmark_per_query", std::move(rows)},
      {"benchmark_retrieval_p50_ms", report.benchmark_retrieval_p50_ms},
      {"benchmark_retrieval_p95_ms", report.benchmark_retrieval_p95_ms},
      {"benchmark_reader_p50_ms", report.benchmark_reader_p50_ms},
      {"benchmark_reader_p95_ms", report.benchmark_reader_p95_ms},
      {"benchmark_avg_retrieved_blocks_per_query", report.benchmark_avg_retrieved_blocks_per_query},
      {"benchmark_avg_retrieved_source_groups_per_query", report.benchmark_avg_retrieved_source_groups_per_query},
      {"benchmark_multi_source_group_query_rate", report.benchmark_multi_source_group_query_rate},
      {"benchmark_avg_source_tokens_per_query", report.benchmark_avg_source_tokens_per_query},
      {"benchmark_avg_retrieved_tokens_per_query", report.benchmark_avg_retrieved_tokens_per_query},
      {"benchmark_max_retrieved_tokens_per_query", report.benchmark_max_retrieved_tokens_per_query},
      {"benchmark_token_reduction_percent", report.benchmark_token_reduction_percent},
  };
}

inline void ValidateReportContract(const nlohmann::json& report) {
  static const std::vector<std::string> required = {
      "schema",
      "benchmark_family",
      "input_sha256",
      "input_bytes",
      "benchmark_hit_at_k",
      "benchmark_recall_at_k",
      "benchmark_mean_reciprocal_rank",
      "benchmark_token_reduction_percent",
      "benchmark_retrieval_p50_ms",
      "benchmark_retrieval_p95_ms",
      "benchmark_reader_p50_ms",
      "benchmark_reader_p95_ms",
      "benchmark_quality_ready",
      "benchmark_threshold_passed",
      "benchmark_threshold_violation_count",
      "benchmark_threshold_violations",
      "benchmark_thresholds",
      "category_breakdown",
      "weak_category_count",
      "weak_categories",
      "weak_category_policy",
      "benchmark_per_query_count",
      "benchmark_avg_retrieved_source_groups_per_query",
      "benchmark_multi_source_group_query_rate",
      "benchmark_max_retrieved_tokens_per_query",
      "case_count",
      "hit_rate",
      "reader_hit_rate",
      "reader_mode_requested",
      "reader_mode_effective",
      "reader_provider_name",
      "reader_model",
      "paper_comparable_claim_ready",
      "rust_temporalstore_full_replay_ready",
  };
  for (const auto& field : required) {
    if (!report.contains(field)) {
      throw std::invalid_argument("missing benchmark report field: " + field);
    }
  }
  if (!report.contains("benchmark_per_query") || !report["benchmark_per_query"].is_array()) {
    throw std::invalid_argument("benchmark_per_query must be an array");
  }
  static const std::vector<std::string> per_query_required = {
      "query_id",
      "category",
      "hit",
      "rank",
      "reader_hit",
      "reader_answer",
      "expected_answer_terms",
      "expected_source_ref_ids",
      "retrieved_source_ids",
      "retrieved_source_groups",
      "retrieved_source_group_ids",
      "retrieval_ms",
      "reader_ms",
      "retrieved_blocks",
      "retrieved_tokens",
      "source_tokens",
      "token_reduction_percent",
  };
  for (const auto& row : report["benchmark_per_query"]) {
    if (!row.is_object()) {
      throw std::invalid_argument("benchmark_per_query row must be an object");
    }
    for (const auto& field : per_query_required) {
      if (!row.contains(field)) {
        throw std::invalid_argument("missing benchmark per-query field: " + field);
      }
    }
  }
}

inline nlohmann::json FinalizedJson(ContextBenchmarkReport report) {
  FinalizeReport(&report);
  auto json = ToJson(report);
  ValidateReportContract(json);
  return json;
}

}  // namespace temporalstore::compat
