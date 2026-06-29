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
  std::vector<UnifiedCaseReport> cases;
};

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
      {"cases", nlohmann::json::array()},
  };
  for (const auto& report : archive.cases) {
    root["cases"].push_back(ToJson(report));
  }
  return root;
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

}  // namespace temporalstore::compat
