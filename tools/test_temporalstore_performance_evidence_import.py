#!/usr/bin/env python3
"""Unit tests for fail-closed C++/Rust performance evidence import."""

from __future__ import annotations

import unittest

from import_temporalstore_cpp_rust_performance_evidence import import_report
from validate_storage_tuning_parity import EXPECTED_DEFAULTS as STORAGE_TUNING


def _empty_row(workload: str) -> dict:
    return {
        "workload": workload,
        "status": "missing_live_evidence",
        "same_config_match": False,
        "dataset": None,
        "storage_mode": None,
        "topology": None,
        "batch_size": None,
        "token_budget": None,
        "embedding_model": None,
        "reader_model": None,
        "judge_model": None,
        "storage_tuning": None,
        "cpp": {},
        "rust": {},
        "ratios": {},
        "open_blockers": [f"{workload} evidence missing"],
    }


def _matrix() -> dict:
    workloads = [
        "1K_event_ingestion",
        "10K_event_ingestion",
        "100K_event_ingestion",
        "retrieve_workers_4",
        "retrieve_workers_8",
        "retrieve_workers_16",
        "retrieve_workers_32",
    ]
    return {
        "schema": "temporalstore_cpp_rust_performance_parity_matrix_v1",
        "status": {
            "performance_candidate": False,
            "production_performance_parity": False,
            "open_blockers": ["missing evidence"],
        },
        "thresholds": {
            "min_rust_cpp_qps_ratio": 0.8,
            "max_rust_cpp_latency_ratio": 2.0,
            "max_timeout_count": 0,
            "max_error_count": 0,
            "allow_fallback_flags": False,
            "require_selected_ref_parity": True,
        },
        "same_config": {
            "dataset": "required_per_row",
            "storage_mode": "required_per_row",
            "topology": "required_per_row",
            "batch_size": "required_per_row",
            "token_budget": "required_per_row",
            "embedding_model": "required_per_row",
            "reader_model": "required_per_row",
            "judge_model": "required_per_row",
            "storage_tuning": "required_per_row",
        },
        "required_workloads": workloads,
        "required_metrics": [
            "message_qps",
            "retrieve_qps",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "timeout_count",
            "error_count",
            "fallback_flags",
            "selected_ref_parity",
            "scanned_records",
            "cache_hit_rate",
            "append_watermark",
            "compaction_watermark",
        ],
        "rows": [_empty_row(workload) for workload in workloads],
    }


def _report_with_bad_qps_ratio() -> dict:
    config = {
        "events": 1000,
        "dataset": "matrixark-scale-synthetic",
        "storage_options": {"storage_family": "shared_store", "write_mode": "async"},
        "topology": {"metaserver": "127.0.0.1:18000"},
        "batch_size": 20,
        "max_context_tokens": 12000,
        "embedding_model": "matrixark-local-token-hash-v1",
        "reader_model": "matrixark-deterministic-reader",
        "judge_model": "matrixark-deterministic-judge",
        "effective_storage_tuning": dict(STORAGE_TUNING),
    }
    backend_template = {
        "status": "passed",
        "ingest": {"p50_ms": 10, "p95_ms": 20, "p99_ms": 30, "timeout_count": 0},
        "retrieve": {"qps": 10, "p50_ms": 10, "p95_ms": 20, "p99_ms": 30, "stage_metrics": {}},
        "storage_lifecycle_metrics": {"append_watermark": 10, "compaction_watermark": 9},
        "effective_storage_tuning": dict(STORAGE_TUNING),
        "errors": [],
    }
    cpp = {
        **backend_template,
        "ingest_messages": {"message_qps": 100},
    }
    rust = {
        **backend_template,
        "ingest_messages": {"message_qps": 10},
    }
    return {
        "config": config,
        "comparison": {"phase0_correctness": {"evidence": {"selected_ref_parity": True}}},
        "phase_scale_matrix": _passed_phase_scale_matrix(),
        "backends": {"cpp": cpp, "rust": rust},
    }


def _passed_phase_scale_matrix() -> dict:
    return {
        "status": "passed",
        "require_gate": True,
        "open_required_cases": [],
        "full_contextmemory_pipeline": {"status": "passed"},
    }


def _report_with_good_parity() -> dict:
    report = _report_with_bad_qps_ratio()
    report["backends"]["rust"]["ingest_messages"]["message_qps"] = 90
    return report


class PerformanceEvidenceImportTest(unittest.TestCase):
    def test_threshold_failure_stays_missing_live_evidence(self) -> None:
        updated = import_report(_matrix(), _report_with_bad_qps_ratio())
        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertFalse(row["same_config_match"])
        self.assertIn("message_qps_ratio_below_0.8", row["open_blockers"])
        self.assertFalse(updated["status"]["performance_candidate"])
        self.assertFalse(updated["status"]["production_performance_parity"])

    def test_missing_phase_scale_gate_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        report.pop("phase_scale_matrix")

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("phase_scale_matrix_missing", row["open_blockers"])
        self.assertFalse(updated["status"]["performance_candidate"])

    def test_same_config_drift_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        report["config"]["dataset"] = "different-dataset"
        report["config"]["batch_size"] = 10

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertFalse(row["same_config_match"])
        self.assertIn("same_config_drift:dataset", row["open_blockers"])
        self.assertIn("same_config_drift:batch_size", row["open_blockers"])

    def test_storage_tuning_drift_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        del report["config"]["effective_storage_tuning"]["TS_PAGE_INDEX_CACHE_BYTES"]
        report["config"]["effective_storage_tuning"]["TS_STREAM_MAX_BLOB_SIZE"] = 20 * 1024 * 1024

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("storage_tuning_missing:TS_PAGE_INDEX_CACHE_BYTES", row["open_blockers"])
        self.assertIn("storage_tuning_drift:TS_STREAM_MAX_BLOB_SIZE", row["open_blockers"])

    def test_backend_storage_tuning_drift_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        del report["backends"]["cpp"]["effective_storage_tuning"]["TS_BLOCK_INDEX_CACHE_BYTES"]
        report["backends"]["rust"]["effective_storage_tuning"]["TS_STORAGE_ZONE_SIZE"] = 123

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("cpp_storage_tuning_missing:TS_BLOCK_INDEX_CACHE_BYTES", row["open_blockers"])
        self.assertIn("rust_storage_tuning_drift:TS_STORAGE_ZONE_SIZE", row["open_blockers"])

    def test_backend_storage_tuning_missing_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        report["backends"]["rust"].pop("effective_storage_tuning")

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("rust_storage_tuning_missing", row["open_blockers"])

    def test_watermark_regression_blocks_otherwise_good_report(self) -> None:
        report = _report_with_good_parity()
        report["backends"]["cpp"]["storage_lifecycle_metrics"]["append_watermark"] = 0
        report["backends"]["rust"]["storage_lifecycle_metrics"]["compaction_watermark"] = 11

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("cpp_append_watermark_not_advanced", row["open_blockers"])
        self.assertIn("rust_compaction_watermark_ahead_of_append", row["open_blockers"])

    def test_phase_scale_gate_must_be_required_and_passed(self) -> None:
        report = _report_with_good_parity()
        report["phase_scale_matrix"] = {
            "status": "incomplete",
            "require_gate": False,
            "open_required_cases": [{"group": "event_ingestion", "case": 100000}],
            "full_contextmemory_pipeline": {"status": "incomplete"},
        }

        updated = import_report(_matrix(), report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("phase_scale_matrix_not_required", row["open_blockers"])
        self.assertIn("phase_scale_matrix_incomplete", row["open_blockers"])
        self.assertIn("phase_scale_matrix_open_required_cases", row["open_blockers"])
        self.assertIn("phase_scale_contextmemory_pipeline_incomplete", row["open_blockers"])

    def test_passed_phase_scale_gate_allows_clean_report(self) -> None:
        updated = import_report(_matrix(), _report_with_good_parity())

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "production_performance_parity")
        self.assertTrue(row["same_config_match"])
        self.assertEqual(row["open_blockers"], [])

    def test_threshold_config_controls_timeout_error_and_fallback_policy(self) -> None:
        matrix = _matrix()
        matrix["thresholds"]["max_timeout_count"] = 2
        matrix["thresholds"]["max_error_count"] = 1
        matrix["thresholds"]["allow_fallback_flags"] = True
        matrix["thresholds"]["require_selected_ref_parity"] = False
        report = _report_with_good_parity()
        report["comparison"]["phase0_correctness"]["evidence"]["selected_ref_parity"] = False
        report["backends"]["cpp"]["ingest"]["timeout_count"] = 2
        report["backends"]["rust"]["ingest"]["timeout_count"] = 2
        report["backends"]["cpp"]["errors"] = ["transient"]
        report["backends"]["rust"]["fallback_flags"] = ["debug_pack_fallback"]

        updated = import_report(matrix, report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "production_performance_parity")
        self.assertEqual(row["open_blockers"], [])

    def test_threshold_config_blocks_counts_above_limits(self) -> None:
        matrix = _matrix()
        matrix["thresholds"]["max_timeout_count"] = 1
        matrix["thresholds"]["max_error_count"] = 0
        report = _report_with_good_parity()
        report["backends"]["cpp"]["ingest"]["timeout_count"] = 2
        report["backends"]["rust"]["errors"] = ["write_path_error"]

        updated = import_report(matrix, report)

        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertIn("timeout_count_above_1", row["open_blockers"])
        self.assertIn("error_count_above_0", row["open_blockers"])


if __name__ == "__main__":
    unittest.main()
