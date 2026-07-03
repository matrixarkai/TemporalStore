#!/usr/bin/env python3
"""Unit tests for fail-closed C++/Rust performance evidence import."""

from __future__ import annotations

import unittest

from import_temporalstore_cpp_rust_performance_evidence import import_report


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
        "batch_size": 10,
        "max_context_tokens": 12000,
        "embedding_model": "hashing-local",
        "reader_model": "deterministic-reader",
        "judge_model": "deterministic-judge",
        "effective_storage_tuning": {"TS_CONTEXT_PAGE_TARGET_BYTES": 65536},
    }
    backend_template = {
        "status": "passed",
        "ingest": {"p50_ms": 10, "p95_ms": 20, "p99_ms": 30, "timeout_count": 0},
        "retrieve": {"qps": 10, "p50_ms": 10, "p95_ms": 20, "p99_ms": 30, "stage_metrics": {}},
        "storage_lifecycle_metrics": {"append_watermark": 10, "compaction_watermark": 9},
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
        "backends": {"cpp": cpp, "rust": rust},
    }


class PerformanceEvidenceImportTest(unittest.TestCase):
    def test_threshold_failure_stays_missing_live_evidence(self) -> None:
        updated = import_report(_matrix(), _report_with_bad_qps_ratio())
        row = next(row for row in updated["rows"] if row["workload"] == "1K_event_ingestion")
        self.assertEqual(row["status"], "missing_live_evidence")
        self.assertFalse(row["same_config_match"])
        self.assertIn("message_qps_ratio_below_0.8", row["open_blockers"])
        self.assertFalse(updated["status"]["performance_candidate"])
        self.assertFalse(updated["status"]["production_performance_parity"])


if __name__ == "__main__":
    unittest.main()
