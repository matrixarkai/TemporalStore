#!/usr/bin/env python3
"""Unit tests for C++/Rust performance parity matrix validation helpers."""

from __future__ import annotations

import unittest

from validate_temporalstore_cpp_rust_performance_parity import (
    REQUIRED_STORAGE_MODE_MATRIX,
    _exceeds_limit,
    _validate_global_blocker_ledger,
    _validate_completed_same_config,
    _validate_metric_block,
    _validate_ratios,
    _validate_storage_mode_matrix_hint,
    _validate_source_report,
)
from validate_storage_tuning_parity import EXPECTED_DEFAULTS as STORAGE_TUNING


class PerformanceParityValidatorTest(unittest.TestCase):
    def test_limit_allows_values_below_or_equal_threshold(self) -> None:
        self.assertFalse(_exceeds_limit(0, 2))
        self.assertFalse(_exceeds_limit(1, 2))
        self.assertFalse(_exceeds_limit(2, 2))

    def test_limit_blocks_values_above_threshold(self) -> None:
        self.assertTrue(_exceeds_limit(3, 2))

    def test_limit_ignores_missing_or_non_numeric_values(self) -> None:
        self.assertFalse(_exceeds_limit(None, 0))
        self.assertFalse(_exceeds_limit("unknown", 0))
        self.assertFalse(_exceeds_limit(True, 0))

    def test_selected_ref_parity_is_required_by_default(self) -> None:
        failures: list[str] = []
        row = {"workload": "1K_event_ingestion", "cpp": {"selected_ref_parity": False}}

        _validate_metric_block(row, "cpp", failures)

        self.assertIn("1K_event_ingestion cpp.selected_ref_parity must be true", failures)

    def test_selected_ref_parity_can_follow_threshold_policy(self) -> None:
        failures: list[str] = []
        row = {"workload": "1K_event_ingestion", "cpp": {"selected_ref_parity": False}}

        _validate_metric_block(row, "cpp", failures, require_selected_ref_parity=False)

        self.assertNotIn("1K_event_ingestion cpp.selected_ref_parity must be true", failures)

    def test_watermarks_must_show_valid_lifecycle_progress(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "cpp": {
                "append_watermark": 0,
                "compaction_watermark": 1,
                "selected_ref_parity": True,
            },
        }

        _validate_metric_block(row, "cpp", failures)

        self.assertIn("1K_event_ingestion cpp.append_watermark must be positive", failures)
        self.assertIn(
            "1K_event_ingestion cpp.compaction_watermark cannot exceed append_watermark",
            failures,
        )

    def test_qps_latency_and_cache_hit_rate_have_real_bounds(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "cpp": {
                "message_qps": 0,
                "retrieve_qps": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "p99_ms": 0,
                "cache_hit_rate": 1.1,
                "append_watermark": 1,
                "compaction_watermark": 0,
                "selected_ref_parity": True,
            },
        }

        _validate_metric_block(row, "cpp", failures)

        self.assertIn("1K_event_ingestion cpp.message_qps must be positive", failures)
        self.assertIn("1K_event_ingestion cpp.retrieve_qps must be positive", failures)
        self.assertIn("1K_event_ingestion cpp.p50_ms must be positive", failures)
        self.assertIn("1K_event_ingestion cpp.p95_ms must be positive", failures)
        self.assertIn("1K_event_ingestion cpp.p99_ms must be positive", failures)
        self.assertIn("1K_event_ingestion cpp.cache_hit_rate must be <= 1", failures)

    def test_qps_ratios_are_required_for_completed_rows(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "ratios": {
                "p50_ratio": 1.0,
                "p95_ratio": 1.0,
                "p99_ratio": 1.0,
            },
        }

        _validate_ratios(row, {"min_rust_cpp_qps_ratio": 0.8, "max_rust_cpp_latency_ratio": 2.0}, failures)

        self.assertIn("1K_event_ingestion message_qps_ratio missing", failures)
        self.assertIn("1K_event_ingestion retrieve_qps_ratio missing", failures)

    def test_qps_ratios_below_threshold_are_rejected(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "ratios": {
                "message_qps_ratio": 0.79,
                "retrieve_qps_ratio": 0.8,
                "p50_ratio": 1.0,
                "p95_ratio": 1.0,
                "p99_ratio": 1.0,
            },
        }

        _validate_ratios(row, {"min_rust_cpp_qps_ratio": 0.8, "max_rust_cpp_latency_ratio": 2.0}, failures)

        self.assertIn("1K_event_ingestion message_qps_ratio below 0.8", failures)
        self.assertNotIn("1K_event_ingestion retrieve_qps_ratio below 0.8", failures)

    def test_completed_same_config_rejects_drift_from_required_run_policy(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "dataset": "other",
            "storage_mode": "shared_store",
            "topology": {"metaserver": "127.0.0.1:18000"},
            "batch_size": 10,
            "token_budget": 12000,
            "embedding_model": "matrixark-local-token-hash-v1",
            "reader_model": "matrixark-deterministic-reader",
            "judge_model": "matrixark-deterministic-judge",
            "storage_tuning": dict(STORAGE_TUNING),
        }

        _validate_completed_same_config(row, failures)

        self.assertIn(
            "1K_event_ingestion same-config field `dataset` drift: expected "
            "'matrixark-scale-synthetic' got 'other'",
            failures,
        )
        self.assertIn(
            "1K_event_ingestion same-config field `batch_size` drift: expected 20 got 10",
            failures,
        )

    def test_completed_same_config_rejects_missing_or_drifted_storage_tuning(self) -> None:
        failures: list[str] = []
        tuning = dict(STORAGE_TUNING)
        del tuning["TS_BLOCK_INDEX_CACHE_BYTES"]
        tuning["TS_STORAGE_ZONE_SIZE"] = 123
        row = {
            "workload": "1K_event_ingestion",
            "dataset": "matrixark-scale-synthetic",
            "storage_mode": "shared_store",
            "topology": {"metaserver": "127.0.0.1:18000"},
            "batch_size": 20,
            "token_budget": 12000,
            "embedding_model": "matrixark-local-token-hash-v1",
            "reader_model": "matrixark-deterministic-reader",
            "judge_model": "matrixark-deterministic-judge",
            "storage_tuning": tuning,
        }

        _validate_completed_same_config(row, failures)

        self.assertIn("1K_event_ingestion storage_tuning missing `TS_BLOCK_INDEX_CACHE_BYTES`", failures)
        self.assertIn(
            "1K_event_ingestion storage_tuning `TS_STORAGE_ZONE_SIZE` drift: expected 10485760 got 123",
            failures,
        )

    def test_completed_rows_require_canonical_source_report(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "source_report": "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
        }

        _validate_source_report(row, failures)

        self.assertIn(
            "1K_event_ingestion source_report must be "
            "docs/benchmarks/parity_1K_event_ingestion/comparison.json",
            failures,
        )

    def test_global_blocker_ledger_must_cover_every_row_blocker(self) -> None:
        failures: list[str] = []
        status = {"open_blockers": ["1K_event_ingestion:cpp_backend_not_passed"]}
        rows = {
            "1K_event_ingestion": {
                "workload": "1K_event_ingestion",
                "open_blockers": ["cpp_backend_not_passed", "rust_backend_not_passed"],
            }
        }

        _validate_global_blocker_ledger(status, rows, failures)

        self.assertIn("global open_blockers missing `1K_event_ingestion:rust_backend_not_passed`", failures)

    def test_blocker_ledgers_must_be_unique(self) -> None:
        failures: list[str] = []
        status = {
            "open_blockers": [
                "1K_event_ingestion:cpp_backend_not_passed",
                "1K_event_ingestion:cpp_backend_not_passed",
            ]
        }
        rows = {
            "1K_event_ingestion": {
                "workload": "1K_event_ingestion",
                "open_blockers": ["cpp_backend_not_passed", "cpp_backend_not_passed"],
            }
        }

        _validate_global_blocker_ledger(status, rows, failures)

        self.assertIn("global open_blockers must be unique", failures)
        self.assertIn("1K_event_ingestion open_blockers must be unique", failures)

    def test_missing_evidence_hint_requires_full_storage_mode_matrix(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "retrieve_workers_4",
            "next_run_hint": {
                "storage_mode_matrix": {
                    "shared_store_async": {
                        "storage_family": "shared_store",
                        "write_mode": "async",
                        "replication_mode": "shared_store",
                        "comparison_path": "docs/benchmarks/parity_retrieve_workers_4/shared_store_async/comparison.json",
                        "command": [
                            "python",
                            "tools/run_matrixark_cpp_rust_scale_report.py",
                            "--retrieve-workers",
                            "4",
                            "--backends",
                            "cpp",
                            "rust",
                            "--artifact-dir",
                            "docs/benchmarks/parity_retrieve_workers_4/shared_store_async",
                            "--dataset",
                            "matrixark-scale-synthetic",
                            "--messages-per-ingest",
                            "20",
                            "--max-context-tokens",
                            "12000",
                            "--embedding-model",
                            "matrixark-local-token-hash-v1",
                            "--reader-model",
                            "matrixark-deterministic-reader",
                            "--judge-model",
                            "matrixark-deterministic-judge",
                            "--metaserver",
                            "127.0.0.1:18000",
                            "--namespace",
                            "deploy_ns",
                            "--table",
                            "deploy_table",
                            "--storage-family",
                            "shared_store",
                            "--storage-mode",
                            "multi_node",
                            "--write-mode",
                            "async",
                            "--oplog-mode",
                            "async",
                            "--replication-mode",
                            "shared_store",
                            "--require-perf-parity",
                            "--require-phase-scale-matrix",
                        ],
                    }
                }
            },
        }

        _validate_storage_mode_matrix_hint(row, failures)

        for mode_name in REQUIRED_STORAGE_MODE_MATRIX:
            if mode_name == "shared_store_async":
                continue
            self.assertIn(
                f"retrieve_workers_4 next_run_hint.storage_mode_matrix missing `{mode_name}`",
                failures,
            )

    def test_storage_mode_matrix_rejects_mode_command_drift(self) -> None:
        failures: list[str] = []
        row = {
            "workload": "1K_event_ingestion",
            "next_run_hint": {
                "storage_mode_matrix": {
                    "raft_sync": {
                        "storage_family": "raft",
                        "write_mode": "sync",
                        "replication_mode": "raft",
                        "comparison_path": "docs/benchmarks/parity_1K_event_ingestion/raft_sync/comparison.json",
                        "command": [
                            "python",
                            "tools/run_matrixark_cpp_rust_scale_report.py",
                            "--events",
                            "1000",
                            "--backends",
                            "cpp",
                            "rust",
                            "--artifact-dir",
                            "docs/benchmarks/parity_1K_event_ingestion/raft_sync",
                            "--dataset",
                            "matrixark-scale-synthetic",
                            "--messages-per-ingest",
                            "20",
                            "--max-context-tokens",
                            "12000",
                            "--embedding-model",
                            "matrixark-local-token-hash-v1",
                            "--reader-model",
                            "matrixark-deterministic-reader",
                            "--judge-model",
                            "matrixark-deterministic-judge",
                            "--metaserver",
                            "127.0.0.1:18000",
                            "--namespace",
                            "deploy_ns",
                            "--table",
                            "deploy_table",
                            "--storage-family",
                            "raft",
                            "--storage-mode",
                            "multi_node",
                            "--write-mode",
                            "async",
                            "--oplog-mode",
                            "sync",
                            "--replication-mode",
                            "raft",
                            "--require-perf-parity",
                            "--require-phase-scale-matrix",
                        ],
                    }
                }
            },
        }

        _validate_storage_mode_matrix_hint(row, failures)

        self.assertIn(
            "1K_event_ingestion next_run_hint.storage_mode_matrix.raft_sync.command --write-mode drift: expected 'sync' got 'async'",
            failures,
        )


if __name__ == "__main__":
    unittest.main()
