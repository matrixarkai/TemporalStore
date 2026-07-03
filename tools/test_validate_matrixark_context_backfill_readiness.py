#!/usr/bin/env python3
"""Unit coverage for MatrixArk context backfill readiness validator."""

from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

import validate_matrixark_context_backfill_readiness as readiness


class MatrixArkContextBackfillReadinessTest(unittest.TestCase):
    def make_args(self, **overrides):
        values = {
            "records": 32,
            "batch_size": 8,
            "batch_sizes": "8,16",
            "incremental_records": 8,
            "payload_bytes": 8,
            "repeat": 1,
            "skip_local_benchmark": False,
            "skip_baseline_gate": False,
            "skip_cutover_gate": False,
            "skip_dead_letter_gate": False,
            "skip_source_scan_gate": False,
            "skip_partial_repair_gate": False,
            "skip_resume_gate": False,
            "skip_prometheus_gate": False,
            "json_output": "",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_static_readiness_checks_cover_public_surface(self) -> None:
        summary = readiness.run_readiness(self.make_args(skip_local_benchmark=True, skip_baseline_gate=True, skip_cutover_gate=True, skip_dead_letter_gate=True, skip_source_scan_gate=True, skip_partial_repair_gate=True, skip_resume_gate=True, skip_prometheus_gate=True))
        self.assertEqual(summary["status"], "ok")
        names = {item["name"]: item["passed"] for item in summary["checks"]}
        self.assertTrue(names["backfill_modes_cover_batch_and_incremental"])
        self.assertTrue(names["backfill_raw_backend_choices_cover_all_raw_options"])
        self.assertTrue(names["benchmark_has_batch_sweep_option"])
        self.assertTrue(names["benchmark_has_latency_gate_options"])
        self.assertTrue(names["benchmark_has_partial_repair_qps_gate"])
        self.assertTrue(names["benchmark_has_baseline_regression_gate"])
        self.assertTrue(names["run_backfill_reports_data_quality_status"])
        self.assertTrue(names["backfill_has_dry_run_target_check_option"])
        self.assertTrue(names["backfill_has_active_target_confirmation"])
        self.assertTrue(names["backfill_has_expect_active_prefix_precondition"])
        self.assertTrue(names["backfill_has_skip_validation_confirmation"])
        self.assertTrue(names["backfill_has_resume_range_change_confirmation"])
        self.assertTrue(names["manual_mentions_--batch-sizes"])
        self.assertTrue(names["manual_mentions_--baseline-json"])
        self.assertTrue(names["manual_mentions_--confirm-skip-validation"])
        self.assertTrue(names["manual_mentions_--confirm-resume-range-change"])
        self.assertTrue(names["manual_mentions_--confirm-active-target"])
        self.assertTrue(names["manual_mentions_--expect-active-prefix"])
        self.assertTrue(names["manual_mentions_--dry-run-check-target"])
        self.assertTrue(names["manual_mentions_promotion_readiness"])
        self.assertTrue(names["manual_mentions_matrixark_context_backfill_promotion_readiness_status"])
        self.assertTrue(names["manual_mentions_matrixark_context_backfill_data_quality_status"])
        self.assertTrue(names["manual_mentions_completed_with_errors"])
        self.assertEqual(summary["benchmark"], {})
        self.assertEqual(summary["baseline_gate"], {})
        self.assertEqual(summary["cutover_gate"], {})
        self.assertEqual(summary["dead_letter_gate"], {})
        self.assertEqual(summary["source_scan_gate"], {})
        self.assertEqual(summary["partial_repair_gate"], {})
        self.assertEqual(summary["resume_gate"], {})
        self.assertEqual(summary["prometheus_gate"], {})

    def test_local_readiness_gate_runs_batch_sweep_for_both_raw_backends(self) -> None:
        summary = readiness.run_readiness(self.make_args())
        self.assertEqual(summary["status"], "ok")
        names = {item["name"]: item["passed"] for item in summary["checks"]}
        self.assertTrue(names["local_benchmark_status_ok"])
        self.assertTrue(names["local_benchmark_gate_passed"])
        self.assertTrue(names["local_benchmark_baseline_gate_available"])
        self.assertTrue(names["local_benchmark_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["local_benchmark_exercised_batch_sweep"])
        self.assertTrue(names["local_benchmark_reports_partial_repair_recommendation"])
        self.assertEqual(summary["benchmark"]["batch_sizes"], [8, 16])
        self.assertEqual(set(summary["benchmark"]["raw_backends"]), {"temporalstore", "matrixkv"})
        self.assertTrue(names["baseline_gate_status_ok"])
        self.assertTrue(names["baseline_gate_baseline_artifact_written"])
        self.assertTrue(names["baseline_gate_candidate_enabled"])
        self.assertTrue(names["baseline_gate_candidate_passed"])
        self.assertTrue(names["baseline_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["baseline_gate_compared_qps_and_latency"])
        self.assertTrue(names["baseline_gate_exercised_batch_sweep"])
        self.assertEqual(set(summary["baseline_gate"]["raw_backends"]), {"temporalstore", "matrixkv"})
        self.assertTrue(names["cutover_gate_status_ok"])
        self.assertTrue(names["cutover_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["cutover_gate_shadow_wrote_records"])
        self.assertTrue(names["cutover_gate_activation_validated_shadow"])
        self.assertTrue(names["cutover_gate_activation_updates_active_pointer"])
        self.assertTrue(names["cutover_gate_activation_audit_written"])
        self.assertTrue(names["cutover_gate_rollback_restores_previous_pointer"])
        self.assertTrue(names["cutover_gate_rollback_audit_written"])
        self.assertEqual({item["raw_backend"] for item in summary["cutover_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertTrue(names["dead_letter_gate_status_ok"])
        self.assertTrue(names["dead_letter_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["dead_letter_gate_records_failure"])
        self.assertTrue(names["dead_letter_gate_marks_completed_with_errors"])
        self.assertTrue(names["dead_letter_gate_writes_dead_letter"])
        self.assertTrue(names["dead_letter_gate_continues_good_records"])
        self.assertTrue(names["dead_letter_gate_checkpoint_reaches_end"])
        self.assertTrue(names["dead_letter_gate_validation_rejects_shadow"])
        self.assertEqual({item["raw_backend"] for item in summary["dead_letter_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertTrue(names["source_scan_gate_status_ok"])
        self.assertTrue(names["source_scan_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["source_scan_gate_covers_record_count_record_index_scan_hash"])
        self.assertTrue(names["source_scan_gate_validates_shadow"])
        self.assertTrue(names["source_scan_gate_writes_all_records"])
        self.assertTrue(names["source_scan_gate_has_no_failures"])
        self.assertTrue(names["source_scan_gate_marks_scan_hash_estimated"])
        self.assertEqual({item["raw_backend"] for item in summary["source_scan_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertEqual({item["scan_mode"] for item in summary["source_scan_gate"]["results"]}, {"record_count", "record_index", "scan_hash"})
        self.assertTrue(names["partial_repair_gate_status_ok"])
        self.assertTrue(names["partial_repair_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["partial_repair_gate_filters_source_records"])
        self.assertTrue(names["partial_repair_gate_writes_expected_slice"])
        self.assertTrue(names["partial_repair_gate_validates_shadow"])
        self.assertTrue(names["partial_repair_gate_promotes_expected_slice"])
        self.assertTrue(names["partial_repair_gate_retry_is_idempotent"])
        self.assertTrue(names["partial_repair_gate_partial_matches_validation"])
        self.assertTrue(names["partial_repair_gate_audit_written"])
        self.assertEqual({item["raw_backend"] for item in summary["partial_repair_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertTrue(names["resume_gate_status_ok"])
        self.assertTrue(names["resume_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["resume_gate_checkpoint_found_on_second_run"])
        self.assertTrue(names["resume_gate_second_run_started_after_first_window"])
        self.assertTrue(names["resume_gate_blocks_incompatible_source_range"])
        self.assertTrue(names["resume_gate_confirmed_range_change_ignores_checkpoint"])
        self.assertTrue(names["resume_gate_confirmed_range_change_scans_requested_window"])
        self.assertTrue(names["resume_gate_confirmed_range_change_is_idempotent"])
        self.assertTrue(names["resume_gate_confirmed_range_change_has_no_failures"])
        self.assertTrue(names["resume_gate_completed_expected_records"])
        self.assertTrue(names["resume_gate_fingerprint_match"])
        self.assertEqual({item["raw_backend"] for item in summary["resume_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertTrue(names["prometheus_gate_status_ok"])
        self.assertTrue(names["prometheus_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["prometheus_gate_shadow_metrics_present"])
        self.assertTrue(names["prometheus_gate_validation_metrics_present"])
        self.assertTrue(names["prometheus_gate_incremental_repair_metrics_present"])
        self.assertTrue(names["prometheus_gate_emitted_samples"])
        self.assertEqual({item["raw_backend"] for item in summary["prometheus_gate"]["results"]}, {"temporalstore", "matrixkv"})

    def test_main_writes_json_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "readiness.json"
            rc = readiness.main([
                "--records=16",
                "--batch-size=8",
                "--batch-sizes=8,16",
                "--incremental-records=4",
                "--repeat=1",
                f"--json-output={output}",
            ])
            self.assertEqual(rc, 0)
            self.assertTrue(output.exists())
            self.assertIn("local_benchmark_gate_passed", output.read_text(encoding="utf-8"))
            self.assertIn("resume_gate_checkpoint_found_on_second_run", output.read_text(encoding="utf-8"))
            self.assertIn("resume_gate_blocks_incompatible_source_range", output.read_text(encoding="utf-8"))
            self.assertIn("resume_gate_confirmed_range_change_scans_requested_window", output.read_text(encoding="utf-8"))
            self.assertIn("prometheus_gate_incremental_repair_metrics_present", output.read_text(encoding="utf-8"))
            self.assertIn("prometheus_gate_validation_metrics_present", output.read_text(encoding="utf-8"))
            self.assertIn("local_benchmark_baseline_gate_available", output.read_text(encoding="utf-8"))
            self.assertIn("baseline_gate_candidate_passed", output.read_text(encoding="utf-8"))
            self.assertIn("cutover_gate_rollback_restores_previous_pointer", output.read_text(encoding="utf-8"))
            self.assertIn("dead_letter_gate_validation_rejects_shadow", output.read_text(encoding="utf-8"))
            self.assertIn("source_scan_gate_covers_record_count_record_index_scan_hash", output.read_text(encoding="utf-8"))
            self.assertIn("partial_repair_gate_partial_matches_validation", output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
