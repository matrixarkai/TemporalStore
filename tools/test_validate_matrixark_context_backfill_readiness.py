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
            "skip_resume_gate": False,
            "skip_prometheus_gate": False,
            "json_output": "",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_static_readiness_checks_cover_public_surface(self) -> None:
        summary = readiness.run_readiness(self.make_args(skip_local_benchmark=True, skip_resume_gate=True, skip_prometheus_gate=True))
        self.assertEqual(summary["status"], "ok")
        names = {item["name"]: item["passed"] for item in summary["checks"]}
        self.assertTrue(names["backfill_modes_cover_batch_and_incremental"])
        self.assertTrue(names["backfill_raw_backend_choices_cover_all_raw_options"])
        self.assertTrue(names["benchmark_has_batch_sweep_option"])
        self.assertTrue(names["benchmark_has_latency_gate_options"])
        self.assertTrue(names["benchmark_has_baseline_regression_gate"])
        self.assertTrue(names["manual_mentions_--batch-sizes"])
        self.assertTrue(names["manual_mentions_--baseline-json"])
        self.assertEqual(summary["benchmark"], {})
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
        self.assertEqual(summary["benchmark"]["batch_sizes"], [8, 16])
        self.assertEqual(set(summary["benchmark"]["raw_backends"]), {"temporalstore", "matrixkv"})
        self.assertTrue(names["resume_gate_status_ok"])
        self.assertTrue(names["resume_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["resume_gate_checkpoint_found_on_second_run"])
        self.assertTrue(names["resume_gate_second_run_started_after_first_window"])
        self.assertTrue(names["resume_gate_completed_expected_records"])
        self.assertTrue(names["resume_gate_fingerprint_match"])
        self.assertEqual({item["raw_backend"] for item in summary["resume_gate"]["results"]}, {"temporalstore", "matrixkv"})
        self.assertTrue(names["prometheus_gate_status_ok"])
        self.assertTrue(names["prometheus_gate_covers_temporalstore_and_matrixkv"])
        self.assertTrue(names["prometheus_gate_shadow_metrics_present"])
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
            self.assertIn("prometheus_gate_incremental_repair_metrics_present", output.read_text(encoding="utf-8"))
            self.assertIn("local_benchmark_baseline_gate_available", output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
