#!/usr/bin/env python3
"""Unit coverage for MatrixArk context backfill benchmark."""

from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

import matrixark_context_backfill_benchmark as bench


class MatrixArkContextBackfillBenchmarkTest(unittest.TestCase):
    def make_args(self, **overrides):
        values = {
            "records": 64,
            "batch_size": 16,
            "payload_bytes": 8,
            "incremental_records": 16,
            "repeat": 1,
            "raw_backends": "both",
            "min_full_shadow_qps": 0.0,
            "min_incremental_repair_qps": 0.0,
            "min_backend_qps_ratio": 0.0,
            "gate_aggregation": "min",
            "json_output": "",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_local_benchmark_covers_both_raw_backends(self) -> None:
        summary = bench.run_benchmark(self.make_args())
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["raw_backends"], ["temporalstore", "matrixkv"])
        self.assertEqual(summary["repeat"], 1)
        self.assertEqual(len(summary["results"]), 2)
        by_backend = {item["raw_backend"]: item for item in summary["results"]}
        self.assertEqual(set(by_backend), {"temporalstore", "matrixkv"})
        for backend, result in by_backend.items():
            self.assertEqual(result["full_shadow"]["summary"]["raw_backend"], backend)
            self.assertEqual(result["full_shadow"]["summary"]["metrics"]["scanned"], 64)
            self.assertEqual(result["full_shadow"]["summary"]["metrics"]["written"], 64)
            self.assertEqual(result["incremental_shadow"]["summary"]["metrics"]["written"], 16)
            self.assertEqual(result["incremental_repair"]["summary"]["raw_backend"], backend)
            self.assertEqual(result["incremental_repair"]["summary"]["promotion"]["metrics"]["written"], 16)
            self.assertEqual(result["repeat_index"], 1)
            self.assertGreater(result["full_shadow"]["qps"], 0)
            self.assertGreater(result["incremental_repair"]["qps"], 0)
        self.assertIn("full_shadow_qps", summary["qps_summary"])
        self.assertIn("incremental_shadow_qps", summary["qps_summary"])
        self.assertIn("incremental_repair_qps", summary["qps_summary"])
        self.assertGreater(summary["qps_summary"]["full_shadow_qps"]["min_max_ratio"], 0)

    def test_local_benchmark_can_write_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "summary.json"
            summary = bench.run_benchmark(self.make_args(raw_backends="temporalstore", json_output=str(output)))
            self.assertTrue(output.exists())
            self.assertEqual(summary["raw_backends"], ["temporalstore"])
            self.assertIn("full_shadow_qps_avg", summary["qps_summary"])
            self.assertIn("incremental_shadow_qps_avg", summary["qps_summary"])

    def test_local_benchmark_can_repeat_samples(self) -> None:
        summary = bench.run_benchmark(self.make_args(records=16, incremental_records=4, raw_backends="temporalstore", repeat=2))
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["repeat"], 2)
        self.assertEqual(summary["raw_backends"], ["temporalstore"])
        self.assertEqual([result["repeat_index"] for result in summary["results"]], [1, 2])
        self.assertEqual(summary["performance_gate"]["gate_aggregation"], "min")
        self.assertEqual(len(summary["performance_gate"]["checks"]), 2)
        self.assertTrue(all(check["samples"] == 2 for check in summary["performance_gate"]["checks"]))

    def test_local_benchmark_can_gate_each_sample(self) -> None:
        summary = bench.run_benchmark(self.make_args(
            records=16,
            incremental_records=4,
            raw_backends="temporalstore",
            repeat=2,
            gate_aggregation="sample",
        ))
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["performance_gate"]["gate_aggregation"], "sample")
        self.assertEqual(len(summary["performance_gate"]["checks"]), 4)
        self.assertEqual([check["repeat_index"] for check in summary["performance_gate"]["checks"]], [1, 1, 2, 2])

    def test_performance_gate_passes_and_fails_thresholds(self) -> None:
        passing = bench.run_benchmark(self.make_args(records=32, incremental_records=8, min_full_shadow_qps=1.0, min_incremental_repair_qps=1.0))
        self.assertEqual(passing["status"], "ok")
        self.assertTrue(passing["performance_gate"]["enabled"])
        self.assertTrue(passing["performance_gate"]["passed"])

        parity = bench.run_benchmark(self.make_args(records=32, incremental_records=8, min_backend_qps_ratio=0.000001))
        self.assertEqual(parity["status"], "ok")
        self.assertTrue(parity["performance_gate"]["enabled"])
        self.assertEqual(parity["performance_gate"]["gate_aggregation"], "min")
        parity_checks = [check for check in parity["performance_gate"]["checks"] if check["metric"].endswith("_qps_ratio")]
        self.assertEqual(len(parity_checks), 3)

        failing = bench.run_benchmark(self.make_args(records=32, incremental_records=8, raw_backends="temporalstore", min_full_shadow_qps=10**12))
        self.assertEqual(failing["status"], "failed")
        self.assertFalse(failing["performance_gate"]["passed"])
        failed_checks = [check for check in failing["performance_gate"]["checks"] if not check["passed"]]
        self.assertEqual(len(failed_checks), 1)
        self.assertEqual(failed_checks[0]["metric"], "full_shadow_qps")

    def test_cli_returns_nonzero_when_performance_gate_fails(self) -> None:
        rc = bench.main([
            "--records=16",
            "--batch-size=8",
            "--incremental-records=4",
            "--raw-backends=temporalstore",
            "--min-full-shadow-qps=1000000000000",
        ])
        self.assertEqual(rc, 2)

    def test_rejects_invalid_record_count(self) -> None:
        with self.assertRaises(bench.BackfillBenchmarkError):
            bench.run_benchmark(self.make_args(records=0))

    def test_rejects_invalid_repeat_count(self) -> None:
        with self.assertRaises(bench.BackfillBenchmarkError):
            bench.run_benchmark(self.make_args(repeat=0))

    def test_rejects_invalid_backend_ratio(self) -> None:
        with self.assertRaises(bench.BackfillBenchmarkError):
            bench.run_benchmark(self.make_args(min_backend_qps_ratio=1.01))

    def test_rejects_invalid_gate_aggregation(self) -> None:
        with self.assertRaises(bench.BackfillBenchmarkError):
            bench.run_benchmark(self.make_args(gate_aggregation="median"))


if __name__ == "__main__":
    unittest.main()
