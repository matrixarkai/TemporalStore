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
            "raw_backends": "both",
            "json_output": "",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_local_benchmark_covers_both_raw_backends(self) -> None:
        summary = bench.run_benchmark(self.make_args())
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["raw_backends"], ["temporalstore", "matrixkv"])
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
            self.assertGreater(result["full_shadow"]["qps"], 0)
            self.assertGreater(result["incremental_repair"]["qps"], 0)

    def test_local_benchmark_can_write_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "summary.json"
            summary = bench.run_benchmark(self.make_args(raw_backends="temporalstore", json_output=str(output)))
            self.assertTrue(output.exists())
            self.assertEqual(summary["raw_backends"], ["temporalstore"])
            self.assertIn("full_shadow_qps_avg", summary["qps_summary"])

    def test_rejects_invalid_record_count(self) -> None:
        with self.assertRaises(bench.BackfillBenchmarkError):
            bench.run_benchmark(self.make_args(records=0))


if __name__ == "__main__":
    unittest.main()
