#!/usr/bin/env python3
"""Unit coverage for MatrixArk dual-write ingestion benchmark."""

from __future__ import annotations

import argparse
import unittest

import matrixark_dual_write_ingestion_benchmark as bench


class MatrixArkDualWriteIngestionBenchmarkTest(unittest.TestCase):
    def make_args(self, **overrides):
        values = {
            "mode": "local",
            "records": 120,
            "workers": 3,
            "batch_size": 20,
            "payload_bytes": 16,
            "scope_key": "benchmark:unit",
            "local_write_delay_us": 0,
            "storage_prefix": "matrixark:mcp:bench-test",
            "raw_storage_prefix": "",
            "raw_backend": "temporalstore",
            "shard_size": 4096,
            "metaserver": "unused",
            "namespace": "unused",
            "table": "unused",
            "library_path": "",
            "request_timeout_ms": 1000,
            "io_timeout_ms": 1000,
            "min_ingestion_qps": 0.0,
            "max_batch_p95_ms": 0.0,
            "require_dual_write_counts": 0,
            "json_output": "",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_local_benchmark_measures_dual_write_before_return(self) -> None:
        summary = bench.run_benchmark(self.make_args())
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["records"], 120)
        self.assertGreater(summary["ingestion_qps"], 0)
        self.assertEqual(summary["raw_record_count_observed"], 120)
        self.assertGreater(summary["serving_log_entries_observed"], 0)
        self.assertTrue(summary["dual_write_counts_validated"])
        calls = summary["local_native_call_counts"]["calls_by_append_path"]
        raw_backends = summary["local_native_call_counts"]["calls_by_raw_backend"]
        self.assertEqual(summary["raw_backend"], "temporalstore")
        self.assertGreater(calls["matrixark_raw_ingestion_temporalstore_log"], 0)
        self.assertGreater(raw_backends["temporalstore"], 0)
        self.assertGreater(calls["native_append_queue"], 0)
        self.assertGreater(summary["caller_visible_batch_latency_ms"]["samples"], 0)
        self.assertFalse(summary["performance_gate"]["enabled"])

    def test_local_benchmark_can_enforce_release_gates(self) -> None:
        passing = bench.run_benchmark(self.make_args(
            min_ingestion_qps=1.0,
            max_batch_p95_ms=1000.0,
            require_dual_write_counts=1,
        ))
        self.assertEqual(passing["status"], "ok")
        self.assertTrue(passing["performance_gate"]["enabled"])
        self.assertTrue(passing["performance_gate"]["passed"])

        failing_qps = bench.run_benchmark(self.make_args(min_ingestion_qps=10**12))
        self.assertEqual(failing_qps["status"], "failed")
        self.assertFalse(failing_qps["performance_gate"]["passed"])

        failing_latency = bench.run_benchmark(self.make_args(local_write_delay_us=1000, max_batch_p95_ms=0.001))
        self.assertEqual(failing_latency["status"], "failed")
        failed_metrics = [check["metric"] for check in failing_latency["performance_gate"]["checks"] if not check["passed"]]
        self.assertIn("caller_visible_batch_latency_ms_p95", failed_metrics)

    def test_local_benchmark_can_label_matrixkv_raw_backend(self) -> None:
        summary = bench.run_benchmark(self.make_args(raw_backend="matrixkv"))
        self.assertTrue(summary["dual_write_counts_validated"])
        self.assertEqual(summary["raw_backend"], "matrixkv")
        calls = summary["local_native_call_counts"]["calls_by_append_path"]
        raw_backends = summary["local_native_call_counts"]["calls_by_raw_backend"]
        self.assertGreater(calls["matrixark_raw_ingestion_matrixkv_log"], 0)
        self.assertGreater(raw_backends["matrixkv"], 0)

    def test_rejects_invalid_record_count(self) -> None:
        with self.assertRaises(bench.BenchmarkError):
            bench.run_benchmark(self.make_args(records=0))

    def test_rejects_invalid_performance_thresholds(self) -> None:
        with self.assertRaises(bench.BenchmarkError):
            bench.run_benchmark(self.make_args(min_ingestion_qps=-1))
        with self.assertRaises(bench.BenchmarkError):
            bench.run_benchmark(self.make_args(max_batch_p95_ms=-1))

    def test_cli_returns_nonzero_when_gate_fails(self) -> None:
        rc = bench.main([
            "--mode=local",
            "--records=16",
            "--workers=1",
            "--batch-size=8",
            "--min-ingestion-qps=1000000000000",
        ])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
