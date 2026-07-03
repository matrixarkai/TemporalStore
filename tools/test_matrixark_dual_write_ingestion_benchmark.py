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


if __name__ == "__main__":
    unittest.main()
