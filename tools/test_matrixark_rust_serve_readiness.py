#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkRustCliClient


class MatrixArkRustServeReadinessTest(unittest.TestCase):
    def _cli_path(self) -> Path:
        repo = Path(__file__).resolve().parents[1]
        return Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", repo / "target/release/matrixark_record_log"))

    def test_single_shot_mode_is_debug_only(self) -> None:
        cli_path = self._cli_path()
        if not cli_path.exists():
            self.skipTest(f"Rust matrixark_record_log binary is not built: {cli_path}")
        completed = subprocess.run(
            [str(cli_path)],
            input='{"op":"health","namespace":"deploy_ns","table":"deploy_table"}\n',
            text=True,
            capture_output=True,
            timeout=5,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("single-shot mode is debug-only", completed.stderr)

    def test_rust_serve_mode_round_trips_readiness_hset_and_hget(self) -> None:
        cli_path = self._cli_path()
        if not cli_path.exists():
            self.skipTest(f"Rust matrixark_record_log binary is not built: {cli_path}")

        with tempfile.TemporaryDirectory(prefix="matrixark-rust-serve-readiness-") as tmpdir:
            old_root = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT")
            os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = tmpdir
            client = MatrixArkRustCliClient(
                cli_path=str(cli_path),
                metaserver="127.0.0.1:18000",
                namespace="deploy_ns",
                table="deploy_table",
                request_timeout_ms=10000,
                io_timeout_ms=10000,
            )
            try:
                readiness = client.readiness()
                self.assertTrue(readiness.get("ok"), readiness)
                key = "matrixark:test:rust-serve-readiness"
                field = "00000000000000000001"
                value = '{"record_type":"readiness_probe","backend":"rust"}'
                client.hset(key, field, value)
                self.assertEqual(client.hget(key, field), value)
                client.batch_hset(
                    [
                        {"key": key, "field": "00000000000000000002", "value": "two"},
                        {"key": key, "field": "00000000000000000003", "value": "three"},
                    ]
                )
                records = client.batch_hget(
                    [
                        {"key": key, "field": "00000000000000000002"},
                        {"key": key, "field": "00000000000000000003"},
                    ]
                )
                self.assertEqual([record.get("value") for record in records], ["two", "three"])
                scan = client.scan_hash(key)
                self.assertEqual(scan.get("count"), 3)
                self.assertGreaterEqual(scan.get("cached_clients", 0), 1)
                self.assertIn("00000000000000000001", scan.get("entries", {}))
                readiness_after_writes = client.readiness()
                self.assertGreaterEqual(readiness_after_writes.get("cached_clients", 0), 1)
                metrics = client.metrics_prometheus()
                self.assertIn("matrixark_rust_record_log_commands_total", metrics)
                self.assertIn("matrixark_rust_record_log_records_written_total", metrics)
                self.assertIn("matrixark_rust_record_log_cached_clients", metrics)
                self.assertIn('matrixark_backend_cached_clients{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_qps{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_errors_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_timeouts_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_command_latency_ms_bucket{backend="rust",le="100"}', metrics)
                client_metrics = client.metrics_snapshot()
                self.assertEqual(client_metrics.get("gateway_mode"), "long_lived_stdio_gateway")
                self.assertEqual(client_metrics.get("transport"), "stdio")
                self.assertGreaterEqual(client_metrics.get("commands_total", 0), 7)
                self.assertGreaterEqual(client_metrics.get("qps", 0), 0)
                self.assertGreaterEqual(client_metrics.get("p95_latency_ms", 0), 0)
                self.assertGreaterEqual(client_metrics.get("p99_latency_ms", 0), 0)
                self.assertEqual(client_metrics.get("timeouts_total"), 0)
                self.assertEqual(client_metrics.get("backpressure_rejections_total"), 0)
            finally:
                client.shutdown()
                if old_root is None:
                    os.environ.pop("MATRIXARK_TEMPORALSTORE_RUST_ROOT", None)
                else:
                    os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = old_root


if __name__ == "__main__":
    unittest.main()
