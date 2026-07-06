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
        return Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", repo / "target/release/matrixark_rust_proxy"))

    def _compat_cli_path(self) -> Path:
        repo = Path(__file__).resolve().parents[1]
        return Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_COMPAT_CLI", repo / "target/release/matrixark_record_log"))

    def test_single_shot_mode_is_debug_only(self) -> None:
        cli_path = self._compat_cli_path()
        if not cli_path.exists():
            self.skipTest(f"Rust matrixark_rust_proxy binary is not built: {cli_path}")
        completed = subprocess.run(
            [str(cli_path)],
            input='{"op":"health","namespace":"deploy_ns","table":"deploy_table"}\n',
            text=True,
            capture_output=True,
            timeout=5,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("single-shot mode is debug-only", completed.stdout + completed.stderr)

    def test_rust_serve_mode_round_trips_readiness_hset_and_hget(self) -> None:
        cli_path = self._cli_path()
        if not cli_path.exists():
            self.skipTest(f"Rust matrixark_rust_proxy binary is not built: {cli_path}")

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
                count_key = "matrixark:test:rust-serve-readiness:count"
                client.matrixark_batch_append_records(
                    [
                        {"key": key, "field": "00000000000000000004", "value": "four"},
                        {"key": key, "field": "00000000000000000005", "value": "five"},
                    ],
                    count_key=count_key,
                    count_value="5",
                )
                self.assertEqual(client.hget(key, "00000000000000000004"), "four")
                self.assertEqual(client.hget(key, "00000000000000000005"), "five")
                scanned = client.scan_hash(key)
                self.assertTrue(scanned.get("native_prefix_scan"), scanned)
                scanned_values = {record.get("value") for record in scanned.get("records", [])}
                self.assertIn("four", scanned_values)
                self.assertIn("five", scanned_values)
                self.assertEqual(client.get_string(count_key), "5")
                scan = client.scan_hash(key)
                self.assertEqual(scan.get("count"), 5)
                self.assertGreaterEqual(scan.get("cached_clients", 0), 1)
                self.assertIn("00000000000000000001", scan.get("entries", {}))
                bad_response = client._call_json("not_a_real_op", raise_on_error=False)
                self.assertFalse(bad_response.get("ok"))
                self.assertEqual(bad_response.get("error_code"), "invalid_argument")
                self.assertFalse(bad_response.get("retryable"))
                readiness_after_writes = client.readiness()
                self.assertGreaterEqual(readiness_after_writes.get("cached_clients", 0), 1)
                metrics = client.metrics_prometheus()
                self.assertIn("matrixark_rust_record_log_commands_total", metrics)
                self.assertIn("matrixark_rust_record_log_records_written_total", metrics)
                self.assertIn("matrixark_rust_record_log_clients_created_total", metrics)
                self.assertIn('matrixark_backend_cached_clients{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_qps{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_errors_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_timeouts_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_command_latency_ms_bucket{backend="rust",le="100"}', metrics)
                self.assertIn('matrixark_backend_records_written_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_records_read_total{backend="rust"}', metrics)
                self.assertIn('matrixark_context_records_total{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_audit_buffered_records{backend="rust"}', metrics)
                self.assertIn('matrixark_backend_audit_flush_failures_total{backend="rust"}', metrics)
                client_metrics = client.metrics_snapshot()
                self.assertEqual(client_metrics.get("gateway_mode"), "rust_native_proxy")
                self.assertEqual(client_metrics.get("sdk_mode"), "rust_native_proxy")
                self.assertEqual(client_metrics.get("transport"), "stdio")
                self.assertTrue(client_metrics.get("shared_process_mode"))
                self.assertGreaterEqual(client_metrics.get("read_pool_size", 0), 1)
                self.assertEqual(
                    client_metrics.get("read_pool_enabled"),
                    client_metrics.get("read_pool_size", 0) > 1,
                )
                self.assertEqual(client_metrics.get("max_inflight"), 1)
                self.assertFalse(client_metrics.get("process_per_operation_enabled"))
                self.assertEqual(client_metrics.get("single_shot_mode"), "debug_only")
                self.assertFalse(client_metrics.get("direct_sdk_bridge"))
                self.assertTrue(client_metrics.get("supports_health"))
                self.assertTrue(client_metrics.get("supports_readiness"))
                self.assertTrue(client_metrics.get("supports_metrics"))
                self.assertTrue(client_metrics.get("supports_batch_append"))
                self.assertTrue(client_metrics.get("supports_prefix_scan"))
                self.assertTrue(client_metrics.get("supports_graceful_shutdown"))
                self.assertTrue(client_metrics.get("structured_errors"))
                self.assertGreaterEqual(client_metrics.get("commands_total", 0), 11)
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
