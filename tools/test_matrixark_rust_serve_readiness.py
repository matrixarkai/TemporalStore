#!/usr/bin/env python3
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkRustCliClient


class MatrixArkRustServeReadinessTest(unittest.TestCase):
    def test_rust_serve_mode_round_trips_readiness_hset_and_hget(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cli_path = Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", repo / "target/release/matrixark_record_log"))
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
            finally:
                client.shutdown()
                if old_root is None:
                    os.environ.pop("MATRIXARK_TEMPORALSTORE_RUST_ROOT", None)
                else:
                    os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = old_root


if __name__ == "__main__":
    unittest.main()
