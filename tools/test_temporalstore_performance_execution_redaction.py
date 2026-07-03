#!/usr/bin/env python3
"""Tests for parity execution artifact redaction validation."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from validate_temporalstore_performance_execution_redaction import validate_artifact


class PerformanceExecutionRedactionTest(unittest.TestCase):
    def test_accepts_redacted_execution_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "execution.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "temporalstore_cpp_rust_next_performance_execution_v1",
                        "results": [
                            {
                                "step": "run_workload",
                                "argv": [
                                    "wsl",
                                    "-d",
                                    "Ubuntu-22.04",
                                    "--cd",
                                    "<WORKSPACE_ROOT_WSL>",
                                    "--",
                                    "python3",
                                    "tool.py",
                                    "--cpp-lib",
                                    "<MATRIXARK_PARITY_CPP_LIB>",
                                    "--rust-cli=<MATRIXARK_PARITY_RUST_CLI>",
                                    "--require-perf-parity",
                                    "--require-phase-scale-matrix",
                                ]
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(validate_artifact(path), [])

    def test_rejects_local_paths_and_unredacted_backend_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "execution.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "temporalstore_cpp_rust_next_performance_execution_v1",
                        "results": [
                            {
                                "step": "run_workload",
                                "argv": [
                                    "wsl",
                                    "-d",
                                    "Ubuntu-22.04",
                                    "--cd",
                                    "/mnt/c/Users/Deeproute/private/repo",
                                    "--",
                                    "python3",
                                    "tool.py",
                                    "--cpp-lib",
                                    "/mnt/c/Users/Deeproute/libbcache2.so",
                                    "--require-perf-parity",
                                ]
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            failures = validate_artifact(path)

        self.assertTrue(any("unredacted local path marker" in failure for failure in failures))
        self.assertTrue(any("--cpp-lib value is not redacted" in failure for failure in failures))
        self.assertTrue(any("missing --require-phase-scale-matrix" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
