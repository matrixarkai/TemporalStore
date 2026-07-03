#!/usr/bin/env python3
"""Tests for parity execution artifact redaction validation."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from validate_temporalstore_performance_execution_redaction import (
    REQUIRED_PHASE_SCALE_COVERAGE,
    validate_artifact,
)


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
                                "phase_scale_coverage_required": REQUIRED_PHASE_SCALE_COVERAGE,
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
                            },
                            {
                                "step": "import_evidence",
                                "phase_scale_coverage_required": REQUIRED_PHASE_SCALE_COVERAGE,
                                "argv": ["python", "tools/import_temporalstore_cpp_rust_performance_evidence.py"],
                                "status": "passed",
                            },
                            {
                                "step": "post_import_validation",
                                "argv": ["python", "tools/validate_temporalstore_cpp_rust_goal_parity.py"],
                                "status": "passed",
                            },
                            {
                                "step": "post_import_validation",
                                "argv": ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
                                "status": "passed",
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
                            },
                            {
                                "step": "import_evidence",
                                "argv": ["python", "tools/import_temporalstore_cpp_rust_performance_evidence.py"],
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
        self.assertTrue(any("missing phase_scale_coverage_required" in failure for failure in failures))

    def test_rejects_successful_import_without_post_validators(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "execution.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "temporalstore_cpp_rust_next_performance_execution_v1",
                        "results": [
                            {
                                "step": "run_workload",
                                "phase_scale_coverage_required": REQUIRED_PHASE_SCALE_COVERAGE,
                                "argv": [
                                    "python",
                                    "tools/run_matrixark_cpp_rust_scale_report.py",
                                    "--require-perf-parity",
                                    "--require-phase-scale-matrix",
                                ],
                                "status": "passed",
                            },
                            {
                                "step": "import_evidence",
                                "phase_scale_coverage_required": REQUIRED_PHASE_SCALE_COVERAGE,
                                "argv": ["python", "tools/import_temporalstore_cpp_rust_performance_evidence.py"],
                                "status": "passed",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            failures = validate_artifact(path)

        self.assertTrue(any("validate_temporalstore_cpp_rust_goal_parity.py" in failure for failure in failures))
        self.assertTrue(any("validate_storage_engine_9_phase_parity.py" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
