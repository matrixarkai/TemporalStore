#!/usr/bin/env python3
"""Tests for C++/Rust performance artifact audit reporting."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from audit_temporalstore_cpp_rust_performance_artifacts import audit_artifacts
from test_temporalstore_performance_evidence_import import _matrix, _report_with_bad_qps_ratio


class PerformanceArtifactAuditTest(unittest.TestCase):
    def test_blocked_artifact_reports_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            matrix = root / "matrix.json"
            report_dir = root / "run"
            report_dir.mkdir()
            report = report_dir / "comparison.json"
            execution = report_dir / "execution.json"
            matrix_data = _matrix()
            row = next(row for row in matrix_data["rows"] if row["workload"] == "10K_event_ingestion")
            row["next_run_hint"] = {
                "artifact_dir": "docs/benchmarks/parity_10K_event_ingestion",
                "comparison_path": "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                "recommended_execution_output": "docs/benchmarks/parity_10K_event_ingestion/execution.json",
                "command": [
                    "python",
                    "tools/run_matrixark_cpp_rust_scale_report.py",
                    "--events",
                    "10000",
                    "--backends",
                    "cpp",
                    "rust",
                    "--artifact-dir",
                    "docs/benchmarks/parity_10K_event_ingestion",
                    "--require-perf-parity",
                ],
                "import_command": [
                    "python",
                    "tools/import_temporalstore_cpp_rust_performance_evidence.py",
                    "--report",
                    "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                    "--validate",
                ],
                "required_same_config_fields": ["dataset", "storage_mode", "topology", "batch_size"],
                "required_result": ["matrix-provided required result"],
                "source": "matrix_fixture",
            }
            matrix.write_text(json.dumps(matrix_data), encoding="utf-8")
            report.write_text(json.dumps(_report_with_bad_qps_ratio()), encoding="utf-8")
            execution.write_text(
                json.dumps(
                    {
                        "schema": "temporalstore_cpp_rust_next_performance_execution_v1",
                        "continue_on_error": True,
                        "status": "failed",
                        "failed_count": 1,
                        "results": [
                            {
                                "step": "run_workload",
                                "workload": "1K_event_ingestion",
                                "reason": "blocked_no_importable",
                                "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py"],
                                "returncode": 124,
                                "status": "failed",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            audit = audit_artifacts(root, matrix)

        self.assertEqual(audit["reports_scanned"], 1)
        self.assertEqual(audit["execution_artifacts_scanned"], 1)
        self.assertEqual(audit["reports_with_candidate_workloads"], 1)
        self.assertEqual(audit["reports_with_importable_workloads"], 0)
        self.assertIn("10K_event_ingestion", audit["missing_required_workloads"])
        self.assertIn("1K_event_ingestion", audit["blocked_required_workloads"])
        coverage = audit["workload_coverage"]["1K_event_ingestion"]
        self.assertEqual(coverage["candidate_report_count"], 1)
        self.assertEqual(coverage["importable_report_count"], 0)
        self.assertIn("message_qps_ratio_below_0.8", coverage["blockers"])
        statuses = audit["required_workload_status"]
        self.assertEqual(statuses["1K_event_ingestion"]["status"], "blocked_no_importable")
        self.assertEqual(statuses["1K_event_ingestion"]["execution_attempt_count"], 1)
        self.assertEqual(statuses["1K_event_ingestion"]["last_execution_attempt"]["status"], "failed")
        self.assertEqual(
            statuses["1K_event_ingestion"]["last_execution_attempt"]["failed_steps"][0]["returncode"],
            124,
        )
        self.assertEqual(statuses["10K_event_ingestion"]["status"], "missing_candidate")
        self.assertIn("batch_size", statuses["1K_event_ingestion"]["next_run_hint"]["required_same_config_fields"])
        self.assertIn("selected_ref_parity=true", statuses["1K_event_ingestion"]["next_run_hint"]["required_result"])
        self.assertEqual(statuses["1K_event_ingestion"]["next_run_hint"]["source"], "audit_default")
        self.assertEqual(
            statuses["10K_event_ingestion"]["next_run_hint"]["required_result"],
            ["matrix-provided required result"],
        )
        self.assertEqual(statuses["10K_event_ingestion"]["next_run_hint"]["source"], "matrix_fixture")
        next_runs = audit["next_required_runs"]
        self.assertEqual(next_runs[0]["workload"], "10K_event_ingestion")
        self.assertEqual(next_runs[0]["reason"], "missing_candidate")
        self.assertEqual(next_runs[0]["artifact_dir"], "docs/benchmarks/parity_10K_event_ingestion")
        self.assertEqual(next_runs[0]["comparison_path"], "docs/benchmarks/parity_10K_event_ingestion/comparison.json")
        self.assertEqual(next_runs[0]["recommended_execution_output"], "docs/benchmarks/parity_10K_event_ingestion/execution.json")
        self.assertIn("--events", next_runs[0]["command"])
        self.assertIn("10000", next_runs[0]["command"])
        self.assertIn("--require-perf-parity", next_runs[0]["command"])
        self.assertNotIn("--require-phase-scale-matrix", next_runs[0]["command"])
        self.assertEqual(next_runs[0]["phase_scale_coverage_required"]["events"], [1000, 10000, 100000])
        self.assertEqual(next_runs[0]["phase_scale_coverage_required"]["retrieve_workers"], [4, 8, 16, 32])
        self.assertIn("large_pdf", next_runs[0]["phase_scale_coverage_required"]["resource_imports"])
        self.assertEqual(next_runs[0]["required_result"], ["matrix-provided required result"])
        self.assertEqual(
            next_runs[0]["import_command"],
            [
                "python",
                "tools/import_temporalstore_cpp_rust_performance_evidence.py",
                "--report",
                "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                "--validate",
            ],
        )
        self.assertEqual(next_runs[-1]["workload"], "1K_event_ingestion")
        self.assertEqual(next_runs[-1]["reason"], "blocked_no_importable")
        self.assertIn("message_qps_ratio_below_0.8", next_runs[-1]["blockers"])
        self.assertIn("1000", next_runs[-1]["command"])
        workflow = audit["next_required_workflow"]
        self.assertEqual(workflow["commands"][0]["step"], "run_workload")
        self.assertEqual(workflow["commands"][0]["workload"], "10K_event_ingestion")
        self.assertEqual(
            workflow["commands"][0]["recommended_execution_output"],
            "docs/benchmarks/parity_10K_event_ingestion/execution.json",
        )
        self.assertEqual(workflow["commands"][1]["step"], "import_evidence")
        self.assertEqual(workflow["commands"][1]["workload"], "10K_event_ingestion")
        self.assertIn(
            ["python", "tools/validate_temporalstore_cpp_rust_goal_parity.py"],
            workflow["post_import_validation"],
        )
        self.assertIn(
            ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
            workflow["post_import_validation"],
        )
        blocked = audit["entries"][0]["blocked_workloads"][0]
        self.assertEqual(blocked["workload"], "1K_event_ingestion")
        self.assertIn("message_qps_ratio_below_0.8", blocked["open_blockers"])


if __name__ == "__main__":
    unittest.main()
