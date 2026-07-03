#!/usr/bin/env python3
"""Tests for the next C++/Rust performance workflow runner."""

from __future__ import annotations

import sys
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from run_temporalstore_cpp_rust_next_performance_workflow import DEFAULT_WSL_DISTRO, _pythonize, _wslize, build_execution_plan, default_execution_output, run_plan


class NextPerformanceWorkflowTest(unittest.TestCase):
    def test_plan_limits_workloads_and_keeps_post_validators(self) -> None:
        audit = {
            "next_required_runs": [
                {"workload": "10K_event_ingestion"},
                {"workload": "100K_event_ingestion"},
            ],
            "next_required_workflow": {
                "commands": [
                    {
                        "step": "run_workload",
                        "workload": "10K_event_ingestion",
                        "reason": "missing_candidate",
                        "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py"],
                        "recommended_execution_output": "docs/benchmarks/parity_10K_event_ingestion/execution.json",
                    },
                    {
                        "step": "import_evidence",
                        "workload": "10K_event_ingestion",
                        "reason": "missing_candidate",
                        "argv": ["python", "tools/import_temporalstore_cpp_rust_performance_evidence.py"],
                    },
                    {
                        "step": "run_workload",
                        "workload": "100K_event_ingestion",
                        "reason": "missing_candidate",
                        "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py", "--events", "100000"],
                    },
                ],
                "post_import_validation": [
                    ["python", "tools/validate_temporalstore_cpp_rust_goal_parity.py"],
                    ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
                ],
            },
        }

        plan = build_execution_plan(audit, max_workloads=1)

        self.assertEqual(plan["schema"], "temporalstore_cpp_rust_next_performance_workflow_v1")
        self.assertTrue(plan["dry_run_default"])
        self.assertEqual(plan["workload_count"], 1)
        self.assertEqual([command["workload"] for command in plan["commands"]], ["10K_event_ingestion", "10K_event_ingestion"])
        self.assertEqual(
            plan["commands"][0]["recommended_execution_output"],
            "docs/benchmarks/parity_10K_event_ingestion/execution.json",
        )
        self.assertEqual(plan["commands"][0]["wsl_argv"][:3], ["wsl", "-d", DEFAULT_WSL_DISTRO])
        self.assertIn("python3", plan["commands"][0]["wsl_argv"])
        self.assertNotIn("wsl_argv", plan["commands"][1])
        self.assertIn(
            ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
            plan["post_import_validation"],
        )
        self.assertEqual(
            default_execution_output(plan),
            Path(__file__).resolve().parents[1] / "docs/benchmarks/parity_10K_event_ingestion/execution.json",
        )

    def test_default_execution_output_ignores_import_only_plans(self) -> None:
        self.assertIsNone(
            default_execution_output(
                {
                    "commands": [
                        {
                            "step": "import_evidence",
                            "workload": "10K_event_ingestion",
                            "recommended_execution_output": "docs/benchmarks/parity_10K_event_ingestion/execution.json",
                        }
                    ]
                }
            )
        )

    def test_pythonize_uses_current_interpreter(self) -> None:
        self.assertEqual(_pythonize(["python", "tool.py", "--x"]), [sys.executable, "tool.py", "--x"])
        self.assertEqual(_pythonize(["custom-python", "tool.py"]), ["custom-python", "tool.py"])

    def test_wslize_wraps_python_workload_command(self) -> None:
        command = _wslize(["python", "tools/run_matrixark_cpp_rust_scale_report.py"], distro="Ubuntu-22.04")
        self.assertEqual(command[:3], ["wsl", "-d", "Ubuntu-22.04"])
        self.assertIn("--cd", command)
        self.assertEqual(command[-2:], ["python3", "tools/run_matrixark_cpp_rust_scale_report.py"])

    def test_run_plan_can_continue_after_failure(self) -> None:
        plan = {
            "commands": [
                {"step": "run_workload", "workload": "10K", "reason": "missing", "argv": ["python", "first.py"]},
                {"step": "import_evidence", "workload": "10K", "reason": "missing", "argv": ["python", "second.py"]},
            ],
            "post_import_validation": [["python", "validator.py"]],
        }

        class Result:
            def __init__(self, returncode: int) -> None:
                self.returncode = returncode

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "execution.json"
            with patch(
                "run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run",
                side_effect=[Result(1), Result(0), Result(0)],
            ) as run:
                result = run_plan(
                    plan,
                    include_post_validation=True,
                    continue_on_error=True,
                    execution_output=output,
                )
            persisted = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(run.call_count, 3)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual([row["status"] for row in result["results"]], ["failed", "passed", "passed"])
        self.assertEqual(result["execution_output"], str(output))
        self.assertEqual(persisted["status"], "failed")
        self.assertEqual(persisted["failed_count"], 1)

    def test_run_plan_can_execute_workload_through_wsl(self) -> None:
        plan = {
            "commands": [
                {
                    "step": "run_workload",
                    "workload": "10K",
                    "reason": "missing",
                    "argv": ["python", "first.py"],
                    "wsl_argv": ["wsl", "-d", "Ubuntu-22.04", "--", "python3", "first.py"],
                },
                {"step": "import_evidence", "workload": "10K", "reason": "missing", "argv": ["python", "second.py"]},
            ],
            "post_import_validation": [],
        }

        class Result:
            returncode = 0

        with patch(
            "run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run",
            return_value=Result(),
        ) as run:
            result = run_plan(plan, include_post_validation=False, execute_in_wsl=True)

        self.assertEqual(result["status"], "passed")
        self.assertEqual(run.call_args_list[0].args[0], ["wsl", "-d", "Ubuntu-22.04", "--", "python3", "first.py"])
        self.assertEqual(run.call_args_list[1].args[0], [sys.executable, "second.py"])

    def test_run_plan_fails_fast_by_default(self) -> None:
        plan = {
            "commands": [
                {"step": "run_workload", "workload": "10K", "reason": "missing", "argv": ["python", "first.py"]},
                {"step": "import_evidence", "workload": "10K", "reason": "missing", "argv": ["python", "second.py"]},
            ],
            "post_import_validation": [],
        }

        class Result:
            returncode = 2

        with patch(
            "run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run",
            return_value=Result(),
        ) as run:
            with self.assertRaises(SystemExit):
                run_plan(plan, include_post_validation=False)

        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
