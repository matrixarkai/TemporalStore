#!/usr/bin/env python3
"""Tests for the next C++/Rust performance workflow runner."""

from __future__ import annotations

import sys
import unittest

from run_temporalstore_cpp_rust_next_performance_workflow import _pythonize, build_execution_plan


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
        self.assertIn(
            ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
            plan["post_import_validation"],
        )

    def test_pythonize_uses_current_interpreter(self) -> None:
        self.assertEqual(_pythonize(["python", "tool.py", "--x"]), [sys.executable, "tool.py", "--x"])
        self.assertEqual(_pythonize(["custom-python", "tool.py"]), ["custom-python", "tool.py"])


if __name__ == "__main__":
    unittest.main()
