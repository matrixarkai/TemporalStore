#!/usr/bin/env python3
"""Tests for validate_temporalstore_next_performance_plan.py."""

from __future__ import annotations

import unittest

from audit_temporalstore_cpp_rust_performance_artifacts import PHASE_SCALE_COVERAGE
from run_temporalstore_cpp_rust_next_performance_workflow import build_execution_plan
from validate_temporalstore_next_performance_plan import validate_plan


def _valid_audit() -> dict:
    return {
        "next_required_runs": [{"workload": "10K_event_ingestion"}],
        "next_required_workflow": {
            "commands": [
                {
                    "step": "run_workload",
                    "workload": "10K_event_ingestion",
                    "reason": "blocked_no_importable",
                    "argv": [
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
                        "--require-phase-scale-matrix",
                    ],
                    "artifact_dir": "docs/benchmarks/parity_10K_event_ingestion",
                    "comparison_path": "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                    "recommended_execution_output": "docs/benchmarks/parity_10K_event_ingestion/execution.json",
                    "phase_scale_coverage_required": PHASE_SCALE_COVERAGE,
                    "required_same_config_fields": ["dataset", "storage_mode"],
                    "required_result": ["selected_ref_parity=true"],
                    "next_run_hint_source": "matrix",
                },
                {
                    "step": "import_evidence",
                    "workload": "10K_event_ingestion",
                    "reason": "blocked_no_importable",
                    "argv": [
                        "python",
                        "tools/import_temporalstore_cpp_rust_performance_evidence.py",
                        "--report",
                        "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                        "--validate",
                    ],
                    "phase_scale_coverage_required": PHASE_SCALE_COVERAGE,
                },
            ],
            "post_import_validation": [
                ["python", "tools/validate_temporalstore_cpp_rust_goal_parity.py"],
                ["python", "tools/validate_storage_engine_9_phase_parity.py", "--loops", "9"],
            ],
        },
    }


class NextPerformancePlanValidatorTest(unittest.TestCase):
    def test_valid_plan_passes(self) -> None:
        self.assertEqual(validate_plan(build_execution_plan(_valid_audit())), [])

    def test_missing_phase_scale_coverage_fails(self) -> None:
        audit = _valid_audit()
        audit["next_required_workflow"]["commands"][0].pop("phase_scale_coverage_required")

        failures = validate_plan(build_execution_plan(audit))

        self.assertTrue(any("phase_scale_coverage_required drift" in failure for failure in failures))

    def test_backend_path_leak_in_wsl_command_fails(self) -> None:
        plan = build_execution_plan(_valid_audit())
        run_command = plan["commands"][0]
        run_command["wsl_argv"].extend(["--cpp-lib", "/mnt/c/private/libbcache2.so"])

        failures = validate_plan(plan)

        self.assertTrue(any("leaked backend artifact paths" in failure for failure in failures))

    def test_missing_post_validator_fails(self) -> None:
        audit = _valid_audit()
        audit["next_required_workflow"]["post_import_validation"] = [
            ["python", "tools/validate_temporalstore_cpp_rust_goal_parity.py"]
        ]

        failures = validate_plan(build_execution_plan(audit))

        self.assertTrue(any("validate_storage_engine_9_phase_parity.py" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
