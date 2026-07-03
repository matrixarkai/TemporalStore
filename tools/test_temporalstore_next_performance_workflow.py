#!/usr/bin/env python3
"""Tests for the next C++/Rust performance workflow runner."""

from __future__ import annotations

import os
import sys
import subprocess
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from run_temporalstore_cpp_rust_next_performance_workflow import (
    DEFAULT_WSL_DISTRO,
    _backend_artifact_preflight,
    _pythonize,
    _redact_sensitive_argv,
    _wsl_path,
    _with_backend_artifact_overrides,
    _wslize,
    build_execution_plan,
    default_execution_output,
    run_plan,
)


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
                        "artifact_dir": "docs/benchmarks/parity_10K_event_ingestion",
                        "comparison_path": "docs/benchmarks/parity_10K_event_ingestion/comparison.json",
                        "recommended_execution_output": "docs/benchmarks/parity_10K_event_ingestion/execution.json",
                        "required_same_config_fields": ["dataset", "storage_mode"],
                        "required_result": ["selected_ref_parity=true"],
                        "next_run_hint_source": "matrix",
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
        self.assertEqual(plan["execution_environment"]["wsl_distro"], DEFAULT_WSL_DISTRO)
        self.assertIn("backend_artifacts", plan["execution_environment"])
        self.assertEqual(plan["workload_count"], 1)
        self.assertEqual([command["workload"] for command in plan["commands"]], ["10K_event_ingestion", "10K_event_ingestion"])
        self.assertEqual(
            plan["commands"][0]["recommended_execution_output"],
            "docs/benchmarks/parity_10K_event_ingestion/execution.json",
        )
        self.assertEqual(plan["commands"][0]["artifact_dir"], "docs/benchmarks/parity_10K_event_ingestion")
        self.assertEqual(plan["commands"][0]["comparison_path"], "docs/benchmarks/parity_10K_event_ingestion/comparison.json")
        self.assertEqual(plan["commands"][0]["required_same_config_fields"], ["dataset", "storage_mode"])
        self.assertEqual(plan["commands"][0]["required_result"], ["selected_ref_parity=true"])
        self.assertEqual(plan["commands"][0]["next_run_hint_source"], "matrix")
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

    def test_redact_sensitive_backend_artifact_args(self) -> None:
        self.assertEqual(
            _redact_sensitive_argv(
                [
                    "python",
                    "tool.py",
                    "--cpp-lib",
                    "/mnt/c/private/libbcache2.so",
                    "--rust-cli=/mnt/c/private/matrixark_record_log",
                    _wsl_path(Path(__file__).resolve().parents[1]),
                ]
            ),
            [
                "python",
                "tool.py",
                "--cpp-lib",
                "<MATRIXARK_PARITY_CPP_LIB>",
                "--rust-cli=<MATRIXARK_PARITY_RUST_CLI>",
                "<WORKSPACE_ROOT_WSL>",
            ],
        )

    def test_wslize_wraps_python_workload_command(self) -> None:
        command = _wslize(["python", "tools/run_matrixark_cpp_rust_scale_report.py"], distro="Ubuntu-22.04")
        self.assertEqual(command[:3], ["wsl", "-d", "Ubuntu-22.04"])
        self.assertIn("--cd", command)
        self.assertEqual(command[-2:], ["python3", "tools/run_matrixark_cpp_rust_scale_report.py"])

    def test_plan_accepts_custom_wsl_distro(self) -> None:
        audit = {
            "next_required_runs": [{"workload": "10K_event_ingestion"}],
            "next_required_workflow": {
                "commands": [
                    {
                        "step": "run_workload",
                        "workload": "10K_event_ingestion",
                        "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py"],
                    }
                ]
            },
        }

        plan = build_execution_plan(audit, max_workloads=1, wsl_distro="CustomUbuntu")

        self.assertEqual(plan["commands"][0]["wsl_argv"][:3], ["wsl", "-d", "CustomUbuntu"])

    def test_backend_artifact_overrides_respect_explicit_paths(self) -> None:
        command = _with_backend_artifact_overrides(
            [
                "python",
                "tools/run_matrixark_cpp_rust_scale_report.py",
                "--cpp-lib",
                "/custom/libbcache2.so",
                "--rust-cli",
                "/custom/matrixark_record_log",
            ]
        )
        self.assertEqual(command.count("--cpp-lib"), 1)
        self.assertEqual(command.count("--rust-cli"), 1)
        self.assertIn("/custom/libbcache2.so", command)
        self.assertIn("/custom/matrixark_record_log", command)

    def test_backend_artifact_overrides_use_environment_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cpp_lib = root / "libbcache2.so"
            rust_cli = root / "matrixark_record_log"
            cpp_lib.write_text("", encoding="utf-8")
            rust_cli.write_text("", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "MATRIXARK_PARITY_CPP_LIB": str(cpp_lib),
                    "MATRIXARK_PARITY_RUST_CLI": str(rust_cli),
                },
            ):
                command = _with_backend_artifact_overrides(
                    ["python", "tools/run_matrixark_cpp_rust_scale_report.py"]
                )

        self.assertIn("--cpp-lib", command)
        self.assertIn("--rust-cli", command)
        self.assertIn("libbcache2.so", command[command.index("--cpp-lib") + 1])
        self.assertIn("matrixark_record_log", command[command.index("--rust-cli") + 1])

    def test_backend_artifact_preflight_reports_environment_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cpp_lib = root / "libbcache2.so"
            rust_cli = root / "matrixark_record_log"
            cpp_lib.write_text("", encoding="utf-8")
            rust_cli.write_text("", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "MATRIXARK_PARITY_CPP_LIB": str(cpp_lib),
                    "MATRIXARK_PARITY_RUST_CLI": str(rust_cli),
                },
            ):
                preflight = _backend_artifact_preflight()

        self.assertTrue(preflight["ready"])
        self.assertEqual(preflight["cpp_lib"]["source"], "env")
        self.assertEqual(preflight["rust_cli"]["source"], "env")

    def test_plan_redacts_environment_backend_artifact_paths(self) -> None:
        audit = {
            "next_required_runs": [{"workload": "10K_event_ingestion"}],
            "next_required_workflow": {
                "commands": [
                    {
                        "step": "run_workload",
                        "workload": "10K_event_ingestion",
                        "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py"],
                    }
                ]
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cpp_lib = root / "private" / "libbcache2.so"
            rust_cli = root / "private" / "matrixark_record_log"
            cpp_lib.parent.mkdir()
            cpp_lib.write_text("", encoding="utf-8")
            rust_cli.write_text("", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "MATRIXARK_PARITY_CPP_LIB": str(cpp_lib),
                    "MATRIXARK_PARITY_RUST_CLI": str(rust_cli),
                },
            ):
                plan = build_execution_plan(audit, max_workloads=1)

        artifacts = plan["execution_environment"]["backend_artifacts"]
        self.assertEqual(artifacts["cpp_lib"]["path"], "<MATRIXARK_PARITY_CPP_LIB>")
        self.assertEqual(artifacts["rust_cli"]["path"], "<MATRIXARK_PARITY_RUST_CLI>")
        self.assertEqual(
            plan["commands"][0]["wsl_argv"][3:5],
            ["--cd", "<WORKSPACE_ROOT_WSL>"],
        )
        self.assertEqual(
            plan["commands"][0]["wsl_argv"][-4:],
            [
                "--cpp-lib",
                "<MATRIXARK_PARITY_CPP_LIB>",
                "--rust-cli",
                "<MATRIXARK_PARITY_RUST_CLI>",
            ],
        )

    def test_run_plan_can_continue_after_failure(self) -> None:
        plan = {
            "commands": [
                {"step": "run_workload", "workload": "10K", "reason": "missing", "argv": ["python", "first.py"]},
                {
                    "step": "import_evidence",
                    "workload": "10K",
                    "reason": "missing",
                    "argv": ["python", "second.py"],
                    "comparison_path": "docs/benchmarks/parity_10K/comparison.json",
                    "required_result": ["selected_ref_parity=true"],
                },
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

        self.assertEqual(run.call_count, 2)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual([row["status"] for row in result["results"]], ["failed", "skipped", "passed"])
        self.assertEqual(result["results"][1]["comparison_path"], "docs/benchmarks/parity_10K/comparison.json")
        self.assertEqual(result["results"][1]["required_result"], ["selected_ref_parity=true"])
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

    def test_run_plan_records_command_timeout(self) -> None:
        plan = {
            "commands": [
                {"step": "run_workload", "workload": "10K", "reason": "missing", "argv": ["python", "first.py"]},
                {"step": "import_evidence", "workload": "10K", "reason": "missing", "argv": ["python", "second.py"]},
            ],
            "post_import_validation": [],
        }

        class Result:
            returncode = 0

        with patch(
            "run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run",
            side_effect=[subprocess.TimeoutExpired(cmd=["python", "first.py"], timeout=7), Result()],
        ):
            result = run_plan(
                plan,
                include_post_validation=False,
                continue_on_error=True,
                command_timeout_sec=7,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["results"][0]["status"], "timeout")
        self.assertEqual(result["results"][0]["returncode"], 124)
        self.assertEqual(result["results"][0]["timeout_sec"], 7)

    def test_run_plan_records_redacted_paths_but_executes_real_paths(self) -> None:
        plan = {
            "commands": [
                {
                    "step": "run_workload",
                    "workload": "10K",
                    "reason": "missing",
                    "argv": [
                        "python",
                        "tools/run_matrixark_cpp_rust_scale_report.py",
                        "--cpp-lib",
                        "/mnt/c/private/libbcache2.so",
                        "--rust-cli",
                        "/mnt/c/private/matrixark_record_log",
                    ],
                },
            ],
            "post_import_validation": [],
        }

        class Result:
            returncode = 0

        with patch(
            "run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run",
            return_value=Result(),
        ) as run:
            result = run_plan(plan, include_post_validation=False)

        self.assertIn("/mnt/c/private/libbcache2.so", run.call_args.args[0])
        self.assertEqual(
            result["results"][0]["argv"],
            [
                "python",
                "tools/run_matrixark_cpp_rust_scale_report.py",
                "--cpp-lib",
                "<MATRIXARK_PARITY_CPP_LIB>",
                "--rust-cli",
                "<MATRIXARK_PARITY_RUST_CLI>",
            ],
        )

    def test_run_plan_fails_closed_when_backend_artifacts_missing(self) -> None:
        plan = {
            "commands": [
                {
                    "step": "run_workload",
                    "workload": "10K",
                    "reason": "missing",
                    "argv": ["python", "tools/run_matrixark_cpp_rust_scale_report.py"],
                    "artifact_dir": "docs/benchmarks/parity_10K",
                    "comparison_path": "docs/benchmarks/parity_10K/comparison.json",
                    "required_same_config_fields": ["dataset"],
                    "required_result": ["selected_ref_parity=true"],
                },
                {"step": "import_evidence", "workload": "10K", "reason": "missing", "argv": ["python", "second.py"]},
            ],
            "post_import_validation": [],
        }
        preflight = {
            "cpp_lib": {"exists": False},
            "rust_cli": {"exists": False},
            "ready": False,
        }

        with patch(
            "run_temporalstore_cpp_rust_next_performance_workflow._backend_artifact_preflight",
            return_value=preflight,
        ), patch("run_temporalstore_cpp_rust_next_performance_workflow.subprocess.run") as run:
            result = run_plan(
                plan,
                include_post_validation=False,
                continue_on_error=True,
                require_backend_artifacts=True,
            )

        self.assertEqual(run.call_count, 0)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["results"][0]["status"], "preflight_failed")
        self.assertEqual(result["results"][0]["returncode"], 125)
        self.assertEqual(result["results"][0]["artifact_dir"], "docs/benchmarks/parity_10K")
        self.assertEqual(result["results"][0]["comparison_path"], "docs/benchmarks/parity_10K/comparison.json")
        self.assertEqual(result["results"][0]["required_same_config_fields"], ["dataset"])
        self.assertEqual(result["results"][0]["required_result"], ["selected_ref_parity=true"])
        self.assertIn("missing_cpp_lib", result["results"][0]["preflight_blockers"][0])
        self.assertIn("missing_rust_cli", result["results"][0]["preflight_blockers"][1])
        self.assertEqual(result["results"][1]["status"], "skipped")
        self.assertEqual(result["results"][1]["skip_reason"], "upstream_workload_failed")

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
