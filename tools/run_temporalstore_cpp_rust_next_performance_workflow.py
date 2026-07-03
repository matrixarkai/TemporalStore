#!/usr/bin/env python3
"""Run or print the next C++/Rust performance parity evidence workflow.

The workflow is derived from audit_temporalstore_cpp_rust_performance_artifacts.py.
By default this script is a dry run: it prints the exact run/import/validation
commands that should be executed next. Pass --execute to run them.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from audit_temporalstore_cpp_rust_performance_artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_MATRIX,
    audit_artifacts,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WSL_DISTRO = "Ubuntu-22.04"
WSL_DISTRO_ENV = "MATRIXARK_PARITY_WSL_DISTRO"


CPP_LIB_ENV = "MATRIXARK_PARITY_CPP_LIB"
RUST_CLI_ENV = "MATRIXARK_PARITY_RUST_CLI"


CPP_LIB_CANDIDATES = [
    ROOT / "output-ubuntu22/release/sdk/lib/libbcache2.so",
]


RUST_CLI_CANDIDATES = [
    ROOT / "sdk/rust/temporalstore/target/release/matrixark_record_log",
]


def _wsl_path(path: Path | str) -> str:
    normalized = str(path).replace("\\", "/")
    if len(normalized) >= 3 and normalized[1] == ":" and normalized[2] == "/":
        return f"/mnt/{normalized[0].lower()}/{normalized[3:]}"
    return normalized


def _pythonize(argv: list[str]) -> list[str]:
    if argv and argv[0] == "python":
        return [sys.executable, *argv[1:]]
    return argv


def _wslize(argv: list[str], *, distro: str) -> list[str]:
    if not argv:
        return argv
    inner = ["python3" if argv[0] == "python" else argv[0], *argv[1:]]
    return ["wsl", "-d", distro, "--cd", _wsl_path(ROOT), "--", *inner]


def _first_existing(env_name: str, candidates: list[Path]) -> Path | None:
    override = os.environ.get(env_name)
    if override:
        path = Path(override)
        if path.exists():
            return path
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _resolve_artifact(env_name: str, candidates: list[Path]) -> dict[str, Any]:
    override = os.environ.get(env_name)
    if override:
        path = Path(override)
        return {
            "env": env_name,
            "path": str(path),
            "wsl_path": _wsl_path(path),
            "source": "env",
            "exists": path.exists(),
        }
    existing = [path for path in candidates if path.exists()]
    if existing:
        path = max(existing, key=lambda candidate: candidate.stat().st_mtime)
        return {
            "env": env_name,
            "path": str(path),
            "wsl_path": _wsl_path(path),
            "source": "repo_default",
            "exists": True,
        }
    return {
        "env": env_name,
        "path": None,
        "wsl_path": None,
        "source": "missing",
        "exists": False,
        "candidates": [str(path) for path in candidates],
    }


def _backend_artifact_preflight() -> dict[str, Any]:
    cpp_lib = _resolve_artifact(CPP_LIB_ENV, CPP_LIB_CANDIDATES)
    rust_cli = _resolve_artifact(RUST_CLI_ENV, RUST_CLI_CANDIDATES)
    return {
        "cpp_lib": cpp_lib,
        "rust_cli": rust_cli,
        "ready": cpp_lib["exists"] and rust_cli["exists"],
        "override_env": [CPP_LIB_ENV, RUST_CLI_ENV],
    }


def _with_backend_artifact_overrides(argv: list[str]) -> list[str]:
    patched = list(argv)
    if "--cpp-lib" not in patched:
        cpp_lib = _first_existing(CPP_LIB_ENV, CPP_LIB_CANDIDATES)
        if cpp_lib is not None:
            patched.extend(["--cpp-lib", _wsl_path(cpp_lib)])
    if "--rust-cli" not in patched:
        rust_cli = _first_existing(RUST_CLI_ENV, RUST_CLI_CANDIDATES)
        if rust_cli is not None:
            patched.extend(["--rust-cli", _wsl_path(rust_cli)])
    return patched


def build_execution_plan(audit: dict[str, Any], max_workloads: int | None = None, *, wsl_distro: str = DEFAULT_WSL_DISTRO) -> dict[str, Any]:
    workflow = audit.get("next_required_workflow") if isinstance(audit.get("next_required_workflow"), dict) else {}
    commands = workflow.get("commands") if isinstance(workflow.get("commands"), list) else []
    if max_workloads is not None:
        allowed_workloads = {
            item.get("workload")
            for item in audit.get("next_required_runs", [])[:max_workloads]
            if isinstance(item, dict)
        }
        commands = [
            command
            for command in commands
            if isinstance(command, dict) and command.get("workload") in allowed_workloads
        ]
    post_import_validation = workflow.get("post_import_validation")
    if not isinstance(post_import_validation, list):
        post_import_validation = []
    return {
        "schema": "temporalstore_cpp_rust_next_performance_workflow_v1",
        "dry_run_default": True,
        "execution_environment": {
            "wsl_distro": wsl_distro,
            "wsl_distro_env": WSL_DISTRO_ENV,
            "backend_artifacts": _backend_artifact_preflight(),
        },
        "workload_count": len({command.get("workload") for command in commands if isinstance(command, dict)}),
        "commands": [
            ({
                "step": command.get("step"),
                "workload": command.get("workload"),
                "reason": command.get("reason"),
                "argv": command.get("argv"),
                "recommended_execution_output": command.get("recommended_execution_output"),
            }
            | (
                {
                    "wsl_argv": _wslize(
                        _with_backend_artifact_overrides(command.get("argv")),
                        distro=wsl_distro,
                    )
                }
                if command.get("step") == "run_workload"
                and isinstance(command.get("argv"), list)
                and all(isinstance(item, str) for item in command.get("argv"))
                else {}
            ))
            for command in commands
            if isinstance(command, dict)
        ],
        "post_import_validation": post_import_validation,
    }


def default_execution_output(plan: dict[str, Any]) -> Path | None:
    commands = plan.get("commands") if isinstance(plan.get("commands"), list) else []
    for command in commands:
        if not isinstance(command, dict) or command.get("step") != "run_workload":
            continue
        output = command.get("recommended_execution_output")
        if isinstance(output, str) and output:
            return ROOT / output
    return None


def run_plan(
    plan: dict[str, Any],
    *,
    include_post_validation: bool,
    continue_on_error: bool = False,
    execution_output: Path | None = None,
    execute_in_wsl: bool = False,
    command_timeout_sec: int | None = None,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []

    def finish(status: str | None = None) -> dict[str, Any]:
        failed = sum(1 for row in results if row["status"] != "passed")
        execution = {
            "schema": "temporalstore_cpp_rust_next_performance_execution_v1",
            "continue_on_error": continue_on_error,
            "status": status or ("passed" if failed == 0 else "failed"),
            "failed_count": failed,
            "results": results,
        }
        if execution_output is not None:
            execution_output.parent.mkdir(parents=True, exist_ok=True)
            execution_output.write_text(json.dumps(execution, indent=2) + "\n", encoding="utf-8")
            execution["execution_output"] = str(execution_output)
        return execution

    def run_one(step: str, argv: list[str], *, workload: str | None = None, reason: str | None = None) -> None:
        timeout = command_timeout_sec if command_timeout_sec and command_timeout_sec > 0 else None
        try:
            completed = subprocess.run(_pythonize(argv), cwd=ROOT, check=False, timeout=timeout)
            returncode = completed.returncode
            status = "passed" if returncode == 0 else "failed"
        except subprocess.TimeoutExpired:
            returncode = 124
            status = "timeout"
        row = {
            "step": step,
            "workload": workload,
            "reason": reason,
            "argv": argv,
            "returncode": returncode,
            "status": status,
        }
        if status == "timeout":
            row["timeout_sec"] = timeout
        results.append(row)
        if returncode != 0 and not continue_on_error:
            raise SystemExit(json.dumps(finish(), indent=2))

    commands = plan.get("commands") if isinstance(plan.get("commands"), list) else []
    for command in commands:
        if not isinstance(command, dict):
            continue
        argv = command.get("argv")
        if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
            raise SystemExit(f"invalid workflow command: {command!r}")
        if execute_in_wsl and command.get("step") == "run_workload":
            wsl_argv = command.get("wsl_argv")
            if not isinstance(wsl_argv, list) or not all(isinstance(item, str) for item in wsl_argv):
                raise SystemExit(f"workflow command has no valid wsl_argv: {command!r}")
            argv = wsl_argv
        run_one(
            str(command.get("step") or "workflow"),
            argv,
            workload=str(command.get("workload")) if command.get("workload") is not None else None,
            reason=str(command.get("reason")) if command.get("reason") is not None else None,
        )
    if include_post_validation:
        validators = plan.get("post_import_validation") if isinstance(plan.get("post_import_validation"), list) else []
        for argv in validators:
            if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
                raise SystemExit(f"invalid validation command: {argv!r}")
            run_one("post_import_validation", argv)
    return finish()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--max-workloads", type=int, default=1)
    parser.add_argument("--execute", action="store_true", help="Run the generated commands. Default is dry-run JSON output.")
    parser.add_argument("--execute-in-wsl", action="store_true", help="With --execute, run workload commands through WSL so Linux libbcache2.so can load.")
    parser.add_argument(
        "--wsl-distro",
        default=os.environ.get(WSL_DISTRO_ENV, DEFAULT_WSL_DISTRO),
        help=f"WSL distro used for generated workload commands. Defaults to ${WSL_DISTRO_ENV} or {DEFAULT_WSL_DISTRO}.",
    )
    parser.add_argument("--continue-on-error", action="store_true", help="With --execute, keep running later commands after a failure.")
    parser.add_argument("--workflow-command-timeout-sec", type=int, default=900, help="With --execute, cap each generated workflow command and record timeout rows.")
    parser.add_argument("--execution-output", type=Path, help="With --execute, write the execution summary JSON here.")
    parser.add_argument(
        "--skip-post-validation",
        action="store_true",
        help="With --execute, skip final goal and 9-phase validators.",
    )
    args = parser.parse_args()

    max_workloads = args.max_workloads if args.max_workloads > 0 else None
    audit = audit_artifacts(args.artifact_root, args.matrix)
    plan = build_execution_plan(audit, max_workloads=max_workloads, wsl_distro=args.wsl_distro)
    if not args.execute:
        print(json.dumps(plan, indent=2) + "\n", end="")
        return 0
    execution_output = args.execution_output or default_execution_output(plan)
    execution = run_plan(
        plan,
        include_post_validation=not args.skip_post_validation,
        continue_on_error=args.continue_on_error,
        execution_output=execution_output,
        execute_in_wsl=args.execute_in_wsl,
        command_timeout_sec=args.workflow_command_timeout_sec,
    )
    print(json.dumps(execution, indent=2) + "\n", end="")
    return 0 if execution["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
