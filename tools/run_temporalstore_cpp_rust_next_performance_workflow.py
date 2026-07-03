#!/usr/bin/env python3
"""Run or print the next C++/Rust performance parity evidence workflow.

The workflow is derived from audit_temporalstore_cpp_rust_performance_artifacts.py.
By default this script is a dry run: it prints the exact run/import/validation
commands that should be executed next. Pass --execute to run them.
"""

from __future__ import annotations

import argparse
import json
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


def _pythonize(argv: list[str]) -> list[str]:
    if argv and argv[0] == "python":
        return [sys.executable, *argv[1:]]
    return argv


def build_execution_plan(audit: dict[str, Any], max_workloads: int | None = None) -> dict[str, Any]:
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
        "workload_count": len({command.get("workload") for command in commands if isinstance(command, dict)}),
        "commands": [
            {
                "step": command.get("step"),
                "workload": command.get("workload"),
                "reason": command.get("reason"),
                "argv": command.get("argv"),
            }
            for command in commands
            if isinstance(command, dict)
        ],
        "post_import_validation": post_import_validation,
    }


def run_plan(plan: dict[str, Any], *, include_post_validation: bool) -> int:
    commands = plan.get("commands") if isinstance(plan.get("commands"), list) else []
    for command in commands:
        if not isinstance(command, dict):
            continue
        argv = command.get("argv")
        if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
            raise SystemExit(f"invalid workflow command: {command!r}")
        subprocess.run(_pythonize(argv), cwd=ROOT, check=True)
    if include_post_validation:
        validators = plan.get("post_import_validation") if isinstance(plan.get("post_import_validation"), list) else []
        for argv in validators:
            if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
                raise SystemExit(f"invalid validation command: {argv!r}")
            subprocess.run(_pythonize(argv), cwd=ROOT, check=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--max-workloads", type=int, default=1)
    parser.add_argument("--execute", action="store_true", help="Run the generated commands. Default is dry-run JSON output.")
    parser.add_argument(
        "--skip-post-validation",
        action="store_true",
        help="With --execute, skip final goal and 9-phase validators.",
    )
    args = parser.parse_args()

    max_workloads = args.max_workloads if args.max_workloads > 0 else None
    audit = audit_artifacts(args.artifact_root, args.matrix)
    plan = build_execution_plan(audit, max_workloads=max_workloads)
    if not args.execute:
        print(json.dumps(plan, indent=2) + "\n", end="")
        return 0
    return run_plan(plan, include_post_validation=not args.skip_post_validation)


if __name__ == "__main__":
    raise SystemExit(main())
