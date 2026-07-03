#!/usr/bin/env python3
"""Validate the generated next C++/Rust performance workflow plan.

This gate checks the plan before anybody runs it. It keeps the long-running
storage parity loop fail-closed by ensuring the next live workload commands
carry the same-config, phase-scale, redaction, and post-validation guarantees.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from audit_temporalstore_cpp_rust_performance_artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_MATRIX,
    GOAL_VALIDATOR,
    NINE_PHASE_VALIDATOR,
    PHASE_SCALE_COVERAGE,
    RUNNER,
    audit_artifacts,
)
from run_temporalstore_cpp_rust_next_performance_workflow import (
    DEFAULT_WSL_DISTRO,
    WORKSPACE_ROOT_WSL_PLACEHOLDER,
    build_execution_plan,
)
from validate_temporalstore_cpp_rust_performance_parity import (
    REQUIRED_SAME_CONFIG_COMMAND_ARGS,
    SAME_CONFIG_KEYS,
)


SCHEMA = "temporalstore_cpp_rust_next_performance_workflow_v1"
REQUIRED_RUN_FLAGS = {
    "--require-perf-parity",
    "--require-phase-scale-matrix",
}
REQUIRED_POST_VALIDATORS = {
    ("python", GOAL_VALIDATOR),
    ("python", NINE_PHASE_VALIDATOR, "--loops", "9"),
}
SENSITIVE_PLACEHOLDERS = {
    "<MATRIXARK_PARITY_CPP_LIB>",
    "<MATRIXARK_PARITY_RUST_CLI>",
}


def _items_after_flags(argv: list[str], flags: set[str]) -> list[str]:
    values: list[str] = []
    for index, item in enumerate(argv):
        if item in flags and index + 1 < len(argv):
            values.append(argv[index + 1])
    return values


def _has_runner(argv: list[str]) -> bool:
    return RUNNER in argv or any(item.endswith(RUNNER) for item in argv)


def _validate_run_command(index: int, command: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    prefix = f"commands[{index}] workload={command.get('workload')!r}"
    argv = command.get("argv")
    if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
        return [f"{prefix}: argv must be a string list"]
    if not _has_runner(argv):
        failures.append(f"{prefix}: run_workload argv does not call {RUNNER}")
    missing_flags = sorted(flag for flag in REQUIRED_RUN_FLAGS if flag not in argv)
    if missing_flags:
        failures.append(f"{prefix}: argv missing required flags {missing_flags}")

    coverage = command.get("phase_scale_coverage_required")
    if coverage != PHASE_SCALE_COVERAGE:
        failures.append(f"{prefix}: phase_scale_coverage_required drift")
    for key in ("artifact_dir", "comparison_path", "recommended_execution_output"):
        if not isinstance(command.get(key), str) or not command.get(key):
            failures.append(f"{prefix}: missing {key}")
    if not command.get("required_same_config_fields"):
        failures.append(f"{prefix}: missing required_same_config_fields")
    else:
        missing_same_config_fields = [
            key for key in SAME_CONFIG_KEYS if key not in command.get("required_same_config_fields")
        ]
        if missing_same_config_fields:
            failures.append(
                f"{prefix}: required_same_config_fields missing {missing_same_config_fields}"
            )
    if not command.get("required_result"):
        failures.append(f"{prefix}: missing required_result")
    for flag, expected_value in REQUIRED_SAME_CONFIG_COMMAND_ARGS.items():
        if flag not in argv:
            failures.append(f"{prefix}: argv missing same-config flag {flag}")
            continue
        flag_index = argv.index(flag)
        actual_value = argv[flag_index + 1] if flag_index + 1 < len(argv) else None
        if actual_value != expected_value:
            failures.append(
                f"{prefix}: argv {flag} drift expected {expected_value!r} got {actual_value!r}"
            )

    wsl_argv = command.get("wsl_argv")
    if not isinstance(wsl_argv, list) or not all(isinstance(item, str) for item in wsl_argv):
        failures.append(f"{prefix}: missing generated wsl_argv")
    else:
        missing_wsl_flags = sorted(flag for flag in REQUIRED_RUN_FLAGS if flag not in wsl_argv)
        if missing_wsl_flags:
            failures.append(f"{prefix}: wsl_argv missing required flags {missing_wsl_flags}")
        for flag, expected_value in REQUIRED_SAME_CONFIG_COMMAND_ARGS.items():
            if flag not in wsl_argv:
                failures.append(f"{prefix}: wsl_argv missing same-config flag {flag}")
                continue
            flag_index = wsl_argv.index(flag)
            actual_value = wsl_argv[flag_index + 1] if flag_index + 1 < len(wsl_argv) else None
            if actual_value != expected_value:
                failures.append(
                    f"{prefix}: wsl_argv {flag} drift expected {expected_value!r} got {actual_value!r}"
                )
        if "--cd" not in wsl_argv or WORKSPACE_ROOT_WSL_PLACEHOLDER not in _items_after_flags(wsl_argv, {"--cd"}):
            failures.append(f"{prefix}: wsl_argv must redact --cd workspace root")
        backend_values = _items_after_flags(wsl_argv, {"--cpp-lib", "--rust-cli"})
        leaked_backend_values = [
            value for value in backend_values if value not in SENSITIVE_PLACEHOLDERS
        ]
        if leaked_backend_values:
            failures.append(f"{prefix}: wsl_argv leaked backend artifact paths")
    return failures


def _validate_import_command(index: int, command: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    prefix = f"commands[{index}] workload={command.get('workload')!r}"
    if "wsl_argv" in command:
        failures.append(f"{prefix}: import_evidence should not carry wsl_argv")
    argv = command.get("argv")
    if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
        failures.append(f"{prefix}: argv must be a string list")
    if command.get("phase_scale_coverage_required") != PHASE_SCALE_COVERAGE:
        failures.append(f"{prefix}: phase_scale_coverage_required drift")
    return failures


def validate_plan(plan: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if plan.get("schema") != SCHEMA:
        failures.append(f"schema drift: expected {SCHEMA!r}")
    if plan.get("dry_run_default") is not True:
        failures.append("dry_run_default must be true")
    execution_environment = plan.get("execution_environment")
    if not isinstance(execution_environment, dict):
        failures.append("missing execution_environment")
    elif execution_environment.get("wsl_distro") != DEFAULT_WSL_DISTRO:
        failures.append("unexpected default WSL distro")

    commands = plan.get("commands")
    if not isinstance(commands, list) or not commands:
        failures.append("plan must include at least one command")
        commands = []
    run_command_count = 0
    for index, command in enumerate(commands):
        if not isinstance(command, dict):
            failures.append(f"commands[{index}] must be an object")
            continue
        step = command.get("step")
        if step == "run_workload":
            run_command_count += 1
            failures.extend(_validate_run_command(index, command))
        elif step == "import_evidence":
            failures.extend(_validate_import_command(index, command))
        else:
            failures.append(f"commands[{index}]: unexpected step {step!r}")
    if run_command_count == 0:
        failures.append("plan must include at least one run_workload command")

    validators = plan.get("post_import_validation")
    if not isinstance(validators, list):
        failures.append("post_import_validation must be a list")
    else:
        validator_set = {
            tuple(item)
            for item in validators
            if isinstance(item, list) and all(isinstance(part, str) for part in item)
        }
        for required in sorted(REQUIRED_POST_VALIDATORS):
            if required not in validator_set:
                failures.append(f"missing post_import_validation {list(required)!r}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument(
        "--max-workloads",
        type=int,
        default=1,
        help="Use the same workload cap as the dry-run workflow by default.",
    )
    args = parser.parse_args()

    max_workloads = args.max_workloads if args.max_workloads > 0 else None
    audit = audit_artifacts(args.artifact_root, args.matrix)
    plan = build_execution_plan(audit, max_workloads=max_workloads)
    failures = validate_plan(plan)
    if failures:
        for failure in failures:
            print(f"next_performance_plan_failure: {failure}")
        return 1
    print(
        "next_performance_plan_validated "
        f"commands={len(plan.get('commands', []))} "
        f"workloads={plan.get('workload_count')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
