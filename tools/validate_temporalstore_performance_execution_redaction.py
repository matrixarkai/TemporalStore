#!/usr/bin/env python3
"""Validate that C++/Rust parity execution artifacts are publishable.

The next-performance workflow may execute with local C++ SDK and Rust CLI
paths, but committed evidence must keep those paths redacted. This gate scans
the parity execution JSON artifacts and fails if local Windows/WSL workspace or
backend artifact paths leak into stored evidence.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = ROOT / "docs" / "benchmarks"
EXECUTION_SCHEMA = "temporalstore_cpp_rust_next_performance_execution_v1"

PRIVATE_PATH_MARKERS = (
    "C:\\Users\\",
    "/mnt/c/Users/",
    "/mnt/c/Users\\",
    "/root/src/",
    "Deeproute",
)

REQUIRED_PLACEHOLDERS = {
    "--cpp-lib": "<MATRIXARK_PARITY_CPP_LIB>",
    "--rust-cli": "<MATRIXARK_PARITY_RUST_CLI>",
    "--cd": "<WORKSPACE_ROOT_WSL>",
}
REQUIRED_RUN_WORKLOAD_FLAGS = (
    "--require-perf-parity",
    "--require-phase-scale-matrix",
)


def _walk_json(value: Any, path: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            found.extend(_walk_json(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_walk_json(child, f"{path}[{index}]"))
    elif isinstance(value, str):
        for marker in PRIVATE_PATH_MARKERS:
            if marker in value:
                found.append((path, marker))
                break
    return found


def _validate_sensitive_flag_placeholders(path: Path, data: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for row_index, row in enumerate(data.get("results") or []):
        if not isinstance(row, dict):
            continue
        argv = row.get("argv")
        if not isinstance(argv, list):
            continue
        for flag, placeholder in REQUIRED_PLACEHOLDERS.items():
            for index, item in enumerate(argv):
                if item == flag and index + 1 < len(argv) and argv[index + 1] != placeholder:
                    failures.append(
                        f"{path}: results[{row_index}].argv {flag} value is not redacted"
                    )
                if isinstance(item, str) and item.startswith(f"{flag}="):
                    expected = f"{flag}={placeholder}"
                    if item != expected:
                        failures.append(
                            f"{path}: results[{row_index}].argv {flag}= value is not redacted"
                        )
    return failures


def _validate_run_workload_flags(path: Path, data: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for row_index, row in enumerate(data.get("results") or []):
        if not isinstance(row, dict) or row.get("step") != "run_workload":
            continue
        argv = row.get("argv")
        if not isinstance(argv, list):
            failures.append(f"{path}: results[{row_index}].argv must be a list")
            continue
        for flag in REQUIRED_RUN_WORKLOAD_FLAGS:
            if flag not in argv:
                failures.append(f"{path}: results[{row_index}].argv missing {flag}")
    return failures


def validate_artifact(path: Path) -> list[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{path}: invalid JSON: {exc}"]
    if not isinstance(data, dict) or data.get("schema") != EXECUTION_SCHEMA:
        return []

    failures = [
        f"{path}: unredacted local path marker {marker!r} at {location}"
        for location, marker in _walk_json(data)
    ]
    failures.extend(_validate_sensitive_flag_placeholders(path, data))
    failures.extend(_validate_run_workload_flags(path, data))
    return failures


def main() -> int:
    artifacts = sorted(DEFAULT_ARTIFACT_ROOT.glob("parity_*/execution*.json"))
    failures: list[str] = []
    for artifact in artifacts:
        failures.extend(validate_artifact(artifact))
    if failures:
        raise SystemExit(
            "TemporalStore performance execution redaction failed:\n"
            + "\n".join(f"- {failure}" for failure in failures[:50])
        )
    print(
        "TemporalStore performance execution artifacts are redacted "
        f"artifacts_scanned={len(artifacts)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
