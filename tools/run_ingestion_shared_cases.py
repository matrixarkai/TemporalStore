#!/usr/bin/env python3
"""Run shared ingestion parity cases from the unified corpus.

The ingestion cases are service/runtime workflows, so they are represented as
``existing_test`` corpus commands. This runner makes their Rust side executable
by running the ``rust_runner`` commands embedded in the shared corpus. C++
native execution remains optional through ``TS_CPP_INGESTION_NATIVE_CMD``;
without it, C++ is still validated as source/harness surface evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
INGESTION_PREFIX = "ingestion_"
INGESTION_SUITE = "cpp_ingestion_parity"
RUST_EXECUTABLE_MODE = "rust_executable_cxx_static"


def load_ingestion_cases(corpus_path: Path) -> list[dict]:
    with corpus_path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)
    cases = [
        case
        for case in corpus.get("cases", [])
        if isinstance(case.get("name"), str) and case["name"].startswith(INGESTION_PREFIX)
    ]
    if not cases:
        raise SystemExit(f"{corpus_path}: no {INGESTION_PREFIX} shared cases found")
    return cases


def validate_case(case: dict, cpp_repo: Path | None) -> list[tuple[str, str]]:
    commands: list[tuple[str, str]] = []
    steps = case.get("steps")
    if not isinstance(steps, list) or not steps:
        raise SystemExit(f"{case.get('name')}: ingestion case has no steps")
    for step in steps:
        command = step.get("command", {})
        location = f"{case.get('name')}/{step.get('name')}"
        if command.get("kind") != "existing_test":
            raise SystemExit(f"{location}: ingestion step must use existing_test command")
        if command.get("suite") != INGESTION_SUITE:
            raise SystemExit(f"{location}: unexpected suite {command.get('suite')!r}")
        if command.get("mode") != RUST_EXECUTABLE_MODE:
            raise SystemExit(
                f"{location}: mode must be {RUST_EXECUTABLE_MODE!r} so Rust execution "
                "and C++ static status stay explicit"
            )
        rust_runner = command.get("rust_runner")
        if not isinstance(rust_runner, str) or not rust_runner:
            raise SystemExit(f"{location}: missing rust_runner")
        if "cargo test -p temporalstore-rust" not in rust_runner:
            raise SystemExit(f"{location}: rust_runner must invoke temporalstore-rust tests")
        rust_validator = command.get("rust_validator")
        if rust_validator != "python3 tools/validate_ingestion_ops_parity_evidence.py":
            raise SystemExit(f"{location}: missing ingestion rust_validator")
        required_paths = command.get("required_paths")
        if not isinstance(required_paths, list) or not required_paths:
            raise SystemExit(f"{location}: missing C++ required_paths")
        if cpp_repo is not None:
            for required_path in required_paths:
                if not (cpp_repo / required_path).exists():
                    raise SystemExit(f"{location}: C++ required path missing: {required_path}")
        commands.append((location, rust_runner))
    return commands


def run_command(command: str, cwd: Path) -> None:
    print(f"+ {command}", flush=True)
    subprocess.run(command, cwd=cwd, shell=True, check=True)


def render_cpp_native_command(template: str, corpus: Path, case: str, cpp_repo: Path | None) -> str:
    values = {
        "corpus": shlex.quote(str(corpus)),
        "case": shlex.quote(case),
    }
    if cpp_repo is not None:
        values["cpp_repo"] = shlex.quote(str(cpp_repo))
    if any(f"{{{key}}}" in template for key in values):
        return template.format(**values)
    return f"{template} --corpus {shlex.quote(str(corpus))} --case {shlex.quote(case)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=CORPUS)
    parser.add_argument("--cpp-repo", type=Path)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--rust", action="store_true", help="run Rust ingestion case commands")
    parser.add_argument(
        "--cpp-native",
        action="store_true",
        help="run TS_CPP_INGESTION_NATIVE_CMD for every ingestion case",
    )
    parser.add_argument(
        "--require-cpp-native",
        action="store_true",
        help="fail unless TS_CPP_INGESTION_NATIVE_CMD is configured",
    )
    args = parser.parse_args()

    cpp_repo = args.cpp_repo.resolve() if args.cpp_repo else None
    corpus = args.corpus.resolve()
    case_commands: list[tuple[str, str, str]] = []
    for case in load_ingestion_cases(corpus):
        for location, command in validate_case(case, cpp_repo):
            case_commands.append((case["name"], location, command))

    print(f"ingestion_shared_cases={len({case for case, _, _ in case_commands})}")
    print(f"ingestion_rust_runners={len(case_commands)}")
    print(f"ingestion_mode={RUST_EXECUTABLE_MODE}")

    if args.validate_only:
        return 0

    if args.rust:
        for _, location, command in case_commands:
            print(f"== rust ingestion case: {location} ==")
            run_command(command, ROOT)

    native_template = os.environ.get("TS_CPP_INGESTION_NATIVE_CMD")
    if args.require_cpp_native and not native_template:
        raise SystemExit("TS_CPP_INGESTION_NATIVE_CMD is required for native C++ ingestion execution")
    if args.cpp_native:
        if not native_template:
            raise SystemExit("set TS_CPP_INGESTION_NATIVE_CMD to run C++ ingestion cases")
        cwd = cpp_repo or ROOT
        for case, location, _ in case_commands:
            print(f"== c++ ingestion case: {location} ==")
            run_command(render_cpp_native_command(native_template, corpus, case, cpp_repo), cwd)

    return 0


if __name__ == "__main__":
    sys.exit(main())
