#!/usr/bin/env python3
"""Run shared control-plane parity cases from the unified corpus.

The control-plane cases currently use ``existing_test`` corpus commands because
their workflows are service/harness oriented instead of direct engine
command/response steps. This runner makes the Rust side executable by running
the ``rust_runner`` commands embedded in those shared cases. The C++ side can
be made native by supplying ``TS_CPP_CONTROL_PLANE_NATIVE_CMD``; otherwise C++
remains a required-path surface/evidence gate.
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
CONTROL_PREFIX = "control_"
CONTROL_SUITES = {
    "cpp_client_control_plane_parity",
    "cpp_proxy_control_plane_parity",
    "cpp_metaserver_control_plane_parity",
    "cpp_data_node_lifecycle_parity",
}
RUST_EXECUTABLE_MODE = "rust_executable_cxx_static"


def load_control_cases(corpus_path: Path) -> list[dict]:
    with corpus_path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)
    cases = [
        case
        for case in corpus.get("cases", [])
        if isinstance(case.get("name"), str) and case["name"].startswith(CONTROL_PREFIX)
    ]
    if not cases:
        raise SystemExit(f"{corpus_path}: no {CONTROL_PREFIX} shared cases found")
    return cases


def validate_case(case: dict, cpp_repo: Path | None) -> list[tuple[str, str]]:
    commands: list[tuple[str, str]] = []
    steps = case.get("steps")
    if not isinstance(steps, list) or not steps:
        raise SystemExit(f"{case.get('name')}: control-plane case has no steps")
    for step in steps:
        command = step.get("command", {})
        location = f"{case.get('name')}/{step.get('name')}"
        if command.get("kind") != "existing_test":
            raise SystemExit(f"{location}: control-plane step must use existing_test command")
        if command.get("suite") not in CONTROL_SUITES:
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
        if rust_validator != "python3 tools/validate_control_plane_parity_evidence.py":
            raise SystemExit(f"{location}: missing control-plane rust_validator")
        required_paths = command.get("required_paths")
        if not isinstance(required_paths, list) or not required_paths:
            raise SystemExit(f"{location}: missing C++ required_paths")
        if cpp_repo is not None:
            for required_path in required_paths:
                if not (cpp_repo / required_path).exists():
                    raise SystemExit(f"{location}: C++ required path missing: {required_path}")
        commands.append((location, rust_runner))
    return commands


def expand_cargo_test_command(command: str) -> list[str]:
    """Split shared-case cargo test commands that list multiple TESTNAME filters.

    Cargo accepts only one positional TESTNAME before ``--``. Some shared
    control-plane cases intentionally bundle closely related Rust tests in one
    corpus step; expand those into one cargo invocation per filter so the
    shared case remains a single product contract while execution stays valid.
    """

    tokens = shlex.split(command)
    if len(tokens) < 3 or tokens[:2] != ["cargo", "test"]:
        return [command]
    try:
        separator = tokens.index("--")
    except ValueError:
        separator = len(tokens)

    before = tokens[:separator]
    after = tokens[separator:]
    options_with_values = {
        "-p",
        "--package",
        "--bin",
        "--test",
        "--example",
        "--bench",
        "--target",
        "--manifest-path",
        "--features",
        "--color",
        "--message-format",
        "--profile",
        "--config",
        "-Z",
        "-j",
    }
    test_filters: list[str] = []
    filter_indexes: set[int] = set()
    index = 2
    while index < len(before):
        token = before[index]
        if token in options_with_values:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        test_filters.append(token)
        filter_indexes.add(index)
        index += 1

    if len(test_filters) <= 1:
        return [command]

    base = [token for index, token in enumerate(before) if index not in filter_indexes]
    return [
        shlex.join([*base, test_filter, *after]) for test_filter in test_filters
    ]


def run_command(command: str, cwd: Path) -> None:
    expanded = expand_cargo_test_command(command)
    if len(expanded) > 1:
        print(f"# expanded multi-filter cargo test into {len(expanded)} commands", flush=True)
    for command in expanded:
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
    parser.add_argument("--rust", action="store_true", help="run Rust control-plane case commands")
    parser.add_argument(
        "--cpp-native",
        action="store_true",
        help="run TS_CPP_CONTROL_PLANE_NATIVE_CMD for every control-plane case",
    )
    parser.add_argument(
        "--require-cpp-native",
        action="store_true",
        help="fail unless TS_CPP_CONTROL_PLANE_NATIVE_CMD is configured",
    )
    args = parser.parse_args()

    cpp_repo = args.cpp_repo.resolve() if args.cpp_repo else None
    corpus = args.corpus.resolve()
    case_commands: list[tuple[str, str, str]] = []
    for case in load_control_cases(corpus):
        for location, command in validate_case(case, cpp_repo):
            case_commands.append((case["name"], location, command))

    print(f"control_plane_shared_cases={len({case for case, _, _ in case_commands})}")
    print(f"control_plane_rust_runners={len(case_commands)}")
    print(f"control_plane_mode={RUST_EXECUTABLE_MODE}")

    if args.validate_only:
        return 0

    if args.rust:
        for _, location, command in case_commands:
            print(f"== rust control-plane case: {location} ==")
            run_command(command, ROOT)

    native_template = os.environ.get("TS_CPP_CONTROL_PLANE_NATIVE_CMD")
    if args.require_cpp_native and not native_template:
        raise SystemExit(
            "TS_CPP_CONTROL_PLANE_NATIVE_CMD is required for native C++ control-plane execution"
        )
    if args.cpp_native:
        if not native_template:
            raise SystemExit("set TS_CPP_CONTROL_PLANE_NATIVE_CMD to run C++ control-plane cases")
        cwd = cpp_repo or ROOT
        for case, location, _ in case_commands:
            print(f"== c++ control-plane case: {location} ==")
            run_command(render_cpp_native_command(native_template, corpus, case, cpp_repo), cwd)

    return 0


if __name__ == "__main__":
    sys.exit(main())
