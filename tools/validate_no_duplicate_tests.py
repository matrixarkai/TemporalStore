#!/usr/bin/env python3
"""Reject duplicate TemporalStore test surfaces.

The shared Rust/C++ corpus already rejects duplicate case names, step names, and
command payloads inside a case. This repo-level guard adds the broader checks
that matter when pruning duplicate test cases:

* Rust attributed test function names are unique across temporalstore-rust.
* Shared corpus case names are unique.
* Shared corpus step names and command payloads are unique within each case.
* C++ existing-test surface references are not repeated across cases.
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUST_CRATE = ROOT / "crates" / "temporalstore-rust"
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"

# These cases intentionally reuse lower-level C++ required_paths so the shared
# corpus can carry the exact current C++ Raft case names while preserving the
# older granular surface gates. Treat them as aliases for duplicate-path checks,
# not as duplicate behavioral tests.
CPP_RAFT_CASE_ALIASES = {
    "storage_data_raft_replication_gtest",
    "raft_metaserver_membership_failover_snapshot",
    "raft_data_node_scale_failover_snapshot",
    "raft_data_node_mixed_rw_and_membership",
    "raft_production_gate",
}

TEST_ATTR = re.compile(r"^\s*#\[(?:tokio::)?test\]")
FN_NAME = re.compile(r"\bfn\s+([A-Za-z0-9_]+)\s*\(")


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


def rust_tests() -> dict[str, list[str]]:
    tests: dict[str, list[str]] = defaultdict(list)
    for path in sorted(RUST_CRATE.rglob("*.rs")):
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line_no, line in enumerate(lines):
            if not TEST_ATTR.match(line):
                continue
            for fn_line_no in range(line_no + 1, min(line_no + 8, len(lines))):
                match = FN_NAME.search(lines[fn_line_no])
                if match:
                    tests[match.group(1)].append(f"{relative(path)}:{fn_line_no + 1}")
                    break
    return tests


def fail_duplicates(title: str, duplicates: dict[str, list[str]]) -> None:
    if not duplicates:
        return
    print(title, file=sys.stderr)
    for name, locations in sorted(duplicates.items()):
        print(f"  {name}", file=sys.stderr)
        for location in locations:
            print(f"    - {location}", file=sys.stderr)
    raise SystemExit(1)


def validate_rust_test_names() -> int:
    tests = rust_tests()
    duplicates = {name: locations for name, locations in tests.items() if len(locations) > 1}
    fail_duplicates("duplicate Rust attributed test function names:", duplicates)
    return sum(len(locations) for locations in tests.values())


def command_signature(command: dict) -> str:
    return json.dumps(command, sort_keys=True, separators=(",", ":"))


def validate_corpus() -> tuple[int, int, int]:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    cases = corpus.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{relative(CORPUS)}: cases must be a list")

    case_locations: dict[str, list[str]] = defaultdict(list)
    existing_test_refs: dict[str, list[str]] = defaultdict(list)
    step_count = 0

    for case_index, case in enumerate(cases):
        case_name = case.get("name")
        if not isinstance(case_name, str) or not case_name:
            raise SystemExit(f"{relative(CORPUS)}: case[{case_index}] has no name")
        case_locations[case_name].append(f"{relative(CORPUS)}:case[{case_index}]")

        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{relative(CORPUS)}: case {case_name} must have steps")

        step_locations: dict[str, list[str]] = defaultdict(list)
        command_locations: dict[str, list[str]] = defaultdict(list)
        for step_index, step in enumerate(steps):
            step_count += 1
            step_name = step.get("name")
            if not isinstance(step_name, str) or not step_name:
                raise SystemExit(f"{relative(CORPUS)}: case {case_name} step[{step_index}] has no name")
            location = f"{relative(CORPUS)}:{case_name}/{step_name}"
            step_locations[step_name].append(location)

            command = step.get("command")
            if not isinstance(command, dict):
                raise SystemExit(f"{location}: command must be an object")
            command_locations[command_signature(command)].append(location)

            if command.get("kind") == "existing_test" and case_name not in CPP_RAFT_CASE_ALIASES:
                for required_path in command.get("required_paths", []):
                    if not isinstance(required_path, str) or not required_path:
                        raise SystemExit(f"{location}: existing_test required_paths must be strings")
                    existing_test_refs[required_path].append(location)
            elif command.get("kind") == "existing_test":
                for required_path in command.get("required_paths", []):
                    if not isinstance(required_path, str) or not required_path:
                        raise SystemExit(f"{location}: existing_test required_paths must be strings")

        fail_duplicates(
            f"duplicate shared corpus step names in case {case_name}:",
            {name: locations for name, locations in step_locations.items() if len(locations) > 1},
        )
        fail_duplicates(
            f"duplicate shared corpus command payloads in case {case_name}:",
            {
                signature: locations
                for signature, locations in command_locations.items()
                if len(locations) > 1
            },
        )

    fail_duplicates(
        "duplicate shared corpus case names:",
        {name: locations for name, locations in case_locations.items() if len(locations) > 1},
    )
    fail_duplicates(
        "duplicate C++ existing_test required_paths:",
        {path: locations for path, locations in existing_test_refs.items() if len(locations) > 1},
    )
    return len(cases), step_count, len(existing_test_refs)


def main() -> None:
    rust_count = validate_rust_test_names()
    case_count, step_count, existing_test_count = validate_corpus()
    print("no duplicate TemporalStore test cases found")
    print(f"rust_attributed_tests={rust_count}")
    print(f"shared_corpus_cases={case_count}")
    print(f"shared_corpus_steps={step_count}")
    print(f"cpp_existing_test_surfaces={existing_test_count}")


if __name__ == "__main__":
    main()
