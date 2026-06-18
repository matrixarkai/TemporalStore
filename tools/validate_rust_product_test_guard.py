#!/usr/bin/env python3
"""Guard new Rust product-behavior tests.

Existing Rust attributed tests are a migration backlog captured in
``tools/rust_product_test_baseline.json``. New tests must be explicit:

* ``// shared-corpus: case_name[, other_case]`` for product behavior.
* ``// rust-internal: short reason`` for Rust-only implementation mechanics.

The shared-corpus marker is validated against the canonical Rust-owned
C++/Rust corpus so product behavior does not drift into Rust-only tests.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUST_CRATE = ROOT / "crates" / "temporalstore-rust"
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
BASELINE = ROOT / "tools" / "rust_product_test_baseline.json"

TEST_ATTR = re.compile(r"^\s*#\[(?:tokio::)?test\]")
FN_NAME = re.compile(r"\bfn\s+([A-Za-z0-9_]+)\s*\(")
SHARED_MARKER = re.compile(r"shared-corpus:\s*([A-Za-z0-9_,\-\s]+)")
INTERNAL_MARKER = re.compile(r"rust-internal:\s*(\S.{7,})")


@dataclass(frozen=True)
class RustTest:
    test_id: str
    path: Path
    line_no: int
    context: str


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


def rust_tests() -> list[RustTest]:
    tests: list[RustTest] = []
    for path in sorted(RUST_CRATE.rglob("*.rs")):
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line_no, line in enumerate(lines):
            if not TEST_ATTR.match(line):
                continue
            for fn_line_no in range(line_no + 1, min(line_no + 8, len(lines))):
                match = FN_NAME.search(lines[fn_line_no])
                if not match:
                    continue
                context_start = max(0, line_no - 6)
                context = "\n".join(lines[context_start : fn_line_no + 1])
                rel = relative(path)
                name = match.group(1)
                tests.append(RustTest(f"{rel}::{name}", path, fn_line_no + 1, context))
                break
    return tests


def load_baseline() -> set[str]:
    with BASELINE.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    tests = data.get("grandfathered_tests")
    if not isinstance(tests, list) or not all(isinstance(item, str) for item in tests):
        raise SystemExit(f"{relative(BASELINE)}: grandfathered_tests must be a string list")
    duplicates = sorted({item for item in tests if tests.count(item) > 1})
    if duplicates:
        raise SystemExit(f"{relative(BASELINE)}: duplicate baseline test ids: {duplicates}")
    return set(tests)


def load_corpus_cases() -> set[str]:
    with CORPUS.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    cases = data.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{relative(CORPUS)}: cases must be a list")
    names = {case.get("name") for case in cases}
    return {name for name in names if isinstance(name, str) and name}


def parse_shared_marker(context: str) -> list[str]:
    match = SHARED_MARKER.search(context)
    if not match:
        return []
    return [
        item.strip()
        for item in re.split(r"[,\s]+", match.group(1))
        if item.strip()
    ]


def validate_marked_test(test: RustTest, corpus_cases: set[str]) -> tuple[bool, bool]:
    shared_cases = parse_shared_marker(test.context)
    internal = INTERNAL_MARKER.search(test.context) is not None
    location = f"{relative(test.path)}:{test.line_no}"

    if shared_cases and internal:
        raise SystemExit(f"{location}: use either shared-corpus or rust-internal, not both")
    if shared_cases:
        missing = sorted(set(shared_cases) - corpus_cases)
        if missing:
            raise SystemExit(
                f"{location}: shared-corpus marker references missing cases: {', '.join(missing)}"
            )
        return True, False
    if internal:
        return False, True
    return False, False


def validate() -> dict[str, int]:
    baseline = load_baseline()
    corpus_cases = load_corpus_cases()
    tests = rust_tests()
    current_ids = {test.test_id for test in tests}

    removed = sorted(baseline - current_ids)
    if removed:
        raise SystemExit(
            f"{relative(BASELINE)}: remove deleted tests from grandfathered_tests: "
            + ", ".join(removed[:20])
            + (" ..." if len(removed) > 20 else "")
        )

    new_without_marker: list[str] = []
    marked_shared = 0
    marked_internal = 0
    for test in tests:
        has_shared, has_internal = validate_marked_test(test, corpus_cases)
        marked_shared += int(has_shared)
        marked_internal += int(has_internal)
        if test.test_id not in baseline and not has_shared and not has_internal:
            new_without_marker.append(f"{test.test_id} at {relative(test.path)}:{test.line_no}")

    if new_without_marker:
        print("new Rust tests must declare shared-corpus or rust-internal markers:", file=sys.stderr)
        for item in new_without_marker:
            print(f"  - {item}", file=sys.stderr)
        raise SystemExit(1)

    return {
        "rust_attributed_tests": len(tests),
        "grandfathered_tests": len(baseline),
        "new_marked_shared_corpus_tests": marked_shared,
        "new_marked_rust_internal_tests": marked_internal,
    }


def main() -> None:
    result = validate()
    print("Rust product test guard passed")
    for key, value in result.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
