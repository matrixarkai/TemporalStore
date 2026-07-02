#!/usr/bin/env python3
"""Validate shared C++/Rust Raft parity fixtures and live report pairs."""

from __future__ import annotations

import argparse
import pathlib
import sys

from validate_raft_cpp_rust_parity_contract import (
    DATA_NODE_REPORT_PAIR_CORPUS,
    METASERVER_REPORT_PAIR_CORPUS,
    REPORT_PAIR_CORPUS,
    _load_json,
    validate_report_pair,
)


DEFAULT_FIXTURES = [
    REPORT_PAIR_CORPUS,
    METASERVER_REPORT_PAIR_CORPUS,
    DATA_NODE_REPORT_PAIR_CORPUS,
]


def _validate_pair_file(path: pathlib.Path) -> list[str]:
    corpus = _load_json(path)
    failures = validate_report_pair(corpus["cpp"], corpus["rust"])
    return [f"{path.name}: {failure}" for failure in failures]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    parser.add_argument("--fixture", action="append", type=pathlib.Path)
    args = parser.parse_args()

    failures: list[str] = []
    if bool(args.cpp_report) != bool(args.rust_report):
        failures.append("--cpp-report and --rust-report must be provided together")
    if args.cpp_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))
    else:
        for fixture in args.fixture or DEFAULT_FIXTURES:
            failures.extend(_validate_pair_file(fixture))

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    validated = [path.name for path in (args.fixture or DEFAULT_FIXTURES)]
    print("raft C++/Rust shared parity passed:")
    print("- fixtures=" + ", ".join(validated))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
