#!/usr/bin/env python3
"""Compare live C++ and Rust TemporalStore storage lifecycle reports.

This is the operator-facing wrapper around validate_storage_lifecycle_parity.py.
It fails closed unless both reports expose the canonical public storage contract,
write/read/cold-scan sequences, lifecycle phases, cache layers, reclaim semantics,
effective tuning, and storage lifecycle metrics.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

from validate_storage_lifecycle_parity import _load_json, validate_contract_and_runner, validate_report_pair


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", required=True, type=pathlib.Path)
    parser.add_argument("--rust-report", required=True, type=pathlib.Path)
    args = parser.parse_args()

    failures = validate_contract_and_runner()
    failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("live C++/Rust storage lifecycle reports match the canonical public contract")
    print(f"- cpp_report={args.cpp_report}")
    print(f"- rust_report={args.rust_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
