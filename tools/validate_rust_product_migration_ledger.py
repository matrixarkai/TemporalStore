#!/usr/bin/env python3
"""Validate the Rust product-test migration ledger.

The ledger is intentionally separate from the guard baseline: the baseline keeps
existing tests grandfathered, while the ledger explains where each test must go
next. This validator makes sure every grandfathered test has exactly one
migration disposition.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tools" / "rust_product_test_baseline.json"
LEDGER = ROOT / "tools" / "rust_product_test_migration_ledger.json"

VALID_CLASSIFICATIONS = {
    "move_to_shared",
    "rust_internal",
    "cpp_out_of_scope",
    "duplicate/remove",
}

VALID_FAMILIES = {
    "storage/cache",
    "Raft",
    "context",
    "Redis/admin",
    "Feature",
    "Sequence",
    "IPS",
    "Risk",
    "control plane",
    "ingestion",
    "benchmarks",
    "ops/scale",
    "rust internals",
}


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise SystemExit(f"{path.relative_to(ROOT)}: expected JSON object")
    return data


def main() -> None:
    baseline = load_json(BASELINE).get("grandfathered_tests")
    if not isinstance(baseline, list) or not all(isinstance(item, str) for item in baseline):
        raise SystemExit("tools/rust_product_test_baseline.json: grandfathered_tests must be a string list")

    ledger = load_json(LEDGER)
    entries = ledger.get("entries")
    if not isinstance(entries, list):
        raise SystemExit("tools/rust_product_test_migration_ledger.json: entries must be a list")

    baseline_set = set(baseline)
    entry_ids: list[str] = []
    classifications: Counter[str] = Counter()
    families: Counter[str] = Counter()
    errors: list[str] = []

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            errors.append(f"entry {index}: expected object")
            continue
        test_id = entry.get("test_id")
        family = entry.get("family")
        classification = entry.get("classification")
        target = entry.get("next_migration_target")

        if not isinstance(test_id, str) or not test_id:
            errors.append(f"entry {index}: missing test_id")
            continue
        entry_ids.append(test_id)
        if test_id not in baseline_set:
            errors.append(f"{test_id}: not present in grandfathered baseline")
        if family not in VALID_FAMILIES:
            errors.append(f"{test_id}: invalid family {family!r}")
        if classification not in VALID_CLASSIFICATIONS:
            errors.append(f"{test_id}: invalid classification {classification!r}")
        if not isinstance(target, str) or not target:
            errors.append(f"{test_id}: missing next_migration_target")

        classifications[str(classification)] += 1
        families[str(family)] += 1

    duplicate_ids = sorted({item for item in entry_ids if entry_ids.count(item) > 1})
    if duplicate_ids:
        errors.append("duplicate ledger test ids: " + ", ".join(duplicate_ids[:20]))

    missing = sorted(baseline_set - set(entry_ids))
    if missing:
        errors.append("baseline tests missing from ledger: " + ", ".join(missing[:20]))

    extra = sorted(set(entry_ids) - baseline_set)
    if extra:
        errors.append("ledger tests absent from baseline: " + ", ".join(extra[:20]))

    summary = ledger.get("summary", {})
    if isinstance(summary, dict):
        expected = {
            "grandfathered_tests": len(baseline),
            "by_classification": dict(sorted(classifications.items())),
            "by_family": dict(sorted(families.items())),
        }
        for key, value in expected.items():
            if summary.get(key) != value:
                errors.append(f"summary.{key} mismatch: expected {value!r}, got {summary.get(key)!r}")

    if errors:
        raise SystemExit("\n".join(errors))

    print("Rust product test migration ledger passed")
    print(f"grandfathered_tests={len(baseline)}")
    for key, value in sorted(classifications.items()):
        print(f"{key}={value}")
    for key, value in sorted(families.items()):
        print(f"family.{key}={value}")


if __name__ == "__main__":
    main()
