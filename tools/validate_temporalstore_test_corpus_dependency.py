#!/usr/bin/env python3
"""Validate the external TemporalStoreTestCorpus dependency wiring."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

try:
    from tools.resolve_temporalstore_test_corpus import CANDIDATES, resolve
except ModuleNotFoundError:
    from resolve_temporalstore_test_corpus import CANDIDATES, resolve


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if data.get("schema_version") != 1:
        raise SystemExit(f"{path}: schema_version must be 1")
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit(f"{path}: cases must be a non-empty list")
    names = [case.get("name") for case in cases]
    if any(not isinstance(name, str) or not name for name in names):
        raise SystemExit(f"{path}: every case needs a non-empty name")
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise SystemExit(f"{path}: duplicate case names: {duplicates}")
    return {
        "name": data.get("name"),
        "case_count": len(cases),
        "step_count": sum(len(case.get("steps", [])) for case in cases),
        "sha256": sha256(path),
        "path": str(path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-corpus", type=Path)
    parser.add_argument("--require-external", action="store_true", help="kept for compatibility; external corpus is always required")
    parser.add_argument("--allow-drift", action="store_true", help="kept for compatibility after local fallback removal")
    args = parser.parse_args()

    try:
        corpus = resolve(str(args.external_corpus) if args.external_corpus else None)
    except SystemExit as exc:
        checked = "\n".join(str(path) for path in CANDIDATES)
        raise SystemExit(f"external TemporalStoreTestCorpus not found. Checked:\n{checked}") from exc
    report = validate(corpus)
    print(
        "validated external TemporalStoreTestCorpus "
        f"path={report['path']} cases={report['case_count']} steps={report['step_count']} sha256={report['sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
