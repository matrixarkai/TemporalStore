#!/usr/bin/env python3
"""Validate the external TemporalStoreTestCorpus dependency wiring."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL_FALLBACK = ROOT / "compat" / "unified_temporalstore_cases.json"
DEFAULT_EXTERNAL_CANDIDATES = [
    ROOT / "third_party" / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
    ROOT.parent / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate(path: Path) -> dict:
    data = load(path)
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
    }


def resolve_external(explicit: Path | None) -> Path | None:
    if explicit:
        return explicit
    for candidate in DEFAULT_EXTERNAL_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external-corpus", type=Path)
    parser.add_argument("--require-external", action="store_true")
    parser.add_argument("--allow-drift", action="store_true")
    args = parser.parse_args()

    if not LOCAL_FALLBACK.exists():
        raise SystemExit(f"missing local fallback corpus: {LOCAL_FALLBACK}")
    local = validate(LOCAL_FALLBACK)

    external_path = resolve_external(args.external_corpus)
    if external_path is None:
        if args.require_external:
            candidates = "\n".join(str(path) for path in DEFAULT_EXTERNAL_CANDIDATES)
            raise SystemExit(f"external TemporalStoreTestCorpus not found. Checked:\n{candidates}")
        print(
            "external TemporalStoreTestCorpus not found; "
            f"local fallback cases={local['case_count']} sha256={local['sha256']}"
        )
        return

    if not external_path.exists():
        raise SystemExit(f"external corpus not found: {external_path}")
    external = validate(external_path)
    if external["sha256"] != local["sha256"] and not args.allow_drift:
        raise SystemExit(
            "external TemporalStoreTestCorpus differs from local fallback during transition:\n"
            f"external={external_path} sha256={external['sha256']}\n"
            f"local={LOCAL_FALLBACK} sha256={local['sha256']}"
        )

    print(
        "validated TemporalStoreTestCorpus dependency "
        f"external={external_path} cases={external['case_count']} "
        f"steps={external['step_count']} sha256={external['sha256']}"
    )


if __name__ == "__main__":
    main()
