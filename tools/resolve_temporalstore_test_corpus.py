#!/usr/bin/env python3
"""Resolve the external TemporalStore unified test corpus path."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CANDIDATES = [
    ROOT / "third_party" / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
    ROOT.parent / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
]


def resolve(explicit: str | None = None) -> Path:
    if explicit:
        path = Path(explicit)
        if path.exists():
            return path.resolve()
        raise SystemExit(f"TemporalStore unified corpus not found: {path}")
    env_path = os.environ.get("TEMPORALSTORE_TEST_CORPUS")
    if env_path:
        path = Path(env_path)
        if path.exists():
            return path.resolve()
        raise SystemExit(f"TEMPORALSTORE_TEST_CORPUS does not exist: {path}")
    for candidate in CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    checked = "\n".join(str(path) for path in CANDIDATES)
    raise SystemExit(
        "TemporalStore unified corpus is external now. Set TEMPORALSTORE_TEST_CORPUS "
        "or initialize third_party/TemporalStoreTestCorpus. Checked:\n" + checked
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus")
    args = parser.parse_args()
    print(resolve(args.corpus))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
