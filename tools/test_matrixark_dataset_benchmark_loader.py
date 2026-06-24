#!/usr/bin/env python3
"""Compatibility wrapper for shared benchmark loader tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "tools" / "test_matrixark_dataset_benchmark_loader.py",
        ROOT.parent / "TemporalStoreTestCorpus" / "tools" / "test_matrixark_dataset_benchmark_loader.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            return subprocess.run([sys.executable, str(candidate)], cwd=ROOT, check=False).returncode
    checked = "\n".join(str(candidate) for candidate in candidates)
    raise SystemExit(f"shared benchmark loader test not found. Checked:\n{checked}")


if __name__ == "__main__":
    raise SystemExit(main())
