#!/usr/bin/env python3
"""Compatibility helpers for MatrixArk runners moved to TemporalStoreTestCorpus."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def resolve_shared_runner(script_name: str) -> Path:
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "tools" / script_name,
        ROOT.parent / "TemporalStoreTestCorpus" / "tools" / script_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise SystemExit(
        f"Unable to locate shared MatrixArk runner {script_name}. "
        "Run `git submodule update --init third_party/TemporalStoreTestCorpus` "
        "or check out TemporalStoreTestCorpus as a sibling repo."
    )


def main(script_name: str) -> int:
    runner = resolve_shared_runner(script_name)
    env = os.environ.copy()
    env.setdefault("TEMPORALSTORE_CONSUMER_REPO", str(ROOT))
    command = [sys.executable, str(runner), *sys.argv[1:]]
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode
