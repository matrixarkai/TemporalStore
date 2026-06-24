#!/usr/bin/env python3
"""Compatibility wrapper for the shared MatrixArk context scale E2E runner."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def resolve_shared_runner() -> Path:
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "tools" / "run_context_pipeline_scale_e2e.py",
        ROOT.parent / "TemporalStoreTestCorpus" / "tools" / "run_context_pipeline_scale_e2e.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    checked = "\n".join(str(candidate) for candidate in candidates)
    raise SystemExit(f"shared context scale runner not found. Checked:\n{checked}")


def main() -> int:
    runner = resolve_shared_runner()
    env = os.environ.copy()
    env.setdefault("TEMPORALSTORE_CONSUMER_REPO", str(ROOT))
    command = [sys.executable, str(runner), "--consumer-repo", str(ROOT), *sys.argv[1:]]
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
