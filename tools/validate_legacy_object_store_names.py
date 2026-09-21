#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Fail if retired legacy object-store naming leaks into first-party files."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEGACY_TOKEN = "byte" + "store"
# The two-word spelling has to END there. Without the boundary, `byte\s+store` also matches
# ordinary English -- "cost per byte STORED" in an occupancy comment tripped this guard and is
# what kept it red, and therefore unwired. `stores?` still catches the plural.
PATTERN = re.compile(LEGACY_TOKEN + r"|byte\s+stores?(?![A-Za-z])", re.IGNORECASE)
# The one file allowed to spell the retired name, and why.
#
# `validate_open_source_readiness.py` is the guard whose job is to ENUMERATE forbidden vendor
# names; its regex necessarily contains every one of them. Skipping it is not a hole being hidden:
# a scanner that refuses the dictionary cannot have a dictionary. The entry is narrow -- one named
# file, not a directory or a pattern -- so anything else acquiring the token still fails.
#
# There is a test below asserting this file still contains the token, so the skip cannot quietly
# become dead weight after someone rewrites that guard.
SKIP_FILES = {
    "tools/validate_open_source_readiness.py",
}

SKIP_DIRS = {
    ".git",
    "build-ubuntu22",
    "output-ubuntu22",
    "target",
    ".local",
}
SKIP_SUFFIXES = {
    ".a",
    ".bin",
    ".gz",
    ".jpg",
    ".jpeg",
    ".png",
    ".so",
    ".zip",
}


def should_skip(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if relative.as_posix() in SKIP_FILES:
        return True
    parts = set(relative.parts)
    if parts & SKIP_DIRS:
        return True
    return path.suffix.lower() in SKIP_SUFFIXES


def stale_skips() -> list[str]:
    """Skipped files that no longer earn their exemption.

    An exemption is defensible only while the thing it excuses is still true. If
    `validate_open_source_readiness.py` is rewritten to stop enumerating the retired name, this
    skip stops being a dictionary exception and becomes a file nobody checks. So the skip checks
    itself and the run fails until someone deletes it.
    """
    stale: list[str] = []
    for relative in sorted(SKIP_FILES):
        candidate = ROOT / relative
        if not candidate.is_file():
            stale.append(f"{relative}: skipped, but the file is gone")
            continue
        try:
            text = candidate.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if not PATTERN.search(text):
            stale.append(f"{relative}: skipped, but it no longer spells the retired name")
    return stale


def main() -> int:
    matches: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or should_skip(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if PATTERN.search(line):
                matches.append(f"{path.relative_to(ROOT)}:{lineno}: {line.strip()}")
    if matches:
        raise SystemExit(
            "retired legacy object-store naming found:\n" + "\n".join(matches[:200])
        )
    stale = stale_skips()
    if stale:
        raise SystemExit(
            "the skip list has entries that no longer apply -- remove them:\n"
            + "\n".join(stale)
        )
    print("legacy_object_store_names: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
