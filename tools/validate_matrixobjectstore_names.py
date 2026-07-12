#!/usr/bin/env python3
"""Fail if retired legacy object-store naming leaks into first-party files."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEGACY_TOKEN = "byte" + "store"
PATTERN = re.compile(LEGACY_TOKEN + r"|byte\s+store", re.IGNORECASE)
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
    parts = set(path.relative_to(ROOT).parts)
    if parts & SKIP_DIRS:
        return True
    return path.suffix.lower() in SKIP_SUFFIXES


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
    print("matrixobjectstore_names: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
