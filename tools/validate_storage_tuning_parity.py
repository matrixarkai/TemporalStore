#!/usr/bin/env python3
"""Validate C++ launcher and Rust storage tuning expose the same TS_* knobs."""

from __future__ import annotations

import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]

EXPECTED_KNOBS = {
    "TS_CONTEXT_PAGE_TARGET_BYTES",
    "TS_BLOCK_SEGMENT_TARGET_BYTES",
    "TS_STORAGE_ZONE_SIZE",
    "TS_STREAM_MAX_BLOB_SIZE",
    "TS_COMPACTION_WATERMARK_BYTES",
    "TS_COLD_SCAN_NO_CACHE_FILL",
}


def extract_names(path: pathlib.Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"TS_[A-Z0-9_]+", text))


def main() -> int:
    files = {
        "cpp_runtime": ROOT / "tools" / "temporalstore_runtime_env.sh",
        "rust_config": ROOT / "crates" / "temporalstore-rust" / "src" / "storage_config.rs",
        "rust_readme": ROOT / "crates" / "temporalstore-rust" / "README.md",
    }
    missing: list[str] = []
    for label, path in files.items():
        names = extract_names(path)
        absent = sorted(EXPECTED_KNOBS - names)
        if absent:
            missing.append(f"{label} ({path}) missing: {', '.join(absent)}")

    if missing:
        for line in missing:
            print(line, file=sys.stderr)
        return 1

    print("storage tuning parity knobs present:")
    for name in sorted(EXPECTED_KNOBS):
        print(f"- {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
