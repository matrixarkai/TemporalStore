#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate launcher and Rust storage tuning expose the same TS_* knobs."""

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
    "TS_PAGE_INDEX_CACHE_BYTES",
    "TS_BLOCK_INDEX_CACHE_BYTES",
}

EXPECTED_DEFAULTS = {
    "TS_CONTEXT_PAGE_TARGET_BYTES": 65536,
    "TS_BLOCK_SEGMENT_TARGET_BYTES": 1073741824,
    # 1 GiB. Was recorded here as 10 MiB, which never matched the engine: DEFAULT_STORAGE_ZONE_SIZE
    # has a single commit in this repository and has been `1 << 30` since it landed. The old figure
    # came from the launcher script this validator used to cross-check, and outlived it.
    "TS_STORAGE_ZONE_SIZE": 1073741824,
    "TS_STREAM_MAX_BLOB_SIZE": 10485760,
    "TS_COMPACTION_WATERMARK_BYTES": 268435456,
    "TS_COLD_SCAN_NO_CACHE_FILL": True,
    "TS_PAGE_INDEX_CACHE_BYTES": 67108864,
    "TS_BLOCK_INDEX_CACHE_BYTES": 67108864,
}


def extract_names(path: pathlib.Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"TS_[A-Z0-9_]+", text))


def _eval_numeric_expr(expr: str) -> int:
    expr = expr.strip()
    if not re.fullmatch(r"[0-9 <*+()]+", expr):
        raise ValueError(f"unsupported numeric expression: {expr}")
    return int(eval(expr, {"__builtins__": {}}, {}))


def extract_runtime_defaults(path: pathlib.Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    values: dict[str, object] = {}
    for name in EXPECTED_KNOBS:
        match = re.search(rf'{name}="\$\{{{name}:-([^}}]+)\}}"', text)
        if not match:
            continue
        raw = match.group(1)
        if raw.lower() in {"true", "false"}:
            values[name] = raw.lower() == "true"
        else:
            values[name] = int(raw)
    return values


def extract_rust_defaults(path: pathlib.Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    constant_map = {
        "TS_CONTEXT_PAGE_TARGET_BYTES": "DEFAULT_CONTEXT_PAGE_TARGET_BYTES",
        # The identifier does not match the variable name for this one: the engine declares
        # `pub const TS_BLOCK_SLAB_TARGET_BYTES: &str = "TS_BLOCK_SEGMENT_TARGET_BYTES"`, and its
        # default constant is named after the identifier, not the variable.
        "TS_BLOCK_SEGMENT_TARGET_BYTES": "DEFAULT_BLOCK_SLAB_TARGET_BYTES",
        "TS_STORAGE_ZONE_SIZE": "DEFAULT_STORAGE_ZONE_SIZE",
        "TS_STREAM_MAX_BLOB_SIZE": "DEFAULT_STREAM_MAX_BLOB_SIZE",
        "TS_COMPACTION_WATERMARK_BYTES": "DEFAULT_COMPACTION_WATERMARK_BYTES",
        "TS_COLD_SCAN_NO_CACHE_FILL": "DEFAULT_COLD_SCAN_NO_CACHE_FILL",
        "TS_PAGE_INDEX_CACHE_BYTES": "DEFAULT_PAGE_INDEX_CACHE_BYTES",
        "TS_BLOCK_INDEX_CACHE_BYTES": "DEFAULT_BLOCK_INDEX_CACHE_BYTES",
    }
    values: dict[str, object] = {}
    for knob, constant in constant_map.items():
        match = re.search(rf"pub const {constant}:\s*(?:usize|u64|bool)\s*=\s*([^;]+);", text)
        if not match:
            continue
        raw = match.group(1).strip()
        if raw.lower() in {"true", "false"}:
            values[knob] = raw.lower() == "true"
        else:
            values[knob] = _eval_numeric_expr(raw)
    return values


def main() -> int:
    files = {
        "rust_config": ROOT / "crates" / "temporalstore-rust" / "src" / "storage_config.rs",
        "rust_readme": ROOT / "crates" / "temporalstore-rust" / "README.md",
    }
    missing: list[str] = []
    for label, path in files.items():
        names = extract_names(path)
        absent = sorted(EXPECTED_KNOBS - names)
        if absent:
            missing.append(f"{label} ({path}) missing: {', '.join(absent)}")

    # One source, deliberately. This once cross-checked a launcher script that set each knob
    # with a shell default; that script no longer exists, and reading a missing key raised
    # KeyError on every run -- so this validator has not reported anything, correct or otherwise,
    # since the entry was removed.
    default_sources = {
        "rust_config": extract_rust_defaults(files["rust_config"]),
    }
    for label, defaults in default_sources.items():
        for name, expected in EXPECTED_DEFAULTS.items():
            if name not in defaults:
                missing.append(f"{label} missing default for {name}")
                continue
            if defaults[name] != expected:
                missing.append(f"{label} default drift for {name}: got {defaults[name]!r}, expected {expected!r}")

    if missing:
        for line in missing:
            print(line, file=sys.stderr)
        return 1

    print("storage tuning knobs agreeing with the engine:")
    for name in sorted(EXPECTED_KNOBS):
        print(f"- {name}={EXPECTED_DEFAULTS[name]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
