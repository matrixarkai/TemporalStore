#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Merge Rust hook TemporalStore roots into one live shared root.

The Rust local store keeps page addresses in ``indexes/shard-1.index.json``.
To combine two roots without replaying every event through the slow hook path,
copy one root as the base, append each additional root's page segment bytes to
the base page segment, offset the appended page addresses, and merge the JSON
indexes/session indexes. The result remains a normal Rust hook root: future hook
writes append through the regular engine path.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any


INDEX = Path("indexes/shard-1.index.json")
MANIFEST = Path("pages/page_extent_manifest.json")
SEGMENT0 = Path("pages/page_segment_00000000000000000000.seg")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _is_page_address(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and "page_segment_id" in value
        and "offset" in value
        and "length" in value
    )


def _offset_addresses(value: Any, *, offset_delta: int, page_id_delta: int) -> Any:
    if _is_page_address(value):
        adjusted = dict(value)
        adjusted["page_segment_id"] = 0
        adjusted["band_id"] = 0
        adjusted["offset"] = int(adjusted.get("offset", 0)) + offset_delta
        if "page_id" in adjusted:
            adjusted["page_id"] = int(adjusted.get("page_id", 0)) + page_id_delta
        if "generation" in adjusted:
            adjusted["generation"] = int(adjusted.get("generation", 0)) + page_id_delta
        return adjusted
    if isinstance(value, dict):
        return {key: _offset_addresses(child, offset_delta=offset_delta, page_id_delta=page_id_delta) for key, child in value.items()}
    if isinstance(value, list):
        return [_offset_addresses(child, offset_delta=offset_delta, page_id_delta=page_id_delta) for child in value]
    return value


def _merge_tree(dst: Any, src: Any) -> Any:
    if isinstance(dst, dict) and isinstance(src, dict):
        for key, value in src.items():
            if key in dst:
                dst[key] = _merge_tree(dst[key], value)
            else:
                dst[key] = value
        return dst
    return src


def _page_count(manifest: dict[str, Any]) -> int:
    bands = manifest.get("bands") if isinstance(manifest, dict) else None
    if not bands:
        return 0
    band = bands[0]
    return max(0, int(band.get("last_page_id", -1)) - int(band.get("first_page_id", 0)) + 1)


def _append_pages(dst_root: Path, src_root: Path) -> tuple[int, int, dict[str, Any]]:
    dst_segment = dst_root / SEGMENT0
    src_segment = src_root / SEGMENT0
    dst_manifest = _load_json(dst_root / MANIFEST, {"version": 1, "bands": []})
    src_manifest = _load_json(src_root / MANIFEST, {"version": 1, "bands": []})
    if not src_segment.exists():
        return 0, 0, src_manifest

    dst_segment.parent.mkdir(parents=True, exist_ok=True)
    offset_delta = dst_segment.stat().st_size if dst_segment.exists() else 0
    with dst_segment.open("ab") as out, src_segment.open("rb") as inp:
        shutil.copyfileobj(inp, out, length=8 * 1024 * 1024)

    dst_bands = dst_manifest.setdefault("bands", [])
    if not dst_bands:
        dst_bands.append(
            {
                "band_id": 0,
                "page_segment_id": 0,
                "state": "active",
                "physical_bytes": 0,
                "logical_bytes": 0,
                "created_unix_ms": int(time.time() * 1000),
                "updated_unix_ms": int(time.time() * 1000),
                "first_page_id": 0,
                "last_page_id": -1,
                "readable_prefix_physical_bytes": 0,
                "has_corruption": False,
            }
        )
    dst_band = dst_bands[0]
    src_band = (src_manifest.get("bands") or [{}])[0]
    page_id_delta = int(dst_band.get("last_page_id", -1)) + 1 - int(src_band.get("first_page_id", 0))
    dst_band["physical_bytes"] = int(dst_band.get("physical_bytes", 0)) + int(src_band.get("physical_bytes", src_segment.stat().st_size))
    dst_band["logical_bytes"] = int(dst_band.get("logical_bytes", 0)) + int(src_band.get("logical_bytes", 0))
    dst_band["readable_prefix_physical_bytes"] = int(dst_band.get("readable_prefix_physical_bytes", 0)) + int(
        src_band.get("readable_prefix_physical_bytes", src_segment.stat().st_size)
    )
    dst_band["last_page_id"] = int(dst_band.get("last_page_id", -1)) + _page_count(src_manifest)
    dst_band["updated_unix_ms"] = max(int(dst_band.get("updated_unix_ms", 0)), int(src_band.get("updated_unix_ms", 0)))
    dst_band["has_corruption"] = bool(dst_band.get("has_corruption", False) or src_band.get("has_corruption", False))
    _write_json(dst_root / MANIFEST, dst_manifest)
    return offset_delta, page_id_delta, src_manifest


def _merge_session_indexes(dst_root: Path, src_root: Path) -> None:
    for src_path in src_root.glob("*-session-index.json"):
        dst_path = dst_root / src_path.name
        src = _load_json(src_path, {"sessions": {}})
        dst = _load_json(dst_path, {"sessions": {}})
        sessions = dst.setdefault("sessions", {})
        for session_id, nodes in src.get("sessions", {}).items():
            merged = set(sessions.get(session_id, []))
            merged.update(nodes)
            sessions[session_id] = sorted(merged)
        _write_json(dst_path, dst)


def _copy_base(base: Path, output: Path, replace: bool) -> None:
    if output.exists():
        if not replace:
            raise SystemExit(f"output exists: {output}")
        backup = output.with_name(f"{output.name}.backup-{int(time.time())}")
        output.rename(backup)
    shutil.copytree(base, output)


def merge_roots(base: Path, sources: list[Path], output: Path, replace: bool) -> dict[str, Any]:
    _copy_base(base, output, replace)
    merged = {
        "output": str(output),
        "base": str(base),
        "sources": [],
        "context_events": 0,
        "context_nodes": 0,
    }
    dst_index = _load_json(output / INDEX, {})
    _merge_session_indexes(output, base)
    for src_root in sources:
        if not (src_root / INDEX).exists():
            continue
        offset_delta, page_id_delta, _ = _append_pages(output, src_root)
        src_index = _load_json(src_root / INDEX, {})
        src_index = _offset_addresses(src_index, offset_delta=offset_delta, page_id_delta=page_id_delta)
        _merge_tree(dst_index, src_index)
        _merge_session_indexes(output, src_root)
        merged["sources"].append({"root": str(src_root), "offset_delta": offset_delta, "page_id_delta": page_id_delta})
    _write_json(output / INDEX, dst_index)
    marker = {
        "contract": "shared_rust_hook_store_complete",
        "completed_at_ms": int(time.time() * 1000),
        "base": str(base),
        "sources": [str(src) for src in sources],
    }
    _write_json(output / ".matrixark-local-context-backfill.complete.json", marker)
    merged["context_events"] = len(dst_index.get("context_events", {}))
    merged["context_nodes"] = len(dst_index.get("context_nodes", {}))
    merged["index_bytes"] = (output / INDEX).stat().st_size
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--source", action="append", default=[], type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    report = merge_roots(args.base, args.source, args.output, args.replace)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
