#!/usr/bin/env python3
"""Export deterministic C++ storage migration artifacts for Rust replay.

The exporter is intentionally migration-only: it preserves the logical
object/page/slot/index/oplog shape expected from the C++ side, while Rust keeps
its native page and log formats for live storage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "compat" / "storage_migration_corpus.json"


def stable_hash(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(data).hexdigest()


def command_key(command: dict[str, Any]) -> str:
    if "key" in command:
        return str(command["key"])
    if command.get("kind", "").startswith("context_"):
        tenant = command.get("tenant_hash", 0)
        node = command.get("node_hash") or command.get("node", {}).get("node_hash", 0)
        return f"context:{tenant}:{node}"
    return command.get("kind", "unknown")


def routing_slot(key: str) -> int:
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:2], "big") % 16_384


def build_case_artifacts(case: dict[str, Any]) -> dict[str, Any]:
    objects: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    slots: dict[int, dict[str, Any]] = {}
    index_records: list[dict[str, Any]] = []
    oplog_records: list[dict[str, Any]] = []

    sequence = 0
    for step in case["operations"]:
        if not step.get("storage_mutation", False):
            continue
        sequence += 1
        command = step["command"]
        key = command_key(command)
        slot = routing_slot(key)
        object_id = f"cpp-object-{case['shard_id']}-{sequence:06d}"
        page_id = f"cpp-page-{case['shard_id']}-{sequence:06d}"
        payload_checksum = stable_hash(command)
        objects.append(
            {
                "object_id": object_id,
                "key": key,
                "command_kind": command["kind"],
                "routing_slot": slot,
                "logical_bytes": len(json.dumps(command, sort_keys=True).encode()),
                "source_step": step["name"],
            }
        )
        pages.append(
            {
                "page_id": page_id,
                "object_id": object_id,
                "routing_slot": slot,
                "contains_timestamp_key": "timestamp_ms" in json.dumps(command),
                "payload_checksum": payload_checksum,
                "rust_target_format": "native_page_envelope_v5",
            }
        )
        slot_summary = slots.setdefault(
            slot,
            {
                "routing_slot": slot,
                "dirty_generation": 0,
                "object_ids": [],
                "page_ids": [],
                "logical_bytes": 0,
                "physical_bytes": 0,
            },
        )
        slot_summary["dirty_generation"] += 1
        slot_summary["object_ids"].append(object_id)
        slot_summary["page_ids"].append(page_id)
        slot_summary["logical_bytes"] += objects[-1]["logical_bytes"]
        slot_summary["physical_bytes"] += len(payload_checksum)
        index_records.append(
            {
                "sequence": sequence,
                "key": key,
                "routing_slot": slot,
                "page_id": page_id,
                "object_id": object_id,
                "command_kind": command["kind"],
            }
        )
        oplog_records.append(
            {
                "sequence": sequence,
                "step": step["name"],
                "command": command,
                "checksum": payload_checksum,
            }
        )

    return {
        "objects": objects,
        "pages": pages,
        "slots": list(slots.values()),
        "index": index_records,
        "oplog": oplog_records,
    }


def write_json(path: Path, value: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return stable_hash(value)


def export(corpus_path: Path, output_dir: Path) -> dict[str, Any]:
    corpus = json.loads(corpus_path.read_text())
    output_dir.mkdir(parents=True, exist_ok=True)
    cases = []
    for case in corpus["cases"]:
        case_dir = output_dir / "cases" / case["name"]
        artifacts = build_case_artifacts(case)
        checksums = {
            name: write_json(case_dir / f"{name}.json", value)
            for name, value in artifacts.items()
        }
        cases.append(
            {
                "case_name": case["name"],
                "shard_id": case["shard_id"],
                "artifact_files": {name: f"cases/{case['name']}/{name}.json" for name in artifacts},
                "checksums": checksums,
                "expected_read_count": len(case["expected_reads"]),
            }
        )

    manifest = {
        "schema_version": 1,
        "name": "temporalstore-cpp-storage-migration-artifacts",
        "compatibility_decision": "migration_only_rust_native_pages",
        "source_corpus": str(corpus_path),
        "source_format": corpus["source_format"],
        "rust_target_format": "rust_native_page_log_formats",
        "artifact_kinds": ["objects", "pages", "slots", "index", "oplog"],
        "cases": cases,
    }
    manifest["checksum"] = write_json(output_dir / "manifest.json", manifest)
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = export(args.corpus, args.output)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
