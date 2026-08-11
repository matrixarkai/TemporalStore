#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Inspect recent MatrixArk/Codex hook records stored in TemporalStore."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


Json = dict[str, Any]
DEFAULT_SHARD_SIZE = 256


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def add_sdk_path() -> None:
    sdk_path = repo_root() / "sdk" / "python"
    if str(sdk_path) not in sys.path:
        sys.path.insert(0, str(sdk_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--storage-prefix", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--shard-size", type=int, default=DEFAULT_SHARD_SIZE)
    parser.add_argument("--library-path", default=os.environ.get("TEMPORALSTORE_LIB", ""))
    parser.add_argument("--include-serving", action="store_true")
    return parser.parse_args()


def default_prefixes(args: argparse.Namespace) -> list[str]:
    prefixes = list(args.storage_prefix or [])
    env_prefix = os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX")
    if env_prefix:
        prefixes.append(env_prefix)
    prefixes.extend(["matrixark:codex-hook", "matrixark:agent-hook", "matrixark:mcp:codex"])
    seen: set[str] = set()
    unique: list[str] = []
    for prefix in prefixes:
        clean = str(prefix or "").strip().rstrip(":")
        if clean and clean not in seen:
            seen.add(clean)
            unique.append(clean)
    return unique


def nested(obj: Json, path: str) -> Any:
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur.get(part)
    return cur


def first_value(obj: Json, paths: list[str]) -> Any:
    for path in paths:
        value = nested(obj, path)
        if value not in (None, "", []):
            return value
    return None


def text_preview(record: Json) -> str:
    messages = record.get("messages") if isinstance(record.get("messages"), list) else []
    candidates = [
        record.get("text"),
        record.get("content"),
        record.get("message"),
        record.get("summary_text"),
    ]
    if messages and isinstance(messages[0], dict):
        candidates.append(messages[0].get("content"))
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())[:220]
    return ""


def compact_row(prefix: str, log_type: str, sequence: int, record: Json) -> Json:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
    if not hook and isinstance(metadata.get("agent_hook"), dict):
        hook = metadata.get("agent_hook") or {}
    messages = record.get("messages") if isinstance(record.get("messages"), list) else []
    role = record.get("role")
    if not role and messages and isinstance(messages[0], dict):
        role = messages[0].get("role")
    write_debug = record.get("matrixark_write_debug")
    return {
        "prefix": prefix,
        "log_type": log_type,
        "sequence": sequence,
        "record_type": record.get("record_type") or record.get("type"),
        "role": role or "",
        "codex_api_event": metadata.get("codex_event") or metadata.get("agent_event") or hook.get("trigger") or "",
        "hook_type": hook.get("hook_type") or "",
        "hook_id": hook.get("hook_id") or "",
        "hook_observed_at_ms": hook.get("observed_at_ms") or "",
        "session_id": first_value(record, ["scope.session_id", "session_id"]) or first_value(metadata, ["agent_context.session_id"]) or "",
        "session_id_source": hook.get("session_id_source") or metadata.get("codex_session_id_source") or metadata.get("session_id_source") or "",
        "write_debug": write_debug if isinstance(write_debug, dict) else {},
        "text": text_preview(record),
    }


def get_count(client: Any, key: str) -> tuple[int, str]:
    try:
        raw = client.get_string(key)
    except Exception as exc:
        return 0, str(exc)
    if not raw:
        return 0, ""
    try:
        return max(0, int(raw)), ""
    except ValueError:
        return 0, f"invalid count value: {raw!r}"


def get_record(client: Any, base_prefix: str, sequence: int, shard_size: int) -> Json | None:
    shard = sequence // shard_size
    offset = sequence % shard_size
    keys = [
        (f"{base_prefix}:records:{shard:06d}", f"{offset:020d}"),
        (f"{base_prefix}:records", f"{sequence:020d}"),
    ]
    for key, field in keys:
        try:
            raw = client.hget(key, field)
        except Exception:
            raw = ""
        if raw:
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {"_unparsed": raw[:1000]}
            return parsed if isinstance(parsed, dict) else {"value": parsed}
    return None


def inspect_log(client: Any, prefix: str, log_type: str, base_prefix: str, limit: int, shard_size: int) -> Json:
    count, error = get_count(client, f"{base_prefix}:record_count")
    result: Json = {
        "prefix": prefix,
        "log_type": log_type,
        "base_prefix": base_prefix,
        "count": count,
        "error": error,
        "records": [],
    }
    if error or count <= 0:
        return result
    start = max(0, count - max(1, limit))
    for sequence in range(count - 1, start - 1, -1):
        record = get_record(client, base_prefix, sequence, shard_size)
        if record is not None:
            result["records"].append(compact_row(prefix, log_type, sequence, record))
    return result


def main() -> int:
    args = parse_args()
    add_sdk_path()
    from temporalstore.client import Client, Options

    client = Client(
        Options(
            metaserver_addr=args.metaserver,
            namespace_name=args.namespace,
            table_name=args.table,
            request_timeout_ms=5000,
            io_timeout_ms=5000,
            max_read_retries=1,
        ),
        library_path=args.library_path or None,
    )
    summary: Json = {
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "limit": args.limit,
        "logs": [],
    }
    for prefix in default_prefixes(args):
        summary["logs"].append(inspect_log(client, prefix, "raw_ingestion", f"{prefix}:raw_ingestion", args.limit, args.shard_size))
        if args.include_serving:
            summary["logs"].append(inspect_log(client, prefix, "serving", prefix, args.limit, args.shard_size))
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
