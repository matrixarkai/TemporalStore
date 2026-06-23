#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

Json = dict[str, Any]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run Codex hook lifecycle through real C++ TemporalStore.")
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument("--storage-prefix", default=f"matrixark:codex-hook:e2e:{int(time.time() * 1000)}")
    parser.add_argument("--account-id", default="acct_codex")
    parser.add_argument("--tenant-id", default="tenant_codex")
    parser.add_argument("--user-id", default=os.environ.get("USER", "codex_user"))
    parser.add_argument("--session-id", default=f"codex-session-{int(time.time())}")
    parser.add_argument("--artifact-dir", default=".local/context-debug/codex-cpp-hook-e2e")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument("--temporalstore-lib", default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"))
    return parser.parse_args()


def run_hook(args: argparse.Namespace, *, event: str, payload: Json, query: str = "") -> Json:
    command = [
        str(args.repo_root / "tools" / "matrixark_codex_cpp_hook.sh"),
        "--event",
        event,
        "--account-id",
        args.account_id,
        "--tenant-id",
        args.tenant_id,
        "--user-id",
        args.user_id,
        "--session-id",
        args.session_id,
        "--storage-prefix",
        args.storage_prefix,
        "--metaserver",
        args.metaserver,
        "--namespace",
        args.namespace,
        "--table",
        args.table,
        "--temporalstore-lib",
        args.temporalstore_lib,
        "--session-commit-threshold",
        "4",
    ]
    if query:
        command.extend(["--query", query])
    env = os.environ.copy()
    env.update(
        {
            "MATRIXARK_REPO_ROOT": str(args.repo_root),
            "MATRIXARK_MCP_BACKEND": "temporalstore-direct",
            "MATRIXARK_TEMPORALSTORE_PREFIX": args.storage_prefix,
            "MATRIXARK_TEMPORALSTORE_METASERVER": args.metaserver,
            "MATRIXARK_TEMPORALSTORE_NAMESPACE": args.namespace,
            "MATRIXARK_TEMPORALSTORE_TABLE": args.table,
            "TEMPORALSTORE_LIB": args.temporalstore_lib,
            "MATRIXARK_EMBEDDING_PROVIDER": "oss",
            "MATRIXARK_REQUIRE_OSS_EMBEDDINGS": "1",
            "MATRIXARK_EMBEDDING_MODEL_PATH": str(args.repo_root / ".local" / "context-oss-models" / "sentence-transformers" / "all-MiniLM-L6-v2"),
            "MATRIXARK_UNDERSTANDING_PROVIDER": "oss_encoder",
            "MATRIXARK_REQUIRE_OSS_UNDERSTANDING": "1",
        }
    )
    proc = subprocess.run(
        command,
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(args.repo_root),
        env=env,
    )
    if proc.returncode != 0:
        failure = {
            "event": event,
            "returncode": proc.returncode,
            "command": command,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        raise RuntimeError(json.dumps(failure, indent=2, sort_keys=True))
    return {
        "event": event,
        "payload": payload,
        "stdout": json.loads(proc.stdout),
        "stderr": proc.stderr,
    }


def read_cpp_records(args: argparse.Namespace) -> list[Json]:
    sys.path.insert(0, str(args.repo_root / "sdk" / "python"))
    from temporalstore import Client, Options  # type: ignore

    client = Client(
        Options(
            metaserver_addr=args.metaserver,
            namespace_name=args.namespace,
            table_name=args.table,
            request_timeout_ms=60000,
            io_timeout_ms=60000,
            max_read_retries=2,
            max_write_retries=1,
        ),
        library_path=args.temporalstore_lib,
    )
    try:
        raw_count = client.get_string(f"{args.storage_prefix}:record_count")
        count = int(raw_count or "0")
        records = []
        for sequence in range(count):
            shard = sequence // 256
            offset = sequence % 256
            payload = client.hget(f"{args.storage_prefix}:records:{shard:06d}", f"{offset:020d}")
            if payload:
                records.append(json.loads(payload))
        return records
    finally:
        client.close()


def main() -> int:
    args = parse_args()
    artifact_dir = Path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    events = [
        run_hook(
            args,
            event="UserPromptSubmit",
            payload={"prompt": "Remember that Alice approved the GPU budget for Project Orion."},
        ),
        run_hook(
            args,
            event="UserPromptSubmit",
            payload={"prompt": "Remember that I prefer Rust for low latency context systems."},
        ),
        run_hook(
            args,
            event="UserPromptSubmit",
            payload={"prompt": "I moved to Austin for the MatrixArk benchmark rollout."},
        ),
        run_hook(
            args,
            event="UserPromptSubmit",
            payload={"prompt": "My role is AI memory platform owner."},
        ),
        run_hook(
            args,
            event="Stop",
            payload={"message": "Codex turn completed; commit the useful session memory."},
        ),
        run_hook(
            args,
            event="UserPromptSubmit",
            payload={"prompt": "What was approved and what do I prefer now?"},
            query="What was approved and what do I prefer now?",
        ),
    ]
    records = read_cpp_records(args)
    counts: dict[str, int] = {}
    for record in records:
        key = str(record.get("record_type", "unknown"))
        counts[key] = counts.get(key, 0) + 1
    final_retrieve = events[-1]["stdout"].get("retrieve", {})
    report = {
        "status": "passed",
        "backend": "temporalstore-direct",
        "storage_prefix": args.storage_prefix,
        "account_id": args.account_id,
        "tenant_id": args.tenant_id,
        "user_id": args.user_id,
        "session_id": args.session_id,
        "events": events,
        "record_count": len(records),
        "record_counts": dict(sorted(counts.items())),
        "final_selected_ref_count": final_retrieve.get("selected_ref_count", 0),
        "final_context_pack_id": final_retrieve.get("context_pack_id"),
        "sample_records": records[:24],
    }
    json_path = artifact_dir / "matrixark_codex_cpp_hook_e2e.json"
    md_path = artifact_dir / "matrixark_codex_cpp_hook_e2e.md"
    html_path = artifact_dir / "matrixark_codex_cpp_hook_e2e.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md = "\n".join(
        [
            "# MatrixArk Codex Hook E2E With C++ TemporalStore",
            "",
            f"- status: `{report['status']}`",
            f"- backend: `{report['backend']}`",
            f"- storage_prefix: `{report['storage_prefix']}`",
            f"- account/user/session: `{args.account_id}` / `{args.user_id}` / `{args.session_id}`",
            f"- C++ record count: `{report['record_count']}`",
            f"- final selected refs: `{report['final_selected_ref_count']}`",
            "",
            "## Record Counts",
            "",
            "```json",
            json.dumps(report["record_counts"], indent=2, sort_keys=True),
            "```",
            "",
            "## Hook Events",
            "",
            "```json",
            json.dumps(
                [
                    {
                        "event": event["event"],
                        "status": event["stdout"].get("status"),
                        "ingest_status": event["stdout"].get("ingest", {}).get("status"),
                        "selected_ref_count": event["stdout"].get("retrieve", {}).get("selected_ref_count", 0),
                        "session_commit": event["stdout"].get("session_commit", {}),
                    }
                    for event in events
                ],
                indent=2,
                sort_keys=True,
            ),
            "```",
        ]
    )
    md_path.write_text(md + "\n", encoding="utf-8")
    html_path.write_text(
        "<!doctype html><meta charset='utf-8'><title>MatrixArk Codex C++ Hook E2E</title>"
        "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1120px;margin:32px auto;padding:0 24px;color:#172033;line-height:1.45}"
        "pre{background:#f6f8fb;border:1px solid #dde5f0;padding:14px;overflow:auto;border-radius:8px}</style><pre>"
        + md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        + "</pre>",
        encoding="utf-8",
    )
    print(json.dumps({k: report[k] for k in ["status", "backend", "storage_prefix", "record_count", "record_counts", "final_selected_ref_count"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
