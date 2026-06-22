#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Validate deterministic Entity Update Algorithm patches.")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:eua:test:{int(time.time() * 1000)}")
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


def call(proc: subprocess.Popen[str], request_id: int, name: str, arguments: Json) -> Json:
    request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(json.dumps(request) + "\n")
    proc.stdin.flush()
    response = json.loads(proc.stdout.readline())
    if "error" in response:
        raise RuntimeError(response["error"])
    return json.loads(response["result"]["content"][0]["text"])


def start_server(args: argparse.Namespace, event_log: str | None) -> subprocess.Popen[str]:
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(root / "tools" / "matrixark_mcp_server.py"),
        "--line-json",
        "--backend",
        args.backend,
    ]
    if args.backend == "local":
        command.extend(["--event-log", str(event_log)])
    else:
        command.extend(
            [
                "--metaserver",
                args.metaserver,
                "--namespace",
                args.namespace,
                "--table",
                args.table,
                "--temporalstore-lib",
                args.temporalstore_lib,
                "--storage-prefix",
                args.storage_prefix,
            ]
        )
    env = os.environ.copy()
    env["TEMPORALSTORE_LIB"] = args.temporalstore_lib
    env["PYTHONPATH"] = str(root / "sdk" / "python") + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def batch_messages(content: str) -> list[Json]:
    return [{"role": "user" if i % 2 == 0 else "assistant", "content": content} for i in range(20)]


def main() -> int:
    args = parse_args()
    temp_log = None
    if args.backend == "local":
        temp_log = tempfile.NamedTemporaryFile(prefix="matrixark-eua-", suffix=".jsonl", delete=False)
        temp_log.close()
    proc = start_server(args, temp_log.name if temp_log else None)
    scope = {"user_id": "user-eua", "session_id": "session-eua", "team": "infra", "project": "project-1"}
    metadata = {"node_path": ["infra", "project-1", "preferences"]}
    try:
        first = call(
            proc,
            1,
            "matrixark_batch_extract",
            {
                "messages": batch_messages("I prefer coffee."),
                "scope": scope,
                "metadata": metadata,
                "threshold_messages": 20,
            },
        )
        second = call(
            proc,
            2,
            "matrixark_batch_extract",
            {
                "messages": batch_messages("I prefer jasmine tea now, not coffee."),
                "scope": scope,
                "metadata": metadata,
                "threshold_messages": 20,
            },
        )
        retrieval = call(
            proc,
            3,
            "matrixark_retrieve",
            {
                "query": "What is the current preference?",
                "scope": {"user_id": "user-eua", "session_id": "session-eua", "team": "infra"},
                "max_context_tokens": 8,
            },
        )
        replay = call(proc, 4, "matrixark_replay", {"context_pack_id": retrieval["context_pack_id"]})
    finally:
        proc.kill()
        proc.wait(timeout=5)
    records = replay.get("events", [])
    entity_records = [record for record in records if record.get("record_type") == "context_entity"]
    update_audits = [record for record in records if record.get("record_type") == "context_entity_update_audit"]
    latest_preference = next(
        (
            record
            for record in reversed(entity_records)
            if record.get("entity_type") == "preference"
        ),
        {},
    )
    result = {
        "status": "passed",
        "backend": args.backend,
        "first_batch": first,
        "second_batch": second,
        "latest_preference_state": latest_preference.get("state", ""),
        "latest_update_mode": latest_preference.get("update_mode", ""),
        "update_audit_count": len(update_audits),
        "retrieved_ref_types": [item.get("ref_type") for item in retrieval.get("selected_refs", [])],
    }
    if "jasmine tea" not in str(result["latest_preference_state"]).lower():
        raise AssertionError(f"preference was not patched to jasmine tea: {result}")
    if result["latest_update_mode"] != "deterministic_eua":
        raise AssertionError(f"EUA update mode missing: {result}")
    if not update_audits or any(audit.get("llm_calls") != 0 for audit in update_audits):
        raise AssertionError(f"EUA audit missing or used LLM: {update_audits}")
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if temp_log:
        Path(temp_log.name).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
