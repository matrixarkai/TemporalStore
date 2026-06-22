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
    parser = argparse.ArgumentParser(description="Validate MatrixArk one-pass batch memory extraction.")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:one-pass:test:{int(time.time() * 1000)}")
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


def build_messages() -> list[Json]:
    base = [
        "I prefer jasmine tea now, not coffee.",
        "My manager Alice approved the GPU budget.",
        "The current plan is to buy two GPUs next month.",
        "Correction: the budget is 42000 dollars instead of 40000.",
        "I moved to Seattle for the new infra role.",
        "My teammate Bob owns the deployment runbook.",
        "Yes, that approval answer is correct.",
    ]
    messages = []
    for index in range(21):
        messages.append({"role": "user" if index % 2 == 0 else "assistant", "content": base[index % len(base)]})
    return messages


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(root / "tools" / "matrixark_mcp_server.py"),
        "--line-json",
        "--backend",
        args.backend,
    ]
    temp_log = None
    if args.backend == "local":
        temp_log = tempfile.NamedTemporaryFile(prefix="matrixark-one-pass-", suffix=".jsonl", delete=False)
        temp_log.close()
        command.extend(["--event-log", temp_log.name])
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
    proc = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        scope = {"user_id": "user-batch", "session_id": "session-batch", "team": "infra", "project": "project-1"}
        batch = call(
            proc,
            1,
            "matrixark_batch_extract",
            {
                "messages": build_messages(),
                "scope": scope,
                "metadata": {"node_path": ["infra", "project-1", "session_batch"]},
                "threshold_messages": 20,
            },
        )
        retrieval = call(
            proc,
            2,
            "matrixark_retrieve",
            {
                "query": "What is the current tea preference and GPU budget?",
                "scope": {"user_id": "user-batch", "session_id": "session-batch", "team": "infra"},
                "max_context_tokens": 8,
            },
        )
    finally:
        proc.kill()
        proc.wait(timeout=5)
    result = {
        "status": "passed",
        "backend": args.backend,
        "batch": batch,
        "retrieved_refs": len(retrieval.get("selected_refs", [])),
        "retrieved_ref_types": [item.get("ref_type") for item in retrieval.get("selected_refs", [])],
    }
    if batch.get("mode") != "matrixark_one_pass_schema" or not batch.get("one_pass"):
        raise AssertionError(f"batch extraction did not use one-pass schema: {batch}")
    if batch.get("events_written", 0) < 20:
        raise AssertionError(f"expected >=20 events written: {batch}")
    if batch.get("entities_written", 0) < 3:
        raise AssertionError(f"expected multiple entities: {batch}")
    if "entity" not in result["retrieved_ref_types"]:
        raise AssertionError(f"expected entity retrieval: {retrieval}")
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if temp_log:
        Path(temp_log.name).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
