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
    parser = argparse.ArgumentParser(description="Validate intelligent memory segmentation.")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:segmentation:test:{int(time.time() * 1000)}")
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


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


def messages() -> list[Json]:
    return [
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello"},
        {"role": "user", "content": "Recursion means a function calls itself to solve a smaller subproblem."},
        {"role": "assistant", "content": "The base case is essential in recursion, otherwise it can run forever."},
        {"role": "user", "content": "For the game algorithm, minimax scores moves for an opponent."},
        {"role": "assistant", "content": "Alpha beta pruning can speed up the game search."},
        {"role": "user", "content": "Merge sort uses recursion to split arrays and merge sorted halves."},
        {"role": "assistant", "content": "Recursion efficiency depends on subproblem size, branching, and stack depth."},
        {"role": "user", "content": "Thanks"},
        {"role": "assistant", "content": "Okay"},
    ] + [
        {"role": "user", "content": f"filler acknowledgement {i}"}
        for i in range(10)
    ]


def main() -> int:
    args = parse_args()
    temp_log = None
    if args.backend == "local":
        temp_log = tempfile.NamedTemporaryFile(prefix="matrixark-seg-", suffix=".jsonl", delete=False)
        temp_log.close()
    proc = start_server(args, temp_log.name if temp_log else None)
    scope = {"user_id": "user-seg", "session_id": "session-seg", "team": "learning", "project": "algorithms"}
    try:
        batch = call(
            proc,
            1,
            "matrixark_batch_extract",
            {
                "messages": messages(),
                "scope": scope,
                "metadata": {"node_path": ["learning", "algorithms", "session_batch"]},
                "threshold_messages": 20,
            },
        )
        retrieval = call(
            proc,
            2,
            "matrixark_retrieve",
            {
                "query": "Explain recursion, base case warnings, merge sort, and recursion efficiency.",
                "scope": {"user_id": "user-seg", "session_id": "session-seg", "team": "learning"},
                "max_context_tokens": 8,
            },
        )
        replay = call(proc, 3, "matrixark_replay", {"context_pack_id": retrieval["context_pack_id"]})
    finally:
        proc.kill()
        proc.wait(timeout=5)
    segments = [record for record in replay.get("events", []) if record.get("record_type") == "context_segment"]
    recursion = next((segment for segment in segments if segment.get("topic") == "recursion"), {})
    game = next((segment for segment in segments if segment.get("topic") == "game_algorithm"), {})
    selected_segments = [item for item in retrieval.get("selected_refs", []) if item.get("ref_type") == "segment"]
    result = {
        "status": "passed",
        "backend": args.backend,
        "batch": batch,
        "segment_count": len(segments),
        "recursion_segment": recursion,
        "game_segment": game,
        "selected_segment_topics": [item.get("topic") for item in selected_segments],
        "selected_ref_types": [item.get("ref_type") for item in retrieval.get("selected_refs", [])],
    }
    if not recursion or not recursion.get("non_contiguous"):
        raise AssertionError(f"expected non-contiguous recursion segment: {result}")
    if "game_algorithm" in result["selected_segment_topics"][:1]:
        raise AssertionError(f"game segment outranked recursion for recursion query: {result}")
    if "recursion" not in result["selected_segment_topics"]:
        raise AssertionError(f"recursion segment was not retrieved: {result}")
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if temp_log:
        Path(temp_log.name).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
