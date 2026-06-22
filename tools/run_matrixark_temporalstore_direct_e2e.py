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
    parser = argparse.ArgumentParser(
        description="Run MatrixArk extraction/ingestion/retrieval through native C++ TemporalStore storage."
    )
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:mcp:e2e:{int(time.time() * 1000)}")
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


def call(proc: subprocess.Popen[str], request_id: int, name: str, arguments: Json) -> Json:
    request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    assert proc.stdin is not None
    assert proc.stdout is not None
    proc.stdin.write(json.dumps(request) + "\n")
    proc.stdin.flush()
    line = proc.stdout.readline()
    if not line:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"MCP server exited before response. stderr={stderr}")
    response = json.loads(line)
    if "error" in response:
        raise RuntimeError(response["error"])
    text = response["result"]["content"][0]["text"]
    return json.loads(text)


def read_record_count(args: argparse.Namespace) -> int:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "sdk" / "python"))
    from temporalstore import Client, Options  # type: ignore

    options = Options(
        metaserver_addr=args.metaserver,
        namespace_name=args.namespace,
        table_name=args.table,
        request_timeout_ms=20000,
        io_timeout_ms=20000,
        max_read_retries=2,
        max_write_retries=1,
    )
    with Client(options, library_path=args.temporalstore_lib) as client:
        raw = client.get_string(f"{args.storage_prefix}:record_index")
        return len(json.loads(raw))


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    server = root / "tools" / "matrixark_mcp_server.py"
    env = os.environ.copy()
    env["TEMPORALSTORE_LIB"] = args.temporalstore_lib
    env["PYTHONPATH"] = str(root / "sdk" / "python") + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(server),
            "--line-json",
            "--backend",
            "temporalstore-direct",
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
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        scope = {"user_id": "user-a", "session_id": "session-gpu", "team": "infra", "project": "project-1"}
        ingest_1 = call(
            proc,
            1,
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU purchase for project 1 today."}],
                "scope": scope,
                "metadata": {"node_path": ["infra", "project-1", "approvals"]},
            },
        )
        ingest_2 = call(
            proc,
            2,
            "matrixark_ingest",
            {
                "messages": [{"role": "assistant", "content": "The approved GPU budget is 42000 dollars."}],
                "scope": scope,
                "metadata": {"node_path": ["infra", "project-1", "cost"]},
            },
        )
        retrieve_1 = call(
            proc,
            3,
            "matrixark_retrieve",
            {
                "query": "What GPU approval and budget are current for project 1?",
                "scope": {"user_id": "user-a", "session_id": "session-gpu", "team": "infra"},
                "max_context_tokens": 8,
            },
        )
        feedback = call(
            proc,
            4,
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "Yes, that is correct."}],
                "scope": scope,
                "context_pack_id": retrieve_1["context_pack_id"],
                "accepted_refs": retrieve_1["selected_refs"][:1],
            },
        )
        retrieve_2 = call(
            proc,
            5,
            "matrixark_retrieve",
            {
                "query": "Was the GPU budget answer confirmed?",
                "scope": {"user_id": "user-a", "session_id": "session-gpu", "team": "infra"},
                "max_context_tokens": 8,
            },
        )
        stored_records = read_record_count(args)
        result = {
            "status": "passed",
            "backend": "temporalstore-direct",
            "metaserver": args.metaserver,
            "namespace": args.namespace,
            "table": args.table,
            "storage_prefix": args.storage_prefix,
            "stored_record_count": stored_records,
            "ingest_classifications": [
                ingest_1["classification"],
                ingest_2["classification"],
                feedback["classification"],
            ],
            "first_retrieve_selected": len(retrieve_1["selected_refs"]),
            "second_retrieve_selected": len(retrieve_2["selected_refs"]),
            "context_pack_id": retrieve_1["context_pack_id"],
            "feedback_prior_context": feedback.get("prior_context", ""),
            "feedback_prior_refs": len(feedback.get("prior_refs", [])),
            "temporalstore_lib": args.temporalstore_lib,
        }
        if result["first_retrieve_selected"] < 1:
            raise AssertionError("first retrieval returned no context")
        if feedback["classification"] != "CONFIRMATION":
            raise AssertionError(f"feedback was not confirmation: {feedback}")
        if stored_records < 8:
            raise AssertionError(f"too few records persisted to TemporalStore: {stored_records}")
        print(json.dumps(result, indent=2, sort_keys=True))
        if args.report_json:
            Path(args.report_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    finally:
        proc.kill()
        proc.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
