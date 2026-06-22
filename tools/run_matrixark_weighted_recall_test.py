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
    parser = argparse.ArgumentParser(description="Validate MatrixArk weighted multi-path recall.")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:weighted-recall:test:{int(time.time() * 1000)}")
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


def ingest(proc: subprocess.Popen[str], request_id: int, text: str, *, metadata: Json) -> Json:
    return call(
        proc,
        request_id,
        "matrixark_ingest",
        {
            "messages": [{"role": "user", "content": text}],
            "scope": {"user_id": "weighted-user", "session_id": "weighted-session", "team": "infra"},
            "metadata": metadata,
        },
    )


def main() -> int:
    args = parse_args()
    temp_log = None
    if args.backend == "local":
        temp_log = tempfile.NamedTemporaryFile(prefix="matrixark-weighted-", suffix=".jsonl", delete=False)
        temp_log.close()
    proc = start_server(args, temp_log.name if temp_log else None)
    try:
        ingest(
            proc,
            1,
            "Alice approved the urgent GPU budget purchase for Project Orion.",
            metadata={
                "node_path": ["infra", "finance", "approvals"],
                "business_weight": 1.0,
            },
        )
        ingest(
            proc,
            2,
            "The GPU chat had a casual lunch note with no approval decision.",
            metadata={
                "node_path": ["infra", "social", "notes"],
                "business_weight": 0.05,
            },
        )
        ingest(
            proc,
            3,
            "Compliance note filed for the quarterly controls package.",
            metadata={
                "node_path": ["infra", "risk", "ledger"],
                "business_weight": 0.6,
            },
        )
        retrieval = call(
            proc,
            4,
            "matrixark_retrieve",
            {
                "query": "GPU approval risk ledger",
                "scope": {"user_id": "weighted-user", "session_id": "weighted-session", "team": "infra"},
                "max_context_tokens": 6,
                "reference_time_ms": int(time.time() * 1000) + 3 * 24 * 60 * 60 * 1000,
                "ranking": {
                    "weights": {"time": 0.2, "business": 0.35},
                    "freshness_tolerance_ms": 60 * 1000,
                    "half_life_ms": 24 * 60 * 60 * 1000,
                    "business_type_weights": {
                        "confirmation": 1.0,
                        "dialogue_batch": 0.2,
                    },
                    "auxiliary_quota": 2,
                },
            },
        )
    finally:
        proc.kill()
        proc.wait(timeout=5)

    selected = retrieval["selected_refs"]
    first = selected[0] if selected else {}
    score_fields = {"origin_score", "time_score", "business_score", "final_score", "recall_path"}
    if not selected:
        raise AssertionError("expected selected refs")
    if not score_fields.issubset(first):
        raise AssertionError(f"missing weighted score fields: {first}")
    if "approved" not in first.get("text", "").lower():
        raise AssertionError(f"business-important approval did not rank first: {selected}")
    if retrieval.get("auxiliary_candidate_count", 0) <= 0:
        raise AssertionError(f"expected auxiliary keyword graph candidates: {retrieval}")
    if not any(item.get("time_score", 0) < 1.0 for item in selected):
        raise AssertionError(f"expected time decay below 1.0 with future reference time: {selected}")

    result = {
        "status": "passed",
        "backend": args.backend,
        "recall_policy": retrieval.get("recall_policy", {}),
        "primary_candidate_count": retrieval.get("primary_candidate_count"),
        "auxiliary_candidate_count": retrieval.get("auxiliary_candidate_count"),
        "top_ref": first,
        "selected_refs": selected,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if temp_log:
        Path(temp_log.name).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
