#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Benchmark MatrixArk context pipelines against local or C++ storage.")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--events", type=int, default=120, help="Number of messages/events to ingest.")
    parser.add_argument("--queries", type=int, default=30)
    parser.add_argument(
        "--ingest-mode",
        choices=["batch", "one-by-one"],
        default="batch",
        help=(
            "batch uses matrixark_batch_extract over logical session windows, closer to "
            "VikingMem's >=20-message memory extraction. one-by-one uses matrixark_ingest."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Messages per logical extraction batch when --ingest-mode=batch.",
    )
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:mcp:bench:{int(time.time() * 1000)}")
    parser.add_argument(
        "--restart-before-query",
        action="store_true",
        help="Restart MCP after ingestion so retrieval reloads records from the selected storage backend.",
    )
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
    line = proc.stdout.readline()
    if not line:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"MCP server exited before response. stderr={stderr}")
    response = json.loads(line)
    if "error" in response:
        raise RuntimeError(response["error"])
    return json.loads(response["result"]["content"][0]["text"])


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = min(len(values) - 1, max(0, round((pct / 100.0) * (len(values) - 1))))
    return values[index]


def generated_message(index: int, *, topic: str, project: str) -> Json:
    amount = 1000 + index
    templates = [
        f"User {index % 7} recorded {topic} context item {index} for {project}; amount {amount}.",
        f"Assistant confirmed {topic} context item {index} for {project} with amount {amount}.",
        f"Correction for {project}: the latest {topic} amount is {amount}.",
        f"Current plan for {project} keeps {topic} item {index} active.",
    ]
    return {
        "role": "user" if index % 2 == 0 else "assistant",
        "content": templates[index % len(templates)],
    }


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if args.ingest_mode == "batch" and args.batch_size < 20:
        raise SystemExit("--batch-size must be at least 20 for VikingMem-style benchmark ingestion")

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
        temp_log = tempfile.NamedTemporaryFile(prefix="matrixark-local-bench-", suffix=".jsonl", delete=False)
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
    def start_proc() -> subprocess.Popen[str]:
        return subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

    proc = start_proc()
    ingest_latencies: list[float] = []
    retrieval_latencies: list[float] = []
    hits = 0
    batches = 0
    ingested_messages = 0
    request_id = 1
    try:
        if args.ingest_mode == "one-by-one":
            for i in range(args.events):
                team = f"team-{i % 3}"
                project = f"project-{i % 5}"
                session = f"session-{i % 9}"
                topic = ["gpu", "budget", "approval", "runbook", "customer"][i % 5]
                started = time.perf_counter()
                call(
                    proc,
                    request_id,
                    "matrixark_ingest",
                    {
                        "messages": [generated_message(i, topic=topic, project=project)],
                        "scope": {
                            "user_id": f"user-{i % 7}",
                            "session_id": session,
                            "team": team,
                            "project": project,
                        },
                        "metadata": {"node_path": [team, project, topic]},
                    },
                )
                ingest_latencies.append((time.perf_counter() - started) * 1000.0)
                ingested_messages += 1
                request_id += 1
        else:
            for start in range(0, args.events, args.batch_size):
                batch_index = start // args.batch_size
                count = min(args.batch_size, args.events - start)
                if count < args.batch_size:
                    break
                team = f"team-{batch_index % 3}"
                project = f"project-{batch_index % 5}"
                session = f"batch-session-{batch_index}"
                topic = ["gpu", "budget", "approval", "runbook", "customer"][batch_index % 5]
                messages = [
                    generated_message(start + offset, topic=topic, project=project)
                    for offset in range(count)
                ]
                started = time.perf_counter()
                result = call(
                    proc,
                    request_id,
                    "matrixark_batch_extract",
                    {
                        "messages": messages,
                        "scope": {
                            "user_id": f"user-{batch_index % 7}",
                            "session_id": session,
                            "team": team,
                            "project": project,
                        },
                        "metadata": {"node_path": [team, project, topic]},
                        "threshold_messages": args.batch_size,
                    },
                )
                if result.get("status") != "accepted":
                    raise RuntimeError(f"batch extraction was not accepted: {result}")
                ingest_latencies.append((time.perf_counter() - started) * 1000.0)
                batches += 1
                ingested_messages += int(result.get("events_written", count))
                request_id += 1

        if args.restart_before_query:
            proc.kill()
            proc.wait(timeout=5)
            proc = start_proc()

        for i in range(args.queries):
            if args.ingest_mode == "batch":
                batch_index = i % max(1, batches)
                team = f"team-{batch_index % 3}"
                project = f"project-{batch_index % 5}"
                topic = ["gpu", "budget", "approval", "runbook", "customer"][batch_index % 5]
            else:
                team = f"team-{i % 3}"
                project = f"project-{i % 5}"
                topic = ["gpu", "budget", "approval", "runbook", "customer"][i % 5]
            query = f"What {topic} context exists for {project}?"
            started = time.perf_counter()
            result = call(
                proc,
                request_id,
                "matrixark_retrieve",
                {
                    "query": query,
                    "scope": {"team": team, "project": project},
                    "max_context_tokens": 8,
                },
            )
            retrieval_latencies.append((time.perf_counter() - started) * 1000.0)
            selected = result.get("selected_refs", [])
            if any(topic in str(item.get("text", "")).lower() for item in selected):
                hits += 1
            request_id += 1
    finally:
        proc.kill()
        proc.wait(timeout=5)

    report = {
        "status": "passed",
        "backend": args.backend,
        "storage_log_mode": "sharded_compact_count_log" if args.backend == "temporalstore-direct" else "jsonl",
        "ingest_mode": args.ingest_mode,
        "events_requested": args.events,
        "messages_ingested": ingested_messages,
        "batch_size": args.batch_size if args.ingest_mode == "batch" else 1,
        "batches": batches,
        "queries": args.queries,
        "hit_rate": round(hits / max(1, args.queries), 6),
        "ingest_latency_ms": {
            "avg": round(statistics.mean(ingest_latencies), 3),
            "p50": round(percentile(ingest_latencies, 50), 3),
            "p95": round(percentile(ingest_latencies, 95), 3),
        },
        "retrieve_latency_ms": {
            "avg": round(statistics.mean(retrieval_latencies), 3),
            "p50": round(percentile(retrieval_latencies, 50), 3),
            "p95": round(percentile(retrieval_latencies, 95), 3),
        },
        "metaserver": args.metaserver if args.backend == "temporalstore-direct" else "",
        "storage_prefix": args.storage_prefix if args.backend == "temporalstore-direct" else "",
        "restart_before_query": args.restart_before_query,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if temp_log:
        Path(temp_log.name).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
