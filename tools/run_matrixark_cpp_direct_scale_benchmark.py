#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from tools.matrixark_mcp_server import (
    MatrixArkMcpServer,
    MatrixArkTemporalStoreDirectAdapter,
    embedding_for_text,
    oss_encoder_rank_labels,
    QUERY_TYPE_LABELS,
    UNDERSTANDING_LABELS,
)

Json = dict[str, Any]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Run concurrent MatrixArk extraction/ingestion/retrieval scale benchmark against real C++ TemporalStore."
    )
    parser.add_argument("--ingest-ops", type=int, default=12)
    parser.add_argument("--retrieve-ops", type=int, default=24)
    parser.add_argument("--ingest-concurrency", type=int, default=2)
    parser.add_argument("--retrieve-concurrency", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-context-tokens", type=int, default=256)
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:cpp:scale:{int(time.time() * 1000)}")
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--artifact-dir", default=".local/context-debug/cpp-direct-scale")
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


def call_tool(server: MatrixArkMcpServer, name: str, arguments: Json) -> Json:
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    )
    if "error" in response:
        raise RuntimeError(response["error"]["message"])
    return json.loads(response["result"]["content"][0]["text"])


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[index]


def latency_summary(values: list[float]) -> Json:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "avg": round(statistics.mean(values), 3),
        "p50": round(percentile(values, 50), 3),
        "p95": round(percentile(values, 95), 3),
        "p99": round(percentile(values, 99), 3),
        "max": round(max(values), 3),
    }


def service(args: argparse.Namespace, prefix: str) -> MatrixArkMcpServer:
    adapter = MatrixArkTemporalStoreDirectAdapter(
        metaserver=args.metaserver,
        namespace=args.namespace,
        table=args.table,
        library_path=args.temporalstore_lib,
        storage_prefix=prefix,
        request_timeout_ms=args.request_timeout_ms,
        io_timeout_ms=args.io_timeout_ms,
    )
    return MatrixArkMcpServer(adapter)


def generated_messages(op_index: int, batch_size: int) -> list[Json]:
    topics = ["approval budget", "current preference", "location update", "role status", "launch plan"]
    topic = topics[op_index % len(topics)]
    messages = []
    for offset in range(batch_size):
        absolute = op_index * batch_size + offset
        project = f"project_{op_index % 7}"
        if offset % 5 == 0:
            content = f"Alice approved the GPU budget for {project}; amount {42000 + absolute}."
        elif offset % 5 == 1:
            content = f"The user currently prefers Rust for low latency context services in {project}."
        elif offset % 5 == 2:
            content = f"The user moved to Austin for the {project} rollout."
        elif offset % 5 == 3:
            content = f"The user's role is AI memory platform owner for {project}."
        else:
            content = f"The current plan is to finish the {topic} benchmark report for {project} Friday."
        messages.append({"role": "user" if offset % 2 == 0 else "assistant", "content": content})
    return messages


def ingest_op(args: argparse.Namespace, op_index: int) -> Json:
    prefix = f"{args.storage_prefix}:ingest:{op_index:06d}"
    scope = {
        "account_id": "acct_scale",
        "tenant_id": "tenant_scale",
        "user_id": f"user_{op_index % 5}",
        "session_id": f"scale_session_{op_index}",
    }
    node_path = ["scale_memory", f"user_{op_index % 5}", f"session_{op_index}", f"topic_{op_index % 5}"]
    server = service(args, prefix)
    started = time.perf_counter()
    result = call_tool(
        server,
        "matrixark_batch_extract",
        {
            "messages": generated_messages(op_index, args.batch_size),
            "scope": scope,
            "metadata": {"node_path": node_path},
            "threshold_messages": args.batch_size,
            "understanding_provider": "oss_encoder",
            "segment_provider": "oss_encoder",
        },
    )
    refresh = call_tool(server, "matrixark_refresh_summaries", {"scope": scope, "limit": 128})
    latency_ms = (time.perf_counter() - started) * 1000.0
    records = server.adapter.read_all()
    return {
        "status": "ok",
        "op_index": op_index,
        "storage_prefix": prefix,
        "scope": scope,
        "node_path": node_path,
        "latency_ms": round(latency_ms, 3),
        "events_written": result.get("events_written", 0),
        "entities_written": result.get("entities_written", 0),
        "segments_written": result.get("segments_written", 0),
        "indexes_written": result.get("indexes_written", 0),
        "summary_refresh": refresh,
        "record_count": len(records),
    }


def retrieve_op(args: argparse.Namespace, ingest_result: Json, op_index: int) -> Json:
    server = service(args, str(ingest_result["storage_prefix"]))
    topic = ["approval", "preference", "location", "role", "plan"][op_index % 5]
    query = {
        "approval": "Who approved the GPU budget and what amount is current?",
        "preference": "What does the user currently prefer for low latency services?",
        "location": "Where is the user currently located?",
        "role": "What is the user's current role?",
        "plan": "What is the current benchmark plan?",
    }[topic]
    started = time.perf_counter()
    pack = call_tool(
        server,
        "matrixark_retrieve",
        {
            "query": query,
            "scope": ingest_result["scope"],
            "max_context_tokens": args.max_context_tokens,
            "ranking": {
                "top_k_per_layer": 8,
                "max_children_scored_per_parent": 10000,
                "auxiliary_quota": 4,
            },
        },
    )
    latency_ms = (time.perf_counter() - started) * 1000.0
    return {
        "status": "ok",
        "op_index": op_index,
        "storage_prefix": ingest_result["storage_prefix"],
        "query": query,
        "latency_ms": round(latency_ms, 3),
        "selected_ref_count": len(pack.get("selected_refs", [])),
        "used_remote_context_tokens": pack.get("used_remote_context_tokens", 0),
        "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", 0),
        "question_type": pack.get("question_type", ""),
        "insufficient_context": pack.get("insufficient_context", False),
    }


def run_phase(total_ops: int, concurrency: int, fn) -> tuple[list[Json], list[Json], float]:
    started = time.perf_counter()
    ok: list[Json] = []
    errors: list[Json] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(fn, index): index for index in range(total_ops)}
        for future in as_completed(futures):
            index = futures[future]
            try:
                ok.append(future.result())
            except Exception as exc:  # Keep going to expose failure thresholds.
                errors.append({"op_index": index, "error": str(exc)})
    elapsed = time.perf_counter() - started
    ok.sort(key=lambda row: row.get("op_index", 0))
    errors.sort(key=lambda row: row.get("op_index", 0))
    return ok, errors, elapsed


def process_snapshot() -> Json:
    try:
        output = subprocess.check_output(
            "ps -eo pid,comm,%cpu,%mem,rss,args | grep -E 'bcache2-(server|metaserver)' | grep -v grep",
            shell=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        output = ""
    rows = []
    for line in output.splitlines():
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        rows.append(
            {
                "pid": parts[0],
                "command": parts[1],
                "cpu_percent": parts[2],
                "mem_percent": parts[3],
                "rss_kb": parts[4],
                "args": parts[5],
            }
        )
    return {"processes": rows}


def write_report(report: Json, artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "matrixark_cpp_direct_scale_benchmark.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# MatrixArk C++ TemporalStore Concurrent Scale Benchmark",
        "",
        "## Summary",
        "",
        f"- backend: `{report['backend']}`",
        f"- status: `{report['status']}`",
        f"- storage_prefix: `{report['storage_prefix']}`",
        f"- ingest concurrency: `{report['ingest']['concurrency']}`",
        f"- retrieve concurrency: `{report['retrieve']['concurrency']}`",
        f"- model warmup: `{report.get('model_warmup_ms', 0.0)} ms`",
        f"- ingest QPS: `{report['ingest']['qps']}`",
        f"- retrieve QPS: `{report['retrieve']['qps']}`",
        f"- ingest errors: `{len(report['ingest']['errors'])}`",
        f"- retrieve errors: `{len(report['retrieve']['errors'])}`",
        "",
        "## Latency",
        "",
        "```json",
        json.dumps({"ingest": report["ingest"]["latency_ms"], "retrieve": report["retrieve"]["latency_ms"]}, indent=2, sort_keys=True),
        "```",
        "",
        "## What Is Measured",
        "",
        "- Ingest operation = `matrixark_batch_extract` with 20-message logical batch, OSS encoder understanding, ContextEvent/Entity/Segment/Index/Summary/Embedding writes, then `matrixark_refresh_summaries`.",
        "- Retrieve operation = new process-local adapter reads the persisted C++ TemporalStore prefix and runs tree/summary/index/event retrieval into a ContextPack.",
        "- Each ingest worker uses its own storage prefix. This avoids the current Python append-log count key becoming a write serialization artifact and gives a cleaner C++ storage + MatrixArk pipeline cap.",
        "- This is not raw C++ engine QPS. It includes Python orchestration and OSS embedding/query-understanding work.",
        "",
        "## C++ Service Snapshot",
        "",
        "```json",
        json.dumps(report["process_snapshot_after"], indent=2, sort_keys=True),
        "```",
        "",
        "## Sample Ingest Results",
        "",
        "```json",
        json.dumps(report["ingest"]["results"][:5], indent=2, sort_keys=True),
        "```",
        "",
        "## Sample Retrieve Results",
        "",
        "```json",
        json.dumps(report["retrieve"]["results"][:5], indent=2, sort_keys=True),
        "```",
    ]
    md = "\n".join(lines) + "\n"
    (artifact_dir / "matrixark_cpp_direct_scale_benchmark.md").write_text(md, encoding="utf-8")
    (artifact_dir / "matrixark_cpp_direct_scale_benchmark.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>MatrixArk C++ Scale Benchmark</title>"
        "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1120px;margin:32px auto;padding:0 24px;color:#172033;line-height:1.45}"
        "pre{background:#f6f8fb;border:1px solid #dde5f0;padding:14px;overflow:auto;border-radius:8px}code{background:#edf2f7;padding:2px 4px;border-radius:4px}</style>"
        + "<pre>" + md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre>",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    if args.batch_size < 20:
        raise SystemExit("--batch-size must be >= 20")
    if args.ingest_concurrency <= 0 or args.retrieve_concurrency <= 0:
        raise SystemExit("concurrency must be positive")

    os.environ.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "oss")
    os.environ.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "1")
    os.environ.setdefault("MATRIXARK_UNDERSTANDING_PROVIDER", "oss_encoder")
    os.environ.setdefault("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "1")

    warmup_started = time.perf_counter()
    embedding_for_text("MatrixArk warmup for OSS embeddings.")
    oss_encoder_rank_labels("current approval budget preference location role", UNDERSTANDING_LABELS, limit=3)
    oss_encoder_rank_labels("what is current?", QUERY_TYPE_LABELS, limit=2)
    model_warmup_ms = round((time.perf_counter() - warmup_started) * 1000.0, 3)

    process_before = process_snapshot()
    ingest_results, ingest_errors, ingest_elapsed = run_phase(
        args.ingest_ops,
        args.ingest_concurrency,
        lambda index: ingest_op(args, index),
    )
    retrieve_inputs = ingest_results or []
    if retrieve_inputs:
        def retrieve_by_index(index: int) -> Json:
            return retrieve_op(args, retrieve_inputs[index % len(retrieve_inputs)], index)
        retrieve_results, retrieve_errors, retrieve_elapsed = run_phase(
            args.retrieve_ops,
            args.retrieve_concurrency,
            retrieve_by_index,
        )
    else:
        retrieve_results, retrieve_errors, retrieve_elapsed = [], [{"error": "no successful ingest results"}], 0.0

    ingest_latencies = [float(row["latency_ms"]) for row in ingest_results]
    retrieve_latencies = [float(row["latency_ms"]) for row in retrieve_results]
    report = {
        "status": "passed" if not ingest_errors and not retrieve_errors else "completed_with_errors",
        "backend": "temporalstore-direct",
        "storage_prefix": args.storage_prefix,
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "temporalstore_lib": args.temporalstore_lib,
        "embedding_provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER"),
        "embedding_model": os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get("MATRIXARK_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"),
        "understanding_provider": os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER"),
        "model_warmup_ms": model_warmup_ms,
        "process_snapshot_before": process_before,
        "process_snapshot_after": process_snapshot(),
        "ingest": {
            "ops_requested": args.ingest_ops,
            "ops_ok": len(ingest_results),
            "concurrency": args.ingest_concurrency,
            "elapsed_sec": round(ingest_elapsed, 3),
            "qps": round(len(ingest_results) / ingest_elapsed, 3) if ingest_elapsed else 0.0,
            "messages_per_batch": args.batch_size,
            "message_qps": round((len(ingest_results) * args.batch_size) / ingest_elapsed, 3) if ingest_elapsed else 0.0,
            "latency_ms": latency_summary(ingest_latencies),
            "errors": ingest_errors,
            "results": ingest_results,
        },
        "retrieve": {
            "ops_requested": args.retrieve_ops,
            "ops_ok": len(retrieve_results),
            "concurrency": args.retrieve_concurrency,
            "elapsed_sec": round(retrieve_elapsed, 3),
            "qps": round(len(retrieve_results) / retrieve_elapsed, 3) if retrieve_elapsed else 0.0,
            "latency_ms": latency_summary(retrieve_latencies),
            "errors": retrieve_errors,
            "results": retrieve_results,
        },
    }
    artifact_dir = Path(args.artifact_dir)
    write_report(report, artifact_dir)
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ["status", "backend", "storage_prefix", "ingest", "retrieve"]}, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
