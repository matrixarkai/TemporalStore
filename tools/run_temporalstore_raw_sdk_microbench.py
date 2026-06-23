#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

Json = dict[str, Any]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Raw C++ TemporalStore direct SDK hset/hget microbenchmark.")
    parser.add_argument("--ops", type=int, default=1000)
    parser.add_argument("--write-workers", type=int, default=4)
    parser.add_argument("--read-workers", type=int, default=8)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--key-prefix", default=f"matrixark:raw-sdk:{int(time.time() * 1000)}")
    parser.add_argument("--artifact-dir", default=".local/context-debug/raw-sdk-microbench")
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


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


def make_client(args: argparse.Namespace):
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "sdk" / "python"))
    from temporalstore import Client, Options  # type: ignore

    return Client(
        Options(
            metaserver_addr=args.metaserver,
            namespace_name=args.namespace,
            table_name=args.table,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        ),
        library_path=args.temporalstore_lib,
    )


class ThreadLocalClientPool:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.local = threading.local()

    def get(self):
        client = getattr(self.local, "client", None)
        if client is None:
            client = make_client(self.args)
            self.local.client = client
        return client


def run_parallel(total_ops: int, workers: int, fn) -> tuple[list[Json], list[Json], float]:
    started = time.perf_counter()
    ok: list[Json] = []
    errors: list[Json] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fn, index): index for index in range(total_ops)}
        for future in as_completed(futures):
            index = futures[future]
            try:
                ok.append(future.result())
            except Exception as exc:
                errors.append({"op_index": index, "error": str(exc)})
    elapsed = time.perf_counter() - started
    ok.sort(key=lambda item: item["op_index"])
    errors.sort(key=lambda item: item["op_index"])
    return ok, errors, elapsed


def write_report(report: Json, artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "temporalstore_raw_sdk_microbench.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# TemporalStore Raw C++ Direct SDK Microbenchmark",
        "",
        "## Summary",
        "",
        f"- status: `{report['status']}`",
        f"- key_prefix: `{report['key_prefix']}`",
        f"- payload_bytes: `{report['payload_bytes']}`",
        f"- write workers: `{report['write']['workers']}`",
        f"- read workers: `{report['read']['workers']}`",
        f"- write QPS: `{report['write']['qps']}`",
        f"- read QPS: `{report['read']['qps']}`",
        f"- write errors: `{len(report['write']['errors'])}`",
        f"- read errors: `{len(report['read']['errors'])}`",
        "",
        "## Latency",
        "",
        "```json",
        json.dumps({"write": report["write"]["latency_ms"], "read": report["read"]["latency_ms"]}, indent=2, sort_keys=True),
        "```",
        "",
        "## Scope",
        "",
        "This microbenchmark intentionally bypasses MatrixArk extraction, OSS models, tree traversal, token packing, and JSON record replay. It measures direct SDK hash writes and reads against the live C++ service.",
    ]
    md = "\n".join(lines) + "\n"
    (artifact_dir / "temporalstore_raw_sdk_microbench.md").write_text(md, encoding="utf-8")
    (artifact_dir / "temporalstore_raw_sdk_microbench.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>TemporalStore Raw SDK Microbenchmark</title>"
        "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1120px;margin:32px auto;padding:0 24px;color:#172033;line-height:1.45}"
        "pre{background:#f6f8fb;border:1px solid #dde5f0;padding:14px;overflow:auto;border-radius:8px}</style><pre>"
        + md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        + "</pre>",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    payload = "x" * max(1, args.payload_bytes)
    write_pool = ThreadLocalClientPool(args)
    read_pool = ThreadLocalClientPool(args)

    def write_one(index: int) -> Json:
        client = write_pool.get()
        started = time.perf_counter()
        client.hset(f"{args.key_prefix}:hash:{index % 64:04d}", f"{index:020d}", payload)
        latency_ms = (time.perf_counter() - started) * 1000.0
        return {"op_index": index, "latency_ms": round(latency_ms, 3)}

    write_results, write_errors, write_elapsed = run_parallel(args.ops, args.write_workers, write_one)

    def read_one(index: int) -> Json:
        client = read_pool.get()
        started = time.perf_counter()
        value = client.hget(f"{args.key_prefix}:hash:{index % 64:04d}", f"{index:020d}")
        latency_ms = (time.perf_counter() - started) * 1000.0
        return {"op_index": index, "latency_ms": round(latency_ms, 3), "bytes": len(value)}

    read_results, read_errors, read_elapsed = run_parallel(args.ops, args.read_workers, read_one)
    write_latencies = [float(item["latency_ms"]) for item in write_results]
    read_latencies = [float(item["latency_ms"]) for item in read_results]
    report = {
        "status": "passed" if not write_errors and not read_errors else "completed_with_errors",
        "backend": "temporalstore-direct-sdk",
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "temporalstore_lib": args.temporalstore_lib,
        "key_prefix": args.key_prefix,
        "payload_bytes": args.payload_bytes,
        "write": {
            "ops_requested": args.ops,
            "ops_ok": len(write_results),
            "workers": args.write_workers,
            "elapsed_sec": round(write_elapsed, 3),
            "qps": round(len(write_results) / write_elapsed, 3) if write_elapsed else 0.0,
            "latency_ms": latency_summary(write_latencies),
            "errors": write_errors,
        },
        "read": {
            "ops_requested": args.ops,
            "ops_ok": len(read_results),
            "workers": args.read_workers,
            "elapsed_sec": round(read_elapsed, 3),
            "qps": round(len(read_results) / read_elapsed, 3) if read_elapsed else 0.0,
            "latency_ms": latency_summary(read_latencies),
            "errors": read_errors,
        },
    }
    artifact_dir = Path(args.artifact_dir)
    write_report(report, artifact_dir)
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ["status", "backend", "key_prefix", "write", "read"]}, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
