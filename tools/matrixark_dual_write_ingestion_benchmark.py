#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Measure MatrixArk live-ingestion dual-write QPS and latency."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from matrixark_mcp_core import stable_hash  # noqa: E402
from matrixark_mcp_temporal_adapters import MatrixArkTemporalStoreDirectAdapter  # noqa: E402
from matrixark_raw_message_storage_contract import (  # noqa: E402
    RawMessageStorageTarget,
    contract_report,
    raw_message_marker,
)

Json = dict[str, Any]
RAW_BACKEND_CHOICES = ["temporalstore", "matrixkv", "s3", "objectstore"]
RAW_BACKEND_SWEEP_CHOICES = ["temporalstore", "matrixkv"]


class BenchmarkError(RuntimeError):
    pass


class InMemoryDualWriteClient:
    """Native-client stand-in that preserves the direct adapter append API."""

    def __init__(self, *, write_delay_us: int = 0) -> None:
        self._lock = threading.RLock()
        self._strings: dict[str, str] = {}
        self._hashes: dict[tuple[str, str], str] = {}
        self.write_delay_s = max(0, write_delay_us) / 1_000_000.0
        self.calls_by_path: dict[str, int] = {}
        self.calls_by_raw_backend: dict[str, int] = {}
        self.entries_by_path: dict[str, int] = {}

    def get_string(self, key: str) -> str:
        with self._lock:
            return self._strings.get(key, "")

    def put_string(self, key: str, value: str) -> None:
        with self._lock:
            self._strings[key] = value

    def hset(self, key: str, field: str, value: str) -> None:
        with self._lock:
            self._hashes[(key, field)] = value

    def hget(self, key: str, field: str) -> str:
        with self._lock:
            return self._hashes.get((key, field), "")

    def batch_hset(self, entries: list[Json]) -> None:
        with self._lock:
            for entry in entries:
                self._hashes[(str(entry["key"]), str(entry["field"]))] = str(entry["value"])

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        if self.write_delay_s:
            time.sleep(self.write_delay_s)
        options = append_options or {}
        path = str(options.get("append_path") or "unknown")
        raw_backend = str(options.get("raw_storage_backend") or "").strip() or "serving"
        with self._lock:
            self.calls_by_path[path] = self.calls_by_path.get(path, 0) + 1
            self.calls_by_raw_backend[raw_backend] = self.calls_by_raw_backend.get(raw_backend, 0) + 1
            self.entries_by_path[path] = self.entries_by_path.get(path, 0) + len(entries)
            for entry in entries:
                self._hashes[(str(entry["key"]), str(entry["field"]))] = str(entry["value"])
            if count_key is not None and count_value is not None:
                self._strings[str(count_key)] = str(count_value)


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    index = min(len(values) - 1, max(0, math.ceil(q * len(values)) - 1))
    return sorted(values)[index]


def make_record(sequence: int, *, payload_bytes: int, scope_key: str) -> Json:
    text = f"dual-write benchmark record {sequence} " + ("x" * max(0, payload_bytes))
    return {
        "record_type": "context_event",
        "event_id_hash": stable_hash(f"dual-write-benchmark:{sequence}"),
        "tenant_hash": 1001,
        "scope_key": scope_key,
        "node_hash": sequence % 64,
        "updated_at_ms": 1780000000000 + sequence,
        "event_time_ms": 1780000000000 + sequence,
        "body": text,
        "text": text,
        "source_kind": "benchmark",
    }


def make_direct_adapter(args: argparse.Namespace, client: Any | None = None) -> MatrixArkTemporalStoreDirectAdapter:
    if client is None:
        return MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.library_path,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    adapter = MatrixArkTemporalStoreDirectAdapter.__new__(MatrixArkTemporalStoreDirectAdapter)
    adapter._client = client
    adapter._metaserver = args.metaserver
    adapter._namespace = args.namespace
    adapter._table = args.table
    adapter._storage_prefix = args.storage_prefix.rstrip(":")
    adapter._record_hash_key = f"{adapter._storage_prefix}:records"
    adapter._index_key = f"{adapter._storage_prefix}:record_index"
    adapter._count_key = f"{adapter._storage_prefix}:record_count"
    adapter._raw_ingestion_prefix = (args.raw_storage_prefix or f"{adapter._storage_prefix}:raw_ingestion").rstrip(":")
    adapter._raw_record_hash_key = f"{adapter._raw_ingestion_prefix}:records"
    adapter._raw_count_key = f"{adapter._raw_ingestion_prefix}:record_count"
    adapter._raw_storage_backend = args.raw_backend
    adapter._raw_entry_count_cache = None
    # This benchmark exists to measure both writes, and its reported return policy
    # claims append_many waits for both, so the raw half has to be on here.
    adapter._direct_raw_ingestion_enabled = True
    adapter._shard_size = args.shard_size
    adapter._index_cache = None
    adapter._records_cache = None
    adapter._retrieval_candidate_cache = {}
    adapter._retrieval_candidate_cache_lock = threading.RLock()
    adapter._entry_count_cache = None
    adapter._legacy_index_mode = False
    adapter._records_lock = threading.RLock()
    adapter._write_retries = 0
    adapter._write_backoff_s = 0.0
    adapter._write_throttle_s = 0.0
    return adapter


def evaluate_performance_gate(args: argparse.Namespace, summary: Json) -> Json:
    min_qps = float(getattr(args, "min_ingestion_qps", 0.0) or 0.0)
    max_p95_ms = float(getattr(args, "max_batch_p95_ms", 0.0) or 0.0)
    require_counts = bool(getattr(args, "require_dual_write_counts", False))
    checks: list[Json] = []
    observed_qps = float(summary.get("ingestion_qps", 0.0) or 0.0)
    checks.append({
        "metric": "ingestion_qps",
        "observed": round(observed_qps, 3),
        "minimum": min_qps,
        "passed": observed_qps >= min_qps,
    })
    p95_ms = float((summary.get("caller_visible_batch_latency_ms") or {}).get("p95", 0.0) or 0.0)
    if max_p95_ms > 0.0:
        checks.append({
            "metric": "caller_visible_batch_latency_ms_p95",
            "observed": round(p95_ms, 3),
            "maximum": max_p95_ms,
            "passed": p95_ms <= max_p95_ms,
        })
    if require_counts:
        counts_validated = bool(summary.get("dual_write_counts_validated"))
        checks.append({
            "metric": "dual_write_counts_validated",
            "observed": 1 if counts_validated else 0,
            "minimum": 1,
            "passed": counts_validated,
        })
    enabled = min_qps > 0.0 or max_p95_ms > 0.0 or require_counts
    passed = all(bool(check["passed"]) for check in checks) if checks else True
    return {
        "enabled": enabled,
        "passed": passed,
        "min_ingestion_qps": min_qps,
        "max_batch_p95_ms": max_p95_ms,
        "require_dual_write_counts": require_counts,
        "checks": checks,
    }


def parse_raw_backends(value: str, fallback: str) -> list[str]:
    selected = (value or "").strip()
    if not selected:
        selected = fallback
    if selected == "both":
        return list(RAW_BACKEND_SWEEP_CHOICES)
    backends: list[str] = []
    for item in selected.split(","):
        backend = item.strip()
        if not backend:
            continue
        if backend not in RAW_BACKEND_CHOICES:
            raise BenchmarkError(f"unsupported raw backend {backend!r}; expected one of {', '.join(RAW_BACKEND_CHOICES)} or both")
        if backend not in backends:
            backends.append(backend)
    if not backends:
        raise BenchmarkError("--raw-backends selected no backends")
    return backends


def summarize_sweep(results: list[Json]) -> Json:
    qps_values = [float(result.get("ingestion_qps", 0.0) or 0.0) for result in results]
    p95_values = [float((result.get("caller_visible_batch_latency_ms") or {}).get("p95", 0.0) or 0.0) for result in results]
    qps_min = min(qps_values) if qps_values else 0.0
    qps_max = max(qps_values) if qps_values else 0.0
    p95_max = max(p95_values) if p95_values else 0.0
    return {
        "ingestion_qps": {
            "avg": round(statistics.fmean(qps_values), 3) if qps_values else 0.0,
            "min": round(qps_min, 3),
            "max": round(qps_max, 3),
            "min_max_ratio": round(qps_min / qps_max, 6) if qps_max > 0 else 0.0,
        },
        "caller_visible_batch_latency_ms_p95": {
            "avg": round(statistics.fmean(p95_values), 3) if p95_values else 0.0,
            "max": round(p95_max, 3),
        },
    }


def prometheus_escape(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus(summary: Json) -> str:
    lines = [
        "# HELP matrixark_dual_write_ingestion_status Dual-write ingestion benchmark status.",
        "# TYPE matrixark_dual_write_ingestion_status gauge",
    ]
    status = str(summary.get("status") or "unknown")
    raw_backend = str(summary.get("raw_backend") or "")
    if isinstance(summary.get("results"), list):
        lines.append(f'matrixark_dual_write_ingestion_status{{status="{prometheus_escape(status)}"}} {1 if status == "ok" else 0}')
        lines.extend([
            "# HELP matrixark_dual_write_ingestion_qps Caller-visible dual-write records per second.",
            "# TYPE matrixark_dual_write_ingestion_qps gauge",
            "# HELP matrixark_dual_write_ingestion_batch_latency_ms Caller-visible append_many batch latency.",
            "# TYPE matrixark_dual_write_ingestion_batch_latency_ms gauge",
            "# HELP matrixark_dual_write_ingestion_records_total Dual-write ingestion records processed.",
            "# TYPE matrixark_dual_write_ingestion_records_total gauge",
            "# HELP matrixark_dual_write_ingestion_counts_validated Local-mode proof that raw and serving append paths completed.",
            "# TYPE matrixark_dual_write_ingestion_counts_validated gauge",
        ])
        for result in summary.get("results") or []:
            backend = prometheus_escape(result.get("raw_backend") or "")
            result_status = str(result.get("status") or "unknown")
            labels = f'raw_backend="{backend}",status="{prometheus_escape(result_status)}"'
            lines.append(f"matrixark_dual_write_ingestion_qps{{{labels}}} {float(result.get('ingestion_qps', 0.0) or 0.0)}")
            p95 = float((result.get("caller_visible_batch_latency_ms") or {}).get("p95", 0.0) or 0.0)
            lines.append(f'matrixark_dual_write_ingestion_batch_latency_ms{{raw_backend="{backend}",quantile="p95"}} {p95}')
            lines.append(f'matrixark_dual_write_ingestion_records_total{{raw_backend="{backend}"}} {int(result.get("records", 0) or 0)}')
            counts_validated = 1 if bool(result.get("dual_write_counts_validated")) else 0
            lines.append(f'matrixark_dual_write_ingestion_counts_validated{{raw_backend="{backend}"}} {counts_validated}')
        ratio = float(((summary.get("summary") or {}).get("ingestion_qps") or {}).get("min_max_ratio", 0.0) or 0.0)
        lines.extend([
            "# HELP matrixark_dual_write_ingestion_backend_qps_ratio Slowest selected raw backend QPS divided by fastest selected backend QPS.",
            "# TYPE matrixark_dual_write_ingestion_backend_qps_ratio gauge",
            f"matrixark_dual_write_ingestion_backend_qps_ratio {ratio}",
        ])
    else:
        lines.append(f'matrixark_dual_write_ingestion_status{{raw_backend="{prometheus_escape(raw_backend)}",status="{prometheus_escape(status)}"}} {1 if status == "ok" else 0}')
        lines.extend([
            "# HELP matrixark_dual_write_ingestion_qps Caller-visible dual-write records per second.",
            "# TYPE matrixark_dual_write_ingestion_qps gauge",
            f'matrixark_dual_write_ingestion_qps{{raw_backend="{prometheus_escape(raw_backend)}",status="{prometheus_escape(status)}"}} {float(summary.get("ingestion_qps", 0.0) or 0.0)}',
            "# HELP matrixark_dual_write_ingestion_batch_latency_ms Caller-visible append_many batch latency.",
            "# TYPE matrixark_dual_write_ingestion_batch_latency_ms gauge",
            f'matrixark_dual_write_ingestion_batch_latency_ms{{raw_backend="{prometheus_escape(raw_backend)}",quantile="p95"}} {float((summary.get("caller_visible_batch_latency_ms") or {}).get("p95", 0.0) or 0.0)}',
            "# HELP matrixark_dual_write_ingestion_records_total Dual-write ingestion records processed.",
            "# TYPE matrixark_dual_write_ingestion_records_total gauge",
            f'matrixark_dual_write_ingestion_records_total{{raw_backend="{prometheus_escape(raw_backend)}"}} {int(summary.get("records", 0) or 0)}',
            "# HELP matrixark_dual_write_ingestion_counts_validated Local-mode proof that raw and serving append paths completed.",
            "# TYPE matrixark_dual_write_ingestion_counts_validated gauge",
            f'matrixark_dual_write_ingestion_counts_validated{{raw_backend="{prometheus_escape(raw_backend)}"}} {1 if bool(summary.get("dual_write_counts_validated")) else 0}',
        ])
    gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    gate_status = "passed" if gate.get("passed", True) else "failed"
    lines.extend([
        "# HELP matrixark_dual_write_ingestion_performance_gate_status Dual-write ingestion performance gate status.",
        "# TYPE matrixark_dual_write_ingestion_performance_gate_status gauge",
        f'matrixark_dual_write_ingestion_performance_gate_status{{status="{gate_status}"}} {1 if gate.get("passed", True) else 0}',
    ])
    return "\n".join(lines) + "\n"


def run_backend_sweep(args: argparse.Namespace) -> Json:
    backends = parse_raw_backends(getattr(args, "raw_backends", ""), args.raw_backend)
    min_backend_qps_ratio = float(getattr(args, "min_backend_qps_ratio", 0.0) or 0.0)
    if min_backend_qps_ratio < 0.0:
        raise BenchmarkError("--min-backend-qps-ratio must be non-negative")
    results: list[Json] = []
    for backend in backends:
        one_args = argparse.Namespace(**vars(args))
        one_args.raw_backend = backend
        one_args.raw_backends = ""
        results.append(run_benchmark(one_args))
    sweep_summary = summarize_sweep(results)
    gate_checks: list[Json] = []
    for result in results:
        gate = result.get("performance_gate") if isinstance(result.get("performance_gate"), dict) else {}
        for check in gate.get("checks", []):
            enriched = dict(check)
            enriched["raw_backend"] = result.get("raw_backend")
            gate_checks.append(enriched)
    if min_backend_qps_ratio > 0.0 and len(results) > 1:
        observed_ratio = float((sweep_summary.get("ingestion_qps") or {}).get("min_max_ratio", 0.0) or 0.0)
        gate_checks.append({
            "metric": "backend_ingestion_qps_ratio",
            "observed": round(observed_ratio, 6),
            "minimum": min_backend_qps_ratio,
            "passed": observed_ratio >= min_backend_qps_ratio,
        })
    gate_enabled = any(bool((result.get("performance_gate") or {}).get("enabled")) for result in results) or min_backend_qps_ratio > 0.0
    gate_passed = all(bool((result.get("performance_gate") or {}).get("passed", True)) for result in results)
    gate_passed = gate_passed and all(bool(check.get("passed", True)) for check in gate_checks)
    status = "ok" if all(result.get("status") == "ok" for result in results) and gate_passed else "failed"
    return {
        "status": status,
        "mode": args.mode,
        "raw_backends": backends,
        "records_per_backend": args.records,
        "total_records": sum(int(result.get("records", 0) or 0) for result in results),
        "workers": args.workers,
        "batch_size": args.batch_size,
        "payload_bytes": args.payload_bytes,
        "dual_write_return_policy": "append_many returns after raw message append and serving TemporalStore append both finish",
        "raw_message_storage_contract": {
            "schema": "matrixark.raw_message_storage_contract.v1",
            "raw_backends": backends,
            "default_backend": "temporalstore",
            "uses_timestamp_and_event_key": True,
            "stored_value_mode": "raw_body_utf8",
        },
        "results": results,
        "summary": sweep_summary,
        "performance_gate": {
            "enabled": gate_enabled,
            "passed": gate_passed,
            "min_backend_qps_ratio": min_backend_qps_ratio,
            "checks": gate_checks,
        },
    }


def run_benchmark(args: argparse.Namespace) -> Json:
    if args.records <= 0:
        raise BenchmarkError("--records must be positive")
    if args.workers <= 0:
        raise BenchmarkError("--workers must be positive")
    if args.batch_size <= 0:
        raise BenchmarkError("--batch-size must be positive")
    if float(getattr(args, "min_ingestion_qps", 0.0) or 0.0) < 0.0:
        raise BenchmarkError("--min-ingestion-qps must be non-negative")
    if float(getattr(args, "max_batch_p95_ms", 0.0) or 0.0) < 0.0:
        raise BenchmarkError("--max-batch-p95-ms must be non-negative")
    client = InMemoryDualWriteClient(write_delay_us=args.local_write_delay_us) if args.mode == "local" else None
    adapter = make_direct_adapter(args, client)
    latencies_ms: list[float] = []
    latency_lock = threading.Lock()
    sequence_lock = threading.Lock()
    next_sequence = 0

    def next_batch() -> list[Json]:
        nonlocal next_sequence
        with sequence_lock:
            start = next_sequence
            if start >= args.records:
                return []
            end = min(args.records, start + args.batch_size)
            next_sequence = end
        return [make_record(seq, payload_bytes=args.payload_bytes, scope_key=args.scope_key) for seq in range(start, end)]

    def worker() -> int:
        written = 0
        while True:
            batch = next_batch()
            if not batch:
                return written
            started = time.perf_counter()
            adapter.append_many(batch)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            with latency_lock:
                latencies_ms.append(elapsed_ms)
            written += len(batch)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(worker) for _ in range(args.workers)]
        total_written = sum(future.result() for future in as_completed(futures))
    elapsed_s = max(0.000001, time.perf_counter() - started)
    latencies_sorted = sorted(latencies_ms)
    raw_count = getattr(adapter, "_raw_entry_count_cache", None)
    serving_log_entries = getattr(adapter, "_entry_count_cache", None)
    sample_record = make_record(0, payload_bytes=args.payload_bytes, scope_key=args.scope_key)
    if args.raw_backend == "matrixkv":
        sample_target = RawMessageStorageTarget.matrixkv(
            args.namespace,
            "raw_agent_messages",
            "sample/raw-message",
        )
    elif args.raw_backend == "s3":
        sample_target = RawMessageStorageTarget.s3(bucket="matrixark-large-resources", prefix="raw-agent-messages")
    elif args.raw_backend == "objectstore":
        sample_target = RawMessageStorageTarget.objectstore()
    else:
        sample_target = RawMessageStorageTarget.temporalstore()
    raw_contract = contract_report(sample_record, sample_target, event_id_hash=int(sample_record["event_id_hash"]))
    raw_contract["marker"] = raw_message_marker(
        sample_record,
        target=sample_target,
        event_id_hash=int(sample_record["event_id_hash"]),
    ) if args.raw_backend != "temporalstore" else {}

    summary: Json = {
        "status": "ok",
        "mode": args.mode,
        "records": total_written,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "payload_bytes": args.payload_bytes,
        "elapsed_ms": round(elapsed_s * 1000.0, 3),
        "ingestion_qps": round(total_written / elapsed_s, 3),
        "batch_qps": round(len(latencies_ms) / elapsed_s, 3),
        "caller_visible_batch_latency_ms": {
            "samples": len(latencies_ms),
            "avg": round(statistics.fmean(latencies_ms), 3) if latencies_ms else 0.0,
            "p50": round(percentile(latencies_sorted, 0.50), 3),
            "p95": round(percentile(latencies_sorted, 0.95), 3),
            "p99": round(percentile(latencies_sorted, 0.99), 3),
            "max": round(max(latencies_ms), 3) if latencies_ms else 0.0,
        },
        "caller_visible_record_latency_ms_estimate": {
            "avg": round((statistics.fmean(latencies_ms) / args.batch_size), 6) if latencies_ms else 0.0,
            "p95": round((percentile(latencies_sorted, 0.95) / args.batch_size), 6) if latencies_ms else 0.0,
        },
        "dual_write_return_policy": "append_many returns after raw message append and serving TemporalStore append both finish",
        "raw_backend": args.raw_backend,
        "raw_message_storage_contract": raw_contract,
        "storage_prefix": args.storage_prefix,
        "raw_storage_prefix": getattr(adapter, "_raw_ingestion_prefix", args.raw_storage_prefix or f"{args.storage_prefix}:raw_ingestion"),
        "raw_record_count_observed": raw_count,
        "serving_log_entries_observed": serving_log_entries,
    }
    if client is not None:
        summary["local_native_call_counts"] = {
            "calls_by_append_path": dict(sorted(client.calls_by_path.items())),
            "calls_by_raw_backend": dict(sorted(client.calls_by_raw_backend.items())),
            "entries_by_append_path": dict(sorted(client.entries_by_path.items())),
        }
        summary["dual_write_counts_validated"] = (
            raw_count == total_written
            and serving_log_entries is not None
            and serving_log_entries > 0
            and client.calls_by_raw_backend.get(args.raw_backend, 0) > 0
            and client.calls_by_path.get("native_append_queue", 0) > 0
        )
    performance_gate = evaluate_performance_gate(args, summary)
    summary["performance_gate"] = performance_gate
    summary["status"] = "ok" if performance_gate["passed"] else "failed"
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark synchronous MatrixArk dual-write ingestion QPS and latency.")
    parser.add_argument("--mode", choices=["local", "direct"], default=os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_MODE", "local"))
    parser.add_argument("--records", type=int, default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_RECORDS", "10000")))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_WORKERS", "4")))
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_BATCH_SIZE", "128")))
    parser.add_argument("--payload-bytes", type=int, default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_PAYLOAD_BYTES", "128")))
    parser.add_argument("--scope-key", default=os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_SCOPE_KEY", "benchmark:tenant=1001"))
    parser.add_argument("--local-write-delay-us", type=int, default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_LOCAL_WRITE_DELAY_US", "0")))
    parser.add_argument("--storage-prefix", default=os.environ.get("MATRIXARK_STORAGE_PREFIX", "matrixark:mcp:bench"))
    parser.add_argument("--raw-storage-prefix", default=os.environ.get("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX", ""))
    parser.add_argument(
        "--raw-backend",
        choices=RAW_BACKEND_CHOICES,
        default=os.environ.get("MATRIXARK_RAW_INGESTION_BACKEND", "temporalstore"),
        help="Raw-message durability backend label used by the direct adapter.",
    )
    parser.add_argument(
        "--raw-backends",
        default=os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_RAW_BACKENDS", ""),
        help="Run a backend sweep for temporalstore, matrixkv, both, or a comma-separated subset. Empty means --raw-backend only.",
    )
    parser.add_argument("--shard-size", type=int, default=int(os.environ.get("MATRIXARK_DIRECT_RECORD_LOG_SHARD_SIZE", "4096")))
    parser.add_argument("--metaserver", default=os.environ.get("TEMPORALSTORE_METASERVER", "127.0.0.1:65000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_NAMESPACE", "matrixark"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TABLE", "context"))
    parser.add_argument("--library-path", default=os.environ.get("TEMPORALSTORE_LIBRARY_PATH", ""))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("TEMPORALSTORE_REQUEST_TIMEOUT_MS", "20000")))
    parser.add_argument("--io-timeout-ms", type=int, default=int(os.environ.get("TEMPORALSTORE_IO_TIMEOUT_MS", "20000")))
    parser.add_argument("--min-ingestion-qps", type=float, default=float(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_MIN_INGESTION_QPS", "0")), help="optional release gate for minimum caller-visible records per second")
    parser.add_argument("--max-batch-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_MAX_BATCH_P95_MS", "0")), help="optional release gate for maximum p95 append_many latency in milliseconds, 0 disables")
    parser.add_argument("--min-backend-qps-ratio", type=float, default=float(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_MIN_BACKEND_QPS_RATIO", "0")), help="sweep-mode gate: slowest selected raw backend QPS must be at least this fraction of fastest selected backend QPS")
    parser.add_argument("--require-dual-write-counts", type=int, choices=[0, 1], default=int(os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_REQUIRE_COUNTS", "0")), help="require local-mode proof that both raw and serving append paths completed before return")
    parser.add_argument("--json-output", default=os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_JSON", ""))
    parser.add_argument("--prometheus-output", default=os.environ.get("MATRIXARK_DUAL_WRITE_BENCH_PROMETHEUS", ""), help="optional Prometheus-compatible metrics output path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_backend_sweep(args) if getattr(args, "raw_backends", "") else run_benchmark(args)
    text = json.dumps(summary, indent=2, sort_keys=True)
    print(text)
    if args.json_output:
        Path(args.json_output).write_text(text + "\n")
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(render_prometheus(summary))
    return 0 if summary.get("status") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
