#!/usr/bin/env python3
"""Run a live MatrixArk C++ vs Rust TemporalStore scale comparison.

The runner intentionally exercises the same in-process MCP tool boundary used by
agent integrations, while avoiding JSONL/local fallback paths. It writes
side-by-side latency/QPS/error artifacts for ingestion and retrieval.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Keep the scale path storage-focused and avoid replay/audit write amplification.
os.environ.setdefault("MATRIXARK_DIRECT_AUDIT_MODE", "drop")
os.environ.setdefault("MATRIXARK_ENABLE_REPLAY", "0")
os.environ.setdefault("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0")
os.environ.setdefault("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "0")
os.environ.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "hash")
os.environ.setdefault("MATRIXARK_EMBEDDING_MODEL", "hashing-local")
os.environ.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "0")
os.environ.setdefault("MATRIXARK_SEGMENT_PROVIDER", "deterministic")
os.environ.setdefault("MATRIXARK_RETRIEVE_TIMEOUT_MS", "10000")
os.environ.setdefault("MATRIXARK_BACKPRESSURE_TIMEOUT_MS", "5000")
os.environ.setdefault("MATRIXARK_MAX_CONCURRENT_INGEST", "128")
os.environ.setdefault("MATRIXARK_MAX_CONCURRENT_RETRIEVE", "128")

from tools.matrixark_mcp_server import MatrixArkMcpServer  # noqa: E402
from tools.matrixark_mcp_temporal_adapters import (  # noqa: E402
    MatrixArkRustCliClient,
    MatrixArkTemporalStoreDirectAdapter,
    MatrixArkTemporalStoreRustAdapter,
)


Json = dict[str, Any]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return round(float(ordered[index]), 3)


def summarize_latencies(latencies_ms: list[float], *, total_ops: int, elapsed_s: float, errors: int) -> Json:
    return {
        "ops": total_ops,
        "ok": max(0, total_ops - errors),
        "errors": errors,
        "elapsed_s": round(elapsed_s, 3),
        "qps": round((max(0, total_ops - errors) / elapsed_s) if elapsed_s > 0 else 0.0, 3),
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "avg_ms": round(statistics.fmean(latencies_ms), 3) if latencies_ms else 0.0,
        "max_ms": round(max(latencies_ms), 3) if latencies_ms else 0.0,
    }


def selected_ref_count(result: Json) -> int:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    refs = pack.get("refs") or pack.get("selected_refs") or pack.get("context_refs") or []
    if isinstance(refs, list) and refs:
        return len(refs)
    grouped = pack.get("groups")
    if isinstance(grouped, dict):
        return sum(len(v) for v in grouped.values() if isinstance(v, list))
    if isinstance(grouped, list):
        total = 0
        for group in grouped:
            if not isinstance(group, dict):
                continue
            items = group.get("items")
            if isinstance(items, list):
                total += len(items)
                continue
            try:
                total += max(0, int(group.get("n") or 0))
            except (TypeError, ValueError):
                continue
        return total
    return 0


RETRIEVAL_STAGE_METRICS = [
    "query_plan_ms",
    "node_traversal_ms",
    "index_prefilter_ms",
    "candidate_fetch_ms",
    "score_ms",
    "pack_ms",
    "audit_ms",
]


def retrieval_metrics_from_result(result: Json) -> Json:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    metrics = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
    return metrics if isinstance(metrics, dict) else {}


def summarize_retrieval_metrics(rows: list[Json]) -> Json:
    if not rows:
        return {
            "samples": 0,
            "stage_avg_ms": {name: 0.0 for name in RETRIEVAL_STAGE_METRICS},
            "stage_p95_ms": {name: 0.0 for name in RETRIEVAL_STAGE_METRICS},
            "selected_refs_avg": 0.0,
            "scanned_records_avg": 0.0,
            "cache_hit_rate": 0.0,
            "placement_partitions_touched_avg": 0.0,
        }
    stage_values: dict[str, list[float]] = {name: [] for name in RETRIEVAL_STAGE_METRICS}
    selected_refs: list[float] = []
    scanned_records: list[float] = []
    placement_partitions: list[float] = []
    cache_hits = 0
    for row in rows:
        for name in RETRIEVAL_STAGE_METRICS:
            try:
                stage_values[name].append(float(row.get(name) or 0.0))
            except (TypeError, ValueError):
                stage_values[name].append(0.0)
        try:
            selected_refs.append(float(row.get("selected_refs") or 0.0))
        except (TypeError, ValueError):
            selected_refs.append(0.0)
        try:
            scanned_records.append(float(row.get("scanned_records") or 0.0))
        except (TypeError, ValueError):
            scanned_records.append(0.0)
        try:
            placement_partitions.append(float(row.get("placement_partitions_touched") or 0.0))
        except (TypeError, ValueError):
            placement_partitions.append(0.0)
        if bool(row.get("cache_hit")):
            cache_hits += 1
    return {
        "samples": len(rows),
        "stage_avg_ms": {
            name: round(statistics.fmean(values), 3) if values else 0.0
            for name, values in stage_values.items()
        },
        "stage_p95_ms": {
            name: percentile(values, 95) if values else 0.0
            for name, values in stage_values.items()
        },
        "selected_refs_avg": round(statistics.fmean(selected_refs), 3) if selected_refs else 0.0,
        "scanned_records_avg": round(statistics.fmean(scanned_records), 3) if scanned_records else 0.0,
        "cache_hit_rate": round(cache_hits / len(rows), 6) if rows else 0.0,
        "placement_partitions_touched_avg": round(statistics.fmean(placement_partitions), 3) if placement_partitions else 0.0,
    }


def timeout_count(errors: list[str]) -> int:
    return sum(1 for error in errors if "timeout" in str(error).lower() or "timed out" in str(error).lower())


def fallback_flags_from_backend(result: Json) -> Json:
    status = str(result.get("status") or "")
    retrieve = result.get("retrieve", {}) if isinstance(result.get("retrieve"), dict) else {}
    metrics = retrieve.get("stage_metrics", {}) if isinstance(retrieve.get("stage_metrics"), dict) else {}
    readiness = result.get("readiness", {}) if isinstance(result.get("readiness"), dict) else {}
    backend_metrics = result.get("backend_metrics", {}) if isinstance(result.get("backend_metrics"), dict) else {}
    backend_metrics_result = backend_metrics.get("result", {}) if isinstance(backend_metrics.get("result"), dict) else {}
    errors = result.get("errors", {}) if isinstance(result.get("errors"), dict) else {}
    error_text = " ".join(
        str(item)
        for bucket in errors.values()
        for item in (bucket if isinstance(bucket, list) else [bucket])
    ).lower()
    return {
        "backend_startup_failed": status == "backend_startup_failed",
        "topology_not_ready": status == "topology_not_ready" or readiness.get("status") == "topology_not_ready",
        "memory_fallback": "memory fallback" in error_text or bool(result.get("memory_fallback")),
        "hash_embedding_fallback": bool(result.get("embedding_fallback_used") or backend_metrics_result.get("embedding_fallback_used")),
        "partial_context_pack": int(retrieve.get("partial_context_packs") or 0) > 0,
        "native_metrics_missing": int(metrics.get("samples") or 0) == 0,
    }


def make_adapter(backend: str, args: argparse.Namespace, storage_prefix: str):
    common = {
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "storage_prefix": storage_prefix,
        "request_timeout_ms": args.request_timeout_ms,
        "io_timeout_ms": args.io_timeout_ms,
    }
    if backend == "cpp":
        return MatrixArkTemporalStoreDirectAdapter(library_path=args.cpp_lib, **common)
    if backend == "rust":
        return MatrixArkTemporalStoreRustAdapter(rust_cli=args.rust_cli, **common)
    raise ValueError(f"unknown backend: {backend}")


def make_raw_client(backend: str, args: argparse.Namespace):
    if backend == "cpp":
        sdk_root = ROOT / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=args.metaserver,
            namespace_name=args.namespace,
            table_name=args.table,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        )
        return Client(options, library_path=args.cpp_lib or None)
    if backend == "rust":
        return MatrixArkRustCliClient(
            cli_path=args.rust_cli,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    raise ValueError(f"unknown backend: {backend}")


def call_with_latency(server: MatrixArkMcpServer, name: str, payload: Json) -> tuple[float, Json | None, str | None]:
    started = time.perf_counter()
    try:
        result = server.call_tool(name, payload)
        return (time.perf_counter() - started) * 1000.0, result, None
    except Exception as exc:  # Keep comparison artifact instead of aborting on first failure.
        return (time.perf_counter() - started) * 1000.0, None, f"{type(exc).__name__}: {exc}"


def raw_call_with_latency(fn) -> tuple[float, str | None]:
    started = time.perf_counter()
    try:
        fn()
        return (time.perf_counter() - started) * 1000.0, None
    except Exception as exc:
        return (time.perf_counter() - started) * 1000.0, f"{type(exc).__name__}: {exc}"


def raw_batch_hget(client: Any, entries: list[Json]) -> None:
    batch_hget = getattr(client, "batch_hget", None)
    if callable(batch_hget):
        batch_hget(entries)
        return
    for entry in entries:
        client.hget(str(entry.get("key") or ""), str(entry.get("field") or ""))


def run_raw_storage(backend: str, args: argparse.Namespace, run_id: str, *, client: Any | None = None) -> Json:
    owns_client = client is None
    if client is None:
        client = make_raw_client(backend, args)
    key = f"{args.storage_prefix}:{run_id}:{backend}:raw"
    batches: list[list[Json]] = []
    for start in range(0, args.raw_ops, args.raw_batch_size):
        batch = []
        for seq in range(start, min(args.raw_ops, start + args.raw_batch_size)):
            batch.append({"key": key, "field": f"{seq:08d}", "value": f"value-{backend}-{run_id}-{seq}"})
        batches.append(batch)

    write_latencies: list[float] = []
    write_errors: list[str] = []
    write_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.raw_workers) as pool:
        futures = [pool.submit(raw_call_with_latency, lambda b=batch: client.batch_hset(b)) for batch in batches]
        for future in as_completed(futures):
            latency, error = future.result()
            write_latencies.append(latency)
            if error:
                write_errors.append(error)
    write_elapsed = time.perf_counter() - write_started

    read_latencies: list[float] = []
    read_errors: list[str] = []
    read_count = min(args.raw_read_ops, args.raw_ops)
    read_batches: list[list[Json]] = []
    read_batch_size = max(1, args.raw_read_batch_size)
    for start in range(0, read_count, read_batch_size):
        read_batches.append(
            [
                {"key": key, "field": f"{seq:08d}"}
                for seq in range(start, min(read_count, start + read_batch_size))
            ]
        )
    read_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.raw_workers) as pool:
        futures = [pool.submit(raw_call_with_latency, lambda b=batch: raw_batch_hget(client, b)) for batch in read_batches]
        for future in as_completed(futures):
            latency, error = future.result()
            read_latencies.append(latency)
            if error:
                read_errors.append(error)
    read_elapsed = time.perf_counter() - read_started

    if owns_client:
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except TypeError:
                close(timeout_s=5.0)

    return {
        "write": {
            **summarize_latencies(write_latencies, total_ops=len(batches), elapsed_s=write_elapsed, errors=len(write_errors)),
            "records": args.raw_ops,
            "record_qps": round((args.raw_ops - (len(write_errors) * args.raw_batch_size)) / write_elapsed, 3) if write_elapsed > 0 else 0.0,
            "batch_size": args.raw_batch_size,
        },
        "read": {
            **summarize_latencies(read_latencies, total_ops=read_count, elapsed_s=read_elapsed, errors=len(read_errors)),
            "records": read_count,
            "batch_size": read_batch_size,
            "batches": len(read_batches),
        },
        "errors": {"write": write_errors[:10], "read": read_errors[:10]},
    }


def run_backend(backend: str, args: argparse.Namespace, run_id: str) -> Json:
    prefix = f"{args.storage_prefix}:{run_id}:{backend}"
    adapter = make_adapter(backend, args, prefix)
    server = MatrixArkMcpServer(adapter, access_mode="dev")
    # This runner measures ingestion/retrieval/storage latency. Admin/context
    # audit durability is covered by separate parity tests; keeping it enabled
    # here can make backend readiness itself become an audit write benchmark.
    server.access.append_audit = lambda *unused_args, **unused_kwargs: None  # type: ignore[method-assign]
    server.access.append_denied_audit = lambda *unused_args, **unused_kwargs: None  # type: ignore[method-assign]
    scope = {
        "account_id": "acct_scale",
        "tenant_id": "tenant_scale",
        "user_id": "user_scale",
        "session_id": f"scale-{run_id}-{backend}",
    }
    node_path = ["tenant:tenant_scale", "user:user_scale", f"session:scale-{run_id}-{backend}", "conversation:scale"]
    try:
        readiness = server.call_tool("matrixark_backend_ready", {"probe": True, "timeout_ms": args.readiness_timeout_ms})
        if readiness.get("status") != "ready":
            result = {
                "backend": backend,
                "status": "topology_not_ready",
                "storage_prefix": prefix,
                "readiness": readiness,
                "ingest": {**summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0), "timeout_count": 0},
                "retrieve": {
                    **summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0),
                    "timeout_count": 0,
                    "partial_context_packs": 0,
                    "selected_refs_avg": 0.0,
                    "selected_refs_max": 0,
                    "stage_metrics": summarize_retrieval_metrics([]),
                },
            }
            result["fallback_flags"] = fallback_flags_from_backend(result)
            return result
        raw_storage = run_raw_storage(backend, args, run_id, client=getattr(adapter, "_client", None))
        if args.skip_context_pipeline:
            result = {
                "backend": backend,
                "status": "passed" if not raw_storage.get("errors", {}).get("write") and not raw_storage.get("errors", {}).get("read") else "failed",
                "storage_prefix": prefix,
                "readiness": readiness,
                "raw_storage": raw_storage,
                "ingest": {**summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0), "timeout_count": 0},
                "ingest_messages": {"messages": 0, "messages_per_ingest": 0, "message_qps": 0.0},
                "retrieve": {
                    **summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0),
                    "timeout_count": 0,
                    "partial_context_packs": 0,
                    "selected_refs_avg": 0.0,
                    "selected_refs_max": 0,
                    "stage_metrics": summarize_retrieval_metrics([]),
                },
                "summary_refresh": {"skipped": True},
                "backend_metrics": {"skipped": True},
                "errors": raw_storage.get("errors", {}),
            }
            result["fallback_flags"] = fallback_flags_from_backend(result)
            return result

        ingest_payloads: list[Json] = []
        for batch_start in range(0, args.events, args.messages_per_ingest):
            messages = []
            for seq in range(batch_start, min(args.events, batch_start + args.messages_per_ingest)):
                messages.append(
                    {
                        "role": "user" if seq % 2 == 0 else "assistant",
                        "content": (
                            f"Scale event {seq}: Alice approved GPU budget item {seq % 17}; "
                            f"Bob owns procurement lane {seq % 9}; Project Aurora status is batch {seq // args.messages_per_ingest}."
                        ),
                    }
                )
            ingest_payloads.append(
                {
                    "kind": "message",
                    "messages": messages,
                    "scope": scope,
                    "metadata": {"node_path": node_path, "source": "scale_report"},
                    "auto_batch_extract": True,
                    "session_buffer_threshold": max(2, args.messages_per_ingest),
                    "threshold_messages": max(2, args.messages_per_ingest),
                    "wait": True,
                    "storage_options": args.storage_options,
                    "deadline_ms": args.ingest_deadline_ms,
                }
            )

        ingest_latencies: list[float] = []
        ingest_errors: list[str] = []
        ingest_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.ingest_workers) as pool:
            futures = [pool.submit(call_with_latency, server, "matrixark_ingest", payload) for payload in ingest_payloads]
            for future in as_completed(futures):
                latency, _result, error = future.result()
                ingest_latencies.append(latency)
                if error:
                    ingest_errors.append(error)
        ingest_elapsed = time.perf_counter() - ingest_started

        # Refresh summaries once so retrieval has the same post-ingest shape on both backends.
        refresh_latency_ms, refresh_result, refresh_error = call_with_latency(
            server,
            "matrixark_refresh_summaries",
            {"scope": scope, "limit": 128, "force": True, "storage_options": args.storage_options},
        )

        retrieve_payloads = []
        for seq in range(args.retrieve_queries):
            retrieve_payloads.append(
                {
                    "query": f"Who approved GPU budget item {seq % 17} and who owns procurement lane {seq % 9}?",
                    "scope": scope,
                    "max_context_tokens": args.max_context_tokens,
                    "deadline_ms": args.retrieve_deadline_ms,
                    "include_retrieval_metrics": True,
                    "storage_options": args.storage_options,
                    "ranking": {
                        "weights": {"time": 0.18, "business": 0.22},
                        "business_type_weights": {"approval": 0.95, "status_update": 0.76},
                    },
                }
            )

        retrieve_latencies: list[float] = []
        retrieve_errors: list[str] = []
        selected_counts: list[int] = []
        retrieval_metric_rows: list[Json] = []
        partial_count = 0
        retrieve_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.retrieve_workers) as pool:
            futures = [pool.submit(call_with_latency, server, "matrixark_retrieve", payload) for payload in retrieve_payloads]
            for future in as_completed(futures):
                latency, result, error = future.result()
                retrieve_latencies.append(latency)
                if error:
                    retrieve_errors.append(error)
                    continue
                assert result is not None
                selected_counts.append(selected_ref_count(result))
                metrics = retrieval_metrics_from_result(result)
                if metrics:
                    retrieval_metric_rows.append(metrics)
                if result.get("partial_context_pack") or "timeout_partial" in str(result.get("quality_warnings", "")):
                    partial_count += 1
        retrieve_elapsed = time.perf_counter() - retrieve_started

        metrics_latency_ms, metrics_result, metrics_error = call_with_latency(server, "matrixark_backend_metrics", {})
        result = {
            "backend": backend,
            "status": "passed" if not ingest_errors and not retrieve_errors else "failed",
            "storage_prefix": prefix,
            "readiness": readiness,
            "raw_storage": raw_storage,
            "ingest": {
                **summarize_latencies(
                    ingest_latencies,
                    total_ops=len(ingest_payloads),
                    elapsed_s=ingest_elapsed,
                    errors=len(ingest_errors),
                ),
                "timeout_count": timeout_count(ingest_errors),
            },
            "ingest_messages": {
                "messages": args.events,
                "messages_per_ingest": args.messages_per_ingest,
                "message_qps": round((args.events - (len(ingest_errors) * args.messages_per_ingest)) / ingest_elapsed, 3)
                if ingest_elapsed > 0
                else 0.0,
            },
            "retrieve": {
                **summarize_latencies(
                    retrieve_latencies,
                    total_ops=len(retrieve_payloads),
                    elapsed_s=retrieve_elapsed,
                    errors=len(retrieve_errors),
                ),
                "timeout_count": timeout_count(retrieve_errors),
                "partial_context_packs": partial_count,
                "selected_refs_avg": round(statistics.fmean(selected_counts), 3) if selected_counts else 0.0,
                "selected_refs_max": max(selected_counts) if selected_counts else 0,
                "stage_metrics": summarize_retrieval_metrics(retrieval_metric_rows),
            },
            "summary_refresh": {
                "latency_ms": round(refresh_latency_ms, 3),
                "error": refresh_error,
                "result": refresh_result,
            },
            "backend_metrics": {
                "latency_ms": round(metrics_latency_ms, 3),
                "error": metrics_error,
                "result": metrics_result,
            },
            "errors": {
                "ingest": ingest_errors[:10],
                "retrieve": retrieve_errors[:10],
            },
        }
        result["fallback_flags"] = fallback_flags_from_backend(result)
        return result
    finally:
        server.close()


def comparison(cpp: Json | None, rust: Json | None, args: argparse.Namespace | None = None) -> Json:
    if not cpp or not rust or cpp.get("status") != "passed" or rust.get("status") != "passed":
        return {"status": "not_comparable", "reason": "both backends must pass"}
    min_qps_ratio = float(getattr(args, "perf_min_qps_ratio", 0.8) if args is not None else 0.8)
    max_latency_ratio = float(getattr(args, "perf_max_latency_ratio", 2.0) if args is not None else 2.0)
    rows = []
    metrics = [
        ("raw_write_record_qps", ("raw_storage", "write", "record_qps"), "higher"),
        ("raw_write_p95_ms", ("raw_storage", "write", "p95_ms"), "lower"),
        ("raw_read_qps", ("raw_storage", "read", "qps"), "higher"),
        ("raw_read_p95_ms", ("raw_storage", "read", "p95_ms"), "lower"),
        ("message_qps", ("ingest_messages", "message_qps"), "higher"),
        ("ingest_p50_ms", ("ingest", "p50_ms"), "lower"),
        ("ingest_p95_ms", ("ingest", "p95_ms"), "lower"),
        ("ingest_p99_ms", ("ingest", "p99_ms"), "lower"),
        ("ingest_timeout_count", ("ingest", "timeout_count"), "lower"),
        ("retrieve_qps", ("retrieve", "qps"), "higher"),
        ("retrieve_p50_ms", ("retrieve", "p50_ms"), "lower"),
        ("retrieve_p95_ms", ("retrieve", "p95_ms"), "lower"),
        ("retrieve_p99_ms", ("retrieve", "p99_ms"), "lower"),
        ("retrieve_timeout_count", ("retrieve", "timeout_count"), "lower"),
        ("partial_context_packs", ("retrieve", "partial_context_packs"), "lower"),
        ("selected_refs_avg", ("retrieve", "selected_refs_avg"), "approx"),
        ("query_plan_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "query_plan_ms"), "lower"),
        ("node_traversal_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "node_traversal_ms"), "lower"),
        ("index_prefilter_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "index_prefilter_ms"), "lower"),
        ("candidate_fetch_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "candidate_fetch_ms"), "lower"),
        ("score_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "score_ms"), "lower"),
        ("pack_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "pack_ms"), "lower"),
        ("audit_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "audit_ms"), "lower"),
        ("scanned_records_avg", ("retrieve", "stage_metrics", "scanned_records_avg"), "lower"),
        ("cache_hit_rate", ("retrieve", "stage_metrics", "cache_hit_rate"), "higher"),
        ("placement_partitions_touched_avg", ("retrieve", "stage_metrics", "placement_partitions_touched_avg"), "approx"),
        ("memory_fallback", ("fallback_flags", "memory_fallback"), "lower"),
        ("hash_embedding_fallback", ("fallback_flags", "hash_embedding_fallback"), "lower"),
        ("partial_pack_fallback", ("fallback_flags", "partial_context_pack"), "lower"),
        ("native_metrics_missing", ("fallback_flags", "native_metrics_missing"), "lower"),
    ]
    for name, path, direction in metrics:
        cpp_value: Any = cpp
        rust_value: Any = rust
        for key in path:
            cpp_value = cpp_value.get(key, 0) if isinstance(cpp_value, dict) else 0
            rust_value = rust_value.get(key, 0) if isinstance(rust_value, dict) else 0
        if isinstance(cpp_value, bool):
            cpp_value = int(cpp_value)
        if isinstance(rust_value, bool):
            rust_value = int(rust_value)
        delta = float(rust_value or 0) - float(cpp_value or 0)
        percent_delta = (delta / float(cpp_value) * 100.0) if cpp_value else 0.0
        rust_float = float(rust_value or 0)
        cpp_float = float(cpp_value or 0)
        ratio = (rust_float / cpp_float) if cpp_float else (1.0 if rust_float == 0 else float("inf"))
        if direction == "higher":
            passed = cpp_float == 0 or rust_float >= cpp_float * min_qps_ratio
            threshold = round(cpp_float * min_qps_ratio, 6)
            threshold_label = f">= {threshold}"
        elif direction == "lower":
            passed = cpp_float == 0 or rust_float <= cpp_float * max_latency_ratio
            threshold = round(cpp_float * max_latency_ratio, 6)
            threshold_label = f"<= {threshold}"
        elif direction == "approx":
            allowed_delta = max(1.0, abs(cpp_float) * 0.25)
            passed = abs(delta) <= allowed_delta
            threshold_label = f"abs(delta) <= {round(allowed_delta, 6)}"
        else:
            passed = True
            threshold_label = "informational"
        rows.append(
            {
                "metric": name,
                "cpp": cpp_value,
                "rust": rust_value,
                "rust_minus_cpp": round(delta, 3),
                "percent_delta": round(percent_delta, 3),
                "rust_to_cpp_ratio": round(ratio, 6) if ratio != float("inf") else "inf",
                "direction": direction,
                "parity_threshold": threshold_label,
                "parity_passed": passed,
            }
        )
    blockers = [row for row in rows if not row.get("parity_passed")]
    return {
        "status": "passed" if not blockers else "failed",
        "rows": rows,
        "perf_parity": {
            "passed": not blockers,
            "min_qps_ratio": min_qps_ratio,
            "max_latency_ratio": max_latency_ratio,
            "blockers": blockers,
        },
    }


def write_report(path: Path, report: Json) -> None:
    lines = [
        "# MatrixArk C++ vs Rust Scale Report",
        "",
        f"- run_id: `{report['run_id']}`",
        f"- generated_at_ms: `{report['generated_at_ms']}`",
        f"- events: `{report['config']['events']}`",
        f"- messages_per_ingest: `{report['config']['messages_per_ingest']}`",
        f"- ingest_workers: `{report['config']['ingest_workers']}`",
        f"- retrieve_workers: `{report['config']['retrieve_workers']}`",
        f"- retrieve_queries: `{report['config']['retrieve_queries']}`",
        "",
        "## Results",
        "",
        "| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for backend in ("cpp", "rust"):
        item = report["backends"].get(backend, {})
        ingest = item.get("ingest", {})
        ingest_messages = item.get("ingest_messages", {})
        retrieve = item.get("retrieve", {})
        errors = int(ingest.get("errors") or 0) + int(retrieve.get("errors") or 0)
        lines.append(
            f"| {backend} | {item.get('status')} | {ingest_messages.get('message_qps', 0)} | "
            f"{ingest.get('p50_ms', 0)} ms | {ingest.get('p95_ms', 0)} ms | {ingest.get('p99_ms', 0)} ms | "
            f"{retrieve.get('qps', 0)} | {retrieve.get('p50_ms', 0)} ms | {retrieve.get('p95_ms', 0)} ms | "
            f"{retrieve.get('p99_ms', 0)} ms | {errors} | {retrieve.get('partial_context_packs', 0)} |"
        )
    lines.extend(["", "## Raw Storage", "", "| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |", "|---|---:|---:|---:|---:|---:|---:|"])
    for backend in ("cpp", "rust"):
        item = report["backends"].get(backend, {})
        raw = item.get("raw_storage", {})
        write = raw.get("write", {})
        read = raw.get("read", {})
        errors = raw.get("errors", {})
        lines.append(
            f"| {backend} | {write.get('record_qps', 0)} | {write.get('p95_ms', 0)} ms | "
            f"{read.get('qps', 0)} | {read.get('p95_ms', 0)} ms | "
            f"{len(errors.get('write', [])) if isinstance(errors, dict) else 0} | {len(errors.get('read', [])) if isinstance(errors, dict) else 0} |"
        )
    lines.extend(
        [
            "",
            "## Retrieval Stage Metrics",
            "",
            "| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | scanned records avg | cache hit rate | placement partitions avg |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for backend in ("cpp", "rust"):
        retrieve = report["backends"].get(backend, {}).get("retrieve", {})
        metrics = retrieve.get("stage_metrics", {})
        p95 = metrics.get("stage_p95_ms", {})
        lines.append(
            f"| {backend} | {metrics.get('samples', 0)} | "
            f"{p95.get('query_plan_ms', 0)} ms | {p95.get('node_traversal_ms', 0)} ms | "
            f"{p95.get('index_prefilter_ms', 0)} ms | {p95.get('candidate_fetch_ms', 0)} ms | "
            f"{p95.get('score_ms', 0)} ms | {p95.get('pack_ms', 0)} ms | {p95.get('audit_ms', 0)} ms | "
            f"{metrics.get('scanned_records_avg', 0)} | {metrics.get('cache_hit_rate', 0)} | "
            f"{metrics.get('placement_partitions_touched_avg', 0)} |"
        )
    comp = report.get("comparison", {})
    if comp.get("status") in {"passed", "failed"}:
        parity = comp.get("perf_parity", {})
        lines.extend(
            [
                "",
                "## Performance Parity Gate",
                "",
                f"- status: `{'passed' if parity.get('passed') else 'failed'}`",
                f"- minimum QPS ratio: `{parity.get('min_qps_ratio')}`",
                f"- maximum latency ratio: `{parity.get('max_latency_ratio')}`",
                f"- blockers: `{len(parity.get('blockers', []))}`",
            ]
        )
        if parity.get("blockers"):
            lines.extend(["", "| metric | C++ | Rust | threshold | ratio |", "|---|---:|---:|---:|---:|"])
            for row in parity.get("blockers", []):
                lines.append(
                    f"| {row['metric']} | {row['cpp']} | {row['rust']} | {row['parity_threshold']} | {row['rust_to_cpp_ratio']} |"
                )
        lines.extend(["", "## Rust Minus C++", "", "| metric | C++ | Rust | delta | percent delta |", "|---|---:|---:|---:|---:|"])
        for row in comp.get("rows", []):
            lines.append(
                f"| {row['metric']} | {row['cpp']} | {row['rust']} | {row['rust_minus_cpp']} | {row['percent_delta']}% |"
            )
    else:
        lines.extend(["", "## Comparison", "", f"`{comp.get('status')}`: {comp.get('reason', '')}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--raw-ops", type=int, default=1000)
    parser.add_argument("--raw-read-ops", type=int, default=500)
    parser.add_argument("--raw-batch-size", type=int, default=50)
    parser.add_argument("--raw-read-batch-size", type=int, default=25)
    parser.add_argument("--raw-workers", type=int, default=4)
    parser.add_argument("--messages-per-ingest", type=int, default=20)
    parser.add_argument("--ingest-workers", type=int, default=4)
    parser.add_argument("--retrieve-queries", type=int, default=128)
    parser.add_argument("--retrieve-workers", type=int, default=16)
    parser.add_argument("--max-context-tokens", type=int, default=12000)
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--storage-prefix", default="matrixark:scale")
    parser.add_argument("--cpp-lib", default=str(ROOT / "output-ubuntu22/release/sdk/lib/libbcache2.so"))
    parser.add_argument("--rust-cli", default=str(ROOT / "sdk/rust/temporalstore/target/release/matrixark_record_log"))
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--readiness-timeout-ms", type=int, default=60000)
    parser.add_argument("--ingest-deadline-ms", type=int, default=60000)
    parser.add_argument("--retrieve-deadline-ms", type=int, default=10000)
    parser.add_argument("--backends", nargs="+", choices=["cpp", "rust"], default=["cpp", "rust"])
    parser.add_argument("--artifact-dir", default="")
    parser.add_argument("--skip-context-pipeline", action="store_true")
    parser.add_argument("--perf-min-qps-ratio", type=float, default=0.8)
    parser.add_argument("--perf-max-latency-ratio", type=float, default=2.0)
    parser.add_argument("--require-perf-parity", action="store_true")
    parsed = parser.parse_args()

    parsed.storage_options = {
        "storage_family": "shared_store",
        "storage_mode": "multi_node",
        "write_mode": "async",
        "oplog_mode": "async",
        "replication_mode": "shared_store",
    }
    run_id = str(int(time.time() * 1000))
    artifact_dir = Path(parsed.artifact_dir) if parsed.artifact_dir else ROOT / "docs" / "benchmarks" / f"cpp_rust_scale_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    report: Json = {
        "run_id": run_id,
        "generated_at_ms": int(time.time() * 1000),
        "config": {
            "events": parsed.events,
            "raw_ops": parsed.raw_ops,
            "raw_read_ops": parsed.raw_read_ops,
            "raw_batch_size": parsed.raw_batch_size,
            "raw_read_batch_size": parsed.raw_read_batch_size,
            "raw_workers": parsed.raw_workers,
            "messages_per_ingest": parsed.messages_per_ingest,
            "ingest_workers": parsed.ingest_workers,
            "retrieve_queries": parsed.retrieve_queries,
            "retrieve_workers": parsed.retrieve_workers,
            "max_context_tokens": parsed.max_context_tokens,
            "metaserver": parsed.metaserver,
            "namespace": parsed.namespace,
            "table": parsed.table,
            "storage_options": parsed.storage_options,
            "skip_context_pipeline": parsed.skip_context_pipeline,
            "perf_min_qps_ratio": parsed.perf_min_qps_ratio,
            "perf_max_latency_ratio": parsed.perf_max_latency_ratio,
            "require_perf_parity": parsed.require_perf_parity,
        },
        "backends": {},
    }
    for backend in parsed.backends:
        try:
            report["backends"][backend] = run_backend(backend, parsed, run_id)
        except Exception as exc:
            report["backends"][backend] = {
                "backend": backend,
                "status": "backend_startup_failed",
                "error": str(exc),
                "config": {
                    "metaserver": parsed.metaserver,
                    "namespace": parsed.namespace,
                    "table": parsed.table,
                    "cpp_lib": parsed.cpp_lib if backend == "cpp" else "",
                    "rust_cli": parsed.rust_cli if backend == "rust" else "",
                },
                "retrieve": {"stage_metrics": summarize_retrieval_metrics([])},
            }
        (artifact_dir / f"{backend}.json").write_text(json.dumps(report["backends"][backend], indent=2, sort_keys=True), encoding="utf-8")
    report["comparison"] = comparison(report["backends"].get("cpp"), report["backends"].get("rust"), parsed)
    (artifact_dir / "comparison.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    write_report(artifact_dir / "comparison.md", report)
    print(json.dumps({"artifact_dir": str(artifact_dir), "comparison": report["comparison"]}, indent=2, sort_keys=True))
    backends_passed = all(report["backends"].get(b, {}).get("status") == "passed" for b in parsed.backends)
    parity_passed = bool(report.get("comparison", {}).get("perf_parity", {}).get("passed", True))
    if parsed.require_perf_parity and not parity_passed:
        return 2
    return 0 if backends_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
