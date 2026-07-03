#!/usr/bin/env python3
"""Benchmark MatrixArk context backfill batch and incremental repair paths."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import matrixark_context_backfill as backfill  # noqa: E402

Json = dict[str, Any]


class BackfillBenchmarkError(RuntimeError):
    pass


def make_raw_record(sequence: int, *, payload_bytes: int) -> Json:
    return {
        "record_type": "context_event",
        "event_id_hash": sequence + 1,
        "updated_at_ms": 1780000000000 + sequence,
        "scope": {
            "tenant_id": f"tenant-{sequence % 8}",
            "user_id": f"user-{sequence % 32}",
            "session_id": f"session-{sequence % 64}",
        },
        "text": f"backfill benchmark record {sequence} " + ("x" * max(0, payload_bytes)),
    }


def seed_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> None:
    kv.begin_bulk()
    try:
        for sequence in range(records):
            shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.hset(
                f"{prefix}:records:{shard:06d}",
                f"{offset:020d}",
                json.dumps(make_raw_record(sequence, payload_bytes=payload_bytes), sort_keys=True),
            )
        kv.put_string(f"{prefix}:record_count", str(records))
    finally:
        kv.end_bulk()


def seed_partial_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> int:
    expected = 0
    kv.begin_bulk()
    try:
        for sequence in range(records):
            record = make_raw_record(sequence, payload_bytes=payload_bytes)
            if sequence % 4 == 0:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": f"user-partial-{sequence % 2}",
                    "session_id": "session-hot",
                    "team": "search",
                }
                record["kind"] = "message"
                expected += 1
            elif sequence % 4 == 1:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": "user-other",
                    "session_id": "session-cold",
                    "team": "search",
                }
                record["kind"] = "message"
            elif sequence % 4 == 2:
                record["scope"] = {
                    "tenant_id": "tenant-other",
                    "user_id": "user-other",
                    "session_id": "session-hot",
                    "team": "search",
                }
                record["kind"] = "message"
            else:
                record["scope"] = {
                    "tenant_id": "tenant-partial",
                    "user_id": "user-other",
                    "session_id": "session-hot",
                    "team": "billing",
                }
                record["kind"] = "metric"
            shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.hset(
                f"{prefix}:records:{shard:06d}",
                f"{offset:020d}",
                json.dumps(record, sort_keys=True),
            )
        kv.put_string(f"{prefix}:record_count", str(records))
    finally:
        kv.end_bulk()
    return expected


def make_backfill_args(
    *,
    kv_path: Path,
    source_prefix: str,
    target_prefix: str,
    raw_backend: str,
    job_id: str,
    batch_size: int,
    start_seq: int = 0,
    end_seq: int | None = None,
    mode: str = "shadow",
    confirm_incremental_repair: str = "",
    expect_active_prefix: str = "",
) -> argparse.Namespace:
    return argparse.Namespace(
        metaserver="unused",
        namespace="unused",
        table="unused",
        library_path="",
        source_prefix=source_prefix,
        raw_backend=raw_backend,
        target_prefix=target_prefix,
        mode=mode,
        confirm_in_place="",
        confirm_activate="",
        confirm_rollback="",
        confirm_rollback_noop="",
        confirm_incremental_repair=confirm_incremental_repair,
        confirm_no_active_prefix_precondition="",
        confirm_skip_validation="",
        confirm_non_strict_validation="",
        active_prefix_key="matrixark:context:active_prefix",
        expect_active_prefix=expect_active_prefix,
        repair_active_prefix="",
        validation_strict=True,
        skip_validation=False,
        job_id=job_id,
        start_seq=start_seq,
        end_seq=end_seq,
        partial=False,
        partial_record_types="",
        partial_tenant_ids="",
        partial_user_ids="",
        partial_session_ids="",
        partial_filter_json="",
        partial_require_bounded=True,
        batch_size=batch_size,
        source_scan_max_empty_shards=2,
        dry_run=False,
        resume=False,
        fail_fast=False,
        prometheus_output="",
        local_kv=str(kv_path),
    )


def timed_call(fn, *args, **kwargs) -> tuple[Json, float]:
    started = time.perf_counter()
    summary = fn(*args, **kwargs)
    elapsed_s = max(0.000001, time.perf_counter() - started)
    return summary, elapsed_s


def run_one_backend(args: argparse.Namespace, raw_backend: str) -> Json:
    with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_bench_{raw_backend}_") as tmp:
        kv_path = Path(tmp) / "kv.json"
        source_prefix = "matrixark:mcp:raw_ingestion"
        kv = backfill.LocalJsonKV(kv_path)
        seed_started = time.perf_counter()
        seed_raw_log(kv, prefix=source_prefix, records=args.records, payload_bytes=args.payload_bytes)
        seed_elapsed_s = max(0.000001, time.perf_counter() - seed_started)
        active_prefix = f"matrixark:context:active:{raw_backend}"
        kv.put_string("matrixark:context:active_prefix", active_prefix)

        full_summary, full_elapsed_s = timed_call(
            backfill.run_backfill,
            make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:bench:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"bench-{raw_backend}-full",
                batch_size=args.batch_size,
            ),
        )

        incremental_records = min(args.incremental_records, args.records)
        incremental_start = max(0, args.records - incremental_records)
        incremental_end = args.records
        repair_prefix = f"matrixark:context_repair:bench:{raw_backend}"
        repair_shadow_args = make_backfill_args(
            kv_path=kv_path,
            source_prefix=source_prefix,
            target_prefix=repair_prefix,
            raw_backend=raw_backend,
            job_id=f"bench-{raw_backend}-repair",
            batch_size=args.batch_size,
            start_seq=incremental_start,
            end_seq=incremental_end,
        )
        repair_shadow_summary, repair_shadow_elapsed_s = timed_call(backfill.run_backfill, repair_shadow_args)
        repair_summary, repair_elapsed_s = timed_call(
            backfill.run_incremental_repair,
            make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=f"bench-{raw_backend}-repair",
                batch_size=args.batch_size,
                start_seq=incremental_start,
                end_seq=incremental_end,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
                expect_active_prefix=active_prefix,
            ),
        )

        partial_source_prefix = "matrixark:mcp:raw_ingestion:partial"
        partial_expected = seed_partial_raw_log(
            kv,
            prefix=partial_source_prefix,
            records=args.records,
            payload_bytes=args.payload_bytes,
        )
        partial_prefix = f"matrixark:context_repair:bench:{raw_backend}:partial"
        partial_values = {
            "partial": True,
            "partial_record_types": "context_event",
            "partial_tenant_ids": "tenant-partial",
            "partial_session_ids": "session-hot",
            "partial_filter_json": json.dumps({"kind": "message", "scope": {"team": "search"}}, sort_keys=True),
        }
        partial_active_prefix = f"matrixark:context:active:{raw_backend}:partial"
        kv.put_string("matrixark:context:active_prefix", partial_active_prefix)
        partial_shadow_args = make_backfill_args(
            kv_path=kv_path,
            source_prefix=partial_source_prefix,
            target_prefix=partial_prefix,
            raw_backend=raw_backend,
            job_id=f"bench-{raw_backend}-partial",
            batch_size=args.batch_size,
            end_seq=args.records,
        )
        for key, value in partial_values.items():
            setattr(partial_shadow_args, key, value)
        partial_shadow_summary, partial_shadow_elapsed_s = timed_call(backfill.run_backfill, partial_shadow_args)
        partial_repair_args = make_backfill_args(
            kv_path=kv_path,
            source_prefix=partial_source_prefix,
            target_prefix=partial_prefix,
            raw_backend=raw_backend,
            job_id=f"bench-{raw_backend}-partial",
            batch_size=args.batch_size,
            end_seq=args.records,
            mode="incremental_repair",
            confirm_incremental_repair="YES",
            expect_active_prefix=partial_active_prefix,
        )
        for key, value in partial_values.items():
            setattr(partial_repair_args, key, value)
        partial_repair_summary, partial_repair_elapsed_s = timed_call(backfill.run_incremental_repair, partial_repair_args)

        return {
            "raw_backend": raw_backend,
            "records": args.records,
            "batch_size": args.batch_size,
            "payload_bytes": args.payload_bytes,
            "seed": {
                "elapsed_ms": round(seed_elapsed_s * 1000.0, 3),
                "qps": round(args.records / seed_elapsed_s, 3),
            },
            "full_shadow": {
                "elapsed_ms": round(full_elapsed_s * 1000.0, 3),
                "qps": round(full_summary["metrics"]["written"] / full_elapsed_s, 3),
                "summary": full_summary,
            },
            "incremental_shadow": {
                "records": incremental_records,
                "elapsed_ms": round(repair_shadow_elapsed_s * 1000.0, 3),
                "qps": round(repair_shadow_summary["metrics"]["written"] / repair_shadow_elapsed_s, 3),
                "summary": repair_shadow_summary,
            },
            "incremental_repair": {
                "records": incremental_records,
                "elapsed_ms": round(repair_elapsed_s * 1000.0, 3),
                "qps": round(repair_summary["promotion"]["metrics"]["written"] / repair_elapsed_s, 3),
                "summary": repair_summary,
            },
            "partial_shadow": {
                "records": partial_expected,
                "elapsed_ms": round(partial_shadow_elapsed_s * 1000.0, 3),
                "qps": round(partial_shadow_summary["metrics"]["written"] / partial_shadow_elapsed_s, 3),
                "summary": partial_shadow_summary,
            },
            "partial_repair": {
                "records": partial_expected,
                "elapsed_ms": round(partial_repair_elapsed_s * 1000.0, 3),
                "qps": round(partial_repair_summary["promotion"]["metrics"]["written"] / partial_repair_elapsed_s, 3),
                "summary": partial_repair_summary,
            },
        }


def summarize_backend_qps(results: list[Json]) -> Json:
    full = [float(item["full_shadow"]["qps"]) for item in results]
    incremental_shadow = [float(item["incremental_shadow"]["qps"]) for item in results]
    repair = [float(item["incremental_repair"]["qps"]) for item in results]
    partial_shadow = [float(item["partial_shadow"]["qps"]) for item in results]
    partial_repair = [float(item["partial_repair"]["qps"]) for item in results]
    def qps_stats(values: list[float]) -> Json:
        if not values:
            return {"avg": 0.0, "min": 0.0, "max": 0.0, "min_max_ratio": 1.0}
        minimum = min(values)
        maximum = max(values)
        return {
            "avg": round(statistics.fmean(values), 3),
            "min": round(minimum, 3),
            "max": round(maximum, 3),
            "min_max_ratio": round((minimum / maximum) if maximum > 0.0 else 0.0, 6),
        }
    full_stats = qps_stats(full)
    incremental_shadow_stats = qps_stats(incremental_shadow)
    repair_stats = qps_stats(repair)
    partial_shadow_stats = qps_stats(partial_shadow)
    partial_repair_stats = qps_stats(partial_repair)
    return {
        "full_shadow_qps_avg": full_stats["avg"],
        "incremental_shadow_qps_avg": incremental_shadow_stats["avg"],
        "incremental_repair_qps_avg": repair_stats["avg"],
        "partial_shadow_qps_avg": partial_shadow_stats["avg"],
        "partial_repair_qps_avg": partial_repair_stats["avg"],
        "full_shadow_qps": full_stats,
        "incremental_shadow_qps": incremental_shadow_stats,
        "incremental_repair_qps": repair_stats,
        "partial_shadow_qps": partial_shadow_stats,
        "partial_repair_qps": partial_repair_stats,
    }


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((percentile / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def summarize_phase_latency_ms(results: list[Json]) -> Json:
    def phase_stats(phase: str) -> Json:
        values = [float(item[phase]["elapsed_ms"]) for item in results]
        if not values:
            return {"avg": 0.0, "min": 0.0, "max": 0.0, "p95": 0.0}
        return {
            "avg": round(statistics.fmean(values), 3),
            "min": round(min(values), 3),
            "max": round(max(values), 3),
            "p95": round(_percentile(values, 95.0), 3),
        }
    return {
        "full_shadow_ms": phase_stats("full_shadow"),
        "incremental_shadow_ms": phase_stats("incremental_shadow"),
        "incremental_repair_ms": phase_stats("incremental_repair"),
        "partial_shadow_ms": phase_stats("partial_shadow"),
        "partial_repair_ms": phase_stats("partial_repair"),
    }


def _qps_values_by_backend(results: list[Json], phase: str) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = {}
    for result in results:
        grouped.setdefault(str(result["raw_backend"]), []).append(float(result[phase]["qps"]))
    return grouped


def _aggregate_qps(values: list[float], aggregation: str) -> float:
    if not values:
        return 0.0
    if aggregation == "avg":
        return statistics.fmean(values)
    return min(values)


def _latency_values_by_backend(results: list[Json], phase: str) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = {}
    for result in results:
        grouped.setdefault(str(result["raw_backend"]), []).append(float(result[phase]["elapsed_ms"]))
    return grouped


def _aggregate_latency_ms(values: list[float], aggregation: str) -> float:
    if not values:
        return 0.0
    if aggregation == "avg":
        return statistics.fmean(values)
    return _percentile(values, 95.0)


def parse_batch_sizes(value: str, default_batch_size: int) -> list[int]:
    raw = str(value or "").strip()
    if not raw:
        return [default_batch_size]
    batch_sizes: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            batch_size = int(item)
        except ValueError as exc:
            raise BackfillBenchmarkError("--batch-sizes must be a comma-separated list of positive integers") from exc
        if batch_size <= 0:
            raise BackfillBenchmarkError("--batch-sizes values must be positive")
        if batch_size not in batch_sizes:
            batch_sizes.append(batch_size)
    if not batch_sizes:
        raise BackfillBenchmarkError("--batch-sizes must include at least one positive integer")
    return batch_sizes


def summarize_batch_size_performance(results: list[Json]) -> Json:
    grouped: dict[int, list[Json]] = {}
    for result in results:
        grouped.setdefault(int(result["batch_size"]), []).append(result)
    by_batch_size: Json = {}
    recommendations: Json = {}
    for batch_size in sorted(grouped):
        batch_results = grouped[batch_size]
        qps_summary = summarize_backend_qps(batch_results)
        latency_summary = summarize_phase_latency_ms(batch_results)
        full_qps = float(qps_summary["full_shadow_qps"]["min"])
        repair_qps = float(qps_summary["incremental_repair_qps"]["min"])
        partial_repair_qps = float(qps_summary["partial_repair_qps"]["min"])
        balanced_qps = min(full_qps, repair_qps, partial_repair_qps)
        by_batch_size[str(batch_size)] = {
            "samples": len(batch_results),
            "raw_backends": sorted({str(item["raw_backend"]) for item in batch_results}),
            "qps_summary": qps_summary,
            "latency_ms_summary": latency_summary,
            "balanced_min_qps": round(balanced_qps, 3),
        }
        for name, value in [
            ("best_full_shadow_qps", full_qps),
            ("best_incremental_repair_qps", repair_qps),
            ("best_partial_repair_qps", partial_repair_qps),
            ("best_balanced_min_qps", balanced_qps),
        ]:
            current = recommendations.get(name)
            if not isinstance(current, dict) or value > float(current.get("observed", -1.0)):
                recommendations[name] = {
                    "batch_size": batch_size,
                    "observed": round(value, 3),
                }
    return {
        "by_batch_size": by_batch_size,
        "recommendations": recommendations,
    }


def evaluate_performance_gate(args: argparse.Namespace, results: list[Json]) -> Json:
    min_full = float(getattr(args, "min_full_shadow_qps", 0.0) or 0.0)
    min_repair = float(getattr(args, "min_incremental_repair_qps", 0.0) or 0.0)
    min_partial_repair = float(getattr(args, "min_partial_repair_qps", 0.0) or 0.0)
    min_backend_ratio = float(getattr(args, "min_backend_qps_ratio", 0.0) or 0.0)
    max_full_latency = float(getattr(args, "max_full_shadow_p95_ms", 0.0) or 0.0)
    max_incremental_shadow_latency = float(getattr(args, "max_incremental_shadow_p95_ms", 0.0) or 0.0)
    max_repair_latency = float(getattr(args, "max_incremental_repair_p95_ms", 0.0) or 0.0)
    max_partial_shadow_latency = float(getattr(args, "max_partial_shadow_p95_ms", 0.0) or 0.0)
    max_partial_repair_latency = float(getattr(args, "max_partial_repair_p95_ms", 0.0) or 0.0)
    gate_aggregation = str(getattr(args, "gate_aggregation", "min") or "min")
    checks: list[Json] = []
    if gate_aggregation == "sample":
        for result in results:
            backend = result["raw_backend"]
            repeat_index = int(result.get("repeat_index", 1))
            full_qps = float(result["full_shadow"]["qps"])
            repair_qps = float(result["incremental_repair"]["qps"])
            partial_repair_qps = float(result["partial_repair"]["qps"])
            full_latency = float(result["full_shadow"]["elapsed_ms"])
            incremental_shadow_latency = float(result["incremental_shadow"]["elapsed_ms"])
            repair_latency = float(result["incremental_repair"]["elapsed_ms"])
            partial_shadow_latency = float(result["partial_shadow"]["elapsed_ms"])
            partial_repair_latency = float(result["partial_repair"]["elapsed_ms"])
            checks.append({
                "raw_backend": backend,
                "metric": "full_shadow_qps",
                "aggregation": "sample",
                "repeat_index": repeat_index,
                "observed": round(full_qps, 3),
                "minimum": min_full,
                "passed": full_qps >= min_full,
            })
            checks.append({
                "raw_backend": backend,
                "metric": "incremental_repair_qps",
                "aggregation": "sample",
                "repeat_index": repeat_index,
                "observed": round(repair_qps, 3),
                "minimum": min_repair,
                "passed": repair_qps >= min_repair,
            })
            checks.append({
                "raw_backend": backend,
                "metric": "partial_repair_qps",
                "aggregation": "sample",
                "repeat_index": repeat_index,
                "observed": round(partial_repair_qps, 3),
                "minimum": min_partial_repair,
                "passed": partial_repair_qps >= min_partial_repair,
            })
            for metric, observed, maximum in [
                ("full_shadow_p95_ms", full_latency, max_full_latency),
                ("incremental_shadow_p95_ms", incremental_shadow_latency, max_incremental_shadow_latency),
                ("incremental_repair_p95_ms", repair_latency, max_repair_latency),
                ("partial_shadow_p95_ms", partial_shadow_latency, max_partial_shadow_latency),
                ("partial_repair_p95_ms", partial_repair_latency, max_partial_repair_latency),
            ]:
                if maximum > 0.0:
                    checks.append({
                        "raw_backend": backend,
                        "metric": metric,
                        "aggregation": "sample",
                        "repeat_index": repeat_index,
                        "observed": round(observed, 3),
                        "maximum": maximum,
                        "passed": observed <= maximum,
                    })
    else:
        for backend, values in _qps_values_by_backend(results, "full_shadow").items():
            observed = _aggregate_qps(values, gate_aggregation)
            checks.append({
                "raw_backend": backend,
                "metric": "full_shadow_qps",
                "aggregation": gate_aggregation,
                "samples": len(values),
                "observed": round(observed, 3),
                "minimum": min_full,
                "passed": observed >= min_full,
            })
        for backend, values in _qps_values_by_backend(results, "incremental_repair").items():
            observed = _aggregate_qps(values, gate_aggregation)
            checks.append({
                "raw_backend": backend,
                "metric": "incremental_repair_qps",
                "aggregation": gate_aggregation,
                "samples": len(values),
                "observed": round(observed, 3),
                "minimum": min_repair,
                "passed": observed >= min_repair,
            })
        for backend, values in _qps_values_by_backend(results, "partial_repair").items():
            observed = _aggregate_qps(values, gate_aggregation)
            checks.append({
                "raw_backend": backend,
                "metric": "partial_repair_qps",
                "aggregation": gate_aggregation,
                "samples": len(values),
                "observed": round(observed, 3),
                "minimum": min_partial_repair,
                "passed": observed >= min_partial_repair,
            })
        for metric, phase, maximum in [
            ("full_shadow_p95_ms", "full_shadow", max_full_latency),
            ("incremental_shadow_p95_ms", "incremental_shadow", max_incremental_shadow_latency),
            ("incremental_repair_p95_ms", "incremental_repair", max_repair_latency),
            ("partial_shadow_p95_ms", "partial_shadow", max_partial_shadow_latency),
            ("partial_repair_p95_ms", "partial_repair", max_partial_repair_latency),
        ]:
            if maximum <= 0.0:
                continue
            for backend, values in _latency_values_by_backend(results, phase).items():
                observed = _aggregate_latency_ms(values, gate_aggregation)
                checks.append({
                    "raw_backend": backend,
                    "metric": metric,
                    "aggregation": "avg" if gate_aggregation == "avg" else "p95",
                    "samples": len(values),
                    "observed": round(observed, 3),
                    "maximum": maximum,
                    "passed": observed <= maximum,
                })
    if min_backend_ratio > 0.0 and len(results) > 1:
        for metric, path in [
            ("full_shadow_qps_ratio", ("full_shadow", "qps")),
            ("incremental_shadow_qps_ratio", ("incremental_shadow", "qps")),
            ("incremental_repair_qps_ratio", ("incremental_repair", "qps")),
            ("partial_shadow_qps_ratio", ("partial_shadow", "qps")),
            ("partial_repair_qps_ratio", ("partial_repair", "qps")),
        ]:
            if gate_aggregation == "sample":
                values = [float(result[path[0]][path[1]]) for result in results]
                samples = len(values)
            else:
                values = [
                    _aggregate_qps(backend_values, gate_aggregation)
                    for backend_values in _qps_values_by_backend(results, path[0]).values()
                ]
                samples = len(results)
            minimum = min(values)
            maximum = max(values)
            observed = (minimum / maximum) if maximum > 0.0 else 0.0
            checks.append({
                "raw_backend": "all",
                "metric": metric,
                "aggregation": gate_aggregation,
                "samples": samples,
                "observed": round(observed, 6),
                "minimum": min_backend_ratio,
                "passed": observed >= min_backend_ratio,
            })
    enabled = (
        min_full > 0.0
        or min_repair > 0.0
        or min_partial_repair > 0.0
        or min_backend_ratio > 0.0
        or max_full_latency > 0.0
        or max_incremental_shadow_latency > 0.0
        or max_repair_latency > 0.0
        or max_partial_shadow_latency > 0.0
        or max_partial_repair_latency > 0.0
    )
    passed = all(bool(check["passed"]) for check in checks) if checks else True
    return {
        "enabled": enabled,
        "passed": passed,
        "min_full_shadow_qps": min_full,
        "min_incremental_repair_qps": min_repair,
        "min_partial_repair_qps": min_partial_repair,
        "min_backend_qps_ratio": min_backend_ratio,
        "max_full_shadow_p95_ms": max_full_latency,
        "max_incremental_shadow_p95_ms": max_incremental_shadow_latency,
        "max_incremental_repair_p95_ms": max_repair_latency,
        "max_partial_shadow_p95_ms": max_partial_shadow_latency,
        "max_partial_repair_p95_ms": max_partial_repair_latency,
        "gate_aggregation": gate_aggregation,
        "checks": checks,
    }


def _result_key(result: Json) -> tuple[str, int]:
    return str(result.get("raw_backend") or ""), int(result.get("batch_size") or 0)


def evaluate_baseline_gate(args: argparse.Namespace, results: list[Json]) -> Json:
    baseline_path = str(getattr(args, "baseline_json", "") or "").strip()
    min_qps_ratio = float(getattr(args, "min_baseline_qps_ratio", 0.0) or 0.0)
    max_latency_ratio = float(getattr(args, "max_baseline_latency_ratio", 0.0) or 0.0)
    enabled = bool(baseline_path) and (min_qps_ratio > 0.0 or max_latency_ratio > 0.0)
    checks: list[Json] = []
    if not baseline_path:
        return {
            "enabled": False,
            "passed": True,
            "baseline_json": "",
            "min_baseline_qps_ratio": min_qps_ratio,
            "max_baseline_latency_ratio": max_latency_ratio,
            "checks": checks,
        }
    path = Path(baseline_path)
    if not path.exists():
        raise BackfillBenchmarkError(f"--baseline-json does not exist: {baseline_path}")
    try:
        baseline = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BackfillBenchmarkError(f"--baseline-json is not valid JSON: {baseline_path}") from exc
    baseline_results = baseline.get("results")
    if not isinstance(baseline_results, list):
        raise BackfillBenchmarkError("--baseline-json must contain a results array")
    baseline_by_key = {_result_key(result): result for result in baseline_results if isinstance(result, dict)}
    current_by_key = {_result_key(result): result for result in results}
    for key, current in sorted(current_by_key.items()):
        baseline_result = baseline_by_key.get(key)
        if not isinstance(baseline_result, dict):
            checks.append({
                "raw_backend": key[0],
                "batch_size": key[1],
                "metric": "baseline_result_present",
                "passed": False,
                "detail": "baseline missing matching raw_backend and batch_size",
            })
            continue
        for phase in ["full_shadow", "incremental_shadow", "incremental_repair", "partial_shadow", "partial_repair"]:
            current_phase = current.get(phase) if isinstance(current.get(phase), dict) else {}
            baseline_phase = baseline_result.get(phase) if isinstance(baseline_result.get(phase), dict) else {}
            current_qps = float(current_phase.get("qps", 0.0) or 0.0)
            baseline_qps = float(baseline_phase.get("qps", 0.0) or 0.0)
            current_latency = float(current_phase.get("elapsed_ms", 0.0) or 0.0)
            baseline_latency = float(baseline_phase.get("elapsed_ms", 0.0) or 0.0)
            if min_qps_ratio > 0.0:
                observed = (current_qps / baseline_qps) if baseline_qps > 0.0 else 0.0
                checks.append({
                    "raw_backend": key[0],
                    "batch_size": key[1],
                    "phase": phase,
                    "metric": "baseline_qps_ratio",
                    "observed": round(observed, 6),
                    "minimum": min_qps_ratio,
                    "current": round(current_qps, 3),
                    "baseline": round(baseline_qps, 3),
                    "passed": observed >= min_qps_ratio,
                })
            if max_latency_ratio > 0.0:
                observed = (current_latency / baseline_latency) if baseline_latency > 0.0 else 0.0
                checks.append({
                    "raw_backend": key[0],
                    "batch_size": key[1],
                    "phase": phase,
                    "metric": "baseline_latency_ratio",
                    "observed": round(observed, 6),
                    "maximum": max_latency_ratio,
                    "current": round(current_latency, 3),
                    "baseline": round(baseline_latency, 3),
                    "passed": observed <= max_latency_ratio,
                })
    passed = all(bool(check["passed"]) for check in checks) if checks else True
    return {
        "enabled": enabled,
        "passed": passed,
        "baseline_json": baseline_path,
        "min_baseline_qps_ratio": min_qps_ratio,
        "max_baseline_latency_ratio": max_latency_ratio,
        "checks": checks,
    }


def run_benchmark(args: argparse.Namespace) -> Json:
    if args.records <= 0:
        raise BackfillBenchmarkError("--records must be positive")
    if args.batch_size <= 0:
        raise BackfillBenchmarkError("--batch-size must be positive")
    if args.repeat <= 0:
        raise BackfillBenchmarkError("--repeat must be positive")
    if args.incremental_records <= 0:
        raise BackfillBenchmarkError("--incremental-records must be positive")
    if float(getattr(args, "min_full_shadow_qps", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--min-full-shadow-qps must be non-negative")
    if float(getattr(args, "min_incremental_repair_qps", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--min-incremental-repair-qps must be non-negative")
    if float(getattr(args, "min_partial_repair_qps", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--min-partial-repair-qps must be non-negative")
    if float(getattr(args, "max_full_shadow_p95_ms", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--max-full-shadow-p95-ms must be non-negative")
    if float(getattr(args, "max_incremental_shadow_p95_ms", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--max-incremental-shadow-p95-ms must be non-negative")
    if float(getattr(args, "max_incremental_repair_p95_ms", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--max-incremental-repair-p95-ms must be non-negative")
    if float(getattr(args, "max_partial_shadow_p95_ms", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--max-partial-shadow-p95-ms must be non-negative")
    if float(getattr(args, "max_partial_repair_p95_ms", 0.0) or 0.0) < 0.0:
        raise BackfillBenchmarkError("--max-partial-repair-p95-ms must be non-negative")
    min_backend_ratio = float(getattr(args, "min_backend_qps_ratio", 0.0) or 0.0)
    if min_backend_ratio < 0.0 or min_backend_ratio > 1.0:
        raise BackfillBenchmarkError("--min-backend-qps-ratio must be between 0 and 1")
    min_baseline_qps_ratio = float(getattr(args, "min_baseline_qps_ratio", 0.0) or 0.0)
    if min_baseline_qps_ratio < 0.0:
        raise BackfillBenchmarkError("--min-baseline-qps-ratio must be non-negative")
    max_baseline_latency_ratio = float(getattr(args, "max_baseline_latency_ratio", 0.0) or 0.0)
    if max_baseline_latency_ratio < 0.0:
        raise BackfillBenchmarkError("--max-baseline-latency-ratio must be non-negative")
    args.gate_aggregation = str(getattr(args, "gate_aggregation", "min") or "min")
    if args.gate_aggregation not in {"sample", "min", "avg"}:
        raise BackfillBenchmarkError("--gate-aggregation must be sample, min, or avg")
    batch_sizes = parse_batch_sizes(getattr(args, "batch_sizes", ""), args.batch_size)
    raw_backends = ["temporalstore", "matrixkv"] if args.raw_backends == "both" else [args.raw_backends]
    started = time.perf_counter()
    results: list[Json] = []
    original_batch_size = args.batch_size
    try:
        for batch_size in batch_sizes:
            args.batch_size = batch_size
            for repeat_index in range(1, args.repeat + 1):
                for raw_backend in raw_backends:
                    result = run_one_backend(args, raw_backend)
                    result["repeat_index"] = repeat_index
                    results.append(result)
    finally:
        args.batch_size = original_batch_size
    elapsed_s = max(0.000001, time.perf_counter() - started)
    performance_gate = evaluate_performance_gate(args, results)
    baseline_gate = evaluate_baseline_gate(args, results)
    status_ok = performance_gate["passed"] and baseline_gate["passed"]
    summary = {
        "status": "ok" if status_ok else "failed",
        "mode": "local",
        "records": args.records,
        "batch_size": batch_sizes[0],
        "batch_sizes": batch_sizes,
        "payload_bytes": args.payload_bytes,
        "incremental_records": min(args.incremental_records, args.records),
        "repeat": args.repeat,
        "raw_backends": raw_backends,
        "elapsed_ms": round(elapsed_s * 1000.0, 3),
        "results": results,
        "qps_summary": summarize_backend_qps(results),
        "latency_ms_summary": summarize_phase_latency_ms(results),
        "batch_size_summary": summarize_batch_size_performance(results),
        "performance_gate": performance_gate,
        "baseline_gate": baseline_gate,
    }
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark local MatrixArk context backfill paths.")
    parser.add_argument("--records", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_RECORDS", "10000")))
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_BATCH_SIZE", "1024")))
    parser.add_argument("--batch-sizes", default=os.environ.get("MATRIXARK_BACKFILL_BENCH_BATCH_SIZES", ""), help="optional comma-separated batch-size sweep; overrides single --batch-size for benchmark execution")
    parser.add_argument("--payload-bytes", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_PAYLOAD_BYTES", "128")))
    parser.add_argument("--incremental-records", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_INCREMENTAL_RECORDS", "1000")))
    parser.add_argument("--repeat", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_REPEAT", "1")), help="number of samples to run for each selected raw backend")
    parser.add_argument("--raw-backends", choices=["both", "temporalstore", "matrixkv"], default=os.environ.get("MATRIXARK_BACKFILL_BENCH_RAW_BACKENDS", "both"))
    parser.add_argument("--min-full-shadow-qps", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MIN_FULL_SHADOW_QPS", "0")))
    parser.add_argument("--min-incremental-repair-qps", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MIN_INCREMENTAL_REPAIR_QPS", "0")))
    parser.add_argument("--min-partial-repair-qps", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MIN_PARTIAL_REPAIR_QPS", "0")))
    parser.add_argument("--min-backend-qps-ratio", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MIN_BACKEND_QPS_RATIO", "0")), help="optional parity floor: slowest selected backend QPS divided by fastest selected backend QPS, 0 disables")
    parser.add_argument("--max-full-shadow-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_FULL_SHADOW_P95_MS", "0")), help="optional p95 latency ceiling for full shadow backfill, 0 disables")
    parser.add_argument("--max-incremental-shadow-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_INCREMENTAL_SHADOW_P95_MS", "0")), help="optional p95 latency ceiling for incremental shadow build, 0 disables")
    parser.add_argument("--max-incremental-repair-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_INCREMENTAL_REPAIR_P95_MS", "0")), help="optional p95 latency ceiling for incremental active repair, 0 disables")
    parser.add_argument("--max-partial-shadow-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_PARTIAL_SHADOW_P95_MS", "0")), help="optional p95 latency ceiling for bounded partial shadow backfill, 0 disables")
    parser.add_argument("--max-partial-repair-p95-ms", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_PARTIAL_REPAIR_P95_MS", "0")), help="optional p95 latency ceiling for bounded partial active repair, 0 disables")
    parser.add_argument("--gate-aggregation", choices=["sample", "min", "avg"], default=os.environ.get("MATRIXARK_BACKFILL_BENCH_GATE_AGGREGATION", "min"), help="how performance gates aggregate repeated samples: min is the conservative default, avg smooths noisy local runs, sample checks every sample")
    parser.add_argument("--baseline-json", default=os.environ.get("MATRIXARK_BACKFILL_BENCH_BASELINE_JSON", ""), help="optional prior benchmark JSON used for regression gating")
    parser.add_argument("--min-baseline-qps-ratio", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MIN_BASELINE_QPS_RATIO", "0")), help="fail when current QPS for a matching backend/batch/phase is below this fraction of baseline, 0 disables")
    parser.add_argument("--max-baseline-latency-ratio", type=float, default=float(os.environ.get("MATRIXARK_BACKFILL_BENCH_MAX_BASELINE_LATENCY_RATIO", "0")), help="fail when current elapsed latency for a matching backend/batch/phase exceeds this multiple of baseline, 0 disables")
    parser.add_argument("--json-output", default=os.environ.get("MATRIXARK_BACKFILL_BENCH_JSON", ""))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        summary = run_benchmark(args)
    except BackfillBenchmarkError as exc:
        parser.error(str(exc))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("status") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
