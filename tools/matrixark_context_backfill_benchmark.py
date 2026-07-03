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
        confirm_incremental_repair=confirm_incremental_repair,
        active_prefix_key="matrixark:context:active_prefix",
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
        kv.put_string("matrixark:context:active_prefix", f"matrixark:context:active:{raw_backend}")

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
            ),
        )

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
        }


def summarize_backend_qps(results: list[Json]) -> Json:
    full = [float(item["full_shadow"]["qps"]) for item in results]
    repair = [float(item["incremental_repair"]["qps"]) for item in results]
    return {
        "full_shadow_qps_avg": round(statistics.fmean(full), 3) if full else 0.0,
        "incremental_repair_qps_avg": round(statistics.fmean(repair), 3) if repair else 0.0,
    }


def run_benchmark(args: argparse.Namespace) -> Json:
    if args.records <= 0:
        raise BackfillBenchmarkError("--records must be positive")
    if args.batch_size <= 0:
        raise BackfillBenchmarkError("--batch-size must be positive")
    if args.incremental_records <= 0:
        raise BackfillBenchmarkError("--incremental-records must be positive")
    raw_backends = ["temporalstore", "matrixkv"] if args.raw_backends == "both" else [args.raw_backends]
    started = time.perf_counter()
    results = [run_one_backend(args, raw_backend) for raw_backend in raw_backends]
    elapsed_s = max(0.000001, time.perf_counter() - started)
    summary = {
        "status": "ok",
        "mode": "local",
        "records": args.records,
        "batch_size": args.batch_size,
        "payload_bytes": args.payload_bytes,
        "incremental_records": min(args.incremental_records, args.records),
        "raw_backends": raw_backends,
        "elapsed_ms": round(elapsed_s * 1000.0, 3),
        "results": results,
        "qps_summary": summarize_backend_qps(results),
    }
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark local MatrixArk context backfill paths.")
    parser.add_argument("--records", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_RECORDS", "10000")))
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_BATCH_SIZE", "1024")))
    parser.add_argument("--payload-bytes", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_PAYLOAD_BYTES", "128")))
    parser.add_argument("--incremental-records", type=int, default=int(os.environ.get("MATRIXARK_BACKFILL_BENCH_INCREMENTAL_RECORDS", "1000")))
    parser.add_argument("--raw-backends", choices=["both", "temporalstore", "matrixkv"], default=os.environ.get("MATRIXARK_BACKFILL_BENCH_RAW_BACKENDS", "both"))
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
