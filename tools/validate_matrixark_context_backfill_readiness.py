#!/usr/bin/env python3
"""Validate MatrixArk context backfill production-readiness surface."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import matrixark_context_backfill as backfill  # noqa: E402
import matrixark_context_backfill_benchmark as bench  # noqa: E402

Json = dict[str, Any]


REQUIRED_DOC_MARKERS = [
    "--raw-backend=temporalstore",
    "--raw-backend=matrixkv",
    "--batch-sizes",
    "record_count",
    "record_index",
    "scan_hash",
    "--partial-session-ids",
    "promotion_partial_matches_validation",
    "incremental_repair",
    "serving_record_fingerprint_match",
    "promotion_readiness",
    "matrixark_context_backfill_promotion_readiness_status",
    "--confirm-skip-validation",
    "--confirm-non-strict-validation",
    "--confirm-resume-range-change",
    "--confirm-active-target",
    "--expect-active-prefix",
    "--dry-run-check-target",
    "matrixark_context_backfill_validation_check",
    "matrixark_context_backfill_incremental_repair_status",
    "matrixark_context_backfill_data_quality_status",
    "completed_with_errors",
    "--baseline-json",
    "append-time idempotency",
]


def check(name: str, passed: bool, detail: str = "") -> Json:
    return {"name": name, "passed": bool(passed), "detail": detail}


def parser_support_checks() -> list[Json]:
    backfill_parser = backfill.build_parser()
    benchmark_parser = bench.build_parser()
    backfill_options = {option for action in backfill_parser._actions for option in action.option_strings}
    benchmark_options = {option for action in benchmark_parser._actions for option in action.option_strings}
    mode_action = next(action for action in backfill_parser._actions if "--mode" in action.option_strings)
    raw_backend_action = next(action for action in backfill_parser._actions if "--raw-backend" in action.option_strings)
    benchmark_raw_action = next(action for action in benchmark_parser._actions if "--raw-backends" in action.option_strings)
    return [
        check("backfill_modes_cover_batch_and_incremental", {"shadow", "validate_shadow", "activate_shadow", "rollback_activation", "incremental_repair", "in_place"}.issubset(set(mode_action.choices or []))),
        check("backfill_raw_backend_choices_cover_all_raw_options", {"temporalstore", "matrixkv"}.issubset(set(raw_backend_action.choices or []))),
        check("benchmark_raw_backend_choices_cover_all_raw_options", {"both", "temporalstore", "matrixkv"}.issubset(set(benchmark_raw_action.choices or []))),
        check("benchmark_has_batch_sweep_option", "--batch-sizes" in benchmark_options),
        check("benchmark_has_latency_gate_options", {"--max-full-shadow-p95-ms", "--max-incremental-shadow-p95-ms", "--max-incremental-repair-p95-ms", "--max-partial-shadow-p95-ms", "--max-partial-repair-p95-ms"}.issubset(benchmark_options)),
        check("benchmark_has_partial_repair_qps_gate", "--min-partial-repair-qps" in benchmark_options),
        check("benchmark_has_backend_parity_gate", "--min-backend-qps-ratio" in benchmark_options),
        check("benchmark_has_baseline_regression_gate", {"--baseline-json", "--min-baseline-qps-ratio", "--max-baseline-latency-ratio"}.issubset(benchmark_options)),
        check("backfill_has_prometheus_output", "--prometheus-output" in backfill_options),
        check("backfill_has_dry_run_target_check_option", "--dry-run-check-target" in backfill_options),
        check("backfill_has_active_target_confirmation", "--confirm-active-target" in backfill_options),
        check("backfill_has_expect_active_prefix_precondition", "--expect-active-prefix" in backfill_options),
        check("backfill_has_skip_validation_confirmation", "--confirm-skip-validation" in backfill_options),
        check("backfill_has_non_strict_validation_confirmation", "--confirm-non-strict-validation" in backfill_options),
        check("backfill_has_resume_range_change_confirmation", "--confirm-resume-range-change" in backfill_options),
    ]


def docs_checks() -> list[Json]:
    doc_path = ROOT / "docs" / "matrixark_context_backfill.md"
    text = doc_path.read_text(encoding="utf-8") if doc_path.exists() else ""
    checks = [check("manual_exists", doc_path.exists(), str(doc_path))]
    checks.extend(check(f"manual_mentions_{marker}", marker in text) for marker in REQUIRED_DOC_MARKERS)
    return checks


def append_accounting_checks() -> list[Json]:
    source_path = ROOT / "tools" / "matrixark_context_backfill.py"
    text = source_path.read_text(encoding="utf-8") if source_path.exists() else ""
    return [
        check("append_many_reports_attempted_written_duplicate", all(token in text for token in ["'attempted'", "'written'", "'duplicate'", "'appended_records'"])),
        check("run_backfill_uses_append_stats_for_written_metrics", all(token in text for token in ["append_stats = target.append_many(pending)", "metrics.written += append_written", "metrics.duplicate += append_duplicate", "metrics.observe_records(appended_records)"])),
        check("run_backfill_reports_data_quality_status", all(token in text for token in ["data_quality_status", "completed_with_errors", "matrixark_context_backfill_data_quality_status"])),
    ]


def run_local_gate(args: argparse.Namespace) -> Json:
    benchmark_args = argparse.Namespace(
        records=args.records,
        batch_size=args.batch_size,
        batch_sizes=args.batch_sizes,
        payload_bytes=args.payload_bytes,
        incremental_records=args.incremental_records,
        repeat=args.repeat,
        raw_backends="both",
        min_full_shadow_qps=1.0,
        min_incremental_repair_qps=1.0,
        min_partial_repair_qps=1.0,
        min_backend_qps_ratio=0.000001,
        max_full_shadow_p95_ms=1000.0,
        max_incremental_shadow_p95_ms=1000.0,
        max_incremental_repair_p95_ms=1000.0,
        max_partial_shadow_p95_ms=1000.0,
        max_partial_repair_p95_ms=1000.0,
        gate_aggregation="min",
        baseline_json="",
        min_baseline_qps_ratio=0.0,
        max_baseline_latency_ratio=0.0,
        json_output="",
    )
    summary = bench.run_benchmark(benchmark_args)
    return {
        "status": summary.get("status"),
        "performance_gate": summary.get("performance_gate", {}),
        "baseline_gate": summary.get("baseline_gate", {}),
        "batch_sizes": summary.get("batch_sizes", []),
        "raw_backends": summary.get("raw_backends", []),
        "batch_size_summary": summary.get("batch_size_summary", {}),
        "qps_summary": summary.get("qps_summary", {}),
        "latency_ms_summary": summary.get("latency_ms_summary", {}),
    }


def run_baseline_gate(args: argparse.Namespace) -> Json:
    with tempfile.TemporaryDirectory(prefix="matrixark_backfill_baseline_gate_") as tmp:
        tmp_path = Path(tmp)
        baseline_path = tmp_path / "baseline.json"
        baseline_args = argparse.Namespace(
            records=max(2, int(args.records)),
            batch_size=args.batch_size,
            batch_sizes=args.batch_sizes,
            payload_bytes=args.payload_bytes,
            incremental_records=args.incremental_records,
            repeat=1,
            raw_backends="both",
            min_full_shadow_qps=1.0,
            min_incremental_repair_qps=1.0,
            min_partial_repair_qps=1.0,
            min_backend_qps_ratio=0.000001,
            max_full_shadow_p95_ms=1000.0,
            max_incremental_shadow_p95_ms=1000.0,
            max_incremental_repair_p95_ms=1000.0,
            max_partial_shadow_p95_ms=1000.0,
            max_partial_repair_p95_ms=1000.0,
            gate_aggregation="min",
            baseline_json="",
            min_baseline_qps_ratio=0.0,
            max_baseline_latency_ratio=0.0,
            json_output=str(baseline_path),
        )
        baseline = bench.run_benchmark(baseline_args)
        candidate_args = argparse.Namespace(**{
            **vars(baseline_args),
            "baseline_json": str(baseline_path),
            "min_baseline_qps_ratio": 0.000001,
            "max_baseline_latency_ratio": 1000000.0,
            "json_output": "",
        })
        candidate = bench.run_benchmark(candidate_args)
        gate = candidate.get("baseline_gate") if isinstance(candidate.get("baseline_gate"), dict) else {}
        checks = gate.get("checks") if isinstance(gate.get("checks"), list) else []
        return {
            "status": "ok" if baseline.get("status") == "ok" and candidate.get("status") == "ok" and gate.get("passed") else "failed",
            "baseline_status": baseline.get("status"),
            "candidate_status": candidate.get("status"),
            "baseline_json_written": baseline_path.exists(),
            "baseline_gate": gate,
            "raw_backends": candidate.get("raw_backends", []),
            "batch_sizes": candidate.get("batch_sizes", []),
            "check_count": len(checks),
        }


def run_cutover_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_cutover"
    records = max(2, int(args.records))
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_cutover_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            active_key = "matrixark:context:active_prefix"
            old_prefix = f"matrixark:context:active:old:{raw_backend}"
            target_prefix = f"matrixark:context_backfill:readiness_cutover:{raw_backend}"
            job_id = f"readiness-cutover-{raw_backend}"
            kv.put_string(active_key, old_prefix)

            shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=records,
            )
            shadow = backfill.run_backfill(shadow_args)
            activate_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="activate_shadow",
            )
            activate_args.confirm_activate = "YES"
            activated = backfill.run_activate_shadow(activate_args)
            kv_after_activate = backfill.LocalJsonKV(kv_path)
            activation_audit = kv_after_activate.hget(f"{active_key}:audit", job_id)

            rollback_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=f"{job_id}:rollback",
                batch_size=args.batch_size,
                mode="rollback_activation",
            )
            rollback_args.confirm_rollback = "YES"
            rollback_args.rollback_job_id = job_id
            rollback = backfill.run_rollback_activation(rollback_args)
            kv_after_rollback = backfill.LocalJsonKV(kv_path)
            rollback_audit = kv_after_rollback.hget(f"{active_key}:rollback_audit", f"{job_id}:rollback")

            results.append({
                "raw_backend": raw_backend,
                "shadow_status": shadow.get("status"),
                "shadow_written": int(shadow.get("metrics", {}).get("written", 0) or 0),
                "activation_status": activated.get("status"),
                "activation_validation_status": activated.get("validation_status"),
                "activation_validation_skipped": bool(activated.get("validation_skipped")),
                "activation_new_prefix": activated.get("new_prefix"),
                "active_after_activation": kv_after_activate.get_string(active_key),
                "activation_audit_written": bool(activation_audit),
                "rollback_status": rollback.get("status"),
                "rollback_to_prefix": rollback.get("to_prefix"),
                "active_after_rollback": kv_after_rollback.get_string(active_key),
                "rollback_audit_written": bool(rollback_audit),
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["shadow_written"] == records
        and item["activation_status"] == "ok"
        and item["activation_validation_status"] == "ok"
        and not item["activation_validation_skipped"]
        and item["active_after_activation"] == item["activation_new_prefix"]
        and item["activation_audit_written"]
        and item["rollback_status"] == "ok"
        and item["rollback_to_prefix"] == item["active_after_rollback"]
        and item["rollback_audit_written"]
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def seed_legacy_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> None:
    record_ids = []
    kv.begin_bulk()
    try:
        for sequence in range(records):
            record_id = f"legacy-{sequence:020d}"
            record_ids.append(record_id)
            kv.hset(
                f"{prefix}:records",
                record_id,
                json.dumps(bench.make_raw_record(sequence, payload_bytes=payload_bytes), sort_keys=True),
            )
        kv.put_string(f"{prefix}:record_index", json.dumps(record_ids, separators=(",", ":")))
    finally:
        kv.end_bulk()


def seed_scan_hash_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> None:
    kv.begin_bulk()
    try:
        for sequence in range(records):
            shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.hset(
                f"{prefix}:records:{shard:06d}",
                f"{offset:020d}",
                json.dumps(bench.make_raw_record(sequence, payload_bytes=payload_bytes), sort_keys=True),
            )
    finally:
        kv.end_bulk()


def run_source_scan_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    records = max(4, int(args.records))
    scenarios = [
        ("record_count", bench.seed_raw_log, None),
        ("record_index", seed_legacy_raw_log, None),
        ("scan_hash", seed_scan_hash_raw_log, records),
    ]
    for raw_backend in ["temporalstore", "matrixkv"]:
        for expected_scan_mode, seed_fn, end_seq in scenarios:
            with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_scan_{raw_backend}_{expected_scan_mode}_") as tmp:
                kv_path = Path(tmp) / "kv.json"
                kv = backfill.LocalJsonKV(kv_path)
                source_prefix = f"matrixark:mcp:readiness_scan:{expected_scan_mode}"
                seed_fn(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
                target_prefix = f"matrixark:context_backfill:readiness_scan:{raw_backend}:{expected_scan_mode}"
                job_id = f"readiness-scan-{raw_backend}-{expected_scan_mode}"
                run_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=job_id,
                    batch_size=args.batch_size,
                    end_seq=end_seq,
                )
                summary = backfill.run_backfill(run_args)
                validation = backfill.run_validate_shadow(bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=job_id,
                    batch_size=args.batch_size,
                    mode="validate_shadow",
                    end_seq=end_seq,
                ))
                source_range = summary.get("source_range") if isinstance(summary.get("source_range"), dict) else {}
                metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
                results.append({
                    "raw_backend": raw_backend,
                    "expected_scan_mode": expected_scan_mode,
                    "scan_mode": source_range.get("scan_mode"),
                    "status": summary.get("status"),
                    "records": records,
                    "scanned": int(metrics.get("scanned", 0) or 0),
                    "written": int(metrics.get("written", 0) or 0),
                    "failed": int(metrics.get("failed", 0) or 0),
                    "validation_status": validation.get("status"),
                    "validation_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
                    "source_record_count_estimated": bool(source_range.get("source_record_count_estimated")),
                    "source_high_watermark_seq": source_range.get("source_high_watermark_seq"),
                })
    status = "ok" if all(
        item["status"] == "ok"
        and item["validation_status"] == "ok"
        and item["validation_fingerprint_match"] is True
        and item["scan_mode"] == item["expected_scan_mode"]
        and item["scanned"] == item["records"]
        and item["written"] == item["records"]
        and item["failed"] == 0
        and item["source_high_watermark_seq"] == item["records"] - 1
        and (item["source_record_count_estimated"] is (item["expected_scan_mode"] == "scan_hash"))
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def seed_partial_raw_log(kv: backfill.LocalJsonKV, *, prefix: str, records: int, payload_bytes: int) -> int:
    expected = 0
    kv.begin_bulk()
    try:
        for sequence in range(records):
            record = bench.make_raw_record(sequence, payload_bytes=payload_bytes)
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


def run_partial_repair_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    records = max(8, int(args.records))
    partial_values = {
        "partial": True,
        "partial_record_types": "context_event",
        "partial_tenant_ids": "tenant-partial",
        "partial_session_ids": "session-hot",
        "partial_filter_json": json.dumps({"kind": "message", "scope": {"team": "search"}}, sort_keys=True),
    }
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_partial_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            source_prefix = "matrixark:mcp:readiness_partial"
            expected_matches = seed_partial_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            active_key = "matrixark:context:active_prefix"
            active_prefix = f"matrixark:context:active:partial:{raw_backend}"
            repair_prefix = f"matrixark:context_repair:readiness_partial:{raw_backend}"
            job_id = f"readiness-partial-{raw_backend}"
            kv.put_string(active_key, active_prefix)
            shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            for key, value in partial_values.items():
                setattr(shadow_args, key, value)
            shadow = backfill.run_backfill(shadow_args)
            validation_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
                end_seq=records,
            )
            for key, value in partial_values.items():
                setattr(validation_args, key, value)
            validation = backfill.run_validate_shadow(validation_args)
            repair_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=records,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
            )
            for key, value in partial_values.items():
                setattr(repair_args, key, value)
            repair = backfill.run_incremental_repair(repair_args)
            retried = backfill.run_incremental_repair(repair_args)
            kv_after = backfill.LocalJsonKV(kv_path)
            audit = kv_after.hget(f"{active_key}:incremental_repair_audit", job_id)
            consistency = repair.get("promotion_consistency") if isinstance(repair.get("promotion_consistency"), dict) else {}
            shadow_metrics = shadow.get("metrics") if isinstance(shadow.get("metrics"), dict) else {}
            promotion = repair.get("promotion") if isinstance(repair.get("promotion"), dict) else {}
            retry_promotion = retried.get("promotion") if isinstance(retried.get("promotion"), dict) else {}
            promotion_metrics = promotion.get("metrics") if isinstance(promotion.get("metrics"), dict) else {}
            retry_metrics = retry_promotion.get("metrics") if isinstance(retry_promotion.get("metrics"), dict) else {}
            target_count = int(kv_after.get_string(f"{active_prefix}:record_count") or 0)
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "expected_matches": expected_matches,
                "shadow_status": shadow.get("status"),
                "shadow_scanned": int(shadow_metrics.get("scanned", 0) or 0),
                "shadow_filtered": int(shadow_metrics.get("filtered", 0) or 0),
                "shadow_written": int(shadow_metrics.get("written", 0) or 0),
                "validation_status": validation.get("status"),
                "validation_expected_records": int(validation.get("expected_records", 0) or 0),
                "validation_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
                "repair_status": repair.get("status"),
                "repair_written": int(promotion_metrics.get("written", 0) or 0),
                "retry_duplicate": int(retry_metrics.get("duplicate", 0) or 0),
                "target_count": target_count,
                "partial_enabled": bool(shadow.get("partial", {}).get("enabled")),
                "promotion_partial_matches_validation": bool(consistency.get("checks", {}).get("promotion_partial_matches_validation")),
                "promotion_source_range_matches_validation": bool(consistency.get("checks", {}).get("promotion_source_range_matches_validation")),
                "promotion_covered_expected_records": bool(consistency.get("checks", {}).get("promotion_covered_expected_records")),
                "audit_written": bool(audit),
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["validation_status"] == "ok"
        and item["repair_status"] == "ok"
        and item["partial_enabled"]
        and item["shadow_scanned"] == item["records"]
        and item["shadow_written"] == item["expected_matches"]
        and item["shadow_filtered"] == item["records"] - item["expected_matches"]
        and item["validation_expected_records"] == item["expected_matches"]
        and item["validation_fingerprint_match"] is True
        and item["repair_written"] == item["expected_matches"]
        and item["retry_duplicate"] == item["expected_matches"]
        and item["target_count"] == item["expected_matches"]
        and item["promotion_partial_matches_validation"]
        and item["promotion_source_range_matches_validation"]
        and item["promotion_covered_expected_records"]
        and item["audit_written"]
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_dead_letter_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_dead_letter"
    records = max(3, int(args.records))
    missing_sequence = 1
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_dead_letter_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            shard = missing_sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            offset = missing_sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
            kv.data["hashes"].get(f"{source_prefix}:records:{shard:06d}", {}).pop(f"{offset:020d}", None)
            kv._flush()

            target_prefix = f"matrixark:context_backfill:readiness_dead_letter:{raw_backend}"
            job_id = f"readiness-dead-letter-{raw_backend}"
            run_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            summary = backfill.run_backfill(run_args)
            validation = backfill.run_validate_shadow(bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
            ))
            kv_after = backfill.LocalJsonKV(kv_path)
            dead_letter_count = kv_after.get_string(f"{target_prefix}:dead_letter_count")
            dead_letter_preview = kv_after.hget(f"{target_prefix}:dead_letter", "00000000000000000000")
            partial = backfill.build_partial_spec(run_args)
            checkpoint_state = backfill.read_checkpoint_state(
                kv_after,
                backfill.checkpoint_key(
                    job_id=job_id,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    partial=partial,
                ),
            )
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "missing_sequence": missing_sequence,
                "status": summary.get("status"),
                "data_quality_status": summary.get("data_quality_status"),
                "failed": int(summary.get("metrics", {}).get("failed", 0) or 0),
                "dead_letter": int(summary.get("metrics", {}).get("dead_letter", 0) or 0),
                "written": int(summary.get("metrics", {}).get("written", 0) or 0),
                "dead_letter_count": int(dead_letter_count or 0),
                "dead_letter_preview_has_error": "missing sharded record" in dead_letter_preview,
                "checkpoint_last_sequence": checkpoint_state.get("checkpoint_last_sequence"),
                "validation_status": validation.get("status"),
                "validation_no_shadow_dead_letters": validation.get("checks", {}).get("no_shadow_dead_letters"),
                "validation_source_scan_had_no_failures": validation.get("checks", {}).get("source_scan_had_no_failures"),
            })
    status = "ok" if all(
        item["status"] == "ok"
        and item["data_quality_status"] == "completed_with_errors"
        and item["failed"] == 1
        and item["dead_letter"] == 1
        and item["dead_letter_count"] == 1
        and item["dead_letter_preview_has_error"]
        and item["written"] == item["records"] - 1
        and item["checkpoint_last_sequence"] == item["records"] - 1
        and item["validation_status"] == "failed"
        and item["validation_no_shadow_dead_letters"] is False
        and item["validation_source_scan_had_no_failures"] is False
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def run_resume_gate(args: argparse.Namespace) -> Json:
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_resume"
    records = max(2, int(args.records))
    first_end_seq = max(1, min(records - 1, records // 2))
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_resume_{raw_backend}_") as tmp:
            kv_path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            target_prefix = f"matrixark:context_backfill:readiness_resume:{raw_backend}"
            job_id = f"readiness-resume-{raw_backend}"
            first_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                end_seq=first_end_seq,
            )
            first_args.resume = True
            first = backfill.run_backfill(first_args)
            second_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
            )
            second_args.resume = True
            second = backfill.run_backfill(second_args)
            incompatible_blocked = False
            incompatible_error = ""
            incompatible_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                start_seq=1,
                end_seq=records,
            )
            incompatible_args.resume = True
            try:
                backfill.run_backfill(incompatible_args)
            except backfill.BackfillError as exc:
                incompatible_blocked = "confirm-resume-range-change=YES" in str(exc)
                incompatible_error = str(exc)
            confirmed_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                start_seq=1,
                end_seq=records,
            )
            confirmed_args.resume = True
            confirmed_args.confirm_resume_range_change = "YES"
            confirmed = backfill.run_backfill(confirmed_args)
            validation = backfill.run_validate_shadow(bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=job_id,
                batch_size=args.batch_size,
                mode="validate_shadow",
            ))
            results.append({
                "raw_backend": raw_backend,
                "records": records,
                "first_end_seq": first_end_seq,
                "first_written": int(first["metrics"]["written"]),
                "second_written": int(second["metrics"]["written"]),
                "second_resume_state": second.get("resume_state", {}),
                "incompatible_resume_blocked": incompatible_blocked,
                "incompatible_resume_error": incompatible_error,
                "confirmed_resume_state": confirmed.get("resume_state", {}),
                "confirmed_scanned": int(confirmed.get("metrics", {}).get("scanned", 0) or 0),
                "confirmed_duplicate": int(confirmed.get("metrics", {}).get("duplicate", 0) or 0),
                "confirmed_written": int(confirmed.get("metrics", {}).get("written", 0) or 0),
                "confirmed_failed": int(confirmed.get("metrics", {}).get("failed", 0) or 0),
                "confirmed_dead_letter": int(confirmed.get("metrics", {}).get("dead_letter", 0) or 0),
                "validation_status": validation.get("status"),
                "actual_records": validation.get("actual_records"),
                "expected_records": validation.get("expected_records"),
                "serving_record_fingerprint_match": validation.get("checks", {}).get("serving_record_fingerprint_match"),
            })
    return {"status": "ok" if all(item["validation_status"] == "ok" for item in results) else "failed", "results": results}


def run_prometheus_gate(args: argparse.Namespace) -> Json:
    required_shadow_metrics = [
        "matrixark_context_backfill_run_elapsed_ms",
        "matrixark_context_backfill_scan_qps",
        "matrixark_context_backfill_records_total",
        "matrixark_context_backfill_serving_records_total",
        "matrixark_context_backfill_serving_record_fingerprint_info",
        "matrixark_context_backfill_source_range",
        "matrixark_context_backfill_source_scan_mode",
    ]
    required_repair_metrics = [
        "matrixark_context_backfill_incremental_repair_status",
        "matrixark_context_backfill_incremental_repair_promotion_consistency_status",
        "matrixark_context_backfill_incremental_repair_promotion_consistency_check",
        "matrixark_context_backfill_incremental_repair_promotion_records",
        "matrixark_context_backfill_incremental_repair_promotion_source_range",
        "matrixark_context_backfill_incremental_repair_validation_status",
    ]
    required_validation_metrics = [
        "matrixark_context_backfill_validation_status",
        "matrixark_context_backfill_validation_check",
        "matrixark_context_backfill_promotion_readiness_status",
        "matrixark_context_backfill_validation_source_range",
        "matrixark_context_backfill_validation_source_scan_mode",
    ]
    results: list[Json] = []
    source_prefix = "matrixark:mcp:readiness_prometheus"
    records = max(2, int(args.records))
    incremental_records = max(1, min(int(args.incremental_records), records))
    incremental_start = records - incremental_records
    for raw_backend in ["temporalstore", "matrixkv"]:
        with tempfile.TemporaryDirectory(prefix=f"matrixark_backfill_prometheus_{raw_backend}_") as tmp:
            tmp_path = Path(tmp)
            kv_path = tmp_path / "kv.json"
            kv = backfill.LocalJsonKV(kv_path)
            bench.seed_raw_log(kv, prefix=source_prefix, records=records, payload_bytes=args.payload_bytes)
            kv.put_string("matrixark:context:active_prefix", f"matrixark:context:active:prometheus:{raw_backend}")

            shadow_prometheus = tmp_path / "shadow.prom"
            shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-full",
                batch_size=args.batch_size,
            )
            shadow_args.prometheus_output = str(shadow_prometheus)
            shadow_summary = backfill.run_backfill(shadow_args)
            shadow_text = shadow_prometheus.read_text(encoding="utf-8") if shadow_prometheus.exists() else ""
            validation_prometheus = tmp_path / "validation.prom"
            validation_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=f"matrixark:context_backfill:readiness_prometheus:{raw_backend}:full",
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-full",
                batch_size=args.batch_size,
                mode="validate_shadow",
            )
            validation_args.prometheus_output = str(validation_prometheus)
            validation_summary = backfill.run_validate_shadow(validation_args)
            validation_text = validation_prometheus.read_text(encoding="utf-8") if validation_prometheus.exists() else ""

            repair_prefix = f"matrixark:context_repair:readiness_prometheus:{raw_backend}"
            repair_shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-repair-shadow",
                batch_size=args.batch_size,
                start_seq=incremental_start,
                end_seq=records,
            )
            backfill.run_backfill(repair_shadow_args)
            repair_prometheus = tmp_path / "repair.prom"
            repair_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=repair_prefix,
                raw_backend=raw_backend,
                job_id=f"readiness-prometheus-{raw_backend}-repair",
                batch_size=args.batch_size,
                start_seq=incremental_start,
                end_seq=records,
                mode="incremental_repair",
                confirm_incremental_repair="YES",
            )
            repair_args.prometheus_output = str(repair_prometheus)
            repair_summary = backfill.run_incremental_repair(repair_args)
            repair_text = repair_prometheus.read_text(encoding="utf-8") if repair_prometheus.exists() else ""

            results.append({
                "raw_backend": raw_backend,
                "shadow_status": shadow_summary.get("status"),
                "validation_status": validation_summary.get("status"),
                "repair_status": repair_summary.get("status"),
                "shadow_prometheus_output": str(shadow_prometheus),
                "validation_prometheus_output": str(validation_prometheus),
                "repair_prometheus_output": str(repair_prometheus),
                "shadow_metric_count": sum(1 for line in shadow_text.splitlines() if line and not line.startswith("#")),
                "validation_metric_count": sum(1 for line in validation_text.splitlines() if line and not line.startswith("#")),
                "repair_metric_count": sum(1 for line in repair_text.splitlines() if line and not line.startswith("#")),
                "shadow_metrics_present": {metric: metric in shadow_text for metric in required_shadow_metrics},
                "validation_metrics_present": {metric: metric in validation_text for metric in required_validation_metrics},
                "repair_metrics_present": {metric: metric in repair_text for metric in required_repair_metrics},
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["validation_status"] == "ok"
        and item["repair_status"] == "ok"
        and all(item["shadow_metrics_present"].values())
        and all(item["validation_metrics_present"].values())
        and all(item["repair_metrics_present"].values())
        for item in results
    ) else "failed"
    return {"status": status, "results": results}


def benchmark_checks(summary: Json) -> list[Json]:
    performance_gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    batch_size_summary = summary.get("batch_size_summary") if isinstance(summary.get("batch_size_summary"), dict) else {}
    recommendations = batch_size_summary.get("recommendations") if isinstance(batch_size_summary.get("recommendations"), dict) else {}
    return [
        check("local_benchmark_status_ok", summary.get("status") == "ok"),
        check("local_benchmark_gate_passed", bool(performance_gate.get("passed"))),
        check("local_benchmark_baseline_gate_available", isinstance(summary.get("baseline_gate"), dict)),
        check("local_benchmark_covers_temporalstore_and_matrixkv", set(summary.get("raw_backends") or []) == {"temporalstore", "matrixkv"}),
        check("local_benchmark_exercised_batch_sweep", len(summary.get("batch_sizes") or []) >= 2),
        check("local_benchmark_reports_partial_repair_recommendation", isinstance(recommendations.get("best_partial_repair_qps"), dict)),
        check("local_benchmark_reports_balanced_recommendation", isinstance(recommendations.get("best_balanced_min_qps"), dict)),
    ]


def baseline_gate_checks(summary: Json) -> list[Json]:
    gate = summary.get("baseline_gate") if isinstance(summary.get("baseline_gate"), dict) else {}
    checks = gate.get("checks") if isinstance(gate.get("checks"), list) else []
    return [
        check("baseline_gate_status_ok", summary.get("status") == "ok"),
        check("baseline_gate_baseline_artifact_written", bool(summary.get("baseline_json_written"))),
        check("baseline_gate_candidate_enabled", bool(gate.get("enabled"))),
        check("baseline_gate_candidate_passed", bool(gate.get("passed"))),
        check("baseline_gate_covers_temporalstore_and_matrixkv", set(summary.get("raw_backends") or []) == {"temporalstore", "matrixkv"}),
        check("baseline_gate_compared_qps_and_latency", {item.get("metric") for item in checks} == {"baseline_qps_ratio", "baseline_latency_ratio"}),
        check("baseline_gate_exercised_batch_sweep", len(summary.get("batch_sizes") or []) >= 2),
    ]


def cutover_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("cutover_gate_status_ok", summary.get("status") == "ok"),
        check("cutover_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("cutover_gate_shadow_wrote_records", all(int(item.get("shadow_written", 0) or 0) > 0 for item in results)),
        check("cutover_gate_activation_validated_shadow", all(item.get("activation_validation_status") == "ok" and not item.get("activation_validation_skipped") for item in results)),
        check("cutover_gate_activation_updates_active_pointer", all(item.get("active_after_activation") == item.get("activation_new_prefix") for item in results)),
        check("cutover_gate_activation_audit_written", all(bool(item.get("activation_audit_written")) for item in results)),
        check("cutover_gate_rollback_restores_previous_pointer", all(item.get("rollback_to_prefix") == item.get("active_after_rollback") for item in results)),
        check("cutover_gate_rollback_audit_written", all(bool(item.get("rollback_audit_written")) for item in results)),
    ]


def dead_letter_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("dead_letter_gate_status_ok", summary.get("status") == "ok"),
        check("dead_letter_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("dead_letter_gate_records_failure", all(int(item.get("failed", 0) or 0) == 1 for item in results)),
        check("dead_letter_gate_marks_completed_with_errors", all(item.get("data_quality_status") == "completed_with_errors" for item in results)),
        check("dead_letter_gate_writes_dead_letter", all(int(item.get("dead_letter_count", 0) or 0) == 1 and bool(item.get("dead_letter_preview_has_error")) for item in results)),
        check("dead_letter_gate_continues_good_records", all(int(item.get("written", 0) or 0) == int(item.get("records", -1) or -1) - 1 for item in results)),
        check("dead_letter_gate_checkpoint_reaches_end", all(int(item.get("checkpoint_last_sequence", -1) or -1) == int(item.get("records", -2) or -2) - 1 for item in results)),
        check("dead_letter_gate_validation_rejects_shadow", all(item.get("validation_status") == "failed" for item in results)),
    ]


def source_scan_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    scan_modes = {item.get("scan_mode") for item in results}
    backends = {item.get("raw_backend") for item in results}
    return [
        check("source_scan_gate_status_ok", summary.get("status") == "ok"),
        check("source_scan_gate_covers_temporalstore_and_matrixkv", backends == {"temporalstore", "matrixkv"}),
        check("source_scan_gate_covers_record_count_record_index_scan_hash", scan_modes == {"record_count", "record_index", "scan_hash"}),
        check("source_scan_gate_validates_shadow", all(item.get("validation_status") == "ok" and bool(item.get("validation_fingerprint_match")) for item in results)),
        check("source_scan_gate_writes_all_records", all(int(item.get("written", 0) or 0) == int(item.get("records", -1) or -1) for item in results)),
        check("source_scan_gate_has_no_failures", all(int(item.get("failed", 0) or 0) == 0 for item in results)),
        check("source_scan_gate_marks_scan_hash_estimated", all(bool(item.get("source_record_count_estimated")) is (item.get("expected_scan_mode") == "scan_hash") for item in results)),
    ]


def partial_repair_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("partial_repair_gate_status_ok", summary.get("status") == "ok"),
        check("partial_repair_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("partial_repair_gate_filters_source_records", all(int(item.get("shadow_filtered", 0) or 0) > 0 for item in results)),
        check("partial_repair_gate_writes_expected_slice", all(int(item.get("shadow_written", 0) or 0) == int(item.get("expected_matches", -1) or -1) for item in results)),
        check("partial_repair_gate_validates_shadow", all(item.get("validation_status") == "ok" and bool(item.get("validation_fingerprint_match")) for item in results)),
        check("partial_repair_gate_promotes_expected_slice", all(int(item.get("repair_written", 0) or 0) == int(item.get("expected_matches", -1) or -1) for item in results)),
        check("partial_repair_gate_retry_is_idempotent", all(int(item.get("retry_duplicate", 0) or 0) == int(item.get("expected_matches", -1) or -1) for item in results)),
        check("partial_repair_gate_partial_matches_validation", all(bool(item.get("promotion_partial_matches_validation")) for item in results)),
        check("partial_repair_gate_audit_written", all(bool(item.get("audit_written")) for item in results)),
    ]


def resume_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("resume_gate_status_ok", summary.get("status") == "ok"),
        check("resume_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("resume_gate_checkpoint_found_on_second_run", all(bool(item.get("second_resume_state", {}).get("checkpoint_found")) for item in results)),
        check("resume_gate_second_run_started_after_first_window", all(int(item.get("second_resume_state", {}).get("effective_start_seq", -1)) == int(item.get("first_end_seq", -2)) for item in results)),
        check("resume_gate_blocks_incompatible_source_range", all(bool(item.get("incompatible_resume_blocked")) for item in results)),
        check("resume_gate_confirmed_range_change_ignores_checkpoint", all(bool(item.get("confirmed_resume_state", {}).get("checkpoint_ignored")) for item in results)),
        check("resume_gate_confirmed_range_change_scans_requested_window", all(int(item.get("confirmed_scanned", -1)) == max(0, int(item.get("records", 0)) - 1) for item in results)),
        check("resume_gate_confirmed_range_change_is_idempotent", all(int(item.get("confirmed_duplicate", 0) or 0) > 0 and int(item.get("confirmed_written", -1)) == 0 for item in results)),
        check("resume_gate_confirmed_range_change_has_no_failures", all(int(item.get("confirmed_failed", -1)) == 0 and int(item.get("confirmed_dead_letter", -1)) == 0 for item in results)),
        check("resume_gate_completed_expected_records", all(int(item.get("actual_records", -1)) == int(item.get("expected_records", -2)) == int(item.get("records", -3)) for item in results)),
        check("resume_gate_fingerprint_match", all(bool(item.get("serving_record_fingerprint_match")) for item in results)),
    ]


def prometheus_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("prometheus_gate_status_ok", summary.get("status") == "ok"),
        check("prometheus_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("prometheus_gate_shadow_metrics_present", all(all((item.get("shadow_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_validation_metrics_present", all(all((item.get("validation_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_incremental_repair_metrics_present", all(all((item.get("repair_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_emitted_samples", all(int(item.get("shadow_metric_count", 0) or 0) > 0 and int(item.get("validation_metric_count", 0) or 0) > 0 and int(item.get("repair_metric_count", 0) or 0) > 0 for item in results)),
    ]


def run_readiness(args: argparse.Namespace) -> Json:
    checks = parser_support_checks() + docs_checks() + append_accounting_checks()
    benchmark_summary: Json = {}
    baseline_summary: Json = {}
    cutover_summary: Json = {}
    dead_letter_summary: Json = {}
    source_scan_summary: Json = {}
    partial_repair_summary: Json = {}
    resume_summary: Json = {}
    prometheus_summary: Json = {}
    if not args.skip_local_benchmark:
        benchmark_summary = run_local_gate(args)
        checks.extend(benchmark_checks(benchmark_summary))
    if not args.skip_baseline_gate:
        baseline_summary = run_baseline_gate(args)
        checks.extend(baseline_gate_checks(baseline_summary))
    if not args.skip_cutover_gate:
        cutover_summary = run_cutover_gate(args)
        checks.extend(cutover_checks(cutover_summary))
    if not args.skip_dead_letter_gate:
        dead_letter_summary = run_dead_letter_gate(args)
        checks.extend(dead_letter_checks(dead_letter_summary))
    if not args.skip_source_scan_gate:
        source_scan_summary = run_source_scan_gate(args)
        checks.extend(source_scan_checks(source_scan_summary))
    if not args.skip_partial_repair_gate:
        partial_repair_summary = run_partial_repair_gate(args)
        checks.extend(partial_repair_checks(partial_repair_summary))
    if not args.skip_resume_gate:
        resume_summary = run_resume_gate(args)
        checks.extend(resume_checks(resume_summary))
    if not args.skip_prometheus_gate:
        prometheus_summary = run_prometheus_gate(args)
        checks.extend(prometheus_checks(prometheus_summary))
    status = "ok" if all(item["passed"] for item in checks) else "failed"
    return {
        "status": status,
        "checks": checks,
        "benchmark": benchmark_summary,
        "baseline_gate": baseline_summary,
        "cutover_gate": cutover_summary,
        "dead_letter_gate": dead_letter_summary,
        "source_scan_gate": source_scan_summary,
        "partial_repair_gate": partial_repair_summary,
        "resume_gate": resume_summary,
        "prometheus_gate": prometheus_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate MatrixArk context backfill readiness surface.")
    parser.add_argument("--records", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--batch-sizes", default="32,64")
    parser.add_argument("--incremental-records", type=int, default=32)
    parser.add_argument("--payload-bytes", type=int, default=16)
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--skip-local-benchmark", action="store_true")
    parser.add_argument("--skip-baseline-gate", action="store_true")
    parser.add_argument("--skip-cutover-gate", action="store_true")
    parser.add_argument("--skip-dead-letter-gate", action="store_true")
    parser.add_argument("--skip-source-scan-gate", action="store_true")
    parser.add_argument("--skip-partial-repair-gate", action="store_true")
    parser.add_argument("--skip-resume-gate", action="store_true")
    parser.add_argument("--skip-prometheus-gate", action="store_true")
    parser.add_argument("--json-output", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_readiness(args)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
