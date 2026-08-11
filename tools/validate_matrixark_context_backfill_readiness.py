#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate MatrixArk context backfill production-readiness surface."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import matrixark_context_backfill as backfill  # noqa: E402
import matrixark_context_backfill_benchmark as bench  # noqa: E402
import matrixark_dual_write_ingestion_benchmark as dual_bench  # noqa: E402

Json = dict[str, Any]


REQUIRED_DOC_MARKERS = [
    "--raw-backend=temporalstore",
    "--raw-backend=matrixkv",
    "--raw-backends=both",
    "--min-backend-qps-ratio",
    "--prometheus-output",
    "--dual-write-evidence-dir",
    "matrixark_dual_write_ingestion_qps",
    "matrixark_dual_write_ingestion_backend_qps_ratio",
    "--batch-sizes",
    "production_candidate",
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
    "--confirm-unvalidated-target-state",
    "--confirm-empty-activation",
    "--confirm-resume-range-change",
    "--confirm-active-target",
    "--confirm-rollback-noop",
    "--confirm-rollback-target-state",
    "--expect-active-prefix",
    "--confirm-no-active-prefix-precondition",
    "--dry-run-check-target",
    "matrixark_context_backfill_validation_check",
    "matrixark_context_backfill_incremental_repair_status",
    "matrixark_context_backfill_incremental_repair_promotion_data_quality_status",
    "matrixark_context_backfill_data_quality_status",
    "completed_with_errors",
    "--baseline-json",
    "append-time idempotency",
    "manifest_payload_sha256",
    "matrixark_context_backfill_manifest_v1",
    "verify_manifest",
    "matrixark_context_backfill_manifest_verification_status",
    "matrixark_context_backfill_manifest_verification_check",
    "matrixark_context_backfill_ci_evidence_verification_status",
    "verify_matrixark_context_backfill_ci_evidence.py",
    "uploaded evidence bundle remains verifiable after download or relocation",
    "--require-relative-paths=1",
    "readiness report has `status=\"ok\"`",
    "all readiness checks passed",
    "all required readiness gate sections report `status=\"ok\"`",
    "dual-write readiness JSON reports `status=\"ok\"`",
    "dual-write evidence manifest reports `status=\"ok\"`",
    "dual-write evidence manifest uses `matrixark_dual_write_readiness_evidence_v1`",
    "dual-write evidence manifest stores portable relative artifact paths",
    "dual-write evidence manifest artifact paths stay inside the dual-write evidence directory",
    "dual-write evidence manifest artifact sizes and SHA-256 checksums match",
    "nested_verified_artifacts",
]


def check(name: str, passed: bool, detail: str = "") -> Json:
    return {"name": name, "passed": bool(passed), "detail": detail}


def int_or_default(value: Any, default: int = -1) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parser_support_checks() -> list[Json]:
    backfill_parser = backfill.build_parser()
    benchmark_parser = bench.build_parser()
    dual_write_parser = dual_bench.build_parser()
    backfill_options = {option for action in backfill_parser._actions for option in action.option_strings}
    benchmark_options = {option for action in benchmark_parser._actions for option in action.option_strings}
    dual_write_options = {option for action in dual_write_parser._actions for option in action.option_strings}
    mode_action = next(action for action in backfill_parser._actions if "--mode" in action.option_strings)
    raw_backend_action = next(action for action in backfill_parser._actions if "--raw-backend" in action.option_strings)
    benchmark_raw_action = next(action for action in benchmark_parser._actions if "--raw-backends" in action.option_strings)
    return [
        check("backfill_modes_cover_batch_and_incremental", {"shadow", "validate_shadow", "activate_shadow", "rollback_activation", "incremental_repair", "in_place"}.issubset(set(mode_action.choices or []))),
        check("backfill_has_manifest_verification_mode", "verify_manifest" in set(mode_action.choices or [])),
        check("backfill_has_local_recovery_report_mode", "local_recovery_report" in set(mode_action.choices or [])),
        check("backfill_raw_backend_choices_cover_all_raw_options", {"temporalstore", "matrixkv"}.issubset(set(raw_backend_action.choices or []))),
        check("benchmark_raw_backend_choices_cover_all_raw_options", {"both", "temporalstore", "matrixkv"}.issubset(set(benchmark_raw_action.choices or []))),
        check("benchmark_has_batch_sweep_option", "--batch-sizes" in benchmark_options),
        check("benchmark_has_latency_gate_options", {"--max-full-shadow-p95-ms", "--max-incremental-shadow-p95-ms", "--max-incremental-repair-p95-ms", "--max-partial-shadow-p95-ms", "--max-partial-repair-p95-ms"}.issubset(benchmark_options)),
        check("benchmark_has_partial_repair_qps_gate", "--min-partial-repair-qps" in benchmark_options),
        check("benchmark_has_backend_parity_gate", "--min-backend-qps-ratio" in benchmark_options),
        check("benchmark_has_baseline_regression_gate", {"--baseline-json", "--min-baseline-qps-ratio", "--max-baseline-latency-ratio"}.issubset(benchmark_options)),
        check("dual_write_benchmark_has_raw_backend_sweep", "--raw-backends" in dual_write_options),
        check("dual_write_benchmark_has_backend_parity_gate", "--min-backend-qps-ratio" in dual_write_options),
        check("dual_write_benchmark_has_count_gate", "--require-dual-write-counts" in dual_write_options),
        check("dual_write_benchmark_has_prometheus_output", "--prometheus-output" in dual_write_options),
        check("backfill_has_prometheus_output", "--prometheus-output" in backfill_options),
        check("backfill_has_dry_run_target_check_option", "--dry-run-check-target" in backfill_options),
        check("backfill_has_active_target_confirmation", "--confirm-active-target" in backfill_options),
        check("backfill_has_rollback_noop_confirmation", "--confirm-rollback-noop" in backfill_options),
        check("backfill_has_rollback_target_state_confirmation", "--confirm-rollback-target-state" in backfill_options),
        check("backfill_has_expect_active_prefix_precondition", "--expect-active-prefix" in backfill_options),
        check("backfill_has_active_prefix_precondition_bypass_confirmation", "--confirm-no-active-prefix-precondition" in backfill_options),
        check("backfill_has_skip_validation_confirmation", "--confirm-skip-validation" in backfill_options),
        check("backfill_has_non_strict_validation_confirmation", "--confirm-non-strict-validation" in backfill_options),
        check("backfill_has_unvalidated_target_state_confirmation", "--confirm-unvalidated-target-state" in backfill_options),
        check("backfill_has_empty_activation_confirmation", "--confirm-empty-activation" in backfill_options),
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
        check("run_backfill_writes_self_verifying_manifest", all(token in text for token in ["matrixark_context_backfill_manifest_v1", "manifest_payload_sha256", "canonical_json_sha256"])),
        check("run_backfill_can_verify_manifest_hash", all(token in text for token in ["def run_verify_manifest", "manifest_payload_sha256_match", "computed_manifest_payload_sha256"])),
        check("run_backfill_exports_verify_manifest_prometheus", all(token in text for token in ["def verify_manifest_to_prometheus", "matrixark_context_backfill_manifest_verification_status", "matrixark_context_backfill_manifest_verification_check"])),
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
            bypass_target_prefix = f"matrixark:context_backfill:readiness_cutover:{raw_backend}:bypass"
            empty_source_prefix = f"{source_prefix}:empty"
            empty_target_prefix = f"{target_prefix}:empty"
            job_id = f"readiness-cutover-{raw_backend}"
            bypass_job_id = f"{job_id}:bypass"
            empty_job_id = f"{job_id}:empty"
            kv.put_string(active_key, old_prefix)
            kv.put_string(f"{empty_source_prefix}:record_count", "0")
            backfill.MatrixKVBackfillTarget(kv, prefix=old_prefix, raw_backend=raw_backend).append_many([
                {"record_type": "context_event", "event_id_hash": f"old-{raw_backend}"}
            ])

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
            bypass_shadow_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=bypass_target_prefix,
                raw_backend=raw_backend,
                job_id=bypass_job_id,
                batch_size=args.batch_size,
                end_seq=records,
            )
            bypass_shadow = backfill.run_backfill(bypass_shadow_args)
            missing_precondition_blocked = False
            try:
                missing_precondition_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=bypass_target_prefix,
                    raw_backend=raw_backend,
                    job_id=f"{bypass_job_id}:blocked",
                    batch_size=args.batch_size,
                    mode="activate_shadow",
                )
                missing_precondition_args.confirm_activate = "YES"
                backfill.run_activate_shadow(missing_precondition_args)
            except backfill.BackfillError as exc:
                missing_precondition_blocked = "requires --expect-active-prefix" in str(exc)
            bypass_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=bypass_target_prefix,
                raw_backend=raw_backend,
                job_id=bypass_job_id,
                batch_size=args.batch_size,
                mode="activate_shadow",
            )
            bypass_args.confirm_activate = "YES"
            bypass_args.confirm_no_active_prefix_precondition = "YES"
            bypass_activation = backfill.run_activate_shadow(bypass_args)
            kv_after_bypass = backfill.LocalJsonKV(kv_path)
            bypass_audit = kv_after_bypass.hget(f"{active_key}:audit", bypass_job_id)
            kv_after_bypass.put_string(active_key, old_prefix)
            empty_activation_blocked = False
            try:
                empty_blocked_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=empty_source_prefix,
                    target_prefix=empty_target_prefix,
                    raw_backend=raw_backend,
                    job_id=f"{empty_job_id}:blocked",
                    batch_size=args.batch_size,
                    mode="activate_shadow",
                    end_seq=0,
                )
                empty_blocked_args.confirm_activate = "YES"
                empty_blocked_args.expect_active_prefix = old_prefix
                backfill.run_activate_shadow(empty_blocked_args)
            except backfill.BackfillError as exc:
                empty_activation_blocked = "confirm-empty-activation=YES" in str(exc)
            empty_confirmed_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=empty_source_prefix,
                target_prefix=empty_target_prefix,
                raw_backend=raw_backend,
                job_id=empty_job_id,
                batch_size=args.batch_size,
                mode="activate_shadow",
                end_seq=0,
            )
            empty_confirmed_args.confirm_activate = "YES"
            empty_confirmed_args.confirm_empty_activation = "YES"
            empty_confirmed_args.expect_active_prefix = old_prefix
            empty_activation = backfill.run_activate_shadow(empty_confirmed_args)
            kv_after_empty = backfill.LocalJsonKV(kv_path)
            empty_activation_audit = kv_after_empty.hget(f"{active_key}:audit", empty_job_id)
            kv_after_empty.put_string(active_key, old_prefix)
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
            activate_args.expect_active_prefix = f"matrixark:context:active:old:{raw_backend}"
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
            rollback_args.expect_active_prefix = target_prefix
            rollback = backfill.run_rollback_activation(rollback_args)
            kv_after_rollback = backfill.LocalJsonKV(kv_path)
            rollback_audit = kv_after_rollback.hget(f"{active_key}:rollback_audit", f"{job_id}:rollback")
            noop_rollback_job_id = f"{job_id}:rollback-noop"
            kv_after_rollback.put_string(f"{active_key}:previous:{noop_rollback_job_id}", old_prefix)
            noop_rollback_blocked = False
            try:
                noop_rollback_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=noop_rollback_job_id,
                    batch_size=args.batch_size,
                    mode="rollback_activation",
                )
                noop_rollback_args.confirm_rollback = "YES"
                noop_rollback_args.rollback_job_id = noop_rollback_job_id
                noop_rollback_args.expect_active_prefix = old_prefix
                backfill.run_rollback_activation(noop_rollback_args)
            except backfill.BackfillError as exc:
                noop_rollback_blocked = "previous prefix equals current active prefix" in str(exc)
            noop_rollback_confirmed_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=f"{noop_rollback_job_id}:confirmed",
                batch_size=args.batch_size,
                mode="rollback_activation",
            )
            noop_rollback_confirmed_args.confirm_rollback = "YES"
            noop_rollback_confirmed_args.confirm_rollback_noop = "YES"
            noop_rollback_confirmed_args.rollback_job_id = noop_rollback_job_id
            noop_rollback_confirmed_args.expect_active_prefix = old_prefix
            noop_rollback_confirmed = backfill.run_rollback_activation(noop_rollback_confirmed_args)
            kv_after_noop = backfill.LocalJsonKV(kv_path)
            noop_rollback_audit = kv_after_noop.hget(f"{active_key}:rollback_audit", f"{noop_rollback_job_id}:confirmed")
            unhealthy_rollback_job_id = f"{job_id}:rollback-unhealthy"
            missing_prefix = f"{old_prefix}:missing"
            kv_after_noop.put_string(active_key, target_prefix)
            kv_after_noop.put_string(f"{active_key}:previous:{unhealthy_rollback_job_id}", missing_prefix)
            unhealthy_rollback_blocked = False
            try:
                unhealthy_rollback_args = bench.make_backfill_args(
                    kv_path=kv_path,
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    job_id=unhealthy_rollback_job_id,
                    batch_size=args.batch_size,
                    mode="rollback_activation",
                )
                unhealthy_rollback_args.confirm_rollback = "YES"
                unhealthy_rollback_args.rollback_job_id = unhealthy_rollback_job_id
                unhealthy_rollback_args.expect_active_prefix = target_prefix
                backfill.run_rollback_activation(unhealthy_rollback_args)
            except backfill.BackfillError as exc:
                unhealthy_rollback_blocked = "previous prefix is empty or unhealthy" in str(exc)
            unhealthy_rollback_confirmed_args = bench.make_backfill_args(
                kv_path=kv_path,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                job_id=f"{unhealthy_rollback_job_id}:confirmed",
                batch_size=args.batch_size,
                mode="rollback_activation",
            )
            unhealthy_rollback_confirmed_args.confirm_rollback = "YES"
            unhealthy_rollback_confirmed_args.confirm_rollback_target_state = "YES"
            unhealthy_rollback_confirmed_args.rollback_job_id = unhealthy_rollback_job_id
            unhealthy_rollback_confirmed_args.expect_active_prefix = target_prefix
            unhealthy_rollback_confirmed = backfill.run_rollback_activation(unhealthy_rollback_confirmed_args)
            kv_after_unhealthy = backfill.LocalJsonKV(kv_path)
            unhealthy_rollback_audit = kv_after_unhealthy.hget(f"{active_key}:rollback_audit", f"{unhealthy_rollback_job_id}:confirmed")

            results.append({
                "raw_backend": raw_backend,
                "shadow_status": shadow.get("status"),
                "shadow_written": int(shadow.get("metrics", {}).get("written", 0) or 0),
                "missing_active_precondition_blocked": missing_precondition_blocked,
                "bypass_shadow_status": bypass_shadow.get("status"),
                "bypass_activation_status": bypass_activation.get("status"),
                "bypass_audit_written": bool(bypass_audit),
                "bypass_audited": bool(bypass_activation.get("active_prefix_precondition_bypassed")) and "active_prefix_precondition_bypassed" in str(bypass_audit),
                "empty_activation_blocked": empty_activation_blocked,
                "empty_activation_confirmed": bool(empty_activation.get("empty_activation_confirmed")),
                "empty_activation_audit_written": bool(empty_activation_audit),
                "empty_activation_audited": "empty_activation_confirmed" in str(empty_activation_audit),
                "activation_status": activated.get("status"),
                "activation_validation_status": activated.get("validation_status"),
                "activation_validation_skipped": bool(activated.get("validation_skipped")),
                "activation_new_prefix": activated.get("new_prefix"),
                "active_after_activation": kv_after_activate.get_string(active_key),
                "activation_audit_written": bool(activation_audit),
                "rollback_status": rollback.get("status"),
                "rollback_to_prefix": rollback.get("to_prefix"),
                "rollback_target_healthy": bool(rollback.get("rollback_target_state", {}).get("healthy_for_rollback")),
                "active_after_rollback": kv_after_rollback.get_string(active_key),
                "rollback_audit_written": bool(rollback_audit),
                "noop_rollback_blocked": noop_rollback_blocked,
                "noop_rollback_confirmed": bool(noop_rollback_confirmed.get("rollback_noop_confirmed")),
                "noop_rollback_audit_written": bool(noop_rollback_audit),
                "noop_rollback_audited": "rollback_noop_confirmed" in str(noop_rollback_audit),
                "unhealthy_rollback_blocked": unhealthy_rollback_blocked,
                "unhealthy_rollback_confirmed": bool(unhealthy_rollback_confirmed.get("rollback_target_state_confirmed")),
                "unhealthy_rollback_audit_written": bool(unhealthy_rollback_audit),
                "unhealthy_rollback_audited": "rollback_target_state_confirmed" in str(unhealthy_rollback_audit),
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["shadow_written"] == records
        and item["missing_active_precondition_blocked"]
        and item["bypass_shadow_status"] == "ok"
        and item["bypass_activation_status"] == "ok"
        and item["bypass_audit_written"]
        and item["bypass_audited"]
        and item["empty_activation_blocked"]
        and item["empty_activation_confirmed"]
        and item["empty_activation_audit_written"]
        and item["empty_activation_audited"]
        and item["activation_status"] == "ok"
        and item["activation_validation_status"] == "ok"
        and not item["activation_validation_skipped"]
        and item["active_after_activation"] == item["activation_new_prefix"]
        and item["activation_audit_written"]
        and item["rollback_status"] == "ok"
        and item["rollback_target_healthy"]
        and item["rollback_to_prefix"] == item["active_after_rollback"]
        and item["rollback_audit_written"]
        and item["noop_rollback_blocked"]
        and item["noop_rollback_confirmed"]
        and item["noop_rollback_audit_written"]
        and item["noop_rollback_audited"]
        and item["unhealthy_rollback_blocked"]
        and item["unhealthy_rollback_confirmed"]
        and item["unhealthy_rollback_audit_written"]
        and item["unhealthy_rollback_audited"]
        for item in results
    ) else "failed"
    return {"status": status, "results": results}




def benchmark_checks(summary: Json) -> list[Json]:
    performance_gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    batch_size_summary = summary.get("batch_size_summary") if isinstance(summary.get("batch_size_summary"), dict) else {}
    recommendations = batch_size_summary.get("recommendations") if isinstance(batch_size_summary.get("recommendations"), dict) else {}
    production_candidate = batch_size_summary.get("production_candidate") if isinstance(batch_size_summary.get("production_candidate"), dict) else {}
    return [
        check("local_benchmark_status_ok", summary.get("status") == "ok"),
        check("local_benchmark_gate_passed", bool(performance_gate.get("passed"))),
        check("local_benchmark_baseline_gate_available", isinstance(summary.get("baseline_gate"), dict)),
        check("local_benchmark_covers_temporalstore_and_matrixkv", set(summary.get("raw_backends") or []) == {"temporalstore", "matrixkv"}),
        check("local_benchmark_exercised_batch_sweep", len(summary.get("batch_sizes") or []) >= 2),
        check("local_benchmark_reports_partial_repair_recommendation", isinstance(recommendations.get("best_partial_repair_qps"), dict)),
        check("local_benchmark_reports_balanced_recommendation", isinstance(recommendations.get("best_balanced_min_qps"), dict)),
        check("local_benchmark_reports_production_candidate", int(production_candidate.get("batch_size", 0) or 0) in set(summary.get("batch_sizes") or []) and float(production_candidate.get("balanced_min_qps", 0.0) or 0.0) > 0.0 and float(production_candidate.get("backend_qps_min_max_ratio", 0.0) or 0.0) > 0.0),
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
        check("cutover_gate_blocks_missing_active_precondition", all(bool(item.get("missing_active_precondition_blocked")) for item in results)),
        check("cutover_gate_bypass_is_explicitly_audited", all(bool(item.get("bypass_audited")) for item in results)),
        check("cutover_gate_blocks_empty_validated_activation", all(bool(item.get("empty_activation_blocked")) for item in results)),
        check("cutover_gate_empty_activation_is_explicitly_audited", all(bool(item.get("empty_activation_confirmed")) and bool(item.get("empty_activation_audited")) for item in results)),
        check("cutover_gate_activation_validated_shadow", all(item.get("activation_validation_status") == "ok" and not item.get("activation_validation_skipped") for item in results)),
        check("cutover_gate_activation_updates_active_pointer", all(item.get("active_after_activation") == item.get("activation_new_prefix") for item in results)),
        check("cutover_gate_activation_audit_written", all(bool(item.get("activation_audit_written")) for item in results)),
        check("cutover_gate_rollback_restores_previous_pointer", all(item.get("rollback_to_prefix") == item.get("active_after_rollback") for item in results)),
        check("cutover_gate_rollback_audit_written", all(bool(item.get("rollback_audit_written")) for item in results)),
        check("cutover_gate_blocks_noop_rollback", all(bool(item.get("noop_rollback_blocked")) for item in results)),
        check("cutover_gate_noop_rollback_is_explicitly_audited", all(bool(item.get("noop_rollback_confirmed")) and bool(item.get("noop_rollback_audited")) for item in results)),
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
        check("partial_repair_gate_promotion_data_quality_clean", all(item.get("promotion_data_quality_status") == "clean" and bool(item.get("promotion_data_quality_clean")) for item in results)),
        check("partial_repair_gate_audit_written", all(bool(item.get("audit_written")) for item in results)),
    ]


def unvalidated_repair_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("unvalidated_repair_gate_status_ok", summary.get("status") == "ok"),
        check("unvalidated_repair_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("unvalidated_repair_gate_blocks_without_target_state_confirmation", all(bool(item.get("blocked_without_target_state_confirmation")) for item in results)),
        check("unvalidated_repair_gate_confirms_and_audits_target_state", all(bool(item.get("unvalidated_target_state_confirmed")) and bool(item.get("audit_unvalidated_target_state_confirmed")) for item in results)),
        check("unvalidated_repair_gate_records_unhealthy_target_state", all(int_or_default(item.get("validation_target_record_count")) == 0 and item.get("validation_target_healthy") is False and int_or_default(item.get("audit_validation_target_record_count")) == 0 and item.get("audit_validation_target_healthy") is False for item in results)),
        check("unvalidated_repair_gate_promotes_records", all(int_or_default(item.get("promotion_written"), 0) > 0 and int_or_default(item.get("promotion_failed")) == 0 and int_or_default(item.get("promotion_dead_letter")) == 0 for item in results)),
    ]


def manifest_verification_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("manifest_verification_gate_status_ok", summary.get("status") == "ok"),
        check("manifest_verification_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("manifest_verification_gate_schema_and_hash_present", all(item.get("manifest_schema") == "matrixark_context_backfill_manifest_v1" and isinstance(item.get("manifest_payload_sha256"), str) and len(str(item.get("manifest_payload_sha256"))) == 64 for item in results)),
        check("manifest_verification_gate_verifies_valid_manifest", all(item.get("verify_status") == "ok" and bool(item.get("verify_hash_match")) and bool(item.get("verify_schema_supported")) for item in results)),
        check("manifest_verification_gate_checks_identity", all(bool(item.get("verify_job_id_matches")) and bool(item.get("verify_target_prefix_matches")) and bool(item.get("verify_raw_backend_matches")) for item in results)),
        check("manifest_verification_gate_exports_valid_prometheus_metrics", all(bool(item.get("verify_prometheus_metrics_present")) for item in results)),
        check("manifest_verification_gate_rejects_tampering", all(item.get("tampered_status") == "failed" and item.get("tampered_hash_match") is False for item in results)),
        check("manifest_verification_gate_exports_tampered_prometheus_metrics", all(bool(item.get("tampered_prometheus_metrics_present")) for item in results)),
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
        check("prometheus_gate_plan_execution_readiness_ready", all(item.get("plan_execution_readiness_status") == "ready" and bool(item.get("plan_execution_readiness_ready")) for item in results)),
        check("prometheus_gate_plan_metrics_present", all(all((item.get("plan_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_shadow_metrics_present", all(all((item.get("shadow_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_validation_metrics_present", all(all((item.get("validation_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_local_recovery_metrics_present", all(all((item.get("local_recovery_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_activation_metrics_present", all(all((item.get("activation_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_rollback_metrics_present", all(all((item.get("rollback_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_incremental_repair_metrics_present", all(all((item.get("repair_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_incremental_repair_manifest_verified", all(item.get("repair_manifest_verification_status") == "ok" for item in results)),
        check("prometheus_gate_emitted_samples", all(int(item.get("plan_metric_count", 0) or 0) > 0 and int(item.get("shadow_metric_count", 0) or 0) > 0 and int(item.get("validation_metric_count", 0) or 0) > 0 and int(item.get("local_recovery_metric_count", 0) or 0) > 0 and int(item.get("activation_metric_count", 0) or 0) > 0 and int(item.get("rollback_metric_count", 0) or 0) > 0 and int(item.get("repair_metric_count", 0) or 0) > 0 for item in results)),
    ]


def dual_write_gate_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    performance_gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    gate_checks = performance_gate.get("checks") if isinstance(performance_gate.get("checks"), list) else []
    return [
        check("dual_write_gate_status_ok", summary.get("status") == "ok"),
        check("dual_write_gate_covers_temporalstore_and_matrixkv", set(summary.get("raw_backends") or []) == {"temporalstore", "matrixkv"}),
        check("dual_write_gate_counts_validated", bool(results) and all(bool(item.get("dual_write_counts_validated")) for item in results)),
        check("dual_write_gate_backend_ratio_checked", any(item.get("metric") == "backend_ingestion_qps_ratio" and item.get("passed") for item in gate_checks)),
        check("dual_write_gate_performance_gate_passed", bool(performance_gate.get("passed"))),
        check("dual_write_gate_prometheus_metrics_present", all((summary.get("prometheus_metrics_present") or {}).values()) and int(summary.get("prometheus_metric_count", 0) or 0) > 0),
        check("dual_write_gate_evidence_persistent", bool(summary.get("evidence_persistent"))),
        check("dual_write_gate_evidence_manifest_present", Path(str(summary.get("evidence_manifest") or "")).exists() and all(str(value) for value in (summary.get("evidence_checksums") or {}).values())),
    ]


def run_readiness(args: argparse.Namespace) -> Json:
    checks = parser_support_checks() + docs_checks() + append_accounting_checks()
    benchmark_summary: Json = {}
    baseline_summary: Json = {}
    cutover_summary: Json = {}
    dead_letter_summary: Json = {}
    source_scan_summary: Json = {}
    partial_repair_summary: Json = {}
    unvalidated_repair_summary: Json = {}
    manifest_verification_summary: Json = {}
    resume_summary: Json = {}
    prometheus_summary: Json = {}
    dual_write_summary: Json = {}
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
    if not args.skip_unvalidated_repair_gate:
        unvalidated_repair_summary = run_unvalidated_repair_gate(args)
        checks.extend(unvalidated_repair_checks(unvalidated_repair_summary))
    if not args.skip_manifest_verification_gate:
        manifest_verification_summary = run_manifest_verification_gate(args)
        checks.extend(manifest_verification_checks(manifest_verification_summary))
    if not args.skip_resume_gate:
        resume_summary = run_resume_gate(args)
        checks.extend(resume_checks(resume_summary))
    if not args.skip_prometheus_gate:
        prometheus_summary = run_prometheus_gate(args)
        checks.extend(prometheus_checks(prometheus_summary))
    if not args.skip_dual_write_gate:
        dual_write_summary = run_dual_write_gate(args)
        checks.extend(dual_write_gate_checks(dual_write_summary))
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
        "unvalidated_repair_gate": unvalidated_repair_summary,
        "manifest_verification_gate": manifest_verification_summary,
        "resume_gate": resume_summary,
        "prometheus_gate": prometheus_summary,
        "dual_write_gate": dual_write_summary,
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
    parser.add_argument("--skip-unvalidated-repair-gate", action="store_true")
    parser.add_argument("--skip-manifest-verification-gate", action="store_true")
    parser.add_argument("--skip-resume-gate", action="store_true")
    parser.add_argument("--skip-prometheus-gate", action="store_true")
    parser.add_argument("--skip-dual-write-gate", action="store_true")
    parser.add_argument("--dual-write-evidence-dir", default="", help="directory for persistent dual-write readiness JSON and Prometheus artifacts")
    parser.add_argument("--json-output", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_readiness(args)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "ok" else 2



# Re-export helpers split into validate_backfill_gates.py
try:  # package path
    from .validate_backfill_gates import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from validate_backfill_gates import *  # noqa: E402,F401,F403


if __name__ == "__main__":
    raise SystemExit(main())
