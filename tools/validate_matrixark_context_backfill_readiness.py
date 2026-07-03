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
    "incremental_repair",
    "serving_record_fingerprint_match",
    "matrixark_context_backfill_validation_check",
    "matrixark_context_backfill_incremental_repair_status",
    "--baseline-json",
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
        check("benchmark_has_latency_gate_options", {"--max-full-shadow-p95-ms", "--max-incremental-shadow-p95-ms", "--max-incremental-repair-p95-ms"}.issubset(benchmark_options)),
        check("benchmark_has_backend_parity_gate", "--min-backend-qps-ratio" in benchmark_options),
        check("benchmark_has_baseline_regression_gate", {"--baseline-json", "--min-baseline-qps-ratio", "--max-baseline-latency-ratio"}.issubset(benchmark_options)),
        check("backfill_has_prometheus_output", "--prometheus-output" in backfill_options),
    ]


def docs_checks() -> list[Json]:
    doc_path = ROOT / "docs" / "matrixark_context_backfill.md"
    text = doc_path.read_text(encoding="utf-8") if doc_path.exists() else ""
    checks = [check("manual_exists", doc_path.exists(), str(doc_path))]
    checks.extend(check(f"manual_mentions_{marker}", marker in text) for marker in REQUIRED_DOC_MARKERS)
    return checks


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
        min_backend_qps_ratio=0.000001,
        max_full_shadow_p95_ms=1000.0,
        max_incremental_shadow_p95_ms=1000.0,
        max_incremental_repair_p95_ms=1000.0,
        gate_aggregation="min",
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
            min_backend_qps_ratio=0.000001,
            max_full_shadow_p95_ms=1000.0,
            max_incremental_shadow_p95_ms=1000.0,
            max_incremental_repair_p95_ms=1000.0,
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
                "repair_status": repair_summary.get("status"),
                "shadow_prometheus_output": str(shadow_prometheus),
                "repair_prometheus_output": str(repair_prometheus),
                "shadow_metric_count": sum(1 for line in shadow_text.splitlines() if line and not line.startswith("#")),
                "repair_metric_count": sum(1 for line in repair_text.splitlines() if line and not line.startswith("#")),
                "shadow_metrics_present": {metric: metric in shadow_text for metric in required_shadow_metrics},
                "repair_metrics_present": {metric: metric in repair_text for metric in required_repair_metrics},
            })
    status = "ok" if all(
        item["shadow_status"] == "ok"
        and item["repair_status"] == "ok"
        and all(item["shadow_metrics_present"].values())
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


def resume_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("resume_gate_status_ok", summary.get("status") == "ok"),
        check("resume_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("resume_gate_checkpoint_found_on_second_run", all(bool(item.get("second_resume_state", {}).get("checkpoint_found")) for item in results)),
        check("resume_gate_second_run_started_after_first_window", all(int(item.get("second_resume_state", {}).get("effective_start_seq", -1)) == int(item.get("first_end_seq", -2)) for item in results)),
        check("resume_gate_completed_expected_records", all(int(item.get("actual_records", -1)) == int(item.get("expected_records", -2)) == int(item.get("records", -3)) for item in results)),
        check("resume_gate_fingerprint_match", all(bool(item.get("serving_record_fingerprint_match")) for item in results)),
    ]


def prometheus_checks(summary: Json) -> list[Json]:
    results = summary.get("results") if isinstance(summary.get("results"), list) else []
    return [
        check("prometheus_gate_status_ok", summary.get("status") == "ok"),
        check("prometheus_gate_covers_temporalstore_and_matrixkv", {item.get("raw_backend") for item in results} == {"temporalstore", "matrixkv"}),
        check("prometheus_gate_shadow_metrics_present", all(all((item.get("shadow_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_incremental_repair_metrics_present", all(all((item.get("repair_metrics_present") or {}).values()) for item in results)),
        check("prometheus_gate_emitted_samples", all(int(item.get("shadow_metric_count", 0) or 0) > 0 and int(item.get("repair_metric_count", 0) or 0) > 0 for item in results)),
    ]


def run_readiness(args: argparse.Namespace) -> Json:
    checks = parser_support_checks() + docs_checks()
    benchmark_summary: Json = {}
    baseline_summary: Json = {}
    resume_summary: Json = {}
    prometheus_summary: Json = {}
    if not args.skip_local_benchmark:
        benchmark_summary = run_local_gate(args)
        checks.extend(benchmark_checks(benchmark_summary))
    if not args.skip_baseline_gate:
        baseline_summary = run_baseline_gate(args)
        checks.extend(baseline_gate_checks(baseline_summary))
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
