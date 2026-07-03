#!/usr/bin/env python3
"""Validate MatrixArk context backfill production-readiness surface."""

from __future__ import annotations

import argparse
import json
import sys
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
        "batch_sizes": summary.get("batch_sizes", []),
        "raw_backends": summary.get("raw_backends", []),
        "batch_size_summary": summary.get("batch_size_summary", {}),
        "qps_summary": summary.get("qps_summary", {}),
        "latency_ms_summary": summary.get("latency_ms_summary", {}),
    }


def benchmark_checks(summary: Json) -> list[Json]:
    performance_gate = summary.get("performance_gate") if isinstance(summary.get("performance_gate"), dict) else {}
    batch_size_summary = summary.get("batch_size_summary") if isinstance(summary.get("batch_size_summary"), dict) else {}
    recommendations = batch_size_summary.get("recommendations") if isinstance(batch_size_summary.get("recommendations"), dict) else {}
    return [
        check("local_benchmark_status_ok", summary.get("status") == "ok"),
        check("local_benchmark_gate_passed", bool(performance_gate.get("passed"))),
        check("local_benchmark_covers_temporalstore_and_matrixkv", set(summary.get("raw_backends") or []) == {"temporalstore", "matrixkv"}),
        check("local_benchmark_exercised_batch_sweep", len(summary.get("batch_sizes") or []) >= 2),
        check("local_benchmark_reports_balanced_recommendation", isinstance(recommendations.get("best_balanced_min_qps"), dict)),
    ]


def run_readiness(args: argparse.Namespace) -> Json:
    checks = parser_support_checks() + docs_checks()
    benchmark_summary: Json = {}
    if not args.skip_local_benchmark:
        benchmark_summary = run_local_gate(args)
        checks.extend(benchmark_checks(benchmark_summary))
    status = "ok" if all(item["passed"] for item in checks) else "failed"
    return {
        "status": status,
        "checks": checks,
        "benchmark": benchmark_summary,
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
