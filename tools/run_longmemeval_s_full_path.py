#!/usr/bin/env python3
"""Run LongMemEval_s as ingest-once/query-many full-path scoring.

This is the LongMemEval_s sibling of the LOCOMO 90% gate. It accepts the real
LongMemEval_s export shape, loads each long conversation once, scores every
question against that shared bundle, and emits a compact gate report.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from benchmark_threshold_policy import add_threshold_policy_args, resolve_threshold_policy


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LongMemEval_s full-path retrieval/reader scoring.")
    parser.add_argument("--input", default="/tmp/longmemeval_s.json", help="LongMemEval_s JSON export path.")
    parser.add_argument(
        "--report",
        default="/tmp/temporalstore_longmemeval_s_full_path_result.json",
        help="Harness JSON report path.",
    )
    parser.add_argument("--misses", default="/tmp/temporalstore_longmemeval_s_full_path_misses.jsonl")
    add_threshold_policy_args(parser)
    parser.add_argument("--max-events", type=int, default=14)
    parser.add_argument("--reader-mode", choices=("deterministic", "open-source", "auto"), default="deterministic")
    parser.add_argument("--reader-provider-name", default="matrixark-cpp-oss-context")
    parser.add_argument("--reader-model", default="google/flan-t5-small")
    parser.add_argument("--reader-base-url", default="")
    parser.add_argument("--reader-api-key-env", default="TEMPORALSTORE_READER_API_KEY")
    parser.add_argument("--reader-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument("--reader-no-fallback", action="store_true")
    parser.add_argument("--require-rust-temporalstore", action="store_true")
    parser.add_argument("--rust-temporalstore-max-cases", type=int, default=4)
    parser.add_argument("--rust-temporalstore-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--rust-temporalstore-source-limit", type=int, default=64)
    parser.add_argument("--rust-temporalstore-score-tolerance", type=float, default=0.0)
    parser.add_argument("--rust-temporalstore-jsonl", default="")
    parser.add_argument("--rust-temporalstore-report", default="")
    args = parser.parse_args()
    thresholds = resolve_threshold_policy(args)

    repo = Path(__file__).resolve().parents[1]
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"missing LongMemEval_s input: {input_path}", file=sys.stderr)
        return 2

    command = [
        sys.executable,
        str(repo / "tools" / "run_locomo_ingest_once.py"),
        "--input",
        str(input_path),
        "--output",
        args.report,
        "--misses",
        args.misses,
        "--dataset-name",
        "longmemeval_s",
        "--min-hit-rate",
        str(thresholds["min_hit_rate"]),
        "--min-case-count",
        str(thresholds["min_case_count"]),
        "--min-reader-hit-rate",
        str(thresholds["min_reader_hit_rate"]),
        "--min-token-reduction-percent",
        str(thresholds["min_token_reduction_percent"]),
        "--max-retrieval-p95-ms",
        str(thresholds["max_retrieval_p95_ms"]),
        "--max-reader-p95-ms",
        str(thresholds["max_reader_p95_ms"]),
        "--max-events",
        str(args.max_events),
        "--reader-mode",
        args.reader_mode,
        "--reader-provider-name",
        args.reader_provider_name,
        "--reader-model",
        args.reader_model,
        "--reader-api-key-env",
        args.reader_api_key_env,
        "--reader-timeout-seconds",
        str(args.reader_timeout_seconds),
        "--reader-max-context-chars",
        str(args.reader_max_context_chars),
    ]
    if args.reader_base_url:
        command.extend(["--reader-base-url", args.reader_base_url])
    if args.reader_no_fallback:
        command.append("--reader-no-fallback")
    if args.require_rust_temporalstore:
        command.append("--require-rust-temporalstore")
        command.extend(["--rust-temporalstore-max-cases", str(args.rust_temporalstore_max_cases)])
        command.extend(["--rust-temporalstore-timeout-seconds", str(args.rust_temporalstore_timeout_seconds)])
        command.extend(["--rust-temporalstore-source-limit", str(args.rust_temporalstore_source_limit)])
        command.extend(["--rust-temporalstore-score-tolerance", str(args.rust_temporalstore_score_tolerance)])
        if args.rust_temporalstore_jsonl:
            command.extend(["--rust-temporalstore-jsonl", args.rust_temporalstore_jsonl])
        if args.rust_temporalstore_report:
            command.extend(["--rust-temporalstore-report", args.rust_temporalstore_report])
    if thresholds["require_open_source_reader"]:
        command.append("--require-open-source-reader")
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    case_count = int(report.get("case_count") or 0)
    hit_rate = float(report.get("hit_rate") or 0.0)
    output = {
        "longmemeval_s_full_path_ready": hit_rate >= thresholds["min_hit_rate"],
        "dataset": report.get("dataset"),
        "mode": report.get("mode"),
        "threshold_profile": args.threshold_profile,
        "case_count": case_count,
        "min_case_count": thresholds["min_case_count"],
        "conversation_count": int(report.get("conversation_count") or 0),
        "source_count": int(report.get("source_count") or 0),
        "hit_rate": hit_rate,
        "min_hit_rate": thresholds["min_hit_rate"],
        "mean_reciprocal_rank": float(report.get("mean_reciprocal_rank") or 0.0),
        "answer_term_coverage": float(report.get("answer_term_coverage") or 0.0),
        "reader_hit_rate": float(report.get("reader_hit_rate") or report.get("deterministic_reader_hit_rate") or 0.0),
        "reader_answer_coverage": float(
            report.get("reader_answer_coverage") or report.get("deterministic_reader_answer_coverage") or 0.0
        ),
        "deterministic_reader_hit_rate": float(report.get("deterministic_reader_hit_rate") or 0.0),
        "deterministic_reader_answer_coverage": float(report.get("deterministic_reader_answer_coverage") or 0.0),
        "reader_mode_requested": report.get("reader_mode_requested"),
        "reader_mode_effective": report.get("reader_mode_effective"),
        "reader_provider_name": report.get("reader_provider_name"),
        "reader_model": report.get("reader_model"),
        "reader_open_source_calls": int(report.get("reader_open_source_calls") or 0),
        "reader_fallback_count": int(report.get("reader_fallback_count") or 0),
        "reader_error_count": int(report.get("reader_error_count") or 0),
        "zero_hit_queries": int(report.get("zero_hit_queries") or 0),
        "reader_zero_hit_queries": int(report.get("reader_zero_hit_queries") or 0),
        "benchmark_quality_ready": bool(report.get("benchmark_quality_ready")),
        "benchmark_hit_at_k": float(report.get("benchmark_hit_at_k") or 0.0),
        "benchmark_recall_at_k": float(report.get("benchmark_recall_at_k") or 0.0),
        "benchmark_mean_reciprocal_rank": float(report.get("benchmark_mean_reciprocal_rank") or 0.0),
        "benchmark_token_reduction_percent": float(report.get("benchmark_token_reduction_percent") or 0.0),
        "benchmark_retrieval_p50_ms": float(report.get("benchmark_retrieval_p50_ms") or 0.0),
        "benchmark_retrieval_p95_ms": float(report.get("benchmark_retrieval_p95_ms") or 0.0),
        "benchmark_reader_p50_ms": float(report.get("benchmark_reader_p50_ms") or 0.0),
        "benchmark_reader_p95_ms": float(report.get("benchmark_reader_p95_ms") or 0.0),
        "benchmark_avg_source_tokens_per_query": float(report.get("benchmark_avg_source_tokens_per_query") or 0.0),
        "benchmark_avg_retrieved_tokens_per_query": float(report.get("benchmark_avg_retrieved_tokens_per_query") or 0.0),
        "benchmark_avg_retrieved_blocks_per_query": float(report.get("benchmark_avg_retrieved_blocks_per_query") or 0.0),
        "benchmark_per_query_count": int(report.get("benchmark_per_query_count") or 0),
        "benchmark_threshold_passed": bool(report.get("benchmark_threshold_passed")),
        "benchmark_threshold_violation_count": int(report.get("benchmark_threshold_violation_count") or 0),
        "benchmark_threshold_violations": report.get("benchmark_threshold_violations") or [],
        "benchmark_thresholds": report.get("benchmark_thresholds") or {},
        "category_breakdown": report.get("category_breakdown") or {},
        "report": str(report_path),
        "misses": args.misses,
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["longmemeval_s_full_path_ready"] else 1


def run(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
