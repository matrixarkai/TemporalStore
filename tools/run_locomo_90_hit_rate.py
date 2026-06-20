#!/usr/bin/env python3
"""Run the LOCOMO retrieval/context hit-rate gate used for VikingMem parity.

This intentionally gates the metric comparable to the MatrixArk/C++ path's
"retrieval/context hit" number. It also prints answer-term coverage so reader
accuracy gaps stay visible instead of being folded into retrieval scoring.

The gate uses the conversation-load-once/query-many runner rather than the
generic per-case JSONL harness.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LOCOMO 90%+ retrieval/context hit-rate gate.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO JSON export path.")
    parser.add_argument(
        "--report",
        default="/tmp/temporalstore_locomo_ingest_once_result.json",
        help="Harness JSON report path.",
    )
    parser.add_argument("--misses", default="/tmp/temporalstore_locomo_ingest_once_misses.jsonl")
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--min-case-count", type=int, default=1)
    parser.add_argument("--min-reader-hit-rate", type=float, default=0.0)
    parser.add_argument("--min-token-reduction-percent", type=float, default=0.0)
    parser.add_argument("--max-retrieval-p95-ms", type=float, default=1000.0)
    parser.add_argument("--max-reader-p95-ms", type=float, default=30000.0)
    parser.add_argument("--max-events", type=int, default=128)
    parser.add_argument("--reader-mode", choices=("deterministic", "open-source", "auto"), default="deterministic")
    parser.add_argument("--reader-provider-name", default="matrixark-cpp-oss-context")
    parser.add_argument("--reader-model", default="google/flan-t5-small")
    parser.add_argument("--reader-base-url", default="")
    parser.add_argument("--reader-api-key-env", default="TEMPORALSTORE_READER_API_KEY")
    parser.add_argument("--reader-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument("--reader-no-fallback", action="store_true")
    parser.add_argument("--require-open-source-reader", action="store_true")
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help="Optional diagnostic evidence window. Omit for full conversation-load-once scoring.",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"missing LOCOMO input: {input_path}", file=sys.stderr)
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
        "--min-hit-rate",
        str(args.min_hit_rate),
        "--min-case-count",
        str(args.min_case_count),
        "--min-reader-hit-rate",
        str(args.min_reader_hit_rate),
        "--min-token-reduction-percent",
        str(args.min_token_reduction_percent),
        "--max-retrieval-p95-ms",
        str(args.max_retrieval_p95_ms),
        "--max-reader-p95-ms",
        str(args.max_reader_p95_ms),
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
    if args.require_open_source_reader:
        command.append("--require-open-source-reader")
    if args.evidence_window is not None:
        command.extend(["--evidence-window", str(args.evidence_window)])
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    hit_rate = float(report.get("hit_rate") or 0.0)
    answer_coverage = float(report.get("answer_term_coverage") or 0.0)
    evidence_coverage = float(report.get("evidence_ref_coverage") or 0.0)
    reader_hit_rate = float(report.get("reader_hit_rate") or report.get("deterministic_reader_hit_rate") or 0.0)
    reader_answer_coverage = float(
        report.get("reader_answer_coverage") or report.get("deterministic_reader_answer_coverage") or 0.0
    )
    case_count = int(report.get("case_count") or 0)
    print(
        json.dumps(
            {
                "locomo_comparable_metric": "retrieval_context_hit_at_k",
                "mode": report.get("mode"),
                "case_count": case_count,
                "min_case_count": args.min_case_count,
                "hit_rate": hit_rate,
                "min_hit_rate": args.min_hit_rate,
                "passed": hit_rate >= args.min_hit_rate,
                "evidence_ref_coverage": evidence_coverage,
                "answer_term_coverage": answer_coverage,
                "deterministic_reader_hit_rate": reader_hit_rate,
                "deterministic_reader_answer_coverage": reader_answer_coverage,
                "reader_hit_rate": reader_hit_rate,
                "reader_answer_coverage": reader_answer_coverage,
                "reader_mode_requested": report.get("reader_mode_requested"),
                "reader_mode_effective": report.get("reader_mode_effective"),
                "reader_provider_name": report.get("reader_provider_name"),
                "reader_model": report.get("reader_model"),
                "reader_open_source_calls": report.get("reader_open_source_calls"),
                "reader_fallback_count": report.get("reader_fallback_count"),
                "reader_error_count": report.get("reader_error_count"),
                "benchmark_quality_ready": report.get("benchmark_quality_ready"),
                "benchmark_hit_at_k": report.get("benchmark_hit_at_k"),
                "benchmark_recall_at_k": report.get("benchmark_recall_at_k"),
                "benchmark_mean_reciprocal_rank": report.get("benchmark_mean_reciprocal_rank"),
                "benchmark_token_reduction_percent": report.get("benchmark_token_reduction_percent"),
                "benchmark_retrieval_p50_ms": report.get("benchmark_retrieval_p50_ms"),
                "benchmark_retrieval_p95_ms": report.get("benchmark_retrieval_p95_ms"),
                "benchmark_reader_p50_ms": report.get("benchmark_reader_p50_ms"),
                "benchmark_reader_p95_ms": report.get("benchmark_reader_p95_ms"),
                "benchmark_avg_source_tokens_per_query": report.get("benchmark_avg_source_tokens_per_query"),
                "benchmark_avg_retrieved_tokens_per_query": report.get("benchmark_avg_retrieved_tokens_per_query"),
                "benchmark_avg_retrieved_blocks_per_query": report.get("benchmark_avg_retrieved_blocks_per_query"),
                "benchmark_per_query_count": report.get("benchmark_per_query_count"),
                "benchmark_threshold_passed": report.get("benchmark_threshold_passed"),
                "benchmark_threshold_violation_count": report.get("benchmark_threshold_violation_count"),
                "benchmark_threshold_violations": report.get("benchmark_threshold_violations"),
                "benchmark_thresholds": report.get("benchmark_thresholds"),
                "answer_reader_gap_visible": answer_coverage < args.min_hit_rate,
                "report": str(report_path),
                "misses": args.misses,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if hit_rate >= args.min_hit_rate else 1


def run(
    command: list[str],
    cwd: Path,
) -> None:
    subprocess.run(command, cwd=cwd, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
