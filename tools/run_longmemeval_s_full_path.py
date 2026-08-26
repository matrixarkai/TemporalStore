#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
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
    parser.add_argument("--question-limit", type=int, default=0)
    parser.add_argument("--question-offset", type=int, default=0)
    parser.add_argument("--embedding-model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--baseline-provider-name", default="")
    parser.add_argument("--baseline-reader-model", default="")
    parser.add_argument("--baseline-embedding-model", default="")
    parser.add_argument("--baseline-max-events", type=int, default=0)
    parser.add_argument("--baseline-reader-max-context-chars", type=int, default=0)
    parser.add_argument("--judge-model", default="")
    parser.add_argument("--judge-prompt", default="")
    parser.add_argument("--baseline-judge-model", default="")
    parser.add_argument("--baseline-judge-prompt", default="")
    parser.add_argument(
        "--require-shared-oss-models",
        dest="require_shared_oss_models",
        action="store_true",
        default=True,
        help="Require MatrixArk and ExternalBaseline/ExternalBaseline to use the same OSS reader, encoder, cap, and context budget.",
    )
    parser.add_argument(
        "--allow-shared-oss-model-drift",
        action="store_true",
        help="Diagnostic-only escape hatch for intentionally unfair local model/budget experiments.",
    )
    parser.add_argument("--reader-mode", choices=("deterministic", "open-source", "auto"), default="deterministic")
    parser.add_argument("--reader-provider-name", default="external_baseline-gpt-4o-mini-reader")
    parser.add_argument("--reader-model", default="gpt-4o-mini")
    parser.add_argument("--reader-base-url", default="")
    parser.add_argument("--reader-api-key-env", default="OPENAI_API_KEY")
    parser.add_argument("--reader-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument("--retrieval-same-session-percent", type=float, default=0.70)
    parser.add_argument("--retrieval-cross-session-percent", type=float, default=0.45)
    parser.add_argument("--retrieval-summary-percent", type=float, default=0.25)
    parser.add_argument("--retrieval-entity-percent", type=float, default=0.35)
    parser.add_argument("--retrieval-event-percent", type=float, default=0.80)
    parser.add_argument(
        "--reader-include-extractive-hint",
        action="store_true",
        help="Forward the scorer's retrieved-context-derived answer hint into the OSS reader prompt.",
    )
    parser.add_argument(
        "--reader-focus-evidence",
        action="store_true",
        help="Forward only the most question-relevant sentence/span from each retrieved block.",
    )
    parser.add_argument(
        "--reader-candidate-first",
        action="store_true",
        help="Put the retrieved-context-derived answer candidate first in the OSS reader context.",
    )
    parser.add_argument(
        "--reader-candidate-only",
        action="store_true",
        help="Use only the retrieved-context-derived answer candidate as OSS reader context when available.",
    )
    parser.add_argument(
        "--reader-candidate-hybrid",
        action="store_true",
        help=(
            "Send candidate-first focused evidence to the OSS reader, then fall back to the extracted "
            "candidate only when the reader returns an empty, insufficient, or obviously noisy span."
        ),
    )
    parser.add_argument("--reader-no-fallback", action="store_true")
    parser.add_argument(
        "--require-rust-temporalstore",
        action="store_true",
        default=True,
        help="Require the Rust TemporalStore context backend proof. Enabled by default.",
    )
    parser.add_argument(
        "--skip-rust-temporalstore",
        dest="require_rust_temporalstore",
        action="store_false",
        help="Diagnostic-only Python scorer run; requires --allow-python-only-diagnostic.",
    )
    parser.add_argument(
        "--allow-python-only-diagnostic",
        action="store_true",
        help="Permit --skip-rust-temporalstore for local debugging. Reports from this mode are not benchmark evidence.",
    )
    parser.add_argument("--rust-temporalstore-max-cases", type=int, default=4)
    parser.add_argument("--rust-temporalstore-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--rust-temporalstore-source-limit", type=int, default=64)
    parser.add_argument("--rust-temporalstore-batch-size", type=int, default=0)
    parser.add_argument("--rust-temporalstore-source-pack-size", type=int, default=32)
    parser.add_argument("--rust-temporalstore-release", action="store_true")
    parser.add_argument("--rust-temporalstore-score-tolerance", type=float, default=0.0)
    parser.add_argument("--rust-temporalstore-jsonl", default="")
    parser.add_argument("--rust-temporalstore-report", default="")
    parser.add_argument(
        "--require-full-rust-temporalstore-replay",
        action="store_true",
        help="Require every converted LongMemEval_s case and all sources to run through Rust TemporalStore.",
    )
    args = parser.parse_args()
    if not args.baseline_reader_model:
        args.baseline_reader_model = args.reader_model
    if not args.baseline_embedding_model:
        args.baseline_embedding_model = args.embedding_model
    if not args.baseline_max_events:
        args.baseline_max_events = args.max_events
    if not args.baseline_reader_max_context_chars:
        args.baseline_reader_max_context_chars = args.reader_max_context_chars
    if not args.baseline_judge_model:
        args.baseline_judge_model = args.judge_model
    if not args.baseline_judge_prompt:
        args.baseline_judge_prompt = args.judge_prompt
    if not args.allow_shared_oss_model_drift and (
        args.baseline_provider_name
        or args.baseline_reader_model
        or args.baseline_embedding_model
        or args.baseline_max_events
        or args.baseline_reader_max_context_chars
    ):
        args.require_shared_oss_models = True
    if not args.require_rust_temporalstore and not args.allow_python_only_diagnostic:
        print(
            "Rust TemporalStore backend is required for LongMemEval_s benchmark evidence; "
            "use --allow-python-only-diagnostic with --skip-rust-temporalstore only for local debugging.",
            file=sys.stderr,
        )
        return 2
    thresholds = resolve_threshold_policy(args)
    full_min_case_count = int(thresholds["min_case_count"])
    effective_min_case_count = full_min_case_count
    if args.question_limit > 0:
        effective_min_case_count = min(full_min_case_count, args.question_limit)

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
        str(effective_min_case_count),
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
        "--question-limit",
        str(args.question_limit),
        "--question-offset",
        str(args.question_offset),
        "--embedding-model",
        args.embedding_model,
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
        "--retrieval-same-session-percent",
        str(args.retrieval_same_session_percent),
        "--retrieval-cross-session-percent",
        str(args.retrieval_cross_session_percent),
        "--retrieval-summary-percent",
        str(args.retrieval_summary_percent),
        "--retrieval-entity-percent",
        str(args.retrieval_entity_percent),
        "--retrieval-event-percent",
        str(args.retrieval_event_percent),
    ]
    if args.baseline_provider_name:
        command.extend(["--baseline-provider-name", args.baseline_provider_name])
    if args.baseline_reader_model:
        command.extend(["--baseline-reader-model", args.baseline_reader_model])
    if args.baseline_embedding_model:
        command.extend(["--baseline-embedding-model", args.baseline_embedding_model])
    if args.baseline_max_events:
        command.extend(["--baseline-max-events", str(args.baseline_max_events)])
    if args.baseline_reader_max_context_chars:
        command.extend(["--baseline-reader-max-context-chars", str(args.baseline_reader_max_context_chars)])
    if args.require_shared_oss_models:
        command.append("--require-shared-oss-models")
    if args.allow_shared_oss_model_drift:
        command.append("--allow-shared-oss-model-drift")
    if args.reader_base_url:
        command.extend(["--reader-base-url", args.reader_base_url])
    if args.reader_include_extractive_hint:
        command.append("--reader-include-extractive-hint")
    if args.reader_focus_evidence:
        command.append("--reader-focus-evidence")
    if args.reader_candidate_first:
        command.append("--reader-candidate-first")
    if args.reader_candidate_only:
        command.append("--reader-candidate-only")
    if args.reader_candidate_hybrid:
        command.append("--reader-candidate-hybrid")
    if args.reader_no_fallback:
        command.append("--reader-no-fallback")
    if args.require_rust_temporalstore:
        rust_report_path = args.rust_temporalstore_report or str(Path(args.report).with_suffix(".rust_temporalstore.json"))
        command.append("--require-rust-temporalstore")
        command.extend(["--rust-temporalstore-max-cases", str(args.rust_temporalstore_max_cases)])
        command.extend(["--rust-temporalstore-timeout-seconds", str(args.rust_temporalstore_timeout_seconds)])
        command.extend(["--rust-temporalstore-source-limit", str(args.rust_temporalstore_source_limit)])
        command.extend(["--rust-temporalstore-batch-size", str(args.rust_temporalstore_batch_size)])
        command.extend(["--rust-temporalstore-source-pack-size", str(args.rust_temporalstore_source_pack_size)])
        command.extend(["--rust-temporalstore-score-tolerance", str(args.rust_temporalstore_score_tolerance)])
        if args.rust_temporalstore_release:
            command.append("--rust-temporalstore-release")
        if args.require_full_rust_temporalstore_replay:
            command.append("--require-full-rust-temporalstore-replay")
        if args.rust_temporalstore_jsonl:
            command.extend(["--rust-temporalstore-jsonl", args.rust_temporalstore_jsonl])
        command.extend(["--rust-temporalstore-report", rust_report_path])
    elif args.allow_python_only_diagnostic:
        command.extend(["--skip-rust-temporalstore", "--allow-python-only-diagnostic"])
    if thresholds["require_open_source_reader"]:
        command.append("--require-open-source-reader")
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rust_backend_report = report.get("rust_temporalstore_backend_report") or {}
    rust_score_parity = {}
    if isinstance(rust_backend_report, dict):
        rust_score_parity = rust_backend_report.get("rust_vs_python_subset_score") or {}
    case_count = int(report.get("case_count") or 0)
    hit_rate = float(report.get("hit_rate") or 0.0)
    output = {
        "longmemeval_s_full_path_ready": hit_rate >= thresholds["min_hit_rate"],
        "dataset": report.get("dataset"),
        "mode": report.get("mode"),
        "threshold_profile": args.threshold_profile,
        "case_count": case_count,
        "min_case_count": effective_min_case_count,
        "full_run_min_case_count": full_min_case_count,
        "bounded_question_limit": int(args.question_limit or 0),
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
        "all_pipelines_use_rust_temporalstore": bool(report.get("all_pipelines_use_rust_temporalstore")),
        "python_only_diagnostic": bool(report.get("python_only_diagnostic")),
        "rust_temporalstore_backend_required": bool(report.get("rust_temporalstore_backend_required")),
        "rust_temporalstore_backend_ready": bool(report.get("rust_temporalstore_backend_ready")),
        "rust_temporalstore_context_event_ingest_ready": bool(
            report.get("rust_temporalstore_context_event_ingest_ready")
        ),
        "rust_temporalstore_direct_source_scoring": bool(report.get("rust_temporalstore_direct_source_scoring")),
        "rust_temporalstore_ingested_source_sets": report.get("rust_temporalstore_ingested_source_sets"),
        "rust_temporalstore_retrieved_source_sets": report.get("rust_temporalstore_retrieved_source_sets"),
        "rust_temporalstore_full_replay_required": bool(report.get("rust_temporalstore_full_replay_required")),
        "rust_temporalstore_full_replay_ready": bool(report.get("rust_temporalstore_full_replay_ready")),
        "rust_temporalstore_converted_jsonl": rust_backend_report.get("converted_jsonl")
        if isinstance(rust_backend_report, dict)
        else None,
        "rust_temporalstore_report": rust_backend_report.get("report_path") if isinstance(rust_backend_report, dict) else None,
        "rust_temporalstore_score_parity": rust_score_parity,
        "paper_comparable_claim_ready": bool(report.get("paper_comparable_claim_ready")),
        "category_breakdown": report.get("category_breakdown") or {},
        "report": str(report_path),
        "misses": args.misses,
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["longmemeval_s_full_path_ready"] and output["benchmark_threshold_passed"] else 1


def run(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
