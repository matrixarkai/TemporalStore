#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run the LOCOMO retrieval/context hit-rate gate used for ExternalBaseline parity.

This intentionally gates the metric comparable to the MatrixArk path's
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

from benchmark_threshold_policy import add_threshold_policy_args, resolve_threshold_policy


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LOCOMO 90%+ retrieval/context hit-rate gate.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO JSON export path.")
    parser.add_argument(
        "--report",
        default="/tmp/temporalstore_locomo_ingest_once_result.json",
        help="Harness JSON report path.",
    )
    parser.add_argument("--misses", default="/tmp/temporalstore_locomo_ingest_once_misses.jsonl")
    add_threshold_policy_args(parser)
    parser.add_argument("--max-events", type=int, default=128)
    parser.add_argument("--adaptive-max-events", action="store_true")
    parser.add_argument("--adaptive-base-max-events", type=int, default=128)
    parser.add_argument(
        "--question-limit",
        type=int,
        default=0,
        help="Optional maximum number of supported questions to score. Diagnostic/slice reports only.",
    )
    parser.add_argument(
        "--question-offset",
        type=int,
        default=0,
        help="Optional number of supported questions to skip before scoring. Diagnostic/slice reports only.",
    )
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
    parser.add_argument("--reuse-rust-temporalstore-report", action="store_true")
    parser.add_argument(
        "--require-full-rust-temporalstore-replay",
        action="store_true",
        help="Require every converted LOCOMO case and all sources to run through Rust TemporalStore.",
    )
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help="Optional diagnostic evidence window. Omit for full conversation-load-once scoring.",
    )
    args = parser.parse_args()
    if not args.baseline_provider_name:
        args.baseline_provider_name = args.reader_provider_name
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
            "Rust TemporalStore backend is required for LOCOMO benchmark evidence; "
            "use --allow-python-only-diagnostic with --skip-rust-temporalstore only for local debugging.",
            file=sys.stderr,
        )
        return 2
    thresholds = resolve_threshold_policy(args)
    if args.evidence_window is not None and args.threshold_profile in {"locomo_full", "oss_reader_full"}:
        print(
            "--evidence-window uses gold evidence refs and is diagnostic-only; "
            f"it is not allowed with production threshold profile {args.threshold_profile}.",
            file=sys.stderr,
        )
        return 2

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
        "--adaptive-base-max-events",
        str(args.adaptive_base_max_events),
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
    if args.baseline_judge_model:
        command.extend(["--baseline-judge-model", args.baseline_judge_model])
    if args.baseline_judge_prompt:
        command.extend(["--baseline-judge-prompt", args.baseline_judge_prompt])
    if args.require_shared_oss_models:
        command.append("--require-shared-oss-models")
    if args.adaptive_max_events:
        command.append("--adaptive-max-events")
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
        if args.reuse_rust_temporalstore_report:
            command.append("--reuse-rust-temporalstore-report")
    elif args.allow_python_only_diagnostic:
        command.extend(["--skip-rust-temporalstore", "--allow-python-only-diagnostic"])
    if thresholds["require_open_source_reader"]:
        command.append("--require-open-source-reader")
    if args.evidence_window is not None:
        command.extend(["--evidence-window", str(args.evidence_window)])
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rust_backend_report = report.get("rust_temporalstore_backend_report") or {}
    rust_score_parity = {}
    if isinstance(rust_backend_report, dict):
        rust_score_parity = rust_backend_report.get("rust_vs_python_subset_score") or {}
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
                "threshold_profile": args.threshold_profile,
                "case_count": case_count,
                "min_case_count": thresholds["min_case_count"],
                "hit_rate": hit_rate,
                "min_hit_rate": thresholds["min_hit_rate"],
                "passed": hit_rate >= thresholds["min_hit_rate"],
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
                "gold_evidence_window_used": bool(report.get("gold_evidence_window_used")),
                "evidence_window": report.get("evidence_window"),
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
                "all_pipelines_use_rust_temporalstore": bool(
                    report.get("all_pipelines_use_rust_temporalstore")
                ),
                "python_only_diagnostic": bool(report.get("python_only_diagnostic")),
                "rust_temporalstore_backend_required": bool(report.get("rust_temporalstore_backend_required")),
                "rust_temporalstore_backend_ready": bool(report.get("rust_temporalstore_backend_ready")),
                "rust_temporalstore_context_event_ingest_ready": bool(
                    report.get("rust_temporalstore_context_event_ingest_ready")
                ),
                "rust_temporalstore_direct_source_scoring": bool(
                    report.get("rust_temporalstore_direct_source_scoring")
                ),
                "rust_temporalstore_ingested_source_sets": report.get("rust_temporalstore_ingested_source_sets"),
                "rust_temporalstore_retrieved_source_sets": report.get("rust_temporalstore_retrieved_source_sets"),
                "rust_temporalstore_full_replay_required": bool(
                    report.get("rust_temporalstore_full_replay_required")
                ),
                "rust_temporalstore_full_replay_ready": bool(report.get("rust_temporalstore_full_replay_ready")),
                "rust_temporalstore_converted_jsonl": rust_backend_report.get("converted_jsonl")
                if isinstance(rust_backend_report, dict)
                else None,
                "rust_temporalstore_report": rust_backend_report.get("report_path")
                if isinstance(rust_backend_report, dict)
                else None,
                "rust_temporalstore_score_parity": rust_score_parity,
                "paper_comparable_claim_ready": bool(report.get("paper_comparable_claim_ready")),
                "answer_reader_gap_visible": answer_coverage < thresholds["min_hit_rate"],
                "report": str(report_path),
                "misses": args.misses,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if bool(report.get("benchmark_threshold_passed")) else 1


def run(
    command: list[str],
    cwd: Path,
) -> None:
    subprocess.run(command, cwd=cwd, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
