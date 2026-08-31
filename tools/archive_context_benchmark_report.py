#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Create a compact paper-comparable context benchmark archive report."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive paper-comparable context benchmark fields.")
    parser.add_argument("--report", required=True, help="Raw LOCOMO/LongMemEval_s benchmark JSON report.")
    parser.add_argument("--input", default="", help="Dataset artifact path, used to fill hash/bytes if absent.")
    parser.add_argument("--output", required=True, help="Paper-comparable archive JSON path.")
    parser.add_argument("--paper-name", default="ExternalBaseline/ExternalBaseline long-memory benchmark")
    parser.add_argument("--claim-level", default="paper_comparable_report_fields")
    args = parser.parse_args()

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    input_path = Path(args.input or str(report.get("input") or ""))
    dataset_hash = str(report.get("input_sha256") or "")
    input_bytes = int(report.get("input_bytes") or 0)
    if input_path.exists():
        if not dataset_hash:
            dataset_hash = sha256_file(input_path)
        if not input_bytes:
            input_bytes = input_path.stat().st_size
    thresholds = report.get("benchmark_thresholds") if isinstance(report.get("benchmark_thresholds"), dict) else {}
    case_count = int(report.get("case_count") or 0)
    min_case_count = int(thresholds.get("min_case_count") or 0)
    strict_paper_ready = (
        bool(report.get("benchmark_threshold_passed"))
        and not bool(report.get("gold_evidence_window_used"))
        and bool(dataset_hash)
        and input_bytes > 0
        and case_count >= min_case_count
        and min_case_count > 4
        and bool(thresholds.get("require_open_source_reader"))
        and bool(report.get("all_pipelines_use_rust_temporalstore"))
        and not bool(report.get("python_only_diagnostic"))
        and bool(report.get("rust_temporalstore_backend_ready"))
        and bool(report.get("rust_temporalstore_context_event_ingest_ready"))
        and not bool(report.get("rust_temporalstore_direct_source_scoring"))
        and int(report.get("rust_temporalstore_ingested_source_sets") or 0) > 0
        and int(report.get("rust_temporalstore_retrieved_source_sets") or 0) > 0
        and bool(report.get("rust_temporalstore_full_replay_ready"))
        and int(report.get("reader_open_source_calls") or 0) > 0
    )

    archived = {
        "schema": "matrixark_external_baseline_paper_comparable_report_v1",
        "report_contract_format": "matrixark_external_baseline_context_benchmark_report_v1",
        "created_at_unix": int(time.time()),
        "paper_name": args.paper_name,
        "claim_level": args.claim_level,
        "source_report": str(report_path),
        "report_path": str(report_path),
        "archive_required_fields_ready": archive_required_fields_ready(report, dataset_hash, input_bytes),
        "dataset": {
            "name": report.get("dataset"),
            "input": str(input_path) if str(input_path) else str(report.get("input") or ""),
            "sha256": dataset_hash,
            "bytes": input_bytes,
            "case_count": int(report.get("case_count") or 0),
            "conversation_count": int(report.get("conversation_count") or 0),
            "source_count": int(report.get("source_count") or 0),
            "record_counts": report.get("dataset_record_counts") or {},
        },
        "model": {
            "provider_name": report.get("reader_provider_name"),
            "reader_model": report.get("reader_model"),
            "reader_mode_requested": report.get("reader_mode_requested"),
            "reader_mode_effective": report.get("reader_mode_effective"),
            "reader_open_source_calls": int(report.get("reader_open_source_calls") or 0),
            "reader_fallback_count": int(report.get("reader_fallback_count") or 0),
            "reader_error_count": int(report.get("reader_error_count") or 0),
        },
        "rust_temporalstore_backend": {
            "all_pipelines_use_rust_temporalstore": bool(report.get("all_pipelines_use_rust_temporalstore")),
            "python_only_diagnostic": bool(report.get("python_only_diagnostic")),
            "required": bool(report.get("rust_temporalstore_backend_required")),
            "ready": bool(report.get("rust_temporalstore_backend_ready")),
            "context_event_ingest_ready": bool(report.get("rust_temporalstore_context_event_ingest_ready")),
            "direct_source_scoring": bool(report.get("rust_temporalstore_direct_source_scoring")),
            "ingested_source_sets": int(report.get("rust_temporalstore_ingested_source_sets") or 0),
            "retrieved_source_sets": int(report.get("rust_temporalstore_retrieved_source_sets") or 0),
            "full_replay_required": bool(report.get("rust_temporalstore_full_replay_required")),
            "full_replay_ready": bool(report.get("rust_temporalstore_full_replay_ready")),
            "strict_external_ready": bool(
                (report.get("rust_temporalstore_backend_report") or {}).get(
                    "rust_temporalstore_strict_external_ready"
                )
            )
            if isinstance(report.get("rust_temporalstore_backend_report"), dict)
            else False,
            "report_path": (report.get("rust_temporalstore_backend_report") or {}).get("report_path")
            if isinstance(report.get("rust_temporalstore_backend_report"), dict)
            else "",
            "converted_jsonl": (report.get("rust_temporalstore_backend_report") or {}).get("converted_jsonl")
            if isinstance(report.get("rust_temporalstore_backend_report"), dict)
            else "",
            "case_count": (
                (report.get("rust_temporalstore_backend_report") or {})
                .get("harness", {})
                .get("external_benchmark_case_count")
                if isinstance(report.get("rust_temporalstore_backend_report"), dict)
                else 0
            ),
            "score_parity": (report.get("rust_temporalstore_backend_report") or {}).get(
                "rust_vs_python_subset_score"
            )
            if isinstance(report.get("rust_temporalstore_backend_report"), dict)
            else {},
        },
        "prompt": {
            "system": report.get("reader_prompt_system") or "",
            "user_template": report.get("reader_prompt_user_template") or "",
            "max_context_chars": report.get("reader_max_context_chars"),
        },
        "thresholds": thresholds,
        "metrics": {
            "hit_at_k": float(report.get("benchmark_hit_at_k") or report.get("hit_rate") or 0.0),
            "recall_at_k": float(report.get("benchmark_recall_at_k") or report.get("hit_rate") or 0.0),
            "mean_reciprocal_rank": float(
                report.get("benchmark_mean_reciprocal_rank") or report.get("mean_reciprocal_rank") or 0.0
            ),
            "reader_hit_rate": float(report.get("reader_hit_rate") or 0.0),
            "reader_answer_coverage": float(report.get("reader_answer_coverage") or 0.0),
            "answer_term_coverage": float(report.get("answer_term_coverage") or 0.0),
            "evidence_ref_coverage": float(report.get("evidence_ref_coverage") or 0.0),
            "retrieval_p50_ms": float(report.get("benchmark_retrieval_p50_ms") or 0.0),
            "retrieval_p95_ms": float(report.get("benchmark_retrieval_p95_ms") or 0.0),
            "reader_p50_ms": float(report.get("benchmark_reader_p50_ms") or 0.0),
            "reader_p95_ms": float(report.get("benchmark_reader_p95_ms") or 0.0),
            "token_reduction_percent": float(report.get("benchmark_token_reduction_percent") or 0.0),
            "avg_source_tokens_per_query": float(report.get("benchmark_avg_source_tokens_per_query") or 0.0),
            "avg_retrieved_tokens_per_query": float(
                report.get("benchmark_avg_retrieved_tokens_per_query") or 0.0
            ),
            "avg_retrieved_blocks_per_query": float(report.get("benchmark_avg_retrieved_blocks_per_query") or 0.0),
        },
        "category_breakdown": report.get("category_breakdown") or {},
        "quality_gate": {
            "passed": bool(report.get("benchmark_threshold_passed")),
            "quality_ready": bool(report.get("benchmark_quality_ready")),
            "paper_comparable_claim_ready": strict_paper_ready,
            "violation_count": int(report.get("benchmark_threshold_violation_count") or 0),
            "violations": report.get("benchmark_threshold_violations") or [],
            "gold_evidence_window_used": bool(report.get("gold_evidence_window_used")),
        },
    }
    Path(args.output).write_text(json.dumps(archived, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(archived, indent=2, sort_keys=True))
    return 0


def archive_required_fields_ready(report: dict[str, Any], dataset_hash: str, input_bytes: int) -> bool:
    required_report_fields = (
        "case_count",
        "reader_model",
        "reader_mode_effective",
        "benchmark_thresholds",
        "benchmark_retrieval_p50_ms",
        "benchmark_retrieval_p95_ms",
        "benchmark_reader_p50_ms",
        "benchmark_reader_p95_ms",
        "benchmark_token_reduction_percent",
        "category_breakdown",
    )
    return (
        bool(dataset_hash)
        and input_bytes > 0
        and bool(report.get("input") or report.get("dataset"))
        and all(field in report for field in required_report_fields)
        and bool(report.get("all_pipelines_use_rust_temporalstore"))
        and not bool(report.get("python_only_diagnostic"))
        and bool(report.get("rust_temporalstore_backend_ready"))
        and bool(report.get("rust_temporalstore_context_event_ingest_ready"))
        and not bool(report.get("rust_temporalstore_direct_source_scoring"))
        and int(report.get("rust_temporalstore_ingested_source_sets") or 0) > 0
        and int(report.get("rust_temporalstore_retrieved_source_sets") or 0) > 0
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
