#!/usr/bin/env python3
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
    parser.add_argument("--paper-name", default="VikingMem/OpenViking long-memory benchmark")
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

    archived = {
        "schema": "matrixark_vikingmem_paper_comparable_report_v1",
        "created_at_unix": int(time.time()),
        "paper_name": args.paper_name,
        "claim_level": args.claim_level,
        "source_report": str(report_path),
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
            "required": bool(report.get("rust_temporalstore_backend_required")),
            "ready": bool(report.get("rust_temporalstore_backend_ready")),
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
        },
        "prompt": {
            "system": report.get("reader_prompt_system") or "",
            "user_template": report.get("reader_prompt_user_template") or "",
            "max_context_chars": report.get("reader_max_context_chars"),
        },
        "thresholds": report.get("benchmark_thresholds") or {},
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
            "violation_count": int(report.get("benchmark_threshold_violation_count") or 0),
            "violations": report.get("benchmark_threshold_violations") or [],
            "gold_evidence_window_used": bool(report.get("gold_evidence_window_used")),
        },
    }
    Path(args.output).write_text(json.dumps(archived, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(archived, indent=2, sort_keys=True))
    return 0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
