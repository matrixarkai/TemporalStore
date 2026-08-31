#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate the Context benchmark truth contract wiring.

This is deliberately static and fast. It ensures the benchmark path says what it
can honestly prove before deeper parity execution is enabled: archive-level
truth mode first, shared report contract second, native execution third.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"


def main() -> int:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    full_case = next(
        (case for case in corpus["cases"] if case.get("name") == "context_benchmark_full_dataset_gates"),
        None,
    )
    if full_case is None:
        raise SystemExit("missing context_benchmark_full_dataset_gates")
    command = full_case["steps"][0]["command"]
    require_path(command, "tools/compare_context_benchmark_archives.py")
    require_path(command, "compat/native_context_benchmark_report_adapter.h")
    if command.get("mode") != "shared_benchmark_contract":
        raise SystemExit("context_benchmark_full_dataset_gates must stay a shared benchmark contract")
    if "Production evidence requires real mounted artifacts" not in command.get("description", ""):
        raise SystemExit("full benchmark description must keep production evidence caveat")

    require_snippets(
        ROOT / "tools" / "compare_context_benchmark_archives.py",
        (
            "--truth-mode",
            "benchmark_truth_ready",
            "truth_blockers",
            '"production"',
            "require_executed",
            "report comparison failed",
            "summary_compare",
            "category_compare",
            "token_reduction_delta",
            "latency_deltas",
        ),
    )
    require_snippets(
        ROOT / "tools" / "compare_context_benchmark_reports.py",
        (
            "report_pair_summary",
            "misses_by_category",
            "field_mismatches_by_query",
            "reader_shared_hard_miss_count",
        ),
    )
    require_snippets(
        ROOT / "tools" / "validate_context_benchmark_comparator.py",
        (
            "context benchmark comparator self-test passed",
            "native_only_miss_count",
            "field_mismatches_by_query",
        ),
    )
    require_snippets(
        ROOT / "tools" / "archive_context_benchmark_report.py",
        (
            "matrixark_external_baseline_context_benchmark_report_v1",
            "archive_required_fields_ready",
            "benchmark_retrieval_p50_ms",
            "benchmark_retrieval_p95_ms",
            "benchmark_token_reduction_percent",
            "category_breakdown",
        ),
    )
    require_snippets(
        ROOT / "compat" / "native_context_benchmark_report_adapter.h",
        (
            "matrixark_external_baseline_context_benchmark_report_v1",
            "FinalizeReport",
            "FinalizedJson",
            "input_sha256",
            "retrieved_source_group_ids",
            "benchmark_avg_retrieved_source_groups_per_query",
        ),
    )
    require_snippets(
        ROOT / "docs" / "native_context_benchmark_report_adapter.md",
        (
            "--truth-mode production",
            "benchmark_truth_ready",
            "truth_blockers",
            "benchmark truth first",
            "unified report contract next",
            "deeper parity execution",
        ),
    )
    require_snippets(
        ROOT / "docs" / "benchmark_reproducibility_evidence.md",
        (
            "does not claim production parity",
            "live OSS reader path",
            "real LongMemEval_s artifact",
        ),
    )
    print("context_benchmark_truth_contract=true")
    print("order=benchmark_truth,unified_report_contract,deeper_parity_execution")
    return 0


def require_path(command: dict, required_path: str) -> None:
    paths = command.get("required_paths")
    if not isinstance(paths, list) or required_path not in paths:
        raise SystemExit(f"context benchmark contract missing required path {required_path}")


def require_snippets(path: Path, snippets: tuple[str, ...]) -> None:
    text = path.read_text(encoding="utf-8", errors="ignore")
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise SystemExit(f"{path.relative_to(ROOT)} missing snippets: {', '.join(missing)}")


if __name__ == "__main__":
    raise SystemExit(main())
