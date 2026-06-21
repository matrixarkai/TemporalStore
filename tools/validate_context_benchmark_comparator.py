#!/usr/bin/env python3
"""Self-test the shared C++/Rust context benchmark report comparator."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPARATOR = ROOT / "tools" / "compare_context_benchmark_reports.py"


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="ts-context-report-compare-") as tmp:
        tmp_path = Path(tmp)
        rust = sample_report()
        cpp = sample_report()
        rust_path = tmp_path / "rust.json"
        cpp_path = tmp_path / "cpp.json"
        out_path = tmp_path / "compare.json"
        write_json(rust_path, rust)
        write_json(cpp_path, cpp)
        result = run_compare(rust_path, cpp_path, out_path)
        if result.returncode != 0:
            raise SystemExit(f"identical report comparison should pass:\n{result.stdout}\n{result.stderr}")
        passed = json.loads(out_path.read_text())
        if not passed.get("ready"):
            raise SystemExit("identical report comparison did not produce ready=true")

        cpp["benchmark_per_query"][1]["hit"] = False
        cpp["benchmark_per_query"][1]["rank"] = None
        cpp["benchmark_hit_at_k"] = 0.5
        cpp["hit_rate"] = 0.5
        cpp["benchmark_mean_reciprocal_rank"] = 0.5
        cpp["mean_reciprocal_rank"] = 0.5
        cpp["category_breakdown"]["temporal"]["hit_rate"] = 0.0
        cpp["category_breakdown"]["temporal"]["mean_reciprocal_rank"] = 0.0
        write_json(cpp_path, cpp)
        result = run_compare(rust_path, cpp_path, out_path)
        if result.returncode == 0:
            raise SystemExit("mismatched report comparison should fail")
        failed = json.loads(out_path.read_text())
        if failed.get("cpp_only_miss_count") != 1:
            raise SystemExit(f"expected cpp_only_miss_count=1, got {failed.get('cpp_only_miss_count')}")
        if failed.get("misses_by_category", {}).get("retrieval", {}).get("temporal", {}).get("cpp_only") != 1:
            raise SystemExit("expected temporal cpp_only miss taxonomy")
        if "q2" not in failed.get("field_mismatches_by_query", {}):
            raise SystemExit("expected field_mismatches_by_query to include q2")
    print("context benchmark comparator self-test passed")
    return 0


def run_compare(rust_path: Path, cpp_path: Path, out_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(COMPARATOR),
            "--rust-report",
            str(rust_path),
            "--cpp-report",
            str(cpp_path),
            "--case-name",
            "context_benchmark_full_dataset_gates",
            "--dataset",
            "locomo",
            "--output",
            str(out_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sample_report() -> dict:
    rows = [
        row("q1", "single_hop", True, True, 1, ["src-1"]),
        row("q2", "temporal", True, True, 1, ["src-2"]),
    ]
    return {
        "schema": "matrixark_vikingmem_context_benchmark_report_v1",
        "benchmark_family": "vikingmem_long_memory",
        "dataset": "locomo",
        "mode": "conversation_load_once_query_many",
        "input": "/tmp/locomo10.json",
        "input_sha256": "a" * 64,
        "input_bytes": 1024,
        "case_count": 2,
        "conversation_count": 1,
        "source_count": 2,
        "hit_rate": 1.0,
        "benchmark_hit_at_k": 1.0,
        "benchmark_recall_at_k": 1.0,
        "mean_reciprocal_rank": 1.0,
        "benchmark_mean_reciprocal_rank": 1.0,
        "answer_term_coverage": 1.0,
        "evidence_ref_coverage": 1.0,
        "reader_hit_rate": 1.0,
        "reader_answer_coverage": 1.0,
        "reader_mode_requested": "deterministic",
        "reader_mode_effective": "deterministic",
        "reader_provider_name": "matrixark-cpp-oss-context",
        "reader_model": "google/flan-t5-small",
        "reader_open_source_calls": 0,
        "reader_fallback_count": 0,
        "reader_error_count": 0,
        "reader_last_error": "",
        "zero_hit_queries": 0,
        "reader_zero_hit_queries": 0,
        "benchmark_quality_ready": True,
        "benchmark_threshold_passed": True,
        "paper_comparable_claim_ready": False,
        "rust_temporalstore_full_replay_ready": False,
        "benchmark_threshold_violation_count": 0,
        "benchmark_threshold_violations": [],
        "benchmark_thresholds": {
            "min_case_count": 2,
            "min_hit_at_k": 0.9,
            "min_reader_hit_rate": 0.0,
            "min_token_reduction_percent": 0.0,
            "max_retrieval_p95_ms": 1000.0,
            "max_reader_p95_ms": 30000.0,
            "require_open_source_reader": False,
        },
        "category_breakdown": {
            "single_hop": category(1, 1.0),
            "temporal": category(1, 1.0),
        },
        "weak_category_count": 0,
        "weak_categories": [],
        "weak_category_policy": {},
        "benchmark_per_query_count": len(rows),
        "benchmark_per_query": rows,
        "benchmark_retrieval_p50_ms": 5.0,
        "benchmark_retrieval_p95_ms": 7.0,
        "benchmark_reader_p50_ms": 2.0,
        "benchmark_reader_p95_ms": 3.0,
        "benchmark_avg_retrieved_blocks_per_query": 1.0,
        "benchmark_avg_retrieved_source_groups_per_query": 1.0,
        "benchmark_multi_source_group_query_rate": 0.0,
        "benchmark_avg_source_tokens_per_query": 100.0,
        "benchmark_avg_retrieved_tokens_per_query": 20.0,
        "benchmark_max_retrieved_tokens_per_query": 20.0,
        "benchmark_token_reduction_percent": 80.0,
    }


def row(
    query_id: str,
    category_name: str,
    hit: bool,
    reader_hit: bool,
    rank: int | None,
    source_ids: list[str],
) -> dict:
    return {
        "query_id": query_id,
        "category": category_name,
        "hit": hit,
        "rank": rank,
        "reader_hit": reader_hit,
        "reader_answer": "answer",
        "matched_answer_terms": 1,
        "answer_terms": 1,
        "expected_answer_terms": ["answer"],
        "matched_retrieval_answer_terms": 1,
        "expected_source_refs": 1,
        "expected_source_ref_ids": source_ids,
        "matched_source_refs": 1,
        "retrieved_blocks": 1,
        "retrieved_source_ids": source_ids,
        "retrieved_source_groups": 1,
        "retrieved_source_group_ids": [source_ids[0].split("-")[0]],
        "source_tokens": 100,
        "retrieved_tokens": 20,
        "token_reduction_percent": 80.0,
        "retrieval_ms": 5.0,
        "reader_ms": 2.0,
    }


def category(count: int, hit_rate: float) -> dict:
    return {
        "case_count": count,
        "hit_rate": hit_rate,
        "mean_reciprocal_rank": hit_rate,
        "answer_term_coverage": hit_rate,
        "zero_hit_queries": 0 if hit_rate else count,
        "reader_hit_rate": hit_rate,
        "reader_answer_coverage": hit_rate,
    }


if __name__ == "__main__":
    raise SystemExit(main())
