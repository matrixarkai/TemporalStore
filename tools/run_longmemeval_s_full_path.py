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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LongMemEval_s full-path retrieval/reader scoring.")
    parser.add_argument("--input", default="/tmp/longmemeval_s.json", help="LongMemEval_s JSON export path.")
    parser.add_argument(
        "--report",
        default="/tmp/temporalstore_longmemeval_s_full_path_result.json",
        help="Harness JSON report path.",
    )
    parser.add_argument("--misses", default="/tmp/temporalstore_longmemeval_s_full_path_misses.jsonl")
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--max-events", type=int, default=256)
    args = parser.parse_args()

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
        str(args.min_hit_rate),
        "--max-events",
        str(args.max_events),
    ]
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    case_count = int(report.get("case_count") or 0)
    hit_rate = float(report.get("hit_rate") or 0.0)
    output = {
        "longmemeval_s_full_path_ready": hit_rate >= args.min_hit_rate,
        "dataset": report.get("dataset"),
        "mode": report.get("mode"),
        "case_count": case_count,
        "conversation_count": int(report.get("conversation_count") or 0),
        "source_count": int(report.get("source_count") or 0),
        "hit_rate": hit_rate,
        "min_hit_rate": args.min_hit_rate,
        "mean_reciprocal_rank": float(report.get("mean_reciprocal_rank") or 0.0),
        "answer_term_coverage": float(report.get("answer_term_coverage") or 0.0),
        "deterministic_reader_hit_rate": float(report.get("deterministic_reader_hit_rate") or 0.0),
        "deterministic_reader_answer_coverage": float(report.get("deterministic_reader_answer_coverage") or 0.0),
        "zero_hit_queries": int(report.get("zero_hit_queries") or 0),
        "reader_zero_hit_queries": int(report.get("reader_zero_hit_queries") or 0),
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
