#!/usr/bin/env python3
"""Run the LOCOMO retrieval/context hit-rate gate used for VikingMem parity.

This intentionally gates the metric comparable to the MatrixArk/C++ path's
"retrieval/context hit" number. It also prints answer-term coverage so reader
accuracy gaps stay visible instead of being folded into retrieval scoring.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LOCOMO 90%+ retrieval/context hit-rate gate.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO JSON export path.")
    parser.add_argument(
        "--jsonl",
        default="/tmp/temporalstore_locomo_all_evidence_window.jsonl",
        help="Converted benchmark JSONL output path.",
    )
    parser.add_argument(
        "--report",
        default="/tmp/temporalstore_locomo_all_direct_result.json",
        help="Harness JSON report path.",
    )
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--evidence-window", type=int, default=3)
    parser.add_argument("--max-events", type=int, default=64)
    parser.add_argument(
        "--harness",
        default="target/debug/context_workflow_harness",
        help="Path to built context_workflow_harness binary.",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    input_path = Path(args.input)
    harness_path = repo / args.harness
    if not input_path.exists():
        print(f"missing LOCOMO input: {input_path}", file=sys.stderr)
        return 2
    if not harness_path.exists():
        print(
            f"missing harness binary: {harness_path}; "
            "run cargo build -p temporalstore-rust --bin context_workflow_harness",
            file=sys.stderr,
        )
        return 2

    run(
        [
            sys.executable,
            str(repo / "tools" / "convert_locomo_to_context_jsonl.py"),
            str(input_path),
            args.jsonl,
            "--evidence-window",
            str(args.evidence_window),
        ],
        cwd=repo,
    )

    env = os.environ.copy()
    env.update(
        {
            "TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY": "1",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL": args.jsonl,
            "TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY": "1",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS": str(args.max_events),
            "TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING": "1",
        }
    )
    report_path = Path(args.report)
    with report_path.open("w", encoding="utf-8") as handle:
        run([str(harness_path)], cwd=repo, env=env, stdout=handle)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    hit_rate = float(report.get("external_benchmark_hit_at_k") or 0.0)
    answer_coverage = float(report.get("external_benchmark_answer_term_coverage") or 0.0)
    evidence_coverage = float(report.get("external_benchmark_evidence_ref_coverage") or 0.0)
    case_count = int(report.get("external_benchmark_case_count") or 0)
    print(
        json.dumps(
            {
                "locomo_comparable_metric": "retrieval_context_hit_at_k",
                "case_count": case_count,
                "hit_rate": hit_rate,
                "min_hit_rate": args.min_hit_rate,
                "passed": hit_rate >= args.min_hit_rate,
                "evidence_ref_coverage": evidence_coverage,
                "answer_term_coverage": answer_coverage,
                "answer_reader_gap_visible": answer_coverage < args.min_hit_rate,
                "report": str(report_path),
                "jsonl": args.jsonl,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if hit_rate >= args.min_hit_rate else 1


def run(
    command: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
    stdout: object | None = None,
) -> None:
    subprocess.run(command, cwd=cwd, env=env, stdout=stdout, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
