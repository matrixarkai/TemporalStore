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
    parser.add_argument("--max-events", type=int, default=128)
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
        "--max-events",
        str(args.max_events),
    ]
    if args.evidence_window is not None:
        command.extend(["--evidence-window", str(args.evidence_window)])
    run(command, cwd=repo)

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    hit_rate = float(report.get("hit_rate") or 0.0)
    answer_coverage = float(report.get("answer_term_coverage") or 0.0)
    evidence_coverage = float(report.get("evidence_ref_coverage") or 0.0)
    reader_hit_rate = float(report.get("deterministic_reader_hit_rate") or 0.0)
    reader_answer_coverage = float(report.get("deterministic_reader_answer_coverage") or 0.0)
    case_count = int(report.get("case_count") or 0)
    print(
        json.dumps(
            {
                "locomo_comparable_metric": "retrieval_context_hit_at_k",
                "mode": report.get("mode"),
                "case_count": case_count,
                "hit_rate": hit_rate,
                "min_hit_rate": args.min_hit_rate,
                "passed": hit_rate >= args.min_hit_rate,
                "evidence_ref_coverage": evidence_coverage,
                "answer_term_coverage": answer_coverage,
                "deterministic_reader_hit_rate": reader_hit_rate,
                "deterministic_reader_answer_coverage": reader_answer_coverage,
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
