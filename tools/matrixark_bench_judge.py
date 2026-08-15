#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Claude-as-judge for the context benchmarks — the DEFAULT judge.

Why this is the default: the OSS qwen judge over-scores and self-contradicts. On an
anaphoric follow-up thread it rated answers 6-8 that Claude judged 1-3, and its per-turn
signal reversed direction turn to turn. Absolute qwen grades are not trustworthy; every
real conclusion needed Claude as judge.

There is no ANTHROPIC_API_KEY in the local benchmark environment, so "Claude judge" runs
IN-LOOP with the agent, not via API:

  1. Run a harness with `--judge claude` (the default). It generates answers with the
     reader, skips scoring, and writes a sibling `<out>.cases.json` of gradeable cases.
  2. The agent (Claude) reads the cases and scores each 0-10 against the rubric, writing
     `{case_id: score}` to `<out>.scores.json`.
  3. `python3 -m matrixark_bench_judge apply <out>.json <out>.scores.json` merges the
     scores back into the report (quality + judge="claude").

`--judge qwen` keeps the fast, noisy OSS judge. `--judge anthropic` uses the Anthropic API
when ANTHROPIC_API_KEY is set. This module is stdlib-only and shared by all the harnesses.
"""
from __future__ import annotations

import json
import sys
from typing import Any, Iterable, Optional

Json = dict[str, Any]

CLAUDE_JUDGE_RUBRIC = (
    "Score each answer 0-10 for how correctly and completely it answers the QUESTION, "
    "grounded in the REFERENCE ground truth.\n"
    "  0-2  wrong, irrelevant, hallucinated, or a raw tool/command dump.\n"
    "  3-5  partially correct but incomplete, generic, or drifting off-topic.\n"
    "  6-7  correct and mostly complete, minor gaps.\n"
    "  8-10 directly and completely answers the actual question.\n"
    "Penalize off-topic content, hallucinations, and dumps. Reward answering the real question."
)


def make_case(case_id: str, query: str, answer: str, reference: str, expected_terms: Iterable[str] = ()) -> Json:
    return {
        "case_id": case_id,
        "query": query,
        "answer": answer,
        "reference": (reference or "")[:3000],
        "expected_terms": list(expected_terms),
    }


def cases_from_arm_turns(turns: list[Json], arms: Iterable[str], *, ref_key: str = "reference") -> list[Json]:
    """Flatten a [{turn, query, reference, configs:{arm:{answer}}}] report into judge cases."""
    cases: list[Json] = []
    for t in turns:
        for arm in arms:
            c = (t.get("configs") or {}).get(arm) or {}
            if "answer" in c and c.get("answer"):
                cases.append(make_case(f"t{t['turn']}:{arm}", t.get("query", ""), c["answer"],
                                       t.get(ref_key, ""), t.get("expected_terms", ())))
    return cases


def write_cases(path: str, cases: list[Json]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"rubric": CLAUDE_JUDGE_RUBRIC, "instructions":
                   "Score each case 0-10 per the rubric. Write {case_id: score} to a scores JSON.",
                   "cases": cases}, fh, indent=2, ensure_ascii=False)


def apply_judge_scores(report: Json, scores: Json, arms: Iterable[str]) -> Json:
    """Merge {case_id -> score} into an arm-turns report; sets quality + judge='claude'."""
    arms = list(arms)
    for t in report.get("turns", []):
        for arm in arms:
            cid = f"t{t['turn']}:{arm}"
            cfg = (t.get("configs") or {}).get(arm)
            if cid in scores and cfg is not None:
                cfg["quality"] = float(scores[cid])
                cfg["judge"] = "claude"
    # refresh aggregate averages if present
    if "aggregate" in report and isinstance(report["aggregate"], dict):
        for arm in arms:
            vals = [t["configs"][arm]["quality"] for t in report.get("turns", [])
                    if (t.get("configs") or {}).get(arm) and t["configs"][arm].get("quality") is not None]
            if arm in report["aggregate"] and isinstance(report["aggregate"][arm], dict):
                report["aggregate"][arm]["avg_quality"] = round(sum(vals) / len(vals), 3) if vals else None
    report["judge"] = "claude"
    return report


def default_judge() -> str:
    """The default judge for the harnesses."""
    return "claude"


def _main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "emit":
        report = json.load(open(argv[1], encoding="utf-8"))
        arms = argv[2].split(",") if len(argv) > 2 else ["baseline", "big_budget", "relaxed"]
        cases = cases_from_arm_turns(report.get("turns", []), arms)
        json.dump({"rubric": CLAUDE_JUDGE_RUBRIC, "cases": cases}, sys.stdout, indent=2, ensure_ascii=False)
        return 0
    if len(argv) >= 3 and argv[0] == "apply":
        report = json.load(open(argv[1], encoding="utf-8"))
        scores = json.load(open(argv[2], encoding="utf-8"))
        arms = argv[3].split(",") if len(argv) > 3 else ["baseline", "big_budget", "relaxed"]
        merged = apply_judge_scores(report, scores, arms)
        out = argv[1]
        json.dump(merged, open(out, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
        print(f"[applied] {len(scores)} Claude scores -> {out}")
        return 0
    print(__doc__)
    return 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
