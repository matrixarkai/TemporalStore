#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Is the relevance-score floor actually filtering usable context, or is it inert?

Hypothesis: at realistic budgets the floor is INERT — the budget fills with score>0
candidates long before the score<=0 (floor-dropped) tail would be reached, so relaxing
the floor changes nothing. This proves/disproves it deterministically (no LLM): for each
query and budget, build the pack with a STRICT floor (min_score 0) vs a RELAXED floor
(min_score -1, admit score<=0), and check whether the packs are identical.
"""
from __future__ import annotations
import json
from pathlib import Path
import run_local_context_token_quality_sweep as S

QUERIES = [
    "What did we decide about real-time Codex/Claude prompt ingestion via hooks?",
    "Why did that matter for token savings?",                       # anaphoric follow-up
    "What did the 3-arm token and quality benchmark show?",
]
BUDGETS = [3000, 8000, 30000, 100000, 300000]


def main() -> int:
    roles = {"user", "assistant"}
    claude = S.iter_claude_events(Path("/mnt/c/Users/Deeproute/.claude/projects"), 200, 400, roles)
    codex = S.iter_codex_events(Path("/mnt/c/Users/Deeproute/.codex/sessions"), 200, 400, roles)
    events = sorted(claude + codex, key=lambda e: e.timestamp_ms)
    segments = S.build_segments(events, 10)
    entities = S.build_entities(events, 18)
    summaries = S.build_summaries(events, entities)
    total = len(events) + len(segments) + len(entities) + len(summaries)
    print("corpus candidates:", total)

    def ref_ids(pack):
        return [(r["ref_type"], r["ref_id"]) for r in pack["selected_refs"]]

    report = {"corpus_candidates": total, "queries": []}
    for q in QUERIES:
        qterms = set(S.keywords(q))
        pos = sum(1 for rank, e in enumerate(reversed(events)) if S.score_text(qterms, e.text, rank) > 0) \
            + sum(1 for rank, s in enumerate(reversed(segments)) if S.score_text(qterms, s.text, rank) > 0) \
            + sum(1 for rank, en in enumerate(entities) if S.score_text(qterms, en.name + " " + en.state, rank) > 0) \
            + sum(1 for rank, sm in enumerate(summaries) if S.score_text(qterms, sm.text, rank) > 0)
        print("\nQUERY:", q[:60])
        print("  score>0 candidates (events+segments approx):", pos)
        rows = []
        for b in BUDGETS:
            strict = S.retrieve_pack(q, events, segments, entities, summaries, b, current_session_ratio=None, min_score=0.0)
            relaxed = S.retrieve_pack(q, events, segments, entities, summaries, b, current_session_ratio=None, min_score=-1.0)
            identical = ref_ids(strict) == ref_ids(relaxed)
            # how many refs in the relaxed pack have score<=0 (only these are floor-admitted)
            relaxed_zero = sum(1 for r in relaxed["selected_refs"] if r["score"] <= 0)
            print("  budget=%6d strict_refs=%3d relaxed_refs=%3d identical=%s relaxed_zeroScoreRefs=%d"
                  % (b, len(strict["selected_refs"]), len(relaxed["selected_refs"]), identical, relaxed_zero))
            rows.append({"budget": b, "strict_refs": len(strict["selected_refs"]),
                         "relaxed_refs": len(relaxed["selected_refs"]), "identical": identical,
                         "relaxed_zero_score_refs": relaxed_zero,
                         "strict_tokens": strict["used_tokens"], "relaxed_tokens": relaxed["used_tokens"]})
        report["queries"].append({"query": q, "score_gt0_candidates": pos, "budgets": rows})
    out = Path("docs/benchmarks/local_context_token_quality/floor_inertness.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("\n[out]", out)
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
