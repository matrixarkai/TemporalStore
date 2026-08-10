#!/usr/bin/env python3
"""Does relaxing the relevance floor + using the large budget help follow-up prompts?

A deep thread of follow-up-style queries (low term-overlap on purpose), answered
remote-only under three configs that isolate the two levers:

  baseline    budget 3k,  strict floor (min_score 0)   -- what we ship / measured
  big_budget  budget 16k, strict floor (min_score 0)   -- more budget, same floor
  relaxed     budget 16k, relaxed floor (min_score -1) -- more budget + admit recency fill

Same corpus, same queries; only (budget, floor) change. qwen-7b reader + judge,
graded vs query-relevant ground truth.
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import run_local_context_token_quality_sweep as S

# Deliberately anaphoric follow-ups: pronouns/references that DON'T lexically match the facts.
THREAD = [
    ("What did we decide about real-time Codex/Claude prompt ingestion via hooks?",
     ("hook", "real", "ingest", "codex", "claude", "session")),
    ("How are tool events handled during that?",
     ("tool", "lean", "tool_use", "tool_result", "text")),
    ("What was the fix for the ones being skipped?",
     ("tool", "skip", "lean", "backfill", "fix")),
    ("Why did that matter for token savings?",
     ("token", "saving", "context", "replay", "quality")),
    ("And how does the session buffer fit into it?",
     ("session", "buffer", "idle", "commit", "extract")),
    ("Summarize the whole decision for me.",
     ("hook", "ingest", "tool", "session", "extract", "token")),
]

CONFIGS = [
    ("baseline",   3000, 0.0),
    ("big_budget", 6000, 0.0),
    ("relaxed",    6000, -1.0),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reader-model", default="qwen2.5:7b")
    ap.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    ap.add_argument("--reader-timeout", type=float, default=400.0)
    ap.add_argument("--reader-max-tokens", type=int, default=220)
    ap.add_argument("--current-session-ratio", type=float, default=0.5)
    ap.add_argument("--out", default="docs/benchmarks/local_context_token_quality/floor_followup_qwen7b.json")
    ap.add_argument("--no-judge", action="store_true", help="capture answers only (skip qwen judge); an external judge scores them")
    ap.add_argument("--query-rewrite", action="store_true",
                    help="resolve follow-up anaphora: build the RETRIEVAL query from the running thread "
                         "(prior turns + current), so 'that'/'the ones' carry their referent terms")
    ap.add_argument("--rewrite-window", type=int, default=3, help="how many prior turns to fold into the retrieval query")
    args = ap.parse_args()

    roles = {"user", "assistant"}
    claude = S.iter_claude_events(Path("/mnt/c/Users/Deeproute/.claude/projects"), 200, 400, roles)
    codex = S.iter_codex_events(Path("/mnt/c/Users/Deeproute/.codex/sessions"), 200, 400, roles)
    events = sorted(claude + codex, key=lambda e: e.timestamp_ms)
    segments = S.build_segments(events, 10)
    entities = S.build_entities(events, 18)
    summaries = S.build_summaries(events, entities)
    print(json.dumps({"ingest": {"events": len(events), "segments": len(segments)}}), flush=True)

    reader = S.OpenAICompatReader(args.reader_base_url, args.reader_model, args.reader_timeout, args.reader_max_tokens)
    print(json.dumps({"reader": args.reader_model}), flush=True)

    rows = []
    prior_queries: list[str] = []
    for ti, (query, terms) in enumerate(THREAD, 1):
        # Resolve anaphora: retrieve with the running thread, but the reader still answers the raw follow-up.
        rq = (" ".join(prior_queries[-args.rewrite_window:]) + " " + query).strip() if args.query_rewrite else query
        print(f"[turn {ti}] {query[:44]}", flush=True)
        ref = S.build_judge_reference(rq, events)
        turn = {"turn": ti, "query": query, "retrieval_query": rq, "reference": ref[:3000], "configs": {}}
        for name, budget, minscore in CONFIGS:
            pack = S.retrieve_pack(rq, events, segments, entities, summaries, budget,
                                   current_session_ratio=(args.current_session_ratio or None), min_score=minscore)
            ans = reader.answer(query, S.pack_to_context_str(pack)).get("text", "")
            score = None if args.no_judge else S._combined(reader.judge(query, ans, ref), S.term_coverage(ans, terms))
            turn["configs"][name] = {"budget": budget, "min_score": minscore,
                                     "used_tokens": pack["used_tokens"], "refs": len(pack["selected_refs"]),
                                     "answer": ans, "quality": score}
            print(f"    {name:11s} refs={len(pack['selected_refs']):3d} tok={pack['used_tokens']:6d} q={score}", flush=True)
        rows.append(turn)
        prior_queries.append(query)

    def avg(name):
        v = [t["configs"][name]["quality"] for t in rows if t["configs"][name]["quality"] is not None]
        return round(sum(v) / len(v), 3) if v else None
    agg = {name: {"avg_quality": avg(name),
                  "avg_refs": round(sum(t["configs"][name]["refs"] for t in rows) / len(rows), 1),
                  "avg_tokens": round(sum(t["configs"][name]["used_tokens"] for t in rows) / len(rows))}
           for name, _, _ in CONFIGS}
    report = {"reader": args.reader_model, "aggregate": agg, "turns": rows}
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"aggregate": agg}, indent=2), flush=True)
    print("[out]", out, flush=True)
    print("DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
