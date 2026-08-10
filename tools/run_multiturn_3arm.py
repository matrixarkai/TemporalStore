#!/usr/bin/env python3
"""Multi-turn 3-arm token/quality experiment.

Deep back-and-forth conversations, turn by turn. Each turn appends to the local
conversation, so the local context GROWS while a managed pack stays bounded — the
real cost curve. Three arms per turn, same reader/judge model:

  LOCAL     replay the whole growing conversation-so-far (what the agent holds locally).
  REMOTE    remote-only: a token-budgeted ranked ContextPack (no local).
  AUGMENT   local+remote: a bounded window of recent local turns + a cross-session
            pack, deduped against the recent local turns.

Reports per-turn and cumulative token breakdowns per arm + LLM quality (judged vs
query-relevant ground truth from the corpus).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import run_local_context_token_quality_sweep as S

# Deep, single-topic conversations grounded in this repo's real history.
CONVERSATIONS = [
    {
        "topic": "hooks_ingestion",
        "turns": [
            ("What did we decide about real-time Codex/Claude prompt ingestion via hooks?",
             ("hook", "real", "ingest", "codex", "claude", "session")),
            ("How are tool events handled during ingestion?",
             ("tool", "lean", "tool_use", "tool_result", "text")),
            ("What was the fix for tool messages being skipped?",
             ("tool", "skip", "lean", "backfill", "fix")),
            ("How do the session buffer and idle commit work?",
             ("session", "buffer", "idle", "commit", "extract")),
            ("What is the difference between provisional and final extraction?",
             ("provisional", "final", "boundary", "commit", "extract")),
        ],
    },
    {
        "topic": "benchmark_budget",
        "turns": [
            ("What did the 3-arm token and quality benchmark show?",
             ("managed", "token", "saving", "quality", "baseline")),
            ("Which model was used as reader and judge?",
             ("qwen", "reader", "judge", "ollama")),
            ("Why did the recency-truncated baseline fail on some queries?",
             ("recency", "truncat", "window", "off-topic", "replay")),
            ("How much token saving at equal-or-better quality?",
             ("saving", "token", "percent", "quality")),
            ("What is the budget split for cross-session vs current session?",
             ("cross", "session", "current", "ratio", "budget")),
        ],
    },
]


def _dedup_pack_against_local(pack_refs: list[dict], local_text: str) -> tuple[list[dict], int]:
    """Drop pack refs whose head already appears in the recent local text (augment dedup)."""
    local_norms = set()
    for w in local_text.split("\n"):
        local_norms.add(w.strip()[:80])
    kept, tokens = [], 0
    for r in pack_refs:
        head = str(r.get("text", ""))[:80].strip()
        if head and head in local_norms:
            continue
        kept.append(r)
        tokens += int(r.get("tokens", 0))
    return kept, tokens


def _pack_breakdown(pack: dict) -> dict:
    by_layer, cur, cross = {}, 0, 0
    for r in pack.get("selected_refs", []):
        by_layer[r["ref_type"]] = by_layer.get(r["ref_type"], 0) + int(r["tokens"])
        if r.get("is_current"):
            cur += int(r["tokens"])
        else:
            cross += int(r["tokens"])
    return {"by_layer": by_layer, "current_session": cur, "cross_session": cross}


def main() -> int:
    ap = argparse.ArgumentParser(description="Multi-turn 3-arm token/quality experiment.")
    ap.add_argument("--codex-root", default="/mnt/c/Users/Deeproute/.codex/sessions")
    ap.add_argument("--claude-root", default="/mnt/c/Users/Deeproute/.claude/projects")
    ap.add_argument("--max-sessions", type=int, default=200)
    ap.add_argument("--max-msgs", type=int, default=400)
    ap.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    ap.add_argument("--reader-model", default="qwen2.5:7b")
    ap.add_argument("--reader-timeout", type=float, default=400.0)
    ap.add_argument("--reader-max-tokens", type=int, default=220)
    ap.add_argument("--remote-budget", type=int, default=3000)
    ap.add_argument("--augment-local-turns", type=int, default=2, help="recent local turns kept in augment mode")
    ap.add_argument("--augment-remote-budget", type=int, default=1500)
    ap.add_argument("--current-session-ratio", type=float, default=0.5)
    ap.add_argument("--local-window-cap", type=int, default=8000, help="cap fed to reader (its context window)")
    ap.add_argument("--max-quality-turns", type=int, default=2,
                    help="measure LLM quality only for turns <= this (token curve computed for ALL turns)")
    ap.add_argument("--out-dir", default="docs/benchmarks/local_context_token_quality")
    ap.add_argument("--tag", default="multiturn_qwen7b")
    args = ap.parse_args()

    roles = {"user", "assistant"}
    claude = S.iter_claude_events(Path(args.claude_root), args.max_sessions, args.max_msgs, roles)
    codex = S.iter_codex_events(Path(args.codex_root), args.max_sessions, args.max_msgs, roles)
    events = sorted(claude + codex, key=lambda e: e.timestamp_ms)
    segments = S.build_segments(events, 10)
    entities = S.build_entities(events, 18)
    summaries = S.build_summaries(events, entities)
    print(json.dumps({"ingest": {"events": len(events), "segments": len(segments),
                                 "entities": len(entities), "summaries": len(summaries),
                                 "corpus_tokens": sum(S.token_estimate(e.text) for e in events)}}), flush=True)

    reader = S.OpenAICompatReader(args.reader_base_url, args.reader_model, args.reader_timeout, args.reader_max_tokens)
    judge = reader
    probe = reader.ready() if hasattr(reader, "ready") else {"ready": True}
    print(json.dumps({"reader": args.reader_model, "probe": probe}), flush=True)

    conv_reports = []
    for conv in CONVERSATIONS:
        print(f"[conv] {conv['topic']}", flush=True)
        local_history: list[tuple[str, str]] = []   # (q, a) accumulates
        turns_out = []
        cum = {"local": 0, "remote": 0, "augment": 0}
        # deterministic proxy answer (~reader_max_tokens) to grow the local curve past the quality window
        proxy_answer = ("Assistant answer summarizing the step and result. " * max(1, args.reader_max_tokens // 8)).strip()
        for ti, (query, terms) in enumerate(conv["turns"], 1):
            measure = ti <= args.max_quality_turns
            print(f"  [turn {ti}] measure={measure} {query[:44]}", flush=True)
            ref = S.build_judge_reference(query, events) if measure else ""

            # ---- ARM LOCAL: full growing conversation replay ----
            convo_text = "\n\n".join(f"User: {q}\nAssistant: {a}" for q, a in local_history)
            local_ctx_full = (convo_text + f"\n\nUser: {query}").strip()
            local_true_tokens = S.token_estimate(local_ctx_full)
            if measure:
                a_local = reader.answer(query, local_ctx_full[: args.local_window_cap * 4]).get("text", "")
                s_local = S._combined(judge.judge(query, a_local, ref), S.term_coverage(a_local, terms))
            else:
                a_local, s_local = proxy_answer, None
            local_history.append((query, a_local))   # local accumulates for next turn

            # ---- ARM REMOTE: remote-only bounded pack ----
            pack = S.retrieve_pack(query, events, segments, entities, summaries, args.remote_budget,
                                   current_session_ratio=(args.current_session_ratio or None))
            remote_tokens = pack["used_tokens"]
            if measure:
                a_remote = reader.answer(query, S.pack_to_context_str(pack)).get("text", "")
                s_remote = S._combined(judge.judge(query, a_remote, ref), S.term_coverage(a_remote, terms))
            else:
                s_remote = None

            # ---- ARM AUGMENT: recent local window + cross-session pack (deduped) ----
            recent = local_history[-args.augment_local_turns - 1:-1]  # exclude current turn's just-added answer
            recent_text = "\n\n".join(f"User: {q}\nAssistant: {a}" for q, a in recent)
            apack = S.retrieve_pack(query, events, segments, entities, summaries, args.augment_remote_budget,
                                    current_session_ratio=(args.current_session_ratio or None))
            kept_refs, apack_tokens = _dedup_pack_against_local(apack["selected_refs"], recent_text)
            aug_tokens = S.token_estimate(recent_text) + apack_tokens
            if measure:
                aug_ctx = (recent_text + "\n\n" + "\n\n".join(f"[{r['ref_type']}] {r['text']}" for r in kept_refs)).strip()
                a_aug = reader.answer(query, aug_ctx).get("text", "")
                s_aug = S._combined(judge.judge(query, a_aug, ref), S.term_coverage(a_aug, terms))
            else:
                s_aug = None

            cum["local"] += local_true_tokens
            cum["remote"] += remote_tokens
            cum["augment"] += aug_tokens
            turns_out.append({
                "turn": ti, "query": query,
                "tokens": {"local": local_true_tokens, "remote": remote_tokens, "augment": aug_tokens},
                "cumulative": dict(cum),
                "quality": {"local": s_local, "remote": s_remote, "augment": s_aug},
                "remote_pack_breakdown": _pack_breakdown(pack),
                "augment_pack_kept_refs": len(kept_refs),
            })
        def _avg_q(turns, arm):
            vals = [t["quality"][arm] for t in turns if t["quality"][arm] is not None]
            return round(sum(vals) / len(vals), 3) if vals else None
        conv_reports.append({"topic": conv["topic"], "turns": turns_out,
                             "final_cumulative": dict(cum),
                             "avg_quality": {arm: _avg_q(turns_out, arm) for arm in ("local", "remote", "augment")}})

    # aggregate across conversations
    n_turns = sum(len(c["turns"]) for c in conv_reports)
    agg = {
        "total_turns": n_turns,
        "cumulative_tokens": {arm: sum(c["final_cumulative"][arm] for c in conv_reports) for arm in ("local", "remote", "augment")},
        "avg_quality": {arm: (lambda v: round(sum(v) / len(v), 3) if v else None)(
                            [t["quality"][arm] for c in conv_reports for t in c["turns"] if t["quality"][arm] is not None])
                        for arm in ("local", "remote", "augment")},
        "quality_turns_measured": sum(1 for c in conv_reports for t in c["turns"] if t["quality"]["remote"] is not None),
    }
    agg["token_saving_pct"] = {
        "remote_vs_local": round(100 * (1 - agg["cumulative_tokens"]["remote"] / max(1, agg["cumulative_tokens"]["local"])), 2),
        "augment_vs_local": round(100 * (1 - agg["cumulative_tokens"]["augment"] / max(1, agg["cumulative_tokens"]["local"])), 2),
    }
    report = {"reader": args.reader_model, "remote_budget": args.remote_budget,
              "current_session_ratio": args.current_session_ratio, "aggregate": agg, "conversations": conv_reports}
    out = Path(args.out_dir) / f"multiturn_3arm_{args.tag}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"aggregate": agg}, indent=2), flush=True)
    print("[out]", out, flush=True)
    print("DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
