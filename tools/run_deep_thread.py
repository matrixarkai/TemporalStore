#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Deep-thread validation: does quality hold as a thread accumulates context?

Models a real, deep back-and-forth: each turn's Q+A is appended to a growing "current
session" that is ITSELF retrievable (newest timestamps, so it competes for the pack via
current_session_ratio). As the thread deepens, more context piles up — this tests whether
the bounded managed pack keeps the relevant recent thread in view and whether quality holds,
and whether conditional query rewriting matters more as depth grows.

Reader generates the assistant turns (so the thread accumulates real content); Claude judges
(default) — run captures answers, agent scores in-loop.
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import run_local_context_token_quality_sweep as S
import matrixark_query_rewrite as QR

# A deep, coherent, evolving thread (16 turns) with heavy anaphora and back-references.
THREAD = [
    ("What did we decide about ingesting all local Codex/Claude context into TemporalStore?", ("ingest","local","context","codex","claude")),
    ("How are tool events handled in that ingestion?", ("tool","lean","tool_use","tool_result")),
    ("What was the fix when those were being dropped?", ("tool","skip","lean","fix","backfill")),
    ("Why did that matter for token savings?", ("token","saving","context","replay")),
    ("How much did it save on deep sessions?", ("token","saving","percent","median","484k")),
    ("What was the managed pack size compared to that?", ("managed","pack","tokens","4k","bounded")),
    ("And which model judged the quality?", ("qwen","claude","judge","reader")),
    ("Why did we switch that judge?", ("qwen","claude","over","score","noisy","judge")),
    ("What did the switch reveal about the earlier numbers?", ("qwen","over","score","1-3","6-8")),
    ("So did relaxing the relevance floor help at all?", ("floor","relax","inert","score","budget")),
    ("Why was it inert exactly?", ("budget","rank","score","tail","identical")),
    ("Then what actually improved the follow-ups?", ("query","rewrite","anaphora","followup")),
    ("Is that rewrite always on, or conditional?", ("conditional","followup","standalone","rewrite")),
    ("How do we detect a follow-up for it?", ("anaphora","pronoun","continuation","detect")),
    ("Where should that live in production?", ("retrieve","session","buffer","gated","rank")),
    ("Summarize the whole thread decision for me.", ("ingest","tool","floor","rewrite","judge","token")),
]


def make_event(text, role, ts_ms, idx):
    return S.ContextEvent(event_id=S.stable_id("thread", idx, text[:40]), timestamp="", timestamp_ms=ts_ms,
                          session_id="__thread__", origin="thread", source_file="thread", line_no=idx, role=role, text=text)


def run_config(rewrite, reader, corpus, segments, entities, summaries, base_ts, budget, ratio):
    events0 = list(corpus)
    thread_events = []
    prior_q = []
    turns = []
    for ti, (query, terms) in enumerate(THREAD, 1):
        rq, was, _ = QR.conditional_retrieval_query(query, prior_q, enabled=rewrite)
        all_events = events0 + thread_events                 # growing thread is retrievable (newest ts)
        ref = S.build_judge_reference(rq, all_events)
        pack = S.retrieve_pack(rq, all_events, segments, entities, summaries, budget, current_session_ratio=ratio)
        cur = sum(r["tokens"] for r in pack["selected_refs"] if r.get("is_current"))
        ans = reader.answer(query, S.pack_to_context_str(pack)).get("text", "")
        turns.append({"turn": ti, "query": query, "rewritten": was, "reference": ref[:2500],
                      "expected_terms": list(terms),
                      "configs": {"deep": {"answer": ans, "quality": None,
                                           "pack_tokens": pack["used_tokens"], "refs": len(pack["selected_refs"]),
                                           "thread_tokens": cur, "corpus_tokens": pack["used_tokens"] - cur,
                                           "thread_depth_events": len(thread_events)}}})
        # append this turn's Q + A to the growing thread (newest timestamps)
        thread_events.append(make_event("User: " + query, "user", base_ts + 2 * ti, 2 * ti))
        thread_events.append(make_event("Assistant: " + ans, "assistant", base_ts + 2 * ti + 1, 2 * ti + 1))
        prior_q.append(query)
        print(f"  turn {ti:2d} rewritten={was} refs={len(pack['selected_refs']):3d} thread_tok={cur} depth={len(thread_events)}", flush=True)
    return turns


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reader-model", default="qwen2.5:1.5b")
    ap.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    ap.add_argument("--reader-timeout", type=float, default=300.0)
    ap.add_argument("--reader-max-tokens", type=int, default=200)
    ap.add_argument("--budget", type=int, default=4000)
    ap.add_argument("--current-session-ratio", type=float, default=0.5)
    ap.add_argument("--out", default="docs/benchmarks/local_context_token_quality/deep_thread.json")
    args = ap.parse_args()

    roles = {"user", "assistant"}
    claude = S.iter_claude_events(Path(S.default_claude_root()), 200, 400, roles)
    codex = S.iter_codex_events(Path(S.default_codex_root()), 200, 400, roles)
    corpus = sorted(claude + codex, key=lambda e: e.timestamp_ms)
    segments = S.build_segments(corpus, 10)
    entities = S.build_entities(corpus, 18)
    summaries = S.build_summaries(corpus, entities)
    base_ts = max(e.timestamp_ms for e in corpus) + 1000 if corpus else 1
    print(json.dumps({"ingest": {"corpus_events": len(corpus)}, "reader": args.reader_model}), flush=True)

    reader = S.OpenAICompatReader(args.reader_base_url, args.reader_model, args.reader_timeout, args.reader_max_tokens)
    report = {"reader": args.reader_model, "judge": "claude", "budget": args.budget, "configs": {}}
    for rewrite in (False, True):
        name = "rewrite" if rewrite else "no_rewrite"
        print(f"[config] {name}", flush=True)
        report["configs"][name] = run_config(rewrite, reader, corpus, segments, entities, summaries,
                                              base_ts, args.budget, args.current_session_ratio)
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    # emit Claude-judge cases (per config, per turn)
    import matrixark_bench_judge as J
    cases = []
    for cfg, turns in report["configs"].items():
        for t in turns:
            c = t["configs"]["deep"]
            cases.append(J.make_case(f"{cfg}:t{t['turn']}", t["query"], c["answer"], t["reference"], t["expected_terms"]))
    J.write_cases(str(out) + ".cases.json", cases)
    print("[out]", out, "| cases:", str(out) + ".cases.json", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
