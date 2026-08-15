#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Per-prompt token report: local Claude memory (full replay) vs TemporalStore managed pack.

For several REAL prompts taken from local Claude threads, report:
  * Local Claude memory tokens = full local context the agent would replay to answer.
  * TemporalStore managed tokens = the retrieved working pack that answers the same prompt.
  * TemporalStore breakdown = tokens by ref layer (event/segment/entity/summary),
    local vs cross-session share, distinct sessions touched, per-ref detail.

Reuses the ingest + packer + tokenizer from run_local_context_token_quality_sweep so the
numbers are consistent with the existing benchmark. Managed pack = the same deterministic
term-scored ContextPack the token/quality sweep validated against full-context quality.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
import run_local_context_token_quality_sweep as S  # noqa: E402

CLAUDE_ROOT = Path(S.default_claude_root())
CODEX_ROOT = Path(S.default_codex_root())
WORKING_BUDGET = 4000          # working-turn managed budget (fill with relevant refs)
BASELINE_WINDOW = 128000       # let full local replay use the whole window (uncapped-ish)


def pick_real_claude_prompts(events, limit=8, min_tokens=12, max_tokens=220):
    """Substantive, distinct real user prompts drawn from Claude threads (newest first)."""
    picked, seen = [], set()
    for e in reversed(events):  # newest first
        if e.origin != "claude" or e.role != "user":
            continue
        text = e.text.strip()
        if not S._is_visible_real_prompt(text):
            continue
        toks = S.token_estimate(text)
        if toks < min_tokens or toks > max_tokens:
            continue
        key = text[:80].lower()
        if key in seen:
            continue
        seen.add(key)
        picked.append(e)
        if len(picked) >= limit:
            break
    return picked


RELEVANCE_MIN_SCORE = 8.0  # score_text = 10*term_overlap + recency(<=5); >=8 keeps >=1 real hit


def claude_real_turn_context(root: Path, max_files: int):
    """Real tokens each Claude turn carried today, from transcript message.usage."""
    files = [p for p in root.rglob("*.jsonl") if "/subagents/" not in p.as_posix()]
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[:max_files]
    peaks = []
    for path in files:
        peak = 0
        try:
            h = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with h:
            for line in h:
                if '"usage"' not in line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                u = (d.get("message") or {}).get("usage") or {}
                eff = (u.get("input_tokens", 0) or 0) + (u.get("cache_read_input_tokens", 0) or 0) \
                    + (u.get("cache_creation_input_tokens", 0) or 0)
                peak = max(peak, eff)
        if peak > 0:
            peaks.append(peak)
    peaks.sort()
    if not peaks:
        return {}
    n = len(peaks)
    return {
        "threads_measured": n,
        "median_peak_context_tokens": peaks[n // 2],
        "max_peak_context_tokens": peaks[-1],
        "mean_peak_context_tokens": round(sum(peaks) / n),
    }


def managed_breakdown(query, events, segments, entities, summaries, budget):
    pack = S.retrieve_pack(query, events, segments, entities, summaries, budget)
    refs = pack["selected_refs"]
    by_layer = Counter()
    by_layer_count = Counter()
    for r in refs:
        by_layer[r["ref_type"]] += r["tokens"]
        by_layer_count[r["ref_type"]] += 1
    # cross-session vs same (most-recent) session share, using ref ids -> source events
    event_session = {e.event_id: e.session_id for e in events}
    segment_session = {s.segment_id: s.session_id for s in segments}
    newest_session = events[-1].session_id if events else ""
    cross_tokens = same_tokens = 0
    sessions_touched = set()
    for r in refs:
        sess = None
        if r["ref_type"] == "event":
            sess = event_session.get(r["ref_id"])
        elif r["ref_type"] == "segment":
            sess = segment_session.get(r["ref_id"])
        if sess:
            sessions_touched.add(sess)
            if sess == newest_session:
                same_tokens += r["tokens"]
            else:
                cross_tokens += r["tokens"]
        else:  # entity/summary aggregate across sessions -> count as cross-session memory
            cross_tokens += r["tokens"]
    relevant = [r for r in refs if r["score"] >= RELEVANCE_MIN_SCORE]
    relevant_tokens = sum(r["tokens"] for r in relevant)
    relevant_by_layer = Counter()
    for r in relevant:
        relevant_by_layer[r["ref_type"]] += r["tokens"]
    return {
        "working_pack_tokens": pack["used_tokens"],          # fills the working budget
        "relevant_pack_tokens": relevant_tokens,             # only refs with a real query-term hit
        "relevant_ref_count": len(relevant),
        "ref_count": len(refs),
        "by_layer_tokens": dict(by_layer),
        "by_layer_count": dict(by_layer_count),
        "relevant_by_layer_tokens": dict(relevant_by_layer),
        "cross_session_tokens": cross_tokens,
        "same_session_tokens": same_tokens,
        "distinct_sessions": len(sessions_touched),
        "top_refs": [
            {"layer": r["ref_type"], "tokens": r["tokens"], "score": r["score"],
             "text": r["text"][:140]}
            for r in relevant[:6]
        ],
    }


def main():
    claude = S.iter_claude_events(CLAUDE_ROOT, 60, 200, {"user", "assistant"})
    codex = S.iter_codex_events(CODEX_ROOT, 60, 200, {"user", "assistant"})
    events = sorted(claude + codex, key=lambda e: e.timestamp_ms)
    segments = S.build_segments(events, 10)
    entities = S.build_entities(events, 18)
    summaries = S.build_summaries(events, entities)

    full_all = sum(S.token_estimate(e.text) for e in events)
    claude_all = sum(S.token_estimate(e.text) for e in claude)
    real_turn = claude_real_turn_context(CLAUDE_ROOT, 60)
    ingestion = {
        "total_events": len(events), "claude_events": len(claude), "codex_events": len(codex),
        "claude_sessions": len({e.session_id for e in claude}),
        "codex_sessions": len({e.session_id for e in codex}),
        "segments": len(segments), "entities": len(entities), "summaries": len(summaries),
        "full_all_tokens": full_all, "claude_local_memory_tokens": claude_all,
        "working_budget": WORKING_BUDGET,
        "real_claude_turn_context": real_turn,
    }

    prompts = pick_real_claude_prompts(events, limit=8)
    rows = []
    for e in prompts:
        query = e.text.strip()
        full_ctx = S.build_full_context(events, BASELINE_WINDOW)
        local_full = full_ctx["full_all_tokens"]                # everything the agent could replay
        local_fed = full_ctx["fed_tokens"]                      # what fits a 128k window
        mb = managed_breakdown(query, events, segments, entities, summaries, WORKING_BUDGET)
        saving = round(100 * (1 - mb["relevant_pack_tokens"] / max(1, local_fed)), 1)
        rows.append({
            "prompt": query[:160],
            "prompt_tokens": S.token_estimate(query),
            "local_claude_memory_fed_tokens": local_fed,
            "local_claude_memory_full_tokens": local_full,
            "temporalstore_relevant_tokens": mb["relevant_pack_tokens"],
            "temporalstore_working_tokens": mb["working_pack_tokens"],
            "token_saving_vs_local_fed_pct": saving,
            "temporalstore_breakdown": mb,
        })

    agg = {
        "avg_local_fed": round(sum(r["local_claude_memory_fed_tokens"] for r in rows) / len(rows)),
        "avg_relevant_managed": round(sum(r["temporalstore_relevant_tokens"] for r in rows) / len(rows)),
        "avg_working_managed": round(sum(r["temporalstore_working_tokens"] for r in rows) / len(rows)),
        "avg_saving_pct": round(sum(r["token_saving_vs_local_fed_pct"] for r in rows) / len(rows), 1),
        "relevant_layer_tokens_total": dict(sum(
            (Counter(r["temporalstore_breakdown"]["relevant_by_layer_tokens"]) for r in rows), Counter())),
    }
    report = {"ingestion": ingestion, "prompts": rows, "aggregate": agg}
    out = Path("docs/benchmarks/local_context_token_quality/claude_prompt_memory_breakdown.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"ingestion": ingestion, "aggregate": agg, "n_prompts": len(rows)}, indent=2))
    print("[out]", out)


if __name__ == "__main__":
    main()
