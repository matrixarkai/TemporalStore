# TemporalStore Benchmarks

Reproducible evidence for the two questions enterprises ask before adopting a managed
context tier: **how many tokens does it save, and does answer quality hold?**

## Token & Quality: managed context vs full local replay (3-arm)

**Question.** For a real agent working set (the user's full local Codex + Claude history,
**tool events included**), does a small TemporalStore-managed pack answer as well as
replaying the entire local context — and how many tokens does that save?

**Method.** Three arms over the *same* queries and the *same* reader/judge model:
1. **local-only baseline** — replay the full local context (all events, newest-first, up to the reader's input budget).
2. **remote-only managed** — a token-budgeted `ContextPack` retrieved from the ingested Context model (events → segments → entities → summaries).
3. quality is graded 0–10 by a judge model against **query-relevant ground truth** (the most
   on-topic events from the whole corpus), with a deterministic term-coverage score as an
   objective anchor. *Grading against a recency slice is wrong — see the methodology note below.*

Tool traffic is **retained, not dropped**: `tool_use` → `[tool:NAME] <arg>`, `tool_result`
head-truncated (lean). This is the fix in `origin/main 9a803784` — ingestion previously
skipped tool messages entirely, losing ~35% of real local context.

### Headline (`qwen2.5:7b` reader+judge, 1k/event cap, 0.5 current/cross ratio, 2026-08-09)

Full local corpus ingested: **12,384 events / 97 sessions / 1,698,940 tokens** (Codex + Claude, tool-inclusive).

| Metric | Local-only replay | TemporalStore managed | Result |
|---|---:|---:|---|
| Tokens the model works from | **1,698,940** (full corpus) | **~1,333** | **99.92% fewer** |
| Tokens the reader can physically ingest | 7,879 (window-capped) | ~1,333 | 83% fewer at the same window |
| Answer quality (0–10) | 6.59 | **8.30** | managed **+1.71** |
| Queries matched/beaten | — | **6 / 6** | every query |

**Finding.** With a stronger reader+judge (7b) grading against query-relevant ground truth,
the managed pack **wins every query** at ~1.3k tokens. The margin is *largest exactly where
recency-truncated replay fails* — `q_storage` 10.0 vs 6.05, `q_parity` 8.05 vs 5.25, `q_hooks`
8.18 vs 6.05 — and smallest where the newest local context happened to already be on-topic
(`q_context` 8.98 vs 8.67). That gradient is the mechanism, visible: **ranked retrieval beats
"replay the newest N tokens"**, and the enterprise pays for the full local context (~1.7M
tokens) while the managed pack delivers the answer in ~1.3k — a **~99.9% context-cost reduction
with a quality gain**.

> **Why the reader "only ingests 7,879 tokens".** The OSS reader's context window (Ollama
> serves 7b at 4,096) caps what *any* single call can attend to. So "full local replay" that
> exceeds the window is physically truncated to the newest tokens — which is precisely why
> naive replay loses: it can't see the older, on-topic history that ranked retrieval surfaces.
> Token *saving* is therefore measured against the full corpus the agent actually holds and
> pays for (1.7M), while *quality* is probed by feeding each arm to the reader.

### Corroboration (`qwen2.5:1.5b`, tool-inclusive)

A smaller reader+judge over the same design gave managed **9.31 vs 8.46** (6/6) at 999 tokens.
The 1.5b *judge* is lenient (it over-credits), so treat its absolute grades as soft; the 7b run
above is the stronger evidence. Both agree on direction: **managed ≥ local, every query.**

### Detailed reports

- 7b sweep (primary): [JSON](local_context_token_quality/local_context_token_quality_qwen7b_1k_ratio.json)
- 1.5b sweep (tool-inclusive corroboration): [JSON](local_context_token_quality/local_context_token_quality_tool_inclusive.json)

### Methodology note: the judge must grade against topical ground truth, not recency

An earlier 7b pass scored *every* answer ~0. Cause: the judge's reference was the **newest
9,000 chars** of local context, which for this corpus is recent HTML/benchmark work — off-topic
for the questions. The judge (correctly, given that reference) said "not supported." The fix
(`build_judge_reference`) grades against the **most query-relevant events across the whole
corpus** — neutral between arms (independent of the baseline's recency window *and* the managed
pack's selection) and it rewards whichever answer got the topical facts right. Absolute qwen
grades still shift with model/leniency, so the **deterministic term-coverage** anchor is reported
alongside every score; on that anchor the managed pack reaches full coverage (1.0) at ~1–2k tokens
while the recency baseline sits at 0.0 for most queries.

### Reproduce

```bash
# reader/judge = OSS model via Ollama (no API key needed)
python3 tools/run_local_context_token_quality_sweep.py \
  --reader ollama --reader-model qwen2.5:7b --judge ollama \
  --roles user,assistant \
  --baseline-max-input-tokens 8000 \
  --budgets 4000,2000,1000 \
  --current-session-ratio 0.5 \
  --reader-timeout 400 \
  --tag qwen7b_1k_ratio
```

> Cap `--baseline-max-input-tokens` near the reader's context window (Ollama serves 7b at
> 4,096). Feeding the full 1.7M-token corpus makes Ollama `--context-shift` prefill dozens of
> windows on CPU — hours per query and past the timeout — without the model attending to more
> than its window anyway. The saving headline is still computed against the full corpus.

Knobs:
- `--reader-model` — reader/judge model (e.g. `qwen2.5:1.5b`, `qwen2.5:7b`).
- `--budgets` — pack token budgets to sweep; the report reports the *minimum* budget that still matches/beats baseline quality.
- `--current-session-ratio` — reserve this fraction of the pack budget for newest-session refs (0 = pure relevance fill; 0.5 = balanced current/cross split, the remote-only reconstruction case).
- `SWEEP_PER_REF_TEXT_CHARS` (env, default 4000) — per-ref fed-text cap; ~4000 chars ≈ 1k tokens per context event.

## Scope note: memory only (no skills/resources tier yet)

This benchmark measures **conversational + tool memory**. TemporalStore ingests **resources**
(imported docs/markdown are captured as `resource` refs and budgeted separately), but it does
**not yet have a first-class *skills* layer** (discover → capture → learn reusable procedures).
Competing memory products that unify memory + resources + skills will show breadth this
benchmark does not exercise. Closing the skills gap is tracked as a follow-up; until then,
adoption claims should be scoped to memory + resources, not skills.
