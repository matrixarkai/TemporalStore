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
3. quality is graded 0–10 by a judge model *against the full local context as ground truth*, with a deterministic term-coverage score as an objective anchor.

Tool traffic is **retained, not dropped**: `tool_use` → `[tool:NAME] <arg>`, `tool_result`
head-truncated (lean). This is the fix in `origin/main 9a803784` — ingestion previously
skipped tool messages entirely, losing ~35% of real local context.

### Headline (tool-inclusive, `qwen2.5:1.5b` reader+judge, 2026-08-09)

| Metric | Local-only replay | TemporalStore managed | Result |
|---|---:|---:|---|
| Tokens fed to model | **194,981** | **999** | **99.5% fewer** (99.85% vs the full 661,347-token corpus) |
| Answer quality (0–10) | 8.46 | **9.31** | managed **+0.85** |
| Queries matched/beaten | — | **6 / 6** | every query |

**Finding.** A **999-token** managed pack *outperforms* a **195k-token** verbose local
replay. Retaining tool information *leanly* preserves the answer signal while denoising:
the verbose local replay suffers lost-in-the-middle, the compact pack does not. For an
enterprise that owns its model loop and pays per token, that is a **~99.5% context-cost
reduction with no quality loss** — a quality *gain*, in fact.

> **Re-run in flight:** a larger-model pass (`qwen2.5:7b` reader+judge), with the per-ref
> cap relaxed so a single context event can return up to ~1k tokens, and a tunable
> current-vs-cross-session budget ratio, is being generated. This page's headline will be
> refreshed with those numbers; the tuning knobs are documented under *Reproduce* below.

### Detailed report

- [Full per-query, per-budget sweep (tool-inclusive)](local_context_token_quality/local_context_token_quality_tool_inclusive.md)
- Machine-readable: [`local_context_token_quality_tool_inclusive.json`](local_context_token_quality/local_context_token_quality_tool_inclusive.json)

### Reproduce

```bash
# reader/judge = OSS model via Ollama (no API key needed)
python3 tools/run_local_context_token_quality_sweep.py \
  --reader ollama --reader-model qwen2.5:7b --judge ollama \
  --roles user,assistant \
  --baseline-max-input-tokens 200000 \
  --budgets 4000,2000,1000 \
  --current-session-ratio 0.5 \
  --tag qwen7b_1k_ratio
```

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

## Other benchmarks

- [OSS comparison: TemporalStore vs OpenViking (LoCoMo + LongMemEval)](../benchmark_archives/temporalstore_vs_openviking_oss_2026-08-08.md)
- [Benchmark & readiness evidence](../benchmark_readiness_evidence_20260629.md)
