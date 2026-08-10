# TemporalStore vs OpenViking — Claude-as-judge evaluation
### LOCOMO & LongMemEval_s, OSS reader + Claude judge · 2026-08-08

**One-line result:** Under a **Claude LLM judge** (not deterministic term-matching), TemporalStore and the
OpenViking-style baseline **tie on LOCOMO** (they produce byte-identical answers when retrieval fits the
budget) and TemporalStore **wins LongMemEval_s ~2.4×** (38.0% vs 16.0%) — confirming the deterministic run
with a real evaluator on the same class of protocol the competitor papers use (they judge with GPT-4o).

---

## 1. Why this run, and what changed

The prior OSS run scored answers with a **deterministic answer-term matcher** (an honest lower bound). The
memory-system papers (Mem0, Zep, MemOS) instead score with an **LLM judge (GPT-4o)**. This run swaps in a
**Claude judge** to put quality on the same footing.

**Reader vs judge — the honest split.** There is no `ANTHROPIC_API_KEY` in the benchmark environment, so we
could not use Claude as the *reader*. Per the harness's built-in design (`tools/matrixark_bench_judge.py`:
*"Claude-as-judge … the DEFAULT judge"*), the **reader stayed OSS (`qwen2.5:1.5b`, identical for both arms)**
and **Claude (this session) is the evaluator**. So this is *OSS reader + Claude judge*, not Claude-as-reader.

## 2. Setup

| Dimension | Value |
|---|---|
| Judge | **Claude** (interactive session) — rubric-scored 0–10, `correct := score ≥ 6` |
| Reader | `qwen2.5:1.5b` (OSS, candidate-hybrid), identical for both arms |
| Encoder | `all-MiniLM-L6-v2` (shared) |
| Sample | **100 LOCOMO + 50 LongMemEval_s**, paired (same question to both arms), deterministic hash sample of the full-run per-query outputs |
| Rubric | 0–2 wrong/hallucinated/raw-dump · 3–5 partial · 6–7 correct, minor gaps · 8–10 fully correct |

## 3. Results

| Dataset | n | TemporalStore acc | OpenViking acc | TS mean | OV mean |
|---|--:|--:|--:|--:|--:|
| **LOCOMO** (tie — identical answers) | 100 | **44.0%** | 44.0% | 4.50 | 4.50 |
| **LongMemEval_s** | 50 | **38.0%** | 16.0% | 4.12 | 1.94 |
| Overall | 150 | 42.0% | 34.7% | 4.37 | 3.65 |

## 4. Interpretation

- **LOCOMO is a tie by construction.** On the sampled LOCOMO questions the two backends retrieved the same
  in-budget context, so the shared reader emitted **byte-identical answers** — Claude necessarily scores them
  equally (44.0% each). The memory backend cannot differentiate answer quality when everything fits the budget;
  its LOCOMO edge is latency (shown in the deterministic run).
- **LongMemEval_s is where the memory layer wins.** On long multi-session histories TemporalStore's structured
  temporal retrieval yields clean, direct answers (*"16 GB"*, *"yellow dress"*, *"5 hours"*) while the flat
  source-retrieval baseline returns raw dumps or misses (*"Have you made any other recent changes…"*). Claude
  scores TemporalStore **38.0% vs 16.0% — ~2.4×**.
- **Claude judge vs deterministic scorer.** The deterministic run reported LongMemEval reader-hit
  **0.534 (TS) / 0.208 (OV)**; Claude reports **0.38 / 0.16**. Claude is *stricter* — it rejects OV's raw-dump
  "answers" (which term-matching sometimes counted as hits) and TS's noisy multi-guess strings
  (*"3; 1; 4; 10; 15; 2"* for a "how many" question). Absolutes drop, the **~2.4× relative gap is preserved** —
  and the gap is arguably *cleaner* under a real judge.

## 5. Relation to the competitor protocol

Mem0/Zep/MemOS publish LOCOMO/LongMemEval accuracy scored by a **GPT-4o LLM judge**. This run uses a **Claude
judge** — the same class of methodology — so the *evaluation* side is now paper-comparable. The remaining gap
to a full paper-comparable claim is the **reader**: those papers pair the judge with a strong agent LLM, while
this run's reader is a 1.5B OSS model. A full Claude-reader + Claude-judge run needs an `ANTHROPIC_API_KEY`
(the harness already supports it via `--reader-base-url https://api.anthropic.com/v1` + `--judge anthropic`).

## 6. Caveats & provenance
- **OSS reader, Claude judge** — not Claude-as-reader (no API key). Absolute accuracy is bounded by the 1.5B reader.
- **Sample** (150 paired cases), not the full datasets; deterministic hash sample for reproducibility.
- **LOCOMO answers were identical across arms** in the sample → tie is structural, not a judging artifact.
- Judge = this Claude session; scores in `claude_judge_results.json`; cases in the run dir
  `/root/matrixark_benchmarks/runs/claude_judge/`.
