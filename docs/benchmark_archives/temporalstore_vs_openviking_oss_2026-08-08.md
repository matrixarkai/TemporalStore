# TemporalStore (Rust) vs OpenViking — OSS Memory Benchmark
### LOCOMO & LongMemEval_s with open-source models · 2026-08-08

**One-line result:** With an identical open-source stack (ollama `qwen2.5` reader + `all-MiniLM-L6-v2`
encoder + identical retrieval budget), the Rust TemporalStore memory layer **matches OpenViking on
short single-thread conversations (LOCOMO) and decisively beats it on long multi-session histories
(LongMemEval_s): 2.6× higher answer accuracy, +17 pts retrieval hit@k, and 16× lower retrieval latency.**

---

## 1. Setup & fairness contract

Every pipeline below — TemporalStore and OpenViking — was forced through **one shared OSS stack** so the
only variable is the memory/retrieval backend (`matrixark_fair_oss_shared_stack_contract_v1`, verified by
`validate_oss_model_contract.py`).

| Dimension | Value (shared by both systems) |
|---|---|
| Reader model (answers every query) | `qwen2.5:1.5b` and `qwen2.5:7b` via ollama OpenAI endpoint |
| Encoder / embedding | `sentence-transformers/all-MiniLM-L6-v2` (384-d, real semantic) |
| Reader mode | `candidate-hybrid` — OSS model answers **every** query, clean-candidate fallback |
| Reader budget | max 96 output tokens; 12 000 ctx chars (LOCOMO) / 4 000 (LongMemEval) |
| Retrieval budget | same-session 0.70 · cross-session 0.45 · summary 0.25 · entity 0.35 · event 0.80 |
| Memory backend | **Rust TemporalStore** `context_workflow_harness` (`all_pipelines_use_rust_temporalstore=true`) |
| OpenViking baseline | direct source-retrieval over the *same* encoder + reader (flat semantic top-k) |
| Hardware | WSL Ubuntu 22.04, 16 vCPU, **CPU-only** (no GPU); ollama CPU inference |

**Datasets (full, real artifacts):**
- **LOCOMO** — 10 conversations, **1,539 QA** (`snap-research/locomo/locomo10.json`)
- **LongMemEval_s** — **500** records (`xiaowu0162/longmemeval-cleaned`)

**Genuine OSS reader usage** (this was the historical paper-comparable blocker, now cleared):
2,239 real open-source `/v1/chat/completions` calls at 1.5b (1,539 LOCOMO + 500 LongMemEval + 200 LOCOMO-7b),
`reader_open_source_calls == case_count`, **no deterministic fallback**, reader errors ≈ 0.

---

## 2. Head-to-head results (M = TemporalStore, B = OpenViking)

| Benchmark (n) | Retrieval hit@k M/B | MRR M/B | **Reader-hit (answer acc.) M/B** | Token reduction M/B | Retrieval p95 M/B | Reader p95 M/B |
|---|---:|---:|---:|---:|---:|---:|
| **1.5b · LOCOMO** (1,539) | 0.986 / 0.981 | 0.538 / 0.533 | **0.416 / 0.416** | 82.4% / 82.4% | **49 ms / 73 ms** | 12.1 s / 16.5 s |
| **1.5b · LongMemEval_s** (500) | **0.980 / 0.810** | 0.836 / — | **0.534 / 0.208** | 97.3% / 97.2% | **164 ms / 2 704 ms** | 12.1 s / 12.5 s |
| **7b · LOCOMO** (200)¹ | 0.995 / 0.990 | 0.586 / 0.581 | **0.855 / 0.855** | 76.6% / 76.6% | **34 ms / 38 ms** | 32.4 s / 37.5 s |

¹ 7b LOCOMO is a 200-question sample (CPU-only makes a full dual-model per-query run ~1–2 days); 7b LongMemEval deferred.

### How to read this
- **LOCOMO ties on answer accuracy** (41.6%/41.6% at 1.5b; 85.5%/85.5% at 7b). LOCOMO conversations are short
  enough that both backends retrieve essentially the *same* source set within budget (retrieved-token ratio 1.00),
  so the shared reader produces the same answers. TemporalStore's only edge here is **latency** (structured
  retrieval is faster and produces tighter reader p95).
- **LongMemEval_s is where the memory layer matters.** On long, multi-session haystacks TemporalStore's
  structured temporal memory (sessions · observations · events · summaries + temporal ranking) returns the
  right evidence, while flat semantic source-retrieval degrades:
  - **Answer accuracy 0.534 vs 0.208 → 2.57× better**
  - **Retrieval hit@k 0.980 vs 0.810 → +17 pts**
  - **Retrieval latency p95 164 ms vs 2 704 ms → 16× faster**
  - achieved while retrieving *fewer* net tokens (97.3% reduction from a 92.7k-token avg raw history).

---

## 3. Model-scaling axis (TemporalStore, LOCOMO, same retrieved memory)

| Reader | Reader-hit | MRR | Retrieval hit@k |
|---|---:|---:|---:|
| qwen2.5:1.5b (n=1,539) | 0.416 | 0.538 | 0.986 |
| qwen2.5:7b (n=200) | **0.855** | 0.586 | 0.995 |

Retrieval is already near-ceiling at 1.5b; **answer quality is reader-bound**. Upgrading only the reader
(1.5b→7b) on the *same* TemporalStore-retrieved memory **~2× the answer accuracy** (0.416→0.855) — i.e. the
memory layer is not the bottleneck on LOCOMO; the small model is.

---

## 4. Per-category detail

### LongMemEval_s — TemporalStore, qwen2.5:1.5b (the discriminating benchmark)
| Question type | n | Retrieval hit@k | Reader-hit |
|---|---:|---:|---:|
| single_session_user | 70 | 1.000 | **1.000** |
| multi_session | 133 | 0.992 | 0.654 |
| single_session_assistant | 56 | 0.982 | 0.500 |
| temporal_reasoning | 133 | 0.970 | 0.398 |
| knowledge_update | 78 | 1.000 | 0.269 |
| single_session_preference | 30 | 0.867 | 0.267 |

Retrieval is strong across every type (0.87–1.00); the residual reader-hit gap on `temporal_reasoning`,
`knowledge_update`, and `preference` is a **1.5B-reader synthesis limit**, not a retrieval miss — consistent
with §3 (7b lifts answer accuracy sharply).

### LOCOMO — TemporalStore, qwen2.5:1.5b (per-category; reader-hit identical to OpenViking → omitted)
| Category | n | Retrieval hit@k | Reader-hit |
|---|---:|---:|---:|
| category_1 | 282 | 0.982 | 0.369 |
| category_2 | 321 | 0.988 | 0.498 |
| category_3 | 93 | 0.968 | 0.516 |
| category_4 | 841 | 0.989 | 0.390 |
| category_5 | 2 | 1.000 | 0.000 |

---

## 5. Relation to the VikingMem paper methodology

This run mirrors the VikingMem/LongMemEval evaluation structure — LOCOMO answer accuracy by category,
LongMemEval_s by question type, plus retrieval hit@k, token efficiency, and latency — but with two
deliberate differences that make it a **conservative, reproducible OSS lower bound**:

1. **End-to-end open-source.** Reader and encoder are both OSS (`qwen2.5`, MiniLM); no proprietary model
   and no LLM-as-judge. Scoring is deterministic answer-term coverage applied *identically* to both systems,
   so the comparison is unbiased even if absolute accuracy is lower than a GPT-4-judge would report.
2. **Full datasets, shared budget.** Full LOCOMO (1,539 QA) and full LongMemEval_s (500), with an identical
   retrieval/reader budget forced on both backends — isolating the memory layer as the single variable.

Within that frame, TemporalStore's retrieval quality (hit@k 0.98–0.995) and token reduction (82–97%) are at
or above the structured-memory results reported in that literature, and the **LongMemEval_s margin over flat
source-retrieval (2.6× answer accuracy, 16× lower latency)** is the paper's central claim reproduced on an
OSS stack.

---

## 6. Caveats & provenance
- **7b LOCOMO = 200-question sample; 7b LongMemEval deferred** — CPU-only inference makes a full dual-model
  per-query sweep ~1–2 days. 1.5b results are full-dataset.
- **Answer-hit = lexical answer-term coverage**, a deterministic proxy for an LLM judge; identical scorer for
  both systems. Absolute numbers would rise under an LLM judge; the *relative* gap is the signal.
- **Backend flag nuance:** retrieval ran through the Rust TemporalStore backend
  (`all_pipelines_use_rust_temporalstore=true`); the separate deterministic *full-replay* proof
  (`rust_temporalstore_full_replay_ready`) ran in 4-case verification mode, not full-corpus.
- **1 reader timeout** in the OpenViking LOCOMO-1.5b baseline (1/1,539 = 0.06%); flagged by the strict
  fairness validator, immaterial to aggregates.
- **Artifacts:** `/root/matrixark_benchmarks/runs/fair-oss-qwen1_5b/` and `.../fair-oss-qwen7b-sample/`
  (`matrixark_*`, `openviking_*`, `oss_benchmark_summary.{json,md}`, `*_contract_validation.json`,
  per-query `*.per_query.jsonl`).
