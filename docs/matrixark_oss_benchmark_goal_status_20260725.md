# MatrixArk OSS Memory Benchmark Goal Status - 2026-07-25

## Goal

Run LoCoMo and LongMemEval benchmarks with OSS models, compare MatrixArk/TemporalStore against OpenViking/VikingMem and other memory baselines on token savings and answer quality, then close gaps until MatrixArk is competitive or blockers are explicit.

## Current Local MatrixArk Evidence

Local subset runs used Rust TemporalStore retrieval plus the local OpenAI-compatible Qwen2.5-0.5B-Instruct Transformers server.

| Dataset | Cases | Retrieval hit | OSS reader hit | Token reduction | Retrieval p95 | Reader p95 |
|---|---:|---:|---:|---:|---:|---:|
| LoCoMo tiny | 2 | 1.0 | 0.0 | 50.21% | 1.73 ms | 3466.13 ms |
| LongMemEval_s tiny | 2 | 1.0 | 0.0 | 99.53% | 23.11 ms | 4663.61 ms |

Interpretation:

- TemporalStore retrieval is returning answer-bearing evidence on these subset gates.
- The local Qwen2.5-0.5B reader is not strong enough for credible answer-quality claims. It failed even when the answer was in the first retrieved evidence line.
- Token-savings numbers are valid for the subset evidence pack size, but not yet paper-comparable.

## OpenViking / VikingMem Baseline Status

OpenViking is now available locally at `/root/src/github-services/OpenViking` from the GitHub archive.

Relevant baseline paths:

- `benchmark/locomo/openviking/README.md`
- `benchmark/locomo/openviking/import_to_ov.py`
- `benchmark/locomo/openviking/run_eval.py`
- `benchmark/longmemeval/openviking/README.md`
- `benchmark/longmemeval/openviking/import_to_ov.py`
- `benchmark/longmemeval/openviking/run_eval.py`

OpenViking's own README reports LoCoMo agent integrations reaching about 80-83% accuracy with OpenViking and input token reductions of 34.3-91.0%. Those are public reference numbers, not yet a local apples-to-apples MatrixArk run.

Local OpenViking benchmark still requires:

- A running OpenViking server, normally on `localhost:1933`.
- Local or configured model providers for embedding, extraction, rerank, answering, and judging.
- Importing the same LoCoMo / LongMemEval_s datasets into OpenViking user spaces.
- Running its `run_eval.py`, `judge.py`, and `stat_judge_result.py` with the same reader model/token budget policy used for MatrixArk.

## OSS Model Readiness

Available:

- `qwen2.5-0.5b-instruct`: installed and runnable locally, but not adequate as a benchmark answer reader.
- `tiny-gpt2`: smoke-only, not benchmark quality.

Attempted but not ready:

- `qwen2.5-1.5b-instruct`: config/tokenizer files landed, but `model.safetensors` did not complete. Direct Hugging Face transfer was slow and unstable.
- `distilbert-base-cased-distilled-squad`: tokenizer/config partial files landed, but weights did not complete.

## Gap Closure Plan

1. Install a stronger local OSS reader:
   - Preferred: Qwen2.5 1.5B or 3B via Ollama, Transformers, or vLLM.
   - Fallback: a local extractive QA model if generative OSS models remain blocked.
2. Rerun MatrixArk subset gates until both retrieval hit and OSS reader hit are non-zero.
3. Run full LoCoMo and LongMemEval_s with the same token budget and reader settings.
4. Bring up local OpenViking and run its official LoCoMo / LongMemEval scripts on the same datasets.
5. Compare answer quality, retrieved evidence hit rate, input/context tokens, token reduction, ingestion latency, retrieval latency, and reader latency.

## Current Completion State

The goal is not complete. MatrixArk retrieval is promising on subset evidence, but local OSS reader quality and local OpenViking/VikingMem executable comparison remain open.

