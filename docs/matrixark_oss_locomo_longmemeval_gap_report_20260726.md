# MatrixArk OSS LoCoMo / LongMemEval Benchmark Gap Report

Date: 2026-07-26

## Goal

Use local open-source readers to benchmark MatrixArk / TemporalStore memory retrieval against OpenViking / VikingMem-style baselines on LoCoMo and LongMemEval, measure token savings and answer quality, and close the gaps until the comparison is fair.

This report is intentionally conservative: it records what ran locally, what improved, and what still blocks an apples-to-apples VikingMem / OpenViking claim.

## What Changed In This Pass

- Added OpenAI-compatible `/v1/embeddings` support to `tools/openai_compatible_hf_reader.py` using deterministic local hash embeddings.
- Added causal language model support for Qwen-style OSS readers through the same OpenAI-compatible local server.
- Added answer normalization and temporal fallback handling for common date formats and "not enough context" responses.
- Fixed LoCoMo evidence-window source expansion so `speaker:line` evidence refs match the actual dialogue text candidates instead of silently missing.
- Kept benchmark scoring fail-closed: paper-comparable claims remain disabled until full datasets and external baselines run under the same budget/model config.

## Local OSS Readers

| Reader | Endpoint | Status | Notes |
|---|---:|---|---|
| deepset/minilm-uncased-squad2 | `127.0.0.1:18086` | Working | Fast extractive QA diagnostic reader. |
| Qwen/Qwen2.5-0.5B-Instruct | `127.0.0.1:18087` | Working | Local causal-LM reader; slower but exercises OSS generation path. |
| vLLM | n/a | Blocked | Python package/import state is inconsistent locally; no reliable vLLM service yet. |
| Ollama/Qwen larger models | n/a | Not used in final evidence | Use next for stronger full-run reader quality once model serving is stable. |

## MatrixArk Results Collected

| Run | Cases | Retrieval Hit@K | Reader Hit | Token Reduction | Retrieval p95 | Reader p95 | Backend Evidence | Claim Status |
|---|---:|---:|---:|---:|---:|---:|---|---|
| MiniLM LoCoMo 3 conversations | 387 | 1.000 | 0.388 | 33.54% | 1.47 ms | 228.04 ms | Rust TemporalStore ready | Diagnostic |
| MiniLM LongMemEval_s 50 | 50 | 0.980 | 0.560 | 98.59% | 250.06 ms | 305.90 ms | Rust TemporalStore ready | Diagnostic |
| Qwen LoCoMo tiny | 2 | 1.000 | 1.000 | 0.00% | 4.40 ms | 11.97 s | Rust TemporalStore ready | Smoke only |
| Qwen LongMemEval_s tiny | 2 | 1.000 | 1.000 | 93.03% | 233.81 ms | 25.58 s | Python-only diagnostic | Smoke only |

Notes:

- The LoCoMo tiny token reduction is 0% because the run intentionally used a gold evidence window. It validates reader/retrieval plumbing, not savings.
- The Qwen LongMemEval tiny run was Python-only because the Rust LongMemEval harness failed readiness on that tiny rerun. That must be fixed before using it as backend evidence.
- The MiniLM runs are useful engineering diagnostics, but MiniLM is too weak to claim final LLM quality against VikingMem/OpenViking.

## OpenViking / VikingMem Status

OpenViking source is present at `/root/src/github-services/OpenViking`. The server progressed through Python dependency installation and app startup wiring, but local execution is blocked by the native AGFS/RAGFS binding:

```text
ImportError: ragfs_python native library is not available: Rust binding not available
```

The binding crate requires Rust `1.91.1`, while the available WSL toolchain is Rust/Cargo `1.75.0` and no working `rustup` is currently installed. Until that native binding builds or a compatible prebuilt wheel is available, there is no valid local OpenViking/VikingMem benchmark number to compare against.

## Current Gaps

1. **External baseline gap:** OpenViking/VikingMem cannot run locally yet because `ragfs_python` is missing.
2. **Reader strength gap:** Qwen 0.5B works but is slow and small; full comparison should use Qwen 7B/14B or another stronger OSS reader through Ollama or vLLM.
3. **Backend readiness gap:** Qwen LongMemEval tiny exposed a Rust backend readiness failure in the LongMemEval harness; this needs a real fix, not a Python-only workaround.
4. **Token-savings methodology gap:** Savings must be computed against the same source universe and token budget. Gold evidence windows are not valid savings evidence.
5. **Paper-comparable scale gap:** Full LoCoMo 1,542 questions and LongMemEval_s 500 records have not run end-to-end with the same reader, token budget, and baseline.

## Next Gap-Closure Steps

1. Install or vendor a Rust `1.91.1+` toolchain for OpenViking `ragfs_python`, or obtain a compatible prebuilt native binding.
2. Start OpenViking with the same local OSS reader/embedding endpoint and run import/eval on the same LoCoMo and LongMemEval subsets first.
3. Fix the Rust LongMemEval harness readiness issue for tiny Qwen runs.
4. Run full LoCoMo and LongMemEval_s with one stronger OSS reader, same retrieval budget, same threshold profile, and no Python-only diagnostic mode.
5. Only then publish MatrixArk vs OpenViking/VikingMem quality and token-savings numbers.

## Evidence Files

- `/tmp/matrixark_hfqa_locomo_3conv_e16_w2_afterfix.json`
- `/tmp/matrixark_hfqa_longmem_50_e16_afterfix.json`
- `/tmp/matrixark_qwen_locomo_tiny_postfix2_20260726.json`
- `/tmp/matrixark_qwen_longmem_tiny_pythononly_20260726.json`
