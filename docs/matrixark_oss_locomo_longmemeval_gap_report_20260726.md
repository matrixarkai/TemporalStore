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
- Unblocked local OpenViking enough to run tiny LoCoMo baselines: built `ragfs_python`, rebuilt the native vector engine without the local tcmalloc crash, started OpenViking on `127.0.0.1:1934`, imported LoCoMo sessions, executed MiniLM and Qwen VikingBot evals, and probed direct OpenViking search/recall APIs.
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

OpenViking source is present at `/root/src/github-services/OpenViking`. The previous native binding blocker is no longer the current blocker:

- Installed a local Rust toolchain through `rustup` and built `ragfs_python` from `crates/ragfs-python`.
- Built the OpenViking vector engine in place.
- Fixed the local vector-store crash by rebuilding LevelDB without the local tcmalloc link path (`OV_DISABLE_TCMALLOC=1`). Before that, `PersistStore(...)` aborted on an invalid free in tcmalloc.
- Started OpenViking on `127.0.0.1:1934` with local storage at `/tmp/openviking_matrixark_oss/data`.
- Imported LoCoMo sample `conv-26`, sessions 1-4, through the user API key path. Import completed for 4 sessions with 0 failed sessions and about 12.27 seconds total import time.
- Ran VikingBot eval on two LoCoMo questions with the local OpenAI-compatible MiniLM endpoint.

OpenViking tiny eval results:

| Run | Cases | Reader Hit | Token Usage Evidence | Time Cost | Claim Status |
|---|---:|---:|---|---:|---|
| OpenViking/VikingBot MiniLM LoCoMo tiny | 2 | 0.000 | Token counters returned 0 for both rows | 2.59 s / 1.51 s | Baseline unblocked, quality not competitive |
| OpenViking/VikingBot Qwen LoCoMo tiny | 2 | 0.000 | Token counters returned 0 for both rows | 43.73 s / 53.61 s | Qwen endpoint reached, but VikingBot tool loop failed |
| OpenViking direct recall after Qwen memory-enabled import | 1 session imported | 0 retrievable memories | Import token evidence: 363 embedding tokens, 0 LLM tokens | 24.69 s import | Extraction produced empty memory diff |

The MiniLM run answered both questions as `2026` for expected answers `7 May 2023` and `2022`. The Qwen VikingBot run reached the local Qwen endpoint, but answered with a fragment of VikingBot tool instructions instead of using tools/retrieval. Direct OpenViking recall is now technically callable, but the memory-enabled Qwen import produced `memories_extracted: {}` and `memory_diff.json` contained no adds, updates, or deletes. So the current OpenViking blocker has moved from native setup to baseline quality/extraction: the server runs, import/commit succeeds, but local OSS extraction does not create recallable memory entries yet.

## Current Gaps

1. **External baseline quality gap:** OpenViking now runs locally, but the tiny VikingBot MiniLM/Qwen baselines answered 0/2 questions correctly.
2. **OpenViking extraction gap:** With memory extraction enabled and Qwen configured, session commit completed but extracted 0 memories, leaving direct recall empty.
3. **Token accounting gap:** OpenViking returned zero LLM token counters in these local runs, so token savings versus MatrixArk cannot be computed from that output yet.
4. **Reader/tool-loop gap:** Qwen 0.5B works as a direct OpenAI-compatible chat endpoint, but it does not follow VikingBot's tool loop well enough to answer through VikingBot. Full comparison should use Qwen 7B/14B or another stronger OSS reader through Ollama or vLLM.
5. **Backend readiness gap:** Qwen LongMemEval tiny exposed a Rust backend readiness failure in the LongMemEval harness; this needs a real fix, not a Python-only workaround.
6. **Token-savings methodology gap:** Savings must be computed against the same source universe and token budget. Gold evidence windows are not valid savings evidence.
7. **Paper-comparable scale gap:** Full LoCoMo 1,542 questions and LongMemEval_s 500 records have not run end-to-end with the same reader, token budget, and baseline.

## Next Gap-Closure Steps

1. Fix or bypass OpenViking's OSS memory extraction so a tiny committed LoCoMo session creates recallable event/entity memories.
2. Move the OpenViking baseline from Qwen 0.5B tool-loop smoke to a stronger local Ollama/vLLM reader, or use direct OpenViking retrieval plus the same MatrixArk reader for a retrieval-only baseline.
3. Fix OpenViking token accounting for local OSS endpoints, or collect prompt/completion token estimates externally with the same tokenizer used for MatrixArk.
4. Fix the Rust LongMemEval harness readiness issue for tiny Qwen runs.
5. Run OpenViking and MatrixArk on the same LoCoMo and LongMemEval subsets with the same reader, same token budget, and same threshold profile.
6. Scale only after tiny runs show nonzero token accounting and nontrivial answer quality.
7. Only then publish MatrixArk vs OpenViking/VikingMem quality and token-savings numbers.

## Evidence Files

- `/tmp/matrixark_hfqa_locomo_3conv_e16_w2_afterfix.json`
- `/tmp/matrixark_hfqa_longmem_50_e16_afterfix.json`
- `/tmp/matrixark_qwen_locomo_tiny_postfix2_20260726.json`
- `/tmp/matrixark_qwen_longmem_tiny_pythononly_20260726.json`
- `/tmp/openviking_matrixark_oss/results/import_success.csv`
- `/tmp/openviking_matrixark_oss/results/locomo_qa_result.csv`
- `/tmp/openviking_matrixark_oss/results/locomo_qa_result_qwen.csv`
- `/tmp/openviking_matrixark_oss/results_qwen_memory/import_success.csv`
- `/tmp/openviking_matrixark_oss/ov_qwen_memory_fresh.conf`
