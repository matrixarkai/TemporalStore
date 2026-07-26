# MatrixArk OSS LoCoMo / LongMemEval Benchmark Gap Report

Date: 2026-07-26

## Goal

Use local open-source readers to benchmark MatrixArk / TemporalStore memory retrieval against OpenViking / VikingMem-style baselines on LoCoMo and LongMemEval, measure token savings and answer quality, and close the gaps until the comparison is fair.

This report is intentionally conservative: it records what ran locally, what improved, and what still blocks an apples-to-apples VikingMem / OpenViking claim.

## What Changed In This Pass

- Completed a full LoCoMo Rust TemporalStore replay using the local canonical dataset: 1,542 evaluated questions, 1,541 retrieval hits, `hit_at_k=0.9993514915693904`, with `rust_temporalstore_full_replay_ready=true`.
- Added progress artifacts for long OSS reader runs so hung reader/model calls are visible before the final JSON report is written.
- Added curl-based wall-time limits for OpenAI-compatible OSS reader calls and best-effort Ollama model unload on local timeout, after `SIGALRM` proved insufficient for stuck Ollama requests.
- Added an in-process `transformers://...` OSS reader backend for cached Hugging Face models, avoiding the flaky local Ollama HTTP path when CPU-only OSS smoke data is needed.
- Verified cached `Qwen/Qwen2.5-0.5B-Instruct` loads locally on CPU and can answer a simple prompt through Transformers.
- Ran a 5-question LoCoMo OSS-reader sample with Qwen 0.5B over the full Rust TemporalStore replay: retrieval hit remained 1.0 and token reduction was 94.11%, but reader hit was 0.0, so the current gap is reader/model quality and compact evidence packing rather than TemporalStore retrieval.
- Added reader-aware evidence packing that labels selected sources and fills the reader context from compact per-source snippets instead of slicing one body-only string. On the same 5-question LoCoMo sample, Qwen 0.5B reader hit improved from 0.0 to 0.2 while preserving 94.11% token reduction.
- Ran a 5-record LongMemEval_s OSS-reader sample with Qwen 0.5B: retrieval hit was 1.0, token reduction was 98.01%, and reader hit was 0.2. Reader-aware packing kept the same hit rate but changed which fact the small model answered correctly, confirming the remaining gap is model/ranking quality.
- Updated the artifact summarizer to understand nested Rust full-replay reports and to fail closed on tiny samples. A run is no longer marked paper-comparable unless it reaches the dataset-scale minimum: 1,542 LoCoMo cases or 500 LongMemEval_s records.
- Tried cached Transformers Qwen 1.5B as a stronger in-process reader, but the local Hugging Face cache is incomplete, so it cannot be used offline yet.
- Added OpenAI-compatible `/v1/embeddings` support to `tools/openai_compatible_hf_reader.py` using deterministic local hash embeddings.
- Added causal language model support for Qwen-style OSS readers through the same OpenAI-compatible local server.
- Added answer normalization and temporal fallback handling for common date formats and "not enough context" responses.
- Fixed LoCoMo evidence-window source expansion so `speaker:line` evidence refs match the actual dialogue text candidates instead of silently missing.
- Unblocked local OpenViking enough to run tiny LoCoMo baselines: built `ragfs_python`, rebuilt the native vector engine without the local tcmalloc crash, started OpenViking on `127.0.0.1:1934`, imported LoCoMo sessions, executed MiniLM and Qwen VikingBot evals, and probed direct OpenViking search/recall APIs.
- Added direct diagnostic baselines for OpenViking archived LoCoMo messages and LongMemEval source sessions so retrieval, token savings, and reader quality can be measured even while OpenViking memory extraction returns empty memory records.
- Added an OSS reader memory-capability probe so weak/smoke readers are not accidentally used for competitive MatrixArk vs OpenViking/VikingMem claims.
- Installed local Ollama and pulled `qwen2.5:1.5b`; verified Ollama OpenAI-compatible `/v1` serving on `127.0.0.1:11434`.
- Tightened the OSS reader capability prompt so temporal questions require explicit date/year normalization and personal-fact questions copy the exact answer span, including `degree in X -> X`.
- Propagated the same temporal/fact-safe OSS reader prompt into the MatrixArk benchmark runner and the OpenViking direct diagnostic baselines so the comparison no longer mixes reader instructions.
- Added `tools/check_oss_model_readiness.py` so future benchmark continuations can record whether a target OSS reader is actually installed/callable before running quality gates.
- Added `tools/run_oss_memory_benchmarks_when_ready.sh`, a fail-closed driver that runs model readiness, reader capability, MatrixArk LoCoMo/LongMemEval, and OpenViking direct diagnostic baselines only after the target OSS reader is installed and passes the reader gate.
- Added `tools/summarize_oss_memory_benchmark_artifacts.py` to render MatrixArk/OpenViking JSON artifacts into one token-savings and reader-quality table without upgrading diagnostic runs into paper-comparable claims.
- Added `tools/diagnose_openviking_memory_extraction.py` to turn OpenViking memory-extraction logs/config/import CSVs into a fail-closed baseline readiness report.
- Attempted `qwen2.5:7b` through the configured WSL network proxy; the blocking pull reached only about 475 MB / 4.7 GB after the 15 minute command cap. A background pull is still running and had reached about 1.0 GB / 4.7 GB, with roughly two hours still estimated at the observed throughput.
- Kept benchmark scoring fail-closed: paper-comparable claims remain disabled until full datasets and external baselines run under the same budget/model config.

## Local OSS Readers

| Reader | Endpoint | Status | Notes |
|---|---:|---|---|
| deepset/minilm-uncased-squad2 | `127.0.0.1:18086` | Working | Fast extractive QA diagnostic reader. |
| Qwen/Qwen2.5-0.5B-Instruct | `127.0.0.1:18087` | Working | Local causal-LM reader; slower but exercises OSS generation path. |
| Ollama/qwen2.5:1.5b | `127.0.0.1:11434/v1` | Working, gate-passed | Installed locally and passes the four-case OSS reader capability gate after extractive prompt tightening. Still smaller than the intended final Qwen 7B/14B reader. |
| Ollama/qwen2.5:7b | n/a | Download in progress | Readiness check shows the model is not installed yet; pull through proxy reached about 1.0 GB / 4.7 GB and remains active. |
| Transformers/Qwen2.5-0.5B | in-process `transformers://Qwen/Qwen2.5-0.5B-Instruct` | Working, CPU smoke only | Loads from local Hugging Face cache without a model server; useful for bounded OSS smoke runs, but not competitive quality. |
| Transformers/Qwen2.5-1.5B | in-process `transformers://Qwen/Qwen2.5-1.5B-Instruct` | Cache incomplete | The local cache exists but is missing required files; the reader reports that it cannot load offline. |
| vLLM | n/a | Installed/importable, no CUDA device | `vllm` imports locally, but WSL reports `torch.cuda.is_available() == false`, so it was not used as the stable local full-benchmark reader path. |

## OSS Reader Capability Gate

| Reader | Cases | Hit Rate | p95 | Gate Status | Notes |
|---|---:|---:|---:|---|---|
| deepset/minilm-uncased-squad2 | 4 | 1.000 | 742.57 ms | competitive_reader_ready | Passes the tiny temporal/personal-fact probe, but remains an extractive diagnostic reader rather than final paper-quality LLM. |
| Qwen/Qwen2.5-0.5B-Instruct | 4 | 0.750 | 5.41 s | reader_smoke_only | Misses the relative-date LoCoMo probe by answering the conversation timestamp instead of `7 May 2023`; keep as smoke-only until a stronger Qwen/Ollama/vLLM reader is live. |
| Ollama/qwen2.5:1.5b | 4 | 1.000 | 1.52 s | competitive_reader_ready | Passes after the extractive degree-span instruction: `degree in X` means return `X`, not the credential level. |

The gate uses four compact memory QA probes: two LoCoMo temporal questions and two LongMemEval-style personal facts. Default readiness requires at least 0.90 hit rate and p95 under 30 seconds. This does not replace full benchmark scoring; it prevents weak local readers from being mistaken for competitive evidence.

The local full input files are `/root/matrixark_benchmarks/data/locomo10.json` and `/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json`. The local LoCoMo file contains 10 conversations and 1,986 QA cases; LongMemEval_s contains 500 records. The default `/tmp/locomo10.json` and `/tmp/longmemeval_s.json` paths are not present in this environment, so benchmark launches must point at the canonical `/root/matrixark_benchmarks/data` files.

## MatrixArk Results Collected

| Run | Cases | Retrieval Hit@K | Reader Hit | Token Reduction | Retrieval p95 | Reader p95 | Backend Evidence | Claim Status |
|---|---:|---:|---:|---:|---:|---:|---|---|
| Rust TemporalStore full LoCoMo replay | 1,542 | 0.99935 | n/a | n/a | n/a | n/a | Rust TemporalStore full replay ready | Retrieval-ready, reader not included |
| Qwen 0.5B Transformers LoCoMo 5-question compact sample | 5 | 1.000 | 0.000 | 94.11% | 85.47 ms | 19.39 s | Reused full Rust TemporalStore replay | Gap evidence: retrieval/token savings good, reader quality poor |
| Qwen 0.5B Transformers LoCoMo 5-question reader-packed sample | 5 | 1.000 | 0.200 | 94.11% | 101.01 ms | 26.53 s | Reused full Rust TemporalStore replay | Small packing win; still reader/model limited |
| Qwen 0.5B Transformers LongMemEval_s 5-record compact sample | 5 | 1.000 | 0.200 | 98.01% | 205.85 ms | 21.14 s | Rust TemporalStore ready | Gap evidence: retrieval/token savings good, reader quality poor |
| Qwen 0.5B Transformers LongMemEval_s 5-record reader-packed sample | 5 | 1.000 | 0.200 | 98.01% | 209.08 ms | 30.59 s | Rust TemporalStore ready | Packing changed correct fact, not aggregate quality |
| MiniLM LoCoMo 3 conversations | 387 | 1.000 | 0.388 | 33.54% | 1.47 ms | 228.04 ms | Rust TemporalStore ready | Diagnostic |
| MiniLM LongMemEval_s 50 | 50 | 0.980 | 0.560 | 98.59% | 250.06 ms | 305.90 ms | Rust TemporalStore ready | Diagnostic |
| Qwen LoCoMo tiny | 2 | 1.000 | 1.000 | 0.00% | 4.40 ms | 11.97 s | Rust TemporalStore ready | Smoke only |
| Qwen LongMemEval_s tiny | 2 | 1.000 | 1.000 | 93.03% | 233.81 ms | 25.58 s | Python-only diagnostic | Smoke only |
| Ollama Qwen 1.5B LoCoMo tiny | 2 | 1.000 | 0.500 | 0.00% | 4.92 ms | 58.35 s | Python-only diagnostic | Gate failed: reader p95 and one temporal answer miss |
| Ollama Qwen 1.5B LongMemEval_s tiny | 2 | 1.000 | 1.000 | 93.03% | 402.84 ms | 58.43 s | Python-only diagnostic | Gate failed only on reader p95 |
| Ollama Qwen 1.5B LoCoMo tiny, shared prompt | 2 | 1.000 | 0.500 | 0.00% | 3.36 ms | 57.70 s | Python-only diagnostic | Same retrieval; prompt alignment did not fix Qwen 1.5B temporal confusion |
| Ollama Qwen 1.5B LongMemEval_s tiny, shared prompt | 2 | 1.000 | 0.500 | 93.03% | 373.04 ms | 57.57 s | Python-only diagnostic | Prompt alignment fixed the degree case but missed the commute case in this run |

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

| Run | Cases | Retrieval Hit@K | Reader Hit | Token Reduction | Retrieval p95 | Reader p95 | Claim Status |
|---|---:|---:|---:|---:|---:|---:|---|
| OpenViking/VikingBot MiniLM LoCoMo tiny | 2 | n/a | 0.000 | n/a | n/a | 2.59 s / 1.51 s | Baseline unblocked, quality not competitive |
| OpenViking/VikingBot Qwen LoCoMo tiny | 2 | n/a | 0.000 | n/a | n/a | 43.73 s / 53.61 s | Qwen endpoint reached, but VikingBot tool loop failed |
| OpenViking direct recall after Qwen memory-enabled import | 1 session imported | 0.000 | n/a | n/a | n/a | 24.69 s import | Extraction produced empty memory diff |
| OpenViking direct archive retrieval + Qwen LoCoMo tiny | 2 | 1.000 | 0.000 | 52.50% | 0.26 ms | 9.31 s | Retrieval works, Qwen 0.5B answer quality fails |
| OpenViking direct archive retrieval + MiniLM LoCoMo tiny | 2 | 1.000 | 0.500 | 52.50% | 0.49 ms | 227.30 ms | Retrieval works, weak reader misses one temporal answer |
| OpenViking official LongMemEval import/eval smoke | 1 | 0.000 | 0.000 | n/a | n/a | 30.12 s | Import with correct user key completed, but eval retrieved zero memories/context |
| OpenViking-style direct source retrieval + Qwen LongMemEval_s tiny | 2 | 1.000 | 1.000 | 96.87% | 47.16 ms | 26.42 s | Fallback source retrieval works; not OpenViking memory recall |
| OpenViking-style direct source retrieval + MiniLM LongMemEval_s tiny | 2 | 1.000 | 1.000 | 96.87% | 89.42 ms | 5.33 s | Fallback source retrieval works; not OpenViking memory recall |
| OpenViking direct archive retrieval + Ollama Qwen 1.5B LoCoMo tiny | 2 | 1.000 | n/a | 52.50% | 0.54 ms | 63.61 s | Retrieval works; reader latency too high for final claims |
| OpenViking-style direct source retrieval + Ollama Qwen 1.5B LongMemEval_s tiny | 2 | 1.000 | n/a | 96.87% | 76.77 ms | 71.01 s | Source retrieval works; reader latency too high and still not official memory recall |
| OpenViking direct archive retrieval + Ollama Qwen 1.5B LoCoMo tiny, shared prompt | 2 | 1.000 | 0.000 | 52.50% | 0.46 ms | 56.27 s | Prompt-aligned retrieval works; reader still fails both tiny LoCoMo answers |
| OpenViking-style direct source retrieval + Ollama Qwen 1.5B LongMemEval_s tiny, shared prompt | 2 | 1.000 | 0.500 | 96.87% | 63.12 ms | 59.01 s | Prompt-aligned source retrieval works; reader quality/latency still not final-claim ready |

The MiniLM VikingBot run answered both questions as `2026` for expected answers `7 May 2023` and `2022`. The Qwen VikingBot run reached the local Qwen endpoint, but answered with a fragment of VikingBot tool instructions instead of using tools/retrieval. Direct OpenViking recall is now technically callable, but the memory-enabled Qwen import produced `memories_extracted: {}` and `memory_diff.json` contained no adds, updates, or deletes.

The OpenViking memory-extraction diagnosis is now machine-readable. For the local Qwen memory-enabled imports, `memory.extraction_enabled` was true, commit tasks completed, and 3,132 task-level embedding tokens were recorded across five inspected tasks. The configured embedding and VLM `/models` endpoints are both reachable and advertise the expected local models, and a direct VLM `/chat/completions` probe returns `ready`. However, OpenViking still records zero task-level LLM tokens, zero extracted memories, and five `memory_diff.json` files with zero adds, updates, or deletes. The current diagnosis is `messages_archived_and_embedded_but_openviking_did_not_record_chat_completion_usage_for_memory_extraction`: the local endpoint is callable, but official OpenViking memory recall remains not baseline-ready; direct source/archive retrieval is still only a diagnostic fallback.

To separate retrieval from the broken tool-loop/extraction path, this pass added a direct OpenViking archive retrieval diagnostic. It reads the committed OpenViking `messages.jsonl` archive, ranks archived messages for each LoCoMo question, and feeds the same retrieved context into the local OSS reader. That path retrieved both gold evidence refs (`D1:3` and `D1:12`) and reduced context from 360 estimated source tokens to about 171 retrieved tokens per query. However, Qwen 0.5B answered `2023` for both questions and MiniLM only answered the first question (`yesterday`) correctly.

For LongMemEval, the official OpenViking importer initially failed every session with `Missing API Key`; setting `OPENVIKING_API_KEY` and creating a key for `lm_user_208c35a6ff43` fixed the import path for the last four sessions, including the answer session. The official eval still retrieved no memory context: `retrieved_uris` and `context_uris` were empty, token usage was zero, and the generated prompt contained `(No relevant memories found)`. A direct-source fallback over the same LongMemEval tiny file retrieved both answer sessions and answered both questions with Qwen 0.5B and MiniLM, but that is explicitly not OpenViking memory recall evidence. So the OpenViking setup gap is now sharper: raw/session text can support the answers, but local OpenViking memory extraction/indexing is not producing searchable memories.

## Current Gaps

1. **External baseline quality gap:** OpenViking now runs locally, but the tiny VikingBot MiniLM/Qwen baselines answered 0/2 questions correctly.
2. **OpenViking extraction/indexing gap:** With memory extraction enabled and Qwen configured, LoCoMo session commit extracted 0 memories. LongMemEval import completed only after using the correct user API key, but official eval still retrieved zero memories/context.
3. **Token accounting gap:** OpenViking returned zero LLM token counters in VikingBot runs; the direct archive baseline therefore uses the same external whitespace-token estimate as the MatrixArk diagnostics.
4. **Reader/tool-loop gap:** Qwen 0.5B works as a direct OpenAI-compatible chat endpoint, but it does not follow VikingBot's tool loop and fails the stricter reader capability gate. MiniLM passes the tiny gate but is extractive and diagnostic; full comparison should use Qwen 7B/14B or another stronger OSS reader through Ollama or vLLM.
5. **Backend readiness gap:** Qwen LongMemEval tiny exposed a Rust backend readiness failure in the LongMemEval harness; this needs a real fix, not a Python-only workaround.
6. **Token-savings methodology gap:** Savings must be computed against the same source universe and token budget. Gold evidence windows are not valid savings evidence.
7. **Paper-comparable scale gap:** Full LoCoMo 1,542 questions and LongMemEval_s 500 records have not run end-to-end with the same reader, token budget, and baseline.

## Next Gap-Closure Steps

1. Fix OpenViking's OSS memory extraction so a tiny committed LoCoMo session creates recallable event/entity memories; direct archive retrieval is only a diagnostic fallback.
2. Replace Qwen 0.5B/1.5B with a stronger local OSS reader, preferably Qwen 7B/14B through Ollama or vLLM. The `qwen2.5:7b` pull is currently still in progress and limited by proxy/download throughput, not by benchmark code.
3. Keep direct OpenViking/archive/source retrieval as a retrieval-only diagnostic baseline until VikingBot memory extraction is non-empty; compare it against MatrixArk using the same external token estimator and mark it non-paper-comparable.
4. Fix OpenViking/VikingBot token accounting for local OSS endpoints, or collect prompt/completion token estimates externally with the same tokenizer used for MatrixArk.
5. Fix the Rust LongMemEval harness readiness issue for tiny Qwen runs.
6. Run OpenViking and MatrixArk on the same LoCoMo and LongMemEval subsets with the same reader, same token budget, and same threshold profile.
7. Scale only after tiny runs show nonzero retrieval, defensible token accounting, and nontrivial answer quality.
8. Only then publish MatrixArk vs OpenViking/VikingMem quality and token-savings numbers.

## Latest Qwen 1.5B/Ollama Findings

The local Ollama Qwen 1.5B path is now installed and callable, so the OSS model path itself is no longer blocked. It is still not strong enough for competitive LoCoMo/LongMemEval claims:

- Reader gate: 3/4 correct, p95 1.81 s, failed quality because it answered `Bachelor's degree` instead of the requested `Business Administration` fact.
- MatrixArk LoCoMo tiny: retrieval hit 1.0, reader hit 0.5, p95 retrieval 4.92 ms, p95 reader 58.35 s, gate failed. With the shared prompt, retrieval stayed 1.0 and reader hit stayed 0.5, so the remaining issue is model quality rather than a missing prompt instruction.
- MatrixArk LongMemEval tiny: retrieval hit 1.0, reader hit 1.0, token reduction 93.03%, p95 retrieval 402.84 ms, p95 reader 58.43 s, gate failed only on reader latency. With the shared prompt, reader hit varied to 0.5, showing Qwen 1.5B is not stable enough for final claims.
- OpenViking direct LoCoMo archive: retrieval hit 1.0, token reduction 52.50%, p95 retrieval 0.46-0.54 ms, p95 reader 56-64 s depending on prompt.
- OpenViking direct LongMemEval source: retrieval hit 1.0, token reduction 96.87%, p95 retrieval 63-77 ms, p95 reader 59-71 s depending on prompt.
- A reader-only snippet-packing experiment reduced latency on LoCoMo but also reduced reader hit rate, so it was not kept as a production benchmark fix. The next gap is a stronger reader, not more narrow prompt surgery around Qwen 1.5B.

The practical gap is now precise: retrieval can find the tiny evidence on both MatrixArk and direct OpenViking diagnostics, but the local generative reader needs either a faster/stronger serving stack or a larger model before we can run full LoCoMo 1,542-question and LongMemEval_s 500-record comparisons with defensible LLM quality. The active benchmark goal should remain open until a stronger OSS reader passes the capability gate and the full LoCoMo / LongMemEval runs complete against MatrixArk, OpenViking/VikingMem, and the retrieval-only diagnostic baselines under one shared token budget.

## Latest Full-Replay / OSS Reader Progress

The most useful current MatrixArk result is the full Rust TemporalStore LoCoMo replay:

- Report: `/tmp/matrixark_oss_goal_runs/oss_memory_ready_20260726T035752Z/oss_reader_endpoint_20260726T035802Z/locomo_report.rust_temporalstore.json`
- Questions evaluated: 1,542
- Retrieval hits: 1,541
- Retrieval Hit@K: 0.9993514915693904
- Backend flags: `rust_temporalstore_backend_ready=true`, `rust_temporalstore_full_replay_ready=true`

The OSS reader gap remains open:

- Ollama `qwen2.5:1.5b` passes the tiny four-case capability gate, but full-reader runs stall on CPU because abandoned HTTP generations keep the local llama-server busy even after client-side timeout. Curl wall-time limits and Ollama unload cleanup were added, but the Ollama path is still not the preferred full benchmark route on this machine.
- Local vLLM is installed, but this WSL environment has no CUDA device, so vLLM was not used for full LoCoMo/LongMemEval evidence.
- In-process Transformers with cached Qwen 0.5B is stable enough for smoke runs. On a 5-question LoCoMo compact-context sample, it achieved 94.11% token reduction and 1.0 retrieval hit, but 0.0 reader hit. The model returned answers like `not enough context`, `transgender`, `research`, and `2023` where the strict LoCoMo expected terms did not match.
- Reader-aware source labels and compact snippets improved the same LoCoMo sample to 0.2 reader hit without reducing token savings. The improvement is useful but too small for a competitive claim.
- On a 5-record LongMemEval_s sample, compact and reader-packed contexts both produced 1.0 retrieval hit, 98.01% token reduction, and 0.2 reader hit. The small model could answer one exact fact but remained unstable on simple retrieved facts.

So the current gap-closure target is not raw TemporalStore retrieval. It is:

1. stronger OSS reader availability, preferably Qwen 7B/14B through a stable serving stack;
2. compact evidence packing that keeps the exact answer span inside the reader's first context window;
3. full LongMemEval Rust replay readiness under the same runner shape;
4. OpenViking official memory extraction producing non-empty searchable memories rather than only archive/source diagnostic retrieval.

## Current Comparison Summary

The current summary artifact is:

- JSON: `/tmp/matrixark_oss_goal_runs/oss_memory_current_comparison_summary.json`
- Markdown: `/tmp/matrixark_oss_goal_runs/oss_memory_current_comparison_summary.md`

It intentionally marks all rows non-paper-comparable today:

- full MatrixArk LoCoMo Rust replay is retrieval-ready but has no OSS reader pass attached yet;
- Qwen 0.5B samples are too small and reader quality is weak;
- OpenViking rows are direct archive/source diagnostics, not official memory-recall baselines;
- OpenViking official memory extraction still produces empty searchable memories in this local setup.

The summary now fails closed on tiny samples, so a relaxed local smoke threshold cannot accidentally become a published benchmark claim.

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
- `/tmp/openviking_direct_retrieval_locomo_tiny_20260726.json`
- `/tmp/openviking_direct_retrieval_locomo_tiny_minilm_20260726.json`
- `/tmp/openviking_matrixark_oss/results_longmem/import_success_last4.csv`
- `/tmp/openviking_matrixark_oss/results_longmem/longmem_eval_last4_userkey_config.csv`
- `/tmp/openviking_direct_source_longmem_tiny_qwen_20260726.json`
- `/tmp/openviking_direct_source_longmem_tiny_minilm_20260726.json`
- `/tmp/oss_reader_capability_minilm_20260726.json`
- `/tmp/oss_reader_capability_qwen05_20260726.json`
- `/tmp/oss_reader_capability_qwen25_15b_ollama_promptfix2_20260726.json`
- `/tmp/openviking_direct_retrieval_locomo_tiny_qwen25_15b_ollama_20260726.json`
- `/tmp/openviking_direct_source_longmem_tiny_qwen25_15b_ollama_20260726.json`
- `/tmp/matrixark_qwen25_15b_locomo_tiny_validation_20260726.json`
- `/tmp/matrixark_qwen25_15b_locomo_tiny_benchmark_20260726.json`
- `/tmp/matrixark_qwen25_15b_longmem_tiny_validation_20260726.json`
- `/tmp/matrixark_qwen25_15b_longmem_tiny_benchmark_20260726.json`
- `/tmp/matrixark_qwen25_15b_locomo_tiny_validation_promptfix_20260726.json`
- `/tmp/matrixark_qwen25_15b_locomo_tiny_benchmark_promptfix_20260726.json`
- `/tmp/matrixark_qwen25_15b_longmem_tiny_validation_promptfix_20260726.json`
- `/tmp/matrixark_qwen25_15b_longmem_tiny_benchmark_promptfix_20260726.json`
- `/tmp/openviking_direct_retrieval_locomo_tiny_qwen25_15b_ollama_promptfix_20260726.json`
- `/tmp/openviking_direct_source_longmem_tiny_qwen25_15b_ollama_promptfix_20260726.json`
- `/tmp/ollama_pull_qwen25_7b_20260726.log`
- `/tmp/ollama_pull_qwen25_7b_bg_20260726.log`
- `/tmp/oss_model_readiness_qwen25_7b_20260726.json`
- `/tmp/oss_memory_benchmark_summary_20260726.json`
- `/tmp/oss_memory_benchmark_summary_20260726.md`
- `/tmp/openviking_memory_extraction_diagnosis_20260726.json`
- `/tmp/openviking_memory_extraction_diagnosis_probe_20260726.json`
- `/tmp/openviking_memory_extraction_diagnosis_chat_probe_20260726.json`
- `/tmp/openviking_memory_extraction_diagnosis_task_evidence_20260726.json`
- `/tmp/matrixark_oss_goal_runs/oss_memory_ready_20260726T035752Z/oss_reader_endpoint_20260726T035802Z/locomo_report.rust_temporalstore.json`
- `/tmp/matrixark_oss_goal_runs/qwen05_transformers_locomo5_ctx2k_report.json`
- `/tmp/matrixark_oss_goal_runs/qwen05_transformers_locomo5_ctx2k_readerpack_report.json`
- `/tmp/matrixark_oss_goal_runs/qwen05_transformers_longmem5_ctx2k_report.json`
- `/tmp/matrixark_oss_goal_runs/qwen05_transformers_longmem5_ctx2k_readerpack_report.json`
- `/tmp/matrixark_oss_goal_runs/qwen05_transformers_oneq_ctx2k_report.json`
- `/tmp/matrixark_oss_goal_runs/oss_memory_current_comparison_summary.json`
- `/tmp/matrixark_oss_goal_runs/oss_memory_current_comparison_summary.md`
