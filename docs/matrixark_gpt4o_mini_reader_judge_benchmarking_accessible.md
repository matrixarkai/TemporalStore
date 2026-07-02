# MatrixArk GPT-4o Mini Reader/Judge Benchmarking

This doc explains how to use OpenAI API models, especially `gpt-4o-mini`, as
MatrixArk benchmark reader and judge models for LOCOMO and LongMemEval-style
runs.

Important: ChatGPT Free/Plus/Pro access is different from OpenAI API access.
There is no guaranteed free GPT-4 API model for benchmarking. API usage normally
requires an OpenAI Platform account, API key, and billing/credits.

Official references:

- OpenAI API quickstart: <https://developers.openai.com/api/docs/quickstart>
- OpenAI API authentication overview: <https://developers.openai.com/api/reference/overview/>
- GPT-4o mini model page: <https://developers.openai.com/api/docs/models/gpt-4o-mini>
- OpenAI API pricing: <https://openai.com/api/pricing/>

## Why Use GPT-4o Mini

For MatrixArk vs VikingMem-style benchmarking, we need to separate:

```text
retrieval quality  = did TemporalStore find the right context?
reader quality     = did the model turn that context into the right answer?
judge quality      = did the evaluator score the answer fairly?
```

Our deterministic reader is useful for CI and debugging, but it is not
paper-style benchmark parity. A stronger OpenAI-compatible reader/judge path is
needed when comparing with systems that use strong LLM readers or judges.

`gpt-4o-mini` is a reasonable benchmark reader/judge option because it is
OpenAI-compatible, low cost compared with larger models, and strong enough for
many answer extraction and judge tasks.

## Get An OpenAI API Key

1. Open <https://platform.openai.com/>.
2. Sign in or create an OpenAI Platform account.
3. Go to **Dashboard -> API keys**.
4. Create a new secret key.
5. Copy it once and store it securely.
6. Go to **Billing** and add payment method or credits if required.

Do not commit the key to Git.

Linux/macOS/WSL:

```bash
export OPENAI_API_KEY="sk-..."
```

PowerShell:

```powershell
$env:OPENAI_API_KEY="sk-..."
```

## Recommended Benchmark Env

For MatrixArk benchmarking:

```bash
export MATRIXARK_READER_PROVIDER=openai
export MATRIXARK_READER_MODEL=gpt-4o-mini
export MATRIXARK_JUDGE_PROVIDER=openai
export MATRIXARK_JUDGE_MODEL=gpt-4o-mini
export OPENAI_API_KEY="sk-..."
```

If you want to keep ingestion and retrieval fully local but use OpenAI only for
answering/judging, keep:

```bash
export MATRIXARK_MCP_BACKEND=temporalstore-direct
export MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:18000
export MATRIXARK_TEMPORALSTORE_NAMESPACE=deploy_ns
export MATRIXARK_TEMPORALSTORE_TABLE=deploy_table
export MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:benchmark:gpt4o-mini
```

## LOCOMO Command Shape

Use C++ TemporalStore-backed retrieval and GPT-4o-mini reader/judge:

```bash
cd <repo>

python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset locomo \
  --data-path /root/matrixark_benchmarks/data/locomo10.json \
  --artifact-dir /root/matrixark_benchmarks/artifacts/gpt4o_mini_reader_judge \
  --artifact-prefix locomo_cpp_gpt4o_mini_reader_judge_YYYYMMDD \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-prefix matrixark:benchmark:locomo:gpt4o-mini:YYYYMMDD \
  --batch-size 20 \
  --max-context-tokens 1200 \
  --checkpoint-interval 50
```

## LongMemEval_s Command Shape

Use the official cleaned local file when available:

```bash
cd <repo>

python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset longmemeval_s \
  --data-path /root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json \
  --artifact-dir /root/matrixark_benchmarks/artifacts/gpt4o_mini_reader_judge \
  --artifact-prefix longmemeval_s_cpp_gpt4o_mini_reader_judge_YYYYMMDD \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-prefix matrixark:benchmark:longmemeval:gpt4o-mini:YYYYMMDD \
  --batch-size 20 \
  --max-message-chars 800 \
  --max-context-tokens 1200 \
  --checkpoint-interval 50
```

LongMemEval_s is large. The checkpoint interval matters because it writes
partial artifacts while the run is still in progress.

## Required Artifacts

Every run should write:

```text
result.json
report.json
report.md
hypotheses.jsonl
context_packs.jsonl
judge.jsonl
progress.json
```

The benchmark report should keep MatrixArk metrics separate from VikingMem
paper numbers until the same dataset, reader model, judge model, prompt, and
scoring protocol are matched.

## Metrics To Watch

Core quality:

```text
context_recall
evidence_session_recall
answer_support_hit
final_judge_score
answer_quality_under_budget
```

Token efficiency:

```text
used_context_tokens
answer_bearing_token_density
judge_score_per_1k_tokens
dropped_duplicate_tokens
dropped_stale_tokens
dropped_low_score_tokens
dropped_over_budget_tokens
```

Acceptance gates:

```text
compression_answer_hidden_count == 0
all canonical artifacts written
reader/judge provider recorded as openai
reader/judge model recorded as gpt-4o-mini
```

## Reader/Judge Prompt Policy

Reader should be question-type aware:

```text
date            -> answer date only, cite exact turn
current-state   -> latest entity state plus stale blocker if relevant
multi-hop       -> combine evidence across sessions/entities
why/emotion     -> extract reason or feeling only
list/name       -> compact list
fact            -> short direct answer
```

Judge should score the final answer against the reference answer and selected
evidence. It should not reward unsupported guesses.

## Cost Control

To reduce API spend:

1. Run deterministic reader first for smoke tests.
2. Run GPT-4o-mini on a small LOCOMO subset.
3. Use Batch API where possible for offline benchmark judging.
4. Keep `max_context_tokens` fixed for fair budget curves.
5. Save all artifacts so failed runs do not need full reruns.

Suggested budget curve:

```text
500
1000
2000
4000
8000 tokens
```

Report:

```text
same budget, higher score
same score, fewer tokens
quality per 1K tokens
```

## Current Gap

MatrixArk already has C++ TemporalStore-backed extraction, ingestion, retrieval,
ContextPack audit, token-efficiency reporting, and checkpoint artifacts.

The main remaining gap vs VikingMem-style reported scores is reader/judge
parity:

```text
deterministic reader  -> CI/debug only
gpt-4o-mini reader    -> benchmark-quality answer generation
gpt-4o-mini judge     -> paper-style scoring path
```

Do not claim direct VikingMem score parity until the full official datasets and
matched reader/judge protocol have run successfully.
