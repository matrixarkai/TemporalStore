# MatrixArk OSS LoCoMo / LongMemEval Benchmark Goal Update

Date: 2026-07-26

## Goal Status

Goal is not complete yet. This pass moved the local OSS reader path from "model is installed but reader quality is weak" to a working local extractive QA reader for small TemporalStore-backed benchmark slices.

The remaining gaps are now clearer:

- Full 1,542-question LoCoMo and 500-record LongMemEval_s are not complete.
- Local OpenViking/VikingMem apples-to-apples server baseline is not complete.
- LoCoMo needs neighbor/dialogue expansion around annotated evidence turns, especially image/resource turns whose answer appears in a nearby follow-up turn.
- Paper-comparable claims are still blocked until the full datasets run with the same reader, token budget, storage mode, threshold profile, and judge.

## Implemented

- `tools/openai_compatible_hf_reader.py` now supports `--task question-answering` for SQuAD-style OSS extractive readers.
- The reader can run `deepset/minilm-uncased-squad2` behind an OpenAI-compatible `/v1/chat/completions` API.
- QA mode chunks retrieved context before model inference, so relevant evidence below the first BERT window can still be answered.
- The reader normalizes benchmark-critical answer spans:
  - relative date spans such as `yesterday`;
  - relative year spans such as `last year`;
  - degree phrases;
  - commute duration phrases such as `45 minutes each way`.

## Local OSS Reader

Endpoint used:

```bash
python3 tools/openai_compatible_hf_reader.py \
  --host 127.0.0.1 \
  --port 18086 \
  --model deepset/minilm-uncased-squad2 \
  --task question-answering \
  --max-length 512
```

This is still a small extractive QA reader, not a strong Qwen 7B/14B reader. It is useful for validating the retrieval and token-saving path while larger OSS model installs remain expensive.

## Latest Local Results

| Run | Cases | Retrieval hit | Reader hit | Token reduction | Retrieval p95 | Reader p95 | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| LoCoMo tiny compact | 2 | 1.0 | 0.5 | 50.21% | 1.15 ms | 201.11 ms | Miss exposes neighbor evidence gap around visual/source turn. |
| LoCoMo tiny wider neighbor | 2 | 1.0 | 1.0 | 45.44% | 1.10 ms | 335.14 ms | Goes green when nearby dialogue around evidence is included. |
| LongMemEval_s tiny | 2 | 1.0 | 1.0 | 99.53% | 12.74 ms | 209.75 ms | Chunked QA fixes answer below first BERT window. |

## Per-Query Evidence

LoCoMo compact:

- `conv_26-q1`: expected `7 May 2023`, reader returned `7 May 2023`, hit true.
- `conv_26-q2`: expected `2022`, compact reader returned `1:56 pm on 8 May, 2023`, hit false.

LoCoMo wider neighbor:

- `conv_26-q1`: expected `7 May 2023`, reader returned `7 May 2023`, hit true.
- `conv_26-q2`: expected `2022`, reader returned `2022`, hit true.

LongMemEval_s tiny:

- `conversation_1-q1`: expected `Business Administration`, reader returned `Business Administration`, hit true.
- `conversation_2-q1`: expected `45 minutes each way`, reader returned `45 minutes each way`, hit true.

## Comparison To OpenViking / VikingMem

Local OpenViking is present under `/root/src/github-services/OpenViking`, and the LoCoMo / LongMemEval benchmark scripts are available there. A true local OpenViking baseline still requires running its server path and importing the same corpora.

Do not compare the tiny MatrixArk subset above directly against OpenViking paper numbers. Public OpenViking/VikingMem figures should only be treated as references until the local server baseline is run under the same model and budget.

## Next Gap-Closing Steps

1. Add retrieval-side neighbor expansion for LoCoMo evidence refs, especially image/resource turns where the answer is often in the next dialogue turn.
2. Run a larger LoCoMo subset without gold-only shortcuts and check category-level misses.
3. Run the full 500 LongMemEval_s records with the same MiniLM QA endpoint as a fast local gate.
4. Bring up local OpenViking server and run its import/eval scripts with the same local reader profile.
5. Install or finish downloading a stronger OSS reader, preferably Qwen 7B/14B via Ollama or vLLM, then repeat the same reports before making quality claims.

