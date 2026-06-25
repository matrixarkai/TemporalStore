# LOCOMO-Style Context Benchmark Docker Results

Run date: 2026-06-19

## Scope

This run validates the TemporalStore context ingestion, extraction, retrieval, and injection pipeline
inside the Docker image built from the current `rust-main` source.

The harness uses the built-in LOCOMO/LongMemEval-style fixture because a licensed full LOCOMO export
was not present in the workspace. It also uses the deterministic `mock-openai-compatible` provider
path. The Docker image exposes the OpenViking-style open-source model profiles, including
the `matrixark-cpp-oss-context` profile that matches the C++/MatrixArk LOCOMO path
(`google/flan-t5-small` extraction plus `sentence-transformers/all-MiniLM-L6-v2` embeddings) and
the `Vision-CAIR/MiniGPT-4` VLM profile. This run did not start a live Ollama/vLLM/MiniGPT-4 model
server.

## Docker Image

```bash
docker build -t temporalstore-rust-context:locomo-current .
docker run --rm temporalstore-rust-context:locomo-current /usr/local/bin/context_workflow_harness \
  > /tmp/locomo_context_docker_current.json
python3 tools/validate_aws_validation_log.py \
  --job context-workflow-validation \
  --log /tmp/locomo_context_docker_current.json
```

Fresh image ID:

```text
temporalstore-rust-context:locomo-current -> 743d0f2f4b54
```

Validation:

```text
context-workflow-validation: JSON validation passed
```

## Model Profiles Present In The Image

The current Docker run reported these OpenViking-style model profiles:

| Profile | VLM | Embedding |
| --- | --- | --- |
| `openviking-qwen2_5_vl-local` | `qwen2.5vl:7b` | `nomic-embed-text` |
| `openviking-llava-local` | `llava:7b` | `nomic-embed-text` |
| `openviking-internvl-vllm` | `OpenGVLab/InternVL2_5-8B` | `BAAI/bge-m3` |
| `matrixark-cpp-oss-context` | `none` | `sentence-transformers/all-MiniLM-L6-v2` |
| `openviking-minigpt4-gpt-style-vlm` | `Vision-CAIR/MiniGPT-4` | `BAAI/bge-m3` |

## LOCOMO/LongMemEval-Style Fixture Score

| Metric | Result |
| --- | ---: |
| External benchmark source | `built-in-locomo-longmemeval-fixture` |
| Dataset labels | `locomo_style+longmemeval_s_style` |
| Case count | 18 |
| Hit@K | 1.0 |
| Mean reciprocal rank | 1.0 |
| Answer-term coverage | 1.0 |
| Missing expected terms | 0 |
| Zero-hit queries | 0 |
| All categories passed | true |
| Minimum category Hit@K | 1.0 |
| Minimum category MRR | 1.0 |

The fixture covers LOCOMO-style and LongMemEval_s-style memory questions across stale/current
memory updates, temporal reasoning, multi-hop evidence, social-link questions, quantity updates,
and entity aliases.

## VikingMem-Style Local Sweep Score

| Metric | Result |
| --- | ---: |
| Main benchmark sources | 48 |
| Main benchmark queries | 6 |
| Main Hit@K | 1.0 |
| Main MRR | 1.0 |
| Evidence retention@K | 1.0 |
| Token reduction | 83.67876% |
| Retrieval p50 | 9 ms |
| Retrieval p95 | 10 ms |
| Sweep profiles | 4 |
| Sweep total sources | 204 |
| Sweep total queries | 20 |
| Sweep minimum Hit@K | 1.0 |
| Sweep minimum MRR | 1.0 |
| Sweep minimum evidence retention@K | 1.0 |
| Sweep minimum token reduction | 57.170174% |

## Production Follow-Up

To score the full LOCOMO dataset with live open-source models, mount a JSONL export and run the same
container with `TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL` set:

```bash
docker run --rm \
  -e TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL=/bench/locomo.jsonl \
  -v /absolute/path/to/bench:/bench:ro \
  temporalstore-rust-context:locomo-current \
  /usr/local/bin/context_workflow_harness
```

For live VLM/model execution, run an OpenAI-compatible gateway for the selected profile
(`qwen2.5vl:7b`, `llava:7b`, `OpenGVLab/InternVL2_5-8B`, or `Vision-CAIR/MiniGPT-4`) and configure
the provider with `mock_mode=false`. The deterministic Docker score above is valid for pipeline
regression and benchmark-shape validation, but it is not a full paper-equivalent LOCOMO score.
