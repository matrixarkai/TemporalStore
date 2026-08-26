# Docker Open-Model Context Benchmarks

This path packages the open-source reader benchmark flow that the LOCOMO and
LongMemEval_s runners already support. It starts an Ollama OpenAI-compatible
reader endpoint in Docker, runs the benchmark runners from a Python container,
and archives reports under `benchmark_reports/`.

The repo also includes a Hugging Face Transformers endpoint for the exact
`matrixark-native-oss-context` text-reader profile. It serves the OpenAI-compatible
`/v1/models` and `/v1/chat/completions` APIs from
`tools/openai_compatible_hf_reader.py`, defaults to `google/flan-t5-small`, and
is packaged by `docker/Dockerfile.context-oss-reader`.

Claim level: packaged open-model benchmark path. This page does not present production conformance or
the external baseline paper-comparable evidence by itself. Those labels require a mounted real dataset, a
successful real reader call, no deterministic fallback, full Rust TemporalStore replay, and passing
threshold output in the archived report.

## Defaults

| Setting | Default |
| --- | --- |
| Reader image | `ollama/ollama:0.3.14` |
| Reader API | `http://open-reader:11434/v1` |
| Model | `qwen2.5:0.5b` |
| Input mount | `/tmp` on the host mounted as `/bench-input` |
| LOCOMO input | `/bench-input/locomo10.json` |
| LongMemEval_s input | `/bench-input/longmemeval_s.json` |
| Output directory | `benchmark_reports/open_model_<timestamp>/` |

The script skips a dataset if its input artifact is not mounted. That keeps the
fixture and missing-artifact behavior honest: a missing LongMemEval_s file is a
blocked real-dataset run, not a passing score.

## Run

```bash
TEMPORALSTORE_READER_MODEL=qwen2.5:0.5b \
TEMPORALSTORE_BENCHMARK_INPUT_DIR=/tmp \
TEMPORALSTORE_BENCHMARK_REPORT_DIR=./benchmark_reports \
bash tools/run_context_benchmarks_docker_open_model.sh
```

To use another local open model supported by Ollama:

```bash
TEMPORALSTORE_READER_MODEL=llama3.2:1b \
bash tools/run_context_benchmarks_docker_open_model.sh
```

## External OSS Reader Endpoint

The Docker/Ollama path above is useful when the chosen model is available in
Ollama. The MatrixArk / external-baseline benchmark path uses the
`matrixark-native-oss-context` profile with `google/flan-t5-small`. Run that exact
reader/model through the packaged Hugging Face endpoint with:

```bash
TEMPORALSTORE_READER_MODEL=google/flan-t5-small \
TEMPORALSTORE_BENCHMARK_REPORT_DIR=/tmp/temporalstore_hf_oss_reader_reports \
bash tools/run_hf_oss_reader_endpoint.sh
```

That script starts `tools/openai_compatible_hf_reader.py`, waits for
`/v1/models`, and runs a fixture smoke through the LongMemEval_s runner with
`--reader-mode open-source`, `--require-open-source-reader`, and the Rust
TemporalStore backend enabled. For a long-running endpoint without the smoke,
set `TEMPORALSTORE_OSS_READER_RUN_SMOKE=0` and use:

```bash
TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:8000/v1 \
TEMPORALSTORE_READER_PROVIDER_NAME=matrixark-native-oss-context \
TEMPORALSTORE_READER_MODEL=google/flan-t5-small \
bash tools/run_context_benchmarks_oss_reader_endpoint.sh
```

To build the same endpoint in Docker:

```bash
docker-compose -f docker/docker-compose.context-benchmarks.yml build hf-reader
docker-compose -f docker/docker-compose.context-benchmarks.yml up -d hf-reader
```

When the endpoint is already running, execute the full dataset benchmark path
with:

```bash
TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:8000/v1 \
TEMPORALSTORE_READER_PROVIDER_NAME=matrixark-native-oss-context \
TEMPORALSTORE_READER_MODEL=google/flan-t5-small \
TEMPORALSTORE_LOCOMO_INPUT=/tmp/locomo10.json \
TEMPORALSTORE_LONGMEMEVAL_INPUT=/tmp/longmemeval_s.json \
bash tools/run_context_benchmarks_oss_reader_endpoint.sh
```

This runner is fail-closed: it exits non-zero when the endpoint is missing, no
dataset artifact is present, the reader falls back to deterministic mode, or the
benchmark thresholds fail. It writes an archive under
`benchmark_reports/oss_reader_endpoint_<timestamp>/` with `manifest.json`,
raw reports, misses JSONL files, and `*_paper_comparable_report.json` summaries when
the corresponding datasets are present and pass.

The local endpoint runner requires Rust TemporalStore unconditionally. It converts benchmark cases to the
Rust context JSONL contract and runs `context_workflow_harness` through a real
`TemporalEngine` before the Python reader/scorer emits the report. It also compares
Rust case count, Hit@K, mean reciprocal rank, and zero-hit queries with Python on
the exact converted subset and fails closed if they are not on par. Tune the bounded
proof with `TEMPORALSTORE_RUST_BACKEND_MAX_CASES`,
`TEMPORALSTORE_RUST_BACKEND_SOURCE_LIMIT`, and
`TEMPORALSTORE_RUST_BACKEND_TIMEOUT_SECONDS`; tune score drift with
`TEMPORALSTORE_RUST_BACKEND_SCORE_TOLERANCE`.

To use a local registry mirror or a pre-pulled compatible image:

```bash
TEMPORALSTORE_READER_IMAGE=registry.example.com/ollama/ollama:0.3.14 \
bash tools/run_context_benchmarks_docker_open_model.sh
```

To mount artifacts from a custom directory:

```bash
TEMPORALSTORE_BENCHMARK_INPUT_DIR=/data/context-benchmarks \
bash tools/run_context_benchmarks_docker_open_model.sh
```

The runner writes:

- `manifest.json`
- `locomo_report.json` and `locomo_misses.jsonl` when LOCOMO is present
- `locomo_paper_comparable_report.json` when LOCOMO produces a strict live-reader archive; this is
  paper-comparable only when `paper_comparable_claim_ready=true`
- `longmemeval_s_report.json` and `longmemeval_s_misses.jsonl` when LongMemEval_s is present
- `longmemeval_s_paper_comparable_report.json` when LongMemEval_s produces a strict live-reader
  archive; this is paper-comparable only when `paper_comparable_claim_ready=true`
- `docker_start.log` or `model_pull.log` when infrastructure setup fails before scoring

The `*_paper_comparable_report.json` files use
`matrixark_external_paper_comparable_report_v1` and include the dataset SHA-256,
input bytes, model/provider, reader mode, exact reader prompt templates, Rust
TemporalStore backend evidence, thresholds, p50/p95 latencies, token reduction,
quality-gate state, and category breakdown. They are diagnostic archives unless
`quality_gate.paper_comparable_claim_ready=true`; only then should they be used as
the external OSS baseline paper-comparable evidence or compared as paper-comparable benchmark
outputs.

The local endpoint runner always requires the Rust TemporalStore backend. The lower-level
LOCOMO/LongMemEval_s full gate commands also require it unless they are run in an explicitly
marked local diagnostic mode. Accepted pipeline and benchmark evidence invokes the Rust
`context_workflow_harness`, compare Rust case count, Hit@K, mean reciprocal rank,
and zero-hit queries with the Python scorer on the exact converted subset, and
fails closed unless the conformance result is on par. Production benchmark evidence requires
`all_pipelines_use_rust_temporalstore=true`,
`rust_temporalstore_context_event_ingest_ready=true`, and
`rust_temporalstore_direct_source_scoring=false`.

## Hugging Face Endpoint Validation

Local validation on 2026-06-21 packaged the endpoint and attempted to start the
live `google/flan-t5-small` reader:

```bash
TEMPORALSTORE_BENCHMARK_REPORT_DIR=/tmp/temporalstore_hf_oss_reader_reports \
TEMPORALSTORE_READER_MODEL=google/flan-t5-small \
TEMPORALSTORE_HF_READER_PORT=8000 \
bash tools/run_hf_oss_reader_endpoint.sh
```

Packaging checks passed:

```bash
bash -n tools/run_hf_oss_reader_endpoint.sh
python3 -m py_compile tools/openai_compatible_hf_reader.py
docker-compose -f docker/docker-compose.context-benchmarks.yml config
```

The live run failed closed before any benchmark score because the requested model
was not cached locally and WSL could not reach Hugging Face:

```text
Network is unreachable while requesting
https://huggingface.co/google/flan-t5-small/resolve/main/config.json
```

Failure archive:

```text
docs/benchmark_archives/hf_oss_reader_endpoint_failed_latest.json
```

No paper-comparable claim is made from this run:
`reader_open_source_calls = 0` and `paper_comparable_claim_ready = false`. To
complete the live gate, pre-populate the Hugging Face cache or pass
`TEMPORALSTORE_READER_MODEL=/path/to/local/flan-t5-small`, rerun
`tools/run_hf_oss_reader_endpoint.sh`, then run
`tools/run_context_benchmarks_oss_reader_endpoint.sh` against the same
`TEMPORALSTORE_READER_BASE_URL`.

The later required-reader run reached the packaged endpoint at `/v1/models`, then
failed closed on both real datasets because `/v1/chat/completions` timed out while
the model was still unavailable locally:

```text
docs/benchmark_archives/oss_reader_required_failed_latest.json
```

That archive is the current honest the external OSS baseline live-reader status:
endpoint packaging is proven far enough to answer model discovery, Rust
TemporalStore bounded proofs execute, but `reader_open_source_calls = 0` and no
paper-comparable score is claimed.

If Docker Hub, the local registry, or the model registry is unreachable, the
script exits non-zero and still writes a manifest with `phase` set to
`docker_start_failed` or `model_pull_failed`.

## Gates

LOCOMO uses the `oss_reader_full` threshold profile:

- at least 1542 cases
- retrieval hit rate at least 0.94
- reader hit rate at least 0.58
- token reduction at least 80%
- retrieval p95 at most 250 ms
- at least one successful OSS reader call

LongMemEval_s uses the `longmemeval_full` profile plus
`--require-open-source-reader`:

- at least 500 cases
- retrieval hit rate at least 0.90
- reader hit rate at least 0.58
- token reduction at least 80%
- retrieval p95 at most 2000 ms
- at least one successful OSS reader call

These thresholds are defined in
[benchmark_threshold_policy.md](benchmark_threshold_policy.md).

## Manual Reader Probe

The compose file exposes Ollama on the host at port `11434` by default:

```bash
docker compose -f docker/docker-compose.context-benchmarks.yml up -d open-reader
docker compose -f docker/docker-compose.context-benchmarks.yml exec -T open-reader ollama pull qwen2.5:0.5b
python3 tools/run_live_oss_reader_validation.py \
  --dataset locomo \
  --input /tmp/locomo10.json \
  --base-url http://127.0.0.1:11434/v1 \
  --model qwen2.5:0.5b \
  --report /tmp/temporalstore_live_oss_reader_validation.json
```

## Local Packaging Validation

Validation on 2026-06-20:

```bash
bash -n tools/run_context_benchmarks_docker_open_model.sh
docker-compose -f docker/docker-compose.context-benchmarks.yml config
TEMPORALSTORE_BENCHMARK_INPUT_DIR=/tmp \
TEMPORALSTORE_BENCHMARK_REPORT_DIR=/tmp/temporalstore_context_benchmark_reports \
bash tools/run_context_benchmarks_docker_open_model.sh
```

Results:

| Check | Result |
| --- | --- |
| Shell syntax | passed |
| Compose config | passed with local `docker-compose` |
| Docker daemon | reachable |
| Reader image pull | blocked by Docker Hub timeout |
| Archive manifest | written |

Failure archive:

```text
/tmp/temporalstore_context_benchmark_reports/open_model_20260620T060714Z/manifest.json
```

Manifest phase:

```json
{
  "phase": "docker_start_failed",
  "error": "see docker_start.log",
  "reader_image": "ollama/ollama:0.3.14",
  "reader_model": "qwen2.5:0.5b",
  "locomo_status": "not_run",
  "longmemeval_status": "not_run"
}
```

No LOCOMO or LongMemEval_s open-model score is claimed from this validation because
the reader image could not be pulled in the local environment.

Follow-up validation on 2026-06-20 retried:

```bash
docker pull ollama/ollama:0.3.14
```

The pull still failed with a Docker Hub manifest request timeout. The local
`temporalstore-context-oss:local` image was inspected and does not expose an
OpenAI-compatible model server; it is a repo/context fixture image. Therefore the
live OSS-reader benchmark gap remains open until an Ollama/vLLM/OpenAI-compatible
reader image or endpoint is reachable locally.

Follow-up validation retried the text-reader image pull:

```bash
timeout 90 docker pull ollama/ollama:0.3.14
```

It still failed while resolving `docker.io/ollama/ollama:0.3.14` with a registry request timeout.
No live text-reader score is claimed from this attempt.

The Context workflow state and harness outputs now carry separate proof fields:
`open_model_provider_packaged=true`, `open_model_local_run_proven=false`,
`vlm_provider_configured=true`, and `vlm_benchmark_proven=false` for this evidence set. This keeps
the external OSS system-style provider configuration distinct from a successful local OSS reader or VLM
benchmark run.
