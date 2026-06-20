# Docker Open-Model Context Benchmarks

This path packages the open-source reader benchmark flow that the LOCOMO and
LongMemEval_s runners already support. It starts an Ollama OpenAI-compatible
reader endpoint in Docker, runs the benchmark runners from a Python container,
and archives reports under `benchmark_reports/`.

Claim level: packaged open-model benchmark path. A production parity claim requires a mounted real
dataset, a successful real reader call, and passing threshold output in the archived report.

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
- `longmemeval_s_report.json` and `longmemeval_s_misses.jsonl` when LongMemEval_s is present
- `docker_start.log` or `model_pull.log` when infrastructure setup fails before scoring

If Docker Hub, the local registry, or the model registry is unreachable, the
script exits non-zero and still writes a manifest with `phase` set to
`docker_start_failed` or `model_pull_failed`.

## Gates

LOCOMO uses the `oss_reader_full` threshold profile:

- at least 1542 cases
- retrieval hit rate at least 0.90
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
docker compose -f docker-compose.context-benchmarks.yml up -d open-reader
docker compose -f docker-compose.context-benchmarks.yml exec -T open-reader ollama pull qwen2.5:0.5b
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
docker-compose -f docker-compose.context-benchmarks.yml config
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
OpenViking-style provider configuration distinct from a successful local OSS reader or VLM
benchmark run.
