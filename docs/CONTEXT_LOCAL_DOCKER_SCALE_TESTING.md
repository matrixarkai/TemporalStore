# Context Local Docker Scale Testing

This document records the local Docker scale test for the MatrixArk / TemporalStore
LLM context pipeline. The goal is to validate the full extraction, ingestion,
retrieval, summary, entity, index, and compression loop with local open-source
models while keeping enough local state for debugging.

## What The Docker Test Covers

The Docker path runs `tools/run_context_pipeline_scale_e2e.py` with the cached
OSS embedding model:

```text
sentence-transformers/all-MiniLM-L6-v2
```

The E2E validates:

```text
ContextNode      upsert/get and child listing after API, stream, and resource ingest
ContextEvent     API, batch, stream, resource extraction, feedback, query, retrieval
ContextEntity    creation/update from API, batch, stream, resource, feedback, retrieval
ContextIndex     status/event_type/project indexes, scoped hash indexes, AND intersection
ContextSummary   L0 refresh, as-of query, summary embeddings, retrieval summary refs
Compression      old-window compression with source event ids for replay
Retrieval        tree traversal, staleness scoring, token budgeting, ContextPack refs
```

## Local Debug Environment

The Docker runner now preserves useful local artifacts by default under:

```text
.local/context-debug/
```

Current artifacts from the latest run:

```text
.local/context-debug/context_pipeline_docker_oss_models.json
.local/context-debug/docker_scale_50.log
```

The Docker image is:

```text
temporalstore-context-oss:local
```

The stopped debug container from the latest kept run is:

```text
temporalstore-context-scale-debug-50b
```

Inspect it with:

```bash
docker ps -a --filter name=temporalstore-context-scale-debug
docker logs temporalstore-context-scale-debug-50b
docker cp temporalstore-context-scale-debug-50b:/work/.local/context-debug ./context-debug-copy
```

The repository and debug artifacts are mounted into the container at:

```text
/work
```

The local model cache is mounted read-only at:

```text
/models
```

## Reproduce The 50-Event Docker Scale Test

From the repo root:

```bash
mkdir -p .local/context-debug

DEBUG_DIR=.local/context-debug \
EVENTS_PER_LANE=50 \
KEEP_CONTAINER=1 \
CONTAINER_NAME=temporalstore-context-scale-debug-50b \
tools/run_context_pipeline_docker_oss_models.sh \
  | tee .local/context-debug/docker_scale_50.log
```

If the container name already exists, choose a new name or remove the old stopped
debug container:

```bash
docker rm temporalstore-context-scale-debug-50b
```

## Latest Result

Latest local Docker scale run:

```text
status=passed
events_per_lane=50
context_steps=63
embedding_backend=sentence-transformers
embedding_dim=384
api_events=1
batch_events=50
stream_events=50
resource_chunks=1
resource_extracted_events=1
entity_records=2
summary_records=7
summary_embedding_refs=6
summary_refs_in_context_packs=6
compression_records=2
compression_source_event_refs=103
feedback_events=1
total_expected_events=103
```

The run also passed both unified gates inside Docker:

```text
C++ unified context contract passed: cases=1 context_steps=63
Rust unified corpus proxy contract: 1 passed
```

## Quick Result Inspection

```bash
python3 - <<'PY'
import json
from pathlib import Path

p = Path(".local/context-debug/context_pipeline_docker_oss_models.json")
data = json.loads(p.read_text())
print(json.dumps({
    "status": data["status"],
    "events_per_lane": data["expected"]["events_per_lane"],
    "steps": data["expected"]["steps"],
    "embedding_dim": data["expected"]["embedding_dim"],
    "total_expected_events": data["expected"]["total_expected_events"],
    "summary_refs_in_context_packs": data["expected"]["summary_refs_in_context_packs"],
}, indent=2))
PY
```

## Model Cache

The test expects the local embedding model here:

```text
.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2
```

If missing, download it first:

```bash
python3 -m pip install --user modelscope
python3 tools/download_context_oss_models.py --source modelscope --skip-vlm
```

## Cleanup When Finished

Keep `.local/context-debug/` while debugging. When done:

```bash
docker rm temporalstore-context-scale-debug-50b
rm -rf .local/context-debug
```

Do not commit `.local/`, Docker build outputs, Rust `target/`, or generated
result JSON files.
