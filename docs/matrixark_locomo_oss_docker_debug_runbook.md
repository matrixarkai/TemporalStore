# MatrixArk LOCOMO OSS Docker Debug Runbook

This run validates MatrixArk ingestion, extraction, retrieval, L0/L1 summaries, ContextSegment, ContextEvent, evolving ContextEntity, ContextIndex, ContextPackAudit, and stored embeddings using the local OSS embedding model.

## Live Container

Container left running for inspection:

```bash
docker exec -it matrixark-locomo-oss-debug bash
```

Inside the container, the repo is mounted at `/work` and the OSS model cache is mounted at `/models`.

## Model Used

```text
MATRIXARK_EMBEDDING_PROVIDER=oss
MATRIXARK_EMBEDDING_MODEL_PATH=/models/sentence-transformers/all-MiniLM-L6-v2
```

The debug artifact confirms ContextEmbedding vectors are 384-dimensional, which matches `all-MiniLM-L6-v2` rather than the deterministic 32-dimensional fallback.

## Exact Docker Command Used

```bash
docker rm -f matrixark-locomo-oss-debug >/dev/null 2>&1 || true
docker run -d --name matrixark-locomo-oss-debug   -v <repo>:/work   -v <repo>/.local/context-oss-models:/models:ro   -w /work   matrixark-locomo-oss-debug:local sleep infinity

docker exec -w /work   -e MATRIXARK_EMBEDDING_PROVIDER=oss   -e MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1   -e MATRIXARK_EMBEDDING_MODEL_PATH=/models/sentence-transformers/all-MiniLM-L6-v2   -e PYTHONPATH=/work   matrixark-locomo-oss-debug   python3 tools/run_matrixark_locomo_debug_flow.py     --artifact-dir .local/context-debug/locomo-oss-docker-debug
```

## Debug Files Left On Disk

```text
<repo>/.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_event_log.jsonl
<repo>/.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_data_flow.json
<repo>/.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_data_flow.md
<repo>/.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_data_flow.html
<repo>/.local/context-debug/locomo-oss-docker-debug/run_stdout.json
```

## Data Model Counts From The Run

```text
context_batch_commit: 3
context_embedding: 90
context_entity: 9
context_entity_update_audit: 9
context_event: 12
context_extraction_audit: 3
context_index: 24
context_pack_audit: 9
context_segment: 6
context_summary: 63
context_summary_dirty: 51
context_summary_refresh_audit: 24
matrixark_audit_log: 27
session_buffer_event: 12
```

## What To Inspect

Use the event log JSONL to inspect raw records by model:

```bash
cd /work
python3 - <<'PY'
import json
from pathlib import Path
path = Path('.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_event_log.jsonl')
records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
for record_type in ['context_event', 'context_segment', 'context_entity', 'context_summary', 'context_embedding', 'context_index', 'context_pack_audit']:
    rows = [r for r in records if r.get('record_type') == record_type]
    print(record_type, len(rows))
    print(json.dumps(rows[:2], indent=2)[:2000])
PY
```

## Accessible Report

A compact report is copied into docs:

- `docs/matrixark_locomo_oss_docker_debug_data_flow.md`
- `docs/matrixark_locomo_oss_docker_debug_data_flow.html`
