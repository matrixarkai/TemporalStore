# ContextCompressionModel Workflow

`ContextCompressionModel` stores non-destructive temporal compression records for
older context windows. It lets MatrixArk keep raw `ContextEvent` records
queryable while also serving a compact summary when old context would otherwise
consume too many prompt tokens.

## Storage Shape

TemporalStore object key:

```text
ctx:compress:{tenant_hash}:{node_hash}
```

Timeline key:

```text
compressed_time_ms + compression_id_hash suffix
```

Payload:

```text
ContextCompressionEvent
  compression_id_hash
  node_hash
  source_start_ms
  source_end_ms
  compressed_time_ms
  summary
```

MatrixArk keeps source ids in its serving/test contract so replay can prove what
was compressed:

```json
{
  "compression_id_hash": 70000,
  "node_hash": 4210,
  "source_start_ms": 1781500000000,
  "source_end_ms": 1781500000999,
  "compressed_time_ms": 1781500005000,
  "compressed_summary": "Compressed approved GPU requests for Project 1.",
  "source_event_ids": [50000, 51000, 51001]
}
```

## Write Path

Event ingestion should not run compression synchronously. The write path stays:

```text
raw event/resource/feedback
-> extract event/entity
-> append ContextEvent
-> mark summary dirty
-> return
```

An async worker or scheduled maintenance loop then runs:

```text
select old window for a node
-> read source ContextEvent ids
-> model/rule summary of that window
-> WRITE_COMPRESSION_EVENT
-> keep source events queryable
```

Compression is therefore a serving optimization, not a destructive archive.

## Query Path

For a fresh current-state query:

```text
query raw events in fresh window
query entities/current summaries
optionally query compression windows for older history
pack fresh evidence first
use compression summary when old raw events exceed budget
```

For historical replay:

```text
query raw events by time range
query compression records by time range
return both source ids and compressed summaries for audit
```

## E2E Coverage

The generated local scale E2E writes two compression records:

```text
approval compression:
  node = approval_leaf
  sources = API approval event + batch approval events

incident compression:
  node = incident_leaf
  sources = stream incident events + resource-derived event + feedback event
```

Both C++ and Rust unified tests assert:

```text
context_write_compression
context_query_compression
expect_compression_ids
expect_compression_source_event_ids
```

This verifies the TemporalStore-only setup: events, summaries, embeddings,
entities, compression windows, and replay metadata all live in TemporalStore
serving records or the unified TemporalStore contract. No Milvus/S3/MatrixDB path
is required for this local MVP gate.

## Debug Commands

Fast deterministic run:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --model-provider deterministic \
  --events-per-lane 50 \
  --write-results /tmp/context_compression_e2e_50.json
```

OSS-model Docker run with cached embedding model:

```bash
EVENTS_PER_LANE=5 \
RESULT_PATH=/tmp/context_compression_docker_oss.json \
tools/run_context_pipeline_docker_oss_models.sh
```

Expected result fields:

```json
{
  "compression_records": 2,
  "compression_source_event_refs": 13,
  "summary_records": 7,
  "summary_embedding_refs": 6,
  "entity_records": 2
}
```

For `events_per_lane = 50`, `compression_source_event_refs` should be `103`.
