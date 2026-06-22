# MatrixArk One-Pass Batch Memory Extraction

## Why

Per-message extraction is fast and fresh, but it is often too local. A single
message such as "yes, correct" may be impossible to classify without session
context. Running one prompt per memory type is also expensive: if there are N
memory types, the same session is sent to the model N times.

MatrixArk now supports a VikingMem-style one-pass extraction path:

```text
logical session batch
-> one compiled extraction schema
-> one extraction pass
-> ContextEvents + ContextEntities + ContextSummary + ContextIndex + audit
```

## Policy

- Immediate ingest still writes lightweight `ContextEvent` records.
- Batch extraction is triggered by:
  - `>= 20` messages by default;
  - explicit `force=true`;
  - future `session_commit` hooks.
- The customer still sends a simple message envelope. MatrixArk owns the internal
  extraction schema.

## One-Pass Schema

The current schema is compact and internal:

```json
{
  "version": "matrixark-one-pass-memory-v1",
  "input": "logical session batch",
  "outputs": [
    "ContextEvent",
    "ContextEntity",
    "ContextSummary",
    "ContextIndex",
    "stale_blocker",
    "extraction_audit"
  ],
  "entity_types": [
    "preference",
    "relationship",
    "location",
    "job_status",
    "current_plan",
    "family_profile",
    "correction",
    "confirmation"
  ]
}
```

The local implementation is deterministic for testing. Production can replace
the extraction function with a single GPT-4o-mini/OSS call that emits the same
JSON shape.

## API

MCP tool:

```text
matrixark_batch_extract
```

Request:

```json
{
  "messages": [
    {"role": "user", "content": "I prefer jasmine tea now, not coffee."}
  ],
  "scope": {
    "user_id": "user-batch",
    "session_id": "session-batch",
    "team": "infra",
    "project": "project-1"
  },
  "metadata": {
    "node_path": ["infra", "project-1", "session_batch"]
  },
  "threshold_messages": 20,
  "force": false
}
```

If the batch is below threshold and `force=false`, MatrixArk returns:

```json
{
  "status": "deferred",
  "reason": "logical batch below extraction threshold"
}
```

## Records Written

One batch extraction writes:

- one `ContextEvent` per message;
- one `ContextEmbedding` per event;
- multiple `ContextEntity` records;
- deterministic entity field patches when the extractor sees corrections or
  updates;
- one `ContextEmbedding` per entity;
- one `ContextSummary` for the batch L0 summary;
- one summary embedding;
- multiple `ContextIndex` records;
- one `ContextExtractionAudit`.

Retrieval now considers both raw `ContextEvent` records and extracted
`ContextEntity` records, so current-state questions can use entity state before
falling back to raw dialogue.

## Faster Entity Update Without A Second LLM

The one-pass extractor can emit compact field patches:

```text
<< SEARCH
old field span
====
new field content
>> REPLACE
```

MatrixArk applies those patches with the Entity Update Algorithm:

```text
target entity = node_hash + entity_type + entity_name
old field + patch
-> exact match
-> approximate edit-distance span match if exact match misses
-> write updated ContextEntity
-> write ContextEntityUpdateAudit with llm_calls=0
```

This keeps entity maintenance online and low cost. The batch extractor does one
schema-driven pass; the entity update itself is deterministic and does not call
another model.

## Validation

Local:

```bash
python3 tools/run_matrixark_one_pass_batch_extract_test.py \
  --backend local \
  --report-json /tmp/matrixark_one_pass_local.json
```

C++ TemporalStore direct:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_one_pass_batch_extract_test.py \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_one_pass_cpp_direct.json
```
