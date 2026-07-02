# MatrixArk Context Backfill From MatrixKV Raw Logs

## Purpose

This backfill path replays MatrixArk raw/serving records stored in MatrixKV or TemporalStore by the direct adapter into a new context-serving prefix. It is designed for safe backfills of LLM context management data without mutating the source ingestion log.

The default mode is shadow-first. Backfill writes to `matrixark:context_backfill:<job_id>` unless a target prefix is provided. In-place writes are guarded by `--mode=in_place --confirm-in-place=YES`.

## Source Layout

The preferred source is the sharded MatrixArk direct-adapter log:

```text
<prefix>:record_count
<prefix>:records:<000000 shard> field <00000000000000000000 offset> -> JSON record
```

The runner also supports the older legacy layout:

```text
<prefix>:record_index
<prefix>:records field <record_id> -> JSON record
```

The source prefix defaults to `matrixark:mcp`.

## Running A Shadow Backfill

```bash
JOB_ID=context-backfill-001 \
SOURCE_PREFIX=matrixark:mcp \
TARGET_PREFIX=matrixark:context_backfill:context-backfill-001 \
DRY_RUN=0 \
BATCH_SIZE=256 \
bash tools/run_matrixark_context_backfill_ubuntu22.sh
```

The runner processes source records in `BATCH_SIZE` chunks. It uses backend batch reads (`batch_hget`) and batch appends (`matrixark_append_records` or `batch_hset`) when available, falling back to single-record operations only when the backend does not expose a batch API. The runner prints a JSON summary and writes Prometheus-compatible metrics, including source and target batch counts, to `/tmp/matrixark_context_backfill_<job_id>.prom` unless `PROM_OUTPUT` is set.

## Resume And Dead Letters

The checkpoint key is:

```text
matrixark:backfill:<job_id>:checkpoint:<target_prefix_hash>
```

With `--resume=1`, the next run starts after the last successfully processed source sequence. Checkpoints are advanced after the pending target batch has been flushed, so a restart replays at most the last uncommitted batch. Bad or corrupt records are written to the target dead-letter hash and do not block later records unless `--fail-fast` is set.

## Validate Shadow

Before any cutover, validate the candidate prefix against the same source range. Validation performs a dry-run source scan, compares expected materialized records with the target prefix record count, checks dead letters, and returns JSON status.

```bash
python3 tools/matrixark_context_backfill.py \
  --mode=validate_shadow \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_backfill:context-backfill-001 \
  --job-id=context-backfill-001 \
  --batch-size=1024
```

Strict validation is enabled by default. Use `--validation-strict=0` only when a target prefix is expected to contain validated extra records from an earlier compatible run.

## Activate Or Roll Back

For a full rebuild, activation is a guarded metadata flip. It does not rewrite the source raw log and does not copy records. The active prefix pointer defaults to `matrixark:context:active_prefix`; the previous pointer and an audit record are retained.

```bash
python3 tools/matrixark_context_backfill.py \
  --mode=activate_shadow \
  --confirm-activate=YES \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_backfill:context-backfill-001 \
  --job-id=context-backfill-001 \
  --dry-run=0
```

Rollback for a full rebuild is another guarded metadata flip back to the value stored at `matrixark:context:active_prefix:previous:<job_id>`. Never rewrite or delete the source raw log during rollback.

## Incremental Repair Promotion

For incremental repair, do not switch the whole active prefix. Backfill the lost source sequence range into a shadow repair prefix, validate that prefix, then replay the same range into the active prefix with guarded in-place mode. The deterministic idempotency keys make the promotion safe to retry.

```bash
python3 tools/matrixark_context_backfill.py \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_repair:partition-123 \
  --job-id=repair-partition-123 \
  --start-seq=1200000 \
  --end-seq=1255000 \
  --batch-size=1024 \
  --dry-run=0

python3 tools/matrixark_context_backfill.py \
  --mode=validate_shadow \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_repair:partition-123 \
  --job-id=repair-partition-123 \
  --start-seq=1200000 \
  --end-seq=1255000 \
  --batch-size=1024

python3 tools/matrixark_context_backfill.py \
  --mode=in_place \
  --confirm-in-place=YES \
  --source-prefix=matrixark:mcp \
  --job-id=repair-partition-123-promote \
  --start-seq=1200000 \
  --end-seq=1255000 \
  --batch-size=1024 \
  --dry-run=0
```

## Local Unit Test

```bash
python3 tools/test_matrixark_context_backfill.py
```