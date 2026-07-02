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

The runner prints a JSON summary and writes Prometheus-compatible metrics to `/tmp/matrixark_context_backfill_<job_id>.prom` unless `PROM_OUTPUT` is set.

## Resume And Dead Letters

The checkpoint key is:

```text
matrixark:backfill:<job_id>:checkpoint:<target_prefix_hash>
```

With `--resume=1`, the next run starts after the last successfully processed source sequence. Bad or corrupt records are written to the target dead-letter hash and do not block later records unless `--fail-fast` is set.

## Promote Or Roll Back

Validate the shadow prefix before cutover by comparing record counts, context-pack retrieval, and representative selected refs against the source path. To roll back, discard the shadow prefix. Never rewrite or delete the source raw log during rollback.

Only use in-place mode after shadow validation:

```bash
python3 tools/matrixark_context_backfill.py \
  --mode=in_place \
  --confirm-in-place=YES \
  --source-prefix=matrixark:mcp \
  --job-id=context-backfill-prod \
  --dry-run=0
```

## Local Unit Test

```bash
python3 tools/test_matrixark_context_backfill.py
```