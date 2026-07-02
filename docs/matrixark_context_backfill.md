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

If both metadata keys are missing, the runner falls back to bounded MatrixKV hash scanning over `<prefix>:records:<shard>`. The scan starts at `--start-seq`, respects `--end-seq`, and stops after `--source-scan-max-empty-shards` consecutive empty shards when no explicit end sequence is supplied. This makes repair jobs tolerant of missing or stale source metadata without unbounded keyspace scans.

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

The runner processes source records in `BATCH_SIZE` chunks. It uses backend batch reads (`batch_hget`) and batch appends (`matrixark_append_records` or `batch_hset`) when available, falling back to single-record operations only when the backend does not expose a batch API. Source discovery can use record-count, legacy index, or bounded `scan_hash`; Prometheus metrics include source, target, and scan-hash batch counts. The runner prints a JSON summary and writes Prometheus-compatible metrics to `/tmp/matrixark_context_backfill_<job_id>.prom` unless `PROM_OUTPUT` is set.

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

For incremental repair, do not switch the whole active prefix. Use a bounded source sequence window, backfill that window into a shadow repair prefix, then run `incremental_repair`. The repair mode validates the shadow prefix, resolves the active prefix pointer, replays the same bounded range into the active prefix, writes an audit record, and keeps the active prefix pointer unchanged.

`incremental_repair` is intentionally guarded:

- requires `--start-seq` and `--end-seq`; open-ended repair is rejected
- requires `--target-prefix` for the shadow repair prefix
- requires `--confirm-incremental-repair=YES`
- uses `--repair-active-prefix` when supplied, otherwise reads `--active-prefix-key`
- uses target-side idempotency keys so retrying the same repair does not append duplicate active records

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
  --mode=incremental_repair \
  --confirm-incremental-repair=YES \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_repair:partition-123 \
  --job-id=repair-partition-123 \
  --start-seq=1200000 \
  --end-seq=1255000 \
  --batch-size=1024 \
  --dry-run=0
```

If the active prefix pointer is not available in the target store, pass it explicitly:

```bash
python3 tools/matrixark_context_backfill.py \
  --mode=incremental_repair \
  --confirm-incremental-repair=YES \
  --repair-active-prefix=matrixark:context:active \
  --source-prefix=matrixark:mcp \
  --target-prefix=matrixark:context_repair:partition-123 \
  --job-id=repair-partition-123 \
  --start-seq=1200000 \
  --end-seq=1255000 \
  --dry-run=0
```

## Local Unit Test

```bash
python3 tools/test_matrixark_context_backfill.py
```