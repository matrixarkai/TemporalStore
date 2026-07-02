#!/usr/bin/env python3
"""Backfill MatrixArk context records from a MatrixKV raw ingestion log.

The source log is the sharded record format used by the MatrixArk direct
TemporalStore adapter:

    <prefix>:record_count              string global count
    <prefix>:records:<shard>           hash of zero-padded offsets -> JSON record

Legacy prefixes using <prefix>:record_index plus <prefix>:records are also
supported. Backfill writes normalized MatrixArk serving records to a shadow
prefix by default and never mutates source records.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'sdk' / 'python'))
sys.path.insert(0, str(ROOT / 'tools'))

from matrixark_mcp_core import (  # noqa: E402
    DIRECT_RECORD_LOG_SHARD_SIZE,
    materialize_serving_record_batch,
    materialize_serving_records,
    stable_hash,
)
from matrixark_mcp_local_adapter import MatrixArkLocalAdapter  # noqa: E402

Json = dict[str, Any]


class BackfillError(RuntimeError):
    pass


@dataclass
class BackfillMetrics:
    scanned: int = 0
    skipped: int = 0
    written: int = 0
    duplicate: int = 0
    failed: int = 0
    dead_letter: int = 0
    context_events: int = 0
    context_entities: int = 0
    context_summaries: int = 0
    context_embeddings: int = 0
    context_indexes: int = 0
    context_audits: int = 0
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    finished_at_ms: int = 0

    def observe_records(self, records: list[Json]) -> None:
        for record in records:
            record_type = str(record.get('record_type') or '')
            if record_type == 'context_event':
                self.context_events += 1
            elif record_type == 'context_entity':
                self.context_entities += 1
            elif record_type == 'context_summary':
                self.context_summaries += 1
            elif record_type == 'context_embedding':
                self.context_embeddings += 1
            elif record_type == 'context_index':
                self.context_indexes += 1
            elif record_type == 'context_pack_audit':
                self.context_audits += 1

    def finish(self) -> None:
        self.finished_at_ms = int(time.time() * 1000)

    def to_json(self, *, job_id: str, source_prefix: str, target_prefix: str, mode: str) -> Json:
        elapsed_ms = max(0, (self.finished_at_ms or int(time.time() * 1000)) - self.started_at_ms)
        qps = (self.scanned * 1000.0 / elapsed_ms) if elapsed_ms else 0.0
        return {
            'status': 'ok',
            'job_id': job_id,
            'source_prefix': source_prefix,
            'target_prefix': target_prefix,
            'mode': mode,
            'elapsed_ms': elapsed_ms,
            'scan_qps': round(qps, 3),
            'metrics': {
                'scanned': self.scanned,
                'skipped': self.skipped,
                'written': self.written,
                'duplicate': self.duplicate,
                'failed': self.failed,
                'dead_letter': self.dead_letter,
                'context_events': self.context_events,
                'context_entities': self.context_entities,
                'context_summaries': self.context_summaries,
                'context_embeddings': self.context_embeddings,
                'context_indexes': self.context_indexes,
                'context_audits': self.context_audits,
            },
        }

    def to_prometheus(self, *, job_id: str) -> str:
        labels = f'job_id="{job_id}"'
        lines = [
            '# HELP matrixark_context_backfill_records_total Records processed by context backfill.',
            '# TYPE matrixark_context_backfill_records_total counter',
        ]
        for name in ['scanned', 'skipped', 'written', 'duplicate', 'failed', 'dead_letter']:
            lines.append(f'matrixark_context_backfill_records_total{{{labels},status="{name}"}} {getattr(self, name)}')
        lines.extend([
            '# HELP matrixark_context_backfill_serving_records_total Serving records materialized by type.',
            '# TYPE matrixark_context_backfill_serving_records_total counter',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_event"}} {self.context_events}',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_entity"}} {self.context_entities}',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_summary"}} {self.context_summaries}',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_embedding"}} {self.context_embeddings}',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_index"}} {self.context_indexes}',
            f'matrixark_context_backfill_serving_records_total{{{labels},type="context_pack_audit"}} {self.context_audits}',
        ])
        return '\n'.join(lines) + '\n'


class TemporalStoreKV:
    def __init__(self, *, metaserver: str, namespace: str, table: str, library_path: str = '') -> None:
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=metaserver,
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=20000,
            io_timeout_ms=20000,
            max_read_retries=2,
            max_write_retries=1,
        )
        self.client = Client(options, library_path=library_path or None)

    def get_string(self, key: str) -> str:
        return self.client.get_string(key) or ''

    def put_string(self, key: str, value: str) -> None:
        self.client.put_string(key, value)

    def hget(self, key: str, field: str) -> str:
        return self.client.hget(key, field) or ''

    def hset(self, key: str, field: str, value: str) -> None:
        self.client.hset(key, field, value)


class LocalJsonKV:
    """Small test backend with TemporalStore-like string/hash operations."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._bulk_depth = 0
        if path.exists():
            self.data = json.loads(path.read_text(encoding='utf-8'))
        else:
            self.data = {'strings': {}, 'hashes': {}}

    def _flush(self) -> None:
        if self._bulk_depth > 0:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, sort_keys=True), encoding='utf-8')

    def begin_bulk(self) -> None:
        self._bulk_depth += 1

    def end_bulk(self) -> None:
        self._bulk_depth = max(0, self._bulk_depth - 1)
        if self._bulk_depth == 0:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self.data, sort_keys=True), encoding='utf-8')

    def get_string(self, key: str) -> str:
        return str(self.data['strings'].get(key, ''))

    def put_string(self, key: str, value: str) -> None:
        self.data['strings'][key] = str(value)
        self._flush()

    def hget(self, key: str, field: str) -> str:
        return str(self.data['hashes'].get(key, {}).get(field, ''))

    def hset(self, key: str, field: str, value: str) -> None:
        self.data['hashes'].setdefault(key, {})[field] = str(value)
        self._flush()


class MatrixKVRecordLog:
    def __init__(self, kv: Any, *, prefix: str, shard_size: int = DIRECT_RECORD_LOG_SHARD_SIZE) -> None:
        self.kv = kv
        self.prefix = prefix.rstrip(':')
        self.shard_size = shard_size

    def count(self) -> int:
        raw = self.kv.get_string(f'{self.prefix}:record_count')
        try:
            return max(0, int(raw)) if raw else 0
        except ValueError:
            return 0

    def legacy_index(self) -> list[str]:
        raw = self.kv.get_string(f'{self.prefix}:record_index')
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in value] if isinstance(value, list) else []

    def read_at(self, sequence: int) -> Json:
        shard = sequence // self.shard_size
        offset = sequence % self.shard_size
        payload = self.kv.hget(f'{self.prefix}:records:{shard:06d}', f'{offset:020d}')
        if not payload:
            raise BackfillError(f'missing sharded record at sequence {sequence}')
        return json.loads(payload)

    def read_legacy(self, record_id: str) -> Json:
        payload = self.kv.hget(f'{self.prefix}:records', record_id)
        if not payload:
            raise BackfillError(f'missing legacy record {record_id}')
        return json.loads(payload)

    def iter_records(self, *, start_seq: int, end_seq: int | None) -> Iterable[tuple[int, Json]]:
        count = self.count()
        if count > 0:
            stop = min(count, end_seq if end_seq is not None else count)
            for sequence in range(max(0, start_seq), stop):
                yield sequence, self.read_at(sequence)
            return
        index = self.legacy_index()
        stop = min(len(index), end_seq if end_seq is not None else len(index))
        for sequence in range(max(0, start_seq), stop):
            yield sequence, self.read_legacy(index[sequence])


class MatrixKVBackfillTarget:
    def __init__(self, kv: Any, *, prefix: str, shard_size: int = DIRECT_RECORD_LOG_SHARD_SIZE) -> None:
        self.kv = kv
        self.prefix = prefix.rstrip(':')
        self.shard_size = shard_size

    def count(self) -> int:
        raw = self.kv.get_string(f'{self.prefix}:record_count')
        try:
            return max(0, int(raw)) if raw else 0
        except ValueError:
            return 0

    def append_many(self, records: list[Json]) -> None:
        sequence = self.count()
        if hasattr(self.kv, 'begin_bulk'):
            self.kv.begin_bulk()
        try:
            for record in records:
                shard = sequence // self.shard_size
                offset = sequence % self.shard_size
                payload = json.dumps(record, sort_keys=True, separators=(',', ':'))
                self.kv.hset(f'{self.prefix}:records:{shard:06d}', f'{offset:020d}', payload)
                sequence += 1
            self.kv.put_string(f'{self.prefix}:record_count', str(sequence))
        finally:
            if hasattr(self.kv, 'end_bulk'):
                self.kv.end_bulk()

    def count_dead_letters(self) -> int:
        raw = self.kv.get_string(f'{self.prefix}:dead_letter_count')
        try:
            return max(0, int(raw)) if raw else 0
        except ValueError:
            return 0

    def append_dead_letter(self, item: Json) -> None:
        sequence = self.count_dead_letters()
        payload = json.dumps(item, sort_keys=True, separators=(',', ':'))
        self.kv.hset(f'{self.prefix}:dead_letter', f'{sequence:020d}', payload)
        self.kv.put_string(f'{self.prefix}:dead_letter_count', str(sequence + 1))


class CaptureAdapter(MatrixArkLocalAdapter):
    def __init__(self) -> None:
        super().__init__(Path('/tmp/matrixark-context-backfill-unused.jsonl'))
        self.records: list[Json] = []

    def append(self, record: Json) -> None:
        self.records.extend(materialize_serving_records(record))

    def append_many(self, records: list[Json]) -> None:
        self.records.extend(materialize_serving_record_batch(records))


def checkpoint_key(target_prefix: str, job_id: str) -> str:
    return f'matrixark:backfill:{job_id}:checkpoint:{stable_hash(target_prefix)}'


def default_target_prefix(job_id: str) -> str:
    return f'matrixark:context_backfill:{job_id}'


def derive_backfill_record(source_prefix: str, sequence: int, raw_record: Json) -> Json:
    record = dict(raw_record)
    backfill = dict(record.get('backfill') or {})
    backfill.update({
        'source_prefix': source_prefix,
        'source_sequence': sequence,
        'source_record_type': raw_record.get('record_type', ''),
    })
    record['backfill'] = backfill
    if 'idempotency_key' not in record:
        seed = f'{source_prefix}:{sequence}:{json.dumps(raw_record, sort_keys=True)}'
        record['idempotency_key'] = f'backfill:{stable_hash(seed)}'
    return record


def should_backfill_record(raw_record: Json) -> bool:
    record_type = str(raw_record.get('record_type') or '')
    if record_type.startswith('context_') or record_type in {'resource_chunk', 'resource_manifest'}:
        return True
    return 'messages' in raw_record or 'kind' in raw_record or 'scope' in raw_record


def materialize_backfill_record(raw_record: Json) -> list[Json]:
    adapter = CaptureAdapter()
    if 'messages' in raw_record and not str(raw_record.get('record_type') or ''):
        adapter.ingest(raw_record, hook=raw_record.get('agent_hook'))
    else:
        adapter.append(raw_record)
    return adapter.records


def run_backfill(args: argparse.Namespace) -> Json:
    if args.mode == 'in_place' and args.confirm_in_place != 'YES':
        raise BackfillError('in-place mode requires --confirm-in-place=YES')
    if args.mode == 'shadow' and args.target_prefix == args.source_prefix:
        raise BackfillError('shadow mode requires target-prefix different from source-prefix')

    if args.local_kv:
        kv = LocalJsonKV(Path(args.local_kv))
    else:
        kv = TemporalStoreKV(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.library_path,
        )

    target_prefix = args.source_prefix if args.mode == 'in_place' else (args.target_prefix or default_target_prefix(args.job_id))
    source = MatrixKVRecordLog(kv, prefix=args.source_prefix)
    target = MatrixKVBackfillTarget(kv, prefix=target_prefix)
    metrics = BackfillMetrics()
    cp_key = checkpoint_key(target_prefix, args.job_id)
    start_seq = max(0, args.start_seq)
    if args.resume:
        raw_checkpoint = kv.get_string(cp_key)
        if raw_checkpoint:
            try:
                start_seq = max(start_seq, int(raw_checkpoint) + 1)
            except ValueError:
                pass

    seen_ids: set[str] = set()
    pending: list[Json] = []
    outer_bulk = hasattr(kv, 'begin_bulk') and hasattr(kv, 'end_bulk')
    if outer_bulk:
        kv.begin_bulk()

    def flush() -> None:
        nonlocal pending
        if not pending:
            return
        if not args.dry_run:
            target.append_many(pending)
        metrics.written += len(pending)
        metrics.observe_records(pending)
        pending = []

    count = source.count()
    if count > 0:
        stop = min(count, args.end_seq if args.end_seq is not None else count)
        source_items = ((sequence, None) for sequence in range(start_seq, stop))
    else:
        legacy_index = source.legacy_index()
        stop = min(len(legacy_index), args.end_seq if args.end_seq is not None else len(legacy_index))
        source_items = ((sequence, legacy_index[sequence]) for sequence in range(start_seq, stop))

    for sequence, legacy_record_id in source_items:
        metrics.scanned += 1
        raw_record: Json = {}
        try:
            raw_record = source.read_at(sequence) if legacy_record_id is None else source.read_legacy(legacy_record_id)
            if not should_backfill_record(raw_record):
                metrics.skipped += 1
                continue
            record = derive_backfill_record(args.source_prefix, sequence, raw_record)
            dedupe_id = str(record.get('idempotency_key') or f'{args.source_prefix}:{sequence}')
            if dedupe_id in seen_ids:
                metrics.duplicate += 1
                continue
            seen_ids.add(dedupe_id)
            materialized = materialize_backfill_record(record)
            if not materialized:
                metrics.skipped += 1
                continue
            pending.extend(materialized)
            if len(pending) >= args.batch_size:
                flush()
            if not args.dry_run:
                kv.put_string(cp_key, str(sequence))
        except Exception as exc:
            metrics.failed += 1
            metrics.dead_letter += 1
            if not args.dry_run:
                target.append_dead_letter({
                    'source_prefix': args.source_prefix,
                    'source_sequence': sequence,
                    'error': str(exc),
                    'record_preview': json.dumps(raw_record, sort_keys=True)[:2048],
                })
            if args.fail_fast:
                raise
    flush()
    if outer_bulk:
        kv.end_bulk()
    metrics.finish()
    summary = metrics.to_json(
        job_id=args.job_id,
        source_prefix=args.source_prefix,
        target_prefix=target_prefix,
        mode=args.mode,
    )
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(metrics.to_prometheus(job_id=args.job_id), encoding='utf-8')
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Backfill MatrixArk context records from MatrixKV raw ingestion logs.')
    parser.add_argument('--metaserver', default=os.environ.get('MATRIXARK_METASERVER', '127.0.0.1:65000'))
    parser.add_argument('--namespace', default=os.environ.get('MATRIXARK_NAMESPACE', 'matrixark'))
    parser.add_argument('--table', default=os.environ.get('MATRIXARK_TABLE', 'context'))
    parser.add_argument('--library-path', default=os.environ.get('TEMPORALSTORE_LIBRARY_PATH', ''))
    parser.add_argument('--source-prefix', default='matrixark:mcp')
    parser.add_argument('--target-prefix', default='')
    parser.add_argument('--mode', choices=['shadow', 'in_place'], default='shadow')
    parser.add_argument('--confirm-in-place', default='')
    parser.add_argument('--job-id', default=f'local-{int(time.time())}')
    parser.add_argument('--start-seq', type=int, default=0)
    parser.add_argument('--end-seq', type=int)
    parser.add_argument('--batch-size', type=int, default=256)
    parser.add_argument('--dry-run', type=int, choices=[0, 1], default=1)
    parser.add_argument('--resume', type=int, choices=[0, 1], default=1)
    parser.add_argument('--fail-fast', action='store_true')
    parser.add_argument('--prometheus-output', default='')
    parser.add_argument('--local-kv', default='', help='test-only JSON KV backend path')
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.dry_run = bool(args.dry_run)
    args.resume = bool(args.resume)
    if args.batch_size <= 0:
        parser.error('--batch-size must be positive')
    try:
        summary = run_backfill(args)
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'error': str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())