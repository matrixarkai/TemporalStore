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
from argparse import Namespace
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
    filtered: int = 0
    context_events: int = 0
    context_entities: int = 0
    context_summaries: int = 0
    context_embeddings: int = 0
    context_indexes: int = 0
    context_audits: int = 0
    source_batches: int = 0
    target_batches: int = 0
    scan_hash_batches: int = 0
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

    def to_json(
        self,
        *,
        job_id: str,
        source_prefix: str,
        target_prefix: str,
        mode: str,
        raw_backend: str,
        partial: Json | None = None,
    ) -> Json:
        elapsed_ms = max(0, (self.finished_at_ms or int(time.time() * 1000)) - self.started_at_ms)
        qps = (self.scanned * 1000.0 / elapsed_ms) if elapsed_ms else 0.0
        return {
            'status': 'ok',
            'job_id': job_id,
            'source_prefix': source_prefix,
            'target_prefix': target_prefix,
            'mode': mode,
            'raw_backend': raw_backend,
            'partial': partial or {},
            'elapsed_ms': elapsed_ms,
            'scan_qps': round(qps, 3),
            'metrics': {
                'scanned': self.scanned,
                'skipped': self.skipped,
                'written': self.written,
                'duplicate': self.duplicate,
                'failed': self.failed,
                'dead_letter': self.dead_letter,
                'filtered': self.filtered,
                'context_events': self.context_events,
                'context_entities': self.context_entities,
                'context_summaries': self.context_summaries,
                'context_embeddings': self.context_embeddings,
                'context_indexes': self.context_indexes,
                'context_audits': self.context_audits,
                'source_batches': self.source_batches,
                'target_batches': self.target_batches,
                'scan_hash_batches': self.scan_hash_batches,
            },
        }

    def to_prometheus(self, *, job_id: str, raw_backend: str) -> str:
        labels = f'job_id="{job_id}",raw_backend="{raw_backend}"'
        lines = [
            '# HELP matrixark_context_backfill_records_total Records processed by context backfill.',
            '# TYPE matrixark_context_backfill_records_total counter',
        ]
        for name in ['scanned', 'skipped', 'written', 'duplicate', 'failed', 'dead_letter', 'filtered']:
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
            '# HELP matrixark_context_backfill_batches_total Source and target batches processed.',
            '# TYPE matrixark_context_backfill_batches_total counter',
            f'matrixark_context_backfill_batches_total{{{labels},phase="source"}} {self.source_batches}',
            f'matrixark_context_backfill_batches_total{{{labels},phase="target"}} {self.target_batches}',
            f'matrixark_context_backfill_batches_total{{{labels},phase="scan_hash"}} {self.scan_hash_batches}',
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

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        batch_hget = getattr(self.client, 'batch_hget', None)
        if callable(batch_hget):
            return list(batch_hget(entries))
        return [
            {
                'key': str(entry.get('key') or ''),
                'field': str(entry.get('field') or ''),
                'value': self.hget(str(entry.get('key') or ''), str(entry.get('field') or '')),
            }
            for entry in entries
        ]

    def batch_hset(self, entries: list[Json]) -> None:
        batch_hset = getattr(self.client, 'batch_hset', None)
        if callable(batch_hset):
            batch_hset(entries)
            return
        for entry in entries:
            self.hset(str(entry.get('key') or ''), str(entry.get('field') or ''), str(entry.get('value') or ''))

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        append_records = getattr(self.client, 'matrixark_append_records', None)
        if callable(append_records):
            append_records(entries, count_key=count_key, count_value=count_value, append_options=append_options)
            return
        self.batch_hset(entries)
        if count_key is not None and count_value is not None:
            self.put_string(count_key, count_value)

    def scan_hash(self, key: str) -> Json:
        scan_hash = getattr(self.client, 'scan_hash', None)
        if callable(scan_hash):
            result = scan_hash(key)
            return result if isinstance(result, dict) else {}
        return {}


class LocalJsonKV:
    """Small test backend with TemporalStore-like string/hash operations."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._bulk_depth = 0
        self.batch_hget_calls = 0
        self.batch_hset_calls = 0
        self.matrixark_append_records_calls = 0
        self.matrixark_append_records_options: list[Json] = []
        self.scan_hash_calls = 0
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

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        self.batch_hget_calls += 1
        return [
            {
                'key': str(entry.get('key') or ''),
                'field': str(entry.get('field') or ''),
                'value': self.hget(str(entry.get('key') or ''), str(entry.get('field') or '')),
            }
            for entry in entries
        ]

    def batch_hset(self, entries: list[Json]) -> None:
        self.batch_hset_calls += 1
        close_bulk = False
        if self._bulk_depth == 0:
            self.begin_bulk()
            close_bulk = True
        try:
            for entry in entries:
                self.hset(str(entry.get('key') or ''), str(entry.get('field') or ''), str(entry.get('value') or ''))
        finally:
            if close_bulk:
                self.end_bulk()

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        self.matrixark_append_records_calls += 1
        self.matrixark_append_records_options.append(dict(append_options or {}))
        self.batch_hset(entries)
        if count_key is not None and count_value is not None:
            self.put_string(count_key, count_value)

    def scan_hash(self, key: str) -> Json:
        self.scan_hash_calls += 1
        return dict(self.data['hashes'].get(key, {}))


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

    def read_many(self, refs: list[tuple[int, str | None]]) -> list[tuple[int, Json | None, Exception | None]]:
        batch_hget = getattr(self.kv, 'batch_hget', None)
        if not callable(batch_hget):
            return [self._read_one_ref(sequence, legacy_record_id) for sequence, legacy_record_id in refs]
        entries: list[Json] = []
        for sequence, legacy_record_id in refs:
            if legacy_record_id is None:
                shard = sequence // self.shard_size
                offset = sequence % self.shard_size
                entries.append({'key': f'{self.prefix}:records:{shard:06d}', 'field': f'{offset:020d}', 'sequence': sequence})
            else:
                entries.append({'key': f'{self.prefix}:records', 'field': legacy_record_id, 'sequence': sequence})
        try:
            rows = list(batch_hget(entries))
        except Exception:
            return [self._read_one_ref(sequence, legacy_record_id) for sequence, legacy_record_id in refs]
        rows_by_ref: dict[tuple[str, str], Json] = {}
        for row in rows:
            if isinstance(row, dict) and ('key' in row or 'field' in row):
                rows_by_ref[(str(row.get('key') or ''), str(row.get('field') or ''))] = row
        results: list[tuple[int, Json | None, Exception | None]] = []
        for index, (sequence, legacy_record_id) in enumerate(refs):
            if legacy_record_id is None:
                shard = sequence // self.shard_size
                offset = sequence % self.shard_size
                ref_key = (f'{self.prefix}:records:{shard:06d}', f'{offset:020d}')
            else:
                ref_key = (f'{self.prefix}:records', legacy_record_id)
            row = rows_by_ref.get(ref_key, rows[index] if index < len(rows) else {})
            payload = row if isinstance(row, str) else str((row or {}).get('value') or '')
            if not payload:
                error = BackfillError(f'missing legacy record {legacy_record_id}' if legacy_record_id is not None else f'missing sharded record at sequence {sequence}')
                results.append((sequence, None, error))
                continue
            try:
                decoded = json.loads(payload)
            except Exception as exc:
                results.append((sequence, None, exc))
                continue
            results.append((sequence, decoded, None))
        return results

    def _read_one_ref(self, sequence: int, legacy_record_id: str | None) -> tuple[int, Json | None, Exception | None]:
        try:
            record = self.read_at(sequence) if legacy_record_id is None else self.read_legacy(legacy_record_id)
            return sequence, record, None
        except Exception as exc:
            return sequence, None, exc

    def source_range(self, *, start_seq: int, end_seq: int | None) -> Json:
        effective_start = max(0, start_seq)
        count = self.count()
        if count > 0:
            effective_end = min(count, end_seq if end_seq is not None else count)
            return {
                'scan_mode': 'record_count',
                'requested_start_seq': start_seq,
                'requested_end_seq': end_seq,
                'effective_start_seq': effective_start,
                'effective_end_seq': effective_end,
                'source_record_count': count,
                'source_high_watermark_seq': count - 1,
                'user_bounded_end': end_seq is not None,
            }
        index = self.legacy_index()
        if index:
            effective_end = min(len(index), end_seq if end_seq is not None else len(index))
            return {
                'scan_mode': 'record_index',
                'requested_start_seq': start_seq,
                'requested_end_seq': end_seq,
                'effective_start_seq': effective_start,
                'effective_end_seq': effective_end,
                'source_record_count': len(index),
                'source_high_watermark_seq': len(index) - 1,
                'user_bounded_end': end_seq is not None,
            }
        scan_hash = getattr(self.kv, 'scan_hash', None)
        if callable(scan_hash):
            return {
                'scan_mode': 'scan_hash',
                'requested_start_seq': start_seq,
                'requested_end_seq': end_seq,
                'effective_start_seq': effective_start,
                'effective_end_seq': end_seq,
                'source_record_count': None,
                'source_high_watermark_seq': None,
                'user_bounded_end': end_seq is not None,
            }
        return {
            'scan_mode': 'empty',
            'requested_start_seq': start_seq,
            'requested_end_seq': end_seq,
            'effective_start_seq': effective_start,
            'effective_end_seq': effective_start,
            'source_record_count': 0,
            'source_high_watermark_seq': None,
            'user_bounded_end': end_seq is not None,
        }

    def source_refs(
        self,
        *,
        start_seq: int,
        end_seq: int | None,
        max_empty_scan_shards: int,
        source_range: Json | None = None,
    ) -> tuple[Iterable[tuple[int, str | None]], str]:
        range_info = source_range or self.source_range(start_seq=start_seq, end_seq=end_seq)
        scan_mode = str(range_info.get('scan_mode') or 'empty')
        effective_start = int(range_info.get('effective_start_seq') or 0)
        effective_end = range_info.get('effective_end_seq')
        if scan_mode == 'record_count':
            stop = int(effective_end or 0)
            return ((sequence, None) for sequence in range(effective_start, stop)), scan_mode
        if scan_mode == 'record_index':
            index = self.legacy_index()
            stop = min(len(index), int(effective_end if effective_end is not None else len(index)))
            return ((sequence, index[sequence]) for sequence in range(effective_start, stop)), scan_mode
        if scan_mode == 'scan_hash':
            return self._scan_sharded_refs(start_seq=effective_start, end_seq=end_seq, max_empty_scan_shards=max_empty_scan_shards), scan_mode
        return iter(()), scan_mode

    def _scan_sharded_refs(self, *, start_seq: int, end_seq: int | None, max_empty_scan_shards: int) -> Iterable[tuple[int, str | None]]:
        first_shard = start_seq // self.shard_size
        last_shard = (end_seq - 1) // self.shard_size if end_seq is not None and end_seq > 0 else None
        empty_seen = 0
        shard = first_shard
        while True:
            if last_shard is not None and shard > last_shard:
                break
            payload = self.kv.scan_hash(f'{self.prefix}:records:{shard:06d}')
            fields = self._scan_hash_fields(payload)
            if not fields:
                empty_seen += 1
                if last_shard is None and empty_seen >= max_empty_scan_shards:
                    break
            else:
                empty_seen = 0
                for field in fields:
                    try:
                        offset = int(field)
                    except ValueError:
                        continue
                    sequence = shard * self.shard_size + offset
                    if sequence < start_seq:
                        continue
                    if end_seq is not None and sequence >= end_seq:
                        continue
                    yield sequence, None
            shard += 1

    @staticmethod
    def _scan_hash_fields(payload: Json) -> list[str]:
        if not payload:
            return []
        if isinstance(payload.get('fields'), dict):
            return sorted(str(field) for field in payload['fields'].keys())
        if isinstance(payload.get('records'), dict):
            return sorted(str(field) for field in payload['records'].keys())
        if isinstance(payload.get('items'), list):
            fields = []
            for item in payload['items']:
                if isinstance(item, dict) and 'field' in item:
                    fields.append(str(item.get('field') or ''))
                elif isinstance(item, (list, tuple)) and item:
                    fields.append(str(item[0]))
            return sorted(field for field in fields if field)
        return sorted(str(field) for field in payload.keys())

    def iter_records(self, *, start_seq: int, end_seq: int | None) -> Iterable[tuple[int, Json]]:
        refs, _ = self.source_refs(start_seq=start_seq, end_seq=end_seq, max_empty_scan_shards=1)
        for sequence, legacy_record_id in refs:
            yield sequence, self.read_at(sequence) if legacy_record_id is None else self.read_legacy(legacy_record_id)


class MatrixKVBackfillTarget:
    def __init__(
        self,
        kv: Any,
        *,
        prefix: str,
        raw_backend: str = 'temporalstore',
        shard_size: int = DIRECT_RECORD_LOG_SHARD_SIZE,
    ) -> None:
        self.kv = kv
        self.prefix = prefix.rstrip(':')
        self.raw_backend = normalize_raw_backend(raw_backend)
        self.shard_size = shard_size
        self._next_sequence: int | None = None

    def count(self) -> int:
        raw = self.kv.get_string(f'{self.prefix}:record_count')
        try:
            return max(0, int(raw)) if raw else 0
        except ValueError:
            return 0

    def _idempotency_key(self, record: Json) -> str:
        key = str(record.get('idempotency_key') or '')
        if key:
            return key
        backfill = record.get('backfill') if isinstance(record.get('backfill'), dict) else {}
        return str((backfill or {}).get('idempotency_key') or '')

    def has_idempotency_key(self, key: str) -> bool:
        return bool(key and self.kv.hget(f'{self.prefix}:idempotency', key))

    def existing_idempotency_keys(self, keys: list[str]) -> set[str]:
        unique_keys = sorted({key for key in keys if key})
        if not unique_keys:
            return set()
        batch_hget = getattr(self.kv, 'batch_hget', None)
        if not callable(batch_hget):
            return {key for key in unique_keys if self.has_idempotency_key(key)}
        entries = [{'key': f'{self.prefix}:idempotency', 'field': key} for key in unique_keys]
        try:
            rows = list(batch_hget(entries))
        except Exception:
            return {key for key in unique_keys if self.has_idempotency_key(key)}
        existing: set[str] = set()
        for index, row in enumerate(rows):
            if isinstance(row, dict):
                key = str(row.get('field') or unique_keys[index])
                value = str(row.get('value') or '')
            else:
                key = unique_keys[index]
                value = str(row or '')
            if value:
                existing.add(key)
        return existing

    def append_many(self, records: list[Json]) -> None:
        if not records:
            return
        if self._next_sequence is None:
            self._next_sequence = self.count()
        sequence = self._next_sequence
        entries: list[Json] = []
        idempotency_entries: list[Json] = []
        dedupe_keys = [self._idempotency_key(record) for record in records]
        existing_keys = self.existing_idempotency_keys(dedupe_keys)
        if hasattr(self.kv, 'begin_bulk'):
            self.kv.begin_bulk()
        try:
            for record, dedupe_key in zip(records, dedupe_keys):
                shard = sequence // self.shard_size
                offset = sequence % self.shard_size
                if dedupe_key and dedupe_key in existing_keys:
                    continue
                payload = json.dumps(record, sort_keys=True, separators=(',', ':'))
                entries.append({'key': f'{self.prefix}:records:{shard:06d}', 'field': f'{offset:020d}', 'value': payload})
                if dedupe_key:
                    idempotency_entries.append({'key': f'{self.prefix}:idempotency', 'field': dedupe_key, 'value': str(sequence)})
                sequence += 1
            append_records = getattr(self.kv, 'matrixark_append_records', None)
            all_entries = entries + idempotency_entries
            if callable(append_records):
                append_records(
                    all_entries,
                    count_key=f'{self.prefix}:record_count',
                    count_value=str(sequence),
                    append_options={
                        'append_path': 'matrixark_context_backfill_target',
                        'source': 'matrixark_context_backfill',
                        'raw_storage_backend': self.raw_backend,
                    },
                )
            else:
                batch_hset = getattr(self.kv, 'batch_hset', None)
                if callable(batch_hset):
                    batch_hset(all_entries)
                else:
                    for entry in all_entries:
                        self.kv.hset(str(entry['key']), str(entry['field']), str(entry['value']))
                self.kv.put_string(f'{self.prefix}:record_count', str(sequence))
            self._next_sequence = sequence
        finally:
            if hasattr(self.kv, 'end_bulk'):
                self.kv.end_bulk()

    def count_dead_letters(self) -> int:
        raw = self.kv.get_string(f'{self.prefix}:dead_letter_count')
        try:
            return max(0, int(raw)) if raw else 0
        except ValueError:
            return 0

    def read_at(self, sequence: int) -> Json:
        shard = sequence // self.shard_size
        offset = sequence % self.shard_size
        payload = self.kv.hget(f'{self.prefix}:records:{shard:06d}', f'{offset:020d}')
        if not payload:
            raise BackfillError(f'missing target record at sequence {sequence}')
        return json.loads(payload)

    def serving_type_counts(self) -> Json:
        counts: Json = {}
        for sequence in range(self.count()):
            record = self.read_at(sequence)
            record_type = str(record.get('record_type') or 'unknown')
            counts[record_type] = int(counts.get(record_type, 0)) + 1
        return dict(sorted(counts.items()))

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


def normalize_raw_backend(value: str) -> str:
    backend = str(value or 'temporalstore').strip().lower().replace('-', '_')
    if backend in {'', 'temporal', 'temporal_store', 'ts'}:
        backend = 'temporalstore'
    if backend in {'matrix_kv', 'kv'}:
        backend = 'matrixkv'
    if backend not in {'temporalstore', 'matrixkv'}:
        raise BackfillError('--raw-backend must be temporalstore or matrixkv')
    return backend


def checkpoint_key(
    target_prefix: str,
    job_id: str,
    *,
    source_prefix: str = '',
    raw_backend: str = 'temporalstore',
    partial: Json | None = None,
) -> str:
    fingerprint = partial_checkpoint_fingerprint(
        source_prefix,
        target_prefix,
        normalize_raw_backend(raw_backend),
        partial or {},
    )
    return f'matrixark:backfill:{job_id}:checkpoint:{fingerprint}'


def read_checkpoint_sequence(kv: Any, key: str) -> int | None:
    raw_checkpoint = kv.get_string(key)
    if not raw_checkpoint:
        return None
    try:
        return int(raw_checkpoint)
    except ValueError:
        pass
    try:
        checkpoint = json.loads(raw_checkpoint)
    except json.JSONDecodeError:
        return None
    if not isinstance(checkpoint, dict):
        return None
    value = checkpoint.get('last_sequence')
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def read_checkpoint_state(kv: Any, key: str) -> Json:
    raw_checkpoint = kv.get_string(key)
    state: Json = {
        'checkpoint_key': key,
        'checkpoint_found': bool(raw_checkpoint),
        'checkpoint_format': 'missing',
        'checkpoint_last_sequence': None,
        'checkpoint_source_range': None,
        'checkpoint_updated_at_ms': None,
    }
    if not raw_checkpoint:
        return state
    try:
        state['checkpoint_last_sequence'] = int(raw_checkpoint)
        state['checkpoint_format'] = 'legacy_integer'
        return state
    except ValueError:
        pass
    try:
        checkpoint = json.loads(raw_checkpoint)
    except json.JSONDecodeError:
        state['checkpoint_format'] = 'invalid_json'
        return state
    if not isinstance(checkpoint, dict):
        state['checkpoint_format'] = 'invalid_type'
        return state
    state['checkpoint_format'] = 'json'
    if isinstance(checkpoint.get('source_range'), dict):
        state['checkpoint_source_range'] = checkpoint.get('source_range')
    if checkpoint.get('updated_at_ms') is not None:
        state['checkpoint_updated_at_ms'] = checkpoint.get('updated_at_ms')
    value = checkpoint.get('last_sequence')
    try:
        state['checkpoint_last_sequence'] = int(value)
    except (TypeError, ValueError):
        state['checkpoint_format'] = 'json_missing_last_sequence'
    return state


def build_checkpoint_metadata(
    *,
    job_id: str,
    source_prefix: str,
    target_prefix: str,
    raw_backend: str,
    mode: str,
    partial: Json,
    source_range: Json | None,
    batch_size: int,
    last_sequence: int,
    metrics: 'BackfillMetrics',
) -> Json:
    return {
        'version': 2,
        'job_id': job_id,
        'source_prefix': source_prefix,
        'target_prefix': target_prefix,
        'raw_backend': normalize_raw_backend(raw_backend),
        'mode': mode,
        'partial': partial,
        'source_range': source_range or {},
        'batch_size': batch_size,
        'last_sequence': last_sequence,
        'updated_at_ms': int(time.time() * 1000),
        'metrics': {
            'scanned': metrics.scanned,
            'written': metrics.written,
            'duplicate': metrics.duplicate,
            'failed': metrics.failed,
            'dead_letter': metrics.dead_letter,
            'source_batches': metrics.source_batches,
            'target_batches': metrics.target_batches,
        },
    }


def default_target_prefix(job_id: str) -> str:
    return f'matrixark:context_backfill:{job_id}'


def make_kv(args: argparse.Namespace) -> Any:
    if args.local_kv:
        return LocalJsonKV(Path(args.local_kv))
    return TemporalStoreKV(
        metaserver=args.metaserver,
        namespace=args.namespace,
        table=args.table,
        library_path=args.library_path,
    )


def resolve_target_prefix(args: argparse.Namespace) -> str:
    return args.source_prefix if args.mode == 'in_place' else (args.target_prefix or default_target_prefix(args.job_id))


def clone_args(args: argparse.Namespace, **overrides: Any) -> argparse.Namespace:
    values = vars(args).copy()
    values.update(overrides)
    return Namespace(**values)


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in str(value or '').split(',') if item.strip()}


def _scope_value(record: Json, name: str) -> str:
    scope = record.get('scope') if isinstance(record.get('scope'), dict) else {}
    for key in (name, name.replace('_id', '')):
        value = record.get(key)
        if value not in (None, ''):
            return str(value)
        value = scope.get(key) if isinstance(scope, dict) else None
        if value not in (None, ''):
            return str(value)
    return ''


def build_partial_spec(args: argparse.Namespace) -> Json:
    filter_json = getattr(args, 'partial_filter_json', '') or ''
    parsed_filter: Json = {}
    if filter_json:
        try:
            decoded = json.loads(filter_json)
        except json.JSONDecodeError as exc:
            raise BackfillError(f'invalid --partial-filter-json: {exc}') from exc
        if not isinstance(decoded, dict):
            raise BackfillError('--partial-filter-json must decode to a JSON object')
        parsed_filter = decoded
    spec: Json = {
        'enabled': bool(getattr(args, 'partial', False)),
        'record_types': sorted(_csv_set(getattr(args, 'partial_record_types', '') or '')),
        'tenant_ids': sorted(_csv_set(getattr(args, 'partial_tenant_ids', '') or '')),
        'user_ids': sorted(_csv_set(getattr(args, 'partial_user_ids', '') or '')),
        'session_ids': sorted(_csv_set(getattr(args, 'partial_session_ids', '') or '')),
        'filter_json': parsed_filter,
    }
    if any(value for key, value in spec.items() if key != 'enabled'):
        spec['enabled'] = True
    return spec


def validate_partial_args(args: argparse.Namespace, partial: Json) -> None:
    if not partial.get('enabled'):
        return
    has_filter = any(partial.get(key) for key in ['record_types', 'tenant_ids', 'user_ids', 'session_ids']) or bool(partial.get('filter_json'))
    has_bounded_range = args.end_seq is not None and args.end_seq > args.start_seq
    if getattr(args, 'partial_require_bounded', True) and not (has_bounded_range or has_filter):
        raise BackfillError('partial backfill requires --end-seq or at least one partial filter')


def record_matches_partial(raw_record: Json, partial: Json) -> bool:
    if not partial.get('enabled'):
        return True
    record_types = set(partial.get('record_types') or [])
    if record_types and str(raw_record.get('record_type') or '') not in record_types:
        return False
    checks = [
        ('tenant_ids', 'tenant_id'),
        ('user_ids', 'user_id'),
        ('session_ids', 'session_id'),
    ]
    for spec_key, record_key in checks:
        allowed = set(partial.get(spec_key) or [])
        if allowed and _scope_value(raw_record, record_key) not in allowed:
            return False
    filter_json = partial.get('filter_json') if isinstance(partial.get('filter_json'), dict) else {}
    for key, expected in filter_json.items():
        if key == 'scope' and isinstance(expected, dict):
            scope = raw_record.get('scope') if isinstance(raw_record.get('scope'), dict) else {}
            if any(scope.get(scope_key) != scope_value for scope_key, scope_value in expected.items()):
                return False
        elif raw_record.get(key) != expected:
            return False
    return True


def partial_checkpoint_fingerprint(source_prefix: str, target_prefix: str, raw_backend: str, partial: Json) -> str:
    seed = json.dumps({
        'source_prefix': source_prefix,
        'target_prefix': target_prefix,
        'raw_backend': normalize_raw_backend(raw_backend),
        'partial': partial,
    }, sort_keys=True, separators=(',', ':'))
    return stable_hash(seed)


def derive_backfill_idempotency_key(source_prefix: str, raw_backend: str, sequence: int, raw_record: Json) -> str:
    key = str(raw_record.get('idempotency_key') or '')
    if key:
        return key
    seed = f'{normalize_raw_backend(raw_backend)}:{source_prefix}:{sequence}:{json.dumps(raw_record, sort_keys=True)}'
    return f'backfill:{stable_hash(seed)}'


def derive_backfill_record(source_prefix: str, raw_backend: str, sequence: int, raw_record: Json) -> Json:
    record = dict(raw_record)
    backfill = dict(record.get('backfill') or {})
    backfill.update({
        'source_prefix': source_prefix,
        'raw_backend': normalize_raw_backend(raw_backend),
        'source_sequence': sequence,
        'source_record_type': raw_record.get('record_type', ''),
    })
    record['backfill'] = backfill
    if 'idempotency_key' not in record:
        record['idempotency_key'] = derive_backfill_idempotency_key(source_prefix, raw_backend, sequence, raw_record)
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

    partial = build_partial_spec(args)
    validate_partial_args(args, partial)
    raw_backend = normalize_raw_backend(args.raw_backend)
    kv = make_kv(args)

    target_prefix = resolve_target_prefix(args)
    source = MatrixKVRecordLog(kv, prefix=args.source_prefix)
    target = MatrixKVBackfillTarget(kv, prefix=target_prefix, raw_backend=raw_backend)
    metrics = BackfillMetrics()
    cp_key = checkpoint_key(
        target_prefix,
        args.job_id,
        source_prefix=args.source_prefix,
        raw_backend=raw_backend,
        partial=partial,
    )
    start_seq = max(0, args.start_seq)
    checkpoint: Json | None = None
    resume_state = read_checkpoint_state(kv, cp_key)
    resume_state.update({
        'resume_requested': bool(args.resume),
        'requested_start_seq': args.start_seq,
        'effective_start_seq': start_seq,
    })
    if args.resume:
        checkpoint_sequence = resume_state.get('checkpoint_last_sequence')
        if checkpoint_sequence is not None:
            start_seq = max(start_seq, checkpoint_sequence + 1)
            resume_state['effective_start_seq'] = start_seq

    seen_ids: set[str] = set()
    pending: list[Json] = []
    checkpoint_pending_seq: int | None = None
    outer_bulk = hasattr(kv, 'begin_bulk') and hasattr(kv, 'end_bulk')

    def flush() -> None:
        nonlocal pending, checkpoint, checkpoint_pending_seq
        if not pending and checkpoint_pending_seq is None:
            return
        if not args.dry_run:
            if pending:
                target.append_many(pending)
        if pending:
            metrics.target_batches += 1
            metrics.written += len(pending)
            metrics.observe_records(pending)
        if not args.dry_run:
            if checkpoint_pending_seq is not None:
                checkpoint = build_checkpoint_metadata(
                    job_id=args.job_id,
                    source_prefix=args.source_prefix,
                    target_prefix=target_prefix,
                    raw_backend=raw_backend,
                    mode=args.mode,
                    partial=partial,
                    source_range=source_range,
                    batch_size=args.batch_size,
                    last_sequence=checkpoint_pending_seq,
                    metrics=metrics,
                )
                kv.put_string(cp_key, json.dumps(checkpoint, sort_keys=True, separators=(',', ':')))
        pending = []
        checkpoint_pending_seq = None

    def handle_failure(sequence: int, raw_record: Json, exc: Exception) -> None:
        nonlocal checkpoint_pending_seq
        metrics.failed += 1
        metrics.dead_letter += 1
        if not args.dry_run:
            target.append_dead_letter({
                'source_prefix': args.source_prefix,
                'source_sequence': sequence,
                'error': str(exc),
                'record_preview': json.dumps(raw_record, sort_keys=True)[:2048],
            })
        checkpoint_pending_seq = sequence
        if args.fail_fast:
            raise exc

    def process_raw_record(sequence: int, raw_record: Json, existing_dedupe_ids: set[str] | None = None) -> None:
        nonlocal checkpoint_pending_seq
        try:
            if not record_matches_partial(raw_record, partial):
                metrics.filtered += 1
                checkpoint_pending_seq = sequence
                return
            if not should_backfill_record(raw_record):
                metrics.skipped += 1
                checkpoint_pending_seq = sequence
                return
            record = derive_backfill_record(args.source_prefix, raw_backend, sequence, raw_record)
            dedupe_id = str(record.get('idempotency_key') or f'{args.source_prefix}:{sequence}')
            exists_in_target = False
            if not args.dry_run:
                exists_in_target = dedupe_id in existing_dedupe_ids if existing_dedupe_ids is not None else target.has_idempotency_key(dedupe_id)
            if dedupe_id in seen_ids or exists_in_target:
                metrics.duplicate += 1
                checkpoint_pending_seq = sequence
                return
            seen_ids.add(dedupe_id)
            materialized = materialize_backfill_record(record)
            if not materialized:
                metrics.skipped += 1
                checkpoint_pending_seq = sequence
                return
            for item in materialized:
                if not item.get('idempotency_key'):
                    item['idempotency_key'] = dedupe_id
            pending.extend(materialized)
            checkpoint_pending_seq = sequence
            if len(pending) >= args.batch_size:
                flush()
        except Exception as exc:
            handle_failure(sequence, raw_record, exc)

    def process_source_batch(batch: list[tuple[int, str | None]]) -> None:
        if not batch:
            return
        metrics.source_batches += 1
        if scan_mode == 'scan_hash':
            metrics.scan_hash_batches += 1
        rows = source.read_many(batch)
        existing_dedupe_ids: set[str] | None = None
        if not args.dry_run:
            dedupe_candidates = [
                derive_backfill_idempotency_key(args.source_prefix, raw_backend, sequence, raw_record or {})
                for sequence, raw_record, read_error in rows
                if read_error is None
                and record_matches_partial(raw_record or {}, partial)
                and should_backfill_record(raw_record or {})
            ]
            existing_dedupe_ids = target.existing_idempotency_keys(dedupe_candidates)
        for sequence, raw_record, read_error in rows:
            metrics.scanned += 1
            if read_error is not None:
                handle_failure(sequence, {}, read_error)
                continue
            process_raw_record(sequence, raw_record or {}, existing_dedupe_ids)

    source_range = source.source_range(start_seq=start_seq, end_seq=args.end_seq)
    source_items, scan_mode = source.source_refs(
        start_seq=start_seq,
        end_seq=args.end_seq,
        max_empty_scan_shards=args.source_scan_max_empty_shards,
        source_range=source_range,
    )

    if outer_bulk:
        kv.begin_bulk()
    try:
        source_batch: list[tuple[int, str | None]] = []
        for item in source_items:
            source_batch.append(item)
            if len(source_batch) >= args.batch_size:
                process_source_batch(source_batch)
                source_batch = []
        process_source_batch(source_batch)
        flush()
    finally:
        if outer_bulk:
            kv.end_bulk()

    metrics.finish()
    summary = metrics.to_json(
        job_id=args.job_id,
        source_prefix=args.source_prefix,
        target_prefix=target_prefix,
        mode=args.mode,
        raw_backend=raw_backend,
        partial=partial,
    )
    summary['resume_state'] = resume_state
    summary['source_range'] = source_range
    manifest = {
        'job_id': args.job_id,
        'mode': args.mode,
        'source_prefix': args.source_prefix,
        'raw_backend': raw_backend,
        'target_prefix': target_prefix,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': partial,
        'checkpoint_key': cp_key,
        'checkpoint': checkpoint,
        'resume_state': resume_state,
        'source_range': source_range,
        'summary': summary,
    }
    summary['manifest_key'] = f'{target_prefix}:backfill_manifest'
    if not args.dry_run:
        kv.hset(summary['manifest_key'], args.job_id, json.dumps(manifest, sort_keys=True, separators=(',', ':')))
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(
            metrics.to_prometheus(job_id=args.job_id, raw_backend=raw_backend),
            encoding='utf-8',
        )
    return summary


SERVING_TYPE_METRIC_MAP = {
    'context_event': 'context_events',
    'context_entity': 'context_entities',
    'context_summary': 'context_summaries',
    'context_embedding': 'context_embeddings',
    'context_index': 'context_indexes',
    'context_pack_audit': 'context_audits',
}


def expected_serving_type_counts(metrics: Json) -> Json:
    counts = {
        record_type: int(metrics.get(metric_name, 0) or 0)
        for record_type, metric_name in SERVING_TYPE_METRIC_MAP.items()
        if int(metrics.get(metric_name, 0) or 0) > 0
    }
    accounted = sum(counts.values())
    written = int(metrics.get('written', 0) or 0)
    if written > accounted:
        counts['other'] = written - accounted
    return dict(sorted(counts.items()))


def run_validate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('validate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('validate_shadow target-prefix must differ from source-prefix')
    validation_args = Namespace(**vars(args))
    validation_args.mode = 'shadow'
    validation_args.dry_run = True
    validation_args.resume = False
    validation_args.prometheus_output = ''
    expected_summary = run_backfill(validation_args)
    kv = make_kv(args)
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    actual_count = target.count()
    dead_letters = target.count_dead_letters()
    expected_count = int(expected_summary['metrics']['written'])
    expected_type_counts = expected_serving_type_counts(expected_summary['metrics'])
    actual_type_counts = target.serving_type_counts()
    target_state: Json = {
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'record_count': actual_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': actual_type_counts,
    }
    exact_match = actual_count == expected_count
    enough_records = actual_count >= expected_count
    exact_type_match = actual_type_counts == expected_type_counts
    enough_type_records = all(int(actual_type_counts.get(record_type, 0)) >= int(count) for record_type, count in expected_type_counts.items())
    type_counts_passed = exact_type_match if args.validation_strict else enough_type_records
    passed = (exact_match if args.validation_strict else enough_records) and type_counts_passed and dead_letters == 0 and int(expected_summary['metrics']['failed']) == 0
    return {
        'status': 'ok' if passed else 'failed',
        'job_id': args.job_id,
        'mode': 'validate_shadow',
        'source_prefix': args.source_prefix,
        'raw_backend': raw_backend,
        'target_prefix': args.target_prefix,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation_strict': bool(args.validation_strict),
        'expected_records': expected_count,
        'actual_records': actual_count,
        'expected_type_counts': expected_type_counts,
        'actual_type_counts': actual_type_counts,
        'dead_letters': dead_letters,
        'expected_scan': expected_summary['metrics'],
        'source_range': expected_summary.get('source_range', {}),
        'target_state': target_state,
        'checks': {
            'exact_record_count_match': exact_match,
            'actual_records_at_least_expected': enough_records,
            'exact_serving_type_counts_match': exact_type_match,
            'actual_serving_type_counts_at_least_expected': enough_type_records,
            'no_shadow_dead_letters': dead_letters == 0,
            'source_scan_had_no_failures': int(expected_summary['metrics']['failed']) == 0,
        },
    }


def run_activate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('activate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('activate_shadow target-prefix must differ from source-prefix')
    if args.confirm_activate != 'YES':
        raise BackfillError('activate_shadow requires --confirm-activate=YES')
    validation: Json | None = None
    if not args.skip_validation:
        validation = run_validate_shadow(args)
        if validation.get('status') != 'ok':
            raise BackfillError(f'shadow validation failed: {json.dumps(validation, sort_keys=True)}')
    if args.dry_run:
        return {
            'status': 'ok',
            'mode': 'activate_shadow',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'target_prefix': args.target_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'validation': validation,
        }
    kv = make_kv(args)
    previous = kv.get_string(args.active_prefix_key)
    activated_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'activated_at_ms': activated_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'source_prefix': args.source_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation': validation,
    }
    kv.put_string(f'{args.active_prefix_key}:previous:{args.job_id}', previous)
    kv.hset(f'{args.active_prefix_key}:audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, args.target_prefix)
    return {
        'status': 'ok',
        'mode': 'activate_shadow',
        'active_prefix_key': args.active_prefix_key,
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:audit',
        'job_id': args.job_id,
        'validation': validation,
    }


def run_rollback_activation(args: argparse.Namespace) -> Json:
    rollback_job_id = str(getattr(args, 'rollback_job_id', '') or args.job_id)
    if not rollback_job_id:
        raise BackfillError('rollback_activation requires --rollback-job-id or --job-id')
    if args.confirm_rollback != 'YES':
        raise BackfillError('rollback_activation requires --confirm-rollback=YES')
    kv = make_kv(args)
    previous_key = f'{args.active_prefix_key}:previous:{rollback_job_id}'
    previous_prefix = kv.get_string(previous_key)
    if not previous_prefix:
        raise BackfillError(f'rollback_activation could not find previous prefix at {previous_key}')
    current_prefix = kv.get_string(args.active_prefix_key)
    rolled_back_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'rollback_job_id': rollback_job_id,
        'rolled_back_at_ms': rolled_back_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'previous_key': previous_key,
        'raw_backend': normalize_raw_backend(args.raw_backend),
    }
    if args.dry_run:
        return {
            'status': 'ok',
            'mode': 'rollback_activation',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'from_prefix': current_prefix,
            'to_prefix': previous_prefix,
            'rollback_job_id': rollback_job_id,
            'raw_backend': normalize_raw_backend(args.raw_backend),
        }
    kv.hset(f'{args.active_prefix_key}:rollback_audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, previous_prefix)
    return {
        'status': 'ok',
        'mode': 'rollback_activation',
        'active_prefix_key': args.active_prefix_key,
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'rollback_job_id': rollback_job_id,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:rollback_audit',
        'job_id': args.job_id,
    }


def run_incremental_repair(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('incremental_repair requires --target-prefix for the shadow repair prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('incremental_repair target-prefix must differ from source-prefix')
    if args.end_seq is None or args.end_seq <= args.start_seq:
        raise BackfillError('incremental_repair requires a bounded --start-seq/--end-seq window')
    if args.confirm_incremental_repair != 'YES':
        raise BackfillError('incremental_repair requires --confirm-incremental-repair=YES')

    validation: Json | None = None
    if not args.skip_validation:
        validation = run_validate_shadow(args)
        if validation.get('status') != 'ok':
            raise BackfillError(f'incremental repair shadow validation failed: {json.dumps(validation, sort_keys=True)}')

    kv = make_kv(args)
    active_prefix = args.repair_active_prefix or kv.get_string(args.active_prefix_key)
    if not active_prefix:
        raise BackfillError('incremental_repair requires --repair-active-prefix or an active prefix stored under --active-prefix-key')
    if active_prefix in {args.source_prefix, args.target_prefix}:
        raise BackfillError('incremental_repair active prefix must differ from source-prefix and shadow repair prefix')

    promote_args = clone_args(
        args,
        mode='shadow',
        target_prefix=active_prefix,
        job_id=f'{args.job_id}:active',
    )
    promotion = run_backfill(promote_args)

    if not args.dry_run:
        kv = make_kv(args)
        repaired_at_ms = int(time.time() * 1000)
        audit = {
            'job_id': args.job_id,
            'repaired_at_ms': repaired_at_ms,
            'source_prefix': args.source_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'shadow_prefix': args.target_prefix,
            'active_prefix': active_prefix,
            'start_seq': args.start_seq,
            'end_seq': args.end_seq,
            'partial': build_partial_spec(args),
            'validation': validation,
            'promotion_metrics': promotion.get('metrics', {}),
        }
        kv.hset(f'{args.active_prefix_key}:incremental_repair_audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))

    return {
        'status': 'ok',
        'mode': 'incremental_repair',
        'job_id': args.job_id,
        'source_prefix': args.source_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'shadow_prefix': args.target_prefix,
        'active_prefix': active_prefix,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation': validation,
        'promotion': promotion,
        'audit_key': f'{args.active_prefix_key}:incremental_repair_audit',
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Backfill MatrixArk context records from MatrixArk raw ingestion logs.')
    parser.add_argument('--metaserver', default=os.environ.get('MATRIXARK_METASERVER', '127.0.0.1:65000'))
    parser.add_argument('--namespace', default=os.environ.get('MATRIXARK_NAMESPACE', 'matrixark'))
    parser.add_argument('--table', default=os.environ.get('MATRIXARK_TABLE', 'context'))
    parser.add_argument('--library-path', default=os.environ.get('TEMPORALSTORE_LIBRARY_PATH', ''))
    parser.add_argument('--source-prefix', default='matrixark:mcp:raw_ingestion')
    parser.add_argument(
        '--raw-backend',
        choices=['temporalstore', 'matrixkv'],
        default=os.environ.get('MATRIXARK_RAW_INGESTION_BACKEND', 'temporalstore'),
        help='raw ingestion message store that owns source-prefix; affects checkpoints, idempotency, manifests, and metrics',
    )
    parser.add_argument('--target-prefix', default='')
    parser.add_argument('--mode', choices=['shadow', 'in_place', 'validate_shadow', 'activate_shadow', 'rollback_activation', 'incremental_repair'], default='shadow')
    parser.add_argument('--confirm-in-place', default='')
    parser.add_argument('--confirm-activate', default='')
    parser.add_argument('--confirm-rollback', default='')
    parser.add_argument('--confirm-incremental-repair', default='')
    parser.add_argument('--active-prefix-key', default='matrixark:context:active_prefix')
    parser.add_argument('--rollback-job-id', default='', help='activation job id whose previous active prefix should be restored')
    parser.add_argument('--repair-active-prefix', default='')
    parser.add_argument('--validation-strict', type=int, choices=[0, 1], default=1)
    parser.add_argument('--skip-validation', type=int, choices=[0, 1], default=0)
    parser.add_argument('--job-id', default=f'local-{int(time.time())}')
    parser.add_argument('--start-seq', type=int, default=0)
    parser.add_argument('--end-seq', type=int)
    parser.add_argument('--partial', type=int, choices=[0, 1], default=0, help='mark this as a partial/slice backfill')
    parser.add_argument('--partial-record-types', default='', help='comma-separated raw record_type allow-list for partial backfill')
    parser.add_argument('--partial-tenant-ids', default='', help='comma-separated tenant ids for partial backfill')
    parser.add_argument('--partial-user-ids', default='', help='comma-separated user ids for partial backfill')
    parser.add_argument('--partial-session-ids', default='', help='comma-separated session ids for partial backfill')
    parser.add_argument('--partial-filter-json', default='', help='exact-match JSON object filter for partial backfill')
    parser.add_argument('--partial-require-bounded', type=int, choices=[0, 1], default=1, help='require bounded range or filters for partial backfill')
    parser.add_argument('--batch-size', type=int, default=256)
    parser.add_argument('--source-scan-max-empty-shards', type=int, default=2)
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
    args.validation_strict = bool(args.validation_strict)
    args.skip_validation = bool(args.skip_validation)
    args.partial = bool(args.partial)
    args.partial_require_bounded = bool(args.partial_require_bounded)
    args.raw_backend = normalize_raw_backend(args.raw_backend)
    if args.batch_size <= 0:
        parser.error('--batch-size must be positive')
    if args.source_scan_max_empty_shards <= 0:
        parser.error('--source-scan-max-empty-shards must be positive')
    try:
        if args.mode == 'validate_shadow':
            summary = run_validate_shadow(args)
        elif args.mode == 'activate_shadow':
            summary = run_activate_shadow(args)
        elif args.mode == 'rollback_activation':
            summary = run_rollback_activation(args)
        elif args.mode == 'incremental_repair':
            summary = run_incremental_repair(args)
        else:
            summary = run_backfill(args)
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'error': str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
