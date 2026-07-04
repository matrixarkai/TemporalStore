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
import hashlib
import json
import os
import shlex
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
SourceRef = tuple[int, str | None] | tuple[int, str | None, str | None]

VOLATILE_SERVING_FINGERPRINT_FIELDS = {
    'context_event_key',
    'timestamp_key_ms',
    'updated_at_ms',
}


class BackfillError(RuntimeError):
    pass


def stable_serving_fingerprint_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): stable_serving_fingerprint_value(nested)
            for key, nested in value.items()
            if str(key) not in VOLATILE_SERVING_FINGERPRINT_FIELDS
        }
    if isinstance(value, list):
        return [stable_serving_fingerprint_value(item) for item in value]
    return value


def update_serving_fingerprint(hasher: Any, record: Json) -> None:
    payload = json.dumps(stable_serving_fingerprint_value(record), sort_keys=True, separators=(',', ':')).encode('utf-8')
    hasher.update(len(payload).to_bytes(8, 'big'))
    hasher.update(payload)


def canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


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
    _serving_fingerprint: Any = field(default_factory=hashlib.sha256, repr=False)

    def observe_records(self, records: list[Json]) -> None:
        for record in records:
            update_serving_fingerprint(self._serving_fingerprint, record)
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

    def serving_record_fingerprint(self) -> str:
        return self._serving_fingerprint.hexdigest()

    def data_quality_status(self) -> str:
        return 'clean' if self.failed == 0 and self.dead_letter == 0 else 'completed_with_errors'

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
        quality_status = self.data_quality_status()
        return {
            'status': 'ok',
            'data_quality_status': quality_status,
            'has_failures': quality_status != 'clean',
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
                'serving_record_fingerprint': self.serving_record_fingerprint(),
            },
        }

    def to_prometheus(self, *, job_id: str, raw_backend: str, source_range: Json | None = None) -> str:
        labels = f'job_id="{job_id}",raw_backend="{raw_backend}"'
        elapsed_ms = max(0, (self.finished_at_ms or int(time.time() * 1000)) - self.started_at_ms)
        scan_qps = (self.scanned * 1000.0 / elapsed_ms) if elapsed_ms else 0.0
        lines = [
            '# HELP matrixark_context_backfill_run_elapsed_ms Backfill run elapsed time in milliseconds.',
            '# TYPE matrixark_context_backfill_run_elapsed_ms gauge',
            f'matrixark_context_backfill_run_elapsed_ms{{{labels}}} {elapsed_ms}',
            '# HELP matrixark_context_backfill_scan_qps Backfill source scan throughput in records per second.',
            '# TYPE matrixark_context_backfill_scan_qps gauge',
            f'matrixark_context_backfill_scan_qps{{{labels}}} {round(scan_qps, 3)}',
            '# HELP matrixark_context_backfill_data_quality_status Backfill source processing quality status. Value is 1 for the observed status.',
            '# TYPE matrixark_context_backfill_data_quality_status gauge',
            f'matrixark_context_backfill_data_quality_status{{{labels},status="{self.data_quality_status()}"}} 1',
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
            '# HELP matrixark_context_backfill_serving_record_fingerprint_info Ordered fingerprint for materialized serving records in this run.',
            '# TYPE matrixark_context_backfill_serving_record_fingerprint_info gauge',
            f'matrixark_context_backfill_serving_record_fingerprint_info{{{labels},fingerprint="{self.serving_record_fingerprint()}"}} 1',
            '# HELP matrixark_context_backfill_batches_total Source and target batches processed.',
            '# TYPE matrixark_context_backfill_batches_total counter',
            f'matrixark_context_backfill_batches_total{{{labels},phase="source"}} {self.source_batches}',
            f'matrixark_context_backfill_batches_total{{{labels},phase="target"}} {self.target_batches}',
            f'matrixark_context_backfill_batches_total{{{labels},phase="scan_hash"}} {self.scan_hash_batches}',
        ])
        if isinstance(source_range, dict):
            lines.extend([
                '# HELP matrixark_context_backfill_source_range Source range boundary used by the backfill run.',
                '# TYPE matrixark_context_backfill_source_range gauge',
            ])
            for name in [
                'effective_start_seq',
                'effective_end_seq',
                'source_high_watermark_seq',
                'source_record_count',
                'discovered_record_count',
                'discovered_start_seq',
                'discovered_high_watermark_seq',
                'scan_hash_max_empty_shards',
            ]:
                value = source_range.get(name)
                if value is not None:
                    lines.append(f'matrixark_context_backfill_source_range{{{labels},boundary="{name}"}} {int(value)}')
            lines.extend([
                '# HELP matrixark_context_backfill_source_range_info Source range boolean metadata for recovery and audit.',
                '# TYPE matrixark_context_backfill_source_range_info gauge',
                f'matrixark_context_backfill_source_range_info{{{labels},property="source_record_count_estimated"}} {1 if source_range.get("source_record_count_estimated") else 0}',
                f'matrixark_context_backfill_source_range_info{{{labels},property="user_bounded_end"}} {1 if source_range.get("user_bounded_end") else 0}',
                '# HELP matrixark_context_backfill_source_scan_mode Source scan mode selected by the backfill runner.',
                '# TYPE matrixark_context_backfill_source_scan_mode gauge',
                f'matrixark_context_backfill_source_scan_mode{{{labels},scan_mode="{source_range.get("scan_mode") or "unknown"}"}} 1',
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

    def read_many(self, refs: list[SourceRef]) -> list[tuple[int, Json | None, Exception | None]]:
        batch_hget = getattr(self.kv, 'batch_hget', None)
        if not callable(batch_hget):
            return [self._read_one_ref(*self._normalize_ref(ref)) for ref in refs]
        entries: list[Json] = []
        normalized_refs = [self._normalize_ref(ref) for ref in refs]
        for sequence, legacy_record_id, scan_field in normalized_refs:
            if legacy_record_id is None:
                shard = sequence // self.shard_size
                field = scan_field or f'{sequence % self.shard_size:020d}'
                entries.append({'key': f'{self.prefix}:records:{shard:06d}', 'field': field, 'sequence': sequence})
            else:
                entries.append({'key': f'{self.prefix}:records', 'field': legacy_record_id, 'sequence': sequence})
        try:
            rows = list(batch_hget(entries))
        except Exception:
            return [self._read_one_ref(*ref) for ref in normalized_refs]
        rows_by_ref: dict[tuple[str, str], Json] = {}
        for row in rows:
            if isinstance(row, dict) and ('key' in row or 'field' in row):
                rows_by_ref[(str(row.get('key') or ''), str(row.get('field') or ''))] = row
        results: list[tuple[int, Json | None, Exception | None]] = []
        for index, (sequence, legacy_record_id, scan_field) in enumerate(normalized_refs):
            if legacy_record_id is None:
                shard = sequence // self.shard_size
                ref_key = (f'{self.prefix}:records:{shard:06d}', scan_field or f'{sequence % self.shard_size:020d}')
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

    @staticmethod
    def _normalize_ref(ref: SourceRef) -> tuple[int, str | None, str | None]:
        sequence = int(ref[0])
        legacy_record_id = ref[1] if len(ref) > 1 else None
        scan_field = ref[2] if len(ref) > 2 else None
        return sequence, legacy_record_id, scan_field

    def _read_one_ref(self, sequence: int, legacy_record_id: str | None, scan_field: str | None = None) -> tuple[int, Json | None, Exception | None]:
        try:
            if legacy_record_id is not None:
                record = self.read_legacy(legacy_record_id)
            elif scan_field:
                shard = sequence // self.shard_size
                payload = self.kv.hget(f'{self.prefix}:records:{shard:06d}', scan_field)
                if not payload:
                    raise BackfillError(f'missing sharded record at sequence {sequence}')
                record = json.loads(payload)
            else:
                record = self.read_at(sequence)
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
                'source_record_count_estimated': True,
                'discovered_record_count': 0,
                'discovered_high_watermark_seq': None,
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
    ) -> tuple[Iterable[SourceRef], str]:
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

    def _scan_sharded_refs(self, *, start_seq: int, end_seq: int | None, max_empty_scan_shards: int) -> Iterable[SourceRef]:
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
                    yield sequence, None, field
            shard += 1

    @staticmethod
    def _scan_hash_fields(payload: Json) -> list[str]:
        if not payload:
            return []
        if isinstance(payload.get('fields'), dict):
            return MatrixKVRecordLog._sort_scan_hash_fields(str(field) for field in payload['fields'].keys())
        if isinstance(payload.get('records'), dict):
            return MatrixKVRecordLog._sort_scan_hash_fields(str(field) for field in payload['records'].keys())
        if isinstance(payload.get('items'), list):
            fields = []
            for item in payload['items']:
                if isinstance(item, dict) and 'field' in item:
                    fields.append(str(item.get('field') or ''))
                elif isinstance(item, (list, tuple)) and item:
                    fields.append(str(item[0]))
            return MatrixKVRecordLog._sort_scan_hash_fields(field for field in fields if field)
        return MatrixKVRecordLog._sort_scan_hash_fields(str(field) for field in payload.keys())

    @staticmethod
    def _sort_scan_hash_fields(fields: Iterable[str]) -> list[str]:
        values = [str(field) for field in fields if str(field)]

        def sort_key(field: str) -> tuple[int, int, str]:
            try:
                return (0, int(field), field)
            except ValueError:
                return (1, 0, field)

        return sorted(values, key=sort_key)

    def iter_records(self, *, start_seq: int, end_seq: int | None) -> Iterable[tuple[int, Json]]:
        refs, _ = self.source_refs(start_seq=start_seq, end_seq=end_seq, max_empty_scan_shards=1)
        for ref in refs:
            sequence, legacy_record_id, scan_field = self._normalize_ref(ref)
            _, record, read_error = self._read_one_ref(sequence, legacy_record_id, scan_field)
            if read_error is not None:
                raise read_error
            yield sequence, record or {}


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

    def append_many(self, records: list[Json]) -> Json:
        stats: Json = {
            'attempted': len(records),
            'written': 0,
            'duplicate': 0,
            'appended_records': [],
        }
        if not records:
            return stats
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
                    stats['duplicate'] += 1
                    continue
                payload = json.dumps(record, sort_keys=True, separators=(',', ':'))
                entries.append({'key': f'{self.prefix}:records:{shard:06d}', 'field': f'{offset:020d}', 'value': payload})
                if dedupe_key:
                    idempotency_entries.append({'key': f'{self.prefix}:idempotency', 'field': dedupe_key, 'value': str(sequence)})
                sequence += 1
                stats['written'] += 1
                stats['appended_records'].append(record)
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
            return stats
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

    def read_many(self, start_sequence: int, end_sequence: int) -> list[tuple[int, Json | None, Exception | None]]:
        if end_sequence <= start_sequence:
            return []
        entries: list[Json] = []
        for sequence in range(start_sequence, end_sequence):
            shard = sequence // self.shard_size
            offset = sequence % self.shard_size
            entries.append({'key': f'{self.prefix}:records:{shard:06d}', 'field': f'{offset:020d}', 'sequence': sequence})
        batch_hget = getattr(self.kv, 'batch_hget', None)
        if not callable(batch_hget):
            return [self._read_target_ref(sequence) for sequence in range(start_sequence, end_sequence)]
        try:
            rows = list(batch_hget(entries))
        except Exception:
            return [self._read_target_ref(sequence) for sequence in range(start_sequence, end_sequence)]
        rows_by_ref: dict[tuple[str, str], Json] = {}
        for row in rows:
            if isinstance(row, dict) and ('key' in row or 'field' in row):
                rows_by_ref[(str(row.get('key') or ''), str(row.get('field') or ''))] = row
        results: list[tuple[int, Json | None, Exception | None]] = []
        for index, sequence in enumerate(range(start_sequence, end_sequence)):
            shard = sequence // self.shard_size
            offset = sequence % self.shard_size
            ref_key = (f'{self.prefix}:records:{shard:06d}', f'{offset:020d}')
            row = rows_by_ref.get(ref_key, rows[index] if index < len(rows) else {})
            payload = row if isinstance(row, str) else str((row or {}).get('value') or '')
            if not payload:
                results.append((sequence, None, BackfillError(f'missing target record at sequence {sequence}')))
                continue
            try:
                results.append((sequence, json.loads(payload), None))
            except Exception as exc:
                results.append((sequence, None, exc))
        return results

    def _read_target_ref(self, sequence: int) -> tuple[int, Json | None, Exception | None]:
        try:
            return sequence, self.read_at(sequence), None
        except Exception as exc:
            return sequence, None, exc

    def serving_type_counts(self) -> Json:
        counts, stats = self.serving_type_counts_with_stats()
        if int(stats.get('read_errors', 0) or 0) > 0:
            raise BackfillError(f'target serving type scan failed with {stats["read_errors"]} unreadable records')
        return counts

    def serving_type_counts_with_stats(self, *, batch_size: int = 1024) -> tuple[Json, Json]:
        counts: Json = {}
        total = self.count()
        fingerprint = hashlib.sha256()
        stats: Json = {
            'record_count': total,
            'batch_size': max(1, batch_size),
            'batches': 0,
            'read_errors': 0,
            'missing_records': 0,
        }
        step = max(1, batch_size)
        for start in range(0, total, step):
            stats['batches'] += 1
            for sequence, record, read_error in self.read_many(start, min(total, start + step)):
                if read_error is not None or record is None:
                    stats['read_errors'] += 1
                    if 'missing target record' in str(read_error):
                        stats['missing_records'] += 1
                    continue
                record_type = str(record.get('record_type') or 'unknown')
                counts[record_type] = int(counts.get(record_type, 0)) + 1
                update_serving_fingerprint(fingerprint, record)
        stats['serving_record_fingerprint'] = fingerprint.hexdigest()
        return dict(sorted(counts.items())), stats


    def append_dead_letter(self, item: Json) -> None:
        sequence = self.count_dead_letters()
        payload = json.dumps(item, sort_keys=True, separators=(',', ':'))
        self.kv.hset(f'{self.prefix}:dead_letter', f'{sequence:020d}', payload)
        self.kv.put_string(f'{self.prefix}:dead_letter_count', str(sequence + 1))

    def read_dead_letters(self, *, start: int = 0, limit: int = 100) -> list[Json]:
        total = self.count_dead_letters()
        start = max(0, int(start))
        limit = max(0, int(limit))
        if limit <= 0 or start >= total:
            return []
        end = min(total, start + limit)
        entries = [{'key': f'{self.prefix}:dead_letter', 'field': f'{sequence:020d}', 'sequence': sequence} for sequence in range(start, end)]
        batch_hget = getattr(self.kv, 'batch_hget', None)
        rows: list[Any]
        if callable(batch_hget):
            try:
                rows = list(batch_hget(entries))
            except Exception:
                rows = []
        else:
            rows = []
        exported: list[Json] = []
        for index, sequence in enumerate(range(start, end)):
            payload = ''
            if rows:
                row = rows[index] if index < len(rows) else {}
                payload = row if isinstance(row, str) else str((row or {}).get('value') or '')
            if not payload:
                payload = self.kv.hget(f'{self.prefix}:dead_letter', f'{sequence:020d}')
            item: Json
            try:
                decoded = json.loads(payload) if payload else {}
                item = decoded if isinstance(decoded, dict) else {'raw': decoded}
            except Exception as exc:
                item = {'read_error': str(exc), 'raw_preview': str(payload)[:2048]}
            item.setdefault('dead_letter_sequence', sequence)
            exported.append(item)
        return exported


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


def checkpoint_resume_compatible(checkpoint_range: Json | None, *, requested_start_seq: int, requested_end_seq: int | None) -> bool:
    if not isinstance(checkpoint_range, dict) or not checkpoint_range:
        return True
    checkpoint_start = checkpoint_range.get('requested_start_seq')
    checkpoint_end = checkpoint_range.get('requested_end_seq')
    if checkpoint_start is not None:
        try:
            if int(checkpoint_start) != int(requested_start_seq):
                return False
        except (TypeError, ValueError):
            return False
    if checkpoint_end == requested_end_seq:
        return True
    if checkpoint_end is not None and requested_end_seq is None:
        return True
    return False


def apply_resume_checkpoint(
    *,
    args: argparse.Namespace,
    start_seq: int,
    resume_state: Json,
) -> tuple[int, Json]:
    resume_state.update({
        'resume_requested': bool(args.resume),
        'requested_start_seq': args.start_seq,
        'requested_end_seq': args.end_seq,
        'effective_start_seq': start_seq,
        'checkpoint_ignored': False,
        'checkpoint_ignore_reason': '',
    })
    if not args.resume:
        return start_seq, resume_state
    checkpoint_sequence = resume_state.get('checkpoint_last_sequence')
    if checkpoint_sequence is None:
        return start_seq, resume_state
    checkpoint_range = resume_state.get('checkpoint_source_range')
    compatible = checkpoint_resume_compatible(
        checkpoint_range,
        requested_start_seq=args.start_seq,
        requested_end_seq=args.end_seq,
    )
    if not compatible:
        if getattr(args, 'confirm_resume_range_change', '') != 'YES':
            raise BackfillError('resume checkpoint source range differs from requested --start-seq/--end-seq; use --confirm-resume-range-change=YES to ignore the checkpoint')
        resume_state['checkpoint_ignored'] = True
        resume_state['checkpoint_ignore_reason'] = 'source_range_mismatch_confirmed'
        return start_seq, resume_state
    start_seq = max(start_seq, int(checkpoint_sequence) + 1)
    resume_state['effective_start_seq'] = start_seq
    return start_seq, resume_state


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


def estimate_source_window_records(source_range: Json) -> int | None:
    if (
        source_range.get('source_record_count_estimated')
        and source_range.get('discovered_record_count') is not None
        and source_range.get('scan_hash_max_empty_shards') is not None
    ):
        try:
            return max(0, int(source_range.get('discovered_record_count') or 0))
        except (TypeError, ValueError):
            return None
    effective_start = source_range.get('effective_start_seq')
    effective_end = source_range.get('effective_end_seq')
    if effective_start is None or effective_end is None:
        return None
    try:
        return max(0, int(effective_end) - int(effective_start))
    except (TypeError, ValueError):
        return None


def discover_scan_hash_source_range(
    source: MatrixKVRecordLog,
    source_range: Json,
    *,
    end_seq: int | None,
    max_empty_scan_shards: int,
) -> Json:
    if str(source_range.get('scan_mode') or '') != 'scan_hash':
        return source_range
    discovered = dict(source_range)
    discovered_min_sequence: int | None = None
    discovered_max_sequence: int | None = None
    discovered_count = 0
    refs, _ = source.source_refs(
        start_seq=int(discovered.get('effective_start_seq') or 0),
        end_seq=end_seq,
        max_empty_scan_shards=max_empty_scan_shards,
        source_range=discovered,
    )
    for ref in refs:
        sequence, _, _ = source._normalize_ref(ref)
        discovered_count += 1
        discovered_min_sequence = sequence if discovered_min_sequence is None else min(discovered_min_sequence, sequence)
        discovered_max_sequence = sequence if discovered_max_sequence is None else max(discovered_max_sequence, sequence)
    discovered.update({
        'source_record_count': discovered_count,
        'source_record_count_estimated': True,
        'source_high_watermark_seq': discovered_max_sequence,
        'discovered_record_count': discovered_count,
        'discovered_start_seq': discovered_min_sequence,
        'discovered_high_watermark_seq': discovered_max_sequence,
        'scan_hash_max_empty_shards': max_empty_scan_shards,
    })
    if discovered_min_sequence is not None:
        discovered['effective_start_seq'] = discovered_min_sequence
    if discovered.get('effective_end_seq') is None and discovered_max_sequence is not None:
        discovered['effective_end_seq'] = discovered_max_sequence + 1
    if discovered_count == 0 and discovered.get('effective_end_seq') is None:
        discovered['effective_end_seq'] = int(discovered.get('effective_start_seq') or 0)
    return discovered


def _plan_arg(name: str, value: Any) -> str:
    return f'--{name}={value}'


def _append_plan_arg(args_out: list[str], name: str, value: Any) -> None:
    if value not in (None, ''):
        args_out.append(_plan_arg(name, value))


def build_plan_command_base_args(args: argparse.Namespace, *, start_seq: int, end_seq: int) -> list[str]:
    out: list[str] = [
        _plan_arg('metaserver', args.metaserver),
        _plan_arg('namespace', args.namespace),
        _plan_arg('table', args.table),
        _plan_arg('source-prefix', args.source_prefix),
        _plan_arg('raw-backend', normalize_raw_backend(args.raw_backend)),
        _plan_arg('start-seq', start_seq),
        _plan_arg('end-seq', end_seq),
        _plan_arg('batch-size', args.batch_size),
        _plan_arg('source-scan-max-empty-shards', args.source_scan_max_empty_shards),
    ]
    _append_plan_arg(out, 'library-path', getattr(args, 'library_path', ''))
    _append_plan_arg(out, 'local-kv', getattr(args, 'local_kv', ''))
    if getattr(args, 'partial', False):
        out.append('--partial=1')
    for option_name, attr_name in [
        ('partial-record-types', 'partial_record_types'),
        ('partial-tenant-ids', 'partial_tenant_ids'),
        ('partial-user-ids', 'partial_user_ids'),
        ('partial-session-ids', 'partial_session_ids'),
        ('partial-filter-json', 'partial_filter_json'),
    ]:
        _append_plan_arg(out, option_name, getattr(args, attr_name, ''))
    if not bool(getattr(args, 'partial_require_bounded', True)):
        out.append('--partial-require-bounded=0')
    if not bool(getattr(args, 'resume', True)):
        out.append('--resume=0')
    _append_plan_arg(out, 'confirm-resume-range-change', getattr(args, 'confirm_resume_range_change', ''))
    if bool(getattr(args, 'fail_fast', False)):
        out.append('--fail-fast')
    return out


def build_plan_active_args(args: argparse.Namespace) -> list[str]:
    out = [_plan_arg('active-prefix-key', args.active_prefix_key)]
    _append_plan_arg(out, 'expect-active-prefix', getattr(args, 'expect_active_prefix', ''))
    _append_plan_arg(out, 'repair-active-prefix', getattr(args, 'repair_active_prefix', ''))
    _append_plan_arg(out, 'confirm-no-active-prefix-precondition', getattr(args, 'confirm_no_active_prefix_precondition', ''))
    return out


def build_plan_validation_args(args: argparse.Namespace) -> list[str]:
    out = [
        _plan_arg('validation-strict', 1 if bool(getattr(args, 'validation_strict', True)) else 0),
        _plan_arg('skip-validation', 1 if bool(getattr(args, 'skip_validation', False)) else 0),
    ]
    _append_plan_arg(out, 'confirm-skip-validation', getattr(args, 'confirm_skip_validation', ''))
    _append_plan_arg(out, 'confirm-non-strict-validation', getattr(args, 'confirm_non_strict_validation', ''))
    _append_plan_arg(out, 'confirm-unvalidated-target-state', getattr(args, 'confirm_unvalidated_target_state', ''))
    return out


def build_plan_execution(args: argparse.Namespace, windows: list[Json]) -> Json:
    parallelism = max(1, int(getattr(args, 'plan_parallelism', 1) or 1))
    waves: list[Json] = []
    for offset in range(0, len(windows), parallelism):
        wave_windows = windows[offset:offset + parallelism]
        waves.append({
            'wave': len(waves),
            'window_indexes': [int(window['index']) for window in wave_windows],
            'shadow_command_args': [window['shadow_command_args'] for window in wave_windows],
            'validate_command_args': [window['validate_command_args'] for window in wave_windows],
        })
    return {
        'plan_parallelism': parallelism,
        'shadow_validation_waves': waves,
        'promotion_sequence': [
            {
                'order': index,
                'window_index': int(window['index']),
                'incremental_repair_command_args': window['incremental_repair_command_args'],
            }
            for index, window in enumerate(windows)
        ],
        'execution_order': [
            'run each shadow_validation_waves[].shadow_command_args group concurrently up to plan_parallelism',
            'run each shadow_validation_waves[].validate_command_args group after its shadow wave finishes',
            'run promotion_sequence[].incremental_repair_command_args one at a time in order',
        ],
    }


def _script_command(args_list: list[str]) -> str:
    command = ['python3', 'tools/matrixark_context_backfill.py', *args_list]
    return ' '.join(shlex.quote(part) for part in command)


def _render_plan_script(commands: list[list[str]], *, parallel: bool) -> str:
    lines = [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        f'cd {shlex.quote(str(ROOT))}',
        '',
    ]
    if parallel:
        lines.append('pids=()')
        for args_list in commands:
            lines.append(f'{_script_command(args_list)} &')
            lines.append('pids+=("$!")')
        lines.extend([
            'for pid in "${pids[@]}"; do',
            '  wait "$pid"',
            'done',
        ])
    else:
        for args_list in commands:
            lines.append(_script_command(args_list))
    return '\n'.join(lines) + '\n'


def _write_plan_script(path: Path, commands: list[list[str]], *, parallel: bool) -> None:
    path.write_text(_render_plan_script(commands, parallel=parallel), encoding='utf-8')
    path.chmod(0o755)


def _artifact_file_info(path: Path, *, output_dir: Path | None = None) -> Json:
    payload = path.read_bytes()
    info: Json = {
        'path': str(path),
        'size_bytes': len(payload),
        'sha256': hashlib.sha256(payload).hexdigest(),
        'executable': bool(path.stat().st_mode & 0o111),
    }
    if output_dir is not None:
        try:
            info['relative_path'] = str(path.relative_to(output_dir))
        except ValueError:
            info['relative_path'] = path.name
    return info


def _resolve_artifact_manifest_path(item: Json, output_dir: Path) -> tuple[Path, str]:
    relative_path = str(item.get('relative_path') or '')
    if relative_path:
        path = Path(relative_path)
        if path.is_absolute() or '..' in path.parts:
            return output_dir / path, 'unsafe_relative_path'
        return output_dir / path, 'relative_path'
    path = Path(str(item.get('path') or ''))
    if not path.is_absolute():
        return output_dir / path, 'path_relative_to_output_dir'
    return path, 'absolute_path'


def _expected_plan_script_payloads(plan: Json) -> dict[str, str]:
    chunk_plan = plan.get('chunk_plan') if isinstance(plan.get('chunk_plan'), dict) else {}
    execution_plan = chunk_plan.get('execution_plan') if isinstance(chunk_plan.get('execution_plan'), dict) else {}
    expected: dict[str, str] = {}
    for wave in execution_plan.get('shadow_validation_waves') or []:
        if not isinstance(wave, dict):
            continue
        wave_id = int(wave.get('wave', len(expected)) or 0)
        shadow_commands = [
            command for command in (wave.get('shadow_command_args') or [])
            if isinstance(command, list)
        ]
        validate_commands = [
            command for command in (wave.get('validate_command_args') or [])
            if isinstance(command, list)
        ]
        expected[f'shadow_wave_{wave_id:04d}.sh'] = _render_plan_script(shadow_commands, parallel=True)
        expected[f'validate_wave_{wave_id:04d}.sh'] = _render_plan_script(validate_commands, parallel=True)
    promotion_commands = [
        item.get('incremental_repair_command_args')
        for item in (execution_plan.get('promotion_sequence') or [])
        if isinstance(item, dict) and isinstance(item.get('incremental_repair_command_args'), list)
    ]
    if promotion_commands:
        expected['promote_serial.sh'] = _render_plan_script(promotion_commands, parallel=False)
    return expected


def _verify_plan_script_payloads(output_dir: Path) -> tuple[bool, list[Json], list[str]]:
    plan_path = output_dir / 'plan.json'
    if not plan_path.exists():
        return False, [], ['plan.json not found for script semantic verification']
    try:
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        return False, [], [f'invalid plan.json for script semantic verification: {exc}']
    if not isinstance(plan, dict):
        return False, [], ['plan.json is not an object']
    expected = _expected_plan_script_payloads(plan)
    checks: list[Json] = []
    errors: list[str] = []
    for relative_path, expected_payload in sorted(expected.items()):
        path = output_dir / relative_path
        exists = path.exists()
        actual_payload = path.read_text(encoding='utf-8') if exists else ''
        matches = exists and actual_payload == expected_payload
        checks.append({
            'relative_path': relative_path,
            'exists': exists,
            'matches_plan': matches,
            'expected_sha256': hashlib.sha256(expected_payload.encode('utf-8')).hexdigest(),
            'actual_sha256': hashlib.sha256(actual_payload.encode('utf-8')).hexdigest() if exists else '',
        })
        if not matches:
            errors.append(f'generated plan script does not match plan.json execution arguments: {relative_path}')
    return bool(expected) and all(bool(item.get('matches_plan')) for item in checks), checks, errors


def _require_plan_output_dir_writable(args: argparse.Namespace, output_dir: Path) -> None:
    if not output_dir.exists():
        return
    existing = [item for item in output_dir.iterdir() if item.name not in {'.', '..'}]
    if existing and getattr(args, 'confirm_plan_output_overwrite', '') != 'YES':
        raise BackfillError(
            f'plan output directory {output_dir} is not empty; '
            'use --confirm-plan-output-overwrite=YES to replace generated plan artifacts'
        )


def write_plan_artifacts(args: argparse.Namespace, summary: Json) -> Json:
    output_dir_arg = str(getattr(args, 'plan_output_dir', '') or '')
    if not output_dir_arg:
        return {}
    output_dir = Path(output_dir_arg)
    _require_plan_output_dir_writable(args, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_plan = summary.get('chunk_plan') if isinstance(summary.get('chunk_plan'), dict) else {}
    execution_plan = chunk_plan.get('execution_plan') if isinstance(chunk_plan.get('execution_plan'), dict) else {}
    shadow_scripts: list[str] = []
    validate_scripts: list[str] = []
    for wave in execution_plan.get('shadow_validation_waves') or []:
        if not isinstance(wave, dict):
            continue
        wave_id = int(wave.get('wave', len(shadow_scripts)) or 0)
        shadow_path = output_dir / f'shadow_wave_{wave_id:04d}.sh'
        validate_path = output_dir / f'validate_wave_{wave_id:04d}.sh'
        _write_plan_script(shadow_path, list(wave.get('shadow_command_args') or []), parallel=True)
        _write_plan_script(validate_path, list(wave.get('validate_command_args') or []), parallel=True)
        shadow_scripts.append(str(shadow_path))
        validate_scripts.append(str(validate_path))
    promotion_commands = [
        item.get('incremental_repair_command_args')
        for item in (execution_plan.get('promotion_sequence') or [])
        if isinstance(item, dict) and isinstance(item.get('incremental_repair_command_args'), list)
    ]
    promote_script = ''
    if promotion_commands:
        promote_path = output_dir / 'promote_serial.sh'
        _write_plan_script(promote_path, promotion_commands, parallel=False)
        promote_script = str(promote_path)
    plan_path = output_dir / 'plan.json'
    manifest_path = output_dir / 'artifact_manifest.json'
    artifact_summary = {
        'output_dir': str(output_dir),
        'plan_json': str(plan_path),
        'artifact_manifest': str(manifest_path),
        'shadow_wave_scripts': shadow_scripts,
        'validate_wave_scripts': validate_scripts,
        'promote_serial_script': promote_script,
        'script_cwd': str(ROOT),
        'overwrite_confirmed': getattr(args, 'confirm_plan_output_overwrite', '') == 'YES',
    }
    summary_with_artifacts = dict(summary)
    summary_with_artifacts['plan_artifacts'] = artifact_summary
    plan_path.write_text(json.dumps(summary_with_artifacts, sort_keys=True, indent=2), encoding='utf-8')
    artifact_paths = [
        plan_path,
        *[Path(path) for path in shadow_scripts],
        *[Path(path) for path in validate_scripts],
        *([Path(promote_script)] if promote_script else []),
    ]
    manifest = {
        'manifest_schema': 'matrixark_context_backfill_plan_artifacts_v1',
        'job_id': args.job_id,
        'generated_at_ms': int(time.time() * 1000),
        'output_dir': str(output_dir),
        'files': [_artifact_file_info(path, output_dir=output_dir) for path in artifact_paths],
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding='utf-8')
    artifact_summary['artifact_manifest_sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    return artifact_summary


def run_verify_plan_artifacts(args: argparse.Namespace) -> Json:
    output_dir_arg = str(getattr(args, 'plan_output_dir', '') or '')
    if not output_dir_arg:
        raise BackfillError('verify_plan_artifacts requires --plan-output-dir')
    output_dir = Path(output_dir_arg)
    manifest_path = output_dir / 'artifact_manifest.json'
    checks: Json = {
        'output_dir_exists': output_dir.exists() and output_dir.is_dir(),
        'manifest_found': manifest_path.exists(),
        'manifest_json_valid': False,
        'manifest_schema_supported': False,
        'manifest_job_id_matches': False,
        'all_files_exist': False,
        'all_paths_safe': False,
        'all_file_sizes_match': False,
        'all_file_sha256_match': False,
        'all_executable_bits_match': False,
        'generated_scripts_match_plan': False,
    }
    manifest: Json = {}
    errors: list[str] = []
    if not checks['output_dir_exists']:
        errors.append(f'plan output directory not found: {output_dir}')
    if not checks['manifest_found']:
        errors.append(f'artifact manifest not found: {manifest_path}')
    if checks['manifest_found']:
        try:
            decoded = json.loads(manifest_path.read_text(encoding='utf-8'))
            if isinstance(decoded, dict):
                manifest = decoded
                checks['manifest_json_valid'] = True
            else:
                errors.append('artifact manifest JSON is not an object')
        except json.JSONDecodeError as exc:
            errors.append(f'invalid artifact manifest JSON: {exc}')
    checks['manifest_schema_supported'] = manifest.get('manifest_schema') == 'matrixark_context_backfill_plan_artifacts_v1'
    if manifest and not checks['manifest_schema_supported']:
        errors.append('unsupported artifact manifest schema')
    expected_job_id = str(getattr(args, 'job_id', '') or '')
    manifest_job_id = str(manifest.get('job_id') or '')
    checks['manifest_job_id_matches'] = not expected_job_id or manifest_job_id == expected_job_id
    if expected_job_id and manifest_job_id != expected_job_id:
        errors.append(f'artifact manifest job_id mismatch: expected {expected_job_id}, found {manifest_job_id or "<empty>"}')
    file_checks: list[Json] = []
    files = manifest.get('files') if isinstance(manifest.get('files'), list) else []
    for item in files:
        if not isinstance(item, dict):
            file_checks.append({'status': 'failed', 'error': 'file entry is not an object'})
            continue
        path, path_source = _resolve_artifact_manifest_path(item, output_dir)
        path_safe = path_source != 'unsafe_relative_path'
        exists = path.exists() if path_safe else False
        size_matches = False
        sha_matches = False
        executable_matches = False
        actual_sha = ''
        actual_size = None
        actual_executable = False
        if exists:
            info = _artifact_file_info(path)
            actual_sha = str(info['sha256'])
            actual_size = int(info['size_bytes'])
            actual_executable = bool(info['executable'])
            size_matches = actual_size == int(item.get('size_bytes', -1) or -1)
            sha_matches = actual_sha == str(item.get('sha256') or '')
            executable_matches = actual_executable == bool(item.get('executable'))
        file_checks.append({
            'path': str(path),
            'manifest_path': str(item.get('path') or ''),
            'manifest_relative_path': str(item.get('relative_path') or ''),
            'path_source': path_source,
            'path_safe': path_safe,
            'exists': exists,
            'size_matches': size_matches,
            'sha256_matches': sha_matches,
            'executable_matches': executable_matches,
            'expected_size_bytes': item.get('size_bytes'),
            'actual_size_bytes': actual_size,
            'expected_sha256': item.get('sha256'),
            'actual_sha256': actual_sha,
            'expected_executable': bool(item.get('executable')),
            'actual_executable': actual_executable,
        })
    checks['all_files_exist'] = bool(files) and all(bool(item.get('exists')) for item in file_checks)
    checks['all_paths_safe'] = bool(files) and all(bool(item.get('path_safe')) for item in file_checks)
    checks['all_file_sizes_match'] = bool(files) and all(bool(item.get('size_matches')) for item in file_checks)
    checks['all_file_sha256_match'] = bool(files) and all(bool(item.get('sha256_matches')) for item in file_checks)
    checks['all_executable_bits_match'] = bool(files) and all(bool(item.get('executable_matches')) for item in file_checks)
    for item in file_checks:
        if not item.get('path_safe'):
            errors.append(f'unsafe artifact relative path: {item.get("manifest_relative_path")}')
        elif not item.get('exists'):
            errors.append(f'missing artifact file: {item.get("path")}')
        elif not item.get('size_matches') or not item.get('sha256_matches') or not item.get('executable_matches'):
            errors.append(f'artifact file mismatch: {item.get("path")}')
    script_semantics_match, script_checks, script_errors = _verify_plan_script_payloads(output_dir)
    checks['generated_scripts_match_plan'] = script_semantics_match
    errors.extend(script_errors)
    status = 'ok' if all(bool(value) for value in checks.values()) else 'failed'
    return {
        'status': status,
        'mode': 'verify_plan_artifacts',
        'job_id': expected_job_id or manifest_job_id,
        'plan_output_dir': str(output_dir),
        'artifact_manifest': str(manifest_path),
        'artifact_manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest() if manifest_path.exists() else '',
        'manifest_schema': manifest.get('manifest_schema', ''),
        'manifest_file_count': len(files),
        'checks': checks,
        'file_checks': file_checks,
        'script_semantic_checks': script_checks,
        'errors': errors,
    }


def run_export_dead_letters(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('export_dead_letters requires --target-prefix')
    raw_backend = normalize_raw_backend(args.raw_backend)
    kv = make_kv(args)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    total = target.count_dead_letters()
    start = max(0, int(getattr(args, 'dead_letter_start', 0) or 0))
    limit = max(0, int(getattr(args, 'dead_letter_limit', 100) or 0))
    rows = target.read_dead_letters(start=start, limit=limit)
    fingerprint = hashlib.sha256()
    for row in rows:
        fingerprint.update(json.dumps(row, sort_keys=True, separators=(',', ':')).encode('utf-8'))
        fingerprint.update(b'\n')
    output_path = str(getattr(args, 'dead_letter_output', '') or '')
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows), encoding='utf-8')
    summary = {
        'status': 'ok',
        'mode': 'export_dead_letters',
        'job_id': args.job_id,
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'dead_letter_total': total,
        'dead_letter_start': start,
        'dead_letter_limit': limit,
        'exported_count': len(rows),
        'has_more': start + len(rows) < total,
        'next_start': start + len(rows) if start + len(rows) < total else None,
        'dead_letter_fingerprint': fingerprint.hexdigest(),
        'dead_letter_output': output_path,
        'dead_letters': rows,
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(dead_letter_export_to_prometheus(summary), encoding='utf-8')
    return summary


def dead_letter_export_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'target_prefix': str(summary.get('target_prefix') or ''),
        'mode': 'export_dead_letters',
    }
    fingerprint = str(summary.get('dead_letter_fingerprint') or '')
    lines = [
        '# HELP matrixark_context_backfill_dead_letter_export_status Dead-letter export status.',
        '# TYPE matrixark_context_backfill_dead_letter_export_status gauge',
        f'matrixark_context_backfill_dead_letter_export_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_dead_letter_export_records Dead-letter export record counts.',
        '# TYPE matrixark_context_backfill_dead_letter_export_records gauge',
        f'matrixark_context_backfill_dead_letter_export_records{{{_prom_labels(**base, kind="total")}}} {int(summary.get("dead_letter_total", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_records{{{_prom_labels(**base, kind="exported")}}} {int(summary.get("exported_count", 0) or 0)}',
        '# HELP matrixark_context_backfill_dead_letter_export_page Dead-letter export pagination state.',
        '# TYPE matrixark_context_backfill_dead_letter_export_page gauge',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="start")}}} {int(summary.get("dead_letter_start", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="limit")}}} {int(summary.get("dead_letter_limit", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="has_more")}}} {1 if summary.get("has_more") else 0}',
        '# HELP matrixark_context_backfill_dead_letter_export_fingerprint_info Stable fingerprint of exported dead-letter rows.',
        '# TYPE matrixark_context_backfill_dead_letter_export_fingerprint_info gauge',
        f'matrixark_context_backfill_dead_letter_export_fingerprint_info{{{_prom_labels(**base, fingerprint=fingerprint)}}} 1',
    ]
    next_start = summary.get('next_start')
    if next_start is not None:
        lines.append(f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="next_start")}}} {int(next_start)}')
    return '\n'.join(lines) + '\n'


def build_plan_windows(args: argparse.Namespace, *, source_range: Json, target_prefix: str) -> Json:
    window_size = int(getattr(args, 'plan_window_size', 0) or 0)
    max_windows = int(getattr(args, 'plan_max_windows', 0) or 0)
    if window_size <= 0:
        return {
            'enabled': False,
            'window_size': 0,
            'windows': [],
            'total_windows': 0,
            'truncated': False,
            'parallel_write_safety': 'not_applicable',
        }
    effective_start = source_range.get('effective_start_seq')
    effective_end = source_range.get('effective_end_seq')
    if effective_start is None or effective_end is None:
        return {
            'enabled': False,
            'window_size': window_size,
            'windows': [],
            'total_windows': 0,
            'truncated': False,
            'parallel_write_safety': 'requires_bounded_effective_end_seq',
            'reason': 'source range has no effective_end_seq; pass --end-seq, use record_count/index metadata, or enable scan-hash discovery',
        }
    start = int(effective_start)
    end = int(effective_end)
    if end <= start:
        return {
            'enabled': True,
            'window_size': window_size,
            'windows': [],
            'total_windows': 0,
            'truncated': False,
            'parallel_write_safety': 'empty_window',
        }
    windows: list[Json] = []
    sequence = start
    index = 0
    while sequence < end:
        if max_windows > 0 and index >= max_windows:
            break
        window_end = min(end, sequence + window_size)
        chunk_job_id = f'{args.job_id}:w{index:04d}'
        chunk_shadow_prefix = f'{target_prefix}:chunk:{index:04d}'
        base_args = build_plan_command_base_args(args, start_seq=sequence, end_seq=window_end)
        active_args = build_plan_active_args(args)
        validation_args = build_plan_validation_args(args)
        windows.append({
            'index': index,
            'start_seq': sequence,
            'end_seq': window_end,
            'record_count': window_end - sequence,
            'job_id': chunk_job_id,
            'shared_target_prefix': target_prefix,
            'parallel_shadow_prefix': chunk_shadow_prefix,
            'shadow_command_args': [
                '--mode=shadow',
                f'--job-id={chunk_job_id}',
                f'--target-prefix={chunk_shadow_prefix}',
                '--dry-run=0',
                *active_args,
                *base_args,
            ],
            'validate_command_args': [
                '--mode=validate_shadow',
                f'--job-id={chunk_job_id}',
                f'--target-prefix={chunk_shadow_prefix}',
                *validation_args,
                *base_args,
            ],
            'incremental_repair_command_args': [
                '--mode=incremental_repair',
                f'--job-id={chunk_job_id}',
                f'--target-prefix={chunk_shadow_prefix}',
                '--confirm-incremental-repair=YES',
                *active_args,
                *validation_args,
                *base_args,
            ],
        })
        sequence = window_end
        index += 1
    total_windows = (end - start + window_size - 1) // window_size
    execution_plan = build_plan_execution(args, windows)
    return {
        'enabled': True,
        'window_size': window_size,
        'windows': windows,
        'execution_plan': execution_plan,
        'total_windows': total_windows,
        'emitted_windows': len(windows),
        'truncated': len(windows) < total_windows,
        'parallel_write_safety': 'do_not_parallel_write_same_target_prefix; use per-window shadow prefixes and serialize active-prefix promotion',
        'shared_target_strategy': 'sequential_only',
        'parallel_shadow_strategy': 'independent_chunk_prefixes_can_be_built_and_validated_concurrently; incremental_repair_promotion_should_be_serialized',
    }


def require_active_target_confirmation(args: argparse.Namespace, kv: Any, target_prefix: str) -> None:
    if args.dry_run or args.mode != 'shadow':
        return
    active_prefix = kv.get_string(args.active_prefix_key) if getattr(args, 'active_prefix_key', '') else ''
    if active_prefix and active_prefix == target_prefix and getattr(args, 'confirm_active_target', '') != 'YES':
        raise BackfillError('shadow backfill target-prefix is the current active prefix; use incremental_repair for bounded active repairs or pass --confirm-active-target=YES')


def require_expected_active_prefix(args: argparse.Namespace, current_prefix: str) -> None:
    expected = str(getattr(args, 'expect_active_prefix', '') or '')
    if expected and current_prefix != expected:
        raise BackfillError(f'active prefix precondition failed: expected {expected}, found {current_prefix or "<empty>"}')


def active_prefix_precondition_bypassed(args: argparse.Namespace) -> bool:
    return bool(
        not str(getattr(args, 'expect_active_prefix', '') or '')
        and getattr(args, 'confirm_no_active_prefix_precondition', '') == 'YES'
    )


def require_active_prefix_precondition(args: argparse.Namespace, *, mode: str) -> None:
    if bool(getattr(args, 'dry_run', False)):
        return
    if str(getattr(args, 'expect_active_prefix', '') or ''):
        return
    if getattr(args, 'confirm_no_active_prefix_precondition', '') == 'YES':
        return
    raise BackfillError(f'{mode} requires --expect-active-prefix or --confirm-no-active-prefix-precondition=YES')


def rollback_noop_bypassed(args: argparse.Namespace) -> bool:
    return getattr(args, 'confirm_rollback_noop', '') == 'YES'


def require_non_noop_rollback(args: argparse.Namespace, current_prefix: str, previous_prefix: str) -> None:
    if current_prefix != previous_prefix:
        return
    if rollback_noop_bypassed(args):
        return
    raise BackfillError('rollback_activation previous prefix equals current active prefix; use --confirm-rollback-noop=YES to audit an intentional no-op rollback')


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
    require_active_target_confirmation(args, kv, target_prefix)
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
    start_seq, resume_state = apply_resume_checkpoint(
        args=args,
        start_seq=start_seq,
        resume_state=resume_state,
    )

    seen_ids: set[str] = set()
    pending: list[Json] = []
    checkpoint_pending_seq: int | None = None
    discovered_min_sequence: int | None = None
    discovered_max_sequence: int | None = None
    outer_bulk = hasattr(kv, 'begin_bulk') and hasattr(kv, 'end_bulk')

    def flush() -> None:
        nonlocal pending, checkpoint, checkpoint_pending_seq
        if not pending and checkpoint_pending_seq is None:
            return
        if not args.dry_run:
            if pending:
                append_stats = target.append_many(pending)
            else:
                append_stats = {'written': 0, 'duplicate': 0, 'appended_records': []}
        else:
            append_stats = {
                'written': len(pending),
                'duplicate': 0,
                'appended_records': list(pending),
            }
        if pending:
            metrics.target_batches += 1
            append_written = int(append_stats.get('written', 0) or 0)
            append_duplicate = int(append_stats.get('duplicate', 0) or 0)
            appended_records = append_stats.get('appended_records')
            if not isinstance(appended_records, list):
                appended_records = pending[:append_written]
            metrics.written += append_written
            metrics.duplicate += append_duplicate
            metrics.observe_records(appended_records)
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
            check_target_duplicates = (not args.dry_run) or bool(getattr(args, 'dry_run_check_target', True))
            if check_target_duplicates:
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

    def process_source_batch(batch: list[SourceRef]) -> None:
        nonlocal discovered_min_sequence, discovered_max_sequence
        if not batch:
            return
        metrics.source_batches += 1
        if scan_mode == 'scan_hash':
            metrics.scan_hash_batches += 1
        for ref in batch:
            sequence, _, _ = source._normalize_ref(ref)
            discovered_min_sequence = sequence if discovered_min_sequence is None else min(discovered_min_sequence, sequence)
            discovered_max_sequence = sequence if discovered_max_sequence is None else max(discovered_max_sequence, sequence)
        rows = source.read_many(batch)
        existing_dedupe_ids: set[str] | None = None
        check_target_duplicates = (not args.dry_run) or bool(getattr(args, 'dry_run_check_target', True))
        if check_target_duplicates:
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
        source_batch: list[SourceRef] = []
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
    if scan_mode == 'scan_hash':
        source_range.update({
            'source_record_count': metrics.scanned,
            'source_record_count_estimated': True,
            'source_high_watermark_seq': discovered_max_sequence,
            'discovered_record_count': metrics.scanned,
            'discovered_start_seq': discovered_min_sequence,
            'discovered_high_watermark_seq': discovered_max_sequence,
            'scan_hash_max_empty_shards': args.source_scan_max_empty_shards,
        })
        if source_range.get('effective_end_seq') is None and discovered_max_sequence is not None:
            source_range['effective_end_seq'] = discovered_max_sequence + 1
        if not args.dry_run and checkpoint is not None and checkpoint.get('last_sequence') is not None:
            checkpoint = build_checkpoint_metadata(
                job_id=args.job_id,
                source_prefix=args.source_prefix,
                target_prefix=target_prefix,
                raw_backend=raw_backend,
                mode=args.mode,
                partial=partial,
                source_range=source_range,
                batch_size=args.batch_size,
                last_sequence=int(checkpoint['last_sequence']),
                metrics=metrics,
            )
            kv.put_string(cp_key, json.dumps(checkpoint, sort_keys=True, separators=(',', ':')))
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
    summary['dry_run'] = bool(args.dry_run)
    summary['dry_run_check_target'] = bool(getattr(args, 'dry_run_check_target', True))
    manifest = {
        'manifest_schema': 'matrixark_context_backfill_manifest_v1',
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
        'dry_run': bool(args.dry_run),
        'dry_run_check_target': bool(getattr(args, 'dry_run_check_target', True)),
        'summary': dict(summary),
    }
    manifest_payload_sha256 = canonical_json_sha256(manifest)
    manifest['manifest_payload_sha256'] = manifest_payload_sha256
    summary['manifest_schema'] = manifest['manifest_schema']
    summary['manifest_payload_sha256'] = manifest_payload_sha256
    summary['manifest_key'] = f'{target_prefix}:backfill_manifest'
    if not args.dry_run:
        kv.hset(summary['manifest_key'], args.job_id, json.dumps(manifest, sort_keys=True, separators=(',', ':')))
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(
            metrics.to_prometheus(job_id=args.job_id, raw_backend=raw_backend, source_range=source_range),
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


def _prom_label_value(value: Any) -> str:
    return str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')


def _prom_labels(**labels: Any) -> str:
    return ','.join(f'{key}="{_prom_label_value(value)}"' for key, value in labels.items())


def validation_to_prometheus(validation: Json) -> str:
    job_id = str(validation.get('job_id') or '')
    raw_backend = str(validation.get('raw_backend') or '')
    target_prefix = str(validation.get('target_prefix') or '')
    mode = str(validation.get('mode') or 'validate_shadow')
    base = {
        'job_id': job_id,
        'raw_backend': raw_backend,
        'target_prefix': target_prefix,
        'mode': mode,
    }
    lines = [
        '# HELP matrixark_context_backfill_validation_status Validation status for a shadow target.',
        '# TYPE matrixark_context_backfill_validation_status gauge',
        f'matrixark_context_backfill_validation_status{{{_prom_labels(**base, status=str(validation.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_validation_records Record counts observed during validation.',
        '# TYPE matrixark_context_backfill_validation_records gauge',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="expected")}}} {int(validation.get("expected_records", 0) or 0)}',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="actual")}}} {int(validation.get("actual_records", 0) or 0)}',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="dead_letter")}}} {int(validation.get("dead_letters", 0) or 0)}',
        '# HELP matrixark_context_backfill_validation_check Validation check result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_validation_check gauge',
    ]
    checks = validation.get('checks') if isinstance(validation.get('checks'), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(f'matrixark_context_backfill_validation_check{{{_prom_labels(**base, check=check_name)}}} {1 if passed else 0}')
    readiness = validation.get('promotion_readiness') if isinstance(validation.get('promotion_readiness'), dict) else {}
    readiness_status = str(readiness.get('status') or 'unknown')
    lines.extend([
        '# HELP matrixark_context_backfill_promotion_readiness_status Whether validation proved a shadow prefix ready for activation or incremental repair.',
        '# TYPE matrixark_context_backfill_promotion_readiness_status gauge',
        f'matrixark_context_backfill_promotion_readiness_status{{{_prom_labels(**base, status=readiness_status)}}} {1 if readiness.get("ready") else 0}',
    ])
    readiness_blockers = readiness.get('blockers') if isinstance(readiness.get('blockers'), list) else []
    for blocker in readiness_blockers:
        lines.append(f'matrixark_context_backfill_promotion_readiness_status{{{_prom_labels(**base, status="blocked", blocker=blocker)}}} 1')
    target_state = validation.get('target_state') if isinstance(validation.get('target_state'), dict) else {}
    target_scan = target_state.get('serving_type_count_scan') if isinstance(target_state.get('serving_type_count_scan'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_validation_target_scan Target serving-record scan stats observed during validation.',
        '# TYPE matrixark_context_backfill_validation_target_scan gauge',
    ])
    for name in ['record_count', 'batch_size', 'batches', 'read_errors', 'missing_records']:
        lines.append(f'matrixark_context_backfill_validation_target_scan{{{_prom_labels(**base, stat=name)}}} {int(target_scan.get(name, 0) or 0)}')
    expected_fingerprint = str(validation.get('expected_serving_record_fingerprint') or '')
    actual_fingerprint = str(validation.get('actual_serving_record_fingerprint') or '')
    lines.extend([
        '# HELP matrixark_context_backfill_validation_serving_record_fingerprint_info Ordered serving-record fingerprints compared during validation.',
        '# TYPE matrixark_context_backfill_validation_serving_record_fingerprint_info gauge',
        f'matrixark_context_backfill_validation_serving_record_fingerprint_info{{{_prom_labels(**base, kind="expected", fingerprint=expected_fingerprint)}}} 1',
        f'matrixark_context_backfill_validation_serving_record_fingerprint_info{{{_prom_labels(**base, kind="actual", fingerprint=actual_fingerprint)}}} 1',
    ])
    source_range = validation.get('source_range') if isinstance(validation.get('source_range'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_validation_source_range Source range boundary used during validation.',
        '# TYPE matrixark_context_backfill_validation_source_range gauge',
    ])
    for name in [
        'effective_start_seq',
        'effective_end_seq',
        'source_high_watermark_seq',
        'source_record_count',
        'discovered_record_count',
        'discovered_start_seq',
        'discovered_high_watermark_seq',
        'scan_hash_max_empty_shards',
    ]:
        value = source_range.get(name)
        if value is not None:
            lines.append(f'matrixark_context_backfill_validation_source_range{{{_prom_labels(**base, boundary=name)}}} {int(value)}')
    lines.extend([
        '# HELP matrixark_context_backfill_validation_source_range_info Source range boolean metadata observed during validation.',
        '# TYPE matrixark_context_backfill_validation_source_range_info gauge',
        f'matrixark_context_backfill_validation_source_range_info{{{_prom_labels(**base, property="source_record_count_estimated")}}} {1 if source_range.get("source_record_count_estimated") else 0}',
        f'matrixark_context_backfill_validation_source_range_info{{{_prom_labels(**base, property="user_bounded_end")}}} {1 if source_range.get("user_bounded_end") else 0}',
        '# HELP matrixark_context_backfill_validation_source_scan_mode Source scan mode observed during validation.',
        '# TYPE matrixark_context_backfill_validation_source_scan_mode gauge',
        f'matrixark_context_backfill_validation_source_scan_mode{{{_prom_labels(**base, scan_mode=str(source_range.get("scan_mode") or "unknown"))}}} 1',
    ])
    return '\n'.join(lines) + '\n'


def build_promotion_readiness(validation: Json) -> Json:
    checks = validation.get('checks') if isinstance(validation.get('checks'), dict) else {}
    blockers = [name for name, passed in sorted(checks.items()) if not bool(passed)]
    ready = validation.get('status') == 'ok' and not blockers
    expected_records = int(validation.get('expected_records', 0) or 0)
    actual_records = int(validation.get('actual_records', 0) or 0)
    dead_letters = int(validation.get('dead_letters', 0) or 0)
    expected_scan = validation.get('expected_scan') if isinstance(validation.get('expected_scan'), dict) else {}
    source_failures = int(expected_scan.get('failed', 0) or 0)
    return {
        'status': 'ready' if ready else 'blocked',
        'ready': ready,
        'action': 'activate_shadow_or_incremental_repair',
        'blockers': blockers,
        'validation_strict': bool(validation.get('validation_strict')),
        'expected_records': expected_records,
        'actual_records': actual_records,
        'dead_letters': dead_letters,
        'source_failures': source_failures,
    }


def validation_audit_fields(validation: Json | None, *, skip_validation: bool = False) -> Json:
    if not isinstance(validation, dict):
        return {
            'validation_status': 'skipped',
            'validation_skipped': True,
            'validation_skip_reason': 'skip_validation_flag' if skip_validation else 'validation_not_run',
            'validation_source_range': {},
            'validation_target_state': {},
        }
    return {
        'validation_status': validation.get('status', 'unknown'),
        'validation_skipped': False,
        'validation_skip_reason': '',
        'validation_source_range': validation.get('source_range', {}),
        'validation_target_state': validation.get('target_state', {}),
    }


def require_skip_validation_confirmation(args: argparse.Namespace, *, mode: str) -> None:
    if not args.skip_validation:
        return
    if getattr(args, 'confirm_skip_validation', '') != 'YES':
        raise BackfillError(f'{mode} with --skip-validation=1 requires --confirm-skip-validation=YES')


def inspect_activation_target_state(args: argparse.Namespace, kv: Any) -> Json:
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    counts, scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    dead_letters = target.count_dead_letters()
    record_count = int(scan.get('record_count', 0) or 0)
    read_errors = int(scan.get('read_errors', 0) or 0)
    missing_records = int(scan.get('missing_records', 0) or 0)
    return {
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'record_count': record_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': counts,
        'serving_record_fingerprint': str(scan.get('serving_record_fingerprint') or ''),
        'serving_type_count_scan': scan,
        'healthy_for_unvalidated_activation': record_count > 0 and dead_letters == 0 and read_errors == 0 and missing_records == 0,
    }


def require_unvalidated_target_state(args: argparse.Namespace, kv: Any, *, mode: str) -> Json:
    if not args.skip_validation:
        return {}
    state = inspect_activation_target_state(args, kv)
    if state['healthy_for_unvalidated_activation']:
        return state
    if getattr(args, 'confirm_unvalidated_target_state', '') == 'YES':
        return state
    raise BackfillError(
        f'{mode} with --skip-validation=1 found an empty or unhealthy target prefix; '
        'run validate_shadow or pass --confirm-unvalidated-target-state=YES to audit the break-glass activation'
    )


def inspect_rollback_target_state(args: argparse.Namespace, kv: Any, target_prefix: str) -> Json:
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=target_prefix, raw_backend=raw_backend)
    counts, scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    dead_letters = target.count_dead_letters()
    record_count = int(scan.get('record_count', 0) or 0)
    read_errors = int(scan.get('read_errors', 0) or 0)
    missing_records = int(scan.get('missing_records', 0) or 0)
    return {
        'target_prefix': target_prefix,
        'raw_backend': raw_backend,
        'record_count': record_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': counts,
        'serving_record_fingerprint': str(scan.get('serving_record_fingerprint') or ''),
        'serving_type_count_scan': scan,
        'healthy_for_rollback': record_count > 0 and dead_letters == 0 and read_errors == 0 and missing_records == 0,
    }


def require_rollback_target_state(args: argparse.Namespace, kv: Any, target_prefix: str) -> Json:
    state = inspect_rollback_target_state(args, kv, target_prefix)
    if state['healthy_for_rollback']:
        return state
    if getattr(args, 'confirm_rollback_target_state', '') == 'YES':
        return state
    raise BackfillError(
        'rollback_activation previous prefix is empty or unhealthy; '
        'restore/validate the target prefix or pass --confirm-rollback-target-state=YES to audit the break-glass rollback'
    )


def require_non_strict_validation_confirmation(args: argparse.Namespace, *, mode: str) -> None:
    if args.skip_validation or args.validation_strict:
        return
    if getattr(args, 'confirm_non_strict_validation', '') != 'YES':
        raise BackfillError(f'{mode} with --validation-strict=0 requires --confirm-non-strict-validation=YES')


def empty_activation_confirmed(args: argparse.Namespace) -> bool:
    return getattr(args, 'confirm_empty_activation', '') == 'YES'


def require_non_empty_activation(args: argparse.Namespace, validation: Json | None) -> None:
    if not isinstance(validation, dict):
        return
    expected_records = int(validation.get('expected_records', 0) or 0)
    actual_records = int(validation.get('actual_records', 0) or 0)
    if expected_records > 0 and actual_records > 0:
        return
    if empty_activation_confirmed(args):
        return
    raise BackfillError(
        'activate_shadow validation found an empty source or target; '
        'pass --confirm-empty-activation=YES only for an explicitly reviewed empty cutover'
    )


def run_validate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('validate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('validate_shadow target-prefix must differ from source-prefix')
    validation_args = Namespace(**vars(args))
    validation_args.mode = 'shadow'
    validation_args.dry_run = True
    validation_args.dry_run_check_target = False
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
    actual_type_counts, target_scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    expected_fingerprint = str(expected_summary['metrics'].get('serving_record_fingerprint') or '')
    actual_fingerprint = str(target_scan.get('serving_record_fingerprint') or '')
    target_state: Json = {
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'record_count': actual_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': actual_type_counts,
        'serving_record_fingerprint': actual_fingerprint,
        'serving_type_count_scan': target_scan,
    }
    exact_match = actual_count == expected_count
    enough_records = actual_count >= expected_count
    exact_type_match = actual_type_counts == expected_type_counts
    enough_type_records = all(int(actual_type_counts.get(record_type, 0)) >= int(count) for record_type, count in expected_type_counts.items())
    type_counts_passed = exact_type_match if args.validation_strict else enough_type_records
    fingerprint_match = actual_fingerprint == expected_fingerprint
    target_records_readable = int(target_scan.get('read_errors', 0) or 0) == 0
    passed = (
        (exact_match if args.validation_strict else enough_records)
        and type_counts_passed
        and fingerprint_match
        and target_records_readable
        and dead_letters == 0
        and int(expected_summary['metrics']['failed']) == 0
    )
    summary = {
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
        'expected_serving_record_fingerprint': expected_fingerprint,
        'actual_serving_record_fingerprint': actual_fingerprint,
        'dead_letters': dead_letters,
        'expected_scan': expected_summary['metrics'],
        'source_range': expected_summary.get('source_range', {}),
        'target_state': target_state,
        'checks': {
            'exact_record_count_match': exact_match,
            'actual_records_at_least_expected': enough_records,
            'exact_serving_type_counts_match': exact_type_match,
            'actual_serving_type_counts_at_least_expected': enough_type_records,
            'serving_record_fingerprint_match': fingerprint_match,
            'target_records_readable': target_records_readable,
            'no_shadow_dead_letters': dead_letters == 0,
            'source_scan_had_no_failures': int(expected_summary['metrics']['failed']) == 0,
        },
    }
    summary['promotion_readiness'] = build_promotion_readiness(summary)
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(validation_to_prometheus(summary), encoding='utf-8')
    return summary


def run_activate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('activate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('activate_shadow target-prefix must differ from source-prefix')
    if args.confirm_activate != 'YES':
        raise BackfillError('activate_shadow requires --confirm-activate=YES')
    require_skip_validation_confirmation(args, mode='activate_shadow')
    require_non_strict_validation_confirmation(args, mode='activate_shadow')
    validation: Json | None = None
    if not args.skip_validation:
        validation = run_validate_shadow(args)
        if validation.get('status') != 'ok':
            raise BackfillError(f'shadow validation failed: {json.dumps(validation, sort_keys=True)}')
        require_non_empty_activation(args, validation)
    validation_audit = validation_audit_fields(validation, skip_validation=args.skip_validation)
    kv = make_kv(args)
    unvalidated_target_state = require_unvalidated_target_state(args, kv, mode='activate_shadow')
    if unvalidated_target_state:
        validation_audit['validation_target_state'] = unvalidated_target_state
    previous = kv.get_string(args.active_prefix_key)
    require_expected_active_prefix(args, previous)
    require_active_prefix_precondition(args, mode='activate_shadow')
    if args.dry_run:
        return {
            'status': 'ok',
            'mode': 'activate_shadow',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
            'previous_prefix': previous,
            'target_prefix': args.target_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'validation': validation,
            **validation_audit,
            'validation_strict': bool(args.validation_strict),
            'non_strict_validation_confirmed': bool(not args.validation_strict and args.confirm_non_strict_validation == 'YES'),
            'empty_activation_confirmed': empty_activation_confirmed(args),
            'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
            'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        }
    activated_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'activated_at_ms': activated_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'source_prefix': args.source_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation': validation,
        **validation_audit,
        'validation_strict': bool(args.validation_strict),
        'non_strict_validation_confirmed': bool(not args.validation_strict and args.confirm_non_strict_validation == 'YES'),
        'empty_activation_confirmed': empty_activation_confirmed(args),
        'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
    }
    kv.put_string(f'{args.active_prefix_key}:previous:{args.job_id}', previous)
    kv.hset(f'{args.active_prefix_key}:audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, args.target_prefix)
    return {
        'status': 'ok',
        'mode': 'activate_shadow',
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:audit',
        'job_id': args.job_id,
        'validation': validation,
        **validation_audit,
        'empty_activation_confirmed': empty_activation_confirmed(args),
        'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
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
    require_expected_active_prefix(args, current_prefix)
    require_active_prefix_precondition(args, mode='rollback_activation')
    require_non_noop_rollback(args, current_prefix, previous_prefix)
    rollback_target_state = require_rollback_target_state(args, kv, previous_prefix)
    rolled_back_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'rollback_job_id': rollback_job_id,
        'rolled_back_at_ms': rolled_back_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'previous_key': previous_key,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'rollback_target_state': rollback_target_state,
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        'rollback_noop_confirmed': rollback_noop_bypassed(args),
        'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
    }
    if args.dry_run:
        return {
            'status': 'ok',
            'mode': 'rollback_activation',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
            'from_prefix': current_prefix,
            'to_prefix': previous_prefix,
            'rollback_job_id': rollback_job_id,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'rollback_target_state': rollback_target_state,
            'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
            'rollback_noop_confirmed': rollback_noop_bypassed(args),
            'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
        }
    kv.hset(f'{args.active_prefix_key}:rollback_audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, previous_prefix)
    return {
        'status': 'ok',
        'mode': 'rollback_activation',
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'rollback_job_id': rollback_job_id,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:rollback_audit',
        'job_id': args.job_id,
        'rollback_target_state': rollback_target_state,
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        'rollback_noop_confirmed': rollback_noop_bypassed(args),
        'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
    }


def incremental_promotion_consistency(validation: Json | None, promotion: Json, partial: Json, *, skip_validation: bool = False) -> Json:
    metrics = promotion.get('metrics') if isinstance(promotion.get('metrics'), dict) else {}
    data_quality_status = str(promotion.get('data_quality_status') or '')
    checks: Json = {
        'promotion_data_quality_clean': data_quality_status == 'clean',
        'promotion_had_no_failures': int(metrics.get('failed', 0) or 0) == 0,
        'promotion_had_no_dead_letters': int(metrics.get('dead_letter', 0) or 0) == 0,
        'promotion_source_scan_had_no_failures': int(metrics.get('failed', 0) or 0) == 0,
    }
    if isinstance(validation, dict):
        validation_source_range = validation.get('source_range') if isinstance(validation.get('source_range'), dict) else {}
        promotion_source_range = promotion.get('source_range') if isinstance(promotion.get('source_range'), dict) else {}
        range_keys = [
            'effective_start_seq',
            'effective_end_seq',
            'source_high_watermark_seq',
            'source_record_count',
            'scan_mode',
            'user_bounded_end',
        ]
        checks['promotion_source_range_matches_validation'] = all(
            validation_source_range.get(key) == promotion_source_range.get(key)
            for key in range_keys
        )
        checks['promotion_partial_matches_validation'] = validation.get('partial', {}) == partial == promotion.get('partial', {})
        expected_records = int(validation.get('expected_records', 0) or 0)
        promoted_or_duplicate = int(metrics.get('written', 0) or 0) + int(metrics.get('duplicate', 0) or 0)
        checks['promotion_covered_expected_records'] = promoted_or_duplicate >= expected_records
    else:
        checks['promotion_source_range_matches_validation'] = bool(skip_validation)
        checks['promotion_partial_matches_validation'] = promotion.get('partial', {}) == partial
        checks['promotion_covered_expected_records'] = True
    passed = all(bool(value) for value in checks.values())
    return {
        'status': 'ok' if passed else 'failed',
        'checks': checks,
        'promotion_data_quality_status': data_quality_status or 'unknown',
        'promotion_source_range': promotion.get('source_range', {}),
        'promotion_metrics': metrics,
    }


def incremental_repair_to_prometheus(summary: Json) -> str:
    job_id = str(summary.get('job_id') or '')
    raw_backend = str(summary.get('raw_backend') or '')
    shadow_prefix = str(summary.get('shadow_prefix') or '')
    active_prefix = str(summary.get('active_prefix') or '')
    base = {
        'job_id': job_id,
        'raw_backend': raw_backend,
        'shadow_prefix': shadow_prefix,
        'active_prefix': active_prefix,
        'mode': 'incremental_repair',
    }
    consistency = summary.get('promotion_consistency') if isinstance(summary.get('promotion_consistency'), dict) else {}
    consistency_status = str(consistency.get('status') or 'unknown')
    lines = [
        '# HELP matrixark_context_backfill_incremental_repair_status Incremental repair status.',
        '# TYPE matrixark_context_backfill_incremental_repair_status gauge',
        f'matrixark_context_backfill_incremental_repair_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_incremental_repair_promotion_consistency_status Incremental repair promotion consistency status.',
        '# TYPE matrixark_context_backfill_incremental_repair_promotion_consistency_status gauge',
        f'matrixark_context_backfill_incremental_repair_promotion_consistency_status{{{_prom_labels(**base, status=consistency_status)}}} 1',
        '# HELP matrixark_context_backfill_incremental_repair_promotion_consistency_check Incremental repair promotion consistency check result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_incremental_repair_promotion_consistency_check gauge',
    ]
    checks = consistency.get('checks') if isinstance(consistency.get('checks'), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(f'matrixark_context_backfill_incremental_repair_promotion_consistency_check{{{_prom_labels(**base, check=check_name)}}} {1 if passed else 0}')
    metrics = consistency.get('promotion_metrics') if isinstance(consistency.get('promotion_metrics'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_incremental_repair_promotion_records Promotion record counters for the active-prefix replay.',
        '# TYPE matrixark_context_backfill_incremental_repair_promotion_records gauge',
    ])
    for name in ['scanned', 'filtered', 'written', 'duplicate', 'failed', 'dead_letter', 'skipped']:
        lines.append(f'matrixark_context_backfill_incremental_repair_promotion_records{{{_prom_labels(**base, status=name)}}} {int(metrics.get(name, 0) or 0)}')
    lines.extend([
        '# HELP matrixark_context_backfill_incremental_repair_promotion_data_quality_status Data-quality status observed while replaying the repair window into the active prefix.',
        '# TYPE matrixark_context_backfill_incremental_repair_promotion_data_quality_status gauge',
        f'matrixark_context_backfill_incremental_repair_promotion_data_quality_status{{{_prom_labels(**base, status=str(consistency.get("promotion_data_quality_status") or "unknown"))}}} 1',
    ])
    source_range = consistency.get('promotion_source_range') if isinstance(consistency.get('promotion_source_range'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_incremental_repair_promotion_source_range Source range replayed into the active prefix.',
        '# TYPE matrixark_context_backfill_incremental_repair_promotion_source_range gauge',
    ])
    for name in ['effective_start_seq', 'effective_end_seq', 'source_high_watermark_seq', 'source_record_count']:
        value = source_range.get(name)
        if value is not None:
            lines.append(f'matrixark_context_backfill_incremental_repair_promotion_source_range{{{_prom_labels(**base, boundary=name)}}} {int(value)}')
    lines.extend([
        '# HELP matrixark_context_backfill_incremental_repair_validation_status Validation status observed before active-prefix promotion.',
        '# TYPE matrixark_context_backfill_incremental_repair_validation_status gauge',
        f'matrixark_context_backfill_incremental_repair_validation_status{{{_prom_labels(**base, status=str(summary.get("validation_status") or "unknown"), skipped=str(bool(summary.get("validation_skipped"))).lower())}}} 1',
    ])
    return '\n'.join(lines) + '\n'


def run_plan(args: argparse.Namespace) -> Json:
    partial = build_partial_spec(args)
    validate_partial_args(args, partial)
    raw_backend = normalize_raw_backend(args.raw_backend)
    kv = make_kv(args)
    target_prefix = resolve_target_prefix(args)
    source = MatrixKVRecordLog(kv, prefix=args.source_prefix)
    target = MatrixKVBackfillTarget(kv, prefix=target_prefix, raw_backend=raw_backend)
    cp_key = checkpoint_key(
        target_prefix,
        args.job_id,
        source_prefix=args.source_prefix,
        raw_backend=raw_backend,
        partial=partial,
    )
    resume_state = read_checkpoint_state(kv, cp_key)
    effective_start_seq, resume_state = apply_resume_checkpoint(
        args=args,
        start_seq=max(0, args.start_seq),
        resume_state=resume_state,
    )
    source_range = source.source_range(start_seq=effective_start_seq, end_seq=args.end_seq)
    plan_scan_hash_discovery_enabled = bool(getattr(args, 'plan_discover_scan_hash', True))
    if plan_scan_hash_discovery_enabled:
        source_range = discover_scan_hash_source_range(
            source,
            source_range,
            end_seq=args.end_seq,
            max_empty_scan_shards=args.source_scan_max_empty_shards,
        )
    current_active_prefix = kv.get_string(args.active_prefix_key) if args.active_prefix_key else ''
    expected_active_prefix = str(getattr(args, 'expect_active_prefix', '') or '')
    target_count = target.count()
    dead_letter_count = target.count_dead_letters()
    dead_letter_export_command_args: list[str] = []
    if dead_letter_count > 0:
        dead_letter_export_command_args = [
            '--mode=export_dead_letters',
            _plan_arg('metaserver', args.metaserver),
            _plan_arg('namespace', args.namespace),
            _plan_arg('table', args.table),
            _plan_arg('raw-backend', raw_backend),
            _plan_arg('target-prefix', target_prefix),
            _plan_arg('job-id', args.job_id),
            '--dead-letter-start=0',
            _plan_arg('dead-letter-limit', max(1, int(getattr(args, 'dead_letter_limit', 100) or 100))),
        ]
        _append_plan_arg(dead_letter_export_command_args, 'library-path', getattr(args, 'library_path', ''))
        _append_plan_arg(dead_letter_export_command_args, 'local-kv', getattr(args, 'local_kv', ''))
    planned_source_records = estimate_source_window_records(source_range)
    chunk_plan = build_plan_windows(args, source_range=source_range, target_prefix=target_prefix)
    active_target = bool(current_active_prefix and current_active_prefix == target_prefix)
    incremental_window_bounded = args.end_seq is not None and args.end_seq > args.start_seq
    safety_checks: Json = {
        'no_writes_performed': True,
        'source_prefix_present': bool(args.source_prefix),
        'target_prefix_present': bool(target_prefix),
        'target_differs_from_source': target_prefix != args.source_prefix,
        'batch_size_positive': args.batch_size > 0,
        'partial_filters_valid': True,
        'resume_checkpoint_compatible': not bool(resume_state.get('checkpoint_ignored')),
        'in_place_confirmed_if_needed': args.mode != 'in_place' or args.confirm_in_place == 'YES',
        'active_target_confirmed_if_needed': not active_target or getattr(args, 'confirm_active_target', '') == 'YES',
        'incremental_window_bounded': incremental_window_bounded,
        'incremental_confirmed_if_requested': getattr(args, 'confirm_incremental_repair', '') == 'YES',
        'active_prefix_precondition_satisfied': not expected_active_prefix or current_active_prefix == expected_active_prefix,
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
    }
    required_confirmations: list[str] = []
    if target_prefix == args.source_prefix:
        required_confirmations.append('--mode=in_place --confirm-in-place=YES')
    if active_target and getattr(args, 'confirm_active_target', '') != 'YES':
        required_confirmations.append('--confirm-active-target=YES for direct writes into the active prefix')
    if getattr(args, 'confirm_incremental_repair', '') != 'YES':
        required_confirmations.append('--confirm-incremental-repair=YES before incremental_repair promotion')
    if not expected_active_prefix and not active_prefix_precondition_bypassed(args):
        required_confirmations.append('--expect-active-prefix=<current> or --confirm-no-active-prefix-precondition=YES before active-prefix mutation')
    readiness_blockers = [
        name
        for name, passed in sorted(safety_checks.items())
        if name not in {
            'active_prefix_precondition_bypassed',
            'incremental_confirmed_if_requested',
            'incremental_window_bounded',
        }
        and not bool(passed)
    ]
    if partial.get('enabled') and not incremental_window_bounded and not any(
        partial.get(key) for key in ['record_types', 'tenant_ids', 'user_ids', 'session_ids', 'filter_json']
    ):
        readiness_blockers.append('partial_backfill_requires_bounded_range_or_filter')
    summary = {
        'status': 'ok' if not readiness_blockers else 'needs_confirmation',
        'mode': 'plan',
        'job_id': args.job_id,
        'raw_backend': raw_backend,
        'source_prefix': args.source_prefix,
        'target_prefix': target_prefix,
        'target_is_default_shadow_prefix': not bool(args.target_prefix),
        'active_prefix_key': args.active_prefix_key,
        'current_active_prefix': current_active_prefix,
        'expected_active_prefix': expected_active_prefix,
        'repair_active_prefix': str(getattr(args, 'repair_active_prefix', '') or ''),
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'effective_start_seq': effective_start_seq,
        'source_range': source_range,
        'plan_scan_hash_discovery_enabled': plan_scan_hash_discovery_enabled,
        'plan_scan_hash_discovery_used': bool(plan_scan_hash_discovery_enabled and source_range.get('scan_hash_max_empty_shards') is not None),
        'planned_source_records': planned_source_records,
        'planned_source_records_estimated': bool(source_range.get('source_record_count_estimated')),
        'partial': partial,
        'batch_size': args.batch_size,
        'source_scan_max_empty_shards': args.source_scan_max_empty_shards,
        'chunk_plan': chunk_plan,
        'resume_state': resume_state,
        'checkpoint_key': cp_key,
        'target_state': {
            'record_count': target_count,
            'dead_letter_count': dead_letter_count,
            'is_current_active_prefix': active_target,
            'raw_backend': raw_backend,
            'dead_letter_export_command_args': dead_letter_export_command_args,
            'dead_letter_export_recommended': dead_letter_count > 0,
        },
        'execution_modes': {
            'batch_shadow': {
                'command_mode': 'shadow',
                'dry_run_default': True,
                'write_command_requires': ['--dry-run=0'],
            },
            'incremental_repair': {
                'command_mode': 'incremental_repair',
                'bounded_window_required': True,
                'active_prefix_required': True,
                'write_command_requires': ['--dry-run=0', '--confirm-incremental-repair=YES'],
            },
            'in_place': {
                'command_mode': 'in_place',
                'guarded': True,
                'write_command_requires': ['--dry-run=0', '--confirm-in-place=YES'],
            },
        },
        'safety_checks': safety_checks,
        'required_confirmations': sorted(set(required_confirmations)),
        'readiness_blockers': sorted(set(readiness_blockers)),
        'next_steps': [
            'run shadow backfill with --dry-run=0 into target_prefix',
            'run validate_shadow against target_prefix',
            'run activate_shadow for full cutover or incremental_repair for bounded active repair',
        ],
    }
    artifacts = write_plan_artifacts(args, summary)
    if artifacts:
        summary['plan_artifacts'] = artifacts
    return summary


def run_incremental_repair(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('incremental_repair requires --target-prefix for the shadow repair prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('incremental_repair target-prefix must differ from source-prefix')
    if args.end_seq is None or args.end_seq <= args.start_seq:
        raise BackfillError('incremental_repair requires a bounded --start-seq/--end-seq window')
    if args.confirm_incremental_repair != 'YES':
        raise BackfillError('incremental_repair requires --confirm-incremental-repair=YES')
    require_skip_validation_confirmation(args, mode='incremental_repair')
    require_non_strict_validation_confirmation(args, mode='incremental_repair')

    validation: Json | None = None
    if not args.skip_validation:
        validation = run_validate_shadow(args)
        if validation.get('status') != 'ok':
            raise BackfillError(f'incremental repair shadow validation failed: {json.dumps(validation, sort_keys=True)}')
    validation_audit = validation_audit_fields(validation, skip_validation=args.skip_validation)

    kv = make_kv(args)
    unvalidated_target_state = require_unvalidated_target_state(args, kv, mode='incremental_repair')
    if unvalidated_target_state:
        validation_audit['validation_target_state'] = unvalidated_target_state
    current_active_prefix = kv.get_string(args.active_prefix_key)
    require_expected_active_prefix(args, current_active_prefix)
    require_active_prefix_precondition(args, mode='incremental_repair')
    active_prefix = args.repair_active_prefix or current_active_prefix
    if not active_prefix:
        raise BackfillError('incremental_repair requires --repair-active-prefix or an active prefix stored under --active-prefix-key')
    if active_prefix in {args.source_prefix, args.target_prefix}:
        raise BackfillError('incremental_repair active prefix must differ from source-prefix and shadow repair prefix')

    promote_args = clone_args(
        args,
        mode='shadow',
        target_prefix=active_prefix,
        job_id=f'{args.job_id}:active',
        confirm_active_target='YES',
    )
    promotion = run_backfill(promote_args)
    partial = build_partial_spec(args)
    promotion_consistency = incremental_promotion_consistency(
        validation,
        promotion,
        partial,
        skip_validation=args.skip_validation,
    )
    if promotion_consistency.get('status') != 'ok':
        raise BackfillError(f'incremental repair promotion consistency failed: {json.dumps(promotion_consistency, sort_keys=True)}')

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
            'active_prefix_key': args.active_prefix_key,
            'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
            'current_active_prefix': current_active_prefix,
            'start_seq': args.start_seq,
            'end_seq': args.end_seq,
            'partial': partial,
            'validation': validation,
            **validation_audit,
            'validation_strict': bool(args.validation_strict),
            'non_strict_validation_confirmed': bool(not args.validation_strict and args.confirm_non_strict_validation == 'YES'),
            'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
            'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
            'promotion_consistency': promotion_consistency,
            'promotion_metrics': promotion.get('metrics', {}),
        }
        kv.hset(f'{args.active_prefix_key}:incremental_repair_audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))

    summary = {
        'status': 'ok',
        'mode': 'incremental_repair',
        'job_id': args.job_id,
        'source_prefix': args.source_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'shadow_prefix': args.target_prefix,
        'active_prefix': active_prefix,
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'current_active_prefix': current_active_prefix,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': partial,
        'validation': validation,
        **validation_audit,
        'promotion': promotion,
        'promotion_consistency': promotion_consistency,
        'audit_key': f'{args.active_prefix_key}:incremental_repair_audit',
        'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(incremental_repair_to_prometheus(summary), encoding='utf-8')
    return summary


def verify_manifest_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'target_prefix': str(summary.get('target_prefix') or ''),
        'mode': 'verify_manifest',
    }
    lines = [
        '# HELP matrixark_context_backfill_manifest_verification_status Manifest verification status for persisted backfill evidence.',
        '# TYPE matrixark_context_backfill_manifest_verification_status gauge',
        f'matrixark_context_backfill_manifest_verification_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_manifest_verification_check Manifest verification check result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_manifest_verification_check gauge',
    ]
    checks = summary.get('checks') if isinstance(summary.get('checks'), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(f'matrixark_context_backfill_manifest_verification_check{{{_prom_labels(**base, check=check_name)}}} {1 if passed else 0}')
    return '\n'.join(lines) + '\n'


def maybe_write_verify_manifest_prometheus(args: argparse.Namespace, summary: Json) -> None:
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(verify_manifest_to_prometheus(summary), encoding='utf-8')


def run_verify_manifest(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('verify_manifest requires --target-prefix')
    kv = make_kv(args)
    manifest_key = f'{args.target_prefix}:backfill_manifest'
    raw_manifest = kv.hget(manifest_key, args.job_id)
    if not raw_manifest:
        summary = {
            'status': 'failed',
            'mode': 'verify_manifest',
            'job_id': args.job_id,
            'target_prefix': args.target_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'manifest_key': manifest_key,
            'checks': {
                'manifest_found': False,
                'manifest_schema_supported': False,
                'manifest_payload_sha256_match': False,
            },
            'error': 'manifest not found',
        }
        maybe_write_verify_manifest_prometheus(args, summary)
        return summary
    try:
        manifest = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        summary = {
            'status': 'failed',
            'mode': 'verify_manifest',
            'job_id': args.job_id,
            'target_prefix': args.target_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'manifest_key': manifest_key,
            'checks': {
                'manifest_found': True,
                'manifest_json_valid': False,
                'manifest_schema_supported': False,
                'manifest_payload_sha256_match': False,
            },
            'error': f'invalid manifest JSON: {exc}',
        }
        maybe_write_verify_manifest_prometheus(args, summary)
        return summary
    expected_hash = str(manifest.get('manifest_payload_sha256') or '')
    payload = dict(manifest)
    payload.pop('manifest_payload_sha256', None)
    actual_hash = canonical_json_sha256(payload)
    checks = {
        'manifest_found': True,
        'manifest_json_valid': True,
        'manifest_schema_supported': manifest.get('manifest_schema') == 'matrixark_context_backfill_manifest_v1',
        'manifest_payload_sha256_present': bool(expected_hash),
        'manifest_payload_sha256_match': bool(expected_hash) and expected_hash == actual_hash,
        'manifest_job_id_matches': manifest.get('job_id') == args.job_id,
        'manifest_target_prefix_matches': manifest.get('target_prefix') == args.target_prefix,
        'manifest_raw_backend_matches': normalize_raw_backend(str(manifest.get('raw_backend') or args.raw_backend)) == normalize_raw_backend(args.raw_backend),
    }
    status = 'ok' if all(bool(value) for value in checks.values()) else 'failed'
    summary = {
        'status': status,
        'mode': 'verify_manifest',
        'job_id': args.job_id,
        'target_prefix': args.target_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'manifest_key': manifest_key,
        'manifest_schema': manifest.get('manifest_schema', ''),
        'manifest_payload_sha256': expected_hash,
        'computed_manifest_payload_sha256': actual_hash,
        'manifest_mode': manifest.get('mode', ''),
        'manifest_source_prefix': manifest.get('source_prefix', ''),
        'manifest_source_range': manifest.get('source_range', {}),
        'manifest_partial': manifest.get('partial', {}),
        'checks': checks,
    }
    maybe_write_verify_manifest_prometheus(args, summary)
    return summary


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
    parser.add_argument('--mode', choices=['plan', 'shadow', 'in_place', 'validate_shadow', 'activate_shadow', 'rollback_activation', 'incremental_repair', 'verify_manifest', 'verify_plan_artifacts', 'export_dead_letters'], default='shadow')
    parser.add_argument('--confirm-in-place', default='')
    parser.add_argument('--confirm-activate', default='')
    parser.add_argument('--confirm-rollback', default='')
    parser.add_argument('--confirm-rollback-noop', default='', help='required YES to audit an intentional rollback whose previous prefix equals the current active prefix')
    parser.add_argument('--confirm-rollback-target-state', default='', help='required YES to roll back to an empty or unhealthy previous prefix')
    parser.add_argument('--confirm-incremental-repair', default='')
    parser.add_argument('--confirm-active-target', default='', help='required YES for direct non-dry-run shadow writes to the current active prefix')
    parser.add_argument('--expect-active-prefix', default='', help='optional active-prefix precondition for activation, rollback, and incremental repair')
    parser.add_argument('--confirm-no-active-prefix-precondition', default='', help='required YES to mutate the active prefix without --expect-active-prefix')
    parser.add_argument('--confirm-skip-validation', default='', help='required YES when activate_shadow or incremental_repair uses --skip-validation=1')
    parser.add_argument('--confirm-non-strict-validation', default='', help='required YES when activate_shadow or incremental_repair uses --validation-strict=0')
    parser.add_argument('--confirm-unvalidated-target-state', default='', help='required YES to activate an empty or unhealthy target while using --skip-validation=1')
    parser.add_argument('--confirm-empty-activation', default='', help='required YES to activate a validated shadow whose expected or actual record count is zero')
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
    parser.add_argument('--plan-window-size', type=int, default=0, help='plan-only bounded source records per execution window; 0 disables chunk planning')
    parser.add_argument('--plan-max-windows', type=int, default=128, help='plan-only maximum windows to emit; 0 emits all windows')
    parser.add_argument('--plan-parallelism', type=int, default=1, help='plan-only number of independent chunk shadows to group into each preparation wave')
    parser.add_argument('--plan-output-dir', default='', help='plan-only directory for plan.json plus runnable shadow/validation/promotion scripts')
    parser.add_argument('--plan-discover-scan-hash', type=int, choices=[0, 1], default=1, help='plan-only read scan-hash raw-log refs to discover high watermark when record_count/index metadata is absent')
    parser.add_argument('--confirm-plan-output-overwrite', default='', help='required YES when --plan-output-dir already contains files')
    parser.add_argument('--source-scan-max-empty-shards', type=int, default=2)
    parser.add_argument('--dry-run', type=int, choices=[0, 1], default=1)
    parser.add_argument('--dry-run-check-target', type=int, choices=[0, 1], default=1, help='during dry-run, check target idempotency so duplicate and would-write counts match a real run')
    parser.add_argument('--resume', type=int, choices=[0, 1], default=1)
    parser.add_argument('--confirm-resume-range-change', default='', help='required YES to ignore an existing checkpoint whose source range differs from requested start/end')
    parser.add_argument('--fail-fast', action='store_true')
    parser.add_argument('--dead-letter-start', type=int, default=0, help='export_dead_letters start offset')
    parser.add_argument('--dead-letter-limit', type=int, default=100, help='export_dead_letters maximum rows to return and optionally write')
    parser.add_argument('--dead-letter-output', default='', help='optional JSONL output path for export_dead_letters')
    parser.add_argument('--prometheus-output', default='')
    parser.add_argument('--local-kv', default='', help='test-only JSON KV backend path')
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.dry_run = bool(args.dry_run)
    args.dry_run_check_target = bool(args.dry_run_check_target)
    args.resume = bool(args.resume)
    args.validation_strict = bool(args.validation_strict)
    args.skip_validation = bool(args.skip_validation)
    args.partial = bool(args.partial)
    args.partial_require_bounded = bool(args.partial_require_bounded)
    args.plan_discover_scan_hash = bool(args.plan_discover_scan_hash)
    args.raw_backend = normalize_raw_backend(args.raw_backend)
    if args.batch_size <= 0:
        parser.error('--batch-size must be positive')
    if args.plan_window_size < 0:
        parser.error('--plan-window-size must be non-negative')
    if args.plan_max_windows < 0:
        parser.error('--plan-max-windows must be non-negative')
    if args.plan_parallelism <= 0:
        parser.error('--plan-parallelism must be positive')
    if args.source_scan_max_empty_shards <= 0:
        parser.error('--source-scan-max-empty-shards must be positive')
    if args.dead_letter_start < 0:
        parser.error('--dead-letter-start must be non-negative')
    if args.dead_letter_limit < 0:
        parser.error('--dead-letter-limit must be non-negative')
    try:
        if args.mode == 'plan':
            summary = run_plan(args)
        elif args.mode == 'validate_shadow':
            summary = run_validate_shadow(args)
        elif args.mode == 'activate_shadow':
            summary = run_activate_shadow(args)
        elif args.mode == 'rollback_activation':
            summary = run_rollback_activation(args)
        elif args.mode == 'incremental_repair':
            summary = run_incremental_repair(args)
        elif args.mode == 'verify_manifest':
            summary = run_verify_manifest(args)
        elif args.mode == 'verify_plan_artifacts':
            summary = run_verify_plan_artifacts(args)
        elif args.mode == 'export_dead_letters':
            summary = run_export_dead_letters(args)
        else:
            summary = run_backfill(args)
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'error': str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
