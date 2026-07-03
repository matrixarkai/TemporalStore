#!/usr/bin/env python3
"""Unit coverage for MatrixArk context backfill."""

from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import matrixark_context_backfill as backfill


def write_sharded(kv: backfill.LocalJsonKV, prefix: str, sequence: int, record: dict) -> None:
    shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
    offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
    kv.hset(f"{prefix}:records:{shard:06d}", f"{offset:020d}", json.dumps(record, sort_keys=True))


def read_target_records(kv: backfill.LocalJsonKV, prefix: str) -> list[dict]:
    count = int(kv.get_string(f"{prefix}:record_count") or 0)
    records = []
    for sequence in range(count):
        shard = sequence // backfill.DIRECT_RECORD_LOG_SHARD_SIZE
        offset = sequence % backfill.DIRECT_RECORD_LOG_SHARD_SIZE
        records.append(json.loads(kv.hget(f"{prefix}:records:{shard:06d}", f"{offset:020d}")))
    return records


class MatrixArkContextBackfillTest(unittest.TestCase):
    def make_args(self, path: Path, **overrides):
        values = {
            "metaserver": "unused",
            "namespace": "unused",
            "table": "unused",
            "library_path": "",
            "source_prefix": "matrixark:mcp",
            "raw_backend": "temporalstore",
            "target_prefix": "matrixark:context_backfill:test",
            "mode": "shadow",
            "confirm_in_place": "",
            "confirm_activate": "",
            "confirm_rollback": "",
            "confirm_incremental_repair": "",
            "confirm_active_target": "",
            "confirm_skip_validation": "",
            "confirm_non_strict_validation": "",
            "active_prefix_key": "matrixark:context:active_prefix",
            "expect_active_prefix": "",
            "rollback_job_id": "",
            "repair_active_prefix": "",
            "validation_strict": True,
            "skip_validation": False,
            "job_id": "unit",
            "start_seq": 0,
            "end_seq": None,
            "partial": False,
            "partial_record_types": "",
            "partial_tenant_ids": "",
            "partial_user_ids": "",
            "partial_session_ids": "",
            "partial_filter_json": "",
            "partial_require_bounded": True,
            "batch_size": 2,
            "source_scan_max_empty_shards": 2,
            "dry_run": False,
            "dry_run_check_target": True,
            "resume": True,
            "confirm_resume_range_change": "",
            "fail_fast": False,
            "prometheus_output": "",
            "local_kv": str(path),
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_sharded_backfill_checkpoint_and_prometheus(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1, "text": "alpha"})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_summary", "summary_text": "alpha summary"})
            kv.put_string("matrixark:mcp:record_count", "2")
            prom = Path(tmp) / "backfill.prom"

            summary = backfill.run_backfill(self.make_args(path, prometheus_output=str(prom)))
            self.assertEqual(summary["data_quality_status"], "clean")
            self.assertFalse(summary["has_failures"])
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["metrics"]["written"], 2)
            self.assertEqual(summary["metrics"]["source_batches"], 1)
            self.assertEqual(summary["metrics"]["target_batches"], 1)
            self.assertRegex(summary["metrics"]["serving_record_fingerprint"], r"^[0-9a-f]{64}$")
            self.assertEqual(summary["resume_state"]["checkpoint_format"], "missing")
            self.assertFalse(summary["resume_state"]["checkpoint_found"])
            self.assertEqual(summary["resume_state"]["effective_start_seq"], 0)
            self.assertEqual(summary["source_range"]["scan_mode"], "record_count")
            self.assertEqual(summary["source_range"]["source_record_count"], 2)
            self.assertEqual(summary["source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(summary["source_range"]["effective_start_seq"], 0)
            self.assertEqual(summary["source_range"]["effective_end_seq"], 2)
            self.assertFalse(summary["source_range"]["user_bounded_end"])
            prom_text = prom.read_text()
            self.assertIn("matrixark_context_backfill_run_elapsed_ms", prom_text)
            self.assertIn("matrixark_context_backfill_scan_qps", prom_text)
            self.assertIn("matrixark_context_backfill_data_quality_status", prom_text)
            self.assertIn('status="clean"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_records_total", prom_text)
            self.assertIn("matrixark_context_backfill_batches_total", prom_text)
            self.assertIn("matrixark_context_backfill_serving_record_fingerprint_info", prom_text)
            self.assertIn('boundary="source_high_watermark_seq"} 1', prom_text)
            self.assertIn('boundary="effective_end_seq"} 2', prom_text)
            self.assertIn('raw_backend="temporalstore"', prom_text)
            self.assertEqual(summary["raw_backend"], "temporalstore")
            self.assertEqual(len(read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")), 2)
            cp_key = backfill.checkpoint_key(
                "matrixark:context_backfill:test",
                "unit",
                source_prefix="matrixark:mcp",
                raw_backend="temporalstore",
                partial=summary["partial"],
            )
            checkpoint = json.loads(backfill.LocalJsonKV(path).get_string(cp_key))
            self.assertEqual(checkpoint["version"], 2)
            self.assertEqual(checkpoint["last_sequence"], 1)
            self.assertEqual(checkpoint["job_id"], "unit")
            self.assertEqual(checkpoint["source_prefix"], "matrixark:mcp")
            self.assertEqual(checkpoint["target_prefix"], "matrixark:context_backfill:test")
            self.assertEqual(checkpoint["raw_backend"], "temporalstore")
            self.assertEqual(checkpoint["source_range"]["scan_mode"], "record_count")
            self.assertEqual(checkpoint["source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(checkpoint["source_range"]["effective_end_seq"], 2)
            self.assertEqual(checkpoint["metrics"]["written"], 2)

            resumed = backfill.run_backfill(self.make_args(path, prometheus_output=str(prom)))
            self.assertEqual(resumed["metrics"]["scanned"], 0)
            self.assertEqual(resumed["resume_state"]["checkpoint_format"], "json")
            self.assertEqual(resumed["resume_state"]["checkpoint_last_sequence"], 1)
            self.assertEqual(resumed["resume_state"]["checkpoint_source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(resumed["resume_state"]["effective_start_seq"], 2)

    def test_dry_run_checks_target_idempotency_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")

            applied = backfill.run_backfill(self.make_args(path, dry_run=False, resume=False))
            self.assertEqual(applied["metrics"]["written"], 2)

            dry = backfill.run_backfill(self.make_args(path, dry_run=True, resume=False))
            self.assertTrue(dry["dry_run"])
            self.assertTrue(dry["dry_run_check_target"])
            self.assertEqual(dry["metrics"]["scanned"], 2)
            self.assertEqual(dry["metrics"]["duplicate"], 2)
            self.assertEqual(dry["metrics"]["written"], 0)
            self.assertEqual(len(read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")), 2)

            source_only = backfill.run_backfill(self.make_args(path, dry_run=True, dry_run_check_target=False, resume=False))
            self.assertFalse(source_only["dry_run_check_target"])
            self.assertEqual(source_only["metrics"]["scanned"], 2)
            self.assertEqual(source_only["metrics"]["duplicate"], 0)
            self.assertEqual(source_only["metrics"]["written"], 2)
            self.assertEqual(len(read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")), 2)

    def test_shadow_write_to_active_prefix_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")

            args = self.make_args(path, target_prefix="matrixark:context:active", dry_run=False, resume=False)
            with self.assertRaisesRegex(backfill.BackfillError, "confirm-active-target=YES"):
                backfill.run_backfill(args)

            dry = backfill.run_backfill(self.make_args(path, target_prefix="matrixark:context:active", dry_run=True, resume=False))
            self.assertEqual(dry["metrics"]["written"], 1)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context:active:record_count"), "")

            confirmed = backfill.run_backfill(self.make_args(
                path,
                target_prefix="matrixark:context:active",
                dry_run=False,
                resume=False,
                confirm_active_target="YES",
            ))
            self.assertEqual(confirmed["metrics"]["written"], 1)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context:active:record_count"), "1")

    def test_target_serving_type_counts_use_batched_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            target = backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context_backfill:test")
            target.append_many([
                {"record_type": "context_event", "idempotency_key": "event-1"},
                {"record_type": "context_summary", "idempotency_key": "summary-1"},
                {"record_type": "context_event", "idempotency_key": "event-2"},
            ])
            kv.batch_hget_calls = 0

            counts, stats = target.serving_type_counts_with_stats(batch_size=2)

            self.assertEqual(counts, {"context_event": 2, "context_summary": 1})
            self.assertEqual(stats["record_count"], 3)
            self.assertEqual(stats["batch_size"], 2)
            self.assertEqual(stats["batches"], 2)
            self.assertEqual(stats["read_errors"], 0)
            self.assertRegex(stats["serving_record_fingerprint"], r"^[0-9a-f]{64}$")
            self.assertEqual(kv.batch_hget_calls, 2)

    def test_resume_accepts_legacy_integer_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 3})
            kv.put_string("matrixark:mcp:record_count", "3")
            cp_key = backfill.checkpoint_key("matrixark:context_backfill:test", "unit", source_prefix="matrixark:mcp", raw_backend="temporalstore", partial=backfill.build_partial_spec(self.make_args(path)))
            kv.put_string(cp_key, "0")

            summary = backfill.run_backfill(self.make_args(path, batch_size=2, resume=True))
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["resume_state"]["checkpoint_format"], "legacy_integer")
            self.assertEqual(summary["resume_state"]["checkpoint_last_sequence"], 0)
            self.assertEqual(summary["resume_state"]["effective_start_seq"], 1)
            self.assertEqual([record["event_id_hash"] for record in read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")], [2, 3])

    def test_resume_accepts_structured_checkpoint_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")
            cp_key = backfill.checkpoint_key("matrixark:context_backfill:test", "unit", source_prefix="matrixark:mcp", raw_backend="temporalstore", partial=backfill.build_partial_spec(self.make_args(path)))
            kv.put_string(cp_key, json.dumps({"version": 2, "last_sequence": 0, "job_id": "unit"}))

            summary = backfill.run_backfill(self.make_args(path, batch_size=2, resume=True))
            self.assertEqual(summary["metrics"]["scanned"], 1)
            self.assertEqual(summary["resume_state"]["checkpoint_format"], "json")
            self.assertEqual(summary["resume_state"]["checkpoint_last_sequence"], 0)
            self.assertEqual(summary["resume_state"]["effective_start_seq"], 1)
            self.assertEqual([record["event_id_hash"] for record in read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")], [2])

    def test_resume_rejects_incompatible_source_range_without_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 3})
            kv.put_string("matrixark:mcp:record_count", "3")

            first = backfill.run_backfill(self.make_args(path, start_seq=0, end_seq=2, batch_size=2, resume=True))
            self.assertEqual(first["metrics"]["written"], 2)

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-resume-range-change=YES"):
                backfill.run_backfill(self.make_args(path, start_seq=1, end_seq=3, batch_size=2, resume=True))

            confirmed = backfill.run_backfill(self.make_args(
                path,
                start_seq=1,
                end_seq=3,
                batch_size=2,
                resume=True,
                confirm_resume_range_change="YES",
            ))
            self.assertTrue(confirmed["resume_state"]["checkpoint_ignored"])
            self.assertEqual(confirmed["resume_state"]["checkpoint_ignore_reason"], "source_range_mismatch_confirmed")
            self.assertEqual(confirmed["resume_state"]["effective_start_seq"], 1)
            self.assertEqual(confirmed["metrics"]["scanned"], 2)
            self.assertEqual(confirmed["metrics"]["duplicate"], 1)
            self.assertEqual(confirmed["metrics"]["written"], 1)
            records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")
            self.assertEqual([record["event_id_hash"] for record in records], [1, 2, 3])


    def test_scan_hash_backfill_without_record_count_or_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 2})
            prom = Path(tmp) / "scan_hash.prom"

            summary = backfill.run_backfill(self.make_args(path, batch_size=1, resume=False, prometheus_output=str(prom)))
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["metrics"]["written"], 2)
            self.assertEqual(summary["metrics"]["scan_hash_batches"], 2)
            self.assertEqual(summary["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(summary["source_range"]["source_record_count"], 2)
            self.assertTrue(summary["source_range"]["source_record_count_estimated"])
            self.assertEqual(summary["source_range"]["source_high_watermark_seq"], 2)
            self.assertEqual(summary["source_range"]["discovered_record_count"], 2)
            self.assertEqual(summary["source_range"]["discovered_start_seq"], 0)
            self.assertEqual(summary["source_range"]["discovered_high_watermark_seq"], 2)
            self.assertEqual(summary["source_range"]["effective_end_seq"], 3)
            self.assertEqual(summary["source_range"]["scan_hash_max_empty_shards"], 2)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:record_count"), "2")
            prom_text = prom.read_text()
            self.assertIn('boundary="discovered_record_count"} 2', prom_text)
            self.assertIn('boundary="discovered_start_seq"} 0', prom_text)
            self.assertIn('boundary="discovered_high_watermark_seq"} 2', prom_text)
            self.assertIn('boundary="scan_hash_max_empty_shards"} 2', prom_text)
            self.assertIn('property="source_record_count_estimated"} 1', prom_text)
            self.assertIn('property="user_bounded_end"} 0', prom_text)
            self.assertIn('scan_mode="scan_hash"} 1', prom_text)
            validate_prom = Path(tmp) / "scan_hash_validate.prom"
            validation = backfill.run_validate_shadow(self.make_args(
                path,
                mode="validate_shadow",
                batch_size=1,
                prometheus_output=str(validate_prom),
            ))
            self.assertEqual(validation["status"], "ok")
            validate_prom_text = validate_prom.read_text()
            self.assertIn("matrixark_context_backfill_validation_source_range", validate_prom_text)
            self.assertIn('boundary="discovered_record_count"} 2', validate_prom_text)
            self.assertIn('property="source_record_count_estimated"} 1', validate_prom_text)
            self.assertIn("matrixark_context_backfill_validation_source_scan_mode", validate_prom_text)
            self.assertIn('scan_mode="scan_hash"} 1', validate_prom_text)
            cp_key = backfill.checkpoint_key(
                "matrixark:context_backfill:test",
                "unit",
                source_prefix="matrixark:mcp",
                raw_backend="temporalstore",
                partial=summary["partial"],
            )
            checkpoint = json.loads(backfill.LocalJsonKV(path).get_string(cp_key))
            self.assertEqual(checkpoint["last_sequence"], 2)
            self.assertEqual(checkpoint["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(checkpoint["source_range"]["source_record_count"], 2)
            self.assertTrue(checkpoint["source_range"]["source_record_count_estimated"])
            self.assertEqual(checkpoint["source_range"]["source_high_watermark_seq"], 2)
            self.assertEqual(checkpoint["source_range"]["effective_end_seq"], 3)
            self.assertEqual(checkpoint["source_range"]["discovered_record_count"], 2)

    def test_scan_hash_respects_sequence_range(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 0})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 2})

            summary = backfill.run_backfill(self.make_args(path, start_seq=1, end_seq=3, resume=False))
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(summary["source_range"]["requested_start_seq"], 1)
            self.assertEqual(summary["source_range"]["requested_end_seq"], 3)
            self.assertEqual(summary["source_range"]["effective_start_seq"], 1)
            self.assertEqual(summary["source_range"]["effective_end_seq"], 3)
            self.assertEqual(summary["source_range"]["source_record_count"], 2)
            self.assertTrue(summary["source_range"]["source_record_count_estimated"])
            self.assertEqual(summary["source_range"]["source_high_watermark_seq"], 2)
            self.assertEqual(summary["source_range"]["discovered_record_count"], 2)
            self.assertEqual(summary["source_range"]["discovered_start_seq"], 1)
            self.assertEqual(summary["source_range"]["discovered_high_watermark_seq"], 2)
            self.assertTrue(summary["source_range"]["user_bounded_end"])
            records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")
            self.assertEqual([record["event_id_hash"] for record in records], [1, 2])

    def test_legacy_index_backfill(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("legacy:record_index", json.dumps(["000:event:1"]))
            kv.hset("legacy:records", "000:event:1", json.dumps({"record_type": "context_event", "event_id_hash": 10}))

            summary = backfill.run_backfill(self.make_args(path, source_prefix="legacy", target_prefix="shadow:legacy"))
            self.assertEqual(summary["metrics"]["scanned"], 1)
            self.assertEqual(summary["metrics"]["context_events"], 1)


    def test_batch_helpers_use_backend_batch_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            source = backfill.MatrixKVRecordLog(kv, prefix="matrixark:mcp")

            rows = source.read_many([(0, None), (1, None)])
            self.assertEqual([row[0] for row in rows], [0, 1])
            self.assertTrue(all(row[2] is None for row in rows))
            self.assertEqual(kv.batch_hget_calls, 1)

            target = backfill.MatrixKVBackfillTarget(kv, prefix="shadow:batch")
            target.append_many([{ "record_type": "context_event", "event_id_hash": 3 }])
            self.assertEqual(kv.matrixark_append_records_calls, 1)
            self.assertEqual(kv.matrixark_append_records_options[-1]["raw_storage_backend"], "temporalstore")
            self.assertEqual(kv.get_string("shadow:batch:record_count"), "1")

    def test_run_backfill_prefetches_source_batch_idempotency_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1, "idempotency_key": "existing"})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2, "idempotency_key": "new"})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 3, "idempotency_key": "new"})
            kv.put_string("matrixark:mcp:record_count", "3")
            kv.hset("matrixark:context_backfill:test:idempotency", "existing", "0")

            original_make_kv = backfill.make_kv
            backfill.make_kv = lambda args: kv
            try:
                summary = backfill.run_backfill(self.make_args(path, batch_size=3, resume=False))
            finally:
                backfill.make_kv = original_make_kv

            self.assertEqual(summary["metrics"]["scanned"], 3)
            self.assertEqual(summary["metrics"]["duplicate"], 2)
            self.assertEqual(summary["metrics"]["written"], 1)
            self.assertEqual(kv.batch_hget_calls, 3)
            records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")
            self.assertEqual([record["event_id_hash"] for record in records], [2])

    def test_target_prefetches_idempotency_keys_and_skips_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.hset("shadow:dedupe:idempotency", "already", "0")
            target = backfill.MatrixKVBackfillTarget(kv, prefix="shadow:dedupe")

            append_stats = target.append_many([
                {"record_type": "context_event", "event_id_hash": 1, "idempotency_key": "already"},
                {"record_type": "context_event", "event_id_hash": 2, "idempotency_key": "new"},
                {"record_type": "context_debug_record", "ref_hash": 2, "idempotency_key": "new"},
                {"record_type": "context_event", "event_id_hash": 4, "idempotency_key": "other"},
            ])

            self.assertEqual(append_stats["attempted"], 4)
            self.assertEqual(append_stats["written"], 3)
            self.assertEqual(append_stats["duplicate"], 1)
            self.assertEqual([record["event_id_hash"] for record in append_stats["appended_records"] if "event_id_hash" in record], [2, 4])
            self.assertEqual(kv.batch_hget_calls, 1)
            self.assertEqual(kv.get_string("shadow:dedupe:record_count"), "3")
            records = read_target_records(backfill.LocalJsonKV(path), "shadow:dedupe")
            self.assertEqual([record["record_type"] for record in records], ["context_event", "context_debug_record", "context_event"])
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.hget("shadow:dedupe:idempotency", "new"), "1")
            self.assertEqual(kv_after.hget("shadow:dedupe:idempotency", "other"), "2")

    def test_run_backfill_uses_append_stats_for_write_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")

            original_append_many = backfill.MatrixKVBackfillTarget.append_many

            def append_one_and_skip_one(target, records):
                self.assertEqual(len(records), 2)
                stats = original_append_many(target, [records[0]])
                stats["attempted"] = len(records)
                stats["duplicate"] += 1
                return stats

            with patch.object(backfill.MatrixKVBackfillTarget, "append_many", append_one_and_skip_one):
                summary = backfill.run_backfill(self.make_args(path, batch_size=2, resume=False))

            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["metrics"]["written"], 1)
            self.assertEqual(summary["metrics"]["duplicate"], 1)
            self.assertEqual(summary["metrics"]["context_events"], 1)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:record_count"), "1")


    def test_validate_and_activate_shadow_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:old")

            summary = backfill.run_backfill(self.make_args(path, target_prefix="matrixark:context_backfill:candidate"))
            self.assertEqual(summary["metrics"]["written"], 2)

            prom = Path(tmp) / "validate.prom"
            validation = backfill.run_validate_shadow(self.make_args(
                path,
                mode="validate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                prometheus_output=str(prom),
            ))
            self.assertEqual(validation["status"], "ok")
            self.assertEqual(validation["expected_records"], 2)
            self.assertEqual(validation["actual_records"], 2)
            self.assertEqual(validation["expected_type_counts"], {"context_event": 2})
            self.assertEqual(validation["actual_type_counts"], {"context_event": 2})
            self.assertRegex(validation["expected_serving_record_fingerprint"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                validation["actual_serving_record_fingerprint"],
                validation["expected_serving_record_fingerprint"],
            )
            self.assertEqual(validation["source_range"]["scan_mode"], "record_count")
            self.assertEqual(validation["source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(validation["source_range"]["effective_end_seq"], 2)
            self.assertEqual(validation["target_state"]["target_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(validation["target_state"]["record_count"], 2)
            self.assertEqual(validation["target_state"]["dead_letter_count"], 0)
            self.assertEqual(validation["target_state"]["serving_type_counts"], {"context_event": 2})
            self.assertEqual(
                validation["target_state"]["serving_record_fingerprint"],
                validation["expected_serving_record_fingerprint"],
            )
            self.assertTrue(validation["checks"]["exact_serving_type_counts_match"])
            self.assertTrue(validation["checks"]["serving_record_fingerprint_match"])
            self.assertEqual(validation["promotion_readiness"]["status"], "ready")
            self.assertTrue(validation["promotion_readiness"]["ready"])
            self.assertEqual(validation["promotion_readiness"]["blockers"], [])
            self.assertEqual(validation["promotion_readiness"]["expected_records"], 2)
            self.assertEqual(validation["promotion_readiness"]["actual_records"], 2)
            prom_text = prom.read_text()
            self.assertIn("matrixark_context_backfill_validation_status", prom_text)
            self.assertIn('status="ok"', prom_text)
            self.assertIn('kind="expected"} 2', prom_text)
            self.assertIn('check="target_records_readable"} 1', prom_text)
            self.assertIn('check="serving_record_fingerprint_match"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_validation_serving_record_fingerprint_info", prom_text)
            self.assertIn("matrixark_context_backfill_validation_source_range", prom_text)
            self.assertIn('boundary="effective_end_seq"} 2', prom_text)
            self.assertIn('boundary="source_high_watermark_seq"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_validation_source_scan_mode", prom_text)
            self.assertIn('scan_mode="record_count"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_promotion_readiness_status", prom_text)
            self.assertIn('status="ready"} 1', prom_text)

            with self.assertRaises(backfill.BackfillError):
                backfill.run_activate_shadow(self.make_args(path, mode="activate_shadow", target_prefix="matrixark:context_backfill:candidate"))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                confirm_activate="YES",
                dry_run=False,
            ))
            self.assertEqual(activated["status"], "ok")
            self.assertEqual(activated["validation_status"], "ok")
            self.assertFalse(activated["validation_skipped"])
            self.assertEqual(activated["validation_skip_reason"], "")
            self.assertEqual(activated["validation_source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(activated["validation_target_state"]["record_count"], 2)
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context_backfill:candidate")
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix:previous:unit"), "matrixark:context:old")
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertEqual(activation_audit["validation_status"], "ok")
            self.assertFalse(activation_audit["validation_skipped"])
            self.assertEqual(activation_audit["validation_skip_reason"], "")
            self.assertTrue(activation_audit["validation_strict"])
            self.assertFalse(activation_audit["non_strict_validation_confirmed"])
            self.assertEqual(activation_audit["validation_target_state"]["record_count"], 2)
            self.assertIn("matrixark:context_backfill:candidate", json.dumps(activation_audit, sort_keys=True))

            with self.assertRaisesRegex(backfill.BackfillError, "active prefix precondition failed"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:candidate",
                    confirm_activate="YES",
                    expect_active_prefix="matrixark:context:old",
                    dry_run=False,
                ))

            with self.assertRaises(backfill.BackfillError):
                backfill.run_rollback_activation(self.make_args(path, mode="rollback_activation", rollback_job_id="unit"))

            dry_run = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                confirm_rollback="YES",
                dry_run=True,
            ))
            self.assertEqual(dry_run["to_prefix"], "matrixark:context:old")
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context:active_prefix"), "matrixark:context_backfill:candidate")

            with self.assertRaisesRegex(backfill.BackfillError, "active prefix precondition failed"):
                backfill.run_rollback_activation(self.make_args(
                    path,
                    mode="rollback_activation",
                    rollback_job_id="unit",
                    job_id="rollback-stale",
                    confirm_rollback="YES",
                    expect_active_prefix="matrixark:context:other",
                    dry_run=False,
                ))

            rollback = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                job_id="rollback-unit",
                confirm_rollback="YES",
                expect_active_prefix="matrixark:context_backfill:candidate",
                dry_run=False,
            ))
            self.assertEqual(rollback["status"], "ok")
            self.assertEqual(rollback["expected_active_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(rollback["from_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(rollback["to_prefix"], "matrixark:context:old")
            kv_rolled_back = backfill.LocalJsonKV(path)
            self.assertEqual(kv_rolled_back.get_string("matrixark:context:active_prefix"), "matrixark:context:old")
            self.assertIn("matrixark:context_backfill:candidate", kv_rolled_back.hget("matrixark:context:active_prefix:rollback_audit", "rollback-unit"))

    def test_activate_shadow_skip_validation_is_explicitly_audited(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:old")

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-skip-validation=YES"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:candidate",
                    confirm_activate="YES",
                    skip_validation=True,
                    dry_run=False,
                ))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                confirm_activate="YES",
                confirm_skip_validation="YES",
                skip_validation=True,
                dry_run=False,
            ))

            self.assertEqual(activated["status"], "ok")
            self.assertIsNone(activated["validation"])
            self.assertEqual(activated["validation_status"], "skipped")
            self.assertTrue(activated["validation_skipped"])
            self.assertEqual(activated["validation_skip_reason"], "skip_validation_flag")
            self.assertEqual(activated["validation_source_range"], {})
            self.assertEqual(activated["validation_target_state"], {})

            kv_after = backfill.LocalJsonKV(path)
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertIsNone(activation_audit["validation"])
            self.assertEqual(activation_audit["validation_status"], "skipped")
            self.assertTrue(activation_audit["validation_skipped"])
            self.assertEqual(activation_audit["validation_skip_reason"], "skip_validation_flag")
            self.assertTrue(activation_audit["validation_strict"])
            self.assertFalse(activation_audit["non_strict_validation_confirmed"])
            self.assertEqual(activation_audit["validation_source_range"], {})
            self.assertEqual(activation_audit["validation_target_state"], {})

    def test_activate_shadow_non_strict_validation_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:old")
            shadow = backfill.run_backfill(self.make_args(
                path,
                target_prefix="matrixark:context_backfill:candidate",
                resume=False,
            ))
            self.assertEqual(shadow["metrics"]["written"], 1)

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-non-strict-validation=YES"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:candidate",
                    confirm_activate="YES",
                    validation_strict=False,
                    dry_run=False,
                ))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                confirm_activate="YES",
                confirm_non_strict_validation="YES",
                validation_strict=False,
                dry_run=False,
            ))

            self.assertEqual(activated["status"], "ok")
            self.assertEqual(activated["validation_status"], "ok")
            kv_after = backfill.LocalJsonKV(path)
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertFalse(activation_audit["validation_strict"])
            self.assertTrue(activation_audit["non_strict_validation_confirmed"])


    def test_validate_shadow_fails_on_type_mismatch_even_when_count_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            target = backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context_backfill:test")
            target.append_many([{"record_type": "context_summary", "summary_text": "wrong type"}])

            validation = backfill.run_validate_shadow(self.make_args(path, mode="validate_shadow"))

            self.assertEqual(validation["expected_records"], 1)
            self.assertEqual(validation["actual_records"], 1)
            self.assertEqual(validation["status"], "failed")
            self.assertEqual(validation["promotion_readiness"]["status"], "blocked")
            self.assertFalse(validation["promotion_readiness"]["ready"])
            self.assertIn("exact_serving_type_counts_match", validation["promotion_readiness"]["blockers"])
            self.assertIn("serving_record_fingerprint_match", validation["promotion_readiness"]["blockers"])
            self.assertEqual(validation["expected_type_counts"], {"context_event": 1})
            self.assertEqual(validation["actual_type_counts"], {"context_summary": 1})
            self.assertEqual(validation["source_range"]["source_high_watermark_seq"], 0)
            self.assertEqual(validation["target_state"]["record_count"], 1)
            self.assertEqual(validation["target_state"]["serving_type_counts"], {"context_summary": 1})
            self.assertEqual(validation["target_state"]["serving_type_count_scan"]["read_errors"], 0)
            self.assertFalse(validation["checks"]["exact_serving_type_counts_match"])
            self.assertFalse(validation["checks"]["serving_record_fingerprint_match"])

    def test_validate_shadow_fails_on_content_mismatch_even_when_type_counts_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1, "text": "expected"})
            kv.put_string("matrixark:mcp:record_count", "1")
            target = backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context_backfill:test")
            target.append_many([{"record_type": "context_event", "event_id_hash": 1, "text": "different"}])

            validation = backfill.run_validate_shadow(self.make_args(path, mode="validate_shadow"))

            self.assertEqual(validation["expected_records"], 1)
            self.assertEqual(validation["actual_records"], 1)
            self.assertEqual(validation["expected_type_counts"], {"context_event": 1})
            self.assertEqual(validation["actual_type_counts"], {"context_event": 1})
            self.assertEqual(validation["status"], "failed")
            self.assertTrue(validation["checks"]["exact_record_count_match"])
            self.assertTrue(validation["checks"]["exact_serving_type_counts_match"])
            self.assertFalse(validation["checks"]["serving_record_fingerprint_match"])
            self.assertNotEqual(
                validation["actual_serving_record_fingerprint"],
                validation["expected_serving_record_fingerprint"],
            )

    def test_validate_shadow_reports_unreadable_target_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")
            target = backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context_backfill:test")
            target.append_many([{"record_type": "context_event", "event_id_hash": 1, "idempotency_key": "event-1"}])
            kv.put_string("matrixark:context_backfill:test:record_count", "2")

            prom = Path(tmp) / "validate_failed.prom"
            validation = backfill.run_validate_shadow(self.make_args(path, mode="validate_shadow", batch_size=2, prometheus_output=str(prom)))

            self.assertEqual(validation["status"], "failed")
            self.assertEqual(validation["expected_records"], 2)
            self.assertEqual(validation["actual_records"], 2)
            self.assertEqual(validation["actual_type_counts"], {"context_event": 1})
            self.assertEqual(validation["target_state"]["serving_type_count_scan"]["read_errors"], 1)
            self.assertEqual(validation["target_state"]["serving_type_count_scan"]["missing_records"], 1)
            self.assertFalse(validation["checks"]["target_records_readable"])
            self.assertEqual(validation["promotion_readiness"]["status"], "blocked")
            self.assertIn("target_records_readable", validation["promotion_readiness"]["blockers"])
            prom_text = prom.read_text()
            self.assertIn('status="failed"', prom_text)
            self.assertIn('check="target_records_readable"} 0', prom_text)
            self.assertIn("matrixark_context_backfill_promotion_readiness_status", prom_text)
            self.assertIn('status="blocked"} 0', prom_text)
            self.assertIn('status="blocked",blocker="target_records_readable"} 1', prom_text)
            self.assertIn('stat="read_errors"} 1', prom_text)


    def test_incremental_repair_promotes_bounded_range_to_active_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2})
            kv.put_string("matrixark:mcp:record_count", "2")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")

            shadow_args = self.make_args(
                path,
                target_prefix="matrixark:context_repair:p1",
                start_seq=1,
                end_seq=2,
                resume=False,
            )
            shadow = backfill.run_backfill(shadow_args)
            self.assertEqual(shadow["metrics"]["written"], 1)

            with self.assertRaises(backfill.BackfillError):
                backfill.run_incremental_repair(self.make_args(
                    path,
                    mode="incremental_repair",
                    target_prefix="matrixark:context_repair:p1",
                    start_seq=1,
                    end_seq=2,
                    resume=False,
                ))

            repair_args = self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:p1",
                start_seq=1,
                end_seq=2,
                confirm_incremental_repair="YES",
                resume=False,
            )
            with self.assertRaisesRegex(backfill.BackfillError, "active prefix precondition failed"):
                backfill.run_incremental_repair(self.make_args(
                    path,
                    mode="incremental_repair",
                    target_prefix="matrixark:context_repair:p1",
                    start_seq=1,
                    end_seq=2,
                    confirm_incremental_repair="YES",
                    expect_active_prefix="matrixark:context:other",
                    resume=False,
                ))

            repair_args.expect_active_prefix = "matrixark:context:active"
            repaired = backfill.run_incremental_repair(repair_args)
            self.assertEqual(repaired["status"], "ok")
            self.assertEqual(repaired["active_prefix"], "matrixark:context:active")
            self.assertEqual(repaired["expected_active_prefix"], "matrixark:context:active")
            self.assertEqual(repaired["current_active_prefix"], "matrixark:context:active")
            self.assertEqual(repaired["promotion"]["metrics"]["written"], 1)
            self.assertEqual(repaired["promotion"]["data_quality_status"], "clean")
            self.assertEqual(repaired["validation_status"], "ok")
            self.assertFalse(repaired["validation_skipped"])
            self.assertEqual(repaired["validation_skip_reason"], "")
            self.assertEqual(repaired["validation_source_range"]["effective_start_seq"], 1)
            self.assertEqual(repaired["validation_target_state"]["target_prefix"], "matrixark:context_repair:p1")
            self.assertEqual(repaired["promotion_consistency"]["status"], "ok")
            self.assertEqual(repaired["promotion_consistency"]["promotion_data_quality_status"], "clean")
            self.assertTrue(repaired["promotion_consistency"]["checks"]["promotion_data_quality_clean"])
            self.assertTrue(repaired["promotion_consistency"]["checks"]["promotion_had_no_failures"])
            self.assertTrue(repaired["promotion_consistency"]["checks"]["promotion_source_range_matches_validation"])
            self.assertTrue(repaired["promotion_consistency"]["checks"]["promotion_covered_expected_records"])

            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context:active")
            repair_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:incremental_repair_audit", "unit"))
            self.assertEqual(repair_audit["validation_status"], "ok")
            self.assertFalse(repair_audit["validation_skipped"])
            self.assertEqual(repair_audit["validation_skip_reason"], "")
            self.assertTrue(repair_audit["validation_strict"])
            self.assertFalse(repair_audit["non_strict_validation_confirmed"])
            self.assertEqual(repair_audit["validation_target_state"]["record_count"], 1)
            self.assertEqual(repair_audit["expected_active_prefix"], "matrixark:context:active")
            self.assertEqual(repair_audit["current_active_prefix"], "matrixark:context:active")
            self.assertEqual(repair_audit["promotion_consistency"]["status"], "ok")
            self.assertIn("matrixark:context:active", json.dumps(repair_audit, sort_keys=True))
            active_records = read_target_records(kv_after, "matrixark:context:active")
            self.assertEqual([record["event_id_hash"] for record in active_records], [2])

            retried = backfill.run_incremental_repair(repair_args)
            self.assertEqual(retried["promotion"]["metrics"]["duplicate"], 1)
            self.assertEqual(retried["promotion_consistency"]["status"], "ok")
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context:active:record_count"), "1")

            prom = Path(tmp) / "incremental_repair.prom"
            monitored = backfill.run_incremental_repair(self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:p1",
                start_seq=1,
                end_seq=2,
                confirm_incremental_repair="YES",
                resume=False,
                prometheus_output=str(prom),
            ))
            self.assertEqual(monitored["promotion"]["metrics"]["duplicate"], 1)
            prom_text = prom.read_text()
            self.assertIn("matrixark_context_backfill_incremental_repair_promotion_consistency_status", prom_text)
            self.assertIn('status="ok"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_incremental_repair_promotion_data_quality_status", prom_text)
            self.assertIn('status="clean"} 1', prom_text)
            self.assertIn('check="promotion_source_range_matches_validation"} 1', prom_text)
            self.assertIn('status="duplicate"} 1', prom_text)
            self.assertIn('boundary="effective_start_seq"} 1', prom_text)
            self.assertIn('matrixark_context_backfill_incremental_repair_validation_status', prom_text)

    def test_incremental_repair_rejects_inconsistent_promotion(self):
        partial = {"enabled": False, "record_types": [], "tenant_ids": [], "user_ids": [], "session_ids": [], "filter_json": {}}
        validation = {
            "expected_records": 1,
            "partial": partial,
            "source_range": {
                "effective_start_seq": 1,
                "effective_end_seq": 2,
                "source_high_watermark_seq": 1,
                "source_record_count": 2,
                "scan_mode": "record_count",
                "user_bounded_end": True,
            },
        }
        promotion = {
            "data_quality_status": "clean",
            "partial": partial,
            "source_range": {
                "effective_start_seq": 1,
                "effective_end_seq": 3,
                "source_high_watermark_seq": 2,
                "source_record_count": 3,
                "scan_mode": "record_count",
                "user_bounded_end": True,
            },
            "metrics": {"written": 1, "duplicate": 0, "failed": 0, "dead_letter": 0},
        }

        consistency = backfill.incremental_promotion_consistency(validation, promotion, partial)

        self.assertEqual(consistency["status"], "failed")
        self.assertFalse(consistency["checks"]["promotion_source_range_matches_validation"])

    def test_incremental_repair_consistency_requires_clean_promotion_quality(self):
        partial = {"enabled": False, "record_types": [], "tenant_ids": [], "user_ids": [], "session_ids": [], "filter_json": {}}
        validation = {
            "expected_records": 1,
            "partial": partial,
            "source_range": {
                "effective_start_seq": 0,
                "effective_end_seq": 1,
                "source_high_watermark_seq": 0,
                "source_record_count": 1,
                "scan_mode": "record_count",
                "user_bounded_end": True,
            },
        }
        promotion = {
            "data_quality_status": "completed_with_errors",
            "partial": partial,
            "source_range": validation["source_range"],
            "metrics": {"written": 1, "duplicate": 0, "failed": 0, "dead_letter": 0},
        }

        consistency = backfill.incremental_promotion_consistency(validation, promotion, partial)

        self.assertEqual(consistency["status"], "failed")
        self.assertEqual(consistency["promotion_data_quality_status"], "completed_with_errors")
        self.assertFalse(consistency["checks"]["promotion_data_quality_clean"])

    def test_incremental_repair_raises_when_promotion_has_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")
            args = self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:p1",
                start_seq=1,
                end_seq=2,
                confirm_incremental_repair="YES",
                resume=False,
            )
            validation = {
                "status": "ok",
                "expected_records": 1,
                "partial": backfill.build_partial_spec(args),
                "source_range": {
                    "effective_start_seq": 1,
                    "effective_end_seq": 2,
                    "source_high_watermark_seq": 1,
                    "source_record_count": 2,
                    "scan_mode": "record_count",
                    "user_bounded_end": True,
                },
                "target_state": {},
            }
            promotion = {
                "partial": backfill.build_partial_spec(args),
                "source_range": validation["source_range"],
                "metrics": {"written": 0, "duplicate": 0, "failed": 1, "dead_letter": 1},
            }
            with patch.object(backfill, "run_validate_shadow", return_value=validation), patch.object(backfill, "run_backfill", return_value=promotion):
                with self.assertRaisesRegex(backfill.BackfillError, "promotion consistency failed"):
                    backfill.run_incremental_repair(args)

    def test_incremental_repair_skip_validation_is_explicitly_audited(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-skip-validation=YES"):
                backfill.run_incremental_repair(self.make_args(
                    path,
                    mode="incremental_repair",
                    target_prefix="matrixark:context_repair:skip",
                    start_seq=0,
                    end_seq=1,
                    confirm_incremental_repair="YES",
                    skip_validation=True,
                    resume=False,
                ))

            repaired = backfill.run_incremental_repair(self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:skip",
                start_seq=0,
                end_seq=1,
                confirm_incremental_repair="YES",
                confirm_skip_validation="YES",
                skip_validation=True,
                resume=False,
            ))

            self.assertEqual(repaired["status"], "ok")
            self.assertIsNone(repaired["validation"])
            self.assertEqual(repaired["validation_status"], "skipped")
            self.assertTrue(repaired["validation_skipped"])
            self.assertEqual(repaired["validation_skip_reason"], "skip_validation_flag")
            self.assertEqual(repaired["validation_source_range"], {})
            self.assertEqual(repaired["validation_target_state"], {})
            self.assertEqual(repaired["promotion"]["metrics"]["written"], 1)

            kv_after = backfill.LocalJsonKV(path)
            repair_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:incremental_repair_audit", "unit"))
            self.assertIsNone(repair_audit["validation"])
            self.assertEqual(repair_audit["validation_status"], "skipped")
            self.assertTrue(repair_audit["validation_skipped"])
            self.assertEqual(repair_audit["validation_skip_reason"], "skip_validation_flag")
            self.assertTrue(repair_audit["validation_strict"])
            self.assertFalse(repair_audit["non_strict_validation_confirmed"])
            self.assertEqual(repair_audit["validation_source_range"], {})
            self.assertEqual(repair_audit["validation_target_state"], {})
            active_records = read_target_records(kv_after, "matrixark:context:active")
            self.assertEqual([record["event_id_hash"] for record in active_records], [1])

    def test_incremental_repair_non_strict_validation_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")
            shadow = backfill.run_backfill(self.make_args(
                path,
                target_prefix="matrixark:context_repair:p1",
                start_seq=0,
                end_seq=1,
                resume=False,
            ))
            self.assertEqual(shadow["metrics"]["written"], 1)

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-non-strict-validation=YES"):
                backfill.run_incremental_repair(self.make_args(
                    path,
                    mode="incremental_repair",
                    target_prefix="matrixark:context_repair:p1",
                    start_seq=0,
                    end_seq=1,
                    confirm_incremental_repair="YES",
                    validation_strict=False,
                    resume=False,
                    dry_run=False,
                ))

            repaired = backfill.run_incremental_repair(self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:p1",
                start_seq=0,
                end_seq=1,
                confirm_incremental_repair="YES",
                confirm_non_strict_validation="YES",
                validation_strict=False,
                resume=False,
                dry_run=False,
            ))

            self.assertEqual(repaired["status"], "ok")
            kv_after = backfill.LocalJsonKV(path)
            repair_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:incremental_repair_audit", "unit"))
            self.assertFalse(repair_audit["validation_strict"])
            self.assertTrue(repair_audit["non_strict_validation_confirmed"])

    def test_partial_backfill_filters_and_isolates_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1, "scope": {"tenant_id": "t1", "user_id": "u1"}})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2, "scope": {"tenant_id": "t2", "user_id": "u2"}})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_summary", "summary_text": "skip", "scope": {"tenant_id": "t1", "user_id": "u1"}})
            kv.put_string("matrixark:mcp:record_count", "3")

            with self.assertRaises(backfill.BackfillError):
                backfill.run_backfill(self.make_args(path, partial=True))

            full = backfill.run_backfill(self.make_args(path, target_prefix="shadow:full", job_id="same", resume=False))
            partial = backfill.run_backfill(self.make_args(
                path,
                target_prefix="shadow:partial",
                job_id="same",
                partial=True,
                partial_user_ids="u1",
                partial_record_types="context_event",
                resume=True,
            ))
            self.assertEqual(full["metrics"]["written"], 3)
            self.assertEqual(partial["metrics"]["scanned"], 3)
            self.assertEqual(partial["metrics"]["filtered"], 2)
            self.assertEqual(partial["metrics"]["written"], 1)
            self.assertTrue(partial["partial"]["enabled"])

            kv_after = backfill.LocalJsonKV(path)
            records = read_target_records(kv_after, "shadow:partial")
            self.assertEqual([record["event_id_hash"] for record in records], [1])
            manifest = kv_after.hget("shadow:partial:backfill_manifest", "same")
            self.assertIn('"partial"', manifest)

            full_cp = backfill.checkpoint_key("shadow:full", "same", source_prefix="matrixark:mcp", raw_backend="temporalstore", partial=full["partial"])
            partial_cp = backfill.checkpoint_key("shadow:partial", "same", source_prefix="matrixark:mcp", raw_backend="temporalstore", partial=partial["partial"])
            self.assertNotEqual(full_cp, partial_cp)
            self.assertEqual(json.loads(kv_after.get_string(partial_cp))["last_sequence"], 2)

    def test_raw_backend_isolates_checkpoint_manifest_and_append_options(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 11, "text": "raw backend"})
            kv.put_string("matrixark:mcp:record_count", "1")

            temporal = backfill.run_backfill(self.make_args(
                path,
                target_prefix="shadow:temporal",
                job_id="raw-mode",
                raw_backend="temporalstore",
                resume=True,
            ))
            matrixkv = backfill.run_backfill(self.make_args(
                path,
                target_prefix="shadow:matrixkv",
                job_id="raw-mode",
                raw_backend="matrixkv",
                resume=True,
            ))

            self.assertEqual(temporal["raw_backend"], "temporalstore")
            self.assertEqual(matrixkv["raw_backend"], "matrixkv")
            self.assertNotEqual(
                backfill.checkpoint_key("shadow:temporal", "raw-mode", source_prefix="matrixark:mcp", raw_backend="temporalstore", partial=temporal["partial"]),
                backfill.checkpoint_key("shadow:temporal", "raw-mode", source_prefix="matrixark:mcp", raw_backend="matrixkv", partial=temporal["partial"]),
            )

            kv_after = backfill.LocalJsonKV(path)
            manifest = json.loads(kv_after.hget("shadow:matrixkv:backfill_manifest", "raw-mode"))
            self.assertEqual(manifest["raw_backend"], "matrixkv")
            record = read_target_records(kv_after, "shadow:matrixkv")[0]
            self.assertEqual(record["backfill"]["raw_backend"], "matrixkv")

    def test_incremental_repair_honors_partial_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1, "scope": {"session_id": "s1"}})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 2, "scope": {"session_id": "s2"}})
            kv.put_string("matrixark:mcp:record_count", "2")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")

            common = {
                "partial": True,
                "partial_session_ids": "s2",
                "start_seq": 0,
                "end_seq": 2,
                "resume": False,
            }
            shadow = backfill.run_backfill(self.make_args(path, target_prefix="matrixark:context_repair:partial", **common))
            self.assertEqual(shadow["metrics"]["written"], 1)
            repaired = backfill.run_incremental_repair(self.make_args(
                path,
                mode="incremental_repair",
                target_prefix="matrixark:context_repair:partial",
                confirm_incremental_repair="YES",
                **common,
            ))
            self.assertEqual(repaired["promotion"]["metrics"]["written"], 1)
            self.assertEqual(repaired["promotion"]["metrics"]["filtered"], 1)
            active_records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context:active")
            self.assertEqual([record["event_id_hash"] for record in active_records], [2])

    def test_missing_record_dead_letters_and_in_place_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:mcp:record_count", "1")

            summary = backfill.run_backfill(self.make_args(path))
            self.assertEqual(summary["metrics"]["failed"], 1)
            self.assertEqual(summary["metrics"]["dead_letter"], 1)
            self.assertEqual(summary["data_quality_status"], "completed_with_errors")
            self.assertTrue(summary["has_failures"])
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:dead_letter_count"), "1")

            with self.assertRaises(backfill.BackfillError):
                backfill.run_backfill(self.make_args(path, mode="in_place"))


if __name__ == "__main__":
    unittest.main()
