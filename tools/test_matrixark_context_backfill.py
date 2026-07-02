#!/usr/bin/env python3
"""Unit coverage for MatrixArk context backfill."""

from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path

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
            "target_prefix": "matrixark:context_backfill:test",
            "mode": "shadow",
            "confirm_in_place": "",
            "confirm_activate": "",
            "confirm_incremental_repair": "",
            "active_prefix_key": "matrixark:context:active_prefix",
            "repair_active_prefix": "",
            "validation_strict": True,
            "skip_validation": False,
            "job_id": "unit",
            "start_seq": 0,
            "end_seq": None,
            "batch_size": 2,
            "source_scan_max_empty_shards": 2,
            "dry_run": False,
            "resume": True,
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
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["metrics"]["written"], 2)
            self.assertEqual(summary["metrics"]["source_batches"], 1)
            self.assertEqual(summary["metrics"]["target_batches"], 1)
            prom_text = prom.read_text()
            self.assertIn("matrixark_context_backfill_records_total", prom_text)
            self.assertIn("matrixark_context_backfill_batches_total", prom_text)
            self.assertEqual(len(read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")), 2)

            resumed = backfill.run_backfill(self.make_args(path, prometheus_output=str(prom)))
            self.assertEqual(resumed["metrics"]["scanned"], 0)


    def test_scan_hash_backfill_without_record_count_or_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 2})

            summary = backfill.run_backfill(self.make_args(path, batch_size=1, resume=False))
            self.assertEqual(summary["metrics"]["scanned"], 2)
            self.assertEqual(summary["metrics"]["written"], 2)
            self.assertEqual(summary["metrics"]["scan_hash_batches"], 2)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:record_count"), "2")

    def test_scan_hash_respects_sequence_range(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 0})
            write_sharded(kv, "matrixark:mcp", 1, {"record_type": "context_event", "event_id_hash": 1})
            write_sharded(kv, "matrixark:mcp", 2, {"record_type": "context_event", "event_id_hash": 2})

            summary = backfill.run_backfill(self.make_args(path, start_seq=1, end_seq=3, resume=False))
            self.assertEqual(summary["metrics"]["scanned"], 2)
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
            self.assertEqual(kv.get_string("shadow:batch:record_count"), "1")


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

            validation = backfill.run_validate_shadow(self.make_args(path, mode="validate_shadow", target_prefix="matrixark:context_backfill:candidate"))
            self.assertEqual(validation["status"], "ok")
            self.assertEqual(validation["expected_records"], 2)
            self.assertEqual(validation["actual_records"], 2)

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
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context_backfill:candidate")
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix:previous:unit"), "matrixark:context:old")
            self.assertIn("matrixark:context_backfill:candidate", kv_after.hget("matrixark:context:active_prefix:audit", "unit"))


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
            repaired = backfill.run_incremental_repair(repair_args)
            self.assertEqual(repaired["status"], "ok")
            self.assertEqual(repaired["active_prefix"], "matrixark:context:active")
            self.assertEqual(repaired["promotion"]["metrics"]["written"], 1)

            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context:active")
            self.assertIn("matrixark:context:active", kv_after.hget("matrixark:context:active_prefix:incremental_repair_audit", "unit"))
            active_records = read_target_records(kv_after, "matrixark:context:active")
            self.assertEqual([record["event_id_hash"] for record in active_records], [2])

            retried = backfill.run_incremental_repair(repair_args)
            self.assertEqual(retried["promotion"]["metrics"]["duplicate"], 1)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context:active:record_count"), "1")

    def test_missing_record_dead_letters_and_in_place_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:mcp:record_count", "1")

            summary = backfill.run_backfill(self.make_args(path))
            self.assertEqual(summary["metrics"]["failed"], 1)
            self.assertEqual(summary["metrics"]["dead_letter"], 1)
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:dead_letter_count"), "1")

            with self.assertRaises(backfill.BackfillError):
                backfill.run_backfill(self.make_args(path, mode="in_place"))


if __name__ == "__main__":
    unittest.main()