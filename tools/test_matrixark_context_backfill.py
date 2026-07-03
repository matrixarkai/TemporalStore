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
            "raw_backend": "temporalstore",
            "target_prefix": "matrixark:context_backfill:test",
            "mode": "shadow",
            "confirm_in_place": "",
            "confirm_activate": "",
            "confirm_rollback": "",
            "confirm_incremental_repair": "",
            "active_prefix_key": "matrixark:context:active_prefix",
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
            self.assertEqual(summary["resume_state"]["checkpoint_format"], "missing")
            self.assertFalse(summary["resume_state"]["checkpoint_found"])
            self.assertEqual(summary["resume_state"]["effective_start_seq"], 0)
            prom_text = prom.read_text()
            self.assertIn("matrixark_context_backfill_records_total", prom_text)
            self.assertIn("matrixark_context_backfill_batches_total", prom_text)
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
            self.assertEqual(checkpoint["metrics"]["written"], 2)

            resumed = backfill.run_backfill(self.make_args(path, prometheus_output=str(prom)))
            self.assertEqual(resumed["metrics"]["scanned"], 0)
            self.assertEqual(resumed["resume_state"]["checkpoint_format"], "json")
            self.assertEqual(resumed["resume_state"]["checkpoint_last_sequence"], 1)
            self.assertEqual(resumed["resume_state"]["effective_start_seq"], 2)

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

            target.append_many([
                {"record_type": "context_event", "event_id_hash": 1, "idempotency_key": "already"},
                {"record_type": "context_event", "event_id_hash": 2, "idempotency_key": "new"},
                {"record_type": "context_debug_record", "ref_hash": 2, "idempotency_key": "new"},
                {"record_type": "context_event", "event_id_hash": 4, "idempotency_key": "other"},
            ])

            self.assertEqual(kv.batch_hget_calls, 1)
            self.assertEqual(kv.get_string("shadow:dedupe:record_count"), "3")
            records = read_target_records(backfill.LocalJsonKV(path), "shadow:dedupe")
            self.assertEqual([record["record_type"] for record in records], ["context_event", "context_debug_record", "context_event"])
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.hget("shadow:dedupe:idempotency", "new"), "1")
            self.assertEqual(kv_after.hget("shadow:dedupe:idempotency", "other"), "2")


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
            self.assertEqual(validation["expected_type_counts"], {"context_event": 2})
            self.assertEqual(validation["actual_type_counts"], {"context_event": 2})
            self.assertTrue(validation["checks"]["exact_serving_type_counts_match"])

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

            rollback = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                job_id="rollback-unit",
                confirm_rollback="YES",
                dry_run=False,
            ))
            self.assertEqual(rollback["status"], "ok")
            self.assertEqual(rollback["from_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(rollback["to_prefix"], "matrixark:context:old")
            kv_rolled_back = backfill.LocalJsonKV(path)
            self.assertEqual(kv_rolled_back.get_string("matrixark:context:active_prefix"), "matrixark:context:old")
            self.assertIn("matrixark:context_backfill:candidate", kv_rolled_back.hget("matrixark:context:active_prefix:rollback_audit", "rollback-unit"))


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
            self.assertEqual(validation["expected_type_counts"], {"context_event": 1})
            self.assertEqual(validation["actual_type_counts"], {"context_summary": 1})
            self.assertFalse(validation["checks"]["exact_serving_type_counts_match"])


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
            self.assertEqual(backfill.LocalJsonKV(path).get_string("matrixark:context_backfill:test:dead_letter_count"), "1")

            with self.assertRaises(backfill.BackfillError):
                backfill.run_backfill(self.make_args(path, mode="in_place"))


if __name__ == "__main__":
    unittest.main()
