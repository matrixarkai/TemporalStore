#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Unit coverage for MatrixArk context backfill."""

from __future__ import annotations

import argparse
import json
import shutil
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
            "confirm_rollback_noop": "",
            "confirm_rollback_target_state": "",
            "confirm_incremental_repair": "",
            "confirm_active_target": "",
            "confirm_no_active_prefix_precondition": "",
            "confirm_skip_validation": "",
            "confirm_non_strict_validation": "",
            "confirm_unvalidated_target_state": "",
            "confirm_empty_activation": "",
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
            "plan_window_size": 0,
            "plan_max_windows": 128,
            "plan_parallelism": 1,
            "plan_output_dir": "",
            "plan_discover_scan_hash": True,
            "confirm_plan_output_overwrite": "",
            "source_scan_max_empty_shards": 2,
            "dry_run": False,
            "dry_run_check_target": True,
            "resume": True,
            "confirm_resume_range_change": "",
            "fail_fast": False,
            "dead_letter_start": 0,
            "dead_letter_limit": 100,
            "dead_letter_output": "",
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
            manifest = json.loads(backfill.LocalJsonKV(path).hget(summary["manifest_key"], "unit"))
            manifest_payload_hash = manifest.pop("manifest_payload_sha256")
            self.assertEqual(manifest["manifest_schema"], "matrixark_context_backfill_manifest_v1")
            self.assertRegex(manifest_payload_hash, r"^[0-9a-f]{64}$")
            self.assertEqual(summary["manifest_schema"], "matrixark_context_backfill_manifest_v1")
            self.assertEqual(summary["manifest_payload_sha256"], manifest_payload_hash)
            self.assertEqual(backfill.canonical_json_sha256(manifest), manifest_payload_hash)
            verified = backfill.run_verify_manifest(self.make_args(
                path,
                mode="verify_manifest",
                target_prefix="matrixark:context_backfill:test",
                job_id="unit",
                raw_backend="temporalstore",
            ))
            self.assertEqual(verified["status"], "ok")
            self.assertTrue(verified["checks"]["manifest_payload_sha256_match"])
            self.assertEqual(verified["manifest_payload_sha256"], manifest_payload_hash)

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

    def test_local_recovery_report_requires_rebuildable_memory_layers_and_target_parity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            records = [
                {
                    "record_type": "context_event",
                    "event_id_hash": 11,
                    "node_hash": 101,
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "text": "user asked to recover local context memory",
                    "idempotency_key": "event-11",
                },
                {
                    "record_type": "context_entity",
                    "entity_hash": 22,
                    "node_hash": 101,
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "entity_type": "project",
                    "entity_name": "TemporalStore",
                    "state": "Local recovery rebuilds context serving layers from TemporalStore records.",
                    "idempotency_key": "entity-22",
                },
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": 22,
                    "node_hash": 101,
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "vector": [1.0, 0.0],
                    "idempotency_key": "embedding-22",
                },
                {
                    "record_type": "context_index",
                    "index_name": "entity",
                    "index_value": "TemporalStore",
                    "ref_hash": 22,
                    "node_hash": 101,
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "scope_key": "tenant:t/user:u/session:s",
                    "idempotency_key": "index-22",
                },
                {
                    "record_type": "context_summary",
                    "summary_type": "session_l1",
                    "summary_hash": 33,
                    "node_hash": 101,
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "summary_text": "Session summary: local recovery rebuilds context serving layers.",
                    "idempotency_key": "summary-33",
                },
                {
                    "record_type": "context_pack_telemetry",
                    "context_pack_id": "pack-44",
                    "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "selected_ref_count": 2,
                    "used_remote_context_tokens": 12,
                    "remote_context_budget_tokens": 128,
                    "memory_layer_budget": {
                        "by_session_continuity": {"same_session": {"refs": 1, "tokens": 6}},
                    },
                    "idempotency_key": "telemetry-44",
                },
            ]
            for sequence, record in enumerate(records):
                write_sharded(kv, "matrixark:mcp", sequence, record)
            kv.put_string("matrixark:mcp:record_count", str(len(records)))
            built = backfill.run_backfill(self.make_args(
                path,
                target_prefix="matrixark:context_backfill:recovered",
                batch_size=2,
                resume=False,
                dry_run=False,
            ))
            self.assertEqual(built["metrics"]["written"], 6)
            prom = Path(tmp) / "local_recovery.prom"

            report = backfill.run_local_recovery_report(self.make_args(
                path,
                mode="local_recovery_report",
                target_prefix="matrixark:context_backfill:recovered",
                batch_size=2,
                prometheus_output=str(prom),
            ))

            self.assertEqual(report["status"], "ok")
            self.assertTrue(report["ready"])
            self.assertEqual(report["blockers"], [])
            self.assertTrue(report["checks"]["has_context_event"])
            self.assertTrue(report["checks"]["has_context_entity"])
            self.assertTrue(report["checks"]["has_context_embedding"])
            self.assertTrue(report["checks"]["has_context_index"])
            self.assertTrue(report["checks"]["has_context_summary"])
            self.assertTrue(report["checks"]["has_context_pack_telemetry"])
            self.assertTrue(report["checks"]["target_record_count_matches_rebuild"])
            self.assertTrue(report["checks"]["target_fingerprint_matches_rebuild"])
            self.assertEqual(report["serving_layers"]["events"], 1)
            self.assertEqual(report["serving_layers"]["entities"], 1)
            self.assertEqual(report["serving_layers"]["embeddings"], 1)
            self.assertEqual(report["serving_layers"]["secondary_indexes"], 1)
            self.assertEqual(report["serving_layers"]["summaries"], 1)
            self.assertEqual(report["serving_layers"]["retrieval_telemetry"], 1)
            prom_text = prom.read_text(encoding="utf-8")
            self.assertIn("matrixark_context_local_recovery_status", prom_text)
            self.assertIn('status="ok"} 1', prom_text)
            self.assertIn('blocker="none"} 0', prom_text)
            self.assertIn('layer="secondary_indexes"} 1', prom_text)
            self.assertIn('layer="summaries"} 1', prom_text)
            self.assertIn('layer="retrieval_telemetry"} 1', prom_text)

    def test_local_recovery_report_fails_closed_without_secondary_index_summary_or_retrieval_telemetry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            for sequence, record in enumerate([
                {"record_type": "context_event", "event_id_hash": 1, "idempotency_key": "event"},
                {"record_type": "context_entity", "entity_hash": 2, "idempotency_key": "entity"},
                {"record_type": "context_embedding", "ref_hash": 2, "idempotency_key": "embedding"},
            ]):
                write_sharded(kv, "matrixark:mcp", sequence, record)
            kv.put_string("matrixark:mcp:record_count", "3")

            report = backfill.run_local_recovery_report(self.make_args(
                path,
                mode="local_recovery_report",
                target_prefix="",
                batch_size=2,
            ))

            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["ready"])
            self.assertIn("recovery:context_index_missing", report["blockers"])
            self.assertIn("recovery:context_summary_missing", report["blockers"])
            self.assertIn("recovery:context_pack_telemetry_missing", report["blockers"])
            self.assertFalse(report["checks"]["has_context_index"])
            self.assertFalse(report["checks"]["has_context_summary"])
            self.assertFalse(report["checks"]["has_context_pack_telemetry"])
            self.assertIsNone(report["checks"]["target_fingerprint_matches_rebuild"])

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

    def test_scan_hash_uses_numeric_field_order_and_exact_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            shard_key = "matrixark:mcp:records:000000"
            kv.hset(shard_key, "10", json.dumps({"record_type": "context_event", "event_id_hash": 10}))
            kv.hset(shard_key, "2", json.dumps({"record_type": "context_event", "event_id_hash": 2}))
            kv.hset(shard_key, "1", json.dumps({"record_type": "context_event", "event_id_hash": 1}))

            summary = backfill.run_backfill(self.make_args(path, batch_size=8, resume=False))

            self.assertEqual(summary["status"], "ok")
            self.assertEqual(summary["metrics"]["scanned"], 3)
            self.assertEqual(summary["metrics"]["written"], 3)
            self.assertEqual(summary["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(summary["source_range"]["discovered_start_seq"], 1)
            self.assertEqual(summary["source_range"]["discovered_high_watermark_seq"], 10)
            records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")
            self.assertEqual([record["event_id_hash"] for record in records], [1, 2, 10])

    def test_scan_hash_resume_continues_with_exact_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            shard_key = "matrixark:mcp:records:000000"
            kv.hset(shard_key, "1", json.dumps({"record_type": "context_event", "event_id_hash": 1}))
            kv.hset(shard_key, "2", json.dumps({"record_type": "context_event", "event_id_hash": 2}))

            first = backfill.run_backfill(self.make_args(path, batch_size=4, end_seq=3, resume=True))
            self.assertEqual(first["status"], "ok")
            self.assertEqual(first["metrics"]["written"], 2)
            self.assertEqual(first["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(first["source_range"]["discovered_high_watermark_seq"], 2)

            kv = backfill.LocalJsonKV(path)
            kv.hset(shard_key, "10", json.dumps({"record_type": "context_event", "event_id_hash": 10}))

            resumed = backfill.run_backfill(self.make_args(path, batch_size=4, end_seq=None, resume=True))
            self.assertEqual(resumed["status"], "ok")
            self.assertEqual(resumed["resume_state"]["checkpoint_format"], "json")
            self.assertEqual(resumed["resume_state"]["checkpoint_last_sequence"], 2)
            self.assertEqual(resumed["resume_state"]["effective_start_seq"], 3)
            self.assertEqual(resumed["metrics"]["scanned"], 1)
            self.assertEqual(resumed["metrics"]["written"], 1)
            self.assertEqual(resumed["source_range"]["scan_mode"], "scan_hash")
            self.assertEqual(resumed["source_range"]["discovered_start_seq"], 10)
            self.assertEqual(resumed["source_range"]["discovered_high_watermark_seq"], 10)
            records = read_target_records(backfill.LocalJsonKV(path), "matrixark:context_backfill:test")
            self.assertEqual([record["event_id_hash"] for record in records], [1, 2, 10])

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

    def test_plan_discovers_scan_hash_range_for_chunk_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            shard_key = "matrixark:mcp:records:000000"
            kv.hset(shard_key, "10", json.dumps({"record_type": "context_event", "event_id_hash": 10}))
            kv.hset(shard_key, "2", json.dumps({"record_type": "context_event", "event_id_hash": 2}))
            kv.hset(shard_key, "1", json.dumps({"record_type": "context_event", "event_id_hash": 1}))

            plan = backfill.run_plan(self.make_args(path, mode="plan", plan_window_size=5, plan_parallelism=2))

            self.assertEqual(plan["source_range"]["scan_mode"], "scan_hash")
            self.assertTrue(plan["plan_scan_hash_discovery_enabled"])
            self.assertTrue(plan["plan_scan_hash_discovery_used"])
            self.assertEqual(plan["planned_source_records"], 3)
            self.assertEqual(plan["source_range"]["discovered_record_count"], 3)
            self.assertEqual(plan["source_range"]["effective_start_seq"], 1)
            self.assertEqual(plan["source_range"]["effective_end_seq"], 11)
            self.assertEqual(plan["source_range"]["source_high_watermark_seq"], 10)
            self.assertTrue(plan["chunk_plan"]["enabled"])
            self.assertEqual(plan["chunk_plan"]["total_windows"], 2)
            self.assertEqual([(item["start_seq"], item["end_seq"]) for item in plan["chunk_plan"]["windows"]], [(1, 6), (6, 11)])
            self.assertTrue(plan["execution_readiness"]["ready"])
            self.assertEqual(plan["execution_readiness"]["status"], "ready")
            self.assertEqual(plan["execution_readiness"]["blockers"], [])
            self.assertEqual(plan["execution_readiness"]["coverage_start_seq"], 1)
            self.assertEqual(plan["execution_readiness"]["coverage_end_seq"], 11)
            self.assertEqual(plan["execution_readiness"]["coverage_record_count"], 10)
            self.assertEqual(plan["execution_readiness"]["wave_count"], 2)
            self.assertEqual(plan["execution_readiness"]["promotion_step_count"], 2)
            self.assertEqual(plan["execution_readiness"]["requested_plan_parallelism"], 2)
            self.assertEqual(plan["execution_readiness"]["plan_parallelism"], 1)
            self.assertTrue(plan["execution_readiness"]["local_kv_serialized"])
            self.assertTrue(plan["chunk_plan"]["execution_plan"]["local_kv_serialized"])
            prom_path = Path(tmp) / "plan.prom"
            backfill.run_plan(self.make_args(
                path,
                mode="plan",
                plan_window_size=5,
                plan_parallelism=2,
                prometheus_output=str(prom_path),
            ))
            prom_text = prom_path.read_text(encoding="utf-8")
            self.assertIn("matrixark_context_backfill_plan_status", prom_text)
            self.assertIn('scan_mode="scan_hash"} 1', prom_text)
            self.assertIn('field="total_windows"} 2', prom_text)
            self.assertIn("matrixark_context_backfill_plan_execution_readiness_status", prom_text)
            self.assertIn('status="ready"} 1', prom_text)
            self.assertIn('blocker="none"} 0', prom_text)
            self.assertIn('field="coverage_record_count"} 10', prom_text)

            truncated = backfill.run_plan(self.make_args(
                path,
                mode="plan",
                plan_window_size=5,
                plan_parallelism=2,
                plan_max_windows=1,
            ))
            self.assertFalse(truncated["execution_readiness"]["ready"])
            self.assertEqual(truncated["execution_readiness"]["status"], "blocked")
            self.assertIn("chunk_plan_truncated", truncated["execution_readiness"]["blockers"])

            disabled = backfill.run_plan(self.make_args(
                path,
                mode="plan",
                plan_window_size=5,
                plan_discover_scan_hash=False,
            ))
            self.assertFalse(disabled["plan_scan_hash_discovery_enabled"])
            self.assertIsNone(disabled["planned_source_records"])
            self.assertFalse(disabled["chunk_plan"]["enabled"])
            self.assertEqual(disabled["execution_readiness"]["status"], "disabled")
            self.assertIn("chunk_plan_disabled", disabled["execution_readiness"]["blockers"])

            kv.put_string("matrixark:context:active_prefix", "matrixark:context_backfill:test")
            blocked_prom_path = Path(tmp) / "blocked_plan.prom"
            blocked = backfill.run_plan(self.make_args(
                path,
                mode="plan",
                plan_window_size=5,
                target_prefix="matrixark:context_backfill:test",
                prometheus_output=str(blocked_prom_path),
            ))
            self.assertEqual(blocked["status"], "needs_confirmation")
            blocked_prom_text = blocked_prom_path.read_text(encoding="utf-8")
            self.assertIn('status="needs_confirmation"} 1', blocked_prom_text)
            self.assertIn('check="active_target_confirmed_if_needed"} 0', blocked_prom_text)
            self.assertIn('blocker="active_target_confirmed_if_needed"} 1', blocked_prom_text)

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
            backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context:old").append_many([
                {"record_type": "context_event", "event_id_hash": 0}
            ])

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

            with self.assertRaisesRegex(backfill.BackfillError, "requires --expect-active-prefix"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:candidate",
                    confirm_activate="YES",
                    dry_run=False,
                ))

            activation_prom = Path(tmp) / "activation.prom"
            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                confirm_activate="YES",
                expect_active_prefix="matrixark:context:old",
                prometheus_output=str(activation_prom),
                dry_run=False,
            ))
            self.assertEqual(activated["status"], "ok")
            self.assertEqual(activated["validation_status"], "ok")
            self.assertFalse(activated["validation_skipped"])
            self.assertEqual(activated["validation_skip_reason"], "")
            self.assertEqual(activated["validation_source_range"]["source_high_watermark_seq"], 1)
            self.assertEqual(activated["validation_target_state"]["record_count"], 2)
            self.assertFalse(activated["active_prefix_precondition_bypassed"])
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context_backfill:candidate")
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix:previous:unit"), "matrixark:context:old")
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertEqual(activation_audit["validation_status"], "ok")
            self.assertFalse(activation_audit["validation_skipped"])
            self.assertEqual(activation_audit["validation_skip_reason"], "")
            self.assertTrue(activation_audit["validation_strict"])
            self.assertFalse(activation_audit["non_strict_validation_confirmed"])
            self.assertFalse(activation_audit["empty_activation_confirmed"])
            self.assertFalse(activation_audit["active_prefix_precondition_bypassed"])
            self.assertEqual(activation_audit["validation_target_state"]["record_count"], 2)
            self.assertIn("matrixark:context_backfill:candidate", json.dumps(activation_audit, sort_keys=True))
            activation_prom_text = activation_prom.read_text()
            self.assertIn("matrixark_context_backfill_activation_status", activation_prom_text)
            self.assertIn("matrixark_context_backfill_activation_validation_status", activation_prom_text)
            self.assertIn("matrixark_context_backfill_activation_guard_status", activation_prom_text)
            self.assertIn("matrixark_context_backfill_activation_target_records", activation_prom_text)
            self.assertIn('status="ok"', activation_prom_text)
            self.assertIn('skipped="false"', activation_prom_text)
            self.assertIn('kind="record_count"} 2', activation_prom_text)

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

            rollback_prom = Path(tmp) / "rollback.prom"
            rollback = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                job_id="rollback-unit",
                confirm_rollback="YES",
                expect_active_prefix="matrixark:context_backfill:candidate",
                prometheus_output=str(rollback_prom),
                dry_run=False,
            ))
            self.assertEqual(rollback["status"], "ok")
            self.assertEqual(rollback["expected_active_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(rollback["from_prefix"], "matrixark:context_backfill:candidate")
            self.assertEqual(rollback["to_prefix"], "matrixark:context:old")
            self.assertTrue(rollback["rollback_target_state"]["healthy_for_rollback"])
            self.assertFalse(rollback["rollback_target_state_confirmed"])
            kv_rolled_back = backfill.LocalJsonKV(path)
            self.assertEqual(kv_rolled_back.get_string("matrixark:context:active_prefix"), "matrixark:context:old")
            self.assertIn("matrixark:context_backfill:candidate", kv_rolled_back.hget("matrixark:context:active_prefix:rollback_audit", "rollback-unit"))
            rollback_prom_text = rollback_prom.read_text()
            self.assertIn("matrixark_context_backfill_rollback_status", rollback_prom_text)
            self.assertIn("matrixark_context_backfill_rollback_guard_status", rollback_prom_text)
            self.assertIn("matrixark_context_backfill_rollback_target_records", rollback_prom_text)
            self.assertIn("matrixark_context_backfill_rollback_target_health", rollback_prom_text)
            self.assertIn('status="ok"', rollback_prom_text)
            self.assertIn('kind="record_count"} 1', rollback_prom_text)

    def test_rollback_activation_unhealthy_target_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:context:active_prefix", "matrixark:context_backfill:candidate")
            kv.put_string("matrixark:context:active_prefix:previous:unit", "matrixark:context:old")

            with self.assertRaisesRegex(backfill.BackfillError, "previous prefix is empty or unhealthy"):
                backfill.run_rollback_activation(self.make_args(
                    path,
                    mode="rollback_activation",
                    rollback_job_id="unit",
                    job_id="rollback-unhealthy",
                    confirm_rollback="YES",
                    expect_active_prefix="matrixark:context_backfill:candidate",
                    dry_run=False,
                ))

            rollback = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                job_id="rollback-unhealthy",
                confirm_rollback="YES",
                confirm_rollback_target_state="YES",
                expect_active_prefix="matrixark:context_backfill:candidate",
                dry_run=False,
            ))

            self.assertEqual(rollback["status"], "ok")
            self.assertFalse(rollback["rollback_target_state"]["healthy_for_rollback"])
            self.assertEqual(rollback["rollback_target_state"]["record_count"], 0)
            self.assertTrue(rollback["rollback_target_state_confirmed"])
            kv_after = backfill.LocalJsonKV(path)
            rollback_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:rollback_audit", "rollback-unhealthy"))
            self.assertTrue(rollback_audit["rollback_target_state_confirmed"])
            self.assertFalse(rollback_audit["rollback_target_state"]["healthy_for_rollback"])

    def test_rollback_activation_noop_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:active")
            kv.put_string("matrixark:context:active_prefix:previous:unit", "matrixark:context:active")
            backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context:active").append_many([
                {"record_type": "context_event", "event_id_hash": 1}
            ])

            with self.assertRaisesRegex(backfill.BackfillError, "previous prefix equals current active prefix"):
                backfill.run_rollback_activation(self.make_args(
                    path,
                    mode="rollback_activation",
                    rollback_job_id="unit",
                    job_id="rollback-noop",
                    confirm_rollback="YES",
                    expect_active_prefix="matrixark:context:active",
                    dry_run=False,
                ))

            rollback = backfill.run_rollback_activation(self.make_args(
                path,
                mode="rollback_activation",
                rollback_job_id="unit",
                job_id="rollback-noop",
                confirm_rollback="YES",
                confirm_rollback_noop="YES",
                expect_active_prefix="matrixark:context:active",
                dry_run=False,
            ))

            self.assertEqual(rollback["status"], "ok")
            self.assertTrue(rollback["rollback_noop_confirmed"])
            kv_after = backfill.LocalJsonKV(path)
            rollback_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:rollback_audit", "rollback-noop"))
            self.assertTrue(rollback_audit["rollback_noop_confirmed"])

    def test_activate_shadow_requires_confirmation_for_empty_validated_shadow(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            kv.put_string("matrixark:mcp:record_count", "0")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:old")
            backfill.MatrixKVBackfillTarget(kv, prefix="matrixark:context:old").append_many([
                {"record_type": "context_event", "event_id_hash": 0}
            ])

            validation = backfill.run_validate_shadow(self.make_args(
                path,
                mode="validate_shadow",
                target_prefix="matrixark:context_backfill:empty",
            ))
            self.assertEqual(validation["status"], "ok")
            self.assertEqual(validation["expected_records"], 0)
            self.assertEqual(validation["actual_records"], 0)

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-empty-activation=YES"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:empty",
                    confirm_activate="YES",
                    expect_active_prefix="matrixark:context:old",
                    dry_run=False,
                ))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:empty",
                confirm_activate="YES",
                confirm_empty_activation="YES",
                expect_active_prefix="matrixark:context:old",
                dry_run=False,
            ))

            self.assertEqual(activated["status"], "ok")
            self.assertTrue(activated["empty_activation_confirmed"])
            kv_after = backfill.LocalJsonKV(path)
            self.assertEqual(kv_after.get_string("matrixark:context:active_prefix"), "matrixark:context_backfill:empty")
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertTrue(activation_audit["empty_activation_confirmed"])
            self.assertEqual(activation_audit["validation_target_state"]["record_count"], 0)

    def test_activate_shadow_without_active_precondition_requires_explicit_bypass(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 1})
            kv.put_string("matrixark:mcp:record_count", "1")
            kv.put_string("matrixark:context:active_prefix", "matrixark:context:old")
            shadow = backfill.run_backfill(self.make_args(
                path,
                target_prefix="matrixark:context_backfill:bypass",
                resume=False,
            ))
            self.assertEqual(shadow["metrics"]["written"], 1)

            with self.assertRaisesRegex(backfill.BackfillError, "requires --expect-active-prefix"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:bypass",
                    confirm_activate="YES",
                    dry_run=False,
                ))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:bypass",
                confirm_activate="YES",
                confirm_no_active_prefix_precondition="YES",
                dry_run=False,
            ))

            self.assertEqual(activated["status"], "ok")
            self.assertTrue(activated["active_prefix_precondition_bypassed"])
            kv_after = backfill.LocalJsonKV(path)
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertTrue(activation_audit["active_prefix_precondition_bypassed"])

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

            with self.assertRaisesRegex(backfill.BackfillError, "empty or unhealthy target prefix"):
                backfill.run_activate_shadow(self.make_args(
                    path,
                    mode="activate_shadow",
                    target_prefix="matrixark:context_backfill:candidate",
                    confirm_activate="YES",
                    confirm_skip_validation="YES",
                    expect_active_prefix="matrixark:context:old",
                    skip_validation=True,
                    dry_run=False,
                ))

            activated = backfill.run_activate_shadow(self.make_args(
                path,
                mode="activate_shadow",
                target_prefix="matrixark:context_backfill:candidate",
                confirm_activate="YES",
                confirm_skip_validation="YES",
                confirm_unvalidated_target_state="YES",
                expect_active_prefix="matrixark:context:old",
                skip_validation=True,
                dry_run=False,
            ))

            self.assertEqual(activated["status"], "ok")
            self.assertIsNone(activated["validation"])
            self.assertEqual(activated["validation_status"], "skipped")
            self.assertTrue(activated["validation_skipped"])
            self.assertEqual(activated["validation_skip_reason"], "skip_validation_flag")
            self.assertEqual(activated["validation_source_range"], {})
            self.assertEqual(activated["validation_target_state"]["record_count"], 0)
            self.assertFalse(activated["validation_target_state"]["healthy_for_unvalidated_activation"])
            self.assertTrue(activated["unvalidated_target_state_confirmed"])
            self.assertFalse(activated["active_prefix_precondition_bypassed"])

            kv_after = backfill.LocalJsonKV(path)
            activation_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:audit", "unit"))
            self.assertIsNone(activation_audit["validation"])
            self.assertEqual(activation_audit["validation_status"], "skipped")
            self.assertTrue(activation_audit["validation_skipped"])
            self.assertEqual(activation_audit["validation_skip_reason"], "skip_validation_flag")
            self.assertTrue(activation_audit["validation_strict"])
            self.assertFalse(activation_audit["non_strict_validation_confirmed"])
            self.assertTrue(activation_audit["unvalidated_target_state_confirmed"])
            self.assertEqual(activation_audit["validation_source_range"], {})
            self.assertEqual(activation_audit["validation_target_state"]["record_count"], 0)
            self.assertFalse(activation_audit["validation_target_state"]["healthy_for_unvalidated_activation"])

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
                expect_active_prefix="matrixark:context:old",
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
                expect_active_prefix="matrixark:context:active",
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
            self.assertEqual(repaired["promotion_manifest_verification"]["status"], "ok")
            self.assertFalse(repaired["promotion_manifest_verification"]["skipped"])
            self.assertTrue(repaired["promotion_manifest_verification"]["checks"]["manifest_payload_sha256_match"])
            self.assertEqual(repaired["promotion_manifest_verification"]["target_prefix"], "matrixark:context:active")
            self.assertEqual(repaired["promotion_manifest_verification"]["job_id"], "unit:active")

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
            self.assertEqual(repair_audit["promotion_manifest_verification"]["status"], "ok")
            self.assertTrue(repair_audit["promotion_manifest_verification"]["checks"]["manifest_payload_sha256_match"])
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
                expect_active_prefix="matrixark:context:active",
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
            self.assertIn("matrixark_context_backfill_incremental_repair_promotion_manifest_status", prom_text)
            self.assertIn('status="ok",skipped="false"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_incremental_repair_promotion_manifest_check", prom_text)
            self.assertIn('check="manifest_payload_sha256_match"} 1', prom_text)

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
                expect_active_prefix="matrixark:context:active",
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

            with self.assertRaisesRegex(backfill.BackfillError, "confirm-unvalidated-target-state=YES"):
                backfill.run_incremental_repair(self.make_args(
                    path,
                    mode="incremental_repair",
                    target_prefix="matrixark:context_repair:skip",
                    start_seq=0,
                    end_seq=1,
                    confirm_incremental_repair="YES",
                    confirm_skip_validation="YES",
                    skip_validation=True,
                    expect_active_prefix="matrixark:context:active",
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
                confirm_unvalidated_target_state="YES",
                skip_validation=True,
                expect_active_prefix="matrixark:context:active",
                resume=False,
            ))

            self.assertEqual(repaired["status"], "ok")
            self.assertIsNone(repaired["validation"])
            self.assertEqual(repaired["validation_status"], "skipped")
            self.assertTrue(repaired["validation_skipped"])
            self.assertEqual(repaired["validation_skip_reason"], "skip_validation_flag")
            self.assertEqual(repaired["validation_source_range"], {})
            self.assertEqual(repaired["validation_target_state"]["record_count"], 0)
            self.assertFalse(repaired["validation_target_state"]["healthy_for_unvalidated_activation"])
            self.assertEqual(repaired["promotion"]["metrics"]["written"], 1)
            self.assertTrue(repaired["unvalidated_target_state_confirmed"])
            self.assertFalse(repaired["active_prefix_precondition_bypassed"])

            kv_after = backfill.LocalJsonKV(path)
            repair_audit = json.loads(kv_after.hget("matrixark:context:active_prefix:incremental_repair_audit", "unit"))
            self.assertIsNone(repair_audit["validation"])
            self.assertEqual(repair_audit["validation_status"], "skipped")
            self.assertTrue(repair_audit["validation_skipped"])
            self.assertEqual(repair_audit["validation_skip_reason"], "skip_validation_flag")
            self.assertTrue(repair_audit["validation_strict"])
            self.assertFalse(repair_audit["non_strict_validation_confirmed"])
            self.assertTrue(repair_audit["unvalidated_target_state_confirmed"])
            self.assertFalse(repair_audit["active_prefix_precondition_bypassed"])
            self.assertEqual(repair_audit["validation_source_range"], {})
            self.assertEqual(repair_audit["validation_target_state"]["record_count"], 0)
            self.assertFalse(repair_audit["validation_target_state"]["healthy_for_unvalidated_activation"])
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
                expect_active_prefix="matrixark:context:active",
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


    def test_raw_message_store_reader_reads_temporalstore_and_matrixkv_with_same_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            raw_record = {
                "record_type": "context_event",
                "event_id_hash": 42,
                "event_time_ms": 1781777200000,
                "body": "same general raw event API",
                "text": "same general raw event API",
            }
            write_sharded(kv, "matrixark:mcp", 0, raw_record)
            kv.put_string("matrixark:mcp:record_count", "1")

            reports = []
            for backend in ["temporalstore", "matrixkv"]:
                reader = backfill.make_raw_message_reader(
                    kv,
                    prefix="matrixark:mcp",
                    raw_backend=backend,
                )
                self.assertEqual(reader.count(), 1)
                self.assertEqual(reader.source_range(start_seq=0, end_seq=None)["raw_backend"], backend)
                event = reader.read_raw_event(0)
                reports.append(event["storage_contract"])
                self.assertEqual(event["backend"], backend)
                self.assertEqual(event["record"]["body"], "same general raw event API")
                self.assertEqual(event["storage_contract"]["timestamp_key_ms"], 1781777200000)
                self.assertEqual(event["storage_contract"]["event_key_hash"], 42)
                self.assertTrue(event["storage_contract"]["uses_timestamp_and_event_key"])

                cli_event = backfill.run_read_raw_event(self.make_args(
                    path,
                    mode="read_raw_event",
                    raw_backend=backend,
                    read_seq=0,
                ))
                self.assertEqual(cli_event["mode"], "read_raw_event")
                self.assertEqual(cli_event["backend"], backend)
                self.assertEqual(cli_event["raw_store_reader"], "matrixark.raw_message_store_reader.v1")
                self.assertEqual(cli_event["record"], event["record"])
                self.assertEqual(cli_event["storage_contract"], event["storage_contract"])

            self.assertEqual(reports[0]["stored_value"], reports[1]["stored_value"])
            self.assertEqual(reports[0]["timeline_key"], reports[1]["timeline_key"])
            self.assertEqual(reports[0]["target"]["backend"], "temporalstore")
            self.assertEqual(reports[1]["target"]["backend"], "matrixkv")

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
            manifest_payload_hash = manifest.pop("manifest_payload_sha256")
            self.assertEqual(manifest["manifest_schema"], "matrixark_context_backfill_manifest_v1")
            self.assertRegex(manifest_payload_hash, r"^[0-9a-f]{64}$")
            self.assertEqual(matrixkv["manifest_payload_sha256"], manifest_payload_hash)
            self.assertEqual(backfill.canonical_json_sha256(manifest), manifest_payload_hash)
            record = read_target_records(kv_after, "shadow:matrixkv")[0]
            self.assertEqual(record["backfill"]["raw_backend"], "matrixkv")

    def test_verify_manifest_rejects_tampered_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kv.json"
            kv = backfill.LocalJsonKV(path)
            write_sharded(kv, "matrixark:mcp", 0, {"record_type": "context_event", "event_id_hash": 17, "text": "manifest"})
            kv.put_string("matrixark:mcp:record_count", "1")

            summary = backfill.run_backfill(self.make_args(path, target_prefix="shadow:manifest", job_id="manifest-unit", resume=False))
            verify_prom = Path(tmp) / "verify_manifest.prom"
            verified = backfill.run_verify_manifest(self.make_args(
                path,
                mode="verify_manifest",
                target_prefix="shadow:manifest",
                job_id="manifest-unit",
                prometheus_output=str(verify_prom),
            ))
            self.assertEqual(verified["status"], "ok")
            self.assertEqual(verified["manifest_payload_sha256"], summary["manifest_payload_sha256"])
            verify_prom_text = verify_prom.read_text(encoding="utf-8")
            self.assertIn("matrixark_context_backfill_manifest_verification_status", verify_prom_text)
            self.assertIn('status="ok"', verify_prom_text)
            self.assertIn('check="manifest_payload_sha256_match"} 1', verify_prom_text)

            kv_after = backfill.LocalJsonKV(path)
            manifest = json.loads(kv_after.hget("shadow:manifest:backfill_manifest", "manifest-unit"))
            manifest["source_range"]["source_record_count"] = 99
            kv_after.hset("shadow:manifest:backfill_manifest", "manifest-unit", json.dumps(manifest, sort_keys=True, separators=(",", ":")))

            tampered_prom = Path(tmp) / "verify_manifest_tampered.prom"
            tampered = backfill.run_verify_manifest(self.make_args(
                path,
                mode="verify_manifest",
                target_prefix="shadow:manifest",
                job_id="manifest-unit",
                prometheus_output=str(tampered_prom),
            ))
            self.assertEqual(tampered["status"], "failed")
            self.assertFalse(tampered["checks"]["manifest_payload_sha256_match"])
            tampered_prom_text = tampered_prom.read_text(encoding="utf-8")
            self.assertIn('status="failed"', tampered_prom_text)
            self.assertIn('check="manifest_payload_sha256_match"} 0', tampered_prom_text)

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
                expect_active_prefix="matrixark:context:active",
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
            export_path = Path(tmp) / "dead_letters.jsonl"
            prom_path = Path(tmp) / "dead_letters.prom"
            exported = backfill.run_export_dead_letters(self.make_args(
                path,
                mode="export_dead_letters",
                target_prefix="matrixark:context_backfill:test",
                dead_letter_start=0,
                dead_letter_limit=1,
                dead_letter_output=str(export_path),
                prometheus_output=str(prom_path),
            ))
            self.assertEqual(exported["status"], "ok")
            self.assertEqual(exported["dead_letter_total"], 1)
            self.assertEqual(exported["exported_count"], 1)
            self.assertFalse(exported["has_more"])
            self.assertRegex(exported["dead_letter_fingerprint"], r"^[0-9a-f]{64}$")
            self.assertEqual(exported["dead_letters"][0]["dead_letter_sequence"], 0)
            self.assertIn("missing sharded record", exported["dead_letters"][0]["error"])
            exported_rows = [json.loads(line) for line in export_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(exported_rows, exported["dead_letters"])
            prom_text = prom_path.read_text(encoding="utf-8")
            self.assertIn("matrixark_context_backfill_dead_letter_export_status", prom_text)
            self.assertIn('kind="total"} 1', prom_text)
            self.assertIn('kind="exported"} 1', prom_text)
            self.assertIn("matrixark_context_backfill_dead_letter_export_fingerprint_info", prom_text)
            plan = backfill.run_plan(self.make_args(path, mode="plan", target_prefix="matrixark:context_backfill:test"))
            self.assertTrue(plan["target_state"]["dead_letter_export_recommended"])
            self.assertIn("--mode=export_dead_letters", plan["target_state"]["dead_letter_export_command_args"])
            self.assertIn("--target-prefix=matrixark:context_backfill:test", plan["target_state"]["dead_letter_export_command_args"])

            with self.assertRaises(backfill.BackfillError):
                backfill.run_backfill(self.make_args(path, mode="in_place"))

    def test_plan_artifact_manifest_verifies_after_bundle_restore(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "plan_bundle"
            restored_dir = Path(tmp) / "restored_bundle"
            args = argparse.Namespace(
                plan_output_dir=str(output_dir),
                job_id="portable-plan",
                confirm_plan_output_overwrite="",
            )
            summary = {
                "status": "ok",
                "chunk_plan": {
                    "execution_plan": {
                        "shadow_validation_waves": [
                            {
                                "wave": 0,
                                "shadow_command_args": [["--mode=shadow", "--job-id=portable-plan"]],
                                "validate_command_args": [["--mode=validate_shadow", "--job-id=portable-plan"]],
                            }
                        ],
                        "promotion_sequence": [
                            {
                                "incremental_repair_command_args": [
                                    "--mode=incremental_repair",
                                    "--job-id=portable-plan",
                                ]
                            }
                        ],
                    }
                },
            }

            artifacts = backfill.write_plan_artifacts(args, summary)
            manifest = json.loads(Path(artifacts["artifact_manifest"]).read_text(encoding="utf-8"))
            self.assertTrue(all(item.get("relative_path") for item in manifest["files"]))
            shadow_script = (output_dir / "shadow_wave_0000.sh").read_text(encoding="utf-8")
            self.assertIn('PLAN_BUNDLE_DIR=', shadow_script)
            self.assertIn('execution_evidence/shadow_wave_0000_cmd_0000.json', shadow_script)
            self.assertIn('execution_evidence/shadow_wave_0000_cmd_0000.prom', shadow_script)
            self.assertIn('execution_evidence/shadow_wave_0000_cmd_0000.stderr.log', shadow_script)

            shutil.copytree(output_dir, restored_dir)
            restored_manifest_path = restored_dir / "artifact_manifest.json"
            restored_manifest = json.loads(restored_manifest_path.read_text(encoding="utf-8"))
            for item in restored_manifest["files"]:
                item["path"] = f"/missing/original/{item['relative_path']}"
            restored_manifest_path.write_text(json.dumps(restored_manifest, sort_keys=True, indent=2), encoding="utf-8")

            verified = backfill.run_verify_plan_artifacts(argparse.Namespace(
                plan_output_dir=str(restored_dir),
                job_id="portable-plan",
            ))
            self.assertEqual(verified["status"], "ok")
            self.assertTrue(all(item["path_source"] == "relative_path" for item in verified["file_checks"]))
            self.assertTrue(verified["checks"]["generated_scripts_match_plan"])

            tampered_script = restored_dir / "shadow_wave_0000.sh"
            tampered_script.write_text(
                tampered_script.read_text(encoding="utf-8").replace("--mode=shadow", "--mode=validate_shadow"),
                encoding="utf-8",
            )
            restored_manifest = json.loads(restored_manifest_path.read_text(encoding="utf-8"))
            for item in restored_manifest["files"]:
                if item["relative_path"] == "shadow_wave_0000.sh":
                    item.update(backfill._artifact_file_info(tampered_script, output_dir=restored_dir))
            restored_manifest_path.write_text(json.dumps(restored_manifest, sort_keys=True, indent=2), encoding="utf-8")
            semantically_tampered = backfill.run_verify_plan_artifacts(argparse.Namespace(
                plan_output_dir=str(restored_dir),
                job_id="portable-plan",
            ))
            self.assertEqual(semantically_tampered["status"], "failed")
            self.assertTrue(semantically_tampered["checks"]["all_file_sha256_match"])
            self.assertFalse(semantically_tampered["checks"]["generated_scripts_match_plan"])

            restored_manifest["files"][0]["relative_path"] = "../plan.json"
            restored_manifest_path.write_text(json.dumps(restored_manifest, sort_keys=True, indent=2), encoding="utf-8")
            unsafe = backfill.run_verify_plan_artifacts(argparse.Namespace(
                plan_output_dir=str(restored_dir),
                job_id="portable-plan",
            ))
            self.assertEqual(unsafe["status"], "failed")
            self.assertFalse(unsafe["checks"]["all_paths_safe"])

            prom_path = Path(tmp) / "plan_artifacts.prom"
            backfill.run_verify_plan_artifacts(argparse.Namespace(
                plan_output_dir=str(restored_dir),
                job_id="portable-plan",
                prometheus_output=str(prom_path),
            ))
            prom_text = prom_path.read_text(encoding="utf-8")
            self.assertIn("matrixark_context_backfill_plan_artifact_verification_status", prom_text)
            self.assertIn('check="generated_scripts_match_plan"} 0', prom_text)
            self.assertIn('file="shadow_wave_0000.sh",check="matches_plan"} 0', prom_text)



class TheConnectionDefaultsAddressTheDeployment(unittest.TestCase):
    """Run with no flags, this tool must address the store the deployment runs.

    All three of its connection defaults pointed elsewhere. The namespace and table defaulted to
    "matrixark" and "context" while config/temporalstore.toml declares deploy_ns and deploy_table
    for those exact variables, and nothing else in the repository names either value. The
    metaserver was read only as MATRIXARK_METASERVER -- a spelling nothing sets -- and defaulted to
    127.0.0.1:65000, a port no deployment listens on.

    A backfill that addresses an empty namespace does not fail. It writes, reports what it wrote,
    and the deployment reads none of it, which is why these are asserted rather than left to be
    noticed.
    """

    def _defaults(self, env: dict) -> argparse.Namespace:
        import os

        keys = ("MATRIXARK_TEMPORALSTORE_METASERVER", "MATRIXARK_METASERVER",
                "MATRIXARK_NAMESPACE", "MATRIXARK_TABLE")
        saved = {key: os.environ.get(key) for key in keys}
        for key in keys:
            os.environ.pop(key, None)
        os.environ.update(env)
        try:
            return backfill.build_parser().parse_args([])
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_the_namespace_and_table_are_the_ones_the_config_declares(self) -> None:
        args = self._defaults({})
        self.assertEqual("deploy_ns", args.namespace)
        self.assertEqual("deploy_table", args.table)

    def test_the_documented_metaserver_spelling_is_honoured(self) -> None:
        """The one the config, the compose file and the deploy documents all use."""
        args = self._defaults({"MATRIXARK_TEMPORALSTORE_METASERVER": "local"})
        self.assertEqual("local", args.metaserver,
                         "the tool ignored the variable the running processes carry")

    def test_the_older_spelling_is_still_accepted(self) -> None:
        """Accepting an old name costs nothing; refusing it breaks whoever set it."""
        args = self._defaults({"MATRIXARK_METASERVER": "127.0.0.1:19000"})
        self.assertEqual("127.0.0.1:19000", args.metaserver)

    def test_the_documented_spelling_wins_when_both_are_set(self) -> None:
        args = self._defaults({"MATRIXARK_TEMPORALSTORE_METASERVER": "local",
                               "MATRIXARK_METASERVER": "127.0.0.1:19000"})
        self.assertEqual("local", args.metaserver)

    def test_unset_falls_back_to_what_the_config_declares(self) -> None:
        self.assertEqual("127.0.0.1:18000", self._defaults({}).metaserver)


class TheConnectionDefaultsAddressTheDeployment(unittest.TestCase):
    """Run with no flags, this tool must address the store the deployment runs.

    All three of its connection defaults pointed elsewhere. The namespace and table defaulted to
    "matrixark" and "context" while config/temporalstore.toml declares deploy_ns and deploy_table
    for those exact variables, and nothing else in the repository names either value. The
    metaserver was read only as MATRIXARK_METASERVER -- a spelling nothing sets -- and defaulted to
    127.0.0.1:65000, a port no deployment listens on.

    A backfill that addresses an empty namespace does not fail. It writes, reports what it wrote,
    and the deployment reads none of it, which is why these are asserted rather than left to be
    noticed.
    """

    def _defaults(self, env: dict) -> argparse.Namespace:
        import os

        keys = ("MATRIXARK_TEMPORALSTORE_METASERVER", "MATRIXARK_METASERVER",
                "MATRIXARK_NAMESPACE", "MATRIXARK_TABLE")
        saved = {key: os.environ.get(key) for key in keys}
        for key in keys:
            os.environ.pop(key, None)
        os.environ.update(env)
        try:
            return backfill.build_parser().parse_args([])
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_the_namespace_and_table_are_the_ones_the_config_declares(self) -> None:
        args = self._defaults({})
        self.assertEqual("deploy_ns", args.namespace)
        self.assertEqual("deploy_table", args.table)

    def test_the_documented_metaserver_spelling_is_honoured(self) -> None:
        """The one the config, the compose file and the deploy documents all use."""
        args = self._defaults({"MATRIXARK_TEMPORALSTORE_METASERVER": "local"})
        self.assertEqual("local", args.metaserver,
                         "the tool ignored the variable the running processes carry")

    def test_the_older_spelling_is_still_accepted(self) -> None:
        """Accepting an old name costs nothing; refusing it breaks whoever set it."""
        args = self._defaults({"MATRIXARK_METASERVER": "127.0.0.1:19000"})
        self.assertEqual("127.0.0.1:19000", args.metaserver)

    def test_the_documented_spelling_wins_when_both_are_set(self) -> None:
        args = self._defaults({"MATRIXARK_TEMPORALSTORE_METASERVER": "local",
                               "MATRIXARK_METASERVER": "127.0.0.1:19000"})
        self.assertEqual("local", args.metaserver)

    def test_unset_falls_back_to_what_the_config_declares(self) -> None:
        self.assertEqual("127.0.0.1:18000", self._defaults({}).metaserver)

if __name__ == "__main__":
    unittest.main()
