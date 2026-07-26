#!/usr/bin/env python3
"""Recovery-report tests for MatrixArk local serving models."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_latest_values import compact_latest_value_records
from matrixark_mcp_recovery import load_jsonl_records_for_recovery, matrixark_local_recovery_report


class MatrixArkMcpRecoveryTest(unittest.TestCase):
    def test_recovery_report_proves_hot_serving_models_are_rebuildable(self) -> None:
        records = [
            {
                "record_type": "agent_message",
                "session_id": "codex:session-a",
                "messages": [{"role": "user", "content": "Use Ubuntu shared repos."}],
            },
            {
                "record_type": "context_event",
                "event_id_hash": 101,
                "node_hash": 10,
                "node_path": ["tenant:t", "user:u", "session:codex:session-a", "conversation:codex_hook"],
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-a"},
                "text": "user: Use Ubuntu shared repos.",
                "updated_at_ms": 100,
            },
            {
                "record_type": "context_entity",
                "entity_hash": 201,
                "node_hash": 10,
                "node_path": ["tenant:t", "user:u", "session:codex:session-a"],
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-a"},
                "access_scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-a"},
                "entity_type": "preference",
                "entity_name": "repo location",
                "state": "Use Ubuntu shared repos.",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "updated_at_ms": 100,
            },
            {
                "record_type": "context_entity",
                "entity_hash": 301,
                "node_hash": 30,
                "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u"},
                "access_scope": {"account_id": "a", "tenant_id": "t", "user_id": "u"},
                "entity_type": "preference",
                "entity_name": "repo location",
                "state": "Use /root/src/github-services in Ubuntu.",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_session_ids": ["codex:session-a", "codex:session-b"],
                "updated_at_ms": 200,
            },
            {"record_type": "context_embedding", "embedding_type": "entity_state", "ref_type": "entity", "ref_hash": 301},
            {"record_type": "context_index", "index_name": "memory_scope:user_profile", "data_model": "context_profile_entity", "ref_hashes": [301]},
            {"record_type": "context_summary_dirty", "dirty_hash": 401, "status": "dirty", "updated_at_ms": 200},
            {"record_type": "context_summary", "summary_type": "batch_l0", "summary_hash": 501, "summary_text": "Repo location preference."},
        ]

        report = matrixark_local_recovery_report(
            records,
            scope={"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-c"},
        )

        self.assertEqual("ok", report["status"])
        self.assertTrue(report["hot_memory_persisted"])
        self.assertTrue(report["cache_rebuild"]["read_cache_rebuildable_from_durable_log"])
        self.assertTrue(report["cache_rebuild"]["retrieval_cache_rebuildable_from_hot_records"])
        self.assertEqual(1, report["memory_hierarchy"]["session_entity_count"])
        self.assertEqual(1, report["memory_hierarchy"]["profile_entity_count"])
        self.assertEqual(["codex:session-a", "codex:session-b"], report["memory_hierarchy"]["source_session_ids"])
        self.assertEqual(1, report["derived_views"]["index_posting_count"])
        self.assertEqual(1, report["derived_views"]["dirty_summary_count"])
        self.assertEqual("ok", report["retrieval_smoke"]["status"])
        self.assertEqual(1, report["retrieval_smoke"]["profile_entity_count"])
        self.assertTrue(report["retrieval_smoke"]["profile_entity_bridge_rebuildable"])
        self.assertEqual([], report["blockers"])

    def test_recovery_report_detects_corrupt_jsonl_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark.jsonl"
            event_log.write_text(
                json.dumps({"record_type": "context_event", "event_id_hash": 1}) + "\n"
                + '{"record_type":"context_entity"',
                encoding="utf-8",
            )

            records, errors = load_jsonl_records_for_recovery(event_log)
            report = matrixark_local_recovery_report(records, parse_errors=errors)

            self.assertEqual(1, len(records))
            self.assertEqual(1, len(errors))
            self.assertTrue(errors[0]["corrupt_tail"])
            self.assertEqual("repair_required", report["status"])
            self.assertIn("recovery:corrupt_tail_detected", report["blockers"])

    def test_recovery_cli_accepts_scope_json_for_retrieval_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark.jsonl"
            event_log.write_text(
                json.dumps(
                    {
                        "record_type": "context_event",
                        "event_id_hash": 1,
                        "scope": {"account_id": "a"},
                        "text": "user: recover this event",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            output = subprocess.check_output(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parent / "matrixark_mcp_recovery.py"),
                    "--event-log",
                    str(event_log),
                    "--scope-json",
                    json.dumps({"account_id": "a"}),
                ]
            )
            report = json.loads(output)

            self.assertEqual("ok", report["status"])
            self.assertTrue(report["retrieval_smoke"]["enabled"])
            self.assertEqual("ok", report["retrieval_smoke"]["status"])
            self.assertEqual(1, report["retrieval_smoke"]["context_event_count"])

    def test_context_index_latest_value_key_preserves_data_model(self) -> None:
        compacted = compact_latest_value_records(
            [
                {
                    "record_type": "context_index",
                    "index_name": "term:repo",
                    "data_model": "context_entity",
                    "scope_key": "tenant:1",
                    "node_hash": 7,
                    "timestamp_key_ms": 10,
                    "ref_hashes": [1],
                    "updated_at_ms": 10,
                },
                {
                    "record_type": "context_index",
                    "index_name": "term:repo",
                    "data_model": "context_profile_entity",
                    "scope_key": "tenant:1",
                    "node_hash": 7,
                    "timestamp_key_ms": 10,
                    "ref_hashes": [2],
                    "updated_at_ms": 10,
                },
            ]
        )

        self.assertEqual(2, len(compacted))
        self.assertEqual(
            {"context_entity", "context_profile_entity"},
            {record["data_model"] for record in compacted},
        )


if __name__ == "__main__":
    unittest.main()
