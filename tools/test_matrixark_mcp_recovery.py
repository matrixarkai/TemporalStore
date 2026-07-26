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
                "extraction_phase": "provisional",
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
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_session_ids": ["codex:session-a", "codex:session-b"],
                "source_roles": ["user", "assistant"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["Stop"],
                "updated_at_ms": 200,
            },
            {"record_type": "context_embedding", "embedding_type": "entity_state", "ref_type": "entity", "ref_hash": 301},
            {"record_type": "context_index", "index_name": "memory_scope:user_profile", "data_model": "context_profile_entity", "ref_hashes": [301]},
            {
                "record_type": "context_summary_dirty",
                "dirty_hash": 401,
                "status": "dirty",
                "dirty_reason": "profile_entity_promoted",
                "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
                "updated_at_ms": 200,
            },
            {"record_type": "context_embedding", "embedding_type": "event_text", "ref_type": "event", "ref_hash": 101},
            {"record_type": "context_embedding", "embedding_type": "entity_state", "ref_type": "entity", "ref_hash": 201},
            {"record_type": "context_embedding", "embedding_type": "summary_text", "ref_type": "summary", "ref_hash": 501},
            {
                "record_type": "context_summary",
                "summary_type": "batch_l0",
                "summary_hash": 501,
                "summary_text": "Repo location preference.",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_memory_scopes": ["user_profile"],
                "source_session_continuities": ["cross_session"],
                "source_extraction_phases": ["final"],
                "source_roles": ["assistant"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["Stop"],
            },
            {
                "record_type": "context_pack_telemetry",
                "context_pack_id": "pack-recover-1",
                "query_hash": 901,
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-c"},
                "audit_mode": "telemetry_only",
                "question_type": "fact",
                "selected_ref_count": 2,
                "dropped_ref_count": 1,
                "used_remote_context_tokens": 42,
                "remote_context_budget_tokens": 512,
                "memory_layer_budget": {"by_memory_scope": {"user_profile": {"refs": 1, "tokens": 12}}},
                "retrieval_request_metadata": {
                    "retrieval_source": "codex_hook_retrieve",
                    "codex_event": "UserPromptSubmit",
                    "hook_type": "user_prompt_submit",
                    "session_id_source": "payload_field",
                    "lifecycle_stage": "before_llm_retrieve",
                },
                "created_at_ms": 250,
            },
        ]

        report = matrixark_local_recovery_report(
            records,
            scope={"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "codex:session-c"},
        )

        self.assertEqual("rebuild_required", report["status"])
        self.assertTrue(report["hot_memory_persisted"])
        self.assertTrue(report["cache_rebuild"]["read_cache_rebuildable_from_durable_log"])
        self.assertTrue(report["cache_rebuild"]["retrieval_cache_rebuildable_from_hot_records"])
        self.assertEqual(1, report["memory_hierarchy"]["session_entity_count"])
        self.assertEqual(1, report["memory_hierarchy"]["profile_entity_count"])
        self.assertEqual(0, report["memory_hierarchy"]["session_dirty_summary_count"])
        self.assertEqual(1, report["memory_hierarchy"]["profile_dirty_summary_count"])
        self.assertEqual(["codex:session-a", "codex:session-b"], report["memory_hierarchy"]["source_session_ids"])
        self.assertEqual(2, report["memory_hierarchy"]["memory_scope_counts"]["user_profile"])
        self.assertEqual(2, report["memory_hierarchy"]["session_continuity_counts"]["cross_session"])
        self.assertEqual(2, report["memory_hierarchy"]["extraction_phase_counts"]["final"])
        self.assertGreaterEqual(report["memory_hierarchy"]["final_session_boundary_ref_count"], 2)
        self.assertTrue(report["memory_hierarchy"]["profile_cross_session_bridge_rebuildable"])
        self.assertIn("assistant", report["memory_hierarchy"]["source_roles"])
        self.assertIn("hook_boundary", report["memory_hierarchy"]["source_hook_types"])
        self.assertIn("Stop", report["memory_hierarchy"]["source_codex_events"])
        self.assertEqual(1, report["derived_views"]["index_posting_count"])
        self.assertEqual(1, report["derived_views"]["dirty_summary_count"])
        self.assertEqual(0, report["derived_views"]["session_dirty_summary_count"])
        self.assertEqual(1, report["derived_views"]["profile_dirty_summary_count"])
        self.assertEqual("rebuild_required", report["derived_views"]["readiness"]["status"])
        self.assertEqual(["derived:summaries_dirty"], report["warnings"])
        self.assertIn("run matrixark_refresh_summaries for dirty context nodes", report["recovery_actions"])
        self.assertEqual("ok", report["retrieval_smoke"]["status"])
        self.assertEqual(1, report["retrieval_smoke"]["profile_entity_count"])
        self.assertTrue(report["retrieval_smoke"]["profile_entity_bridge_rebuildable"])
        self.assertTrue(report["retrieval_smoke"]["profile_cross_session_bridge_rebuildable"])
        self.assertGreaterEqual(report["retrieval_smoke"]["memory_scope_counts"]["user_profile"], 1)
        self.assertGreaterEqual(report["retrieval_smoke"]["session_continuity_counts"]["cross_session"], 1)
        self.assertGreaterEqual(report["retrieval_smoke"]["extraction_phase_counts"]["final"], 1)
        self.assertGreaterEqual(report["retrieval_smoke"]["final_session_boundary_ref_count"], 1)
        self.assertEqual(1, report["retrieval_visibility"]["telemetry_count"])
        self.assertEqual(0, report["retrieval_visibility"]["audit_count"])
        self.assertEqual(1, report["retrieval_visibility"]["hook_retrieval_telemetry_count"])
        self.assertTrue(report["retrieval_visibility"]["telemetry_rebuildable_from_durable_log"])
        self.assertEqual(["before_llm_retrieve"], report["retrieval_visibility"]["lifecycle_stages"])
        self.assertEqual(["pack-recover-1"], report["retrieval_visibility"]["context_pack_ids"])
        self.assertEqual(1, report["retrieval_visibility"]["memory_layer_budget_record_count"])
        self.assertEqual(2, report["retrieval_visibility"]["selected_ref_count"])
        self.assertEqual(1, report["retrieval_visibility"]["dropped_ref_count"])
        self.assertEqual(512, report["retrieval_visibility"]["max_remote_context_budget_tokens"])
        self.assertTrue(report["cache_rebuild"]["retrieval_visibility_rebuildable_from_durable_log"])
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

    def test_recovery_report_flags_missing_derived_views_without_log_blocker(self) -> None:
        report = matrixark_local_recovery_report(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 1,
                    "scope": {"account_id": "a"},
                    "text": "user: remember this",
                }
            ],
            scope={"account_id": "a"},
        )

        self.assertEqual("rebuild_required", report["status"])
        self.assertEqual([], report["blockers"])
        self.assertIn("derived:embeddings_missing_or_stale", report["warnings"])
        self.assertIn("derived:indexes_missing", report["warnings"])
        self.assertIn("derived:summaries_missing", report["warnings"])
        self.assertGreaterEqual(len(report["recovery_actions"]), 3)

    def test_recovery_cli_accepts_scope_json_for_retrieval_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark.jsonl"
            event_log.write_text(
                "\n".join(
                    json.dumps(record)
                    for record in [
                        {
                            "record_type": "context_event",
                            "event_id_hash": 1,
                            "scope": {"account_id": "a"},
                            "text": "user: recover this event",
                        },
                        {"record_type": "context_embedding", "embedding_type": "event_text", "ref_type": "event", "ref_hash": 1},
                        {"record_type": "context_index", "index_name": "event", "data_model": "context_event", "ref_hashes": [1]},
                        {"record_type": "context_summary", "summary_type": "batch_l0", "summary_hash": 2, "summary_text": "recover this event"},
                        {"record_type": "context_embedding", "embedding_type": "summary_text", "ref_type": "summary", "ref_hash": 2},
                    ]
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

            self.assertEqual("ready", report["status"])
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
