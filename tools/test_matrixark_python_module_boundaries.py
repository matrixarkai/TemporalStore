#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Regression tests for the MatrixArk Python MCP module layout."""

from __future__ import annotations

import ast
import importlib
import os
import re
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = REPO_ROOT / "tools"


try:  # mixin
    from tools.test_python_module_boundaries_part3 import _ModuleBoundaryPart3
except ImportError:
    from test_python_module_boundaries_part3 import _ModuleBoundaryPart3

try:  # mixin
    from tools.test_python_module_boundaries_part2 import _ModuleBoundaryPart2
except ImportError:
    from test_python_module_boundaries_part2 import _ModuleBoundaryPart2

class MatrixArkPythonModuleBoundaryTest(unittest.TestCase, _ModuleBoundaryPart3, _ModuleBoundaryPart2):
    def test_package_imports_expose_server_and_schema_catalog(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        schemas_mod = importlib.import_module("tools.matrixark_mcp_schemas")
        self.assertTrue(hasattr(server_mod, "MatrixArkMcpServer"))
        self.assertTrue(hasattr(server_mod, "MatrixArkLocalAdapter"))
        self.assertGreaterEqual(len(schemas_mod.TOOLS), 10)

    def test_one_pass_memory_extraction_falls_back_to_single_segment_for_nonempty_batch(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        result = core_mod.one_pass_memory_extraction(
            {
                "kind": "message",
                "scope": {"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
                "metadata": {},
                "messages": [{"role": "tool", "content": "ok"}],
                "ingestion_time_ms": 123,
                "segment_provider": "deterministic",
            },
            prior_context={"level": "", "refs": [], "messages": []},
        )

        self.assertEqual(1, len(result["segments"]))
        segment = result["segments"][0]
        self.assertEqual([0], segment["message_indexes"])
        self.assertEqual([[0, 0]], segment["coordinate_tuples"])
        self.assertEqual("fallback_derived_from_events", segment["segment_origin"])
        self.assertTrue(segment["derived_from_context_events"])
        self.assertTrue(result["segment_provider"]["fallback_used"])
        self.assertEqual("empty_segment_output", result["segment_provider"]["fallback_reason"])

    def test_modular_extractor_promotes_codex_directives_to_entities(self) -> None:
        extract_mod = importlib.import_module("tools.matrixark_mcp_extraction_normalization")
        entities = extract_mod.extract_batch_entities(
            [
                {
                    "role": "user",
                    "content": "Remember: use Ubuntu /opt/github-services for all TemporalStore repos.",
                }
            ],
            {"source_event_ids": [123]},
        )

        preferences = [entity for entity in entities if entity.get("entity_type") == "preference"]
        self.assertTrue(preferences)
        self.assertEqual("preference", preferences[0]["entity_name"])
        self.assertIn("/opt/github-services", preferences[0]["state"])
        self.assertEqual(["123"], preferences[0]["source_refs"])

    def test_serving_record_materializer_strips_embedding_dirty_lineage_by_default(self) -> None:
        serving_mod = importlib.import_module("tools.matrixark_mcp_serving_records")
        materialized = serving_mod.materialize_serving_records(
            {
                "record_type": "context_embedding",
                "embedding_type": "node_l0",
                "ref_type": "summary",
                "ref_hash": 44,
                "node_hash": 55,
                "node_path": ["tenant:t", "user:u", "session:s"],
                "dim": 2,
                "model": "matrixark-local-token-hash-v1",
                "vector": [0.1, 0.2],
                "memory_scope": "session",
                "session_continuity": "same_session",
                "dirty_hash": 66,
                "summary_generation_policy": {"provider": "deterministic"},
                "source_event_ids": [77],
                "source_roles": ["user"],
                "source_role_counts": {"user": 1},
            }
        )

        self.assertEqual(1, len(materialized))
        embedding = materialized[0]
        self.assertEqual("context_embedding", embedding["record_type"])
        self.assertEqual("session", embedding["memory_scope"])
        self.assertEqual("same_session", embedding["session_continuity"])
        for field in [
            "dirty_hash",
            "summary_generation_policy",
            "source_event_ids",
            "source_roles",
            "source_role_counts",
            "profile_source_event_count",
        ]:
            self.assertNotIn(field, embedding)

    def test_serving_record_materializer_compacts_profile_embedding_lineage_by_default(self) -> None:
        serving_mod = importlib.import_module("tools.matrixark_mcp_serving_records")
        materialized = serving_mod.materialize_serving_records(
            {
                "record_type": "context_embedding",
                "embedding_type": "profile_entity_state",
                "ref_type": "entity",
                "ref_hash": 101,
                "node_hash": 202,
                "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
                "dim": 2,
                "model": "matrixark-local-token-hash-v1",
                "vector": [0.1, 0.2],
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_session_ids": ["s1", "s2"],
                "source_entity_hashes": [11, 12],
                "source_roles": ["assistant", "tool"],
                "source_role_counts": {"assistant": 1, "tool": 1},
                "source_memory_selection_policies": [
                    "selected_assistant_decision_outcome_only",
                    "selected_tool_evidence_only",
                ],
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 1,
                    "selected_tool_evidence_only": 1,
                },
                "promoted_from_memory_scope": "session",
                "extraction_phase": "final",
                "final_session_boundary": True,
            }
        )

        self.assertEqual(1, len(materialized))
        embedding = materialized[0]
        self.assertEqual("context_embedding", embedding["record_type"])
        self.assertEqual("profile_entity_state", embedding["embedding_type"])
        self.assertEqual("user_profile", embedding["memory_scope"])
        self.assertEqual("cross_session", embedding["session_continuity"])
        self.assertEqual(2, embedding["profile_source_session_count"])
        self.assertEqual(2, embedding["profile_source_entity_count"])
        for field in [
            "source_session_ids",
            "source_entity_hashes",
            "source_roles",
            "source_role_counts",
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
            "promoted_from_memory_scope",
            "extraction_phase",
            "final_session_boundary",
        ]:
            self.assertNotIn(field, embedding)

    def test_batch_runtime_embedding_compaction_matches_serving_shape(self) -> None:
        runtime_mod = importlib.import_module("tools.matrixark_mcp_local_batch_extract_runtime")
        compacted = runtime_mod.compact_context_embedding_record(
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "source_event_ids": [7],
                "source_roles": ["user"],
                "source_role_counts": {"user": 1},
                "source_hook_types": ["before_llm"],
                "source_memory_selection_policies": ["selected_user_prompt"],
                "extraction_phase": "provisional",
                "final_session_boundary": False,
                "vector": [0.3, 0.4],
            }
        )

        self.assertEqual("context_embedding", compacted["record_type"])
        self.assertEqual("event_text", compacted["embedding_type"])
        self.assertEqual("session", compacted["memory_scope"])
        self.assertEqual("same_session", compacted["session_continuity"])
        self.assertEqual([0.3, 0.4], compacted["vector"])
        for field in [
            "source_event_ids",
            "source_roles",
            "source_role_counts",
            "source_hook_types",
            "source_memory_selection_policies",
            "extraction_phase",
            "final_session_boundary",
        ]:
            self.assertNotIn(field, compacted)

    def test_modular_extractor_promotes_bounded_assistant_memory_terms(self) -> None:
        extract_mod = importlib.import_module("tools.matrixark_mcp_extraction_normalization")
        entities = extract_mod.extract_batch_entities(
            [
                {
                    "role": "assistant",
                    "content": "Validated profile cross-session memory retrieval and updated the current-state budget.",
                }
            ],
            {"source_event_ids": [456]},
        )

        decisions = [entity for entity in entities if entity.get("entity_type") == "assistant_decision"]
        self.assertTrue(decisions)
        self.assertIn("Validated profile cross-session memory retrieval", decisions[0]["state"])
        self.assertEqual(["456"], decisions[0]["source_refs"])

    def test_modular_extractor_uses_canonical_assistant_and_tool_entities(self) -> None:
        extract_mod = importlib.import_module("tools.matrixark_mcp_extraction_normalization")
        entities = extract_mod.extract_batch_entities(
            [
                {
                    "role": "assistant",
                    "content": "Updated profile memory extraction and validated cross-session retrieval.",
                },
                {
                    "role": "tool",
                    "content": "Exit code: 0; Ran 42 tests; pushed commit abc1234 to origin/main.",
                },
            ],
            {"source_event_ids": [789]},
        )

        by_type = {entity.get("entity_type"): entity for entity in entities}
        self.assertEqual("assistant_decision", by_type["assistant_decision"]["entity_name"])
        self.assertEqual("tool_evidence", by_type["tool_evidence"]["entity_name"])
        self.assertTrue(by_type["assistant_decision"]["field_patches"])
        self.assertTrue(by_type["tool_evidence"]["field_patches"])

    def test_modular_extractor_uses_role_specific_source_refs_for_assistant_and_tool(self) -> None:
        extract_mod = importlib.import_module("tools.matrixark_mcp_extraction_normalization")
        entities = extract_mod.extract_batch_entities(
            [
                {"role": "user", "content": "Remember: use compact profile budgets."},
                {"role": "assistant", "content": "Decision: keep profile extraction enabled."},
                {"role": "tool", "content": "Exit code: 0; Ran 42 tests; pushed commit abc1234 to origin/main."},
            ],
            {"source_event_ids": [111, 222, 333]},
        )
        by_type = {entity.get("entity_type"): entity for entity in entities}

        self.assertEqual(["222"], by_type["assistant_decision"]["source_refs"])
        self.assertEqual(["333"], by_type["tool_evidence"]["source_refs"])
        self.assertEqual(["111", "222", "333"], by_type["preference"]["source_refs"])

    def test_memory_phase_and_retrieval_budget_fields_are_public_schema(self) -> None:
        schemas_mod = importlib.import_module("tools.matrixark_mcp_schemas")
        tools_by_name = {tool["name"]: tool for tool in schemas_mod.TOOLS}
        session_props = tools_by_name["matrixark_session_commit"]["inputSchema"]["properties"]
        batch_props = tools_by_name["matrixark_batch_extract"]["inputSchema"]["properties"]
        retrieve_props = tools_by_name["matrixark_retrieve"]["inputSchema"]["properties"]
        retrieve_output_props = tools_by_name["matrixark_retrieve"]["outputSchema"]["properties"]
        dashboard_table_enum = tools_by_name["matrixark_ingestion_dashboard"]["inputSchema"]["properties"]["table"]["enum"]

        self.assertEqual(["provisional", "final", "standalone"], session_props["extraction_phase"]["enum"])
        self.assertEqual(["provisional", "final", "standalone"], batch_props["extraction_phase"]["enum"])
        self.assertIn("final_session_boundary", session_props)
        self.assertIn("final_session_boundary", batch_props)
        self.assertIn("include_retrieval_metrics", retrieve_props)
        self.assertIn("include_retrieval_debug", retrieve_props)
        self.assertIn("debug_context_pack", retrieve_props)
        self.assertIn("remote MatrixArk budget", retrieve_props["local_context_safety_margin_tokens"]["description"])
        self.assertIn("memory_hierarchy", retrieve_output_props)
        self.assertIn("async_pipeline_readiness", retrieve_output_props)
        self.assertIn("memory_layer_budget", retrieve_output_props)
        self.assertIn("dropped_memory_layer_budget", retrieve_output_props)
        self.assertIn("memory_layer_pressure", retrieve_output_props)
        self.assertIn("debug-only", tools_by_name["matrixark_retrieve"]["outputSchema"]["description"])
        hierarchy_props = retrieve_output_props["memory_hierarchy"]["properties"]
        self.assertIn("profile entity bridge", retrieve_output_props["memory_hierarchy"]["description"])
        self.assertIn("cross_session_budget_floor_status", hierarchy_props)
        self.assertIn("selected_ref_flow", hierarchy_props)
        self.assertIn("freshness_warnings", retrieve_output_props["async_pipeline_readiness"]["properties"])
        self.assertIn("pending_memory_selection_policies", retrieve_output_props["async_pipeline_readiness"]["properties"])
        self.assertIn("debug-only", retrieve_output_props["async_pipeline_readiness"]["description"])
        self.assertIn("debug-only", retrieve_output_props["memory_layer_budget"]["description"])
        self.assertIn("summary_refresh", dashboard_table_enum)
        self.assertIn("async_pipeline", dashboard_table_enum)

    def test_recent_ingestion_report_tracks_serving_visibility_gaps(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        raw_rows = [
            {
                "sequence": 0,
                "record": {
                    "record_type": "resource_chunk",
                    "text": "resource content was ingested",
                },
            }
        ]
        broken = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            1,
            0,
            raw_rows,
            4,
            0,
            [
                {"sequence": 10, "record": {"record_type": "context_entity", "memory_scope": "session"}},
                {"sequence": 11, "record": {"record_type": "context_index"}},
                {"sequence": 12, "record": {"record_type": "context_segment"}},
                {"sequence": 13, "record": {"record_type": "context_summary"}},
            ],
        )

        self.assertEqual("gap", broken["serving_visibility_status"])
        self.assertIn("context_event_missing_while_derived_memory_present", broken["serving_visibility_gaps"])
        self.assertIn("context_embedding_missing_while_derived_memory_present", broken["serving_visibility_gaps"])
        self.assertIn("profile_records_missing_from_recent_serving_window", broken["serving_visibility_gaps"])
        self.assertIn("resource_skill_records_missing_from_recent_serving_window", broken["serving_visibility_gaps"])

        healthy = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            1,
            0,
            raw_rows,
            8,
            0,
            [
                {"sequence": 20, "record": {"record_type": "context_entity", "memory_scope": "session"}},
                {"sequence": 21, "record": {"record_type": "context_index", "data_model": "context_profile_entity"}},
                {"sequence": 22, "record": {"record_type": "context_event", "text": "user: hello"}},
                {"sequence": 23, "record": {"record_type": "context_embedding", "embedding_type": "event_text"}},
                {"sequence": 24, "record": {"record_type": "context_entity", "memory_scope": "user_profile"}},
                {"sequence": 25, "record": {"record_type": "resource_chunk", "text": "resource content"}},
            ],
        )

        self.assertEqual("ok", healthy["serving_visibility_status"])
        self.assertEqual([], healthy["serving_visibility_gaps"])
        self.assertEqual(1, healthy["recent_context_event_count"])
        self.assertEqual(1, healthy["recent_context_embedding_count"])
        self.assertGreaterEqual(healthy["recent_profile_record_count"], 1)
        self.assertEqual(1, healthy["recent_resource_skill_record_count"])

    def test_recent_ingestion_report_keeps_raw_rows_out_of_serving_memory_lists(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        backend = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            3,
            0,
            [
                {
                    "sequence": 1,
                    "record": {
                        "record_type": "agent_message",
                        "raw_record_type": "raw_agent_message",
                        "raw_ingestion_visibility": "backfill_only",
                        "codex_api_event": "UserPromptSubmit",
                        "synthetic": False,
                    },
                },
                {"sequence": 2, "record": {"record_type": "context_entity", "entity_name": "raw_only_entity"}},
                {"sequence": 3, "record": {"record_type": "context_summary", "summary_text": "raw only summary"}},
            ],
            2,
            0,
            [
                {"sequence": 10, "record": {"record_type": "context_entity", "entity_name": "serving_entity"}},
                {"sequence": 11, "record": {"record_type": "context_summary", "summary_text": "serving summary"}},
            ],
        )

        self.assertEqual("raw_agent_message_backfill_only_context_event_serving", backend["serving_boundary_policy"])
        self.assertEqual(1, backend["raw_backfill_only_count"])
        self.assertEqual(["serving_entity"], [item["text"] for item in backend["recent_entities"]])
        self.assertEqual(["serving summary"], [item["text"] for item in backend["recent_summaries"]])

    def test_raw_agent_message_routes_to_raw_ingestion_storage_part(self) -> None:
        storage_mod = importlib.import_module("tools.matrixark_mcp_storage_options")

        raw_hook_message = {
            "record_type": "agent_message",
            "raw_record_type": "raw_agent_message",
            "raw_ingestion_visibility": "backfill_only",
            "serving_projection_record_type": "context_event",
            "serving_context_event_hash": 12345,
        }
        serving_event = {
            "record_type": "context_event",
            "event_id_hash": 12345,
        }

        self.assertEqual("raw_ingestion", storage_mod.storage_record_kind(raw_hook_message))
        self.assertEqual("context_event", storage_mod.storage_record_kind(serving_event))

    def test_latest_value_compaction_keeps_one_context_event_per_event_hash(self) -> None:
        latest_mod = importlib.import_module("tools.matrixark_mcp_latest_values")

        compacted = latest_mod.compact_latest_value_records(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 12345,
                    "status": "pending",
                    "updated_at_ms": 100,
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 12345,
                    "status": "extraction_committed",
                    "updated_at_ms": 200,
                },
                {
                    "record_type": "session_buffer_event",
                    "buffer_key": ["acct", "tenant", "user", "session"],
                    "event_id_hash": 12345,
                    "status": "pending",
                    "envelope": {"messages": [{"role": "user", "content": "raw pending text"}]},
                    "updated_at_ms": 100,
                },
                {
                    "record_type": "session_buffer_event",
                    "buffer_key": ["acct", "tenant", "user", "session"],
                    "event_id_hash": 12345,
                    "status": "committed",
                    "commit_id_hash": 67890,
                    "updated_at_ms": 300,
                },
            ]
        )

        self.assertEqual(2, len(compacted))
        event = next(record for record in compacted if record["record_type"] == "context_event")
        buffer = next(record for record in compacted if record["record_type"] == "session_buffer_event")
        self.assertEqual("extraction_committed", event["status"])
        self.assertEqual("committed", buffer["status"])
        self.assertNotIn("envelope", buffer)

    def test_recent_ingestion_report_summarizes_serving_visibility_gate(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        broken_report = {
            "backends": [
                {
                    "backend": "Rust TemporalStore",
                    "prefix": "matrixark:codex-hook:rust-live-v2",
                    "serving_visibility_gaps": [
                        "context_event_missing_while_derived_memory_present",
                        "context_embedding_missing_while_derived_memory_present",
                    ],
                },
                {
                    "backend": "TemporalStore",
                    "prefix": "matrixark:codex-hook:native-live-v2",
                    "serving_visibility_gaps": [],
                },
            ]
        }
        summary = report_mod.serving_visibility_summary(broken_report)

        self.assertFalse(summary["serving_visibility_pass"])
        self.assertEqual(2, summary["serving_visibility_gap_count"])
        self.assertEqual(["Rust TemporalStore"], [item["backend"] for item in summary["serving_visibility_gap_backends"]])

        healthy = report_mod.serving_visibility_summary(
            {"backends": [{"backend": "Rust TemporalStore", "serving_visibility_gaps": []}]}
        )
        self.assertTrue(healthy["serving_visibility_pass"])
        self.assertEqual(0, healthy["serving_visibility_gap_count"])
        self.assertEqual([], healthy["serving_visibility_gap_backends"])

    def test_recent_ingestion_report_serving_visibility_requirement_flags(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")

        self.assertTrue(report_mod.require_serving_visibility("1"))
        self.assertTrue(report_mod.require_serving_visibility("required"))
        self.assertFalse(report_mod.require_serving_visibility(""))
        self.assertFalse(report_mod.require_serving_visibility("false"))
        self.assertTrue(report_mod.parse_args(["--require-serving-visibility"]).require_serving_visibility)
        self.assertFalse(report_mod.parse_args([]).require_serving_visibility)

    def test_ingestion_dashboard_projects_event_and_profile_lineage(self) -> None:
        adapter_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = adapter_mod.MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-dashboard-profile.jsonl")
            scope = {
                "account_id": "acct_dashboard_profile",
                "tenant_id": "tenant_dashboard_profile",
                "user_id": "user_dashboard_profile",
                "session_id": "session_dashboard_profile",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "user",
                            "content": "above context segment is the same as context event? why no user profile? fix them",
                        }
                    ],
                    "threshold_messages": 1,
                    "force": True,
                    "skip_prior_context": True,
                }
            )
            self.assertEqual(1, result["events_written"])
            self.assertEqual(1, result["segments_written"])
            self.assertEqual(1, result["profile_entities_written"])

            events = adapter.ingestion_dashboard({"scope": scope, "table": "events"})
            self.assertEqual(1, events["total"])
            event = events["rows"][0]
            self.assertEqual("context_event", event["row_type"])
            self.assertEqual("session", event["memory_scope"])
            self.assertEqual("same_session", event["session_continuity"])

            entities = adapter.ingestion_dashboard({"scope": scope, "table": "entities", "page_size": 10})
            profile_rows = [
                row
                for row in entities["rows"]
                if row.get("memory_scope") == "user_profile"
                and row.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(profile_rows, entities)
            self.assertEqual("always_when_profile_scope_available", profile_rows[0]["profile_promotion_policy"])
            self.assertEqual("", profile_rows[0]["profile_promotion_blocker"])
            self.assertTrue(profile_rows[0]["value"])

            segment_rows = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_segment"
            ]
            self.assertEqual(1, len(segment_rows))
            self.assertNotEqual("context_event", segment_rows[0]["record_type"])
            self.assertEqual("context_event", segment_rows[0]["source_record_type"])
            self.assertTrue(segment_rows[0]["derived_from_context_events"])

    def test_recent_ingestion_report_summarizes_extraction_input_coverage_gate(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        broken_report = {
            "backends": [
                {
                    "backend": "Rust TemporalStore",
                    "prefix": "matrixark:codex-hook:rust-live-v2",
                    "extraction_input_coverage_gaps": [
                        {
                            "session_id": "codex-tool-gap",
                            "gaps": ["source_role:tool:missing_from_derived_serving_memory"],
                        }
                    ],
                },
                {
                    "backend": "TemporalStore",
                    "prefix": "matrixark:codex-hook:native-live-v2",
                    "extraction_input_coverage_gaps": [],
                },
            ]
        }
        summary = report_mod.extraction_input_coverage_summary(broken_report)

        self.assertFalse(summary["extraction_input_coverage_pass"])
        self.assertEqual(1, summary["extraction_input_coverage_gap_count"])
        self.assertEqual(
            ["Rust TemporalStore"],
            [item["backend"] for item in summary["extraction_input_coverage_gap_backends"]],
        )

        healthy = report_mod.extraction_input_coverage_summary(
            {"backends": [{"backend": "Rust TemporalStore", "extraction_input_coverage_gaps": []}]}
        )
        self.assertTrue(healthy["extraction_input_coverage_pass"])
        self.assertEqual(0, healthy["extraction_input_coverage_gap_count"])
        self.assertEqual([], healthy["extraction_input_coverage_gap_backends"])

    def test_recent_ingestion_report_extraction_input_requirement_flags(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")

        self.assertTrue(report_mod.require_extraction_input_coverage("1"))
        self.assertTrue(report_mod.require_extraction_input_coverage("required"))
        self.assertFalse(report_mod.require_extraction_input_coverage(""))
        self.assertFalse(report_mod.require_extraction_input_coverage("false"))
        self.assertTrue(
            report_mod.parse_args(["--require-extraction-input-coverage"]).require_extraction_input_coverage
        )
        self.assertFalse(report_mod.parse_args([]).require_extraction_input_coverage)

    def test_recent_ingestion_report_tracks_profile_promotion_policy_gaps(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        broken = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 9,
                    "record": {
                        "record_type": "context_extraction_audit",
                        "outputs": {
                            "entities": 2,
                            "profile_entities": 1,
                            "profile_promotion_policy": "important_enough",
                            "profile_promotion_importance_gate": True,
                            "profile_promotion_scope_available": True,
                            "profile_promotion_blocker": "",
                            "profile_promotion_summary": [],
                        },
                    },
                }
            ],
        )

        self.assertEqual("gap", broken["profile_promotion_policy_status"])
        self.assertEqual(1, len(broken["profile_promotion_policy_gaps"]))
        self.assertIn("profile_promotion:policy_not_always", broken["profile_promotion_policy_gaps"][0]["gaps"])
        self.assertIn(
            "profile_promotion:profile_entities_less_than_session_entities",
            broken["profile_promotion_policy_gaps"][0]["gaps"],
        )
        self.assertIn("profile_promotion:importance_gate_enabled", broken["profile_promotion_policy_gaps"][0]["gaps"])
        self.assertIn("profile_promotion:summary_less_than_profile_entities", broken["profile_promotion_policy_gaps"][0]["gaps"])

        missing_scope_blocker = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 11,
                    "record": {
                        "record_type": "context_extraction_audit",
                        "outputs": {
                            "entities": 1,
                            "profile_entities": 0,
                            "profile_promotion_policy": "always_when_profile_scope_available",
                            "profile_promotion_scope_available": False,
                            "profile_promotion_blocker": "",
                        },
                    },
                }
            ],
        )
        self.assertIn(
            "profile_promotion:profile_scope_missing_blocker_absent",
            missing_scope_blocker["profile_promotion_policy_gaps"][0]["gaps"],
        )

        healthy = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 10,
                    "record": {
                        "record_type": "context_extraction_audit",
                        "outputs": {
                            "entities": 2,
                            "profile_entities": 2,
                            "profile_promotion_policy": "always_when_profile_scope_available",
                            "profile_promotion_importance_gate": False,
                            "profile_promotion_scope_available": True,
                            "profile_promotion_blocker": "",
                            "profile_promotion_summary": [
                                {"profile_entity_hash": 1},
                                {"profile_entity_hash": 2},
                            ],
                        },
                    },
                }
            ],
        )
        self.assertEqual("ok", healthy["profile_promotion_policy_status"])
        self.assertEqual([], healthy["profile_promotion_policy_gaps"])

    def test_recent_ingestion_report_profile_promotion_policy_requirement_flags(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")

        self.assertTrue(report_mod.require_profile_promotion_policy("1"))
        self.assertTrue(report_mod.require_profile_promotion_policy("required"))
        self.assertFalse(report_mod.require_profile_promotion_policy(""))
        self.assertFalse(report_mod.require_profile_promotion_policy("false"))
        self.assertTrue(
            report_mod.parse_args(["--require-profile-promotion-policy"]).require_profile_promotion_policy
        )
        self.assertFalse(report_mod.parse_args([]).require_profile_promotion_policy)

    def test_recent_ingestion_report_summarizes_strict_memory_gate(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        healthy = {
            "serving_visibility": {"serving_visibility_pass": True},
            "extraction_input_coverage": {"extraction_input_coverage_pass": True},
            "retrieval_memory_coverage": {"retrieval_memory_coverage_pass": True},
            "profile_promotion_policy": {"profile_promotion_policy_pass": True},
        }

        self.assertEqual(
            {
                "strict_memory_gate_pass": True,
                "strict_memory_gate_failed": [],
                "strict_memory_gate_status": "pass",
            },
            report_mod.strict_memory_gate_summary(healthy),
        )

        broken = {
            "serving_visibility": {"serving_visibility_pass": False},
            "extraction_input_coverage": {"extraction_input_coverage_pass": True},
            "retrieval_memory_coverage": {"retrieval_memory_coverage_pass": False},
            "profile_promotion_policy": {"profile_promotion_policy_pass": False},
        }
        summary = report_mod.strict_memory_gate_summary(broken)

        self.assertFalse(summary["strict_memory_gate_pass"])
        self.assertEqual(
            ["serving_visibility", "retrieval_memory_coverage", "profile_promotion_policy"],
            summary["strict_memory_gate_failed"],
        )
        self.assertEqual("gap", summary["strict_memory_gate_status"])

    def test_recent_ingestion_report_strict_memory_gate_requirement_flags(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")

        self.assertTrue(report_mod.require_all_memory_gates("1"))
        self.assertTrue(report_mod.require_all_memory_gates("strict"))
        self.assertTrue(report_mod.require_all_memory_gates("required"))
        self.assertFalse(report_mod.require_all_memory_gates(""))
        self.assertFalse(report_mod.require_all_memory_gates("false"))
        self.assertTrue(report_mod.parse_args(["--require-all-memory-gates"]).require_all_memory_gates)
        self.assertFalse(report_mod.parse_args([]).require_all_memory_gates)

    def test_recent_ingestion_report_tracks_retrieval_memory_coverage(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        backend = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 7,
                    "record": {
                        "record_type": "context_pack_audit",
                        "context_pack_id": "pack-ok",
                        "query": "latest TemporalStore retrieval gate",
                        "selected_refs": [
                            {
                                "ref_type": "entity",
                                "memory_scope": "user_profile",
                                "session_continuity": "cross_session",
                            }
                        ],
                        "memory_layer_budget": {
                            "total_selected_refs": 1,
                            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 7}},
                            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 7}},
                            "by_memory_selection_policy": {
                                "selected_profile_current_state": {"refs": 1, "tokens": 7}
                            },
                        },
                        "recall_policy": {
                            "memory_selection_policy_budget_policy": {
                                "enabled": True,
                                "mode": "auto",
                                "budget_tokens": {"selected_profile_current_state": 96},
                                "selected_ref_count_by_policy": {"selected_profile_current_state": 1},
                            }
                        },
                    },
                }
            ],
        )

        coverage = backend["recent_retrieval_memory_coverages"][0]
        self.assertEqual("ok", coverage["status"])
        self.assertEqual([], coverage["gaps"])
        self.assertEqual(1, coverage["selected_ref_count"])
        self.assertEqual(1, coverage["profile_memory_refs"])
        self.assertTrue(coverage["memory_selection_policy_budget_enabled"])
        self.assertEqual(1, coverage["memory_selection_policy_refs"])
        self.assertEqual(["selected_profile_current_state"], coverage["memory_selection_policy_names"])
        self.assertEqual("ok", backend["retrieval_memory_coverage_status"])

    def test_recent_ingestion_report_flags_missing_memory_selection_policy_budget(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        backend = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 9,
                    "record": {
                        "record_type": "context_pack_audit",
                        "context_pack_id": "pack-missing-policy-budget",
                        "query": "latest promoted profile memory",
                        "selected_refs": [
                            {
                                "ref_type": "entity",
                                "memory_scope": "user_profile",
                                "session_continuity": "cross_session",
                            }
                        ],
                        "memory_layer_budget": {
                            "total_selected_refs": 1,
                            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 7}},
                            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 7}},
                        },
                    },
                }
            ],
        )

        coverage = backend["recent_retrieval_memory_coverages"][0]
        self.assertEqual("gap", coverage["status"])
        self.assertEqual(1, coverage["selected_ref_count"])
        self.assertFalse(coverage["memory_selection_policy_budget_enabled"])
        self.assertEqual(0, coverage["memory_selection_policy_refs"])
        self.assertIn("retrieval:memory_selection_policy_budget_missing_selected_refs", coverage["gaps"])
        self.assertEqual("gap", backend["retrieval_memory_coverage_status"])

    def test_recent_ingestion_report_flags_retrieval_memory_coverage_gaps(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        backend = report_mod.summarize_backend(
            "test",
            "matrixark:test",
            0,
            0,
            [],
            1,
            0,
            [
                {
                    "sequence": 8,
                    "record": {
                        "record_type": "context_pack_audit",
                        "context_pack_id": "pack-gap",
                        "query": "empty retrieval",
                        "selected_refs": [],
                        "memory_layer_budget": {},
                    },
                }
            ],
        )

        coverage = backend["recent_retrieval_memory_coverages"][0]
        self.assertEqual("gap", coverage["status"])
        self.assertIn("retrieval:no_remote_refs_selected", coverage["gaps"])
        self.assertIn("retrieval:no_session_or_profile_memory_selected", coverage["gaps"])
        self.assertIn("retrieval:no_session_continuity_refs_selected", coverage["gaps"])
        self.assertIn("retrieval:memory_layer_budget_missing_selected_refs", coverage["gaps"])
        self.assertEqual("gap", backend["retrieval_memory_coverage_status"])
        self.assertEqual(
            ["retrieval:no_remote_refs_selected", "retrieval:no_session_or_profile_memory_selected", "retrieval:no_session_continuity_refs_selected", "retrieval:memory_layer_budget_missing_selected_refs"],
            backend["retrieval_memory_coverage_gaps"][0]["gaps"],
        )

    def test_recent_ingestion_report_summarizes_retrieval_memory_coverage_gate(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        broken_report = {
            "backends": [
                {
                    "backend": "Rust TemporalStore",
                    "prefix": "matrixark:codex-hook:rust-live-v2",
                    "retrieval_memory_coverage_gaps": [
                        {
                            "context_pack_id": "pack-gap",
                            "gaps": ["retrieval:no_remote_refs_selected"],
                        }
                    ],
                }
            ]
        }
        summary = report_mod.retrieval_memory_coverage_summary(broken_report)

        self.assertFalse(summary["retrieval_memory_coverage_pass"])
        self.assertEqual(1, summary["retrieval_memory_coverage_gap_count"])
        self.assertEqual(["Rust TemporalStore"], [item["backend"] for item in summary["retrieval_memory_coverage_gap_backends"]])
        self.assertTrue(report_mod.require_retrieval_memory_coverage("1"))
        self.assertTrue(report_mod.parse_args(["--require-retrieval-memory-coverage"]).require_retrieval_memory_coverage)




class MatrixArkMcpProtocolHardeningTest(unittest.TestCase):
    def _server(self):
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        event_log = Path(tmpdir.name) / "events.jsonl"
        return server_mod.MatrixArkMcpServer(server_mod.MatrixArkLocalAdapter(event_log), line_json=True)

    def test_jsonrpc_errors_are_specific_and_stable(self) -> None:
        server = self._server()
        invalid = server.handle({"jsonrpc": "1.0", "id": 1, "method": "tools/list"})
        self.assertEqual(-32600, invalid["error"]["code"])

        missing = server.handle({"jsonrpc": "2.0", "id": 2, "method": "unknown/method"})
        self.assertEqual(-32601, missing["error"]["code"])

        bad_params = server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": []})
        self.assertEqual(-32602, bad_params["error"]["code"])

    def test_initialize_reports_server_identity_and_version(self) -> None:
        server = self._server()
        response = server.handle({"jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {}})
        self.assertEqual("matrixark-context", response["result"]["serverInfo"]["name"])
        self.assertRegex(response["result"]["serverInfo"]["version"], r"^\d+\.\d+\.\d+$")
        self.assertIn("tools", response["result"]["capabilities"])

    def test_call_tool_does_not_mutate_caller_arguments(self) -> None:
        server = self._server()
        args = {
            "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
            "agent_hook": {
                "hook_id": "hook-1",
                "hook_type": "before_llm",
                "source": "codex",
                "auto_captured": True,
                "observed_at_ms": 1,
            },
        }
        result = server.call_tool("matrixark_ingest", args)
        self.assertIn(result["status"], {"accepted", "ok"})
        self.assertIn("event_id_hash", result)
        self.assertIsInstance(result["idempotency_key_hash"], int)
        self.assertFalse(result["idempotent_replay"])
        self.assertIn("agent_hook", args)

    def test_retrieval_audit_is_off_by_default(self) -> None:
        server = self._server()
        server.call_tool("matrixark_ingest", {"messages": [{"role": "user", "content": "Alice approved the GPU budget."}]})
        pack = server.call_tool("matrixark_retrieve", {"query": "what did Alice approve?"})
        self.assertNotIn("operational_visibility_policy", pack)
        self.assertNotIn("question_type", pack)
        self.assertNotIn("recall_policy", pack)
        records = server.adapter.read_all()
        self.assertFalse(any(record.get("record_type") == "context_pack_audit" for record in records))
        self.assertFalse(any(record.get("record_type") == "context_pack_telemetry" for record in records))

    def test_context_debug_record_keeps_its_payload_through_the_local_log(self) -> None:
        # debug_payload is on the local-JSONL bulky strip list, which is correct for rows where it
        # is incidental. On context_debug_record it is the entire record -- the writer refuses to
        # emit one without a payload -- so stripping it stored a husk, and opting in via
        # MATRIXARK_CONTEXT_DEBUG_RECORDS bought no diagnostics at all. The exemption is limited
        # to that record type and to that one field; this test pins both edges.
        server = self._server()
        adapter = server.adapter
        # Read the flag off the live method's globals rather than by module name: discovery keeps
        # several generations of the adapter module alive, so importing it by name can return a
        # different object than the one defining the class in use here.
        live_globals = type(adapter)._sanitize_jsonl_record.__globals__
        self.assertFalse(
            live_globals["LOCAL_JSONL_INCLUDE_BULKY_FIELDS"],
            "this test is only meaningful against the default, stripping configuration")

        adapter.append_many(
            [
                {
                    "record_type": "context_debug_record",
                    "debug_type": "event_extraction_detail",
                    "ref_type": "event",
                    "ref_hash": 7,
                    "debug_payload": {"kept": True},
                    "raw_payload": {"stripped": True},
                    "updated_at_ms": 1780000000000,
                },
                {
                    "record_type": "agent_message",
                    "role": "tool",
                    "text": "summary",
                    "debug_payload": {"stripped": True},
                    "updated_at_ms": 1780000000001,
                },
            ]
        )
        records = adapter.read_all()
        debug = next(record for record in records if record.get("record_type") == "context_debug_record")
        message = next(record for record in records if record.get("record_type") == "agent_message")

        self.assertEqual({"kept": True}, debug["debug_payload"])
        # Only the defining field is exempt -- other bulky fields still go.
        self.assertNotIn("raw_payload", debug)
        # And no other record type is affected by the exemption.
        self.assertNotIn("debug_payload", message)

    def test_storage_options_are_validated_stored_and_audited(self) -> None:
        core_modules = []
        for module_name in ("matrixark_mcp_core", "tools.matrixark_mcp_core"):
            try:
                module = importlib.import_module(module_name)
                core_modules.append((module, module.ENABLE_CONTEXT_DEBUG_RECORDS))
                module.ENABLE_CONTEXT_DEBUG_RECORDS = True
            except ModuleNotFoundError:
                pass
        server = self._server()
        try:
            ingest = server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                    "storage_options": {"oplog_mode": "async", "raft_mode": True, "consistency": "linearizable"},
                },
            )
        finally:
            for module, old_debug in core_modules:
                module.ENABLE_CONTEXT_DEBUG_RECORDS = old_debug
        self.assertEqual("accepted", ingest["status"])
        records = server.adapter.read_all()
        event = next(record for record in records if record.get("record_type") == "context_event")
        debug = next(
            record
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_hash") == event.get("event_id_hash")
        )
        storage_options = debug["debug_payload"]["storage_options"]
        self.assertEqual("async", storage_options["oplog_mode"])
        self.assertEqual("raft", storage_options["storage_mode"])
        self.assertEqual("raft", storage_options["replication_mode"])
        self.assertTrue(storage_options["raft_mode"])
        self.assertEqual("raft_async", storage_options["route"])
        self.assertEqual("raft_async", event["storage_route"]["route"])
        self.assertEqual("raft", event["storage_route"]["storage_mode"])
        self.assertEqual("async", event["storage_route"]["oplog_mode"])
        self.assertEqual("raft", event["storage_route"]["storage_family"])
        self.assertEqual("async", event["storage_route"]["write_mode"])
        self.assertTrue(event["storage_route"]["background_write"])
        self.assertEqual("ack_after_memory_append", event["storage_route"]["write_ack_policy"])
        self.assertEqual("context_node", event["context_event_parent_type"])
        self.assertNotIn("node_id", event)
        self.assertEqual(event["node_hash"], event["context_event_parent_hash"])
        self.assertNotIn("parent_type", event)
        self.assertNotIn("parent_hash", event)
        self.assertIn("context_event:context_node:", event["context_event_key"])
        self.assertRegex(event["event_time_key"], r"^\d{20}:\d+$")

        route_cases = [
            ("shared_store_async", "shared_store", "shared_store", "async", False),
            ("shared_store_sync", "shared_store", "shared_store", "sync", False),
            ("raft_async", "raft", "raft", "async", True),
            ("raft_sync", "raft", "raft", "sync", True),
        ]
        for route, storage_mode, replication_mode, oplog_mode, raft_mode in route_cases:
            normalized = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options({"storage_options": {"route": route}})
            self.assertEqual(route, normalized["route"])
            self.assertEqual(storage_mode, normalized["storage_mode"])
            self.assertEqual(replication_mode, normalized["replication_mode"])
            self.assertEqual(oplog_mode, normalized["oplog_mode"])
            self.assertEqual(oplog_mode, normalized["write_mode"])
            self.assertEqual(storage_mode, normalized["storage_family"])
            self.assertEqual(raft_mode, normalized["raft_mode"])
            self.assertEqual(route, normalized["route_key"])
            self.assertEqual(oplog_mode == "async", normalized["background_write"])
            self.assertEqual("ack_after_durable_commit" if oplog_mode == "sync" else "ack_after_memory_append", normalized["write_ack_policy"])

        friendly = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "storage_options": {
                    "storage_family": "shared_store",
                    "write_mode": "sync",
                    "consistency": "read_your_writes",
                }
            }
        )
        self.assertEqual("shared_store_sync", friendly["route"])
        self.assertEqual("shared_store", friendly["storage_family"])
        self.assertEqual("sync", friendly["write_mode"])
        self.assertFalse(friendly["background_write"])

        top_level_alias = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "temporalstore_storage_family": "raft",
                "temporalstore_write_mode": "sync",
            }
        )
        self.assertEqual("raft_sync", top_level_alias["route"])
        self.assertEqual("raft", top_level_alias["storage_family"])
        self.assertEqual("sync", top_level_alias["write_mode"])
        self.assertTrue(top_level_alias["raft_mode"])

        with self.assertRaisesRegex(Exception, "background_write cannot be true"):
            importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "write_mode": "sync", "background_write": True}}
            )

        pack = server.call_tool(
            "matrixark_retrieve",
            {
                "query": "Who approved the GPU budget?",
                "storage_options": {"storage_mode": "shared_store", "oplog_mode": "sync"},
                "audit_mode": "full",
            },
        )
        self.assertNotIn("recall_policy", pack)
        audit = next(record for record in reversed(server.adapter.read_all()) if record.get("record_type") == "context_pack_audit")
        self.assertEqual("shared_store", audit["recall_policy"]["storage_options"]["storage_mode"])
        self.assertEqual("sync", audit["storage_options"]["oplog_mode"])

    def test_invalid_storage_options_are_rejected(self) -> None:
        server = self._server()
        with self.assertRaises(Exception):
            server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "hello"}],
                    "storage_options": {"oplog_mode": "fast"},
                },
            )

        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        with self.assertRaisesRegex(Exception, "exactly one storage_family"):
            core_mod.normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "storage_mode": "shared_store"}}
            )

    def test_live_backend_rejects_unconfigured_storage_family(self) -> None:
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        adapter = object.__new__(temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        adapter._supported_storage_families = {"default", "shared_store"}
        with self.assertRaisesRegex(Exception, "not configured"):
            adapter._validate_storage_routes_available(
                [{"record_type": "context_event", "storage_route": {"storage_family": "raft"}}]
            )


    def test_debug_lineage_serving_pack_still_omits_raw_candidate_fields(self) -> None:
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        pack = context_pack_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "debug-lineage-pack",
                "selected_refs": [
                    {
                        "ref_type": "entity",
                        "text": "debug profile decision",
                        "ref_hash": 123,
                        "node_hash": 456,
                        "matched_index_terms": ["entity:debug"],
                        "score": 0.91,
                        "token_estimate": 7,
                        "memory_scope": "user_profile",
                        "source_roles": ["llm", "tool"],
                        "source_role_counts": {"llm": 2, "tool": 1},
                        "source_hook_type_counts": {"hook_boundary": 3},
                        "source_codex_event_counts": {"Stop": 3},
                    }
                ],
                "used_context_tokens": 3,
            },
            include_debug=True,
        )

        item = pack["groups"][0]["items"][0]
        self.assertEqual({"assistant": 2, "tool": 1}, item["source_role_counts"])
        for field in ["ref_hash", "node_hash", "matched_index_terms", "score", "token_estimate"]:
            self.assertNotIn(field, item)

    def test_flat_debug_lineage_serving_pack_still_omits_raw_candidate_fields(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving_flat(
            {
                "context_pack_id": "flat-debug-lineage-pack",
                "selected_refs": [
                    {
                        "ref_type": "entity",
                        "text": "debug profile decision",
                        "ref_hash": 123,
                        "node_hash": 456,
                        "matched_index_terms": ["entity:debug"],
                        "score": 0.91,
                        "token_estimate": 7,
                        "memory_scope": "user_profile",
                        "source_roles": ["llm", "tool"],
                        "source_role_counts": {"llm": 2, "tool": 1},
                        "source_hook_type_counts": {"hook_boundary": 3},
                        "source_codex_event_counts": {"Stop": 3},
                    }
                ],
                "used_context_tokens": 3,
            },
            include_debug=True,
        )

        item = pack["selected_refs"][0]
        self.assertEqual({"assistant": 2, "tool": 1}, item["source_role_counts"])
        for field in ["ref_hash", "node_hash", "matched_index_terms", "score", "token_estimate"]:
            self.assertNotIn(field, item)



if __name__ == "__main__":
    unittest.main()


