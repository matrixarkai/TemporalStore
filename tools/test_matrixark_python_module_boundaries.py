#!/usr/bin/env python3
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


class MatrixArkPythonModuleBoundaryTest(unittest.TestCase):
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
                    "content": "Remember: use Ubuntu /root/src/github-services for all TemporalStore repos.",
                }
            ],
            {"source_event_ids": [123]},
        )

        preferences = [entity for entity in entities if entity.get("entity_type") == "preference"]
        self.assertTrue(preferences)
        self.assertEqual("preference", preferences[0]["entity_name"])
        self.assertIn("/root/src/github-services", preferences[0]["state"])
        self.assertEqual(["123"], preferences[0]["source_refs"])

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
                    "backend": "C++ TemporalStore",
                    "prefix": "matrixark:codex-hook:cpp-live-v2",
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
                    "backend": "C++ TemporalStore",
                    "prefix": "matrixark:codex-hook:cpp-live-v2",
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

    def test_recent_ingestion_report_tracks_extraction_input_role_coverage(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        scope = {"session_id": "codex-real-workflow"}
        raw_rows = [
            {
                "sequence": 1,
                "record": {
                    "record_type": "agent_message",
                    "role": "user",
                    "scope": scope,
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "text": "How does batch extraction use my prompt, assistant answer, and tool result?",
                },
            },
            {
                "sequence": 2,
                "record": {
                    "record_type": "agent_message",
                    "role": "assistant",
                    "scope": scope,
                    "metadata": {"hook_type": "after_llm", "codex_event": "AssistantResponse"},
                    "text": "Decision: extract bounded user, assistant, and tool evidence into memory.",
                },
            },
            {
                "sequence": 3,
                "record": {
                    "record_type": "agent_message",
                    "role": "tool",
                    "scope": scope,
                    "metadata": {"hook_type": "tool_result", "codex_event": "PostToolUse"},
                    "text": "Exit code: 0; Ran 141 tests; OK",
                },
            },
        ]
        serving_rows = [
            {
                "sequence": 10,
                "record": {
                    "record_type": "context_entity",
                    "source_session_ids": ["codex-real-workflow"],
                    "source_role_counts": {"user": 1, "assistant": 1, "tool": 1},
                    "source_hook_type_counts": {"before_llm": 1, "after_llm": 1, "tool_result": 1},
                    "source_codex_event_counts": {
                        "UserPromptSubmit": 1,
                        "AssistantResponse": 1,
                        "PostToolUse": 1,
                    },
                    "entity_type": "tool_evidence",
                },
            }
        ]

        backend = report_mod.summarize_backend("test", "matrixark:test", 3, 0, raw_rows, 1, 0, serving_rows)
        batch = backend["recent_extraction_input_batches"][0]

        self.assertEqual("codex-real-workflow", batch["session_id"])
        self.assertEqual(3, batch["message_count"])
        self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, batch["source_role_counts"])
        self.assertEqual("ok", batch["source_role_coverage_status"])
        self.assertEqual([], backend["extraction_input_coverage_gaps"])
        self.assertIn("matrixark_session_commit", batch["extraction_input_shape"])

    def test_recent_ingestion_report_flags_missing_tool_extraction_coverage(self) -> None:
        report_mod = importlib.import_module("tools.generate_codex_recent_ingestion_workflow_report")
        raw_rows = [
            {
                "sequence": 1,
                "record": {
                    "record_type": "agent_message",
                    "role": "tool",
                    "scope": {"session_id": "codex-tool-gap"},
                    "metadata": {"hook_type": "tool_result", "codex_event": "PostToolUse"},
                    "text": "Exit code: 0; pushed aa12aa17",
                },
            }
        ]
        serving_rows = [
            {
                "sequence": 2,
                "record": {
                    "record_type": "context_entity",
                    "source_session_ids": ["codex-tool-gap"],
                    "source_role_counts": {"user": 1},
                },
            }
        ]

        backend = report_mod.summarize_backend("test", "matrixark:test", 1, 0, raw_rows, 1, 0, serving_rows)

        self.assertEqual("gap", backend["extraction_input_coverage_status"])
        self.assertEqual(
            ["source_role:tool:missing_from_derived_serving_memory"],
            backend["extraction_input_coverage_gaps"][0]["gaps"],
        )

    def test_moduleized_request_runs_pre_retrieval_refresh_and_derives_budgets(self) -> None:
        request_mod = importlib.import_module("tools.matrixark_mcp_retrieve_request")

        class FakeTarget:
            _context_pack_cache_max_entries = 0
            _context_pack_cache_ttl_s = 0
            _retrieval_records_cache_generation = 7

            def __init__(self) -> None:
                self.refresh_request = {}

            def _observe_model_latency(self, *_args: object) -> None:
                return None

            def refresh_summaries(self, request: dict[str, object]) -> dict[str, object]:
                self.refresh_request = request
                return {
                    "refreshed_count": 1,
                    "compression_created_count": 0,
                    "skipped_dirty_count": 0,
                    "refreshed": [
                        {
                            "record_type": "context_summary",
                            "summary_hash": 101,
                            "node_hash": 202,
                            "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
                            "summary_text": "assistant decision retained as bounded profile memory",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u"},
                        }
                    ],
                }

        target = FakeTarget()
        request = request_mod.prepare_retrieval_request(
            target,
            {
                "query": "latest assistant decision and tool status",
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                "max_context_tokens": 2000,
                "pre_retrieval_summary_refresh": True,
                "pre_retrieval_summary_refresh_limit": 3,
                "ranking": {
                    "source_role_budget_mode": "auto",
                    "memory_selection_policy_budget_mode": "auto",
                },
            },
            started_perf=0.0,
        )

        self.assertEqual(3, target.refresh_request["limit"])
        self.assertEqual("refreshed", request["pre_retrieval_summary_refresh"]["status"])
        self.assertEqual(1, request["pre_retrieval_summary_refresh"]["refreshed_count"])
        self.assertEqual("auto", request["source_role_budget_mode"])
        self.assertIn("assistant", request["source_role_budget_tokens"])
        self.assertEqual("pre_retrieval_summary_refresh_balanced", request["memory_layer_budget_mode"])
        self.assertIn("pending_async_event", request["memory_layer_budget_tokens"])
        self.assertIn("profile_entity", request["memory_layer_budget_tokens"])
        self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
        self.assertEqual(
            {
                "selected_user_prompt": 760,
                "selected_assistant_decision_outcome_only": 855,
                "selected_tool_evidence_only": 570,
            },
            request["memory_selection_policy_budget_tokens"],
        )
        self.assertEqual(1, len(request["pre_retrieval_refreshed_records"]))

    def test_moduleized_runtime_merges_pre_refreshed_summaries(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")
        base = {
            "record_type": "context_summary",
            "summary_hash": 101,
            "node_hash": 202,
            "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
            "summary_text": "old profile summary",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u"},
        }
        refreshed = {
            **base,
            "summary_hash": 303,
            "node_hash": 404,
            "summary_text": "new profile summary from assistant and tool evidence",
        }

        class FakeTarget:
            def read_all(self) -> list[dict[str, object]]:
                return [base, refreshed]

        merged = helper_mod.merge_refreshed_summary_records(
            FakeTarget(),
            [base.copy()],
            retrieval_scope={"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "current"},
            refreshed_records=[refreshed],
            refresh={"enabled": True, "refreshed_count": 1},
        )
        identities = {(record.get("summary_hash"), record.get("node_hash")) for record in merged}
        self.assertEqual({(101, 202), (303, 404)}, identities)

    def test_moduleized_summary_candidate_preserves_profile_memory_fields(self) -> None:
        builders = importlib.import_module("tools.matrixark_mcp_retrieve_candidate_builders")
        continuity = importlib.import_module("tools.matrixark_mcp_retrieve_continuity")
        record = {
            "record_type": "context_summary",
            "summary_hash": 101,
            "node_hash": 202,
            "node_path": ["tenant:tenant", "user:user", "profile:long_term_memory"],
            "summary_text": "assistant_decision: delayed rollout backfill commits profile memory",
            "summary_type": "node_l0",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "extraction_phase": "final",
            "final_session_boundary": True,
            "source_roles": ["assistant"],
            "source_role_counts": {"assistant": 2},
            "source_hook_types": ["session_commit"],
            "source_hook_type_counts": {"session_commit": 1},
            "source_codex_events": ["PreviousAssistantBackfill"],
            "source_codex_event_counts": {"PreviousAssistantBackfill": 1},
            "source_memory_scopes": ["user_profile"],
            "source_session_continuities": ["cross_session"],
            "source_extraction_phases": ["final"],
            "source_final_session_boundary_count": 1,
            "scope": {
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "current_session",
            },
            "updated_at_ms": 123,
        }
        candidate = builders.summary_candidate(
            record,
            summary_type="node_l0",
            index_terms={"entity_type:assistant_decision"},
            origin_score=0.9,
            keyword_score=2,
            sparse_score=0.5,
            embedding_score=0.4,
            node_score=0.3,
            text=record["summary_text"],
        )
        annotated = continuity.annotate_session_continuity(
            candidate,
            record,
            retrieval_scope={
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "current_session",
                "_session_scope": "prefer",
            },
            question_type="broad_exploration",
        )

        self.assertEqual("summary", annotated["ref_type"])
        self.assertEqual("user_profile", annotated["memory_scope"])
        self.assertEqual("cross_session", annotated["session_continuity"])
        self.assertTrue(annotated["final_session_boundary"])
        self.assertEqual(["assistant"], annotated["source_roles"])
        self.assertEqual({"assistant": 2}, annotated["source_role_counts"])
        self.assertEqual(["PreviousAssistantBackfill"], annotated["source_codex_events"])
        self.assertEqual({"PreviousAssistantBackfill": 1}, annotated["source_codex_event_counts"])
        self.assertEqual(1, annotated["source_final_session_boundary_count"])


    def test_python_and_rust_retrieval_drop_reasons_stay_in_parity(self) -> None:
        python_source = (TOOLS_DIR / "matrixark_mcp_budget_pack.py").read_text(encoding="utf-8")
        rust_telemetry_source = (
            REPO_ROOT
            / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy/matrixark_rust_proxy_retrieve_telemetry.rs"
        ).read_text(encoding="utf-8")
        rust_proxy_source = (
            REPO_ROOT / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy_impl.rs"
        ).read_text(encoding="utf-8")
        required_reasons = {
            "over_budget",
            "cross_session_budget",
            "cross_session_session_cap",
            "cross_session_candidate_cap",
            "entity_bridge_slot_reserved",
            "low_score",
        }

        python_drop_reasons = set(re.findall(r'"([a-z_]+)": "candidate[^"]+"', python_source))
        python_drop_reasons.update(re.findall(r'"(cross_session_[a-z_]+|entity_bridge_slot_reserved|over_budget|low_score)"', python_source))
        rust_drop_reasons = set(re.findall(r'"([a-z_]+)": dropped\.', rust_telemetry_source))
        rust_drop_reasons.update(re.findall(r'"([a-z_]+)": dropped_', rust_proxy_source))

        self.assertTrue(required_reasons <= python_drop_reasons, sorted(required_reasons - python_drop_reasons))
        self.assertTrue(required_reasons <= rust_drop_reasons, sorted(required_reasons - rust_drop_reasons))
        self.assertIn(
            "candidate was skipped to preserve a minimum cross-session entity bridge slot",
            python_source,
        )

    def test_dashboard_helper_exposes_async_pipeline_and_summary_refresh_tables(self) -> None:
        dashboard_mod = importlib.import_module("tools.matrixark_mcp_dashboard")
        scope = {
            "account_id": "acct",
            "tenant_id": "tenant",
            "user_id": "user",
            "session_id": "session",
        }

        class Adapter:
            def read_all(self):
                return [
                    {
                        "record_type": "context_batch_commit",
                        "scope": scope,
                        "commit_id_hash": 11,
                        "batch_id_hash": 12,
                        "trigger_policy": "threshold",
                        "summary_refresh": {
                            "status": "dirty_marked",
                            "dirty_hashes": [21, 22],
                            "session_dirty_hashes": [21],
                            "profile_dirty_hashes": [22],
                            "profile_summary_refresh_required": True,
                        },
                        "memory_layers_written": {"profile_entities": 1},
                        "created_at_ms": 100,
                    },
                    {
                        "record_type": "context_summary_dirty",
                        "scope": scope,
                        "dirty_node_hash": 22,
                        "dirty_reason": "profile_entity_promoted",
                        "updated_at_ms": 101,
                    },
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "scope": scope,
                        "task_hash": 31,
                        "event_id_hash": 41,
                        "commit_id_hash": 11,
                        "status": "extraction_committed",
                        "completed_stages": ["extraction"],
                        "remaining_stages": ["summary", "compression", "embedding"],
                        "trigger_policy": "threshold",
                        "summary_refresh_status": "dirty_marked",
                        "memory_layers_written": {"profile_entities": 1},
                        "updated_at_ms": 102,
                    },
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "scope": scope,
                        "task_hash": 31,
                        "event_id_hash": 41,
                        "commit_id_hash": 11,
                        "status": "summary_completed",
                        "completed_stages": ["extraction", "summary", "embedding", "compression"],
                        "remaining_stages": [],
                        "trigger_policy": "threshold",
                        "summary_completed": True,
                        "embedding_completed": True,
                        "compression_completed": True,
                        "updated_at_ms": 103,
                    },
                ]

        summary = dashboard_mod.ingestion_dashboard(Adapter(), {"scope": scope, "table": "summary_refresh"})
        self.assertEqual(2, summary["total"])
        self.assertEqual("dirty_marked", summary["rows"][1]["summary_refresh_status"])
        self.assertTrue(summary["rows"][1]["profile_summary_refresh_required"])
        self.assertEqual(1, summary["totals"]["async_pipeline"])

        pipeline = dashboard_mod.ingestion_dashboard(Adapter(), {"scope": scope, "table": "async_pipeline"})
        self.assertEqual(1, pipeline["total"])
        self.assertEqual("summary_completed", pipeline["rows"][0]["status"])
        self.assertFalse(pipeline["rows"][0]["summary_pending"])
        self.assertFalse(pipeline["rows"][0]["compression_pending"])
        self.assertFalse(pipeline["rows"][0]["embedding_pending"])

    def test_direct_tools_path_imports_still_work_for_script_launches(self) -> None:
        sys.path.insert(0, str(TOOLS_DIR))
        try:
            server_mod = importlib.import_module("matrixark_mcp_server")
            self.assertTrue(hasattr(server_mod, "MatrixArkMcpServer"))
            self.assertTrue(hasattr(server_mod, "MatrixArkTemporalStoreDirectAdapter"))
        finally:
            try:
                sys.path.remove(str(TOOLS_DIR))
            except ValueError:
                pass

    def test_modular_async_ingest_reports_session_buffer_readiness(self) -> None:
        async_mod = importlib.import_module("tools.matrixark_mcp_async_ingest")

        class Batch:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class Target:
            def __init__(self) -> None:
                self.records = []
                self.pending = []
                self.commit_calls = []

            def auto_batch_extract_enabled(self, args, *, kind):
                return bool(args.get("auto_batch_extract"))

            def default_session_node_path(self, scope):
                return ["tenant:t", "user:u", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **kwargs):
                return {"created": True}

            def write_batch(self, _name):
                return Batch()

            def mark_node_summary_dirty(self, **_kwargs):
                return [10]

            def append(self, record):
                self.records.append(record)

            def session_buffer_enabled(self, args, *, kind):
                return True

            def append_session_buffer_event(self, **kwargs):
                self.pending.append({"event_id_hash": kwargs["event_id_hash"], "envelope": kwargs["envelope"]})

            def pending_session_events(self, _scope):
                return list(self.pending)

            def session_boundary_commit_requested(self, args, *, hook=None):
                return False

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append(args)
                self.pending.clear()
                return {"status": "committed", "trigger_policy": "threshold"}

        target = Target()
        envelope = {
            "kind": "message",
            "scope": {
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "session",
            },
            "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
            "messages": [{"role": "user", "content": "first live message"}],
            "ingestion_time_ms": 123,
            "storage_options": {},
            "storage_route": {},
        }
        args = {"async_processing": True, "auto_batch_extract": True, "session_buffer_threshold": 2}

        first = async_mod.lightweight_async_accept(target, args, envelope=envelope, hook=None, idle_commit_result={})
        self.assertFalse(first["session_buffer"]["threshold_ready"])
        self.assertFalse(first["session_buffer"]["idle_ready"])
        self.assertEqual(1, first["session_buffer"]["pending_event_count"])
        self.assertEqual(1, first["session_buffer"]["pending_message_count"])
        self.assertEqual({"user": 1}, first["session_buffer"]["source_role_counts"])
        self.assertEqual({"before_llm": 1}, first["session_buffer"]["source_hook_type_counts"])
        self.assertEqual({"UserPromptSubmit": 1}, first["session_buffer"]["source_codex_event_counts"])
        first_event = next(record for record in target.records if record["record_type"] == "context_event")
        first_task = next(record for record in target.records if record["record_type"] == "matrixark_async_pipeline_task")
        self.assertEqual({"user": 1}, first_event["source_role_counts"])
        self.assertEqual({"before_llm": 1}, first_event["source_hook_type_counts"])
        self.assertEqual({"UserPromptSubmit": 1}, first_event["source_codex_event_counts"])
        self.assertEqual(first_event["source_role_counts"], first_task["source_role_counts"])
        self.assertEqual(first_event["source_hook_type_counts"], first_task["source_hook_type_counts"])
        self.assertEqual(first_event["source_codex_event_counts"], first_task["source_codex_event_counts"])
        self.assertIsNone(first["auto_batch_extract_result"])

        second_envelope = {**envelope, "messages": [{"role": "assistant", "content": "second live message"}], "ingestion_time_ms": 124}
        second = async_mod.lightweight_async_accept(target, args, envelope=second_envelope, hook=None, idle_commit_result={})
        self.assertTrue(second["session_buffer"]["threshold_ready"])
        self.assertFalse(second["session_buffer"]["idle_ready"])
        self.assertEqual(2, second["session_buffer"]["pending_event_count"])
        self.assertEqual(2, second["session_buffer"]["pending_message_count"])
        self.assertEqual("committed", second["auto_batch_extract_result"]["status"])
        self.assertEqual(1, len(target.commit_calls))

        target = Target()
        multi_message_envelope = {
            **envelope,
            "metadata": {
                "source_role_counts": {"user": 1, "assistant": 1, "tool": 2},
                "source_hook_type_counts": {"after_llm": 1, "hook_boundary": 1},
                "source_codex_event_counts": {"PostToolUse": 2, "Stop": 1},
            },
            "messages": [
                {"role": "user", "content": "prompt with enough context"},
                {"role": "assistant", "content": "assistant decision to remember"},
            ],
            "ingestion_time_ms": 125,
        }
        multi = async_mod.lightweight_async_accept(
            target,
            args,
            envelope=multi_message_envelope,
            hook=None,
            idle_commit_result={},
        )
        self.assertEqual(1, multi["session_buffer"]["pending_event_count"])
        self.assertEqual(2, multi["session_buffer"]["pending_message_count"])
        self.assertEqual({"assistant": 1, "tool": 2, "user": 1}, multi["session_buffer"]["source_role_counts"])
        self.assertEqual({"after_llm": 1, "hook_boundary": 1}, multi["session_buffer"]["source_hook_type_counts"])
        self.assertEqual({"PostToolUse": 2, "Stop": 1}, multi["session_buffer"]["source_codex_event_counts"])
        multi_event = next(record for record in target.records if record["record_type"] == "context_event")
        self.assertEqual(multi["session_buffer"]["source_role_counts"], multi_event["source_role_counts"])
        self.assertEqual(multi["session_buffer"]["source_hook_type_counts"], multi_event["source_hook_type_counts"])
        self.assertEqual(multi["session_buffer"]["source_codex_event_counts"], multi_event["source_codex_event_counts"])
        self.assertTrue(multi["session_buffer"]["threshold_ready"])
        self.assertEqual("committed", multi["auto_batch_extract_result"]["status"])

        target = Target()
        idle_result = {"status": "committed", "trigger_policy": "idle_timeout", "committed_event_count": 1}
        idle_visible = async_mod.lightweight_async_accept(
            target,
            {**args, "session_buffer_threshold": 20},
            envelope={**envelope, "messages": [{"role": "assistant", "content": "fresh event after idle flush"}], "ingestion_time_ms": 126},
            hook=None,
            idle_commit_result=idle_result,
        )
        self.assertTrue(idle_visible["session_buffer"]["idle_ready"])
        self.assertFalse(idle_visible["session_buffer"]["threshold_ready"])
        self.assertEqual(idle_result, idle_visible["auto_batch_extract_result"])
        self.assertEqual(idle_result, idle_visible["idle_commit_result"])
        self.assertEqual([], target.commit_calls)

    def test_modular_session_runtime_reports_trigger_evidence(self) -> None:
        runtime_mod = importlib.import_module("tools.matrixark_mcp_session_runtime")

        class Adapter:
            def __init__(self) -> None:
                self.pending = []
                self.appended = []
                self.batch_extract_calls = []

            def pending_session_events(self, _scope):
                return list(self.pending)

            def default_session_node_path(self, scope):
                return ["tenant:t", "user:u", f"session:{scope['session_id']}"]

            def read_all(self):
                return []

            def batch_extract(self, args, *, hook=None):
                self.batch_extract_calls.append(args)
                return {
                    "batch_id_hash": 7,
                    "node_hash": 8,
                    "node_path": args["metadata"]["node_path"],
                    "events_written": len(args.get("source_event_ids", [])),
                    "extraction_context_event_count": len(args.get("extraction_context_event_ids", [])),
                    "segments_written": 1,
                    "entities_written": 3,
                    "profile_entities_written": 1,
                    "indexes_written": 5,
                    "summary_refresh": {"status": "dirty_marked", "dirty_hashes": [10, 11]},
                }

            def append(self, record):
                self.appended.append(record)

        scope = {
            "account_id": "acct",
            "tenant_id": "tenant",
            "user_id": "user",
            "session_id": "session",
        }
        adapter = Adapter()
        adapter.pending = [
            {
                "event_id_hash": 1,
                "updated_at_ms": 100,
                "envelope": {
                    "messages": [{"role": "user", "content": "first pending message"}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "ingestion_time_ms": 100,
                },
            }
        ]

        deferred = runtime_mod.session_commit(
            adapter,
            {"scope": scope, "threshold_messages": 2, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("deferred", deferred["status"])
        self.assertFalse(deferred["trigger_evidence"]["threshold_ready"])
        self.assertFalse(deferred["trigger_evidence"]["idle_ready"])
        self.assertEqual(1, deferred["trigger_evidence"]["pending_event_count"])
        self.assertEqual(1, deferred["trigger_evidence"]["pending_message_count"])

        committed = runtime_mod.session_commit(
            adapter,
            {"scope": scope, "threshold_messages": 1, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("committed", committed["status"])
        self.assertTrue(committed["trigger_evidence"]["threshold_ready"])
        self.assertFalse(committed["trigger_evidence"]["idle_ready"])
        self.assertEqual("threshold", committed["trigger_policy"])
        self.assertEqual(1, committed["memory_layers_written"]["context_events"])
        self.assertEqual(1, committed["memory_layers_written"]["segments"])
        self.assertEqual(3, committed["memory_layers_written"]["session_entities"])
        self.assertEqual(1, committed["memory_layers_written"]["profile_entities"])
        self.assertEqual(5, committed["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(2, committed["memory_layers_written"]["summary_dirty_nodes"])
        self.assertEqual("dirty_marked", committed["memory_layers_written"]["summary_refresh_status"])
        self.assertEqual("provisional", committed["memory_layers_written"]["extraction_phase"])
        self.assertEqual({"user": 1}, committed["source_role_counts"])
        self.assertEqual({"before_llm": 1}, committed["source_hook_type_counts"])
        self.assertEqual({"UserPromptSubmit": 1}, committed["source_codex_event_counts"])
        self.assertEqual(committed["trigger_evidence"], adapter.appended[0]["trigger_evidence"])
        self.assertEqual(committed["source_role_counts"], adapter.appended[0]["source_role_counts"])
        self.assertEqual(committed["source_hook_type_counts"], adapter.appended[0]["source_hook_type_counts"])
        self.assertEqual(committed["source_codex_event_counts"], adapter.appended[0]["source_codex_event_counts"])
        async_task = next(record for record in adapter.appended if record["record_type"] == "matrixark_async_pipeline_task")
        self.assertEqual(committed["source_role_counts"], async_task["source_role_counts"])
        self.assertEqual(committed["source_hook_type_counts"], async_task["source_hook_type_counts"])
        self.assertEqual(committed["source_codex_event_counts"], async_task["source_codex_event_counts"])

        multi_adapter = Adapter()
        multi_adapter.pending = [
            {
                "event_id_hash": 2,
                "updated_at_ms": 200,
                "envelope": {
                    "messages": [
                        {"role": "user", "content": "What decision should be remembered?"},
                        {"role": "assistant", "content": "Remember the assistant decision."},
                        {"role": "tool", "content": "Exit code: 0"},
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "ingestion_time_ms": 200,
                },
            }
        ]
        multi_committed = runtime_mod.session_commit(
            multi_adapter,
            {"scope": scope, "threshold_messages": 3, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("committed", multi_committed["status"])
        self.assertEqual(1, multi_committed["pending_event_count"])
        self.assertEqual(3, multi_committed["pending_message_count"])
        self.assertTrue(multi_committed["trigger_evidence"]["threshold_ready"])
        self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, multi_committed["source_role_counts"])
        multi_commit_record = next(
            record for record in multi_adapter.appended if record["record_type"] == "context_batch_commit"
        )
        self.assertEqual(1, multi_commit_record["pending_event_count_before_commit"])
        self.assertEqual(3, multi_commit_record["pending_message_count_before_commit"])
        self.assertEqual(
            ["user", "assistant", "tool"],
            [message["role"] for message in multi_adapter.batch_extract_calls[0]["messages"]],
        )
        self.assertEqual([2], multi_adapter.batch_extract_calls[0]["source_event_ids"])

    def test_modular_batch_extract_promotes_profile_entities(self) -> None:
        batch_mod = importlib.import_module("tools.matrixark_mcp_local_batch_extract_runtime")

        class Adapter:
            def __init__(self) -> None:
                self.records = []
                self.nodes = []
                self.dirty_counter = 0

            def read_all(self):
                return []

            def _observe_model_latency(self, *_args):
                return None

            def default_session_node_path(self, scope):
                return ["tenant:tenant_mod", "user:user_mod", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **kwargs):
                self.nodes.append(kwargs)
                return {"created": True, "node_path": kwargs["node_path"]}

            def find_latest_entity(self, **_kwargs):
                return None

            def append_many(self, records):
                self.records.extend(records)

            def node_summary_dirty_records(self, **kwargs):
                self.dirty_counter += 1
                dirty_hash = 44 + self.dirty_counter
                return [
                    dirty_hash
                ], [
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_path": kwargs["node_path"],
                        "dirty_reason": kwargs["dirty_reason"],
                        "source_ref_type": kwargs["source_ref_type"],
                        "scope": kwargs["scope"],
                    }
                ]

        adapter = Adapter()
        envelope = {
            "kind": "message",
            "scope": {
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
                "session_id": "session_mod",
            },
            "metadata": {
                "source_roles": ["llm", "model", "assistant", "tool"],
                "source_role_counts": {"llm": 1, "model": 1, "assistant": 1, "tool": 2},
                "source_hook_types": ["after_llm", "hook_boundary"],
                "source_hook_type_counts": {"after_llm": 3, "hook_boundary": 2},
                "source_codex_events": ["Stop", "PostToolUse"],
                "source_codex_event_counts": {"Stop": 3, "PostToolUse": 2},
            },
            "messages": [{"role": "llm", "content": "Commit abc123 was pushed."}],
            "ingestion_time_ms": 123,
            "storage_options": {},
            "storage_route": {},
        }
        extraction = {
            "mode": "one_pass",
            "segment_provider": {"name": "deterministic"},
            "classification": "NEW_EVENT",
            "event_type": "memory_update",
            "schema": "matrixark.memory.v1",
            "message_count": 1,
            "token_count_estimate": 6,
            "batch_summary": "Commit abc123 was pushed.",
            "events": [],
            "entities": [
                {
                    "entity_type": "assistant_decision",
                    "entity_name": "commit",
                    "state": "Commit abc123 was pushed.",
                    "confidence": 0.9,
                    "operator": "UPSERT",
                    "source_refs": ["assistant:0"],
                    "field_patches": [],
                }
            ],
            "segments": [
                {
                    "topic": "commit pushed",
                    "coordinate_tuples": [[0, 0]],
                    "message_indexes": [0],
                    "saliency_score": 0.8,
                    "summary_text": "Commit abc123 was pushed.",
                    "text": "Commit abc123 was pushed.",
                    "non_contiguous": False,
                }
            ],
            "indexes": ["entity_type:assistant_decision"],
        }

        with mock.patch.object(batch_mod, "one_pass_memory_extraction", return_value=extraction):
            result = batch_mod.batch_extract_after_start(
                adapter,
                {"skip_prior_context": True},
                {
                    "envelope": envelope,
                    "hook": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "threshold": 1,
                    "derive_from_existing_events": True,
                    "source_event_ids": [101],
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "force": True,
                    "deferred_result": None,
                },
            )

        self.assertEqual(1, result["entities_written"])
        self.assertEqual(1, result["segments_written"])
        self.assertEqual(1, result["profile_entities_written"])
        self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
        self.assertFalse(result["profile_promotion_importance_gate"])
        self.assertTrue(result["profile_promotion_scope_available"])
        self.assertEqual("", result["profile_promotion_blocker"])
        self.assertEqual(1, len(result["profile_promotion_summary"]))
        self.assertGreaterEqual(result["entity_indexes_written"], 14)
        self.assertGreaterEqual(result["indexes_written"], result["entity_indexes_written"] + 1)
        self.assertIn("summary_refresh", result)
        self.assertGreaterEqual(len(result["summary_refresh"]["dirty_hashes"]), 2)
        self.assertTrue(result["summary_refresh"]["session_dirty_hashes"])
        self.assertTrue(result["summary_refresh"]["profile_dirty_hashes"])
        session_entities = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_entity"
            and record.get("memory_scope") == "session"
            and record.get("session_continuity") == "same_session"
        ]
        profile_entities = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_entity"
            and record.get("memory_scope") == "user_profile"
            and record.get("session_continuity") == "cross_session"
        ]
        self.assertEqual(1, len(session_entities))
        self.assertEqual(1, len(profile_entities))
        session_segments = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_segment"
            and record.get("memory_scope") == "session"
            and record.get("session_continuity") == "same_session"
        ]
        self.assertEqual(1, len(session_segments))
        self.assertEqual(["session"], session_segments[0]["source_memory_scopes"])
        self.assertEqual(["same_session"], session_segments[0]["source_session_continuities"])
        self.assertEqual(["final"], session_segments[0]["source_extraction_phases"])
        self.assertEqual(session_entities[0]["source_role_counts"], session_segments[0]["source_role_counts"])
        self.assertEqual(session_entities[0]["source_hook_type_counts"], session_segments[0]["source_hook_type_counts"])
        self.assertEqual(session_entities[0]["source_codex_event_counts"], session_segments[0]["source_codex_event_counts"])
        self.assertEqual(["assistant", "tool"], session_entities[0]["source_roles"])
        self.assertEqual({"assistant": 3, "tool": 2}, session_entities[0]["source_role_counts"])
        self.assertEqual(["after_llm", "hook_boundary"], session_entities[0]["source_hook_types"])
        self.assertEqual({"after_llm": 3, "hook_boundary": 2}, session_entities[0]["source_hook_type_counts"])
        self.assertEqual(["PostToolUse", "Stop"], session_entities[0]["source_codex_events"])
        self.assertEqual({"PostToolUse": 2, "Stop": 3}, session_entities[0]["source_codex_event_counts"])
        self.assertEqual(session_entities[0]["source_role_counts"], profile_entities[0]["source_role_counts"])
        self.assertEqual(session_entities[0]["source_hook_type_counts"], profile_entities[0]["source_hook_type_counts"])
        self.assertEqual(session_entities[0]["source_codex_event_counts"], profile_entities[0]["source_codex_event_counts"])
        self.assertEqual(["session_mod"], profile_entities[0]["source_session_ids"])
        self.assertEqual([session_entities[0]["entity_hash"]], profile_entities[0]["source_entity_hashes"])
        self.assertEqual("always_when_profile_scope_available", profile_entities[0]["profile_promotion_policy"])
        self.assertFalse(profile_entities[0]["profile_promotion_importance_gate"])
        self.assertEqual("", profile_entities[0]["profile_promotion_blocker"])
        self.assertEqual(session_entities[0]["entity_hash"], result["profile_promotion_summary"][0]["source_entity_hash"])
        self.assertEqual(profile_entities[0]["entity_hash"], result["profile_promotion_summary"][0]["profile_entity_hash"])
        self.assertEqual(
            ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            profile_entities[0]["node_path"],
        )
        embeddings = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_embedding"
        ]
        embeddings_by_ref = {
            (record.get("ref_type"), record.get("ref_hash"), record.get("embedding_type")): record
            for record in embeddings
        }
        session_entity_embedding = embeddings_by_ref[
            ("entity", session_entities[0]["entity_hash"], "entity_state")
        ]
        profile_entity_embedding = embeddings_by_ref[
            ("entity", profile_entities[0]["entity_hash"], "entity_state")
        ]
        segment_embedding = embeddings_by_ref[
            ("segment", session_segments[0]["segment_hash"], "segment_text")
        ]
        self.assertEqual("session", session_entity_embedding["memory_scope"])
        self.assertEqual("same_session", session_entity_embedding["session_continuity"])
        self.assertEqual("final", session_entity_embedding["extraction_phase"])
        self.assertTrue(session_entity_embedding["final_session_boundary"])
        self.assertEqual(session_entities[0]["source_event_ids"], session_entity_embedding["source_event_ids"])
        self.assertEqual(session_entities[0]["source_role_counts"], session_entity_embedding["source_role_counts"])
        self.assertEqual("user_profile", profile_entity_embedding["memory_scope"])
        self.assertEqual("cross_session", profile_entity_embedding["session_continuity"])
        self.assertEqual("session", profile_entity_embedding["promoted_from_memory_scope"])
        self.assertEqual(profile_entities[0]["source_session_ids"], profile_entity_embedding["source_session_ids"])
        self.assertEqual(profile_entities[0]["source_entity_hashes"], profile_entity_embedding["source_entity_hashes"])
        self.assertEqual(profile_entities[0]["source_role_counts"], profile_entity_embedding["source_role_counts"])
        self.assertEqual("session", segment_embedding["memory_scope"])
        self.assertEqual("same_session", segment_embedding["session_continuity"])
        self.assertEqual(["session"], segment_embedding["source_memory_scopes"])
        self.assertEqual(["same_session"], segment_embedding["source_session_continuities"])
        self.assertEqual(session_segments[0]["source_event_ids"], segment_embedding["source_event_ids"])
        event_embeddings = [
            record
            for record in embeddings
            if record.get("ref_type") == "event" and record.get("embedding_type") == "event_text"
        ]
        self.assertTrue(event_embeddings)
        self.assertTrue(all(record.get("memory_scope") == "session" for record in event_embeddings))
        self.assertTrue(all(record.get("session_continuity") == "same_session" for record in event_embeddings))
        summary_records = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_summary" and record.get("summary_type") == "batch_l0"
        ]
        self.assertEqual(1, len(summary_records))
        self.assertEqual("session", summary_records[0]["memory_scope"])
        self.assertEqual("same_session", summary_records[0]["session_continuity"])
        summary_embedding = embeddings_by_ref[
            ("summary", summary_records[0]["summary_hash"], "batch_l0")
        ]
        self.assertEqual("session", summary_embedding["memory_scope"])
        self.assertEqual("same_session", summary_embedding["session_continuity"])
        self.assertEqual("final", summary_embedding["extraction_phase"])
        self.assertEqual(summary_records[0]["source_entity_hashes"], summary_embedding["source_entity_hashes"])
        self.assertEqual(summary_records[0]["source_segment_hashes"], summary_embedding["source_segment_hashes"])
        profile_indexes = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_profile_entity"
        ]
        session_indexes = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_entity"
        ]
        session_index_names = {str(record.get("index_name") or "") for record in session_indexes}
        profile_index_names = {str(record.get("index_name") or "") for record in profile_indexes}
        for index_names, memory_scope, session_continuity in [
            (session_index_names, "memory_scope:session", "session_continuity:same_session"),
            (profile_index_names, "memory_scope:user_profile", "session_continuity:cross_session"),
        ]:
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn(memory_scope, index_names)
            self.assertIn(session_continuity, index_names)
            self.assertIn("extraction_phase:final", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("hook_type:after_llm", index_names)
            self.assertIn("hook_type:hook_boundary", index_names)
            self.assertIn("codex_event:posttooluse", index_names)
            self.assertIn("codex_event:stop", index_names)
        dirty_records = [record for record in adapter.records if record.get("record_type") == "context_summary_dirty"]
        self.assertTrue(any(record.get("dirty_reason") == "new_event" for record in dirty_records))
        profile_dirty = [
            record
            for record in dirty_records
            if record.get("dirty_reason") == "profile_entity_promoted"
        ]
        self.assertEqual(1, len(profile_dirty))
        self.assertEqual(
            ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            profile_dirty[0]["node_path"],
        )

    def test_modular_batch_extract_writes_distinct_segments_and_profile_entities(self) -> None:
        batch_mod = importlib.import_module("tools.matrixark_mcp_local_batch_extract_runtime")

        class Adapter:
            def __init__(self) -> None:
                self.records = []
                self.dirty_counter = 0

            def read_all(self):
                return []

            def _observe_model_latency(self, *_args):
                return None

            def default_session_node_path(self, scope):
                return ["tenant:tenant_mod", "user:user_mod", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **kwargs):
                return {"created": True, "node_path": kwargs["node_path"]}

            def find_latest_entity(self, **_kwargs):
                return None

            def append_many(self, records):
                self.records.extend(records)

            def node_summary_dirty_records(self, **kwargs):
                self.dirty_counter += 1
                dirty_hash = 8800 + self.dirty_counter
                return [
                    dirty_hash
                ], [
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_path": kwargs["node_path"],
                        "dirty_reason": kwargs["dirty_reason"],
                        "source_ref_type": kwargs["source_ref_type"],
                        "scope": kwargs["scope"],
                    }
                ]

        adapter = Adapter()
        envelope = {
            "kind": "message",
            "scope": {
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
                "session_id": "session_mod_real",
            },
            "metadata": {},
            "messages": [
                {
                    "role": "user",
                    "content": "Remember: use Ubuntu /root/src/github-services for all TemporalStore repos.",
                }
            ],
            "ingestion_time_ms": 456,
            "storage_options": {},
            "storage_route": {},
        }

        result = batch_mod.batch_extract_after_start(
            adapter,
            {"skip_prior_context": True},
            {
                "envelope": envelope,
                "hook": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                "threshold": 1,
                "derive_from_existing_events": True,
                "source_event_ids": [202],
                "extraction_phase": "final",
                "final_session_boundary": True,
                "force": True,
                "deferred_result": None,
            },
        )

        self.assertGreaterEqual(result["segments_written"], 1)
        self.assertGreaterEqual(result["profile_entities_written"], 1)
        segments = [record for record in adapter.records if record.get("record_type") == "context_segment"]
        self.assertTrue(segments)
        self.assertNotEqual("context_event", segments[0]["record_type"])
        self.assertEqual("context_event", segments[0]["source_record_type"])
        self.assertEqual([202], segments[0]["source_event_ids"])
        self.assertTrue(segments[0]["derived_from_context_events"])
        self.assertIn("segment_origin", segments[0])
        profile_entities = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_entity"
            and record.get("memory_scope") == "user_profile"
            and record.get("session_continuity") == "cross_session"
        ]
        self.assertTrue(profile_entities)
        self.assertEqual(
            ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            profile_entities[0]["node_path"],
        )
        self.assertNotIn("session_id", profile_entities[0]["access_scope"])

    def test_modular_entity_scan_admits_cross_session_profile_bridge(self) -> None:
        scan_mod = importlib.import_module("tools.matrixark_mcp_retrieve_entity_scan")
        record = {
            "record_type": "context_entity",
            "entity_hash": 77,
            "node_hash": 88,
            "node_path": ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            "access_scope": {
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
            },
            "entity_type": "assistant_decision",
            "entity_name": "commit",
            "state": "Commit abc123 was pushed for modular profile retrieval.",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "source_session_ids": ["session-a", "session-b"],
            "source_entity_hashes": [11, 22],
            "source_roles": ["assistant"],
            "source_hook_types": ["hook_boundary"],
            "source_codex_events": ["Stop"],
            "updated_at_ms": 200,
        }

        primary, auxiliary, dropped, matched, reason = scan_mod.scan_entity_candidates(
            [record],
            retrieval_scope={
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
                "session_id": "session-c",
            },
            selected_by_tree=lambda _record: False,
            index_terms_by_batch={},
            index_terms_by_node={},
            index_terms_by_ref={},
            secondary_index_filter_groups=[],
            secondary_index_filter_mode="off",
            admit_candidate_for_node=lambda _record: True,
            query_terms={"commit", "abc123"},
            query_embedding=[],
            entity_embedding_vectors={},
            node_scores={},
            annotate_session_continuity=lambda candidate, _record: candidate,
            ranking={},
            reference_time_ms=300,
            deadline_exceeded=lambda: False,
        )

        self.assertEqual("", reason)
        self.assertEqual(0, dropped)
        self.assertEqual(1, matched)
        self.assertEqual(1, len(primary))
        self.assertGreaterEqual(len(auxiliary), 1)
        candidate = primary[0]
        self.assertEqual("user_profile", candidate["memory_scope"])
        self.assertEqual("cross_session", candidate["session_continuity"])
        self.assertEqual(["session-a", "session-b"], candidate["source_session_ids"])
        self.assertEqual([11, 22], candidate["source_entity_hashes"])
        self.assertEqual("selected as cross-session user-profile entity bridge", candidate["selection_reason"])

    def test_mcp_entrypoint_reexports_split_modules(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        admin_mod = importlib.import_module("tools.matrixark_mcp_admin")
        backends_mod = importlib.import_module("tools.matrixark_mcp_backends")
        dispatch_mod = importlib.import_module("tools.matrixark_mcp_dispatch")
        ingestion_mod = importlib.import_module("tools.matrixark_mcp_ingestion")
        retrieval_mod = importlib.import_module("tools.matrixark_mcp_retrieval")
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")

        self.assertIs(server_mod.MatrixArkServiceMetrics, metrics_mod.MatrixArkServiceMetrics)
        self.assertIs(server_mod.MatrixArkLocalAdapter, local_mod.MatrixArkLocalAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreDirectAdapter, temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreRustAdapter, temporal_mod.MatrixArkTemporalStoreRustAdapter)
        self.assertTrue(ingestion_mod.is_ingestion_tool("matrixark_ingest"))
        self.assertTrue(retrieval_mod.is_retrieval_tool("matrixark_retrieve"))
        self.assertTrue(admin_mod.is_admin_tool("matrixark_management_portal"))
        self.assertTrue(callable(backends_mod.build_mcp_adapter))
        self.assertTrue(callable(dispatch_mod.dispatch_matrixark_tool))
        self.assertTrue(callable(requests_mod.normalize_mcp_tool_request))

    def test_mcp_entrypoint_stays_small(self) -> None:
        server_lines = (TOOLS_DIR / "matrixark_mcp_server.py").read_text(encoding="utf-8").splitlines()
        self.assertLessEqual(len(server_lines), 750)

    def test_public_mcp_modules_avoid_wildcard_imports(self) -> None:
        module_names = [
            "matrixark_mcp_server.py",
            "matrixark_mcp_backends.py",
            "matrixark_mcp_dispatch.py",
            "matrixark_mcp_requests.py",
            "matrixark_mcp_ingestion.py",
            "matrixark_mcp_retrieval.py",
            "matrixark_mcp_admin.py",
        ]
        offenders: list[str] = []
        for module_name in module_names:
            tree = ast.parse((TOOLS_DIR / module_name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
                    offenders.append(f"{module_name}:{node.lineno}")
        self.assertEqual([], offenders)

    def test_pyproject_exposes_matrixark_console_scripts(self) -> None:
        pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('matrixark-mcp-server = "tools.matrixark_mcp_server:main"', pyproject_text)
        self.assertIn('matrixark-http-portal = "tools.matrixark_admin:http_portal_main"', pyproject_text)
        self.assertIn('matrixark-agent-hook = "tools.matrixark_agent_hook:main"', pyproject_text)
        self.assertIn('matrixark-admin = "tools.matrixark_admin:main"', pyproject_text)
        self.assertIn('matrixark-local-recovery = "tools.matrixark_mcp_recovery:main"', pyproject_text)

    def test_production_defaults_doc_records_control_plane_boundary(self) -> None:
        defaults_doc = (REPO_ROOT / "docs" / "matrixark_mcp_production_defaults.md").read_text(encoding="utf-8")
        required_snippets = [
            "Python remains the MCP/HTTP/control-plane layer",
            "Native C++ or Rust MCP servers are future optimizations, not a v1 requirement",
            "C++ and Rust TemporalStore remain the serving engines",
            "compact and audit-light",
            "Full replay/debug audit is opt-in",
            "Cloud mode requires an API key or trusted SSO gateway identity",
            "Local/dev mode may use generated local scope defaults",
            "Codex is the only production-supported hook client today",
            "Claude Code,",
            "Claude Desktop, Cursor, OpenClaw, OpenCode, Aider, Continue, Cline/Roo",
            "visible local context only",
            "before LLM: `matrixark_retrieve`",
            "after answer/tool: `matrixark_ingest`",
            "resource added: import resource or skill",
            "feedback: `matrixark_feedback`",
            "session boundary: `matrixark_session_commit`",
            "PromptSubmit, assistant responses, and selected tool evidence",
            "`extraction_phase=provisional`",
            "`final_session_boundary=false`",
            "Idle timeout triggers the same provisional checkpoint",
            "Stop, SubagentStop, and PostCompact trigger the final session boundary",
            "`extraction_phase=final`",
            "`final_session_boundary=true`",
            "Retrieval keeps visible local context and a safety margin first",
            "Retrieval metrics and debug",
            "ContextPacks are opt-in audit fields",
            "ingest/retrieve QPS",
            "p50/p95/p99 latency",
            "partial ContextPack count",
            "audit write failures",
            "dirty summary lag",
            "resource import lag",
            "model fallback flags",
            "backend readiness",
            "messages;",
            "resources;",
            "skills;",
            "events/entities;",
            "ContextPacks;",
            "users;",
            "API keys;",
            "audit logs.",
            "scoped, redacted, paged tables",
            "no private checkout paths",
            "no local credentials or secrets",
            "no vendored build outputs",
            "reproducible local validation commands",
            "C++/Rust scale matrix gate",
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, defaults_doc)

    def test_metrics_prometheus_contract_includes_production_signals(self) -> None:
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        metrics = metrics_mod.MatrixArkServiceMetrics()
        metrics.observe_operation("ingest", "ok", 12.5)
        metrics.observe_operation("retrieve", "ok", 25.0, timeout=True)
        metrics.observe_retrieve_result({"partial": True, "tokens": {"remote_budget": 100, "remote": 80}})
        metrics.observe_model_latency("embedding", 8.0)
        metrics.update_gauges(dirty_summary_lag_ms=10, resource_import_lag_ms=20, queue_depth=3, audit_write_failures=1)
        metrics.observe_backend_ready(True, "ready")
        metrics.update_model_fallback_flags(embedding=True, reader=False, judge=True)

        prometheus = metrics.render_prometheus(backend="temporalstore-direct", storage_mode="shared_store")
        required_metrics = [
            'matrixark_backend_info{backend="temporalstore-direct",storage_mode="shared_store"} 1',
            "matrixark_backend_ready",
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="ingest"}',
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.5"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.95"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.99"}',
            "matrixark_timeouts_total",
            "matrixark_partial_context_pack_total",
            "matrixark_resource_import_queue_depth",
            "matrixark_audit_write_failures_total",
            "matrixark_dirty_summary_lag_ms",
            "matrixark_resource_import_lag_ms",
            "matrixark_token_pressure_ratio",
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="embedding"} 1',
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="judge"} 1',
            "matrixark_model_latency_ms",
        ]
        for metric in required_metrics:
            self.assertIn(metric, prometheus)

    def test_request_boundary_generates_idempotency_and_validates_storage(self) -> None:
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")
        args = requests_mod.normalize_mcp_tool_request(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                "storage_options": {"route": "raft_async"},
                "agent_name": "codex",
            },
            write_tools={"matrixark_ingest"},
        )
        self.assertRegex(args["idempotency_key"], r"^auto:matrixark_ingest:")
        self.assertEqual("raft", args["storage_options"]["storage_family"])
        self.assertEqual("async", args["storage_options"]["write_mode"])
        self.assertEqual("acct_local", args["scope"]["account_id"])
        self.assertEqual("tenant_codex", args["scope"]["tenant_id"])
        self.assertIn("scope_key", args["scope"])
        self.assertGreater(args["scope"]["tenant_hash"], 0)

        with self.assertRaises(Exception):
            requests_mod.normalize_mcp_tool_request(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "bad"}], "storage_options": {"route": "not_real"}},
                write_tools={"matrixark_ingest"},
            )

    def test_all_mutating_mcp_routes_include_idempotency_boundary(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        self.assertIn("matrixark_auth_sso_login", server_mod.MatrixArkMcpServer.IDEMPOTENT_WRITE_TOOLS)

    def test_core_module_has_no_duplicate_top_level_symbols(self) -> None:
        module_path = TOOLS_DIR / "matrixark_mcp_core.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        seen: dict[str, int] = {}
        duplicates: dict[str, list[int]] = {}
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            previous = seen.get(node.name)
            if previous is not None:
                duplicates.setdefault(node.name, [previous]).append(node.lineno)
            else:
                seen[node.name] = node.lineno
        self.assertEqual({}, duplicates)

    def test_token_selector_stops_on_hard_deadline(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        calls = {"count": 0}

        def deadline_after_first_candidate() -> bool:
            calls["count"] += 1
            return calls["count"] > 1

        candidates = [
            {
                "ref_type": "event",
                "ref_hash": index,
                "score": 0.9 - (index * 0.01),
                "text": f"important answer-bearing context {index}",
            }
            for index in range(5)
        ]
        selected, used_tokens, dropped = core_mod.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=200,
            auxiliary_quota=0,
            deadline_exceeded=deadline_after_first_candidate,
            deadline_reason="test_deadline",
        )
        self.assertEqual(1, len(selected))
        self.assertGreater(used_tokens, 0)
        self.assertTrue(dropped["deadline_exceeded"])
        self.assertEqual("test_deadline", dropped["deadline_reason"])
        self.assertEqual(4, dropped["deadline"])

    def test_serving_pack_lifts_repeated_session_continuity(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-1",
                "selected_refs": [
                    {"ref_type": "event", "text": "same session fact", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "entity", "text": "same session entity", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "entity", "text": "profile entity", "token_estimate": 3, "memory_scope": "user_profile", "session_continuity": "cross_session"},
                    {"ref_type": "summary", "text": "cross session summary", "token_estimate": 4, "session_continuity": "cross_session"},
                ],
                "used_context_tokens": 13,
            }
        )
        self.assertNotIn("selected_refs", pack)
        self.assertNotIn("selected_ref_groups", pack)
        self.assertIn("groups", pack)
        self.assertEqual("same_session", pack["defaults"]["session_continuity"])
        self.assertEqual(2, pack["counts"]["session_continuity"]["same_session"])
        self.assertEqual("session", pack["defaults"]["memory_layer"])
        self.assertEqual(2, pack["counts"]["memory_layer"]["session"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["profile"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["cross_session"])
        event_group = next(group for group in pack["groups"] if group["type"] == "event")
        entity_group = next(group for group in pack["groups"] if group["type"] == "entity")
        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        self.assertNotIn("ref_type", event_group["items"][0])
        self.assertNotIn("session_continuity", event_group["items"][0])
        self.assertNotIn("memory_layer", event_group["items"][0])
        profile_item = next(item for item in entity_group["items"] if item["text"] == "profile entity")
        self.assertEqual("profile", profile_item["memory_layer"])
        self.assertEqual("cross_session", summary_group["items"][0]["session_continuity"])
        self.assertEqual("cross_session", summary_group["items"][0]["memory_layer"])
        self.assertNotIn("tokens", event_group["items"][0])

    def test_serving_pack_hides_summary_cross_session_lineage_by_default(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-2",
                "selected_refs": [
                    {
                        "ref_type": "event",
                        "text": "same-session working note.",
                        "token_estimate": 3,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                    },
                    {
                        "ref_type": "summary",
                        "text": "profile summary: user prefers Ubuntu shared repo folders.",
                        "token_estimate": 9,
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "extraction_phase": "final",
                        "source_session_ids": ["session-a", "session-b"],
                        "source_entity_hashes": [101, 202, 303],
                        "source_roles": ["user", "assistant"],
                        "source_hook_types": ["hook_boundary"],
                        "source_codex_events": ["UserPromptSubmit", "Stop"],
                    }
                ],
                "used_context_tokens": 9,
            }
        )

        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        item = summary_group["items"][0]
        self.assertEqual("user_profile", item["memory_scope"])
        self.assertEqual("profile", item["memory_layer"])
        self.assertEqual("cross_session", item["session_continuity"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["profile"])
        for field in [
            "source_session_ids",
            "source_entity_count",
            "source_roles",
            "source_hook_types",
            "source_codex_events",
            "source_entity_hashes",
        ]:
            self.assertNotIn(field, item)

    def test_current_profile_entity_serving_pack_hides_provenance_by_default(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "entity",
                "text": "preference: repo path = /root/src/github-services/TemporalStore",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "entity_type": "preference",
                "entity_name": "repo path",
                "profile_current_state_boost": 0.18,
                "source_session_ids": [f"session-{index}" for index in range(10)],
                "source_entity_hashes": [101, 202, 303],
                "source_roles": ["llm", "tool"],
                "source_codex_events": ["Stop", "PostToolUse"],
                "source_hook_types": ["hook_boundary"],
            }
        ]

        for compact_refs in [
            core_mod.compact_context_pack_refs,
            context_pack_mod.compact_context_pack_refs,
        ]:
            item = compact_refs(selected)[0]
            self.assertNotIn("source_session_ids", item)
            self.assertNotIn("source_entity_count", item)
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_codex_events", item)
            self.assertNotIn("source_hook_types", item)
            self.assertNotIn("source_entity_hashes", item)

            debug_item = compact_refs(selected, include_debug=True)[0]
            self.assertEqual([f"session-{index}" for index in range(8)], debug_item["source_session_ids"])
            self.assertEqual(3, debug_item["source_entity_count"])
            self.assertEqual(["assistant", "tool"], debug_item["source_roles"])
            self.assertEqual(["Stop", "PostToolUse"], debug_item["source_codex_events"])
            self.assertEqual(["hook_boundary"], debug_item["source_hook_types"])
            self.assertNotIn("source_entity_hashes", debug_item)

    def test_pending_async_event_serving_pack_uses_live_memory_layer_without_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "event",
                "text": "user: remember this live hook message",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "event_type": "pending_async",
                "classification": "PENDING_ASYNC_EXTRACTION",
                "extraction_phase": "pending_async",
                "source_roles": ["user"],
                "source_hook_types": ["before_llm"],
                "source_codex_events": ["UserPromptSubmit"],
                "source_role_counts": {"user": 1},
            }
        ]

        for pack_groups in [
            core_mod.serving_ref_groups_for_pack,
            lambda refs: context_pack_mod.serving_ref_groups_for_pack(refs, include_debug=False),
        ]:
            groups = pack_groups(selected)
            item = groups[0]["items"][0]

            self.assertEqual("pending_async", item["memory_layer"])
            self.assertEqual("pending_async", item["extraction_phase"])
            self.assertEqual("session", item["memory_scope"])
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_hook_types", item)
            self.assertNotIn("source_codex_events", item)
        self.assertEqual({"pending_async": 1}, core_mod.memory_layer_counts(selected))
        self.assertEqual({"pending_async": 1}, context_pack_mod._memory_layer_counts(selected))

    def test_profile_entity_serving_pack_keeps_profile_layer_without_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "entity",
                "context_class": "profile_entity",
                "text": "repo preference: use /root/src/github-services for TemporalStore work",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "entity_type": "preference",
                "entity_name": "repo location",
                "source_session_ids": ["codex:thread-a", "codex:thread-b"],
                "source_roles": ["user", "llm"],
                "source_hook_types": ["before_llm", "stop"],
            }
        ]

        for pack_groups in [
            core_mod.serving_ref_groups_for_pack,
            lambda refs: context_pack_mod.serving_ref_groups_for_pack(refs, include_debug=False),
        ]:
            item = pack_groups(selected)[0]["items"][0]

            self.assertEqual("profile", item["memory_layer"])
            self.assertEqual("user_profile", item["memory_scope"])
            self.assertEqual("cross_session", item["session_continuity"])
            self.assertEqual("repo location", item["entity"])
            self.assertNotIn("source_session_ids", item)
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_hook_types", item)
        self.assertEqual({"profile": 1}, core_mod.memory_layer_counts(selected))
        self.assertEqual({"profile": 1}, context_pack_mod._memory_layer_counts(selected))

    def test_hot_context_event_records_are_first_class_session_memory(self) -> None:
        records_mod = importlib.import_module("tools.matrixark_mcp_ingest_message_records")
        envelope = {
            "kind": "message",
            "messages": [
                {
                    "role": "user",
                    "content": "why is context event missing from serving memory?",
                }
            ],
            "scope": {
                "tenant_id": "tenant_hot",
                "user_id": "user_hot",
                "session_id": "codex:thread-hot",
            },
            "metadata": {
                "source_hook_type_counts": {"before_llm": 1},
                "source_codex_event_counts": {"UserPromptSubmit": 1},
            },
            "ingestion_time_ms": 1780000000000,
        }
        extraction = {
            "classification": "PENDING_ASYNC_EXTRACTION",
            "event_type": "pending_async",
            "status": "observed",
        }

        event = records_mod.context_event_record(
            event_id_hash=101,
            node_hash=202,
            node_path=["tenant:tenant_hot", "user:user_hot", "session:codex:thread-hot"],
            text="user: why is context event missing from serving memory?",
            extraction=extraction,
            envelope=envelope,
            prior_context={},
            hook={"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
        )
        index_terms = records_mod.context_event_index_terms(
            extraction=extraction,
            text=event["text"],
            envelope=envelope,
        )
        indexes = records_mod.context_event_index_records(
            index_terms=index_terms,
            event_id_hash=101,
            node_hash=202,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )

        self.assertEqual("context_event", event["record_type"])
        self.assertEqual("session", event["memory_scope"])
        self.assertEqual("same_session", event["session_continuity"])
        self.assertEqual("pending_async", event["extraction_phase"])
        self.assertEqual(envelope["scope"], event["scope"])
        self.assertEqual(envelope["scope"], event["access_scope"])
        self.assertEqual({"user": 1}, event["source_role_counts"])
        self.assertEqual({"before_llm": 1}, event["source_hook_type_counts"])
        self.assertEqual({"UserPromptSubmit": 1}, event["source_codex_event_counts"])
        self.assertEqual(1780000000000, event["updated_at_ms"])
        self.assertIn("memory_scope:session", index_terms)
        self.assertIn("session_continuity:same_session", index_terms)
        self.assertTrue(all(record["memory_scope"] == "session" for record in indexes))
        self.assertTrue(all(record["session_continuity"] == "same_session" for record in indexes))
        self.assertTrue(all(record["extraction_phase"] == "pending_async" for record in indexes))

    def test_shared_pack_builder_exposes_memory_layer_budget_and_pressure(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        selected = [
            {
                "ref_type": "entity",
                "ref_hash": 10,
                "text": "tool_evidence: tests = Exit code: 0 Ran 87 tests OK",
                "token_estimate": 8,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "entity_type": "tool_evidence",
                "source_roles": ["tool"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["PostToolUse"],
            }
        ]
        pack = builder_mod.build_context_pack(
            context_pack_id=123,
            selected=selected,
            local_budget={"items": [], "tokens": 0},
            serving_selected=selected,
            dropped_over_budget={},
            serving_dropped=[],
            layer_scores=[],
            question_type="fact",
            query_plan={},
            retrieval_session_scope="prefer",
            cross_session_policy={"enabled": True},
            shared_context_policy={"enabled": True},
            retrieval_scan_stats={},
            ranking={},
            min_similarity_score=0.2,
            max_global_candidates=24,
            max_selected_refs=8,
            budget_fill_policy="quality_first",
            traversal={},
            top_k_per_layer=4,
            max_children_scored_per_parent=16,
            hard_max_children_scored_per_parent=64,
            max_candidates_per_node=8,
            max_raw_events_per_node=4,
            selected_node_hashes=set(),
            selected_paths=set(),
            tree_candidate_records_count=1,
            tree_prefilter_dropped_count=0,
            fanout_dropped_count=0,
            raw_event_time_window_dropped_count=0,
            secondary_index_filter_groups=[],
            secondary_index_matched_count=0,
            secondary_index_dropped_count=0,
            secondary_index_filter_mode="off",
            rerank_policy={},
            time_weighted_recall={"freshness_tolerance_ms": 0, "half_life_ms": 0},
            reinforcement={},
            auxiliary_quota=0.0,
            storage_options={},
            deadline_ms=0,
            started_perf=0.0,
            partial_context_pack=False,
            primary_candidate_count=1,
            auxiliary_candidate_count=0,
            used_context_tokens=8,
            local_tokens=0,
            remote_context_budget_tokens=100,
            max_context_tokens=100,
            safety_margin_tokens=0,
            budget_source="test",
            quality_warnings=[],
            audit_mode="off",
            audit_sample_rate=0.0,
            source_role_budget_tokens={"tool": 32},
            source_role_budget_mode="auto",
            memory_layer_budget_tokens={"profile_entity": 48},
            memory_layer_budget_mode="auto",
            memory_selection_policy_budget_tokens={"selected_tool_evidence_only": 40},
            memory_selection_policy_budget_mode="auto",
            debug_refs=False,
        )
        source_role_policy = pack["recall_policy"]["source_role_budget_policy"]
        self.assertTrue(source_role_policy["enabled"])
        self.assertEqual("auto", source_role_policy["mode"])
        self.assertEqual(100, source_role_policy["remote_budget_tokens"])
        self.assertTrue(source_role_policy["derived"])
        self.assertTrue(source_role_policy["independent_caps"])
        self.assertTrue(source_role_policy["global_remote_budget_enforced"])
        self.assertEqual("independent_per_role_caps_under_global_remote_budget", source_role_policy["budget_semantics"])
        self.assertEqual({"tool": 32}, source_role_policy["budget_tokens"])
        memory_policy = pack["recall_policy"]["memory_layer_budget_policy"]
        self.assertTrue(memory_policy["enabled"])
        self.assertEqual("auto", memory_policy["mode"])
        self.assertEqual("fact", memory_policy["question_type"])
        self.assertEqual(100, memory_policy["remote_budget_tokens"])
        self.assertEqual({"profile_entity": 48}, memory_policy["budget_tokens"])
        selection_policy = pack["recall_policy"]["memory_selection_policy_budget_policy"]
        self.assertTrue(selection_policy["enabled"])
        self.assertEqual("auto", selection_policy["mode"])
        self.assertEqual(100, selection_policy["remote_budget_tokens"])
        self.assertTrue(selection_policy["independent_caps"])
        self.assertTrue(selection_policy["global_remote_budget_enforced"])
        self.assertEqual(
            "independent_per_memory_selection_policy_caps_under_global_remote_budget",
            selection_policy["budget_semantics"],
        )
        self.assertEqual({"selected_tool_evidence_only": 40}, selection_policy["budget_tokens"])
        layer_budget = pack["recall_policy"]["memory_layer_budget"]
        self.assertEqual(1, layer_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, layer_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, layer_budget["by_ref_type"]["entity"]["refs"])
        self.assertEqual(1, layer_budget["by_entity_type"]["tool_evidence"]["refs"])
        self.assertEqual(1, layer_budget["by_source_role"]["tool"]["refs"])
        self.assertEqual(1, layer_budget["by_hook_type"]["hook_boundary"]["refs"])
        self.assertEqual(1, layer_budget["by_codex_event"]["PostToolUse"]["refs"])
        self.assertEqual(1, layer_budget["final_session_boundary_ref_count"])
        layer_pressure = pack["recall_policy"]["memory_layer_pressure"]
        self.assertEqual(1, layer_pressure["selected_refs"])
        self.assertEqual(8, layer_pressure["selected_tokens"])
        self.assertEqual(0, layer_pressure["dropped_refs"])
        self.assertEqual(0, layer_pressure["dropped_tokens"])
        self.assertFalse(layer_pressure["profile_memory_pressure"])
        self.assertFalse(layer_pressure["cross_session_pressure"])
        self.assertIn("by_memory_scope", layer_pressure["by_dimension"])
        self.assertEqual(
            1,
            layer_pressure["by_dimension"]["by_memory_scope"]["user_profile"]["selected_refs"],
        )

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget={},
            records=[],
        )
        metrics_budget = pack["retrieval_metrics"]["memory_layer_budget"]
        self.assertEqual(1, metrics_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, metrics_budget["by_entity_type"]["tool_evidence"]["refs"])
        self.assertNotIn("by_source_role", metrics_budget)
        self.assertNotIn("by_hook_type", metrics_budget)
        self.assertNotIn("by_codex_event", metrics_budget)
        self.assertNotIn("source_message_counts_by_role", metrics_budget)
        metrics_pressure = pack["retrieval_metrics"]["memory_layer_pressure"]
        self.assertEqual(1, metrics_pressure["selected_refs"])
        self.assertNotIn("by_source_role", metrics_pressure.get("by_dimension", {}))
        self.assertNotIn("tool_source_message_pressure", metrics_pressure)

    def test_modular_budget_pack_caps_memory_selection_policy(self) -> None:
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        candidates = [
            {
                "ref_type": "entity",
                "ref_hash": 101,
                "text": "assistant decided to push the rollout after validation",
                "score": 0.99,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 1
                },
            },
            {
                "ref_type": "entity",
                "ref_hash": 202,
                "text": "tool evidence shows validation passed",
                "score": 0.8,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_memory_selection_policy_counts": {
                    "selected_tool_evidence_only": 1
                },
            },
        ]

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            max_selected_refs=2,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 2,
                "max_candidates": 2,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 1
            },
            budget_fill_policy="quality_first",
        )

        self.assertGreater(used_tokens, 0)
        self.assertEqual([202], [ref["ref_hash"] for ref in selected])
        self.assertEqual(["selected_tool_evidence_only"], selected[0]["budget_memory_selection_policies"])
        self.assertEqual(1, dropped["memory_selection_policy_budget"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            dropped["memory_selection_policy_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 0},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )
        self.assertEqual(
            8,
            dropped["estimated_tokens"]["memory_selection_policy_budget"],
        )

    def test_modular_budget_pack_force_fill_respects_memory_selection_policy_cap(self) -> None:
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        assistant_candidate = {
            "ref_type": "summary",
            "ref_hash": 303,
            "text": "assistant decision rollout evidence should not bypass retrieval caps",
            "score": 0.99,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
        }

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [assistant_candidate],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 8,
                "max_sessions": 1,
                "max_candidates": 1,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 1
            },
            budget_fill_policy="force_fill",
        )

        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["cross_session_budget"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 0},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [assistant_candidate],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 8,
                "max_sessions": 1,
                "max_candidates": 1,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 8
            },
            budget_fill_policy="force_fill",
        )

        self.assertEqual([303], [ref["ref_hash"] for ref in selected])
        self.assertEqual(
            used_tokens,
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"][
                "selected_assistant_decision_outcome_only"
            ],
        )
        self.assertEqual(
            ["selected_assistant_decision_outcome_only"],
            selected[0]["budget_memory_selection_policies"],
        )

    def test_shared_pack_builder_reports_profile_shadowed_dropped_budget(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        selected = [
            {
                "ref_type": "entity",
                "ref_hash": 22,
                "text": "decision: shared repo path = /root/src/github-services/TemporalStore-memory-next",
                "token_estimate": 9,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "entity_type": "decision",
                "extraction_phase": "final",
                "source_session_ids": ["session-a", "session-b"],
                "source_entity_hashes": [11, 12],
                "profile_current_state_representative": True,
                "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                "current_state_source_session_count": 2,
                "current_state_source_entity_count": 2,
            }
        ]
        dropped = {
            "stale": 1,
            "estimated_tokens": {"stale": 6},
            "refs": [
                {
                    "ref_type": "entity",
                    "ref_hash": 11,
                    "drop_reason": "stale",
                    "token_estimate": 6,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "entity_type": "tool_evidence",
                    "source_roles": ["tool"],
                    "source_hook_types": ["tool_result"],
                    "source_codex_events": ["PostToolUse"],
                    "stale_or_superseded": True,
                    "profile_shadowed_by_ref_hash": 22,
                    "profile_shadowed_reason": "source_entity_lineage",
                }
            ],
        }
        serving_selected, serving_dropped = builder_mod.prepare_serving_refs(
            selected=selected,
            dropped_over_budget=dropped,
            debug_refs=False,
        )
        self.assertTrue(serving_selected[0]["profile_current_state_representative"])
        for field in [
            "current_state_policy",
            "current_state_source_session_count",
            "current_state_source_entity_count",
            "source_entity_count",
        ]:
            self.assertNotIn(field, serving_selected[0])
        pack = builder_mod.build_context_pack(
            context_pack_id=124,
            selected=selected,
            local_budget={"items": [], "tokens": 0},
            serving_selected=serving_selected,
            dropped_over_budget=dropped,
            serving_dropped=serving_dropped,
            layer_scores=[],
            question_type="current_state",
            query_plan={"query_type": "current_state"},
            retrieval_session_scope="prefer",
            cross_session_policy={"enabled": True},
            shared_context_policy={"enabled": True},
            retrieval_scan_stats={},
            ranking={},
            min_similarity_score=0.2,
            max_global_candidates=24,
            max_selected_refs=8,
            budget_fill_policy="quality_first",
            traversal={},
            top_k_per_layer=4,
            max_children_scored_per_parent=16,
            hard_max_children_scored_per_parent=64,
            max_candidates_per_node=8,
            max_raw_events_per_node=4,
            selected_node_hashes=set(),
            selected_paths=set(),
            tree_candidate_records_count=2,
            tree_prefilter_dropped_count=0,
            fanout_dropped_count=0,
            raw_event_time_window_dropped_count=0,
            secondary_index_filter_groups=[],
            secondary_index_matched_count=0,
            secondary_index_dropped_count=0,
            secondary_index_filter_mode="off",
            rerank_policy={},
            time_weighted_recall={"freshness_tolerance_ms": 0, "half_life_ms": 0},
            reinforcement={},
            auxiliary_quota=0.0,
            storage_options={},
            deadline_ms=0,
            started_perf=0.0,
            partial_context_pack=False,
            primary_candidate_count=2,
            auxiliary_candidate_count=0,
            used_context_tokens=9,
            local_tokens=0,
            remote_context_budget_tokens=100,
            max_context_tokens=100,
            safety_margin_tokens=0,
            budget_source="test",
            quality_warnings=[],
            audit_mode="off",
            audit_sample_rate=0.0,
            debug_refs=False,
        )
        dropped_budget = pack["recall_policy"]["dropped_memory_layer_budget"]
        pressure = pack["recall_policy"]["memory_layer_pressure"]
        self.assertEqual(1, dropped_budget["stale_ref_count"])
        self.assertEqual(6, dropped_budget["stale_token_estimate"])
        self.assertEqual(1, dropped_budget["profile_shadowed_ref_count"])
        self.assertEqual(6, dropped_budget["profile_shadowed_token_estimate"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_drop_reason"]["stale"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_memory_scope"]["session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_session_continuity"]["same_session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_entity_type"]["tool_evidence"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_source_role"]["tool"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_hook_type"]["tool_result"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_codex_event"]["PostToolUse"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_profile_shadowed_reason"]["source_entity_lineage"])
        self.assertEqual(1, pressure["selected_refs"])
        self.assertEqual(9, pressure["selected_tokens"])
        self.assertEqual(1, pressure["dropped_refs"])
        self.assertEqual(6, pressure["dropped_tokens"])
        self.assertTrue(pressure["session_memory_pressure"])
        self.assertFalse(pressure["profile_memory_pressure"])
        self.assertFalse(pressure["cross_session_pressure"])
        self.assertIn("by_memory_scope", pressure["dropped_dimensions"])
        self.assertEqual(
            1,
            pressure["by_dimension"]["by_memory_scope"]["session"]["dropped_refs"],
        )

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget=serving_dropped,
            records=[],
        )
        metrics_dropped_budget = pack["retrieval_metrics"]["dropped_memory_layer_budget"]
        self.assertEqual(1, metrics_dropped_budget["by_memory_scope"]["session"]["refs"])
        self.assertEqual(1, metrics_dropped_budget["by_profile_shadowed_reason"]["source_entity_lineage"]["refs"])
        self.assertNotIn("by_source_role", metrics_dropped_budget)
        self.assertNotIn("by_hook_type", metrics_dropped_budget)
        self.assertNotIn("by_codex_event", metrics_dropped_budget)
        metrics_pressure = pack["retrieval_metrics"]["memory_layer_pressure"]
        self.assertEqual(1, metrics_pressure["dropped_refs"])
        self.assertNotIn("by_source_role", metrics_pressure.get("by_dimension", {}))
        self.assertNotIn("tool_source_message_pressure", metrics_pressure)

    def test_memory_layer_pressure_reports_pending_async_event_drops(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")

        pressure = builder_mod.memory_layer_pressure_summary(
            {"total_selected_refs": 1, "by_memory_layer": {"pending_async_event": {"refs": 1, "tokens": 5}}},
            {"total_dropped_refs": 1, "by_memory_layer": {"pending_async_event": {"refs": 1, "tokens": 7}}},
        )

        self.assertTrue(pressure["pending_async_event_pressure"])
        self.assertIn("by_memory_layer", pressure["dropped_dimensions"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_layer"]["pending_async_event"]["dropped_refs"])

    def test_deadline_fallback_hides_memory_layer_lineage_by_default(self) -> None:
        deadline_mod = importlib.import_module("tools.matrixark_mcp_deadline_pack")

        class Adapter:
            def __init__(self) -> None:
                self.audit_records = []

            def append_audit(self, record):
                self.audit_records.append(record)

        adapter = Adapter()
        scope = {
            "account_id": "acct_deadline",
            "tenant_id": "tenant_deadline",
            "user_id": "user_deadline",
            "session_id": "session_now",
        }
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1001,
                "scope": scope,
                "text": "assistant decided to push the local recovery gate after tests passed.",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": "provisional",
                "source_roles": ["llm"],
                "source_role_counts": {"llm": 1},
                "source_hook_types": ["hook_boundary"],
                "source_hook_type_counts": {"hook_boundary": 1},
                "source_codex_events": ["Stop"],
                "source_codex_event_counts": {"Stop": 1},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
            },
            {
                "record_type": "context_entity",
                "entity_hash": 2002,
                "scope": {
                    "account_id": "acct_deadline",
                    "tenant_id": "tenant_deadline",
                    "user_id": "user_deadline",
                },
                "entity_type": "assistant_decision",
                "entity_name": "local_recovery_gate",
                "state": "The user wants recovery and retrieval gates to fail closed.",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_roles": ["model", "assistant"],
                "source_role_counts": {"model": 2, "assistant": 1},
                "source_hook_types": ["hook_boundary"],
                "source_hook_type_counts": {"hook_boundary": 3},
                "source_codex_events": ["Stop"],
                "source_codex_event_counts": {"Stop": 3},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 2},
                "source_session_ids": ["session_old", "session_now"],
                "source_entity_hashes": [10, 11],
            },
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": 3003,
                "event_id_hash": 1001,
                "scope": {
                    "account_id": "acct_deadline",
                    "tenant_id": "tenant_deadline",
                    "user_id": "user_deadline",
                },
                "status": "extraction_committed",
                "completed_stages": ["extraction"],
                "remaining_stages": ["summary", "compression", "embedding"],
                "trigger_policy": "threshold",
                "updated_at_ms": 100,
            },
        ]

        pack = deadline_mod.deadline_fallback_pack(
            adapter,
            query="what recovery gate did we decide?",
            scope=scope,
            question_type="current_state",
            max_context_tokens=96,
            local_budget={"items": [{"ref": "visible-buffer", "text": "local context"}], "token_estimate": 12, "safety_margin_tokens": 4},
            deadline_ms=1,
            elapsed_ms=2.0,
            records=records,
            reason="deadline_after_record_load",
            budget_source="test",
            source_role_budget_tokens={"assistant": 40},
            source_role_budget_mode="auto",
            memory_layer_budget_tokens={"profile_entity": 48},
            memory_layer_budget_mode="auto",
            memory_selection_policy_budget_tokens={"selected_assistant_decision_outcome_only": 56},
            memory_selection_policy_budget_mode="auto",
        )

        self.assertTrue(pack["partial"])
        self.assertLessEqual(pack["tokens"]["remote"], pack["tokens"]["remote_budget"])
        self.assertEqual(80, pack["tokens"]["remote_budget"])
        self.assertEqual(2, pack["counts"]["refs"]["entity"] + pack["counts"]["refs"]["event"])
        self.assertEqual(1, pack["counts"]["session_continuity"]["cross_session"])
        for field in [
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "async_pipeline_readiness",
            "pre_retrieval_summary_refresh",
        ]:
            self.assertNotIn(field, pack)
        audit_record = adapter.audit_records[0]
        budget = audit_record["memory_layer_budget"]
        self.assertEqual(1, budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, budget["by_entity_type"]["assistant_decision"]["refs"])
        for field in [
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
            "by_memory_selection_policy",
        ]:
            self.assertNotIn(field, budget)
        self.assertEqual(1, budget["final_session_boundary_ref_count"])
        pressure = audit_record["memory_layer_pressure"]
        self.assertEqual(2, pressure["selected_refs"])
        self.assertEqual(pack["tokens"]["remote"], pressure["selected_tokens"])
        self.assertEqual(0, pressure["dropped_refs"])
        self.assertFalse(pressure["profile_memory_pressure"])
        self.assertFalse(pressure["cross_session_pressure"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_scope"]["user_profile"]["selected_refs"])
        self.assertNotIn("by_source_role", pressure["by_dimension"])
        self.assertNotIn("assistant_source_message_pressure", pressure)
        self.assertEqual(1, pressure["by_dimension"]["by_session_continuity"]["cross_session"]["selected_refs"])
        readiness = audit_record["async_pipeline_readiness"]
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertEqual(1, readiness["task_count"])
        self.assertEqual(1, readiness["extraction_committed_task_count"])
        self.assertEqual(["compression", "embedding", "summary"], readiness["remaining_stages"])
        self.assertNotIn("memory_hierarchy", pack)
        self.assertNotIn("retrieval_metrics", pack)
        self.assertIn("async_pipeline_followup_pending", pack["warnings"])
        entity_group = next(group for group in pack["groups"] if group["type"] == "entity")
        entity_item = entity_group["items"][0]
        self.assertEqual("user_profile", entity_item["memory_scope"])
        self.assertEqual("cross_session", entity_item["session_continuity"])
        for field in [
            "source_session_ids",
            "source_entity_count",
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
        ]:
            self.assertNotIn(field, entity_item)
        self.assertEqual(1, len(adapter.audit_records))
        self.assertEqual(budget, audit_record["memory_layer_budget"])
        self.assertEqual(pressure, audit_record["memory_layer_pressure"])
        self.assertEqual(readiness, audit_record["async_pipeline_readiness"])
        self.assertNotIn("memory_hierarchy", audit_record)
        policy_budget = audit_record["recall_policy_summary"]["memory_selection_policy_budget"]
        self.assertTrue(policy_budget["enabled"])
        self.assertEqual("auto", policy_budget["mode"])
        self.assertEqual(80, policy_budget["remote_budget_tokens"])
        self.assertEqual(56, policy_budget["budget_tokens"]["selected_assistant_decision_outcome_only"])
        self.assertEqual(
            2,
            policy_budget["selected_ref_count_by_policy"]["selected_assistant_decision_outcome_only"],
        )
        self.assertEqual(pressure, audit_record["recall_policy_summary"]["memory_layer_pressure"])
        self.assertEqual(readiness, audit_record["recall_policy_summary"]["async_pipeline_readiness"])
        self.assertEqual(
            1,
            audit_record["recall_policy_summary"]["session_continuity"]["cross_session_selected_ref_count"],
        )
        self.assertTrue(audit_record["partial_context_pack"])
        self.assertEqual(1, audit_record["selected_ref_counts"]["entity"])
        self.assertIn("retrieval_deadline_exceeded:deadline_after_record_load", audit_record["quality_warnings"])
        self.assertIn("async_pipeline_followup_pending", audit_record["quality_warnings"])

    def test_async_readiness_normalizes_llm_role_aliases_to_assistant(self) -> None:
        readiness_mod = importlib.import_module("tools.matrixark_mcp_async_readiness")
        scope = {
            "account_id": "acct_alias_readiness",
            "tenant_id": "tenant_alias_readiness",
            "user_id": "user_alias_readiness",
            "session_id": "session_alias_readiness",
        }
        readiness = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 1,
                    "scope": scope,
                    "status": "extraction_committed",
                    "remaining_stages": ["summary"],
                    "source_roles": ["llm", "model", "assistant"],
                    "memory_layers_written": {
                        "session_entities": 1,
                        "profile_entities": 1,
                        "same_session_entities": 1,
                        "cross_session_entities": 1,
                    },
                }
            ],
            {**scope, "_session_scope": "prefer"},
        )

        self.assertEqual({"assistant": 3}, readiness["pending_source_roles"])
        self.assertNotIn("llm", readiness["pending_source_roles"])
        self.assertNotIn("model", readiness["pending_source_roles"])
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertIn("summary", readiness["remaining_stages"])
        layer_readiness = readiness["memory_layer_readiness"]
        self.assertIn("user_profile", layer_readiness["blocked_layers"])
        self.assertIn("cross_session", layer_readiness["blocked_layers"])
        self.assertIn("summary", layer_readiness["blocked_layers"])

    def test_async_readiness_uses_source_count_maps_before_list_fallback(self) -> None:
        readiness_mod = importlib.import_module("tools.matrixark_mcp_async_readiness")
        scope = {
            "account_id": "acct_count_readiness",
            "tenant_id": "tenant_count_readiness",
            "user_id": "user_count_readiness",
            "session_id": "session_count_readiness",
        }
        readiness = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 1,
                    "scope": scope,
                    "status": "extraction_committed",
                    "remaining_stages": ["summary"],
                    "source_role_counts": {"llm": 2, "model": 1, "assistant": 1, "tool": 2, "": 10},
                    "source_roles": ["user"],
                    "source_hook_type_counts": {"after_llm": 3, "hook_boundary": 1},
                    "source_hook_types": ["prompt_submit"],
                    "source_codex_event_counts": {"Stop": 2, "PostToolUse": 1},
                    "source_codex_events": ["UserPromptSubmit"],
                    "memory_layers_written": {
                        "session_entities": 1,
                        "profile_entities": 1,
                        "same_session_entities": 1,
                        "cross_session_entities": 1,
                    },
                }
            ],
            {**scope, "_session_scope": "prefer"},
        )

        self.assertEqual({"assistant": 4, "tool": 2}, readiness["pending_source_roles"])
        self.assertNotIn("user", readiness["pending_source_roles"])
        self.assertNotIn("llm", readiness["pending_source_roles"])
        self.assertNotIn("model", readiness["pending_source_roles"])
        self.assertEqual({"after_llm": 3, "hook_boundary": 1}, readiness["pending_source_hook_types"])
        self.assertNotIn("prompt_submit", readiness["pending_source_hook_types"])
        self.assertEqual({"PostToolUse": 1, "Stop": 2}, readiness["pending_source_codex_events"])
        self.assertNotIn("UserPromptSubmit", readiness["pending_source_codex_events"])

    def test_compact_serving_pack_exposes_async_summary_readiness(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = {
            "context_pack_id": "pack-summary-stale",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision: keep profile summaries visible",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_roles": ["llm", "model", "tool"],
                    "source_role_counts": {"llm": 1, "model": 1, "tool": 1},
                    "source_hook_types": ["hook_boundary"],
                    "source_hook_type_counts": {"hook_boundary": 3},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 3},
                    "source_session_ids": ["session_summary_1", "session_summary_2"],
                    "source_entity_hashes": [101, 102],
                    "current_state_policy": "merged_profile_state",
                    "source_lineage": {"source_role_counts": {"assistant": 1}},
                }
            ],
            "context_pack_payload_policy": {"enable_debug_refs_with": "include_debug_refs=true"},
            "operational_visibility_policy": {"audit_mode": "sampled"},
            "recall_policy": {
                "session_continuity": {
                    "mode": "prefer",
                    "policy": "same-session first, profile entity bridge second",
                },
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                    "remote_budget_tokens": 100,
                    "computed_budget_tokens": 64,
                    "budget_floor_tokens": 256,
                    "budget_floor_applied": False,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                    "max_sessions": 3,
                    "max_candidates": 24,
                },
                "shared_context": {"enabled": False},
                "async_pipeline_readiness": {
                    "task_count": 2,
                    "ready_for_retrieval": False,
                    "remaining_stages": ["summary", "compression"],
                    "remaining_stage_counts": {"summary": 1, "compression": 1},
                    "pending_memory_scopes": {"user_profile": 1},
                    "pending_session_continuities": {"cross_session": 1},
                    "freshness_warnings": ["profile_summary_stale"],
                },
                "memory_layer_budget": {
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 7}},
                    "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 7}},
                    "by_source_role": {
                        "llm": {"refs": 1, "tokens": 3},
                        "model": {"refs": 1, "tokens": 4},
                        "tool": {"refs": 1, "tokens": 2},
                    },
                    "source_message_counts_by_role": {"llm": 1, "model": 1, "tool": 1},
                    "source_hook_counts_by_type": {"hook_boundary": 3},
                    "source_codex_event_counts_by_event": {"Stop": 3},
                },
                "memory_layer_pressure": {
                    "selected_refs": 1,
                    "selected_tokens": 7,
                    "dropped_refs": 1,
                    "dropped_tokens": 13,
                    "profile_memory_pressure": True,
                    "cross_session_pressure": True,
                    "assistant_source_message_pressure": True,
                },
            },
        }
        compact = core_mod.compact_context_pack_for_serving_flat(pack, include_debug=False)
        grouped_compact = core_mod.compact_context_pack_for_serving(pack, include_debug=False)
        debug_compact = core_mod.compact_context_pack_for_serving_flat(pack, include_debug=True)

        hidden_fragments = ("debug", "lineage")
        hidden_fields = {
            "current_state_policy",
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
            "source_lineage",
            "context_pack_payload_policy",
            "operational_visibility_policy",
            "memory_hierarchy",
        }

        def assert_no_default_debug_lineage(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    self.assertNotIn(key, hidden_fields)
                    lowered = str(key).lower()
                    self.assertFalse(any(fragment in lowered for fragment in hidden_fragments), key)
                    assert_no_default_debug_lineage(item)
            elif isinstance(value, list):
                for item in value:
                    assert_no_default_debug_lineage(item)

        for default_pack in [compact, grouped_compact]:
            assert_no_default_debug_lineage(default_pack)

        for field in [
            "async_pipeline_readiness",
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "pre_retrieval_summary_refresh",
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
        ]:
            self.assertNotIn(field, compact)
        flat_item = compact["selected_refs"][0]
        for field in [
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
        ]:
            self.assertNotIn(field, flat_item)
        for field in [
            "async_pipeline_readiness",
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "pre_retrieval_summary_refresh",
        ]:
            self.assertNotIn(field, grouped_compact)
        entity_item = grouped_compact["groups"][0]["items"][0]
        for field in [
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
        ]:
            self.assertNotIn(field, entity_item)
        self.assertNotIn("memory_hierarchy", compact)
        self.assertNotIn("memory_hierarchy", grouped_compact)
        readiness = debug_compact["async_pipeline_readiness"]
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertEqual(["summary", "compression"], readiness["remaining_stages"])
        self.assertEqual(["profile_summary_stale"], readiness["freshness_warnings"])
        self.assertEqual({"user_profile": 1}, readiness["pending_memory_scopes"])
        self.assertEqual({"cross_session": 1}, readiness["pending_session_continuities"])
        self.assertEqual({"user_profile": {"refs": 1, "tokens": 7}}, debug_compact["memory_layer_budget"]["by_memory_scope"])
        self.assertTrue(debug_compact["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertTrue(debug_compact["memory_layer_pressure"]["cross_session_pressure"])
        self.assertIn("assistant_source_message_pressure", debug_compact["memory_layer_pressure"])
        hierarchy = debug_compact["memory_hierarchy"]
        self.assertEqual("user_profile", hierarchy["models"]["profile_entity"]["memory_scope"])
        self.assertEqual("context_profile_entity", hierarchy["models"]["profile_index"]["data_model"])
        self.assertEqual("same-session first, profile entity bridge second", hierarchy["retrieval_strategy"])
        self.assertEqual("prefer", hierarchy["session_scope_mode"])
        self.assertTrue(hierarchy["cross_session_enabled"])
        self.assertEqual(64, hierarchy["cross_session_budget_tokens"])
        self.assertEqual(100, hierarchy["cross_session_remote_budget_tokens"])
        self.assertEqual(256, hierarchy["cross_session_budget_floor_tokens"])
        self.assertFalse(hierarchy["cross_session_budget_floor_applied"])
        self.assertEqual(
            "remote_budget_too_small_for_profile_floor",
            hierarchy["cross_session_budget_floor_status"],
        )
        self.assertIn("profile_entity_bridge", hierarchy["selected_ref_flow"])
        self.assertIn("summary_or_compression", hierarchy["selected_ref_flow"])
        self.assertNotIn("recall_policy", compact)


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

    def test_debug_lineage_serving_pack_can_include_source_count_maps(self) -> None:
        with mock.patch.dict(os.environ, {"MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE": "1"}):
            context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
            reloaded = importlib.reload(context_pack_mod)
            self.addCleanup(lambda: importlib.reload(context_pack_mod))
            pack = reloaded.compact_context_pack_for_serving(
                {
                    "context_pack_id": "debug-lineage-pack",
                    "selected_refs": [
                        {
                            "ref_type": "entity",
                            "text": "debug profile decision",
                            "token_estimate": 3,
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "source_roles": ["llm", "tool"],
                            "source_role_counts": {"llm": 2, "tool": 1},
                            "source_hook_type_counts": {"hook_boundary": 3},
                            "source_codex_event_counts": {"Stop": 3},
                            "source_session_ids": ["session-debug-1"],
                            "source_entity_hashes": [101, 202],
                        }
                    ],
                    "used_context_tokens": 3,
                }
            )
        item = pack["groups"][0]["items"][0]
        self.assertEqual(["assistant", "tool"], item["source_roles"])
        self.assertEqual({"assistant": 2, "tool": 1}, item["source_role_counts"])
        self.assertEqual({"hook_boundary": 3}, item["source_hook_type_counts"])
        self.assertEqual({"Stop": 3}, item["source_codex_event_counts"])
        self.assertEqual(["session-debug-1"], item["source_session_ids"])
        self.assertEqual(2, item["source_entity_count"])

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
