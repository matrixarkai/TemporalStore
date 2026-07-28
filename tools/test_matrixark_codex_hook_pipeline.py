#!/usr/bin/env python3

from __future__ import annotations

from argparse import Namespace
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path

import matrixark_codex_hook
from matrixark_mcp_context_pack import (
    compact_context_pack_audit_record,
    compact_context_pack_for_serving,
    compact_dropped_refs_for_context_pack,
    compact_refs_for_audit,
)
from matrixark_mcp_core import (
    candidate_index_terms,
    candidate_memory_layer_name,
    compact_context_pack_audit_record as core_compact_context_pack_audit_record,
    compact_context_pack_for_serving as core_compact_context_pack_for_serving,
    compact_context_pack_for_serving_flat,
    identity_hashes,
    packing_sort_key,
    select_token_budgeted_refs,
)
from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness
from matrixark_mcp_recovery import matrixark_local_recovery_report
from matrixark_mcp_retrieve_pack_builder import dropped_ref_layer_budget, memory_layer_pressure_summary, selected_ref_layer_budget
from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer
from matrixark_mcp_summary_runtime import build_node_summary_refresh_records


class CountingLocalAdapter(MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.flushed_batch_sizes: list[int] = []
        self.retrieval_call_count = 0

    def append_many(self, records: list[dict]) -> None:
        active_batch = self._current_write_batch()
        super().append_many(records)
        if active_batch is None and records:
            self.flushed_batch_sizes.append(len(records))

    def retrieval_records(self, **kwargs):
        self.retrieval_call_count += 1
        return super().retrieval_records(**kwargs)


class FastHookLocalAdapter(MatrixArkLocalAdapter):
    def enqueue_raw_ingestion_records(self, records: list[dict]) -> None:
        self.append_many(records)

    def _enqueue_direct_write(self, records: list[dict]) -> None:
        self.append_many(records)


class NativeCaptureLocalAdapter(MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.native_requests: list[dict] = []

    def supports_native_context_pack(self) -> bool:
        return True

    def native_context_pack(self, request: dict) -> dict | None:
        self.native_requests.append(dict(request))
        return {
            "context_pack_id": "local-native-pack",
            "selected_refs": [],
            "used_context_tokens": 0,
            "used_remote_context_tokens": 0,
            "remote_context_budget_tokens": request.get("max_context_tokens", 0),
            "recall_policy": {
                "source_role_budget": {
                    "enabled": bool(request.get("source_role_budget_tokens")),
                    "budget_tokens": request.get("source_role_budget_tokens", {}),
                },
                "memory_layer_budget_policy": {
                    "enabled": bool(request.get("memory_layer_budget_tokens")),
                    "budget_tokens": request.get("memory_layer_budget_tokens", {}),
                    "mode": request.get("memory_layer_budget_mode"),
                    "question_type": request.get("memory_layer_budget_question_type"),
                    "question_budget_reason": request.get("memory_layer_budget_question_reason"),
                    "derived": request.get("memory_layer_budget_mode") in {
                        "auto",
                        "balanced",
                        "codex_auto",
                        "pre_retrieval_summary_refresh_balanced",
                    },
                },
                "memory_selection_policy_budget_policy": {
                    "enabled": bool(request.get("memory_selection_policy_budget_tokens")),
                    "budget_tokens": request.get("memory_selection_policy_budget_tokens", {}),
                    "mode": request.get("memory_selection_policy_budget_mode"),
                }
            },
        }


class MatrixArkCodexHookPipelineTest(unittest.TestCase):
    def test_summary_runtime_preserves_event_child_and_entity_lineage_counts(self) -> None:
        result = build_node_summary_refresh_records(
            node_path=["tenant:tenant_runtime", "user:user_runtime", "profile:long_term_memory"],
            node_hash=424242,
            scope={
                "account_id": "acct_runtime",
                "tenant_id": "tenant_runtime",
                "user_id": "user_runtime",
            },
            events=[
                {
                    "record_type": "context_event",
                    "event_id_hash": 101,
                    "text": "user: remember summary runtime lineage",
                    "source_role": "user",
                    "hook_type": "before_llm",
                    "codex_event": "UserPromptSubmit",
                    "source_role_counts": {"user": 1},
                    "source_hook_type_counts": {"before_llm": 1},
                    "source_codex_event_counts": {"UserPromptSubmit": 1},
                    "source_memory_selection_policies": ["selected_user_prompt"],
                    "source_memory_selection_policy_counts": {"selected_user_prompt": 1},
                    "extraction_phase": "provisional",
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 102,
                    "text": "tool: Exit code 0 proves summary runtime lineage",
                    "source_roles": ["tool"],
                    "source_hook_types": ["tool_result"],
                    "source_codex_events": ["PostToolUse"],
                    "source_memory_selection_policies": ["selected_tool_evidence_only"],
                    "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                },
            ],
            child_summaries=[
                {
                    "record_type": "context_summary",
                    "summary_hash": 201,
                    "summary_text": "assistant decision summary",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 2},
                    "source_hook_types": ["after_llm"],
                    "source_hook_type_counts": {"after_llm": 1},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 1},
                    "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                    "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                    "source_memory_scopes": ["session", "user_profile"],
                    "source_session_continuities": ["same_session", "cross_session"],
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                }
            ],
            entity_states=[
                {
                    "record_type": "context_entity",
                    "entity_hash": 301,
                    "entity_type": "assistant_decision",
                    "entity_name": "summary_runtime",
                    "state": "preserve assistant/tool/user lineage in summaries",
                    "source_roles": ["assistant", "tool"],
                    "source_role_counts": {"assistant": 1, "tool": 1},
                    "source_hook_types": ["after_llm", "tool_result"],
                    "source_hook_type_counts": {"after_llm": 1, "tool_result": 1},
                    "source_codex_events": ["Stop", "PostToolUse"],
                    "source_codex_event_counts": {"Stop": 1, "PostToolUse": 1},
                    "source_memory_selection_policies": ["selected_assistant_decision_outcome_only", "selected_tool_evidence_only"],
                    "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1, "selected_tool_evidence_only": 1},
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "extraction_phase": "provisional",
                }
            ],
            operator_states=[],
            summary_source_policy={},
            dirty_hash=909,
            refreshed_at_ms=1780000000000,
        )

        summaries = [record for record in result["records"] if record.get("record_type") == "context_summary"]
        self.assertTrue(summaries)
        summary = summaries[0]
        self.assertEqual(["assistant", "tool", "user"], summary["source_roles"])
        self.assertEqual({"assistant": 3, "tool": 2, "user": 1}, summary["source_role_counts"])
        self.assertEqual(["after_llm", "before_llm", "tool_result"], summary["source_hook_types"])
        self.assertEqual({"after_llm": 2, "before_llm": 1, "tool_result": 2}, summary["source_hook_type_counts"])
        self.assertEqual(["PostToolUse", "Stop", "UserPromptSubmit"], summary["source_codex_events"])
        self.assertEqual({"PostToolUse": 2, "Stop": 2, "UserPromptSubmit": 1}, summary["source_codex_event_counts"])
        self.assertEqual(["selected_assistant_decision_outcome_only", "selected_tool_evidence_only", "selected_user_prompt"], summary["source_memory_selection_policies"])
        self.assertEqual({"selected_assistant_decision_outcome_only": 2, "selected_tool_evidence_only": 2, "selected_user_prompt": 1}, summary["source_memory_selection_policy_counts"])
        self.assertEqual(["session", "user_profile"], summary["source_memory_scopes"])
        self.assertEqual(["cross_session", "same_session"], summary["source_session_continuities"])
        self.assertEqual("user_profile", summary["memory_scope"])
        self.assertEqual("cross_session", summary["session_continuity"])
        self.assertEqual(summary["source_role_counts"], result["source_role_counts"])

    def assert_no_default_context_pack_debug_lineage(self, value: object) -> None:
        hidden_keys = {
            "debug_payload",
            "lineage",
            "memory_lineage",
            "memory_hierarchy",
            "source_session_ids",
            "source_roles",
            "budget_source_roles",
            "source_hook_types",
            "source_codex_events",
            "source_memory_scopes",
            "source_session_continuities",
            "source_extraction_phases",
            "source_profile_promotion_policies",
            "source_profile_promotion_blockers",
            "source_final_session_boundary_count",
            "source_role_counts",
            "budget_source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
            "by_memory_selection_policy",
            "pending_source_roles",
            "pending_source_hook_types",
            "pending_source_codex_events",
            "pending_memory_scopes",
            "pending_session_continuities",
            "pending_extraction_phases",
            "pending_final_session_boundary_count",
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_entity_types",
            "source_entity_hashes",
            "source_entity_count",
            "source_entity_lineage",
            "current_state_source_session_count",
            "current_state_source_entity_count",
        }
        if isinstance(value, dict):
            for key, child in value.items():
                self.assertNotIn(key, hidden_keys)
                self.assertNotIn("lineage", str(key).lower())
                self.assertNotIn("debug_", str(key).lower())
                self.assertNotIn("_debug", str(key).lower())
                self.assert_no_default_context_pack_debug_lineage(child)
        elif isinstance(value, list):
            for child in value:
                self.assert_no_default_context_pack_debug_lineage(child)

    def test_context_pack_serving_hides_debug_lineage_recursively_by_default(self) -> None:
        pack = {
            "context_pack_id": "pack-debug-default",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "context_class": "entity",
                    "text": "assistant decided to keep default context compact",
                    "entity_type": "assistant_decision",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 2},
                    "source_hook_types": ["after_llm"],
                    "source_hook_type_counts": {"after_llm": 2},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 2},
                    "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                    "source_profile_promotion_blockers": ["profile_scope_missing"],
                    "source_entity_types": ["assistant_decision", "tool_evidence"],
                    "source_session_ids": ["session-a", "session-b"],
                    "source_entity_hashes": ["aaa", "bbb"],
                    "current_state_source_session_count": 2,
                    "metadata": {
                        "source_role_counts": {"assistant": 2},
                        "debug_payload": {"raw": "hidden"},
                        "custom_lineage_marker": "hidden",
                    },
                }
            ],
            "groups": [
                {
                    "type": "entity",
                    "items": [
                        {
                            "text": "already serving-shaped ref",
                            "source_role_counts": {"assistant": 1},
                            "debug_payload": {"raw": "hidden"},
                        }
                    ],
                }
            ],
            "retrieval_metrics": {
                "memory_layer_budget": {
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 10}},
                    "by_source_role": {"assistant": {"refs": 1, "tokens": 10}},
                    "source_message_counts_by_role": {"assistant": 2},
                },
                "async_pipeline_readiness": {
                    "task_count": 3,
                    "remaining_stage_counts": {"summary": 1},
                    "pending_source_roles": {"assistant": 2, "tool": 1},
                    "pending_source_hook_types": {"after_llm": 2},
                    "pending_source_codex_events": {"Stop": 1},
                    "pending_memory_scopes": {"user_profile": 1},
                },
                "pre_retrieval_summary_refresh": {
                    "enabled": False,
                    "status": "disabled",
                    "requested_limit": 2,
                    "elapsed_ms": 0.0,
                },
                "memory_layer_pressure": {
                    "dropped_dimensions": ["by_source_role", "by_memory_scope"],
                    "by_dimension": {
                        "by_source_role": {"assistant": {"dropped_refs": 1}},
                        "by_memory_scope": {"session": {"dropped_refs": 1}},
                        "by_profile_shadowed_reason": {
                            "source_entity_lineage": {"dropped_refs": 1},
                        },
                    },
                    "assistant_source_message_pressure": True,
                },
            },
            "recall_policy": {
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                },
                "memory_layer_budget": {
                    "by_source_role": {"assistant": {"refs": 1}},
                    "source_message_counts_by_role": {"assistant": 2},
                }
            },
        }
        serving = compact_context_pack_for_serving(pack)
        self.assert_no_default_context_pack_debug_lineage(serving)
        self.assertEqual("pack-debug-default", serving["context_pack_id"])
        self.assertEqual("assistant_decision", serving["groups"][0]["items"][0]["entity_type"])
        self.assertNotIn("retrieval_metrics", serving)
        for field in [
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "async_pipeline_readiness",
            "pre_retrieval_summary_refresh",
        ]:
            self.assertNotIn(field, serving)

        metrics_serving = compact_context_pack_for_serving({**pack, "include_retrieval_metrics": True})
        self.assertTrue(metrics_serving["include_retrieval_metrics"])
        self.assertIn("retrieval_metrics", metrics_serving)
        self.assertIn("memory_layer_budget", metrics_serving["retrieval_metrics"])
        self.assert_no_default_context_pack_debug_lineage(metrics_serving["retrieval_metrics"])
        self.assertNotIn("by_source_role", metrics_serving["retrieval_metrics"]["memory_layer_budget"])
        self.assertNotIn(
            "source_message_counts_by_role",
            metrics_serving["retrieval_metrics"]["memory_layer_budget"],
        )
        self.assertNotIn(
            "pending_source_roles",
            metrics_serving["retrieval_metrics"]["async_pipeline_readiness"],
        )
        self.assertNotIn(
            "by_source_role",
            metrics_serving["retrieval_metrics"]["memory_layer_pressure"].get("by_dimension", {}),
        )
        self.assertNotIn(
            "by_profile_shadowed_reason",
            metrics_serving["retrieval_metrics"]["memory_layer_pressure"].get("by_dimension", {}),
        )
        self.assertEqual(
            {"by_memory_scope"},
            set(metrics_serving["retrieval_metrics"]["memory_layer_pressure"]["dropped_dimensions"]),
        )

    def test_context_pack_serving_includes_debug_lineage_with_flag(self) -> None:
        pack = {
            "context_pack_id": "pack-debug-enabled",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision debug view",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 2},
                    "source_hook_type_counts": {"after_llm": 2},
                    "source_codex_event_counts": {"Stop": 2},
                    "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                    "source_profile_promotion_blockers": ["profile_scope_missing"],
                    "source_entity_types": ["assistant_decision", "tool_evidence"],
                    "source_entity_hashes": ["aaa", "bbb"],
                    "current_state_source_session_count": 2,
                }
            ],
            "retrieval_metrics": {
                "memory_layer_budget": {
                    "by_source_role": {"assistant": {"refs": 1}},
                    "source_message_counts_by_role": {"assistant": 2},
                }
            },
            "recall_policy": {
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                },
            },
        }
        serving = compact_context_pack_for_serving(pack, include_debug=True)
        item = serving["groups"][0]["items"][0]
        self.assertEqual(["assistant"], item["source_roles"])
        self.assertEqual({"assistant": 2}, item["source_role_counts"])
        self.assertEqual({"after_llm": 2}, item["source_hook_type_counts"])
        self.assertEqual({"Stop": 2}, item["source_codex_event_counts"])
        self.assertEqual(["always_when_profile_scope_available"], item["source_profile_promotion_policies"])
        self.assertEqual(["profile_scope_missing"], item["source_profile_promotion_blockers"])
        self.assertEqual(["assistant_decision", "tool_evidence"], item["source_entity_types"])
        self.assertEqual(2, item["source_entity_count"])
        self.assertEqual(2, item["current_state_source_session_count"])
        self.assertIn("retrieval_metrics", serving)
        self.assertEqual(
            {"assistant": 2},
            serving["retrieval_metrics"]["memory_layer_budget"]["source_message_counts_by_role"],
        )
        self.assertIn("memory_hierarchy", serving)
        self.assertTrue(serving["memory_hierarchy"]["cross_session_enabled"])

    def test_core_context_pack_debug_flag_exposes_only_bounded_lineage(self) -> None:
        pack = {
            "context_pack_id": "pack-core-debug-enabled",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision with compact debug lineage",
                    "entity_type": "assistant_decision",
                    "source_role_counts": {"assistant": 2},
                    "source_hook_type_counts": {"after_llm": 1},
                    "source_codex_event_counts": {"Stop": 1},
                    "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                    "source_profile_promotion_blockers": ["profile_scope_missing"],
                    "source_entity_types": ["assistant_decision", "tool_evidence"],
                    "pending_source_roles": {"assistant": 2},
                    "pending_source_hook_types": {"after_llm": 1},
                    "pending_source_codex_events": {"Stop": 1},
                    "source_entity_hashes": ["aaa", "bbb", "ccc"],
                    "debug_payload": {"raw": "must stay out of ContextPack"},
                    "lineage": {"raw_source_ids": ["hidden"]},
                    "metadata": {
                        "debug_payload": {"raw": "metadata hidden"},
                        "lineage": {"raw_source_ids": ["metadata hidden"]},
                    },
                }
            ],
            "debug_payload": {"raw": "pack hidden"},
            "memory_lineage": {"raw": "pack hidden"},
            "retrieval_metrics": {
                "async_pipeline_readiness": {
                    "task_count": 1,
                    "pending_source_roles": {"assistant": 2},
                    "pending_source_hook_types": {"after_llm": 1},
                    "pending_source_codex_events": {"Stop": 1},
                },
                "pre_retrieval_summary_refresh": {
                    "enabled": True,
                    "status": "refreshed",
                    "requested_limit": 4,
                    "refreshed_count": 2,
                    "skipped_dirty_reasons": {"source_lineage_pending": 1},
                },
            },
        }

        default_serving = core_compact_context_pack_for_serving(pack)
        self.assert_no_default_context_pack_debug_lineage(default_serving)

        debug_serving = core_compact_context_pack_for_serving(pack, include_debug=True)
        item = debug_serving["groups"][0]["items"][0]
        self.assertEqual({"assistant": 2}, item["source_role_counts"])
        self.assertEqual({"after_llm": 1}, item["source_hook_type_counts"])
        self.assertEqual({"Stop": 1}, item["source_codex_event_counts"])
        self.assertEqual(["always_when_profile_scope_available"], item["source_profile_promotion_policies"])
        self.assertEqual(["profile_scope_missing"], item["source_profile_promotion_blockers"])
        self.assertEqual(["assistant_decision", "tool_evidence"], item["source_entity_types"])
        self.assertEqual(3, item["source_entity_count"])
        self.assertEqual(
            {"assistant": 2},
            debug_serving["async_pipeline_readiness"]["pending_source_roles"],
        )
        self.assertEqual(
            {"after_llm": 1},
            debug_serving["async_pipeline_readiness"]["pending_source_hook_types"],
        )
        self.assertEqual(
            {"Stop": 1},
            debug_serving["async_pipeline_readiness"]["pending_source_codex_events"],
        )
        self.assertEqual("refreshed", debug_serving["pre_retrieval_summary_refresh"]["status"])
        self.assertEqual(2, debug_serving["pre_retrieval_summary_refresh"]["refreshed_count"])
        self.assertNotIn("debug_payload", json.dumps(debug_serving))
        self.assertNotIn("raw_source_ids", json.dumps(debug_serving))
        self.assertNotIn("selected_refs", debug_serving)

    def test_prebuilt_context_pack_groups_never_expose_raw_debug_payloads(self) -> None:
        pack = {
            "context_pack_id": "pack-prebuilt-group-debug",
            "groups": [
                {
                    "type": "entity",
                    "class": "entity",
                    "n": 1,
                    "debug_payload": {"raw": "hidden group debug"},
                    "lineage": {"raw_source_ids": ["hidden-group-source"]},
                    "items": [
                        {
                            "text": "assistant decision from a native prebuilt group",
                            "entity_type": "assistant_decision",
                            "source_roles": ["llm", "model", "assistant"],
                            "source_role_counts": {"llm": 2, "model": 3},
                            "source_hook_type_counts": {"after_llm": 2},
                            "source_codex_event_counts": {"Stop": 1},
                            "source_session_ids": [f"session-{index}" for index in range(12)],
                            "source_entity_hashes": ["aaa", "bbb", "ccc"],
                            "debug_payload": {"raw": "hidden item debug"},
                            "lineage": {"raw_source_ids": ["hidden-item-source"]},
                        }
                    ],
                }
            ],
        }

        default_serving = core_compact_context_pack_for_serving(pack)
        self.assert_no_default_context_pack_debug_lineage(default_serving)

        debug_serving = core_compact_context_pack_for_serving(pack, include_debug=True)
        serialized = json.dumps(debug_serving)
        self.assertNotIn("debug_payload", serialized)
        self.assertNotIn("raw_source_ids", serialized)
        item = debug_serving["groups"][0]["items"][0]
        self.assertEqual(["assistant"], item["source_roles"])
        self.assertEqual({"assistant": 5}, item["source_role_counts"])
        self.assertEqual({"after_llm": 2}, item["source_hook_type_counts"])
        self.assertEqual({"Stop": 1}, item["source_codex_event_counts"])
        self.assertEqual(8, len(item["source_session_ids"]))
        self.assertEqual(3, item["source_entity_count"])

    def test_prebuilt_context_pack_groups_honor_debug_lineage_flag(self) -> None:
        pack = {
            "context_pack_id": "pack-prebuilt-group-env-debug",
            "groups": [
                {
                    "type": "entity",
                    "n": 1,
                    "debug_payload": {"raw": "hidden group debug"},
                    "items": [
                        {
                            "text": "assistant decision from a native prebuilt group",
                            "entity_type": "assistant_decision",
                            "source_roles": ["llm", "tool"],
                            "source_role_counts": {"llm": 2, "tool": 1},
                            "source_hook_type_counts": {"after_llm": 2},
                            "source_codex_event_counts": {"Stop": 1},
                            "source_session_ids": ["session-a"],
                            "source_entity_hashes": ["aaa", "bbb"],
                            "lineage": {"raw_source_ids": ["hidden-source"]},
                        }
                    ],
                }
            ],
        }

        default_serving = core_compact_context_pack_for_serving(pack)
        self.assert_no_default_context_pack_debug_lineage(default_serving)

        with (
            mock.patch("matrixark_mcp_context_pack.DEBUG_LINEAGE_PAYLOAD", True),
            mock.patch("tools.matrixark_mcp_context_pack.DEBUG_LINEAGE_PAYLOAD", True),
        ):
            debug_serving = core_compact_context_pack_for_serving(pack)

        serialized = json.dumps(debug_serving)
        self.assertNotIn("debug_payload", serialized)
        self.assertNotIn("raw_source_ids", serialized)
        item = debug_serving["groups"][0]["items"][0]
        self.assertEqual(["assistant", "tool"], item["source_roles"])
        self.assertEqual({"assistant": 2, "tool": 1}, item["source_role_counts"])
        self.assertEqual({"after_llm": 2}, item["source_hook_type_counts"])
        self.assertEqual({"Stop": 1}, item["source_codex_event_counts"])
        self.assertEqual(["session-a"], item["source_session_ids"])
        self.assertEqual(2, item["source_entity_count"])

    def test_async_readiness_reports_layer_specific_freshness_warnings(self) -> None:
        scope = {
            "account_id": "acct_ready",
            "tenant_id": "tenant_ready",
            "user_id": "user_ready",
            "session_id": "session_ready",
        }
        readiness = async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 1,
                    "scope": scope,
                    "status": "extraction_committed",
                    "remaining_stages": ["summary", "compression", "embedding"],
                    "memory_layers_written": {
                        "session_entities": 1,
                        "profile_entities": 1,
                        "same_session_entities": 1,
                        "cross_session_entities": 1,
                    },
                }
            ],
            scope,
        )

        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertIn("session_memory_stale", readiness["freshness_warnings"])
        self.assertIn("profile_memory_stale", readiness["freshness_warnings"])
        self.assertIn("cross_session_memory_stale", readiness["freshness_warnings"])
        self.assertIn("summary_memory_stale", readiness["freshness_warnings"])
        self.assertIn("compression_memory_pending", readiness["freshness_warnings"])
        self.assertIn("embedding_memory_pending", readiness["freshness_warnings"])

    def test_flat_context_pack_serving_hides_async_source_readiness_by_default(self) -> None:
        pack = {
            "context_pack_id": "pack-flat-readiness-default",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision default flat readiness should stay lean",
                    "entity_type": "assistant_decision",
                    "source_role_counts": {"assistant": 2},
                }
            ],
            "recall_policy": {
                "async_pipeline_readiness": {
                    "task_count": 2,
                    "remaining_stage_counts": {"summary": 1, "embedding": 1},
                    "pending_source_roles": {"assistant": 2, "tool": 1},
                    "pending_source_hook_types": {"after_llm": 1},
                    "pending_source_codex_events": {"Stop": 1},
                    "pending_memory_scopes": {"user_profile": 1},
                },
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                },
                "memory_layer_budget_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "question_type": "current_state",
                    "question_budget_reason": "current_state_or_latest_queries_prioritize_profile_entity and cross-session current state",
                    "budget_tokens": {"profile_entity": 40},
                },
                "dropped_memory_layer_budget": {
                    "total_dropped_refs": 1,
                    "profile_shadowed_ref_count": 1,
                    "by_profile_shadowed_reason": {
                        "source_entity_lineage": {"refs": 1, "tokens": 6},
                    },
                    "by_memory_scope": {"session": {"refs": 1, "tokens": 6}},
                    "by_source_role": {"assistant": {"refs": 1, "tokens": 6}},
                },
                "memory_layer_pressure": {
                    "dropped_refs": 1,
                    "dropped_dimensions": [
                        "by_profile_shadowed_reason",
                        "by_memory_scope",
                        "by_source_role",
                    ],
                    "by_dimension": {
                        "by_profile_shadowed_reason": {
                            "source_entity_lineage": {"dropped_refs": 1},
                        },
                        "by_memory_scope": {"session": {"dropped_refs": 1}},
                        "by_source_role": {"assistant": {"dropped_refs": 1}},
                    },
                    "assistant_source_message_pressure": True,
                },
            },
        }

        default_serving = compact_context_pack_for_serving_flat(pack)
        self.assert_no_default_context_pack_debug_lineage(default_serving)
        self.assertNotIn("memory_hierarchy", default_serving)
        for field in [
            "async_pipeline_readiness",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "memory_layer_budget",
        ]:
            self.assertNotIn(field, default_serving)

        debug_serving = compact_context_pack_for_serving_flat(pack, include_debug=True)
        self.assertIn("memory_hierarchy", debug_serving)
        self.assertTrue(debug_serving["memory_hierarchy"]["cross_session_enabled"])
        self.assertEqual("current_state", debug_serving["memory_hierarchy"]["memory_layer_budget_question_type"])
        self.assertIn(
            "prioritize_profile_entity",
            debug_serving["memory_hierarchy"]["memory_layer_budget_question_reason"],
        )
        debug_readiness = debug_serving["async_pipeline_readiness"]
        self.assertEqual({"assistant": 2, "tool": 1}, debug_readiness["pending_source_roles"])
        self.assertEqual({"after_llm": 1}, debug_readiness["pending_source_hook_types"])
        self.assertEqual({"Stop": 1}, debug_readiness["pending_source_codex_events"])
        self.assertEqual(
            {"refs": 1, "tokens": 6},
            debug_serving["dropped_memory_layer_budget"]["by_profile_shadowed_reason"]["source_entity_lineage"],
        )
        self.assertEqual(
            {"dropped_refs": 1},
            debug_serving["memory_layer_pressure"]["by_dimension"]["by_profile_shadowed_reason"]["source_entity_lineage"],
        )

    def test_context_pack_audit_hides_memory_hierarchy_by_default(self) -> None:
        audit = {
            "record_type": "context_pack_audit",
            "context_pack_id": "audit-hierarchy-default",
            "query": "profile hierarchy debug",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision audit hierarchy",
                    "entity_type": "assistant_decision",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                }
            ],
            "recall_policy": {
                "session_continuity": {"mode": "prefer"},
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                },
                "memory_layer_budget_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "question_type": "current_state",
                    "question_budget_reason": "current_state_or_latest_queries_prioritize_profile_entity and cross-session current state",
                    "budget_tokens": {"profile_entity": 40},
                },
            },
            "memory_hierarchy": {
                "cross_session_enabled": True,
                "cross_session_budget_tokens": 64,
                "memory_layer_budget_question_type": "current_state",
                "memory_layer_budget_question_reason": "current_state_or_latest_queries_prioritize_profile_entity and cross-session current state",
            },
        }

        compact = compact_context_pack_audit_record(audit)
        self.assertNotIn("memory_hierarchy", compact)
        self.assertEqual("compact_audit", compact["payload_policy"]["mode"])
        core_compact = core_compact_context_pack_audit_record(audit)
        self.assertNotIn("memory_hierarchy", core_compact)

        debug_compact = compact_context_pack_audit_record(audit, include_debug=True)
        self.assertIn("memory_hierarchy", debug_compact)
        self.assertTrue(debug_compact["memory_hierarchy"]["cross_session_enabled"])
        self.assertEqual(64, debug_compact["memory_hierarchy"]["cross_session_budget_tokens"])
        self.assertEqual("current_state", debug_compact["memory_hierarchy"]["memory_layer_budget_question_type"])

    def test_context_pack_audit_refs_preserve_memory_layer_lineage(self) -> None:
        refs = compact_refs_for_audit(
            [
                {
                    "ref_type": "entity",
                    "text": "assistant_decision Commit aaa111 pushed after tests passed.",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "entity_name": "Commit aaa111",
                    "extraction_phase": "final",
                    "profile_current_state_representative": True,
                    "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                    "final_session_boundary": True,
                    "source_roles": ["assistant"],
                    "source_hook_types": ["hook_boundary"],
                    "source_codex_events": ["Stop"],
                    "source_memory_scopes": ["session", "user_profile"],
                    "source_session_continuities": ["same_session", "cross_session"],
                    "source_extraction_phases": ["final"],
                    "source_final_session_boundary_count": 1,
                    "current_state_source_session_count": 2,
                    "current_state_source_entity_count": 3,
                }
            ]
        )
        ref = refs[0]
        self.assertEqual("user_profile", ref["memory_scope"])
        self.assertEqual("cross_session", ref["session_continuity"])
        self.assertEqual("assistant_decision", ref["entity_type"])
        self.assertEqual("final", ref["extraction_phase"])
        self.assertTrue(ref["profile_current_state_representative"])
        self.assertTrue(ref["final_session_boundary"])
        self.assertEqual(["assistant"], ref["source_roles"])
        self.assertEqual(["hook_boundary"], ref["source_hook_types"])
        self.assertEqual(["Stop"], ref["source_codex_events"])
        self.assertEqual(["session", "user_profile"], ref["source_memory_scopes"])
        self.assertEqual(["same_session", "cross_session"], ref["source_session_continuities"])
        self.assertEqual(["final"], ref["source_extraction_phases"])
        self.assertEqual(1, ref["source_final_session_boundary_count"])
        self.assertEqual(2, ref["current_state_source_session_count"])
        self.assertEqual(3, ref["current_state_source_entity_count"])

    def test_dropped_memory_layer_budget_tracks_final_phase_and_boundary(self) -> None:
        budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        "drop_reason": "over_budget",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "extraction_phase": "final",
                        "ref_type": "entity",
                        "entity_type": "assistant_decision",
                        "source_roles": ["assistant"],
                        "source_hook_types": ["hook_boundary"],
                        "source_codex_events": ["Stop"],
                        "final_session_boundary": True,
                        "token_estimate": 9,
                    }
                ]
            }
        )
        self.assertEqual(1, budget["by_memory_layer"]["profile_entity"]["refs"])
        self.assertEqual(9, budget["by_memory_layer"]["profile_entity"]["tokens"])
        self.assertEqual(1, budget["by_extraction_phase"]["final"]["refs"])
        self.assertEqual(9, budget["by_extraction_phase"]["final"]["tokens"])
        self.assertEqual(1, budget["final_ref_count"])
        self.assertEqual(1, budget["final_session_boundary_ref_count"])

    def test_memory_layer_budget_tracks_persisted_source_counts(self) -> None:
        selected_budget = selected_ref_layer_budget(
            [
                {
                    "ref_type": "entity",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_memory_scopes": ["session", "user_profile"],
                    "source_session_continuities": ["same_session", "cross_session"],
                    "source_extraction_phases": ["final", "provisional"],
                    "source_roles": ["assistant", "user"],
                    "source_role_counts": {"assistant": 3, "user": 1},
                    "source_hook_types": ["hook_boundary"],
                    "source_hook_type_counts": {"hook_boundary": 4},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 4},
                    "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                    "token_estimate": 11,
                }
            ]
        )
        dropped_budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        "drop_reason": "over_budget",
                        "ref_type": "summary",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "source_memory_scopes": ["session", "user_profile"],
                        "source_session_continuities": ["same_session", "cross_session"],
                        "source_extraction_phases": ["final", "provisional"],
                        "source_roles": ["assistant", "tool"],
                        "source_role_counts": {"assistant": 2, "tool": 1},
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 3},
                        "source_codex_events": ["Stop"],
                        "source_codex_event_counts": {"Stop": 3},
                        "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                        "source_profile_promotion_blockers": ["profile_scope_missing"],
                        "token_estimate": 13,
                    }
                ]
            }
        )
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)

        self.assertEqual(1, selected_budget["by_memory_layer"]["profile_entity"]["refs"])
        self.assertEqual(11, selected_budget["by_memory_layer"]["profile_entity"]["tokens"])
        self.assertEqual(1, selected_budget["by_memory_scope"]["session"]["refs"])
        self.assertEqual(1, selected_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, selected_budget["by_session_continuity"]["same_session"]["refs"])
        self.assertEqual(1, selected_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, selected_budget["by_extraction_phase"]["final"]["refs"])
        self.assertEqual(1, selected_budget["by_extraction_phase"]["provisional"]["refs"])
        self.assertEqual(1, selected_budget["by_profile_promotion_policy"]["always_when_profile_scope_available"]["refs"])
        self.assertEqual(1, selected_budget["final_ref_count"])
        self.assertEqual(1, selected_budget["provisional_ref_count"])
        self.assertEqual(1, dropped_budget["by_memory_layer"]["profile_summary"]["refs"])
        self.assertEqual(13, dropped_budget["by_memory_layer"]["profile_summary"]["tokens"])
        self.assertEqual(1, dropped_budget["by_memory_scope"]["session"]["refs"])
        self.assertEqual(1, dropped_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, dropped_budget["by_session_continuity"]["same_session"]["refs"])
        self.assertEqual(1, dropped_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, dropped_budget["by_extraction_phase"]["final"]["refs"])
        self.assertEqual(1, dropped_budget["by_extraction_phase"]["provisional"]["refs"])
        self.assertEqual(1, dropped_budget["by_profile_promotion_policy"]["always_when_profile_scope_available"]["refs"])
        self.assertEqual(1, dropped_budget["by_profile_promotion_blocker"]["profile_scope_missing"]["refs"])
        self.assertEqual(1, dropped_budget["final_ref_count"])
        self.assertEqual(1, dropped_budget["provisional_ref_count"])
        self.assertEqual(
            {"selected_refs": 1, "selected_tokens": 11, "dropped_refs": 0, "dropped_tokens": 0, "selected_and_dropped": False},
            pressure["by_dimension"]["by_memory_layer"]["profile_entity"],
        )
        self.assertEqual(
            {"selected_refs": 0, "selected_tokens": 0, "dropped_refs": 1, "dropped_tokens": 13, "selected_and_dropped": False},
            pressure["by_dimension"]["by_memory_layer"]["profile_summary"],
        )
        self.assertTrue(pressure["summary_layer_pressure"])
        self.assertFalse(pressure["profile_entity_pressure"])
        self.assertTrue(pressure["profile_promotion_policy_pressure"])
        self.assertTrue(pressure["profile_promotion_blocker_pressure"])
        self.assertEqual({"assistant": 3, "user": 1}, selected_budget["source_message_counts_by_role"])
        self.assertEqual({"assistant": 2, "tool": 1}, dropped_budget["source_message_counts_by_role"])
        self.assertEqual({"hook_boundary": 4}, selected_budget["source_hook_counts_by_type"])
        self.assertEqual({"Stop": 3}, dropped_budget["source_codex_event_counts_by_event"])
        self.assertEqual(
            {"selected_count": 3, "dropped_count": 2, "selected_and_dropped": True},
            pressure["by_dimension"]["source_message_counts_by_role"]["assistant"],
        )
        self.assertTrue(pressure["assistant_source_message_pressure"])
        self.assertTrue(pressure["tool_source_message_pressure"])
        self.assertTrue(pressure["hook_boundary_source_pressure"])
        self.assertFalse(pressure["tool_result_source_pressure"])
        self.assertTrue(pressure["stop_event_source_pressure"])
        self.assertFalse(pressure["post_tool_use_source_pressure"])
        self.assertGreaterEqual(pressure["dropped_bucket_count"], 1)

    def test_memory_layer_budget_normalizes_llm_role_aliases(self) -> None:
        selected_budget = selected_ref_layer_budget(
            [
                {
                    "ref_type": "entity",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_roles": ["llm", "model", "assistant"],
                    "source_role_counts": {"llm": 1, "model": 2, "assistant": 3},
                    "entity_type": "assistant_decision",
                    "token_estimate": 17,
                }
            ]
        )
        dropped_budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        "drop_reason": "over_budget",
                        "ref_type": "summary",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "source_roles": ["model"],
                        "source_role_counts": {"model": 4},
                        "summary_type": "node_l0",
                        "token_estimate": 19,
                    }
                ]
            }
        )
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)

        self.assertEqual({"assistant": {"refs": 1, "tokens": 17}}, selected_budget["by_source_role"])
        self.assertEqual({"assistant": 6}, selected_budget["source_message_counts_by_role"])
        self.assertEqual({"assistant": {"refs": 1, "tokens": 19}}, dropped_budget["by_source_role"])
        self.assertEqual({"assistant": 4}, dropped_budget["source_message_counts_by_role"])
        self.assertNotIn("llm", selected_budget["by_source_role"])
        self.assertNotIn("model", selected_budget["source_message_counts_by_role"])
        self.assertTrue(pressure["assistant_memory_pressure"])
        self.assertTrue(pressure["assistant_source_message_pressure"])
        self.assertEqual(
            {"selected_count": 6, "dropped_count": 4, "selected_and_dropped": True},
            pressure["by_dimension"]["source_message_counts_by_role"]["assistant"],
        )

    def test_memory_layer_budget_derives_source_buckets_from_count_only_refs(self) -> None:
        selected_budget = selected_ref_layer_budget(
            [
                {
                    "ref_type": "entity",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_role_counts": {"llm": 1, "model": 1, "tool": 2},
                    "source_hook_type_counts": {"after_llm": 2, "hook_boundary": 2},
                    "source_codex_event_counts": {"Stop": 2, "PostToolUse": 2},
                    "token_estimate": 21,
                }
            ]
        )
        dropped_budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        "drop_reason": "cross_session_budget",
                        "ref_type": "summary",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "budget_source_role_counts": {"model": 3, "tool": 1},
                        "source_hook_type_counts": {"after_llm": 3, "hook_boundary": 1},
                        "source_codex_event_counts": {"Stop": 3, "PostToolUse": 1},
                        "token_estimate": 8,
                    }
                ]
            }
        )
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)

        self.assertEqual({"assistant": {"refs": 1, "tokens": 21}, "tool": {"refs": 1, "tokens": 21}}, selected_budget["by_source_role"])
        self.assertEqual({"after_llm": {"refs": 1, "tokens": 21}, "hook_boundary": {"refs": 1, "tokens": 21}}, selected_budget["by_hook_type"])
        self.assertEqual({"PostToolUse": {"refs": 1, "tokens": 21}, "Stop": {"refs": 1, "tokens": 21}}, selected_budget["by_codex_event"])
        self.assertEqual({"assistant": 2, "tool": 2}, selected_budget["source_message_counts_by_role"])
        self.assertEqual({"assistant": {"refs": 1, "tokens": 8}, "tool": {"refs": 1, "tokens": 8}}, dropped_budget["by_source_role"])
        self.assertEqual({"after_llm": {"refs": 1, "tokens": 8}, "hook_boundary": {"refs": 1, "tokens": 8}}, dropped_budget["by_hook_type"])
        self.assertEqual({"PostToolUse": {"refs": 1, "tokens": 8}, "Stop": {"refs": 1, "tokens": 8}}, dropped_budget["by_codex_event"])
        self.assertTrue(pressure["assistant_memory_pressure"])
        self.assertTrue(pressure["tool_memory_pressure"])
        self.assertTrue(pressure["hook_boundary_source_pressure"])
        self.assertTrue(pressure["post_tool_use_source_pressure"])
        self.assertTrue(pressure["stop_event_source_pressure"])

    def test_memory_layer_budget_reads_metadata_backed_recovered_refs(self) -> None:
        selected_budget = selected_ref_layer_budget(
            [
                {
                    "metadata": {
                        "ref_type": "entity",
                        "entity_type": "assistant_decision",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "extraction_phase": "final",
                        "token_estimate": 17,
                        "final_session_boundary": True,
                        "source_role_counts": {"llm": 2},
                        "source_hook_type_counts": {"after_llm": 1},
                        "source_codex_event_counts": {"Stop": 1},
                        "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                    },
                }
            ]
        )
        dropped_budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        "metadata": {
                            "ref_type": "entity",
                            "entity_type": "assistant_decision",
                            "memory_scope": "session",
                            "session_continuity": "same_session",
                            "extraction_phase": "final",
                            "token_estimate": 11,
                            "drop_reason": "profile_shadowed",
                            "stale_or_superseded": True,
                            "profile_shadowed_by_ref_hash": 777,
                            "profile_shadowed_reason": "newer_profile_entity",
                            "source_role_counts": {"tool_result": 1},
                            "source_hook_type_counts": {"PostToolUse": 1},
                            "source_codex_event_counts": {"PostToolUse": 1},
                            "source_memory_selection_policies": ["selected_tool_evidence_only"],
                        },
                    }
                ]
            }
        )

        self.assertEqual(1, selected_budget["by_memory_layer"]["profile_entity"]["refs"])
        self.assertEqual(17, selected_budget["by_memory_layer"]["profile_entity"]["tokens"])
        self.assertEqual(1, selected_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, selected_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, selected_budget["by_entity_type"]["assistant_decision"]["refs"])
        self.assertEqual(1, selected_budget["final_session_boundary_ref_count"])
        self.assertEqual(2, selected_budget["source_message_counts_by_role"]["assistant"])
        self.assertEqual(1, selected_budget["source_hook_counts_by_type"]["after_llm"])
        self.assertEqual(1, selected_budget["source_codex_event_counts_by_event"]["Stop"])
        self.assertEqual(
            1,
            selected_budget["by_memory_selection_policy"]["selected_assistant_decision_outcome_only"]["refs"],
        )

        self.assertEqual(1, dropped_budget["by_memory_layer"]["same_session_entity"]["refs"])
        self.assertEqual(11, dropped_budget["by_drop_reason"]["profile_shadowed"]["tokens"])
        self.assertEqual(1, dropped_budget["by_memory_scope"]["session"]["refs"])
        self.assertEqual(1, dropped_budget["by_session_continuity"]["same_session"]["refs"])
        self.assertEqual(1, dropped_budget["by_entity_type"]["assistant_decision"]["refs"])
        self.assertEqual(1, dropped_budget["stale_ref_count"])
        self.assertEqual(1, dropped_budget["profile_shadowed_ref_count"])
        self.assertEqual(1, dropped_budget["by_profile_shadowed_reason"]["newer_profile_entity"]["refs"])
        self.assertEqual(1, dropped_budget["source_message_counts_by_role"]["tool"])
        self.assertEqual(1, dropped_budget["source_hook_counts_by_type"]["PostToolUse"])
        self.assertEqual(1, dropped_budget["source_codex_event_counts_by_event"]["PostToolUse"])
        self.assertEqual(
            1,
            dropped_budget["by_memory_selection_policy"]["selected_tool_evidence_only"]["refs"],
        )

        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)
        self.assertTrue(pressure["profile_shadowed_current_state_pressure"])
        self.assertTrue(pressure["stale_current_state_pressure"])
        self.assertTrue(pressure["tool_source_message_pressure"])
        self.assertTrue(pressure["post_tool_use_source_pressure"])
        self.assertTrue(pressure["memory_selection_policy_pressure"])
        self.assertIn("by_memory_selection_policy", pressure["dropped_dimensions"])
        self.assertEqual(
            1,
            pressure["by_dimension"]["by_memory_selection_policy"]["selected_tool_evidence_only"]["dropped_refs"],
        )

    def test_recovery_report_surfaces_memory_selection_policy_budget_pressure(self) -> None:
        report = matrixark_local_recovery_report(
            [
                {
                    "record_type": "context_pack_telemetry",
                    "memory_layer_budget": {
                        "by_memory_selection_policy": {
                            "selected_assistant_decision_outcome_only": {"refs": 1, "tokens": 9},
                        },
                    },
                    "dropped_memory_layer_budget": {
                        "by_memory_selection_policy": {
                            "selected_tool_evidence_only": {"refs": 2, "tokens": 17},
                        },
                    },
                    "memory_layer_pressure": {
                        "memory_selection_policy_pressure": True,
                        "dropped_dimensions": ["by_memory_selection_policy"],
                    },
                }
            ]
        )

        self.assertEqual(
            {"selected_assistant_decision_outcome_only": {"refs": 1, "tokens": 9}},
            report["retrieval_visibility"]["selected_budget_by_memory_selection_policy"],
        )
        self.assertEqual(
            {"selected_tool_evidence_only": {"refs": 2, "tokens": 17}},
            report["retrieval_visibility"]["dropped_budget_by_memory_selection_policy"],
        )
        self.assertEqual(1, report["retrieval_visibility"]["memory_layer_pressure_flags"]["memory_selection_policy"])

    def test_candidate_index_terms_normalize_llm_role_aliases(self) -> None:
        summary_terms = candidate_index_terms(
            {
                "record_type": "context_summary",
                "summary_type": "node_l0",
                "source_roles": ["llm", "model", "assistant"],
            },
            {},
            {},
            {},
        )
        compression_terms = candidate_index_terms(
            {
                "record_type": "context_compression_event",
                "compression_id_hash": 901,
                "source_roles": ["model", "assistant"],
            },
            {},
            {},
            {},
        )

        for terms in [summary_terms, compression_terms]:
            self.assertIn("source_role:assistant", terms)
            self.assertNotIn("source_role:llm", terms)
            self.assertNotIn("source_role:model", terms)

    def test_candidate_index_terms_include_count_only_live_memory_lineage(self) -> None:
        entity_terms = candidate_index_terms(
            {
                "record_type": "context_entity",
                "entity_type": "assistant_decision",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "source_role_counts": {"llm": 2, "tool_result": 1, "bad": "n/a"},
                "source_hook_type_counts": {"hook_boundary": 2},
                "source_codex_event_counts": {"Stop": 2},
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 2},
            },
            {},
            {},
            {},
        )
        summary_terms = candidate_index_terms(
            {
                "record_type": "context_summary",
                "summary_type": "node_l1",
                "source_role_counts": {"model": 3},
                "source_memory_scopes": ["user_profile"],
                "source_memory_selection_policies": ["selected_tool_evidence_only"],
                "source_session_continuities": ["cross_session"],
                "source_extraction_phases": ["final"],
            },
            {},
            {},
            {},
        )
        compression_terms = candidate_index_terms(
            {
                "record_type": "context_compression_event",
                "compression_id_hash": 902,
                "source_role_counts": {"assistant_response": 1},
                "source_hook_type_counts": {"PostToolUse": 1},
                "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
            },
            {},
            {},
            {},
        )
        event_terms = candidate_index_terms(
            {
                "record_type": "context_event",
                "event_type": "dialogue_batch",
                "source_role_counts": {"tool_output": 1},
                "source_codex_event_counts": {"PostToolUse": 1},
                "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
            },
            {},
            {},
            {},
        )

        self.assertIn("entity_type:assistant_decision", entity_terms)
        self.assertIn("source_role:assistant", entity_terms)
        self.assertIn("source_role:tool", entity_terms)
        self.assertIn("hook_type:hook_boundary", entity_terms)
        self.assertIn("codex_event:stop", entity_terms)
        self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", entity_terms)
        self.assertIn("memory_scope:user_profile", entity_terms)
        self.assertIn("session_continuity:cross_session", entity_terms)
        self.assertIn("extraction_phase:final", entity_terms)
        self.assertIn("source_role:assistant", summary_terms)
        self.assertIn("memory_scope:user_profile", summary_terms)
        self.assertIn("session_continuity:cross_session", summary_terms)
        self.assertIn("extraction_phase:final", summary_terms)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", summary_terms)
        self.assertIn("source_role:assistant", compression_terms)
        self.assertIn("hook_type:posttooluse", compression_terms)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", compression_terms)
        self.assertIn("source_role:tool", event_terms)
        self.assertIn("codex_event:posttooluse", event_terms)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", event_terms)

    def test_time_compression_preserves_source_lineage_for_budgeting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-compression-lineage.jsonl")
            scope = {
                "account_id": "acct_compress",
                "tenant_id": "tenant_compress",
                "user_id": "user_compress",
                "session_id": "session_compress",
            }
            scope.update(identity_hashes(scope["account_id"], scope["tenant_id"], scope["user_id"], scope["session_id"]))
            scope["_explicit_scope_keys"] = ["account_id", "tenant_id", "user_id", "session_id"]
            node_hash = 424242
            node_path = ["tenant:tenant_compress", "user:user_compress", "session:session_compress"]
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 101,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "assistant: Commit d0152479 pushed after validation.",
                        "summary_text": "assistant decision pushed",
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 1},
                        "source_codex_events": ["Stop"],
                        "source_codex_event_counts": {"Stop": 1},
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "envelope": {"scope": scope, "ingestion_time_ms": 1000},
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": 102,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "tool: Exit code: 0 from hook validation.",
                        "summary_text": "tool evidence passed",
                        "source_roles": ["tool"],
                        "source_role_counts": {"tool": 1},
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 1},
                        "source_codex_events": ["PostToolUse"],
                        "source_codex_event_counts": {"PostToolUse": 1},
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "envelope": {"scope": scope, "ingestion_time_ms": 1100},
                        "updated_at_ms": 1100,
                    },
                ]
            )

            compression = adapter.write_time_compression(
                scope=scope,
                node_hash=node_hash,
                node_path=node_path,
                source_start_ms=900,
                source_end_ms=1200,
                compressed_time_ms=2000,
                summary="Compressed assistant decision and tool validation evidence.",
            )

            self.assertEqual(["assistant", "tool"], compression["source_roles"])
            self.assertEqual({"assistant": 1, "tool": 1}, compression["source_role_counts"])
            self.assertEqual(["hook_boundary"], compression["source_hook_types"])
            self.assertEqual({"hook_boundary": 2}, compression["source_hook_type_counts"])
            self.assertEqual(["PostToolUse", "Stop"], compression["source_codex_events"])
            self.assertEqual({"Stop": 1, "PostToolUse": 1}, compression["source_codex_event_counts"])
            self.assertEqual(["session"], compression["source_memory_scopes"])
            self.assertEqual(["same_session"], compression["source_session_continuities"])
            self.assertEqual(["final"], compression["source_extraction_phases"])
            self.assertEqual(2, compression["source_final_session_boundary_count"])
            self.assertEqual("session", compression["memory_scope"])
            self.assertEqual("same_session", compression["session_continuity"])
            self.assertEqual("final", compression["extraction_phase"])
            self.assertTrue(compression["final_session_boundary"])
            compression_index_names = {
                record.get("index_name")
                for record in adapter.read_all()
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_compression_event"
                and record.get("ref_type") == "compression"
                and compression["compression_id_hash"] in record.get("ref_hashes", [])
            }
            self.assertIn("context_class:compression", compression_index_names)
            self.assertIn("operator:TIME_COMPRESS", compression_index_names)
            self.assertIn("source_role:assistant", compression_index_names)
            self.assertIn("source_role:tool", compression_index_names)
            self.assertIn("hook_type:hook_boundary", compression_index_names)
            self.assertIn("codex_event:Stop", compression_index_names)
            self.assertIn("codex_event:PostToolUse", compression_index_names)
            self.assertIn("memory_scope:session", compression_index_names)
            self.assertIn("session_continuity:same_session", compression_index_names)
            self.assertIn("extraction_phase:final", compression_index_names)
            index_terms_by_ref: dict[int, list[str]] = {}
            index_terms_by_node: dict[int, list[str]] = {}
            for record in adapter.read_all():
                if record.get("record_type") != "context_index":
                    continue
                for ref_hash in record.get("ref_hashes", []):
                    index_terms_by_ref.setdefault(ref_hash, []).append(record.get("index_name", ""))
                if record.get("node_hash") is not None:
                    index_terms_by_node.setdefault(record["node_hash"], []).append(record.get("index_name", ""))
            compression_terms = candidate_index_terms(compression, {}, index_terms_by_node, index_terms_by_ref)
            self.assertIn("context_class:compression", compression_terms)
            self.assertIn("operator:time_compress", compression_terms)
            self.assertIn("source_role:assistant", compression_terms)
            self.assertIn("source_role:tool", compression_terms)
            self.assertIn("hook_type:hook_boundary", compression_terms)
            self.assertIn("codex_event:stop", compression_terms)
            self.assertIn("codex_event:posttooluse", compression_terms)
            self.assertIn("memory_scope:session", compression_terms)
            self.assertIn("session_continuity:same_session", compression_terms)
            self.assertIn("extraction_phase:final", compression_terms)

            budget = selected_ref_layer_budget(
                [
                    {
                        **compression,
                        "ref_type": "compression",
                        "ref_hash": compression["compression_id_hash"],
                        "token_estimate": 17,
                        "text": compression["summary_text"],
                    }
                ]
            )
            self.assertEqual(1, budget["by_memory_layer"]["compression"]["refs"])
            self.assertEqual(17, budget["by_memory_layer"]["compression"]["tokens"])
            self.assertEqual(1, budget["by_memory_scope"]["session"]["refs"])
            self.assertEqual(1, budget["by_session_continuity"]["same_session"]["refs"])
            self.assertEqual(1, budget["by_extraction_phase"]["final"]["refs"])
            self.assertEqual(1, budget["by_source_role"]["assistant"]["refs"])
            self.assertEqual(1, budget["by_source_role"]["tool"]["refs"])
            self.assertEqual(1, budget["by_hook_type"]["hook_boundary"]["refs"])
            self.assertEqual(1, budget["by_codex_event"]["Stop"]["refs"])
            self.assertEqual(1, budget["by_codex_event"]["PostToolUse"]["refs"])
            self.assertEqual({"assistant": 1, "tool": 1}, budget["source_message_counts_by_role"])
            self.assertEqual({"hook_boundary": 2}, budget["source_hook_counts_by_type"])
            self.assertEqual({"Stop": 1, "PostToolUse": 1}, budget["source_codex_event_counts_by_event"])
            self.assertEqual(1, budget["final_session_boundary_ref_count"])
            self.assertEqual(1, budget["final_ref_count"])

    def test_source_role_budget_caps_assistant_context_without_blocking_user_refs(self) -> None:
        selected, used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 101,
                    "text": "alpha",
                    "score": 0.95,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 102,
                    "text": "bravo",
                    "score": 0.94,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                },
                {
                    "ref_type": "event",
                    "ref_hash": 103,
                    "text": "charlie",
                    "score": 0.93,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "source_roles": ["user"],
                    "source_role_counts": {"user": 1},
                },
            ],
            [],
            max_context_tokens=20,
            auxiliary_quota=0,
            min_score=0.0,
            max_selected_refs=5,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 20,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            source_role_budget_tokens={"llm": 1},
        )

        selected_hashes = [ref["ref_hash"] for ref in selected]
        self.assertEqual([101, 103], selected_hashes)
        self.assertEqual(2, used_tokens)
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertEqual(1, dropped["estimated_tokens"]["source_role_budget"])
        self.assertEqual({"assistant": 1}, dropped["source_role_budget_policy"]["budget_tokens"])
        self.assertEqual({"assistant": 1}, dropped["source_role_budget_policy"]["selected_tokens_by_role"])
        self.assertEqual(["assistant"], dropped["refs"][0]["source_role_budget_capped_roles"])

        compact_dropped = compact_dropped_refs_for_context_pack(dropped)
        self.assertEqual(1, compact_dropped["source_role_budget"])
        self.assertEqual(1, compact_dropped["estimated_tokens"]["source_role_budget"])
        self.assertEqual(
            {"assistant": 1},
            compact_dropped["source_role_budget_policy"]["selected_tokens_by_role"],
        )

        selected_budget = selected_ref_layer_budget(selected)
        dropped_budget = dropped_ref_layer_budget(dropped)
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)
        self.assertEqual({"assistant": 1, "user": 1}, selected_budget["source_message_counts_by_role"])
        self.assertEqual({"assistant": 1}, dropped_budget["source_message_counts_by_role"])
        self.assertEqual(
            {"selected_count": 1, "dropped_count": 1, "selected_and_dropped": True},
            pressure["by_dimension"]["source_message_counts_by_role"]["assistant"],
        )
        self.assertTrue(pressure["assistant_source_message_pressure"])

    def test_source_role_budget_fallback_allows_clipped_summary_within_role_budget(self) -> None:
        selected, used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "summary",
                    "ref_hash": 140,
                    "text": "assistant summary contains a long decision memory that can be clipped for the role budget",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                }
            ],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            budget_fill_policy="force_fill",
            max_selected_refs=1,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 100,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            source_role_budget_tokens={"assistant": 8},
        )

        self.assertEqual([140], [ref["ref_hash"] for ref in selected])
        self.assertEqual(8, used_tokens)
        self.assertEqual(8, selected[0]["token_estimate"])
        self.assertEqual(["assistant"], selected[0]["budget_source_roles"])
        self.assertEqual({"assistant": 1}, selected[0]["budget_source_role_counts"])
        self.assertLessEqual(len(selected[0]["text"].split()), 8)
        self.assertEqual(0, dropped["source_role_budget"])
        self.assertEqual({"assistant": 8}, dropped["source_role_budget_policy"]["selected_tokens_by_role"])
        self.assertEqual({"assistant": 1}, dropped["source_role_budget_policy"]["selected_ref_count_by_role"])

    def test_source_role_budget_fallback_does_not_reinsert_capped_assistant_summary(self) -> None:
        selected, used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "summary",
                    "ref_hash": 141,
                    "text": "assistant summary contains a long decision memory that exceeds the assistant budget",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                }
            ],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            budget_fill_policy="force_fill",
            max_selected_refs=1,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 100,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            source_role_budget_tokens={"assistant": 1},
        )

        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertGreater(dropped["estimated_tokens"]["source_role_budget"], 1)
        self.assertTrue(
            any(
                ref.get("ref_hash") == 141
                and ref.get("drop_reason") == "source_role_budget"
                and ref.get("source_role_budget_capped_roles") == ["assistant"]
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )
        compact_dropped = compact_dropped_refs_for_context_pack(dropped)
        self.assertEqual(1, compact_dropped["source_role_budget"])
        self.assertEqual({"assistant": 0}, compact_dropped["source_role_budget_policy"]["selected_tokens_by_role"])

    def test_memory_selection_policy_budget_caps_assistant_decision_without_blocking_tool_evidence(self) -> None:
        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 158,
                    "text": "assistant_decision: keep the verbose implementation decision out when policy capped",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                    "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                    "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 159,
                    "text": "tool_evidence: tests passed",
                    "score": 0.82,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "tool_evidence",
                    "source_roles": ["tool"],
                    "source_memory_selection_policies": ["selected_tool_evidence_only"],
                    "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            question_type="broad_exploration",
            min_score=0.0,
            max_selected_refs=2,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            memory_selection_policy_budget_tokens={"selected_assistant_decision_outcome_only": 1},
        )

        self.assertEqual([159], [ref["ref_hash"] for ref in selected])
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
        self.assertTrue(
            any(
                ref.get("ref_hash") == 158
                and ref.get("drop_reason") == "memory_selection_policy_budget"
                and ref.get("memory_selection_policy_budget_capped_policies")
                == ["selected_assistant_decision_outcome_only"]
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )
        compact_dropped = compact_dropped_refs_for_context_pack(dropped)
        self.assertEqual(1, compact_dropped["memory_selection_policy_budget"])

    def test_source_role_budget_caps_assistant_summary_without_blocking_tool_entity(self) -> None:
        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "summary",
                    "ref_hash": 151,
                    "text": "assistant summary says tests passed and commit was pushed",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant", "tool"],
                    "source_role_counts": {"assistant": 2, "tool": 1},
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 152,
                    "text": "tool_evidence: Exit code: 0; tests passed",
                    "score": 0.72,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "tool_evidence",
                    "entity_name": "tests passed",
                    "source_roles": ["assistant", "tool"],
                    "source_role_counts": {"assistant": 2, "tool": 1},
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            question_type="broad_exploration",
            min_score=0.0,
            max_selected_refs=2,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            source_role_budget_tokens={"assistant": 1},
        )

        self.assertEqual([152], [ref["ref_hash"] for ref in selected])
        self.assertEqual(["tool"], selected[0]["budget_source_roles"])
        self.assertEqual({"tool": 1}, selected[0]["budget_source_role_counts"])
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertTrue(
            any(
                ref.get("ref_hash") == 151
                and ref.get("ref_type") == "summary"
                and ref.get("drop_reason") == "source_role_budget"
                and ref.get("source_role_budget_capped_roles") == ["assistant"]
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )
        selected_budget = selected_ref_layer_budget(selected)
        dropped_budget = dropped_ref_layer_budget(dropped)
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)
        self.assertEqual(1, selected_budget["by_memory_layer"]["profile_entity"]["refs"])
        self.assertEqual(1, dropped_budget["by_memory_layer"]["profile_summary"]["refs"])
        self.assertEqual({"tool": 1}, selected_budget["source_message_counts_by_role"])
        self.assertEqual({"assistant": 2, "tool": 1}, dropped_budget["source_message_counts_by_role"])
        self.assertTrue(pressure["summary_layer_pressure"])
        self.assertTrue(pressure["summary_memory_pressure"])
        self.assertTrue(pressure["assistant_source_message_pressure"])

    def test_memory_layer_budget_caps_summary_without_blocking_profile_entity(self) -> None:
        selected, used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "summary",
                    "ref_hash": 161,
                    "text": "assistant summary pushed",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant"],
                },
                {
                    "ref_type": "summary",
                    "ref_hash": 162,
                    "text": "assistant summary contains another pushed commit decision",
                    "score": 0.98,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant"],
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 163,
                    "text": "assistant_decision: keep profile entity retrieval available",
                    "score": 0.72,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            question_type="broad_exploration",
            min_score=0.0,
            max_selected_refs=3,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            memory_layer_budget_tokens={"profile_summary": 6},
        )

        selected_hashes = [ref["ref_hash"] for ref in selected]
        self.assertEqual(2, len(selected_hashes))
        self.assertEqual(161, selected_hashes[0])
        self.assertEqual(163, selected_hashes[1])
        self.assertEqual(9, used_tokens)
        self.assertEqual("profile_summary", selected[0]["budget_memory_layer"])
        self.assertEqual("profile_entity", selected[1]["budget_memory_layer"])
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual({"profile_summary": 6}, dropped["memory_layer_budget_policy"]["budget_tokens"])
        self.assertEqual({"profile_summary": 3}, dropped["memory_layer_budget_policy"]["selected_tokens_by_layer"])
        self.assertEqual({"profile_summary": 1}, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"])
        dropped_summary_hash = 162
        self.assertTrue(
            any(
                ref.get("ref_hash") == dropped_summary_hash
                and ref.get("drop_reason") == "memory_layer_budget"
                and ref.get("memory_layer_budget_capped_layer") == "profile_summary"
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )
        compact_dropped = compact_dropped_refs_for_context_pack(dropped)
        self.assertEqual(1, compact_dropped["memory_layer_budget"])
        self.assertEqual(
            {"profile_summary": 3},
            compact_dropped["memory_layer_budget_policy"]["selected_tokens_by_layer"],
        )

    def test_memory_layer_floor_keeps_profile_entity_ahead_of_refreshed_summary(self) -> None:
        selected, used_tokens, dropped = select_token_budgeted_refs(
            [
                {
                    "ref_type": "summary",
                    "ref_hash": 171,
                    "text": "assistant summary: pre-retrieval refresh created a compact rollout memory",
                    "score": 0.99,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "summary_type": "node_l0",
                    "source_roles": ["assistant"],
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 172,
                    "text": "assistant_decision: keep direct profile entity evidence available before summaries",
                    "score": 0.61,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            question_type="broad_exploration",
            min_score=0.0,
            max_selected_refs=1,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 0,
            },
            memory_layer_budget_tokens={"profile_summary": 32, "profile_entity": 32},
        )

        self.assertEqual([172], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual("profile_entity", selected[0]["budget_memory_layer"])
        self.assertEqual(1, dropped["memory_layer_floor"])
        self.assertGreater(dropped["estimated_tokens"]["memory_layer_floor"], 0)
        self.assertTrue(
            any(
                ref.get("ref_hash") == 171
                and ref.get("drop_reason") == "memory_layer_floor"
                and ref.get("memory_layer_floor_reserved_layer") == "profile_entity"
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )

    def test_current_state_retrieval_prefers_profile_entity_over_stale_session_and_summary(self) -> None:
        session_entity = {
            "ref_type": "entity",
            "ref_hash": 201,
            "text": "user_preference: recovery_mode = wait for Stop before extracting memories",
            "score": 0.96,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "entity_type": "user_preference",
            "entity_name": "recovery_mode",
            "updated_at_ms": 1000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 202,
            "text": "user_preference: recovery_mode = commit memories on threshold or idle timeout; Stop only flushes remaining pending messages",
            "score": 0.60,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "entity_type": "user_preference",
            "entity_name": "recovery_mode",
            "updated_at_ms": 2000,
            "source_entity_hashes": [201],
            "source_roles": ["user", "assistant"],
            "source_role_counts": {"user": 1, "assistant": 1},
        }
        summary = {
            "ref_type": "summary",
            "ref_hash": 203,
            "text": "summary: recovery memories previously discussed Stop-only extraction.",
            "score": 0.99,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "summary_type": "session_l0",
            "updated_at_ms": 1500,
            "source_roles": ["assistant"],
            "source_role_counts": {"assistant": 1},
        }

        selected, used_tokens, dropped = select_token_budgeted_refs(
            [session_entity, summary, profile_entity],
            [],
            max_context_tokens=32,
            auxiliary_quota=0,
            question_type="current_state",
            min_score=0.0,
            max_selected_refs=1,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 32,
                "max_sessions": 4,
                "max_candidates": 4,
                "min_entity_bridge_refs": 1,
            },
        )

        self.assertEqual([202], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual("user_profile", selected[0]["memory_scope"])
        self.assertEqual("cross_session", selected[0]["session_continuity"])
        self.assertEqual("current profile entity preferred over session-local historical state", selected[0]["selection_reason"])
        self.assertEqual(0.18, selected[0]["profile_current_state_boost"])
        self.assertEqual(1, dropped["stale"])
        self.assertTrue(
            any(
                ref.get("ref_hash") == 201
                and ref.get("drop_reason") == "stale"
                and ref.get("profile_shadowed_by_ref_hash") == 202
                for ref in dropped["refs"]
            )
        )
        self.assertTrue(
            any(
                ref.get("ref_hash") == 203
                and ref.get("drop_reason") == "max_selected_refs"
                for ref in dropped["refs"]
            )
        )
        selected_budget = selected_ref_layer_budget(selected)
        dropped_budget = dropped_ref_layer_budget(dropped)
        pressure = memory_layer_pressure_summary(selected_budget, dropped_budget)
        self.assertTrue(pressure["profile_memory_pressure"])
        self.assertTrue(pressure["summary_memory_pressure"])
        self.assertTrue(pressure["entity_memory_pressure"])
        self.assertTrue(pressure["stale_current_state_pressure"])
        self.assertTrue(pressure["profile_shadowed_current_state_pressure"])
        self.assertEqual(1, dropped_budget["profile_shadowed_ref_count"])
        self.assertEqual(
            1,
            pressure["by_dimension"]["by_profile_shadowed_reason"]["source_entity_lineage"]["dropped_refs"],
        )
        self.assertEqual(
            1,
            pressure["by_dimension"]["by_ref_type"]["summary"]["dropped_refs"],
        )

    def test_memory_layer_pressure_summary_marks_dropped_profile_final_tool_layers(self) -> None:
        selected = {
            "total_selected_refs": 2,
            "total_selected_tokens": 14,
            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 9}},
            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 9}},
            "by_extraction_phase": {"final": {"refs": 1, "tokens": 9}},
            "by_source_role": {"assistant": {"refs": 1, "tokens": 9}},
        }
        dropped = {
            "total_dropped_refs": 3,
            "total_dropped_tokens": 21,
            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 8}},
            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 8}},
            "by_extraction_phase": {"final": {"refs": 2, "tokens": 16}},
            "by_source_role": {"assistant": {"refs": 1, "tokens": 8}, "tool": {"refs": 1, "tokens": 5}},
        }
        pressure = memory_layer_pressure_summary(selected, dropped)
        self.assertEqual(2, pressure["selected_refs"])
        self.assertEqual(3, pressure["dropped_refs"])
        self.assertTrue(pressure["profile_memory_pressure"])
        self.assertTrue(pressure["cross_session_pressure"])
        self.assertTrue(pressure["final_memory_pressure"])
        self.assertTrue(pressure["assistant_memory_pressure"])
        self.assertTrue(pressure["tool_memory_pressure"])
        self.assertIn("by_memory_scope", pressure["pressure_dimensions"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_scope"]["user_profile"]["selected_refs"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_scope"]["user_profile"]["dropped_refs"])

    def test_pending_async_events_are_demoted_for_budget_packing(self) -> None:
        pending_event = {
            "ref_type": "event",
            "event_type": "pending_async",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "extraction_status": "pending",
            "extraction_mode": "async_pending",
            "score": 0.92,
            "text": "assistant: Commit d0152479 pushed and hook pipeline tests passed.",
        }
        ordinary_event = {
            **pending_event,
            "event_type": "assistant_decision",
            "classification": "ANSWER",
            "extraction_status": "observed",
            "extraction_mode": "one_pass",
            "text": "assistant_decision: Commit d0152479 pushed after extracted entity tests passed.",
        }
        extracted_entity = {
            "ref_type": "entity",
            "entity_type": "assistant_decision",
            "score": 0.76,
            "text": "assistant_decision Commit d0152479 pushed and hook pipeline tests passed.",
        }

        self.assertGreater(packing_sort_key(ordinary_event, "fact"), packing_sort_key(pending_event, "fact"))
        self.assertGreater(packing_sort_key(extracted_entity, "fact"), packing_sort_key(pending_event, "fact"))
        self.assertEqual("pending_async_event", candidate_memory_layer_name(pending_event))
        pending_budget = selected_ref_layer_budget([{**pending_event, "token_estimate": 7}])
        self.assertEqual(1, pending_budget["by_memory_layer"]["pending_async_event"]["refs"])
        self.assertEqual(7, pending_budget["by_memory_layer"]["pending_async_event"]["tokens"])

        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [
                {**pending_event, "ref_hash": 171, "token_estimate": 7},
                {**ordinary_event, "ref_hash": 172, "token_estimate": 7},
            ],
            [],
            max_context_tokens=32,
            auxiliary_quota=0,
            question_type="fact",
            min_score=0.0,
            max_selected_refs=2,
            memory_layer_budget_tokens={"pending_async_event": 1},
        )
        self.assertEqual([172], [ref["ref_hash"] for ref in selected])
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual("pending_async_event", dropped["refs"][0]["memory_layer_budget_capped_layer"])
        dropped_budget = dropped_ref_layer_budget(dropped)
        self.assertEqual(1, dropped_budget["by_memory_layer"]["pending_async_event"]["refs"])

    def test_fast_hook_pending_event_is_retrievable_before_batch_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-pending-retrieval.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            scope = {
                "account_id": "acct_fast_pending_retrieval",
                "tenant_id": "tenant_fast_pending_retrieval",
                "user_id": "user_fast_pending_retrieval",
                "session_id": "session_fast_pending_retrieval",
            }
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=300000,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                ),
                text="Remember live provisional event retrieval before batch extraction marker phoenix quartz.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={
                    "session_id_source": "payload_field",
                    "thread_id": "thread-fast-pending-retrieval",
                    "turn_id": "turn-fast-pending-retrieval-1",
                    "hook_type": "before_llm",
                },
            )
            self.assertEqual("accepted", result["status"])
            self.assertFalse(result["session_buffer"]["threshold_ready"])
            self.assertFalse(result["auto_batch_extract_result"])

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What marker proves live provisional event retrieval before batch extraction?",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                    },
                }
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "event"
                    and ref.get("event_type") == "pending_async"
                    and ref.get("memory_scope") == "session"
                    and ref.get("session_continuity") == "same_session"
                    and ref.get("extraction_phase") == "pending_async"
                    and "phoenix quartz" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )
            pending_ref = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "event" and ref.get("event_type") == "pending_async"
            )
            self.assertEqual("pending_async_event", candidate_memory_layer_name(pending_ref))
            budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_layer"]["pending_async_event"]["refs"], 1)
            self.assertGreaterEqual(budget["by_extraction_phase"]["pending_async"]["refs"], 1)
            readiness = pack["retrieval_metrics"]["async_pipeline_readiness"]
            self.assertFalse(readiness["ready_for_retrieval"])
            self.assertEqual({"user": 1}, readiness["pending_source_roles"])
            self.assertEqual({"before_llm": 1}, readiness["pending_source_hook_types"])
            self.assertEqual({"UserPromptSubmit": 1}, readiness["pending_source_codex_events"])
            self.assertEqual({"session": 1, "user_profile": 1}, readiness["pending_memory_scopes"])
            self.assertEqual({"cross_session": 1, "same_session": 1}, readiness["pending_session_continuities"])
            self.assertEqual({"pending_async": 1}, readiness["pending_extraction_phases"])
            self.assertEqual(
                {"compression": 1, "embedding": 1, "extraction": 1, "summary": 1},
                readiness["remaining_stage_counts"],
            )
            self.assertIn(
                "async_pipeline_remaining_stages:compression,embedding,extraction,summary",
                readiness["freshness_warnings"],
            )
            self.assertIn("profile_memory_stale", readiness["freshness_warnings"])
            self.assertIn("cross_session_memory_stale", readiness["freshness_warnings"])
            self.assertIn("session", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("user_profile", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("same_session", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("cross_session", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("summary", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("compression", readiness["memory_layer_readiness"]["blocked_layers"])
            self.assertIn("embedding", readiness["memory_layer_readiness"]["blocked_layers"])
            coverage = pack["retrieval_metrics"]["retrieval_model_coverage"]
            self.assertGreaterEqual(coverage["event_embedding_vectors"], 1)
            self.assertGreaterEqual(coverage["index_terms_by_ref"], 1)

            compact_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What marker proves live provisional event retrieval before batch extraction?",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                    },
                }
            )
            compact_ref = next(
                ref
                for ref in compact_pack["selected_refs"]
                if ref.get("ref_type") == "event" and "phoenix quartz" in str(ref.get("text") or "")
            )
            self.assertEqual("pending_async", compact_ref["event_type"])
            self.assertNotIn("matched_index_terms", compact_ref)
            self.assertNotIn("metadata", compact_ref)
            self.assertNotIn("scope", compact_ref)

    def test_fast_hook_dirty_markers_refresh_into_retrievable_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-summary.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_fast",
                tenant_id="tenant_fast",
                user_id="user_fast",
                session_id="session_fast_summary",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=args,
                text="Remember that the fast hook summary refresh path must turn dirty markers into retrievable summaries.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
            self.assertEqual("accepted", result["status"])
            self.assertGreaterEqual(result["summary_dirty_count"], 1)

            refresh = adapter.refresh_summaries(
                {
                    "scope": {
                        "account_id": "acct_fast",
                        "tenant_id": "tenant_fast",
                        "user_id": "user_fast",
                        "session_id": "session_fast_summary",
                    },
                    "limit": 16,
                    "refreshed_at_ms": int(time.time() * 1000) + 1000,
                }
            )
            self.assertGreaterEqual(refresh.get("refreshed_count", 0), 1)
            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("status") == "completed"
                    for record in records
                )
            )
            self.assertTrue(any(record.get("record_type") == "context_summary" for record in records))

            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_fast",
                        "tenant_id": "tenant_fast",
                        "user_id": "user_fast",
                        "session_id": "session_fast_summary",
                    },
                    "query": "Summarize the fast hook summary refresh path.",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            self.assertLessEqual(pack["used_context_tokens"], 100)
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]))

    def test_pre_retrieval_refresh_can_skip_pending_new_event_dirty_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-skip-new-event-summary.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_fast_skip",
                tenant_id="tenant_fast_skip",
                user_id="user_fast_skip",
                session_id="session_fast_skip_summary",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=args,
                text="Remember that pre-retrieval summary refresh must not summarize pending current-turn raw events.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
            self.assertGreaterEqual(result["summary_dirty_count"], 1)

            skipped = adapter.refresh_summaries(
                {
                    "scope": {
                        "account_id": "acct_fast_skip",
                        "tenant_id": "tenant_fast_skip",
                        "user_id": "user_fast_skip",
                        "session_id": "session_fast_skip_summary",
                    },
                    "limit": 16,
                    "skip_dirty_reasons": ["new_event"],
                }
            )
            self.assertEqual(0, skipped.get("refreshed_count", 0))
            self.assertGreaterEqual(skipped.get("skipped_dirty_count", 0), 1)
            self.assertGreaterEqual(skipped.get("skipped_dirty_reasons", {}).get("new_event", 0), 1)
            self.assertFalse(any(record.get("record_type") == "context_summary" for record in adapter.read_all()))

            refreshed = adapter.refresh_summaries(
                {
                    "scope": {
                        "account_id": "acct_fast_skip",
                        "tenant_id": "tenant_fast_skip",
                        "user_id": "user_fast_skip",
                        "session_id": "session_fast_skip_summary",
                    },
                    "limit": 16,
                }
            )
            self.assertGreaterEqual(refreshed.get("refreshed_count", 0), 1)

    def test_retrieve_can_pre_refresh_dirty_summaries_before_serving(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-pre-retrieve-summary.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            scope = {
                "account_id": "acct_pre_refresh",
                "tenant_id": "tenant_pre_refresh",
                "user_id": "user_pre_refresh",
                "session_id": "session_pre_refresh",
            }
            args = Namespace(
                event="UserPromptSubmit",
                **scope,
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            ingest = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=args,
                text="Remember that pre-retrieval summary refresh should drain dirty summary nodes before serving Codex memory.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
            self.assertEqual("accepted", ingest["status"])
            self.assertGreaterEqual(ingest["summary_dirty_count"], 1)
            self.assertFalse(any(record.get("record_type") == "context_summary" for record in adapter.read_all()))

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Summarize pre-retrieval summary refresh for Codex memory.",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 2,
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            refresh = pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertTrue(any(record.get("record_type") == "context_summary" for record in adapter.read_all()))
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]))
            self.assertLessEqual(pack["used_context_tokens"], 120)

    def test_fast_hook_threshold_commit_persists_real_adapter_memory_layers(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-threshold.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_threshold",
                    "tenant_id": "tenant_fast_threshold",
                    "user_id": "user_fast_threshold",
                    "session_id": "session_fast_threshold",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 2,
                    "idle_commit_timeout_ms": 300000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                server = Server()
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="User prompt: make live fast hook extraction fire on threshold.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold",
                        "turn_id": "turn-fast-threshold-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertEqual(1, first["session_buffer"]["pending_event_count"])
                self.assertEqual(1, first["session_buffer"]["pending_message_count"])
                self.assertFalse(first["auto_batch_extract_result"])

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="Assistant decision: threshold commits should extract session and profile memory now.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold",
                        "turn_id": "turn-fast-threshold-2",
                    },
                )
                commit = second["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual("provisional", commit["extraction_phase"])
                self.assertFalse(commit["final_session_boundary"])
                self.assertEqual(2, commit["committed_event_count"])
                self.assertEqual(["assistant", "user"], commit["source_roles"])
                self.assertEqual(2, commit["trigger_evidence"]["pending_event_count"])
                self.assertEqual(2, commit["trigger_evidence"]["pending_message_count"])
                self.assertTrue(second["session_buffer"]["threshold_ready"])
                self.assertEqual(2, second["session_buffer"]["pending_event_count"])
                self.assertEqual(2, second["session_buffer"]["pending_message_count"])
                self.assertFalse(adapter.pending_session_events({
                    "account_id": "acct_fast_threshold",
                    "tenant_id": "tenant_fast_threshold",
                    "user_id": "user_fast_threshold",
                    "session_id": "session_fast_threshold",
                }))

                layers = commit["memory_layers_written"]
                self.assertGreaterEqual(layers["segments"], 1)
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)

                records = adapter.read_all()
                record_types = {record.get("record_type") for record in records}
                for record_type in {
                    "context_event",
                    "context_embedding",
                    "context_segment",
                    "context_entity",
                    "context_index",
                    "context_summary_dirty",
                    "context_extraction_audit",
                    "context_batch_commit",
                }:
                    self.assertIn(record_type, record_types)
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual("threshold", commits[0]["trigger_policy"])
                self.assertEqual(["assistant", "user"], commits[0]["source_roles"])
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["session_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["profile_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["secondary_indexes"], 1)
                self.assertTrue(isinstance(commits[0].get("trigger_evidence"), dict))
                self.assertTrue(commits[0]["trigger_evidence"]["threshold_ready"])
                self.assertEqual(2, commits[0]["trigger_evidence"]["pending_event_count"])
                committed_event_hashes = {int(event_id) for event_id in commit["source_event_ids"]}
                committed_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("status") == "extraction_committed"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertEqual(2, len(committed_events))
                committed_event_embeddings = [
                    record
                    for record in records
                    if record.get("record_type") == "context_embedding"
                    and record.get("embedding_type") == "event_text"
                    and record.get("ref_hash") in committed_event_hashes
                ]
                self.assertEqual(2, len(committed_event_embeddings))
                extraction_audits = [
                    record
                    for record in records
                    if record.get("record_type") == "context_extraction_audit"
                    and record.get("batch_id_hash") == commit["batch_id_hash"]
                ]
                self.assertEqual(1, len(extraction_audits))
                audit_outputs = extraction_audits[0]["outputs"]
                self.assertEqual("always_when_profile_scope_available", audit_outputs["profile_promotion_policy"])
                self.assertTrue(audit_outputs["profile_promotion_scope_available"])
                self.assertEqual(audit_outputs["entities"], audit_outputs["profile_entities"])
                self.assertGreaterEqual(audit_outputs["indexes"], audit_outputs["entity_indexes"])
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "session"
                        for record in records
                    )
                )
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )
                progress = [
                    record
                    for record in records
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "extraction_committed"
                    and record.get("commit_id_hash") == commit["commit_id_hash"]
                ]
                self.assertEqual(
                    sorted(int(event_id) for event_id in commit["source_event_ids"]),
                    sorted(int(record["event_id_hash"]) for record in progress),
                )
                self.assertTrue(all(record["completed_stages"] == ["extraction"] for record in progress))
                self.assertTrue(all("summary" in record["remaining_stages"] for record in progress))
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_threshold_commit_counts_messages_inside_buffered_event(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-message-threshold.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_fast_message_threshold",
                    "tenant_id": "tenant_fast_message_threshold",
                    "user_id": "user_fast_message_threshold",
                    "session_id": "session_fast_message_threshold",
                    "team": "codex",
                    "project": "temporalstore",
                }
                node_path = [
                    "tenant:tenant_fast_message_threshold",
                    "user:user_fast_message_threshold",
                    "session:session_fast_message_threshold",
                    "conversation:codex_hook",
                ]
                seed_ms = int(time.time() * 1000) - 10
                seed_envelope = {
                    "kind": "message",
                    "scope": scope,
                    "metadata": {
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                        "source_roles": ["user", "assistant"],
                    },
                    "messages": [
                        {"role": "user", "content": "User prompt: remember multi-message hook envelopes."},
                        {"role": "assistant", "content": "Assistant decision: commit by message threshold."},
                    ],
                    "ingestion_time_ms": seed_ms,
                    "storage_options": {},
                }
                adapter.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": 10101,
                        "node_hash": 20202,
                        "node_path": node_path,
                        "text": (
                            "user: User prompt: remember multi-message hook envelopes.\n"
                            "assistant: Assistant decision: commit by message threshold."
                        ),
                        "summary_text": "User prompt and assistant decision about multi-message hook envelopes.",
                        "classification": "PENDING_ASYNC_EXTRACTION",
                        "event_type": "pending_async",
                        "status": "pending",
                        "source_kind": "message",
                        "source_role": "user",
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                        "scope": scope,
                        "envelope": seed_envelope,
                        "async_processing": True,
                        "updated_at_ms": seed_ms,
                    }
                )
                adapter.append_session_buffer_event(
                    envelope=seed_envelope,
                    event_id_hash=10101,
                    node_hash=20202,
                    node_path=node_path,
                    hook={"hook_type": "session_commit", "trigger": "Stop"},
                )

                scope_args = {
                    "event": "UserPromptSubmit",
                    **scope,
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 3,
                    "idle_commit_timeout_ms": 300000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                result = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=Namespace(**scope_args),
                    text="Tool evidence: Exit code: 0 proves the direct fast hook sees the third message.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-message-threshold",
                        "turn_id": "turn-fast-message-threshold-3",
                        "hook_type": "tool_result",
                    },
                )

                self.assertEqual("accepted", result["status"])
                self.assertTrue(result["session_buffer"]["threshold_ready"])
                self.assertEqual(2, result["session_buffer"]["pending_event_count"])
                self.assertEqual(3, result["session_buffer"]["pending_message_count"])
                self.assertEqual(1, result["session_buffer"]["pending_before_ingest_count"])
                self.assertEqual(2, result["session_buffer"]["pending_before_ingest_message_count"])
                commit = result["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(2, commit["trigger_evidence"]["pending_event_count"])
                self.assertEqual(3, commit["trigger_evidence"]["pending_message_count"])
                self.assertEqual(["assistant", "tool", "user"], commit["source_roles"])
                self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, commit["source_role_counts"])
                commits = [record for record in adapter.read_all() if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual(2, commits[0]["pending_event_count_before_commit"])
                self.assertEqual(3, commits[0]["pending_message_count_before_commit"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_threshold_commit_promotes_user_assistant_tool_to_profile_retrieval(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-mixed-profile.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_mixed",
                    "tenant_id": "tenant_fast_mixed",
                    "user_id": "user_fast_mixed",
                    "session_id": "session_fast_mixed",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 3,
                    "idle_commit_timeout_ms": 300000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                server = Server()
                messages = [
                    (
                        "user",
                        "User prompt: prove mixed Codex hook messages promote into profile memory.",
                        "turn-fast-mixed-1",
                    ),
                    (
                        "assistant",
                        "Assistant decision: keep mixed-role extraction and retrieval within the budget.",
                        "turn-fast-mixed-2",
                    ),
                    (
                        "tool",
                        "Exit code: 0\nRan 75 tests in 1.22s\nOK\nf05e40ed refs/heads/main",
                        "turn-fast-mixed-3",
                    ),
                ]
                results = []
                for role, message, turn_id in messages:
                    results.append(
                        matrixark_codex_hook.fast_async_hook_ingest(
                            server,
                            args=Namespace(**scope_args),
                            text=message,
                            role=role,
                            agent_context={"workspace_root": "/repo"},
                            hook={
                                "session_id_source": "payload_field",
                                "thread_id": "thread-fast-mixed",
                                "turn_id": turn_id,
                                "hook_type": "tool_result" if role == "tool" else "after_llm" if role == "assistant" else "before_llm",
                            },
                        )
                    )

                self.assertFalse(results[0]["session_buffer"]["threshold_ready"])
                self.assertFalse(results[1]["session_buffer"]["threshold_ready"])
                self.assertTrue(results[2]["session_buffer"]["threshold_ready"])
                commit = results[2]["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(3, commit["committed_event_count"])
                self.assertEqual(["assistant", "tool", "user"], commit["source_roles"])
                self.assertEqual(
                    [
                        "selected_assistant_decision_outcome_only",
                        "selected_tool_evidence_only",
                        "selected_user_prompt",
                    ],
                    commit["source_memory_selection_policies"],
                )
                self.assertEqual(
                    {
                        "selected_assistant_decision_outcome_only": 1,
                        "selected_tool_evidence_only": 1,
                        "selected_user_prompt": 1,
                    },
                    commit["source_memory_selection_policy_counts"],
                )
                self.assertGreaterEqual(commit["memory_layers_written"]["profile_entities"], 1)
                self.assertGreaterEqual(commit["memory_layers_written"]["secondary_indexes"], 1)

                records = adapter.read_all()
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("entity_type") == "assistant_decision"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        and "mixed-role extraction" in str(record.get("state") or "")
                        for record in records
                    )
                )
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("entity_type") == "tool_evidence"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        and "Ran 75 tests" in str(record.get("state") or "")
                        for record in records
                    )
                )
                self.assertIn(
                    "entity_type:tool_evidence",
                    {str(record.get("index_name") or "") for record in records if record.get("record_type") == "context_index"},
                )
                self.assertIn(
                    "entity_type:assistant_decision",
                    {str(record.get("index_name") or "") for record in records if record.get("record_type") == "context_index"},
                )

                pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_mixed",
                            "tenant_id": "tenant_fast_mixed",
                            "user_id": "user_fast_mixed",
                            "session_id": "session_fast_mixed_followup",
                        },
                        "session_scope": "prefer",
                        "query": "What tool evidence proves the mixed Codex hook extraction was validated and pushed?",
                        "max_context_tokens": 140,
                        "source_role_budget_tokens": {"assistant": 1},
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                self.assertLessEqual(pack["used_context_tokens"], 140)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "tool_evidence"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "Ran 75 tests" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                selected_tool_ref = next(
                    ref
                    for ref in pack["selected_refs"]
                    if ref.get("ref_type") == "entity" and ref.get("entity_type") == "tool_evidence"
                )
                self.assertNotIn("budget_source_roles", selected_tool_ref)
                self.assertNotIn("budget_source_role_counts", selected_tool_ref)
                self.assertNotIn("source_memory_selection_policies", selected_tool_ref)
                self.assertNotIn("source_memory_selection_policy_counts", selected_tool_ref)
                tool_role_policy = pack["recall_policy"]["source_role_budget"]
                self.assertTrue(tool_role_policy["enabled"])
                self.assertEqual({"assistant": 1}, tool_role_policy["budget_tokens"])
                self.assertEqual(0, tool_role_policy["selected_tokens_by_role"]["assistant"])
                tool_layer_budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertEqual({"tool": 1}, tool_layer_budget["source_message_counts_by_role"])
                self.assertIn("selected_tool_evidence_only", tool_layer_budget["by_memory_selection_policy"])
                self.assertGreaterEqual(tool_layer_budget["by_memory_selection_policy"]["selected_tool_evidence_only"]["refs"], 1)

                assistant_pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_mixed",
                            "tenant_id": "tenant_fast_mixed",
                            "user_id": "user_fast_mixed",
                            "session_id": "session_fast_mixed_assistant_followup",
                        },
                        "session_scope": "prefer",
                        "query": "What assistant decision kept mixed-role extraction and retrieval within budget?",
                        "max_context_tokens": 140,
                        "source_role_budget_tokens": {"tool": 1},
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                self.assertLessEqual(assistant_pack["used_context_tokens"], 140)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "assistant_decision"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "mixed-role extraction" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in assistant_pack["selected_refs"]
                    ),
                    assistant_pack["selected_refs"],
                )
                selected_assistant_ref = next(
                    ref
                    for ref in assistant_pack["selected_refs"]
                    if ref.get("ref_type") == "entity" and ref.get("entity_type") == "assistant_decision"
                )
                self.assertNotIn("budget_source_roles", selected_assistant_ref)
                self.assertNotIn("budget_source_role_counts", selected_assistant_ref)
                self.assertNotIn("source_memory_selection_policies", selected_assistant_ref)
                self.assertNotIn("source_memory_selection_policy_counts", selected_assistant_ref)
                assistant_role_policy = assistant_pack["recall_policy"]["source_role_budget"]
                self.assertTrue(assistant_role_policy["enabled"])
                self.assertEqual({"tool": 1}, assistant_role_policy["budget_tokens"])
                self.assertEqual(0, assistant_role_policy["selected_tokens_by_role"]["tool"])
                assistant_layer_budget = assistant_pack["recall_policy"]["memory_layer_budget"]
                self.assertEqual({"assistant": 1}, assistant_layer_budget["source_message_counts_by_role"])
                self.assertIn("selected_assistant_decision_outcome_only", assistant_layer_budget["by_memory_selection_policy"])
                self.assertGreaterEqual(
                    assistant_layer_budget["by_memory_selection_policy"]["selected_assistant_decision_outcome_only"]["refs"],
                    1,
                )
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_threshold_commit_recovers_pending_buffer_after_restart(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                event_log = Path(tmp_dir) / "matrixark-fast-hook-threshold-restart.jsonl"

                class Server:
                    def __init__(self, adapter: MatrixArkLocalAdapter) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_threshold_restart",
                    "tenant_id": "tenant_fast_threshold_restart",
                    "user_id": "user_fast_threshold_restart",
                    "session_id": "session_fast_threshold_restart",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 2,
                    "idle_commit_timeout_ms": 300000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                first_adapter = FastHookLocalAdapter(event_log)
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(first_adapter),
                    args=Namespace(**scope_args),
                    text="User prompt: restart recovery should keep the pending threshold buffer durable.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold-restart",
                        "turn_id": "turn-fast-threshold-restart-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertFalse(first["auto_batch_extract_result"])

                recovered_adapter = FastHookLocalAdapter(event_log)
                scope = {
                    "account_id": scope_args["account_id"],
                    "tenant_id": scope_args["tenant_id"],
                    "user_id": scope_args["user_id"],
                    "session_id": scope_args["session_id"],
                }
                recovered_pending = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(recovered_pending))
                self.assertIn("pending threshold buffer", recovered_pending[0]["text"])

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(recovered_adapter),
                    args=Namespace(**scope_args),
                    text="Assistant decision: restart recovery threshold commit extracted both buffered messages.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold-restart",
                        "turn_id": "turn-fast-threshold-restart-2",
                    },
                )
                commit = second["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(2, commit["committed_event_count"])
                self.assertEqual(["assistant", "user"], commit["source_roles"])
                self.assertEqual(2, commit["trigger_evidence"]["pending_event_count"])
                self.assertTrue(second["session_buffer"]["threshold_ready"])
                self.assertFalse(recovered_adapter.pending_session_events(scope))

                records = recovered_adapter.read_all()
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual(
                    [int(event_id) for event_id in commit["source_event_ids"]],
                    [int(event_id) for event_id in commits[0]["source_event_ids"]],
                )
                layers = commit["memory_layers_written"]
                self.assertGreaterEqual(layers["segments"], 1)
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                self.assertTrue(any(record.get("record_type") == "context_index" for record in records))
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )

                reopened_again = FastHookLocalAdapter(event_log)
                pack = reopened_again.retrieve(
                    {
                        "scope": {**scope, "session_id": "session_fast_threshold_restart_followup"},
                        "session_scope": "prefer",
                        "query": "What assistant decision proved restart recovery threshold commit?",
                        "max_context_tokens": 180,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 4},
                    }
                )
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "assistant_decision"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "restart recovery threshold commit" in str(ref.get("text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
                self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
                self.assertIn("assistant", budget["source_message_counts_by_role"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_idle_preflush_persists_real_adapter_memory_layers(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-idle.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_idle",
                    "tenant_id": "tenant_fast_idle",
                    "user_id": "user_fast_idle",
                    "session_id": "session_fast_idle",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 20,
                    "idle_commit_timeout_ms": 1,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                server = Server()
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="Tool evidence before idle: Exit code: 0 and hook pipeline tests passed.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle",
                        "turn_id": "turn-fast-idle-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertFalse(first["idle_commit_result"])
                time.sleep(0.01)

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="New user prompt should not mix into the previous idle batch.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle",
                        "turn_id": "turn-fast-idle-2",
                    },
                )
                idle_commit = second["idle_commit_result"]
                self.assertEqual("committed", idle_commit["status"])
                self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
                self.assertEqual("provisional", idle_commit["extraction_phase"])
                self.assertFalse(idle_commit["final_session_boundary"])
                self.assertEqual(1, idle_commit["committed_event_count"])
                self.assertEqual(["tool"], idle_commit["source_roles"])
                self.assertTrue(second["session_buffer"]["pre_ingest_idle_ready"])
                self.assertGreaterEqual(second["session_buffer"]["pre_ingest_idle_elapsed_ms"], 1)
                self.assertEqual(idle_commit, second["auto_batch_extract_result"])

                layers = idle_commit["memory_layers_written"]
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)

                scope = {
                    "account_id": "acct_fast_idle",
                    "tenant_id": "tenant_fast_idle",
                    "user_id": "user_fast_idle",
                    "session_id": "session_fast_idle",
                }
                pending_after_preflush = adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending_after_preflush))
                self.assertIn("New user prompt", pending_after_preflush[0]["text"])

                records = adapter.read_all()
                record_types = {record.get("record_type") for record in records}
                for record_type in {
                    "context_event",
                    "context_embedding",
                    "context_segment",
                    "context_entity",
                    "context_index",
                    "context_summary_dirty",
                    "context_extraction_audit",
                    "context_batch_commit",
                }:
                    self.assertIn(record_type, record_types)
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual("idle_timeout", commits[0]["trigger_policy"])
                self.assertEqual(["tool"], commits[0]["source_roles"])
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["session_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["profile_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["secondary_indexes"], 1)
                self.assertTrue(commits[0]["trigger_evidence"]["idle_ready"])
                self.assertEqual(1, commits[0]["trigger_evidence"]["pending_event_count"])
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                committed_event_hashes = {int(event_id) for event_id in idle_commit["source_event_ids"]}
                committed_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("status") == "extraction_committed"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertEqual(1, len(committed_events))
                committed_event_embeddings = [
                    record
                    for record in records
                    if record.get("record_type") == "context_embedding"
                    and record.get("embedding_type") == "event_text"
                    and record.get("ref_hash") in committed_event_hashes
                ]
                self.assertEqual(1, len(committed_event_embeddings))
                extraction_audits = [
                    record
                    for record in records
                    if record.get("record_type") == "context_extraction_audit"
                    and record.get("batch_id_hash") == idle_commit["batch_id_hash"]
                ]
                self.assertEqual(1, len(extraction_audits))
                audit_outputs = extraction_audits[0]["outputs"]
                self.assertEqual("always_when_profile_scope_available", audit_outputs["profile_promotion_policy"])
                self.assertTrue(audit_outputs["profile_promotion_scope_available"])
                self.assertEqual(audit_outputs["entities"], audit_outputs["profile_entities"])
                self.assertGreaterEqual(audit_outputs["indexes"], audit_outputs["entity_indexes"])
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "session"
                        for record in records
                    )
                )
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )
                progress = [
                    record
                    for record in records
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "extraction_committed"
                    and record.get("commit_id_hash") == idle_commit["commit_id_hash"]
                ]
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(record["event_id_hash"]) for record in progress],
                )
                self.assertTrue(all(record["completed_stages"] == ["extraction"] for record in progress))
                self.assertTrue(all("summary" in record["remaining_stages"] for record in progress))

                pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_idle",
                            "tenant_id": "tenant_fast_idle",
                            "user_id": "user_fast_idle",
                            "session_id": "session_fast_idle_followup",
                        },
                        "session_scope": "prefer",
                        "query": "What tool evidence was captured by the idle timeout memory commit?",
                        "max_context_tokens": 120,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                self.assertLessEqual(pack["used_context_tokens"], 120)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "tool_evidence"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "hook pipeline tests passed" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                memory_budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertIn("user_profile", memory_budget["by_memory_scope"])
                self.assertIn("cross_session", memory_budget["by_session_continuity"])
                self.assertEqual({"tool": 1}, memory_budget["source_message_counts_by_role"])
                session_only_pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_idle",
                            "tenant_id": "tenant_fast_idle",
                            "user_id": "user_fast_idle",
                            "session_id": "session_fast_idle_followup",
                        },
                        "session_scope": "only",
                        "query": "What tool evidence was captured by the idle timeout memory commit?",
                        "max_context_tokens": 120,
                        "audit_mode": "off",
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                session_only_refs = session_only_pack.get("selected_refs", [])
                self.assertFalse(
                    any(ref.get("session_continuity") == "cross_session" for ref in session_only_refs),
                    session_only_refs,
                )
                self.assertFalse(
                    any(ref.get("memory_scope") == "user_profile" for ref in session_only_refs),
                    session_only_refs,
                )
                session_only_budget = session_only_pack.get("recall_policy", {}).get("memory_layer_budget", {})
                self.assertEqual(0, session_only_budget.get("by_memory_scope", {}).get("user_profile", {}).get("refs", 0))
                self.assertEqual(0, session_only_budget.get("by_session_continuity", {}).get("cross_session", {}).get("refs", 0))
                session_only_cross_policy = session_only_pack.get("recall_policy", {}).get("cross_session", {})
                if session_only_cross_policy:
                    self.assertFalse(session_only_cross_policy["enabled"])
                    self.assertEqual(0, session_only_cross_policy["budget_tokens"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_idle_preflush_recovers_pending_buffer_after_restart(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                event_log = Path(tmp_dir) / "matrixark-fast-hook-idle-restart.jsonl"

                class Server:
                    def __init__(self, adapter: MatrixArkLocalAdapter) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_idle_restart",
                    "tenant_id": "tenant_fast_idle_restart",
                    "user_id": "user_fast_idle_restart",
                    "session_id": "session_fast_idle_restart",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 20,
                    "idle_commit_timeout_ms": 1,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                first_adapter = FastHookLocalAdapter(event_log)
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(first_adapter),
                    args=Namespace(**scope_args),
                    text="Tool evidence before restart idle: Exit code: 0 and restart idle recovery passed.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle-restart",
                        "turn_id": "turn-fast-idle-restart-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertFalse(first["idle_commit_result"])
                self.assertFalse(first["auto_batch_extract_result"])
                time.sleep(0.01)

                recovered_adapter = FastHookLocalAdapter(event_log)
                scope = {
                    "account_id": scope_args["account_id"],
                    "tenant_id": scope_args["tenant_id"],
                    "user_id": scope_args["user_id"],
                    "session_id": scope_args["session_id"],
                }
                recovered_pending = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(recovered_pending))
                self.assertIn("restart idle recovery", recovered_pending[0]["text"])

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(recovered_adapter),
                    args=Namespace(**scope_args),
                    text="New prompt after restart should preflush the older idle batch before entering the buffer.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle-restart",
                        "turn_id": "turn-fast-idle-restart-2",
                    },
                )
                idle_commit = second["idle_commit_result"]
                self.assertEqual("committed", idle_commit["status"])
                self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
                self.assertEqual("provisional", idle_commit["extraction_phase"])
                self.assertFalse(idle_commit["final_session_boundary"])
                self.assertEqual(1, idle_commit["committed_event_count"])
                self.assertEqual(["tool"], idle_commit["source_roles"])
                self.assertTrue(second["session_buffer"]["pre_ingest_idle_ready"])
                self.assertGreaterEqual(second["session_buffer"]["pre_ingest_idle_elapsed_ms"], 1)
                self.assertEqual(idle_commit, second["auto_batch_extract_result"])

                pending_after_preflush = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending_after_preflush))
                self.assertIn("New prompt after restart", pending_after_preflush[0]["text"])

                records = recovered_adapter.read_all()
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual("idle_timeout", commits[0]["trigger_policy"])
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(event_id) for event_id in commits[0]["source_event_ids"]],
                )
                layers = idle_commit["memory_layers_written"]
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                self.assertTrue(any(record.get("record_type") == "context_index" for record in records))
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )
                progress = [
                    record
                    for record in records
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "extraction_committed"
                    and record.get("commit_id_hash") == idle_commit["commit_id_hash"]
                ]
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(record["event_id_hash"]) for record in progress],
                )
                self.assertTrue(all(record["completed_stages"] == ["extraction"] for record in progress))
                self.assertTrue(all("summary" in record["remaining_stages"] for record in progress))

                reopened_again = FastHookLocalAdapter(event_log)
                pack = reopened_again.retrieve(
                    {
                        "scope": {**scope, "session_id": "session_fast_idle_restart_followup"},
                        "session_scope": "prefer",
                        "query": "What tool evidence proved restart idle recovery?",
                        "max_context_tokens": 180,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 4},
                    }
                )
                self.assertLessEqual(pack["used_context_tokens"], 180)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "tool_evidence"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "restart idle recovery passed" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
                self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
                self.assertEqual({"tool": 1}, budget["source_message_counts_by_role"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_local_native_context_pack_receives_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-budget.jsonl")
            scope = {
                "account_id": "acct_native_budget",
                "tenant_id": "tenant_native_budget",
                "user_id": "user_native_budget",
                "session_id": "session_native_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 64, "tool": 32},
                    "ranking": {"max_selected_refs": 4},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual({"assistant": 64, "tool": 32}, request["source_role_budget_tokens"])
            self.assertEqual({"max_selected_refs": 4}, request["ranking"])

    def test_local_native_context_pack_receives_memory_selection_policy_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-selection-policy-budget.jsonl")
            scope = {
                "account_id": "acct_native_selection_policy_budget",
                "tenant_id": "tenant_native_selection_policy_budget",
                "user_id": "user_native_selection_policy_budget",
                "session_id": "session_native_selection_policy_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "selected tool evidence budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {
                        "selected_assistant_decision_outcome_only": 24,
                        "selected_tool_evidence_only": 48,
                    },
                    "ranking": {"max_selected_refs": 4},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual(
                {
                    "selected_assistant_decision_outcome_only": 24,
                    "selected_tool_evidence_only": 48,
                },
                request["memory_selection_policy_budget_tokens"],
            )
            self.assertEqual("explicit", request["memory_selection_policy_budget_mode"])

    def test_local_native_context_pack_receives_auto_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-auto-budget.jsonl")
            scope = {
                "account_id": "acct_native_auto_budget",
                "tenant_id": "tenant_native_auto_budget",
                "user_id": "user_native_auto_budget",
                "session_id": "session_native_auto_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Codex role budget",
                    "max_context_tokens": 100,
                    "ranking": {
                        "source_role_budget_mode": "auto",
                        "memory_layer_budget_mode": "auto",
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual({"assistant": 42, "tool": 33, "user": 57}, request["source_role_budget_tokens"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual(
                {
                    "summary": 19,
                    "profile_summary": 28,
                    "same_session_summary": 19,
                    "cross_session_summary": 19,
                    "compression": 23,
                    "same_session_event": 42,
                    "cross_session_event": 23,
                    "same_session_segment": 33,
                    "cross_session_segment": 23,
                    "profile_entity": 38,
                },
                request["memory_layer_budget_tokens"],
            )
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual(
                {
                    "selected_user_prompt": 42,
                    "selected_assistant_decision_outcome_only": 28,
                    "selected_tool_evidence_only": 28,
                },
                request["memory_selection_policy_budget_tokens"],
            )
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])

    def test_auto_memory_layer_budget_expands_profile_for_current_state_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-current-budget.jsonl")
            scope = {
                "account_id": "acct_native_current_budget",
                "tenant_id": "tenant_native_current_budget",
                "user_id": "user_native_current_budget",
                "session_id": "session_native_current_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What is the latest Codex profile preference?",
                    "max_context_tokens": 100,
                    "ranking": {"memory_layer_budget_mode": "auto"},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("current_state", request["question_type"])
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual("current_state", request["memory_layer_budget_question_type"])
            self.assertIn("prioritize_profile_entity", request["memory_layer_budget_question_reason"])
            self.assertEqual(
                {
                    "summary": 14,
                    "profile_summary": 19,
                    "same_session_summary": 14,
                    "cross_session_summary": 14,
                    "compression": 19,
                    "same_session_event": 33,
                    "cross_session_event": 28,
                    "same_session_segment": 28,
                    "cross_session_segment": 28,
                    "profile_entity": 52,
                },
                request["memory_layer_budget_tokens"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["cross_session_event"],
                23,
            )

    def test_invalid_pre_retrieval_summary_refresh_limit_env_does_not_break_import(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = "tools"
        env["MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT"] = "not-an-int"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import matrixark_mcp_local_adapter as m; print(m.PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT)",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )
        self.assertEqual("2", result.stdout.strip())

    def test_pre_retrieval_summary_refresh_derives_balanced_layer_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-pre-refresh-budget.jsonl")
            scope = {
                "account_id": "acct_native_pre_refresh_budget",
                "tenant_id": "tenant_native_pre_refresh_budget",
                "user_id": "user_native_pre_refresh_budget",
                "session_id": "session_native_pre_refresh_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "pre refresh summary budget",
                    "max_context_tokens": 100,
                    "pre_retrieval_summary_refresh": True,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual(
                "pre_retrieval_summary_refresh_balanced",
                request["memory_layer_budget_mode"],
            )
            self.assertEqual(14, request["memory_layer_budget_tokens"]["summary"])
            self.assertEqual(28, request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(42, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["summary"],
            )

    def test_context_pack_cache_key_includes_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-source-role-cache.jsonl")
            scope = {
                "account_id": "acct_source_role_cache",
                "tenant_id": "tenant_source_role_cache",
                "user_id": "user_source_role_cache",
                "session_id": "session_source_role_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_context_pack_cache_key_includes_memory_layer_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-memory-layer-cache.jsonl")
            scope = {
                "account_id": "acct_memory_layer_cache",
                "tenant_id": "tenant_memory_layer_cache",
                "user_id": "user_memory_layer_cache",
                "session_id": "session_memory_layer_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "summary budget",
                    "max_context_tokens": 256,
                    "memory_layer_budget_tokens": {"summary": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "summary budget",
                    "max_context_tokens": 256,
                    "memory_layer_budget_tokens": {"summary": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_context_pack_cache_key_includes_auto_memory_selection_policy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-selection-policy-auto-cache.jsonl")
            scope = {
                "account_id": "acct_selection_policy_auto_cache",
                "tenant_id": "tenant_selection_policy_auto_cache",
                "user_id": "user_selection_policy_auto_cache",
                "session_id": "session_selection_policy_auto_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))
            self.assertEqual(
                "auto",
                first["recall_policy"]["memory_selection_policy_budget_policy"]["mode"],
            )
            self.assertEqual(
                "disabled",
                second["recall_policy"]["memory_selection_policy_budget_policy"]["mode"],
            )

    def test_context_pack_cache_key_includes_memory_selection_policy_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-selection-policy-cache.jsonl")
            scope = {
                "account_id": "acct_selection_policy_cache",
                "tenant_id": "tenant_selection_policy_cache",
                "user_id": "user_selection_policy_cache",
                "session_id": "session_selection_policy_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {"selected_assistant_decision_outcome_only": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {"selected_assistant_decision_outcome_only": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_retrieval_metrics_expose_shared_local_remote_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-retrieval-budget.jsonl")
            scope = {
                "account_id": "acct_budget",
                "tenant_id": "tenant_budget",
                "user_id": "user_budget",
                "session_id": "session_budget",
            }
            adapter.ingest(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Budget fact: Alice approved the GPU checklist after Bob confirmed the owner.",
                        }
                    ],
                }
            )
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Who approved the GPU checklist?",
                    "max_context_tokens": 100,
                    "local_context": [{"ref": "open-file:notes.md", "text": "Visible local context already mentions Bob."}],
                    "local_context_tokens": 30,
                    "local_context_safety_margin_tokens": 10,
                    "include_retrieval_metrics": True,
                    "audit_mode": "off",
                }
            )
            metrics = pack["retrieval_metrics"]
            self.assertEqual(100, metrics["requested_max_context_tokens"])
            self.assertEqual(30, metrics["used_local_context_tokens"])
            self.assertEqual(10, metrics["local_context_safety_margin_tokens"])
            self.assertEqual(60, metrics["remote_context_budget_tokens"])
            self.assertLessEqual(metrics["used_remote_context_tokens"], metrics["remote_context_budget_tokens"])
            self.assertEqual(
                metrics["used_local_context_tokens"] + metrics["used_remote_context_tokens"],
                metrics["total_prompt_context_tokens"],
            )
            self.assertTrue(metrics["remote_is_additive_only_within_remaining_budget"])

    def test_local_deadline_fallback_hides_source_lineage_budget_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-deadline-fallback.jsonl")
            scope = {
                "account_id": "acct_deadline_local",
                "tenant_id": "tenant_deadline_local",
                "user_id": "user_deadline_local",
                "session_id": "session_deadline_local",
            }
            pack = adapter.deadline_fallback_pack(
                query="what tool result proved fallback safety?",
                scope=scope,
                question_type="specific_fact",
                max_context_tokens=160,
                local_budget={"items": [], "token_estimate": 0, "safety_margin_tokens": 0},
                deadline_ms=1,
                elapsed_ms=2.0,
                records=[
                    {
                        "record_type": "context_entity",
                        "scope": scope,
                        "entity_hash": 991,
                        "entity_type": "tool_evidence",
                        "entity_name": "fallback safety test",
                        "state": "Exit code: 0 proved fallback serving output stays lean.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "extraction_phase": "final",
                        "source_roles": ["tool"],
                        "source_role_counts": {"tool": 2},
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 2},
                        "source_codex_events": ["PostToolUse"],
                        "source_codex_event_counts": {"PostToolUse": 2},
                    }
                ],
                reason="deadline_unit_test",
            )
            selected = pack["selected_refs"]
            self.assertTrue(selected, pack)
            self.assertEqual("tool_evidence", selected[0]["entity_type"])
            for field in ["source_roles", "source_role_counts", "source_hook_types", "source_codex_events"]:
                self.assertNotIn(field, selected[0])
            budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertEqual(1, budget["by_memory_scope"]["user_profile"]["refs"])
            self.assertEqual(1, budget["by_entity_type"]["tool_evidence"]["refs"])
            self.assertNotIn("memory_layer_budget", pack)
            for field in [
                "by_source_role",
                "by_hook_type",
                "by_codex_event",
                "source_message_counts_by_role",
                "source_hook_counts_by_type",
                "source_codex_event_counts_by_event",
            ]:
                self.assertNotIn(field, budget)

    def test_retrieve_source_role_budget_uses_entity_semantics_for_mixed_profile_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-mixed-role-budget.jsonl")
            scope = {
                "account_id": "acct_mixed_role_budget",
                "tenant_id": "tenant_mixed_role_budget",
                "user_id": "user_mixed_role_budget",
                "session_id": "session_mixed_role_budget",
            }
            adapter.batch_extract(
                {
                    "scope": scope,
                    "force": True,
                    "messages": [
                        {"role": "assistant", "content": "Assistant decision alpha: prefer threshold extraction for Codex memory."},
                        {"role": "assistant", "content": "Assistant decision bravo: maintain source role budget during retrieval."},
                        {"role": "user", "content": "User requirement charlie: keep user evidence available in retrieval."},
                        {"role": "tool", "content": "Exit code: 0\nRan 76 tests in 1.03s\nOK"},
                    ],
                    "metadata": {
                        "source_roles": ["assistant", "assistant", "user", "tool"],
                        "source_hook_types": ["before_llm", "after_llm", "tool_result"],
                        "source_codex_events": ["UserPromptSubmit", "PreviousAssistantBackfill", "PostToolUse"],
                    },
                }
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_mixed_role_budget_followup"},
                    "session_scope": "prefer",
                    "query": "What test evidence was kept for retrieval budget?",
                    "max_context_tokens": 80,
                    "source_role_budget_tokens": {"assistant": 1},
                    "ranking": {"max_selected_refs": 5, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertLessEqual(pack["used_context_tokens"], 80)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "tool_evidence"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "Ran 76 tests" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )
            selected_tool_ref = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity" and ref.get("entity_type") == "tool_evidence"
            )
            self.assertEqual("tool_evidence", selected_tool_ref["entity_type"])
            self.assertNotIn("budget_source_roles", selected_tool_ref)
            self.assertNotIn("budget_source_role_counts", selected_tool_ref)
            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertTrue(role_policy["enabled"])
            self.assertEqual({"assistant": 1}, role_policy["budget_tokens"])
            self.assertEqual(0, role_policy["selected_tokens_by_role"]["assistant"])
            memory_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"tool": 1}, memory_budget["source_message_counts_by_role"])
            self.assertIn("tool", memory_budget["by_source_role"])
            self.assertNotIn("assistant", memory_budget["by_source_role"])

    def test_retrieve_auto_source_role_budget_policy_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-auto-role-budget.jsonl")
            scope = {
                "account_id": "acct_auto_role_budget",
                "tenant_id": "tenant_auto_role_budget",
                "user_id": "user_auto_role_budget",
                "session_id": "session_auto_role_budget",
            }
            adapter.batch_extract(
                {
                    "scope": scope,
                    "force": True,
                    "messages": [
                        {"role": "assistant", "content": "Assistant decision: use idle extraction for memory quality."},
                        {"role": "user", "content": "User requirement: keep user requests retrievable."},
                        {"role": "tool", "content": "Exit code: 0\nRan auto budget tests successfully."},
                    ],
                    "metadata": {
                        "source_roles": ["assistant", "user", "tool"],
                        "source_hook_types": ["after_llm", "before_llm", "tool_result"],
                        "source_codex_events": ["PreviousAssistantBackfill", "UserPromptSubmit", "PostToolUse"],
                    },
                }
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_auto_role_budget_followup"},
                    "session_scope": "prefer",
                    "query": "What extraction and test evidence should be remembered?",
                    "max_context_tokens": 120,
                    "ranking": {
                        "max_selected_refs": 5,
                        "min_similarity_score": 0.0,
                        "source_role_budget_mode": "auto",
                        "memory_layer_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertTrue(role_policy["enabled"])
            self.assertEqual("auto", role_policy["mode"])
            self.assertTrue(role_policy["derived"])
            self.assertEqual(114, role_policy["remote_budget_tokens"])
            self.assertEqual("independent_per_role_caps_under_global_remote_budget", role_policy["budget_semantics"])
            self.assertTrue(role_policy["independent_caps"])
            self.assertTrue(role_policy["global_remote_budget_enforced"])
            self.assertEqual({"assistant": 51, "tool": 39, "user": 68}, role_policy["budget_tokens"])
            self.assertGreater(sum(role_policy["budget_tokens"].values()), role_policy["remote_budget_tokens"])
            layer_policy = pack["recall_policy"]["memory_layer_budget_policy"]
            self.assertTrue(layer_policy["enabled"])
            self.assertEqual("auto", layer_policy["mode"])
            self.assertTrue(layer_policy["derived"])
            self.assertEqual(114, layer_policy["remote_budget_tokens"])
            self.assertEqual("evidence", layer_policy["question_type"])
            self.assertIn("broad_or_evidence_queries", layer_policy["question_budget_reason"])
            self.assertEqual("independent_per_layer_caps_under_global_remote_budget", layer_policy["budget_semantics"])
            self.assertTrue(layer_policy["independent_caps"])
            self.assertTrue(layer_policy["global_remote_budget_enforced"])
            self.assertEqual(22, layer_policy["budget_tokens"]["summary"])
            self.assertEqual(39, layer_policy["budget_tokens"]["profile_summary"])
            self.assertEqual(34, layer_policy["budget_tokens"]["cross_session_event"])
            self.assertEqual(51, layer_policy["budget_tokens"]["profile_entity"])
            self.assertGreater(sum(layer_policy["budget_tokens"].values()), layer_policy["remote_budget_tokens"])
            self.assertLessEqual(pack["used_remote_context_tokens"], pack["remote_context_budget_tokens"])

    def test_codex_hook_pre_retrieval_summary_refresh_is_explicit_opt_in(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            default_log = tmp / "matrixark-hook-refresh-default.jsonl"
            opt_in_log = tmp / "matrixark-hook-refresh-opt-in.jsonl"

            default_msg = self.run_hook(
                repo,
                default_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt: keep profile entities ahead of summaries by default.",
                    "thread_id": "codex-refresh-default-thread",
                },
            )
            self.assertNotIn("pre_retrieval_summary_refresh", default_msg["retrieve"])

            opt_in_msg = self.run_hook(
                repo,
                opt_in_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt: refresh dirty summaries before retrieval when explicitly enabled.",
                    "thread_id": "codex-refresh-opt-in-thread",
                },
                extra_env={
                    "MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH": "1",
                    "MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT": "3",
                },
            )
            self.assertNotIn("pre_retrieval_summary_refresh", opt_in_msg["retrieve"])

            debug_msg = self.run_hook(
                repo,
                opt_in_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt: show refresh diagnostics only when debug lineage is enabled.",
                    "thread_id": "codex-refresh-debug-thread",
                },
                extra_env={
                    "MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH": "1",
                    "MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT": "3",
                    "MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE": "1",
                },
            )
            opt_in_refresh = debug_msg["retrieve"]["pre_retrieval_summary_refresh"]
            self.assertTrue(opt_in_refresh["enabled"])
            self.assertEqual(3, opt_in_refresh["requested_limit"])
            self.assertEqual("no_dirty_nodes", opt_in_refresh["status"])
            self.assertEqual(0, opt_in_refresh["refreshed_count"])
            self.assertGreaterEqual(opt_in_refresh["skipped_dirty_count"], 1)
            self.assertGreaterEqual(opt_in_refresh["skipped_dirty_reasons"]["new_event"], 1)
            self.assertEqual(
                debug_msg["retrieve"]["pre_retrieval_summary_refresh"],
                debug_msg["retrieve"]["layers"]["pre_retrieval_summary_refresh"],
            )
            self.assertIn(
                "summary_refresh[enabled=true",
                debug_msg["hookSpecificOutput"]["additionalContext"],
            )
            self.assertIn("skipped_reasons[new_event=", debug_msg["hookSpecificOutput"]["additionalContext"])

    def run_hook(self, repo: Path, event_log: Path, *, event: str, payload: dict, query: str = "", extra_env: dict | None = None) -> dict:
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_codex_hook.py"),
            "--backend",
            "local",
            "--event-log",
            str(event_log),
            "--event",
            event,
            "--account-id",
            "acct_hook",
            "--tenant-id",
            "tenant_hook",
            "--user-id",
            "codex_user",
            "--session-id",
            "codex_session_1",
            "--team",
            "agent",
            "--project",
            "context",
        ]
        if query:
            cmd.extend(["--query", query])
        proc = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            cwd=repo,
            timeout=30,
            env={**os.environ, "MATRIXARK_ALLOW_LOCAL_BACKEND": "1", **(extra_env or {})},
        )
        if proc.returncode != 0:
            raise AssertionError(f"hook failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def test_user_prompt_cli_idle_preflushes_previous_tool_event(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-idle-preflush.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="PostToolUse",
                payload={
                    "output": "Exit code: 0\nRan 160 tests in 6.51s\nOK\nidle-preflush-tool-evidence",
                    "thread_id": "codex-cli-idle-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])
            self.assertFalse(first["ingest"]["session_buffer"]["threshold_ready"])
            self.assertFalse(first["ingest"]["session_buffer"]["idle_ready"])
            self.assertEqual("deferred", first["ingest"]["idle_commit_result"]["status"])
            self.assertFalse(first["ingest"]["auto_batch_extract_result"])

            time.sleep(0.01)
            second = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt after idle should preflush previous tool evidence before this prompt enters the buffer.",
                    "thread_id": "codex-cli-idle-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", second["status"])
            idle_commit = second["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual("provisional", idle_commit["extraction_phase"])
            self.assertFalse(idle_commit["final_session_boundary"])
            self.assertEqual(1, idle_commit["committed_event_count"])
            self.assertEqual(["tool"], idle_commit["source_roles"])
            self.assertIn("PostToolUse", idle_commit["source_codex_events"])
            self.assertTrue(second["ingest"]["session_buffer"]["idle_ready"])
            self.assertEqual(idle_commit, second["ingest"]["auto_batch_extract_result"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            pending = adapter.pending_session_events(scope)
            self.assertEqual(1, len(pending))
            self.assertIn("User prompt after idle", pending[0]["text"])

            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "tool_evidence"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "Ran 160 tests" in str(record.get("state") or "")
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("codex_event:posttooluse", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What tool evidence was preflushed after idle?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            selected_evidence = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "tool_evidence"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("Ran 160 tests", selected_evidence["text"])
            self.assertNotIn("source_codex_events", selected_evidence)

    def test_stop_cli_empty_identity_payload_flushes_pending_memory_without_fake_assistant_event(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-empty-stop-flush.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "300000",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User preference: prefer Stop boundary flush for pending Codex memory without fake assistant JSON; marker stop-boundary-flush-922.",
                    "thread_id": "codex-cli-empty-stop-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])
            self.assertFalse(first["ingest"]["session_buffer"]["threshold_ready"])
            self.assertEqual("deferred", first["ingest"]["idle_commit_result"]["status"])
            self.assertFalse(first["ingest"]["auto_batch_extract_result"])

            stop = self.run_hook(
                repo,
                event_log,
                event="Stop",
                payload={"thread_id": "codex-cli-empty-stop-thread"},
                extra_env=env,
            )
            self.assertEqual("ok", stop["status"])
            self.assertFalse(stop["ingest"])
            self.assertEqual("committed", stop["session_commit"]["status"])
            self.assertEqual("hook_boundary", stop["session_commit"]["commit_reason"])
            self.assertEqual("force", stop["session_commit"]["trigger_policy"])
            self.assertEqual(["user"], stop["session_commit"]["source_roles"])
            self.assertIn("UserPromptSubmit", stop["session_commit"]["source_codex_events"])
            self.assertTrue(stop["session_commit"]["final_session_boundary"])
            self.assertGreaterEqual(stop["session_commit"]["memory_layers_written"]["profile_entities"], 1)

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            self.assertEqual([], adapter.pending_session_events(scope))
            records = adapter.read_all()
            self.assertFalse(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "assistant_decision"
                    and "thread_id" in str(record.get("state") or record.get("text") or "")
                    for record in records
                ),
                records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "preference"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and record.get("extraction_phase") == "final"
                    and "Stop boundary flush" in str(record.get("state") or "")
                    for record in records
                ),
                records,
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex-cli-empty-stop-followup"},
                    "session_scope": "prefer",
                    "query": "What should Stop boundary do with pending Codex memory?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 3},
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            selected_preference = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "preference"
                and ref.get("memory_scope") == "user_profile"
            )
            self.assertIn("Stop boundary flush", selected_preference["text"])
            self.assertNotIn("source_codex_events", selected_preference)

    def test_user_prompt_cli_idle_tool_memory_pre_refreshes_profile_summaries(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-tool-summary-refresh.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="PostToolUse",
                payload={
                    "output": (
                        "Exit code: 0\n"
                        "Ran 164 tests in 8.40s\n"
                        "OK\n"
                        "tool-summary-refresh-evidence"
                    ),
                    "thread_id": "codex-cli-tool-summary-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])
            self.assertEqual("deferred", first["ingest"]["idle_commit_result"]["status"])

            time.sleep(0.01)
            second = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt should idle-flush prior tool evidence before profile summary retrieval.",
                    "thread_id": "codex-cli-tool-summary-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", second["status"])
            idle_commit = second["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual(["tool"], idle_commit["source_roles"])
            self.assertIn("PostToolUse", idle_commit["source_codex_events"])
            self.assertEqual(idle_commit, second["ingest"]["auto_batch_extract_result"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            before_refresh_records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "tool_evidence"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "Ran 164 tests" in str(record.get("state") or "")
                    and "tool" in record.get("source_roles", [])
                    and "PostToolUse" in record.get("source_codex_events", [])
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_summary"
                    and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "Summarize the tool evidence that proved profile summary refresh.",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            refresh = pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(pack["used_context_tokens"], 160)
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]), pack["selected_refs"])
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "tool_evidence"
                    and ref.get("memory_scope") == "user_profile"
                    and "Ran 164 tests" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )

            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, records)
            self.assertTrue(any(record.get("summary_type") == "node_l0" for record in profile_summaries))
            self.assertTrue(any("tool" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_role_counts", {}).get("tool", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any("PostToolUse" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any("user_profile" in record.get("source_memory_scopes", []) for record in profile_summaries))
            self.assertTrue(any("cross_session" in record.get("source_session_continuities", []) for record in profile_summaries))
            self.assertTrue(any("tool_evidence" in record.get("source_entity_types", []) for record in profile_summaries))

    def test_user_prompt_cli_idle_preflushes_previous_user_preference(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-user-idle-preflush.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User preference: prefer threshold or idle extraction for Codex long-term memory; marker user-idle-pref-314.",
                    "thread_id": "codex-cli-user-idle-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])
            self.assertFalse(first["ingest"]["session_buffer"]["threshold_ready"])
            self.assertFalse(first["ingest"]["session_buffer"]["idle_ready"])
            self.assertEqual("deferred", first["ingest"]["idle_commit_result"]["status"])
            self.assertFalse(first["ingest"]["auto_batch_extract_result"])

            time.sleep(0.01)
            second = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt after idle should preflush the older user preference before this new prompt enters the buffer.",
                    "thread_id": "codex-cli-user-idle-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", second["status"])
            idle_commit = second["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual("provisional", idle_commit["extraction_phase"])
            self.assertFalse(idle_commit["final_session_boundary"])
            self.assertEqual(1, idle_commit["committed_event_count"])
            self.assertEqual(["user"], idle_commit["source_roles"])
            self.assertIn("UserPromptSubmit", idle_commit["source_codex_events"])
            self.assertTrue(second["ingest"]["session_buffer"]["idle_ready"])
            self.assertEqual(idle_commit, second["ingest"]["auto_batch_extract_result"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            pending = adapter.pending_session_events(scope)
            self.assertEqual(1, len(pending))
            self.assertIn("User prompt after idle", pending[0]["text"])

            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "preference"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "threshold or idle extraction" in str(record.get("state") or "")
                    and "user" in record.get("source_roles", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:preference", index_names)
            self.assertIn("source_role:user", index_names)
            self.assertIn("codex_event:userpromptsubmit", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What does the user prefer for Codex long-term memory extraction?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 3},
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            selected_preference = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "preference"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("threshold or idle extraction", selected_preference["text"])
            self.assertNotIn("source_codex_events", selected_preference)

            debug_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What does the user prefer for Codex long-term memory extraction?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "include_retrieval_debug": True,
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            debug_selected_preference = next(
                ref
                for ref in debug_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "preference"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertEqual(["UserPromptSubmit"], debug_selected_preference["source_codex_events"])
            memory_budget = debug_pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"user": 1}, memory_budget["source_message_counts_by_role"])

    def test_user_prompt_cli_idle_user_memory_pre_refreshes_profile_summaries(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-user-summary-refresh.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": (
                        "User preference: prefer pre-retrieval summaries for user profile memory; "
                        "marker user-summary-refresh-628."
                    ),
                    "thread_id": "codex-cli-user-summary-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])
            self.assertEqual("deferred", first["ingest"]["idle_commit_result"]["status"])

            time.sleep(0.01)
            second = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt should idle-flush prior user preference before profile summary retrieval.",
                    "thread_id": "codex-cli-user-summary-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", second["status"])
            idle_commit = second["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual(["user"], idle_commit["source_roles"])
            self.assertIn("UserPromptSubmit", idle_commit["source_codex_events"])
            self.assertEqual(idle_commit, second["ingest"]["auto_batch_extract_result"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            before_refresh_records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "preference"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "pre-retrieval summaries" in str(record.get("state") or "")
                    and "user" in record.get("source_roles", [])
                    and "UserPromptSubmit" in record.get("source_codex_events", [])
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_summary"
                    and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "question_type": "broad_exploration",
                    "query": "Summarize the user's preference for profile memory summaries.",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            refresh = pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(pack["used_context_tokens"], 160)
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]), pack["selected_refs"])
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "preference"
                    and ref.get("memory_scope") == "user_profile"
                    and "pre-retrieval summaries" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )

            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, records)
            self.assertTrue(any(record.get("summary_type") == "node_l0" for record in profile_summaries))
            self.assertTrue(any("user" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_role_counts", {}).get("user", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any("UserPromptSubmit" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any("user_profile" in record.get("source_memory_scopes", []) for record in profile_summaries))
            self.assertTrue(any("cross_session" in record.get("source_session_continuities", []) for record in profile_summaries))
            self.assertTrue(any("preference" in record.get("source_entity_types", []) for record in profile_summaries))

    def test_user_prompt_cli_mixed_memory_retrieves_profile_summaries_under_budget(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-mixed-profile-pack.jsonl"
            rollout = tmp / "rollout.jsonl"
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "3",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            first = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": (
                        "User preference: prefer mixed ContextPack retrieval for user assistant and tool memories in mixed "
                        "ContextPack retrieval; marker mixed-pack-731."
                    ),
                    "thread_id": "codex-cli-mixed-pack-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", first["status"])

            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": (
                                "Assistant decision: mixed ContextPack retrieval should include "
                                "profile summaries and role-balanced evidence."
                            ),
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            second = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "Followup prompt should backfill prior assistant decision.",
                    "transcript_path": str(rollout),
                    "thread_id": "codex-cli-mixed-pack-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", second["status"])

            tool = self.run_hook(
                repo,
                event_log,
                event="PostToolUse",
                payload={
                    "output": "Exit code: 0\nRan 166 tests in 9.42s\nOK\nmixed-pack-tool-evidence",
                    "thread_id": "codex-cli-mixed-pack-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", tool["status"])

            time.sleep(0.01)
            flush = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "Next prompt should flush the remaining mixed evidence and retrieve all long-term layers.",
                    "thread_id": "codex-cli-mixed-pack-thread",
                },
                extra_env=env,
            )
            self.assertEqual("ok", flush["status"])
            self.assertIn(flush["ingest"]["idle_commit_result"]["status"], {"committed", "empty"})

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            records = adapter.read_all()
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(
                any(
                    record.get("entity_type") == "preference"
                    and "mixed ContextPack retrieval" in str(record.get("state") or "")
                    for record in profile_entities
                ),
                profile_entities,
            )
            self.assertTrue(any(record.get("entity_type") == "assistant_decision" for record in profile_entities), profile_entities)
            self.assertTrue(any(record.get("entity_type") == "tool_evidence" for record in profile_entities), profile_entities)

            retrieve_args = {
                "scope": {**scope, "session_id": "codex_session_2"},
                "session_scope": "prefer",
                "question_type": "broad_exploration",
                "query": "Summarize mixed user assistant tool evidence for ContextPack retrieval.",
                "max_context_tokens": 240,
                "audit_mode": "off",
                "ranking": {
                    "max_selected_refs": 8,
                    "min_similarity_score": 0.0,
                    "budget_fill_policy": "force_fill",
                    "pre_retrieval_summary_refresh": True,
                    "pre_retrieval_summary_refresh_limit": 12,
                    "source_role_budget_mode": "auto",
                    "memory_layer_budget_mode": "auto",
                },
            }
            pack = adapter.retrieve(retrieve_args)
            self.assert_no_default_context_pack_debug_lineage(pack)
            refresh = pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(pack["used_context_tokens"], 240)
            selected_refs = pack["selected_refs"]
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in selected_refs), selected_refs)
            self.assertTrue(
                any(ref.get("ref_type") == "entity" and ref.get("entity_type") == "assistant_decision" for ref in selected_refs),
                selected_refs,
            )
            self.assertTrue(
                any(ref.get("ref_type") == "entity" and ref.get("entity_type") == "tool_evidence" for ref in selected_refs),
                selected_refs,
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "summary"
                    and "mixed ContextPack retrieval" in str(ref.get("text") or "")
                    for ref in selected_refs
                ),
                selected_refs,
            )

            debug_pack = adapter.retrieve(
                {
                    **retrieve_args,
                    "include_retrieval_debug": True,
                    "include_debug_refs": True,
                }
            )
            debug_budget = debug_pack["retrieval_metrics"]["memory_layer_budget"]
            debug_recall_budget = debug_pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(debug_budget["by_memory_layer"]["profile_summary"]["refs"], 1)
            self.assertGreaterEqual(debug_budget["by_memory_layer"]["profile_entity"]["refs"], 1)
            self.assertGreaterEqual(debug_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(debug_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertNotIn("source_message_counts_by_role", debug_budget)
            self.assertGreaterEqual(debug_recall_budget["source_message_counts_by_role"]["user"], 1)
            self.assertGreaterEqual(debug_recall_budget["source_message_counts_by_role"]["assistant"], 1)
            self.assertGreaterEqual(debug_recall_budget["source_message_counts_by_role"]["tool"], 1)
            self.assertGreaterEqual(debug_recall_budget["by_source_role"]["user"]["refs"], 1)
            self.assertGreaterEqual(debug_recall_budget["by_source_role"]["assistant"]["refs"], 1)
            self.assertGreaterEqual(debug_recall_budget["by_source_role"]["tool"]["refs"], 1)
            role_policy = debug_pack["recall_policy"]["source_role_budget"]
            self.assertTrue(role_policy["enabled"])
            self.assertEqual("auto", role_policy["mode"])
            self.assertLessEqual(debug_pack["used_remote_context_tokens"], role_policy["remote_budget_tokens"])

    def test_user_prompt_cli_idle_preflushes_previous_assistant_rollout(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-assistant-idle-preflush.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": (
                                "Assistant decision: use idle assistant rollout extraction "
                                "for richer Codex long-term memory."
                            ),
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            env = {
                "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
            }

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": (
                        "User prompt after an assistant answer should preflush the prior assistant "
                        "rollout before this new prompt enters the buffer."
                    ),
                    "transcript_path": str(rollout),
                    "thread_id": "codex-cli-assistant-idle-thread",
                },
                extra_env=env,
            )

            self.assertEqual("ok", msg["status"])
            idle_commit = msg["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual("provisional", idle_commit["extraction_phase"])
            self.assertFalse(idle_commit["final_session_boundary"])
            self.assertEqual(1, idle_commit["committed_event_count"])
            self.assertEqual(["assistant"], idle_commit["source_roles"])
            self.assertIn("PreviousAssistantBackfill", idle_commit["source_codex_events"])
            self.assertIn("UserPromptSubmit:previous_assistant_backfill", idle_commit["source_codex_events"])
            self.assertEqual(idle_commit, msg["ingest"]["auto_batch_extract_result"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            pending = adapter.pending_session_events(scope)
            self.assertEqual(1, len(pending))
            self.assertIn("User prompt after an assistant answer", pending[0]["text"])

            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "assistant_decision"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "idle assistant rollout extraction" in str(record.get("state") or "")
                    and "assistant" in record.get("source_roles", [])
                    and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("codex_event:previousassistantbackfill", index_names)
            self.assertIn("codex_event:userpromptsubmit:previous_assistant_backfill", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What assistant decision should be remembered about idle rollout extraction?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 3},
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            selected_decision = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("idle assistant rollout extraction", selected_decision["text"])
            self.assertNotIn("source_codex_events", selected_decision)

            debug_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What assistant decision should be remembered about idle rollout extraction?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "include_retrieval_debug": True,
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            debug_selected_decision = next(
                ref
                for ref in debug_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("PreviousAssistantBackfill", debug_selected_decision["source_codex_events"])
            memory_budget = debug_pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"assistant": 1}, memory_budget["source_message_counts_by_role"])

    def test_user_prompt_cli_idle_assistant_memory_pre_refreshes_profile_summaries(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-cli-assistant-summary-refresh.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": (
                                "Assistant decision: pre-retrieval summaries should summarize "
                                "idle rollout profile memory."
                            ),
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt after assistant answer should trigger assistant idle profile memory.",
                    "transcript_path": str(rollout),
                    "thread_id": "codex-cli-assistant-summary-thread",
                },
                extra_env={
                    "MATRIXARK_SESSION_COMMIT_THRESHOLD": "20",
                    "MATRIXARK_IDLE_COMMIT_TIMEOUT_MS": "1",
                },
            )
            self.assertEqual("ok", msg["status"])
            idle_commit = msg["ingest"]["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual(["assistant"], idle_commit["source_roles"])
            self.assertIn("PreviousAssistantBackfill", idle_commit["source_codex_events"])

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            before_refresh_records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_summary"
                    and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
                    for record in before_refresh_records
                ),
                before_refresh_records,
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "Summarize idle rollout profile memory for Codex.",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            self.assert_no_default_context_pack_debug_lineage(pack)
            refresh = pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(pack["used_context_tokens"], 160)
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]), pack["selected_refs"])
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "assistant_decision"
                    and ref.get("memory_scope") == "user_profile"
                    and "pre-retrieval summaries" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )

            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, records)
            self.assertTrue(any(record.get("summary_type") == "node_l0" for record in profile_summaries))
            self.assertTrue(any("assistant" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(
                any(record.get("source_role_counts", {}).get("assistant", 0) >= 1 for record in profile_summaries)
            )
            self.assertTrue(
                any("PreviousAssistantBackfill" in record.get("source_codex_events", []) for record in profile_summaries)
            )
            self.assertTrue(any("user_profile" in record.get("source_memory_scopes", []) for record in profile_summaries))
            self.assertTrue(any("cross_session" in record.get("source_session_continuities", []) for record in profile_summaries))
            self.assertTrue(any("assistant_decision" in record.get("source_entity_types", []) for record in profile_summaries))

    def test_user_prompt_fast_async_still_backfills_previous_assistant_rollout(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-fast-backfill.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": "Assistant decision: keep prior answer memory even when fast async ingest is enabled.",
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt: verify previous assistant backfill under fast async.",
                    "transcript_path": str(rollout),
                    "thread_id": "codex-fast-backfill-thread",
                },
                extra_env={"MATRIXARK_SESSION_COMMIT_THRESHOLD": "1"},
            )

            self.assertEqual("ok", msg["status"])
            records = [
                json.loads(line)
                for line in event_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            assistant_events = [
                record
                for record in records
                if record.get("record_type") == "context_event"
                and "Assistant decision: keep prior answer memory" in str(record.get("text") or "")
            ]
            self.assertTrue(assistant_events, records)
            self.assertTrue(
                any(
                    record.get("record_type") == "session_buffer_event"
                    and (record.get("agent_hook") or {}).get("trigger") == "UserPromptSubmit:previous_assistant_backfill"
                    and ((record.get("envelope") or {}).get("messages") or [{}])[0].get("role") == "assistant"
                    and ((record.get("envelope") or {}).get("metadata") or {}).get("codex_event") == "PreviousAssistantBackfill"
                    for record in records
                ),
                records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_event"
                    and "User prompt: verify previous assistant backfill" in str(record.get("text") or "")
                    for record in records
                ),
                records,
            )

            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            records = adapter.read_all()
            auto_backfill_commits = [
                record
                for record in records
                if record.get("record_type") == "context_batch_commit"
                and record.get("commit_reason") == "threshold"
                and "assistant" in record.get("source_roles", [])
                and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
            ]
            self.assertTrue(auto_backfill_commits, records)
            commit = auto_backfill_commits[0]
            self.assertEqual("provisional", commit["extraction_phase"])
            self.assertFalse(commit["final_session_boundary"])
            self.assertGreaterEqual(commit["memory_layers_written"]["session_entities"], 1)
            self.assertGreaterEqual(commit["memory_layers_written"]["profile_entities"], 1)
            self.assertGreaterEqual(commit["memory_layers_written"]["secondary_indexes"], 1)
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "assistant_decision"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "prior answer memory" in str(record.get("state") or "")
                    and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("codex_event:previousassistantbackfill", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What assistant decision should be remembered from prior answer memory?",
                    "max_context_tokens": 120,
                    "source_role_budget_tokens": {"tool": 1},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            selected_decision = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("prior answer memory", selected_decision["text"])
            self.assertNotIn("source_codex_events", selected_decision)
            self.assertNotIn("budget_source_roles", selected_decision)
            self.assertNotIn("budget_source_role_counts", selected_decision)
            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertEqual({"tool": 1}, role_policy["budget_tokens"])
            self.assertEqual(0, role_policy["selected_tokens_by_role"]["tool"])
            memory_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"assistant": 1}, memory_budget["source_message_counts_by_role"])

    def test_user_prompt_fast_async_threshold_commits_previous_tool_rollout(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-fast-tool-backfill.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "function_call_output",
                            "output": "Exit code: 0\nRan 159 tests in 6.34s\nOK\n250a703a refs/heads/main",
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "User prompt: verify previous tool backfill under fast async.",
                    "transcript_path": str(rollout),
                    "thread_id": "codex-fast-tool-backfill-thread",
                },
                extra_env={"MATRIXARK_SESSION_COMMIT_THRESHOLD": "1"},
            )

            self.assertEqual("ok", msg["status"])
            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "session_buffer_event"
                    and (record.get("agent_hook") or {}).get("trigger") == "UserPromptSubmit:previous_tool_output_backfill"
                    and ((record.get("envelope") or {}).get("messages") or [{}])[0].get("role") == "tool"
                    and ((record.get("envelope") or {}).get("metadata") or {}).get("codex_event") == "PreviousToolOutputBackfill"
                    for record in records
                ),
                records,
            )
            auto_backfill_commits = [
                record
                for record in records
                if record.get("record_type") == "context_batch_commit"
                and record.get("commit_reason") == "threshold"
                and "tool" in record.get("source_roles", [])
                and "PreviousToolOutputBackfill" in record.get("source_codex_events", [])
            ]
            self.assertTrue(auto_backfill_commits, records)
            commit = auto_backfill_commits[0]
            self.assertEqual("provisional", commit["extraction_phase"])
            self.assertFalse(commit["final_session_boundary"])
            self.assertGreaterEqual(commit["memory_layers_written"]["session_entities"], 1)
            self.assertGreaterEqual(commit["memory_layers_written"]["profile_entities"], 1)
            self.assertGreaterEqual(commit["memory_layers_written"]["secondary_indexes"], 1)
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "tool_evidence"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "Ran 159 tests" in str(record.get("state") or "")
                    and "PreviousToolOutputBackfill" in record.get("source_codex_events", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("codex_event:previoustooloutputbackfill", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_2"},
                    "session_scope": "prefer",
                    "query": "What tool evidence proved tests passed and main was pushed?",
                    "max_context_tokens": 120,
                    "source_role_budget_tokens": {"assistant": 1},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            selected_evidence = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "tool_evidence"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("Ran 159 tests", selected_evidence["text"])
            self.assertNotIn("source_codex_events", selected_evidence)
            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertEqual({"assistant": 1}, role_policy["budget_tokens"])
            self.assertEqual(0, role_policy["selected_tokens_by_role"]["assistant"])
            memory_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"tool": 1}, memory_budget["source_message_counts_by_role"])

    def test_stop_rollout_backfill_only_commits_previous_assistant_profile_memory(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-async-rollout-backfill.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": "Assistant decision: delayed rollout backfill commits profile memory.",
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            cmd = [
                sys.executable,
                str(repo / "tools" / "matrixark_codex_hook.py"),
                "--backend",
                "local",
                "--event-log",
                str(event_log),
                "--event",
                "Stop",
                "--rollout-backfill-only",
                "--rollout-backfill-delay-ms",
                "0",
                "--account-id",
                "acct_hook",
                "--tenant-id",
                "tenant_hook",
                "--user-id",
                "codex_user",
                "--session-id",
                "codex_session_1",
                "--team",
                "agent",
                "--project",
                "context",
            ]
            proc = subprocess.run(
                cmd,
                input=json.dumps({"transcript_path": str(rollout), "thread_id": "codex-async-backfill-thread"}),
                text=True,
                capture_output=True,
                cwd=repo,
                timeout=30,
                env={**os.environ, "MATRIXARK_ALLOW_LOCAL_BACKEND": "1"},
            )
            if proc.returncode != 0:
                raise AssertionError(f"async rollout backfill failed\nstdout={proc.stdout}\nstderr={proc.stderr}")

            adapter = MatrixArkLocalAdapter(event_log)
            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_batch_commit"
                    and record.get("commit_reason") == "async_rollout_backfill"
                    and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
                    and "Stop:async_rollout_backfill" in record.get("source_codex_events", [])
                    and record.get("memory_layers_written", {}).get("profile_entities", 0) >= 1
                    for record in records
                ),
                records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "assistant_decision"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "delayed rollout backfill" in str(record.get("state") or "")
                    and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
                    and "Stop:async_rollout_backfill" in record.get("source_codex_events", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("hook_type:after_llm", index_names)
            self.assertIn("hook_type:session_commit", index_names)
            self.assertIn("codex_event:previousassistantbackfill", index_names)
            self.assertIn("codex_event:stop:async_rollout_backfill", index_names)

            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_2",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "What assistant decision was recovered by delayed rollout backfill?",
                    "max_context_tokens": 120,
                    "source_role_budget_tokens": {"tool": 1},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            selected_decision = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("delayed rollout backfill", selected_decision["text"])
            self.assertNotIn("source_codex_events", selected_decision)
            self.assertNotIn("budget_source_roles", selected_decision)
            self.assertNotIn("budget_source_role_counts", selected_decision)
            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertEqual({"tool": 1}, role_policy["budget_tokens"])
            self.assertEqual(0, role_policy["selected_tokens_by_role"]["tool"])
            memory_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"assistant": 1}, memory_budget["source_message_counts_by_role"])

            summary_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_3"},
                    "session_scope": "prefer",
                    "question_type": "broad_exploration",
                    "query": "Summarize delayed rollout backfill profile long term memory assistant decision.",
                    "max_context_tokens": 240,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 8,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            self.assert_no_default_context_pack_debug_lineage(summary_pack)
            refresh = summary_pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(summary_pack["used_context_tokens"], 240)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "assistant_decision"
                    and "delayed rollout backfill" in str(ref.get("text") or "")
                    for ref in summary_pack["selected_refs"]
                ),
                summary_pack["selected_refs"],
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "summary"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "delayed rollout backfill" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in summary_pack["selected_refs"]
                ),
                summary_pack["selected_refs"],
            )
            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, records)
            self.assertTrue(any("assistant" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any("PreviousAssistantBackfill" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any("assistant_decision" in record.get("source_entity_types", []) for record in profile_summaries))

    def test_post_tool_rollout_backfill_only_commits_tool_evidence_profile_memory(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-async-tool-rollout-backfill.jsonl"
            rollout = tmp / "rollout.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "function_call_output",
                            "output": "Exit code: 0\nRan 88 tests in 1.77s\nOK\nabc1234 refs/heads/main",
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            cmd = [
                sys.executable,
                str(repo / "tools" / "matrixark_codex_hook.py"),
                "--backend",
                "local",
                "--event-log",
                str(event_log),
                "--event",
                "PostToolUse",
                "--rollout-backfill-only",
                "--rollout-backfill-delay-ms",
                "0",
                "--account-id",
                "acct_hook",
                "--tenant-id",
                "tenant_hook",
                "--user-id",
                "codex_user",
                "--session-id",
                "codex_session_1",
                "--team",
                "agent",
                "--project",
                "context",
            ]
            proc = subprocess.run(
                cmd,
                input=json.dumps({"transcript_path": str(rollout), "thread_id": "codex-async-tool-backfill-thread"}),
                text=True,
                capture_output=True,
                cwd=repo,
                timeout=30,
                env={**os.environ, "MATRIXARK_ALLOW_LOCAL_BACKEND": "1"},
            )
            if proc.returncode != 0:
                raise AssertionError(f"async tool rollout backfill failed\nstdout={proc.stdout}\nstderr={proc.stderr}")

            adapter = MatrixArkLocalAdapter(event_log)
            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_batch_commit"
                    and record.get("commit_reason") == "async_rollout_backfill"
                    and "PreviousToolOutputBackfill" in record.get("source_codex_events", [])
                    and "PostToolUse:async_rollout_backfill" in record.get("source_codex_events", [])
                    and record.get("memory_layers_written", {}).get("profile_entities", 0) >= 1
                    for record in records
                ),
                records,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("entity_type") == "tool_evidence"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "Ran 88 tests" in str(record.get("state") or "")
                    and "PreviousToolOutputBackfill" in record.get("source_codex_events", [])
                    and "PostToolUse:async_rollout_backfill" in record.get("source_codex_events", [])
                    for record in records
                ),
                records,
            )
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("hook_type:tool_result", index_names)
            self.assertIn("hook_type:session_commit", index_names)
            self.assertIn("codex_event:previoustooloutputbackfill", index_names)
            self.assertIn("codex_event:posttooluse:async_rollout_backfill", index_names)

            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_2",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "What delayed tool evidence proved tests passed and main was pushed?",
                    "max_context_tokens": 120,
                    "source_role_budget_tokens": {"assistant": 1},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 3},
                }
            )
            selected_tool_ref = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "tool_evidence"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            )
            self.assertIn("Ran 88 tests", selected_tool_ref["text"])
            self.assertNotIn("source_codex_events", selected_tool_ref)
            self.assertNotIn("budget_source_roles", selected_tool_ref)
            self.assertNotIn("budget_source_role_counts", selected_tool_ref)
            role_policy = pack["recall_policy"]["source_role_budget"]
            self.assertEqual({"assistant": 1}, role_policy["budget_tokens"])
            self.assertEqual(0, role_policy["selected_tokens_by_role"]["assistant"])
            memory_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertEqual({"tool": 1}, memory_budget["source_message_counts_by_role"])

            summary_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "codex_session_3"},
                    "session_scope": "prefer",
                    "question_type": "broad_exploration",
                    "query": "Summarize delayed rollout backfill profile long term memory tool evidence.",
                    "max_context_tokens": 240,
                    "audit_mode": "off",
                    "ranking": {
                        "max_selected_refs": 8,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )
            self.assert_no_default_context_pack_debug_lineage(summary_pack)
            refresh = summary_pack["pre_retrieval_summary_refresh"]
            self.assertTrue(refresh["enabled"])
            self.assertEqual("refreshed", refresh["status"])
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            self.assertLessEqual(summary_pack["used_context_tokens"], 240)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "tool_evidence"
                    and "Ran 88 tests" in str(ref.get("text") or "")
                    for ref in summary_pack["selected_refs"]
                ),
                summary_pack["selected_refs"],
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "summary"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "Ran 88 tests" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in summary_pack["selected_refs"]
                ),
                summary_pack["selected_refs"],
            )
            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_hook", "user:codex_user", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, records)
            self.assertTrue(any("tool" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any("PreviousToolOutputBackfill" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any("tool_evidence" in record.get("source_entity_types", []) for record in profile_summaries))

    def test_message_ingest_batches_hot_path_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark-message-batch.jsonl"
            adapter = CountingLocalAdapter(event_log)
            server = MatrixArkMcpServer(adapter, line_json=True, access_mode="dev")
            result = server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Alice approved the GPU request after finance reviewed the budget.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_batch",
                        "tenant_id": "tenant_batch",
                        "user_id": "user_batch",
                        "session_id": "session_batch",
                    },
                    "metadata": {"node_path": ["tenant:tenant_batch", "user:user_batch", "session:session_batch"]},
                },
            )
            self.assertEqual("accepted", result["status"])
            self.assertTrue(any(size >= 7 for size in adapter.flushed_batch_sizes), adapter.flushed_batch_sizes)
            records = adapter.read_all()
            record_types = {record.get("record_type") for record in records}
            self.assertIn("context_event", record_types)
            self.assertIn("context_embedding", record_types)
            self.assertIn("context_index", record_types)
            self.assertIn("session_buffer_event", record_types)
            self.assertIn("context_summary_dirty", record_types)
            index_records = [record for record in records if record.get("record_type") == "context_index"]
            self.assertTrue(index_records)
            self.assertTrue(all("timestamp_key_ms" in record for record in index_records))
            self.assertTrue(all(isinstance(record.get("ref_hashes"), list) for record in index_records))
            self.assertTrue(all(isinstance(record.get("index_hash"), int) for record in index_records))
            self.assertEqual(len(index_records), len({record.get("index_hash") for record in index_records}))
            hot_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("summary_type") == "session_l0"
            ]
            self.assertTrue(hot_summaries)
            self.assertTrue(all(record.get("source_roles") == ["user"] for record in hot_summaries))
            self.assertTrue(all(record.get("source_role_counts") == {"user": 1} for record in hot_summaries))
            self.assertTrue(all(record.get("source_memory_selection_policies") == ["selected_user_prompt"] for record in hot_summaries))
            self.assertTrue(all(record.get("source_memory_selection_policy_counts") == {"selected_user_prompt": 1} for record in hot_summaries))
            self.assertTrue(all(record.get("memory_scope") == "session" for record in hot_summaries))
            self.assertTrue(all(record.get("session_continuity") == "same_session" for record in hot_summaries))

    def test_lightweight_async_ingest_promotes_profile_with_local_tenant_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-local-profile-fallback.jsonl")
            scope = {
                "account_id": "acct_local_profile",
                "user_id": "user_local_profile",
                "session_id": "session_local_profile",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": True,
                "session_buffer_threshold": 2,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Remember that local fallback profile promotion must always run."}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )
            self.assertEqual("accepted", first["status"])
            first_tasks = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "matrixark_async_pipeline_task"
            ]
            self.assertEqual(1, len(first_tasks))
            self.assertEqual(["session", "user_profile"], first_tasks[0]["source_memory_scopes"])
            self.assertEqual(["same_session", "cross_session"], first_tasks[0]["source_session_continuities"])
            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "assistant", "content": "Decision: use tenant_local_agent when tenant is missing."}],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                }
            )
            commit = second["auto_batch_extract_result"]
            self.assertEqual("committed", commit["status"])
            self.assertEqual("always_when_profile_scope_available", commit["profile_promotion_policy"])
            self.assertFalse(commit["profile_promotion_importance_gate"])
            self.assertEqual("", commit["profile_promotion_blocker"])
            self.assertTrue(commit["profile_promotion_scope_available"])
            self.assertGreaterEqual(commit["profile_entities_written"], 1)
            self.assertEqual(commit["entities_written"], commit["profile_entities_written"])
            self.assertTrue(commit["summary_refresh"]["profile_dirty_hashes"])

            records = adapter.read_all()
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("batch_id_hash") == commit["batch_id_hash"]
            ]
            self.assertTrue(profile_entities)
            self.assertTrue(
                all(
                    (record.get("scope") or record.get("access_scope") or {}).get("tenant_id") == "tenant_local_agent"
                    and (record.get("scope") or record.get("access_scope") or {}).get("user_id") == "user_local_profile"
                    and not (record.get("scope") or record.get("access_scope") or {}).get("session_id")
                    and record.get("profile_promotion_policy") == "always_when_profile_scope_available"
                    and record.get("profile_promotion_importance_gate") is False
                    and record.get("profile_promotion_blocker") == ""
                    for record in profile_entities
                )
            )
            self.assertTrue(all("session" in record.get("source_memory_scopes", []) for record in profile_entities))
            self.assertTrue(all("user_profile" in record.get("source_memory_scopes", []) for record in profile_entities))
            self.assertTrue(
                all("same_session" in record.get("source_session_continuities", []) for record in profile_entities)
            )
            self.assertTrue(
                all("cross_session" in record.get("source_session_continuities", []) for record in profile_entities)
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_node"
                    and record.get("node_path") == [
                        "tenant:tenant_local_agent",
                        "user:user_local_profile",
                        "profile:long_term_memory",
                    ]
                    for record in records
                )
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_local_profile_followup"},
                    "session_scope": "prefer",
                    "query": "What decision did we make for missing tenant profile promotion?",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 4},
                }
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "tenant_local_agent" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )
            coverage = pack["retrieval_metrics"]["retrieval_model_coverage"]
            self.assertTrue(coverage["compact_scope_recovery_enabled"])
            self.assertGreaterEqual(coverage["entity_embedding_vectors"], 1)

    def test_batch_extract_promotes_every_entity_when_profile_scope_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-promote-all.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_promote_all",
                        "tenant_id": "tenant_promote_all",
                        "user_id": "user_promote_all",
                        "session_id": "session_promote_all",
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": "Plain low-signal note without a durable preference keyword.",
                        }
                    ],
                    "metadata": {
                        "hook_type": "before_llm",
                        "codex_event": "UserPromptSubmit",
                        "importance": 0.01,
                    },
                    "threshold_messages": 1,
                    "skip_prior_context": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
            self.assertFalse(result["profile_promotion_importance_gate"])
            self.assertEqual("", result["profile_promotion_blocker"])
            self.assertTrue(result["profile_promotion_scope_available"])
            self.assertGreaterEqual(result["entities_written"], 1)
            self.assertEqual(result["entities_written"], result["profile_entities_written"])
            self.assertEqual(result["entities_written"], len(result["profile_promotion_summary"]))

            records = adapter.read_all()
            session_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("batch_id_hash") == result["batch_id_hash"]
                and record.get("memory_scope") == "session"
            ]
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("batch_id_hash") == result["batch_id_hash"]
                and record.get("memory_scope") == "user_profile"
            ]
            self.assertEqual(len(session_entities), len(profile_entities))
            self.assertTrue(all(float(record.get("confidence") or 0.0) <= 0.6 for record in session_entities))
            self.assertTrue(
                all(
                    record.get("session_continuity") == "cross_session"
                    and record.get("promoted_from_memory_scope") == "session"
                    and record.get("profile_promotion_policy") == "always_when_profile_scope_available"
                    and record.get("profile_promotion_importance_gate") is False
                    and record.get("profile_promotion_blocker") == ""
                    for record in profile_entities
                )
            )
            self.assertTrue(result["summary_refresh"]["profile_dirty_hashes"])


    def test_lightweight_async_ingest_threshold_and_idle_commit_flush_session_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-async-threshold-idle.jsonl")
            scope = {
                "account_id": "acct_async",
                "tenant_id": "tenant_async",
                "user_id": "user_async",
                "session_id": "session_async",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": True,
                "session_buffer_threshold": 2,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Plan to extract Codex prompts in batches."}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )
            self.assertEqual("accepted", first["status"])
            self.assertIsNone(first["auto_batch_extract_result"])
            self.assertEqual(1, first["session_buffer"]["pending_event_count"])
            self.assertFalse(first["session_buffer"]["threshold_ready"])
            self.assertFalse(first["session_buffer"]["idle_ready"])
            first_records = adapter.read_all()
            first_dirty = [record for record in first_records if record.get("record_type") == "context_summary_dirty"]
            self.assertTrue(first_dirty)
            self.assertTrue(all(record.get("source_role_counts") == {"user": 1} for record in first_dirty))
            self.assertTrue(all(record.get("source_hook_type_counts") == {"before_llm": 1} for record in first_dirty))
            self.assertTrue(all(record.get("source_codex_event_counts") == {"UserPromptSubmit": 1} for record in first_dirty))
            self.assertTrue(all(record.get("source_memory_selection_policies") == ["selected_user_prompt"] for record in first_dirty))
            self.assertTrue(all(record.get("source_memory_selection_policy_counts") == {"selected_user_prompt": 1} for record in first_dirty))
            first_tasks = [record for record in first_records if record.get("record_type") == "matrixark_async_pipeline_task"]
            self.assertEqual(1, len(first_tasks))
            self.assertEqual(["user"], first_tasks[0]["source_roles"])
            self.assertEqual(["before_llm"], first_tasks[0]["source_hook_types"])
            self.assertEqual(["UserPromptSubmit"], first_tasks[0]["source_codex_events"])
            self.assertEqual({"user": 1}, first_tasks[0]["source_role_counts"])
            self.assertEqual({"before_llm": 1}, first_tasks[0]["source_hook_type_counts"])
            self.assertEqual({"UserPromptSubmit": 1}, first_tasks[0]["source_codex_event_counts"])
            self.assertEqual(["selected_user_prompt"], first_tasks[0]["source_memory_selection_policies"])
            self.assertEqual({"selected_user_prompt": 1}, first_tasks[0]["source_memory_selection_policy_counts"])
            self.assertEqual(["session", "user_profile"], first_tasks[0]["source_memory_scopes"])
            self.assertEqual(["same_session", "cross_session"], first_tasks[0]["source_session_continuities"])

            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "assistant", "content": "Decision: threshold commits should flush without waiting for Stop."}],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                }
            )
            self.assertEqual("committed", second["auto_batch_extract_result"]["status"])
            self.assertEqual("threshold", second["auto_batch_extract_result"]["trigger_policy"])
            self.assertEqual("provisional", second["auto_batch_extract_result"]["extraction_phase"])
            self.assertFalse(second["auto_batch_extract_result"]["final_session_boundary"])
            self.assertEqual(2, second["auto_batch_extract_result"]["committed_event_count"])
            threshold_layers = second["auto_batch_extract_result"]["memory_layers_written"]
            self.assertEqual(2, threshold_layers["context_events"])
            self.assertGreaterEqual(threshold_layers["segments"], 1)
            self.assertGreaterEqual(threshold_layers["session_entities"], 1)
            self.assertEqual(threshold_layers["session_entities"], threshold_layers["profile_entities"])
            self.assertGreaterEqual(threshold_layers["secondary_indexes"], 1)
            self.assertGreaterEqual(threshold_layers["summary_dirty_nodes"], 1)
            self.assertEqual("dirty_marked", threshold_layers["summary_refresh_status"])
            self.assertEqual(["assistant", "user"], threshold_layers["source_roles"])
            self.assertEqual(["after_llm", "before_llm"], threshold_layers["source_hook_types"])
            self.assertEqual(["Stop", "UserPromptSubmit"], threshold_layers["source_codex_events"])
            self.assertEqual("always_when_profile_scope_available", threshold_layers["profile_promotion_policy"])
            self.assertEqual(["assistant", "user"], second["auto_batch_extract_result"]["source_roles"])
            self.assertEqual({"assistant": 1, "user": 1}, second["auto_batch_extract_result"]["source_role_counts"])
            self.assertEqual({"after_llm": 1, "before_llm": 1}, second["auto_batch_extract_result"]["source_hook_type_counts"])
            self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, second["auto_batch_extract_result"]["source_codex_event_counts"])
            threshold_records = adapter.read_all()
            threshold_committed_events = [
                record
                for record in threshold_records
                if record.get("record_type") == "context_event"
                and record.get("batch_id_hash") == second["auto_batch_extract_result"]["batch_id_hash"]
                and record.get("status") == "extraction_committed"
            ]
            self.assertEqual(2, len(threshold_committed_events))
            threshold_committed_event_hashes = {record.get("event_id_hash") for record in threshold_committed_events}
            self.assertEqual(
                {int(event_id) for event_id in second["auto_batch_extract_result"]["source_event_ids"]},
                threshold_committed_event_hashes,
            )
            threshold_event_embeddings = [
                record
                for record in threshold_records
                if record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "event_text"
                and record.get("ref_hash") in threshold_committed_event_hashes
            ]
            self.assertGreaterEqual(len(threshold_event_embeddings), 2)
            threshold_commit = next(
                record
                for record in threshold_records
                if record.get("record_type") == "context_batch_commit"
                and record.get("commit_id_hash") == second["auto_batch_extract_result"]["commit_id_hash"]
            )
            self.assertEqual(second["auto_batch_extract_result"]["source_role_counts"], threshold_commit["source_role_counts"])
            self.assertEqual(second["auto_batch_extract_result"]["source_hook_type_counts"], threshold_commit["source_hook_type_counts"])
            self.assertEqual(second["auto_batch_extract_result"]["source_codex_event_counts"], threshold_commit["source_codex_event_counts"])
            self.assertEqual("always_when_profile_scope_available", threshold_commit["profile_promotion_policy"])
            self.assertFalse(threshold_commit["profile_promotion_importance_gate"])
            self.assertEqual("always_when_profile_scope_available", threshold_commit["memory_layers_written"]["profile_promotion_policy"])
            self.assertFalse(threshold_commit["memory_layers_written"]["profile_promotion_importance_gate"])
            threshold_pending_tasks = [
                record
                for record in threshold_records
                if record.get("record_type") == "matrixark_async_pipeline_task"
                and record.get("status") == "pending"
            ]
            self.assertGreaterEqual(len(threshold_pending_tasks), 2)
            self.assertTrue(any(task.get("source_role_counts") == {"assistant": 1} for task in threshold_pending_tasks))
            self.assertTrue(any(task.get("source_hook_type_counts") == {"after_llm": 1} for task in threshold_pending_tasks))
            self.assertTrue(any(task.get("source_codex_event_counts") == {"Stop": 1} for task in threshold_pending_tasks))
            threshold_tasks = [
                record
                for record in threshold_records
                if record.get("record_type") == "matrixark_async_pipeline_task"
                and record.get("commit_id_hash") == second["auto_batch_extract_result"]["commit_id_hash"]
            ]
            self.assertEqual(2, len(threshold_tasks))
            self.assertTrue(all(task["source_role_counts"] == {"assistant": 1, "user": 1} for task in threshold_tasks))
            self.assertTrue(all(task["source_hook_type_counts"] == {"after_llm": 1, "before_llm": 1} for task in threshold_tasks))
            self.assertTrue(all(task["source_codex_event_counts"] == {"Stop": 1, "UserPromptSubmit": 1} for task in threshold_tasks))
            threshold_refresh = second["auto_batch_extract_result"]["summary_refresh"]
            self.assertTrue(threshold_refresh["session_dirty_hashes"])
            self.assertTrue(threshold_refresh["profile_dirty_hashes"])
            self.assertTrue(threshold_refresh["profile_summary_refresh_required"])
            threshold_promotions = second["auto_batch_extract_result"]["profile_promotion_summary"]
            self.assertGreaterEqual(len(threshold_promotions), 1)
            self.assertEqual(second["auto_batch_extract_result"]["entities_written"], len(threshold_promotions))
            self.assertEqual("always_when_profile_scope_available", second["auto_batch_extract_result"]["profile_promotion_policy"])
            self.assertEqual("", second["auto_batch_extract_result"]["profile_promotion_blocker"])
            self.assertTrue(second["auto_batch_extract_result"]["profile_promotion_scope_available"])
            self.assertTrue(all(item.get("source_session_ids") == ["session_async"] for item in threshold_promotions))
            self.assertTrue(all(item.get("source_entity_count", 0) >= 1 for item in threshold_promotions))
            self.assertTrue(second["session_buffer"]["threshold_ready"])
            self.assertFalse(second["session_buffer"]["idle_ready"])
            threshold_evidence = second["auto_batch_extract_result"]["trigger_evidence"]
            self.assertTrue(threshold_evidence["threshold_ready"])
            self.assertFalse(threshold_evidence["idle_ready"])
            self.assertEqual(2, threshold_evidence["pending_event_count"])
            self.assertEqual(2, threshold_evidence["threshold_messages"])

            third = adapter.ingest(
                {
                    **base_args,
                    "auto_batch_extract": False,
                    "messages": [{"role": "tool", "content": "Exit code: 0\nRan 3 tests in 0.01s\nOK"}],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "PostToolUse"},
                }
            )
            self.assertEqual(1, third["session_buffer"]["pending_event_count"])
            self.assertFalse(third["session_buffer"]["threshold_ready"])
            self.assertFalse(third["session_buffer"]["idle_ready"])
            idle = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": False,
                    "idle_timeout_ms": 0,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", idle["status"])
            self.assertEqual("idle_timeout", idle["trigger_policy"])
            self.assertEqual("provisional", idle["extraction_phase"])
            self.assertFalse(idle["final_session_boundary"])
            self.assertEqual(1, idle["committed_event_count"])
            self.assertEqual(2, idle["extraction_context_event_count"])
            idle_layers = idle["memory_layers_written"]
            self.assertEqual(1, idle_layers["context_events"])
            self.assertGreaterEqual(idle_layers["segments"], 1)
            self.assertGreaterEqual(idle_layers["session_entities"], 1)
            self.assertEqual(idle_layers["session_entities"], idle_layers["profile_entities"])
            self.assertGreaterEqual(idle_layers["secondary_indexes"], 1)
            self.assertGreaterEqual(idle_layers["summary_dirty_nodes"], 1)
            self.assertEqual("dirty_marked", idle_layers["summary_refresh_status"])
            self.assertEqual(["tool"], idle_layers["source_roles"])
            self.assertEqual(["hook_boundary"], idle_layers["source_hook_types"])
            self.assertEqual(["PostToolUse"], idle_layers["source_codex_events"])
            self.assertEqual("always_when_profile_scope_available", idle_layers["profile_promotion_policy"])
            idle_refresh = idle["summary_refresh"]
            self.assertTrue(idle_refresh["session_dirty_hashes"])
            self.assertTrue(idle_refresh["profile_dirty_hashes"])
            self.assertTrue(idle_refresh["profile_summary_refresh_required"])
            self.assertEqual(["tool"], idle["source_roles"])
            self.assertEqual(["hook_boundary"], idle["source_hook_types"])
            self.assertEqual(["PostToolUse"], idle["source_codex_events"])
            self.assertEqual({"tool": 1}, idle["source_role_counts"])
            self.assertEqual({"hook_boundary": 1}, idle["source_hook_type_counts"])
            self.assertEqual({"PostToolUse": 1}, idle["source_codex_event_counts"])
            idle_promotions = idle["profile_promotion_summary"]
            self.assertGreaterEqual(len(idle_promotions), 1)
            self.assertEqual(idle["entities_written"], len(idle_promotions))
            self.assertEqual("always_when_profile_scope_available", idle["profile_promotion_policy"])
            self.assertEqual("", idle["profile_promotion_blocker"])
            self.assertTrue(idle["profile_promotion_scope_available"])
            self.assertTrue(all(item.get("source_session_ids") == ["session_async"] for item in idle_promotions))
            self.assertTrue(any("tool" in item.get("source_roles", []) for item in idle_promotions))
            self.assertEqual(
                [int(event_id) for event_id in second["auto_batch_extract_result"]["source_event_ids"]],
                [int(event_id) for event_id in idle["extraction_context_event_ids"]],
            )
            idle_evidence = idle["trigger_evidence"]
            self.assertFalse(idle_evidence["threshold_ready"])
            self.assertTrue(idle_evidence["idle_ready"])
            self.assertEqual(1, idle_evidence["pending_event_count"])
            self.assertEqual(0, idle_evidence["idle_timeout_ms"])

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertEqual(2, len(commits))
            self.assertTrue(all(commit.get("extraction_phase") == "provisional" for commit in commits))
            self.assertTrue(all(commit.get("final_session_boundary") is False for commit in commits))
            self.assertTrue(all(isinstance(commit.get("trigger_evidence"), dict) for commit in commits))
            self.assertTrue(all(commit.get("profile_promotion_summary") for commit in commits))
            self.assertTrue(all(commit.get("profile_promotion_policy") == "always_when_profile_scope_available" for commit in commits))
            self.assertTrue(all(commit.get("memory_layers_written", {}).get("profile_promotion_policy") == "always_when_profile_scope_available" for commit in commits))
            self.assertTrue(all(commit.get("summary_refresh", {}).get("profile_summary_refresh_required") for commit in commits))
            self.assertTrue(all(commit.get("memory_layers_written", {}).get("session_entities", 0) >= 1 for commit in commits))
            self.assertTrue(all(commit.get("memory_layers_written", {}).get("profile_entities", 0) >= 1 for commit in commits))
            self.assertTrue(all(commit.get("memory_layers_written", {}).get("secondary_indexes", 0) >= 1 for commit in commits))
            refresh_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "summary_refresh", "page_size": 20}
            )
            refresh_rows = refresh_dashboard["rows"]
            refresh_commits = [
                row for row in refresh_rows if row.get("row_type") == "context_batch_commit"
            ]
            self.assertEqual(2, len(refresh_commits), refresh_dashboard)
            self.assertTrue(all(row.get("profile_summary_refresh_required") for row in refresh_commits))
            self.assertTrue(all(row.get("profile_dirty_hash_count", 0) > 0 for row in refresh_commits))
            self.assertTrue(all(row.get("memory_layers_written", {}).get("session_entities", 0) >= 1 for row in refresh_commits))
            self.assertTrue(all(row.get("memory_layers_written", {}).get("profile_entities", 0) >= 1 for row in refresh_commits))
            self.assertTrue(all(row.get("memory_layers_written", {}).get("secondary_indexes", 0) >= 1 for row in refresh_commits))
            self.assertTrue(any(row.get("source_role_counts", {}).get("assistant", 0) >= 1 for row in refresh_commits))
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("after_llm", 0) >= 1 for row in refresh_commits))
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("Stop", 0) >= 1 for row in refresh_commits))
            self.assertTrue(
                any(
                    row.get("row_type") == "context_summary_dirty"
                    and row.get("dirty_reason") == "profile_entity_promoted"
                    and row.get("source_role_counts", {}).get("assistant", 0) >= 1
                    for row in refresh_rows
                ),
                refresh_dashboard,
            )
            self.assertTrue(
                any(
                    row.get("row_type") == "context_summary_dirty"
                    and row.get("dirty_reason") == "new_event"
                    and row.get("source_hook_type_counts", {}).get("before_llm", 0) >= 1
                    for row in refresh_rows
                ),
                refresh_dashboard,
            )
            pipeline_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "async_pipeline", "page_size": 20}
            )
            pipeline_rows = pipeline_dashboard["rows"]
            extraction_tasks = [
                row
                for row in pipeline_rows
                if row.get("row_type") == "matrixark_async_pipeline_task"
                and row.get("status") == "extraction_committed"
            ]
            self.assertEqual(3, len(extraction_tasks), pipeline_dashboard)
            self.assertEqual(
                {
                    int(event_id)
                    for commit in commits
                    for event_id in commit.get("source_event_ids", [])
                },
                {int(row.get("event_id_hash")) for row in extraction_tasks},
            )
            self.assertEqual(
                {int(commit.get("commit_id_hash")) for commit in commits},
                {int(row.get("commit_id_hash")) for row in extraction_tasks},
            )
            self.assertEqual(
                {"threshold", "idle_timeout"},
                {row.get("trigger_policy") for row in extraction_tasks},
            )
            self.assertTrue(all(row.get("completed_stages") == ["extraction"] for row in extraction_tasks))
            self.assertTrue(any(row.get("source_role_counts", {}).get("assistant", 0) >= 1 for row in extraction_tasks))
            self.assertTrue(any(row.get("source_role_counts", {}).get("tool", 0) >= 1 for row in extraction_tasks))
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for row in extraction_tasks))
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for row in extraction_tasks))
            self.assertTrue(all(row.get("summary_pending") for row in extraction_tasks))
            self.assertTrue(all(row.get("compression_pending") for row in extraction_tasks))
            self.assertTrue(all(row.get("embedding_pending") for row in extraction_tasks))
            self.assertTrue(all(row.get("summary_refresh_status") == "dirty_marked" for row in extraction_tasks))
            self.assertTrue(all(row.get("memory_layers_written", {}).get("profile_entities", 0) >= 1 for row in extraction_tasks))
            events_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "events", "page_size": 20}
            )
            event_rows = [row for row in events_dashboard["rows"] if row.get("row_type") == "context_event"]
            self.assertGreaterEqual(len(event_rows), 3, events_dashboard)
            self.assertTrue(all(row.get("memory_scope") == "session" for row in event_rows), events_dashboard)
            self.assertTrue(all(row.get("session_continuity") == "same_session" for row in event_rows), events_dashboard)
            self.assertTrue(any(row.get("source_role_counts", {}).get("assistant", 0) >= 1 for row in event_rows), events_dashboard)
            self.assertTrue(any(row.get("source_role_counts", {}).get("tool", 0) >= 1 for row in event_rows), events_dashboard)
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for row in event_rows), events_dashboard)
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for row in event_rows), events_dashboard)
            entities_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "entities", "page_size": 50}
            )
            entity_rows = [row for row in entities_dashboard["rows"] if row.get("row_type") == "context_entity"]
            self.assertTrue(any(row.get("memory_scope") == "session" for row in entity_rows), entities_dashboard)
            profile_entity_rows = [
                row
                for row in entity_rows
                if row.get("memory_scope") == "user_profile"
                and row.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(profile_entity_rows, entities_dashboard)
            self.assertTrue(
                all(
                    row.get("profile_promotion_policy") == "always_when_profile_scope_available"
                    and row.get("profile_promotion_blocker") == ""
                    for row in profile_entity_rows
                ),
                entities_dashboard,
            )
            self.assertTrue(any("session_async" in row.get("source_session_ids", []) for row in profile_entity_rows), entities_dashboard)
            self.assertTrue(any(row.get("source_role_counts", {}).get("assistant", 0) >= 1 for row in profile_entity_rows), entities_dashboard)
            self.assertTrue(any(row.get("source_role_counts", {}).get("tool", 0) >= 1 for row in profile_entity_rows), entities_dashboard)
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for row in profile_entity_rows), entities_dashboard)
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for row in profile_entity_rows), entities_dashboard)
            embeddings_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "embeddings", "page_size": 80}
            )
            embedding_rows = [row for row in embeddings_dashboard["rows"] if row.get("row_type") == "context_embedding"]
            embedding_types = {row.get("embedding_type") for row in embedding_rows}
            self.assertIn("event_text", embedding_types, embeddings_dashboard)
            self.assertIn("entity_state", embedding_types, embeddings_dashboard)
            self.assertIn("segment_text", embedding_types, embeddings_dashboard)
            self.assertIn("batch_l0", embedding_types, embeddings_dashboard)
            self.assertTrue(all(row.get("dim", 0) > 0 for row in embedding_rows), embeddings_dashboard)
            self.assertTrue(all(row.get("has_vector") for row in embedding_rows), embeddings_dashboard)
            self.assertTrue(all("vector" not in row for row in embedding_rows), embeddings_dashboard)
            indexes_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "indexes", "page_size": 80}
            )
            index_rows = [row for row in indexes_dashboard["rows"] if row.get("row_type") == "context_index"]
            self.assertTrue(index_rows, indexes_dashboard)
            self.assertTrue(any(row.get("data_model") == "context_entity" for row in index_rows), indexes_dashboard)
            self.assertTrue(any(row.get("data_model") == "context_profile_entity" for row in index_rows), indexes_dashboard)
            self.assertTrue(any(row.get("data_model") == "context_batch_commit" for row in index_rows), indexes_dashboard)
            self.assertTrue(all(row.get("ref_hash_count", 0) >= 0 for row in index_rows), indexes_dashboard)
            summaries_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "summaries", "page_size": 20}
            )
            summary_rows = [row for row in summaries_dashboard["rows"] if row.get("row_type") == "context_summary"]
            self.assertTrue(summary_rows, summaries_dashboard)
            self.assertTrue(any(row.get("summary_type") == "batch_l0" for row in summary_rows), summaries_dashboard)
            self.assertTrue(any(row.get("source_role_counts", {}).get("assistant", 0) >= 1 for row in summary_rows), summaries_dashboard)
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for row in summary_rows), summaries_dashboard)
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for row in summary_rows), summaries_dashboard)
            for table_name in ["embeddings", "indexes", "summaries"]:
                self.assertIn(table_name, embeddings_dashboard["totals"])
            committed_ids = {
                int(event_id)
                for commit in commits
                for event_id in commit.get("source_event_ids", [])
            }
            pending_ids = {
                int(record.get("event_id_hash"))
                for record in records
                if record.get("record_type") == "session_buffer_event"
            }
            self.assertEqual(pending_ids, committed_ids)
            self.assertFalse(adapter.pending_session_events(scope))
            idle_commit = next(commit for commit in commits if commit.get("trigger_policy") == "idle_timeout")
            self.assertEqual(2, idle_commit["extraction_context_event_count"])
            self.assertEqual(["tool"], idle_commit["source_roles"])
            self.assertEqual(["hook_boundary"], idle_commit["source_hook_types"])
            self.assertEqual(["PostToolUse"], idle_commit["source_codex_events"])
            extraction_audits = [
                record
                for record in records
                if record.get("record_type") == "context_extraction_audit"
                and record.get("batch_id_hash") == idle["batch_id_hash"]
            ]
            self.assertEqual(1, len(extraction_audits))
            self.assertEqual(
                [int(event_id) for event_id in idle["extraction_context_event_ids"]],
                [int(event_id) for event_id in extraction_audits[0]["extraction_context_event_ids"]],
            )
            profile_tool_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("entity_type") == "tool_evidence"
            ]
            self.assertTrue(profile_tool_entities)
            self.assertTrue(any("Exit code: 0" in str(record.get("state") or "") for record in profile_tool_entities))
            self.assertTrue(any("tool" in record.get("source_roles", []) for record in profile_tool_entities))
            self.assertTrue(any("hook_boundary" in record.get("source_hook_types", []) for record in profile_tool_entities))
            self.assertTrue(any("PostToolUse" in record.get("source_codex_events", []) for record in profile_tool_entities))
            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "later_async_session"},
                    "session_scope": "prefer",
                    "query": "What tool evidence proved the async threshold and idle extraction path?",
                    "max_context_tokens": 240,
                    "audit_mode": "telemetry_only",
                    "ranking": {"max_selected_refs": 4},
                }
            )
            selected_refs = pack["selected_refs"]
            selected_tool_refs = [
                ref
                for ref in selected_refs
                if ref.get("ref_type") == "entity"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
                and ref.get("entity_type") == "tool_evidence"
            ]
            self.assertTrue(selected_tool_refs, selected_refs)
            self.assertTrue(any("Exit code: 0" in str(ref.get("text") or "") for ref in selected_tool_refs))
            for ref in selected_tool_refs:
                self.assertNotIn("source_session_ids", ref)
                self.assertNotIn("source_roles", ref)
                self.assertNotIn("source_hook_types", ref)
                self.assertNotIn("source_codex_events", ref)
            self.assertNotIn("memory_layer_budget", pack)
            budget = pack["retrieval_metrics"]["memory_layer_budget"]
            coverage = pack["retrieval_metrics"]["retrieval_model_coverage"]
            self.assertTrue(coverage["compact_scope_recovery_enabled"])
            self.assertGreaterEqual(coverage["node_scope_recovered_count"], 1)
            self.assertGreaterEqual(coverage["event_embedding_vectors"], 1)
            self.assertGreaterEqual(coverage["entity_embedding_vectors"], 1)
            self.assertGreaterEqual(coverage["segment_embedding_vectors"], 1)
            self.assertGreaterEqual(coverage["index_terms_by_ref"], 1)
            self.assertGreaterEqual(coverage["index_terms_by_batch"], 1)
            if "recall_policy" in pack:
                self.assertEqual(coverage, pack["recall_policy"]["retrieval_model_coverage"])
            self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(budget["by_extraction_phase"]["provisional"]["refs"], 1)
            for field in [
                "by_source_role",
                "by_hook_type",
                "by_codex_event",
                "source_message_counts_by_role",
                "source_hook_counts_by_type",
                "source_codex_event_counts_by_event",
            ]:
                self.assertNotIn(field, budget)
            readiness = pack["retrieval_metrics"]["async_pipeline_readiness"]
            self.assertEqual(3, readiness["task_count"])
            self.assertEqual(3, readiness["extraction_committed_task_count"])
            self.assertFalse(readiness["ready_for_retrieval"])
            self.assertIn("summary", readiness["remaining_stages"])
            self.assertIn("compression", readiness["remaining_stages"])
            self.assertIn("embedding", readiness["remaining_stages"])
            self.assertEqual(3, readiness["remaining_stage_counts"]["summary"])
            self.assertEqual(3, readiness["remaining_stage_counts"]["compression"])
            self.assertEqual(3, readiness["remaining_stage_counts"]["embedding"])
            self.assertNotIn("pending_source_roles", readiness)
            self.assertNotIn("pending_source_hook_types", readiness)
            self.assertNotIn("pending_source_codex_events", readiness)
            self.assertNotIn("pending_memory_scopes", readiness)
            self.assertNotIn("pending_session_continuities", readiness)
            self.assertNotIn("pending_extraction_phases", readiness)
            self.assertNotIn("pending_final_session_boundary_count", readiness)
            layer_readiness = readiness["memory_layer_readiness"]
            self.assertFalse(layer_readiness["ready_for_retrieval"])
            self.assertIn("user_profile", layer_readiness["blocked_layers"])
            self.assertIn("cross_session", layer_readiness["blocked_layers"])
            self.assertIn("summary", layer_readiness["blocked_layers"])
            self.assertIn("compression", layer_readiness["blocked_layers"])
            self.assertIn("embedding", layer_readiness["blocked_layers"])
            self.assertFalse(layer_readiness["layers"]["user_profile"]["ready"])
            self.assertFalse(layer_readiness["layers"]["cross_session"]["ready"])
            self.assertEqual(3, layer_readiness["layers"]["summary"]["pending_task_count"])
            self.assertEqual(["summary"], layer_readiness["layers"]["summary"]["remaining_stages"])
            self.assertIn("async_pipeline_followup_pending", readiness["freshness_warnings"])
            self.assertTrue(
                any(str(warning).startswith("async_pipeline_remaining_stages:") for warning in pack.get("quality_warnings", [])),
                pack,
            )
            debug_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "later_async_session"},
                    "session_scope": "prefer",
                    "query": "What source roles are still pending in async threshold extraction?",
                    "max_context_tokens": 240,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 4},
                }
            )
            debug_readiness = debug_pack["retrieval_metrics"]["async_pipeline_readiness"]
            self.assertEqual(2, debug_readiness["pending_source_roles"]["assistant"])
            self.assertEqual(2, debug_readiness["pending_source_roles"]["user"])
            self.assertEqual(1, debug_readiness["pending_source_roles"]["tool"])
            self.assertEqual(1, debug_readiness["pending_source_hook_types"]["hook_boundary"])
            self.assertEqual(1, debug_readiness["pending_source_codex_events"]["PostToolUse"])
            self.assertEqual(3, debug_readiness["pending_memory_scopes"]["session"])
            self.assertEqual(3, debug_readiness["pending_memory_scopes"]["user_profile"])
            self.assertEqual(3, debug_readiness["pending_session_continuities"]["same_session"])
            self.assertEqual(3, debug_readiness["pending_session_continuities"]["cross_session"])
            self.assertEqual(3, debug_readiness["pending_extraction_phases"]["provisional"])
            self.assertNotIn("final", debug_readiness["pending_extraction_phases"])
            self.assertEqual(0, debug_readiness["pending_final_session_boundary_count"])
            telemetry_rows = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_pack_telemetry"
                and record.get("context_pack_id") == pack["pack_id"]
            ]
            self.assertTrue(telemetry_rows)
            telemetry_readiness = telemetry_rows[-1]["async_pipeline_readiness"]
            self.assertEqual(readiness["task_count"], telemetry_readiness["task_count"])
            self.assertEqual(readiness["remaining_stage_counts"], telemetry_readiness["remaining_stage_counts"])
            self.assertNotIn("pending_memory_scopes", readiness)
            self.assertEqual(debug_readiness["pending_memory_scopes"], telemetry_readiness["pending_memory_scopes"])
            self.assertEqual(2, telemetry_readiness["pending_source_roles"]["assistant"])
            self.assertEqual(1, telemetry_readiness["pending_source_hook_types"]["hook_boundary"])
            self.assertEqual(1, telemetry_readiness["pending_source_codex_events"]["PostToolUse"])
            replay = adapter.replay({"context_pack_id": pack["pack_id"], "enable_replay": True})
            replay_telemetry = [
                row for row in replay["events"] if row.get("record_type") == "context_pack_telemetry"
            ]
            self.assertTrue(replay_telemetry, replay)
            self.assertEqual(telemetry_readiness, replay_telemetry[-1]["async_pipeline_readiness"])
            pack_dashboard = adapter.ingestion_dashboard({"scope": scope, "table": "context_packs", "page_size": 10})
            pack_rows = [row for row in pack_dashboard["rows"] if row.get("context_pack_id") == pack["pack_id"]]
            self.assertTrue(pack_rows, pack_dashboard)
            self.assertEqual(telemetry_readiness, pack_rows[-1]["async_pipeline_readiness"])

            refresh = adapter.refresh_summaries(
                {
                    "scope": {
                        "account_id": scope["account_id"],
                        "tenant_id": scope["tenant_id"],
                        "user_id": scope["user_id"],
                    },
                    "limit": 16,
                    "refreshed_at_ms": 1780000000999,
                }
            )
            profile_summaries = [
                item
                for item in refresh.get("refreshed", [])
                if item.get("node_path") == ["tenant:tenant_async", "user:user_async", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries, refresh)
            self.assertTrue(any("tool" in item.get("source_roles", []) for item in profile_summaries))
            self.assertTrue(any(item.get("source_role_counts", {}).get("tool", 0) >= 1 for item in profile_summaries))
            self.assertTrue(any("hook_boundary" in item.get("source_hook_types", []) for item in profile_summaries))
            self.assertTrue(any(item.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for item in profile_summaries))
            self.assertTrue(any("PostToolUse" in item.get("source_codex_events", []) for item in profile_summaries))
            self.assertTrue(any(item.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for item in profile_summaries))
            self.assertTrue(any("user_profile" in item.get("source_memory_scopes", []) for item in profile_summaries))
            self.assertTrue(any("session" in item.get("source_memory_scopes", []) for item in profile_summaries))
            self.assertTrue(any("cross_session" in item.get("source_session_continuities", []) for item in profile_summaries))
            self.assertTrue(any("same_session" in item.get("source_session_continuities", []) for item in profile_summaries))
            self.assertTrue(any(item.get("async_summary_progress_count", 0) >= 1 for item in profile_summaries))
            summary_progress = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "matrixark_async_pipeline_task"
                and record.get("status") == "summary_completed"
            ]
            self.assertTrue(summary_progress)
            self.assertTrue(all("summary" in record.get("completed_stages", []) for record in summary_progress))
            self.assertTrue(all("embedding" in record.get("completed_stages", []) for record in summary_progress))
            self.assertTrue(all("compression" in record.get("completed_stages", []) for record in summary_progress))
            self.assertTrue(all(not record.get("remaining_stages", []) for record in summary_progress))
            self.assertTrue(all(record.get("summary_completed") for record in summary_progress))
            self.assertTrue(all(record.get("embedding_completed") for record in summary_progress))
            self.assertTrue(all(record.get("compression_completed") for record in summary_progress))
            self.assertTrue(any(record.get("generated_summary_types") for record in summary_progress))
            completed_pipeline_dashboard = adapter.ingestion_dashboard(
                {"scope": scope, "table": "async_pipeline", "page_size": 20}
            )
            completed_pipeline_rows = completed_pipeline_dashboard["rows"]
            self.assertEqual(3, len(completed_pipeline_rows), completed_pipeline_dashboard)
            summary_progress_event_ids = {int(record.get("event_id_hash")) for record in summary_progress}
            completed_pipeline_rows_by_event = {
                int(row.get("event_id_hash")): row
                for row in completed_pipeline_rows
                if int(row.get("event_id_hash")) in summary_progress_event_ids
            }
            self.assertEqual(summary_progress_event_ids, set(completed_pipeline_rows_by_event))
            self.assertTrue(all(row.get("status") == "summary_completed" for row in completed_pipeline_rows_by_event.values()))
            self.assertTrue(all(not row.get("summary_pending") for row in completed_pipeline_rows_by_event.values()))
            self.assertTrue(all(not row.get("compression_pending") for row in completed_pipeline_rows_by_event.values()))
            self.assertTrue(all(not row.get("embedding_pending") for row in completed_pipeline_rows_by_event.values()))
            self.assertTrue(all(not row.get("remaining_stages", []) for row in completed_pipeline_rows_by_event.values()))
            completed_pipeline_values = list(completed_pipeline_rows_by_event.values())
            self.assertTrue(any("tool" in row.get("source_roles", []) for row in completed_pipeline_values))
            self.assertTrue(any(row.get("source_role_counts", {}).get("tool", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(any("hook_boundary" in row.get("source_hook_types", []) for row in completed_pipeline_values))
            self.assertTrue(any(row.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(any("PostToolUse" in row.get("source_codex_events", []) for row in completed_pipeline_values))
            self.assertTrue(any(row.get("source_codex_event_counts", {}).get("PostToolUse", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(all("session" in row.get("source_memory_scopes", []) for row in completed_pipeline_values))
            self.assertTrue(all("user_profile" in row.get("source_memory_scopes", []) for row in completed_pipeline_values))
            self.assertTrue(all("same_session" in row.get("source_session_continuities", []) for row in completed_pipeline_values))
            self.assertTrue(all("cross_session" in row.get("source_session_continuities", []) for row in completed_pipeline_values))
            self.assertTrue(all(row.get("session_entities_written", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(all(row.get("profile_entities_written", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(all(row.get("same_session_entities_written", 0) >= 1 for row in completed_pipeline_values))
            self.assertTrue(all(row.get("cross_session_entities_written", 0) >= 1 for row in completed_pipeline_values))

            summary_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "later_async_summary_session"},
                    "session_scope": "prefer",
                    "question_type": "broad_exploration",
                    "query": "Summarize the user_profile cross_session PostToolUse hook_boundary async idle tool evidence memory.",
                    "max_context_tokens": 2000,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 8, "min_similarity_score": 0.0, "budget_fill_policy": "force_fill"},
                }
            )
            summary_refs = [ref for ref in summary_pack["selected_refs"] if ref.get("ref_type") == "summary"]
            self.assertTrue(summary_refs, summary_pack["selected_refs"])
            for ref in summary_refs:
                self.assertNotIn("source_roles", ref)
                self.assertNotIn("source_hook_types", ref)
                self.assertNotIn("source_codex_events", ref)
                self.assertNotIn("source_role_counts", ref)
                self.assertNotIn("source_hook_type_counts", ref)
                self.assertNotIn("source_codex_event_counts", ref)
            summary_budget = summary_pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(summary_budget["by_ref_type"]["summary"]["refs"], 1)
            self.assertGreaterEqual(summary_budget["by_memory_layer"]["profile_summary"]["refs"], 1)
            self.assertGreaterEqual(summary_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(summary_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            for field in [
                "by_source_role",
                "by_hook_type",
                "by_codex_event",
                "source_message_counts_by_role",
                "source_hook_counts_by_type",
                "source_codex_event_counts_by_event",
            ]:
                self.assertNotIn(field, summary_budget)
                self.assertNotIn(field, summary_pack["retrieval_metrics"]["memory_layer_pressure"].get("by_dimension", {}))
            self.assertNotIn(
                "tool_source_message_pressure",
                summary_pack["retrieval_metrics"]["memory_layer_pressure"],
            )
            summary_readiness = summary_pack["retrieval_metrics"]["async_pipeline_readiness"]
            self.assertEqual(3, summary_readiness["task_count"])
            self.assertEqual(3, summary_readiness["summary_completed_task_count"])
            self.assertTrue(summary_readiness["ready_for_retrieval"])
            self.assertEqual([], summary_readiness["remaining_stages"])
            self.assertEqual({}, summary_readiness["remaining_stage_counts"])
            self.assertNotIn("pending_source_roles", summary_readiness)
            self.assertNotIn("pending_source_hook_types", summary_readiness)
            self.assertNotIn("pending_source_codex_events", summary_readiness)
            self.assertNotIn("pending_memory_scopes", summary_readiness)
            self.assertNotIn("pending_session_continuities", summary_readiness)
            self.assertNotIn("pending_extraction_phases", summary_readiness)
            self.assertNotIn("pending_final_session_boundary_count", summary_readiness)
            summary_layer_readiness = summary_readiness["memory_layer_readiness"]
            self.assertTrue(summary_layer_readiness["ready_for_retrieval"])
            self.assertEqual([], summary_layer_readiness["blocked_layers"])
            self.assertTrue(summary_layer_readiness["layers"]["user_profile"]["ready"])
            self.assertTrue(summary_layer_readiness["layers"]["cross_session"]["ready"])
            self.assertTrue(summary_layer_readiness["layers"]["summary"]["ready"])
            self.assertEqual([], summary_readiness["freshness_warnings"])

    def test_lightweight_async_ingest_reports_idle_commit_as_auto_batch_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-async-idle-auto.jsonl")
            scope = {
                "account_id": "acct_async_idle",
                "tenant_id": "tenant_async_idle",
                "user_id": "user_async_idle",
                "session_id": "session_async_idle",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": True,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "assistant", "content": "Decision: idle commits should be visible as auto extraction."}],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                }
            )
            self.assertEqual("accepted", first["status"])
            self.assertIsNone(first["auto_batch_extract_result"])
            time.sleep(0.01)

            second = adapter.ingest(
                {
                    **base_args,
                    "idle_commit_timeout_ms": 1,
                    "messages": [{"role": "user", "content": "New prompt after idle should start a fresh batch."}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )

            idle_commit = second["idle_commit_result"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual("provisional", idle_commit["extraction_phase"])
            self.assertEqual(idle_commit, second["auto_batch_extract_result"])
            self.assertTrue(second["session_buffer"]["idle_ready"])
            self.assertFalse(second["session_buffer"]["threshold_ready"])
            self.assertEqual(1, idle_commit["committed_event_count"])
            self.assertGreaterEqual(idle_commit["memory_layers_written"]["session_entities"], 1)
            self.assertGreaterEqual(idle_commit["memory_layers_written"]["profile_entities"], 1)

    def test_lightweight_async_ingest_threshold_defaults_to_auto_batch_for_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-async-default-threshold.jsonl")
            scope = {
                "account_id": "acct_async_default",
                "tenant_id": "tenant_async_default",
                "user_id": "user_async_default",
                "session_id": "session_async_default",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "session_buffer_threshold": 2,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember: use Ubuntu /root/src/github-services for TemporalStore work.",
                        }
                    ],
                }
            )
            self.assertTrue(first["session_buffer"]["auto_batch_extract"])
            self.assertFalse(first["session_buffer"]["threshold_ready"])
            self.assertIsNone(first["auto_batch_extract_result"])

            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "assistant", "content": "Decision: default auto-batch should threshold commit."}],
                }
            )
            self.assertTrue(second["session_buffer"]["auto_batch_extract"])
            self.assertTrue(second["session_buffer"]["threshold_ready"])
            self.assertEqual("committed", second["auto_batch_extract_result"]["status"])
            self.assertEqual("threshold", second["auto_batch_extract_result"]["trigger_policy"])
            self.assertEqual(2, second["auto_batch_extract_result"]["committed_event_count"])
            self.assertFalse(adapter.pending_session_events(scope))

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertEqual(1, len(commits))
            self.assertEqual("threshold", commits[0]["trigger_policy"])
            self.assertEqual("provisional", commits[0]["extraction_phase"])
            self.assertGreaterEqual(second["auto_batch_extract_result"].get("entities_written", 0), 1)
            self.assertEqual(
                second["auto_batch_extract_result"].get("entities_written"),
                second["auto_batch_extract_result"].get("profile_entities_written"),
            )
            self.assertEqual(
                "always_when_profile_scope_available",
                second["auto_batch_extract_result"]["profile_promotion_policy"],
            )
            self.assertTrue(second["auto_batch_extract_result"]["profile_promotion_scope_available"])
            session_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "session"
                and record.get("session_continuity") == "same_session"
            ]
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(session_entities)
            self.assertTrue(profile_entities)
            self.assertTrue(
                any(
                    record.get("entity_type") == "assistant_decision"
                    and "threshold commit" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            self.assertTrue(
                any(
                    record.get("entity_type") == "preference"
                    and "/root/src/github-services" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "later_async_default_session"},
                    "session_scope": "prefer",
                    "query": "What did the assistant decide about the threshold commit?",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0, "budget_fill_policy": "force_fill"},
                }
            )
            selected_refs = pack["selected_refs"]
            self.assertLessEqual(pack["used_context_tokens"], 160)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "assistant_decision"
                    and ref.get("session_continuity") == "cross_session"
                    for ref in selected_refs
                ),
                selected_refs,
            )
            layer_budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(layer_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_memory_scope"]["user_profile"]["refs"], 1)

    def test_stop_boundary_force_commits_full_live_conversation_tail_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-stop-boundary.jsonl")
            scope = {
                "account_id": "acct_stop",
                "tenant_id": "tenant_stop",
                "user_id": "user_stop",
                "session_id": "session_stop_boundary",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": False,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            user_ingest = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Should memory wait for Stop or commit on thresholds?"}],
                }
            )
            assistant_ingest = adapter.ingest(
                {
                    **base_args,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": (
                                "Decision: threshold and idle commits are provisional checkpoints; "
                                "Stop force-drains the remaining conversation tail."
                            ),
                        }
                    ],
                }
            )
            self.assertEqual(1, user_ingest["session_buffer"]["pending_event_count"])
            self.assertEqual(2, assistant_ingest["session_buffer"]["pending_event_count"])

            pending_before_stop = adapter.pending_session_events(scope)
            stop = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": True,
                    "commit_reason": "hook_boundary",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", stop["status"])
            self.assertEqual("force", stop["trigger_policy"])
            self.assertEqual("final", stop["extraction_phase"])
            self.assertTrue(stop["final_session_boundary"])
            self.assertEqual(2, stop["committed_event_count"])
            self.assertEqual(
                [int(record["event_id_hash"]) for record in pending_before_stop],
                [int(event_id) for event_id in stop["source_event_ids"]],
            )
            self.assertFalse(adapter.pending_session_events(scope))

            second_stop = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": True,
                    "commit_reason": "hook_boundary",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("empty", second_stop["status"])
            self.assertTrue(second_stop["trigger_evidence"]["force"])
            self.assertFalse(second_stop["trigger_evidence"]["threshold_ready"])
            self.assertEqual(0, second_stop["trigger_evidence"]["pending_event_count"])

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertEqual(1, len(commits))
            self.assertEqual(2, commits[0]["committed_event_count"])
            self.assertEqual("final", commits[0]["extraction_phase"])
            self.assertTrue(commits[0]["final_session_boundary"])
            assistant_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("entity_type") == "assistant_decision"
            ]
            self.assertTrue(assistant_decisions)
            self.assertTrue(
                any("Stop force-drains" in str(record.get("state") or "") for record in assistant_decisions)
            )

    def test_compact_hot_prefix_preserves_boundary_session_commits(self) -> None:
        with mock.patch.object(matrixark_codex_hook, "HOOK_COMPACT_HOT_PREFIX_ONLY", True):
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("SessionIdle", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("Stop", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("UserPromptSubmit", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", "timeout"))

    def test_batch_extract_events_are_timestamp_keyed_under_segment_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-batch-segment-time.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_seg",
                        "tenant_id": "tenant_seg",
                        "user_id": "user_seg",
                        "session_id": "session_seg",
                    },
                    "messages": [
                        {"role": "user", "content": "Alice discussed recursion base cases."},
                        {"role": "assistant", "content": "Use a base case to stop recursion."},
                    ],
                    "force": True,
                }
            )
            self.assertGreaterEqual(result.get("segments_written", 0), 1)
            records = adapter.read_all()
            segments = [row for row in records if row.get("record_type") == "context_segment"]
            events = [row for row in records if row.get("record_type") == "context_event"]
            self.assertTrue(segments)
            self.assertTrue(events)
            segment_hashes = {row.get("segment_hash") for row in segments}
            self.assertTrue(all(row.get("context_event_parent_type") == "context_segment" for row in events))
            self.assertTrue(all(row.get("context_event_parent_hash") in segment_hashes for row in events))
            self.assertTrue(all(str(row.get("event_time_key", "")).startswith(str(row.get("event_time_ms", 0)).zfill(20)) for row in events))

    def test_batch_extract_promotes_session_entities_to_cross_session_profile_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-memory.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "session_codex_1",
                        "session_hash": 10101,
                        "scope_key": "acct_profile:tenant_profile:user_profile:session_codex_1",
                        "_explicit_scope_keys": ["account_id", "tenant_id", "user_id", "session_id"],
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "I prefer no external Codex hook logs outside TemporalStore. "
                                "We will batch user prompts and assistant decisions for extraction."
                            ),
                        },
                        {
                            "role": "assistant",
                            "content": "Done. Commit d0152479 pushed; the Rust hook daemon log is /dev/null.",
                        },
                        {
                            "role": "tool",
                            "content": (
                                "Exit code: 0\n"
                                "Ran 32 tests in 0.508s\n"
                                "OK\n"
                                "002fbd45034c69ce4487a64ab40d90135a55ae1a refs/heads/main"
                            ),
                        },
                    ],
                    "metadata": {
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                        "source_memory_selection_policy_counts": {
                            "selected_assistant_decision_outcome_only": 1,
                            "selected_tool_evidence_only": 1,
                        },
                    },
                    "force": True,
                }
            )

            self.assertGreaterEqual(result.get("entities_written", 0), 1)
            self.assertEqual(result.get("entities_written"), result.get("profile_entities_written"))
            self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
            self.assertTrue(result["profile_promotion_scope_available"])
            self.assertEqual("", result["profile_promotion_blocker"])
            promotion_summary = result["profile_promotion_summary"]
            self.assertEqual(result.get("profile_entities_written"), len(promotion_summary))
            self.assertTrue(all(item.get("source_session_ids") == ["session_codex_1"] for item in promotion_summary))
            self.assertTrue(all(item.get("profile_entity_hash") for item in promotion_summary))
            self.assertTrue(all(item.get("session_entity_hash") for item in promotion_summary))
            self.assertTrue(result["summary_refresh"]["session_dirty_hashes"])
            self.assertTrue(result["summary_refresh"]["profile_dirty_hashes"])
            self.assertTrue(result["summary_refresh"]["profile_summary_refresh_required"])
            self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, result["source_role_counts"])
            self.assertEqual({"hook_boundary": 3}, result["source_hook_type_counts"])
            self.assertEqual({"Stop": 3}, result["source_codex_event_counts"])
            records = adapter.read_all()
            durable_records = [
                record
                for record in records
                if record.get("record_type")
                in {"context_event", "context_entity", "context_segment", "context_summary", "context_extraction_audit"}
                and record.get("batch_id_hash") == result["batch_id_hash"]
            ]
            self.assertTrue(durable_records)
            for record in durable_records:
                self.assertIn("source_roles", record)
                self.assertIn("source_role_counts", record)
                self.assertIn("source_hook_types", record)
                self.assertIn("source_hook_type_counts", record)
                self.assertIn("source_codex_events", record)
                self.assertIn("source_codex_event_counts", record)
            batch_level_records = [
                record
                for record in durable_records
                if record.get("record_type") in {"context_entity", "context_segment", "context_summary", "context_extraction_audit"}
            ]
            self.assertTrue(batch_level_records)
            for record in batch_level_records:
                self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, record["source_role_counts"])
                self.assertEqual({"hook_boundary": 3}, record["source_hook_type_counts"])
                self.assertEqual({"Stop": 3}, record["source_codex_event_counts"])
            extraction_audits = [
                record for record in durable_records if record.get("record_type") == "context_extraction_audit"
            ]
            self.assertTrue(extraction_audits)
            self.assertEqual(
                "always_when_profile_scope_available",
                extraction_audits[0]["outputs"]["profile_promotion_policy"],
            )
            self.assertTrue(extraction_audits[0]["outputs"]["profile_promotion_scope_available"])
            self.assertEqual("", extraction_audits[0]["outputs"]["profile_promotion_blocker"])
            event_counts_by_role = {
                record["source_role"]: record["source_role_counts"]
                for record in durable_records
                if record.get("record_type") == "context_event"
            }
            self.assertEqual(
                {
                    "assistant": {"assistant": 1},
                    "tool": {"tool": 1},
                    "user": {"user": 1},
                },
                event_counts_by_role,
            )
            session_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "session"
                and record.get("session_continuity") == "same_session"
            ]
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(session_entities)
            self.assertEqual(len(session_entities), len(profile_entities))
            profile_entity = profile_entities[0]
            self.assertEqual(
                {"account_id": "acct_profile", "tenant_id": "tenant_profile", "user_id": "user_profile"},
                profile_entity["access_scope"],
            )
            for key in ["session_id", "session_hash", "scope_key", "_explicit_scope_keys"]:
                self.assertNotIn(key, profile_entity["access_scope"])
                if "scope" in profile_entity:
                    self.assertNotIn(key, profile_entity["scope"])
            self.assertEqual(["session_codex_1"], profile_entity["source_session_ids"])
            self.assertEqual("always_when_profile_scope_available", profile_entity["profile_promotion_policy"])
            self.assertEqual("", profile_entity["profile_promotion_blocker"])
            self.assertTrue(profile_entity["source_entity_hashes"])
            self.assertTrue(profile_entity["source_refs"])
            self.assertIn("assistant", profile_entity["source_roles"])
            self.assertIn("tool", profile_entity["source_roles"])
            self.assertIn("hook_boundary", profile_entity["source_hook_types"])
            self.assertIn("Stop", profile_entity["source_codex_events"])
            index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("memory_scope:user_profile", index_names)
            self.assertIn("session_continuity:cross_session", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("hook_type:hook_boundary", index_names)
            self.assertIn("codex_event:stop", index_names)
            self.assertTrue(any(str(name).startswith("entity_type:") for name in index_names))
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertTrue(
                any(
                    record.get("entity_type") == "assistant_decision"
                    and "Commit d0152479 pushed" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            self.assertTrue(
                any(
                    record.get("entity_type") == "tool_evidence"
                    and "Exit code: 0" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            retrieved = adapter.retrieval_records(
                scope={
                    "account_id": "acct_profile",
                    "tenant_id": "tenant_profile",
                    "user_id": "user_profile",
                    "session_id": "later_session",
                    "session_scope": "prefer",
                },
                record_types={"context_entity"},
            )
            retrieved_profile_entities = [
                record
                for record in retrieved["records"]
                if record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(retrieved_profile_entities)
            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "query": "What tool evidence proves the hook extraction fix was pushed?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            selected = pack["selected_refs"]
            self.assertLessEqual(pack["used_context_tokens"], 120)
            self.assertLessEqual(len(selected), 2)
            self.assertEqual(len(selected), pack["retrieval_metrics"]["selected_refs"])
            layer_budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertEqual(len(selected), layer_budget["total_selected_refs"])
            self.assertLessEqual(layer_budget["total_selected_tokens"], pack["retrieval_metrics"]["remote_context_budget_tokens"])
            self.assertGreaterEqual(layer_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_extraction_phase"]["final"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_profile_promotion_policy"]["always_when_profile_scope_available"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_entity_type"]["tool_evidence"]["refs"], 1)
            self.assertNotIn("by_source_role", layer_budget)
            self.assertNotIn("by_hook_type", layer_budget)
            self.assertGreaterEqual(layer_budget["final_session_boundary_ref_count"], 1)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("session_continuity") == "cross_session"
                    and "Exit code: 0" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in selected
                )
            )
            retrieved_tool_profile_ref = next(
                ref
                for ref in selected
                if ref.get("ref_type") == "entity"
                and ref.get("session_continuity") == "cross_session"
                and "Exit code: 0" in str(ref.get("text") or ref.get("summary_text") or "")
            )
            for key in ["session_id", "session_hash", "scope_key"]:
                self.assertNotIn(key, retrieved_tool_profile_ref)
            self.assertNotIn("source_session_ids", retrieved_tool_profile_ref)
            self.assertEqual("always_when_profile_scope_available", retrieved_tool_profile_ref["profile_promotion_policy"])
            self.assertNotIn("profile_promotion_blocker", retrieved_tool_profile_ref)
            decision_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "query": "What did the assistant decide was done?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            self.assertLessEqual(decision_pack["used_context_tokens"], 120)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "assistant_decision"
                    and "Commit d0152479 pushed" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in decision_pack["selected_refs"]
                )
            )
            retrieved_decision_profile_ref = next(
                ref
                for ref in decision_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and "Commit d0152479 pushed" in str(ref.get("text") or ref.get("summary_text") or "")
            )
            for key in ["session_id", "session_hash", "scope_key"]:
                self.assertNotIn(key, retrieved_decision_profile_ref)
            self.assertNotIn("source_session_ids", retrieved_decision_profile_ref)
            self.assertEqual("always_when_profile_scope_available", retrieved_decision_profile_ref["profile_promotion_policy"])
            self.assertNotIn("profile_promotion_blocker", retrieved_decision_profile_ref)
            session_only_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "only",
                    "query": "What tool evidence proves the hook extraction fix was pushed?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 4},
                }
            )
            session_only_refs = session_only_pack.get("selected_refs", [])
            self.assertFalse(
                any(ref.get("session_continuity") == "cross_session" for ref in session_only_refs),
                session_only_refs,
            )
            self.assertFalse(
                any(ref.get("memory_scope") == "user_profile" for ref in session_only_refs),
                session_only_refs,
            )
            session_only_metrics = session_only_pack.get("retrieval_metrics", {})
            session_only_budget = session_only_metrics.get("memory_layer_budget", {})
            self.assertEqual(0, session_only_budget.get("by_session_continuity", {}).get("cross_session", {}).get("refs", 0))
            self.assertEqual(0, session_only_budget.get("by_memory_scope", {}).get("user_profile", {}).get("refs", 0))
            session_only_cross_policy = session_only_pack.get("recall_policy", {}).get("cross_session", {})
            if session_only_cross_policy:
                self.assertEqual(0, session_only_cross_policy["budget_tokens"])
                self.assertFalse(session_only_cross_policy["enabled"])
            profile_dirty_records = [
                record
                for record in records
                if record.get("record_type") == "context_summary_dirty"
                and record.get("dirty_reason") == "profile_entity_promoted"
            ]
            self.assertTrue(profile_dirty_records)
            self.assertTrue(all(record.get("memory_scope") == "user_profile" for record in profile_dirty_records))
            self.assertTrue(all(record.get("session_continuity") == "cross_session" for record in profile_dirty_records))
            self.assertTrue(all("user_profile" in record.get("source_memory_scopes", []) for record in profile_dirty_records))
            self.assertTrue(all("cross_session" in record.get("source_session_continuities", []) for record in profile_dirty_records))
            self.assertTrue(
                all(
                    "always_when_profile_scope_available" in record.get("source_profile_promotion_policies", [])
                    for record in profile_dirty_records
                )
            )
            self.assertTrue(all(not record.get("source_profile_promotion_blockers") for record in profile_dirty_records))
            self.assertTrue(any(record.get("source_role_counts", {}).get("assistant", 0) >= 1 for record in profile_dirty_records))
            self.assertTrue(any(record.get("source_role_counts", {}).get("tool", 0) >= 1 for record in profile_dirty_records))
            self.assertTrue(any(record.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for record in profile_dirty_records))
            self.assertTrue(any(record.get("source_codex_event_counts", {}).get("Stop", 0) >= 1 for record in profile_dirty_records))
            session_dirty_records = [
                record
                for record in records
                if record.get("record_type") == "context_summary_dirty"
                and record.get("dirty_reason") == "new_event"
                and record.get("source_ref_type") == "batch"
                and record.get("source_batch_hash") == result["batch_id_hash"]
            ]
            self.assertTrue(session_dirty_records)
            self.assertTrue(all(record.get("memory_scope") == "session" for record in session_dirty_records))
            self.assertTrue(all(record.get("session_continuity") == "same_session" for record in session_dirty_records))
            self.assertTrue(all(record.get("source_role_counts") == {"assistant": 1, "tool": 1, "user": 1} for record in session_dirty_records))
            self.assertTrue(all(record.get("source_hook_type_counts") == {"hook_boundary": 3} for record in session_dirty_records))
            self.assertTrue(all(record.get("source_codex_event_counts") == {"Stop": 3} for record in session_dirty_records))
            self.assertFalse(
                any(
                    record.get("record_type") == "context_summary"
                    and record.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
                    for record in records
                )
            )
            refresh = adapter.refresh_summaries(
                {
                    "scope": {"account_id": "acct_profile", "tenant_id": "tenant_profile", "user_id": "user_profile"},
                    "limit": 16,
                    "refreshed_at_ms": 1234567890,
                }
            )
            self.assertGreaterEqual(refresh.get("refreshed_count", 0), 1)
            refreshed_profile = next(
                item
                for item in refresh["refreshed"]
                if item.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
            )
            self.assertIn("assistant", refreshed_profile["source_roles"])
            self.assertIn("tool", refreshed_profile["source_roles"])
            self.assertIn("hook_boundary", refreshed_profile["source_hook_types"])
            self.assertIn("Stop", refreshed_profile["source_codex_events"])
            self.assertIn("user_profile", refreshed_profile["source_memory_scopes"])
            self.assertIn("cross_session", refreshed_profile["source_session_continuities"])
            self.assertIn("final", refreshed_profile["source_extraction_phases"])
            self.assertIn("always_when_profile_scope_available", refreshed_profile["source_profile_promotion_policies"])
            self.assertEqual([], refreshed_profile["source_profile_promotion_blockers"])
            self.assertGreaterEqual(refreshed_profile["source_final_session_boundary_count"], 1)
            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries)
            self.assertTrue(any(record.get("summary_type") == "node_l0" for record in profile_summaries))
            self.assertTrue(any("assistant" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any("tool" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_role_counts", {}).get("assistant", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any(record.get("source_role_counts", {}).get("tool", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any("hook_boundary" in record.get("source_hook_types", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_hook_type_counts", {}).get("hook_boundary", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any("Stop" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_codex_event_counts", {}).get("Stop", 0) >= 1 for record in profile_summaries))
            self.assertTrue(any("user_profile" in record.get("source_memory_scopes", []) for record in profile_summaries))
            self.assertTrue(any("cross_session" in record.get("source_session_continuities", []) for record in profile_summaries))
            self.assertTrue(any("final" in record.get("source_extraction_phases", []) for record in profile_summaries))
            self.assertTrue(
                any(
                    "always_when_profile_scope_available" in record.get("source_profile_promotion_policies", [])
                    for record in profile_summaries
                )
            )
            self.assertTrue(all(not record.get("source_profile_promotion_blockers") for record in profile_summaries))
            self.assertTrue(any(record.get("source_final_session_boundary_count", 0) >= 1 for record in profile_summaries))
            summary_index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_summary"
                and record.get("ref_type") == "summary"
                and record.get("node_hash") in {summary.get("node_hash") for summary in profile_summaries}
            }
            self.assertIn("summary_type:node_l0", summary_index_names)
            self.assertIn("source_role:assistant", summary_index_names)
            self.assertIn("source_role:tool", summary_index_names)
            self.assertIn("hook_type:hook_boundary", summary_index_names)
            self.assertIn("codex_event:stop", summary_index_names)
            self.assertIn("memory_scope:user_profile", summary_index_names)
            self.assertIn("session_continuity:cross_session", summary_index_names)
            self.assertIn("extraction_phase:final", summary_index_names)
            self.assertIn("profile_promotion_policy:always_when_profile_scope_available", summary_index_names)
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", summary_index_names)
            self.assertIn("memory_selection_policy:selected_tool_evidence_only", summary_index_names)
            self.assertIn("entity_type:assistant_decision", summary_index_names)
            self.assertIn("entity_type:tool_evidence", summary_index_names)
            profile_entity_hashes_by_type = {
                record.get("entity_type"): record.get("entity_hash")
                for record in profile_entities
                if record.get("entity_hash") is not None
            }
            self.assertTrue(
                any(
                    profile_entity_hashes_by_type.get("assistant_decision") in record.get("source_entity_hashes", [])
                    and profile_entity_hashes_by_type.get("tool_evidence") in record.get("source_entity_hashes", [])
                    and "assistant_decision" in record.get("source_entity_types", [])
                    and "tool_evidence" in record.get("source_entity_types", [])
                    for record in profile_summaries
                )
            )
            current_state_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the current tool evidence for the hook extraction fix?",
                    "max_context_tokens": 90,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {"max_selected_refs": 2},
                }
            )
            current_refs = current_state_pack["selected_refs"]
            self.assertLessEqual(current_state_pack["used_context_tokens"], 90)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "tool_evidence"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "Exit code: 0" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in current_refs
                ),
                current_refs,
            )
            self.assertFalse(any(ref.get("ref_type") == "summary" for ref in current_refs), current_refs)
            current_budget = current_state_pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(current_budget["by_entity_type"]["tool_evidence"]["refs"], 1)
            self.assertNotIn("summary", current_budget["by_ref_type"])
            summary_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "question_type": "broad_exploration",
                    "query": (
                        "user_profile cross_session profile long_term_memory "
                        "assistant_decision tool_evidence selected_tool_evidence_only always_when_profile_scope_available"
                    ),
                    "max_context_tokens": 200,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 5},
                }
            )
            self.assertLessEqual(summary_pack["used_context_tokens"], 200)
            selected_summary_refs = [ref for ref in summary_pack["selected_refs"] if ref.get("ref_type") == "summary"]
            self.assertTrue(selected_summary_refs, summary_pack["selected_refs"])
            self.assertTrue(
                any(
                    "selected_tool_evidence_only" in ref.get("source_memory_selection_policies", [])
                    and "always_when_profile_scope_available" in ref.get("source_profile_promotion_policies", [])
                    for ref in selected_summary_refs
                ),
                selected_summary_refs,
            )
            self.assertTrue(
                any(
                    "assistant_decision" in ref.get("source_entity_types", [])
                    and "tool_evidence" in ref.get("source_entity_types", [])
                    for ref in selected_summary_refs
                ),
                selected_summary_refs,
            )
            summary_layer_budget = summary_pack["retrieval_metrics"]["memory_layer_budget"]
            summary_debug_layer_budget = summary_pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(summary_layer_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_extraction_phase"]["final"]["refs"], 1)
            self.assertGreaterEqual(
                summary_layer_budget["by_profile_promotion_policy"]["always_when_profile_scope_available"]["refs"],
                1,
            )
            self.assertGreaterEqual(
                summary_debug_layer_budget["by_memory_selection_policy"]["selected_tool_evidence_only"]["refs"],
                1,
            )
            self.assertGreaterEqual(summary_layer_budget["by_entity_type"]["assistant_decision"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_entity_type"]["tool_evidence"]["refs"], 1)
            for field in [
                "by_source_role",
                "by_hook_type",
                "by_codex_event",
                "source_message_counts_by_role",
                "source_hook_counts_by_type",
                "source_codex_event_counts_by_event",
            ]:
                self.assertNotIn(field, summary_layer_budget)
            self.assertGreaterEqual(summary_layer_budget["final_session_boundary_ref_count"], 1)
            serving_summary_pack = compact_context_pack_for_serving(summary_pack)
            served_items = [
                item
                for group in serving_summary_pack.get("groups", [])
                for item in group.get("items", [])
            ]
            self.assertTrue(served_items, serving_summary_pack)
            self.assertTrue(any(item.get("memory_scope") == "user_profile" for item in served_items))
            self.assertTrue(any(item.get("final_session_boundary") is True for item in served_items))
            summary_serving_pack = compact_context_pack_for_serving(
                {
                    "context_pack_id": "profile-summary-lineage",
                    "selected_refs": [
                        {
                            "ref_type": "summary",
                            "context_class": "summary",
                            "text": record.get("summary_text", ""),
                            "summary_type": record.get("summary_type"),
                            "memory_scope": record.get("memory_scope"),
                            "session_continuity": record.get("session_continuity"),
                            "extraction_phase": record.get("extraction_phase"),
                            "final_session_boundary": record.get("final_session_boundary"),
                            "source_memory_scopes": record.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities", []),
                            "source_extraction_phases": record.get("source_extraction_phases", []),
                            "source_entity_types": record.get("source_entity_types", []),
                            "source_profile_promotion_policies": record.get("source_profile_promotion_policies", []),
                            "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers", []),
                            "source_final_session_boundary_count": record.get("source_final_session_boundary_count", 0),
                            "source_roles": record.get("source_roles", []),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_codex_events": record.get("source_codex_events", []),
                        }
                        for record in profile_summaries
                    ],
                }
            )
            summary_items = [
                item
                for group in summary_serving_pack.get("groups", [])
                if group.get("type") == "summary"
                for item in group.get("items", [])
            ]
            self.assertTrue(any(item.get("memory_scope") == "user_profile" for item in summary_items))
            for item in summary_items:
                self.assertNotIn("source_memory_scopes", item)
                self.assertNotIn("source_session_continuities", item)
                self.assertNotIn("source_extraction_phases", item)
                self.assertNotIn("source_entity_types", item)
                self.assertNotIn("source_profile_promotion_policies", item)
                self.assertNotIn("source_profile_promotion_blockers", item)
                self.assertNotIn("source_roles", item)
                self.assertNotIn("source_hook_types", item)
                self.assertNotIn("source_codex_events", item)
                self.assertNotIn("source_memory_selection_policies", item)
                self.assertNotIn("source_memory_selection_policy_counts", item)
            self.assertTrue(any(item.get("final_session_boundary") is True for item in summary_items))

    def test_batch_extract_reports_profile_scope_missing_without_importance_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-missing.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "session_id": "session_without_user_profile",
                        "session_hash": 20202,
                        "scope_key": "acct_profile:tenant_profile:session_without_user_profile",
                        "_explicit_scope_keys": ["account_id", "tenant_id", "session_id"],
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": "Always keep Codex hook memories inside TemporalStore.",
                        }
                    ],
                    "force": True,
                }
            )

            self.assertGreaterEqual(result.get("entities_written", 0), 1)
            self.assertEqual(0, result.get("profile_entities_written"))
            self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
            self.assertFalse(result["profile_promotion_scope_available"])
            self.assertEqual("profile_scope_missing", result["profile_promotion_blocker"])
            self.assertFalse(result["summary_refresh"]["profile_summary_refresh_required"])
            records = adapter.read_all()
            audits = [
                record
                for record in records
                if record.get("record_type") == "context_extraction_audit"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            ]
            self.assertTrue(audits)
            self.assertEqual("profile_scope_missing", audits[0]["outputs"]["profile_promotion_blocker"])
            self.assertFalse(
                any(
                    record.get("record_type") == "context_entity"
                    and record.get("memory_scope") == "user_profile"
                    for record in records
                )
            )

    def test_batch_extract_normalizes_llm_response_aliases_to_assistant_profile_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-llm-aliases.jsonl")
            scope = {
                "account_id": "acct_profile_alias",
                "tenant_id": "tenant_profile_alias",
                "user_id": "user_profile_alias",
                "session_id": "session_llm_alias_1",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {"role": "user", "content": "Remember the LLM response alias memory behavior."},
                        {
                            "role": "llm",
                            "content": "Decision: commit abc123 after LLM alias extraction passes.",
                        },
                        {
                            "role": "model",
                            "content": "Done. Use assistant budget controls for model alias response def456.",
                        },
                    ],
                    "metadata": {"source_roles": ["llm", "model"], "hook_type": "hook_boundary"},
                    "force": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual({"assistant": 2, "user": 1}, result["source_role_counts"])
            records = adapter.read_all()
            events_by_original_role = {
                record.get("original_source_role"): record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("source_role") == "assistant"
                and record.get("original_source_role") in {"llm", "model"}
            }
            self.assertEqual({"llm", "model"}, set(events_by_original_role))
            self.assertTrue(all(record["source_role_counts"] == {"assistant": 1} for record in events_by_original_role.values()))

            profile_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "assistant_decision"
            ]
            self.assertTrue(profile_decisions)
            profile_decision = profile_decisions[0]
            self.assertEqual(["assistant", "user"], profile_decision["source_roles"])
            self.assertEqual({"assistant": 2, "user": 1}, profile_decision["source_role_counts"])
            self.assertIn("abc123", profile_decision["state"])
            self.assertIn("def456", profile_decision["state"])

            index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("source_role:assistant", index_names)
            self.assertNotIn("source_role:llm", index_names)
            self.assertNotIn("source_role:model", index_names)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_llm_alias_2"},
                    "session_scope": "prefer",
                    "query": "abc123 def456 assistant decision",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "source_role_budget_tokens": {"llm": 500},
                    "ranking": {"max_selected_refs": 5, "min_similarity_score": 0.0},
                }
            )
            selected_decision = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and ref.get("memory_scope") == "user_profile"
            )
            self.assertNotIn("budget_source_roles", selected_decision)
            self.assertNotIn("budget_source_role_counts", selected_decision)

    def test_profile_entity_updates_preserve_cross_session_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark-profile-entity-update.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            base_scope = {
                "account_id": "acct_profile_update",
                "tenant_id": "tenant_profile_update",
                "user_id": "user_profile_update",
            }
            first = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_1"},
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Done. Commit aaa111 pushed and profile memory update tests passed.",
                        }
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )
            second = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_2"},
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Done. Commit bbb222 pushed and cross-session lineage was preserved.",
                        }
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )

            self.assertGreaterEqual(first.get("profile_entities_written", 0), 1)
            self.assertGreaterEqual(second.get("profile_entities_written", 0), 1)
            records = adapter.read_all()
            profile_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "assistant_decision"
            ]
            self.assertEqual(1, len(profile_decisions))
            profile_decision = profile_decisions[0]
            self.assertEqual(
                ["session_profile_update_1", "session_profile_update_2"],
                profile_decision["source_session_ids"],
            )
            self.assertGreaterEqual(len(profile_decision["source_entity_hashes"]), 2)
            self.assertIn("assistant", profile_decision["source_roles"])
            self.assertEqual({"assistant": 2}, profile_decision["source_role_counts"])
            self.assertIn("hook_boundary", profile_decision["source_hook_types"])
            self.assertEqual({"hook_boundary": 2}, profile_decision["source_hook_type_counts"])
            self.assertIn("Stop", profile_decision["source_codex_events"])
            self.assertEqual({"Stop": 2}, profile_decision["source_codex_event_counts"])
            self.assertIn("aaa111", profile_decision["state"])
            self.assertIn("bbb222", profile_decision["state"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_3"},
                    "session_scope": "prefer",
                    "query": "aaa111 bbb222 assistant",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 10},
                }
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "bbb222" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in pack["selected_refs"]
                )
            )
            reopened = MatrixArkLocalAdapter(event_log)
            recovered_pack = reopened.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_3"},
                    "session_scope": "prefer",
                    "query": "aaa111 bbb222 assistant",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 10},
                }
            )
            recovered_profile_refs = [
                ref
                for ref in recovered_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
                and ref.get("entity_type") == "assistant_decision"
            ]
            self.assertTrue(recovered_profile_refs, recovered_pack["selected_refs"])
            recovered_decision = recovered_profile_refs[0]
            self.assertNotIn("source_session_ids", recovered_decision)
            self.assertNotIn("source_role_counts", recovered_decision)
            self.assertNotIn("source_hook_type_counts", recovered_decision)
            self.assertNotIn("source_codex_event_counts", recovered_decision)
            self.assertIn("bbb222", recovered_decision["text"])
            self.assertNotIn("memory_layer_budget", recovered_pack)
            recovered_budget = recovered_pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(recovered_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(recovered_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertNotIn("source_message_counts_by_role", recovered_budget)
            self.assertNotIn("source_hook_counts_by_type", recovered_budget)
            self.assertNotIn("source_codex_event_counts_by_event", recovered_budget)

            current_pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_2"},
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the latest assistant decision for bbb222?",
                    "max_context_tokens": 500,
                    "audit_mode": "telemetry_only",
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                }
            )
            self.assertEqual(1, len(current_pack["selected_refs"]))
            current_ref = current_pack["selected_refs"][0]
            self.assertEqual("entity", current_ref["ref_type"])
            self.assertEqual("assistant_decision", current_ref["entity_type"])
            self.assertEqual("user_profile", current_ref["memory_scope"])
            self.assertEqual("cross_session", current_ref["session_continuity"])
            self.assertEqual(
                ["session_profile_update_1", "session_profile_update_2"],
                current_ref["source_session_ids"],
            )
            self.assertEqual({"assistant": 2}, current_ref["source_role_counts"])
            self.assertEqual({"hook_boundary": 2}, current_ref["source_hook_type_counts"])
            self.assertEqual({"Stop": 2}, current_ref["source_codex_event_counts"])
            self.assertTrue(current_ref["profile_current_state_representative"])
            self.assertEqual(
                "profile_entity_bridge_preferred_over_session_local_history",
                current_ref["current_state_policy"],
            )
            self.assertEqual(2, current_ref["current_state_source_session_count"])
            self.assertGreaterEqual(current_ref["current_state_source_entity_count"], 2)
            self.assertGreaterEqual(current_ref["source_entity_count"], 2)
            self.assertIn("bbb222", current_ref["text"])
            current_metrics = current_pack["retrieval_metrics"]
            self.assertGreaterEqual(current_metrics["stale_dropped_refs"], 1)
            self.assertEqual(
                current_metrics["stale_dropped_refs"],
                current_metrics["dropped_ref_bucket_counts"]["stale"],
            )
            self.assertIn("dropped_memory_layer_budget", current_pack)
            debug_dropped_budget = current_pack["dropped_memory_layer_budget"]
            self.assertGreaterEqual(debug_dropped_budget["by_source_role"]["assistant"]["refs"], 1)
            self.assertGreaterEqual(debug_dropped_budget["source_message_counts_by_role"]["assistant"], 1)
            self.assertGreaterEqual(debug_dropped_budget["source_hook_counts_by_type"]["hook_boundary"], 1)
            self.assertGreaterEqual(debug_dropped_budget["source_codex_event_counts_by_event"]["Stop"], 1)
            dropped_budget = current_metrics["dropped_memory_layer_budget"]
            self.assertGreaterEqual(dropped_budget["total_dropped_refs"], 1)
            self.assertGreaterEqual(dropped_budget["by_memory_scope"]["session"]["refs"], 1)
            self.assertGreaterEqual(dropped_budget["by_session_continuity"]["same_session"]["refs"], 1)
            self.assertGreaterEqual(dropped_budget["by_ref_type"]["entity"]["refs"], 1)
            self.assertGreaterEqual(dropped_budget["by_entity_type"]["assistant_decision"]["refs"], 1)
            self.assertGreaterEqual(dropped_budget["by_extraction_phase"]["final"]["refs"], 1)
            self.assertGreaterEqual(dropped_budget["final_ref_count"], 1)
            self.assertNotIn("by_source_role", dropped_budget)
            self.assertNotIn("source_message_counts_by_role", dropped_budget)
            self.assertNotIn("by_hook_type", dropped_budget)
            self.assertNotIn("source_hook_counts_by_type", dropped_budget)
            self.assertNotIn("by_codex_event", dropped_budget)
            self.assertNotIn("source_codex_event_counts_by_event", dropped_budget)
            self.assertGreaterEqual(dropped_budget["profile_shadowed_ref_count"], 1)
            self.assertNotIn("source_entity_lineage", dropped_budget.get("by_profile_shadowed_reason", {}))
            self.assertEqual(dropped_budget, current_metrics["dropped_memory_layer_budget"])
            layer_pressure = current_metrics["memory_layer_pressure"]
            self.assertIn("memory_layer_pressure", current_pack)
            debug_layer_pressure = current_pack["memory_layer_pressure"]
            self.assertIn("assistant_memory_pressure", debug_layer_pressure)
            self.assertIn("assistant_source_message_pressure", debug_layer_pressure)
            self.assertGreaterEqual(layer_pressure["dropped_refs"], 1)
            self.assertTrue(layer_pressure["session_memory_pressure"])
            self.assertTrue(layer_pressure["same_session_pressure"])
            self.assertTrue(layer_pressure["final_memory_pressure"])
            self.assertTrue(layer_pressure["stale_current_state_pressure"])
            self.assertTrue(layer_pressure["profile_shadowed_current_state_pressure"])
            self.assertNotIn("assistant_memory_pressure", layer_pressure)
            self.assertNotIn("assistant_source_message_pressure", layer_pressure)
            self.assertNotIn("by_profile_shadowed_reason", layer_pressure["dropped_dimensions"])
            self.assertIn("by_extraction_phase", layer_pressure["dropped_dimensions"])
            self.assertNotIn("source_message_counts_by_role", layer_pressure["dropped_dimensions"])
            self.assertNotIn(
                "source_entity_lineage",
                layer_pressure["by_dimension"].get("by_profile_shadowed_reason", {}),
            )
            self.assertGreaterEqual(layer_pressure["by_dimension"]["by_extraction_phase"]["final"]["dropped_refs"], 1)
            self.assertNotIn("source_message_counts_by_role", layer_pressure["by_dimension"])
            current_dashboard = adapter.ingestion_dashboard(
                {"scope": {**base_scope, "session_id": "session_profile_update_2"}, "table": "context_packs", "page_size": 5}
            )
            current_pack_id = current_pack.get("context_pack_id") or current_pack.get("pack_id")
            current_rows = [
                row
                for row in current_dashboard["rows"]
                if row.get("context_pack_id") == current_pack_id
            ]
            self.assertTrue(current_rows, current_dashboard)
            current_row = current_rows[-1]
            self.assertEqual(current_metrics["stale_dropped_refs"], current_row["stale_dropped_refs"])
            self.assertEqual(current_metrics["dropped_ref_bucket_counts"]["stale"], current_row["dropped_ref_bucket_counts"]["stale"])
            self.assertGreaterEqual(current_row["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertEqual(
                dropped_budget["by_memory_scope"],
                current_row["dropped_memory_layer_budget"]["by_memory_scope"],
            )
            self.assertGreaterEqual(
                current_row["dropped_memory_layer_budget"]["by_source_role"]["assistant"]["refs"],
                1,
            )
            self.assertEqual(layer_pressure["dropped_refs"], current_row["memory_layer_pressure"]["dropped_refs"])
            self.assertIn("assistant_memory_pressure", current_row["memory_layer_pressure"])
            self.assertLessEqual(current_row["used_remote_context_tokens"], current_row["remote_context_budget_tokens"])

            default_current_pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_2"},
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the latest assistant decision for bbb222?",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                }
            )
            self.assertEqual(1, len(default_current_pack["selected_refs"]))
            default_current_ref = default_current_pack["selected_refs"][0]
            self.assertEqual("user_profile", default_current_ref["memory_scope"])
            self.assertEqual("cross_session", default_current_ref["session_continuity"])
            self.assertNotIn("source_session_ids", default_current_ref)
            self.assertNotIn("source_role_counts", default_current_ref)
            self.assertNotIn("source_hook_type_counts", default_current_ref)
            self.assertNotIn("source_codex_event_counts", default_current_ref)
            self.assertNotIn("source_entity_count", default_current_ref)
            self.assertNotIn("current_state_policy", default_current_ref)
            self.assertNotIn("current_state_source_session_count", default_current_ref)
            self.assertNotIn("current_state_source_entity_count", default_current_ref)
            self.assertNotIn("dropped_memory_layer_budget", default_current_pack)
            self.assertNotIn("memory_layer_pressure", default_current_pack)

    def test_retrieval_flags_state_file_session_identity_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark-session-identity-warning.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_session_identity",
                "tenant_id": "tenant_session_identity",
                "user_id": "user_session_identity",
                "session_id": "codex:local:fallback",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "What should retrieval report when Codex session identity is workspace fallback?",
                    "max_context_tokens": 120,
                    "audit_mode": "telemetry_only",
                    "include_retrieval_debug": True,
                    "metadata": {
                        "retrieval_source": "codex_hook_retrieve",
                        "codex_event": "UserPromptSubmit",
                        "lifecycle_stage": "before_llm_retrieve",
                        "session_id_source": "state_file_created",
                    },
                }
            )

            self.assertIn("session_identity_fallback:state_file_created", pack["quality_warnings"])
            identity_policy = pack["recall_policy"]["session_identity"]
            self.assertEqual("state_file_created", identity_policy["session_id_source"])
            self.assertTrue(identity_policy["fallback_session_identity"])
            self.assertFalse(identity_policy["strong_session_identity"])
            self.assertEqual("workspace_fallback_may_merge_multiple_codex_tasks", identity_policy["risk"])

            telemetry_rows = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_pack_telemetry"
                and record.get("context_pack_id") == pack["context_pack_id"]
            ]
            self.assertTrue(telemetry_rows)
            telemetry = telemetry_rows[-1]
            self.assertEqual(1, telemetry["quality_warning_count"])
            self.assertIn("session_identity_fallback:state_file_created", telemetry["quality_warnings"])
            self.assertEqual("state_file_created", telemetry["retrieval_request_metadata"]["session_id_source"])

            dashboard = adapter.ingestion_dashboard({"scope": scope, "table": "context_packs", "page_size": 5})
            rows = [row for row in dashboard["rows"] if row.get("context_pack_id") == pack["context_pack_id"]]
            self.assertTrue(rows)
            self.assertIn("session_identity_fallback:state_file_created", rows[-1]["quality_warnings"])

    def test_async_resource_import_uses_bounded_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, mock.patch.dict(
            os.environ,
            {
                "MATRIXARK_RESOURCE_IMPORT_WORKERS": "1",
                "MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX": "2",
            },
        ):
            event_log = Path(tmp_dir) / "matrixark-resource-queue.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            server = MatrixArkMcpServer(adapter, line_json=True, access_mode="dev")
            result = server.call_tool(
                "matrixark_ingest",
                {
                    "kind": "resource",
                    "raw_uri": "inline://resource-queue.md",
                    "resource_type": "md",
                    "wait": False,
                    "messages": [
                        {
                            "role": "user",
                            "content": "# GPU Runbook\n\nAlice owns the GPU approval checklist. Finance review is required before purchase.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_queue",
                        "tenant_id": "tenant_queue",
                        "user_id": "user_queue",
                        "session_id": "session_queue",
                    },
                    "metadata": {"node_path": ["users", "user_queue", "resources", "runbooks"]},
                },
            )
            self.assertEqual("queued", result["status"])
            task = result["resource_import_task"]
            self.assertFalse(task["wait"])
            self.assertTrue(task["worker_pool"]["bounded"])
            self.assertEqual(1, task["worker_pool"]["worker_count"])
            self.assertEqual(2, task["worker_pool"]["queue_max"])

            task_hash = task["task_hash"]
            deadline = time.time() + 5.0
            records: list[dict] = []
            while time.time() < deadline:
                records = adapter.read_all()
                if any(
                    record.get("record_type") == "resource_import_task"
                    and record.get("task_hash") == task_hash
                    and record.get("status") == "completed"
                    for record in records
                ):
                    break
                time.sleep(0.05)
            else:
                self.fail("background resource import did not complete")

            self.assertTrue(any(record.get("record_type") == "resource_chunk" for record in records))
            self.assertTrue(any(record.get("record_type") == "context_embedding" for record in records))
            self.assertTrue(any(record.get("record_type") == "context_summary" for record in records))

    def test_tenant_shared_resource_and_skill_live_outside_session_and_retrieve_with_quota(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-shared-context.jsonl"
            resource = tmp / "tenant_policy.md"
            resource.write_text(
                "# Tenant GPU Policy\n\n"
                "Decision: shared GPU purchases require finance approval.\n"
                "Owner: Alice finance.\n",
                encoding="utf-8",
            )
            skill = tmp / "SKILL.md"
            skill.write_text(
                "---\n"
                "name: shared-context-debugger\n"
                "description: Inspect MatrixArk replay and selected refs.\n"
                "triggers:\n  - replay\n"
                "allowed_tools:\n  - matrixark_replay\n"
                "status: active\n"
                "---\n\n"
                "# Shared Context Debugger\n\nUse this skill to inspect replay evidence.\n",
                encoding="utf-8",
            )
            adapter = MatrixArkLocalAdapter(event_log)
            shared_scope = {"account_id": "acct_shared", "tenant_id": "tenant_shared"}
            adapter.ingest(
                {
                    "kind": "resource",
                    "raw_uri": str(resource),
                    "resource_type": "md",
                    "scope": shared_scope,
                    "messages": [{"role": "user", "content": "Import tenant shared resource."}],
                    "wait": True,
                }
            )
            adapter.ingest(
                {
                    "kind": "skill",
                    "raw_uri": str(skill),
                    "resource_type": "skill",
                    "scope": shared_scope,
                    "messages": [{"role": "user", "content": "Import tenant shared skill."}],
                    "wait": True,
                }
            )
            records = adapter.read_all()
            resource_chunks = [row for row in records if row.get("record_type") == "resource_chunk" and row.get("resource_type") != "skill"]
            skill_sections = [row for row in records if row.get("record_type") == "skill_section"]
            self.assertTrue(resource_chunks)
            self.assertTrue(skill_sections)
            resource_manifests = [row for row in records if row.get("record_type") == "resource_manifest"]
            skill_manifests = [row for row in records if row.get("record_type") == "skill_manifest"]
            self.assertTrue(any(row.get("node_path")[:3] == ["tenant:tenant_shared", "shared", "resources"] for row in resource_manifests))
            self.assertTrue(any(row.get("node_path")[:3] == ["tenant:tenant_shared", "shared", "skills"] for row in skill_manifests))
            self.assertTrue(all(row.get("access_scope", {}).get("sharing_scope") == "tenant_shared" for row in resource_chunks + skill_sections))

            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_shared",
                        "tenant_id": "tenant_shared",
                        "user_id": "user_123",
                        "session_id": "active_session",
                    },
                    "query": "Which shared policy requires finance approval and which skill inspects replay evidence?",
                    "max_context_tokens": 1600,
                    "shared_context": {"resource_budget_tokens": 120, "skill_budget_tokens": 120},
                    "audit_mode": "off",
                    "include_retrieval_debug": True,
                }
            )
            ref_types = {ref.get("ref_type") for ref in pack["selected_refs"]}
            self.assertIn("resource_chunk", ref_types)
            self.assertIn("skill_section", ref_types)
            self.assertEqual("tenant_shared", next(ref for ref in pack["selected_refs"] if ref.get("ref_type") == "resource_chunk")["sharing_scope"])
            shared_policy = pack["recall_policy"]["shared_context"]
            self.assertGreaterEqual(shared_policy["resource_selected_ref_count"], 1)
            self.assertGreaterEqual(shared_policy["skill_selected_ref_count"], 1)

    def test_codex_hook_ingests_conversation_resource_and_skill_then_retrieves_all(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-codex-hook.jsonl"
            resource = tmp / "gpu_policy.md"
            resource.write_text(
                "# GPU Approval Policy\n\n"
                "Decision: GPU budget requests require finance approval before purchase.\n"
                "Owner: Alice from finance.\n"
                "Deadline: requests must be reviewed within 3 business days.\n",
                encoding="utf-8",
            )
            skill = tmp / "SKILL.md"
            skill.write_text(
                "---\n"
                "name: context-debugger\n"
                "description: Inspect MatrixArk selected refs, dropped refs, and replay evidence.\n"
                "triggers:\n"
                "  - contextpack\n"
                "  - replay evidence\n"
                "allowed_tools:\n"
                "  - matrixark_replay\n"
                "status: active\n"
                "version: '1'\n"
                "---\n\n"
                "# Context Debugger\n\n"
                "Use this skill to inspect selected ContextPack refs and replay evidence.\n",
                encoding="utf-8",
            )

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "Alice asked Codex to verify GPU budget approval workflow.",
                    "thread_id": "codex-thread-1",
                },
            )
            self.assertEqual("ok", msg["status"])
            self.assertTrue(msg["ingest"].get("event_id_hash"))
            self.assertTrue(msg["retrieve"]["context_pack_id"])
            records_after_prompt = [
                json.loads(line)
                for line in event_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            prompt_telemetry = [
                record
                for record in records_after_prompt
                if record.get("record_type") == "context_pack_telemetry"
                and record.get("context_pack_id") == msg["retrieve"]["context_pack_id"]
            ]
            self.assertTrue(prompt_telemetry, records_after_prompt)
            telemetry = prompt_telemetry[-1]
            self.assertEqual("telemetry_only", telemetry["audit_mode"])
            self.assertEqual("codex_hook_retrieve", telemetry["retrieval_request_metadata"]["retrieval_source"])
            self.assertEqual("UserPromptSubmit", telemetry["retrieval_request_metadata"]["codex_event"])
            self.assertEqual("before_llm_retrieve", telemetry["retrieval_request_metadata"]["lifecycle_stage"])
            self.assertEqual("explicit", telemetry["retrieval_request_metadata"]["session_id_source"])
            self.assertGreater(telemetry["remote_context_budget_tokens"], 0)
            self.assertEqual(0, telemetry["used_remote_context_tokens"])
            self.assertIn("memory_layer_budget", telemetry)
            prompt_replay = MatrixArkMcpServer(MatrixArkLocalAdapter(event_log), line_json=True, access_mode="dev").call_tool(
                "matrixark_replay",
                {"scope": telemetry["scope"], "context_pack_id": msg["retrieve"]["context_pack_id"], "enable_replay": True},
            )
            prompt_replay_telemetry = [
                row for row in prompt_replay["events"] if row.get("record_type") == "context_pack_telemetry"
            ]
            self.assertTrue(prompt_replay_telemetry, prompt_replay)
            self.assertEqual(
                "codex_hook_retrieve",
                prompt_replay_telemetry[-1]["retrieval_request_metadata"]["retrieval_source"],
            )
            self.assertIn("memory_layer_budget", prompt_replay_telemetry[-1])
            context_pack_dashboard = MatrixArkLocalAdapter(event_log).ingestion_dashboard(
                {"scope": telemetry["scope"], "table": "context_packs", "page_size": 5}
            )
            dashboard_rows = [
                row
                for row in context_pack_dashboard["rows"]
                if row.get("context_pack_id") == msg["retrieve"]["context_pack_id"]
            ]
            self.assertTrue(dashboard_rows, context_pack_dashboard)
            dashboard_row = dashboard_rows[-1]
            self.assertEqual("context_pack_telemetry", dashboard_row["row_type"])
            self.assertEqual("codex_hook_retrieve", dashboard_row["retrieval_source"])
            self.assertEqual("UserPromptSubmit", dashboard_row["codex_event"])
            self.assertEqual("before_llm_retrieve", dashboard_row["lifecycle_stage"])
            self.assertGreater(dashboard_row["remote_context_budget_tokens"], 0)
            self.assertIn("memory_layer_budget", dashboard_row)

            resource_result = self.run_hook(
                repo,
                event_log,
                event="ResourceAdded",
                payload={"raw_uri": str(resource), "resource_type": "md", "thread_id": "codex-thread-1", "wait": True},
            )
            self.assertEqual("ok", resource_result["status"])
            self.assertEqual(str(resource), resource_result["resource_uri"])
            self.assertTrue(resource_result["ingest"].get("resource_chunks"))
            self.assertGreaterEqual(resource_result["ingest"].get("resource_fact_event_count", 0), 1)
            self.assertLessEqual(resource_result["ingest"].get("resource_fact_event_count", 0), 8)

            skill_result = self.run_hook(
                repo,
                event_log,
                event="ResourceAdded",
                payload={"raw_uri": str(skill), "thread_id": "codex-thread-1", "wait": True},
            )
            self.assertEqual("ok", skill_result["status"])
            self.assertEqual("skill", skill_result["resource_type"])
            self.assertIsInstance(skill_result["ingest"].get("skill_hash"), int)

            records = MatrixArkLocalAdapter(event_log).read_all()
            context_tree_records = [
                record
                for record in records
                if record.get("record_type") in {"context_node", "context_child_ref"}
            ]
            self.assertTrue(context_tree_records)
            self.assertFalse(any("status" in record for record in context_tree_records))
            resource_chunks = [
                record
                for record in records
                if record.get("record_type") == "resource_chunk"
                and record.get("resource_type") != "skill"
            ]
            self.assertTrue(resource_chunks)
            self.assertTrue(all(record.get("resource_hash") for record in resource_chunks))
            self.assertFalse(any(record.get("raw_uri") or record.get("source_ref") for record in resource_chunks))
            resource_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and str(record.get("entity_type") or "").startswith("resource_")
            ]
            self.assertTrue(resource_entities)
            self.assertFalse(
                any(
                    "/" in str(record.get("entity_name") or "")
                    or "\\" in str(record.get("entity_name") or "")
                    for record in resource_entities
                )
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_index"
                    and str(record.get("index_name") or "").lower() == "classification:new_event"
                    for record in records
                )
            )

            server = MatrixArkMcpServer(MatrixArkLocalAdapter(event_log), line_json=True, access_mode="dev")
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            pack = server.call_tool(
                "matrixark_retrieve",
                {
                    "scope": scope,
                    "query": "Which policy requires finance approval and which skill inspects replay evidence?",
                    "max_context_tokens": 1200,
                    "audit_mode": "full",
                },
            )
            ref_types = {str(ref.get("ref_type")) for ref in pack["selected_refs"]}
            self.assertIn("event", ref_types)
            self.assertIn("resource_chunk", ref_types)
            self.assertIn("skill_section", ref_types)
            self.assertGreaterEqual(sum(1 for ref in pack["selected_refs"] if ref.get("ref_type") == "resource_chunk"), 1)
            self.assertGreaterEqual(sum(1 for ref in pack["selected_refs"] if ref.get("ref_type") == "skill_section"), 1)
            self.assertNotIn("context_assembly_policy", pack)
            self.assertNotIn("recall_policy", pack)
            audit = next(
                record
                for record in reversed(server.adapter.read_all())
                if record.get("record_type") == "context_pack_audit"
                and isinstance(record.get("backend_retrieval_pushdown"), dict)
            )
            pushdown = audit["backend_retrieval_pushdown"]
            self.assertIn(pushdown["execution_mode"], {"adapter_prefilter", "adapter_prefilter_cached"})
            self.assertGreater(pushdown["dropped_by_type"], 0)
            context_pack_id = pack.get("context_pack_id") or pack.get("pack_id")
            replay = server.call_tool("matrixark_replay", {"scope": scope, "context_pack_id": context_pack_id, "enable_replay": True})
            audits = [row for row in replay["events"] if row.get("record_type") == "context_pack_audit"]
            self.assertTrue(any(row.get("context_pack_id") == context_pack_id for row in audits))


if __name__ == "__main__":
    unittest.main()
