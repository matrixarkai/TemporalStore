# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexPipelinePart2 methods split from test_matrixark_codex_hook_pipeline.MatrixArkCodexHookPipelineTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_pipeline import (
    FastHookLocalAdapter,
    Namespace,
    Path,
    candidate_memory_layer_name,
    compact_dropped_refs_for_context_pack,
    dropped_ref_layer_budget,
    matrixark_codex_hook,
    memory_layer_for_serving_ref,
    memory_layer_pressure_summary,
    mock,
    os,
    packing_sort_key,
    select_token_budgeted_refs,
    selected_ref_layer_budget,
    suppress_extracted_represented_pending_events,
    suppress_profile_shadowed_session_entities,
    tempfile,
    time,
)
except ImportError:
    from test_matrixark_codex_hook_pipeline import (
    FastHookLocalAdapter,
    Namespace,
    Path,
    candidate_memory_layer_name,
    compact_dropped_refs_for_context_pack,
    dropped_ref_layer_budget,
    matrixark_codex_hook,
    memory_layer_for_serving_ref,
    memory_layer_pressure_summary,
    mock,
    os,
    packing_sort_key,
    select_token_budgeted_refs,
    selected_ref_layer_budget,
    suppress_extracted_represented_pending_events,
    suppress_profile_shadowed_session_entities,
    tempfile,
    time,
)


class _CodexPipelinePart2:
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

    def test_current_state_floor_keeps_profile_entity_ahead_of_same_session_event(self) -> None:
        same_session_event = {
            "ref_type": "event",
            "ref_hash": 191,
            "text": "user: latest recovery preference used to wait for Stop only",
            "score": 0.99,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "event_type": "preference_update",
            "updated_at_ms": 1000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 192,
            "text": "user_preference: recovery_mode = commit memories on threshold or idle timeout; Stop flushes leftovers",
            "score": 0.05,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "entity_type": "user_preference",
            "entity_name": "recovery_mode",
            "updated_at_ms": 2000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }

        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [same_session_event, profile_entity],
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

        self.assertEqual([192], [ref["ref_hash"] for ref in selected])
        self.assertEqual("profile_entity", selected[0]["budget_memory_layer"])
        self.assertEqual(1, dropped["memory_layer_floor"])
        self.assertTrue(
            any(
                ref.get("ref_hash") == 191
                and ref.get("drop_reason") == "memory_layer_floor"
                and ref.get("memory_layer_floor_reserved_layer") == "profile_entity"
                and ref.get("budget_memory_layer") == "same_session_event"
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )

    def test_normal_query_floor_keeps_profile_entity_ahead_of_same_session_event(self) -> None:
        same_session_event = {
            "ref_type": "event",
            "ref_hash": 195,
            "text": "user: retrieval should use only same-session event evidence for this ordinary follow-up",
            "score": 0.99,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "event_type": "preference_update",
            "updated_at_ms": 1000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 196,
            "text": "user_preference: retrieval should include durable profile entity bridges even for ordinary follow-ups",
            "score": 0.05,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "entity_type": "user_preference",
            "entity_name": "retrieval_scope",
            "updated_at_ms": 2000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }

        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [same_session_event, profile_entity],
            [],
            max_context_tokens=32,
            auxiliary_quota=0,
            question_type="fact",
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

        self.assertEqual([196], [ref["ref_hash"] for ref in selected])
        self.assertEqual("profile_entity", selected[0]["budget_memory_layer"])
        self.assertEqual(1, dropped["memory_layer_floor"])
        self.assertEqual(1, dropped["cross_session_policy"]["entity_bridge_selected_ref_count"])
        self.assertTrue(
            any(
                ref.get("ref_hash") == 195
                and ref.get("drop_reason") == "memory_layer_floor"
                and ref.get("memory_layer_floor_reserved_layer") == "profile_entity"
                and ref.get("budget_memory_layer") == "same_session_event"
                for ref in dropped["refs"]
            ),
            dropped["refs"],
        )

    def test_latest_floor_keeps_profile_entity_ahead_of_same_session_event(self) -> None:
        same_session_event = {
            "ref_type": "event",
            "ref_hash": 193,
            "text": "user: latest recovery preference used to wait for Stop only",
            "score": 0.99,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "event_type": "preference_update",
            "updated_at_ms": 1000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 194,
            "text": "user_preference: recovery_mode = commit memories on threshold or idle timeout; Stop flushes leftovers",
            "score": 0.05,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "entity_type": "user_preference",
            "entity_name": "recovery_mode",
            "updated_at_ms": 2000,
            "source_roles": ["user"],
            "source_role_counts": {"user": 1},
        }

        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [same_session_event, profile_entity],
            [],
            max_context_tokens=32,
            auxiliary_quota=0,
            question_type="latest",
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

        self.assertEqual([194], [ref["ref_hash"] for ref in selected])
        self.assertEqual("profile_entity", selected[0]["budget_memory_layer"])
        self.assertEqual(1, dropped["memory_layer_floor"])
        self.assertTrue(
            any(
                ref.get("ref_hash") == 193
                and ref.get("drop_reason") == "memory_layer_floor"
                and ref.get("memory_layer_floor_reserved_layer") == "profile_entity"
                and ref.get("budget_memory_layer") == "same_session_event"
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
        feature_pending_event = {
            **pending_event,
            "profile_memory_kind": "memory_feature",
            "profile_memory_class": "memory_feature",
            "token_estimate": 8,
        }
        same_session_feature_segment = {
            "ref_type": "segment",
            "session_continuity": "same_session",
            "memory_scope": "session",
            "source_profile_memory_kinds": ["memory_feature"],
            "token_estimate": 9,
            "text": "feature memory segment",
        }
        cross_session_feature_event = {
            "ref_type": "event",
            "session_continuity": "cross_session",
            "memory_scope": "session",
            "source_profile_memory_classes": ["memory_feature"],
            "token_estimate": 10,
            "text": "cross-session feature memory event",
        }
        feature_budget = selected_ref_layer_budget(
            [feature_pending_event, same_session_feature_segment, cross_session_feature_event]
        )
        self.assertEqual(1, feature_budget["by_memory_layer"]["pending_async_memory_feature_event"]["refs"])
        self.assertEqual(8, feature_budget["by_memory_layer"]["pending_async_memory_feature_event"]["tokens"])
        self.assertEqual(1, feature_budget["by_memory_layer"]["same_session_memory_feature_segment"]["refs"])
        self.assertEqual(9, feature_budget["by_memory_layer"]["same_session_memory_feature_segment"]["tokens"])
        self.assertEqual(1, feature_budget["by_memory_layer"]["cross_session_memory_feature_event"]["refs"])
        self.assertEqual(10, feature_budget["by_memory_layer"]["cross_session_memory_feature_event"]["tokens"])
        phase_only_feature_pending_event = {
            "ref_type": "event",
            "event_type": "memory_feature",
            "profile_memory_kind": "memory_feature",
            "extraction_phase": "pending_async",
            "token_estimate": 6,
            "text": "pending feature memory after compact context projection",
        }
        self.assertEqual(
            "pending_async_memory_feature_event",
            candidate_memory_layer_name(phase_only_feature_pending_event),
        )
        self.assertEqual(
            "pending_async_memory_feature_event",
            memory_layer_for_serving_ref(phase_only_feature_pending_event),
        )

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
        feature_dropped_budget = dropped_ref_layer_budget(
            {
                "refs": [
                    {
                        **cross_session_feature_event,
                        "drop_reason": "memory_layer_budget",
                        "memory_layer_budget_capped_layer": "cross_session_memory_feature_event",
                    }
                ]
            }
        )
        self.assertEqual(
            1,
            feature_dropped_budget["by_memory_layer"]["cross_session_memory_feature_event"]["refs"],
        )
        self.assertEqual(
            10,
            feature_dropped_budget["by_memory_layer"]["cross_session_memory_feature_event"]["tokens"],
        )

    def test_pending_async_cleanup_only_suppresses_represented_events(self) -> None:
        represented_pending = {
            "ref_type": "event",
            "ref_hash": 501,
            "event_type": "pending_async",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "extraction_phase": "pending_async",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "token_estimate": 6,
            "text": "user: represented pending live hook message",
        }
        unrelated_pending = {
            **represented_pending,
            "ref_hash": 502,
            "text": "user: unrelated pending live hook message that still has no extracted memory",
        }
        extracted_entity = {
            "ref_type": "entity",
            "ref_hash": 601,
            "entity_type": "user_requirement",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "source_event_ids": [501],
            "token_estimate": 7,
            "text": "user_requirement: represented pending live hook message",
        }
        dropped: dict = {}

        selected, removed_tokens = suppress_extracted_represented_pending_events(
            [represented_pending, unrelated_pending, extracted_entity],
            dropped,
        )

        self.assertEqual([502, 601], [item["ref_hash"] for item in selected])
        self.assertEqual(6, removed_tokens)
        self.assertEqual(1, dropped["pending_async_event_superseded_by_extracted_refs"])

    def test_pending_async_cleanup_reads_compacted_lineage_metadata(self) -> None:
        represented_pending = {
            "ref_type": "event",
            "metadata": {"ref_hash": 701},
            "event_type": "pending_async",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "extraction_phase": "pending_async",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "token_estimate": 5,
            "text": "assistant: compacted lineage pending event",
        }
        extracted_segment = {
            "ref_type": "segment",
            "ref_hash": 801,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "metadata": {"source_event_ids": [701]},
            "token_estimate": 4,
            "text": "assistant: compacted lineage extracted segment",
        }
        dropped: dict = {}

        selected, removed_tokens = suppress_extracted_represented_pending_events(
            [represented_pending, extracted_segment],
            dropped,
        )

        self.assertEqual([801], [item["ref_hash"] for item in selected])
        self.assertEqual(5, removed_tokens)
        self.assertEqual(1, dropped["pending_async_event_superseded_by_extracted_refs"])

    def test_pending_async_cleanup_suppresses_feature_memory_pending_event(self) -> None:
        represented_feature_pending = {
            "ref_type": "event",
            "ref_hash": 901,
            "event_type": "memory_feature",
            "extraction_phase": "pending_async",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "profile_memory_kind": "memory_feature",
            "profile_memory_class": "memory_feature",
            "token_estimate": 8,
            "text": "user: focus on profile memory features",
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 902,
            "entity_type": "memory_feature_profile",
            "entity_name": "memory_feature_profile",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "profile_memory_kind": "memory_feature",
            "profile_memory_class": "memory_feature",
            "source_event_ids": [901],
            "token_estimate": 7,
            "text": "memory_feature_profile: focus on profile memory features",
        }
        dropped: dict = {}

        self.assertEqual("pending_async_memory_feature_event", candidate_memory_layer_name(represented_feature_pending))
        selected, removed_tokens = suppress_extracted_represented_pending_events(
            [represented_feature_pending, profile_entity],
            dropped,
        )

        self.assertEqual([902], [item["ref_hash"] for item in selected])
        self.assertEqual(8, removed_tokens)
        self.assertEqual(1, dropped["pending_async_event_superseded_by_extracted_refs"])

    def test_profile_entity_cleanup_suppresses_shadowed_same_session_entity(self) -> None:
        session_entity = {
            "ref_type": "entity",
            "ref_hash": 4501,
            "entity_type": "user_preference",
            "entity_name": "memory_mode",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "token_estimate": 4,
            "text": "old same-session memory mode",
        }
        profile_entity = {
            "ref_type": "entity",
            "ref_hash": 4502,
            "entity_type": "user_preference",
            "entity_name": "memory_mode",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "source_entity_hashes": [4501],
            "token_estimate": 5,
            "text": "current profile memory mode",
        }
        other_session_entity = {
            "ref_type": "entity",
            "ref_hash": 4503,
            "entity_type": "user_preference",
            "entity_name": "other_mode",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "token_estimate": 3,
            "text": "keep different same-session entity",
        }
        dropped: dict = {}

        selected, removed_tokens = suppress_profile_shadowed_session_entities(
            [session_entity, profile_entity, other_session_entity],
            dropped,
        )

        self.assertEqual([4502, 4503], [item["ref_hash"] for item in selected])
        self.assertEqual(4, removed_tokens)
        self.assertEqual(1, dropped["profile_entity_shadowed_session_entities"])
        self.assertTrue(
            any(
                ref.get("drop_reason") == "profile_entity_shadowed_session_entity"
                and ref.get("profile_shadowed_reason") == "selected_profile_entity_supersedes_session_entity"
                for ref in dropped.get("refs", [])
            ),
            dropped,
        )

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
            self.assertEqual("deferred", result["auto_batch_extract_result"]["status"])
            self.assertEqual("idle_timeout", result["auto_batch_extract_result"]["trigger_policy"])

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
                    and ref.get("event_type") == "user_prompt"
                    and ref.get("memory_layer") == "pending_async_event"
                    and ref.get("memory_scope") == "session"
                    and ref.get("session_continuity") == "same_session"
                    and "phoenix quartz" in str(ref.get("text") or "")
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )
            pending_ref = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "event" and ref.get("memory_layer") == "pending_async_event"
            )
            self.assertEqual("pending_async_event", candidate_memory_layer_name(pending_ref))
            budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_layer"]["pending_async_event"]["refs"], 1)
            self.assertGreaterEqual(budget["by_extraction_phase"]["pending_async"]["refs"], 1)
            self.assertIn("selected_pending_async_event_refs:1", pack["quality_warnings"])
            self.assertEqual(1, pack["recall_policy"]["selected_pending_async"]["selected_ref_count"])
            self.assertEqual(1, pack["retrieval_metrics"]["selected_pending_async_ref_count"])
            readiness = pack["retrieval_metrics"]["async_pipeline_readiness"]
            self.assertFalse(readiness["ready_for_retrieval"])
            self.assertGreaterEqual(readiness["pending_source_roles"].get("user", 0), 1)
            self.assertGreaterEqual(readiness["pending_source_hook_types"].get("before_llm", 0), 1)
            self.assertGreaterEqual(readiness["pending_source_codex_events"].get("UserPromptSubmit", 0), 1)
            self.assertEqual({"session": 1, "user_profile": 1}, readiness["pending_memory_scopes"])
            self.assertEqual({"cross_session": 1, "same_session": 1}, readiness["pending_session_continuities"])
            self.assertEqual({"pending_async": 1}, readiness["pending_extraction_phases"])
            for stage in ["compression", "embedding", "extraction", "summary"]:
                self.assertGreaterEqual(readiness["remaining_stage_counts"].get(stage, 0), 1)
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
            self.assertEqual("user_prompt", compact_ref["event_type"])
            self.assertEqual("pending_async_event", compact_ref["memory_layer"])
            self.assertIn("selected_pending_async_event_refs:1", compact_pack["quality_warnings"])
            self.assertNotIn("retrieval_metrics", compact_pack)
            self.assertNotIn("matched_index_terms", compact_ref)
            self.assertNotIn("metadata", compact_ref)
            self.assertNotIn("scope", compact_ref)

    def test_fast_hook_message_ingestion_omits_legacy_hook_type_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, mock.patch.dict(
            os.environ,
            {"MATRIXARK_HOOK_INCLUDE_LEGACY_HOOK_TYPE": "0"},
            clear=False,
        ):
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-minimal-lineage.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            scope = {
                "account_id": "acct_minimal_hook",
                "tenant_id": "tenant_minimal_hook",
                "user_id": "user_minimal_hook",
                "session_id": "session_minimal_hook",
            }
            hook = matrixark_codex_hook.codex_agent_hook(
                hook_type="before_llm",
                hook_id="minimal-hook-1",
                idempotency_key="minimal-hook-1",
                trigger="UserPromptSubmit",
                session_id_source="payload_field",
                identity={"thread_id": "thread-minimal-hook", "turn_id": "turn-minimal-hook-1"},
                observed_at_ms=123456,
            )
            self.assertNotIn("hook_type", hook)
            self.assertEqual("UserPromptSubmit", hook["trigger"])

            prompt_text = "Always use the Ubuntu TemporalStore repo and never use Windows folders for builds."
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
                text=prompt_text,
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook=hook,
                original_text=prompt_text,
            )
            self.assertEqual("accepted", result["status"])

            records = adapter.read_all()
            raw_messages = [record for record in records if record.get("record_type") == "agent_message"]
            pending_events = [
                record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("extraction_phase") == "pending_async"
            ]
            self.assertEqual(1, len(raw_messages))
            self.assertEqual(1, len(pending_events))
            # Folded: the pending event carries its vector; no separate record exists.
            self.assertTrue(pending_events[0].get("vector"),
                            "the pending event must carry its vector inline")
            pending_embeddings = [pending_events[0].get("embedding_meta") or {}]
            self.assertNotIn("hook_type", raw_messages[0])
            self.assertNotIn("hook_type", raw_messages[0].get("metadata", {}))
            self.assertNotIn("hook_type", raw_messages[0].get("agent_hook", {}))
            self.assertNotIn("hook_type", pending_events[0])
            self.assertNotIn("hook_type", pending_events[0].get("metadata", {}))
            self.assertNotIn("hook_type", pending_events[0].get("envelope", {}))
            self.assertEqual("UserPromptSubmit", raw_messages[0]["codex_event"])
            self.assertEqual("UserPromptSubmit", pending_events[0]["codex_event"])
            self.assertEqual({"before_llm": 1}, pending_events[0]["source_hook_type_counts"])
            self.assertEqual({"UserPromptSubmit": 1}, pending_events[0]["source_codex_event_counts"])
            raw_selection = raw_messages[0]["messages"][0]["metadata"]["codex_memory_selection"]
            self.assertEqual("selected_user_prompt", raw_selection["policy"])
            self.assertIn("selected_user_profile_fact", raw_selection["policies"])
            self.assertEqual(raw_selection["policies"], pending_events[0]["source_memory_selection_policies"])
            self.assertIn("selected_user_profile_fact", pending_events[0]["source_memory_selection_policies"])
            self.assertEqual("pending_async_event", pending_embeddings[0]["memory_layer"])
            self.assertEqual("user_prompt", pending_embeddings[0]["event_type"])
            self.assertNotIn("source_memory_selection_policies", pending_embeddings[0])
            self.assertNotIn("source_hook_type_counts", pending_embeddings[0])
            self.assertNotIn("source_codex_event_counts", pending_embeddings[0])

    def test_tool_output_memory_uses_short_structured_evidence(self) -> None:
        noisy_output = "\n".join(
            [
                "compiling dependency chunk that should not become memory",
                "Exit code: 0",
                "Ran 132 tests in 1.44s",
                "OK",
                "pushed commit f8c4907 to origin/main",
                *[f"verbose build line {index}" for index in range(80)],
            ]
        )
        summary = matrixark_codex_hook.selected_tool_memory_text(
            noisy_output,
            {"tool_name": "shell_command", "tool_status": "ok"},
        )

        self.assertEqual(
            "tool_name=shell_command; tool_status=ok; Exit code: 0; Ran 132 tests; pushed commit f8c4907 to origin/main",
            summary,
        )
        self.assertNotIn("verbose build line", summary)

    def test_tool_output_memory_captures_benchmark_quality_facts(self) -> None:
        noisy_output = "\n".join(
            [
                "large benchmark payload row that should not become memory",
                "LoCoMo benchmark workload: read-index reads",
                "p50 latency: 8.4 ms",
                "p99 latency=22.8ms",
                "throughput: 12,500 ops/s",
                "read hit rate: 91.7%",
                *[f"verbose benchmark row {index}" for index in range(120)],
            ]
        )
        summary = matrixark_codex_hook.selected_tool_memory_text(
            noisy_output,
            {"tool_name": "shell_command", "tool_status": "ok"},
        )

        self.assertIn("tool_name=shell_command", summary)
        self.assertIn("workload=read-index reads", summary)
        self.assertIn("p50 latency=8.4 ms", summary)
        self.assertIn("p99 latency=22.8ms", summary)
        self.assertIn("throughput=12,500 ops/s", summary)
        self.assertIn("hit_rate=91.7%", summary)
        self.assertIn("benchmark=locomo", summary)
        self.assertNotIn("verbose benchmark row", summary)

    def test_assistant_memory_captures_benchmark_outcome_facts(self) -> None:
        selected = matrixark_codex_hook.selected_assistant_memory_text(
            "Done. LoCoMo workload: profile retrieval p50 latency: 7ms p99 latency: 19ms throughput: 900 qps hit rate: 88%. "
            "Next: tune cross-session profile budget."
        )

        self.assertIn("Benchmark:", selected)
        self.assertIn("p50 latency=7ms", selected)
        self.assertIn("p99 latency=19ms", selected)
        self.assertIn("throughput=900 qps", selected)
        self.assertIn("hit_rate=88%", selected)
        self.assertIn("Next: tune cross-session profile budget", selected)

    def test_tool_output_memory_captures_validation_and_rejection_facts(self) -> None:
        noisy_output = "\n".join(
            [
                "building lots of crates that should not become memory",
                "cargo test -p temporalstore-rust succeeded",
                "warning: unused debug field was skipped",
                " ! [rejected] HEAD -> main (non-fast-forward)",
                *[f"verbose stdout line {index}" for index in range(80)],
            ]
        )
        summary = matrixark_codex_hook.selected_tool_memory_text(
            noisy_output,
            {"tool_name": "shell_command", "tool_status": "ok"},
        )

        self.assertIn("tool_name=shell_command", summary)
        self.assertIn("tool_status=ok", summary)
        self.assertIn("Validation: tests passed", summary)
        self.assertIn("notable=! [rejected] HEAD -> main (non-fast-forward)", summary)
        self.assertNotIn("verbose stdout line", summary)

    def test_fast_hook_tool_message_stores_structured_tool_fields(self) -> None:
        with (
            mock.patch.object(matrixark_codex_hook, "HOOK_TOOL_RESULT_RAW", True),
            mock.patch.object(matrixark_codex_hook, "HOOK_TOOL_RESULT_SERVING", True),
            tempfile.TemporaryDirectory() as tmp_dir,
        ):
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-tool-structured.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            scope = {
                "account_id": "acct_tool_summary",
                "tenant_id": "tenant_tool_summary",
                "user_id": "user_tool_summary",
                "session_id": "session_tool_summary",
            }
            text = matrixark_codex_hook.selected_tool_memory_text(
                "Exit code: 0\nRan 132 tests in 1.44s\nOK\npushed commit f8c4907 to origin/main\nlarge omitted stdout",
                {"tool_name": "shell_command", "tool_status": "ok"},
            )
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=Namespace(
                    event="PostToolUse",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=300000,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                ),
                text=text,
                role="tool",
                agent_context={"workspace_root": "/repo", "tool_name": "shell_command", "tool_status": "ok"},
                hook={
                    "source": "codex",
                    "hook_id": "tool-summary-hook-1",
                    "observed_at_ms": 123456,
                    "idempotency_key": "tool-summary-hook-1",
                    "trigger": "PostToolUse",
                    "auto_captured": True,
                    "session_id_source": "payload_field",
                },
            )
            self.assertEqual("accepted", result["status"])

            records = adapter.read_all()
            raw_message = next(record for record in records if record.get("record_type") == "agent_message")
            pending_event = next(
                record
                for record in records
                if record.get("record_type") == "context_event" and record.get("extraction_phase") == "pending_async"
            )
            self.assertEqual("shell_command", raw_message["tool_name"])
            self.assertEqual("ok", raw_message["tool_status"])
            self.assertEqual("shell_command", pending_event["tool_name"])
            self.assertEqual("ok", pending_event["tool_status"])
            self.assertIn("Ran 132 tests", raw_message["messages"][0]["content"])
            self.assertIn("pushed commit f8c4907", pending_event["text"])
            self.assertNotIn("large omitted stdout", pending_event["text"])

    def test_fast_hook_tool_evidence_is_serving_pending_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-tool-default-serving.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            scope = {
                "account_id": "acct_tool_default",
                "tenant_id": "tenant_tool_default",
                "user_id": "user_tool_default",
                "session_id": "session_tool_default",
            }
            selected_tool_text = matrixark_codex_hook.selected_tool_memory_text(
                "Exit code: 0\nRan 9 focused tests\nOK\n"
                + "\n".join(f"huge stdout line {index}" for index in range(80)),
                {"tool_name": "shell_command", "tool_status": "ok"},
            )
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=Namespace(
                    event="PostToolUse",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=300000,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                ),
                text=selected_tool_text,
                role="tool",
                original_text="Exit code: 0\nRan 9 focused tests\nOK\n" + "\n".join(
                    f"huge stdout line {index}" for index in range(80)
                ),
                agent_context={"workspace_root": "/repo", "tool_name": "shell_command", "tool_status": "ok"},
                hook={
                    "source": "codex",
                    "hook_id": "tool-default-serving",
                    "observed_at_ms": 123456,
                    "idempotency_key": "tool-default-serving",
                    "trigger": "PostToolUse",
                    "auto_captured": True,
                    "session_id_source": "payload_field",
                },
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual("skipped_tool_result_raw_capture", result["raw_ingestion_status"])
            self.assertEqual("accepted", result["serving_projection_status"])
            self.assertEqual("pending", result["async_pipeline_status"])
            self.assertEqual(1, result["session_buffer"]["pending_event_count"])
            self.assertEqual(1, result["session_buffer"]["pending_message_count"])
            self.assertFalse(result["session_buffer"]["threshold_ready"])
            self.assertTrue(result["session_buffer"]["idle_commit_scheduled"])
            records = adapter.read_all()
            self.assertFalse(any(record.get("record_type") == "agent_message" for record in records))
            pending_event = next(
                record
                for record in records
                if record.get("record_type") == "context_event" and record.get("extraction_phase") == "pending_async"
            )
            self.assertEqual("tool", pending_event["role"])
            self.assertEqual(["selected_tool_evidence_only"], pending_event["source_memory_selection_policies"])
            self.assertEqual({"selected_tool_evidence_only": 1}, pending_event["source_memory_selection_policy_counts"])
            self.assertIn("Validation: tests passed", pending_event["text"])
            self.assertNotIn("huge stdout line", pending_event["text"])
            self.assertEqual(1, len(adapter.pending_session_events(scope)))

    def test_fast_hook_skipped_tool_result_reports_pre_ingest_idle_commit(self) -> None:
        old_raw = matrixark_codex_hook.HOOK_TOOL_RESULT_RAW
        old_serving = matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING
        old_rollout = matrixark_codex_hook.HOOK_TOOL_RESULT_ROLLOUT_BACKFILL
        old_auto = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_TOOL_RESULT_RAW = False
        matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING = False
        matrixark_codex_hook.HOOK_TOOL_RESULT_ROLLOUT_BACKFILL = False
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-skipped-tool-idle.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_skip_tool_idle",
                    "tenant_id": "tenant_skip_tool_idle",
                    "user_id": "user_skip_tool_idle",
                    "session_id": "session_skip_tool_idle",
                }
                user_args = Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=1,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )
                user_result = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=user_args,
                    text="Remember skipped tool hooks should still report idle memory flushes.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "source": "codex",
                        "hook_id": "skip-tool-idle-user",
                        "observed_at_ms": 123456,
                        "idempotency_key": "skip-tool-idle-user",
                        "trigger": "UserPromptSubmit",
                        "auto_captured": True,
                        "session_id_source": "payload_field",
                    },
                )
                self.assertEqual("accepted", user_result["status"])
                self.assertEqual("deferred", user_result["auto_batch_extract_result"]["status"])
                time.sleep(0.05)

                tool_args = Namespace(
                    event="PostToolUse",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=1,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )
                tool_result = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=tool_args,
                    text="Exit code: 0; Ran 1 focused test",
                    role="tool",
                    agent_context={"workspace_root": "/repo", "tool_name": "shell_command", "tool_status": "ok"},
                    hook={
                        "source": "codex",
                        "hook_id": "skip-tool-idle-tool",
                        "observed_at_ms": 123457,
                        "idempotency_key": "skip-tool-idle-tool",
                        "trigger": "PostToolUse",
                        "auto_captured": True,
                        "session_id_source": "payload_field",
                    },
                    original_text="Exit code: 0\nRan 1 focused test\nOK",
                )

                self.assertEqual("skipped", tool_result["status"])
                self.assertEqual("tool_result_ingestion_disabled", tool_result["reason"])
                self.assertEqual("skipped_tool_result", tool_result["raw_ingestion_status"])
                self.assertEqual("skipped_tool_result", tool_result["serving_projection_status"])
                self.assertEqual("idle_timeout", tool_result["idle_commit_result"]["trigger_policy"])
                self.assertEqual("idle_timeout", tool_result["auto_batch_extract_result"]["trigger_policy"])
                self.assertIn(tool_result["auto_batch_extract_result"]["status"], {"committed", "finalized"})

                records = adapter.read_all()
                committed_batches = [
                    record
                    for record in records
                    if record.get("record_type") == "context_batch_commit"
                    and record.get("trigger_policy") == "idle_timeout"
                ]
                self.assertEqual(1, len(committed_batches))
                self.assertIn("user", committed_batches[0].get("source_roles", []))
                self.assertEqual([], adapter.pending_session_events(scope))
                self.assertFalse(
                    any(
                        record.get("record_type") == "context_event"
                        and record.get("source_role") == "tool"
                        for record in records
                    )
                )
        finally:
            matrixark_codex_hook.HOOK_TOOL_RESULT_RAW = old_raw
            matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING = old_serving
            matrixark_codex_hook.HOOK_TOOL_RESULT_ROLLOUT_BACKFILL = old_rollout
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = old_auto

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
            self.assertNotIn("pre_retrieval_summary_refresh", pack)
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
                first_auto_batch = first["auto_batch_extract_result"]
                self.assertEqual("deferred", first_auto_batch["status"])
                self.assertEqual("idle_timeout", first_auto_batch["trigger_policy"])
                self.assertTrue(first_auto_batch["idle_commit_scheduled"])

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
                latest_context_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertGreaterEqual(len(latest_context_events), 2)
                committed_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("status") == "extraction_committed"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertEqual(2, len(committed_events))
                # Folded: each committed event carries its own vector, and the retired
                # record's compact metadata rides along under embedding_meta.
                committed_with_vectors = [
                    record for record in committed_events if record.get("vector")
                ]
                self.assertEqual(2, len(committed_with_vectors))
                committed_event_embeddings = [
                    record.get("embedding_meta") or {} for record in committed_with_vectors
                ]
                self.assertTrue(all(
                    (meta.get("memory_scope") or record.get("memory_scope")) == "session"
                    for meta, record in zip(committed_event_embeddings, committed_with_vectors)
                ))
                self.assertTrue(all(
                    (meta.get("session_continuity") or record.get("session_continuity")) == "same_session"
                    for meta, record in zip(committed_event_embeddings, committed_with_vectors)
                ))
                for embedding in committed_event_embeddings:
                    self.assertNotIn("source_roles", embedding)
                    self.assertNotIn("source_role_counts", embedding)
                    self.assertNotIn("source_hook_types", embedding)
                    self.assertNotIn("source_hook_type_counts", embedding)
                    self.assertNotIn("source_codex_events", embedding)
                    self.assertNotIn("source_codex_event_counts", embedding)
                    self.assertNotIn("extraction_context_event_ids", embedding)
                batch_summary_hashes = {
                    record.get("summary_hash")
                    for record in records
                    if record.get("record_type") == "context_summary"
                    and record.get("summary_type") == "batch_l0"
                    and record.get("batch_id_hash") == commit["batch_id_hash"]
                }
                batch_summaries = [
                    record
                    for record in records
                    if record.get("record_type") == "context_summary"
                    and record.get("summary_hash") in batch_summary_hashes
                ]
                self.assertTrue(batch_summaries)
                self.assertTrue(all(record.get("source_event_ids") for record in batch_summaries))
                # Folded: the batch summaries carry their vectors; the compact metadata of
                # the retired records rides along under embedding_meta.
                summaries_with_vectors = [
                    record for record in batch_summaries if record.get("vector")
                ]
                self.assertTrue(summaries_with_vectors)
                batch_summary_embeddings = [
                    record.get("embedding_meta") or {} for record in summaries_with_vectors
                ]
                for embedding, owner in zip(batch_summary_embeddings, summaries_with_vectors):
                    self.assertEqual("session", embedding.get("memory_scope") or owner.get("memory_scope"))
                    self.assertEqual("same_session", embedding.get("session_continuity") or owner.get("session_continuity"))
                    for field in [
                        "source_event_ids",
                        "source_entity_hashes",
                        "source_segment_hashes",
                        "source_roles",
                        "source_role_counts",
                        "source_hook_types",
                        "source_hook_type_counts",
                        "source_codex_events",
                        "source_codex_event_counts",
                        "source_memory_selection_policies",
                        "source_memory_selection_policy_counts",
                        "source_memory_scopes",
                        "source_session_continuities",
                        "source_extraction_phases",
                        "extraction_context_event_ids",
                    ]:
                        self.assertNotIn(field, embedding)
                extraction_audits = [
                    record
                    for record in records
                    if record.get("record_type") == "context_extraction_audit"
                    and record.get("batch_id_hash") == commit["batch_id_hash"]
                ]
                self.assertEqual(1, len(extraction_audits))
                audit_outputs = extraction_audits[0]["outputs"]
                self.assertEqual("always_when_profile_scope_available", audit_outputs["profile_promotion_policy"])
                self.assertFalse(audit_outputs["profile_promotion_importance_gate"])
                self.assertTrue(audit_outputs["profile_promotion_scope_available"])
                self.assertEqual(audit_outputs["entities"], audit_outputs["profile_entities"])
                self.assertGreaterEqual(audit_outputs["indexes"], audit_outputs["entity_indexes"])
                self.assertGreaterEqual(audit_outputs["summary_indexes"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["secondary_indexes"], audit_outputs["summary_indexes"])
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

    def test_fast_hook_threshold_commit_arms_idle_for_uncommitted_tail(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-threshold-tail-idle.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                base_scope = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_threshold_tail_idle",
                    "tenant_id": "tenant_fast_threshold_tail_idle",
                    "user_id": "user_fast_threshold_tail_idle",
                    "session_id": "session_fast_threshold_tail_idle",
                    "team": "codex",
                    "project": "temporalstore",
                    "idle_commit_timeout_ms": 120000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                server = Server()
                seed_args = {**base_scope, "session_commit_threshold": 20}
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**seed_args),
                    text="First live prompt should wait in the threshold buffer.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "turn_id": "tail-idle-1"},
                )
                second = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**seed_args),
                    text="Second assistant response should also wait in the buffer.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "turn_id": "tail-idle-2"},
                )
                self.assertEqual("deferred", first["auto_batch_extract_result"]["status"])
                self.assertEqual("deferred", second["auto_batch_extract_result"]["status"])

                threshold_args = {**base_scope, "session_commit_threshold": 2}
                third = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**threshold_args),
                    text="Third live prompt is the uncommitted tail that must be idle armed.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "turn_id": "tail-idle-3"},
                )
                commit = third["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(2, commit["committed_event_count"])
                self.assertTrue(commit["tail_idle_commit_scheduled"])
                self.assertEqual(1, commit["tail_pending_event_count"])
                self.assertEqual(1, commit["tail_pending_message_count"])
                self.assertTrue(third["session_buffer"]["tail_idle_commit_scheduled"])
                self.assertTrue(third["session_buffer"]["idle_commit_scheduled"])

                scope = {
                    "account_id": base_scope["account_id"],
                    "tenant_id": base_scope["tenant_id"],
                    "user_id": base_scope["user_id"],
                    "session_id": base_scope["session_id"],
                }
                pending_after = adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending_after))
                self.assertEqual(third["event_id_hash"], pending_after[0]["event_id_hash"])
                self.assertIn("uncommitted tail", pending_after[0]["text"])
                idle_tasks = [
                    record
                    for record in adapter.read_all()
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "idle_commit_scheduled"
                    and record.get("reason") == "session_buffer_threshold_tail_idle_deadline"
                ]
                self.assertEqual(1, len(idle_tasks))
                self.assertEqual(1, idle_tasks[0]["idle_commit_pending_event_count"])
                self.assertEqual(1, idle_tasks[0]["idle_commit_pending_message_count"])
                self.assertIn("user", idle_tasks[0]["source_roles"])
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
                selected_tool_text = "Tool evidence: Exit code: 0 proves the direct fast hook sees the third message."
                original_tool_text = selected_tool_text + "\n" + "\n".join(
                    f"verbose build output line {index} without serving value"
                    for index in range(40)
                )
                result = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=Namespace(**scope_args),
                    text=selected_tool_text,
                    role="tool",
                    original_text=original_tool_text,
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
                self.assertEqual(1, commit["source_memory_selection_lossy_count"])
                self.assertGreater(commit["source_memory_selection_dropped_text_chars"], 0)
                self.assertLess(commit["source_memory_selection_retained_text_ratio_avg"], 1.0)
                records = adapter.read_all()
                derived_records = [
                    record
                    for record in records
                    if record.get("batch_id_hash") == commit["batch_id_hash"]
                    and record.get("record_type")
                    in {"context_entity", "context_segment", "context_summary", "context_extraction_audit"}
                ]
                self.assertTrue(derived_records)
                self.assertTrue(
                    all(record.get("source_memory_selection_lossy_count") == 1 for record in derived_records)
                )
                self.assertTrue(
                    all(record.get("source_memory_selection_dropped_text_chars", 0) > 0 for record in derived_records)
                )
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual(2, commits[0]["pending_event_count_before_commit"])
                self.assertEqual(3, commits[0]["pending_message_count_before_commit"])
                self.assertEqual(1, commits[0]["source_memory_selection_lossy_count"])
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
                summary_indexes = [
                    record
                    for record in records
                    if record.get("record_type") == "context_index"
                    and record.get("data_model") == "context_summary"
                    and record.get("ref_type") == "summary"
                ]
                summary_index_names = {str(record.get("index_name") or "") for record in summary_indexes}
                self.assertIn("summary_type:batch_l0", summary_index_names)
                self.assertIn("memory_scope:session", summary_index_names)
                self.assertIn("session_continuity:same_session", summary_index_names)
                self.assertIn("memory_selection_policy:selected_user_prompt", summary_index_names)
                self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", summary_index_names)
                self.assertIn("memory_selection_policy:selected_tool_evidence_only", summary_index_names)

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
                self.assertEqual(1, tool_layer_budget["source_message_counts_by_role"].get("tool"))
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

    def test_fast_hook_previous_assistant_backfill_uses_after_llm_buffer_semantics(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-assistant-backfill.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_fast_assistant_backfill",
                    "tenant_id": "tenant_fast_assistant_backfill",
                    "user_id": "user_fast_assistant_backfill",
                    "session_id": "session_fast_assistant_backfill",
                }
                args = Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=2,
                    idle_commit_timeout_ms=300000,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )
                server = Server()
                assistant_result = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=args,
                    text="Assistant decision: previous response selected profile memory for Codex.",
                    role="assistant",
                    original_text="Assistant decision: previous response selected profile memory for Codex.\nLarge answer omitted.",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "hook_type": "after_llm",
                        "trigger": "UserPromptSubmit:previous_assistant_backfill",
                        "thread_id": "thread-fast-assistant-backfill",
                        "turn_id": "turn-fast-assistant-backfill-1",
                        "session_id_source": "payload_field",
                    },
                )
                self.assertEqual("accepted", assistant_result["status"])
                self.assertFalse(assistant_result["session_buffer"]["threshold_ready"])

                pending = adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending))
                self.assertEqual(["after_llm"], pending[0]["source_hook_types"])
                self.assertEqual({"after_llm": 1}, pending[0]["source_hook_type_counts"])
                self.assertEqual(["assistant"], pending[0]["source_roles"])

                user_result = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=args,
                    text="User prompt: what did the previous assistant response decide about profile memory?",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "hook_type": "before_llm",
                        "trigger": "UserPromptSubmit",
                        "thread_id": "thread-fast-assistant-backfill",
                        "turn_id": "turn-fast-assistant-backfill-2",
                        "session_id_source": "payload_field",
                    },
                )
                self.assertTrue(user_result["session_buffer"]["threshold_ready"])
                commit = user_result["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(["assistant", "user"], commit["source_roles"])
                self.assertEqual({"after_llm": 1, "before_llm": 1}, commit["source_hook_type_counts"])

                committed_event_hashes = {int(event_id) for event_id in commit["source_event_ids"]}
                records = adapter.read_all()
                committed_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("status") == "extraction_committed"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertEqual(2, len(committed_events))
                assistant_events = [
                    record
                    for record in committed_events
                    if record.get("source_role") == "assistant"
                ]
                self.assertEqual(1, len(assistant_events))
                self.assertIn("after_llm", assistant_events[0]["source_hook_types"])
                profile_entities = [
                    record
                    for record in records
                    if record.get("record_type") == "context_entity"
                    and record.get("memory_scope") == "user_profile"
                    and record.get("session_continuity") == "cross_session"
                    and "previous response selected profile memory" in str(record.get("state") or "")
                ]
                self.assertTrue(profile_entities)
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_previous_tool_backfill_respects_raw_only_tool_policy(self) -> None:
        old_raw = matrixark_codex_hook.HOOK_TOOL_RESULT_RAW
        old_serving = matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING
        old_auto = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_TOOL_RESULT_RAW = True
        matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING = False
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-tool-backfill-raw-only.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_fast_tool_raw_only",
                    "tenant_id": "tenant_fast_tool_raw_only",
                    "user_id": "user_fast_tool_raw_only",
                    "session_id": "session_fast_tool_raw_only",
                }
                result = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=Namespace(
                        event="UserPromptSubmit",
                        **scope,
                        team="codex",
                        project="temporalstore",
                        session_commit_threshold=2,
                        idle_commit_timeout_ms=300000,
                        understanding_provider="rules",
                        segment_provider="deterministic",
                    ),
                    text="Validation: tests passed. Commit abc123 pushed.",
                    role="tool",
                    original_text="Exit code: 0\nValidation: tests passed. Commit abc123 pushed.\nLarge stdout omitted.",
                    agent_context={"workspace_root": "/repo", "tool_name": "shell_command", "tool_status": "ok"},
                    hook={
                        "hook_type": "tool_result",
                        "trigger": "UserPromptSubmit:previous_tool_output_backfill",
                        "thread_id": "thread-fast-tool-raw-only",
                        "turn_id": "turn-fast-tool-raw-only-1",
                        "session_id_source": "payload_field",
                    },
                )
                self.assertEqual("accepted", result["status"])
                self.assertEqual("skipped_raw_only_tool_result", result["serving_projection_status"])
                self.assertEqual("raw_only", result["extraction_mode"])
                self.assertEqual("raw_only_compact_evidence", result["tool_result_policy"])
                self.assertEqual(0, result["serving_record_count"])
                self.assertFalse(adapter.pending_session_events(scope))

                records = adapter.read_all()
                self.assertTrue(any(record.get("record_type") == "agent_message" for record in records))
                self.assertFalse(any(record.get("record_type") == "context_event" for record in records))
        finally:
            matrixark_codex_hook.HOOK_TOOL_RESULT_RAW = old_raw
            matrixark_codex_hook.HOOK_TOOL_RESULT_SERVING = old_serving
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = old_auto

