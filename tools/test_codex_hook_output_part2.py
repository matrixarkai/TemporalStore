# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexHookOutputPart2 methods split from test_matrixark_codex_hook_output.MatrixArkCodexHookOutputTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_output import (
    Namespace,
    Path,
    hook,
    json,
    subprocess,
    sys,
    tempfile,
)
except ImportError:
    from test_matrixark_codex_hook_output import (
    Namespace,
    Path,
    hook,
    json,
    subprocess,
    sys,
    tempfile,
)


class _CodexHookOutputPart2:
    def test_codex_retrieve_ranking_options_enable_all_auto_memory_budgets(self) -> None:
        ranking = hook.codex_retrieve_ranking_options()

        self.assertEqual("auto", ranking["source_role_budget_mode"])
        self.assertEqual("auto", ranking["memory_layer_budget_mode"])
        self.assertEqual("auto", ranking["memory_selection_policy_budget_mode"])
        self.assertEqual("auto", ranking["extraction_phase_budget_mode"])

    def test_codex_retrieve_cross_session_options_are_bounded_profile_bridge(self) -> None:
        policy = hook.codex_retrieve_cross_session_options()

        self.assertTrue(policy["enabled"])
        self.assertEqual(0.12, policy["budget_ratio"])
        self.assertEqual(0.20, policy["max_budget_ratio"])
        self.assertEqual(4, policy["max_sessions"])
        self.assertEqual(24, policy["max_candidates"])
        self.assertEqual(1, policy["min_entity_bridge_refs"])
        self.assertEqual(
            ["entity", "summary", "compression", "event", "segment"],
            policy["preferred_ref_types"],
        )

    def test_session_commit_tool_call_trace_records_trigger_evidence(self) -> None:
        class Server:
            def handle(self, request):
                self.request = request
                result = {
                    "status": "committed",
                    "commit_id_hash": 77,
                    "commit_reason": "hook_boundary",
                    "trigger_policy": "force",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "committed_event_count": 3,
                    "extraction_context_event_count": 0,
                    "segments_written": 2,
                    "entities_written": 5,
                    "profile_entities_written": 2,
                    "indexes_written": 7,
                    "index_total_cap": 64,
                    "index_emitted_count": 7,
                    "index_dropped_by_total_cap_count": 3,
                    "trigger_evidence": {
                        "pending_event_count": 3,
                        "threshold_messages": 20,
                        "threshold_ready": False,
                        "force": True,
                        "commit_reason": "hook_boundary",
                    },
                }
                return {"result": {"content": [{"text": json.dumps(result)}]}}

        trace = {"tool_calls": []}
        result = hook.trace_tool_call(Server(), "matrixark_session_commit", {"force": True}, trace)

        self.assertEqual("committed", result["status"])
        item = trace["tool_calls"][0]
        self.assertEqual("ok", item["status"])
        self.assertEqual("force", item["result"]["trigger_policy"])
        self.assertEqual("final", item["result"]["extraction_phase"])
        self.assertTrue(item["result"]["final_session_boundary"])
        self.assertEqual(3, item["result"]["source_event_count"])
        self.assertEqual(2, item["result"]["segments_written"])
        self.assertEqual(5, item["result"]["entities_written"])
        self.assertEqual(5, item["result"]["session_entities_written"])
        self.assertEqual(2, item["result"]["profile_entities_written"])
        self.assertEqual(7, item["result"]["indexes_written"])
        self.assertEqual(64, item["result"]["index_total_cap"])
        self.assertEqual(3, item["result"]["index_dropped_by_total_cap_count"])
        self.assertTrue(item["result"]["trigger_evidence"]["force"])

    def test_ingest_tool_call_trace_records_auto_batch_commit_evidence(self) -> None:
        class Server:
            def handle(self, request):
                self.request = request
                result = {
                    "status": "accepted",
                    "event_id_hash": 11,
                    "node_hash": 22,
                    "hook_captured": True,
                    "auto_batch_extract_result": {
                        "status": "committed",
                        "commit_id_hash": 88,
                        "commit_reason": "threshold",
                        "trigger_policy": "threshold",
                        "extraction_phase": "provisional",
                        "final_session_boundary": False,
                        "committed_event_count": 2,
                        "extraction_context_event_count": 1,
                        "source_roles": ["llm", "model", "assistant", "user"],
                        "source_hook_types": ["before_llm", "after_llm"],
                        "source_codex_events": ["Stop", "UserPromptSubmit"],
                        "source_role_counts": {"llm": 1, "model": 2, "assistant": 1, "user": 1},
                        "source_hook_type_counts": {"before_llm": 1, "after_llm": 1},
                        "source_codex_event_counts": {"Stop": 1, "UserPromptSubmit": 1},
                        "profile_promotion_summary": [
                            {
                                "profile_entity_hash": 801,
                                "session_entity_hash": 701,
                                "entity_type": "assistant_decision",
                                "entity_name": "threshold_commit",
                                "source_session_ids": ["codex-session-threshold"],
                                "source_entity_count": 1,
                                "source_roles": ["model", "assistant", "user"],
                                "source_hook_types": ["before_llm", "after_llm"],
                                "source_codex_events": ["Stop", "UserPromptSubmit"],
                            }
                        ],
                        "segments_written": 1,
                        "entities_written": 4,
                        "profile_entities_written": 1,
                        "indexes_written": 6,
                        "index_total_cap": 64,
                        "index_emitted_count": 6,
                        "index_dropped_by_total_cap_count": 0,
                        "summary_refresh": {"status": "dirty_marked", "dirty_hashes": [101]},
                        "trigger_evidence": {
                            "pending_event_count": 2,
                            "threshold_messages": 2,
                            "threshold_ready": True,
                            "idle_ready": False,
                            "force": False,
                            "commit_reason": "threshold",
                        },
                    },
                }
                return {"result": {"content": [{"text": json.dumps(result)}]}}

        trace = {"tool_calls": []}
        result = hook.trace_tool_call(Server(), "matrixark_ingest", {"messages": []}, trace)

        self.assertEqual("accepted", result["status"])
        item = trace["tool_calls"][0]
        self.assertEqual("ok", item["status"])
        self.assertEqual("committed", item["result"]["auto_batch_extract_status"])
        auto_batch = item["result"]["auto_batch_extract"]
        self.assertEqual("threshold", auto_batch["trigger_policy"])
        self.assertEqual("provisional", auto_batch["extraction_phase"])
        self.assertFalse(auto_batch["final_session_boundary"])
        self.assertEqual(2, auto_batch["source_event_count"])
        self.assertEqual(1, auto_batch["extraction_context_event_count"])
        self.assertEqual(["assistant", "user"], auto_batch["source_roles"])
        self.assertEqual(["before_llm", "after_llm"], auto_batch["source_hook_types"])
        self.assertEqual(["Stop", "UserPromptSubmit"], auto_batch["source_codex_events"])
        self.assertEqual({"assistant": 4, "user": 1}, auto_batch["source_role_counts"])
        self.assertEqual({"before_llm": 1, "after_llm": 1}, auto_batch["source_hook_type_counts"])
        self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, auto_batch["source_codex_event_counts"])
        self.assertEqual(801, auto_batch["profile_promotion_summary"][0]["profile_entity_hash"])
        self.assertEqual(["codex-session-threshold"], auto_batch["profile_promotion_summary"][0]["source_session_ids"])
        self.assertEqual(1, auto_batch["segments_written"])
        self.assertEqual(4, auto_batch["entities_written"])
        self.assertEqual(4, auto_batch["session_entities_written"])
        self.assertEqual(1, auto_batch["profile_entities_written"])
        self.assertEqual(1, auto_batch["memory_layers_written"]["context_events"])
        self.assertEqual(1, auto_batch["memory_layers_written"]["segments"])
        self.assertEqual(4, auto_batch["memory_layers_written"]["session_entities"])
        self.assertEqual(1, auto_batch["memory_layers_written"]["profile_entities"])
        self.assertEqual(4, auto_batch["memory_layers_written"]["same_session_entities"])
        self.assertEqual(1, auto_batch["memory_layers_written"]["cross_session_entities"])
        self.assertEqual(6, auto_batch["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(1, auto_batch["memory_layers_written"]["summary_dirty_nodes"])
        self.assertEqual("dirty_marked", auto_batch["memory_layers_written"]["summary_refresh_status"])
        self.assertEqual("provisional", auto_batch["memory_layers_written"]["extraction_phase"])
        self.assertFalse(auto_batch["memory_layers_written"]["final_session_boundary"])
        self.assertEqual(6, auto_batch["indexes_written"])
        self.assertEqual(64, auto_batch["index_total_cap"])
        self.assertEqual(0, auto_batch["index_dropped_by_total_cap_count"])
        self.assertTrue(auto_batch["trigger_evidence"]["threshold_ready"])
        self.assertFalse(auto_batch["trigger_evidence"]["idle_ready"])
        decision = item["result"]["auto_batch_extract_decision"]
        self.assertEqual("committed", decision["decision"])
        self.assertEqual("committed", decision["auto_batch_extract_status"])
        self.assertEqual(["assistant", "user"], decision["source_roles"])
        self.assertEqual(["before_llm", "after_llm"], decision["source_hook_types"])
        self.assertEqual(["Stop", "UserPromptSubmit"], decision["source_codex_events"])
        self.assertEqual({"assistant": 4, "user": 1}, decision["source_role_counts"])
        self.assertEqual({"before_llm": 1, "after_llm": 1}, decision["source_hook_type_counts"])
        self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, decision["source_codex_event_counts"])
        self.assertEqual(801, decision["profile_promotion_summary"][0]["profile_entity_hash"])
        self.assertNotIn("memory_lineage", item["result"])

    def test_ingest_tool_call_trace_memory_lineage_requires_debug_lineage(self) -> None:
        original_debug_lineage = hook.CONTEXT_PACK_DEBUG_LINEAGE
        hook.CONTEXT_PACK_DEBUG_LINEAGE = True
        try:
            class Server:
                def handle(self, request):
                    return {
                        "result": {
                            "content": [
                                {
                                    "text": json.dumps(
                                        {
                                            "status": "accepted",
                                            "event_id_hash": 11,
                                            "node_hash": 22,
                                            "hook_captured": True,
                                            "auto_batch_extract_result": {
                                                "status": "committed",
                                                "trigger_policy": "threshold",
                                                "source_roles": ["user", "assistant"],
                                                "source_role_counts": {"user": 1, "assistant": 2},
                                                "source_hook_type_counts": {"before_llm": 1, "after_llm": 1},
                                                "source_codex_event_counts": {"UserPromptSubmit": 1, "Stop": 1},
                                            },
                                        }
                                    )
                                }
                            ]
                        }
                    }

            trace = {"tool_calls": []}
            hook.trace_tool_call(Server(), "matrixark_ingest", {"text": "remember this"}, trace)
        finally:
            hook.CONTEXT_PACK_DEBUG_LINEAGE = original_debug_lineage

        lineage = trace["tool_calls"][0]["result"]["memory_lineage"]
        self.assertTrue(lineage["user_prompt_captured"])
        self.assertTrue(lineage["assistant_response_captured"])
        self.assertEqual({"assistant": 2, "user": 1}, lineage["source_role_counts"])
        self.assertEqual({"after_llm": 1, "before_llm": 1}, lineage["source_hook_type_counts"])
        self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, lineage["source_codex_event_counts"])

    def test_ingest_tool_call_trace_records_auto_batch_deferred_decision(self) -> None:
        class Server:
            def handle(self, request):
                self.request = request
                result = {
                    "status": "accepted",
                    "event_id_hash": 11,
                    "node_hash": 22,
                    "hook_captured": True,
                    "session_buffer": {
                        "registered": True,
                        "pending_event_count": 1,
                        "threshold_messages": 2,
                        "threshold_ready": False,
                        "idle_commit_timeout_ms": 300000,
                        "idle_ready": False,
                        "auto_batch_extract": True,
                        "boundary_commit_requested": False,
                    },
                    "auto_batch_extract_result": {},
                }
                return {"result": {"content": [{"text": json.dumps(result)}]}}

        trace = {"tool_calls": []}
        result = hook.trace_tool_call(Server(), "matrixark_ingest", {"messages": []}, trace)

        self.assertEqual("accepted", result["status"])
        item = trace["tool_calls"][0]
        self.assertEqual("ok", item["status"])
        decision = item["result"]["auto_batch_extract_decision"]
        self.assertEqual("deferred", decision["decision"])
        self.assertEqual("threshold_not_reached", decision["reason"])
        self.assertEqual(1, decision["pending_event_count"])
        self.assertEqual(2, decision["threshold_messages"])
        self.assertFalse(decision["threshold_ready"])
        self.assertFalse(decision["idle_ready"])
        self.assertTrue(decision["auto_batch_extract"])
        self.assertEqual("threshold_not_reached", decision["trigger_evidence"]["commit_reason"])
        self.assertEqual(1, decision["trigger_evidence"]["pending_event_count"])
        self.assertEqual(2, decision["trigger_evidence"]["threshold_messages"])
        self.assertFalse(decision["trigger_evidence"]["threshold_ready"])
        self.assertEqual(300000, decision["trigger_evidence"]["idle_timeout_ms"])
        self.assertFalse(decision["trigger_evidence"]["idle_ready"])
        self.assertNotIn("batch me into entities", json.dumps(decision))

    def test_hook_output_memory_lineage_requires_debug_lineage(self) -> None:
        args = Namespace(session_id="codex-session-lineage")
        auto_batch_extract = {
            "status": "committed",
            "trigger_policy": "threshold",
            "source_roles": ["user", "assistant"],
            "source_role_counts": {"user": 1, "assistant": 2},
            "source_hook_type_counts": {"before_llm": 1, "after_llm": 1},
            "source_codex_event_counts": {"UserPromptSubmit": 1, "Stop": 1},
        }
        ingest = {
            "status": "accepted",
            "auto_batch_extract": auto_batch_extract,
            "auto_batch_extract_result": auto_batch_extract,
        }
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="payload_field",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            ingest=ingest,
            retrieve={},
            query="lineage default",
        )
        self.assertNotIn("memory_lineage", output)

        original_debug_lineage = hook.CONTEXT_PACK_DEBUG_LINEAGE
        hook.CONTEXT_PACK_DEBUG_LINEAGE = True
        try:
            debug_output = hook.codex_hook_output(
                args=args,
                status="ok",
                event="UserPromptSubmit",
                session_id_source="payload_field",
                agent_context={"local_context": [], "workspace_root": "/repo"},
                ingest=ingest,
                retrieve={},
                query="lineage debug",
            )
        finally:
            hook.CONTEXT_PACK_DEBUG_LINEAGE = original_debug_lineage

        lineage = debug_output["memory_lineage"]
        self.assertTrue(lineage["user_prompt_captured"])
        self.assertTrue(lineage["assistant_response_captured"])
        self.assertFalse(lineage["tool_evidence_captured"])
        self.assertEqual({"assistant": 2, "user": 1}, lineage["source_role_counts"])
        self.assertEqual({"after_llm": 1, "before_llm": 1}, lineage["source_hook_type_counts"])
        self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, lineage["source_codex_event_counts"])

    def test_user_prompt_emit_codex_additional_context_from_selected_refs(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="payload_field",
            agent_context={"local_context": [{"ref": "src/main.rs", "text": "local code"}], "workspace_root": "/repo"},
            ingest={"status": "accepted", "event_id_hash": 123},
            retrieve={
                "context_pack_id": "pack-1",
                "used_context_tokens": 42,
                "used_remote_context_tokens": 42,
                "used_local_context_tokens": 8,
                "total_prompt_context_tokens": 50,
                "remote_context_budget_tokens": 100,
                "requested_max_context_tokens": 160,
                "local_context_safety_margin_tokens": 4,
                "budget_source": "agent_provided_max_context_tokens",
                "selected_ref_counts": {"event": 1, "entity": 1, "summary": 1},
                "recall_policy": {
                    "session_identity": {
                        "session_id_source": "payload_field",
                        "strong_session_identity": True,
                        "fallback_session_identity": False,
                        "risk": "",
                    },
                    "session_continuity": {
                        "mode": "prefer",
                        "policy": "same-session continuity first; entity state bridges cross-session memory",
                        "same_session_selected_ref_count": 1,
                        "cross_session_selected_ref_count": 2,
                        "entity_bridge_selected_ref_count": 1,
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
                    "source_role_budget": {
                        "enabled": True,
                        "budget_tokens": {"llm": 10, "model": 22, "tool": 24, "user": 40},
                        "selected_tokens_by_role": {"llm": 7, "model": 15, "tool": 18},
                        "selected_ref_count_by_role": {"model": 1, "tool": 1},
                    },
                    "memory_layer_budget_policy": {
                        "enabled": True,
                        "mode": "auto",
                        "budget_tokens": {
                            "summary": 30,
                            "compression": 25,
                            "profile_entity": 40,
                            "same_session_event": 45,
                            "cross_session_event": 25,
                        },
                        "selected_tokens_by_layer": {
                            "summary": 12,
                            "profile_entity": 18,
                            "same_session_event": 12,
                        },
                        "selected_ref_count_by_layer": {
                            "summary": 1,
                            "profile_entity": 1,
                            "same_session_event": 1,
                        },
                    },
                },
                "local_context_policy": {"local_context_count": 1},
                "retrieval_metrics": {
                    "pre_retrieval_summary_refresh": {
                        "enabled": True,
                        "status": "refreshed",
                        "requested_limit": 2,
                        "refreshed_count": 1,
                        "compression_created_count": 1,
                        "elapsed_ms": 3.25,
                    },
                    "async_pipeline_readiness": {
                        "task_count": 4,
                        "ready_for_retrieval": False,
                        "remaining_stages": ["entity", "secondary_index", "summary"],
                        "remaining_stage_counts": {
                            "entity": 1,
                            "secondary_index": 1,
                            "summary": 2,
                        },
                        "pending_source_roles": {
                            "llm": 1,
                            "model": 1,
                            "tool": 1,
                            "user": 1,
                        },
                        "pending_source_hook_types": {
                            "after_llm": 2,
                            "hook_boundary": 1,
                        },
                        "pending_source_codex_events": {
                            "PostToolUse": 1,
                            "Stop": 1,
                        },
                        "pending_memory_scopes": {
                            "session": 3,
                            "user_profile": 2,
                        },
                        "pending_session_continuities": {
                            "cross_session": 2,
                            "same_session": 3,
                        },
                        "pending_extraction_phases": {
                            "final": 1,
                            "provisional": 3,
                        },
                        "pending_final_session_boundary_count": 1,
                        "memory_layer_readiness": {
                            "ready_for_retrieval": False,
                            "blocked_layers": [
                                "session",
                                "user_profile",
                                "same_session",
                                "cross_session",
                                "summary",
                            ],
                            "ready_layers": ["compression", "embedding"],
                            "layers": {
                                "session": {"ready": False, "pending_task_count": 3, "remaining_stages": []},
                                "user_profile": {"ready": False, "pending_task_count": 2, "remaining_stages": []},
                                "same_session": {"ready": False, "pending_task_count": 3, "remaining_stages": []},
                                "cross_session": {"ready": False, "pending_task_count": 2, "remaining_stages": []},
                                "summary": {"ready": False, "pending_task_count": 2, "remaining_stages": ["summary"]},
                                "compression": {"ready": True, "pending_task_count": 0, "remaining_stages": []},
                                "embedding": {"ready": True, "pending_task_count": 0, "remaining_stages": []},
                            },
                        },
                        "freshness_warnings": ["async_pipeline_followup_pending", "profile_summary_stale"],
                    },
                    "memory_layer_budget": {
                        "by_memory_scope": {
                            "session": {"refs": 1, "tokens": 12},
                            "user_profile": {"refs": 2, "tokens": 30},
                        },
                        "by_session_continuity": {
                            "same_session": {"refs": 1, "tokens": 12},
                            "cross_session": {"refs": 2, "tokens": 30},
                        },
                        "by_extraction_phase": {
                            "provisional": {"refs": 1, "tokens": 12},
                            "final": {"refs": 2, "tokens": 30},
                        },
                        "by_ref_type": {
                            "entity": {"refs": 1, "tokens": 18},
                            "summary": {"refs": 1, "tokens": 12},
                        },
                        "by_entity_type": {
                            "assistant_decision": {"refs": 1, "tokens": 14},
                            "tool_evidence": {"refs": 1, "tokens": 16},
                        },
                        "by_source_role": {
                            "llm": {"refs": 1, "tokens": 6},
                            "model": {"refs": 1, "tokens": 8},
                            "tool": {"refs": 1, "tokens": 16},
                        },
                        "by_hook_type": {
                            "after_llm": {"refs": 1, "tokens": 14},
                            "hook_boundary": {"refs": 1, "tokens": 16},
                        },
                        "by_codex_event": {
                            "PostToolUse": {"refs": 1, "tokens": 18},
                            "Stop": {"refs": 1, "tokens": 12},
                        },
                        "source_message_counts_by_role": {
                            "llm": 1,
                            "model": 2,
                            "tool": 1,
                        },
                        "source_hook_counts_by_type": {
                            "after_llm": 2,
                            "hook_boundary": 1,
                        },
                        "source_codex_event_counts_by_event": {
                            "PostToolUse": 1,
                            "Stop": 2,
                        },
                        "final_session_boundary_ref_count": 2,
                    },
                    "dropped_memory_layer_budget": {
                        "by_memory_scope": {
                            "user_profile": {"refs": 1, "tokens": 22},
                        },
                        "by_session_continuity": {
                            "cross_session": {"refs": 1, "tokens": 22},
                        },
                        "by_ref_type": {
                            "entity": {"refs": 1, "tokens": 22},
                        },
                        "by_entity_type": {
                            "assistant_decision": {"refs": 1, "tokens": 22},
                        },
                        "by_source_role": {
                            "model": {"refs": 1, "tokens": 22},
                        },
                        "by_hook_type": {
                            "after_llm": {"refs": 1, "tokens": 22},
                        },
                    },
                    "memory_layer_pressure": {
                        "selected_refs": 3,
                        "selected_tokens": 42,
                        "dropped_refs": 5,
                        "dropped_tokens": 76,
                        "pressure_dimensions": ["by_memory_scope", "by_source_role"],
                        "dropped_dimensions": ["by_memory_scope", "by_extraction_phase", "by_source_role"],
                        "profile_memory_pressure": True,
                        "cross_session_pressure": True,
                        "final_memory_pressure": True,
                        "assistant_memory_pressure": True,
                        "tool_memory_pressure": True,
                        "hook_boundary_source_pressure": True,
                        "after_llm_source_pressure": True,
                        "stop_event_source_pressure": True,
                        "post_tool_use_source_pressure": True,
                        "pressure_bucket_count": 2,
                        "dropped_bucket_count": 5,
                    },
                },
                "dropped_refs": {
                    "cross_session_budget": 2,
                    "source_role_budget": 1,
                    "max_selected_refs": 3,
                    "estimated_tokens": {"cross_session_budget": 21, "source_role_budget": 8, "max_selected_refs": 55},
                    "budget_fill_policy": "quality_first",
                },
                "selected_refs": [
                    {
                        "ref_type": "context_event",
                        "citation": "session:abc#turn=2",
                        "score": 0.91,
                        "token_estimate": 12,
                        "summary_text": "Alice asked Codex to keep TemporalStore production readiness context.",
                    }
                ],
            },
            query="TemporalStore hook status",
        )

        self.assertEqual("ok", output["status"])
        self.assertTrue(output["retrieve"]["additional_context_emitted"])
        self.assertEqual(1, output["retrieve"]["selected_ref_count"])
        self.assertEqual(100, output["retrieve"]["budget"]["remote_context_budget_tokens"])
        self.assertNotIn("memory_hierarchy", output["retrieve"])
        self.assertTrue(output["retrieve"]["budget_pressure"]["budget_pressure"])
        self.assertEqual(
            {"cross_session_budget": 2, "max_selected_refs": 3},
            output["retrieve"]["budget_pressure"]["dropped_by_reason"],
        )
        self.assertEqual(5, output["retrieve"]["budget_pressure"]["budget_pressure_reason_count"])
        self.assertEqual(
            1,
            output["retrieve"]["budget_pressure"]["dropped_memory_layer_budget"]["by_memory_scope"][
                "user_profile"
            ]["refs"],
        )
        self.assertEqual(5, output["retrieve"]["layers"]["memory_layer_pressure"]["dropped_refs"])
        self.assertNotIn("by_source_role", output["retrieve"]["layers"]["memory_layer_budget"])
        self.assertNotIn("source_message_counts_by_role", output["retrieve"]["layers"]["memory_layer_budget"])
        self.assertNotIn("assistant_memory_pressure", output["retrieve"]["layers"]["memory_layer_pressure"])
        self.assertEqual(58, output["retrieve"]["budget"]["remote_budget_remaining_tokens"])
        self.assertFalse(output["retrieve"]["budget"]["remote_budget_overrun"])
        self.assertEqual("payload_field", output["retrieve"]["session_identity"]["session_id_source"])
        self.assertTrue(output["retrieve"]["session_identity"]["strong_session_identity"])
        self.assertFalse(output["retrieve"]["session_identity"]["fallback_session_identity"])
        self.assertEqual(4, output["retrieve"]["async_pipeline_readiness"]["task_count"])
        self.assertFalse(output["retrieve"]["async_pipeline_readiness"]["ready_for_retrieval"])
        self.assertNotIn("pending_source_roles", output["retrieve"]["async_pipeline_readiness"])
        self.assertEqual(
            {
                "enabled": True,
                "status": "refreshed",
                "requested_limit": 2,
                "refreshed_count": 1,
                "compression_created_count": 1,
                "elapsed_ms": 3.25,
            },
            output["retrieve"]["pre_retrieval_summary_refresh"],
        )
        self.assertEqual(
            output["retrieve"]["pre_retrieval_summary_refresh"],
            output["retrieve"]["layers"]["pre_retrieval_summary_refresh"],
        )
        self.assertEqual(
            "local_first_remote_fill_remaining",
            output["retrieve"]["budget"]["budget_contract"]["mode"],
        )
        self.assertEqual(
            "requested_max_context_tokens-used_local_context_tokens-local_context_safety_margin_tokens",
            output["retrieve"]["budget"]["budget_contract"]["remote_budget_formula"],
        )
        self.assertTrue(output["retrieve"]["budget"]["budget_contract"]["contract_holds"])
        hook_output = output["hookSpecificOutput"]
        self.assertEqual("UserPromptSubmit", hook_output["hookEventName"])
        additional = hook_output["additionalContext"]
        self.assertIn("MatrixArk/TemporalStore retrieved context for Codex", additional)
        self.assertIn("Merge this remote memory with the visible local Codex context", additional)
        self.assertIn("session:abc#turn=2", additional)
        self.assertIn("Alice asked Codex", additional)
        self.assertIn("local_context_refs_seen=1", additional)
        self.assertNotIn("Budget summary:", additional)
        self.assertNotIn("remote_budget=100", additional)
        self.assertNotIn("budget_source=agent_provided_max_context_tokens", additional)
        self.assertNotIn("contract=local_first_remote_fill_remaining", additional)
        self.assertNotIn("Session identity:", additional)
        self.assertNotIn("source=payload_field", additional)
        self.assertNotIn("Layer summary:", additional)
        self.assertNotIn("memory_layer_budget:", additional)
        self.assertNotIn("memory_layer_pressure:", additional)
        self.assertNotIn("Budget pressure:", additional)
        self.assertNotIn("summary_refresh[", additional)
        self.assertNotIn("pressure_dimensions[by_memory_scope]", additional)
        self.assertNotIn("pressure_dimensions[by_memory_scope,by_source_role]", additional)
        self.assertNotIn("scope[session=1/12t, user_profile=2/30t]", additional)
        self.assertNotIn("continuity[cross_session=2/30t, same_session=1/12t]", additional)
        self.assertNotIn("phase[final=2/30t, provisional=1/12t]", additional)
        self.assertNotIn("ref_type[entity=1/18t, summary=1/12t]", additional)
        self.assertNotIn("entity_type[assistant_decision=1/14t, tool_evidence=1/16t]", additional)
        self.assertNotIn("source_role[assistant=2/14t, tool=1/16t]", additional)
        self.assertNotIn("hook_type[after_llm=1/14t, hook_boundary=1/16t]", additional)
        self.assertNotIn("codex_event[PostToolUse=1/18t, Stop=1/12t]", additional)
        self.assertNotIn("source_messages[assistant=3, tool=1]", additional)
        self.assertNotIn("source_hooks[after_llm=2, hook_boundary=1]", additional)
        self.assertNotIn("source_codex_events[PostToolUse=1, Stop=2]", additional)
        self.assertNotIn("final_boundary_refs=2", additional)
        self.assertNotIn("async_pipeline[", additional)
        self.assertNotIn("pending_scopes[", additional)
        self.assertNotIn("pending_continuity[", additional)
        self.assertNotIn("pending_phases[", additional)
        self.assertNotIn("pending_roles[", additional)
        self.assertIn("Memory hierarchy:", additional)
        self.assertIn("user_profile/cross_session refs are long-term state", additional)
        self.assertNotIn("cross_session_budget_floor_status=remote_budget_too_small_for_profile_floor", additional)
        self.assertNotIn("cross_session_budget=64", additional)
        self.assertNotIn("computed=64", additional)
        self.assertNotIn("floor=256", additional)
        self.assertNotIn("floor_applied=false", additional)
        self.assertNotIn(
            "memory_layer_budget[summary=30,compression=25,profile_entity=40,same_session_event=45,cross_session_event=25]",
            additional,
        )
        self.assertNotIn(
            "memory_layer_selected_tokens[profile_entity=18,same_session_event=12,summary=12]",
            additional,
        )
        self.assertNotIn("Budget pressure:", additional)
        self.assertNotIn("cross_session_budget=2", additional)
        self.assertNotIn("source_role_budget=1", additional)
        self.assertNotIn("max_selected_refs=3", additional)
        self.assertNotIn("budget_fill_policy=quality_first", additional)
        self.assertNotIn("dropped_memory_layer_budget:", additional)
        self.assertNotIn("scope[user_profile=1/22t]", additional)
        self.assertNotIn("continuity[cross_session=1/22t]", additional)
        self.assertNotIn("entity_type[assistant_decision=1/22t]", additional)
        self.assertNotIn("source_role[assistant=1/22t]", additional)
        self.assertNotIn("hook_type[after_llm=1/22t]", additional)

    def test_hook_output_memory_hierarchy_requires_debug_lineage(self) -> None:
        original_debug_lineage = hook.CONTEXT_PACK_DEBUG_LINEAGE
        hook.CONTEXT_PACK_DEBUG_LINEAGE = True
        try:
            output = hook.codex_hook_output(
                args=Namespace(session_id="codex-session-debug-hierarchy"),
                status="ok",
                event="UserPromptSubmit",
                session_id_source="payload_field",
                agent_context={"local_context": [], "workspace_root": "/repo"},
                retrieve={
                    "context_pack_id": "pack-debug-hierarchy",
                    "used_context_tokens": 18,
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
                            "budget_tokens": {"profile_entity": 40},
                            "selected_tokens_by_layer": {"profile_entity": 18},
                        },
                    },
                    "selected_refs": [
                        {
                            "ref_type": "entity",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "text": "profile entity bridge",
                            "token_estimate": 18,
                        }
                    ],
                },
                query="debug hierarchy",
            )
        finally:
            hook.CONTEXT_PACK_DEBUG_LINEAGE = original_debug_lineage

        hierarchy = output["retrieve"]["memory_hierarchy"]
        self.assertEqual("context_profile_entity", hierarchy["models"]["profile_index"]["data_model"])
        self.assertEqual("prefer", hierarchy["session_scope_mode"])
        self.assertTrue(hierarchy["cross_session_enabled"])
        self.assertEqual(64, hierarchy["cross_session_budget_tokens"])
        self.assertEqual("remote_budget_too_small_for_profile_floor", hierarchy["cross_session_budget_floor_status"])
        self.assertTrue(hierarchy["memory_layer_budget_enabled"])
        self.assertEqual("auto", hierarchy["memory_layer_budget_mode"])
        self.assertEqual({"profile_entity": 40}, hierarchy["memory_layer_budget_tokens"])
        self.assertEqual({"profile_entity": 18}, hierarchy["memory_layer_selected_tokens_by_layer"])

    def test_additional_context_memory_hierarchy_details_require_debug_lineage(self) -> None:
        original_debug_lineage = hook.CONTEXT_PACK_DEBUG_LINEAGE
        hook.CONTEXT_PACK_DEBUG_LINEAGE = True
        try:
            additional = hook.additional_context_from_retrieve(
                {
                    "context_pack_id": "pack-debug-hierarchy",
                    "used_context_tokens": 24,
                    "recall_policy": {
                        "cross_session": {
                            "budget_tokens": 64,
                            "computed_budget_tokens": 64,
                            "budget_floor_tokens": 256,
                            "budget_floor_applied": False,
                            "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                        },
                        "memory_layer_budget_policy": {
                            "budget_tokens": {
                                "summary": 30,
                                "profile_summary": 35,
                                "cross_session_summary": 25,
                                "profile_entity": 40,
                                "profile_compression": 22,
                                "cross_session_compression": 18,
                            },
                            "selected_tokens_by_layer": {
                                "profile_entity": 12,
                            },
                        },
                        "memory_selection_policy_budget_policy": {
                            "budget_tokens": {
                                "selected_assistant_decision_outcome_only": 36,
                                "selected_tool_evidence_only": 28,
                            },
                            "selected_tokens_by_policy": {
                                "selected_assistant_decision_outcome_only": 12,
                            },
                        },
                        "memory_layer_budget": {
                            "by_memory_scope": {
                                "user_profile": {"refs": 1, "tokens": 12},
                            },
                            "by_session_continuity": {
                                "cross_session": {"refs": 1, "tokens": 12},
                            },
                        }
                    },
                    "selected_refs": [
                        {
                            "ref_type": "entity",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "text": "profile entity bridge",
                            "token_estimate": 12,
                        }
                    ],
                },
                query="debug profile hierarchy",
                local_context_count=0,
                session_id_source="payload_field",
            )
        finally:
            hook.CONTEXT_PACK_DEBUG_LINEAGE = original_debug_lineage

        self.assertIn("Memory hierarchy:", additional)
        self.assertIn("cross_session_budget_floor_status=remote_budget_too_small_for_profile_floor", additional)
        self.assertIn("cross_session_budget=64", additional)
        self.assertIn("computed=64", additional)
        self.assertIn("floor=256", additional)
        self.assertIn("floor_applied=false", additional)
        self.assertIn(
            "memory_layer_budget[summary=30,profile_summary=35,cross_session_summary=25,profile_compression=22,cross_session_compression=18,profile_entity=40]",
            additional,
        )
        self.assertIn(
            "memory_selection_policy_budget[selected_assistant_decision_outcome_only=36,selected_tool_evidence_only=28]",
            additional,
        )
        self.assertIn(
            "memory_selection_policy_selected_tokens[selected_assistant_decision_outcome_only=12]",
            additional,
        )
        self.assertIn("memory_layer_selected_tokens[profile_entity=12]", additional)

    def test_additional_context_layer_summary_falls_back_to_serving_refs(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-layer-fallback",
                "selected_refs": [
                    {
                        "ref_type": "event",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "text": "current session prompt",
                    },
                    {
                        "ref_type": "entity",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "entity_type": "assistant_decision",
                        "source_roles": ["llm", "model", "assistant"],
                        "source_role_counts": {"llm": 1, "model": 1, "assistant": 4, "user": 1},
                        "source_hook_types": ["after_llm"],
                        "source_hook_type_counts": {"after_llm": 2},
                        "text": "assistant: keep cross-session profile decision",
                    },
                    {
                        "context_class": "summary",
                        "entity_type": "tool_evidence",
                        "source_memory_scopes": ["session", "user_profile"],
                        "source_session_continuities": ["same_session", "cross_session"],
                        "source_extraction_phases": ["final", "provisional"],
                        "source_roles": ["tool"],
                        "source_role_counts": {"tool": 3},
                        "source_hook_types": ["tool_result"],
                        "source_hook_type_counts": {"tool_result": 2},
                        "source_codex_events": ["PostToolUse"],
                        "source_codex_event_counts": {"PostToolUse": 2},
                        "text": "tool: tests ran successfully",
                    },
                ],
            },
            query="memory layers",
        )

        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertEqual("explicit", output["retrieve"]["session_identity"]["session_id_source"])
        self.assertTrue(output["retrieve"]["session_identity"]["strong_session_identity"])
        self.assertEqual("hook_metadata_fallback", output["retrieve"]["session_identity"]["source"])
        self.assertNotIn("Session identity:", additional)
        self.assertNotIn("source=explicit", additional)
        self.assertNotIn("Layer summary:", additional)
        self.assertNotIn("scope[session=2/", additional)
        self.assertNotIn("entity_type[assistant_decision=", additional)
        self.assertIn("assistant: keep cross-session profile decision", additional)
        self.assertIn("tool: tests ran successfully", additional)
        self.assertNotIn("source_role[", additional)
        self.assertNotIn("hook_type[", additional)
        self.assertNotIn("codex_event[", additional)
        self.assertNotIn("source_messages[", additional)
        self.assertNotIn("source_messages[llm=", additional)
        self.assertNotIn("source_messages[model=", additional)
        self.assertNotIn("source_hooks[", additional)
        self.assertNotIn("source_codex_events[", additional)
        self.assertNotIn("Retrieved memory lineage:", additional)
        self.assertNotIn("roles[assistant=6,tool=3,user=1]", additional)
        self.assertNotIn("hooks[after_llm=2,tool_result=2]", additional)
        self.assertNotIn("codex_events[PostToolUse=2]", additional)
        self.assertNotIn("captured[user_prompt,assistant_response,tool_evidence]", additional)

    def test_additional_context_lineage_uses_budget_roles_and_entity_type_fallback(self) -> None:
        original_debug_lineage = hook.CONTEXT_PACK_DEBUG_LINEAGE
        hook.CONTEXT_PACK_DEBUG_LINEAGE = True
        try:
            args = Namespace(session_id="codex-session-lineage-fallback")
            output = hook.codex_hook_output(
                args=args,
                status="ok",
                event="UserPromptSubmit",
                session_id_source="explicit",
                agent_context={"local_context": [], "workspace_root": "/repo"},
                retrieve={
                    "pack_id": "pack-lineage-fallback",
                    "selected_refs": [
                        {
                            "ref_type": "entity",
                            "entity_type": "assistant_decision",
                            "text": "assistant decided to keep profile extraction enabled",
                        },
                        {
                            "ref_type": "entity",
                            "entity_type": "tool_evidence",
                            "budget_source_role_counts": {"tool": 2},
                            "text": "tests passed after hook extraction",
                        },
                        {
                            "ref_type": "entity",
                            "entity_type": "user_requirement",
                            "budget_source_roles": ["user"],
                            "text": "user requested threshold and idle extraction",
                        },
                    ],
                },
                query="which memory sources were retrieved?",
            )
        finally:
            hook.CONTEXT_PACK_DEBUG_LINEAGE = original_debug_lineage

        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertIn("Retrieved memory lineage:", additional)
        self.assertIn("roles[assistant=1,tool=2,user=1]", additional)
        self.assertIn("captured[user_prompt,assistant_response,tool_evidence]", additional)

    def test_grouped_refs_count_and_format_as_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-grouped",
                "tokens": {"remote": 9},
                "selected_ref_groups": [
                    {
                        "count": 2,
                        "refs": [
                            {"ref_type": "summary", "source_locator": "node/a", "text": "Node A summary."},
                            {"ref_type": "event", "source_locator": "node/b", "body": "Node B event."},
                        ],
                    }
                ],
            },
            query="grouped refs",
        )
        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(2, output["retrieve"]["selected_ref_count"])
        self.assertEqual(9, output["retrieve"]["used_context_tokens"])
        self.assertIn("node/a", additional)
        self.assertIn("Node B event", additional)

    def test_codex_hook_heartbeat_is_filtered_from_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: TemporalStore is live and accepting MatrixArk hook writes."
        self.assertTrue(hook.is_codex_hook_heartbeat_text(heartbeat))
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-heartbeat",
                "selected_refs": [
                    {"ref_type": "event", "text": heartbeat},
                    {"ref_type": "event", "text": "user: real TemporalStore question"},
                ],
            },
            query="real question",
        )
        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(1, output["retrieve"]["selected_ref_count"])
        self.assertEqual({"event": 1}, output["retrieve"]["layers"]["selected_ref_counts"])
        self.assertNotIn("Codex hook heartbeat", additional)
        self.assertIn("real TemporalStore question", additional)

    def test_codex_hook_heartbeat_line_is_filtered_from_rendered_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: TemporalStore is live and accepting MatrixArk hook writes."
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-rendered-heartbeat",
                "context": heartbeat + "\nuser: real rendered TemporalStore memory",
            },
            query="real rendered memory",
        )

        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertNotIn("Codex hook heartbeat", additional)
        self.assertIn("real rendered TemporalStore memory", additional)
        self.assertEqual(
            len("user: real rendered TemporalStore memory"),
            output["retrieve"]["rendered_context_chars"],
        )

    def test_heartbeat_only_rendered_context_does_not_emit_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: TemporalStore is live and accepting MatrixArk hook writes."
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={"pack_id": "pack-heartbeat-only", "context": heartbeat},
            query="real memory",
        )

        self.assertNotIn("hookSpecificOutput", output)
        self.assertFalse(output["retrieve"]["additional_context_emitted"])
        self.assertEqual(0, output["retrieve"]["selected_ref_count"])
        self.assertEqual(0, output["retrieve"]["rendered_context_chars"])

    def test_non_prompt_event_keeps_audit_json_without_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="PostToolUse",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            ingest={"status": "accepted"},
        )
        self.assertNotIn("hookSpecificOutput", output)
        self.assertFalse(output["retrieve"]["additional_context_emitted"])
        self.assertTrue(output["lifecycle_stage"]["after_llm_ingest_only"])

    def test_selected_tool_evidence_filters_large_stdout(self) -> None:
        raw = "\n".join(
            [f"noise line {index}" for index in range(40)]
            + [
                "Exit code: 0",
                "Ran 32 tests in 0.508s",
                "OK",
                "To https://github.com/matrixarkai/TemporalStore.git",
                "002fbd45034c69ce4487a64ab40d90135a55ae1a refs/heads/main",
                "another verbose blob " * 200,
            ]
        )
        evidence = hook.selected_tool_evidence_text(raw, max_chars=1000)
        self.assertIn("Exit code: 0", evidence)
        self.assertIn("Ran 32 tests", evidence)
        self.assertIn("refs/heads/main", evidence)
        self.assertNotIn("noise line 0", evidence)
        self.assertLess(len(evidence), 1000)
        policy = hook.codex_memory_selection_metadata(role="tool", event="PostToolUse", text=evidence, original_text=raw)
        self.assertEqual("selected_tool_evidence_only", policy["policy"])
        self.assertFalse(policy["large_payload_verbatim_stored"])
        self.assertEqual("tool", policy["source_role"])
        self.assertGreater(policy["original_text_chars"], policy["selected_text_chars"])
        self.assertGreater(policy["dropped_text_chars"], 0)
        self.assertGreater(policy["dropped_line_count"], 0)
        self.assertLess(policy["retained_text_ratio"], 1.0)
        self.assertTrue(policy["selection_lossy"])

    def test_selected_tool_and_assistant_memory_capture_git_push_head_range(self) -> None:
        raw = "\n".join(
            [
                "Enumerating objects: 7, done.",
                "To https://github.com/matrixarkai/TemporalStore.git",
                "   b223ca8c..4eafaf9c  HEAD -> main",
            ]
        )
        tool_memory = hook.selected_tool_memory_text(raw, {"tool_name": "shell_command", "tool_status": "ok"})
        self.assertIn("pushed commit 4eafaf9c to origin/main", tool_memory)
        self.assertIn("tool_name=shell_command", tool_memory)
        self.assertIn("tool_status=ok", tool_memory)

        assistant_memory = hook.selected_assistant_memory_text(
            "Done. Git push output was b223ca8c..4eafaf9c  HEAD -> main after validation passed."
        )
        self.assertIn("Outcome: pushed commit 4eafaf9c to origin/main", assistant_memory)
        self.assertIn("Validation: tests passed", assistant_memory)

    def test_pushed_commit_is_the_new_head_however_the_push_is_described(self) -> None:
        """A range names both ends, and the commit that landed is the SECOND one.

        The looser patterns take the first hash they meet after the word "push", which in a
        range is the commit that was already there. Git's own output never says "push", so it
        reached the range pattern and read correctly while a sentence about the same push
        reported the old head. Both spellings are asserted here so the order cannot drift back.
        """
        old, new = "b223ca8c", "4eafaf9c"
        for description in (
            "   %s..%s  HEAD -> main" % (old, new),
            "To https://github.com/matrixarkai/TemporalStore.git\n   %s..%s  HEAD -> main" % (old, new),
            "Done. Git push output was %s..%s  HEAD -> main after validation passed." % (old, new),
            "git push succeeded: %s..%s HEAD -> main" % (old, new),
            "pushed %s..%s HEAD -> origin/main" % (old, new),
        ):
            self.assertEqual(
                new, hook.pushed_main_commit_from_text(description),
                "read the wrong end of the range in: %r" % description,
            )
        # Without a range there is only one commit to name, and it is still found.
        self.assertEqual(
            new, hook.pushed_main_commit_from_text("pushed commit %s to origin/main" % new)
        )
        self.assertEqual("", hook.pushed_main_commit_from_text("nothing was pushed here"))

    def test_selected_assistant_memory_filters_large_response(self) -> None:
        raw = "\n".join(
            [f"background explanation line {index}" for index in range(40)]
            + [
                "```",
                "large code block should not become long-term memory",
                "```",
                "Decision: keep assistant memory bounded to outcomes.",
                "Done. Tests passed and origin/main was pushed.",
                "Next: use profile entities for cross-session retrieval.",
                "another verbose paragraph " * 200,
            ]
        )
        evidence = hook.selected_assistant_memory_text(raw, max_chars=1000)
        self.assertIn("Decision: keep assistant memory bounded", evidence)
        self.assertIn("Tests passed and origin/main was pushed", evidence)
        self.assertIn("profile entities for cross-session retrieval", evidence)
        self.assertNotIn("background explanation line 0", evidence)
        self.assertNotIn("large code block", evidence)
        self.assertLess(len(evidence), 1000)
        policy = hook.codex_memory_selection_metadata(role="assistant", event="Stop", text=evidence, original_text=raw)
        self.assertEqual("selected_assistant_decision_outcome_only", policy["policy"])
        self.assertFalse(policy["large_payload_verbatim_stored"])
        self.assertEqual("assistant", policy["source_role"])
        self.assertGreater(policy["original_text_chars"], policy["selected_text_chars"])
        self.assertGreater(policy["dropped_text_chars"], 0)
        self.assertGreater(policy["dropped_line_count"], 0)
        self.assertLess(policy["retained_line_ratio"], 1.0)
        self.assertTrue(policy["selection_lossy"])

    def test_selected_assistant_memory_normalizes_response_formatting(self) -> None:
        raw = "\n".join(
            [
                "## Summary",
                "- Decision: promote Codex assistant outcomes into profile memory.",
                "1. Done. Tests passed and origin/main was pushed.",
                "> Next: retrieve profile entities across sessions with compact budgets.",
                "```",
                "- Decision: code block should not be selected.",
                "```",
            ]
        )
        evidence = hook.selected_assistant_memory_text(raw)

        self.assertIn("Decision: promote Codex assistant outcomes into profile memory.", evidence)
        self.assertIn("Done. Tests passed and origin/main was pushed.", evidence)
        self.assertIn("Next: retrieve profile entities across sessions with compact budgets", evidence)
        self.assertNotIn("- Decision:", evidence)
        self.assertNotIn("1. Done", evidence)
        self.assertNotIn("> Next", evidence)
        self.assertNotIn("code block should not be selected", evidence)

    def test_selected_assistant_memory_synthesizes_outcome_facts(self) -> None:
        raw = "\n".join(
            [
                "background explanation " * 120,
                "Implemented compact profile memory retrieval and pushed commit abc1234 to origin/main.",
                "Validation ran 73 tests passed and py_compile stayed clean.",
                "MatrixArk mirror push rejected as non-fast-forward.",
                "Changed context embedding lineage to count-only fields.",
                "Next: continue retrieval budget tuning.",
            ]
        )

        evidence = hook.selected_assistant_memory_text(raw, max_chars=1000)

        self.assertIn("Outcome: pushed commit abc1234 to origin/main", evidence)
        self.assertIn("Validation: 73 tests passed", evidence)
        self.assertIn("Blocker: MatrixArk mirror push rejected as non-fast-forward", evidence)
        self.assertIn("Changed: context embedding lineage to count-only fields", evidence)
        self.assertIn("Next: continue retrieval budget tuning", evidence)
        self.assertNotIn("background explanation background explanation", evidence)
        self.assertLess(len(evidence), 1000)

    def test_hook_async_message_ingest_args_marks_selected_memory_policy(self) -> None:
        args = Namespace(
            event="Stop",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        ingest_args = hook.hook_async_message_ingest_args(
            {"scope": {"tenant_id": "tenant", "user_id": "user", "session_id": "session"}},
            args,
            event="Stop",
            role="assistant",
            text="Decision: keep assistant memory compact.",
            metadata={"source": "test"},
            agent_hook={"hook_type": "after_llm"},
        )

        selection = ingest_args["metadata"]["codex_memory_selection"]
        self.assertEqual("selected_assistant_decision_outcome_only", selection["policy"])
        self.assertFalse(selection["large_payload_verbatim_stored"])
        self.assertEqual("codex_hook_before_temporalstore_ingest", selection["selection_stage"])
        self.assertEqual(selection["selected_text_chars"], selection["original_text_chars"])
        self.assertEqual(selection["selected_line_count"], selection["original_line_count"])
        self.assertEqual(0, selection["dropped_text_chars"])
        self.assertEqual(0, selection["dropped_line_count"])
        self.assertFalse(selection["selection_lossy"])

    def test_latest_assistant_rollout_returns_bounded_memory_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rollout = Path(tmp_dir) / "rollout-test.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {"type": "output_text", "text": "background detail\n" * 80},
                                {"type": "text", "text": "Decision: bounded assistant memory is enabled."},
                                {"type": "text", "text": "Done. Profile retrieval tests should stay focused."},
                            ],
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            evidence = hook.latest_codex_assistant_message_from_rollout({"transcript_path": str(rollout)})
            self.assertIn("Decision: bounded assistant memory is enabled.", evidence)
            self.assertIn("Profile retrieval tests should stay focused.", evidence)
            self.assertNotIn("background detail", evidence)

    def test_strict_codex_stdout_removes_rich_audit_fields(self) -> None:
        prompt_output = {
            "status": "ok",
            "event": "UserPromptSubmit",
            "ingest": {"status": "accepted"},
            "retrieve": {"additional_context_emitted": True},
            "hookSpecificOutput": {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": "remote memory",
            },
        }
        self.assertEqual(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": "remote memory",
                }
            },
            hook.strict_codex_stdout(prompt_output),
        )
        self.assertEqual({}, hook.strict_codex_stdout({"status": "ok", "event": "Stop"}))

    def test_fail_open_backend_error_is_visible_to_codex_user_prompt(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_codex_hook.py"),
            "--event",
            "UserPromptSubmit",
            "--backend",
            "temporalstore-direct",
            "--metaserver",
            "127.0.0.1:1",
            "--namespace",
            "missing_ns",
            "--table",
            "missing_table",
            "--temporalstore-lib",
            "/tmp/definitely-missing-libtemporalstore.so",
            "--request-timeout-ms",
            "1",
            "--io-timeout-ms",
            "1",
        ]
        proc = subprocess.run(
            cmd,
            input=json.dumps({"prompt": "Will this hook fail open?", "thread_id": "codex-fail-open"}),
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        output = json.loads(proc.stdout)
        self.assertEqual("warning", output["status"])
        self.assertEqual("hook_failed_fail_open", output["reason"])
        self.assertIn("hookSpecificOutput", output)
        self.assertIn("retrieval was attempted", output["hookSpecificOutput"]["additionalContext"])


    def test_fast_async_hook_ingest_writes_raw_and_serving_scope(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-native-session-1",
            team="codex",
            project="temporalstore",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="real hooked Codex message",
            role="user",
            agent_context={"workspace_root": "/repo"},
            hook={
                "session_id_source": "payload_field",
                "thread_id": "thread-fast-1",
                "turn_id": "turn-fast-1",
                "conversation_id": "conversation-fast-1",
            },
        )

        self.assertEqual("accepted", result["status"])
        self.assertEqual("accepted", result["raw_ingestion_status"])
        self.assertEqual("accepted", result["serving_projection_status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual(len(server.adapter.serving_records), result["serving_projection_record_count"])
        raw = server.adapter.raw_records[0]
        serving = server.adapter.serving_records[0]
        task = server.adapter.serving_records[1]
        embedding_records = [
            record
            for record in server.adapter.serving_records
            if record.get("record_type") == "context_embedding"
        ]
        index_records = [
            record
            for record in server.adapter.serving_records
            if record.get("record_type") == "context_index"
        ]
        dirty_records = [
            record
            for record in server.adapter.serving_records
            if record.get("record_type") == "context_summary_dirty"
        ]
        self.assertEqual("agent_message", raw["record_type"])
        self.assertEqual("raw_agent_message", raw["raw_record_type"])
        self.assertEqual("backfill_only", raw["raw_ingestion_visibility"])
        self.assertEqual("user", raw["role"])
        self.assertEqual("real hooked Codex message", raw["text"])
        self.assertEqual("UserPromptSubmit", raw["codex_api_event"])
        self.assertNotIn("hook_type", raw)
        self.assertNotIn("hook_type", raw.get("agent_hook", {}))
        self.assertFalse(raw["serving_visible"])
        self.assertEqual("context_event", raw["serving_projection_record_type"])
        self.assertEqual(serving["event_id_hash"], raw["serving_context_event_hash"])
        self.assertEqual(serving["event_id_hash"], raw["serving_event_id_hash"])
        self.assertEqual(serving["event_id_hash"], raw["metadata"]["serving_projection"]["event_id_hash"])
        self.assertEqual("context_event", raw["metadata"]["serving_projection"]["record_type"])
        self.assertEqual("metadata_only_for_backfill_batching", raw["session_binding"])
        self.assertEqual("real hooked Codex message", raw["messages"][0]["content"])
        self.assertEqual(["selected_user_prompt"], raw["source_memory_selection_policies"])
        self.assertEqual({"selected_user_prompt": 1}, raw["source_memory_selection_policy_counts"])
        self.assertEqual("selected_user_prompt", raw["codex_memory_selection"]["policy"])
        self.assertEqual("codex-native-session-1", raw["scope"]["session_id"])
        self.assertNotIn("thread_id", raw)
        self.assertNotIn("turn_id", raw)
        self.assertEqual("context_event", serving["record_type"])
        self.assertEqual("user", serving["role"])
        self.assertEqual("UserPromptSubmit", serving["codex_api_event"])
        self.assertNotIn("hook_type", serving)
        self.assertEqual(["before_llm"], serving["source_hook_types"])
        self.assertEqual("codex-native-session-1", serving["session_id"])
        self.assertEqual("codex-native-session-1", serving["scope"]["session_id"])
        self.assertNotIn("thread_id", serving)
        self.assertNotIn("turn_id", serving)
        self.assertNotIn("conversation_id", serving["metadata"])
        self.assertNotIn("turn_id", serving["envelope"])
        self.assertEqual("UserPromptSubmit", serving["metadata"]["codex_event"])
        self.assertEqual(["selected_user_prompt"], serving["source_memory_selection_policies"])
        self.assertEqual({"selected_user_prompt": 1}, serving["source_memory_selection_policy_counts"])
        self.assertEqual("selected_user_prompt", serving["codex_memory_selection"]["policy"])
        self.assertEqual(["selected_user_prompt"], serving["envelope"]["source_memory_selection_policies"])
        self.assertEqual("PENDING_ASYNC_EXTRACTION", serving["classification"])
        self.assertEqual("pending_async", serving["event_type"])
        self.assertEqual("pending", serving["status"])
        self.assertEqual("session", serving["memory_scope"])
        self.assertEqual("same_session", serving["session_continuity"])
        self.assertEqual("pending_async", serving["extraction_phase"])
        self.assertEqual("async_pending", serving["internal_extraction"]["mode"])
        self.assertIn("real hooked Codex message", serving["summary_text"])
        self.assertIn("real hooked Codex message", serving["text"])
        self.assertEqual(1, len(embedding_records))
        self.assertEqual("event_text", embedding_records[0]["embedding_type"])
        self.assertEqual("session", embedding_records[0]["memory_scope"])
        self.assertEqual("same_session", embedding_records[0]["session_continuity"])
        self.assertNotIn("source_roles", embedding_records[0])
        self.assertNotIn("source_role_counts", embedding_records[0])
        self.assertNotIn("source_hook_types", embedding_records[0])
        self.assertNotIn("source_hook_type_counts", embedding_records[0])
        self.assertNotIn("source_codex_events", embedding_records[0])
        self.assertNotIn("source_codex_event_counts", embedding_records[0])
        self.assertNotIn("source_memory_scopes", embedding_records[0])
        self.assertNotIn("source_session_continuities", embedding_records[0])
        self.assertNotIn("source_extraction_phases", embedding_records[0])
        self.assertEqual("pending_async", embedding_records[0]["extraction_phase"])
        self.assertFalse(embedding_records[0]["final_session_boundary"])
        self.assertNotIn("source_memory_selection_policies", embedding_records[0])
        self.assertNotIn("source_memory_selection_policy_counts", embedding_records[0])
        self.assertNotIn("source_event_ids", embedding_records[0])
        self.assertEqual(["selected_user_prompt"], task["source_memory_selection_policies"])
        self.assertEqual({"selected_user_prompt": 1}, task["source_memory_selection_policy_counts"])
        self.assertTrue(
            any(record.get("index_name") == "memory_selection_policy:selected_user_prompt" for record in index_records),
            index_records,
        )
        self.assertEqual(serving["event_id_hash"], embedding_records[0]["ref_hash"])
        self.assertEqual("fast_hook_pending_async", embedding_records[0]["projection_phase"])
        self.assertTrue(index_records)
        self.assertTrue(all(record["ref_hash"] == serving["event_id_hash"] for record in index_records))
        self.assertTrue(all(record["data_model"] == "context_event" for record in index_records))
        self.assertTrue(all(record["projection_phase"] == "fast_hook_pending_async" for record in index_records))
        self.assertTrue(all(record["memory_scope"] == "session" for record in index_records))
        self.assertTrue(all(record["session_continuity"] == "same_session" for record in index_records))
        self.assertTrue(all(record["extraction_phase"] == "pending_async" for record in index_records))
        self.assertTrue(all(record["source_roles"] == ["user"] for record in index_records))
        self.assertTrue(all(record["source_memory_scopes"] == ["session", "user_profile"] for record in index_records))
        self.assertTrue(all(record["source_session_continuities"] == ["same_session", "cross_session"] for record in index_records))
        self.assertTrue(all(record["source_event_ids"] == [serving["event_id_hash"]] for record in index_records))
        self.assertIn("event_type:pending_async", {record["index_name"] for record in index_records})
        self.assertIn("source_role:user", {record["index_name"] for record in index_records})
        self.assertIn("codex_event:userpromptsubmit", {record["index_name"] for record in index_records})
        self.assertEqual("matrixark_async_pipeline_task", task["record_type"])
        self.assertEqual(serving["event_id_hash"], task["event_id_hash"])
        self.assertNotIn("thread_id", task)
        self.assertNotIn("turn_id", task)
        self.assertEqual("pending", task["status"])
        self.assertEqual(["extraction", "summary", "compression", "embedding"], task["stages"])
        self.assertEqual(task["task_hash"], result["async_pipeline_task_hash"])
        self.assertEqual(4, result["summary_dirty_count"])
        self.assertEqual(4, len(dirty_records))
        self.assertTrue(all(record["record_type"] == "context_summary_dirty" for record in dirty_records))
        self.assertTrue(all(record["source_event_hash"] == serving["event_id_hash"] for record in dirty_records))
        self.assertTrue(all(record["source_roles"] == ["user"] for record in dirty_records))
        self.assertTrue(all(record["source_role_counts"] == {"user": 1} for record in dirty_records))
        self.assertTrue(all(record["source_hook_types"] == ["before_llm"] for record in dirty_records))
        self.assertTrue(all(record["source_hook_type_counts"] == {"before_llm": 1} for record in dirty_records))
        self.assertTrue(all(record["source_codex_events"] == ["UserPromptSubmit"] for record in dirty_records))
        self.assertTrue(all(record["source_codex_event_counts"] == {"UserPromptSubmit": 1} for record in dirty_records))
        self.assertTrue(all(record["source_memory_scopes"] == ["session", "user_profile"] for record in dirty_records))
        self.assertTrue(all(record["source_session_continuities"] == ["same_session", "cross_session"] for record in dirty_records))
        self.assertTrue(all(record["source_extraction_phases"] == ["pending_async"] for record in dirty_records))
        self.assertTrue(all(record["source_memory_selection_policies"] == ["selected_user_prompt"] for record in dirty_records))
        self.assertTrue(all(record["source_memory_selection_policy_counts"] == {"selected_user_prompt": 1} for record in dirty_records))
        self.assertEqual([1, 2, 3, 4], [record["depth"] for record in dirty_records])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("turn-fast-1", server.adapter.session_buffer_records[0]["hook"]["turn_id"])
        self.assertTrue(result["session_buffer"]["registered"])

    def test_fast_async_hook_ingest_increments_raw_and_serving_for_all_message_roles(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.materialized_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.materialized_records.extend(records)

            def _append_many_materialized(self, records, *, allow_queue=True):
                self.materialized_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        server = Server()
        cases = [
            ("UserPromptSubmit", "user", "TemporalStore user prompt count marker"),
            ("PostToolUse", "tool", "Exit code: 0\nTemporalStore tool returned count marker"),
            ("Stop", "assistant", "TemporalStore assistant response count marker"),
        ]

        for event, role, text in cases:
            args = Namespace(
                event=event,
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-native-session-1",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            hook.fast_async_hook_ingest(
                server,
                args=args,
                text=text,
                role=role,
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )

        serving_events = [
            record
            for record in server.adapter.materialized_records
            if record.get("record_type") == "context_event"
        ]

        self.assertEqual(3, len(server.adapter.raw_records))
        self.assertEqual(3, len(serving_events))
        self.assertEqual(3, len(server.adapter.session_buffer_records))
        self.assertEqual(["assistant", "tool", "user"], sorted(record["role"] for record in server.adapter.raw_records))
        self.assertEqual(["assistant", "tool", "user"], sorted(record["role"] for record in serving_events))
        self.assertEqual(
            ["PostToolUse", "Stop", "UserPromptSubmit"],
            sorted(record["codex_api_event"] for record in server.adapter.raw_records),
        )
        self.assertEqual(
            ["PostToolUse", "Stop", "UserPromptSubmit"],
            sorted(record["codex_api_event"] for record in serving_events),
        )
        self.assertEqual(3, len({record["serving_event_id_hash"] for record in server.adapter.raw_records}))
        self.assertEqual(3, len({record["event_id_hash"] for record in serving_events}))

    def test_fast_async_hook_ingest_reports_raw_append_fallback_as_accepted(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []

            def _append_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-raw-fallback-session",
            team="codex",
            project="temporalstore",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="fallback raw append still persisted",
            role="user",
            agent_context={},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("accepted", result["raw_ingestion_status"])
        self.assertEqual("accepted", result["serving_projection_status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual(len(server.adapter.serving_records), result["serving_projection_record_count"])
        self.assertTrue(any(record.get("record_type") == "context_embedding" for record in server.adapter.serving_records))
        self.assertTrue(any(record.get("record_type") == "context_index" for record in server.adapter.serving_records))

    def test_fast_async_hook_ingest_runs_threshold_batch_commit(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}, {"event_id_hash": 2}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "accepted",
                    "commit_reason": "threshold",
                    "trigger_policy": "threshold",
                    "extraction_context_event_count": 2,
                    "segments_written": 1,
                    "entities_written": 2,
                    "profile_entities_written": 1,
                    "indexes_written": 3,
                    "summary_refresh": {
                        "status": "dirty_marked",
                        "dirty_hashes": [11, 12],
                        "profile_summary_refresh_required": True,
                    },
                    "source_roles": ["user", "assistant"],
                    "source_hook_types": ["before_llm", "after_llm"],
                    "source_codex_events": ["UserPromptSubmit", "Stop"],
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-session-threshold",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=300000,
                understanding_provider="rules",
                extraction_provider="rules",
                segment_provider="deterministic",
                segment_model="codex-memory-segmenter",
                segment_model_path="/models/codex-memory-segmenter",
                segment_max_new_tokens=128,
                segment_provider_fallback="deterministic",
                skip_prior_context=True,
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="batch me into entities",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("accepted", result["auto_batch_extract_result"]["status"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("threshold", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(2, commit_args["threshold_messages"])
        self.assertEqual(2, commit_args["max_messages"])
        self.assertEqual("rules", commit_args["understanding_provider"])
        self.assertEqual("rules", commit_args["extraction_provider"])
        self.assertEqual("deterministic", commit_args["segment_provider"])
        self.assertEqual("codex-memory-segmenter", commit_args["segment_model"])
        self.assertEqual("/models/codex-memory-segmenter", commit_args["segment_model_path"])
        self.assertEqual(128, commit_args["segment_max_new_tokens"])
        self.assertEqual("deterministic", commit_args["segment_provider_fallback"])
        self.assertTrue(commit_args["skip_prior_context"])
        self.assertEqual("session_commit", commit_hook["hook_type"])
        decision = hook.auto_batch_decision_summary(result)
        self.assertEqual("committed", decision["decision"])
        self.assertEqual("threshold", decision["reason"])
        self.assertEqual(2, decision["pending_before_ingest_count"])
        self.assertEqual(2, decision["pending_after_ingest_count"])
        self.assertTrue(decision["commit_after_current_ingest"])
        self.assertEqual(2, decision["memory_layers_written"]["context_events"])
        self.assertEqual(2, decision["memory_layers_written"]["session_entities"])
        self.assertEqual(1, decision["memory_layers_written"]["profile_entities"])
        self.assertEqual(3, decision["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(2, decision["memory_layers_written"]["summary_dirty_nodes"])
        self.assertTrue(decision["summary_refresh"]["profile_summary_refresh_required"])
        self.assertTrue(decision["trigger_evidence"]["threshold_ready"])
        self.assertFalse(decision["trigger_evidence"]["idle_ready"])
        self.assertEqual(2, decision["trigger_evidence"]["pending_event_count"])
        self.assertEqual(2, decision["trigger_evidence"]["threshold_messages"])
        self.assertEqual(["before_llm", "after_llm"], decision["source_hook_types"])

    def test_fast_async_hook_ingest_preflushes_idle_tail_before_next_prompt(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []
                self.pending = [
                    {
                        "event_id_hash": 77,
                        "envelope": {"ingestion_time_ms": 1},
                    }
                ]

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)
                self.pending.append(
                    {
                        "event_id_hash": kwargs["event_id_hash"],
                        "envelope": kwargs["envelope"],
                    }
                )

            def pending_session_events(self, scope):
                return list(self.pending)

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                committed = list(self.pending)
                self.pending.clear()
                return {
                    "status": "committed",
                    "commit_reason": "idle_timeout",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": len(committed),
                    "source_event_ids": [record["event_id_hash"] for record in committed],
                    "extraction_context_event_count": len(committed),
                    "segments_written": 1,
                    "entities_written": 1,
                    "indexes_written": 2,
                    "summary_refresh": {
                        "status": "dirty_marked",
                        "dirty_hashes": [77],
                        "profile_summary_refresh_required": False,
                    },
                    "trigger_evidence": {
                        "pending_event_count": len(committed),
                        "idle_ready": True,
                    },
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-session-idle-next-prompt",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=1,
                understanding_provider="rules",
                extraction_provider="rules",
                segment_provider="deterministic",
                segment_model="codex-memory-idle-segmenter",
                segment_model_path="/models/codex-memory-idle-segmenter",
                segment_max_new_tokens=64,
                segment_provider_fallback="deterministic",
                skip_prior_context=True,
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="new prompt should not be mixed into the prior idle batch",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={
                    "session_id_source": "payload_field",
                    "thread_id": "thread-idle-preflush",
                    "turn_id": "turn-new-prompt",
                },
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("committed", result["idle_commit_result"]["status"])
        self.assertEqual("idle_timeout", result["idle_commit_result"]["trigger_policy"])
        self.assertTrue(result["session_buffer"]["pre_ingest_idle_ready"])
        self.assertGreaterEqual(result["session_buffer"]["pre_ingest_idle_elapsed_ms"], 1)
        self.assertEqual(1, result["session_buffer"]["pending_before_ingest_count"])
        self.assertEqual(1, result["session_buffer"]["pending_after_ingest_count"])
        self.assertFalse(result["session_buffer"]["commit_after_current_ingest"])
        self.assertEqual(result["idle_commit_result"], result["auto_batch_extract_result"])
        decision = hook.auto_batch_decision_summary(result)
        self.assertEqual("idle_commit", decision["decision"])
        self.assertEqual("committed", decision["auto_batch_extract_status"])
        self.assertEqual("idle_timeout", decision["reason"])
        self.assertTrue(decision["pre_ingest_idle_ready"])
        self.assertGreaterEqual(decision["pre_ingest_idle_elapsed_ms"], 1)
        self.assertEqual(1, decision["pending_before_ingest_count"])
        self.assertEqual(1, decision["pending_after_ingest_count"])
        self.assertFalse(decision["commit_after_current_ingest"])
        self.assertEqual(1, decision["memory_layers_written"]["context_events"])
        self.assertEqual(1, decision["memory_layers_written"]["session_entities"])
        self.assertEqual(2, decision["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(1, decision["memory_layers_written"]["summary_dirty_nodes"])
        self.assertEqual("dirty_marked", decision["summary_refresh"]["status"])
        self.assertTrue(decision["trigger_evidence"]["idle_ready"])
        self.assertEqual(1, decision["trigger_evidence"]["pending_event_count"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(1, commit_args["idle_timeout_ms"])
        self.assertEqual("rules", commit_args["understanding_provider"])
        self.assertEqual("rules", commit_args["extraction_provider"])
        self.assertEqual("deterministic", commit_args["segment_provider"])
        self.assertEqual("codex-memory-idle-segmenter", commit_args["segment_model"])
        self.assertEqual("/models/codex-memory-idle-segmenter", commit_args["segment_model_path"])
        self.assertEqual(64, commit_args["segment_max_new_tokens"])
        self.assertEqual("deterministic", commit_args["segment_provider_fallback"])
        self.assertTrue(commit_args["skip_prior_context"])
        self.assertEqual("idle_timeout_before_ingest", commit_hook["trigger"])
        self.assertNotIn("thread_id", commit_hook)
        self.assertNotIn("turn_id", commit_hook)
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("new prompt should not be mixed into the prior idle batch", server.adapter.session_buffer_records[0]["envelope"]["messages"][0]["content"])
        self.assertEqual(1, result["session_buffer"]["pending_event_count"])

    def test_fast_async_hook_ingest_commits_assistant_stop_boundary(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {"status": "accepted", "entities_written": 1}

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-stop",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=300000,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Done. Commit d0152479 pushed and tests passed.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field", "thread_id": "thread-stop-1", "turn_id": "turn-stop-1"},
        )

        self.assertEqual("accepted", result["session_commit"]["status"])
        self.assertEqual("accepted", result["auto_batch_extract_result"]["status"])
        self.assertEqual(result["session_commit"], hook.fast_async_boundary_commit_from_ingest(result))
        decision = hook.auto_batch_decision_summary(result)
        self.assertEqual("boundary_commit", decision["decision"])
        self.assertEqual("hook_boundary", decision["reason"])
        self.assertEqual("accepted", decision["auto_batch_extract_status"])
        self.assertTrue(decision["boundary_commit_requested"])
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="Stop",
            session_id_source="payload_field",
            agent_context={"workspace_root": "/repo"},
            ingest=result,
            commit=hook.fast_async_boundary_commit_from_ingest(result),
        )
        self.assertEqual("accepted", output["ingest"]["auto_batch_extract_status"])
        self.assertEqual("boundary_commit", output["ingest"]["auto_batch_extract_decision"]["decision"])
        self.assertEqual("accepted", output["session_commit"]["status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        raw_record = server.adapter.raw_records[0]
        self.assertEqual("assistant", raw_record["messages"][0]["role"])
        self.assertNotIn("thread_id", raw_record)
        self.assertNotIn("turn_id", raw_record)
        self.assertEqual("assistant", raw_record["source_role"])
        self.assertNotIn("hook_type", raw_record)
        self.assertNotIn("hook_type", raw_record.get("agent_hook", {}))
        self.assertEqual("Stop", raw_record["codex_event"])
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("assistant", serving_event["source_role"])
        self.assertEqual(["after_llm"], serving_event["source_hook_types"])
        self.assertEqual("Stop", serving_event["codex_event"])
        self.assertEqual("assistant", serving_event["envelope"]["source_role"])
        self.assertNotIn("hook_type", serving_event["envelope"])
        self.assertNotIn("thread_id", serving_event)
        self.assertNotIn("turn_id", serving_event)
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("hook_boundary", commit_args["commit_reason"])
        self.assertTrue(commit_args["force"])
        self.assertNotIn("thread_id", commit_hook)
        self.assertNotIn("turn_id", commit_hook)

