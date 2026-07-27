#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import matrixark_codex_hook as hook
import matrixark_http
from matrixark_mcp_core import memory_hierarchy_contract_from_recall_policy
from matrixark_codex_hook_payload import decode_payload, extract_identity, extract_prompt


class MatrixArkCodexHookOutputTest(unittest.TestCase):
    def test_loose_stop_payload_extracts_current_input_message_and_thread_identity(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8cb5-b4d5-77f2-8c82-0499440da36f,'
            'turn-id:019f8cd9-7669-7891-ab92-7353efe82e4d,'
            'cwd:C:\\Users\\Deeproute\\Documents\\Codex,client:Codex Desktop,'
            'input-messages:[older prompt,'
            '<codex_delegation>\\n <input>fresh old task prompt should win</input>\\n</codex_delegation>]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual("fresh old task prompt should win", extract_prompt(payload, event="Stop"))
        identity = extract_identity(payload)
        self.assertEqual("codex:019f8cb5-b4d5-77f2-8c82-0499440da36f", identity["session_id"])
        self.assertEqual("019f8cb5-b4d5-77f2-8c82-0499440da36f", identity["thread_id"])
        self.assertEqual("019f8cd9-7669-7891-ab92-7353efe82e4d", identity["turn_id"])

    def test_json_user_prompt_payload_extracts_prompt_and_session_identity(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "turn_id": "019f-turn",
                    "prompt": "natural current prompt",
                }
            ).encode("utf-8")
        )

        self.assertEqual("natural current prompt", extract_prompt(payload, event="UserPromptSubmit"))
        identity = extract_identity(payload)
        self.assertEqual("codex:019efad7-2f87-77e1-b082-c294fcb5e731", identity["session_id"])
        self.assertEqual("019efad7-2f87-77e1-b082-c294fcb5e731", identity["thread_id"])
        self.assertEqual("019f-turn", identity["turn_id"])

    def test_codex_agent_hook_preserves_thread_and_turn_lineage(self) -> None:
        payload = {"thread_id": "thread-main-1", "turn_id": "turn-main-1", "conversation_id": "conversation-main-1"}
        args = Namespace(session_id="codex:thread-main-1")

        identity = hook.codex_hook_lineage_from_payload(payload, args, session_id_source="payload_field")
        agent_hook = hook.codex_agent_hook(
            hook_type="before_llm",
            hook_id="UserPromptSubmit:1",
            idempotency_key="turn-main-1",
            trigger="UserPromptSubmit",
            session_id_source="payload_field",
            identity=identity,
            observed_at_ms=123,
        )

        self.assertEqual("codex:thread-main-1", identity["session_id"])
        self.assertEqual("thread-main-1", agent_hook["thread_id"])
        self.assertEqual("turn-main-1", agent_hook["turn_id"])
        self.assertEqual("conversation-main-1", agent_hook["conversation_id"])

    def test_payload_text_flattens_structured_assistant_content_parts(self) -> None:
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": "Decision: ingest assistant responses."},
                        {"type": "text", "text": "Next: extract profile entities on idle timeout."},
                    ],
                }
            ]
        }

        self.assertEqual(
            "Decision: ingest assistant responses.\nNext: extract profile entities on idle timeout.",
            hook.payload_text(payload),
        )

    def test_rollout_assistant_extraction_flattens_structured_content_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rollout = Path(tmp_dir) / "rollout-test.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {"type": "output_text", "text": "Committed hook batching fix."},
                                {"type": "text", "text": "Tests passed and origin/main was pushed."},
                            ],
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            self.assertEqual(
                "Committed hook batching fix.\nTests passed and origin/main was pushed.",
                hook._extract_assistant_text_from_rollout(rollout),
            )

    def test_user_prompt_payload_preserves_explicit_thread_identity(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "thread_id": "019f8d12-86c6-7100-9a44-7537cdd30aec",
                    "turn_id": "019f-turn",
                    "prompt": "delegated prompt",
                }
            ).encode("utf-8")
        )

        identity = extract_identity(payload)
        self.assertEqual("codex:019efad7-2f87-77e1-b082-c294fcb5e731", identity["session_id"])
        self.assertEqual("019f8d12-86c6-7100-9a44-7537cdd30aec", identity["thread_id"])

    def test_user_prompt_payload_unwraps_delegation_input(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "turn_id": "019f-turn",
                    "prompt": (
                        "<codex_delegation>\n"
                        "  <source_thread_id>019f8d12-86c6-7100-9a44-7537cdd30aec</source_thread_id>\n"
                        "  <input>clean delegated prompt</input>\n"
                        "</codex_delegation>"
                    ),
                }
            ).encode("utf-8")
        )

        self.assertEqual("clean delegated prompt", extract_prompt(payload, event="UserPromptSubmit"))

    def test_loose_stop_payload_preserves_single_prompt_with_commas(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8d12-86c6-7100-9a44-7537cdd30aec,'
            'input-messages:[QUERY TOP 10 MESSAGES FROM C++ AND RUST TEMPORALSTORE WITH DETAILS TO COMPARE, '
            'MAKE SURE CURRENT THREAD IS FETCHED]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertIn(
            "QUERY TOP 10 MESSAGES FROM C++ AND RUST TEMPORALSTORE",
            extract_prompt(payload, event="Stop"),
        )
        identity = extract_identity(payload)
        self.assertEqual("codex:019f8d12-86c6-7100-9a44-7537cdd30aec", identity["session_id"])

    def test_loose_stop_payload_extracts_latest_newline_separated_input_message(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019efad7-2f87-77e1-b082-c294fcb5e731,'
            'input-messages:[inference for llm\\n,'
            'vllm, sglang, others for comparison\\n,'
            'MatrixArk natural live acceptance after launcher bash fix]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual(
            "MatrixArk natural live acceptance after launcher bash fix",
            extract_prompt(payload, event="Stop"),
        )

    def test_loose_stop_payload_keeps_delegations_after_bracketed_tail(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019efad7-2f87-77e1-b082-c294fcb5e731,'
            'input-messages:[inference for llm\\n,vllm, sglang\\n,deep dive into vllm and sglang\\n,]\\n,'
            '<codex_delegation>\\n'
            ' <source_thread_id>019f8d12-86c6-7100-9a44-7537cdd30aec</source_thread_id>\\n'
            ' <input>MatrixArk trace live add-llm payload capture 1784782700: reply with one concise sentence about hook payloads.</input>\\n'
            '</codex_delegation>]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual(
            "MatrixArk trace live add-llm payload capture 1784782700: reply with one concise sentence about hook payloads.",
            extract_prompt(payload, event="Stop"),
        )

    def test_loose_stop_payload_prefers_plain_prompt_after_initial_delegation(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8d12-86c6-7100-9a44-7537cdd30aec,'
            'input-messages:[<codex_delegation>\\n'
            ' <source_thread_id>019ea4c9-a88c-71e1-baca-df7ff879e020</source_thread_id>\\n'
            ' <input>initial delegated task prompt</input>\\n'
            '</codex_delegation>\\n,'
            'REBASE ALL CHANGES\\n,'
            'COMMIT ALL CHANGES TO REMOTE MAIN\\n,'
            'test again in current session\\n,]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual("test again in current session", extract_prompt(payload, event="Stop"))

    def test_env_thread_identity_fallback_when_payload_has_no_session(self) -> None:
        identity = extract_identity(
            {"hook_event_name": "Stop", "transcript_path": "ignored.jsonl"},
            env={"CODEX_THREAD_ID": "019f8d12-86c6-7100-9a44-7537cdd30aec"},
        )

        self.assertEqual("codex:019f8d12-86c6-7100-9a44-7537cdd30aec", identity["session_id"])
        self.assertEqual("019f8d12-86c6-7100-9a44-7537cdd30aec", identity["thread_id"])
        self.assertEqual("env", identity["session_id_source"])

    def test_hook_trace_records_output_summary(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.records = []

            def append(self, record):
                self.records.append(record)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            backend="temporalstore-rust",
            storage_prefix="matrixark:codex-hook:rust-live-v2",
            session_id="codex-session-1",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            team="codex",
            project="temporalstore",
        )
        trace = hook.begin_hook_trace(
            args=args,
            payload={"prompt": "trace me", "session_id": "codex-session-1"},
            text="trace me",
            session_id_source="payload_field",
        )
        server = Server()
        hook.append_hook_trace(
            server,
            trace,
            output={
                "hookSpecificOutput": {"additionalContext": "remote memory"},
                "retrieve": {
                    "context_pack_id": "pack-1",
                    "selected_ref_count": 2,
                    "budget": {"remote_context_budget_tokens": 100, "used_remote_context_tokens": 12},
                    "layers": {
                        "selected_ref_counts": {"event": 1, "entity": 1},
                        "same_session_refs": 1,
                        "cross_session_refs": 1,
                        "entity_bridge_refs": 1,
                        "profile_memory_refs": 1,
                        "memory_layer_budget": {
                            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 9}},
                            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 9}},
                            "by_extraction_phase": {"final": {"refs": 1, "tokens": 9}},
                            "final_session_boundary_ref_count": 1,
                        },
                    },
                    "async_pipeline_readiness": {
                        "task_count": 2,
                        "ready_for_retrieval": False,
                        "remaining_stages": ["entity", "summary"],
                        "freshness_warnings": ["async_pipeline_followup_pending"],
                    },
                    "memory_hierarchy": {
                        "models": {
                            "profile_entity": {
                                "record_type": "context_entity",
                                "memory_scope": "user_profile",
                                "session_continuity": "cross_session",
                            }
                        },
                        "retrieval_strategy": "same-session first, profile entity bridge second",
                    },
                    "budget_pressure": {
                        "budget_pressure": True,
                        "dropped_by_reason": {"over_budget": 2},
                        "estimated_tokens_by_reason": {"over_budget": 120},
                    },
                    "rendered_context_chars": 37,
                },
                "ingest": {
                    "status": "accepted",
                    "auto_batch_extract_status": "committed",
                    "auto_batch_extract": {
                        "status": "committed",
                        "commit_reason": "threshold",
                        "trigger_policy": "threshold",
                        "memory_layers_written": {
                            "context_events": 2,
                            "session_entities": 3,
                            "profile_entities": 1,
                            "secondary_indexes": 5,
                            "summary_dirty_nodes": 2,
                        },
                    },
                    "auto_batch_extract_decision": {
                        "decision": "committed",
                        "reason": "threshold",
                        "memory_layers_written": {
                            "context_events": 2,
                            "session_entities": 3,
                            "profile_entities": 1,
                            "secondary_indexes": 5,
                            "summary_dirty_nodes": 2,
                        },
                        "summary_refresh": {
                            "status": "dirty_marked",
                            "dirty_hashes": [7, 8],
                            "profile_summary_refresh_required": True,
                        },
                    },
                },
                "session_commit": {
                    "status": "committed",
                    "commit_id_hash": 42,
                    "commit_reason": "idle_timeout",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": 1,
                    "extraction_context_event_count": 2,
                    "source_roles": ["tool"],
                    "source_hook_types": ["hook_boundary"],
                    "source_codex_events": ["PostToolUse"],
                    "profile_promotion_summary": [
                        {
                            "profile_entity_hash": 701,
                            "session_entity_hash": 601,
                            "entity_type": "tool_evidence",
                            "entity_name": "pytest",
                            "source_session_ids": ["codex-session-1"],
                            "source_entity_count": 1,
                            "source_roles": ["tool"],
                            "source_hook_types": ["hook_boundary"],
                            "source_codex_events": ["PostToolUse"],
                        }
                    ],
                    "segments_written": 1,
                    "entities_written": 3,
                    "profile_entities_written": 1,
                    "indexes_written": 5,
                    "summary_refresh": {"status": "dirty_marked", "dirty_hashes": [7, 8]},
                    "trigger_evidence": {
                        "pending_event_count": 1,
                        "threshold_messages": 20,
                        "threshold_ready": False,
                        "idle_timeout_ms": 0,
                        "idle_ready": True,
                        "force": False,
                        "commit_reason": "idle_timeout",
                    },
                },
            },
        )

        self.assertEqual(1, len(server.adapter.records))
        record = server.adapter.records[0]
        self.assertEqual("codex_hook_trace", record["record_type"])
        self.assertEqual("temporalstore-rust", record["backend"])
        self.assertEqual("UserPromptSubmit", record["event"])
        self.assertEqual("payload_field", record["session_id_source"])
        self.assertEqual(["prompt", "session_id"], record["payload_keys"])
        self.assertEqual("ok", record["status"])
        self.assertEqual("pack-1", record["output_summary"]["context_pack_id"])
        self.assertEqual(100, record["output_summary"]["retrieval_budget"]["remote_context_budget_tokens"])
        self.assertEqual({"over_budget": 2}, record["output_summary"]["retrieval_budget_pressure"]["dropped_by_reason"])
        self.assertEqual(
            "user_profile",
            record["output_summary"]["memory_hierarchy"]["models"]["profile_entity"]["memory_scope"],
        )
        self.assertEqual({"event": 1, "entity": 1}, record["output_summary"]["retrieval_layers"]["selected_ref_counts"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["cross_session_refs"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["profile_memory_refs"])
        self.assertEqual(
            1,
            record["output_summary"]["retrieval_layers"]["memory_layer_budget"]["final_session_boundary_ref_count"],
        )
        self.assertEqual(2, record["output_summary"]["async_pipeline_readiness"]["task_count"])
        self.assertFalse(record["output_summary"]["async_pipeline_readiness"]["ready_for_retrieval"])
        self.assertEqual(37, record["output_summary"]["rendered_context_chars"])
        self.assertTrue(record["output_summary"]["strict_additional_context_emitted"])
        self.assertEqual("committed", record["output_summary"]["auto_batch_extract_status"])
        auto_batch = record["output_summary"]["auto_batch_extract"]
        self.assertEqual("threshold", auto_batch["trigger_policy"])
        self.assertEqual(3, auto_batch["memory_layers_written"]["session_entities"])
        auto_batch_decision = record["output_summary"]["auto_batch_extract_decision"]
        self.assertEqual("committed", auto_batch_decision["decision"])
        self.assertEqual("threshold", auto_batch_decision["reason"])
        self.assertEqual(1, auto_batch_decision["memory_layers_written"]["profile_entities"])
        self.assertTrue(auto_batch_decision["summary_refresh"]["profile_summary_refresh_required"])
        commit_summary = record["output_summary"]["session_commit"]
        self.assertEqual("idle_timeout", commit_summary["trigger_policy"])
        self.assertEqual("provisional", commit_summary["extraction_phase"])
        self.assertFalse(commit_summary["final_session_boundary"])
        self.assertEqual(1, commit_summary["source_event_count"])
        self.assertEqual(2, commit_summary["extraction_context_event_count"])
        self.assertEqual(["tool"], commit_summary["source_roles"])
        self.assertEqual(["hook_boundary"], commit_summary["source_hook_types"])
        self.assertEqual(["PostToolUse"], commit_summary["source_codex_events"])
        self.assertEqual(701, commit_summary["profile_promotion_summary"][0]["profile_entity_hash"])
        self.assertEqual(["codex-session-1"], commit_summary["profile_promotion_summary"][0]["source_session_ids"])
        self.assertEqual(2, commit_summary["memory_layers_written"]["context_events"])
        self.assertEqual(1, commit_summary["memory_layers_written"]["segments"])
        self.assertEqual(3, commit_summary["memory_layers_written"]["session_entities"])
        self.assertEqual(1, commit_summary["memory_layers_written"]["profile_entities"])
        self.assertEqual(3, commit_summary["memory_layers_written"]["same_session_entities"])
        self.assertEqual(1, commit_summary["memory_layers_written"]["cross_session_entities"])
        self.assertEqual(5, commit_summary["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(2, commit_summary["memory_layers_written"]["summary_dirty_nodes"])
        self.assertEqual("dirty_marked", commit_summary["memory_layers_written"]["summary_refresh_status"])
        self.assertEqual("provisional", commit_summary["memory_layers_written"]["extraction_phase"])
        self.assertFalse(commit_summary["memory_layers_written"]["final_session_boundary"])
        self.assertTrue(commit_summary["trigger_evidence"]["idle_ready"])
        self.assertFalse(commit_summary["trigger_evidence"]["threshold_ready"])

    def test_retrieve_tool_call_trace_records_budget_and_layers(self) -> None:
        class Server:
            def handle(self, request):
                self.request = request
                result = {
                    "context_pack_id": "pack-tool",
                    "used_context_tokens": 33,
                    "remote_context_budget_tokens": 90,
                    "context": "user: rendered profile decision",
                    "selected_ref_counts": {"event": 2, "entity": 1},
                    "retrieval_metrics": {
                        "memory_layer_budget": {
                            "by_memory_scope": {
                                "session": {"refs": 1, "tokens": 4},
                                "user_profile": {"refs": 1, "tokens": 3},
                            },
                            "by_session_continuity": {
                                "same_session": {"refs": 1, "tokens": 4},
                                "cross_session": {"refs": 1, "tokens": 3},
                            },
                            "by_extraction_phase": {
                                "provisional": {"refs": 1, "tokens": 4},
                                "final": {"refs": 1, "tokens": 3},
                            },
                            "final_session_boundary_ref_count": 1,
                        },
                        "async_pipeline_readiness": {
                            "task_count": 3,
                            "ready_for_retrieval": False,
                            "remaining_stages": ["entity", "secondary_index", "summary"],
                            "freshness_warnings": ["entity_extraction_pending"],
                        },
                    },
                    "recall_policy": {
                        "session_continuity": {
                            "mode": "prefer",
                            "policy": "same-session continuity first; entity state bridges cross-session memory",
                        },
                        "cross_session": {
                            "enabled": True,
                            "budget_tokens": 18,
                            "remote_budget_tokens": 90,
                            "computed_budget_tokens": 18,
                            "budget_floor_tokens": 256,
                            "budget_floor_applied": False,
                            "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                            "max_sessions": 3,
                            "max_candidates": 24,
                        },
                        "shared_context": {"enabled": True},
                    },
                    "dropped_refs": {
                        "over_budget": 2,
                        "cross_session_budget": 1,
                        "estimated_tokens": {"over_budget": 44, "cross_session_budget": 12},
                        "budget_fill_policy": "quality_first",
                    },
                    "selected_refs": [
                        {
                            "ref_type": "event",
                            "memory_scope": "session",
                            "session_continuity": "same_session",
                            "text": (
                                "user: Codex hook heartbeat 2026-07-15T13:32:00Z: "
                                "C++ TemporalStore is live and accepting MatrixArk hook writes."
                            ),
                        },
                        {
                            "ref_type": "event",
                            "memory_scope": "session",
                            "session_continuity": "same_session",
                            "text": "current turn evidence",
                        },
                        {
                            "ref_type": "entity",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "text": "profile decision",
                        },
                    ],
                }
                return {"result": {"content": [{"text": json.dumps(result)}]}}

        trace = {"tool_calls": []}
        result = hook.trace_tool_call(Server(), "matrixark_retrieve", {"query": "profile decision"}, trace)

        self.assertEqual("pack-tool", result["context_pack_id"])
        self.assertEqual(1, len(trace["tool_calls"]))
        item = trace["tool_calls"][0]
        self.assertEqual("ok", item["status"])
        self.assertEqual(2, item["result"]["selected_ref_count"])
        self.assertEqual(90, item["result"]["retrieval_budget"]["remote_context_budget_tokens"])
        self.assertEqual(57, item["result"]["retrieval_budget"]["remote_budget_remaining_tokens"])
        self.assertEqual(
            "local_first_remote_fill_remaining",
            item["result"]["retrieval_budget"]["budget_contract"]["mode"],
        )
        self.assertTrue(item["result"]["retrieval_budget"]["budget_contract"]["contract_holds"])
        self.assertEqual({"event": 1, "entity": 1}, item["result"]["retrieval_layers"]["selected_ref_counts"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["same_session_refs"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["cross_session_refs"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["profile_memory_refs"])
        self.assertEqual(
            1,
            item["result"]["retrieval_layers"]["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
        )
        self.assertEqual(3, item["result"]["async_pipeline_readiness"]["task_count"])
        self.assertEqual(
            ["entity", "secondary_index", "summary"],
            item["result"]["retrieval_layers"]["async_pipeline_readiness"]["remaining_stages"],
        )
        pressure = item["result"]["retrieval_budget_pressure"]
        self.assertTrue(pressure["budget_pressure"])
        self.assertEqual({"over_budget": 2, "cross_session_budget": 1}, pressure["dropped_by_reason"])
        self.assertEqual({"over_budget": 44, "cross_session_budget": 12}, pressure["estimated_tokens_by_reason"])
        self.assertEqual(3, pressure["budget_pressure_reason_count"])
        hierarchy = item["result"]["memory_hierarchy"]
        self.assertEqual("context_entity", hierarchy["models"]["session_entity"]["record_type"])
        self.assertEqual("user_profile", hierarchy["models"]["profile_entity"]["memory_scope"])
        self.assertEqual("context_profile_entity", hierarchy["models"]["profile_index"]["data_model"])
        self.assertEqual("prefer", hierarchy["session_scope_mode"])
        self.assertTrue(hierarchy["cross_session_enabled"])
        self.assertEqual(18, hierarchy["cross_session_budget_tokens"])
        self.assertEqual(90, hierarchy["cross_session_remote_budget_tokens"])
        self.assertEqual(18, hierarchy["cross_session_computed_budget_tokens"])
        self.assertEqual(256, hierarchy["cross_session_budget_floor_tokens"])
        self.assertFalse(hierarchy["cross_session_budget_floor_applied"])
        self.assertEqual(
            "remote_budget_too_small_for_profile_floor",
            hierarchy["cross_session_budget_floor_status"],
        )
        self.assertIn("profile_entity_bridge", hierarchy["selected_ref_flow"])
        self.assertEqual(len("user: rendered profile decision"), item["result"]["rendered_context_chars"])

    def test_codex_hierarchy_contract_delegates_to_core_contract(self) -> None:
        recall_policy = {
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
        }

        self.assertEqual(
            memory_hierarchy_contract_from_recall_policy(recall_policy),
            hook.retrieval_memory_hierarchy_contract_from_retrieve({"recall_policy": recall_policy}),
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
                        "source_roles": ["assistant", "user"],
                        "source_hook_types": ["before_llm", "after_llm"],
                        "source_codex_events": ["Stop", "UserPromptSubmit"],
                        "source_role_counts": {"assistant": 1, "user": 1},
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
                                "source_roles": ["assistant", "user"],
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
        self.assertEqual({"assistant": 1, "user": 1}, auto_batch["source_role_counts"])
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
        self.assertEqual({"assistant": 1, "user": 1}, decision["source_role_counts"])
        self.assertEqual({"before_llm": 1, "after_llm": 1}, decision["source_hook_type_counts"])
        self.assertEqual({"Stop": 1, "UserPromptSubmit": 1}, decision["source_codex_event_counts"])
        self.assertEqual(801, decision["profile_promotion_summary"][0]["profile_entity_hash"])

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
        self.assertNotIn("batch me into entities", json.dumps(decision))

    def test_user_prompt_emit_codex_additional_context_from_selected_refs(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="payload_field",
            agent_context={"local_context": [{"ref": "src/main.cc", "text": "local code"}], "workspace_root": "/repo"},
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
                },
                "local_context_policy": {"local_context_count": 1},
                "retrieval_metrics": {
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
                            "assistant": 2,
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
                            "assistant": {"refs": 1, "tokens": 14},
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
                            "assistant": {"refs": 1, "tokens": 22},
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
                        "pressure_bucket_count": 2,
                        "dropped_bucket_count": 5,
                    },
                },
                "dropped_refs": {
                    "cross_session_budget": 2,
                    "max_selected_refs": 3,
                    "estimated_tokens": {"cross_session_budget": 21, "max_selected_refs": 55},
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
        self.assertEqual(
            "context_profile_entity",
            output["retrieve"]["memory_hierarchy"]["models"]["profile_index"]["data_model"],
        )
        self.assertTrue(output["retrieve"]["memory_hierarchy"]["cross_session_enabled"])
        self.assertEqual(64, output["retrieve"]["memory_hierarchy"]["cross_session_budget_tokens"])
        self.assertEqual(
            "remote_budget_too_small_for_profile_floor",
            output["retrieve"]["memory_hierarchy"]["cross_session_budget_floor_status"],
        )
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
        self.assertTrue(output["retrieve"]["layers"]["memory_layer_pressure"]["assistant_memory_pressure"])
        self.assertEqual(58, output["retrieve"]["budget"]["remote_budget_remaining_tokens"])
        self.assertFalse(output["retrieve"]["budget"]["remote_budget_overrun"])
        self.assertEqual("payload_field", output["retrieve"]["session_identity"]["session_id_source"])
        self.assertTrue(output["retrieve"]["session_identity"]["strong_session_identity"])
        self.assertFalse(output["retrieve"]["session_identity"]["fallback_session_identity"])
        self.assertEqual(4, output["retrieve"]["async_pipeline_readiness"]["task_count"])
        self.assertFalse(output["retrieve"]["async_pipeline_readiness"]["ready_for_retrieval"])
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
        self.assertIn("Budget summary:", additional)
        self.assertIn("remote_budget=100", additional)
        self.assertIn("remote_remaining=58", additional)
        self.assertIn("budget_source=agent_provided_max_context_tokens", additional)
        self.assertIn("contract=local_first_remote_fill_remaining", additional)
        self.assertIn("contract_holds=true", additional)
        self.assertIn("Session identity:", additional)
        self.assertIn("source=payload_field", additional)
        self.assertIn("strong=true", additional)
        self.assertIn("fallback=false", additional)
        self.assertIn("Layer summary:", additional)
        self.assertIn("event=1", additional)
        self.assertIn("entity=1", additional)
        self.assertIn("summary=1", additional)
        self.assertIn("same_session_refs=1", additional)
        self.assertIn("cross_session_refs=2", additional)
        self.assertIn("entity_bridge_refs=1", additional)
        self.assertIn("local_context_refs=1", additional)
        self.assertIn("memory_layer_budget:", additional)
        self.assertIn("memory_layer_pressure:", additional)
        self.assertIn("selected=3", additional)
        self.assertIn("dropped=5", additional)
        self.assertIn("flags[profile,cross_session,final,assistant,tool]", additional)
        self.assertIn("pressure_dimensions[by_memory_scope,by_source_role]", additional)
        self.assertIn("scope[session=1/12t, user_profile=2/30t]", additional)
        self.assertIn("continuity[cross_session=2/30t, same_session=1/12t]", additional)
        self.assertIn("phase[final=2/30t, provisional=1/12t]", additional)
        self.assertIn("ref_type[entity=1/18t, summary=1/12t]", additional)
        self.assertIn("entity_type[assistant_decision=1/14t, tool_evidence=1/16t]", additional)
        self.assertIn("source_role[assistant=1/14t, tool=1/16t]", additional)
        self.assertIn("hook_type[after_llm=1/14t, hook_boundary=1/16t]", additional)
        self.assertIn("codex_event[PostToolUse=1/18t, Stop=1/12t]", additional)
        self.assertIn("final_boundary_refs=2", additional)
        self.assertIn(
            "async_pipeline[tasks=4; ready=false; remaining=entity,secondary_index,summary; memory_layers_ready=false; blocked_layers[session,user_profile,same_session,cross_session,summary]; ready_layers[compression,embedding]; layer_pending[cross_session=2,same_session=3,session=3,summary=2:summary,user_profile=2]; stage_counts[entity=1,secondary_index=1,summary=2]; pending_roles[assistant=2,tool=1,user=1]; pending_hooks[after_llm=2,hook_boundary=1]; pending_codex_events[PostToolUse=1,Stop=1]; pending_scopes[session=3,user_profile=2]; pending_continuity[cross_session=2,same_session=3]; pending_phases[final=1,provisional=3]; pending_final_boundary=1; warnings=async_pipeline_followup_pending,profile_summary_stale]",
            additional,
        )
        self.assertIn("Memory hierarchy:", additional)
        self.assertIn("user_profile/cross_session refs are long-term state", additional)
        self.assertIn("cross_session_budget_floor_status=remote_budget_too_small_for_profile_floor", additional)
        self.assertIn("cross_session_budget=64", additional)
        self.assertIn("computed=64", additional)
        self.assertIn("floor=256", additional)
        self.assertIn("floor_applied=false", additional)
        self.assertIn("Budget pressure:", additional)
        self.assertIn("cross_session_budget=2", additional)
        self.assertIn("max_selected_refs=3", additional)
        self.assertIn("budget_fill_policy=quality_first", additional)
        self.assertIn("dropped_memory_layer_budget:", additional)
        self.assertIn("scope[user_profile=1/22t]", additional)
        self.assertIn("continuity[cross_session=1/22t]", additional)
        self.assertIn("entity_type[assistant_decision=1/22t]", additional)
        self.assertIn("source_role[assistant=1/22t]", additional)
        self.assertIn("hook_type[after_llm=1/22t]", additional)

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
                        "source_roles": ["assistant"],
                        "source_hook_types": ["after_llm"],
                        "text": "assistant: keep cross-session profile decision",
                    },
                    {
                        "context_class": "summary",
                        "entity_type": "tool_evidence",
                        "source_roles": ["tool"],
                        "source_hook_types": ["tool_result"],
                        "source_codex_events": ["PostToolUse"],
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
        self.assertIn("Session identity:", additional)
        self.assertIn("source=explicit", additional)
        self.assertIn("strong=true", additional)
        self.assertIn("Layer summary:", additional)
        self.assertIn("event=1", additional)
        self.assertIn("entity=1", additional)
        self.assertIn("summary=1", additional)
        self.assertIn("same_session_refs=1", additional)
        self.assertIn("cross_session_refs=1", additional)
        self.assertIn("entity_bridge_refs=1", additional)
        self.assertIn("session_memory_refs=1", additional)
        self.assertIn("profile_memory_refs=1", additional)
        self.assertIn("entity_type[assistant_decision=", additional)
        self.assertIn("tool_evidence=", additional)
        self.assertIn("source_role[assistant=", additional)
        self.assertIn("tool=", additional)
        self.assertIn("hook_type[after_llm=", additional)
        self.assertIn("tool_result=", additional)
        self.assertIn("codex_event[PostToolUse=", additional)

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
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
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
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
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
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
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
                "To https://github.com/bjmeetsfo/TemporalStore.git",
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
            "/tmp/definitely-missing-libbcache2.so",
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
            user_id="deeproute",
            session_id="codex-cpp-session-1",
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
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual(6, len(server.adapter.serving_records))
        raw = server.adapter.raw_records[0]
        serving = server.adapter.serving_records[0]
        task = server.adapter.serving_records[1]
        dirty_records = server.adapter.serving_records[2:]
        self.assertEqual("agent_message", raw["record_type"])
        self.assertEqual("real hooked Codex message", raw["messages"][0]["content"])
        self.assertEqual("codex-cpp-session-1", raw["scope"]["session_id"])
        self.assertEqual("thread-fast-1", raw["thread_id"])
        self.assertEqual("turn-fast-1", raw["turn_id"])
        self.assertEqual("context_event", serving["record_type"])
        self.assertEqual("codex-cpp-session-1", serving["session_id"])
        self.assertEqual("codex-cpp-session-1", serving["scope"]["session_id"])
        self.assertEqual("thread-fast-1", serving["thread_id"])
        self.assertEqual("turn-fast-1", serving["turn_id"])
        self.assertEqual("conversation-fast-1", serving["metadata"]["conversation_id"])
        self.assertEqual("turn-fast-1", serving["envelope"]["turn_id"])
        self.assertEqual("UserPromptSubmit", serving["metadata"]["codex_event"])
        self.assertEqual("PENDING_ASYNC_EXTRACTION", serving["classification"])
        self.assertEqual("pending_async", serving["event_type"])
        self.assertEqual("pending", serving["status"])
        self.assertEqual("async_pending", serving["internal_extraction"]["mode"])
        self.assertIn("real hooked Codex message", serving["summary_text"])
        self.assertIn("real hooked Codex message", serving["text"])
        self.assertEqual("matrixark_async_pipeline_task", task["record_type"])
        self.assertEqual(serving["event_id_hash"], task["event_id_hash"])
        self.assertEqual("thread-fast-1", task["thread_id"])
        self.assertEqual("turn-fast-1", task["turn_id"])
        self.assertEqual("pending", task["status"])
        self.assertEqual(["extraction", "summary", "compression", "embedding"], task["stages"])
        self.assertEqual(task["task_hash"], result["async_pipeline_task_hash"])
        self.assertEqual(4, result["summary_dirty_count"])
        self.assertEqual(4, len(dirty_records))
        self.assertTrue(all(record["record_type"] == "context_summary_dirty" for record in dirty_records))
        self.assertTrue(all(record["source_event_hash"] == serving["event_id_hash"] for record in dirty_records))
        self.assertEqual([1, 2, 3, 4], [record["depth"] for record in dirty_records])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("turn-fast-1", server.adapter.session_buffer_records[0]["hook"]["turn_id"])
        self.assertTrue(result["session_buffer"]["registered"])

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
                user_id="deeproute",
                session_id="codex-session-threshold",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=300000,
                understanding_provider="rules",
                segment_provider="deterministic",
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
                user_id="deeproute",
                session_id="codex-session-idle-next-prompt",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=1,
                understanding_provider="rules",
                segment_provider="deterministic",
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
        self.assertEqual({}, result["auto_batch_extract_result"])
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
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(1, commit_args["idle_timeout_ms"])
        self.assertEqual("idle_timeout_before_ingest", commit_hook["trigger"])
        self.assertEqual("thread-idle-preflush", commit_hook["thread_id"])
        self.assertEqual("turn-new-prompt", commit_hook["turn_id"])
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
            user_id="deeproute",
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
        self.assertEqual(1, len(server.adapter.raw_records))
        raw_record = server.adapter.raw_records[0]
        self.assertEqual("assistant", raw_record["messages"][0]["role"])
        self.assertEqual("thread-stop-1", raw_record["thread_id"])
        self.assertEqual("turn-stop-1", raw_record["turn_id"])
        self.assertEqual("assistant", raw_record["source_role"])
        self.assertEqual("after_llm", raw_record["hook_type"])
        self.assertEqual("Stop", raw_record["codex_event"])
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("assistant", serving_event["source_role"])
        self.assertEqual("after_llm", serving_event["hook_type"])
        self.assertEqual("Stop", serving_event["codex_event"])
        self.assertEqual("assistant", serving_event["envelope"]["source_role"])
        self.assertEqual("after_llm", serving_event["envelope"]["hook_type"])
        self.assertEqual("thread-stop-1", serving_event["thread_id"])
        self.assertEqual("turn-stop-1", serving_event["turn_id"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("hook_boundary", commit_args["commit_reason"])
        self.assertTrue(commit_args["force"])
        self.assertEqual("thread-stop-1", commit_hook["thread_id"])
        self.assertEqual("turn-stop-1", commit_hook["turn_id"])

    def test_fast_async_hook_ingest_marks_tool_evidence_lifecycle(self) -> None:
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
                return [{"event_id_hash": 1}]

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="PostToolUse",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            session_id="codex-session-tool",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Exit code: 0\nRan 77 tests in 2.4s\nOK",
            role="tool",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        raw_record = server.adapter.raw_records[0]
        self.assertEqual("tool", raw_record["source_role"])
        self.assertEqual("tool_result", raw_record["hook_type"])
        self.assertEqual("PostToolUse", raw_record["codex_event"])
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("tool", serving_event["source_role"])
        self.assertEqual("tool_result", serving_event["hook_type"])
        self.assertEqual("PostToolUse", serving_event["codex_event"])
        self.assertEqual("tool", serving_event["envelope"]["source_role"])
        self.assertEqual("tool_result", serving_event["envelope"]["hook_type"])

    def test_fast_async_hook_ingest_threshold_commits_tool_evidence(self) -> None:
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
                    "status": "committed",
                    "trigger_policy": "threshold",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": 2,
                    "entities_written": 1,
                    "profile_entities_written": 1,
                    "indexes_written": 2,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="PostToolUse",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="deeproute",
                session_id="codex-session-tool-threshold",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0\nRan 81 tests in 1.2s\nOK",
                role="tool",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field", "thread_id": "thread-tool-threshold"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("committed", result["auto_batch_extract_result"]["status"])
        self.assertEqual("threshold", result["auto_batch_extract_result"]["trigger_policy"])
        self.assertEqual(2, result["session_buffer"]["pending_before_ingest_count"])
        self.assertEqual(2, result["session_buffer"]["pending_after_ingest_count"])
        self.assertTrue(result["session_buffer"]["commit_after_current_ingest"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("threshold", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(2, commit_args["max_messages"])
        self.assertEqual("session_commit", commit_hook["hook_type"])
        self.assertEqual("thread-tool-threshold", commit_hook["thread_id"])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("tool", server.adapter.session_buffer_records[0]["envelope"]["messages"][0]["role"])

    def test_fast_async_hook_ingest_preflushes_idle_tail_before_tool_evidence(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []
                self.pending = [{"event_id_hash": 91, "envelope": {"ingestion_time_ms": 1}}]

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)
                self.pending.append({"event_id_hash": kwargs["event_id_hash"], "envelope": kwargs["envelope"]})

            def pending_session_events(self, scope):
                return list(self.pending)

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                committed = list(self.pending)
                self.pending.clear()
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": len(committed),
                    "source_event_ids": [record["event_id_hash"] for record in committed],
                    "entities_written": 1,
                    "profile_entities_written": 1,
                    "indexes_written": 1,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="PostToolUse",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="deeproute",
                session_id="codex-session-tool-idle",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=1,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0\nTool evidence arrived after an idle tail.",
                role="tool",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field", "thread_id": "thread-tool-idle"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("committed", result["idle_commit_result"]["status"])
        self.assertEqual("idle_timeout", result["idle_commit_result"]["trigger_policy"])
        self.assertEqual([91], result["idle_commit_result"]["source_event_ids"])
        self.assertTrue(result["session_buffer"]["pre_ingest_idle_ready"])
        self.assertEqual(1, result["session_buffer"]["pending_before_ingest_count"])
        self.assertEqual(1, result["session_buffer"]["pending_after_ingest_count"])
        self.assertFalse(result["session_buffer"]["commit_after_current_ingest"])
        self.assertTrue(result["session_buffer"]["auto_batch_extract"])
        self.assertEqual({}, result["auto_batch_extract_result"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(1, commit_args["idle_timeout_ms"])
        self.assertEqual("idle_timeout_before_ingest", commit_hook["trigger"])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("tool", server.adapter.session_buffer_records[0]["envelope"]["messages"][0]["role"])

    def test_fast_async_hook_ingest_commits_idle_timeout_with_zero_timeout(self) -> None:
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
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": 1,
                    "extraction_context_event_count": 2,
                    "entities_written": 3,
                    "profile_entities_written": 1,
                    "trigger_evidence": {
                        "pending_event_count": 1,
                        "threshold_messages": 20,
                        "threshold_ready": False,
                        "idle_timeout_ms": 0,
                        "idle_ready": True,
                        "force": False,
                    },
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="IdleTimeout",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            session_id="codex-session-idle",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="idle tick",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("committed", result["session_commit"]["status"])
        self.assertEqual("idle_timeout", result["session_commit"]["trigger_policy"])
        self.assertEqual("provisional", result["session_commit"]["extraction_phase"])
        self.assertFalse(result["session_commit"]["final_session_boundary"])
        self.assertEqual(2, result["session_commit"]["extraction_context_event_count"])
        self.assertEqual(3, result["session_commit"]["memory_layers_written"]["session_entities"])
        self.assertEqual(1, result["session_commit"]["memory_layers_written"]["profile_entities"])
        self.assertEqual("provisional", result["session_commit"]["memory_layers_written"]["extraction_phase"])
        self.assertTrue(result["session_commit"]["trigger_evidence"]["idle_ready"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, _commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(0, commit_args["idle_timeout_ms"])
        self.assertNotIn("max_messages", commit_args)

    def test_fast_async_hook_ingest_stop_boundary_force_commits_assistant_response(self) -> None:
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
                return [{"event_id_hash": 1}, {"event_id_hash": 2}, {"event_id_hash": 3}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "committed",
                    "trigger_policy": "force",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "committed_event_count": 3,
                    "entities_written": 4,
                    "profile_entities_written": 2,
                    "indexes_written": 6,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            session_id="codex-session-stop",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Done. The hook now commits the final assistant decision into profile memory.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field", "thread_id": "thread-stop"},
        )

        self.assertEqual("accepted", result["status"])
        self.assertTrue(result["session_buffer"]["boundary_commit_requested"])
        self.assertFalse(result["session_buffer"]["threshold_ready"])
        self.assertEqual("committed", result["session_commit"]["status"])
        self.assertEqual("force", result["session_commit"]["trigger_policy"])
        self.assertEqual("final", result["session_commit"]["extraction_phase"])
        self.assertTrue(result["session_commit"]["final_session_boundary"])
        self.assertEqual(2, result["session_commit"]["profile_entities_written"])
        self.assertEqual(2, result["session_commit"]["memory_layers_written"]["profile_entities"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("hook_boundary", commit_args["commit_reason"])
        self.assertTrue(commit_args["force"])
        self.assertNotIn("max_messages", commit_args)
        self.assertEqual("session_commit", commit_hook["hook_type"])
        self.assertEqual("Stop", commit_hook["trigger"])
        self.assertEqual("thread-stop", commit_hook["thread_id"])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        buffered = server.adapter.session_buffer_records[0]
        self.assertEqual("assistant", buffered["envelope"]["messages"][0]["role"])
        self.assertEqual("after_llm", buffered["envelope"]["hook_type"])
        self.assertEqual("Stop", buffered["envelope"]["codex_event"])

    def test_retention_keeps_acceptance_prompt_that_mentions_synthetic_rows(self) -> None:
        fields = hook.hook_retention_fields(
            text=(
                "Fix MatrixArk Codex realtime hook ingestion/query. "
                "Query top K with validation/probe/synthetic rows hidden by default."
            ),
            role="user",
            now_ms=123,
        )

        self.assertFalse(fields["synthetic"])
        self.assertEqual("normal", fields["retention_class"])

    def test_retention_marks_explicit_synthetic_probe_debug(self) -> None:
        fields = hook.hook_retention_fields(
            text="MatrixArk synthetic probe global cmd bash 1784781133",
            role="user",
            now_ms=123,
        )

        self.assertTrue(fields["synthetic"])
        self.assertEqual("debug", fields["retention_class"])

    def test_query_filter_keeps_natural_prompt_with_stale_synthetic_flag(self) -> None:
        text = (
            "Fix MatrixArk Codex realtime hook ingestion/query. "
            "Query top K with validation/probe/synthetic rows hidden by default."
        )

        self.assertTrue(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                text,
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                "MatrixArk synthetic probe global cmd bash 1784781133",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "manual validation loose payload parser row 1784778866123",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "context_event"},
                "user",
                "user: You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "MatrixArk cmd wrapper direct smoke 1784771237422",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "matrixark plain string prompt hook proof 1784770203",
                real_user_only=True,
                include_synthetic=False,
            )
        )

    def test_hook_dedupe_prefers_raw_over_context_event_user_prefix(self) -> None:
        rows = matrixark_http._hook_dedupe_rows(
            [
                {
                    "backend": "c++",
                    "session_id": "codex:session",
                    "text": "user: same prompt",
                    "timestamp_ms": 100,
                    "sequence": 1,
                    "projection": "records",
                },
                {
                    "backend": "c++",
                    "session_id": "codex:session",
                    "text": "same prompt",
                    "timestamp_ms": 100,
                    "sequence": 2,
                    "projection": "raw_ingestion",
                },
            ]
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("raw_ingestion", rows[0]["projection"])

    def test_dual_hook_has_no_persistent_hook_logs(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('CPP_HOOK_STDOUT="/dev/null"', script)
        self.assertIn('RUST_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('CPP_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('export MATRIXARK_CODEX_HOOK_DIAG_LOG=""', script)
        self.assertNotIn('MATRIXARK_CODEX_HOOK_LOG_DIR', script)
        self.assertNotIn('dispatch-diagnostics.jsonl', script)
        self.assertNotIn('with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG"', script)
        self.assertNotIn('cpp-$EVENT.out', script)
        self.assertNotIn('rust-service-publish.err', script)
        self.assertNotIn('cpp-direct-publish.err', script)

    def test_cpp_and_rust_hooks_do_not_persist_external_logs(self) -> None:
        tools_dir = Path(__file__).resolve().parents[1] / "tools"
        cpp_script = (tools_dir / "matrixark_codex_cpp_hook.sh").read_text()
        rust_script = (tools_dir / "matrixark_codex_rust_hook.sh").read_text()
        dual_script = (tools_dir / "matrixark_codex_dual_hook.sh").read_text()
        combined = "\n".join([cpp_script, rust_script, dual_script])

        self.assertIn('export MATRIXARK_RUST_PROXY_DAEMON_LOG="/dev/null"', rust_script)
        self.assertNotIn("daemon.log", rust_script)
        self.assertNotIn('mkdir -p "$(dirname "$MATRIXARK_RUST_PROXY_DAEMON_LOG")"', rust_script)
        self.assertNotIn("MATRIXARK_CODEX_HOOK_LOG_DIR", combined)
        self.assertNotIn("dispatch-diagnostics.jsonl", combined)
        self.assertNotIn("cpp-$EVENT.out", combined)
        self.assertNotIn("rust-service-publish.err", combined)
        self.assertNotIn("cpp-direct-publish.err", combined)

    def test_live_codex_hooks_enable_assistant_capture_and_auto_batch_extraction(self) -> None:
        tools_dir = Path(__file__).resolve().parents[1] / "tools"
        cpp_script = (tools_dir / "matrixark_codex_cpp_hook.sh").read_text()
        rust_script = (tools_dir / "matrixark_codex_rust_hook.sh").read_text()
        dual_script = (tools_dir / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('MATRIXARK_CODEX_CPP_USER_PROMPTS_ONLY="${MATRIXARK_CODEX_CPP_USER_PROMPTS_ONLY:-0}"', cpp_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', cpp_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', rust_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', dual_script)

    def test_live_ingest_auto_batch_decision_covers_tool_but_not_boundaries(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        try:
            hook.HOOK_AUTO_BATCH_EXTRACT = True
            self.assertTrue(hook.should_auto_batch_extract_on_ingest("UserPromptSubmit"))
            self.assertTrue(hook.should_auto_batch_extract_on_ingest("PostToolUse"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("Stop"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("IdleTimeout"))

            hook.HOOK_AUTO_BATCH_EXTRACT = False
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("UserPromptSubmit"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("PostToolUse"))
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_live_ingest_auto_batch_options_are_explicit_for_tool_and_stop(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        try:
            hook.HOOK_AUTO_BATCH_EXTRACT = True
            tool_args = hook.apply_hook_auto_batch_ingest_options(
                {},
                event="PostToolUse",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertTrue(tool_args["auto_batch_extract"])
            self.assertEqual(7, tool_args["session_buffer_threshold"])
            self.assertEqual(123, tool_args["idle_commit_timeout_ms"])

            stop_args = hook.apply_hook_auto_batch_ingest_options(
                {"idle_commit_timeout_ms": 456},
                event="Stop",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertFalse(stop_args["auto_batch_extract"])
            self.assertEqual(7, stop_args["session_buffer_threshold"])
            self.assertNotIn("idle_commit_timeout_ms", stop_args)

            hook.HOOK_AUTO_BATCH_EXTRACT = False
            disabled_args = hook.apply_hook_auto_batch_ingest_options(
                {},
                event="UserPromptSubmit",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertFalse(disabled_args["auto_batch_extract"])
            self.assertEqual(7, disabled_args["session_buffer_threshold"])
            self.assertNotIn("idle_commit_timeout_ms", disabled_args)
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_dual_hook_keeps_derived_context_out_of_raw_ingestion(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('f"{prefix}:raw_ingestion:records", raw_record', script)
        self.assertNotIn('for extracted_record in rust_live_extraction_records()', script)
        self.assertNotIn('for extracted_record in cpp_live_extraction_records()', script)


    def test_codex_hook_messages_both_skips_local_proxy_debug_reader(self) -> None:
        original_cpp = matrixark_http._CppHookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        original_local = matrixark_http._RustLocalHookStoreReader
        calls = []

        class EmptyReader:
            name = "empty"

            def get_string(self, key):
                return "0"

            def hget(self, key, field):
                return None

        try:
            matrixark_http._CppHookStoreReader = lambda args: calls.append("c++") or EmptyReader()
            matrixark_http._RustServiceHookStoreReader = lambda args: calls.append("rust-service") or EmptyReader()

            def fail_local(args):
                raise AssertionError("rust-local proxy should be explicit-only")

            matrixark_http._RustLocalHookStoreReader = fail_local
            matrixark_http.query_codex_hook_messages({"backend": "both", "top_k": 1})
        finally:
            matrixark_http._CppHookStoreReader = original_cpp
            matrixark_http._RustServiceHookStoreReader = original_service
            matrixark_http._RustLocalHookStoreReader = original_local

        self.assertEqual(["c++", "rust-service"], calls)

    def test_query_effective_synthetic_status_uses_text_classifier(self) -> None:
        self.assertTrue(matrixark_http._hook_text_is_synthetic("matrixark plain string prompt hook proof 1784770203"))
        self.assertFalse(
            matrixark_http._hook_text_is_synthetic(
                "Fix MatrixArk query with validation/probe/synthetic rows hidden by default"
            )
        )

if __name__ == "__main__":
    unittest.main()
