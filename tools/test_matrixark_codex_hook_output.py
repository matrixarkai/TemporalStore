#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import json
import os
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
from matrixark_mcp_local_adapter import MatrixArkLocalAdapter


try:  # mixin
    from tools.test_codex_hook_output_part3 import _CodexHookOutputPart3
except ImportError:
    from test_codex_hook_output_part3 import _CodexHookOutputPart3

try:  # mixin
    from tools.test_codex_hook_output_part2 import _CodexHookOutputPart2
except ImportError:
    from test_codex_hook_output_part2 import _CodexHookOutputPart2

class MatrixArkCodexHookOutputTest(unittest.TestCase, _CodexHookOutputPart3, _CodexHookOutputPart2):
    def test_retrieve_layer_summary_reads_rust_proxy_extra_context_pack_pressure(self) -> None:
        result = hook.retrieval_layer_summary_from_retrieve(
            {
                "ok": True,
                "extra": {
                    "context_pack": {
                        "selected_refs": [
                            {
                                "ref_type": "entity",
                                "memory_scope": "user_profile",
                                "session_continuity": "cross_session",
                                "token_estimate": 9,
                            }
                        ],
                        "retrieval_metrics": {
                            "memory_layer_budget": {
                                "total_selected_refs": 1,
                                "total_selected_tokens": 9,
                                "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 9}},
                                "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 9}},
                            },
                            "memory_layer_pressure": {
                                "selected_refs": 1,
                                "dropped_refs": 1,
                                "profile_memory_pressure": True,
                                "cross_session_pressure": True,
                                "hook_boundary_source_pressure": True,
                            },
                        },
                    }
                },
            }
        )

        self.assertEqual(1, result["profile_memory_refs"])
        self.assertEqual(1, result["cross_session_refs"])
        self.assertEqual(
            1,
            result["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
        )
        self.assertTrue(result["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertNotIn("hook_boundary_source_pressure", result["memory_layer_pressure"])

    def test_retrieve_budget_summary_reads_nested_context_pack_wrapper(self) -> None:
        wrapped = {
            "ok": True,
            "used_context_tokens": 0,
            "extra": {
                "context_pack": {
                    "context_pack_id": "pack-rust-nested",
                    "used_context_tokens": 42,
                    "remote_context_budget_tokens": 100,
                    "requested_max_context_tokens": 128,
                    "used_local_context_tokens": 10,
                    "total_prompt_context_tokens": 52,
                    "local_context_safety_margin_tokens": 8,
                    "budget_source": "rust_proxy_context_pack",
                    "selected_refs": [
                        {
                            "ref_type": "entity",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "text": "profile decision from nested Rust proxy pack",
                            "token_estimate": 9,
                        }
                    ],
                    "retrieval_metrics": {
                        "memory_layer_budget": {
                            "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 9}},
                            "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 9}},
                        }
                    },
                }
            },
        }

        self.assertEqual(1, hook.selected_ref_count_from_retrieve(wrapped))
        self.assertEqual(42, hook.used_context_tokens_from_retrieve(wrapped))
        budget = hook.retrieval_budget_summary_from_retrieve(wrapped)
        self.assertEqual(42, budget["used_remote_context_tokens"])
        self.assertEqual(100, budget["remote_context_budget_tokens"])
        self.assertEqual(58, budget["remote_budget_remaining_tokens"])
        self.assertEqual(128, budget["requested_max_context_tokens"])
        self.assertEqual(52, budget["total_prompt_context_tokens"])
        self.assertEqual("rust_proxy_context_pack", budget["budget_source"])
        self.assertEqual(110, budget["budget_contract"]["computed_remote_context_budget_tokens"])
        self.assertTrue(budget["budget_contract"]["contract_holds"])

        context = hook.additional_context_from_retrieve(
            wrapped,
            query="profile decision",
            local_context_count=0,
        )
        self.assertIn("context_pack_id=pack-rust-nested", context)
        self.assertIn("used_context_tokens=42", context)
        self.assertNotIn("Budget summary:", context)
        self.assertNotIn("remote_budget=100", context)
        self.assertNotIn("remote_remaining=58", context)
        self.assertNotIn("budget_source=rust_proxy_context_pack", context)
        self.assertIn("profile decision from nested Rust proxy pack", context)

    def test_retrieve_layer_summary_infers_metadata_backed_nested_refs_without_metrics(self) -> None:
        wrapped = {
            "ok": True,
            "extra": {
                "context_pack": {
                    "context_pack_id": "pack-metadata-only",
                    "selected_refs": [
                        {
                            "text": "assistant: decided to keep profile entity memory compact",
                            "metadata": {
                                "ref_type": "entity",
                                "context_class": "entity",
                                "memory_scope": "user_profile",
                                "session_continuity": "cross_session",
                                "extraction_phase": "final",
                                "entity_type": "assistant_decision",
                                "token_estimate": 13,
                                "source_role_counts": {"llm": 2},
                                "source_hook_type_counts": {"after_llm": 1},
                                "source_codex_event_counts": {"Stop": 1},
                            },
                        }
                    ],
                    "recall_policy": {
                        "session_identity": {
                            "session_id_source": "payload_field",
                            "strong_session_identity": True,
                            "fallback_session_identity": False,
                        },
                        "cross_session": {
                            "enabled": True,
                            "budget_tokens": 64,
                        },
                    },
                }
            },
        }

        summary = hook.retrieval_layer_summary_from_retrieve(wrapped)
        self.assertEqual({"entity": 1}, summary["selected_ref_counts"])
        self.assertEqual(1, summary["cross_session_refs"])
        self.assertEqual(1, summary["entity_bridge_refs"])
        self.assertEqual(1, summary["profile_memory_refs"])
        budget = summary["memory_layer_budget"]
        self.assertEqual(1, budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(13, budget["by_memory_scope"]["user_profile"]["tokens"])
        self.assertEqual(1, budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, budget["by_extraction_phase"]["final"]["refs"])
        self.assertEqual(1, budget["by_entity_type"]["assistant_decision"]["refs"])
        self.assertNotIn("source_message_counts_by_role", budget)
        self.assertNotIn("source_hook_counts_by_type", budget)
        self.assertNotIn("source_codex_event_counts_by_event", budget)
        self.assertEqual(13, budget["total_selected_tokens"])

    def test_layer_pressure_flags_include_source_message_pressure_aliases(self) -> None:
        pressure = {
            "selected_refs": 2,
            "assistant_source_message_pressure": True,
            "user_source_message_pressure": True,
            "tool_source_message_pressure": True,
        }
        bits = hook._format_memory_layer_pressure_bits(pressure)

        self.assertIn("selected=2", bits)
        self.assertNotIn("flags[assistant,user,tool]", bits)


    def test_budget_pressure_uses_native_layer_metrics_without_dropped_refs(self) -> None:
        pressure = hook.retrieval_budget_pressure_from_retrieve(
            {
                "context_pack_id": "pack-native-pressure",
                "retrieval_metrics": {
                    "dropped_memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 21}},
                        "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 21}},
                    },
                    "memory_layer_pressure": {
                        "dropped_refs": 1,
                        "dropped_tokens": 21,
                        "profile_memory_pressure": True,
                        "cross_session_pressure": True,
                        "assistant_source_message_pressure": True,
                        "dropped_dimensions": ["by_memory_scope", "source_message_counts_by_role"],
                    },
                },
            }
        )

        self.assertTrue(pressure["budget_pressure"])
        self.assertEqual(
            1,
            pressure["dropped_memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
        )
        self.assertTrue(pressure["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertNotIn("assistant_source_message_pressure", pressure["memory_layer_pressure"])

    def test_loose_stop_payload_extracts_current_input_message_and_thread_identity(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8cb5-b4d5-77f2-8c82-0499440da36f,'
            'turn-id:019f8cd9-7669-7891-ab92-7353efe82e4d,'
            'cwd:C:\\Users\\example\\Documents\\Codex,client:Codex Desktop,'
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

    def test_codex_agent_hook_omits_thread_and_turn_lineage_by_default(self) -> None:
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
        self.assertNotIn("thread_id", agent_hook)
        self.assertNotIn("turn_id", agent_hook)
        self.assertNotIn("conversation_id", agent_hook)


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

    def test_payload_text_reads_common_assistant_stop_fields(self) -> None:
        self.assertEqual(
            "Decision: commit assistant response into profile memory.",
            hook.payload_text(
                {
                    "hook_event_name": "Stop",
                    "last_assistant_message": "Decision: commit assistant response into profile memory.",
                }
            ),
        )
        self.assertEqual(
            "Outcome: extracted bounded assistant decision.",
            hook.payload_text(
                {
                    "hook_event_name": "Stop",
                    "params": {
                        "last-assistant-message": "Outcome: extracted bounded assistant decision.",
                    },
                }
            ),
        )
        self.assertEqual(
            "Final: profile bridge is ready.",
            hook.payload_text(
                {
                    "hook_event_name": "Stop",
                    "turn": {
                        "finalAnswer": "Final: profile bridge is ready.",
                    },
                }
            ),
        )

    def test_payload_text_prefers_assistant_fields_for_stop_payloads(self) -> None:
        payload = {
            "hook_event_name": "Stop",
            "prompt": "original user prompt should not become assistant memory",
            "last_assistant_message": "Decision: assistant outcome should be extracted.",
        }

        self.assertEqual(
            "Decision: assistant outcome should be extracted.",
            hook.payload_text(payload, event="Stop"),
        )

    def test_payload_text_prefers_tool_fields_for_tool_payloads(self) -> None:
        payload = {
            "hook_event_name": "PostToolUse",
            "prompt": "original user prompt should not become tool evidence",
            "params": {
                "tool_result": "Exit code: 0\nRan 9 tests\nOK",
            },
        }

        self.assertEqual(
            "Exit code: 0\nRan 9 tests\nOK",
            hook.payload_text(payload, event="PostToolUse"),
        )

    def test_payload_text_prefers_assistant_role_from_mixed_stop_messages(self) -> None:
        payload = {
            "hook_event_name": "Stop",
            "messages": [
                {"role": "user", "content": "original prompt should stay out of assistant memory"},
                {
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": "Decision: keep role-filtered assistant memory."},
                        {"type": "text", "text": "Done. Mixed payload no longer stores the prompt."},
                    ],
                },
            ],
        }

        text = hook.payload_text(payload, event="Stop")

        self.assertIn("Decision: keep role-filtered assistant memory.", text)
        self.assertIn("Done. Mixed payload no longer stores the prompt.", text)
        self.assertNotIn("original prompt should stay out", text)

    def test_payload_text_extracts_assistant_output_aliases_for_stop_events(self) -> None:
        self.assertEqual(
            "Decision: capture assistant output alias.",
            hook.payload_text(
                {"assistant_output": "Decision: capture assistant output alias."},
                event="Stop",
            ),
        )
        self.assertEqual(
            "Done. LLM response should be ingested.",
            hook.payload_text(
                {"params": {"llm_response": "Done. LLM response should be ingested."}},
                event="Stop",
            ),
        )
        self.assertEqual(
            "Implemented model response memory capture.",
            hook.payload_text(
                {
                    "model_response": [
                        {"text": "Implemented model response memory capture."},
                    ]
                },
                event="Stop",
            ),
        )
        self.assertEqual(
            "First assistant decision.\nSecond assistant decision.",
            hook.payload_text(
                {"assistant_outputs": ["First assistant decision.", "Second assistant decision."]},
                event="Stop",
            ),
        )

    def test_payload_text_prefers_assistant_alias_from_mixed_stop_messages(self) -> None:
        payload = {
            "hook_event_name": "Stop",
            "messages": [
                {"role": "user", "content": "prompt should not be extracted as assistant memory"},
                {"role": "assistant_response", "content": "Decision: alias assistant response wins."},
                {"role": "llm", "content": "Result: LLM alias is also assistant memory."},
            ],
        }

        text = hook.payload_text(payload, event="Stop")

        self.assertIn("Decision: alias assistant response wins.", text)
        self.assertIn("Result: LLM alias is also assistant memory.", text)
        self.assertNotIn("prompt should not be extracted", text)

    def test_payload_text_prefers_tool_role_from_mixed_tool_messages(self) -> None:
        payload = {
            "hook_event_name": "PostToolUse",
            "messages": [
                {"role": "user", "content": "run the tests"},
                {"role": "assistant", "content": "I will run validation."},
                {"role": "tool", "content": "Exit code: 0\nRan 11 tests\nOK"},
            ],
        }

        self.assertEqual(
            "Exit code: 0\nRan 11 tests\nOK",
            hook.payload_text(payload, event="PostToolUse"),
        )

    def test_payload_text_prefers_tool_alias_from_mixed_tool_messages(self) -> None:
        payload = {
            "hook_event_name": "PostToolUse",
            "messages": [
                {"role": "user_prompt", "content": "run the tests"},
                {"role": "assistant_output", "content": "I will run validation."},
                {"role": "tool_result", "content": "Exit code: 0\nRan 13 tests\nOK"},
                {"role": "function_call_output", "content": "pushed commit abc123 to origin/main"},
            ],
        }

        text = hook.payload_text(payload, event="PostToolUse")

        self.assertIn("Exit code: 0\nRan 13 tests\nOK", text)
        self.assertIn("pushed commit abc123 to origin/main", text)
        self.assertNotIn("run the tests", text)
        self.assertNotIn("I will run validation", text)

    def test_payload_text_extracts_terminal_output_for_tool_events(self) -> None:
        self.assertEqual(
            "Exit code: 0\nRan 9 tests\nOK",
            hook.payload_text(
                {"terminal_output": "Exit code: 0\nRan 9 tests\nOK"},
                event="PostToolUse",
            ),
        )
        self.assertEqual(
            "noise\nExit code: 0\nRan 5 tests\nOK",
            hook.payload_text(
                {"tool_outputs": ["noise", {"output": "Exit code: 0\nRan 5 tests\nOK"}]},
                event="PostToolUse",
            ),
        )

    def test_payload_text_prefers_user_role_from_mixed_prompt_messages(self) -> None:
        payload = {
            "hook_event_name": "UserPromptSubmit",
            "messages": [
                {"role": "system", "content": "system context"},
                {"role": "assistant", "content": "old answer"},
                {"role": "user", "content": "new user prompt should be ingested"},
            ],
        }

        self.assertEqual(
            "new user prompt should be ingested",
            hook.payload_text(payload, event="UserPromptSubmit"),
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

    def test_assistant_memory_selection_marks_synthesized_facts_lossy(self) -> None:
        raw = "\n".join(
            [
                "Implementation details " * 80,
                "Implemented profile entity promotion and pushed commit def7890 to origin/main.",
                "Validation ran 112 tests passed.",
                "Changed assistant outcome extraction to preserve count-only facts.",
                "Next: continue retrieval budget tuning.",
            ]
        )

        selected = hook.selected_assistant_memory_text(raw)
        metadata = hook.codex_memory_selection_metadata(
            role="assistant",
            event="Stop",
            text=selected,
            original_text=raw,
        )

        self.assertIn("Outcome: pushed commit def7890 to origin/main", selected)
        self.assertIn("Validation: 112 tests passed", selected)
        self.assertIn("assistant outcome extraction", selected)
        self.assertNotIn("Implementation details Implementation details", selected)
        self.assertNotIn("Implemented profile entity promotion and pushed commit def7890", selected)
        self.assertNotIn("Validation ran 112 tests passed.", selected)
        self.assertNotIn("Next: continue retrieval budget tuning.", selected)
        self.assertEqual(1, selected.count("def7890"))
        self.assertEqual(1, selected.count("112 tests passed"))
        self.assertEqual("selected_assistant_decision_outcome_only", metadata["policy"])
        self.assertTrue(metadata["selection_lossy"])
        self.assertLessEqual(metadata["retained_text_ratio"], 1.0)
        self.assertLessEqual(metadata["retained_line_ratio"], 1.0)
        self.assertFalse(metadata["large_payload_verbatim_stored"])

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

    def test_codex_hook_persisted_metadata_omits_transport_lineage_by_default(self) -> None:
        payload = {
            "hook_event_name": "UserPromptSubmit",
            "thread_id": "019f8d12-86c6-7100-9a44-7537cdd30aec",
            "turn_id": "019f-turn",
            "prompt": "clean memory payload",
        }
        args = Namespace(repo_root=Path("/repo"))
        agent_context = hook.agent_context_from_payload(
            payload,
            event="UserPromptSubmit",
            session_id_source="payload_field",
            args=args,
        )


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
            'input-messages:[QUERY TOP 10 MESSAGES FROM AND RUST TEMPORALSTORE WITH DETAILS TO COMPARE, '
            'MAKE SURE CURRENT THREAD IS FETCHED]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertIn(
            "QUERY TOP 10 MESSAGES FROM AND RUST TEMPORALSTORE",
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
            user_id="local_user",
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
                        "pre_retrieval_summary_refresh": {
                            "enabled": True,
                            "status": "refreshed",
                            "requested_limit": 2,
                            "refreshed_count": 1,
                        },
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
                    "pre_retrieval_summary_refresh": {
                        "enabled": True,
                        "status": "refreshed",
                        "requested_limit": 2,
                        "refreshed_count": 1,
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
                        "profile_promotion_policy": "always_when_profile_scope_available",
                        "profile_promotion_blocker": "",
                        "memory_layers_written": {
                            "profile_promotion_policy": "always_when_profile_scope_available",
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
                            "profile_promotion_policy": "always_when_profile_scope_available",
                        },
                        "profile_promotion_policy": "always_when_profile_scope_available",
                        "profile_promotion_blocker": "",
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
                    "profile_promotion_policy": "always_when_profile_scope_available",
                    "profile_promotion_blocker": "",
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
        self.assertNotIn("memory_hierarchy", record["output_summary"])
        self.assertEqual({"event": 1, "entity": 1}, record["output_summary"]["retrieval_layers"]["selected_ref_counts"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["cross_session_refs"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["profile_memory_refs"])
        self.assertEqual(
            1,
            record["output_summary"]["retrieval_layers"]["memory_layer_budget"]["final_session_boundary_ref_count"],
        )
        self.assertEqual(
            {"enabled": True, "status": "refreshed", "requested_limit": 2, "refreshed_count": 1},
            record["output_summary"]["pre_retrieval_summary_refresh"],
        )
        self.assertEqual(
            record["output_summary"]["pre_retrieval_summary_refresh"],
            record["output_summary"]["retrieval_layers"]["pre_retrieval_summary_refresh"],
        )
        self.assertEqual(2, record["output_summary"]["async_pipeline_readiness"]["task_count"])
        self.assertFalse(record["output_summary"]["async_pipeline_readiness"]["ready_for_retrieval"])
        self.assertEqual(37, record["output_summary"]["rendered_context_chars"])
        self.assertTrue(record["output_summary"]["strict_additional_context_emitted"])
        self.assertEqual("committed", record["output_summary"]["auto_batch_extract_status"])
        auto_batch = record["output_summary"]["auto_batch_extract"]
        self.assertEqual("threshold", auto_batch["trigger_policy"])
        self.assertEqual("always_when_profile_scope_available", auto_batch["profile_promotion_policy"])
        self.assertEqual("always_when_profile_scope_available", auto_batch["memory_layers_written"]["profile_promotion_policy"])
        self.assertEqual(3, auto_batch["memory_layers_written"]["session_entities"])
        auto_batch_decision = record["output_summary"]["auto_batch_extract_decision"]
        self.assertEqual("committed", auto_batch_decision["decision"])
        self.assertEqual("threshold", auto_batch_decision["reason"])
        self.assertEqual(1, auto_batch_decision["memory_layers_written"]["profile_entities"])
        self.assertEqual("always_when_profile_scope_available", auto_batch_decision["profile_promotion_policy"])
        self.assertEqual("always_when_profile_scope_available", auto_batch_decision["memory_layers_written"]["profile_promotion_policy"])
        self.assertTrue(auto_batch_decision["summary_refresh"]["profile_summary_refresh_required"])
        commit_summary = record["output_summary"]["session_commit"]
        self.assertEqual("idle_timeout", commit_summary["trigger_policy"])
        self.assertEqual("provisional", commit_summary["extraction_phase"])
        self.assertFalse(commit_summary["final_session_boundary"])
        self.assertEqual(1, commit_summary["source_event_count"])
        self.assertEqual(2, commit_summary["extraction_context_event_count"])
        materialization = commit_summary["context_materialization"]
        self.assertEqual(2, materialization["context_event"]["count"])
        self.assertEqual("per-message serving event", materialization["context_event"]["role"])
        self.assertEqual(1, materialization["context_segment"]["count"])
        self.assertEqual("context_event", materialization["context_segment"]["derived_from"])
        self.assertTrue(materialization["context_segment"]["not_a_context_event_alias"])
        self.assertEqual(3, materialization["session_entity"]["count"])
        self.assertEqual(1, materialization["profile_entity"]["count"])
        self.assertEqual("user_profile", materialization["profile_entity"]["scope"])
        self.assertEqual("", materialization["profile_entity"]["blocker"])
        self.assertTrue(materialization["profile_entity"]["scope_available"])
        self.assertEqual(["tool"], commit_summary["source_roles"])
        self.assertEqual(["hook_boundary"], commit_summary["source_hook_types"])
        self.assertEqual(["PostToolUse"], commit_summary["source_codex_events"])
        self.assertEqual("always_when_profile_scope_available", commit_summary["profile_promotion_policy"])
        self.assertEqual("always_when_profile_scope_available", commit_summary["memory_layers_written"]["profile_promotion_policy"])
        self.assertEqual(701, commit_summary["profile_promotion_summary"][0]["profile_entity_hash"])
        self.assertEqual(["codex-session-1"], commit_summary["profile_promotion_summary"][0]["source_session_ids"])
        self.assertNotIn("memory_lineage", record["output_summary"])
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
                        "memory_layer_pressure": {
                            "selected_refs": 2,
                            "selected_tokens": 7,
                            "dropped_refs": 3,
                            "dropped_tokens": 56,
                            "profile_memory_pressure": True,
                            "cross_session_pressure": True,
                            "assistant_source_message_pressure": True,
                            "dropped_dimensions": ["by_memory_scope", "source_message_counts_by_role"],
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
                                "TemporalStore is live and accepting MatrixArk hook writes."
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
        self.assertTrue(pressure["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertTrue(pressure["memory_layer_pressure"]["cross_session_pressure"])
        self.assertNotIn("assistant_source_message_pressure", pressure["memory_layer_pressure"])
        self.assertEqual(
            ["by_memory_scope"],
            pressure["memory_layer_pressure"]["dropped_dimensions"],
        )
        self.assertNotIn("memory_hierarchy", item["result"])
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
            "source_role_budget": {
                "enabled": True,
                "mode": "auto",
                "remote_budget_tokens": 100,
                "budget_semantics": "independent_per_role_caps_under_global_remote_budget",
                "independent_caps": True,
                "global_remote_budget_enforced": True,
                "budget_tokens": {"llm": 10, "model": 22, "tool": 24, "user": 40},
                "selected_tokens_by_role": {"llm": 7, "model": 15, "tool": 18},
                "selected_ref_count_by_role": {"model": 1, "tool": 1},
            },
            "memory_layer_budget_policy": {
                "enabled": True,
                "mode": "auto",
                "remote_budget_tokens": 100,
                "budget_semantics": "independent_per_layer_caps_under_global_remote_budget",
                "independent_caps": True,
                "global_remote_budget_enforced": True,
                "budget_tokens": {"summary": 30, "profile_entity": 40},
                "selected_tokens_by_layer": {"summary": 12, "profile_entity": 18},
                "selected_ref_count_by_layer": {"summary": 1, "profile_entity": 1},
            },
            "memory_selection_policy_budget_policy": {
                "enabled": True,
                "mode": "auto",
                "remote_budget_tokens": 100,
                "budget_semantics": "independent_per_memory_selection_policy_caps_under_global_remote_budget",
                "independent_caps": True,
                "global_remote_budget_enforced": True,
                "budget_tokens": {"selected_assistant_decision_outcome_only": 32, "selected_tool_evidence_only": 24},
                "selected_tokens_by_policy": {"selected_assistant_decision_outcome_only": 18},
                "selected_ref_count_by_policy": {"selected_assistant_decision_outcome_only": 1},
            },
            "extraction_phase_budget_policy": {
                "enabled": True,
                "mode": "explicit",
                "remote_budget_tokens": 100,
                "budget_semantics": "independent_per_extraction_phase_caps_under_global_remote_budget",
                "independent_caps": True,
                "global_remote_budget_enforced": True,
                "budget_tokens": {"pending_async": 16, "provisional": 24, "final": 60},
                "selected_tokens_by_phase": {"pending_async": 8, "final": 22},
                "selected_ref_count_by_phase": {"pending_async": 1, "final": 1},
            },
        }

        contract = hook.retrieval_memory_hierarchy_contract_from_retrieve({"recall_policy": recall_policy})
        self.assertEqual(memory_hierarchy_contract_from_recall_policy(recall_policy), contract)
        self.assertEqual("auto", contract["source_role_budget_mode"])
        self.assertEqual(100, contract["source_role_remote_budget_tokens"])
        self.assertEqual("independent_per_role_caps_under_global_remote_budget", contract["source_role_budget_semantics"])
        self.assertTrue(contract["source_role_independent_caps"])
        self.assertTrue(contract["source_role_global_remote_budget_enforced"])
        self.assertEqual({"assistant": 32, "tool": 24, "user": 40}, contract["source_role_budget_tokens"])
        self.assertEqual({"assistant": 22, "tool": 18}, contract["source_role_selected_tokens_by_role"])
        self.assertEqual({"assistant": 1, "tool": 1}, contract["source_role_selected_ref_count_by_role"])
        self.assertTrue(contract["memory_layer_budget_enabled"])
        self.assertEqual("auto", contract["memory_layer_budget_mode"])
        self.assertEqual(100, contract["memory_layer_remote_budget_tokens"])
        self.assertEqual("independent_per_layer_caps_under_global_remote_budget", contract["memory_layer_budget_semantics"])
        self.assertTrue(contract["memory_layer_independent_caps"])
        self.assertTrue(contract["memory_layer_global_remote_budget_enforced"])
        self.assertEqual({"summary": 30, "profile_entity": 40}, contract["memory_layer_budget_tokens"])
        self.assertEqual({"summary": 12, "profile_entity": 18}, contract["memory_layer_selected_tokens_by_layer"])
        self.assertEqual({"summary": 1, "profile_entity": 1}, contract["memory_layer_selected_ref_count_by_layer"])
        self.assertTrue(contract["memory_selection_policy_budget_enabled"])
        self.assertEqual("auto", contract["memory_selection_policy_budget_mode"])
        self.assertEqual(
            "independent_per_memory_selection_policy_caps_under_global_remote_budget",
            contract["memory_selection_policy_budget_semantics"],
        )
        self.assertTrue(contract["memory_selection_policy_independent_caps"])
        self.assertTrue(contract["memory_selection_policy_global_remote_budget_enforced"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 32, "selected_tool_evidence_only": 24},
            contract["memory_selection_policy_budget_tokens"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 18},
            contract["memory_selection_policy_selected_tokens_by_policy"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            contract["memory_selection_policy_selected_ref_count_by_policy"],
        )
        self.assertTrue(contract["extraction_phase_budget_enabled"])
        self.assertEqual("explicit", contract["extraction_phase_budget_mode"])
        self.assertEqual(
            "independent_per_extraction_phase_caps_under_global_remote_budget",
            contract["extraction_phase_budget_semantics"],
        )
        self.assertTrue(contract["extraction_phase_independent_caps"])
        self.assertTrue(contract["extraction_phase_global_remote_budget_enforced"])
        self.assertEqual({"pending_async": 16, "provisional": 24, "final": 60}, contract["extraction_phase_budget_tokens"])
        self.assertEqual({"pending_async": 8, "final": 22}, contract["extraction_phase_selected_tokens_by_phase"])
        self.assertEqual({"pending_async": 1, "final": 1}, contract["extraction_phase_selected_ref_count_by_phase"])
        self.assertIn("memory_layer_budget_gate", contract["selected_ref_flow"])
        self.assertIn("memory_selection_policy_budget_gate", contract["selected_ref_flow"])
        self.assertIn("extraction_phase_budget_gate", contract["selected_ref_flow"])



if __name__ == "__main__":
    unittest.main()


