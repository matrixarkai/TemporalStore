# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexPipelinePart5 methods split from test_matrixark_codex_hook_pipeline.MatrixArkCodexHookPipelineTest (mixin)."""
from __future__ import annotations

import os
from unittest import mock

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_pipeline import (
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    Path,
    compact_context_pack_for_serving,
    embedding_for_text,
    infer_query_type,
    json,
    matrixark_codex_hook,
    matrixark_mcp_core,
    matrixark_mcp_local_adapter,
    matrixark_mcp_query,
    mock,
    os,
    tempfile,
    time,
)
except ImportError:
    from test_matrixark_codex_hook_pipeline import (
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    Path,
    compact_context_pack_for_serving,
    embedding_for_text,
    infer_query_type,
    json,
    matrixark_codex_hook,
    matrixark_mcp_core,
    matrixark_mcp_local_adapter,
    matrixark_mcp_query,
    mock,
    os,
    tempfile,
    time,
)


class _CodexPipelinePart5:
    def test_pending_event_message_reconstruction_preserves_memory_selection_metadata(self) -> None:
        original_message = {
            "role": "assistant",
            "content": (
                "I will focus on feature parity for session memory; "
                "no testing, monitoring, debugging, or evidence for this step."
            ),
            "metadata": {
                "codex_memory_selection": {
                    "policy": "selected_assistant_profile_fact",
                    "policies": ["selected_assistant_profile_fact"],
                }
            },
        }
        pending_event = {
            "record_type": "context_event",
            "event_id_hash": 7701,
            "text": f"{original_message['role']}: {original_message['content']}",
            "envelope": {"messages": [original_message]},
        }

        reconstructed = matrixark_mcp_core.messages_from_event_record(pending_event)

        self.assertEqual(original_message["metadata"], reconstructed[0]["metadata"])
        self.assertEqual(
            "memory_feature",
            matrixark_mcp_local_adapter.context_event_type_for_message(
                reconstructed[0],
                "pending_async",
            ),
        )

    def test_batch_extract_scopes_mixed_pending_event_lineage_per_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-mixed-pending-lineage.jsonl")
            scope = {
                "account_id": "acct_mixed_pending_lineage",
                "tenant_id": "tenant_mixed_pending_lineage",
                "user_id": "user_mixed_pending_lineage",
                "session_id": "session_mixed_pending_lineage",
            }
            messages = [
                {
                    "role": "user",
                    "content": "Remember mixed lineage user prompt marker amber atlas.",
                    "metadata": {
                        "codex_memory_selection": {
                            "policy": "selected_user_prompt",
                            "policies": ["selected_user_prompt"],
                        }
                    },
                },
                {
                    "role": "assistant",
                    "content": (
                        "I will focus on feature parity for session memory; "
                        "no testing, monitoring, debugging, or evidence."
                    ),
                    "metadata": {
                        "codex_memory_selection": {
                            "policy": "selected_assistant_profile_fact",
                            "policies": ["selected_assistant_profile_fact"],
                        }
                    },
                },
                {
                    "role": "tool",
                    "content": "Exit code: 0; Ran 3 focused tests; validation passed marker copper chisel.",
                    "metadata": {
                        "codex_memory_selection": {
                            "policy": "selected_tool_evidence_only",
                            "policies": ["selected_tool_evidence_only"],
                        }
                    },
                },
            ]
            source_event_records = [
                {
                    "record_type": "context_event",
                    "event_id_hash": 8101,
                    "source_roles": ["user"],
                    "source_role_counts": {"user": 1},
                    "source_hook_types": ["before_llm"],
                    "source_hook_type_counts": {"before_llm": 1},
                    "source_codex_events": ["UserPromptSubmit"],
                    "source_codex_event_counts": {"UserPromptSubmit": 1},
                    "source_memory_selection_policies": ["selected_user_prompt"],
                    "source_memory_selection_policy_counts": {"selected_user_prompt": 1},
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 8102,
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                    "source_hook_types": ["after_llm"],
                    "source_hook_type_counts": {"after_llm": 1},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 1},
                    "source_memory_selection_policies": ["selected_assistant_profile_fact"],
                    "source_memory_selection_policy_counts": {"selected_assistant_profile_fact": 1},
                    "source_profile_memory_classes": ["memory_feature"],
                    "source_profile_memory_kinds": ["memory_feature"],
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 8103,
                    "source_roles": ["tool"],
                    "source_role_counts": {"tool": 1},
                    "source_hook_types": ["tool_result"],
                    "source_hook_type_counts": {"tool_result": 1},
                    "source_codex_events": ["PostToolUse"],
                    "source_codex_event_counts": {"PostToolUse": 1},
                    "source_memory_selection_policies": ["selected_tool_evidence_only"],
                    "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                },
            ]

            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": messages,
                    "threshold_messages": 3,
                    "force": True,
                    "derive_from_existing_events": True,
                    "source_event_ids": [8101, 8102, 8103],
                    "source_event_records": source_event_records,
                    "skip_prior_context": True,
                    "metadata": {"node_path": adapter.default_session_node_path(scope)},
                }
            )

            self.assertEqual("accepted", result["status"])
            events = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_event"
                and record.get("status") == "extraction_committed"
            ]
            by_role = {record["source_role"]: record for record in events}
            self.assertEqual(["before_llm"], by_role["user"]["source_hook_types"])
            self.assertEqual(["UserPromptSubmit"], by_role["user"]["source_codex_events"])
            self.assertEqual(["selected_user_prompt"], by_role["user"]["source_memory_selection_policies"])
            self.assertEqual("memory_feature", by_role["assistant"]["event_type"])
            self.assertEqual(["after_llm"], by_role["assistant"]["source_hook_types"])
            self.assertEqual(["Stop"], by_role["assistant"]["source_codex_events"])
            self.assertEqual(["selected_assistant_profile_fact"], by_role["assistant"]["source_memory_selection_policies"])
            self.assertEqual(["memory_feature"], by_role["assistant"]["source_profile_memory_classes"])
            self.assertEqual(["tool_result"], by_role["tool"]["source_hook_types"])
            self.assertEqual(["PostToolUse"], by_role["tool"]["source_codex_events"])
            self.assertEqual(["selected_tool_evidence_only"], by_role["tool"]["source_memory_selection_policies"])

    def test_assistant_feature_scope_overrides_decision_policy_for_event_type(self) -> None:
        message = {
            "role": "assistant",
            "content": (
                "I will focus on external-memory-style session memory features; "
                "no testing, monitoring, debugging, or evidence in this step."
            ),
            "metadata": {
                "codex_memory_selection": {
                    "policies": [
                        "selected_assistant_profile_fact",
                        "selected_assistant_decision_outcome_only",
                    ],
                    "policy": "selected_assistant_profile_fact",
                }
            },
        }

        self.assertEqual(
            "memory_feature",
            matrixark_mcp_local_adapter.context_event_type_for_message(
                message,
                "pending_async",
            ),
        )

    def test_profile_memory_feature_updates_accumulate_across_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-feature-accumulate.jsonl")
            base_scope = {
                "account_id": "acct_profile_feature_accumulate",
                "tenant_id": "tenant_profile_feature_accumulate",
                "user_id": "user_profile_feature_accumulate",
            }
            messages_by_session = [
                (
                    "session_profile_feature_accumulate_1",
                    "Focus on session memory features and threshold batch extraction.",
                ),
                (
                    "session_profile_feature_accumulate_2",
                    "Focus on profile retrieval budgets and idle batch extraction.",
                ),
            ]
            for session_id, content in messages_by_session:
                scope = {**base_scope, "session_id": session_id}
                result = adapter.batch_extract(
                    {
                        "scope": scope,
                        "messages": [{"role": "user", "content": content}],
                        "threshold_messages": 1,
                        "force": True,
                        "skip_prior_context": True,
                        "metadata": {"node_path": adapter.default_session_node_path(scope)},
                    }
                )
                self.assertEqual("accepted", result["status"])

            profile_entities = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("entity_type") == "memory_feature_profile"
                and record.get("profile_entity_current") is True
            ]
            self.assertTrue(profile_entities)
            latest_profile = profile_entities[-1]
            self.assertIn("session memory features", latest_profile["state"])
            self.assertIn("profile retrieval budgets", latest_profile["state"])
            self.assertIn("session_profile_feature_accumulate_1", latest_profile["source_session_ids"])
            self.assertIn("session_profile_feature_accumulate_2", latest_profile["source_session_ids"])
            self.assertGreaterEqual(latest_profile["profile_revision"], 2)

    def test_lightweight_async_assistant_and_tool_pending_events_keep_semantic_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-lightweight-pending-role-types.jsonl")
            base_scope = {
                "account_id": "acct_lightweight_pending_roles",
                "tenant_id": "tenant_lightweight_pending_roles",
                "user_id": "user_lightweight_pending_roles",
            }
            cases = [
                (
                    "assistant",
                    "after_llm",
                    "Stop",
                    "Decision: use riverstone adapter for profile extraction marker cobalt compass.",
                    "What assistant decision mentions cobalt compass?",
                    "assistant_response",
                    "cobalt compass",
                ),
                (
                    "tool",
                    "tool_result",
                    "ToolResult",
                    "Exit code: 0; validation passed for marker silver anvil.",
                    "What tool evidence mentions silver anvil?",
                    "tool_evidence",
                    "silver anvil",
                ),
            ]
            for role, hook_type, codex_event, text, query, event_type, marker in cases:
                with self.subTest(role=role):
                    scope = {**base_scope, "session_id": f"session_lightweight_pending_{role}"}
                    result = adapter.ingest(
                        {
                            "scope": scope,
                            "async_processing": True,
                            "auto_batch_extract": True,
                            "session_buffer_threshold": 20,
                            "idle_commit_timeout_ms": 60000,
                            "skip_prior_context": True,
                            "messages": [{"role": role, "content": text}],
                            "metadata": {"hook_type": hook_type, "codex_event": codex_event},
                        }
                    )
                    self.assertEqual("accepted", result["status"])

                    pack = adapter.retrieve(
                        {
                            "scope": scope,
                            "query": query,
                            "max_context_tokens": 260,
                            "audit_mode": "off",
                            "ranking": {
                                "max_selected_refs": 6,
                                "min_similarity_score": 0.0,
                                "budget_fill_policy": "force_fill",
                            },
                        }
                    )

                    event_refs = [
                        ref
                        for ref in pack["selected_refs"]
                        if ref.get("ref_type") == "event" and marker in str(ref.get("text") or "")
                    ]
                    self.assertTrue(event_refs, pack["selected_refs"])
                    self.assertEqual(event_type, event_refs[0]["event_type"])
                    self.assertEqual("pending_async_event", event_refs[0]["memory_layer"])
                    self.assertEqual("session", event_refs[0]["memory_scope"])
                    self.assertEqual("same_session", event_refs[0]["session_continuity"])

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
            time.sleep(0.05)

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
                            "content": "Remember: use Ubuntu /opt/github-services for TemporalStore work.",
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
                    record.get("entity_type") == "workspace_profile"
                    and "/opt/github-services" in str(record.get("state") or "")
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
                    "include_retrieval_metrics": True,
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

    def test_batch_extraction_keeps_entity_source_event_ids_role_specific(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-role-specific-entity-source-events.jsonl")
            scope = {
                "account_id": "acct_role_specific_events",
                "tenant_id": "tenant_role_specific_events",
                "user_id": "user_role_specific_events",
                "session_id": "session_role_specific_events",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {"role": "user", "content": "Please keep extracting live Codex memory."},
                        {
                            "role": "assistant",
                            "content": "Decision: profile promotion keeps marker amber sextant.",
                        },
                        {
                            "role": "tool",
                            "content": "Exit code: 0; validation passed for marker granite violin.",
                        },
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            event_hash_by_role = {
                record["source_role"]: record["event_id_hash"]
                for record in records
                if record.get("record_type") == "context_event"
            }
            self.assertIn("assistant", event_hash_by_role)
            self.assertIn("tool", event_hash_by_role)
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            assistant_entity = next(
                record for record in profile_entities if record.get("entity_type") == "assistant_decision"
            )
            tool_entity = next(record for record in profile_entities if record.get("entity_type") == "tool_evidence")
            self.assertEqual([event_hash_by_role["assistant"]], assistant_entity["source_event_ids"])
            self.assertEqual([event_hash_by_role["tool"]], tool_entity["source_event_ids"])
            self.assertNotIn(event_hash_by_role["tool"], assistant_entity["source_event_ids"])
            self.assertNotIn(event_hash_by_role["assistant"], tool_entity["source_event_ids"])
            self.assertEqual({"hook_boundary": 1}, assistant_entity["source_hook_type_counts"])
            self.assertEqual({"hook_boundary": 1}, tool_entity["source_hook_type_counts"])
            self.assertEqual({"Stop": 1}, assistant_entity["source_codex_event_counts"])
            self.assertEqual({"Stop": 1}, tool_entity["source_codex_event_counts"])
            self.assertIn(
                "selected_assistant_decision_outcome_only",
                assistant_entity["source_memory_selection_policies"],
            )
            self.assertNotIn("selected_tool_evidence_only", assistant_entity["source_memory_selection_policies"])
            self.assertIn("selected_tool_evidence_only", tool_entity["source_memory_selection_policies"])
            self.assertNotIn(
                "selected_assistant_decision_outcome_only",
                tool_entity["source_memory_selection_policies"],
            )

    # This test exercises context_segment rows, which are OFF unless a tenant asks for them.
    # Patched for the duration of the test only: setting it at module scope would leak across
    # the single-process suite run and flip the knob for tests that assert it is off.
    @mock.patch.dict(os.environ, {"MATRIXARK_EXTRACT_SEGMENTS": "1"})
    def test_batch_extraction_keeps_segment_selection_policies_role_specific(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-role-specific-segment-policies.jsonl")
            scope = {
                "account_id": "acct_role_specific_segments",
                "tenant_id": "tenant_role_specific_segments",
                "user_id": "user_role_specific_segments",
                "session_id": "session_role_specific_segments",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Decision: keep assistant segment marker prism ledger for memory retrieval.",
                        },
                        {
                            "role": "tool",
                            "content": "Exit code: 0; tool segment marker copper orbit validation passed and blocker cleared.",
                        },
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            segments = [record for record in records if record.get("record_type") == "context_segment"]
            self.assertTrue(segments)
            assistant_segments = [
                record
                for record in segments
                if "assistant" in record.get("source_roles", [])
                and "prism ledger" in str(record.get("text") or record.get("summary_text") or "")
            ]
            tool_segments = [
                record
                for record in segments
                if "tool" in record.get("source_roles", [])
                and "copper orbit" in str(record.get("text") or record.get("summary_text") or "")
            ]
            self.assertTrue(assistant_segments, segments)
            self.assertTrue(tool_segments, segments)
            self.assertIn(
                "selected_assistant_decision_outcome_only",
                assistant_segments[0]["source_memory_selection_policies"],
            )
            self.assertEqual({"hook_boundary": 1}, assistant_segments[0]["source_hook_type_counts"])
            self.assertEqual({"Stop": 1}, assistant_segments[0]["source_codex_event_counts"])
            self.assertNotIn("selected_tool_evidence_only", assistant_segments[0]["source_memory_selection_policies"])
            self.assertIn("selected_tool_evidence_only", tool_segments[0]["source_memory_selection_policies"])
            self.assertEqual({"hook_boundary": 1}, tool_segments[0]["source_hook_type_counts"])
            self.assertEqual({"Stop": 1}, tool_segments[0]["source_codex_event_counts"])
            self.assertNotIn(
                "selected_assistant_decision_outcome_only",
                tool_segments[0]["source_memory_selection_policies"],
            )

    def test_session_buffer_events_preserve_memory_selection_lineage_after_threshold_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-buffer-selection-lineage.jsonl")
            scope = {
                "account_id": "acct_buffer_lineage",
                "tenant_id": "tenant_buffer_lineage",
                "user_id": "user_buffer_lineage",
                "session_id": "session_buffer_lineage",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": False,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Capture the Codex user prompt for memory."}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )
            self.assertEqual(1, first["session_buffer"]["pending_event_count"])
            pending_rows = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "session_buffer_event"
                and record.get("status") == "pending"
            ]
            self.assertEqual(1, len(pending_rows))
            self.assertEqual({"selected_user_prompt": 1}, pending_rows[0]["source_memory_selection_policy_counts"])

            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Decision: threshold extraction should include assistant decisions.",
                        }
                    ],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                }
            )
            self.assertEqual(2, second["session_buffer"]["pending_event_count"])
            threshold = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 2,
                    "force": False,
                    "commit_reason": "threshold",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", threshold["status"])
            self.assertEqual(
                {"selected_assistant_decision_outcome_only": 1, "selected_user_prompt": 1},
                threshold["source_memory_selection_policy_counts"],
            )
            committed_rows = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "session_buffer_event"
                and record.get("commit_id_hash") == threshold["commit_id_hash"]
            ]
            self.assertEqual(2, len(committed_rows))
            self.assertTrue(
                all(
                    record.get("source_memory_selection_policy_counts")
                    == {"selected_assistant_decision_outcome_only": 1, "selected_user_prompt": 1}
                    for record in committed_rows
                )
            )

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

    def test_stop_boundary_finalizes_after_threshold_drained_session_without_reextracting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-stop-after-threshold.jsonl")
            scope = {
                "account_id": "acct_stop_after_threshold",
                "tenant_id": "tenant_stop_after_threshold",
                "user_id": "user_stop_after_threshold",
                "session_id": "session_stop_after_threshold",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": False,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "User asks whether Stop should duplicate extraction."}],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )
            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Decision: threshold extracts provisionally; Stop only finalizes the drained session.",
                        }
                    ],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                }
            )
            self.assertEqual(1, first["session_buffer"]["pending_event_count"])
            self.assertEqual(2, second["session_buffer"]["pending_event_count"])

            threshold = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 2,
                    "force": False,
                    "commit_reason": "threshold",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", threshold["status"])
            self.assertEqual("threshold", threshold["trigger_policy"])
            self.assertEqual("provisional", threshold["extraction_phase"])
            self.assertFalse(threshold["final_session_boundary"])
            self.assertFalse(adapter.pending_session_events(scope))

            stop = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": True,
                    "commit_reason": "hook_boundary",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("finalized", stop["status"])
            self.assertEqual("force", stop["trigger_policy"])
            self.assertEqual("final", stop["extraction_phase"])
            self.assertTrue(stop["final_session_boundary"])
            self.assertEqual(1, stop["prior_commit_count"])
            self.assertEqual(2, stop["prior_committed_event_count"])
            self.assertEqual("dirty_marked", stop["summary_refresh"]["status"])

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
            self.assertTrue(second_stop["trigger_evidence"]["already_finalized"])

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            boundaries = [
                record
                for record in records
                if record.get("record_type") == "context_session_boundary"
                and record.get("final_session_boundary")
            ]
            self.assertEqual(1, len(commits))
            self.assertEqual(1, len(boundaries))
            self.assertEqual(2, boundaries[0]["prior_committed_event_count"])
            self.assertEqual(["assistant", "user"], boundaries[0]["source_roles"])
            session_final_index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_summary"
                and record.get("ref_type") == "summary"
            }
            self.assertIn("summary_type:session_final", session_final_index_names)
            self.assertIn("memory_scope:session", session_final_index_names)
            self.assertIn("session_continuity:same_session", session_final_index_names)
            self.assertIn("extraction_phase:final", session_final_index_names)
            self.assertIn("final_session_boundary:true", session_final_index_names)
            self.assertIn("source_role:assistant", session_final_index_names)
            self.assertIn("source_role:user", session_final_index_names)
            self.assertIn("hook_type:after_llm", session_final_index_names)
            self.assertIn("hook_type:before_llm", session_final_index_names)
            self.assertIn("codex_event:stop", session_final_index_names)
            self.assertIn("codex_event:userpromptsubmit", session_final_index_names)
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", session_final_index_names)
            self.assertIn("memory_selection_policy:selected_user_prompt", session_final_index_names)
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "session_finalized"
                    and record.get("source_boundary_hash") == boundaries[0]["boundary_hash"]
                    for record in records
                )
            )
            refresh = adapter.refresh_dirty_node_summaries(
                scope=scope,
                limit=4,
                refreshed_at_ms=1785339000000,
                min_compression_event_age_ms=0,
            )
            self.assertGreaterEqual(refresh["refreshed_count"], 1)
            refreshed_records = adapter.read_all()
            boundary_summaries = [
                record
                for record in refreshed_records
                if record.get("record_type") == "context_summary"
                and record.get("source_final_session_boundary_count", 0) >= 1
                and boundaries[0]["boundary_hash"] in (record.get("source_operator_hashes") or [])
            ]
            self.assertTrue(boundary_summaries)
            self.assertTrue(any(record.get("final_session_boundary") for record in boundary_summaries))

    def test_threshold_commit_keeps_uncommitted_tail_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-threshold-tail.jsonl")
            scope = {
                "account_id": "acct_threshold_tail",
                "tenant_id": "tenant_threshold_tail",
                "user_id": "user_threshold_tail",
                "session_id": "session_threshold_tail",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": False,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            first = adapter.ingest({**base_args, "messages": [{"role": "user", "content": "first threshold message"}]})
            second = adapter.ingest({**base_args, "messages": [{"role": "assistant", "content": "second threshold tail"}]})
            self.assertEqual(1, first["session_buffer"]["pending_event_count"])
            self.assertEqual(2, second["session_buffer"]["pending_event_count"])

            threshold = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 1,
                    "force": False,
                    "commit_reason": "threshold",
                    "max_messages": 1,
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", threshold["status"])
            self.assertEqual("threshold", threshold["trigger_policy"])
            self.assertEqual(1, threshold["committed_event_count"])

            pending_after_threshold = adapter.pending_session_events(scope)
            self.assertEqual(1, len(pending_after_threshold))
            self.assertEqual(second["event_id_hash"], pending_after_threshold[0]["event_id_hash"])

    def test_compact_hot_prefix_preserves_boundary_session_commits(self) -> None:
        with mock.patch.object(matrixark_codex_hook, "HOOK_COMPACT_HOT_PREFIX_ONLY", True):
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("SessionIdle", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("Stop", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("UserPromptSubmit", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", "timeout"))

    # This test exercises context_segment rows, which are OFF unless a tenant asks for them.
    # Patched for the duration of the test only: setting it at module scope would leak across
    # the single-process suite run and flip the knob for tests that assert it is off.
    @mock.patch.dict(os.environ, {"MATRIXARK_EXTRACT_SEGMENTS": "1"})
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

    def test_batch_extract_promotes_user_directives_as_profile_entities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-user-directives.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_directive",
                        "tenant_id": "tenant_directive",
                        "user_id": "user_directive",
                        "session_id": "session_directive_1",
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "Do not store external raw logs outside TemporalStore. "
                                "Always push to remote main after code changes. "
                                "We should ingest assistant decisions into profile memory."
                            ),
                        },
                        {
                            "role": "assistant",
                            "content": "Done. I kept the hook path adapter-only and pushed the change.",
                        },
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )

            self.assertGreaterEqual(result.get("profile_entities_written", 0), 3)
            records = adapter.read_all()
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            preference_states = [
                str(record.get("state") or "")
                for record in profile_entities
                if record.get("entity_type") == "preference"
            ]
            plan_states = [
                str(record.get("state") or "")
                for record in profile_entities
                if record.get("entity_type") == "current_plan"
            ]
            self.assertTrue(any("Do not store external raw logs outside TemporalStore" in state for state in preference_states))
            self.assertTrue(any("push to remote main after code changes" in state for state in preference_states))
            self.assertTrue(any("assistant decisions into profile memory" in state for state in plan_states))
            user_directive_entities = [
                record
                for record in profile_entities
                if "external raw logs outside TemporalStore" in str(record.get("state") or "")
                or "push to remote main after code changes" in str(record.get("state") or "")
                or "assistant decisions into profile memory" in str(record.get("state") or "")
            ]
            self.assertTrue(user_directive_entities)
            for entity in user_directive_entities:
                self.assertEqual(["user"], entity.get("source_roles"))
                self.assertEqual({"user": 1}, entity.get("source_role_counts"))

            self.assertEqual(
                "profile_memory",
                infer_query_type("What should I remember about raw logs and pushing main?"),
            )
            self.assertEqual(
                "profile_memory",
                matrixark_mcp_query.infer_query_type("What are my standing instructions for raw logs?"),
            )
            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_directive",
                        "tenant_id": "tenant_directive",
                        "user_id": "user_directive",
                        "session_id": "session_directive_2",
                    },
                    "session_scope": "prefer",
                    "query": "What should I remember about raw logs and pushing main?",
                    "max_context_tokens": 400,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 4},
                }
            )
            selected_text = "\n".join(str(ref.get("text") or ref.get("summary_text") or "") for ref in pack["selected_refs"])
            self.assertIn("Do not store external raw logs outside TemporalStore", selected_text)
            self.assertIn("push to remote main after code changes", selected_text)

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
                        "codex_memory_selection": {
                            "policy": "selected_tool_evidence_only",
                            "selection_lossy": True,
                            "dropped_text_chars": 4096,
                            "dropped_line_count": 96,
                            "retained_text_ratio": 0.125,
                            "retained_line_ratio": 0.25,
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
                self.assertEqual(1, record.get("source_memory_selection_lossy_count"))
                self.assertEqual(4096, record.get("source_memory_selection_dropped_text_chars"))
                self.assertEqual(96, record.get("source_memory_selection_dropped_line_count"))
                self.assertEqual(0.125, record.get("source_memory_selection_retained_text_ratio_avg"))
            batch_level_records = [
                record
                for record in durable_records
                if record.get("record_type") in {"context_segment", "context_summary", "context_extraction_audit"}
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
            assistant_profile_entity = next(
                record for record in profile_entities if record.get("entity_type") == "assistant_decision"
            )
            tool_profile_entity = next(
                record for record in profile_entities if record.get("entity_type") == "tool_evidence"
            )
            self.assertEqual(["assistant"], assistant_profile_entity["source_roles"])
            self.assertEqual(["tool"], tool_profile_entity["source_roles"])
            for promoted_record in [assistant_profile_entity, tool_profile_entity]:
                self.assertIn("hook_boundary", promoted_record["source_hook_types"])
                self.assertIn("Stop", promoted_record["source_codex_events"])
            index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            profile_index_rows = [
                record
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            ]
            self.assertIn("memory_scope:user_profile", index_names)
            self.assertIn("session_continuity:cross_session", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("hook_type:hook_boundary", index_names)
            self.assertIn("codex_event:stop", index_names)
            self.assertIn("memory_selection_quality:lossy", index_names)
            self.assertTrue(any(str(name).startswith("entity_type:") for name in index_names))
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertTrue(profile_index_rows)
            self.assertTrue(all(record.get("memory_scope") == "user_profile" for record in profile_index_rows))
            self.assertTrue(all(record.get("session_continuity") == "cross_session" for record in profile_index_rows))
            self.assertTrue(all(record.get("profile_entity_current") is True for record in profile_index_rows))
            self.assertTrue(all(record.get("profile_revision", 0) >= 1 for record in profile_index_rows))
            self.assertTrue(all(record.get("promoted_from_memory_scope") == "session" for record in profile_index_rows))
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
                    "include_retrieval_metrics": True,
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
            self.assertNotIn("profile_promotion_policy", retrieved_tool_profile_ref)
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
                    "max_context_tokens": 400,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 6},
                }
            )
            self.assertLessEqual(decision_pack["used_context_tokens"], 400)
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
            self.assertNotIn("profile_promotion_policy", retrieved_decision_profile_ref)
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
                and record.get("source_ref_type") == "entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
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
            session_dirty_records = [
                record
                for record in records
                if record.get("record_type") == "context_summary_dirty"
                and record.get("source_ref_type") == "batch"
                and record.get("source_batch_hash") == result["batch_id_hash"]
            ]
            self.assertTrue(session_dirty_records)
            self.assertTrue(all(record.get("memory_scope") == "session" for record in session_dirty_records))
            self.assertTrue(all(record.get("session_continuity") == "same_session" for record in session_dirty_records))
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
            profile_summary_hashes = {summary.get("summary_hash") for summary in profile_summaries}
            summary_index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_summary"
                and record.get("ref_type") == "summary"
                and profile_summary_hashes.intersection(set(record.get("ref_hashes", []) or []))
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
            self.assertFalse(any("final_session_boundary" in item for item in served_items))
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
            self.assertFalse(any("final_session_boundary" in item for item in summary_items))

    def test_current_state_pre_retrieval_refresh_injects_profile_summary_layer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-current-refresh-profile-summary.jsonl")
            scope = {
                "account_id": "acct_current_refresh",
                "tenant_id": "tenant_current_refresh",
                "user_id": "user_current_refresh",
                "session_id": "session_current_refresh",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Current decision: preserve always-promoted profile summaries for latest retrieval.",
                        },
                        {
                            "role": "tool",
                            "content": "Tool evidence: current profile summary refresh should be returned with the profile entity.",
                        },
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )
            self.assertGreaterEqual(result.get("profile_entities_written", 0), 1)
            self.assertTrue(result["summary_refresh"]["profile_summary_refresh_required"])

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_current_refresh_later"},
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the current profile summary refresh decision and tool evidence?",
                    "max_context_tokens": 180,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "ranking": {
                        "max_selected_refs": 3,
                        "min_similarity_score": 0.0,
                        "pre_retrieval_summary_refresh": True,
                        "pre_retrieval_summary_refresh_limit": 8,
                    },
                }
            )

            self.assertEqual("refreshed", pack["pre_retrieval_summary_refresh"]["status"])
            selected_refs = pack["selected_refs"]
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    for ref in selected_refs
                ),
                selected_refs,
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "summary"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    for ref in selected_refs
                ),
                selected_refs,
            )
            budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_layer"]["profile_summary"]["refs"], 1)
            self.assertGreaterEqual(budget["by_memory_layer"]["profile_entity"]["refs"], 1)

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
                and record.get("entity_name") == "assistant_decision"
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
            # The llm/model -> assistant normalisation is proven above, on the records: the
            # events carry source_role "assistant" with original_source_role in {llm, model},
            # and the profile decision carries source_roles ["assistant", "user"]. This block
            # re-checked the same thing through a posting, and #562 stopped writing a posting
            # whose term the record it points at already carries -- `source_role` is exactly
            # such a field, so all three of these names are gone rather than just the aliases.
            self.assertNotIn("source_role:assistant", index_names)
            self.assertNotIn("source_role:llm", index_names)
            self.assertNotIn("source_role:model", index_names)
            # ...and the entity really was indexed, so the three checks above cannot pass just
            # because nothing was written at all.
            indexed = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
            }
            self.assertIn("entity_type:assistant_decision", indexed)

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

    def test_selected_assistant_outcome_facts_promote_to_profile_entity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-assistant-outcomes.jsonl")
            scope = {
                "account_id": "acct_profile_outcome",
                "tenant_id": "tenant_profile_outcome",
                "user_id": "user_profile_outcome",
                "session_id": "session_profile_outcome_1",
            }
            raw = "\n".join(
                [
                    "verbose implementation explanation " * 120,
                    "Implemented profile entity promotion and pushed commit def7890 to origin/main.",
                    "Validation ran 112 tests passed.",
                    "Changed assistant outcome extraction to preserve count-only facts.",
                    "Next: continue retrieval budget tuning.",
                ]
            )
            selected = matrixark_codex_hook.selected_assistant_memory_text(raw)
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {"role": "user", "content": "Keep assistant outcome facts in profile memory."},
                        {"role": "assistant", "content": selected},
                    ],
                    "metadata": {
                        "source_roles": ["assistant"],
                        "hook_type": "after_llm",
                        "codex_event": "Stop",
                        "codex_memory_selection": matrixark_codex_hook.codex_memory_selection_metadata(
                            role="assistant",
                            event="Stop",
                            text=selected,
                            original_text=raw,
                        ),
                    },
                    "force": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            profile_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "assistant_decision"
                and record.get("entity_name") == "assistant_decision"
            ]
            self.assertTrue(profile_decisions)
            state = "\n".join(str(record.get("state") or "") for record in profile_decisions)
            self.assertIn("pushed commit def7890 to origin/main", state)
            self.assertIn("112 tests passed", state)
            self.assertIn("assistant outcome extraction", state)
            self.assertNotIn("verbose implementation explanation verbose implementation", state)

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
                and record.get("entity_name") == "assistant_decision"
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
            self.assertEqual(["always_when_profile_scope_available"], profile_decision["source_profile_promotion_policies"])
            self.assertEqual([], profile_decision["source_profile_promotion_blockers"])
            self.assertEqual(2, profile_decision["profile_revision"])
            self.assertTrue(profile_decision["profile_entity_current"])
            self.assertEqual(1, profile_decision["previous_profile_revision"])
            self.assertGreater(profile_decision["previous_profile_updated_at_ms"], 0)
            self.assertIn(
                profile_decision["supersedes_session_entity_hash"],
                profile_decision["supersedes_session_entity_hashes"],
            )
            self.assertEqual(
                profile_decision["source_entity_hashes"],
                profile_decision["supersedes_session_entity_hashes"],
            )
            self.assertIn("aaa111", profile_decision["state"])
            self.assertIn("bbb222", profile_decision["state"])
            # The embedding folded onto its owner: the entity record carries the vector, and
            # the retired record's lineage rides along under embedding_meta.
            profile_decision_owner = next(
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("entity_hash") == profile_decision["entity_hash"]
                and record.get("vector")
            )
            embedding_meta = profile_decision_owner.get("embedding_meta") or {}
            self.assertEqual("user_profile",
                             embedding_meta.get("memory_scope") or profile_decision_owner.get("memory_scope"))
            self.assertEqual("cross_session",
                             embedding_meta.get("session_continuity") or profile_decision_owner.get("session_continuity"))
            self.assertNotIn("supersedes_session_entity_hashes", embedding_meta)
            superseded_session_entity_hashes = list(profile_decision["supersedes_session_entity_hashes"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_3"},
                    "session_scope": "prefer",
                    "query": "aaa111 bbb222 assistant",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
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
                    "include_retrieval_metrics": True,
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
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0},
                }
            )
            current_ref = next(
                ref
                for ref in current_pack["selected_refs"]
                if ref.get("entity_name") == "assistant_decision"
            )
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
            self.assertEqual(2, current_ref["profile_revision"])
            self.assertEqual(1, current_ref["previous_profile_revision"])
            self.assertGreater(current_ref["previous_profile_updated_at_ms"], 0)
            self.assertIn(
                current_ref["supersedes_session_entity_hash"],
                current_ref["supersedes_session_entity_hashes"],
            )
            self.assertEqual(
                superseded_session_entity_hashes,
                current_ref["supersedes_session_entity_hashes"],
            )
            self.assertEqual(2, current_ref["supersedes_session_entity_count"])
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
            self.assertNotIn("profile_source_session_count", default_current_ref)
            self.assertNotIn("profile_source_entity_count", default_current_ref)
            self.assertNotIn("current_state_policy", default_current_ref)
            self.assertNotIn("current_state_source_session_count", default_current_ref)
            self.assertNotIn("current_state_source_entity_count", default_current_ref)
            self.assertNotIn("profile_revision", default_current_ref)
            self.assertNotIn("previous_profile_revision", default_current_ref)
            self.assertNotIn("previous_profile_updated_at_ms", default_current_ref)
            self.assertNotIn("supersedes_session_entity_hash", default_current_ref)
            self.assertNotIn("supersedes_session_entity_hashes", default_current_ref)
            self.assertNotIn("supersedes_session_entity_count", default_current_ref)
            self.assertNotIn("dropped_memory_layer_budget", default_current_pack)
            self.assertNotIn("memory_layer_pressure", default_current_pack)

    def test_profile_preference_correction_supersedes_stale_cross_session_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-preference-correction.jsonl")
            base_scope = {
                "account_id": "acct_profile_preference_correction",
                "tenant_id": "tenant_profile_preference_correction",
                "user_id": "user_profile_preference_correction",
            }
            first = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_preference_1"},
                    "messages": [
                        {
                            "role": "user",
                            "content": "I prefer waiting for Stop before extracting memories.",
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "force": True,
                }
            )
            second = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_preference_2"},
                    "messages": [
                        {
                            "role": "user",
                            "content": "I prefer threshold or idle extraction now instead of waiting for Stop.",
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "force": True,
                }
            )

            self.assertGreaterEqual(first.get("profile_entities_written", 0), 1)
            self.assertGreaterEqual(second.get("profile_entities_written", 0), 1)
            profile_preferences = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "preference"
                and record.get("entity_name") == "preference"
            ]
            self.assertEqual(1, len(profile_preferences), profile_preferences)
            profile_preference = profile_preferences[0]
            self.assertEqual(2, profile_preference["profile_revision"])
            self.assertEqual(
                ["session_profile_preference_1", "session_profile_preference_2"],
                profile_preference["source_session_ids"],
            )
            self.assertIn("threshold or idle extraction", profile_preference["state"])
            self.assertNotIn("prefer waiting for Stop before extracting memories", profile_preference["state"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_preference_3"},
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the current extraction preference?",
                    "max_context_tokens": 180,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                }
            )
            self.assertEqual(1, len(pack["selected_refs"]))
            current_ref = pack["selected_refs"][0]
            self.assertEqual("user_profile", current_ref["memory_scope"])
            self.assertEqual("cross_session", current_ref["session_continuity"])
            self.assertIn("threshold or idle extraction", current_ref["text"])
            self.assertNotIn("prefer waiting for Stop before extracting memories", current_ref["text"])

            natural_pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_preference_4"},
                    "session_scope": "prefer",
                    "query": "What should you always remember about extraction by default?",
                    "max_context_tokens": 180,
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                }
            )
            self.assertEqual(1, len(natural_pack["selected_refs"]))
            natural_ref = natural_pack["selected_refs"][0]
            self.assertEqual("user_profile", natural_ref["memory_scope"])
            self.assertEqual("cross_session", natural_ref["session_continuity"])
            self.assertIn("threshold or idle extraction", natural_ref["text"])
            self.assertGreaterEqual(
                natural_pack["retrieval_metrics"]["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
                1,
            )

    def test_retrieval_recovers_profile_layer_from_compact_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-embedding-recovery.jsonl")
            scope = {
                "account_id": "acct_embed_recovery",
                "tenant_id": "tenant_embed_recovery",
                "user_id": "user_embed_recovery",
                "session_id": "session_current",
            }
            profile_scope = {
                "account_id": "acct_embed_recovery",
                "tenant_id": "tenant_embed_recovery",
                "user_id": "user_embed_recovery",
            }
            node_path = ["tenant:tenant_embed_recovery", "user:user_embed_recovery", "profile:long_term_memory"]
            node_hash = 91001
            entity_hash = 91002
            stale_entity_hash = 91005
            stale_profile_text = "assistant_decision: latest_profile_marker_991 = stale_profile_marker_991 disabled threshold extraction."
            profile_text = "assistant_decision: latest_profile_marker_991 = Keep threshold and idle extraction enabled."
            adapter.append_many(
                [
                    {
                        "record_type": "context_entity",
                        "entity_hash": stale_entity_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": profile_scope,
                        "access_scope": profile_scope,
                        "entity_type": "assistant_decision",
                        "entity_name": "latest_profile_marker_991",
                        "state": "stale_profile_marker_991 disabled threshold extraction.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_entity_current": False,
                        "profile_revision": 1,
                        "updated_at_ms": 900,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "entity_state",
                        "ref_type": "entity",
                        "ref_hash": stale_entity_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(stale_profile_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(stale_profile_text),
                        "scope": profile_scope,
                        "access_scope": profile_scope,
                        "entity_type": "assistant_decision",
                        "entity_name": "latest_profile_marker_991",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_entity_current": False,
                        "profile_revision": 1,
                        "updated_at_ms": 900,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": entity_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": profile_scope,
                        "access_scope": profile_scope,
                        "entity_type": "assistant_decision",
                        "entity_name": "latest_profile_marker_991",
                        "state": "Keep threshold and idle extraction enabled.",
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "entity_state",
                        "ref_type": "entity",
                        "ref_hash": entity_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(profile_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(profile_text),
                        "scope": profile_scope,
                        "entity_type": "assistant_decision",
                        "entity_name": "latest_profile_marker_991",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "promoted_from_memory_scope": "session",
                        "profile_promotion_policy": "always_when_profile_scope_available",
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "source_hook_types": ["after_llm"],
                        "source_hook_type_counts": {"after_llm": 1},
                        "source_codex_events": ["Stop"],
                        "source_codex_event_counts": {"Stop": 1},
                        "source_session_ids": ["session_prior"],
                        "source_event_ids": [91003],
                        "source_entity_hashes": [91004],
                        "extraction_context_event_ids": [91003],
                        "source_memory_scopes": ["session", "user_profile"],
                        "source_session_continuities": ["same_session", "cross_session"],
                        "profile_entity_current": True,
                        "profile_revision": 2,
                        "previous_profile_revision": 1,
                        "previous_profile_updated_at_ms": 900,
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "updated_at_ms": 1000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "What is the latest assistant decision for profile marker 991 threshold idle extraction?",
                    "question_type": "current_state",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_retrieval_debug": True,
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0},
                }
            )
            profile_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "assistant_decision"
                and "latest_profile_marker_991" in ref.get("text", "")
            ]
            self.assertTrue(profile_refs, pack["selected_refs"])
            self.assertFalse(
                any("stale_profile_marker_991" in ref.get("text", "") for ref in pack["selected_refs"]),
                pack["selected_refs"],
            )
            profile_ref = profile_refs[0]
            self.assertEqual("user_profile", profile_ref["memory_scope"])
            self.assertEqual("cross_session", profile_ref["session_continuity"])
            self.assertNotIn("source_session_ids", profile_ref)
            self.assertNotIn("source_event_ids", profile_ref)
            self.assertNotIn("source_entity_hashes", profile_ref)
            self.assertNotIn("source_role_counts", profile_ref)
            self.assertNotIn("profile_source_session_count", profile_ref)
            self.assertNotIn("profile_source_entity_count", profile_ref)
            pushdown = pack["recall_policy"]["backend_retrieval_pushdown"]
            self.assertTrue(pushdown["secondary_index_prefilter_enabled"], pushdown)
            self.assertEqual(0, pushdown["secondary_index_matched_posting_count"])
            self.assertGreaterEqual(pushdown["secondary_embedding_matched_posting_count"], 1)
            budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertNotIn("by_source_role", budget)

            debug_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "latest profile marker 991 threshold idle extraction",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0},
                }
            )
            debug_ref = next(
                ref
                for ref in debug_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and "latest_profile_marker_991" in ref.get("text", "")
            )
            self.assertEqual({"assistant": 1}, debug_ref["source_role_counts"])
            self.assertEqual({"after_llm": 1}, debug_ref["source_hook_type_counts"])
            self.assertEqual({"Stop": 1}, debug_ref["source_codex_event_counts"])
            self.assertEqual(1, debug_ref["profile_source_session_count"])
            self.assertEqual(1, debug_ref["profile_source_entity_count"])
            self.assertEqual(1, debug_ref["current_state_source_session_count"])
            self.assertEqual(1, debug_ref["current_state_source_entity_count"])
            self.assertTrue(debug_ref["profile_entity_current"])
            self.assertEqual(2, debug_ref["profile_revision"])

    def test_retrieval_recovers_profile_summary_layer_from_compact_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-summary-embedding-recovery.jsonl")
            scope = {
                "account_id": "acct_summary_embed_recovery",
                "tenant_id": "tenant_summary_embed_recovery",
                "user_id": "user_summary_embed_recovery",
                "session_id": "session_current_summary",
            }
            profile_scope = {
                "account_id": "acct_summary_embed_recovery",
                "tenant_id": "tenant_summary_embed_recovery",
                "user_id": "user_summary_embed_recovery",
            }
            node_path = ["profile:long_term_memory"]
            node_hash = 92001
            summary_hash = 92002
            summary_text = "profile summary: latest_summary_marker_992 says keep profile summaries retrievable."
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_type": "node_l0",
                        "summary_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_text": summary_text,
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_summary_dirty",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": profile_scope,
                        "reason": "profile_summary_embedding_recovery_test",
                        "dirty_at_ms": 900,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "node_l0",
                        "ref_type": "summary",
                        "ref_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(summary_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(summary_text),
                        "access_scope": profile_scope,
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "promoted_from_memory_scope": "session",
                        "profile_promotion_policy": "always_when_profile_scope_available",
                        "source_roles": ["assistant", "tool"],
                        "source_role_counts": {"assistant": 1, "tool": 1},
                        "source_hook_types": ["after_llm", "tool_result"],
                        "source_hook_type_counts": {"after_llm": 1, "tool_result": 1},
                        "source_codex_events": ["Stop", "PostToolUse"],
                        "source_codex_event_counts": {"Stop": 1, "PostToolUse": 1},
                        "source_memory_selection_policies": [
                            "selected_assistant_decision_outcome_only",
                            "selected_tool_evidence_only",
                        ],
                        "source_memory_selection_policy_counts": {
                            "selected_assistant_decision_outcome_only": 1,
                            "selected_tool_evidence_only": 1,
                        },
                        "source_session_ids": ["session_prior_summary"],
                        "source_event_ids": [92003],
                        "source_entity_hashes": [92004],
                        "extraction_context_event_ids": [92003],
                        "source_memory_scopes": ["session", "user_profile"],
                        "source_session_continuities": ["same_session", "cross_session"],
                        "source_extraction_phases": ["final"],
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "updated_at_ms": 1000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "latest summary marker 992 profile summaries retrievable",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0},
                    "pre_retrieval_summary_refresh": True,
                    "pre_retrieval_summary_refresh_limit": 1,
                }
            )
            summary_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "summary" and "latest_summary_marker_992" in ref.get("text", "")
            ]
            self.assertTrue(summary_refs, pack["selected_refs"])
            summary_ref = summary_refs[0]
            self.assertEqual("user_profile", summary_ref["memory_scope"])
            self.assertEqual("cross_session", summary_ref["session_continuity"])
            self.assertNotIn("source_session_ids", summary_ref)
            self.assertNotIn("source_event_ids", summary_ref)
            self.assertNotIn("source_entity_hashes", summary_ref)
            self.assertNotIn("source_role_counts", summary_ref)
            self.assertNotIn("source_memory_selection_policy_counts", summary_ref)
            self.assertNotIn("profile_source_session_count", summary_ref)
            self.assertNotIn("profile_source_entity_count", summary_ref)
            budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(budget["by_memory_layer"]["profile_summary"]["refs"], 1)

            debug_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "latest summary marker 992 profile summaries retrievable",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 3, "min_similarity_score": 0.0},
                    "pre_retrieval_summary_refresh": True,
                    "pre_retrieval_summary_refresh_limit": 1,
                }
            )
            debug_ref = next(
                ref
                for ref in debug_pack["selected_refs"]
                if ref.get("ref_type") == "summary" and "latest_summary_marker_992" in ref.get("text", "")
            )
            self.assertNotIn("source_session_ids", debug_ref)
            self.assertNotIn("source_event_ids", debug_ref)
            self.assertNotIn("extraction_context_event_ids", debug_ref)
            self.assertNotIn("source_event_count", debug_ref)
            self.assertNotIn("source_entity_count", debug_ref)
            self.assertEqual({"assistant": 1, "tool": 1}, debug_ref["source_role_counts"])
            self.assertEqual({"after_llm": 1, "tool_result": 1}, debug_ref["source_hook_type_counts"])
            self.assertEqual({"Stop": 1, "PostToolUse": 1}, debug_ref["source_codex_event_counts"])
            self.assertEqual(
                {"selected_assistant_decision_outcome_only": 1, "selected_tool_evidence_only": 1},
                debug_ref["source_memory_selection_policy_counts"],
            )
            self.assertEqual(1, debug_ref["profile_source_session_count"])
            self.assertEqual(1, debug_ref["profile_source_entity_count"])


    def test_retrieval_prefilter_recovers_summary_from_compact_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-summary-prefilter-recovery.jsonl")
            scope = {
                "account_id": "acct_summary_prefilter",
                "tenant_id": "tenant_summary_prefilter",
                "user_id": "user_summary_prefilter",
                "session_id": "session_summary_prefilter",
            }
            profile_scope = {
                "account_id": "acct_summary_prefilter",
                "tenant_id": "tenant_summary_prefilter",
                "user_id": "user_summary_prefilter",
            }
            summary_text = "summary prefilter marker 993 keeps profile summary rows reachable from embeddings."
            summary_hash = 93001
            node_hash = 93002
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_type": "node_l0",
                        "summary_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": ["profile:long_term_memory"],
                        "summary_text": summary_text,
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "node_l0",
                        "ref_type": "summary",
                        "ref_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": ["profile:long_term_memory"],
                        "summary_type": "node_l0",
                        "dim": len(embedding_for_text(summary_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(summary_text),
                        "access_scope": profile_scope,
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "source_entity_types": ["assistant_decision"],
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "source_hook_types": ["after_llm"],
                        "source_hook_type_counts": {"after_llm": 1},
                        "source_codex_events": ["Stop"],
                        "source_codex_event_counts": {"Stop": 1},
                        "updated_at_ms": 1000,
                    },
                ]
            )

            result = adapter.retrieval_records(
                scope=scope,
                record_types={"context_summary"},
                secondary_index_groups=[{"summary_type:node_l0"}],
            )
            self.assertEqual(0, result["scan_stats"]["secondary_index_matched_posting_count"])
            self.assertGreaterEqual(result["scan_stats"]["secondary_embedding_matched_posting_count"], 1)
            summaries = [record for record in result["records"] if record.get("record_type") == "context_summary"]
            self.assertTrue(any(record.get("summary_hash") == summary_hash for record in summaries), result)

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "question_type": "profile_memory",
                    "query": "profile summary rows reachable from embeddings marker 993",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "include_debug_refs": True,
                    "ranking": {"max_selected_refs": 2, "min_similarity_score": 0.0},
                }
            )
            summary_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "summary"
                and ref.get("summary_type") == "node_l0"
                and "marker 993" in ref.get("text", "")
            ]
            self.assertTrue(summary_refs, pack["selected_refs"])
            summary_ref = summary_refs[0]
            self.assertEqual("user_profile", summary_ref["memory_scope"])
            self.assertEqual("cross_session", summary_ref["session_continuity"])
            self.assertEqual({"assistant": 1}, summary_ref["source_role_counts"])
            budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_layer"]["profile_summary"]["refs"], 1)

    def test_retrieval_prefilter_recovers_segment_from_compact_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-segment-prefilter-recovery.jsonl")
            scope = {
                "account_id": "acct_segment_prefilter",
                "tenant_id": "tenant_segment_prefilter",
                "user_id": "user_segment_prefilter",
                "session_id": "session_segment_prefilter",
            }
            segment_text = "segment prefilter marker 994 records threshold and idle extraction decisions."
            segment_hash = 94001
            adapter.append_many(
                [
                    {
                        "record_type": "context_segment",
                        "segment_hash": segment_hash,
                        "segment_text": segment_text,
                        "topic": "threshold_idle_extraction",
                        "scope": scope,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "segment_text",
                        "ref_type": "segment",
                        "ref_hash": segment_hash,
                        "topic": "threshold_idle_extraction",
                        "dim": len(embedding_for_text(segment_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(segment_text),
                        "access_scope": scope,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["user", "assistant"],
                        "source_hook_types": ["before_llm", "after_llm"],
                        "updated_at_ms": 1000,
                    },
                ]
            )

            result = adapter.retrieval_records(
                scope=scope,
                record_types={"context_segment"},
                secondary_index_groups=[{"segment_topic:threshold_idle_extraction"}],
            )
            self.assertEqual(0, result["scan_stats"]["secondary_index_matched_posting_count"])
            self.assertGreaterEqual(result["scan_stats"]["secondary_embedding_matched_posting_count"], 1)
            segments = [record for record in result["records"] if record.get("record_type") == "context_segment"]
            self.assertTrue(any(record.get("segment_hash") == segment_hash for record in segments), result)

    def test_retrieval_prefilter_recovers_compression_from_compact_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-compression-prefilter-recovery.jsonl")
            scope = {
                "account_id": "acct_compression_prefilter",
                "tenant_id": "tenant_compression_prefilter",
                "user_id": "user_compression_prefilter",
                "session_id": "session_compression_prefilter",
            }
            compression_text = "compression prefilter marker 995 keeps time compression rows reachable."
            compression_hash = 95001
            adapter.append_many(
                [
                    {
                        "record_type": "context_compression_event",
                        "compression_id_hash": compression_hash,
                        "summary_text": compression_text,
                        "operator": "TIME_COMPRESS",
                        "scope": scope,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "compression_summary",
                        "ref_type": "compression",
                        "ref_hash": compression_hash,
                        "operator": "TIME_COMPRESS",
                        "dim": len(embedding_for_text(compression_text)),
                        "model": "deterministic-test",
                        "vector": embedding_for_text(compression_text),
                        "access_scope": scope,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["assistant"],
                        "source_hook_types": ["after_llm"],
                        "updated_at_ms": 1000,
                    },
                ]
            )

            result = adapter.retrieval_records(
                scope=scope,
                record_types={"context_compression_event"},
                secondary_index_groups=[{"context_class:compression"}],
            )
            self.assertEqual(0, result["scan_stats"]["secondary_index_matched_posting_count"])
            self.assertGreaterEqual(result["scan_stats"]["secondary_embedding_matched_posting_count"], 1)
            compressions = [
                record for record in result["records"] if record.get("record_type") == "context_compression_event"
            ]
            self.assertTrue(
                any(record.get("compression_id_hash") == compression_hash for record in compressions),
                result,
            )

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
            try:
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
                # Folded: the vectors live on the owners (chunks and summaries carry them).
                self.assertTrue(any(record.get("vector") for record in records))
                self.assertTrue(any(record.get("record_type") == "context_summary" for record in records))
            finally:
                # The import runs on a worker thread that keeps writing into this directory --
                # the event log and the read-cache sidecars each append maintains -- until the
                # task is drained. Without this the temp dir is torn down underneath the worker,
                # which re-creates the log it just deleted and fails cleanup with ENOTEMPTY.
                server.close(timeout_s=10.0)

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
            self.assertGreaterEqual(telemetry["used_remote_context_tokens"], 0)
            self.assertIn("memory_layer_budget", telemetry)
            self.assertEqual("auto", msg["retrieve"]["layers"]["memory_selection_policy_budget"]["mode"])
            self.assertEqual("auto", telemetry["memory_selection_policy_budget"]["mode"])
            self.assertTrue(telemetry["memory_selection_policy_budget"]["enabled"])
            self.assertIn("selected_tool_evidence_only", telemetry["memory_selection_policy_budget"]["budget_tokens"])
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
            self.assertEqual("auto", prompt_replay_telemetry[-1]["memory_selection_policy_budget"]["mode"])
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

