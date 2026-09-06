# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexPipelinePart4 methods split from test_matrixark_codex_hook_pipeline.MatrixArkCodexHookPipelineTest (mixin)."""
from __future__ import annotations

import os
from unittest import mock

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # package path
    from tools.matrixark_mcp_local_adapter import expand_interned_records
except ImportError:
    from matrixark_mcp_local_adapter import expand_interned_records

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_pipeline import (
    CountingLocalAdapter,
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    Path,
    candidate_memory_layer_name,
    compact_context_pack_for_serving,
    compact_context_pack_ref,
    identity_hashes,
    io,
    json,
    matrixark_codex_hook,
    mock,
    os,
    refresh_final_selected_budget_policies,
    subprocess,
    suppress_extracted_represented_pending_events,
    sys,
    tempfile,
    time,
)
except ImportError:
    from test_matrixark_codex_hook_pipeline import (
    CountingLocalAdapter,
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    Path,
    candidate_memory_layer_name,
    compact_context_pack_for_serving,
    compact_context_pack_ref,
    identity_hashes,
    io,
    json,
    matrixark_codex_hook,
    mock,
    os,
    refresh_final_selected_budget_policies,
    subprocess,
    suppress_extracted_represented_pending_events,
    sys,
    tempfile,
    time,
)


class _CodexPipelinePart4:
    def test_retrieve_recovers_hot_event_type_from_embedding_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark-hot-event-embedding-recovery.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            scope = {
                "account_id": "acct_hot_event_recovery",
                "tenant_id": "tenant_hot_event_recovery",
                "user_id": "user_hot_event_recovery",
                "session_id": "session_hot_event_recovery",
            }
            adapter.ingest(
                {
                    "scope": scope,
                    "metadata": {
                        "codex_memory_selection": {
                            "policy": "selected_assistant_decision_outcome_only",
                            "source_role": "assistant",
                            "codex_event": "Stop",
                            "selection_lossy": True,
                        }
                    },
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Decision: recover assistant response type from hot event embedding metadata.",
                        }
                    ],
                },
                hook={
                    "source": "codex",
                    "hook_type": "after_llm",
                    "hook_id": "Stop:hot-event-embedding-recovery",
                    "observed_at_ms": 1000,
                    "idempotency_key": "hot-event-embedding-recovery",
                    "trigger": "Stop",
                    "auto_captured": True,
                },
            )

            sparse_records = []
            for record in adapter.read_all():
                if record.get("record_type") == "context_index":
                    continue
                if record.get("record_type") == "context_summary":
                    continue
                if record.get("record_type") == "context_embedding" and record.get("ref_type") == "summary":
                    continue
                if record.get("record_type") == "context_event":
                    record = dict(record)
                    for key in ("event_type", "classification", "status", "source_kind"):
                        record.pop(key, None)
                    record["scope"] = scope
                sparse_records.append(record)
            event_log.write_text(
                "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in sparse_records),
                encoding="utf-8",
            )
            adapter = MatrixArkLocalAdapter(event_log)

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "_session_scope": "prefer"},
                    "query": "What selected assistant decision outcome did Codex implement?",
                    "question_type": "current_state",
                    "max_context_tokens": 240,
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "include_retrieval_debug": True,
                    "include_debug_refs": True,
                    "audit_mode": "off",
                }
            )
            selected_events = [ref for ref in pack["selected_refs"] if ref.get("ref_type") == "event"]
            self.assertTrue(selected_events, pack["selected_refs"])
            recovered = selected_events[0]
            self.assertEqual("assistant_response", recovered.get("event_type"))
            pushdown = pack["recall_policy"]["backend_retrieval_pushdown"]
            self.assertTrue(pushdown["secondary_index_prefilter_enabled"], pushdown)
            self.assertEqual(0, pushdown["secondary_index_matched_posting_count"])
            self.assertGreaterEqual(pushdown["secondary_embedding_matched_posting_count"], 1)

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
            self.assertIsInstance(metrics, dict)
            self.assertLessEqual(pack["used_context_tokens"], 100)

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
            self.assertNotIn("retrieval_metrics", pack)
            self.assertNotIn("memory_layer_budget", pack)

    def test_final_budget_policy_counters_follow_pending_event_dedupe(self) -> None:
        pending_event_hash = 424242
        selected = [
            {
                "ref_type": "event",
                "ref_hash": pending_event_hash,
                "event_type": "pending_async",
                "classification": "PENDING_ASYNC_EXTRACTION",
                "extraction_status": "pending",
                "text": "assistant pending raw event",
                "source_roles": ["assistant"],
                "source_role_counts": {"assistant": 1},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                "extraction_phase": "pending_async",
                "token_estimate": 6,
            },
            {
                "ref_type": "entity",
                "ref_hash": 99,
                "text": "user profile entity from extracted assistant event",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_event_ids": [pending_event_hash],
                "source_roles": ["assistant"],
                "source_role_counts": {"assistant": 1},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                "extraction_phase": "provisional",
                "budget_memory_layer": "profile_entity",
                "token_estimate": 9,
            },
        ]
        dropped = {
            "source_role_budget_policy": {
                "enabled": True,
                "budget_tokens": {"assistant": 64},
                "selected_tokens_by_role": {"assistant": 15},
                "selected_ref_count_by_role": {"assistant": 2},
            },
            "memory_selection_policy_budget_policy": {
                "enabled": True,
                "budget_tokens": {"selected_assistant_decision_outcome_only": 64},
                "selected_tokens_by_policy": {"selected_assistant_decision_outcome_only": 15},
                "selected_ref_count_by_policy": {"selected_assistant_decision_outcome_only": 2},
            },
            "memory_layer_budget_policy": {
                "enabled": True,
                "budget_tokens": {"pending_async_event": 64, "profile_entity": 64},
                "selected_tokens_by_layer": {"pending_async_event": 6, "profile_entity": 9},
                "selected_ref_count_by_layer": {"pending_async_event": 1, "profile_entity": 1},
            },
        }

        selected, removed_tokens = suppress_extracted_represented_pending_events(selected, dropped)
        self.assertEqual(6, removed_tokens)
        refresh_final_selected_budget_policies(selected, dropped)

        self.assertEqual({"assistant": 9}, dropped["source_role_budget_policy"]["selected_tokens_by_role"])
        self.assertEqual({"assistant": 1}, dropped["source_role_budget_policy"]["selected_ref_count_by_role"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 9},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            dropped["memory_selection_policy_budget_policy"]["selected_ref_count_by_policy"],
        )
        self.assertEqual(
            {"pending_async_event": 0, "profile_entity": 9},
            dropped["memory_layer_budget_policy"]["selected_tokens_by_layer"],
        )
        self.assertEqual(
            {"profile_entity": 1},
            dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"],
        )

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

    def test_retrieve_source_role_budget_applies_to_raw_context_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-event-source-role-budget.jsonl")
            scope = {
                "account_id": "acct_event_source_role_budget",
                "tenant_id": "tenant_event_source_role_budget",
                "user_id": "user_event_source_role_budget",
                "session_id": "session_event_source_role_budget",
            }
            scope = {
                **scope,
                **identity_hashes(
                    scope["account_id"],
                    scope["tenant_id"],
                    scope["user_id"],
                    scope["session_id"],
                ),
            }
            node_path = [
                "tenant:tenant_event_source_role_budget",
                "user:user_event_source_role_budget",
                "session:session_event_source_role_budget",
            ]
            node_hash = 9123401
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 9123402,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "tool evidence marker: Exit code 0 and hook pipeline passed.",
                        "summary_text": "tool evidence marker",
                        "event_type": "tool_evidence",
                        "classification": "tool_evidence",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_role": "tool_result",
                        "source_memory_selection_policies": ["selected_tool_evidence_only"],
                        "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                        "scope": scope,
                        "updated_at_ms": 2000,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": 9123403,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "assistant evidence marker: decision text is verbose enough to exceed the tiny assistant budget.",
                        "summary_text": "assistant evidence marker",
                        "event_type": "assistant_decision",
                        "classification": "assistant_decision",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_role": "assistant_response",
                        "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                        "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                        "scope": scope,
                        "updated_at_ms": 3000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What evidence marker passed?",
                    "max_context_tokens": 120,
                    "ranking": {
                        "max_selected_refs": 2,
                        "min_similarity_score": 0.0,
                        "source_role_budget_tokens": {"assistant": 1},
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )

            selected_events = [ref for ref in pack["selected_refs"] if ref.get("ref_type") == "event"]
            self.assertTrue(any(ref.get("event_type") == "tool_evidence" for ref in selected_events), pack)
            self.assertFalse(any(ref.get("event_type") == "assistant_decision" for ref in selected_events), pack)
            tool_ref = next(ref for ref in selected_events if ref.get("event_type") == "tool_evidence")
            self.assertEqual(["tool"], tool_ref["budget_source_roles"])
            self.assertEqual(["selected_tool_evidence_only"], tool_ref["source_memory_selection_policies"])
            dropped = pack["dropped_refs"]
            self.assertEqual(1, dropped.get("source_role_budget"))
            self.assertTrue(
                any(
                    ref.get("ref_hash") == 9123403
                    and ref.get("drop_reason") == "source_role_budget"
                    and ref.get("source_role_budget_capped_roles") == ["assistant"]
                    for ref in dropped.get("refs", [])
                ),
                dropped,
            )

    def test_retrieve_memory_selection_policy_budget_applies_to_context_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-segment-selection-budget.jsonl")
            scope = {
                "account_id": "acct_segment_selection_budget",
                "tenant_id": "tenant_segment_selection_budget",
                "user_id": "user_segment_selection_budget",
                "session_id": "session_segment_selection_budget",
            }
            scope = {
                **scope,
                **identity_hashes(
                    scope["account_id"],
                    scope["tenant_id"],
                    scope["user_id"],
                    scope["session_id"],
                ),
            }
            node_path = [
                "tenant:tenant_segment_selection_budget",
                "user:user_segment_selection_budget",
                "session:session_segment_selection_budget",
            ]
            node_hash = 9123501
            adapter.append_many(
                [
                    {
                        "record_type": "context_segment",
                        "segment_hash": 9123502,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "topic": "tool_evidence",
                        "summary_text": "tool segment marker: Exit code 0 and hook pipeline passed.",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["tool"],
                        "source_role_counts": {"tool": 1},
                        "source_memory_selection_policies": ["selected_tool_evidence_only"],
                        "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                        "scope": scope,
                        "updated_at_ms": 2000,
                    },
                    {
                        "record_type": "context_segment",
                        "segment_hash": 9123503,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "topic": "assistant_decision",
                        "summary_text": "assistant segment marker: verbose decision text should exceed the tiny assistant policy budget.",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                        "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                        "scope": scope,
                        "updated_at_ms": 3000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What segment marker should be remembered?",
                    "max_context_tokens": 120,
                    "ranking": {
                        "max_selected_refs": 2,
                        "min_similarity_score": 0.0,
                        "memory_selection_policy_budget_tokens": {
                            "selected_assistant_decision_outcome_only": 1,
                        },
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )

            selected_segments = [ref for ref in pack["selected_refs"] if ref.get("ref_type") == "segment"]
            self.assertTrue(any("tool segment marker" in str(ref.get("text") or "") for ref in selected_segments), pack)
            self.assertFalse(any("assistant segment marker" in str(ref.get("text") or "") for ref in selected_segments), pack)
            tool_ref = next(ref for ref in selected_segments if "tool segment marker" in str(ref.get("text") or ""))
            self.assertEqual(["selected_tool_evidence_only"], tool_ref["source_memory_selection_policies"])
            dropped = pack["dropped_refs"]
            self.assertEqual(1, dropped.get("memory_selection_policy_budget"))
            self.assertTrue(
                any(
                    ref.get("ref_hash") == 9123503
                    and ref.get("drop_reason") == "memory_selection_policy_budget"
                    and ref.get("memory_selection_policy_budget_capped_policies")
                    == ["selected_assistant_decision_outcome_only"]
                    for ref in dropped.get("refs", [])
                ),
                dropped,
            )

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
            self.assertEqual({"assistant": 39, "tool": 57, "user": 51}, role_policy["budget_tokens"])
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
                "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER": "1",
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

            time.sleep(0.05)
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
            self.assertEqual("deferred", first["ingest"]["auto_batch_extract_result"]["status"])
            self.assertEqual("idle_timeout", first["ingest"]["auto_batch_extract_result"]["trigger_policy"])

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
                "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER": "1",
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

            time.sleep(0.05)
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
            self.assertNotIn("pre_retrieval_summary_refresh", pack)
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
                "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER": "1",
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
            self.assertEqual("deferred", first["ingest"]["auto_batch_extract_result"]["status"])
            self.assertEqual("idle_timeout", first["ingest"]["auto_batch_extract_result"]["trigger_policy"])

            time.sleep(0.05)
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
                "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER": "1",
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

            time.sleep(0.05)
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
            self.assertNotIn("pre_retrieval_summary_refresh", pack)
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

            time.sleep(0.05)
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
            self.assertNotIn("pre_retrieval_summary_refresh", pack)
            self.assertLessEqual(pack["used_context_tokens"], 240)
            selected_refs = pack["selected_refs"]
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in selected_refs), selected_refs)
            self.assertTrue(
                any(ref.get("ref_type") == "summary" for ref in selected_refs),
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

    def test_user_prompt_backfills_previous_assistant_when_tool_backfill_times_out(self) -> None:
        calls: list[tuple[str, str]] = []

        def fake_trace_tool_call(server, name, arguments, trace):
            role = arguments.get("messages", [{}])[0].get("role", "")
            trigger = arguments.get("agent_hook", {}).get("trigger", "")
            calls.append((role, trigger))
            if role == "tool":
                return {
                    "status": "timeout",
                    "_hook_tool_timeout": True,
                    "tool": name,
                    "timeout_ms": 5,
                }
            return {"status": "accepted", "event_id_hash": 1, "node_hash": 2, "hook_captured": True}

        argv = [
            "matrixark_codex_hook.py",
            "--backend",
            "local",
            "--event-log",
            "/tmp/matrixark-backfill-isolation.jsonl",
            "--event",
            "UserPromptSubmit",
            "--account-id",
            "acct_backfill_isolation",
            "--tenant-id",
            "tenant_backfill_isolation",
            "--user-id",
            "user_backfill_isolation",
            "--session-id",
            "session_backfill_isolation",
        ]
        payload = {
            "prompt": "User asks the next question after prior tool and assistant output.",
            "thread_id": "backfill-isolation-thread",
        }
        with (
            mock.patch.object(matrixark_codex_hook.sys, "argv", argv),
            mock.patch.object(matrixark_codex_hook, "read_stdin_payload", return_value=payload),
            mock.patch.object(matrixark_codex_hook, "validate_hook_backend_policy", return_value=None),
            mock.patch.object(matrixark_codex_hook, "build_server", return_value=object()),
            mock.patch.object(matrixark_codex_hook, "close_server_best_effort", return_value=None),
            mock.patch.object(matrixark_codex_hook, "append_hook_trace", return_value=None),
            mock.patch.object(matrixark_codex_hook, "trace_tool_call", side_effect=fake_trace_tool_call),
            mock.patch.object(matrixark_codex_hook, "should_rollout_backfill_tool_result", return_value=True),
            mock.patch.object(matrixark_codex_hook, "latest_codex_tool_output_from_rollout", return_value="Exit code: 0\nRan 3 tests\nOK"),
            mock.patch.object(
                matrixark_codex_hook,
                "latest_codex_assistant_message_from_rollout_raw",
                return_value="Assistant decision: capture previous assistant even if tool backfill timed out.",
            ),
            mock.patch.object(matrixark_codex_hook, "spawn_rollout_backfill_child", return_value=None),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            exit_code = matrixark_codex_hook.main()

        self.assertEqual(0, exit_code)
        self.assertIn(("tool", "UserPromptSubmit:previous_tool_output_backfill"), calls)
        self.assertIn(("assistant", "UserPromptSubmit:previous_assistant_backfill"), calls)
        output = json.loads(stdout.getvalue())
        self.assertEqual("warning", output["status"])
        self.assertIn("timed out", output["error"])

    # Pinned for the same reason as the sibling backfill test: a posting is only dropped
    # when the record it points at carries a vector, so with embeddings off nothing is
    # dropped and the names asserted absent below are written after all. The suite shares
    # one process and other tests move this policy.
    @mock.patch.dict(os.environ, {"MATRIXARK_GENERATE_EMBEDDINGS": "1"})
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
            # Postings are folded onto the batch commit that wrote them, so filtering for
            # data_model "context_profile_entity" now matches nothing; the entity's own postings
            # are still there under the commit. What the entity carries itself -- its roles and
            # its codex events -- is asserted directly above, and #562 stopped writing a posting
            # whose term the record already carries, so `source_role:` and `codex_event:` names
            # are gone rather than merely relabelled.
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
            }
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertNotIn("source_role:assistant", index_names)
            self.assertNotIn("codex_event:previousassistantbackfill", index_names)
            self.assertNotIn("codex_event:userpromptsubmit:previous_assistant_backfill", index_names)

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
            self.assertNotIn("pre_retrieval_summary_refresh", pack)
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

    # The absence checks below depend on embeddings being ON. A posting is only dropped when the
    # record it points at carries a vector -- that is the condition drop_owner_derivable_postings
    # is gated on -- so with embeddings off nothing is dropped and every one of those names is
    # written after all. The suite shares one process and other tests move this policy, which is
    # why this passed alone and failed under discovery. Pin it rather than depend on whoever ran
    # first.
    @mock.patch.dict(os.environ, {"MATRIXARK_GENERATE_EMBEDDINGS": "1"})
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
            # The writer interns repeated metadata, so a raw line carries an ``_imb`` token
            # where ``agent_hook`` used to sit inline. ``expand_interned_records`` is the
            # read-side inverse every other consumer goes through; it is a no-op on a log
            # with nothing interned.
            records = expand_interned_records([
                json.loads(line)
                for line in event_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ])
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
            # Postings are folded onto the batch commit that wrote them, so filtering for
            # data_model "context_profile_entity" now matches nothing; the entity's own postings
            # are still there under the commit. What the entity carries itself -- its roles and
            # its codex events -- is asserted directly above, and #562 stopped writing a posting
            # whose term the record already carries, so `source_role:` and `codex_event:` names
            # are gone rather than merely relabelled.
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
            }
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertNotIn("source_role:assistant", index_names)
            self.assertNotIn("codex_event:previousassistantbackfill", index_names)
            self.assertNotIn("codex_event:userpromptsubmit:previous_assistant_backfill", index_names)

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
            self.assertGreaterEqual(memory_budget["source_message_counts_by_role"].get("assistant", 0), 1)

    # Pinned for the same reason as the sibling backfill test: a posting is only dropped
    # when the record it points at carries a vector, so with embeddings off nothing is
    # dropped and the names asserted absent below are written after all. The suite shares
    # one process and other tests move this policy.
    @mock.patch.dict(os.environ, {"MATRIXARK_GENERATE_EMBEDDINGS": "1"})
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
                    record.get("record_type") == "context_event"
                    and record.get("source_role") == "tool"
                    and "PreviousToolOutputBackfill" in record.get("source_codex_events", [])
                    and "Ran 159 tests" in str(record.get("text") or "")
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
            # Same shape as the idle-preflush test above: postings are folded onto the batch
            # commit that wrote them, so the "context_profile_entity" data_model matches
            # nothing, and #562 stopped writing a posting whose term the record already
            # carries -- which is what `source_role:` and `codex_event:` were.
            index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
            }
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertNotIn("source_role:tool", index_names)
            self.assertNotIn("codex_event:previoustooloutputbackfill", index_names)

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
            self.assertGreaterEqual(memory_budget["source_message_counts_by_role"].get("tool", 0), 1)

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
                            "content": "\n".join(
                                [
                                    "Verbose answer body that should not become durable memory. " * 80,
                                    "```",
                                    "large_code_block_that_should_not_be_ingested_as_memory()",
                                    "```",
                                    "Assistant decision: delayed rollout backfill commits profile memory.",
                                    "Validation ran 64 tests passed.",
                                ]
                            ),
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
            assistant_events = [
                record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("source_role") == "assistant"
            ]
            self.assertTrue(assistant_events, records)
            self.assertTrue(all("large_code_block" not in str(record.get("text") or "") for record in assistant_events))
            self.assertTrue(all("Verbose answer body" not in str(record.get("text") or "") for record in assistant_events))
            self.assertTrue(
                any(
                    record.get("source_memory_selection_policy_counts", {}).get(
                        "selected_assistant_decision_outcome_only"
                    ) == 1
                    and record.get("source_memory_selection_lossy_count") == 1
                    and record.get("source_memory_selection_dropped_text_chars", 0) > 0
                    for record in assistant_events
                ),
                assistant_events,
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_batch_commit"
                    and record.get("commit_reason") == "async_rollout_backfill"
                    and "PreviousAssistantBackfill" in record.get("source_codex_events", [])
                    and "Stop:async_rollout_backfill" in record.get("source_codex_events", [])
                    and record.get("source_memory_selection_policy_counts", {}).get("selected_assistant_decision_outcome_only") == 1
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
                    and record.get("source_memory_selection_policy_counts", {}).get("selected_assistant_decision_outcome_only") == 1
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
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", index_names)
            self.assertIn("memory_selection_policy:selected_profile_current_state", index_names)
            profile_embeddings = [
                record
                for record in records
                if record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "profile_entity_state"
                and record.get("memory_scope") == "user_profile"
            ]
            self.assertTrue(profile_embeddings, records)
            self.assertTrue(
                any(
                    record.get("entity_type") == "assistant_decision"
                    and record.get("memory_scope") == "user_profile"
                    for record in profile_embeddings
                ),
                profile_embeddings,
            )
            segment_index_names = {
                str(record.get("index_name") or "")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_segment"
            }
            self.assertIn("source_role:assistant", segment_index_names)
            self.assertIn("hook_type:after_llm", segment_index_names)
            self.assertIn(
                "memory_selection_policy:selected_assistant_decision_outcome_only",
                segment_index_names,
            )

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
            self.assertGreaterEqual(memory_budget["source_message_counts_by_role"].get("assistant", 0), 1)

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
            self.assertNotIn("pre_retrieval_summary_refresh", summary_pack)
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
            self.assertNotIn("pre_retrieval_summary_refresh", summary_pack)
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
            # Folded: no separate embedding records; the events carry their vectors.
            self.assertNotIn("context_embedding", record_types)
            self.assertTrue(
                any(
                    record.get("record_type") == "context_event" and record.get("vector")
                    for record in records
                ),
                "hot-path events must carry their vectors inline",
            )
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
            # Folded: each profile entity carries its vector; the retired record's compact
            # copy rides along under embedding_meta.
            promoted_with_vectors = [
                record for record in profile_entities if record.get("vector")
            ]
            self.assertEqual(len(profile_entities), len(promoted_with_vectors))
            profile_embeddings = [
                record.get("embedding_meta") or {} for record in promoted_with_vectors
            ]
            self.assertTrue(all(
                meta.get("memory_scope", record.get("memory_scope")) == "user_profile"
                and meta.get("session_continuity", record.get("session_continuity")) == "cross_session"
                for meta, record in zip(profile_embeddings, promoted_with_vectors)
            ))
            self.assertTrue(all(not record.get("extraction_phase") for record in profile_embeddings))
            self.assertTrue(all(not record.get("final_session_boundary") for record in profile_embeddings))
            for record in profile_embeddings:
                for field in [
                    "source_event_ids",
                    "source_session_ids",
                    "source_entity_hashes",
                    "source_roles",
                    "source_role_counts",
                    "source_hook_types",
                    "source_hook_type_counts",
                    "source_codex_events",
                    "source_codex_event_counts",
                    "source_memory_selection_policies",
                    "source_memory_selection_policy_counts",
                    "source_profile_promotion_policies",
                    "source_profile_promotion_blockers",
                    "source_memory_scopes",
                    "source_session_continuities",
                    "supersedes_session_entity_hash",
                    "supersedes_session_entity_hashes",
                    "previous_profile_revision",
                    "previous_profile_updated_at_ms",
                    "extraction_context_event_ids",
                ]:
                    self.assertNotIn(field, record)
                self.assertNotIn("profile_source_session_count", record)
                self.assertNotIn("profile_source_entity_count", record)
                self.assertNotIn("profile_source_event_count", record)

            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_promote_all",
                        "tenant_id": "tenant_promote_all",
                        "user_id": "user_promote_all",
                        "session_id": "session_promote_all_followup",
                    },
                    "session_scope": "prefer",
                    "query": "What profile memory mentions durable preference keyword?",
                    "max_context_tokens": 160,
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                    "ranking": {"max_selected_refs": 4},
                }
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    for ref in pack["selected_refs"]
                ),
                pack["selected_refs"],
            )
            self.assertIsInstance(pack.get("retrieval_metrics"), dict)
            self.assertTrue(result["summary_refresh"]["profile_dirty_hashes"])

    # Pinned for the same reason as the sibling backfill test: a posting is only dropped
    # when the record it points at carries a vector, so with embeddings off nothing is
    # dropped and the names asserted absent below are written after all. The suite shares
    # one process and other tests move this policy.
    @mock.patch.dict(os.environ, {"MATRIXARK_GENERATE_EMBEDDINGS": "1"})
    def test_batch_extract_assigns_role_specific_event_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-role-event-types.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_role_event_types",
                        "tenant_id": "tenant_role_event_types",
                        "user_id": "user_role_event_types",
                        "session_id": "session_role_event_types",
                    },
                    "messages": [
                        {"role": "user", "content": "User asks Codex to keep role-specific event types."},
                        {"role": "assistant", "content": "Assistant decision: implement compact event typing."},
                        {"role": "tool", "content": "Tool evidence: tests passed for role-specific event typing."},
                    ],
                    "metadata": {
                        "source_role_counts": {"user": 1, "assistant": 1, "tool": 1},
                        "source_memory_selection_policy_counts": {
                            "selected_user_prompt": 1,
                            "selected_assistant_decision_outcome_only": 1,
                            "selected_tool_evidence_only": 1,
                        },
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                    },
                    "threshold_messages": 1,
                    "skip_prior_context": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            events = [
                record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            ]
            by_role = {record.get("source_role"): record for record in events}
            self.assertEqual("user_prompt", by_role["user"]["event_type"])
            self.assertEqual("assistant_response", by_role["assistant"]["event_type"])
            self.assertEqual("tool_evidence", by_role["tool"]["event_type"])
            self.assertTrue(all(record.get("batch_event_type") for record in events))

            # Folded: the events carry their vectors; the retired records' compact copy
            # rides along under embedding_meta with the same event-type attribution.
            folded = [record for record in events if record.get("vector")]
            self.assertEqual(3, len(folded), "every role's event must carry its vector")
            event_embeddings = [record.get("embedding_meta") or {} for record in folded]
            embedding_types = {
                record.get("source_role"): (record.get("embedding_meta") or {}).get("event_type", record.get("event_type"))
                for record in folded
            }
            self.assertEqual(
                {"user": "user_prompt", "assistant": "assistant_response", "tool": "tool_evidence"},
                embedding_types,
            )
            self.assertTrue(all("source_role_counts" not in record for record in event_embeddings))
            self.assertGreaterEqual(result["event_indexes_written"], 3)

            # Every term this used to look for -- the event types and the roles -- is one the
            # context_event row it points at carries itself, and #562 stops writing a posting
            # whose owner carries a vector and can derive the term. All twenty-one are dropped on
            # the way to the log, so this set is empty rather than differently spelled.
            #
            # Asserting emptiness proves nothing on its own, so the batch's own postings are
            # asserted present alongside it: those point at the commit, which derives nothing, and
            # they are still written.
            event_index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_event"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            }
            batch_index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_batch_commit"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            }
            self.assertEqual(set(), event_index_names, batch_index_names)
            self.assertIn("classification:batch_memory", batch_index_names)

            prefiltered = adapter.retrieval_records(
                scope={
                    "account_id": "acct_role_event_types",
                    "tenant_id": "tenant_role_event_types",
                    "user_id": "user_role_event_types",
                    "session_id": "session_role_event_types",
                    "_session_scope": "prefer",
                },
                record_types={"context_event"},
                secondary_index_groups=[{"event_type:tool_evidence"}],
            )
            self.assertTrue(prefiltered["scan_stats"]["secondary_index_prefilter_enabled"], prefiltered)
            # index_postings_read is 0 because no posting was stored to read. The match now comes
            # from the owner branch -- the same branch dropping those postings is gated on -- and
            # that is the count that carries it.
            stats = prefiltered["scan_stats"]
            self.assertEqual(0, stats["index_postings_read"], stats)
            self.assertGreaterEqual(stats["secondary_embedding_matched_posting_count"], 1, stats)
            # And it NARROWED. Falling back to a broad scan would return these same records while
            # proving nothing about the prefilter, so the answer below needs this next to it.
            self.assertFalse(stats["broad_scan_used"], stats)
            self.assertLess(stats["returned_records"], stats["scanned_records"], stats)
            self.assertTrue(
                any(
                    record.get("record_type") == "context_event"
                    and record.get("event_type") == "tool_evidence"
                    and record.get("source_role") == "tool"
                    for record in prefiltered["records"]
                ),
                prefiltered["records"],
            )

    # The inventory below counts context_segment rows, and segments are OFF by default
    # (tenant knob extract_segments / MATRIXARK_EXTRACT_SEGMENTS, default False), so without
    # this the count is 0. Patched per test rather than at module scope, for the same reason
    # the sibling tests give: the suite is one process, and setting it wider would flip the
    # knob for the tests that assert it is off.
    @mock.patch.dict(os.environ, {"MATRIXARK_EXTRACT_SEGMENTS": "1"})
    def test_retrieve_serving_pack_hides_inventory_until_metrics_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            adapter = MatrixArkLocalAdapter(tmp / "matrixark-memory-inventory.jsonl")
            scope = {
                "account_id": "acct_inventory",
                "tenant_id": "tenant_inventory",
                "user_id": "user_inventory",
                "session_id": "session_inventory",
            }
            result = adapter.batch_extract(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "user",
                            "content": "User note: context events are the source records for profile inventory segment marker blue quartz.",
                        },
                        {
                            "role": "assistant",
                            "content": "Assistant decision: context segments remain derived serving chunks while retrieval reports session, profile, and shared memory layers.",
                        },
                    ],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                    "threshold_messages": 1,
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("accepted", result["status"])
            self.assertGreaterEqual(result["profile_entities_written"], 1)

            resource = tmp / "inventory_policy.md"
            resource.write_text(
                "# Inventory Policy\n\nDecision: tenant shared inventory records are retrievable outside sessions.\n",
                encoding="utf-8",
            )
            adapter.ingest(
                {
                    "kind": "resource",
                    "raw_uri": str(resource),
                    "resource_type": "md",
                    "scope": {"account_id": "acct_inventory", "tenant_id": "tenant_inventory"},
                    "messages": [{"role": "user", "content": "Import inventory shared resource."}],
                    "wait": True,
                }
            )

            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_inventory_followup"},
                    "session_scope": "prefer",
                    "query": "Show profile inventory memory and tenant shared inventory policy.",
                    "max_context_tokens": 800,
                    "shared_context": {"resource_budget_tokens": 120},
                    "audit_mode": "off",
                }
            )
            self.assertNotIn("memory_inventory", pack)

            metrics_pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "session_inventory_followup"},
                    "session_scope": "prefer",
                    "query": "Show profile inventory memory and tenant shared inventory policy.",
                    "max_context_tokens": 800,
                    "shared_context": {"resource_budget_tokens": 120},
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                }
            )
            inventory = metrics_pack["retrieval_metrics"]["memory_inventory"]
            self.assertTrue(inventory["has_session_memory"])
            self.assertTrue(inventory["has_profile_memory"])
            self.assertTrue(inventory["has_shared_memory"])
            self.assertIn("session", inventory["available_layers"])
            self.assertIn("profile", inventory["available_layers"])
            self.assertIn("shared", inventory["available_layers"])
            self.assertGreaterEqual(inventory["session"]["context_events"], 1)
            self.assertGreaterEqual(inventory["session"]["context_segments"], 1)
            self.assertGreaterEqual(inventory["profile"]["context_entities"], 1)
            self.assertGreaterEqual(inventory["profile"]["context_embeddings"], 1)
            # The postings this used to find under "profile" are attributed to the batch commit
            # that wrote them, not to the profile entity they point at, so the profile layer has
            # none and the session layer holds them. Counting them at all is what was fixed
            # alongside this: a posting carries neither a memory scope nor a session continuity,
            # so before, every layer reported 0 while twenty postings sat on the log.
            self.assertEqual(0, inventory["profile"]["context_indexes"], inventory["profile"])
            self.assertGreaterEqual(inventory["session"]["context_indexes"], 1, inventory["session"])
            self.assertGreaterEqual(inventory["shared"]["resource_chunks"], 1)
            self.assertEqual("prefer", inventory["query_scope"]["session_scope"])
            self.assertNotIn("source_event_ids", json.dumps(inventory, sort_keys=True))

    # This test exercises context_segment rows, which are OFF unless a tenant asks for them.
    # Patched for the duration of the test only: setting it at module scope would leak across
    # the single-process suite run and flip the knob for tests that assert it is off.
    @mock.patch.dict(os.environ, {"MATRIXARK_EXTRACT_SEGMENTS": "1"})
    def test_context_segment_debug_pack_shows_source_context_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-segment-source-events.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_segment_lineage",
                        "tenant_id": "tenant_segment_lineage",
                        "user_id": "user_segment_lineage",
                        "session_id": "session_segment_lineage",
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": "User note: context events are the source records for segment lineage marker blue quartz.",
                        },
                        {
                            "role": "assistant",
                            "content": "Assistant decision: context segments remain derived serving chunks, not raw events.",
                        },
                    ],
                    "metadata": {"hook_type": "after_llm", "codex_event": "Stop"},
                    "threshold_messages": 1,
                    "skip_prior_context": True,
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            segments = [
                record
                for record in records
                if record.get("record_type") == "context_segment"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            ]
            events = [
                record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("batch_id_hash") == result["batch_id_hash"]
            ]
            self.assertTrue(segments)
            self.assertEqual(result["events_written"], len(events))
            segment = segments[0]
            self.assertEqual("context_event", segment["source_record_type"])
            self.assertTrue(segment["derived_from_context_events"])
            self.assertGreaterEqual(len(segment["source_event_ids"]), 1)
            self.assertTrue(set(segment["source_event_ids"]).issubset({event["event_id_hash"] for event in events}))

            ref = {
                "ref_type": "segment",
                "text": segment["summary_text"],
                "memory_scope": segment["memory_scope"],
                "session_continuity": segment["session_continuity"],
                "source_event_ids": segment["source_event_ids"],
                "source_ref": "context_event:debug-only-source-ref",
                "source_event_count": len(segment["source_event_ids"]),
                "source_record_type": segment["source_record_type"],
                "segment_origin": segment["segment_origin"],
                "derived_from_context_events": segment["derived_from_context_events"],
            }
            event_ref = {
                "ref_type": "event",
                "text": "tool evidence marker: Exit code 0 and hook pipeline passed.",
                "event_type": "tool_evidence",
                "source_role": "tool",
                "source_roles": ["tool"],
                "source_role_counts": {"tool": 1},
                "memory_scope": "session",
                "session_continuity": "same_session",
            }
            compact_event = compact_context_pack_ref(event_ref)
            self.assertEqual("tool", compact_event["source_role"])
            self.assertEqual("tool_evidence", compact_event["event_type"])
            self.assertNotIn("source_roles", compact_event)
            self.assertNotIn("source_role_counts", compact_event)

            feature_event_ref = {
                "ref_type": "event",
                "text": "feature preference: focus on functionality parity before monitoring.",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "profile_memory_kind": "memory_feature",
                "profile_memory_class": "memory_feature",
            }
            self.assertEqual(
                "same_session_memory_feature_event",
                compact_context_pack_ref(feature_event_ref)["memory_layer"],
            )
            cross_feature_segment_ref = {
                "ref_type": "segment",
                "text": "cross-session memory preference about feature-only work.",
                "memory_scope": "session",
                "session_continuity": "cross_session",
                "source_profile_memory_kinds": ["memory_feature"],
            }
            self.assertEqual(
                "cross_session_memory_feature_segment",
                compact_context_pack_ref(cross_feature_segment_ref)["memory_layer"],
            )

            compact_default = compact_context_pack_ref(ref)
            self.assertNotIn("source_event_ids", compact_default)
            self.assertNotIn("source_ref", compact_default)
            self.assertNotIn("source_record_type", compact_default)
            self.assertNotIn("derived_from_context_events", compact_default)
            self.assertNotIn("segment_origin", compact_default)

            compact_debug = compact_context_pack_ref(ref, include_debug=True)
            self.assertEqual("context_event:debug-only-source-ref", compact_debug["source_ref"])
            self.assertEqual(segment["source_event_ids"][:8], compact_debug["source_event_ids"])
            self.assertEqual(len(segment["source_event_ids"]), compact_debug["source_event_count"])
            self.assertEqual("context_event", compact_debug["source_record_type"])
            self.assertTrue(compact_debug["derived_from_context_events"])
            self.assertEqual(segment["segment_origin"], compact_debug["segment_origin"])

            grouped_pack = {
                "context_pack_id": "segment-source-events",
                "selected_refs": [ref],
                "remote_context_refs": [ref],
                "recall_policy": {},
                "retrieval_metrics": {},
                "used_context_tokens": 8,
            }
            grouped_default = compact_context_pack_for_serving(grouped_pack)
            grouped_default_item = grouped_default["groups"][0]["items"][0]
            self.assertNotIn("source_event_ids", grouped_default_item)
            self.assertNotIn("source_ref", grouped_default_item)
            self.assertNotIn("source_record_type", grouped_default_item)
            self.assertNotIn("derived_from_context_events", grouped_default_item)
            self.assertNotIn("segment_origin", grouped_default_item)

            grouped_debug = compact_context_pack_for_serving(grouped_pack, include_debug=True)
            grouped_debug_item = grouped_debug["groups"][0]["items"][0]
            self.assertEqual("context_event:debug-only-source-ref", grouped_debug_item["source_ref"])
            self.assertEqual(segment["source_event_ids"][:8], grouped_debug_item["source_event_ids"])
            self.assertEqual(len(segment["source_event_ids"]), grouped_debug_item["source_event_count"])
            self.assertEqual("context_event", grouped_debug_item["source_record_type"])
            self.assertTrue(grouped_debug_item["derived_from_context_events"])
            self.assertEqual(segment["segment_origin"], grouped_debug_item["segment_origin"])


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
            self.assertTrue(any(record.get("dirty_reason") for record in first_dirty))
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
                    for row in refresh_rows
                ),
                refresh_dashboard,
            )
            self.assertTrue(
                any(
                    row.get("row_type") == "context_summary_dirty"
                    and row.get("dirty_reason") == "new_event"
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
            for row in embedding_rows:
                for field in [
                    "source_roles",
                    "source_role_counts",
                    "source_hook_types",
                    "source_hook_type_counts",
                    "source_codex_events",
                    "source_codex_event_counts",
                    "source_memory_scopes",
                    "source_session_continuities",
                ]:
                    self.assertNotIn(field, row, embeddings_dashboard)
            self.assertTrue(
                any(
                    row.get("embedding_type") == "event_text"
                    and row.get("memory_scope") == "session"
                    and row.get("session_continuity") == "same_session"
                    for row in embedding_rows
                ),
                embeddings_dashboard,
            )
            profile_embedding_rows = [
                row
                for row in embedding_rows
                if row.get("memory_scope") == "user_profile"
                and row.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(profile_embedding_rows, embeddings_dashboard)
            self.assertTrue(
                all(row.get("promoted_from_memory_scope") in {"", "session"} for row in profile_embedding_rows),
                embeddings_dashboard,
            )
            self.assertTrue(
                all(not row.get("source_role_counts") for row in profile_embedding_rows),
                embeddings_dashboard,
            )
            self.assertTrue(
                all(not row.get("source_hook_type_counts") for row in profile_embedding_rows),
                embeddings_dashboard,
            )
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
            self.assertNotIn("retrieval_metrics", pack)
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
            self.assertEqual(debug_readiness["task_count"], telemetry_readiness["task_count"])
            self.assertEqual(debug_readiness["remaining_stage_counts"], telemetry_readiness["remaining_stage_counts"])
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
            self.assertNotIn("retrieval_metrics", summary_pack)

    def test_lightweight_async_ingest_reports_scheduled_idle_auto_batch_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-async-idle-scheduled.jsonl")
            scope = {
                "account_id": "acct_async_idle_scheduled",
                "tenant_id": "tenant_async_idle_scheduled",
                "user_id": "user_async_idle_scheduled",
                "session_id": "session_async_idle_scheduled",
            }
            result = adapter.ingest(
                {
                    "scope": scope,
                    "async_processing": True,
                    "auto_batch_extract": True,
                    "session_buffer_threshold": 20,
                    "idle_commit_timeout_ms": 60000,
                    "skip_prior_context": True,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember this live prompt and arm idle extraction without waiting for Stop.",
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertFalse(result["session_buffer"]["threshold_ready"])
            self.assertFalse(result["session_buffer"]["idle_ready"])
            self.assertTrue(result["session_buffer"]["idle_commit_scheduled"])
            self.assertEqual("deferred", result["idle_commit_result"]["status"])
            self.assertEqual("deferred", result["auto_batch_extract_result"]["status"])
            self.assertEqual("idle_timeout", result["auto_batch_extract_result"]["trigger_policy"])
            self.assertEqual("session_buffer_idle_deadline_armed", result["auto_batch_extract_result"]["reason"])
            self.assertEqual(1, result["auto_batch_extract_result"]["pending_event_count"])
            self.assertEqual(1, result["auto_batch_extract_result"]["pending_message_count"])
            self.assertTrue(result["auto_batch_extract_result"]["idle_commit_scheduled"])
            self.assertEqual("provisional", result["auto_batch_extract_result"]["extraction_phase"])
            self.assertFalse(result["auto_batch_extract_result"]["final_session_boundary"])
            self.assertEqual(1, len(adapter.pending_session_events(scope)))
            records = adapter.read_all()
            pending_event = next(record for record in records if record.get("record_type") == "context_event")
            self.assertEqual("user_prompt", pending_event["event_type"])
            self.assertEqual("pending_async", pending_event["batch_event_type"])
            self.assertEqual("session", pending_event["memory_scope"])
            self.assertEqual("same_session", pending_event["session_continuity"])
            self.assertEqual(["session", "user_profile"], pending_event["source_memory_scopes"])
            self.assertEqual(["same_session", "cross_session"], pending_event["source_session_continuities"])
            self.assertEqual(["user"], pending_event["source_roles"])
            self.assertEqual({"user": 1}, pending_event["source_role_counts"])
            self.assertEqual(["selected_user_prompt"], pending_event["source_memory_selection_policies"])
            index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_event"
                and record.get("ref_hashes") == [pending_event["event_id_hash"]]
            }
            self.assertIn("event_type:user_prompt", index_names)
            self.assertIn("memory_selection_policy:selected_user_prompt", index_names)
            self.assertIn("memory_layer:pending_async_event", index_names)

    def test_lightweight_async_pending_event_is_retrievable_before_batch_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-lightweight-pending-retrieval.jsonl")
            scope = {
                "account_id": "acct_lightweight_pending_retrieval",
                "tenant_id": "tenant_lightweight_pending_retrieval",
                "user_id": "user_lightweight_pending_retrieval",
                "session_id": "session_lightweight_pending_retrieval",
            }
            result = adapter.ingest(
                {
                    "scope": scope,
                    "async_processing": True,
                    "auto_batch_extract": True,
                    "session_buffer_threshold": 20,
                    "idle_commit_timeout_ms": 60000,
                    "skip_prior_context": True,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember live lightweight pending retrieval marker obsidian lantern.",
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual("deferred", result["auto_batch_extract_result"]["status"])
            self.assertEqual(1, len(adapter.pending_session_events(scope)))

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What marker proves live lightweight pending retrieval for obsidian lantern?",
                    "max_context_tokens": 220,
                    "audit_mode": "off",
                    "debug_context_pack": True,
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
                if ref.get("ref_type") == "event" and "obsidian lantern" in str(ref.get("text") or "")
            ]
            self.assertTrue(event_refs, pack["selected_refs"])
            self.assertEqual("user_prompt", event_refs[0]["event_type"])
            self.assertEqual("pending_async_event", event_refs[0]["memory_layer"])
            self.assertEqual("session", event_refs[0]["memory_scope"])
            self.assertEqual("same_session", event_refs[0]["session_continuity"])
            self.assertEqual("pending_async_event", candidate_memory_layer_name(event_refs[0]))

    def test_lightweight_async_feature_memory_pending_event_uses_feature_budget_layer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-lightweight-pending-feature-memory.jsonl")
            scope = {
                "account_id": "acct_lightweight_pending_feature",
                "tenant_id": "tenant_lightweight_pending_feature",
                "user_id": "user_lightweight_pending_feature",
                "session_id": "session_lightweight_pending_feature",
            }
            result = adapter.ingest(
                {
                    "scope": scope,
                    "async_processing": True,
                    "auto_batch_extract": True,
                    "session_buffer_threshold": 20,
                    "idle_commit_timeout_ms": 60000,
                    "skip_prior_context": True,
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "Focus on feature parity for session memory; "
                                "no testing, monitoring, debugging, or evidence at this point."
                            ),
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                }
            )

            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            pending_event = next(
                record
                for record in records
                if record.get("record_type") == "context_event"
                and record.get("extraction_phase") == "pending_async"
            )
            self.assertEqual("memory_feature", pending_event["event_type"])
            self.assertEqual("pending_async_memory_feature_event", candidate_memory_layer_name(pending_event))
            # Folded: the event itself carries the vector, and the retired record's layer
            # tag rides along under embedding_meta.
            self.assertTrue(pending_event.get("vector"), "the pending event must carry its vector")
            pending_meta = pending_event.get("embedding_meta") or {}
            self.assertEqual(
                "pending_async_memory_feature_event",
                pending_meta.get("memory_layer") or pending_event.get("memory_layer"),
            )

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "session_scope": "prefer",
                    "query": "What feature parity memory instruction is pending?",
                    "max_context_tokens": 220,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_retrieval_metrics": True,
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                    },
                }
            )

            pending_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "event"
                and "feature parity" in str(ref.get("text") or "")
            ]
            self.assertTrue(pending_refs, pack["selected_refs"])
            self.assertEqual("memory_feature", pending_refs[0]["event_type"])
            self.assertEqual("pending_async_memory_feature_event", pending_refs[0]["memory_layer"])
            layer_budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(
                layer_budget["by_memory_layer"]["pending_async_memory_feature_event"]["refs"],
                1,
            )

