#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI

from __future__ import annotations

from argparse import Namespace
import io
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
import matrixark_mcp_core
import matrixark_mcp_local_adapter
import matrixark_mcp_query
import matrixark_mcp_retrieve_request
import matrixark_mcp_summary_runtime
from matrixark_mcp_context_pack import (
    compact_context_pack_audit_record,
    compact_context_pack_for_serving,
    compact_dropped_refs_for_context_pack,
    compact_refs_for_audit,
)
from matrixark_mcp_core import (
    candidate_index_terms,
    candidate_memory_layer_name,
    compact_context_pack_ref,
    compact_context_pack_audit_record as core_compact_context_pack_audit_record,
    compact_context_pack_for_serving as core_compact_context_pack_for_serving,
    compact_context_pack_for_serving_flat,
    embedding_for_text,
    identity_hashes,
    infer_query_type,
    infer_secondary_index_filter_groups,
    memory_layer_for_serving_ref,
    packing_sort_key,
    select_token_budgeted_refs,
)
from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness
from matrixark_mcp_recovery import matrixark_local_recovery_report
from matrixark_mcp_retrieve_pack_builder import dropped_ref_layer_budget, memory_layer_pressure_summary, selected_ref_layer_budget
from matrixark_mcp_local_adapter import (
    compression_context_index_records,
    quality_first_underfill_summary,
    refresh_final_selected_budget_policies,
    suppress_extracted_represented_pending_events,
    suppress_profile_shadowed_session_entities,
)
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


try:  # mixin
    from tools.test_codex_pipeline_part5 import _CodexPipelinePart5
except ImportError:
    from test_codex_pipeline_part5 import _CodexPipelinePart5

try:  # mixin
    from tools.test_codex_pipeline_part4 import _CodexPipelinePart4
except ImportError:
    from test_codex_pipeline_part4 import _CodexPipelinePart4

try:  # mixin
    from tools.test_codex_pipeline_part3 import _CodexPipelinePart3
except ImportError:
    from test_codex_pipeline_part3 import _CodexPipelinePart3

try:  # mixin
    from tools.test_codex_pipeline_part2 import _CodexPipelinePart2
except ImportError:
    from test_codex_pipeline_part2 import _CodexPipelinePart2

try:  # mixin
    from tools.test_codex_pipeline_part1 import _CodexPipelinePart1
except ImportError:
    from test_codex_pipeline_part1 import _CodexPipelinePart1

class MatrixArkCodexHookPipelineTest(unittest.TestCase, _CodexPipelinePart5, _CodexPipelinePart4, _CodexPipelinePart3, _CodexPipelinePart2, _CodexPipelinePart1):
    def test_hook_lineage_summary_counts_scalar_source_roles(self) -> None:
        summary = matrixark_codex_hook.memory_lineage_summary(
            {
                "source_role": "assistant_response",
                "source_hook_type_counts": {"after_llm": 1},
                "source_codex_event_counts": {"Stop": 1},
            },
            {
                "source_role": "tool_result",
                "source_hook_types": ["tool_result"],
                "source_codex_events": ["PostToolUse"],
            },
        )

        self.assertEqual({"assistant": 1, "tool": 1}, summary["source_role_counts"])
        self.assertTrue(summary["assistant_response_captured"])
        self.assertTrue(summary["tool_evidence_captured"])

    def test_hook_budget_summary_counts_scalar_source_roles(self) -> None:
        budget = matrixark_codex_hook.inferred_live_ref_layer_budget(
            [
                {
                    "ref_type": "event",
                    "source_role": "assistant_response",
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "text": "assistant: selected decision",
                    "token_estimate": 4,
                },
                {
                    "ref_type": "event",
                    "source_role": "tool_result",
                    "source_hook_types": ["tool_result"],
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "text": "tool: Exit code: 0",
                    "token_estimate": 3,
                },
            ]
        )

        self.assertEqual(4, budget["by_source_role"]["assistant"]["tokens"])
        self.assertEqual(3, budget["by_source_role"]["tool"]["tokens"])
        self.assertEqual(1, budget["source_message_counts_by_role"]["assistant"])
        self.assertEqual(1, budget["source_message_counts_by_role"]["tool"])

    def test_extraction_promotes_selected_assistant_and_tool_outcome_facts(self) -> None:
        entities = matrixark_mcp_core.extract_batch_entities(
            [
                {
                    "role": "assistant",
                    "content": (
                        "Outcome: pushed commit abc1234 to origin/main; "
                        "Changed: promoted profile summary indexes; "
                        "Next: validate retrieval against real hooked messages; "
                        "Blocker: matrixarkai mirror push rejected non-fast-forward"
                    ),
                },
                {
                    "role": "tool",
                    "content": (
                        "Exit code: 0\n"
                        "Validation: 42 tests passed\n"
                        "pushed commit abc1234 to origin/main\n"
                    ),
                },
            ],
            {"source_event_ids": ["assistant_event", "tool_event"]},
        )

        by_name = {entity["entity_name"]: entity for entity in entities}
        self.assertIn("assistant_decision", by_name)
        self.assertIn("assistant_decision:outcome", by_name)
        self.assertIn("assistant_decision:changed", by_name)
        self.assertIn("assistant_decision:next", by_name)
        self.assertIn("assistant_decision:blocker", by_name)
        self.assertIn("tool_evidence", by_name)
        self.assertIn("tool_evidence:validation", by_name)
        self.assertEqual(["assistant_event"], by_name["assistant_decision:next"]["source_refs"])
        self.assertEqual(["tool_event"], by_name["tool_evidence:validation"]["source_refs"])
        self.assertEqual(["assistant"], by_name["assistant_decision:blocker"]["source_roles"])
        self.assertEqual(["tool"], by_name["tool_evidence:validation"]["source_roles"])

    def test_extraction_normalizes_codex_tool_output_role_aliases(self) -> None:
        entities = matrixark_mcp_core.extract_batch_entities(
            [
                {
                    "role": "function_call_output",
                    "content": "Exit code: 0\nValidation: 17 tests passed\npushed commit cafe123 to origin/main",
                },
                {
                    "role": "custom_tool_call_output",
                    "content": "Exit code: 0\nValidation: 19 tests passed",
                },
            ],
            {"source_event_ids": ["function_event", "custom_tool_event"]},
        )

        by_name = {entity["entity_name"]: entity for entity in entities}
        by_type = {}
        for entity in entities:
            by_type.setdefault(entity["entity_type"], []).append(entity)
        self.assertIn("tool_evidence", by_name)
        self.assertIn("codex_validation", by_type)
        self.assertIn("codex_publish_outcome", by_type)
        self.assertTrue(all(entity["source_roles"] == ["tool"] for entity in by_type["codex_validation"]))
        self.assertTrue(any(entity["source_refs"] == ["function_event"] for entity in by_type["codex_publish_outcome"]))
        self.assertNotIn("session_memory", by_name)

    def test_assistant_outcome_does_not_create_tool_evidence_without_tool_message(self) -> None:
        selected = matrixark_codex_hook.selected_assistant_memory_text(
            "Implemented profile memory retrieval for validation-result queries and pushed commit abc1234 to origin/main. "
            "Tests passed: 164."
        )
        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "assistant", "content": selected}],
            {"source_event_ids": ["assistant_event"]},
        )
        by_name = {entity["entity_name"]: entity for entity in entities}

        self.assertIn("assistant_decision", by_name)
        self.assertIn("assistant_decision:outcome", by_name)
        self.assertIn("assistant_decision:validation", by_name)
        self.assertNotIn("tool_evidence", by_name)
        self.assertNotIn("tool_evidence:outcome", by_name)
        self.assertEqual(["assistant"], by_name["assistant_decision:outcome"]["source_roles"])
        self.assertEqual(["assistant_event"], by_name["assistant_decision:outcome"]["source_refs"])

    def test_assistant_push_success_prose_becomes_outcome_memory(self) -> None:
        examples = [
            ("Done - git push accepted to main at abc1234 after the hook pipeline test suite passed.", "abc1234"),
            ("Push succeeded on main: def5678 after 165 tests passed.", "def5678"),
            ("Published fedcba9 to main and validation passed.", "fedcba9"),
        ]
        for raw, commit_hash in examples:
            selected = matrixark_codex_hook.selected_assistant_memory_text(raw)
            entities = matrixark_mcp_core.extract_batch_entities(
                [{"role": "assistant", "content": selected}],
                {"source_event_ids": ["assistant_event"]},
            )
            by_name = {entity["entity_name"]: entity for entity in entities}

            self.assertIn(f"pushed commit {commit_hash}", selected, raw)
            self.assertIn("assistant_decision:outcome", by_name, raw)
            self.assertIn(commit_hash, by_name["assistant_decision:outcome"]["state"])
            self.assertEqual(["assistant"], by_name["assistant_decision:outcome"]["source_roles"])
            self.assertNotIn("tool_evidence", by_name)
            self.assertNotIn("tool_evidence:outcome", by_name)

    def test_assistant_zero_failed_summary_does_not_create_blocker_memory(self) -> None:
        selected = matrixark_codex_hook.selected_assistant_memory_text(
            "Ran 166 tests; 0 failed; pushed commit abc1234 to origin/main."
        )
        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "assistant", "content": selected}],
            {"source_event_ids": ["assistant_event"]},
        )
        by_name = {entity["entity_name"]: entity for entity in entities}

        self.assertIn("Outcome: pushed commit abc1234", selected)
        self.assertIn("Validation: 166 tests passed", selected)
        self.assertNotIn("Blocker:", selected)
        self.assertIn("assistant_decision:outcome", by_name)
        self.assertIn("assistant_decision:validation", by_name)
        self.assertNotIn("assistant_decision:blocker", by_name)

    def test_assistant_nonzero_failed_summary_still_creates_blocker_memory(self) -> None:
        selected = matrixark_codex_hook.selected_assistant_memory_text(
            "Validation failed: 165 passed; 1 failed."
        )
        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "assistant", "content": selected}],
            {"source_event_ids": ["assistant_event"]},
        )
        by_name = {entity["entity_name"]: entity for entity in entities}

        self.assertIn("Blocker:", selected)
        self.assertNotIn("Validation: 165 tests passed", selected)
        self.assertIn("assistant_decision:blocker", by_name)
        self.assertNotIn("assistant_decision:validation", by_name)
        self.assertIn("1 failed", by_name["assistant_decision:blocker"]["state"])

    def test_tool_memory_selection_normalizes_common_test_result_summaries(self) -> None:
        noisy_tool_output = "\n".join(
            [
                "compiling temporalstore v0.1.0",
                "warning: many unrelated build lines",
                "test result: ok. 132 passed; 0 failed; 0 ignored; finished in 12.34s",
                "To https://github.com/matrixarkai/TemporalStore.git",
                "   39f93050..e2f645c5  HEAD -> main",
            ]
        )

        selected = matrixark_codex_hook.selected_tool_memory_text(noisy_tool_output)
        self.assertIn("Validation: 132 tests passed", selected)
        self.assertIn("pushed commit e2f645c5 to origin/main", selected)
        self.assertNotIn("warning: many unrelated build lines", selected)

        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "tool", "content": noisy_tool_output}],
            {"source_event_ids": ["tool_event"]},
        )
        by_name = {entity["entity_name"]: entity for entity in entities}
        self.assertIn("tool_evidence:validation", by_name)
        self.assertIn("tool_evidence:outcome", by_name)
        self.assertNotIn("tool_evidence:blocker", by_name)
        self.assertIn("132", by_name["tool_evidence:validation"]["state"])
        self.assertIn("e2f645c5", by_name["tool_evidence:outcome"]["state"])
        self.assertEqual(["tool_event"], by_name["tool_evidence:validation"]["source_refs"])
        self.assertEqual(["tool_event"], by_name["tool_evidence:outcome"]["source_refs"])
        materialized_outcome = {**by_name["tool_evidence:outcome"], "record_type": "context_entity"}
        for terms in [
            candidate_index_terms(materialized_outcome, {}, {}),
            matrixark_mcp_query.candidate_index_terms(materialized_outcome, {}, {}),
        ]:
            self.assertIn("codex_outcome:outcome", terms)
            self.assertNotIn("codex_outcome:blocker", terms)

    def test_payload_text_compacts_assistant_and_tool_outputs_at_ingestion_boundary(self) -> None:
        assistant_raw = "\n".join(
            [
                "verbose implementation details " * 80,
                "Implemented profile memory retrieval and pushed commit face123 to origin/main.",
                "Validation: 44 tests passed.",
                "Next: continue feature parity work.",
            ]
        )
        assistant_selected = matrixark_codex_hook.payload_text(
            {"last_assistant_message": assistant_raw},
            event="Stop",
        )
        self.assertIn("Outcome: pushed commit face123 to origin/main", assistant_selected)
        self.assertIn("Validation: 44 tests passed", assistant_selected)
        self.assertNotIn("verbose implementation details verbose implementation", assistant_selected)

        tool_raw = "\n".join(
            [
                "build line " * 120,
                "Exit code: 0",
                "Ran 45 tests in 0.02s",
                "To https://github.com/matrixarkai/TemporalStore.git",
                "   39f93050..bee1234  HEAD -> main",
            ]
        )
        tool_selected = matrixark_codex_hook.payload_text(
            {"tool_result": tool_raw, "tool_name": "shell_command", "status": "ok"},
            event="PostToolUse",
        )
        self.assertIn("tool_name=shell_command", tool_selected)
        self.assertIn("tool_status=ok", tool_selected)
        self.assertIn("Exit code: 0", tool_selected)
        self.assertIn("Ran 45 tests", tool_selected)
        self.assertIn("pushed commit bee1234 to origin/main", tool_selected)
        self.assertNotIn("build line build line", tool_selected)

    def test_user_prompt_selection_keeps_goal_and_task_memory_without_large_context(self) -> None:
        large_prompt = "\n".join(
            [
                "goal: implement external-memory profile memory extraction for Codex hooks",
                "```",
                "irrelevant pasted code " * 200,
                "```",
                "please implement threshold and idle batch extraction for live hooks",
                "make sure retrieval budgets include profile and cross-session entities",
            ]
        )

        selected = matrixark_codex_hook.selected_user_prompt_memory_text(large_prompt, max_chars=500)
        self.assertIn("goal: implement external-memory profile memory extraction", selected)
        self.assertIn("please implement threshold and idle batch extraction", selected)
        self.assertIn("retrieval budgets include profile and cross-session entities", selected)
        self.assertNotIn("irrelevant pasted code", selected)

        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "user", "content": selected}],
            {"source_event_ids": ["user_event"]},
        )
        plan_entities = [
            entity
            for entity in entities
            if entity.get("entity_type") == "current_plan"
        ]
        self.assertTrue(plan_entities, entities)
        plan_text = " ".join(str(entity.get("state") or "") for entity in plan_entities)
        self.assertIn("external-memory profile memory extraction", plan_text)
        self.assertIn("threshold and idle batch extraction", plan_text)
        self.assertTrue(all(entity.get("source_roles") == ["user"] for entity in plan_entities))

    def test_user_goal_queries_target_current_plan_and_selected_prompt_memory(self) -> None:
        query = "What goal did I ask Codex to implement for profile memory retrieval?"
        self.assertEqual("profile_memory", infer_query_type(query))
        self.assertEqual("profile_memory", matrixark_mcp_query.infer_query_type(query))

        core_groups = infer_secondary_index_filter_groups(query, "profile_memory")
        query_groups = matrixark_mcp_query.infer_secondary_index_filter_groups(query, "profile_memory")
        for groups in [core_groups, query_groups]:
            flattened = {term for group in groups for term in group}
            self.assertIn("entity_type:current_plan", flattened)
            self.assertIn("event_type:user_prompt", flattened)
            self.assertIn("source_role:user", flattened)
            self.assertIn("memory_selection_policy:selected_user_prompt", flattened)
            self.assertIn("memory_selection_policy:selected_user_profile_fact", flattened)

        budgets, mode = matrixark_mcp_retrieve_request.pre_refresh_helpers.auto_memory_selection_policy_budget_tokens(
            {"query": query},
            {},
            remote_budget_tokens=1000,
            question_type="profile_memory",
        )
        self.assertEqual("auto", mode)
        self.assertGreater(budgets["selected_user_prompt"], budgets["selected_profile_current_state"])
        self.assertGreater(budgets["selected_user_prompt"], budgets["selected_assistant_decision_outcome_only"])
        self.assertGreater(budgets["selected_user_prompt"], budgets["selected_tool_evidence_only"])

    def test_user_prompt_profile_preferences_get_profile_fact_policy(self) -> None:
        metadata = matrixark_codex_hook.codex_memory_selection_metadata(
            role="user",
            event="UserPromptSubmit",
            text="Always use the Ubuntu TemporalStore repo and never use Windows folders for builds.",
        )

        self.assertEqual("selected_user_prompt", metadata["policy"])
        self.assertIn("selected_user_profile_fact", metadata["policies"])
        self.assertEqual(1, metadata["policy_counts"]["selected_user_profile_fact"])

    def test_assistant_standing_workspace_response_gets_profile_fact_policy(self) -> None:
        text = "Going forward, I'll use the Ubuntu TemporalStore repo and avoid Windows folders for builds."
        selected = matrixark_codex_hook.selected_assistant_memory_text(text)
        metadata = matrixark_codex_hook.codex_memory_selection_metadata(
            role="assistant",
            event="Stop",
            text=selected,
            original_text=text,
        )

        self.assertIn("Ubuntu TemporalStore repo", selected)
        self.assertIn("selected_assistant_profile_fact", metadata["policies"])
        self.assertEqual(1, metadata["policy_counts"]["selected_assistant_profile_fact"])
        entities = matrixark_mcp_core.extract_batch_entities(
            [{"role": "assistant", "content": selected}],
            {"source_event_ids": ["assistant_profile_fact_event"]},
        )
        profile_entities = [
            entity
            for entity in entities
            if entity.get("entity_type") == "workspace_profile"
            and "Ubuntu TemporalStore repo" in str(entity.get("state") or "")
        ]
        self.assertTrue(profile_entities, entities)
        self.assertEqual(["assistant_profile_fact_event"], profile_entities[0]["source_refs"])
        assistant_only_entity_types = {
            entity.get("entity_type")
            for entity in entities
            if entity.get("source_roles") == ["assistant"]
        }
        self.assertNotIn("current_plan", assistant_only_entity_types)
        self.assertNotIn("job_status", assistant_only_entity_types)

    def test_user_goal_still_extracts_current_plan_after_assistant_profile_cleanup(self) -> None:
        entities = matrixark_mcp_core.extract_batch_entities(
            [
                {
                    "role": "user",
                    "content": "Goal: implement external-memory profile memory retrieval for TemporalStore.",
                }
            ],
            {"source_event_ids": ["user_goal_event"]},
        )

        plans = [
            entity
            for entity in entities
            if entity.get("entity_type") == "current_plan"
            and "external-memory profile memory retrieval" in str(entity.get("state") or "")
        ]
        self.assertTrue(plans, entities)
        self.assertEqual(["user"], plans[0]["source_roles"])

    def test_codex_async_ingest_messages_carry_selection_metadata(self) -> None:
        args = Namespace(session_commit_threshold=20, idle_commit_timeout_ms=120000)
        text = "Always use the Ubuntu TemporalStore repo and never use Windows folders for builds."
        ingest_args = matrixark_codex_hook.hook_async_message_ingest_args(
            {"scope": {"account_id": "acct", "tenant_id": "tenant", "user_id": "user", "session_id": "session"}},
            args,
            event="UserPromptSubmit",
            role="user",
            text=text,
            original_text=text,
            metadata={},
            agent_hook={"source": "codex", "hook_type": "before_llm"},
        )

        message_selection = ingest_args["messages"][0]["metadata"]["codex_memory_selection"]
        self.assertEqual("selected_user_prompt", message_selection["policy"])
        self.assertIn("selected_user_profile_fact", message_selection["policies"])
        self.assertEqual(
            ingest_args["metadata"]["codex_memory_selection"]["policies"],
            message_selection["policies"],
        )

    def test_first_person_codex_outcome_queries_target_assistant_and_tool_memory(self) -> None:
        queries = [
            "What did you implement and validate last?",
            "What did we push and verify for TemporalStore memory?",
            "What tests passed recently?",
            "Which commit was pushed last?",
            "Show recent validation result",
        ]
        for query in queries:
            self.assertEqual("evidence", infer_query_type(query), query)
            self.assertEqual("evidence", matrixark_mcp_query.infer_query_type(query), query)
            for groups in [
                infer_secondary_index_filter_groups(query, "evidence"),
                matrixark_mcp_query.infer_secondary_index_filter_groups(query, "evidence"),
            ]:
                flattened = {term for group in groups for term in group}
                self.assertIn("entity_type:assistant_decision", flattened)
                self.assertIn("entity_type:tool_evidence", flattened)
                self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", flattened)
                self.assertIn("memory_selection_policy:selected_tool_evidence_only", flattened)

    def test_outcome_fact_entities_emit_and_query_specific_index_terms(self) -> None:
        assistant_next = {
            "record_type": "context_entity",
            "entity_type": "assistant_decision",
            "entity_name": "assistant_decision:next",
            "state": "assistant decision next: validate retrieval against real hooked messages",
            "source_roles": ["assistant"],
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
        }
        tool_validation = {
            "record_type": "context_entity",
            "entity_type": "tool_evidence",
            "entity_name": "tool_evidence:validation",
            "state": "tool evidence validation: Validation: 42 tests passed",
            "source_roles": ["tool"],
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
        }

        assistant_terms = candidate_index_terms(assistant_next, {}, {})
        query_assistant_terms = matrixark_mcp_query.candidate_index_terms(assistant_next, {}, {})
        for terms in [assistant_terms, query_assistant_terms]:
            self.assertIn("entity_name:assistant_decision_next", terms)
            self.assertIn("codex_outcome:next", terms)
            self.assertIn("entity_type:assistant_decision", terms)

        tool_terms = candidate_index_terms(tool_validation, {}, {})
        query_tool_terms = matrixark_mcp_query.candidate_index_terms(tool_validation, {}, {})
        for terms in [tool_terms, query_tool_terms]:
            self.assertIn("entity_name:tool_evidence_validation", terms)
            self.assertIn("codex_outcome:validation", terms)
            self.assertIn("entity_type:tool_evidence", terms)

        assistant_groups = infer_secondary_index_filter_groups(
            "What is the next action Codex decided?",
            "current_state",
        )
        assistant_flattened = {term for group in assistant_groups for term in group}
        self.assertIn("codex_outcome:next", assistant_flattened)

        query_assistant_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(
                "What is the next action Codex decided?",
                "current_state",
            )
            for term in group
        }
        self.assertIn("codex_outcome:next", query_assistant_flattened)

        tool_groups = infer_secondary_index_filter_groups(
            "Show validation evidence and tests passed for the pushed commit",
            "evidence",
        )
        tool_flattened = {term for group in tool_groups for term in group}
        self.assertIn("codex_outcome:validation", tool_flattened)
        self.assertIn("codex_outcome:outcome", tool_flattened)

        query_tool_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(
                "Show validation evidence and tests passed for the pushed commit",
                "evidence",
            )
            for term in group
        }
        self.assertIn("codex_outcome:validation", query_tool_flattened)
        self.assertIn("codex_outcome:outcome", query_tool_flattened)

    def test_evidence_packing_prioritizes_structured_assistant_outcome_entities(self) -> None:
        assistant_outcome = {
            "ref_type": "entity",
            "ref_hash": 2101,
            "entity_type": "assistant_decision",
            "entity_name": "assistant_decision:outcome",
            "score": 0.45,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "text": "assistant decision outcome: pushed commit abc1234 to origin/main after validation",
        }
        generic_assistant = {
            "ref_type": "entity",
            "ref_hash": 2102,
            "entity_type": "assistant_decision",
            "entity_name": "assistant_decision",
            "score": 0.55,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "text": "assistant decision: keep working on the implementation",
        }
        raw_event = {
            "ref_type": "event",
            "ref_hash": 2103,
            "event_type": "assistant_response",
            "score": 0.55,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "text": "assistant: pushed commit abc1234 to origin/main after validation",
        }

        self.assertGreater(
            packing_sort_key(assistant_outcome, "evidence"),
            packing_sort_key(generic_assistant, "evidence"),
        )
        self.assertGreater(
            packing_sort_key(assistant_outcome, "evidence"),
            packing_sort_key(raw_event, "evidence"),
        )
        selected, _used_tokens, dropped = select_token_budgeted_refs(
            [generic_assistant, raw_event, assistant_outcome],
            [],
            max_context_tokens=128,
            auxiliary_quota=0,
            question_type="evidence",
            min_score=0.0,
            max_selected_refs=1,
        )

        self.assertEqual([2101], [item["ref_hash"] for item in selected])
        self.assertGreaterEqual(dropped["max_selected_refs"], 2)

    def test_benchmark_quality_queries_use_evidence_and_profile_budgets(self) -> None:
        query = "Compare LoCoMo hit rate p50 p99 latency and throughput quality across sessions"
        self.assertEqual("benchmark_quality", infer_query_type(query))
        self.assertEqual("benchmark_quality", matrixark_mcp_query.infer_query_type(query))

        groups = infer_secondary_index_filter_groups(query, "benchmark_quality")
        flattened = {term for group in groups for term in group}
        self.assertIn("entity_type:tool_evidence", flattened)
        self.assertIn("event_type:tool_evidence", flattened)
        self.assertIn("entity_type:assistant_decision", flattened)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", flattened)
        self.assertIn("memory_scope:user_profile", flattened)
        self.assertIn("session_continuity:cross_session", flattened)

        budgets, mode = matrixark_mcp_retrieve_request.pre_refresh_helpers.auto_memory_selection_policy_budget_tokens(
            {},
            {},
            remote_budget_tokens=1000,
            question_type="benchmark_quality",
        )
        self.assertEqual("auto", mode)
        self.assertGreater(budgets["selected_tool_evidence_only"], budgets["selected_user_prompt"])
        self.assertGreater(budgets["selected_assistant_decision_outcome_only"], budgets["selected_user_prompt"])

    def test_codex_outcome_queries_use_assistant_and_tool_evidence_budget(self) -> None:
        queries = [
            "What did Codex implement and push last?",
            "What failed or was blocked in the last TemporalStore memory work?",
            "Show the assistant decision and validation evidence for the pushed commit",
        ]
        for query in queries:
            self.assertEqual("evidence", infer_query_type(query), query)
            self.assertEqual("evidence", matrixark_mcp_query.infer_query_type(query), query)

        groups = infer_secondary_index_filter_groups(queries[0], "evidence")
        flattened = {term for group in groups for term in group}
        self.assertIn("entity_type:assistant_decision", flattened)
        self.assertIn("event_type:assistant_response", flattened)
        self.assertIn("entity_type:tool_evidence", flattened)
        self.assertIn("event_type:tool_evidence", flattened)

        budgets, mode = matrixark_mcp_retrieve_request.pre_refresh_helpers.auto_memory_selection_policy_budget_tokens(
            {},
            {},
            remote_budget_tokens=1000,
            question_type="evidence",
        )
        self.assertEqual("auto", mode)
        self.assertGreater(budgets["selected_tool_evidence_only"], budgets["selected_user_prompt"])
        self.assertGreaterEqual(budgets["selected_assistant_decision_outcome_only"], 350)

    def test_query_candidate_index_terms_match_core_source_lineage_terms(self) -> None:
        record = {
            "record_type": "context_entity",
            "entity_type": "assistant_decision",
            "state": "Decision: commit the live hook extraction fix after validation.",
            "source_roles": ["assistant"],
            "source_role_counts": {"assistant": 1, "tool": 0},
            "source_hook_types": ["hook_boundary"],
            "source_hook_type_counts": {"hook_boundary": 1},
            "source_codex_events": ["Stop"],
            "source_codex_event_counts": {"Stop": 1},
            "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
            "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
            "source_memory_selection_lossy_count": 1,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "extraction_phase": "final",
        }

        core_terms = candidate_index_terms(record, {}, {})
        query_terms = matrixark_mcp_query.candidate_index_terms(record, {}, {})
        for term in {
            "source_role:assistant",
            "hook_type:hook_boundary",
            "codex_event:stop",
            "memory_selection_policy:selected_assistant_decision_outcome_only",
            "memory_selection_quality:lossy",
            "memory_scope:user_profile",
            "session_continuity:cross_session",
            "extraction_phase:final",
        }:
            self.assertIn(term, core_terms)
            self.assertIn(term, query_terms)

    def test_benchmark_quality_records_emit_metric_index_terms(self) -> None:
        record = {
            "record_type": "context_event",
            "event_type": "tool_evidence",
            "source_type": "message",
            "text": "LoCoMo workload: read-index reads p50 latency: 8.4 ms p99 latency=22.8ms throughput: 12500 ops/s read hit rate: 91.7%",
        }

        core_terms = candidate_index_terms(record, {}, {})
        query_terms = matrixark_mcp_query.candidate_index_terms(record, {}, {})
        for terms in [core_terms, query_terms]:
            self.assertIn("benchmark:locomo", terms)
            self.assertIn("metric:p50_latency", terms)
            self.assertIn("metric:p99_latency", terms)
            self.assertIn("metric:throughput", terms)
            self.assertIn("metric:hit_rate", terms)
            self.assertIn("workload:read-index_reads", terms)

        groups = infer_secondary_index_filter_groups(
            "Show LoCoMo p99 latency throughput and read hit rate quality",
            "benchmark_quality",
        )
        flattened = {term for group in groups for term in group}
        self.assertIn("benchmark:locomo", flattened)
        self.assertIn("metric:p99_latency", flattened)
        self.assertIn("metric:throughput", flattened)
        self.assertIn("metric:hit_rate", flattened)

        query_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(
                "Show LoCoMo p99 latency throughput and read hit rate quality",
                "benchmark_quality",
            )
            for term in group
        }
        self.assertIn("benchmark:locomo", query_flattened)
        self.assertIn("metric:p99_latency", query_flattened)

    def test_secondary_index_filters_understand_selected_evidence_quality(self) -> None:
        groups = infer_secondary_index_filter_groups(
            "Show lossy selected tool evidence with exit code and pushed commit details",
            "evidence",
        )
        flattened = {term for group in groups for term in group}
        self.assertIn("entity_type:tool_evidence", flattened)
        self.assertIn("event_type:tool_evidence", flattened)
        self.assertIn("source_role:tool", flattened)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", flattened)
        self.assertIn("memory_selection_quality:lossy", flattened)

        query_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(
                "Show lossy selected tool evidence with exit code and pushed commit details",
                "evidence",
            )
            for term in group
        }
        self.assertIn("entity_type:tool_evidence", query_flattened)
        self.assertIn("event_type:tool_evidence", query_flattened)

        assistant_groups = infer_secondary_index_filter_groups(
            "What selected assistant decision outcome did Codex implement?",
            "current_state",
        )
        assistant_flattened = {term for group in assistant_groups for term in group}
        self.assertIn("entity_type:assistant_decision", assistant_flattened)
        self.assertIn("event_type:assistant_response", assistant_flattened)
        self.assertIn("source_role:assistant", assistant_flattened)
        self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", assistant_flattened)

        user_flattened = {
            term
            for group in infer_secondary_index_filter_groups(
                "Show the original user prompt and user request Codex handled",
                "evidence",
            )
            for term in group
        }
        self.assertIn("event_type:user_prompt", user_flattened)
        self.assertIn("source_role:user", user_flattened)

    def test_profile_current_entities_emit_and_query_current_index_terms(self) -> None:
        record = {
            "record_type": "context_entity",
            "entity_type": "assistant_decision",
            "entity_name": "codex_outcome",
            "state": "Codex pushed the memory retrieval change after validation.",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "profile_entity_current": True,
            "source_memory_selection_policies": ["selected_profile_current_state"],
            "source_memory_selection_policy_counts": {"selected_profile_current_state": 1},
        }
        core_terms = candidate_index_terms(record, {}, {})
        query_terms = matrixark_mcp_query.candidate_index_terms(record, {}, {})
        for terms in [core_terms, query_terms]:
            self.assertIn("profile_entity_current:true", terms)
            self.assertIn("memory_scope:user_profile", terms)
            self.assertIn("session_continuity:cross_session", terms)

        groups = infer_secondary_index_filter_groups("What is the current profile memory for Codex outcomes?", "current_state")
        flattened = {term for group in groups for term in group}
        self.assertIn("profile_entity_current:true", flattened)
        self.assertIn("profile_summary_current:true", flattened)
        self.assertIn("memory_scope:user_profile", flattened)

        query_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(
                "Show latest profile entity memories across sessions",
                "profile_memory",
            )
            for term in group
        }
        self.assertIn("profile_entity_current:true", query_flattened)
        self.assertIn("profile_summary_current:true", query_flattened)

    def test_profile_memory_queries_target_profile_and_cross_session_indexes(self) -> None:
        self.assertEqual(
            "profile_memory",
            infer_query_type("Show user profile long-term memory across sessions"),
        )
        self.assertEqual(
            "profile_memory",
            infer_query_type("What is the latest user profile memory across sessions?"),
        )
        self.assertEqual(
            "profile_memory",
            matrixark_mcp_query.infer_query_type("Show cross-session memories and profile entities"),
        )
        for query in [
            "What do you remember about me?",
            "What do you know about my preferences across tasks?",
            "Show what you know about the user from previous sessions",
            "What have I told you before?",
            "What have I told you about myself?",
            "What did I tell you before?",
            "What are my preferences?",
            "What do I prefer?",
            "Do I prefer Ubuntu folders?",
            "What is my local repo policy?",
            "What are my always-push instructions?",
            "Which repo should you use for TemporalStore builds?",
            "Which folder should Codex use for TemporalStore?",
            "Where should you build TemporalStore?",
            "Should you use Ubuntu or Windows folders for TemporalStore?",
            "Which remote main branch should you push to?",
            "What should you always do after code changes?",
            "What default behavior should Codex follow?",
            "Which standing instructions should you remember?",
            "How should you behave by default?",
        ]:
            self.assertEqual("profile_memory", infer_query_type(query), query)
            self.assertEqual("profile_memory", matrixark_mcp_query.infer_query_type(query), query)
        groups = infer_secondary_index_filter_groups(
            "Show user profile long-term memory and cross-session entities",
            "profile_memory",
        )
        flattened = {term for group in groups for term in group}
        self.assertIn("memory_scope:user_profile", flattened)
        self.assertIn("session_continuity:cross_session", flattened)
        self.assertIn("profile_summary_current:true", flattened)
        self.assertIn("memory_selection_policy:selected_user_profile_fact", flattened)
        self.assertIn("memory_selection_policy:selected_assistant_profile_fact", flattened)

        standing_rule_groups = infer_secondary_index_filter_groups(
            "Which repo should you use for TemporalStore builds?",
            "fact",
        )
        standing_rule_flattened = {term for group in standing_rule_groups for term in group}
        self.assertIn("memory_scope:user_profile", standing_rule_flattened)
        self.assertIn("session_continuity:cross_session", standing_rule_flattened)
        self.assertIn("profile_memory_class:workspace", standing_rule_flattened)
        self.assertIn("memory_selection_policy:selected_user_profile_fact", standing_rule_flattened)

        default_behavior_groups = infer_secondary_index_filter_groups(
            "What should you always do after code changes?",
            "fact",
        )
        default_behavior_flattened = {term for group in default_behavior_groups for term in group}
        self.assertIn("memory_scope:user_profile", default_behavior_flattened)
        self.assertIn("session_continuity:cross_session", default_behavior_flattened)
        self.assertIn("profile_entity_current:true", default_behavior_flattened)
        self.assertIn("memory_selection_policy:selected_user_profile_fact", default_behavior_flattened)

        session_groups = infer_secondary_index_filter_groups(
            "Show session-specific same-session context entities",
            "fact",
        )
        session_flattened = {term for group in session_groups for term in group}
        self.assertIn("memory_scope:session", session_flattened)
        self.assertIn("session_continuity:same_session", session_flattened)

    def test_active_memory_goal_queries_retrieve_profile_feature_memory(self) -> None:
        query = "What should we focus on next for memory functionality?"
        self.assertEqual("profile_memory", infer_query_type(query))
        self.assertEqual("profile_memory", matrixark_mcp_query.infer_query_type(query))
        flattened = {term for group in infer_secondary_index_filter_groups(query, "profile_memory") for term in group}
        helper_flattened = {
            term
            for group in matrixark_mcp_query.infer_secondary_index_filter_groups(query, "profile_memory")
            for term in group
        }
        for terms in (flattened, helper_flattened):
            self.assertIn("entity_type:memory_feature_profile", terms)
            self.assertIn("profile_memory_class:memory_feature", terms)
            self.assertIn("memory_scope:user_profile", terms)
            self.assertIn("session_continuity:cross_session", terms)

        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-active-memory-goal-query.jsonl")
            base_scope = {
                "account_id": "acct_active_memory_goal",
                "tenant_id": "tenant_active_memory_goal",
                "user_id": "user_active_memory_goal",
            }
            result = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_active_memory_goal_1"},
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "Focus memory work on live ingestion, profile promotion, "
                                "retrieval budgets, and feature functionality only."
                            ),
                        }
                    ],
                    "metadata": {"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "force": True,
                }
            )
            self.assertEqual("accepted", result["status"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_active_memory_goal_2"},
                    "session_scope": "prefer",
                    "query": query,
                    "max_context_tokens": 220,
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                    "ranking": {"max_selected_refs": 2, "min_similarity_score": 0.0},
                }
            )
            profile_feature_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_type") == "memory_feature_profile"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(profile_feature_refs, pack["selected_refs"])
            self.assertIn("live ingestion", profile_feature_refs[0]["text"])
            self.assertIn("retrieval budgets", profile_feature_refs[0]["text"])
            self.assertNotIn("source_session_ids", profile_feature_refs[0])
            self.assertGreaterEqual(
                pack["retrieval_metrics"]["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
                1,
            )

    def test_assistant_feature_focus_response_promotes_to_profile_memory(self) -> None:
        raw = (
            "Next focus: live ingestion should preserve assistant responses, profile promotion, "
            "retrieval budgets, secondary indexes, and context summaries."
        )
        selected = matrixark_codex_hook.selected_assistant_memory_text(raw)
        metadata = matrixark_codex_hook.codex_memory_selection_metadata(
            role="assistant",
            event="Stop",
            text=selected,
            original_text=raw,
        )
        self.assertIn("live ingestion", selected)
        self.assertIn("retrieval budgets", selected)
        self.assertIn("selected_assistant_profile_fact", metadata["policies"])
        self.assertEqual("memory_feature", metadata["profile_memory_class"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-assistant-feature-focus-profile.jsonl")
            base_scope = {
                "account_id": "acct_assistant_feature_focus",
                "tenant_id": "tenant_assistant_feature_focus",
                "user_id": "user_assistant_feature_focus",
            }
            result = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_assistant_feature_focus_1"},
                    "messages": [{"role": "assistant", "content": selected, "metadata": {"codex_memory_selection": metadata}}],
                    "metadata": {
                        "hook_type": "after_llm",
                        "codex_event": "Stop",
                        "codex_memory_selection": metadata,
                    },
                    "force": True,
                }
            )
            self.assertEqual("accepted", result["status"])

            profile_entities = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "memory_feature_profile"
            ]
            self.assertTrue(profile_entities)
            self.assertIn("profile promotion", profile_entities[-1]["state"])
            self.assertIn("retrieval budgets", profile_entities[-1]["state"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_assistant_feature_focus_2"},
                    "session_scope": "prefer",
                    "query": "What is the active memory feature focus?",
                    "max_context_tokens": 220,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2, "min_similarity_score": 0.0},
                }
            )
            selected_refs = [
                ref
                for ref in pack["selected_refs"]
                if ref.get("entity_type") == "memory_feature_profile"
                and ref.get("memory_scope") == "user_profile"
            ]
            self.assertTrue(selected_refs, pack["selected_refs"])
            self.assertIn("assistant responses", selected_refs[0]["text"])

    def test_query_helper_oss_labels_cover_profile_memory_layers(self) -> None:
        labels = matrixark_mcp_query.QUERY_INDEX_LABELS
        core_labels = matrixark_mcp_core.QUERY_INDEX_LABELS

        for label in [
            "memory_scope:user_profile",
            "memory_scope:session",
            "session_continuity:cross_session",
            "session_continuity:same_session",
            "profile_entity_current:true",
            "profile_summary_current:true",
            "memory_selection_policy:selected_profile_current_state",
            "memory_selection_policy:selected_user_profile_fact",
            "memory_selection_policy:selected_assistant_profile_fact",
            "memory_selection_policy:selected_assistant_decision_outcome_only",
            "memory_selection_policy:selected_tool_evidence_only",
            "source_role:user",
            "source_role:assistant",
            "source_role:tool",
        ]:
            self.assertIn(label, labels)
            self.assertIn(label, core_labels)
            self.assertTrue(labels[label].strip(), label)
            self.assertTrue(core_labels[label].strip(), label)

    def test_oss_query_filters_preserve_explicit_resource_and_skill_terms(self) -> None:
        query = "Which policy requires finance approval and which skill inspects replay evidence?"

        for builder in [
            matrixark_mcp_core.oss_encoder_secondary_index_filter_groups,
            matrixark_mcp_query.oss_encoder_secondary_index_filter_groups,
        ]:
            flattened = {term for group in builder(query, "evidence") for term in group}
            self.assertIn("source_type:resource", flattened)
            self.assertIn("source_type:resource_fact", flattened)
            self.assertIn("source_type:skill", flattened)







if __name__ == "__main__":
    unittest.main()





