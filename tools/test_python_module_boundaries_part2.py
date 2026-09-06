# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_ModuleBoundaryPart2 methods split from test_matrixark_python_module_boundaries.MatrixArkPythonModuleBoundaryTest (mixin)."""
from __future__ import annotations

import os
from unittest import mock

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_python_module_boundaries import (
    REPO_ROOT,
    TOOLS_DIR,
    importlib,
    mock,
    re,
    sys,
)
except ImportError:
    from test_matrixark_python_module_boundaries import (
    REPO_ROOT,
    TOOLS_DIR,
    importlib,
    mock,
    re,
    sys,
)


class _ModuleBoundaryPart2:
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
                    "extraction_phase_budget_mode": "auto",
                },
            },
            started_perf=0.0,
        )

        self.assertEqual(3, target.refresh_request["limit"])
        self.assertEqual("refreshed", request["pre_retrieval_summary_refresh"]["status"])
        self.assertEqual(1, request["pre_retrieval_summary_refresh"]["refreshed_count"])
        self.assertEqual("auto", request["source_role_budget_mode"])
        self.assertIn("assistant", request["source_role_budget_tokens"])
        self.assertEqual("pre_retrieval_summary_refresh_current_state", request["memory_layer_budget_mode"])
        self.assertIn("pending_async_event", request["memory_layer_budget_tokens"])
        self.assertIn("profile_entity", request["memory_layer_budget_tokens"])
        self.assertIn("profile_compression", request["memory_layer_budget_tokens"])
        self.assertIn("cross_session_compression", request["memory_layer_budget_tokens"])
        self.assertEqual(1045, request["memory_layer_budget_tokens"]["profile_entity"])
        self.assertEqual(665, request["memory_layer_budget_tokens"]["profile_summary"])
        self.assertEqual(570, request["memory_layer_budget_tokens"]["cross_session_event"])
        self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
        self.assertEqual(
            {
                "selected_user_prompt": 760,
                "selected_assistant_decision_outcome_only": 855,
                "selected_tool_evidence_only": 570,
                "selected_profile_current_state": 950,
            },
            request["memory_selection_policy_budget_tokens"],
        )
        self.assertEqual("auto", request["extraction_phase_budget_mode"])
        self.assertEqual(
            {
                "pending_async": 228,
                "provisional": 475,
                "final": 1425,
            },
            request["extraction_phase_budget_tokens"],
        )
        self.assertEqual(1, len(request["pre_retrieval_refreshed_records"]))

    def test_pre_retrieval_summary_refresh_fallback_prioritizes_profile_memory(self) -> None:
        pre_refresh = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")

        fact_budgets, fact_mode = pre_refresh.pre_retrieval_summary_refresh_memory_layer_budget_tokens(
            remote_budget_tokens=100,
        )
        self.assertEqual("pre_retrieval_summary_refresh_balanced", fact_mode)
        self.assertEqual(45, fact_budgets["profile_entity"])

        profile_budgets, profile_mode = pre_refresh.pre_retrieval_summary_refresh_memory_layer_budget_tokens(
            remote_budget_tokens=100,
            question_type="profile_memory",
        )
        self.assertEqual("pre_retrieval_summary_refresh_feature_profile_memory", profile_mode)
        self.assertEqual(65, profile_budgets["profile_entity"])
        self.assertEqual(50, profile_budgets["profile_summary"])
        self.assertEqual(45, profile_budgets["cross_session_summary"])
        self.assertEqual(35, profile_budgets["cross_session_event"])

    def test_moduleized_request_flushes_due_idle_commit_before_retrieval_cache(self) -> None:
        request_mod = importlib.import_module("tools.matrixark_mcp_retrieve_request")

        class FakeTarget:
            _context_pack_cache_max_entries = 2
            _context_pack_cache_ttl_s = 60
            _retrieval_records_cache_generation = 11

            def __init__(self) -> None:
                import threading

                self._context_pack_cache_lock = threading.RLock()
                self._context_pack_cache = {("stale",): (0.0, {"selected_refs": []})}
                self.commit_requests = []
                self.records = [
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": 101,
                        "event_id_hash": 202,
                        "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                        "status": "idle_commit_scheduled",
                        "trigger_policy": "idle_timeout",
                        "threshold_messages": 20,
                        "idle_commit_timeout_ms": 1,
                        "idle_commit_deadline_ms": 1,
                        "updated_at_ms": 1,
                    }
                ]

            def _observe_model_latency(self, *_args: object) -> None:
                return None

            def read_all(self):
                return list(self.records)

            def append(self, record):
                self.records.append(record)

            def session_commit(self, request):
                self.commit_requests.append(request)
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "committed_event_count": 1,
                }

        target = FakeTarget()
        request = request_mod.prepare_retrieval_request(
            target,
            {
                "query": "what did we decide after the tool result?",
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                "max_context_tokens": 2000,
                "ranking": {"pre_retrieval_idle_commit_flush": True},
            },
            started_perf=0.0,
        )

        self.assertEqual(1, len(target.commit_requests))
        self.assertEqual("idle_timeout", target.commit_requests[0]["commit_reason"])
        self.assertFalse(target.commit_requests[0]["force"])
        self.assertEqual("committed", request["pre_retrieval_idle_commit"]["status"])
        self.assertEqual(1, request["pre_retrieval_idle_commit"]["committed_event_count"])
        self.assertEqual(1, request["pre_retrieval_idle_commit"]["cleared_context_pack_cache_count"])
        self.assertEqual(12, request["pre_retrieval_idle_commit"]["retrieval_records_cache_generation"])
        self.assertEqual({}, target._context_pack_cache)
        self.assertEqual(12, target._retrieval_records_cache_generation)
        self.assertTrue(
            any(
                record.get("status") == "idle_commit_committed"
                and record.get("scheduled_task_hash") == 101
                for record in target.records
            )
        )

        second = request_mod.prepare_retrieval_request(
            target,
            {
                "query": "what did we decide after the tool result?",
                "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                "max_context_tokens": 2000,
                "ranking": {"pre_retrieval_idle_commit_flush": True},
            },
            started_perf=0.0,
        )
        self.assertEqual(1, len(target.commit_requests))
        self.assertEqual("no_due_idle_commits", second["pre_retrieval_idle_commit"]["status"])

    def test_moduleized_request_flush_resolves_all_due_idle_schedules_for_scope(self) -> None:
        request_mod = importlib.import_module("tools.matrixark_mcp_retrieve_request")

        class FakeTarget:
            def __init__(self) -> None:
                self.commit_requests = []
                self.records = [
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": 101,
                        "event_id_hash": 201,
                        "node_hash": 301,
                        "node_path": ["tenant:t", "user:u", "session:s"],
                        "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                        "status": "idle_commit_scheduled",
                        "trigger_policy": "idle_timeout",
                        "threshold_messages": 20,
                        "idle_commit_timeout_ms": 1,
                        "idle_commit_deadline_ms": 1,
                        "source_role_counts": {"assistant": 1},
                        "source_hook_type_counts": {"after_llm": 1},
                        "source_codex_event_counts": {"Stop": 1},
                        "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                        "source_memory_scopes": ["session", "user_profile"],
                        "source_session_continuities": ["same_session", "cross_session"],
                        "source_extraction_phases": ["provisional"],
                        "extraction_phase": "provisional",
                        "updated_at_ms": 1,
                    },
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": 102,
                        "event_id_hash": 202,
                        "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                        "status": "idle_commit_scheduled",
                        "trigger_policy": "idle_timeout",
                        "threshold_messages": 20,
                        "idle_commit_timeout_ms": 1,
                        "idle_commit_deadline_ms": 1,
                        "updated_at_ms": 1,
                    },
                ]

            def read_all(self):
                return list(self.records)

            def append(self, record):
                self.records.append(record)

            def session_commit(self, request):
                self.commit_requests.append(request)
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "committed_event_count": 2,
                }

        target = FakeTarget()
        scope = {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"}
        result = request_mod.pre_retrieval_idle_commit_flush(
            target,
            {"scope": scope},
            {},
            scope=scope,
        )

        self.assertEqual("committed", result["status"])
        self.assertEqual(2, result["due_task_count"])
        self.assertEqual(2, result["resolved_scheduled_task_count"])
        self.assertEqual(1, len(target.commit_requests))
        resolved_hashes = {
            record.get("scheduled_task_hash")
            for record in target.records
            if record.get("status") == "idle_commit_committed"
        }
        self.assertEqual({101, 102}, resolved_hashes)
        first_resolution = next(
            record
            for record in target.records
            if record.get("status") == "idle_commit_committed"
            and record.get("scheduled_task_hash") == 101
        )
        self.assertEqual(301, first_resolution["node_hash"])
        self.assertEqual(["tenant:t", "user:u", "session:s"], first_resolution["node_path"])
        self.assertEqual({"assistant": 1}, first_resolution["source_role_counts"])
        self.assertEqual({"after_llm": 1}, first_resolution["source_hook_type_counts"])
        self.assertEqual({"Stop": 1}, first_resolution["source_codex_event_counts"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            first_resolution["source_memory_selection_policy_counts"],
        )
        self.assertEqual(["session", "user_profile"], first_resolution["source_memory_scopes"])
        self.assertEqual(["same_session", "cross_session"], first_resolution["source_session_continuities"])
        self.assertEqual(["provisional"], first_resolution["source_extraction_phases"])
        self.assertEqual("provisional", first_resolution["extraction_phase"])

        second = request_mod.pre_retrieval_idle_commit_flush(target, {"scope": scope}, {}, scope=scope)
        self.assertEqual("no_due_idle_commits", second["status"])

    def test_moduleized_request_flush_marks_all_due_idle_schedules_failed_with_lineage(self) -> None:
        request_mod = importlib.import_module("tools.matrixark_mcp_retrieve_request")

        class FakeTarget:
            def __init__(self) -> None:
                self.records = [
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": 201,
                        "event_id_hash": 301,
                        "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                        "status": "idle_commit_scheduled",
                        "trigger_policy": "idle_timeout",
                        "threshold_messages": 20,
                        "idle_commit_timeout_ms": 1,
                        "idle_commit_deadline_ms": 1,
                        "source_role_counts": {"tool": 1},
                        "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                        "source_extraction_phases": ["provisional"],
                    },
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": 202,
                        "event_id_hash": 302,
                        "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                        "status": "idle_commit_scheduled",
                        "trigger_policy": "idle_timeout",
                        "threshold_messages": 20,
                        "idle_commit_timeout_ms": 1,
                        "idle_commit_deadline_ms": 1,
                    },
                ]

            def read_all(self):
                return list(self.records)

            def append(self, record):
                self.records.append(record)

            def session_commit(self, _request):
                raise RuntimeError("commit backend unavailable")

        target = FakeTarget()
        scope = {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"}
        result = request_mod.pre_retrieval_idle_commit_flush(target, {"scope": scope}, {}, scope=scope)

        self.assertEqual("error", result["status"])
        self.assertEqual("session_commit_failed", result["reason"])
        self.assertEqual(2, result["due_task_count"])
        self.assertEqual(2, result["resolved_scheduled_task_count"])
        failed_hashes = {
            record.get("scheduled_task_hash")
            for record in target.records
            if record.get("status") == "idle_commit_failed"
        }
        self.assertEqual({201, 202}, failed_hashes)
        first_failure = next(
            record
            for record in target.records
            if record.get("status") == "idle_commit_failed"
            and record.get("scheduled_task_hash") == 201
        )
        self.assertEqual({"tool": 1}, first_failure["source_role_counts"])
        self.assertEqual({"selected_tool_evidence_only": 1}, first_failure["source_memory_selection_policy_counts"])
        self.assertEqual(["provisional"], first_failure["source_extraction_phases"])

        second = request_mod.pre_retrieval_idle_commit_flush(target, {"scope": scope}, {}, scope=scope)
        self.assertEqual("no_due_idle_commits", second["status"])

    def test_async_readiness_tracks_scheduled_and_due_idle_commits(self) -> None:
        readiness_mod = importlib.import_module("tools.matrixark_mcp_async_readiness")
        readiness = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 303,
                    "event_id_hash": 404,
                    "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "status": "idle_commit_scheduled",
                    "trigger_policy": "idle_timeout",
                    "stages": ["extraction", "summary", "compression", "embedding"],
                    "idle_commit_deadline_ms": 1,
                    "source_role_counts": {"assistant": 1},
                    "source_hook_type_counts": {"after_llm": 1},
                    "source_codex_event_counts": {"Stop": 1},
                    "source_memory_scopes": ["session"],
                    "source_session_continuities": ["same_session"],
                    "source_extraction_phases": ["provisional"],
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                }
            ],
            {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
        )

        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertEqual(1, readiness["pending_task_count"])
        self.assertEqual(1, readiness["scheduled_idle_task_count"])
        self.assertEqual(1, readiness["due_idle_task_count"])
        self.assertEqual({}, readiness["pending_extraction_phases"])
        self.assertEqual({}, readiness["pending_source_roles"])
        self.assertEqual({}, readiness["remaining_stage_counts"])
        self.assertEqual(0, readiness["pending_final_session_boundary_count"])
        self.assertIn("idle_commit_scheduled", readiness["freshness_warnings"])

        resolved = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 303,
                    "event_id_hash": 404,
                    "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "status": "idle_commit_scheduled",
                    "trigger_policy": "idle_timeout",
                    "stages": ["extraction", "summary", "compression", "embedding"],
                    "idle_commit_deadline_ms": 1,
                },
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 303,
                    "scheduled_task_hash": 303,
                    "event_id_hash": 404,
                    "scope": {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
                    "status": "idle_commit_committed",
                    "trigger_policy": "idle_timeout",
                    "committed_event_count": 1,
                    "updated_at_ms": 2,
                },
            ],
            {"account_id": "a", "tenant_id": "t", "user_id": "u", "session_id": "s"},
        )
        self.assertTrue(resolved["ready_for_retrieval"])
        self.assertEqual(0, resolved["pending_task_count"])
        self.assertEqual(0, resolved["scheduled_idle_task_count"])
        self.assertEqual(0, resolved["due_idle_task_count"])
        self.assertNotIn("idle_commit_scheduled", resolved["freshness_warnings"])
        self.assertIn("idle_commit_due", readiness["freshness_warnings"])

    def test_moduleized_source_role_budget_matches_local_question_type_defaults(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")

        for question_type in ["fact", "current_state", "profile_memory", "evidence", "multi_hop", "date"]:
            with self.subTest(question_type=question_type):
                helper_budget, helper_mode = helper_mod.auto_source_role_budget_tokens(
                    {"source_role_budget_mode": "auto"},
                    {},
                    remote_budget_tokens=100,
                    question_type=question_type,
                )
                local_budget, local_mode = local_mod.auto_source_role_budget_tokens(
                    {"source_role_budget_mode": "auto"},
                    {},
                    remote_budget_tokens=100,
                    question_type=question_type,
                )
                self.assertEqual("auto", helper_mode)
                self.assertEqual(local_mode, helper_mode)
                self.assertEqual(local_budget, helper_budget)

    def test_moduleized_budget_helpers_default_to_auto_for_explicit_cross_session(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")

        args = {"cross_session": True}
        helper_source_budget, helper_source_mode = helper_mod.auto_source_role_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        local_source_budget, local_source_mode = local_mod.auto_source_role_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        self.assertEqual("auto", helper_source_mode)
        self.assertEqual(local_source_mode, helper_source_mode)
        self.assertEqual(local_source_budget, helper_source_budget)
        self.assertEqual({"assistant": 45, "tool": 35, "user": 60}, helper_source_budget)

        helper_layer_budget, helper_layer_mode = helper_mod.auto_memory_layer_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        local_layer_budget, local_layer_mode = local_mod.auto_memory_layer_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        self.assertEqual("auto", helper_layer_mode)
        self.assertEqual(local_layer_mode, helper_layer_mode)
        self.assertEqual(local_layer_budget, helper_layer_budget)
        self.assertEqual(40, helper_layer_budget["profile_entity"])
        self.assertEqual(25, helper_layer_budget["cross_session_event"])

        helper_selection_budget, helper_selection_mode = helper_mod.auto_memory_selection_policy_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        local_selection_budget, local_selection_mode = local_mod.auto_memory_selection_policy_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        self.assertEqual("auto", helper_selection_mode)
        self.assertEqual(local_selection_mode, helper_selection_mode)
        self.assertEqual(local_selection_budget, helper_selection_budget)
        self.assertEqual(45, helper_selection_budget["selected_user_prompt"])

        helper_phase_budget, helper_phase_mode = helper_mod.auto_extraction_phase_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        local_phase_budget, local_phase_mode = local_mod.auto_extraction_phase_budget_tokens(
            args,
            {},
            remote_budget_tokens=100,
            question_type="fact",
        )
        self.assertEqual("auto", helper_phase_mode)
        self.assertEqual(local_phase_mode, helper_phase_mode)
        self.assertEqual(local_phase_budget, helper_phase_budget)
        self.assertEqual({"pending_async": 12, "provisional": 25, "final": 70}, helper_phase_budget)

    def test_local_adapter_uses_moduleized_feature_profile_pre_refresh_budget(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")

        args = {
            "query": "feature-focused profile memory threshold extraction",
            "memory_layer_budget_mode": "auto",
        }
        ranking = {"query": "feature profile memory across sessions"}

        helper_budget, helper_mode = helper_mod.pre_retrieval_summary_refresh_memory_layer_budget_tokens(
            remote_budget_tokens=100,
            question_type="profile_memory",
            args=args,
            ranking=ranking,
        )
        local_budget, local_mode = local_mod.pre_retrieval_summary_refresh_memory_layer_budget_tokens(
            remote_budget_tokens=100,
            question_type="profile_memory",
            args=args,
            ranking=ranking,
        )

        self.assertEqual("pre_retrieval_summary_refresh_feature_profile_memory", helper_mode)
        self.assertEqual(helper_mode, local_mode)
        self.assertEqual(helper_budget, local_budget)
        self.assertEqual(65, local_budget["profile_entity"])
        self.assertEqual(75, local_budget["cross_session_memory_feature_entity"])
        self.assertEqual(75, local_budget["cross_session_memory_feature_summary"])
        self.assertEqual(75, local_budget["cross_session_memory_feature_compression"])

    def test_moduleized_memory_selection_budget_infers_from_related_auto_modes(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")

        for budget_mode_field in ["source_role_budget_mode", "memory_layer_budget_mode"]:
            with self.subTest(budget_mode_field=budget_mode_field):
                args = {budget_mode_field: "auto"}
                helper_budget, helper_mode = helper_mod.auto_memory_selection_policy_budget_tokens(
                    args,
                    {},
                    remote_budget_tokens=100,
                    question_type="profile_memory",
                )
                local_budget, local_mode = local_mod.auto_memory_selection_policy_budget_tokens(
                    args,
                    {},
                    remote_budget_tokens=100,
                    question_type="profile_memory",
                )
                self.assertEqual("auto", helper_mode)
                self.assertEqual(local_mode, helper_mode)
                self.assertEqual(local_budget, helper_budget)
                self.assertEqual(65, helper_budget["selected_profile_current_state"])

    def test_auto_memory_selection_budget_prioritizes_profile_memory_current_state(self) -> None:
        helper_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pre_refresh")

        current_budgets, current_mode = helper_mod.auto_memory_selection_policy_budget_tokens(
            {"memory_selection_policy_budget_mode": "auto"},
            {},
            remote_budget_tokens=100,
            question_type="current_state",
        )
        self.assertEqual("auto", current_mode)
        self.assertEqual(50, current_budgets["selected_profile_current_state"])

        profile_budgets, profile_mode = helper_mod.auto_memory_selection_policy_budget_tokens(
            {"memory_selection_policy_budget_mode": "auto"},
            {},
            remote_budget_tokens=100,
            question_type="profile_memory",
        )
        self.assertEqual("auto", profile_mode)
        self.assertEqual(
            {
                "selected_user_prompt": 70,
                "selected_assistant_profile_fact": 70,
                "selected_assistant_decision_outcome_only": 20,
                "selected_tool_evidence_only": 20,
                "selected_profile_current_state": 70,
            },
            profile_budgets,
        )

        current_phase_budgets, current_phase_mode = helper_mod.auto_extraction_phase_budget_tokens(
            {"extraction_phase_budget_mode": "auto"},
            {},
            remote_budget_tokens=100,
            question_type="current_state",
        )
        self.assertEqual("auto", current_phase_mode)
        self.assertEqual({"pending_async": 12, "provisional": 25, "final": 75}, current_phase_budgets)

        profile_phase_budgets, profile_phase_mode = helper_mod.auto_extraction_phase_budget_tokens(
            {"extraction_phase_budget_mode": "auto"},
            {},
            remote_budget_tokens=100,
            question_type="profile_memory",
        )
        self.assertEqual("auto", profile_phase_mode)
        self.assertEqual({"pending_async": 10, "provisional": 20, "final": 80}, profile_phase_budgets)

        evidence_phase_budgets, evidence_phase_mode = helper_mod.auto_extraction_phase_budget_tokens(
            {"extraction_phase_budget_mode": "auto"},
            {},
            remote_budget_tokens=100,
            question_type="evidence",
        )
        self.assertEqual("auto", evidence_phase_mode)
        self.assertEqual({"pending_async": 15, "provisional": 35, "final": 70}, evidence_phase_budgets)

        inferred_phase_budgets, inferred_phase_mode = helper_mod.auto_extraction_phase_budget_tokens(
            {},
            {"memory_layer_budget_mode": "auto"},
            remote_budget_tokens=100,
        )
        self.assertEqual("auto", inferred_phase_mode)
        self.assertEqual({"pending_async": 12, "provisional": 25, "final": 70}, inferred_phase_budgets)

    def test_cross_session_policy_enables_feature_and_current_memory_queries(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        policy_mod = importlib.import_module("tools.matrixark_mcp_budget_policies")

        scenarios = [
            (
                {"query": "focus on profile memory and threshold batch extraction features"},
                "fact",
                "always_consider_same_user_cross_session_for_profile_memory",
                0.30,
            ),
            (
                {"query": "what is the latest current preference for memory extraction?"},
                "current_state",
                "always_consider_same_user_cross_session_for_query_type",
                0.20,
            ),
            (
                {"query": "show the evidence from previous tool results"},
                "evidence",
                "always_consider_same_user_cross_session_for_query_type",
                0.15,
            ),
        ]
        for args, question_type, decision, budget_ratio in scenarios:
            with self.subTest(question_type=question_type, query=args["query"]):
                core_policy = core_mod.build_cross_session_policy(
                    args,
                    {},
                    question_type=question_type,
                    session_scope="same_session",
                    remote_budget_tokens=2000,
                )
                module_policy = policy_mod.build_cross_session_policy(
                    args,
                    {},
                    question_type=question_type,
                    session_scope="same_session",
                    remote_budget_tokens=2000,
                )
                self.assertEqual(core_policy, module_policy)
                self.assertTrue(core_policy["enabled"])
                self.assertEqual(decision, core_policy["decision"])
                self.assertEqual(budget_ratio, core_policy["budget_ratio"])
                self.assertGreater(core_policy["budget_tokens"], 0)

        disabled = core_mod.build_cross_session_policy(
            {"query": "direct fact lookup in this turn"},
            {},
            question_type="fact",
            session_scope="same_session",
            remote_budget_tokens=2000,
        )
        self.assertFalse(disabled["enabled"])
        self.assertEqual(0, disabled["budget_tokens"])

    def test_moduleized_cross_session_policy_matches_profile_memory_core_budget(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        policy_mod = importlib.import_module("tools.matrixark_mcp_budget_policies")

        for question_type in ["profile_memory", "multi_hop", "date"]:
            for remote_budget_tokens in [1000, 1500]:
                with self.subTest(question_type=question_type, remote_budget_tokens=remote_budget_tokens):
                    core_policy = core_mod.build_cross_session_policy(
                        {},
                        {},
                        question_type=question_type,
                        session_scope="prefer",
                        remote_budget_tokens=remote_budget_tokens,
                    )
                    module_policy = policy_mod.build_cross_session_policy(
                        {},
                        {},
                        question_type=question_type,
                        session_scope="prefer",
                        remote_budget_tokens=remote_budget_tokens,
                    )
                    for field in [
                        "enabled",
                        "question_type",
                        "question_budget_reason",
                        "budget_ratio",
                        "budget_tokens",
                        "computed_budget_tokens",
                        "budget_floor_tokens",
                        "budget_floor_applied",
                        "budget_floor_status",
                        "preferred_ref_types",
                        "budget_guidance",
                    ]:
                        self.assertEqual(core_policy[field], module_policy[field])

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
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
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
            "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
            "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 2},
            "source_profile_promotion_policies": ["always_when_profile_scope_available"],
            "source_profile_promotion_blockers": ["profile_scope_missing"],
            "source_profile_memory_classes": ["memory_feature"],
            "source_profile_memory_kinds": ["memory_feature"],
            "source_memory_scopes": ["user_profile"],
            "source_session_continuities": ["cross_session"],
            "source_extraction_phases": ["final"],
            "source_entity_types": ["assistant_decision", "tool_evidence"],
            "source_final_session_boundary_count": 1,
            "profile_summary_current": True,
            "profile_memory_class": "memory_feature",
            "profile_memory_kind": "memory_feature",
            "profile_promotion_policy": "always_when_profile_scope_available",
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
        self.assertEqual(
            ["selected_assistant_decision_outcome_only"],
            annotated["source_memory_selection_policies"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 2},
            annotated["source_memory_selection_policy_counts"],
        )
        self.assertEqual(
            ["always_when_profile_scope_available"],
            annotated["source_profile_promotion_policies"],
        )
        self.assertEqual(["profile_scope_missing"], annotated["source_profile_promotion_blockers"])
        self.assertEqual(["memory_feature"], annotated["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], annotated["source_profile_memory_kinds"])
        self.assertEqual(["assistant_decision", "tool_evidence"], annotated["source_entity_types"])
        self.assertTrue(annotated["final_session_boundary"])
        self.assertTrue(annotated["profile_summary_current"])
        self.assertEqual("memory_feature", annotated["profile_memory_class"])
        self.assertEqual("memory_feature", annotated["profile_memory_kind"])
        self.assertEqual("always_when_profile_scope_available", annotated["profile_promotion_policy"])
        self.assertEqual("profile_summary", budget_mod.candidate_memory_layer_name(annotated))
        self.assertEqual(["assistant"], annotated["source_roles"])
        self.assertEqual({"assistant": 2}, annotated["source_role_counts"])
        self.assertEqual(["PreviousAssistantBackfill"], annotated["source_codex_events"])
        self.assertEqual({"PreviousAssistantBackfill": 1}, annotated["source_codex_event_counts"])
        self.assertEqual(1, annotated["source_final_session_boundary_count"])

    def test_moduleized_summary_scan_admits_final_session_summary(self) -> None:
        scan_mod = importlib.import_module("tools.matrixark_mcp_retrieve_summary_scan")
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        scope = {
            "tenant_id": "tenant_final_summary",
            "user_id": "user_final_summary",
            "session_id": "session_final_summary",
        }
        summary = {
            "record_type": "context_summary",
            "summary_type": "session_final",
            "summary_hash": 101,
            "node_hash": 202,
            "node_path": [
                "tenant:tenant_final_summary",
                "user:user_final_summary",
                "session:session_final_summary",
            ],
            "summary_text": "Session finalized after threshold extraction covering assistant decision evidence.",
            "scope": scope,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": "final",
            "final_session_boundary": True,
            "source_extraction_phases": ["provisional"],
            "updated_at_ms": 1000,
        }

        matches, dropped, matched, reason = scan_mod.scan_summary_candidates(
            [summary],
            retrieval_scope=scope,
            selected_by_tree=lambda _record: True,
            index_terms_by_batch={},
            index_terms_by_node={},
            index_terms_by_ref={},
            secondary_index_filter_groups=[],
            secondary_index_filter_mode="any",
            admit_candidate_for_node=lambda _record: True,
            query_terms={"session", "finalized", "assistant"},
            query_embedding=[0.1, 0.2, 0.3],
            node_scores={202: {"score": 0.4}},
            annotate_session_continuity=lambda candidate, _record: candidate,
            ranking={},
            reference_time_ms=1000,
            deadline_exceeded=lambda: False,
        )

        self.assertEqual("", reason)
        self.assertEqual(0, dropped)
        self.assertEqual(1, matched)
        self.assertEqual(1, len(matches))
        self.assertEqual("session_final", matches[0]["summary_type"])
        self.assertEqual("same_session_summary", budget_mod.candidate_memory_layer_name(matches[0]))
        self.assertEqual("final", matches[0]["extraction_phase"])
        self.assertTrue(matches[0]["final_session_boundary"])

    def test_event_candidate_preserves_live_memory_layer_tags(self) -> None:
        builders = importlib.import_module("tools.matrixark_mcp_retrieve_candidate_builders")
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        record = {
            "record_type": "context_event",
            "event_id_hash": 303,
            "node_hash": 404,
            "node_path": ["tenant:tenant_live", "user:user_live", "session:session_live"],
            "text": "user: live hook event waiting for async extraction",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": "pending_async",
            "final_session_boundary": False,
            "source_memory_scopes": ["session"],
            "source_session_continuities": ["same_session"],
            "source_extraction_phases": ["pending_async"],
            "updated_at_ms": 123,
        }

        candidate = builders.event_candidate(
            record,
            envelope={"ingestion_time_ms": 123},
            record_scope={"tenant_id": "tenant_live", "user_id": "user_live", "session_id": "session_live"},
            index_terms={"memory_scope:session", "extraction_phase:pending_async"},
            event_type="pending_async",
            origin_score=0.8,
            keyword_score=2,
            sparse_score=0.4,
            embedding_score=0.3,
            node_score=0.2,
            metadata={},
            text=record["text"],
        )

        self.assertEqual("session", candidate["memory_scope"])
        self.assertEqual("same_session", candidate["session_continuity"])
        self.assertEqual("pending_async", candidate["extraction_phase"])
        self.assertFalse(candidate["final_session_boundary"])
        self.assertEqual(["session"], candidate["source_memory_scopes"])
        self.assertEqual(["same_session"], candidate["source_session_continuities"])
        self.assertEqual(["pending_async"], candidate["source_extraction_phases"])
        self.assertEqual("pending_async_event", budget_mod.candidate_memory_layer_name(candidate))


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
        agent_hook_mod = importlib.import_module("tools.matrixark_agent_hook")
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        runtime_mod = importlib.import_module("tools.matrixark_mcp_session_runtime")

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
        args = {
            "async_processing": True,
            "auto_batch_extract": True,
            "session_buffer_threshold": 2,
            "idle_commit_timeout_ms": 50,
        }

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
        self.assertEqual(173, first["session_buffer"]["idle_commit_deadline_ms"])
        self.assertEqual(1, first["session_buffer"]["idle_commit_pending_message_count"])
        scheduled_task = next(
            record
            for record in target.records
            if record.get("record_type") == "matrixark_async_pipeline_task"
            and record.get("status") == "idle_commit_scheduled"
        )
        self.assertEqual(173, scheduled_task["idle_commit_deadline_ms"])
        self.assertEqual(1, scheduled_task["idle_commit_pending_event_count"])
        self.assertEqual(1, scheduled_task["idle_commit_pending_message_count"])
        self.assertEqual(["pending_async"], scheduled_task["source_extraction_phases"])
        self.assertEqual("pending_async", scheduled_task["extraction_phase"])
        self.assertFalse(scheduled_task["final_session_boundary"])

        feature_prompt = (
            "Focus on feature parity for session memory, "
            "no testing or monitoring evidence in this pass."
        )
        feature_metadata = agent_hook_mod.agent_memory_selection_metadata(
            {"messages": [{"role": "user", "content": feature_prompt}]},
            event="UserPromptSubmit",
            text=feature_prompt,
            messages=[{"role": "user", "content": feature_prompt}],
        )
        self.assertEqual(["memory_feature"], feature_metadata["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], feature_metadata["source_profile_memory_kinds"])
        target = Target()
        feature_envelope = {
            **envelope,
            "metadata": {
                "hook_type": "before_llm",
                "codex_event": "UserPromptSubmit",
                "codex_memory_selection": feature_metadata["codex_memory_selection"],
            },
            "messages": [{"role": "user", "content": feature_prompt}],
            "ingestion_time_ms": 223,
        }
        feature_result = async_mod.lightweight_async_accept(
            target,
            {**args, "session_buffer_threshold": 20},
            envelope=feature_envelope,
            hook=None,
            idle_commit_result={},
        )
        feature_event = next(record for record in target.records if record["record_type"] == "context_event")
        feature_idle_task = next(
            record
            for record in target.records
            if record.get("record_type") == "matrixark_async_pipeline_task"
            and record.get("status") == "idle_commit_scheduled"
        )
        feature_index_names = {
            str(record.get("index_name") or "")
            for record in target.records
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_event"
        }
        self.assertEqual(["memory_feature"], feature_event["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], feature_event["source_profile_memory_kinds"])
        self.assertEqual("memory_feature", feature_event["profile_memory_class"])
        self.assertEqual("memory_feature", feature_event["profile_memory_kind"])
        self.assertEqual(
            "pending_async_memory_feature_event",
            budget_mod.candidate_memory_layer_name({**feature_event, "ref_type": "event"}),
        )
        self.assertEqual(["memory_feature"], feature_idle_task["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], feature_idle_task["source_profile_memory_kinds"])
        self.assertIn("profile_memory_class:memory_feature", feature_index_names)
        self.assertIn("profile_memory_kind:memory_feature", feature_index_names)
        self.assertIsNone(feature_result["auto_batch_extract_result"])
        self.assertEqual(273, feature_result["session_buffer"]["idle_commit_deadline_ms"])
        self.assertEqual(223, feature_result["session_buffer"]["idle_commit_cutoff_ms"])

        class BufferAdapter:
            def __init__(self) -> None:
                self.records = []

            def append(self, record):
                self.records.append(record)

        buffer_adapter = BufferAdapter()
        runtime_mod.append_session_buffer_event(
            buffer_adapter,
            envelope={
                **envelope,
                "metadata": {"codex_event": "Stop"},
                "messages": [{"role": "assistant", "content": "final assistant decision"}],
                "ingestion_time_ms": 124,
            },
            event_id_hash=99,
            node_hash=100,
            node_path=["tenant:tenant", "user:user", "session:session"],
            hook=None,
        )
        buffered = buffer_adapter.records[0]
        self.assertEqual({"assistant": 1}, buffered["source_role_counts"])
        self.assertEqual({"after_llm": 1}, buffered["source_hook_type_counts"])
        self.assertEqual({"Stop": 1}, buffered["source_codex_event_counts"])

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

    def test_modular_local_ingest_threshold_uses_pending_message_count(self) -> None:
        local_ingest_mod = importlib.import_module("tools.matrixark_mcp_local_ingest")

        class Batch:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class Target:
            def __init__(self) -> None:
                self.pending = []
                self.records = []
                self.commit_calls = []

            def read_all(self):
                return []

            def _observe_model_latency(self, *_args):
                return None

            def write_batch(self, _name):
                return Batch()

            def default_session_node_path(self, scope):
                return ["tenant:t", "user:u", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **_kwargs):
                return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

            def append(self, record):
                self.records.append(record)

            def append_many(self, records):
                self.records.extend(records)

            def session_buffer_enabled(self, _args, *, kind):
                return True

            def auto_batch_extract_enabled(self, _args, *, kind):
                return True

            def session_boundary_commit_requested(self, _args, *, hook=None):
                return False

            def append_session_buffer_event(self, **kwargs):
                self.pending.append(
                    {
                        "event_id_hash": kwargs["event_id_hash"],
                        "envelope": kwargs["envelope"],
                        "agent_hook": kwargs.get("hook"),
                    }
                )

            def pending_session_events(self, _scope):
                return list(self.pending)

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append(args)
                self.pending.clear()
                return {
                    "status": "committed",
                    "trigger_policy": "threshold",
                    "committed_event_count": 1,
                    "trigger_evidence": {
                        "pending_event_count": 1,
                        "pending_message_count": 2,
                        "threshold_ready": True,
                    },
                }

            def append_node_summary_embeddings(self, **_kwargs):
                return {"status": "dirty_marked", "dirty_hashes": [1]}

        scope = {
            "account_id": "acct",
            "tenant_id": "tenant",
            "user_id": "user",
            "session_id": "session",
        }
        envelope = {
            "kind": "message",
            "scope": scope,
            "metadata": {},
            "messages": [
                {"role": "user", "content": "first message in a combined hook event"},
                {"role": "assistant", "content": "second message should satisfy threshold"},
            ],
            "ingestion_time_ms": 123,
            "storage_options": {},
            "storage_route": {},
        }
        ingest_start = {
            "envelope": envelope,
            "hook": None,
            "backend_readiness": None,
            "idle_commit_result": None,
            "lightweight_result": None,
        }

        result = local_ingest_mod.ingest_after_start(
            Target(),
            {"session_buffer_threshold": 2, "skip_prior_context": True},
            ingest_start,
        )

        self.assertTrue(result["session_buffer"]["threshold_ready"])
        self.assertEqual(1, result["session_buffer"]["pending_event_count"])
        self.assertEqual(2, result["session_buffer"]["pending_message_count"])
        self.assertEqual("committed", result["auto_batch_extract_result"]["status"])

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
                return list(self.appended)

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
                    "summary_refresh": {"status": "dirty_marked", "dirty_hashes": [10, 11], "profile_dirty_hashes": [12]},
                    "profile_promotion_summary": [{"entity_hash": 44, "profile_entity_hash": 55}],
                    "profile_promotion_policy": "always_when_profile_scope_available",
                    "profile_promotion_importance_gate": False,
                    "profile_promotion_blocker": "",
                }

            def append(self, record):
                self.appended.append(record)

            def append_many(self, records):
                self.appended.extend(records)

            def node_summary_dirty_records(self, *, node_path, scope, updated_at_ms, source_ref_type, source_hash_field, source_hash, dirty_reason, propagate_depth, source_lineage):
                return [
                    source_hash + 1,
                ], [
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": source_hash + 1,
                        "node_path": node_path,
                        "scope": scope,
                        "source_ref_type": source_ref_type,
                        source_hash_field: source_hash,
                        "dirty_reason": dirty_reason,
                        "updated_at_ms": updated_at_ms,
                        "source_role_counts": source_lineage.get("source_role_counts", {}),
                    }
                ]

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
                    "metadata": {
                        "hook_type": "before_llm",
                        "codex_event": "UserPromptSubmit",
                        "codex_memory_selection": {
                            "policy": "selected_user_prompt",
                            "policies": ["selected_user_prompt", "selected_user_profile_fact"],
                            "policy_counts": {"selected_user_prompt": 1, "selected_user_profile_fact": 1},
                            "selection_lossy": False,
                            "retained_text_ratio": 1.0,
                            "retained_line_ratio": 1.0,
                        },
                        "source_profile_memory_classes": ["memory_feature"],
                        "source_profile_memory_kinds": ["memory_feature"],
                    },
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
        self.assertEqual(
            ["selected_user_profile_fact", "selected_user_prompt"],
            committed["source_memory_selection_policies"],
        )
        self.assertEqual(
            {"selected_user_prompt": 1, "selected_user_profile_fact": 1},
            committed["source_memory_selection_policy_counts"],
        )
        self.assertEqual(1, committed["source_memory_selection_complete_count"])
        self.assertEqual(1.0, committed["source_memory_selection_retained_text_ratio_avg"])
        self.assertEqual(["memory_feature"], committed["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], committed["source_profile_memory_kinds"])
        self.assertEqual("always_when_profile_scope_available", committed["profile_promotion_policy"])
        self.assertFalse(committed["profile_promotion_importance_gate"])
        self.assertEqual("", committed["profile_promotion_blocker"])
        self.assertEqual([{"entity_hash": 44, "profile_entity_hash": 55}], committed["profile_promotion_summary"])
        self.assertEqual(
            {"selected_user_prompt": 1, "selected_user_profile_fact": 1},
            adapter.batch_extract_calls[0]["metadata"]["source_memory_selection_policy_counts"],
        )
        self.assertEqual(["memory_feature"], adapter.batch_extract_calls[0]["metadata"]["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], adapter.batch_extract_calls[0]["metadata"]["source_profile_memory_kinds"])
        self.assertEqual(committed["trigger_evidence"], adapter.appended[0]["trigger_evidence"])
        self.assertEqual(committed["source_role_counts"], adapter.appended[0]["source_role_counts"])
        self.assertEqual(committed["source_hook_type_counts"], adapter.appended[0]["source_hook_type_counts"])
        self.assertEqual(committed["source_codex_event_counts"], adapter.appended[0]["source_codex_event_counts"])
        self.assertEqual(
            committed["source_memory_selection_policy_counts"],
            adapter.appended[0]["source_memory_selection_policy_counts"],
        )
        self.assertEqual(["memory_feature"], adapter.appended[0]["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], adapter.appended[0]["source_profile_memory_kinds"])
        self.assertEqual("always_when_profile_scope_available", adapter.appended[0]["profile_promotion_policy"])
        self.assertEqual(committed["profile_promotion_summary"], adapter.appended[0]["profile_promotion_summary"])
        async_task = next(record for record in adapter.appended if record["record_type"] == "matrixark_async_pipeline_task")
        self.assertEqual(committed["source_role_counts"], async_task["source_role_counts"])
        self.assertEqual(committed["source_hook_type_counts"], async_task["source_hook_type_counts"])
        self.assertEqual(committed["source_codex_event_counts"], async_task["source_codex_event_counts"])
        self.assertEqual(committed["source_memory_selection_policy_counts"], async_task["source_memory_selection_policy_counts"])
        self.assertEqual(["memory_feature"], async_task["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], async_task["source_profile_memory_kinds"])
        self.assertEqual("always_when_profile_scope_available", async_task["profile_promotion_policy"])
        self.assertEqual(committed["profile_promotion_summary"], async_task["profile_promotion_summary"])
        self.assertEqual(1, async_task["source_memory_selection_complete_count"])
        committed_buffer = next(
            record
            for record in adapter.appended
            if record.get("record_type") == "session_buffer_event"
            and record.get("event_id_hash") == 1
            and record.get("status") == "committed"
        )
        self.assertEqual(committed["commit_id_hash"], committed_buffer["commit_id_hash"])
        self.assertNotIn("envelope", committed_buffer)
        self.assertNotIn("agent_hook", committed_buffer)

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
                    "metadata": {
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                        "source_profile_memory_classes": ["memory_feature"],
                        "source_profile_memory_kinds": ["memory_feature"],
                    },
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

        codex_only_adapter = Adapter()
        codex_only_adapter.pending = [
            {
                "event_id_hash": 3,
                "updated_at_ms": 300,
                "envelope": {
                    "messages": [{"role": "assistant", "content": "final assistant decision"}],
                    "metadata": {"codex_event": "Stop"},
                    "ingestion_time_ms": 300,
                },
            }
        ]
        codex_only_committed = runtime_mod.session_commit(
            codex_only_adapter,
            {"scope": scope, "threshold_messages": 1, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("committed", codex_only_committed["status"])
        self.assertEqual({"after_llm": 1}, codex_only_committed["source_hook_type_counts"])
        self.assertEqual(
            {"after_llm": 1},
            codex_only_adapter.batch_extract_calls[0]["metadata"]["source_hook_type_counts"],
        )
        codex_only_task = next(
            record
            for record in codex_only_adapter.appended
            if record["record_type"] == "matrixark_async_pipeline_task"
        )
        self.assertEqual({"after_llm": 1}, codex_only_task["source_hook_type_counts"])

        multi_adapter.pending = []
        finalized = runtime_mod.session_commit(
            multi_adapter,
            {"scope": scope, "threshold_messages": 20, "force": True, "commit_reason": "hook_boundary"},
        )
        self.assertEqual("finalized", finalized["status"])
        self.assertEqual("force", finalized["trigger_policy"])
        self.assertEqual("final", finalized["extraction_phase"])
        self.assertTrue(finalized["final_session_boundary"])
        self.assertEqual(1, finalized["prior_commit_count"])
        self.assertEqual(1, finalized["prior_committed_event_count"])
        self.assertIn("summary_hash", finalized)
        self.assertEqual(1, finalized["memory_layers_written"]["session_final_summary"])
        self.assertGreaterEqual(finalized["memory_layers_written"]["secondary_indexes"], 5)
        self.assertEqual("dirty_marked", finalized["summary_refresh"]["status"])
        boundaries = [
            record
            for record in multi_adapter.appended
            if record.get("record_type") == "context_session_boundary"
        ]
        self.assertEqual(1, len(boundaries))
        self.assertTrue(boundaries[0]["final_session_boundary"])
        self.assertEqual({"assistant": 1, "tool": 1, "user": 1}, boundaries[0]["source_role_counts"])
        final_summaries = [
            record
            for record in multi_adapter.appended
            if record.get("record_type") == "context_summary"
            and record.get("summary_type") == "session_final"
        ]
        self.assertEqual(1, len(final_summaries))
        self.assertEqual("final", final_summaries[0]["extraction_phase"])
        self.assertTrue(final_summaries[0]["final_session_boundary"])
        self.assertEqual(["provisional"], final_summaries[0]["source_extraction_phases"])
        self.assertEqual(["memory_feature"], final_summaries[0]["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], final_summaries[0]["source_profile_memory_kinds"])
        self.assertEqual("memory_feature", final_summaries[0]["profile_memory_class"])
        self.assertEqual("memory_feature", final_summaries[0]["profile_memory_kind"])
        self.assertEqual(
            ["always_when_profile_scope_available"],
            final_summaries[0]["source_profile_promotion_policies"],
        )
        self.assertEqual("always_when_profile_scope_available", final_summaries[0]["profile_promotion_policy"])
        self.assertEqual([2], final_summaries[0]["source_event_ids"])
        summary_indexes = [
            record
            for record in multi_adapter.appended
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_summary"
            and record.get("ref_hashes") == [final_summaries[0]["summary_hash"]]
        ]
        summary_index_names = {str(record.get("index_name") or "") for record in summary_indexes}
        self.assertIn("summary_type:session_final", summary_index_names)
        self.assertIn("extraction_phase:final", summary_index_names)
        self.assertIn("source_extraction_phase:provisional", summary_index_names)
        self.assertIn("profile_memory_class:memory_feature", summary_index_names)
        self.assertIn("profile_memory_kind:memory_feature", summary_index_names)
        self.assertIn("profile_promotion_policy:always_when_profile_scope_available", summary_index_names)

        second_finalized = runtime_mod.session_commit(
            multi_adapter,
            {"scope": scope, "threshold_messages": 20, "force": True, "commit_reason": "hook_boundary"},
        )
        self.assertEqual("empty", second_finalized["status"])
        self.assertTrue(second_finalized["trigger_evidence"]["already_finalized"])

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
                        **kwargs.get("source_lineage", {}),
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
                "source_memory_selection_policies": [
                    "selected_assistant_decision_outcome_only",
                    "selected_tool_evidence_only",
                ],
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 3,
                    "selected_tool_evidence_only": 2,
                },
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
        self.assertEqual(
            session_entities[0]["source_memory_selection_policy_counts"],
            session_segments[0]["source_memory_selection_policy_counts"],
        )
        self.assertEqual(["assistant", "tool"], session_entities[0]["source_roles"])
        self.assertEqual({"assistant": 3, "tool": 2}, session_entities[0]["source_role_counts"])
        self.assertEqual(["after_llm", "hook_boundary"], session_entities[0]["source_hook_types"])
        self.assertEqual({"after_llm": 3, "hook_boundary": 2}, session_entities[0]["source_hook_type_counts"])
        self.assertEqual(["PostToolUse", "Stop"], session_entities[0]["source_codex_events"])
        self.assertEqual({"PostToolUse": 2, "Stop": 3}, session_entities[0]["source_codex_event_counts"])
        self.assertEqual(
            ["selected_assistant_decision_outcome_only", "selected_tool_evidence_only"],
            session_entities[0]["source_memory_selection_policies"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 3, "selected_tool_evidence_only": 2},
            session_entities[0]["source_memory_selection_policy_counts"],
        )
        self.assertEqual(session_entities[0]["source_role_counts"], profile_entities[0]["source_role_counts"])
        self.assertEqual(session_entities[0]["source_hook_type_counts"], profile_entities[0]["source_hook_type_counts"])
        self.assertEqual(session_entities[0]["source_codex_event_counts"], profile_entities[0]["source_codex_event_counts"])
        expected_profile_selection_counts = dict(session_entities[0]["source_memory_selection_policy_counts"])
        expected_profile_selection_counts["selected_profile_current_state"] = 1
        self.assertEqual(
            expected_profile_selection_counts,
            profile_entities[0]["source_memory_selection_policy_counts"],
        )
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
            ("entity", profile_entities[0]["entity_hash"], "profile_entity_state")
        ]
        segment_embedding = embeddings_by_ref[
            ("segment", session_segments[0]["segment_hash"], "segment_text")
        ]
        self.assertEqual("session", session_entity_embedding["memory_scope"])
        self.assertEqual("same_session", session_entity_embedding["session_continuity"])
        self.assertNotIn("extraction_phase", session_entity_embedding)
        self.assertNotIn("final_session_boundary", session_entity_embedding)
        self.assertNotIn("source_event_ids", session_entity_embedding)
        self.assertNotIn("source_role_counts", session_entity_embedding)
        self.assertNotIn("source_memory_selection_policy_counts", session_entity_embedding)
        self.assertEqual("user_profile", profile_entity_embedding["memory_scope"])
        self.assertEqual("cross_session", profile_entity_embedding["session_continuity"])
        self.assertNotIn("promoted_from_memory_scope", profile_entity_embedding)
        self.assertNotIn("source_session_ids", profile_entity_embedding)
        self.assertNotIn("source_entity_hashes", profile_entity_embedding)
        self.assertNotIn("source_role_counts", profile_entity_embedding)
        self.assertNotIn("source_memory_selection_policy_counts", profile_entity_embedding)
        self.assertEqual("session", segment_embedding["memory_scope"])
        self.assertEqual("same_session", segment_embedding["session_continuity"])
        self.assertNotIn("source_memory_scopes", segment_embedding)
        self.assertNotIn("source_session_continuities", segment_embedding)
        self.assertNotIn("source_event_ids", segment_embedding)
        self.assertNotIn("source_memory_selection_policy_counts", segment_embedding)
        event_embeddings = [
            record
            for record in embeddings
            if record.get("ref_type") == "event" and record.get("embedding_type") == "event_text"
        ]
        self.assertTrue(event_embeddings)
        self.assertTrue(all(record.get("memory_scope") == "session" for record in event_embeddings))
        self.assertTrue(all(record.get("session_continuity") == "same_session" for record in event_embeddings))
        self.assertTrue(all("extraction_phase" not in record for record in event_embeddings))
        self.assertTrue(all("final_session_boundary" not in record for record in event_embeddings))
        self.assertTrue(all("extraction_context_event_ids" not in record for record in event_embeddings))
        summary_records = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_summary" and record.get("summary_type") == "batch_l0"
        ]
        self.assertEqual(1, len(summary_records))
        self.assertEqual("session", summary_records[0]["memory_scope"])
        self.assertEqual("same_session", summary_records[0]["session_continuity"])
        self.assertEqual(
            session_entities[0]["source_memory_selection_policy_counts"],
            summary_records[0]["source_memory_selection_policy_counts"],
        )
        summary_embedding = embeddings_by_ref[
            ("summary", summary_records[0]["summary_hash"], "batch_l0")
        ]
        self.assertEqual("session", summary_embedding["memory_scope"])
        self.assertEqual("same_session", summary_embedding["session_continuity"])
        self.assertNotIn("extraction_phase", summary_embedding)
        self.assertNotIn("source_entity_hashes", summary_embedding)
        self.assertNotIn("source_segment_hashes", summary_embedding)
        self.assertNotIn("source_memory_selection_policy_counts", summary_embedding)
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
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", index_names)
            self.assertIn("memory_selection_policy:selected_tool_evidence_only", index_names)
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
        self.assertEqual("user_profile", profile_dirty[0]["memory_scope"])
        self.assertEqual("cross_session", profile_dirty[0]["session_continuity"])
        self.assertEqual(["session_mod"], profile_dirty[0]["source_session_ids"])
        self.assertEqual([session_entities[0]["entity_hash"]], profile_dirty[0]["source_entity_hashes"])
        self.assertEqual([101], profile_dirty[0]["source_event_ids"])
        self.assertEqual(session_entities[0]["source_role_counts"], profile_dirty[0]["source_role_counts"])
        self.assertEqual(session_entities[0]["source_hook_type_counts"], profile_dirty[0]["source_hook_type_counts"])
        self.assertEqual(session_entities[0]["source_codex_event_counts"], profile_dirty[0]["source_codex_event_counts"])
        self.assertEqual(
            expected_profile_selection_counts,
            profile_dirty[0]["source_memory_selection_policy_counts"],
        )
        self.assertEqual("always_when_profile_scope_available", profile_dirty[0]["profile_promotion_policy"])
        self.assertEqual("final", profile_dirty[0]["extraction_phase"])
        self.assertTrue(profile_dirty[0]["final_session_boundary"])

    # This test exercises context_segment rows, which are OFF unless a tenant asks for them.
    # Patched for the duration of the test only: setting it at module scope would leak across
    # the single-process suite run and flip the knob for tests that assert it is off.
    @mock.patch.dict(os.environ, {"MATRIXARK_EXTRACT_SEGMENTS": "1"})
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
                    "content": "Remember: use Ubuntu /opt/github-services for all TemporalStore repos.",
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

