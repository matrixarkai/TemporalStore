# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexPipelinePart1 methods split from test_matrixark_codex_hook_pipeline.MatrixArkCodexHookPipelineTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_pipeline import (
    MatrixArkLocalAdapter,
    Path,
    async_pipeline_retrieval_readiness,
    build_node_summary_refresh_records,
    candidate_index_terms,
    compact_context_pack_audit_record,
    compact_context_pack_for_serving,
    compact_context_pack_for_serving_flat,
    compact_context_pack_ref,
    compact_dropped_refs_for_context_pack,
    compact_refs_for_audit,
    compression_context_index_records,
    core_compact_context_pack_audit_record,
    core_compact_context_pack_for_serving,
    dropped_ref_layer_budget,
    identity_hashes,
    infer_query_type,
    json,
    matrixark_codex_hook,
    matrixark_local_recovery_report,
    matrixark_mcp_core,
    matrixark_mcp_query,
    matrixark_mcp_summary_runtime,
    memory_layer_pressure_summary,
    mock,
    quality_first_underfill_summary,
    select_token_budgeted_refs,
    selected_ref_layer_budget,
    tempfile,
)
except ImportError:
    from test_matrixark_codex_hook_pipeline import (
    MatrixArkLocalAdapter,
    Path,
    async_pipeline_retrieval_readiness,
    build_node_summary_refresh_records,
    candidate_index_terms,
    compact_context_pack_audit_record,
    compact_context_pack_for_serving,
    compact_context_pack_for_serving_flat,
    compact_context_pack_ref,
    compact_dropped_refs_for_context_pack,
    compact_refs_for_audit,
    compression_context_index_records,
    core_compact_context_pack_audit_record,
    core_compact_context_pack_for_serving,
    dropped_ref_layer_budget,
    identity_hashes,
    infer_query_type,
    json,
    matrixark_codex_hook,
    matrixark_local_recovery_report,
    matrixark_mcp_core,
    matrixark_mcp_query,
    matrixark_mcp_summary_runtime,
    memory_layer_pressure_summary,
    mock,
    quality_first_underfill_summary,
    select_token_budgeted_refs,
    selected_ref_layer_budget,
    tempfile,
)


class _CodexPipelinePart1:
    def test_cross_session_query_words_override_latest_and_date_classifiers(self) -> None:
        for query in [
            "Compare the latest Codex decisions across sessions",
            "What changed today between previous sessions?",
            "Show current blockers from other sessions together",
        ]:
            self.assertEqual("multi_hop", infer_query_type(query), query)
            self.assertEqual("multi_hop", matrixark_mcp_query.infer_query_type(query), query)

    def test_retrieval_records_use_secondary_index_posting_prefilter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-secondary-prefilter.jsonl")
            scope = {
                "account_id": "acct_prefilter",
                "tenant_id": "tenant_prefilter",
                "user_id": "user_prefilter",
                "session_id": "session_prefilter",
            }
            wanted_entity_hash = matrixark_mcp_core.stable_hash("wanted-profile-prefilter-entity")
            unrelated_entity_hash = matrixark_mcp_core.stable_hash("unrelated-profile-prefilter-entity")
            wanted_node_hash = matrixark_mcp_core.stable_hash("wanted-profile-node")
            unrelated_node_hash = matrixark_mcp_core.stable_hash("unrelated-profile-node")
            adapter.append_many(
                [
                    {
                        "record_type": "context_entity",
                        "entity_hash": wanted_entity_hash,
                        "node_hash": wanted_node_hash,
                        "entity_type": "assistant_decision",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "state": "Codex selected the durable rollout decision.",
                        "text": "assistant decision rollout metadata selected",
                        "access_scope": scope,
                        "scope": scope,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": unrelated_entity_hash,
                        "node_hash": unrelated_node_hash,
                        "entity_type": "tool_evidence",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "state": "Unrelated tool evidence should not pass the profile index prefilter.",
                        "text": "unrelated tool evidence",
                        "access_scope": scope,
                        "scope": scope,
                    },
                    matrixark_mcp_core.context_index_posting_record(
                        index_name="memory_scope:user_profile",
                        data_model="context_profile_entity",
                        ref_type="entity",
                        ref_hashes=[wanted_entity_hash],
                        node_hash=wanted_node_hash,
                        scope=scope,
                    ),
                ]
            )

            result = adapter.retrieval_records(
                scope={**scope, "_session_scope": "prefer"},
                secondary_index_groups=[{"memory_scope:user_profile"}],
            )
            records = result["records"]
            stats = result["scan_stats"]
            self.assertTrue(stats["secondary_index_prefilter_enabled"], stats)
            self.assertFalse(stats["broad_scan_used"], stats)
            self.assertEqual("local_secondary_index_prefilter", stats["broad_scan_reason"])
            self.assertEqual(1, stats["index_postings_read"])
            self.assertEqual(1, stats["index_posting_ref_hash_count"])
            self.assertEqual(1, stats["index_posting_node_hash_count"])
            self.assertGreaterEqual(stats["dropped_by_secondary_index"], 1)
            self.assertTrue(any(record.get("entity_hash") == wanted_entity_hash for record in records), records)
            self.assertFalse(any(record.get("entity_hash") == unrelated_entity_hash for record in records), records)

            fallback = adapter.retrieval_records(
                scope={**scope, "_session_scope": "prefer"},
                secondary_index_groups=[{"memory_scope:missing_profile"}],
            )
            self.assertFalse(fallback["scan_stats"]["secondary_index_prefilter_enabled"], fallback)
            self.assertTrue(fallback["scan_stats"]["broad_scan_used"], fallback)
            self.assertEqual("no_matching_secondary_index_postings", fallback["scan_stats"]["broad_scan_reason"])

    def test_profile_memory_queries_stay_profile_memory_in_oss_understanding_mode(self) -> None:
        query = "Show user profile long-term memory and cross-session entities"
        with mock.patch("matrixark_mcp_core.understanding_provider", return_value="oss_encoder"):
            self.assertEqual("profile_memory", infer_query_type(query))
            self.assertEqual("profile_memory", matrixark_mcp_query.infer_query_type(query))
        self.assertIn("profile_memory", matrixark_mcp_core.QUERY_TYPE_LABELS)
        self.assertIn("profile_memory", matrixark_mcp_query.QUERY_TYPE_LABELS)

    def test_candidate_index_terms_keep_context_event_distinct_from_context_segment(self) -> None:
        event_terms = candidate_index_terms(
            {
                "record_type": "context_event",
                "event_type": "tool_evidence",
                "source_type": "message",
                "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                "source_memory_selection_lossy_count": 1,
            },
            {},
            {},
        )
        segment_terms = candidate_index_terms(
            {
                "record_type": "context_segment",
                "topic": "tool_evidence",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": "provisional",
                "source_roles": ["tool"],
                "source_role_counts": {"tool": 1},
                "source_hook_types": ["tool_result"],
                "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                "source_memory_selection_lossy_count": 1,
            },
            {},
            {},
        )
        self.assertIn("event_type:tool_evidence", event_terms)
        self.assertIn("source_type:message", event_terms)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", event_terms)
        self.assertIn("memory_selection_quality:lossy", event_terms)
        self.assertIn("segment_topic:tool_evidence", segment_terms)
        self.assertIn("memory_scope:session", segment_terms)
        self.assertIn("session_continuity:same_session", segment_terms)
        self.assertIn("extraction_phase:provisional", segment_terms)
        self.assertIn("source_role:tool", segment_terms)
        self.assertIn("hook_type:tool_result", segment_terms)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", segment_terms)
        self.assertIn("memory_selection_quality:lossy", segment_terms)
        self.assertNotIn("event_type:tool_evidence", segment_terms)
        self.assertNotIn("source_type:message", segment_terms)

    def test_secondary_index_cap_preserves_memory_layer_and_selection_terms(self) -> None:
        from tools.matrixark_mcp_core import limited_index_terms as core_limited_index_terms
        from tools.matrixark_mcp_indexing import limited_index_terms

        crowded_terms = [
            "keyword:alpha",
            "keyword:beta",
            "keyword:gamma",
            "keyword:delta",
            "keyword:epsilon",
            "keyword:zeta",
            "keyword:eta",
            "keyword:theta",
            "keyword:iota",
            "keyword:kappa",
            "memory_scope:user_profile",
            "session_continuity:cross_session",
            "extraction_phase:final",
            "memory_selection_policy:selected_profile_current_state",
            "memory_selection_quality:complete",
            "entity_type:assistant_decision",
        ]

        for selected in [
            limited_index_terms(crowded_terms, limit=10),
            core_limited_index_terms(crowded_terms, limit=10),
        ]:
            self.assertIn("entity_type:assistant_decision", selected)
            self.assertIn("memory_scope:user_profile", selected)
            self.assertIn("session_continuity:cross_session", selected)
            self.assertIn("extraction_phase:final", selected)
            self.assertIn("memory_selection_policy:selected_profile_current_state", selected)
            self.assertIn("memory_selection_quality:complete", selected)

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
        self.assertTrue(summary["profile_summary_current"])
        self.assertEqual(summary["source_role_counts"], result["source_role_counts"])
        embeddings = [record for record in result["records"] if record.get("record_type") == "context_embedding"]
        self.assertTrue(embeddings)
        for embedding in embeddings:
            self.assertEqual("summary", embedding["ref_type"])
            self.assertEqual("user_profile", embedding["memory_scope"])
            self.assertEqual("cross_session", embedding["session_continuity"])
            self.assertTrue(embedding["profile_summary_current"])
            self.assertEqual(["assistant", "tool", "user"], embedding["source_roles"])
            self.assertEqual({"assistant": 3, "tool": 2, "user": 1}, embedding["source_role_counts"])
            self.assertEqual(["after_llm", "before_llm", "tool_result"], embedding["source_hook_types"])
            self.assertEqual({"after_llm": 2, "before_llm": 1, "tool_result": 2}, embedding["source_hook_type_counts"])
            self.assertEqual(["PostToolUse", "Stop", "UserPromptSubmit"], embedding["source_codex_events"])
            self.assertEqual({"PostToolUse": 2, "Stop": 2, "UserPromptSubmit": 1}, embedding["source_codex_event_counts"])
            self.assertEqual(["session", "user_profile"], embedding["source_memory_scopes"])
            self.assertEqual(["cross_session", "same_session"], embedding["source_session_continuities"])
            self.assertEqual(["selected_assistant_decision_outcome_only", "selected_tool_evidence_only", "selected_user_prompt"], embedding["source_memory_selection_policies"])
            self.assertEqual({"selected_assistant_decision_outcome_only": 2, "selected_tool_evidence_only": 2, "selected_user_prompt": 1}, embedding["source_memory_selection_policy_counts"])
        index_names = {
            record.get("index_name")
            for record in result["records"]
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_summary"
        }
        self.assertIn("profile_summary_current:true", index_names)

    def test_summary_runtime_refresh_preserves_selection_counts_in_audit_and_result(self) -> None:
        scope = {
            "account_id": "acct_runtime_refresh",
            "tenant_id": "tenant_runtime_refresh",
            "user_id": "user_runtime_refresh",
        }
        node_path = ["tenant:tenant_runtime_refresh", "user:user_runtime_refresh", "profile:long_term_memory"]
        node_hash = 525252
        dirty_hash = 525253
        event = {
            "record_type": "context_event",
            "event_id_hash": 525254,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "access_scope": scope,
            "text": "tool: Exit code 0 proves summary refresh selection lineage",
            "source_roles": ["tool"],
            "source_role_counts": {"tool": 1},
            "source_hook_types": ["tool_result"],
            "source_hook_type_counts": {"tool_result": 1},
            "source_codex_events": ["PostToolUse"],
            "source_codex_event_counts": {"PostToolUse": 1},
            "source_memory_selection_policies": ["selected_tool_evidence_only"],
            "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": "provisional",
        }
        entity = {
            "record_type": "context_entity",
            "entity_hash": 525255,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "access_scope": scope,
            "entity_type": "tool_evidence",
            "entity_name": "summary_refresh_selection_lineage",
            "state": "tool evidence should survive later summary refresh",
            "source_roles": ["tool"],
            "source_role_counts": {"tool": 1},
            "source_hook_types": ["tool_result"],
            "source_hook_type_counts": {"tool_result": 1},
            "source_codex_events": ["PostToolUse"],
            "source_codex_event_counts": {"PostToolUse": 1},
            "source_memory_selection_policies": ["selected_tool_evidence_only"],
            "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
            "source_event_ids": [525254],
            "source_profile_promotion_policies": ["always_when_profile_scope_available"],
            "source_profile_memory_classes": ["memory_feature"],
            "source_profile_memory_kinds": ["memory_feature"],
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "extraction_phase": "provisional",
            "profile_memory_class": "memory_feature",
            "profile_memory_kind": "memory_feature",
            "profile_promotion_policy": "always_when_profile_scope_available",
        }

        class RuntimeRefreshAdapter:
            def __init__(self) -> None:
                self.records = [
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": scope,
                        "status": "pending",
                        "updated_at_ms": 1000,
                    },
                    event,
                    entity,
                ]

            def read_all(self) -> list[dict]:
                return list(self.records)

            def node_summary_source_records(self, **_kwargs):
                return [event], [], [entity], [], {"source_policy": "direct_events_and_profile_state"}

            def append_many(self, records: list[dict]) -> None:
                self.records.extend(records)

            def append(self, record: dict) -> None:
                self.records.append(record)

            def auto_time_compress_node_events(self, **_kwargs) -> dict:
                return {"created_count": 0}

            def context_event_ingestion_time_ms(self, record: dict, _debug_by_ref=None) -> int:
                return int(record.get("updated_at_ms") or record.get("event_time_ms") or 0)

        adapter = RuntimeRefreshAdapter()
        previous_audit_flag = matrixark_mcp_summary_runtime.ENABLE_SUMMARY_REFRESH_AUDIT
        matrixark_mcp_summary_runtime.ENABLE_SUMMARY_REFRESH_AUDIT = True
        try:
            refresh = matrixark_mcp_summary_runtime.refresh_dirty_node_summaries(
                adapter,
                scope=scope,
                limit=8,
                refreshed_at_ms=2000,
            )
        finally:
            matrixark_mcp_summary_runtime.ENABLE_SUMMARY_REFRESH_AUDIT = previous_audit_flag

        self.assertEqual("ok", refresh["status"])
        self.assertEqual(1, refresh["refreshed_count"])
        refreshed = refresh["refreshed"][0]
        self.assertEqual(["selected_tool_evidence_only"], refreshed["source_memory_selection_policies"])
        self.assertEqual({"selected_tool_evidence_only": 2}, refreshed["source_memory_selection_policy_counts"])
        self.assertEqual(["always_when_profile_scope_available"], refreshed["source_profile_promotion_policies"])
        self.assertEqual(["memory_feature"], refreshed["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], refreshed["source_profile_memory_kinds"])
        self.assertEqual({"tool": 2}, refreshed["source_role_counts"])
        self.assertEqual({"tool_result": 2}, refreshed["source_hook_type_counts"])
        self.assertEqual({"PostToolUse": 2}, refreshed["source_codex_event_counts"])

        audit = next(
            record
            for record in adapter.records
            if record.get("record_type") == "context_summary_refresh_audit"
        )
        self.assertEqual({"selected_tool_evidence_only": 2}, audit["source_memory_selection_policy_counts"])
        self.assertEqual(["always_when_profile_scope_available"], audit["source_profile_promotion_policies"])
        self.assertEqual(["memory_feature"], audit["source_profile_memory_classes"])
        self.assertEqual(["memory_feature"], audit["source_profile_memory_kinds"])
        self.assertEqual({"tool": 2}, audit["source_role_counts"])
        self.assertEqual({"tool_result": 2}, audit["source_hook_type_counts"])
        self.assertEqual({"PostToolUse": 2}, audit["source_codex_event_counts"])
        summaries = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_summary"
            and record.get("node_hash") == node_hash
        ]
        self.assertTrue(summaries)
        for summary in summaries:
            self.assertEqual(["always_when_profile_scope_available"], summary["source_profile_promotion_policies"])
            self.assertEqual(["memory_feature"], summary["source_profile_memory_classes"])
            self.assertEqual(["memory_feature"], summary["source_profile_memory_kinds"])
            self.assertEqual("memory_feature", summary["profile_memory_class"])
            self.assertEqual("memory_feature", summary["profile_memory_kind"])
            self.assertEqual("always_when_profile_scope_available", summary["profile_promotion_policy"])
        summary_index_names = {
            str(record.get("index_name") or "")
            for record in adapter.records
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_summary"
            and record.get("node_hash") == node_hash
        }
        self.assertIn("profile_memory_class:memory_feature", summary_index_names)
        self.assertIn("profile_memory_kind:memory_feature", summary_index_names)
        self.assertIn("profile_promotion_policy:always_when_profile_scope_available", summary_index_names)

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
            "profile_promotion_policy",
            "profile_promotion_blocker",
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

    def test_context_pack_debug_normalizes_codex_tool_role_aliases(self) -> None:
        ref = {
            "ref_type": "entity",
            "entity_type": "tool_evidence",
            "text": "tool evidence: Validation: 17 tests passed",
            "source_roles": ["function_call_output", "custom_tool_call_output", "tool-output"],
            "source_role_counts": {
                "function_call_output": 1,
                "custom_tool_call_output": 2,
                "tool-output": 1,
            },
            "budget_source_roles": ["tool_call_output"],
            "budget_source_role_counts": {"tool_call_output": 4},
        }

        default_ref = compact_context_pack_ref(ref)
        self.assertNotIn("source_roles", default_ref)
        self.assertNotIn("source_role_counts", default_ref)

        debug_ref = compact_context_pack_ref(ref, include_debug=True)
        self.assertEqual(["tool"], debug_ref["source_roles"])
        self.assertEqual({"tool": 4}, debug_ref["source_role_counts"])
        self.assertEqual(["tool"], debug_ref["budget_source_roles"])
        self.assertEqual({"tool": 4}, debug_ref["budget_source_role_counts"])

    def test_hook_layer_summary_surfaces_memory_selection_policy_budget(self) -> None:
        pack = {
            "context_pack_id": "pack-selection-budget",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "entity_type": "assistant_decision",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "text": "assistant_decision: use threshold and idle extraction",
                }
            ],
            "retrieval_metrics": {
                "memory_layer_budget": {
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 8}},
                    "by_memory_selection_policy": {
                        "selected_assistant_decision_outcome_only": {"refs": 1, "tokens": 8}
                    },
                }
            },
            "recall_policy": {
                "memory_selection_policy_budget_policy": {
                    "enabled": True,
                    "mode": "auto",
                    "remote_budget_tokens": 100,
                    "budget_tokens": {
                        "selected_user_prompt": 40,
                        "selected_assistant_decision_outcome_only": 45,
                        "selected_tool_evidence_only": 30,
                    },
                    "selected_tokens_by_policy": {"selected_assistant_decision_outcome_only": 8},
                    "selected_ref_count_by_policy": {"selected_assistant_decision_outcome_only": 1},
                }
            },
        }

        summary = matrixark_codex_hook.retrieval_layer_summary_from_retrieve(pack)
        selection_budget = summary["memory_selection_policy_budget"]
        self.assertTrue(selection_budget["enabled"])
        self.assertEqual("auto", selection_budget["mode"])
        self.assertEqual(100, selection_budget["remote_budget_tokens"])
        self.assertEqual(45, selection_budget["budget_tokens"]["selected_assistant_decision_outcome_only"])
        self.assertEqual(1, selection_budget["selected_ref_count_by_policy"]["selected_assistant_decision_outcome_only"])
        self.assertNotIn("by_memory_selection_policy", summary["memory_layer_budget"])
        rendered = matrixark_codex_hook._format_retrieval_layer_summary(summary)
        self.assertIn("memory_selection_policy_budget", rendered)
        self.assertIn("mode=auto", rendered)
        self.assertIn("selected_assistant_decision_outcome_only=45", rendered)

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
                "quality_first_underfill": {
                    "enabled": True,
                    "unused_remote_context_tokens": 56,
                    "dropped_ref_count": 3,
                    "dropped_reason_counts": {
                        "memory_layer_budget": 2,
                        "source_role_budget": 1,
                    },
                    "warning": "quality_first_budget_underfill:unused_tokens=56,dropped_refs=3",
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
        self.assertEqual(
            {
                "enabled": True,
                "unused_remote_context_tokens": 56,
                "dropped_ref_count": 3,
                "dropped_reason_counts": {
                    "memory_layer_budget": 2,
                    "source_role_budget": 1,
                },
            },
            metrics_serving["retrieval_metrics"]["quality_first_underfill"],
        )
        self.assertNotIn("warning", metrics_serving["retrieval_metrics"]["quality_first_underfill"])

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

    def test_quality_first_underfill_summary_tracks_budget_dropped_memory(self) -> None:
        summary = quality_first_underfill_summary(
            budget_fill_policy="quality_first",
            selected_ref_count=1,
            used_context_tokens=24,
            remote_context_budget_tokens=80,
            dropped_over_budget={
                "memory_layer_budget": 2,
                "cross_session_budget": 1,
                "deadline_exceeded": True,
            },
        )

        self.assertTrue(summary["enabled"])
        self.assertEqual(56, summary["unused_remote_context_tokens"])
        self.assertEqual(3, summary["dropped_ref_count"])
        self.assertEqual({"cross_session_budget": 1, "memory_layer_budget": 2}, summary["dropped_reason_counts"])
        self.assertEqual("quality_first_budget_underfill:unused_tokens=56,dropped_refs=3", summary["warning"])
        self.assertFalse(
            quality_first_underfill_summary(
                budget_fill_policy="force_fill",
                selected_ref_count=1,
                used_context_tokens=24,
                remote_context_budget_tokens=80,
                dropped_over_budget={"memory_layer_budget": 2},
            )["enabled"]
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
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "source_role_counts": {"model": 3},
                "source_memory_selection_policies": ["selected_tool_evidence_only"],
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
                "source_role": "tool_result",
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

    def test_compression_index_records_include_memory_selection_and_promotion_lineage(self) -> None:
        records = compression_context_index_records(
            {
                "record_type": "context_compression_event",
                "compression_id_hash": 903,
                "node_hash": 904,
                "summary_text": "compressed assistant and tool evidence",
                "source_roles": ["assistant", "tool"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["Stop"],
                "source_memory_selection_policies": [
                    "selected_assistant_decision_outcome_only",
                    "selected_tool_evidence_only",
                ],
                "source_memory_scopes": ["session", "user_profile"],
                "source_session_continuities": ["same_session", "cross_session"],
                "source_extraction_phases": ["final"],
                "source_profile_promotion_policies": ["always_when_profile_scope_available"],
                "source_profile_promotion_blockers": ["profile_scope_missing"],
                "source_final_session_boundary_count": 1,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "scope": {
                    "account_id": "acct_compression_index",
                    "tenant_id": "tenant_compression_index",
                    "user_id": "user_compression_index",
                },
                "compressed_time_ms": 1780000000000,
            }
        )

        index_names = {record.get("index_name") for record in records}
        self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", index_names)
        self.assertIn("memory_selection_policy:selected_tool_evidence_only", index_names)
        self.assertIn("profile_promotion_policy:always_when_profile_scope_available", index_names)
        self.assertIn("profile_promotion_blocker:profile_scope_missing", index_names)
        self.assertIn("final_session_boundary:true", index_names)
        self.assertTrue(all(record.get("data_model") == "context_compression_event" for record in records))
        self.assertTrue(all(record.get("ref_type") == "compression" for record in records))

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
                        "source_role": "assistant_response",
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 1},
                        "source_codex_events": ["Stop"],
                        "source_codex_event_counts": {"Stop": 1},
                        "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                        "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
                        "source_memory_selection_complete_count": 1,
                        "source_profile_promotion_policies": ["always_when_profile_scope_available"],
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
                        "source_role": "tool_result",
                        "source_hook_types": ["hook_boundary"],
                        "source_hook_type_counts": {"hook_boundary": 1},
                        "source_codex_events": ["PostToolUse"],
                        "source_codex_event_counts": {"PostToolUse": 1},
                        "source_memory_selection_policies": ["selected_tool_evidence_only"],
                        "source_memory_selection_policy_counts": {"selected_tool_evidence_only": 1},
                        "source_memory_selection_complete_count": 1,
                        "source_profile_promotion_policies": ["always_when_profile_scope_available"],
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
            self.assertEqual(
                ["selected_assistant_decision_outcome_only", "selected_tool_evidence_only"],
                compression["source_memory_selection_policies"],
            )
            self.assertEqual(
                {"selected_assistant_decision_outcome_only": 1, "selected_tool_evidence_only": 1},
                compression["source_memory_selection_policy_counts"],
            )
            self.assertEqual(2, compression["source_memory_selection_complete_count"])
            self.assertEqual(["always_when_profile_scope_available"], compression["source_profile_promotion_policies"])
            self.assertEqual(["session"], compression["source_memory_scopes"])
            self.assertEqual(["same_session"], compression["source_session_continuities"])
            self.assertEqual(["final"], compression["source_extraction_phases"])
            self.assertEqual(2, compression["source_final_session_boundary_count"])
            self.assertEqual("session", compression["memory_scope"])
            self.assertEqual("same_session", compression["session_continuity"])
            self.assertEqual("final", compression["extraction_phase"])
            self.assertTrue(compression["final_session_boundary"])
            compression_embeddings = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "compression_summary"
                and record.get("ref_hash") == compression["compression_id_hash"]
            ]
            self.assertEqual(1, len(compression_embeddings))
            compression_embedding = compression_embeddings[0]
            self.assertEqual("session", compression_embedding["memory_scope"])
            self.assertEqual("same_session", compression_embedding["session_continuity"])
            self.assertEqual(["assistant", "tool"], compression_embedding["source_roles"])
            self.assertEqual({"assistant": 1, "tool": 1}, compression_embedding["source_role_counts"])
            self.assertEqual(["hook_boundary"], compression_embedding["source_hook_types"])
            self.assertEqual({"hook_boundary": 2}, compression_embedding["source_hook_type_counts"])
            self.assertEqual(["PostToolUse", "Stop"], compression_embedding["source_codex_events"])
            self.assertEqual({"Stop": 1, "PostToolUse": 1}, compression_embedding["source_codex_event_counts"])
            self.assertEqual(["selected_assistant_decision_outcome_only", "selected_tool_evidence_only"], compression_embedding["source_memory_selection_policies"])
            self.assertEqual({"selected_assistant_decision_outcome_only": 1, "selected_tool_evidence_only": 1}, compression_embedding["source_memory_selection_policy_counts"])
            self.assertEqual(["session"], compression_embedding["source_memory_scopes"])
            self.assertEqual(["same_session"], compression_embedding["source_session_continuities"])
            self.assertEqual(["final"], compression_embedding["source_extraction_phases"])
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
            self.assertIn("source_type:message", compression_index_names)
            self.assertIn("keyword:compressed", compression_index_names)
            self.assertIn("keyword:evidence", compression_index_names)
            self.assertIn("source_role:assistant", compression_index_names)
            self.assertIn("source_role:tool", compression_index_names)
            self.assertIn("hook_type:hook_boundary", compression_index_names)
            self.assertIn("codex_event:Stop", compression_index_names)
            self.assertIn("codex_event:PostToolUse", compression_index_names)
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", compression_index_names)
            self.assertIn("memory_selection_policy:selected_tool_evidence_only", compression_index_names)
            self.assertIn("profile_promotion_policy:always_when_profile_scope_available", compression_index_names)
            self.assertIn("final_session_boundary:true", compression_index_names)
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
            self.assertIn("memory_selection_policy:selected_assistant_decision_outcome_only", compression_terms)
            self.assertIn("memory_selection_policy:selected_tool_evidence_only", compression_terms)
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
            self.assertEqual(1, budget["by_memory_layer"]["same_session_compression"]["refs"])
            self.assertEqual(17, budget["by_memory_layer"]["same_session_compression"]["tokens"])
            self.assertEqual(1, budget["by_memory_scope"]["session"]["refs"])
            self.assertEqual(1, budget["by_session_continuity"]["same_session"]["refs"])
            self.assertEqual(1, budget["by_extraction_phase"]["final"]["refs"])
            self.assertEqual(1, budget["by_source_role"]["assistant"]["refs"])
            self.assertEqual(1, budget["by_source_role"]["tool"]["refs"])
            self.assertEqual(1, budget["by_hook_type"]["hook_boundary"]["refs"])
            self.assertEqual(1, budget["by_codex_event"]["Stop"]["refs"])
            self.assertEqual(1, budget["by_codex_event"]["PostToolUse"]["refs"])
            self.assertEqual(1, budget["by_memory_selection_policy"]["selected_assistant_decision_outcome_only"]["refs"])
            self.assertEqual(1, budget["by_memory_selection_policy"]["selected_tool_evidence_only"]["refs"])
            self.assertEqual({"assistant": 1, "tool": 1}, budget["source_message_counts_by_role"])
            self.assertEqual({"hook_boundary": 2}, budget["source_hook_counts_by_type"])
            self.assertEqual({"Stop": 1, "PostToolUse": 1}, budget["source_codex_event_counts_by_event"])
            self.assertEqual(1, budget["final_session_boundary_ref_count"])
            self.assertEqual(1, budget["final_ref_count"])

            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Compressed assistant decision and tool validation evidence",
                    "max_context_tokens": 120,
                    "ranking": {
                        "max_selected_refs": 3,
                        "min_similarity_score": 0.0,
                        "memory_layer_budget_tokens": {"same_session_event": 1},
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )
            selected_compression = [
                ref for ref in pack["selected_refs"] if ref.get("ref_type") == "compression"
            ]
            self.assertTrue(selected_compression, pack)
            self.assertEqual(
                ["selected_assistant_decision_outcome_only", "selected_tool_evidence_only"],
                selected_compression[0]["source_memory_selection_policies"],
            )
            self.assertEqual(
                {"selected_assistant_decision_outcome_only": 1, "selected_tool_evidence_only": 1},
                selected_compression[0]["source_memory_selection_policy_counts"],
            )
            retrieved_budget = pack["recall_policy"]["memory_layer_budget"]
            self.assertIn("same_session_compression", retrieved_budget["by_memory_layer"])
            self.assertIn("selected_tool_evidence_only", retrieved_budget["by_memory_selection_policy"])

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

