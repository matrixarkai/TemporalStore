"""_BackendPolicyPart4 methods split from test_matrixark_mcp_backend_policy.MatrixArkMcpBackendPolicyTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_mcp_backend_policy import (
    Path,
    _BadNativeContextPackClient,
    _EmptyNativeEnvelopeContextPackClient,
    _NativeCandidateScanClient,
    _NativeContextPackClient,
    _NativeEnvelopeContextPackClient,
    _direct_adapter_for_readiness,
    json,
    mcp,
    mcp_budget_pack,
    mcp_context_pack,
    mcp_core,
    mcp_local,
    os,
    threading,
)
except ImportError:
    from test_matrixark_mcp_backend_policy import (
    Path,
    _BadNativeContextPackClient,
    _EmptyNativeEnvelopeContextPackClient,
    _NativeCandidateScanClient,
    _NativeContextPackClient,
    _NativeEnvelopeContextPackClient,
    _direct_adapter_for_readiness,
    json,
    mcp,
    mcp_budget_pack,
    mcp_context_pack,
    mcp_core,
    mcp_local,
    os,
    threading,
)


class _BackendPolicyPart4:
    def test_budget_packer_enforces_source_role_caps(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 8201,
                    "text": "assistant decision",
                    "score": 0.95,
                    "entity_type": "assistant_decision",
                    "source_roles": ["llm", "model"],
                    "source_role_counts": {"llm": 1, "model": 1},
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8202,
                    "text": "another assistant decision",
                    "score": 0.94,
                    "entity_type": "assistant_decision",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8203,
                    "text": "tool evidence",
                    "score": 0.93,
                    "entity_type": "tool_evidence",
                    "source_roles": ["function_call_output", "custom_tool_call_output"],
                    "source_role_counts": {"function_call_output": 1, "custom_tool_call_output": 1},
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={"enabled": True, "budget_tokens": 40, "max_sessions": 4, "max_candidates": 4},
            source_role_budget_tokens={"assistant": 2, "tool": 4},
        )

        self.assertEqual([8201, 8203], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertEqual(3, dropped["estimated_tokens"]["source_role_budget"])
        self.assertEqual({"assistant": 2, "tool": 4}, dropped["source_role_budget_policy"]["budget_tokens"])
        self.assertEqual(1, dropped["source_role_budget_policy"]["selected_ref_count_by_role"]["assistant"])
        self.assertEqual(1, dropped["source_role_budget_policy"]["selected_ref_count_by_role"]["tool"])

    def test_current_state_prioritizes_assistant_decision_and_tool_evidence(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 8301,
                    "text": "generic profile preference with a higher base score",
                    "score": 0.86,
                    "entity_type": "preference",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8302,
                    "text": "assistant decision: keep the live memory extraction path",
                    "score": 0.72,
                    "entity_type": "assistant_decision",
                    "source_roles": ["model"],
                    "source_role_counts": {"model": 1},
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8303,
                    "text": "tool evidence: Exit code: 0; Ran 78 tests OK",
                    "score": 0.70,
                    "entity_type": "tool_evidence",
                    "source_roles": ["tool"],
                    "source_role_counts": {"tool": 1},
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                },
            ],
            [],
            question_type="current_state",
            max_context_tokens=24,
            max_selected_refs=2,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={"enabled": True, "budget_tokens": 24, "max_sessions": 4, "max_candidates": 4},
        )

        self.assertEqual([8302, 8303], [ref["ref_hash"] for ref in selected])
        self.assertNotIn(8301, [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)

    def test_codex_session_identity_policy_treats_exact_payload_sources_as_strong(self) -> None:
        payload_policy = mcp_local.codex_session_identity_policy("payload.conversation_id")
        self.assertTrue(payload_policy["strong_session_identity"])
        self.assertFalse(payload_policy["fallback_session_identity"])
        self.assertEqual("", payload_policy["risk"])

        env_policy = mcp_local.codex_session_identity_policy("env.CODEX_THREAD_ID")
        self.assertTrue(env_policy["strong_session_identity"])
        self.assertFalse(env_policy["fallback_session_identity"])

        workspace_policy = mcp_local.codex_session_identity_policy("workspace_hash")
        self.assertFalse(workspace_policy["strong_session_identity"])
        self.assertTrue(workspace_policy["fallback_session_identity"])
        self.assertEqual("workspace_fallback_may_merge_multiple_codex_tasks", workspace_policy["risk"])

    def test_service_backpressure_fallback_does_not_read_all_for_native_backend(self) -> None:
        class _NoReadAllNativeAdapter:
            def __init__(self) -> None:
                self.records_seen = None

            def _backend_label(self) -> str:
                return "temporalstore-direct"

            def read_all(self):
                raise AssertionError("native timeout fallback must not read all records")

            def deadline_fallback_pack(self, **kwargs):
                self.records_seen = len(kwargs["records"])
                return {
                    "context_pack_id": "timeout-partial",
                    "quality_warnings": ["timeout_partial"],
                    "records_seen": self.records_seen,
                }

        adapter = _NoReadAllNativeAdapter()
        server = mcp.MatrixArkMcpServer(adapter)
        old_limit = os.environ.get("MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT")
        os.environ["MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT"] = "5"
        try:
            pack = server._retrieve_timeout_fallback(
                {"query": "What did Alice approve?", "scope": {"account_id": "acct"}},
                deadline_ms=10,
                elapsed_ms=12.3,
                reason="service_backpressure",
            )
        finally:
            if old_limit is None:
                os.environ.pop("MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT", None)
            else:
                os.environ["MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT"] = old_limit

        self.assertEqual(pack["records_seen"], 0)
        self.assertEqual(adapter.records_seen, 0)

    def test_production_retrieve_dispatches_native_pack_before_query_embedding(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        pack = adapter.retrieve({
            "query": "What did Alice approve?",
            "scope": {"account_id": "acct"},
            "max_context_tokens": 1000,
            "audit_mode": "off",
        })

        self.assertEqual(pack["pack_id"], "native-pack-1")
        self.assertNotIn("context_pack_id", pack)
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("query_embedding", client.calls[0]["request"])
        self.assertEqual(client.calls[0]["request"]["query"], "What did Alice approve?")
        cross_session = client.calls[0]["request"]["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(cross_session["budget_ratio"], 0.12)
        self.assertEqual(cross_session["budget_tokens"], 114)
        self.assertEqual(cross_session["max_sessions"], 3)
        self.assertEqual(cross_session["max_candidates"], 24)
        self.assertEqual(cross_session["min_score"], 0.2)
        self.assertEqual(cross_session["decision"], "always_consider_same_user_cross_session_when_session_scope_prefer")
        self.assertEqual(cross_session["strategy"], "same_session_first_entity_bridge_then_bounded_cross_session")

    def test_production_retrieve_gives_profile_memory_full_cross_session_budget(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        adapter.retrieve({
            "query": "What do you remember about me across previous sessions?",
            "scope": {"account_id": "acct", "tenant_id": "tenant", "user_id": "user", "session_id": "s1"},
            "max_context_tokens": 1000,
            "ranking": {
                "source_role_budget_mode": "auto",
                "memory_layer_budget_mode": "auto",
                "memory_selection_policy_budget_mode": "auto",
            },
            "audit_mode": "off",
        })

        request = client.calls[0]["request"]
        self.assertEqual("profile_memory", request["question_type"])
        cross_session = request["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(cross_session["budget_ratio"], 0.2)
        self.assertEqual(cross_session["budget_tokens"], 190)
        self.assertTrue(cross_session["question_budget_reason"].startswith("profile_memory_queries_need_long_term"))
        self.assertEqual(["entity", "summary", "compression"], cross_session["preferred_ref_types"])
        self.assertEqual("auto", request["source_role_budget_mode"])
        self.assertEqual({"assistant": 475, "tool": 427, "user": 475}, request["source_role_budget_tokens"])
        self.assertEqual("auto", request["memory_layer_budget_mode"])
        self.assertEqual(570, request["memory_layer_budget_tokens"]["profile_entity"])
        self.assertEqual(427, request["memory_layer_budget_tokens"]["profile_summary"])
        self.assertEqual(380, request["memory_layer_budget_tokens"]["cross_session_event"])
        self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
        self.assertEqual(
            {
                "selected_user_prompt": 332,
                "selected_assistant_decision_outcome_only": 380,
                "selected_tool_evidence_only": 285,
                "selected_profile_current_state": 617,
            },
            request["memory_selection_policy_budget_tokens"],
        )

    def test_production_retrieve_routes_multi_session_current_queries_to_multi_hop_budget(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        adapter.retrieve({
            "query": "Compare the latest Codex decisions across sessions",
            "scope": {"account_id": "acct", "tenant_id": "tenant", "user_id": "user", "session_id": "s1"},
            "max_context_tokens": 1000,
            "ranking": {
                "source_role_budget_mode": "auto",
                "memory_layer_budget_mode": "auto",
                "memory_selection_policy_budget_mode": "auto",
            },
            "audit_mode": "off",
        })

        request = client.calls[0]["request"]
        self.assertEqual("multi_hop", request["question_type"])
        cross_session = request["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(0.2, cross_session["budget_ratio"])
        self.assertEqual(190, cross_session["budget_tokens"])
        self.assertIn("cross-session memory for comparisons", cross_session["question_budget_reason"])
        self.assertEqual("auto", request["memory_layer_budget_mode"])
        self.assertEqual(332, request["memory_layer_budget_tokens"]["cross_session_event"])
        self.assertEqual(332, request["memory_layer_budget_tokens"]["cross_session_segment"])
        self.assertEqual(427, request["memory_layer_budget_tokens"]["profile_entity"])

    def test_direct_native_context_pack_accepts_explicit_cross_session_budget(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        adapter.retrieve({
            "query": "What is my current storage preference?",
            "scope": {"account_id": "acct", "session_id": "s1"},
            "question_type": "current_state",
            "max_context_tokens": 2000,
            "cross_session": {"budget_tokens": 777, "max_sessions": 3, "parallelism": 2},
            "audit_mode": "off",
        })

        cross_session = client.calls[0]["request"]["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(cross_session["budget_tokens"], 380)
        self.assertEqual(cross_session["max_budget_ratio"], 0.2)
        self.assertEqual(cross_session["raw_evidence_min_score"], 0.45)
        self.assertEqual(cross_session["max_sessions"], 3)
        self.assertEqual(cross_session["parallelism"], 2)

    def test_direct_native_context_pack_accepts_explicit_extraction_phase_budget(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        adapter.retrieve({
            "query": "show final and provisional Codex memory",
            "scope": {"account_id": "acct", "tenant_id": "tenant", "user_id": "user", "session_id": "s1"},
            "max_context_tokens": 1000,
            "extraction_phase_budget_tokens": {"pending_async": 16, "provisional": 32, "final": 128},
            "audit_mode": "off",
        })

        request = client.calls[0]["request"]
        self.assertEqual(
            {"pending_async": 16, "provisional": 32, "final": 128},
            request["extraction_phase_budget_tokens"],
        )
        self.assertEqual("explicit", request["extraction_phase_budget_mode"])
        self.assertIn("extraction_phase_budget", request["required_output"]["drop_counters"])

    def test_direct_native_empty_context_pack_is_compacted_by_default(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _EmptyNativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        pack = adapter.retrieve({
            "query": "What memory is available?",
            "scope": {"account_id": "acct"},
            "max_context_tokens": 1000,
            "audit_mode": "off",
            "include_retrieval_metrics": True,
        })

        self.assertEqual("native-empty-pack", pack["pack_id"])
        self.assertNotIn("context_pack_id", pack)
        self.assertNotIn("recall_policy", pack)
        self.assertNotIn("memory_layer_budget", pack)
        self.assertIn("retrieval_metrics", pack)
        serialized = json.dumps(pack, sort_keys=True)
        for field in [
            "by_source_role",
            "source_message_counts_by_role",
            "pending_source_roles",
            "pending_memory_scopes",
        ]:
            self.assertNotIn(field, serialized)

    def test_tiny_remote_budget_reports_profile_floor_unavailable(self) -> None:
        policy = mcp_core.build_cross_session_policy(
            {},
            {},
            question_type="current_state",
            session_scope="prefer",
            remote_budget_tokens=90,
        )
        self.assertTrue(policy["enabled"])
        self.assertEqual(90, policy["remote_budget_tokens"])
        self.assertEqual(18, policy["computed_budget_tokens"])
        self.assertEqual(18, policy["budget_tokens"])
        self.assertEqual(256, policy["budget_floor_tokens"])
        self.assertFalse(policy["budget_floor_applied"])
        self.assertEqual("remote_budget_too_small_for_profile_floor", policy["budget_floor_status"])

        floored = mcp_core.build_cross_session_policy(
            {},
            {},
            question_type="fact",
            session_scope="prefer",
            remote_budget_tokens=1280,
        )
        self.assertEqual(153, floored["computed_budget_tokens"])
        self.assertEqual(256, floored["budget_tokens"])
        self.assertTrue(floored["budget_floor_applied"])
        self.assertEqual("floor_applied", floored["budget_floor_status"])

    def test_cross_session_budget_defaults_by_query_type(self) -> None:
        expected = {
            "fact": (120, "normal_queries_keep_cross_session_small"),
            "specific_fact": (120, "normal_queries_keep_cross_session_small"),
            "broad_exploration": (150, "broad_or_evidence_queries_get_extra"),
            "evidence": (150, "broad_or_evidence_queries_get_extra"),
            "current_state": (200, "current_state_or_latest_queries_need_prior"),
            "latest": (200, "current_state_or_latest_queries_need_prior"),
            "profile_memory": (200, "profile_memory_queries_need_long_term"),
            "multi_hop": (200, "multi_hop_or_date_queries_need cross-session memory"),
            "date": (200, "multi_hop_or_date_queries_need cross-session memory"),
        }
        for question_type, (budget_tokens, reason_prefix) in expected.items():
            with self.subTest(question_type=question_type):
                policy = mcp_core.build_cross_session_policy(
                    {},
                    {},
                    question_type=question_type,
                    session_scope="prefer",
                    remote_budget_tokens=1000,
                )
                self.assertTrue(policy["enabled"])
                self.assertEqual(budget_tokens, policy["computed_budget_tokens"])
                self.assertEqual(budget_tokens, policy["budget_tokens"])
                self.assertEqual("remote_budget_too_small_for_profile_floor", policy["budget_floor_status"])
                self.assertTrue(policy["question_budget_reason"].startswith(reason_prefix))
                self.assertEqual(2, policy["min_entity_bridge_refs"])
                self.assertEqual(["entity", "summary", "compression"], policy["preferred_ref_types"])

        session_only = mcp_core.build_cross_session_policy(
            {},
            {},
            question_type="current_state",
            session_scope="only",
            remote_budget_tokens=1000,
        )
        self.assertFalse(session_only["enabled"])
        self.assertEqual(0, session_only["budget_tokens"])
        self.assertEqual(0, session_only["min_entity_bridge_refs"])
        self.assertEqual([], session_only["preferred_ref_types"])
        self.assertEqual("disabled", session_only["budget_floor_status"])

    def test_cross_session_budget_is_cap_and_raw_events_need_high_confidence(self) -> None:
        policy = mcp_core.build_cross_session_policy(
            {"cross_session": {"budget_tokens": 900, "raw_evidence_min_score": 0.45}},
            {},
            question_type="current_state",
            session_scope="prefer",
            remote_budget_tokens=2000,
        )
        self.assertEqual(policy["budget_tokens"], 400)
        self.assertEqual(policy["max_budget_ratio"], 0.2)

        selected, used_tokens, dropped = mcp_core.select_token_budgeted_refs(
            [
                {
                    "ref_type": "event",
                    "ref_hash": 1,
                    "session_continuity": "cross_session",
                    "score": 0.40,
                    "scope": {"session_id": "old"},
                    "text": "Old raw event says Alice may approve the GPU request later.",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 2,
                    "session_continuity": "cross_session",
                    "score": 0.24,
                    "scope": {"session_id": "old"},
                    "text": "Current entity state: Alice approved the GPU request.",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="current_state",
            min_score=0.2,
            cross_session_policy={
                **policy,
                "budget_tokens": 400,
                "raw_evidence_min_score": 0.45,
            },
        )

        self.assertEqual([item["ref_type"] for item in selected], ["entity"])
        self.assertGreater(used_tokens, 0)
        self.assertGreaterEqual(dropped["low_score"], 1)
        self.assertEqual(dropped["cross_session_policy"]["selected_ref_count"], 1)

    def test_user_profile_entities_match_same_user_across_sessions(self) -> None:
        record = {
            "record_type": "context_entity",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "access_scope": {
                "account_id": "acct_memory",
                "tenant_id": "tenant_memory",
                "user_id": "user_memory",
            },
        }

        self.assertTrue(
            mcp_core.access_scope_matches_before_scoring(
                record,
                {
                    "account_id": "acct_memory",
                    "tenant_id": "tenant_memory",
                    "user_id": "user_memory",
                    "session_id": "new_session",
                },
            )
        )
        self.assertFalse(
            mcp_core.access_scope_matches_before_scoring(
                record,
                {
                    "account_id": "acct_memory",
                    "tenant_id": "tenant_memory",
                    "user_id": "other_user",
                    "session_id": "new_session",
                },
            )
        )

    def test_current_state_prefers_profile_entity_over_stale_session_entity(self) -> None:
        selected, used_tokens, dropped = mcp_core.select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 11,
                    "entity_type": "preference",
                    "entity_name": "storage location",
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "score": 0.58,
                    "updated_at_ms": 100,
                    "text": "preference: storage location = old Windows folder path.",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 22,
                    "entity_type": "preference",
                    "entity_name": "storage location",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "score": 0.46,
                    "updated_at_ms": 200,
                    "source_session_ids": ["session_old", "session_new"],
                    "source_entity_hashes": [11, 12],
                    "source_roles": ["user", "assistant"],
                    "source_codex_events": ["UserPromptSubmit", "Stop"],
                    "text": "preference: storage location = use /opt/github-services in Ubuntu.",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="current_state",
            max_selected_refs=1,
            min_score=0.2,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 400,
                "max_sessions": 3,
                "max_candidates": 8,
                "min_score": 0.2,
                "raw_evidence_min_score": 0.45,
                "min_entity_bridge_refs": 1,
            },
        )

        self.assertEqual([item["ref_hash"] for item in selected], [22])
        self.assertEqual(selected[0]["memory_scope"], "user_profile")
        self.assertEqual(selected[0]["profile_current_state_boost"], 0.18)
        serving_ref = mcp_core.compact_context_pack_refs(selected)[0]
        modular_serving_ref = mcp_context_pack.compact_context_pack_refs(selected)[0]
        self.assertEqual(serving_ref["memory_scope"], "user_profile")
        self.assertEqual(serving_ref["session_continuity"], "cross_session")
        self.assertEqual(serving_ref["entity_type"], "preference")
        self.assertEqual(serving_ref["entity_name"], "storage location")
        self.assertEqual(serving_ref["source_session_ids"], ["session_old", "session_new"])
        self.assertNotIn("source_entity_count", serving_ref)
        self.assertNotIn("source_roles", serving_ref)
        self.assertNotIn("source_codex_events", serving_ref)
        self.assertEqual(modular_serving_ref["memory_scope"], serving_ref["memory_scope"])
        self.assertEqual(modular_serving_ref["source_session_ids"], serving_ref["source_session_ids"])
        self.assertNotIn("source_entity_count", modular_serving_ref)
        self.assertGreater(used_tokens, 0)
        self.assertEqual(dropped["cross_session_policy"]["entity_bridge_selected_ref_count"], 1)
        self.assertEqual(dropped["stale"], 1)
        self.assertEqual(dropped["max_selected_refs"], 0)
        self.assertTrue(
            any(ref.get("reason") == "stale" and ref.get("ref_hash") == 11 for ref in dropped.get("refs", []))
        )
        stale_ref = next(ref for ref in dropped.get("refs", []) if ref.get("reason") == "stale")
        self.assertEqual(stale_ref["profile_shadowed_by_ref_hash"], 22)
        self.assertEqual(stale_ref["profile_shadowed_reason"], "source_entity_lineage")
        self.assertEqual(stale_ref["memory_scope"], "session")
        self.assertEqual(stale_ref["session_continuity"], "same_session")

    def test_current_state_shadows_session_entity_by_profile_identity_without_lineage(self) -> None:
        selected, _used_tokens, dropped = mcp_core.select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 31,
                    "entity_type": "preference",
                    "entity_name": "repo location",
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "score": 0.7,
                    "updated_at_ms": 100,
                    "text": "preference: repo location = use a Windows Codex folder.",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 42,
                    "entity_type": "preference",
                    "entity_name": "repo location",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "score": 0.45,
                    "updated_at_ms": 300,
                    "source_entity_hashes": [99],
                    "text": "preference: repo location = use /opt/github-services in Ubuntu.",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="current_state",
            max_selected_refs=1,
            min_score=0.2,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 400,
                "max_sessions": 3,
                "max_candidates": 8,
                "min_score": 0.2,
                "raw_evidence_min_score": 0.45,
                "min_entity_bridge_refs": 1,
            },
        )

        self.assertEqual([item["ref_hash"] for item in selected], [42])
        self.assertEqual(dropped["stale"], 1)
        stale_ref = next(ref for ref in dropped.get("refs", []) if ref.get("reason") == "stale")
        self.assertEqual(stale_ref["ref_hash"], 31)
        self.assertEqual(stale_ref["profile_shadowed_by_ref_hash"], 42)
        self.assertEqual(stale_ref["profile_shadowed_reason"], "same_entity_identity")

    def test_shared_context_budget_caps_shared_resources_and_skills(self) -> None:
        capped_policy = mcp_core.build_shared_context_policy(
            {
                "shared_context": {
                    "resource_budget_tokens": 900,
                    "resource_max_budget_ratio": 0.2,
                    "skill_budget_tokens": 900,
                    "skill_max_budget_ratio": 0.05,
                }
            },
            {},
            remote_budget_tokens=1000,
        )
        self.assertEqual(capped_policy["resource_budget_tokens"], 200)
        self.assertEqual(capped_policy["skill_budget_tokens"], 50)
        self.assertEqual(capped_policy["resource_max_budget_ratio"], 0.2)
        self.assertEqual(capped_policy["skill_max_budget_ratio"], 0.05)

        shared_policy = mcp_core.build_shared_context_policy(
            {"shared_context": {"resource_budget_tokens": 8, "skill_budget_tokens": 8}},
            {},
            remote_budget_tokens=1000,
        )
        selected, used_tokens, dropped = mcp_core.select_token_budgeted_refs(
            [
                {
                    "ref_type": "resource_chunk",
                    "ref_hash": 1,
                    "score": 0.9,
                    "sharing_scope": "tenant_shared",
                    "text": "GPU approval policy",
                },
                {
                    "ref_type": "resource_chunk",
                    "ref_hash": 2,
                    "score": 0.88,
                    "sharing_scope": "tenant_shared",
                    "text": "finance review requires budget owner signoff",
                },
                {
                    "ref_type": "skill_section",
                    "ref_hash": 3,
                    "score": 0.87,
                    "sharing_scope": "tenant_shared",
                    "text": "Use replay debugger for selected refs",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="fact",
            min_score=0.2,
            shared_context_policy=shared_policy,
        )

        self.assertGreater(used_tokens, 0)
        self.assertEqual(sum(1 for item in selected if item["ref_type"] == "resource_chunk"), 1)
        self.assertEqual(sum(1 for item in selected if item["ref_type"] == "skill_section"), 1)
        self.assertGreaterEqual(dropped["shared_resource_budget"], 1)
        self.assertEqual(dropped["shared_context_policy"]["resource_selected_ref_count"], 1)
        self.assertEqual(dropped["shared_context_policy"]["skill_selected_ref_count"], 1)

    def test_direct_native_context_pack_dispatches_single_backend_request(self) -> None:
        client = _NativeEnvelopeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-rust"

        pack = adapter.native_context_pack({
            "query": "gpu approval",
            "query_embedding": [0.1, 0.2],
            "scope": {"account_id": "acct"},
            "secondary_index_groups": [["resource_type:pdf"]],
            "max_context_tokens": 1000,
        })

        self.assertIsNotNone(pack)
        assert pack is not None
        self.assertEqual(pack["context_pack_id"], "native-pack-1")
        self.assertEqual(pack["context_pack_assembly"], "native_backend")
        self.assertEqual(pack["backend"], "temporalstore-rust")
        contract = pack["recall_policy"]["native_response_contract"]
        self.assertFalse(contract["raw_records_returned_to_python"])
        self.assertEqual(contract["python_hot_path_records"], 0)
        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["count_key"], "matrixark:test:record_count")
        self.assertEqual(call["record_hash_key"], "matrixark:test:records")
        self.assertEqual(call["request"]["query"], "gpu approval")
        self.assertEqual(call["request"]["secondary_index_groups"], [["resource_type:pdf"]])

    def test_native_context_pack_rejects_raw_record_payloads(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _BadNativeContextPackClient()
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        with self.assertRaisesRegex(mcp.MatrixArkError, "must return a finished ContextPack"):
            adapter.native_context_pack({"query": "gpu", "scope": {}, "max_context_tokens": 1000})

    def test_direct_supports_native_context_pack_only_when_client_exposes_api(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeContextPackClient()
        self.assertTrue(adapter.supports_native_context_pack())
        adapter._client = _NativeCandidateScanClient()
        self.assertFalse(adapter.supports_native_context_pack())

    def test_direct_supports_native_candidate_prefilter_when_client_exposes_api(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeCandidateScanClient()
        self.assertTrue(adapter.supports_native_candidate_prefilter())
        adapter._client = _NativeEnvelopeContextPackClient()
        self.assertFalse(adapter.supports_native_candidate_prefilter())

    def test_raw_ingestion_visibility_only_required_for_dedicated_proxy_clients(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._publish_visibility_after_flush = False
        adapter._dedicated_proxy_clients_enabled = False
        adapter._dedicated_pack_lanes_enabled = False
        self.assertFalse(adapter._raw_ingestion_visibility_required_after_flush())

        adapter._publish_visibility_after_flush = True
        adapter._dedicated_proxy_clients_enabled = False
        adapter._dedicated_pack_lanes_enabled = True
        self.assertFalse(adapter._raw_ingestion_visibility_required_after_flush())

        adapter._dedicated_proxy_clients_enabled = True
        self.assertTrue(adapter._raw_ingestion_visibility_required_after_flush())

    def test_rust_visibility_publish_groups_raw_and_serving_partitions(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreRustAdapter.__new__(mcp.MatrixArkTemporalStoreRustAdapter)
        grouped = adapter._visibility_key_groups_by_partition(
            [
                "matrixark:mcp:codex:raw_ingestion:record_count",
                "matrixark:mcp:codex:raw_ingestion:records:000000",
                "matrixark:mcp:codex:record_count",
                "matrixark:mcp:codex:records:000000",
                "matrixark:mcp:codex:context_event_by_ingestion_time:context_node:7",
            ]
        )

        self.assertEqual(
            grouped,
            [
                [
                    "matrixark:mcp:codex:raw_ingestion:record_count",
                    "matrixark:mcp:codex:raw_ingestion:records:000000",
                ],
                [
                    "matrixark:mcp:codex:record_count",
                    "matrixark:mcp:codex:records:000000",
                    "matrixark:mcp:codex:context_event_by_ingestion_time:context_node:7",
                ],
            ],
        )

    def test_rust_proxy_context_pack_requests_top_level_response(self) -> None:
        client = mcp.MatrixArkRustProxyClient.__new__(mcp.MatrixArkRustProxyClient)
        calls = []

        def fake_call_json(op: str, **kwargs: object) -> dict[str, object]:
            calls.append((op, kwargs))
            return {"context_pack": {"context_pack_id": "pack-top-level"}}

        client._call_json = fake_call_json
        pack = client.matrixark_retrieve_context_pack(
            count_key="matrixark:test:record_count",
            record_hash_key="matrixark:test:records",
            shard_size=128,
            request={"query": "gpu", "scope": {}},
        )

        self.assertEqual(pack["context_pack"]["context_pack_id"], "pack-top-level")
        self.assertEqual(calls[0][0], "matrixark_retrieve_context_pack")
        self.assertTrue(calls[0][1]["top_level_response"])

    def test_production_retrieve_fails_closed_without_native_context_pack(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeCandidateScanClient()
        adapter._storage_prefix = "matrixark:test:pack-required"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 256
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._retrieval_records_cache_lock = threading.RLock()
        adapter._retrieval_records_cache = {}
        adapter.native_context_pack_required = lambda: True
        with self.assertRaisesRegex(mcp.MatrixArkError, "backend-native ContextPack assembly"):
            adapter.retrieve({
                "query": "What did Alice approve?",
                "scope": {"account_id": "acct"},
                "max_context_tokens": 1000,
            })

    def test_rust_matrixark_append_uses_native_proxy_path(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy.rs").read_text()
        implementation = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy_impl.rs").read_text()
        crate_implementation = (repo / "crates/temporalstore-rust/src/bin/matrixark_rust_proxy_impl.rs").read_text()

        self.assertIn("Production-facing alias for the MatrixArk Rust proxy", source)
        self.assertIn('"batch_hset" =>', implementation)
        self.assertIn('"matrixark_append_records" | "matrixark_batch_append_records" =>', implementation)
        self.assertNotIn('"batch_hset" | "matrixark_append_records"', implementation)
        self.assertIn("client\n                .matrixark_batch_append_records", implementation)
        self.assertIn('"native_append": true', implementation)
        self.assertIn("entries_compact", implementation)
        self.assertIn("expanded_hash_entries", implementation)
        self.assertNotIn("put_string(&serving_count_key", implementation)
        self.assertNotIn("value_contains_serving_context_record", implementation)

        self.assertIn('"scan_hash" =>', implementation)
        self.assertIn("client.hgetall", implementation)

        execute_start = crate_implementation.index("fn execute_record_log_request")
        crate_start = crate_implementation.index(
            '"matrixark_append_records" | "matrixark_batch_append_records" =>',
            execute_start,
        )
        crate_end = crate_implementation.index('"batch_hget" =>', crate_start)
        crate_append_body = crate_implementation[crate_start:crate_end]
        self.assertIn("execute_empty_batch_runtime", crate_append_body)
        self.assertIn("BatchExecuteRequest", crate_implementation)
        self.assertIn("rust_proxy_matrixark_batch_runtime_default", crate_append_body)
        self.assertIn("matrixark_batch_uses_forced_sync_durable_writes", crate_append_body)
        self.assertNotIn("execute_empty(&engine", crate_append_body)

    def test_rust_direct_sdk_bridge_has_production_binary(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cargo = (repo / "sdk/rust/temporalstore/Cargo.toml").read_text()
        direct_source = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_direct_sdk.rs").read_text()
        implementation = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy_impl.rs").read_text()
        backends = (repo / "tools/matrixark_mcp_backends.py").read_text()
        adapters = (repo / "tools/matrixark_mcp_temporal_adapters.py").read_text()

        self.assertIn('name = "matrixark_rust_direct_sdk"', cargo)
        self.assertIn("Production-facing alias for the MatrixArk Rust direct SDK bridge", direct_source)
        self.assertIn("matrixark_rust_sdk_mode_is_direct", implementation)
        self.assertIn("matrixark_rust_direct_sdk", implementation)
        self.assertIn("rust-direct-sdk-bridge", implementation)
        self.assertIn("rust-direct-sdk-bridge", direct_source)
        self.assertIn("MatrixArkTemporalStoreRustDirectAdapter", adapters)
        self.assertIn("rust-direct-sdk-bridge", adapters)

    def test_rust_cdylib_direct_binding_exposes_native_matrixark_api(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cargo = (repo / "sdk/rust/temporalstore/Cargo.toml").read_text()
        rust_lib = (repo / "sdk/rust/temporalstore/src/lib.rs").read_text()
        adapter = (repo / "tools/matrixark_mcp_temporal_adapters.py").read_text()
        server = (repo / "tools/matrixark_mcp_server.py").read_text()

        self.assertIn('crate-type = ["rlib", "cdylib"]', cargo)
        for symbol in [
            "temporalstore_rust_connect_json",
            "temporalstore_rust_close",
            "temporalstore_rust_hset",
            "temporalstore_rust_hget",
            "temporalstore_rust_hgetall_json",
            "temporalstore_rust_matrixark_batch_append_records_json",
            "temporalstore_rust_matrixark_scan_candidates_json",
            "temporalstore_rust_matrixark_retrieve_context_pack_json",
        ]:
            self.assertIn(symbol, rust_lib)
        self.assertIn("class MatrixArkRustCdylibClient", adapter)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", adapter)
        self.assertIn("rust_direct_cdylib_matrixark_batch_append_records", adapter)
        self.assertIn("rust_direct_cdylib_matrixark_retrieve_context_pack", adapter)
        self.assertIn("--rust-direct-lib", server)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", server)

    def test_cpp_python_sdk_exposes_native_hash_scan(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "sdk/python/temporalstore/client.py").read_text()

        self.assertIn("has_hgetall", source)
        self.assertIn("def hgetall", source)
        self.assertIn("def scan_hash", source)

    def test_proxy_contract_exposes_matrixark_native_hot_path(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        openapi = (repo / "sdk/proxy/openapi.yaml").read_text()
        proxy_client = (repo / "sdk/python/temporalstore/proxy_client.py").read_text()
        adapter = (repo / "tools/matrixark_mcp_temporal_adapters.py").read_text()

        self.assertIn("/v1/matrixark/append_records", openapi)
        self.assertIn("/v1/matrixark/scan_candidates", openapi)
        self.assertIn("/v1/matrixark/retrieve_context_pack", openapi)
        self.assertIn("MatrixArkScanCandidatesRequest", openapi)
        self.assertIn("MatrixArkRetrieveContextPackRequest", openapi)
        self.assertIn("def matrixark_scan_candidates", proxy_client)
        self.assertIn("def matrixark_retrieve_context_pack", proxy_client)
        self.assertIn("MATRIXARK_TEMPORALSTORE_CPP_PROXY_ENDPOINT", adapter)
        self.assertIn("cpp_proxy_matrixark_batch_append_records", adapter)

    def test_direct_readiness_reports_metaserver_failure(self) -> None:
        adapter = _direct_adapter_for_readiness(metaserver="127.0.0.1:1")
        result = adapter._run_backend_readiness_gate(reason="unit-metaserver-down", timeout_ms=1)

        self.assertEqual(result["status"], "topology_not_ready")
        self.assertIn("metaserver unreachable", result["error"])
        self.assertFalse(result["checks"]["metaserver_reachable"]["ok"])
        self.assertFalse(result["checks"]["namespace_table_opened"])
        self.assertFalse(result["checks"]["slot_coverage_verified_by_warmup_hset_hget"])
        self.assertTrue(result["attempt_log"])

    def test_direct_readiness_reports_slot_failure(self) -> None:
        adapter = _direct_adapter_for_readiness(metaserver="")
        result = adapter._run_backend_readiness_gate(reason="unit-slot-missing", timeout_ms=1)

        self.assertEqual(result["status"], "topology_not_ready")
        self.assertIn("Slot not found", result["error"])
        self.assertTrue(result["checks"]["namespace_table_opened"])
        self.assertFalse(result["checks"]["slot_coverage_verified_by_warmup_hset_hget"])
        self.assertEqual(result["topology"]["namespace"], "deploy_ns")
        self.assertEqual(result["topology"]["table"], "deploy_table")

