# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_BackendPolicyPart1 methods split from test_matrixark_mcp_backend_policy.MatrixArkMcpBackendPolicyTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_mcp_backend_policy import (
    Path,
    SimpleNamespace,
    _HashStoreClient,
    _SHARED_CORRECTNESS_EVIDENCE,
    _direct_adapter_for_hash_store,
    argparse,
    comparison,
    fallback_flags_from_backend,
    json,
    mcp,
    mcp_context_pack,
    mcp_core,
    mcp_summary_runtime,
    message_record_builders,
    retrieve_index_terms,
    tempfile,
    timeout_count,
)
except ImportError:
    from test_matrixark_mcp_backend_policy import (
    Path,
    SimpleNamespace,
    _HashStoreClient,
    _SHARED_CORRECTNESS_EVIDENCE,
    _direct_adapter_for_hash_store,
    argparse,
    comparison,
    fallback_flags_from_backend,
    json,
    mcp,
    mcp_context_pack,
    mcp_core,
    mcp_summary_runtime,
    message_record_builders,
    retrieve_index_terms,
    tempfile,
    timeout_count,
)


class _BackendPolicyPart1:
    def test_scale_report_requires_shared_correctness_evidence(self) -> None:
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {
                "qps": 100,
                "p50_ms": 1,
                "p95_ms": 1,
                "p99_ms": 1,
                "selected_refs_avg": 2,
                "stage_metrics": {
                    "selected_refs_avg": 2,
                    "selected_refs_max": 2,
                    "correctness_evidence": {
                        **_SHARED_CORRECTNESS_EVIDENCE,
                        "scope_filtering": False,
                        "compact_secondary_index_prefilter": False,
                    },
                    "selected_ref_signatures_by_query": {"0": ["event:1", "entity:7"]},
                },
            },
        }
        rust = json.loads(json.dumps(base))

        result = comparison(base, rust)

        self.assertEqual(result["status"], "failed")
        missing = [
            failure["requirement"]
            for failure in result["phase0_correctness"]["failures"]
            if failure["reason"] == "missing_correctness_evidence"
        ]
        self.assertIn("scope_filtering", missing)
        self.assertIn("compact_secondary_index_prefilter", missing)

    def test_scale_report_counts_timeouts_and_fallback_flags(self) -> None:
        self.assertEqual(timeout_count(["request timed out", "Slot not found", "timeout waiting for response"]), 2)
        flags = fallback_flags_from_backend(
            {
                "status": "failed",
                "retrieve": {"partial_context_packs": 1, "stage_metrics": {"samples": 0}},
                "errors": {"ingest": ["memory fallback used"], "retrieve": []},
                "backend_metrics": {"result": {"embedding_fallback_used": True}},
            }
        )

        self.assertTrue(flags["memory_fallback"])
        self.assertTrue(flags["hash_embedding_fallback"])
        self.assertTrue(flags["partial_context_pack"])
        self.assertTrue(flags["native_metrics_missing"])

    def _args(self, backend: str) -> argparse.Namespace:
        return argparse.Namespace(backend=backend)

    def test_context_pack_access_view_omits_hashes(self) -> None:
        access = mcp.MatrixArkMcpServer._serving_access(
            {
                "_matrixark_auth": {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "local_user",
                    "session_id": "debug-message-pdf-session",
                    "agent_name": "codex",
                    "api_key_id": "dev",
                    "role": "dev_admin",
                    "mode": "dev",
                    "scope_key": "t=1|u=2|s=3|",
                    "tenant_hash": 1,
                    "user_hash": 2,
                    "session_hash": 3,
                }
            }
        )
        self.assertEqual("acct_local", access.get("account_id"))
        self.assertEqual("tenant_codex", access.get("tenant_id"))
        self.assertEqual("local_user", access.get("user_id"))
        self.assertEqual("debug-message-pdf-session", access.get("session_id"))
        self.assertNotIn("scope_key", access)
        self.assertNotIn("tenant_hash", access)
        self.assertNotIn("user_hash", access)
        self.assertNotIn("session_hash", access)

    def test_message_event_secondary_indexes_are_not_written_per_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.ingest(
                {
                    "messages": [{"role": "user", "content": "Alice approved the GPU request."}],
                    "scope": {
                        "account_id": "acct_local",
                        "tenant_id": "tenant_codex",
                        "user_id": "codex_user",
                        "session_id": "secondary-index-cap",
                    },
                }
            )
            records = adapter.read_all()
            event_indexes = [
                record
                for record in records
                if record.get("record_type") == "context_index" and record.get("data_model") == "context_event"
            ]
            self.assertEqual([], event_indexes)

    def test_context_index_postings_group_by_model_term_and_time_bucket(self) -> None:
        scope = mcp.enrich_scope_with_identity(
            {"session_id": "index-grouping-test"},
            {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "codex_user",
                "session_id": "",
                "agent_name": "codex",
                "mode": "dev",
            },
        )
        records = [
            mcp.context_index_posting_record(
                index_name="resource_type:pdf",
                data_model="resource_chunk",
                ref_type="resource_chunk",
                ref_hashes=[100 + index],
                node_hash=77,
                scope=scope,
                updated_at_ms=1780000000123 + index,
            )
            for index in range(8)
        ]

        materialized = mcp.materialize_serving_record_batch(records)
        indexes = [record for record in materialized if record.get("record_type") == "context_index"]

        self.assertEqual(1, len(indexes))
        self.assertEqual("resource_chunk", indexes[0].get("data_model"))
        self.assertEqual("resource_type:pdf", indexes[0].get("index_name"))
        self.assertEqual(8, indexes[0].get("posting_count"))
        self.assertEqual(list(range(100, 108)), indexes[0].get("ref_hashes"))
        self.assertNotIn("ref_hash", indexes[0])
        self.assertEqual((1780000000123 // mcp.SECONDARY_INDEX_TIME_BUCKET_MS) * mcp.SECONDARY_INDEX_TIME_BUCKET_MS, indexes[0].get("timestamp_key_ms"))

    def test_context_index_postings_split_only_when_ref_cap_is_exceeded(self) -> None:
        cap = mcp.MAX_SECONDARY_INDEX_REFS_PER_POSTING
        records = [
            mcp.context_index_posting_record(
                index_name="keyword:gpu",
                data_model="resource_chunk",
                ref_type="resource_chunk",
                ref_hashes=[index],
                node_hash=88,
                updated_at_ms=1780000060000,
            )
            for index in range(cap + 2)
        ]
        materialized = mcp.materialize_serving_record_batch(records)
        indexes = [record for record in materialized if record.get("record_type") == "context_index"]

        self.assertEqual(2, len(indexes))
        self.assertEqual(list(range(cap)), indexes[0].get("ref_hashes"))
        self.assertEqual([cap, cap + 1], indexes[1].get("ref_hashes"))
        self.assertEqual([0, 1], [record.get("posting_part") for record in indexes])
        self.assertEqual(cap + 2, indexes[0].get("posting_count"))

    def test_context_index_postings_feed_direct_ref_and_node_prefilter(self) -> None:
        record = {
            "record_type": "context_index",
            "index_name": "entity_type:context_management",
            "data_model": "context_entity",
            "ref_type": "entity",
            "ref_hashes": [7101],
            "node_hashes": [44, "45", 44],
            "updated_at_ms": 1785289825466,
        }
        index_terms_by_batch: dict[object, list[str]] = {}
        index_terms_by_node: dict[object, list[str]] = {}
        index_terms_by_ref: dict[object, list[str]] = {}
        index_terms_by_node_for_prefilter: dict[int, list[str]] = {}

        self.assertTrue(
            retrieve_index_terms.add_context_index_terms(
                record,
                index_terms_by_batch=index_terms_by_batch,
                index_terms_by_node=index_terms_by_node,
                index_terms_by_ref=index_terms_by_ref,
                index_terms_by_node_for_prefilter=index_terms_by_node_for_prefilter,
            )
        )

        self.assertEqual(["entity_type:context_management"], index_terms_by_ref[7101])
        self.assertEqual(
            {
                44: ["entity_type:context_management"],
                45: ["entity_type:context_management"],
            },
            index_terms_by_node_for_prefilter,
        )
        self.assertEqual({}, index_terms_by_node)

    def test_secondary_index_budget_caps_total_operation_terms(self) -> None:
        budget = mcp_core.new_secondary_index_budget(3)
        first = mcp_core.take_secondary_index_terms(
            [
                "source_type:resource",
                "resource_type:pdf",
                "keyword:gpu",
                "keyword:budget",
            ],
            budget,
        )
        second = mcp_core.take_secondary_index_terms(
            [
                "event_type:approval",
                "entity_type:owner",
            ],
            budget,
        )

        self.assertEqual(first, ["source_type:resource", "resource_type:pdf", "keyword:gpu"])
        self.assertEqual(second, [])
        self.assertEqual(
            mcp_core.secondary_index_budget_summary(budget),
            {
                "index_total_cap": 3,
                "index_emitted_count": 3,
                "index_dropped_by_total_cap_count": 3,
            },
        )

    def test_approval_state_entity_name_uses_stable_subject_not_full_state(self) -> None:
        self.assertEqual(
            mcp_core.canonical_entity_name("approval_state", "Project Aurora GPU procurement after Q3 budget review"),
            "Project Aurora GPU procurement",
        )
        self.assertEqual(
            mcp_core.canonical_entity_name("approval_state", "the Project Aurora GPU procurement after finance review"),
            "Project Aurora GPU procurement",
        )
        self.assertEqual(
            mcp_core.canonical_entity_name("approval_state", "attachment is required before vendor selection"),
            "attachment",
        )
        self.assertEqual(
            mcp_core.canonical_entity_name("approval_state", "attachment as a blocker before vendor selection"),
            "attachment",
        )

    def test_assistant_decision_extraction_keeps_decision_lines_not_full_response(self) -> None:
        result = mcp_core.extract_batch_entities(
            [
                {
                    "role": "assistant",
                    "content": (
                        "Here is a long implementation explanation that should stay out of durable profile memory.\n"
                        "It describes file layouts, helper functions, and examples in detail.\n"
                        "Decision: use Stop plus threshold/idle provisional extraction for live Codex memory.\n"
                        "More verbose explanation that should not become the assistant_decision state.\n"
                        "Next: retrieve same-session memory first, then profile entities under budget."
                    ),
                }
            ],
            {"source_event_ids": [101], "metadata": {}},
        )

        decision = next(entity for entity in result if entity["entity_type"] == "assistant_decision")
        self.assertIn("Decision: use Stop plus threshold/idle provisional extraction", decision["state"])
        self.assertIn("Next: retrieve same-session memory first", decision["state"])
        self.assertNotIn("long implementation explanation", decision["state"])
        self.assertNotIn("file layouts", decision["state"])
        self.assertLessEqual(len(decision["state"]), 260)
        self.assertEqual(["101"], decision["source_refs"])

    def test_assistant_decision_extraction_accepts_llm_role_aliases(self) -> None:
        result = mcp_core.extract_batch_entities(
            [
                {
                    "role": "model",
                    "content": "Decision: keep profile memory query budgets visible after summary refresh.",
                },
                {
                    "role": "assistant_response",
                    "content": "Next: retrieve current profile entities before stale session facts.",
                },
            ],
            {"source_event_ids": [301, 302], "metadata": {}},
        )

        decision = next(entity for entity in result if entity["entity_type"] == "assistant_decision")
        self.assertIn("Decision: keep profile memory query budgets", decision["state"])
        self.assertIn("Next: retrieve current profile entities", decision["state"])
        self.assertEqual(["301", "302"], decision["source_refs"])

    def test_tool_evidence_extraction_keeps_result_lines_not_full_output(self) -> None:
        result = mcp_core.extract_batch_entities(
            [
                {
                    "role": "tool",
                    "content": (
                        "Compiling crate alpha with hundreds of verbose warnings that should not be durable.\n"
                        "warning: unused import in unrelated generated file\n"
                        "Exit code: 0\n"
                        "Ran 86 tests in 1.59s\n"
                        "OK\n"
                        "To https://github.com/matrixarkai/TemporalStore.git\n"
                        "abbf5f23 HEAD -> main\n"
                        "More streaming logs that do not carry final evidence."
                    ),
                }
            ],
            {"source_event_ids": [202], "metadata": {}},
        )

        evidence = next(entity for entity in result if entity["entity_type"] == "tool_evidence")
        self.assertIn("Exit code: 0", evidence["state"])
        self.assertIn("Ran 86 tests", evidence["state"])
        self.assertIn("OK", evidence["state"])
        self.assertNotIn("hundreds of verbose warnings", evidence["state"])
        self.assertNotIn("unused import", evidence["state"])
        self.assertLessEqual(len(evidence["state"]), 260)
        self.assertEqual(["202"], evidence["source_refs"])

    def test_tool_evidence_extraction_accepts_tool_role_aliases(self) -> None:
        result = mcp_core.extract_batch_entities(
            [
                {
                    "role": "function",
                    "content": "Exit code: 0\nRan 12 tests in 0.2s\nOK",
                },
                {
                    "role": "tool_output",
                    "content": "pushed commit abcdef1 to origin/main",
                },
                {
                    "role": "function_call_output",
                    "content": "codex_memory_roles tests passed",
                },
                {
                    "role": "custom_tool_call_output",
                    "content": "rebase completed cleanly",
                },
            ],
            {"source_event_ids": [401, 402, 403, 404], "metadata": {}},
        )

        evidence = next(entity for entity in result if entity["entity_type"] == "tool_evidence")
        self.assertIn("Exit code: 0", evidence["state"])
        self.assertIn("Ran 12 tests", evidence["state"])
        self.assertIn("pushed commit abcdef1", evidence["state"])
        self.assertIn("codex_memory_roles tests passed", evidence["state"])
        self.assertIn("rebase completed cleanly", evidence["state"])
        self.assertEqual(["401", "402", "403", "404"], evidence["source_refs"])

    def test_openai_compatible_batch_extraction_uses_model_entities(self) -> None:
        extraction_globals = mcp_core.one_pass_memory_extraction.__globals__
        old_call = extraction_globals["openai_compatible_json_call"]
        self.addCleanup(lambda: extraction_globals.__setitem__("openai_compatible_json_call", old_call))

        def fake_json_call(*, system: str, user: str) -> dict:
            return {
                "classification": "BATCH_MEMORY",
                "event_type": "approval_state",
                "batch_summary": "Project Aurora GPU procurement is approved with Bob as owner.",
                "entities": [
                    {
                        "entity_type": "approval_state",
                        "entity_name": "Project Aurora GPU procurement",
                        "state": "Approved by Alice after Q3 budget review.",
                        "confidence": 0.93,
                        "operator": "LLM_MERGE",
                    },
                    {
                        "entity_type": "resource_owner",
                        "entity_name": "Project Aurora procurement owner",
                        "state": "Bob owns procurement and vendor coordination.",
                        "confidence": 0.9,
                    },
                ],
                "segments": [
                    {
                        "topic": "project_aurora_gpu",
                        "coordinate_tuples": [[0, 1]],
                        "message_indexes": [0, 1],
                        "saliency_score": 0.95,
                        "summary_text": "Approval and owner for Project Aurora GPU procurement.",
                    }
                ],
                "indexes": ["event_type:approval_state", "entity_type:approval_state"],
            }

        extraction_globals["openai_compatible_json_call"] = fake_json_call
        result = mcp_core.one_pass_memory_extraction(
            {
                "kind": "message",
                "messages": [
                    {"role": "user", "content": "Alice approved Project Aurora GPU procurement."},
                    {"role": "assistant", "content": "Bob owns procurement follow-up."},
                ],
                "scope": {},
                "metadata": {},
                "source_event_ids": ["evt-1", "evt-2"],
                "extraction_provider": "openai-compatible",
                "understanding_provider": "openai-compatible",
            },
            prior_context={"level": "", "refs": [], "messages": [], "summaries": []},
        )

        self.assertEqual(result["mode"], "matrixark_one_pass_schema_openai_compatible")
        self.assertEqual(result["segment_provider"]["provider"], "openai_compatible")
        self.assertEqual(result["entities"][0]["extracted_by"], "openai_compatible")
        self.assertEqual(result["entities"][0]["entity_name"], "Project Aurora GPU procurement")
        self.assertEqual(result["entities"][0]["state"], "Approved by Alice after Q3 budget review.")
        self.assertEqual(result["entities"][0]["operator"], "EUA_MERGE")
        self.assertNotEqual(result["entities"][0]["entity_name"], result["entities"][0]["state"])

    def test_openai_compatible_resource_fact_extraction_uses_model_facts(self) -> None:
        extraction_globals = mcp_core.extract_resource_facts.__globals__
        old_call = extraction_globals["openai_compatible_json_call"]
        self.addCleanup(lambda: extraction_globals.__setitem__("openai_compatible_json_call", old_call))

        def fake_json_call(*, system: str, user: str) -> dict:
            return {
                "facts": [
                    {
                        "event_type": "resource_decision",
                        "entity_type": "resource_decision",
                        "entity_name": "Project Aurora GPU approval",
                        "value": "Alice approved the GPU purchase after finance review.",
                        "confidence": 0.91,
                    }
                ]
            }

        extraction_globals["openai_compatible_json_call"] = fake_json_call
        chunk = SimpleNamespace(
            text="Alice approved the GPU purchase after finance review.",
            metadata={"heading": "Approval Packet"},
            chunk_hash="chunk-123",
            source_ref="gpu.pdf#page=1",
        )
        facts = mcp_core.extract_resource_facts(
            chunk,
            chunk_metadata={"heading": "Approval Packet"},
            envelope={"extraction_provider": "openai-compatible", "understanding_provider": "openai-compatible"},
            raw_uri="/tmp/gpu.pdf",
            resource_version="v1",
        )

        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["mode"], "matrixark_resource_schema_openai_compatible")
        self.assertEqual(facts[0]["extraction_provider"], "openai_compatible")
        self.assertEqual(facts[0]["entity_name"], "Project Aurora GPU approval")
        self.assertEqual(facts[0]["value"], "Alice approved the GPU purchase after finance review.")
        self.assertNotEqual(facts[0]["entity_name"], facts[0]["value"])

    def test_deterministic_entities_do_not_advertise_llm_merge(self) -> None:
        result = mcp_core.one_pass_memory_extraction(
            {
                "kind": "message",
                "messages": [{"role": "user", "content": "Alice approved Project Aurora GPU procurement."}],
                "scope": {},
                "metadata": {},
            },
            prior_context={"level": "", "refs": [], "messages": [], "summaries": []},
        )

        operators = {entity.get("operator") for entity in result["entities"]}
        self.assertIn("EUA_MERGE", operators)
        self.assertNotIn("LLM_MERGE", operators)

    def test_context_pack_serving_refs_drop_debug_index_and_hash_fields(self) -> None:
        ref = {
            "ref_type": "resource_chunk",
            "ref_hash": 123,
            "node_hash": 456,
            "node_path": ["tenant", "shared", "resources"],
            "score": 0.812345,
            "matched_index_terms": ["keyword:gpu", "resource_type:pdf"],
            "keyword_score": 2,
            "embedding_score": 0.91,
            "text": "Alice approved the GPU purchase after finance review.",
            "source_ref": "docs/gpu.pdf#page=2",
            "raw_uri": "s3://bucket/docs/gpu.pdf",
            "resource_type": "pdf",
            "metadata": {
                "unit_kind": "pdf_page",
                "page": 2,
                "keywords": ["gpu", "approval", "finance"],
                "content_hash": "abcdef",
                "relative_path": "docs/gpu.pdf",
            },
        }
        compact = mcp.compact_context_pack_refs([ref])[0]

        self.assertEqual(compact["ref_type"], "resource_chunk")
        self.assertEqual(compact["text"], ref["text"])
        self.assertEqual(compact["source_ref"], "docs/gpu.pdf#page=2")
        self.assertNotIn("score", compact)
        self.assertNotIn("token_estimate", compact)
        self.assertNotIn("raw_uri", compact)
        self.assertNotIn("ref_hash", compact)
        self.assertNotIn("node_hash", compact)
        self.assertNotIn("node_path", compact)
        self.assertNotIn("matched_index_terms", compact)
        self.assertNotIn("keyword_score", compact)
        self.assertNotIn("embedding_score", compact)
        self.assertNotIn("keywords", compact.get("metadata", {}))
        self.assertNotIn("content_hash", compact.get("metadata", {}))
        self.assertEqual(compact["metadata"], {"unit_kind": "pdf_page", "relative_path": "docs/gpu.pdf", "page": 2})

    def test_context_pack_serving_top_level_omits_operational_cache_fields(self) -> None:
        compact = mcp.compact_context_pack_for_serving({
            "context_pack_id": "pack-123",
            "context_pack_assembly": "native_direct",
            "context_pack_cache_hit": False,
            "cache_hit": False,
            "cache_hit_used": False,
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "scan_count": 42,
            "selected_refs": [],
            "remote_context_refs": [],
        })

        self.assertEqual(compact["pack_id"], "pack-123")
        for field in [
            "assembly",
            "cache_hit",
            "cache_hit_used",
            "context_pack_id",
            "context_pack_assembly",
            "context_pack_cache_hit",
            "native_pack_assembly",
            "raw_records_returned",
            "python_hot_path_records",
            "scan_count",
        ]:
            self.assertNotIn(field, compact)

    def test_context_pack_serving_omits_duplicate_and_operational_fields(self) -> None:
        ref = {"ref_type": "event", "text": "Alice approved the GPU request."}
        compact = mcp.compact_context_pack_for_serving({
            "context_pack_id": "pack-123",
            "selected_refs": [ref],
            "remote_context_refs": [ref],
            "selected_ref_counts": {"event": 1},
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "primary_candidate_count": 10,
            "auxiliary_candidate_count": 2,
            "budget_source": "agent_provided_max_context_tokens",
            "local_context_safety_margin_tokens": 70,
            "remote_context_budget_tokens": 1330,
            "request_deadline_ms": 120000,
            "request_elapsed_ms": 25.15,
            "used_context_tokens": 12,
            "used_remote_context_tokens": 12,
            "used_local_context_tokens": 0,
            "total_prompt_context_tokens": 12,
            "local_context_policy": {
                "local_context_count": 0,
                "local_context_tokens": 0,
                "safety_margin_tokens": 70,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "recall_policy": {
                "query_plan": {"query_type": "fact", "temporal_window": {"mode": "latest"}},
                "tree_traversal": {"enabled": True, "selected_node_count": 3},
            },
            "dropped_refs": {"duplicate": 3},
            "insufficient_context": False,
            "partial_context_pack": False,
            "packing_policy": "question_type_aware:fact",
            "requested_max_context_tokens": 1000,
            "context_pack_cache_hit": True,
            "cache_hit": True,
            "cache_hit_used": True,
            "context_pack_assembly": "native_direct",
            "assembly": "native_direct",
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "scan_count": 50,
            "selected_ref_count": 1,
            "dropped_ref_count": 0,
        })

        self.assertEqual(compact["selected_refs"], [{"ref_type": "event", "text": "Alice approved the GPU request."}])
        self.assertNotIn("remote_context_refs", compact)
        for field in [
            "selected_ref_counts",
            "context_sources_order",
            "primary_candidate_count",
            "auxiliary_candidate_count",
            "budget_source",
            "local_context_safety_margin_tokens",
            "remote_context_budget_tokens",
            "request_deadline_ms",
            "request_elapsed_ms",
            "used_remote_context_tokens",
            "used_local_context_tokens",
            "total_prompt_context_tokens",
            "local_context_policy",
            "recall_policy",
            "dropped_refs",
            "insufficient_context",
            "partial_context_pack",
            "packing_policy",
            "requested_max_context_tokens",
            "context_pack_cache_hit",
            "cache_hit",
            "cache_hit_used",
            "context_pack_assembly",
            "assembly",
            "native_pack_assembly",
            "raw_records_returned",
            "python_hot_path_records",
            "scan_count",
            "selected_ref_count",
            "dropped_ref_count",
        ]:
            self.assertNotIn(field, compact)
        self.assertEqual(compact.get("recall"), {"temporal": "latest"})

    def test_context_pack_serving_dropped_policy_omits_numeric_knobs(self) -> None:
        compact = mcp.compact_dropped_refs_for_context_pack({
            "cross_session_budget": 2,
            "cross_session_policy": {
                "enabled": True,
                "mode": "prefer",
                "decision": "always_consider_same_user_cross_session_when_session_scope_prefer",
                "budget_ratio": 0.2,
                "budget_tokens": 8192,
                "max_budget_tokens": 8192,
                "max_candidates": 24,
                "max_sessions": 3,
                "parallelism": 4,
                "selected_ref_count": 0,
                "selected_tokens": 0,
            },
        })

        policy = compact["cross_session_policy"]
        self.assertTrue(policy["enabled"])
        self.assertEqual(policy["mode"], "prefer")
        self.assertIn("decision", policy)
        self.assertNotIn("budget_ratio", policy)
        self.assertNotIn("budget_tokens", policy)
        self.assertNotIn("max_budget_tokens", policy)
        self.assertNotIn("max_candidates", policy)
        self.assertNotIn("max_sessions", policy)
        self.assertNotIn("parallelism", policy)
        self.assertNotIn("selected_ref_count", policy)
        self.assertNotIn("selected_tokens", policy)

    def test_context_pack_serving_exposes_compact_retrieval_decision(self) -> None:
        compact = mcp_context_pack.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-policy",
                "selected_refs": [],
                "recall_policy": {
                    "query_plan": {"query_type": "current_state"},
                    "cross_session": {
                        "enabled": True,
                        "mode": "prefer",
                        "decision": "always_consider_same_user_cross_session_when_session_scope_prefer",
                        "question_type": "current_state",
                        "question_budget_reason": "current_state_or_latest_queries_need_prior entity state and stale blockers",
                        "budget_ratio": 0.2,
                        "budget_tokens": 400,
                        "max_budget_tokens": 8192,
                        "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                        "budget_floor_status": "not_needed",
                    },
                },
            }
        )

        decision = compact["retrieval_decision"]
        self.assertEqual("current_state", decision["query_type"])
        self.assertEqual("current_latest_multi_hop_or_date_20_percent", decision["cross_session"]["budget_class"])
        self.assertEqual("prefer", decision["cross_session"]["mode"])
        self.assertIn("question_budget_reason", decision["cross_session"])
        self.assertNotIn("budget_ratio", decision["cross_session"])
        self.assertNotIn("budget_tokens", decision["cross_session"])

    def test_replay_returns_compact_context_pack_scope_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.append_many([
                {
                    "record_type": "context_pack_audit",
                    "context_pack_id": "pack-1",
                    "query": "gpu approval",
                    "selected_refs": [{"ref_type": "resource_chunk", "text_preview": "gpu approved", "ref_hash": 1}],
                    "dropped_refs": {
                        "over_budget": 2,
                        "refs": [{"ref_type": "event", "ref_hash": 99, "text_preview": "debug"}],
                        "reason_descriptions": {"over_budget": "verbose explanation"},
                    },
                    "recall_policy": {
                        "query_plan": {"query_type": "fact", "secondary_filters": {"keyword": ["gpu"]}},
                        "tree_traversal": {"enabled": True, "selected_node_count": 2, "trace": ["verbose"]},
                        "secondary_index_filter": {"enabled": True, "matched_candidate_count": 3},
                        "rerank": {"enabled": True, "mode": "question_type_token_efficiency"},
                    },
                    "layer_scores": [{"verbose": True}],
                    "used_remote_context_tokens": 42,
                    "created_at_ms": 10,
                },
                {"record_type": "context_pack_audit", "context_pack_id": "pack-2", "query": "other", "created_at_ms": 11},
                {"record_type": "context_pack_telemetry", "context_pack_id": "pack-1", "selected_ref_count": 1, "dropped_ref_count": 2},
                {"record_type": "context_event", "context_pack_id": "unrelated", "event_id_hash": 7, "text": "not replayed"},
            ])

            replay = adapter.replay({"pack_id": "pack-1"})
            self.assertEqual(replay["replay_payload_policy"], "compact_context_pack_scope")
            self.assertTrue(replay["events"])
            self.assertTrue(all(row.get("context_pack_id") == "pack-1" for row in replay["events"]))
            audit = next(row for row in replay["events"] if row.get("record_type") == "context_pack_audit")
            self.assertNotIn("recall_policy", audit)
            self.assertNotIn("layer_scores", audit)
            self.assertNotIn("refs", audit.get("dropped_refs", {}))
            self.assertNotIn("reason_descriptions", audit.get("dropped_refs", {}))
            self.assertEqual(audit["dropped_refs"]["over_budget"], 2)
            self.assertTrue(audit["dropped_refs"]["dropped_ref_detail_available_in_audit"])
            self.assertIn("recall_policy_summary", audit)

    def test_serving_records_store_compact_scope_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp.enrich_scope_with_identity(
                {"session_id": "debug-message-pdf-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_hash": 1,
                        "node_hash": 10,
                        "summary_text": "summary",
                        "node_path": ["tenant:tenant_codex", "user:codex_user"],
                        "depth": 2,
                        "scope": scope,
                        "created_at_ms": 1780000000000,
                        "updated_at_ms": 1780000000000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "node_l0",
                        "ref_type": "node",
                        "ref_hash": 10,
                        "node_hash": 10,
                        "vector": [0.1],
                        "dim": 1,
                        "model": "matrixark-local-token-hash-v1",
                        "scope": scope,
                    },
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": 2,
                        "raw_uri": "local://policy.pdf",
                        "access_scope": mcp.registry_access_scope(scope),
                        "scope": scope,
                    },
                    {
                        "record_type": "resource_chunk",
                        "chunk_hash": 3,
                        "node_hash": 10,
                        "text": "chunk",
                        "access_scope": mcp.registry_access_scope(scope),
                        "scope": scope,
                    },
                ]
            )

            compacted = adapter.read_all()
            self.assertTrue(compacted)
            for record in compacted:
                if record.get("record_type") in {"context_debug_record", "context_model_registry"}:
                    continue
                self.assertEqual(scope["scope_key"], record.get("scope_key"))
                self.assertNotIn("scope", record)
                self.assertNotIn("_explicit_scope_keys", str(record))
                if record.get("record_type") == "context_summary":
                    self.assertNotIn("created_at_ms", record)
                    self.assertNotIn("depth", record)
                    self.assertEqual(1780000000000, record.get("updated_at_ms"))
                if record.get("record_type") == "context_embedding":
                    self.assertNotIn("dim", record)
                    self.assertNotIn("model", record)
                    self.assertNotIn("model_hash", record)
                    self.assertEqual(mcp.embedding_model_ref_for_name("matrixark-local-token-hash-v1"), record.get("model_ref"))
            registry = [record for record in compacted if record.get("record_type") == "context_model_registry"]
            self.assertEqual(1, len(registry))
            self.assertEqual("matrixark-local-token-hash-v1", registry[0].get("model_name"))
            self.assertEqual(mcp.embedding_model_ref_for_name("matrixark-local-token-hash-v1"), registry[0].get("model_ref"))
            scoped_record = next(record for record in compacted if record.get("record_type") != "context_model_registry")
            self.assertTrue(
                mcp.scope_matches(mcp.candidate_access_scope(scoped_record), scope),
                "compacted scope_key should still satisfy scoped retrieval",
            )

    def test_embedding_model_registry_groups_model_name_once(self) -> None:
        records = [
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": 1,
                "vector": [0.1, 0.2],
                "dim": 2,
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "updated_at_ms": 1000,
            },
            {
                "record_type": "context_embedding",
                "embedding_type": "entity_state",
                "ref_type": "entity",
                "ref_hash": 2,
                "vector": [0.3, 0.4],
                "dim": 2,
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "updated_at_ms": 2000,
            },
        ]

        materialized = mcp.materialize_serving_record_batch(records)
        registry = [record for record in materialized if record.get("record_type") == "context_model_registry"]
        embeddings = [record for record in materialized if record.get("record_type") == "context_embedding"]

        self.assertEqual(1, len(registry))
        self.assertEqual("sentence-transformers/all-MiniLM-L6-v2", registry[0]["model_name"])
        self.assertEqual(2000, registry[0]["updated_at_ms"])
        self.assertEqual(2, len(embeddings))
        self.assertTrue(all(record.get("model_ref") == registry[0]["model_ref"] for record in embeddings))
        self.assertTrue(all("model" not in record for record in embeddings))
        self.assertTrue(all("model_hash" not in record for record in embeddings))
        self.assertTrue(all("dim" not in record for record in embeddings))

    def test_context_embedding_materialization_strips_bulky_lineage(self) -> None:
        materialized = mcp.materialize_serving_record_batch(
            [
                {
                    "record_type": "context_embedding",
                    "embedding_type": "profile_entity_state",
                    "ref_type": "entity",
                    "ref_hash": 44,
                    "node_hash": 55,
                    "vector": [0.1, 0.2],
                    "dim": 2,
                    "model": "matrixark-local-token-hash-v1",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "extraction_phase": "final",
                    "source_event_ids": [11],
                    "source_session_ids": ["session_old"],
                    "source_role_counts": {"assistant": 1},
                    "source_hook_type_counts": {"after_llm": 1},
                    "source_codex_event_counts": {"Stop": 1},
                    "source_memory_selection_policy_counts": {
                        "selected_assistant_decision_outcome_only": 1,
                    },
                    "source_event_count": 3,
                    "source_final_session_boundary_count": 1,
                    "final_session_boundary": True,
                    "summary_generation_policy": {"provider": "local", "max_chars": 220},
                    "profile_revision": 2,
                    "promoted_from_memory_scope": "session",
                    "supersedes_session_entity_hashes": [33],
                }
            ]
        )
        embeddings = [record for record in materialized if record.get("record_type") == "context_embedding"]

        self.assertEqual(1, len(embeddings))
        embedding = embeddings[0]
        self.assertEqual("user_profile", embedding["memory_scope"])
        self.assertEqual("cross_session", embedding["session_continuity"])
        for field in [
            "source_event_ids",
            "source_session_ids",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_memory_selection_policy_counts",
            "source_event_count",
            "source_final_session_boundary_count",
            "extraction_phase",
            "final_session_boundary",
            "summary_generation_policy",
            "profile_revision",
            "promoted_from_memory_scope",
            "supersedes_session_entity_hashes",
        ]:
            self.assertNotIn(field, embedding)

    def test_message_embedding_builder_keeps_only_serving_layer_fields(self) -> None:
        embedding = message_record_builders.context_embedding_record(
            embedding_type="event_text",
            ref_type="event",
            ref_hash=44,
            node_hash=55,
            node_path=["tenant:t", "user:u", "session:s"],
            vector=[0.1, 0.2],
            scope={"tenant_id": "t", "user_id": "u", "session_id": "s"},
            updated_at_ms=1000,
            memory_scope="session",
            session_continuity="same_session",
        )

        self.assertEqual("session", embedding["memory_scope"])
        self.assertEqual("same_session", embedding["session_continuity"])
        self.assertNotIn("extraction_phase", embedding)
        self.assertNotIn("source_event_ids", embedding)
        self.assertNotIn("source_role_counts", embedding)

    def test_summary_embedding_compaction_strips_lineage_debug_fields(self) -> None:
        embedding = mcp_summary_runtime.compact_summary_embedding_record(
            {
                "record_type": "context_embedding",
                "embedding_type": "node_l1",
                "ref_type": "summary",
                "ref_hash": 44,
                "node_hash": 55,
                "node_path": ["tenant:t", "user:u", "profile:long_term_memory"],
                "vector": [0.1, 0.2],
                "dim": 2,
                "model": "matrixark-local-token-hash-v1",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_event_ids": [11],
                "source_entity_hashes": [22],
                "source_summary_hashes": [33],
                "source_roles": ["user", "assistant"],
                "source_role_counts": {"user": 1, "assistant": 1},
                "source_hook_types": ["before_llm", "after_llm"],
                "source_hook_type_counts": {"before_llm": 1, "after_llm": 1},
                "source_codex_events": ["UserPromptSubmit", "Stop"],
                "source_codex_event_counts": {"UserPromptSubmit": 1, "Stop": 1},
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 1,
                },
                "source_memory_scopes": ["session", "user_profile"],
                "source_session_continuities": ["same_session", "cross_session"],
                "source_extraction_phases": ["provisional", "final"],
                "source_final_session_boundary_count": 1,
                "summary_generation_policy": {"provider": "deterministic"},
                "dirty_hash": 66,
            }
        )

        self.assertEqual("user_profile", embedding["memory_scope"])
        self.assertEqual("cross_session", embedding["session_continuity"])
        for field in [
            "extraction_phase",
            "final_session_boundary",
            "source_event_ids",
            "source_entity_hashes",
            "source_summary_hashes",
            "source_roles",
            "source_role_counts",
            "source_hook_types",
            "source_hook_type_counts",
            "source_codex_events",
            "source_codex_event_counts",
            "source_memory_selection_policy_counts",
            "source_memory_scopes",
            "source_session_continuities",
            "source_extraction_phases",
            "source_final_session_boundary_count",
            "summary_generation_policy",
            "dirty_hash",
        ]:
            self.assertNotIn(field, embedding)

    def test_latest_context_state_compacts_summary_and_summary_embedding_versions(self) -> None:
        records = [
            {"record_type": "context_event", "event_id_hash": 7, "text": "keep me"},
            {
                "record_type": "context_summary",
                "summary_type": "session_l0",
                "summary_hash": 11,
                "summary_text": "old summary",
                "summary_version_hash": 101,
                "updated_at_ms": 1000,
            },
            {
                "record_type": "context_embedding",
                "embedding_type": "session_l0",
                "ref_type": "summary",
                "ref_hash": 11,
                "vector": [0.1],
                "summary_version_hash": 101,
                "updated_at_ms": 1000,
            },
            {
                "record_type": "context_summary",
                "summary_type": "session_l0",
                "summary_hash": 11,
                "summary_text": "new summary",
                "summary_version_hash": 202,
                "updated_at_ms": 2000,
            },
            {
                "record_type": "context_embedding",
                "embedding_type": "session_l0",
                "ref_type": "summary",
                "ref_hash": 11,
                "vector": [0.9],
                "summary_version_hash": 202,
                "updated_at_ms": 2000,
            },
        ]

        compacted = mcp.compact_latest_context_state_records(records)
        summaries = [record for record in compacted if record.get("record_type") == "context_summary"]
        embeddings = [record for record in compacted if record.get("record_type") == "context_embedding"]

        self.assertEqual(1, len(summaries))
        self.assertEqual("new summary", summaries[0]["summary_text"])
        self.assertNotIn("summary_version_hash", summaries[0])
        self.assertEqual(1, len(embeddings))
        self.assertEqual([0.9], embeddings[0]["vector"])
        self.assertNotIn("summary_version_hash", embeddings[0])
        self.assertTrue(any(record.get("record_type") == "context_event" for record in compacted))

    def test_local_read_all_returns_latest_summary_state_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {"account_id": "acct_local", "tenant_id": "tenant_codex", "user_id": "codex_user", "session_id": "summary-latest-test"}
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": 22,
                        "summary_text": "old session summary",
                        "summary_version_hash": 1,
                        "scope": scope,
                        "updated_at_ms": 1000,
                    },
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": 22,
                        "summary_text": "new session summary",
                        "summary_version_hash": 2,
                        "scope": scope,
                        "updated_at_ms": 2000,
                    },
                ]
            )

            summaries = [record for record in adapter.read_all() if record.get("record_type") == "context_summary"]
            self.assertEqual(1, len(summaries))
            self.assertEqual("new session summary", summaries[0]["summary_text"])
            self.assertNotIn("summary_version_hash", summaries[0])

    def test_local_temporalstore_restart_reloads_hot_state_from_disk(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "events.jsonl"
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "codex_user",
                "session_id": "reload-test",
            }
            writer = mcp.MatrixArkLocalAdapter(store_path)
            writer.append_many(
                [
                    {
                        "record_type": "context_node",
                        "node_hash": 501,
                        "scope": scope,
                        "updated_at_ms": 1780000000000,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": 601,
                        "node_hash": 501,
                        "scope": scope,
                        "text": "disk backed event survives restart",
                        "updated_at_ms": 1780000000001,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": 701,
                        "entity_type": "decision",
                        "entity_name": "fallback",
                        "scope": scope,
                        "state": "load this entity back into memory",
                        "updated_at_ms": 1780000000002,
                    },
                ]
            )

            restarted = mcp.MatrixArkLocalAdapter(store_path)
            result = restarted.reload_context_hot_state_from_disk(scope=scope)

            self.assertEqual(result["status"], "reloaded")
            self.assertEqual(result["records_scanned"], 3)
            self.assertEqual(result["records_warmed"], 3)
            self.assertEqual(result["context_events_loaded"], 1)
            self.assertIn(501, restarted._context_node_hashes)
            self.assertIn(601, restarted._context_event_by_hash)
            self.assertIn(701, restarted._latest_entity_by_hash)

    def test_temporalstore_direct_writes_context_summary_as_latest_state(self) -> None:
        client = _HashStoreClient()
        adapter = _direct_adapter_for_hash_store(client)

        adapter.append_many(
            [
                {
                    "record_type": "context_summary",
                    "summary_type": "node_l0",
                    "summary_hash": 44,
                    "node_hash": 44,
                    "summary_text": "old node summary",
                    "summary_version_hash": 1,
                    "updated_at_ms": 1000,
                },
                {
                    "record_type": "context_summary",
                    "summary_type": "node_l0",
                    "summary_hash": 44,
                    "node_hash": 44,
                    "summary_text": "new node summary",
                    "summary_version_hash": 2,
                    "updated_at_ms": 2000,
                },
            ]
        )

        self.assertEqual("0", client.get_string("matrixark:test:record_count"))
        latest_rows = client.scan_hash("matrixark:test:context_latest_state")["records"]
        self.assertEqual(1, len(latest_rows))
        latest_payload = json.loads(latest_rows[0]["value"])
        self.assertEqual("new node summary", latest_payload["summary_text"])
        self.assertNotIn("summary_version_hash", latest_payload)

        records = adapter.read_all()
        summaries = [record for record in records if record.get("record_type") == "context_summary"]
        self.assertEqual(1, len(summaries))
        self.assertEqual("new node summary", summaries[0]["summary_text"])

    def test_context_events_do_not_duplicate_summaries_or_embeddings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            result = adapter.ingest(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Alice approved the Project Aurora GPU request after finance review.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_local",
                        "tenant_id": "tenant_codex",
                        "user_id": "codex_user",
                        "session_id": "summary-split-test",
                    },
                }
            )
            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            events = [record for record in records if record.get("record_type") == "context_event"]
            summaries = [record for record in records if record.get("record_type") == "context_summary"]
            embeddings = [record for record in records if record.get("record_type") == "context_embedding"]

            self.assertTrue(events)
            self.assertTrue(summaries)
            self.assertTrue(embeddings)
            self.assertTrue(any(record.get("summary_type") == "session_l0" for record in summaries))
            self.assertTrue(any(record.get("embedding_type") == "session_l0" for record in embeddings))
            self.assertTrue(any(record.get("embedding_type") == "event_text" for record in embeddings))
            for event in events:
                self.assertNotIn("summary_text", event)
                self.assertNotIn("summary_embedding", event)
                self.assertNotIn("vector", event)
                self.assertNotIn("embedding", event)
            for summary in summaries:
                self.assertIn("summary_text", summary)
                self.assertNotIn("vector", summary)
            for embedding in embeddings:
                self.assertIn("vector", embedding)
                self.assertNotIn("summary_text", embedding)

