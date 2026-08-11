# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_BackendPolicyPart2 methods split from test_matrixark_mcp_backend_policy.MatrixArkMcpBackendPolicyTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_mcp_backend_policy import (
    Path,
    _HashStoreClient,
    _NativeAppendClient,
    _direct_adapter_for_hash_store,
    json,
    mcp,
    mcp_core,
    mcp_local,
    os,
    tempfile,
    threading,
)
except ImportError:
    from test_matrixark_mcp_backend_policy import (
    Path,
    _HashStoreClient,
    _NativeAppendClient,
    _direct_adapter_for_hash_store,
    json,
    mcp,
    mcp_core,
    mcp_local,
    os,
    tempfile,
    threading,
)


class _BackendPolicyPart2:
    def test_user_directive_entities_promote_to_profile_on_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "codex_user",
                "session_id": "profile-promotion-session",
            }
            result = adapter.batch_extract(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember: use Ubuntu /opt/github-services for all TemporalStore repos.",
                        }
                    ],
                    "scope": scope,
                    "skip_prior_context": True,
                    "threshold_messages": 1,
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
            self.assertTrue(result["profile_promotion_scope_available"])
            self.assertGreaterEqual(result["entities_written"], 1)
            self.assertEqual(result["entities_written"], result["profile_entities_written"])

            records = adapter.read_all()
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
            ]
            self.assertTrue(profile_entities)
            profile_entity = profile_entities[0]
            self.assertEqual("cross_session", profile_entity["session_continuity"])
            self.assertEqual(["profile-promotion-session"], profile_entity["source_session_ids"])
            self.assertEqual("acct_local", profile_entity["access_scope"]["account_id"])
            self.assertEqual("tenant_codex", profile_entity["access_scope"]["tenant_id"])
            self.assertEqual("codex_user", profile_entity["access_scope"]["user_id"])
            self.assertNotIn("session_id", profile_entity["access_scope"])
            self.assertIn("/opt/github-services", profile_entity["state"])
            self.assertTrue(
                any(
                    record.get("record_type") == "context_embedding"
                    and record.get("ref_hash") == profile_entity["entity_hash"]
                    for record in records
                )
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_index"
                    and record.get("data_model") == "context_profile_entity"
                    and profile_entity["entity_hash"] in record.get("ref_hashes", [])
                    for record in records
                )
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    for record in records
                )
            )

    def test_historical_batch_extraction_still_promotes_profile_entities(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            historical_time_ms = 1700000000000
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "codex_user",
                "session_id": "historical-profile-promotion-session",
            }
            result = adapter.batch_extract(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember: historical Codex imports should still update profile memory.",
                        }
                    ],
                    "scope": scope,
                    "skip_prior_context": True,
                    "threshold_messages": 1,
                    "ingestion_time_ms": historical_time_ms,
                }
            )

            self.assertEqual("accepted", result["status"])
            self.assertEqual("always_when_profile_scope_available", result["profile_promotion_policy"])
            self.assertTrue(result["profile_promotion_scope_available"])
            self.assertEqual(result["entities_written"], result["profile_entities_written"])

            records = adapter.read_all()
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(profile_entities)
            profile_entity = profile_entities[0]
            self.assertEqual(historical_time_ms, profile_entity["updated_at_ms"])
            self.assertEqual(["historical-profile-promotion-session"], profile_entity["source_session_ids"])
            self.assertNotIn("session_id", profile_entity["access_scope"])
            self.assertIn("historical Codex imports", profile_entity["state"])
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    and record.get("updated_at_ms") == historical_time_ms
                    for record in records
                )
            )

    def test_context_node_topology_records_store_compact_scope_only(self) -> None:
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

            adapter.ensure_context_node_path(
                node_path=["tenant:tenant_codex", "user:codex_user", "session:debug-message-pdf-session"],
                scope=scope,
                updated_at_ms=1780000000000,
            )

            topology_records = [
                record
                for record in adapter.read_all()
                if record.get("record_type") in {"context_node", "context_child_ref"}
            ]
            self.assertTrue(topology_records)
            for record in topology_records:
                self.assertEqual(scope["scope_key"], record.get("scope_key"))
                self.assertEqual(1780000000000, record.get("updated_at_ms"))
                self.assertNotIn("created_at_ms", record)
                self.assertNotIn("depth", record)
                self.assertNotIn("scope", record)
                for duplicate_field in (
                    "account_id",
                    "tenant_id",
                    "user_id",
                    "session_id",
                    "tenant_hash",
                    "user_hash",
                    "session_hash",
                ):
                    self.assertNotIn(duplicate_field, record)
                if record.get("record_type") == "context_node":
                    self.assertNotIn("node_name", record)
                if record.get("record_type") == "context_child_ref":
                    self.assertNotIn("parent_path", record)
                    self.assertNotIn("child_path", record)
                    self.assertNotIn("child_name", record)

    def test_context_node_embeddings_are_generated_by_embedding_model(self) -> None:
        calls: list[str] = []

        def fake_embedding_for_text(text: str) -> list[float]:
            calls.append(text)
            return [0.25, 0.75]

        old_embedding_for_text = mcp_local.embedding_for_text
        old_embedding_model_name = mcp_local.embedding_model_name
        mcp_local.embedding_for_text = fake_embedding_for_text
        mcp_local.embedding_model_name = lambda: "unit-node-embedding-model"
        self.addCleanup(lambda: setattr(mcp_local, "embedding_for_text", old_embedding_for_text))
        self.addCleanup(lambda: setattr(mcp_local, "embedding_model_name", old_embedding_model_name))

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {"tenant_id": "tenant_codex", "user_id": "codex_user", "session_id": "node-embedding-test"}
            result = adapter.ensure_context_node_path(
                node_path=["tenant:tenant_codex", "user:codex_user", "session:node-embedding-test"],
                scope=scope,
                updated_at_ms=1780000000000,
            )
            second_result = adapter.ensure_context_node_path(
                node_path=["tenant:tenant_codex", "user:codex_user", "session:node-embedding-test"],
                scope=scope,
                updated_at_ms=1780000001000,
            )

            records = adapter.read_all()
            node_embeddings = [
                record
                for record in records
                if record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "context_node"
                and record.get("ref_type") == "node"
            ]

            self.assertEqual(3, result["node_embeddings_created"])
            self.assertEqual(0, second_result["node_embeddings_created"])
            self.assertEqual(3, len(node_embeddings))
            self.assertEqual(3, len(calls))
            self.assertTrue(all(record.get("vector") == [0.25, 0.75] for record in node_embeddings))
            self.assertTrue(all(record.get("dim") == 2 for record in node_embeddings))
            self.assertTrue(all(record.get("model") == "unit-node-embedding-model" for record in node_embeddings))
            self.assertTrue(
                all(
                    record.get("model_ref") == mcp.embedding_model_ref_for_name("unit-node-embedding-model")
                    for record in node_embeddings
                )
            )
            self.assertTrue(any("session:node-embedding-test" in text for text in calls))

    def test_context_node_embedding_regenerates_stale_model_ref(self) -> None:
        calls: list[str] = []

        def fake_embedding_for_text(text: str) -> list[float]:
            calls.append(text)
            return [0.5, 0.6, 0.7]

        old_embedding_for_text = mcp_local.embedding_for_text
        old_embedding_model_name = mcp_local.embedding_model_name
        mcp_local.embedding_for_text = fake_embedding_for_text
        mcp_local.embedding_model_name = lambda: "unit-node-current-model"
        self.addCleanup(lambda: setattr(mcp_local, "embedding_for_text", old_embedding_for_text))
        self.addCleanup(lambda: setattr(mcp_local, "embedding_model_name", old_embedding_model_name))

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {"tenant_id": "tenant_codex", "user_id": "codex_user", "session_id": "node-stale-embedding-test"}
            node_path = ["tenant:tenant_codex", "user:codex_user", "session:node-stale-embedding-test"]
            node_hash = mcp_core.stable_hash("/".join(node_path))
            adapter.append_many(
                [
                    {
                        "record_type": "context_node",
                        "node_hash": node_hash,
                        "node_name": "session:node-stale-embedding-test",
                        "node_path": node_path,
                        "depth": len(node_path),
                        "scope": scope,
                        "updated_at_ms": 1780000000000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "context_node",
                        "ref_type": "node",
                        "ref_hash": node_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "model": "legacy-node-model",
                        "vector": [0.1, 0.2],
                        "scope": scope,
                        "updated_at_ms": 1780000000000,
                    },
                ]
            )

            result = adapter.ensure_context_node_path(
                node_path=node_path,
                scope=scope,
                updated_at_ms=1780000001000,
            )

            records = adapter.read_all()
            current_model_ref = mcp.embedding_model_ref_for_name("unit-node-current-model")
            current_embeddings = [
                record
                for record in records
                if record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "context_node"
                and record.get("ref_type") == "node"
                and record.get("ref_hash") == node_hash
                and record.get("model_ref") == current_model_ref
            ]

            self.assertEqual(3, result["node_embeddings_created"])
            self.assertEqual(3, len(calls))
            self.assertEqual(1, len(current_embeddings))
            self.assertEqual([0.5, 0.6, 0.7], current_embeddings[0]["vector"])
            self.assertEqual(3, current_embeddings[0]["dim"])
            self.assertEqual("unit-node-current-model", current_embeddings[0]["model"])
            self.assertEqual("context_node", current_embeddings[0]["source_record_type"])
            self.assertEqual(1780000001000, current_embeddings[0]["source_updated_at_ms"])

    def test_embedding_backfill_maps_all_context_source_records(self) -> None:
        calls: list[str] = []

        def fake_embeddings_for_texts(texts: list[str]) -> list[list[float]]:
            calls.extend(texts)
            return [[round(index + 0.1, 1), round(index + 0.2, 1)] for index, _text in enumerate(texts)]

        old_embeddings_for_texts = mcp_local.embeddings_for_texts
        old_embedding_model_name = mcp_local.embedding_model_name
        mcp_local.embeddings_for_texts = fake_embeddings_for_texts
        mcp_local.embedding_model_name = lambda: "unit-completeness-model"
        self.addCleanup(lambda: setattr(mcp_local, "embeddings_for_texts", old_embeddings_for_texts))
        self.addCleanup(lambda: setattr(mcp_local, "embedding_model_name", old_embedding_model_name))

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {"tenant_id": "tenant_codex", "user_id": "codex_user", "session_id": "embedding-completeness-test"}
            node_path = ["tenant:tenant_codex", "user:codex_user", "session:embedding-completeness-test"]
            node_hash = mcp_core.stable_hash("/".join(node_path))
            summary_hash = mcp_core.stable_hash("summary-needs-refresh")
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 11,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "event text should be embedded",
                        "scope": scope,
                        "updated_at_ms": 100,
                    },
                    {
                        "record_type": "context_segment",
                        "segment_hash": 22,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "topic": "segment topic",
                        "summary_text": "segment summary should be embedded",
                        "scope": scope,
                        "updated_at_ms": 110,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": 33,
                        "entity_type": "preference",
                        "entity_name": "editor",
                        "state": "prefers precise answers",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": scope,
                        "updated_at_ms": 120,
                    },
                    {
                        "record_type": "context_node",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "node_name": "session:embedding-completeness-test",
                        "depth": len(node_path),
                        "scope": scope,
                        "updated_at_ms": 130,
                    },
                    {
                        "record_type": "context_summary",
                        "summary_type": "node_l0",
                        "summary_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_text": "fresh summary should replace stale embedding",
                        "scope": scope,
                        "updated_at_ms": 200,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "node_l0",
                        "ref_type": "summary",
                        "ref_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "model": "unit-completeness-model",
                        "vector": [9.9, 9.8],
                        "scope": scope,
                        "updated_at_ms": 100,
                    },
                    {
                        "record_type": "context_compression_event",
                        "compression_id_hash": 44,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_text": "compression summary should be embedded",
                        "scope": scope,
                        "updated_at_ms": 140,
                    },
                ]
            )

            result = adapter.ensure_context_embeddings(limit=16, updated_at_ms=300)
            records = adapter.read_all()
            embeddings = {
                (record.get("embedding_type"), record.get("ref_type"), record.get("ref_hash")): record
                for record in records
                if record.get("record_type") == "context_embedding"
            }

            expected_keys = {
                ("event_text", "event", 11),
                ("segment_text", "segment", 22),
                ("profile_entity_state", "entity", 33),
                ("context_node", "node", node_hash),
                ("node_l0", "summary", summary_hash),
                ("compression_summary", "compression", 44),
            }
            self.assertEqual(6, result["generated_count"])
            self.assertEqual(expected_keys, set(embeddings).intersection(expected_keys))
            self.assertEqual(6, len(calls))
            self.assertEqual([4.1, 4.2], embeddings[("node_l0", "summary", summary_hash)]["vector"])
            self.assertEqual(300, embeddings[("node_l0", "summary", summary_hash)]["updated_at_ms"])
            self.assertTrue(
                all(
                    embeddings[key].get("model_ref") == mcp.embedding_model_ref_for_name("unit-completeness-model")
                    for key in expected_keys
                )
            )

    def test_refresh_summaries_runs_embedding_completeness_after_dirty_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {"tenant_id": "tenant_codex", "user_id": "codex_user", "session_id": "dirty-refresh-embedding-test"}
            node_path = ["tenant:tenant_codex", "user:codex_user", "session:dirty-refresh-embedding-test"]
            node_hash = mcp_core.stable_hash("/".join(node_path))
            event_hash = mcp_core.stable_hash("dirty-refresh-source-event")
            adapter.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": event_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "text": "dirty summary refresh source event",
                    "scope": scope,
                    "updated_at_ms": 1780000000000,
                }
            )
            adapter.mark_node_summary_dirty(
                node_path=node_path,
                scope=scope,
                updated_at_ms=1780000000001,
                source_ref_type="event",
                source_hash_field="source_event_hash",
                source_hash=event_hash,
                dirty_reason="unit_dirty_summary",
            )
            calls: list[dict] = []
            adapter.ensure_context_embeddings = lambda **kwargs: calls.append(dict(kwargs)) or {
                "status": "ok",
                "generated_count": 3,
            }

            result = adapter.refresh_summaries(
                {
                    "scope": scope,
                    "limit": 4,
                    "refreshed_at_ms": 1780000000100,
                    "embedding_backfill_limit": 12,
                }
            )

            self.assertGreaterEqual(result["refreshed_count"], 1)
            self.assertEqual({"status": "ok", "generated_count": 3}, result["embedding_refresh"])
            self.assertEqual(1, len(calls))
            self.assertEqual(scope, calls[0]["scope"])
            self.assertEqual(12, calls[0]["limit"])
            self.assertEqual(1780000000100, calls[0]["updated_at_ms"])

    def test_parent_summary_uses_child_summaries_and_state_not_recursive_raw_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp_core.enrich_scope_with_identity(
                {"session_id": "parent-summary-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )
            parent_path = ["tenant:tenant_codex", "user:codex_user"]
            child_path = [*parent_path, "session:parent-summary-session"]
            parent_hash = mcp_core.stable_hash("/".join(parent_path))
            child_hash = mcp_core.stable_hash("/".join(child_path))
            child_summary_hash = mcp_core.stable_hash("child-summary")
            entity_hash = mcp_core.stable_hash("gpu-approval-entity")
            compression_hash = mcp_core.stable_hash("old-approval-compression")
            dirty_hash = mcp_core.stable_hash("parent-summary-dirty")

            adapter.ensure_context_node_path(
                node_path=child_path,
                scope=scope,
                updated_at_ms=1780000000000,
            )
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": child_summary_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "summary_text": "Child summary says finance approved the GPU budget.",
                        "source_roles": ["assistant"],
                        "source_hook_types": ["hook_boundary"],
                        "source_codex_events": ["Stop"],
                        "scope": scope,
                        "updated_at_ms": 1780000000100,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": entity_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "entity_type": "approval_state",
                        "entity_name": "gpu_purchase",
                        "state": "Alice approved the GPU purchase after finance review.",
                        "source_roles": ["user"],
                        "source_hook_types": ["live_ingest"],
                        "source_codex_events": ["UserPromptSubmit"],
                        "scope": scope,
                        "updated_at_ms": 1780000000200,
                    },
                    {
                        "record_type": "context_compression_event",
                        "compression_id_hash": compression_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "operator": "TIME_COMPRESS",
                        "summary_text": "Older compressed context covers earlier GPU review notes.",
                        "scope": scope,
                        "updated_at_ms": 1780000000300,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": mcp_core.stable_hash("raw-leaf-event"),
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "text": "RAW_LEAF_SHOULD_NOT_APPEAR_IN_PARENT_SUMMARY",
                        "source_role": "tool",
                        "hook_type": "hook_boundary",
                        "codex_event": "PostToolUse",
                        "scope": scope,
                        "updated_at_ms": 1780000000400,
                    },
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_hash": parent_hash,
                        "node_path": parent_path,
                        "dirty_reason": "child_update",
                        "source_ref_type": "summary",
                        "source_summary_hash": child_summary_hash,
                        "scope": scope,
                        "status": "pending",
                        "updated_at_ms": 1780000000500,
                    },
                ]
            )

            result = adapter.refresh_dirty_node_summaries(scope=scope, limit=4, refreshed_at_ms=1780000000600)
            self.assertEqual("ok", result["status"])
            self.assertEqual(1, result["refreshed_count"])

            records = adapter.read_all()
            parent_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_hash") == parent_hash
                and record.get("dirty_hash") == dirty_hash
            ]
            self.assertTrue(parent_summaries)
            combined_summary_text = " ".join(str(record.get("summary_text", "")) for record in parent_summaries)
            self.assertIn("Child summary says finance approved the GPU budget", combined_summary_text)
            self.assertIn("Alice approved the GPU purchase", combined_summary_text)
            self.assertIn("Older compressed context", combined_summary_text)
            self.assertNotIn("RAW_LEAF_SHOULD_NOT_APPEAR", combined_summary_text)

            for summary in parent_summaries:
                policy = summary["summary_generation_policy"]
                self.assertEqual("child_summaries_plus_state", policy["source_policy"])
                self.assertFalse(policy["raw_recursive_leaf_event_scan"])
                self.assertEqual([], summary["source_event_ids"])
                self.assertIn(child_summary_hash, summary["source_summary_hashes"])
                self.assertIn(entity_hash, summary["source_entity_hashes"])
                self.assertIn(compression_hash, summary["source_operator_hashes"])
                self.assertEqual(["assistant", "user"], summary["source_roles"])
                self.assertEqual(["hook_boundary", "live_ingest"], summary["source_hook_types"])
                self.assertEqual(["Stop", "UserPromptSubmit"], summary["source_codex_events"])

    def test_leaf_summary_preserves_raw_event_hook_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp_core.enrich_scope_with_identity(
                {"session_id": "leaf-summary-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )
            node_path = ["tenant:tenant_codex", "user:codex_user", "session:leaf-summary-session"]
            node_hash = mcp_core.stable_hash("/".join(node_path))
            dirty_hash = mcp_core.stable_hash("leaf-summary-dirty")
            adapter.ensure_context_node_path(
                node_path=node_path,
                scope=scope,
                updated_at_ms=1780000000000,
            )
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": mcp_core.stable_hash("leaf-user-event"),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "User asked whether memory should wait for Stop.",
                        "source_role": "user",
                        "hook_type": "live_ingest",
                        "codex_event": "UserPromptSubmit",
                        "scope": scope,
                        "updated_at_ms": 1780000000100,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": mcp_core.stable_hash("leaf-tool-event"),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "Tool evidence: Exit code: 0 proved threshold commit worked.",
                        "source_role": "tool",
                        "hook_type": "hook_boundary",
                        "codex_event": "PostToolUse",
                        "scope": scope,
                        "updated_at_ms": 1780000000200,
                    },
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dirty_reason": "new_event",
                        "source_ref_type": "event",
                        "scope": scope,
                        "status": "pending",
                        "updated_at_ms": 1780000000300,
                    },
                ]
            )

            result = adapter.refresh_dirty_node_summaries(scope=scope, limit=4, refreshed_at_ms=1780000000400)
            self.assertEqual("ok", result["status"])
            summaries = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_summary" and record.get("dirty_hash") == dirty_hash
            ]
            self.assertTrue(summaries)
            for summary in summaries:
                self.assertEqual(["tool", "user"], summary["source_roles"])
                self.assertEqual(["hook_boundary", "live_ingest"], summary["source_hook_types"])
                self.assertEqual(["PostToolUse", "UserPromptSubmit"], summary["source_codex_events"])
            summary_hashes = {record["summary_hash"] for record in summaries}
            summary_embeddings = [
                record
                for record in adapter.read_all()
                if record.get("record_type") == "context_embedding"
                and record.get("ref_type") == "summary"
                and record.get("ref_hash") in summary_hashes
            ]
            self.assertTrue(summary_embeddings)
            for embedding in summary_embeddings:
                for field in [
                    "source_event_ids",
                    "source_roles",
                    "source_role_counts",
                    "source_hook_types",
                    "source_hook_type_counts",
                    "source_codex_events",
                    "source_codex_event_counts",
                    "source_summary_hashes",
                    "source_entity_hashes",
                    "source_operator_hashes",
                    "summary_generation_policy",
                    "dirty_hash",
                    "extraction_phase",
                    "final_session_boundary",
                ]:
                    self.assertNotIn(field, embedding)

    def test_context_summary_secondary_terms_include_hook_provenance(self) -> None:
        terms = mcp_core.candidate_index_terms(
            {
                "record_type": "context_summary",
                "summary_type": "node_l0",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "source_entity_types": ["assistant_decision", "tool_evidence"],
                "source_roles": ["assistant", "tool"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["Stop", "PostToolUse"],
            },
            {},
            {},
            {},
        )

        self.assertIn("summary_type:node_l0", terms)
        self.assertIn("entity_type:assistant_decision", terms)
        self.assertIn("entity_type:tool_evidence", terms)
        self.assertIn("source_role:assistant", terms)
        self.assertIn("source_role:tool", terms)
        self.assertIn("hook_type:hook_boundary", terms)
        self.assertIn("codex_event:stop", terms)
        self.assertIn("codex_event:posttooluse", terms)
        self.assertIn("memory_scope:user_profile", terms)
        self.assertIn("session_continuity:cross_session", terms)
        self.assertIn("extraction_phase:final", terms)

    def test_production_profile_rejects_local_storage(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = False
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("local"))
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("temporalstore-local"))

    def test_production_profile_allows_native_backends(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = False
        mcp.validate_mcp_backend_policy(self._args("temporalstore-direct"))
        mcp.validate_mcp_backend_policy(self._args("temporalstore-rust"))
        mcp.validate_mcp_backend_policy(self._args("temporalstore-rust-direct"))

    def test_debug_override_does_not_restore_jsonl_storage(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = True
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("local"))

    def test_local_jsonl_guardrails_strip_bulky_fields_by_default(self) -> None:
        mcp_local.LOCAL_JSONL_INCLUDE_BULKY_FIELDS = False
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.append(
                {
                    "record_type": "agent_message",
                    "role": "tool",
                    "text": "compact tool result summary",
                    "raw_payload": {"very": "large"},
                    "debug_payload": {"trace": "large"},
                    "tool_result": "x" * 1024,
                    "updated_at_ms": 1780000000000,
                }
            )

            records = adapter.read_all()
            self.assertEqual(1, len(records))
            self.assertEqual("tool", records[0]["role"])
            self.assertEqual("compact tool result summary", records[0]["text"])
            self.assertNotIn("raw_payload", records[0])
            self.assertNotIn("debug_payload", records[0])
            self.assertNotIn("tool_result", records[0])
            self.assertEqual(
                ["debug_payload", "raw_payload", "tool_result"],
                records[0]["jsonl_guardrails"]["dropped_bulky_fields"],
            )

    def test_local_jsonl_can_keep_bulky_fields_for_explicit_debug(self) -> None:
        mcp_local.LOCAL_JSONL_INCLUDE_BULKY_FIELDS = True
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.append(
                {
                    "record_type": "agent_message",
                    "role": "tool",
                    "text": "debug capture",
                    "raw_payload": {"keep": True},
                    "debug_payload": {"keep": True},
                    "updated_at_ms": 1780000000000,
                }
            )

            records = adapter.read_all()
            self.assertEqual({"keep": True}, records[0]["raw_payload"])
            self.assertEqual({"keep": True}, records[0]["debug_payload"])

    def test_local_jsonl_rotates_and_retains_by_count(self) -> None:
        mcp_local.LOCAL_JSONL_MAX_BYTES = 180
        mcp_local.LOCAL_JSONL_RETENTION_COUNT = 2
        mcp_local.LOCAL_JSONL_RETENTION_AGE_MS = 7 * 24 * 60 * 60 * 1000
        with tempfile.TemporaryDirectory() as tmpdir:
            event_log = Path(tmpdir) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(event_log)
            for index in range(4):
                adapter.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": index,
                        "text": f"retained event {index} " + ("x" * 80),
                        "updated_at_ms": 1780000000000 + index,
                    }
                )

            retained_files = sorted(path.name for path in Path(tmpdir).glob("events.jsonl*"))
            self.assertLessEqual(len(retained_files), 2)
            self.assertIn("events.jsonl", retained_files)
            self.assertIn("events.jsonl.1", retained_files)
            retained_ids = [record["event_id_hash"] for record in adapter.read_all()]
            self.assertEqual([2, 3], retained_ids)

    def test_local_jsonl_retains_by_age(self) -> None:
        mcp_local.LOCAL_JSONL_MAX_BYTES = 180
        mcp_local.LOCAL_JSONL_RETENTION_COUNT = 3
        mcp_local.LOCAL_JSONL_RETENTION_AGE_MS = 7 * 24 * 60 * 60 * 1000
        with tempfile.TemporaryDirectory() as tmpdir:
            event_log = Path(tmpdir) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(event_log)
            adapter.append({"record_type": "context_event", "event_id_hash": 1, "text": "x" * 220})
            adapter.append({"record_type": "context_event", "event_id_hash": 2, "text": "x" * 220})
            rotated = event_log.with_name("events.jsonl.1")
            self.assertTrue(rotated.exists())
            os.utime(rotated, (0, 0))
            mcp_local.LOCAL_JSONL_RETENTION_AGE_MS = 1
            with adapter._event_log_lock:
                adapter._prune_jsonl_retention_locked()
            self.assertFalse(rotated.exists())
            mcp_local.LOCAL_JSONL_RETENTION_AGE_MS = 7 * 24 * 60 * 60 * 1000
            adapter.append({"record_type": "context_event", "event_id_hash": 3, "text": "tiny"})
            self.assertEqual([2, 3], [record["event_id_hash"] for record in adapter.read_all()])

    def test_local_jsonl_reports_testing_debug_guardrails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            guardrails = adapter.backend_metrics()["metrics"]["jsonl_guardrails"]
            self.assertEqual("testing_debug_only", guardrails["usage"])
            self.assertIn("max_bytes", guardrails)
            self.assertIn("retention_count", guardrails)
            self.assertIn("retention_age_ms", guardrails)

    def test_backend_readiness_default_policy(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = ""
        self.assertTrue(mcp.backend_ready_required("temporalstore-rust"))
        self.assertTrue(mcp.backend_ready_required("temporalstore-rust-direct"))
        self.assertTrue(mcp.backend_ready_required("temporalstore-direct"))
        self.assertFalse(mcp.backend_ready_required("local"))
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = "1"
        self.assertTrue(mcp.backend_ready_required("local"))

    def test_context_serving_records_share_stable_placement_route(self) -> None:
        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        node_hash = 44
        records = [
            {"record_type": "context_event", "event_id_hash": 1, "scope": scope, "node_hash": node_hash, "updated_at_ms": 1780000000000, "text": "event"},
            {"record_type": "context_entity", "entity_hash": 2, "scope": scope, "node_hash": node_hash, "state": "entity"},
            {"record_type": "context_segment", "segment_hash": 3, "scope": scope, "node_hash": node_hash, "text": "segment"},
            {"record_type": "context_embedding", "ref_hash": 4, "scope": scope, "node_hash": node_hash, "embedding": [0.1]},
            {"record_type": "resource_chunk", "chunk_hash": 5, "scope": scope, "node_hash": node_hash, "text": "chunk"},
            {"record_type": "skill_section", "section_hash": 6, "scope": scope, "node_hash": node_hash, "text": "skill"},
            {"record_type": "context_index", "index_name": "source_type:message", "ref_hash": 7, "scope": scope, "node_hash": node_hash},
        ]

        materialized = [
            record
            for record in mcp_core.materialize_serving_record_batch(records)
            if record.get("record_type") != "context_debug_record"
        ]

        placement_keys = {record.get("placement_key") for record in materialized}
        self.assertEqual(placement_keys, {"context:t=11|u=22|s=33|:node=44"})
        for record in materialized:
            route = record.get("storage_route")
            self.assertIsInstance(route, dict)
            self.assertEqual(route.get("placement_key"), record.get("placement_key"))
            self.assertEqual(route.get("routing_key"), record.get("placement_key"))
            self.assertEqual(route.get("partition_key"), record.get("placement_key"))
            self.assertEqual(route.get("colocation_group"), "matrixark_context")
            self.assertEqual(route.get("placement_hash"), record.get("placement_hash"))

    def test_native_context_pack_default_policy(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        self.assertTrue(mcp.native_context_pack_required("temporalstore-rust"))
        self.assertTrue(mcp.native_context_pack_required("temporalstore-rust-direct"))
        self.assertTrue(mcp.native_context_pack_required("temporalstore-direct"))
        self.assertFalse(mcp.native_context_pack_required("local"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = "0"
        self.assertFalse(mcp.native_context_pack_required("temporalstore-rust"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = "1"
        self.assertTrue(mcp.native_context_pack_required("local"))


    def test_python_hot_cache_default_policy(self) -> None:
        mcp.MATRIXARK_ALLOW_PYTHON_HOT_CACHE = ""
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-rust"))
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="local"))
        mcp.MATRIXARK_MCP_PROFILE = "production"
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="local"))
        mcp.MATRIXARK_ALLOW_PYTHON_HOT_CACHE = "1"
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))

    def test_native_candidate_prefilter_default_policy(self) -> None:
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = ""
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust-direct"))
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-direct"))
        self.assertFalse(mcp.native_candidate_prefilter_required_for_backend("local"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = "0"
        self.assertFalse(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = "1"
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))


    def test_direct_append_prefers_native_matrixark_batch_append_records(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-append"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._retrieval_candidate_cache = {}
        adapter._retrieval_candidate_cache_lock = threading.RLock()
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        adapter.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 123,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "node_hash": 44,
                    "storage_options": {"storage_family": "shared_store", "write_mode": "async"},
                    "updated_at_ms": 1780000000000,
                    "text": "native batch append works",
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 123 + mcp_core.CONTEXT_TIMELINE_FANOUT,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "node_hash": 44,
                    "storage_options": {"storage_family": "shared_store", "write_mode": "async"},
                    "updated_at_ms": 1780000000000,
                    "text": "same millisecond collision slot",
                },
            ]
        )

        self.assertEqual(len(client.calls), 2)
        raw_call = client.calls[0]
        self.assertEqual(raw_call["count_key"], "matrixark:test:native-append:raw_ingestion:record_count")
        self.assertEqual(raw_call["count_value"], "2")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_temporalstore_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "temporalstore")
        self.assertEqual(raw_call["append_options"]["source"], "matrixark_live_ingestion_dual_write")
        self.assertEqual({entry["key"] for entry in raw_call["entries"]}, {"matrixark:test:native-append:raw_ingestion:records:000000"})
        raw_payloads = [json.loads(entry["value"]) for entry in raw_call["entries"]]
        self.assertEqual([payload["text"] for payload in raw_payloads], ["native batch append works", "same millisecond collision slot"])
        self.assertTrue(all("placement_key" not in payload for payload in raw_payloads))

        call = client.calls[1]
        self.assertEqual(call["count_key"], "matrixark:test:native-append:record_count")
        self.assertEqual(call["count_value"], "1")
        self.assertEqual(call["append_options"]["append_path"], "native_append_queue")
        self.assertTrue(call["append_options"]["coalesce_writes"])
        self.assertEqual(call["append_options"]["route_by"], "placement_key")
        self.assertTrue(call["append_options"]["persist_from_storage_options"])
        self.assertEqual(call["append_options"]["hset_lowering"], "forbidden_for_parity")
        self.assertEqual(call["append_options"]["audit_hot_path"], "inline_counters_only")
        self.assertEqual(call["append_options"]["full_context_pack_audit"], "sample_or_enqueue_async_policy_enabled")
        keys = {entry["key"] for entry in call["entries"]}
        self.assertIn("matrixark:test:native-append:records:000000", keys)
        routed_entries = [entry for entry in call["entries"] if entry.get("storage_route", {}).get("placement_key")]
        self.assertTrue(routed_entries)
        for entry in routed_entries:
            route = entry["storage_route"]
            self.assertEqual(route["placement_key"], "context:scope:node=44")
            self.assertEqual(route["routing_key"], "context:scope:node=44")
            self.assertEqual(route["write_mode"], "async")
            self.assertTrue(route["background_write"])
        self.assertTrue(any("context_event_by_ingestion_time" in key for key in keys))
        time_index_entries = [entry for entry in call["entries"] if "context_event_by_ingestion_time" in entry["key"]]
        self.assertEqual(len(time_index_entries), 2)
        time_index_payloads = [json.loads(entry["value"]) for entry in time_index_entries]
        self.assertEqual({payload["record_type"] for payload in time_index_payloads}, {"context_event_ref"})
        self.assertEqual({payload["ref_hash"] for payload in time_index_payloads}, {123, 123 + mcp_core.CONTEXT_TIMELINE_FANOUT})
        self.assertEqual({payload["timestamp_key_ms"] for payload in time_index_payloads}, {1780000000000})
        self.assertEqual(len({payload["context_event_key"] for payload in time_index_payloads}), 2)
        self.assertTrue(all("text" not in payload for payload in time_index_payloads))

    def test_direct_append_shadows_records_to_disk_fallback_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "fallback.jsonl"
            adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(store_path)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_write_failures = 0

            adapter._append_disk_fallback_records(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 808,
                        "scope": {"tenant_id": "tenant_codex", "session_id": "shadow-test"},
                        "text": "shadow copy survives native crash",
                        "updated_at_ms": 1780000000100,
                    }
                ]
            )

            reloaded = mcp.MatrixArkLocalAdapter(store_path)
            records = reloaded.read_all()
            self.assertEqual(0, adapter._disk_fallback_write_failures)
            self.assertEqual(1, len(records))
            self.assertEqual("shadow copy survives native crash", records[0]["text"])

    def test_direct_append_dual_writes_raw_and_serving_records(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:dual-write"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._retrieval_candidate_cache = {}
        adapter._retrieval_candidate_cache_lock = threading.RLock()
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        raw_record = {
            "record_type": "context_event",
            "event_id_hash": 991,
            "tenant_hash": 7,
            "scope_key": "tenant=7",
            "node_hash": 9,
            "updated_at_ms": 1780000001000,
            "text": "raw must stay canonical",
            "internal_extraction": {"classification": "memory", "status": "observed"},
        }
        adapter.append(raw_record)

        self.assertEqual(len(client.calls), 2)
        raw_call, serving_call = client.calls
        self.assertEqual(raw_call["count_key"], "matrixark:test:dual-write:raw_ingestion:record_count")
        self.assertEqual(raw_call["count_value"], "1")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_temporalstore_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "temporalstore")
        self.assertEqual(raw_call["entries"][0]["key"], "matrixark:test:dual-write:raw_ingestion:records:000000")
        raw_payload = json.loads(raw_call["entries"][0]["value"])
        self.assertEqual(raw_payload["text"], "raw must stay canonical")
        self.assertIn("internal_extraction", raw_payload)
        self.assertNotIn("placement_key", raw_payload)

        self.assertEqual(serving_call["count_key"], "matrixark:test:dual-write:record_count")
        self.assertEqual(serving_call["count_value"], "1")
        serving_payloads = [json.loads(entry["value"]) for entry in serving_call["entries"] if entry["key"].endswith(":records:000000")]
        self.assertEqual(len(serving_payloads), 1)
        self.assertEqual(serving_payloads[0]["record_type"], "context_event")
        self.assertEqual(serving_payloads[0]["placement_key"], "context:tenant=7:node=9")

    def test_direct_fast_paths_shadow_raw_and_serving_records_to_disk_fallback_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_log = Path(tmpdir) / "fallback.jsonl"
            client = _HashStoreClient()
            adapter = _direct_adapter_for_hash_store(client)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(fallback_log)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_recovery_in_progress = False

            raw_record = {
                "record_type": "agent_message",
                "role": "tool",
                "text": "tool returned message must shadow to disk",
                "updated_at_ms": 1780000004000,
            }
            serving_record = {
                "record_type": "context_event",
                "event_id_hash": 994,
                "tenant_hash": 9,
                "scope": {"tenant_hash": 9},
                "scope_key": mcp_core.scope_key_from_hashes(9, 0, 0),
                "node_hash": 10,
                "updated_at_ms": 1780000005000,
                "text": "serving context should shadow to disk",
            }

            adapter._append_raw_ingestion_records([raw_record], allow_queue=False)
            adapter._append_many_materialized([serving_record], allow_queue=False)

            fallback_records = mcp.MatrixArkLocalAdapter(fallback_log).read_all()
            self.assertTrue(any(record.get("role") == "tool" for record in fallback_records))
            self.assertTrue(any(record.get("event_id_hash") == 994 for record in fallback_records))

    def test_direct_disk_fallback_recovery_rebuilds_serving_count_and_skips_compressed_old_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_log = Path(tmpdir) / "fallback.jsonl"
            fallback = mcp.MatrixArkLocalAdapter(fallback_log)
            fallback.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 995,
                        "tenant_hash": 9,
                        "scope": {"tenant_hash": 9},
                        "scope_key": mcp_core.scope_key_from_hashes(9, 0, 0),
                        "node_hash": 10,
                        "updated_at_ms": 1780000006000,
                        "text": "recover me after native restart",
                    },
                    {
                        "record_type": "context_compression_event",
                        "event_id_hash": 996,
                        "tenant_hash": 9,
                        "updated_at_ms": 1780000007000,
                        "text": "compressed old data should not refill hot memory",
                    },
                ]
            )
            client = _HashStoreClient()
            adapter = _direct_adapter_for_hash_store(client)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(fallback_log)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_recovery_enabled = True
            adapter._disk_fallback_recovery_attempted = False
            adapter._disk_fallback_recovery_in_progress = False
            adapter._disk_fallback_recovery_status = {"status": "not_attempted"}

            report = adapter._recover_serving_from_disk_fallback_if_needed(reason="unit_restart")
            records = adapter.read_all_without_disk_fallback_recovery()

            self.assertEqual(report["status"], "recovered")
            self.assertEqual(report["recovered_records"], 1)
            self.assertEqual(client.strings[adapter._count_key], "1")
            self.assertTrue(any(record.get("event_id_hash") == 995 for record in records))
            self.assertFalse(any(record.get("record_type") == "context_compression_event" for record in records))

    def test_direct_disk_fallback_recovery_skips_shared_store_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_log = Path(tmpdir) / "fallback.jsonl"
            fallback = mcp.MatrixArkLocalAdapter(fallback_log)
            fallback.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": 997,
                    "tenant_hash": 9,
                    "scope": {"tenant_hash": 9},
                    "scope_key": mcp_core.scope_key_from_hashes(9, 0, 0),
                    "node_hash": 10,
                    "updated_at_ms": 1780000008000,
                    "text": "must not become authoritative in shared store mode",
                }
            )
            client = _HashStoreClient()
            adapter = _direct_adapter_for_hash_store(client)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(fallback_log)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_recovery_enabled = True
            adapter._disk_fallback_recovery_attempted = False
            adapter._disk_fallback_recovery_in_progress = False
            adapter._disk_fallback_recovery_status = {"status": "not_attempted"}
            adapter._storage_family = "shared_store"
            adapter._storage_mode = "shared_store"
            adapter._replication_mode = "shared_store"

            report = adapter._recover_serving_from_disk_fallback_if_needed(reason="unit_restart")

            self.assertEqual("skipped", report["status"])
            self.assertFalse(report["replay_gate"]["allowed"])
            self.assertEqual("distributed_storage_uses_replication_or_shared_store_recovery", report["replay_gate"]["skip_reason"])
            self.assertNotIn(adapter._count_key, client.strings)
            self.assertEqual([], adapter.read_all_without_disk_fallback_recovery())

    def test_direct_disk_fallback_recovery_allows_single_node_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_log = Path(tmpdir) / "fallback.jsonl"
            mcp.MatrixArkLocalAdapter(fallback_log).append(
                {
                    "record_type": "context_event",
                    "event_id_hash": 998,
                    "tenant_hash": 9,
                    "scope": {"tenant_hash": 9},
                    "scope_key": mcp_core.scope_key_from_hashes(9, 0, 0),
                    "node_hash": 10,
                    "updated_at_ms": 1780000009000,
                    "text": "single node can replay local guard",
                }
            )
            client = _HashStoreClient()
            adapter = _direct_adapter_for_hash_store(client)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(fallback_log)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_recovery_enabled = True
            adapter._disk_fallback_recovery_attempted = False
            adapter._disk_fallback_recovery_in_progress = False
            adapter._disk_fallback_recovery_status = {"status": "not_attempted"}
            adapter._storage_family = "local"
            adapter._storage_mode = "single_node"
            adapter._replication_mode = "none"

            report = adapter._recover_serving_from_disk_fallback_if_needed(reason="unit_restart")

            self.assertEqual("recovered", report["status"])
            self.assertEqual(1, report["recovered_records"])
            self.assertEqual(client.strings[adapter._count_key], "1")

    def test_direct_disk_fallback_recovery_override_allows_test_replay_any_mode(self) -> None:
        os.environ["MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE"] = "1"
        with tempfile.TemporaryDirectory() as tmpdir:
            fallback_log = Path(tmpdir) / "fallback.jsonl"
            mcp.MatrixArkLocalAdapter(fallback_log).append(
                {
                    "record_type": "context_event",
                    "event_id_hash": 999,
                    "tenant_hash": 9,
                    "scope": {"tenant_hash": 9},
                    "scope_key": mcp_core.scope_key_from_hashes(9, 0, 0),
                    "node_hash": 10,
                    "updated_at_ms": 1780000010000,
                    "text": "test override can replay shared store fallback",
                }
            )
            client = _HashStoreClient()
            adapter = _direct_adapter_for_hash_store(client)
            adapter._disk_fallback_adapter = None
            adapter._disk_fallback_path = str(fallback_log)
            adapter._disk_fallback_enabled = True
            adapter._disk_fallback_recovery_enabled = True
            adapter._disk_fallback_recovery_attempted = False
            adapter._disk_fallback_recovery_in_progress = False
            adapter._disk_fallback_recovery_status = {"status": "not_attempted"}
            adapter._storage_family = "shared_store"
            adapter._storage_mode = "shared_store"
            adapter._replication_mode = "shared_store"

            report = adapter._recover_serving_from_disk_fallback_if_needed(reason="unit_restart")

            self.assertEqual("recovered", report["status"])
            self.assertTrue(report["replay_gate"]["override"])
            self.assertEqual(1, report["recovered_records"])

    def test_cpp_backend_metrics_report_recovery_and_cache_state(self) -> None:
        client = _HashStoreClient()
        adapter = _direct_adapter_for_hash_store(client)
        adapter._disk_fallback_recovery_status = {
            "status": "recovered",
            "recovered_records": 3,
            "replay_gate": {"policy": "local_single_node_only", "allowed": True},
        }
        adapter._entry_count_cache = 3
        adapter._records_cache = [{"record_type": "context_event", "event_id_hash": 1}]

        metrics = adapter.backend_metrics()["metrics"]

        self.assertEqual("recovered", metrics["recovery_status"]["status"])
        self.assertEqual("local_disk_fallback_replay", metrics["recovery_status"]["recovery_source"])
        self.assertEqual(3, metrics["recovery_status"]["disk_fallback_recovery"]["recovered_records"])
        self.assertTrue(metrics["cache_state"]["records_cache_ready"])
        self.assertEqual(1, metrics["cache_state"]["records_cache_count"])

