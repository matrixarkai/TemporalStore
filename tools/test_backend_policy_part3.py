# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_BackendPolicyPart3 methods split from test_matrixark_mcp_backend_policy.MatrixArkMcpBackendPolicyTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_mcp_backend_policy import (
    Path,
    _CandidateCacheClient,
    _FailingNativeCandidateScanClient,
    _FailingNativeContextPackClient,
    _NativeAppendClient,
    _NativeCandidateScanClient,
    _NativeContextPackClient,
    _NativeIndexClient,
    _RustRecoveryParityClient,
    _rust_adapter_for_recovery_parity,
    compression_scan,
    json,
    mcp,
    mcp_budget_pack,
    mcp_core,
    retrieve_planning,
    tempfile,
    threading,
)
except ImportError:
    from test_matrixark_mcp_backend_policy import (
    Path,
    _CandidateCacheClient,
    _FailingNativeCandidateScanClient,
    _FailingNativeContextPackClient,
    _NativeAppendClient,
    _NativeCandidateScanClient,
    _NativeContextPackClient,
    _NativeIndexClient,
    _RustRecoveryParityClient,
    _rust_adapter_for_recovery_parity,
    compression_scan,
    json,
    mcp,
    mcp_budget_pack,
    mcp_core,
    retrieve_planning,
    tempfile,
    threading,
)


class _BackendPolicyPart3:
    def test_rust_backend_metrics_report_replicated_recovery_and_cache_state(self) -> None:
        client = _RustRecoveryParityClient()
        adapter = _rust_adapter_for_recovery_parity(client)
        adapter._entry_count_cache = 7
        adapter._records_cache = [{"record_type": "context_event", "event_id_hash": 1}]

        metrics = adapter.backend_metrics()["metrics"]

        self.assertEqual("native_replicated_storage_ready", metrics["recovery_status"]["status"])
        self.assertEqual("rust_replicated_page_store_read_through", metrics["recovery_status"]["recovery_source"])
        self.assertTrue(metrics["recovery_status"]["replicated_storage_recovery"])
        self.assertTrue(metrics["recovery_status"]["read_through_cache_warmup"])
        self.assertEqual(4, metrics["recovery_status"]["shared_store_read_throughs"])
        self.assertEqual(6, metrics["recovery_status"]["page_store_reads"])
        self.assertEqual(7, metrics["cache_state"]["entry_count_cache"])
        self.assertTrue(metrics["cache_state"]["records_cache_ready"])

    def test_rust_native_context_pack_invokes_shared_recovery_hook_before_read(self) -> None:
        client = _RustRecoveryParityClient()
        adapter = _rust_adapter_for_recovery_parity(client)
        adapter._disk_fallback_recovery_enabled = True
        adapter._disk_fallback_path = "/tmp/matrixark-rust-recovery-parity-unused.jsonl"

        pack = adapter.native_context_pack({"query": "gpu budget"})

        self.assertIsNotNone(pack)
        self.assertEqual("skipped", adapter._disk_fallback_recovery_status["status"])
        self.assertEqual(
            "distributed_storage_uses_replication_or_shared_store_recovery",
            adapter._disk_fallback_recovery_status["replay_gate"]["skip_reason"],
        )
        self.assertEqual(1, len(client.requests))

    def test_direct_append_supports_matrixkv_raw_backend_option(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:matrixkv-raw"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._raw_ingestion_prefix = "matrixkv:raw:messages"
        adapter._raw_record_hash_key = f"{adapter._raw_ingestion_prefix}:records"
        adapter._raw_count_key = f"{adapter._raw_ingestion_prefix}:record_count"
        adapter._raw_storage_backend = "matrixkv"
        adapter._raw_entry_count_cache = None
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

        adapter.append(
            {
                "record_type": "context_event",
                "event_id_hash": 992,
                "tenant_hash": 8,
                "scope_key": "tenant=8",
                "node_hash": 10,
                "updated_at_ms": 1780000002000,
                "text": "matrixkv raw option",
            }
        )

        self.assertEqual(len(client.calls), 2)
        raw_call = client.calls[0]
        self.assertEqual(raw_call["count_key"], "matrixkv:raw:messages:record_count")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_matrixkv_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "matrixkv")
        self.assertEqual(raw_call["entries"][0]["key"], "matrixkv:raw:messages:records:000000")

    def test_direct_retrieval_records_reuses_candidate_cache_by_count_and_scope(self) -> None:
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1,
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
                "text": "approval happened",
            },
            {
                "record_type": "context_pack_audit",
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
            },
            {
                "record_type": "context_event",
                "event_id_hash": 2,
                "tenant_id": "tenant_b",
                "user_id": "user_b",
                "scope": {"tenant_id": "tenant_b", "user_id": "user_b"},
                "text": "wrong scope",
            },
        ]
        client = _CandidateCacheClient(records)
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:candidate-cache"
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
        adapter._metrics_lock = threading.RLock()

        scope = {"tenant_id": "tenant_a", "user_id": "user_a"}
        first = adapter.retrieval_records(scope=scope)
        second = adapter.retrieval_records(scope=scope)

        self.assertEqual(client.batch_hget_calls, 1)
        self.assertFalse(first["scan_stats"]["candidate_cache_hit"])
        self.assertTrue(second["scan_stats"]["candidate_cache_hit"])
        self.assertEqual([record["event_id_hash"] for record in second["records"]], [1])
        self.assertEqual(second["scan_stats"]["dropped_type"], 1)
        self.assertEqual(second["scan_stats"]["dropped_scope"], 1)

    def test_direct_retrieval_uses_native_compact_index_prefilter(self) -> None:
        client = _NativeIndexClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-index"
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
        adapter._metrics_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        adapter.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "updated_at_ms": 1780000000000,
                    "text": "Alice approved the GPU budget.",
                },
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "embedding": [0.1, 0.2],
                },
                {
                    "record_type": "context_index",
                    "index_name": "source_type:message",
                    "ref_type": "event",
                    "ref_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "updated_at_ms": 1780000000000,
                },
            ]
        )

        result = adapter.retrieval_records(
            scope=scope,
            record_types={"context_event", "context_embedding", "context_index"},
            secondary_index_groups=[{"source_type:message"}],
        )

        stats = result["scan_stats"]
        self.assertTrue(stats["native_pushdown"])
        self.assertEqual(stats["execution_mode"], "native_secondary_index_prefilter")
        self.assertEqual(stats["native_index_ref_hash_count"], 1)
        self.assertGreaterEqual(stats["native_index_postings_found"], 1)
        self.assertTrue(stats["native_index_posting_buckets"])
        self.assertFalse(stats["broad_scan_used"])
        self.assertGreaterEqual(stats["native_locations"], 1)
        self.assertEqual({record["record_type"] for record in result["records"]}, {"context_event", "context_embedding", "context_index"})
        lookup_values = [
            value
            for (key, field), value in client.store.items()
            if ":context_index_lookup:" in key and field == "source_type:message"
        ]
        self.assertTrue(lookup_values)
        lookup_payload = json.loads(lookup_values[0])
        self.assertEqual(lookup_payload["ref_hashes"], [101])
        self.assertTrue(lookup_payload["posting_buckets"])
        broad_record_loads = [
            entries
            for entries in client.batch_hget_entries
            if entries and all(str(entry["key"]).startswith(f"{adapter._record_hash_key}:") for entry in entries)
        ]
        self.assertTrue(broad_record_loads)
        self.assertTrue(all(len(entries) < adapter._shard_size for entries in broad_record_loads))

    def test_direct_retrieval_blocks_broad_scan_without_explicit_fallback(self) -> None:
        client = _NativeIndexClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:phase2-no-broad"
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
        adapter._metrics_lock = threading.RLock()

        result = adapter.retrieval_records(
            scope={"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
            secondary_index_groups=[{"source_type:message"}],
            selected_node_hashes={44},
        )

        self.assertEqual(result["records"], [])
        stats = result["scan_stats"]
        self.assertEqual(stats["execution_mode"], "native_prefilter_no_match_broad_scan_blocked")
        self.assertFalse(stats["native_pushdown"])
        self.assertFalse(stats["broad_scan_fallback_allowed"])
        self.assertFalse(stats["broad_scan_used"])
        self.assertTrue(stats["broad_scan_blocked"])
        self.assertEqual(stats["scanned"], 0)

    def test_direct_native_retrieve_matches_reference_logical_refs(self) -> None:
        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        other_scope = {"tenant_hash": 11, "user_hash": 99, "session_hash": 33}
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 101,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 44,
                "resource_version": "rv1",
                "updated_at_ms": 1780000000000,
                "text": "Alice approved the GPU budget.",
            },
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": 101,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 44,
                "vector": [0.1, 0.2],
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 101,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000000,
            },
            {
                "record_type": "context_event",
                "event_id_hash": 202,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 55,
                "updated_at_ms": 1780000000001,
                "text": "Same scope but wrong node.",
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 202,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 55,
                "updated_at_ms": 1780000000001,
            },
            {
                "record_type": "context_event",
                "event_id_hash": 303,
                "scope": other_scope,
                "scope_key": mcp_core.canonical_scope_key(other_scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000002,
                "text": "Wrong scope should be filtered.",
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 303,
                "scope": other_scope,
                "scope_key": mcp_core.canonical_scope_key(other_scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000002,
            },
        ]

        client = _NativeIndexClient()
        direct = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        direct._client = client
        direct._storage_prefix = "matrixark:test:native-reference-parity"
        direct._record_hash_key = f"{direct._storage_prefix}:records"
        direct._index_key = f"{direct._storage_prefix}:record_index"
        direct._count_key = f"{direct._storage_prefix}:record_count"
        direct._shard_size = 1024
        direct._index_cache = None
        direct._records_cache = None
        direct._retrieval_candidate_cache = {}
        direct._retrieval_candidate_cache_lock = threading.RLock()
        direct._entry_count_cache = None
        direct._legacy_index_mode = False
        direct._records_lock = threading.RLock()
        direct._metrics_lock = threading.RLock()
        direct._write_retries = 0
        direct._write_backoff_s = 0.0
        direct._write_throttle_s = 0.0
        direct.append_many(records)

        with tempfile.TemporaryDirectory() as tmpdir:
            reference = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "reference.jsonl")
            reference.append_many(records)
            kwargs = {
                "scope": scope,
                "record_types": {"context_event", "context_embedding", "context_index"},
                "secondary_index_groups": [{"source_type:message"}],
                "selected_node_hashes": {44},
            }
            direct_result = direct.retrieval_records(**kwargs)
            reference_result = reference.retrieval_records(**kwargs)
            mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
            direct_cached_table_result = direct.retrieval_records(**kwargs)

        def logical_refs(rows: list[dict]) -> set[tuple[str, int]]:
            refs: set[tuple[str, int]] = set()
            for row in rows:
                record_type = str(row.get("record_type") or "")
                if isinstance(row.get("ref_hashes"), list):
                    for ref_hash in row["ref_hashes"]:
                        refs.add((record_type, int(ref_hash)))
                    continue
                ref_hash = row.get("event_id_hash") or row.get("ref_hash") or row.get("chunk_hash") or row.get("section_hash")
                if ref_hash is not None:
                    refs.add((record_type, int(ref_hash)))
            return refs

        self.assertEqual(logical_refs(direct_result["records"]), logical_refs(reference_result["records"]))
        self.assertEqual(logical_refs(direct_result["records"]), {("context_event", 101), ("context_embedding", 101), ("context_index", 101)})
        self.assertTrue(direct_result["scan_stats"]["native_pushdown"])
        self.assertEqual(direct_result["scan_stats"]["execution_mode"], "native_placement_prefetch")
        self.assertEqual(
            direct_result["scan_stats"]["native_candidate_cache_key_shape"],
            "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
        )
        self.assertEqual(direct_result["scan_stats"]["native_candidate_cache_payload"], "compact_struct")
        self.assertEqual(direct_result["scan_stats"]["native_resource_version_watermark"], "rv1")
        self.assertEqual(direct_result["scan_stats"]["native_placement_nodes"], 1)
        self.assertGreaterEqual(direct_result["scan_stats"]["native_placement_locator_rows"], 1)
        self.assertGreaterEqual(direct_result["scan_stats"]["native_placement_locations"], 1)
        placement_lookups = [
            entries
            for entries in client.batch_hget_entries
            if entries and all("context_placement_lookup" in str(entry["key"]) for entry in entries)
        ]
        self.assertTrue(placement_lookups)
        self.assertEqual({str(entry["field"]) for entry in placement_lookups[0]}, {"44"})
        self.assertEqual(direct_result["scan_stats"]["dropped_scope"], reference_result["scan_stats"]["dropped_by_scope"])
        self.assertEqual(reference_result["scan_stats"]["dropped_by_node"], 2)
        self.assertEqual(direct_result["scan_stats"]["dropped_node"], 0)
        self.assertTrue(direct_cached_table_result["scan_stats"]["native_placement_candidate_cache_hit"])
        self.assertEqual(
            logical_refs(direct_cached_table_result["records"]),
            {("context_event", 101), ("context_embedding", 101), ("context_index", 101)},
        )
        direct.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 404,
                    "scope": scope,
                    "scope_key": mcp_core.canonical_scope_key(scope),
                    "node_hash": 44,
                    "resource_version": "rv2",
                    "updated_at_ms": 1780000000003,
                    "text": "New append changes the watermark.",
                }
            ]
        )
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        direct_after_append_result = direct.retrieval_records(**kwargs)
        self.assertFalse(direct_after_append_result["scan_stats"]["native_placement_candidate_cache_hit"])
        self.assertEqual(direct_after_append_result["scan_stats"]["native_resource_version_watermark"], "rv1|rv2")
        self.assertIn(("context_event", 404), logical_refs(direct_after_append_result["records"]))

    def test_direct_retrieve_uses_native_context_pack_api_when_available(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None
        adapter._shard_size = 128
        adapter.append_context_pack_visibility = lambda **kwargs: {
            "audit_mode": "off",
            "telemetry_record": False,
            "rich_replay_audit": False,
        }

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "max_context_tokens": 2048,
                "local_context_tokens": 128,
                "ranking": {"max_selected_refs": 8},
                "source_role_budget_tokens": {"assistant": 128},
                "debug_context_pack": True,
                "include_retrieval_metrics": True,
            }
        )

        self.assertEqual(result["context_pack_id"], "native-pack-1")
        self.assertTrue(result["native_context_pack"])
        self.assertEqual(result["context_pack_assembly"], "native_direct")
        self.assertEqual(client.batch_hget_calls, 0)
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertEqual(request["storage_prefix"], "matrixark:test:native-pack")
        self.assertEqual(request["watermark_count"], 7)
        self.assertEqual(request["append_watermark"], 7)
        self.assertEqual(request["index_posting_watermark"], 7)
        self.assertEqual(request["resource_version_watermark"], "")
        self.assertEqual(request["skill_status_watermark"], "")
        self.assertEqual(request["scope_key"], "t=11|u=22|s=33|")
        self.assertEqual(request["placement_node_hash"], request["start_node_hash"])
        self.assertEqual(request["placement_key"], f"context:{request['scope_key']}:node={request['start_node_hash']}")
        self.assertFalse(request["include_superseded"])
        self.assertFalse(request["include_superseded_resources"])
        self.assertEqual(request["shared_resource_max_refs"], 4)
        self.assertEqual(request["skill_max_refs"], 4)
        self.assertEqual(request["cross_session_max_refs"], 4)
        self.assertEqual(request["source_role_budget_tokens"], {"assistant": 128})
        self.assertTrue(request["cross_session_rerank"])
        self.assertTrue(request["same_session_priority"])
        self.assertEqual(
            request["required_native_apis"],
            [
                "health",
                "readiness",
                "metrics",
                "matrixark_batch_append_records",
                "matrixark_retrieve_context_pack",
                "compact_secondary_index_lookup",
                "placement_key_candidate_fetch",
            ],
        )
        self.assertEqual(
            request["normal_path_stages"],
            [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
        )
        self.assertEqual(request["normalization_requirements"]["scope_key"], "canonical")
        self.assertEqual(request["normalization_requirements"]["placement_key"], "context:{scope_key}:node={node_hash}")
        self.assertEqual(request["execution_plan_requirements"]["phase"], "phase4_native_score_rerank_pack")
        self.assertEqual(request["execution_plan_requirements"]["candidate_fetch"], "selected_node_placement_partitions_only")
        self.assertEqual(
            request["execution_plan_requirements"]["candidate_cache"],
            "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
        )
        self.assertEqual(request["execution_plan_requirements"]["candidate_cache_payload"], "compact_structs_not_json_strings")
        self.assertEqual(
            request["execution_plan_requirements"]["scoring"],
            "native_embedding_similarity_temporal_decay_business_boost_same_session_boost",
        )
        self.assertEqual(
            request["execution_plan_requirements"]["quotas"],
            "native_shared_resource_quota_cross_session_quota_current_session_priority",
        )
        self.assertEqual(request["execution_plan_requirements"]["rerank"], "native_score_fusion_then_budget_aware_rerank")
        self.assertEqual(request["execution_plan_requirements"]["token_budget_pack"], "native_budget_pack_with_selected_refs_and_dropped_summary")
        self.assertEqual(request["execution_plan_requirements"]["python_role"], "dispatcher_only_no_candidate_materialization_no_hot_path_pack")
        self.assertEqual(request["execution_plan_requirements"]["pack_assembly"], "native_score_rank_budget_pack_selected_refs_dropped_summary")
        self.assertEqual(request["execution_plan_requirements"]["write_path"], "native_batch_append_records_append_queue_coalesced_persistence")
        self.assertEqual(request["execution_plan_requirements"]["write_route"], "placement_key_partition_route_before_persistence")
        self.assertEqual(request["execution_plan_requirements"]["write_coalescing"], "native_append_queue_coalesces_by_record_key_field")
        self.assertEqual(request["execution_plan_requirements"]["durability"], "storage_options_select_async_sync_shared_store_or_raft")
        self.assertEqual(request["execution_plan_requirements"]["retrieval_hot_path_audit"], "inline_counters_only_no_full_audit_blocking")
        self.assertEqual(request["execution_plan_requirements"]["context_pack_audit"], "sample_or_enqueue_async_policy_enabled")
        self.assertEqual(request["execution_plan_requirements"]["full_replay_audit_default"], "disabled")
        self.assertEqual(request["execution_plan_requirements"]["secondary_index"], "compact_postings_by_scope_index_time_bucket")
        self.assertEqual(request["execution_plan_requirements"]["broad_prefix_scan"], "disabled_unless_explicit_debug_fallback")
        self.assertEqual(request["execution_plan_requirements"]["health_readiness_metrics"], "native_backend_must_expose_health_readiness_metrics")
        self.assertEqual(
            request["execution_plan_requirements"]["normal_path"],
            "query_understanding_scope_filter_l0_l1_traversal_compact_index_placement_fetch_native_score_rerank_pack",
        )
        self.assertIn("scope", request["required_output"]["drop_counters"])
        self.assertIn("score_threshold", request["required_output"]["drop_counters"])
        self.assertIn("source_role_budget", request["required_output"]["drop_counters"])
        self.assertIn("memory_layer_budget", request["required_output"]["drop_counters"])
        self.assertIn("memory_selection_policy_budget", request["required_output"]["drop_counters"])
        self.assertIn("extraction_phase_budget", request["required_output"]["drop_counters"])
        self.assertTrue(request["required_output"]["broad_scan_used"])
        self.assertTrue(request["required_output"]["normal_path_stages"])
        self.assertTrue(request["required_output"]["health_readiness_metrics"])
        self.assertTrue(request["required_output"]["candidate_cache_key_shape"])
        self.assertTrue(request["required_output"]["native_pack_assembly"])
        self.assertFalse(request["required_output"]["raw_candidate_tables"])
        self.assertFalse(request["required_output"]["python_pack_fallback"])
        self.assertTrue(request["required_output"]["selected_refs"])
        self.assertTrue(request["required_output"]["dropped_summary"])
        self.assertTrue(request["required_output"]["retrieval_metrics"])
        self.assertEqual(result["retrieval_metrics"]["query_plan_ms"], 1.5)
        self.assertEqual(result["retrieval_metrics"]["placement_partitions_touched"], 2)
        self.assertEqual(result["retrieval_metrics"]["placement_fetch_count"], 8)
        self.assertEqual(result["retrieval_metrics"]["index_postings_read"], 9)
        self.assertTrue(result["retrieval_metrics"]["compact_index_bucket_used"])
        self.assertEqual(result["retrieval_metrics"]["compact_index_bucket_count"], 3)
        self.assertTrue(result["retrieval_metrics"]["candidate_cache_hit"])
        self.assertEqual(result["retrieval_metrics"]["append_queue_wait_ms"], 0.25)
        self.assertEqual(result["retrieval_metrics"]["append_engine_ms"], 0.75)
        self.assertEqual(result["retrieval_metrics"]["timeout_count"], 0)
        self.assertEqual(result["retrieval_metrics"]["fallback_flags"], ["native_context_pack"])
        self.assertEqual(result["retrieval_metrics"]["dropped_refs"], 21)
        self.assertTrue(result["retrieval_metrics"]["native_pack_assembly"])
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertEqual(result["retrieval_metrics"]["broad_scan_policy"], "explicit_fallback_or_debug_only")
        self.assertEqual(result["retrieval_metrics"]["normal_path_stages"], request["normal_path_stages"])
        self.assertEqual(result["retrieval_metrics"]["health_readiness_metrics"], {"health": True, "readiness": True, "metrics": True})
        self.assertFalse(result["retrieval_metrics"]["raw_candidate_tables_returned"])
        self.assertEqual(result["retrieval_metrics"]["drop_counters"]["scope"], 1)
        self.assertEqual(result["retrieval_metrics"]["drop_counters"]["score_threshold"], 6)
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["scope_filtering"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["placement_filtering"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["compact_secondary_index_prefilter"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["stale_superseded_exclusion"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["shared_resource_skill_quota"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["cross_session_quota_rerank"])
        self.assertEqual(
            result["recall_policy"]["backend_retrieval_pushdown"]["execution_mode"],
            "native_context_pack",
        )

    def test_direct_native_context_pack_flushes_due_idle_commit_before_request(self) -> None:
        client = _NativeContextPackClient()
        client.get_string = lambda key: "8" if key.endswith(":record_count") else ""
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack-idle"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None
        adapter._shard_size = 128
        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        idle_records = [
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": 707,
                "event_id_hash": 808,
                "scope": scope,
                "status": "idle_commit_scheduled",
                "trigger_policy": "idle_timeout",
                "threshold_messages": 20,
                "idle_commit_timeout_ms": 1,
                "idle_commit_deadline_ms": 1,
                "updated_at_ms": 1,
            }
        ]
        client.scan_calls = []

        def scan_candidates(**kwargs):
            client.scan_calls.append(kwargs)
            return {
                "records": list(idle_records),
                "scan_stats": {
                    "execution_mode": "native_temporalstore_candidate_prefilter",
                    "returned_records": len(idle_records),
                },
            }

        client.matrixark_scan_candidates = scan_candidates
        adapter._appended_idle_markers = []
        adapter._session_commit_requests = []
        adapter.read_all = lambda: (_ for _ in ()).throw(AssertionError("native idle flush must not call read_all"))
        adapter.append = lambda record: adapter._appended_idle_markers.append(record)

        def session_commit(request):
            adapter._session_commit_requests.append(request)
            return {
                "status": "committed",
                "trigger_policy": "idle_timeout",
                "committed_event_count": 1,
            }

        adapter.session_commit = session_commit
        adapter.append_context_pack_visibility = lambda **kwargs: {
            "audit_mode": "off",
            "telemetry_record": False,
            "rich_replay_audit": False,
        }

        result = adapter.retrieve(
            {
                "query": "what did the idle tail decide?",
                "scope": scope,
                "max_context_tokens": 2048,
                "ranking": {"pre_retrieval_idle_commit_flush": True},
                "debug_context_pack": True,
                "include_retrieval_metrics": True,
            }
        )

        self.assertEqual(1, len(adapter._session_commit_requests))
        self.assertEqual("idle_timeout", adapter._session_commit_requests[0]["commit_reason"])
        self.assertFalse(adapter._session_commit_requests[0]["force"])
        self.assertEqual(1, len(client.scan_calls))
        self.assertEqual(["matrixark_async_pipeline_task"], client.scan_calls[0]["record_types"])
        self.assertEqual(scope, client.scan_calls[0]["scope"])
        self.assertEqual(1, len(client.requests))
        self.assertEqual(8, client.requests[0]["watermark_count"])
        self.assertEqual(8, client.requests[0]["append_watermark"])
        self.assertEqual("committed", result["recall_policy"]["pre_retrieval_idle_commit"]["status"])
        self.assertEqual("committed", result["retrieval_metrics"]["pre_retrieval_idle_commit"]["status"])
        self.assertTrue(
            any(record.get("status") == "idle_commit_committed" for record in adapter._appended_idle_markers)
        )

    def test_direct_retrieve_derives_native_hash_scope_from_plain_ids(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None
        adapter._shard_size = 128
        adapter.append_context_pack_visibility = lambda **kwargs: {
            "audit_mode": "off",
            "telemetry_record": False,
            "rich_replay_audit": False,
        }
        scope = {
            "account_id": "acct_scale",
            "tenant_id": "tenant_scale",
            "user_id": "user_scale",
            "session_id": "session_scale",
        }

        adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": scope,
                "max_context_tokens": 2048,
                "ranking": {"max_selected_refs": 8},
            }
        )

        request = client.requests[0]
        expected_hashes = mcp_core.identity_hashes("acct_scale", "tenant_scale", "user_scale", "session_scale")
        self.assertEqual(request["tenant_hash"], expected_hashes["tenant_hash"])
        self.assertEqual(request["scope"]["tenant_hash"], expected_hashes["tenant_hash"])
        self.assertEqual(request["scope"]["user_hash"], expected_hashes["user_hash"])
        self.assertEqual(request["scope"]["session_hash"], expected_hashes["session_hash"])
        self.assertEqual(request["scope_key"], expected_hashes["scope_key"])
        self.assertEqual(request["scope_hash"], mcp_core.stable_hash(expected_hashes["scope_key"]))
        self.assertEqual(
            request["placement_key"],
            f"context:{expected_hashes['scope_key']}:node={request['start_node_hash']}",
        )

    def test_direct_retrieve_compact_pack_can_include_native_retrieval_metrics(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None
        adapter._shard_size = 128
        adapter.append_context_pack_visibility = lambda **kwargs: {
            "audit_mode": "off",
            "telemetry_record": False,
            "rich_replay_audit": False,
        }

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "max_context_tokens": 1200,
                "local_context": [{"ref": "open-file:budget.md", "text": "Visible local prompt context."}],
                "local_context_tokens": 40,
                "local_context_safety_margin_tokens": 20,
                "include_retrieval_metrics": True,
            }
        )

        self.assertIn("selected_refs", result)
        self.assertEqual(1, len(result["selected_refs"]))
        self.assertNotIn("score", result["selected_refs"][0])
        self.assertEqual(result["retrieval_metrics"]["score_ms"], 5.5)
        self.assertEqual(result["retrieval_metrics"]["scanned_records"], 7)
        self.assertEqual(result["retrieval_metrics"]["requested_max_context_tokens"], 1200)
        self.assertEqual(result["retrieval_metrics"]["used_local_context_tokens"], 40)
        self.assertEqual(result["retrieval_metrics"]["used_remote_context_tokens"], 6)
        self.assertEqual(result["retrieval_metrics"]["total_prompt_context_tokens"], 46)
        self.assertEqual(result["retrieval_metrics"]["remote_context_budget_tokens"], 1024)
        self.assertEqual(result["retrieval_metrics"]["local_context_safety_margin_tokens"], 20)
        self.assertTrue(result["retrieval_metrics"]["remote_is_additive_only_within_remaining_budget"])

    def test_native_retrieve_failure_blocks_python_broad_scan_by_default(self) -> None:
        client = _FailingNativeContextPackClient("raise")
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "include_retrieval_metrics": True,
            }
        )

        self.assertEqual(result["context_pack_assembly"], "native_context_pack_blocked")
        self.assertEqual(result["retrieval_metrics"]["native_api"], "matrixark_retrieve_context_pack")
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertTrue(result["retrieval_metrics"]["broad_scan_blocked"])
        self.assertEqual(result["retrieval_metrics"]["scanned_records"], 0)
        self.assertEqual(
            result["retrieval_metrics"]["normal_path_stages"],
            [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
        )
        self.assertEqual(result["retrieval_metrics"]["health_readiness_metrics"], {"health": True, "readiness": True, "metrics": True})
        self.assertEqual(client.read_all_calls, 0)
        self.assertIn("quality_warnings", result)

    def test_native_retrieve_raw_candidate_tables_are_rejected_without_python_pack(self) -> None:
        client = _FailingNativeContextPackClient("raw_tables")
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
            }
        )

        self.assertEqual(result["context_pack_assembly"], "native_context_pack_blocked")
        self.assertTrue(result["retrieval_metrics"]["raw_candidate_tables_returned"])
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertEqual(client.read_all_calls, 0)

    def test_native_retrieve_explicit_debug_fallback_can_leave_native_pack_path(self) -> None:
        client = _FailingNativeContextPackClient("raise")
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = 0

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "allow_python_pack_fallback": True,
            }
        )

        self.assertNotEqual(result.get("context_pack_assembly"), "native_context_pack_blocked")

    def test_direct_retrieval_candidate_cache_is_shared_across_adapters(self) -> None:
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1,
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
                "text": "approval happened",
            }
        ]
        scope = {"tenant_id": "tenant_a", "user_id": "user_a"}

        def make_adapter(client: _CandidateCacheClient) -> mcp.MatrixArkTemporalStoreDirectAdapter:
            adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
            adapter._client = client
            adapter._storage_prefix = "matrixark:test:candidate-cache-shared"
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
            adapter._metrics_lock = threading.RLock()
            return adapter

        first_client = _CandidateCacheClient(records)
        second_client = _CandidateCacheClient(records)
        first = make_adapter(first_client).retrieval_records(scope=scope)
        second = make_adapter(second_client).retrieval_records(scope=scope)

        self.assertEqual(first_client.batch_hget_calls, 1)
        self.assertEqual(second_client.batch_hget_calls, 0)
        self.assertFalse(first["scan_stats"]["candidate_cache_hit"])
        self.assertTrue(second["scan_stats"]["candidate_cache_hit"])
        self.assertEqual(second["scan_stats"]["candidate_cache_scope"], "process_global")
        self.assertEqual([record["event_id_hash"] for record in second["records"]], [1])

    def test_direct_append_time_index_uses_segment_parent_when_present(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:segment-time"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        adapter._append_many_materialized(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 456,
                    "node_hash": 999,
                    "parent_segment_hash": 777,
                    "updated_at_ms": 1780000000001,
                    "text": "segmented event",
                }
            ]
        )

        time_entry = next(entry for entry in client.calls[0]["entries"] if "context_event_by_ingestion_time" in entry["key"])
        self.assertIn("context_event_by_ingestion_time:context_segment:777", time_entry["key"])
        self.assertEqual(time_entry["field"], f"{1780000000001:020d}:456")
        time_payload = json.loads(time_entry["value"])
        self.assertEqual(time_payload["record_type"], "context_event")
        self.assertEqual(time_payload["event_id_hash"], 456)
        self.assertEqual(time_payload["text"], "segmented event")
        self.assertNotIn("event_time_key", time_payload)
        self.assertNotIn("ingestion_time_ms", time_payload)


    def test_direct_retrieval_records_prefilters_scope_type_and_secondary_indexes(self) -> None:
        policy_globals = mcp.MatrixArkTemporalStoreDirectAdapter._native_candidate_scan.__globals__["native_candidate_prefilter_required"].__globals__
        old_core_prefilter = policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"]
        policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = "0"
        self.addCleanup(lambda: policy_globals.__setitem__("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", old_core_prefilter))
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._records_cache = []
        adapter._backend_label = lambda: "temporalstore-direct"
        scope = {"account_id": "acct", "tenant_id": "tenant", "user_id": "user"}
        adapter.read_all = lambda: [
            {
                "record_type": "resource_chunk",
                "scope": scope,
                "node_hash": 101,
                "chunk_hash": 1001,
                "resource_type": "pdf",
                "text": "GPU approval policy",
            },
            {
                "record_type": "resource_chunk",
                "scope": scope,
                "node_hash": 102,
                "chunk_hash": 1002,
                "resource_type": "md",
                "text": "Markdown notes",
            },
            {
                "record_type": "context_event",
                "scope": scope,
                "node_hash": 103,
                "text": "Alice approved the request",
            },
            {
                "record_type": "context_pack_audit",
                "scope": scope,
                "context_pack_id": "audit-only",
            },
            {
                "record_type": "resource_chunk",
                "scope": {"account_id": "other", "tenant_id": "tenant", "user_id": "user"},
                "node_hash": 104,
                "chunk_hash": 1004,
                "resource_type": "pdf",
                "text": "wrong account",
            },
        ]

        result = adapter.retrieval_records(
            scope=scope,
            secondary_index_groups=[{"resource_type:pdf"}],
        )

        texts = {record.get("text") for record in result["records"]}
        self.assertIn("GPU approval policy", texts)
        self.assertIn("Alice approved the request", texts)
        self.assertNotIn("Markdown notes", texts)
        self.assertNotIn("wrong account", texts)
        stats = result["scan_stats"]
        self.assertTrue(stats["backend_pushdown"])
        self.assertTrue(stats["direct_backend_prefilter"])
        self.assertFalse(stats["native_pushdown"])
        self.assertEqual(stats["execution_mode"], "direct_backend_hot_cache_prefilter")
        self.assertGreaterEqual(stats["secondary_index_dropped_candidate_count"], 1)
        self.assertEqual(stats["dropped_by_type"], 1)
        self.assertEqual(stats["dropped_by_scope"], 1)

    def test_direct_retrieval_prefers_native_candidate_scan_when_available(self) -> None:
        client = _NativeCandidateScanClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-rust"
        scope = {"account_id": "acct", "tenant_id": "tenant", "user_id": "user"}

        result = adapter.retrieval_records(
            scope=scope,
            record_types={"resource_chunk"},
            secondary_index_groups=[{"resource_type:pdf"}],
            selected_node_hashes={123},
        )

        self.assertEqual(result["records"][0]["text"], "native filtered pdf")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["count_key"], "matrixark:test:record_count")
        self.assertEqual(client.calls[0]["record_hash_key"], "matrixark:test:records")
        self.assertEqual(client.calls[0]["secondary_index_groups"], [["resource_type:pdf"]])
        self.assertEqual(client.calls[0]["selected_node_hashes"], [123])
        stats = result["scan_stats"]
        self.assertTrue(stats["native_prefix_scan"])
        self.assertTrue(stats["native_secondary_index_prefilter"])
        self.assertEqual(stats["pack_assembly_location"], "python_reference_packer")

    def test_production_native_candidate_scan_failure_does_not_fallback_to_read_all(self) -> None:
        policy_globals = mcp.MatrixArkTemporalStoreDirectAdapter._native_candidate_scan.__globals__["native_candidate_prefilter_required"].__globals__
        old_core_profile = policy_globals["MATRIXARK_MCP_PROFILE"]
        old_core_prefilter = policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"]
        policy_globals["MATRIXARK_MCP_PROFILE"] = "production"
        policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = ""
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _FailingNativeCandidateScanClient()
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter.read_all = lambda: self.fail("read_all fallback must not run when native prefilter is required")

        try:
            with self.assertRaisesRegex(mcp.MatrixArkError, "backend-native candidate prefilter failed"):
                adapter.retrieval_records(
                    scope={"account_id": "acct"},
                    record_types={"resource_chunk"},
                    secondary_index_groups=[{"resource_type:pdf"}],
                )
        finally:
            policy_globals["MATRIXARK_MCP_PROFILE"] = old_core_profile
            policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = old_core_prefilter

    def test_retrieve_prefers_same_session_but_allows_entity_bridge_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "matrixark.jsonl")
            same_scope = {
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "session_current",
                "tenant_hash": mcp_core.stable_hash("acct:tenant"),
                "user_hash": mcp_core.stable_hash(f"{mcp_core.stable_hash('acct:tenant')}:user:user"),
                "session_hash": mcp_core.stable_hash(f"{mcp_core.stable_hash('acct:tenant')}:session:session_current"),
                "_explicit_scope_keys": ["account_id", "tenant_id", "user_id", "session_id"],
            }
            same_scope["scope_key"] = mcp_core.scope_key_from_hashes(same_scope["tenant_hash"], same_scope["user_hash"], same_scope["session_hash"])
            prior_session_hash = mcp_core.stable_hash(f"{same_scope['tenant_hash']}:session:session_prior")
            prior_scope = {**same_scope, "session_id": "session_prior", "session_hash": prior_session_hash}
            prior_scope["scope_key"] = mcp_core.scope_key_from_hashes(prior_scope["tenant_hash"], prior_scope["user_hash"], prior_session_hash)
            adapter.append_many([
                {
                    "record_type": "context_event",
                    "event_id_hash": 101,
                    "node_hash": 11,
                    "node_path": ["memory", "approvals"],
                    "scope": same_scope,
                    "scope_key": same_scope["scope_key"],
                    "classification": "confirmation",
                    "event_type": "confirmation",
                    "text": "The current session is about the GPU request and approval follow-up.",
                    "updated_at_ms": 1000,
                },
                {
                    "record_type": "context_entity",
                    "entity_hash": 202,
                    "node_hash": 12,
                    "node_path": ["memory", "approvals"],
                    "scope": prior_scope,
                    "scope_key": prior_scope["scope_key"],
                    "entity_type": "approval_state",
                    "entity_name": "gpu_request",
                    "state": "Alice approved the GPU request after finance review.",
                    "updated_at_ms": 1100,
                },
                {
                    "record_type": "context_index",
                    "index_name": "event_type:confirmation",
                    "ref_hash": 101,
                    "node_hash": 11,
                    "scope": same_scope,
                    "scope_key": same_scope["scope_key"],
                },
                {
                    "record_type": "context_index",
                    "index_name": "entity_type:approval_state",
                    "ref_hash": 202,
                    "node_hash": 12,
                    "scope": prior_scope,
                    "scope_key": prior_scope["scope_key"],
                },
            ])

            pack = adapter.retrieve({
                "query": "Who approved the GPU request?",
                "scope": same_scope,
                "session_scope": "prefer",
                "max_context_tokens": 1000,
                "audit_mode": "off",
                "ranking": {"max_selected_refs": 8},
            })

            selected = pack["selected_refs"]
            self.assertTrue(any(ref.get("session_continuity") == "same_session" for ref in selected))
            self.assertTrue(any(ref.get("session_continuity") == "cross_session" and ref.get("ref_type") == "entity" for ref in selected))
            policy = (pack.get("recall_policy") or pack.get("recall", {})).get("session_continuity", {})
            if policy:
                self.assertEqual(policy["mode"], "prefer")
                self.assertGreaterEqual(policy["same_session_selected_ref_count"], 1)
                self.assertGreaterEqual(policy["entity_bridge_selected_ref_count"], 1)

            strict_pack = adapter.retrieve({
                "query": "Who approved the GPU request?",
                "scope": same_scope,
                "session_scope": "only",
                "max_context_tokens": 1000,
                "audit_mode": "off",
                "ranking": {"max_selected_refs": 8},
            })
            self.assertFalse(any(ref.get("session_continuity") == "cross_session" for ref in strict_pack["selected_refs"]))

    def test_budget_packer_reserves_slot_for_required_profile_entity_bridge(self) -> None:
        primary = [
            {
                "ref_type": "event",
                "text": "Current session says the storage migration is blocked on capacity review.",
                "score": 0.99,
                "session_continuity": "same_session",
            },
            {
                "ref_type": "event",
                "text": "Build hosts passed the local disk capacity gate.",
                "score": 0.98,
                "session_continuity": "same_session",
            },
            {
                "ref_type": "entity",
                "text": "User profile says Alice approved the GPU request after finance review.",
                "score": 0.21,
                "session_continuity": "cross_session",
                "scope": {"session_id": "prior-session"},
            },
        ]

        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            primary,
            [],
            max_context_tokens=200,
            auxiliary_quota=0,
            question_type="current_state",
            max_selected_refs=1,
            max_global_candidates=8,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 200,
                "max_sessions": 3,
                "max_candidates": 8,
                "min_score": 0.0,
                "raw_evidence_min_score": 0.45,
                "min_entity_bridge_refs": 1,
            },
        )

        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, len(selected))
        self.assertTrue(
            any(ref.get("session_continuity") == "cross_session" and ref.get("ref_type") == "entity" for ref in selected)
        )
        self.assertEqual(1, dropped["entity_bridge_slot_reserved"])
        self.assertEqual(1, dropped["cross_session_policy"]["entity_bridge_selected_ref_count"])
        self.assertEqual(
            "candidate was skipped to preserve a minimum cross-session entity bridge slot",
            dropped["reason_descriptions"]["entity_bridge_slot_reserved"],
        )

    def test_budget_packer_drops_stale_session_entity_for_profile_bridge(self) -> None:
        primary = [
            {
                "ref_type": "entity",
                "ref_hash": 7001,
                "text": "Session entity says the Windows repo folder is still the TemporalStore workspace.",
                "score": 0.99,
                "session_continuity": "same_session",
                "entity_type": "repo_workspace",
                "entity_name": "TemporalStore",
                "memory_scope": "session",
                "updated_at_ms": 1000,
            },
            {
                "ref_type": "entity",
                "ref_hash": 7002,
                "text": "User profile says TemporalStore work must use /opt/github-services/TemporalStore.",
                "score": 0.72,
                "session_continuity": "cross_session",
                "entity_type": "repo_workspace",
                "entity_name": "TemporalStore",
                "memory_scope": "user_profile",
                "version_state": "current",
                "updated_at_ms": 2000,
                "source_entity_hashes": [7001],
                "scope": {"session_id": "profile"},
            },
        ]

        selected, _, dropped = mcp_budget_pack.select_token_budgeted_refs(
            primary,
            [],
            max_context_tokens=180,
            auxiliary_quota=0,
            question_type="current_state",
            max_selected_refs=2,
            max_global_candidates=8,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 180,
                "max_sessions": 3,
                "max_candidates": 8,
                "min_score": 0.0,
                "raw_evidence_min_score": 0.45,
                "min_entity_bridge_refs": 1,
            },
        )

        self.assertEqual(["cross_session"], [ref.get("session_continuity") for ref in selected])
        self.assertEqual(["current"], [ref.get("version_state") for ref in selected])
        self.assertEqual(0.18, selected[0]["profile_current_state_boost"])
        self.assertEqual(1, dropped["stale"])
        self.assertEqual(1, dropped["cross_session_policy"]["entity_bridge_selected_ref_count"])
        stale_drop = next(ref for ref in dropped["refs"] if ref.get("drop_reason") == "stale")
        self.assertEqual("same_session", stale_drop["session_continuity"])
        self.assertTrue(stale_drop["stale_or_superseded"])
        self.assertEqual(7002, stale_drop["profile_shadowed_by_ref_hash"])

    def test_budget_packer_enforces_memory_layer_caps(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "segment",
                    "ref_hash": 8101,
                    "text": "same session segment",
                    "score": 0.95,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                },
                {
                    "ref_type": "segment",
                    "ref_hash": 8102,
                    "text": "another same session segment",
                    "score": 0.94,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8103,
                    "text": "profile entity remains selectable",
                    "score": 0.93,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                },
            ],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={"enabled": True, "budget_tokens": 40, "max_sessions": 4, "max_candidates": 4},
            memory_layer_budget_tokens={"same_session_segment": 3, "profile_entity": 8},
        )

        self.assertEqual([8101, 8103], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual(4, dropped["estimated_tokens"]["memory_layer_budget"])
        self.assertEqual(
            {"same_session_segment": 3, "profile_entity": 8},
            dropped["memory_layer_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["same_session_segment"])
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["profile_entity"])

    def test_budget_packer_enforces_pending_async_event_caps_separately(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "event",
                    "ref_hash": 8111,
                    "text": "user: remember this live pending hook event",
                    "score": 0.95,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "event_type": "pending_async",
                    "classification": "PENDING_ASYNC_EXTRACTION",
                    "extraction_phase": "pending_async",
                },
                {
                    "ref_type": "event",
                    "ref_hash": 8112,
                    "text": "user: another pending hook event should be capped",
                    "score": 0.94,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "event_type": "pending_async",
                    "classification": "PENDING_ASYNC_EXTRACTION",
                    "extraction_phase": "pending_async",
                },
                {
                    "ref_type": "event",
                    "ref_hash": 8113,
                    "text": "final extracted same-session event remains separate",
                    "score": 0.93,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "event_type": "memory_update",
                    "classification": "BATCH_MEMORY",
                    "extraction_phase": "final",
                },
            ],
            [],
            max_context_tokens=80,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={"enabled": True, "budget_tokens": 80, "max_sessions": 4, "max_candidates": 4},
            memory_layer_budget_tokens={"pending_async_event": 7, "same_session_event": 12},
        )

        self.assertEqual([8111, 8113], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual(8, dropped["estimated_tokens"]["memory_layer_budget"])
        self.assertEqual(
            {"pending_async_event": 7, "same_session_event": 12},
            dropped["memory_layer_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["pending_async_event"])
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["same_session_event"])

    def test_budget_packer_enforces_extraction_phase_caps(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "event",
                    "ref_hash": 8121,
                    "text": "provisional idle extraction captures a live preference",
                    "score": 0.95,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": "provisional",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8122,
                    "text": "another provisional extraction should be capped",
                    "score": 0.94,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": "provisional",
                },
                {
                    "ref_type": "summary",
                    "ref_hash": 8123,
                    "text": "final Stop boundary summary remains selectable",
                    "score": 0.93,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                },
            ],
            [],
            max_context_tokens=120,
            auxiliary_quota=0,
            min_score=0.0,
            extraction_phase_budget_tokens={"provisional": 7, "final": 20},
        )

        self.assertEqual(2, len(selected))
        self.assertEqual(1, sum(1 for ref in selected if ref.get("extraction_phase") == "provisional"))
        self.assertIn(8123, [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["extraction_phase_budget"])
        self.assertGreater(dropped["estimated_tokens"]["extraction_phase_budget"], 0)
        self.assertEqual(
            {"provisional": 7, "final": 20},
            dropped["extraction_phase_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(1, dropped["extraction_phase_budget_policy"]["selected_ref_count_by_phase"]["provisional"])
        self.assertEqual(1, dropped["extraction_phase_budget_policy"]["selected_ref_count_by_phase"]["final"])

    def test_retrieval_plan_preserves_extraction_phase_budget_tokens(self) -> None:
        plan = retrieve_planning.retrieval_query_budget_plan(
            {
                "query": "show final profile decisions",
                "scope": {"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
                "max_context_tokens": 400,
                "extraction_phase_budget_tokens": {"final": 80, "provisional": 20},
            },
            {},
            query="show final profile decisions",
            scope={"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
            default_max_context_tokens=400,
        )

        self.assertEqual({"final": 80, "provisional": 20}, plan["extraction_phase_budget_tokens"])
        self.assertEqual("explicit", plan["extraction_phase_budget_mode"])

    def test_feature_scope_prunes_codex_tool_alias_budgets(self) -> None:
        source_roles, memory_layers, selection_policies = retrieve_planning.prune_feature_scope_evidence_budgets(
            query="focus on feature parity only, no evidence or debugging",
            source_role_budget_tokens={"function_call_output": 32, "assistant": 64},
            memory_layer_budget_tokens={"cross_session_codex_outcome_event": 32, "profile_entity": 64},
            memory_selection_policy_budget_tokens={
                "selected_tool_evidence_only": 32,
                "selected_profile_current_state": 64,
            },
        )

        self.assertEqual({"assistant": 64, "tool": 0}, source_roles)
        self.assertEqual(64, memory_layers["profile_entity"])
        self.assertEqual(0, memory_layers["cross_session_codex_outcome_event"])
        self.assertEqual(0, memory_layers["cross_session_codex_outcome_entity"])
        self.assertEqual(64, selection_policies["selected_profile_current_state"])
        self.assertEqual(0, selection_policies["selected_tool_evidence_only"])
        self.assertEqual(0, selection_policies["selected_assistant_decision_outcome_only"])

    def test_budget_packer_treats_zero_role_budget_as_exclusion(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "entity",
                    "ref_hash": 8211,
                    "text": "tool evidence should be excluded",
                    "score": 0.99,
                    "entity_type": "tool_evidence",
                    "source_roles": ["function_call_output"],
                    "source_role_counts": {"function_call_output": 1},
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 8212,
                    "text": "profile current state should stay",
                    "score": 0.80,
                    "entity_type": "memory_feature_profile",
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                },
            ],
            [],
            max_context_tokens=80,
            auxiliary_quota=0,
            min_score=0.0,
            source_role_budget_tokens={"tool": 0, "assistant": 80},
        )

        self.assertEqual([8212], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertEqual({"assistant": 80, "tool": 0}, dropped["source_role_budget_policy"]["budget_tokens"])

    def test_retrieval_plan_uses_question_type_source_role_budget_defaults(self) -> None:
        plan = retrieve_planning.retrieval_query_budget_plan(
            {
                "query": "what do you remember about me",
                "scope": {"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
                "max_context_tokens": 400,
                "ranking": {"source_role_budget_mode": "auto"},
            },
            {"source_role_budget_mode": "auto"},
            query="what do you remember about me",
            scope={"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
            default_max_context_tokens=400,
        )

        self.assertEqual("profile_memory", plan["question_type"])
        self.assertEqual("auto", plan["source_role_budget_mode"])
        self.assertEqual({"assistant": 190, "tool": 171, "user": 190}, plan["source_role_budget_tokens"])

    def test_retrieval_plan_infers_memory_selection_budget_from_memory_layer_auto(self) -> None:
        plan = retrieve_planning.retrieval_query_budget_plan(
            {
                "query": "what do you remember about me",
                "scope": {"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
                "max_context_tokens": 400,
                "ranking": {"memory_layer_budget_mode": "auto"},
            },
            {"memory_layer_budget_mode": "auto"},
            query="what do you remember about me",
            scope={"tenant_id": "tenant", "user_id": "user", "session_id": "session"},
            default_max_context_tokens=400,
        )

        self.assertEqual("profile_memory", plan["question_type"])
        self.assertEqual("auto", plan["memory_layer_budget_mode"])
        self.assertEqual("auto", plan["memory_selection_policy_budget_mode"])
        self.assertEqual(247, plan["memory_selection_policy_budget_tokens"]["selected_profile_current_state"])

    def test_compression_scan_honors_secondary_memory_layer_filters(self) -> None:
        scope = {"tenant_id": "tenant", "user_id": "user", "session_id": "session"}
        records = [
            {
                "record_type": "context_compression_event",
                "compression_id_hash": 9101,
                "node_hash": 501,
                "node_path": ["tenant:tenant", "user:user", "session:session"],
                "scope": scope,
                "summary_text": "provisional session evidence that should be filtered",
                "operator": "TIME_COMPRESS",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": "provisional",
                "updated_at_ms": 1000,
            },
            {
                "record_type": "context_compression_event",
                "compression_id_hash": 9102,
                "node_hash": 501,
                "node_path": ["tenant:tenant", "user:user", "profile:long_term_memory"],
                "scope": scope,
                "summary_text": "final profile decision evidence that should remain",
                "operator": "TIME_COMPRESS",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "updated_at_ms": 1001,
            },
        ]

        primary, auxiliary, dropped, matched, fallback = compression_scan.scan_compression_candidates(
            records,
            retrieval_scope=scope,
            selected_by_tree=lambda _record: True,
            index_terms_by_batch={},
            index_terms_by_node={},
            index_terms_by_ref={},
            secondary_index_filter_groups=[
                {"memory_scope:user_profile"},
                {"session_continuity:cross_session"},
                {"extraction_phase:final"},
            ],
            secondary_index_filter_mode="all_groups",
            admit_candidate_for_node=lambda _record: True,
            query_terms={"final", "profile", "decision"},
            query_embedding=mcp_core.embedding_for_text("final profile decision"),
            compression_embedding_vectors={},
            node_scores={501: {"score": 0.7}},
            annotate_session_continuity=lambda candidate, record: {
                **candidate,
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "extraction_phase": record.get("extraction_phase", ""),
            },
            ranking={},
            reference_time_ms=2000,
            deadline_exceeded=lambda: False,
        )

        self.assertEqual("", fallback)
        self.assertEqual(1, dropped)
        self.assertEqual(1, matched)
        self.assertEqual([9102], [candidate["ref_hash"] for candidate in primary])
        self.assertTrue(all(candidate.get("extraction_phase") == "final" for candidate in primary + auxiliary))
        self.assertEqual("user_profile", primary[0]["memory_scope"])
        self.assertEqual("cross_session", primary[0]["session_continuity"])
        self.assertEqual("profile_compression", mcp_core.candidate_memory_layer_name(primary[0]))

    def test_budget_packer_can_cap_profile_compression_separately(self) -> None:
        selected, used_tokens, dropped = mcp_budget_pack.select_token_budgeted_refs(
            [
                {
                    "ref_type": "compression",
                    "ref_hash": 9201,
                    "text": "profile compression one",
                    "score": 0.97,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "extraction_phase": "final",
                },
                {
                    "ref_type": "compression",
                    "ref_hash": 9202,
                    "text": "profile compression two",
                    "score": 0.96,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "extraction_phase": "final",
                },
                {
                    "ref_type": "compression",
                    "ref_hash": 9203,
                    "text": "session compression one",
                    "score": 0.95,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": "provisional",
                },
            ],
            [],
            max_context_tokens=160,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={"enabled": True, "budget_tokens": 40, "max_sessions": 4, "max_candidates": 4},
            memory_layer_budget_tokens={"profile_compression": 4, "same_session_compression": 20},
        )

        self.assertEqual([9201, 9203], [ref["ref_hash"] for ref in selected])
        self.assertGreater(used_tokens, 0)
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual(
            {"profile_compression": 4, "same_session_compression": 20},
            dropped["memory_layer_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["profile_compression"])
        self.assertEqual(1, dropped["memory_layer_budget_policy"]["selected_ref_count_by_layer"]["same_session_compression"])

