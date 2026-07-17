#!/usr/bin/env python3
"""Regression tests for the MatrixArk Python MCP module layout."""

from __future__ import annotations

import ast
import importlib
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = REPO_ROOT / "tools"


class MatrixArkPythonModuleBoundaryTest(unittest.TestCase):
    def test_package_imports_expose_server_and_schema_catalog(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        schemas_mod = importlib.import_module("tools.matrixark_mcp_schemas")
        schema_common_mod = importlib.import_module("tools.matrixark_mcp_schema_common")
        self.assertTrue(hasattr(server_mod, "MatrixArkMcpServer"))
        self.assertTrue(hasattr(server_mod, "MatrixArkLocalAdapter"))
        self.assertGreaterEqual(len(schemas_mod.TOOLS), 10)
        self.assertIs(schemas_mod.MESSAGE_SCHEMA, schema_common_mod.MESSAGE_SCHEMA)
        self.assertIs(schemas_mod.SCOPE_SCHEMA, schema_common_mod.SCOPE_SCHEMA)

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


    def test_mcp_entrypoint_reexports_split_modules(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        server_metrics_mod = importlib.import_module("tools.matrixark_mcp_server_metrics")
        server_request_policy_mod = importlib.import_module("tools.matrixark_mcp_server_request_policy")
        schema_common_mod = importlib.import_module("tools.matrixark_mcp_schema_common")
        auth_schemas_mod = importlib.import_module("tools.matrixark_mcp_auth_schemas")
        admin_schemas_mod = importlib.import_module("tools.matrixark_mcp_admin_schemas")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        runtime_exports_mod = importlib.import_module("tools.matrixark_mcp_runtime_exports")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")
        local_core_mixin_mod = importlib.import_module("tools.matrixark_mcp_local_core_mixin")
        local_portal_imports_mod = importlib.import_module("tools.matrixark_mcp_local_portal_imports")
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        temporal_direct_cache_mod = importlib.import_module("tools.matrixark_mcp_temporal_direct_cache")
        admin_mod = importlib.import_module("tools.matrixark_mcp_admin")
        backends_mod = importlib.import_module("tools.matrixark_mcp_backends")
        dispatch_mod = importlib.import_module("tools.matrixark_mcp_dispatch")
        ingestion_mod = importlib.import_module("tools.matrixark_mcp_ingestion")
        retrieval_mod = importlib.import_module("tools.matrixark_mcp_retrieval")
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")
        validation_mod = importlib.import_module("tools.matrixark_mcp_validation")
        query_mod = importlib.import_module("tools.matrixark_mcp_query")
        identity_mod = importlib.import_module("tools.matrixark_mcp_identity")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        entity_ops_mod = importlib.import_module("tools.matrixark_mcp_entity_ops")
        tree_mod = importlib.import_module("tools.matrixark_mcp_tree")
        rust_direct_mod = importlib.import_module("tools.matrixark_mcp_rust_direct_client")
        rust_proxy_mod = importlib.import_module("tools.matrixark_mcp_rust_proxy_client")
        rust_proxy_cache_mixin_mod = importlib.import_module("tools.matrixark_mcp_rust_proxy_cache_mixin")
        rust_proxy_lane_select_mod = importlib.import_module("tools.matrixark_mcp_rust_proxy_lane_select")
        rust_proxy_metrics_record_mod = importlib.import_module("tools.matrixark_mcp_rust_proxy_metrics_record")
        rust_proxy_metrics_snapshot_mod = importlib.import_module("tools.matrixark_mcp_rust_proxy_metrics_snapshot")
        session_policy_mod = importlib.import_module("tools.matrixark_mcp_session_policy")
        session_runtime_mod = importlib.import_module("tools.matrixark_mcp_session_runtime")
        dashboard_mod = importlib.import_module("tools.matrixark_mcp_dashboard")
        visibility_mod = importlib.import_module("tools.matrixark_mcp_visibility")
        deadline_pack_mod = importlib.import_module("tools.matrixark_mcp_deadline_pack")
        direct_write_queue_mod = importlib.import_module("tools.matrixark_mcp_direct_write_queue")
        direct_cache_state_mod = importlib.import_module("tools.matrixark_mcp_direct_cache_state")
        retrieval_records_mod = importlib.import_module("tools.matrixark_mcp_retrieval_records")
        temporal_retrieval_records_mod = importlib.import_module("tools.matrixark_mcp_temporal_retrieval_records")
        temporal_readiness_mod = importlib.import_module("tools.matrixark_mcp_temporal_readiness")
        temporal_proxy_readiness_mod = importlib.import_module("tools.matrixark_mcp_temporal_proxy_readiness")
        temporal_record_load_mod = importlib.import_module("tools.matrixark_mcp_temporal_record_load_runtime")
        temporal_rust_adapters_mod = importlib.import_module("tools.matrixark_mcp_temporal_rust_adapters")
        retrieve_planning_mod = importlib.import_module("tools.matrixark_mcp_retrieve_planning")
        retrieve_cache_mod = importlib.import_module("tools.matrixark_mcp_retrieve_cache")
        retrieve_candidate_builders_mod = importlib.import_module("tools.matrixark_mcp_retrieve_candidate_builders")
        native_retrieve_mod = importlib.import_module("tools.matrixark_mcp_native_retrieve")
        retrieve_continuity_mod = importlib.import_module("tools.matrixark_mcp_retrieve_continuity")
        retrieve_deadline_mod = importlib.import_module("tools.matrixark_mcp_retrieve_deadline")
        retrieve_embeddings_mod = importlib.import_module("tools.matrixark_mcp_retrieve_embeddings")
        retrieve_fallback_mod = importlib.import_module("tools.matrixark_mcp_retrieve_fallback")
        retrieve_identity_mod = importlib.import_module("tools.matrixark_mcp_retrieve_identity")
        retrieve_index_terms_mod = importlib.import_module("tools.matrixark_mcp_retrieve_index_terms")
        retrieve_scan_state_mod = importlib.import_module("tools.matrixark_mcp_retrieve_scan_state")
        retrieve_resources_mod = importlib.import_module("tools.matrixark_mcp_retrieve_resources")
        retrieve_temporal_window_mod = importlib.import_module("tools.matrixark_mcp_retrieve_temporal_window")
        retrieve_tree_filter_mod = importlib.import_module("tools.matrixark_mcp_retrieve_tree_filter")
        retrieve_metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        retrieve_audit_mod = importlib.import_module("tools.matrixark_mcp_retrieve_audit")
        retrieve_pack_builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        ingest_planning_mod = importlib.import_module("tools.matrixark_mcp_ingest_planning")
        async_ingest_mod = importlib.import_module("tools.matrixark_mcp_async_ingest")
        local_ingest_mod = importlib.import_module("tools.matrixark_mcp_local_ingest")
        ingest_message_records_mod = importlib.import_module("tools.matrixark_mcp_ingest_message_records")
        ingest_resource_records_mod = importlib.import_module("tools.matrixark_mcp_ingest_resource_records")
        ingest_resource_facts_mod = importlib.import_module("tools.matrixark_mcp_ingest_resource_facts")
        ingest_response_mod = importlib.import_module("tools.matrixark_mcp_ingest_response")
        batch_extract_planning_mod = importlib.import_module("tools.matrixark_mcp_batch_extract_planning")
        local_batch_extract_runtime_mod = importlib.import_module("tools.matrixark_mcp_local_batch_extract_runtime")
        local_backend_mod = importlib.import_module("tools.matrixark_mcp_local_backend")
        local_idempotency_mod = importlib.import_module("tools.matrixark_mcp_local_idempotency")
        local_cache_mod = importlib.import_module("tools.matrixark_mcp_local_cache")
        local_read_mod = importlib.import_module("tools.matrixark_mcp_local_read")
        local_replay_mod = importlib.import_module("tools.matrixark_mcp_local_replay")
        local_runtime_mod = importlib.import_module("tools.matrixark_mcp_local_runtime")
        local_retrieve_runtime_mod = importlib.import_module("tools.matrixark_mcp_local_retrieve_runtime")
        errors_mod = importlib.import_module("tools.matrixark_mcp_errors")
        models_mod = importlib.import_module("tools.matrixark_mcp_models")
        indexing_mod = importlib.import_module("tools.matrixark_mcp_indexing")
        storage_options_mod = importlib.import_module("tools.matrixark_mcp_storage_options")
        storage_schemas_mod = importlib.import_module("tools.matrixark_mcp_storage_schemas")
        debug_mod = importlib.import_module("tools.matrixark_mcp_debug")
        native_helpers_mod = importlib.import_module("tools.matrixark_mcp_native_helpers")
        native_side_index_mod = importlib.import_module("tools.matrixark_mcp_native_side_index")
        native_pack_policy_mod = importlib.import_module("tools.matrixark_mcp_native_pack_policy")
        native_pack_runtime_mod = importlib.import_module("tools.matrixark_mcp_native_pack_runtime")
        native_lookup_runtime_mod = importlib.import_module("tools.matrixark_mcp_native_lookup_runtime")
        env_mod = importlib.import_module("tools.matrixark_mcp_env")
        scoring_mod = importlib.import_module("tools.matrixark_mcp_scoring")
        text_mod = importlib.import_module("tools.matrixark_mcp_text")
        embeddings_mod = importlib.import_module("tools.matrixark_mcp_embeddings")
        extraction_provider_mod = importlib.import_module("tools.matrixark_mcp_extraction_provider")
        latest_values_mod = importlib.import_module("tools.matrixark_mcp_latest_values")
        event_keys_mod = importlib.import_module("tools.matrixark_mcp_event_keys")
        serving_records_mod = importlib.import_module("tools.matrixark_mcp_serving_records")
        budget_pack_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        budget_policies_mod = importlib.import_module("tools.matrixark_mcp_budget_policies")
        recall_scoring_mod = importlib.import_module("tools.matrixark_mcp_recall_scoring")
        access_scope_mod = importlib.import_module("tools.matrixark_mcp_access_scope")
        oss_understanding_mod = importlib.import_module("tools.matrixark_mcp_oss_understanding")
        extraction_norm_mod = importlib.import_module("tools.matrixark_mcp_extraction_normalization")
        segments_mod = importlib.import_module("tools.matrixark_mcp_segments")
        extraction_runtime_mod = importlib.import_module("tools.matrixark_mcp_extraction_runtime")
        prior_context_mod = importlib.import_module("tools.matrixark_mcp_prior_context")
        resources_mod = importlib.import_module("tools.matrixark_mcp_resources")
        raw_ingestion_mod = importlib.import_module("tools.matrixark_mcp_raw_ingestion")
        resource_import_task_mod = importlib.import_module("tools.matrixark_mcp_resource_import_task")
        summaries_mod = importlib.import_module("tools.matrixark_mcp_summaries")
        summary_dirty_mod = importlib.import_module("tools.matrixark_mcp_summary_dirty")
        summary_runtime_mod = importlib.import_module("tools.matrixark_mcp_summary_runtime")
        time_compression_runtime_mod = importlib.import_module("tools.matrixark_mcp_time_compression_runtime")
        runtime_config_mod = importlib.import_module("tools.matrixark_mcp_runtime_config")
        backend_readiness_mod = importlib.import_module("tools.matrixark_mcp_backend_readiness")
        envelope_keys_mod = importlib.import_module("tools.matrixark_mcp_envelope_keys")
        envelope_normalization_mod = importlib.import_module("tools.matrixark_mcp_envelope_normalization")
        hook_validation_mod = importlib.import_module("tools.matrixark_mcp_hook_validation")
        model_registry_mod = importlib.import_module("tools.matrixark_mcp_model_registry")
        scope_identity_mod = importlib.import_module("tools.matrixark_mcp_scope_identity")
        core_mod = importlib.import_module("tools.matrixark_mcp_core")

        self.assertIs(server_mod.MatrixArkServiceMetrics, metrics_mod.MatrixArkServiceMetrics)
        self.assertTrue(
            issubclass(server_mod.MatrixArkMcpServer, server_request_policy_mod.MatrixArkServerRequestPolicyMixin)
        )
        self.assertTrue(issubclass(server_mod.MatrixArkMcpServer, server_metrics_mod.MatrixArkServerMetricsMixin))
        self.assertIn("properties", schema_common_mod.SCOPE_SCHEMA)
        self.assertIn("api_key", auth_schemas_mod.ADMIN_ACCOUNT_PROPERTIES)
        self.assertTrue(any(tool.get("name") == "matrixark_admin_create_account" for tool in admin_schemas_mod.ADMIN_TOOLS))
        self.assertIs(server_mod.MatrixArkLocalAdapter, local_mod.MatrixArkLocalAdapter)
        self.assertTrue(issubclass(local_mod.MatrixArkLocalAdapter, local_portal_imports_mod.MatrixArkLocalPortalImportMixin))
        self.assertIs(server_mod.MatrixArkTemporalStoreDirectAdapter, temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        self.assertTrue(
            issubclass(
                temporal_mod.MatrixArkTemporalStoreDirectAdapter,
                temporal_direct_cache_mod.TemporalDirectCacheAdapterMixin,
            )
        )
        self.assertTrue(callable(env_mod.env_bool))
        self.assertTrue(callable(env_mod.env_float))
        self.assertIs(server_mod.MatrixArkTemporalStoreRustAdapter, temporal_mod.MatrixArkTemporalStoreRustAdapter)
        self.assertTrue(ingestion_mod.is_ingestion_tool("matrixark_ingest"))
        self.assertTrue(retrieval_mod.is_retrieval_tool("matrixark_retrieve"))
        self.assertTrue(admin_mod.is_admin_tool("matrixark_management_portal"))
        self.assertTrue(callable(backends_mod.build_mcp_adapter))
        self.assertTrue(callable(dispatch_mod.dispatch_matrixark_tool))
        self.assertTrue(callable(requests_mod.normalize_mcp_tool_request))
        self.assertIs(core_mod.require_string, validation_mod.require_string)
        self.assertIs(core_mod.require_messages, validation_mod.require_messages)
        self.assertIs(core_mod.optional_object, validation_mod.optional_object)
        self.assertIs(core_mod.optional_string, validation_mod.optional_string)
        self.assertIs(core_mod.optional_string_list, validation_mod.optional_string_list)
        self.assertIs(core_mod.integer_arg, validation_mod.integer_arg)
        self.assertIs(core_mod.float_arg, validation_mod.float_arg)
        self.assertIs(core_mod.slug_candidates_from_query, query_mod.slug_candidates_from_query)
        self.assertIs(core_mod.path_candidates_from_query, query_mod.path_candidates_from_query)
        self.assertIs(core_mod.keyword_candidates_from_query, query_mod.keyword_candidates_from_query)
        self.assertIs(core_mod.infer_query_type, query_mod.infer_query_type)
        self.assertIs(core_mod.candidate_index_terms, query_mod.candidate_index_terms)
        self.assertIs(core_mod.passes_secondary_index_filters, query_mod.passes_secondary_index_filters)
        self.assertIs(core_mod.secondary_filter_terms_to_fields, query_mod.secondary_filter_terms_to_fields)
        self.assertIs(core_mod.infer_temporal_window, query_mod.infer_temporal_window)
        self.assertIs(core_mod.build_structured_query_plan, query_mod.build_structured_query_plan)
        self.assertIs(server_mod.stable_hash, identity_mod.stable_hash)
        self.assertIs(core_mod.canonical_scope_key, identity_mod.canonical_scope_key)
        self.assertIs(core_mod.compact_context_pack_refs, context_pack_mod.compact_context_pack_refs)
        self.assertIs(core_mod.compact_context_pack_for_serving, context_pack_mod.compact_context_pack_for_serving)
        self.assertIs(core_mod.selected_ref_count_from_pack, context_pack_mod.selected_ref_count_from_pack)
        self.assertIs(core_mod.serving_ref_groups_for_pack, context_pack_mod.serving_ref_groups_for_pack)
        self.assertIs(server_mod.MatrixArkError, errors_mod.MatrixArkError)
        self.assertIs(core_mod.MatrixArkError, errors_mod.MatrixArkError)
        self.assertIs(core_mod.embedding_model_ref_for_name, models_mod.embedding_model_ref_for_name)
        self.assertIs(core_mod.compact_model_slug, models_mod.compact_model_slug)
        self.assertIs(core_mod.context_index_name, indexing_mod.context_index_name)
        self.assertIs(core_mod.limited_index_terms, indexing_mod.limited_index_terms)
        self.assertIs(core_mod.compact_context_index_postings, indexing_mod.compact_context_index_postings)
        self.assertIs(core_mod.context_index_posting_record, indexing_mod.context_index_posting_record)
        self.assertIs(core_mod.normalize_storage_options, storage_options_mod.normalize_storage_options)
        self.assertIs(storage_schemas_mod.PART_STORAGE_OPTIONS_SCHEMA, storage_schemas_mod.RECORD_STORAGE_OPTIONS_SCHEMA)
        self.assertIs(core_mod.storage_options_for_record, storage_options_mod.storage_options_for_record)
        self.assertIs(core_mod.canonical_storage_route, storage_options_mod.canonical_storage_route)
        self.assertIs(core_mod._mcp_debug_log, debug_mod._mcp_debug_log)
        self.assertIn("DEFAULT_MAX_CONTEXT_TOKENS", runtime_exports_mod.RUNTIME_CONFIG_EXPORTS)
        self.assertEqual(core_mod.DEFAULT_MAX_CONTEXT_TOKENS, runtime_exports_mod.runtime_config_exports()["DEFAULT_MAX_CONTEXT_TOKENS"])
        self.assertIs(temporal_mod._float_metric_or_default, native_helpers_mod.float_metric_or_default)
        self.assertIs(temporal_mod._compact_native_selected_refs, native_helpers_mod.compact_native_selected_refs)
        self.assertEqual(
            native_side_index_mod.merge_ref_hashes('{"ref_hashes":[3,"4","bad"]}', [4, 5]),
            [3, 4, 5],
        )
        self.assertTrue(callable(native_pack_policy_mod.native_context_pack_required_for_backend))
        self.assertTrue(callable(native_pack_runtime_mod.try_native_context_pack))
        self.assertTrue(callable(native_pack_runtime_mod.native_context_pack))
        self.assertTrue(callable(native_pack_runtime_mod.native_context_pack_fallback_blocker))
        self.assertTrue(callable(native_lookup_runtime_mod.native_index_ref_hashes))
        self.assertTrue(callable(native_lookup_runtime_mod.native_locations_for_refs))
        self.assertTrue(callable(native_lookup_runtime_mod.load_records_from_locations))
        self.assertTrue(
            issubclass(
                rust_proxy_mod.MatrixArkRustProxyClient,
                rust_proxy_cache_mixin_mod.MatrixArkRustProxyCacheMixin,
            )
        )
        self.assertIs(core_mod.hybrid_origin_score, scoring_mod.hybrid_origin_score)
        self.assertIs(core_mod.final_recall_score, scoring_mod.final_recall_score)
        self.assertIs(core_mod.business_score_for_candidate, scoring_mod.business_score_for_candidate)
        self.assertIs(core_mod.numeric_field, scoring_mod.numeric_field)
        self.assertIs(core_mod.apply_statistical_operator, scoring_mod.apply_statistical_operator)
        self.assertIs(core_mod.latest_record, scoring_mod.latest_record)
        self.assertIs(core_mod.text_from_messages, text_mod.text_from_messages)
        self.assertIs(core_mod.token_count, text_mod.token_count)
        self.assertIs(core_mod.clip_context_text, text_mod.clip_context_text)
        self.assertIs(core_mod.MAX_CONTEXT_REF_CHARS, text_mod.MAX_CONTEXT_REF_CHARS)
        self.assertIs(core_mod.embedding_for_text, embeddings_mod.embedding_for_text)
        self.assertIs(core_mod.embeddings_for_texts, embeddings_mod.embeddings_for_texts)
        self.assertIs(core_mod.embedding_model_name, embeddings_mod.embedding_model_name)
        self.assertIs(core_mod.embedding_execution_mode_name, embeddings_mod.embedding_execution_mode_name)
        self.assertIs(core_mod.embedding_fallback_used, embeddings_mod.embedding_fallback_used)
        self.assertIs(core_mod.openai_compatible_json_call, extraction_provider_mod.openai_compatible_json_call)
        self.assertIs(core_mod.parse_first_json_object, extraction_provider_mod.parse_first_json_object)
        self.assertIs(core_mod.entity_patch, entity_ops_mod.entity_patch)
        self.assertIs(core_mod.apply_entity_patch, entity_ops_mod.apply_entity_patch)
        self.assertIs(core_mod.apply_entity_patches, entity_ops_mod.apply_entity_patches)
        self.assertTrue(callable(local_cache_mod.find_latest_entity))
        self.assertTrue(callable(ingest_message_records_mod.session_l0_summary_record))
        self.assertTrue(callable(ingest_message_records_mod.context_embedding_record))
        self.assertTrue(callable(ingest_message_records_mod.context_event_record))
        self.assertTrue(callable(ingest_message_records_mod.context_event_index_terms))
        self.assertTrue(callable(ingest_message_records_mod.context_event_index_records))
        self.assertTrue(callable(ingest_resource_records_mod.skill_section_record))
        self.assertTrue(callable(ingest_resource_records_mod.skill_manifest_record))
        self.assertTrue(callable(ingest_resource_records_mod.skill_parse_debug_record))
        self.assertTrue(callable(ingest_resource_records_mod.skill_registry_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_manifest_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_registry_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_chunk_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_chunk_debug_record))
        self.assertTrue(callable(ingest_resource_records_mod.context_embedding_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_l0_summary_record))
        self.assertTrue(callable(ingest_resource_records_mod.resource_chunk_index_record))
        self.assertTrue(callable(ingest_resource_facts_mod.build_resource_fact_records))
        self.assertTrue(callable(ingest_response_mod.build_ingest_response))
        self.assertTrue(callable(ingest_response_mod.build_resource_import_queued_response))
        self.assertIs(core_mod.node_path_tuple, tree_mod.node_path_tuple)
        self.assertIs(core_mod.starts_with_path, tree_mod.starts_with_path)
        self.assertIs(core_mod.tree_first_traversal, tree_mod.tree_first_traversal)
        self.assertIs(temporal_mod.MatrixArkRustCdylibClient, rust_direct_mod.MatrixArkRustCdylibClient)
        self.assertIs(temporal_mod.MatrixArkRustProxyClient, rust_proxy_mod.MatrixArkRustProxyClient)
        self.assertEqual(rust_proxy_lane_select_mod.lane_group_for_op("matrixark_batch_append_records"), "write")
        self.assertEqual(rust_proxy_lane_select_mod.lane_group_for_op("matrixark_retrieve_context_pack"), "pack")
        self.assertEqual(rust_proxy_lane_select_mod.lane_group_for_op("batch_hget"), "read")
        self.assertIsNone(rust_proxy_lane_select_mod.pack_lane_sticky_index([{"lane": 0}], {}))
        self.assertTrue(callable(rust_proxy_metrics_record_mod.nested_float))
        self.assertTrue(callable(rust_proxy_metrics_record_mod.percentile))
        self.assertTrue(callable(rust_proxy_metrics_record_mod.record_call_metrics))
        self.assertTrue(callable(rust_proxy_metrics_snapshot_mod.metrics_snapshot))
        self.assertIs(temporal_mod.MatrixArkRustCliClient, rust_proxy_mod.MatrixArkRustCliClient)
        self.assertTrue(
            issubclass(
                local_mod.MatrixArkLocalAdapter,
                local_core_mixin_mod.MatrixArkLocalCoreMixin,
            )
        )
        adapter = local_mod.MatrixArkLocalAdapter(Path("/tmp/matrixark-module-boundary-unused.jsonl"))
        sample_scope = {"tenant_id": "t", "user_id": "u", "session_id": "s"}
        self.assertEqual(
            adapter.default_session_node_path(sample_scope),
            session_policy_mod.default_session_node_path(sample_scope),
        )
        sample_event = {
            "record_type": "context_event",
            "event_id_hash": 1,
            "envelope": {
                "kind": "message",
                "messages": [{"role": "user", "content": "hello"}],
                "scope": sample_scope,
            },
        }
        self.assertEqual(
            adapter._dashboard_message_rows([sample_event], sample_scope),
            dashboard_mod.dashboard_message_rows([sample_event], sample_scope),
        )
        self.assertTrue(callable(dashboard_mod.ingestion_dashboard))
        sample_pack = {"context_pack_id": "pack-1", "selected_refs": [], "recall_policy": {}}
        adapter_telemetry = adapter.telemetry_record_for_context_pack(
            sample_pack,
            query="hello",
            scope=sample_scope,
            audit_mode="async",
        )
        helper_telemetry = visibility_mod.telemetry_record_for_context_pack(
            sample_pack,
            query="hello",
            scope=sample_scope,
            audit_mode="async",
        )
        self.assertEqual(adapter_telemetry["query_hash"], helper_telemetry["query_hash"])
        self.assertTrue(callable(visibility_mod.append_context_pack_visibility))
        self.assertTrue(callable(deadline_pack_mod.deadline_fallback_pack))
        durable_payload = direct_write_queue_mod.direct_write_durable_payload(
            [{"record_type": "context_event"}],
            backend="temporalstore-cpp",
            storage_prefix="matrixark:mcp",
        )
        self.assertEqual(durable_payload["record_count"], 1)
        self.assertTrue(direct_write_queue_mod.direct_write_durable_field(durable_payload))
        self.assertTrue(direct_write_queue_mod.direct_write_payload_is_pending(durable_payload))
        self.assertIs(core_mod._DIRECT_RECORD_CACHE, direct_cache_state_mod._DIRECT_RECORD_CACHE)
        self.assertIs(core_mod._DIRECT_RETRIEVAL_CANDIDATE_CACHE, direct_cache_state_mod._DIRECT_RETRIEVAL_CANDIDATE_CACHE)
        self.assertIs(core_mod._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE, direct_cache_state_mod._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)
        self.assertIs(local_mod.RETRIEVAL_HOT_RECORD_TYPES, local_core_mixin_mod.RETRIEVAL_HOT_RECORD_TYPES)
        self.assertIs(local_mod.RETRIEVAL_HOT_RECORD_TYPES, retrieval_records_mod.RETRIEVAL_HOT_RECORD_TYPES)
        self.assertTrue(callable(temporal_retrieval_records_mod.retrieval_records))
        self.assertTrue(callable(temporal_retrieval_records_mod.filter_retrieval_candidates))
        self.assertTrue(callable(temporal_retrieval_records_mod.native_locations_for_selected_nodes))
        self.assertTrue(callable(temporal_readiness_mod.run_backend_readiness_gate))
        self.assertTrue(callable(temporal_proxy_readiness_mod.ensure_backend_ready))
        self.assertTrue(callable(temporal_record_load_mod.load_records_by_count))
        self.assertIs(temporal_rust_adapters_mod.MatrixArkTemporalStoreRustAdapter, temporal_mod.MatrixArkTemporalStoreRustAdapter)
        self.assertTrue(callable(retrieve_planning_mod.retrieval_audit_policy))
        self.assertTrue(callable(retrieve_planning_mod.retrieval_query_budget_plan))
        self.assertTrue(callable(retrieve_planning_mod.retrieval_ranking_limits))
        self.assertTrue(callable(retrieve_planning_mod.retrieval_stage_budgets))
        self.assertEqual(retrieve_planning_mod.RETRIEVAL_STAGE_NAMES[0], "query_understanding")
        limits = retrieve_planning_mod.retrieval_ranking_limits({"max_selected_refs": 7})
        self.assertEqual(limits.max_selected_refs, 7)
        self.assertEqual(limits.budget_fill_policy, "quality_first")
        self.assertTrue(callable(retrieve_cache_mod.context_pack_cache_key))
        self.assertTrue(callable(retrieve_cache_mod.get_cached_context_pack))
        self.assertTrue(callable(retrieve_candidate_builders_mod.event_candidate))
        self.assertTrue(callable(retrieve_candidate_builders_mod.entity_candidate))
        self.assertTrue(callable(retrieve_candidate_builders_mod.segment_candidate))
        self.assertTrue(callable(retrieve_candidate_builders_mod.compression_candidate))
        self.assertTrue(callable(retrieve_candidate_builders_mod.summary_candidate))
        self.assertTrue(callable(retrieve_candidate_builders_mod.resource_skill_candidate))
        self.assertTrue(callable(native_retrieve_mod.try_native_context_pack))
        self.assertTrue(callable(retrieve_continuity_mod.annotate_session_continuity))
        continuity_annotator = retrieve_continuity_mod.make_session_continuity_annotator(
            retrieval_scope={"session_id": "s1"},
            question_type="current_state",
        )
        self.assertEqual(
            continuity_annotator({"origin_score": 1.0}, {"scope": {"session_id": "s1"}})["session_continuity"],
            "same_session",
        )
        event_vectors: dict[int, list[float]] = {}
        empty_vectors: dict[int, list[float]] = {}
        self.assertTrue(
            retrieve_embeddings_mod.add_context_embedding_vector(
                {"record_type": "context_embedding", "embedding_type": "event_text", "ref_hash": 8, "vector": [0.5]},
                event_embedding_vectors=event_vectors,
                entity_embedding_vectors=empty_vectors,
                segment_embedding_vectors=empty_vectors,
                compression_embedding_vectors=empty_vectors,
                resource_embedding_vectors=empty_vectors,
                skill_embedding_vectors=empty_vectors,
            )
        )
        self.assertEqual(event_vectors[8], [0.5])
        self.assertTrue(callable(retrieve_fallback_mod.deadline_fallback_pack))
        by_batch: dict[object, list[str]] = {}
        by_node: dict[object, list[str]] = {}
        by_ref: dict[object, list[str]] = {}
        node_prefilter: dict[int, list[str]] = {}
        self.assertTrue(
            retrieve_index_terms_mod.add_context_index_terms(
                {"record_type": "context_index", "index_name": "entity:gpu", "ref_hashes": [5], "node_hash": 9},
                index_terms_by_batch=by_batch,
                index_terms_by_node=by_node,
                index_terms_by_ref=by_ref,
                index_terms_by_node_for_prefilter=node_prefilter,
            )
        )
        self.assertEqual(by_ref[5], ["entity:gpu"])
        self.assertEqual(node_prefilter[9], ["entity:gpu"])
        self.assertTrue(callable(retrieve_scan_state_mod.scan_context_indexes))
        self.assertTrue(callable(retrieve_scan_state_mod.scan_context_embeddings))
        scan_state = retrieve_scan_state_mod.RetrieveScanState()
        self.assertTrue(
            retrieve_scan_state_mod.add_index_terms(
                {"record_type": "context_index", "index_name": "entity:cpu", "ref_hash": 6, "node_hash": 10},
                state=scan_state,
            )
        )
        self.assertEqual(scan_state.index_terms_by_ref[6], ["entity:cpu"])
        observed_stage_metrics: list[tuple[str, float]] = []
        tracker = retrieve_deadline_mod.RetrievalDeadlineTracker(
            started_perf=0.0,
            deadline_ms=0,
            stage_budgets_ms=retrieve_planning_mod.default_stage_budgets(0),
            explicit_stage_budgets=set(),
            observe_latency=lambda name, value: observed_stage_metrics.append((name, value)),
        )
        self.assertFalse(tracker.deadline_exceeded())
        tracker.finish_stage("unit", 0.0)
        self.assertEqual(observed_stage_metrics[0][0], "retrieval_unit")
        self.assertIn("stages", tracker.stage_budget_snapshot())
        base_records = [{"record_type": "context_event", "event_id_hash": 5, "text": "old"}]
        retrieve_identity_mod.append_unique_records(
            base_records,
            [
                {"record_type": "context_event", "event_id_hash": 5, "text": "duplicate"},
                {"record_type": "context_event", "event_id_hash": 6, "text": "new"},
            ],
        )
        self.assertEqual([record["event_id_hash"] for record in base_records], [5, 6])
        latest_versions, resource_uris = retrieve_resources_mod.latest_resource_metadata(
            [
                {
                    "record_type": "resource_manifest",
                    "scope": sample_scope,
                    "resource_hash": 7,
                    "raw_uri": "old.pdf",
                    "resource_version": "v1",
                },
                {
                    "record_type": "resource_manifest",
                    "scope": sample_scope,
                    "resource_hash": 7,
                    "raw_uri": "new.pdf",
                    "resource_version": "v2",
                },
            ],
            sample_scope,
        )
        self.assertEqual(latest_versions[7], "v2")
        self.assertEqual(resource_uris[7], "new.pdf")
        admitted_events, dropped_events = retrieve_temporal_window_mod.raw_event_admission_window(
            [
                {"record_type": "context_compression_event", "node_hash": 9},
                {"record_type": "context_event", "node_hash": 9, "event_id_hash": 1, "ingestion_time_ms": 10},
                {"record_type": "context_event", "node_hash": 9, "event_id_hash": 2, "ingestion_time_ms": 20},
                {"record_type": "context_event", "node_hash": 9, "event_id_hash": 3, "ingestion_time_ms": 30},
            ],
            max_raw_events_per_node=2,
            context_event_ingestion_time_ms=lambda record: int(record.get("ingestion_time_ms") or 0),
        )
        self.assertEqual(admitted_events[9], {2, 3})
        self.assertEqual(dropped_events, 1)
        self.assertTrue(
            retrieve_tree_filter_mod.selected_by_tree(
                {"node_path": ["tenant:t", "user:u"], "node_hash": 44},
                traversal={},
                selected_paths={("tenant:t", "user:u")},
                selected_leaf_paths=set(),
                selected_node_hashes=set(),
            )
        )
        selector = retrieve_tree_filter_mod.make_tree_selector(
            traversal={},
            selected_paths={("tenant:t", "user:u")},
            selected_leaf_paths=set(),
            selected_node_hashes=set(),
        )
        self.assertTrue(selector({"node_path": ["tenant:t", "user:u"], "node_hash": 44}))
        limiter = retrieve_tree_filter_mod.CandidateFanoutLimiter(1)
        self.assertTrue(limiter.admit({"node_hash": 1}))
        self.assertFalse(limiter.admit({"node_hash": 1}))
        self.assertEqual(limiter.dropped_count, 1)
        admit, factory_limiter = retrieve_tree_filter_mod.make_candidate_admitter(1)
        self.assertTrue(admit({"node_hash": 2}))
        self.assertFalse(admit({"node_hash": 2}))
        self.assertEqual(factory_limiter.dropped_count, 1)
        self.assertTrue(callable(retrieve_metrics_mod.attach_python_retrieval_metrics))
        self.assertTrue(callable(retrieve_audit_mod.build_context_pack_audit_record))
        self.assertTrue(callable(retrieve_pack_builder_mod.build_context_pack))
        self.assertTrue(callable(retrieve_pack_builder_mod.prepare_serving_refs))
        self.assertTrue(callable(ingest_planning_mod.prepare_ingest_start))
        self.assertIs(local_ingest_mod.lightweight_async_accept, async_ingest_mod.lightweight_async_accept)
        self.assertTrue(callable(local_ingest_mod.ingest_after_start))
        self.assertTrue(callable(batch_extract_planning_mod.prepare_batch_extract_start))
        self.assertTrue(callable(local_batch_extract_runtime_mod.batch_extract_after_start))
        self.assertEqual(adapter.ensure_backend_ready()["backend"], local_backend_mod.ensure_backend_ready(adapter)["backend"])
        self.assertEqual(adapter.backend_metrics()["metrics_format"], local_backend_mod.backend_metrics(adapter)["metrics_format"])
        self.assertTrue(callable(local_backend_mod.observe_model_latency))
        self.assertTrue(callable(local_idempotency_mod.find_idempotency_record))
        self.assertTrue(callable(local_idempotency_mod.append_idempotency_record))
        self.assertTrue(callable(local_read_mod.read_all))
        self.assertTrue(callable(local_read_mod.retrieval_records))
        self.assertTrue(callable(local_replay_mod.replay))
        self.assertTrue(callable(local_replay_mod.compact_replay_record))
        self.assertTrue(callable(local_runtime_mod.init_local_runtime_state))
        self.assertTrue(callable(local_runtime_mod.write_batch))
        self.assertTrue(callable(local_runtime_mod.append))
        self.assertTrue(callable(local_runtime_mod.append_many))
        self.assertTrue(callable(local_retrieve_runtime_mod.retrieve))
        self.assertTrue(callable(session_policy_mod.default_session_node_path))
        self.assertTrue(callable(session_runtime_mod.append_session_buffer_event))
        self.assertIs(local_mod.compact_latest_value_records, latest_values_mod.compact_latest_value_records)
        self.assertIs(local_mod.latest_value_record_key, latest_values_mod.latest_value_record_key)
        self.assertIs(core_mod.context_event_time_key, event_keys_mod.context_event_time_key)
        self.assertIs(core_mod.context_event_time_index_payload, event_keys_mod.context_event_time_index_payload)
        self.assertIs(core_mod.attach_context_placement, event_keys_mod.attach_context_placement)
        self.assertTrue(callable(core_mod.materialize_serving_records))
        self.assertTrue(callable(serving_records_mod.materialize_serving_records))
        self.assertIs(core_mod.materialize_serving_records, serving_records_mod.materialize_serving_records)
        self.assertIs(core_mod.materialize_serving_record_batch, serving_records_mod.materialize_serving_record_batch)
        self.assertIs(core_mod.local_context_budget, budget_pack_mod.local_context_budget)
        self.assertIs(core_mod.local_context_refs_for_pack, budget_pack_mod.local_context_refs_for_pack)
        self.assertIs(core_mod.build_cross_session_policy, budget_pack_mod.build_cross_session_policy)
        self.assertIs(core_mod.build_shared_context_policy, budget_pack_mod.build_shared_context_policy)
        self.assertIs(budget_pack_mod.build_cross_session_policy, budget_policies_mod.build_cross_session_policy)
        self.assertIs(budget_pack_mod.build_shared_context_policy, budget_policies_mod.build_shared_context_policy)
        self.assertIs(
            budget_pack_mod.bounded_max_children_scored_per_parent,
            budget_policies_mod.bounded_max_children_scored_per_parent,
        )
        self.assertIs(core_mod.select_token_budgeted_refs, budget_pack_mod.select_token_budgeted_refs)
        self.assertIs(core_mod.bounded_max_children_scored_per_parent, budget_pack_mod.bounded_max_children_scored_per_parent)
        self.assertIs(core_mod.scope_matches, access_scope_mod.scope_matches)
        self.assertIs(core_mod.candidate_access_scope, access_scope_mod.candidate_access_scope)
        self.assertIs(core_mod.access_scope_matches_before_scoring, access_scope_mod.access_scope_matches_before_scoring)
        self.assertIs(core_mod.session_continuity_status, access_scope_mod.session_continuity_status)
        self.assertIs(core_mod.session_continuity_boost, access_scope_mod.session_continuity_boost)
        self.assertIs(core_mod.cross_session_rerank_adjustment, access_scope_mod.cross_session_rerank_adjustment)
        self.assertIs(core_mod.sharing_scope_from_candidate, access_scope_mod.sharing_scope_from_candidate)
        self.assertIs(core_mod.score_recall_candidate, recall_scoring_mod.score_recall_candidate)
        self.assertIs(core_mod.merge_ranked_paths, recall_scoring_mod.merge_ranked_paths)
        self.assertIs(core_mod.question_type_ref_boost, recall_scoring_mod.question_type_ref_boost)
        self.assertIs(core_mod.packing_sort_key, recall_scoring_mod.packing_sort_key)
        self.assertIs(core_mod.record_dropped_candidate, recall_scoring_mod.record_dropped_candidate)
        self.assertIs(core_mod.diversify_for_question_type, recall_scoring_mod.diversify_for_question_type)
        self.assertTrue(callable(oss_understanding_mod.oss_encoder_rank_labels))
        self.assertTrue(callable(oss_understanding_mod.oss_encoder_compact_extraction))
        self.assertIs(core_mod.normalize_entity_operator, extraction_norm_mod.normalize_entity_operator)
        self.assertIs(core_mod.normalize_extracted_entities, extraction_norm_mod.normalize_extracted_entities)
        self.assertIs(core_mod.normalize_extracted_segments, extraction_norm_mod.normalize_extracted_segments)
        self.assertIs(core_mod.normalize_extracted_facts, extraction_norm_mod.normalize_extracted_facts)
        self.assertTrue(callable(extraction_norm_mod.extract_batch_entities))
        self.assertIs(extraction_runtime_mod.detect_memory_segments, segments_mod.detect_memory_segments)
        self.assertIs(extraction_runtime_mod.intelligent_memory_segments, segments_mod.intelligent_memory_segments)
        self.assertIs(extraction_runtime_mod.contiguous_ranges, segments_mod.contiguous_ranges)
        self.assertTrue(callable(extraction_runtime_mod.one_pass_memory_extraction))
        self.assertTrue(callable(prior_context_mod.collect_prior_context))
        self.assertTrue(callable(prior_context_mod.prior_context_payload))
        self.assertIs(core_mod.compact_latest_context_state_records, serving_records_mod.compact_latest_context_state_records)
        self.assertIs(core_mod.sanitize_resource_metadata, resources_mod.sanitize_resource_metadata)
        self.assertIs(core_mod.resolve_raw_resource_for_ingest, resources_mod.resolve_raw_resource_for_ingest)
        self.assertIs(core_mod.rewrite_chunk_uris, resources_mod.rewrite_chunk_uris)
        self.assertIs(core_mod.should_extract_resource_fact, resources_mod.should_extract_resource_fact)
        self.assertIs(core_mod.resource_fact_entity_name, resources_mod.resource_fact_entity_name)
        self.assertEqual(raw_ingestion_mod.normalize_raw_storage_backend("matrix_object_store"), "matrixobject")
        self.assertEqual(
            raw_ingestion_mod.raw_ingestion_append_path_for_backend("s3"),
            "matrixark_raw_ingestion_s3_object_ref",
        )
        task_record = resource_import_task_mod.resource_import_task_record(
            task_hash=11,
            status="queued",
            kind="resource",
            raw_uri="s3://bucket/doc.pdf",
            requested_raw_uri="doc.pdf",
            resource_type="pdf",
            raw_storage_mode="object",
            raw_storage_policy="raw_uri_only",
            node_hash=22,
            node_path=["tenant:t", "resources"],
            scope=sample_scope,
            updated_at_ms=33,
            wait=False,
            async_default_reason="large_resource",
        )
        self.assertEqual(task_record["record_type"], "resource_import_task")
        self.assertEqual(task_record["progress"], {"stage": "queued", "percent": 0})
        self.assertFalse(task_record["wait"])
        self.assertIs(core_mod.summarize_text, summaries_mod.summarize_text)
        self.assertIs(core_mod.generate_time_compression_summary, summaries_mod.generate_time_compression_summary)
        self.assertIs(core_mod.node_l1_generation_policy, summaries_mod.node_l1_generation_policy)
        self.assertEqual(core_mod.MAX_PRIOR_MESSAGES, runtime_config_mod.MAX_PRIOR_MESSAGES)
        self.assertIs(core_mod.DEFAULT_BUSINESS_TYPE_WEIGHTS, runtime_config_mod.DEFAULT_BUSINESS_TYPE_WEIGHTS)
        self.assertIs(core_mod.matrixark_production_profile_enabled, runtime_config_mod.matrixark_production_profile_enabled)
        self.assertIs(core_mod.python_hot_cache_allowed, runtime_config_mod.python_hot_cache_allowed)
        self.assertIs(core_mod.native_candidate_prefilter_required, runtime_config_mod.native_candidate_prefilter_required)
        self.assertIs(core_mod.metaserver_reachable, backend_readiness_mod.metaserver_reachable)
        self.assertIs(core_mod.adapter_ensure_backend_ready, backend_readiness_mod.adapter_ensure_backend_ready)
        self.assertIs(core_mod.session_buffer_key, envelope_keys_mod.session_buffer_key)
        self.assertIs(core_mod.context_node_key, envelope_keys_mod.context_node_key)
        self.assertIs(core_mod.has_confirmation_context, envelope_keys_mod.has_confirmation_context)
        self.assertIs(core_mod.normalize_envelope, envelope_normalization_mod.normalize_envelope)
        self.assertIs(core_mod.validate_hook, hook_validation_mod.validate_hook)
        self.assertIs(core_mod.context_model_registry_records, model_registry_mod.context_model_registry_records)
        self.assertIs(core_mod.enrich_scope_with_identity, scope_identity_mod.enrich_scope_with_identity)
        self.assertIs(summary_runtime_mod.mark_node_summary_dirty, summary_dirty_mod.mark_node_summary_dirty)
        self.assertIs(summary_runtime_mod.node_summary_dirty_records, summary_dirty_mod.node_summary_dirty_records)
        self.assertIs(summary_runtime_mod.pending_dirty_node_records, summary_dirty_mod.pending_dirty_node_records)
        self.assertTrue(callable(summary_runtime_mod.append_node_summary_embeddings))
        self.assertTrue(callable(summary_runtime_mod.refresh_dirty_node_summaries))
        self.assertTrue(callable(summary_runtime_mod.refresh_summaries))
        self.assertTrue(callable(time_compression_runtime_mod.write_time_compression))
        self.assertTrue(callable(time_compression_runtime_mod.append_recall_reinforcement_markers))
        self.assertTrue(callable(time_compression_runtime_mod.query_time_compressions))

    def test_mcp_entrypoint_stays_small(self) -> None:
        server_lines = (TOOLS_DIR / "matrixark_mcp_server.py").read_text(encoding="utf-8").splitlines()
        self.assertLessEqual(len(server_lines), 750)

    def test_public_mcp_modules_avoid_wildcard_imports(self) -> None:
        module_paths = sorted(TOOLS_DIR.glob("matrixark_mcp*.py"))
        self.assertGreater(len(module_paths), 20)
        legacy_orchestrators: set[str] = set()
        offenders: list[str] = []
        for module_path in module_paths:
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
                    if module_path.name not in legacy_orchestrators:
                        offenders.append(f"{module_path.name}:{node.lineno}")
        self.assertEqual([], offenders)

    def test_pyproject_exposes_matrixark_console_scripts(self) -> None:
        pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('matrixark-mcp-server = "tools.matrixark_mcp_server:main"', pyproject_text)
        self.assertIn('matrixark-http-portal = "tools.matrixark_admin:http_portal_main"', pyproject_text)
        self.assertIn('matrixark-agent-hook = "tools.matrixark_agent_hook:main"', pyproject_text)
        self.assertIn('matrixark-admin = "tools.matrixark_admin:main"', pyproject_text)

    def test_production_defaults_doc_records_control_plane_boundary(self) -> None:
        defaults_doc = (REPO_ROOT / "docs" / "matrixark_mcp_production_defaults.md").read_text(encoding="utf-8")
        required_snippets = [
            "Python remains the MCP/HTTP/control-plane layer",
            "Native C++ or Rust MCP servers are future optimizations, not a v1 requirement",
            "C++ and Rust TemporalStore remain the serving engines",
            "compact and audit-light",
            "Full replay/debug audit is opt-in",
            "Cloud mode requires an API key or trusted SSO gateway identity",
            "Local/dev mode may use generated local scope defaults",
            "Codex, Claude, Cursor, OpenClaw, OpenCode, Aider, Continue, Cline/Roo",
            "visible local context only",
            "before LLM: `matrixark_retrieve`",
            "after answer/tool: `matrixark_ingest`",
            "resource added: import resource or skill",
            "feedback: `matrixark_feedback`",
            "session boundary: `matrixark_session_commit`",
            "ingest/retrieve QPS",
            "p50/p95/p99 latency",
            "partial ContextPack count",
            "audit write failures",
            "dirty summary lag",
            "resource import lag",
            "model fallback flags",
            "backend readiness",
            "messages;",
            "resources;",
            "skills;",
            "events/entities;",
            "ContextPacks;",
            "users;",
            "API keys;",
            "audit logs.",
            "scoped, redacted, paged tables",
            "no private checkout paths",
            "no local credentials or secrets",
            "no vendored build outputs",
            "reproducible local validation commands",
            "C++/Rust scale matrix gate",
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, defaults_doc)

    def test_metrics_prometheus_contract_includes_production_signals(self) -> None:
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        metrics = metrics_mod.MatrixArkServiceMetrics()
        metrics.observe_operation("ingest", "ok", 12.5)
        metrics.observe_operation("retrieve", "ok", 25.0, timeout=True)
        metrics.observe_retrieve_result({"partial": True, "tokens": {"remote_budget": 100, "remote": 80}})
        metrics.observe_model_latency("embedding", 8.0)
        metrics.update_gauges(dirty_summary_lag_ms=10, resource_import_lag_ms=20, queue_depth=3, audit_write_failures=1)
        metrics.observe_backend_ready(True, "ready")
        metrics.update_model_fallback_flags(embedding=True, reader=False, judge=True)

        prometheus = metrics.render_prometheus(backend="temporalstore-direct", storage_mode="shared_store")
        required_metrics = [
            'matrixark_backend_info{backend="temporalstore-direct",storage_mode="shared_store"} 1',
            "matrixark_backend_ready",
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="ingest"}',
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.5"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.95"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.99"}',
            "matrixark_timeouts_total",
            "matrixark_partial_context_pack_total",
            "matrixark_resource_import_queue_depth",
            "matrixark_audit_write_failures_total",
            "matrixark_dirty_summary_lag_ms",
            "matrixark_resource_import_lag_ms",
            "matrixark_token_pressure_ratio",
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="embedding"} 1',
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="judge"} 1',
            "matrixark_model_latency_ms",
        ]
        for metric in required_metrics:
            self.assertIn(metric, prometheus)

    def test_request_boundary_generates_idempotency_and_validates_storage(self) -> None:
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")
        args = requests_mod.normalize_mcp_tool_request(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                "storage_options": {"route": "raft_async"},
                "agent_name": "codex",
            },
            write_tools={"matrixark_ingest"},
        )
        self.assertRegex(args["idempotency_key"], r"^auto:matrixark_ingest:")
        self.assertEqual("raft", args["storage_options"]["storage_family"])
        self.assertEqual("async", args["storage_options"]["write_mode"])
        self.assertEqual("acct_local", args["scope"]["account_id"])
        self.assertEqual("tenant_codex", args["scope"]["tenant_id"])
        self.assertIn("scope_key", args["scope"])
        self.assertGreater(args["scope"]["tenant_hash"], 0)

        with self.assertRaises(Exception):
            requests_mod.normalize_mcp_tool_request(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "bad"}], "storage_options": {"route": "not_real"}},
                write_tools={"matrixark_ingest"},
            )

    def test_all_mutating_mcp_routes_include_idempotency_boundary(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        self.assertIn("matrixark_auth_sso_login", server_mod.MatrixArkMcpServer.IDEMPOTENT_WRITE_TOOLS)

    def test_core_module_has_no_duplicate_top_level_symbols(self) -> None:
        module_path = TOOLS_DIR / "matrixark_mcp_core.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        seen: dict[str, int] = {}
        duplicates: dict[str, list[int]] = {}
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            previous = seen.get(node.name)
            if previous is not None:
                duplicates.setdefault(node.name, [previous]).append(node.lineno)
            else:
                seen[node.name] = node.lineno
        self.assertEqual({}, duplicates)

    def test_token_selector_stops_on_hard_deadline(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        calls = {"count": 0}

        def deadline_after_first_candidate() -> bool:
            calls["count"] += 1
            return calls["count"] > 1

        candidates = [
            {
                "ref_type": "event",
                "ref_hash": index,
                "score": 0.9 - (index * 0.01),
                "text": f"important answer-bearing context {index}",
            }
            for index in range(5)
        ]
        selected, used_tokens, dropped = core_mod.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=200,
            auxiliary_quota=0,
            deadline_exceeded=deadline_after_first_candidate,
            deadline_reason="test_deadline",
        )
        self.assertEqual(1, len(selected))
        self.assertGreater(used_tokens, 0)
        self.assertTrue(dropped["deadline_exceeded"])
        self.assertEqual("test_deadline", dropped["deadline_reason"])
        self.assertEqual(4, dropped["deadline"])

    def test_serving_pack_lifts_repeated_session_continuity(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-1",
                "selected_refs": [
                    {"ref_type": "event", "text": "same session fact", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "entity", "text": "same session entity", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "summary", "text": "cross session summary", "token_estimate": 4, "session_continuity": "cross_session"},
                ],
                "used_context_tokens": 10,
            }
        )
        self.assertNotIn("selected_refs", pack)
        self.assertNotIn("selected_ref_groups", pack)
        self.assertIn("groups", pack)
        self.assertEqual("same_session", pack["defaults"]["session_continuity"])
        self.assertEqual(2, pack["counts"]["session_continuity"]["same_session"])
        event_group = next(group for group in pack["groups"] if group["type"] == "event")
        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        self.assertNotIn("ref_type", event_group["items"][0])
        self.assertNotIn("session_continuity", event_group["items"][0])
        self.assertEqual("cross_session", summary_group["items"][0]["session_continuity"])
        self.assertEqual(3, event_group["items"][0]["tokens"])


class MatrixArkMcpProtocolHardeningTest(unittest.TestCase):
    def _server(self):
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        event_log = Path(tmpdir.name) / "events.jsonl"
        return server_mod.MatrixArkMcpServer(server_mod.MatrixArkLocalAdapter(event_log), line_json=True)

    def test_jsonrpc_errors_are_specific_and_stable(self) -> None:
        server = self._server()
        invalid = server.handle({"jsonrpc": "1.0", "id": 1, "method": "tools/list"})
        self.assertEqual(-32600, invalid["error"]["code"])

        missing = server.handle({"jsonrpc": "2.0", "id": 2, "method": "unknown/method"})
        self.assertEqual(-32601, missing["error"]["code"])

        bad_params = server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": []})
        self.assertEqual(-32602, bad_params["error"]["code"])

    def test_initialize_reports_server_identity_and_version(self) -> None:
        server = self._server()
        response = server.handle({"jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {}})
        self.assertEqual("matrixark-context", response["result"]["serverInfo"]["name"])
        self.assertRegex(response["result"]["serverInfo"]["version"], r"^\d+\.\d+\.\d+$")
        self.assertIn("tools", response["result"]["capabilities"])

    def test_call_tool_does_not_mutate_caller_arguments(self) -> None:
        server = self._server()
        args = {
            "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
            "agent_hook": {
                "hook_id": "hook-1",
                "hook_type": "before_llm",
                "source": "codex",
                "auto_captured": True,
                "observed_at_ms": 1,
            },
        }
        result = server.call_tool("matrixark_ingest", args)
        self.assertIn(result["status"], {"accepted", "ok"})
        self.assertIn("event_id_hash", result)
        self.assertIsInstance(result["idempotency_key_hash"], int)
        self.assertFalse(result["idempotent_replay"])
        self.assertIn("agent_hook", args)

    def test_retrieval_audit_is_off_by_default(self) -> None:
        server = self._server()
        server.call_tool("matrixark_ingest", {"messages": [{"role": "user", "content": "Alice approved the GPU budget."}]})
        pack = server.call_tool("matrixark_retrieve", {"query": "what did Alice approve?"})
        self.assertNotIn("operational_visibility_policy", pack)
        self.assertNotIn("question_type", pack)
        self.assertNotIn("recall_policy", pack)
        records = server.adapter.read_all()
        self.assertFalse(any(record.get("record_type") == "context_pack_audit" for record in records))
        self.assertFalse(any(record.get("record_type") == "context_pack_telemetry" for record in records))

    def test_storage_options_are_validated_stored_and_audited(self) -> None:
        core_modules = []
        serving_modules = []
        for module_name in ("matrixark_mcp_core", "tools.matrixark_mcp_core"):
            try:
                module = importlib.import_module(module_name)
                core_modules.append((module, module.ENABLE_CONTEXT_DEBUG_RECORDS))
                module.ENABLE_CONTEXT_DEBUG_RECORDS = True
            except ModuleNotFoundError:
                pass
        for module_name in ("matrixark_mcp_serving_records", "tools.matrixark_mcp_serving_records"):
            try:
                module = importlib.import_module(module_name)
                serving_modules.append((module, module.ENABLE_CONTEXT_DEBUG_RECORDS))
                module.ENABLE_CONTEXT_DEBUG_RECORDS = True
            except ModuleNotFoundError:
                pass
        server = self._server()
        try:
            ingest = server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                    "storage_options": {"oplog_mode": "async", "raft_mode": True, "consistency": "linearizable"},
                },
            )
        finally:
            for module, old_debug in core_modules:
                module.ENABLE_CONTEXT_DEBUG_RECORDS = old_debug
            for module, old_debug in serving_modules:
                module.ENABLE_CONTEXT_DEBUG_RECORDS = old_debug
        self.assertEqual("accepted", ingest["status"])
        records = server.adapter.read_all()
        event = next(record for record in records if record.get("record_type") == "context_event")
        debug = next(
            record
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_hash") == event.get("event_id_hash")
        )
        storage_options = debug["debug_payload"]["storage_options"]
        self.assertEqual("async", storage_options["oplog_mode"])
        self.assertEqual("raft", storage_options["storage_mode"])
        self.assertEqual("raft", storage_options["replication_mode"])
        self.assertTrue(storage_options["raft_mode"])
        self.assertEqual("raft_async", storage_options["route"])
        self.assertEqual("raft_async", event["storage_route"]["route"])
        self.assertEqual("raft", event["storage_route"]["storage_mode"])
        self.assertEqual("async", event["storage_route"]["oplog_mode"])
        self.assertEqual("raft", event["storage_route"]["storage_family"])
        self.assertEqual("async", event["storage_route"]["write_mode"])
        self.assertTrue(event["storage_route"]["background_write"])
        self.assertEqual("ack_after_memory_append", event["storage_route"]["write_ack_policy"])
        self.assertEqual("context_node", event["context_event_parent_type"])
        self.assertNotIn("node_id", event)
        self.assertEqual(event["node_hash"], event["context_event_parent_hash"])
        self.assertNotIn("parent_type", event)
        self.assertNotIn("parent_hash", event)
        self.assertIsInstance(event["context_event_key"], int)
        self.assertGreater(event["context_event_key"], 0)
        self.assertIsInstance(event["timestamp_key_ms"], int)
        self.assertGreater(event["timestamp_key_ms"], 0)
        self.assertNotIn("event_time_key", event)

        route_cases = [
            ("shared_store_async", "shared_store", "shared_store", "async", False),
            ("shared_store_sync", "shared_store", "shared_store", "sync", False),
            ("raft_async", "raft", "raft", "async", True),
            ("raft_sync", "raft", "raft", "sync", True),
        ]
        for route, storage_mode, replication_mode, oplog_mode, raft_mode in route_cases:
            normalized = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options({"storage_options": {"route": route}})
            self.assertEqual(route, normalized["route"])
            self.assertEqual(storage_mode, normalized["storage_mode"])
            self.assertEqual(replication_mode, normalized["replication_mode"])
            self.assertEqual(oplog_mode, normalized["oplog_mode"])
            self.assertEqual(oplog_mode, normalized["write_mode"])
            self.assertEqual(storage_mode, normalized["storage_family"])
            self.assertEqual(raft_mode, normalized["raft_mode"])
            self.assertEqual(route, normalized["route_key"])
            self.assertEqual(oplog_mode == "async", normalized["background_write"])
            self.assertEqual("ack_after_durable_commit" if oplog_mode == "sync" else "ack_after_memory_append", normalized["write_ack_policy"])

        friendly = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "storage_options": {
                    "storage_family": "shared_store",
                    "write_mode": "sync",
                    "consistency": "read_your_writes",
                }
            }
        )
        self.assertEqual("shared_store_sync", friendly["route"])
        self.assertEqual("shared_store", friendly["storage_family"])
        self.assertEqual("sync", friendly["write_mode"])
        self.assertFalse(friendly["background_write"])

        top_level_alias = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "temporalstore_storage_family": "raft",
                "temporalstore_write_mode": "sync",
            }
        )
        self.assertEqual("raft_sync", top_level_alias["route"])
        self.assertEqual("raft", top_level_alias["storage_family"])
        self.assertEqual("sync", top_level_alias["write_mode"])
        self.assertTrue(top_level_alias["raft_mode"])

        with self.assertRaisesRegex(Exception, "background_write cannot be true"):
            importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "write_mode": "sync", "background_write": True}}
            )

        pack = server.call_tool(
            "matrixark_retrieve",
            {
                "query": "Who approved the GPU budget?",
                "storage_options": {"storage_mode": "shared_store", "oplog_mode": "sync"},
                "audit_mode": "full",
            },
        )
        self.assertNotIn("recall_policy", pack)
        audit = next(record for record in reversed(server.adapter.read_all()) if record.get("record_type") == "context_pack_audit")
        self.assertEqual("shared_store", audit["recall_policy_summary"]["storage_route"]["storage_family"])
        self.assertEqual("sync", audit["recall_policy_summary"]["storage_route"]["write_mode"])
        self.assertEqual("debug", audit["storage_options"]["storage_part"])

    def test_invalid_storage_options_are_rejected(self) -> None:
        server = self._server()
        with self.assertRaises(Exception):
            server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "hello"}],
                    "storage_options": {"oplog_mode": "fast"},
                },
            )

        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        with self.assertRaisesRegex(Exception, "exactly one storage_family"):
            core_mod.normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "storage_mode": "shared_store"}}
            )

    def test_live_backend_rejects_unconfigured_storage_family(self) -> None:
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        adapter = object.__new__(temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        adapter._supported_storage_families = {"default", "shared_store"}
        with self.assertRaisesRegex(Exception, "not configured"):
            adapter._validate_storage_routes_available(
                [{"record_type": "context_event", "storage_route": {"storage_family": "raft"}}]
            )



if __name__ == "__main__":
    unittest.main()
