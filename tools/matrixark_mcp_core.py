#!/usr/bin/env python3
"""MatrixArk MCP server for LLM context ingestion and retrieval.

This is intentionally dependency-free. It implements the small JSON-RPC subset
needed by MCP clients over stdio, and keeps the storage boundary behind a local
adapter that can be replaced with TemporalStore RPC calls later.
"""

from __future__ import annotations

import argparse
import importlib
import select
import shutil
import subprocess
import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from tools.matrixark_skill_parser import parse_skill
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from matrixark_skill_parser import parse_skill


Json = dict[str, Any]


try:
    from tools.matrixark_mcp_errors import MatrixArkError, is_retryable_temporalstore_error
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError, is_retryable_temporalstore_error


try:
    from tools.matrixark_mcp_debug import _mcp_debug_log, mcp_debug_log
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_debug import _mcp_debug_log, mcp_debug_log


try:
    from tools.matrixark_mcp_validation import (
        float_arg,
        integer_arg,
        optional_object,
        optional_string,
        optional_string_list,
        require_messages,
        require_string,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_validation import (
        float_arg,
        integer_arg,
        optional_object,
        optional_string,
        optional_string_list,
        require_messages,
        require_string,
    )


try:
    from tools.matrixark_mcp_models import compact_model_slug, embedding_model_ref_for_name
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_models import compact_model_slug, embedding_model_ref_for_name

try:
    from tools.matrixark_mcp_embeddings import (
        EMBEDDING_DIM,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        oss_embedding_for_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_embeddings import (
        EMBEDDING_DIM,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        oss_embedding_for_text,
    )

try:
    from tools.matrixark_mcp_extraction_provider import (
        EXTRACTION_LLM_MAX_TOKENS,
        EXTRACTION_LLM_MODEL,
        openai_compatible_json_call,
        parse_first_json_object,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_extraction_provider import (
        EXTRACTION_LLM_MAX_TOKENS,
        EXTRACTION_LLM_MODEL,
        openai_compatible_json_call,
        parse_first_json_object,
    )


try:
    from tools.matrixark_mcp_summaries import (
        SUMMARY_LLM_MAX_TOKENS,
        SUMMARY_LLM_MODEL,
        SUMMARY_LLM_PROVIDER,
        summary_provider,
        synthesize_context_node_summary,
        TIME_COMPRESSION_REQUIRE_LLM_SUMMARY,
        TIME_COMPRESSION_SUMMARY_API_KEY_ENV,
        TIME_COMPRESSION_SUMMARY_BASE_URL,
        TIME_COMPRESSION_SUMMARY_MODEL,
        TIME_COMPRESSION_SUMMARY_PROVIDER,
        TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC,
        deterministic_time_compression_summary,
        estimated_context_tokens,
        generate_time_compression_summary,
        node_l1_generation_policy,
        summarize_text,
        time_compression_summary_provider_name,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summaries import (
        SUMMARY_LLM_MAX_TOKENS,
        SUMMARY_LLM_MODEL,
        SUMMARY_LLM_PROVIDER,
        summary_provider,
        synthesize_context_node_summary,
        TIME_COMPRESSION_REQUIRE_LLM_SUMMARY,
        TIME_COMPRESSION_SUMMARY_API_KEY_ENV,
        TIME_COMPRESSION_SUMMARY_BASE_URL,
        TIME_COMPRESSION_SUMMARY_MODEL,
        TIME_COMPRESSION_SUMMARY_PROVIDER,
        TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC,
        deterministic_time_compression_summary,
        estimated_context_tokens,
        generate_time_compression_summary,
        node_l1_generation_policy,
        summarize_text,
        time_compression_summary_provider_name,
    )

try:
    from tools.matrixark_mcp_tree import (
        node_path_tuple,
        node_prefixes,
        normalized_node_path,
        starts_with_path,
        top_scored_nodes,
        tree_first_traversal,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_tree import (
        node_path_tuple,
        node_prefixes,
        normalized_node_path,
        starts_with_path,
        top_scored_nodes,
        tree_first_traversal,
    )

try:
    from tools.matrixark_mcp_entity_ops import (
        apply_entity_patch,
        apply_entity_patches,
        best_span_by_edit_distance,
        edit_distance,
        entity_patch,
        parse_entity_patch,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_entity_ops import (
        apply_entity_patch,
        apply_entity_patches,
        best_span_by_edit_distance,
        edit_distance,
        entity_patch,
        parse_entity_patch,
    )

try:
    from tools.matrixark_mcp_event_keys import (
        CONTEXT_TIMELINE_FANOUT,
        attach_context_event_time_key,
        attach_context_placement,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_time_key,
        context_event_timestamp_ms,
        context_placement_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_event_keys import (
        CONTEXT_TIMELINE_FANOUT,
        attach_context_event_time_key,
        attach_context_placement,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_time_key,
        context_event_timestamp_ms,
        context_placement_key,
    )


try:
    from tools.matrixark_mcp_identity import (
        MATRIXARK_ADMIN_SCOPES,
        MATRIXARK_ALL_SCOPES,
        MATRIXARK_CONTEXT_SCOPES,
        MATRIXARK_ROLE_SCOPE_LIMITS,
        MATRIXARK_TOOL_SCOPES,
        canonical_account_id,
        canonical_scope_key,
        canonical_tenant_id,
        identity_hashes,
        json_text,
        local_account_user_id,
        local_agent_name,
        local_identity_defaults,
        make_api_key,
        node_id_ref,
        normalize_matrixark_role,
        now_ms,
        parse_scope_key,
        role_allows_scopes,
        safe_identifier,
        scope_from_serving_record,
        scope_key_from_hashes,
        scope_key_matches_query,
        scope_key_prefix_for_query,
        secret_hash,
        serving_scope_ref,
        session_scope_mode,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import (
        MATRIXARK_ADMIN_SCOPES,
        MATRIXARK_ALL_SCOPES,
        MATRIXARK_CONTEXT_SCOPES,
        MATRIXARK_ROLE_SCOPE_LIMITS,
        MATRIXARK_TOOL_SCOPES,
        canonical_account_id,
        canonical_scope_key,
        canonical_tenant_id,
        identity_hashes,
        json_text,
        local_account_user_id,
        local_agent_name,
        local_identity_defaults,
        make_api_key,
        node_id_ref,
        normalize_matrixark_role,
        now_ms,
        parse_scope_key,
        role_allows_scopes,
        safe_identifier,
        scope_from_serving_record,
        scope_key_from_hashes,
        scope_key_matches_query,
        scope_key_prefix_for_query,
        secret_hash,
        serving_scope_ref,
        session_scope_mode,
        stable_hash,
    )


try:
    from tools.matrixark_mcp_context_pack import (
        compact_context_pack_audit_record,
        compact_context_pack_policy,
        compact_context_pack_ref,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_context_pack_for_serving,
        compact_recall_policy_for_audit,
        compact_refs_for_audit,
        default_session_continuity_for_pack,
        selected_context_class_counts,
        selected_ref_count_from_pack,
        serving_ref_for_pack,
        serving_ref_groups_for_pack,
        serving_refs_for_pack,
        session_continuity_counts,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_context_pack import (
        compact_context_pack_audit_record,
        compact_context_pack_policy,
        compact_context_pack_ref,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_context_pack_for_serving,
        compact_recall_policy_for_audit,
        compact_refs_for_audit,
        default_session_continuity_for_pack,
        selected_context_class_counts,
        selected_ref_count_from_pack,
        serving_ref_for_pack,
        serving_ref_groups_for_pack,
        serving_refs_for_pack,
        session_continuity_counts,
    )


try:
    from tools.matrixark_mcp_indexing import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        MAX_INDEX_TERMS_PER_RESOURCE_FACT,
        MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION,
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        MAX_SECONDARY_INDEX_TERMS_PER_RECORD,
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        SECONDARY_INDEX_TIME_BUCKET_MS,
        compact_context_index_postings,
        context_index_capability,
        context_index_posting_bucket,
        context_index_posting_record,
        context_index_record_node_hashes,
        context_index_record_ref_hashes,
        context_index_ref_hashes,
        context_index_time_bucket,
        context_index_timestamp_key,
        context_index_name,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
        non_default_classification,
        normalized_index_value,
        ordered_unique_any,
        secondary_index_budget_summary,
        take_secondary_index_terms,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_indexing import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        MAX_INDEX_TERMS_PER_RESOURCE_FACT,
        MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION,
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        MAX_SECONDARY_INDEX_TERMS_PER_RECORD,
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        SECONDARY_INDEX_TIME_BUCKET_MS,
        compact_context_index_postings,
        context_index_capability,
        context_index_posting_bucket,
        context_index_posting_record,
        context_index_record_node_hashes,
        context_index_record_ref_hashes,
        context_index_ref_hashes,
        context_index_time_bucket,
        context_index_timestamp_key,
        context_index_name,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
        non_default_classification,
        normalized_index_value,
        ordered_unique_any,
        secondary_index_budget_summary,
        take_secondary_index_terms,
    )


try:
    from tools.matrixark_mcp_storage_options import (
        DEFAULT_ASYNC_INGEST_STORAGE_OPTIONS,
        KNOWN_PART_STORAGE_KEYS,
        KNOWN_RECORD_STORAGE_KEYS,
        STORAGE_ROUTE_PRESETS,
        canonical_storage_route,
        default_async_ingest_storage_options,
        normalize_part_storage_options,
        normalize_record_storage_options,
        normalize_storage_options,
        storage_options_for_record,
        storage_part_for_record,
        storage_record_kind,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_storage_options import (
        DEFAULT_ASYNC_INGEST_STORAGE_OPTIONS,
        KNOWN_PART_STORAGE_KEYS,
        KNOWN_RECORD_STORAGE_KEYS,
        STORAGE_ROUTE_PRESETS,
        canonical_storage_route,
        default_async_ingest_storage_options,
        normalize_part_storage_options,
        normalize_record_storage_options,
        normalize_storage_options,
        storage_options_for_record,
        storage_part_for_record,
        storage_record_kind,
    )


try:
    from tools.matrixark_mcp_serving_records import (
        COMPACT_SCOPE_RECORD_TYPES,
        COMPACT_TIMESTAMP_RECORD_TYPES,
        HOT_SERVING_RECORD_TYPES,
        attach_storage_route,
        compact_latest_context_state_records,
        compact_record_lifecycle_fields,
        compact_record_scope,
        compact_storage_record,
        latest_context_state_key,
        materialize_serving_record_batch,
        materialize_serving_records,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import (
        COMPACT_SCOPE_RECORD_TYPES,
        COMPACT_TIMESTAMP_RECORD_TYPES,
        HOT_SERVING_RECORD_TYPES,
        attach_storage_route,
        compact_latest_context_state_records,
        compact_record_lifecycle_fields,
        compact_record_scope,
        compact_storage_record,
        latest_context_state_key,
        materialize_serving_record_batch,
        materialize_serving_records,
    )



try:
    from tools.matrixark_mcp_resources import (
        DEBUG_RESOURCE_METADATA_FIELDS,
        ENABLE_GENERIC_RESOURCE_FACTS,
        MAX_RESOURCE_FACT_CHUNKS,
        MAX_RESOURCE_FACTS_PER_CHUNK,
        MAX_RESOURCE_FACTS_PER_RESOURCE,
        RAW_BYTE_METADATA_FIELDS,
        RESOURCE_FACT_KEYWORDS,
        RESOURCE_FACT_SCHEMAS,
        SERVING_RESOURCE_METADATA_FIELDS,
        aggregate_parse_warnings_from_chunks,
        cleanup_temp_paths,
        debug_resource_metadata,
        deployment_scope_from_args,
        download_s3_to_file,
        extract_resource_fact_value,
        infer_resource_suffix,
        is_s3_uri,
        matched_resource_fact_schemas,
        parse_s3_uri,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_fact_entity_name,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        sanitize_resource_metadata,
        serving_resource_metadata,
        should_extract_resource_fact,
        source_locator_from_ref,
        source_ref_from_locator,
        upload_file_to_s3,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_resources import (
        DEBUG_RESOURCE_METADATA_FIELDS,
        ENABLE_GENERIC_RESOURCE_FACTS,
        MAX_RESOURCE_FACT_CHUNKS,
        MAX_RESOURCE_FACTS_PER_CHUNK,
        MAX_RESOURCE_FACTS_PER_RESOURCE,
        RAW_BYTE_METADATA_FIELDS,
        RESOURCE_FACT_KEYWORDS,
        RESOURCE_FACT_SCHEMAS,
        SERVING_RESOURCE_METADATA_FIELDS,
        aggregate_parse_warnings_from_chunks,
        cleanup_temp_paths,
        debug_resource_metadata,
        deployment_scope_from_args,
        download_s3_to_file,
        extract_resource_fact_value,
        infer_resource_suffix,
        is_s3_uri,
        matched_resource_fact_schemas,
        parse_s3_uri,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_fact_entity_name,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        sanitize_resource_metadata,
        serving_resource_metadata,
        should_extract_resource_fact,
        source_locator_from_ref,
        source_ref_from_locator,
        upload_file_to_s3,
    )

try:
    from tools.matrixark_mcp_scoring import (
        business_instance_weight,
        business_score_for_candidate,
        business_type_score,
        clamp01,
        cosine,
        final_recall_score,
        hybrid_origin_score,
        apply_statistical_operator,
        latest_record,
        normalized_dense_score,
        numeric_field,
        sparse_lexical_score,
        time_decay_score,
        tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_scoring import (
        business_instance_weight,
        business_score_for_candidate,
        business_type_score,
        clamp01,
        cosine,
        final_recall_score,
        hybrid_origin_score,
        apply_statistical_operator,
        latest_record,
        normalized_dense_score,
        numeric_field,
        sparse_lexical_score,
        time_decay_score,
        tokens,
    )

try:
    from tools.matrixark_mcp_text import (
        MAX_CONTEXT_REF_CHARS,
        clip_context_text,
        text_from_messages,
        token_count,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_text import (
        MAX_CONTEXT_REF_CHARS,
        clip_context_text,
        text_from_messages,
        token_count,
    )


try:
    from tools.matrixark_mcp_query import (
        candidate_index_terms,
        infer_query_type,
        infer_secondary_index_filter_groups,
        oss_encoder_query_type,
        oss_encoder_secondary_index_filter_groups,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        build_structured_query_plan,
        deterministic_secondary_index_filter_groups,
        infer_temporal_window,
        keyword_candidates_from_query,
        path_candidates_from_query,
        secondary_filter_terms_to_fields,
        slug_candidates_from_query,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_query import (
        candidate_index_terms,
        infer_query_type,
        infer_secondary_index_filter_groups,
        oss_encoder_query_type,
        oss_encoder_secondary_index_filter_groups,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        build_structured_query_plan,
        deterministic_secondary_index_filter_groups,
        infer_temporal_window,
        keyword_candidates_from_query,
        path_candidates_from_query,
        secondary_filter_terms_to_fields,
        slug_candidates_from_query,
    )


try:
    from tools.matrixark_mcp_backend_readiness import (
        adapter_ensure_backend_ready,
        metaserver_reachable,
        parse_host_port,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_backend_readiness import (
        adapter_ensure_backend_ready,
        metaserver_reachable,
        parse_host_port,
    )


try:
    from tools.matrixark_mcp_envelope_keys import (
        context_node_key,
        explicit_context_pack_id,
        has_confirmation_context,
        session_buffer_key,
        session_buffer_key_from_scope,
        session_key,
        user_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_envelope_keys import (
        context_node_key,
        explicit_context_pack_id,
        has_confirmation_context,
        session_buffer_key,
        session_buffer_key_from_scope,
        session_key,
        user_key,
    )


try:
    from tools.matrixark_mcp_hook_validation import validate_hook
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_hook_validation import validate_hook


try:
    from tools.matrixark_mcp_envelope_normalization import normalize_envelope
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_envelope_normalization import normalize_envelope


try:
    from tools.matrixark_mcp_model_registry import (
        context_model_registry_record,
        context_model_registry_records,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_model_registry import (
        context_model_registry_record,
        context_model_registry_records,
    )


try:
    from tools.matrixark_mcp_scope_identity import enrich_scope_with_identity
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_scope_identity import enrich_scope_with_identity


try:
    from tools.matrixark_mcp_prior_context import (
        collect_prior_context,
        message_from_event_record,
        prior_context_payload,
        session_summary_for_events,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_prior_context import (
        collect_prior_context,
        message_from_event_record,
        prior_context_payload,
        session_summary_for_events,
    )


try:
    from tools.matrixark_mcp_extraction_normalization import (
        normalize_entity_operator,
        normalize_extracted_entities,
        normalize_extracted_facts,
        normalize_extracted_segments,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_extraction_normalization import (
        normalize_entity_operator,
        normalize_extracted_entities,
        normalize_extracted_facts,
        normalize_extracted_segments,
    )


_RUNTIME_CONFIG_EXPORTS = (
    "AUDIT_DEBUG_PAYLOAD",
    "BACKEND_READINESS_BACKOFF_MS",
    "BACKEND_READINESS_CONNECT_TIMEOUT_MS",
    "BACKEND_READINESS_TIMEOUT_MS",
    "CONTEXT_PACK_DEBUG_REFS",
    "CONTEXT_TELEMETRY_WRITE_MODE",
    "DEFAULT_BUDGET_FILL_POLICY",
    "DEFAULT_BUSINESS_TYPE_WEIGHTS",
    "DEFAULT_BUSINESS_WEIGHT",
    "DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO",
    "DEFAULT_CROSS_SESSION_BUDGET_RATIO",
    "DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO",
    "DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO",
    "DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS",
    "DEFAULT_CROSS_SESSION_MAX_CANDIDATES",
    "DEFAULT_CROSS_SESSION_MAX_SESSIONS",
    "DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS",
    "DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS",
    "DEFAULT_CROSS_SESSION_MIN_SCORE",
    "DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO",
    "DEFAULT_CROSS_SESSION_PARALLELISM",
    "DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES",
    "DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE",
    "DEFAULT_ENTITY_MERGE_OPERATOR",
    "DEFAULT_MAX_CANDIDATES_PER_NODE",
    "DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT",
    "DEFAULT_MAX_CONTEXT_TOKENS",
    "DEFAULT_MAX_GLOBAL_CANDIDATES",
    "DEFAULT_MAX_SELECTED_REFS",
    "DEFAULT_RETRIEVAL_MIN_SCORE",
    "DEFAULT_SHARED_CONTEXT_MIN_SCORE",
    "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO",
    "DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS",
    "DEFAULT_SHARED_SKILL_BUDGET_RATIO",
    "DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS",
    "DEFAULT_TIME_DECAY_HALFLIFE_MS",
    "DEFAULT_TIME_DECAY_TOLERANCE_MS",
    "DEFAULT_TIME_WEIGHT",
    "DEFAULT_TOP_K_PER_LAYER",
    "DIRECT_AUDIT_BUFFER_MAX_RECORDS",
    "DIRECT_AUDIT_FLUSH_INTERVAL_MS",
    "DIRECT_AUDIT_MODE",
    "DIRECT_RECORD_BUNDLE_MAX_BYTES",
    "DIRECT_RECORD_HOT_CACHE_MAX_RECORDS",
    "DIRECT_RECORD_LOG_SHARD_SIZE",
    "DIRECT_WRITE_BACKOFF_MS",
    "DIRECT_WRITE_RETRIES",
    "DIRECT_WRITE_THROTTLE_MS",
    "ENABLE_CONTEXT_DEBUG_RECORDS",
    "ENABLE_CONTEXT_REPLAY",
    "ENABLE_LLM_MERGE_OPERATOR",
    "ENABLE_SUMMARY_REFRESH_AUDIT",
    "HARD_MAX_CHILDREN_SCORED_PER_PARENT",
    "MATRIXARK_ALLOW_LOCAL_BACKEND",
    "MATRIXARK_ALLOW_PYTHON_HOT_CACHE",
    "MATRIXARK_MCP_PROFILE",
    "MATRIXARK_REQUIRE_BACKEND_READY",
    "MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER",
    "MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK",
    "MAX_PRIOR_CHARS",
    "MAX_PRIOR_MESSAGES",
    "matrixark_production_profile_enabled",
    "native_candidate_prefilter_required",
    "python_hot_cache_allowed",
    "RESOURCE_ASYNC_DEFAULT_BYTES",
    "RESOURCE_ASYNC_DEFAULT_PATH_COUNT",
    "RESOURCE_ASYNC_DEFAULT_TEXT_CHARS",
    "SUMMARY_REFRESH_INTERVAL_MS",
    "SUMMARY_REFRESH_LIMIT",
    "TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE",
    "TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH",
    "TIME_COMPRESSION_MIN_EVENTS",
    "TIME_COMPRESSION_MIN_EVENT_AGE_MS",
    "TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS",
    "TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS",
    "TIME_COMPRESSION_WINDOW_EVENTS",
)
try:
    _runtime_config = importlib.import_module("tools.matrixark_mcp_runtime_config")
except ModuleNotFoundError:  # Direct script execution from tools/.
    _runtime_config = importlib.import_module("matrixark_mcp_runtime_config")
globals().update({name: getattr(_runtime_config, name) for name in _RUNTIME_CONFIG_EXPORTS})


_OSS_SEGMENT_MODEL_CACHE: dict[str, Any] = {}
_OSS_UNDERSTANDING_PROTOTYPE_CACHE: dict[str, dict[str, list[float]]] = {}
try:
    from tools.matrixark_mcp_direct_cache_state import (
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_direct_cache_state import (
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    )


try:
    from tools.matrixark_mcp_extraction_runtime import (
        ONE_PASS_MEMORY_SCHEMA,
        build_segment_prompt,
        canonical_entity_name,
        clean_patch_value,
        compact_internal_extraction,
        contiguous_ranges,
        dedupe_entities,
        detect_memory_segments,
        extract_batch_entities,
        extract_resource_facts,
        infer_entity_field_patches,
        infer_event_type,
        infer_segment_topic,
        intelligent_memory_segments,
        normalize_coordinate_tuples,
        normalize_message_indexes,
        normalize_model_segments,
        one_pass_memory_extraction,
        openai_compatible_one_pass_memory_extraction,
        openai_compatible_resource_facts,
        ordered_unique,
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_memory_segments,
        oss_encoder_rank_labels,
        oss_model_memory_segments,
        prototype_vectors,
        require_oss_understanding,
        resource_extraction_mode,
        semantic_saliency_score,
        understanding_provider,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_extraction_runtime import (
        ONE_PASS_MEMORY_SCHEMA,
        build_segment_prompt,
        canonical_entity_name,
        clean_patch_value,
        compact_internal_extraction,
        contiguous_ranges,
        dedupe_entities,
        detect_memory_segments,
        extract_batch_entities,
        extract_resource_facts,
        infer_entity_field_patches,
        infer_event_type,
        infer_segment_topic,
        intelligent_memory_segments,
        normalize_coordinate_tuples,
        normalize_message_indexes,
        normalize_model_segments,
        one_pass_memory_extraction,
        openai_compatible_one_pass_memory_extraction,
        openai_compatible_resource_facts,
        ordered_unique,
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_memory_segments,
        oss_encoder_rank_labels,
        oss_model_memory_segments,
        prototype_vectors,
        require_oss_understanding,
        resource_extraction_mode,
        semantic_saliency_score,
        understanding_provider,
    )


try:
    from tools.matrixark_mcp_budget_pack import (
        build_cross_session_policy,
        build_shared_context_policy,
        bounded_max_children_scored_per_parent,
        compact_local_context_refs,
        context_text_hashes,
        local_context_budget,
        local_context_refs_for_pack,
        select_token_budgeted_refs,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_budget_pack import (
        build_cross_session_policy,
        build_shared_context_policy,
        bounded_max_children_scored_per_parent,
        compact_local_context_refs,
        context_text_hashes,
        local_context_budget,
        local_context_refs_for_pack,
        select_token_budgeted_refs,
    )


try:
    from tools.matrixark_mcp_access_scope import (
        access_scope_matches_before_scoring,
        candidate_access_scope,
        cross_session_rerank_adjustment,
        scope_matches,
        session_continuity_boost,
        session_continuity_status,
        sharing_scope_from_candidate,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_access_scope import (
        access_scope_matches_before_scoring,
        candidate_access_scope,
        cross_session_rerank_adjustment,
        scope_matches,
        session_continuity_boost,
        session_continuity_status,
        sharing_scope_from_candidate,
    )


try:
    from tools.matrixark_mcp_recall_scoring import (
        diversify_for_question_type,
        dropped_candidate_audit_ref,
        is_resource_or_skill_candidate,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        question_type_ref_boost,
        record_dropped_candidate,
        score_recall_candidate,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_recall_scoring import (
        diversify_for_question_type,
        dropped_candidate_audit_ref,
        is_resource_or_skill_candidate,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        question_type_ref_boost,
        record_dropped_candidate,
        score_recall_candidate,
    )
