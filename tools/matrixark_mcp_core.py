#!/usr/bin/env python3
"""MatrixArk MCP server for LLM context ingestion and retrieval.

This is intentionally dependency-free. It implements the small JSON-RPC subset
needed by MCP clients over stdio, and keeps the storage boundary behind a local
adapter that can be replaced with TemporalStore RPC calls later.
"""

from __future__ import annotations

import argparse
import select
import shutil
import socket
import subprocess
import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import os
import re
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

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
    from tools.matrixark_mcp_validation import (
        optional_object,
        optional_string,
        optional_string_list,
        require_messages,
        require_string,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_validation import (
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
    import tools.matrixark_mcp_serving_records as _serving_records
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
    import matrixark_mcp_serving_records as _serving_records
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


def _mcp_debug_log(message: str) -> None:
    path = os.environ.get("MATRIXARK_MCP_DEBUG_LOG")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{time.time():.3f} {message}\n")
    except Exception:
        pass
MAX_PRIOR_MESSAGES = 8
MAX_PRIOR_CHARS = 4096
DIRECT_RECORD_LOG_SHARD_SIZE = 256
DIRECT_RECORD_BUNDLE_MAX_BYTES = int(os.environ.get("MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES", "65536"))
DIRECT_RECORD_HOT_CACHE_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS", "20000"))
DIRECT_WRITE_RETRIES = int(os.environ.get("MATRIXARK_DIRECT_WRITE_RETRIES", "3"))
DIRECT_WRITE_BACKOFF_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_BACKOFF_MS", "25"))
DIRECT_WRITE_THROTTLE_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_THROTTLE_MS", "0"))
DIRECT_AUDIT_MODE = os.environ.get("MATRIXARK_DIRECT_AUDIT_MODE", "buffered").strip().lower()
DIRECT_AUDIT_BUFFER_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS", "128"))
DIRECT_AUDIT_FLUSH_INTERVAL_MS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS", "1000"))
CONTEXT_TELEMETRY_WRITE_MODE = os.environ.get("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE", "inline").strip().lower()
ENABLE_CONTEXT_DEBUG_RECORDS = os.environ.get("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_CONTEXT_REPLAY = os.environ.get("MATRIXARK_ENABLE_REPLAY", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_SUMMARY_REFRESH_AUDIT = os.environ.get("MATRIXARK_SUMMARY_REFRESH_AUDIT", "0").strip().lower() in {"1", "true", "yes"}
SUMMARY_REFRESH_INTERVAL_MS = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "1000"))
SUMMARY_REFRESH_LIMIT = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_LIMIT", "64"))
BACKEND_READINESS_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_TIMEOUT_MS", "30000"))
BACKEND_READINESS_BACKOFF_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_BACKOFF_MS", "200"))
MATRIXARK_MCP_PROFILE = os.environ.get("MATRIXARK_MCP_PROFILE", "dev").strip().lower()
MATRIXARK_ALLOW_LOCAL_BACKEND = os.environ.get("MATRIXARK_ALLOW_LOCAL_BACKEND", "0").strip().lower() in {"1", "true", "yes"}
MATRIXARK_REQUIRE_BACKEND_READY = os.environ.get("MATRIXARK_REQUIRE_BACKEND_READY", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK", "").strip().lower()
MATRIXARK_ALLOW_PYTHON_HOT_CACHE = os.environ.get("MATRIXARK_ALLOW_PYTHON_HOT_CACHE", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", "").strip().lower()
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))

DEFAULT_MAX_CONTEXT_TOKENS = int(os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", "32000"))
CONTEXT_PACK_DEBUG_REFS = os.environ.get("MATRIXARK_CONTEXT_PACK_DEBUG_REFS", "0").strip().lower() in {"1", "true", "yes"}
AUDIT_DEBUG_PAYLOAD = os.environ.get("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "0").strip().lower() in {"1", "true", "yes"}


def _sync_serving_record_flags() -> None:
    _serving_records.ENABLE_CONTEXT_DEBUG_RECORDS = ENABLE_CONTEXT_DEBUG_RECORDS


def materialize_serving_records(record: Json) -> list[Json]:
    _sync_serving_record_flags()
    return _serving_records.materialize_serving_records(record)


def materialize_serving_record_batch(records: list[Json]) -> list[Json]:
    _sync_serving_record_flags()
    return _serving_records.materialize_serving_record_batch(records)


DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
HARD_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
RESOURCE_ASYNC_DEFAULT_BYTES = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES", str(2 * 1024 * 1024)))
RESOURCE_ASYNC_DEFAULT_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_TEXT_CHARS", "200000"))
RESOURCE_ASYNC_DEFAULT_PATH_COUNT = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_PATH_COUNT", "32"))
DEFAULT_TIME_DECAY_TOLERANCE_MS = 24 * 60 * 60 * 1000
DEFAULT_TIME_DECAY_HALFLIFE_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TIME_WEIGHT = 0.18
DEFAULT_BUSINESS_WEIGHT = 0.22
DEFAULT_RETRIEVAL_MIN_SCORE = float(os.environ.get("MATRIXARK_RETRIEVAL_MIN_SCORE", "0.20"))
DEFAULT_TOP_K_PER_LAYER = int(os.environ.get("MATRIXARK_TOP_K_PER_LAYER", "8"))
DEFAULT_MAX_CANDIDATES_PER_NODE = int(os.environ.get("MATRIXARK_MAX_CANDIDATES_PER_NODE", "1024"))
DEFAULT_MAX_GLOBAL_CANDIDATES = int(os.environ.get("MATRIXARK_MAX_GLOBAL_CANDIDATES", "512"))
DEFAULT_MAX_SELECTED_REFS = int(os.environ.get("MATRIXARK_MAX_SELECTED_REFS", "24"))
DEFAULT_BUDGET_FILL_POLICY = os.environ.get("MATRIXARK_BUDGET_FILL_POLICY", "quality_first").strip().lower()
DEFAULT_CROSS_SESSION_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_BUDGET_RATIO", "0.12"))
DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_BROAD_BUDGET_RATIO", "0.15"))
DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS", "1536"))
DEFAULT_CROSS_SESSION_MAX_SESSIONS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_SESSIONS", "3"))
DEFAULT_CROSS_SESSION_MAX_CANDIDATES = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_CANDIDATES", "24"))
DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS", "2"))
DEFAULT_CROSS_SESSION_PARALLELISM = int(os.environ.get("MATRIXARK_CROSS_SESSION_PARALLELISM", "4"))
DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_BUDGET_TOKENS", "256"))
DEFAULT_CROSS_SESSION_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_SCORE", "0.20"))
DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE", "0.45"))
DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES = tuple(
    item.strip()
    for item in os.environ.get("MATRIXARK_CROSS_SESSION_PREFERRED_REF_TYPES", "entity,summary,compression").split(",")
    if item.strip()
)
DEFAULT_SHARED_RESOURCE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO", "0.25"))
DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS", "4096"))
DEFAULT_SHARED_SKILL_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", "0.10"))
DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", "2048"))
DEFAULT_SHARED_CONTEXT_MIN_SCORE = float(os.environ.get("MATRIXARK_SHARED_CONTEXT_MIN_SCORE", "0.20"))
TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE", "256"))
TIME_COMPRESSION_WINDOW_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS", "64"))
TIME_COMPRESSION_MIN_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENTS", "8"))
TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH", "4"))
TIME_COMPRESSION_MIN_EVENT_AGE_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENT_AGE_MS", "0"))
TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS", str(30 * 24 * 60 * 60 * 1000)))
ENABLE_LLM_MERGE_OPERATOR = os.environ.get("MATRIXARK_ENABLE_LLM_MERGE_OPERATOR", "").strip().lower() in {"1", "true", "yes"}
DEFAULT_ENTITY_MERGE_OPERATOR = os.environ.get("MATRIXARK_ENTITY_MERGE_OPERATOR", "EUA_MERGE").strip().upper() or "EUA_MERGE"
_OSS_SEGMENT_MODEL_CACHE: dict[str, Any] = {}
_OSS_UNDERSTANDING_PROTOTYPE_CACHE: dict[str, dict[str, list[float]]] = {}
_DIRECT_RECORD_CACHE: dict[str, tuple[int, list[Json]]] = {}
_DIRECT_RECORD_CACHE_LOCK = threading.RLock()
_DIRECT_RECORD_CACHE_MAX_PREFIXES = 64
_DIRECT_RECORD_LOAD_LOCKS: dict[str, threading.RLock] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE: dict[str, Json] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK = threading.RLock()
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES = int(os.environ.get("MATRIXARK_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES", "256"))
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE: dict[str, list[tuple[str, int, int, Json]]] = {}
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK = threading.RLock()
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES = int(os.environ.get("MATRIXARK_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES", "1024"))


def matrixark_production_profile_enabled() -> bool:
    return MATRIXARK_MCP_PROFILE in {"prod", "production", "benchmark", "bench", "parity"}


def python_hot_cache_allowed(*, backend_label: str = "") -> bool:
    if MATRIXARK_ALLOW_PYTHON_HOT_CACHE:
        return MATRIXARK_ALLOW_PYTHON_HOT_CACHE in {"1", "true", "yes"}
    return backend_label == "local"


def native_candidate_prefilter_required(*, backend_label: str = "") -> bool:
    if MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER:
        return MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER in {"1", "true", "yes"}
    return backend_label != "local"

DEFAULT_BUSINESS_TYPE_WEIGHTS: Json = {
    "confirmation": 1.0,
    "correction": 1.0,
    "approval_budget": 0.95,
    "approval": 0.95,
    "approval_state": 0.95,
    "budget": 0.9,
    "preference_update": 0.82,
    "plan_update": 0.78,
    "status_update": 0.76,
    "job_status": 0.76,
    "relationship": 0.74,
    "location": 0.7,
    "current_plan": 0.78,
    "family_profile": 0.72,
    "skill": 0.84,
    "resource": 0.68,
    "dialogue_batch": 0.45,
    "session": 0.45,
}


def parse_host_port(address: str) -> tuple[str, int] | None:
    if not address or ":" not in address:
        return None
    host, port_text = address.rsplit(":", 1)
    try:
        return host or "127.0.0.1", int(port_text)
    except ValueError:
        return None


def metaserver_reachable(address: str, timeout_ms: int = BACKEND_READINESS_CONNECT_TIMEOUT_MS) -> Json:
    parsed = parse_host_port(address)
    if parsed is None:
        return {"ok": False, "address": address, "error": "invalid metaserver address"}
    host, port = parsed
    try:
        with socket.create_connection((host, port), timeout=max(0.05, timeout_ms / 1000.0)):
            return {"ok": True, "address": address}
    except OSError as exc:
        return {"ok": False, "address": address, "error": str(exc)}


def context_model_registry_record(model_name: str, *, model_kind: str = "embedding", updated_at_ms: int | None = None) -> Json:
    model_name = str(model_name or "").strip()
    model_hash = stable_hash(f"{model_kind}_model:{model_name}")
    return {
        "record_type": "context_model_registry",
        "model_kind": model_kind,
        "model_ref": embedding_model_ref_for_name(model_name) if model_kind == "embedding" else f"{model_kind}:{compact_model_slug(model_name)}:{model_hash % 10000:04d}",
        "model_name": model_name,
        "model_hash": model_hash,
        "provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic") if model_kind == "embedding" else "",
        "execution_mode": embedding_execution_mode_name() if model_kind == "embedding" else "",
        "updated_at_ms": int(updated_at_ms or now_ms()),
    }


def context_model_registry_records(records: list[Json]) -> list[Json]:
    models: dict[str, int] = {}
    for record in records:
        if str(record.get("record_type") or "") != "context_embedding":
            continue
        model_name = str(record.get("model") or "").strip()
        if not model_name:
            continue
        updated_at_ms = record.get("updated_at_ms") or record.get("created_at_ms") or now_ms()
        try:
            timestamp = int(updated_at_ms)
        except (TypeError, ValueError):
            timestamp = now_ms()
        models[model_name] = max(models.get(model_name, 0), timestamp)
    return [context_model_registry_record(model_name, updated_at_ms=timestamp) for model_name, timestamp in sorted(models.items())]


def enrich_scope_with_identity(scope: Json, identity: Json) -> Json:
    account_id = str(identity["account_id"])
    tenant_id = str(identity["tenant_id"])
    user_id = str(scope.get("user_id") or identity.get("user_id") or "")
    session_id = str(scope.get("session_id") or identity.get("session_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id)
    explicit_scope_keys = {str(key) for key in scope.keys()}
    if identity.get("mode") == "api_key":
        if user_id:
            explicit_scope_keys.add("user_id")
        if session_id:
            explicit_scope_keys.add("session_id")
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if identity.get("agent_name") and "agent_name" not in enriched:
        enriched["agent_name"] = identity["agent_name"]
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    return enriched


def validate_hook(hook: Json | None) -> Json | None:
    if hook is None:
        return None
    if not isinstance(hook, dict):
        raise MatrixArkError("agent_hook must be an object")
    hook_type = require_string(hook, "hook_type")
    if hook_type not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise MatrixArkError("agent_hook.hook_type is invalid")
    require_string(hook, "source")
    require_string(hook, "hook_id")
    if not isinstance(hook.get("observed_at_ms"), int):
        raise MatrixArkError("agent_hook.observed_at_ms must be an integer")
    if not isinstance(hook.get("auto_captured"), bool):
        raise MatrixArkError("agent_hook.auto_captured must be a boolean")
    if "idempotency_key" in hook and not isinstance(hook["idempotency_key"], str):
        raise MatrixArkError("agent_hook.idempotency_key must be a string")
    return hook


def adapter_ensure_backend_ready(adapter: Any, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
    """Call adapter readiness across old/new adapter signatures."""
    try:
        return adapter.ensure_backend_ready(reason=reason, probe=probe, timeout_ms=timeout_ms)
    except TypeError as exc:
        text = str(exc)
        if "unexpected keyword argument" not in text or "probe" not in text:
            raise
        return adapter.ensure_backend_ready(reason=reason)

def has_confirmation_context(envelope: Json) -> bool:
    metadata = optional_object(envelope, "metadata")
    return bool(
        envelope.get("context_pack_id")
        or metadata.get("reply_to_context_pack_id")
        or envelope.get("accepted_refs")
        or envelope.get("rejected_refs")
    )


def explicit_context_pack_id(envelope: Json) -> str:
    metadata = optional_object(envelope, "metadata")
    value = envelope.get("context_pack_id") or metadata.get("reply_to_context_pack_id") or ""
    return str(value) if value else ""


def session_key(envelope: Json) -> tuple[Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
    )


def user_key(envelope: Json) -> tuple[Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("team", ""),
    )


def context_node_key(envelope: Json) -> tuple[Any, Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
        scope.get("project", ""),
    )


def session_buffer_key_from_scope(scope: Json) -> tuple[str, str, str, str]:
    user_id = str(scope.get("user_id") or "")
    session_id = str(scope.get("session_id") or user_id or "")
    return (
        str(scope.get("account_id") or "acct_local"),
        str(scope.get("tenant_id") or "tenant_local_agent"),
        user_id,
        session_id,
    )


def session_buffer_key(envelope: Json) -> tuple[str, str, str, str]:
    return session_buffer_key_from_scope(envelope.get("scope", {}))


def message_from_event_record(record: Json) -> Json | None:
    try:
        from tools.matrixark_mcp_prior_context import message_from_event_record as _message_from_event_record
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_prior_context import message_from_event_record as _message_from_event_record

    return _message_from_event_record(record)


def session_summary_for_events(level: str, event_records: list[Json], all_records: list[Json]) -> Json | None:
    try:
        from tools.matrixark_mcp_prior_context import session_summary_for_events as _session_summary_for_events
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_prior_context import session_summary_for_events as _session_summary_for_events

    return _session_summary_for_events(level, event_records, all_records)


def collect_prior_context(envelope: Json, records: list[Json]) -> Json:
    try:
        from tools.matrixark_mcp_prior_context import collect_prior_context as _collect_prior_context
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_prior_context import collect_prior_context as _collect_prior_context

    return _collect_prior_context(envelope, records)


def prior_context_payload(level: str, event_records: list[Json], all_records: list[Json]) -> Json:
    try:
        from tools.matrixark_mcp_prior_context import prior_context_payload as _prior_context_payload
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_prior_context import prior_context_payload as _prior_context_payload

    return _prior_context_payload(level, event_records, all_records)


def normalize_entity_operator(raw_operator: Any, entity_type: str) -> str:
    try:
        from tools.matrixark_mcp_extraction_normalization import normalize_entity_operator as _normalize_entity_operator
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_extraction_normalization import normalize_entity_operator as _normalize_entity_operator

    return _normalize_entity_operator(raw_operator, entity_type)

def normalize_extracted_entities(raw_entities: Any, *, fallback_text: str, source_refs: list[str], extracted_by: str) -> list[Json]:
    try:
        from tools.matrixark_mcp_extraction_normalization import normalize_extracted_entities as _normalize_extracted_entities
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_extraction_normalization import normalize_extracted_entities as _normalize_extracted_entities

    return _normalize_extracted_entities(raw_entities, fallback_text=fallback_text, source_refs=source_refs, extracted_by=extracted_by)


def normalize_extracted_segments(raw_segments: Any, messages: list[Json]) -> list[Json]:
    try:
        from tools.matrixark_mcp_extraction_normalization import normalize_extracted_segments as _normalize_extracted_segments
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_extraction_normalization import normalize_extracted_segments as _normalize_extracted_segments

    return _normalize_extracted_segments(raw_segments, messages)


def normalize_extracted_facts(raw_facts: Any, *, chunk: Any, chunk_metadata: Json, raw_uri: str, resource_version: str, provider: str) -> list[Json]:
    try:
        from tools.matrixark_mcp_extraction_normalization import normalize_extracted_facts as _normalize_extracted_facts
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_extraction_normalization import normalize_extracted_facts as _normalize_extracted_facts

    return _normalize_extracted_facts(raw_facts, chunk=chunk, chunk_metadata=chunk_metadata, raw_uri=raw_uri, resource_version=resource_version, provider=provider)


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


def normalize_envelope(args: Json, *, default_kind: str) -> Json:
    messages = require_messages(args)
    scope = optional_object(args, "scope")
    metadata = optional_object(args, "metadata")
    kind = args.get("kind", default_kind)
    if kind not in {"message", "feedback", "resource", "skill", "business_data"}:
        raise MatrixArkError("kind is invalid")
    storage_options = normalize_storage_options(args, metadata)
    if not storage_options:
        storage_options = default_async_ingest_storage_options()
        storage_options["request_level"] = True
    record_storage_options = normalize_record_storage_options(args, metadata, storage_options)
    envelope: Json = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
        "ingestion_time_ms": now_ms(),
        "storage_options": storage_options,
    }
    if record_storage_options:
        envelope["record_storage_options"] = record_storage_options
        envelope["part_storage_options"] = record_storage_options
    envelope["storage_route"] = canonical_storage_route(envelope.get("storage_options", {}))
    for field in [
        "context_pack_id",
        "query_id_hash",
        "accepted_refs",
        "rejected_refs",
        "raw_uri",
        "resource_type",
        "source_event_ids",
        "segment_provider",
        "segment_model",
        "segment_model_path",
        "segment_max_new_tokens",
        "segment_provider_fallback",
        "understanding_provider",
        "extraction_provider",
    ]:
        if field in args:
            envelope[field] = args[field]
    return envelope



def final_recall_score(origin_score: float, time_score: float, business_score: float, weights: Json) -> float:
    time_weight = clamp01(weights.get("time", DEFAULT_TIME_WEIGHT), DEFAULT_TIME_WEIGHT)
    business_weight = clamp01(weights.get("business", DEFAULT_BUSINESS_WEIGHT), DEFAULT_BUSINESS_WEIGHT)
    if time_weight + business_weight > 1.0:
        scale = 1.0 / (time_weight + business_weight)
        time_weight *= scale
        business_weight *= scale
    origin_weight = 1.0 - time_weight - business_weight
    return round(
        origin_weight * origin_score + time_weight * time_score + business_weight * business_score,
        6,
    )


def integer_arg(data: Json, field: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(field, default)
    if not isinstance(value, int):
        raise MatrixArkError(f"{field} must be an integer")
    if value < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    return value


def float_arg(data: Json, field: str, default: float, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    value = data.get(field, default)
    if not isinstance(value, (int, float)):
        raise MatrixArkError(f"{field} must be a number")
    result = float(value)
    if result < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise MatrixArkError(f"{field} must be <= {maximum}")
    return result


def build_cross_session_policy(args: Json, ranking: Json, *, question_type: str, session_scope: str, remote_budget_tokens: int) -> Json:
    try:
        from tools.matrixark_mcp_budget_pack import build_cross_session_policy as _build_cross_session_policy
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import build_cross_session_policy as _build_cross_session_policy

    return _build_cross_session_policy(
        args,
        ranking,
        question_type=question_type,
        session_scope=session_scope,
        remote_budget_tokens=remote_budget_tokens,
    )


def build_shared_context_policy(args: Json, ranking: Json, *, remote_budget_tokens: int) -> Json:
    try:
        from tools.matrixark_mcp_budget_pack import build_shared_context_policy as _build_shared_context_policy
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import build_shared_context_policy as _build_shared_context_policy

    return _build_shared_context_policy(args, ranking, remote_budget_tokens=remote_budget_tokens)


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



def is_shared_resource_candidate(candidate: Json) -> bool:
    try:
        from tools.matrixark_mcp_recall_scoring import is_shared_resource_candidate as _is_shared_resource_candidate
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import is_shared_resource_candidate as _is_shared_resource_candidate

    return _is_shared_resource_candidate(candidate)


def is_shared_skill_candidate(candidate: Json) -> bool:
    try:
        from tools.matrixark_mcp_recall_scoring import is_shared_skill_candidate as _is_shared_skill_candidate
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import is_shared_skill_candidate as _is_shared_skill_candidate

    return _is_shared_skill_candidate(candidate)


def bounded_max_children_scored_per_parent(value: int) -> int:
    hard_cap = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
    if value > hard_cap:
        raise MatrixArkError(
            "max_children_scored_per_parent must be <= "
            f"{hard_cap}; split over-wide ContextNode children into deeper node layers"
        )
    return value


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    try:
        from tools.matrixark_mcp_recall_scoring import score_recall_candidate as _score_recall_candidate
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import score_recall_candidate as _score_recall_candidate

    return _score_recall_candidate(candidate, ranking, reference_time_ms=reference_time_ms)


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    try:
        from tools.matrixark_mcp_recall_scoring import merge_ranked_paths as _merge_ranked_paths
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import merge_ranked_paths as _merge_ranked_paths

    return _merge_ranked_paths(primary, auxiliary, total_limit=total_limit, auxiliary_quota=auxiliary_quota)


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    try:
        from tools.matrixark_mcp_recall_scoring import question_type_ref_boost as _question_type_ref_boost
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import question_type_ref_boost as _question_type_ref_boost

    return _question_type_ref_boost(candidate, question_type)


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, float, float]:
    try:
        from tools.matrixark_mcp_recall_scoring import packing_sort_key as _packing_sort_key
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import packing_sort_key as _packing_sort_key

    return _packing_sort_key(candidate, question_type)


def is_resource_or_skill_candidate(candidate: Json) -> bool:
    try:
        from tools.matrixark_mcp_recall_scoring import is_resource_or_skill_candidate as _is_resource_or_skill_candidate
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import is_resource_or_skill_candidate as _is_resource_or_skill_candidate

    return _is_resource_or_skill_candidate(candidate)


def dropped_candidate_audit_ref(candidate: Json, *, reason: str, token_estimate: int) -> Json:
    try:
        from tools.matrixark_mcp_recall_scoring import dropped_candidate_audit_ref as _dropped_candidate_audit_ref
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import dropped_candidate_audit_ref as _dropped_candidate_audit_ref

    return _dropped_candidate_audit_ref(candidate, reason=reason, token_estimate=token_estimate)


def record_dropped_candidate(dropped: Json, candidate: Json, *, reason: str, token_estimate: int) -> None:
    try:
        from tools.matrixark_mcp_recall_scoring import record_dropped_candidate as _record_dropped_candidate
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import record_dropped_candidate as _record_dropped_candidate

    _record_dropped_candidate(dropped, candidate, reason=reason, token_estimate=token_estimate)


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    try:
        from tools.matrixark_mcp_recall_scoring import diversify_for_question_type as _diversify_for_question_type
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_recall_scoring import diversify_for_question_type as _diversify_for_question_type

    return _diversify_for_question_type(candidates, question_type, total_limit=total_limit)

def context_text_hashes(text: str) -> set[int]:
    try:
        from tools.matrixark_mcp_budget_pack import context_text_hashes as _context_text_hashes
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import context_text_hashes as _context_text_hashes

    return _context_text_hashes(text)


def local_context_budget(args: Json) -> Json:
    try:
        from tools.matrixark_mcp_budget_pack import local_context_budget as _local_context_budget
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import local_context_budget as _local_context_budget

    return _local_context_budget(args)


def compact_local_context_refs(local_budget: Json) -> list[Json]:
    try:
        from tools.matrixark_mcp_budget_pack import compact_local_context_refs as _compact_local_context_refs
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import compact_local_context_refs as _compact_local_context_refs

    return _compact_local_context_refs(local_budget)


def local_context_refs_for_pack(local_budget: Json) -> list[Json]:
    try:
        from tools.matrixark_mcp_budget_pack import local_context_refs_for_pack as _local_context_refs_for_pack
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import local_context_refs_for_pack as _local_context_refs_for_pack

    return _local_context_refs_for_pack(local_budget)



def select_token_budgeted_refs(
    primary: list[Json],
    auxiliary: list[Json],
    *,
    max_context_tokens: int,
    auxiliary_quota: int,
    question_type: str = "fact",
    reserved_tokens: int = 0,
    max_selected_refs: int | None = None,
    min_score: float = DEFAULT_RETRIEVAL_MIN_SCORE,
    max_global_candidates: int | None = None,
    budget_fill_policy: str = DEFAULT_BUDGET_FILL_POLICY,
    duplicate_text_hashes: set[int] | None = None,
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
    cross_session_policy: Json | None = None,
    shared_context_policy: Json | None = None,
) -> tuple[list[Json], int, Json]:
    try:
        from tools.matrixark_mcp_budget_pack import select_token_budgeted_refs as _select_token_budgeted_refs
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_budget_pack import select_token_budgeted_refs as _select_token_budgeted_refs

    return _select_token_budgeted_refs(
        primary,
        auxiliary,
        max_context_tokens=max_context_tokens,
        auxiliary_quota=auxiliary_quota,
        question_type=question_type,
        reserved_tokens=reserved_tokens,
        max_selected_refs=max_selected_refs,
        min_score=min_score,
        max_global_candidates=max_global_candidates,
        budget_fill_policy=budget_fill_policy,
        duplicate_text_hashes=duplicate_text_hashes,
        deadline_exceeded=deadline_exceeded,
        deadline_reason=deadline_reason,
        cross_session_policy=cross_session_policy,
        shared_context_policy=shared_context_policy,
    )
