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
SUMMARY_LLM_PROVIDER = os.environ.get(
    "MATRIXARK_SUMMARY_PROVIDER",
    os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", os.environ.get("MATRIXARK_EXTRACTION_PROVIDER", "deterministic")),
).strip().lower().replace("-", "_")
SUMMARY_LLM_MODEL = os.environ.get("MATRIXARK_SUMMARY_MODEL", EXTRACTION_LLM_MODEL)
SUMMARY_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_SUMMARY_MAX_TOKENS", "900"))
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
    messages = record.get("envelope", {}).get("messages", [])
    if isinstance(messages, list) and messages:
        message = messages[0]
        if isinstance(message, dict) and "role" in message and "content" in message:
            return dict(message)
    text = str(record.get("text", ""))
    if ":" in text:
        role, content = text.split(":", 1)
        role = role.strip() or "user"
        return {"role": role if role in {"user", "assistant", "tool", "system"} else "user", "content": content.strip()}
    return None


def session_summary_for_events(level: str, event_records: list[Json], all_records: list[Json]) -> Json | None:
    if level != "session" or not event_records:
        return None
    target_key = context_node_key(event_records[0].get("envelope", {}))
    event_hashes = {record.get("event_id_hash") for record in event_records if record.get("event_id_hash") is not None}
    fallback_summary = None
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary" or record.get("summary_type") != "session_l0":
            continue
        if target_key and tuple(record.get("context_node_key", [])) == target_key:
            return record
        if record.get("source_event_hash") in event_hashes and fallback_summary is None:
            fallback_summary = record
    return fallback_summary


def collect_prior_context(envelope: Json, records: list[Json]) -> Json:
    pack_id = explicit_context_pack_id(envelope)
    if pack_id:
        for record in reversed(records):
            if record.get("record_type") == "context_pack_audit" and str(record.get("context_pack_id")) == pack_id:
                summary_text = str(record.get("summary_text", ""))
                refs = record.get("selected_refs", [])
                return {
                    "level": "explicit",
                    "refs": refs,
                    "messages": [],
                    "summaries": [
                        {
                            "ref_type": "context_pack",
                            "ref_hash": pack_id,
                            "text": summary_text[:MAX_PRIOR_CHARS],
                        }
                    ]
                    if summary_text
                    else [],
                    "char_count": min(len(summary_text), MAX_PRIOR_CHARS),
                    "limit": MAX_PRIOR_MESSAGES,
                }

    if has_confirmation_context(envelope):
        return {
            "level": "explicit",
            "refs": envelope.get("accepted_refs") or envelope.get("rejected_refs") or [],
            "messages": [],
            "summaries": [],
            "char_count": 0,
            "limit": MAX_PRIOR_MESSAGES,
        }

    selected_events = []
    key = session_key(envelope)
    if key[1]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if session_key(prior_envelope) == key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("session", selected_events, records)

    selected_events = []
    fallback_key = user_key(envelope)
    if fallback_key[0]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if user_key(prior_envelope) == fallback_key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("user", selected_events, records)

    return {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}


def prior_context_payload(level: str, event_records: list[Json], all_records: list[Json]) -> Json:
    messages = []
    refs = []
    summaries = []
    char_count = 0
    session_summary = session_summary_for_events(level, event_records, all_records)
    if session_summary:
        text = str(session_summary.get("summary_text", ""))[:MAX_PRIOR_CHARS]
        char_count += len(text)
        summaries.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
                "text": text,
            }
        )
        refs.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
            }
        )

    event_hashes = {record.get("event_id_hash") for record in event_records}
    seen_summaries = set()
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary":
            continue
        if record.get("source_event_hash") not in event_hashes:
            continue
        summary_hash = record.get("summary_hash") or record.get("node_hash")
        node_hash = record.get("node_hash")
        if summary_hash in seen_summaries:
            continue
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        text = str(record.get("summary_text", ""))[:remaining]
        char_count += len(text)
        seen_summaries.add(summary_hash)
        summaries.append(
            {
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "text": text,
            }
        )
        refs.append({"ref_type": "summary", "ref_hash": summary_hash, "node_hash": node_hash})

    for record in event_records:
        text = str(record.get("text", ""))
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        clipped = text[:remaining]
        char_count += len(clipped)
        messages.append(
            {
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
                "text": clipped,
            }
        )
        refs.append(
            {
                "ref_type": "event",
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
            }
        )
    return {
        "level": level,
        "refs": refs,
        "messages": messages,
        "summaries": summaries,
        "char_count": char_count,
        "limit": MAX_PRIOR_MESSAGES,
    }


def normalize_entity_operator(raw_operator: Any, entity_type: str) -> str:
    """Return the operator actually used by runtime entity maintenance.

    LLM_MERGE is a future/optional production operator because it implies a
    separate LLM merge pass. The current online runtime applies field patches
    deterministically, so serving records should say EUA_MERGE unless the LLM
    merge feature is explicitly enabled.
    """
    if entity_type in {"confirmation", "correction"}:
        return "LATEST"
    operator = str(raw_operator or DEFAULT_ENTITY_MERGE_OPERATOR).strip().upper() or DEFAULT_ENTITY_MERGE_OPERATOR
    if operator == "LLM_MERGE" and not ENABLE_LLM_MERGE_OPERATOR:
        return DEFAULT_ENTITY_MERGE_OPERATOR
    return operator

def normalize_extracted_entities(raw_entities: Any, *, fallback_text: str, source_refs: list[str], extracted_by: str) -> list[Json]:
    if not isinstance(raw_entities, list):
        return []
    entities: list[Json] = []
    for raw in raw_entities[:12]:
        if not isinstance(raw, dict):
            continue
        entity_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("entity_type") or raw.get("type") or "entity").lower()).strip("_") or "entity"
        entity_name = summarize_text(str(raw.get("entity_name") or raw.get("name") or entity_type).strip(), limit=96)
        state = summarize_text(str(raw.get("state") or raw.get("value") or raw.get("summary") or fallback_text).strip(), limit=320)
        if not state:
            continue
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.82))))
        except (TypeError, ValueError):
            confidence = 0.82
        operator = normalize_entity_operator(raw.get("operator"), entity_type)
        patches = raw.get("field_patches") if isinstance(raw.get("field_patches"), list) else []
        if not patches and entity_type not in {"confirmation", "correction"}:
            patches = [entity_patch("", state)]
        refs = raw.get("source_refs") if isinstance(raw.get("source_refs"), list) else source_refs
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": entity_name or entity_type,
                "state": state,
                "confidence": round(confidence, 6),
                "source_refs": [str(ref) for ref in refs] if refs else source_refs,
                "operator": operator,
                "field_patches": patches[:3],
                "extracted_by": extracted_by,
            }
        )
    return dedupe_entities(entities)


def normalize_extracted_segments(raw_segments: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw_segments, list):
        try:
            return normalize_model_segments({"segments": raw_segments}, messages)
        except MatrixArkError:
            return []
    return []


def normalize_extracted_facts(raw_facts: Any, *, chunk: Any, chunk_metadata: Json, raw_uri: str, resource_version: str, provider: str) -> list[Json]:
    if not isinstance(raw_facts, list):
        return []
    facts: list[Json] = []
    for raw in raw_facts[:12]:
        if not isinstance(raw, dict):
            continue
        event_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("event_type") or raw.get("fact_type") or "resource_fact").lower()).strip("_") or "resource_fact"
        if not event_type.startswith("resource_"):
            event_type = f"resource_{event_type}"
        entity_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("entity_type") or event_type).lower()).strip("_") or event_type
        if not entity_type.startswith("resource_"):
            entity_type = f"resource_{entity_type}"
        value = summarize_text(str(raw.get("value") or raw.get("summary_text") or raw.get("state") or "").strip(), limit=260)
        if not value:
            continue
        entity_name = summarize_text(str(raw.get("entity_name") or raw.get("name") or "").strip(), limit=140)
        if not entity_name:
            entity_name = resource_fact_entity_name({"entity_type": entity_type, "entity_prefix": entity_type.removeprefix("resource_")}, value, chunk_metadata, raw_uri)
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.86))))
        except (TypeError, ValueError):
            confidence = 0.86
        facts.append(
            {
                "mode": "matrixark_resource_schema_openai_compatible",
                "classification": "RESOURCE_FACT",
                "event_type": event_type,
                "entity_type": entity_type,
                "status": str(raw.get("status") or "observed"),
                "value": value,
                "entity_name": entity_name,
                "confidence": round(confidence, 6),
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": provider,
            }
        )
    return facts


def openai_compatible_one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    messages = envelope["messages"]
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    system = "Return only JSON. You are a one-pass memory extractor for MatrixArk."
    user = (
        "Extract memory from this logical conversation batch in one pass. "
        "Return JSON with keys classification, event_type, batch_summary, entities, segments, indexes. "
        "Entities must use a stable concise entity_name and a separate state sentence; do not copy the same phrase into both. "
        "Each entity shape: {entity_type, entity_name, state, confidence, field_patches}; operator is optional and MatrixArk will coerce it to the actual runtime operator. "
        "Segments shape: {topic, coordinate_tuples, message_indexes, saliency_score, summary_text}. "
        "Use event_type values like approval_state, budget_update, deadline, procedure, correction, status_update.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )
    raw = openai_compatible_json_call(system=system, user=user)
    batch_text = text_from_messages(messages)
    entities = normalize_extracted_entities(raw.get("entities"), fallback_text=batch_text, source_refs=source_refs, extracted_by="openai_compatible")
    if not entities:
        if require_oss_understanding():
            raise MatrixArkError("OpenAI-compatible extraction returned no entities")
        entities = extract_batch_entities(messages, envelope)
        for entity in entities:
            entity["extracted_by"] = "deterministic_fallback"
    segments = normalize_extracted_segments(raw.get("segments"), messages)
    if not segments:
        if require_oss_understanding():
            raise MatrixArkError("OpenAI-compatible extraction returned no segments")
        segments, _segment_meta = detect_memory_segments(messages, {**envelope, "segment_provider": "deterministic"})
    event_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("event_type") or infer_event_type(batch_text)).lower()).strip("_") or "session"
    classification = re.sub(r"[^A-Z0-9_]+", "_", str(raw.get("classification") or "BATCH_MEMORY").upper()).strip("_") or "BATCH_MEMORY"
    indexes = raw.get("indexes") if isinstance(raw.get("indexes"), list) else []
    normalized_indexes = [str(item) for item in indexes if isinstance(item, str)]
    normalized_indexes = ordered_unique(normalized_indexes + [context_index_name("event_type", event_type), context_index_name("classification", classification)])
    return {
        "mode": "matrixark_one_pass_schema_openai_compatible",
        "understanding_provider": "openai_compatible",
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": str(raw.get("status") or "observed"),
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": {"provider": "openai_compatible", "execution_mode": "llm_json", "model": EXTRACTION_LLM_MODEL, "fallback_used": False, "segment_count": len(segments)},
        "indexes": normalized_indexes[:8],
        "batch_summary": summarize_text(str(raw.get("batch_summary") or raw.get("summary") or batch_text), limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(tokens(batch_text)),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }


def openai_compatible_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    system = "Return only JSON. You extract cited resource facts for MatrixArk."
    user = (
        "Extract decisions, owners, costs, deadlines, API contracts, troubleshooting steps, policies, approvals, control_states, and procedures from the resource chunk. "
        "Return JSON with {facts:[...]}. Each fact shape: {event_type, entity_type, entity_name, value, confidence}. "
        "event_type and entity_type should use resource_* names. entity_name must be a stable subject, while value is the factual state. "
        "Do not invent facts; use an empty facts list if nothing is useful.\n\n"
        f"Source ref: {chunk.source_ref}\nMetadata: {json.dumps(chunk_metadata, sort_keys=True)[:1200]}\nChunk text:\n{chunk.text[:6000]}\n\nJSON:"
    )
    raw = openai_compatible_json_call(system=system, user=user)
    return normalize_extracted_facts(raw.get("facts"), chunk=chunk, chunk_metadata=chunk_metadata, raw_uri=raw_uri, resource_version=resource_version, provider="openai_compatible")


def compact_internal_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Rules-first internal extraction used by the local MCP MVP.

    Production MatrixArk can replace this with OSS/OpenAI/provider extraction,
    but callers still see the same Mem0-style envelope contract.
    """

    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return oss_encoder_compact_extraction(envelope, prior_context=prior_context)

    text = text_from_messages(envelope["messages"]).lower()
    if envelope["kind"] == "feedback":
        positive = any(term in text for term in ["yes", "confirmed", "approved", "correct", "looks good"])
        negative = any(term in text for term in ["no", "wrong", "incorrect", "reject", "not correct"])
        prior_level = prior_context.get("level", "")
        if not prior_level:
            return {
                "mode": "matrixark_internal",
                "classification": "AMBIGUOUS",
                "quality_warning": "short feedback lacks prior context",
                "prior_refs": [],
            }
        warning = ""
        if prior_level == "user":
            warning = "session_id missing; used user_id fallback for prior context"
        prior_refs = prior_context.get("refs", [])
        if positive:
            return {
                "mode": "matrixark_internal",
                "classification": "CONFIRMATION",
                "status": "accepted",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        if negative:
            return {
                "mode": "matrixark_internal",
                "classification": "CORRECTION",
                "status": "rejected",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        return {
            "mode": "matrixark_internal",
            "classification": "FEEDBACK",
            "status": "observed",
            "prior_context": prior_level,
            "prior_refs": prior_refs,
            "prior_message_count": len(prior_context.get("messages", [])),
            "prior_summary_count": len(prior_context.get("summaries", [])),
            "quality_warning": warning,
        }
    return {
        "mode": "matrixark_internal",
        "classification": "NEW_EVENT",
        "status": "observed",
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "",
    }


ONE_PASS_MEMORY_SCHEMA: Json = {
    "version": "matrixark-one-pass-memory-v1",
    "input": "logical session batch",
    "outputs": [
        "ContextEvent",
        "ContextEntity",
        "ContextSummary",
        "ContextIndex",
        "stale_blocker",
        "EntityPatch",
        "MemorySegment",
        "extraction_audit",
    ],
    "entity_types": [
        "preference",
        "relationship",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    ],
    "segmentation": {
        "phase_1": "semantic_saliency_filtering",
        "phase_2": "event_centric_partitioning",
        "output": "topic plus coordinate tuples over message indexes",
    },
}


def one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Extract events, entities, summaries, and indexes from one batch pass.

    This mirrors the VikingMem one-pass idea: compile the desired memory outputs
    into one schema and process the input session once. The local MVP uses
    deterministic rules, while a production provider can replace this function
    with one GPT-4o-mini/OSS call that emits the same JSON shape.
    """

    provider = understanding_provider(envelope)
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            return openai_compatible_one_pass_memory_extraction(envelope, prior_context=prior_context)
        except MatrixArkError:
            if require_oss_understanding():
                raise
    messages = envelope["messages"]
    batch_text = text_from_messages(messages)
    batch_terms = tokens(batch_text)
    segments, segment_provider_meta = detect_memory_segments(messages, envelope)
    if provider == "oss_encoder":
        entities = oss_encoder_extract_batch_entities(messages, envelope)
        event_type = oss_encoder_event_type(batch_text)
    else:
        entities = extract_batch_entities(messages, envelope)
        event_type = infer_event_type(batch_text)
    classification = "BATCH_MEMORY"
    if any(entity["entity_type"] == "confirmation" for entity in entities):
        classification = "CONFIRMATION"
    elif any(entity["entity_type"] == "correction" for entity in entities):
        classification = "CORRECTION"
    indexes = ordered_unique(
        [
            context_index_name("event_type", event_type),
            context_index_name("classification", classification),
            context_index_name("status", "observed"),
            context_index_name("source_type", envelope.get("kind", "message")),
        ]
        + [context_index_name("entity_type", entity["entity_type"]) for entity in entities]
        + [context_index_name("segment_topic", segment["topic"]) for segment in segments]
    )
    return {
        "mode": "matrixark_one_pass_schema_oss_encoder" if provider == "oss_encoder" else "matrixark_one_pass_schema",
        "understanding_provider": provider,
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": "observed",
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": segment_provider_meta,
        "indexes": indexes[:8],
        "batch_summary": summarize_text(batch_text, limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(batch_terms),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }



def detect_memory_segments(messages: list[Json], envelope: Json | None = None) -> tuple[list[Json], Json]:
    envelope = envelope or {}
    provider = str(envelope.get("segment_provider") or os.getenv("MATRIXARK_SEGMENT_PROVIDER", "deterministic")).strip().lower()
    if provider in {"oss_encoder", "oss-encoder", "embedding"}:
        segments = oss_encoder_memory_segments(messages)
        return segments, {
            "provider": "oss_encoder",
            "execution_mode": "oss_embedding_model",
            "model": embedding_model_name(),
            "fallback_used": False,
            "segment_count": len(segments),
        }
    if provider in {"", "deterministic", "rules", "local"}:
        if require_oss_understanding():
            raise MatrixArkError("deterministic segmentation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
        return intelligent_memory_segments(messages), {
            "provider": "deterministic",
            "execution_mode": "rules",
            "model": "matrixark-local-segmentation-v1",
            "fallback_used": False,
        }

    fallback_enabled = bool(envelope.get("segment_provider_fallback", False)) or provider in {"oss-fallback", "oss_with_fallback"} or os.getenv("MATRIXARK_SEGMENT_PROVIDER_FALLBACK", "").lower() in {"1", "true", "yes"}
    if provider in {"oss", "oss-fallback", "oss_with_fallback"}:
        model = str(envelope.get("segment_model") or os.getenv("MATRIXARK_SEGMENT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"))
        model_path = str(envelope.get("segment_model_path") or os.getenv("MATRIXARK_SEGMENT_MODEL_PATH", ""))
        max_new_tokens = int(envelope.get("segment_max_new_tokens") or os.getenv("MATRIXARK_SEGMENT_MAX_NEW_TOKENS", "512"))
        try:
            raw = oss_model_memory_segments(
                messages,
                model=model,
                model_path=model_path,
                max_new_tokens=max_new_tokens,
                local_only=fallback_enabled,
            )
            segments = normalize_model_segments(raw, messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "oss_model",
                "model": model_path or model,
                "fallback_used": False,
                "segment_count": len(segments),
            }
        except Exception as exc:  # pragma: no cover - optional local model stack.
            if not fallback_enabled:
                raise MatrixArkError(f"OSS segment provider failed: {exc}") from exc
            segments = intelligent_memory_segments(messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "rules_fallback",
                "model": model_path or model,
                "fallback_used": True,
                "fallback_reason": str(exc),
                "segment_count": len(segments),
            }
    raise MatrixArkError("segment_provider must be deterministic, oss, or oss-fallback")


def build_segment_prompt(messages: list[Json]) -> str:
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    return (
        "You are MatrixArk's memory segmentation extractor. Identify high-saliency memory segments from the indexed conversation. "
        "Prune greetings, acknowledgements, and filler. Merge semantically related non-contiguous messages into the same segment. "
        "Return only valid JSON with this shape: "
        '{"segments":[{"topic":"short_snake_case","coordinate_tuples":[[start,end]],"message_indexes":[0],"saliency_score":0.0,"summary_text":"short summary"}]} '
        "Indexes are zero-based and coordinate end is inclusive. Do not include messages that are only filler.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )


def oss_model_memory_segments(messages: list[Json], *, model: str, model_path: str = "", max_new_tokens: int = 512, local_only: bool = False) -> Json:
    try:
        import torch  # type: ignore
        from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional OSS stack.
        raise MatrixArkError("torch and transformers are required for segment_provider=oss") from exc

    target = model_path or model
    cache_key = f"{target}:{max_new_tokens}"
    cached = _OSS_SEGMENT_MODEL_CACHE.get(cache_key)
    if cached is None:
        local_only = bool(local_only) or bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").lower() in {"1", "true", "yes"}
        tokenizer = AutoTokenizer.from_pretrained(target, local_files_only=local_only)
        model_obj = AutoModelForCausalLM.from_pretrained(target, local_files_only=local_only)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model_obj.to(device)
        model_obj.eval()
        cached = {"tokenizer": tokenizer, "model": model_obj, "device": device}
        _OSS_SEGMENT_MODEL_CACHE[cache_key] = cached
    tokenizer = cached["tokenizer"]
    model_obj = cached["model"]
    device = cached["device"]
    prompt = build_segment_prompt(messages)
    if getattr(tokenizer, "chat_template", None):
        chat = [
            {"role": "system", "content": "Return only JSON. No markdown."},
            {"role": "user", "content": prompt},
        ]
        input_ids = tokenizer.apply_chat_template(chat, add_generation_prompt=True, return_tensors="pt").to(device)
        outputs = model_obj.generate(input_ids, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][input_ids.shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    else:
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
        inputs = {key: value.to(device) for key, value in inputs.items()}
        outputs = model_obj.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][inputs["input_ids"].shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    return parse_first_json_object(response)


def normalize_model_segments(raw: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw, list):
        raw_segments = raw
    elif isinstance(raw, dict) and isinstance(raw.get("segments"), list):
        raw_segments = raw["segments"]
    else:
        raise MatrixArkError("OSS segment provider must return {segments:[...]}")
    max_index = len(messages) - 1
    normalized: list[Json] = []
    for raw_segment in raw_segments[:12]:
        if not isinstance(raw_segment, dict):
            continue
        topic = re.sub(r"[^a-z0-9_]+", "_", str(raw_segment.get("topic") or "model_segment").lower()).strip("_") or "model_segment"
        coordinate_tuples = normalize_coordinate_tuples(raw_segment.get("coordinate_tuples"), max_index)
        message_indexes = normalize_message_indexes(raw_segment.get("message_indexes"), coordinate_tuples, max_index)
        if not message_indexes:
            continue
        if not coordinate_tuples:
            coordinate_tuples = contiguous_ranges(message_indexes)
        segment_text = "\n".join(f"{index}: {messages[index].get('content', '')}" for index in message_indexes)
        saliency = raw_segment.get("saliency_score", 0.85)
        try:
            saliency_score = max(0.0, min(1.0, float(saliency)))
        except (TypeError, ValueError):
            saliency_score = 0.85
        summary_text = str(raw_segment.get("summary_text") or summarize_text(segment_text, limit=420))
        normalized.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": message_indexes,
                "saliency_score": round(saliency_score, 6),
                "summary_text": summarize_text(summary_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "detected_by": "oss_model",
            }
        )
    normalized.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return normalized


def normalize_coordinate_tuples(value: Any, max_index: int) -> list[list[int]]:
    ranges: list[list[int]] = []
    if not isinstance(value, list):
        return ranges
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            continue
        try:
            start = int(item[0])
            end = int(item[1])
        except (TypeError, ValueError):
            continue
        start = max(0, min(max_index, start))
        end = max(0, min(max_index, end))
        if end < start:
            start, end = end, start
        ranges.append([start, end])
    return ranges


def normalize_message_indexes(value: Any, coordinate_tuples: list[list[int]], max_index: int) -> list[int]:
    indexes: set[int] = set()
    if isinstance(value, list):
        for item in value:
            try:
                index = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= index <= max_index:
                indexes.add(index)
    for start, end in coordinate_tuples:
        indexes.update(range(start, end + 1))
    return sorted(indexes)

def intelligent_memory_segments(messages: list[Json]) -> list[Json]:
    """Segment a batch into salient, event-centric memories.

    The production provider can emit the same coordinate tuples from one LLM
    call. The local implementation does deterministic semantic saliency and
    topic grouping, including non-contiguous segment consolidation.
    """

    salient: list[tuple[int, Json, str, str, float]] = []
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        saliency = semantic_saliency_score(text)
        if saliency < 0.5:
            continue
        topic = infer_segment_topic(text)
        salient.append((index, message, text, topic, saliency))
    grouped: dict[str, list[tuple[int, Json, str, float]]] = {}
    for index, message, text, topic, saliency in salient:
        grouped.setdefault(topic, []).append((index, message, text, saliency))

    segments = []
    for topic, items in grouped.items():
        if not items:
            continue
        coordinate_tuples = contiguous_ranges([item[0] for item in items])
        segment_text = "\n".join(f"{index}: {text}" for index, _message, text, _score in items)
        avg_saliency = sum(item[3] for item in items) / len(items)
        segments.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": [item[0] for item in items],
                "saliency_score": round(avg_saliency, 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def semantic_saliency_score(text: str) -> float:
    lower = text.lower().strip()
    if not lower:
        return 0.0
    filler = {
        "hi",
        "hello",
        "hey",
        "thanks",
        "thank you",
        "ok",
        "okay",
        "cool",
        "great",
        "sounds good",
    }
    compact = re.sub(r"[^a-z0-9 ]+", "", lower).strip()
    if compact in filler or len(compact) < 8:
        return 0.0
    score = 0.2
    if re.search(r"\b(recursion|base case|merge sort|algorithm|complexity|efficiency|dynamic programming|graph|game)\b", lower):
        score += 0.55
    if re.search(r"\b(prefer|favorite|approved|budget|plan|correction|instead|current|remember|important|moved|moving|located|location|live|lives|staying|deadline|owner|owns|reviewer|checklist|decision|decided|require|requires|required|incident|runbook|alert|outage|rollback|metric|latency|p95|p99|sla|policy|control_state|blocked|blocker)\b", lower):
        score += 0.45
    if re.search(r"\b(is|means|because|therefore|warning|avoid|must|should|cannot|can|require|requires|required|blocked|blocker)\b", lower):
        score += 0.2
    if re.search(r"\b(\d{2,}|monday|tuesday|wednesday|thursday|friday|saturday|sunday|january|february|march|april|may|june|july|august|september|october|november|december)\b", lower):
        score += 0.1
    if len(tokens(text)) >= 8:
        score += 0.15
    return min(score, 1.0)


def infer_segment_topic(text: str) -> str:
    lower = text.lower()
    topic_keywords = [
        ("recursion", ["recursion", "recursive", "base case", "merge sort", "call stack"]),
        ("game_algorithm", ["game", "minimax", "alpha beta", "pathfinding", "npc"]),
        ("preference", ["prefer", "favorite", "likes", "loves"]),
        ("location", ["moved", "moving", "located", "location", "live", "lives", "staying"]),
        ("approval_budget", ["approved", "approval", "budget", "cost", "purchase"]),
        ("incident_runbook", ["incident", "runbook", "alert", "outage", "rollback", "postmortem"]),
        ("task_decision", ["decision", "decided", "owner", "owns", "deadline", "checklist", "reviewer", "require", "requires", "required"]),
        ("metric_sla", ["metric", "latency", "p95", "p99", "qps", "sla", "error rate"]),
        ("plan_status", ["plan", "current", "status", "going to", "will"]),
        ("correction", ["correction", "instead", "wrong", "changed", "updated"]),
    ]
    for topic, keywords in topic_keywords:
        if any(keyword in lower for keyword in keywords):
            return topic
    token_list = [token for token in tokens(text) if len(token) > 4]
    return token_list[0] if token_list else "general"


def contiguous_ranges(indexes: list[int]) -> list[list[int]]:
    if not indexes:
        return []
    ordered = sorted(set(indexes))
    ranges: list[list[int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append([start, previous])
        start = previous = value
    ranges.append([start, previous])
    return ranges


def infer_event_type(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ["correct", "correction", "wrong", "instead", "updated", "changed"]):
        return "correction"
    if any(term in lower for term in ["yes", "confirmed", "approved", "looks good"]):
        return "confirmation"
    if any(term in lower for term in ["prefer", "favorite", "like", "love"]):
        return "preference_update"
    if any(term in lower for term in ["plan", "going to", "will ", "schedule"]):
        return "plan_update"
    if any(term in lower for term in ["work", "job", "role", "status", "position"]):
        return "status_update"
    return "dialogue_batch"


UNDERSTANDING_LABELS: dict[str, str] = {
    "confirmation": "confirmation approval accepted answer yes correct looks good",
    "correction": "correction wrong changed updated instead stale fact",
    "preference_update": "user preference likes prefers favorite language tool choice",
    "plan_update": "future plan schedule going to next step planned trip",
    "status_update": "job role work status position current responsibility",
    "approval": "business approval purchase approval budget approval confirmed cost",
    "location": "current location city moved to lives in staying at",
    "relationship": "relationship manager sister brother teammate family person",
    "family_profile": "family profile pet dog cat child sibling household fact",
    "current_plan": "current plan upcoming action task to complete next milestone",
    "session": "general conversation memory useful session fact",
}

QUERY_TYPE_LABELS: dict[str, str] = {
    "date": "question asks when date before after yesterday tomorrow week month year",
    "current_state": "question asks current latest now still status preference location role valid state",
    "why_emotion": "question asks why reason feeling emotion because",
    "evidence": "question asks quote exact message evidence what did someone say",
    "procedure": "question asks procedure steps troubleshoot debug rollback runbook checklist how to fix",
    "broad_exploration": "question asks overview summarize broad exploration topics inventory what is known",
    "multi_hop": "question requires combining multiple sessions people facts cross conversation reasoning",
    "fact": "question asks a direct factual answer",
}

QUERY_INDEX_LABELS: dict[str, str] = {
    "entity_type:location": "location city moved lives staying where user is",
    "entity_type:preference": "preference prefer favorite likes language tool choice",
    "event_type:preference_update": "preference update changed choice likes prefers",
    "entity_type:relationship": "relationship manager sister brother teammate family person",
    "entity_type:family_profile": "family pet dog cat child household",
    "entity_type:job_status": "job role work status position responsibility",
    "event_type:status_update": "job status role work update",
    "entity_type:current_plan": "plan current plan upcoming task schedule next milestone",
    "event_type:plan_update": "plan update going to schedule will next",
    "event_type:confirmation": "confirmation approved accepted yes correct confirmed",
    "entity_type:approval_state": "approval budget purchase cost approved",
    "entity_type:confirmation": "confirmation approved correct accepted",
    "classification:confirmation": "confirmation approved accepted yes correct",
    "segment_topic:approval_budget": "approval budget purchase GPU cost finance",
    "event_type:correction": "correction wrong changed updated instead stale",
    "entity_type:correction": "correction wrong changed updated instead",
    "classification:correction": "correction wrong changed update",
    "segment_topic:correction": "correction updated stale changed",
    "source_type:message": "raw message dialogue evidence",
    "source_type:feedback": "feedback accepted rejected final answer",
    "source_type:resource": "resource document file pdf markdown text csv table runbook policy docs",
    "source_type:skill": "skill tool instruction playbook procedure capability",
    "source_type:resource_fact": "extracted fact from resource decision owner cost deadline policy approval control_state procedure api",
    "resource_type:pdf": "pdf document page file",
    "resource_type:md": "markdown md readme documentation runbook",
    "resource_type:txt": "text txt note plain document",
    "resource_type:csv": "csv table rows spreadsheet",
    "resource_type:tsv": "tsv table rows spreadsheet",
    "resource_type:xlsx": "excel xlsx spreadsheet workbook sheet table",
    "resource_type:html": "html web page documentation",
    "resource_type:docx": "word docx document",
    "resource_type:pptx": "powerpoint pptx slide deck presentation",
    "unit_kind:paragraph": "paragraph text passage",
    "unit_kind:heading": "heading section title",
    "unit_kind:table_row_group": "table rows row group csv spreadsheet",
    "unit_kind:page": "page pdf page",
    "unit_kind:slide": "slide presentation deck",
    "unit_kind:code_symbol": "code function class symbol",
    "skill_trigger:context_pack_replay": "context pack replay audit inspect selected refs",
    "skill_tool:matrixark_replay": "matrixark replay tool context replay",
    "skill_tool:matrixark_audit": "matrixark audit tool context audit",
}


def require_oss_understanding() -> bool:
    return os.getenv("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "").strip().lower() in {"1", "true", "yes"}


def understanding_provider(envelope: Json | None = None) -> str:
    provider = ""
    if envelope:
        provider = str(envelope.get("understanding_provider") or envelope.get("extraction_provider") or "")
    provider = provider or os.getenv("MATRIXARK_UNDERSTANDING_PROVIDER", os.getenv("MATRIXARK_EXTRACTION_PROVIDER", "deterministic"))
    provider = provider.strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "embedding", "oss_embedding"}:
        return "oss_encoder"
    if provider in {"", "deterministic", "rules", "local"} and require_oss_understanding():
        raise MatrixArkError("deterministic extraction/query understanding is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
    return provider or "deterministic"


def prototype_vectors(labels: dict[str, str]) -> dict[str, list[float]]:
    cache_key = json.dumps(labels, sort_keys=True) + "|" + embedding_model_name()
    cached = _OSS_UNDERSTANDING_PROTOTYPE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    vectors = {label: embedding_for_text(description) for label, description in labels.items()}
    _OSS_UNDERSTANDING_PROTOTYPE_CACHE[cache_key] = vectors
    return vectors


def oss_encoder_rank_labels(text: str, labels: dict[str, str], *, limit: int = 5) -> list[Json]:
    query_vector = embedding_for_text(text)
    ranked = [
        {
            "label": label,
            "score": round(normalized_dense_score(cosine(query_vector, vector)), 6),
            "description": labels[label],
        }
        for label, vector in prototype_vectors(labels).items()
    ]
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:limit]


def oss_encoder_event_type(text: str) -> str:
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
    label = str(ranked[0]["label"]) if ranked else "session"
    if label == "approval":
        return "confirmation"
    if label == "location":
        return "status_update"
    if label in {"relationship", "family_profile"}:
        return "dialogue_batch"
    return label


def oss_encoder_compact_extraction(envelope: Json, *, prior_context: Json) -> Json:
    text = text_from_messages(envelope["messages"])
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=5)
    top = str(ranked[0]["label"]) if ranked else "session"
    classification = "NEW_EVENT"
    status = "observed"
    if envelope["kind"] == "feedback":
        if not prior_context.get("level"):
            classification = "AMBIGUOUS"
        elif top in {"confirmation", "approval"}:
            classification = "CONFIRMATION"
            status = "accepted"
        elif top == "correction":
            classification = "CORRECTION"
            status = "rejected"
        else:
            classification = "FEEDBACK"
    return {
        "mode": "matrixark_internal_oss_encoder",
        "understanding_provider": "oss_encoder",
        "classification": classification,
        "status": status,
        "event_type": oss_encoder_event_type(text),
        "label_scores": ranked,
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "" if classification != "AMBIGUOUS" else "short feedback lacks prior context",
    }


def oss_encoder_extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    text = text_from_messages(messages)
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=8)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    entities: list[Json] = []
    for item in ranked:
        label = str(item["label"])
        if label == "approval":
            entity_type = "approval_state"
        elif label == "status_update":
            entity_type = "job_status"
        elif label == "plan_update":
            entity_type = "current_plan"
        elif label == "preference_update":
            entity_type = "preference"
        else:
            entity_type = label
        if float(item["score"]) < 0.42 and entity_type != "session":
            continue
        state = summarize_text(f"{entity_type}: {text}", limit=220)
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": canonical_entity_name(entity_type, state) or entity_type,
                "state": state,
                "confidence": round(float(item["score"]), 6),
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, entity_type),
                "field_patches": [entity_patch("", state)] if entity_type != "session" else [],
                "extracted_by": "oss_encoder",
            }
        )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.5,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "session"),
                "field_patches": [],
                "extracted_by": "oss_encoder",
            }
        )
    return dedupe_entities(entities)


def oss_encoder_memory_segments(messages: list[Json]) -> list[Json]:
    labeled: dict[str, list[tuple[int, Json, float]]] = {}
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        if not text.strip():
            continue
        ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
        label = str(ranked[0]["label"]) if ranked else "session"
        score = float(ranked[0]["score"]) if ranked else 0.5
        labeled.setdefault(label, []).append((index, message, score))
    segments = []
    for label, items in labeled.items():
        indexes = [index for index, _message, _score in items]
        ranges = contiguous_ranges(indexes)
        segment_text = "\n".join(f"{index}: {message.get('content', '')}" for index, message, _score in items)
        segments.append(
            {
                "topic": label,
                "coordinate_tuples": ranges,
                "message_indexes": indexes,
                "saliency_score": round(sum(score for _index, _message, score in items) / max(len(items), 1), 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(ranges) > 1,
                "detected_by": "oss_encoder",
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*([^.;!?]{2,120})"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
    ]
    for entity_type, pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = " ".join(match.group(1).split()).strip(" :-") if match.groups() else ""
            if entity_type == "confirmation" and not envelope.get("context_pack_id") and not lower.strip() in {
                "yes",
                "yes.",
                "correct",
                "correct.",
                "approved",
                "approved.",
            }:
                continue
            entity_name = canonical_entity_name(entity_type, value)
            field_patches = infer_entity_field_patches(entity_type, value, text)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": source_refs,
                    "operator": normalize_entity_operator(None, entity_type),
                    "field_patches": field_patches,
                }
            )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.6,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "session"),
                "field_patches": [],
            }
        )
    return dedupe_entities(entities)


def infer_entity_field_patches(entity_type: str, value: str, text: str) -> list[Json]:
    patches: list[Json] = []
    correction = re.search(
        r"\b(?:correction|correct|wrong|updated|changed)[:\s]+([^.;!?]+?)\s+(?:instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if correction:
        replace = clean_patch_value(correction.group(1))
        search = clean_patch_value(correction.group(2))
        patches.append(entity_patch(search, replace))
    preference = re.search(
        r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]+?)\s+(?:now|instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if entity_type == "preference" and preference:
        replace = clean_patch_value(preference.group(1))
        search = clean_patch_value(preference.group(2))
        patches.append(entity_patch(search, replace))
    evolving_entity_types = {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "relationship",
        "approval_state",
        "correction",
        "confirmation",
    }
    if entity_type in evolving_entity_types and not patches and value:
        patches.append(entity_patch("", summarize_text(value, limit=180)))
    return patches[:3]


def clean_patch_value(value: str) -> str:
    return summarize_text(" ".join(value.split()).strip(" ,;:-"), limit=180)


def canonical_entity_name(entity_type: str, value: str) -> str:
    compact_value = " ".join(str(value or "").split()).strip(" ,;:-")
    if entity_type in {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    }:
        return entity_type
    if entity_type == "approval_state":
        subject = compact_value
        subject_patterns = [
            r"^(?:the\s+)?(.+?)\s+(?:is|was|are|were|has been|have been)\s+(?:approved|required|missing|blocked|ready|done|complete|needed)\b",
            r"^(?:the\s+)?(.+?)\s+as\s+(?:a\s+|an\s+|the\s+)?(?:blocker|requirement|approval|decision|status)\b",
            r"^(?:the\s+)?(.+?)\s+after\b",
            r"^(?:the\s+)?(.+?)\s+before\b",
            r"^(?:the\s+)?(.+?)\s+because\b",
            r"^(?:the\s+)?(.+?)\s+as\b",
        ]
        for pattern in subject_patterns:
            match = re.search(pattern, subject, flags=re.IGNORECASE)
            if match:
                subject = match.group(1)
                break
        subject = re.sub(r"^(?:the|a|an)\s+", "", subject, flags=re.IGNORECASE).strip(" ,;:-")
        return summarize_text(subject or compact_value, limit=80) if (subject or compact_value) else entity_type
    return compact_value[:80] if compact_value else entity_type


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    positions: dict[tuple[Any, str], int] = {}
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            if entity.get("entity_name") == entity.get("entity_type"):
                out[positions[key]] = entity
            continue
        seen.add(key)
        positions[key] = len(out)
        out.append(entity)
    return out[:12]


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def resource_extraction_mode(envelope: Json) -> str:
    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return "matrixark_resource_schema_oss_encoder"
    if provider in {"openai", "openai_compatible", "openai-compatible"}:
        return "matrixark_resource_schema_openai_compatible"
    return "matrixark_resource_schema"


def extract_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    """Extract cited resource facts through the same provider-shaped contract as messages.

    The local implementation is deterministic for CI. OSS/OpenAI-compatible
    providers should emit the same fields so storage, indexes, and replay stay
    unchanged.
    """
    mode = resource_extraction_mode(envelope)
    provider = understanding_provider(envelope)
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            model_facts = openai_compatible_resource_facts(
                chunk,
                chunk_metadata=chunk_metadata,
                envelope=envelope,
                raw_uri=raw_uri,
                resource_version=resource_version,
            )
            if model_facts or require_oss_understanding():
                return model_facts
        except MatrixArkError:
            if require_oss_understanding():
                raise
    facts: list[Json] = []
    for fact_schema in matched_resource_fact_schemas(chunk.text, chunk.metadata):
        fact_event_type = str(fact_schema["fact_type"])
        fact_entity_type = str(fact_schema["entity_type"])
        fact_value = extract_resource_fact_value(chunk.text, fact_event_type)
        facts.append(
            {
                "mode": mode,
                "classification": "RESOURCE_FACT",
                "event_type": fact_event_type,
                "entity_type": fact_entity_type,
                "status": "observed",
                "value": fact_value,
                "entity_name": resource_fact_entity_name(fact_schema, fact_value, chunk_metadata, raw_uri),
                "confidence": 0.82 if fact_event_type != "resource_fact" else 0.68,
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": understanding_provider(envelope),
            }
        )
    return facts


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


def infer_query_type(query: str) -> str:
    if understanding_provider() == "oss_encoder":
        return oss_encoder_query_type(query)
    lower = query.lower()
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week|before|after|as of|valid as of)\b", lower):
        return "date"
    if re.search(r"\b(current|currently|latest|now|still|today|valid|status|preference|prefer|likes|where does|where is)\b", lower):
        return "current_state"
    if re.search(r"\b(why|reason|because|feel|felt|emotion|happy|sad|angry|worried|excited)\b", lower):
        return "why_emotion"
    if re.search(r"\b(evidence|quote|exactly|what did .* say|conversation|dialogue|message)\b", lower):
        return "evidence"
    if re.search(r"\b(overview|summarize|summary|explore|broad|what is in|what do we know|topics|map|inventory)\b", lower):
        return "broad_exploration"
    if re.search(r"\b(procedure|steps?|how to|troubleshoot|debug|rollback|runbook|playbook|checklist|fix|remediate|mitigate)\b", lower):
        return "procedure"
    if re.search(r"\b(both|together|across|between|compare|combine|sessions|multi-hop|multi session|multi-session)\b", lower):
        return "multi_hop"
    return "fact"


def infer_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    if understanding_provider() == "oss_encoder":
        return oss_encoder_secondary_index_filter_groups(query, question_type)
    return deterministic_secondary_index_filter_groups(query, question_type)


def oss_encoder_query_type(query: str) -> str:
    ranked = oss_encoder_rank_labels(query, QUERY_TYPE_LABELS, limit=2)
    if not ranked:
        return "fact"
    top = str(ranked[0]["label"])
    if len(ranked) > 1 and top == "fact" and float(ranked[1]["score"]) >= float(ranked[0]["score"]) - 0.015:
        return str(ranked[1]["label"])
    return top


def oss_encoder_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    ranked = oss_encoder_rank_labels(f"{question_type}: {query}", QUERY_INDEX_LABELS, limit=5)
    selected = [str(item["label"]) for item in ranked if float(item["score"]) >= 0.46]
    if not selected and ranked:
        selected = [str(ranked[0]["label"])]
    groups: list[set[str]] = []
    by_prefix: dict[str, set[str]] = {}
    for label in selected:
        prefix = label.split(":", 1)[0]
        by_prefix.setdefault(prefix, set()).add(label)
    for labels in by_prefix.values():
        if labels and labels not in groups:
            groups.append(labels)
    return groups[:4]


def candidate_index_terms(
    record: Json,
    index_terms_by_batch: dict[Any, list[str]],
    index_terms_by_node: dict[Any, list[str]],
    index_terms_by_ref: dict[Any, list[str]] | None = None,
) -> set[str]:
    terms: set[str] = set()
    index_terms_by_ref = index_terms_by_ref or {}
    record_type = record.get("record_type")
    if record_type == "context_event":
        terms.update(index_terms_by_batch.get(record.get("batch_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("event_type", record.get("event_type")))
        if not require_oss_understanding() and not record.get("event_type"):
            terms.add(context_index_name("event_type", infer_event_type(str(record.get("text", "")))))
        classification = non_default_classification(record.get("classification"))
        if classification:
            terms.add(context_index_name("classification", classification))
        terms.add(context_index_name("status", record.get("status") or "observed"))
        terms.add(context_index_name("source_type", record.get("source_type") or "message"))
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
    elif record_type == "resource_chunk":
        terms.update(index_terms_by_ref.get(record.get("chunk_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "resource"))
        terms.add(context_index_name("resource_type", record.get("resource_type")))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    elif record_type == "skill_manifest":
        terms.update(index_terms_by_ref.get(record.get("skill_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.add(context_index_name("skill_name", record.get("name")))
        for trigger in record.get("triggers", [])[:8]:
            terms.add(context_index_name("skill_trigger", trigger))
        for tool in record.get("allowed_tools", [])[:8]:
            terms.add(context_index_name("skill_tool", tool))
    elif record_type == "skill_section":
        terms.update(index_terms_by_ref.get(record.get("section_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    return {term for term in terms if term}


def passes_secondary_index_filters(candidate_terms: set[str], required_groups: list[set[str]], *, mode: str = "all_groups") -> bool:
    if not required_groups:
        return True
    if mode == "any_group":
        return any(bool(candidate_terms.intersection(group)) for group in required_groups)
    return all(bool(candidate_terms.intersection(group)) for group in required_groups)


def passes_applicable_secondary_index_filters(
    candidate_terms: set[str],
    required_groups: list[set[str]],
    *,
    mode: str = "all_groups",
) -> bool:
    """Apply only filter groups whose index prefix is present on this candidate."""
    candidate_prefixes = {term.split(":", 1)[0] for term in candidate_terms if ":" in term}
    candidate_is_context_asset = bool(
        candidate_terms.intersection({"source_type:resource", "source_type:skill"})
    )
    applicable_groups = [
        group
        for group in required_groups
        if candidate_prefixes.intersection({term.split(":", 1)[0] for term in group if ":" in term})
        and not (
            candidate_is_context_asset
            and {term.split(":", 1)[0] for term in group if ":" in term} == {"source_type"}
            and not candidate_terms.intersection(group)
        )
    ]
    return passes_secondary_index_filters(candidate_terms, applicable_groups, mode=mode)



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
    raw = args.get("cross_session", ranking.get("cross_session", {}))
    if isinstance(raw, bool):
        config: Json = {"enabled": raw}
    elif raw is None:
        config = {}
    elif isinstance(raw, dict):
        config = raw
    else:
        raise MatrixArkError("cross_session must be an object or boolean")

    default_enabled = session_scope == "prefer" and remote_budget_tokens > 0
    enabled = bool(config.get("enabled", default_enabled)) and session_scope == "prefer" and remote_budget_tokens > 0
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type in {"current_state", "latest"}:
        default_ratio = DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO
        question_budget_reason = "current_state_or_latest_queries_need_prior entity state and stale blockers"
    elif normalized_question_type in {"multi_hop", "date"}:
        default_ratio = DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO
        question_budget_reason = "multi_hop_or_date_queries_often_need_multiple sessions"
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        default_ratio = DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO
        question_budget_reason = "broad_or_evidence_queries_get_extra cross-session exploration"
    else:
        default_ratio = DEFAULT_CROSS_SESSION_BUDGET_RATIO
        question_budget_reason = "normal_queries_keep_cross_session_small so current session/resources/skills dominate"
    max_budget_ratio = max(0.0, min(1.0, float(config.get("max_budget_ratio", DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO))))
    budget_ratio = float_arg(config, "budget_ratio", min(default_ratio, max_budget_ratio), minimum=0.0, maximum=max_budget_ratio)
    max_budget_default = DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS
    max_budget_tokens = integer_arg(config, "max_budget_tokens", max_budget_default, minimum=0)
    computed_budget = int(remote_budget_tokens * budget_ratio)
    if remote_budget_tokens >= 1200 and computed_budget > 0:
        computed_budget = max(DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS, computed_budget)
    budget_tokens = integer_arg(config, "budget_tokens", computed_budget, minimum=0) if "budget_tokens" in config else computed_budget
    ratio_budget_cap = int(remote_budget_tokens * max_budget_ratio) if max_budget_ratio > 0 else 0
    budget_tokens = min(
        remote_budget_tokens,
        budget_tokens,
        ratio_budget_cap if ratio_budget_cap > 0 else remote_budget_tokens,
        max_budget_tokens if max_budget_tokens > 0 else remote_budget_tokens,
    )
    max_sessions = integer_arg(config, "max_sessions", DEFAULT_CROSS_SESSION_MAX_SESSIONS, minimum=0)
    max_candidates = integer_arg(config, "max_candidates", DEFAULT_CROSS_SESSION_MAX_CANDIDATES, minimum=0)
    min_entity_bridge_refs = integer_arg(config, "min_entity_bridge_refs", DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS, minimum=0)
    parallelism = integer_arg(config, "parallelism", DEFAULT_CROSS_SESSION_PARALLELISM, minimum=1)
    min_score = float_arg(config, "min_score", DEFAULT_CROSS_SESSION_MIN_SCORE, minimum=0.0, maximum=1.0)
    raw_evidence_min_score = float_arg(config, "raw_evidence_min_score", DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE, minimum=0.0, maximum=1.0)
    preferred_ref_types = config.get("preferred_ref_types", list(DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES))
    if not isinstance(preferred_ref_types, list):
        raise MatrixArkError("cross_session.preferred_ref_types must be an array")
    preferred_ref_types = [str(item).strip() for item in preferred_ref_types if str(item).strip()]
    return {
        "enabled": enabled,
        "mode": "prefer" if enabled else "disabled",
        "decision": "always_consider_same_user_cross_session_when_session_scope_prefer" if enabled else "disabled_by_session_scope_or_budget",
        "question_type": normalized_question_type,
        "question_budget_reason": question_budget_reason,
        "budget_ratio": round(budget_ratio, 6),
        "max_budget_ratio": round(max_budget_ratio, 6),
        "budget_tokens": budget_tokens if enabled else 0,
        "remote_budget_tokens": remote_budget_tokens,
        "max_budget_tokens": max_budget_tokens,
        "max_sessions": max_sessions if enabled else 0,
        "max_candidates": max_candidates if enabled else 0,
        "min_score": min_score if enabled else 0.0,
        "raw_evidence_min_score": raw_evidence_min_score if enabled else 0.0,
        "preferred_ref_types": preferred_ref_types if enabled else [],
        "min_entity_bridge_refs": min_entity_bridge_refs if enabled else 0,
        "parallelism": parallelism if enabled else 0,
        "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
        "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events",
    }


def build_shared_context_policy(args: Json, ranking: Json, *, remote_budget_tokens: int) -> Json:
    raw = args.get("shared_context", ranking.get("shared_context", {}))
    if isinstance(raw, bool):
        config: Json = {"enabled": raw}
    elif raw is None:
        config = {}
    elif isinstance(raw, dict):
        config = raw
    else:
        raise MatrixArkError("shared_context must be an object or boolean")
    enabled = bool(config.get("enabled", True)) and remote_budget_tokens > 0
    resource_budget_ratio = float_arg(
        config,
        "resource_budget_ratio",
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    skill_budget_ratio = float_arg(
        config,
        "skill_budget_ratio",
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    resource_budget_tokens = int(remote_budget_tokens * resource_budget_ratio)
    skill_budget_tokens = int(remote_budget_tokens * skill_budget_ratio)
    if "resource_budget_tokens" in config:
        resource_budget_tokens = integer_arg(config, "resource_budget_tokens", resource_budget_tokens, minimum=0)
    if "skill_budget_tokens" in config:
        skill_budget_tokens = integer_arg(config, "skill_budget_tokens", skill_budget_tokens, minimum=0)
    resource_max = integer_arg(config, "resource_max_budget_tokens", DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS, minimum=0)
    skill_max = integer_arg(config, "skill_max_budget_tokens", DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS, minimum=0)
    if resource_max > 0:
        resource_budget_tokens = min(resource_budget_tokens, resource_max)
    if skill_max > 0:
        skill_budget_tokens = min(skill_budget_tokens, skill_max)
    min_score = float_arg(config, "min_score", DEFAULT_SHARED_CONTEXT_MIN_SCORE, minimum=0.0, maximum=1.0)
    return {
        "enabled": enabled,
        "mode": "bounded_shared_context" if enabled else "disabled",
        "decision": "tenant_or_global_shared_resources_and_skills_visible_after_access_scope_then_quota_bounded" if enabled else "disabled_by_budget_or_config",
        "resource_budget_ratio": round(resource_budget_ratio, 6),
        "skill_budget_ratio": round(skill_budget_ratio, 6),
        "resource_budget_tokens": resource_budget_tokens if enabled else 0,
        "skill_budget_tokens": skill_budget_tokens if enabled else 0,
        "resource_max_budget_tokens": resource_max,
        "skill_max_budget_tokens": skill_max,
        "remote_budget_tokens": remote_budget_tokens,
        "min_score": min_score if enabled else 0.0,
        "visibility_labels": ["tenant_shared", "global_shared"],
        "strategy": "shared_resources_and_skills_live_outside_sessions_and_are_bounded_before_final_pack",
    }


def sharing_scope_from_candidate(candidate: Json) -> str:
    for source in [candidate, candidate.get("access_scope", {}), candidate.get("metadata", {}), candidate.get("scope", {})]:
        if isinstance(source, dict):
            value = str(source.get("sharing_scope") or "").strip().lower()
            if value:
                return value
    node_path = [str(part).lower() for part in candidate.get("node_path", []) if str(part)]
    if node_path[:2] == ["global", "shared"]:
        return "global_shared"
    if len(node_path) >= 2 and node_path[0].startswith("tenant:") and node_path[1] == "shared":
        return "tenant_shared"
    return "private_user"


def is_shared_resource_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "resource_chunk" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def is_shared_skill_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "skill_section" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def bounded_max_children_scored_per_parent(value: int) -> int:
    hard_cap = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
    if value > hard_cap:
        raise MatrixArkError(
            "max_children_scored_per_parent must be <= "
            f"{hard_cap}; split over-wide ContextNode children into deeper node layers"
        )
    return value


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    freshness_tolerance_ms = integer_arg(
        ranking,
        "freshness_tolerance_ms",
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        minimum=0,
    )
    half_life_ms = integer_arg(
        ranking,
        "half_life_ms",
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        minimum=1,
    )
    type_weights = {**DEFAULT_BUSINESS_TYPE_WEIGHTS, **optional_object(ranking, "business_type_weights")}
    weights = optional_object(ranking, "weights")
    origin_score = clamp01(candidate.get("origin_score"))
    s_time = time_decay_score(
        candidate.get("updated_at_ms"),
        reference_time_ms=reference_time_ms,
        freshness_tolerance_ms=freshness_tolerance_ms,
        half_life_ms=half_life_ms,
    )
    s_busi = business_score_for_candidate(candidate, type_weights)
    continuity_boost = session_continuity_boost(candidate, str(candidate.get("question_type") or candidate.get("packing_policy") or "fact"))
    cross_session_rerank_boost = cross_session_rerank_adjustment(candidate, str(candidate.get("question_type") or candidate.get("packing_policy") or "fact"))
    final_score = clamp01(final_recall_score(origin_score, s_time, s_busi, weights) + continuity_boost + cross_session_rerank_boost)
    return {
        **candidate,
        "origin_score": origin_score,
        "time_score": s_time,
        "business_score": s_busi,
        "continuity_boost": round(continuity_boost, 6),
        "cross_session_rerank_boost": round(cross_session_rerank_boost, 6),
        "final_score": final_score,
        "score": final_score,
        "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi+continuity_boost+cross_session_rerank_boost",
    }


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    selected: list[Json] = []
    seen: set[tuple[str, Any]] = set()

    def take(items: list[Json], limit: int) -> None:
        for item in items:
            key = (str(item.get("ref_type", "")), item.get("ref_hash"))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= limit:
                return

    auxiliary_quota = max(0, min(auxiliary_quota, total_limit))
    primary_quota = max(0, total_limit - auxiliary_quota)
    take(primary, primary_quota)
    take(auxiliary, total_limit)
    if len(selected) < total_limit:
        take(primary, total_limit)
    return selected[:total_limit]


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    ref_type = str(candidate.get("ref_type", ""))
    context_class = str(candidate.get("context_class") or ref_type)
    text = str(candidate.get("text", "")).lower()
    event_type = str(candidate.get("event_type") or candidate.get("entity_type") or candidate.get("topic") or "").lower()
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    if question_type == "procedure":
        if ref_type == "skill_section":
            return 0.36
        if context_class in {"resource_fact", "resource_entity_fact"} and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", event_type + " " + text):
            return 0.34
        if ref_type == "resource_chunk" and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", text):
            return 0.30
        return 0.0
    if question_type == "broad_exploration":
        if ref_type == "summary":
            return 0.35
        if ref_type in {"segment", "compression"}:
            return 0.16
        return 0.02 if ref_type in {"resource_chunk", "event", "entity"} else 0.0
    if ref_type == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        # Multi-event TIME_COMPRESS records should win tight fact/current/multi-hop packs
        # because they preserve an old answer-bearing window in fewer tokens.
        return 0.50 if source_count >= 2 else 0.24
    if question_type == "current_state":
        if ref_type == "entity":
            return 0.30
        if context_class == "resource_entity_fact":
            return 0.28
        if context_class == "resource_fact" or "correction" in event_type or "confirmation" in event_type:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.10
        return 0.0
    if question_type == "evidence":
        if ref_type == "resource_chunk" and has_citation:
            return 0.30
        if ref_type == "event":
            return 0.24
        return 0.05 if ref_type == "segment" else 0.0
    if question_type == "date":
        if ref_type == "event" and re.search(r"\b(20\d{2}|19\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|monday|tuesday|wednesday|thursday|friday|saturday|sunday|before|after|on)\b", text):
            return 0.28
        return 0.08 if ref_type == "entity" else 0.0
    if question_type == "multi_hop":
        return 0.14 if ref_type in {"entity", "segment"} else 0.04
    if question_type == "why_emotion":
        return 0.18 if re.search(r"\b(because|reason|felt|feel|happy|sad|angry|worried|excited|concerned)\b", text) else 0.0
    if question_type == "fact":
        negated_approval = bool(
            re.search(r"\b(no|not|without|missing|lacks?|lacked)\b.{0,48}\b(approval|approved|decision)\b", text)
            or re.search(r"\b(approval|approved|decision)\b.{0,48}\b(no|not|without|missing|lacks?|lacked)\b", text)
        )
        affirmative_approval = bool(re.search(r"\b(approved|approval granted|approval confirmed|confirmed approval)\b", text))
        if negated_approval and not affirmative_approval:
            return -0.12
        if affirmative_approval:
            return 0.38 if ref_type in {"event", "entity"} else 0.26
        if context_class in {"resource_fact", "resource_entity_fact"}:
            return 0.30
        if ref_type in {"entity", "event"}:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.06
        return 0.03
    return 0.0


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, float, float]:
    score = float(candidate.get("score", 0.0))
    boosted = clamp01(
        score
        + question_type_ref_boost(candidate, question_type)
        + session_continuity_boost(candidate, question_type)
        + cross_session_rerank_adjustment(candidate, question_type)
    )
    token_efficiency = boosted / max(1, token_count(str(candidate.get("text", ""))))
    if candidate.get("ref_type") == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        if source_count >= 2:
            token_efficiency *= 1.5
    return (boosted, token_efficiency, score)


def context_text_hashes(text: str) -> set[int]:
    compact = " ".join(str(text).split())
    variants = {compact[:512]}
    without_role = re.sub(r"^(user|assistant|tool|system):\s*", "", compact, flags=re.IGNORECASE)
    variants.add(without_role[:512])
    tokenized = tokens(compact)
    if tokenized:
        variants.add(" ".join(tokenized)[:512])
        if tokenized[0] in {"user", "assistant", "tool", "system"}:
            variants.add(" ".join(tokenized[1:])[:512])
    return {stable_hash(variant) for variant in variants if variant}


def local_context_budget(args: Json) -> Json:
    raw_items = args.get("local_context", [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise MatrixArkError("local_context must be an array")
    items: list[Json] = []
    text_hashes: set[int] = set()
    token_total = 0
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            text = item
            source = f"local:{index}"
            ref_type = "local_context"
        elif isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or "")
            source = str(item.get("source") or item.get("ref") or f"local:{index}")
            ref_type = str(item.get("ref_type") or "local_context")
        else:
            raise MatrixArkError("local_context items must be strings or objects")
        text = clip_context_text(text)
        if not text:
            continue
        item_tokens = token_count(text)
        token_total += item_tokens
        text_hashes.update(context_text_hashes(text))
        items.append(
            {
                "ref_type": ref_type,
                "source": source,
                "text": text,
                "token_estimate": item_tokens,
                "text_hash": stable_hash(text[:512]),
            }
        )
    explicit_tokens = args.get("local_context_tokens")
    token_source = "estimated_from_local_context"
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
        token_source = "agent_provided_local_context_tokens"
    raw_safety_margin = args.get("local_context_safety_margin_tokens")
    if raw_safety_margin is None:
        raw_safety_margin = os.environ.get("MATRIXARK_LOCAL_CONTEXT_SAFETY_MARGIN_TOKENS")
    if raw_safety_margin is None:
        raw_max_context = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        try:
            max_context_tokens = max(0, int(raw_max_context or DEFAULT_MAX_CONTEXT_TOKENS))
        except (TypeError, ValueError):
            max_context_tokens = DEFAULT_MAX_CONTEXT_TOKENS
        safety_margin_tokens = min(512, max_context_tokens // 20)
        safety_margin_source = "matrixark_default_5_percent_capped"
    else:
        try:
            safety_margin_tokens = int(raw_safety_margin or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
        safety_margin_source = "agent_provided_safety_margin" if "local_context_safety_margin_tokens" in args else "env_safety_margin"
    if safety_margin_tokens < 0:
        raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
        "token_source": token_source,
        "safety_margin_tokens": safety_margin_tokens,
        "safety_margin_source": safety_margin_source,
    }


def compact_local_context_refs(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
            }
        )
    return refs


def local_context_refs_for_pack(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
                "text": item.get("text", ""),
                "selection_reason": "provided by agent-visible local context before MatrixArk remote retrieval",
            }
        )
    return refs


def is_resource_or_skill_candidate(candidate: Json) -> bool:
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or "")
    return ref_type in {"resource_chunk", "skill_section"} or context_class in {"resource_fact", "resource_entity_fact"}


def dropped_candidate_audit_ref(candidate: Json, *, reason: str, token_estimate: int) -> Json:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    return {
        "ref_type": candidate.get("ref_type", ""),
        "ref_hash": candidate.get("ref_hash"),
        "context_class": candidate.get("context_class") or candidate.get("ref_type", ""),
        "drop_reason": reason,
        "reason": reason,
        "score": candidate.get("score", 0.0),
        "origin_score": candidate.get("origin_score", 0.0),
        "packing_score": round(packing_sort_key(candidate, str(candidate.get("packing_policy") or "fact"))[0], 6),
        "token_estimate": token_estimate,
        "token_cost": token_estimate,
        "raw_uri": candidate.get("raw_uri", ""),
        "source_ref": candidate.get("source_ref", ""),
        "citation": candidate.get("citation") or candidate.get("source_ref", ""),
        "resource_type": candidate.get("resource_type", ""),
        "resource_version": candidate.get("resource_version") or metadata.get("resource_version", ""),
        "version_state": candidate.get("version_state", "current"),
        "stale_or_superseded": bool(candidate.get("stale_or_superseded", False)),
        "access_decision": candidate.get("access_decision", "allowed_by_scope"),
        "selection_reason": candidate.get("selection_reason", ""),
        "matched_index_terms": candidate.get("matched_index_terms", []),
        "node_hash": candidate.get("node_hash"),
        "node_path": candidate.get("node_path", []),
    }


def record_dropped_candidate(dropped: Json, candidate: Json, *, reason: str, token_estimate: int) -> None:
    if not is_resource_or_skill_candidate(candidate):
        return
    dropped.setdefault("refs", []).append(dropped_candidate_audit_ref(candidate, reason=reason, token_estimate=token_estimate))


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    if question_type != "multi_hop":
        return candidates[:total_limit]
    selected: list[Json] = []
    deferred: list[Json] = []
    seen_nodes: set[Any] = set()
    for candidate in candidates:
        node_hash = candidate.get("node_hash")
        if node_hash not in seen_nodes:
            selected.append(candidate)
            seen_nodes.add(node_hash)
        else:
            deferred.append(candidate)
        if len(selected) >= total_limit:
            return selected
    selected.extend(deferred)
    return selected[:total_limit]


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
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
    candidate_pool_limit = max(
        selected_ref_cap,
        max(1, int(max_global_candidates or DEFAULT_MAX_GLOBAL_CANDIDATES)),
    )
    min_score = max(0.0, min(1.0, float(min_score)))
    budget_fill_policy = (budget_fill_policy or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=candidate_pool_limit)
    selected: list[Json] = []
    used_tokens = 0
    cross_session_policy = cross_session_policy or {"enabled": False, "budget_tokens": 0, "max_sessions": 0, "max_candidates": 0, "min_entity_bridge_refs": 0}
    cross_enabled = bool(cross_session_policy.get("enabled"))
    cross_budget_tokens = int(cross_session_policy.get("budget_tokens") or 0)
    cross_max_sessions = int(cross_session_policy.get("max_sessions") or 0)
    cross_max_candidates = int(cross_session_policy.get("max_candidates") or 0)
    cross_min_score = max(0.0, min(1.0, float(cross_session_policy.get("min_score") or 0.0)))
    cross_raw_evidence_min_score = max(0.0, min(1.0, float(cross_session_policy.get("raw_evidence_min_score") or 0.0)))
    cross_min_entity_bridge_refs = int(cross_session_policy.get("min_entity_bridge_refs") or 0)
    shared_context_policy = shared_context_policy or {"enabled": False, "resource_budget_tokens": 0, "skill_budget_tokens": 0, "min_score": 0.0}
    shared_enabled = bool(shared_context_policy.get("enabled"))
    shared_resource_budget_tokens = int(shared_context_policy.get("resource_budget_tokens") or 0)
    shared_skill_budget_tokens = int(shared_context_policy.get("skill_budget_tokens") or 0)
    shared_min_score = max(0.0, min(1.0, float(shared_context_policy.get("min_score") or 0.0)))
    cross_used_tokens = 0
    cross_selected_ref_count = 0
    entity_bridge_selected_ref_count = 0
    shared_resource_used_tokens = 0
    shared_skill_used_tokens = 0
    shared_resource_selected_ref_count = 0
    shared_skill_selected_ref_count = 0
    selected_cross_sessions: set[str] = set()
    def cross_session_key(candidate: Json) -> str:
        for source in [candidate, candidate.get("access_scope", {}), candidate.get("scope", {}), candidate.get("metadata", {}).get("access_scope", {}) if isinstance(candidate.get("metadata"), dict) else {}]:
            if isinstance(source, dict):
                for field in ["session_id", "scope_key", "node_hash"]:
                    value = source.get(field)
                    if value:
                        return str(value)
        return "unknown_cross_session"

    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "cross_session_budget": 0,
        "cross_session_session_cap": 0,
        "cross_session_candidate_cap": 0,
        "shared_resource_budget": 0,
        "shared_skill_budget": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "cross_session_budget": 0,
            "cross_session_session_cap": 0,
            "cross_session_candidate_cap": 0,
            "shared_resource_budget": 0,
            "shared_skill_budget": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "cross_session_budget": "cross-session candidate exceeded the configured cross-session token budget",
            "cross_session_session_cap": "cross-session candidate came from a session beyond max cross-session session fanout",
            "cross_session_candidate_cap": "cross-session candidate exceeded the configured cross-session candidate cap",
            "shared_resource_budget": "shared resource candidate exceeded the configured shared-resource token budget",
            "shared_skill_budget": "shared skill candidate exceeded the configured shared-skill token budget",
            "deadline": "candidate was not packed because the hard retrieval deadline was reached",
            "max_selected_refs": "candidate was relevant but dropped because max_selected_refs was reached",
        },
        "refs": [],
        "deadline_exceeded": False,
        "deadline_reason": "",
        "min_score": min_score,
        "budget_fill_policy": budget_fill_policy,
    }
    seen_text_hashes: set[int] = set()
    for index, candidate in enumerate(candidates):
        if len(selected) >= selected_ref_cap:
            remaining_candidates = candidates[index:]
            dropped["max_selected_refs"] += len(remaining_candidates)
            for skipped in remaining_candidates:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["max_selected_refs"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="max_selected_refs", token_estimate=skipped_tokens)
            break
        if deadline_exceeded is not None and deadline_exceeded():
            dropped["deadline_exceeded"] = True
            dropped["deadline_reason"] = deadline_reason
            remaining = max(0, len(candidates) - index)
            dropped["deadline"] += remaining
            for skipped in candidates[index:]:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["deadline"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="deadline", token_estimate=skipped_tokens)
            break
        ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
        candidate_text_hashes = context_text_hashes(str(candidate.get("text", "")))
        if candidate_text_hashes.intersection(duplicate_text_hashes):
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        text_hash = stable_hash(str(candidate.get("text", ""))[:512])
        if text_hash in seen_text_hashes:
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        if float(candidate.get("score", 0.0)) < min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if remote_budget <= 0 or (selected and used_tokens + ref_tokens > remote_budget):
            dropped["over_budget"] += 1
            dropped["estimated_tokens"]["over_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="over_budget", token_estimate=ref_tokens)
            continue
        is_cross_session = candidate.get("session_continuity") == "cross_session"
        ref_type = str(candidate.get("ref_type") or "")
        is_entity_bridge = is_cross_session and ref_type == "entity"
        is_cross_session_raw_evidence = is_cross_session and ref_type in {"event", "segment"}
        candidate_score = float(candidate.get("score", 0.0))
        candidate_cross_key = cross_session_key(candidate) if is_cross_session else ""
        if is_cross_session and not cross_enabled:
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_min_score > 0.0 and candidate_score < cross_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_cross_session_raw_evidence and cross_raw_evidence_min_score > 0.0 and candidate_score < cross_raw_evidence_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_candidates > 0 and cross_selected_ref_count >= cross_max_candidates:
            dropped["cross_session_candidate_cap"] += 1
            dropped["estimated_tokens"]["cross_session_candidate_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_candidate_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_sessions > 0 and candidate_cross_key not in selected_cross_sessions and len(selected_cross_sessions) >= cross_max_sessions:
            dropped["cross_session_session_cap"] += 1
            dropped["estimated_tokens"]["cross_session_session_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_session_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_budget_tokens > 0 and cross_used_tokens + ref_tokens > cross_budget_tokens and not (is_entity_bridge and entity_bridge_selected_ref_count < cross_min_entity_bridge_refs):
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        is_shared_resource = is_shared_resource_candidate(candidate)
        is_shared_skill = is_shared_skill_candidate(candidate)
        if (is_shared_resource or is_shared_skill) and not shared_enabled:
            reason = "shared_resource_budget" if is_shared_resource else "shared_skill_budget"
            dropped[reason] += 1
            dropped["estimated_tokens"][reason] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason=reason, token_estimate=ref_tokens)
            continue
        if (is_shared_resource or is_shared_skill) and shared_min_score > 0.0 and candidate_score < shared_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_shared_resource and shared_resource_budget_tokens > 0 and shared_resource_used_tokens + ref_tokens > shared_resource_budget_tokens:
            dropped["shared_resource_budget"] += 1
            dropped["estimated_tokens"]["shared_resource_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_resource_budget", token_estimate=ref_tokens)
            continue
        if is_shared_skill and shared_skill_budget_tokens > 0 and shared_skill_used_tokens + ref_tokens > shared_skill_budget_tokens:
            dropped["shared_skill_budget"] += 1
            dropped["estimated_tokens"]["shared_skill_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_skill_budget", token_estimate=ref_tokens)
            continue
        seen_text_hashes.add(text_hash)
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
            }
        )
        used_tokens += ref_tokens
        if is_cross_session:
            cross_used_tokens += ref_tokens
            cross_selected_ref_count += 1
            selected_cross_sessions.add(candidate_cross_key)
            if is_entity_bridge:
                entity_bridge_selected_ref_count += 1
        if is_shared_resource_candidate(candidate):
            shared_resource_used_tokens += ref_tokens
            shared_resource_selected_ref_count += 1
        if is_shared_skill_candidate(candidate):
            shared_skill_used_tokens += ref_tokens
            shared_skill_selected_ref_count += 1
        if used_tokens >= remote_budget:
            break
    dropped["cross_session_policy"] = {
        **cross_session_policy,
        "selected_tokens": cross_used_tokens,
        "selected_ref_count": cross_selected_ref_count,
        "selected_session_count": len(selected_cross_sessions),
        "entity_bridge_selected_ref_count": entity_bridge_selected_ref_count,
    }
    dropped["shared_context_policy"] = {
        **shared_context_policy,
        "resource_selected_tokens": shared_resource_used_tokens,
        "skill_selected_tokens": shared_skill_used_tokens,
        "resource_selected_ref_count": shared_resource_selected_ref_count,
        "skill_selected_ref_count": shared_skill_selected_ref_count,
    }
    if not selected and candidates and remote_budget > 0 and budget_fill_policy != "quality_first":
        first = next(
            (
                candidate
                for candidate in candidates
                if not context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes)
            ),
            None,
        )
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = tokens(str(first.get("text", "")))[:remote_budget]
        selected = [{**first, "text": " ".join(clipped_words), "token_estimate": len(clipped_words)}]
        used_tokens = len(clipped_words)
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped


def summary_provider() -> str:
    provider = os.environ.get("MATRIXARK_SUMMARY_PROVIDER", SUMMARY_LLM_PROVIDER).strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "local_llm", "oss_llm"}:
        return "openai_compatible"
    if provider in {"", "deterministic", "rules", "local"} and require_oss_understanding():
        raise MatrixArkError("deterministic L0/L1 summary generation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
    return provider or "deterministic"


def synthesize_context_node_summary(
    *,
    level: str,
    node_path: list[str],
    source_text: str,
    fallback_text: str,
    max_chars: int,
    policy: Json,
) -> tuple[str, Json]:
    provider = summary_provider()
    fallback_summary = summarize_text(fallback_text, limit=max_chars)
    provider_meta: Json = {
        "provider": provider,
        "model": SUMMARY_LLM_MODEL if provider in {"openai", "openai_compatible", "openai_compatible_llm"} else "",
        "fallback_used": False,
        "summary_level": level,
    }
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        system = (
            "You generate MatrixArk ContextNode traversal summaries. Return JSON only. "
            "The summary must be faithful to the supplied child summaries, entity state, operator state, and recent events. "
            "For node_l0 return a compact routing abstract. For node_l1 return a richer semantic synthesis for tree-first retrieval. "
            "Do not invent facts. Prefer current state and resolved contradictions."
        )
        user = json.dumps(
            {
                "summary_level": level,
                "node_path": node_path,
                "generation_policy": policy,
                "source_text": summarize_text(source_text, limit=5000),
                "required_json_shape": {"summary_text": "string"},
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        try:
            result = openai_compatible_json_call(
                system=system,
                user=user,
                model=SUMMARY_LLM_MODEL,
                max_tokens=SUMMARY_LLM_MAX_TOKENS,
            )
            summary_text = summarize_text(str(result.get("summary_text") or result.get("summary") or ""), limit=max_chars)
            if not summary_text:
                raise MatrixArkError("summary provider returned empty summary_text")
            provider_meta["execution_mode"] = "llm_json"
            return summary_text, provider_meta
        except MatrixArkError:
            if require_oss_understanding():
                raise
            provider_meta["fallback_used"] = True
            provider_meta["execution_mode"] = "deterministic_fallback"
            return fallback_summary, provider_meta
    if require_oss_understanding():
        raise MatrixArkError(f"unsupported OSS summary provider: {provider}")
    provider_meta["fallback_used"] = True
    provider_meta["execution_mode"] = "deterministic_fallback"
    return fallback_summary, provider_meta

def scope_matches(record_scope: Json, query_scope: Json) -> bool:
    if not query_scope:
        return True
    sharing_scope = str(record_scope.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    explicit_keys = set(query_scope.get("_explicit_scope_keys", []))
    record_scope_key = str(record_scope.get("scope_key") or "")
    if record_scope_key:
        if not scope_key_matches_query(record_scope_key, query_scope, explicit_keys):
            return False
        if set(record_scope.keys()).issubset({"scope_key"}):
            return True
    for key, value in query_scope.items():
        if str(key).startswith("_"):
            continue
        if key == "scope_key":
            continue
        if key == "agent_name" and key not in record_scope:
            continue
        if key in {"team", "project"} and key not in record_scope and record_scope_key:
            continue
        if key in {"agent_name", "team", "project"} and key not in explicit_keys:
            continue
        if key in {"account_id", "tenant_id", "account_hash", "tenant_hash"} and record_scope_key and key not in record_scope:
            continue
        if key in {"user_id", "user_hash"} and "user_id" not in explicit_keys:
            continue
        if key in {"session_id", "session_hash"}:
            if "session_id" not in explicit_keys or session_scope_mode(query_scope) == "prefer":
                continue
        if record_scope.get(key) != value:
            return False
    return True


def candidate_access_scope(record: Json) -> Json:
    access_scope = record.get("access_scope")
    if isinstance(access_scope, dict) and access_scope:
        return access_scope
    metadata = record.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("access_scope"), dict):
        return metadata["access_scope"]
    serving_scope = scope_from_serving_record(record)
    if serving_scope:
        return serving_scope
    envelope = record.get("envelope", {})
    if isinstance(envelope, dict):
        return envelope.get("scope", {})
    return {}


def access_scope_matches_before_scoring(record: Json, query_scope: Json) -> bool:
    """Gate candidate eligibility before semantic scoring."""
    record_scope = candidate_access_scope(record)
    sharing_scope = str(record_scope.get("sharing_scope") or record.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    return scope_matches(record_scope, query_scope)


def session_continuity_status(record_scope: Json, query_scope: Json) -> str:
    query_session = str(query_scope.get("session_id") or "")
    if not query_session:
        return "unscoped"
    record_session = str(record_scope.get("session_id") or "")
    if record_session == query_session:
        return "same_session"
    record_key = str(record_scope.get("scope_key") or "")
    query_session_hash = int(query_scope.get("session_hash") or 0)
    if record_key and query_session_hash and parse_scope_key(record_key).get("s") == query_session_hash:
        return "same_session"
    if record_session or record_key:
        return "cross_session"
    return "unscoped"


def session_continuity_boost(candidate: Json, question_type: str) -> float:
    status = str(candidate.get("session_continuity") or "")
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    if status == "same_session":
        if ref_type in {"event", "segment"}:
            return 0.16
        if ref_type == "summary":
            return 0.12
        if ref_type == "entity":
            return 0.10
        return 0.08
    if status == "cross_session":
        if ref_type == "entity" or context_class in {"resource_entity_fact", "resource_fact"}:
            return 0.11
        if question_type in {"multi_hop", "current_state"} and ref_type in {"event", "segment", "compression"}:
            return 0.06
    return 0.0


def cross_session_rerank_adjustment(candidate: Json, question_type: str) -> float:
    if str(candidate.get("session_continuity") or "") != "cross_session":
        return 0.0
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    if ref_type == "entity":
        return 0.10 if question_type in {"current_state", "latest", "multi_hop"} else 0.06
    if context_class in {"resource_fact", "resource_entity_fact"}:
        return 0.06 if has_citation else 0.04
    if ref_type == "resource_chunk" and has_citation:
        return 0.04
    if ref_type in {"event", "segment"} and question_type in {"multi_hop", "why_emotion", "fact", "evidence"}:
        return 0.01
    if ref_type == "compression":
        return 0.05
    if ref_type == "summary":
        return 0.05 if question_type == "broad_exploration" else 0.02
    return 0.0
