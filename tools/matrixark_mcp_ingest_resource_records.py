#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource record builders for MatrixArk local ingestion."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, clip_context_text, compact_embedding_vector, embedding_model_name, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, clip_context_text, compact_embedding_vector, embedding_model_name, stable_hash

try:
    from tools.matrixark_mcp_ingest_skill_records import (
        skill_manifest_record,
        skill_parse_debug_record,
        skill_registry_record,
        skill_section_record,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_ingest_skill_records import (
        skill_manifest_record,
        skill_parse_debug_record,
        skill_registry_record,
        skill_section_record,
    )


def resource_l0_summary_record(
    *,
    resource_kind: str,
    summary_hash: int,
    import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    raw_uri: str,
    summary_text: str,
    source_chunk_hashes: list[int],
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "context_summary",
        "summary_type": f"{resource_kind}_l0",
        "summary_hash": summary_hash,
        "import_task_hash": import_task_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "raw_uri": raw_uri,
        "summary_text": summary_text,
        "source_chunk_hashes": source_chunk_hashes,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }



def resource_chunk_record(
    *,
    import_task_hash: int,
    chunk_hash: int,
    node_hash: int,
    node_path: list[str],
    resource_hash: int,
    raw_uri_hash: int,
    resource_type: str,
    source_locator: str,
    text: str,
    token_estimate: int,
    metadata: Json,
    access_scope: Json,
    deployment_scope: str,
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "resource_chunk",
        "import_task_hash": import_task_hash,
        "chunk_hash": chunk_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "resource_hash": resource_hash,
        "raw_uri_hash": raw_uri_hash,
        "resource_type": resource_type,
        "source_locator": source_locator,
        "text": text,
        "token_estimate": token_estimate,
        "metadata": metadata,
        "access_scope": access_scope,
        "deployment_scope": deployment_scope,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def resource_manifest_record(
    *,
    resource_hash: int,
    import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    raw_uri: str,
    requested_raw_uri: str,
    resource_type: str,
    resource_version: str,
    content_hash: str,
    raw_storage_mode: str,
    raw_storage_policy: str,
    storage_resolution: Json,
    parse_warnings: list[str],
    chunk_count: int,
    original_chunk_count: int,
    deduped_chunk_count: int,
    deduped_source_refs: list[str],
    superseded_chunk_count: int,
    superseded_chunk_hashes: list[int],
    summary_dirty_hashes: list[int],
    access_scope: Json,
    deployment_scope: str,
    token_estimate: int,
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "resource_manifest",
        "resource_hash": resource_hash,
        "import_task_hash": import_task_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "raw_uri": raw_uri,
        "requested_raw_uri": requested_raw_uri,
        "resource_type": resource_type,
        "resource_version": resource_version,
        "content_hash": content_hash,
        "raw_storage_mode": raw_storage_mode,
        "raw_storage_policy": raw_storage_policy,
        "raw_bytes_stored": False,
        "upload_status": storage_resolution.get("upload_status", "not_required"),
        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
        "cloud_key": storage_resolution.get("cloud_key", ""),
        "parse_warnings": parse_warnings[:100],
        "parse_warning_count": len(parse_warnings),
        "chunk_count": chunk_count,
        "original_chunk_count": original_chunk_count,
        "deduped_chunk_count": deduped_chunk_count,
        "deduped_source_refs": deduped_source_refs[:50],
        "superseded_chunk_count": superseded_chunk_count,
        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
        "summary_dirty_hashes": summary_dirty_hashes,
        "async_parent_summary_required": bool(summary_dirty_hashes),
        "access_scope": access_scope,
        "deployment_scope": deployment_scope,
        "token_estimate": token_estimate,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def resource_registry_record(
    *,
    resource_hash: int,
    import_task_hash: int,
    raw_uri: str,
    requested_raw_uri: str,
    resource_type: str,
    resource_version: str,
    content_hash: str,
    chunk_count: int,
    superseded_chunk_hashes: list[int],
    raw_storage_mode: str,
    raw_storage_policy: str,
    storage_resolution: Json,
    access_scope: Json,
    deployment_scope: str,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "resource_registry",
        "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version}:{deployment_scope}"),
        "resource_hash": resource_hash,
        "import_task_hash": import_task_hash,
        "raw_uri": raw_uri,
        "requested_raw_uri": requested_raw_uri,
        "resource_type": resource_type,
        "resource_version": resource_version,
        "content_hash": content_hash,
        "chunk_count": chunk_count,
        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
        "raw_storage_mode": raw_storage_mode,
        "raw_storage_policy": raw_storage_policy,
        "upload_status": storage_resolution.get("upload_status", "not_required"),
        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
        "cloud_key": storage_resolution.get("cloud_key", ""),
        "access_scope": access_scope,
        "deployment_scope": deployment_scope,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def resource_chunk_debug_record(
    *,
    ref_type: str,
    chunk_hash: int,
    import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    resource_hash: int,
    raw_uri_hash: int,
    raw_uri: str,
    source_locator: str,
    source_ref: str,
    metadata_debug: Json,
    text: str,
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "context_debug_record",
        "debug_type": "resource_chunk_parse_detail",
        "ref_type": ref_type,
        "ref_hash": chunk_hash,
        "chunk_hash": chunk_hash,
        "import_task_hash": import_task_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "resource_hash": resource_hash,
        "raw_uri_hash": raw_uri_hash,
        "raw_uri": raw_uri,
        "source_locator": source_locator,
        "source_ref": source_ref,
        "metadata_debug": metadata_debug,
        "text_preview": clip_context_text(text),
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def context_embedding_record(
    *,
    embedding_type: str,
    ref_type: str,
    ref_hash: int,
    node_hash: int,
    node_path: list[str],
    vector: list[float],
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "context_embedding",
        "embedding_type": embedding_type,
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "dim": len(vector),
        "model": embedding_model_name(),
        "vector": compact_embedding_vector(vector),
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def resource_chunk_index_record(
    *,
    index_name: str,
    ref_type: str,
    chunk_hash: int,
    resource_hash: int,
    source_locator: str,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "context_index",
        "index_name": index_name,
        "index_hash": stable_hash(f"{index_name}:{chunk_hash}"),
        "ref_type": ref_type,
        "ref_hash": chunk_hash,
        "chunk_hash": chunk_hash,
        "resource_hash": resource_hash,
        "source_locator": source_locator,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }
