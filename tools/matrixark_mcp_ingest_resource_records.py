#!/usr/bin/env python3
"""Resource and skill record builders for MatrixArk local ingestion."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, clip_context_text, embedding_model_name, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, clip_context_text, embedding_model_name, stable_hash


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


def skill_section_record(
    *,
    import_task_hash: int,
    skill_hash: int,
    section_hash: int,
    node_hash: int,
    node_path: list[str],
    raw_uri_hash: int,
    source_locator: str,
    heading: str,
    text: str,
    token_estimate: int,
    metadata: Json,
    access_scope: Json,
    deployment_scope: str,
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "skill_section",
        "import_task_hash": import_task_hash,
        "skill_hash": skill_hash,
        "section_hash": section_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "resource_hash": skill_hash,
        "raw_uri_hash": raw_uri_hash,
        "source_locator": source_locator,
        "heading": heading,
        "text": text,
        "token_estimate": token_estimate,
        "metadata": metadata,
        "access_scope": access_scope,
        "deployment_scope": deployment_scope,
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
        "vector": vector,
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
