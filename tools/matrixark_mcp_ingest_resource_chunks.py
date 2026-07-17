#!/usr/bin/env python3
"""Resource chunk normalization helpers for MatrixArk local ingest."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, aggregate_parse_warnings_from_chunks, content_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, aggregate_parse_warnings_from_chunks, content_hash


def normalize_resource_chunks(parsed_chunks: list[Any]) -> Json:
    original_chunk_count = len(parsed_chunks)
    deduped_source_refs: list[str] = []
    seen_content_hashes: set[str] = set()
    unique_chunks: list[Any] = []
    for chunk in parsed_chunks:
        chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
        if chunk_content_hash in seen_content_hashes:
            deduped_source_refs.append(chunk.source_ref)
            continue
        seen_content_hashes.add(chunk_content_hash)
        unique_chunks.append(chunk)
    resource_version_value = str(unique_chunks[0].metadata.get("resource_version") or "") if unique_chunks else ""
    resource_content_hash = content_hash(
        "\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in unique_chunks)
    )
    superseded_chunk_hashes = [
        int(chunk.metadata["supersedes_chunk_hash"])
        for chunk in unique_chunks
        if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
    ]
    return {
        "chunks": unique_chunks,
        "original_chunk_count": original_chunk_count,
        "deduped_chunk_count": original_chunk_count - len(unique_chunks),
        "deduped_source_refs": deduped_source_refs,
        "resource_version": resource_version_value,
        "resource_content_hash": resource_content_hash,
        "superseded_chunk_count": len(superseded_chunk_hashes),
        "superseded_chunk_hashes": superseded_chunk_hashes,
        "parse_warnings": aggregate_parse_warnings_from_chunks(unique_chunks),
    }
