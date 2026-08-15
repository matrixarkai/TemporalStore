#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Index-term collection helpers for MatrixArk retrieval."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, context_index_ref_hashes
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, context_index_ref_hashes


def add_context_index_terms(
    record: Json,
    *,
    index_terms_by_batch: dict[Any, list[str]],
    index_terms_by_node: dict[Any, list[str]],
    index_terms_by_ref: dict[Any, list[str]],
    index_terms_by_node_for_prefilter: dict[int, list[str]],
) -> bool:
    if record.get("record_type") != "context_index":
        return False
    index_name = str(record.get("index_name", ""))
    if not index_name:
        return False
    ref_hashes = context_index_ref_hashes(record)
    if record.get("batch_id_hash") is not None:
        index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
    node_hashes_for_index: list[Any] = []
    if isinstance(record.get("node_hashes"), list):
        node_hashes_for_index.extend(record.get("node_hashes", []))
    if record.get("node_hash") is not None:
        node_hashes_for_index.append(record.get("node_hash"))
    seen_node_hashes: set[int] = set()
    for node_hash_for_index in node_hashes_for_index:
        try:
            node_hash_int = int(node_hash_for_index)
        except (TypeError, ValueError):
            continue
        if node_hash_int in seen_node_hashes:
            continue
        seen_node_hashes.add(node_hash_int)
        index_terms_by_node_for_prefilter.setdefault(node_hash_int, []).append(index_name)
    if ref_hashes:
        for ref_hash in ref_hashes:
            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
        return True
    ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
    if ref_hash is not None:
        index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
    else:
        index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
    return True
