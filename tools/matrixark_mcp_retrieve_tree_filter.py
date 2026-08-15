#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tree traversal filtering helpers for MatrixArk retrieval."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, node_path_tuple, starts_with_path
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, node_path_tuple, starts_with_path


def selected_by_tree(
    record: Json,
    *,
    traversal: Json,
    selected_paths: set[tuple[str, ...]],
    selected_leaf_paths: set[tuple[str, ...]],
    selected_node_hashes: set[int],
) -> bool:
    if traversal.get("fallback_to_flat"):
        return True
    path = node_path_tuple(record.get("node_path", []))
    if path and path in selected_paths:
        return True
    if path and any(
        starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
        for leaf_path in selected_leaf_paths
    ):
        return True
    try:
        return int(record.get("node_hash")) in selected_node_hashes
    except (TypeError, ValueError):
        return False


def make_tree_selector(
    *,
    traversal: Json,
    selected_paths: set[tuple[str, ...]],
    selected_leaf_paths: set[tuple[str, ...]],
    selected_node_hashes: set[int],
):
    def selector(record: Json) -> bool:
        return selected_by_tree(
            record,
            traversal=traversal,
            selected_paths=selected_paths,
            selected_leaf_paths=selected_leaf_paths,
            selected_node_hashes=selected_node_hashes,
        )

    return selector


class CandidateFanoutLimiter:
    def __init__(self, max_candidates_per_node: int) -> None:
        self.max_candidates_per_node = max_candidates_per_node
        self.candidate_count_by_node: dict[Any, int] = {}
        self.dropped_count = 0

    def admit(self, record: Json) -> bool:
        node_key: Any = record.get("node_hash")
        if node_key is None:
            node_key = tuple(record.get("node_path", []))
        count = self.candidate_count_by_node.get(node_key, 0)
        if count >= self.max_candidates_per_node:
            self.dropped_count += 1
            return False
        self.candidate_count_by_node[node_key] = count + 1
        return True


def make_candidate_admitter(max_candidates_per_node: int):
    limiter = CandidateFanoutLimiter(max_candidates_per_node)
    return limiter.admit, limiter
