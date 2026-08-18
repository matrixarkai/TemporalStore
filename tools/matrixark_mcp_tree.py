#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Context tree path and traversal helpers for MatrixArk MCP."""

from __future__ import annotations

from typing import Any

Json = dict[str, Any]


# This module used to carry a byte-identical second copy of the node-tree helpers, including the
# traversal. Two copies of one algorithm drift -- the retrieval budget constants in
# matrixark_mcp_core vs matrixark_mcp_runtime_config already did exactly that (top-k 8 vs 24) -- so
# it re-exports the single implementation instead of repeating it.
try:  # package path
    from tools.matrixark_mcp_core_node_tree import (  # noqa: F401
        node_path_tuple,
        node_prefixes,
        normalized_node_path,
        starts_with_path,
        top_scored_nodes,
        tree_first_traversal,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_node_tree import (  # noqa: F401
        node_path_tuple,
        node_prefixes,
        normalized_node_path,
        starts_with_path,
        top_scored_nodes,
        tree_first_traversal,
    )
