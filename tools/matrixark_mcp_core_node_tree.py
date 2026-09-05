# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
from typing import Any

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        Json,
        estimated_context_tokens,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        Json,
        estimated_context_tokens,
    )

__all__ = ['node_l1_generation_policy', 'normalized_node_path', 'node_prefixes', 'node_path_tuple', 'starts_with_path', 'top_scored_nodes', 'tree_first_traversal']


def node_l1_generation_policy(
    *,
    source_text: str,
    event_count: int,
    child_summary_count: int,
) -> Json:
    """Decide when a node needs a richer L1 overview.

    L0 is mandatory for traversal. L1 is useful once a node has enough local
    content or child summaries that a short abstract would lose routing detail.
    """
    token_estimate = estimated_context_tokens(source_text)
    base = {
        "token_estimate": token_estimate,
        "event_count": event_count,
        "child_summary_count": child_summary_count,
    }
    if child_summary_count > 0:
        return {**base, "generate_l1": True, "reason": "has_child_summaries"}
    if event_count >= 3:
        return {**base, "generate_l1": True, "reason": "event_count_threshold"}
    if token_estimate >= 180:
        return {**base, "generate_l1": True, "reason": "token_threshold"}
    return {**base, "generate_l1": False, "reason": "l0_sufficient"}


def normalized_node_path(envelope: Json, node_hint: list[Any]) -> list[str]:
    return [str(part) for part in node_hint if str(part)]


def node_prefixes(node_path: list[str]) -> list[list[str]]:
    return [node_path[: index + 1] for index in range(len(node_path))]


def node_path_tuple(node_path: Any) -> tuple[str, ...]:
    if not isinstance(node_path, list):
        return ()
    return tuple(str(part) for part in node_path if str(part))


def starts_with_path(path: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return len(path) >= len(prefix) and path[: len(prefix)] == prefix


def top_scored_nodes(nodes: list[Json], limit: int) -> list[Json]:
    return sorted(
        nodes,
        key=lambda item: (-float(item.get("score", 0.0)), int(item.get("depth", 0)), str(item.get("node_path", []))),
    )[:limit]


def tree_first_traversal(
    node_scores: dict[int, Json],
    *,
    top_k_per_layer: int,
    max_children_scored_per_parent: int,
    guaranteed_path=None,
) -> Json:
    """Traverse ContextNode summaries layer by layer and return selected subtrees.

    The current Python runtime infers ContextNode children from node_path prefixes.
    can later replace this with native ContextChildRef/list-children APIs while
    preserving the retrieval contract.
    """
    node_by_path: dict[tuple[str, ...], Json] = {}
    children_by_parent: dict[tuple[str, ...], list[Json]] = {}
    for node in node_scores.values():
        path = node_path_tuple(node.get("node_path", []))
        if not path:
            continue
        current = node_by_path.get(path)
        if current is None or float(node.get("score", 0.0)) > float(current.get("score", 0.0)):
            node_by_path[path] = node
    for path, node in node_by_path.items():
        parent = path[:-1]
        children_by_parent.setdefault(parent, []).append(node)

    guaranteed = node_path_tuple(list(guaranteed_path)) if guaranteed_path else ()
    if guaranteed and guaranteed not in node_by_path:
        # A path this store does not have must change nothing at all.
        guaranteed = ()

    def admit_guaranteed(parent: tuple[str, ...], picked: list[Json]) -> list[Json]:
        """Add the guaranteed path's child under this parent without displacing a ranked pick.

        top_k_per_layer is applied per parent, so every session under a user competes for the same
        places, and the current session -- newest and least summarised -- is the likeliest to lose
        to its own history. Losing it means asking about something said minutes ago and being
        handed a much older session instead. So it is admitted alongside the ranking rather than
        competing in it, and it is looked up directly rather than through the
        max_children_scored_per_parent slice, so the guarantee does not depend on the scoring cap.
        """
        if not guaranteed or len(guaranteed) <= len(parent) or guaranteed[:len(parent)] != parent:
            return picked
        wanted = guaranteed[:len(parent) + 1]
        if any(node_path_tuple(entry.get("node_path", [])) == wanted for entry in picked):
            return picked
        node = node_by_path.get(wanted)
        return picked + [node] if node is not None else picked

    roots = children_by_parent.get((), [])
    if not roots:
        return {
            "selected_node_hashes": set(),
            "selected_paths": set(),
            "leaf_paths": set(),
            "trace": [],
            "fallback_to_flat": True,
        }

    frontier = admit_guaranteed(
        (), top_scored_nodes(roots[:max_children_scored_per_parent], top_k_per_layer))
    selected_paths: set[tuple[str, ...]] = set()
    selected_node_hashes: set[int] = set()
    leaf_paths: set[tuple[str, ...]] = set()
    trace: list[Json] = []

    while frontier:
        next_frontier: list[Json] = []
        for node in frontier:
            path = node_path_tuple(node.get("node_path", []))
            if not path:
                continue
            selected_paths.add(path)
            try:
                selected_node_hashes.add(int(node.get("node_hash")))
            except (TypeError, ValueError):
                pass
            children = children_by_parent.get(path, [])[:max_children_scored_per_parent]
            picked_children = admit_guaranteed(
                path, top_scored_nodes(children, top_k_per_layer) if children else [])
            trace.append(
                {
                    "node_hash": node.get("node_hash"),
                    "node_path": list(path),
                    "depth": node.get("depth", len(path)),
                    "score": node.get("score", 0.0),
                    "dense_score": node.get("dense_score", 0.0),
                    "sparse_score": node.get("sparse_score", 0.0),
                    "children_scored": len(children),
                    "children_selected": len(picked_children),
                    "selected": True,
                }
            )
            if picked_children:
                next_frontier.extend(picked_children)
            else:
                leaf_paths.add(path)
        frontier = next_frontier

    if not leaf_paths:
        leaf_paths = set(selected_paths)
    return {
        "selected_node_hashes": selected_node_hashes,
        "selected_paths": selected_paths,
        "leaf_paths": leaf_paths,
        "trace": trace,
        "fallback_to_flat": False,
    }


