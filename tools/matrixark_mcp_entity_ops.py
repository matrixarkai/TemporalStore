#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Entity patch and merge helpers for MatrixArk MCP extraction."""

from __future__ import annotations

import re
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_summaries import summarize_text
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summaries import summarize_text


def entity_patch(search: str, replace: str, *, field: str = "state") -> Json:
    return {
        "field": field,
        "patch": f"<< SEARCH\n{search}\n====\n{replace}\n>> REPLACE",
    }


def parse_entity_patch(patch_text: str) -> tuple[str, str] | None:
    match = re.search(
        r"<<\s*SEARCH\s*\n(?P<search>.*?)\n====\s*\n(?P<replace>.*?)\n>>\s*REPLACE",
        patch_text,
        flags=re.DOTALL,
    )
    if not match:
        return None
    return match.group("search").strip(), match.group("replace").strip()


def edit_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        for j, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            current.append(
                min(
                    current[j - 1] + 1,
                    previous[j] + 1,
                    previous[j - 1] + cost,
                )
            )
        previous = current
    return previous[-1]


def best_span_by_edit_distance(text: str, search: str) -> tuple[int, int, float]:
    if not text or not search:
        return -1, -1, 1.0
    lower_text = text.lower()
    lower_search = search.lower()
    exact = lower_text.find(lower_search)
    if exact >= 0:
        return exact, exact + len(search), 0.0
    search_len = len(search)
    best = (-1, -1, 1.0)
    min_len = max(1, int(search_len * 0.7))
    max_len = min(len(text), max(search_len + 8, int(search_len * 1.3)))
    step = max(1, search_len // 8)
    for start in range(0, len(text), step):
        for span_len in range(min_len, max_len + 1, step):
            end = min(len(text), start + span_len)
            if end <= start:
                continue
            candidate = text[start:end]
            distance = edit_distance(candidate.lower(), lower_search)
            ratio = distance / max(len(candidate), len(search), 1)
            if ratio < best[2]:
                best = (start, end, ratio)
    return best


def apply_entity_patch(old_value: str, patch_text: str, *, max_distance_ratio: float = 0.45) -> Json:
    parsed = parse_entity_patch(patch_text)
    if not parsed:
        return {
            "updated": old_value,
            "applied": False,
            "reason": "invalid_patch",
            "distance_ratio": 1.0,
        }
    search, replace = parsed
    if not old_value:
        return {
            "updated": replace,
            "applied": True,
            "reason": "empty_old_value",
            "distance_ratio": 0.0,
        }
    start, end, ratio = best_span_by_edit_distance(old_value, search)
    if start < 0 or ratio > max_distance_ratio:
        return {
            "updated": replace,
            "applied": False,
            "reason": "no_close_span",
            "distance_ratio": round(ratio, 6),
        }
    return {
        "updated": old_value[:start] + replace + old_value[end:],
        "applied": True,
        "reason": "approximate_patch",
        "distance_ratio": round(ratio, 6),
        "span": [start, end],
    }


def apply_entity_patches(old_entity: Json | None, extracted_entity: Json) -> Json:
    state = str((old_entity or {}).get("state", ""))
    patches = extracted_entity.get("field_patches", [])
    patch_results = []
    updated_state = state or str(extracted_entity.get("state", ""))
    for patch in patches:
        if not isinstance(patch, dict) or patch.get("field", "state") != "state":
            continue
        result = apply_entity_patch(updated_state, str(patch.get("patch", "")))
        updated_state = str(result.get("updated", updated_state))
        patch_results.append(result)
    if not patches and not state:
        updated_state = str(extracted_entity.get("state", ""))
    elif not patches and state:
        new_state = str(extracted_entity.get("state", ""))
        if new_state and new_state.lower() not in state.lower():
            updated_state = summarize_text(state + " " + new_state, limit=320)
    return {
        **extracted_entity,
        "state": summarize_text(updated_state, limit=320),
        "previous_state": state,
        "patch_results": patch_results,
        "update_mode": "deterministic_eua" if patches else "merge_without_patch",
    }
