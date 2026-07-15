#!/usr/bin/env python3
"""MatrixArk secondary-index term and posting budget helpers."""

from __future__ import annotations

import os
import re
from typing import Any


Json = dict[str, Any]


MAX_SECONDARY_INDEX_TERMS_PER_RECORD = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD", "10"))
SECONDARY_INDEX_POSTING_BUCKET_MS = int(os.environ.get("MATRIXARK_SECONDARY_INDEX_POSTING_BUCKET_MS", "60000"))
MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK", "6"))
MAX_INDEX_TERMS_PER_RESOURCE_CHUNK = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
MAX_INDEX_TERMS_PER_RESOURCE_FACT = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_FACT", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION", "128"))
MAX_SECONDARY_INDEX_REFS_PER_POSTING = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_REFS_PER_POSTING", "512"))
SECONDARY_INDEX_TIME_BUCKET_MS = int(os.environ.get("MATRIXARK_SECONDARY_INDEX_TIME_BUCKET_MS", "60000"))

SECONDARY_INDEX_PRIORITY_PREFIXES = (
    "source_type:",
    "resource_type:",
    "unit_kind:",
    "entity_type:",
    "event_type:",
    "classification:",
    "status:",
    "skill_name:",
    "skill_trigger:",
    "skill_tool:",
    "relative_path:",
    "heading_slug:",
    "segment_topic:",
    "keyword:",
)


def _ordered_unique(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def normalized_index_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_.:/-]+", "_", text)
    return text.strip("_")


def context_index_name(kind: str, value: Any) -> str:
    normalized = normalized_index_value(value)
    return f"{kind}:{normalized}" if normalized else ""


def non_default_classification(value: Any) -> str:
    classification = str(value or "").strip().upper()
    return "" if classification in {"", "NEW_EVENT"} else classification


def metadata_index_terms(metadata: Json, *, keyword_limit: int = MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK) -> list[str]:
    terms: list[str] = []
    for field in ["unit_kind", "heading_slug", "relative_path"]:
        terms.append(context_index_name(field, metadata.get(field)))
    for keyword in metadata.get("keywords", [])[: max(0, keyword_limit)]:
        terms.append(context_index_name("keyword", keyword))
    return _ordered_unique(terms)


def secondary_index_priority(term: str) -> int:
    for index, prefix in enumerate(SECONDARY_INDEX_PRIORITY_PREFIXES):
        if term.startswith(prefix):
            return index
    return len(SECONDARY_INDEX_PRIORITY_PREFIXES)


def limited_index_terms(terms: list[str], *, limit: int) -> list[str]:
    unique_terms = _ordered_unique([term for term in terms if term])
    capped_limit = max(0, int(limit))
    return [
        term
        for _, term in sorted(
            enumerate(unique_terms),
            key=lambda item: (secondary_index_priority(item[1]), item[0]),
        )
    ][:capped_limit]


def new_secondary_index_budget(limit: int | None = None) -> Json:
    configured_limit = MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION if limit is None else int(limit)
    return {"limit": max(0, configured_limit), "emitted": 0, "dropped": 0}


def take_secondary_index_terms(terms: list[str], budget: Json) -> list[str]:
    unique_terms = _ordered_unique([term for term in terms if term])
    limit = max(0, int(budget.get("limit", 0)))
    emitted = max(0, int(budget.get("emitted", 0)))
    remaining = max(0, limit - emitted)
    selected = unique_terms[:remaining]
    budget["emitted"] = emitted + len(selected)
    budget["dropped"] = max(0, int(budget.get("dropped", 0))) + max(0, len(unique_terms) - len(selected))
    return selected


def secondary_index_budget_summary(budget: Json) -> Json:
    return {
        "index_total_cap": max(0, int(budget.get("limit", 0))),
        "index_emitted_count": max(0, int(budget.get("emitted", 0))),
        "index_dropped_by_total_cap_count": max(0, int(budget.get("dropped", 0))),
    }
