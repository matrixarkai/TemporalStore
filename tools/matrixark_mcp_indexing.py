#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk secondary-index term and posting budget helpers."""

from __future__ import annotations

import os
import re
import json
from typing import Any


Json = dict[str, Any]

try:
    from tools.matrixark_mcp_identity import now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import now_ms, stable_hash


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
    "memory_scope:",
    "session_continuity:",
    "extraction_phase:",
    "memory_selection_policy:",
    "memory_selection_quality:",
    "profile_promotion_policy:",
    "benchmark:",
    "metric:",
    "workload:",
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



def _joined_index_text(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (dict, list, tuple, set)):
            try:
                parts.append(json.dumps(value, sort_keys=True))
            except TypeError:
                parts.append(str(value))
        else:
            parts.append(str(value))
    return " ".join(parts)


def benchmark_quality_index_terms(*values: Any) -> list[str]:
    text = _joined_index_text(*values).lower()
    if not text:
        return []
    terms: list[str] = []
    for benchmark in ["locomo", "longmemeval"]:
        if re.search(rf"\b{benchmark}\b", text):
            terms.append(context_index_name("benchmark", benchmark))
    metric_patterns = {
        "p50_latency": r"\bp50(?:\s+latency)?\b",
        "p90_latency": r"\bp90(?:\s+latency)?\b",
        "p95_latency": r"\bp95(?:\s+latency)?\b",
        "p99_latency": r"\bp99(?:\s+latency)?\b",
        "throughput": r"\b(throughput|qps|ops/s|req/s|requests/s|ops per second)\b",
        "hit_rate": r"\b(hit[- ]?rate|read[- ]?hit)\b",
        "recall": r"\brecall\b",
        "precision": r"\bprecision\b",
    }
    for metric, pattern in metric_patterns.items():
        if re.search(pattern, text):
            terms.append(context_index_name("metric", metric))
    workload_match = re.search(
        r"\bworkload\s*[:=]\s*([a-z0-9_.:/ -]{3,80}?)(?=\s+(?:p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|recall|precision)\b|[.;,\n]|$)",
        text,
    )
    if workload_match:
        terms.append(context_index_name("workload", workload_match.group(1)))
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


def context_index_timestamp_key(record: Json) -> int:
    for field in ("timestamp_key_ms", "updated_at_ms", "created_at_ms", "event_time_ms"):
        try:
            value = int(record.get(field) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return now_ms()


def context_index_posting_bucket(timestamp_ms: int) -> int:
    bucket_ms = max(1, int(SECONDARY_INDEX_POSTING_BUCKET_MS))
    return int(timestamp_ms) - (int(timestamp_ms) % bucket_ms)


def context_index_capability(record: Json) -> str:
    explicit = str(record.get("capability") or "").strip()
    if explicit:
        return explicit
    ref_type = str(record.get("ref_type") or "").strip()
    if ref_type:
        return ref_type
    if record.get("batch_id_hash") is not None:
        return "context_batch_commit"
    if record.get("summary_hash") is not None:
        return "context_summary"
    if record.get("chunk_hash") is not None:
        return "resource_chunk"
    if record.get("skill_hash") is not None or record.get("section_hash") is not None:
        return "skill"
    return "context"


def context_index_ref_hashes(record: Json) -> list[int]:
    values: list[Any] = []
    raw_refs = record.get("ref_hashes")
    if isinstance(raw_refs, list):
        values.extend(raw_refs)
    for field in (
        "ref_hash",
        "event_id_hash",
        "chunk_hash",
        "section_hash",
        "skill_hash",
        "resource_hash",
        "summary_hash",
        "batch_id_hash",
    ):
        if record.get(field) is not None:
            values.append(record.get(field))
    refs: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            ref_hash = int(value)
        except (TypeError, ValueError):
            continue
        if ref_hash and ref_hash not in seen:
            seen.add(ref_hash)
            refs.append(ref_hash)
    return refs


def context_index_posting_record(
    *,
    index_name: str,
    data_model: str | None = None,
    capability: str | None = None,
    ref_type: str | None = None,
    ref_hashes: list[Any] | None = None,
    batch_id_hash: Any = None,
    node_hash: Any = None,
    scope: Json | None = None,
    updated_at_ms: Any = None,
    source_ref: str | None = None,
    storage_options: Json | None = None,
) -> Json:
    """Build a compact TemporalStore-style secondary-index posting row."""
    indexed_capability = str(capability or "").strip()
    if not indexed_capability:
        indexed_capability = "context"
    refs: list[Any] = []
    seen_refs: set[str] = set()
    for ref in ref_hashes or []:
        if ref is None:
            continue
        key = str(ref)
        if key in seen_refs:
            continue
        seen_refs.add(key)
        refs.append(ref)
    timestamp_key_ms = int(updated_at_ms or now_ms())
    identity = {
        "index_name": index_name,
        "data_model": str(data_model or ""),
        "capability": indexed_capability,
        "timestamp_key_ms": timestamp_key_ms,
        "batch_id_hash": batch_id_hash,
        "node_hash": node_hash,
        "ref_hashes": refs,
    }
    record: Json = {
        "record_type": "context_index",
        "index_name": index_name,
        "index_hash": stable_hash(json.dumps(identity, sort_keys=True, separators=(",", ":"))),
        "capability": indexed_capability,
        "timestamp_key_ms": timestamp_key_ms,
        "ref_hashes": refs,
        "updated_at_ms": timestamp_key_ms,
    }
    if data_model:
        record["data_model"] = str(data_model)
    if len(refs) == 1:
        record["ref_hash"] = refs[0]
    if ref_type:
        record["ref_type"] = ref_type
    if batch_id_hash is not None:
        record["batch_id_hash"] = batch_id_hash
    if node_hash is not None:
        record["node_hash"] = node_hash
    if scope:
        record["scope"] = scope
    if source_ref:
        record["source_ref"] = source_ref
    if storage_options:
        record["storage_options"] = storage_options
    return record


def context_index_record_ref_hashes(record: Json) -> list[Any]:
    refs = record.get("ref_hashes")
    if isinstance(refs, list):
        return [ref for ref in refs if ref is not None]
    legacy = record.get("ref_hash")
    return [legacy] if legacy is not None else []


def context_index_record_node_hashes(record: Json) -> list[Any]:
    node_hashes = record.get("node_hashes")
    if isinstance(node_hashes, list) and node_hashes:
        return [node_hash for node_hash in node_hashes if node_hash is not None]
    node_hash = record.get("node_hash")
    return [node_hash] if node_hash is not None else []


def ordered_unique_any(values: list[Any]) -> list[Any]:
    output: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def context_index_time_bucket(timestamp_ms: Any) -> int:
    try:
        timestamp = int(timestamp_ms)
    except (TypeError, ValueError):
        timestamp = now_ms()
    bucket_ms = max(1, int(SECONDARY_INDEX_TIME_BUCKET_MS))
    return (timestamp // bucket_ms) * bucket_ms


def _chunked_refs(refs: list[Any], *, limit: int) -> list[list[Any]]:
    cap = max(1, int(limit))
    return [refs[index:index + cap] for index in range(0, len(refs), cap)] or [[]]


def compact_context_index_postings(records: list[Json]) -> list[Json]:
    """Group ContextIndex writes into Feature-style timestamped posting rows."""
    scalar_lineage_fields = [
        "memory_scope",
        "session_continuity",
        "profile_memory_class",
        "profile_memory_kind",
        "profile_entity_current",
        "profile_revision",
        "promoted_from_memory_scope",
        "extraction_phase",
        "final_session_boundary",
    ]
    list_lineage_fields = [
        "source_session_ids",
        "source_entity_hashes",
        "source_memory_scopes",
        "source_session_continuities",
        "source_profile_memory_classes",
        "source_profile_memory_kinds",
    ]
    grouped: dict[tuple[Any, ...], Json] = {}
    grouped_scalar_values: dict[tuple[Any, ...], dict[str, set[str]]] = {}
    grouped_list_values: dict[tuple[Any, ...], dict[str, list[Any]]] = {}
    passthrough: list[Json] = []
    order: list[tuple[Any, ...]] = []
    for record in records:
        if str(record.get("record_type") or "") != "context_index":
            passthrough.append(record)
            continue
        index_name = str(record.get("index_name") or "")
        capability = context_index_capability(record)
        data_model = str(record.get("data_model") or "")
        if not index_name or not capability:
            passthrough.append(record)
            continue
        bucket_ms = context_index_time_bucket(record.get("timestamp_key_ms") or record.get("updated_at_ms"))
        key = (
            str(record.get("scope_key") or ""),
            capability,
            data_model,
            index_name,
            str(record.get("ref_type") or ""),
            bucket_ms,
        )
        if key not in grouped:
            grouped[key] = {
                "record_type": "context_index",
                "index_name": index_name,
                "capability": capability,
                "timestamp_key_ms": bucket_ms,
                "updated_at_ms": bucket_ms,
                "ref_hashes": [],
                "node_hashes": [],
                "batch_id_hashes": [],
                "posting_count": 0,
                "posting_policy": "bucketed_by_scope_capability_index_time",
            }
            for field in ("data_model", "scope_key", "ref_type", "storage_options", "storage_record_kind", "storage_part", "storage_route", "placement_key", "placement_hash"):
                value = record.get(field)
                if value not in (None, "", [], {}):
                    grouped[key][field] = value
            grouped_scalar_values[key] = {field: set() for field in scalar_lineage_fields}
            grouped_list_values[key] = {field: [] for field in list_lineage_fields}
            order.append(key)
        posting = grouped[key]
        for field in scalar_lineage_fields:
            value = record.get(field)
            if value not in (None, "", [], {}):
                grouped_scalar_values[key][field].add(str(value))
        for field in list_lineage_fields:
            values = record.get(field)
            if not isinstance(values, list):
                value = record.get(field)
                values = [value] if value not in (None, "", [], {}) else []
            for value in values:
                if value not in (None, "", [], {}) and str(value) not in {str(item) for item in grouped_list_values[key][field]}:
                    grouped_list_values[key][field].append(value)
        node_hash = record.get("node_hash")
        if node_hash is not None and str(node_hash) not in {str(item) for item in posting.get("node_hashes", [])}:
            posting["node_hashes"].append(node_hash)
        batch_id_hash = record.get("batch_id_hash")
        if batch_id_hash is not None and str(batch_id_hash) not in {str(item) for item in posting.get("batch_id_hashes", [])}:
            posting["batch_id_hashes"].append(batch_id_hash)
        existing = {str(ref) for ref in posting.get("ref_hashes", [])}
        for ref in context_index_record_ref_hashes(record):
            if ref is None or str(ref) in existing:
                continue
            posting["ref_hashes"].append(ref)
            existing.add(str(ref))
        if record.get("source_ref") and not posting.get("sample_source_ref"):
            posting["sample_source_ref"] = record.get("source_ref")
        try:
            posting["posting_count"] += max(1, int(record.get("posting_count") or len(context_index_record_ref_hashes(record)) or 1))
        except (TypeError, ValueError):
            posting["posting_count"] += 1
    compacted_indexes: list[Json] = []
    for key in order:
        base = grouped[key]
        for field, values in grouped_scalar_values.get(key, {}).items():
            if len(values) == 1:
                raw_value = next(iter(values))
                if raw_value == "True":
                    base[field] = True
                elif raw_value == "False":
                    base[field] = False
                elif field == "profile_revision":
                    try:
                        base[field] = int(raw_value)
                    except (TypeError, ValueError):
                        base[field] = raw_value
                else:
                    base[field] = raw_value
        for field, values in grouped_list_values.get(key, {}).items():
            if values:
                base[field] = values
        refs = []
        seen_ref_keys: set[str] = set()
        for ref in base.get("ref_hashes", []):
            ref_key = str(ref)
            if ref_key in seen_ref_keys:
                continue
            seen_ref_keys.add(ref_key)
            refs.append(ref)
        for part, ref_chunk in enumerate(_chunked_refs(refs, limit=MAX_SECONDARY_INDEX_REFS_PER_POSTING)):
            record = dict(base)
            record["ref_hashes"] = ref_chunk
            record["posting_part"] = part
            # `ref_hashes` is the one place a posting names what it points at. The singular
            # `ref_hash` restated it on every single-ref row, and `chunk_hash` restated it again;
            # the serving accessor reads neither when the list is present, and `index_hash` is
            # derived from the list, so both are dropped rather than written three ways. Older
            # rows still resolve -- the fallbacks that read them are unchanged.
            record.pop("ref_hash", None)
            record.pop("chunk_hash", None)
            if len(record.get("node_hashes", [])) == 1:
                record["node_hash"] = record["node_hashes"][0]
            else:
                record.pop("node_hash", None)
            route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
            if route.get("placement_key") and not record.get("placement_key"):
                record["placement_key"] = route.get("placement_key")
            if route.get("placement_hash") and not record.get("placement_hash"):
                record["placement_hash"] = route.get("placement_hash")
            if not record.get("storage_record_kind"):
                record["storage_record_kind"] = "index"
            if not record.get("storage_part"):
                record["storage_part"] = record["storage_record_kind"]
            if len(record.get("batch_id_hashes", [])) == 1:
                record["batch_id_hash"] = record["batch_id_hashes"][0]
            else:
                record.pop("batch_id_hash", None)
            if not record.get("node_hashes"):
                record.pop("node_hashes", None)
            if not record.get("batch_id_hashes"):
                record.pop("batch_id_hashes", None)
            identity = {
                "scope_key": record.get("scope_key"),
                "index_name": record.get("index_name"),
                "capability": record.get("capability"),
                "timestamp_key_ms": record.get("timestamp_key_ms"),
                "node_hashes": record.get("node_hashes") or ([record.get("node_hash")] if record.get("node_hash") is not None else []),
                "batch_id_hashes": record.get("batch_id_hashes") or ([record.get("batch_id_hash")] if record.get("batch_id_hash") is not None else []),
                "ref_type": record.get("ref_type"),
                "posting_part": part,
                "ref_hashes": ref_chunk,
            }
            record["index_hash"] = stable_hash(json.dumps(identity, sort_keys=True, separators=(",", ":")))
            compacted_indexes.append(record)
    return passthrough + compacted_indexes
