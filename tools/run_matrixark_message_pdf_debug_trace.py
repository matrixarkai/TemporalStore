#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run a MatrixArk message + resource debug trace and render docs.

The goal of this runner is observability, not load testing. It creates a small
conversation, several PDF and Markdown resources, runs ingestion/extraction/summary/retrieval,
and then renders the exact records written by MatrixArk into Markdown/HTML.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.matrixark_mcp_server import (  # noqa: E402
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
)
from tools import matrixark_mcp_core as mcp_core  # noqa: E402


Json = dict[str, Any]
HIDDEN_COMPACT_REPORT_RECORD_TYPES = {
    "context_batch_commit",
}


MESSAGES = [
    {
        "role": "user",
        "content": "Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.",
    },
    {
        "role": "assistant",
        "content": "Recorded: Project Aurora GPU purchase is approved by Alice in finance, pending procurement owner assignment.",
    },
    {
        "role": "user",
        "content": "Bob will own procurement, and the budget cap is 42000 dollars for the initial GPU batch.",
    },
    {
        "role": "assistant",
        "content": "I will track Bob as procurement owner and the 42000 dollar cap for the initial batch.",
    },
    {
        "role": "user",
        "content": "The deadline is July 15, 2026, and the runbook says finance approval must be attached before vendor selection.",
    },
    {
        "role": "assistant",
        "content": "The active deadline is July 15, 2026. Vendor selection requires the finance approval attachment.",
    },
    {
        "role": "user",
        "content": "Correction: Alice raised the cap to 45000 dollars after the backup GPU quote came in.",
    },
    {
        "role": "assistant",
        "content": "Updated: the current Project Aurora GPU budget cap is 45000 dollars.",
    },
]


PDF_FIXTURES = [
    {
        "filename": "aurora_gpu_approval_packet.pdf",
        "title": "Project Aurora GPU Approval Packet",
        "lines": [
            "Decision: Alice approved the Project Aurora GPU purchase after finance review.",
            "Owner: Bob owns procurement and vendor coordination.",
            "Budget: Current approved cap is 45000 dollars.",
            "Deadline: Purchase order must be ready by July 15, 2026.",
            "Risk: Vendor selection is blocked if finance approval is not attached.",
        ],
    },
    {
        "filename": "aurora_gpu_runbook.pdf",
        "title": "GPU Procurement Runbook",
        "lines": [
            "Procedure: Attach finance approval before vendor selection.",
            "Procedure: Compare primary and backup GPU quotes before purchase order creation.",
            "Troubleshooting: If approval attachment is missing, notify Alice and stop vendor selection.",
            "Audit: Store final vendor selection evidence with the purchase order.",
        ],
    },
    {
        "filename": "aurora_budget_update.pdf",
        "title": "Budget Update Memo",
        "lines": [
            "Update: The backup GPU quote increased the cap from 42000 dollars to 45000 dollars.",
            "Current state: 45000 dollars is the valid active budget cap.",
            "Stale blocker: 42000 dollars is historical and should not be used for current-state answers.",
            "Approver: Alice confirmed the updated cap.",
        ],
    },
]

MD_FIXTURES = [
    {
        "filename": "aurora_gpu_policy.md",
        "title": "Project Aurora GPU Policy",
        "lines": [
            "# Project Aurora GPU Policy",
            "Decision: Alice from finance approved the GPU purchase.",
            "Owner: Bob owns procurement and vendor coordination.",
            "Budget: The current cap is 45000 dollars.",
            "Deadline: The purchase order must be ready by July 15, 2026.",
            "Blocker: Vendor selection must stop if finance approval is missing.",
        ],
    },
    {
        "filename": "aurora_gpu_troubleshooting.md",
        "title": "Project Aurora GPU Troubleshooting",
        "lines": [
            "# Troubleshooting",
            "If vendor selection fails, first verify the finance approval attachment.",
            "If the backup quote is used, keep the 45000 dollar cap and cite Alice's approval.",
            "If procurement owner is missing, assign Bob before creating a purchase order.",
        ],
    },
]


def generated_codex_messages(count: int) -> list[Json]:
    if count <= len(MESSAGES):
        return MESSAGES[:count]
    facts = [
        "Alice from finance approved Project Aurora GPU procurement after Q3 budget review.",
        "Bob owns procurement and vendor coordination for the Project Aurora GPU purchase.",
        "The active GPU budget cap is 45000 dollars after Alice approved the backup quote.",
        "The purchase order deadline is July 15, 2026.",
        "Finance approval must be attached before vendor selection can proceed.",
        "The backup GPU quote should be compared against the primary quote before PO creation.",
        "If the approval attachment is missing, vendor selection must stop and Alice should be notified.",
        "The final vendor selection evidence must be stored with the purchase order.",
        "The historical 42000 dollar cap is stale and must not be used for current-state answers.",
        "The current answer should cite the resource packet or policy when available.",
    ]
    messages: list[Json] = []
    for index in range(count):
        role = "user" if index % 2 == 0 else "assistant"
        prefix = "Codex note" if role == "user" else "Codex assistant recorded"
        messages.append({
            "role": role,
            "content": f"{prefix} [codex_hook_seq={index + 1:03d}]: {facts[index % len(facts)]}",
        })
    return messages


def expanded_resource_fixtures(count: int) -> list[Json]:
    seed: list[Json] = [{**fixture, "resource_type": "pdf"} for fixture in PDF_FIXTURES]
    seed.extend({**fixture, "resource_type": "md"} for fixture in MD_FIXTURES)
    expanded: list[Json] = []
    for index, fixture in enumerate(seed[: max(0, count)], start=1):
        base_lines = list(fixture["lines"])
        lines: list[str] = []
        for section in range(1, 7):
            lines.append(f"Section {section}: {fixture['title']} detail block {section}.")
            lines.extend(base_lines)
            lines.append(
                f"Evidence {section}: Project Aurora GPU approval, Bob owner, 45000 dollar cap, "
                "July 15 deadline, and finance approval attachment blocker."
            )
        expanded.append({**fixture, "title": f"{fixture['title']} - multi chunk {index}", "lines": lines})
    return expanded


QUERY = "What is the current Project Aurora GPU approval, owner, budget cap, deadline, and runbook blocker?"


def call_tool(server: MatrixArkMcpServer, name: str, arguments: Json) -> Json:
    arguments.setdefault("timeout_ms", 120000)
    result = server.call_tool(name, dict(arguments))
    if not isinstance(result, dict):
        raise RuntimeError(f"{name} returned non-object result: {result!r}")
    return result


def write_pdf(path: Path, title: str, lines: list[str]) -> None:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except Exception as exc:  # pragma: no cover - local dependency guard.
        raise RuntimeError("reportlab is required to generate PDF debug fixtures") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    canvas_obj = canvas.Canvas(str(path), pagesize=letter)
    _, height = letter
    canvas_obj.setFont("Helvetica-Bold", 14)
    canvas_obj.drawString(72, height - 72, title)
    canvas_obj.setFont("Helvetica", 10)
    y = height - 102
    for line in lines:
        if y < 72:
            canvas_obj.showPage()
            canvas_obj.setFont("Helvetica", 10)
            y = height - 72
        canvas_obj.drawString(72, y, line[:110])
        y -= 18
    canvas_obj.save()


def compact(value: Any, limit: int = 180) -> str:
    text = json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
    text = " ".join(text.split())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def short_source(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = text.replace("\\", "/")
    marker = "/fixtures/"
    if marker in text:
        return text.split(marker, 1)[1]
    if str(REPO_ROOT).replace("\\", "/") in text:
        text = text.replace(str(REPO_ROOT).replace("\\", "/"), "<repo>")
    if re.search(r"\.(pdf|md|csv|txt|json|html)(#\S+)?$", text, flags=re.IGNORECASE):
        base, sep, suffix = text.partition("#")
        return Path(base).name + (sep + suffix if sep else "")
    return text


def sanitize_debug_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = text.replace("\\", "/")
    text = text.replace(str(REPO_ROOT).replace("\\", "/"), "<repo>")
    text = re.sub(
        r"(?:[A-Za-z]:)?/?[^ \n\r\t\"']*?/fixtures/([^ \n\r\t\"']+)",
        r"\1",
        text,
    )
    return text


def sanitize_compact_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sanitize_compact_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_compact_payload(item) for item in value]
    if isinstance(value, str):
        return sanitize_debug_text(value)
    return value


def short_node_path(value: Any) -> str:
    if not isinstance(value, list):
        return str(value or "")
    if len(value) <= 2:
        return "/".join(str(part) for part in value)
    return "/".join(str(part) for part in value[-2:])


def first_present(record: Json, *fields: str) -> Any:
    for field in fields:
        value: Any = record
        for part in field.split("."):
            value = value.get(part, "") if isinstance(value, dict) else ""
        if value not in ("", None, [], {}):
            return value
    return ""


class ReportAliases:
    """Stable short ids for one debug report.

    The raw records still carry durable hashes. The default debug page should
    not spend space on those long values, so it maps them to compact per-run
    aliases such as n1/e2/c3.
    """

    def __init__(self) -> None:
        self._values: dict[str, dict[str, str]] = defaultdict(dict)

    def alias(self, namespace: str, value: Any) -> str:
        if value in ("", None, [], {}):
            return ""
        key = str(value)
        bucket = self._values[namespace]
        if key not in bucket:
            bucket[key] = f"{namespace}{len(bucket) + 1}"
        return bucket[key]

    def node(self, value: Any) -> str:
        return self.alias("n", value)

    def event(self, value: Any) -> str:
        return self.alias("e", value)

    def entity(self, value: Any) -> str:
        return self.alias("x", value)

    def summary(self, value: Any) -> str:
        return self.alias("s", value)

    def resource(self, value: Any) -> str:
        return self.alias("r", value)

    def chunk(self, value: Any) -> str:
        return self.alias("c", value)

    def ref(self, ref_type: Any, ref_hash: Any) -> str:
        ref_type_text = str(ref_type or "")
        if ref_type_text in {"event", "context_event"}:
            return self.event(ref_hash)
        if ref_type_text in {"entity", "context_entity"}:
            return self.entity(ref_hash)
        if ref_type_text in {"summary", "context_summary"}:
            return self.summary(ref_hash)
        if ref_type_text in {"resource", "resource_manifest"}:
            return self.resource(ref_hash)
        if ref_type_text in {"resource_chunk", "chunk"}:
            return self.chunk(ref_hash)
        return self.alias("ref", f"{ref_type_text}:{ref_hash}")


def compact_resources(resources: list[Json]) -> list[Json]:
    rows = []
    for index, resource in enumerate(resources, start=1):
        rows.append(
            {
                "rid": f"r{index}",
                "type": resource.get("resource_type", ""),
                "title": resource.get("title", ""),
                "source": short_source(resource.get("raw_uri")),
                "lines": resource.get("line_count", 0),
            }
        )
    return rows


def compact_entity_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = [part.strip() for part in text.split(":") if part.strip()]
    if len(parts) >= 3 and any(("/" in part or "\\" in part or "<repo>" in part) for part in parts[1:-1]):
        return parts[-1]
    if len(parts) >= 2 and parts[0] in {"decision", "owner", "budget", "deadline", "blocker", "approval"}:
        return parts[-1]
    return short_source(text)


def compact_resource_chunk(record: Json, aliases: ReportAliases) -> Json:
    metadata = record.get("metadata", {}) if isinstance(record.get("metadata"), dict) else {}
    return {
        "chunk": aliases.chunk(record.get("chunk_hash")),
        "resource": aliases.resource(record.get("resource_hash")),
        "source": short_source(record.get("source_ref") or metadata.get("citation") or record.get("source_locator")),
        "kind": metadata.get("unit_kind") or record.get("unit_kind") or record.get("resource_type"),
        "tokens": record.get("token_estimate", 0),
        "text": sanitize_debug_text(record.get("text", "")),
    }


def compact_context_event(record: Json, aliases: ReportAliases) -> Json:
    event_type = first_present(record, "event_type", "internal_extraction.event_type")
    entity_type = first_present(record, "entity_type", "internal_extraction.entity_type")
    classification = record.get("classification")
    row: Json = {
        "event": aliases.event(record.get("event_id_hash")),
        "node": aliases.node(record.get("node_hash")) or short_node_path(record.get("node_path")),
        "type": event_type,
        "entity": entity_type,
        "source": short_source(first_present(record, "source_ref", "source_locator")),
        "text": sanitize_debug_text(first_present(record, "summary_text", "text")),
    }
    if classification not in ("", None, "NEW_EVENT"):
        row["class"] = classification
    return {key: value for key, value in row.items() if value not in ("", None, [], {})}


def compact_context_entity(record: Json, aliases: ReportAliases) -> Json:
    return {
        "entity": aliases.entity(record.get("entity_hash")),
        "node": aliases.node(record.get("node_hash")) or short_node_path(record.get("node_path")),
        "type": record.get("entity_type", ""),
        "name": compact_entity_name(record.get("entity_name", "")),
        "op": record.get("operator", ""),
        "state": sanitize_debug_text(record.get("state", "")),
        "source": short_source(first_present(record, "source_ref", "source_locator")),
    }


def compact_context_summary(record: Json, aliases: ReportAliases) -> Json:
    return {
        "type": record.get("summary_type", ""),
        "summary": aliases.summary(record.get("summary_hash")),
        "node": aliases.node(record.get("node_hash")) or short_node_path(record.get("node_path")),
        "text": sanitize_debug_text(record.get("summary_text", "")),
        "sources": len(record.get("source_chunk_hashes") or record.get("source_event_ids") or []),
    }


def compact_context_embedding(record: Json, aliases: ReportAliases) -> Json:
    preview = vector_preview(record)
    return {
        "type": record.get("embedding_type", ""),
        "ref": aliases.ref(record.get("ref_type"), record.get("ref_hash")),
        **preview,
    }


def compact_import_task(record: Json) -> Json:
    return {
        "status": record.get("status", ""),
        "type": record.get("resource_type", ""),
        "source": short_source(record.get("raw_uri") or record.get("requested_raw_uri")),
        "chunks": record.get("chunk_count", ""),
        "facts": record.get("resource_fact_count", ""),
        "entities": record.get("resource_entity_count", ""),
    }


def compact_summary_policy(record: Json, aliases: ReportAliases) -> Json:
    policy = record.get("summary_generation_policy", {}) if isinstance(record.get("summary_generation_policy"), dict) else {}
    return {
        "node": aliases.node(record.get("node_hash")) or short_node_path(record.get("node_path")),
        "types": record.get("generated_summary_types", []),
        "l1": policy.get("generate_l1", ""),
        "reason": policy.get("reason", ""),
        "tokens": policy.get("token_estimate", ""),
        "events": record.get("source_event_count", 0),
        "child_summaries": record.get("source_summary_count", 0),
    }


def compact_context_indexes(records: list[Json], aliases: ReportAliases) -> list[Json]:
    postings: dict[tuple[str, str, Any, Any], set[Any]] = {}
    for record in records:
        if str(record.get("data_model") or "") in HIDDEN_COMPACT_REPORT_RECORD_TYPES:
            continue
        key = (
            str(record.get("data_model") or record.get("ref_type") or ""),
            str(record.get("index_name") or ""),
            record.get("node_hash") or "",
            record.get("ref_type") or "",
        )
        refs = postings.setdefault(key, set())
        for ref in record.get("ref_hashes") or []:
            refs.add(ref)
        ref_hash = record.get("ref_hash") or record.get("event_id_hash") or record.get("chunk_hash")
        if ref_hash not in ("", None):
            refs.add(ref_hash)
    rows = []
    for (model, index_name, node_hash, ref_type), refs in sorted(postings.items(), key=lambda item: (item[0][0], item[0][1], str(item[0][2]))):
        sample_refs = []
        if model not in {"resource_fact", "resource_entity_fact"}:
            sample_refs = [aliases.ref(ref_type, ref) for ref in list(sorted(refs, key=str))[:3]]
        rows.append(
            {
                "model": model,
                "index": index_name,
                "node": aliases.node(node_hash),
                "ref_count": len(refs),
                "sample_refs": sample_refs,
            }
        )
    return rows


def compact_context_child_refs(records: list[Json], aliases: ReportAliases) -> list[Json]:
    rows = []
    for record in records:
        parent_hash = record.get("parent_hash")
        child_hash = record.get("child_hash")
        rows.append(
            {
                "index_key": f"ctx:child:{first_present(record, 'scope.tenant_hash', 'tenant_hash')}:{parent_hash}",
                "parent": aliases.node(parent_hash),
                "child": aliases.node(child_hash),
                "child_name": record.get("child_name", ""),
                "updated_at_ms": record.get("updated_at_ms") or record.get("created_at_ms") or "",
                "ref": aliases.ref("child_ref", record.get("child_ref_hash")),
            }
        )
    return sorted(rows, key=lambda item: (str(item["parent"]), str(item["child_name"]), str(item["child"])))


def compact_placement_routes(records: list[Json], aliases: ReportAliases) -> list[Json]:
    rows_by_key: dict[tuple[str, str, str], Json] = {}
    for record in records:
        route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
        placement_key = record.get("placement_key") or route.get("placement_key") or route.get("routing_key") or route.get("partition_key")
        placement_hash = record.get("placement_hash") or route.get("placement_hash")
        node_hash = record.get("node_hash") or record.get("node_id")
        if not placement_key and node_hash:
            scope_key = record.get("scope_key") or first_present(record, "scope.scope_key")
            if scope_key:
                placement_key = f"context:{scope_key}:node={node_hash}"
        if placement_key in ("", None):
            continue
        record_type = str(record.get("record_type") or "")
        key = (str(placement_key), record_type, str(node_hash or ""))
        row = rows_by_key.setdefault(
            key,
            {
                "record_type": record_type,
                "node": aliases.node(node_hash) if node_hash else "",
                "placement": aliases.alias("p", placement_key),
                "placement_hash": placement_hash or "",
                "example_shard_16": int(placement_hash) % 16 if str(placement_hash).isdigit() else "",
                "records": 0,
            },
        )
        row["records"] += 1
    return sorted(rows_by_key.values(), key=lambda item: (str(item["placement"]), str(item["record_type"])))[:80]


def data_field_inventory(records: list[Json]) -> list[Json]:
    fields_by_type: dict[str, set[str]] = defaultdict(set)
    for record in records:
        record_type = str(record.get("record_type") or "unknown")
        if record_type in HIDDEN_COMPACT_REPORT_RECORD_TYPES:
            continue
        for key, value in record.items():
            if isinstance(value, dict):
                for child_key in value.keys():
                    fields_by_type[record_type].add(f"{key}.{child_key}")
            else:
                fields_by_type[record_type].add(key)
    rows = []
    for record_type, fields in sorted(fields_by_type.items()):
        rows.append(
            {
                "record_type": record_type,
                "field_count": len(fields),
                "fields": ", ".join(sorted(fields)),
            }
        )
    return rows


def compact_replay(result: Json) -> Json:
    if not result:
        return {}
    return {
        key: result.get(key)
        for key in ("status", "event_count", "replay_event_count", "warning")
        if result.get(key) not in ("", None, [], {})
    }


def compact_tool_result(result: Json) -> Json:
    if not isinstance(result, dict):
        return {}
    return {
        key: result.get(key)
        for key in (
            "status",
            "record_count",
            "messages_ingested",
            "chunk_count",
            "resource_fact_count",
            "resource_entity_count",
            "committed",
            "dirty_nodes_refreshed",
            "summaries_refreshed",
            "selected_refs",
            "used_context_tokens",
        )
        if result.get(key) not in ("", None, [], {})
    }


def compact_trace(trace: Json) -> Json:
    compacted = {
        "scope": {
            key: trace.get("scope", {}).get(key)
            for key in ("account_id", "tenant_id", "user_id", "session_id", "agent_name")
            if trace.get("scope", {}).get(key)
        },
        "query": trace.get("query", ""),
        "embedding_model": trace.get("embedding_model", ""),
        "embedding_execution_mode": trace.get("embedding_execution_mode", ""),
        "summary_refresh_policy": trace.get("summary_refresh_policy", {}),
        "resources": compact_resources(trace.get("resources", [])),
        "calls": [],
    }
    for call in trace.get("calls", []):
        if not isinstance(call, dict):
            continue
        compacted["calls"].append(
            {
                key: value
                for key, value in {
                    "tool": call.get("tool"),
                    "kind": call.get("kind"),
                    "message_index": call.get("message_index"),
                    "resource_type": call.get("resource_type"),
                    "resource": short_source(call.get("raw_uri")),
                    "result": compact_tool_result(call.get("result", {})),
                }.items()
                if value not in ("", None, [], {})
            }
        )
    return compacted


def vector_preview(record: Json) -> Json:
    vector = record.get("vector")
    # The stored vector has two forms; a preview that only understood the list would
    # report an encoded vector as missing.
    if isinstance(vector, str):
        from matrixark_mcp_core import decode_stored_vector
        vector = decode_stored_vector(vector)
    if not isinstance(vector, list):
        return {"dim": record.get("dim", 0), "preview": []}
    return {
        "dim": len(vector),
        "preview": [round(float(value), 5) for value in vector[:8]],
    }


def model_registry_map(records: list[Json]) -> dict[str, str]:
    registry: dict[str, str] = {}
    for record in records:
        if str(record.get("record_type") or "") != "context_model_registry":
            continue
        model_ref = str(record.get("model_ref") or "")
        model_name = str(record.get("model_name") or "")
        if model_ref and model_name:
            registry[model_ref] = model_name
    return registry


def embedding_model_name_for_display(record: Json, registry: dict[str, str]) -> str:
    """Display readable model names while hot records keep compact model_ref."""
    model = str(record.get("model") or "")
    if model:
        return model
    model_ref = str(record.get("model_ref") or "")
    if model_ref and registry.get(model_ref):
        return registry[model_ref]
    if model_ref:
        return model_ref
    model_hash = record.get("model_hash")
    if model_hash is not None:
        return f"legacy_model_hash:{model_hash}"
    return ""


def read_records(path: Path) -> list[Json]:
    records: list[Json] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return compact_latest_context_state_records(records)


def latest_by_key(records: list[Json], key_fields: list[str]) -> list[Json]:
    latest: dict[tuple[Any, ...], Json] = {}
    for record in records:
        key = tuple(record.get(field) for field in key_fields)
        existing = latest.get(key)
        if existing is None or int(record.get("updated_at_ms") or 0) >= int(existing.get("updated_at_ms") or 0):
            latest[key] = record
    return list(latest.values())


def node_tree(records: list[Json]) -> list[Json]:
    nodes_by_hash: dict[int, Json] = {}
    for record in records:
        if record.get("record_type") == "context_node":
            node_hash = int(record.get("node_hash", 0))
            nodes_by_hash[node_hash] = {
                "node_hash": node_hash,
                "parent_hash": int(record.get("parent_hash", 0) or 0),
                "name": record.get("node_name") or record.get("name") or "",
                "path": record.get("node_path", []),
                "depth": record.get("depth", 0),
                "record": record,
                "children": [],
            }
    for node in nodes_by_hash.values():
        parent_hash = node["parent_hash"]
        if parent_hash in nodes_by_hash:
            nodes_by_hash[parent_hash]["children"].append(node)
    roots = [node for node in nodes_by_hash.values() if node["parent_hash"] not in nodes_by_hash]
    roots.sort(key=lambda item: (item["depth"], item["name"]))
    return roots


def render_node_html(node: Json, aliases: ReportAliases) -> str:
    label = str(node.get("name") or aliases.node(node.get("node_hash")))
    record_obj = node.get("record", {}) if isinstance(node.get("record"), dict) else {}
    compact_record = {
        "node": aliases.node(record_obj.get("node_hash")),
        "parent": aliases.node(record_obj.get("parent_hash")),
        "name": record_obj.get("node_name"),
    }
    record = html.escape(json.dumps(compact_record, indent=2, sort_keys=True))
    children = "\n".join(render_node_html(child, aliases) for child in sorted(node.get("children", []), key=lambda item: item["name"]))
    return (
        "<details open class=\"node\">"
        f"<summary><span class=\"node-name\">{html.escape(label)}</span> "
        f"<span class=\"muted\">{html.escape(aliases.node(node.get('node_hash')))}</span></summary>"
        f"<pre>{record}</pre>{children}</details>"
    )


def records_table(records: list[Json], fields: list[str]) -> str:
    header = "".join(f"<th>{html.escape(field)}</th>" for field in fields)
    rows = []
    for record in records:
        cells = []
        for field in fields:
            value = record
            for part in field.split("."):
                if isinstance(value, dict):
                    value = value.get(part, "")
                else:
                    value = ""
            cells.append(f"<td>{html.escape(compact(value, 240))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def event_display_rows(records: list[Json]) -> tuple[list[Json], list[Json]]:
    raw_events: list[Json] = []
    fact_events: list[Json] = []
    for record in records:
        event_type = str(record.get("event_type") or record.get("entity_type") or "")
        classification = str(record.get("classification") or "")
        is_resource_fact = bool(record.get("source_ref")) or classification == "RESOURCE_FACT" or event_type.startswith("resource_")
        context_event_key = record.get("event_time_key") or ""
        if not context_event_key:
            timestamp = record.get("timestamp_key_ms") or record.get("ingestion_time_ms") or record.get("updated_at_ms") or record.get("created_at_ms")
            event_hash = record.get("event_id_hash") or ""
            if timestamp:
                context_event_key = f"{int(timestamp):020d}:{event_hash}"
        row = {
            "event_id_hash": record.get("event_id_hash"),
            "context_event_key": context_event_key,
            "classification": classification,
            "event_type": event_type,
            "summary_text": record.get("summary_text") or record.get("text"),
            "text": record.get("text") or record.get("summary_text"),
            "source_ref": record.get("source_ref"),
        }
        if is_resource_fact:
            fact_events.append(row)
        else:
            raw_events.append(row)
    return raw_events, fact_events


def latest_records_by_key(records: list[Json], key_fields: list[str]) -> list[Json]:
    latest: dict[tuple[str, ...], Json] = {}
    for record in records:
        key = tuple(str(record.get(field, "")) for field in key_fields)
        previous = latest.get(key)
        if previous is None or int(record.get("updated_at_ms") or 0) >= int(previous.get("updated_at_ms") or 0):
            latest[key] = record
    return sorted(latest.values(), key=lambda item: (str(item.get("summary_type", "")), str(item.get("node_path", "")), int(item.get("updated_at_ms") or 0)))



PIPELINE_MERMAID = """flowchart TD
  A["Codex/agent message or file URI"]
  B["matrixark_ingest via MCP"]
  C["Lightweight ContextEvent write"]
  D["Session buffer + batch commit"]
  E["ResourceParser: PDF/MD chunks"]
  F["OSS embedding provider"]
  G["ContextSummary L0/L1 + embeddings"]
  H["ContextIndex secondary filters"]
  I["matrixark_retrieve query"]
  J["Tree-first node scan using L0/L1"]
  K["Leaf candidates: segment/event/entity/resource"]
  L["Question-type packer"]
  M["ContextPack + audit/replay"]

  A --> B
  B --> C
  B --> D
  B --> E
  C --> F
  D --> K
  E --> F
  F --> G
  F --> H
  I --> J --> K --> L --> M
  H --> K
"""


DATA_MODEL_ROWS = [
    {"model": "ContextNode", "purpose": "Filesystem-like topology. Messages/resources attach to a leaf node, parents are used for traversal.", "important_fields": "node, parent, name, compact path"},
    {"model": "ContextEvent", "purpose": "Replayable extracted fact or raw conversational event.", "important_fields": "event, node, type, entity, source, text"},
    {"model": "ContextSegment", "purpose": "Batch/session topic segment when a logical window is committed.", "important_fields": "segment, node, source events, summary, time range"},
    {"model": "ContextEntity", "purpose": "Evolving state for current preference/status/owner/budget/deadline.", "important_fields": "entity, node, type, name, operator, state, source"},
    {"model": "ResourceManifest", "purpose": "Logical imported file/resource version. Raw bytes stay outside TemporalStore.", "important_fields": "resource, type, version, content digest, compact scope"},
    {"model": "ResourceChunk", "purpose": "Cited serving chunk from PDF/MD/etc. The manifest owns full raw paths; chunks show compact source labels.", "important_fields": "chunk, resource, source, kind, tokens, text"},
    {"model": "ContextSummary", "purpose": "L0/L1 node/resource summary used for preview and tree traversal.", "important_fields": "summary, type, node, source count, text"},
    {"model": "ContextEmbedding", "purpose": "Vector stored separately for summaries, chunks, events, entities, and resources.", "important_fields": "type, ref, dim, preview"},
    {"model": "ContextIndex", "purpose": "Bounded secondary filters before similarity scoring.", "important_fields": "model, index, time, node, ref count, sample"},
    {"model": "ContextPackAudit", "purpose": "Optional observability record. Default report shows compact pack, not full audit payloads.", "important_fields": "pack id, grouped refs, token summary, warnings"},
]

def markdown_table(records: list[Json], fields: list[str], limit: int = 24) -> str:
    lines = ["|" + "|".join(fields) + "|", "|" + "|".join(["---"] * len(fields)) + "|"]
    for record in records[:limit]:
        row = []
        for field in fields:
            value: Any = record
            for part in field.split("."):
                value = value.get(part, "") if isinstance(value, dict) else ""
            row.append(compact(value, 120).replace("|", "\\|"))
        lines.append("|" + "|".join(row) + "|")
    return "\n".join(lines)


def write_outputs(
    *,
    output_dir: Path,
    event_log: Path,
    trace: Json,
    records: list[Json],
    retrieve_result: Json,
    replay_result: Json,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "matrixark_message_resource_debug_trace.json"
    md_path = output_dir / "matrixark_message_resource_debug_trace.md"
    html_path = output_dir / "matrixark_message_resource_debug_trace.html"

    visible_records = [
        record
        for record in records
        if str(record.get("record_type", "unknown")) not in HIDDEN_COMPACT_REPORT_RECORD_TYPES
    ]
    counts = Counter(str(record.get("record_type", "unknown")) for record in visible_records)
    by_type: dict[str, list[Json]] = defaultdict(list)
    for record in records:
        by_type[str(record.get("record_type", "unknown"))].append(record)

    aliases = ReportAliases()
    for record in by_type["context_node"]:
        aliases.node(record.get("node_hash"))
    for index, resource in enumerate(trace.get("resources", []), start=1):
        if resource.get("resource_hash"):
            aliases._values["r"][str(resource["resource_hash"])] = f"r{index}"

    current_embedding_records = latest_by_key(
        by_type["context_embedding"],
        ["embedding_type", "ref_type", "ref_hash"],
    )
    model_counts = Counter(str(record.get("model") or "default") for record in current_embedding_records)
    embedding_models = [
        {"model": model, "embedding_count": count}
        for model, count in sorted(model_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    embeddings = [
        compact_context_embedding(record, aliases)
        for record in current_embedding_records
    ]
    summary_policy_rows = [compact_summary_policy(record, aliases) for record in by_type["context_summary_refresh_audit"]]
    compact_records_by_type: dict[str, list[Json]] = {
        "context_node": [
            {
                "node": aliases.node(record.get("node_hash")),
                "parent": aliases.node(record.get("parent_hash")),
                "name": record.get("node_name") or record.get("name"),
                "path": short_node_path(record.get("node_path")),
            }
            for record in by_type["context_node"]
        ],
        "context_event": [compact_context_event(record, aliases) for record in by_type["context_event"]],
        "context_entity": [compact_context_entity(record, aliases) for record in by_type["context_entity"]],
        "context_summary": [compact_context_summary(record, aliases) for record in latest_by_key(by_type["context_summary"], ["summary_type", "summary_hash", "node_hash"])],
        "context_embedding": embeddings,
        "context_index_postings": compact_context_indexes(by_type["context_index"], aliases),
        "context_child_ref": compact_context_child_refs(by_type["context_child_ref"], aliases),
        "resource_import_task": [compact_import_task(record) for record in by_type["resource_import_task"]],
        "resource_chunk": [compact_resource_chunk(record, aliases) for record in by_type["resource_chunk"]],
    }
    placement_routes = compact_placement_routes(visible_records, aliases)
    field_inventory = data_field_inventory(visible_records)
    compact_pack = sanitize_compact_payload(mcp_core.compact_context_pack_for_serving(retrieve_result))
    if isinstance(compact_pack, dict):
        compact_pack.pop("context_pack_id", None)

    exported = {
        "trace": compact_trace(trace),
        "record_counts": dict(counts),
        "context_pack": compact_pack,
        "replay": compact_replay(replay_result),
        "records_by_type": compact_records_by_type,
        "embeddings": embeddings,
        "summary_generation_policy": summary_policy_rows,
        "parent_child_index": compact_records_by_type["context_child_ref"],
        "placement_routes": placement_routes,
        "data_field_inventory": field_inventory,
    }
    json_path.write_text(json.dumps(exported, indent=2, sort_keys=True), encoding="utf-8")

    if trace.get("embedding_execution_mode") == "oss_embedding_model":
        rerun_command = (
            "MATRIXARK_EMBEDDING_PROVIDER=oss MATRIXARK_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2 "
            "MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1 python3 tools/run_matrixark_message_pdf_debug_trace.py "
            "--output-dir docs/debug/matrixark_message_resource_trace"
        )
        embedding_note = "OSS embedding provider completed for this run."
    else:
        rerun_command = (
            "MATRIXARK_EMBEDDING_PROVIDER=deterministic python3 tools/run_matrixark_message_pdf_debug_trace.py "
            "--output-dir docs/debug/matrixark_message_resource_trace"
        )
        embedding_note = (
            "This run completed with the local deterministic embedding backend. "
            "The local sentence-transformers OSS probe timed out before this trace was generated, so the data-flow artifact is complete but not an OSS-embedding proof."
        )

    md = [
        "# MatrixArk Message + Resource Debug Trace",
        "",
        "This debug run ingests LOCOMO-style multi-turn conversation messages and several PDF/Markdown resources, then retrieves one ContextPack. "
        "It is meant for inspecting exactly what MatrixArk writes and reads during ingestion, extraction, chunking, "
        "summary generation, embedding storage, tree traversal, secondary-index filtering, packing, audit, and replay.",
        "",
        "## Pipeline Diagram",
        "",
        "```mermaid",
        PIPELINE_MERMAID,
        "```",
        "",
        "## Re-run",
        "",
        "```bash",
        rerun_command,
        "```",
        "",
        "## Configuration",
        "",
        f"- Embedding model: `{trace['embedding_model']}`",
        f"- Embedding execution mode: `{trace['embedding_execution_mode']}`",
        f"- Extraction provider: `{trace.get('extraction_provider')}`",
        f"- Extraction model: `{trace.get('extraction_model')}` at `{trace.get('extraction_base_url')}`",
        f"- Require OSS understanding: `{trace.get('require_oss_understanding')}`",
        f"- Query: `{QUERY}`",
        f"- Summary refresh: background interval `{trace['summary_refresh_policy']['background_interval_ms']}` ms, limit `{trace['summary_refresh_policy']['background_limit']}` dirty nodes per tick",
        f"- Node L1 policy: {trace['summary_refresh_policy']['node_l1_policy']}",
        f"- Embedding note: {embedding_note}",
        "",
        "## Data Model Field Guide",
        "",
        markdown_table(DATA_MODEL_ROWS, ["model", "purpose", "important_fields"], limit=50),
        "",
        "## Record Counts",
        "",
        markdown_table([{"record_type": key, "count": value} for key, value in sorted(counts.items())], ["record_type", "count"]),
        "",
        "## Data Field Inventory",
        "",
        "Observed compact/raw fields by data model for this run. This is for debugging schema shape; token-facing ContextPack still stays compact.",
        "",
        markdown_table(field_inventory, ["record_type", "field_count", "fields"], limit=120),
        "",
        "## Input Messages",
        "",
        markdown_table([{"role": item["role"], "content": item["content"]} for item in MESSAGES], ["role", "content"], limit=50),
        "",
        "## Resources",
        "",
        markdown_table(compact_resources(trace["resources"]), ["rid", "type", "title", "source", "lines"], limit=20),
        "",
        "## Resource Import Tasks",
        "",
        markdown_table(compact_records_by_type["resource_import_task"], ["status", "type", "source", "chunks", "facts", "entities"], limit=50),
        "",
        "## Resource Chunks",
        "",
        markdown_table(compact_records_by_type["resource_chunk"], ["chunk", "resource", "source", "kind", "tokens", "text"], limit=80),
        "",
        "## Parent-To-Child Index",
        "",
        "ContextNode children are scanned through the narrow adjacency key `ctx:child:{tenant_hash}:{parent_hash}`. "
        "The node record does not persist a child count; the UI derives graph edges from these refs.",
        "",
        markdown_table(compact_records_by_type["context_child_ref"], ["index_key", "parent", "child", "child_name", "updated_at_ms", "ref"], limit=120),
        "",
        "## Placement / Data-Node Mapping Examples",
        "",
        "Serving records carry a stable placement key so TemporalStore can colocate node-local records and route them to a shard/data node. "
        "This compact report aliases the full key as `pN`; raw placement keys stay in audit/debug artifacts. "
        "`example_shard_16` is an illustrative modulo of the placement hash; a live topology maps the same placement hash through the metaserver slot table.",
        "",
        markdown_table(placement_routes, ["record_type", "node", "placement", "placement_hash", "example_shard_16", "records"], limit=120),
        "",
        "## Raw Conversation Events",
        "",
        markdown_table(compact_records_by_type["context_event"], ["event", "node", "type", "entity", "source", "text"], limit=80),
        "",
        "## Extracted Resource/Fact Events",
        "",
        markdown_table(extracted_resource_fact_events, ["event_type", "summary_text", "source_ref", "event_id_hash", "context_event_key"], limit=120),
        "",
        "## ContextEntities",
        "",
        markdown_table(compact_records_by_type["context_entity"], ["entity", "node", "type", "name", "op", "state", "source"], limit=80),
        "",
        "## ContextSummaries",
        "",
        markdown_table(compact_records_by_type["context_summary"], ["type", "summary", "node", "sources", "text"], limit=80),
        "",
        "## Node L0/L1 Generation Policy",
        "",
        markdown_table(summary_policy_rows, ["node", "types", "l1", "reason", "tokens", "events", "child_summaries"], limit=80),
        "",
        "## Embedding Model Registry",
        "",
        "Embedding records carry compact `model_ref`; this latest-state registry stores each readable model name once for debug and audit compatibility.",
        "",
        markdown_table(by_type["context_model_registry"], ["model_ref", "model_name", "provider", "execution_mode", "updated_at_ms"], limit=20),
        "",
        "## Embeddings",
        "",
        markdown_table(embedding_models, ["model", "embedding_count"], limit=20),
        "",
        markdown_table(embeddings, ["type", "ref", "dim", "preview"], limit=120),
        "",
        "## Secondary Index Postings",
        "",
        "Resource fact postings show counts only in the compact report; full fact ref lists are raw audit/debug data.",
        "",
        markdown_table(compact_records_by_type["context_index_postings"], ["model", "index", "node", "ref_count", "sample_refs"], limit=120),
        "",
        "## Retrieval Scan",
        "",
        "Retrieval uses the same scope as ingestion. The intended order is: understand the query, apply scope and secondary-index filters, "
        "scan ContextNode L0/L1 summary embeddings to choose folders, fetch leaf candidates, then score segments/events/entities/resource chunks "
        "and pack the final ContextPack under the token budget. If a summary is missing, MatrixArk can still fall back to recent events/entities/chunks.",
        "",
        "```json",
        json.dumps(
            {
                "query": QUERY,
                "used_context_tokens": retrieve_result.get("used_context_tokens"),
                "context_pack": compact_pack,
                "quality_warnings": retrieve_result.get("quality_warnings"),
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## ContextPack",
        "",
        "```json",
        json.dumps(compact_pack, indent=2, sort_keys=True)[:20000],
        "```",
        "",
        "## Replay",
        "",
        "```json",
        json.dumps(compact_replay(replay_result), indent=2, sort_keys=True)[:12000],
        "```",
    ]
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")

    roots = node_tree(records)
    graph_html = "\n".join(render_node_html(root, aliases) for root in roots) or "<p>No context_node records found.</p>"
    model_table_html = records_table(DATA_MODEL_ROWS, ["model", "purpose", "important_fields"])
    html_embedding_note = html.escape(embedding_note)
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MatrixArk Message + Resource Debug Trace</title>
  <style>
    body {{ font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: #17202a; background: #f7f8fa; }}
    header {{ padding: 28px 32px; background: #ffffff; border-bottom: 1px solid #d9dee5; }}
    main {{ padding: 24px 32px 48px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }}
    h2 {{ margin-top: 32px; font-size: 19px; }}
    .muted {{ color: #667085; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }}
    .metric {{ background: #fff; border: 1px solid #d9dee5; border-radius: 8px; padding: 12px; }}
    .metric strong {{ display: block; font-size: 22px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee5; border-radius: 8px; overflow: hidden; }}
    th, td {{ text-align: left; vertical-align: top; border-bottom: 1px solid #edf0f3; padding: 8px 10px; font-size: 12px; }}
    th {{ background: #eef2f6; font-size: 11px; text-transform: uppercase; color: #405066; }}
    pre {{ overflow: auto; background: #111827; color: #e5e7eb; border-radius: 8px; padding: 12px; font-size: 12px; }}
    details.node {{ margin-left: 18px; padding: 6px 0; }}
    details.node > summary {{ cursor: pointer; padding: 7px 9px; background: #fff; border: 1px solid #d9dee5; border-radius: 6px; }}
    .node-name {{ font-weight: 700; }}
    .section {{ margin-bottom: 28px; }}
    .pill {{ display: inline-block; background: #e8f1ff; color: #174a8b; padding: 2px 8px; border-radius: 999px; font-size: 12px; margin-right: 6px; }}
  </style>
</head>
<body>
  <header>
    <h1>MatrixArk Message + Resource Debug Trace</h1>
    <p class="muted">Conversation + resource ingestion, extraction, resource chunking, embeddings, summaries, tree traversal, ContextPack, audit, and replay.</p>
    <p><span class="pill">{html.escape(trace['embedding_model'])}</span><span class="pill">{html.escape(trace['embedding_execution_mode'])}</span><span class="pill">Extraction: {html.escape(str(trace.get('extraction_provider')))}</span><span class="pill">Summary refresh: background interval {trace['summary_refresh_policy']['background_interval_ms']} ms</span><span class="pill">Limit {trace['summary_refresh_policy']['background_limit']} dirty nodes/tick</span></p>
    <p class="muted">Node L1 policy: {html.escape(trace['summary_refresh_policy']['node_l1_policy'])}</p>
    <p class="muted">Embedding note: {html_embedding_note}</p>
  </header>
  <main>
    <section class="grid">
      <div class="metric"><span class="muted">Visible Records</span><strong>{len(visible_records)}</strong></div>
      <div class="metric"><span class="muted">Events</span><strong>{counts.get('context_event', 0)}</strong></div>
      <div class="metric"><span class="muted">Entities</span><strong>{counts.get('context_entity', 0)}</strong></div>
      <div class="metric"><span class="muted">Chunks</span><strong>{counts.get('resource_chunk', 0)}</strong></div>
      <div class="metric"><span class="muted">Embeddings</span><strong>{counts.get('context_embedding', 0)}</strong></div>
      <div class="metric"><span class="muted">Selected Refs</span><strong>{len(retrieve_result.get('selected_refs', []))}</strong></div>
    </section>
    <section class="section"><h2>Pipeline</h2><pre>{html.escape(PIPELINE_MERMAID)}</pre></section>
    <section class="section"><h2>Data Model Field Guide</h2>{model_table_html}</section>
    <section class="section"><h2>Data Field Inventory</h2><p class="muted">Observed fields by data model for this run. Token-facing ContextPack remains compact.</p>{records_table(field_inventory, ['record_type', 'field_count', 'fields'])}</section>
    <section class="section"><h2>ContextNode Graph</h2>{graph_html}</section>
    <section class="section"><h2>Messages</h2>{records_table([{'role': m['role'], 'content': m['content']} for m in MESSAGES], ['role', 'content'])}</section>
    <section class="section"><h2>Resources</h2>{records_table(compact_resources(trace['resources']), ['rid', 'type', 'title', 'source', 'lines'])}</section>
    <section class="section"><h2>Resource Import Tasks</h2>{records_table(compact_records_by_type['resource_import_task'], ['status', 'type', 'source', 'chunks', 'facts', 'entities'])}</section>
    <section class="section"><h2>Resource Chunks</h2>{records_table(compact_records_by_type['resource_chunk'], ['chunk', 'resource', 'source', 'kind', 'tokens', 'text'])}</section>
    <section class="section"><h2>Parent-To-Child Index</h2><p class="muted">Children are discovered by the narrow adjacency key <code>ctx:child:{{tenant_hash}}:{{parent_hash}}</code>. ContextNode records do not persist child counts.</p>{records_table(compact_records_by_type['context_child_ref'], ['index_key', 'parent', 'child', 'child_name', 'updated_at_ms', 'ref'])}</section>
    <section class="section"><h2>Placement / Data-Node Mapping Examples</h2><p class="muted">Placement keys route records to TemporalStore shards/data nodes. This compact report aliases full placement keys as <code>pN</code>; production uses metaserver slot placement.</p>{records_table(placement_routes, ['record_type', 'node', 'placement', 'placement_hash', 'example_shard_16', 'records'])}</section>
    <section class="section"><h2>Extracted Events</h2>{records_table(compact_records_by_type['context_event'], ['event', 'node', 'type', 'entity', 'source', 'text'])}</section>
    <section class="section"><h2>Extracted Entities</h2>{records_table(compact_records_by_type['context_entity'], ['entity', 'node', 'type', 'name', 'op', 'state', 'source'])}</section>
    <section class="section"><h2>Summaries</h2>{records_table(compact_records_by_type['context_summary'], ['type', 'summary', 'node', 'sources', 'text'])}</section>
    <section class="section"><h2>Node L0/L1 Generation Policy</h2>{records_table(summary_policy_rows, ['node', 'types', 'l1', 'reason', 'tokens', 'events', 'child_summaries'])}</section>
    <section class="section"><h2>Embedding Models</h2>{records_table(embedding_models, ['model', 'embedding_count'])}</section>
    <section class="section"><h2>Embeddings</h2><p class="muted">Latest serving embedding per ref. Full vectors stay out of the page.</p>{records_table(embeddings, ['type', 'ref', 'dim', 'preview'])}</section>
    <section class="section"><h2>Secondary Index Postings</h2><p class="muted">Grouped postings view. Resource fact postings show counts only; raw fact refs and batch-commit indexes stay out of this compact report.</p>{records_table(compact_records_by_type['context_index_postings'], ['model', 'index', 'node', 'ref_count', 'sample_refs'])}</section>
    <section class="section"><h2>Retrieval Scan And ContextPack</h2><p class="muted">Serving view only: grouped refs, citations, token summary, and warnings. Planner/audit fields stay out of the token-facing report.</p><pre>{html.escape(json.dumps(compact_pack, indent=2, sort_keys=True)[:20000])}</pre></section>
    <section class="section"><h2>Replay</h2><pre>{html.escape(json.dumps(compact_replay(replay_result), indent=2, sort_keys=True)[:12000])}</pre></section>
    <section class="section"><h2>Compact JSON</h2><p><a href="./matrixark_message_resource_debug_trace.json">Open compact JSON artifact</a>. Raw append/event logs are intentionally kept out of this compact report by default.</p></section>
  </main>
</body>
</html>
"""
    html_path.write_text(html_doc, encoding="utf-8")
    return json_path, md_path, html_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "docs" / "debug" / "matrixark_message_pdf_trace"),
        help="Directory for generated fixtures, event log, JSON, Markdown, and HTML.",
    )
    parser.add_argument("--max-context-tokens", type=int, default=1400)
    parser.add_argument("--pdf-count", type=int, default=len(PDF_FIXTURES), help="Number of mocked PDF fixtures to ingest.")
    parser.add_argument("--include-md-resources", action="store_true", help="Also ingest the Markdown fixtures.")
    parser.add_argument(
        "--include-debug-audit",
        action="store_true",
        help="Enable heavyweight context debug, replay, and summary-refresh audit records.",
    )
    args = parser.parse_args()
    mcp_core.ENABLE_CONTEXT_DEBUG_RECORDS = bool(args.include_debug_audit)
    mcp_core.ENABLE_CONTEXT_REPLAY = bool(args.include_debug_audit)
    mcp_core.ENABLE_SUMMARY_REFRESH_AUDIT = bool(args.include_debug_audit)

    if args.require_oss_understanding:
        os.environ["MATRIXARK_REQUIRE_OSS_UNDERSTANDING"] = "1"
    output_dir = Path(args.output_dir).resolve()
    fixture_dir = output_dir / "fixtures"
    event_log = output_dir / "matrixark_message_resource_debug_trace.jsonl"
    if event_log.exists():
        event_log.unlink()
    fixture_dir.mkdir(parents=True, exist_ok=True)

    adapter = MatrixArkLocalAdapter(event_log)
    server = MatrixArkMcpServer(adapter, access_mode="dev")
    scope = {
        "account_id": "acct_local",
        "tenant_id": "tenant_codex",
        "user_id": "local_user",
        "session_id": "debug-message-pdf-session",
        "agent_name": "codex",
    }
    message_node_path = [
        "tenant:tenant_codex",
        "user:local_user",
        "session:debug-message-pdf-session",
        "conversation:project_aurora",
    ]
    resource_node_path = [
        "tenant:tenant_codex",
        "user:local_user",
        "resources",
        "project_aurora",
        "gpu_procurement",
    ]

    trace: Json = {
        "scope": scope,
        "message_node_path": message_node_path,
        "resource_node_path": resource_node_path,
        "query": QUERY,
        "embedding_model": mcp_core.embedding_model_name(),
        "embedding_execution_mode": mcp_core.embedding_execution_mode_name(),
        "summary_refresh_policy": {
            "background_interval_ms": int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "1000")),
            "background_limit": int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_LIMIT", "64")),
            "boundary_refresh_tool": "matrixark_refresh_summaries",
            "node_l1_policy": "generate when child summaries, >=3 source events, or >=180 estimated source tokens",
        },
        "message_count": args.message_count,
        "resource_count": args.resource_count,
        "calls": [],
        "resources": [],
        "messages": [],
    }

    messages = generated_codex_messages(args.message_count)
    resources = expanded_resource_fixtures(args.resource_count)
    trace["messages"] = messages

    call_tool(server, "matrixark_backend_ready", {"scope": scope, "reason": "message_pdf_debug_trace"})
    for index, message in enumerate(messages, start=1):
        result = call_tool(
            server,
            "matrixark_ingest",
            {
                "kind": "message",
                "messages": [message],
                "scope": scope,
                "metadata": {
                    "node_path": message_node_path,
                    "source": "debug_trace",
                    "message_index": index,
                },
                "auto_batch_extract": True,
                "session_buffer_threshold": 20,
                "extraction_provider": args.extraction_provider,
                "understanding_provider": args.extraction_provider,
            },
        )
        trace["calls"].append({"tool": "matrixark_ingest", "kind": "message", "message_index": index, "result": result})

    commit_result = call_tool(
        server,
        "matrixark_session_commit",
        {
            "scope": scope,
            "metadata": {"node_path": message_node_path, "source": "debug_trace"},
            "force": True,
            "commit_reason": "manual_api",
            "threshold_messages": 20,
            "extraction_provider": args.extraction_provider,
            "understanding_provider": args.extraction_provider,
        },
    )
    trace["calls"].append({"tool": "matrixark_session_commit", "result": commit_result})

    for fixture in PDF_FIXTURES[: max(0, args.pdf_count)]:
        pdf_path = fixture_dir / fixture["filename"]
        write_pdf(pdf_path, str(fixture["title"]), list(fixture["lines"]))
        trace["resources"].append(
            {
                "raw_uri": str(resource_path),
                "resource_type": resource_type,
                "title": fixture["title"],
                "line_count": len(fixture["lines"]),
            }
        )
        result = call_tool(
            server,
            "matrixark_ingest",
            {
                "kind": "resource",
                "raw_uri": str(resource_path),
                "resource_type": resource_type,
                "messages": [{"role": "tool", "content": import_message}],
                "scope": scope,
                "metadata": {
                    "node_path": resource_node_path,
                    "source": "debug_trace",
                    "resource_title": fixture["title"],
                },
                "wait": True,
                "extraction_provider": args.extraction_provider,
                "understanding_provider": args.extraction_provider,
            },
        )
        trace["calls"].append({"tool": "matrixark_ingest", "kind": "resource", "resource_type": "pdf", "raw_uri": str(pdf_path), "result": result})

    for fixture in (MD_FIXTURES if args.include_md_resources else []):
        md_path = fixture_dir / fixture["filename"]
        md_path.write_text("\n".join(fixture["lines"]) + "\n", encoding="utf-8")
        trace["resources"].append(
            {
                "raw_uri": str(md_path),
                "resource_type": "md",
                "title": fixture["title"],
                "line_count": len(fixture["lines"]),
            }
        )
        result = call_tool(
            server,
            "matrixark_ingest",
            {
                "kind": "resource",
                "raw_uri": str(md_path),
                "resource_type": "md",
                "messages": [{"role": "tool", "content": "Import Markdown resource for MatrixArk parsing: " + str(fixture["title"])}],
                "scope": scope,
                "metadata": {
                    "node_path": resource_node_path,
                    "source": "debug_trace",
                    "resource_title": fixture["title"],
                },
                "wait": True,
            },
        )
        trace["calls"].append({"tool": "matrixark_ingest", "kind": "resource", "resource_type": "md", "raw_uri": str(md_path), "result": result})

    refresh_result = call_tool(
        server,
        "matrixark_refresh_summaries",
        {"scope": scope, "limit": 200},
    )
    trace["calls"].append({"tool": "matrixark_refresh_summaries", "result": refresh_result})

    retrieve_result = call_tool(
        server,
        "matrixark_retrieve",
        {
            "query": QUERY,
            "scope": scope,
            "max_context_tokens": args.max_context_tokens,
            "audit_mode": "full" if args.include_debug_audit else "off",
            "debug_context_pack": True,
            "ranking": {
                "weights": {"time": 0.15, "business": 0.1},
                "business_type_weights": {"approval": 1.0, "deadline": 0.95, "policy": 0.9, "procedure": 0.9},
                "auxiliary_quota": 6,
            },
        },
    )
    trace["calls"].append(
        {
            "tool": "matrixark_retrieve",
            "result": retrieve_result if args.include_debug_audit else mcp_core.compact_context_pack_for_serving(retrieve_result),
        }
    )

    replay_result = call_tool(
        server,
        "matrixark_replay",
        {
            "scope": scope,
            "context_pack_id": retrieve_result.get("context_pack_id", ""),
            "enable_replay": True,
        },
    )
    trace["calls"].append({"tool": "matrixark_replay", "result": replay_result})

    records = read_records(event_log)
    json_path, md_path, html_path = write_outputs(
        output_dir=output_dir,
        event_log=event_log,
        trace=trace,
        records=records,
        retrieve_result=retrieve_result,
        replay_result=replay_result,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "event_log": str(event_log),
                "json": str(json_path),
                "markdown": str(md_path),
                "html": str(html_path),
                "record_count": len(records),
                "selected_refs": mcp_core.selected_ref_count_from_pack(retrieve_result),
                "used_context_tokens": retrieve_result.get("used_context_tokens") or retrieve_result.get("tokens", {}).get("remote"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
