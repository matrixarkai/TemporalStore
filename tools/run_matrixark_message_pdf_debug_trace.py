#!/usr/bin/env python3
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
        canvas_obj.drawString(72, y, line)
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
    marker = "/fixtures/"
    if marker in text:
        text = text.split(marker, 1)[1]
    else:
        text = text.replace(str(REPO_ROOT), "<repo>")
    return text


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


def compact_resource_chunk(record: Json) -> Json:
    metadata = record.get("metadata", {}) if isinstance(record.get("metadata"), dict) else {}
    return {
        "chunk": record.get("chunk_hash"),
        "resource": record.get("resource_hash"),
        "source": short_source(record.get("source_ref") or metadata.get("citation") or record.get("source_locator")),
        "kind": metadata.get("unit_kind") or record.get("unit_kind") or record.get("resource_type"),
        "tokens": record.get("token_estimate", 0),
        "text": record.get("text", ""),
    }


def compact_context_event(record: Json) -> Json:
    event_type = first_present(record, "event_type", "internal_extraction.event_type")
    entity_type = first_present(record, "entity_type", "internal_extraction.entity_type")
    classification = record.get("classification")
    row: Json = {
        "event": record.get("event_id_hash"),
        "node": record.get("node_hash") or short_node_path(record.get("node_path")),
        "type": event_type,
        "entity": entity_type,
        "source": short_source(first_present(record, "source_ref", "source_locator")),
        "text": first_present(record, "summary_text", "text"),
    }
    if classification not in ("", None, "NEW_EVENT"):
        row["class"] = classification
    return {key: value for key, value in row.items() if value not in ("", None, [], {})}


def compact_context_entity(record: Json) -> Json:
    return {
        "entity": record.get("entity_hash"),
        "node": record.get("node_hash") or short_node_path(record.get("node_path")),
        "type": record.get("entity_type", ""),
        "name": record.get("entity_name", ""),
        "op": record.get("operator", ""),
        "state": record.get("state", ""),
        "source": short_source(first_present(record, "source_ref", "source_locator")),
    }


def compact_context_summary(record: Json) -> Json:
    return {
        "type": record.get("summary_type", ""),
        "summary": record.get("summary_hash"),
        "node": record.get("node_hash") or short_node_path(record.get("node_path")),
        "text": record.get("summary_text", ""),
        "sources": len(record.get("source_chunk_hashes") or record.get("source_event_ids") or []),
    }


def compact_context_embedding(record: Json) -> Json:
    preview = vector_preview(record)
    return {
        "type": record.get("embedding_type", ""),
        "ref": f"{record.get('ref_type', '')}:{record.get('ref_hash', '')}",
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


def compact_summary_policy(record: Json) -> Json:
    policy = record.get("summary_generation_policy", {}) if isinstance(record.get("summary_generation_policy"), dict) else {}
    return {
        "node": record.get("node_hash") or short_node_path(record.get("node_path")),
        "types": record.get("generated_summary_types", []),
        "l1": policy.get("generate_l1", ""),
        "reason": policy.get("reason", ""),
        "tokens": policy.get("token_estimate", ""),
        "events": record.get("source_event_count", 0),
        "child_summaries": record.get("source_summary_count", 0),
    }


def compact_context_indexes(records: list[Json]) -> list[Json]:
    postings: dict[tuple[str, str, Any, Any], set[Any]] = {}
    for record in records:
        key = (
            str(record.get("data_model") or record.get("ref_type") or ""),
            str(record.get("index_name") or ""),
            record.get("timestamp_key_ms") or record.get("updated_at_ms") or "",
            record.get("node_hash") or "",
        )
        refs = postings.setdefault(key, set())
        for ref in record.get("ref_hashes") or []:
            refs.add(ref)
        ref_hash = record.get("ref_hash") or record.get("event_id_hash") or record.get("chunk_hash")
        if ref_hash not in ("", None):
            refs.add(ref_hash)
    rows = []
    for (model, index_name, timestamp_key, node_hash), refs in sorted(postings.items(), key=lambda item: (item[0][0], item[0][1], str(item[0][2]))):
        rows.append(
            {
                "model": model,
                "index": index_name,
                "time": timestamp_key,
                "node": node_hash,
                "refs": len(refs),
                "sample": list(sorted(refs, key=str))[:3],
            }
        )
    return rows


def compact_replay(result: Json) -> Json:
    if not result:
        return {}
    return {
        key: result.get(key)
        for key in ("status", "context_pack_id", "event_count", "replay_event_count", "warning")
        if result.get(key) not in ("", None, [], {})
    }


def compact_tool_result(result: Json) -> Json:
    if not isinstance(result, dict):
        return {}
    return {
        key: result.get(key)
        for key in (
            "status",
            "context_pack_id",
            "pack_id",
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
    if not isinstance(vector, list):
        return {"dim": record.get("dim", 0), "preview": []}
    return {
        "dim": len(vector),
        "preview": [round(float(value), 5) for value in vector[:8]],
    }


def read_records(path: Path) -> list[Json]:
    records: list[Json] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


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


def render_node_html(node: Json) -> str:
    label = "/".join(str(part) for part in node.get("path", [])) or str(node.get("name") or node.get("node_hash"))
    record_obj = node.get("record", {}) if isinstance(node.get("record"), dict) else {}
    compact_record = {
        "node": record_obj.get("node_hash"),
        "parent": record_obj.get("parent_hash"),
        "name": record_obj.get("node_name"),
        "children": len(node.get("children", [])),
    }
    record = html.escape(json.dumps(compact_record, indent=2, sort_keys=True))
    children = "\n".join(render_node_html(child) for child in sorted(node.get("children", []), key=lambda item: item["name"]))
    return (
        "<details open class=\"node\">"
        f"<summary><span class=\"node-name\">{html.escape(label)}</span> "
        f"<span class=\"muted\">hash={node.get('node_hash')}</span></summary>"
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

    counts = Counter(str(record.get("record_type", "unknown")) for record in records)
    by_type: dict[str, list[Json]] = defaultdict(list)
    for record in records:
        by_type[str(record.get("record_type", "unknown"))].append(record)

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
        compact_context_embedding(record)
        for record in current_embedding_records
    ]
    summary_policy_rows = [compact_summary_policy(record) for record in by_type["context_summary_refresh_audit"]]
    compact_records_by_type: dict[str, list[Json]] = {
        "context_node": [
            {
                "node": record.get("node_hash"),
                "parent": record.get("parent_hash"),
                "name": record.get("node_name") or record.get("name"),
                "path": short_node_path(record.get("node_path")),
            }
            for record in by_type["context_node"]
        ],
        "context_event": [compact_context_event(record) for record in by_type["context_event"]],
        "context_entity": [compact_context_entity(record) for record in by_type["context_entity"]],
        "context_summary": [compact_context_summary(record) for record in latest_by_key(by_type["context_summary"], ["summary_type", "summary_hash", "node_hash"])],
        "context_embedding": embeddings,
        "context_index_postings": compact_context_indexes(by_type["context_index"]),
        "resource_import_task": [compact_import_task(record) for record in by_type["resource_import_task"]],
        "resource_chunk": [compact_resource_chunk(record) for record in by_type["resource_chunk"]],
    }
    compact_pack = mcp_core.compact_context_pack_for_serving(retrieve_result)

    exported = {
        "trace": compact_trace(trace),
        "record_counts": dict(counts),
        "context_pack": compact_pack,
        "replay": compact_replay(replay_result),
        "records_by_type": compact_records_by_type,
        "raw_event_log": str(event_log),
        "embeddings": embeddings,
        "summary_generation_policy": summary_policy_rows,
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
        f"- Event log: `{event_log}`",
        f"- Embedding model: `{trace['embedding_model']}`",
        f"- Embedding execution mode: `{trace['embedding_execution_mode']}`",
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
        "## Extracted Events",
        "",
        markdown_table(compact_records_by_type["context_event"], ["event", "node", "type", "entity", "source", "text"], limit=80),
        "",
        "## Extracted Entities",
        "",
        markdown_table(compact_records_by_type["context_entity"], ["entity", "node", "type", "name", "op", "state", "source"], limit=80),
        "",
        "## Summaries",
        "",
        markdown_table(compact_records_by_type["context_summary"], ["type", "summary", "node", "sources", "text"], limit=80),
        "",
        "## Node L0/L1 Generation Policy",
        "",
        markdown_table(summary_policy_rows, ["node", "types", "l1", "reason", "tokens", "events", "child_summaries"], limit=80),
        "",
        "## Embeddings",
        "",
        markdown_table(embedding_models, ["model", "embedding_count"], limit=20),
        "",
        markdown_table(embeddings, ["type", "ref", "dim", "preview"], limit=120),
        "",
        "## Secondary Index Postings",
        "",
        markdown_table(compact_records_by_type["context_index_postings"], ["model", "index", "time", "node", "refs", "sample"], limit=120),
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
                "context_pack_id": retrieve_result.get("context_pack_id"),
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
    graph_html = "\n".join(render_node_html(root) for root in roots) or "<p>No context_node records found.</p>"
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
    <p><span class="pill">{html.escape(trace['embedding_model'])}</span><span class="pill">{html.escape(trace['embedding_execution_mode'])}</span><span class="pill">Summary refresh: background interval {trace['summary_refresh_policy']['background_interval_ms']} ms</span><span class="pill">Limit {trace['summary_refresh_policy']['background_limit']} dirty nodes/tick</span></p>
    <p class="muted">Node L1 policy: {html.escape(trace['summary_refresh_policy']['node_l1_policy'])}</p>
    <p class="muted">Embedding note: {html_embedding_note}</p>
  </header>
  <main>
    <section class="grid">
      <div class="metric"><span class="muted">Records</span><strong>{len(records)}</strong></div>
      <div class="metric"><span class="muted">Events</span><strong>{counts.get('context_event', 0)}</strong></div>
      <div class="metric"><span class="muted">Entities</span><strong>{counts.get('context_entity', 0)}</strong></div>
      <div class="metric"><span class="muted">Chunks</span><strong>{counts.get('resource_chunk', 0)}</strong></div>
      <div class="metric"><span class="muted">Embeddings</span><strong>{counts.get('context_embedding', 0)}</strong></div>
      <div class="metric"><span class="muted">Selected Refs</span><strong>{len(retrieve_result.get('selected_refs', []))}</strong></div>
    </section>
    <section class="section"><h2>Pipeline</h2><pre>{html.escape(PIPELINE_MERMAID)}</pre></section>
    <section class="section"><h2>Data Model Field Guide</h2>{model_table_html}</section>
    <section class="section"><h2>ContextNode Graph</h2>{graph_html}</section>
    <section class="section"><h2>Messages</h2>{records_table([{'role': m['role'], 'content': m['content']} for m in MESSAGES], ['role', 'content'])}</section>
    <section class="section"><h2>Resources</h2>{records_table(compact_resources(trace['resources']), ['rid', 'type', 'title', 'source', 'lines'])}</section>
    <section class="section"><h2>Resource Import Tasks</h2>{records_table(compact_records_by_type['resource_import_task'], ['status', 'type', 'source', 'chunks', 'facts', 'entities'])}</section>
    <section class="section"><h2>Resource Chunks</h2>{records_table(compact_records_by_type['resource_chunk'], ['chunk', 'resource', 'source', 'kind', 'tokens', 'text'])}</section>
    <section class="section"><h2>Extracted Events</h2>{records_table(compact_records_by_type['context_event'], ['event', 'node', 'type', 'entity', 'source', 'text'])}</section>
    <section class="section"><h2>Extracted Entities</h2>{records_table(compact_records_by_type['context_entity'], ['entity', 'node', 'type', 'name', 'op', 'state', 'source'])}</section>
    <section class="section"><h2>Summaries</h2>{records_table(compact_records_by_type['context_summary'], ['type', 'summary', 'node', 'sources', 'text'])}</section>
    <section class="section"><h2>Node L0/L1 Generation Policy</h2>{records_table(summary_policy_rows, ['node', 'types', 'l1', 'reason', 'tokens', 'events', 'child_summaries'])}</section>
    <section class="section"><h2>Embedding Models</h2>{records_table(embedding_models, ['model', 'embedding_count'])}</section>
    <section class="section"><h2>Embeddings</h2><p class="muted">Latest serving embedding per ref. Full vectors stay out of the page.</p>{records_table(embeddings, ['type', 'ref', 'dim', 'preview'])}</section>
    <section class="section"><h2>Secondary Index Postings</h2><p class="muted">Grouped postings view. The raw event log can still be opened when forensic detail is needed.</p>{records_table(compact_records_by_type['context_index_postings'], ['model', 'index', 'time', 'node', 'refs', 'sample'])}</section>
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
        "user_id": "deeproute",
        "session_id": "debug-message-pdf-session",
        "agent_name": "codex",
    }
    message_node_path = [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora",
    ]
    resource_node_path = [
        "tenant:tenant_codex",
        "user:deeproute",
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
        "calls": [],
        "resources": [],
    }

    call_tool(server, "matrixark_backend_ready", {"scope": scope, "reason": "message_pdf_debug_trace"})
    for index, message in enumerate(MESSAGES, start=1):
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
        },
    )
    trace["calls"].append({"tool": "matrixark_session_commit", "result": commit_result})

    for fixture in PDF_FIXTURES[: max(0, args.pdf_count)]:
        pdf_path = fixture_dir / fixture["filename"]
        write_pdf(pdf_path, str(fixture["title"]), list(fixture["lines"]))
        trace["resources"].append(
            {
                "raw_uri": str(pdf_path),
                "resource_type": "pdf",
                "title": fixture["title"],
                "line_count": len(fixture["lines"]),
            }
        )
        result = call_tool(
            server,
            "matrixark_ingest",
            {
                "kind": "resource",
                "raw_uri": str(pdf_path),
                "resource_type": "pdf",
                "messages": [{"role": "tool", "content": "Import PDF resource for MatrixArk parsing: " + str(fixture["title"])}],
                "scope": scope,
                "metadata": {
                    "node_path": resource_node_path,
                    "source": "debug_trace",
                    "resource_title": fixture["title"],
                },
                "wait": True,
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
