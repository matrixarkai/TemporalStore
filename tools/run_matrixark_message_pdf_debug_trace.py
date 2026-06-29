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
    embedding_execution_mode_name,
    embedding_model_name,
)


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
    record = html.escape(json.dumps(node.get("record", {}), indent=2, sort_keys=True))
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
    {"model": "ContextNode", "purpose": "Filesystem-like topology. Messages/resources attach to a leaf node, parents are used for traversal.", "important_fields": "node_hash, parent_hash, node_name, node_path, depth, scope_key"},
    {"model": "ContextEvent", "purpose": "Replayable extracted fact or raw conversational event.", "important_fields": "event_id_hash, node_hash, source_chunk_hash, resource_hash, source_locator, summary_text, event_type, entity_type, timestamp"},
    {"model": "ContextSegment", "purpose": "Batch/session topic segment when a logical window is committed.", "important_fields": "segment_hash, node_hash, source_event_ids, summary_text, topic, time_range"},
    {"model": "ContextEntity", "purpose": "Evolving state for current preference/status/owner/budget/deadline.", "important_fields": "entity_hash, entity_type, entity_name, state, source_chunk_hash, resource_hash, source_locator, valid_from, stale_blockers"},
    {"model": "ResourceManifest", "purpose": "Logical imported file/resource version. Raw bytes stay outside TemporalStore.", "important_fields": "resource_hash, raw_uri, resource_type, resource_version, content_hash, scope_key"},
    {"model": "ResourceChunk", "purpose": "Cited serving chunk from PDF/MD/etc. Full raw_uri lives on ResourceManifest; chunks carry resource_hash plus source_locator.", "important_fields": "chunk_hash, resource_hash, source_locator, text, token_estimate, unit_kind, page_number, heading_slug"},
    {"model": "ContextSummary", "purpose": "L0/L1 node/resource summary used for preview and tree traversal.", "important_fields": "summary_hash, summary_type, node_hash, summary_text, source_event_ids, source_chunk_hashes"},
    {"model": "ContextEmbedding", "purpose": "Vector stored separately for summaries, chunks, events, entities, and resources.", "important_fields": "embedding_type, ref_type, ref_hash, model, dim, vector"},
    {"model": "ContextIndex", "purpose": "Bounded secondary filters before similarity scoring.", "important_fields": "index_name, index_value, ref_type, ref_hash, node_hash, chunk_hash"},
    {"model": "ContextPackAudit", "purpose": "Explains selected/dropped refs, scores, token costs, warnings, and replay path.", "important_fields": "context_pack_id, selected_refs, dropped_refs, used_context_tokens, quality_warnings"},
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
        {
            "embedding_type": record.get("embedding_type"),
            "ref_type": record.get("ref_type"),
            "ref_hash": record.get("ref_hash"),
            "node_path": record.get("node_path"),
            **vector_preview(record),
        }
        for record in current_embedding_records
    ]
    summary_policy_rows = [
        {
            "node_path": record.get("node_path", []),
            "generated_summary_types": record.get("generated_summary_types", []),
            "l1_policy": record.get("summary_generation_policy", {}),
            "source_event_count": record.get("source_event_count", 0),
            "source_summary_count": record.get("source_summary_count", 0),
        }
        for record in by_type["context_summary_refresh_audit"]
    ]

    exported = {
        "trace": trace,
        "record_counts": dict(counts),
        "retrieve_result": retrieve_result,
        "replay_result": replay_result,
        "records_by_type": by_type,
        "embeddings": embeddings,
        "summary_generation_policy": summary_policy_rows,
        "event_log": str(event_log),
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
        markdown_table(trace["resources"], ["raw_uri", "title", "line_count"], limit=20),
        "",
        "## Resource Import Tasks",
        "",
        markdown_table(by_type["resource_import_task"], ["status", "raw_uri", "resource_type", "chunk_count", "resource_fact_count", "resource_entity_count", "metrics"], limit=50),
        "",
        "## Resource Chunks",
        "",
        markdown_table(by_type["resource_chunk"], ["chunk_hash", "resource_hash", "source_locator", "token_estimate", "metadata.unit_kind", "metadata.content_hash", "text"], limit=80),
        "",
        "## Extracted Events",
        "",
        markdown_table(by_type["context_event"], ["event_id_hash", "node_path", "internal_extraction.event_type", "internal_extraction.entity_type", "summary_text", "source_chunk_hash", "resource_hash", "source_locator"], limit=80),
        "",
        "## Extracted Entities",
        "",
        markdown_table(by_type["context_entity"], ["entity_hash", "node_path", "entity_type", "entity_name", "operator", "state", "source_chunk_hash", "resource_hash", "source_locator"], limit=80),
        "",
        "## Summaries",
        "",
        markdown_table(by_type["context_summary"], ["summary_type", "summary_hash", "node_path", "summary_generation_policy.reason", "summary_text", "source_chunk_hashes"], limit=80),
        "",
        "## Node L0/L1 Generation Policy",
        "",
        markdown_table(summary_policy_rows, ["node_path", "generated_summary_types", "l1_policy.generate_l1", "l1_policy.reason", "l1_policy.token_estimate", "source_event_count", "source_summary_count"], limit=80),
        "",
        "## Embeddings",
        "",
        markdown_table(embedding_models, ["model", "embedding_count"], limit=20),
        "",
        markdown_table(embeddings, ["embedding_type", "ref_type", "ref_hash", "dim", "preview"], limit=120),
        "",
        "## Secondary Indexes",
        "",
        markdown_table(by_type["context_index"], ["index_name", "ref_type", "ref_hash", "chunk_hash", "node_path"], limit=120),
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
                "recall_policy": retrieve_result.get("recall_policy"),
                "selected_refs": retrieve_result.get("selected_refs"),
                "dropped_refs": retrieve_result.get("dropped_refs"),
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
        json.dumps(retrieve_result, indent=2, sort_keys=True)[:20000],
        "```",
        "",
        "## Replay",
        "",
        "```json",
        json.dumps(replay_result, indent=2, sort_keys=True)[:12000],
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
    <section class="section"><h2>Resources</h2>{records_table(trace['resources'], ['raw_uri', 'title', 'line_count'])}</section>
    <section class="section"><h2>Resource Import Tasks</h2>{records_table(by_type['resource_import_task'], ['status', 'raw_uri', 'resource_type', 'chunk_count', 'resource_fact_count', 'resource_entity_count', 'metrics'])}</section>
    <section class="section"><h2>Resource Chunks</h2>{records_table(by_type['resource_chunk'], ['chunk_hash', 'resource_hash', 'source_locator', 'token_estimate', 'metadata.unit_kind', 'metadata.content_hash', 'text'])}</section>
    <section class="section"><h2>Extracted Events</h2>{records_table(by_type['context_event'], ['event_id_hash', 'node_path', 'internal_extraction.event_type', 'internal_extraction.entity_type', 'summary_text', 'source_chunk_hash', 'resource_hash', 'source_locator'])}</section>
    <section class="section"><h2>Extracted Entities</h2>{records_table(by_type['context_entity'], ['entity_hash', 'node_path', 'entity_type', 'entity_name', 'operator', 'state', 'source_chunk_hash', 'resource_hash', 'source_locator'])}</section>
    <section class="section"><h2>Summaries</h2>{records_table(by_type['context_summary'], ['summary_type', 'summary_hash', 'node_path', 'summary_generation_policy.reason', 'summary_text', 'source_chunk_hashes'])}</section>
    <section class="section"><h2>Node L0/L1 Generation Policy</h2>{records_table(summary_policy_rows, ['node_path', 'generated_summary_types', 'l1_policy.generate_l1', 'l1_policy.reason', 'l1_policy.token_estimate', 'source_event_count', 'source_summary_count'])}</section>
    <section class="section"><h2>Embedding Models</h2>{records_table(embedding_models, ['model', 'embedding_count'])}</section>
    <section class="section"><h2>Embeddings</h2><p class="muted">Latest serving embedding per embedding_type/ref_type/ref_hash. Historical refresh rows stay in audit/debug logs.</p>{records_table(embeddings, ['embedding_type', 'ref_type', 'ref_hash', 'dim', 'preview'])}</section>
    <section class="section"><h2>Secondary Indexes</h2>{records_table(by_type['context_index'], ['index_name', 'ref_type', 'ref_hash', 'chunk_hash', 'node_path'])}</section>
    <section class="section"><h2>Retrieval Scan And ContextPack</h2><p class="muted">Query understanding runs first, then scope and secondary-index filters, then ContextNode L0/L1 summary embeddings choose the folders. MatrixArk fetches leaf segments, events, entities, resource chunks, and skill sections from selected nodes before final packing.</p><pre>{html.escape(json.dumps(retrieve_result, indent=2, sort_keys=True)[:60000])}</pre></section>
    <section class="section"><h2>Replay</h2><pre>{html.escape(json.dumps(replay_result, indent=2, sort_keys=True)[:30000])}</pre></section>
    <section class="section"><h2>Raw Trace JSON</h2><p><a href="./matrixark_message_resource_debug_trace.json">Open JSON artifact</a></p></section>
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
    args = parser.parse_args()

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
        "embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
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

    for fixture in PDF_FIXTURES:
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

    for fixture in MD_FIXTURES:
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
            "ranking": {
                "weights": {"time": 0.15, "business": 0.1},
                "business_type_weights": {"approval": 1.0, "deadline": 0.95, "policy": 0.9, "procedure": 0.9},
                "auxiliary_quota": 6,
            },
        },
    )
    trace["calls"].append({"tool": "matrixark_retrieve", "result": retrieve_result})

    replay_result = call_tool(
        server,
        "matrixark_replay",
        {
            "scope": scope,
            "context_pack_id": retrieve_result.get("context_pack_id", ""),
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
                "selected_refs": len(retrieve_result.get("selected_refs", [])),
                "used_context_tokens": retrieve_result.get("used_context_tokens"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
