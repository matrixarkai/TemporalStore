#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from tools.matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer

Json = dict[str, Any]


def call_tool(server: MatrixArkMcpServer, name: str, arguments: Json) -> Json:
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    )
    if "error" in response:
        raise RuntimeError(response["error"]["message"])
    return json.loads(response["result"]["content"][0]["text"])


def model_counts(records: list[Json]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        record_type = str(record.get("record_type", "unknown"))
        counts[record_type] = counts.get(record_type, 0) + 1
    return dict(sorted(counts.items()))


def compact_records(records: list[Json], record_type: str, limit: int = 12) -> list[Json]:
    out = []
    for record in records:
        if record.get("record_type") != record_type:
            continue
        keep = {
            key: record.get(key)
            for key in [
                "record_type",
                "event_id_hash",
                "entity_hash",
                "segment_hash",
                "summary_hash",
                "index_name",
                "embedding_type",
                "summary_type",
                "node_hash",
                "node_path",
                "entity_type",
                "entity_name",
                "state",
                "previous_state",
                "topic",
                "coordinate_tuples",
                "message_indexes",
                "source_event_ids",
                "source_refs",
                "text",
                "summary_text",
            ]
            if key in record
        }
        if "vector" in record:
            vector = record.get("vector", [])
            keep["vector_preview"] = vector[:6]
            keep["vector_dim"] = len(vector)
        out.append(keep)
        if len(out) >= limit:
            break
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LOCOMO-style MatrixArk debug flow and persist data-model artifacts.")
    parser.add_argument("--artifact-dir", default="", help="Directory for JSONL/JSON/Markdown/HTML artifacts.")
    parser.add_argument("--event-log", default="", help="Explicit event-log JSONL path.")
    return parser.parse_args()


def run(artifact_dir: Path | None = None, event_log_path: str = "") -> Json:
    tmpdir = None
    if artifact_dir is not None:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        event_log = Path(event_log_path) if event_log_path else artifact_dir / "matrixark_locomo_debug_event_log.jsonl"
        event_log.unlink(missing_ok=True)
    else:
        tmpdir = tempfile.TemporaryDirectory()
        event_log = Path(event_log_path) if event_log_path else Path(tmpdir.name) / "matrixark-locomo-debug.jsonl"
    server = MatrixArkMcpServer(MatrixArkLocalAdapter(event_log))

    sessions = [
        {
            "name": "conversation_a_location_preference_approval",
            "scope": {"account_id": "acct_locomo_debug", "tenant_id": "tenant_memory", "user_id": "locomo_user_a", "session_id": "locomo_a"},
            "messages": [
                "I moved to Seattle today, please remember this location.",
                "Actually I moved to Austin now for the new infra project.",
                "I prefer Rust for low latency storage engines.",
                "Alice approved the GPU purchase after finance reviewed the budget.",
            ],
            "queries": [
                "Where is the user currently located?",
                "What does the user prefer for low latency storage?",
                "Who approved the GPU purchase?",
            ],
        },
        {
            "name": "conversation_b_relationship_family_job",
            "scope": {"account_id": "acct_locomo_debug", "tenant_id": "tenant_memory", "user_id": "locomo_user_b", "session_id": "locomo_b"},
            "messages": [
                "My manager Priya is helping with the launch plan.",
                "My family has a dog named Mochi.",
                "My job role is storage infrastructure lead.",
                "I plan to visit Berlin next month for the conference.",
            ],
            "queries": [
                "Who is the user's manager?",
                "What pet is in the user's family?",
                "What is the user's current job role?",
            ],
        },
        {
            "name": "conversation_c_temporal_update",
            "scope": {"account_id": "acct_locomo_debug", "tenant_id": "tenant_memory", "user_id": "locomo_user_c", "session_id": "locomo_c"},
            "messages": [
                "On March 2 I lived in Seattle.",
                "On April 10 I moved to Austin.",
                "I liked Python for dashboards before.",
                "Actually I now prefer Rust for backend services.",
            ],
            "queries": [
                "Where is the user currently located?",
                "What language does the user currently prefer?",
                "Where was the user before April 10?",
            ],
        },
    ]

    session_results = []
    query_results = []
    for session in sessions:
        ingests = []
        for idx, text in enumerate(session["messages"]):
            ingests.append(
                call_tool(
                    server,
                    "matrixark_ingest",
                    {
                        "messages": [{"role": "user", "content": text}],
                        "scope": session["scope"],
                        "metadata": {"source": "locomo_debug", "message_index": idx},
                        "agent_hook": {
                            "source": "locomo_debug_runner",
                            "hook_type": "before_llm",
                            "hook_id": f"{session['name']}:{idx}",
                            "observed_at_ms": 1781500000000 + idx,
                            "auto_captured": True,
                        },
                    },
                )
            )
        commit = call_tool(
            server,
            "matrixark_session_commit",
            {
                "scope": session["scope"],
                "threshold_messages": 20,
                "force": True,
                "agent_hook": {
                    "source": "locomo_debug_runner",
                    "hook_type": "session_commit",
                    "hook_id": f"{session['name']}:commit",
                    "observed_at_ms": 1781500100000,
                    "auto_captured": True,
                },
            },
        )
        summary_refresh = call_tool(
            server,
            "matrixark_refresh_summaries",
            {
                "scope": session["scope"],
                "limit": 100,
            },
        )
        session_results.append(
            {
                "name": session["name"],
                "scope": session["scope"],
                "ingests": ingests,
                "commit": commit,
                "summary_refresh": summary_refresh,
            }
        )
        for query in session["queries"]:
            pack = call_tool(
                server,
                "matrixark_retrieve",
                {"query": query, "scope": session["scope"], "max_context_tokens": 80},
            )
            query_results.append(
                {
                    "session": session["name"],
                    "query": query,
                    "context_pack_id": pack["context_pack_id"],
                    "question_type": pack.get("question_type"),
                    "selected_refs": pack.get("selected_refs", [])[:5],
                    "recall_policy": pack.get("recall_policy", {}),
                    "used_context_tokens": pack.get("used_context_tokens"),
                    "insufficient_context": pack.get("insufficient_context"),
                }
            )

    records = [json.loads(line) for line in event_log.read_text().splitlines() if line.strip()]
    debug = {
        "dataset_note": "LOCOMO-style sample conversations; no official LOCOMO dataset file was present in this repo at run time.",
        "event_log": str(event_log),
        "embedding_provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic"),
        "embedding_model": os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get("MATRIXARK_EMBEDDING_MODEL", "matrixark-local-token-hash-v1"),
        "segment_provider": "deterministic unless batch calls specify segment_provider=oss",
        "model_counts": model_counts(records),
        "sessions": session_results,
        "queries": query_results,
        "records_by_model": {
            "context_event": compact_records(records, "context_event", 20),
            "session_buffer_event": compact_records(records, "session_buffer_event", 20),
            "context_batch_commit": compact_records(records, "context_batch_commit", 10),
            "context_segment": compact_records(records, "context_segment", 20),
            "context_entity": compact_records(records, "context_entity", 20),
            "context_summary": compact_records(records, "context_summary", 20),
            "context_embedding": compact_records(records, "context_embedding", 20),
            "context_index": compact_records(records, "context_index", 40),
            "context_pack_audit": compact_records(records, "context_pack_audit", 20),
        },
    }
    if artifact_dir is not None:
        write_artifacts(debug, artifact_dir)
    if tmpdir is not None:
        tmpdir.cleanup()
    return debug



def write_artifacts(debug: Json, artifact_dir: Path) -> None:
    json_path = artifact_dir / "matrixark_locomo_debug_data_flow.json"
    md_path = artifact_dir / "matrixark_locomo_debug_data_flow.md"
    html_path = artifact_dir / "matrixark_locomo_debug_data_flow.html"
    json_path.write_text(json.dumps(debug, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# MatrixArk LOCOMO Debug Data Flow",
        "",
        f"Event log: `{debug['event_log']}`",
        f"Embedding provider: `{debug['embedding_provider']}`",
        f"Embedding model: `{debug['embedding_model']}`",
        "",
        "## Data Model Counts",
        "",
    ]
    for key, value in debug.get("model_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Retrieval Queries", ""])
    for query in debug.get("queries", []):
        lines.append(f"### {query['query']}")
        lines.append(f"- session: `{query['session']}`")
        lines.append(f"- question_type: `{query.get('question_type')}`")
        lines.append(f"- context_pack_id: `{query.get('context_pack_id')}`")
        lines.append(f"- used_context_tokens: `{query.get('used_context_tokens')}`")
        tree = query.get("recall_policy", {}).get("tree_traversal", {})
        lines.append(f"- tree traversal: selected_nodes={tree.get('selected_node_count')} selected_paths={tree.get('selected_path_count')} fallback={tree.get('fallback_to_flat')}")
        lines.append("- selected refs:")
        for ref in query.get("selected_refs", []):
            lines.append(f"  - `{ref.get('ref_type')}` score={ref.get('score')} node={ref.get('node_path')} text={str(ref.get('text', ''))[:180]}")
        lines.append("")
    lines.extend(["", "## Compact Records By Model", ""])
    for model, rows in debug.get("records_by_model", {}).items():
        lines.append(f"### {model}")
        lines.append("```json")
        lines.append(json.dumps(rows, indent=2, sort_keys=True))
        lines.append("```")
        lines.append("")
    md = "\n".join(lines)
    md_path.write_text(md + "\n", encoding="utf-8")
    html_path.write_text(
        "<!doctype html><meta charset='utf-8'><title>MatrixArk LOCOMO Debug Data Flow</title>"
        "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1180px;margin:32px auto;padding:0 24px;line-height:1.45;color:#172033}pre{background:#f5f7fb;padding:16px;overflow:auto;border-radius:8px}code{background:#edf1f7;padding:2px 4px;border-radius:4px}h1,h2,h3{color:#0f172a}</style>"
        + "<pre>" + html.escape(md) + "</pre>",
        encoding="utf-8",
    )


if __name__ == "__main__":
    args = parse_args()
    artifact_dir = Path(args.artifact_dir) if args.artifact_dir else None
    print(json.dumps(run(artifact_dir=artifact_dir, event_log_path=args.event_log), indent=2, sort_keys=True))
