#!/usr/bin/env python3
from __future__ import annotations

import json
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


def run() -> Json:
    tmpdir = tempfile.TemporaryDirectory()
    event_log = Path(tmpdir.name) / "matrixark-locomo-debug.jsonl"
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
    tmpdir.cleanup()
    return debug


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, sort_keys=True))
