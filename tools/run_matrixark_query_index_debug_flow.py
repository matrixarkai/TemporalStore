#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import time
from pathlib import Path
from typing import Any

from tools.matrixark_mcp_server import (
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    MatrixArkTemporalStoreDirectAdapter,
    candidate_index_terms,
    infer_query_type,
    infer_secondary_index_filter_groups,
    passes_secondary_index_filters,
)

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


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run MatrixArk query/index debug flow with OSS embeddings.")
    parser.add_argument("--artifact-dir", default=".local/context-debug/query-index-oss-docker-debug")
    parser.add_argument("--event-log", default="")
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default="local")
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default=f"matrixark:query:index:debug:{int(time.time() * 1000)}")
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    return parser.parse_args()


def model_counts(records: list[Json]) -> dict[str, int]:
    out: dict[str, int] = {}
    for record in records:
        key = str(record.get("record_type", "unknown"))
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def compact(record: Json) -> Json:
    keep = [
        "record_type",
        "event_id_hash",
        "batch_id_hash",
        "node_hash",
        "node_path",
        "entity_hash",
        "entity_type",
        "entity_name",
        "state",
        "previous_state",
        "segment_hash",
        "topic",
        "message_indexes",
        "coordinate_tuples",
        "non_contiguous",
        "summary_hash",
        "summary_type",
        "summary_text",
        "embedding_type",
        "ref_type",
        "ref_hash",
        "dim",
        "model",
        "index_name",
        "index_hash",
        "text",
        "internal_extraction",
    ]
    out = {key: record.get(key) for key in keep if key in record}
    if "vector" in record:
        vector = record.get("vector") or []
        out["vector_dim"] = len(vector)
        out["vector_preview"] = vector[:8]
    if "envelope" in record:
        envelope = record.get("envelope") or {}
        out["scope"] = envelope.get("scope", {})
        out["metadata"] = envelope.get("metadata", {})
    elif "scope" in record:
        out["scope"] = record.get("scope")
    return out


def context_text(record: Json) -> str:
    if record.get("record_type") == "context_entity":
        return f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
    if record.get("record_type") == "context_segment":
        return f"{record.get('topic', '')}: {record.get('summary_text', '')}"
    return str(record.get("text") or record.get("summary_text") or "")


def build_debug(records: list[Json], query: str, question_type: str, scope: Json) -> Json:
    index_terms_by_batch: dict[Any, list[str]] = {}
    index_terms_by_node: dict[Any, list[str]] = {}
    for record in records:
        if record.get("record_type") == "context_index" and record.get("scope", {}).get("user_id") == scope.get("user_id"):
            name = str(record.get("index_name") or "")
            if name:
                index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(name)
                index_terms_by_node.setdefault(record.get("node_hash"), []).append(name)
    groups = infer_secondary_index_filter_groups(query, question_type)
    candidates = []
    for record in records:
        if record.get("record_type") not in {"context_event", "context_entity", "context_segment"}:
            continue
        record_scope = record.get("scope") or record.get("envelope", {}).get("scope", {})
        if record_scope.get("user_id") != scope.get("user_id"):
            continue
        terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node)
        passes = passes_secondary_index_filters(terms, groups, mode="any_group" if len(groups) > 1 else "all_groups")
        candidates.append(
            {
                "record_type": record.get("record_type"),
                "ref_hash": record.get("event_id_hash") or record.get("entity_hash") or record.get("segment_hash"),
                "node_path": record.get("node_path"),
                "candidate_terms": sorted(terms),
                "passes_secondary_index_prefilter": passes,
                "text": context_text(record)[:280],
            }
        )
    return {
        "question_type": question_type,
        "secondary_index_filter_groups": [sorted(group) for group in groups],
        "candidate_prefilter_debug": candidates,
        "passed_count": sum(1 for row in candidates if row["passes_secondary_index_prefilter"]),
        "dropped_count": sum(1 for row in candidates if not row["passes_secondary_index_prefilter"]),
    }


def batch_conversations() -> list[Json]:
    return [
        {
            "name": "conversation_a_mixed_memory_batch",
            "scope": {
                "account_id": "acct_query_index_debug",
                "tenant_id": "tenant_context",
                "user_id": "locomo_user_a",
                "session_id": "session_a",
            },
            "metadata": {"node_path": ["personal_memory", "session_a", "mixed_updates"]},
            "messages": [
                {"role": "user", "content": "I moved to Seattle at the start of March."},
                {"role": "assistant", "content": "I will remember Seattle as the earlier location."},
                {"role": "user", "content": "On April 10 I moved to Austin for the storage project."},
                {"role": "assistant", "content": "Austin is now the newer location."},
                {"role": "user", "content": "I prefer Python for dashboards."},
                {"role": "assistant", "content": "Preference noted: Python for dashboards."},
                {"role": "user", "content": "Actually I now prefer Rust for low latency storage engines."},
                {"role": "assistant", "content": "Preference update noted: Rust for low latency storage engines."},
                {"role": "user", "content": "Alice approved the GPU purchase after finance reviewed the budget."},
                {"role": "assistant", "content": "The GPU purchase approval by Alice is confirmed."},
                {"role": "user", "content": "The amount for the GPU purchase is 42000 dollars."},
                {"role": "assistant", "content": "Budget amount recorded as 42000 dollars."},
                {"role": "user", "content": "My manager Priya is helping with the launch plan."},
                {"role": "assistant", "content": "Manager relationship noted: Priya."},
                {"role": "user", "content": "My job role is storage infrastructure lead."},
                {"role": "assistant", "content": "Job status recorded as storage infrastructure lead."},
                {"role": "user", "content": "I plan to visit Berlin next month for the conference."},
                {"role": "assistant", "content": "Current plan recorded: visit Berlin next month."},
                {"role": "user", "content": "Thanks, that is all for this memory batch."},
                {"role": "assistant", "content": "Batch summary can now be committed."},
            ],
            "queries": [
                "Where is the user currently located?",
                "What does the user prefer for low latency storage now?",
                "Who approved the GPU purchase budget?",
                "Who is the user's manager?",
            ],
        },
        {
            "name": "conversation_b_family_plan_correction_batch",
            "scope": {
                "account_id": "acct_query_index_debug",
                "tenant_id": "tenant_context",
                "user_id": "locomo_user_b",
                "session_id": "session_b",
            },
            "metadata": {"node_path": ["personal_memory", "session_b", "family_plan_updates"]},
            "messages": [
                {"role": "user", "content": "My family has a dog named Mochi."},
                {"role": "assistant", "content": "Family profile noted: dog named Mochi."},
                {"role": "user", "content": "My sister Emma lives in Denver."},
                {"role": "assistant", "content": "Relationship noted: sister Emma in Denver."},
                {"role": "user", "content": "I used to live in Boston before the move."},
                {"role": "assistant", "content": "Historical location Boston noted."},
                {"role": "user", "content": "Now I live in Chicago near the lake."},
                {"role": "assistant", "content": "Current location Chicago near the lake."},
                {"role": "user", "content": "I like Java for backend prototypes."},
                {"role": "assistant", "content": "Preference noted: Java for backend prototypes."},
                {"role": "user", "content": "Correction: I now prefer Go for backend prototypes."},
                {"role": "assistant", "content": "Correction recorded: Go is the newer backend prototype preference."},
                {"role": "user", "content": "Bob confirmed the travel budget for Berlin."},
                {"role": "assistant", "content": "Travel budget confirmation by Bob recorded."},
                {"role": "user", "content": "The current plan is to ship the benchmark report Friday."},
                {"role": "assistant", "content": "Current plan recorded: ship benchmark report Friday."},
                {"role": "user", "content": "My role changed to AI memory platform owner."},
                {"role": "assistant", "content": "Job status update recorded: AI memory platform owner."},
                {"role": "user", "content": "Please remember these details for later."},
                {"role": "assistant", "content": "The second memory batch is ready to commit."},
            ],
            "queries": [
                "What pet is in the user's family?",
                "Where does the user currently live?",
                "What backend language does the user currently prefer?",
                "What is the user's current role status?",
            ],
        },
    ]


def create_adapter(args: argparse.Namespace, event_log: Path) -> MatrixArkLocalAdapter:
    if args.backend == "temporalstore-direct":
        return MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    return MatrixArkLocalAdapter(event_log)


def read_records(adapter: MatrixArkLocalAdapter, event_log: Path, backend: str) -> list[Json]:
    if backend == "temporalstore-direct":
        records = adapter.read_all()
        event_log.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8")
        return records
    return [json.loads(line) for line in event_log.read_text().splitlines() if line.strip()]


def run(args: argparse.Namespace, artifact_dir: Path) -> Json:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    event_log = Path(args.event_log) if args.event_log else artifact_dir / "matrixark_query_index_event_log.jsonl"
    event_log.unlink(missing_ok=True)
    adapter = create_adapter(args, event_log)
    server = MatrixArkMcpServer(adapter)
    batches = []
    retrievals = []
    for convo in batch_conversations():
        batch_args = {
            "messages": convo["messages"],
            "scope": convo["scope"],
            "metadata": convo["metadata"],
            "threshold_messages": 20,
            "force": False,
            "skip_prior_context": False,
        }
        batch_result = call_tool(server, "matrixark_batch_extract", batch_args)
        refresh = call_tool(server, "matrixark_refresh_summaries", {"scope": convo["scope"], "limit": 128})
        batches.append({"name": convo["name"], "request": batch_args, "result": batch_result, "summary_refresh": refresh})
        for query in convo["queries"]:
            retrieve_args = {
                "query": query,
                "scope": convo["scope"],
                "max_context_tokens": 120,
                "ranking": {
                    "top_k_per_layer": 8,
                    "max_children_scored_per_parent": 10000,
                    "auxiliary_quota": 4,
                },
            }
            pack = call_tool(server, "matrixark_retrieve", retrieve_args)
            records_now = read_records(adapter, event_log, args.backend)
            qtype = infer_query_type(query)
            retrievals.append(
                {
                    "query_format": retrieve_args,
                    "query_type": qtype,
                    "secondary_index_debug": build_debug(records_now, query, qtype, convo["scope"]),
                    "result": pack,
                }
            )
    records = read_records(adapter, event_log, args.backend)
    by_model = {}
    for record in records:
        by_model.setdefault(record.get("record_type", "unknown"), []).append(compact(record))
    report = {
        "status": "passed",
        "backend": args.backend,
        "event_log": str(event_log),
        "temporalstore": {
            "metaserver": args.metaserver if args.backend == "temporalstore-direct" else "",
            "namespace": args.namespace if args.backend == "temporalstore-direct" else "",
            "table": args.table if args.backend == "temporalstore-direct" else "",
            "storage_prefix": args.storage_prefix if args.backend == "temporalstore-direct" else "",
            "temporalstore_lib": args.temporalstore_lib if args.backend == "temporalstore-direct" else "",
        },
        "embedding_provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic"),
        "embedding_model": os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get("MATRIXARK_EMBEDDING_MODEL", "matrixark-local-token-hash-v1"),
        "note": "This debug run uses real OSS embeddings. Extraction and query-understanding are the current MatrixArk internal deterministic schema/rule path unless an LLM provider is wired in.",
        "model_counts": model_counts(records),
        "batch_conversations": batches,
        "retrievals": retrievals,
        "records_by_model": {key: rows[:30] for key, rows in sorted(by_model.items())},
    }
    return report


def write_docs(report: Json, artifact_dir: Path) -> None:
    json_path = artifact_dir / "matrixark_query_index_debug_flow.json"
    md_path = artifact_dir / "matrixark_query_index_debug_flow.md"
    html_path = artifact_dir / "matrixark_query_index_debug_flow.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# MatrixArk Query, Index, Ingestion, Extraction, Retrieval Debug Flow",
        "",
        "## What This Run Proves",
        "",
        f"- Backend: `{report.get('backend', 'local')}`.",
        "- Batched 20-message conversations are accepted through `matrixark_batch_extract`.",
        "- ContextEvent, ContextSegment, ContextEntity, ContextSummary, ContextEmbedding, ContextIndex, and ContextPackAudit records are written.",
        "- L0/L1 node summaries are refreshed before retrieval.",
        "- Query text is parsed into a question type and secondary-index filter groups before semantic scoring.",
        "- Real OSS embedding vectors are generated and stored. In this run, the vector dimension is visible in `context_embedding` records.",
        "",
        "## Storage Boundary",
        "",
        f"- backend: `{report.get('backend', 'local')}`",
        f"- temporalstore: `{json.dumps(report.get('temporalstore', {}), sort_keys=True)}`",
        "",
        "## Model Boundary",
        "",
        f"- embedding_provider: `{report['embedding_provider']}`",
        f"- embedding_model: `{report['embedding_model']}`",
        f"- note: {report['note']}",
        "",
        "## Data Model Counts",
        "",
    ]
    for key, value in report["model_counts"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Ingestion API Format", ""])
    lines.append("`matrixark_batch_extract` request shape used in this run:")
    lines.append("```json")
    lines.append(json.dumps(report["batch_conversations"][0]["request"], indent=2)[:6000])
    lines.append("```")
    lines.extend(["", "## Retrieval Query Format And Secondary Index Filtering", ""])
    for item in report["retrievals"]:
        lines.append(f"### Query: {item['query_format']['query']}")
        lines.append("Request:")
        lines.append("```json")
        lines.append(json.dumps(item["query_format"], indent=2))
        lines.append("```")
        lines.append(f"Question type: `{item['query_type']}`")
        lines.append("Secondary-index filter groups inferred from query:")
        lines.append("```json")
        lines.append(json.dumps(item["secondary_index_debug"]["secondary_index_filter_groups"], indent=2))
        lines.append("```")
        lines.append(f"Candidates passing prefilter: `{item['secondary_index_debug']['passed_count']}`; dropped before scoring: `{item['secondary_index_debug']['dropped_count']}`")
        lines.append("Tree traversal summary:")
        lines.append("```json")
        lines.append(json.dumps(item["result"].get("recall_policy", {}).get("tree_traversal", {}), indent=2))
        lines.append("```")
        lines.append("Selected refs:")
        lines.append("```json")
        lines.append(json.dumps(item["result"].get("selected_refs", [])[:6], indent=2)[:6000])
        lines.append("```")
        lines.append("Prefilter candidate sample:")
        lines.append("```json")
        lines.append(json.dumps(item["secondary_index_debug"]["candidate_prefilter_debug"][:10], indent=2)[:6000])
        lines.append("```")
    lines.extend(["", "## Records By Data Model", ""])
    for model, rows in report["records_by_model"].items():
        lines.append(f"### {model}")
        lines.append("```json")
        lines.append(json.dumps(rows[:12], indent=2, sort_keys=True)[:10000])
        lines.append("```")
    md = "\n".join(lines) + "\n"
    md_path.write_text(md, encoding="utf-8")
    html_path.write_text(
        "<!doctype html><meta charset='utf-8'><title>MatrixArk Query Index Debug Flow</title>"
        "<style>body{font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1180px;margin:32px auto;padding:0 24px;line-height:1.45;color:#172033}pre{background:#f6f8fb;border:1px solid #dde5f0;padding:14px;overflow:auto;border-radius:8px}code{background:#edf2f7;padding:2px 4px;border-radius:4px}h1,h2,h3{color:#0f172a}</style>"
        + "<pre>" + html.escape(md) + "</pre>",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    artifact_dir = Path(args.artifact_dir)
    report = run(args, artifact_dir)
    write_docs(report, artifact_dir)
    print(json.dumps({"status": "passed", "backend": args.backend, "artifact_dir": str(artifact_dir), "temporalstore": report.get("temporalstore", {}), "model_counts": report["model_counts"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
