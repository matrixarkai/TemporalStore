#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from tools.matrixark_mcp_server import (
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    apply_statistical_operator,
    latest_record,
    score_recall_candidate,
)

Json = dict[str, Any]


def call(server: MatrixArkMcpServer, name: str, args: Json) -> Json:
    response = server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}}
    )
    if "error" in response:
        raise RuntimeError(response["error"])
    return json.loads(response["result"]["content"][0]["text"])


def run() -> Json:
    tmp = tempfile.TemporaryDirectory()
    server = MatrixArkMcpServer(MatrixArkLocalAdapter(Path(tmp.name) / "operator-compression.jsonl"))
    numeric_records = [
        {"metadata": {"value": 10}, "updated_at_ms": 1000, "state": "old"},
        {"metadata": {"value": 25}, "updated_at_ms": 3000, "state": "new"},
        {"metadata": {"value": 5}, "updated_at_ms": 2000, "state": "middle"},
    ]
    statistical = {
        op: apply_statistical_operator(op, numeric_records)
        for op in ["COUNT", "SUM", "AVG", "MAX"]
    }
    latest = latest_record(numeric_records)
    decayed = score_recall_candidate(
        {
            "origin_score": 0.4,
            "updated_at_ms": 1_000,
            "event_type": "approval",
            "metadata": {"business_weight": 1.0},
        },
        {
            "freshness_tolerance_ms": 0,
            "half_life_ms": 1_000,
            "weights": {"time": 0.25, "business": 0.35},
        },
        reference_time_ms=5_000,
    )

    scope = {"user_id": "alice", "session_id": "operator-window", "team": "infra"}
    node_path = [
        "account:acct_dev",
        "tenant:tenant_dev",
        "principal:user:alice",
        "collection:sessions",
        "session:operator-window",
    ]
    ingests = []
    for text in [
        "Alice approved the old GPU purchase after finance reviewed it.",
        "The GPU approval budget was 42000 dollars.",
        "The approval was confirmed by infra lead Sam.",
    ]:
        ingests.append(
            call(
                server,
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": text}],
                    "scope": scope,
                    "metadata": {"node_path": node_path, "importance": 0.95, "business_weight": 0.95},
                },
            )
        )
    event_records = [record for record in server.adapter.read_all() if record.get("record_type") == "context_event"]
    times = [record["envelope"]["ingestion_time_ms"] for record in event_records]
    compression = server.adapter.write_time_compression(
        scope=scope,
        node_hash=ingests[0]["node_hash"],
        node_path=node_path,
        source_start_ms=min(times),
        source_end_ms=max(times),
        compressed_time_ms=max(times) + 10_000,
        max_source_events=2,
        min_importance=0.9,
    )
    compression_query = server.adapter.query_time_compressions(
        scope=compression["scope"],
        node_hashes={ingests[0]["node_hash"]},
        start_time_ms=min(times),
        end_time_ms=max(times),
    )
    pack = call(
        server,
        "matrixark_retrieve",
        {
            "query": "old GPU approval budget finance",
            "scope": scope,
            "max_context_tokens": 20,
            "ranking": {"weights": {"time": 0.05, "business": 0.25}, "auxiliary_quota": 2},
        },
    )
    compression_refs = [ref for ref in pack["selected_refs"] if ref.get("ref_type") == "compression"]
    if not compression_refs:
        raise AssertionError(f"compression ref was not selected: {pack['selected_refs']}")
    replay = call(server, "matrixark_replay", {"context_pack_id": "debug"})
    record_counts: dict[str, int] = {}
    for record in replay["events"]:
        record_counts[record.get("record_type", "unknown")] = record_counts.get(record.get("record_type", "unknown"), 0) + 1
    return {
        "status": "passed",
        "operators": {
            "statistical": statistical,
            "LATEST": latest,
            "DECAY_SCORE": decayed,
            "LLM_MERGE": "covered by ContextEntity update tests and deterministic field patch audits",
            "TIME_COMPRESS": {
                "compression_id_hash": compression["compression_id_hash"],
                "source_event_count": compression["source_event_count"],
                "source_event_ids": compression["source_event_ids"],
                "truncated_source_events": compression["truncated_source_events"],
                "selected_in_context_pack": True,
                "selected_ref": compression_refs[0],
            },
            "VALID_AS_OF": "covered by C++ QuerySummaries/QueryNodeContext as_of tests",
            "BLOCK_IF_STALE": "covered by stale blocker packing/failure bucket policy; no destructive prune in MVP",
        },
        "compression_query_count": len(compression_query),
        "record_counts": dict(sorted(record_counts.items())),
        "context_pack": {
            "used_context_tokens": pack["used_context_tokens"],
            "selected_refs": pack["selected_refs"],
            "recall_policy": pack["recall_policy"],
        },
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, sort_keys=True))
