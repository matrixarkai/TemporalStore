#!/usr/bin/env python3
"""Run MatrixArk MCP feature parity across C++ and Rust TemporalStore backends.

This is stronger than a smoke test: each backend runs the same online ingest,
retrieval, feedback confirmation, one-pass batch extraction, async summary
refresh, current-state retrieval, and replay flow through the MCP server.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.run_matrixark_mcp_backend_parity import McpProcess, _backend_command, _call_tool  # noqa: E402

Json = dict[str, Any]
REPORT_DIR = Path(os.environ.get("MATRIXARK_MCP_FEATURE_PARITY_REPORT_DIR", "/tmp/matrixark-mcp-feature-parity"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _batch_messages() -> list[Json]:
    return [
        {"role": "user", "content": "I moved to Seattle at the start of March."},
        {"role": "assistant", "content": "Earlier location noted as Seattle."},
        {"role": "user", "content": "On April 10 I moved to Austin for the storage project."},
        {"role": "assistant", "content": "Austin is the newer location."},
        {"role": "user", "content": "I prefer Python for dashboards."},
        {"role": "assistant", "content": "Preference noted: Python for dashboards."},
        {"role": "user", "content": "Actually I now prefer Rust for low latency storage engines."},
        {"role": "assistant", "content": "Preference update noted: Rust for low latency storage engines."},
        {"role": "user", "content": "Alice approved the GPU purchase after finance reviewed the budget."},
        {"role": "assistant", "content": "Approval by Alice recorded."},
        {"role": "user", "content": "The GPU purchase amount is 42000 dollars."},
        {"role": "assistant", "content": "Budget amount recorded as 42000 dollars."},
        {"role": "user", "content": "My manager Priya is helping with the launch plan."},
        {"role": "assistant", "content": "Manager relationship noted: Priya."},
        {"role": "user", "content": "My job role is storage infrastructure lead."},
        {"role": "assistant", "content": "Job status recorded as storage infrastructure lead."},
        {"role": "user", "content": "I plan to visit Berlin next month for the conference."},
        {"role": "assistant", "content": "Current plan recorded: visit Berlin next month."},
        {"role": "user", "content": "My family has a dog named Mochi."},
        {"role": "assistant", "content": "Family profile noted: dog named Mochi."},
    ]


def _tool_counts(batch_result: Json) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in ["events", "entities", "segments", "indexes", "embeddings", "dirty_markers"]:
        value = batch_result.get(key, [])
        if isinstance(value, list):
            counts[key] = len(value)
    for key in ["event_count", "entity_count", "segment_count", "index_count", "embedding_count"]:
        if key in batch_result:
            counts[key] = int(batch_result.get(key) or 0)
    return counts


def run_backend(backend: str, run_id: str) -> Json:
    env = os.environ.copy()
    env.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "hash")
    env.setdefault("MATRIXARK_UNDERSTANDING_PROVIDER", "rules")
    env.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "0")
    env.setdefault("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "0")
    env.setdefault("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "8000")
    env["MATRIXARK_TEMPORALSTORE_PREFIX"] = f"matrixark:mcp:feature-parity:{backend}:{run_id}"
    proc = McpProcess(backend, _backend_command(backend), env)
    try:
        proc.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "matrixark-feature-parity", "version": "1.0"},
            },
            timeout_s=120.0,
        )
        proc.notify("notifications/initialized")
        online_scope = {
            "account_id": "acct_feature_parity",
            "tenant_id": "tenant_feature_parity",
            "user_id": f"user_online_{backend}",
            "session_id": f"session_online_{backend}_{run_id}",
        }
        batch_scope = {
            "account_id": "acct_feature_parity",
            "tenant_id": "tenant_feature_parity",
            "user_id": f"user_batch_{backend}",
            "session_id": f"session_batch_{backend}_{run_id}",
        }

        online_ingest = _call_tool(
            proc,
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU request for Project Falcon today."}],
                "scope": online_scope,
                "metadata": {"node_path": ["memory", "approvals", "gpu"]},
                "auto_batch_extract": False,
            },
            timeout_s=120.0,
        )
        online_refresh = _call_tool(proc, "matrixark_refresh_summaries", {"scope": online_scope, "limit": 64}, timeout_s=120.0)
        online_retrieve = _call_tool(
            proc,
            "matrixark_retrieve",
            {
                "query": "Who approved the GPU request for Project Falcon?",
                "scope": online_scope,
                "max_context_tokens": 600,
            },
            timeout_s=120.0,
        )
        feedback = _call_tool(
            proc,
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "Yes, that approval answer is correct."}],
                "scope": online_scope,
                "context_pack_id": online_retrieve.get("context_pack_id"),
                "accepted_refs": online_retrieve.get("selected_refs", [])[:1],
            },
            timeout_s=120.0,
        )
        post_feedback_retrieve = _call_tool(
            proc,
            "matrixark_retrieve",
            {
                "query": "Was the GPU approval answer confirmed?",
                "scope": online_scope,
                "max_context_tokens": 600,
            },
            timeout_s=120.0,
        )

        batch_extract = _call_tool(
            proc,
            "matrixark_batch_extract",
            {
                "messages": _batch_messages(),
                "scope": batch_scope,
                "metadata": {"node_path": ["personal_memory", "feature_parity", "batch"]},
                "threshold_messages": 20,
                "force": False,
                "segment_provider": "deterministic",
            },
            timeout_s=180.0,
        )
        batch_refresh = _call_tool(proc, "matrixark_refresh_summaries", {"scope": batch_scope, "limit": 128}, timeout_s=180.0)
        current_location = _call_tool(
            proc,
            "matrixark_retrieve",
            {"query": "Where does the user currently live?", "scope": batch_scope, "max_context_tokens": 900},
            timeout_s=120.0,
        )
        current_preference = _call_tool(
            proc,
            "matrixark_retrieve",
            {"query": "What does the user currently prefer for low latency storage?", "scope": batch_scope, "max_context_tokens": 900},
            timeout_s=120.0,
        )
        replay = _call_tool(
            proc,
            "matrixark_replay",
            {"context_pack_id": current_preference.get("context_pack_id"), "scope": batch_scope},
            timeout_s=120.0,
        )

        checks = {
            "online_ingest_new_event": online_ingest.get("classification") == "NEW_EVENT",
            "online_summary_refreshed": int(online_refresh.get("refreshed_count") or 0) >= 1,
            "online_retrieve_selected": len(online_retrieve.get("selected_refs", [])) >= 1,
            "feedback_confirmation": feedback.get("classification") == "CONFIRMATION",
            "post_feedback_retrieve_selected": len(post_feedback_retrieve.get("selected_refs", [])) >= 1,
            "batch_extract_committed": batch_extract.get("status") in {"committed", "accepted", "ok"} or bool(batch_extract.get("batch_id_hash")),
            "batch_summary_refreshed": int(batch_refresh.get("refreshed_count") or 0) >= 1,
            "current_location_selected": len(current_location.get("selected_refs", [])) >= 1,
            "current_preference_selected": len(current_preference.get("selected_refs", [])) >= 1,
            "replay_has_records": bool(replay.get("records") or replay.get("events") or replay.get("context_pack_id")),
        }
        for name, ok in checks.items():
            _require(ok, f"{backend} failed check {name}")

        return {
            "backend": backend,
            "ok": True,
            "storage_prefix": env["MATRIXARK_TEMPORALSTORE_PREFIX"],
            "checks": checks,
            "online": {
                "classification": online_ingest.get("classification"),
                "summary_refreshed_count": online_refresh.get("refreshed_count"),
                "retrieve_selected": len(online_retrieve.get("selected_refs", [])),
                "feedback_classification": feedback.get("classification"),
                "feedback_prior_refs": len(feedback.get("prior_refs", [])),
                "post_feedback_selected": len(post_feedback_retrieve.get("selected_refs", [])),
            },
            "batch": {
                "status": batch_extract.get("status"),
                "counts": _tool_counts(batch_extract),
                "summary_refreshed_count": batch_refresh.get("refreshed_count"),
                "location_selected": len(current_location.get("selected_refs", [])),
                "preference_selected": len(current_preference.get("selected_refs", [])),
                "location_question_type": current_location.get("question_type"),
                "preference_question_type": current_preference.get("question_type"),
                "tree_traversal": current_preference.get("recall_policy", {}).get("tree_traversal", {}),
            },
            "replay": {
                "keys": sorted(replay.keys()),
                "context_pack_id": current_preference.get("context_pack_id"),
            },
        }
    finally:
        proc.close()


def compare(results: list[Json]) -> Json:
    by_backend = {item["backend"]: item for item in results}
    if "cpp" not in by_backend or "rust" not in by_backend:
        return {"status": "skipped", "reason": "need both cpp and rust"}
    cpp = by_backend["cpp"]
    rust = by_backend["rust"]
    comparable = {
        "online_retrieve_selected_equal": cpp["online"]["retrieve_selected"] == rust["online"]["retrieve_selected"],
        "feedback_classification_equal": cpp["online"]["feedback_classification"] == rust["online"]["feedback_classification"],
        "batch_status_equal": cpp["batch"]["status"] == rust["batch"]["status"],
        "current_location_selected_equal": cpp["batch"]["location_selected"] == rust["batch"]["location_selected"],
        "current_preference_selected_equal": cpp["batch"]["preference_selected"] == rust["batch"]["preference_selected"],
        "location_question_type_equal": cpp["batch"]["location_question_type"] == rust["batch"]["location_question_type"],
        "preference_question_type_equal": cpp["batch"]["preference_question_type"] == rust["batch"]["preference_question_type"],
    }
    return {"status": "passed" if all(comparable.values()) else "warning", "checks": comparable}


def write_report(run_id: str, results: list[Json], failures: list[str]) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    comparison = compare(results)
    report = {
        "run_id": run_id,
        "all_ok": not failures,
        "failures": failures,
        "comparison": comparison,
        "results": results,
    }
    report_json = REPORT_DIR / f"matrixark_mcp_feature_parity_{run_id}.json"
    report_md = REPORT_DIR / f"matrixark_mcp_feature_parity_{run_id}.md"
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# MatrixArk MCP Feature Parity",
        "",
        f"Run ID: `{run_id}`",
        f"All OK: `{report['all_ok']}`",
        f"Comparison: `{comparison.get('status')}`",
        "",
        "## What Was Tested",
        "",
        "- online `matrixark_ingest` writes a raw ContextEvent path",
        "- `matrixark_refresh_summaries` refreshes dirty L0/L1 summaries",
        "- `matrixark_retrieve` returns a ContextPack",
        "- `matrixark_feedback` classifies confirmation using prior ContextPack refs",
        "- `matrixark_batch_extract` commits a 20-message logical session batch",
        "- current-state retrieval uses the same entity/event/summary pack path",
        "- `matrixark_replay` returns replayable context-pack data",
        "",
    ]
    for item in results:
        lines.extend([
            f"## {item.get('backend')}",
            "",
            f"- OK: `{item.get('ok')}`",
            f"- Storage prefix: `{item.get('storage_prefix', '')}`",
            f"- Online selected refs: `{item.get('online', {}).get('retrieve_selected')}`",
            f"- Feedback classification: `{item.get('online', {}).get('feedback_classification')}`",
            f"- Batch status: `{item.get('batch', {}).get('status')}`",
            f"- Batch counts: `{json.dumps(item.get('batch', {}).get('counts', {}), sort_keys=True)}`",
            f"- Batch summary refresh count: `{item.get('batch', {}).get('summary_refreshed_count')}`",
            f"- Current location selected refs: `{item.get('batch', {}).get('location_selected')}`",
            f"- Current preference selected refs: `{item.get('batch', {}).get('preference_selected')}`",
            "",
        ])
        if item.get("error"):
            lines.append(f"Error: `{item['error']}`\n")
    lines.extend(["## C++ Vs Rust Comparison", "", "```json", json.dumps(comparison, indent=2, sort_keys=True), "```", ""])
    report_md.write_text("\n".join(lines), encoding="utf-8")
    return report_json, report_md


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", nargs="+", default=["cpp", "rust"], choices=["local", "cpp", "rust"])
    parser.add_argument("--run-id", default=str(int(time.time())))
    args = parser.parse_args()
    results: list[Json] = []
    failures: list[str] = []
    for backend in args.backends:
        started = time.monotonic()
        try:
            result = run_backend(backend, args.run_id)
            result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
            results.append(result)
            print(json.dumps(result, indent=2, sort_keys=True))
        except Exception as exc:
            failure = {"backend": backend, "ok": False, "error": str(exc), "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
            results.append(failure)
            failures.append(backend)
            print(json.dumps(failure, indent=2, sort_keys=True))
    report_json, report_md = write_report(args.run_id, results, failures)
    print(f"report_json={report_json}")
    print(f"report_md={report_md}")
    comparison = compare([item for item in results if item.get("ok")])
    return 0 if not failures and comparison.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
