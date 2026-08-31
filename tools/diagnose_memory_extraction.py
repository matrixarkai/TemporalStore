#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Diagnose local external-memory-engine extraction evidence for benchmark baselines."""

from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
import os


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/tmp/external_memory_matrixark_oss/qwen_memory_fresh.conf")
    parser.add_argument("--import-success", default="/tmp/external_memory_matrixark_oss/results_qwen_memory/import_success.csv")
    parser.add_argument("--server-log", default="/tmp/external_memory_matrixark_oss/server_qwen_memory.log")
    parser.add_argument("--workspace", default="/tmp/external_memory_matrixark_oss/qwen_memory_data")
    parser.add_argument("--report", default="/tmp/external_memory_extraction_diagnosis.json")
    args = parser.parse_args()

    config = read_json(Path(args.config))
    rows = read_csv(Path(args.import_success))
    log_text = Path(args.server_log).read_text(encoding="utf-8", errors="replace") if Path(args.server_log).exists() else ""
    extracted_counts = [int(value) for value in re.findall(r"Extracted\s+(\d+)\s+memories", log_text)]
    completion_tokens = sum_int(rows, "llm_output_tokens")
    vlm_tokens = sum_int(rows, "vlm_tokens")
    embedding_tokens = sum_int(rows, "embedding_tokens")
    extraction_enabled = bool((config.get("memory") or {}).get("extraction_enabled"))
    vlm_api_base = ((config.get("vlm") or {}).get("api_base"))
    vlm_model = ((config.get("vlm") or {}).get("model"))
    embedding_api_base = (((config.get("embedding") or {}).get("dense") or {}).get("api_base"))
    vlm_endpoint_probe = probe_openai_models(vlm_api_base)
    vlm_chat_probe = probe_chat_completion(vlm_api_base, vlm_model)
    embedding_endpoint_probe = probe_openai_models(embedding_api_base)
    task_evidence = collect_task_evidence(Path(args.workspace))
    report: dict[str, Any] = {
        "schema": "matrixark_external_memory_extraction_diagnosis_v1",
        "config": args.config,
        "import_success": args.import_success,
        "server_log": args.server_log,
        "workspace": args.workspace,
        "session_rows": len(rows),
        "memory_extraction_enabled": extraction_enabled,
        "embedding_tokens": embedding_tokens,
        "vlm_tokens": vlm_tokens,
        "llm_output_tokens": completion_tokens,
        "extracted_memory_counts": extracted_counts,
        "total_extracted_memories": sum(extracted_counts),
        "vlm_provider": ((config.get("vlm") or {}).get("provider")),
        "vlm_model": vlm_model,
        "vlm_api_base": vlm_api_base,
        "vlm_endpoint_probe": vlm_endpoint_probe,
        "vlm_chat_probe": vlm_chat_probe,
        "embedding_model": (((config.get("embedding") or {}).get("dense") or {}).get("model")),
        "embedding_api_base": embedding_api_base,
        "embedding_endpoint_probe": embedding_endpoint_probe,
        "task_evidence": task_evidence,
        "baseline_ready": extraction_enabled and sum(extracted_counts) > 0,
        "direct_source_fallback_required": not (extraction_enabled and sum(extracted_counts) > 0),
        "likely_gap": infer_gap(
            extraction_enabled,
            embedding_tokens,
            vlm_tokens,
            completion_tokens,
            extracted_counts,
            vlm_endpoint_probe,
            vlm_chat_probe,
        ),
    }
    Path(args.report).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.report)
    return 0 if report["baseline_ready"] else 1


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sum_int(rows: list[dict[str, str]], field: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(float(row.get(field) or 0))
        except ValueError:
            pass
    return total


def infer_gap(
    extraction_enabled: bool,
    embedding_tokens: int,
    vlm_tokens: int,
    completion_tokens: int,
    extracted_counts: list[int],
    vlm_endpoint_probe: dict[str, Any],
    vlm_chat_probe: dict[str, Any],
) -> str:
    if not extraction_enabled:
        return "memory_extraction_disabled"
    if not vlm_endpoint_probe.get("ok"):
        return "configured_vlm_endpoint_unreachable_or_not_openai_compatible"
    if not vlm_chat_probe.get("ok"):
        return "configured_vlm_endpoint_models_list_works_but_chat_completion_failed"
    if embedding_tokens > 0 and vlm_tokens == 0 and completion_tokens == 0:
        return "messages_archived_and_embedded_but_engine_did_not_record_chat_completion_usage_for_memory_extraction"
    if extracted_counts and sum(extracted_counts) == 0:
        return "memory_extractor_completed_but_returned_zero_memories"
    if not extracted_counts:
        return "no_memory_extraction_completion_log_found"
    return "unknown"


def collect_task_evidence(workspace: Path) -> dict[str, Any]:
    task_root = workspace / os.environ.get("EXTERNAL_BASELINE_WORKSPACE_SUBDIR", "workspace") / "default" / "_system" / "tasks"
    user_root = workspace / os.environ.get("EXTERNAL_BASELINE_WORKSPACE_SUBDIR", "workspace") / "default" / "user"
    task_files = sorted(task_root.glob("*/*.json")) if task_root.exists() else []
    memory_diff_files = (
        sorted(user_root.glob("*/sessions/*/history/archive_001/memory_diff.json"))
        if user_root.exists()
        else []
    )

    task_rows = []
    llm_total_tokens = 0
    embedding_total_tokens = 0
    extracted_total = 0
    for path in task_files:
        data = read_json(path)
        result = data.get("result") if isinstance(data.get("result"), dict) else {}
        token_usage = result.get("token_usage") if isinstance(result.get("token_usage"), dict) else {}
        llm_usage = token_usage.get("llm") if isinstance(token_usage.get("llm"), dict) else {}
        embedding_usage = (
            token_usage.get("embedding") if isinstance(token_usage.get("embedding"), dict) else {}
        )
        memories = result.get("memories_extracted")
        extracted_count = (
            sum(int(value) for value in memories.values())
            if isinstance(memories, dict)
            else 0
        )
        extracted_total += extracted_count
        llm_total_tokens += int(float(llm_usage.get("total_tokens") or 0))
        embedding_total_tokens += int(float(embedding_usage.get("total_tokens") or 0))
        task_rows.append(
            {
                "task_id": data.get("task_id"),
                "status": data.get("status"),
                "session_id": result.get("session_id") or data.get("resource_id"),
                "archive_uri": result.get("archive_uri"),
                "memories_extracted": memories if isinstance(memories, dict) else {},
                "llm_total_tokens": int(float(llm_usage.get("total_tokens") or 0)),
                "embedding_total_tokens": int(float(embedding_usage.get("total_tokens") or 0)),
                "memory_diff_uri": result.get("memory_diff_uri"),
                "error": data.get("error"),
            }
        )

    diff_rows = []
    diff_adds = 0
    diff_updates = 0
    diff_deletes = 0
    for path in memory_diff_files:
        data = read_json(path)
        summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
        adds = int(float(summary.get("total_adds") or 0))
        updates = int(float(summary.get("total_updates") or 0))
        deletes = int(float(summary.get("total_deletes") or 0))
        diff_adds += adds
        diff_updates += updates
        diff_deletes += deletes
        diff_rows.append(
            {
                "path": str(path),
                "archive_uri": data.get("archive_uri"),
                "total_adds": adds,
                "total_updates": updates,
                "total_deletes": deletes,
            }
        )

    return {
        "task_count": len(task_rows),
        "task_rows": task_rows,
        "task_llm_total_tokens": llm_total_tokens,
        "task_embedding_total_tokens": embedding_total_tokens,
        "task_memories_extracted_total": extracted_total,
        "memory_diff_count": len(diff_rows),
        "memory_diff_rows": diff_rows,
        "memory_diff_total_adds": diff_adds,
        "memory_diff_total_updates": diff_updates,
        "memory_diff_total_deletes": diff_deletes,
    }


def probe_openai_models(api_base: str | None) -> dict[str, Any]:
    if not api_base:
        return {"ok": False, "error": "missing_api_base"}
    url = api_base.rstrip("/") + "/models"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = resp.read(4096).decode("utf-8", errors="replace")
        return {"ok": True, "status": resp.status, "url": url, "body_prefix": body[:500]}
    except urllib.error.HTTPError as exc:
        body = exc.read(1024).decode("utf-8", errors="replace")
        return {"ok": False, "status": exc.code, "url": url, "error": body[:500]}
    except Exception as exc:  # noqa: BLE001 - local diagnostic should surface the concrete issue.
        return {"ok": False, "url": url, "error": f"{type(exc).__name__}: {exc}"}


def probe_chat_completion(api_base: str | None, model: str | None) -> dict[str, Any]:
    if not api_base:
        return {"ok": False, "error": "missing_api_base"}
    if not model:
        return {"ok": False, "error": "missing_model"}
    url = api_base.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return exactly the word ready."},
            {"role": "user", "content": "health check"},
        ],
        "temperature": 0,
        "max_tokens": 8,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read(4096).decode("utf-8", errors="replace")
        data = json.loads(body)
        content = str(data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        return {
            "ok": bool(content),
            "status": resp.status,
            "url": url,
            "content": content[:200],
            "usage": usage,
        }
    except urllib.error.HTTPError as exc:
        body = exc.read(1024).decode("utf-8", errors="replace")
        return {"ok": False, "status": exc.code, "url": url, "error": body[:500]}
    except Exception as exc:  # noqa: BLE001 - local diagnostic should surface the concrete issue.
        return {"ok": False, "url": url, "error": f"{type(exc).__name__}: {exc}"}


if __name__ == "__main__":
    raise SystemExit(main())
