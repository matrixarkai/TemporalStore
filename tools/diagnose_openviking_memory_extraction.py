#!/usr/bin/env python3
"""Diagnose local OpenViking memory-extraction evidence for benchmark baselines."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/tmp/openviking_matrixark_oss/ov_qwen_memory_fresh.conf")
    parser.add_argument("--import-success", default="/tmp/openviking_matrixark_oss/results_qwen_memory/import_success.csv")
    parser.add_argument("--server-log", default="/tmp/openviking_matrixark_oss/server_qwen_memory.log")
    parser.add_argument("--report", default="/tmp/openviking_memory_extraction_diagnosis.json")
    args = parser.parse_args()

    config = read_json(Path(args.config))
    rows = read_csv(Path(args.import_success))
    log_text = Path(args.server_log).read_text(encoding="utf-8", errors="replace") if Path(args.server_log).exists() else ""
    extracted_counts = [int(value) for value in re.findall(r"Extracted\s+(\d+)\s+memories", log_text)]
    completion_tokens = sum_int(rows, "llm_output_tokens")
    vlm_tokens = sum_int(rows, "vlm_tokens")
    embedding_tokens = sum_int(rows, "embedding_tokens")
    extraction_enabled = bool((config.get("memory") or {}).get("extraction_enabled"))
    report: dict[str, Any] = {
        "schema": "matrixark_openviking_memory_extraction_diagnosis_v1",
        "config": args.config,
        "import_success": args.import_success,
        "server_log": args.server_log,
        "session_rows": len(rows),
        "memory_extraction_enabled": extraction_enabled,
        "embedding_tokens": embedding_tokens,
        "vlm_tokens": vlm_tokens,
        "llm_output_tokens": completion_tokens,
        "extracted_memory_counts": extracted_counts,
        "total_extracted_memories": sum(extracted_counts),
        "vlm_provider": ((config.get("vlm") or {}).get("provider")),
        "vlm_model": ((config.get("vlm") or {}).get("model")),
        "vlm_api_base": ((config.get("vlm") or {}).get("api_base")),
        "embedding_model": (((config.get("embedding") or {}).get("dense") or {}).get("model")),
        "baseline_ready": extraction_enabled and sum(extracted_counts) > 0,
        "direct_source_fallback_required": not (extraction_enabled and sum(extracted_counts) > 0),
        "likely_gap": infer_gap(extraction_enabled, embedding_tokens, vlm_tokens, completion_tokens, extracted_counts),
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
) -> str:
    if not extraction_enabled:
        return "memory_extraction_disabled"
    if embedding_tokens > 0 and vlm_tokens == 0 and completion_tokens == 0:
        return "messages_archived_and_embedded_but_no_chat_completion_tokens_recorded_for_memory_extraction"
    if extracted_counts and sum(extracted_counts) == 0:
        return "memory_extractor_completed_but_returned_zero_memories"
    if not extracted_counts:
        return "no_memory_extraction_completion_log_found"
    return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
