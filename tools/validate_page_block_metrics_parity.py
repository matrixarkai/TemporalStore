#!/usr/bin/env python3
"""Validate the shared C++/Rust page/block metric parity contract."""

from __future__ import annotations

import ast
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "temporalstore_page_block_address_contract.md"
SCALE_REPORT = ROOT / "tools" / "run_matrixark_cpp_rust_scale_report.py"

REQUIRED_METRICS = [
    "page_index_lookup_count",
    "page_index_lookup_ms",
    "page_index_cache_hit_rate",
    "block_index_lookup_count",
    "block_index_lookup_ms",
    "block_index_cache_hit_rate",
    "page_reads",
    "page_writes",
    "block_reads",
    "block_writes",
    "bytes_read",
    "bytes_written",
    "append_queue_wait_ms",
    "append_engine_ms",
    "append_queue_depth",
    "append_batch_size",
    "append_batch_bytes",
    "append_coalesced_writes",
    "append_durability_failures",
    "compaction_reclaimed_bytes",
    "cold_scan_no_cache_reads",
    "hot_cache_promotions",
    "append_watermark",
    "compaction_watermark",
]


def _extract_page_block_metric_names_from_runner() -> list[str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == "PAGE_BLOCK_METRIC_NAMES" for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                    raise AssertionError("PAGE_BLOCK_METRIC_NAMES must be a list[str]")
                return value
    raise AssertionError("PAGE_BLOCK_METRIC_NAMES not found in scale report runner")


def main() -> int:
    contract_text = CONTRACT.read_text(encoding="utf-8")
    runner_metrics = _extract_page_block_metric_names_from_runner()
    missing = []

    for metric in REQUIRED_METRICS:
        if f"`{metric}`" not in contract_text:
            missing.append(f"contract:{metric}")
        if metric not in runner_metrics:
            missing.append(f"runner:{metric}")

    extra_runner_metrics = sorted(set(runner_metrics) - set(REQUIRED_METRICS))
    if extra_runner_metrics:
        missing.append(f"runner_extra:{','.join(extra_runner_metrics)}")

    # Guard against accidental hyphen/camelCase variants in the public contract.
    contract_metric_tokens = set(re.findall(r"`([a-z]+[a-z0-9_]*(?:_[a-z0-9]+)+)`", contract_text))
    drift = sorted(name for name in contract_metric_tokens if name.endswith("_watermark") or name.endswith("_reads") or name.endswith("_writes"))
    for name in drift:
        if name not in REQUIRED_METRICS and name.startswith(("page_", "block_", "bytes_", "cold_", "hot_", "append_", "compaction_")):
            missing.append(f"contract_drift:{name}")

    if missing:
        for item in missing:
            print(item, file=sys.stderr)
        return 1

    print("page/block metrics parity names present:")
    for metric in REQUIRED_METRICS:
        print(f"- {metric}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
