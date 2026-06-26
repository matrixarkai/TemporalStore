#!/usr/bin/env python3
"""Validate MatrixArk tests/runners stay in the shared TemporalStoreTestCorpus.

MatrixArk C++/Rust parity runners are shared product-behavior tests. The root
TemporalStore repo should not grow duplicate local copies after migration; local
Python tests are allowed only for implementation internals of this repo.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

MIGRATED_TOOL_NAMES = {
    "run_matrixark_codex_cpp_hook_e2e.py",
    "run_matrixark_context_storage_benchmark.py",
    "run_matrixark_cpp_direct_scale_benchmark.py",
    "run_matrixark_dataset_benchmark.py",
    "run_matrixark_entity_update_algorithm_test.py",
    "run_matrixark_full_dataset_cpp_benchmark.py",
    "run_matrixark_locomo_debug_flow.py",
    "run_matrixark_mcp_backend_parity.py",
    "run_matrixark_mcp_feature_parity.py",
    "run_matrixark_memory_segmentation_test.py",
    "run_matrixark_one_pass_batch_extract_test.py",
    "run_matrixark_operator_compression_test.py",
    "run_matrixark_query_index_debug_flow.py",
    "run_matrixark_resource_skill_backend_parity.py",
    "run_matrixark_temporalstore_direct_e2e.py",
    "run_matrixark_weighted_recall_test.py",
    "test_matrixark_codex_hook.py",
    "test_matrixark_dataset_benchmark_loader.py",
    "test_matrixark_direct_adapter_compact_log.py",
    "test_matrixark_full_dataset_cpp_guard.py",
    "test_matrixark_mcp_server.py",
    "test_matrixark_resource_parser.py",
}

LOCAL_INTERNAL_TOOL_NAMES = {
    "test_matrixark_access_governance.py",
    "test_matrixark_mcp_backend_policy.py",
    "test_matrixark_python_module_boundaries.py",
    "test_matrixark_rust_serve_readiness.py",
    "test_matrixark_summary_worker.py",
}

FORBIDDEN_LOCAL_RUNNER_RE = re.compile(r"python3\s+tools/(" + "|".join(re.escape(name) for name in sorted(MIGRATED_TOOL_NAMES)) + r")")


def corpus_tool_dirs() -> list[Path]:
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "tools",
        ROOT.parent / "TemporalStoreTestCorpus" / "tools",
    ]
    return [candidate for candidate in candidates if candidate.exists()]


def validate_migrated_tools_present_in_corpus(tool_dirs: list[Path]) -> list[str]:
    errors: list[str] = []
    for name in sorted(MIGRATED_TOOL_NAMES):
        root_copy = ROOT / "tools" / name
        if root_copy.exists():
            errors.append(f"duplicate local MatrixArk tool should stay in TemporalStoreTestCorpus: tools/{name}")
        if not any((tool_dir / name).exists() for tool_dir in tool_dirs):
            errors.append(f"shared MatrixArk tool missing from TemporalStoreTestCorpus: {name}")
    return errors


def validate_script_references() -> list[str]:
    errors: list[str] = []
    for path in sorted((ROOT / "tools").glob("*.sh")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in FORBIDDEN_LOCAL_RUNNER_RE.finditer(text):
            rel = path.relative_to(ROOT).as_posix()
            errors.append(f"{rel} calls migrated local runner tools/{match.group(1)}; use TemporalStoreTestCorpus/tools instead")
    return errors


def validate_local_matrixark_tests_are_internal() -> list[str]:
    errors: list[str] = []
    for path in sorted((ROOT / "tools").glob("test_matrixark*.py")):
        if path.name not in LOCAL_INTERNAL_TOOL_NAMES:
            errors.append(f"unexpected local MatrixArk test {path.relative_to(ROOT).as_posix()}; migrate product behavior to TemporalStoreTestCorpus")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print machine-readable result.")
    args = parser.parse_args()

    tool_dirs = corpus_tool_dirs()
    errors: list[str] = []
    if not tool_dirs:
        errors.append("TemporalStoreTestCorpus tools directory not found; initialize third_party/TemporalStoreTestCorpus or sibling checkout")
    errors.extend(validate_migrated_tools_present_in_corpus(tool_dirs))
    errors.extend(validate_script_references())
    errors.extend(validate_local_matrixark_tests_are_internal())

    result = {
        "status": "failed" if errors else "passed",
        "migrated_tool_count": len(MIGRATED_TOOL_NAMES),
        "local_internal_matrixark_tests": sorted(LOCAL_INTERNAL_TOOL_NAMES),
        "corpus_tool_dirs": [str(path) for path in tool_dirs],
        "errors": errors,
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif errors:
        for error in errors:
            print(error, file=sys.stderr)
    else:
        print(f"matrixark shared test migration ok: migrated_tools={len(MIGRATED_TOOL_NAMES)} local_internal_tests={len(LOCAL_INTERNAL_TOOL_NAMES)}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
