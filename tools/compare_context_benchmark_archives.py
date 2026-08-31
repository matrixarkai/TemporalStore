#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Compare Rust and Context benchmark archive directories.

Each archive must contain a ``manifest.json`` plus report JSON files for every
executed dataset. Skipped datasets are allowed only when both sides explicitly
record a skipped/not-run status. Executed report pairs are delegated to
``compare_context_benchmark_reports.py`` so archive-level validation stays tied
to the shared MatrixArk/baseline benchmark contract.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORT_COMPARATOR = ROOT / "tools" / "compare_context_benchmark_reports.py"
DEFAULT_DATASETS = ("locomo", "longmemeval_s")
SKIPPED_STATUSES = {
    "skipped",
    "skipped_missing_input",
    "not_run",
    "missing_input",
    "blocked",
}
PASSED_STATUSES = {"passed", "ready"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-archive", type=Path, required=True)
    parser.add_argument("--native-archive", type=Path, required=True)
    parser.add_argument("--case-name", default="context_benchmark_full_dataset_gates")
    parser.add_argument("--datasets", nargs="*", default=list(DEFAULT_DATASETS))
    parser.add_argument("--numeric-tolerance", type=float, default=1e-9)
    parser.add_argument("--latency-ratio-tolerance", type=float, default=5.0)
    parser.add_argument("--require-executed", action="store_true")
    parser.add_argument(
        "--truth-mode",
        choices=("contract", "production"),
        default="contract",
        help=(
            "contract validates shape and matched skip/pass statuses; production requires every "
            "requested real dataset to execute and pass thresholds on both sides."
        ),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    rust_manifest = load_manifest(args.rust_archive)
    native_manifest = load_manifest(args.native_archive)
    failures: list[str] = []
    truth_blockers: list[str] = []
    dataset_results = []
    miss_totals = {
        "rust_only": 0,
        "native_only": 0,
        "shared_hard": 0,
        "reader_rust_only": 0,
        "reader_only": 0,
        "reader_shared_hard": 0,
    }
    summary_compare: dict[str, Any] = {}
    category_compare: dict[str, Any] = {}
    token_reduction_delta: dict[str, Any] = {}
    latency_deltas: dict[str, Any] = {}
    require_executed = args.require_executed or args.truth_mode == "production"

    compare_manifest_field("reader_model", rust_manifest, native_manifest, failures)
    compare_manifest_field("reader_base_url", rust_manifest, native_manifest, failures, required=False)

    for dataset in args.datasets:
        rust_status = dataset_status(rust_manifest, dataset)
        native_status = dataset_status(native_manifest, dataset)
        result: dict[str, Any] = {
            "dataset": dataset,
            "rust_status": rust_status,
            "native_status": native_status,
        }
        if rust_status in PASSED_STATUSES and native_status in PASSED_STATUSES:
            rust_report = find_report(args.rust_archive, dataset)
            native_report = find_report(args.native_archive, dataset)
            result["rust_report"] = str(rust_report)
            result["native_report"] = str(native_report)
            if rust_report is None or native_report is None:
                failures.append(
                    f"{dataset}: passed status requires both report files "
                    f"(rust={rust_report}, native={native_report})"
                )
                result["ready"] = False
            else:
                compare_result = run_report_compare(
                    rust_report,
                    native_report,
                    args.case_name,
                    dataset,
                    args.numeric_tolerance,
                    args.latency_ratio_tolerance,
                )
                result["ready"] = compare_result["ready"]
                result["report_compare"] = compare_result
                miss_totals["rust_only"] += int(compare_result.get("rust_only_miss_count") or 0)
                miss_totals["native_only"] += int(compare_result.get("native_only_miss_count") or 0)
                miss_totals["shared_hard"] += int(compare_result.get("shared_hard_miss_count") or 0)
                miss_totals["reader_rust_only"] += int(compare_result.get("reader_rust_only_miss_count") or 0)
                miss_totals["reader_only"] += int(compare_result.get("reader_only_miss_count") or 0)
                miss_totals["reader_shared_hard"] += int(compare_result.get("reader_shared_hard_miss_count") or 0)
                failures.extend(f"{dataset}: {item}" for item in compare_result["failures"])
                summary_compare[dataset] = compare_result.get("summary_compare") or {}
                category_compare[dataset] = compare_result.get("category_compare") or {}
                token_reduction_delta[dataset] = compare_result.get("token_reduction_delta")
                latency_deltas[dataset] = compare_result.get("latency_deltas") or {}
                if not compare_result["ready"]:
                    truth_blockers.append(f"{dataset}: report comparison failed")
        elif rust_status in SKIPPED_STATUSES and native_status in SKIPPED_STATUSES:
            result["ready"] = not require_executed
            if require_executed:
                failures.append(f"{dataset}: execution required but both archives skipped")
                truth_blockers.append(f"{dataset}: skipped on both sides")
        else:
            result["ready"] = False
            failures.append(f"{dataset}: status mismatch rust={rust_status!r} native={native_status!r}")
            truth_blockers.append(f"{dataset}: status mismatch")
        dataset_results.append(result)

    executed_dataset_count = sum(
        1
        for result in dataset_results
        if result["rust_status"] in PASSED_STATUSES and result["native_status"] in PASSED_STATUSES
    )
    benchmark_truth_ready = not failures and (
        args.truth_mode == "contract" or executed_dataset_count == len(dataset_results)
    )
    if args.truth_mode == "production" and executed_dataset_count != len(dataset_results):
        truth_blockers.append("not all requested datasets executed")

    report = {
        "format": "matrixark_external_baseline_context_benchmark_archive_compare_v1",
        "ready": not failures,
        "benchmark_truth_ready": benchmark_truth_ready,
        "truth_mode": args.truth_mode,
        "truth_blockers": sorted(set(truth_blockers)),
        "rust_archive": str(args.rust_archive),
        "native_archive": str(args.native_archive),
        "case_name": args.case_name,
        "dataset_count": len(dataset_results),
        "executed_dataset_count": executed_dataset_count,
        "miss_totals": miss_totals,
        "summary_compare": summary_compare,
        "category_compare": category_compare,
        "token_reduction_delta": token_reduction_delta,
        "latency_deltas": latency_deltas,
        "failure_count": len(failures),
        "failures": failures,
        "datasets": dataset_results,
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if report["ready"] else 1


def load_manifest(archive: Path) -> dict[str, Any]:
    path = archive / "manifest.json"
    if not path.exists():
        raise SystemExit(f"{path}: missing manifest")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - local validator should surface exact file issue.
        raise SystemExit(f"{path}: failed to parse manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise SystemExit(f"{path}: manifest must be a JSON object")
    return manifest


def compare_manifest_field(
    field: str,
    rust_manifest: dict[str, Any],
    native_manifest: dict[str, Any],
    failures: list[str],
    *,
    required: bool = True,
) -> None:
    rust_value = rust_manifest.get(field)
    native_value = native_manifest.get(field)
    if rust_value is None or native_value is None:
        if required:
            failures.append(f"manifest.{field}: both manifests must include this field")
        return
    if rust_value != native_value:
        failures.append(f"manifest.{field}: rust={rust_value!r} native={native_value!r}")


def dataset_status(manifest: dict[str, Any], dataset: str) -> str:
    direct = manifest.get(f"{dataset}_status")
    if isinstance(direct, str) and direct:
        return direct
    aliases = {
        "longmemeval_s": "longmemeval_status",
        "locomo": "locomo_status",
    }
    alias = aliases.get(dataset)
    if alias:
        alias_value = manifest.get(alias)
        if isinstance(alias_value, str) and alias_value:
            return alias_value
    datasets = manifest.get("datasets")
    if isinstance(datasets, list):
        for item in datasets:
            if isinstance(item, dict) and item.get("name") == dataset and isinstance(item.get("status"), str):
                return item["status"]
    return "not_run"


def find_report(archive: Path, dataset: str) -> Path | None:
    candidates = [
        archive / f"{dataset}_report.json",
        archive / f"{dataset.replace('_', '-')}_report.json",
    ]
    if dataset == "locomo":
        candidates.append(archive / "locomo_report.json")
    if dataset == "longmemeval_s":
        candidates.append(archive / "longmemeval_s_report.json")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def run_report_compare(
    rust_report: Path,
    native_report: Path,
    case_name: str,
    dataset: str,
    numeric_tolerance: float,
    latency_ratio_tolerance: float,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(REPORT_COMPARATOR),
        "--rust-report",
        str(rust_report),
        "--native-report",
        str(native_report),
        "--case-name",
        case_name,
        "--dataset",
        dataset,
        "--numeric-tolerance",
        str(numeric_tolerance),
        "--latency-ratio-tolerance",
        str(latency_ratio_tolerance),
    ]
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    if completed.returncode not in (0, 1):
        return {
            "ready": False,
            "failures": [
                f"report comparator failed with exit {completed.returncode}: "
                f"{completed.stderr.strip() or completed.stdout.strip()}"
            ],
        }
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {
            "ready": False,
            "failures": [f"report comparator emitted invalid JSON: {exc}"],
        }


if __name__ == "__main__":
    raise SystemExit(main())
