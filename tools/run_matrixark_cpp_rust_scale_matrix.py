#!/usr/bin/env python3
"""Run the canonical MatrixArk C++/Rust scale matrix after each fix.

The matrix intentionally reuses ``run_matrixark_cpp_rust_scale_report.py`` so
every cell has the same C++/Rust corpus, readiness gate, latency/QPS fields,
selected-ref parity, timeout counts, fallback flags, and stage metrics.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "run_matrixark_cpp_rust_scale_report.py"
Json = dict[str, Any]


def parse_int_list(raw: str) -> list[int]:
    values: list[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        values.append(int(part))
    return values


def load_json(path: Path) -> Json:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"status": "artifact_read_failed", "error": str(exc), "path": str(path)}


def selected_ref_parity_status(comparison: Json) -> str:
    for row in comparison.get("rows", []):
        if row.get("metric") == "selected_refs_avg":
            return "passed" if row.get("parity_passed") else "failed"
    return "not_available"


def fallback_flags(backends: Json) -> Json:
    return {
        backend: row.get("fallback_flags", {})
        for backend, row in backends.items()
        if isinstance(row, dict)
    }


def run_case(args: argparse.Namespace, *, events: int, retrieve_workers: int, run_id: str, artifact_root: Path) -> Json:
    case_id = f"events_{events}_retrieve_workers_{retrieve_workers}"
    case_dir = artifact_root / case_id
    case_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(RUNNER),
        "--events",
        str(events),
        "--retrieve-workers",
        str(retrieve_workers),
        "--artifact-dir",
        str(case_dir),
        "--raw-ops",
        str(args.raw_ops),
        "--raw-read-ops",
        str(args.raw_read_ops),
        "--raw-workers",
        str(args.raw_workers),
        "--messages-per-ingest",
        str(args.messages_per_ingest),
        "--ingest-workers",
        str(args.ingest_workers),
        "--retrieve-queries",
        str(args.retrieve_queries),
        "--max-context-tokens",
        str(args.max_context_tokens),
        "--request-timeout-ms",
        str(args.request_timeout_ms),
        "--io-timeout-ms",
        str(args.io_timeout_ms),
        "--readiness-timeout-ms",
        str(args.readiness_timeout_ms),
        "--ingest-deadline-ms",
        str(args.ingest_deadline_ms),
        "--retrieve-deadline-ms",
        str(args.retrieve_deadline_ms),
        "--storage-prefix",
        f"{args.storage_prefix}:{run_id}",
        "--metaserver",
        args.metaserver,
        "--namespace",
        args.namespace,
        "--table",
        args.table,
        "--cpp-lib",
        args.cpp_lib,
        "--rust-cli",
        args.rust_cli,
    ]
    if args.skip_context_pipeline:
        cmd.append("--skip-context-pipeline")
    started = time.perf_counter()
    completed = subprocess.run(cmd, cwd=str(ROOT), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    elapsed_s = round(time.perf_counter() - started, 3)
    report = load_json(case_dir / "comparison.json")
    comparison = report.get("comparison", {}) if isinstance(report.get("comparison"), dict) else {}
    backends = report.get("backends", {}) if isinstance(report.get("backends"), dict) else {}
    case_result = {
        "case_id": case_id,
        "events": events,
        "retrieve_workers": retrieve_workers,
        "status": "passed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "elapsed_s": elapsed_s,
        "artifact_dir": str(case_dir),
        "comparison_status": comparison.get("status", "missing"),
        "selected_ref_parity": selected_ref_parity_status(comparison),
        "fallback_flags": fallback_flags(backends),
        "backend_statuses": {backend: row.get("status") for backend, row in backends.items() if isinstance(row, dict)},
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }
    (case_dir / "case_result.json").write_text(json.dumps(case_result, indent=2, sort_keys=True), encoding="utf-8")
    return case_result


def write_summary_md(path: Path, summary: Json) -> None:
    lines = [
        "# MatrixArk C++/Rust Scale Matrix",
        "",
        f"- run_id: `{summary['run_id']}`",
        f"- generated_at_ms: `{summary['generated_at_ms']}`",
        f"- artifact_dir: `{summary['artifact_dir']}`",
        f"- event tiers: `{summary['config']['event_tiers']}`",
        f"- retrieve worker tiers: `{summary['config']['retrieve_worker_tiers']}`",
        "",
        "## Cases",
        "",
        "| case | events | workers | status | comparison | selected ref parity | C++ status | Rust status | artifact |",
        "|---|---:|---:|---|---|---|---|---|---|",
    ]
    for case in summary.get("cases", []):
        statuses = case.get("backend_statuses", {})
        lines.append(
            f"| {case['case_id']} | {case['events']} | {case['retrieve_workers']} | {case['status']} | "
            f"{case.get('comparison_status')} | {case.get('selected_ref_parity')} | "
            f"{statuses.get('cpp', '')} | {statuses.get('rust', '')} | `{case['artifact_dir']}` |"
        )
    lines.extend(
        [
            "",
            "## Required Fields",
            "",
            "Every case delegates to `run_matrixark_cpp_rust_scale_report.py`, which records:",
            "",
            "- 1K / 10K / 100K ingest tiers, or the explicit tiers passed to this runner.",
            "- retrieve worker tiers 4 / 8 / 16 / 32, or the explicit tiers passed to this runner.",
            "- same generated corpus for C++ and Rust within each case.",
            "- selected ref parity via `selected_refs_avg`.",
            "- p50/p95/p99, QPS, timeout counts, partial-pack counts, fallback flags.",
            "- per-stage retrieval metrics: query plan, node traversal, index prefilter, candidate fetch, score, pack, audit.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-tiers", default="1000,10000,100000")
    parser.add_argument("--retrieve-worker-tiers", default="4,8,16,32")
    parser.add_argument("--artifact-dir", default="")
    parser.add_argument("--storage-prefix", default="matrixark:scale_matrix")
    parser.add_argument("--raw-ops", type=int, default=1000)
    parser.add_argument("--raw-read-ops", type=int, default=500)
    parser.add_argument("--raw-workers", type=int, default=4)
    parser.add_argument("--messages-per-ingest", type=int, default=20)
    parser.add_argument("--ingest-workers", type=int, default=4)
    parser.add_argument("--retrieve-queries", type=int, default=128)
    parser.add_argument("--max-context-tokens", type=int, default=12000)
    parser.add_argument("--metaserver", default="127.0.0.1:18000")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument("--cpp-lib", default=str(ROOT / "output-ubuntu22/release/sdk/lib/libbcache2.so"))
    parser.add_argument("--rust-cli", default=str(ROOT / "sdk/rust/temporalstore/target/release/matrixark_record_log"))
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--readiness-timeout-ms", type=int, default=60000)
    parser.add_argument("--ingest-deadline-ms", type=int, default=60000)
    parser.add_argument("--retrieve-deadline-ms", type=int, default=10000)
    parser.add_argument("--skip-context-pipeline", action="store_true")
    parser.add_argument("--fail-on-case-failure", action="store_true")
    args = parser.parse_args()

    event_tiers = parse_int_list(args.event_tiers)
    worker_tiers = parse_int_list(args.retrieve_worker_tiers)
    run_id = str(int(time.time() * 1000))
    artifact_root = Path(args.artifact_dir) if args.artifact_dir else ROOT / "docs" / "benchmarks" / f"cpp_rust_scale_matrix_{run_id}"
    artifact_root.mkdir(parents=True, exist_ok=True)
    cases = [
        run_case(args, events=events, retrieve_workers=workers, run_id=run_id, artifact_root=artifact_root)
        for events in event_tiers
        for workers in worker_tiers
    ]
    summary: Json = {
        "run_id": run_id,
        "generated_at_ms": int(time.time() * 1000),
        "artifact_dir": str(artifact_root),
        "config": {
            "event_tiers": event_tiers,
            "retrieve_worker_tiers": worker_tiers,
            "messages_per_ingest": args.messages_per_ingest,
            "ingest_workers": args.ingest_workers,
            "retrieve_queries": args.retrieve_queries,
            "max_context_tokens": args.max_context_tokens,
        },
        "cases": cases,
        "status": "passed" if all(case["status"] == "passed" for case in cases) else "failed",
    }
    (artifact_root / "matrix_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    write_summary_md(artifact_root / "matrix_summary.md", summary)
    print(json.dumps({"artifact_dir": str(artifact_root), "status": summary["status"], "cases": len(cases)}, indent=2))
    if args.fail_on_case_failure and summary["status"] != "passed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
