#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


Json = dict[str, Any]
CANONICAL_SUFFIXES = (
    "result.json",
    "report.json",
    "report.md",
    "hypotheses.jsonl",
    "context_packs.jsonl",
    "judge.jsonl",
)
CPP_BACKENDS = {"cpp", "temporalstore-direct", "temporalstore_direct", "c++", "cxx"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed wrapper/validator for full MatrixArk LOCOMO and LongMemEval "
            "benchmark runs. Full dataset runs must use C++ TemporalStore storage."
        )
    )
    parser.add_argument("--dataset", choices=["locomo", "longmemeval_s"], required=True)
    parser.add_argument("--artifact-dir", required=True, help="Directory containing or receiving canonical artifacts.")
    parser.add_argument(
        "--artifact-prefix",
        required=True,
        help=(
            "Canonical artifact prefix. For prefix run1, the wrapper validates "
            "run1.result.json, run1.report.json, run1.report.md, run1.hypotheses.jsonl, "
            "run1.context_packs.jsonl, and run1.judge.jsonl."
        ),
    )
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument(
        "--storage-prefix",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", ""),
        help="TemporalStore storage prefix. If omitted, a deterministic prefix is derived from dataset/artifact prefix.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Logical session batch size for full benchmark ingestion.",
    )
    parser.add_argument(
        "--temporalstore-lib",
        default=os.environ.get("TEMPORALSTORE_LIB", ""),
        help="Path to libbcache2.so used by the Python direct SDK.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing artifacts; do not execute a benchmark command.",
    )
    parser.add_argument(
        "--allow-missing-artifacts",
        action="store_true",
        help="For preflight only. Validate backend intent but do not require completed artifacts.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Benchmark command to run after '--'. It receives C++ TemporalStore env vars.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Json:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:  # pragma: no cover - message is user-facing.
        raise SystemExit(f"failed to read JSON artifact {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"JSON artifact must be an object: {path}")
    return data


def nested_values(data: Any, key_names: set[str]) -> list[Any]:
    values: list[Any] = []
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in key_names:
                values.append(value)
            values.extend(nested_values(value, key_names))
    elif isinstance(data, list):
        for item in data:
            values.extend(nested_values(item, key_names))
    return values


def normalized_backend(value: Any) -> str:
    return str(value or "").strip().lower()


def report_backend(report: Json) -> str:
    candidates = nested_values(report, {"temporalstore_backend", "storage_backend", "backend"})
    for value in candidates:
        backend = normalized_backend(value)
        if backend in CPP_BACKENDS or backend == "memory":
            return backend
    return ""


def dataset_shape(report: Json) -> Json:
    dataset = report.get("dataset", {})
    return dataset if isinstance(dataset, dict) else {}


def validate_artifacts(args: argparse.Namespace) -> Json:
    artifact_dir = Path(args.artifact_dir)
    prefix = args.artifact_prefix
    paths = {suffix: artifact_dir / f"{prefix}.{suffix}" for suffix in CANONICAL_SUFFIXES}
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing and not args.allow_missing_artifacts:
        raise SystemExit("missing canonical benchmark artifacts:\n" + "\n".join(missing))

    if args.allow_missing_artifacts and missing:
        return {
            "status": "preflight_only",
            "dataset": args.dataset,
            "artifact_prefix": prefix,
            "missing_artifacts": missing,
            "required_backend": "temporalstore-direct",
        }

    report = load_json(paths["report.json"])
    backend = report_backend(report)
    if backend not in CPP_BACKENDS:
        raise SystemExit(
            f"full {args.dataset} benchmark artifact is not C++ TemporalStore-backed: "
            f"{paths['report.json']} reports backend={backend or '<missing>'}"
        )

    shape = dataset_shape(report)
    dataset_name = str(shape.get("name", report.get("dataset_name", ""))).lower()
    if args.dataset == "locomo" and "locomo" not in dataset_name:
        raise SystemExit(f"report dataset mismatch: expected locomo, got {dataset_name or '<missing>'}")
    if args.dataset == "longmemeval_s" and "longmemeval" not in dataset_name:
        raise SystemExit(f"report dataset mismatch: expected longmemeval_s, got {dataset_name or '<missing>'}")

    questions = int(shape.get("questions_run") or report.get("questions_run") or 0)
    if args.dataset == "locomo" and questions < 1000:
        raise SystemExit(f"LOCOMO full dataset run is too small: questions_run={questions}")
    if args.dataset == "longmemeval_s" and questions < 500:
        raise SystemExit(f"LongMemEval_s full dataset run is too small: questions_run={questions}")

    return {
        "status": "validated",
        "dataset": args.dataset,
        "backend": backend,
        "questions_run": questions,
        "artifact_dir": str(artifact_dir),
        "artifact_prefix": prefix,
        "artifacts": {suffix: str(path) for suffix, path in paths.items()},
    }


def run_command(args: argparse.Namespace) -> int:
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("benchmark command is required unless --validate-only is set")
    if args.batch_size < 20:
        raise SystemExit("--batch-size must be at least 20 for full VikingMem-style benchmark ingestion")

    storage_prefix = args.storage_prefix or f"matrixark:bench:{args.dataset}:{args.artifact_prefix}"
    env = os.environ.copy()
    env.update(
        {
            "MATRIXARK_MCP_BACKEND": "temporalstore-direct",
            "MATRIXARK_FULL_DATASET_REQUIRE_CPP": "1",
            "MATRIXARK_TEMPORALSTORE_BACKEND": "temporalstore-direct",
            "MATRIXARK_TEMPORALSTORE_METASERVER": args.metaserver,
            "MATRIXARK_TEMPORALSTORE_NAMESPACE": args.namespace,
            "MATRIXARK_TEMPORALSTORE_TABLE": args.table,
            "MATRIXARK_TEMPORALSTORE_PREFIX": storage_prefix,
            "MATRIXARK_INGEST_MODE": "batch",
            "MATRIXARK_BATCH_SIZE": str(args.batch_size),
        }
    )
    if args.temporalstore_lib:
        env["TEMPORALSTORE_LIB"] = args.temporalstore_lib

    completed = subprocess.run(command, env=env, check=False)
    return int(completed.returncode)


def main() -> int:
    args = parse_args()
    if not args.validate_only:
        code = run_command(args)
        if code != 0:
            return code

    result = validate_artifacts(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
