#!/usr/bin/env python3
"""Validate a live OpenAI-compatible OSS reader for LOCOMO/LongMemEval gates."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description="Run fail-closed live OSS reader validation.")
    parser.add_argument("--dataset", choices=("locomo", "longmemeval_s"), default="locomo")
    parser.add_argument("--input", default="/tmp/locomo10.json")
    parser.add_argument("--base-url", default=os.environ.get("TEMPORALSTORE_READER_BASE_URL", ""))
    parser.add_argument("--model", default="google/flan-t5-small")
    parser.add_argument("--provider-name", default="matrixark-cpp-oss-context")
    parser.add_argument("--min-case-count", type=int, default=1)
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--report", default="/tmp/temporalstore_live_oss_reader_validation.json")
    parser.add_argument("--benchmark-report", default="/tmp/temporalstore_live_oss_reader_benchmark.json")
    parser.add_argument("--misses", default="/tmp/temporalstore_live_oss_reader_misses.jsonl")
    parser.add_argument("--max-events", type=int, default=128)
    args = parser.parse_args()

    started = time.time()
    evidence: dict[str, Any] = {
        "ready": False,
        "dataset": args.dataset,
        "input": args.input,
        "base_url": args.base_url,
        "provider_name": args.provider_name,
        "model": args.model,
        "min_case_count": args.min_case_count,
        "min_hit_rate": args.min_hit_rate,
        "benchmark_report": args.benchmark_report,
        "misses": args.misses,
        "checked_at_unix": int(started),
        "blockers": [],
    }

    input_path = Path(args.input)
    if not input_path.exists():
        evidence["blockers"].append(f"missing_input:{args.input}")
        return finish(evidence, args.report, started, 2)
    evidence["input_bytes"] = input_path.stat().st_size

    if not args.base_url:
        evidence["blockers"].append("missing_reader_base_url")
        return finish(evidence, args.report, started, 2)

    probe = probe_reader(args.base_url)
    evidence["reader_probe"] = probe
    if not probe["ok"]:
        evidence["blockers"].append("reader_gateway_unreachable")
        return finish(evidence, args.report, started, 2)

    repo = Path(__file__).resolve().parents[1]
    if args.dataset == "locomo":
        command = [
            sys.executable,
            str(repo / "tools" / "run_locomo_90_hit_rate.py"),
        ]
    else:
        command = [
            sys.executable,
            str(repo / "tools" / "run_longmemeval_s_full_path.py"),
        ]
    command.extend(
        [
            "--input",
            args.input,
            "--min-case-count",
            str(args.min_case_count),
            "--min-hit-rate",
            str(args.min_hit_rate),
            "--reader-mode",
            "open-source",
            "--reader-base-url",
            args.base_url,
            "--reader-provider-name",
            args.provider_name,
            "--reader-model",
            args.model,
            "--reader-no-fallback",
            "--require-open-source-reader",
            "--report",
            args.benchmark_report,
            "--misses",
            args.misses,
            "--max-events",
            str(args.max_events),
        ]
    )
    result = subprocess.run(command, cwd=repo, text=True, capture_output=True)
    evidence["command"] = command
    evidence["returncode"] = result.returncode
    evidence["stdout_tail"] = result.stdout[-4000:]
    evidence["stderr_tail"] = result.stderr[-4000:]
    if result.returncode != 0:
        evidence["blockers"].append("benchmark_gate_failed")
        if Path(args.benchmark_report).exists():
            evidence["benchmark"] = json.loads(Path(args.benchmark_report).read_text(encoding="utf-8"))
        return finish(evidence, args.report, started, result.returncode)

    benchmark = json.loads(Path(args.benchmark_report).read_text(encoding="utf-8"))
    evidence["benchmark"] = benchmark
    evidence["ready"] = (
        benchmark.get("benchmark_quality_ready") is True
        and int(benchmark.get("reader_open_source_calls") or 0) > 0
        and int(benchmark.get("benchmark_threshold_violation_count") or 0) == 0
    )
    if not evidence["ready"]:
        evidence["blockers"].append("benchmark_report_not_ready")
    return finish(evidence, args.report, started, 0 if evidence["ready"] else 1)


def probe_reader(base_url: str) -> dict[str, Any]:
    models_url = base_url.rstrip("/") + "/models"
    try:
        with urllib.request.urlopen(models_url, timeout=3.0) as response:
            body = response.read(2048).decode("utf-8", errors="replace")
        return {"ok": True, "url": models_url, "status": 200, "body_prefix": body[:500]}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "url": models_url, "status": exc.code, "error": exc.reason}
    except Exception as exc:  # noqa: BLE001 - evidence report should capture local gateway failures.
        return {"ok": False, "url": models_url, "error": f"{type(exc).__name__}: {exc}"}


def finish(evidence: dict[str, Any], report: str, started: float, code: int) -> int:
    evidence["elapsed_seconds"] = round(time.time() - started, 3)
    Path(report).write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
