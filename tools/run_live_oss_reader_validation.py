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
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--provider-name", default="vikingmem-gpt-4o-mini-reader")
    parser.add_argument("--min-case-count", type=int, default=1)
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--report", default="/tmp/temporalstore_live_oss_reader_validation.json")
    parser.add_argument("--benchmark-report", default="/tmp/temporalstore_live_oss_reader_benchmark.json")
    parser.add_argument("--misses", default="/tmp/temporalstore_live_oss_reader_misses.jsonl")
    parser.add_argument("--max-events", type=int, default=128)
    parser.add_argument(
        "--benchmark-timeout-seconds",
        type=float,
        default=1800.0,
        help="Wall-clock timeout for the underlying benchmark runner. Timeout writes a fail-closed report.",
    )
    parser.add_argument(
        "--reader-timeout-seconds",
        type=float,
        default=20.0,
        help="Per OpenAI-compatible reader request timeout passed to the benchmark runner.",
    )
    parser.add_argument(
        "--reader-max-context-chars",
        type=int,
        default=12000,
        help="Maximum context chars sent to the OSS reader per question.",
    )
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help="Diagnostic-only evidence window passed through to the LOCOMO runner.",
    )
    parser.add_argument(
        "--skip-rust-temporalstore",
        action="store_true",
        help=(
            "Diagnostic-only reader run. This keeps the OpenAI-compatible OSS reader gate live "
            "when the local Rust harness toolchain is unavailable; reports from this mode are "
            "not full benchmark evidence."
        ),
    )
    parser.add_argument(
        "--allow-python-only-diagnostic",
        action="store_true",
        help="Required with --skip-rust-temporalstore so diagnostic reports are explicit.",
    )
    args = parser.parse_args()
    if args.skip_rust_temporalstore and not args.allow_python_only_diagnostic:
        print(
            "--skip-rust-temporalstore requires --allow-python-only-diagnostic; "
            "full benchmark evidence still requires the Rust TemporalStore backend.",
            file=sys.stderr,
        )
        return 2

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
        "benchmark_timeout_seconds": args.benchmark_timeout_seconds,
        "misses": args.misses,
        "reader_timeout_seconds": args.reader_timeout_seconds,
        "reader_max_context_chars": args.reader_max_context_chars,
        "evidence_window": args.evidence_window,
        "checked_at_unix": int(started),
        "python_only_diagnostic": bool(args.skip_rust_temporalstore),
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
            "--reader-timeout-seconds",
            str(args.reader_timeout_seconds),
            "--reader-max-context-chars",
            str(args.reader_max_context_chars),
        ]
    )
    if args.skip_rust_temporalstore:
        command.extend(["--skip-rust-temporalstore", "--allow-python-only-diagnostic"])
    evidence["command"] = command
    if args.evidence_window is not None:
        if args.dataset != "locomo":
            evidence["blockers"].append("evidence_window_only_supported_for_locomo")
            return finish(evidence, args.report, started, 2)
        command.extend(["--evidence-window", str(args.evidence_window)])
    try:
        result = subprocess.run(
            command,
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=args.benchmark_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        evidence["returncode"] = 124
        evidence["timed_out"] = True
        evidence["stdout_tail"] = tail_text(exc.stdout)
        evidence["stderr_tail"] = tail_text(exc.stderr)
        evidence["blockers"].append("benchmark_timeout")
        maybe_attach_benchmark(evidence, args.benchmark_report)
        return finish(evidence, args.report, started, 124)
    evidence["returncode"] = result.returncode
    evidence["stdout_tail"] = result.stdout[-4000:]
    evidence["stderr_tail"] = result.stderr[-4000:]
    if result.returncode != 0:
        evidence["blockers"].append("benchmark_gate_failed")
        maybe_attach_benchmark(evidence, args.benchmark_report)
        return finish(evidence, args.report, started, result.returncode)

    benchmark = json.loads(Path(args.benchmark_report).read_text(encoding="utf-8"))
    evidence["benchmark"] = benchmark
    evidence["reader_gate_ready"] = (
        benchmark.get("benchmark_quality_ready") is True
        and int(benchmark.get("reader_open_source_calls") or 0) > 0
        and int(benchmark.get("benchmark_threshold_violation_count") or 0) == 0
    )
    evidence["full_benchmark_evidence_ready"] = (
        evidence["reader_gate_ready"]
        and benchmark.get("paper_comparable_claim_ready") is True
        and benchmark.get("python_only_diagnostic") is not True
        and benchmark.get("rust_temporalstore_backend_ready") is True
    )
    evidence["ready"] = evidence["full_benchmark_evidence_ready"]
    if not evidence["reader_gate_ready"]:
        evidence["blockers"].append("reader_gate_not_ready")
    if benchmark.get("python_only_diagnostic") is True:
        evidence["blockers"].append("python_only_diagnostic")
    if benchmark.get("paper_comparable_claim_ready") is not True:
        evidence["blockers"].append("paper_comparable_claim_not_ready")
    if benchmark.get("rust_temporalstore_backend_ready") is not True:
        evidence["blockers"].append("rust_temporalstore_backend_not_ready")
    if not evidence["ready"]:
        evidence["blockers"].append("full_benchmark_evidence_not_ready")
    return finish(evidence, args.report, started, 0 if evidence["ready"] else 1)


def tail_text(value: str | bytes | None, limit: int = 4000) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value[-limit:]


def maybe_attach_benchmark(evidence: dict[str, Any], benchmark_report: str) -> None:
    path = Path(benchmark_report)
    if not path.exists():
        evidence["benchmark_report_present"] = False
        return
    evidence["benchmark_report_present"] = True
    try:
        evidence["benchmark"] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - keep the validator fail-closed but inspectable.
        evidence["benchmark_report_error"] = f"{type(exc).__name__}: {exc}"


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
