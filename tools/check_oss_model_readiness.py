#!/usr/bin/env python3
"""Record local OSS reader/model readiness for benchmark runs.

This intentionally does not run LoCoMo or LongMemEval. It captures the local
model-serving state so benchmark reports do not overclaim that a stronger OSS
reader was installed or callable when it was only partially downloaded.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    parser.add_argument("--target-model", default="qwen2.5:7b")
    parser.add_argument("--pull-log", default="/tmp/ollama_pull_qwen25_7b_bg_20260726.log")
    parser.add_argument("--report", default="/tmp/oss_model_readiness_status.json")
    parser.add_argument(
        "--run-reader-gate",
        action="store_true",
        help="When the target model is installed and the endpoint is reachable, run the OSS reader capability gate.",
    )
    parser.add_argument(
        "--reader-gate-report",
        default="/tmp/oss_reader_capability_after_readiness.json",
        help="Output path for the optional reader capability gate.",
    )
    args = parser.parse_args()

    started = time.time()
    installed = list_ollama_models()
    report: dict[str, Any] = {
        "reader_base_url": args.reader_base_url,
        "target_model": args.target_model,
        "installed_models": installed,
        "target_installed": args.target_model in installed,
        "reader_endpoint_reachable": endpoint_reachable(args.reader_base_url),
        "pull_processes": process_lines(args.target_model),
        "pull_log": args.pull_log,
        "pull_progress": parse_pull_progress(Path(args.pull_log)),
    }
    report["ready_for_reader_gate"] = bool(
        report["target_installed"] and report["reader_endpoint_reachable"]
    )
    report["blockers"] = []
    if not report["target_installed"]:
        report["blockers"].append("target_model_not_installed")
    if not report["reader_endpoint_reachable"]:
        report["blockers"].append("reader_endpoint_unreachable")
    if args.run_reader_gate:
        report["reader_gate_requested"] = True
        if report["ready_for_reader_gate"]:
            report["reader_gate"] = run_reader_gate(args)
        else:
            report["reader_gate"] = {
                "status": "skipped",
                "reason": "target_model_not_ready",
                "report": args.reader_gate_report,
            }
    report["duration_seconds"] = round(time.time() - started, 3)
    Path(args.report).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(args.report)
    if args.run_reader_gate and report.get("reader_gate", {}).get("status") == "failed":
        return 2
    return 0 if report["ready_for_reader_gate"] else 1


def list_ollama_models() -> list[str]:
    try:
        proc = subprocess.run(
            ["ollama", "list"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception:
        return []
    models: list[str] = []
    for line in proc.stdout.splitlines()[1:]:
        parts = line.split()
        if parts:
            models.append(parts[0])
    return models


def endpoint_reachable(base_url: str) -> bool:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/models", timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def process_lines(model: str) -> list[str]:
    try:
        proc = subprocess.run(
            ["pgrep", "-fa", f"[o]llama pull {model}"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def parse_pull_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"available": False}
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = re.findall(
        r"pulling\s+\S+:\s+(\d+)%.*?(\d+(?:\.\d+)?)\s*([KMG]B)/(\d+(?:\.\d+)?)\s*([KMG]B)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not matches:
        return {"available": True, "parsed": False, "tail": text[-500:]}
    pct, done, done_unit, total, total_unit = matches[-1]
    return {
        "available": True,
        "parsed": True,
        "percent": int(pct),
        "downloaded": f"{done} {done_unit.upper()}",
        "total": f"{total} {total_unit.upper()}",
    }


def run_reader_gate(args: argparse.Namespace) -> dict[str, Any]:
    script = Path(__file__).resolve().with_name("probe_oss_reader_memory_capability.py")
    command = [
        sys.executable,
        str(script),
        "--reader-base-url",
        args.reader_base_url,
        "--reader-model",
        args.target_model,
        "--report",
        args.reader_gate_report,
    ]
    started = time.time()
    proc = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=600,
    )
    return {
        "status": "passed" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "duration_seconds": round(time.time() - started, 3),
        "report": args.reader_gate_report,
        "stdout": proc.stdout.strip()[-1000:],
        "stderr": proc.stderr.strip()[-1000:],
    }


if __name__ == "__main__":
    raise SystemExit(main())
