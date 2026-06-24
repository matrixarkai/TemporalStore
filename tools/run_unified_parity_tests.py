#!/usr/bin/env python3
"""Run the unified TemporalStore C++/Rust parity contract.

Input API:
  --corpus third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
  --result-dir /tmp/temporalstore-unified-parity

Output API:
  unified_parity_report.json
  unified_parity_report.md

The same corpus drives Python schema validation, the C++ context contract, and
C++ parity awareness so test inputs, expected outputs, and command kinds do
not drift between implementations.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = Path(os.environ.get("TEMPORALSTORE_TEST_CORPUS", ROOT / "third_party" / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json"))
DEFAULT_RESULT_DIR = Path(os.environ.get("TEMPORALSTORE_UNIFIED_RESULT_DIR", "/tmp/temporalstore-unified-parity"))


def load_corpus(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def command_kinds(corpus: dict[str, Any]) -> list[str]:
    kinds = {
        step["command"]["kind"]
        for case in corpus.get("cases", [])
        for step in case.get("steps", [])
        if isinstance(step.get("command"), dict) and "kind" in step["command"]
    }
    return sorted(kinds)


def run_stage(name: str, argv: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None, skipped: bool = False) -> dict[str, Any]:
    if skipped:
        return {
            "name": name,
            "status": "skipped",
            "command": argv,
            "cwd": str(cwd),
            "duration_s": 0.0,
            "returncode": None,
            "stdout_tail": "",
            "stderr_tail": "",
        }
    started = time.time()
    proc = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    duration = time.time() - started
    return {
        "name": name,
        "status": "passed" if proc.returncode == 0 else "failed",
        "command": argv,
        "cwd": str(cwd),
        "duration_s": round(duration, 3),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def write_reports(report: dict[str, Any], result_dir: Path) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    json_path = result_dir / "unified_parity_report.json"
    md_path = result_dir / "unified_parity_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# TemporalStore Unified Parity Report",
        "",
        f"- status: `{report['status']}`",
        f"- corpus: `{report['input']['corpus']}`",
        f"- cases: `{report['input']['case_count']}`",
        f"- command kinds: `{report['input']['command_kind_count']}`",
        f"- context steps: `{report['input']['context_step_count']}`",
        "",
        "## Stages",
        "",
        "| Stage | Status | Duration | Command |",
        "| --- | --- | ---: | --- |",
    ]
    for stage in report["stages"]:
        command = " ".join(stage["command"])
        lines.append(f"| {stage['name']} | `{stage['status']}` | `{stage['duration_s']}` | `{command}` |")
    lines.extend([
        "",
        "## Input / Output Contract",
        "",
        "- Input is one JSON corpus with `schema_version`, `coverage`, `cases`, `steps`, and `command.kind`.",
        "- Output is one JSON report plus this Markdown report.",
        "- Python validates schema and API shape.",
        "- C++ validates behavior against the same command sequence.",
        "- Rust validation is run by the Rust repo against the same external corpus; this C++ wrapper can run its legacy Rust SDK stage with `--run-rust`.",
        "",
    ])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report["artifacts"] = {"json": str(json_path), "markdown": str(md_path)}
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--result-dir", type=Path, default=DEFAULT_RESULT_DIR)
    parser.add_argument("--validate-only", action="store_true", help="Run schema validation only.")
    parser.add_argument("--skip-cpp", action="store_true", help="Skip C++ behavior contract stage.")
    parser.add_argument("--run-rust", action="store_true", help="Also run the legacy Rust SDK stage from this C++ checkout.")
    args = parser.parse_args()

    corpus = args.corpus.resolve()
    corpus_data = load_corpus(corpus)
    kinds = command_kinds(corpus_data)
    context_step_count = sum(
        1
        for case in corpus_data.get("cases", [])
        for step in case.get("steps", [])
        if step.get("command", {}).get("kind", "").startswith("context_")
    )

    stages: list[dict[str, Any]] = []
    stages.append(
        run_stage(
            "python_schema",
            [sys.executable, "tools/run_temporalstore_unified_tests.py", "--corpus", str(corpus), "--validate-only"],
        )
    )
    stages.append(
        run_stage(
            "cpp_context_contract",
            ["bash", "tools/run_cpp_unified_context_contract.sh", str(corpus)],
            skipped=args.validate_only or args.skip_cpp,
        )
    )
    rust_env = os.environ.copy()
    rust_env["TEMPORALSTORE_UNIFIED_CORPUS"] = str(corpus)
    stages.append(
        run_stage(
            "rust_unified_corpus",
            ["cargo", "test", "--no-default-features", "--features", "proxy", "--test", "unified_corpus"],
            cwd=ROOT / "sdk" / "rust" / "temporalstore",
            env=rust_env,
            skipped=args.validate_only or not args.run_rust,
        )
    )

    failed = [stage for stage in stages if stage["status"] == "failed"]
    status = "failed" if failed else "passed"
    report: dict[str, Any] = {
        "status": status,
        "input": {
            "corpus": str(corpus),
            "schema_version": corpus_data.get("schema_version"),
            "name": corpus_data.get("name"),
            "case_count": len(corpus_data.get("cases", [])),
            "command_kind_count": len(kinds),
            "context_step_count": context_step_count,
            "command_kinds": kinds,
        },
        "stages": stages,
    }
    write_reports(report, args.result_dir.resolve())
    print(json.dumps({"status": status, "artifacts": report.get("artifacts", {}), "stages": [s["status"] for s in stages]}, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
