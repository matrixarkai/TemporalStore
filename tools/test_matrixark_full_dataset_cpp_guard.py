#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "tools" / "run_matrixark_full_dataset_cpp_benchmark.py"
SUFFIXES = (
    "result.json",
    "report.json",
    "report.md",
    "hypotheses.jsonl",
    "context_packs.jsonl",
    "judge.jsonl",
)


def write_artifacts(directory: Path, prefix: str, backend: str, questions: int = 1986) -> None:
    for suffix in SUFFIXES:
        path = directory / f"{prefix}.{suffix}"
        if suffix == "report.json":
            path.write_text(
                json.dumps(
                    {
                        "dataset": {
                            "name": "locomo",
                            "questions_run": questions,
                            "turns_ingested": 9363,
                        },
                        "retrieval_config": {"temporalstore_backend": backend},
                    }
                ),
                encoding="utf-8",
            )
        elif suffix.endswith(".json"):
            path.write_text("{}\n", encoding="utf-8")
        else:
            path.write_text("ok\n", encoding="utf-8")


def run_guard(directory: Path, prefix: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(GUARD),
            "--dataset",
            "locomo",
            "--artifact-dir",
            str(directory),
            "--artifact-prefix",
            prefix,
            "--validate-only",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="matrixark-cpp-guard-") as tmp:
        directory = Path(tmp)
        write_artifacts(directory, "cpp_run", "temporalstore-direct")
        accepted = run_guard(directory, "cpp_run")
        if accepted.returncode != 0:
            raise AssertionError(f"C++ report should pass. stderr={accepted.stderr} stdout={accepted.stdout}")
        if '"status": "validated"' not in accepted.stdout:
            raise AssertionError(f"missing validated status: {accepted.stdout}")

        write_artifacts(directory, "memory_run", "memory")
        rejected = run_guard(directory, "memory_run")
        if rejected.returncode == 0:
            raise AssertionError(f"memory report should fail. stdout={rejected.stdout}")
        if "not C++ TemporalStore-backed" not in rejected.stderr:
            raise AssertionError(f"unexpected rejection message: {rejected.stderr}")

    print("PASS matrixark_full_dataset_cpp_guard")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
