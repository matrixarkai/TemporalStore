#!/usr/bin/env python3
"""Validate Rust Raft with the unified C++ Raft case definitions.

The shared corpus is the contract: every C++ Raft case names the C++ runner
surface and the Rust harness/validator that must prove the same behavior. This
script reads those C++ Raft cases, optionally checks their C++ required paths,
then runs the Rust combined data-node/metaserver Raft parity gate once.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
DEFAULT_ARTIFACT_DIR = Path("/tmp/temporalstore-cpp-raft-cases-on-rust")
RAFT_SUITE = "cpp_data_raft_parity"


def load_corpus(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def raft_cases(corpus: dict[str, Any]) -> list[dict[str, Any]]:
    required = set(corpus.get("coverage", {}).get("required_raft_case_names", []))
    cases = []
    for case in corpus.get("cases", []):
        steps = case.get("steps", [])
        has_raft_step = any(step.get("command", {}).get("suite") == RAFT_SUITE for step in steps)
        if case.get("name") in required or has_raft_step:
            cases.append(case)
    return cases


def validate_cpp_paths(cases: list[dict[str, Any]], cpp_repo: Path | None) -> list[str]:
    if cpp_repo is None:
        return []
    missing = []
    for case in cases:
        for step in case.get("steps", []):
            command = step.get("command", {})
            if command.get("suite") != RAFT_SUITE:
                continue
            for rel in command.get("required_paths", []):
                if not (cpp_repo / rel).exists():
                    missing.append(f"{case['name']}/{step['name']}: {rel}")
    return missing


def build_mapping(cases: list[dict[str, Any]], cpp_repo: Path | None, missing_paths: list[str]) -> dict[str, Any]:
    out_cases = []
    for case in cases:
        out_steps = []
        for step in case.get("steps", []):
            command = step.get("command", {})
            if command.get("suite") != RAFT_SUITE:
                continue
            out_steps.append(
                {
                    "name": step["name"],
                    "cpp_runner": command.get("runner"),
                    "rust_runner": command.get("rust_runner"),
                    "rust_validator": command.get("rust_validator"),
                    "required_paths": command.get("required_paths", []),
                }
            )
        if out_steps:
            out_cases.append(
                {
                    "name": case["name"],
                    "rust_parity_gate": case.get("rust_parity_gate"),
                    "rust_parity_validator": case.get("rust_parity_validator"),
                    "steps": out_steps,
                }
            )
    return {
        "schema": "temporalstore-cpp-raft-cases-on-rust-v1",
        "cpp_repo": str(cpp_repo) if cpp_repo is not None else None,
        "cpp_required_paths_checked": cpp_repo is not None,
        "missing_cpp_required_paths": missing_paths,
        "case_count": len(out_cases),
        "step_count": sum(len(case["steps"]) for case in out_cases),
        "cases": out_cases,
    }


def run_command(command: list[str], env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument(
        "--cpp-repo",
        type=Path,
        default=Path(os.environ["TS_CPP_REPO"]) if os.environ.get("TS_CPP_REPO") else None,
        help="C++ TemporalStore checkout used to verify required C++ Raft paths",
    )
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument("--timeout", default="300s")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    corpus = load_corpus(args.corpus)
    cases = raft_cases(corpus)
    missing_paths = validate_cpp_paths(cases, args.cpp_repo)
    report = build_mapping(cases, args.cpp_repo, missing_paths)

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.artifact_dir / "cpp-raft-cases-on-rust.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if missing_paths:
        raise SystemExit(
            "missing C++ Raft required paths:\n" + "\n".join(f"- {item}" for item in missing_paths)
        )
    if report["case_count"] == 0:
        raise SystemExit("no C++ Raft cases found in unified corpus")

    run_command(["python3", "tools/run_temporalstore_unified_tests.py", "--validate-only"])
    if args.cpp_repo is not None:
        run_command(
            [
                "python3",
                "tools/validate_raft_storage_parity_evidence.py",
                "--cpp-repo",
                str(args.cpp_repo),
            ]
        )
    else:
        run_command(["python3", "tools/validate_raft_storage_parity_evidence.py"])

    if not args.validate_only:
        env = os.environ.copy()
        env["TS_RAFT_PARITY_ARTIFACT_DIR"] = str(args.artifact_dir / "rust-raft-parity")
        env["TS_RAFT_PARITY_TIMEOUT"] = args.timeout
        run_command(["bash", "tools/run_raft_distributed_parity.sh"], env=env)

    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"TemporalStore Rust Raft validated from C++ Raft cases. Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
