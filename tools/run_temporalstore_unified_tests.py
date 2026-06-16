#!/usr/bin/env python3
"""Run the shared TemporalStore C++/Rust behavioral corpus.

The JSON corpus is the test contract. Rust executes it through an integration
test. C++ should expose a runner command that accepts the same corpus path via
TS_CPP_UNIFIED_TEST_CMD, using "{corpus}" as an optional path placeholder.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"


def validate_corpus(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)

    if corpus.get("schema_version") != 1:
        raise SystemExit(f"{path}: unsupported schema_version={corpus.get('schema_version')!r}")
    cases = corpus.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit(f"{path}: cases must be a non-empty list")
    for case in cases:
        if not case.get("name"):
            raise SystemExit(f"{path}: every case must have a name")
        if not isinstance(case.get("shard_id"), int):
            raise SystemExit(f"{path}: case {case.get('name')!r} must have an integer shard_id")
        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{path}: case {case['name']} must have non-empty steps")
        for step in steps:
            if not step.get("name"):
                raise SystemExit(f"{path}: case {case['name']} has an unnamed step")
            if not isinstance(step.get("command"), dict):
                raise SystemExit(f"{path}: step {case['name']}/{step.get('name')} needs command")
            if "kind" not in step["command"]:
                raise SystemExit(f"{path}: step {case['name']}/{step['name']} command needs kind")
    return corpus


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(shlex.quote(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def run_rust(corpus: Path) -> None:
    env = os.environ.copy()
    env["TS_UNIFIED_TEMPORALSTORE_CORPUS"] = str(corpus)
    run(
        [
            "cargo",
            "test",
            "-p",
            "temporalstore-rust",
            "--test",
            "unified_temporalstore_corpus",
            "--",
            "--test-threads=1",
        ],
        env=env,
    )


def run_cpp(corpus: Path, required: bool) -> None:
    command = os.environ.get("TS_CPP_UNIFIED_TEST_CMD")
    if not command:
        message = (
            "TS_CPP_UNIFIED_TEST_CMD is not set; set it to the C++ corpus runner "
            "command, optionally using {corpus} as the corpus path placeholder"
        )
        if required:
            raise SystemExit(message)
        print(f"warning: {message}", file=sys.stderr)
        return

    if "{corpus}" in command:
        rendered = command.format(corpus=str(corpus))
        shell = True
    else:
        rendered = f"{command} {shlex.quote(str(corpus))}"
        shell = True
    print(f"+ {rendered}", flush=True)
    subprocess.run(rendered, cwd=ROOT, shell=shell, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--rust", action="store_true", help="run the Rust corpus executor")
    parser.add_argument("--cpp", action="store_true", help="run the C++ corpus executor")
    parser.add_argument(
        "--require-cpp",
        action="store_true",
        help="fail if --cpp is requested but TS_CPP_UNIFIED_TEST_CMD is unset",
    )
    parser.add_argument("--validate-only", action="store_true", help="only validate corpus JSON")
    args = parser.parse_args()

    corpus = args.corpus.resolve()
    data = validate_corpus(corpus)
    print(
        f"validated {data['name']} schema={data['schema_version']} "
        f"cases={len(data['cases'])} path={corpus}"
    )

    if args.validate_only:
        return 0
    if not args.rust and not args.cpp:
        args.rust = True
    if args.rust:
        run_rust(corpus)
    if args.cpp:
        run_cpp(corpus, args.require_cpp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
