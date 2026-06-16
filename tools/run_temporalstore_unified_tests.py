#!/usr/bin/env python3
"""Run the shared TemporalStore C++/Rust behavioral corpus.

The JSON corpus is the test contract. Rust executes it through an integration
test. C++ should expose a runner command that accepts the same corpus path via
TS_CPP_UNIFIED_TEST_CMD, using "{corpus}" as an optional path placeholder.
When TS_CPP_REPO or --cpp-repo is provided, the command also gets "{cpp_repo}"
rendered and otherwise runs from that repository root.
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
DEFAULT_CPP_RUNNER_RELATIVE = Path("tools") / "run_temporalstore_unified_tests.sh"

CPP_RUNNER_TEMPLATE = """#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus)
      CORPUS="${2:-}"
      shift 2
      ;;
    --corpus=*)
      CORPUS="${1#--corpus=}"
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${CORPUS}" ]]; then
  CORPUS="${ROOT}/compat/unified_temporalstore_cases.json"
fi
if [[ ! -f "${CORPUS}" ]]; then
  echo "unified TemporalStore corpus not found: ${CORPUS}" >&2
  exit 2
fi
if [[ -z "${TS_CPP_UNIFIED_NATIVE_CMD:-}" ]]; then
  cat >&2 <<'MSG'
TS_CPP_UNIFIED_NATIVE_CMD is not set.

Set it to the C++ TemporalStore corpus executor. The command may use a
{corpus} placeholder. Example:

  TS_CPP_UNIFIED_NATIVE_CMD='bazel run //temporalstore:corpus_runner -- {corpus}' \
    tools/run_temporalstore_unified_tests.sh --corpus /path/to/compat/unified_temporalstore_cases.json
MSG
  exit 2
fi

if [[ "${TS_CPP_UNIFIED_NATIVE_CMD}" == *"{corpus}"* ]]; then
  CMD="${TS_CPP_UNIFIED_NATIVE_CMD//\{corpus\}/${CORPUS}}"
else
  CMD="${TS_CPP_UNIFIED_NATIVE_CMD} ${CORPUS}"
fi

cd "${ROOT}"
echo "+ ${CMD}"
exec bash -lc "${CMD}"
"""


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


def render_cpp_command(command: str, corpus: Path, cpp_repo: Path | None) -> str:
    values = {"corpus": str(corpus)}
    if cpp_repo is not None:
        values["cpp_repo"] = str(cpp_repo)
    if "{corpus}" in command or "{cpp_repo}" in command:
        return command.format(**values)
    return f"{command} {shlex.quote(str(corpus))}"


def discover_cpp_command(cpp_repo: Path | None) -> str | None:
    command = os.environ.get("TS_CPP_UNIFIED_TEST_CMD")
    if command:
        return command
    if cpp_repo is None:
        return None
    candidate = cpp_repo / DEFAULT_CPP_RUNNER_RELATIVE
    if candidate.exists():
        return f"{shlex.quote(str(candidate))} --corpus {{corpus}}"
    return None


def install_cpp_runner(cpp_repo: Path, overwrite: bool) -> Path:
    target = cpp_repo / DEFAULT_CPP_RUNNER_RELATIVE
    if target.exists() and not overwrite:
        raise SystemExit(
            f"C++ unified runner already exists: {target}; pass --overwrite-cpp-runner to replace it"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(CPP_RUNNER_TEMPLATE, encoding="utf-8")
    target.chmod(0o755)
    return target


def run_cpp(corpus: Path, required: bool, cpp_repo: Path | None) -> None:
    command = discover_cpp_command(cpp_repo)
    if not command:
        message = (
            "no C++ unified corpus runner configured; set TS_CPP_UNIFIED_TEST_CMD "
            "to the C++ corpus runner command, optionally using {corpus} and "
            "{cpp_repo} placeholders, or set TS_CPP_REPO/--cpp-repo to a checkout "
            "containing tools/run_temporalstore_unified_tests.sh"
        )
        if required:
            raise SystemExit(message)
        print(f"warning: {message}", file=sys.stderr)
        return

    rendered = render_cpp_command(command, corpus, cpp_repo)
    cwd = cpp_repo if cpp_repo is not None else ROOT
    print(f"+ {rendered}", flush=True)
    subprocess.run(rendered, cwd=cwd, shell=True, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--rust", action="store_true", help="run the Rust corpus executor")
    parser.add_argument("--cpp", action="store_true", help="run the C++ corpus executor")
    parser.add_argument(
        "--both",
        action="store_true",
        help="run both Rust and C++ corpus executors",
    )
    parser.add_argument(
        "--cpp-repo",
        type=Path,
        default=Path(os.environ["TS_CPP_REPO"]) if os.environ.get("TS_CPP_REPO") else None,
        help="C++ TemporalStore checkout root; also used as cwd for the C++ runner",
    )
    parser.add_argument(
        "--require-cpp",
        action="store_true",
        help="fail if --cpp is requested but TS_CPP_UNIFIED_TEST_CMD is unset",
    )
    parser.add_argument("--validate-only", action="store_true", help="only validate corpus JSON")
    parser.add_argument(
        "--install-cpp-runner",
        action="store_true",
        help="write tools/run_temporalstore_unified_tests.sh into --cpp-repo using the shared wrapper contract",
    )
    parser.add_argument(
        "--overwrite-cpp-runner",
        action="store_true",
        help="allow --install-cpp-runner to replace an existing C++ wrapper",
    )
    parser.add_argument(
        "--print-cpp-runner-template",
        action="store_true",
        help="print the installable C++ wrapper template and exit",
    )
    args = parser.parse_args()

    if args.print_cpp_runner_template:
        print(CPP_RUNNER_TEMPLATE, end="")
        return 0

    corpus = args.corpus.resolve()
    data = validate_corpus(corpus)
    print(
        f"validated {data['name']} schema={data['schema_version']} "
        f"cases={len(data['cases'])} path={corpus}"
    )

    if args.validate_only:
        return 0
    install_only = args.install_cpp_runner and not args.rust and not args.cpp and not args.both
    if args.both:
        args.rust = True
        args.cpp = True
    if not args.rust and not args.cpp and not install_only:
        args.rust = True
    cpp_repo = args.cpp_repo.resolve() if args.cpp_repo is not None else None
    if cpp_repo is not None and not cpp_repo.exists():
        raise SystemExit(f"C++ repo does not exist: {cpp_repo}")
    if args.install_cpp_runner:
        if cpp_repo is None:
            raise SystemExit("--install-cpp-runner requires --cpp-repo or TS_CPP_REPO")
        target = install_cpp_runner(cpp_repo, args.overwrite_cpp_runner)
        print(f"installed C++ unified runner: {target}")
        if install_only:
            return 0
    if args.rust:
        run_rust(corpus)
    if args.cpp:
        run_cpp(corpus, args.require_cpp, cpp_repo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
