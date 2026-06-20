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
CPP_RAFT_PARITY_SUITE = "cpp_data_raft_parity"
COMBINED_RAFT_GATE_CASES = {
    "raft_data_node_scale_failover_snapshot",
    "raft_data_node_mixed_rw_and_membership",
    "raft_production_gate",
}
COMBINED_RAFT_GATE = "tools/run_raft_distributed_parity.sh"
COMBINED_RAFT_VALIDATOR = (
    "python3 tools/validate_aws_validation_log.py --job "
    "temporalstore-raft-distributed-parity-validation --log <raft-distributed-parity.json>"
)
BENCHMARK_REQUIRED_REPORT_FIELDS = {
    "benchmark_family",
    "benchmark_hit_at_k",
    "benchmark_recall_at_k",
    "benchmark_mean_reciprocal_rank",
    "benchmark_token_reduction_percent",
    "benchmark_retrieval_p50_ms",
    "benchmark_retrieval_p95_ms",
    "benchmark_reader_p50_ms",
    "benchmark_reader_p95_ms",
    "benchmark_quality_ready",
    "benchmark_threshold_passed",
    "benchmark_threshold_violation_count",
    "benchmark_threshold_violations",
    "benchmark_thresholds",
    "benchmark_per_query_count",
    "case_count",
    "hit_rate",
    "reader_hit_rate",
    "reader_mode_requested",
    "reader_mode_effective",
    "reader_provider_name",
    "reader_model",
}
BENCHMARK_THRESHOLD_PROFILES = {
    "fixture",
    "locomo_full",
    "longmemeval_full",
    "oss_reader_full",
}

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
    coverage = corpus.get("coverage")
    if not isinstance(coverage, dict):
        raise SystemExit(f"{path}: coverage must declare required shared C++/Rust test families")
    required_case_names = coverage.get("required_case_names")
    required_raft_case_names = coverage.get("required_raft_case_names", [])
    required_command_kinds = coverage.get("required_command_kinds")
    required_response_kinds = coverage.get("required_response_kinds")
    for field_name, values in [
        ("required_case_names", required_case_names),
        ("required_raft_case_names", required_raft_case_names),
        ("required_command_kinds", required_command_kinds),
        ("required_response_kinds", required_response_kinds),
    ]:
        if not isinstance(values, list) or not values or not all(
            isinstance(value, str) and value for value in values
        ):
            raise SystemExit(f"{path}: coverage.{field_name} must be a non-empty string list")

    seen_case_names = set()
    seen_command_kinds = set()
    seen_response_kinds = set()
    for case in cases:
        if not case.get("name"):
            raise SystemExit(f"{path}: every case must have a name")
        if case["name"] in seen_case_names:
            raise SystemExit(f"{path}: duplicate case name {case['name']}")
        seen_case_names.add(case["name"])
        if not isinstance(case.get("shard_id"), int):
            raise SystemExit(f"{path}: case {case.get('name')!r} must have an integer shard_id")
        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{path}: case {case['name']} must have non-empty steps")
        case_step_names = set()
        case_command_signatures = set()
        for step in steps:
            if not step.get("name"):
                raise SystemExit(f"{path}: case {case['name']} has an unnamed step")
            if step["name"] in case_step_names:
                raise SystemExit(f"{path}: duplicate step name {case['name']}/{step['name']}")
            case_step_names.add(step["name"])
            if not isinstance(step.get("command"), dict):
                raise SystemExit(f"{path}: step {case['name']}/{step.get('name')} needs command")
            if "kind" not in step["command"]:
                raise SystemExit(f"{path}: step {case['name']}/{step['name']} command needs kind")
            command_signature = json.dumps(step["command"], sort_keys=True, separators=(",", ":"))
            if command_signature in case_command_signatures:
                raise SystemExit(
                    f"{path}: duplicate command payload in case {case['name']} "
                    f"at step {step['name']}"
                )
            case_command_signatures.add(command_signature)
            seen_command_kinds.add(step["command"]["kind"])
            if "expect" in step:
                if not isinstance(step["expect"], dict) or "kind" not in step["expect"]:
                    raise SystemExit(
                        f"{path}: step {case['name']}/{step['name']} expect needs kind"
                )
                seen_response_kinds.add(step["expect"]["kind"])
            if step["command"].get("suite") == CPP_RAFT_PARITY_SUITE:
                validate_cpp_raft_step(path, case, step)
            if step["command"].get("suite") == "cpp_context_benchmark_parity":
                validate_context_benchmark_step(path, case, step)
        if case["name"] in COMBINED_RAFT_GATE_CASES:
            validate_combined_raft_case(path, case)
    missing_cases = sorted(set(required_case_names) - seen_case_names)
    missing_raft_cases = sorted(set(required_raft_case_names) - seen_case_names)
    missing_commands = sorted(set(required_command_kinds) - seen_command_kinds)
    missing_responses = sorted(set(required_response_kinds) - seen_response_kinds)
    if missing_cases:
        raise SystemExit(f"{path}: missing required cases: {', '.join(missing_cases)}")
    if missing_raft_cases:
        raise SystemExit(f"{path}: missing required Raft cases: {', '.join(missing_raft_cases)}")
    if missing_commands:
        raise SystemExit(f"{path}: missing required command kinds: {', '.join(missing_commands)}")
    if missing_responses:
        raise SystemExit(f"{path}: missing required response kinds: {', '.join(missing_responses)}")
    return corpus


def validate_context_benchmark_step(path: Path, case: dict, step: dict) -> None:
    location = f"{path}: case {case['name']} step {step['name']}"
    command = step["command"]
    contract = command.get("report_contract")
    if not isinstance(contract, dict):
        raise SystemExit(f"{location}: context benchmark step must declare report_contract")
    fields = contract.get("required_fields")
    if not isinstance(fields, list) or not fields:
        raise SystemExit(f"{location}: report_contract.required_fields must be a non-empty list")
    missing_fields = sorted(BENCHMARK_REQUIRED_REPORT_FIELDS - set(fields))
    if missing_fields:
        raise SystemExit(
            f"{location}: report_contract.required_fields missing {', '.join(missing_fields)}"
        )
    threshold_profiles = command.get("threshold_profiles")
    if not isinstance(threshold_profiles, list) or not threshold_profiles:
        raise SystemExit(f"{location}: threshold_profiles must be a non-empty list")
    unknown_profiles = sorted(set(threshold_profiles) - BENCHMARK_THRESHOLD_PROFILES)
    if unknown_profiles:
        raise SystemExit(f"{location}: unknown threshold profiles {', '.join(unknown_profiles)}")
    for field_name in ("rust_runner", "cpp_runner_contract", "archive_contract"):
        if not isinstance(command.get(field_name), str) or not command[field_name]:
            raise SystemExit(f"{location}: benchmark contract must declare {field_name}")
    datasets = command.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise SystemExit(f"{location}: benchmark contract must declare datasets")
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise SystemExit(f"{location}: each dataset entry must be an object")
        for field_name in ("name", "artifact_kind", "threshold_profile"):
            if not isinstance(dataset.get(field_name), str) or not dataset[field_name]:
                raise SystemExit(f"{location}: dataset entry must declare {field_name}")
        if dataset["threshold_profile"] not in BENCHMARK_THRESHOLD_PROFILES:
            raise SystemExit(
                f"{location}: dataset {dataset['name']} uses unknown threshold profile "
                f"{dataset['threshold_profile']}"
            )


def validate_cpp_raft_step(path: Path, case: dict, step: dict) -> None:
    location = f"{path}: case {case['name']} step {step['name']}"
    if step["command"].get("mode") == "static":
        return
    rust_runner = step["command"].get("rust_runner")
    if not isinstance(rust_runner, str) or not rust_runner:
        raise SystemExit(f"{location}: cpp_data_raft_parity step must declare rust_runner")
    if "metaserver" in step["name"] or "metaserver" in case["name"]:
        expected_validator = "temporalstore-metaserver-raft-validation"
    elif "production_gate" in step["name"] or case["name"] == "raft_production_gate":
        expected_validator = "temporalstore-raft-distributed-parity-validation"
    else:
        expected_validator = "temporalstore-raft"
    rust_validator = step["command"].get("rust_validator", "")
    if expected_validator not in rust_validator and case["name"] != "raft_production_gate":
        raise SystemExit(
            f"{location}: rust_validator must include {expected_validator!r}"
        )


def validate_combined_raft_case(path: Path, case: dict) -> None:
    gate = case.get("rust_parity_gate")
    validator = case.get("rust_parity_validator")
    if case["name"] == "raft_production_gate":
        step_runners = " ".join(
            step.get("command", {}).get("rust_runner", "") for step in case.get("steps", [])
        )
        if COMBINED_RAFT_GATE not in step_runners:
            raise SystemExit(
                f"{path}: case {case['name']} rust_runner must include {COMBINED_RAFT_GATE}"
            )
        step_validators = " ".join(
            step.get("command", {}).get("rust_validator", "") for step in case.get("steps", [])
        )
        if "temporalstore-raft-distributed-parity-validation" not in step_validators:
            raise SystemExit(
                f"{path}: case {case['name']} rust_validator must include combined Raft validation"
            )
        return
    if gate != COMBINED_RAFT_GATE:
        raise SystemExit(
            f"{path}: case {case['name']} rust_parity_gate must be {COMBINED_RAFT_GATE!r}"
        )
    if validator != COMBINED_RAFT_VALIDATOR:
        raise SystemExit(
            f"{path}: case {case['name']} rust_parity_validator must be {COMBINED_RAFT_VALIDATOR!r}"
        )


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


def run_cpp(corpus: Path, required: bool, native_required: bool, cpp_repo: Path | None) -> None:
    direct_command = os.environ.get("TS_CPP_UNIFIED_TEST_CMD")
    native_command = os.environ.get("TS_CPP_UNIFIED_NATIVE_CMD")
    if native_required and not direct_command and not native_command:
        raise SystemExit(
            "--require-cpp-native needs TS_CPP_UNIFIED_TEST_CMD or "
            "TS_CPP_UNIFIED_NATIVE_CMD so the C++ side executes the corpus, "
            "not only the discovery/surface hook"
        )
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
    parser.add_argument(
        "--require-cpp-native",
        action="store_true",
        help="fail unless a native C++ corpus executor is configured with TS_CPP_UNIFIED_TEST_CMD or TS_CPP_UNIFIED_NATIVE_CMD",
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
        run_cpp(corpus, args.require_cpp or args.require_cpp_native, args.require_cpp_native, cpp_repo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
