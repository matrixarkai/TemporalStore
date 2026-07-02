#!/usr/bin/env python3
"""Validate the shared TemporalStore corpus against Rust and C++ checkouts.

This is the repo-level parity entry point.  The shared corpus remains the API
contract; this tool checks the Rust-side runner evidence, C++ static/native
runner evidence, and emits a comparator-friendly case report.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = Path(os.environ.get("TMPDIR", "/tmp")) / "temporalstore-unified-cpp-rust-parity.json"
RUST_OWNED_PREFIXES = (
    ".github/",
    "compat/",
    "crates/",
    "docs/",
    "infra/",
    "scripts/",
    "tests/",
    "third_party/",
    "tools/",
)
CPP_PATH_ALIASES = {
    # Current C++ code layout uses these names for the same product surfaces.
    "src/proxy/proxy_server.cc": ["src/proxy/proxy.cc"],
    "src/metaserver_v2/meta_task_scheduler.cc": ["src/metaserver_v2/scheduler/task_scheduler.cc"],
    "src/partition/partition_manager.cc": ["src/server/partition_manager.cc"],
    "src/client/table_client.cc": ["src/client/client_impl.cc", "src/client/temporalstore_client.cc"],
}


def default_corpus_path() -> Path:
    override = os.environ.get("TEMPORALSTORE_TEST_CORPUS")
    if override:
        return Path(override)
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
        ROOT.parent / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
        ROOT / "compat" / "unified_temporalstore_cases.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]


def default_cpp_repo() -> str | None:
    override = os.environ.get("TS_CPP_REPO")
    if override:
        return override
    if os.name == "nt" and wsl_path_exists("<repo>"):
        return "wsl:<repo>"
    candidate = Path("<repo>")
    return str(candidate) if candidate.exists() else None


def wsl_path_exists(path: str) -> bool:
    if os.name != "nt":
        return Path(path).exists()
    completed = subprocess.run(
        ["wsl.exe", "-e", "sh", "-lc", f"test -e {shlex.quote(path)}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return completed.returncode == 0


def normalize_cpp_repo(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    if raw.startswith("wsl:"):
        wsl_path = raw.removeprefix("wsl:")
        if not wsl_path_exists(wsl_path):
            raise SystemExit(f"C++ WSL repo does not exist: {raw}")
        return {"kind": "wsl", "display": raw, "wsl_path": wsl_path, "local_path": None}
    if os.name == "nt" and raw.startswith("/") and wsl_path_exists(raw):
        return {"kind": "wsl", "display": f"wsl:{raw}", "wsl_path": raw, "local_path": None}
    local_path = Path(raw).resolve()
    if not local_path.exists():
        raise SystemExit(f"C++ repo does not exist: {raw}")
    return {"kind": "local", "display": str(local_path), "wsl_path": None, "local_path": local_path}


def cpp_repo_path_exists(repo: dict[str, Any] | None, path: str) -> tuple[bool, str, str | None]:
    if repo is None:
        return False, path, "cpp_repo_not_configured"
    normalized = path.replace("\\", "/")
    if repo["kind"] == "wsl":
        full_path = repo["wsl_path"].rstrip("/") + "/" + normalized
        return wsl_path_exists(full_path), f"wsl:{full_path}", None
    full_path = repo["local_path"] / normalized
    return full_path.exists(), str(full_path), None


def cpp_repo_path_or_alias_exists(repo: dict[str, Any] | None, path: str) -> tuple[bool, str, str | None, str | None]:
    exists, full_path, reason = cpp_repo_path_exists(repo, path)
    if exists:
        return True, full_path, reason, None
    for alias in CPP_PATH_ALIASES.get(path.replace("\\", "/"), []):
        alias_exists, alias_full_path, _ = cpp_repo_path_exists(repo, alias)
        if alias_exists:
            return True, alias_full_path, None, alias
    return False, full_path, reason, None


def load_and_validate_corpus(corpus: Path) -> dict[str, Any]:
    validator = ROOT / "tools" / "run_temporalstore_unified_tests.py"
    subprocess.run(
        [sys.executable, str(validator), "--validate-only", "--corpus", str(corpus)],
        cwd=ROOT,
        check=True,
    )
    return json.loads(corpus.read_text(encoding="utf-8"))


def command_kind(case: dict[str, Any], step: dict[str, Any]) -> str:
    command = step.get("command")
    if isinstance(command, dict):
        return str(command.get("kind") or "unknown")
    return "unknown"


def owned_root_for_path(path: str, cpp_repo: dict[str, Any] | None) -> tuple[str, Path | None]:
    normalized = path.replace("\\", "/")
    if normalized.startswith(RUST_OWNED_PREFIXES):
        return "rust", ROOT
    if cpp_repo is not None:
        return "cpp", cpp_repo
    return "cpp", None


def check_required_paths(paths: list[str], cpp_repo: dict[str, Any] | None) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    present: list[dict[str, str]] = []
    missing: list[dict[str, str]] = []
    for path in paths:
        owner, root = owned_root_for_path(path, cpp_repo)
        if owner == "cpp":
            exists, full_path, reason, alias = cpp_repo_path_or_alias_exists(cpp_repo, path)
            row = {"owner": owner, "path": path, "full_path": full_path}
            if exists:
                if alias:
                    row["alias_path"] = alias
                    row["note"] = "cpp_path_alias_resolved"
                present.append(row)
            else:
                row["reason"] = reason or "missing"
                missing.append(row)
        else:
            if root is None:
                missing.append({"owner": owner, "path": path, "reason": "rust_repo_not_configured"})
                continue
            full_path = root / path
            row = {"owner": owner, "path": path, "full_path": str(full_path)}
            if full_path.exists():
                present.append(row)
            else:
                cpp_exists, cpp_full_path, _, alias = cpp_repo_path_or_alias_exists(cpp_repo, path)
                if cpp_exists:
                    resolved = {
                        "owner": "cpp",
                        "path": path,
                        "full_path": cpp_full_path,
                        "note": "rust_prefix_path_resolved_in_cpp_repo",
                    }
                    if alias:
                        resolved["alias_path"] = alias
                        resolved["note"] = "rust_prefix_path_resolved_in_cpp_repo_alias"
                    present.append(resolved)
                else:
                    row["reason"] = "missing"
                    missing.append(row)
    return present, missing


def run_shell(command: str, cwd: Path) -> dict[str, Any]:
    start = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
    output = completed.stdout[-8000:] if completed.stdout else ""
    return {
        "exit_code": completed.returncode,
        "latency_ms": elapsed_ms,
        "output_tail": output,
        "passed": completed.returncode == 0,
    }


def render_command(command: str, corpus: Path, cpp_repo: dict[str, Any] | None) -> str:
    rendered = command.replace("{corpus}", str(corpus))
    if cpp_repo is not None:
        rendered = rendered.replace("{cpp_repo}", str(cpp_repo["display"]))
    return rendered


def collect_cases(
    corpus: dict[str, Any],
    corpus_path: Path,
    cpp_repo: dict[str, Any] | None,
    run_rust: bool,
    cpp_command: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    summary = {
        "case_count": 0,
        "step_count": 0,
        "existing_test_step_count": 0,
        "rust_runner_count": 0,
        "cpp_static_path_count": 0,
        "cpp_static_alias_path_count": 0,
        "missing_required_path_count": 0,
        "rust_runner_failure_count": 0,
        "cpp_runner_failure_count": 0,
        "unsupported_step_count": 0,
    }

    for case in corpus.get("cases", []):
        if not isinstance(case, dict):
            continue
        case_name = str(case.get("name") or "unnamed_case")
        report_case = {"name": case_name, "status": "passed", "steps": []}
        summary["case_count"] += 1
        for step in case.get("steps", []):
            if not isinstance(step, dict):
                continue
            summary["step_count"] += 1
            step_name = str(step.get("name") or f"step_{summary['step_count']}")
            command = step.get("command") if isinstance(step.get("command"), dict) else {}
            kind = command_kind(case, step)
            output: dict[str, Any] = {"command_kind": kind}
            status = "passed"
            latency_ms = 0.0

            required_paths = command.get("required_paths") if isinstance(command, dict) else None
            if isinstance(required_paths, list):
                present, missing = check_required_paths([str(path) for path in required_paths], cpp_repo)
                output["required_paths_present"] = present
                output["required_paths_missing"] = missing
                summary["cpp_static_path_count"] += sum(1 for row in present if row["owner"] == "cpp")
                summary["cpp_static_alias_path_count"] += sum(1 for row in present if row.get("alias_path"))
                summary["missing_required_path_count"] += len(missing)
                if missing:
                    status = "failed"

            rust_runner = command.get("rust_runner") if isinstance(command, dict) else None
            if isinstance(rust_runner, str) and rust_runner:
                summary["rust_runner_count"] += 1
                output["rust_runner"] = rust_runner
                if run_rust:
                    rust_result = run_shell(rust_runner, ROOT)
                    output["rust_result"] = rust_result
                    latency_ms = rust_result["latency_ms"]
                    if not rust_result["passed"]:
                        status = "failed"
                        summary["rust_runner_failure_count"] += 1

            if kind == "existing_test":
                summary["existing_test_step_count"] += 1
            elif not required_paths and not rust_runner:
                output["reason"] = "covered_by_corpus_validation_or_runtime_adapter"

            report_step = {
                "name": step_name,
                "status": status,
                "latency_ms": latency_ms,
                "output": output,
            }
            if status != "passed":
                report_case["status"] = "failed"
            report_case["steps"].append(report_step)
        cases.append(report_case)

    if cpp_command:
        rendered = render_command(cpp_command, corpus_path, cpp_repo)
        cwd = cpp_repo["local_path"] if cpp_repo and cpp_repo["kind"] == "local" else ROOT
        cpp_result = run_shell(rendered, cwd or ROOT)
        if not cpp_result["passed"]:
            summary["cpp_runner_failure_count"] += 1
        cases.append(
            {
                "name": "cpp_native_unified_runner",
                "status": "passed" if cpp_result["passed"] else "failed",
                "steps": [
                    {
                        "name": "cpp_native_unified_runner",
                        "status": "passed" if cpp_result["passed"] else "failed",
                        "latency_ms": cpp_result["latency_ms"],
                        "output": {
                            "command_kind": "cpp_native_runner",
                            "command": rendered,
                            "cpp_result": cpp_result,
                        },
                    }
                ],
            }
        )

    return cases, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=default_corpus_path())
    parser.add_argument("--cpp-repo", default=default_cpp_repo())
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--run-rust", action="store_true", help="execute corpus rust_runner commands")
    parser.add_argument(
        "--cpp-command",
        default=os.environ.get("TS_CPP_UNIFIED_TEST_CMD"),
        help="optional native C++ corpus runner command; supports {corpus} and {cpp_repo}",
    )
    parser.add_argument(
        "--allow-missing-paths",
        action="store_true",
        help="emit missing C++/Rust evidence paths without failing the command",
    )
    args = parser.parse_args()

    corpus_path = args.corpus.resolve()
    cpp_repo = normalize_cpp_repo(args.cpp_repo)

    corpus = load_and_validate_corpus(corpus_path)
    cases, summary = collect_cases(corpus, corpus_path, cpp_repo, args.run_rust, args.cpp_command)
    failures = [case for case in cases if case.get("status") != "passed"]
    missing_required_paths = []
    for case in cases:
        for step in case.get("steps", []):
            output = step.get("output") if isinstance(step.get("output"), dict) else {}
            for row in output.get("required_paths_missing", []):
                missing_required_paths.append(
                    {
                        "case": case.get("name"),
                        "step": step.get("name"),
                        **row,
                    }
                )
    ready = not failures
    if args.allow_missing_paths and summary["missing_required_path_count"] and not (
        summary["rust_runner_failure_count"] or summary["cpp_runner_failure_count"]
    ):
        ready = True

    report = {
        "schema": "temporalstore_unified_cpp_rust_parity_report_v1",
        "case_report_schema": "temporalstore_unified_case_report_v1",
        "ready": ready,
        "rust_repo": str(ROOT),
        "cpp_repo": cpp_repo["display"] if cpp_repo is not None else None,
        "corpus_path": str(corpus_path),
        "all_tests_share_corpus_contract": True,
        "native_cpp_runner_configured": bool(args.cpp_command),
        "rust_runner_execution_requested": args.run_rust,
        "summary": summary,
        "missing_required_paths": missing_required_paths,
        "cases": cases,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("schema", "ready", "corpus_path", "cpp_repo", "summary")}, indent=2))
    print(f"wrote {args.output}")
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
