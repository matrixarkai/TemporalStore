#!/usr/bin/env python3
"""Validate repository-level open-source readiness files."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "README.md",
    "LICENSE",
    "NOTICE",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    ".gitignore",
    "docs/open_source_readiness.md",
    "tools/run_matrixark_context_backfill_ci_gate_ubuntu22.sh",
    "tools/verify_matrixark_context_backfill_ci_evidence.py",
]

README_TOKENS = [
    "Apache-2.0",
    "rust-main",
    "no brpc/thrift",
    "Rust-native",
    "production-readiness claims",
]

SECURITY_TOKENS = [
    "Do not open a public issue with exploit details",
    "Do not commit API keys",
    "TS_RAFT_AUTH_TOKEN",
]

CONTRIBUTING_TOKENS = [
    "shared-corpus",
    "rust-internal",
    "Do not add brpc or thrift",
]

IGNORE_TOKENS = [
    "target/",
    ".codex/",
    ".local/",
    "build-ubuntu22/",
    "output/",
    "target-rust-bench/",
    "thirdparty/",
    "benchmark_reports/",
]

BACKFILL_CI_GATE_TOKENS = [
    "MatrixArk context backfill CI gate for Ubuntu 22",
    "MATRIXARK_BACKFILL_CI_RECORDS",
    "MATRIXARK_BACKFILL_CI_BATCH_SIZES",
    "MATRIXARK_BACKFILL_CI_INCREMENTAL_RECORDS",
    "MATRIXARK_BACKFILL_CI_EVIDENCE_MANIFEST",
    "python3 -m py_compile",
    "python3 tools/test_matrixark_context_backfill.py",
    "python3 tools/test_matrixark_context_backfill_benchmark.py",
    "python3 tools/test_matrixark_dual_write_ingestion_benchmark.py",
    "python3 tools/test_verify_matrixark_context_backfill_ci_evidence.py",
    "python3 tools/test_validate_matrixark_context_backfill_readiness.py",
    "python3 tools/test_validate_open_source_readiness.py",
    "python3 tools/validate_open_source_readiness.py",
    "python3 tools/validate_matrixark_context_backfill_readiness.py",
    "matrixark_context_backfill_readiness.json",
    "matrixark_context_backfill_ci_evidence_manifest_v1",
    "matrixark_context_backfill_evidence_manifest",
    "verify_matrixark_context_backfill_ci_evidence.py",
    "matrixark_context_backfill_ci_evidence_verification_status",
    "os.path.relpath",
    "--require-relative-paths=1",
    "readiness_status_ok",
    "readiness_checks_all_passed",
    "readiness_required_sections_ok",
    "artifact_paths_within_dir",
    "dual_write_readiness_status_ok",
    "dual_write_manifest_schema_supported",
    "dual_write_manifest_status_ok",
    "dual_write_manifest_required_artifacts_present",
    "dual_write_manifest_artifact_paths_relative",
    "dual_write_manifest_artifact_paths_within_dir",
    "dual_write_manifest_artifact_paths_readable",
    "dual_write_manifest_artifact_sizes_match",
    "dual_write_manifest_artifact_sha256_match",
]

PRIVATE_PATH_TOKENS = [
    "C:\\Users\\",
    "/mnt/c/Users",
    "/mnt/c/Users/",
    "/root/src/",
    "Ubuntu2204Deeproute",
    "Deeproute",
    "Vincent Jiang",
]

FALLBACK_SCAN_SKIP_DIRS = {
    ".git",
    "target",
    "target-rust-bench",
    "build-ubuntu22",
    "output",
    "benchmark_reports",
    "thirdparty",
    "__pycache__",
}

PRIVATE_PATH_FIXTURE_FILES = {
    "tools/validate_open_source_readiness.py",
    "tools/validate_temporalstore_performance_execution_redaction.py",
    "tools/test_temporalstore_performance_execution_redaction.py",
}

def read(relative_path: str) -> str:
    path = ROOT / relative_path
    if not path.exists():
        raise SystemExit(f"missing required file: {relative_path}")
    return path.read_text(encoding="utf-8", errors="replace")


def require_tokens(relative_path: str, tokens: list[str]) -> None:
    text = read(relative_path)
    normalized = " ".join(text.split())
    missing = [token for token in tokens if token not in text and token not in normalized]
    if missing:
        raise SystemExit(f"{relative_path} missing tokens: {', '.join(missing)}")


def validate_cargo_license() -> None:
    cargo = read("Cargo.toml")
    if 'license = "Apache-2.0"' not in cargo:
        raise SystemExit("Cargo.toml must declare workspace Apache-2.0 license")
    for crate in ["crates/temporalstore-rust/Cargo.toml", "crates/temporalstore-snapshot/Cargo.toml"]:
        text = read(crate)
        if "license.workspace = true" not in text:
            raise SystemExit(f"{crate} must inherit workspace license")


def validate_license() -> None:
    license_text = read("LICENSE")
    if "Apache License" not in license_text or "Version 2.0" not in license_text:
        raise SystemExit("LICENSE must contain Apache License 2.0 text")
    notice = read("NOTICE")
    if "MatrixArkAI contributors" not in notice:
        raise SystemExit("NOTICE must include contributor attribution")


def validate_no_tracked_local_codex_config() -> None:
    tracked = repository_files()
    local_codex_files = [path for path in tracked if path == ".codex" or path.startswith(".codex/")]
    if local_codex_files:
        raise SystemExit(
            "tracked local Codex config is not open-source safe: "
            + ", ".join(local_codex_files)
        )


def validate_no_private_paths() -> None:
    tracked = repository_files()
    offenders: list[str] = []
    for relative_path in tracked:
        if relative_path in PRIVATE_PATH_FIXTURE_FILES:
            continue
        path = ROOT / relative_path
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for token in PRIVATE_PATH_TOKENS:
            if token in text:
                offenders.append(f"{relative_path}: {token}")
                break
    if offenders:
        raise SystemExit(
            "tracked files contain local/private path markers:\n" + "\n".join(offenders[:50])
        )


def repository_files() -> list[str]:
    try:
        return subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.splitlines()
    except (FileNotFoundError, subprocess.CalledProcessError):
        files: list[str] = []
        for directory, dirnames, filenames in os.walk(ROOT):
            dirnames[:] = [
                dirname for dirname in dirnames if dirname not in FALLBACK_SCAN_SKIP_DIRS
            ]
            for filename in filenames:
                path = Path(directory) / filename
                files.append(path.relative_to(ROOT).as_posix())
        return sorted(files)


def main() -> int:
    for path in REQUIRED_FILES:
        read(path)
    validate_cargo_license()
    validate_license()
    validate_no_tracked_local_codex_config()
    validate_no_private_paths()
    require_tokens("README.md", README_TOKENS)
    require_tokens("SECURITY.md", SECURITY_TOKENS)
    require_tokens("CONTRIBUTING.md", CONTRIBUTING_TOKENS)
    require_tokens(".gitignore", IGNORE_TOKENS)
    require_tokens(
        "tools/run_matrixark_context_backfill_ci_gate_ubuntu22.sh",
        BACKFILL_CI_GATE_TOKENS,
    )
    require_tokens(
        "docs/open_source_readiness.md",
        [
            "brpc/thrift compatibility remains explicitly out of scope",
            "generated build outputs",
            "`workflow` scope",
        ],
    )
    report = {
        "status": "ok",
        "required_files": REQUIRED_FILES,
        "license": "Apache-2.0",
        "open_source_ready": True,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
