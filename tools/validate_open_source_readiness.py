#!/usr/bin/env python3
"""Validate repository-level open-source readiness files."""

from __future__ import annotations

import json
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
    ".local/",
    "build-ubuntu22/",
    "output/",
    "target-rust-bench/",
    "thirdparty/",
    "benchmark_reports/",
]

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


def main() -> int:
    for path in REQUIRED_FILES:
        read(path)
    validate_cargo_license()
    validate_license()
    require_tokens("README.md", README_TOKENS)
    require_tokens("SECURITY.md", SECURITY_TOKENS)
    require_tokens("CONTRIBUTING.md", CONTRIBUTING_TOKENS)
    require_tokens(".gitignore", IGNORE_TOKENS)
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
