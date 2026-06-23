#!/usr/bin/env python3
"""Validate Rust/C++ Codex MCP integration parity wiring.

This is intentionally a contract validator, not a replacement for the shared
MatrixArk MCP server tests in the C++ repo. It verifies that the Rust repo
provides the Rust record-log CLI and launcher expected by that shared server,
and that both C++ and Rust backend names remain documented.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CPP_REPO = Path(os.environ.get("MATRIXARK_CPP_TEMPORALSTORE_REPO", "/root/src/github-services/TemporalStore"))
CPP_SERVER = Path(os.environ.get("MATRIXARK_MCP_SERVER", CPP_REPO / "tools" / "matrixark_mcp_server.py"))

REQUIRED_RUST_FILES = [
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "matrixark_record_log.rs",
    ROOT / "tools" / "run_matrixark_mcp_server.sh",
    ROOT / "docs" / "rust_cpp_codex_mcp_integration.md",
]

REQUIRED_BACKEND_TOKENS = [
    "temporalstore-direct",
    "temporalstore-rust",
    "MATRIXARK_TEMPORALSTORE_RUST_CLI",
    "matrixark_record_log",
]

REQUIRED_TOOL_NAMES = [
    "matrixark_ingest",
    "matrixark_session_commit",
    "matrixark_retrieve",
    "matrixark_replay",
]


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"missing required file: {path}") from exc


def require_contains(path: Path, tokens: list[str]) -> None:
    text = read(path)
    missing = [token for token in tokens if token not in text]
    if missing:
        raise SystemExit(f"{path} missing tokens: {', '.join(missing)}")


def validate_cpp_server_when_available() -> dict[str, object]:
    if not CPP_SERVER.exists():
        return {
            "cpp_server_checked": False,
            "cpp_server_path": str(CPP_SERVER),
            "reason": "C++ MatrixArk MCP server checkout not present",
        }
    text = read(CPP_SERVER)
    missing = [
        token
        for token in [
            "MatrixArkTemporalStoreDirectAdapter",
            "MatrixArkTemporalStoreRustAdapter",
            "MatrixArkRustCliClient",
            *REQUIRED_BACKEND_TOKENS,
            *REQUIRED_TOOL_NAMES,
        ]
        if token not in text
    ]
    if missing:
        raise SystemExit(f"{CPP_SERVER} missing MCP parity tokens: {', '.join(missing)}")
    return {
        "cpp_server_checked": True,
        "cpp_server_path": str(CPP_SERVER),
        "backends": ["local", "temporalstore-direct", "temporalstore-rust"],
    }


def validate_rust_cli_smoke() -> dict[str, object]:
    # Use cargo run instead of requiring a prebuilt binary. The command is tiny
    # and proves the CLI accepts the exact JSON shape used by the shared server.
    root = Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT", "/tmp/temporalstore-mcp-parity-smoke"))
    env = os.environ.copy()
    env["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = str(root)
    env.setdefault("CARGO_TARGET_DIR", "/tmp/temporalstore-mcp-parity-target")
    request = {
        "op": "put_string",
        "metaserver": "127.0.0.1:18000",
        "namespace": "codex_ns",
        "table": "codex_table",
        "key": "matrixark:mcp:parity",
        "value": "rust-temporalstore-ok",
    }
    proc = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "temporalstore-rust",
            "--bin",
            "matrixark_record_log",
        ],
        cwd=ROOT,
        input=json.dumps(request),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"matrixark_record_log put_string failed:\n{proc.stderr}\n{proc.stdout}")
    payload = json.loads(proc.stdout.splitlines()[-1])
    if not payload.get("ok"):
        raise SystemExit(f"matrixark_record_log returned failure: {payload}")
    return {
        "rust_cli_smoke": True,
        "rust_cli_root": str(root),
        "operation": "put_string",
    }


def main() -> int:
    for path in REQUIRED_RUST_FILES:
        if not path.exists():
            raise SystemExit(f"missing required file: {path}")

    require_contains(ROOT / "tools" / "run_matrixark_mcp_server.sh", REQUIRED_BACKEND_TOKENS)
    require_contains(ROOT / "docs" / "rust_cpp_codex_mcp_integration.md", REQUIRED_BACKEND_TOKENS + REQUIRED_TOOL_NAMES)
    require_contains(
        ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "matrixark_record_log.rs",
        ["put_string", "get_string", "hset", "hget", "TemporalEngine"],
    )

    report = {
        "status": "ok",
        "same_mcp_server_contract": True,
        "rust_backend": "temporalstore-rust",
        "cpp_backend": "temporalstore-direct",
        "rust_files": [str(path.relative_to(ROOT)) for path in REQUIRED_RUST_FILES],
        **validate_cpp_server_when_available(),
        **validate_rust_cli_smoke(),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
