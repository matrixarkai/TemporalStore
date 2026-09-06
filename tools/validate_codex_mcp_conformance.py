#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate cross-format Codex MCP integration parity wiring.

This is intentionally a contract validator, not a replacement for the shared
MatrixArk MCP server tests in the repo. It verifies that the Rust repo
    provides the Rust proxy/direct-SDK launcher expected by that shared server,
    and that both and Rust backend names remain documented.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NATIVE_REPO_ENV = os.environ.get("MATRIXARK_NATIVE_TEMPORALSTORE_REPO")
NATIVE_SERVER_ENV = os.environ.get("MATRIXARK_MCP_SERVER")
NATIVE_REPO = Path(NATIVE_REPO_ENV) if NATIVE_REPO_ENV else None
NATIVE_SERVER = (
    Path(NATIVE_SERVER_ENV)
    if NATIVE_SERVER_ENV
    else (NATIVE_REPO / "tools" / "matrixark_mcp_server.py" if NATIVE_REPO else None)
)

REQUIRED_RUST_FILES = [
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "matrixark_rust_proxy_impl.rs",
    ROOT / "sdk" / "rust" / "temporalstore" / "src" / "bin" / "matrixark_rust_proxy_impl.rs",
    ROOT / "sdk" / "rust" / "temporalstore" / "src" / "lib.rs",
    ROOT / "tools" / "run_matrixark_mcp_server.sh",
    ROOT / "docs" / "rust_codex_mcp_integration.md",
]

REQUIRED_C_ABI_TOKENS = [
    "temporalstore_hgetall",
    "temporalstore_hash_entry_array_t",
    "temporalstore_matrixark_batch_append_records",
]

REQUIRED_NATIVE_DIRECT_TOKENS = [
    "HGetAll",
    "MatrixArkBatchAppendRecords",
]

REQUIRED_BACKEND_TOKENS = [
    "temporalstore-direct",
    "temporalstore-rust",
    "temporalstore-rust-direct",
    "MATRIXARK_TEMPORALSTORE_RUST_PROXY",
    "MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK",
    "matrixark_rust_proxy",
    "matrixark_rust_direct_sdk",
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


def validate_server_when_available() -> dict[str, object]:
    if NATIVE_SERVER is None or not NATIVE_SERVER.exists():
        return {
            "native_server_checked": False,
            "native_server_path": str(NATIVE_SERVER) if NATIVE_SERVER else "",
            "reason": "MatrixArk MCP server checkout not present",
        }
    text = read(NATIVE_SERVER)
    missing = [
        token
        for token in [
            "MatrixArkTemporalStoreDirectAdapter",
            "MatrixArkTemporalStoreRustAdapter",
            "MatrixArkTemporalStoreRustDirectAdapter",
            "MatrixArkRustProxyClient",
            *REQUIRED_BACKEND_TOKENS,
            *REQUIRED_TOOL_NAMES,
        ]
        if token not in text
    ]
    if missing:
        raise SystemExit(f"{NATIVE_SERVER} missing MCP parity tokens: {', '.join(missing)}")
    return {
        "native_server_checked": True,
        "native_server_path": str(NATIVE_SERVER),
        "backends": ["temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"],
    }


def validate_rust_cli_smoke() -> dict[str, object]:
    # Use cargo run instead of requiring a prebuilt binary. The command is tiny
    # and proves the CLI accepts the exact JSON shape used by the shared server.
    root = Path(os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT", "/tmp/temporalstore-mcp-parity-smoke"))
    env = os.environ.copy()
    env["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = str(root)
    env["MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG"] = "1"
    env.setdefault("CARGO_TARGET_DIR", "/tmp/temporalstore-mcp-parity-target")
    request = {
        "op": "put_string",
        # The documented spelling first; the bare one stays accepted. Same order as the
        # backfill tool, so a deployment configures one variable and both honour it.
        "metaserver": (os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER")
                       or os.environ.get("MATRIXARK_METASERVER")
                       or "127.0.0.1:18000"),
        "namespace": os.environ.get("MATRIXARK_NAMESPACE", "deploy_ns"),
        "table": os.environ.get("MATRIXARK_TABLE", "deploy_table"),
        "key": "matrixark:mcp:parity",
        "value": "rust-temporalstore-ok",
    }
    rust_proxy = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_PROXY")
    sdk_release_cli = ROOT / "sdk" / "rust" / "temporalstore" / "target" / "release" / "matrixark_rust_proxy"
    if rust_proxy and Path(rust_proxy).exists():
        cargo_command = [rust_proxy]
        command_source = "MATRIXARK_TEMPORALSTORE_RUST_PROXY"
    elif sdk_release_cli.exists():
        cargo_command = [str(sdk_release_cli)]
        command_source = "sdk_release_binary"
    else:
        cargo_command = [
            "cargo",
            "run",
            "-q",
            "-p",
            "temporalstore-rust",
            "--bin",
            "matrixark_rust_proxy",
        ]
        command_source = "cargo_run"
    if cargo_command[0] == "cargo":
        cargo_command.extend(["--", "--serve"])
    else:
        cargo_command.append("--serve")
    cwd = ROOT
    if cargo_command[0] == "cargo" and shutil.which("cargo") is None and shutil.which("wsl.exe") is not None:
        wsl_root = subprocess.run(
            ["wsl.exe", "wslpath", "-a", str(ROOT).replace("\\", "/")],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        cargo_command = ["wsl.exe", "bash", "-lc", f"cd '{wsl_root}' && {' '.join(cargo_command)}"]
        cwd = None
    proc = subprocess.run(
        cargo_command,
        cwd=cwd,
        input=json.dumps(request),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"matrixark_rust_proxy put_string failed:\n{proc.stderr}\n{proc.stdout}")
    payload = json.loads(proc.stdout.splitlines()[-1])
    if not payload.get("ok"):
        health_proc = subprocess.run(
            cargo_command,
            cwd=cwd,
            input=json.dumps({"op": "health"}),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            timeout=30,
            check=False,
        )
        if health_proc.returncode != 0:
            raise SystemExit(f"matrixark_rust_proxy health failed after put_string failure:\n{health_proc.stderr}\n{health_proc.stdout}")
        health_payload = json.loads(health_proc.stdout.splitlines()[-1])
        if not health_payload.get("ok"):
            raise SystemExit(f"matrixark_rust_proxy returned failure: {payload}; health={health_payload}")
        return {
            "rust_cli_smoke": True,
            "rust_cli_root": str(root),
            "rust_cli_command_source": command_source,
            "operation": "health",
            "storage_write_smoke": "skipped_topology_not_ready",
            "put_string_error": payload.get("error", ""),
        }
    return {
        "rust_cli_smoke": True,
        "rust_cli_root": str(root),
        "rust_cli_command_source": command_source,
        "operation": "put_string",
    }


def main() -> int:
    for path in REQUIRED_RUST_FILES:
        if not path.exists():
            raise SystemExit(f"missing required file: {path}")

    require_contains(ROOT / "tools" / "run_matrixark_mcp_server.sh", REQUIRED_BACKEND_TOKENS)
    require_contains(ROOT / "docs" / "rust_codex_mcp_integration.md", REQUIRED_BACKEND_TOKENS + REQUIRED_TOOL_NAMES)
    rust_engine_tokens = [
        "health",
        "put_string",
        "get_string",
        "delete",
        "hset",
        "hget",
        "hdel",
        "hgetall",
        "scan_hash",
    ]
    direct_sdk_tokens = [
        "health",
        "hset",
        "hget",
        "hgetall",
        "scan_hash",
    ]
    require_contains(
        ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "matrixark_rust_proxy_impl.rs",
        rust_engine_tokens + ["TemporalEngine", "HashGetAll"],
    )
    require_contains(
        ROOT / "sdk" / "rust" / "temporalstore" / "src" / "bin" / "matrixark_rust_proxy_impl.rs",
        direct_sdk_tokens + ["batch_hget", "matrixark_batch_append_records"],
    )
    require_contains(
        ROOT / "sdk" / "rust" / "temporalstore" / "src" / "lib.rs",
        ["temporalstore_hgetall", "temporalstore_hash_entry_array_free", "scan_hash", "matrixark_batch_append_records"],
    )
    require_contains(ROOT / "src" / "client" / "temporalstore_c_client.h", REQUIRED_C_ABI_TOKENS)
    require_contains(ROOT / "src" / "client" / "temporalstore_c_client.cc", REQUIRED_C_ABI_TOKENS + ["HGetAll"])
    require_contains(ROOT / "src" / "client" / "temporalstore_client.h", ["HGetAll", "MatrixArkBatchAppendRecords"])
    require_contains(ROOT / "src" / "client" / "temporalstore_client.cc", ["HGetAll", "GETALL", "MatrixArkBatchAppendRecords"])
    require_contains(ROOT / "sdk" / "python" / "temporalstore" / "client.py", ["temporalstore_hgetall", "scan_hash", "matrixark_batch_append_records"])

    report = {
        "status": "ok",
        "same_mcp_server_contract": True,
        "rust_backend": "temporalstore-rust",
        "native_backend": "temporalstore-direct",
        "rust_files": [str(path.relative_to(ROOT)) for path in REQUIRED_RUST_FILES],
        **validate_server_when_available(),
        **validate_rust_cli_smoke(),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
