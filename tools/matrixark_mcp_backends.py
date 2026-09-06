#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Backend adapter selection for the MatrixArk MCP entrypoint."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from tools.matrixark_mcp_core import (
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        MATRIXARK_MCP_PROFILE,
        MATRIXARK_REQUIRE_BACKEND_READY,
        Json,
        MatrixArkError,
        adapter_ensure_backend_ready,
    )
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_temporal_adapters import (
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        MATRIXARK_MCP_PROFILE,
        MATRIXARK_REQUIRE_BACKEND_READY,
        Json,
        MatrixArkError,
        adapter_ensure_backend_ready,
    )
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_temporal_adapters import (
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )


__all__ = [
    "add_backend_arguments",
    "backend_ready_required",
    "build_mcp_adapter",
    "default_mcp_backend",
    "ensure_startup_backend_ready",
    "production_profile_enabled",
    "validate_mcp_backend_policy",
]


def production_profile_enabled() -> bool:
    return MATRIXARK_MCP_PROFILE in {"prod", "production", "benchmark", "bench", "parity"}


def backend_ready_required(backend: str) -> bool:
    if MATRIXARK_REQUIRE_BACKEND_READY:
        return MATRIXARK_REQUIRE_BACKEND_READY in {"1", "true", "yes"}
    return production_profile_enabled() and backend in {"temporalstore-direct", "temporalstore-rust"}


def default_mcp_backend() -> str:
    configured = os.environ.get("MATRIXARK_MCP_BACKEND")
    if configured:
        return configured
    if production_profile_enabled():
        return "temporalstore-direct"
    # A deployed gateway must never silently fall back to the O(store) JSONL local
    # adapter for durable record storage. Whenever a Rust TemporalStore proxy/CLI
    # binary is configured, prefer the durable `temporalstore-rust` backend so that
    # records land in TemporalStore over the persistent pooled proxy client and the
    # Python gateway only keeps an in-memory async session buffer (for batch
    # extraction). The JSONL `local` backend stays the default only for pure dev
    # (no rust binary configured) and remains explicitly selectable via
    # MATRIXARK_MCP_BACKEND=local.
    rust_cli = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", "").strip()
    if rust_cli:
        return "temporalstore-rust"
    return "local"


def validate_mcp_backend_policy(args: argparse.Namespace) -> None:
    local_backends = {"local", "temporalstore-local"}
    if production_profile_enabled() and args.backend in local_backends and not MATRIXARK_ALLOW_LOCAL_BACKEND:
        raise MatrixArkError(
            "MatrixArk MCP production/benchmark profile requires --backend temporalstore-direct "
            "or --backend temporalstore-rust. Set MATRIXARK_ALLOW_LOCAL_BACKEND=1 only for debug."
        )


def add_backend_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backend",
        choices=["local", "temporalstore-local", "temporalstore-direct", "temporalstore-rust"],
        default=default_mcp_backend(),
        help="Storage backend. local uses JSONL; temporalstore-local uses a no-metaserver local TemporalStore-shaped record log; temporalstore-direct uses the native TemporalStore SDK.",
    )
    parser.add_argument(
        "--event-log",
        type=Path,
        default=Path("/tmp/matrixark-mcp-events.jsonl"),
        help="JSONL event log used by the local adapter.",
    )
    parser.add_argument(
        "--local-store",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", "/tmp/matrixark-mcp-temporalstore-local.jsonl")),
        help="Persistent local record log for --backend temporalstore-local. This mode does not require metaserver.",
    )
    parser.add_argument(
        "--metaserver",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
        help="TemporalStore metaserver address for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"),
        help="TemporalStore namespace for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"),
        help="TemporalStore table for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--temporalstore-lib",
        default=os.environ.get("TEMPORALSTORE_LIB", ""),
        help="Path to libtemporalstore.so for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--storage-prefix",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:mcp"),
        help="TemporalStore key prefix for MatrixArk records.",
    )
    parser.add_argument(
        "--rust-cli",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", ""),
        help="Path to the Rust matrixark_rust_proxy or direct SDK binary for --backend temporalstore-rust.",
    )
    # 60000 for both, which is the number every launcher supplies and the one the deployment
    # runs on. These were 20000 here and 60000 in the two agent hooks: one option, one meaning,
    # three parsers, two numbers -- so a deployment that set nothing got a timeout three times
    # shorter or longer depending on which path asked, and only that deployment, which is the
    # one nobody tests. `matrixark_mcp_rust_server.sh` starts THIS server, exports 60000 and
    # passes --request-timeout-ms explicitly; the three installers export the same. So this
    # default is reached only by a server started outside all of them, and it was the odd one.
    parser.add_argument(
        "--request-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "60000")),
        help="Per-request timeout for the native TemporalStore SDK.",
    )
    parser.add_argument(
        "--io-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "60000")),
        help="BRPC I/O timeout for the native TemporalStore SDK.",
    )


def build_mcp_adapter(args: argparse.Namespace) -> MatrixArkLocalAdapter:
    validate_mcp_backend_policy(args)
    if args.backend == "temporalstore-direct":
        return MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    if args.backend == "temporalstore-rust":
        return MatrixArkTemporalStoreRustAdapter(
            rust_cli=args.rust_cli,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    if args.backend == "temporalstore-local":
        return MatrixArkLocalAdapter(args.local_store)
    return MatrixArkLocalAdapter(args.event_log)


def ensure_startup_backend_ready(adapter: MatrixArkLocalAdapter, backend: str) -> None:
    if not backend_ready_required(backend):
        return
    readiness = adapter_ensure_backend_ready(adapter, reason="mcp_startup", probe=True)
    if readiness.get("status") != "ready":
        raise MatrixArkError(f"MatrixArk MCP backend not ready at startup: {json.dumps(readiness, sort_keys=True)}")
