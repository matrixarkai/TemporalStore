#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Backend readiness helpers for MatrixArk MCP adapters."""

from __future__ import annotations

import socket
from typing import Any

try:
    from tools.matrixark_mcp_runtime_config import BACKEND_READINESS_CONNECT_TIMEOUT_MS
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_runtime_config import BACKEND_READINESS_CONNECT_TIMEOUT_MS


Json = dict[str, Any]


def parse_host_port(address: str) -> tuple[str, int] | None:
    if not address or ":" not in address:
        return None
    host, port_text = address.rsplit(":", 1)
    try:
        return host or "127.0.0.1", int(port_text)
    except ValueError:
        return None


def metaserver_reachable(address: str, timeout_ms: int = BACKEND_READINESS_CONNECT_TIMEOUT_MS) -> Json:
    parsed = parse_host_port(address)
    if parsed is None:
        return {"ok": False, "address": address, "error": "invalid metaserver address"}
    host, port = parsed
    try:
        with socket.create_connection((host, port), timeout=max(0.05, timeout_ms / 1000.0)):
            return {"ok": True, "address": address}
    except OSError as exc:
        return {"ok": False, "address": address, "error": str(exc)}


def adapter_ensure_backend_ready(
    adapter: Any,
    *,
    reason: str = "manual",
    probe: bool = True,
    timeout_ms: int | None = None,
) -> Json:
    """Call adapter readiness across old/new adapter signatures."""
    try:
        return adapter.ensure_backend_ready(reason=reason, probe=probe, timeout_ms=timeout_ms)
    except TypeError as exc:
        text = str(exc)
        if "unexpected keyword argument" not in text or "probe" not in text:
            raise
        return adapter.ensure_backend_ready(reason=reason)
