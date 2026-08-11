#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Debug logging helper for MatrixArk MCP tools."""

from __future__ import annotations

import os
import time


def mcp_debug_log(message: str) -> None:
    path = os.environ.get("MATRIXARK_MCP_DEBUG_LOG")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{time.time():.3f} {message}\n")
    except Exception:
        pass


_mcp_debug_log = mcp_debug_log
