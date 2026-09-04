#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk retrieval orchestration policy."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import os
from typing import Any

RETRIEVAL_OPERATION_TOOLS = {"matrixark_retrieve"}
RETRIEVAL_NATIVE_API = "matrixark_retrieve_context_pack"
RETRIEVAL_BROAD_SCAN_POLICY = "explicit_fallback_or_debug_only"
RETRIEVAL_FALLBACK_FLAGS = {
    "allow_broad_scan_fallback",
    "allow_python_pack_fallback",
    "debug_broad_scan",
}


def is_retrieval_tool(name: str) -> bool:
    return name in RETRIEVAL_OPERATION_TOOLS


def native_retrieve_fallback_allowed(args: dict[str, Any]) -> bool:
    """Return whether Python may leave the native serving path for this request."""

    if env_bool("MATRIXARK_ALLOW_PYTHON_RETRIEVAL_FALLBACK", False):
        return True
    for flag in RETRIEVAL_FALLBACK_FLAGS:
        if bool(args.get(flag)):
            return True
    return False
