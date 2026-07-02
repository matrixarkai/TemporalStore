#!/usr/bin/env python3
"""MatrixArk retrieval orchestration policy."""

from __future__ import annotations

RETRIEVAL_OPERATION_TOOLS = {"matrixark_retrieve"}
RETRIEVAL_NATIVE_API = "matrixark_retrieve_context_pack"
RETRIEVAL_BROAD_SCAN_POLICY = "explicit_fallback_or_debug_only"


def is_retrieval_tool(name: str) -> bool:
    return name in RETRIEVAL_OPERATION_TOOLS
