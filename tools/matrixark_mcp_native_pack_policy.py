#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Native ContextPack backend policy helpers."""

from __future__ import annotations


try:  # package path
    from tools.matrixark_mcp_env import TRUE_VALUES
except ImportError:  # top-level path (direct tools/ execution)
    from matrixark_mcp_env import TRUE_VALUES

def native_context_pack_required_for_backend(backend_label: str, *, require_flag: str = "") -> bool:
    """Return whether Python fallback packing is blocked for this backend."""

    normalized_flag = str(require_flag or "").strip().lower()
    if normalized_flag:
        return normalized_flag in TRUE_VALUES
    return str(backend_label or "local") != "local"
