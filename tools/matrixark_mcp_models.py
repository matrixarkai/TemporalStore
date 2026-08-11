#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk MCP model reference helpers."""

from __future__ import annotations

import re

try:
    from tools.matrixark_mcp_identity import stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import stable_hash


def compact_model_slug(model_name: str) -> str:
    cleaned = str(model_name or "").replace("\\", "/").strip().strip("/")
    if not cleaned:
        return "model"
    parts = [part for part in cleaned.split("/") if part]
    tail = "/".join(parts[-2:]) if len(parts) >= 2 else parts[0]
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", tail).strip("_").lower()
    return (slug or "model")[:40]


def embedding_model_ref_for_name(model_name: str) -> str:
    slug = compact_model_slug(model_name)
    suffix = stable_hash(f"embedding_model:{model_name}") % 10000
    return f"emb:{slug}:{suffix:04d}"
