#!/usr/bin/env python3
"""Native ContextPack backend policy helpers."""

from __future__ import annotations


def native_context_pack_required_for_backend(backend_label: str, *, require_flag: str = "") -> bool:
    """Return whether Python fallback packing is blocked for this backend."""

    normalized_flag = str(require_flag or "").strip().lower()
    if normalized_flag:
        return normalized_flag in {"1", "true", "yes"}
    return str(backend_label or "local") != "local"
