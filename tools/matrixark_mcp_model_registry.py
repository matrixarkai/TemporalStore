#!/usr/bin/env python3
"""Model registry record helpers for MatrixArk context records."""

from __future__ import annotations

import os
from typing import Any

try:
    from tools.matrixark_mcp_embeddings import embedding_execution_mode_name
    from tools.matrixark_mcp_identity import now_ms, stable_hash
    from tools.matrixark_mcp_models import compact_model_slug, embedding_model_ref_for_name
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_embeddings import embedding_execution_mode_name
    from matrixark_mcp_identity import now_ms, stable_hash
    from matrixark_mcp_models import compact_model_slug, embedding_model_ref_for_name


Json = dict[str, Any]


def context_model_registry_record(
    model_name: str,
    *,
    model_kind: str = "embedding",
    updated_at_ms: int | None = None,
) -> Json:
    model_name = str(model_name or "").strip()
    model_hash = stable_hash(f"{model_kind}_model:{model_name}")
    return {
        "record_type": "context_model_registry",
        "model_kind": model_kind,
        "model_ref": embedding_model_ref_for_name(model_name)
        if model_kind == "embedding"
        else f"{model_kind}:{compact_model_slug(model_name)}:{model_hash % 10000:04d}",
        "model_name": model_name,
        "model_hash": model_hash,
        "provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic") if model_kind == "embedding" else "",
        "execution_mode": embedding_execution_mode_name() if model_kind == "embedding" else "",
        "updated_at_ms": int(updated_at_ms or now_ms()),
    }


def context_model_registry_records(records: list[Json]) -> list[Json]:
    models: dict[str, int] = {}
    for record in records:
        if str(record.get("record_type") or "") != "context_embedding":
            continue
        model_name = str(record.get("model") or "").strip()
        if not model_name:
            continue
        updated_at_ms = record.get("updated_at_ms") or record.get("created_at_ms") or now_ms()
        try:
            timestamp = int(updated_at_ms)
        except (TypeError, ValueError):
            timestamp = now_ms()
        models[model_name] = max(models.get(model_name, 0), timestamp)
    return [
        context_model_registry_record(model_name, updated_at_ms=timestamp)
        for model_name, timestamp in sorted(models.items())
    ]
