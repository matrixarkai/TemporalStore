#!/usr/bin/env python3
"""Local MatrixArk backend readiness and service metrics helpers."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


def ensure_backend_ready(adapter: Any, *, reason: str = "matrixark") -> Json:
    return {"status": "ready", "backend": "local", "reason": reason}


def backend_metrics(adapter: Any) -> Json:
    return {
        "backend": getattr(adapter, "_backend_label", lambda: "local")(),
        "metrics_format": "json",
        "metrics": {
            "mode": "local-jsonl",
            "event_log": str(adapter.event_log),
        },
    }


def observe_model_latency(adapter: Any, stage: str, elapsed_ms: float) -> None:
    metrics = getattr(adapter, "_matrixark_service_metrics", None)
    if metrics is not None:
        try:
            metrics.observe_model_latency(stage, elapsed_ms)
        except Exception:
            pass
