#!/usr/bin/env python3
"""MatrixArk ingest/resource/skill orchestration policy."""

from __future__ import annotations

INGEST_OPERATION_TOOLS = {
    "matrixark_ingest",
    "matrixark_batch_extract",
    "matrixark_session_commit",
    "matrixark_refresh_summaries",
}


def is_ingestion_tool(name: str) -> bool:
    return name in INGEST_OPERATION_TOOLS
