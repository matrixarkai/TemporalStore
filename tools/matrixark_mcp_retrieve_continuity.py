#!/usr/bin/env python3
"""Session-continuity helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        session_continuity_boost,
        session_continuity_status,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        session_continuity_boost,
        session_continuity_status,
    )


def annotate_session_continuity(candidate: Json, record: Json, *, retrieval_scope: Json, question_type: str) -> Json:
    record_scope = candidate_access_scope(record)
    status = session_continuity_status(record_scope, retrieval_scope)
    explicit_status = str(record.get("session_continuity") or candidate.get("session_continuity") or "")
    explicit_memory_scope = str(record.get("memory_scope") or candidate.get("memory_scope") or "")
    if explicit_status in {"same_session", "cross_session"} and explicit_memory_scope == "user_profile":
        status = explicit_status
    elif status in {"", "unscoped"} and explicit_status in {"same_session", "cross_session"}:
        status = explicit_status
    boost = session_continuity_boost({**candidate, "session_continuity": status}, question_type)
    reason = (
        "same-session continuity"
        if status == "same_session"
        else "cross-session memory bridge"
        if status == "cross_session"
        else "session-neutral context"
    )
    return {
        **candidate,
        "session_continuity": status,
        "continuity_boost": round(boost, 6),
        "continuity_reason": reason,
        "question_type": question_type,
    }


def make_session_continuity_annotator(*, retrieval_scope: Json, question_type: str):
    def annotator(candidate: Json, record: Json) -> Json:
        return annotate_session_continuity(
            candidate,
            record,
            retrieval_scope=retrieval_scope,
            question_type=question_type,
        )

    return annotator
