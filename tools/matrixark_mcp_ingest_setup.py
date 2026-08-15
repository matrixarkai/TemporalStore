#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Ingest setup helpers for MatrixArk local orchestration."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        MAX_PRIOR_MESSAGES,
        Json,
        collect_prior_context,
        compact_internal_extraction,
        deployment_scope_from_args,
        normalized_node_path,
        stable_hash,
        text_from_messages,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MAX_PRIOR_MESSAGES,
        Json,
        collect_prior_context,
        compact_internal_extraction,
        deployment_scope_from_args,
        normalized_node_path,
        stable_hash,
        text_from_messages,
    )


def prepare_ingest_context(target: Any, args: Json, envelope: Json, prior_records: list[Json]) -> Json:
    prior_context = (
        {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
        if args.get("skip_prior_context")
        else collect_prior_context(envelope, prior_records)
    )
    extraction_started_perf = time.perf_counter()
    extraction = compact_internal_extraction(
        envelope,
        prior_context=prior_context,
    )
    target._observe_model_latency("extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
    text = text_from_messages(envelope["messages"])
    event_id_hash = stable_hash(
        f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
    )
    if envelope["kind"] in {"resource", "skill"}:
        early_deployment_scope = deployment_scope_from_args(args, envelope)
        early_sharing_scope = target.resource_sharing_scope(args, envelope, early_deployment_scope)
        node_hint = target.default_resource_node_path(
            args,
            envelope,
            deployment_scope=early_deployment_scope,
            sharing_scope=early_sharing_scope,
        )
    else:
        early_deployment_scope = "local"
        early_sharing_scope = "private_user"
        node_hint = envelope["metadata"].get("node_path") or target.default_session_node_path(envelope["scope"])
    node_path = normalized_node_path(envelope, node_hint)
    node_hash = stable_hash("/".join(node_path))
    node_materialization = target.ensure_context_node_path(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
    )
    return {
        "prior_context": prior_context,
        "extraction": extraction,
        "text": text,
        "event_id_hash": event_id_hash,
        "early_deployment_scope": early_deployment_scope,
        "early_sharing_scope": early_sharing_scope,
        "node_path": node_path,
        "node_hash": node_hash,
        "node_materialization": node_materialization,
    }
