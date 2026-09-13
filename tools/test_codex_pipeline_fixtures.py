#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The adapters the codex-pipeline suite shares, and the imports they and the parts need.

The parent imports each mixin class from a `test_codex_pipeline_part*` module, and every part
imported these three adapters -- plus the names below -- back from the parent. That is a cycle:
whichever module the loader reaches first fails with a partially initialised import, and which one
that is depends on the order the files are walked in. matrixarkai#1597 and matrixarkai#1603 removed
the same cycle from two other clusters; those two borrowed nothing but stdlib and production
modules, so direct imports were enough. This one shares real test doubles, so they need a home
neither side owns.

The imports are copied from the parent VERBATIM, `tools.` prefix and all -- that is, without one.
A `tools.`-prefixed import is a different module object from the bare one, each with its own state,
and a test patching one through the parent would not reach a part holding the other. That cost four
codex-hook tests in matrixarkai#1603.
"""
from __future__ import annotations

from argparse import Namespace
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path

import matrixark_codex_hook
import matrixark_mcp_core
import matrixark_mcp_local_adapter
import matrixark_mcp_query
import matrixark_mcp_retrieve_request
import matrixark_mcp_summary_runtime
from matrixark_mcp_context_pack import (
    compact_context_pack_audit_record,
    compact_context_pack_for_serving,
    compact_dropped_refs_for_context_pack,
    compact_refs_for_audit,
)
from matrixark_mcp_core import (
    candidate_index_terms,
    candidate_memory_layer_name,
    compact_context_pack_ref,
    compact_context_pack_audit_record as core_compact_context_pack_audit_record,
    compact_context_pack_for_serving as core_compact_context_pack_for_serving,
    compact_context_pack_for_serving_flat,
    embedding_for_text,
    identity_hashes,
    infer_query_type,
    infer_secondary_index_filter_groups,
    memory_layer_for_serving_ref,
    packing_sort_key,
    select_token_budgeted_refs,
)
from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness
from matrixark_mcp_recovery import matrixark_local_recovery_report
from matrixark_mcp_retrieve_pack_builder import dropped_ref_layer_budget, memory_layer_pressure_summary, selected_ref_layer_budget
from matrixark_mcp_local_adapter import (
    compression_context_index_records,
    quality_first_underfill_summary,
    refresh_final_selected_budget_policies,
    suppress_extracted_represented_pending_events,
    suppress_profile_shadowed_session_entities,
)
from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer
from matrixark_mcp_summary_runtime import build_node_summary_refresh_records


class CountingLocalAdapter(MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.flushed_batch_sizes: list[int] = []
        self.retrieval_call_count = 0

    def append_many(self, records: list[dict]) -> None:
        active_batch = self._current_write_batch()
        super().append_many(records)
        if active_batch is None and records:
            self.flushed_batch_sizes.append(len(records))

    def retrieval_records(self, **kwargs):
        self.retrieval_call_count += 1
        return super().retrieval_records(**kwargs)


class FastHookLocalAdapter(MatrixArkLocalAdapter):
    def enqueue_raw_ingestion_records(self, records: list[dict]) -> None:
        self.append_many(records)

    def _enqueue_direct_write(self, records: list[dict]) -> None:
        self.append_many(records)


class NativeCaptureLocalAdapter(MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.native_requests: list[dict] = []

    def supports_native_context_pack(self) -> bool:
        return True

    def native_context_pack(self, request: dict) -> dict | None:
        self.native_requests.append(dict(request))
        return {
            "context_pack_id": "local-native-pack",
            "selected_refs": [],
            "used_context_tokens": 0,
            "used_remote_context_tokens": 0,
            "remote_context_budget_tokens": request.get("max_context_tokens", 0),
            "recall_policy": {
                "source_role_budget": {
                    "enabled": bool(request.get("source_role_budget_tokens")),
                    "budget_tokens": request.get("source_role_budget_tokens", {}),
                },
                "memory_layer_budget_policy": {
                    "enabled": bool(request.get("memory_layer_budget_tokens")),
                    "budget_tokens": request.get("memory_layer_budget_tokens", {}),
                    "mode": request.get("memory_layer_budget_mode"),
                    "question_type": request.get("memory_layer_budget_question_type"),
                    "question_budget_reason": request.get("memory_layer_budget_question_reason"),
                    "derived": request.get("memory_layer_budget_mode") in {
                        "auto",
                        "balanced",
                        "codex_auto",
                        "pre_retrieval_summary_refresh_balanced",
                    },
                },
                "memory_selection_policy_budget_policy": {
                    "enabled": bool(request.get("memory_selection_policy_budget_tokens")),
                    "budget_tokens": request.get("memory_selection_policy_budget_tokens", {}),
                    "mode": request.get("memory_selection_policy_budget_mode"),
                }
            },
        }
