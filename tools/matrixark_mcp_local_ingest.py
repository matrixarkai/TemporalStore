#!/usr/bin/env python3
"""Local MatrixArk ingest orchestration helpers."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        context_node_key,
        embedding_for_text,
        now_ms,
        stable_hash,
        summarize_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        context_node_key,
        embedding_for_text,
        now_ms,
        stable_hash,
        summarize_text,
    )


try:
    from tools.matrixark_mcp_async_ingest import lightweight_async_accept
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_async_ingest import lightweight_async_accept

try:
    from tools.matrixark_mcp_ingest_setup import prepare_ingest_context
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_ingest_setup import prepare_ingest_context

try:
    from tools import matrixark_mcp_ingest_message_records as message_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_message_records as message_record_builders

try:
    from tools import matrixark_mcp_ingest_resource_runtime as resource_runtime_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_runtime as resource_runtime_helpers

try:
    from tools.matrixark_mcp_ingest_response import (
        build_ingest_response,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_ingest_response import (
        build_ingest_response,
    )


def ingest_after_start(self: Any, args: Json, ingest_start: Json) -> Json:
    envelope = ingest_start["envelope"]
    hook = ingest_start["hook"]
    backend_readiness = ingest_start["backend_readiness"]
    idle_commit_result = ingest_start["idle_commit_result"]
    lightweight_result = ingest_start["lightweight_result"]
    if lightweight_result is not None:
        return lightweight_result
    prior_records = [] if args.get("skip_prior_context") else self.read_all()
    ingest_context = prepare_ingest_context(self, args, envelope, prior_records)
    prior_context = ingest_context["prior_context"]
    extraction = ingest_context["extraction"]
    text = ingest_context["text"]
    event_id_hash = ingest_context["event_id_hash"]
    early_deployment_scope = ingest_context["early_deployment_scope"]
    early_sharing_scope = ingest_context["early_sharing_scope"]
    node_path = ingest_context["node_path"]
    node_hash = ingest_context["node_hash"]
    node_materialization = ingest_context["node_materialization"]
    resource_result = resource_runtime_helpers.ingest_resource_or_skill_if_needed(
        self,
        args=args,
        envelope=envelope,
        hook=hook,
        event_id_hash=event_id_hash,
        node_hash=node_hash,
        node_path=node_path,
        node_materialization=node_materialization,
        early_deployment_scope=early_deployment_scope,
        early_sharing_scope=early_sharing_scope,
    )
    queued_response = resource_result.get("queued_response")
    if queued_response is not None:
        return queued_response
    hot_record_scope = resource_result["hot_record_scope"]
    resource_dirty_hashes = resource_result["resource_dirty_hashes"]
    resource_import_task_hash = resource_result["resource_import_task_hash"]
    resource_import_task_status = resource_result["resource_import_task_status"]
    resource_import_wait = resource_result["resource_import_wait"]
    resource_import_metrics = resource_result["resource_import_metrics"]
    raw_uri = resource_result["raw_uri"]
    requested_raw_uri = resource_result["requested_raw_uri"]
    storage_resolution = resource_result["storage_resolution"]
    raw_storage_policy = resource_result["raw_storage_policy"]
    resource_chunk_hashes = resource_result["resource_chunk_hashes"]
    original_chunk_count = resource_result["original_chunk_count"]
    deduped_chunk_count = resource_result["deduped_chunk_count"]
    deduped_source_refs = resource_result["deduped_source_refs"]
    resource_version_value = resource_result["resource_version_value"]
    resource_content_hash = resource_result["resource_content_hash"]
    parse_warnings = resource_result["parse_warnings"]
    superseded_chunk_count = resource_result["superseded_chunk_count"]
    superseded_chunk_hashes = resource_result["superseded_chunk_hashes"]
    resource_fact_event_hashes = resource_result["resource_fact_event_hashes"]
    resource_fact_entity_hashes = resource_result["resource_fact_entity_hashes"]
    index_candidate_count = resource_result["index_candidate_count"]
    index_write_count = resource_result["index_write_count"]
    index_dropped_by_cap_count = resource_result["index_dropped_by_cap_count"]
    skill_hash = resource_result["skill_hash"]
    summary_text = summarize_text(text)
    embedding_started_perf = time.perf_counter()
    event_embedding = embedding_for_text(text)
    self._observe_model_latency("embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
    with self.write_batch("message_ingest_hot_path"):
        session_key_parts = [str(part) for part in context_node_key(envelope)]
        if any(session_key_parts):
            session_summary_source = " ".join(
                [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                + [text]
            )
            session_summary_text = summarize_text(session_summary_source, limit=512)
            session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
            self.append(
                message_record_builders.session_l0_summary_record(
                    summary_hash=session_summary_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    context_node_key=session_key_parts,
                    summary_text=session_summary_text,
                    source_event_hash=event_id_hash,
                    scope=hot_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
            session_summary_embedding = embedding_for_text(session_summary_text)
            self.append(
                message_record_builders.context_embedding_record(
                    embedding_type="session_l0",
                    ref_type="summary",
                    ref_hash=session_summary_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    vector=session_summary_embedding,
                    scope=hot_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        self.append(
            message_record_builders.context_embedding_record(
                embedding_type="event_text",
                ref_type="event",
                ref_hash=event_id_hash,
                node_hash=node_hash,
                node_path=node_path,
                vector=event_embedding,
                scope=hot_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
        )
        record = message_record_builders.context_event_record(
            event_id_hash=event_id_hash,
            node_hash=node_hash,
            node_path=node_path,
            text=text,
            extraction=extraction,
            envelope=envelope,
            prior_context=prior_context,
            hook=hook,
        )
        self.append(record)
        event_index_terms = message_record_builders.context_event_index_terms(
            extraction=extraction,
            text=text,
            envelope=envelope,
        )
        event_index_records = message_record_builders.context_event_index_records(
            index_terms=event_index_terms,
            event_id_hash=event_id_hash,
            node_hash=node_hash,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        if event_index_records:
            self.append_many(event_index_records)
        if self.session_buffer_enabled(args, kind=envelope["kind"]):
            self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=text,
            scope=hot_record_scope,
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_event_hash",
            source_hash=event_id_hash,
        )
    session_buffer_enabled = self.session_buffer_enabled(args, kind=envelope["kind"])
    pending_event_count = len(self.pending_session_events(envelope["scope"])) if session_buffer_enabled else 0
    auto_batch_result: Json | None = None
    auto_batch_extract = self.auto_batch_extract_enabled(args, kind=envelope["kind"])
    session_boundary_commit = self.session_boundary_commit_requested(args, hook=hook)
    session_buffer_threshold = args.get("session_buffer_threshold", 20)
    if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
        raise MatrixArkError("session_buffer_threshold must be a positive integer")
    if auto_batch_extract and (session_boundary_commit or pending_event_count >= session_buffer_threshold):
        auto_batch_result = self.session_commit(
            {
                "scope": hot_record_scope,
                "metadata": envelope["metadata"],
                "threshold_messages": session_buffer_threshold,
                "force": session_boundary_commit,
                "max_messages": None if session_boundary_commit else session_buffer_threshold,
                "commit_reason": "hook_boundary" if session_boundary_commit else "threshold",
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
                "storage_options": envelope.get("storage_options", {}),
            },
            hook=hook,
        )
    return build_ingest_response(
        envelope=envelope,
        hook=hook,
        event_id_hash=event_id_hash,
        node_hash=record["node_hash"],
        extraction=extraction,
        summary_refresh=summary_refresh,
        resource_dirty_hashes=resource_dirty_hashes,
        resource_import_task_hash=resource_import_task_hash,
        resource_import_task_status=resource_import_task_status,
        resource_import_wait=resource_import_wait,
        resource_import_metrics=resource_import_metrics,
        raw_uri=raw_uri,
        requested_raw_uri=requested_raw_uri,
        storage_resolution=storage_resolution,
        raw_storage_policy=raw_storage_policy,
        node_materialization=node_materialization,
        resource_chunk_hashes=resource_chunk_hashes,
        original_chunk_count=original_chunk_count,
        deduped_chunk_count=deduped_chunk_count,
        deduped_source_refs=deduped_source_refs,
        resource_version_value=resource_version_value,
        resource_content_hash=resource_content_hash,
        parse_warnings=parse_warnings,
        superseded_chunk_count=superseded_chunk_count,
        superseded_chunk_hashes=superseded_chunk_hashes,
        resource_fact_event_hashes=resource_fact_event_hashes,
        resource_fact_entity_hashes=resource_fact_entity_hashes,
        index_candidate_count=index_candidate_count,
        index_write_count=index_write_count,
        index_dropped_by_cap_count=index_dropped_by_cap_count,
        skill_hash=skill_hash,
        session_buffer_enabled=session_buffer_enabled,
        pending_event_count=pending_event_count,
        session_buffer_threshold=session_buffer_threshold,
        auto_batch_extract=auto_batch_extract,
        session_boundary_commit=session_boundary_commit,
        idle_commit_result=idle_commit_result,
        auto_batch_result=auto_batch_result,
        backend_readiness=backend_readiness,
    )
