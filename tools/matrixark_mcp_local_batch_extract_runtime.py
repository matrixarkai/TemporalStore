#!/usr/bin/env python3
"""Local MatrixArk batch extraction runtime."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        MAX_PRIOR_MESSAGES,
        Json,
        apply_entity_patches,
        collect_prior_context,
        context_index_posting_record,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        new_secondary_index_budget,
        normalized_node_path,
        now_ms,
        one_pass_memory_extraction,
        secondary_index_budget_summary,
        stable_hash,
        summarize_text,
        take_secondary_index_terms,
        text_from_messages,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MAX_PRIOR_MESSAGES,
        Json,
        apply_entity_patches,
        collect_prior_context,
        context_index_posting_record,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        new_secondary_index_budget,
        normalized_node_path,
        now_ms,
        one_pass_memory_extraction,
        secondary_index_budget_summary,
        stable_hash,
        summarize_text,
        take_secondary_index_terms,
        text_from_messages,
    )


def batch_extract_after_start(self: Any, args: Json, batch_start: Json) -> Json:
    envelope = batch_start["envelope"]
    hook = batch_start["hook"]
    threshold = batch_start["threshold"]
    derive_from_existing_events = bool(batch_start["derive_from_existing_events"])
    source_event_ids = list(batch_start["source_event_ids"])
    if batch_start.get("deferred_result") is not None:
        return batch_start["deferred_result"]

    prior_records = [] if args.get("skip_prior_context") else self.read_all()
    prior_context = (
        {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
        if args.get("skip_prior_context")
        else collect_prior_context(envelope, prior_records)
    )
    extraction_started_perf = time.perf_counter()
    extraction = one_pass_memory_extraction(envelope, prior_context=prior_context)
    self._observe_model_latency("batch_extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
    batch_text = text_from_messages(envelope["messages"])
    batch_id_hash = stable_hash(
        f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
    )
    node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
    node_path = normalized_node_path(envelope, node_hint)
    node_hash = stable_hash("/".join(node_path))
    node_materialization = self.ensure_context_node_path(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
    )
    batch_summary = extraction["batch_summary"]

    event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
    records_to_append: list[Json] = []
    event_rows: list[tuple[int, Json, str, int]] = []
    segment_hash_by_position: dict[int, int] = {}
    segment_hashes_by_position: dict[int, list[int]] = {}
    for segment in extraction["segments"]:
        segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
        for message_index in segment.get("message_indexes", []):
            if not isinstance(message_index, int):
                continue
            segment_hashes_by_position.setdefault(message_index, []).append(segment_hash)
            segment_hash_by_position.setdefault(message_index, segment_hash)
    if not derive_from_existing_events:
        for index, message in enumerate(envelope["messages"]):
            event_text = f"{message['role']}: {message['content']}"
            event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
            event_hashes.append(event_id_hash)
            event_rows.append((index, message, event_text, event_id_hash))
        event_vectors = embeddings_for_texts([event_text for _index, _message, event_text, _event_id_hash in event_rows])
        for (_index, message, event_text, event_id_hash), event_vector in zip(event_rows, event_vectors):
            records_to_append.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": event_id_hash,
                    "batch_id_hash": batch_id_hash,
                    "parent_segment_hash": segment_hash_by_position.get(_index),
                    "parent_segment_hashes": segment_hashes_by_position.get(_index, []),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "text": event_text,
                    "summary_text": summarize_text(event_text),
                    "classification": extraction["classification"],
                    "event_type": extraction["event_type"],
                    "status": "observed",
                    "source_kind": envelope.get("kind", "message"),
                    "envelope": {
                        **envelope,
                        "messages": [message],
                    },
                    "internal_extraction": {
                        "mode": extraction["mode"],
                        "classification": extraction["classification"],
                        "event_type": extraction["event_type"],
                        "batch_id_hash": batch_id_hash,
                    },
                    "prior_context": prior_context,
                    "agent_hook": hook,
                    "storage_options": envelope.get("storage_options", {}),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(event_vector),
                    "model": embedding_model_name(),
                    "vector": event_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

    entity_hashes = []
    for entity in extraction["entities"]:
        entity_hash = stable_hash(
            f"{node_hash}:{entity['entity_type']}:{entity['entity_name']}"
        )
        previous_entity = self.find_latest_entity(
            node_hash=node_hash,
            entity_type=entity["entity_type"],
            entity_name=entity["entity_name"],
        )
        updated_entity = apply_entity_patches(previous_entity, entity)
        entity_hashes.append(entity_hash)
        records_to_append.append(
            {
                "record_type": "context_entity",
                "entity_hash": entity_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "entity_type": updated_entity["entity_type"],
                "entity_name": updated_entity["entity_name"],
                "state": updated_entity["state"],
                "previous_state": updated_entity.get("previous_state", ""),
                "confidence": updated_entity["confidence"],
                "operator": updated_entity["operator"],
                "source_refs": updated_entity["source_refs"],
                "source_event_ids": source_event_ids,
                "field_patches": updated_entity.get("field_patches", []),
                "patch_results": updated_entity.get("patch_results", []),
                "update_mode": updated_entity.get("update_mode", ""),
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        if updated_entity.get("patch_results"):
            records_to_append.append(
                {
                    "record_type": "context_entity_update_audit",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "previous_state": updated_entity.get("previous_state", ""),
                    "new_state": updated_entity["state"],
                    "patch_results": updated_entity.get("patch_results", []),
                    "llm_calls": 0,
                    "update_mode": "deterministic_eua",
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        entity_embedding_text = updated_entity["entity_type"] + " " + updated_entity["state"]
        entity_vector = embedding_for_text(entity_embedding_text)
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "entity_state",
                "ref_type": "entity",
                "ref_hash": entity_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(entity_vector),
                "model": embedding_model_name(),
                "vector": entity_vector,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )

    segment_hashes = []
    for segment in extraction["segments"]:
        segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
        segment_hashes.append(segment_hash)
        records_to_append.append(
            {
                "record_type": "context_segment",
                "segment_hash": segment_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "topic": segment["topic"],
                "coordinate_tuples": segment["coordinate_tuples"],
                "message_indexes": segment["message_indexes"],
                "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                "saliency_score": segment["saliency_score"],
                "summary_text": segment["summary_text"],
                "text": segment["text"],
                "non_contiguous": segment["non_contiguous"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        segment_embedding_text = segment["topic"] + " " + segment["summary_text"]
        segment_vector = embedding_for_text(segment_embedding_text)
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "segment_text",
                "ref_type": "segment",
                "ref_hash": segment_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(segment_vector),
                "model": embedding_model_name(),
                "vector": segment_vector,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )

    summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
    records_to_append.append(
        {
            "record_type": "context_summary",
            "summary_type": "batch_l0",
            "summary_hash": summary_hash,
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "summary_text": batch_summary,
            "source_entity_hashes": entity_hashes,
            "source_segment_hashes": segment_hashes,
            "source_event_ids": event_hashes,
            "scope": envelope["scope"],
            "updated_at_ms": envelope["ingestion_time_ms"],
        }
    )
    summary_embedding_text = " ".join(node_path + [batch_summary])
    summary_vector = embedding_for_text(summary_embedding_text)
    records_to_append.append(
        {
            "record_type": "context_embedding",
            "embedding_type": "batch_l0",
            "ref_type": "summary",
            "ref_hash": summary_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "dim": len(summary_vector),
            "model": embedding_model_name(),
            "vector": summary_vector,
            "scope": envelope["scope"],
            "updated_at_ms": envelope["ingestion_time_ms"],
        }
    )
    secondary_index_budget = new_secondary_index_budget()
    batch_index_terms = take_secondary_index_terms(list(extraction["indexes"]), secondary_index_budget)
    for index_name in batch_index_terms:
        records_to_append.append(
            context_index_posting_record(
                index_name=index_name,
                capability="context_batch_commit",
                batch_id_hash=batch_id_hash,
                node_hash=node_hash,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
            )
        )
    records_to_append.append(
        {
            "record_type": "context_extraction_audit",
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "schema": extraction["schema"],
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "outputs": {
                "events": 0 if derive_from_existing_events else len(envelope["messages"]),
                "source_events": len(event_hashes),
                "entities": len(entity_hashes),
                "segments": len(segment_hashes),
                "summaries": 1,
                "indexes": len(batch_index_terms),
                **secondary_index_budget_summary(secondary_index_budget),
            },
            "mode": extraction["mode"],
            "derive_from_existing_events": derive_from_existing_events,
            "source_event_ids": event_hashes,
            "agent_hook": hook,
            "created_at_ms": now_ms(),
        }
    )
    dirty_hashes, dirty_records = self.node_summary_dirty_records(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
        source_ref_type="batch",
        source_hash_field="source_batch_hash",
        source_hash=batch_id_hash,
        dirty_reason="new_event",
    )
    records_to_append.extend(dirty_records)
    self.append_many(records_to_append)
    summary_refresh = {
        "status": "dirty_marked",
        "dirty_hashes": dirty_hashes,
        "refresh_result": None,
        "async_required": True,
        "write_path": "coalesced_with_batch_extract",
    }
    return {
        "status": "accepted",
        "mode": extraction["mode"],
        "segment_provider": extraction.get("segment_provider", {}),
        "classification": extraction["classification"],
        "batch_id_hash": batch_id_hash,
        "node_hash": node_hash,
        "storage_options": envelope.get("storage_options", {}),
        "storage_route": envelope.get("storage_route", {}),
        "embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "message_count": extraction["message_count"],
        "token_count_estimate": extraction["token_count_estimate"],
        "events_written": 0 if derive_from_existing_events else len(envelope["messages"]),
        "source_event_count": len(event_hashes),
        "raw_events_duplicated": not derive_from_existing_events,
        "entities_written": len(entity_hashes),
        "segments_written": len(segment_hashes),
        "summary_hash": summary_hash,
        "summary_refresh": summary_refresh,
        "node_materialization": node_materialization,
        "indexes_written": len(batch_index_terms),
        **secondary_index_budget_summary(secondary_index_budget),
        "one_pass": True,
        "threshold_messages": threshold,
    }
