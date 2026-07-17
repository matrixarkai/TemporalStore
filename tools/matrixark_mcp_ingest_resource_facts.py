#!/usr/bin/env python3
"""Resource fact record extraction helpers for MatrixArk local ingestion."""

from __future__ import annotations
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        embedding_for_text,
        embedding_model_name,
        extract_resource_facts,
        serving_resource_metadata,
        source_locator_from_ref,
        stable_hash,
        summarize_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        embedding_for_text,
        embedding_model_name,
        extract_resource_facts,
        serving_resource_metadata,
        source_locator_from_ref,
        stable_hash,
        summarize_text,
    )


def build_resource_fact_records(
    *,
    fact_chunks: list[Any],
    envelope: Json,
    raw_uri: str,
    resource_version: str,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    resource_hash: int,
    batch_id_hash: int,
    max_facts: int,
) -> tuple[list[Json], list[int], list[int]]:
    records: list[Json] = []
    event_hashes: list[int] = []
    entity_hashes: list[int] = []
    remaining_budget = max(0, max_facts)
    for chunk in fact_chunks:
        if remaining_budget <= 0:
            break
        source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
        chunk_metadata = serving_resource_metadata({**chunk.metadata, "source_locator": source_locator})
        for fact_extraction in extract_resource_facts(
            chunk,
            chunk_metadata=chunk_metadata,
            envelope=envelope,
            raw_uri=raw_uri,
            resource_version=resource_version,
        )[:remaining_budget]:
            remaining_budget -= 1
            fact_event_type = str(fact_extraction["event_type"])
            fact_entity_type = str(fact_extraction["entity_type"])
            fact_value = str(fact_extraction.get("value", ""))
            fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version}")
            event_hashes.append(fact_event_hash)
            fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
            records.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": fact_event_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "text": chunk.text,
                    "summary_text": fact_summary,
                    "classification": fact_extraction.get("classification", ""),
                    "event_type": fact_extraction.get("event_type", ""),
                    "entity_type": fact_extraction.get("entity_type", ""),
                    "status": fact_extraction.get("status", "observed"),
                    "source_kind": "resource_fact",
                    "envelope": {**envelope, "kind": "resource_fact"},
                    "internal_extraction": fact_extraction,
                    "source_chunk_hash": chunk.chunk_hash,
                    "resource_hash": resource_hash,
                    "source_locator": source_locator,
                    "resource_version": resource_version,
                    "scope": scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
            records.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": fact_event_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(fact_vector),
                    "model": embedding_model_name(),
                    "vector": fact_vector,
                    "scope": scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
            entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
            entity_hashes.append(entity_hash)
            entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
            records.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "entity_type": fact_entity_type,
                    "entity_name": entity_name,
                    "state": entity_state,
                    "confidence": fact_extraction.get("confidence", 0.78),
                    "operator": "LATEST",
                    "source_event_ids": [fact_event_hash],
                    "source_chunk_hash": chunk.chunk_hash,
                    "resource_hash": resource_hash,
                    "source_locator": source_locator,
                    "resource_version": resource_version,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
            records.append(
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
                    "scope": scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            # Resource facts are ContextEvent/ContextEntity records with source_chunk refs.
            # Resource chunk/index rows already provide secondary filtering, so avoid
            # per-fact event index fanout here.
    return records, event_hashes, entity_hashes
