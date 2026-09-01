#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource/skill chunk record append helpers for MatrixArk local ingest."""

from __future__ import annotations
import os

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        Json,
        context_index_name,
        debug_resource_metadata,
        limited_index_terms,
        metadata_index_terms,
        ordered_unique,
        serving_resource_metadata,
        secondary_index_budget_summary,
        source_locator_from_ref,
        take_secondary_index_terms,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        Json,
        context_index_name,
        debug_resource_metadata,
        limited_index_terms,
        metadata_index_terms,
        ordered_unique,
        serving_resource_metadata,
        secondary_index_budget_summary,
        source_locator_from_ref,
        take_secondary_index_terms,
    )

try:
    from tools.matrixark_resource_parser import keywords_for_text
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import keywords_for_text

try:
    from tools import matrixark_mcp_ingest_resource_records as resource_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_records as resource_record_builders


# One append per record meant one backend write per record: a 2126-chunk skill emits
# 8504 of them, four per chunk. append() is defined as append_many([record]), so the
# records can be gathered and handed over in batches without changing what is written
# or the order it is written in.
# One record per (term, chunk) makes complete lexical coverage cost more than the content:
# 152,108 postings and 62.1 MB for a 1.5 MB document, 41.3x amplification, over only 160
# DISTINCT terms. The read path already expands a ref_hashes LIST
# (context_index_ref_hashes checks it before the singular fields), so one record per term
# carrying its posting list needs no reader change and measures 3.05 MB, 2.03x.
# keywords_for_text defaults to 12 terms, which covers only a chunk's opening: a needle at
# 97% through a 215-token chunk matched 0 of its keywords at 12 and all 8 at 200. Complete
# coverage needs roughly 76 per chunk, which is only affordable with posting lists.
INDEX_KEYWORD_LIMIT = int(os.environ.get("MATRIXARK_INDEX_KEYWORD_LIMIT", "12"))

# Default ON. One index record per (chunk, term) pair is 83.3% of everything a skill ingest
# writes -- 33,020 of the 39,624 records a 1 MB skill produces. Coalescing them into one posting
# per term, measured on that document:
#
#     records   39,624 -> 9,659    (-75.6%)
#     bytes     18.4 MB -> 6.9 MB  (-62.2%)   amplification 17.5x -> 6.6x
#     emission   0.656s -> 0.428s  (-34.7%)
#
# The index content is unchanged: the same 3,026 terms carrying the same 36,046 references,
# checked through context_index_ref_hashes, the helper the retrieve path itself uses.
#
# This could not be turned on before. Serving resolved a record's identity through the singular
# fields and never read `ref_hashes`, so a posting carrying two refs had no identity and was
# dropped outright -- silently, with no length mismatch to raise an error. That is fixed, and
# the emitter now splits at MAX_SECONDARY_INDEX_REFS_PER_POSTING like the compactor does.
try:  # package path
    from tools.matrixark_mcp_core_query_analysis import index_term_is_consultable
except ImportError:  # top-level path
    from matrixark_mcp_core_query_analysis import index_term_is_consultable

# Default ON. See index_term_is_consultable: a term whose kind the query analyser can never
# emit cannot appear in a filter group, so it cannot narrow a search or earn the hint boost. On a
# 1 MB skill those terms are 1,418 KB of the 1,471 KB index -- 15.7% of the ingest -- and dropping
# them takes amplification 8.6x to 7.2x while embeddings become the majority of the footprint
# (44.6% -> 53.0%), which is what an embedding-first store should look like.
#
# Set MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS=0 to write every term again.
INDEX_ONLY_CONSULTABLE_TERMS = os.environ.get(
    "MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS", "1"
).strip().lower() not in {"0", "false", "no", "off", ""}

INDEX_POSTING_LISTS = os.environ.get(
    "MATRIXARK_INDEX_POSTING_LISTS", "1"
) not in {"0", "false", "False", ""}

DEDUPE_SKILL_CHUNK_EMBEDDING = os.environ.get(
    "MATRIXARK_DEDUPE_SKILL_CHUNK_EMBEDDING", "1"
) not in {"0", "false", "False", ""}

# A skill chunk's text is written TWICE: once as `resource_chunk` and once as `skill_section`,
# byte for byte. Measured on a 1.41 MB markdown skill: 411 chunks and 411 sections, and all 411
# section texts identical to a chunk's -- `resource_chunk` is 42.1% of the bytes a skill ingest
# writes, for a copy.
#
# Retrieval never reads it: the resource/skill scan skips `resource_chunk` outright when
# `resource_type == "skill"` and serves the section instead. The dashboard's chunk view was the
# only other reader, and it now accepts `skill_section` too.
#
# Off restores the second copy.
try:  # package path
    from tools.matrixark_mcp_core import (  # noqa: F401
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        _chunked_refs as chunked_posting_refs,
    )
except ImportError:  # top-level path
    from matrixark_mcp_core import (  # noqa: F401
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        _chunked_refs as chunked_posting_refs,
    )

DEDUPE_SKILL_CHUNK_TEXT = os.environ.get(
    "MATRIXARK_DEDUPE_SKILL_CHUNK_TEXT", "1"
) not in {"0", "false", "False", ""}

RESOURCE_APPEND_BATCH_RECORDS = int(
    os.environ.get("MATRIXARK_RESOURCE_APPEND_BATCH_RECORDS", "512")
)


def _flush_pending_records(adapter: Any, pending: list) -> list:
    """Hand the gathered records to the adapter and start a fresh batch."""
    if not pending:
        return pending
    append_many = getattr(adapter, "append_many", None)
    if callable(append_many):
        append_many(pending)
    else:
        for record in pending:
            adapter.append(record)
    return []


def append_resource_chunk_records(
    adapter: Any,
    *,
    envelope: Json,
    parsed_chunks: list[Any],
    chunk_vectors: list[list[float]],
    raw_uri: str,
    raw_uri_hash: int,
    resource_type: str,
    resource_manifest_hash: int,
    resource_import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    access_scope: Json,
    deployment_scope: str,
    resource_record_scope: Json,
    skill_hash: int | None,
    skill_name: str,
    skill_metadata: Json,
    secondary_index_budget: Json,
) -> Json:
    resource_chunk_hashes: list[int] = []
    pending_records: list = []
    index_postings: dict = {}
    index_candidate_count = 0
    index_write_count = 0
    index_dropped_by_cap_count = 0
    for chunk, vector in zip(parsed_chunks, chunk_vectors):
        resource_chunk_hashes.append(chunk.chunk_hash)
        source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
        chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
        chunk_metadata = serving_resource_metadata(chunk_metadata_source)
        chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
        chunk_resource_hash = resource_manifest_hash if skill_hash is None else skill_hash
        if skill_hash is not None:
            section_heading = str(chunk_metadata.get("heading", ""))
            section_metadata = (
                {key: value for key, value in chunk_metadata.items() if key != "heading"}
                if section_heading
                else chunk_metadata
            )
            pending_records.append(
                # The record carries `heading` at the top level, so the copy inside its own
                # metadata restates it on every chunk. Dropped only when the top-level field
                # actually received the value, so a chunk without a heading is untouched.
                # `heading_slug` stays: it is the only source of the heading_slug: index terms.
                resource_record_builders.skill_section_record(
                    import_task_hash=resource_import_task_hash,
                    skill_hash=skill_hash,
                    section_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    raw_uri_hash=raw_uri_hash,
                    source_locator=source_locator,
                    heading=section_heading,
                    text=chunk.text,
                    token_estimate=chunk.token_estimate,
                    metadata=section_metadata,
                    access_scope=access_scope,
                    deployment_scope=deployment_scope,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        # For a skill this record is a byte-identical second copy of the section written just
        # above. Retrieval skips it (`resource_chunk` + `resource_type == "skill"` is filtered out
        # of the resource/skill scan) and the dashboard now reads the section, so writing it costs
        # 42.1% of a skill ingest's bytes for a duplicate nobody reads.
        if skill_hash is None or not DEDUPE_SKILL_CHUNK_TEXT:
            pending_records.append(
                resource_record_builders.resource_chunk_record(
                    import_task_hash=resource_import_task_hash,
                    chunk_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    resource_hash=chunk_resource_hash,
                    raw_uri_hash=raw_uri_hash,
                    resource_type=str(chunk_metadata.get("resource_type") or resource_type),
                    source_locator=source_locator,
                    text=chunk.text,
                    token_estimate=chunk.token_estimate,
                    metadata=chunk_metadata,
                    access_scope=access_scope,
                    deployment_scope=deployment_scope,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        if chunk_debug_metadata:
            pending_records.append(
                resource_record_builders.resource_chunk_debug_record(
                    ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                    chunk_hash=chunk.chunk_hash,
                    import_task_hash=resource_import_task_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    resource_hash=chunk_resource_hash,
                    raw_uri_hash=raw_uri_hash,
                    raw_uri=raw_uri,
                    source_locator=source_locator,
                    source_ref=chunk.source_ref,
                    metadata_debug=chunk_debug_metadata,
                    text=chunk.text,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        # A skill chunk used to store the SAME vector twice, once as embedding_type
        # resource_chunk and once as skill_section, with the same ref_hash. Retrieval keys
        # its vector map on ref_hash ALONE and both land in it, so the second copy only ever
        # overwrote the first with an identical value - about 37% of what a skill ingest
        # writes, for nothing. Skills now store the skill_section copy only.
        if skill_hash is None or not DEDUPE_SKILL_CHUNK_EMBEDDING:
            pending_records.append(
                resource_record_builders.context_embedding_record(
                    embedding_type="resource_chunk",
                    ref_type="resource_chunk",
                    ref_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    vector=vector,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        if skill_hash is not None:
            pending_records.append(
                resource_record_builders.context_embedding_record(
                    embedding_type="skill_section",
                    ref_type="skill_section",
                    ref_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    vector=vector,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        # metadata_index_terms reads keywords OUT OF the stored metadata, and slim chunk
        # metadata drops that field - which silently removed the entire content keyword index,
        # the only part of it with any selectivity. Derive them from the chunk text instead, so
        # what gets indexed no longer depends on what happens to be stored.
        index_metadata = chunk.metadata
        if not index_metadata.get("keywords"):
            index_metadata = {
                **index_metadata,
                "keywords": keywords_for_text(chunk.text, limit=INDEX_KEYWORD_LIMIT),
            }
        raw_chunk_index_terms = (
            [
                context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
            ]
            + metadata_index_terms(index_metadata)
            + (
                [context_index_name("skill_name", skill_name)]
                + [context_index_name("skill_trigger", trigger) for trigger in skill_metadata.get("triggers", [])]
                + [context_index_name("skill_tool", tool) for tool in skill_metadata.get("allowed_tools", [])]
                if skill_hash is not None
                else []
            )
        )
        index_candidate_count += len([term for term in raw_chunk_index_terms if term])
        chunk_index_terms = limited_index_terms(
            raw_chunk_index_terms,
            limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        )
        index_dropped_by_cap_count += max(
            0,
            len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms),
        )
        # Drop terms no query can ask for. passes_secondary_index_filters only ever intersects a
        # candidate's terms with the groups infer_secondary_index_filter_groups produces, and that
        # inference emits a fixed set of KINDS -- so a term outside it cannot narrow a search or
        # earn the hint boost whatever its value. On a 1 MB skill those terms are 96.4% of the
        # index and 15.7% of the whole ingest, written and scanned to affect nothing.
        if INDEX_ONLY_CONSULTABLE_TERMS:
            chunk_index_terms = [
                term for term in chunk_index_terms if index_term_is_consultable(term)
            ]
        chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
        for index_name in chunk_index_terms:
            index_write_count += 1
            if INDEX_POSTING_LISTS:
                index_postings.setdefault(index_name, []).append(chunk.chunk_hash)
                continue
            pending_records.append(
                resource_record_builders.resource_chunk_index_record(
                    index_name=index_name,
                    ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                    chunk_hash=chunk.chunk_hash,
                    resource_hash=chunk_resource_hash,
                    source_locator=source_locator,
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        if len(pending_records) >= RESOURCE_APPEND_BATCH_RECORDS:
            pending_records = _flush_pending_records(adapter, pending_records)
    for index_name, chunk_hashes in index_postings.items():
        # Split at the same bound compact_context_index_postings uses. A 1 MB skill puts every
        # one of its chunks under terms like source_type, so an uncapped posting here reached
        # 3,303 refs against a cap of 512 -- and a cap that one producer of a record type
        # observes and another ignores is not a bound on anything.
        for part, ref_chunk in enumerate(
            chunked_posting_refs(chunk_hashes, limit=MAX_SECONDARY_INDEX_REFS_PER_POSTING)
        ):
            record = resource_record_builders.resource_chunk_index_record(
                index_name=index_name,
                ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                chunk_hash=ref_chunk[0],
                resource_hash=resource_manifest_hash if skill_hash is None else skill_hash,
                source_locator="",
                node_hash=node_hash,
                node_path=node_path,
                scope=resource_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
            # The reader takes ref_hashes ahead of the singular fields, so this record stands
            # in for every posting of the term that falls in this part.
            record["ref_hashes"] = ref_chunk
            record["posting_part"] = part
            # `ref_hashes` is the one place a posting names what it points at. The singular
            # `ref_hash` restated it on every single-ref row, and `chunk_hash` restated it again;
            # the serving accessor reads neither when the list is present, and `index_hash` is
            # derived from the list, so both are dropped rather than written three ways. Older
            # rows still resolve -- the fallbacks that read them are unchanged.
            record.pop("ref_hash", None)
            record.pop("chunk_hash", None)
            pending_records.append(record)
            if len(pending_records) >= RESOURCE_APPEND_BATCH_RECORDS:
                pending_records = _flush_pending_records(adapter, pending_records)
    pending_records = _flush_pending_records(adapter, pending_records)
    # index_dropped_by_cap_count only counts terms lost to the PER-CHUNK term cap, which is
    # applied before the per-operation budget. The budget is what actually truncates a large
    # document: at its default of 128 records per ingest call, a 2,011-chunk document offers
    # 10,055 candidate terms and writes 128, and every count above reported that as no drop.
    # Report the budget alongside them so the truncation is visible to the caller.
    budget_summary = secondary_index_budget_summary(secondary_index_budget)
    return {
        "resource_chunk_hashes": resource_chunk_hashes,
        "index_candidate_count": index_candidate_count,
        "index_write_count": index_write_count,
        "index_dropped_by_cap_count": index_dropped_by_cap_count,
        "index_budget": budget_summary,
    }
