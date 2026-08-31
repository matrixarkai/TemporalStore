#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource and skill ingestion runtime for MatrixArk local ingestion."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        MAX_INDEX_TERMS_PER_RESOURCE_FACT,
        MAX_RESOURCE_FACTS_PER_RESOURCE,
        MAX_RESOURCE_FACT_CHUNKS,
        Json,
        MatrixArkError,
        ResourceParserError,
        cleanup_temp_paths,
        debug_resource_metadata,
        embedding_for_text,
        embedding_text_for_chunk,
        embeddings_for_texts,
        new_secondary_index_budget,
        now_ms,
        parse_resource,
        parse_skill,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        secondary_index_budget_summary,
        serving_resource_metadata,
        should_extract_resource_fact,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        MAX_INDEX_TERMS_PER_RESOURCE_FACT,
        MAX_RESOURCE_FACTS_PER_RESOURCE,
        MAX_RESOURCE_FACT_CHUNKS,
        Json,
        MatrixArkError,
        ResourceParserError,
        cleanup_temp_paths,
        debug_resource_metadata,
        embedding_for_text,
        embedding_text_for_chunk,
        embeddings_for_texts,
        new_secondary_index_budget,
        now_ms,
        parse_resource,
        parse_skill,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        secondary_index_budget_summary,
        serving_resource_metadata,
        should_extract_resource_fact,
        stable_hash,
    )

try:
    from tools.matrixark_mcp_resource_import_task import resource_import_task_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_resource_import_task import resource_import_task_record

try:
    from tools import matrixark_mcp_ingest_resource_records as resource_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_records as resource_record_builders

try:
    from tools import matrixark_mcp_ingest_resource_facts as resource_fact_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_facts as resource_fact_helpers

try:
    from tools import matrixark_mcp_ingest_resource_queue as resource_queue_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_queue as resource_queue_helpers

try:
    from tools import matrixark_mcp_ingest_resource_chunks as resource_chunk_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_chunks as resource_chunk_helpers

try:
    from tools import matrixark_mcp_ingest_resource_summary as resource_summary_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_summary as resource_summary_helpers

try:
    from tools import matrixark_mcp_ingest_resource_chunk_records as resource_chunk_record_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_chunk_records as resource_chunk_record_helpers


def ingest_resource_or_skill_if_needed(
    self: Any,
    *,
    args: Json,
    envelope: Json,
    hook: Json,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    node_materialization: Json,
    early_deployment_scope: str,
    early_sharing_scope: str,
) -> Json:
    resource_chunk_hashes: list[int] = []
    resource_dirty_hashes: list[int] = []
    resource_parse_error = ""
    resource_import_task_hash = 0
    resource_import_task_status = "not_applicable"
    resource_import_wait = True
    resource_import_metrics: Json = {}
    resource_fact_event_hashes: list[int] = []
    resource_fact_entity_hashes: list[int] = []
    skill_hash = None
    raw_uri = ""
    requested_raw_uri = ""
    raw_storage_policy = ""
    storage_resolution: Json = {}
    original_chunk_count = 0
    deduped_chunk_count = 0
    deduped_source_refs: list[str] = []
    resource_version_value = ""
    resource_content_hash = ""
    parse_warnings: list[str] = []
    superseded_chunk_count = 0
    superseded_chunk_hashes: list[int] = []
    index_candidate_count = 0
    index_write_count = 0
    index_dropped_by_cap_count = 0
    if envelope["kind"] in {"resource", "skill"}:
        requested_raw_uri = str(envelope.get("raw_uri") or envelope["metadata"].get("raw_uri") or "inline-resource")
        resource_type = str(envelope.get("resource_type") or envelope["metadata"].get("resource_type") or "")
        async_default_reason = self._resource_import_async_default_reason(args, envelope, requested_raw_uri)
        resource_import_wait = bool(args.get("wait", not bool(async_default_reason)))
        resource_import_background = bool(args.get("_background_resource_import", False))
        deployment_scope = early_deployment_scope
        sharing_scope = early_sharing_scope
        access_scope = registry_access_scope(envelope["scope"], sharing_scope=sharing_scope)
        resource_record_scope = access_scope if sharing_scope in {"tenant_shared", "global_shared"} else envelope["scope"]
        provided_task_hash = args.get("_resource_import_task_hash")
        resource_import_task_hash = (
            int(provided_task_hash)
            if isinstance(provided_task_hash, int) and provided_task_hash > 0
            else stable_hash(f"resource_import_task:{envelope['kind']}:{requested_raw_uri}:{node_hash}:{envelope['ingestion_time_ms']}")
        )
        import_started_perf = time.perf_counter()
        raw_uri = requested_raw_uri
        raw_storage_policy = "raw_uri_only"
        storage_resolution: Json = {
            "storage_mode": resource_storage_mode_from_args(args, envelope, deployment_scope),
            "original_raw_uri": requested_raw_uri,
            "stored_raw_uri": requested_raw_uri,
            "parse_uri": requested_raw_uri,
            "parse_text": None,
            "raw_storage_policy": raw_storage_policy,
            "raw_bytes_stored": False,
            "upload_status": "not_started",
            "temp_paths": [],
        }
        queued_response = resource_queue_helpers.queue_resource_import_if_needed(
            self,
            args=args,
            envelope=envelope,
            hook=hook,
            event_id_hash=event_id_hash,
            node_hash=node_hash,
            node_path=node_path,
            node_materialization=node_materialization,
            resource_record_scope=resource_record_scope,
            requested_raw_uri=requested_raw_uri,
            resource_type=resource_type,
            resource_import_task_hash=resource_import_task_hash,
            resource_import_wait=resource_import_wait,
            resource_import_background=resource_import_background,
            storage_resolution=storage_resolution,
            raw_storage_policy=raw_storage_policy,
            async_default_reason=async_default_reason,
        )
        if queued_response is not None:
            return {"queued_response": queued_response}
        resource_import_task_status = "running"
        resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
        try:
            storage_resolution = resolve_raw_resource_for_ingest(
                {**args, "_engine_blob_client": getattr(self, "_client", None)},
                envelope,
                requested_raw_uri,
                resource_type,
                deployment_scope,
                resource_text,
            )
        except MatrixArkError as exc:
            self.append(
                resource_import_task_record(
                    task_hash=resource_import_task_hash,
                    status="failed",
                    kind=envelope["kind"],
                    raw_uri=requested_raw_uri,
                    requested_raw_uri=requested_raw_uri,
                    resource_type=resource_type,
                    raw_storage_mode=str(storage_resolution["storage_mode"]),
                    raw_storage_policy=str(storage_resolution["raw_storage_policy"]),
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    progress={"stage": "failed", "percent": 100},
                    updated_at_ms=now_ms(),
                    extra={"error": str(exc)},
                )
            )
            raise
        raw_uri = str(storage_resolution["stored_raw_uri"])
        parse_uri = str(storage_resolution.get("parse_uri") or raw_uri)
        parse_text = storage_resolution.get("parse_text")
        raw_storage_policy = str(storage_resolution.get("raw_storage_policy") or "raw_uri_only")
        self.append(
            resource_import_task_record(
                task_hash=resource_import_task_hash,
                status="running",
                kind=envelope["kind"],
                raw_uri=raw_uri,
                requested_raw_uri=requested_raw_uri,
                resource_type=resource_type,
                raw_storage_mode=str(storage_resolution["storage_mode"]),
                raw_storage_policy=raw_storage_policy,
                node_hash=node_hash,
                node_path=node_path,
                scope=resource_record_scope,
                storage_options=envelope.get("storage_options", {}),
                progress={"stage": "running", "percent": 10},
                updated_at_ms=now_ms(),
                extra={
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                },
            )
        )
        try:
            if envelope["kind"] == "skill" or (resource_type or "").lower() == "skill":
                parsed_skill = parse_skill(
                    parse_uri,
                    text=parse_text,
                    chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                    max_text_chars=args.get("max_skill_bytes") if isinstance(args.get("max_skill_bytes"), int) else None,
                )
                parsed_skill_chunks = rewrite_chunk_uris(parsed_skill.chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
                skill_hash = stable_hash(f"skill:{raw_uri}:{parsed_skill.name}:{parsed_skill.metadata.get('version', '1')}")
                skill_serving_metadata = serving_resource_metadata(parsed_skill.metadata)
                self.append(
                    resource_record_builders.skill_manifest_record(
                        skill_hash=skill_hash,
                        import_task_hash=resource_import_task_hash,
                        node_hash=node_hash,
                        node_path=node_path,
                        raw_uri=raw_uri,
                        requested_raw_uri=requested_raw_uri,
                        raw_storage_mode=str(storage_resolution["storage_mode"]),
                        raw_storage_policy=raw_storage_policy,
                        storage_resolution=storage_resolution,
                        name=parsed_skill.name,
                        description=parsed_skill.description,
                        metadata=parsed_skill.metadata,
                        access_scope=access_scope,
                        deployment_scope=deployment_scope,
                        text=parsed_skill.text,
                        token_estimate=parsed_skill.token_estimate,
                        serving_metadata=skill_serving_metadata,
                        scope=resource_record_scope,
                        storage_options=envelope.get("storage_options", {}),
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                )
                skill_debug_metadata = debug_resource_metadata(parsed_skill.metadata)
                if skill_debug_metadata or parsed_skill.text:
                    self.append(
                        resource_record_builders.skill_parse_debug_record(
                            skill_hash=skill_hash,
                            import_task_hash=resource_import_task_hash,
                            node_hash=node_hash,
                            node_path=node_path,
                            raw_uri=raw_uri,
                            metadata_debug=skill_debug_metadata,
                            text=parsed_skill.text,
                            scope=resource_record_scope,
                            updated_at_ms=envelope["ingestion_time_ms"],
                        )
                    )
                self.append(
                    resource_record_builders.skill_registry_record(
                        skill_hash=skill_hash,
                        import_task_hash=resource_import_task_hash,
                        raw_uri=raw_uri,
                        requested_raw_uri=requested_raw_uri,
                        raw_storage_mode=str(storage_resolution["storage_mode"]),
                        raw_storage_policy=raw_storage_policy,
                        storage_resolution=storage_resolution,
                        name=parsed_skill.name,
                        description=parsed_skill.description,
                        metadata=parsed_skill.metadata,
                        access_scope=access_scope,
                        deployment_scope=deployment_scope,
                        node_hash=node_hash,
                        node_path=node_path,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                )
                skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                self.append(
                    resource_record_builders.context_embedding_record(
                        embedding_type="skill_summary",
                        ref_type="skill",
                        ref_hash=skill_hash,
                        node_hash=node_hash,
                        node_path=node_path,
                        vector=skill_vector,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                )
                parsed_chunks = parsed_skill_chunks
            else:
                parsed_chunks = parse_resource(
                    parse_uri,
                    resource_type=resource_type or None,
                    text=parse_text,
                    chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                    resource_version=args.get("resource_version") if isinstance(args.get("resource_version"), str) else None,
                    supersedes_chunk_hashes=args.get("supersedes_chunk_hashes") if isinstance(args.get("supersedes_chunk_hashes"), dict) else None,
                )
                parsed_chunks = rewrite_chunk_uris(parsed_chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
        except ResourceParserError as exc:
            resource_parse_error = str(exc)
            parsed_chunks = []
        finally:
            cleanup_temp_paths([str(path) for path in storage_resolution.get("temp_paths", []) if isinstance(path, str)])
        if not parsed_chunks:
            resource_import_task_status = "failed"
            self.append(
                resource_import_task_record(
                    task_hash=resource_import_task_hash,
                    status="failed",
                    kind=envelope["kind"],
                    raw_uri=raw_uri,
                    requested_raw_uri=requested_raw_uri,
                    resource_type=resource_type,
                    raw_storage_mode=str(storage_resolution["storage_mode"]),
                    raw_storage_policy=raw_storage_policy,
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    progress={"stage": "failed", "percent": 100},
                    updated_at_ms=now_ms(),
                    extra={
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "error": resource_parse_error or "resource ingestion produced no chunks",
                    },
                )
            )
            raise MatrixArkError(resource_parse_error or "resource ingestion produced no chunks")
        normalized_chunks = resource_chunk_helpers.normalize_resource_chunks(parsed_chunks)
        parsed_chunks = normalized_chunks["chunks"]
        original_chunk_count = int(normalized_chunks["original_chunk_count"])
        deduped_chunk_count = int(normalized_chunks["deduped_chunk_count"])
        deduped_source_refs = list(normalized_chunks["deduped_source_refs"])
        if not parsed_chunks:
            raise MatrixArkError("resource ingestion produced only duplicate chunks")
        resource_version_value = str(normalized_chunks["resource_version"])
        resource_content_hash = str(normalized_chunks["resource_content_hash"])
        superseded_chunk_count = int(normalized_chunks["superseded_chunk_count"])
        superseded_chunk_hashes = list(normalized_chunks["superseded_chunk_hashes"])
        parse_warnings = list(normalized_chunks["parse_warnings"])
        chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
        index_write_count = 0
        index_candidate_count = 0
        index_dropped_by_cap_count = 0
        secondary_index_budget = new_secondary_index_budget()
        summary_result = resource_summary_helpers.append_resource_summary_and_indexes(
            self,
            envelope=envelope,
            parsed_chunks=parsed_chunks,
            raw_uri=raw_uri,
            resource_type=resource_type,
            resource_import_task_hash=resource_import_task_hash,
            node_hash=node_hash,
            node_path=node_path,
            resource_record_scope=resource_record_scope,
            skill_hash=skill_hash,
            skill_name=parsed_skill.name if skill_hash is not None else "",
            skill_metadata=parsed_skill.metadata if skill_hash is not None else {},
            secondary_index_budget=secondary_index_budget,
        )
        resource_kind = str(summary_result["resource_kind"])
        resource_summary_hash = int(summary_result["resource_summary_hash"])
        resource_dirty_hashes = list(summary_result["resource_dirty_hashes"])
        index_candidate_count += int(summary_result["index_candidate_count"])
        index_write_count += int(summary_result["index_write_count"])
        resource_manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
        raw_uri_hash = stable_hash(raw_uri)
        if envelope["kind"] == "resource":
            manifest_hash = resource_manifest_hash
            self.append(
                resource_record_builders.resource_manifest_record(
                    resource_hash=manifest_hash,
                    import_task_hash=resource_import_task_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    raw_uri=raw_uri,
                    requested_raw_uri=requested_raw_uri,
                    resource_type=str(resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                    resource_version=resource_version_value,
                    content_hash=resource_content_hash,
                    raw_storage_mode=str(storage_resolution["storage_mode"]),
                    raw_storage_policy=raw_storage_policy,
                    storage_resolution=storage_resolution,
                    parse_warnings=parse_warnings,
                    chunk_count=len(parsed_chunks),
                    original_chunk_count=original_chunk_count,
                    deduped_chunk_count=deduped_chunk_count,
                    deduped_source_refs=deduped_source_refs,
                    superseded_chunk_count=superseded_chunk_count,
                    superseded_chunk_hashes=superseded_chunk_hashes,
                    summary_dirty_hashes=resource_dirty_hashes,
                    access_scope=access_scope,
                    deployment_scope=deployment_scope,
                    token_estimate=sum(chunk.token_estimate for chunk in parsed_chunks),
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
            self.append(
                resource_record_builders.resource_registry_record(
                    resource_hash=manifest_hash,
                    import_task_hash=resource_import_task_hash,
                    raw_uri=raw_uri,
                    requested_raw_uri=requested_raw_uri,
                    resource_type=str(resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                    resource_version=resource_version_value,
                    content_hash=resource_content_hash,
                    chunk_count=len(parsed_chunks),
                    superseded_chunk_hashes=superseded_chunk_hashes,
                    raw_storage_mode=str(storage_resolution["storage_mode"]),
                    raw_storage_policy=raw_storage_policy,
                    storage_resolution=storage_resolution,
                    access_scope=access_scope,
                    deployment_scope=deployment_scope,
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        chunk_append_result = resource_chunk_record_helpers.append_resource_chunk_records(
            self,
            envelope=envelope,
            parsed_chunks=parsed_chunks,
            chunk_vectors=chunk_vectors,
            raw_uri=raw_uri,
            raw_uri_hash=raw_uri_hash,
            resource_type=resource_type,
            resource_manifest_hash=resource_manifest_hash,
            resource_import_task_hash=resource_import_task_hash,
            node_hash=node_hash,
            node_path=node_path,
            access_scope=access_scope,
            deployment_scope=deployment_scope,
            resource_record_scope=resource_record_scope,
            skill_hash=skill_hash,
            skill_name=parsed_skill.name if skill_hash is not None else "",
            skill_metadata=parsed_skill.metadata if skill_hash is not None else {},
            secondary_index_budget=secondary_index_budget,
        )
        resource_chunk_hashes = list(chunk_append_result["resource_chunk_hashes"])
        index_candidate_count += int(chunk_append_result["index_candidate_count"])
        index_write_count += int(chunk_append_result["index_write_count"])
        index_dropped_by_cap_count += int(chunk_append_result["index_dropped_by_cap_count"])
        fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
        resource_fact_records, resource_fact_event_hashes, resource_fact_entity_hashes = resource_fact_helpers.build_resource_fact_records(
            fact_chunks=fact_chunks,
            envelope=envelope,
            raw_uri=raw_uri,
            resource_version=resource_version_value,
            node_hash=node_hash,
            node_path=node_path,
            scope=resource_record_scope,
            resource_hash=resource_manifest_hash,
            batch_id_hash=resource_import_task_hash,
            max_facts=MAX_RESOURCE_FACTS_PER_RESOURCE,
        )
        if resource_fact_records:
            self.append_many(resource_fact_records)
        resource_import_metrics = {
            "duration_ms": round((time.perf_counter() - import_started_perf) * 1000.0, 3),
            "parser_chunk_count": original_chunk_count,
            "chunk_count": len(parsed_chunks),
            "dedupe_count": deduped_chunk_count,
            "embedding_count": len(chunk_vectors) + 1 + len(resource_fact_event_hashes) + len(resource_fact_entity_hashes),
            "resource_fact_count": len(resource_fact_event_hashes),
            "resource_entity_count": len(resource_fact_entity_hashes),
            "index_candidate_count": index_candidate_count,
            "index_write_count": index_write_count,
            "index_dropped_by_cap_count": index_dropped_by_cap_count,
            **secondary_index_budget_summary(secondary_index_budget),
            "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
            "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
            "parse_warning_count": len(parse_warnings),
            "parse_warnings": parse_warnings[:100],
            "raw_storage_mode": storage_resolution["storage_mode"],
            "raw_storage_policy": raw_storage_policy,
            "raw_bytes_stored": False,
            "upload_status": storage_resolution.get("upload_status", "not_required"),
            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
            "cloud_key": storage_resolution.get("cloud_key", ""),
            "summary_dirty_count": len(resource_dirty_hashes),
        }
        resource_import_task_status = "completed"
        self.append(
            resource_import_task_record(
                task_hash=resource_import_task_hash,
                status="completed",
                kind=envelope["kind"],
                raw_uri=raw_uri,
                requested_raw_uri=requested_raw_uri,
                resource_type=resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                raw_storage_mode=str(storage_resolution["storage_mode"]),
                raw_storage_policy=raw_storage_policy,
                node_hash=node_hash,
                node_path=node_path,
                scope=resource_record_scope,
                progress={"stage": "completed", "percent": 100},
                updated_at_ms=now_ms(),
                extra={
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "resource_fact_count": len(resource_fact_event_hashes),
                    "resource_entity_count": len(resource_fact_entity_hashes),
                    "index_candidate_count": index_candidate_count,
                    "index_write_count": index_write_count,
                    "index_dropped_by_cap_count": index_dropped_by_cap_count,
                    **secondary_index_budget_summary(secondary_index_budget),
                    "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                    "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "metrics": resource_import_metrics,
                },
            )
        )
        self.append(
            {
                "record_type": "matrixark_metric",
                "metric_name": "resource_import",
                "task_hash": resource_import_task_hash,
                "kind": envelope["kind"],
                "raw_uri": raw_uri,
                "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                "metrics": resource_import_metrics,
                "progress": {"stage": "completed", "percent": 100},
                "scope": resource_record_scope,
                "created_at_ms": now_ms(),
            }
        )
    return {
        "queued_response": None,
        "hot_record_scope": resource_record_scope if envelope["kind"] in {"resource", "skill"} else envelope["scope"],
        "resource_dirty_hashes": resource_dirty_hashes,
        "resource_import_task_hash": resource_import_task_hash,
        "resource_import_task_status": resource_import_task_status,
        "resource_import_wait": resource_import_wait,
        "resource_import_metrics": resource_import_metrics,
        "raw_uri": raw_uri,
        "requested_raw_uri": requested_raw_uri,
        "storage_resolution": storage_resolution,
        "raw_storage_policy": raw_storage_policy,
        "resource_chunk_hashes": resource_chunk_hashes,
        "original_chunk_count": original_chunk_count,
        "deduped_chunk_count": deduped_chunk_count,
        "deduped_source_refs": deduped_source_refs,
        "resource_version_value": resource_version_value,
        "resource_content_hash": resource_content_hash,
        "parse_warnings": parse_warnings,
        "superseded_chunk_count": superseded_chunk_count,
        "superseded_chunk_hashes": superseded_chunk_hashes,
        "resource_fact_event_hashes": resource_fact_event_hashes,
        "resource_fact_entity_hashes": resource_fact_entity_hashes,
        "index_candidate_count": index_candidate_count,
        "index_write_count": index_write_count,
        "index_dropped_by_cap_count": index_dropped_by_cap_count,
        "skill_hash": skill_hash,
    }
