#!/usr/bin/env python3
"""Local MatrixArk ingest orchestration helpers."""

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
        aggregate_parse_warnings_from_chunks,
        cleanup_temp_paths,
        clip_context_text,
        content_hash,
        context_index_name,
        context_index_posting_record,
        context_node_key,
        debug_resource_metadata,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embedding_text_for_chunk,
        embeddings_for_texts,
        extract_resource_facts,
        infer_event_type,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
        non_default_classification,
        now_ms,
        ordered_unique,
        parse_resource,
        parse_skill,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        secondary_index_budget_summary,
        serving_resource_metadata,
        session_buffer_key,
        should_extract_resource_fact,
        source_locator_from_ref,
        stable_hash,
        summarize_resource_chunks,
        summarize_text,
        take_secondary_index_terms,
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
        aggregate_parse_warnings_from_chunks,
        cleanup_temp_paths,
        clip_context_text,
        content_hash,
        context_index_name,
        context_index_posting_record,
        context_node_key,
        debug_resource_metadata,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embedding_text_for_chunk,
        embeddings_for_texts,
        extract_resource_facts,
        infer_event_type,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
        non_default_classification,
        now_ms,
        ordered_unique,
        parse_resource,
        parse_skill,
        registry_access_scope,
        resolve_raw_resource_for_ingest,
        resource_storage_mode_from_args,
        rewrite_chunk_uris,
        secondary_index_budget_summary,
        serving_resource_metadata,
        session_buffer_key,
        should_extract_resource_fact,
        source_locator_from_ref,
        stable_hash,
        summarize_resource_chunks,
        summarize_text,
        take_secondary_index_terms,
    )


try:
    from tools.matrixark_mcp_async_ingest import lightweight_async_accept
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_async_ingest import lightweight_async_accept

try:
    from tools.matrixark_mcp_resource_import_task import resource_import_task_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_resource_import_task import resource_import_task_record

try:
    from tools.matrixark_mcp_ingest_setup import prepare_ingest_context
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_ingest_setup import prepare_ingest_context

try:
    from tools import matrixark_mcp_ingest_resource_records as resource_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_records as resource_record_builders


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
        if not resource_import_background:
            self.append(
                resource_import_task_record(
                    task_hash=resource_import_task_hash,
                    status="queued",
                    kind=envelope["kind"],
                    raw_uri=requested_raw_uri,
                    requested_raw_uri=requested_raw_uri,
                    resource_type=resource_type,
                    raw_storage_mode=str(storage_resolution["storage_mode"]),
                    raw_storage_policy=raw_storage_policy,
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    storage_options=envelope.get("storage_options", {}),
                    wait=resource_import_wait,
                    async_default_reason=async_default_reason,
                    progress={"stage": "queued", "percent": 0},
                    updated_at_ms=envelope["ingestion_time_ms"],
                    extra={"created_at_ms": envelope["ingestion_time_ms"]},
                )
            )
        if not resource_import_wait:
            background_args = {
                **args,
                "wait": True,
                "_background_resource_import": True,
                "_resource_import_task_hash": resource_import_task_hash,
            }
            try:
                queue_status = self._enqueue_resource_import(
                    args=background_args,
                    hook=hook,
                    task_hash=resource_import_task_hash,
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
                        raw_storage_policy=raw_storage_policy,
                        node_hash=node_hash,
                        node_path=node_path,
                        scope=resource_record_scope,
                        storage_options=envelope.get("storage_options", {}),
                        progress={"stage": "failed", "percent": 100},
                        updated_at_ms=now_ms(),
                        extra={"error": str(exc)},
                    )
                )
                raise
            return {
                "status": "queued",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "resource_import_task": {
                    "task_hash": resource_import_task_hash,
                    "status": "queued",
                    "wait": False,
                    "background_started": True,
                    "raw_uri": requested_raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "worker_pool": queue_status,
                    "progress": {"stage": "queued", "percent": 0},
                    "async_default_reason": async_default_reason,
                },
                "node_materialization": node_materialization,
            }
        resource_import_task_status = "running"
        resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
        try:
            storage_resolution = resolve_raw_resource_for_ingest(
                args,
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
                )
                parsed_skill_chunks = rewrite_chunk_uris(parsed_skill.chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
                skill_hash = stable_hash(f"skill:{raw_uri}:{parsed_skill.name}:{parsed_skill.metadata.get('version', '1')}")
                skill_serving_metadata = serving_resource_metadata(parsed_skill.metadata)
                self.append(
                    {
                        "record_type": "skill_manifest",
                        "skill_hash": skill_hash,
                        "import_task_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "name": parsed_skill.name,
                        "description": parsed_skill.description,
                        "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                        "version": parsed_skill.metadata.get("version", "1"),
                        "status": parsed_skill.metadata.get("status", "active"),
                        "precedence": parsed_skill.metadata.get("precedence", "normal"),
                        "triggers": parsed_skill.metadata.get("triggers", []),
                        "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                        "examples": parsed_skill.metadata.get("examples", []),
                        "permissions": parsed_skill.metadata.get("permissions", []),
                        "inputs": parsed_skill.metadata.get("inputs", []),
                        "outputs": parsed_skill.metadata.get("outputs", []),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "text_preview": clip_context_text(parsed_skill.text),
                        "token_estimate": parsed_skill.token_estimate,
                        "metadata": skill_serving_metadata,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                skill_debug_metadata = debug_resource_metadata(parsed_skill.metadata)
                if skill_debug_metadata or parsed_skill.text:
                    self.append(
                        {
                            "record_type": "context_debug_record",
                            "debug_type": "skill_parse_detail",
                            "ref_type": "skill",
                            "ref_hash": skill_hash,
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "raw_uri": raw_uri,
                            "metadata_debug": skill_debug_metadata,
                            "text_preview": clip_context_text(parsed_skill.text),
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "skill_registry",
                        "registry_hash": stable_hash(f"skill_registry:{skill_hash}:{deployment_scope}"),
                        "skill_hash": skill_hash,
                        "import_task_hash": resource_import_task_hash,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "name": parsed_skill.name,
                        "description": parsed_skill.description,
                        "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                        "version": parsed_skill.metadata.get("version", "1"),
                        "status": parsed_skill.metadata.get("status", "active"),
                        "precedence": parsed_skill.metadata.get("precedence", "normal"),
                        "triggers": parsed_skill.metadata.get("triggers", []),
                        "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                        "examples": parsed_skill.metadata.get("examples", []),
                        "permissions": parsed_skill.metadata.get("permissions", []),
                        "inputs": parsed_skill.metadata.get("inputs", []),
                        "outputs": parsed_skill.metadata.get("outputs", []),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "skill_summary",
                        "ref_type": "skill",
                        "ref_hash": skill_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(skill_vector),
                        "model": embedding_model_name(),
                        "vector": skill_vector,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
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
        original_chunk_count = len(parsed_chunks)
        deduped_source_refs: list[str] = []
        seen_content_hashes: set[str] = set()
        unique_chunks = []
        for chunk in parsed_chunks:
            chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
            if chunk_content_hash in seen_content_hashes:
                deduped_source_refs.append(chunk.source_ref)
                continue
            seen_content_hashes.add(chunk_content_hash)
            unique_chunks.append(chunk)
        parsed_chunks = unique_chunks
        deduped_chunk_count = original_chunk_count - len(parsed_chunks)
        if not parsed_chunks:
            raise MatrixArkError("resource ingestion produced only duplicate chunks")
        resource_version_value = str(parsed_chunks[0].metadata.get("resource_version") or "")
        resource_content_hash = content_hash("\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in parsed_chunks))
        superseded_chunk_count = sum(1 for chunk in parsed_chunks if chunk.metadata.get("supersedes_chunk_hash"))
        superseded_chunk_hashes = [
            int(chunk.metadata["supersedes_chunk_hash"])
            for chunk in parsed_chunks
            if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
        ]
        parse_warnings = aggregate_parse_warnings_from_chunks(parsed_chunks)
        chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
        index_write_count = 0
        index_candidate_count = 0
        index_dropped_by_cap_count = 0
        secondary_index_budget = new_secondary_index_budget()
        resource_kind = "skill" if skill_hash is not None else "resource"
        resource_l0_text = summarize_text(
            summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
            limit=700,
        )
        resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
        resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
        self.append(
            {
                "record_type": "context_summary",
                "summary_type": f"{resource_kind}_l0",
                "summary_hash": resource_summary_hash,
                "import_task_hash": resource_import_task_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "raw_uri": raw_uri,
                "summary_text": resource_l0_text,
                "source_chunk_hashes": [chunk.chunk_hash for chunk in parsed_chunks],
                "scope": resource_record_scope,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": f"{resource_kind}_l0",
                "ref_type": "summary",
                "ref_hash": resource_summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(resource_summary_vector),
                "model": embedding_model_name(),
                "vector": resource_summary_vector,
                "scope": resource_record_scope,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        resource_dirty_hashes = self.mark_node_summary_dirty(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_ref_type=f"{resource_kind}_summary",
            source_hash_field="source_summary_hash",
            source_hash=resource_summary_hash,
            dirty_reason=f"{resource_kind}_update",
        )
        raw_resource_indexes = ordered_unique(
            [
                context_index_name("source_type", envelope["kind"]),
                context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
            ]
            + (
                [
                    context_index_name("skill_name", parsed_skill.name),
                ]
                + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                if skill_hash is not None
                else []
            )
        )
        index_candidate_count += len(raw_resource_indexes)
        resource_indexes = take_secondary_index_terms(raw_resource_indexes, secondary_index_budget)
        for index_name in resource_indexes:
            index_write_count += 1
            self.append(
                context_index_posting_record(
                    index_name=index_name,
                    capability=f"{resource_kind}_summary",
                    ref_type="summary",
                    ref_hashes=[resource_summary_hash],
                    node_hash=node_hash,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                    storage_options=envelope.get("storage_options", {}),
                )
            )
        resource_manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
        raw_uri_hash = stable_hash(raw_uri)
        if envelope["kind"] == "resource":
            manifest_hash = resource_manifest_hash
            self.append(
                {
                    "record_type": "resource_manifest",
                    "resource_hash": manifest_hash,
                    "import_task_hash": resource_import_task_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "deduped_source_refs": deduped_source_refs[:50],
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "async_parent_summary_required": bool(resource_dirty_hashes),
                    "access_scope": access_scope,
                    "deployment_scope": deployment_scope,
                    "token_estimate": sum(chunk.token_estimate for chunk in parsed_chunks),
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "resource_registry",
                    "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version_value}:{deployment_scope}"),
                    "resource_hash": manifest_hash,
                    "import_task_hash": resource_import_task_hash,
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "chunk_count": len(parsed_chunks),
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "access_scope": access_scope,
                    "deployment_scope": deployment_scope,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        for chunk, vector in zip(parsed_chunks, chunk_vectors):
            resource_chunk_hashes.append(chunk.chunk_hash)
            source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
            chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
            chunk_metadata = serving_resource_metadata(chunk_metadata_source)
            chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
            if skill_hash is not None:
                self.append(
                    resource_record_builders.skill_section_record(
                        import_task_hash=resource_import_task_hash,
                        skill_hash=skill_hash,
                        section_hash=chunk.chunk_hash,
                        node_hash=node_hash,
                        node_path=node_path,
                        raw_uri_hash=raw_uri_hash,
                        source_locator=source_locator,
                        heading=str(chunk_metadata.get("heading", "")),
                        text=chunk.text,
                        token_estimate=chunk.token_estimate,
                        metadata=chunk_metadata,
                        access_scope=access_scope,
                        deployment_scope=deployment_scope,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                )
            self.append(
                resource_record_builders.resource_chunk_record(
                    import_task_hash=resource_import_task_hash,
                    chunk_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    resource_hash=resource_manifest_hash if skill_hash is None else skill_hash,
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
                self.append(
                    resource_record_builders.resource_chunk_debug_record(
                        ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                        chunk_hash=chunk.chunk_hash,
                        import_task_hash=resource_import_task_hash,
                        node_hash=node_hash,
                        node_path=node_path,
                        resource_hash=resource_manifest_hash if skill_hash is None else skill_hash,
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
            self.append(
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
                self.append(
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
            raw_chunk_index_terms = (
                [
                    context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                    context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                ]
                + metadata_index_terms(chunk.metadata)
                + (
                    [context_index_name("skill_name", parsed_skill.name)]
                    + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                    + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                    if skill_hash is not None and parsed_skill is not None
                    else []
                )
            )
            index_candidate_count += len([term for term in raw_chunk_index_terms if term])
            chunk_index_terms = limited_index_terms(
                raw_chunk_index_terms,
                limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
            )
            index_dropped_by_cap_count += max(0, len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms))
            chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
            for index_name in chunk_index_terms:
                index_write_count += 1
                self.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "index_hash": stable_hash(f"{index_name}:{chunk.chunk_hash}"),
                        "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                        "ref_hash": chunk.chunk_hash,
                        "chunk_hash": chunk.chunk_hash,
                        "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                        "source_locator": source_locator,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
        resource_fact_records: list[Json] = []
        fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
        remaining_resource_fact_budget = max(0, MAX_RESOURCE_FACTS_PER_RESOURCE)
        for chunk in fact_chunks:
            if remaining_resource_fact_budget <= 0:
                break
            source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
            chunk_metadata = serving_resource_metadata({**chunk.metadata, "source_locator": source_locator})
            for fact_extraction in extract_resource_facts(
                chunk,
                chunk_metadata=chunk_metadata,
                envelope=envelope,
                raw_uri=raw_uri,
                resource_version=resource_version_value,
            )[:remaining_resource_fact_budget]:
                remaining_resource_fact_budget -= 1
                fact_event_type = str(fact_extraction["event_type"])
                fact_entity_type = str(fact_extraction["entity_type"])
                fact_value = str(fact_extraction.get("value", ""))
                fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version_value}")
                resource_fact_event_hashes.append(fact_event_hash)
                fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
                resource_fact_records.append(
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
                        "resource_hash": resource_manifest_hash,
                        "source_locator": source_locator,
                        "resource_version": resource_version_value,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
                resource_fact_records.append(
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
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
                entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
                resource_fact_entity_hashes.append(entity_hash)
                entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
                resource_fact_records.append(
                    {
                        "record_type": "context_entity",
                        "entity_hash": entity_hash,
                        "batch_id_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "entity_type": fact_entity_type,
                        "entity_name": entity_name,
                        "state": entity_state,
                        "confidence": fact_extraction.get("confidence", 0.78),
                        "operator": "LATEST",
                        "source_event_ids": [fact_event_hash],
                        "source_chunk_hash": chunk.chunk_hash,
                        "resource_hash": resource_manifest_hash,
                        "source_locator": source_locator,
                        "resource_version": resource_version_value,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
                resource_fact_records.append(
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
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                # Resource facts are ContextEvent/ContextEntity records with
                # source_chunk refs. The resource chunk/index rows already provide
                # secondary filtering, so avoid per-fact event index fanout here.
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
    hot_record_scope = resource_record_scope if envelope["kind"] in {"resource", "skill"} else envelope["scope"]
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
                {
                    "record_type": "context_summary",
                    "summary_type": "session_l0",
                    "summary_hash": session_summary_hash,
                    "summary_identity": "stable_per_session_node",
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "context_node_key": session_key_parts,
                    "summary_text": session_summary_text,
                    "source_event_hash": event_id_hash,
                    "scope": hot_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "session_l0",
                    "ref_type": "summary",
                    "ref_hash": session_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(session_summary_text)),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(session_summary_text),
                    "scope": hot_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(event_embedding),
                "model": embedding_model_name(),
                "vector": event_embedding,
                "scope": hot_record_scope,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        record = {
            "record_type": "context_event",
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "text": text,
            "classification": extraction.get("classification", ""),
            "event_type": extraction.get("event_type", ""),
            "entity_type": extraction.get("entity_type", ""),
            "status": extraction.get("status", "observed"),
            "source_kind": envelope.get("kind", "message"),
            "envelope": envelope,
            "internal_extraction": extraction,
            "prior_context": prior_context,
            "agent_hook": hook,
            "storage_options": envelope.get("storage_options", {}),
        }
        self.append(record)
        event_index_terms = ordered_unique(
            extraction.get("indexes")
            or [
                context_index_name("event_type", extraction.get("event_type") or infer_event_type(text)),
                context_index_name("classification", non_default_classification(extraction.get("classification"))),
                context_index_name("status", extraction.get("status") or "observed"),
                context_index_name("source_type", envelope["kind"]),
            ]
        )
        event_index_records: list[Json] = []
        for index_name in event_index_terms:
            event_index_records.append(
                {
                    "record_type": "context_index",
                    "index_name": index_name,
                    "capability": "context_event",
                    "ref_type": "event",
                    "ref_hashes": [event_id_hash],
                    "node_hash": node_hash,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
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
    return {
        "status": "accepted",
        "event_id_hash": event_id_hash,
        "node_hash": record["node_hash"],
        "storage_options": envelope.get("storage_options", {}),
        "storage_route": envelope.get("storage_route", {}),
        "hook_captured": hook is not None,
        "embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "extraction_mode": extraction["mode"],
        "classification": extraction.get("classification", "UNCLASSIFIED"),
        "prior_context": extraction.get("prior_context", ""),
        "prior_refs": extraction.get("prior_refs", []),
        "prior_message_count": extraction.get("prior_message_count", 0),
        "prior_summary_count": extraction.get("prior_summary_count", 0),
        "quality_warning": extraction.get("quality_warning", ""),
        "summary_refresh": summary_refresh,
        "resource_summary_refresh": {
            "status": "dirty_marked" if resource_dirty_hashes else "not_applicable",
            "dirty_hashes": resource_dirty_hashes,
            "refresh_result": None,
            "async_required": bool(resource_dirty_hashes),
        },
        "resource_import_task": {
            "task_hash": resource_import_task_hash,
            "status": resource_import_task_status,
            "wait": resource_import_wait,
            "metrics": resource_import_metrics,
            "raw_uri": raw_uri if resource_import_task_hash else "",
            "requested_raw_uri": requested_raw_uri if resource_import_task_hash else "",
            "raw_storage_mode": storage_resolution.get("storage_mode", "") if resource_import_task_hash else "",
            "raw_storage_policy": raw_storage_policy if resource_import_task_hash else "",
            "raw_bytes_stored": False if resource_import_task_hash else None,
            "upload_status": storage_resolution.get("upload_status", "") if resource_import_task_hash else "",
            "cloud_bucket": storage_resolution.get("cloud_bucket", "") if resource_import_task_hash else "",
            "cloud_key": storage_resolution.get("cloud_key", "") if resource_import_task_hash else "",
            "progress": {"stage": resource_import_task_status, "percent": 100 if resource_import_task_status == "completed" else 0},
        },
        "node_materialization": node_materialization,
        "resource_chunks": resource_chunk_hashes,
        "resource_chunk_count": len(resource_chunk_hashes),
        "resource_original_chunk_count": original_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_deduped_chunk_count": deduped_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_deduped_source_refs": deduped_source_refs[:20] if envelope["kind"] in {"resource", "skill"} else [],
        "resource_version": resource_version_value if envelope["kind"] in {"resource", "skill"} else "",
        "resource_content_hash": resource_content_hash if envelope["kind"] in {"resource", "skill"} else "",
        "resource_parse_warnings": parse_warnings if envelope["kind"] in {"resource", "skill"} else [],
        "resource_parse_warning_count": len(parse_warnings) if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_raw_uri": raw_uri if envelope["kind"] in {"resource", "skill"} else "",
        "resource_requested_raw_uri": requested_raw_uri if envelope["kind"] in {"resource", "skill"} else "",
        "resource_raw_storage_mode": storage_resolution.get("storage_mode", "") if envelope["kind"] in {"resource", "skill"} else "",
        "resource_raw_storage_policy": raw_storage_policy if envelope["kind"] in {"resource", "skill"} else "",
        "resource_raw_bytes_stored": False if envelope["kind"] in {"resource", "skill"} else None,
        "backend_readiness": backend_readiness or {},
        "resource_superseded_chunk_count": superseded_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_superseded_chunk_hashes": superseded_chunk_hashes if envelope["kind"] in {"resource", "skill"} else [],
        "resource_fact_events": resource_fact_event_hashes,
        "resource_fact_event_count": len(resource_fact_event_hashes),
        "resource_fact_entities": resource_fact_entity_hashes,
        "resource_fact_entity_count": len(resource_fact_entity_hashes),
        "resource_index_candidate_count": index_candidate_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_index_write_count": index_write_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_index_dropped_by_cap_count": index_dropped_by_cap_count if envelope["kind"] in {"resource", "skill"} else 0,
        "resource_index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        "resource_index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
        "skill_hash": skill_hash,
        "session_buffer": {
            "enabled": session_buffer_enabled,
            "buffer_key": list(session_buffer_key(envelope)),
            "pending_event_count": pending_event_count,
            "threshold_messages": session_buffer_threshold,
            "auto_batch_extract": auto_batch_extract,
            "boundary_commit_requested": session_boundary_commit,
        },
        "idle_commit_result": idle_commit_result,
        "auto_batch_extract_result": auto_batch_result,
    }
