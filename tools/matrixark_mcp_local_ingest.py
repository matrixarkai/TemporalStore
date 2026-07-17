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
        cleanup_temp_paths,
        context_index_name,
        context_index_posting_record,
        context_node_key,
        debug_resource_metadata,
        embedding_for_text,
        embedding_text_for_chunk,
        embeddings_for_texts,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
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
        cleanup_temp_paths,
        context_index_name,
        context_index_posting_record,
        context_node_key,
        debug_resource_metadata,
        embedding_for_text,
        embedding_text_for_chunk,
        embeddings_for_texts,
        limited_index_terms,
        metadata_index_terms,
        new_secondary_index_budget,
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

try:
    from tools import matrixark_mcp_ingest_message_records as message_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_message_records as message_record_builders

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
            return queued_response
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
        resource_kind = "skill" if skill_hash is not None else "resource"
        resource_l0_text = summarize_text(
            summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
            limit=700,
        )
        resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
        resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
        self.append(
            resource_record_builders.resource_l0_summary_record(
                resource_kind=resource_kind,
                summary_hash=resource_summary_hash,
                import_task_hash=resource_import_task_hash,
                node_hash=node_hash,
                node_path=node_path,
                raw_uri=raw_uri,
                summary_text=resource_l0_text,
                source_chunk_hashes=[chunk.chunk_hash for chunk in parsed_chunks],
                scope=resource_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
        )
        self.append(
            resource_record_builders.context_embedding_record(
                embedding_type=f"{resource_kind}_l0",
                ref_type="summary",
                ref_hash=resource_summary_hash,
                node_hash=node_hash,
                node_path=node_path,
                vector=resource_summary_vector,
                scope=resource_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
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
                    resource_record_builders.resource_chunk_index_record(
                        index_name=index_name,
                        ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                        chunk_hash=chunk.chunk_hash,
                        resource_hash=resource_manifest_hash if skill_hash is None else skill_hash,
                        source_locator=source_locator,
                        node_hash=node_hash,
                        node_path=node_path,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                )
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
