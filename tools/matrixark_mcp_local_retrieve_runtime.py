#!/usr/bin/env python3
"""Local MatrixArk retrieval runtime."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_TIME_WEIGHT,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_local_context_refs,
        compact_refs_for_audit,
        cosine,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        hybrid_origin_score,
        integer_arg,
        local_context_refs_for_pack,
        normalize_storage_options,
        now_ms,
        optional_object,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        require_string,
        scope_matches,
        score_recall_candidate,
        select_token_budgeted_refs,
        selected_context_class_counts,
        source_ref_from_locator,
        sparse_lexical_score,
        stable_hash,
        summarize_text,
        tokens,
        tree_first_traversal,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_TIME_WEIGHT,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_local_context_refs,
        compact_refs_for_audit,
        cosine,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        hybrid_origin_score,
        integer_arg,
        local_context_refs_for_pack,
        normalize_storage_options,
        now_ms,
        optional_object,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        require_string,
        scope_matches,
        score_recall_candidate,
        select_token_budgeted_refs,
        selected_context_class_counts,
        source_ref_from_locator,
        sparse_lexical_score,
        stable_hash,
        summarize_text,
        tokens,
        tree_first_traversal,
    )

try:
    from tools import matrixark_mcp_retrieve_planning as retrieve_planning_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_planning as retrieve_planning_helpers

try:
    from tools import matrixark_mcp_retrieve_cache as retrieve_cache_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_cache as retrieve_cache_helpers

try:
    from tools import matrixark_mcp_native_retrieve as native_retrieve_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_native_retrieve as native_retrieve_helpers

try:
    from tools import matrixark_mcp_retrieve_continuity as retrieve_continuity_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_continuity as retrieve_continuity_helpers

try:
    from tools import matrixark_mcp_retrieve_deadline as retrieve_deadline_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_deadline as retrieve_deadline_helpers

try:
    from tools import matrixark_mcp_retrieve_resources as retrieve_resource_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_resources as retrieve_resource_helpers

try:
    from tools import matrixark_mcp_retrieve_identity as retrieve_identity_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_identity as retrieve_identity_helpers

try:
    from tools import matrixark_mcp_retrieve_temporal_window as retrieve_temporal_window_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_temporal_window as retrieve_temporal_window_helpers

try:
    from tools import matrixark_mcp_retrieve_tree_filter as retrieve_tree_filter_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_tree_filter as retrieve_tree_filter_helpers

try:
    from tools import matrixark_mcp_retrieval_records as retrieval_record_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieval_records as retrieval_record_helpers

try:
    from tools import matrixark_mcp_visibility as visibility_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_visibility as visibility_helpers

try:
    from tools import matrixark_mcp_time_compression_runtime as time_compression_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_time_compression_runtime as time_compression_runtime

try:
    from tools import matrixark_mcp_summary_runtime as summary_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_summary_runtime as summary_runtime

try:
    from tools.matrixark_mcp_retrieve_metrics import attach_python_retrieval_metrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_metrics import attach_python_retrieval_metrics

try:
    from tools.matrixark_mcp_retrieve_fallback import deadline_fallback_pack
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_fallback import deadline_fallback_pack

try:
    from tools.matrixark_mcp_retrieve_embeddings import add_context_embedding_vector
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_embeddings import add_context_embedding_vector

try:
    from tools.matrixark_mcp_retrieve_index_terms import add_context_index_terms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_index_terms import add_context_index_terms

try:
    from tools.matrixark_mcp_retrieve_node_scores import (
        add_context_summary_text,
        add_node_embedding_score,
        add_secondary_index_hint_node_scores,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_node_scores import (
        add_context_summary_text,
        add_node_embedding_score,
        add_secondary_index_hint_node_scores,
    )

try:
    from tools.matrixark_mcp_retrieve_pack_policy import (
        build_rerank_policy,
        build_time_weighted_recall,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_policy import (
        build_rerank_policy,
        build_time_weighted_recall,
    )

def retrieve(target: Any, args: Json) -> Json:
    self = target
    started_perf = time.perf_counter()
    query = require_string(args, "query")
    scope = optional_object(args, "scope")
    storage_options = normalize_storage_options(args)
    ranking = optional_object(args, "ranking")
    audit_mode, audit_sample_rate = retrieve_planning_helpers.retrieval_audit_policy(args)
    deadline_ms = retrieve_planning_helpers.retrieval_deadline_ms(args, ranking)

    stage_budgets_ms, explicit_stage_budgets = retrieve_planning_helpers.retrieval_stage_budgets(
        args,
        ranking,
        deadline_ms=deadline_ms,
    )
    deadline_tracker = retrieve_deadline_helpers.RetrievalDeadlineTracker(
        started_perf=started_perf,
        deadline_ms=deadline_ms,
        stage_budgets_ms=stage_budgets_ms,
        explicit_stage_budgets=explicit_stage_budgets,
        observe_latency=self._observe_model_latency,
    )
    stage_latencies_ms = deadline_tracker.stage_latencies_ms
    stage_started_perf = time.perf_counter()

    def deadline_exceeded() -> bool:
        return deadline_tracker.deadline_exceeded()

    def finish_retrieval_stage(stage: str, started: float) -> float:
        return deadline_tracker.finish_stage(stage, started)

    def stage_budget_snapshot() -> Json:
        return deadline_tracker.stage_budget_snapshot()

    retrieval_plan = retrieve_planning_helpers.retrieval_query_budget_plan(
        args,
        ranking,
        query=query,
        scope=scope,
        default_max_context_tokens=DEFAULT_MAX_CONTEXT_TOKENS,
    )
    question_type = str(retrieval_plan["question_type"])
    retrieval_session_scope = str(retrieval_plan["retrieval_session_scope"])
    retrieval_scope = retrieval_plan["retrieval_scope"]
    secondary_index_filter_groups = retrieval_plan["secondary_index_filter_groups"]
    secondary_index_filter_mode = str(retrieval_plan["secondary_index_filter_mode"])
    secondary_index_dropped_count = 0
    secondary_index_matched_count = 0
    budget_source = str(retrieval_plan["budget_source"])
    max_context_tokens = int(retrieval_plan["max_context_tokens"])
    local_budget = retrieval_plan["local_budget"]
    local_tokens = int(local_budget.get("token_estimate", 0))
    safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
    remote_context_budget_tokens = int(retrieval_plan["remote_context_budget_tokens"])
    cross_session_policy = retrieval_plan["cross_session_policy"]
    shared_context_policy = retrieval_plan["shared_context_policy"]
    query_terms = retrieval_plan["query_terms"]
    reference_time_ms = int(retrieval_plan["reference_time_ms"])
    query_plan = retrieval_plan["query_plan"]
    debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
    pack_cache_key = retrieve_cache_helpers.context_pack_cache_key(
        self,
        scope=scope,
        query=query,
        question_type=question_type,
        retrieval_session_scope=retrieval_session_scope,
        max_context_tokens=max_context_tokens,
        local_budget=local_budget,
        ranking=ranking,
        include_superseded=bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
    )
    cached_pack = retrieve_cache_helpers.get_cached_context_pack(self, pack_cache_key, include_debug=debug_refs)
    if cached_pack is not None:
        return cached_pack
    auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
    annotate_session_continuity = retrieve_continuity_helpers.make_session_continuity_annotator(
        retrieval_scope=retrieval_scope,
        question_type=question_type,
    )

    finish_retrieval_stage("query_understanding", stage_started_perf)
    native_pack = native_retrieve_helpers.try_native_context_pack(
        self,
        args=args,
        query=query,
        scope=scope,
        retrieval_scope=retrieval_scope,
        question_type=question_type,
        query_plan=query_plan,
        secondary_index_filter_groups=secondary_index_filter_groups,
        secondary_index_filter_mode=secondary_index_filter_mode,
        max_context_tokens=max_context_tokens,
        local_budget=local_budget,
        cross_session_policy=cross_session_policy,
        shared_context_policy=shared_context_policy,
        ranking=ranking,
        deadline_ms=deadline_ms,
        reference_time_ms=reference_time_ms,
        audit_mode=audit_mode,
        audit_sample_rate=audit_sample_rate,
        storage_options=storage_options,
        debug_refs=debug_refs,
        stage_budget_snapshot=stage_budget_snapshot,
    )
    if native_pack is not None:
        return native_pack
    if self.native_context_pack_required():
        raise MatrixArkError(
            "backend-native ContextPack assembly is required for TemporalStore serving, "
            "but this backend did not return matrixark_retrieve_context_pack. "
            "Python reference packing is disabled unless explicitly overridden for local debug."
        )
    embedding_started_perf = time.perf_counter()
    query_embedding = embedding_for_text(query)
    self._observe_model_latency("query_embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
    stage_started_perf = time.perf_counter()
    retrieval_record_result = self.retrieval_records(
        scope=retrieval_scope,
        secondary_index_groups=secondary_index_filter_groups,
    )
    records = retrieval_record_result["records"]
    retrieval_scan_stats = retrieval_record_result.get("scan_stats", {})

    def deadline_fallback(reason: str, fallback_records: list[Json] | None = None) -> Json:
        return deadline_fallback_pack(
            self,
            query=query,
            scope=scope,
            question_type=question_type,
            max_context_tokens=max_context_tokens,
            local_budget=local_budget,
            deadline_ms=deadline_ms,
            started_perf=started_perf,
            records=records if fallback_records is None else fallback_records,
            reason=reason,
            budget_source=budget_source,
        )
    skill_controls = self.latest_skill_controls(records)
    include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
    latest_resource_version_by_hash, resource_uri_by_hash = retrieve_resource_helpers.latest_resource_metadata(
        records,
        scope,
    )
    finish_retrieval_stage("candidate_fetch", stage_started_perf)
    stage_started_perf = time.perf_counter()
    if deadline_exceeded():
        return deadline_fallback("deadline_after_record_load")
    node_scores: dict[int, Json] = {}
    event_embedding_vectors: dict[int, list[float]] = {}
    entity_embedding_vectors: dict[int, list[float]] = {}
    segment_embedding_vectors: dict[int, list[float]] = {}
    compression_embedding_vectors: dict[int, list[float]] = {}
    resource_embedding_vectors: dict[int, list[float]] = {}
    skill_embedding_vectors: dict[int, list[float]] = {}
    index_terms_by_batch: dict[Any, list[str]] = {}
    index_terms_by_node: dict[Any, list[str]] = {}
    index_terms_by_ref: dict[Any, list[str]] = {}
    index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
    node_summary_text_by_hash: dict[int, str] = {}
    for scan_index, record in enumerate(records, 1):
        if scan_index % 128 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_embedding_index_scan")
        record_type = record.get("record_type")
        if record_type == "context_index" and scope_matches(candidate_access_scope(record), retrieval_scope):
            add_context_index_terms(
                record,
                index_terms_by_batch=index_terms_by_batch,
                index_terms_by_node=index_terms_by_node,
                index_terms_by_ref=index_terms_by_ref,
                index_terms_by_node_for_prefilter=index_terms_by_node_for_prefilter,
            )
        add_context_summary_text(record, scope=scope, node_summary_text_by_hash=node_summary_text_by_hash)
    secondary_index_prefilter_node_hashes = {
        node_hash
        for node_hash, terms in index_terms_by_node_for_prefilter.items()
        if passes_secondary_index_filters(set(terms), secondary_index_filter_groups, mode=secondary_index_filter_mode)
    } if secondary_index_filter_groups else set()
    query_plan["secondary_index_prefilter"] = {
        "applied_before_l0_l1_traversal": True,
        "matched_node_count": len(secondary_index_prefilter_node_hashes),
        "fallback_when_no_index_matches": True,
        "strategy": "ContextIndex node hints boost L0/L1 traversal; leaf candidates still verify filters before embedding scoring",
    }
    for scan_index, record in enumerate(records, 1):
        if scan_index % 128 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_embedding_vector_scan")
        record_type = record.get("record_type")
        if record_type == "context_embedding" and not scope_matches(candidate_access_scope(record), scope):
            continue
        if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
            add_node_embedding_score(
                record,
                query_embedding=query_embedding,
                query_terms=query_terms,
                node_summary_text_by_hash=node_summary_text_by_hash,
                secondary_index_prefilter_node_hashes=secondary_index_prefilter_node_hashes,
                node_scores=node_scores,
            )
        elif record_type == "context_embedding":
            add_context_embedding_vector(
                record,
                event_embedding_vectors=event_embedding_vectors,
                entity_embedding_vectors=entity_embedding_vectors,
                segment_embedding_vectors=segment_embedding_vectors,
                compression_embedding_vectors=compression_embedding_vectors,
                resource_embedding_vectors=resource_embedding_vectors,
                skill_embedding_vectors=skill_embedding_vectors,
            )
    add_secondary_index_hint_node_scores(
        records,
        secondary_index_prefilter_node_hashes=secondary_index_prefilter_node_hashes,
        node_scores=node_scores,
    )
    if deadline_exceeded():
        return deadline_fallback("deadline_after_embedding_index_scan")

    ranking_limits = retrieve_planning_helpers.retrieval_ranking_limits(ranking)
    top_k_per_layer = ranking_limits.top_k_per_layer
    max_children_scored_per_parent = ranking_limits.max_children_scored_per_parent
    hard_max_children_scored_per_parent = ranking_limits.hard_max_children_scored_per_parent
    max_candidates_per_node = ranking_limits.max_candidates_per_node
    max_selected_refs = ranking_limits.max_selected_refs
    max_global_candidates = ranking_limits.max_global_candidates
    min_similarity_score = ranking_limits.min_similarity_score
    budget_fill_policy = ranking_limits.budget_fill_policy
    max_raw_events_per_node = ranking_limits.max_raw_events_per_node
    traversal = tree_first_traversal(
        node_scores,
        top_k_per_layer=top_k_per_layer,
        max_children_scored_per_parent=max_children_scored_per_parent,
    )
    finish_retrieval_stage("node_traversal", stage_started_perf)
    stage_started_perf = time.perf_counter()
    selected_paths = traversal["selected_paths"]
    selected_leaf_paths = traversal["leaf_paths"]
    selected_node_hashes = traversal["selected_node_hashes"]

    placement_record_result: Json = {}
    placement_candidate_records: list[Json] = []
    if selected_node_hashes and not traversal.get("fallback_to_flat"):
        placement_record_result = self.retrieval_records(
            scope=scope,
            secondary_index_groups=secondary_index_filter_groups,
            selected_node_hashes=selected_node_hashes,
            allow_broad_scan_fallback=False,
        )
        placement_candidate_records = placement_record_result.get("records", [])

        retrieve_identity_helpers.append_unique_records(records, placement_candidate_records)

        for record in placement_candidate_records:
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(candidate_access_scope(record), scope):
                add_context_index_terms(
                    record,
                    index_terms_by_batch=index_terms_by_batch,
                    index_terms_by_node=index_terms_by_node,
                    index_terms_by_ref=index_terms_by_ref,
                    index_terms_by_node_for_prefilter=index_terms_by_node_for_prefilter,
                )
            elif record_type == "context_embedding" and scope_matches(candidate_access_scope(record), scope):
                add_context_embedding_vector(
                    record,
                    event_embedding_vectors=event_embedding_vectors,
                    entity_embedding_vectors=entity_embedding_vectors,
                    segment_embedding_vectors=segment_embedding_vectors,
                    compression_embedding_vectors=compression_embedding_vectors,
                    resource_embedding_vectors=resource_embedding_vectors,
                    skill_embedding_vectors=skill_embedding_vectors,
                )

    selected_by_tree = retrieve_tree_filter_helpers.make_tree_selector(
        traversal=traversal,
        selected_paths=selected_paths,
        selected_leaf_paths=selected_leaf_paths,
        selected_node_hashes=selected_node_hashes,
    )
    if placement_candidate_records and not traversal.get("fallback_to_flat"):
        tree_candidate_records = [record for record in placement_candidate_records if selected_by_tree(record)]
        tree_prefilter_dropped_count = max(0, len(placement_candidate_records) - len(tree_candidate_records))
        retrieval_scan_stats = {
            **retrieval_scan_stats,
            "leaf_fetch": placement_record_result.get("scan_stats", {}),
            "leaf_fetch_record_count": len(placement_candidate_records),
            "leaf_fetch_strategy": "selected_node_placement",
        }
    else:
        tree_candidate_records = records if traversal.get("fallback_to_flat") else [record for record in records if selected_by_tree(record)]
        tree_prefilter_dropped_count = 0 if traversal.get("fallback_to_flat") else max(0, len(records) - len(tree_candidate_records))
    for scan_index, record in enumerate(tree_candidate_records, 1):
        if scan_index % 128 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_tree_candidate_prefilter", records)
    raw_event_ids_by_node, raw_event_time_window_dropped_count = retrieve_temporal_window_helpers.raw_event_admission_window(
        tree_candidate_records,
        max_raw_events_per_node=max_raw_events_per_node,
        context_event_ingestion_time_ms=self.context_event_ingestion_time_ms,
    )
    admit_candidate_for_node, fanout_limiter = retrieve_tree_filter_helpers.make_candidate_admitter(max_candidates_per_node)

    layer_scores = sorted(
        traversal["trace"] or node_scores.values(),
        key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
    )
    primary_matches = []
    auxiliary_matches = []
    if question_type == "broad_exploration":
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_summary_scan", records)
            if record.get("record_type") != "context_summary":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            summary_type = str(record.get("summary_type") or "")
            if summary_type not in {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0"}:
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = str(record.get("summary_text", ""))
            if not text:
                continue
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
            node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
            origin_score = min(1.0, 0.06 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            if origin_score <= 0:
                continue
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({
                        "ref_type": "summary",
                        "ref_hash": record.get("summary_hash") or record.get("node_hash"),
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(index_terms),
                        "selection_reason": "selected by tree path and L0/L1 summary relevance",
                        "event_type": summary_type,
                        "context_class": "summary",
                        "summary_type": summary_type,
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_summary",
                    }, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_event_scan", records)
        if record.get("record_type") != "context_event":
            continue
        event_node_key: Any = record.get("node_hash")
        if event_node_key is None:
            event_node_key = tuple(record.get("node_path", []))
        if (
            not record.get("source_chunk_hash")
            and event_node_key in raw_event_ids_by_node
            and int(record.get("event_id_hash") or 0) not in raw_event_ids_by_node[event_node_key]
        ):
            continue
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        record_scope = candidate_access_scope(record)
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        text = str(record.get("text", ""))
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
        event_type = str(record.get("event_type") or record.get("classification") or "")
        candidate_metadata: Json = {}
        record_metadata = record.get("metadata")
        envelope_metadata = envelope.get("metadata")
        if isinstance(record_metadata, dict):
            candidate_metadata.update(record_metadata)
        if isinstance(envelope_metadata, dict):
            candidate_metadata.update(envelope_metadata)
        candidate = {
            "ref_type": "event",
            "ref_hash": record["event_id_hash"],
            "node_hash": record["node_hash"],
            "node_path": record.get("node_path", []),
            "origin_score": origin_score,
            "keyword_score": keyword_score,
            "sparse_score": sparse_score,
            "embedding_score": embedding_score,
            "node_score": node_score,
            "matched_index_terms": sorted(index_terms),
            "selection_reason": (
                "selected by tree path, secondary indexes, and resource fact/event hybrid score"
                if record.get("source_chunk_hash")
                else "selected by tree path, secondary indexes, and event hybrid score"
            ),
            "event_type": event_type,
            "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
            "source_chunk_hash": record.get("source_chunk_hash"),
            "source_ref": record.get("source_ref", ""),
            "metadata": candidate_metadata,
            "scope": record_scope,
            "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
            "text": clip_context_text(text),
        }
        if origin_score > 0:
            primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
        graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
        graph_score = sparse_lexical_score(query_terms, graph_text)
        if graph_score > 0:
            auxiliary_matches.append(
                score_recall_candidate(
                    {
                        **annotate_session_continuity(candidate, record),
                        "recall_path": "auxiliary_keyword_graph",
                        "origin_score": graph_score,
                        "keyword_graph_score": graph_score,
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    if deadline_exceeded():
        return deadline_fallback("deadline_after_event_scan")
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_entity_scan", records)
        if record.get("record_type") != "context_entity":
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        origin_score = min(1.0, 0.12 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        candidate = {
            "ref_type": "entity",
            "ref_hash": record["entity_hash"],
            "node_hash": record["node_hash"],
            "node_path": record.get("node_path", []),
            "origin_score": origin_score,
            "keyword_score": keyword_score,
            "sparse_score": sparse_score,
            "embedding_score": embedding_score,
            "node_score": node_score,
            "matched_index_terms": sorted(index_terms),
            "selection_reason": (
                "selected by tree path, secondary indexes, and resource entity state score"
                if record.get("source_chunk_hash")
                else "selected by tree path, secondary indexes, and entity state score"
            ),
            "entity_type": record.get("entity_type", ""),
            "entity_name": record.get("entity_name", ""),
            "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
            "source_chunk_hash": record.get("source_chunk_hash"),
            "source_ref": record.get("source_ref", ""),
            "metadata": record.get("metadata", {}),
            "scope": candidate_access_scope(record),
            "updated_at_ms": record.get("updated_at_ms", now_ms()),
            "text": clip_context_text(text),
        }
        if origin_score > 0:
            primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
        graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
        if graph_score > 0:
            auxiliary_matches.append(
                score_recall_candidate(
                    {
                        **annotate_session_continuity(candidate, record),
                        "recall_path": "auxiliary_keyword_graph",
                        "origin_score": graph_score,
                        "keyword_graph_score": graph_score,
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    if deadline_exceeded():
        return deadline_fallback("deadline_after_entity_scan")
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_segment_scan", records)
        if record.get("record_type") != "context_segment":
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        saliency_score = float(record.get("saliency_score", 0.0))
        origin_score = min(
            1.0,
            0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
        )
        candidate = {
            "ref_type": "segment",
            "ref_hash": record["segment_hash"],
            "node_hash": record["node_hash"],
            "node_path": record.get("node_path", []),
            "origin_score": origin_score,
            "keyword_score": keyword_score,
            "sparse_score": sparse_score,
            "embedding_score": embedding_score,
            "node_score": node_score,
            "matched_index_terms": sorted(index_terms),
            "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
            "saliency_score": saliency_score,
            "topic": record.get("topic", ""),
            "coordinate_tuples": record.get("coordinate_tuples", []),
            "non_contiguous": record.get("non_contiguous", False),
            "scope": candidate_access_scope(record),
            "updated_at_ms": record.get("updated_at_ms", now_ms()),
            "text": clip_context_text(str(record.get("summary_text", ""))),
        }
        if origin_score > 0:
            primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
        graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
        if graph_score > 0:
            auxiliary_matches.append(
                score_recall_candidate(
                    {
                        **annotate_session_continuity(candidate, record),
                        "recall_path": "auxiliary_keyword_graph",
                        "origin_score": graph_score,
                        "keyword_graph_score": graph_score,
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    if deadline_exceeded():
        return deadline_fallback("deadline_after_segment_scan")
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_resource_skill_scan", records)
        if record.get("record_type") not in {"resource_chunk", "skill_section"}:
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        if record.get("record_type") == "skill_section":
            ref_type = "skill_section"
            ref_hash = int(record.get("section_hash") or 0)
            parent_skill_hash = int(record.get("skill_hash") or 0)
            control = skill_controls.get(parent_skill_hash, {})
            if str(control.get("status") or "active") != "active":
                continue
            resource_hash = parent_skill_hash
            raw_uri_value = str(record.get("raw_uri") or "")
            source_locator = str(record.get("source_locator") or "")
            citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
            resource_version_value = str(record.get("metadata", {}).get("resource_version") or record.get("resource_version") or "")
            version_state = "current"
            is_superseded_version = False
            text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
            embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
            business_type = "skill"
            metadata = {**record.get("metadata", {}), "skill_registry": control}
        else:
            ref_type = "resource_chunk"
            ref_hash = int(record.get("chunk_hash") or 0)
            metadata = record.get("metadata", {})
            resource_hash = int(record.get("resource_hash") or 0)
            raw_uri_value = str(record.get("raw_uri") or resource_uri_by_hash.get(resource_hash, ""))
            source_locator = str(record.get("source_locator") or metadata.get("source_locator") or "")
            citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
            resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
            latest_version = latest_resource_version_by_hash.get(resource_hash, resource_version_value)
            is_superseded_version = bool(
                resource_version_value
                and latest_version
                and resource_version_value != latest_version
            )
            if is_superseded_version and not include_superseded_resources:
                secondary_index_dropped_count += 1
                continue
            version_state = "historical" if is_superseded_version else "current"
            text = f"resource {source_locator}: {record.get('text', '')}"
            embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
            business_type = str(record.get("resource_type") or "resource")
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
        origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        if origin_score <= 0:
            continue
        primary_matches.append(
            score_recall_candidate(
                annotate_session_continuity({
                    "ref_type": ref_type,
                    "ref_hash": ref_hash,
                    "node_hash": record.get("node_hash"),
                    "node_path": record.get("node_path", []),
                    "origin_score": origin_score,
                    "keyword_score": keyword_score,
                    "sparse_score": sparse_score,
                    "embedding_score": embedding_score,
                    "node_score": node_score,
                    "matched_index_terms": sorted(index_terms),
                    "selection_reason": (
                        "selected by tree path, secondary indexes, and resource/skill hybrid score"
                        if index_terms
                        else "selected by tree path and resource/skill hybrid score"
                    ),
                    "event_type": business_type,
                    "context_class": ref_type,
                    "resource_hash": resource_hash,
                    "source_locator": source_locator,
                    "resource_type": record.get("resource_type", ""),
                    "resource_version": resource_version_value,
                    "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                    "version_state": version_state,
                    "stale_or_superseded": is_superseded_version,
                    "access_decision": "allowed_by_registry_scope_before_scoring",
                    "access_scope": candidate_access_scope(record),
                    "deployment_scope": record.get("deployment_scope", "local"),
                    "citation": citation,
                    "metadata": metadata,
                    "scope": candidate_access_scope(record),
                    "updated_at_ms": record.get("updated_at_ms", now_ms()),
                    "text": clip_context_text(text),
                    "recall_path": "primary_resource_skill",
                }, record),
                ranking,
                reference_time_ms=reference_time_ms,
            )
        )

    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return deadline_fallback("deadline_during_compression_scan", records)
        if record.get("record_type") != "context_compression_event":
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        if not admit_candidate_for_node(record):
            continue
        text = f"TIME_COMPRESS: {summarize_text(str(record.get('summary_text', '')), limit=96)}"
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        compression_hash = int(record.get("compression_id_hash") or 0)
        embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        candidate = {
            "ref_type": "compression",
            "ref_hash": compression_hash,
            "node_hash": record["node_hash"],
            "node_path": record.get("node_path", []),
            "origin_score": origin_score,
            "keyword_score": keyword_score,
            "sparse_score": sparse_score,
            "embedding_score": embedding_score,
            "node_score": node_score,
            "event_type": "time_compress",
            "operator": "TIME_COMPRESS",
            "source_event_ids": record.get("source_event_ids", []),
            "source_start_ms": record.get("source_start_ms"),
            "source_end_ms": record.get("source_end_ms"),
            "scope": candidate_access_scope(record),
            "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
            "text": clip_context_text(text),
        }
        if origin_score > 0:
            primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_time_compression"}, record), ranking, reference_time_ms=reference_time_ms))
        graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
        if graph_score > 0:
            auxiliary_matches.append(
                score_recall_candidate(
                    {
                        **annotate_session_continuity(candidate, record),
                        "recall_path": "auxiliary_keyword_graph",
                        "origin_score": graph_score,
                        "keyword_graph_score": graph_score,
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    if deadline_exceeded():
        return deadline_fallback("deadline_after_compression_scan")
    finish_retrieval_stage("rerank_score", stage_started_perf)
    stage_started_perf = time.perf_counter()
    primary_matches.sort(key=lambda item: item["score"], reverse=True)
    auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
    selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
    rerank_candidate_limit = max(selected_ref_cap, max_global_candidates)
    first_stage_candidate_count = len(primary_matches) + len(auxiliary_matches)
    rerank_policy = build_rerank_policy(
        first_stage_candidate_count=first_stage_candidate_count,
        rerank_candidate_limit=rerank_candidate_limit,
        question_type=question_type,
        min_similarity_score=min_similarity_score,
        budget_fill_policy=budget_fill_policy,
    )
    selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
        primary_matches,
        auxiliary_matches,
        max_context_tokens=remote_context_budget_tokens,
        auxiliary_quota=auxiliary_quota,
        question_type=question_type,
        reserved_tokens=0,
        max_selected_refs=max_selected_refs,
        min_score=min_similarity_score,
        max_global_candidates=max_global_candidates,
        budget_fill_policy=budget_fill_policy,
        duplicate_text_hashes=local_budget["text_hashes"],
        deadline_exceeded=deadline_exceeded,
        deadline_reason="deadline_during_context_pack",
        cross_session_policy=cross_session_policy,
        shared_context_policy=shared_context_policy,
    )
    partial_context_pack = bool(dropped_over_budget.get("deadline_exceeded"))
    quality_warnings = []
    if partial_context_pack:
        quality_warnings.append(f"retrieval_deadline_exceeded:{dropped_over_budget.get('deadline_reason', 'deadline_during_context_pack')}")
    context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
    context_pack_id_text = str(context_pack_id)
    recall_reinforcement_enabled = bool(ranking.get("recall_reinforcement", True))
    if recall_reinforcement_enabled:
        reinforcement = self.append_recall_reinforcement_markers(
            context_pack_id=context_pack_id_text,
            selected_refs=selected,
            reinforced_at_ms=now_ms(),
        )
    else:
        reinforcement = {
            "reinforced_event_count": 0,
            "protect_ms": 0,
            "protected_until_ms": 0,
            "skipped": True,
            "reason": "disabled_for_read_only_scale_or_benchmark_run",
        }
    debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
    serving_selected = compact_context_pack_refs(selected, include_debug=debug_refs)
    serving_dropped = compact_dropped_refs_for_context_pack(dropped_over_budget, include_debug=debug_refs)
    pack_summary = summarize_text(
        " ".join(str(item.get("text", "")) for item in selected),
        limit=512,
    )
    selected_context_counts = selected_context_class_counts(selected)
    time_weighted_recall = build_time_weighted_recall(
        ranking=ranking,
        selected=selected,
        reference_time_ms=reference_time_ms,
    )
    pack = {
        "context_pack_id": str(context_pack_id),
        "context_sources_order": ["local_context", "matrixark_remote_context"],
        "local_context_refs": local_context_refs_for_pack(local_budget),
        "selected_refs": serving_selected,
        "remote_context_refs": serving_selected,
        "selected_ref_counts": selected_context_counts,
        "context_assembly_policy": {
            "access_scope_before_scoring": True,
            "skill_selection": "skill_section_only",
            "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
            "recall_reinforcement": "selected event refs and compression source ids receive protection markers before raw-event pruning",
        },
        "layer_scores": layer_scores[:24],
        "question_type": question_type,
        "packing_policy": f"question_type_aware:{question_type}",
        "query_embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "recall_policy": {
            "query_plan": query_plan,
            "session_continuity": {
                "mode": retrieval_session_scope,
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
            },
            "cross_session": dropped_over_budget.get("cross_session_policy", cross_session_policy),
            "shared_context": dropped_over_budget.get("shared_context_policy", shared_context_policy),
            "backend_retrieval_pushdown": retrieval_scan_stats,
            "ranking": {
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_selected_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first",
            },
            "tree_traversal": {
                "enabled": True,
                "summary_embeddings": ["node_l0", "node_l1"],
                "top_k_per_layer": top_k_per_layer,
                "max_children_scored_per_parent": max_children_scored_per_parent,
                "hard_max_children_scored_per_parent": hard_max_children_scored_per_parent,
                "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers",
                "max_candidates_per_node": max_candidates_per_node,
                "max_raw_events_per_node": max_raw_events_per_node,
                "max_selected_refs": max_selected_refs,
                "selected_node_count": len(selected_node_hashes),
                "selected_path_count": len(selected_paths),
                "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                "candidate_records_after_tree": len(tree_candidate_records),
                "records_dropped_by_tree": tree_prefilter_dropped_count,
                "records_dropped_by_node_fanout": fanout_limiter.dropped_count,
                "raw_events_dropped_by_time_window": raw_event_time_window_dropped_count,
                "cold_events_represented_by_compression": raw_event_time_window_dropped_count > 0,
                "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders",
                "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
            },
            "secondary_index_filter": {
                "enabled": bool(secondary_index_filter_groups),
                "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                "matched_candidate_count": secondary_index_matched_count,
                "dropped_candidate_count": secondary_index_dropped_count,
                "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                "effective_mode": secondary_index_filter_mode,
                "applied_before_embedding_scoring": True,
                "fanout_cap_applied_before_embedding_scoring": True,
            },
            "rerank": rerank_policy,
            "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
            "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
            "time_decay": {
                "freshness_tolerance_ms": time_weighted_recall["freshness_tolerance_ms"],
                "half_life_ms": time_weighted_recall["half_life_ms"],
            },
            "time_weighted_recall": time_weighted_recall,
            "recall_reinforcement": reinforcement,
            "weights": {
                "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
            },
            "auxiliary_quota": auxiliary_quota,
            "storage_options": storage_options,
            "hard_deadline": {
                "deadline_ms": deadline_ms,
                "elapsed_ms": round((time.perf_counter() - started_perf) * 1000.0, 3),
                "partial_context_pack": partial_context_pack,
                "fallback_reason": dropped_over_budget.get("deadline_reason", "") if partial_context_pack else "",
            },
        },
        "primary_candidate_count": len(primary_matches),
        "auxiliary_candidate_count": len(auxiliary_matches),
        "used_context_tokens": used_context_tokens,
        "used_remote_context_tokens": used_context_tokens,
        "used_local_context_tokens": local_tokens,
        "total_prompt_context_tokens": used_context_tokens + local_tokens,
        "remote_context_budget_tokens": remote_context_budget_tokens,
        "requested_max_context_tokens": max_context_tokens,
        "local_context_safety_margin_tokens": safety_margin_tokens,
        "budget_source": budget_source,
        "local_context_policy": {
            "mode": "shared_budget_dedupe",
            "local_context_count": len(local_budget["items"]),
            "local_context_tokens": local_tokens,
            "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
            "safety_margin_tokens": safety_margin_tokens,
            "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
            "dedupe_remote_against_local": True,
            "remote_is_additive_only_within_remaining_budget": True,
        },
        "dropped_refs": serving_dropped,
        "quality_warnings": quality_warnings,
        "insufficient_context": not selected,
        "partial_context_pack": partial_context_pack,
        "context_pack_payload_policy": {
            "serving_refs": "compact" if not debug_refs else "debug_full",
            "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
            "dropped_ref_details": "audit_only" if not debug_refs else "included",
            "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
        },
        "operational_visibility_policy": {
            "audit_mode": audit_mode,
            "audit_sample_rate": audit_sample_rate,
            "telemetry_record": audit_mode != "off",
            "rich_replay_audit": audit_mode == "full" and audit_sample_rate > 0,
            "rich_replay_audit_force_on_partial_or_warning": True,
        },
    }
    finish_retrieval_stage("pack", stage_started_perf)
    pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
    over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
    if over_budget_stages:
        quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
        pack["quality_warnings"] = quality_warnings
    audit_started_perf = time.perf_counter()
    audit_record = {
        "record_type": "context_pack_audit",
        "context_pack_id": context_pack_id_text,
        "query": query,
        "scope": scope,
        "summary_text": pack_summary,
        "selected_refs": compact_refs_for_audit(selected),
        "local_context_refs": compact_local_context_refs(local_budget),
        "context_sources_order": pack["context_sources_order"],
        "selected_ref_counts": selected_context_counts,
        "context_assembly_policy": pack["context_assembly_policy"],
        "dropped_refs": dropped_over_budget,
        "quality_warnings": quality_warnings,
        "partial_context_pack": partial_context_pack,
        "layer_scores": layer_scores[:24],
        "tree_traversal": pack["recall_policy"]["tree_traversal"],
        "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
        "question_type": question_type,
        "packing_policy": pack["packing_policy"],
        "rerank_policy": rerank_policy,
        "recall_policy": pack["recall_policy"],
        "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
        "storage_options": storage_options,
        "local_context_policy": pack["local_context_policy"],
        "used_local_context_tokens": pack["used_local_context_tokens"],
        "used_remote_context_tokens": pack["used_remote_context_tokens"],
        "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
        "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
        "requested_max_context_tokens": pack["requested_max_context_tokens"],
        "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
        "budget_source": pack["budget_source"],
        "operational_visibility_policy": pack["operational_visibility_policy"],
        "primary_candidate_count": len(primary_matches),
        "auxiliary_candidate_count": len(auxiliary_matches),
        "tree_candidate_records": len(tree_candidate_records),
        "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
        "fanout_dropped_count": fanout_limiter.dropped_count,
        "max_candidates_per_node": max_candidates_per_node,
        "max_selected_refs": max_selected_refs,
        "created_at_ms": now_ms(),
    }
    visibility_decision = self.append_context_pack_visibility(
        pack=pack,
        audit_record=audit_record,
        query=query,
        scope=scope,
        audit_mode=audit_mode,
        audit_sample_rate=audit_sample_rate,
    )
    pack["operational_visibility_policy"] = visibility_decision
    retrieve_cache_helpers.put_cached_context_pack(self, pack_cache_key, pack)
    finish_retrieval_stage("audit", audit_started_perf)
    attach_python_retrieval_metrics(
        pack,
        args,
        stage_latencies_ms=stage_latencies_ms,
        retrieval_scan_stats=retrieval_scan_stats,
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        records=records,
    )
    pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
    over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
    if over_budget_stages and not any(str(warning).startswith("stage_budget_exceeded:") for warning in quality_warnings):
        quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
        pack["quality_warnings"] = quality_warnings
    if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
        return pack
    return compact_context_pack_for_serving(pack)
