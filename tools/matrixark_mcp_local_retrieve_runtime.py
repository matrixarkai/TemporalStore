#!/usr/bin/env python3
"""Local MatrixArk retrieval runtime."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_SELECTED_REFS,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        cosine,
        embedding_for_text,
        hybrid_origin_score,
        integer_arg,
        normalize_storage_options,
        now_ms,
        optional_object,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        require_string,
        scope_matches,
        score_recall_candidate,
        select_token_budgeted_refs,
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
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_SELECTED_REFS,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        cosine,
        embedding_for_text,
        hybrid_origin_score,
        integer_arg,
        normalize_storage_options,
        now_ms,
        optional_object,
        passes_applicable_secondary_index_filters,
        passes_secondary_index_filters,
        require_string,
        scope_matches,
        score_recall_candidate,
        select_token_budgeted_refs,
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
    from tools import matrixark_mcp_retrieve_scan_state as retrieve_scan_state_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_scan_state as retrieve_scan_state_helpers

try:
    from tools import matrixark_mcp_retrieve_summary_scan as retrieve_summary_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_summary_scan as retrieve_summary_scan_helpers

try:
    from tools import matrixark_mcp_retrieve_candidate_builders as candidate_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_candidate_builders as candidate_builders

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

try:
    from tools.matrixark_mcp_retrieve_audit import build_context_pack_audit_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_audit import build_context_pack_audit_record

try:
    from tools.matrixark_mcp_retrieve_pack_builder import (
        build_context_pack,
        prepare_serving_refs,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import (
        build_context_pack,
        prepare_serving_refs,
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
    scan_state = retrieve_scan_state_helpers.RetrieveScanState()
    node_scores = scan_state.node_scores
    event_embedding_vectors = scan_state.event_embedding_vectors
    entity_embedding_vectors = scan_state.entity_embedding_vectors
    segment_embedding_vectors = scan_state.segment_embedding_vectors
    compression_embedding_vectors = scan_state.compression_embedding_vectors
    resource_embedding_vectors = scan_state.resource_embedding_vectors
    skill_embedding_vectors = scan_state.skill_embedding_vectors
    index_terms_by_batch = scan_state.index_terms_by_batch
    index_terms_by_node = scan_state.index_terms_by_node
    index_terms_by_ref = scan_state.index_terms_by_ref
    secondary_index_prefilter_node_hashes, fallback_reason = retrieve_scan_state_helpers.scan_context_indexes(
        records,
        retrieval_scope=retrieval_scope,
        scope=scope,
        query_plan=query_plan,
        secondary_index_filter_groups=secondary_index_filter_groups,
        secondary_index_filter_mode=secondary_index_filter_mode,
        state=scan_state,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason)
    fallback_reason = retrieve_scan_state_helpers.scan_context_embeddings(
        records,
        scope=scope,
        query_embedding=query_embedding,
        query_terms=query_terms,
        secondary_index_prefilter_node_hashes=secondary_index_prefilter_node_hashes,
        state=scan_state,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason)
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
                retrieve_scan_state_helpers.add_index_terms(record, state=scan_state)
            elif record_type == "context_embedding" and scope_matches(candidate_access_scope(record), scope):
                retrieve_scan_state_helpers.add_embedding_vector(record, state=scan_state)

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
        summary_matches, summary_dropped, summary_matched, fallback_reason = retrieve_summary_scan_helpers.scan_summary_candidates(
            tree_candidate_records,
            retrieval_scope=retrieval_scope,
            selected_by_tree=selected_by_tree,
            index_terms_by_batch=index_terms_by_batch,
            index_terms_by_node=index_terms_by_node,
            index_terms_by_ref=index_terms_by_ref,
            secondary_index_filter_groups=secondary_index_filter_groups,
            secondary_index_filter_mode=secondary_index_filter_mode,
            admit_candidate_for_node=admit_candidate_for_node,
            query_terms=query_terms,
            query_embedding=query_embedding,
            node_scores=node_scores,
            annotate_session_continuity=annotate_session_continuity,
            ranking=ranking,
            reference_time_ms=reference_time_ms,
            deadline_exceeded=deadline_exceeded,
        )
        if fallback_reason:
            return deadline_fallback(fallback_reason, records)
        primary_matches.extend(summary_matches)
        secondary_index_dropped_count += summary_dropped
        secondary_index_matched_count += summary_matched
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
        candidate = candidate_builders.event_candidate(
            record,
            envelope=envelope,
            record_scope=record_scope,
            index_terms=index_terms,
            event_type=event_type,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            metadata=candidate_metadata,
            text=text,
        )
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
        candidate = candidate_builders.entity_candidate(
            record,
            index_terms=index_terms,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            text=text,
        )
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
        candidate = candidate_builders.segment_candidate(
            record,
            index_terms=index_terms,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            saliency_score=saliency_score,
        )
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
        candidate = candidate_builders.resource_skill_candidate(
            record,
            ref_type=ref_type,
            ref_hash=ref_hash,
            resource_hash=resource_hash,
            source_locator=source_locator,
            resource_version=resource_version_value,
            supersedes_chunk_hash=metadata.get("supersedes_chunk_hash"),
            version_state=version_state,
            stale_or_superseded=is_superseded_version,
            citation=citation,
            metadata=metadata,
            business_type=business_type,
            index_terms=index_terms,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            text=text,
        )
        primary_matches.append(
            score_recall_candidate(
                annotate_session_continuity(candidate, record),
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
        candidate = candidate_builders.compression_candidate(
            record,
            compression_hash=compression_hash,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            text=text,
        )
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
    serving_selected, serving_dropped = prepare_serving_refs(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        debug_refs=debug_refs,
    )
    pack_summary = summarize_text(
        " ".join(str(item.get("text", "")) for item in selected),
        limit=512,
    )
    time_weighted_recall = build_time_weighted_recall(
        ranking=ranking,
        selected=selected,
        reference_time_ms=reference_time_ms,
    )
    pack = build_context_pack(
        context_pack_id=context_pack_id,
        selected=selected,
        local_budget=local_budget,
        serving_selected=serving_selected,
        dropped_over_budget=dropped_over_budget,
        serving_dropped=serving_dropped,
        layer_scores=layer_scores,
        question_type=question_type,
        query_plan=query_plan,
        retrieval_session_scope=retrieval_session_scope,
        cross_session_policy=cross_session_policy,
        shared_context_policy=shared_context_policy,
        retrieval_scan_stats=retrieval_scan_stats,
        ranking=ranking,
        min_similarity_score=min_similarity_score,
        max_global_candidates=max_global_candidates,
        max_selected_refs=max_selected_refs,
        budget_fill_policy=budget_fill_policy,
        traversal=traversal,
        top_k_per_layer=top_k_per_layer,
        max_children_scored_per_parent=max_children_scored_per_parent,
        hard_max_children_scored_per_parent=hard_max_children_scored_per_parent,
        max_candidates_per_node=max_candidates_per_node,
        max_raw_events_per_node=max_raw_events_per_node,
        selected_node_hashes=selected_node_hashes,
        selected_paths=selected_paths,
        tree_candidate_records_count=len(tree_candidate_records),
        tree_prefilter_dropped_count=tree_prefilter_dropped_count,
        fanout_dropped_count=fanout_limiter.dropped_count,
        raw_event_time_window_dropped_count=raw_event_time_window_dropped_count,
        secondary_index_filter_groups=secondary_index_filter_groups,
        secondary_index_matched_count=secondary_index_matched_count,
        secondary_index_dropped_count=secondary_index_dropped_count,
        secondary_index_filter_mode=secondary_index_filter_mode,
        rerank_policy=rerank_policy,
        time_weighted_recall=time_weighted_recall,
        reinforcement=reinforcement,
        auxiliary_quota=auxiliary_quota,
        storage_options=storage_options,
        deadline_ms=deadline_ms,
        started_perf=started_perf,
        partial_context_pack=partial_context_pack,
        primary_candidate_count=len(primary_matches),
        auxiliary_candidate_count=len(auxiliary_matches),
        used_context_tokens=used_context_tokens,
        local_tokens=local_tokens,
        remote_context_budget_tokens=remote_context_budget_tokens,
        max_context_tokens=max_context_tokens,
        safety_margin_tokens=safety_margin_tokens,
        budget_source=budget_source,
        quality_warnings=quality_warnings,
        audit_mode=audit_mode,
        audit_sample_rate=audit_sample_rate,
        debug_refs=debug_refs,
    )
    finish_retrieval_stage("pack", stage_started_perf)
    pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
    over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
    if over_budget_stages:
        quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
        pack["quality_warnings"] = quality_warnings
    audit_started_perf = time.perf_counter()
    audit_record = build_context_pack_audit_record(
        context_pack_id_text=context_pack_id_text,
        query=query,
        scope=scope,
        pack_summary=pack_summary,
        selected=selected,
        local_budget=local_budget,
        pack=pack,
        dropped_over_budget=dropped_over_budget,
        quality_warnings=quality_warnings,
        partial_context_pack=partial_context_pack,
        layer_scores=layer_scores,
        question_type=question_type,
        rerank_policy=rerank_policy,
        storage_options=storage_options,
        primary_candidate_count=len(primary_matches),
        auxiliary_candidate_count=len(auxiliary_matches),
        tree_candidate_records_count=len(tree_candidate_records),
        tree_prefilter_dropped_count=tree_prefilter_dropped_count,
        fanout_dropped_count=fanout_limiter.dropped_count,
        max_candidates_per_node=max_candidates_per_node,
        max_selected_refs=max_selected_refs,
    )
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
