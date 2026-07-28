#!/usr/bin/env python3
"""Local MatrixArk retrieval runtime."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_MAX_SELECTED_REFS,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        embedding_for_text,
        now_ms,
        passes_secondary_index_filters,
        scope_matches,
        select_token_budgeted_refs,
        stable_hash,
        summarize_text,
        tree_first_traversal,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_MAX_SELECTED_REFS,
        Json,
        MatrixArkError,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        clip_context_text,
        compact_context_pack_for_serving,
        embedding_for_text,
        now_ms,
        passes_secondary_index_filters,
        scope_matches,
        select_token_budgeted_refs,
        stable_hash,
        summarize_text,
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
    from tools import matrixark_mcp_retrieve_request as retrieve_request_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_request as retrieve_request_helpers

try:
    from tools import matrixark_mcp_native_retrieve as native_retrieve_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_native_retrieve as native_retrieve_helpers

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
    from tools import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers

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
    from tools import matrixark_mcp_retrieve_event_scan as retrieve_event_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_event_scan as retrieve_event_scan_helpers

try:
    from tools import matrixark_mcp_retrieve_entity_scan as retrieve_entity_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_entity_scan as retrieve_entity_scan_helpers

try:
    from tools import matrixark_mcp_retrieve_segment_scan as retrieve_segment_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_segment_scan as retrieve_segment_scan_helpers

try:
    from tools import matrixark_mcp_retrieve_resource_skill_scan as retrieve_resource_skill_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_resource_skill_scan as retrieve_resource_skill_scan_helpers

try:
    from tools import matrixark_mcp_retrieve_compression_scan as retrieve_compression_scan_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_compression_scan as retrieve_compression_scan_helpers

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
    retrieval_request = retrieve_request_helpers.prepare_retrieval_request(self, args, started_perf=started_perf)
    query = retrieval_request["query"]
    scope = retrieval_request["scope"]
    storage_options = retrieval_request["storage_options"]
    ranking = retrieval_request["ranking"]
    audit_mode = retrieval_request["audit_mode"]
    audit_sample_rate = retrieval_request["audit_sample_rate"]
    deadline_ms = retrieval_request["deadline_ms"]
    deadline_tracker = retrieval_request["deadline_tracker"]
    stage_latencies_ms = deadline_tracker.stage_latencies_ms
    stage_started_perf = retrieval_request["stage_started_perf"]

    def deadline_exceeded() -> bool:
        return deadline_tracker.deadline_exceeded()

    def finish_retrieval_stage(stage: str, started: float) -> float:
        return deadline_tracker.finish_stage(stage, started)

    def stage_budget_snapshot() -> Json:
        return deadline_tracker.stage_budget_snapshot()

    retrieval_plan = retrieval_request["retrieval_plan"]
    question_type = retrieval_request["question_type"]
    retrieval_session_scope = retrieval_request["retrieval_session_scope"]
    retrieval_scope = retrieval_request["retrieval_scope"]
    secondary_index_filter_groups = retrieval_request["secondary_index_filter_groups"]
    secondary_index_filter_mode = retrieval_request["secondary_index_filter_mode"]
    secondary_index_dropped_count = 0
    secondary_index_matched_count = 0
    budget_source = retrieval_request["budget_source"]
    max_context_tokens = retrieval_request["max_context_tokens"]
    local_budget = retrieval_request["local_budget"]
    local_tokens = retrieval_request["local_tokens"]
    safety_margin_tokens = retrieval_request["safety_margin_tokens"]
    remote_context_budget_tokens = retrieval_request["remote_context_budget_tokens"]
    cross_session_policy = retrieval_request["cross_session_policy"]
    shared_context_policy = retrieval_request["shared_context_policy"]
    source_role_budget_tokens = retrieval_request["source_role_budget_tokens"]
    source_role_budget_mode = retrieval_request["source_role_budget_mode"]
    memory_layer_budget_tokens = retrieval_request["memory_layer_budget_tokens"]
    memory_layer_budget_mode = retrieval_request["memory_layer_budget_mode"]
    pre_retrieval_summary_refresh = retrieval_request["pre_retrieval_summary_refresh"]
    pre_retrieval_refreshed_records = retrieval_request["pre_retrieval_refreshed_records"]
    query_terms = retrieval_request["query_terms"]
    reference_time_ms = retrieval_request["reference_time_ms"]
    query_plan = retrieval_request["query_plan"]
    debug_refs = retrieval_request["debug_refs"]
    pack_cache_key = retrieval_request["pack_cache_key"]
    cached_pack = retrieval_request["cached_pack"]
    if cached_pack is not None:
        return cached_pack
    auxiliary_quota = retrieval_request["auxiliary_quota"]
    annotate_session_continuity = retrieval_request["annotate_session_continuity"]

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
    records = pre_refresh_helpers.merge_refreshed_summary_records(
        self,
        records,
        retrieval_scope=retrieval_scope,
        refreshed_records=pre_retrieval_refreshed_records,
        refresh=pre_retrieval_summary_refresh,
    )
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
    event_primary, event_auxiliary, event_dropped, event_matched, fallback_reason = retrieve_event_scan_helpers.scan_event_candidates(
        tree_candidate_records,
        raw_event_ids_by_node=raw_event_ids_by_node,
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
        event_embedding_vectors=event_embedding_vectors,
        node_scores=node_scores,
        annotate_session_continuity=annotate_session_continuity,
        ranking=ranking,
        reference_time_ms=reference_time_ms,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason, records)
    primary_matches.extend(event_primary)
    auxiliary_matches.extend(event_auxiliary)
    secondary_index_dropped_count += event_dropped
    secondary_index_matched_count += event_matched
    if deadline_exceeded():
        return deadline_fallback("deadline_after_event_scan")
    entity_primary, entity_auxiliary, entity_dropped, entity_matched, fallback_reason = retrieve_entity_scan_helpers.scan_entity_candidates(
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
        entity_embedding_vectors=entity_embedding_vectors,
        node_scores=node_scores,
        annotate_session_continuity=annotate_session_continuity,
        ranking=ranking,
        reference_time_ms=reference_time_ms,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason, records)
    primary_matches.extend(entity_primary)
    auxiliary_matches.extend(entity_auxiliary)
    secondary_index_dropped_count += entity_dropped
    secondary_index_matched_count += entity_matched
    if deadline_exceeded():
        return deadline_fallback("deadline_after_entity_scan")
    segment_primary, segment_auxiliary, segment_dropped, segment_matched, fallback_reason = retrieve_segment_scan_helpers.scan_segment_candidates(
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
        segment_embedding_vectors=segment_embedding_vectors,
        node_scores=node_scores,
        annotate_session_continuity=annotate_session_continuity,
        ranking=ranking,
        reference_time_ms=reference_time_ms,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason, records)
    primary_matches.extend(segment_primary)
    auxiliary_matches.extend(segment_auxiliary)
    secondary_index_dropped_count += segment_dropped
    secondary_index_matched_count += segment_matched
    if deadline_exceeded():
        return deadline_fallback("deadline_after_segment_scan")
    resource_skill_primary, resource_skill_dropped, resource_skill_matched, fallback_reason = (
        retrieve_resource_skill_scan_helpers.scan_resource_skill_candidates(
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
            resource_embedding_vectors=resource_embedding_vectors,
            node_scores=node_scores,
            annotate_session_continuity=annotate_session_continuity,
            ranking=ranking,
            reference_time_ms=reference_time_ms,
            skill_controls=skill_controls,
            latest_resource_version_by_hash=latest_resource_version_by_hash,
            resource_uri_by_hash=resource_uri_by_hash,
            include_superseded_resources=include_superseded_resources,
            deadline_exceeded=deadline_exceeded,
        )
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason, records)
    primary_matches.extend(resource_skill_primary)
    secondary_index_dropped_count += resource_skill_dropped
    secondary_index_matched_count += resource_skill_matched

    compression_primary, compression_auxiliary, fallback_reason = retrieve_compression_scan_helpers.scan_compression_candidates(
        tree_candidate_records,
        retrieval_scope=retrieval_scope,
        selected_by_tree=selected_by_tree,
        admit_candidate_for_node=admit_candidate_for_node,
        query_terms=query_terms,
        query_embedding=query_embedding,
        compression_embedding_vectors=compression_embedding_vectors,
        node_scores=node_scores,
        annotate_session_continuity=annotate_session_continuity,
        ranking=ranking,
        reference_time_ms=reference_time_ms,
        deadline_exceeded=deadline_exceeded,
    )
    if fallback_reason:
        return deadline_fallback(fallback_reason, records)
    primary_matches.extend(compression_primary)
    auxiliary_matches.extend(compression_auxiliary)
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
        source_role_budget_tokens=source_role_budget_tokens,
        memory_layer_budget_tokens=memory_layer_budget_tokens,
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
        source_role_budget_tokens=source_role_budget_tokens,
        source_role_budget_mode=source_role_budget_mode,
        memory_layer_budget_tokens=memory_layer_budget_tokens,
        memory_layer_budget_mode=memory_layer_budget_mode,
        pre_retrieval_summary_refresh=pre_retrieval_summary_refresh,
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
