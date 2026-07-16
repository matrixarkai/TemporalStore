use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;

pub(crate) struct RetrieveRecallPolicyInput<'a> {
    pub(crate) scan_stats: &'a Value,
    pub(crate) cross_policy: &'a CrossSessionPolicy,
    pub(crate) remote_budget: u64,
    pub(crate) max_refs: u64,
    pub(crate) max_global_candidates: u64,
    pub(crate) min_similarity_score: f64,
    pub(crate) budget_fill_policy: &'a str,
    pub(crate) session_scope_mode: &'a str,
    pub(crate) same_session_selected_ref_count: usize,
    pub(crate) cross_selected_refs: u64,
    pub(crate) entity_bridge_selected_refs: u64,
    pub(crate) selected_cross_session_count: u64,
    pub(crate) cross_used_tokens: u64,
    pub(crate) selected_node_count: u64,
    pub(crate) secondary_index_matched_candidate_count: Value,
    pub(crate) secondary_index_dropped_candidate_count: Value,
}

pub(crate) fn build_recall_policy(input: RetrieveRecallPolicyInput<'_>) -> Value {
    json!({
        "native_context_pack": {
            "enabled": true,
            "backend": "rust_proxy",
            "scan_filter_score_pack": true
        },
        "native_response_contract": {
            "raw_records_returned_to_python": false,
            "python_hot_path_records": 0,
            "python_role": "dispatch_request_receive_context_pack",
            "backend_role": "scan_filter_score_pack"
        },
        "scan_stats": input.scan_stats,
        "rerank": {
            "enabled": true,
            "mode": "native_weighted_recall_plus_cross_session_rerank",
            "cross_session_rerank_enabled": true,
            "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
            "heavy_rerank_enabled": false
        },
        "ranking": {
            "min_similarity_score": input.min_similarity_score,
            "max_global_candidates": input.max_global_candidates,
            "max_selected_refs": input.max_refs,
            "budget_fill_policy": input.budget_fill_policy,
            "quality_first_budget_underfill_allowed": input.budget_fill_policy == "quality_first"
        },
        "session_continuity": {
            "mode": input.session_scope_mode,
            "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
            "same_session_selected_ref_count": input.same_session_selected_ref_count,
            "cross_session_selected_ref_count": input.cross_selected_refs,
            "entity_bridge_selected_ref_count": input.entity_bridge_selected_refs
        },
        "cross_session": {
            "enabled": input.cross_policy.enabled,
            "mode": if input.cross_policy.enabled { "prefer" } else { "disabled" },
            "budget_ratio": input.cross_policy.budget_ratio,
            "max_budget_ratio": input.cross_policy.max_budget_ratio,
            "budget_tokens": input.cross_policy.budget_tokens,
            "remote_budget_tokens": input.remote_budget,
            "max_budget_tokens": input.cross_policy.max_budget_tokens,
            "max_sessions": input.cross_policy.max_sessions,
            "max_candidates": input.cross_policy.max_candidates,
            "min_score": input.cross_policy.min_score,
            "raw_evidence_min_score": input.cross_policy.raw_evidence_min_score,
            "parallelism": input.cross_policy.parallelism,
            "selected_tokens": input.cross_used_tokens,
            "selected_ref_count": input.cross_selected_refs,
            "selected_session_count": input.selected_cross_session_count,
            "entity_bridge_selected_ref_count": input.entity_bridge_selected_refs,
            "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
            "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
        },
        "tree_traversal": {
            "enabled": true,
            "native_backend": true,
            "fallback_to_flat": false,
            "selected_node_count": input.selected_node_count,
            "selected_leaf_count": input.selected_node_count,
            "summary_embeddings": ["node_l0", "node_l1"]
        },
        "secondary_index_filter": {
            "enabled": true,
            "native_backend": true,
            "applied_before_embedding_scoring": true,
            "matched_candidate_count": input.secondary_index_matched_candidate_count,
            "dropped_candidate_count": input.secondary_index_dropped_candidate_count
        }
    })
}
