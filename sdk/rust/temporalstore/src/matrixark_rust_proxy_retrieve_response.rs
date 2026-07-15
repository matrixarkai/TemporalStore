use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;
use crate::matrixark_rust_proxy_metrics::unix_ms;
use crate::matrixark_rust_proxy_retrieve_result::{scan_cache_hit, scan_dropped_count};

pub(crate) struct RetrievePackResponseInput {
    pub(crate) request: Value,
    pub(crate) query: String,
    pub(crate) selected: Vec<Value>,
    pub(crate) selected_counts: HashMap<String, u64>,
    pub(crate) selected_nodes: HashSet<u64>,
    pub(crate) scan_stats: Value,
    pub(crate) cross_policy: CrossSessionPolicy,
    pub(crate) remote_budget: u64,
    pub(crate) max_refs: u64,
    pub(crate) max_global_candidates: u64,
    pub(crate) min_similarity_score: f64,
    pub(crate) budget_fill_policy: String,
    pub(crate) session_scope_mode: String,
    pub(crate) used_tokens: u64,
    pub(crate) cross_used_tokens: u64,
    pub(crate) cross_selected_refs: u64,
    pub(crate) entity_bridge_selected_refs: u64,
    pub(crate) selected_cross_sessions: HashSet<String>,
    pub(crate) dropped_over_budget: u64,
    pub(crate) dropped_cross_budget: u64,
    pub(crate) dropped_cross_session_cap: u64,
    pub(crate) dropped_cross_candidate_cap: u64,
    pub(crate) dropped_low_score: u64,
    pub(crate) dropped_policy_ref: u64,
    pub(crate) dropped_duplicate_ref: u64,
}

pub(crate) fn build_retrieve_pack_response(input: RetrievePackResponseInput) -> Value {
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), input.selected.len());
    let mut scan_stats = input.scan_stats;
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let scan_dropped_count = scan_dropped_count(&scan_stats);
    let scan_cache_hit = scan_cache_hit(&scan_stats);
    let dropped_ref_count = input.dropped_over_budget
        + input.dropped_cross_budget
        + input.dropped_cross_session_cap
        + input.dropped_cross_candidate_cap
        + input.dropped_policy_ref
        + input.dropped_duplicate_ref
        + scan_dropped_count;
    let selected_ref_count = input.selected.len();
    let same_session_selected_ref_count = input
        .selected
        .iter()
        .filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session"))
        .count();
    let secondary_index_matched_candidate_count = scan_stats
        .get("secondary_index_matched_candidate_count")
        .cloned()
        .unwrap_or_else(|| json!(0));
    let secondary_index_dropped_candidate_count = scan_stats
        .get("secondary_index_dropped_candidate_count")
        .cloned()
        .unwrap_or_else(|| json!(0));
    let requested_max_context_tokens = input
        .request
        .get("max_context_tokens")
        .cloned()
        .unwrap_or_else(|| json!(input.remote_budget));
    let question_type = input
        .request
        .get("question_type")
        .cloned()
        .unwrap_or_else(|| json!("fact"));
    let selected = input.selected;
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": input.query,
        "question_type": question_type,
        "selected_ref_counts": input.selected_counts,
        "remote_context_refs": selected,
        "selected_refs": selected,
        "dropped_refs": {
            "over_budget": input.dropped_over_budget,
            "cross_session_budget": input.dropped_cross_budget,
            "cross_session_session_cap": input.dropped_cross_session_cap,
            "cross_session_candidate_cap": input.dropped_cross_candidate_cap,
            "low_score": input.dropped_low_score,
            "duplicate_ref": input.dropped_duplicate_ref,
            "policy_ref": input.dropped_policy_ref,
            "reason_counts": {
                "over_budget": input.dropped_over_budget,
                "cross_session_budget": input.dropped_cross_budget,
                "cross_session_session_cap": input.dropped_cross_session_cap,
                "cross_session_candidate_cap": input.dropped_cross_candidate_cap,
                "low_score": input.dropped_low_score,
                "duplicate_ref": input.dropped_duplicate_ref,
                "policy_ref": input.dropped_policy_ref
            }
        },
        "used_context_tokens": input.used_tokens,
        "used_remote_context_tokens": input.used_tokens,
        "remote_context_budget_tokens": input.remote_budget,
        "requested_max_context_tokens": requested_max_context_tokens,
        "packing_policy": "native_rust_proxy_question_type_aware",
        "context_pack_assembly": "native_rust_proxy",
        "context_sources_order": ["entities", "events", "segments", "resources", "skills", "summaries"],
        "recall_policy": {
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
            "scan_stats": scan_stats,
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
                "same_session_selected_ref_count": same_session_selected_ref_count,
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
                "selected_session_count": input.selected_cross_sessions.len() as u64,
                "entity_bridge_selected_ref_count": input.entity_bridge_selected_refs,
                "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
            },
            "tree_traversal": {
                "enabled": true,
                "native_backend": true,
                "fallback_to_flat": false,
                "selected_node_count": input.selected_nodes.len() as u64,
                "selected_leaf_count": input.selected_nodes.len() as u64,
                "summary_embeddings": ["node_l0", "node_l1"]
            },
            "secondary_index_filter": {
                "enabled": true,
                "native_backend": true,
                "applied_before_embedding_scoring": true,
                "matched_candidate_count": secondary_index_matched_candidate_count,
                "dropped_candidate_count": secondary_index_dropped_candidate_count
            }
        },
        "quality_warnings": []
    });
    json!({
        "ok": true,
        "count": selected_ref_count,
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scan_stats.get("scanned_records").and_then(Value::as_u64).unwrap_or(0),
        "cache_hit": scan_cache_hit,
        "cache_hit_used": scan_cache_hit,
        "selected_ref_count": selected_ref_count,
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": input.dropped_duplicate_ref,
        "context_pack": pack,
        "scan_stats": scan_stats
    })
}
