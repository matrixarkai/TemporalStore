use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;
use crate::matrixark_rust_proxy_clock::unix_ms;
use crate::matrixark_rust_proxy_retrieve_policy::{
    build_recall_policy, RetrieveRecallPolicyInput,
};
use crate::matrixark_rust_proxy_retrieve_telemetry::{
    dropped_refs_json, mark_native_pack_scan_stats, same_session_selected_ref_count,
    total_dropped_ref_count, RetrieveDropCounts, RetrieveDroppedRefs,
};
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
    let scan_stats = mark_native_pack_scan_stats(input.scan_stats);
    let scan_dropped_count = scan_dropped_count(&scan_stats);
    let scan_cache_hit = scan_cache_hit(&scan_stats);
    let dropped_ref_count = total_dropped_ref_count(RetrieveDropCounts {
        over_budget: input.dropped_over_budget,
        cross_budget: input.dropped_cross_budget,
        cross_session_cap: input.dropped_cross_session_cap,
        cross_candidate_cap: input.dropped_cross_candidate_cap,
        policy_ref: input.dropped_policy_ref,
        duplicate_ref: input.dropped_duplicate_ref,
        scan_dropped: scan_dropped_count,
    });
    let selected_ref_count = input.selected.len();
    let same_session_selected_ref_count = same_session_selected_ref_count(&input.selected);
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
    let recall_policy = build_recall_policy(RetrieveRecallPolicyInput {
        scan_stats: &scan_stats,
        cross_policy: &input.cross_policy,
        remote_budget: input.remote_budget,
        max_refs: input.max_refs,
        max_global_candidates: input.max_global_candidates,
        min_similarity_score: input.min_similarity_score,
        budget_fill_policy: &input.budget_fill_policy,
        session_scope_mode: &input.session_scope_mode,
        same_session_selected_ref_count,
        cross_selected_refs: input.cross_selected_refs,
        entity_bridge_selected_refs: input.entity_bridge_selected_refs,
        selected_cross_session_count: input.selected_cross_sessions.len() as u64,
        cross_used_tokens: input.cross_used_tokens,
        selected_node_count: input.selected_nodes.len() as u64,
        secondary_index_matched_candidate_count,
        secondary_index_dropped_candidate_count,
    });
    let selected = input.selected;
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": input.query,
        "question_type": question_type,
        "selected_ref_counts": input.selected_counts,
        "remote_context_refs": selected,
        "selected_refs": selected,
        "dropped_refs": dropped_refs_json(RetrieveDroppedRefs {
            over_budget: input.dropped_over_budget,
            cross_budget: input.dropped_cross_budget,
            cross_session_cap: input.dropped_cross_session_cap,
            cross_candidate_cap: input.dropped_cross_candidate_cap,
            low_score: input.dropped_low_score,
            duplicate_ref: input.dropped_duplicate_ref,
            policy_ref: input.dropped_policy_ref,
        }),
        "used_context_tokens": input.used_tokens,
        "used_remote_context_tokens": input.used_tokens,
        "remote_context_budget_tokens": input.remote_budget,
        "requested_max_context_tokens": requested_max_context_tokens,
        "packing_policy": "native_rust_proxy_question_type_aware",
        "context_pack_assembly": "native_rust_proxy",
        "context_sources_order": ["entities", "events", "segments", "resources", "skills", "summaries"],
        "recall_policy": recall_policy,
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
