// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;
use crate::matrixark_rust_proxy_clock::unix_ms;
use crate::matrixark_rust_proxy_retrieve_pack_json::{
    build_context_pack, RetrieveContextPackInput,
};
use crate::matrixark_rust_proxy_retrieve_telemetry::{
    mark_native_pack_scan_stats, same_session_selected_ref_count, total_dropped_ref_count,
    RetrieveDropCounts,
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
    pub(crate) dropped_entity_bridge_slot_reserved: u64,
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
        entity_bridge_slot_reserved: input.dropped_entity_bridge_slot_reserved,
        policy_ref: input.dropped_policy_ref,
        duplicate_ref: input.dropped_duplicate_ref,
        scan_dropped: scan_dropped_count,
    });
    let selected_ref_count = input.selected.len();
    let same_session_selected_ref_count = same_session_selected_ref_count(&input.selected);
    let pack = build_context_pack(RetrieveContextPackInput {
        context_pack_id,
        request: input.request,
        query: input.query,
        selected: input.selected,
        selected_counts: input.selected_counts,
        selected_nodes: input.selected_nodes,
        scan_stats: scan_stats.clone(),
        cross_policy: input.cross_policy,
        remote_budget: input.remote_budget,
        max_refs: input.max_refs,
        max_global_candidates: input.max_global_candidates,
        min_similarity_score: input.min_similarity_score,
        budget_fill_policy: input.budget_fill_policy,
        session_scope_mode: input.session_scope_mode,
        used_tokens: input.used_tokens,
        cross_used_tokens: input.cross_used_tokens,
        cross_selected_refs: input.cross_selected_refs,
        entity_bridge_selected_refs: input.entity_bridge_selected_refs,
        selected_cross_sessions: input.selected_cross_sessions,
        dropped_over_budget: input.dropped_over_budget,
        dropped_cross_budget: input.dropped_cross_budget,
        dropped_cross_session_cap: input.dropped_cross_session_cap,
        dropped_cross_candidate_cap: input.dropped_cross_candidate_cap,
        dropped_entity_bridge_slot_reserved: input.dropped_entity_bridge_slot_reserved,
        dropped_low_score: input.dropped_low_score,
        dropped_policy_ref: input.dropped_policy_ref,
        dropped_duplicate_ref: input.dropped_duplicate_ref,
        same_session_selected_ref_count,
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
