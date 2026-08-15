// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;
use crate::matrixark_rust_proxy_retrieve_policy::{
    build_recall_policy, RetrieveRecallPolicyInput,
};
use crate::matrixark_rust_proxy_retrieve_telemetry::{
    dropped_refs_json, RetrieveDroppedRefs,
};

pub(crate) struct RetrieveContextPackInput {
    pub(crate) context_pack_id: String,
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
    pub(crate) same_session_selected_ref_count: usize,
}

pub(crate) fn build_context_pack(input: RetrieveContextPackInput) -> Value {
    let secondary_index_matched_candidate_count = input
        .scan_stats
        .get("secondary_index_matched_candidate_count")
        .cloned()
        .unwrap_or_else(|| json!(0));
    let secondary_index_dropped_candidate_count = input
        .scan_stats
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
        scan_stats: &input.scan_stats,
        cross_policy: &input.cross_policy,
        remote_budget: input.remote_budget,
        max_refs: input.max_refs,
        max_global_candidates: input.max_global_candidates,
        min_similarity_score: input.min_similarity_score,
        budget_fill_policy: &input.budget_fill_policy,
        session_scope_mode: &input.session_scope_mode,
        same_session_selected_ref_count: input.same_session_selected_ref_count,
        cross_selected_refs: input.cross_selected_refs,
        entity_bridge_selected_refs: input.entity_bridge_selected_refs,
        selected_cross_session_count: input.selected_cross_sessions.len() as u64,
        cross_used_tokens: input.cross_used_tokens,
        selected_node_count: input.selected_nodes.len() as u64,
        secondary_index_matched_candidate_count,
        secondary_index_dropped_candidate_count,
    });
    let selected = input.selected;
    json!({
        "context_pack_id": input.context_pack_id,
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
            entity_bridge_slot_reserved: input.dropped_entity_bridge_slot_reserved,
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
    })
}
