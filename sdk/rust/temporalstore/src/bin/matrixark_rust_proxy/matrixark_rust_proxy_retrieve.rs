// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_cross_session::parse_cross_session_policy;
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_retrieve_request::parse_retrieve_pack_request;
use crate::matrixark_rust_proxy_retrieve_response::{
    build_retrieve_pack_response, RetrievePackResponseInput,
};
use crate::matrixark_rust_proxy_retrieve_result::{
    try_sdk_native_pack, SdkNativePackAttempt,
};
use crate::matrixark_rust_proxy_retrieve_scoring::score_retrieve_candidates;
use crate::matrixark_rust_proxy_retrieve_select::select_retrieve_refs;
use crate::matrixark_rust_proxy_scan::scan_matrixark_candidates;
use crate::matrixark_rust_proxy_scope::session_scope_mode;

pub(crate) fn retrieve_context_pack_native(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    match try_sdk_native_pack(client, command) {
        SdkNativePackAttempt::Response(response) => return Ok(response),
        SdkNativePackAttempt::Error(err) => return Err(err),
        SdkNativePackAttempt::FallbackAllowed => {}
    }
    let parsed = parse_retrieve_pack_request(command);
    let request = parsed.request;
    let query = parsed.query;
    let query_terms = parsed.query_terms;
    let remote_budget = parsed.remote_budget;
    let max_refs = parsed.max_refs;
    let max_global_candidates = parsed.max_global_candidates;
    let min_similarity_score = parsed.min_similarity_score;
    let budget_fill_policy = parsed.budget_fill_policy;
    let question_type = parsed.question_type;
    let scan_command = parsed.scan_command;
    let scan = scan_matrixark_candidates(client, &scan_command)?;
    let empty_records = Vec::new();
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .unwrap_or(&empty_records);
    let scope_for_continuity = scan_command.scope.clone();
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let scored = score_retrieve_candidates(
        records,
        &query_terms,
        scope_for_continuity.as_ref(),
        &question_type,
        min_similarity_score,
        max_global_candidates,
    );
    let selection = select_retrieve_refs(scored, &cross_policy, remote_budget, max_refs);
    let scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    Ok(build_retrieve_pack_response(RetrievePackResponseInput {
        request,
        query,
        selected: selection.selected,
        selected_counts: selection.selected_counts,
        selected_nodes: selection.selected_nodes,
        scan_stats,
        cross_policy,
        remote_budget,
        max_refs,
        max_global_candidates,
        min_similarity_score,
        budget_fill_policy,
        session_scope_mode: scan_command
            .scope
            .as_ref()
            .map(session_scope_mode)
            .unwrap_or("only")
            .to_string(),
        used_tokens: selection.used_tokens,
        cross_used_tokens: selection.cross_used_tokens,
        cross_selected_refs: selection.cross_selected_refs,
        entity_bridge_selected_refs: selection.entity_bridge_selected_refs,
        selected_cross_sessions: selection.selected_cross_sessions,
        dropped_over_budget: selection.dropped_over_budget,
        dropped_cross_budget: selection.dropped_cross_budget,
        dropped_cross_session_cap: selection.dropped_cross_session_cap,
        dropped_cross_candidate_cap: selection.dropped_cross_candidate_cap,
        dropped_entity_bridge_slot_reserved: selection.dropped_entity_bridge_slot_reserved,
        dropped_low_score: selection.dropped_low_score,
        dropped_policy_ref: selection.dropped_policy_ref,
        dropped_duplicate_ref: selection.dropped_duplicate_ref,
    }))
}
