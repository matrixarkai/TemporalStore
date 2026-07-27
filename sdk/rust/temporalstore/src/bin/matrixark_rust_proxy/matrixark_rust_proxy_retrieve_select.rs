use serde_json::Value;

use crate::matrixark_rust_proxy_cross_session::{cross_session_key, CrossSessionPolicy};
use crate::matrixark_rust_proxy_pack::{
    candidate_text, context_class_name, is_serving_selected_ref_class, pack_ref_from_record,
    token_estimate,
};
use crate::matrixark_rust_proxy_retrieve_result::RetrieveSelection;
use crate::matrixark_rust_proxy_retrieve_scoring::ScoredCandidate;
use crate::matrixark_rust_proxy_retrieve_select_state::RetrieveSelectState;
use crate::matrixark_rust_proxy_retrieve_signature::selected_ref_signature;

pub(crate) fn select_retrieve_refs(
    scored: Vec<ScoredCandidate<'_>>,
    cross_policy: &CrossSessionPolicy,
    remote_budget: u64,
    max_refs: u64,
) -> RetrieveSelection {
    let mut state = RetrieveSelectState::new();
    for (index, scored_candidate) in scored.iter().enumerate() {
        let score = scored_candidate.score;
        let record = scored_candidate.record;
        let session_continuity = scored_candidate.session_continuity.clone();
        let continuity_boost_value = scored_candidate.continuity_boost;
        let cross_session_rerank_boost_value = scored_candidate.cross_session_rerank_boost;
        if state.selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        let context_class = context_class_name(record);
        if !is_serving_selected_ref_class(&context_class) {
            state.dropped_policy_ref += 1;
            continue;
        }
        let is_cross_session = session_continuity == "cross_session";
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_entity_bridge = is_cross_session && context_class == "entity";
        let is_cross_session_raw_evidence =
            is_cross_session && matches!(record_type, "context_event" | "context_segment");
        let cross_key = if is_cross_session {
            cross_session_key(record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            state.dropped_cross_budget += 1;
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            state.dropped_low_score += 1;
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            state.dropped_low_score += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && state.cross_selected_refs >= cross_policy.max_candidates
        {
            state.dropped_cross_candidate_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !state.selected_cross_sessions.contains(&cross_key)
            && state.selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            state.dropped_cross_session_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && state.cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && state.entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            state.dropped_cross_budget += 1;
            continue;
        }
        let remaining_slots = max_refs.saturating_sub(state.selected.len() as u64);
        let remaining_required_bridge_refs = cross_policy
            .min_entity_bridge_refs
            .saturating_sub(state.entity_bridge_selected_refs);
        if cross_policy.enabled
            && !is_entity_bridge
            && remaining_required_bridge_refs > 0
            && remaining_slots <= remaining_required_bridge_refs
            && eligible_entity_bridge_remains(&scored, index + 1, cross_policy)
        {
            state.dropped_entity_bridge_slot_reserved += 1;
            continue;
        }
        let ref_signature = selected_ref_signature(record, &context_class);
        if !state.selected_signatures.insert(ref_signature) {
            state.dropped_duplicate_ref += 1;
            continue;
        }
        if state.used_tokens + tokens > remote_budget {
            state.dropped_over_budget += 1;
            continue;
        }
        state.used_tokens += tokens;
        if is_cross_session {
            state.cross_used_tokens += tokens;
            state.cross_selected_refs += 1;
            state.selected_cross_sessions.insert(cross_key);
            if is_entity_bridge {
                state.entity_bridge_selected_refs += 1;
            }
        }
        state.select_node(record, &context_class);
        state.selected.push(pack_ref_from_record(
            record,
            score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        ));
    }
    state.into_selection()
}

fn eligible_entity_bridge_remains(
    scored: &[ScoredCandidate<'_>],
    start_index: usize,
    cross_policy: &CrossSessionPolicy,
) -> bool {
    if !cross_policy.enabled {
        return false;
    }
    scored.iter().skip(start_index).any(|candidate| {
        let record = candidate.record;
        let context_class = context_class_name(record);
        candidate.session_continuity == "cross_session"
            && context_class == "entity"
            && candidate.score >= cross_policy.min_score
    })
}
