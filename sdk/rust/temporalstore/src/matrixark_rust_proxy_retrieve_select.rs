use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::{record_node_hash, record_ref_hash};
use crate::matrixark_rust_proxy_cross_session::{cross_session_key, CrossSessionPolicy};
use crate::matrixark_rust_proxy_pack::{
    candidate_text, context_class_name, is_serving_selected_ref_class, pack_ref_from_record,
    token_estimate,
};
use crate::matrixark_rust_proxy_retrieve_result::RetrieveSelection;
use crate::matrixark_rust_proxy_retrieve_scoring::ScoredCandidate;

pub(crate) fn select_retrieve_refs(
    scored: Vec<ScoredCandidate<'_>>,
    cross_policy: &CrossSessionPolicy,
    remote_budget: u64,
    max_refs: u64,
) -> RetrieveSelection {
    let mut selected = Vec::new();
    let mut selected_signatures: HashSet<String> = HashSet::new();
    let mut dropped_duplicate_ref = 0_u64;
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut used_tokens = 0_u64;
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    for scored_candidate in scored {
        let score = scored_candidate.score;
        let record = scored_candidate.record;
        let session_continuity = scored_candidate.session_continuity;
        let continuity_boost_value = scored_candidate.continuity_boost;
        let cross_session_rerank_boost_value = scored_candidate.cross_session_rerank_boost;
        if selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        let context_class = context_class_name(record);
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
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
            dropped_cross_budget += 1;
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            continue;
        }
        let ref_signature = format!(
            "{}:{}",
            context_class,
            record_ref_hash(record).unwrap_or_else(|| {
                record
                    .get("record_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
        );
        if !selected_signatures.insert(ref_signature) {
            dropped_duplicate_ref += 1;
            continue;
        }
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            continue;
        }
        used_tokens += tokens;
        if is_cross_session {
            cross_used_tokens += tokens;
            cross_selected_refs += 1;
            selected_cross_sessions.insert(cross_key);
            if is_entity_bridge {
                entity_bridge_selected_refs += 1;
            }
        }
        *selected_counts.entry(context_class).or_default() += 1;
        if let Some(node_hash) = record_node_hash(record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            record,
            score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        ));
    }
    RetrieveSelection {
        selected,
        selected_counts,
        selected_nodes,
        used_tokens,
        cross_used_tokens,
        cross_selected_refs,
        entity_bridge_selected_refs,
        selected_cross_sessions,
        dropped_over_budget,
        dropped_cross_budget,
        dropped_cross_session_cap,
        dropped_cross_candidate_cap,
        dropped_low_score,
        dropped_policy_ref,
        dropped_duplicate_ref,
    }
}
