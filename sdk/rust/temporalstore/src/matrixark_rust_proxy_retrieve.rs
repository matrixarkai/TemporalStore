use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_candidates::{record_node_hash, record_ref_hash};
use crate::matrixark_rust_proxy_metrics::unix_ms;
use crate::matrixark_rust_proxy_native_pack::retrieve_context_pack_via_sdk_native;
use crate::matrixark_rust_proxy_pack::{
    candidate_text, context_class_name, is_serving_selected_ref_class, pack_ref_from_record,
    token_estimate,
};
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_retrieve_request::parse_retrieve_pack_request;
use crate::matrixark_rust_proxy_retrieve_scoring::score_retrieve_candidates;
use crate::matrixark_rust_proxy_scan::scan_matrixark_candidates;
use crate::matrixark_rust_proxy_scope::{
    parse_cross_session_policy, record_scope_string, session_scope_mode,
};

fn cross_session_key(record: &Value) -> String {
    record_scope_string(record, "session_id")
        .or_else(|| record_scope_string(record, "scope_key"))
        .or_else(|| record_node_hash(record).map(|node| format!("node:{node}")))
        .unwrap_or_else(|| "unknown_cross_session".to_string())
}

pub(crate) fn retrieve_context_pack_native(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    let use_sdk_native = std::env::var("MATRIXARK_RUST_PROXY_DISABLE_SDK_NATIVE_PACK")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(true);
    if use_sdk_native {
        match retrieve_context_pack_via_sdk_native(client, command) {
            Ok(response) => return Ok(response),
            Err(err) => {
                if std::env::var("MATRIXARK_RUST_PROXY_DISABLE_LEGACY_PACK_FALLBACK")
                    .map(|value| {
                        matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "1" | "true" | "yes"
                        )
                    })
                    .unwrap_or(false)
                {
                    return Err(err);
                }
            }
        }
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
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let mut scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": request.get("question_type").cloned().unwrap_or_else(|| json!("fact")),
        "selected_ref_counts": selected_counts,
        "remote_context_refs": selected,
        "selected_refs": selected,
        "dropped_refs": {
            "over_budget": dropped_over_budget,
            "cross_session_budget": dropped_cross_budget,
            "cross_session_session_cap": dropped_cross_session_cap,
            "cross_session_candidate_cap": dropped_cross_candidate_cap,
            "low_score": dropped_low_score,
            "duplicate_ref": dropped_duplicate_ref,
            "policy_ref": dropped_policy_ref,
            "reason_counts": {
                "over_budget": dropped_over_budget,
                "cross_session_budget": dropped_cross_budget,
                "cross_session_session_cap": dropped_cross_session_cap,
                "cross_session_candidate_cap": dropped_cross_candidate_cap,
                "low_score": dropped_low_score,
                "duplicate_ref": dropped_duplicate_ref,
                "policy_ref": dropped_policy_ref
            }
        },
        "used_context_tokens": used_tokens,
        "used_remote_context_tokens": used_tokens,
        "remote_context_budget_tokens": remote_budget,
        "requested_max_context_tokens": request.get("max_context_tokens").cloned().unwrap_or_else(|| json!(remote_budget)),
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
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first"
            },
            "session_continuity": {
                "mode": scan_command.scope.as_ref().map(session_scope_mode).unwrap_or("only"),
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": selected.iter().filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session")).count(),
                "cross_session_selected_ref_count": cross_selected_refs,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs
            },
            "cross_session": {
                "enabled": cross_policy.enabled,
                "mode": if cross_policy.enabled { "prefer" } else { "disabled" },
                "budget_ratio": cross_policy.budget_ratio,
                "max_budget_ratio": cross_policy.max_budget_ratio,
                "budget_tokens": cross_policy.budget_tokens,
                "remote_budget_tokens": remote_budget,
                "max_budget_tokens": cross_policy.max_budget_tokens,
                "max_sessions": cross_policy.max_sessions,
                "max_candidates": cross_policy.max_candidates,
                "min_score": cross_policy.min_score,
                "raw_evidence_min_score": cross_policy.raw_evidence_min_score,
                "parallelism": cross_policy.parallelism,
                "selected_tokens": cross_used_tokens,
                "selected_ref_count": cross_selected_refs,
                "selected_session_count": selected_cross_sessions.len() as u64,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs,
                "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
            },
            "tree_traversal": {
                "enabled": true,
                "native_backend": true,
                "fallback_to_flat": false,
                "selected_node_count": selected_nodes.len() as u64,
                "selected_leaf_count": selected_nodes.len() as u64,
                "summary_embeddings": ["node_l0", "node_l1"]
            },
            "secondary_index_filter": {
                "enabled": true,
                "native_backend": true,
                "applied_before_embedding_scoring": true,
                "matched_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_matched_candidate_count")).cloned().unwrap_or_else(|| json!(0)),
                "dropped_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_dropped_candidate_count")).cloned().unwrap_or_else(|| json!(0))
            }
        },
        "quality_warnings": []
    });
    let scan_dropped_count = scan_stats
        .get("dropped_by_type")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + scan_stats
            .get("dropped_by_scope")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("selected_node_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("secondary_index_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
    let dropped_ref_count = dropped_over_budget
        + dropped_cross_budget
        + dropped_cross_session_cap
        + dropped_cross_candidate_cap
        + dropped_policy_ref
        + dropped_duplicate_ref
        + scan_dropped_count;
    let scan_cache_hit = scan_stats
        .get("native_filtered_scan_cache_hit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || scan_stats
            .get("native_scan_record_cache_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(json!({
        "ok": true,
        "count": selected.len(),
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scan_stats.get("scanned_records").and_then(Value::as_u64).unwrap_or(0),
        "cache_hit": scan_cache_hit,
        "cache_hit_used": scan_cache_hit,
        "selected_ref_count": selected.len(),
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": dropped_duplicate_ref,
        "context_pack": pack,
        "scan_stats": scan_stats
    }))
}
