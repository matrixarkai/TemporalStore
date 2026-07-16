use std::collections::HashSet;

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_candidates::{record_node_hash, record_ref_hash};
use crate::matrixark_rust_proxy_scope::json_field;

pub(crate) fn candidate_text(record: &Value) -> String {
    for field in ["text", "content", "summary_text", "state", "observation"] {
        if let Some(text) = record.get(field).and_then(Value::as_str) {
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    if let Some(text) =
        json_field(record, &["internal_extraction", "observation"]).and_then(Value::as_str)
    {
        if !text.is_empty() {
            return text.to_string();
        }
    }
    String::new()
}

pub(crate) fn token_estimate(text: &str) -> u64 {
    let words = text.split_whitespace().count() as u64;
    words.max((text.len() as u64 + 3) / 4).max(1)
}

pub(crate) fn sparse_query_score(query_terms: &HashSet<String>, text: &str) -> f64 {
    if query_terms.is_empty() || text.is_empty() {
        return 0.0;
    }
    let lower = text.to_ascii_lowercase();
    let hits = query_terms
        .iter()
        .filter(|term| lower.contains(term.as_str()))
        .count() as f64;
    (hits / query_terms.len() as f64).clamp(0.0, 1.0)
}

pub(crate) fn context_class_name(record: &Value) -> String {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if record_type == "context_event" {
        let classification = record
            .get("classification")
            .and_then(Value::as_str)
            .unwrap_or("");
        let event_type = record
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if classification == "resource_fact" || event_type.starts_with("resource_") {
            return "resource_fact".to_string();
        }
        return "event".to_string();
    }
    match record_type {
        "context_entity" => "entity".to_string(),
        "context_segment" => "segment".to_string(),
        "context_summary" => "summary".to_string(),
        "context_compression_event" => "compression".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn is_serving_selected_ref_class(context_class: &str) -> bool {
    matches!(context_class, "event" | "summary")
}

pub(crate) fn pack_ref_from_record(
    record: &Value,
    score: f64,
    reason: &str,
    session_continuity: &str,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
) -> Value {
    let ref_type = context_class_name(record);
    let text = candidate_text(record);
    let continuity_reason = match session_continuity {
        "same_session" => "same-session continuity",
        "cross_session" => "cross-session memory bridge",
        _ => "session-neutral context",
    };
    json!({
        "ref_type": ref_type,
        "ref_hash": record_ref_hash(record).unwrap_or_else(|| record.get("record_id").and_then(Value::as_str).unwrap_or("").to_string()),
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "text": text,
        "token_estimate": token_estimate(&candidate_text(record)),
        "score": (score * 1000000.0).round() / 1000000.0,
        "session_continuity": session_continuity,
        "continuity_boost": (continuity_boost_value * 1000000.0).round() / 1000000.0,
        "cross_session_rerank_boost": (cross_session_rerank_boost_value * 1000000.0).round() / 1000000.0,
        "continuity_reason": continuity_reason,
        "selection_reason": reason,
        "source_ref": record.get("source_ref").cloned().unwrap_or(Value::Null),
    })
}
