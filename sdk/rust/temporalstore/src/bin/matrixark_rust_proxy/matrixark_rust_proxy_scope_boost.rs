use serde_json::Value;

pub(crate) fn continuity_boost(record: &Value, context_class: &str, status: &str) -> f64 {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    match status {
        "same_session" => match record_type {
            "context_event" | "context_segment" => 0.16,
            "context_summary" => 0.12,
            "context_entity" => 0.10,
            _ => 0.08,
        },
        "cross_session" => {
            if record_type == "context_entity" || context_class == "resource_fact" {
                0.11
            } else if matches!(
                record_type,
                "context_event" | "context_segment" | "context_compression_event"
            ) {
                0.06
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

pub(crate) fn cross_session_rerank_boost(
    record: &Value,
    context_class: &str,
    status: &str,
    question_type: &str,
) -> f64 {
    if status != "cross_session" {
        return 0.0;
    }
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let has_citation = record.get("source_ref").is_some()
        || record.get("citation").is_some()
        || record.get("source_chunk_hash").is_some();
    match record_type {
        "context_entity" => {
            if matches!(question_type, "current_state" | "latest" | "multi_hop") {
                0.10
            } else {
                0.06
            }
        }
        "resource_chunk" if has_citation => 0.04,
        "context_event" | "context_segment"
            if matches!(
                question_type,
                "multi_hop" | "why_emotion" | "fact" | "evidence"
            ) =>
        {
            0.01
        }
        "context_compression_event" => 0.05,
        "context_summary" => {
            if question_type == "broad_exploration" {
                0.05
            } else {
                0.02
            }
        }
        _ if matches!(context_class, "resource_fact" | "resource_entity_fact") => {
            if has_citation {
                0.06
            } else {
                0.04
            }
        }
        _ => 0.0,
    }
}
