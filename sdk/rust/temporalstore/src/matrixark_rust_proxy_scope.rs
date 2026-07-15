use serde_json::Value;

pub(crate) fn json_field<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    Some(current)
}

fn record_scope_value<'a>(record: &'a Value) -> Option<&'a Value> {
    if let Some(scope) = record.get("access_scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    if let Some(scope) =
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object())
    {
        return Some(scope);
    }
    if let Some(scope) = record.get("scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    json_field(record, &["envelope", "scope"]).filter(|value| value.is_object())
}

pub(crate) fn session_scope_mode(query: &Value) -> &str {
    match query
        .get("_session_scope")
        .or_else(|| query.get("session_scope"))
        .and_then(Value::as_str)
        .unwrap_or("only")
    {
        "prefer" | "preferred" | "soft" | "continuity" => "prefer",
        _ => "only",
    }
}

pub(crate) fn scope_matches_record(record: &Value, query_scope: Option<&Value>) -> bool {
    let Some(query) = query_scope.filter(|value| value.is_object()) else {
        return true;
    };
    let Some(record_scope) = record_scope_value(record) else {
        return true;
    };
    for key in [
        "scope_key",
        "account_id",
        "tenant_id",
        "user_id",
        "team",
        "project",
    ] {
        let Some(query_value) = query.get(key) else {
            continue;
        };
        if query_value.is_null() || query_value.as_str() == Some("") {
            continue;
        }
        if record_scope.get(key) != Some(query_value) && record.get(key) != Some(query_value) {
            return false;
        }
    }
    if session_scope_mode(query) == "only" {
        if let Some(query_session) = query.get("session_id").filter(|value| !value.is_null()) {
            if query_session.as_str() != Some("")
                && record_scope.get("session_id") != Some(query_session)
                && record.get("session_id") != Some(query_session)
            {
                return false;
            }
        }
    }
    true
}

pub(crate) fn record_scope_string(record: &Value, field: &str) -> Option<String> {
    for source in [record_scope_value(record), record.get("scope")] {
        if let Some(value) = source
            .and_then(|scope| scope.get(field))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            return Some(value.to_string());
        }
    }
    record
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

pub(crate) fn session_continuity_status(record: &Value, query_scope: Option<&Value>) -> String {
    let Some(query) = query_scope else {
        return "unscoped".to_string();
    };
    let Some(query_session) = query
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    else {
        return "unscoped".to_string();
    };
    if record_scope_string(record, "session_id").as_deref() == Some(query_session) {
        return "same_session".to_string();
    }
    let has_sessionish_scope = record_scope_string(record, "scope_key").is_some()
        || record_scope_string(record, "session_id").is_some();
    if has_sessionish_scope {
        "cross_session".to_string()
    } else {
        "unscoped".to_string()
    }
}

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
