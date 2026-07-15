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

#[derive(Clone, Debug)]
pub(crate) struct CrossSessionPolicy {
    pub(crate) enabled: bool,
    pub(crate) budget_ratio: f64,
    pub(crate) budget_tokens: u64,
    pub(crate) max_budget_ratio: f64,
    pub(crate) max_budget_tokens: u64,
    pub(crate) max_sessions: u64,
    pub(crate) max_candidates: u64,
    pub(crate) min_score: f64,
    pub(crate) raw_evidence_min_score: f64,
    pub(crate) min_entity_bridge_refs: u64,
    pub(crate) parallelism: u64,
}

pub(crate) fn parse_cross_session_policy(
    request: &Value,
    scope: Option<&Value>,
    remote_budget: u64,
    question_type: &str,
) -> CrossSessionPolicy {
    let default_enabled = scope.map(session_scope_mode) == Some("prefer") && remote_budget > 0;
    let config = request
        .get("cross_session")
        .filter(|value| value.is_object());
    let mut budget_ratio = if matches!(
        question_type,
        "current_state" | "latest" | "multi_hop" | "date"
    ) {
        0.20
    } else if matches!(question_type, "broad_exploration" | "evidence") {
        0.15
    } else {
        0.12
    };
    let max_budget_ratio = config
        .and_then(|cfg| cfg.get("max_budget_ratio"))
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    if budget_ratio > max_budget_ratio {
        budget_ratio = max_budget_ratio;
    }
    if let Some(value) = config
        .and_then(|cfg| cfg.get("budget_ratio"))
        .and_then(Value::as_f64)
    {
        budget_ratio = value.clamp(0.0, max_budget_ratio);
    }
    let max_budget_tokens = config
        .and_then(|cfg| cfg.get("max_budget_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(1536);
    let mut computed = (remote_budget as f64 * budget_ratio) as u64;
    if remote_budget >= 1200 && computed > 0 {
        computed = computed.max(256);
    }
    let enabled = config
        .and_then(|cfg| cfg.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(default_enabled)
        && default_enabled;
    let mut budget_tokens = config
        .and_then(|cfg| cfg.get("budget_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(computed);
    let mut max_sessions = config
        .and_then(|cfg| cfg.get("max_sessions"))
        .and_then(Value::as_u64)
        .unwrap_or(3);
    let mut max_candidates = config
        .and_then(|cfg| cfg.get("max_candidates"))
        .and_then(Value::as_u64)
        .unwrap_or(24);
    let mut min_score = config
        .and_then(|cfg| cfg.get("min_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let mut raw_evidence_min_score = config
        .and_then(|cfg| cfg.get("raw_evidence_min_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.45)
        .clamp(0.0, 1.0);
    let mut min_entity_bridge_refs = config
        .and_then(|cfg| cfg.get("min_entity_bridge_refs"))
        .and_then(Value::as_u64)
        .unwrap_or(2);
    let mut parallelism = config
        .and_then(|cfg| cfg.get("parallelism"))
        .and_then(Value::as_u64)
        .unwrap_or(4)
        .max(1);
    if !enabled {
        budget_tokens = 0;
        max_sessions = 0;
        max_candidates = 0;
        min_score = 0.0;
        raw_evidence_min_score = 0.0;
        min_entity_bridge_refs = 0;
        parallelism = 0;
    } else {
        let cap = if max_budget_tokens == 0 {
            remote_budget
        } else {
            max_budget_tokens
        };
        let mut ratio_cap = if max_budget_ratio > 0.0 {
            (remote_budget as f64 * max_budget_ratio) as u64
        } else {
            remote_budget
        };
        if ratio_cap == 0 && remote_budget > 0 && max_budget_ratio > 0.0 {
            ratio_cap = 1;
        }
        budget_tokens = budget_tokens.min(remote_budget).min(cap).min(ratio_cap);
    }
    CrossSessionPolicy {
        enabled,
        budget_ratio,
        budget_tokens,
        max_budget_ratio,
        max_budget_tokens,
        max_sessions,
        max_candidates,
        min_score,
        raw_evidence_min_score,
        min_entity_bridge_refs,
        parallelism,
    }
}
