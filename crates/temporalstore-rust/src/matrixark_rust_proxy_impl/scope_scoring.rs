// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Scope / session-continuity / cross-session scoring helpers, split from
// matrixark_rust_proxy_impl.rs. Textually include!d, so it shares the parent
// file's use-imports and flat scope; do not add use-statements or mod wrappers.

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

fn session_scope_mode(query: &Value) -> &str {
    match query
        .get("_session_scope")
        .or_else(|| query.get("session_scope"))
        .and_then(Value::as_str)
        .unwrap_or("prefer")
    {
        "only" | "strict" => "only",
        _ => "prefer",
    }
}

fn scope_key_explicit(scope: &Value, field: &str) -> bool {
    scope
        .get("_explicit_scope_keys")
        .and_then(Value::as_array)
        .map(|items| items.iter().any(|item| item.as_str() == Some(field)))
        .unwrap_or(false)
}

fn parse_scope_key(scope_key: &str) -> HashMap<String, u64> {
    scope_key
        .split('|')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            if key.is_empty() || value.is_empty() {
                return None;
            }
            value
                .parse::<u64>()
                .ok()
                .map(|parsed| (key.to_string(), parsed))
        })
        .collect()
}

fn scoped_string_value(scope: Option<&Value>, field: &str) -> Option<String> {
    scope
        .filter(|value| value.is_object())
        .and_then(|value| value.get(field))
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn record_scope_sources(record: &Value) -> [Option<&Value>; 5] {
    [
        Some(record),
        record.get("access_scope").filter(|value| value.is_object()),
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object()),
        record.get("scope").filter(|value| value.is_object()),
        json_field(record, &["envelope", "scope"]).filter(|value| value.is_object()),
    ]
}

fn candidate_scope_key(record: &Value) -> String {
    for source in record_scope_sources(record) {
        if let Some(value) = scoped_string_value(source, "scope_key") {
            return value;
        }
    }
    String::new()
}

fn scope_key_matches_query(record_scope_key: &str, query_scope: &Value) -> bool {
    if record_scope_key.is_empty() {
        return true;
    }
    let parts = parse_scope_key(record_scope_key);
    if let Some(tenant_hash) = query_scope.get("tenant_hash").and_then(Value::as_u64) {
        if tenant_hash != 0 && parts.get("t").copied() != Some(tenant_hash) {
            return false;
        }
    }
    if scope_key_explicit(query_scope, "user_id") {
        if let Some(user_hash) = query_scope.get("user_hash").and_then(Value::as_u64) {
            if user_hash != 0 && parts.get("u").copied() != Some(user_hash) {
                return false;
            }
        }
    }
    if scope_key_explicit(query_scope, "session_id") && session_scope_mode(query_scope) == "only" {
        if let Some(session_hash) = query_scope.get("session_hash").and_then(Value::as_u64) {
            if session_hash != 0 && parts.get("s").copied() != Some(session_hash) {
                return false;
            }
        }
    }
    true
}

fn scope_matches_record(record: &Value, query_scope: Option<&Value>) -> bool {
    let Some(query) = query_scope.filter(|value| value.is_object()) else {
        return true;
    };
    if !scope_key_matches_query(&candidate_scope_key(record), query) {
        return false;
    }
    for key in [
        "scope_key",
        "account_id",
        "tenant_id",
        "user_id",
        "session_id",
        "team",
        "project",
        "agent_name",
    ] {
        if key == "scope_key" {
            continue;
        }
        if matches!(key, "account_id" | "tenant_id" | "user_id" | "session_id")
            && !scope_key_explicit(query, key)
        {
            continue;
        }
        if key == "session_id" && session_scope_mode(query) == "prefer" {
            continue;
        }
        if matches!(key, "team" | "project" | "agent_name") && !scope_key_explicit(query, key) {
            continue;
        }
        let Some(query_value) = query.get(key) else {
            continue;
        };
        if query_value.is_null() || query_value.as_str() == Some("") {
            continue;
        }
        let actual = record_scope_sources(record)
            .into_iter()
            .find_map(|source| scoped_string_value(source, key));
        if actual
            .as_deref()
            .is_some_and(|value| Some(value) != query_value.as_str())
        {
            return false;
        }
    }
    true
}

fn record_scope_string(record: &Value, field: &str) -> Option<String> {
    for source in record_scope_sources(record) {
        if let Some(value) = scoped_string_value(source, field) {
            return Some(value);
        }
    }
    None
}

fn session_continuity_status(record: &Value, query_scope: Option<&Value>) -> String {
    if let Some(explicit) = record
        .get("session_continuity")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        return explicit.to_string();
    }
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
    if let Some(query_scope_key) = query
        .get("scope_key")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        if record_scope_string(record, "scope_key").as_deref() == Some(query_scope_key) {
            return "same_session".to_string();
        }
    }
    let has_sessionish_scope = record_scope_string(record, "scope_key").is_some()
        || record_scope_string(record, "session_id").is_some();
    if has_sessionish_scope {
        "cross_session".to_string()
    } else {
        "unscoped".to_string()
    }
}

fn continuity_boost(record: &Value, context_class: &str, status: &str) -> f64 {
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

fn cross_session_rerank_boost(
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
            if matches!(
                question_type,
                "current_state" | "latest" | "multi_hop" | "profile_memory"
            ) {
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

fn type_priority_boost(record: &Value, context_class: &str, question_type: &str) -> f64 {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    match record_type {
        "skill_section" => {
            if matches!(question_type, "procedure" | "evidence") {
                0.42
            } else {
                0.34
            }
        }
        "resource_chunk" => {
            if matches!(question_type, "evidence" | "fact") {
                0.20
            } else {
                0.12
            }
        }
        "context_entity" => {
            if matches!(question_type, "current_state" | "latest" | "profile_memory") {
                0.24
            } else {
                0.12
            }
        }
        "context_event" | "context_segment" => 0.10,
        "context_summary" => {
            if matches!(question_type, "broad" | "exploration") {
                0.12
            } else {
                0.0
            }
        }
        _ => {
            if context_class == "resource_fact" {
                0.18
            } else {
                0.0
            }
        }
    }
}

fn cross_session_key(record: &Value) -> String {
    record_scope_string(record, "session_id")
        .or_else(|| record_scope_string(record, "scope_key"))
        .or_else(|| record_node_hash(record).map(|node| format!("node:{node}")))
        .unwrap_or_else(|| "unknown_cross_session".to_string())
}

#[derive(Clone, Debug)]
struct CrossSessionPolicy {
    enabled: bool,
    budget_ratio: f64,
    budget_tokens: u64,
    max_budget_ratio: f64,
    max_budget_tokens: u64,
    max_sessions: u64,
    max_candidates: u64,
    min_score: f64,
    raw_evidence_min_score: f64,
    min_entity_bridge_refs: u64,
    parallelism: u64,
}

fn parse_cross_session_policy(
    request: &Value,
    scope: Option<&Value>,
    remote_budget: u64,
    question_type: &str,
) -> CrossSessionPolicy {
    let default_enabled = scope.map(session_scope_mode) == Some("prefer") && remote_budget > 0;
    let config = request
        .get("cross_session")
        .filter(|value| value.is_object());
    let profile_memory_query = question_type == "profile_memory";
    let mut budget_ratio = if profile_memory_query {
        0.30
    } else if matches!(
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
        .unwrap_or(if profile_memory_query { 0.35 } else { 0.20 })
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
        .unwrap_or(if profile_memory_query { 12288 } else { 8192 });
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
        .unwrap_or(if profile_memory_query { 8 } else { 3 });
    let mut max_candidates = config
        .and_then(|cfg| cfg.get("max_candidates"))
        .and_then(Value::as_u64)
        .unwrap_or(if profile_memory_query { 48 } else { 24 });
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
        .unwrap_or(if profile_memory_query { 3 } else { 2 });
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
