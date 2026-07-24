use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::record_node_hash;
use crate::matrixark_rust_proxy_cross_session_budget::default_cross_session_budget_ratio;
use crate::matrixark_rust_proxy_scope::record_scope_string;
use crate::matrixark_rust_proxy_scope::session_scope_mode;

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

pub(crate) fn cross_session_key(record: &Value) -> String {
    record_scope_string(record, "session_id")
        .or_else(|| record_scope_string(record, "scope_key"))
        .or_else(|| record_node_hash(record).map(|node| format!("node:{node}")))
        .unwrap_or_else(|| "unknown_cross_session".to_string())
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
    let mut budget_ratio = default_cross_session_budget_ratio(question_type);
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
        .unwrap_or(8192);
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
