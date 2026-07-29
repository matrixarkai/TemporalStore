use std::collections::HashSet;

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_scope::json_field;

pub(crate) struct RetrievePackRequest {
    pub(crate) request: Value,
    pub(crate) query: String,
    pub(crate) query_terms: HashSet<String>,
    pub(crate) remote_budget: u64,
    pub(crate) max_refs: u64,
    pub(crate) max_global_candidates: u64,
    pub(crate) min_similarity_score: f64,
    pub(crate) budget_fill_policy: String,
    pub(crate) question_type: String,
    pub(crate) scan_command: Command,
}

fn contains_any(lower: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| lower.contains(needle))
}

pub(crate) fn infer_native_question_type(query: &str) -> String {
    let lower = query.to_ascii_lowercase();
    if contains_any(
        &lower,
        &[
            "when",
            "what date",
            "which date",
            "yesterday",
            "tomorrow",
            "last week",
            "next week",
            "before",
            "after",
            "as of",
            "valid as of",
        ],
    ) {
        return "date".to_string();
    }
    if contains_any(
        &lower,
        &[
            "current",
            "currently",
            "latest",
            "now",
            "still",
            "today",
            "valid",
            "status",
            "preference",
            "prefer",
            "likes",
            "where does",
            "where is",
        ],
    ) {
        return "current_state".to_string();
    }
    if contains_any(
        &lower,
        &["why", "reason", "because", "feel", "felt", "emotion", "happy", "sad", "angry", "worried", "excited"],
    ) {
        return "why_emotion".to_string();
    }
    if contains_any(
        &lower,
        &["overview", "summarize", "summary", "explore", "broad", "what is in", "what do we know", "topics", "map", "inventory"],
    ) {
        return "broad_exploration".to_string();
    }
    if contains_any(
        &lower,
        &["evidence", "quote", "exactly", "what did ", "conversation", "dialogue", "message"],
    ) {
        return "evidence".to_string();
    }
    if contains_any(
        &lower,
        &["procedure", "step", "steps", "how to", "troubleshoot", "debug", "rollback", "runbook", "playbook", "checklist", "fix", "remediate", "mitigate"],
    ) {
        return "procedure".to_string();
    }
    if contains_any(
        &lower,
        &["both", "together", "across", "between", "compare", "combine", "sessions", "multi-hop", "multi session", "multi-session"],
    ) {
        return "multi_hop".to_string();
    }
    "fact".to_string()
}

pub(crate) fn parse_retrieve_pack_request(command: &Command) -> RetrievePackRequest {
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query_terms: HashSet<String> = query
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| part.len() > 2)
        .map(str::to_string)
        .collect();
    let remote_budget = json_field(&request, &["local_budget", "remote_budget_tokens"])
        .and_then(Value::as_u64)
        .or_else(|| request.get("max_context_tokens").and_then(Value::as_u64))
        .unwrap_or(4000);
    let max_refs = json_field(&request, &["ranking", "max_selected_refs"])
        .and_then(Value::as_u64)
        .unwrap_or(24)
        .max(1);
    let max_global_candidates = json_field(&request, &["ranking", "max_global_candidates"])
        .and_then(Value::as_u64)
        .unwrap_or(512)
        .max(1);
    let min_similarity_score = json_field(&request, &["ranking", "min_similarity_score"])
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let budget_fill_policy = json_field(&request, &["ranking", "budget_fill_policy"])
        .and_then(Value::as_str)
        .filter(|policy| *policy == "quality_first" || *policy == "force_fill")
        .unwrap_or("quality_first")
        .to_string();
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| infer_native_question_type(&query));
    let mut scan_command = command.clone();
    scan_command.scope = request
        .get("scope")
        .cloned()
        .or_else(|| command.scope.clone());
    scan_command.secondary_index_groups = request
        .get("secondary_index_groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|group| {
                    group
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .or_else(|| command.secondary_index_groups.clone());
    if scan_command
        .record_types
        .as_ref()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        scan_command.record_types = Some(vec![
            "context_compression_event".to_string(),
            "context_entity".to_string(),
            "context_event".to_string(),
            "context_segment".to_string(),
            "context_summary".to_string(),
            "resource_chunk".to_string(),
            "skill_section".to_string(),
            "context_index".to_string(),
        ]);
    }
    RetrievePackRequest {
        request,
        query,
        query_terms,
        remote_budget,
        max_refs,
        max_global_candidates,
        min_similarity_score,
        budget_fill_policy,
        question_type,
        scan_command,
    }
}
