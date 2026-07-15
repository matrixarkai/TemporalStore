use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::matrixark_rust_proxy_scope::json_field;

pub(crate) fn record_ref_hash(record: &Value) -> Option<String> {
    for field in ["ref_hash", "chunk_hash", "section_hash", "skill_hash"] {
        if let Some(value) = record.get(field) {
            if let Some(number) = value.as_u64() {
                return Some(number.to_string());
            }
            if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
                return Some(text.to_string());
            }
        }
    }
    None
}

pub(crate) fn record_node_hash(record: &Value) -> Option<u64> {
    record.get("node_hash").and_then(Value::as_u64)
}

fn node_path_for_record(
    record: &Value,
    node_paths_by_hash: &HashMap<u64, Vec<String>>,
) -> Option<Vec<String>> {
    if let Some(path) = record.get("node_path").and_then(Value::as_array) {
        let values: Vec<String> = path
            .iter()
            .filter_map(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .collect();
        if !values.is_empty() {
            return Some(values);
        }
    }
    record_node_hash(record).and_then(|node_hash| node_paths_by_hash.get(&node_hash).cloned())
}

pub(crate) fn query_node_path_filters(query_scope: Option<&Value>) -> Vec<String> {
    let Some(scope) = query_scope.filter(|value| value.is_object()) else {
        return Vec::new();
    };
    ["team", "project"]
        .iter()
        .filter_map(|key| scope.get(*key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) fn node_path_matches_filters(
    record: &Value,
    filters: &[String],
    node_paths_by_hash: &HashMap<u64, Vec<String>>,
) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Some(path) = node_path_for_record(record, node_paths_by_hash) else {
        return false;
    };
    filters
        .iter()
        .all(|required| path.iter().any(|part| part == required))
}

pub(crate) fn record_index_terms(
    record: &Value,
    index_terms_by_batch: &HashMap<String, HashSet<String>>,
    index_terms_by_node: &HashMap<u64, HashSet<String>>,
    index_terms_by_ref: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    let mut terms = HashSet::new();
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
        if let Some(values) = index_terms_by_batch.get(&batch.to_string()) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(node_hash) = record_node_hash(record) {
        if let Some(values) = index_terms_by_node.get(&node_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(ref_hash) = record_ref_hash(record) {
        if let Some(values) = index_terms_by_ref.get(&ref_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    match record_type {
        "context_event" => {
            terms.insert("source_type:message".to_string());
            if let Some(event_type) =
                json_field(record, &["internal_extraction", "event_type"]).and_then(Value::as_str)
            {
                if !event_type.is_empty() {
                    terms.insert(format!("event_type:{event_type}"));
                }
            }
        }
        "context_entity" => {
            if let Some(entity_type) = record.get("entity_type").and_then(Value::as_str) {
                if !entity_type.is_empty() {
                    terms.insert(format!("entity_type:{entity_type}"));
                }
            }
        }
        "resource_chunk" => {
            terms.insert("source_type:resource".to_string());
            if let Some(resource_type) = record.get("resource_type").and_then(Value::as_str) {
                if !resource_type.is_empty() {
                    terms.insert(format!("resource_type:{resource_type}"));
                }
            }
        }
        "skill_manifest" | "skill_section" => {
            terms.insert("source_type:skill".to_string());
            terms.insert("resource_type:skill".to_string());
            if record_type == "skill_manifest" {
                if let Some(name) = record.get("name").and_then(Value::as_str) {
                    if !name.is_empty() {
                        terms.insert(format!("skill_name:{}", name.to_ascii_lowercase()));
                    }
                }
            }
        }
        _ => {}
    }
    terms
}

pub(crate) fn passes_secondary_groups(terms: &HashSet<String>, groups: &[Vec<String>]) -> bool {
    if groups.is_empty() {
        return true;
    }
    let mode_any = groups.len() > 1;
    if mode_any {
        groups
            .iter()
            .any(|group| group.iter().any(|term| terms.contains(term)))
    } else {
        groups
            .iter()
            .all(|group| group.iter().any(|term| terms.contains(term)))
    }
}
