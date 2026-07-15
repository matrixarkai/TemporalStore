use std::collections::HashMap;

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::record_node_hash;

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
