// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::record_node_hash;

pub(crate) fn node_paths_by_hash(records: &[Value]) -> HashMap<u64, Vec<String>> {
    let mut paths_by_hash = HashMap::new();
    for record in records {
        if record.get("record_type").and_then(Value::as_str) != Some("context_node") {
            continue;
        }
        if let (Some(node_hash), Some(path)) = (
            record_node_hash(record),
            record.get("node_path").and_then(Value::as_array),
        ) {
            let values: Vec<String> = path
                .iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
                .collect();
            if !values.is_empty() {
                paths_by_hash.insert(node_hash, values);
            }
        }
    }
    paths_by_hash
}
