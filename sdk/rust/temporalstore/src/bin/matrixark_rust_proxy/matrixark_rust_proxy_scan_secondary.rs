// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::{
    passes_secondary_groups, record_index_terms, record_node_hash, record_ref_hash,
};

pub(crate) struct SecondaryFilterResult {
    pub(crate) records: Vec<Value>,
    pub(crate) secondary_dropped: u64,
    pub(crate) secondary_matched: u64,
}

pub(crate) fn apply_secondary_prefilter(
    records: Vec<Value>,
    secondary_groups: &[Vec<String>],
) -> SecondaryFilterResult {
    if secondary_groups.is_empty() {
        return SecondaryFilterResult {
            records,
            secondary_dropped: 0,
            secondary_matched: 0,
        };
    }

    let (index_terms_by_batch, index_terms_by_node, index_terms_by_ref) =
        build_secondary_index_terms(&records);
    let mut secondary_dropped = 0_u64;
    let mut secondary_matched = 0_u64;
    let records = records
        .into_iter()
        .filter(|record| {
            let terms = record_index_terms(
                record,
                &index_terms_by_batch,
                &index_terms_by_node,
                &index_terms_by_ref,
            );
            if !terms.is_empty() && !passes_secondary_groups(&terms, secondary_groups) {
                secondary_dropped += 1;
                return false;
            }
            if !terms.is_empty() {
                secondary_matched += 1;
            }
            true
        })
        .collect();

    SecondaryFilterResult {
        records,
        secondary_dropped,
        secondary_matched,
    }
}

type SecondaryIndexTerms = (
    HashMap<String, HashSet<String>>,
    HashMap<u64, HashSet<String>>,
    HashMap<String, HashSet<String>>,
);

fn build_secondary_index_terms(records: &[Value]) -> SecondaryIndexTerms {
    let mut index_terms_by_batch: HashMap<String, HashSet<String>> = HashMap::new();
    let mut index_terms_by_node: HashMap<u64, HashSet<String>> = HashMap::new();
    let mut index_terms_by_ref: HashMap<String, HashSet<String>> = HashMap::new();
    for record in records {
        if record.get("record_type").and_then(Value::as_str) != Some("context_index") {
            continue;
        }
        let Some(index_name) = record
            .get("index_name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
            index_terms_by_batch
                .entry(batch.to_string())
                .or_default()
                .insert(index_name.to_string());
        }
        if let Some(ref_hash) = record_ref_hash(record) {
            index_terms_by_ref
                .entry(ref_hash)
                .or_default()
                .insert(index_name.to_string());
        } else if let Some(node_hash) = record_node_hash(record) {
            index_terms_by_node
                .entry(node_hash)
                .or_default()
                .insert(index_name.to_string());
        }
    }
    (
        index_terms_by_batch,
        index_terms_by_node,
        index_terms_by_ref,
    )
}
