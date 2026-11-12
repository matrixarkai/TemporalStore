// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn memory_add_receipt_accepts_zero_or_many_inferred_facts_per_source() {
    assert!(memory_add_ids(json!({"results":[]}), 1, true)
        .unwrap()
        .is_empty());
    assert_eq!(
        memory_add_ids(
            json!({"results":[
                {"id":"a","memory":"First fact"}, {"id":"b","memory":"Second fact"}
            ]}),
            1,
            true
        )
        .unwrap()
        .len(),
        2
    );
    let rows = (0..9)
        .map(|index| json!({"id":index.to_string(),"memory":"Fact"}))
        .collect::<Vec<_>>();
    assert!(memory_add_ids(json!({"results":rows}), 1, true).is_err());
}

#[test]
fn memory_add_receipt_requires_exact_explicit_counts_and_valid_unique_rows() {
    assert!(memory_add_ids(json!({"results":[]}), 1, false).is_err());
    for infer in [false, true] {
        assert!(memory_add_ids(json!({}), 1, infer).is_err());
        assert!(memory_add_ids(json!({"results":[{"memory":"Fact"}]}), 1, infer).is_err());
        assert!(memory_add_ids(json!({"results":[{"id":"a"}]}), 1, infer).is_err());
        assert!(memory_add_ids(
            json!({"results":[
                {"id":"a","memory":"Fact"}, {"id":"a","memory":"Duplicate"}
            ]}),
            2,
            infer
        )
        .is_err());
    }
}
