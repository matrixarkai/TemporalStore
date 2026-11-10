// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::engine::{state::SIBLING_SCAN_CALLS, TemporalEngine};
use crate::types::{Command, CommandResponse, ExecuteRequest};
use std::sync::atomic::Ordering;

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

#[test]
fn hash_batch_ordinals_scan_once_and_preserve_rewrites() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = "catalog".to_string();
    let entries = (0..128)
        .map(|index| (format!("field-{index:04}"), vec![index as u8]))
        .collect::<Vec<_>>();
    let before = SIBLING_SCAN_CALLS.load(Ordering::Relaxed);
    run(&engine, Command::HashMultiSet { key: key.clone(), entries: entries.clone() });
    assert_eq!(SIBLING_SCAN_CALLS.load(Ordering::Relaxed) - before, 1);
    let before = SIBLING_SCAN_CALLS.load(Ordering::Relaxed);
    run(&engine, Command::HashMultiSet { key: key.clone(), entries: entries.clone() });
    assert_eq!(SIBLING_SCAN_CALLS.load(Ordering::Relaxed) - before, 0);
    for (field, value) in entries {
        assert_eq!(run(&engine, Command::HashGet { key: key.clone(), field }),
            CommandResponse::Bytes { value: Some(value) });
    }
    let before = SIBLING_SCAN_CALLS.load(Ordering::Relaxed);
    run(&engine, Command::HashMultiSet {
        key: key.clone(),
        entries: vec![("new".into(), vec![1]), ("new".into(), vec![2]), ("next".into(), vec![3])],
    });
    assert_eq!(SIBLING_SCAN_CALLS.load(Ordering::Relaxed) - before, 1);
    assert_eq!(run(&engine, Command::HashGet { key, field: "new".into() }),
        CommandResponse::Bytes { value: Some(vec![2]) });
}
