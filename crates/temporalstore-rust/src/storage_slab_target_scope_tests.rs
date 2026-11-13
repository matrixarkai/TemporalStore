// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::cell::Cell;

fn read(target: u64, calls: &Cell<usize>) -> Result<u64, String> {
    try_scoped_slab_target_from_getter(|name| {
        calls.set(calls.get() + 1);
        match name {
            TS_BLOCK_SLAB_TARGET_BYTES => Some(target.to_string()),
            TS_STREAM_MAX_BLOB_SIZE => Some("1024".into()),
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME => None,
            _ => panic!("unrelated setting read"),
        }
    })
}

#[test]
fn slab_scope_is_lazy_and_refreshes_between_operations() {
    let calls = Cell::new(0);
    {
        let _scope = SlabTargetScope::new();
        assert_eq!(calls.get(), 0);
        assert_eq!(read(2048, &calls).unwrap(), 2048);
        let captured = calls.get();
        assert!(captured > 0);
        assert_eq!(read(4096, &calls).unwrap(), 2048);
        assert_eq!(calls.get(), captured);
    }
    let _scope = SlabTargetScope::new();
    assert_eq!(read(4096, &calls).unwrap(), 4096);
}

#[test]
fn nested_slab_scopes_restore_the_callers_capture() {
    let calls = Cell::new(0);
    let _outer = SlabTargetScope::new();
    assert_eq!(read(2048, &calls).unwrap(), 2048);
    {
        let _inner = SlabTargetScope::new();
        assert_eq!(read(4096, &calls).unwrap(), 4096);
    }
    assert_eq!(read(8192, &calls).unwrap(), 2048);
}

#[test]
fn failed_validation_does_not_fill_the_scope() {
    let calls = Cell::new(0);
    {
        let _scope = SlabTargetScope::new();
        assert!(read(MAX_BLOCK_SLAB_TARGET_BYTES + 1, &calls).is_err());
        assert_eq!(read(2048, &calls).unwrap(), 2048);
    }
    let _scope = SlabTargetScope::new();
    assert!(read(MAX_BLOCK_SLAB_TARGET_BYTES + 1, &calls).is_err());
}

#[test]
fn slab_scope_unwinds_without_leaking_cached_settings() {
    let calls = Cell::new(0);
    let _outer = SlabTargetScope::new();
    assert_eq!(read(2048, &calls).unwrap(), 2048);
    assert!(std::panic::catch_unwind(|| {
        let _inner = SlabTargetScope::new();
        assert_eq!(read(4096, &Cell::new(0)).unwrap(), 4096);
        panic!("operation failed");
    })
    .is_err());
    assert_eq!(read(8192, &calls).unwrap(), 2048);
}

#[test]
fn unscoped_slab_reads_remain_live_and_threads_are_independent() {
    let calls = Cell::new(0);
    assert_eq!(read(2048, &calls).unwrap(), 2048);
    assert_eq!(read(4096, &calls).unwrap(), 4096);
    let _scope = SlabTargetScope::new();
    assert_eq!(read(8192, &calls).unwrap(), 8192);
    std::thread::spawn(|| {
        let _scope = SlabTargetScope::new();
        assert_eq!(read(16384, &Cell::new(0)).unwrap(), 16384);
    })
    .join()
    .unwrap();
    assert_eq!(read(32768, &calls).unwrap(), 8192);
}

#[test]
fn scoped_slab_targets_keep_alias_precedence_and_blob_floor() {
    for (current, previous, blob, expected) in [
        (None, Some("4096"), "1024", 4096),
        (Some(" "), Some("4096"), "1024", 4096),
        (Some("2048"), Some("4096"), "8192", 8192),
        (
            Some("invalid"),
            Some("4096"),
            "1024",
            DEFAULT_BLOCK_SLAB_TARGET_BYTES,
        ),
        (Some("0"), None, "1024", 1024),
    ] {
        let _scope = SlabTargetScope::new();
        let value = try_scoped_slab_target_from_getter(|name| match name {
            TS_BLOCK_SLAB_TARGET_BYTES => current.map(String::from),
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME => previous.map(String::from),
            TS_STREAM_MAX_BLOB_SIZE => Some(blob.into()),
            _ => panic!("unrelated setting read"),
        })
        .unwrap();
        assert_eq!(value, expected);
    }
}

#[test]
fn engine_batch_captures_one_slab_target_and_releases_it() {
    use crate::types::{BatchExecuteRequest, Command, ExecuteRequest, FeaturePoint};

    let dir = tempfile::tempdir().unwrap();
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        4096,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let before = SLAB_TARGET_CAPTURES.with(Cell::get);
    for generation in 1..=2 {
        let result = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands: (0..16)
                .map(|key| Command::FeatureAppend {
                    key: format!("feature-{key}"),
                    points: vec![FeaturePoint {
                        timestamp_ms: generation,
                        value: vec![generation as u8; 48],
                    }],
                })
                .collect(),
        });
        assert!(result.status.ok, "{:?}", result.status);
        assert_eq!(
            SLAB_TARGET_CAPTURES.with(Cell::get),
            before + generation as usize
        );
        assert_eq!(SCOPED_SLAB_TARGET.with(Cell::get), None);
    }
    engine.cache().invalidate_shard(1).unwrap();
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "feature-0".into(),
            start_ms: 0,
            end_ms: 3,
            count: None,
        },
    });
    assert!(result.status.ok);
    assert!(
        matches!(result.response, crate::types::CommandResponse::FeaturePoints { points } if points.len() == 2)
    );
}
