// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(root: &std::path::Path, cache: &str, asynchronous: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(crate::control::SetConfigRequest {
        shard_id: 1,
        config: crate::control::Config {
            version: 2,
            async_storage: asynchronous,
            ..Default::default()
        },
    }).ok);
    engine
}

fn put(engine: &TemporalEngine, key: &str, value: &str) {
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.into(), value: value.as_bytes().to_vec() },
    });
    assert!(result.status.ok, "{:?}", result.status);
}

fn seed(engine: &TemporalEngine) {
    for index in 0..8 {
        put(engine, &format!("key-{index}"), &format!("value-{index}"));
    }
    assert_eq!(engine.registration_count_for_test(1), 8);
}

fn check(engine: &TemporalEngine, updated: bool) {
    engine.cache().invalidate_shard(1).unwrap();
    for index in 0..8 {
        let expected = if updated && index == 0 { "new-zero".into() } else { format!("value-{index}") };
        let result = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("key-{index}") },
        });
        assert!(result.status.ok, "{:?}", result.status);
        assert_eq!(result.response, CommandResponse::Bytes { value: Some(expected.into_bytes()) });
    }
    if updated {
        assert_eq!(engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: "late".into() },
        }).response, CommandResponse::Bytes { value: Some(b"late-value".to_vec()) });
    }
}

#[test]
fn catalog_publication_retires_only_durable_fallbacks() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), "cache", false);
    seed(&engine);
    assert!(!crate::wal_record::is_wal_resident(
        engine.string_block_address(1, "key-0").unwrap().block_slab_id(),
    ));
    let report = engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0).unwrap();
    assert_eq!(engine.registration_count_for_test(1), 0);
    assert_eq!(report.wal_retention_floor, None);
    assert!(report.wal_records_removed + report.wal_segments_dropped > 0);
    assert!(engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks.is_empty()));
    check(&engine, false);
    drop(engine);
    check(&open(dir.path(), "restart-cache", false), false);
}

#[test]
fn failed_catalog_publication_keeps_fallback_registrations() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), "cache", false);
    seed(&engine);
    let path = engine.index_path(1);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let saved = path.with_extension("before-failed-dump");
    let had_index = path.exists();
    if had_index {
        std::fs::rename(&path, &saved).unwrap();
    }
    std::fs::create_dir(&path).unwrap();
    assert!(!engine.dump_index_catalog(1));
    assert_eq!(engine.registration_count_for_test(1), 8);
    assert_eq!(engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks.len()), 8);
    check(&engine, false);
    std::fs::remove_dir(&path).unwrap();
    if had_index {
        std::fs::rename(saved, &path).unwrap();
    }
    assert!(engine.dump_index_catalog(1));
    assert_eq!(engine.registration_count_for_test(1), 0);
}

#[test]
fn catalog_publication_keeps_live_wal_only_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), "cache", true);
    seed(&engine);
    let report = engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0).unwrap();
    assert_eq!(engine.registration_count_for_test(1), 8);
    assert_eq!(report.wal_retention_floor, Some(1));
    assert_eq!(report.wal_records_removed, 0);
    assert_eq!(report.wal_segments_dropped, 0);
    assert_eq!(engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks.len()), 8);
    check(&engine, false);
    drop(engine);
    check(&open(dir.path(), "restart-cache", true), false);
}

#[test]
fn a_write_during_the_catalog_barrier_keeps_its_fallback() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), "cache", false);
    seed(&engine);
    let writer = engine.clone();
    engine.block_store.with_after_next_sync_for_test(
        move || {
            put(&writer, "key-0", "new-zero");
            put(&writer, "late", "late-value");
        },
        || assert!(engine.dump_index_catalog(1)),
    );
    assert_eq!(engine.registration_count_for_test(1), 9);
    check(&engine, true);
    let report = engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0).unwrap();
    assert_eq!(engine.registration_count_for_test(1), 0);
    assert_eq!(report.wal_retention_floor, None);
    drop(engine);
    check(&open(dir.path(), "restart-cache", false), true);
}
