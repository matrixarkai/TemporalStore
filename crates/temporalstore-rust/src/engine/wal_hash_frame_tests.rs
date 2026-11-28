// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::TemporalEngine;
use crate::types::{
    BatchHashMultiGetRequest, BatchHashMultiSetRequest, BatchHashMultiSetRow,
    BatchHashMultiSetValue, Command, ExecuteRequest,
};

#[test]
fn ordered_physical_coverage_uses_no_index_set() {
    use super::super::PhysicalIndexCoverage;
    let bindings = vec![((1, (1, 0, 8)), 0), ((2, (1, 8, 8)), 1)];
    let coverage = PhysicalIndexCoverage::new(&bindings, 2);
    assert!(matches!(coverage, PhysicalIndexCoverage::All(2)));
    assert!(coverage.covers_all(2));
    assert!(coverage.contains(0) && coverage.contains(1));
    assert!(!coverage.contains(2));
}

#[test]
fn sparse_and_shuffled_physical_coverage_keeps_exact_indices() {
    use super::super::PhysicalIndexCoverage;
    for (indices, count, complete) in [
        (vec![1, 0], 2, true),
        (vec![0, 0], 2, false),
        (vec![1], 2, false),
        (vec![0, 3], 2, false),
        (vec![], 2, false),
    ] {
        let bindings: Vec<_> = indices.iter().map(|index| ((1, (1, 0, 8)), *index)).collect();
        let coverage = PhysicalIndexCoverage::new(&bindings, count);
        assert_eq!(coverage.covers_all(count), complete);
        for index in 0..4 {
            assert_eq!(coverage.contains(index), indices.contains(&index));
        }
    }
}

#[test]
fn physical_only_registration_still_advances_synthetic_offsets() {
    use crate::block_store::ElementEntry;
    use crate::wal::WalOutcomeItem;
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let offset = crate::engine::HOT_BLOCK_OFFSET.load(std::sync::atomic::Ordering::Relaxed)
        .checked_add(256).unwrap();
    let address = ElementEntry::from_parts(crate::wal_record::WAL_LOG_SLAB_ID, offset, 8, None, None);
    let outcomes = vec![WalOutcomeItem {
        kind: "feature".into(), object_key: "series".into(), element_name: Some("1".into()),
        object_id: 42, routing_bucket: 0, address: Some(address.clone()),
        value: None, ttl: None, deleted: false, meta: false,
    }];
    let staged = vec![StagedBlock { object_id: 42, element_name: Some("1".into()), bytes: vec![7; 8] }];
    let registered = super::super::register_record_with_outcomes(&blocks, 1, &staged, &outcomes, 1, 1, &wal);
    assert_eq!(registered.len(), 1);
    let location = lookup(&blocks, 1, 42, crate::engine::live_block_key(&address)).unwrap().unwrap();
    assert_eq!(location.3, 1);
    assert_eq!(location.4, Some(0));
    assert!(crate::engine::HOT_BLOCK_OFFSET.load(std::sync::atomic::Ordering::Relaxed) > offset);
    assert!(registry().lock().unwrap().shard(blocks.store_id(), 1).is_none());
}

fn construct(root: &std::path::Path, index: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        256 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join(index),
    )
}

fn configure(engine: &TemporalEngine, version: u64, asynchronous: bool) {
    assert!(
        engine
            .set_config(crate::SetConfigRequest {
                shard_id: 1,
                config: crate::Config {
                    version,
                    async_storage: asynchronous,
                    ..Default::default()
                }
            })
            .ok
    );
}

fn open(root: &std::path::Path, asynchronous: bool) -> TemporalEngine {
    let engine = construct(root, "indexes");
    engine.load_shard(1);
    configure(&engine, 2, asynchronous);
    engine
}

fn write(engine: &TemporalEngine, fields: &[&str], generations: &[u8]) {
    let (response, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: fields.iter().map(|name| name.to_string()).collect(),
        ttl_ms: None,
        rows: generations
            .iter()
            .map(|generation| {
                BatchHashMultiSetRow(
                    "row".into(),
                    fields
                        .iter()
                        .map(|_| BatchHashMultiSetValue(vec![*generation; 48]))
                        .collect(),
                )
            })
            .collect(),
    });
    assert!(response.status.ok, "{response:?}");
    assert_eq!(response.applied, generations.len());
}

fn check(engine: &TemporalEngine, fields: &[&str], expected: &[Option<u8>]) {
    engine.cache.invalidate_shard(1).unwrap();
    let answer = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["row".into()],
        fields: fields.iter().map(|name| name.to_string()).collect(),
    });
    assert!(answer.status.ok && answer.errors.is_empty(), "{answer:?}");
    assert_eq!(
        answer.rows[0].0,
        expected
            .iter()
            .map(|value| value.map(|value| vec![value; 48]))
            .collect::<Vec<_>>()
    );
}

fn resident_count(engine: &TemporalEngine) -> usize {
    let table = table().lock().unwrap();
    if let Some(physical) = table
        .get(&engine.block_store.store_id())
        .and_then(|shards| shards.get(&1))
    {
        assert_eq!(physical.folded_counts.as_ref().unwrap().values().sum::<usize>(), physical.len);
    }
    drop(table);
    let logical = registry().lock().unwrap();
    if let Some(entries) = logical.shard(engine.block_store.store_id(), 1) {
        assert_eq!(entries.folded_counts.as_ref().unwrap().values().sum::<usize>(), entries.len());
    }
    drop(logical);
    engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks.len())
}

#[test]
fn shared_hash_frames_bound_registration_and_resident_keys_across_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let fields: Vec<_> = (0..24).map(|index| format!("field-{index}")).collect();
    let names: Vec<_> = fields.iter().map(String::as_str).collect();
    for value in 0..64 {
        write(&engine, &names, &[value]);
        assert_eq!(engine.registration_count_for_test(1), 1);
        assert_eq!(resident_count(&engine), 1);
    }
    check(&engine, &names, &vec![Some(63); 24]);
}

#[test]
fn shared_hash_partial_projections_retain_only_live_frames() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    write(&engine, &["a"], &[1]);
    write(&engine, &["b"], &[2]);
    for value in 3..64 {
        write(&engine, &[if value % 2 == 0 { "a" } else { "b" }], &[value]);
        assert_eq!(engine.registration_count_for_test(1), 2);
        assert_eq!(resident_count(&engine), 2);
    }
    check(&engine, &["a", "b", "missing"], &[Some(62), Some(63), None]);
}

#[test]
fn shared_hash_repeated_rows_in_one_record_retire_unpublished_frames() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    write(&engine, &["a", "b"], &[1, 2, 3, 4]);
    assert_eq!(engine.registration_count_for_test(1), 1);
    assert_eq!(resident_count(&engine), 1);
    check(&engine, &["a", "b"], &[Some(4), Some(4)]);
}

#[test]
fn shared_hash_migration_keeps_untouched_legacy_fields_and_retires_old_hints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), true);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashMultiSet {
                    key: "row".into(),
                    entries: vec![("a".into(), vec![1; 48]), ("b".into(), vec![2; 48])]
                }
            })
            .status
            .ok
    );
    configure(&engine, 3, false);
    write(&engine, &["a"], &[3]);
    check(&engine, &["a", "b"], &[Some(3), Some(2)]);
    assert_eq!(engine.registration_count_for_test(1), 2);
    assert_eq!(resident_count(&engine), 2);
    write(&engine, &["b"], &[4]);
    check(&engine, &["a", "b"], &[Some(3), Some(4)]);
    assert_eq!(engine.registration_count_for_test(1), 2);
    assert_eq!(resident_count(&engine), 2);
    write(&engine, &["a", "b"], &[5]);
    assert_eq!(engine.registration_count_for_test(1), 1);
    assert_eq!(resident_count(&engine), 1);
    check(&engine, &["a", "b"], &[Some(5), Some(5)]);
}

#[test]
fn shared_hash_frame_recovery_preserves_partial_and_repeated_rows() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        write(&engine, &["a"], &[1]);
        write(&engine, &["b"], &[2, 3, 4]);
        if checkpoint {
            engine.flush_shard_index(1);
        }
        engine.wal_store.flush(1).unwrap();
        drop(engine);
        let mut restored = construct(dir.path(), if checkpoint { "indexes" } else { "fresh" });
        if !checkpoint {
            restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
        }
        restored.load_shard(1);
        check(&restored, &["a", "b"], &[Some(1), Some(4)]);
        assert_eq!(restored.registration_count_for_test(1), 2);
        assert_eq!(resident_count(&restored), 2);
    }
}

#[test]
fn shared_hash_frame_retirement_preserves_logical_fold_collisions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    write(&engine, &["a"], &[1]);
    let (key, held) = engine.shards.with_state(1, |shard_state| {
        let (key, held) = shard_state
            .unwrap()
            .wal_resident_blocks
            .iter()
            .next()
            .unwrap();
        (*key, *held)
    });
    register_at(
        &engine.block_store,
        1,
        key,
        None,
        held.log_id,
        held.sequence,
        &engine.wal_store,
    );
    write(&engine, &["a"], &[2]);
    assert!(engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks.contains_key(&key)));
    check(&engine, &["a"], &[Some(2)]);
}

#[test]
fn shared_hash_legacy_fallback_refuses_an_unbound_address() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), true);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashMultiSet {
                    key: "row".into(),
                    entries: vec![("a".into(), vec![1; 48]), ("b".into(), vec![2; 48])]
                }
            })
            .status
            .ok
    );
    let address = engine.shards.with_state(1, |state| {
        state.unwrap().bucket_index.hashes.get("row").unwrap().get("b").unwrap().clone()
    });
    configure(&engine, 3, false);
    write(&engine, &["a"], &[3]);
    let object = crate::engine::stable_block_object_id(1, "hash", "row");
    assert!(read_block_at(&engine.block_store, 1, object, Some("b"), &address).is_some());
    let wrong = crate::block_store::ElementEntry::from_parts(
        address.block_slab_id(),
        address.offset() + 32768,
        address.length(),
        None,
        None,
    );
    assert_ne!(
        crate::engine::live_block_key(&address),
        crate::engine::live_block_key(&wrong)
    );
    assert!(matches!(
        lookup(
            &engine.block_store,
            1,
            object,
            crate::engine::live_block_key(&wrong)
        ),
        Some(None)
    ));
    assert!(read_block_at(&engine.block_store, 1, object, Some("b"), &wrong).is_none());
    check(&engine, &["a", "b"], &[Some(3), Some(2)]);
}

#[test]
fn shared_hash_delete_and_recreate_does_not_accumulate_old_frames() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    for value in 1..32 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete { key: "row".into() }
                })
                .status
                .ok
        );
        write(&engine, &["a", "b"], &[value]);
        assert_eq!(engine.registration_count_for_test(1), 1);
        assert_eq!(resident_count(&engine), 1);
    }
    check(&engine, &["a", "b"], &[Some(31), Some(31)]);
}

#[test]
fn shared_hash_physical_registration_stays_scoped_to_its_store() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    let first = open(left.path(), false);
    let second = open(right.path(), false);
    write(&first, &["a"], &[1]);
    write(&second, &["a"], &[2]);
    write(&first, &["a"], &[3]);
    check(&first, &["a"], &[Some(3)]);
    check(&second, &["a"], &[Some(2)]);
    assert_eq!(first.registration_count_for_test(1), 1);
    assert_eq!(second.registration_count_for_test(1), 1);
    drop(first);
    check(&second, &["a"], &[Some(2)]);
}

#[test]
#[ignore = "local process upgrade compatibility fixture"]
fn shared_hash_write_index_for_process_compatibility() {
    let root = std::path::PathBuf::from(std::env::var("TS_HASH_FRAME_COMPAT_DIR").unwrap());
    assert!(root.is_absolute() && !root.exists());
    std::fs::create_dir(&root).unwrap();
    let engine = open(&root, false);
    write(&engine, &["a", "b"], &[1, 2, 3]);
    write(&engine, &["a"], &[4]);
    check(&engine, &["a", "b"], &[Some(4), Some(3)]);
    assert_eq!(engine.registration_count_for_test(1), 2);
    assert_eq!(resident_count(&engine), 2);
    engine.flush_shard_index(1);
    engine.wal_store.flush(1).unwrap();
    std::fs::write(
        root.join("fixture.json"),
        serde_json::to_vec(&serde_json::json!({
            "key": "row", "fields": ["a", "b"], "values": [4, 3], "width": 48,
            "physical_registrations": 2, "resident_keys": 2,
        }))
        .unwrap(),
    )
    .unwrap();
}

fn append_history(engine: &TemporalEngine) {
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "history".into(),
            points: (1..=3).map(|timestamp_ms| crate::types::FeaturePoint {
                timestamp_ms, value: vec![timestamp_ms as u8; 48],
            }).collect(),
        },
    });
    assert!(result.status.ok, "{result:?}");
}

fn check_history(engine: &TemporalEngine) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery { key: "history".into(), start_ms: 0, end_ms: 3, count: None },
    });
    assert!(response.status.ok, "{response:?}");
    assert_eq!(response.response, crate::types::CommandResponse::FeaturePoints {
        points: (1..=3).map(|timestamp_ms| crate::types::FeaturePoint {
            timestamp_ms, value: vec![timestamp_ms as u8; 48],
        }).collect(),
    });
}

fn assert_untracked_history(engine: &TemporalEngine) {
    let owner = engine.block_store.store_id();
    let physical = table().lock().unwrap();
    let entries = physical.get(&owner).unwrap().get(&1).unwrap();
    assert!(entries.len > 0);
    assert!(entries.folded_counts.is_none());
    drop(physical);
    let logical = registry().lock().unwrap();
    assert!(logical.shard(owner, 1).is_none_or(|entries| entries.folded_counts.is_none()));
}

#[test]
fn generic_history_leaves_fold_tracking_disabled_across_index_and_wal_recovery() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        append_history(&engine);
        check_history(&engine);
        assert_untracked_history(&engine);
        if checkpoint { engine.flush_shard_index(1); }
        engine.wal_store.flush(1).unwrap();
        drop(engine);
        let mut restored = construct(dir.path(), if checkpoint { "indexes" } else { "fresh" });
        if !checkpoint {
            restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
        }
        restored.load_shard(1);
        check_history(&restored);
        assert_untracked_history(&restored);
    }
}

#[test]
fn compact_hash_activation_backfills_existing_history_and_logical_registrations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), true);
    append_history(&engine);
    assert!(engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiSet {
            key: "row".into(), entries: vec![("a".into(), vec![1; 48]), ("b".into(), vec![2; 48])],
        },
    }).status.ok);
    assert_untracked_history(&engine);
    configure(&engine, 3, false);
    write(&engine, &["a"], &[3]);
    check(&engine, &["a", "b"], &[Some(3), Some(2)]);
    check_history(&engine);
    resident_count(&engine);
    write(&engine, &["a", "b"], &[4]);
    check(&engine, &["a", "b"], &[Some(4), Some(4)]);
    check_history(&engine);
    resident_count(&engine);
    let logical = registry().lock().unwrap();
    let entries = logical.shard(engine.block_store.store_id(), 1).unwrap();
    assert!(entries.is_empty() && entries.folded_counts.as_ref().unwrap().is_empty());
    assert_eq!(entries.capacity(), 0);
}

#[test]
fn fold_tracking_activation_keeps_preexisting_fold_collisions_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let address = crate::block_store::ElementEntry::from_parts(crate::engine::HOT_BLOCK_SLAB_ID, 64, 48, None, None);
    let key = (11, crate::engine::live_block_key(&address));
    let location = Arc::new((wal.clone(), 1, 1, 1, Some(0)));
    let folded = install(&blocks, 1, key, location);
    register_at(&blocks, 1, folded, None, 2, 2, &wal);
    for _ in 0..2 {
        enable_fold_tracking(&blocks, 1);
        let logical = registry().lock().unwrap();
        let entries = logical.shard(blocks.store_id(), 1).unwrap();
        assert!(entries.has_fold(&folded));
        assert_eq!(entries.folded_counts.as_ref().unwrap()[&folded], 1);
        let physical = table().lock().unwrap();
        assert_eq!(physical[&blocks.store_id()][&1].folded_counts.as_ref().unwrap()[&folded], 1);
    }
}

#[test]
fn enabled_empty_fold_headers_survive_churn_and_clear_with_the_shard() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    enable_fold_tracking(&blocks, 1);
    for offset in 1..=8 {
        let address = crate::block_store::ElementEntry::from_parts(crate::engine::HOT_BLOCK_SLAB_ID, offset, 48, None, None);
        let key = (11, crate::engine::live_block_key(&address));
        let location = Arc::new((wal.clone(), offset, offset, 1, Some(0)));
        let folded = install(&blocks, 1, key, location.clone());
        retire(&blocks, 1, &RegistrationRetirement {
            folded_key: folded, log_id: offset, sequence: offset,
            block: RetirementKey::Physical(key), location,
        });
        let physical = table().lock().unwrap();
        let state = &physical[&blocks.store_id()][&1];
        assert_eq!(state.len, 0);
        assert_eq!(state.objects.capacity(), 0);
        assert_eq!(state.folded_counts.as_ref().unwrap().capacity(), 0);
        drop(physical);
        register_at(&blocks, 1, 17, None, offset, offset, &wal);
        deregister(&blocks, 1, 17, None);
        let logical = registry().lock().unwrap();
        let entries = logical.shard(blocks.store_id(), 1).unwrap();
        assert_eq!(entries.capacity(), 0);
        assert_eq!(entries.folded_counts.as_ref().unwrap().capacity(), 0);
    }
    super::super::clear_shard(&blocks, 1);
    assert!(!table().lock().unwrap().get(&blocks.store_id()).is_some_and(|shards| shards.contains_key(&1)));
    assert!(registry().lock().unwrap().shard(blocks.store_id(), 1).is_none());
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "isolated physical coverage allocation measurement"]
fn measure_physical_index_coverage_allocations() {
    use super::super::PhysicalIndexCoverage;
    for count in [1, 128, 500] {
        let bindings: Vec<_> = (0..count).map(|index| ((1, (1, index as u64, 8)), index)).collect();
        for dense in [false, true] {
            let span = crate::alloc_probe::Probe::start();
            if dense {
                let coverage = PhysicalIndexCoverage::new(std::hint::black_box(&bindings), count);
                assert!(coverage.covers_all(count));
                std::hint::black_box(coverage);
            } else {
                let indices: std::collections::HashSet<_> = bindings.iter().map(|(_, index)| *index).collect();
                assert_eq!(indices.len(), count);
                std::hint::black_box(indices);
            }
            let result = span.stop();
            println!("PHYSICAL_COVERAGE_COST dense={dense} frames={count} allocs={} bytes={}", result.allocs, result.alloc_bytes);
            if dense { assert_eq!((result.allocs, result.alloc_bytes), (0, 0)); }
        }
    }
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "isolated logical registration allocation measurement"]
fn measure_lazy_logical_fold_tracking_allocations() {
    let dir = tempfile::tempdir().unwrap();
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let location = Arc::new((wal, 1, 1, 1, Some(0)));
    let keys: Vec<_> = (0..4096).map(|object| block_key(object, Some("field"))).collect();
    for enabled in [false, true] {
        let mut entries = ShardRegistrations::default();
        if enabled { entries.enable_fold_tracking(); }
        let span = crate::alloc_probe::Probe::start();
        for key in &keys { entries.insert(key.clone(), location.clone()); }
        let result = span.stop();
        assert_eq!(entries.len(), keys.len());
        assert_eq!(entries.folded_counts.is_some(), enabled);
        println!("FOLD_INDEX_COST enabled={enabled} entries={} allocs={} bytes={}", entries.len(), result.allocs, result.alloc_bytes);
    }
}
