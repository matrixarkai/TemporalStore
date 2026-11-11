// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn visits() -> usize {
    REGISTRATION_QUERY_VISITS.with(std::cell::Cell::get)
}

fn reset_visits() {
    REGISTRATION_QUERY_VISITS.with(|count| count.set(0));
}

#[test]
fn count_and_retention_queries_do_not_visit_other_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let other = crate::block_store::BlockStore::new(dir.path().join("other-blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    for object in 1..=3 {
        register_at(&blocks, 1, object, None, object, object, &wal);
    }
    for object in 0..20_000 {
        register_at(&blocks, 2, object, None, object, object + 1, &wal);
        register_at(&other, 1, object, None, object, object + 1, &wal);
    }
    reset_visits();
    for _ in 0..100 {
        assert_eq!(registration_count(&blocks, 1), 3);
        assert_eq!(registration_count(&blocks, 99), 0);
    }
    assert_eq!(visits(), 0, "counts must not walk registered blocks");
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(1));
    assert_eq!(visits(), 3);
    assert!(registered_record_span_has_holes(&blocks, 1, &wal));
    assert_eq!(visits(), 6, "unknown cardinality still inspects only the requested shard");
    assert_eq!(oldest_registered_objects(&blocks, 1), vec![
        (1, (1, None)), (2, (2, None)), (3, (3, None)),
    ]);
    assert_eq!(visits(), 9);
}

#[test]
fn retention_visits_only_local_records_and_keeps_log_identity_checks() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let other_wal = LocalWriteAheadLogStore::new(dir.path().join("other-wal"));
    let owner = blocks.store_id();
    {
        let mut map = registry().lock().unwrap();
        let entries = map.shard_mut(owner, 1);
        for sequence in 5..=7 {
            entries.insert((sequence, None), std::sync::Arc::new((wal.clone(), sequence, sequence, 1, None)));
        }
        entries.insert((99, None), std::sync::Arc::new((other_wal.clone(), 1, 1, 1, None)));
    }
    reset_visits();
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(5));
    assert_eq!(visits(), 4);
    assert_eq!(min_registered_sequence(&blocks, 1, &other_wal), Some(1));
    assert!(!registered_record_span_has_holes(&blocks, 1, &wal));
    assert_eq!(oldest_registered_objects(&blocks, 1), vec![
        (1, (99, None)), (5, (5, None)), (6, (6, None)), (7, (7, None)),
    ]);
    assert_eq!(visits(), 16);
    deregister(&blocks, 1, 6, None);
    assert!(registered_record_span_has_holes(&blocks, 1, &wal));
    assert!(!registered_record_span_has_holes(&blocks, 2, &wal));
}

#[test]
fn replacement_and_removal_keep_other_shards_and_stores() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let other = crate::block_store::BlockStore::new(dir.path().join("other-blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    for (store, shard) in [(&blocks, 1), (&blocks, 2), (&other, 1)] {
        register_at(store, shard, 42, None, 1, 1, &wal);
        register_at(store, shard, 42, Some("field"), 1, 1, &wal);
    }
    register_at(&blocks, 1, 42, None, 9, 9, &wal);
    assert_eq!(registration_count(&blocks, 1), 2);
    deregister(&blocks, 1, 42, None);
    assert_eq!(oldest_registered_objects(&blocks, 1), vec![(1, block_key(42, Some("field")))]);
    clear_shard(&blocks, 1);
    assert_eq!(registration_count(&blocks, 1), 0);
    assert_eq!(registration_count(&blocks, 2), 2);
    assert_eq!(registration_count(&other, 1), 2);
    deregister(&blocks, 2, 42, None);
    deregister(&blocks, 2, 42, Some("field"));
    assert!(!registry().lock().unwrap().stores.contains_key(&blocks.store_id()));
    assert_eq!(registration_count(&other, 1), 2);
    clear_store_registrations(other.store_id());
    assert!(!registry().lock().unwrap().stores.contains_key(&other.store_id()));
    deregister(&blocks, 9, 42, None);
    assert!(!registry().lock().unwrap().stores.contains_key(&blocks.store_id()));
}

#[test]
fn equal_keys_in_multiple_engines_and_shards_read_their_own_frames() {
    use crate::types::{Command, ExecuteRequest};
    let dir = tempfile::tempdir().unwrap();
    let open = |name: &str| crate::engine::TemporalEngine::with_local_dirs(
        1 << 20, dir.path().join(name).join("cache"),
        dir.path().join(name).join("blocks"), dir.path().join(name).join("indexes"),
    );
    let first = open("first");
    let second = open("second");
    for (engine, prefix) in [(&first, "first"), (&second, "second")] {
        for shard in [1, 2] {
            engine.load_shard(shard);
            let value = format!("{prefix}-{shard}").into_bytes();
            assert!(engine.execute_replicated(ExecuteRequest {
                shard_id: shard,
                command: Command::StringSet { key: "same-key".to_string(), value: value.clone() },
            }.with_async_storage()).status.ok);
            let object = crate::engine::stable_block_object_id(shard, "string", "same-key");
            assert_eq!(read_block(&engine.block_store, shard, object, None), Some(value));
        }
    }
    let object = crate::engine::stable_block_object_id(1, "string", "same-key");
    clear_shard(&first.block_store, 1);
    assert_eq!(read_block(&first.block_store, 1, object, None), None);
    assert_eq!(read_block(&second.block_store, 1, object, None), Some(b"second-1".to_vec()));
    assert!(std::mem::size_of::<(usize, ShardId, BlockKey)>() > std::mem::size_of::<BlockKey>());
    #[cfg(target_pointer_width = "64")]
    assert_eq!(std::mem::size_of::<(usize, ShardId, BlockKey)>() - std::mem::size_of::<BlockKey>(), 16);
}
