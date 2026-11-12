// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct IndexedSetting(Option<std::ffi::OsString>);

impl IndexedSetting {
    fn set(value: &str) -> Self {
        let previous = std::env::var_os("TS_WAL_INDEXED_BLOCKS");
        std::env::set_var("TS_WAL_INDEXED_BLOCKS", value);
        Self(previous)
    }
}

impl Drop for IndexedSetting {
    fn drop(&mut self) {
        match &self.0 {
            Some(value) => std::env::set_var("TS_WAL_INDEXED_BLOCKS", value),
            None => std::env::remove_var("TS_WAL_INDEXED_BLOCKS"),
        }
    }
}

fn blocks(count: usize) -> Vec<StagedBlock> {
    (0..count).map(|index| StagedBlock {
        object_id: index as u64 + 1000,
        element_name: None,
        bytes: vec![index as u8; 1024],
    }).collect()
}

#[test]
fn shared_records_keep_log_shard_and_position_isolation_after_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let first = LocalWriteAheadLogStore::new(dir.path().join("first"));
    let second = LocalWriteAheadLogStore::new(dir.path().join("second"));
    let held = cache_record(&first, 1, 10, blocks(3));
    assert!(Arc::ptr_eq(&held, &cached_record(&first, 1, 10).unwrap()));
    assert!(cached_record(&first, 2, 10).is_none());
    assert!(cached_record(&first, 1, 11).is_none());
    assert!(cached_record(&second, 1, 10).is_none());
    let other_shard = cache_record(&first, 2, 10, blocks(2));
    let other_log = cache_record(&second, 1, 10, blocks(1));
    assert!(!Arc::ptr_eq(&held, &other_shard));
    assert!(!Arc::ptr_eq(&held, &other_log));
    for log_id in 100..116 {
        cache_record(&first, 1, log_id, blocks(1));
    }
    assert!(cached_record(&first, 1, 10).is_none());
    assert!(record_lru().lock().unwrap().len() <= 8);
    assert_eq!(held.len(), 3);
    assert_eq!(held[2].bytes, vec![2; 1024]);
}

#[test]
fn concurrent_publications_share_one_immutable_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8).map(|_| {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let input = blocks(128);
            barrier.wait();
            cache_record(&store, 1, 42, input)
        })
    }).collect();
    let returned: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
    assert!(returned.iter().all(|record| Arc::ptr_eq(record, &returned[0])));
    assert_eq!(record_lru().lock().unwrap().iter().filter(|(s, shard, id, _)|
        s.same_log(&store) && *shard == 1 && *id == 42).count(), 1);
    assert_eq!(returned[0][127].bytes, vec![127; 1024]);
}

#[test]
fn decoded_record_cache_does_not_keep_a_wal_owner_and_its_readers_alive() {
    struct ResetRoll;
    impl Drop for ResetRoll {
        fn drop(&mut self) { crate::wal::set_wal_segment_bytes_for_test(None); }
    }
    let _reset = ResetRoll;
    crate::wal::set_wal_segment_bytes_for_test(Some(1));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let staged = blocks(128);
    let (record, log_id) = store.append_batch_as_one_record_at(1, Vec::new(), staged.clone(), false).unwrap();
    let _ = store.read_carried_block_at_log_id(1, log_id, record.sequence, 127, 1127, None).unwrap().unwrap();
    let identity = store.weak_identity();
    let held = cache_record(&store, 1, log_id, staged);
    assert!(identity.is_live());
    drop(store);
    assert!(!identity.is_live());
    assert_eq!(held[127].bytes, vec![127; 1024]);
}

#[test]
fn carried_reads_reuse_one_decoded_batch_and_select_new_record_positions() {
    let _legacy = IndexedSetting::set("0");
    let dir = tempfile::tempdir().unwrap();
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        64 * 1024, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let staged = blocks(512);
    let marker = || crate::types::Command::StringSet { key: "carrier".into(), value: Vec::new() };
    let (record, log_id) = engine.write_ahead_log_store()
        .append_with_sync_staged(1, marker(), true, staged.clone()).unwrap();
    let wal = engine.write_ahead_log_store();
    register_record(&engine.block_store, 1, &staged, log_id, record.sequence, &wal);
    let before = wal.stats(1).bytes_read;
    assert_eq!(read_block(&engine.block_store, 1, 1000, None), Some(vec![0; 1024]));
    let after = wal.stats(1).bytes_read;
    assert!(after > before);
    let cached = cached_record(&wal, 1, log_id).unwrap();
    for index in [511, 127, 255, 0] {
        assert_eq!(read_block(&engine.block_store, 1, 1000 + index, None),
                   Some(vec![index as u8; 1024]));
        assert!(Arc::ptr_eq(&cached, &cached_record(&wal, 1, log_id).unwrap()));
    }
    assert_eq!(wal.stats(1).bytes_read, after);
    let next = vec![StagedBlock { object_id: 1000, element_name: None, bytes: vec![9; 2048] }];
    let (record, next_id) = wal.append_with_sync_staged(1, marker(), true, next.clone()).unwrap();
    register_record(&engine.block_store, 1, &next, next_id, record.sequence, &wal);
    assert_eq!(read_block(&engine.block_store, 1, 1000, None), Some(vec![9; 2048]));
    assert_eq!(cached[0].bytes, vec![0; 1024]);
    assert!(!Arc::ptr_eq(&cached, &cached_record(&wal, 1, next_id).unwrap()));
}

#[test]
fn indexed_carried_reads_select_without_retaining_decoded_batches() {
    let _indexed = IndexedSetting::set("1");
    let dir = tempfile::tempdir().unwrap();
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        64 * 1024, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let staged = blocks(512);
    let marker = || crate::types::Command::StringSet { key: "carrier".into(), value: Vec::new() };
    let wal = engine.write_ahead_log_store();
    let (record, log_id) = wal.append_with_sync_staged(1, marker(), true, staged.clone()).unwrap();
    register_record(&engine.block_store, 1, &staged, log_id, record.sequence, &wal);
    for index in [0, 511, 127, 255, 0] {
        assert_eq!(read_block(&engine.block_store, 1, 1000 + index, None),
                   Some(vec![index as u8; 1024]));
        assert!(cached_record(&wal, 1, log_id).is_none());
    }
    let next = vec![StagedBlock { object_id: 1000, element_name: None, bytes: vec![9; 2048] }];
    let (record, next_id) = wal.append_with_sync_staged(1, marker(), true, next.clone()).unwrap();
    register_record(&engine.block_store, 1, &next, next_id, record.sequence, &wal);
    assert_eq!(read_block(&engine.block_store, 1, 1000, None), Some(vec![9; 2048]));
    assert_eq!(read_block(&engine.block_store, 1, 1511, None), Some(vec![255; 1024]));
}

#[test]
#[cfg(feature = "alloc-probe")]
fn publishing_decoded_batches_does_not_clone_their_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    for log_id in 0..8 {
        cache_record(&store, 1, log_id, blocks(1));
    }
    for count in [1, 8, 128, 512] {
        let input = blocks(count);
        let pointers: Vec<_> = input.iter().map(|block| block.bytes.as_ptr()).collect();
        let probe = crate::alloc_probe::Probe::start();
        let copied = input.clone();
        let old = probe.stop();
        std::hint::black_box(&copied);
        drop(copied);
        let probe = crate::alloc_probe::Probe::start();
        let shared = cache_record(&store, 1, 1000 + count as u64, input);
        let new = probe.stop();
        assert!(shared.iter().zip(&pointers).all(|(block, pointer)| block.bytes.as_ptr() == *pointer));
        assert_eq!(shared[count - 1].bytes, vec![(count - 1) as u8; 1024]);
        assert_eq!(new.allocs, 1);
        assert!(new.alloc_bytes <= 64);
        assert!(old.alloc_bytes >= (count * 1024) as u64);
        println!("SHARED_RECORD_ALLOC blocks={count} old_allocs={} new_allocs={} old_bytes={} new_bytes={}",
                 old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes);
    }
}
