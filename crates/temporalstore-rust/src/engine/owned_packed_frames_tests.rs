// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(root: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        1024 * 1024, root.join("cache"), root.join("blocks"), root.join("indexes"),
        crate::block_store::BlockStoreOptions::default(), false,
    )
}

#[test]
fn physical_append_funnels_keep_shard_zero_separate_from_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut written = Vec::new();
    for (index, (shard_id, owned)) in [(0, false), (9, true), (0, true), (9, false)]
        .into_iter().enumerate()
    {
        block_in_wal::begin_write();
        let bytes = vec![index as u8 + 1; 64];
        let address = if owned {
            append_owned_value_of_object(
                &engine.cache, &engine.block_store, shard_id, bytes.clone(), 42, 7, false,
                execute_on_shard::PackedFrameCache::Eager, || 3,
            )
        } else {
            append_value_inner(
                &engine.cache, &engine.block_store, shard_id, &bytes,
                Some(42), None, Some(7), false, 8191, || 3,
            )
        }.unwrap();
        assert!(!crate::wal_record::is_wal_resident(address.block_slab_id()));
        assert_eq!(address.block_id(), Some(3));
        let staged = block_in_wal::take_staged();
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].bytes, bytes);
        written.push((shard_id, address, bytes));
    }
    assert_eq!(written[0].1.block_slab_id(), written[2].1.block_slab_id());
    assert_eq!(written[1].1.block_slab_id(), written[3].1.block_slab_id());
    assert_ne!(written[0].1.block_slab_id(), written[1].1.block_slab_id());
    assert!(written[2].1.offset() > written[0].1.offset());
    assert!(written[3].1.offset() > written[1].1.offset());
    let legacy = engine.block_store.append_block_of_object(b"legacy", Some(42), Some(7), 3).unwrap();
    for (_, address, _) in &written {
        assert_ne!(legacy.block_slab_id(), address.block_slab_id());
    }
    engine.block_store.sync_durable().unwrap();
    drop(engine);
    let reopened = open(dir.path());
    let descriptors = reopened.block_store.slab_descriptors();
    for (shard_id, address, bytes) in written {
        let slab = descriptors.iter().find(|slab| slab.block_slab_id == address.block_slab_id()).unwrap();
        assert_eq!(slab.owner_shard_id, Some(shard_id));
        assert_eq!(reopened.block_store.read(&address).unwrap(), bytes);
    }
    let slab = descriptors.iter().find(|slab| slab.block_slab_id == legacy.block_slab_id()).unwrap();
    assert_eq!(slab.owner_shard_id, None);
    assert_eq!(reopened.block_store.read(&legacy).unwrap(), b"legacy");
}

#[test]
fn timestamped_batch_append_uses_the_requested_shard_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut written = Vec::new();
    for (index, shard_id) in [0, 9, 0].into_iter().enumerate() {
        block_in_wal::begin_write();
        let points: Vec<_> = [10, 20].into_iter().map(|timestamp_ms| crate::types::FeaturePoint {
            timestamp_ms, value: vec![index as u8 + 1; 64],
        }).collect();
        let addresses = packed_blocks::append_timestamped_kv_blocks(
            &engine.cache, &engine.block_store, shard_id, "feature", "series",
            points.clone(), 7, false, false, 40,
        ).unwrap();
        let _ = block_in_wal::take_staged();
        assert_eq!(addresses.len(), points.len());
        for (timestamp, address) in addresses {
            written.push((shard_id, timestamp, address, points.clone()));
        }
    }
    assert_ne!(written[0].2.block_slab_id(), written[2].2.block_slab_id());
    assert_eq!(written[0].2.block_slab_id(), written[4].2.block_slab_id());
    engine.block_store.sync_durable().unwrap();
    drop(engine);
    let reopened = open(dir.path());
    let descriptors = reopened.block_store.slab_descriptors();
    for (shard_id, timestamp, address, points) in written {
        let slab = descriptors.iter().find(|slab| slab.block_slab_id == address.block_slab_id()).unwrap();
        assert_eq!(slab.owner_shard_id, Some(shard_id));
        let packed = reopened.block_store.read(&address).unwrap();
        let decoded = packed_blocks::decode_feature_block(&packed).unwrap();
        assert_eq!(decoded.iter().find(|point| point.timestamp_ms == timestamp),
                   points.iter().find(|point| point.timestamp_ms == timestamp));
    }
}

#[test]
fn owned_packed_frames_keep_the_original_buffer_and_placement_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for asynchronous in [false, true] {
        for length in [1, 48, 8191, 8192, 65_536] {
            block_in_wal::begin_write();
            let bytes = vec![17; length];
            let original = bytes.as_ptr();
            let ordinal_calls = std::cell::Cell::new(0);
            let address = append_owned_value_of_object(
                &engine.cache, &engine.block_store, 1, bytes, 42, 9, asynchronous,
                execute_on_shard::PackedFrameCache::Eager,
                || { ordinal_calls.set(ordinal_calls.get() + 1); 7 },
            ).unwrap();
            let blocks = block_in_wal::take_staged();
            assert_eq!(blocks.len(), 1);
            assert_eq!(blocks[0].object_id, 42);
            assert!(blocks[0].element_name.is_none());
            assert_eq!(blocks[0].bytes.as_ptr(), original, "the original payload moves into the record");
            assert_eq!(blocks[0].bytes, vec![17; length]);
            let physical = !asynchronous || length >= 8192;
            assert_eq!(crate::wal_record::is_wal_resident(address.block_slab_id()), !physical);
            assert_eq!(ordinal_calls.get(), usize::from(physical));
            assert_eq!(address.block_id(), physical.then_some(7));
            if !physical {
                let key = CacheKey::page_with_slot(1, address.block_slab_id(), address.offset(),
                    address.length(), Some(9));
                let cached = engine.cache.get_shared(&key).unwrap().unwrap();
                assert_eq!(cached.as_ref(), blocks[0].bytes);
                assert_ne!(cached.as_ptr(), original, "the cache holds its own payload copy");
                assert!(!engine.cache.peek(&CacheKey::page_with_slot(1, address.block_slab_id(),
                    address.offset(), address.length(), Some(10))));
                engine.cache.invalidate(&key).unwrap();
                assert_eq!(blocks[0].bytes, vec![17; length]);
                assert_eq!(cached.as_ref(), vec![17; length]);
            }
        }
    }
}

#[test]
fn owned_packed_frames_stage_distinct_rows_once_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    block_in_wal::begin_write();
    let mut pointers = Vec::new();
    let mut addresses = Vec::new();
    for index in 0..16 {
        let bytes = vec![index as u8; 1320];
        pointers.push(bytes.as_ptr());
        addresses.push(append_owned_value_of_object(
            &engine.cache, &engine.block_store, 1, bytes, index, 9, true,
            execute_on_shard::PackedFrameCache::Eager,
            || panic!("WAL placement must not resolve a physical ordinal"),
        ).unwrap());
    }
    let blocks = block_in_wal::take_staged();
    assert_eq!(blocks.len(), 16);
    for (index, block) in blocks.iter().enumerate() {
        assert_eq!(block.object_id, index as u64);
        assert_eq!(block.bytes.as_ptr(), pointers[index]);
        assert_eq!(block.bytes, vec![index as u8; 1320]);
    }
    let unique: std::collections::HashSet<_> = addresses.iter().map(live_block_key).collect();
    assert_eq!(unique.len(), 16);
    assert!(block_in_wal::take_staged().is_empty());
}

#[cfg(feature = "alloc-probe")]
#[test]
fn owned_packed_frames_remove_the_carried_payload_copy() {
    use crate::alloc_probe::{AllocClass, ClassSpan};

    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for length in [48, 1320, 8191, 8192, 65_536] {
        // Warm allocator/cache state before weighing the otherwise identical placement paths.
        let measure = |owned| {
            block_in_wal::begin_write();
            let bytes = vec![23; length];
            let span = ClassSpan::open();
            let address = if owned {
                append_owned_value_of_object(&engine.cache, &engine.block_store, 1,
                    bytes, 42, 9, true, execute_on_shard::PackedFrameCache::Eager, || 0)
            } else {
                append_value_inner(&engine.cache, &engine.block_store, 1,
                    &bytes, Some(42), None, Some(9), true, 8191, || 0)
            }.unwrap();
            let measured = span.close().unwrap();
            let staged = block_in_wal::take_staged();
            assert_eq!(staged.len(), 1);
            assert_eq!(staged[0].bytes.len(), length);
            if crate::wal_record::is_wal_resident(address.block_slab_id()) {
                let key = CacheKey::page_with_slot(1, address.block_slab_id(), address.offset(),
                    address.length(), Some(9));
                engine.cache.invalidate(&key).unwrap();
            }
            measured.classes.row(AllocClass::CarriedBlock)
        };
        let _ = measure(false);
        let old = measure(false);
        let new = measure(true);
        assert_eq!(old.allocs, new.allocs + 1);
        assert_eq!(old.alloc_bytes, new.alloc_bytes + length as u64);
        println!("OWNED_PACKED_COPY length={length} old_allocs={} new_allocs={} old_bytes={} new_bytes={}",
            old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes);
    }
}

#[test]
fn owned_packed_frames_do_not_stage_a_failed_physical_append() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, b"not a directory").unwrap();
    let store = BlockStore::new(blocked);
    for asynchronous in [false, true] {
        block_in_wal::begin_write();
        let before = engine.cache.stats().memory_bytes;
        assert!(append_owned_value_of_object(&engine.cache, &store, 1,
            vec![31; 8192], 42, 9, asynchronous, execute_on_shard::PackedFrameCache::Eager, || 0).is_err());
        assert!(block_in_wal::take_staged().is_empty());
        assert_eq!(engine.cache.stats().memory_bytes, before);
    }
}
