// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn load(engine: &TemporalEngine) {
    assert!(engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1, start_routing_bucket: 0, end_routing_bucket: 7,
        load_version: 1, local_node_id: Some(1), shard_uri: "local://inline-pages/1".into(),
        readonly: false, table_name: "inline-pages".into(),
    }).status.ok);
}

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(1 << 20,
        root.join("cache"), root.join("blocks"), root.join("indexes"));
    load(&engine);
    assert!(engine.set_config(crate::SetConfigRequest {
        shard_id: 1,
        config: crate::Config { version: 2, async_storage: true, ..Default::default() },
    }).ok);
    for index in 0..32 {
        assert!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringSet { key: format!("key-{index}"), value: vec![index as u8; 128] },
        }).status.ok);
    }
    engine
}

fn page_facts(shard: &ShardState) -> Vec<(String, u64, u64, u64, bool, bool)> {
    let mut facts = collect_bucket_index_live_block_entries(shard).into_iter().map(|entry| {
        (entry.object_key.to_string(), entry.address.block_slab_id(), entry.address.offset(),
         entry.address.length(), entry.dirty, entry.deleted)
    }).collect::<Vec<_>>();
    facts.sort();
    facts
}

fn verify(engine: &TemporalEngine) {
    for index in 0..32 {
        assert_eq!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringGet { key: format!("key-{index}") },
        }).response, CommandResponse::Bytes { value: Some(vec![index as u8; 128]) });
    }
}

#[test]
fn dirty_inline_pages_keep_exact_flags_across_release_reload_and_capture() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    let buckets = shard.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>();
    let before = page_facts(shard);
    assert_eq!(before.len(), 32);
    assert!(before.iter().all(|entry| entry.4 && !entry.5));
    let control = release_bucket_blocks(shard, &buckets);
    assert!(control.released_buckets.is_empty());
    assert_eq!(control.refusals.bucket_dirty, buckets.len());
    let released = release_bucket_blocks_preserving_inline_pages(shard, &buckets);
    assert_eq!(released.released_buckets, buckets);
    assert_eq!(released.released_blocks, 32);
    assert_eq!(page_facts(shard), before);
    assert!(shard.bucket_index.bucket_map.values().all(|bucket| bucket.dirty() && !bucket.in_memory()));
    assert!(shard.strings().values().all(|slot| slot.record.retained_page_state() == (true, false)));
    let mut captured = shard.clone();
    restore_released_buckets_in_checkpoint(&mut captured, 1);
    assert_eq!(page_facts(&captured), before);
    for bucket in &buckets { assert!(reload_released_bucket(shard, 1, *bucket)); }
    assert_eq!(page_facts(shard), before);
    assert!(shard.bucket_index.released_buckets.is_empty());
    for _ in 0..32 {
        assert_eq!(release_bucket_blocks_preserving_inline_pages(shard, &buckets).released_blocks, 32);
        for bucket in &buckets { assert!(reload_released_bucket(shard, 1, *bucket)); }
        assert_eq!(shard.bucket_index.bucket_owners.get().unwrap().entries(), 32,
            "reload must replace the owner list rather than append another copy");
        assert_eq!(page_facts(shard), before);
    }
    assert_eq!(std::mem::size_of::<super::super::object_record::ObjectRecord>(), 16);
    assert_eq!(std::mem::size_of::<super::super::object_record::ObjectSlot>(), 48);
    drop(shards);
    verify(&engine);
}

#[test]
fn inline_release_survives_wal_reopen_and_a_later_dump_clears_retained_flags() {
    for dump in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path());
        let buckets = {
            let mut shards = engine.shards.write().unwrap();
            let shard = shards.get_mut(&1).unwrap();
            let buckets = shard.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>();
            assert_eq!(release_bucket_blocks_preserving_inline_pages(shard, &buckets).released_blocks, 32);
            buckets
        };
        verify(&engine);
        if dump {
            let manifest = engine.create_bucket_dump_manifest(1, buckets.clone()).unwrap();
            let decoded = decode_index_bytes(&manifest.index_bytes).unwrap();
            assert!(page_facts(&decoded).iter().all(|entry| entry.4 && !entry.5));
            engine.clear_dumped_bucket_dirty_state(1, &manifest);
            let mut shards = engine.shards.write().unwrap();
            let shard = shards.get_mut(&1).unwrap();
            assert!(shard.strings().values().all(|slot| slot.record.retained_page_state() == (false, false)));
            for bucket in &buckets { assert!(reload_released_bucket(shard, 1, *bucket)); }
            assert!(page_facts(shard).iter().all(|entry| !entry.4 && !entry.5));
        }
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let reopened = TemporalEngine::with_local_dirs(1 << 20,
            root.path().join("fresh-cache"), root.path().join("blocks"), root.path().join("indexes"));
        load(&reopened);
        verify(&reopened);
    }
}

#[test]
fn inline_release_keeps_mixed_owners_resident_and_a_reload_restores_an_unnamed_entry() {
    for mismatch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path());
        if !mismatch {
            assert!(engine.execute(ExecuteRequest { shard_id: 1,
                command: Command::HashSet { key: "key-0".into(), field: "field".into(), value: vec![7] },
            }).status.ok);
        }
        let bucket = engine.routing_bucket_for_key(1, "key-0");
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        if mismatch {
            let ghost = (0..128).map(|i| format!("ghost-{i}"))
                .find(|key| block_routing_bucket(key, 0, 7) == bucket).unwrap();
            let mut slot = shard.strings()["key-0"].clone();
            slot.set_address(&ElementEntry::from_parts(9, 999, 3, Some(0), Some(1)));
            shard.strings_mut().insert(ghost.into(), slot);
        }
        let count = shard.bucket_index.bucket_map.get(&bucket).unwrap().block_index.len();
        let states = shard.strings().iter().map(|(key, slot)| (key.clone(), slot.record)).collect::<BTreeMap<_, _>>();
        let outcome = release_bucket_blocks_preserving_inline_pages(shard, &[bucket]);
        if mismatch {
            // The release is local to the bucket: an entry the model maps hold that the bucket's
            // block list does not name is not found by walking the store. A reload rebuilds the
            // list from the model maps, so that entry comes back with the rest.
            assert_eq!(outcome.released_buckets, vec![bucket], "{outcome:?}");
            assert!(reload_released_bucket(shard, 1, bucket));
            assert_eq!(shard.bucket_index.bucket_map.get(&bucket).unwrap().block_index.len(), count + 1);
            // A released inline page is marked dirty; every address is unchanged.
            let after = shard.strings().iter().map(|(key, slot)| (key.clone(), format!("{:?}", slot.record.address()))).collect::<BTreeMap<_, _>>();
            assert_eq!(after, states.iter().map(|(key, record)| (key.clone(), format!("{:?}", record.address()))).collect::<BTreeMap<_, _>>());
        } else {
            assert!(outcome.released_buckets.is_empty(), "{outcome:?}");
            assert_eq!(outcome.refused_buckets, 1);
            assert_eq!(shard.bucket_index.bucket_map.get(&bucket).unwrap().block_index.len(), count);
            assert_eq!(shard.strings().iter().map(|(key, slot)| (key.clone(), slot.record)).collect::<BTreeMap<_, _>>(), states);
        }
    }
}

#[test]
fn inline_string_eviction_does_not_publish_or_encode_a_checkpoint() {
    for async_storage in [false, true] {
        for dump_fallback in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let engine = open(root.path());
            assert!(engine.set_config(crate::SetConfigRequest { shard_id: 1,
                config: crate::Config { version: 3, async_storage, ..Default::default() },
            }).ok);
            engine.use_sampled_eviction_for_test();
            let before = engine.write_ahead_log_store().stats(1);
            let frontier = engine.storage_dump_wal_sequence(1);
            let dirty = engine.shards.read().unwrap()[&1].bucket_index.bucket_map.values()
                .filter(|bucket| bucket.dirty()).count();
            crate::engine::reset_index_encode_counts();
            crate::engine::reset_bucket_dump_manifest_io_counts();
            let report = engine.apply_storage_eviction(1, 0, 1, dump_fallback, false);
            assert_eq!(report.bucket_index_buckets_released, 1, "{report:?}");
            assert!(report.dump_manifest_ids.is_empty(), "{report:?}");
            assert_eq!(report.dropped_object_count, 0);
            assert!(report.bucket_index_bytes_after < report.bucket_index_bytes_before);
            assert_eq!(crate::engine::index_encode_counts().encodes_total, 0);
            assert_eq!(crate::engine::bucket_dump_manifest_io_counts().file_writes, 0);
            assert_eq!(engine.storage_dump_wal_sequence(1), frontier);
            let after = engine.write_ahead_log_store().stats(1);
            assert_eq!((after.writes, after.flushes, after.syncs), (before.writes, before.flushes, before.syncs));
            assert_eq!(engine.shards.read().unwrap()[&1].bucket_index.bucket_map.values()
                .filter(|bucket| bucket.dirty()).count(), dirty);
            verify(&engine);
        }
    }
}

#[test]
fn cold_inline_pages_keep_overwrites_deletes_and_expiry_durable() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        let buckets = shard.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>();
        assert_eq!(release_bucket_blocks_preserving_inline_pages(shard, &buckets).released_blocks, 32);
    }
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::StringDelete { key: "key-1".into() },
    }).status.ok);
    assert_eq!(engine.shards.read().unwrap()[&1].bucket_index.bucket_owners.get().unwrap().entries(), 31);
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::StringSet { key: "key-0".into(), value: b"new".to_vec() },
    }).status.ok);
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::CommonExpire { key: "key-2".into(), ttl_ms: 1 },
    }).status.ok);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let expected = [("key-0", Some(b"new".to_vec())), ("key-1", None), ("key-2", None)];
    for (key, value) in &expected {
        assert_eq!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringGet { key: (*key).into() },
        }).response, CommandResponse::Bytes { value: value.clone() });
    }
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let reopened = TemporalEngine::with_local_dirs(1 << 20,
        root.path().join("fresh-cache"), root.path().join("blocks"), root.path().join("indexes"));
    load(&reopened);
    for (key, value) in expected {
        assert_eq!(reopened.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringGet { key: key.into() },
        }).response, CommandResponse::Bytes { value });
    }
}

#[test]
fn unsupported_mixed_owners_stay_resident_and_recover_after_dump_fallback() {
    for dump_fallback in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path());
        assert!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::HashSet { key: "key-0".into(), field: "field".into(), value: vec![7] },
        }).status.ok);
        engine.use_sampled_eviction_for_test();
        let mixed = engine.routing_bucket_for_key(1, "key-0");
        let report = engine.apply_storage_eviction(1, 0, 0, dump_fallback, false);
        assert!(report.bucket_index_buckets_released > 0, "{report:?}");
        assert_eq!(report.dropped_object_count, 0);
        assert_eq!(report.dump_manifest_ids.is_empty(), !dump_fallback, "{report:?}");
        assert_eq!(report.bucket_index_release_refused, 1);
        assert!(engine.shards.read().unwrap()[&1].bucket_index.bucket_map.get(&mixed)
            .unwrap().in_memory());
        verify(&engine);
        let hash = ExecuteRequest { shard_id: 1,
            command: Command::HashGet { key: "key-0".into(), field: "field".into() } };
        let expected = engine.execute(hash.clone());
        assert_eq!(expected.response, CommandResponse::Bytes { value: Some(vec![7]) });
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let reopened = TemporalEngine::with_local_dirs(1 << 20,
            root.path().join("fresh-cache"), root.path().join("blocks"), root.path().join("indexes"));
        load(&reopened);
        verify(&reopened);
        assert_eq!(reopened.execute(hash).response, expected.response);
    }
}
