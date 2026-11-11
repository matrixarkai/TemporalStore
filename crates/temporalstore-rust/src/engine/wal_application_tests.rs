// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open_engine(root: &std::path::Path, asynchronous: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1, config: Config { version: 2, async_storage: asynchronous, ..Config::default() },
    }).ok);
    engine
}

fn set(key: &str, value: &[u8]) -> Command {
    Command::StringSet { key: key.into(), value: value.to_vec() }
}

fn execute(engine: &TemporalEngine, command: Command) {
    assert!(engine.execute(ExecuteRequest { shard_id: 1, command }).status.ok);
}

fn read(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringGet { key: key.into() },
    });
    assert!(response.status.ok);
    assert_eq!(response.response, CommandResponse::Bytes { value: Some(value.to_vec()) });
}

#[test]
fn wal_application_receipts_track_single_and_batch_writes_in_both_modes() {
    for asynchronous in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_engine(dir.path(), asynchronous);
        execute(&engine, set("single", b"one"));
        assert_eq!(engine.shards.read().unwrap()[&1].checkpoint_wal_sequence(), Some(1));
        assert!(engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1, commands: vec![
                set("batch-a", b"two"), set("batch-b", b"three"),
                Command::HashMultiSet { key: "hash".into(), entries: vec![
                    ("a".into(), b"alpha".to_vec()), ("b".into(), b"beta".to_vec()),
                ] },
                Command::HashMultiSet { key: "hash".into(), entries: vec![
                    ("c".into(), b"gamma".to_vec()), ("d".into(), b"delta".to_vec()),
                ] },
            ],
        }).status.ok);
        let applied = engine.wal_store.stats_without_rescan(1).last_sequence;
        assert!(applied > 1);
        let (records, _) = engine.wal_store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
        assert!(!records.last().unwrap().1.staged_blocks.is_empty(),
            "batch payloads must survive in the carrying record");
        let record = engine.wal_store.append_with_sync(1, set("pending", b"tail"), true).unwrap();
        assert!(record.sequence > applied);
        for kind in ["manifest", "catalog", "flush"] {
            let anchor = match kind {
                "manifest" => engine.create_bucket_dump_manifest(1, []).unwrap().wal_sequence,
                "catalog" => {
                    assert!(engine.dump_index_catalog(1));
                    decode_index_bytes(&std::fs::read(engine.index_path(1)).unwrap()).unwrap()
                        .applied_wal_sequence.unwrap()
                }
                "flush" => {
                    engine.flush_shard_index(1);
                    decode_index_bytes(&std::fs::read(engine.index_path(1)).unwrap()).unwrap()
                        .applied_wal_sequence.unwrap()
                }
                _ => unreachable!(),
            };
            assert_eq!(anchor, applied, "{kind}, async={asynchronous}");
        }
        drop(engine);
        let restored = open_engine(dir.path(), asynchronous);
        for (key, value) in [("single", b"one".as_slice()), ("batch-a", b"two"),
            ("batch-b", b"three"), ("pending", b"tail")]
        {
            read(&restored, key, value);
        }
        restored.cache.invalidate_shard(1).unwrap();
        for (field, value) in [("a", b"alpha".as_slice()), ("b", b"beta"),
            ("c", b"gamma"), ("d", b"delta")]
        {
            let response = restored.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashGet { key: "hash".into(), field: field.into() },
            });
            assert!(response.status.ok);
            assert_eq!(response.response, CommandResponse::Bytes { value: Some(value.to_vec()) });
        }
    }
}

#[test]
fn wal_application_discontinuous_images_refuse_publication_until_reload() {
    for asynchronous in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = open_engine(dir.path(), asynchronous);
        execute(&store, set("first", b"one"));
        store.flush_shard_index(1);
        let durable = std::fs::read(store.index_path(1)).unwrap();
        store.wal_store.append_with_sync(1, set("pending", b"two"), true).unwrap();
        execute(&store, set("last", b"three"));
        assert!(store.shards.read().unwrap()[&1].checkpoint_wal_sequence().is_none());
        assert!(store.create_bucket_dump_manifest(1, []).is_err());
        assert!(!store.dump_index_catalog(1));
        assert!(store.export_index_bytes(1).is_err());
        store.flush_shard_index(1);
        assert_eq!(std::fs::read(store.index_path(1)).unwrap(), durable);
        drop(store);
        let restored = open_engine(dir.path(), asynchronous);
        for (key, value) in [("first", b"one".as_slice()), ("pending", b"two"), ("last", b"three")] {
            read(&restored, key, value);
        }
        assert!(restored.create_bucket_dump_manifest(1, []).is_ok());
    }
}

#[test]
fn wal_application_range_tracker_rejects_missing_or_reordered_records() {
    let mut state = ShardState::default();
    assert_eq!(state.checkpoint_wal_sequence(), Some(0));
    state.note_wal_application(1, 4);
    assert_eq!(state.checkpoint_wal_sequence(), Some(4));
    state.note_wal_application(6, 7);
    assert_eq!(state.checkpoint_wal_sequence(), None);
    state.note_wal_application(8, 8);
    assert_eq!(state.checkpoint_wal_sequence(), None);
    let mut reordered = ShardState::default();
    reordered.note_wal_application(1, 2);
    reordered.note_wal_application(2, 2);
    assert_eq!(reordered.checkpoint_wal_sequence(), None);
}

#[test]
fn wal_application_maintenance_does_not_cover_an_unapplied_prefix() {
    for asynchronous in [false, true] {
        for expire in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = open_engine(dir.path(), asynchronous);
            execute(&store, set("victim", b"old"));
            if expire {
                execute(&store, Command::CommonExpire { key: "victim".into(), ttl_ms: 2 });
            }
            store.flush_shard_index(1);
            let base = std::fs::read(store.index_path(1)).unwrap();
            let index_sequence = store.index_log_store.stats(1).last_sequence;
            store.wal_store.append_with_sync(1, set("pending", b"tail"), true).unwrap();
            if expire {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let report = store.sweep_expired_records_with_request(ShardExpirySweepRequest {
                    shard_id: 1, load_cold_buckets: true,
                    max_hot_buckets_per_round: 100, max_cold_buckets_per_round: 100,
                    ..Default::default()
                }).unwrap();
                assert_eq!(report.expired_records_removed, 1);
            } else {
                let report = store.apply_storage_eviction(1, 1, 1, false, true);
                assert_eq!(report.dropped_object_count, 1);
            }
            assert_eq!(store.shards.read().unwrap()[&1].checkpoint_wal_sequence(), None);
            assert_eq!(store.index_log_store.stats(1).last_sequence, index_sequence);
            assert_eq!(std::fs::read(store.index_path(1)).unwrap(), base);
            assert!(store.create_bucket_dump_manifest(1, []).is_err());
            drop(store);
            let restored = open_engine(dir.path(), asynchronous);
            read(&restored, "pending", b"tail");
            let response = restored.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringGet { key: "victim".into() },
            });
            assert!(response.status.ok);
            assert_eq!(response.response, CommandResponse::Bytes { value: None });
            assert!(restored.create_bucket_dump_manifest(1, []).is_ok());
        }
    }
}

#[test]
fn wal_application_physical_import_starts_a_new_local_log_domain() {
    let dir = tempfile::tempdir().unwrap();
    let source = open_engine(dir.path(), false);
    for key in ["old-a", "old-b", "old-c"] {
        execute(&source, set(key, key.as_bytes()));
    }
    source.block_store.sync_durable().unwrap();
    let bytes = source.export_index_bytes(1).unwrap();
    assert_eq!(decode_index_bytes(&bytes).unwrap().applied_wal_sequence, Some(3));
    let (start_routing_bucket, end_routing_bucket) = source.shard_routing_range(1);
    let range = crate::engine::routing_range_stamp::RoutingRangeStamp {
        start_routing_bucket, end_routing_bucket,
    };
    drop(source);
    let destination = TemporalEngine::with_local_dirs(
        16 << 20, dir.path().join("dest-cache"), dir.path().join("blocks"),
        dir.path().join("dest-indexes"),
    );
    destination.install_index_bytes_in_empty_wal(1, &bytes, range).unwrap();
    let base = decode_index_bytes(&std::fs::read(destination.index_path(1)).unwrap()).unwrap();
    assert_eq!(base.applied_wal_sequence, Some(0));
    assert!(base.wal_resident_blocks.is_empty());
    destination.load_shard(1);
    for key in ["old-a", "old-b", "old-c"] { read(&destination, key, key.as_bytes()); }
    assert!(destination.install_index_bytes_in_empty_wal(1, &bytes, range).is_err());
    execute(&destination, set("new", b"local"));
    assert_eq!(destination.shards.read().unwrap()[&1].checkpoint_wal_sequence(), Some(1));
    destination.flush_shard_index(1);
    drop(destination);
    let restored = TemporalEngine::with_local_dirs(
        16 << 20, dir.path().join("dest-cache-two"), dir.path().join("blocks"),
        dir.path().join("dest-indexes"),
    );
    restored.load_shard(1);
    read(&restored, "new", b"local");
    for key in ["old-a", "old-b", "old-c"] { read(&restored, key, key.as_bytes()); }

    let hot_dir = tempfile::tempdir().unwrap();
    let hot = open_engine(hot_dir.path(), true);
    execute(&hot, set("hot", b"needs-source-log"));
    let hot_bytes = hot.export_index_bytes(1).unwrap();
    let empty_dir = tempfile::tempdir().unwrap();
    let empty = TemporalEngine::with_local_dirs(
        16 << 20, empty_dir.path().join("cache"), empty_dir.path().join("blocks"),
        empty_dir.path().join("indexes"),
    );
    assert!(empty.install_index_bytes_in_empty_wal(1, &hot_bytes, range).is_err());
    assert!(!empty.index_path(1).exists());
    empty.wal_store.append_with_sync(1, set("unapplied", b"tail"), true).unwrap();
    assert!(empty.install_index_bytes_in_empty_wal(1, &bytes, range).is_err());
}
