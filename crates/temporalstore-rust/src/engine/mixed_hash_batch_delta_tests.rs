// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn write(engine: &TemporalEngine, commands: Vec<Command>) {
    let result = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(result.status.ok, "{:?}", result.status);
    assert!(result.responses.iter().all(|row| row.status.ok));
}

fn seed(engine: &TemporalEngine, fields: usize) {
    let mut commands = vec![Command::HashMultiSet {
        key: "catalog".into(),
        entries: (0..fields)
            .map(|i| (format!("f{i:04}"), vec![i as u8; 128]))
            .collect(),
    }];
    commands.extend((0..32).map(|i| Command::SetAdd {
        key: "order".into(),
        member: format!("m{i:04}").into_bytes(),
    }));
    write(engine, commands);
}

fn mixed() -> Vec<Command> {
    vec![
        Command::HashMultiSet {
            key: "catalog".into(),
            entries: vec![("f0000".into(), b"new".to_vec())],
        },
        Command::SetRemove {
            key: "order".into(),
            member: b"m0000".to_vec(),
        },
        Command::SetAdd {
            key: "order".into(),
            member: b"new-order".to_vec(),
        },
    ]
}

#[test]
fn mixed_updates_use_one_bounded_atomic_record() {
    for fields in [64, 512] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        seed(&engine, fields);
        let before = engine
            .index_log_store
            .read_delta_records(1, 0)
            .unwrap()
            .last()
            .unwrap()
            .sequence;
        write(&engine, mixed());
        let records = engine
            .index_log_store
            .read_delta_records(1, before)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].upsert);
        assert!(records[0].applied_wal_sequence.is_some());
        assert_eq!(
            records[0]
                .items
                .iter()
                .filter(|item| item.object_key.as_ref() == "catalog" && !item.deleted)
                .count(),
            1
        );
        assert_eq!(
            records[0]
                .items
                .iter()
                .filter(|item| item.object_key.as_ref() == "catalog" && item.deleted)
                .count(),
            1
        );
        assert_eq!(records[0].items.len(), 4);
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&1).unwrap();
        for key in ["catalog", "order"] {
            let bucket =
                block_routing_bucket(key, shard.routing_range().0, shard.routing_range().1);
            let claim = shard
                .bucket_index
                .bucket_map
                .get(&bucket)
                .unwrap()
                .first_dirty_index_log_sequence;
            assert!(claim > 0 && claim <= records[0].sequence);
        }
        println!(
            "catalog_fields={fields} atomic_items={}",
            records[0].items.len()
        );
    }
}

#[test]
fn folded_mixed_records_keep_untouched_fields_and_order_members() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    write(&engine, mixed());
    let folded = engine.load_index(1, false).expect("folded index");
    let fields = folded.bucket_index.hashes.get("catalog").unwrap();
    assert_eq!(fields.len(), 64);
    let order = folded.bucket_index.sets.get("order").unwrap();
    assert_eq!(order.len(), 32);
    assert!(!order.contains_key(b"m0000".as_slice()));
    assert!(order.contains_key(b"new-order".as_slice()));
    let live = engine.shards.read().unwrap();
    assert_eq!(
        fields.get("f0000"),
        live.get(&1)
            .unwrap()
            .bucket_index
            .hashes
            .get("catalog")
            .unwrap()
            .get("f0000")
    );
}

#[test]
fn default_wal_recovery_keeps_sparse_updates_and_untouched_values() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        seed(&engine, 64);
        write(&engine, mixed());
    }
    let recovered = open(dir.path());
    for (field, expected) in [("f0000", b"new".to_vec()), ("f0063", vec![63; 128])] {
        let result = recovered.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet {
                key: "catalog".into(),
                field: field.into(),
            },
        });
        assert!(result.status.ok);
        let CommandResponse::Bytes { value } = result.response else {
            panic!("hash value")
        };
        assert_eq!(value, Some(expected));
    }
}

#[test]
fn a_partial_packed_rewrite_does_not_retire_a_shared_frame() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    write(
        &engine,
        vec![
            Command::HashMultiSet {
                key: "catalog".into(),
                entries: vec![
                    ("f0000".into(), vec![7; 8192]),
                    ("sibling".into(), b"keep".to_vec()),
                ],
            },
            Command::SetAdd {
                key: "order".into(),
                member: b"m0000".to_vec(),
            },
        ],
    );
    let before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence;
    write(&engine, mixed());
    let records = engine
        .index_log_store
        .read_delta_records(1, before)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0]
        .items
        .iter()
        .filter(|item| item.object_key.as_ref() == "catalog")
        .all(|item| !item.deleted));
    let folded = engine.load_index(1, false).unwrap();
    assert_eq!(folded.bucket_index.hashes.get("catalog").unwrap().len(), 2);
}

#[test]
fn a_hash_removal_on_the_same_key_keeps_snapshot_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    let before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence;
    let mut commands = mixed();
    commands.push(Command::HashDelete {
        key: "catalog".into(),
        field: "f0063".into(),
    });
    write(&engine, commands);
    let records = engine
        .index_log_store
        .read_delta_records(1, before)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(!records[0].upsert);
    let folded = engine.load_index(1, false).unwrap();
    assert_eq!(folded.bucket_index.hashes.get("catalog").unwrap().len(), 63);
}

#[test]
fn the_atomic_record_has_one_complete_wal_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    let before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence;
    write(&engine, mixed());
    let records = engine
        .index_log_store
        .read_delta_records(1, before)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(crate::index_log::delta_record_is_reflected_by(
        &records[0],
        u64::MAX
    ));
    assert!(records[0].applied_wal_sequence.is_some());
}

#[test]
fn losing_the_atomic_delta_replays_the_complete_wal_batch() {
    struct RestoreRolling;
    impl Drop for RestoreRolling {
        fn drop(&mut self) {
            crate::index_log::set_index_log_segment_bytes_for_test(None);
        }
    }
    crate::index_log::set_index_log_segment_bytes_for_test(Some(1));
    let _restore = RestoreRolling;
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        seed(&engine, 64);
        let before = engine
            .index_log_store
            .read_delta_records(1, 0)
            .unwrap()
            .last()
            .unwrap()
            .sequence;
        write(&engine, mixed());
        let all = engine
            .index_log_store
            .read_delta_records(1, before)
            .unwrap();
        assert_eq!(all.len(), 1);
        std::fs::remove_file(dir.path().join("indexes/indexlogs/shard-1.indexlog.bin")).unwrap();
        let remaining = engine
            .index_log_store
            .read_delta_records(1, before)
            .unwrap();
        assert!(remaining.is_empty());
    }
    let recovered = open(dir.path());
    let value = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "catalog".into(),
            field: "f0000".into(),
        },
    });
    assert!(value.status.ok);
    assert!(
        matches!(value.response, CommandResponse::Bytes { value: Some(ref bytes) } if bytes == b"new")
    );
    let shards = recovered.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    assert_eq!(shard.bucket_index.hashes.get("catalog").unwrap().len(), 64);
    let order = shard.bucket_index.sets.get("order").unwrap();
    assert_eq!(order.len(), 32);
    assert!(!order.contains_key(b"m0000".as_slice()));
    assert!(order.contains_key(b"new-order".as_slice()));
}

#[test]
fn durable_dump_reclaims_the_atomic_delta_by_its_wal_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    write(&engine, mixed());
    let records = engine.index_log_store.read_delta_records(1, 0).unwrap();
    let delta = records.last().unwrap();
    assert!(delta.applied_wal_sequence.is_some());
    let (anchor, meta_sequence) = engine.dump_index_catalog_anchored(1).expect("durable dump");
    assert!(meta_sequence > delta.sequence);
    engine
        .index_log_store
        .gc_reflected_before_anchor(1, anchor, meta_sequence, 0)
        .unwrap();
    let retained = engine.index_log_store.read_delta_records(1, 0).unwrap();
    assert!(retained
        .iter()
        .all(|record| record.sequence >= meta_sequence));
    let folded = engine.load_index(1, false).expect("dump remains readable");
    assert_eq!(folded.bucket_index.hashes.get("catalog").unwrap().len(), 64);
}

#[test]
fn pure_hash_and_set_appends_keep_one_sparse_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 512);
    let before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence;
    write(
        &engine,
        vec![
            Command::HashSet {
                key: "catalog".into(),
                field: "f0000".into(),
                value: b"new".to_vec(),
            },
            Command::SetAdd {
                key: "order".into(),
                member: b"new-order".to_vec(),
            },
        ],
    );
    let records = engine
        .index_log_store
        .read_delta_records(1, before)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].upsert);
    assert_eq!(
        records[0].items.iter().filter(|item| !item.deleted).count(),
        2
    );
    assert!(records[0].items.len() <= 3);
}

#[test]
fn a_counter_adjustment_keeps_the_entire_batch_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    let before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence;
    let mut commands = mixed();
    commands.push(Command::ZSetCountAdjust {
        key: "counter".into(),
        member: b"count".to_vec(),
        increment: 1,
    });
    assert!(!can_split(&commands));
    write(&engine, commands);
    let records = engine
        .index_log_store
        .read_delta_records(1, before)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].applied_wal_sequence.is_some());
    let expected = {
        let shards = engine.shards.read().unwrap();
        shards
            .get(&1)
            .unwrap()
            .bucket_index
            .zsets
            .get("counter")
            .unwrap()
            .get(b"count".as_slice())
            .unwrap()
            .0
    };
    let folded = engine.load_index(1, false).expect("counter fold");
    assert_eq!(
        folded
            .bucket_index
            .zsets
            .get("counter")
            .unwrap()
            .get(b"count".as_slice())
            .unwrap()
            .0,
        expected
    );
}

#[test]
fn a_newer_base_keeps_later_order_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    write(&engine, mixed());
    write(
        &engine,
        vec![Command::SetAdd {
            key: "order".into(),
            member: b"later".to_vec(),
        }],
    );
    engine
        .dump_index_catalog_anchored(1)
        .expect("durable newer base");
    let folded = engine.load_index(1, false).expect("folded newer base");
    let members = folded.bucket_index.sets.get("order").unwrap();
    assert_eq!(members.len(), 33);
    assert!(members.contains_key(b"later".as_slice()));
}

#[test]
fn keys_with_deadlines_stay_out_of_sparse_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    assert!(keys_have_no_deadlines(shard, &mixed()));
    shard.expires_at_ms_mut().insert("catalog".into(), u64::MAX);
    assert!(!keys_have_no_deadlines(shard, &mixed()));
}

#[test]
fn a_live_set_frame_is_never_retired() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 64);
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let mut captured = SetPredecessors::default();
    assert!(captured.capture(
        shard,
        1,
        &Command::SetRemove {
            key: "order".into(),
            member: b"m0000".to_vec()
        }
    ));
    assert!(captured.fully_vacated(shard).is_none());
}
