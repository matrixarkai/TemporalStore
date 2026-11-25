// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_on(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        32 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn request(keys: &[&str], fields: &[&str]) -> BatchHashMultiGetRequest {
    BatchHashMultiGetRequest {
        shard_id: 1,
        keys: keys.iter().map(|key| key.to_string()).collect(),
        fields: fields.iter().map(|field| field.to_string()).collect(),
    }
}

#[test]
fn serialization_releases_guard_and_preserves_owned_and_shared_snapshots() {
    for size in [16, 8192] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        for key in ["first", "second"] {
            write(
                &engine,
                Command::HashMultiSet {
                    key: key.into(),
                    entries: vec![
                        ("value".into(), vec![7; size]),
                        ("neighbor".into(), b"held".to_vec()),
                    ],
                },
            );
        }
        let mut seen = Vec::new();
        engine
            .visit_batch_hash_multi_get(
                request(&["first", "second"], &["value", "neighbor"]),
                |index, result| {
                    assert!(
                        engine.shards.try_write().is_ok(),
                        "encoding must not hold the shard guard"
                    );
                    let values = result.unwrap();
                    assert_eq!(
                        matches!(values, execute_on_shard::ProjectedHashValues::Shared { .. }),
                        size == 8192
                    );
                    if index == 0 {
                        write(
                            &engine,
                            Command::HashMultiSet {
                                key: "first".into(),
                                entries: vec![("value".into(), b"new".to_vec())],
                            },
                        );
                        write(
                            &engine,
                            Command::CommonDelete {
                                key: "second".into(),
                            },
                        );
                    }
                    seen.push((index, values.into_owned()));
                },
            )
            .unwrap();
        assert_eq!(
            seen,
            vec![
                (0, vec![Some(vec![7; size]), Some(b"held".to_vec())]),
                (1, vec![Some(vec![7; size]), Some(b"held".to_vec())]),
            ]
        );
        let current =
            engine.batch_hash_multi_get_compact(request(&["first", "second"], &["value"]));
        assert_eq!(current.rows[0].0, vec![Some(b"new".to_vec())]);
        assert_eq!(current.rows[1].0, vec![None]);
    }
}

#[test]
fn snapshot_preserves_missing_duplicate_and_empty_projections() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    write(
        &engine,
        Command::HashMultiSet {
            key: "row".into(),
            entries: vec![("large".into(), vec![2; 8192]), ("empty".into(), vec![])],
        },
    );
    for fields in [vec![], vec!["empty", "missing", "large", "empty"]] {
        let req = request(&["row", "missing", "row"], &fields);
        let full = engine.batch_hash_multi_get_compact(req.clone());
        let bytes = engine.batch_hash_multi_get_compact_msgpack(req.clone());
        assert_eq!(bytes, rmp_serde::to_vec_named(&full).unwrap());
        engine
            .visit_batch_hash_multi_get(req, |_, values| {
                assert!(engine.shards.try_write().is_ok());
                assert_eq!(values.unwrap().len(), fields.len());
            })
            .unwrap();
    }
}

#[test]
fn oversized_projection_streams_after_releasing_the_guard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    write(
        &engine,
        Command::HashMultiSet {
            key: "large".into(),
            entries: vec![("value".into(), vec![3; 9 * 1024 * 1024])],
        },
    );
    let mut seen = 0;
    engine
        .visit_batch_hash_multi_get(
            request(&["missing", "large", "missing"], &["value"]),
            |index, row| {
                assert!(
                    engine.shards.try_write().is_ok(),
                    "large payloads must not retain the shard guard"
                );
                let row = row.unwrap();
                assert_eq!(index, seen);
                assert_eq!(
                    row.value_at(0).map(<[u8]>::len),
                    (index == 1).then_some(9 * 1024 * 1024)
                );
                seen += 1;
            },
        )
        .unwrap();
    assert_eq!(seen, 3);
}

#[test]
fn unloaded_shard_never_delivers_snapshot_rows() {
    let engine = TemporalEngine::default();
    let result = engine.visit_batch_hash_multi_get(request(&["missing"], &["value"]), |_, _| {
        panic!("an unloaded shard cannot deliver rows");
    });
    assert_eq!(result.unwrap_err().code, "shard_not_loaded");
}

#[test]
fn slab_snapshot_survives_overwrite_delete_and_reclaim_before_payload_loading() {
    let directory = tempfile::tempdir().unwrap();
    let engine = engine_on(directory.path());
    for (key, generation) in [("first", 7), ("second", 8)] {
        write(
            &engine,
            Command::HashMultiSet {
                key: key.into(),
                entries: vec![("value".into(), vec![generation; 8192])],
            },
        );
    }
    let addresses = {
        let shards = engine.shards.read().unwrap();
        ["first", "second"].map(|key| {
            let address = shards[&1]
                .bucket_index
                .hashes
                .get(key)
                .unwrap()
                .get("value")
                .unwrap()
                .clone();
            assert!(!crate::wal_record::is_wal_resident(address.block_slab_id()));
            address
        })
    };
    let old_slabs = addresses
        .iter()
        .map(|address| address.block_slab_id())
        .collect::<std::collections::BTreeSet<_>>();
    engine.cache.invalidate_shard(1).unwrap();
    let writer = engine.clone();
    AFTER_PROJECTED_READ_PLAN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move || {
            assert!(writer.shards.try_write().is_ok());
            assert!(writer.serving_admission.try_lock().is_some());
            std::thread::spawn(move || {
                writer.block_store.roll_slab_for_shard(1).unwrap();
                write(
                    &writer,
                    Command::HashMultiSet {
                        key: "first".into(),
                        entries: vec![("value".into(), vec![9; 8192])],
                    },
                );
                write(
                    &writer,
                    Command::CommonDelete {
                        key: "second".into(),
                    },
                );
                writer.cache.invalidate_shard(1).unwrap();
                writer
                    .block_store
                    .publish_live_block_bytes(Default::default());
                let collected = writer.block_store.gc_slabs_before(u64::MAX).unwrap();
                for address in addresses {
                    assert!(collected
                        .retained_live_block_slab_ids
                        .contains(&address.block_slab_id()));
                    assert!(writer.block_store.read(&address).is_ok());
                }
            })
            .join()
            .unwrap();
        }));
    });
    let response = engine.batch_hash_multi_get_compact(request(
        &["first", "second", "missing", "first"],
        &["value", "missing", "value"],
    ));
    assert!(response.status.ok && response.errors.is_empty());
    for (index, generation) in [(0, 7), (1, 8), (3, 7)] {
        assert_eq!(
            response.rows[index].0,
            vec![
                Some(vec![generation; 8192]),
                None,
                Some(vec![generation; 8192])
            ]
        );
    }
    assert_eq!(response.rows[2].0, vec![None; 3]);
    let released = engine.block_store.gc_slabs_before(u64::MAX).unwrap();
    assert!(old_slabs
        .iter()
        .all(|slab| released.removed_block_slab_ids.contains(slab)));
    let current = engine.batch_hash_multi_get_compact(request(&["first", "second"], &["value"]));
    assert!(current.status.ok);
    assert_eq!(current.rows[0].0, vec![Some(vec![9; 8192])]);
    assert_eq!(current.rows[1].0, vec![None]);
}

#[test]
fn wal_snapshot_survives_overwrite_delete_and_reclaim_before_payload_loading() {
    fn set_row(engine: &TemporalEngine, key: &str, generation: u8) {
        let (response, _) =
            engine.batch_execute_shared_hash_rows(crate::types::BatchHashMultiSetRequest {
                shard_id: 1,
                fields: vec!["value".into(), "neighbor".into()],
                ttl_ms: None,
                rows: vec![crate::types::BatchHashMultiSetRow(
                    key.into(),
                    vec![
                        crate::types::BatchHashMultiSetValue(vec![generation; 48]),
                        crate::types::BatchHashMultiSetValue(b"held".to_vec()),
                    ],
                )],
            });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(response.applied, 1);
    }
    let directory = tempfile::tempdir().unwrap();
    let engine = engine_on(directory.path());
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    for (key, generation) in [("first", 7), ("second", 8)] {
        set_row(&engine, key, generation);
    }
    {
        let shards = engine.shards.read().unwrap();
        for key in ["first", "second"] {
            let address = shards[&1]
                .bucket_index
                .hashes
                .get(key)
                .unwrap()
                .get("value")
                .unwrap();
            assert!(crate::wal_record::is_wal_resident(address.block_slab_id()));
        }
    }
    engine.cache.invalidate_shard(1).unwrap();
    let reclaim_to = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let target = reclaim_to.clone();
    let writer = engine.clone();
    AFTER_PROJECTED_READ_PLAN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move || {
            assert!(writer.shards.try_write().is_ok());
            assert!(writer.serving_admission.try_lock().is_some());
            let sequence = std::thread::spawn(move || {
                set_row(&writer, "first", 9);
                let sequence = writer.wal_store.stats(1).last_sequence;
                write(
                    &writer,
                    Command::CommonDelete {
                        key: "second".into(),
                    },
                );
                writer.cache.invalidate_shard(1).unwrap();
                writer.wal_store.clear_block_retention_floor(1);
                let held = writer
                    .wal_store
                    .gc_before_sequence_unchecked(1, sequence)
                    .unwrap();
                assert!(held.clamped_by_block_retention);
                assert!(held.effective_retain_from_sequence < sequence);
                sequence
            })
            .join()
            .unwrap();
            target.store(sequence, std::sync::atomic::Ordering::Relaxed);
        }));
    });
    let response = engine.batch_hash_multi_get_compact(request(
        &["first", "second", "missing", "first"],
        &["neighbor", "value", "missing", "neighbor"],
    ));
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.errors.is_empty());
    for (index, generation) in [(0, 7), (1, 8), (3, 7)] {
        assert_eq!(
            response.rows[index].0,
            vec![
                Some(b"held".to_vec()),
                Some(vec![generation; 48]),
                None,
                Some(b"held".to_vec())
            ]
        );
    }
    assert_eq!(response.rows[2].0, vec![None; 4]);
    AFTER_PROJECTED_READ_PLAN.with(|slot| assert!(slot.borrow().is_none()));
    assert_eq!(engine.wal_store.block_retention_floor(1), None);
    let released = engine
        .wal_store
        .gc_before_sequence_unchecked(1, reclaim_to.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap();
    assert!(!released.clamped_by_block_retention);
    assert!(released.records_after < released.records_before);
    let current = engine.batch_hash_multi_get_compact(request(&["first", "second"], &["value"]));
    assert!(current.status.ok);
    assert_eq!(current.rows[0].0, vec![Some(vec![9; 48])]);
    assert_eq!(current.rows[1].0, vec![None]);
}
