// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[cfg(target_os = "linux")]
#[path = "hash_execute_scale.rs"]
mod scale;

fn open(dir: &std::path::Path, async_storage: bool, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn write(engine: &TemporalEngine, entries: Vec<(String, Vec<u8>)>) {
    let before = storage_bucket_internals::bucket_block_upserts();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiSet {
            key: "row".into(),
            entries,
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(
        storage_bucket_internals::bucket_block_upserts() - before,
        0,
        "a filed writer must not repeat post-write filing"
    );
}

fn read(engine: &TemporalEngine, fields: &[&str]) -> Vec<Option<Vec<u8>>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiGet {
            key: "row".into(),
            fields: fields.iter().map(|field| field.to_string()).collect(),
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Values { values } = response.response else {
        panic!("hash values expected")
    };
    values
}

fn assert_dirty_hash_blocks(engine: &TemporalEngine) {
    engine.shards.with_state(1, |owner| {
        let shard = owner.expect("shard 1");
        assert!(shard.dirty_objects.contains("row"));
        for at in shard
            .bucket_index
            .block_refs_for("hash", "row", None)
            .unwrap()
        {
            assert!(shard
                .bucket_index
                .bucket_map
                .get(&at.routing_bucket)
                .unwrap()
                .block_index
                .get(&at.block_ref_key)
                .unwrap()
                .dirty());
        }
    })
}

#[test]
fn single_hash_write_preserves_layout_and_avoids_repeated_filing_at_growing_sizes() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage, "cache");
        for count in [0, 1, 128, 1024] {
            let before = storage_bucket_internals::bucket_block_upserts();
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashMultiSet {
                    key: "row".into(),
                    entries: (0..count)
                        .map(|index| {
                            (
                                format!("field-{index:04}"),
                                (index as u64).to_be_bytes().to_vec(),
                            )
                        })
                        .collect(),
                },
            });
            assert!(response.status.ok);
            assert_eq!(storage_bucket_internals::bucket_block_upserts() - before, 0);
            if count == 0 {
                continue;
            }
            engine.shards.with_state_mut(1, |owner| {
                let shard = owner.unwrap();
                let fields = shard.bucket_index.hashes.get("row").unwrap();
                let addresses = fields
                    .iter()
                    .map(|(_, address)| live_block_key(address))
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(addresses.len(), count, "small-field layout must not change");
                assert_eq!(
                    shard
                        .bucket_index
                        .block_refs_for("hash", "row", None)
                        .unwrap()
                        .len(),
                    count
                );
                let names = (0..count)
                    .map(|index| format!("field-{index:04}"))
                    .collect::<Vec<_>>();
                let before = storage_bucket_internals::bucket_block_upserts();
                assert!(storage_bucket_internals::sync_written_hash_fields(
                    shard,
                    1,
                    "row",
                    names.iter().map(String::as_str)
                ));
                assert_eq!(
                    storage_bucket_internals::bucket_block_upserts() - before,
                    count as u64,
                    "the negative control must still detect the old redundant sync"
                );
            });
        }
    }
}

#[test]
fn single_hash_duplicates_delete_readd_and_siblings_survive_cold_reopen() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage, "cache");
        write(
            &engine,
            vec![
                ("a".into(), b"first".to_vec()),
                ("b".into(), b"held".to_vec()),
            ],
        );
        assert!(
            engine
                .execute_durable(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashDelete {
                        key: "row".into(),
                        field: "a".into()
                    }
                })
                .status
                .ok
        );
        write(
            &engine,
            vec![
                ("a".into(), b"middle".to_vec()),
                ("c".into(), b"added".to_vec()),
                ("a".into(), b"last".to_vec()),
            ],
        );
        let expected = vec![
            Some(b"last".to_vec()),
            Some(b"held".to_vec()),
            Some(b"added".to_vec()),
        ];
        assert_eq!(read(&engine, &["a", "b", "c"]), expected);
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let recovered = open(dir.path(), async_storage, "cold-cache");
        assert_eq!(read(&recovered, &["a", "b", "c"]), expected);
        recovered.shards.with_state(1, |owner| {
            assert_eq!(
                owner.expect("shard 1")
                    .bucket_index
                    .block_refs_for("hash", "row", None)
                    .unwrap()
                    .len(),
                3
            );
        });
    }
}

#[test]
fn single_packed_hash_rewrite_keeps_untouched_siblings_after_restart() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage, "cache");
        let original = vec![7; 16 * 1024];
        let replacement = vec![9; 16 * 1024];
        write(
            &engine,
            vec![
                ("large".into(), original),
                ("held".into(), b"sibling".to_vec()),
            ],
        );
        write(&engine, vec![("large".into(), replacement.clone())]);
        assert_dirty_hash_blocks(&engine);
        let expected = vec![Some(replacement), Some(b"sibling".to_vec())];
        assert_eq!(read(&engine, &["large", "held"]), expected);
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let recovered = open(dir.path(), async_storage, "cold-cache");
        assert_eq!(read(&recovered, &["large", "held"]), expected);
        write(&recovered, vec![("extra".into(), vec![1])]);
        assert_dirty_hash_blocks(&recovered);
        assert_eq!(read(&recovered, &["large", "held"]), expected);
    }
}

#[test]
fn durable_single_hash_ack_replays_with_async_config_and_a_cold_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), true, "cache");
    let before = storage_bucket_internals::bucket_block_upserts();
    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::HashMultiSet {
                    key: "row".into(),
                    entries: vec![("acked".into(), b"durable value".to_vec())]
                },
            })
            .status
            .ok
    );
    assert_eq!(storage_bucket_internals::bucket_block_upserts() - before, 0);
    drop(engine);
    let recovered = open(dir.path(), true, "cold-cache");
    assert_eq!(
        read(&recovered, &["acked"]),
        vec![Some(b"durable value".to_vec())]
    );
}

#[test]
fn repeated_dirty_hash_writes_preserve_block_marks_and_checkpoint_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false, "cache");
    write(&engine, vec![("a".into(), vec![1]), ("b".into(), vec![2])]);
    let state = |engine: &TemporalEngine| {
        engine.shards.with_state(1, |owner| {
            let shard = owner.expect("shard 1");
            let refs = shard
                .bucket_index
                .block_refs_for("hash", "row", None)
                .unwrap();
            let marks = refs
                .iter()
                .map(|at| {
                    shard
                        .bucket_index
                        .bucket_map
                        .get(&at.routing_bucket)
                        .unwrap()
                        .block_index
                        .get(&at.block_ref_key)
                        .unwrap()
                        .dirty()
                })
                .collect::<Vec<_>>();
            let generation = shard
                .bucket_index
                .bucket_map
                .get(&refs[0].routing_bucket)
                .unwrap()
                .dirty_generation;
            (shard.dirty_objects.contains("row"), marks, generation)
        })
    };
    let before = state(&engine);
    assert!(before.0 && before.1.iter().all(|dirty| *dirty));
    let visits = storage_bucket_internals::dirty_propagation_visits();
    write(&engine, vec![("c".into(), vec![3])]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        0
    );
    let after = state(&engine);
    assert!(after.0 && after.1.iter().all(|dirty| *dirty));
    assert!(after.2 > before.2);

    engine.unload_shard(1);
    engine.load_shard(1);
    let clean = state(&engine);
    assert!(!clean.0 && clean.1.iter().all(|dirty| !dirty));
    let visits = storage_bucket_internals::dirty_propagation_visits();
    write(&engine, vec![("d".into(), vec![4])]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        4
    );
    let after_reload = state(&engine);
    assert!(after_reload.0 && after_reload.1.iter().all(|dirty| *dirty));
    assert!(after_reload.2 > clean.2);
    let visits = storage_bucket_internals::dirty_propagation_visits();
    write(&engine, vec![("e".into(), vec![5])]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        0
    );
    assert!(state(&engine).1.iter().all(|dirty| *dirty));
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let recovered = open(dir.path(), false, "cold-cache");
    assert_eq!(
        read(&recovered, &["a", "b", "c", "d", "e"]),
        vec![
            Some(vec![1]),
            Some(vec![2]),
            Some(vec![3]),
            Some(vec![4]),
            Some(vec![5])
        ]
    );
}

#[test]
fn batch_hash_dirty_marks_keep_the_first_cold_mark_and_legacy_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false, "cache");
    write(&engine, vec![("a".into(), vec![1]), ("b".into(), vec![2])]);
    let batch = |commands| {
        let result = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(result.status.ok);
        assert!(result.responses.iter().all(|response| response.status.ok));
    };
    let set = |field: &str, value| Command::HashMultiSet {
        key: "row".into(),
        entries: vec![(field.to_string(), vec![value])],
    };
    let dirty = || {
        engine.shards.with_state(1, |owner| {
            let shard = owner.expect("shard 1");
            assert!(shard.dirty_objects.contains("row"));
            for at in shard
                .bucket_index
                .block_refs_for("hash", "row", None)
                .unwrap()
            {
                assert!(shard
                    .bucket_index
                    .bucket_map
                    .get(&at.routing_bucket)
                    .unwrap()
                    .block_index
                    .get(&at.block_ref_key)
                    .unwrap()
                    .dirty());
            }
        })
    };
    let visits = storage_bucket_internals::dirty_propagation_visits();
    batch(vec![set("c", 3), set("d", 4)]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        0
    );
    dirty();
    engine.unload_shard(1);
    engine.load_shard(1);
    assert!(!engine.shards.with_state(1, |owner| owner.unwrap().dirty_objects.contains("row")));
    let visits = storage_bucket_internals::dirty_propagation_visits();
    batch(vec![set("e", 5), set("f", 6)]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        6
    );
    dirty();
    let visits = storage_bucket_internals::dirty_propagation_visits();
    batch(vec![Command::HashSet {
        key: "row".into(),
        field: "g".into(),
        value: vec![7],
    }]);
    assert_eq!(
        storage_bucket_internals::dirty_propagation_visits() - visits,
        7,
        "writers without a filing proof retain the conservative propagation"
    );
    dirty();
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let recovered = open(dir.path(), false, "cold-cache");
    assert_eq!(
        read(&recovered, &["a", "b", "c", "d", "e", "f", "g"]),
        (1..=7).map(|value| Some(vec![value])).collect::<Vec<_>>()
    );
}

#[test]
fn empty_hash_write_after_expiry_does_not_claim_a_filed_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false, "cache");
    {
        let _clock = ReplayClockGuard::enter(Some(1000));
        assert!(
            engine
                .execute_durable(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashMultiSetWithTtl {
                        key: "row".into(),
                        ttl_ms: 10,
                        entries: vec![("expired".into(), b"old".to_vec())]
                    }
                })
                .status
                .ok
        );
    }
    {
        let _clock = ReplayClockGuard::enter(Some(1011));
        write(&engine, Vec::new());
        assert_eq!(read(&engine, &["expired"]), vec![None]);
        write(&engine, vec![("fresh".into(), b"new".to_vec())]);
        assert_eq!(
            read(&engine, &["expired", "fresh"]),
            vec![None, Some(b"new".to_vec())]
        );
    }
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let recovered = open(dir.path(), false, "cold-cache");
    assert_eq!(
        read(&recovered, &["expired", "fresh"]),
        vec![None, Some(b"new".to_vec())]
    );
}
