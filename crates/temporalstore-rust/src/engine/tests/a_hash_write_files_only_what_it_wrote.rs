// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A HASH FIELD WRITE FILES THE FIELD IT WROTE, NOT THE FIELDS THE HASH ALREADY HAD.
//!
//! A single-command hash write ended with a whole-object index sync that re-filed EVERY field the
//! hash held. That sync was given a filter -- "only the fields whose block is not already filed" --
//! so the ordinary case would cost nothing, and the filter asks a lookup that resolves no named
//! element name (`names_the_only_slot`: a `Some` element name has always found nothing). So it never
//! filtered, and one field write cost the fields the hash already had:
//!
//! ```text
//!   fields already in the hash        allocations per overwrite
//!                10                            118
//!               100                            571
//!             1,000                          5,080
//! ```
//!
//! 87% of the write at 100 fields. The batch path files only the element names a command wrote and has
//! done since it existed; the single-command path now does the same for a hash write.
//!
//! TWO ARMS, BECAUSE THEY GUARD DIFFERENT FAILURES.
//!   * `a_hash_write_leaves_every_field_filed_exactly_once` runs in a default build. Narrowing the
//!     sync must not leave a field unfiled or a stale block behind, and nothing else in the suite
//!     compares the hash's own addresses with the index's.
//!   * `a_hash_field_write_does_not_pay_for_the_fields_it_already_has` needs `alloc-probe` and
//!     `--ignored`. It varies the FIELD COUNT, because only work proportional to the fields already
//!     held can make the per-write count depend on it, and it carries its own control: a read of
//!     the whole hash is proportional to the fields by construction, so the same probe must see
//!     growth there or it is blind to the dimension.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn identity(address: &crate::ElementEntry) -> String {
    format!(
        "{:?}/{:?}/{:?}/{:?}",
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id()
    )
}

#[test]
fn a_hash_write_leaves_every_field_filed_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    for object in 0..3 {
        let key = format!("h-{object}");
        for field in 0..40 {
            run(
                &engine,
                Command::HashSet {
                    key: key.clone(),
                    field: format!("f{field:03}"),
                    value: vec![b'a'; 16],
                },
            );
        }
        // Overwrites replace a field's block; a stale one left behind would show as a surplus.
        for field in 0..10 {
            run(
                &engine,
                Command::HashSet {
                    key: key.clone(),
                    field: format!("f{field:03}"),
                    value: vec![b'b'; 24],
                },
            );
        }
        run(
            &engine,
            Command::HashMultiSet {
                key: key.clone(),
                entries: (40..46)
                    .map(|field| (format!("f{field:03}"), vec![b'c'; 8]))
                    .collect(),
            },
        );
        for _ in 0..3 {
            run(
                &engine,
                Command::HashIncrBy {
                    key: key.clone(),
                    field: "counter".to_string(),
                    increment: 1,
                },
            );
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "no bucket index was built, so the arm that narrows the sync never ran"
    );

    let mut checked_fields = 0usize;
    for object in 0..3 {
        let key = format!("h-{object}");
        let fields = shard.bucket_index.hashes.get(key.as_str()).expect("the hash exists");
        let held: BTreeSet<String> = fields.iter().map(|(_, address)| identity(address)).collect();

        let refs = shard
            .bucket_index
            .object_block_refs("hash", &key)
            .expect("the hash has block refs");
        let mut filed: Vec<String> = Vec::new();
        for block_ref in refs.iter() {
            let block = shard
                .bucket_index
                .bucket_map
                .get(&block_ref.routing_bucket)
                .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
                .expect("a ref names a block the index holds");
            if !block.deleted() {
                filed.push(identity(&block.address));
            }
        }
        let filed_set: BTreeSet<String> = filed.iter().cloned().collect();

        // DENOMINATOR: 40 + 6 + 1 fields. A run that wrote fewer cannot have exercised the sync.
        assert_eq!(47, held.len(), "{key}: the hash holds {} distinct blocks", held.len());
        assert_eq!(
            held, filed_set,
            "{key}: the blocks the hash holds and the blocks the index files differ"
        );
        assert_eq!(
            filed.len(),
            filed_set.len(),
            "{key}: a block is filed twice, so a replaced field left its predecessor behind"
        );
        checked_fields += held.len();
    }
    assert_eq!(141, checked_fields);
}

#[test]
fn a_new_packed_hash_files_one_block_for_all_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    run(
        &engine,
        Command::HashMultiSet {
            key: "packed".to_string(),
            entries: vec![
                ("first".to_string(), b"one".to_vec()),
                ("second".to_string(), b"two".to_vec()),
                ("payload".to_string(), vec![b'x'; 8 * 1024]),
            ],
        },
    );

    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let fields = shard.bucket_index.hashes.get("packed").expect("hash exists");
        assert_eq!(fields.len(), 3);
        let addresses: BTreeSet<_> = fields.iter().map(|(_, address)| identity(address)).collect();
        assert_eq!(addresses.len(), 1);
        assert_eq!(shard.bucket_index.object_block_refs("hash", "packed").unwrap().len(), 1);
    }

    run(
        &engine,
        Command::HashSet {
            key: "packed".to_string(),
            field: "first".to_string(),
            value: b"updated".to_vec(),
        },
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll { key: "packed".to_string() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    let CommandResponse::HashEntries { entries } = response.response else {
        panic!("expected hash entries");
    };
    let values = entries.into_iter().collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get("first"), Some(&b"updated".to_vec()));
    assert_eq!(values.get("second"), Some(&b"two".to_vec()));
    assert_eq!(values.get("payload").map(Vec::len), Some(8 * 1024));
}

fn grouped_row(engine: &TemporalEngine, fields: &[&str], generation: u8) {
    use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
    let (response, completed) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: fields.iter().map(|field| field.to_string()).collect(),
        rows: vec![BatchHashMultiSetRow("grouped".to_string(), fields.iter()
            .map(|_| BatchHashMultiSetValue(vec![generation; 48])).collect())],
        ttl_ms: None,
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(response.applied, 1);
    assert_eq!(completed, ["grouped"]);
    let records = engine.write_ahead_log_store().scan(1, 0, u64::MAX, u64::MAX).unwrap();
    let record = crate::wal::decode_wal_line(&records.last().unwrap().1).unwrap();
    assert_eq!(record.outcomes.iter().filter(|item| item.kind == "hash" && !item.deleted).count(), 1);
}

fn grouped_values(engine: &TemporalEngine) -> std::collections::BTreeMap<String, Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll { key: "grouped".to_string() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    let CommandResponse::HashEntries { entries } = response.response else { panic!("hash entries") };
    entries.into_iter().collect()
}

fn grouped_block_count(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let held: BTreeSet<_> = shard.bucket_index.hashes.get("grouped").unwrap()
        .values().map(identity).collect();
    let refs = shard.bucket_index.object_block_refs("hash", "grouped").unwrap();
    let filed: BTreeSet<_> = refs.iter().map(|block_ref| {
        let bucket = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket).unwrap();
        let block = bucket.block_index.get(&block_ref.block_ref_key).unwrap();
        assert!(!block.deleted());
        identity(&block.address)
    }).collect();
    assert_eq!(refs.len(), filed.len());
    assert_eq!(held, filed);
    held.len()
}

fn grouped_tombstone(engine: &TemporalEngine, field: &str) -> bool {
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let (start, end) = shard.routing_range();
    let bucket = shard.bucket_index.bucket_map.get(&block_routing_bucket("grouped", start, end)).unwrap();
    bucket.tombstone_element_block("grouped",
        crate::engine::storage_bucket_internals::stored_model_kind("hash"), field).is_some()
}

#[test]
fn grouped_packed_hash_overwrites_file_one_outcome_and_one_live_block() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let fields: Vec<String> = (0..21).map(|i| format!("f{i}")).collect();
    let names: Vec<&str> = fields.iter().map(String::as_str).collect();
    for generation in 0..5 {
        grouped_row(&engine, &names, generation);
        assert_eq!(grouped_block_count(&engine), 1);
        let values = grouped_values(&engine);
        assert_eq!(values.len(), 21);
        assert!(values.values().all(|value| value == &vec![generation; 48]));
    }
}

#[test]
fn grouped_packed_hash_retires_multiple_blocks_but_preserves_untouched_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for field in ["a0", "a1", "a2", "b0"] {
        run(&engine, Command::HashSet {
            key: "grouped".to_string(), field: field.to_string(), value: vec![0; 48],
        });
    }
    assert_eq!(grouped_block_count(&engine), 4);
    grouped_row(&engine, &["a0", "a1", "a2"], 1);
    assert_eq!(grouped_block_count(&engine), 2);
    let values = grouped_values(&engine);
    assert_eq!(values["b0"], vec![0; 48]);
    assert_eq!(values["a2"], vec![1; 48]);
    grouped_row(&engine, &["a0", "a1", "a2", "b0"], 2);
    assert_eq!(grouped_block_count(&engine), 1);
}

#[test]
fn grouped_packed_hash_partial_readds_survive_wal_and_dump_recovery() {
    for async_storage in [false, true] {
        for dump in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = engine_on(dir.path());
            assert!(engine.set_config(SetConfigRequest {
                shard_id: 1,
                config: Config { version: 2, async_storage, ..Config::default() },
            }).ok);
            grouped_row(&engine, &["a0", "a1", "b0", "b1"], 0);
            grouped_row(&engine, &["a0", "a1"], 1);
            assert_eq!(grouped_block_count(&engine), 2);
            for field in ["a1", "b1"] {
                run(&engine, Command::HashDelete { key: "grouped".to_string(), field: field.to_string() });
            }
            assert!(grouped_tombstone(&engine, "a1"));
            assert!(grouped_tombstone(&engine, "b1"));
            grouped_row(&engine, &["a0", "a1"], 2);
            assert!(!grouped_tombstone(&engine, "a1"));
            assert!(grouped_tombstone(&engine, "b1"));
            let expected = grouped_values(&engine);
            assert_eq!(expected.len(), 3);
            assert_eq!(expected["b0"], vec![0; 48]);
            assert_eq!(expected["a0"], vec![2; 48]);
            assert_eq!(expected["a1"], vec![2; 48]);
            grouped_block_count(&engine);
            engine.write_ahead_log_store().flush(1).unwrap();
            if dump {
                let result = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
                    shard_id: 1, min_undumped_wal_records: 0, min_undumped_wal_bytes: 0,
                    ..crate::engine::reports::StorageLifecycleRequest::default()
                });
                assert!(result.dump_manifest.is_some(), "{result:?}");
            }
            drop(engine);
            let recovered = engine_on(dir.path());
            assert_eq!(grouped_values(&recovered), expected, "async={async_storage}, dump={dump}");
            grouped_block_count(&recovered);
        }
    }
}

#[test]
fn grouped_packed_hash_replays_multiple_frames_of_one_object_in_one_record() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage, ..Config::default() },
        }).ok);
        let mut commands = Vec::new();
        for (timestamp, fields, generation) in [
            (1, vec!["a0", "a1", "b0", "b1"], 0),
            (2, vec!["a0", "a1"], 1),
        ] {
            commands.push(Command::HashMultiSetIfTimestampOlder {
                key: "grouped".to_string(), timestamp_field: "stamp".to_string(),
                timestamp_seconds: timestamp, timestamp_nanos: 0, ttl_ms: None,
                entries: fields.iter().map(|field| (field.to_string(), vec![generation; 48])).collect(),
            });
        }
        let response = engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands });
        assert!(response.status.ok, "{:?}", response.status);
        assert!(response.responses.iter().all(|response| response.status.ok));
        let expected = grouped_values(&engine);
        assert_eq!(expected["a0"], vec![1; 48]);
        assert_eq!(expected["b0"], vec![0; 48]);
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let recovered = engine_on(dir.path());
        if async_storage {
            recovered.compact_shard_blocks(1).unwrap();
            assert_eq!(recovered.registration_count_for_test(1), 0);
        }
        assert_eq!(grouped_values(&recovered), expected, "async={async_storage}");
        grouped_block_count(&recovered);
    }
}

#[test]
fn durable_shared_rows_defer_materialization_but_commit_their_wal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    grouped_row(&engine, &["a0", "a1", "b0"], 1);
    grouped_row(&engine, &["a0", "a1"], 2);
    assert_eq!(engine.block_store.stats().writes, 0);
    let stats = engine.write_ahead_log_store().stats(1);
    assert!(stats.last_sequence > 0);
    assert_eq!(stats.last_flushed_sequence, stats.last_sequence);
    assert!(engine.registration_count_for_test(1) > 0);
    engine.cache.invalidate_shard(1).unwrap();
    let expected = grouped_values(&engine);
    assert_eq!(expected["a0"], vec![2; 48]);
    assert_eq!(expected["b0"], vec![1; 48]);
    drop(engine);
    let recovered = engine_on(dir.path());
    assert_eq!(grouped_values(&recovered), expected);
    recovered.compact_shard_blocks(1).unwrap();
    assert!(recovered.block_store.stats().writes > 0);
    assert_eq!(recovered.registration_count_for_test(1), 0);
    drop(recovered);
    assert_eq!(grouped_values(&engine_on(dir.path())), expected);
}

#[test]
fn durable_shared_rows_keep_physical_storage_quota_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config { version: 2, maxmemory_bytes: Some(64 * 1024 * 1024), ..Config::default() },
    }).ok);
    grouped_row(&engine, &["a0", "a1"], 1);
    assert!(engine.block_store.stats().writes > 0);
    assert_eq!(engine.registration_count_for_test(1), 0);
    assert_eq!(grouped_values(&engine)["a1"], vec![1; 48]);
}

#[test]
fn durable_shared_rows_do_not_ack_a_failed_wal_commit() {
    use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    grouped_row(&engine, &["a0"], 1);
    let path = dir.path().join("indexes/wals/shard-1.wal.bin");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let (response, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1, fields: vec!["a0".to_string()], ttl_ms: None,
        rows: vec![BatchHashMultiSetRow("other".to_string(), vec![BatchHashMultiSetValue(vec![2; 48])])],
    });
    assert!(!response.status.ok);
    assert_eq!(response.status.code, "wal_commit_failed");
    assert_eq!(response.applied, 0);
}

#[test]
fn durable_shared_rows_recover_mixed_large_and_wal_resident_frames() {
    use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let (response, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1, fields: vec!["payload".to_string()], ttl_ms: None,
        rows: vec![
            BatchHashMultiSetRow("large".to_string(), vec![BatchHashMultiSetValue(vec![9; 16 * 1024])]),
            BatchHashMultiSetRow("small".to_string(), vec![BatchHashMultiSetValue(vec![7; 48])]),
        ],
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(response.applied, 2);
    assert_eq!(engine.block_store.stats().writes, 1);
    engine.cache.invalidate_shard(1).unwrap();
    drop(engine);
    let recovered = engine_on(dir.path());
    for (key, expected) in [("large", vec![9; 16 * 1024]), ("small", vec![7; 48])] {
        let response = recovered.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet { key: key.to_string(), field: "payload".to_string() },
        });
        assert!(response.status.ok);
        let CommandResponse::Bytes { value } = response.response else { panic!("hash bytes") };
        assert_eq!(value, Some(expected));
    }
    recovered.compact_shard_blocks(1).unwrap();
    assert_eq!(recovered.registration_count_for_test(1), 0);
}

#[test]
fn durable_shared_rows_keep_latest_fallback_through_size_transitions() {
    use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let object_id = stable_block_object_id(1, "hash", "sized");
    for (generation, width) in [(1, 48), (2, 16 * 1024), (3, 48), (4, 16 * 1024)] {
        let expected = vec![generation; width];
        let (response, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
            shard_id: 1, fields: vec!["payload".to_string()], ttl_ms: None,
            rows: vec![BatchHashMultiSetRow("sized".to_string(), vec![BatchHashMultiSetValue(expected.clone())])],
        });
        assert!(response.status.ok, "{:?}", response.status);
        let frame = super::super::block_in_wal::read_block(&engine.block_store, 1, object_id, Some("payload"))
            .expect("latest carried frame remains available");
        match super::super::container_blocks::select_container_element(&frame, "payload") {
            super::super::container_blocks::ContainerElementRead::Found(value) => assert_eq!(value, expected),
            _ => panic!("latest field must be in the carried frame"),
        }
    }
    engine.compact_shard_blocks(1).unwrap();
    assert_eq!(engine.registration_count_for_test(1), 0);
}

fn timestamp_bytes(seconds: i64) -> Vec<u8> {
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct Timestamp {
        #[prost(int64, tag = "1")]
        seconds: i64,
        #[prost(int32, tag = "2")]
        nanos: i32,
    }
    Timestamp { seconds, nanos: 0 }.encode_to_vec()
}

fn cached_timestamp_write(engine: &TemporalEngine, seconds: i64, entries: Vec<(String, Vec<u8>)>) -> i64 {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiSetIfTimestampOlder {
            key: "grouped".to_string(), timestamp_field: "stamp".to_string(),
            timestamp_seconds: seconds, timestamp_nanos: 0, ttl_ms: None, entries,
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    let CommandResponse::Integer { value } = response.response else { panic!("conditional result") };
    value
}

#[test]
fn cached_timestamp_checks_do_not_read_packed_payloads_after_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let before = engine.block_store.stats().reads;
    for seconds in 1..=8 {
        assert_eq!(cached_timestamp_write(&engine, seconds, vec![
            ("stamp".to_string(), timestamp_bytes(seconds)),
            ("payload".to_string(), vec![seconds as u8; 256 * 1024]),
        ]), 1);
    }
    assert_eq!(engine.block_store.stats().reads, before);
    let writes = engine.block_store.stats().writes;
    assert_eq!(cached_timestamp_write(&engine, 7, vec![
        ("stamp".to_string(), timestamp_bytes(7)),
        ("payload".to_string(), b"stale".to_vec()),
    ]), 0);
    assert_eq!(engine.block_store.stats().reads, before);
    assert_eq!(engine.block_store.stats().writes, writes);
    assert_eq!(grouped_values(&engine)["payload"], vec![8; 256 * 1024]);
}

#[test]
fn cached_timestamp_checks_follow_actual_bytes_and_hash_invalidations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    assert_eq!(cached_timestamp_write(&engine, 1, vec![("stamp".to_string(), timestamp_bytes(1))]), 1);
    run(&engine, Command::HashSet {
        key: "grouped".to_string(), field: "stamp".to_string(), value: timestamp_bytes(50),
    });
    assert_eq!(cached_timestamp_write(&engine, 49, vec![("stamp".to_string(), timestamp_bytes(49))]), 0);
    // Comparison arguments are not a substitute for the actual last stored field value.
    assert_eq!(cached_timestamp_write(&engine, 51, vec![("stamp".to_string(), timestamp_bytes(2))]), 1);
    assert_eq!(cached_timestamp_write(&engine, 3, vec![
        ("stamp".to_string(), timestamp_bytes(3)),
        ("stamp".to_string(), timestamp_bytes(100)),
    ]), 1);
    assert_eq!(cached_timestamp_write(&engine, 99, vec![("stamp".to_string(), timestamp_bytes(99))]), 0);
    assert_eq!(grouped_values(&engine)["stamp"], timestamp_bytes(100));
    run(&engine, Command::HashDelete { key: "grouped".to_string(), field: "stamp".to_string() });
    assert_eq!(cached_timestamp_write(&engine, 1, vec![("stamp".to_string(), timestamp_bytes(1))]), 1);
    run(&engine, Command::CommonExpire { key: "grouped".to_string(), ttl_ms: 0 });
    assert_eq!(cached_timestamp_write(&engine, 1, vec![("stamp".to_string(), timestamp_bytes(1))]), 1);
    run(&engine, Command::CommonDelete { key: "grouped".to_string() });
    assert_eq!(cached_timestamp_write(&engine, 1, vec![("stamp".to_string(), timestamp_bytes(1))]), 1);
}

#[test]
fn cached_timestamp_checks_fall_back_after_eviction_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    assert_eq!(cached_timestamp_write(&engine, 10, vec![("stamp".to_string(), timestamp_bytes(10))]), 1);
    engine.cache.invalidate(&CacheKey::hash(1, "grouped", "stamp")).unwrap();
    let before = engine.block_store.stats().reads;
    assert_eq!(cached_timestamp_write(&engine, 9, vec![("stamp".to_string(), timestamp_bytes(9))]), 0);
    assert!(engine.block_store.stats().reads > before);
    engine.write_ahead_log_store().flush(1).unwrap();
    drop(engine);
    let recovered = engine_on(dir.path());
    assert_eq!(cached_timestamp_write(&recovered, 9, vec![("stamp".to_string(), timestamp_bytes(9))]), 0);
    assert_eq!(cached_timestamp_write(&recovered, 11, vec![("stamp".to_string(), timestamp_bytes(11))]), 1);
    assert_eq!(grouped_values(&recovered)["stamp"], timestamp_bytes(11));
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 100;

    fn hash_of(engine: &TemporalEngine, fields: usize) {
        for field in 0..fields {
            run(
                engine,
                Command::HashSet {
                    key: "h".to_string(),
                    field: format!("f{field:05}"),
                    value: vec![b'v'; 64],
                },
            );
        }
    }

    /// Allocations per operation of `op`, built before the span opens.
    fn per_op(fields: usize, op: impl Fn(usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        hash_of(&engine, fields);
        let commands: Vec<Command> = (0..OPS).map(op).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_hash_field_write_does_not_pay_for_the_fields_it_already_has() {
        const SMALL: usize = 10;
        const LARGE: usize = 400;

        // THE CONTROL, FIRST. Reading the whole hash is proportional to its fields by
        // construction. If this probe cannot see that, it cannot see the write's dependence
        // either and a flat answer below would mean nothing.
        let read_small = per_op(SMALL, |_| Command::HashGetAll {
            key: "h".to_string(),
        });
        let read_large = per_op(LARGE, |_| Command::HashGetAll {
            key: "h".to_string(),
        });
        println!("control  hash read-all: {read_small:.1} at {SMALL} fields, {read_large:.1} at {LARGE}");
        assert!(
            read_large > read_small * 10.0,
            "the control did not grow with the field count ({read_small:.1} -> {read_large:.1}), \
             so this probe is blind to that dimension"
        );

        let over_small = per_op(SMALL, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("f{:05}", j % SMALL),
            value: vec![b'w'; 64],
        });
        let over_large = per_op(LARGE, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("f{:05}", j % SMALL),
            value: vec![b'w'; 64],
        });
        let new_small = per_op(SMALL, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("n{j:05}"),
            value: vec![b'w'; 64],
        });
        let new_large = per_op(LARGE, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("n{j:05}"),
            value: vec![b'w'; 64],
        });
        println!("overwrite: {over_small:.1} at {SMALL} fields, {over_large:.1} at {LARGE}");
        println!("new field: {new_small:.1} at {SMALL} fields, {new_large:.1} at {LARGE}");

        // Before the fix: 118 -> 2,300 and 264 -> 2,400. Allowed slack is a handful, not a
        // fraction of the per-field cost (about 5.7 a field), so a surviving per-field term of
        // even one allocation fails.
        assert!(
            over_large - over_small < 8.0,
            "an overwrite cost {over_small:.1} allocations at {SMALL} fields and {over_large:.1} \
             at {LARGE}: it still pays for the fields the hash already had"
        );
        assert!(
            new_large - new_small < 8.0,
            "a new field cost {new_small:.1} allocations at {SMALL} fields and {new_large:.1} at \
             {LARGE}: it still pays for the fields the hash already had"
        );
    }
}
