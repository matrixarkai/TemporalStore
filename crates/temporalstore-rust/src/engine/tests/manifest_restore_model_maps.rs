// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a durable dump manifest still holds after the DEFAULT recovery arm restores from it.
//!
//! A manifest carries a serialized `ShardState`. Three of the model maps that
//! `collect_model_live_block_entries` walks are `skip_serializing` -- `hashes`, `context_events`,
//! `context_indexes` -- so a freshly decoded manifest index has them EMPTY while its
//! `bucket_index.bucket_map`, which does serialize, still names every one of their blocks.
//!
//! `rebuild_bucket_block_ownership` clears `bucket_map` and repopulates it from the model maps.
//! Run against a decoded manifest index without first re-deriving the unserialized maps, it
//! therefore deletes from the index exactly the blocks only the index still knew about.
//!
//! `install_bucket_dump_manifest` calls `rebuild_unserialized_model_maps_from_bucket_index` first
//! and says in a comment why. The default single-barrier recovery arm in `load_shard_with` decodes
//! the same bytes and calls only the ownership rebuild.
#![allow(clippy::all)]
use super::*;

#[test]
fn hash_free_restore_reconciliation_does_not_build_block_identities() {
    use crate::engine::state::{
        measure_block_identity_builds_for_test, measure_block_identity_visits_for_test,
    };
    use crate::engine::storage_bucket_internals::{
        collect_bucket_index_live_block_entries,
        rebuild_unserialized_model_maps_from_bucket_index,
    };

    const KEYS: usize = 1024;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for start in (0..KEYS).step_by(64) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: (start..start + 64)
                .map(|n| Command::StringSet {
                    key: format!("restore-string-{n:05}"),
                    value: vec![n as u8; 32],
                })
                .collect(),
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    let mut shard = decode_index_bytes(&engine.export_index_bytes(1).unwrap()).unwrap();
    assert_eq!(shard.strings().len(), KEYS);
    assert!(shard.bucket_index.hashes.is_empty());
    let before = serialize_index(&shard);

    let ((entries, control_visits), control_builds) = measure_block_identity_builds_for_test(|| {
        measure_block_identity_visits_for_test(|| collect_bucket_index_live_block_entries(&shard))
    });
    assert!(!entries.is_empty(), "the control must name real blocks");
    assert!(control_visits >= KEYS);
    assert_eq!(control_builds, 1);
    drop(entries);

    let (((), visits), builds) = measure_block_identity_builds_for_test(|| {
        measure_block_identity_visits_for_test(|| {
            rebuild_unserialized_model_maps_from_bucket_index(&mut shard)
        })
    });
    assert_eq!(builds, 0, "an empty hash map needs no shard-wide address set");
    assert_eq!(visits, 0);
    assert_eq!(serialize_index(&shard), before, "the restored image must stay unchanged");
    println!("hash-free reconcile: control_visits={control_visits} visits={visits}");
}

#[test]
fn nonempty_hash_reconciliation_still_filters_stale_addresses() {
    use crate::engine::state::measure_block_identity_visits_for_test;
    use crate::engine::storage_bucket_internals::rebuild_unserialized_model_maps_from_bucket_index;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "restore-hash".into(),
            field: "live".into(),
            value: b"kept".to_vec(),
        },
    });
    assert!(response.status.ok);
    let mut shard = decode_index_bytes(&engine.export_index_bytes(1).unwrap()).unwrap();
    let live = shard.bucket_index.hashes.get("restore-hash").unwrap().get("live").unwrap().clone();
    shard.bucket_index.hashes.insert_element_for_test(
        "restore-hash",
        "ghost".into(),
        crate::block_store::ElementEntry::from_parts(123_456, 0, 32, None, None),
    );
    assert_eq!(shard.bucket_index.hashes.get("restore-hash").unwrap().len(), 2);
    let ((), visits) = measure_block_identity_visits_for_test(|| {
        rebuild_unserialized_model_maps_from_bucket_index(&mut shard)
    });
    assert!(visits > 0, "a nonempty hash map still needs live-address validation");
    let fields = shard.bucket_index.hashes.get("restore-hash").unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields.get("live"), Some(&live));
    assert!(fields.get("ghost").is_none());
}

const SHARD: ShardId = 1;
const HASH_FIELDS: usize = 6;
const STRING_KEYS: usize = 6;

/// How many fields of the one hash key a read can actually see.
fn readable_hash_fields(engine: &TemporalEngine) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashGetAll {
                key: "h".to_string(),
            },
        })
        .response
    {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    }
}

/// THE CONTROL. `strings` is NOT `skip_serializing`, so it survives the manifest round trip and
/// the ownership rebuild re-derives its blocks from a map that still has them. If this ever drops
/// with the hash count, the fault is the restore in general and not the unserialized maps.
fn readable_strings(engine: &TemporalEngine) -> usize {
    (0..STRING_KEYS)
        .filter(|index| {
            matches!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: SHARD,
                        command: Command::StringGet {
                            key: format!("s{index}"),
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(_) }
            )
        })
        .count()
}

fn write_corpus(engine: &TemporalEngine) {
    for index in 0..HASH_FIELDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashSet {
                key: "h".to_string(),
                field: format!("f{index}"),
                value: format!("hash-value-{index}").into_bytes(),
            },
        });
        assert!(response.status.ok, "HSET f{index}: {response:?}");
    }
    for index in 0..STRING_KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("s{index}"),
                value: format!("string-value-{index}").into_bytes(),
            },
        });
        assert!(response.status.ok, "SET s{index}: {response:?}");
    }
}

#[test]
fn a_manifest_restore_on_the_default_recovery_arm_keeps_the_blocks_only_the_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");

    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(SHARD);
    write_corpus(&engine);

    // DENOMINATOR, asserted as two halves rather than one total: a restore that loses every hash
    // field and every string alike would still satisfy a single "some data came back" test.
    assert_eq!(
        readable_hash_fields(&engine),
        HASH_FIELDS,
        "VACUITY: the live engine must serve all {HASH_FIELDS} hash fields before a restore can \
         be said to have lost any"
    );
    assert_eq!(
        readable_strings(&engine),
        STRING_KEYS,
        "VACUITY: the live engine must serve all {STRING_KEYS} strings"
    );

    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("dump manifest");
    assert!(
        manifest.wal_sequence > 0,
        "VACUITY: a manifest anchored at 0 would not beat the base watermark, so the recovery arm \
         under test would never be entered"
    );
    drop(engine);

    // A fresh engine over the SAME blocks + index dirs, the way a restart arrives. No base index
    // file was ever materialized (no unload, no compaction), so the base watermark is 0 and the
    // durable manifest above is newer -- which is the arm being measured.
    let restarted =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    restarted.load_shard(SHARD);

    let strings_after = readable_strings(&restarted);
    let hashes_after = readable_hash_fields(&restarted);
    assert_eq!(
        strings_after, STRING_KEYS,
        "CONTROL: the restore lost serialized model-map state too ({strings_after} of \
         {STRING_KEYS} strings), so this is not about the unserialized maps"
    );
    assert_eq!(
        hashes_after, HASH_FIELDS,
        "the restore served {hashes_after} of {HASH_FIELDS} hash fields while serving \
         {strings_after} of {STRING_KEYS} strings: the manifest's index named every hash block, \
         and the ownership rebuild dropped the ones no serialized model map could re-supply"
    );
}
