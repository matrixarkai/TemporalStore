// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a durable dump manifest still holds after the DEFAULT recovery arm restores from it.
//!
//! A manifest carries a serialized `ShardState`. Three of the model maps that
//! `collect_model_live_block_entries` walks are `skip_serializing` -- `hashes`, `context_events`,
//! `context_indexes` -- so a freshly decoded manifest index has them EMPTY while its
//! `bucket_index.bucket_map`, which does serialize, still names every one of their pages.
//!
//! `rebuild_bucket_block_ownership` clears `bucket_map` and repopulates it from the model maps.
//! Run against a decoded manifest index without first re-deriving the unserialized maps, it
//! therefore deletes from the index exactly the pages only the index still knew about.
//!
//! `install_bucket_dump_manifest` calls `rebuild_unserialized_model_maps_from_bucket_index` first
//! and says in a comment why. The default single-barrier recovery arm in `load_shard_with` decodes
//! the same bytes and calls only the ownership rebuild.
#![allow(clippy::all)]
use super::*;

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
/// the ownership rebuild re-derives its pages from a map that still has them. If this ever drops
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
fn a_manifest_restore_on_the_default_recovery_arm_keeps_the_pages_only_the_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &pages, &indexes);
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

    // A fresh engine over the SAME pages + index dirs, the way a restart arrives. No base index
    // file was ever materialized (no unload, no compaction), so the base watermark is 0 and the
    // durable manifest above is newer -- which is the arm being measured.
    let restarted =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &pages, &indexes);
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
         {strings_after} of {STRING_KEYS} strings: the manifest's index named every hash page, \
         and the ownership rebuild dropped the ones no serialized model map could re-supply"
    );
}
