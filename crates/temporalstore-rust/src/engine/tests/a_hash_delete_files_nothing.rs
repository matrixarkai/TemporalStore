// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A HASH DELETE FILES NOTHING, AND IT USED TO RE-FILE EVERY FIELD THE HASH HELD.
//!
//! After a write, the engine brings the bucket index up to date for the keys the write touched. A
//! hash delete had already marked its field's block deleted, and the maintenance that followed was
//! the whole-object sync: it walked every field the hash still held, asked whether each was filed
//! through a lookup that resolves no named element (so the answer was always no), and filed every
//! one again, each filing shifting the object's block vector. Per delete, with the allocation
//! probe:
//!
//! ```text
//!   fields in the hash        bytes allocated per HashDelete
//!                  200                              534 KB
//!                3,200                               47 MB      (88x for 16x the fields)
//! ```
//!
//! WHAT MOVES ON DISK: nothing. The WAL record, the block and the index-log record of a delete
//! are written by code this does not touch; the maintenance only edits the in-memory bucket index.
//!
//! TWO GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_hash_delete_leaves_the_index_as_the_whole_object_sync_left_it` runs one history twice, with
//!     the whole-object sync forced on and off, and requires the bucket index to agree at every
//!     step: the block lines of every bucket, the dirty flag of every bucket and block, and what
//!     the hashes read back. A control runs the same history twice on the SAME arm, so a history
//!     that is not deterministic cannot read as a difference. It passes with the narrowing undone,
//!     which is what equivalence means -- it is not the guard for the speed-up.
//!   * `a_hash_delete_files_no_block` counts the blocks filed: none for a delete from forty fields,
//!     and the arm with the whole-object sync forced on must file them, or the counter is blind.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;

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

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn hset(engine: &TemporalEngine, key: &str, index: usize, width: usize) {
    run(
        engine,
        Command::HashSet {
            key: key.to_string(),
            field: format!("f{index:03}"),
            value: vec![b'a' + (index % 26) as u8; width],
        },
    );
}

fn hdel(engine: &TemporalEngine, key: &str, index: usize) {
    run(
        engine,
        Command::HashDelete {
            key: key.to_string(),
            field: format!("f{index:03}"),
        },
    );
}

fn with_whole_object_sync<T>(on: bool, body: impl FnOnce() -> T) -> T {
    let cell = &crate::engine::storage_bucket_internals::WHOLE_OBJECT_HASH_DELETE;
    cell.with(|flag| flag.set(on));
    let out = body();
    cell.with(|flag| flag.set(false));
    out
}

/// Everything the maintenance could have changed about the bucket index, in a form two engines can
/// be compared by: the block lines of every bucket, the dirty flag of every bucket and of every
/// block, and what each hash reads back.
fn picture(engine: &TemporalEngine, keys: &[&str]) -> String {
    let mut out = String::new();
    {
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&1).expect("shard 1");
        let lines = crate::engine::storage_reporting::bucket_generation_fingerprints_by_bucket(1, shard);
        for (bucket, set) in &lines {
            out.push_str(&format!("bucket {bucket}: {} lines\n", set.len()));
            for line in set {
                out.push_str(line);
                out.push('\n');
            }
        }
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            let mut blocks = bucket
                .block_index
                .iter()
                .map(|(handle, block)| (format!("{handle:?}"), block.dirty(), block.deleted()))
                .collect::<Vec<_>>();
            blocks.sort();
            out.push_str(&format!(
                "node {routing_bucket}: dirty={} blocks={blocks:?}\n",
                bucket.dirty()
            ));
        }
        out.push_str(&format!("dirty objects {}\n", shard.dirty_objects.len()));
    }
    for key in keys {
        if let CommandResponse::HashEntries { entries: mut members } = run(
            engine,
            Command::HashGetAll {
                key: key.to_string(),
            },
        ) {
            members.sort();
            out.push_str(&format!("{key}: {members:?}\n"));
        }
    }
    out
}

/// One history, with a picture after every step.
fn history(dir: &std::path::Path) -> Vec<String> {
    let engine = engine_on(dir);
    let keys = ["h1", "h2", "s1"];
    let mut pictures = Vec::new();
    let mut snap = |engine: &TemporalEngine| pictures.push(picture(engine, &keys));
    for index in 0..40 {
        hset(&engine, "h1", index, 24);
    }
    for index in 0..6 {
        hset(&engine, "h2", index, 24);
    }
    run(
        &engine,
        Command::StringSet {
            key: "s1".to_string(),
            value: vec![b'v'; 16],
        },
    );
    snap(&engine);
    for index in [7usize, 13, 14, 39, 0] {
        hdel(&engine, "h1", index);
    }
    snap(&engine);
    // A dump that takes the whole dirty set, so the blocks are CLEAN when the next delete lands.
    let dumped = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        ..crate::engine::reports::StorageLifecycleRequest::default()
    });
    assert!(dumped.dump_manifest.is_some(), "the dump took nothing, so the blocks stay dirty");
    snap(&engine);
    // Deleted after a dump: the blocks are clean when the delete lands.
    for index in [8usize, 9] {
        hdel(&engine, "h1", index);
    }
    snap(&engine);
    // A re-add of a deleted field, then its delete again.
    hset(&engine, "h1", 7, 40);
    snap(&engine);
    hdel(&engine, "h1", 7);
    snap(&engine);
    // The LAST field of a hash: the hash is gone, and the caller still rebuilds.
    for index in 0..6 {
        hdel(&engine, "h2", index);
    }
    snap(&engine);
    // A delete of a field that is not there, and of a key that is not there.
    hdel(&engine, "h1", 7);
    hdel(&engine, "nope", 1);
    snap(&engine);
    pictures
}

#[test]
fn a_hash_delete_leaves_the_index_as_the_whole_object_sync_left_it() {
    let narrow_a = tempfile::tempdir().unwrap();
    let narrow_b = tempfile::tempdir().unwrap();
    let whole = tempfile::tempdir().unwrap();
    let first = with_whole_object_sync(false, || history(narrow_a.path()));
    let second = with_whole_object_sync(false, || history(narrow_b.path()));
    let old = with_whole_object_sync(true, || history(whole.path()));

    // THE CONTROL: the same arm twice must agree, or a difference below could be the history.
    assert_eq!(first.len(), second.len());
    for (step, (left, right)) in first.iter().zip(&second).enumerate() {
        assert_eq!(left, right, "step {step}: the history is not deterministic on one arm");
    }
    // DENOMINATOR: every step showed something, and the steps differ from one another.
    assert!(first.iter().all(|picture| picture.len() > 200), "a step pictured almost nothing");
    // The last step deletes a field and a key that are not there, so it is the one step allowed to
    // leave the picture alone.
    let moved = first.windows(2).filter(|pair| pair[0] != pair[1]).count();
    assert!(
        moved + 2 >= first.len(),
        "only {moved} of {} steps changed the picture, so the history does not exercise the index",
        first.len() - 1
    );
    for (step, (narrow, whole)) in first.iter().zip(&old).enumerate() {
        assert_eq!(narrow, whole, "step {step}: the narrow maintenance left another index");
    }
}

#[test]
fn a_hash_delete_files_no_block() {
    for whole in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        for index in 0..40 {
            hset(&engine, "h", index, 24);
        }
        let before_set = crate::engine::storage_bucket_internals::bucket_block_upserts();
        hset(&engine, "h", 40, 24);
        let one_set = crate::engine::storage_bucket_internals::bucket_block_upserts() - before_set;
        // THE CONTROL: the counter sees a write's filing.
        assert!(one_set >= 1, "a hash write filed no block, so the counter is blind");

        let before = crate::engine::storage_bucket_internals::bucket_block_upserts();
        with_whole_object_sync(whole, || hdel(&engine, "h", 7));
        let filed = crate::engine::storage_bucket_internals::bucket_block_upserts() - before;
        if whole {
            assert!(
                filed >= 30,
                "with the whole-object sync forced on, a delete from forty fields filed {filed}: \
                 the counter cannot see the old behaviour"
            );
        } else {
            assert_eq!(filed, 0, "a hash delete filed {filed} block(s)");
        }
    }
}

/// Allocation and profiler driver, not a gate: `HASH_E=<fields> HASH_K=<deletes> HASH_WHOLE=1`
/// forces the whole-object sync. Run under `valgrind --tool=callgrind
/// --toggle-collect='*hash_delete_span*'` for instructions.
#[inline(never)]
fn hash_delete_span(engine: &TemporalEngine, deletes: usize) {
    for index in 0..deletes {
        hdel(engine, "h", index);
    }
}

#[test]
#[ignore]
fn a_hash_delete_for_the_profiler() {
    let fields = std::env::var("HASH_E").ok().and_then(|v| v.parse().ok()).unwrap_or(1000usize);
    let deletes = std::env::var("HASH_K").ok().and_then(|v| v.parse().ok()).unwrap_or(20usize);
    let whole = std::env::var_os("HASH_WHOLE").is_some();
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for index in 0..fields {
        hset(&engine, "h", index, 24);
    }
    #[cfg(feature = "alloc-probe")]
    let probe = crate::alloc_probe::Probe::start();
    with_whole_object_sync(whole, || hash_delete_span(&engine, deletes));
    #[cfg(feature = "alloc-probe")]
    {
        let counts = probe.stop();
        eprintln!(
            "HASHDELETE whole={whole} fields={fields} deletes={deletes}: {} allocs {} bytes per delete",
            counts.allocs as f64 / deletes as f64,
            counts.alloc_bytes as f64 / deletes as f64
        );
    }
    eprintln!("HASHDELETE whole={whole} fields={fields}: {} blocks filed in total", crate::engine::storage_bucket_internals::bucket_block_upserts());
}
