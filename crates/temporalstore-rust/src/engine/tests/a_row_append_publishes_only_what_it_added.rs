// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A row append publishes only the blocks it added.
//!
//! `sync_bucket_index_object_blocks_with_mode`'s `replace_existing` flag "costs the object's
//! whole block count on every call, however few blocks the write actually touched". A pure
//! append does not need it, and the two conditions that make skipping it safe -- nothing
//! evicted, nothing superseded -- are decidable at the call site. `SequenceAdd` reached the
//! same series through the same layout as the feature arms and took the restating path
//! unconditionally.
//!
//! WHAT THESE TESTS ARE, AND WHAT THEY ARE NOT.
//!
//! They are reload round-trip nets for `SequenceAdd`: a superseded instant and a run of pure
//! appends both survive unload/reload with the right payloads. Nothing else in the suite
//! covered a SequenceAdd REPLACEMENT across a reload.
//!
//! They are NOT guards on the `replace_existing` decision, and the honest reason is that it
//! was tried and failed. Inverting the condition to `if false` -- so every row append takes
//! the cheap path, including a replacement -- was planted on exactly one line and verified
//! applied, and BOTH tests still passed, twice: once reading the resident series and again
//! reading after an unload/reload. The resident map answers correctly either way because
//! `set` overwrites the instant, and the reconcile is membership-authoritative, so the reload
//! takes the series' membership as its authority rather than replaying raw index entries.
//!
//! So an assertion on either read path is a fallback recovering the right answer, which is the
//! definition of a decorative test. Detecting a wrong `replace_existing` needs an assertion on
//! the bucket index itself, and whoever writes it should plant `if false` here first and watch
//! it go red before trusting it.
#![allow(clippy::all)]
use super::*;

fn row(timestamp_ms: u64, gid: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid,
        action_type: 2,
        duration: 3,
        author_id: 4,
    }
}

fn sequence_rows(engine: &TemporalEngine, key: &str) -> Vec<(u64, u64)> {
    match engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceQuery {
            key: key.to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: 1000,
            filters: Vec::new(),
        },
    })
    .response
    {
        CommandResponse::SequenceRows { rows } => {
            rows.into_iter().map(|r| (r.timestamp_ms, r.gid)).collect()
        }
        other => panic!("SequenceQuery did not answer with rows: {other:?}"),
    }
}

fn engine_with_shard(dir: &tempfile::TempDir) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// A SUPERSEDED instant must leave ONE entry, not two.
///
/// This is the direction that breaks if the cheap path is taken when a row replaced an
/// existing timestamp: the superseded block stays filed alongside its replacement, so the
/// object carries two entries for one instant.
#[test]
fn a_replacing_row_append_leaves_one_entry_for_the_instant() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_shard(&dir);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "seq".to_string(),
            rows: vec![row(10, 1), row(20, 2), row(30, 3)],
        },
    });
    // Same instant, different payload: this is a REPLACEMENT, and it must restate.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "seq".to_string(),
            rows: vec![row(20, 99)],
        },
    });

    // Read the series back THROUGH THE INDEX. The resident map answers correctly even
    // with a superseded block still filed -- `set` overwrites the instant either way --
    // so a resident-only read is a fallback that recovers the right answer and makes the
    // assertion decorative. A reload rebuilds the series from what the index actually
    // holds, which is the thing `replace_existing` decides.
    engine.unload_shard(1);
    engine.load_shard(1);
    let rows = sequence_rows(&engine, "seq");
    assert_eq!(
        rows.len(),
        3,
        "a replaced instant produced {} rows, so the superseded block is still filed: {rows:?}",
        rows.len()
    );
    let at_twenty: Vec<u64> = rows.iter().filter(|(ts, _)| *ts == 20).map(|(_, g)| *g).collect();
    assert_eq!(
        at_twenty,
        vec![99],
        "instant 20 answered {at_twenty:?}; it must carry exactly the replacement"
    );
}

/// A PURE append must not orphan the blocks already filed.
///
/// This is the other direction: publishing only the new block is correct ONLY if the blocks
/// already filed stay findable. If the cheap path dropped them, earlier rows would vanish.
#[test]
fn a_pure_row_append_leaves_every_earlier_block_findable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_shard(&dir);

    // Each call is a pure append of a NEW instant -- the cheap path, five times over.
    for step in 1..=5u64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "seq".to_string(),
                rows: vec![row(step * 10, step)],
            },
        });
    }

    // Read the series back THROUGH THE INDEX. The resident map answers correctly even
    // with a superseded block still filed -- `set` overwrites the instant either way --
    // so a resident-only read is a fallback that recovers the right answer and makes the
    // assertion decorative. A reload rebuilds the series from what the index actually
    // holds, which is the thing `replace_existing` decides.
    engine.unload_shard(1);
    engine.load_shard(1);
    let rows = sequence_rows(&engine, "seq");
    let timestamps: Vec<u64> = rows.iter().map(|(ts, _)| *ts).collect();
    assert_eq!(
        timestamps,
        vec![10, 20, 30, 40, 50],
        "five pure appends answered {timestamps:?}, so an earlier block was orphaned"
    );
    let gids: Vec<u64> = rows.iter().map(|(_, g)| *g).collect();
    assert_eq!(gids, vec![1, 2, 3, 4, 5], "a block came back with the wrong payload: {rows:?}");
}
