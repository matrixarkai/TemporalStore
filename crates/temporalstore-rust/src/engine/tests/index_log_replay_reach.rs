// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH LOAD PATH ACTUALLY FOLDS THE INDEX LOG, MEASURED RATHER THAN ARGUED.
//!
//! `fold_index_log_deltas` is the only production caller of
//! `for_each_delta_record_above_anchor`, and it is easy to assume from its name that every
//! restore runs it. It does not. `load_shard_with` branches on `wal_single_barrier()`, which is
//! `!wal_legacy_recovery()` and therefore TRUE unless an operator sets `TS_WAL_LEGACY_RECOVERY`:
//! the default arm calls `load_index_base_only`, which passes `fold_deltas = false`, and the fold
//! is not on that path at all. The fold is reached from the OTHER arm -- the escape hatch -- and
//! from `install_latest_manifest_if_newer_on_load`, which that arm calls first.
//!
//! So this module measures reachability instead of asserting it, and it measures BOTH arms over
//! ONE store, so the two rows are about the same files and the same records. The instrument is
//! floored: the arm that is supposed to fold must be shown to have read something, because a
//! reachability run that reads zero everywhere reads exactly like one whose apparatus never
//! spoke.
//!
//! The counter used is `fold_pieces_declined`, and it is used because only ONE entry point can
//! move it: a piece is declined only when a non-zero base anchor is passed, and
//! `fold_index_log_deltas` is the only caller that passes one. A zero in that column is therefore
//! "the load-path fold did not run", not "the fold ran and found nothing" -- provided the fixture
//! holds declinable pieces, which is floored against the other arm.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;
/// Small enough that a few thousand writes seal many pieces. Thread-local, and put back on drop.
const PIECE_BYTES: u64 = 8 * 1024;

struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        crate::index_log::set_index_log_segment_bytes_for_test(None);
    }
}

fn write_keys(engine: &TemporalEngine, first: usize, count: usize) {
    let commands = (first..first + count)
        .map(|value| Command::StringSet {
            key: format!("tenant/1/object/{value:08}"),
            value: vec![b'v'; 16],
        })
        .collect::<Vec<_>>();
    for chunk in commands.chunks(500) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
}

/// THE DEFAULT LOAD PATH DOES NOT FOLD THE INDEX LOG. THE ESCAPE HATCH DOES.
///
/// Both arms are driven over one store, in one test, so neither row can be explained by a
/// different fixture. What the rows say:
///
/// - the default `load_shard` declines no piece, because it never reaches the anchor-aware fold;
/// - `load_index_checked`, which is what the `TS_WAL_LEGACY_RECOVERY` arm and
///   `install_latest_manifest_if_newer_on_load` both call, declines most of them.
///
/// This is the whole reachability claim for the piece-level decline, and it is a NARROW one: the
/// saving lands on the escape hatch an operator flips in the field, not on every restart. It is
/// worth having there -- the population of that arm is the shards already in trouble -- but it
/// must not be described as a restore-path win in general.
#[test]
fn the_default_load_path_does_not_fold_the_index_log_and_the_checked_one_does() {
    crate::index_log::set_index_log_segment_bytes_for_test(Some(PIECE_BYTES));
    let _rolling = RollingThreshold;

    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &pages, &indexes);
    engine.load_shard(SHARD);

    // Everything below the base first, so the pieces these writes seal name anchors the base
    // will cover -- which is what makes them declinable at all.
    write_keys(&engine, 0, 4_000);
    engine.flush_shard_index(SHARD);
    let base_bytes = std::fs::read(engine.index_path(SHARD)).expect("the base index was written");
    let base_state = decode_index_bytes(&base_bytes).expect("the base index decodes");
    let base_anchor = base_state.applied_wal_sequence.unwrap_or(0);
    // DENOMINATOR ONE: a base anchor of 0 turns the decline off entirely, and every row below
    // would then be a zero that means nothing.
    assert!(
        base_anchor > 0,
        "the base index carries no anchor, so no piece can be declinable"
    );
    // A SUFFIX ABOVE IT, AND ENOUGH OF IT TO SEAL PIECES OF ITS OWN. The engine appends one
    // delta per BATCH, not per write, so a couple of hundred writes are one record and land in
    // the piece being written -- which is never declined, and would make "some pieces are not
    // declinable" true for a reason that has nothing to do with the predicate. Several batches
    // seal several pieces above the base instead.
    write_keys(&engine, 4_000, 2_000);

    // DENOMINATOR TWO: the log must be in several sealed pieces, and they must not all name the
    // same side of the base anchor -- some below it, some above. A fixture whose pieces are all
    // one or all the other cannot tell a correct per-piece decision from a constant one.
    let log_root = engine.index_dir.join("indexlogs");
    let mut named_anchors = Vec::new();
    for entry in std::fs::read_dir(&log_root)
        .expect("the index-log directory exists")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(middle) = name
            .strip_prefix(&format!("shard-{SHARD}.indexlog."))
            .and_then(|rest| rest.split('.').next())
        else {
            continue;
        };
        let fields: Vec<&str> = middle.split('-').collect();
        if fields.len() != 3 {
            continue;
        }
        let Ok(anchor) = fields[2].parse::<u64>() else {
            continue;
        };
        named_anchors.push(anchor);
    }
    let sealed = named_anchors.len();
    let below = named_anchors
        .iter()
        .filter(|anchor| **anchor <= base_anchor)
        .count();
    assert!(
        sealed >= 5,
        "the fixture sealed {sealed} pieces at {PIECE_BYTES} bytes -- too few for a per-piece \
         decision to be visible"
    );
    assert!(
        below > 0 && below < sealed,
        "of {sealed} sealed pieces {below} name an anchor at or below the base's {base_anchor} \
         -- the fixture must straddle the base or every piece gets the same answer: \
         {named_anchors:?}"
    );

    // PUT THE STALE BASE BACK. The engine reissues the base-index write as it goes, so by now
    // the file on disk anchors at the newest record and reflects the whole log -- at which point
    // every piece is declinable and the two sides of the predicate cannot both be observed.
    //
    // This is not an artificial state. Under the single-barrier default the base-index write is
    // ISSUED but its fsync is DEFERRED (`wal_only_sync`), so a crash leaves exactly this: an
    // index log that has run ahead of the base checkpoint that survived. Restoring the bytes
    // taken at the last `flush_shard_index` -- the last write that WAS fsynced -- reproduces it.
    // BEFORE EACH ARM, not once: a load RE-MATERIALIZES the base index, so arm A would hand
    // arm B a base that anchors at the newest record and every piece would be declinable again.
    // Both arms must meet the same store, which means the same stale base.
    let index_path = engine.index_path(SHARD);
    let restore_stale_base = || {
        std::fs::write(&index_path, &base_bytes).expect("the stale base writes back");
        let reread =
            decode_index_bytes(&std::fs::read(&index_path).unwrap()).expect("it decodes");
        assert_eq!(
            reread.applied_wal_sequence,
            Some(base_anchor),
            "the restored base anchors at {:?}, not the {base_anchor} the denominators were \
             computed against",
            reread.applied_wal_sequence
        );
    };

    // ARM A: the default load path, over the same files.
    let reader_default = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-default"),
        &pages,
        &indexes,
    );
    restore_stale_base();
    crate::index_log::probe::reset();
    reader_default.load_shard(SHARD);
    let default_frames = crate::index_log::probe::fold_frames_read();
    let default_declined = crate::index_log::probe::fold_pieces_declined();

    // ARM B: the fold-aware load, which is what the escape-hatch arm calls. Same files.
    let reader_checked = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-checked"),
        &pages,
        &indexes,
    );
    restore_stale_base();
    crate::index_log::probe::reset();
    let folded = reader_checked
        .load_index_checked(SHARD, false)
        .expect("the delta log is intact, so the checked load must not refuse it");
    let checked_frames = crate::index_log::probe::fold_frames_read();
    let checked_declined = crate::index_log::probe::fold_pieces_declined();

    println!(
        "index-log fold reachability over one store ({sealed} sealed pieces, base anchor \
         {base_anchor}):\n\
         \x20 load_shard        (the DEFAULT arm, wal_single_barrier) : {default_frames:>6} \
         frames  {default_declined:>4} pieces declined\n\
         \x20 load_index_checked (the TS_WAL_LEGACY_RECOVERY arm and the manifest install) : \
         {checked_frames:>6} frames  {checked_declined:>4} pieces declined"
    );

    // THE FLOOR ON THE APPARATUS, BEFORE ANY CLAIM. Arm B must have read frames and declined
    // pieces, or arm A's zeroes are a probe that never spoke rather than a path that never folds.
    assert!(
        checked_frames > 0,
        "APPARATUS: the fold-aware load read no frame at all, so arm A's zero says nothing"
    );
    assert!(
        checked_declined > 0,
        "APPARATUS: the fold-aware load declined no piece, so the fixture holds nothing \
         declinable and arm A's zero says nothing"
    );
    assert!(folded.is_some(), "APPARATUS: the fold-aware load returned no state");

    // THE FINDING: the default arm reaches none of it.
    assert_eq!(
        default_declined, 0,
        "the default load path declined {default_declined} pieces -- it now reaches the \
         anchor-aware fold, and this module's whole premise has changed"
    );
    // And the decline is exactly the pieces the base covers -- not everything, and not a
    // count that happens to match. The sealed pieces above the base are still read, and so is
    // the piece being written.
    assert_eq!(
        checked_declined as usize, below,
        "the fold-aware load declined {checked_declined} pieces; {below} of {sealed} name an \
         anchor at or below the base's {base_anchor}: {named_anchors:?}"
    );
}
