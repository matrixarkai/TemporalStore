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

// ===========================================================================================
// THE THIRD SITE: A RECORD WITH NO ANCHOR IS NOT A RECORD AT ANCHOR 0
//
// `applied_wal_sequence.unwrap_or(0) <= anchor` turns a MISSING anchor into anchor 0, which is
// at or below every anchor, so an anchor-less record compares as older than everything: already
// reflected, safe to discard. #1832 took that reading out of the post-dump record walk. #1842
// took it out of the piece NAME. It survived at a third site -- the load-path fold in
// `fold_index_log_deltas` -- and the three are not independent: #1842's fix exists to hold a
// piece of anchor-less deltas OPEN so the fold can see the records in it, and the fold then
// skipped exactly those records one line later.
//
// `an_expiry_round_under_replay_writes_a_delta_with_no_anchor` (engine/tests/expiry_scale.rs) is
// the counter that says production writes such a record: an expiry round that reaches its
// checkpoint before anything has anchored the shard emits no items, one key-state per removed
// key, and no anchor. What is built below is that exact shape, put in front of a base that
// anchors somewhere -- which is the state a crash leaves: under the single-barrier default the
// base-index write is ISSUED but its fsync DEFERRED, so the base that survives can be older than
// the delta log that survives beside it.
//
// EVERY SHAPE THE FOLD CAN MEET IS PLANTED, because only one of them is the subject:
//
//   * no anchor, carries content   -- no base reflects it. THE SUBJECT.
//   * anchored AT the base         -- in the base.
//   * anchored ABOVE the base      -- not in the base, and it moves the reconstructed anchor.
//
// A fixture holding only the first cannot tell this change from "fold everything".
//
// THE FOURTH SHAPE IS NOT PLANTED HERE, AND THAT IS A FACT ABOUT THE WALK RATHER THAN A GAP. A
// record carrying no anchor AND nothing the fold would apply -- the legacy whole-index line --
// never reaches the fold at all: `for_each_delta_record_above_anchor` drops it itself, with
// "only keep records that carry a delta payload OR a WAL anchor". So an engine-level fixture
// cannot hold one up in front of the fold; the append lands in the log and the walk swallows it.
// That filter is a FOURTH spelling of the same question, and it agrees with the predicate: the
// records it drops are exactly the ones the predicate calls reflected. The corner of the
// predicate it covers is asserted where the predicate is called directly, in
// `index_log::tests::what_a_base_anchor_reflects_at_every_corner` -- and it has to be asserted
// somewhere, because the post-dump sweep asks the same question of a RAW PAYLOAD, where the
// legacy line very much does appear.
// ===========================================================================================

/// Zero-padded so key order and assertion order agree.
fn deadline_key(index: usize) -> String {
    format!("tenant/1/deadline/{index:04}")
}

/// What the fixture planted, so every control below is built from how it was WRITTEN rather
/// than from the predicate under test.
struct AnchorLessFixture {
    engine: TemporalEngine,
    base_anchor: u64,
    /// Anchor-less records carrying one key-state per key. No base reflects these.
    with_content: Vec<u64>,
    /// Anchored records the base does NOT cover, planted with the HIGHER anchor FIRST so that
    /// "take the last anchor" and "take the highest anchor" give different answers.
    above: Vec<(u64, u64)>,
    /// An anchored record exactly AT the base anchor. The base covers it.
    at_base: u64,
}

impl AnchorLessFixture {
    /// The sequences the fold must apply, in log order.
    fn expected_applied(&self, records: &[crate::index_log::IndexDeltaRecord]) -> Vec<(ShardId, u64)> {
        records
            .iter()
            .filter(|record| !self.expected_reflected(record.sequence))
            .map(|record| (record.shard_id, record.sequence))
            .collect()
    }

    fn expected_reflected(&self, sequence: u64) -> bool {
        if self.with_content.contains(&sequence) {
            return false;
        }
        if self.above.iter().any(|(seq, _)| *seq == sequence) {
            return false;
        }
        true
    }

    /// The anchor a correct fold leaves behind: the HIGHEST anchor among the records it applied.
    fn expected_anchor(&self) -> u64 {
        self.above
            .iter()
            .map(|(_, anchor)| *anchor)
            .max()
            .expect("the fixture plants at least one record above the base")
    }
}

/// A base anchored above zero holding a deadline for every key, and one record of every shape
/// the predicate separates.
///
/// The deadline is ten minutes out ON PURPOSE: nothing here may actually expire. The question is
/// what the FOLD does with a record describing an expiry, not whether an expiry round runs, and a
/// deadline that could fire during the test would let a passing run be explained by the sweep.
fn base_with_deadlines_and_anchor_less_deltas(
    dir: &std::path::Path,
    keys: usize,
    with_content: usize,
) -> AnchorLessFixture {
    let pages = dir.join("pages");
    let indexes = dir.join("indexes");
    let engine = TemporalEngine::with_local_dirs(1 << 20, dir.join("cache"), &pages, &indexes);
    engine.load_shard(SHARD);

    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSetEx {
                key: deadline_key(index),
                value: vec![b'v'; 16],
                ttl_ms: 600_000,
            },
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
    engine.flush_shard_index(SHARD);

    let base_state = decode_index_bytes(&std::fs::read(engine.index_path(SHARD)).unwrap())
        .expect("the base index decodes");
    let base_anchor = base_state
        .applied_wal_sequence
        .expect("the base index must anchor somewhere");

    // DENOMINATOR ONE: a base anchor of 0 turns the whole comparison off, and every row below
    // would be a pass that means nothing.
    assert!(base_anchor > 0, "the base index anchors at 0");
    // DENOMINATOR TWO: the base must HOLD every deadline. A base that holds none is one where a
    // fold that applies nothing and a fold that applies everything look the same.
    let held = (0..keys)
        .filter(|index| base_state.expires_at_ms.contains_key(&deadline_key(*index)))
        .count();
    assert_eq!(
        held, keys,
        "the base index holds {held} of {keys} deadlines, so removing them is not observable"
    );

    // THE ANCHOR-LESS RECORDS WITH CONTENT, appended after the base was taken so no base can
    // reflect them. A key-state blob carrying only `key` is what an expiry round captures AFTER
    // the delete: the ABSENT `expires_at_ms` is the removal, and a present one would RESTORE the
    // deadline of a key the round expired. So applying these must leave the deadline map empty,
    // and skipping them must leave it exactly as the base had it.
    let per_delta = keys.div_ceil(with_content.max(1)).max(1);
    let mut planted = Vec::new();
    for chunk in (0..keys).collect::<Vec<_>>().chunks(per_delta) {
        let key_states: Vec<serde_json::Value> = chunk
            .iter()
            .map(|index| serde_json::json!({ "key": deadline_key(*index) }))
            .collect();
        planted.push(
            engine
                .index_log_store
                .append_delta(SHARD, Vec::new(), key_states, None, None, false, true)
                .expect("the anchor-less delta appends"),
        );
    }
    // An anchored record the base covers EXACTLY. Its key-state would put a deadline BACK, so a
    // fold that stops skipping what the base already holds is visible rather than merely slower.
    let at_base = engine
        .index_log_store
        .append_delta(
            SHARD,
            Vec::new(),
            vec![serde_json::json!({ "key": deadline_key(0), "expires_at_ms": u64::MAX })],
            Some(base_anchor),
            None,
            false,
            true,
        )
        .expect("the at-the-base delta appends");
    // Two anchored records ABOVE the base, HIGHER ANCHOR FIRST. A fold that takes the LAST
    // anchor rather than the HIGHEST lands on the second one, and the two differ.
    let mut above = Vec::new();
    for (offset, index) in [(5_u64, 1_usize), (2_u64, 2_usize)] {
        let sequence = engine
            .index_log_store
            .append_delta(
                SHARD,
                Vec::new(),
                vec![serde_json::json!({ "key": format!("tenant/1/above/{index:04}") })],
                Some(base_anchor + offset),
                None,
                false,
                true,
            )
            .expect("the above-the-base delta appends");
        above.push((sequence, base_anchor + offset));
    }

    AnchorLessFixture {
        engine,
        base_anchor,
        with_content: planted,
        above,
        at_base,
    }
}

/// THE DEFECT, AS THE DEPLOYMENT WOULD SEE IT: a deadline the delta removed comes BACK.
///
/// THE ASSERTION IS PER KEY, NOT A COUNT. A count is equally happy when the right NUMBER of the
/// wrong keys survives, and the failure here is exactly a wrong per-key outcome -- the key an
/// expiry round removed is restored, with its deadline, and the load reports success. So the
/// deadline map is compared key by key, in key order, against a control that names every key.
///
/// This test uses nothing that did not exist before the fix, so it runs against the unmodified
/// tree: there it fails with all six deadlines still standing.
#[test]
fn the_fold_applies_an_anchor_less_delta_no_base_anchor_can_reflect() {
    const KEYS: usize = 6;
    let dir = tempfile::tempdir().unwrap();
    let fixture = base_with_deadlines_and_anchor_less_deltas(dir.path(), KEYS, 1);
    assert_eq!(fixture.with_content.len(), 1, "fixture: one content record");

    // DENOMINATOR THREE: the record really is anchor-less and really carries content. Either
    // half alone describes a record the sweep is SUPPOSED to drop.
    let records = fixture
        .engine
        .index_log_store
        .read_delta_records(SHARD, 0)
        .expect("the index log reads back");
    let planted = records
        .iter()
        .find(|record| record.sequence == fixture.with_content[0])
        .expect("the anchor-less record is in the log");
    assert!(
        planted.applied_wal_sequence.is_none(),
        "fixture: the planted record anchors at {:?}",
        planted.applied_wal_sequence
    );
    assert_eq!(
        planted.key_states.len(),
        KEYS,
        "fixture: the planted record carries {} key-states",
        planted.key_states.len()
    );

    let reader = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-reader"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    let loaded = reader
        .load_index_checked(SHARD, false)
        .expect("the delta log is intact, so the checked load must not refuse it")
        .expect("the base index is present, so the load must return a state");

    let after: Vec<(String, bool)> = (0..KEYS)
        .map(|index| {
            let key = deadline_key(index);
            let still_held = loaded.expires_at_ms.contains_key(&key);
            (key, still_held)
        })
        .collect();
    let control: Vec<(String, bool)> =
        (0..KEYS).map(|index| (deadline_key(index), false)).collect();
    assert_eq!(
        after, control,
        "the fold skipped the anchor-less delta at sequence {} behind a base anchored at {}: \
         every `true` above is a key whose deadline the delta removed and the load put back",
        fixture.with_content[0], fixture.base_anchor
    );
}

/// WHAT THE FOLD DECIDED, WHAT IT DID, AND WHERE IT LEFT THE ANCHOR -- EACH AGAINST ITS OWN
/// CONTROL, ELEMENT BY ELEMENT.
///
/// The test above asserts an OUTCOME, which is what a deployment sees. This asserts the three
/// things that produce it, separately, because a change can break any one of them while leaving
/// the other two intact:
///
///   * the DECISION sequence -- for each record, in the order the fold met it, the (shard,
///     sequence) pair and whether the base reflects it;
///   * the APPLICATION sequence -- which records the fold actually folded, recorded where the
///     work lands rather than where the decision is taken, so a fold that asks the predicate and
///     then ignores the answer is visible;
///   * the reconstructed ANCHOR -- the HIGHEST anchor among the applied records, which is where
///     WAL replay resumes above. The fixture plants the higher anchor FIRST so that "the last
///     one" and "the highest one" are different numbers.
///
/// A test asserting only "three records applied" passes while the right number of the wrong
/// records is applied, which is why all three are compared pairwise against controls built from
/// how the fixture was WRITTEN.
///
/// THE INSTRUMENT IS FLOORED BY PLANTING A KNOWN NUMBER. Three anchor-less content-carrying
/// records go in and the counter inside the predicate must come back reading exactly three --
/// not two, not four, and not the whole log. A counter reading zero because nothing increments
/// it is indistinguishable from a path that never runs.
#[test]
fn every_record_the_fold_met_was_decided_by_its_own_anchor() {
    const KEYS: usize = 6;
    const PLANTED: usize = 3;
    let dir = tempfile::tempdir().unwrap();
    let fixture = base_with_deadlines_and_anchor_less_deltas(dir.path(), KEYS, PLANTED);
    assert_eq!(fixture.with_content.len(), PLANTED, "fixture: {PLANTED} planted");

    let records = fixture
        .engine
        .index_log_store
        .read_delta_records(SHARD, 0)
        .expect("the index log reads back");
    // DENOMINATOR: every shape the fixture claims to plant has to actually be in the log, or the
    // controls below are about records that are not there. Checked one shape at a time.
    let mut seen_with_content = 0;
    let mut seen_above = 0;
    let mut seen_at_base = 0;
    let mut seen_engine_written = 0;
    for record in &records {
        if fixture.with_content.contains(&record.sequence) {
            assert!(
                record.applied_wal_sequence.is_none() && !record.key_states.is_empty(),
                "fixture: planted record {} is {:?} / {} key-states",
                record.sequence,
                record.applied_wal_sequence,
                record.key_states.len()
            );
            seen_with_content += 1;
        } else if let Some((_, anchor)) = fixture
            .above
            .iter()
            .find(|(sequence, _)| *sequence == record.sequence)
        {
            assert_eq!(
                record.applied_wal_sequence,
                Some(*anchor),
                "fixture: the above-the-base record {} anchors elsewhere",
                record.sequence
            );
            assert!(*anchor > fixture.base_anchor, "fixture: {anchor} is not above the base");
            seen_above += 1;
        } else if record.sequence == fixture.at_base {
            assert_eq!(
                record.applied_wal_sequence,
                Some(fixture.base_anchor),
                "fixture: the at-the-base record {} anchors elsewhere",
                record.sequence
            );
            seen_at_base += 1;
        } else {
            let anchor = record.applied_wal_sequence.unwrap_or(0);
            assert!(
                anchor > 0 && anchor <= fixture.base_anchor,
                "fixture: record {} anchors at {anchor}, which the base's {} does not cover",
                record.sequence,
                fixture.base_anchor
            );
            seen_engine_written += 1;
        }
    }
    assert_eq!(seen_with_content, PLANTED, "the log is missing a planted content record");
    assert_eq!(seen_above, fixture.above.len(), "the log is missing an above-the-base record");
    assert_eq!(seen_at_base, 1, "the log is missing the at-the-base record");
    assert!(
        seen_engine_written > 0,
        "the log holds no record the engine wrote, so every row is a planted one"
    );

    let control: Vec<(ShardId, u64, bool)> = records
        .iter()
        .map(|record| {
            (
                record.shard_id,
                record.sequence,
                fixture.expected_reflected(record.sequence),
            )
        })
        .collect();
    let control_applied = fixture.expected_applied(&records);

    let reader = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-reader"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    crate::index_log::probe::reset();
    crate::index_log::probe::arm_decisions();
    let loaded = reader
        .load_index_checked(SHARD, false)
        .expect("the checked load must not refuse an intact log");
    let decisions = crate::index_log::probe::decisions();
    let applied = crate::index_log::probe::applied();
    let tested = crate::index_log::probe::fold_records_tested();
    let anchorless_seen = crate::index_log::probe::fold_anchorless_with_content();
    crate::index_log::probe::disarm_decisions();

    let loaded = loaded.expect("APPARATUS: the load returned no state");
    // THE PLANTED-MARKER CONTROL, before any claim that rests on the counter.
    assert_eq!(
        anchorless_seen as usize, PLANTED,
        "{PLANTED} anchor-less content-carrying records were planted (and one anchor-less record \
         carrying nothing, which must not be counted); the counter inside the predicate read \
         {anchorless_seen}"
    );
    assert_eq!(
        tested as usize,
        records.len(),
        "the fold was asked about {tested} records; the log holds {}",
        records.len()
    );
    assert_eq!(
        decisions, control,
        "the fold's per-record DECISIONS do not match the control. Each triple is (shard, \
         sequence, reflected-by-the-base); the planted content-carrying records are {:?}, the \
         at-the-base one is {}, those above are {:?}, and the base anchors at {}",
        fixture.with_content, fixture.at_base, fixture.above, fixture.base_anchor
    );
    assert_eq!(
        applied, control_applied,
        "the fold APPLIED a different set of records than it decided to apply"
    );
    assert_eq!(
        loaded.applied_wal_sequence,
        Some(fixture.expected_anchor()),
        "the reconstructed anchor must be the HIGHEST anchor the fold applied ({}), not the \
         last one it saw ({:?}) and not the base's {}",
        fixture.expected_anchor(),
        fixture.above.last().map(|(_, anchor)| *anchor),
        fixture.base_anchor
    );
    // AND THE OUTCOME, so a change that keeps every sequence above and still loses the removals
    // is not scored a pass.
    let still_held = (0..KEYS)
        .filter(|index| loaded.expires_at_ms.contains_key(&deadline_key(*index)))
        .count();
    assert_eq!(
        still_held, 0,
        "{still_held} of {KEYS} deadlines survived a fold that decided to apply the records \
         removing them"
    );
}
