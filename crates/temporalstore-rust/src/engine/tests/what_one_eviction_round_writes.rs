// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one eviction round writes, against what it dropped.
//!
//! WHY THIS EXISTS. The delete/drop eviction path takes a FULL CLONE of the shard state, encodes
//! the whole index from it, and then writes those bytes TWICE -- once through `persist_index_bytes`
//! and once as an index-log append. It does that however few objects the round dropped. So the cost
//! of a round is set by the size of the STORE, not by the size of the round, and this measures the
//! slope.
//!
//! WHY IT IS ARGUABLY REDUNDANT, in the code's own words. The compaction flush beside it records
//! why IT may not be deferred, and in doing so says what is true of this one:
//!
//!     "An expiry sweep and a delete_drop eviction both write a WAL tombstone per key before
//!      they flush, so a flush that lands late, lands stale, or never lands is re-derived by
//!      replay."
//!
//! A write whose loss is re-derived by replay is not carrying durability. It is bounding REPLAY
//! LENGTH, which is a real job -- but one that a mutation count can do per interval instead of per
//! round.
//!
//! THAT CHANGE HAS LANDED, AND THE FIRST ARM BELOW IS UNMOVED BY IT, ON PURPOSE. A shard's
//! `index_flush_countdown` is `#[serde(skip)]` and therefore 0 -- DUE -- on every fresh load, so
//! the first logged-deletion round after a reload still checkpoints and still costs exactly what
//! the sweep below prices. That is the safe direction and it is why the single-round figure is
//! still the right reading of what ONE checkpoint costs. What moved is the SHAPE across rounds,
//! which `the_checkpoint_interval_turns_a_flush_per_round_into_a_flush_per_interval` measures
//! directly against an interval-of-1 control in the same process on the same fixture: the arm that
//! still pays per round is kept reachable, because a claim that a cost is gone is only falsifiable
//! against an engine that still pays it.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A round that drops nothing writes nothing and would
//! report a flattering zero, so every arm asserts `dropped_object_count > 0` and asserts the seed
//! landed before any ratio is taken. A probe sized so the contested case cannot arise agrees with
//! whatever it is asked.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the flush runs on
//! the calling thread after the guard drops, so a single-threaded arm sees all of it. Running this
//! arm concurrently with another that encodes would not corrupt it -- each thread has its own --
//! but it would stop being one round's figure, which is why the seed and the round are in the same
//! function.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 7;

/// Seed `keys` string records and read each one back, so they are cached and therefore evictable.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let key = format!("drop-{item}");
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet { key },
                })
                .status
                .ok,
            "the read-back must ack at {item}, or the record is not cached and cannot be evicted"
        );
    }
}

/// The round's cost, swept over the store size, with the dump stage on or off.
///
/// THE DUMP ARM IS THE ATTRIBUTION. One round encodes the whole index TWICE, and only one of the
/// two encodes is the eviction flush at `storage_lifecycle_methods.rs:2265` -- there are eleven
/// whole-index encode sites in production and the round reaches more than one of them. Turning
/// `dump_before_evict` off is the control that says whether the second belongs to the dump, which
/// is cheaper and more reliable than reading the call graph.
fn sweep(dump_before_evict: bool) -> Vec<(usize, u64, u64, u64, f64)> {
    let mut rows: Vec<(usize, u64, u64, u64, f64)> = Vec::new();

    for keys in [500usize, 2_000, 8_000] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = TemporalEngine::with_local_dirs(
            64,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(SHARD);
        seed(&engine, keys);

        // THE DENOMINATOR, ASSERTED BEFORE THE RATIO. An empty store evicts nothing and divides
        // by nothing.
        let seeded = engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&SHARD)
            .expect("shard is loaded")
            .strings()
            .len();
        assert_eq!(keys, seeded, "the seed must have landed, or nothing below means anything");

        crate::engine::shard_write_guard::reset_index_encode_counts();
        let report = engine.apply_storage_eviction(SHARD, 1, 2, dump_before_evict, true);
        let counts = crate::engine::shard_write_guard::index_encode_counts();

        // THE REGIME. A round that dropped nothing is not a round.
        assert_eq!("delete_drop", report.mode, "{report:?}");
        assert!(report.pressure_gate_open, "the gate must open: {report:?}");
        assert!(
            report.dropped_object_count > 0,
            "the round must drop something, or its cost is not the cost of a round: {report:?}"
        );

        let dropped = report.dropped_object_count as u64;
        let per_dropped = counts.encode_bytes_total as f64 / dropped as f64;
        println!(
            "dump={dump_before_evict:<5} keys={keys:<6} dropped={dropped:<4} encodes={:<3} \
             encode_bytes={:<9} bytes/dropped={per_dropped:.1}",
            counts.encodes_total, counts.encode_bytes_total
        );
        rows.push((
            keys,
            dropped,
            counts.encodes_total,
            counts.encode_bytes_total,
            per_dropped,
        ));
    }

    rows
}

#[test]
fn what_one_eviction_round_writes_grows_with_the_store_not_the_round() {
    let rows = sweep(true);
    let without_dump = sweep(false);

    // THE ATTRIBUTION, printed beside the figure it explains rather than left to inference.
    for ((keys, _, with_encodes, with_bytes, _), (_, _, no_encodes, no_bytes, _)) in
        rows.iter().zip(without_dump.iter())
    {
        println!(
            "keys={keys:<6} encodes with dump={with_encodes} without={no_encodes}   \
             bytes with dump={with_bytes} without={no_bytes}"
        );
    }

    // THE SHAPE, stated as an assertion rather than left to the reader of the printout: the bytes
    // one round writes are set by the store, so they grow when the store does while the round
    // itself does not.
    let (first_keys, _, _, first_bytes, _) = rows[0];
    let (last_keys, _, _, last_bytes, _) = rows[rows.len() - 1];
    assert!(
        last_keys > first_keys,
        "the sweep must actually widen: {first_keys} -> {last_keys}"
    );
    assert!(
        last_bytes > first_bytes,
        "one round's encoded bytes did not grow with the store ({first_bytes} -> {last_bytes}), \
         so the premise of this measurement is wrong and the reading needs revisiting rather \
         than reporting"
    );
    for (keys, dropped, encodes, bytes, per) in &rows {
        assert!(
            *encodes > 0 && *bytes > 0,
            "keys={keys}: a round that dropped {dropped} objects encoded {encodes} indexes and \
             {bytes} bytes, which cannot be right -- the counter is thread-local and the flush \
             runs on this thread, so zero means the flush did not happen"
        );
        assert!(
            per.is_finite(),
            "keys={keys}: the per-object figure is not finite"
        );
    }
}

// --------------------------------------------------------------------------------------------
// THE SHAPE ACROSS ROUNDS, which is what the checkpoint interval changed.
// --------------------------------------------------------------------------------------------

/// One arm's reading: how many of `rounds` logged-deletion rounds materialized the base index.
struct RoundShape {
    /// Rounds that actually dropped something. A round that dropped nothing is not a round.
    rounds_with_drops: u64,
    /// Objects dropped across all rounds -- the denominator the interval is counted in.
    dropped_total: u64,
    /// Rounds that encoded the whole base index.
    materializations: u64,
    /// Rounds that dropped something and encoded NOTHING. The contested case: if this is zero the
    /// arm never entered the regime it is arguing about and proves nothing.
    skipped_flushes: u64,
    /// Bytes encoded across all rounds.
    encode_bytes: u64,
    /// What each round that dropped anything dropped, in order. The identity below replays the
    /// countdown over this rather than assuming every round dropped the same number -- a round
    /// that happens to drop one object instead of two moves where the next checkpoint falls, and
    /// a closed-form "about N/K" would read that as a defect.
    drops: Vec<u64>,
}

/// Replay the countdown over a recorded run of per-round drop counts and say how many rounds it
/// makes due.
///
/// THE IDENTITY, NOT A THRESHOLD. The countdown starts at 0 -- DUE, the safe post-reload
/// direction -- so the first round that drops is always a checkpoint, and it is reloaded with the
/// WHOLE interval rather than with the interval minus the overshoot. Spelling both of those out
/// here is what makes the assertion fail when either one is got wrong in the engine, which a
/// bound of the form "fewer than before" would not.
fn materializations_due(drops: &[u64], interval: u64) -> u64 {
    let mut countdown = 0u64;
    let mut due = 0u64;
    for dropped in drops {
        countdown = countdown.saturating_sub(*dropped);
        if countdown == 0 {
            countdown = interval;
            due += 1;
        }
    }
    due
}

/// Drive `rounds` delete_drop rounds at `interval` and count what each one wrote.
///
/// `dump_before_evict` IS OFF, deliberately. The sweep above establishes that the dump stage is
/// the OTHER whole-index encode in a round, so leaving it on would make every round encode once
/// whatever the checkpoint did and the count would answer the wrong question. With it off the only
/// encoder a round can reach is the checkpoint, so `encodes_total == 0` for a round that dropped
/// objects IS a skipped checkpoint, read off the same thread-local instrument as the sweep.
fn round_shape(keys: usize, rounds: u64, interval: u64) -> RoundShape {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    seed(&engine, keys);

    // READ THE SETTING BACK OUT. A knob the round consults under a different name than the arm
    // set leaves the arm silently not running, and the arm would agree with whatever it was asked.
    engine.set_logged_deletion_index_flush_interval_for_test(interval);
    assert_eq!(
        interval,
        engine.logged_deletion_index_flush_interval(),
        "the interval this arm set is not the interval the round will read, so the arm is not \
         running at the interval it reports"
    );

    let mut shape = RoundShape {
        rounds_with_drops: 0,
        dropped_total: 0,
        materializations: 0,
        skipped_flushes: 0,
        encode_bytes: 0,
        drops: Vec::new(),
    };
    for _ in 0..rounds {
        crate::engine::shard_write_guard::reset_index_encode_counts();
        let report = engine.apply_storage_eviction(SHARD, 1, 2, false, true);
        let counts = crate::engine::shard_write_guard::index_encode_counts();
        if report.dropped_object_count == 0 {
            continue;
        }
        shape.rounds_with_drops += 1;
        shape.dropped_total += report.dropped_object_count as u64;
        shape.drops.push(report.dropped_object_count as u64);
        shape.encode_bytes += counts.encode_bytes_total;
        if counts.encodes_total > 0 {
            shape.materializations += 1;
        } else {
            shape.skipped_flushes += 1;
        }
    }
    shape
}

/// A FLUSH PER ROUND BECAME A FLUSH PER INTERVAL, measured against the arm that still pays per
/// round.
///
/// The control is an interval of 1, which is byte-for-byte the behaviour this change replaced:
/// every round's deletions run a countdown of 1 straight out, so every round is due. Running both
/// in one process on one fixture is what makes the shipped number mean something -- a guard that
/// asserts "few materializations" against no control passes just as well against a round that
/// stopped evicting.
#[test]
fn the_checkpoint_interval_turns_a_flush_per_round_into_a_flush_per_interval() {
    const KEYS: usize = 2_000;
    const ROUNDS: u64 = 40;
    const INTERVAL: u64 = crate::engine::LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT;

    let per_round = round_shape(KEYS, ROUNDS, 1);
    let interval = round_shape(KEYS, ROUNDS, INTERVAL);

    println!();
    println!("  keys={KEYS} rounds={ROUNDS}");
    println!(
        "  interval=1          rounds_with_drops={:<3} dropped={:<4} materializations={:<3} \
         skipped={:<3} encode_bytes={}",
        per_round.rounds_with_drops,
        per_round.dropped_total,
        per_round.materializations,
        per_round.skipped_flushes,
        per_round.encode_bytes
    );
    println!(
        "  interval={INTERVAL:<10} rounds_with_drops={:<3} dropped={:<4} materializations={:<3} \
         skipped={:<3} encode_bytes={}",
        interval.rounds_with_drops,
        interval.dropped_total,
        interval.materializations,
        interval.skipped_flushes,
        interval.encode_bytes
    );
    println!();

    // THE REGIME, BOTH ARMS. Neither reading means anything over a store that evicted nothing.
    assert!(
        per_round.rounds_with_drops > 1,
        "the control arm ran {} round(s) that dropped anything, so there is nothing to compare \
         against",
        per_round.rounds_with_drops
    );
    assert!(
        interval.rounds_with_drops > 1,
        "the interval arm ran {} round(s) that dropped anything",
        interval.rounds_with_drops
    );

    // THE CONTROL STILL PAYS PER ROUND. This is what fails if the countdown stops being consulted
    // at all, or if an interval of 1 stops meaning "every round".
    assert_eq!(
        per_round.rounds_with_drops, per_round.materializations,
        "at interval 1 every round that drops must materialize the base index, which is the \
         behaviour being replaced -- {} of {} did",
        per_round.materializations, per_round.rounds_with_drops
    );
    assert_eq!(
        0, per_round.skipped_flushes,
        "at interval 1 no round may skip its checkpoint"
    );

    // THE CONTESTED CASE AROSE. Without this the arm below could pass on a fixture where every
    // round happened to be due.
    assert!(
        interval.skipped_flushes > 0,
        "no round skipped its checkpoint at interval {INTERVAL}, so this arm never entered the \
         regime it is about and its materialization count proves nothing"
    );

    // THE WIN, AS AN ACCOUNTING IDENTITY RATHER THAN A THRESHOLD. The countdown starts at 0 (DUE,
    // the safe post-reload direction), so the first round that drops is always a materialization,
    // and every INTERVAL deletions after it buys exactly one more. Spelling it out as the
    // arithmetic the implementation does -- rather than as "about N/K" -- is what makes it fail
    // when the countdown is reloaded with the wrong number or reset on a round that was not due.
    let expected = materializations_due(&interval.drops, INTERVAL);
    assert_eq!(
        expected, interval.materializations,
        "at interval {INTERVAL}, the recorded per-round drop counts {:?} make {expected} round(s) \
         due and the engine materialized the base index {} time(s)",
        interval.drops, interval.materializations
    );
    // AND THE SAME NUMBER FROM THE OUTSIDE, so the identity above cannot be satisfied by a
    // countdown that is wrong in the same way the replay above is. `dropped_total / INTERVAL`
    // rounded up, plus the always-due first round, is the ceiling however the drops fall.
    assert!(
        interval.materializations <= 1 + interval.dropped_total / INTERVAL,
        "{} deletions at interval {INTERVAL} cannot owe more than {} checkpoints, and {} were \
         taken",
        interval.dropped_total,
        1 + interval.dropped_total / INTERVAL,
        interval.materializations
    );
    assert!(
        interval.materializations < per_round.materializations,
        "the interval arm materialized the base index {} time(s) against the control's {}, so \
         nothing was saved",
        interval.materializations,
        per_round.materializations
    );
    assert!(
        interval.encode_bytes < per_round.encode_bytes,
        "the interval arm encoded {} bytes against the control's {}",
        interval.encode_bytes,
        per_round.encode_bytes
    );
}

/// THE INTERVAL IS A REAL SETTING AND ITS DEFAULT IS NOT 1.
///
/// An interval of 1 reproduces the per-round flush exactly, so a default of 1 would land this
/// change inert -- a feature nothing runs and nothing checks. This asserts the default BEHAVIOURALLY
/// as well as numerically: an engine built with no interval set at all must skip a second
/// consecutive round's checkpoint, which is false for any default of 1 however the constant reads.
#[test]
fn the_logged_deletion_flush_interval_is_a_real_setting_whose_default_is_not_one() {
    assert!(
        crate::engine::LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT > 1,
        "the default interval is {}; at 1 every round is due and this change is inert",
        crate::engine::LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    seed(&engine, 500);

    // The engine reads the setting, not a literal: this is the same reader the round calls, and it
    // is the value an operator's `TS_LOGGED_DELETION_INDEX_FLUSH_INTERVAL` lands in.
    let configured = engine.logged_deletion_index_flush_interval();
    assert_eq!(
        crate::engine::logged_deletion_index_flush_interval_from_env(),
        configured,
        "a fresh engine does not carry the configured interval"
    );
    assert!(
        configured > 1,
        "the interval this engine will use is {configured}, so every round is due and nothing is \
         deferred"
    );

    // BEHAVIOURAL, ON THE DEFAULT, WITH NOTHING SET. Round one is due because the countdown is 0
    // on a fresh load; round two must not be.
    crate::engine::shard_write_guard::reset_index_encode_counts();
    let first = engine.apply_storage_eviction(SHARD, 1, 2, false, true);
    let after_first = crate::engine::shard_write_guard::index_encode_counts();
    assert!(
        first.dropped_object_count > 0,
        "the first round must drop something: {first:?}"
    );
    assert!(
        after_first.encodes_total > 0,
        "the first round after a load must checkpoint -- the countdown is serde-skipped and \
         therefore DUE on a fresh load, which is the safe direction"
    );

    crate::engine::shard_write_guard::reset_index_encode_counts();
    let second = engine.apply_storage_eviction(SHARD, 1, 2, false, true);
    let after_second = crate::engine::shard_write_guard::index_encode_counts();
    assert!(
        second.dropped_object_count > 0,
        "the second round must drop something too, or it is not the contested case: {second:?}"
    );
    assert!(
        (second.dropped_object_count as u64) < configured,
        "the second round dropped {} objects against an interval of {configured}, so it is due \
         on its own account and says nothing about the default",
        second.dropped_object_count
    );
    assert_eq!(
        0, after_second.encodes_total,
        "the second consecutive round materialized the base index on the shipped default, so the \
         default is behaving as 1 whatever the constant says"
    );
}
