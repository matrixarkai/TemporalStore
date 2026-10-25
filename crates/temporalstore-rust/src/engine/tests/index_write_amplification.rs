// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE INDEX WRITES PER BYTE OF INDEX IT LOGS, ON THE ROUTING RANGE A DEPLOYMENT RUNS.
//!
//! # THE NUMBER THIS MODULE EXISTS TO STATE
//!
//! Write amplification on the index, as ONE ratio of two physical byte counts taken off the
//! filesystem:
//!
//! ```text
//!   a = bytes this store writes under `indexes/` in a steady-state round
//!       --------------------------------------------------------------
//!       bytes of index-log record frame those same writes appended
//! ```
//!
//! BOTH SIDES ARE PHYSICAL BYTES ON DISK, which is the only surface a write-amplification figure
//! can live on. The three surfaces the per-object index figures are quoted on -- the json stream,
//! the msgpack payload and the `TSIDX\x01` container -- are surfaces an ENCODER produces, and a
//! ratio that took its numerator off one and its denominator off another would be a unit
//! conversion wearing a ratio's clothes. Where this module does quote a per-object figure it says
//! which surface it is on: the base index on disk is the `TSIDX\x01` container, so it is a
//! COMPRESSED size and names no field; the index-log record frames are the store's own binary
//! framing over a json payload.
//!
//! # WHAT THE NUMERATOR IS MADE OF, BY THE NAME OF THE FILE EACH BYTE LANDS IN
//!
//! Four terms, each one asserted NON-ZERO where the round can produce it, because a decomposition
//! can be wrong in every term and still sum to the whole:
//!
//! - `base_index` -- `shard-{id}.index.json`, the whole served index, rewritten entire by a dump.
//! - `log_appended` -- index-log record frames appended by the writes. This is also the
//!   DENOMINATOR, and it is read from the store's own `IndexLogStats::bytes_written` so the
//!   numerator and the denominator do not come from one book.
//! - `log_rewritten` -- what the post-dump sweep COPIES FORWARD into the piece it is writing. The
//!   sweep drops whole sealed pieces by name without opening them and rewrites only the active
//!   piece, so this term is bounded by the rolling threshold and not by the store.
//! - `other` -- everything else that changed under `indexes/`, NAMED BY FILE, because a term
//!   called "other" is where a surprise hides. The first run of this module read 263,8xx B here
//!   at every store size and every key shape, flat across a 4x corpus, and it was the write-ahead
//!   log: `with_cache_block_store_and_index_dir` puts the log at `indexes/wals/`, so a walk of
//!   `indexes/` charges a 256 KiB log piece to the index and reads a=16.9 where the index's own
//!   writes read 9.09. The `wals/` subtree is excluded by path and reported BESIDE the numerator
//!   rather than silently dropped, and a write phase asserts it NON-ZERO -- an exclusion that
//!   quietly stopped matching anything would take the WAL back into the index figure.
//!
//! EVERY PHASE NAMES A SECOND BOOK, and the two are asserted EQUAL rather than close -- a
//! tolerance here would admit exactly the mis-sized rewrite the numerator is built on:
//!
//! - a WRITE phase appends and nothing else, so the walk's length change must equal the store's
//!   append counter to the byte. This is what stops the counter being believed on its own, and it
//!   holds because a roll is a `fs::rename` and never a copy.
//! - a DUMP phase is checked against the SWEEP'S OWN byte report: the sweep states the log's
//!   length on either side of itself from inside the store, and this walk measures the same two
//!   lengths from outside it.
//!
//! What LEFT the log is then taken as a difference and never as a sum of per-file rows. A per-file
//! sum cannot tell the catalog anchor a dump appends from the anchor the sweep then keeps, and the
//! first draft of this module was wrong by exactly that one record.
//!
//! # WHY THE ROUTING RANGE IS NAMED IN EVERY ARM
//!
//! `load_shard` leaves the range at `0..u32::MAX`, on which a key's routing bucket is effectively
//! unique -- buckets come out EQUAL to objects, so every per-bucket cost divides by one object and
//! the index is a different shape from the one a meta-server-managed shard holds. Every arm here
//! loads `0..1023` and ASSERTS the amortised regime it claims to be in before it reports anything:
//! 8,000 objects over 1,024 buckets is 7.81 per bucket, and an arm that found one object per
//! bucket would be measuring the unbounded range and would say so.
//!
//! # WHY THE SIZES ARE SWEPT AND THE SLOPE IS REPORTED
//!
//! A per-key figure over a hash table is a sawtooth, and a division at one size is not a slope.
//! The sizes are 2,000 / 8,000 / 32,000 objects, a 16x range, and what is reported is the fitted
//! slope of bytes written per object beside the division at each point. `a` itself is
//! dimensionless, so what the sweep shows about `a` is whether it MOVES -- and whether it moves is
//! the whole question, because the shape being priced is "write the whole index, occasionally".
//!
//! # WHY TWO KEY SHAPES
//!
//! The base index on disk is a zstd frame, so a run of identical padding bytes is nearly free in
//! it: a padded fixture once measured a key at 0.009 B per byte of key, which is a statement about
//! the padding and not about the store. Every figure here is taken on a padded key AND on a
//! scattered one, and the two are printed side by side.
//!
//! # THE SPACE SIDE, AND THE BOUND THE TWO SIT ON TOGETHER
//!
//! The index is held as a base image plus an append-only log of records that have not yet been
//! folded into it. A dump folds the log into the base and the sweep then drops what the base now
//! reflects, so the two together are a window that slides: the base is what the window has already
//! absorbed, the log is what is still ahead of it, and reclamation is the deletion of a whole dead
//! piece by name rather than any compaction of one.
//!
//! Space amplification is then
//!
//! ```text
//!   b = (base index on disk + the log at its high-water mark) / base index on disk
//! ```
//!
//! and for a window that absorbs its log at a threshold proportional to the base, the two are one
//! relation rather than two independent numbers:
//!
//! ```text
//!   a <= b / (b - 1)
//! ```
//!
//! `the_measured_amplification_tracks_the_space_amplification_bound` drives the threshold divisor
//! across a sweep and checks the measured `a` against the bound `b` implies at each point, so the
//! relation is a measurement and not an argument. b = 2 admits 2x, b = 5 admits 1.25x, b = 10
//! admits 1.11x: it is a tunable trade of disk for bytes written, and what the sweep says about
//! WHERE the shipped divisor sits on that curve is this module's result.
//!
//! # COUNTED, NEVER TIMED
//!
//! Every number here is a byte count read off the filesystem or off a store counter. This box
//! carries other tenants and sits between load 2 and load 30 for hours at a time; the same round
//! has been seen at 490 ms and at 1,077 ms. A count does not move with load.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const SHARD: ShardId = 1;
/// The shipped routing range's end bucket -- the default an operator is told to set, and the one
/// regime in which the index's per-bucket terms amortise over the objects sharing a bucket.
const SHIPPED_END_BUCKET: u32 = 1023;
/// Buckets that range holds.
const SHIPPED_BUCKETS: usize = 1024;
/// The store sizes every figure is swept over, 8x apart end to end.
///
/// What showing a dimensionless ratio does NOT move needs a RANGE, not a large absolute size, and
/// the absolute cost of a dump at 20,000 and 200,000 records is already priced by
/// `dump_release_scale`. A 32,000-object top arm was measured and dropped: it reads the same
/// `a/bound` to three figures and adds minutes to every run of this suite forever.
const SIZES: [usize; 3] = [2_000, 8_000, 16_000];
/// The size the published per-object index figures are taken at, and the one this module quotes
/// its single-point division at. 7,999 objects over 1,024 buckets is the 7.81-per-bucket regime.
const PUBLISHED_SIZE: usize = 8_000;
/// Fixture key width, including the seven-byte ordinal tail.
const KEY_WIDTH: usize = 32;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';
const VALUE_LEN: usize = 128;
const SEED_BATCH: usize = 500;
/// The rolling threshold, held FIXED across every arm. A deployment knob and not what is under
/// measurement; smaller than the shipped 64 KiB so that a fixture rolls into many pieces -- the
/// sweep's drop-a-whole-piece decision is PER PIECE and a log in one piece cannot price it.
///
/// 1 KiB rather than the 8 KiB a neighbouring fixture uses, because the threshold the SMALLEST arm
/// here holds out for is an eighth of a small base index: at 8 KiB the 2,000-object round fired
/// with the log still in its first piece and `assert_round_can_express_it` refused it, which is
/// that clause doing its job rather than a tuning accident.
const ROLL_BYTES: u64 = 1024;
/// The configured accrual FLOOR every arm passes. Small enough that the relative term binds at
/// every size and every divisor in the sweep, which each arm asserts rather than assumes.
const FLOOR_BYTES: u64 = 2 * 1024;
/// Records written between two cadence checks.
///
/// Small against the smallest threshold any arm holds out for, so a round OVERSHOOTS the threshold
/// by at most one batch. The overshoot is not cosmetic: a round that released 48% more than it
/// held out for -- which 100-record batches did at the 2,000-object arm -- reads a SMALLER `a` and
/// a LARGER `b` than the cadence chose, and the two move together, which is how an arm can sit on
/// the bound for a reason the cadence had nothing to do with.
const BATCH: usize = 25;
/// A round that has not fired after this many batches is an apparatus failure, not a zero.
const MAX_BATCHES: usize = 4_000;
/// Measured rounds per arm in the STORE sweep. Two, so the reported figure is not one round's.
const ROUNDS: usize = 2;
/// The ordinal the fixture never writes, so an absent read is a control and not an absence.
const WITHHELD_ORDINAL: usize = 7;
/// The divisors the bound sweep drives. Each one is a different space-amplification budget: the
/// threshold a dump holds out for is `base_index / divisor`, so the log may stand at `1/divisor`
/// of the base and the budget is `b = 1 + 1/divisor`.
///
/// 32 at the tight end and 2 at the loose one, which spans a bound of about 3 to about 33 -- a
/// factor of eleven, so an arm that holds the bound is not holding the same number as every other
/// arm. Divisor 1 is NOT in the sweep and was dropped after being measured: it admits a log as
/// large as the whole base, so each round roughly doubles the store and the arm's THIRD round
/// wrote 35,000 records. It read a=3.286 against a bound of 3.285 -- a/bound 1.0002, the same
/// answer as every arm here -- and it cost more than the other four together.
const DIVISOR_SWEEP: [u64; 5] = [2, 4, 8, 16, 32];
/// Measured rounds per BUDGET arm. One, not [`ROUNDS`]: the priming dump already makes the first
/// measured round a steady-state round, the relation under test is a per-round one, and a budget
/// arm's round grows the store by `1/divisor` -- so the loose end of the sweep compounds with
/// every extra round while saying nothing new. Round-to-round stability is the STORE sweep's
/// claim, where every arm is on the shipped divisor and the store grows by an eighth a round.
const BUDGET_ROUNDS: usize = 1;

/// Set the rolling threshold for THIS THREAD and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        crate::index_log::set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    crate::index_log::set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// What a fixture key's leading bytes are made of.
///
/// The base index on disk is a zstd frame, so the two shapes bracket the one thing that depends on
/// a key's CONTENT rather than its width.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run -- compressible, and the shape a fixture reaches for by default.
    Padded,
    /// Pseudorandom letters, derived from the ordinal so the fixture is reproducible.
    Scattered,
}

impl KeyShape {
    fn label(self) -> &'static str {
        match self {
            KeyShape::Padded => "padded",
            KeyShape::Scattered => "scattered",
        }
    }
}

const SHAPES: [KeyShape; 2] = [KeyShape::Padded, KeyShape::Scattered];

/// A key of exactly [`KEY_WIDTH`] bytes whose last six characters are `ordinal`.
///
/// The ordinal is fixed-width so no key is a substring of another, and the padding is DERIVED from
/// the ordinal rather than stored, so a reader can rebuild the bytes a given ordinal's key would
/// have had.
fn key_text(ordinal: usize, shape: KeyShape) -> String {
    let pad = KEY_WIDTH - TAIL_WIDTH;
    let head: Vec<u8> = match shape {
        KeyShape::Padded => vec![PAD_BYTE; pad],
        KeyShape::Scattered => {
            let mut state =
                (ordinal as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
            state |= 1;
            (0..pad)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    b'a' + (state % 26) as u8
                })
                .collect()
        }
    };
    let mut text = String::from_utf8(head).expect("the padding is ASCII by construction");
    text.push_str(&format!("-{ordinal:06}"));
    assert_eq!(
        text.len(),
        KEY_WIDTH,
        "APPARATUS: a fixture key must be exactly {KEY_WIDTH} bytes"
    );
    text
}

/// An engine on the SHIPPED routing range. The range is named rather than defaulted: the range
/// `load_shard` leaves behind puts one bucket per object and is a different index shape.
fn shipped_range_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "index-write-amplification".to_string(),
        shard_uri: "local://index-write-amplification/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "APPARATUS: the fixture could not load shard {SHARD} on 0..{SHIPPED_END_BUCKET}: {:?}",
        response.status
    );
    engine
}

/// Write ordinals `from..to`, skipping [`WITHHELD_ORDINAL`].
fn seed_range(engine: &TemporalEngine, from: usize, to: usize, shape: KeyShape) -> usize {
    let mut written = 0usize;
    let mut cursor = from;
    while cursor < to {
        let end = (cursor + SEED_BATCH).min(to);
        let commands = (cursor..end)
            .filter(|ordinal| *ordinal != WITHHELD_ORDINAL)
            .map(|ordinal| Command::StringSet {
                key: key_text(ordinal, shape),
                value: vec![b'v'; VALUE_LEN],
            })
            .collect::<Vec<_>>();
        written += commands.len();
        if !commands.is_empty() {
            let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: SHARD,
                commands,
            });
            assert!(
                response.status.ok,
                "APPARATUS: a seed batch failed: {:?}",
                response.status
            );
        }
        cursor = end;
    }
    written
}

/// Every file under `root`, by path, with its length and mtime.
///
/// Taken by THIS module off the filesystem rather than asked of the store, so the numerator is an
/// independent measurement of what the store did and not a sum of rows the store reports.
fn index_tree(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
            } else {
                out.insert(
                    path,
                    (meta.len(), meta.modified().unwrap_or(std::time::UNIX_EPOCH)),
                );
            }
        }
    }
    out
}

/// Which class a file under the store's index root belongs to, decided by its PATH.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IndexFile {
    /// `shard-{id}.index.json` -- the whole served index, rewritten entire.
    Base,
    /// `indexlogs/shard-{id}.indexlog*.bin` -- append-only, except the sweep's rewrite of the
    /// active piece.
    Log,
    /// `wals/` -- NOT THE INDEX. The write-ahead log shares the index root (see
    /// `with_cache_block_store_and_index_dir`), so it has to be excluded by path, and it is
    /// reported beside the numerator rather than dropped in silence.
    WriteAheadLog,
    Other,
}

fn classify(path: &Path) -> IndexFile {
    // The subtree first: a WAL piece carries neither of the two names below, so a name-only
    // classification put it in `Other` and it stayed there, 256 KiB a round.
    if path
        .components()
        .any(|part| part.as_os_str() == std::ffi::OsStr::new("wals"))
    {
        return IndexFile::WriteAheadLog;
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if format!("shard-{SHARD}.index.json") == name {
        IndexFile::Base
    } else if name.contains(".indexlog") {
        IndexFile::Log
    } else {
        IndexFile::Other
    }
}

/// Total length of every index-log piece in a snapshot.
fn log_total(tree: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Log)
        .map(|(_, (len, _))| *len)
        .sum()
}

/// The physical bytes written to the index across one phase, split by the file each landed in.
#[derive(Debug, Default, Clone)]
struct IndexBytes {
    base_index: u64,
    log_appended: u64,
    log_rewritten: u64,
    other: u64,
    /// The `other` term BY FILE NAME. A term called "other" is where a surprise hides, so it is
    /// never reported as one number.
    other_by_name: Vec<(String, u64)>,
    /// NOT PART OF THE NUMERATOR. The write-ahead log shares the index root, so it is measured and
    /// reported here to make the exclusion auditable instead of invisible.
    write_ahead_log: u64,
    /// Bytes of index log that LEFT the tree: whole sealed pieces unlinked by name, plus whatever
    /// the sweep's rewrite of the active piece did not carry forward. Not part of the numerator
    /// either -- it is what the window dropped behind itself.
    log_dropped: u64,
}

impl IndexBytes {
    /// THE NUMERATOR. The write-ahead log is not in it, and neither is what was dropped.
    fn total(&self) -> u64 {
        self.base_index + self.log_appended + self.log_rewritten + self.other
    }
}

/// Which phase an accounting covers, and therefore which second book checks it.
///
/// A phase that named no second book would be one measurement believed on its own.
#[derive(Debug, Clone, Copy)]
enum Phase {
    /// Pure writes. Nothing may leave the log and nothing may be rewritten, so the walk and the
    /// store's append counter must agree to the byte -- which is the check.
    Writes,
    /// A dump and the sweep behind it, checked against the SWEEP'S OWN byte report. The sweep runs
    /// after the dump has appended its catalog anchor, so its `bytes_before` is the walk's before
    /// plus that append, and its `bytes_after` is the walk's after.
    Dump { bytes_before: u64, bytes_after: u64 },
    /// A planted phase, which states what it planted.
    Planted { dropped: u64 },
}

/// What a phase wrote, from the two tree snapshots that bracket it plus the store's own
/// append counter.
///
/// `log_appended` is the store's counter and NOT the walk: the walk cannot tell an append from a
/// rewrite. The two books are then reconciled against whichever second book the [`Phase`] names,
/// never by deriving one from the other.
fn account(
    before: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    after: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    log_appended: u64,
    phase: Phase,
) -> IndexBytes {
    let mut out = IndexBytes {
        log_appended,
        ..IndexBytes::default()
    };
    for (path, (len, modified)) in after {
        let prior = before.get(path);
        let unchanged = prior == Some(&(*len, *modified));
        match classify(path) {
            IndexFile::Base => {
                if !unchanged {
                    // A whole rewrite: the dump serializes the served index and replaces the file,
                    // so what it wrote is the length it ended at and never a delta.
                    out.base_index += *len;
                }
            }
            IndexFile::Log => match prior {
                // Shrank: the sweep wrote a temporary holding what it carried FORWARD and renamed
                // it over the piece. What it WROTE is the length it ended at. This is the
                // rewrite-forward term, and the sweep reaches only the piece being written -- a
                // sealed piece is dropped by name without being opened -- so it is bounded by the
                // rolling threshold and not by the store.
                Some((was, _)) if *len < *was => out.log_rewritten += *len,
                // Grew, or is new because a roll renamed the active piece aside. Either way the
                // bytes are already in the store's append counter, so nothing is added here --
                // counting them again is exactly the double count the checks below refuse.
                _ => {}
            },
            IndexFile::WriteAheadLog => {
                if !unchanged {
                    out.write_ahead_log += *len;
                }
            }
            IndexFile::Other => {
                if !unchanged {
                    out.other += *len;
                    out.other_by_name.push((
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("?")
                            .to_string(),
                        *len,
                    ));
                }
            }
        }
    }
    // WHAT LEFT THE LOG, as a difference and not a sum of rows: everything that was there plus
    // everything appended, less what is there now. Whole sealed pieces unlinked by name land here
    // together with whatever the active piece's rewrite did not carry forward, because a per-file
    // sum cannot tell the anchor the dump appended from the anchor the sweep then kept.
    let had = log_total(before) + out.log_appended;
    let holds = log_total(after);
    assert!(
        had >= holds,
        "APPARATUS: the index log holds {holds} B after a phase in which it held {} B and had \
         {} B appended. It cannot hold more than was put into it, so one of the two books is \
         measuring something else",
        log_total(before),
        out.log_appended,
    );
    out.log_dropped = had - holds;
    match phase {
        Phase::Writes => {
            assert_eq!(
                out.log_dropped, 0,
                "APPARATUS: a WRITE phase dropped {} B of index log. Only a sweep drops, and a \
                 sweep does not run here, so the walk ({} B -> {holds} B) and the store's append \
                 counter ({} B) disagree",
                out.log_dropped,
                log_total(before),
                out.log_appended,
            );
            assert_eq!(
                out.log_rewritten, 0,
                "APPARATUS: a WRITE phase rewrote {} B of index log. An append-only phase shrinks \
                 no piece",
                out.log_rewritten,
            );
        }
        Phase::Dump {
            bytes_before,
            bytes_after,
        } => {
            // THE SECOND BOOK. The sweep reports the log's length on either side of itself from
            // inside the store; this walk measures the same two lengths from outside it. They are
            // asserted EQUAL rather than close: a tolerance here would admit exactly the
            // mis-sized rewrite the numerator is built on.
            assert_eq!(
                bytes_before, had,
                "APPARATUS: the sweep reports the log at {bytes_before} B before it ran; the walk \
                 measured {} B plus {} B the dump appended = {had} B",
                log_total(before),
                out.log_appended,
            );
            assert_eq!(
                bytes_after, holds,
                "APPARATUS: the sweep reports the log at {bytes_after} B after it ran; the walk \
                 measured {holds} B"
            );
        }
        Phase::Planted { dropped } => assert_eq!(
            out.log_dropped, dropped,
            "the walk attributed {} B dropped against a planted {dropped} B",
            out.log_dropped,
        ),
    }
    out
}

/// One steady-state round: write until the production cadence fires, then price the dump it fired.
#[derive(Debug, Clone)]
struct Round {
    objects_before: usize,
    records_written: usize,
    /// What the production threshold function asked for, from the base index before the round.
    threshold_bytes: u64,
    base_index_before: u64,
    base_index_after: u64,
    /// The log's high-water mark in the round -- what the space side is paid in.
    log_peak: u64,
    write_phase: IndexBytes,
    dump_phase: IndexBytes,
    pieces_before: usize,
    pieces_after: usize,
    /// Index-log records the sweep behind the dump REMOVED. The window slid this far, from the
    /// store's own report -- a fixture whose window never slides agrees with whatever it is asked.
    records_removed: usize,
}

impl Round {
    /// Physical bytes written under `indexes/` across the whole round.
    fn written(&self) -> u64 {
        self.write_phase.total() + self.dump_phase.total()
    }

    /// THE DENOMINATOR: bytes of index-log record frame the round's writes appended.
    fn logged(&self) -> u64 {
        self.write_phase.log_appended
    }

    /// Write amplification.
    fn a(&self) -> f64 {
        assert!(
            self.logged() > 0,
            "APPARATUS: a round at {} objects appended no index-log bytes, so it has no \
             denominator to be a ratio against",
            self.objects_before
        );
        self.written() as f64 / self.logged() as f64
    }

    /// Space amplification: what the index occupies at its high-water mark, over what the folded
    /// base alone occupies.
    fn b(&self) -> f64 {
        assert!(
            self.base_index_after > 0,
            "APPARATUS: a round at {} objects left no base index on disk, so there is nothing for \
             the space figure to be a multiple of",
            self.objects_before
        );
        (self.base_index_after + self.log_peak) as f64 / self.base_index_after as f64
    }

    /// The bound `b` implies. A window that absorbs its log at a threshold proportional to the
    /// base cannot write more than this per byte it logs.
    fn bound(&self) -> f64 {
        let b = self.b();
        assert!(
            b > 1.0,
            "APPARATUS: a round at {} objects measured space amplification {b}, which is not \
             above 1.0 -- the log stood at zero bytes, so there was no window and the bound is a \
             division by zero",
            self.objects_before
        );
        b / (b - 1.0)
    }
}

impl Round {
    /// How close the measured write amplification sits to the bound its own space amplification
    /// admits. 1.0 is ON the bound.
    fn on_bound(&self) -> f64 {
        self.a() / self.bound()
    }
}

/// Every assertion that says a round can express what is claimed of it.
///
/// A fixture that never rolls, never releases or never reaches the relative term agrees with
/// whatever it is asked, so each of those is refused by name.
fn assert_round_can_express_it(round: &Round) {
    assert!(
        round.pieces_before >= 2,
        "FIXTURE: the round at {} objects ran with the log in {} piece(s), so the sweep's \
         drop-a-whole-piece decision is not being exercised and nothing per-piece is priced",
        round.objects_before,
        round.pieces_before
    );
    assert!(
        round.threshold_bytes > FLOOR_BYTES,
        "FIXTURE: the round at {} objects held out for {} B, which is the configured floor \
         {FLOOR_BYTES} B rather than a threshold read off the {} B base index. The relative term \
         does not bind in this arm, so it is not the cadence under measurement",
        round.objects_before,
        round.threshold_bytes,
        round.base_index_before
    );
    assert!(
        round.write_phase.log_appended > 0,
        "FIXTURE: the round at {} objects appended no index-log bytes in its write phase",
        round.objects_before
    );
    assert!(
        round.dump_phase.base_index > 0,
        "FIXTURE: the round at {} objects dumped without writing a base index, so the term this \
         module exists to price is zero and the sum is carrying it somewhere else",
        round.objects_before
    );
    assert!(
        round.dump_phase.log_dropped > 0,
        "FIXTURE: the round at {} objects dumped and released no index log, so the window did not \
         slide and the space figure is a high-water mark of nothing",
        round.objects_before
    );
    assert!(
        round.records_removed > 0,
        "FIXTURE: the sweep behind the round at {} objects removed no index-log RECORD. Bytes can \
         leave a log by a file being replaced; records leave it only because the window moved \
         past them, and that is the claim",
        round.objects_before
    );
    assert!(
        round.pieces_after < round.pieces_before,
        "FIXTURE: the round at {} objects left the log in {} piece(s) against {} before. \
         Reclamation here is the DELETION of a whole dead piece by name, so a sweep that deleted \
         none of them compacted rather than dropped",
        round.objects_before,
        round.pieces_after,
        round.pieces_before
    );
    assert!(
        round.log_peak > 0,
        "FIXTURE: the round at {} objects recorded a zero log high-water mark",
        round.objects_before
    );
    assert!(
        round.write_phase.write_ahead_log > 0,
        "APPARATUS: the round at {} objects attributed ZERO bytes to the write-ahead log. The \
         log shares the index root and every one of these writes goes through it, so a zero \
         means the `wals/` exclusion has stopped matching anything -- and those bytes are back \
         in the index numerator, where they once read 263,8xx B a round",
        round.objects_before
    );
}

fn show_round(tag: &str, round: &Round) {
    eprintln!(
        "  {tag:22} objects={:6} wrote={:4} recs | threshold={:8} B (base {:8} B) | \
         WRITTEN base={:9} log_app={:8} log_rw={:5} other={:5} = {:9} B | \
         LOGGED {:8} B | dropped {:8} B | peak {:8} B | pieces {:3}->{:2} | \
         a={:7.3}  b={:6.4}  b/(b-1)={:7.3}  a/bound={:6.4} | EXCLUDED wal={:8} B",
        round.objects_before,
        round.records_written,
        round.threshold_bytes,
        round.base_index_before,
        round.dump_phase.base_index,
        round.write_phase.log_appended,
        round.dump_phase.log_rewritten + round.write_phase.log_rewritten,
        round.write_phase.other + round.dump_phase.other,
        round.written(),
        round.logged(),
        round.dump_phase.log_dropped,
        round.log_peak,
        round.pieces_before,
        round.pieces_after,
        round.a(),
        round.b(),
        round.bound(),
        round.a() / round.bound(),
        round.write_phase.write_ahead_log + round.dump_phase.write_ahead_log,
    );
    for (name, len) in round
        .write_phase
        .other_by_name
        .iter()
        .chain(round.dump_phase.other_by_name.iter())
    {
        eprintln!("      other: {len:9} B  {name}");
    }
}

/// Write in batches until the PRODUCTION cadence fires, then price the dump it fired.
///
/// The gate is the production one -- `maybe_dump_and_reclaim_with_threshold_and_divisor_for_test`
/// differs from what a reclaim thread calls only in taking its floor, its interval and its divisor
/// as arguments instead of reading them from the environment of the whole process. The
/// prediction made from `effective_index_dump_threshold_bytes` is asserted BOTH WAYS against that
/// gate, so a gate that fires early or refuses late is an apparatus failure and not a measurement.
fn run_round(
    engine: &TemporalEngine,
    dir: &Path,
    next_ordinal: &mut usize,
    shape: KeyShape,
    divisor: u64,
) -> Round {
    let store = engine.index_log_store();
    let indexes = dir.join("indexes");
    let objects_before = *next_ordinal;
    let base_index_before = base_index_on_disk(dir);
    let threshold_bytes = crate::index_log::effective_index_dump_threshold_bytes(
        FLOOR_BYTES,
        base_index_before,
        divisor,
    );

    let write_before = index_tree(&indexes);
    let appended_before = store.stats(SHARD).bytes_written;

    for batch in 0..MAX_BATCHES {
        seed_range(engine, *next_ordinal, *next_ordinal + BATCH, shape);
        *next_ordinal += BATCH;

        let accrued = store.undumped_len_since_dump(SHARD);
        if accrued < threshold_bytes {
            // The gate is still ASKED on the cheap path, and must agree with the prediction.
            let refused = engine
                .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                    SHARD,
                    FLOOR_BYTES,
                    0,
                    0,
                    divisor,
                )
                .is_none();
            assert!(
                refused,
                "APPARATUS: the cadence FIRED at {objects_before} objects on {accrued} B accrued, \
                 below the {threshold_bytes} B `effective_index_dump_threshold_bytes` says it \
                 holds out for -- the gate and the threshold function disagree, so nothing \
                 measured here is attributable"
            );
            assert!(
                batch + 1 < MAX_BATCHES,
                "APPARATUS: a round at {objects_before} objects never fired: {MAX_BATCHES} \
                 batches of {BATCH} against a threshold of {threshold_bytes} B, with {accrued} B \
                 accrued"
            );
            continue;
        }

        let write_after = index_tree(&indexes);
        let write_phase = account(
            &write_before,
            &write_after,
            store.stats(SHARD).bytes_written - appended_before,
            Phase::Writes,
        );
        // THE HIGH-WATER MARK IS HERE, not sampled along the way. A write phase only ever ADDS to
        // the log -- `Phase::Writes` asserts that nothing was dropped and nothing rewritten -- so
        // the log is longest at the instant before the dump, and a walk per batch measured the
        // same number at a directory walk per 25 records.
        let log_peak = log_total(&write_after);
        assert!(
            log_peak >= log_total(&write_before),
            "APPARATUS: the log stood at {log_peak} B before the dump and {} B before the write \
             phase that only appends to it",
            log_total(&write_before)
        );
        let pieces_before = store.piece_count(SHARD);
        let appended_at_dump = store.stats(SHARD).bytes_written;

        let fired = engine.maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
            SHARD,
            FLOOR_BYTES,
            0,
            0,
            divisor,
        );
        let Some(report) = fired else {
            panic!(
                "APPARATUS: the cadence REFUSED at {objects_before} objects with {accrued} B \
                 accrued against the {threshold_bytes} B \
                 `effective_index_dump_threshold_bytes` says is enough -- the gate and the \
                 threshold function disagree"
            );
        };
        assert!(
            !report.index_log_sweep_failed,
            "APPARATUS: the sweep behind the dump at {objects_before} objects FAILED, so its \
             byte report is the three zeros of a refusal and not the three zeros of an empty log"
        );

        let dump_after = index_tree(&indexes);
        let dump_phase = account(
            &write_after,
            &dump_after,
            store.stats(SHARD).bytes_written - appended_at_dump,
            Phase::Dump {
                bytes_before: report.index_log_bytes_before,
                bytes_after: report.index_log_bytes_after,
            },
        );
        return Round {
            objects_before,
            records_written: *next_ordinal - objects_before,
            threshold_bytes,
            base_index_before,
            base_index_after: base_index_on_disk(dir),
            log_peak,
            write_phase,
            dump_phase,
            pieces_before,
            pieces_after: store.piece_count(SHARD),
            records_removed: report.index_log_records_removed,
        };
    }
    unreachable!("the loop above either returns or asserts");
}

fn base_index_on_disk(dir: &Path) -> u64 {
    std::fs::metadata(
        dir.join("indexes")
            .join(format!("shard-{SHARD}.index.json")),
    )
    .map(|meta| meta.len())
    .unwrap_or(0)
}

/// Assert the arm is in the AMORTISED regime the shipped routing range puts it in, and return the
/// objects-per-bucket it reached.
///
/// An arm that found one bucket per object would be measuring `0..u32::MAX`, on which every
/// per-bucket term divides by one object. That is a different index shape and a figure taken there
/// does not travel.
fn assert_amortised_regime(engine: &TemporalEngine, objects: usize) -> f64 {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("the shard is loaded");
    let buckets = shard.bucket_index.bucket_map.len();
    let held = shard.strings().len();
    assert_eq!(
        held, objects,
        "FIXTURE: the arm acked {objects} object(s) and the index holds {held}, so the \
         denominator every figure is taken against is not the corpus"
    );
    assert!(
        buckets <= SHIPPED_BUCKETS,
        "FIXTURE: the index holds {buckets} bucket(s), above the {SHIPPED_BUCKETS} the shipped \
         range 0..{SHIPPED_END_BUCKET} can hold -- this arm is not on the shipped range"
    );
    let per_bucket = held as f64 / buckets as f64;
    assert!(
        per_bucket > 1.5,
        "FIXTURE: {held} object(s) over {buckets} bucket(s) is {per_bucket:.2} per bucket. The \
         amortised regime this module reports in needs objects to SHARE buckets; at one per bucket \
         every per-bucket term divides by one object, which is the unbounded range \
         0..u32::MAX and not what a deployment runs"
    );
    per_bucket
}

/// Least-squares slope of `y` against `x`.
fn slope(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    assert!(
        points.len() >= 2,
        "APPARATUS: a slope needs at least two points"
    );
    let sum_x: f64 = points.iter().map(|(x, _)| *x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| *y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();
    (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
}

/// Every key the corpus wrote reads back with the value it wrote, the withheld one reads absent,
/// and the count equals the corpus. Both directions, same build, same moment.
fn assert_serves_the_corpus(engine: &TemporalEngine, objects: usize, shape: KeyShape) {
    let mut missing = Vec::new();
    let mut wrong = Vec::new();
    let mut found = 0usize;
    for ordinal in 0..objects {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: key_text(ordinal, shape),
            },
        });
        let value = match response.response {
            CommandResponse::Bytes { value } => value,
            other => panic!(
                "APPARATUS: a string read at ordinal {ordinal} answered {other:?} rather than bytes"
            ),
        };
        if ordinal == WITHHELD_ORDINAL {
            assert!(
                value.is_none(),
                "NEGATIVE CONTROL: ordinal {WITHHELD_ORDINAL} was never written and reads back \
                 {} byte(s). A present read for a key that does not exist means a positive read \
                 anywhere in this corpus proves nothing",
                value.map(|bytes| bytes.len()).unwrap_or(0)
            );
            continue;
        }
        match value {
            None => missing.push(ordinal),
            Some(bytes) if bytes != vec![b'v'; VALUE_LEN] => wrong.push(ordinal),
            Some(_) => found += 1,
        }
    }
    assert!(
        missing.is_empty() && wrong.is_empty(),
        "READBACK: {} missing and {} wrong of {objects} on the {} corpus (first missing {:?}, \
         first wrong {:?})",
        missing.len(),
        wrong.len(),
        shape.label(),
        missing.first(),
        wrong.first(),
    );
    assert_eq!(
        found,
        objects - 1,
        "READBACK: {found} of {} written object(s) read back on the {} corpus",
        objects - 1,
        shape.label()
    );
}

/// WHAT THE INDEX WRITES PER BYTE IT LOGS, SWEPT OVER STORE SIZE AND OVER KEY SHAPE.
///
/// The number this module exists to state, with its numerator and denominator named, its regime
/// asserted and a slope rather than a division.
///
/// rust-internal: prices the engine's own index writes in bytes, no product behaviour
#[test]
fn what_the_index_writes_per_byte_of_index_it_logs() {
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;
    let mut table: Vec<(KeyShape, usize, f64, f64, f64, f64, f64, u64)> = Vec::new();

    for shape in SHAPES {
        eprintln!(
            "\n================ 0..{SHIPPED_END_BUCKET}, {}-byte {} keys, divisor {divisor} \
             ================",
            KEY_WIDTH,
            shape.label()
        );
        for objects in SIZES {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = shipped_range_engine(dir.path());
            let acked = seed_range(&engine, 0, objects, shape);
            let per_bucket = assert_amortised_regime(&engine, acked);
            // PRIME, so every measured round is a steady-state round. A FIRST dump has no base
            // index on disk to read its threshold from, which is exactly the case in which the
            // relative term is not the cadence.
            engine
                .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
                .expect("APPARATUS: the priming dump did not complete");

            let mut next_ordinal = objects;
            let mut rounds = Vec::new();
            for _ in 0..ROUNDS {
                let round = run_round(&engine, dir.path(), &mut next_ordinal, shape, divisor);
                assert_round_can_express_it(&round);
                show_round(shape.label(), &round);
                rounds.push(round);
            }
            // Direction, after the rounds that rewrote the index and slid the window past it.
            // Taken at the published size only, on both key shapes: a read of every ordinal is a
            // command apiece, and the claim it supports -- that a slid window serves what it
            // served -- is driven to its strong form, across a RESTART and against a store that
            // never dumped, by `a_store_reloads_what_it_served_after_the_window_slid`.
            if objects == PUBLISHED_SIZE {
                assert_serves_the_corpus(&engine, next_ordinal, shape);
            }

            let mean = |pick: fn(&Round) -> f64| -> f64 {
                rounds.iter().map(pick).sum::<f64>() / rounds.len() as f64
            };
            let a = mean(Round::a);
            let b = mean(Round::b);
            let bound = mean(Round::bound);
            let base = rounds.last().expect("ROUNDS is non-zero").base_index_after;
            let written_per_object =
                rounds.iter().map(|round| round.written()).sum::<u64>() as f64 / rounds.len() as f64
                    / acked as f64;
            eprintln!(
                "  -> {:9} objects {:6} ({per_bucket:6.2}/bucket): a={a:7.3}  b={b:6.4}  \
                 bound b/(b-1)={bound:8.3}  base index {base:9} B ({:6.2} B/object, TSIDX \
                 container -- COMPRESSED, names no field)  round writes {written_per_object:9.1} \
                 B/object",
                shape.label(),
                acked,
                base as f64 / acked as f64,
            );
            table.push((shape, acked, a, b, bound, written_per_object, per_bucket, base));
        }
    }

    eprintln!("\n================ THE SWEEP ================");
    eprintln!(
        "  {:10} {:>8} {:>10} {:>9} {:>8} {:>11} {:>14}",
        "keys", "objects", "per-bucket", "a", "b", "b/(b-1)", "B/object/round"
    );
    for (shape, objects, a, b, bound, per_object, per_bucket, _) in &table {
        eprintln!(
            "  {:10} {objects:8} {per_bucket:10.2} {a:9.3} {b:8.4} {bound:11.3} {per_object:14.1}",
            shape.label()
        );
    }

    for shape in SHAPES {
        let arms = table
            .iter()
            .filter(|(arm, ..)| *arm == shape)
            .collect::<Vec<_>>();
        let per_object_slope = slope(
            &arms
                .iter()
                .map(|(_, objects, _, _, _, per_object, _, _)| (*objects as f64, *per_object))
                .collect::<Vec<_>>(),
        );
        let a_slope = slope(
            &arms
                .iter()
                .map(|(_, objects, a, ..)| (*objects as f64, *a))
                .collect::<Vec<_>>(),
        );
        let first = arms.first().expect("a shape has arms");
        let last = arms.last().expect("a shape has arms");
        eprintln!(
            "\n  {} keys over {} -> {} objects ({:.0}x):\n    a            {:.3} -> {:.3}  \
             ({:.4}x), slope {:+.3e} per object\n    bytes/object {:.1} -> {:.1} B, slope \
             {:+.3e} B per object per object",
            shape.label(),
            first.1,
            last.1,
            last.1 as f64 / first.1 as f64,
            first.2,
            last.2,
            last.2 / first.2,
            a_slope,
            first.5,
            last.5,
            per_object_slope,
        );
        // THE CLAIM: `a` does not grow with the store. A ratio whose numerator is the store and
        // whose denominator is the round would climb linearly; this one is held flat by the
        // threshold being read off the base index, so it is asserted FLAT and not merely bounded.
        assert!(
            last.2 <= first.2 * 1.40,
            "the {} arm's write amplification went {:.3} -> {:.3} over a {:.0}x store, a {:.3}x \
             climb. The threshold a dump holds out for is read off the base index, so this figure \
             is supposed to be independent of the store; a climb means it is not",
            shape.label(),
            first.2,
            last.2,
            last.1 as f64 / first.1 as f64,
            last.2 / first.2,
        );
        assert!(
            last.2 >= first.2 * 0.60,
            "the {} arm's write amplification FELL {:.3} -> {:.3} over the sweep. Far below \
             is not a better result here: it means the round released much more than the \
             threshold it held out for, so the cadence is not what decided and the arm is not \
             measuring one",
            shape.label(),
            first.2,
            last.2,
        );
        // AND IT SITS ON THE BOUND ITS OWN SPACE AMPLIFICATION ADMITS, at every point in the
        // sweep -- which is a stronger statement than being under it, and it is the result.
        //
        // UNDER is the safety claim: a window that writes more than b/(b-1) is not holding the
        // bound it is paying for. ON is the finding: this store does not merely respect the
        // relation, it is AT it, so there is no slack in the mechanism to recover. The only way
        // the figure moves is to move `b`.
        for (_, objects, a, b, bound, ..) in &arms {
            let on_bound = a / bound;
            assert!(
                on_bound <= 1.15,
                "at {objects} objects the {} arm wrote {a:.3} bytes per byte logged against a \
                 bound of {bound:.3} that its own space amplification {b:.4} admits -- \
                 {on_bound:.4} of it. A window that writes more than b/(b-1) is not holding the \
                 bound it is paying for",
                shape.label(),
            );
            assert!(
                on_bound >= 0.85,
                "at {objects} objects the {} arm wrote {a:.3} bytes per byte logged against a \
                 bound of {bound:.3} -- only {on_bound:.4} of it. Slack here is not a better \
                 result, it is a measurement that has come loose: `a` and `b` are two readings \
                 of ONE cadence here, and a round that sat well inside the relation released \
                 something other than what the threshold held out for",
                shape.label(),
            );
        }
    }

    // THE KEY SHAPE IS NOT WHAT DECIDES THIS FIGURE, and that is worth stating rather than
    // assuming: the base index on disk is a zstd frame, so a padded fixture can make a key look
    // nearly free. It cannot make the RATIO look good, because both sides of it are physical.
    let at = |shape: KeyShape| -> f64 {
        table
            .iter()
            .find(|(arm, objects, ..)| *arm == shape && *objects >= PUBLISHED_SIZE - 1)
            .map(|(_, _, a, ..)| *a)
            .expect("the published size is in the sweep")
    };
    let padded = at(KeyShape::Padded);
    let scattered = at(KeyShape::Scattered);
    eprintln!(
        "\n  AT {PUBLISHED_SIZE} OBJECTS the two key shapes read a={padded:.3} (padded) and \
         a={scattered:.3} (scattered), {:.4}x apart. A figure that moved with the padding would \
         be a figure about the fixture.",
        scattered / padded,
    );
    assert!(
        (scattered / padded) < 1.35 && (padded / scattered) < 1.35,
        "the two key shapes read a={padded:.3} and a={scattered:.3} at {PUBLISHED_SIZE} objects. \
         A write amplification that depends on whether the keys compress is not a property of the \
         store"
    );
}

/// WHAT AN IDLE STORE WRITES TO ITS INDEX, WHICH MUST BE NOTHING.
///
/// Without this every byte the measured rounds attribute is unattributable: a store that writes to
/// its index on a timer, a background sweep or a poll would put bytes into the numerator that no
/// write asked for, and the ratio above would be charging them to the records.
///
/// rust-internal: observes the engine's own index directory while nothing writes
#[test]
fn an_idle_store_writes_no_index_bytes() {
    let _rolling = roll_at(ROLL_BYTES);
    const OBJECTS: usize = 2_000;
    /// Cadence checks the idle interval makes. The production reclaim loop runs about one a
    /// second, so this is several minutes of idling counted in rounds rather than in time -- a
    /// count does not move with load and a sleep would measure the box.
    const IDLE_CHECKS: usize = 300;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = shipped_range_engine(dir.path());
    let acked = seed_range(&engine, 0, OBJECTS, KeyShape::Scattered);
    assert_amortised_regime(&engine, acked);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");

    let indexes = dir.path().join("indexes");
    let store = engine.index_log_store();
    let before = index_tree(&indexes);
    let appended_before = store.stats(SHARD).bytes_written;
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;

    let mut fired = 0usize;
    for _ in 0..IDLE_CHECKS {
        if engine
            .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                SHARD,
                FLOOR_BYTES,
                0,
                0,
                divisor,
            )
            .is_some()
        {
            fired += 1;
        }
    }
    let after = index_tree(&indexes);
    let appended = store.stats(SHARD).bytes_written - appended_before;
    let phase = account(&before, &after, appended, Phase::Writes);

    eprintln!(
        "IDLE CONTROL: {IDLE_CHECKS} cadence check(s) with no write in between fired {fired} \
         dump(s) and wrote base={} log_appended={} log_rewritten={} other={} = {} B under \
         indexes/",
        phase.base_index,
        phase.log_appended,
        phase.log_rewritten,
        phase.other,
        phase.total(),
    );
    assert_eq!(
        fired, 0,
        "IDLE CONTROL: {fired} dump(s) fired over {IDLE_CHECKS} checks with nothing written \
         between them. A cadence that fires on an idle store writes the whole index for no work, \
         and every byte the sweep above attributes to records would be partly this"
    );
    assert_eq!(
        phase.total(),
        0,
        "IDLE CONTROL: an idle store wrote {} B under indexes/ -- base {} B, log appended {} B, \
         log rewritten {} B, other {} B. Until this is zero no later byte is attributable to the \
         writes that were measured",
        phase.total(),
        phase.base_index,
        phase.log_appended,
        phase.log_rewritten,
        phase.other,
    );
}

/// THE INSTRUMENT RECOVERS A WRITE THAT WAS PLANTED IN FRONT OF IT.
///
/// The control that stops the idle zero above meaning the walk is blind. It plants a known number
/// of bytes into the index directory under each of the three names the accounting classifies by,
/// and requires the walk to attribute each one to its own term -- so a term that reads zero
/// because its classification is wrong fails here rather than passing quietly there.
///
/// rust-internal: writes and reads files in a temporary directory, no product behaviour
#[test]
fn the_index_byte_instrument_reports_a_planted_write() {
    const BASE_PLANT: usize = 4_096;
    const LOG_PLANT: usize = 1_024;
    const OTHER_PLANT: usize = 512;

    let dir = tempfile::tempdir().expect("tempdir");
    let indexes = dir.path().join("indexes");
    std::fs::create_dir_all(&indexes).expect("the fixture can create its own index directory");
    // A base index already on disk, so the plant below is a REWRITE of one and not a creation --
    // which is the case the accounting has to get right.
    std::fs::write(
        indexes.join(format!("shard-{SHARD}.index.json")),
        vec![b'0'; 7],
    )
    .expect("plant");
    std::fs::write(
        indexes.join(format!("shard-{SHARD}.indexlog.bin")),
        vec![b'0'; 9],
    )
    .expect("plant");
    let before = index_tree(&indexes);

    // The base index: a whole rewrite, so the attribution is the length it ENDS at and not the
    // delta. Planted LONGER than it was so a delta-shaped accounting would read 4,089 and be
    // caught.
    std::fs::write(
        indexes.join(format!("shard-{SHARD}.index.json")),
        vec![b'b'; BASE_PLANT],
    )
    .expect("plant");
    // The log: an append. The walk must NOT add these -- they belong to the store's own counter,
    // which this plant hands in explicitly below.
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(indexes.join(format!("shard-{SHARD}.indexlog.bin")))
        .expect("plant");
    use std::io::Write as _;
    log.write_all(&vec![b'l'; LOG_PLANT]).expect("plant");
    drop(log);
    // A sealed piece the sweep will be able to drop whole.
    std::fs::write(
        indexes.join(format!("shard-{SHARD}.indexlog.1-2-3.bin")),
        vec![b's'; OTHER_PLANT],
    )
    .expect("plant");
    // Something that is neither.
    std::fs::write(indexes.join("routing-range.json"), vec![b'r'; OTHER_PLANT])
        .expect("plant");

    let after = index_tree(&indexes);
    // The store's counter, handed in as the plant: the appended bytes plus the sealed piece, both
    // of which an appending store would have counted.
    let phase = account(
        &before,
        &after,
        (LOG_PLANT + OTHER_PLANT) as u64,
        Phase::Planted { dropped: 0 },
    );

    eprintln!(
        "PLANT: base={} (planted {BASE_PLANT}), log_appended={} (planted {}), \
         log_rewritten={}, other={} (planted {OTHER_PLANT}), total={}",
        phase.base_index,
        phase.log_appended,
        LOG_PLANT + OTHER_PLANT,
        phase.log_rewritten,
        phase.other,
        phase.total(),
    );
    assert_eq!(
        phase.base_index, BASE_PLANT as u64,
        "the walk attributed {} B to the base index against a planted {BASE_PLANT} B whole \
         rewrite",
        phase.base_index
    );
    assert_eq!(
        phase.log_appended,
        (LOG_PLANT + OTHER_PLANT) as u64,
        "the walk attributed {} B of index-log append against a planted {} B",
        phase.log_appended,
        LOG_PLANT + OTHER_PLANT
    );
    assert_eq!(
        phase.log_rewritten, 0,
        "the walk attributed {} B of sweep rewrite where nothing shrank",
        phase.log_rewritten
    );
    assert_eq!(
        phase.other, OTHER_PLANT as u64,
        "the walk attributed {} B to the other term against a planted {OTHER_PLANT} B",
        phase.other
    );

    // AND THE OTHER DIRECTION: a sweep that shrinks the active piece and unlinks a sealed one is
    // attributed as a rewrite plus a drop, with nothing appended.
    let shrink_before = index_tree(&indexes);
    std::fs::write(
        indexes.join(format!("shard-{SHARD}.indexlog.bin")),
        vec![b'w'; 64],
    )
    .expect("plant");
    std::fs::remove_file(indexes.join(format!("shard-{SHARD}.indexlog.1-2-3.bin")))
        .expect("plant");
    let shrink_after = index_tree(&indexes);
    let swept = account(
        &shrink_before,
        &shrink_after,
        0,
        Phase::Planted {
            dropped: (LOG_PLANT + 9 - 64 + OTHER_PLANT) as u64,
        },
    );
    eprintln!(
        "PLANT (sweep shape): log_rewritten={} log_dropped={} base={} other={}",
        swept.log_rewritten, swept.log_dropped, swept.base_index, swept.other
    );
    assert_eq!(
        swept.log_rewritten, 64,
        "the walk attributed {} B of rewrite against a piece that ended at 64 B",
        swept.log_rewritten
    );
    assert_eq!(
        swept.log_dropped,
        (LOG_PLANT + 9 - 64 + OTHER_PLANT) as u64,
        "the walk attributed {} B dropped against a piece that lost {} B and a sealed piece of \
         {OTHER_PLANT} B unlinked whole",
        swept.log_dropped,
        LOG_PLANT + 9 - 64
    );
}

/// WHAT THE STORE SERVES AFTER THE WINDOW SLID, AND AFTER A RELOAD OVER THE SAME DIRECTORIES.
///
/// The correctness half, in BOTH directions and in its strong form. A window that drops what it
/// has folded is silent when it drops too much and merely slow when it drops too little, so this
/// does not count records -- it compares the served sequence ELEMENT BY ELEMENT against a control
/// store that took the same writes and never dumped at all.
///
/// Three points, because a reconstruction that is correct only until the next write is not one:
/// after the rounds, immediately after a RELOAD on a fresh engine over the same directories, and
/// after writing past that reload. The withheld ordinal is absent at every point, same build and
/// same moment as the positive reads, so a present read anywhere here is a result and not the
/// instrument answering yes to everything.
///
/// AND IT ASSERTS THE WINDOW ACTUALLY SLID before comparing anything: records removed above zero
/// and pieces deleted, both from the store's own report. A fixture that never slides agrees with
/// whatever it is asked.
///
/// rust-internal: reconstructs the engine's own served index after a reload, no product behaviour
#[test]
fn a_store_reloads_what_it_served_after_the_window_slid() {
    const OBJECTS: usize = 4_000;
    const PAST_RELOAD: usize = 400;
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;
    let shape = KeyShape::Scattered;

    /// What the store serves for every ordinal below `upto`: the value's length, or `None`.
    fn served(engine: &TemporalEngine, upto: usize, shape: KeyShape) -> Vec<Option<usize>> {
        (0..upto)
            .map(|ordinal| {
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet {
                        key: key_text(ordinal, shape),
                    },
                });
                match response.response {
                    CommandResponse::Bytes { value } => value.map(|bytes| bytes.len()),
                    other => panic!("APPARATUS: a string read answered {other:?}"),
                }
            })
            .collect()
    }

    fn compare(tag: &str, observed: &[Option<usize>], control: &[Option<usize>]) {
        assert_eq!(
            control.len(),
            observed.len(),
            "{tag}: the two stores serve different numbers of records"
        );
        let first = control
            .iter()
            .zip(observed.iter())
            .position(|(left, right)| left != right);
        assert_eq!(
            first, None,
            "{tag}: the store whose window slid first differs from the store that never dumped at \
             ordinal {first:?} -- control {:?} against observed {:?}",
            first.and_then(|at| control.get(at)),
            first.and_then(|at| observed.get(at)),
        );
    }

    let control_dir = tempfile::tempdir().expect("tempdir");
    let subject_dir = tempfile::tempdir().expect("tempdir");
    let control = shipped_range_engine(control_dir.path());
    let subject = shipped_range_engine(subject_dir.path());
    let acked = seed_range(&subject, 0, OBJECTS, shape);
    seed_range(&control, 0, OBJECTS, shape);
    assert_amortised_regime(&subject, acked);
    subject
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");

    let mut next_ordinal = OBJECTS;
    for _ in 0..ROUNDS {
        let round = run_round(&subject, subject_dir.path(), &mut next_ordinal, shape, divisor);
        assert_round_can_express_it(&round);
        show_round("reload", &round);
    }
    seed_range(&control, OBJECTS, next_ordinal, shape);

    // DENOMINATOR: the control has to serve the corpus, or a match below is two stores agreeing
    // about nothing.
    let expected = served(&control, next_ordinal, shape);
    let live = expected.iter().filter(|slot| slot.is_some()).count();
    assert_eq!(
        live,
        next_ordinal - 1,
        "APPARATUS: the control served {live} of {} written ordinal(s) without dumping at all",
        next_ordinal - 1
    );
    assert!(
        expected[WITHHELD_ORDINAL].is_none(),
        "NEGATIVE CONTROL: the control store serves the withheld ordinal {WITHHELD_ORDINAL}, so \
         the fixture never withheld it"
    );

    compare("after the window slid", &served(&subject, next_ordinal, shape), &expected);

    // THE RELOAD. A fresh engine over the SAME directories: the base index the dumps wrote plus
    // the log they left, and nothing the dropped engine was holding in memory.
    drop(subject);
    let reloaded = shipped_range_engine(subject_dir.path());
    compare(
        "immediately after a reload",
        &served(&reloaded, next_ordinal, shape),
        &expected,
    );
    assert_amortised_regime(&reloaded, next_ordinal - 1);

    // AND PAST IT.
    let after = next_ordinal + PAST_RELOAD;
    seed_range(&reloaded, next_ordinal, after, shape);
    seed_range(&control, next_ordinal, after, shape);
    compare(
        "after writing past the reload",
        &served(&reloaded, after, shape),
        &served(&control, after, shape),
    );
    eprintln!(
        "RELOAD: {} ordinal(s) compared element by element at three points, the withheld one \
         absent at each; the reload read the base index plus the log the slides left behind it",
        after
    );
}

/// WHERE THE SHIPPED CADENCE SITS ON THE SPACE-FOR-WRITES CURVE.
///
/// The index is a base image plus a log of records not yet folded into it, and the threshold a
/// dump holds out for is `base_index / divisor`. So the divisor IS a space-amplification budget:
/// the log may stand at `1/divisor` of the base, which is `b = 1 + 1/divisor`, and the bound
/// `a <= b/(b-1)` is a claim this store can be driven across rather than argued about.
///
/// Four divisors on ONE store size, so what differs between the arms is the budget and not the
/// corpus. Each arm asserts the relative term binds before it reports, because an arm that fell
/// back to the configured floor is measuring the floor.
///
/// rust-internal: drives the engine's own dump cadence across a sweep of budgets, no product
/// behaviour
#[test]
fn the_measured_amplification_tracks_the_space_amplification_bound() {
    let _rolling = roll_at(ROLL_BYTES);
    // Smaller than the store sweep's published size on purpose. What this test varies is the
    // BUDGET, which is dimensionless, so it needs a range of budgets and not a large store -- and
    // the loose end of the sweep admits a log a HALF of the base, so a round there writes half the
    // store whatever the store is.
    const OBJECTS: usize = 4_000;
    let mut readings: Vec<(u64, f64, f64, f64)> = Vec::new();

    for divisor in DIVISOR_SWEEP {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = shipped_range_engine(dir.path());
        let acked = seed_range(&engine, 0, OBJECTS, KeyShape::Scattered);
        assert_amortised_regime(&engine, acked);
        engine
            .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
            .expect("APPARATUS: the priming dump did not complete");

        let mut next_ordinal = OBJECTS;
        let mut rounds = Vec::new();
        eprintln!("\n=== divisor {divisor} (budget b = 1 + 1/{divisor}) ===");
        for _ in 0..BUDGET_ROUNDS {
            let round = run_round(
                &engine,
                dir.path(),
                &mut next_ordinal,
                KeyShape::Scattered,
                divisor,
            );
            assert_round_can_express_it(&round);
            show_round(&format!("d={divisor}"), &round);
            rounds.push(round);
        }
        if divisor == crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR {
            assert_serves_the_corpus(&engine, next_ordinal, KeyShape::Scattered);
        }
        let mean = |pick: fn(&Round) -> f64| -> f64 {
            rounds.iter().map(pick).sum::<f64>() / rounds.len() as f64
        };
        readings.push((divisor, mean(Round::a), mean(Round::b), mean(Round::bound)));
    }

    eprintln!("\n================ THE BUDGET SWEEP, {OBJECTS} objects ================");
    eprintln!(
        "  {:>8} {:>10} {:>9} {:>12} {:>10}",
        "divisor", "b", "a", "b/(b-1)", "a/bound"
    );
    for (divisor, a, b, bound) in &readings {
        eprintln!("  {divisor:8} {b:10.4} {a:9.3} {bound:12.3} {:10.4}", a / bound);
    }

    // THE BOUND HOLDS AT EVERY POINT.
    for (divisor, a, b, bound) in &readings {
        assert!(
            *a <= *bound * 1.25,
            "at divisor {divisor} the store wrote {a:.3} bytes per byte logged against the \
             {bound:.3} its measured space amplification {b:.4} admits. The band is 25%: the \
             threshold is read off the base index BEFORE a round and the base grows during the \
             round it admits, so the cost divided is the larger one and the measured figure sits \
             structurally a little above the relation rather than on it"
        );
    }
    // AND IT IS NOT VACUOUS: the bound has to MOVE across the sweep, or every arm is being
    // compared against the same number and a passing arm says nothing.
    let widest = readings
        .iter()
        .map(|(_, _, _, bound)| *bound)
        .fold(f64::MIN, f64::max);
    let tightest = readings
        .iter()
        .map(|(_, _, _, bound)| *bound)
        .fold(f64::MAX, f64::min);
    assert!(
        widest >= tightest * 4.0,
        "APPARATUS: the bound read {tightest:.3} to {widest:.3} across the whole divisor sweep -- \
         too close together for the sweep to be driving a budget, so an arm that holds the bound \
         holds it for the wrong reason"
    );
    // AND `a` TRACKS IT. A figure that ignored the budget would be flat across the sweep.
    let first = readings.first().expect("the sweep has arms");
    let last = readings.last().expect("the sweep has arms");
    eprintln!(
        "\n  divisor {} -> {}: b {:.4} -> {:.4}, a {:.3} -> {:.3} ({:.3}x), bound {:.3} -> {:.3} \
         ({:.3}x)\n  THE SHIPPED DIVISOR IS {}, so the shipped store spends {:.1}% of its base \
         index on log footprint and writes about {:.1} bytes of index per byte of index it logs. \
         The curve is a TRADE: a larger space budget buys a smaller figure, and the reachable \
         range of this knob is bounded below by divisor 1 -- b = 2 -- because the threshold is a \
         DIVISION of the base index and cannot express a log larger than one.",
        first.0,
        last.0,
        first.2,
        last.2,
        first.1,
        last.1,
        last.1 / first.1,
        first.3,
        last.3,
        last.3 / first.3,
        crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR,
        100.0 / crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR as f64,
        crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR as f64 + 1.0,
    );

    assert!(
        last.1 >= first.1 * 2.0,
        "`a` read {:.3} at divisor {} and {:.3} at divisor {} -- it did not track the budget \
         across the sweep, so the divisor is not what decides what this store writes and the \
         bound above is being held for some other reason",
        first.1,
        first.0,
        last.1,
        last.0,
    );
    let _ = BTreeSet::<u64>::new();
}
