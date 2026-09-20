// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CATALOG DUMP COSTS AND WHAT IT RELEASES, AT TWO CORPUS SIZES.
//!
//! Nothing bounds the retained index-log pieces except a completed dump -- measured at 20,000 and
//! 200,000 records in `index_log_scale::what_the_index_log_fold_costs_on_a_large_store`, which
//! found no per-round cap and no ceiling on the log's footprint separate from the records. The
//! dump is therefore the mechanism the whole embedded engine's boundedness rests on, and it had
//! only ever been measured small: `dump_scale` runs at 2,000 and 20,000, `reclaim_dump` at 64.
//! This starts where those stop.
//!
//! Counted, never timed. The box these run on carries five other tenants and one test has been
//! seen to vary 2.4x in a day, so every number below is a byte count, a barrier count or a
//! directory listing.
//!
//! THE RESULT, at a fixed 8 KiB rolling threshold, measuring the SECOND dump -- the one a running
//! store keeps paying, not the first:
//!
//! ```text
//!   records   the dump writes   it releases   written per byte released
//!    20,000       1,584,685 B     160,229 B                       9.89
//!   200,000      13,499,807 B     160,271 B                      84.23
//! ```
//!
//! THE RELEASE IS PROPORTIONAL TO THE WORK. 160,229 B against 160,271 B -- the same 4,000 records
//! written since the previous dump, priced the same at ten times the corpus. The index log goes
//! back to ONE piece of 69 bytes at both sizes, so the bound the store rests on is reachable, and
//! reaching it costs nothing extra as the store grows.
//!
//! THE COST IS PROPORTIONAL TO THE STORE. A dump serializes the whole served index and writes it
//! durably (`engine/persistence.rs`, step 2 of `dump_index_catalog_anchored`), so the bytes it
//! writes are the shard's, not the round's: 1,584,329 B of base index at 20,000 records and
//! 13,499,438 B at 200,000. That is a durability decision and this does not argue with it. What
//! it measures is that the two halves scale differently, so the price of the bound climbs with
//! the corpus while what it buys does not.
//!
//! BOTH REGIMES, BECAUSE THE CADENCE IS A KNOB. `TS_INDEX_DUMP_WAL_GAP_BYTES` decides how much
//! index log accrues before a dump fires, which is the same thing as how many records a dump has
//! to show for itself.
//!
//! - FIXED GAP -- what ships: 1 MiB whatever the store holds, so a dump always releases about the
//!   same amount. Written per byte released goes 9.89 -> 84.23, a factor of 8.52 over a corpus
//!   factor of 10.
//! - PROPORTIONAL GAP -- the gap grows with the store. Written per byte released is 9.89 at
//!   20,000 and 9.89 at 200,000: FLAT to four figures, and a measurement taken only here would
//!   report a dump whose price does not move with the corpus at all.
//!
//! The shipped default is the first one. Derived from the two rates below -- 40.05 B of index log
//! per record, flat at both sizes, and the base index the dump writes -- the 1 MiB gap admits
//! about 26,200 records between dumps, and the base index first exceeds that 1 MiB somewhere
//! between 13,200 and 15,600 records. Past that point every dump the shipped cadence fires writes
//! more bytes than the cadence will ever let it release.
//!
//! CAN IT KEEP UP. `a_store_written_between_dumps_returns_to_its_floor_and_pays_the_whole_store`
//! writes 4,000 records and dumps, five times over. The log returns to 69 bytes every round and
//! the released figure is 160,229 B every round -- the bound is reached, every time. The dump's
//! own cost over those same five rounds is 1,584,685 / 1,848,016 / 2,112,666 / 2,375,802 /
//! 2,640,042 B. Written per byte released: 9.89, 11.53, 13.18, 14.83, 16.48. The store keeps up
//! and gets more expensive doing it, round after round, with nothing levelling off. Only the
//! released figure is asserted equal round to round; the costs are asserted to climb, because the
//! last digits of a byte count move a little between runs and the claim does not rest on them.
//!
//! WHAT HAPPENS WHEN IT CANNOT. Nothing sheds, throttles or signals. Searched across `engine.rs`,
//! `engine/`, `wal.rs`, `index_log.rs`, `storage_config.rs`, `data_node.rs` and `data_node/`:
//! `backpressure` appears only on the ingestion-stream and writeback paths, `shed` only in a
//! comment about follower divergence, and `throttle`, `slow_down`, `over_limit`, `too_many`,
//! `admission_control` and any spelling of dump-lag appear nowhere in the dump path at all. The
//! one mechanism that reacts to the dump falling behind is `TS_WAL_RESIDENT_BLOCKS` (default
//! 4,096), and it reacts by making the WRITER do more work -- a sweep that materializes the
//! oldest log-resident pages, taken on the write path after the ack. It bounds a set; it does not
//! slow, refuse or report anything.
//!
//! THE WRITE-AHEAD LOG HALF USED TO RELEASE NOTHING, AND SAID SO IN THE SAME WORDS AS SUCCESS.
//! At both corpus sizes the dump's WAL reclaim reported `0 -> 0 bytes, 0 records` against a log
//! holding 48 records in 524,288 bytes at 20,000 and 408 records in 3,145,728 bytes at 200,000,
//! while a control arm written one command at a time released 261,933 bytes in 1,199 records off
//! the same call. Underneath the zero, `gc_before_sequence` returned
//! `Corruption("binary record is incomplete")`, and `engine/persistence.rs` took it with `.ok()`
//! and then `.unwrap_or_default()`, so an errored reclaim and a reclaim with nothing to do
//! produced a byte-identical `CatalogDumpReclaimReport`.
//!
//! THE TRIGGER WAS THE PAYLOAD'S LAST BYTE, and it is fixed. A record is written as a binary
//! frame, which carries no delimiter and ends where its declared length ends; the reclaim walk
//! ran `strip_suffix(b"\n")` over it anyway, so a payload whose final byte was `0x0A` lost it,
//! read one byte short of what the frame declares, and was refused as a torn frame. None of the
//! three candidates this module first drove could see it -- it is not the record count, not the
//! bytes in the log, and not a newline INSIDE the payload, which lands in the middle and changes
//! nothing. `wal_reclaim_frame_boundary` holds the decision, the two-sided fixture and the
//! element-by-element comparison; this module's subject arm now releases 254,342 bytes in 8
//! records where it released nothing, and asserts that rather than the zero.
//!
//! The `.ok()` stays -- a failed sweep must not fail a dump that has already completed -- but
//! `CatalogDumpReclaimReport` now carries `wal_sweep_failed`, so the three zeros of a refusal
//! are no longer the three zeros of an empty log. The embedded proxy's reclaim loop is the only
//! thing keeping that store's logs bounded and it prints exactly that report.
//!
//! DIRECTION. A dump that releases too much is silent data loss and a dump that releases too
//! little is merely slow, so the strong form is asserted: after a dump, a reclaim and a reload
//! from disk, the store serves the same records in the same order as a control that never
//! dumped, compared element by element rather than counted.
//!
//! THE RESIDUAL is the kernel's `wchar` for THIS THREAD minus the bytes of every file under the
//! store root whose length or mtime moved across the dump -- an attribution built from two
//! directory walks this test does itself, never from anything the dump reports. It is 0 bytes at
//! both corpus sizes and in both regimes. `the_byte_instrument_reports_a_planted_write` is the
//! control that stops that zero meaning the instrument is blind: it plants 4,096 bytes outside
//! the walked root and requires the residual to recover exactly 4,096.
//!
//! PER THREAD, NOT PER PROCESS, AND THAT DISTINCTION WAS EARNED. Read from `/proc/self/io` the
//! residual is 0 when this module runs alone and 3,194 at 20,000 records when it runs inside the
//! whole lib gate -- bytes written by a background thread an earlier test left running while the
//! measured span was open. A residual is only independent of what it audits if it is also
//! independent of everything else sharing the process, so the counter is read from
//! `/proc/thread-self/io`.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const SHARD: ShardId = 1;
const SMALL: usize = 20_000;
const LARGE: usize = 200_000;
/// Records written between the priming dump and the measured one, in the FIXED-gap regime.
///
/// Small against both corpora on purpose: the claim being made is that the cost tracks THE STORE,
/// which needs the store to be what differs between the two arms.
const FIXED_SUFFIX: usize = 4_000;
/// Denominator of the PROPORTIONAL-gap regime. At SMALL it comes out equal to `FIXED_SUFFIX`, so
/// the two regimes are the same measurement at 20,000 and can only diverge at 200,000.
const PROPORTIONAL_DIVISOR: usize = 5;
const VALUE_LEN: usize = 128;
const SEED_BATCH: usize = 500;
/// The rolling threshold, held FIXED across every arm. It is a deployment knob and not what is
/// under measurement; 8 KiB rather than the shipped 64 KiB for the reason
/// `index_log_scale::what_an_index_log_replay_reads_at_two_corpus_sizes` gives, that the decision
/// being priced is PER PIECE and a fixture in one piece cannot price it.
const ROLL_BYTES: u64 = 8 * 1024;
/// `TS_INDEX_DUMP_WAL_GAP_BYTES`' shipped default, quoted here so the derived cadence figures in
/// the header have their input written down beside them.
const SHIPPED_GAP_BYTES: u64 = 1024 * 1024;

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

/// The kernel's own count of bytes THIS THREAD has written, from outside every book the engine
/// keeps. `None` is an apparatus failure at every call site rather than a zero.
///
/// `/proc/thread-self/io`, not `/proc/self/io`. The process-wide counter was the first thing
/// tried and it measures the other tenants: run alone the residual below is 0 at every size, and
/// run inside the whole lib gate the same measurement charged this dump 3,194 bytes it never
/// wrote -- a background thread some earlier test left running, writing while the span was open.
/// A residual is only independent of the thing it audits if it is also independent of everything
/// ELSE sharing the process.
fn bytes_written_now() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("wchar:") {
            return value.trim().parse().ok();
        }
    }
    None
}

/// Every file under `root`, by path, with its length and mtime.
fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
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
                let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                out.insert(path, (meta.len(), modified));
            }
        }
    }
    out
}

/// The files that are new or whose length or mtime moved between two snapshots, with the length
/// each ended at. THIS is the attributed row: it names what the operation touched without asking
/// the operation, so the residual beside it is a difference of two independent measurements and
/// not a sum of the rows it audits.
fn changed_files(
    before: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    after: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for (path, (len, modified)) in after {
        match before.get(path) {
            Some(prior) if prior == &(*len, *modified) => {}
            _ => out.push((
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("?")
                    .to_string(),
                *len,
            )),
        }
    }
    out
}

fn release_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

fn seed_in_batches_of(engine: &TemporalEngine, from: usize, to: usize, batch: usize) {
    let mut index = from;
    while index < to {
        let end = (index + batch).min(to);
        let commands = (index..end)
            .map(|cursor| Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; VALUE_LEN],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

fn seed_range(engine: &TemporalEngine, from: usize, to: usize) {
    seed_in_batches_of(engine, from, to, SEED_BATCH);
}

fn seed_one_at_a_time(engine: &TemporalEngine, from: usize, to: usize) {
    for index in from..to {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; VALUE_LEN],
            },
        });
        assert!(response.status.ok, "write failed: {:?}", response.status);
    }
}

/// The sealed index-log pieces, read off the directory BY THIS TEST rather than through the store.
///
/// A piece is named `shard-{shard}.indexlog.{start}-{end}-{max_applied_wal}.bin`. The third field
/// is the highest WAL sequence any record in the piece reflects, and it is the one field the
/// reclaim compares against the dump's anchor -- so a fixture whose pieces all carry the same
/// value there cannot tell a correct reclaim from a constant, and the fixture assertion below
/// requires every piece to name a distinct one.
fn sealed_pieces(index_dir: &Path) -> Vec<(u64, u64, u64, u64)> {
    let root = index_dir.join("indexlogs");
    let prefix = format!("shard-{SHARD}.indexlog.");
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(middle) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Some(middle) = middle
            .strip_suffix(".bin")
            .or_else(|| middle.strip_suffix(".jsonl"))
        else {
            continue;
        };
        let parts = middle.split('-').collect::<Vec<_>>();
        if parts.len() != 3 {
            continue;
        }
        let (Ok(start), Ok(end), Ok(max_applied)) = (
            parts[0].parse::<u64>(),
            parts[1].parse::<u64>(),
            parts[2].parse::<u64>(),
        ) else {
            continue;
        };
        let len = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        out.push((start, end, max_applied, len));
    }
    out.sort();
    out
}

/// One measured dump, with its cost and its release side by side.
#[derive(Debug)]
struct DumpAtSize {
    corpus: usize,
    suffix: usize,
    /// COST, from the kernel.
    kernel_bytes_written: u64,
    /// COST, attributed to named files by two directory walks this test does.
    attributed_bytes: u64,
    residual_bytes: i64,
    base_index_bytes: u64,
    barriers: u64,
    /// Pieces the log was in when the dump started.
    ///
    /// A reclaim unlinks whole pieces below its floor and fsyncs the directory entry once for the
    /// unlink; a log in ONE piece has no whole piece to unlink and never takes that barrier. Until
    /// the batch append started rolling, every log here was one piece, so the barrier count could
    /// be compared raw. It is the condition, not the barrier, that is recorded -- `sync_parent_dir`
    /// records every directory fsync under one name.
    wal_pieces_before: usize,
    dir_listings: u64,
    piece_paths: u64,
    /// FIXTURE.
    pieces_before: usize,
    sealed_before: usize,
    distinct_reflected_anchors: usize,
    undumped_before: u64,
    wal_records_before: usize,
    wal_bytes_on_disk_before: u64,
    /// RELEASE.
    index_log_released: u64,
    index_log_records_removed: usize,
    pieces_after: usize,
    wal_released: u64,
    wal_records_removed: usize,
    /// Whether the sweep errored. Its `Result` is dropped with `.ok()`, so without this the
    /// three fields above are defaults that read exactly like a reading.
    wal_sweep_failed: bool,
}

impl DumpAtSize {
    /// Bytes the dump wrote for each byte it freed. The whole question in one number.
    fn written_per_byte_released(&self) -> f64 {
        assert!(
            self.index_log_released > 0,
            "APPARATUS: a dump that released nothing cannot price what a release costs \
             (corpus {}, suffix {})",
            self.corpus,
            self.suffix
        );
        self.kernel_bytes_written as f64 / self.index_log_released as f64
    }

    fn index_log_bytes_per_record(&self) -> f64 {
        assert!(self.suffix > 0, "APPARATUS: empty suffix");
        self.undumped_before as f64 / self.suffix as f64
    }
}

/// Seed `corpus` records, dump once so the measured dump is a steady-state dump and not a first
/// one, write `suffix` more, then measure the dump that follows.
fn measure_dump(dir: &Path, corpus: usize, suffix: usize) -> DumpAtSize {
    let engine = release_engine(dir);
    let index_dir = dir.join("indexes");
    seed_range(&engine, 0, corpus);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");
    seed_range(&engine, corpus, corpus + suffix);

    let store = engine.index_log_store();
    let wal = engine.write_ahead_log_store();
    let sealed = sealed_pieces(&index_dir);
    let distinct_reflected_anchors = sealed
        .iter()
        .map(|(_, _, max_applied, _)| *max_applied)
        .collect::<BTreeSet<_>>()
        .len();
    let pieces_before = store.piece_count(SHARD);
    let undumped_before = store.undumped_len_since_dump(SHARD);
    let wal_info = wal.info(SHARD).ok();
    let wal_records_before = wal_info.as_ref().map(|info| info.records).unwrap_or(0);
    let wal_bytes_on_disk_before = wal_info.as_ref().map(|info| info.length_bytes).unwrap_or(0);
    // Pieces of the WAL, not of the index log: `store.piece_count` above counts the index log's.
    // Read from the directory the log's own path sits in, so it counts what is there rather than
    // what a report says is there.
    let wal_pieces_before = wal_info
        .as_ref()
        .and_then(|info| info.path.parent())
        .map(|root| crate::wal::wal_piece_extents_for_test(root, SHARD).len())
        .unwrap_or(0);
    let tree_before = tree_snapshot(dir);

    crate::index_log::probe::reset();
    crate::durability_metrics::reset();
    let wchar_before =
        bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
    let report = engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the measured dump did not complete");
    let wchar_after = bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");

    let dir_listings = crate::index_log::probe::dir_listings();
    let piece_paths = crate::index_log::probe::piece_paths();
    let barriers: u64 = crate::durability_metrics::snapshot().values().sum();
    let tree_after = tree_snapshot(dir);
    let attributed_bytes: u64 = changed_files(&tree_before, &tree_after)
        .iter()
        .map(|(_, len)| *len)
        .sum();
    let kernel_bytes_written = wchar_after.saturating_sub(wchar_before);

    DumpAtSize {
        corpus,
        suffix,
        kernel_bytes_written,
        attributed_bytes,
        residual_bytes: kernel_bytes_written as i64 - attributed_bytes as i64,
        base_index_bytes: std::fs::metadata(index_dir.join(format!("shard-{SHARD}.index.json")))
            .map(|meta| meta.len())
            .unwrap_or(0),
        barriers,
        wal_pieces_before,
        dir_listings,
        piece_paths,
        pieces_before,
        sealed_before: sealed.len(),
        distinct_reflected_anchors,
        undumped_before,
        wal_records_before,
        wal_bytes_on_disk_before,
        index_log_released: report
            .index_log_bytes_before
            .saturating_sub(report.index_log_bytes_after),
        index_log_records_removed: report.index_log_records_removed,
        pieces_after: store.piece_count(SHARD),
        wal_released: report.wal_bytes_before.saturating_sub(report.wal_bytes_after),
        wal_records_removed: report.wal_records_removed,
        wal_sweep_failed: report.wal_sweep_failed,
    }
}

/// Every assertion that says "this fixture can express what the test claims to measure".
///
/// Run against both arms at both sizes. A fixture in one piece, or one whose pieces all reflect
/// the same anchor, would let a reclaim that ignored the anchor entirely pass as a correct one.
fn assert_fixture_can_express_it(measured: &DumpAtSize) {
    let DumpAtSize { corpus, suffix, .. } = measured;
    assert!(
        measured.pieces_before >= 2,
        "FIXTURE: the log was in {} piece(s) when the measured dump ran, so nothing per-piece is \
         being measured (corpus {corpus}, suffix {suffix})",
        measured.pieces_before
    );
    assert!(
        measured.sealed_before >= 2,
        "FIXTURE: {} sealed piece(s) for the reclaim to decide about (corpus {corpus})",
        measured.sealed_before
    );
    assert_eq!(
        measured.distinct_reflected_anchors, measured.sealed_before,
        "FIXTURE: {} sealed pieces carry only {} distinct reflected-anchor values, and the reclaim \
         decides on exactly that field -- pieces that look alike cannot tell a correct decision \
         from a constant (corpus {corpus})",
        measured.sealed_before, measured.distinct_reflected_anchors
    );
    assert!(
        measured.undumped_before > 0,
        "FIXTURE: nothing had accrued since the priming dump, so the measured dump had nothing to \
         release (corpus {corpus}, suffix {suffix})"
    );
    assert!(
        measured.wal_records_before > 0 && measured.wal_bytes_on_disk_before > 0,
        "FIXTURE: the write-ahead log held {} records in {} bytes, so a zero WAL release below \
         would be an empty log rather than a reclaim that did nothing (corpus {corpus})",
        measured.wal_records_before,
        measured.wal_bytes_on_disk_before
    );
    assert!(
        measured.index_log_released > 0,
        "FIXTURE: the measured dump released nothing from the index log (corpus {corpus})"
    );
}

fn show(tag: &str, measured: &DumpAtSize) {
    eprintln!(
        "\nDUMP {tag} corpus={} suffix={} | COST kernel={} attributed={} residual={} base_index={} barriers={} listings={} piece_paths={}",
        measured.corpus,
        measured.suffix,
        measured.kernel_bytes_written,
        measured.attributed_bytes,
        measured.residual_bytes,
        measured.base_index_bytes,
        measured.barriers,
        measured.dir_listings,
        measured.piece_paths,
    );
    eprintln!(
        "DUMP {tag} corpus={} | RELEASE index_log={} B in {} records, pieces {}->{} | WAL {} B in {} records (log held {} records, {} B) | written per byte released {:.4}",
        measured.corpus,
        measured.index_log_released,
        measured.index_log_records_removed,
        measured.pieces_before,
        measured.pieces_after,
        measured.wal_released,
        measured.wal_records_removed,
        measured.wal_records_before,
        measured.wal_bytes_on_disk_before,
        measured.written_per_byte_released(),
    );
}

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// WHAT ONE DUMP COSTS AND WHAT IT RELEASES, AT 20,000 AND 200,000 RECORDS, IN BOTH REGIMES.
///
/// Four arms: the FIXED-gap regime (a constant number of records between dumps, which is what a
/// constant `TS_INDEX_DUMP_WAL_GAP_BYTES` amounts to) and the PROPORTIONAL-gap regime, each at
/// both corpus sizes. The two regimes are the same measurement at 20,000 by construction and can
/// only part at 200,000, so a difference between them is the corpus and nothing else.
///
/// rust-internal: prices the engine's own dump path in bytes and barriers, no product behaviour
#[test]
fn what_a_dump_costs_and_releases_at_two_corpus_sizes() {
    let _rolling = roll_at(ROLL_BYTES);

    let mut fixed = Vec::new();
    let mut proportional = Vec::new();
    for corpus in [SMALL, LARGE] {
        for (tag, suffix, bucket) in [
            ("fixed", FIXED_SUFFIX, &mut fixed),
            ("proportional", corpus / PROPORTIONAL_DIVISOR, &mut proportional),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let measured = measure_dump(dir.path(), corpus, suffix);
            assert_fixture_can_express_it(&measured);
            show(tag, &measured);
            bucket.push(measured);
            drop(dir);
        }
    }
    let (small_fixed, large_fixed) = (&fixed[0], &fixed[1]);
    let (small_proportional, large_proportional) = (&proportional[0], &proportional[1]);

    // THE CORRECTION BELOW MUST BE ABOUT SOMETHING. If no arm's log had rolled, the barrier
    // subtraction is zero on both sides of every comparison and this test would be asserting the
    // same thing it asserted before while appearing to account for a new term.
    assert!(
        [small_fixed, large_fixed, small_proportional, large_proportional]
            .iter()
            .any(|measured| measured.wal_pieces_before > 1),
        "no arm's log was in more than one piece, so the reclaim never unlinks and the barrier \
         correction below is vacuous: {:?}",
        [small_fixed, large_fixed, small_proportional, large_proportional]
            .map(|measured| measured.wal_pieces_before)
    );

    // ----------------------------------------------------------------- the independent residual
    for measured in [small_fixed, large_fixed, small_proportional, large_proportional] {
        assert_eq!(
            measured.residual_bytes, 0,
            "RESIDUAL: the kernel charged this process {} bytes across the dump and the files \
             under the store root account for {}, leaving {} unattributed at corpus {} -- every \
             byte a dump writes is supposed to land in a file this test can name",
            measured.kernel_bytes_written,
            measured.attributed_bytes,
            measured.residual_bytes,
            measured.corpus
        );
    }

    // ----------------------------------------------------------------- what is FLAT
    for (small, large, regime) in [
        (small_fixed, large_fixed, "fixed"),
        (small_proportional, large_proportional, "proportional"),
    ] {
        // FLAT, with the ONE term this engine's segmentation adds named and subtracted.
        //
        // A reclaim that unlinks whole pieces fsyncs the directory entry once for the unlink. A
        // log in one piece has nothing to unlink and never pays it, so a corpus large enough to
        // have ROLLED takes exactly one more barrier than one that did not -- measured as
        // `engine_wal_dir` 2 -> 3, with `wal_seal_outgoing_piece` at zero on every arm.
        //
        // That is a STEP, taken once per reclaim pass, not growth: it does not scale with the
        // corpus or with the pieces unlinked. Subtracting it by the CONDITION that produces it
        // keeps the claim -- a dump's barriers do not grow with the corpus -- exactly as strong as
        // it was, and asserting the condition occurs somewhere keeps the subtraction from being
        // `0 == 0`.
        let unlink_barrier = |measured: &DumpAtSize| u64::from(measured.wal_pieces_before > 1);
        let small_own = small.barriers - unlink_barrier(small);
        let large_own = large.barriers - unlink_barrier(large);
        assert_eq!(
            small_own, large_own,
            "FLAT: a dump took {small_own} durability barriers at {} records and {large_own} at \
             {} ({regime} regime), after subtracting the reclaim's directory fsync from the arm \
             whose log had pieces to unlink -- raw {} and {}, over logs in {} and {} piece(s)",
            small.corpus,
            large.corpus,
            small.barriers,
            large.barriers,
            small.wal_pieces_before,
            large.wal_pieces_before
        );
        assert_eq!(
            small.dir_listings, large.dir_listings,
            "FLAT: a dump listed the log directory {} times at {} records and {} at {} ({regime} \
             regime) -- this is the quantity that would say the enumeration had become per-piece",
            small.dir_listings, small.corpus, large.dir_listings, large.corpus
        );
        assert_eq!(
            small.pieces_after, 1,
            "FLAT: the log was left in {} pieces at {} records ({regime} regime); the bound the \
             store rests on is ONE piece",
            small.pieces_after, small.corpus
        );
        assert_eq!(
            large.pieces_after, 1,
            "FLAT: the log was left in {} pieces at {} records ({regime} regime)",
            large.pieces_after, large.corpus
        );
        let small_rate = small.index_log_bytes_per_record();
        let large_rate = large.index_log_bytes_per_record();
        assert!(
            (large_rate - small_rate).abs() / small_rate < 0.01,
            "FLAT: index log accrues {small_rate:.3} B per record at {} and {large_rate:.3} B at \
             {} ({regime} regime) -- the cadence arithmetic in this module's header rests on that \
             rate not moving",
            small.corpus,
            large.corpus
        );
    }

    // ----------------------------------------------------------------- what GROWS
    assert!(
        large_fixed.base_index_bytes >= small_fixed.base_index_bytes * 8,
        "GROWS: the base index a dump writes is {} B at {} records and {} B at {} -- under 8x \
         over a 10x corpus would mean the dump had stopped writing the whole store, which is the \
         premise of everything below",
        small_fixed.base_index_bytes,
        small_fixed.corpus,
        large_fixed.base_index_bytes,
        large_fixed.corpus
    );

    // ----------------------------------------------------------------- the release
    let released_drift = (large_fixed.index_log_released as f64
        - small_fixed.index_log_released as f64)
        .abs()
        / small_fixed.index_log_released as f64;
    assert!(
        released_drift < 0.01,
        "RELEASE: the same {FIXED_SUFFIX} records written since the last dump freed {} B at {} \
         records and {} B at {} -- the release is supposed to be priced by the WORK, not by the \
         store",
        small_fixed.index_log_released,
        small_fixed.corpus,
        large_fixed.index_log_released,
        large_fixed.corpus
    );

    // ----------------------------------------------------------------- the two regimes
    let fixed_ratio_small = small_fixed.written_per_byte_released();
    let fixed_ratio_large = large_fixed.written_per_byte_released();
    let proportional_ratio_small = small_proportional.written_per_byte_released();
    let proportional_ratio_large = large_proportional.written_per_byte_released();
    eprintln!(
        "\nDUMP REGIMES written-per-byte-released: fixed {fixed_ratio_small:.4} -> \
         {fixed_ratio_large:.4} ({:.3}x); proportional {proportional_ratio_small:.4} -> \
         {proportional_ratio_large:.4} ({:.4}x)",
        fixed_ratio_large / fixed_ratio_small,
        proportional_ratio_large / proportional_ratio_small,
    );
    assert!(
        fixed_ratio_large >= fixed_ratio_small * 8.0,
        "FIXED GAP: a dump wrote {fixed_ratio_small:.4} bytes per byte released at {} records and \
         {fixed_ratio_large:.4} at {} -- under 8x over a 10x corpus and the shipped cadence would \
         no longer be the regime where the price climbs with the store",
        small_fixed.corpus,
        large_fixed.corpus
    );
    // CONTROL ARM. Its own failure message, because a flat proportional arm is what proves the
    // climb above belongs to the regime and not to the apparatus: if BOTH arms climbed, the
    // measurement would be saying something about this test rather than about the cadence.
    assert!(
        (proportional_ratio_large - proportional_ratio_small).abs() / proportional_ratio_small
            < 0.01,
        "CONTROL: with the gap grown in proportion to the store, a dump wrote \
         {proportional_ratio_small:.4} bytes per byte released at {} records and \
         {proportional_ratio_large:.4} at {} -- this arm is supposed to be FLAT, and a climb here \
         means the fixed arm's climb is not the cadence",
        small_proportional.corpus,
        large_proportional.corpus
    );

    // ----------------------------------------------------------------- the shipped cadence
    let rate = large_fixed.index_log_bytes_per_record();
    let records_the_gap_admits = SHIPPED_GAP_BYTES as f64 / rate;
    let written_per_byte_at_shipped_gap =
        large_fixed.base_index_bytes as f64 / SHIPPED_GAP_BYTES as f64;
    eprintln!(
        "DUMP SHIPPED CADENCE (derived): gap {SHIPPED_GAP_BYTES} B at {rate:.3} B/record admits \
         {records_the_gap_admits:.0} records between dumps; at {} records the base index alone is \
         {} B, {written_per_byte_at_shipped_gap:.3} times what that gap can release",
        large_fixed.corpus, large_fixed.base_index_bytes
    );
    assert!(
        written_per_byte_at_shipped_gap > 1.0,
        "SHIPPED CADENCE: at {} records the dump writes {} B of base index against a {SHIPPED_GAP_BYTES} B \
         gap, which would be a dump that still pays for itself",
        large_fixed.corpus,
        large_fixed.base_index_bytes
    );
}

/// THE WRITE-AHEAD LOG HALF OF THE SAME DUMP RELEASES NOTHING, AND SAYS IT THE SAME WAY AS A
/// SWEEP THAT HAD NOTHING TO SAY.
///
/// Three arms, because the claim needs all three:
///
/// * SUBJECT -- the batch-written store the corpus sizes above are built from. Its WAL release is
///   zero against a log that demonstrably holds records.
/// * CONTROL -- the same call against a log written one command at a time, which releases plenty.
///   Without it the subject's zero could be a reclaim that never works, or an empty log.
/// * EMPTY -- a loaded shard nobody wrote to, where there is genuinely nothing to free.
///
/// The assertion that matters is the last comparison: SUBJECT and EMPTY report the IDENTICAL
/// triple. `dump_and_reclaim_index_logs_with_min_reclaimable` takes the sweep's `Result` with
/// `.ok()` and then reads every field through `.unwrap_or_default()`, so a sweep that returned an
/// error and a sweep that had nothing to do are the same `CatalogDumpReclaimReport` to the byte.
/// That much does not depend on knowing WHY the subject arm's sweep fails -- it is an error
/// rendering as success, and no operator and no test can tell the two apart.
///
/// rust-internal: prices the engine's own reclaim reporting, no product behaviour
#[test]
fn a_dump_reports_the_same_write_ahead_log_release_whether_or_not_one_happened() {
    const CONTROL_RECORDS: usize = 1_200;
    let _rolling = roll_at(ROLL_BYTES);

    let batched_dir = tempfile::tempdir().expect("tempdir");
    let batched = measure_dump(batched_dir.path(), SMALL, FIXED_SUFFIX);
    assert_fixture_can_express_it(&batched);

    // EMPTY ARM: a shard that is loaded and never written. The dump completes (it has an index to
    // serialize, even an empty one) and the sweep finds no log at all, which is the one case where
    // zero is the honest answer.
    let empty_dir = tempfile::tempdir().expect("tempdir");
    let empty = release_engine(empty_dir.path());
    let empty_wal_records = empty
        .write_ahead_log_store()
        .info(SHARD)
        .map(|info| info.records)
        .unwrap_or(0);
    let empty_report = empty
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the empty-shard dump did not complete");

    let control_dir = tempfile::tempdir().expect("tempdir");
    let control = release_engine(control_dir.path());
    seed_one_at_a_time(&control, 0, CONTROL_RECORDS);
    let control_wal = control.write_ahead_log_store();
    let control_records_before = control_wal.info(SHARD).map(|info| info.records).unwrap_or(0);
    let control_report = control
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the control dump did not complete");
    let control_released = control_report
        .wal_bytes_before
        .saturating_sub(control_report.wal_bytes_after);

    eprintln!(
        "\nDUMP WAL batched corpus={} log held {} records in {} B, dump released {} B in {} records",
        batched.corpus,
        batched.wal_records_before,
        batched.wal_bytes_on_disk_before,
        batched.wal_released,
        batched.wal_records_removed,
    );
    eprintln!(
        "DUMP WAL control one-command-at-a-time records={CONTROL_RECORDS} log held {control_records_before} records, dump released {control_released} B in {} records",
        control_report.wal_records_removed,
    );
    eprintln!(
        "DUMP WAL empty shard: log held {empty_wal_records} records, dump released {} B in {} records (before={} after={})",
        empty_report
            .wal_bytes_before
            .saturating_sub(empty_report.wal_bytes_after),
        empty_report.wal_records_removed,
        empty_report.wal_bytes_before,
        empty_report.wal_bytes_after,
    );

    // CONTROL FIRST, so a zero on the subject arm cannot be the reclaim never working at all.
    assert!(
        control_records_before > 0,
        "CONTROL: the control log held no records, so its release proves nothing"
    );
    assert!(
        control_released > 0 && control_report.wal_records_removed > 0,
        "CONTROL: a dump against a log written one command at a time released {control_released} B \
         in {} records -- this arm exists to prove a non-zero WAL release is expressible here, and \
         without it the subject arm's zero says nothing",
        control_report.wal_records_removed
    );
    // SUBJECT. This arm reported 0 B in 0 records when it was written, and the assertion here
    // said so. The cause was found -- the reclaim walk stripped a trailing delimiter off a
    // binary frame, which declares its own length, so a payload ending in `0x0A` read one byte
    // short and the whole sweep was refused as a torn frame. See `wal_reclaim_frame_boundary`.
    // Inverted rather than loosened, per the note this assertion used to carry: the arm still
    // has to say something, and what it says now is that the batch path releases.
    assert!(
        batched.wal_released > 0 && batched.wal_records_removed > 0,
        "SUBJECT: the dump released {} B in {} records from a log holding {} records in {} bytes. \
         A batch-written log reclaims now; a zero here is that defect returning, not a log with \
         nothing to give -- the fixture assertions above have already shown it holds records",
        batched.wal_released,
        batched.wal_records_removed,
        batched.wal_records_before,
        batched.wal_bytes_on_disk_before
    );

    // THE ONE THAT DOES NOT DEPEND ON KNOWING WHY, now the other way round. A store holding a
    // log and a store holding none used to hand back the same three numbers. They must not.
    assert_eq!(
        empty_wal_records, 0,
        "APPARATUS: the empty arm's log held {empty_wal_records} records, so it is not the \
         nothing-to-do case this comparison needs"
    );
    let empty_released = empty_report
        .wal_bytes_before
        .saturating_sub(empty_report.wal_bytes_after);
    assert_ne!(
        (batched.wal_released, batched.wal_records_removed),
        (empty_released, empty_report.wal_records_removed),
        "a store holding {} records in {} bytes and a store holding no log at all report the same \
         release again",
        batched.wal_records_before,
        batched.wal_bytes_on_disk_before
    );

    // And the term that says WHICH of the two a zero is, for the cases where a zero is still the
    // answer. The sweep's `Result` is still dropped with `.ok()` -- a failed sweep must not fail
    // a dump that has already completed -- so without this an error is three zeros again.
    assert!(
        !batched.wal_sweep_failed,
        "SUBJECT: the sweep reported a failure, so its three zeros would be a refusal rendered as \
         success were the term not there to say so"
    );
    assert!(
        !empty_report.wal_sweep_failed,
        "EMPTY: an empty shard's sweep reports a failure, so the term is reading something other \
         than the sweep's own outcome"
    );
}

/// WHAT A CALLER CAN TELL ABOUT A DUMP THAT DID NOT HAPPEN.
///
/// `maybe_dump_and_reclaim_index_logs` is what the embedded proxy polls on a timer, and it is the
/// only thing keeping that store's logs bounded. It answers `None` for a shard below the
/// threshold and `None` for a shard this engine does not serve at all, so a loop that reads it
/// cannot separate "nothing needed doing" from "this never ran".
///
/// rust-internal: reads the engine's own maintenance entry point, no product behaviour
#[test]
fn a_dump_that_was_not_needed_and_one_that_could_not_run_are_the_same_answer() {
    const UNSERVED_SHARD: ShardId = 99;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    seed_range(&engine, 0, 500);

    let below_threshold = engine.maybe_dump_and_reclaim_with_gap_for_test(SHARD, u64::MAX, 0, 0);
    let unserved = engine.maybe_dump_and_reclaim_with_gap_for_test(UNSERVED_SHARD, 1, 0, 0);
    let fired = engine.maybe_dump_and_reclaim_with_gap_for_test(SHARD, 1, 0, 0);

    // DENOMINATOR. Without an arm that fires, three `None`s would be a dump that never works.
    assert!(
        fired.is_some(),
        "APPARATUS: no arm of this fixture produced a dump, so two `None`s below say nothing"
    );
    assert!(
        below_threshold.is_none(),
        "a shard below the gap should not dump"
    );
    assert!(
        unserved.is_none(),
        "a shard this engine does not serve should not dump"
    );
    assert_eq!(
        below_threshold, unserved,
        "the one call the embedded reclaim loop makes answers identically for a shard that needed \
         no dump and a shard it cannot dump at all -- if these ever differ, the loop has gained a \
         way to know it is not running"
    );
}

/// DIRECTION: THE SAME RECORDS IN THE SAME ORDER, NOT THE SAME NUMBER OF THEM.
///
/// Releasing too little is slow and releasing too much is silent loss, so a count is the wrong
/// assertion: a reclaim that dropped one record and kept an extra would pass it. This compares
/// the served sequence element by element against a store that never dumped.
///
/// rust-internal: exercises the engine's dump/reclaim/reload cycle, no product behaviour
#[test]
fn a_dump_and_reclaim_leaves_the_same_records_in_the_same_order_as_a_store_that_never_dumped() {
    const CORPUS: usize = 5_000;
    let _rolling = roll_at(ROLL_BYTES);

    fn served_sequence(engine: &TemporalEngine, count: usize) -> Vec<Option<usize>> {
        (0..count)
            .map(|index| {
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet {
                        key: format!("k-{index:08}"),
                    },
                });
                match response.response {
                    CommandResponse::Bytes { value } => value.map(|bytes| bytes.len()),
                    _ => None,
                }
            })
            .collect()
    }

    let control_dir = tempfile::tempdir().expect("tempdir");
    let control = release_engine(control_dir.path());
    seed_range(&control, 0, CORPUS);
    let control_sequence = served_sequence(&control, CORPUS);

    let dumped_dir = tempfile::tempdir().expect("tempdir");
    let dumped = release_engine(dumped_dir.path());
    seed_range(&dumped, 0, CORPUS);
    let report = dumped
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the dump did not complete");
    drop(dumped);
    let reloaded = release_engine(dumped_dir.path());
    let reloaded_sequence = served_sequence(&reloaded, CORPUS);

    // DENOMINATORS. Two empty stores match element for element.
    let live = control_sequence.iter().filter(|slot| slot.is_some()).count();
    assert_eq!(
        live, CORPUS,
        "APPARATUS: the control served {live} of {CORPUS} records before any dump, so a match \
         below would be two stores agreeing about nothing"
    );
    assert!(
        report.index_log_records_removed > 0,
        "APPARATUS: the dump removed no index-log records, so the reload below did not have to \
         survive a reclaim"
    );

    let first_difference = control_sequence
        .iter()
        .zip(reloaded_sequence.iter())
        .position(|(control_slot, reloaded_slot)| control_slot != reloaded_slot);
    assert_eq!(
        first_difference, None,
        "the store that dumped, reclaimed and reloaded first differs from the one that never \
         dumped at record {first_difference:?}: control {:?} against reloaded {:?}",
        first_difference.and_then(|at| control_sequence.get(at)),
        first_difference.and_then(|at| reloaded_sequence.get(at)),
    );
    assert_eq!(
        control_sequence, reloaded_sequence,
        "the two stores serve different sequences"
    );
}

/// CAN THE DUMP KEEP UP WITH A STORE THAT KEEPS BEING WRITTEN.
///
/// Five rounds of "write 4,000 records, then dump". The log has to come back to its floor every
/// round for the bound to be reachable at all, and it does. What moves is the price: each round
/// releases the same bytes and writes more of them.
///
/// rust-internal: prices repeated engine dumps in bytes, no product behaviour
#[test]
fn a_store_written_between_dumps_returns_to_its_floor_and_pays_the_whole_store() {
    const PER_ROUND: usize = 4_000;
    const ROUNDS: usize = 5;
    let _rolling = roll_at(ROLL_BYTES);

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    seed_range(&engine, 0, SMALL);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");

    let store = engine.index_log_store();
    let mut written = SMALL;
    let mut costs = Vec::new();
    let mut releases = Vec::new();
    let mut floors = Vec::new();
    for _ in 0..ROUNDS {
        seed_range(&engine, written, written + PER_ROUND);
        written += PER_ROUND;
        let before = bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
        let report = engine
            .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
            .expect("APPARATUS: a round's dump did not complete");
        let after = bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
        costs.push(after.saturating_sub(before));
        releases.push(
            report
                .index_log_bytes_before
                .saturating_sub(report.index_log_bytes_after),
        );
        floors.push((store.log_len_bytes(SHARD), store.piece_count(SHARD)));
    }
    eprintln!("\nDUMP ROUNDS costs={costs:?}");
    eprintln!("DUMP ROUNDS releases={releases:?}");
    eprintln!("DUMP ROUNDS floors={floors:?}");

    // THE BOUND IS REACHED, EVERY ROUND.
    for (round, (bytes, pieces)) in floors.iter().enumerate() {
        assert_eq!(
            *pieces, 1,
            "round {round} left the log in {pieces} pieces holding {bytes} B -- the bound the \
             store rests on is one piece, and a round that does not reach it is a round the store \
             does not recover from"
        );
    }
    let first_release = releases[0];
    for (round, released) in releases.iter().enumerate() {
        assert_eq!(
            *released, first_release,
            "round {round} released {released} B against round 0's {first_release} B -- every \
             round wrote the same {PER_ROUND} records, so every round should free the same bytes"
        );
    }

    // AND COSTS MORE EVERY TIME.
    for round in 1..ROUNDS {
        assert!(
            costs[round] > costs[round - 1],
            "round {round} cost {} B against round {}'s {} B -- the store grew by {PER_ROUND} \
             records between them and the dump writes the whole store, so this is the quantity \
             that is supposed to climb",
            costs[round],
            round - 1,
            costs[round - 1]
        );
    }
    let first_ratio = costs[0] as f64 / releases[0] as f64;
    let last_ratio = costs[ROUNDS - 1] as f64 / releases[ROUNDS - 1] as f64;
    eprintln!(
        "DUMP ROUNDS written per byte released: {first_ratio:.2} -> {last_ratio:.2} over {ROUNDS} rounds"
    );
    assert!(
        last_ratio > first_ratio * 1.5,
        "over {ROUNDS} rounds on a store that grew from {SMALL} to {written} records, the price of \
         a dump went {first_ratio:.2} -> {last_ratio:.2} bytes written per byte released; under \
         1.5x and the cost would not be tracking the store"
    );
}

/// CONTROL FOR THE BYTE INSTRUMENT.
///
/// The residual in the measurement above is 0 at every size, and a 0 from a blind instrument
/// looks exactly like a 0 from an exact one. This plants a known write where the attribution walk
/// cannot see it and requires the residual to recover it to the byte.
///
/// rust-internal: proves this module's own measuring apparatus, no product behaviour
#[test]
fn the_byte_instrument_reports_a_planted_write() {
    const PLANTED: usize = 4_096;
    let walked = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("tempdir");

    let tree_before = tree_snapshot(walked.path());
    let before = bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
    std::fs::write(outside.path().join("planted"), vec![b'x'; PLANTED]).expect("plant the write");
    let after = bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
    let tree_after = tree_snapshot(walked.path());

    let attributed: u64 = changed_files(&tree_before, &tree_after)
        .iter()
        .map(|(_, len)| *len)
        .sum();
    let residual = after.saturating_sub(before) as i64 - attributed as i64;
    eprintln!(
        "\nDUMP INSTRUMENT planted={PLANTED} kernel={} attributed={attributed} residual={residual}",
        after - before
    );
    assert_eq!(
        attributed, 0,
        "APPARATUS: the walk attributed {attributed} B inside a root nothing was written to"
    );
    assert_eq!(
        residual, PLANTED as i64,
        "the instrument recovered {residual} of {PLANTED} planted bytes; a residual that cannot \
         see a byte written outside its attribution reports 0 for every dump whatever a dump does"
    );
}

// =============================================================================================
// MAKING THE COST TRACK THE RELEASE
// =============================================================================================
//
// Everything above prices a dump against a CONSTANT accrual threshold and finds bytes written per
// byte released linear in the store. Two things have to be separated to act on that.
//
// THE WHOLE-INDEX WRITE IS IRREDUCIBLE, and `what_a_dump_writes_is_the_base_index` below counts
// it. The base index is not a second copy of something incremental that could be written instead:
// the incremental form already exists and is the index log, `load_index_inner` already loads base
// + folded deltas, and the base index IS the compaction of that log. A dump that wrote only a
// delta would have compacted nothing, so it would release nothing, and #1920 established that
// nothing bounds the retained log except a completed dump. Writing less per dump is not available.
//
// THE RATIO IS NOT IRREDUCIBLE. It is cost over release, and while the cost is the store, the
// release is whatever the cadence held out for -- a constant number of bytes that has nothing to
// do with the base index a dump must write to free them.
// `effective_index_dump_threshold_bytes` makes the configured value a FLOOR and adds a relative
// term: a threshold dump waits until the accrual reaches
// `base_index / INDEX_DUMP_BASE_FRACTION_DIVISOR`. The ratio is then the divisor, at every size.

/// The accrual floor the cadence arms configure. Small enough that the relative term binds at both
/// corpus sizes, large enough that the FIXED control still fires within a few batches.
const CADENCE_FLOOR_BYTES: u64 = 16 * 1024;
/// Corpus sizes for the cadence arms, exactly 10x apart.
///
/// Smaller than the 20,000 and 200,000 this module's first test uses, and deliberately. The figure
/// the cadence bounds is bytes written per byte RELEASED -- dimensionless -- so what showing it
/// needs is a tenfold RANGE, not a large absolute size, and the absolute cost of a dump at 20,000
/// and 200,000 records is already measured and asserted above. A 200,000-record arm here would add
/// minutes to every run of the suite forever to restate a ratio this pair states.
const CADENCE_SMALL: usize = 5_000;
const CADENCE_LARGE: usize = 50_000;
/// Records written between two cadence checks. Small against the smallest threshold any arm holds
/// out for, so a round overshoots by at most one batch and the released figure is the threshold
/// rather than the batch.
const CADENCE_BATCH: usize = 100;
/// A round that has not fired after this many batches is an apparatus failure, not a zero.
const CADENCE_MAX_BATCHES: usize = 1_000;
/// The FIXED cadence, spelled as the divisor that disables the relative term. An arm that is not
/// exercising the relative term says so by passing this.
const FIXED_CADENCE: u64 = 0;

/// One round of a cadence: the records written until the gate fired, and what the dump it fired
/// cost and released.
#[derive(Debug, Clone)]
struct CadenceRound {
    policy: &'static str,
    corpus_before: usize,
    records_written: usize,
    /// What the production threshold function asked for, given the base index below.
    threshold_bytes: u64,
    /// The base index on disk when the round started -- the relative term's only input.
    base_index_before: u64,
    /// COST, from the kernel, for the dump alone.
    kernel_bytes_written: u64,
    attributed_bytes: u64,
    residual_bytes: i64,
    /// The length the ACTIVE index-log piece ended at -- the folded catalog anchor the dump
    /// appended, as the post-dump sweep left it. The only value the residual is allowed to take
    /// besides zero, and it is read off the tree rather than chosen. See
    /// `assert_round_can_express_it`.
    active_piece_bytes: u64,
    /// RELEASE.
    index_log_released: u64,
    index_log_records_removed: usize,
    /// The index log's high-water mark this round. What the relative term buys is paid here.
    undumped_at_dump: u64,
    pieces_before: usize,
    pieces_after: usize,
    /// The dump's bytes, by the name of the file each landed in.
    named_writes: Vec<(String, u64)>,
}

impl CadenceRound {
    fn written_per_byte_released(&self) -> f64 {
        assert!(
            self.index_log_released > 0,
            "APPARATUS: a {} round at corpus {} released nothing, so it cannot price a release",
            self.policy,
            self.corpus_before
        );
        self.kernel_bytes_written as f64 / self.index_log_released as f64
    }
}

fn base_index_on_disk(dir: &Path) -> u64 {
    std::fs::metadata(
        dir.join("indexes")
            .join(format!("shard-{SHARD}.index.json")),
    )
    .map(|meta| meta.len())
    .unwrap_or(0)
}

/// Write in batches until the PRODUCTION cadence fires, then price the dump it fired.
///
/// `maybe_dump_and_reclaim_with_threshold_and_divisor_for_test` differs from what the embedded
/// proxy's reclaim thread calls once a second only in taking its floor, its interval and its
/// divisor as arguments rather than reading them from the environment of the whole process --
/// the same reason `dump_and_reclaim_index_logs_with_min_reclaimable` takes its threshold.
///
/// The attribution walk is not taken before every check -- on a 50,000-record store that is a
/// directory walk per hundred records. It is taken before the check this test PREDICTS will fire,
/// from the same production function the gate uses, and the prediction is then asserted both ways:
/// a gate that fires when the prediction said it would not, or refuses when it said it would, is
/// an apparatus failure and says so. That assertion is also what stops a mutated threshold from
/// being silently mis-measured instead of caught.
fn run_cadence_round(
    engine: &TemporalEngine,
    dir: &Path,
    policy: &'static str,
    next_key: &mut usize,
    floor_bytes: u64,
    divisor: u64,
) -> CadenceRound {
    let store = engine.index_log_store();
    let corpus_before = *next_key;
    let base_index_before = base_index_on_disk(dir);
    let threshold_bytes = crate::index_log::effective_index_dump_threshold_bytes(
        floor_bytes,
        base_index_before,
        divisor,
    );

    for batch in 0..CADENCE_MAX_BATCHES {
        seed_in_batches_of(engine, *next_key, *next_key + CADENCE_BATCH, CADENCE_BATCH);
        *next_key += CADENCE_BATCH;

        let undumped_at_dump = store.undumped_len_since_dump(SHARD);
        let expect_fire = undumped_at_dump >= threshold_bytes;
        if !expect_fire {
            // Cheap path: no attribution walk. The gate is still ASKED, and must agree.
            let refused = engine
                .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                    SHARD, floor_bytes, 0, 0, divisor,
                )
                .is_none();
            assert!(
                refused,
                "APPARATUS: the {policy} cadence fired at corpus {corpus_before} on \
                 {undumped_at_dump} B accrued, below the {threshold_bytes} B that \
                 `effective_index_dump_threshold_bytes` says it holds out for -- the gate and the \
                 threshold function disagree, so nothing measured here is attributable"
            );
            assert!(
                batch + 1 < CADENCE_MAX_BATCHES,
                "APPARATUS: a {policy} round at corpus {corpus_before} never fired: \
                 {CADENCE_MAX_BATCHES} batches of {CADENCE_BATCH} against a threshold of \
                 {threshold_bytes} B, with {undumped_at_dump} B accrued"
            );
            continue;
        }
        let pieces_before = store.piece_count(SHARD);
        let tree_before = tree_snapshot(dir);
        let wchar_before =
            bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
        let fired = engine.maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
            SHARD,
            floor_bytes,
            0,
            0,
            divisor,
        );
        let wchar_after =
            bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
        let Some(report) = fired else {
            panic!(
                "APPARATUS: the {policy} cadence REFUSED at corpus {corpus_before} with \
                 {undumped_at_dump} B accrued against the {threshold_bytes} B that \
                 `effective_index_dump_threshold_bytes` says is enough -- the gate and the \
                 threshold function disagree"
            );
        };
        let tree_after = tree_snapshot(dir);
        let named_writes = changed_files(&tree_before, &tree_after);
        let attributed_bytes: u64 = named_writes.iter().map(|(_, len)| *len).sum();
        let kernel_bytes_written = wchar_after.saturating_sub(wchar_before);
        return CadenceRound {
            policy,
            corpus_before,
            records_written: *next_key - corpus_before,
            threshold_bytes,
            base_index_before,
            kernel_bytes_written,
            attributed_bytes,
            residual_bytes: kernel_bytes_written as i64 - attributed_bytes as i64,
            active_piece_bytes: named_writes
                .iter()
                .find(|(name, _)| name == &format!("shard-{SHARD}.indexlog.bin"))
                .map(|(_, len)| *len)
                .unwrap_or(0),
            index_log_released: report
                .index_log_bytes_before
                .saturating_sub(report.index_log_bytes_after),
            index_log_records_removed: report.index_log_records_removed,
            undumped_at_dump,
            pieces_before,
            pieces_after: store.piece_count(SHARD),
            named_writes,
        };
    }
    unreachable!("the loop above either returns or asserts");
}

fn show_round(round: &CadenceRound) {
    eprintln!(
        "CADENCE {:8} corpus={:6} wrote={:5} recs | threshold={:9} B (base {:9} B) | COST \
         kernel={:9} attributed={:9} residual={:3} (anchor {:3}) | RELEASE {:9} B in \
         {:4} recs, pieces {:4}->{:2} | written per byte released {:9.3}",
        round.policy,
        round.corpus_before,
        round.records_written,
        round.threshold_bytes,
        round.base_index_before,
        round.kernel_bytes_written,
        round.attributed_bytes,
        round.residual_bytes,
        round.active_piece_bytes,
        round.index_log_released,
        round.index_log_records_removed,
        round.pieces_before,
        round.pieces_after,
        round.written_per_byte_released(),
    );
}

/// Every assertion that says a cadence round can express what is claimed of it.
fn assert_round_can_express_it(round: &CadenceRound) {
    assert!(
        round.pieces_before >= 2,
        "FIXTURE: the {} round at corpus {} ran with the log in {} piece(s), so nothing per-piece \
         is being released",
        round.policy,
        round.corpus_before,
        round.pieces_before
    );
    assert!(
        round.index_log_released > 0,
        "FIXTURE: the {} round at corpus {} released nothing",
        round.policy,
        round.corpus_before
    );
    assert!(
        round.undumped_at_dump >= round.threshold_bytes,
        "FIXTURE: the {} round at corpus {} fired on {} B accrued against a threshold of {} B, so \
         the cadence is not what decided",
        round.policy,
        round.corpus_before,
        round.undumped_at_dump,
        round.threshold_bytes
    );
    assert!(
        round.base_index_before > 0,
        "FIXTURE: the {} round at corpus {} started with no base index on disk, so the relative \
         term had nothing to read and the two cadences are one cadence here",
        round.policy,
        round.corpus_before
    );
    // THE RESIDUAL IS NOT ASSERTED TO BE ZERO. It is asserted to be zero or EXACTLY the length
    // the active index-log piece ended at, and nothing else.
    //
    // The attribution is a FINAL-LENGTH attribution: it names the files the dump touched and takes
    // the length each one ended at. A byte written and then SUPERSEDED inside the same dump is
    // invisible to it, and exactly one such byte-run exists here. Step 3 of
    // `dump_index_catalog_anchored` appends the folded catalog anchor to the active piece; the
    // post-dump sweep then rewrites the log down to a single piece holding that same record. The
    // anchor is written twice and survives once, so the kernel counts 69 bytes the walk cannot.
    //
    // Measured both ways before this was written down: a length-and-mtime walk and an inode-aware
    // walk attribute the identical 319,151 bytes on the round that shows it, so this was never a
    // walk that could not see a file.
    //
    // Allowing a value READ OFF THE TREE rather than a chosen tolerance is what keeps this tight:
    // any other unattributed write, of any size, still fails.
    if round.residual_bytes != 0 && round.residual_bytes as u64 != round.active_piece_bytes {
        eprintln!("UNATTRIBUTED, what the dump wrote by file:");
        for (name, len) in &round.named_writes {
            eprintln!("  {len:10} B  {name}");
        }
    }
    assert!(
        round.residual_bytes == 0 || round.residual_bytes as u64 == round.active_piece_bytes,
        "the {} round at corpus {} wrote {} bytes no file under the store root accounts for \
         (kernel {}, attributed {}), and that is not the {} bytes of catalog anchor the sweep \
         rewrites. Any other unattributed write is a byte this test cannot name.",
        round.policy,
        round.corpus_before,
        round.residual_bytes,
        round.kernel_bytes_written,
        round.attributed_bytes,
        round.active_piece_bytes
    );
}

/// WHAT A DUMP'S BYTES ARE, BY THE NAME OF THE FILE EACH ONE LANDS IN.
///
/// The refutation half of this change, counted so a regression in it is visible. A dump writes
/// the whole served index because the base index IS the compaction of the index log: the
/// incremental form exists already, `load_index_inner` already loads base + folded deltas, and a
/// dump that wrote only a delta would have compacted nothing and released nothing. What this
/// prints is that there is no second, removable term hiding beside it -- the base index is
/// essentially the whole write, so there is nothing to take out.
///
/// rust-internal: attributes the engine's own dump bytes to files, no product behaviour
#[test]
fn what_a_dump_writes_is_the_base_index() {
    const CORPUS: usize = 5_000;
    let _rolling = roll_at(ROLL_BYTES);
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    seed_range(&engine, 0, CORPUS);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");
    let mut next_key = CORPUS;
    let round = run_cadence_round(
        &engine,
        dir.path(),
        "RELATIVE",
        &mut next_key,
        CADENCE_FLOOR_BYTES,
        crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR,
    );
    assert_round_can_express_it(&round);
    show_round(&round);

    eprintln!("WHAT THE DUMP WROTE, by file:");
    for (name, len) in &round.named_writes {
        eprintln!("  {len:10} B  {name}");
    }
    let base_index_after = base_index_on_disk(dir.path());
    assert!(
        base_index_after > 0,
        "APPARATUS: no base index on disk after the dump"
    );
    let share = base_index_after as f64 / round.kernel_bytes_written as f64;
    eprintln!(
        "THE BASE INDEX IS {:.2}% OF THE DUMP ({base_index_after} B of {} B). Everything else the \
         dump writes -- the new active log piece and the block extent manifest -- is bounded by \
         the ROUND, not by the store.",
        share * 100.0,
        round.kernel_bytes_written
    );
    assert!(
        share > 0.90,
        "the base index is only {:.2}% of what the dump writes, so there IS a second term beside \
         it worth attacking and this test's claim that there is nothing to remove is wrong",
        share * 100.0
    );
}

/// WHAT A DUMP COSTS PER BYTE IT RELEASES, UNDER BOTH CADENCES, AT TWO CORPUS SIZES.
///
/// Four rounds per corpus size in ABBA order -- FIXED, RELATIVE, RELATIVE, FIXED -- on ONE store
/// per size, so both cadences are measured against the same fixture and the drift as the store
/// grows through the sequence falls on both rather than on whichever went last.
///
/// SUBJECT: the relative cadence. Bytes written per byte released is the divisor at both sizes.
/// CONTROL: the fixed cadence, which is what ships and what #1928 measured. It is asserted to
/// GROW between the two sizes, with its own failure message, because a control arm that has gone
/// flat is an apparatus that can no longer show the defect -- and would report the subject healthy
/// for the wrong reason. A control arm that fails if it ever goes flat guards a refutation, not a
/// fix; this one is here to keep the fix's flat reading meaning something.
///
/// rust-internal: prices the engine's own dump cadence in bytes, no product behaviour
#[test]
fn a_dump_pays_for_what_it_releases_when_the_cadence_reads_the_store() {
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;

    fn arm(corpus: usize, divisor: u64) -> Vec<CadenceRound> {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = release_engine(dir.path());
        seed_range(&engine, 0, corpus);
        // Prime, so every measured round is a steady-state round. A FIRST dump has no base index
        // on disk to read, which is exactly the case in which the two cadences are one cadence.
        engine
            .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
            .expect("APPARATUS: the priming dump did not complete");
        let mut next_key = corpus;
        let mut rounds = Vec::new();
        for (policy, divisor) in [
            ("FIXED", FIXED_CADENCE),
            ("RELATIVE", divisor),
            ("RELATIVE", divisor),
            ("FIXED", FIXED_CADENCE),
        ] {
            let round = run_cadence_round(
                &engine,
                dir.path(),
                policy,
                &mut next_key,
                CADENCE_FLOOR_BYTES,
                divisor,
            );
            assert_round_can_express_it(&round);
            show_round(&round);
            rounds.push(round);
        }
        rounds
    }

    eprintln!("\n=== CADENCE, corpus {CADENCE_SMALL} ===");
    let small = arm(CADENCE_SMALL, divisor);
    eprintln!("\n=== CADENCE, corpus {CADENCE_LARGE} ===");
    let large = arm(CADENCE_LARGE, divisor);

    let ratios = |rounds: &[CadenceRound], policy: &str| -> Vec<f64> {
        rounds
            .iter()
            .filter(|round| round.policy == policy)
            .map(|round| round.written_per_byte_released())
            .collect()
    };
    let mean = |values: &[f64]| -> f64 {
        assert!(!values.is_empty(), "APPARATUS: no rounds to average");
        values.iter().sum::<f64>() / values.len() as f64
    };

    let small_fixed = mean(&ratios(&small, "FIXED"));
    let large_fixed = mean(&ratios(&large, "FIXED"));
    let small_relative = mean(&ratios(&small, "RELATIVE"));
    let large_relative = mean(&ratios(&large, "RELATIVE"));

    eprintln!(
        "\nWRITTEN PER BYTE RELEASED, corpus {CADENCE_SMALL} -> {CADENCE_LARGE} (10x)\n  \
         FIXED    (control) {small_fixed:10.3} -> {large_fixed:10.3}   ({:.3}x)\n  \
         RELATIVE (subject) {small_relative:10.3} -> {large_relative:10.3}   ({:.3}x), divisor {divisor}",
        large_fixed / small_fixed,
        large_relative / small_relative,
    );

    // CONTROL FIRST, AND IT FAILS IF IT GOES FLAT.
    assert!(
        large_fixed >= small_fixed * 5.0,
        "CONTROL: the FIXED cadence read {small_fixed:.3} at {CADENCE_SMALL} records and \
         {large_fixed:.3} at {CADENCE_LARGE} -- only {:.3}x over a 10x corpus. The control arm has \
         gone flat, so this fixture can no longer show the defect the subject arm claims to fix, \
         and the subject's flat reading below means nothing.",
        large_fixed / small_fixed
    );

    // SUBJECT.
    for (tag, corpus, ratio) in [
        ("small", CADENCE_SMALL, small_relative),
        ("large", CADENCE_LARGE, large_relative),
    ] {
        assert!(
            ratio <= divisor as f64 * 1.40,
            "the RELATIVE cadence wrote {ratio:.3} bytes per byte released at {corpus} records \
             ({tag} arm), above the divisor {divisor} it is supposed to bound. The figure sits \
             structurally a little above the divisor and not at it: the threshold is taken from \
             the base index BEFORE the round, and the base grows by about an eighth during the \
             round it admits, so the cost divided is the larger one"
        );
        assert!(
            ratio >= divisor as f64 * 0.50,
            "the RELATIVE cadence wrote only {ratio:.3} bytes per byte released at {corpus} \
             records ({tag} arm). Far BELOW the divisor is not a better result: it means the round \
             released much more than the threshold it held out for, so the cadence is not what \
             decided and this arm is not measuring one."
        );
    }
    assert!(
        large_relative <= small_relative * 1.20,
        "the RELATIVE cadence is not flat across the corpus: {small_relative:.3} at \
         {CADENCE_SMALL} records against {large_relative:.3} at {CADENCE_LARGE}"
    );
    assert!(
        large_fixed >= large_relative * 3.0,
        "APPARATUS: at {CADENCE_LARGE} records the two cadences read {large_fixed:.3} and \
         {large_relative:.3} -- too close together to be two cadences"
    );
}

/// WHAT THE STORE SERVES AFTER A DUMP THE RELATIVE CADENCE DELAYED, INCLUDING AFTER A RESTART.
///
/// Direction decides this test. A cadence that dumps LESS OFTEN holds more index log between
/// dumps, and holding too much is merely slow; a dump that leaves a reader unable to reconstruct
/// is silent, and is discovered when a restore cannot rebuild. So the strong form is asserted,
/// element by element and never by count, at three points: after the delayed dump, immediately
/// after a restart, and after writing past the restart. The comparison is against a control store
/// that took the same writes and never dumped at all.
///
/// rust-internal: reconstructs the engine's own served index after a delayed dump
#[test]
fn a_store_on_the_relative_cadence_serves_what_it_served_before_across_a_restart() {
    const CORPUS: usize = 6_000;
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;

    fn served_sequence(engine: &TemporalEngine, count: usize) -> Vec<Option<usize>> {
        (0..count)
            .map(|index| {
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet {
                        key: format!("k-{index:08}"),
                    },
                });
                match response.response {
                    CommandResponse::Bytes { value } => value.map(|bytes| bytes.len()),
                    _ => None,
                }
            })
            .collect()
    }

    let control_dir = tempfile::tempdir().expect("tempdir");
    let control = release_engine(control_dir.path());
    let subject_dir = tempfile::tempdir().expect("tempdir");
    let subject = release_engine(subject_dir.path());

    seed_range(&control, 0, CORPUS);
    seed_range(&subject, 0, CORPUS);
    subject
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");

    let mut next_key = CORPUS;
    let round = run_cadence_round(
        &subject,
        subject_dir.path(),
        "RELATIVE",
        &mut next_key,
        CADENCE_FLOOR_BYTES,
        divisor,
    );
    assert_round_can_express_it(&round);
    show_round(&round);
    seed_range(&control, CORPUS, next_key);

    // DENOMINATORS.
    let control_sequence = served_sequence(&control, next_key);
    let live = control_sequence.iter().filter(|slot| slot.is_some()).count();
    assert_eq!(
        live, next_key,
        "APPARATUS: the control served {live} of {next_key} records without dumping at all, so a \
         match below would be two stores agreeing about nothing"
    );
    assert!(
        round.undumped_at_dump > CADENCE_FLOOR_BYTES,
        "FIXTURE: the dump fired on {} B of accrual against a configured floor of {} B -- the \
         relative term delayed nothing here, so this is not a test of a delayed dump",
        round.undumped_at_dump,
        CADENCE_FLOOR_BYTES
    );
    assert!(
        round.index_log_records_removed > 0,
        "FIXTURE: the delayed dump's reclaim removed no index-log records, so nothing below had to \
         survive one"
    );

    fn compare(
        tag: &str,
        observed: &[Option<usize>],
        control: &[Option<usize>],
    ) {
        let first_difference = control
            .iter()
            .zip(observed.iter())
            .position(|(left, right)| left != right);
        assert_eq!(
            first_difference, None,
            "{tag}: the store on the relative cadence first differs from the store that never \
             dumped at record {first_difference:?} -- control {:?} against observed {:?}",
            first_difference.and_then(|at| control.get(at)),
            first_difference.and_then(|at| observed.get(at)),
        );
        assert_eq!(
            control.len(),
            observed.len(),
            "{tag}: the two stores serve different numbers of records"
        );
        assert_eq!(control, observed, "{tag}: the two stores serve different sequences");
    }

    // 1. Live, after the delayed dump and its reclaim.
    compare(
        "after the delayed dump",
        &served_sequence(&subject, next_key),
        &control_sequence,
    );

    // 2. Immediately after a restart: the base index the dump wrote plus the log it left, and
    //    nothing else.
    drop(subject);
    let restarted = release_engine(subject_dir.path());
    compare(
        "immediately after a restart",
        &served_sequence(&restarted, next_key),
        &control_sequence,
    );

    // 3. Written past the restart, then compared again. A reconstruction that is correct only
    //    until the next write is not a reconstruction.
    let after_restart = next_key + 500;
    seed_range(&restarted, next_key, after_restart);
    seed_range(&control, next_key, after_restart);
    compare(
        "after writing past the restart",
        &served_sequence(&restarted, after_restart),
        &served_sequence(&control, after_restart),
    );
}

/// A CADENCE AN OPERATOR DISABLED STAYS DISABLED, HOWEVER LARGE THE BASE INDEX GROWS.
///
/// `should_dump_index_catalog` refuses every undumped length against a zero, `u64::MAX` included:
/// that is how dumps are pinned to compaction and unload only. The relative term is a `max`
/// against a quantity that grows without bound, so getting this wrong does not fail -- it silently
/// switches threshold dumping back ON for exactly the largest stores, which are the ones an
/// operator who disabled it was most likely protecting.
///
/// rust-internal: pins the disabled branch of the engine's dump threshold
#[test]
fn a_dump_cadence_an_operator_disabled_stays_disabled_at_every_base_index_size() {
    use crate::index_log::{effective_index_dump_threshold_bytes, should_dump_index_catalog};

    // PIN THE DIVISOR. Every arm above reads INDEX_DUMP_BASE_FRACTION_DIVISOR for both the
    // treatment and the expectation, so a change to it moves both sides and no assertion there
    // can see it. This is the one place its value is written down. It is not a typo guard: the
    // divisor IS the trade -- bytes written per byte released against index-log footprint -- so
    // changing it is a decision that should have to be made here, with the footprint figure in
    // `what_each_cadence_writes_to_get_the_same_work_through_the_store` re-read beside it.
    assert_eq!(
        crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR,
        8,
        "the shipped divisor moved. It bounds bytes written per byte released AND sets how much \
         index log a store holds between dumps (base/divisor); both figures in this module are \
         stated against 8."
    );

    for base in [0u64, 1, 4_096, 1_048_576, 13_499_438, u64::MAX] {
        assert_eq!(
            effective_index_dump_threshold_bytes(0, base, 8),
            0,
            "a disabled cadence was re-enabled by a base index of {base} bytes"
        );
        assert!(
            !should_dump_index_catalog(u64::MAX, effective_index_dump_threshold_bytes(0, base, 8)),
            "a disabled cadence fired on a base index of {base} bytes"
        );
    }
    // A zero divisor is the fixed cadence and must leave the configured value exactly as found.
    for floor in [1u64, 4_096, 1_048_576] {
        assert_eq!(
            effective_index_dump_threshold_bytes(floor, u64::MAX, 0),
            floor,
            "the fixed cadence moved a configured floor of {floor}"
        );
    }
    // POSITIVE CONTROLS, so none of the above can pass by the function returning a constant.
    assert_eq!(
        effective_index_dump_threshold_bytes(8_192, 1_584_329, 8),
        198_041,
        "an enabled cadence is raised by the base index"
    );
    assert_eq!(
        effective_index_dump_threshold_bytes(1_048_576, 1_584_329, 8),
        1_048_576,
        "the configured value is a FLOOR: a base index too small to reach it must not lower it"
    );

    // Driven through the engine, on a store whose base index is far past every floor above.
    let _rolling = roll_at(ROLL_BYTES);
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    seed_range(&engine, 0, 4_000);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");
    seed_range(&engine, 4_000, 8_000);
    let base_index = base_index_on_disk(dir.path());
    assert!(
        base_index > 8 * CADENCE_FLOOR_BYTES,
        "FIXTURE: the base index is {base_index} B, too small for the relative term to be what \
         decides"
    );
    assert!(
        engine
            .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(SHARD, 0, 0, 0, 8)
            .is_none(),
        "the engine dumped on a cadence an operator had disabled, with a base index of \
         {base_index} bytes"
    );
    // The same store, cadence enabled, DOES dump -- so the refusal above is the zero and not a
    // store that had nothing to do.
    assert!(
        engine
            .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                SHARD,
                CADENCE_FLOOR_BYTES,
                0,
                0,
                8
            )
            .is_some(),
        "APPARATUS: the same store did not dump with the cadence enabled either"
    );
}

/// WHAT EACH CADENCE WRITES TO GET THE SAME WORK THROUGH THE STORE -- THE INTEGRAL.
///
/// A round-for-round comparison flatters the relative cadence, because its rounds are bigger by
/// construction. The fair question is what each writes to put the SAME records into the same
/// store, so all three arms write exactly `WORK` records past an identical seeded corpus, ask the
/// production cadence after every batch, and sum every byte of every dump that fired.
///
/// The third arm is the cadence turned off, which is what "it cannot keep up" looks like when
/// nothing sheds, throttles or signals: the store keeps taking writes at full speed and the index
/// log never comes back. It prices the alternative of dumping less by dumping less OFTEN, taken
/// to its limit.
///
/// rust-internal: sums the engine's own dump bytes across a run, no product behaviour
#[test]
fn what_each_cadence_writes_to_get_the_same_work_through_the_store() {
    /// Large enough that the FIXED arm fires a countable number of times over WORK records rather
    /// than hundreds, small enough that the relative term still binds.
    const INTEGRAL_FLOOR_BYTES: u64 = 64 * 1024;
    const SEEDED: usize = 10_000;
    const WORK: usize = 20_000;
    const BATCH: usize = 250;
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;

    #[derive(Debug)]
    struct Integral {
        policy: &'static str,
        dumps: usize,
        bytes_written: u64,
        bytes_released: u64,
        /// The most index log ever outstanding at once. What the relative term buys is paid here.
        log_high_water: u64,
        base_index_after: u64,
    }

    fn arm(policy: &'static str, floor_bytes: u64, divisor: u64) -> Integral {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = release_engine(dir.path());
        seed_range(&engine, 0, SEEDED);
        engine
            .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
            .expect("APPARATUS: the priming dump did not complete");
        let store = engine.index_log_store();

        let mut dumps = 0usize;
        let mut bytes_written = 0u64;
        let mut bytes_released = 0u64;
        let mut log_high_water = 0u64;
        let mut cursor = SEEDED;
        while cursor < SEEDED + WORK {
            let end = (cursor + BATCH).min(SEEDED + WORK);
            seed_in_batches_of(&engine, cursor, end, BATCH);
            cursor = end;
            log_high_water = log_high_water.max(store.undumped_len_since_dump(SHARD));
            let wchar_before =
                bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
            let fired = engine.maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                SHARD,
                floor_bytes,
                0,
                0,
                divisor,
            );
            let wchar_after =
                bytes_written_now().expect("APPARATUS: /proc/thread-self/io carries no wchar line");
            if let Some(report) = fired {
                dumps += 1;
                bytes_written += wchar_after.saturating_sub(wchar_before);
                bytes_released += report
                    .index_log_bytes_before
                    .saturating_sub(report.index_log_bytes_after);
            }
        }
        Integral {
            policy,
            dumps,
            bytes_written,
            bytes_released,
            log_high_water,
            base_index_after: base_index_on_disk(dir.path()),
        }
    }

    let fixed = arm("FIXED", INTEGRAL_FLOOR_BYTES, FIXED_CADENCE);
    let relative = arm("RELATIVE", INTEGRAL_FLOOR_BYTES, divisor);
    let disabled = arm("DISABLED", 0, divisor);

    for measured in [&fixed, &relative, &disabled] {
        eprintln!(
            "INTEGRAL {:8} {WORK} records past {SEEDED} | dumps={:3} | wrote={:11} B | \
             released={:9} B | log high-water={:9} B | base index after={:9} B | cumulative \
             written per byte released {}",
            measured.policy,
            measured.dumps,
            measured.bytes_written,
            measured.bytes_released,
            measured.log_high_water,
            measured.base_index_after,
            if measured.bytes_released == 0 {
                "undefined -- nothing was released".to_string()
            } else {
                format!(
                    "{:.3}",
                    measured.bytes_written as f64 / measured.bytes_released as f64
                )
            },
        );
    }

    // DENOMINATORS.
    for measured in [&fixed, &relative, &disabled] {
        assert!(
            measured.base_index_after > 0,
            "APPARATUS: the {} arm left no base index",
            measured.policy
        );
    }
    assert!(
        fixed.dumps > relative.dumps,
        "APPARATUS: the fixed cadence fired {} times and the relative one {} -- the relative term \
         did not change the cadence on this fixture, so the two arms are one arm",
        fixed.dumps,
        relative.dumps
    );
    assert!(
        relative.dumps > 0,
        "APPARATUS: the relative arm never dumped, so it released nothing to price"
    );
    assert_eq!(
        disabled.dumps, 0,
        "APPARATUS: the disabled arm dumped {} times",
        disabled.dumps
    );

    let fixed_ratio = fixed.bytes_written as f64 / fixed.bytes_released as f64;
    let relative_ratio = relative.bytes_written as f64 / relative.bytes_released as f64;
    eprintln!(
        "\nOVER THE SAME {WORK} RECORDS: fixed wrote {} B in {} dumps, relative wrote {} B in {} \
         dumps -- {:.2}x less, for a log that stood at {} B instead of {} B. ROUNDS RUN: {} and \
         {}.",
        fixed.bytes_written,
        fixed.dumps,
        relative.bytes_written,
        relative.dumps,
        fixed.bytes_written as f64 / relative.bytes_written as f64,
        relative.log_high_water,
        fixed.log_high_water,
        fixed.dumps,
        relative.dumps,
    );

    assert!(
        relative.bytes_written < fixed.bytes_written,
        "the relative cadence wrote {} B over {WORK} records against the fixed cadence's {} B",
        relative.bytes_written,
        fixed.bytes_written
    );
    assert!(
        relative_ratio < fixed_ratio,
        "cumulative written per byte released did not improve: fixed {fixed_ratio:.3}, relative \
         {relative_ratio:.3}"
    );
    assert!(
        relative_ratio <= divisor as f64 * 1.30,
        "the relative cadence's cumulative figure {relative_ratio:.3} is above the divisor \
         {divisor} it is supposed to bound"
    );

    // THE PRICE, ASSERTED SO IT CANNOT GROW UNNOTICED.
    assert!(
        relative.log_high_water > fixed.log_high_water,
        "APPARATUS: the relative cadence held no more log than the fixed one ({} B against {} B), \
         so it did not delay a dump",
        relative.log_high_water,
        fixed.log_high_water
    );
    assert!(
        relative.log_high_water
            <= relative.base_index_after / divisor + INTEGRAL_FLOOR_BYTES + 128 * 1024,
        "the relative cadence let the index log reach {} B against a base index of {} B -- more \
         than the base/{divisor} it is supposed to hold it to",
        relative.log_high_water,
        relative.base_index_after
    );

    // WHAT IT LOOKS LIKE WHEN IT CANNOT KEEP UP.
    assert_eq!(
        disabled.bytes_released, 0,
        "APPARATUS: the disabled arm released {} B",
        disabled.bytes_released
    );
    assert!(
        disabled.log_high_water > relative.log_high_water,
        "APPARATUS: the disabled arm held {} B of log, no more than the relative arm's {} B",
        disabled.log_high_water,
        relative.log_high_water
    );
}

/// EVERY PRODUCTION CADENCE CHECK TAKES ITS THRESHOLD FROM THE ONE FUNCTION THAT READS THE STORE.
///
/// Every arm above reaches the cadence through the argument-taking test variant, so a change that
/// removed the relative term from the PRODUCTION callers alone -- leaving the shared function
/// intact -- would leave all of them passing. This reads  instead and
/// requires that the deployments

/// EVERY PRODUCTION CADENCE CHECK TAKES ITS THRESHOLD FROM THE ONE FUNCTION THAT READS THE STORE.
///
/// Every arm above reaches the cadence through the argument-taking test variant, so a change that
/// removed the relative term from the PRODUCTION callers alone -- leaving the shared function
/// intact -- would leave all of them passing. This reads `engine/persistence.rs` instead and
/// requires that the deployment's configured value is read in exactly ONE place, that both
/// production cadence checks take their threshold from it, and that the checks it counts are all
/// of them.
///
/// A source-text guard, because what is guarded is which expression a call site was written with.
/// It is not observable from any behaviour those two callers have that a test can drive without an
/// 8 MiB base index and a write to the environment of the whole process.
///
/// rust-internal: pins how the engine's own dump cadence obtains its threshold
#[test]
fn every_production_dump_cadence_check_reads_the_store_for_its_threshold() {
    const PERSISTENCE: &str = include_str!("../persistence.rs");

    // DENOMINATOR FIRST. If this file stops holding cadence checks, everything below passes by
    // having nothing to check.
    let checks = PERSISTENCE.matches("should_dump_index_catalog_now(").count();
    assert!(
        checks >= 4,
        "VACUITY: engine/persistence.rs holds {checks} cadence checks, fewer than the 4 this guard \
         was written against -- it is no longer reading what it thinks it is"
    );

    let configured_reads = PERSISTENCE
        .matches("crate::storage_config::index_dump_wal_gap_bytes()")
        .count();
    assert_eq!(
        configured_reads, 1,
        "the deployment's configured accrual floor is read in {configured_reads} places in \
         engine/persistence.rs. It must be read in exactly one -- `index_dump_threshold_bytes`, \
         which is where that floor is raised to keep a dump's cost in proportion to what it \
         releases. A second reader is a production cadence check that skipped the relative term."
    );

    let through_the_helper = PERSISTENCE
        .matches("self.index_dump_threshold_bytes(shard_id)")
        .count();
    assert_eq!(
        through_the_helper, 2,
        "{through_the_helper} production cadence checks take their threshold from \
         `index_dump_threshold_bytes`, not the 2 there are (`maybe_dump_index_catalog` and \
         `maybe_dump_and_reclaim_index_logs`)"
    );

    // And the helper applies the relative term rather than being a renamed passthrough.
    assert!(
        PERSISTENCE.contains("crate::index_log::effective_index_dump_threshold_bytes(")
            && PERSISTENCE.contains("crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR"),
        "engine/persistence.rs no longer applies the relative term at all"
    );
}
