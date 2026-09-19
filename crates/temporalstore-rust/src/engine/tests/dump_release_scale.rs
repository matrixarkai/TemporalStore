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
//! THE WRITE-AHEAD LOG HALF RELEASES NOTHING, AND SAYS SO IN THE SAME WORDS AS SUCCESS. At both
//! corpus sizes the dump's WAL reclaim reports `0 -> 0 bytes, 0 records` against a log holding
//! 48 records in 524,288 bytes at 20,000 and 408 records in 3,145,728 bytes at 200,000. The
//! control arm, a log written one command at a time, releases 261,933 bytes in 1,199 records off
//! the same call. Underneath the zero, `gc_before_sequence` returns
//! `Corruption("binary record is incomplete")`, and `engine/persistence.rs` takes it with `.ok()`
//! and then `.unwrap_or_default()`, so an errored reclaim and a reclaim with nothing to do
//! produce a byte-identical `CatalogDumpReclaimReport`. The embedded proxy's reclaim loop is the
//! only thing keeping that store's logs bounded and it prints exactly that report. The
//! single-command control arm reclaims the same log normally at every size tried, so a non-zero
//! WAL release is something this fixture can express. THE TRIGGER IS NOT CLAIMED HERE: it is not
//! record count alone (three batch records reclaim, four do not, but two written as 500+100 also
//! do not), not total bytes, and not a newline in the payload -- all three were driven and none
//! of them decides it.
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
        assert_eq!(
            small.barriers, large.barriers,
            "FLAT: a dump took {} durability barriers at {} records and {} at {} ({regime} regime)",
            small.barriers, small.corpus, large.barriers, large.corpus
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
    assert_eq!(
        (batched.wal_released, batched.wal_records_removed),
        (0, 0),
        "SUBJECT: the dump reported releasing {} B in {} records from a log holding {} records in \
         {} bytes. If this now reports a release, the reclaim underneath has started working and \
         this module's header needs rewriting rather than this assertion loosening",
        batched.wal_released,
        batched.wal_records_removed,
        batched.wal_records_before,
        batched.wal_bytes_on_disk_before
    );

    // THE ONE THAT DOES NOT DEPEND ON KNOWING WHY. A store holding a log and a store holding none
    // hand back the same three numbers, because every field is read through `unwrap_or_default()`
    // after the sweep's `Result` was dropped with `.ok()`.
    assert_eq!(
        empty_wal_records, 0,
        "APPARATUS: the empty arm's log held {empty_wal_records} records, so it is not the \
         nothing-to-do case this comparison needs"
    );
    assert_eq!(
        (
            batched.wal_released,
            batched.wal_records_removed,
            batched.wal_bytes_on_disk_before > 0
        ),
        (
            empty_report
                .wal_bytes_before
                .saturating_sub(empty_report.wal_bytes_after),
            empty_report.wal_records_removed,
            true
        ),
        "a store holding {} records in {} bytes and a store holding no log at all report the same \
         release. An errored sweep and a sweep with nothing to do are the same \
         `CatalogDumpReclaimReport`, so nothing downstream -- the proxy's reclaim loop included -- \
         can tell them apart",
        batched.wal_records_before,
        batched.wal_bytes_on_disk_before
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
