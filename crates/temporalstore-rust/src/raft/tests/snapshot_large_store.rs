// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one snapshot costs once the store is LARGE, and what that cost is priced by.
//!
//! `snapshot_cost` establishes the shape of a snapshot at 1,000 and 4,000 records: one walk, one
//! encode, one read per slab, and -- since #1914 -- nothing under the cluster write guard. Every
//! one of those is a statement about MULTIPLICITY, and multiplicity is exactly the part of a cost
//! that a small store can tell you about. What a small store cannot tell you is what the cost is
//! priced BY, because on a fresh store of N records the first snapshot discards N log entries and
//! reads N records of store, and those two numbers are the same number.
//!
//! This file separates them. A snapshot is triggered by the LOG -- `max_applied_log_bytes` of
//! applied entries have piled up and want discarding -- and what it reads is the STORE. Those
//! coincide once, on the first snapshot of a fresh store, and never again. So every quantity here
//! is divided by two denominators and both are printed:
//!
//!   * per record of STORE, which is the denominator a whole-store cost hides inside, and
//!   * per record of WORK, which is the log this particular snapshot was triggered to discard.
//!
//! Measured on a 3-node in-process cluster, 128-byte values, `STORE_SMALL` and `STORE_LARGE`
//! records of store, in two regimes -- WHOLE, the first snapshot on a fresh store, and INCREMENT,
//! a second snapshot taken after `INCREMENT_RECORDS` further records, which is the routine case a
//! deployed node spends its life in:
//!
//! ```text
//!                                       WHOLE                     INCREMENT
//!                               25,000      100,000       25,000        100,000
//!   log entries discarded       25,000      100,000            1              1
//!   block addresses walked      25,000      100,000       25,001        100,001
//!   slab bytes read          3,500,000   14,000,000    3,500,140     14,000,140
//!     per record of STORE           140          140          140            140
//!     per record of WORK            140          140    3,500,140     14,000,140
//!   slab reads                       4            4            4              4
//!   served-index bytes       1,651,207    6,660,688    1,519,149      6,125,409
//!   whole image bytes        5,151,207   20,660,688    5,019,289     20,125,549
//!   state images built               1            1            1              1
//!   engine rebuilds                  3            3            3              3
//!   ... under the cluster write guard
//!   engine rebuilds                  0            0            0              0
//!   slab bytes re-installed          0            0            0              0
//!   image bytes copied               0            0            0              0
//! ```
//!
//! Every row above is an exact identity except two: the served-index and whole-image bytes in the
//! INCREMENT arm. Those are the index of a REBUILT engine -- the first snapshot replaced the
//! leader's engine with one reconstructed from its own image -- and they move by about 0.02%
//! between runs of the same code, so nothing asserts them and they are here as scale only. Every
//! figure this file does assert is an identity against something the snapshot path did not
//! compute.
//!
//! The left half is flat under both denominators and says nothing is wrong. The right half is the
//! same store, the same code and one entry of work, and the two denominators disagree by the size
//! of the store. That disagreement is the finding: a snapshot does not read the log it is about to
//! discard, it reads everything the shard holds, and it does that whether the log behind it is one
//! entry or all of them.
//!
//! COUNTED, never timed, for the reason `snapshot_cost` gives: this box varies about 2.4x in wall
//! time across a day and these counters do not move with load at all.
//!
//! Three things follow, and each has a test below.
//!
//! IS A SNAPSHOT BOUNDED IN WHAT IT READS? No. `max_applied_log_bytes` is the only bound on this
//! path and it decides WHETHER a snapshot happens, not how much of the store it touches; once it
//! fires, `build_state_image` reads every slab in the shard's live set and every address the
//! served index holds. It does all the work it finds. `the_snapshot_bound_decides_whether_it_runs_
//! not_how_much_it_reads` holds the store still and moves the bound, and the bytes do not move.
//!
//! WHAT DOES THE TOTAL COME TO? A flat per-snapshot cost over a store that grows between snapshots
//! is a quadratic total. At a fixed cadence of one snapshot per `step` records, reaching N records
//! costs `140 * step * R * (R + 1) / 2` slab bytes read across `R = N / step` snapshots, which
//! `what_a_store_pays_in_total_to_reach_a_size_grows_with_the_square_of_it` asserts as an exact
//! identity rather than as a trend.
//!
//! DOES A PEER COST MORE TO HELP THE FURTHER BEHIND IT IS? No -- and this is a refutation, stated
//! as one. Nothing on this path reads the receiving node at all except to ask whether it may take
//! the snapshot, so the bytes are identical at any lag, and the cost PER ENTRY of lag therefore
//! FALLS as the peer falls further behind. The expensive peer to help is the one that is barely
//! behind. `a_further_behind_peer_costs_no_more_to_help_than_a_barely_behind_one` measures both.

use super::snapshot_cost::{
    assert_the_fixture_is_populated, cluster_with, force_the_threshold, print_counts,
};
use super::*;
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records of STORE, at the two large sizes. A 4.00x step, as `snapshot_cost` uses, so a per-
/// record identity is worth three significant figures and a ratio is unambiguous.
///
/// These are the sizes the whole file runs at, and they are what makes it a large-store
/// measurement rather than a repeat of `snapshot_cost`: the shape this file is looking for -- a
/// cost priced by the store rather than by the work -- is arithmetically present at 1,000 records
/// too, but at 1,000 records it is 1,000x and at 200,000 records it is 200,000x, and only one of
/// those is a thing anyone would notice on a live cluster.
const STORE_SMALL: usize = 25_000;
const STORE_LARGE: usize = 100_000;

/// Records applied between the first snapshot and the second, in the INCREMENT regime.
///
/// ONE. The point of the regime is the smallest backlog a snapshot can legally be triggered on,
/// because the question is whether the path can tell a small backlog from a large one, and one
/// entry against the whole store is the sharpest form of the answer.
const INCREMENT_RECORDS: usize = 1;

/// Slab bytes a record of the fixture corpus occupies, asserted rather than assumed at every size.
/// `snapshot_cost` measures the same 140 at 1,000 and 4,000.
const SLAB_BYTES_PER_RECORD: u64 = 140;

/// Which regime a row was measured in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Regime {
    /// The first snapshot on a fresh store. The log it discards IS the store, so the two
    /// denominators coincide and every per-record figure is flat. This is the control.
    Whole,
    /// A second snapshot, taken after `INCREMENT_RECORDS` further records. The log it discards is
    /// `INCREMENT_RECORDS` entries and the store is everything. This is the subject.
    Increment,
}

impl Regime {
    fn label(self) -> &'static str {
        match self {
            Regime::Whole => "WHOLE",
            Regime::Increment => "INCREMENT",
        }
    }
}

/// One measurement: the counters, and the two denominators they are divided by.
#[derive(Debug, Clone, Copy)]
struct Row {
    regime: Regime,
    store_records: u64,
    /// Log entries this snapshot discarded -- `applied_index - last_snapshot_index`, read off the
    /// trigger report rather than inferred. This is the WORK.
    work_entries: u64,
    counts: SnapshotCounts,
    image_payload_bytes: u64,
    image_slabs: u64,
    image_index_bytes: u64,
}

impl Row {
    fn per_record_of_store(self, quantity: u64) -> f64 {
        quantity as f64 / self.store_records.max(1) as f64
    }

    fn per_record_of_work(self, quantity: u64) -> f64 {
        quantity as f64 / self.work_entries.max(1) as f64
    }

    fn print(&self) {
        println!(
            "  {:9} store={:>7} work={:>7} addresses={:>7} slab_reads={:>3} slab_bytes={:>10} \
             index_bytes={:>9} image_bytes={:>10} slabs={:>3} rebuilds={} publishes={} \
             image_builds={}",
            self.regime.label(),
            self.store_records,
            self.work_entries,
            self.counts.live_slab_scan_addresses,
            self.counts.slab_reads,
            self.counts.slab_read_bytes,
            self.image_index_bytes,
            self.image_payload_bytes,
            self.image_slabs,
            self.counts.engine_rebuilds,
            self.counts.engine_publishes,
            self.counts.image_builds,
        );
        println!(
            "            slab bytes per record of STORE = {:>12.3}   per record of WORK = {:>12.3}",
            self.per_record_of_store(self.counts.slab_read_bytes),
            self.per_record_of_work(self.counts.slab_read_bytes),
        );
        println!(
            "            addresses  per record of STORE = {:>12.3}   per record of WORK = {:>12.3}",
            self.per_record_of_store(self.counts.live_slab_scan_addresses),
            self.per_record_of_work(self.counts.live_slab_scan_addresses),
        );
    }
}

/// Append `count` further records to a cluster that already holds `from` of them, keeping the key
/// shape -- and therefore the key LENGTH -- identical to `cluster_with`'s.
///
/// Key length is held constant on purpose across every arm in this file. A record's slab bytes
/// include its key, so an arm that widened the key by one character would move the slab-bytes
/// rows without anything on the snapshot path having changed.
fn append_records(cluster: &RaftCluster, from: usize, count: usize) {
    for index in from..from + count {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
    }
}

/// The log entries a trigger report says this snapshot discarded.
fn work_entries(report: &RaftSnapshotTriggerReport) -> u64 {
    assert!(
        report.triggered,
        "the snapshot did not fire ({}), so there is no work to divide by and every per-work \
         figure below would be a division of a real cost by a fixture that never ran",
        report.reason
    );
    report
        .applied_index
        .checked_sub(report.last_snapshot_index)
        .expect("applied_index must be at or above the last snapshot index when one fires")
}

/// What the leader's engine currently holds, measured independently of the snapshot path: the
/// shard's live slab set straight off the engine.
fn live_slabs(cluster: &RaftCluster) -> Vec<u64> {
    let mut live = cluster
        .node_engine_for_test(1)
        .expect("the leader serves an engine")
        .live_block_slab_ids(1);
    live.sort_unstable();
    live
}

/// Run ONE `maybe_trigger_snapshot` with the counters reset around it, and return the row.
fn measure_one_snapshot(cluster: &RaftCluster, regime: Regime, store_records: usize) -> Row {
    snapshot_probe::reset();
    let report = cluster
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    let counts = snapshot_probe::counts();
    let work = work_entries(&report);

    // The image the snapshot published, read back off the node it was published into, so the
    // payload figures come from the structure rather than from the counters that measured it.
    let installed = cluster
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .get(&1)
        .and_then(|node| node.installed_snapshot.clone())
        .expect("a snapshot that triggered must leave an installed snapshot on the leader");
    let image = installed
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");

    let row = Row {
        regime,
        store_records: store_records as u64,
        work_entries: work,
        counts,
        image_payload_bytes: image.payload_bytes() as u64,
        image_slabs: image.slabs.len() as u64,
        image_index_bytes: image.index_bytes.len() as u64,
    };
    row.print();
    row
}

/// A snapshot reads the whole store however little log it was triggered to discard.
///
/// TWO REGIMES, each at both sizes. WHOLE is the CONTROL ARM: the first snapshot on a fresh store,
/// where the log discarded and the store read are the same records, so every per-record figure is
/// flat under BOTH denominators and the instrument is shown reading a genuinely proportional cost
/// as proportional. INCREMENT is the SUBJECT: the same store, one entry of work.
///
/// The subject's assertion is deliberately written so that it FAILS IF THE COST EVER GOES FLAT.
/// That is not a mistake. What is recorded here is a refutation of the idea that this path is
/// priced by its work, and a bounded read would refute the recording -- so the day someone bounds
/// it, this test must go red and be re-derived rather than quietly keep passing on `0 == 0`.
#[test]
fn a_snapshot_reads_the_whole_store_however_little_log_it_discards() {
    let mut whole = Vec::new();
    let mut increment = Vec::new();

    for store_records in [STORE_SMALL, STORE_LARGE] {
        println!("=== store={store_records} ===");
        let cluster = cluster_with(store_records);
        let fixture = assert_the_fixture_is_populated(&cluster, store_records);
        println!("  live slab ids before = {:?}", live_slabs(&cluster));
        force_the_threshold(&cluster);

        // REGIME WHOLE -- the control. First snapshot on a fresh store.
        let row = measure_one_snapshot(&cluster, Regime::Whole, store_records);
        assert_eq!(
            row.work_entries, store_records as u64,
            "the first snapshot on a fresh store must discard one log entry per record, which is \
             what makes this the arm where the two denominators coincide: {} entries for \
             {store_records} records",
            row.work_entries
        );

        // REGIME INCREMENT -- the subject. One more record, then snapshot again.
        append_records(&cluster, store_records, INCREMENT_RECORDS);
        let after = store_records + INCREMENT_RECORDS;
        let live_after = live_slabs(&cluster);
        println!("  live slab ids after  = {live_after:?}");
        assert!(
            live_after.len() > 1,
            "the shard's live slab set after the first install is {live_after:?}: with one slab a \
             per-slab cost and a per-snapshot cost are the same number, and the rebuilt engine \
             would not be expressing what the fixture was built to express"
        );
        let row_increment = measure_one_snapshot(&cluster, Regime::Increment, after);
        assert_eq!(
            row_increment.work_entries, INCREMENT_RECORDS as u64,
            "the second snapshot must discard exactly the {INCREMENT_RECORDS} entr(y/ies) applied \
             since the first: {} discarded, so this arm is not measuring a small backlog at all",
            row_increment.work_entries
        );

        println!("  fixture payload bytes = {}", fixture.payload_bytes);
        whole.push(row);
        increment.push(row_increment);
    }

    let (whole_small, whole_large) = (whole[0], whole[1]);
    let (inc_small, inc_large) = (increment[0], increment[1]);

    // ---- EXACT IDENTITIES, at both sizes and in both regimes ----
    //
    // The walk visits one block address per record of STORE. Asserted as an identity against
    // something the snapshot path did not compute -- the record count the fixture was built with
    // -- rather than as a tolerance, because a tolerance is also satisfied by a walk that has
    // stopped happening.
    for row in [whole_small, whole_large, inc_small, inc_large] {
        assert_eq!(
            row.counts.live_slab_scan_addresses, row.store_records,
            "{} at store={}: the live-slab walk must visit exactly one block address per record \
             of STORE, and visited {}",
            row.regime.label(),
            row.store_records,
            row.counts.live_slab_scan_addresses
        );
        assert_eq!(
            row.counts.slab_read_bytes,
            SLAB_BYTES_PER_RECORD * row.store_records,
            "{} at store={}: slab bytes read must be exactly {SLAB_BYTES_PER_RECORD} per record \
             of STORE, and were {}",
            row.regime.label(),
            row.store_records,
            row.counts.slab_read_bytes
        );
        assert_eq!(
            row.counts.image_builds, 1,
            "{} at store={}: one snapshot must build exactly one state image. More than one means \
             the three-attempt loop in create_state_image_snapshot RETRIED, which #1912 measured \
             as never happening anywhere in the tree at 1,000 and 4,000 records; a retry here \
             would be a large-store behaviour that small stores cannot show",
            row.regime.label(),
            row.store_records,
        );
        assert_eq!(
            row.counts.slab_reads, row.image_slabs,
            "{} at store={}: the build must read each carried slab exactly once -- {} reads for \
             {} slabs. It reads every slab it finds and stops at no bound",
            row.regime.label(),
            row.store_records,
            row.counts.slab_reads,
            row.image_slabs
        );
        assert_eq!(
            row.counts.engine_rebuilds_under_guard, 0,
            "{} at store={}: #1914's result must hold at this size -- no engine may be rebuilt \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.engine_rebuilds_under_guard
        );
        assert_eq!(
            row.counts.slab_install_bytes_under_guard, 0,
            "{} at store={}: #1914's result must hold at this size -- no slab byte may be written \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.slab_install_bytes_under_guard
        );
        assert_eq!(
            row.counts.image_clone_bytes_under_guard, 0,
            "{} at store={}: #1912's result must hold at this size -- no image byte may be copied \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.image_clone_bytes_under_guard
        );
        assert!(
            row.counts.engine_rebuilds > 0 && row.counts.engine_publishes > 0,
            "{} at store={}: {} rebuilds and {} publishes. A snapshot that installed into nobody \
             would satisfy every under-guard assertion above by doing nothing at all",
            row.regime.label(),
            row.store_records,
            row.counts.engine_rebuilds,
            row.counts.engine_publishes
        );
    }

    // ---- THE CONTROL ARM: WHOLE ----
    //
    // Named, and with its own failure message. Per record of WORK this arm is flat, because in it
    // the work IS the store. If this arm ever stops being flat, the instrument is reading
    // something other than what it claims to and NOTHING below it can be believed.
    let whole_small_per_work = whole_small.per_record_of_work(whole_small.counts.slab_read_bytes);
    let whole_large_per_work = whole_large.per_record_of_work(whole_large.counts.slab_read_bytes);
    println!("=== CONTROL ARM: WHOLE ===");
    println!(
        "  slab bytes per record of WORK = {whole_small_per_work:.3} -> {whole_large_per_work:.3}"
    );
    assert_eq!(
        whole_small.counts.slab_read_bytes * whole_large.work_entries,
        whole_large.counts.slab_read_bytes * whole_small.work_entries,
        "CONTROL ARM WHOLE is not flat per record of WORK: {whole_small_per_work} at store={} \
         against {whole_large_per_work} at store={}. In this regime the log discarded and the \
         store read are the same records, so this ratio is an identity; if it moves, the \
         denominator is wrong and the SUBJECT arm below cannot be interpreted",
        whole_small.store_records,
        whole_large.store_records,
    );

    // ---- THE SUBJECT ARM: INCREMENT ----
    let inc_small_per_store = inc_small.per_record_of_store(inc_small.counts.slab_read_bytes);
    let inc_large_per_store = inc_large.per_record_of_store(inc_large.counts.slab_read_bytes);
    let inc_small_per_work = inc_small.per_record_of_work(inc_small.counts.slab_read_bytes);
    let inc_large_per_work = inc_large.per_record_of_work(inc_large.counts.slab_read_bytes);
    let store_ratio = inc_large.store_records as f64 / inc_small.store_records as f64;
    println!("=== SUBJECT ARM: INCREMENT ===");
    println!("  store ratio                    = {store_ratio:.3}");
    println!(
        "  work entries                   = {} -> {}",
        inc_small.work_entries, inc_large.work_entries
    );
    println!(
        "  slab bytes read                = {} -> {}  (ratio {:.3})",
        inc_small.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes as f64 / inc_small.counts.slab_read_bytes.max(1) as f64
    );
    println!(
        "  per record of STORE            = {inc_small_per_store:.3} -> {inc_large_per_store:.3}"
    );
    println!(
        "  per record of WORK             = {inc_small_per_work:.3} -> {inc_large_per_work:.3}"
    );

    assert!(
        inc_small.counts.slab_read_bytes > 0 && inc_large.counts.slab_read_bytes > 0,
        "slab bytes read is zero at one of the two sizes ({} and {}), so every ratio above is \
         vacuous",
        inc_small.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes
    );

    // Per record of STORE, the subject is FLAT -- which is precisely why this cost is invisible
    // to a per-record-of-store denominator, and why it took a large store to make the point.
    assert_eq!(
        inc_small.counts.slab_read_bytes * inc_large.store_records,
        inc_large.counts.slab_read_bytes * inc_small.store_records,
        "per record of STORE the INCREMENT arm must be flat -- {inc_small_per_store} against \
         {inc_large_per_store}. That flatness is the finding's camouflage, not its refutation"
    );

    // Per record of WORK, with the work held at ONE entry in both arms, it grows by exactly the
    // store ratio. THIS IS THE FINDING.
    //
    // It is asserted as a strict inequality as well as an identity: the identity says the growth
    // is exactly proportional to the store, and the inequality is what goes red the day someone
    // BOUNDS the read. A flat reading here does not mean this test passed; it means the tree has
    // stopped doing what this file records, and the recording must be redone.
    assert!(
        inc_large.counts.slab_read_bytes > inc_small.counts.slab_read_bytes,
        "SUBJECT ARM INCREMENT HAS GONE FLAT: {} slab bytes read at store={} and {} at store={}, \
         for one entry of work in both. This file exists to record that a snapshot's read is \
         priced by the store rather than by the log it discards; a flat reading refutes that \
         recording. Do not relax this assertion -- re-derive what the path now reads and rewrite \
         the table in this module's doc comment",
        inc_small.counts.slab_read_bytes,
        inc_small.store_records,
        inc_large.counts.slab_read_bytes,
        inc_large.store_records,
    );
    assert_eq!(
        inc_small.counts.slab_read_bytes * inc_large.store_records,
        inc_large.counts.slab_read_bytes * inc_small.store_records,
        "for one entry of work in both arms, slab bytes read must grow by exactly the STORE \
         ratio: {} at store={} against {} at store={}",
        inc_small.counts.slab_read_bytes,
        inc_small.store_records,
        inc_large.counts.slab_read_bytes,
        inc_large.store_records,
    );

    // And the same statement without any division at all: at the larger size, one entry of work
    // costs more bytes than the ENTIRE smaller store's first snapshot did.
    assert!(
        inc_large.counts.slab_read_bytes > whole_small.counts.slab_read_bytes,
        "one entry of work on a {}-record store read {} slab bytes, which should exceed the {} \
         read by the first snapshot of the whole {}-record store",
        inc_large.store_records,
        inc_large.counts.slab_read_bytes,
        whole_small.counts.slab_read_bytes,
        whole_small.store_records,
    );
}

/// The one bound on this path decides WHETHER a snapshot runs, not how much of the store it reads.
///
/// `max_applied_log_bytes` is the whole of the snapshot path's admission control. The store is
/// held still here and the bound is moved across the range in which it still fires; if the bound
/// limited the READ, the bytes would move with it. They do not move at all.
#[test]
fn the_snapshot_bound_decides_whether_it_runs_not_how_much_it_reads() {
    const RECORDS: usize = 10_000;
    let mut rows = Vec::new();

    for bound in [1u64, 4_096, 65_536] {
        let cluster = cluster_with(RECORDS);
        assert_the_fixture_is_populated(&cluster, RECORDS);
        {
            let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
            inner.config.max_applied_log_bytes = bound;
            inner.config.max_retained_log_bytes = 0;
        }
        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        let counts = snapshot_probe::counts();
        println!(
            "  bound={bound:>7}  triggered={}  reason={}  applied_log_bytes={}  \
             addresses={}  slab_reads={}  slab_bytes={}",
            report.triggered,
            report.reason,
            report.applied_log_bytes,
            counts.live_slab_scan_addresses,
            counts.slab_reads,
            counts.slab_read_bytes
        );
        assert!(
            report.triggered,
            "bound={bound} did not fire ({}), so this arm compares a snapshot against no snapshot \
             rather than one bound against another",
            report.reason
        );
        rows.push((bound, counts));
    }

    let first = rows[0].1;
    assert!(
        first.slab_read_bytes > 0 && first.live_slab_scan_addresses > 0,
        "the first arm read {} bytes over {} addresses: with zero, every equality below holds \
         vacuously",
        first.slab_read_bytes,
        first.live_slab_scan_addresses
    );
    assert_eq!(
        first.live_slab_scan_addresses, RECORDS as u64,
        "the walk must visit one address per record of store: {} for {RECORDS}",
        first.live_slab_scan_addresses
    );
    for (bound, counts) in &rows[1..] {
        assert_eq!(
            counts.slab_read_bytes, first.slab_read_bytes,
            "bound={bound} read {} slab bytes against {} at bound={}. The bound is admission \
             control, not a read limit; if these ever differ, this path has acquired a read bound \
             and the rest of this module must be re-derived",
            counts.slab_read_bytes, first.slab_read_bytes, rows[0].0
        );
        assert_eq!(
            counts.live_slab_scan_addresses, first.live_slab_scan_addresses,
            "bound={bound} walked {} addresses against {} at bound={}",
            counts.live_slab_scan_addresses, first.live_slab_scan_addresses, rows[0].0
        );
        assert_eq!(
            counts.slab_reads, first.slab_reads,
            "bound={bound} read {} slabs against {} at bound={}: a snapshot reads every slab it \
             finds",
            counts.slab_reads, first.slab_reads, rows[0].0
        );
    }
}

/// What the walk visits, and what the bytes it reads add up to, each recovered from a PLANTED
/// marker so that neither zero can be an identity dressed as a result.
///
/// TWO instruments, deliberately independent of each other:
///
///   * the VISIT count, bumped inside the live-slab walk, against the record count the fixture
///     was built with -- a number the snapshot path never computes. `MARKERS` extra records go in
///     under a DISJOINT key range of the SAME LENGTH, and the walk must report `RECORDS + MARKERS`
///     exactly. Same length because a record's slab bytes carry its key, so a wider marker key
///     would move the byte row without anything on the snapshot path changing.
///
///   * the BYTE residual: slab bytes as counted inside `BlockStore::read_slab`, against the slab
///     bytes that ended up in the image, read off the returned structure. Those are two different
///     things -- a slab read and discarded, or read twice, would separate them -- so their
///     difference is a real residual and not `x - x`. It reads ZERO, which is exactly the reading
///     the campaign says to distrust, so a marker is planted for it too: one extra `read_slab`
///     inside the measured span, whose bytes the residual must then recover EXACTLY.
#[test]
fn the_snapshot_walk_and_its_byte_residual_each_recover_a_planted_marker() {
    const RECORDS: usize = 10_000;
    const MARKERS: usize = 37;

    let cluster = cluster_with(RECORDS);
    assert_the_fixture_is_populated(&cluster, RECORDS);

    // Plant the markers in a disjoint key range, same key LENGTH as the corpus's own.
    for marker in 0..MARKERS {
        cluster
            .propose(Command::StringSet {
                key: format!("zzz-{marker:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
    }
    let planted_store = RECORDS + MARKERS;

    // ---- instrument one: VISITS ----
    snapshot_probe::reset();
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let counts = snapshot_probe::counts();
    print_counts("planted", &counts);
    let image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    println!(
        "  planted store = {planted_store}  addresses = {}  slabs = {}  index bytes = {}",
        counts.live_slab_scan_addresses,
        image.slabs.len(),
        image.index_bytes.len()
    );
    assert!(
        image.slabs.len() > 1,
        "the image carries {} slab(s): with one, a per-slab residual and a per-snapshot one are \
         the same number",
        image.slabs.len()
    );
    assert_eq!(
        counts.live_slab_scan_addresses,
        planted_store as u64,
        "the walk must visit exactly {RECORDS} + {MARKERS} = {planted_store} block addresses and \
         visited {}. This is the positive control for the walk: a walk that had silently stopped \
         following new records would still report {RECORDS}",
        counts.live_slab_scan_addresses
    );

    // ---- instrument two: the BYTE residual ----
    let image_slab_bytes = image
        .slabs
        .iter()
        .map(|slab| slab.bytes.len() as u64)
        .sum::<u64>();
    let residual = counts.slab_read_bytes as i64 - image_slab_bytes as i64;
    println!(
        "  slab bytes read = {}   image slab bytes = {image_slab_bytes}   residual = {residual}",
        counts.slab_read_bytes
    );
    assert!(
        counts.slab_read_bytes > 0 && image_slab_bytes > 0,
        "one side of the residual is zero ({} read, {image_slab_bytes} in the image), so a \
         residual of zero would mean nothing happened rather than nothing was unattributed",
        counts.slab_read_bytes
    );
    assert_eq!(
        residual, 0,
        "every slab byte the build read must reach the image: {} read, {image_slab_bytes} carried",
        counts.slab_read_bytes
    );

    // ---- the residual's own positive control ----
    //
    // A residual that reads zero is only worth something if it can read non-zero. One extra
    // `read_slab` is planted inside a fresh measured span; the residual must come back equal to
    // that slab's bytes, to the byte.
    let engine = cluster
        .node_engine_for_test(1)
        .expect("the leader serves an engine");
    let block_store = engine.block_store();
    let planted_slab = image.slabs[0].block_slab_id;
    let planted_bytes = image.slabs[0].bytes.len() as u64;
    assert!(
        planted_bytes > 0,
        "the slab chosen to plant with is empty, so the control below cannot distinguish a \
         residual that works from one that is stuck at zero"
    );

    snapshot_probe::reset();
    let extra = block_store
        .read_slab(planted_slab)
        .expect("reading a slab the image already carries must succeed");
    assert_eq!(
        extra.len() as u64,
        planted_bytes,
        "the planted read returned {} bytes against the {planted_bytes} the image carries for \
         slab {planted_slab}",
        extra.len()
    );
    drop(extra);
    let control_snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let control_counts = snapshot_probe::counts();
    let control_image = control_snapshot
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    let control_image_bytes = control_image
        .slabs
        .iter()
        .map(|slab| slab.bytes.len() as u64)
        .sum::<u64>();
    let control_residual = control_counts.slab_read_bytes as i64 - control_image_bytes as i64;
    println!(
        "  CONTROL: planted {planted_bytes} bytes of slab {planted_slab}; slab bytes read = {}, \
         image slab bytes = {control_image_bytes}, residual = {control_residual}",
        control_counts.slab_read_bytes
    );
    assert_eq!(
        control_residual, planted_bytes as i64,
        "the residual must recover the planted {planted_bytes} bytes EXACTLY and returned \
         {control_residual}. Without this the zero above is an instrument that cannot count, not \
         a cost that is not there"
    );
}

/// The total a store pays to reach a size, at a fixed snapshot cadence.
///
/// A per-snapshot cost that is flat per record of STORE is not a flat cost over a store's life,
/// because the store grows between snapshots. At one snapshot per `STEP` records, the k-th
/// snapshot reads `SLAB_BYTES_PER_RECORD * k * STEP` and the total after `R` snapshots is
///
///   SLAB_BYTES_PER_RECORD * STEP * R * (R + 1) / 2
///
/// which is quadratic in the records reached. That is asserted here as an EXACT identity at two
/// numbers of rounds rather than as a trend, and the ratio between the two totals is printed
/// beside the ratio of the sizes they reached so the square is visible directly.
#[test]
fn what_a_store_pays_in_total_to_reach_a_size_grows_with_the_square_of_it() {
    const STEP: usize = 1_000;
    const ROUNDS_SMALL: usize = 10;
    const ROUNDS_LARGE: usize = 20;

    fn run_to(rounds: usize) -> (u64, u64, usize) {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        force_the_threshold(&cluster);
        let mut total_slab_bytes = 0u64;
        let mut total_addresses = 0u64;
        let mut written = 0usize;
        for round in 1..=rounds {
            // Roll onto a fresh slab each round, so the image genuinely spans many slabs rather
            // than collapsing into slab zero the way a gibibyte target would make it.
            if round > 1 {
                assert!(
                    cluster
                        .node_engine_for_test(1)
                        .expect("the leader serves an engine")
                        .block_store()
                        .prepare_next_slab_with_target(1)
                        .expect("rolling onto a fresh slab must succeed")
                        .is_some(),
                    "the slab did not roll before round {round}, so the corpus would collapse \
                     into one slab and every per-slab figure would be a per-snapshot one"
                );
            }
            append_records(&cluster, written, STEP);
            written += STEP;

            snapshot_probe::reset();
            let report = cluster
                .maybe_trigger_snapshot()
                .expect("maybe_trigger_snapshot must succeed");
            let counts = snapshot_probe::counts();
            assert!(
                report.triggered,
                "round {round} did not snapshot ({}), so the total below is short a round",
                report.reason
            );
            assert_eq!(
                counts.live_slab_scan_addresses, written as u64,
                "round {round} walked {} addresses over a {written}-record store",
                counts.live_slab_scan_addresses
            );
            total_slab_bytes += counts.slab_read_bytes;
            total_addresses += counts.live_slab_scan_addresses;
            println!(
                "    round {round:>3}: store={written:>7}  slab bytes={:>10}  running total={:>12}",
                counts.slab_read_bytes, total_slab_bytes
            );
        }
        (total_slab_bytes, total_addresses, written)
    }

    let mut results = Vec::new();
    for rounds in [ROUNDS_SMALL, ROUNDS_LARGE] {
        println!("=== {rounds} rounds of {STEP} records ===");
        let (slab_bytes, addresses, reached) = run_to(rounds);
        // The closed form, computed from the cadence alone and compared with what was counted.
        let expected_bytes =
            SLAB_BYTES_PER_RECORD * STEP as u64 * (rounds as u64) * (rounds as u64 + 1) / 2;
        let expected_addresses = STEP as u64 * (rounds as u64) * (rounds as u64 + 1) / 2;
        println!(
            "  reached {reached} records over {rounds} snapshots: {slab_bytes} slab bytes read \
             (closed form {expected_bytes}), {addresses} addresses walked (closed form \
             {expected_addresses})"
        );
        println!(
            "  slab bytes per record REACHED = {:.3}  -- the per-record figure that grows",
            slab_bytes as f64 / reached as f64
        );
        assert!(
            slab_bytes > 0,
            "the {rounds}-round total is zero, so the identity below is vacuous"
        );
        assert_eq!(
            slab_bytes, expected_bytes,
            "the total over {rounds} snapshots must be exactly \
             {SLAB_BYTES_PER_RECORD} * {STEP} * {rounds} * {} / 2 = {expected_bytes} and was \
             {slab_bytes}",
            rounds + 1
        );
        assert_eq!(
            addresses, expected_addresses,
            "the addresses walked over {rounds} snapshots must be exactly {expected_addresses} \
             and were {addresses}"
        );
        results.push((rounds, reached, slab_bytes));
    }

    let (_, small_reached, small_total) = results[0];
    let (_, large_reached, large_total) = results[1];
    let size_ratio = large_reached as f64 / small_reached as f64;
    let total_ratio = large_total as f64 / small_total as f64;
    println!("=== INTEGRAL ===");
    println!("  records reached ratio = {size_ratio:.3}");
    println!("  total read ratio      = {total_ratio:.3}   (square of the size ratio would be {:.3})",
        size_ratio * size_ratio);
    println!(
        "  total slab bytes per record reached = {:.3} -> {:.3}",
        small_total as f64 / small_reached as f64,
        large_total as f64 / large_reached as f64
    );
    // The per-record-reached figure GROWS, which is what a quadratic total looks like when it is
    // divided by the store. Asserted as a strict inequality on the totals' cross-product, so a
    // total that had gone linear would fail here rather than pass on a loose bound.
    assert!(
        large_total * small_reached as u64 > small_total * large_reached as u64,
        "the total read to reach {large_reached} records ({large_total}) is no more than linear \
         in the total to reach {small_reached} ({small_total}). A flat per-snapshot cost over a \
         growing store is quadratic in total; if this has become linear, a read bound has \
         appeared and this module must be re-derived"
    );
}

/// A peer costs no more to help the further behind it is. STATED AS A REFUTATION.
///
/// A snapshot exists so a peer that has fallen behind the retained log can be caught up without
/// replaying history, so the natural worry about a whole-store cost is that it makes a lagging
/// node progressively more expensive to help. It does not, and the reason is worth writing down:
/// nothing on this path reads the receiving node at all beyond asking whether it may take the
/// snapshot. The image is the same image, the rebuild is the same rebuild, and the bytes are
/// identical at any lag.
///
/// What that means is the opposite of the worry. The cost PER ENTRY OF LAG falls as the peer falls
/// further behind; the expensive peer to help, per entry, is the one that is barely behind at all,
/// which is the same statement the INCREMENT regime makes from the leader's side.
#[test]
fn a_further_behind_peer_costs_no_more_to_help_than_a_barely_behind_one() {
    const RECORDS: usize = 10_000;
    const BARELY_BEHIND: u64 = 1;
    const FAR_BEHIND: u64 = 7_500;

    let mut rows = Vec::new();
    for lag in [BARELY_BEHIND, FAR_BEHIND] {
        let cluster = cluster_with(RECORDS);
        assert_the_fixture_is_populated(&cluster, RECORDS);
        force_the_threshold(&cluster);

        // Put node 2 `lag` entries behind. Node 2 still qualifies for the snapshot --
        // `node_accepts_snapshot` admits any node at or below the snapshot's index -- so what is
        // being compared is two different lags, not a peer that installs against one that does not.
        let (leader_applied, peer_applied) = {
            let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
            let leader_applied = inner.nodes.get(&1).expect("leader").applied_index;
            let peer = inner.nodes.get_mut(&2).expect("node 2 must exist");
            peer.applied_index = leader_applied.saturating_sub(lag);
            peer.commit_index = peer.commit_index.min(peer.applied_index);
            (leader_applied, peer.applied_index)
        };
        println!(
            "  lag={lag:>6}: leader applied={leader_applied}, peer applied={peer_applied}, \
             behind by {}",
            leader_applied - peer_applied
        );
        assert_eq!(
            leader_applied - peer_applied,
            lag,
            "the peer was not put {lag} entries behind, so this arm measures the wrong lag"
        );

        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        let counts = snapshot_probe::counts();
        assert!(
            report.triggered,
            "lag={lag} did not snapshot ({})",
            report.reason
        );
        println!(
            "           addresses={} slab_reads={} slab_bytes={} installs={} install_bytes={} \
             rebuilds={} publishes={}",
            counts.live_slab_scan_addresses,
            counts.slab_reads,
            counts.slab_read_bytes,
            counts.slab_installs,
            counts.slab_install_bytes,
            counts.engine_rebuilds,
            counts.engine_publishes
        );
        println!(
            "           slab bytes per ENTRY OF LAG = {:.3}",
            counts.slab_read_bytes as f64 / lag as f64
        );
        rows.push((lag, counts));
    }

    let (barely_lag, barely) = rows[0];
    let (far_lag, far) = rows[1];
    assert!(
        barely.slab_read_bytes > 0 && barely.engine_publishes > 0,
        "the barely-behind arm read {} bytes and published {} engines: with zero of either, the \
         equalities below hold vacuously",
        barely.slab_read_bytes,
        barely.engine_publishes
    );
    assert_eq!(
        barely.slab_read_bytes, far.slab_read_bytes,
        "helping a peer {far_lag} entries behind read {} slab bytes against {} for one {barely_lag} \
         entry behind. If these ever differ, the path has started reading the receiving node and \
         the refutation recorded here no longer holds",
        far.slab_read_bytes, barely.slab_read_bytes
    );
    assert_eq!(
        barely.slab_install_bytes, far.slab_install_bytes,
        "the install wrote {} slab bytes at lag {far_lag} against {} at lag {barely_lag}",
        far.slab_install_bytes, barely.slab_install_bytes
    );
    assert_eq!(
        barely.engine_rebuilds, far.engine_rebuilds,
        "the install rebuilt {} engines at lag {far_lag} against {} at lag {barely_lag}",
        far.engine_rebuilds, barely.engine_rebuilds
    );

    // The refutation, stated as arithmetic: per ENTRY of lag, the far-behind peer is the cheap one.
    let barely_per_entry = barely.slab_read_bytes as f64 / barely_lag as f64;
    let far_per_entry = far.slab_read_bytes as f64 / far_lag as f64;
    println!("=== REFUTATION ===");
    println!(
        "  slab bytes per entry of lag: {barely_per_entry:.3} at lag {barely_lag} -> \
         {far_per_entry:.3} at lag {far_lag}"
    );
    assert!(
        far_per_entry < barely_per_entry,
        "per entry of lag, helping a peer {far_lag} behind ({far_per_entry}) is not cheaper than \
         helping one {barely_lag} behind ({barely_per_entry}). The cost is flat in lag, so the \
         per-entry figure must fall as lag grows; if it does not, the flatness above is not what \
         it appears to be"
    );
}
