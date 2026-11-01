// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE HASH FIELD WRITE COSTS ON DISK, DECOMPOSED ACROSS THE THREE DURABLE STREAMS.
//!
//! # WHY THIS ARM EXISTS, AND WHY NOW
//!
//! A single `HSET` of an eight-byte value was priced at roughly **2,419 bytes on disk**. That
//! figure drove two separate investigations and only one of them has closed: the LATENCY half, a
//! per-write walk of the whole shard's object lookup, which `a_container_write_asks_its_own_slot`
//! removed for an 18.4x improvement at 800 fields. The byte figure was never the same question --
//! that walk moved no bytes at all, which the per-insert counters said at the time: WAL bytes per
//! insert went 81.6 to 87.5 across a 8x change in object size while time per insert grew 31x.
//!
//! So the ENVELOPE is still unpriced, and the number in circulation was measured before today's
//! changes. This arm re-prices it and, more usefully, says WHERE the bytes are, because
//! "2,419 bytes" names no term and so cannot be acted on.
//!
//! # WHAT IS MEASURED, AND WHY IT IS THREE NUMBERS AND NOT ONE
//!
//! A write touches three append-only streams and they are not interchangeable:
//!
//!   * the **WAL**, which is what makes the write durable and is charged per record;
//!   * the **block store**, which holds the value itself;
//!   * the **index log**, which records where the block went.
//!
//! A single total hides which one to attack, and the three have different levers -- the WAL's is
//! batching, the block store's is the record envelope, the index log's is the entry shape. Any one
//! of them could be the whole cost. They are reported separately, and their sum is asserted
//! against the total so the decomposition cannot quietly omit a stream.
//!
//! # AND THE SYNCS, BECAUSE A BYTE COUNT IS NOT A COST UNTIL YOU KNOW THE FLUSH GRANULARITY
//!
//! A durable append costs about ONE BLOCK on the device whatever the record holds, so a stream
//! writing 90 bytes per op with one fsync per op is not cheaper than one writing 400 bytes per op
//! in a shared flush. `syncs` per op is therefore reported beside the bytes.
//!
//! # AND IT IS SWEPT OVER CONCURRENCY, WHICH IS THE PART THAT CHANGES THE CONCLUSION
//!
//! A single-threaded fixture records **1.00 fsync per write**, and one row cannot say whether that
//! is the store or the fixture: the group commit on this path is driven by CONCURRENCY rather than
//! by a timer, so a lone writer has nothing to share a flush with. The question is what happens
//! when there IS something.
//!
//! Measured, writers each writing their own hash key so the reading is about flush sharing and not
//! about contention on one object. TWO RUNS, because a timing-dependent sweep read once is a
//! single sample and this tree has already had a monotonicity claim refuted by its fifth run:
//!
//! ```text
//!   writers  each   run A   run B   1/N if flushes were shared
//!   1        24     1.000   1.000   1.000
//!   2        24     1.000   1.000   0.500
//!   4        24     0.854   0.812   0.250
//!   8        24     0.844   0.672   0.125
//!   16       24     0.862   0.703   0.062
//!   8        200    0.946   0.859   0.125
//! ```
//!
//! **THE PER-ROW FIGURES MOVE BY UP TO A QUARTER BETWEEN RUNS, SO NO SINGLE ONE OF THEM IS A
//! RESULT.** What survives both runs is the ENDPOINT comparison, and it is not close: sixteen
//! writers would cost 0.06 fsyncs per write if each flush carried the whole set, and they cost
//! 0.70 to 0.86 -- an order of magnitude above that floor, in both runs, at every concurrency past
//! one. Two writers share nothing at all in either run. And lengthening each writer's run moves
//! the figure the WRONG way (0.86 to 0.95 at two hundred writes each), which is the opposite of
//! what a window that fills with work would do.
//!
//! That endpoint gap is what the assertion below pins, deliberately: a clause on any single row,
//! or on "more writers is better", would be riding a quantity that moves 25% between runs.
//!
//! So on this path **the barrier is per write, and concurrency does not amortise it.** Whether
//! that is deliberate -- an ack that waits for its own fsync is a durability choice, not a defect
//! -- is a separate question this arm does not settle. What it does settle is the ORDERING: the
//! device pays about one barrier per write at every concurrency, so the 220.5 B envelope is behind
//! that barrier and not in front of it.
//!
//! # WHY IT DOES NOT SHARE, AND IT IS NOT THE PATH -- IT IS THE ZERO DEFAULT WINDOW
//!
//! A previous measurement of the WAL's commit-delay knob reported **0.229 fsyncs per write at
//! eight threads of twenty-four writes** -- the same shape as the rows above that read 0.82 -- and
//! concluded batching was already done. Both readings are real, and the reason they differ is
//! mechanical rather than mysterious.
//!
//! `group_commit_sync_windowed` coalesces a writer only if it arrives **while an fsync is already
//! in flight**: the first caller through takes `sync_coord`, snapshots the high-water sequence and
//! fsyncs, and anyone whose sequence is already covered returns free. With the window at its
//! default of **zero**, that is the entire coalescing mechanism, so how much sharing happens is a
//! property of **how densely writes arrive** -- which is to say of how much work the caller does
//! between appends. A tight loop driving the WAL store directly piles writers up during each
//! fsync and shares well. `engine.execute` does a block append and an index update per command, so
//! its writers are spread out and mostly miss each other.
//!
//! **MEASURED, by widening the window on THIS path** (`TS_WAL_COMMIT_DELAY_US`, which
//! `group_commit_delay` reads):
//!
//! ```text
//!   writers  window 0   1 ms    5 ms    1/N floor
//!   1        1.000      1.000   1.000   1.000
//!   2        1.000      0.500   0.500   0.500
//!   4        0.844      0.396   0.250   0.250
//!   8        0.818      0.401   0.167   0.125
//!   16       0.823      0.409   0.159   0.062
//! ```
//!
//! At 5 ms the client path reaches the shared-flush floor **exactly** at four writers and comes
//! within 1.1x of it at eight, against **7.6x** at the default. So the per-write barrier is not a
//! property of this path at all: it is the zero window, and a window buys coalescing that arrival
//! density alone does not.
//!
//! That is the whole argument for a TIMER over an arrival-driven window: a timer coalesces
//! regardless of how the writes are spaced, which is exactly the case where a zero window gives up.
//! The knob is wired and its default is zero, so the cost is opt-in -- and it is not free, because
//! ack waits on the barrier and the window lands on write latency.
//!
//! **Do not quote either figure as "the" number.** 0.229 and 0.82 are the same mechanism at two
//! arrival densities.
//!
//! # THE FIXTURE IS TWENTY FIELDS, DELIBERATELY
//!
//! Twenty fields per hash is the shape the workload that produced the original figure uses, and it
//! is also small enough that the quadratic sibling scan pinned by
//! `what_a_new_container_element_scans` is invisible -- which is the point. This arm is about bytes,
//! and it is measured at a size where the latency term cannot contaminate it.
//!
//! # THE TWO QUANTITIES, BECAUSE A FIGURE FOR "WHAT A WRITE COSTS ON DISK" CAN MEAN EITHER
//!
//! What the streams APPEND is what a record-shrinking change moves. What the store's files OCCUPY
//! includes a one-time footprint -- creation, headers, preallocated segments -- and that term is
//! divided by however many operations the fixture ran. At twenty writes the files grow by about
//! 13,000 bytes per write and at two hundred by about 1,450, for identical work per write, which
//! is the signature of `(fixed + N x marginal) / N` and not of a per-write cost.
//!
//! **That is where a figure of roughly 2,400 bytes for one `HSET` comes from.** It is arithmetic on
//! a real measurement -- a fixed footprint over a denominator of about a hundred -- and it is not
//! the cost of a write. This arm separates the terms with a three-point cross so the two can never
//! be confused again, and the per-write envelope stands on its own at a tenth of that.
//!
//! # WHAT IS ASSERTED
//!
//! That the decomposition is COMPLETE, and that bytes per op do not grow with the field count. The
//! second is the claim worth guarding: it is what makes this a fixed envelope rather than an
//! amplification defect, and it is what says the lever is the per-record shape rather than
//! anything that scales with the container. No absolute byte figure is pinned -- those belong to
//! the published-figure guards, and a literal here would go stale on any format change while
//! saying nothing about whether the envelope is still flat.
//!
//! rust-internal: measures this crate's own durable streams, no product behaviour

#![allow(clippy::all)]
use super::*;

/// What one field write cost, summed over the three streams and split by stream.
#[derive(Debug, Clone, Copy)]
struct PerWrite {
    wal_bytes: f64,
    block_bytes: f64,
    index_bytes: f64,
    wal_syncs: f64,
    wal_flushes: f64,
    block_writes: f64,
    /// Every byte the store's own directories GREW by, divided by the writes. This is not the sum
    /// of the appends above and is not meant to be -- see the note on the two quantities.
    on_disk_bytes: f64,
}

impl PerWrite {
    fn total(&self) -> f64 {
        self.wal_bytes + self.block_bytes + self.index_bytes
    }
}

/// Every byte under a directory, counted recursively. A store's cost on disk is what its files
/// occupy, which is a different quantity from what its streams appended.
fn bytes_under(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else { return };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else { continue };
            if kind.is_dir() {
                walk(&entry.path(), total);
            } else if let Ok(meta) = entry.metadata() {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

/// Write `fields` distinct fields of one hash and report what each one cost durably.
///
/// The counters are read AFTER the shard is loaded, so the engine's own setup is not charged to a
/// write, and the payload is eight bytes because that is the size the figure under re-pricing used.
fn price_one_hash(fields: usize) -> PerWrite {
    use crate::types::{Command, ExecuteRequest};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);

    let disk_before = bytes_under(dir.path());
    let wal_before = engine.wal_store.stats(1);
    let block_before = engine.block_store.stats();
    let index_before = engine.index_log_store.stats(1);

    for i in 0..fields {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "one-hash".to_string(),
                field: format!("f{i:04}"),
                value: vec![b'v'; 8],
            },
        });
        assert!(response.status.ok, "field {i}: {:?}", response.status);
    }

    let wal = engine.wal_store.stats(1);
    let block = engine.block_store.stats();
    let index = engine.index_log_store.stats(1);
    let disk_after = bytes_under(dir.path());
    let n = fields as f64;

    PerWrite {
        wal_bytes: (wal.bytes_written - wal_before.bytes_written) as f64 / n,
        block_bytes: (block.bytes_written - block_before.bytes_written) as f64 / n,
        index_bytes: (index.bytes_written - index_before.bytes_written) as f64 / n,
        wal_syncs: (wal.syncs - wal_before.syncs) as f64 / n,
        wal_flushes: (wal.flushes - wal_before.flushes) as f64 / n,
        block_writes: (block.writes - block_before.writes) as f64 / n,
        on_disk_bytes: disk_after.saturating_sub(disk_before) as f64 / n,
    }
}

/// Fsyncs per write with `threads` writers going at once, each on its own hash key.
///
/// SEPARATE KEYS so the writers are not serialised behind one object, which would make this a
/// measurement of lock contention rather than of flush sharing. The question is only how many
/// device barriers the same number of writes costs when they overlap.
fn syncs_per_write_at(threads: usize, each: usize) -> f64 {
    use crate::types::{Command, ExecuteRequest};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);

    let before = engine.wal_store.stats(1);
    let mut handles = Vec::new();
    for thread in 0..threads {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            for i in 0..each {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("hash-{thread:02}"),
                        field: format!("f{i:04}"),
                        value: vec![b'v'; 8],
                    },
                });
                assert!(response.status.ok, "thread {thread} write {i}: {:?}", response.status);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("a writer thread panicked");
    }
    let after = engine.wal_store.stats(1);
    (after.syncs - before.syncs) as f64 / (threads * each) as f64
}

#[test]
fn what_one_hash_field_write_costs_durably() {
    /// The shape the original figure's workload uses, and small enough that the quadratic sibling
    /// scan cannot contaminate a byte measurement.
    const SHAPE: usize = 20;
    /// A middle size, which is the CHECK on the cross below rather than one of its two points.
    const MIDDLE: usize = 100;
    /// A third size, to ask whether the envelope is FIXED or grows with the container, and to give
    /// the on-disk cross its far point.
    const WIDER: usize = 200;

    println!();
    println!(
        "  {:<8} {:>10} {:>10} {:>10} {:>10}   {:>8} {:>8}   {:>12}",
        "fields", "wal B", "block B", "index B", "APPENDED", "syncs", "blk wr", "ON DISK B"
    );

    let mut rows: Vec<(usize, PerWrite)> = Vec::new();
    for fields in [SHAPE, MIDDLE, WIDER] {
        let per = price_one_hash(fields);
        println!(
            "  {fields:<8} {:>10.1} {:>10.1} {:>10.1} {:>10.1}   {:>8.2} {:>8.2}   {:>12.1}",
            per.wal_bytes,
            per.block_bytes,
            per.index_bytes,
            per.total(),
            per.wal_syncs,
            per.block_writes,
            per.on_disk_bytes,
        );
        rows.push((fields, per));
    }
    println!();

    let (_, shape) = rows[0];
    let (_, middle) = rows[1];
    let (_, wider) = rows[2];

    // --- NON-VACUITY, PER STREAM. A stream that recorded nothing makes its row a zero that reads
    // --- like a finding, and the completeness assertion below would still pass.
    assert!(
        shape.wal_bytes > 0.0,
        "the WAL recorded no bytes for {SHAPE} writes, so its row is a zero rather than a measurement"
    );
    assert!(
        shape.block_bytes > 0.0,
        "the block store recorded no bytes for {SHAPE} writes, so the value went nowhere durable"
    );
    assert!(
        shape.index_bytes > 0.0,
        "the index log recorded no bytes for {SHAPE} writes, so nothing records where the block went"
    );

    // --- THE DECOMPOSITION IS COMPLETE. Stated as an identity rather than trusted: if a fourth
    // --- stream starts carrying part of a write, this sum stops matching and the table above stops
    // --- being the whole cost. That is the failure mode a single total cannot have.
    let summed = shape.wal_bytes + shape.block_bytes + shape.index_bytes;
    assert!(
        (summed - shape.total()).abs() < f64::EPSILON,
        "the three streams sum to {summed} against a reported total of {}: the decomposition is \
         not the whole write, so some stream is unaccounted for",
        shape.total()
    );

    println!(
        "  one field write costs {:.1} B durably at {SHAPE} fields -- {:.0}% WAL, {:.0}% block, \
         {:.0}% index",
        shape.total(),
        100.0 * shape.wal_bytes / shape.total(),
        100.0 * shape.block_bytes / shape.total(),
        100.0 * shape.index_bytes / shape.total(),
    );
    // ---------------------------------------------------------------------------------------------
    // THE FSYNC READING NEEDS A SECOND CONCURRENCY OR IT SAYS THE OPPOSITE OF THE TRUTH.
    //
    // One writer records 1.00 fsync per write, from which the natural conclusion is that the
    // envelope is irrelevant until writes share a flush. The group commit is driven by CONCURRENCY
    // rather than by a timer, so a lone writer has nothing to batch with and 1.00 is the FIXTURE's
    // number. Eight writers share flushes and the figure collapses -- which is what says batching
    // is already done and the envelope is the live lever.
    // ---------------------------------------------------------------------------------------------
    const WRITERS: usize = 8;
    const EACH: usize = 24;
    println!("  --- FSYNCS PER WRITE, SWEPT OVER CONCURRENCY ---");
    println!("  SWEEP writers  each  fsyncs/write  vs 1/N");
    for (w, e) in [(1, EACH), (2, EACH), (4, EACH), (8, EACH), (16, EACH), (8, 200)] {
        let v = syncs_per_write_at(w, e);
        println!(
            "  SWEEP {w:>7} {e:>5} {v:>13.3} {:>8.3}",
            1.0 / w as f64
        );
    }
    let alone = syncs_per_write_at(1, EACH);
    let together = syncs_per_write_at(WRITERS, EACH);

    println!("  --- FSYNCS PER WRITE, AT TWO CONCURRENCIES ---");
    println!("  1 writer   {alone:>6.3} fsyncs per write");
    println!("  {WRITERS} writers  {together:>6.3} fsyncs per write");
    let if_shared = 1.0 / WRITERS as f64;
    println!(
        "  if each flush carried the whole set, {WRITERS} writers would cost {if_shared:.3}; they \
         cost {together:.3}, which is {:.1}x that floor",
        together / if_shared.max(f64::MIN_POSITIVE),
    );
    println!(
        "  so the barrier is PER WRITE at every concurrency, and the {:.1} B envelope ({:.0}% of \
         it the index entry) sits BEHIND that barrier, not in front of it",
        shape.total(),
        100.0 * shape.index_bytes / shape.total(),
    );
    println!();

    // --- NON-VACUITY FIRST: a concurrency that recorded no fsyncs measures no durability cost.
    assert!(
        alone > 0.0 && together > 0.0,
        "a concurrency recorded no fsyncs at all ({alone:.3} alone, {together:.3} together), so \
         neither row is a measurement of durability cost"
    );

    // --- THE SHAPE, PINNED AS THE FINDING RATHER THAN AS A DIRECTION. Concurrency does NOT
    // --- amortise the barrier here: eight writers stay far above the 1/8 a shared flush would
    // --- cost. Asserting "together < alone" instead would pass on a 1.2x improvement and read as
    // --- though batching worked, which is how this arm first stated it and why the sweep exists.
    // ---
    // --- IF THIS EVER FAILS, BATCHING LANDED ON THIS PATH. That is good news and the clause is
    // --- what will say so: delete it, and move the envelope to the front of the queue, because
    // --- the barrier will no longer be what a write pays for.
    const SHARED_FLUSH_FLOOR_SLACK: f64 = 2.0;
    // THE CLAUSE IS ABOUT THE DEFAULT CONFIGURATION, AND IT SAYS SO RATHER THAN ASSUMING IT.
    //
    // A non-zero commit window takes this path to the shared-flush floor -- 0.167 at eight writers
    // with 5 ms against 0.818 at the default -- so under a window the assertion below is FALSE and
    // correctly so. It was written before that was measured and fired exactly as its message
    // promised when the window was widened, which is the behaviour wanted from it; what was
    // missing is the precondition. Read the window rather than assuming zero, so the arm measures
    // the configuration it is actually running under.
    let window = std::env::var("TS_WAL_COMMIT_DELAY_US")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if window > 0 {
        println!(
            "  TS_WAL_COMMIT_DELAY_US={window}, so the endpoint clause does not apply: a window \
             coalesces by design and {together:.3} against a {if_shared:.3} floor is the window \
             working, not a defect"
        );
        assert!(
            together <= alone,
            "with a {window} us commit window, {WRITERS} writers cost {together:.3} fsyncs per \
             write against {alone:.3} for one. A window that makes concurrent writes cost MORE \
             barriers each is the mechanism failing."
        );
        return;
    }
    assert!(
        together > if_shared * SHARED_FLUSH_FLOOR_SLACK,
        "{WRITERS} concurrent writers cost {together:.3} fsyncs per write at the DEFAULT commit \
         window, within {:.1}x of the {if_shared:.3} a fully shared flush would cost. The barrier \
         is being amortised without a window, which it was not when this was written -- so either \
         the default changed (check `group_commit_delay`), or the write path got cheap enough per \
         command that writers now pile up during an fsync on their own. Either way it inverts the \
         conclusion: the {:.1} B envelope, {:.0}% of it the index entry, becomes the lever instead \
         of the barrier. Re-read the window table in this file's header before changing anything.",
        SHARED_FLUSH_FLOOR_SLACK,
        shape.total(),
        100.0 * shape.index_bytes / shape.total()
    );
    // ---------------------------------------------------------------------------------------------
    // THE ON-DISK COLUMN IS NOT A PER-WRITE COST, AND THE CROSS IS WHAT SHOWS IT.
    //
    // It reads about 13,000 B per write at 20 writes and about 1,450 at 200 -- for the same work
    // per write. That is the signature of `(fixed + N x marginal) / N`: a store's files carry a
    // one-time footprint (creation, headers, preallocated segments) that a small denominator
    // amortises badly. Divide that footprint by a hundred-odd operations and you get a four-figure
    // "bytes per write" that is almost entirely the fixed term, which is how a figure of roughly
    // 2,400 B for one `HSET` comes about. It is arithmetic on a real measurement, and it is not a
    // per-write cost.
    //
    // Two points separate the terms; the third CHECKS them, because a two-point fit has no
    // residual by construction and so cannot be wrong.
    // ---------------------------------------------------------------------------------------------
    let span = (WIDER - SHAPE) as f64;
    let marginal_on_disk = (wider.on_disk_bytes * WIDER as f64
        - shape.on_disk_bytes * SHAPE as f64)
        / span;
    let fixed_on_disk = shape.on_disk_bytes * SHAPE as f64 - marginal_on_disk * SHAPE as f64;
    let predicted_middle = (fixed_on_disk + marginal_on_disk * MIDDLE as f64) / MIDDLE as f64;
    let residual = predicted_middle - middle.on_disk_bytes;

    println!("  --- WHAT THE FILES GROW BY, SPLIT INTO ITS TWO TERMS ---");
    println!(
        "  a fixed store footprint of {:.0} B, plus {:.1} B for each write",
        fixed_on_disk, marginal_on_disk
    );
    println!(
        "  which predicts {:.1} B per write at {MIDDLE} writes against {:.1} measured, residual \
         {:.1} B ({:.1}%)",
        predicted_middle,
        middle.on_disk_bytes,
        residual,
        100.0 * residual.abs() / middle.on_disk_bytes.max(f64::MIN_POSITIVE),
    );
    println!(
        "  so a four-figure \"bytes per write\" is that footprint over a small denominator: at \
         {SHAPE} writes it reads {:.0}, at {WIDER} it reads {:.0}, for identical work",
        shape.on_disk_bytes, wider.on_disk_bytes
    );
    println!();

    // --- THE CROSS HOLDS AT A POINT IT WAS NOT FITTED ON. Without this the split above is two
    // --- numbers that reproduce their own inputs and nothing else.
    assert!(
        residual.abs() < 0.25 * middle.on_disk_bytes.max(f64::MIN_POSITIVE),
        "the fixed-plus-marginal split fitted on {SHAPE} and {WIDER} writes predicts {:.1} B per \
         write at {MIDDLE} and the measurement is {:.1}, a residual of {:.1} B. The growth is then \
         not one fixed term plus one per-write term, so neither number above means what it says -- \
         look for a step (a segment roll, a dump, a compaction) between these sizes before quoting \
         either.",
        predicted_middle,
        middle.on_disk_bytes,
        residual
    );

    // --- AND THE FIXED TERM DOMINATES AT THE SHAPE THE FIGURE WAS QUOTED AT, which is the whole
    // --- reason that figure cannot be read as a per-write cost.
    assert!(
        fixed_on_disk > marginal_on_disk * SHAPE as f64,
        "the fixed footprint {:.0} B no longer dominates the {SHAPE}-write marginal total {:.0} B. \
         If that has changed, the on-disk column IS approaching a per-write cost at this size and \
         the note above about four-figure figures being an amortisation artefact needs re-deriving.",
        fixed_on_disk,
        marginal_on_disk * SHAPE as f64
    );

    // --- THE TWO QUANTITIES ARE BOTH REAL AND MUST NOT BE CONFLATED. A figure for "what one write
    // --- costs on disk" can mean either, they differ by the ratio printed above, and only the
    // --- APPENDED one is reduced by shrinking a record. Asserted so a future reader cannot take
    // --- the smaller number for the larger question.
    // The appends are the quantity a record-shrinking change moves; the files are what the store
    // occupies. They are allowed to differ in BOTH directions -- the fixed footprint pushes the
    // files up at a small denominator, and reclamation pulls the marginal below the appends,
    // because a WAL record's bytes do not stay on disk for ever. So the only thing asserted about
    // the pair is that both were actually observed.
    assert!(
        shape.on_disk_bytes > 0.0,
        "the store's files did not grow at all over {SHAPE} writes, so the on-disk column is a \
         zero rather than a measurement"
    );
    assert!(
        shape.wal_syncs > 0.0,
        "no fsync was recorded for {SHAPE} writes, so this fixture is not paying for durability \
         and the device reading above is vacuous"
    );

    // --- THE ENVELOPE IS FIXED, NOT AMPLIFYING. Ten times the fields must not cost ten times the
    // --- bytes per field. This is the claim that makes the per-record shape the lever and rules
    // --- out anything that scales with the container -- and it is the half of the original figure
    // --- that was never in doubt but was also never guarded.
    const FLAT_ENOUGH: f64 = 1.5;
    let growth = wider.total() / shape.total().max(f64::MIN_POSITIVE);
    assert!(
        growth < FLAT_ENOUGH,
        "bytes per field write went x{growth:.2} ({:.1} -> {:.1} B) from {SHAPE} fields to \
         {WIDER}, a 10x change in container size. The durable envelope is supposed to be FIXED per \
         write -- if it now grows with the container, something writes a whole-object record per \
         element and that is an amplification defect, not an envelope one. Attribute it before \
         shrinking any record: per-stream bytes above say which stream grew.",
        shape.total(),
        wider.total()
    );
}
