// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the index log COSTS as a store grows, measured at two corpus sizes.
//!
//! Every number here is a COUNT -- directory listings, piece paths, frames read, barriers,
//! bytes -- and never a duration. A duration taken on a shared box says as much about the
//! neighbours as about the code; the same round has been measured at 490 ms and at 1,077 ms on
//! two different afternoons. A count does not move with load, so a count is the claim.
//!
//! Each probe asserts its own DENOMINATOR before it asserts anything else. A probe whose fixture
//! quietly produced one piece instead of sixteen would report a flat cost and be believed.
//!
//! These read process-global counters (`super::probe`), so they require `--test-threads=1`,
//! which is how this suite is run. Each resets immediately before the call it measures.

use super::*;

/// One index item, the smallest a delta record can carry.
fn small_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.to_string(),
        model_id: "m".to_string(),
        component: None,
        object_id: 1,
        block_id: 0,
        address: None,
        size: 8,
        in_log: false,
        deleted: false,
    }
}

/// Set the rolling threshold for THIS THREAD and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// What one append phase cost, taken from the store's own counters and the piece enumeration.
struct AppendCost {
    records: usize,
    writes: u64,
    bytes_written: u64,
    /// Directory listings the WHOLE append phase performed. This is the number that says whether
    /// an append is independent of how many pieces the log is already in.
    dir_listings: u64,
    /// Piece paths those listings handed back, summed -- the per-piece quantity.
    piece_paths: u64,
    /// Times the append phase asked the filesystem to CREATE the log directory. One `mkdir`
    /// each, plus the `statx` behind an `EEXIST`.
    root_creates: u64,
    /// Times the append phase resolved WHICH FILE the log is being written to. At least one
    /// `statx` each.
    path_probes: u64,
    pieces: usize,
}

impl AppendCost {
    fn bytes_per_record(&self) -> f64 {
        self.bytes_written as f64 / self.records as f64
    }
    fn listings_per_record(&self) -> f64 {
        self.dir_listings as f64 / self.records as f64
    }
}

/// Append `records` delta records of `items_each` items into a fresh store, counting what it cost.
///
/// `durable: false` -- the barrier is measured on its own in
/// [`what_a_declined_post_dump_sweep_costs`]; mixing it in here would make the append phase's
/// cost depend on how the flush gate happened to coalesce, which is not what is being measured.
fn append_phase(
    dir: &std::path::Path,
    records: usize,
    items_each: usize,
    shard_id: ShardId,
) -> (LocalIndexLogStore, AppendCost) {
    let store = LocalIndexLogStore::new(dir);
    probe::reset();
    let mut value = 0usize;
    while value < records {
        let mut items = Vec::with_capacity(items_each);
        let mut item = 0usize;
        while item < items_each {
            // ZERO-PADDED, and the padding is the point. An unpadded key is one character longer
            // at 10,000 records than at 1,000, and it is written into every record -- which puts
            // 2.3 bytes per record of FIXTURE into a number that is supposed to be measuring the
            // CODE. Padded, the only field that differs between the two fixtures is `sequence`,
            // and `expected_sequence_width_residual` below accounts for that one exactly.
            items.push(small_item(
                (value % 64) as u32,
                &format!("tenant/1/object/{value:08}/{item:02}"),
            ));
            item += 1;
        }
        store
            .append_delta(shard_id, items, Vec::new(), None, None, false, false)
            .unwrap();
        value += 1;
    }
    // Read the counters BEFORE anything that would enumerate the pieces again.
    let dir_listings = probe::dir_listings();
    let piece_paths = probe::piece_paths();
    let root_creates = probe::append_root_creates();
    let path_probes = probe::append_path_probes();
    let stats = store.stats(shard_id);
    let pieces = store.piece_count(shard_id);
    let cost = AppendCost {
        records,
        writes: stats.writes,
        bytes_written: stats.bytes_written,
        dir_listings,
        piece_paths,
        root_creates,
        path_probes,
        pieces,
    };
    (store, cost)
}

/// AN APPEND IS FLAT, AND THE PIECE COUNT IS NOT A MULTIPLIER ON IT.
///
/// Ten times the records, ten times the pieces -- and the same number of directory listings, which
/// is ONE: the sequence probe that runs on a shard's first append and is cached afterwards. The
/// rolling check is a `stat` of one path, so nothing on the append path enumerates the log.
///
/// This is the result the slab manifest did NOT have (#1867: one `write(2)` per fragment, 1,600,144
/// syscalls, a `BufWriter` took 80,000 slabs from 12,824 ms to 650 ms) and the one the write-ahead
/// log DID (flat per record, and correctly unbuffered because one framed record is one write). The
/// index log is the second shape: it writes one framed record with one `write_all`, and buffering
/// it would only delay bytes an acked write may need.
///
/// Asserted as a RATIO against the record count rather than as an absolute, so the test says
/// "flat" rather than "this many".
#[test]
fn what_one_index_log_append_costs_at_two_corpus_sizes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let (_small_store, small) = append_phase(small_dir.path(), 1_000, 1, 1);
    let (_large_store, large) = append_phase(large_dir.path(), 10_000, 1, 1);

    // THE DENOMINATOR FIRST. A fixture that never rolled would report a flat per-piece cost for
    // the uninteresting reason that there was only ever one piece.
    assert_eq!(small.writes, 1_000, "small fixture did not append 1,000");
    assert_eq!(large.writes, 10_000, "large fixture did not append 10,000");
    assert!(
        large.pieces >= 10 && large.pieces >= small.pieces * 5,
        "large fixture did not roll into many more pieces than the small one: {} against {}",
        large.pieces,
        small.pieces
    );

    println!(
        "index-log append: records {} -> {} ({:.2}x) | bytes {} -> {} ({:.2}x, {:.1} -> {:.1} per record) | pieces {} -> {} ({:.2}x) | dir listings {} -> {} | piece paths {} -> {}",
        small.records,
        large.records,
        large.records as f64 / small.records as f64,
        small.bytes_written,
        large.bytes_written,
        large.bytes_written as f64 / small.bytes_written as f64,
        small.bytes_per_record(),
        large.bytes_per_record(),
        small.pieces,
        large.pieces,
        large.pieces as f64 / small.pieces as f64,
        small.dir_listings,
        large.dir_listings,
        small.piece_paths,
        large.piece_paths,
    );

    // ORDERED so a mutant that kills the first does not stop the second being reached: the
    // listing claim is asserted before the bytes claim, and neither reads the other's value.
    //
    // ONE listing for the whole phase, at BOTH sizes. Not "one per record" and not "one per
    // piece" -- one for the shard, from the sequence probe the first append runs and the rest
    // take from the cache.
    assert_eq!(
        small.dir_listings, 1,
        "1,000 appends performed {} directory listings, expected the single cached sequence probe",
        small.dir_listings
    );
    assert_eq!(
        large.dir_listings, 1,
        "10,000 appends performed {} directory listings, expected the single cached sequence probe",
        large.dir_listings
    );
    assert!(
        large.listings_per_record() <= small.listings_per_record(),
        "listings per record grew with the corpus: {:.6} -> {:.6}",
        small.listings_per_record(),
        large.listings_per_record()
    );

    // THE RESIDUAL, and it is zero.
    //
    // A tolerance would not be a measurement. Every field of the record is byte-identical between
    // the two fixtures except `sequence`, which is a msgpack unsigned integer and therefore costs
    // one byte up to 127, two up to 255 and three up to 65,535. Ten times the records is
    // therefore NOT ten times the bytes, by an amount that is computable in advance -- so compute
    // it, subtract it, and assert what is left over.
    let residual = large.bytes_written as i64
        - 10 * small.bytes_written as i64
        - expected_sequence_width_residual(1_000, 10_000);
    println!(
        "index-log append residual: {} - 10 x {} - {} (sequence width) = {}",
        large.bytes_written,
        small.bytes_written,
        expected_sequence_width_residual(1_000, 10_000),
        residual,
    );
    assert_eq!(
        residual, 0,
        "10x the records cost {} bytes against 10 x {} plus the {} bytes the wider sequence          numbers are worth -- {} bytes unaccounted for, so the append is writing something that          grows with the log",
        large.bytes_written,
        small.bytes_written,
        expected_sequence_width_residual(1_000, 10_000),
        residual
    );
}

/// What one msgpack unsigned integer costs.
///
/// `rmp_serde` writes a positive fixint up to 127, a `uint8` up to 255, a `uint16` up to 65,535,
/// and a `uint32` above that. The record's `sequence` is the only field whose value differs
/// between the two append fixtures, so this is the whole of the difference between them.
fn msgpack_uint_width(value: u64) -> i64 {
    if value <= 127 {
        1
    } else if value <= 255 {
        2
    } else if value <= 65_535 {
        3
    } else if value <= u32::MAX as u64 {
        5
    } else {
        9
    }
}

/// Bytes the wider sequence numbers of the LARGE fixture are worth, over ten copies of the small
/// one. Sequences run 1..=n in both.
fn expected_sequence_width_residual(small: u64, large: u64) -> i64 {
    let total = |n: u64| -> i64 {
        let mut sum = 0i64;
        let mut sequence = 1u64;
        while sequence <= n {
            sum += msgpack_uint_width(sequence);
            sequence += 1;
        }
        sum
    };
    total(large) - (large / small) as i64 * total(small)
}

/// WHAT THE LOAD-PATH FOLD HOLDS WHILE IT FOLDS.
///
/// `for_each_delta_record` exists because a caller that folds every record into one answer has no
/// use for a vector of all of them, and its own note says the folding caller is the load path.
/// This measures the difference the two shapes make, so that claim has a number under it: the
/// collecting form allocates the whole decoded log and holds it until the caller drops it, the
/// streaming form holds one record.
///
/// Feature-gated, because the counting allocator is only installed under `alloc-probe` -- without
/// it the counters never move and this would assert that a path allocates nothing.
#[cfg(feature = "alloc-probe")]
#[test]
fn what_the_two_fold_shapes_hold_at_two_corpus_sizes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let (small_store, small_append) = append_phase(small_dir.path(), 1_000, 1, 1);
    let (large_store, large_append) = append_phase(large_dir.path(), 10_000, 1, 1);
    assert_eq!(small_append.writes, 1_000);
    assert_eq!(large_append.writes, 10_000);

    let measure = |store: &LocalIndexLogStore| {
        let collecting = crate::alloc_probe::Probe::start();
        let held = store.read_delta_records(1, 0).unwrap();
        let collecting = (collecting.stop(), held.len());
        drop(held);
        let streaming = crate::alloc_probe::Probe::start();
        let mut seen = 0usize;
        store
            .for_each_delta_record(1, 0, |_| seen += 1)
            .unwrap();
        (collecting.0, collecting.1, streaming.stop(), seen)
    };
    let (small_collect, small_held, small_stream, small_seen) = measure(&small_store);
    let (large_collect, large_held, large_stream, large_seen) = measure(&large_store);

    // THE DENOMINATOR: both shapes saw every record, so the difference below is in what they
    // KEPT, not in what they did.
    assert_eq!(small_held, small_seen, "the two shapes disagree at 1,000");
    assert_eq!(large_held, large_seen, "the two shapes disagree at 10,000");
    assert_eq!(large_seen, 10_000, "the large fold saw {large_seen} records");

    println!(
        "index-log fold shapes: 1,000 records -- collecting {} allocs / {} outstanding, streaming {} allocs / {} outstanding | 10,000 records -- collecting {} allocs / {} outstanding, streaming {} allocs / {} outstanding",
        small_collect.allocs,
        small_collect.outstanding(),
        small_stream.allocs,
        small_stream.outstanding(),
        large_collect.allocs,
        large_collect.outstanding(),
        large_stream.allocs,
        large_stream.outstanding(),
    );

    // THE DIFFERENCE BETWEEN THEM IS THE MEASUREMENT, not either one on its own.
    //
    // Both shapes report a floor of about two outstanding allocations per record, and neither is
    // holding it: `CountingAllocator::realloc` forwards to `System::realloc`, counting an
    // allocation with no matching free, so every `Vec` that grew during the walk shows up as
    // outstanding whether or not it was dropped. That floor is common to both shapes and cancels.
    // What does not cancel is the vector of decoded records, and that is what is left when the
    // one is subtracted from the other.
    let small_extra = small_collect.outstanding() - small_stream.outstanding();
    let large_extra = large_collect.outstanding() - large_stream.outstanding();
    println!(
        "index-log fold shapes, what COLLECTING retains over STREAMING: {} at 1,000 records ({:.2} per record) -> {} at 10,000 ({:.2} per record), {:.2}x",
        small_extra,
        small_extra as f64 / 1_000.0,
        large_extra,
        large_extra as f64 / 10_000.0,
        large_extra as f64 / small_extra as f64,
    );

    // Half one: the two shapes do the SAME WORK. Within 1% on allocation calls, so what follows
    // is a difference in what is KEPT and not in what is done.
    let work_ratio = large_stream.allocs as f64 / large_collect.allocs as f64;
    assert!(
        (0.99..=1.01).contains(&work_ratio),
        "the two fold shapes did different amounts of work: {} allocations streaming against {} \
         collecting ({:.4}x)",
        large_stream.allocs,
        large_collect.allocs,
        work_ratio
    );
    // Half two, ORDERED after it and reading none of its values: what the collecting form keeps
    // over the streaming one is linear in the record count -- ten times the records, ten times
    // the retained allocations.
    assert!(
        small_extra > 0,
        "the collecting fold retained nothing over the streaming one ({small_extra}) -- there is \
         no difference here to measure"
    );
    assert!(
        large_extra >= small_extra * 9,
        "what the collecting fold retains did not grow with the corpus: {small_extra} at 1,000 \
         records against {large_extra} at 10,000"
    );
}

/// What a fold of the whole log read, and what it handed back.
struct FoldCost {
    frames_read: u64,
    taken: usize,
    declined_pieces: u64,
    log_bytes: u64,
    pieces: usize,
}

fn fold_cost(store: &LocalIndexLogStore, shard_id: ShardId, retain_after: u64) -> FoldCost {
    let pieces = store.piece_count(shard_id);
    let log_bytes = store.log_len_bytes(shard_id);
    probe::reset();
    let mut taken = 0usize;
    store
        .for_each_delta_record(shard_id, retain_after, |_| taken += 1)
        .unwrap();
    FoldCost {
        frames_read: probe::fold_frames_read(),
        taken,
        declined_pieces: probe::fold_pieces_declined(),
        log_bytes,
        pieces,
    }
}

/// THE FOLD TRACKS RECORDS, NOT BYTES -- and the two are measured against each other.
///
/// The write-ahead log's replay tracks BYTES and is superlinear in them: 4.01x the bytes gave
/// 12.45x the `statx`. Two append-heavy writers, the same symptom, and this one comes out the
/// other way. Three fixtures separate the quantities:
///
/// - ten times the RECORDS at the same record size reads ten times the frames;
/// - the same record count at roughly seven times the BYTES reads THE SAME frames.
///
/// Neither claim alone would do it. The first is consistent with tracking bytes, because at a
/// fixed record size the two move together -- which is exactly how a byte-tracking cost hides.
#[test]
fn whether_the_index_log_fold_tracks_records_or_bytes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let thin_small = tempfile::tempdir().unwrap();
    let thin_large = tempfile::tempdir().unwrap();
    let fat_small = tempfile::tempdir().unwrap();
    let (thin_small_store, _) = append_phase(thin_small.path(), 1_000, 1, 1);
    let (thin_large_store, _) = append_phase(thin_large.path(), 10_000, 1, 1);
    let (fat_small_store, _) = append_phase(fat_small.path(), 1_000, 8, 1);

    let thin_small_fold = fold_cost(&thin_small_store, 1, 0);
    let thin_large_fold = fold_cost(&thin_large_store, 1, 0);
    let fat_small_fold = fold_cost(&fat_small_store, 1, 0);

    // THE DENOMINATORS. The fat fixture has to be genuinely fatter in bytes and genuinely equal
    // in records, or the separation below compares nothing.
    assert_eq!(
        thin_small_fold.taken, 1_000,
        "thin small fold took {} records, expected 1,000",
        thin_small_fold.taken
    );
    assert_eq!(
        fat_small_fold.taken, 1_000,
        "fat small fold took {} records, expected 1,000",
        fat_small_fold.taken
    );
    assert!(
        fat_small_fold.log_bytes >= thin_small_fold.log_bytes * 3,
        "fat fixture is not materially fatter: {} bytes against {}",
        fat_small_fold.log_bytes,
        thin_small_fold.log_bytes
    );

    println!(
        "index-log fold: thin 1k = {} frames / {} bytes / {} pieces | thin 10k = {} frames / {} bytes / {} pieces ({:.2}x frames, {:.2}x bytes) | fat 1k = {} frames / {} bytes / {} pieces ({:.2}x frames, {:.2}x bytes against thin 1k)",
        thin_small_fold.frames_read,
        thin_small_fold.log_bytes,
        thin_small_fold.pieces,
        thin_large_fold.frames_read,
        thin_large_fold.log_bytes,
        thin_large_fold.pieces,
        thin_large_fold.frames_read as f64 / thin_small_fold.frames_read as f64,
        thin_large_fold.log_bytes as f64 / thin_small_fold.log_bytes as f64,
        fat_small_fold.frames_read,
        fat_small_fold.log_bytes,
        fat_small_fold.pieces,
        fat_small_fold.frames_read as f64 / thin_small_fold.frames_read as f64,
        fat_small_fold.log_bytes as f64 / thin_small_fold.log_bytes as f64,
    );

    // Half one: ten times the records is ten times the frames.
    assert_eq!(
        thin_large_fold.frames_read,
        thin_small_fold.frames_read * 10,
        "10x the records did not read 10x the frames: {} against {}",
        thin_large_fold.frames_read,
        thin_small_fold.frames_read
    );
    // Half two, ORDERED after it and reading none of its values: several times the bytes at the
    // same record count reads the same frames. This is the half that says RECORDS.
    assert_eq!(
        fat_small_fold.frames_read, 1_000,
        "the fold read {} frames for 1,000 fat records -- it is not tracking records",
        fat_small_fold.frames_read
    );
}

/// THE RETAIN FLOOR SAVES THE FOLD NOTHING -- every record below it is still read and decoded.
///
/// `for_each_delta_record` applies `retain_after_sequence` at the point it HANDS THE RECORD OVER,
/// which is after the frame has been read, the payload decoded and every item's stripped repeats
/// put back. A fold at a floor that covers the entire log therefore does the entire log's work
/// and returns nothing.
///
/// The zero is non-vacuous BECAUSE of the number beside it: the fold took no record AND read
/// every one of the 10,000 frames, so the zero is a fold that ran and declined, not a fold that
/// found an empty log.
///
/// The pieces already carry the answer in their names -- `drop_covered_index_segments` unlinks a
/// piece whose `end` is at or below the same floor WITHOUT OPENING IT, on the same predicate. A
/// piece this fold could decline is exactly a piece reclaim would delete unread.
#[test]
fn the_retain_floor_does_not_spare_the_fold_the_records_below_it() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    let (store, cost) = append_phase(dir.path(), 10_000, 1, 1);
    assert_eq!(cost.writes, 10_000, "fixture did not append 10,000");
    assert!(
        cost.pieces >= 10,
        "fixture rolled into only {} pieces -- there is no sealed prefix to decline",
        cost.pieces
    );

    let everything = fold_cost(&store, 1, 0);
    let nothing = fold_cost(&store, 1, 10_000);

    println!(
        "index-log fold under the retain floor: floor 0 = {} frames read / {} taken | floor 10000 = {} frames read / {} taken / {} of {} pieces declined",
        everything.frames_read,
        everything.taken,
        nothing.frames_read,
        nothing.taken,
        nothing.declined_pieces,
        nothing.pieces,
    );

    // Half one: the fold at the top floor hands back nothing.
    assert_eq!(
        nothing.taken, 0,
        "a fold at a floor above every record handed back {} records",
        nothing.taken
    );
    // Half two, ORDERED after it: and it read all 10,000 frames anyway. Written as a comparison
    // against the unrestricted fold rather than as a literal, so it keeps saying the same thing
    // if the fixture size changes.
    assert_eq!(
        everything.frames_read, 10_000,
        "the unrestricted fold read {} frames of 10,000",
        everything.frames_read
    );
    assert!(
        nothing.frames_read + nothing.declined_pieces > 0,
        "the fold neither read a frame nor declined a piece -- it did not run"
    );
}

/// WHAT A RECLAIM ROUND ENUMERATES, AND WHETHER THE PIECE COUNT MULTIPLIES IT.
///
/// A round performs a FIXED number of directory listings whatever the log holds -- but each one
/// hands back one path per piece, and the caller then stats, opens or compares every path it got.
/// So the round is flat in listings and LINEAR IN PIECES in the work those listings feed.
///
/// This is the shape #1881 found on the dump manifest (27 listings and 30 whole-file reads to
/// read four fields) at a smaller constant and without the whole-file reads: a sealed piece here
/// is decided from its NAME (#1635), so the per-piece work is a `stat`, not a read.
#[test]
fn what_a_reclaim_round_enumerates_at_two_piece_counts() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let (small_store, small_append) = append_phase(small_dir.path(), 1_000, 1, 1);
    let (large_store, large_append) = append_phase(large_dir.path(), 10_000, 1, 1);

    assert!(
        large_append.pieces >= small_append.pieces * 5 && large_append.pieces >= 10,
        "the two fixtures do not differ in piece count: {} against {}",
        small_append.pieces,
        large_append.pieces
    );

    probe::reset();
    let small_report = small_store.gc_before_sequence(1, 1_000).unwrap();
    let small_listings = probe::dir_listings();
    let small_paths = probe::piece_paths();

    probe::reset();
    let large_report = large_store.gc_before_sequence(1, 10_000).unwrap();
    let large_listings = probe::dir_listings();
    let large_paths = probe::piece_paths();

    println!(
        "index-log reclaim round: pieces {} -> {} ({:.2}x) | directory listings {} -> {} ({:.2}x) | piece paths enumerated {} -> {} ({:.2}x) | dropped pieces {} -> {} | bytes copied {} -> {}",
        small_append.pieces,
        large_append.pieces,
        large_append.pieces as f64 / small_append.pieces as f64,
        small_listings,
        large_listings,
        large_listings as f64 / small_listings as f64,
        small_paths,
        large_paths,
        large_paths as f64 / small_paths as f64,
        small_report.dropped_segments,
        large_report.dropped_segments,
        small_report.bytes_copied,
        large_report.bytes_copied,
    );

    // Half one: the number of LISTINGS does not move with the log at all.
    assert_eq!(
        small_listings, large_listings,
        "a reclaim round's directory listings moved with the corpus: {} against {}",
        small_listings, large_listings
    );
    // AND it is at most five. The equality above alone would be satisfied by a round that listed
    // the directory fifty times at both sizes, which is exactly the shape #1881 found on the dump
    // manifest -- 27 listings and 30 whole-file reads to read four fields. A BUDGET, not a pin: a
    // round that learns to list once still passes, a round that grows a sixth listing does not.
    const ROUND_LISTING_BUDGET: u64 = 5;
    assert!(
        large_listings <= ROUND_LISTING_BUDGET,
        "a reclaim round listed the store directory {} times, over the budget of {}",
        large_listings,
        ROUND_LISTING_BUDGET
    );
    // Half two, ORDERED after it: what those listings hand back DOES. Stated as the inequality
    // rather than as a ratio, because the ratio is what the printed line is for.
    assert!(
        large_paths > small_paths * 3,
        "piece paths enumerated did not grow with the piece count: {} against {} for {} against {} pieces",
        small_paths,
        large_paths,
        small_append.pieces,
        large_append.pieces
    );
    // And the round removed what it was asked to, at both sizes -- so neither number above was
    // taken from a round that declined to do anything.
    assert!(
        small_report.dropped_segments > 0 && large_report.dropped_segments > 0,
        "a round dropped no piece: {} and {}",
        small_report.dropped_segments,
        large_report.dropped_segments
    );
}

/// THE DECLINE GUARD TAKES NO BARRIER, AND IT LOOKED AT EVERYTHING BEFORE DECIDING NOT TO.
///
/// `gc_reflected_before_anchor` will not rewrite the piece being written when the rewrite would
/// reclaim less than `min_reclaimable_bytes`. The claim is that the declined round costs no
/// barrier -- which is only worth anything if the round had something it could have taken. Both
/// halves are asserted, the zero second:
///
/// - the round examined every record in the piece being written and found bytes it COULD have
///   reclaimed (so the decline is a decision, not an empty log);
/// - and it took no `fsync` at any index-log site while making it.
#[test]
fn what_a_declined_post_dump_sweep_costs() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    // Every record anchored at WAL sequence 1, so a sweep at anchor 1 finds all of them
    // reflected -- and none of them in a sealed piece, because nothing rolls at 64 KiB before
    // the log gets there. Kept deliberately under one piece: this probe is about the REWRITE
    // decline, and a piece that rolled would be unlinked by the other half of the sweep.
    let records = 200usize;
    let mut value = 0usize;
    while value < records {
        store
            .append_delta(
                1,
                vec![small_item(0, &format!("tenant/1/object/{value}"))],
                Vec::new(),
                Some(1),
                None,
                false,
                false,
            )
            .unwrap();
        value += 1;
    }

    crate::durability_metrics::reset();
    // A floor no rewrite of this log could ever clear.
    let report = store
        .gc_reflected_before_anchor(1, 1, records as u64, u64::MAX)
        .unwrap();
    let barriers = crate::durability_metrics::snapshot();
    let index_log_barriers: u64 = barriers
        .iter()
        .filter(|(site, _)| site.starts_with("engine_index_log"))
        .map(|(_, count)| *count)
        .sum();

    println!(
        "index-log declined sweep: records before {} / after {} | removable before the threshold {} | reclaimable bytes {} | bytes copied {} | rewrite skipped {} | index-log barriers {} | all barriers {:?}",
        report.records_before,
        report.records_after,
        report.removable_records_before_budget,
        report.reclaimable_bytes,
        report.bytes_copied,
        report.rewrite_skipped,
        index_log_barriers,
        barriers,
    );

    // Half one, FIRST: the round read every record and found real bytes to reclaim. Without
    // this the zero below is vacuous -- a sweep of an empty log takes no barrier either.
    assert_eq!(
        report.records_before, records,
        "the sweep examined {} records of {}",
        report.records_before, records
    );
    assert!(
        report.reclaimable_bytes > 0 && report.removable_records_before_budget > 0,
        "the sweep found nothing it could have reclaimed: {} bytes, {} records",
        report.reclaimable_bytes,
        report.removable_records_before_budget
    );
    // Half two, ORDERED after it and reading none of its values: it declined, copied nothing,
    // and took no barrier.
    assert!(report.rewrite_skipped, "the sweep rewrote the log anyway");
    assert_eq!(
        report.bytes_copied, 0,
        "a declined sweep copied {} bytes",
        report.bytes_copied
    );
    assert_eq!(
        index_log_barriers, 0,
        "a declined sweep took {} index-log barriers: {:?}",
        index_log_barriers, barriers
    );
}

/// WHAT THE BACKGROUND RECLAIM POLL COSTS, WHICH IS NOT "ONE FILE-LENGTH STAT".
///
/// The embedded proxy polls `maybe_dump_and_reclaim_index_logs` on a timer, and the note at that
/// call site described the poll as one file-length `stat` per interval. It is not: the threshold
/// it reads comes from `undumped_len_since_dump`, which sums EVERY piece -- a directory listing
/// of the store root plus one `stat` per piece, once per interval, whether or not anything has
/// been written since the last one.
///
/// Flat in listings, linear in pieces, and paid on a timer rather than on a write. At the 64 KiB
/// rolling default a large store has many pieces, so this is the piece count arriving somewhere
/// no write asked for it.
#[test]
fn what_the_undumped_length_probe_enumerates() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let (small_store, small_append) = append_phase(small_dir.path(), 1_000, 1, 1);
    let (large_store, large_append) = append_phase(large_dir.path(), 10_000, 1, 1);

    assert!(
        large_append.pieces >= small_append.pieces * 5 && large_append.pieces >= 10,
        "the two fixtures do not differ in piece count: {} against {}",
        small_append.pieces,
        large_append.pieces
    );

    probe::reset();
    let small_undumped = small_store.undumped_len_since_dump(1);
    let small_listings = probe::dir_listings();
    let small_paths = probe::piece_paths();

    probe::reset();
    let large_undumped = large_store.undumped_len_since_dump(1);
    let large_listings = probe::dir_listings();
    let large_paths = probe::piece_paths();

    println!(
        "index-log undumped-length poll: pieces {} -> {} | directory listings {} -> {} | paths stat'd {} -> {} ({:.2}x) | undumped bytes {} -> {}",
        small_append.pieces,
        large_append.pieces,
        small_listings,
        large_listings,
        small_paths,
        large_paths,
        large_paths as f64 / small_paths as f64,
        small_undumped,
        large_undumped,
    );

    // Half one: it is not one stat. It is a listing, at both sizes.
    assert_eq!(
        small_listings, 1,
        "the poll performed {} directory listings, expected exactly one",
        small_listings
    );
    assert_eq!(
        large_listings, 1,
        "the poll performed {} directory listings, expected exactly one",
        large_listings
    );
    // Half two, ORDERED after it: and the paths it then stats are one per piece.
    assert_eq!(
        large_paths as usize, large_append.pieces,
        "the poll stat'd {} paths for {} pieces",
        large_paths, large_append.pieces
    );
    assert!(
        large_paths > small_paths * 3,
        "the poll's per-piece work did not grow with the piece count: {} against {}",
        small_paths,
        large_paths
    );
}

/// SYSCALL HARNESS, small arm. Ignored by default; run one arm at a time under `strace -c`.
///
/// Two arms differing only in record count, so the DIFFERENCE between their syscall tables is the
/// per-record cost with every fixed cost -- process start, the test harness, the temp directory,
/// the dynamic loader -- subtracted out. Neither arm's absolute table means anything on its own,
/// which is why neither asserts one.
///
///   cargo test -p temporalstore-rust --lib index_log_scale::syscalls_for_one_thousand_appends \
///     -- --exact --ignored --test-threads=1
///
/// run under: strace -f -c -o <file> <the test binary> ...
#[test]
#[ignore = "syscall-count harness; run one arm at a time under strace -c"]
fn syscalls_for_one_thousand_appends() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    let (_store, cost) = append_phase(dir.path(), 1_000, 1, 1);
    assert_eq!(cost.writes, 1_000);
}

/// SYSCALL HARNESS, large arm. See [`syscalls_for_one_thousand_appends`].
#[test]
#[ignore = "syscall-count harness; run one arm at a time under strace -c"]
fn syscalls_for_ten_thousand_appends() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    let (_store, cost) = append_phase(dir.path(), 10_000, 1, 1);
    assert_eq!(cost.writes, 10_000);
}

/// WHAT EVERY MAINTENANCE ROUND READS, WHICH IS THE WHOLE LOG.
///
/// `storage_log_compatibility_report` runs on every storage-manager cycle and asks the index log
/// `record_count`. That walk reads EVERY FRAME OF EVERY PIECE to produce one integer -- the only
/// thing the report does with it is compare it against a threshold. So the round's cost is linear
/// in the number of records the log holds, and it is paid whether or not anything was written
/// since the last round.
///
/// This is the quantity the piece names were made to carry. A sealed piece's name already says
/// `start` and `end`, and `gate_summary` (#1635) answers the same shape of question for the GC
/// gate from those names alone -- 3,401,234 bytes decoded down to 56,511, the same decision. The
/// count here is an upper bound away from being free in exactly the way
/// `index_log_records_removed` already is, and it is documented as a bound at three sites.
///
/// Measured, not argued: the store's own `records_read` counter is the denominator, and it is the
/// walk's own count rather than an estimate of one.
#[test]
fn what_the_round_report_reads_at_two_corpus_sizes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let (small_store, small_append) = append_phase(small_dir.path(), 1_000, 1, 1);
    let (large_store, large_append) = append_phase(large_dir.path(), 10_000, 1, 1);

    let read_count = |store: &LocalIndexLogStore| {
        let before = store.stats(1);
        probe::reset();
        let counted = store.record_count(1).unwrap();
        let listings = probe::dir_listings();
        let paths = probe::piece_paths();
        let after = store.stats(1);
        (
            counted,
            after.records_read - before.records_read,
            after.bytes_read - before.bytes_read,
            listings,
            paths,
        )
    };
    let (small_counted, small_read, small_bytes, small_listings, small_paths) =
        read_count(&small_store);
    let (large_counted, large_read, large_bytes, large_listings, large_paths) =
        read_count(&large_store);

    // THE DENOMINATOR: the walk saw every record that was appended, at both sizes. A round that
    // counted nothing would report a flat cost.
    assert_eq!(
        small_counted, 1_000,
        "the round's count came back {small_counted}, expected 1,000"
    );
    assert_eq!(
        large_counted, 10_000,
        "the round's count came back {large_counted}, expected 10,000"
    );

    println!(
        "index-log round report: records {} -> {} ({:.2}x) | records READ to produce the count {} -> {} ({:.2}x) | bytes read {} -> {} ({:.2}x) | pieces {} -> {} | directory listings {} -> {} | piece paths {} -> {}",
        small_append.writes,
        large_append.writes,
        large_append.writes as f64 / small_append.writes as f64,
        small_read,
        large_read,
        large_read as f64 / small_read as f64,
        small_bytes,
        large_bytes,
        large_bytes as f64 / small_bytes as f64,
        small_append.pieces,
        large_append.pieces,
        small_listings,
        large_listings,
        small_paths,
        large_paths,
    );

    // Half one: the round reads one frame per record in the log, at both sizes. Not a sample,
    // not the active piece -- all of it.
    assert_eq!(
        small_read, 1_000,
        "the round read {small_read} records to count 1,000"
    );
    // Half two, ORDERED after it and reading none of its values: ten times the records is ten
    // times the read. That is the claim, and it is what makes the cost linear in the corpus.
    assert_eq!(
        large_read, 10_000,
        "the round read {large_read} records to count 10,000"
    );
    assert!(
        large_bytes > small_bytes * 5,
        "bytes read did not grow with the corpus: {small_bytes} against {large_bytes}"
    );
}

/// WHAT AN APPEND ASKS THE FILESYSTEM FOR, PER RECORD -- and the two questions it stopped asking.
///
/// Counted at two record counts and taken as a DIFFERENCE, so everything that happens once per
/// store -- `new`'s own directory creation, the first append's sequence probe, the piece
/// enumeration behind it -- cancels and what is left is the per-record cost alone. That is the
/// same shape the `strace -f -c` arms take, in a quantity that does not move with the load on
/// the box.
///
/// Two quantities, and the append used to pay both on every record:
///
/// - a directory creation, which returned `EEXIST` 10,001 times in 10,002 because `new` had
///   already made the directory and nothing removes it under a live store;
/// - a path resolution, run TWICE -- once so the roll could learn the piece's length, once so
///   the open could learn its name -- of the same path, when one `metadata()` answers both.
///
/// Between them they were 4 of the append's 9.033 syscalls.
#[test]
fn what_an_append_asks_the_filesystem_for_per_record() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small = tempfile::tempdir().unwrap();
    let large = tempfile::tempdir().unwrap();
    let (_small_store, small_cost) = append_phase(small.path(), 1_000, 1, 3);
    let (_large_store, large_cost) = append_phase(large.path(), 10_000, 1, 3);

    // THE DENOMINATOR, before anything is divided by it. A phase that quietly appended nothing
    // would report a per-record cost of zero and read exactly like a fix.
    assert_eq!(
        small_cost.writes, 1_000,
        "the small arm wrote {} records, not 1,000",
        small_cost.writes
    );
    assert_eq!(
        large_cost.writes, 10_000,
        "the large arm wrote {} records, not 10,000",
        large_cost.writes
    );

    let extra_records = large_cost.records - small_cost.records;
    let extra_root_creates = large_cost.root_creates - small_cost.root_creates;
    let extra_path_probes = large_cost.path_probes - small_cost.path_probes;
    println!(
        "index-log append: 1k = {} root-creates / {} path-probes / {} pieces | 10k = {} / {} / {} \
         | per extra record: {:.4} root-creates, {:.4} path-probes over {} records",
        small_cost.root_creates,
        small_cost.path_probes,
        small_cost.pieces,
        large_cost.root_creates,
        large_cost.path_probes,
        large_cost.pieces,
        extra_root_creates as f64 / extra_records as f64,
        extra_path_probes as f64 / extra_records as f64,
        extra_records,
    );

    // Half one: nine thousand more appends create the directory NO MORE TIMES. Not fewer --
    // none. `new` made it and the store remembers, so the question is never asked again.
    assert_eq!(
        extra_root_creates, 0,
        "{extra_root_creates} directory creations for {extra_records} extra appends -- the \
         per-append create_dir_all is back"
    );
    // Half two, ORDERED after it and reading none of its values: ONE resolution per record,
    // plus ONE MORE for each roll. Both terms are the claim and the equality pins both.
    //
    // The per-record term is the whole of it in the steady state: the roll and the open share
    // the one answer. The per-roll term is the re-resolve after the seal, and it belongs here
    // rather than being rounded away -- the rename is what invalidates the answer, so a roll is
    // the one place the append is entitled to ask twice. Every piece after the first arrived by
    // a roll, so the piece counts give that term without a counter of its own.
    //
    // An equality, not a bound, so it fails in BOTH directions: an append that resolves twice
    // per record lands at 2x, and one that never re-resolves after a roll lands at exactly
    // `extra_records` -- which is the defect `a_roll_of_a_legacy_named_log_puts_the_next_record_
    // on_the_current_name` names.
    let extra_rolls = (large_cost.pieces - small_cost.pieces) as u64;
    assert!(
        extra_rolls > 0,
        "neither arm rolled more than the other ({} pieces against {}) -- the per-roll term \
         below is untested",
        small_cost.pieces,
        large_cost.pieces
    );
    assert_eq!(
        extra_path_probes,
        extra_records as u64 + extra_rolls,
        "{extra_path_probes} path resolutions for {extra_records} extra appends and \
         {extra_rolls} extra rolls -- an append resolves the active piece once, for the roll \
         and the open together, and once more only when the seal has renamed it away"
    );
}
