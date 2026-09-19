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

/// Bytes this process has read from the kernel's point of view, from `/proc/self/io`.
///
/// `rchar` and not a count of anything this file maintains: the residual below is only worth
/// having if its TOTAL comes from outside the rows it audits. A sum of the probe counters would
/// audit itself and could never show drift.
///
/// `None` when the file cannot be read, which the callers turn into an APPARATUS failure rather
/// than a zero -- a residual of zero and a residual that was never measured read identically.
fn bytes_read_now() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("rchar:") {
            return value.trim().parse().ok();
        }
    }
    None
}

/// What one replay-shaped fold of a whole log cost, at a given base anchor.
struct ReplayCost {
    /// Records in the log, from the fixture.
    records: usize,
    /// Pieces the log is in, the one being written included.
    pieces: usize,
    /// Sealed pieces the fold declined to open, from their names.
    declined: u64,
    /// Frames the fold read off disk.
    frames_read: u64,
    /// Records the load path's own test would APPLY out of what the fold handed back.
    applied: usize,
    /// Bytes of the pieces the fold actually opened -- the ATTRIBUTED row.
    attributed_bytes: u64,
    /// Bytes the kernel says this process read across the whole fold -- the INDEPENDENT total.
    total_bytes_read: u64,
}

impl ReplayCost {
    /// The independent total minus the rows attributed to it. Fixed overhead divides away when
    /// this is taken per record at two corpus sizes; genuine drift climbs.
    fn residual_bytes(&self) -> i64 {
        self.total_bytes_read as i64 - self.attributed_bytes as i64
    }
    fn residual_per_record(&self) -> f64 {
        self.residual_bytes() as f64 / self.records as f64
    }
}

/// Fold a log the way the load path does, counting what it cost.
///
/// `decline` chooses between the two shapes: the base anchor as the load path now passes it, or
/// 0, which declines nothing and is what the walk did before. Everything else is identical, so
/// the difference between two of these is the decline and nothing else.
fn replay_cost(
    dir: &std::path::Path,
    shard_id: ShardId,
    records: usize,
    base_anchor: u64,
    decline: bool,
) -> ReplayCost {
    let store = LocalIndexLogStore::new(dir);
    let passed_anchor = if decline { base_anchor } else { 0 };

    // The ATTRIBUTED row, computed before the fold from the same predicate the fold uses. The
    // piece being written has no sealed name, so it is always counted as opened.
    let mut attributed_bytes = 0u64;
    let mut pieces = 0usize;
    for path in index_log_segment_paths(dir, shard_id) {
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        pieces += 1;
        let declinable = sealed_index_log_span(&path, shard_id)
            .is_some_and(|span| decline && piece_is_reflected_by(span, passed_anchor));
        if !declinable {
            attributed_bytes = attributed_bytes.saturating_add(metadata.len());
        }
    }

    probe::reset();
    let before = bytes_read_now().expect("APPARATUS: /proc/self/io carries no rchar line");
    let mut applied = 0usize;
    store
        .for_each_delta_record_above_anchor(shard_id, 0, passed_anchor, |record| {
            // The load path's own test, verbatim from `fold_index_log_deltas`.
            let record_anchor = record.applied_wal_sequence.unwrap_or(0);
            if !(base_anchor > 0 && record_anchor <= base_anchor) {
                applied += 1;
            }
        })
        .unwrap();
    let after = bytes_read_now().expect("APPARATUS: /proc/self/io carries no rchar line");

    ReplayCost {
        records,
        pieces,
        declined: probe::fold_pieces_declined(),
        frames_read: probe::fold_frames_read(),
        applied,
        attributed_bytes,
        total_bytes_read: after.saturating_sub(before),
    }
}

/// Build a log of `records` anchored deltas whose anchors CLIMB one per record.
fn climbing_anchor_phase(dir: &std::path::Path, shard_id: ShardId, records: usize) {
    let store = LocalIndexLogStore::new(dir);
    for value in 0..records {
        store
            .append_delta(
                shard_id,
                vec![small_item(
                    (value % 64) as u32,
                    // ZERO-PADDED to eight digits at both corpus sizes, so the only field that
                    // differs between the two fixtures is the one being varied. See
                    // `append_phase`.
                    &format!("tenant/1/object/{value:08}"),
                )],
                Vec::new(),
                Some(value as u64 + 1),
                None,
                false,
                false,
            )
            .unwrap();
    }
}

/// WHAT AN INDEX-LOG REPLAY READS, AT TWO CORPUS SIZES, AND WHETHER IT IS SUPERLINEAR.
///
/// IT IS NOT SUPERLINEAR. Ten times the records reads exactly ten times the frames -- the fold is
/// LINEAR in the records the log holds, and the piece count is not a second multiplier on it.
/// That is worth stating plainly, because the write-ahead log's replay, the same shape of walk in
/// the same store, IS superlinear: 4.01x the bytes there gave 12.45x the `statx`. This one is
/// not, and no fix should be sold as if it were.
///
/// WHAT IS WRONG WITH IT IS A DIFFERENT SHAPE. The cost is linear in the WHOLE LOG while the work
/// is linear in the SUFFIX THE BASE DOES NOT REFLECT. Whether those two come apart depends
/// entirely on the regime, and BOTH ARE MEASURED HERE because one of them hides the defect
/// exactly:
///
/// - PROPORTIONAL SUFFIX -- the base reflects a fixed FRACTION of the log. Then the applied count
///   grows with the corpus too, the cost per applied record is flat, and there is nothing to see.
///   This arm is the control, and it is here because it is the arm a careless fixture picks.
/// - FIXED SUFFIX -- the base fails to reflect a fixed NUMBER of records, whatever the log holds.
///   This is the regime a running store is in: a store dumps on a cadence, so what its base does
///   not yet reflect is bounded by the time since the last dump, not by how large the log has
///   grown behind it. Here the cost per applied record climbs with the corpus, and that climb IS
///   the defect.
///
/// The decline puts the cost back on the suffix. In both regimes the applied counts are asserted
/// EQUAL between the two fold shapes FIRST: a cheaper fold that applied fewer records would be a
/// data loss, and it would satisfy every cost assertion below.
#[test]
fn what_an_index_log_replay_reads_at_two_corpus_sizes() {
    // 8 KiB rather than the 64 KiB default, for the same reason
    // `the_gate_reads_piece_names_not_every_record` uses it: the decision under measurement is
    // PER PIECE, and at the default the small fixture is only three pieces -- a granularity at
    // which "declined a strict subset" says almost nothing. The rolling threshold is a
    // deployment knob, not part of the claim; what is being measured is how the cost moves with
    // the corpus at a fixed one.
    let _rolling = roll_at(8 * 1024);
    const SMALL: usize = 2_000;
    const LARGE: usize = 20_000;
    /// Records the base does NOT reflect, the SAME NUMBER at both corpus sizes.
    const FIXED_SUFFIX: usize = 200;

    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    climbing_anchor_phase(small_dir.path(), 21, SMALL);
    climbing_anchor_phase(large_dir.path(), 21, LARGE);

    // Anchors climb one per record, so an anchor of N leaves exactly `records - N` records above
    // the base.
    let fixed = |records: usize| (records - FIXED_SUFFIX) as u64;
    let tenth = |records: usize| (records * 9 / 10) as u64;

    let small_open = replay_cost(small_dir.path(), 21, SMALL, fixed(SMALL), false);
    let large_open = replay_cost(large_dir.path(), 21, LARGE, fixed(LARGE), false);
    let small_decline = replay_cost(small_dir.path(), 21, SMALL, fixed(SMALL), true);
    let large_decline = replay_cost(large_dir.path(), 21, LARGE, fixed(LARGE), true);
    let small_tenth = replay_cost(small_dir.path(), 21, SMALL, tenth(SMALL), false);
    let large_tenth = replay_cost(large_dir.path(), 21, LARGE, tenth(LARGE), false);

    // THE DENOMINATORS, BEFORE ANY RATIO. Each fixture must be in several pieces, or a per-piece
    // decision is not being measured; the large one must be in materially more, or the two arms
    // differ in records without differing in the quantity the decision is made over; and the two
    // corpora must differ tenfold.
    assert!(
        small_open.pieces >= 10 && large_open.pieces >= 100,
        "fixtures rolled into {} and {} pieces -- too few to measure a per-piece decision",
        small_open.pieces,
        large_open.pieces
    );
    assert!(
        large_open.pieces >= small_open.pieces * 5,
        "the large fixture is in {} pieces against the small one's {} -- not a corpus difference \
         the per-piece decision can see",
        large_open.pieces,
        small_open.pieces
    );
    assert_eq!(small_open.records * 10, large_open.records);
    assert!(
        small_open.frames_read > 0 && large_open.frames_read > 0,
        "APPARATUS: a fold read no frame at all"
    );

    let per_applied = |cost: &ReplayCost| cost.frames_read as f64 / cost.applied as f64;
    println!(
        "index-log replay at two corpus sizes ({} and {} pieces):\n\
         \x20 FIXED SUFFIX of {FIXED_SUFFIX} records (the regime a running store is in)\n\
         \x20   opening every piece: {SMALL:>6} = {:>6} frames / {:>4} applied / {:>3} of {:>3} \
         declined  ->  {LARGE:>6} = {:>6} frames / {:>4} applied / {:>3} of {:>3} declined   \
         ({:.2}x frames, {:.2}x applied, {:.2} -> {:.2} frames per applied record)\n\
         \x20   declining by name : {SMALL:>6} = {:>6} frames / {:>4} applied / {:>3} of {:>3} \
         declined  ->  {LARGE:>6} = {:>6} frames / {:>4} applied / {:>3} of {:>3} declined   \
         ({:.2}x frames, {:.2}x applied, {:.2} -> {:.2} frames per applied record)\n\
         \x20 PROPORTIONAL SUFFIX of one record in ten (the control -- the regime that HIDES it)\n\
         \x20   opening every piece: {SMALL:>6} = {:>6} frames / {:>4} applied  ->  {LARGE:>6} = \
         {:>6} frames / {:>4} applied   ({:.2} -> {:.2} frames per applied record)",
        small_open.pieces,
        large_open.pieces,
        small_open.frames_read,
        small_open.applied,
        small_open.declined,
        small_open.pieces,
        large_open.frames_read,
        large_open.applied,
        large_open.declined,
        large_open.pieces,
        large_open.frames_read as f64 / small_open.frames_read as f64,
        large_open.applied as f64 / small_open.applied as f64,
        per_applied(&small_open),
        per_applied(&large_open),
        small_decline.frames_read,
        small_decline.applied,
        small_decline.declined,
        small_decline.pieces,
        large_decline.frames_read,
        large_decline.applied,
        large_decline.declined,
        large_decline.pieces,
        large_decline.frames_read as f64 / small_decline.frames_read as f64,
        large_decline.applied as f64 / small_decline.applied as f64,
        per_applied(&small_decline),
        per_applied(&large_decline),
        small_tenth.frames_read,
        small_tenth.applied,
        large_tenth.frames_read,
        large_tenth.applied,
        per_applied(&small_tenth),
        per_applied(&large_tenth),
    );

    // HALF ONE, AND IT COMES FIRST BECAUSE IT IS THE SAFETY CLAIM: the decline applies exactly
    // the same records at both sizes. A cheaper fold that applied fewer would be a loss, not a
    // win, and it would satisfy every cost assertion below.
    assert_eq!(
        small_decline.applied, small_open.applied,
        "declining changed what the small fold applies: {} against {}",
        small_decline.applied, small_open.applied
    );
    assert_eq!(
        large_decline.applied, large_open.applied,
        "declining changed what the large fold applies: {} against {}",
        large_decline.applied, large_open.applied
    );
    assert_eq!(
        small_open.applied, FIXED_SUFFIX,
        "the small fold applied {} records, expected the {FIXED_SUFFIX} above the base",
        small_open.applied
    );
    assert_eq!(
        large_open.applied, FIXED_SUFFIX,
        "the large fold applied {} records, expected the SAME {FIXED_SUFFIX} the small one did \
         -- the suffix is what has to be held fixed for the ratio below to mean anything",
        large_open.applied
    );

    // HALF TWO: REPLAY IS LINEAR, NOT SUPERLINEAR. Ten times the records, exactly ten times the
    // frames. An equality rather than a bound, because a count repeats.
    assert_eq!(
        large_open.frames_read,
        small_open.frames_read * 10,
        "10x the records read {} frames against 10x {} -- replay is not linear in records",
        large_open.frames_read,
        small_open.frames_read
    );

    // HALF THREE: what DOES grow is the cost per record applied, and that is the defect. Ten
    // times the corpus, the same work, ten times the reading.
    assert!(
        per_applied(&large_open) >= per_applied(&small_open) * 9.0,
        "frames per applied record did not grow with the corpus: {:.2} at {SMALL} against {:.2} \
         at {LARGE}",
        per_applied(&small_open),
        per_applied(&large_open)
    );

    // HALF FOUR, THE CONTROL, AND IT IS WHY THE REGIME IS NAMED: at a suffix that grows WITH the
    // corpus the same fold is flat per applied record. The defect is invisible in that fixture,
    // and a measurement taken there would have reported no cost at all.
    assert!(
        (per_applied(&large_tenth) - per_applied(&small_tenth)).abs() < 0.01,
        "CONTROL: the proportional-suffix arm was expected to be flat per applied record and is \
         not: {:.4} at {SMALL} against {:.4} at {LARGE}",
        per_applied(&small_tenth),
        per_applied(&large_tenth)
    );

    // HALF FIVE: the decline takes the fixed-suffix cost back to flat -- within a factor of two
    // across a tenfold corpus, where opening every piece was a factor of ten.
    assert!(
        per_applied(&large_decline) < per_applied(&small_decline) * 2.0,
        "declining did not make the cost flat per applied record: {:.2} at {SMALL} against {:.2} \
         at {LARGE}",
        per_applied(&small_decline),
        per_applied(&large_decline)
    );
    assert!(
        large_decline.declined > 0 && (large_decline.declined as usize) < large_decline.pieces,
        "expected a strict subset of {} pieces to be declined, got {}",
        large_decline.pieces,
        large_decline.declined
    );

    // HALF SIX: THE RESIDUAL, from a total this file does not maintain.
    //
    // `rchar` is the kernel's count of bytes this process read across the whole fold; the
    // attributed row is the bytes of the pieces the fold opened, computed from the piece names
    // BEFORE the fold ran. What is left over is the fold's unattributed reading -- the two
    // `/proc/self/io` samples themselves and anything else that touches a file -- and it must
    // not grow with the corpus. Taken PER RECORD, so a fixed overhead divides away and genuine
    // drift climbs.
    assert!(
        small_decline.attributed_bytes > 0 && large_decline.attributed_bytes > 0,
        "APPARATUS: nothing was attributed -- the fold opened no piece"
    );
    assert!(
        small_decline.total_bytes_read > 0 && large_decline.total_bytes_read > 0,
        "APPARATUS: the kernel reported no bytes read at all across a fold"
    );
    println!(
        "  residual (kernel rchar total minus the bytes of the pieces the fold opened): \
         small = {} - {} = {} ({:.4} a record) | large = {} - {} = {} ({:.4} a record)",
        small_decline.total_bytes_read,
        small_decline.attributed_bytes,
        small_decline.residual_bytes(),
        small_decline.residual_per_record(),
        large_decline.total_bytes_read,
        large_decline.attributed_bytes,
        large_decline.residual_bytes(),
        large_decline.residual_per_record(),
    );
    assert!(
        large_decline.residual_per_record() <= small_decline.residual_per_record().abs() + 1.0,
        "the unattributed bytes per record CLIMBED with the corpus: {:.4} at {SMALL} against \
         {:.4} at {LARGE} -- something reads the log that these rows do not account for",
        small_decline.residual_per_record(),
        large_decline.residual_per_record(),
    );
}

/// REACHABILITY: the piece-level decline is on the load path, measured rather than argued.
///
/// `fold_index_log_deltas` is the only production caller of
/// `for_each_delta_record_above_anchor`, and this asserts that reaching the load path through
/// `LocalIndexLogStore` alone is not what exercises it -- the counter has to move from a call
/// that goes through the engine's fold.
///
/// The probe is FLOORED: the test fails if the instrument produced no output at all, because a
/// reachability run that reads zero everywhere reads exactly like a reachability run whose
/// apparatus never spoke. Print with `--nocapture` to see the rows.
#[test]
fn the_load_path_fold_is_the_one_that_declines() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    const RECORDS: usize = 4_000;
    let dir = tempfile::tempdir().unwrap();
    climbing_anchor_phase(dir.path(), 22, RECORDS);
    let store = LocalIndexLogStore::new(dir.path());
    let base_anchor = (RECORDS / 2) as u64;

    // Every entry point into this walk, and what each one declines. The list is DERIVED from the
    // two methods that exist rather than written out, and floored below.
    let mut rows: Vec<(&str, u64, u64)> = Vec::new();

    probe::reset();
    store.for_each_delta_record(22, 0, |_| {}).unwrap();
    rows.push((
        "for_each_delta_record (every non-load caller)",
        probe::fold_pieces_declined(),
        probe::fold_frames_read(),
    ));

    probe::reset();
    store.read_delta_records(22, 0).unwrap();
    rows.push((
        "read_delta_records",
        probe::fold_pieces_declined(),
        probe::fold_frames_read(),
    ));

    probe::reset();
    store
        .for_each_delta_record_above_anchor(22, 0, base_anchor, |_| {})
        .unwrap();
    rows.push((
        "for_each_delta_record_above_anchor (the load path)",
        probe::fold_pieces_declined(),
        probe::fold_frames_read(),
    ));

    println!("index-log fold entry points at base anchor {base_anchor}, over {RECORDS} records:");
    for (name, declined, frames) in &rows {
        println!("  {name:<52} {declined:>4} declined  {frames:>6} frames");
    }

    // THE FLOOR ON THE APPARATUS. Every row must have read something, or this measured nothing
    // and a zero in the `declined` column means only that the walk never ran.
    assert_eq!(rows.len(), 3, "the entry-point list lost a row: {rows:?}");
    assert!(
        rows.iter().all(|(_, _, frames)| *frames > 0),
        "APPARATUS: an entry point read no frame at all, so its decline count says nothing: \
         {rows:?}"
    );
    // The two that pass no anchor decline nothing, and that is the right answer for them.
    assert_eq!(rows[0].1, 0, "{} declined a piece", rows[0].0);
    assert_eq!(rows[1].1, 0, "{} declined a piece", rows[1].0);
    // The load-path entry point does.
    assert!(
        rows[2].1 > 0,
        "the load-path entry point declined nothing at base anchor {base_anchor} -- the decline \
         is not reachable"
    );
    assert!(
        rows[2].2 < rows[0].2,
        "the load-path entry point read {} frames against {} -- it declined nothing in practice",
        rows[2].2,
        rows[0].2
    );
}

/// What one fold of a large log cost, in every quantity this file can name.
///
/// The applied records are kept as their OBJECT KEYS rather than as a count. Reading too few
/// records is silent loss and a count cannot see the difference between a fold that applied the
/// right two hundred and one that applied two hundred of the wrong ones; the keys can, and the
/// first half below compares them element by element.
struct LargeFoldCost {
    records: usize,
    /// Files the log is in when the fold starts, the one being written included.
    pieces: usize,
    /// Piece paths the fold's own enumeration handed back. One per piece, and every one of them
    /// is then name-parsed and compared whether or not the piece is opened.
    piece_paths: u64,
    /// Directory listings the fold performed. The enumeration is ONE listing, not one per piece.
    dir_listings: u64,
    /// Sealed pieces declined from their names.
    declined: u64,
    /// Frames the fold read off disk.
    frames_read: u64,
    /// Object keys of the records the load path's own test would apply, IN ORDER.
    applied_keys: Vec<String>,
    /// Bytes of the pieces the fold opened -- the ATTRIBUTED row.
    attributed_bytes: u64,
    /// Bytes the kernel says this process read across the fold -- the INDEPENDENT total.
    total_bytes_read: u64,
}

impl LargeFoldCost {
    fn applied(&self) -> usize {
        self.applied_keys.len()
    }
    fn frames_per_applied(&self) -> f64 {
        self.frames_read as f64 / self.applied() as f64
    }
    fn paths_per_applied(&self) -> f64 {
        self.piece_paths as f64 / self.applied() as f64
    }
    fn pieces_per_record(&self) -> f64 {
        self.pieces as f64 / self.records as f64
    }
    fn residual_bytes(&self) -> i64 {
        self.total_bytes_read as i64 - self.attributed_bytes as i64
    }
    fn residual_per_record(&self) -> f64 {
        self.residual_bytes() as f64 / self.records as f64
    }
}

/// Fold a large log the way the load path does, counting everything it cost.
///
/// The same two shapes [`replay_cost`] takes -- the base anchor as the load path now passes it,
/// or 0, which declines nothing -- with the per-piece enumeration counted as well as the frames,
/// and the applied records kept by key.
fn large_fold_cost(
    dir: &std::path::Path,
    shard_id: ShardId,
    records: usize,
    base_anchor: u64,
    decline: bool,
) -> LargeFoldCost {
    let store = LocalIndexLogStore::new(dir);
    let passed_anchor = if decline { base_anchor } else { 0 };

    // The ATTRIBUTED row, computed before the fold from the same predicate the fold uses, and
    // deliberately not from the fold's own counters: a row taken from the thing being audited
    // cannot show that thing drifting.
    let mut attributed_bytes = 0u64;
    let mut pieces = 0usize;
    for path in index_log_segment_paths(dir, shard_id) {
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        pieces += 1;
        let declinable = sealed_index_log_span(&path, shard_id)
            .is_some_and(|span| decline && piece_is_reflected_by(span, passed_anchor));
        if !declinable {
            attributed_bytes = attributed_bytes.saturating_add(metadata.len());
        }
    }

    probe::reset();
    let before = bytes_read_now().expect("APPARATUS: /proc/self/io carries no rchar line");
    let mut applied_keys: Vec<String> = Vec::new();
    store
        .for_each_delta_record_above_anchor(shard_id, 0, passed_anchor, |record| {
            // The load path's own test, verbatim from `fold_index_log_deltas`.
            let record_anchor = record.applied_wal_sequence.unwrap_or(0);
            if !(base_anchor > 0 && record_anchor <= base_anchor) {
                applied_keys.push(
                    record
                        .items
                        .first()
                        .map(|item| item.object_key.clone())
                        .unwrap_or_default(),
                );
            }
        })
        .unwrap();
    let after = bytes_read_now().expect("APPARATUS: /proc/self/io carries no rchar line");

    LargeFoldCost {
        records,
        pieces,
        piece_paths: probe::piece_paths(),
        dir_listings: probe::dir_listings(),
        declined: probe::fold_pieces_declined(),
        frames_read: probe::fold_frames_read(),
        applied_keys,
        attributed_bytes,
        total_bytes_read: after.saturating_sub(before),
    }
}

/// WHAT THE INDEX LOG COSTS ON A LARGE STORE, AND WHICH OF ITS COSTS STOPS BEING FLAT THERE.
///
/// Every other index-log measurement in this file was taken at 1,000 to 20,000 records. This one
/// starts where they stop and goes an order of magnitude past it: 20,000 and 200,000. Two results
/// come out of it and only one of them is the one #1915 was about.
///
/// WHAT HOLDS. The piece-level decline holds exactly, and holds in ABSOLUTE terms rather than per
/// record. In the regime a running store is in -- a base that fails to reflect a FIXED NUMBER of
/// records, because a store dumps on a cadence and what its base does not yet hold is bounded by
/// the time since the last dump rather than by how deep the log has grown behind it -- the
/// declining fold reads 289 frames at 20,000 records and 280 at 200,000. Ten times the corpus,
/// the same reading. Opening every piece instead reads 20,000 and 200,000, which per record
/// applied is 100 against 1,000.
///
/// WHAT DOES NOT. The fold no longer OPENS the pieces it declines, but it still ENUMERATES them,
/// and their number is linear in the store: 0.0100 pieces per record at 20,000 and 0.0103 at
/// 200,000. (The drift is msgpack's integer width -- a deeper log carries wider sequence numbers
/// and so fills a piece with slightly fewer records. It is the same term
/// `expected_sequence_width_residual` accounts for exactly on the append path.) So per record the
/// fold actually applies, the enumeration climbs by the factor the corpus does: 200 paths for 200
/// applied records at 20,000, and 2,056 for the same 200 at 200,000.
///
/// NOTHING BOUNDS THAT NUMBER BUT A DUMP. A piece leaves the log only when a completed dump's
/// sweep unlinks it, and the last half here shows that sweep taking the whole large fixture to
/// one piece in a single round. Between dumps the piece count is a function of records written
/// and of nothing else -- no ceiling, no per-round cap, no separate budget for what the index
/// costs as against what the records in it cost. Every quantity this file has already measured as
/// "flat in listings, linear in pieces" -- the reclaim round's enumeration
/// ([`what_a_reclaim_round_enumerates_at_two_piece_counts`]) and the background poll's per-piece
/// `stat` ([`what_the_undumped_length_probe_enumerates`]) -- is therefore linear in the whole
/// store, without limit, and this is the test that says so with the piece count under it.
///
/// WHY IT IS NOT FIXED HERE, WITH THE NUMBER BESIDE THE REASON. Bounding the piece count means
/// merging sealed pieces into fewer, larger ones. The safety note on
/// [`LocalIndexLogStore::for_each_delta_record_above_anchor`] rests on there being exactly ONE
/// writer of a sealed name -- the `fs::rename` in `roll_index_log_segment_at` -- which is what
/// makes a piece's name and its contents fixed together and so makes declining by name safe at
/// all. A merger would be a second writer of sealed names, and declining by name is worth 200
/// frames against 200,000 on the load path. The enumeration it would save is 2,056 path
/// constructions behind one directory listing.
///
/// BOTH REGIMES ARE MEASURED, because one of them hides all of this:
///
/// - FIXED SUFFIX -- the base fails to reflect a fixed NUMBER of records whatever the log holds.
///   Cost per applied record climbs with the corpus unless the fold declines.
/// - PROPORTIONAL SUFFIX -- the base reflects a fixed FRACTION. The applied count grows with the
///   corpus too, every per-applied-record cost is flat, and a measurement taken only here would
///   report that the index log is free of charge at any size.
#[test]
fn what_the_index_log_fold_costs_on_a_large_store() {
    // 8 KiB rather than the 64 KiB default, for the reason `what_an_index_log_replay_reads_at_
    // two_corpus_sizes` gives: the decision under measurement is PER PIECE. The rolling
    // threshold is a deployment knob and is held FIXED across the two arms; what is being
    // measured is how the cost moves with the corpus at a fixed one.
    let _rolling = roll_at(8 * 1024);
    const SMALL: usize = 20_000;
    const LARGE: usize = 200_000;
    /// Records the base does NOT reflect, the SAME NUMBER at both corpus sizes.
    const FIXED_SUFFIX: usize = 200;
    const SHARD: ShardId = 43;

    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    climbing_anchor_phase(small_dir.path(), SHARD, SMALL);
    climbing_anchor_phase(large_dir.path(), SHARD, LARGE);

    // Anchors climb one per record, so an anchor of N leaves exactly `records - N` above it.
    let fixed = |records: usize| (records - FIXED_SUFFIX) as u64;
    let tenth = |records: usize| (records * 9 / 10) as u64;

    let small_fixed_open = large_fold_cost(small_dir.path(), SHARD, SMALL, fixed(SMALL), false);
    let small_fixed_decline = large_fold_cost(small_dir.path(), SHARD, SMALL, fixed(SMALL), true);
    let large_fixed_open = large_fold_cost(large_dir.path(), SHARD, LARGE, fixed(LARGE), false);
    let large_fixed_decline = large_fold_cost(large_dir.path(), SHARD, LARGE, fixed(LARGE), true);
    let small_part_open = large_fold_cost(small_dir.path(), SHARD, SMALL, tenth(SMALL), false);
    let small_part_decline = large_fold_cost(small_dir.path(), SHARD, SMALL, tenth(SMALL), true);
    let large_part_open = large_fold_cost(large_dir.path(), SHARD, LARGE, tenth(LARGE), false);
    let large_part_decline = large_fold_cost(large_dir.path(), SHARD, LARGE, tenth(LARGE), true);

    // ---------------------------------------------------------------------------------------
    // THE DENOMINATORS, AND THE PROOF THE FIXTURE IS IN THE REGIME THIS TEST CLAIMS IT IS IN.
    // None of the ratios below mean anything until these hold.
    // ---------------------------------------------------------------------------------------
    assert_eq!(
        small_fixed_open.records * 10,
        large_fixed_open.records,
        "the two corpora do not differ tenfold"
    );
    assert!(
        small_fixed_open.pieces >= 100 && large_fixed_open.pieces >= 1_000,
        "fixtures rolled into {} and {} pieces -- too few to measure a per-piece decision on a \
         large store",
        small_fixed_open.pieces,
        large_fixed_open.pieces
    );
    assert!(
        large_fixed_open.pieces >= small_fixed_open.pieces * 5,
        "the large fixture is in {} pieces against the small one's {} -- not a corpus difference \
         the per-piece decision can see",
        large_fixed_open.pieces,
        small_fixed_open.pieces
    );
    // A fixture whose pieces all looked alike in the field the decision reads could not tell a
    // correct decision from a constant. Every sealed piece of the large fixture must name a
    // DISTINCT `max_applied_wal`, which is the field `piece_is_reflected_by` compares.
    let mut named_pieces = 0usize;
    let mut distinct_anchors = std::collections::BTreeSet::new();
    for path in index_log_segment_paths(large_dir.path(), SHARD) {
        if let Some(span) = sealed_index_log_span(&path, SHARD) {
            named_pieces += 1;
            distinct_anchors.insert(span.max_applied_wal);
        }
    }
    assert!(
        named_pieces >= 1_000,
        "the large fixture holds {named_pieces} sealed pieces -- the decline has almost nothing \
         to decide"
    );
    assert_eq!(
        distinct_anchors.len(),
        named_pieces,
        "{} of the large fixture's {named_pieces} sealed pieces name the same anchor as another \
         -- a fixture whose pieces are alike in the field the decision reads cannot tell a \
         correct decision from a constant",
        named_pieces - distinct_anchors.len()
    );
    // And every arm did some reading, so a flat cost below is not a fold that never ran.
    for (name, cost) in [
        ("small fixed / opening", &small_fixed_open),
        ("small fixed / declining", &small_fixed_decline),
        ("large fixed / opening", &large_fixed_open),
        ("large fixed / declining", &large_fixed_decline),
        ("small partial / opening", &small_part_open),
        ("small partial / declining", &small_part_decline),
        ("large partial / opening", &large_part_open),
        ("large partial / declining", &large_part_decline),
    ] {
        assert!(
            cost.frames_read > 0,
            "APPARATUS: the {name} fold read no frame at all"
        );
        assert!(
            cost.applied() > 0,
            "APPARATUS: the {name} fold applied no record at all"
        );
        assert!(
            cost.total_bytes_read > 0 && cost.attributed_bytes > 0,
            "APPARATUS: the {name} fold reports no bytes read ({}) or none attributed ({})",
            cost.total_bytes_read,
            cost.attributed_bytes
        );
    }

    println!(
        "index log on a large store, {SMALL} -> {LARGE} records ({} -> {} pieces):\n\
         \x20 FIXED SUFFIX of {FIXED_SUFFIX} records (the regime a running store is in)\n\
         \x20   opening every piece : {:>7} frames / {:>6} applied / {:>5} of {:>5} declined / \
         {:>5} paths / {:>2} listings  ->  {:>7} frames / {:>6} applied / {:>5} of {:>5} \
         declined / {:>5} paths / {:>2} listings   ({:>8.2} -> {:>8.2} frames per applied, \
         {:>6.2} -> {:>6.2} paths per applied)\n\
         \x20   declining by name   : {:>7} frames / {:>6} applied / {:>5} of {:>5} declined / \
         {:>5} paths / {:>2} listings  ->  {:>7} frames / {:>6} applied / {:>5} of {:>5} \
         declined / {:>5} paths / {:>2} listings   ({:>8.2} -> {:>8.2} frames per applied, \
         {:>6.2} -> {:>6.2} paths per applied)\n\
         \x20 PROPORTIONAL SUFFIX of one record in ten (the regime that HIDES all of it)\n\
         \x20   opening every piece : {:>7} frames / {:>6} applied  ->  {:>7} frames / {:>6} \
         applied   ({:>8.2} -> {:>8.2} frames per applied)\n\
         \x20   declining by name   : {:>7} frames / {:>6} applied  ->  {:>7} frames / {:>6} \
         applied   ({:>8.2} -> {:>8.2} frames per applied)\n\
         \x20 pieces per record {:.5} -> {:.5} | bytes attributed to the declining fold {} -> {}",
        small_fixed_open.pieces,
        large_fixed_open.pieces,
        small_fixed_open.frames_read,
        small_fixed_open.applied(),
        small_fixed_open.declined,
        small_fixed_open.pieces,
        small_fixed_open.piece_paths,
        small_fixed_open.dir_listings,
        large_fixed_open.frames_read,
        large_fixed_open.applied(),
        large_fixed_open.declined,
        large_fixed_open.pieces,
        large_fixed_open.piece_paths,
        large_fixed_open.dir_listings,
        small_fixed_open.frames_per_applied(),
        large_fixed_open.frames_per_applied(),
        small_fixed_open.paths_per_applied(),
        large_fixed_open.paths_per_applied(),
        small_fixed_decline.frames_read,
        small_fixed_decline.applied(),
        small_fixed_decline.declined,
        small_fixed_decline.pieces,
        small_fixed_decline.piece_paths,
        small_fixed_decline.dir_listings,
        large_fixed_decline.frames_read,
        large_fixed_decline.applied(),
        large_fixed_decline.declined,
        large_fixed_decline.pieces,
        large_fixed_decline.piece_paths,
        large_fixed_decline.dir_listings,
        small_fixed_decline.frames_per_applied(),
        large_fixed_decline.frames_per_applied(),
        small_fixed_decline.paths_per_applied(),
        large_fixed_decline.paths_per_applied(),
        small_part_open.frames_read,
        small_part_open.applied(),
        large_part_open.frames_read,
        large_part_open.applied(),
        small_part_open.frames_per_applied(),
        large_part_open.frames_per_applied(),
        small_part_decline.frames_read,
        small_part_decline.applied(),
        large_part_decline.frames_read,
        large_part_decline.applied(),
        small_part_decline.frames_per_applied(),
        large_part_decline.frames_per_applied(),
        small_fixed_open.pieces_per_record(),
        large_fixed_open.pieces_per_record(),
        small_fixed_decline.attributed_bytes,
        large_fixed_decline.attributed_bytes,
    );

    // ---------------------------------------------------------------------------------------
    // HALF ONE, AND IT IS FIRST BECAUSE IT IS THE SAFETY CLAIM AND THE OTHER DIRECTION IS THE
    // SILENT ONE. Reading too FEW records loses an eviction recorded only in a delta and the
    // load still reports success; reading too many is merely slow. So the claim asserted here is
    // the strong one: the declining fold hands back the SAME RECORDS IN THE SAME ORDER as the
    // fold that opens every piece, element by element, in BOTH regimes and at BOTH sizes. A
    // count would pass a fold that applied the right number of the wrong records.
    // ---------------------------------------------------------------------------------------
    for (name, open, declined) in [
        ("small / fixed suffix", &small_fixed_open, &small_fixed_decline),
        ("large / fixed suffix", &large_fixed_open, &large_fixed_decline),
        ("small / proportional", &small_part_open, &small_part_decline),
        ("large / proportional", &large_part_open, &large_part_decline),
    ] {
        assert_eq!(
            open.applied_keys.len(),
            declined.applied_keys.len(),
            "{name}: declining changed HOW MANY records the fold applies, {} against {}",
            declined.applied_keys.len(),
            open.applied_keys.len()
        );
        if let Some(at) = open
            .applied_keys
            .iter()
            .zip(declined.applied_keys.iter())
            .position(|(a, b)| a != b)
        {
            panic!(
                "{name}: declining changed WHICH records the fold applies -- they first differ \
                 at position {at} of {}: opening every piece gave {:?}, declining by name gave \
                 {:?}",
                open.applied_keys.len(),
                open.applied_keys[at],
                declined.applied_keys[at]
            );
        }
    }
    assert_eq!(
        small_fixed_open.applied(),
        FIXED_SUFFIX,
        "the small fold applied {} records, expected the {FIXED_SUFFIX} above the base",
        small_fixed_open.applied()
    );
    assert_eq!(
        large_fixed_open.applied(),
        FIXED_SUFFIX,
        "the large fold applied {} records, expected the SAME {FIXED_SUFFIX} the small one did \
         -- holding the suffix fixed is what makes every ratio below mean anything",
        large_fixed_open.applied()
    );

    // ---------------------------------------------------------------------------------------
    // HALF TWO: WHAT HOLDS. The declining fold reads a FLAT NUMBER OF FRAMES -- flat in absolute
    // terms across a tenfold corpus, not merely flat per record. #1915 measured 289 at both 2,000
    // and 20,000; an order of magnitude further on it is still the last few pieces and nothing
    // else.
    // ---------------------------------------------------------------------------------------
    assert!(
        large_fixed_decline.frames_read <= small_fixed_decline.frames_read * 2,
        "the declining fold read {} frames at {LARGE} records against {} at {SMALL} -- the \
         decline does not hold at ten times the corpus",
        large_fixed_decline.frames_read,
        small_fixed_decline.frames_read
    );
    assert!(
        large_fixed_decline.frames_read * 100 < large_fixed_open.frames_read,
        "declining saved almost nothing at {LARGE} records: {} frames against {}",
        large_fixed_decline.frames_read,
        large_fixed_open.frames_read
    );
    // And the bytes it opens are flat with it, from the attributed row rather than the counters.
    assert!(
        large_fixed_decline.attributed_bytes <= small_fixed_decline.attributed_bytes * 2,
        "the declining fold opened {} bytes at {LARGE} records against {} at {SMALL}",
        large_fixed_decline.attributed_bytes,
        small_fixed_decline.attributed_bytes
    );

    // ---------------------------------------------------------------------------------------
    // HALF THREE, ORDERED after it and reading none of its values: WHAT GROWS. Opening every
    // piece costs the whole log for a suffix's worth of work, and at this size that is a factor
    // of ten between the two corpora.
    // ---------------------------------------------------------------------------------------
    assert_eq!(
        large_fixed_open.frames_read,
        small_fixed_open.frames_read * 10,
        "10x the records read {} frames against 10x {} -- the fold is not linear in records",
        large_fixed_open.frames_read,
        small_fixed_open.frames_read
    );
    assert!(
        large_fixed_open.frames_per_applied() >= small_fixed_open.frames_per_applied() * 9.0,
        "frames per applied record did not grow with the corpus: {:.2} at {SMALL} against {:.2} \
         at {LARGE}",
        small_fixed_open.frames_per_applied(),
        large_fixed_open.frames_per_applied()
    );

    // ---------------------------------------------------------------------------------------
    // HALF FOUR, THE CONTROL, AND IT IS WHY THE REGIME IS NAMED. At a suffix that grows WITH the
    // corpus the SAME folds are flat per applied record -- both of them, opening and declining.
    // A measurement taken only in this fixture would report that the index log costs the same at
    // any size, which is why this test asserts the other regime rather than choosing between
    // them.
    // ---------------------------------------------------------------------------------------
    assert!(
        (large_part_open.frames_per_applied() - small_part_open.frames_per_applied()).abs() < 0.01,
        "CONTROL: the proportional-suffix arm was expected to be flat per applied record while \
         opening every piece and is not: {:.4} at {SMALL} against {:.4} at {LARGE}",
        small_part_open.frames_per_applied(),
        large_part_open.frames_per_applied()
    );
    assert!(
        large_part_decline.frames_per_applied() <= small_part_decline.frames_per_applied(),
        "CONTROL: the proportional-suffix arm was expected to be flat per applied record while \
         declining and is not: {:.4} at {SMALL} against {:.4} at {LARGE}",
        small_part_decline.frames_per_applied(),
        large_part_decline.frames_per_applied()
    );

    // ---------------------------------------------------------------------------------------
    // HALF FIVE: WHAT DOES NOT HOLD, AND IT IS ONLY VISIBLE AT THIS SIZE. The pieces the fold
    // declines are not opened, but they are still enumerated and name-parsed, and their NUMBER
    // is linear in the store. Two claims, and the second is the one that matters:
    //
    //   - pieces per record is FLAT, which is to say the piece count grows with the corpus;
    //   - so per record the fold APPLIES, the enumeration grows by the factor the corpus does.
    //
    // At 20,000 records the declining fold names 200 pieces to apply 200 records. At 200,000 it
    // names 2,056 to apply the same 200.
    // ---------------------------------------------------------------------------------------
    let piece_growth =
        large_fixed_decline.pieces_per_record() / small_fixed_decline.pieces_per_record();
    assert!(
        (0.95..=1.15).contains(&piece_growth),
        "pieces per record moved by {piece_growth:.3}x between {SMALL} and {LARGE} records \
         ({:.5} -> {:.5}) -- the piece count is not linear in the corpus and the claim below \
         does not follow",
        small_fixed_decline.pieces_per_record(),
        large_fixed_decline.pieces_per_record()
    );
    assert_eq!(
        large_fixed_decline.piece_paths as usize, large_fixed_decline.pieces,
        "the declining fold enumerated {} paths for {} pieces",
        large_fixed_decline.piece_paths, large_fixed_decline.pieces
    );
    assert!(
        large_fixed_decline.paths_per_applied() >= small_fixed_decline.paths_per_applied() * 9.0,
        "piece paths per applied record did not grow with the corpus: {:.2} at {SMALL} against \
         {:.2} at {LARGE} -- if this ever stops holding, something has begun to bound the piece \
         count and this test should be rewritten around whatever that is",
        small_fixed_decline.paths_per_applied(),
        large_fixed_decline.paths_per_applied()
    );
    // What it is NOT is one listing per piece. The enumeration is a single `read_dir` at every
    // size and in both regimes, which is the reason the growth above is cheap per unit rather
    // than free.
    for (name, cost) in [
        ("small fixed / opening", &small_fixed_open),
        ("small fixed / declining", &small_fixed_decline),
        ("large fixed / opening", &large_fixed_open),
        ("large fixed / declining", &large_fixed_decline),
        ("small partial / declining", &small_part_decline),
        ("large partial / declining", &large_part_decline),
    ] {
        assert_eq!(
            cost.dir_listings, 1,
            "the {name} fold performed {} directory listings, expected exactly one",
            cost.dir_listings
        );
    }

    // ---------------------------------------------------------------------------------------
    // HALF SIX: THE RESIDUAL, from a total this file does not maintain.
    //
    // `rchar` is the kernel's count of bytes this process read across the fold; the attributed
    // row is the bytes of the pieces the fold opened, computed from the piece names BEFORE the
    // fold ran. What is left over is everything the rows do not account for, and on a store ten
    // times the size it must not climb. Taken PER RECORD so a fixed overhead divides away.
    // ---------------------------------------------------------------------------------------
    println!(
        "  residual (kernel rchar total minus the bytes of the pieces the fold opened): \
         small = {} - {} = {} ({:.5} a record) | large = {} - {} = {} ({:.5} a record)",
        small_fixed_decline.total_bytes_read,
        small_fixed_decline.attributed_bytes,
        small_fixed_decline.residual_bytes(),
        small_fixed_decline.residual_per_record(),
        large_fixed_decline.total_bytes_read,
        large_fixed_decline.attributed_bytes,
        large_fixed_decline.residual_bytes(),
        large_fixed_decline.residual_per_record(),
    );
    assert!(
        large_fixed_decline.residual_per_record()
            <= small_fixed_decline.residual_per_record().abs() + 1.0,
        "the unattributed bytes per record CLIMBED with the corpus: {:.5} at {SMALL} against \
         {:.5} at {LARGE} -- something reads the log that these rows do not account for",
        small_fixed_decline.residual_per_record(),
        large_fixed_decline.residual_per_record(),
    );
    assert!(
        large_part_decline.residual_per_record()
            <= small_part_decline.residual_per_record().abs() + 1.0,
        "the unattributed bytes per record CLIMBED with the corpus in the proportional regime: \
         {:.5} at {SMALL} against {:.5} at {LARGE}",
        small_part_decline.residual_per_record(),
        large_part_decline.residual_per_record(),
    );

    // ---------------------------------------------------------------------------------------
    // HALF SEVEN, LAST BECAUSE IT MUTATES THE LARGE FIXTURE: WHAT BOUNDS THE PIECE COUNT.
    //
    // A completed dump, and nothing else. One sweep at an anchor that reflects the whole log
    // takes the large fixture from every piece it holds down to the one being written -- so the
    // count is not bounded by a per-round cap or a ceiling on what the index may hold, it is
    // bounded by how recently a dump last finished. `min_reclaimable_bytes` is `u64::MAX` here,
    // which declines the REWRITE of the piece being written: the sealed pieces still go, which
    // is the point -- unlinking a whole piece is not subject to the rewrite threshold, so what
    // is measured is the sweep's own reach and not a byte budget.
    // ---------------------------------------------------------------------------------------
    let large_store = LocalIndexLogStore::new(large_dir.path());
    let pieces_before = large_store.piece_count(SHARD);
    let report = large_store
        .gc_reflected_before_anchor(SHARD, LARGE as u64, LARGE as u64 + 1, u64::MAX)
        .unwrap();
    let pieces_after = large_store.piece_count(SHARD);
    println!(
        "  what bounds the piece count: a completed dump and nothing else -- one sweep at a \
         reflecting anchor took {pieces_before} pieces to {pieces_after}, {} records to {}, {} \
         bytes to {}, copying {}",
        report.records_before,
        report.records_after,
        report.bytes_before,
        report.bytes_after,
        report.bytes_copied,
    );
    assert!(
        pieces_before >= 1_000,
        "the large fixture held {pieces_before} pieces before the sweep -- nothing to bound"
    );
    assert_eq!(
        pieces_after, 1,
        "the sweep left {pieces_after} pieces of {pieces_before}; a completed dump is supposed to \
         be able to take the log back to the piece being written in one round"
    );
    assert_eq!(
        report.bytes_copied, 0,
        "the sweep copied {} bytes -- the sealed pieces are supposed to go by unlink, so the \
         piece count is not bounded by a byte budget",
        report.bytes_copied
    );
}
