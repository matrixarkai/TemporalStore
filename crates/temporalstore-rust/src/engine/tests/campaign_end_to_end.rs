// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE WHOLE CAMPAIGN SAVED, MEASURED AT BOTH ENDS ON ONE INSTRUMENT.
//!
//! Every figure published during the bucket-node campaign is a DELTA taken by one change against
//! its own immediate base, on its own fixture. Deltas do not add: two changes can each remove the
//! same byte, and a config default moved in the middle of the sequence can carry a saving that
//! reads as struct work. Nobody has taken a direct reading at each END of the sequence with one
//! workload and one configuration, and that is what this module does.
//!
//! This file is written to compile UNCHANGED at both ends of the campaign. It touches no field
//! name that moved and no enum payload that moved -- only `len()`, `matches!(.., Arm(..))`,
//! `size_of`, and the public command surface. A measurement whose two arms are two different
//! programs is not a measurement.
//!
//! # FOUR CELLS, NOT TWO
//!
//! The shipped routing-range default moved from the whole 32-bit keyspace to 1023 DURING the
//! sequence. A before/after taken at each tree's own default therefore charges a configuration
//! change to the struct work. The range is held as a parameter and both values are run at both
//! ends, so the column effect (the default) and the row effect (the structures) separate:
//!
//! ```text
//!                         0..u32::MAX        0..1023
//!     pre-campaign        cell A             cell B
//!     post-campaign       cell C             cell D
//! ```
//!
//! Down a column is what the structures bought at a fixed range. Across a row is what the range
//! bought at a fixed tree. A + D against A alone is the number that has been quoted, and it is the
//! two effects multiplied together with no way to tell which did the work.
//!
//! # THE NODE IS PER BUCKET AND THE ENTRY IS PER PAGE
//!
//! At the whole keyspace every key routes to a bucket of its own, so one node per bucket IS one
//! node per record and its width divides by one. At 1023 a bucket holds tens of pages and the same
//! node divides by the fill. The two structures therefore scale differently with the range, and a
//! single "bytes per record" line hides it. Both contributions are reported separately.
//!
//! # NEVER A MEAN
//!
//! Pages per bucket is a histogram with every row printed, percentiles counted off the histogram,
//! a MAX, and the denominator on the line. A mean of 1.98 pages a bucket was once published for a
//! store containing no bucket holding two.
//!
//! # THE CONTROL
//!
//! `the_range_buys_nothing_for_a_store_whose_pages_share_one_object_key` is the workload where the
//! column effect's mechanism predicts NOTHING: routing takes the object key and never the
//! component, so one key's many component pages sit in one bucket at every range. If the column
//! effect showed up there too, the explanation attributed to it here would be wrong.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The pre-campaign shipped default: the whole 32-bit keyspace.
const WIDE_END: u32 = u32::MAX;
/// The post-campaign shipped default.
const NARROW_END: u32 = 1023;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every store path is held at this many characters at every arm. Allocation bytes move at about
/// six bytes a character on this engine, so an arm at a different path length is a different
/// measurement wearing the same label.
const STORE_PATH_CHARS: usize = 15;

/// Base for the arm directories. Chosen so that base + a two-digit arm id is exactly
/// `STORE_PATH_CHARS` characters, which the helper below ASSERTS rather than trusting.
const ARM_BASE: &str = "/tmp/q8112dx";

fn arm_dir(arm: usize) -> std::path::PathBuf {
    assert!(arm < 100, "arm ids are two digits so the path length is fixed");
    let path = std::path::PathBuf::from(format!("{ARM_BASE}/{arm:02}"));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("arm directory");
    assert_eq!(
        STORE_PATH_CHARS,
        path.to_string_lossy().chars().count(),
        "the store path is {} characters at arm {arm} and {STORE_PATH_CHARS} everywhere else; \
         allocation bytes move at about six bytes a character, so an arm at a different length \
         cannot be compared with one at this length",
        path.to_string_lossy().chars().count()
    );
    path
}

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "campaign-end-to-end".to_string(),
        shard_uri: "local://campaign-end-to-end/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// The workload every cell is measured over: distinct routed string keys, one page each.
fn seed_commands(count: usize) -> (Vec<String>, Vec<Command>) {
    let keys: Vec<String> = (0..count).map(|i| format!("fill-{i:06}")).collect();
    let commands = keys
        .iter()
        .map(|key| Command::StringSet {
            key: key.clone(),
            value: vec![b'v'; 32],
        })
        .collect();
    (keys, commands)
}

fn read_back(engine: &TemporalEngine, keys: &[String]) -> usize {
    keys.iter()
        .filter(|key| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: (*key).clone(),
                },
            });
            response.status.ok
                && matches!(
                    &response.response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
        })
        .count()
}

fn range_label(end_routing_bucket: u32) -> String {
    if end_routing_bucket == WIDE_END {
        "0..u32::MAX".to_string()
    } else {
        format!("0..{end_routing_bucket}")
    }
}

// =============================================================================================
// THE DISTRIBUTION
// =============================================================================================

#[derive(Debug, Default, Clone)]
struct PagesPerBucket {
    counts: BTreeMap<usize, usize>,
}

impl PagesPerBucket {
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn pages(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn mean(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.pages() as f64 / buckets as f64
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn min(&self) -> usize {
        self.counts.keys().copied().next().unwrap_or_default()
    }

    /// Counted off the histogram, never interpolated: every value this can return is a page count
    /// that some bucket actually holds.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * fraction).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.max()
    }

    fn line(&self, label: &str) {
        println!(
            "    {label:<40} buckets {:>6} pages {:>6} | mean {:>8.3} min {:>4} p50 {:>4} \
             p90 {:>4} p99 {:>4} MAX {:>4}",
            self.buckets(),
            self.pages(),
            self.mean(),
            self.min(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
    }

    /// Every row, and the rows are ASSERTED to sum to the denominator. A histogram whose printed
    /// rows do not add up to its own denominator is not one.
    fn report(&self, label: &str) {
        self.line(label);
        let buckets = self.buckets();
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, buckets,
            "{label}: rows sum to {printed} over a denominator of {buckets}"
        );
        if self.counts.len() > 24 {
            println!(
                "        {} distinct page counts; printing the sixteen widest and the sixteen \
                 narrowest over a denominator of {buckets}",
                self.counts.len()
            );
            let rows: Vec<(usize, usize)> =
                self.counts.iter().map(|(h, c)| (*h, *c)).collect();
            for (held, count) in rows.iter().take(16) {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
            println!("        ...");
            for (held, count) in rows.iter().rev().take(16).rev() {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
        } else {
            for (held, count) in &self.counts {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
        }
    }
}

fn pages_per_bucket(engine: &TemporalEngine) -> PagesPerBucket {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = PagesPerBucket::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        if bucket.block_index.is_empty() {
            continue;
        }
        *hist.counts.entry(bucket.block_index.len()).or_default() += 1;
    }
    hist
}

/// (Empty, One, Many) arm counts over the whole bucket map -- released and empty buckets included,
/// because a node exists whether or not its bucket holds a page and a node is what the per-bucket
/// contribution is charged on.
fn block_index_arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut arms = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            crate::engine::state::BlockIndexMap::Empty => arms.0 += 1,
            crate::engine::state::BlockIndexMap::One(..) => arms.1 += 1,
            crate::engine::state::BlockIndexMap::Many(..) => arms.2 += 1,
        }
    }
    arms
}

/// Nodes in the bucket map, whether or not their bucket holds a page.
fn node_count(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard.bucket_index.bucket_map.len()
}

// =============================================================================================
// WIDTHS AND STRIDES -- NO FEATURE NEEDED, SO THIS RUNS AT BOTH ENDS UNCONDITIONALLY
// =============================================================================================

/// THE WIDTHS AT THIS END, READ WITH `size_of` RATHER THAN QUOTED, AND THE STRIDE BESIDE THEM.
///
/// A struct width is not what memory costs. The page list holds PAIRS, and a pair re-aligns to the
/// wider of its members' alignments and pads to a multiple of it -- so the number that multiplies
/// by the page count is `size_of::<(u64, BlockIndex)>()` and not `size_of::<BlockIndex>()`. A
/// narrowing that takes the entry below the next multiple of its alignment buys nothing at all in
/// the container, and only the stride can say so.
///
/// This test ASSERTS NOTHING about particular numbers on purpose: it has to pass at both ends of
/// the campaign, where every one of these is different, and an assertion pinning either end would
/// make the other end red instead of measured.
///
/// rust-internal: reads `size_of` on the engine's own structures, no product behaviour
#[test]
fn the_widths_and_the_container_stride_at_this_end_of_the_campaign() {
    use crate::engine::state::{BlockIndex, BlockIndexMap, BucketNode};
    use std::mem::{align_of, size_of};

    let node = size_of::<BucketNode>();
    let entry = size_of::<BlockIndex>();
    let map = size_of::<BlockIndexMap>();
    let address = size_of::<crate::BlockAddress>();
    let stride = size_of::<(u64, BlockIndex)>();

    println!("WIDTHS AT THIS END");
    println!(
        "  BucketNode      {node:>4} B  (align {})   PER BUCKET",
        align_of::<BucketNode>()
    );
    println!(
        "  BlockIndex      {entry:>4} B  (align {})   PER PAGE",
        align_of::<BlockIndex>()
    );
    println!(
        "  BlockIndexMap   {map:>4} B  (align {})   PER BUCKET, inside the node",
        align_of::<BlockIndexMap>()
    );
    println!(
        "  BlockAddress    {address:>4} B  (align {})   PER PAGE, inside the entry",
        align_of::<crate::BlockAddress>()
    );
    println!("CONTAINER STRIDE");
    println!(
        "  (u64, BlockIndex) {stride:>4} B  (align {})  <- THIS is what multiplies by the page \
         count, not {entry}",
        align_of::<(u64, BlockIndex)>()
    );
    println!(
        "  the pair costs {} B more than the entry alone: the key word plus {} B of tail padding",
        stride - entry,
        stride - entry - size_of::<u64>()
    );

    // The one thing that is true at both ends and worth pinning: the stride is at least the entry
    // plus its key, and it is a multiple of the pair's alignment. A stride that came out smaller
    // than its own members would mean the reading is not a stride.
    assert!(
        stride >= entry + size_of::<u64>(),
        "the pair stride {stride} B is smaller than the entry {entry} B plus its key word, which \
         cannot be a stride"
    );
    assert_eq!(
        0,
        stride % align_of::<(u64, BlockIndex)>(),
        "the pair stride {stride} B is not a multiple of the pair's own alignment"
    );
}

// =============================================================================================
// THE INSTRUMENT ITSELF -- BOTH COLUMNS, AND PROOF THE CHUNK COLUMN IS NOT A COPY
// =============================================================================================

/// THE CHUNK COLUMN READS THE ALLOCATOR AT THIS END TOO, AND IS NOT A COPY OF THE REQUEST COLUMN.
///
/// The chunk counter was added late in the campaign. To read both ends on one instrument it has to
/// be present at both, and a counter that quietly degraded to echoing the request would make the
/// pre-campaign chunk column a copy of its request column -- which would under-charge every
/// out-of-line shape on exactly one side of the comparison and in the direction that flatters the
/// later tree. So a size whose chunk is KNOWN and different from its request is planted here and
/// the difference is recovered. This test is the licence to print a chunk column at this end.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn the_chunk_column_reads_the_allocator_at_this_end_and_is_not_a_copy_of_the_request() {
    const REQUEST: usize = 100;
    const PLANTED: usize = 64;
    let expected = crate::alloc_probe::documented_glibc_chunk(REQUEST);
    assert_ne!(
        REQUEST, expected,
        "the planted size must be one the allocator rounds, or this control cannot tell a chunk \
         reading from a request reading"
    );

    let probe = Probe::start();
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(PLANTED);
    for _ in 0..PLANTED {
        held.push(vec![0xA5u8; REQUEST]);
    }
    let counts = probe.stop();
    std::hint::black_box(&held);

    let over = counts.chunk_bytes as i64 - counts.alloc_bytes as i64;
    println!(
        "planted {PLANTED} x {REQUEST} B: request {} B, chunk {} B, over {over} B (documented \
         chunk for {REQUEST} B is {expected} B)",
        counts.alloc_bytes, counts.chunk_bytes
    );
    assert!(
        counts.chunk_bytes > counts.alloc_bytes,
        "the chunk column read {} B against the request column's {} B, so at this end it is a \
         copy of the request column and the two ends of this campaign are two instruments",
        counts.chunk_bytes,
        counts.alloc_bytes
    );
    assert!(
        over >= (PLANTED * (expected - REQUEST)) as i64,
        "the chunk column read {over} B over the request column for {PLANTED} planted requests \
         whose documented rounding is {} B each; the chunk rule is a FLOOR, so a reading BELOW \
         the floor means the counter is not reading the allocator",
        expected - REQUEST
    );
}

// =============================================================================================
// THE FOUR CELLS
// =============================================================================================

#[derive(Debug, Clone, Copy)]
struct Cell {
    records: usize,
    end_routing_bucket: u32,
    allocs: u64,
    request_bytes: u64,
    chunk_bytes: u64,
    freed_bytes: u64,
    nodes: usize,
    pages: usize,
    occupied: usize,
}

impl Cell {
    fn request_per_record(&self) -> f64 {
        self.request_bytes as f64 / self.records as f64
    }
    fn chunk_per_record(&self) -> f64 {
        self.chunk_bytes as f64 / self.records as f64
    }
    fn retained_per_record(&self) -> f64 {
        (self.request_bytes as i64 - self.freed_bytes as i64) as f64 / self.records as f64
    }
    fn allocs_per_record(&self) -> f64 {
        self.allocs as f64 / self.records as f64
    }
    /// The per-BUCKET structure's share, per record: one node per node, divided by records.
    fn node_bytes_per_record(&self, node_width: usize) -> f64 {
        (self.nodes * node_width) as f64 / self.records as f64
    }
    /// The per-PAGE structure's share, per record: one container slot per page.
    fn page_bytes_per_record(&self, stride: usize) -> f64 {
        (self.pages * stride) as f64 / self.records as f64
    }
}

#[cfg(feature = "alloc-probe")]
fn measure_cell(arm: usize, records: usize, end_routing_bucket: u32) -> (Cell, PagesPerBucket) {
    let dir = arm_dir(arm);
    let engine = engine_on(&dir);
    load_on(&engine, end_routing_bucket);

    // Built OUTSIDE the probe span. The command vector is the fixture's own cost, not the
    // engine's, and charging it to the engine would put the same constant in every cell and
    // flatten every ratio towards one.
    let (keys, commands) = seed_commands(records);

    let probe = Probe::start();
    run_batch(&engine, commands);
    let counts = probe.stop();

    // NON-VACUITY. A cell that wrote nothing allocates nothing, and "fewer bytes per record" over
    // a store that holds no record is the cheapest wrong answer available.
    let served = read_back(&engine, &keys);
    assert_eq!(
        records, served,
        "the cell on 0..{end_routing_bucket} at {records} records served {served} of them back; a \
         store that does not hold what it claims is not a measurement of holding it"
    );

    let hist = pages_per_bucket(&engine);
    let nodes = node_count(&engine);
    assert!(
        hist.pages() >= records,
        "the cell on 0..{end_routing_bucket} at {records} records filed {} pages; the fixture did \
         not reach the population it claims",
        hist.pages()
    );

    // THE POPULATION THE RANGE IS SUPPOSED TO PRODUCE, ASSERTED. A fixture that put every key in
    // its own bucket regardless of the range would make the column effect an artefact of the
    // fixture -- which is exactly how one earlier measurement in this campaign went wrong.
    if end_routing_bucket == WIDE_END {
        assert_eq!(
            records,
            hist.buckets(),
            "on the whole keyspace {records} distinct keys occupied {} buckets; the whole point \
             of this arm is that every key gets a bucket of its own",
            hist.buckets()
        );
    } else {
        let ceiling = end_routing_bucket as usize + 1;
        assert!(
            hist.buckets() <= ceiling,
            "on 0..{end_routing_bucket} the store occupied {} buckets, more than the {ceiling} \
             the range can address",
            hist.buckets()
        );
        assert!(
            hist.buckets() * 4 >= ceiling * 3,
            "on 0..{end_routing_bucket} only {} of {ceiling} addressable buckets were occupied by \
             {records} keys; the fill this arm is measuring did not happen",
            hist.buckets()
        );
    }

    let cell = Cell {
        records,
        end_routing_bucket,
        allocs: counts.allocs,
        request_bytes: counts.alloc_bytes,
        chunk_bytes: counts.chunk_bytes,
        freed_bytes: counts.free_bytes,
        nodes,
        pages: hist.pages(),
        occupied: hist.buckets(),
    };
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
    (cell, hist)
}

/// THE WHOLE CAMPAIGN AS A DIRECT READING AT THIS END, WITH THE ROUTING DEFAULT HELD AS A
/// PARAMETER SO THE CONFIG CHANGE AND THE STRUCT CHANGES DO NOT MASQUERADE AS EACH OTHER.
///
/// Run this at the parent of the campaign's first commit and at its head, and the four cells fall
/// out: across a row is the range, down a column is the structures. There is no assertion here
/// comparing the two trees -- a test cannot see the other tree -- so the cross-tree numbers are
/// PRINTED and the within-tree relationships are asserted.
///
/// rust-internal: reads the engine's own bucket index and the counting allocator, no product
/// behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn the_four_cells_of_the_campaign_on_one_instrument_at_two_corpus_sizes() {
    use crate::engine::state::{BlockIndex, BucketNode};
    use std::mem::size_of;

    let node_width = size_of::<BucketNode>();
    let stride = size_of::<(u64, BlockIndex)>();
    println!(
        "THIS END: BucketNode {node_width} B per bucket, (u64, BlockIndex) stride {stride} B per \
         page, store path {STORE_PATH_CHARS} characters"
    );

    let mut arm = 10usize;
    let mut cells: Vec<(Cell, PagesPerBucket)> = Vec::new();
    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            arm += 1;
            let (cell, hist) = measure_cell(arm, records, end_routing_bucket);
            cells.push((cell, hist));
        }
    }

    println!();
    println!("PER-RECORD BYTES AND ALLOCATIONS, ONE ROW A CELL");
    println!(
        "  {:<9} {:<12} {:>9} {:>9} {:>9} {:>8} {:>8} {:>8} {:>9}",
        "records", "range", "req B/rec", "chk B/rec", "held B/rec", "all/rec", "buckets", "pages",
        "nodes"
    );
    for (cell, _) in &cells {
        println!(
            "  {:<9} {:<12} {:>9.2} {:>9.2} {:>10.2} {:>8.3} {:>8} {:>8} {:>9}",
            cell.records,
            range_label(cell.end_routing_bucket),
            cell.request_per_record(),
            cell.chunk_per_record(),
            cell.retained_per_record(),
            cell.allocs_per_record(),
            cell.occupied,
            cell.pages,
            cell.nodes,
        );
    }

    println!();
    println!("WHERE THE STRUCTURES LAND, PER RECORD -- THE TWO DIVIDE DIFFERENTLY");
    println!(
        "  {:<9} {:<12} {:>14} {:>14} {:>12} {:>12}",
        "records", "range", "node B/rec", "page B/rec", "pages/bkt", "keys/bkt"
    );
    for (cell, hist) in &cells {
        println!(
            "  {:<9} {:<12} {:>14.3} {:>14.3} {:>12.3} {:>12.3}",
            cell.records,
            range_label(cell.end_routing_bucket),
            cell.node_bytes_per_record(node_width),
            cell.page_bytes_per_record(stride),
            hist.mean(),
            cell.records as f64 / cell.occupied.max(1) as f64,
        );
    }

    println!();
    println!("THE DISTRIBUTIONS, EVERY ROW, WITH THE DENOMINATOR ON THE LINE");
    for (cell, hist) in &cells {
        hist.report(&format!(
            "{} records on {}",
            cell.records,
            range_label(cell.end_routing_bucket)
        ));
    }

    println!();
    println!("WITHIN THIS TREE: WHAT NARROWING THE RANGE BOUGHT, AT EACH CORPUS SIZE");
    for records in [SMALL, LARGE] {
        let wide = cells
            .iter()
            .find(|(c, _)| c.records == records && c.end_routing_bucket == WIDE_END)
            .expect("wide cell ran")
            .0;
        let narrow = cells
            .iter()
            .find(|(c, _)| c.records == records && c.end_routing_bucket == NARROW_END)
            .expect("narrow cell ran")
            .0;
        let req = narrow.request_per_record() / wide.request_per_record();
        let chk = narrow.chunk_per_record() / wide.chunk_per_record();
        let alc = narrow.allocs_per_record() / wide.allocs_per_record();
        println!(
            "  {records:>6} records: request {req:.4}x  chunk {chk:.4}x  allocations {alc:.4}x  \
             (0..1023 against 0..u32::MAX)"
        );

        // THE CHUNK RULE IS A FLOOR, NOT AN EQUALITY.
        for cell in [wide, narrow] {
            assert!(
                cell.chunk_bytes >= cell.request_bytes,
                "on 0..{} at {} records the chunk column read {} B under the request column's \
                 {} B; the allocator cannot hand over less than it was asked for",
                cell.end_routing_bucket,
                cell.records,
                cell.chunk_bytes,
                cell.request_bytes
            );
        }

        // The mechanism: narrowing groups keys, so there are fewer nodes for the same pages.
        assert!(
            narrow.nodes < wide.nodes,
            "narrowing to 0..{NARROW_END} left {} nodes against the whole keyspace's {}; if the \
             range did not group keys then nothing below is attributable to it",
            narrow.nodes,
            wide.nodes
        );
        assert_eq!(
            wide.pages, narrow.pages,
            "the two ranges filed {} and {} pages for the same {records} records; the range is \
             supposed to move where a page is filed and never how many there are, and a row \
             comparison over two different page counts is not one",
            wide.pages, narrow.pages
        );
    }
}

/// THE PER-CELL ARM SPLIT, SEPARATELY, BECAUSE THE ARM A BUCKET SITS IN IS WHAT DECIDES WHETHER
/// ITS PAGE IS HELD INLINE OR BEHIND A POINTER -- AND THAT IS THE ONE THING THE REQUEST COLUMN
/// CANNOT SEE.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
fn the_arm_a_bucket_sits_in_at_each_range_and_corpus_size() {
    let mut arm = 40usize;
    println!(
        "  {:<9} {:<12} {:>8} {:>8} {:>8} {:>10}",
        "records", "range", "Empty", "One", "Many", "nodes"
    );
    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            arm += 1;
            let dir = arm_dir(arm);
            let engine = engine_on(&dir);
            load_on(&engine, end_routing_bucket);
            let (keys, commands) = seed_commands(records);
            run_batch(&engine, commands);
            let served = read_back(&engine, &keys);
            assert_eq!(records, served, "the arm split's fixture must hold what it claims");
            let arms = block_index_arms(&engine);
            let nodes = node_count(&engine);
            assert_eq!(
                nodes,
                arms.0 + arms.1 + arms.2,
                "the arm counts sum to {} over a node population of {nodes}",
                arms.0 + arms.1 + arms.2
            );
            println!(
                "  {records:<9} {:<12} {:>8} {:>8} {:>8} {:>10}",
                range_label(end_routing_bucket),
                arms.0,
                arms.1,
                arms.2,
                nodes
            );
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

// =============================================================================================
// THE CONTROL
// =============================================================================================

/// THE RANGE BUYS NOTHING FOR A STORE WHOSE PAGES SHARE ONE OBJECT KEY -- AGAINST A NOISE BAND
/// THIS CONTROL MEASURES FIRST, IN ABBA ORDER.
///
/// The column effect is attributed to one mechanism: the range width is the modulus, so narrowing
/// it groups distinct OBJECT KEYS into shared buckets and amortises the per-bucket node. This is
/// the workload where that mechanism predicts NOTHING -- routing takes the object key and never
/// the component, so one key's many component pages sit in a single bucket at every range.
///
/// A FIRST DRAFT OF THIS CONTROL COMPARED ONE ARM AGAINST ONE ARM and read -2.95% of request
/// bytes, with the allocation COUNT apart by 8 in 110,629. Eight allocations cannot carry 189 kB.
/// A shift that large behind a count that flat is one or two large buffer growths landing in one
/// run and not the other -- a quantum of the instrument, not an effect of the knob. A control with
/// no noise band cannot tell those apart, and a control that cannot tell them apart is not one.
///
/// So each arm is run TWICE in ABBA order -- WIDE, NARROW, NARROW, WIDE -- which cancels a
/// monotone order effect (warm caches, a grown arena) rather than hoping interleaving did.
/// The band is the spread WITHIN each arm; the effect is the spread BETWEEN them; and the
/// assertion is that the between-arm shift does not exceed the band the instrument itself showed.
/// That bound comes from the data.
///
/// THE STRUCTURAL FIGURES ARE HELD TO 0.00% EXACTLY, with no band at all: bucket count, page count
/// and widest bucket are counts of what the engine filed, they are not subject to a buffer
/// quantum, and if the range moved any of them the mechanism this module attributes the column
/// effect to would be the wrong mechanism.
///
/// rust-internal: reads the engine's own bucket index and the counting allocator, no product
/// behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn the_range_buys_nothing_for_a_store_whose_pages_share_one_object_key() {
    const MEMBERS: usize = 2_000;
    /// WIDE, NARROW, NARROW, WIDE. Each arm appears twice, and the second half is the mirror of
    /// the first, so anything that drifts monotonically through the run lands equally on both.
    const ABBA: [u32; 4] = [WIDE_END, NARROW_END, NARROW_END, WIDE_END];

    let mut arm = 70usize;
    // (range, buckets, pages, widest, request, chunk, allocations)
    let mut runs: Vec<(u32, usize, usize, usize, u64, u64, u64)> = Vec::new();

    for end_routing_bucket in ABBA {
        arm += 1;
        let dir = arm_dir(arm);
        let engine = engine_on(&dir);
        load_on(&engine, end_routing_bucket);
        let commands: Vec<Command> = (0..MEMBERS)
            .map(|f| Command::HashSet {
                key: "one-container".to_string(),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            })
            .collect();
        let probe = Probe::start();
        run_batch(&engine, commands);
        let counts = probe.stop();
        let hist = pages_per_bucket(&engine);
        println!(
            "  run {} on {:<12} buckets {:>4} pages {:>6} MAX {:>6} | request {:>10} B chunk \
             {:>10} B allocations {:>8}",
            runs.len() + 1,
            range_label(end_routing_bucket),
            hist.buckets(),
            hist.pages(),
            hist.max(),
            counts.alloc_bytes,
            counts.chunk_bytes,
            counts.allocs,
        );
        runs.push((
            end_routing_bucket,
            hist.buckets(),
            hist.pages(),
            hist.max(),
            counts.alloc_bytes,
            counts.chunk_bytes,
            counts.allocs,
        ));
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // NON-VACUITY BEFORE ANYTHING ELSE. "No effect" over a store that wrote nothing is the
    // cheapest false control available, and so is one whose components never shared a bucket.
    for (range, buckets, pages, widest, ..) in &runs {
        assert!(
            *pages >= MEMBERS,
            "an arm on 0..{range} filed {pages} pages for {MEMBERS} components"
        );
        assert_eq!(
            1, *buckets,
            "an arm on 0..{range} occupied {buckets} buckets, so its components did not share a \
             bucket and this is not the shape the control names"
        );
        assert!(
            *widest > 1,
            "an arm on 0..{range} has a widest bucket of {widest} page, so nothing was grouped"
        );
    }

    // THE STRUCTURAL PREDICTION, AT 0.00% EXACTLY.
    let first = (runs[0].1, runs[0].2, runs[0].3);
    for (i, run) in runs.iter().enumerate() {
        assert_eq!(
            first,
            (run.1, run.2, run.3),
            "run {} on 0..{} filed {} buckets / {} pages / widest {} against run 1's {:?}; \
             routing takes the object key and never the component, so the range cannot split one \
             key -- if it did here, the mechanism the column effect is attributed to is not the \
             mechanism",
            i + 1,
            run.0,
            run.1,
            run.2,
            run.3,
            first
        );
    }
    println!(
        "  CONTROL structure: every one of the {} arms filed {:?} (buckets, pages, widest) -- \
         0.00%, exactly, with no band",
        runs.len(),
        first
    );

    // THE BAND, MEASURED FIRST: how far two runs of the SAME range sit apart.
    let wide: Vec<&(u32, usize, usize, usize, u64, u64, u64)> =
        runs.iter().filter(|r| r.0 == WIDE_END).collect();
    let narrow: Vec<&(u32, usize, usize, usize, u64, u64, u64)> =
        runs.iter().filter(|r| r.0 == NARROW_END).collect();
    assert_eq!(2, wide.len(), "ABBA must give two runs of the wide arm");
    assert_eq!(2, narrow.len(), "ABBA must give two runs of the narrow arm");

    for (label, pick) in [
        ("request bytes", 0usize),
        ("chunk bytes", 1usize),
        ("allocations", 2usize),
    ] {
        let get = |r: &(u32, usize, usize, usize, u64, u64, u64)| -> f64 {
            match pick {
                0 => r.4 as f64,
                1 => r.5 as f64,
                _ => r.6 as f64,
            }
        };
        let w: Vec<f64> = wide.iter().map(|r| get(r)).collect();
        let n: Vec<f64> = narrow.iter().map(|r| get(r)).collect();
        let base = (w[0] + w[1]) / 2.0;
        let within_wide = 100.0 * (w[1] - w[0]).abs() / base;
        let within_narrow = 100.0 * (n[1] - n[0]).abs() / base;
        let band = within_wide.max(within_narrow);
        let between = 100.0 * ((n[0] + n[1]) / 2.0 - base) / base;
        println!(
            "  CONTROL {label:<14} band: same-range spread wide {within_wide:.4}% narrow \
             {within_narrow:.4}% -> {band:.4}% | between ranges {between:+.4}%"
        );
        assert!(
            between.abs() <= band.max(0.000_1),
            "on a store whose pages all share one object key the range moved {label} by \
             {between:+.4}%, which is OUTSIDE the {band:.4}% band two runs of the SAME range \
             showed. A shift larger than the instrument's own spread on a workload where the \
             range can group nothing is a second channel, and the column effect measured in this \
             module is then not the grouping alone that it is attributed to"
        );
    }
}

// =============================================================================================
// WHAT AN OPERATOR PAYS: PEAK RSS, ONE CELL A PROCESS
// =============================================================================================

fn proc_status_kb(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let rest = rest.trim_start_matches(':').trim();
            if let Some(number) = rest.split_whitespace().next() {
                return number.parse().unwrap_or_default();
            }
        }
    }
    0
}

/// ONE CELL, ONE PROCESS, SO `VmHWM` MEANS THIS CELL.
///
/// `VmHWM` is a process-lifetime high-water mark: several cells in one process report the widest
/// of them four times. So each cell gets its own `#[test]` and is run with `--exact`, and the
/// baseline reading taken before the seed is printed beside the peak so the store's own share is
/// visible rather than buried under the harness's.
///
/// Allocator bytes are what this campaign optimised; RSS is what an operator pays, and the two can
/// disagree -- freed bytes the allocator retains are RSS and are not live data.
fn rss_cell(arm: usize, records: usize, end_routing_bucket: u32) {
    let baseline_rss = proc_status_kb("VmRSS");
    let baseline_hwm = proc_status_kb("VmHWM");
    let dir = arm_dir(arm);
    let engine = engine_on(&dir);
    load_on(&engine, end_routing_bucket);
    let (keys, commands) = seed_commands(records);
    run_batch(&engine, commands);
    let served = read_back(&engine, &keys);
    assert_eq!(records, served, "the RSS cell must hold what it claims");
    let after_rss = proc_status_kb("VmRSS");
    let after_hwm = proc_status_kb("VmHWM");
    let hist = pages_per_bucket(&engine);
    println!(
        "RSS CELL {records} records on {} | baseline VmRSS {baseline_rss} kB VmHWM \
         {baseline_hwm} kB | after VmRSS {after_rss} kB VmHWM {after_hwm} kB | delta RSS {} kB \
         = {:.2} B/record | peak {} kB = {:.2} B/record | buckets {} pages {}",
        range_label(end_routing_bucket),
        after_rss as i64 - baseline_rss as i64,
        1024.0 * (after_rss as f64 - baseline_rss as f64) / records as f64,
        after_hwm,
        1024.0 * (after_hwm as f64 - baseline_hwm as f64) / records as f64,
        hist.buckets(),
        hist.pages(),
    );
    assert!(
        after_hwm >= baseline_hwm,
        "a high-water mark cannot fall; this reading is not one"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// rust-internal: reads this process's own RSS, no product behaviour
#[test]
fn rss_small_corpus_whole_keyspace() {
    rss_cell(81, SMALL, WIDE_END);
}

/// rust-internal: reads this process's own RSS, no product behaviour
#[test]
fn rss_small_corpus_narrow_range() {
    rss_cell(82, SMALL, NARROW_END);
}

/// rust-internal: reads this process's own RSS, no product behaviour
#[test]
fn rss_large_corpus_whole_keyspace() {
    rss_cell(83, LARGE, WIDE_END);
}

/// rust-internal: reads this process's own RSS, no product behaviour
#[test]
fn rss_large_corpus_narrow_range() {
    rss_cell(84, LARGE, NARROW_END);
}
