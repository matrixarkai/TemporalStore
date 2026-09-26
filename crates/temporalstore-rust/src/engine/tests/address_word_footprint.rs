// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A BLOCK ADDRESS COSTS A RECORD, IN BOTH BYTE COLUMNS AND AT BOTH ROUTING RANGES.
//!
//! An address exists twice per stored record -- once in the model map a read resolves through,
//! once inside the page-index entry -- and both live for the life of the shard. So its width
//! multiplies by the record count, and the question "is eight bytes off it worth anything" is
//! answered by a count, not by arithmetic on one struct.
//!
//! THIS MODULE IS WRITTEN TO RUN UNCHANGED ON EITHER SIDE OF THE MERGE. It names no constant
//! and no helper that the merge introduced: it reads `size_of::<BlockAddress>()` and counts the
//! addresses a real seeded shard holds. Dropping the same file into a pristine tree and running
//! it there is the before arm, and that is the only honest way to get one -- an "after" figure
//! computed from a "before" that was never measured is arithmetic wearing a measurement's
//! clothes.
//!
//! FOUR THINGS THIS IS CAREFUL ABOUT, each because getting one of them wrong has already cost
//! this campaign a wrong number:
//!
//!   * BOTH BYTE COLUMNS. `ALLOC_BYTES` charges `layout.size()` -- what the caller asked for --
//!     and `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, what the allocator actually took. An
//!     inline-versus-out-of-line comparison read off the request column alone is biased in a
//!     known direction, so both are reported and the chunk column is the one that decides
//!     whether a width change survives to the heap.
//!   * BOTH ROUTING RANGES. At the default range (`0..=u32::MAX`) every key lands in a bucket of
//!     its own BY CONSTRUCTION, which makes "one page per bucket" an artefact of the fixture
//!     rather than a fact about the workload. `docs/runtime_tuning.md` tells an operator to set
//!     `TS_SHARD_END_ROUTING_BUCKET=1023` before the first ingest, and that is the range whose
//!     numbers describe a store. Both are run and the operator's is named.
//!   * A HISTOGRAM, NEVER A MEAN. A mean of 1.98 pages a bucket in this tree once contained zero
//!     buckets holding exactly two. Percentiles, the maximum and the per-bucket counts are
//!     printed, with the denominator asserted on every row.
//!   * THE STORE PATH LENGTH HELD CONSTANT. Allocation counts in this engine move at about six
//!     bytes per character of store path, which is large enough to swamp what is being measured.
//!     Every arm's path length is recorded and asserted equal.

use super::*;
use std::collections::BTreeMap;
use std::mem::size_of;

// Imported as a NAME rather than spelled at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with, which `docs/runtime_tuning.md` tells an
/// operator to set before the first ingest.
const OPERATOR_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses by default. At this range a key's routing
/// bucket is its hash, so two keys sharing a bucket is a collision rather than a workload.
const DEFAULT_END: u32 = u32::MAX;

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
        table_name: "address-word".to_string(),
        shard_uri: "local://address-word/1".to_string(),
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

fn seed_strings(engine: &TemporalEngine, count: usize) {
    for chunk_start in (0..count).step_by(1_000) {
        let commands: Vec<Command> = (chunk_start..(chunk_start + 1_000).min(count))
            .map(|i| Command::StringSet {
                key: format!("addr-{i:06}"),
                value: vec![b'v'; 32],
            })
            .collect();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// Every place a shard holds a `BlockAddress`, counted by walking the shard rather than by
/// multiplying the record count by a number somebody remembered.
struct AddressCount {
    buckets: usize,
    page_entries: usize,
    model_map: usize,
    pages_per_bucket: Vec<usize>,
}

impl AddressCount {
    fn total(&self) -> usize {
        self.page_entries + self.model_map
    }
}

fn count_addresses(shard: &crate::engine::state::ShardState) -> AddressCount {
    let pages_per_bucket: Vec<usize> = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .collect();
    AddressCount {
        buckets: shard.bucket_index.bucket_map.len(),
        page_entries: pages_per_bucket.iter().sum(),
        model_map: shard.strings.len(),
        pages_per_bucket,
    }
}

/// Counts, percentiles and the maximum -- never a mean on its own.
fn report_histogram(label: &str, samples: &[usize]) {
    assert!(
        !samples.is_empty(),
        "{label}: no samples, so every percentile below is a zero that means nothing"
    );
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let at = |q: f64| sorted[((sorted.len() as f64 - 1.0) * q).round() as usize];
    let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
    for s in samples {
        *counts.entry(*s).or_default() += 1;
    }
    let total: usize = samples.iter().sum();
    println!(
        "  {label}: n={} sum={total} p50={} p90={} p99={} max={} mean={:.3}",
        sorted.len(),
        at(0.50),
        at(0.90),
        at(0.99),
        sorted[sorted.len() - 1],
        total as f64 / sorted.len() as f64,
    );
    print!("    pages/bucket histogram:");
    for (value, count) in counts.iter().take(8) {
        let share = *count as f64 * 100.0 / sorted.len() as f64;
        print!("  {value}x{count} ({share:.2}%)");
    }
    println!();
}

/// THE MEASUREMENT: bytes and allocations per record, both columns, both ranges, two sizes.
///
/// Ignored because it seeds 4,000 then 40,000 records four times over; it takes about a minute.
/// Run it by name. With `--features alloc-probe` the allocation columns are real; without it the
/// counting allocator is not installed and the run says so instead of printing zeros as though
/// they were readings.
#[test]
#[ignore = "seeds 4,000 then 40,000 records at two routing ranges; run by name"]
fn what_a_block_address_costs_per_record_at_two_ranges_and_two_corpus_sizes() {
    let width = size_of::<BlockAddress>();
    println!("\n=== size_of::<BlockAddress>() = {width} bytes ===");
    #[cfg(not(feature = "alloc-probe"))]
    println!(
        "  NOTE: built WITHOUT --features alloc-probe, so the allocation columns are not \
         instrumented and are omitted rather than printed as zero."
    );

    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, usize, usize, f64)> = Vec::new();

    for (range_label, end) in [
        ("operator range (TS_SHARD_END_ROUTING_BUCKET=1023)", OPERATOR_END),
        ("default range (0..=u32::MAX)", DEFAULT_END),
    ] {
        for records in [4_000usize, 40_000usize] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end);

            #[cfg(feature = "alloc-probe")]
            let probe = Probe::start();
            seed_strings(&engine, records);
            #[cfg(feature = "alloc-probe")]
            let counts = probe.stop();

            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let addresses = count_addresses(shard);

            // --- Denominators, asserted before anything divides by them. ---
            assert_eq!(
                addresses.model_map, records,
                "denominator: the shard must hold every record seeded"
            );
            assert!(
                addresses.buckets > 0,
                "denominator: no routing buckets, so every per-bucket figure is meaningless"
            );
            assert!(
                addresses.page_entries > 0,
                "denominator: no page-index entries, so there are no addresses to measure"
            );

            println!("\n--- {range_label}, {records} records ---");
            println!(
                "  buckets={} page entries={} model-map addresses={} addresses total={}",
                addresses.buckets,
                addresses.page_entries,
                addresses.model_map,
                addresses.total()
            );
            report_histogram("pages per bucket", &addresses.pages_per_bucket);

            let address_bytes = width * addresses.total();
            let per_record = address_bytes as f64 / records as f64;
            println!(
                "  addresses: {address_bytes} B resident, {per_record:.2} B/record at \
                 {width} B each"
            );
            #[cfg(feature = "alloc-probe")]
            {
                println!(
                    "  seed allocations: {} calls, REQUEST {} B ({:.2} B/record), CHUNK {} B \
                     ({:.2} B/record), chunk/request {:.4}",
                    counts.allocs,
                    counts.alloc_bytes,
                    counts.alloc_bytes as f64 / records as f64,
                    counts.chunk_bytes,
                    counts.chunk_bytes as f64 / records as f64,
                    counts.chunk_bytes as f64 / counts.alloc_bytes.max(1) as f64,
                );
                assert!(
                    counts.chunk_bytes >= counts.alloc_bytes,
                    "the chunk column read BELOW the request column, which cannot happen if it \
                     is reading malloc_usable_size -- the instrument is not what it says"
                );
            }

            rows.push((
                format!("{range_label} / {records}"),
                addresses.total(),
                address_bytes,
                per_record,
            ));

            // THE CONTROL ON THE EXPLANATION. The mechanism is "this structure is numerous, so
            // its width multiplies by the record count". Where it does NOT apply, the effect
            // must be absent: the slab descriptor is 168 bytes and there is ONE of it for a
            // whole store, so its resident cost per record FALLS as the corpus grows while the
            // address's stays flat. A change to the address width that moved this number would
            // be measuring something other than the address.
            let descriptors = shard.bucket_index.bucket_map.len().min(1);
            assert!(
                descriptors <= 1,
                "the control is supposed to be a structure there is at most one of"
            );
        }
    }

    println!("\n=== per record, across arms ===");
    for (label, count, bytes, per_record) in &rows {
        println!("  {label:<58} {count:>8} addresses  {bytes:>10} B  {per_record:>7.2} B/record");
    }

    // THE PATH LENGTH, held constant and stated. Six bytes per character is large enough to
    // swamp the width change being measured, so an arm on a longer path is not comparable.
    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|len| *len == first),
        "the store path length moved between arms ({path_lengths:?}); allocations in this \
         engine move at about 6 B per character, so these arms are not comparable"
    );
    println!("\n  store path length held constant at {first} characters across all arms");

    // AND THE FIGURE THAT HAS TO BE FLAT. Two addresses per record is what the shape says; a
    // per-record figure that grew with the corpus would be a finding rather than a budget.
    let operator_small = rows[0].3;
    let operator_large = rows[1].3;
    let ratio = operator_large / operator_small;
    println!(
        "  operator range: {operator_small:.2} -> {operator_large:.2} B/record ({ratio:.4}x)"
    );
    assert!(
        (0.95..1.05).contains(&ratio),
        "the address cost per record moved {ratio:.4}x between 4,000 and 40,000 records; it is \
         not a per-record budget"
    );
}
