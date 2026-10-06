// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! PRICING A HANDLE FOR EACH OF THE THREE NAMES A PAGE ENTRY CARRIES, one name at a time.
//!
//! #1959 opened `BlockIndex` at 104 bytes and called the three names "48 bytes, the largest
//! group". `page_entry_names` then measured the premise under that and found HALF of it false: an
//! object's pages already hold ONE allocation of its key, so the 16 bytes an entry spends on a
//! name is the FAT POINTER and the payload is already shared. A handle therefore saves at most 12
//! bytes of field per page, not 16 -- and an interning table COSTS an entry per distinct name,
//! which is an allocation, so that cost lands on the chunk column where the saving does not.
//!
//! THE THREE NAMES ARE THREE DIFFERENT ANSWERS and a blended number would hide it:
//!
//!   * `model_id` is drawn from a CLOSED SET this engine spells itself -- fifteen live kinds and
//!     two retired spellings, derived in `model_kind_registry` from the walk that is its
//!     authority. A closed set of seventeen needs no table at all: the handle IS the discriminant
//!     and the name is a `&'static str`. TAKEN, and this module states what it cost.
//!   * `object_key` is per-OBJECT, so a handle's table costs one entry per object and pays once
//!     per PAGE. Whether it pays is therefore a property of the pages-per-OBJECT distribution and
//!     of nothing else, and that distribution is measured here -- as a histogram with percentiles
//!     and a MAX, never a mean, because a mean of 1.98 pages a bucket in this engine once
//!     contained ZERO buckets holding two.
//!   * `component` is TWO POPULATIONS, and this module was written expecting one. The reasoning
//!     was that a container key's fields are each their own page and the component is the only
//!     thing telling them apart, so distinct component names must grow with the pages -- true
//!     WITHIN one object and false ACROSS a store, because a schema repeats: forty hashes sharing
//!     a hundred field names hold 400 distinct components over 4,000 pages. The anti-vacuity
//!     assertion caught that and refused to score, so the verdict is stated per population. A
//!     repeated SCHEMA name pays; a CONTENT-DERIVED one -- which is what `zset_component` and
//!     `timestamped_component` build -- does not, and cannot be replaced by an ordinal at all
//!     without a second map to get back.
//!
//! AND ONE THING THE NAMES TURN OUT TO BE THAT IS WORTH MORE THAN THE POINTER. A component name is
//! not a label -- `zset_component` is `format!("{biased:016x}{}", hex::encode(member))` and
//! `timestamped_component` is `format!("{stored_key:016x}{identity:016x}")`. They are HEX TEXT FOR
//! NUMBERS: sixteen characters carrying eight bytes, and `hex::encode` is exactly twice its input.
//! Measured here at 81.3% of component text in a container corpus and 97.1% with twenty-byte
//! members -- separately from the pointer, because it is a bigger term and it needs no table, no
//! ordinal and no map.
//!
//! WHAT SHIPPED IS `model_id` AND NOTHING ELSE. The other two names are priced here and declined,
//! and the prices are the point: a proposal declined with a measured number is worth as much as one
//! taken, and this is the third time in this campaign that the number came out the opposite way
//! round from the reasoning that motivated it.
//!
//! WHAT IS MEASURED WHERE. Counts and structure sizes are unconditional. The table's own cost is
//! only visible on the chunk column, so those arms are `alloc-probe` gated and named in the report
//! -- `ALLOC_BYTES` charges `layout.size()` and would under-charge a table systematically.
//! #1969 corrected the chunk rule to a FLOOR rather than an equality: a 104-byte request read 128
//! against a documented 112, because glibc serves from a chunk that is merely big enough.
//!
//! BOTH ROUTING RANGES, and the default moved. #1973 took `TS_SHARD_END_ROUTING_BUCKET` from the
//! whole keyspace to 1023 because the old default gave every key a bucket of its own BY
//! CONSTRUCTION. Pages per BUCKET is not pages per OBJECT and this module's subject is the second,
//! which is range-INDEPENDENT -- so both ranges are measured anyway, and their agreement is the
//! evidence for that independence rather than an assumption about it.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::control::LoadShardRequest;
use crate::engine::state::{BlockIndex, BlockRefs, ComponentBlocks, ComponentList, ObjectBlockRefs};
use crate::engine::storage_bucket_internals::StoredModelKind;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe s full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The range a production shard is loaded on: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const OPERATOR_END: u32 = 1023;
/// The range `load_shard` hard-codes, and what #1958 and #1959 were measured on.
const WHOLE_KEYSPACE_END: u32 = u32::MAX;

/// An `Arc<str>`'s own allocation: the text behind two words of strong/weak count.
const ARC_HEADER_BYTES: usize = 16;

// =================================================================================================
// FIXTURE. The same shapes `page_entry_names` seeds, so the two modules' numbers are comparable.
// =================================================================================================

fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ))
}

/// Load shard 1 over an EXPLICIT range. No arm here takes the default implicitly.
fn load_shard_over(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        response.status.ok,
        "shard must load over 0..={end_routing_bucket}: {:?}",
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

/// Keys that route one to a bucket and hold ONE page each: the population a handle loses on.
fn seed_routed_keys(engine: &TemporalEngine, records: usize) {
    run_batch(
        engine,
        (0..records)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Container keys: every element is its own page under ONE object key. The population a handle
/// wins on, and the one whose component names are hex.
fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        match k % 4 {
            0 => {
                for f in 0..members {
                    commands.push(Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: vec![b'v'; 32],
                    });
                }
            }
            1 => {
                for m in 0..members {
                    commands.push(Command::SetAdd {
                        key: format!("t{k}"),
                        member: format!("m{m}").into_bytes(),
                    });
                }
            }
            2 => {
                for m in 0..members {
                    commands.push(Command::ZSetAdd {
                        key: format!("z{k}"),
                        member: format!("m{m}").into_bytes(),
                        score: m as f64,
                    });
                }
            }
            _ => {
                for m in 0..members {
                    commands.push(Command::ListPush {
                        key: format!("l{k}"),
                        member: format!("m{m}").into_bytes(),
                        left: false,
                    });
                }
            }
        }
    }
    run_batch(engine, commands);
}

/// Containers whose members are WIDE, so the doubling in `hex::encode` is visible.
///
/// A set member is stored as `hex::encode(member)` and a zset member as `{biased:016x}` followed
/// by the same, so a twenty-byte member becomes a forty- or fifty-six-character component name.
/// Hash fields are seeded alongside deliberately: a field is the caller's own short text, and it is
/// the population the classifier below has to NOT match.
fn seed_wide_members(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members * 3);
    for k in 0..keys {
        for m in 0..members {
            // Twenty bytes, distinct.
            let member = format!("member-{m:013}").into_bytes();
            assert_eq!(20, member.len(), "the fixture's member width is part of the measurement");
            commands.push(Command::SetAdd {
                key: format!("wt{k}"),
                member: member.clone(),
            });
            commands.push(Command::ZSetAdd {
                key: format!("wz{k}"),
                member,
                score: m as f64,
            });
            commands.push(Command::HashSet {
                key: format!("wh{k}"),
                field: format!("f{m}"),
                value: vec![b'v'; 32],
            });
        }
    }
    run_batch(engine, commands);
}

/// Half routed keys, half container elements. What a shard actually holds, and the reason a
/// SINGLE table cannot be judged on either population alone.
fn seed_mixed(engine: &TemporalEngine, records: usize) {
    seed_routed_keys(engine, records / 2);
    seed_container_keys(engine, (records / 2) / 100, 100);
}

// =================================================================================================
// THE DISTRIBUTION. Pages per OBJECT, which is the term that decides `object_key`.
// =================================================================================================

/// Pages per object, as object counts keyed by pages held.
#[derive(Debug, Default, Clone)]
struct PagesPerObject {
    histogram: BTreeMap<usize, usize>,
    /// Distinct component names per object, same shape. The term that decides `component`.
    components_per_object: BTreeMap<usize, usize>,
    /// Distinct component names per BUCKET: a candidate table scoped to a bucket pays per entry.
    components_per_bucket: BTreeMap<usize, usize>,
    object_key_bytes: usize,
    component_name_bytes: usize,
    /// Component-name bytes that are HEX SPELLINGS OF NUMBERS, so half of them carry nothing.
    hex_component_name_bytes: usize,
    hex_components: usize,
    pages: usize,
}

impl PagesPerObject {
    fn objects(&self) -> usize {
        self.histogram.values().copied().sum()
    }

    /// The value at `q` over the object population, by walking the histogram in key order. A
    /// PERCENTILE, because a mean over a two-population distribution names neither population.
    fn percentile(map: &BTreeMap<usize, usize>, q: f64) -> usize {
        let total: usize = map.values().copied().sum();
        if total == 0 {
            return 0;
        }
        let target = ((total as f64) * q).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (value, count) in map {
            seen += count;
            if seen >= target {
                return *value;
            }
        }
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn max(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  objects={}  pages={}  (denominator for every row below)",
            self.objects(),
            self.pages
        );
        for (name, map) in [
            ("pages per OBJECT", &self.histogram),
            ("components per OBJECT", &self.components_per_object),
            ("components per BUCKET", &self.components_per_bucket),
        ] {
            let n: usize = map.values().copied().sum();
            println!(
                "  {name:<22} n={n:<7} p50={:<5} p90={:<5} p99={:<5} MAX={:<5} distinct-values={}",
                Self::percentile(map, 0.50),
                Self::percentile(map, 0.90),
                Self::percentile(map, 0.99),
                Self::max(map),
                map.len(),
            );
            for (value, count) in map.iter().take(4) {
                println!("      {value:>6} : {count:>8} samples");
            }
            if map.len() > 4 {
                println!("      ... {} further values", map.len() - 4);
            }
        }
        println!(
            "  object-key text {} B over {} objects; component text {} B, of which {} B in {} hex \
             spellings of numbers ({:.1}% of component text)",
            self.object_key_bytes,
            self.objects(),
            self.component_name_bytes,
            self.hex_component_name_bytes,
            self.hex_components,
            if self.component_name_bytes == 0 {
                0.0
            } else {
                100.0 * self.hex_component_name_bytes as f64 / self.component_name_bytes as f64
            }
        );
    }
}

/// Is this component name a hex spelling of numbers rather than a caller's own text?
///
/// `zset_component` is `{biased:016x}` followed by `hex::encode(member)`, and
/// `timestamped_component` is `{stored_key:016x}{identity:016x}`. Both are even-length, all-hex,
/// and at least sixteen characters. A hash FIELD is the caller's own bytes and is not, which is
/// what stops this counting everything and reporting a saving that is not there.
fn is_hex_spelling_of_numbers(name: &str) -> bool {
    name.len() >= 16
        && name.len() % 2 == 0
        && name.bytes().all(|b| b.is_ascii_hexdigit())
}

fn pages_per_object(engine: &TemporalEngine) -> PagesPerObject {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut per_object: BTreeMap<(StoredModelKind, String), (usize, BTreeSet<String>)> =
        BTreeMap::new();
    let mut measured = PagesPerObject::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        let mut bucket_components: BTreeSet<Option<&str>> = BTreeSet::new();
        for (_, page) in bucket.block_index.iter() {
            measured.pages += 1;
            bucket_components.insert(page.component.as_deref());
            let slot = per_object
                .entry((page.model_id, page.object_key.to_string()))
                .or_default();
            slot.0 += 1;
            if let Some(component) = page.component.as_deref() {
                slot.1.insert(component.to_string());
                measured.component_name_bytes += component.len();
                if is_hex_spelling_of_numbers(component) {
                    measured.hex_components += 1;
                    measured.hex_component_name_bytes += component.len();
                }
            }
        }
        *measured
            .components_per_bucket
            .entry(bucket_components.len())
            .or_default() += 1;
    }
    for ((_, object_key), (pages, components)) in per_object.iter() {
        *measured.histogram.entry(*pages).or_default() += 1;
        *measured
            .components_per_object
            .entry(components.len())
            .or_default() += 1;
        measured.object_key_bytes += object_key.len();
    }
    measured
}

/// PAGES PER OBJECT IS TWO POPULATIONS, AT BOTH RANGES AND BOTH CORPUS SIZES.
///
/// This is the measurement `object_key`'s verdict rests on, and the reason it is a histogram is
/// #1959's: a mean over this distribution names neither population. A routed key holds ONE page;
/// a container object holds a hundred. A single interning table serves both, so the verdict is a
/// property of the MIX and is stated per population rather than blended.
///
/// THE RANGE IS THE CONTROL HERE, not a variable. An object's pages all route to its own key's
/// bucket, so pages-per-object cannot depend on how many buckets exist -- and both ranges are
/// measured so that independence is evidence rather than an assumption. Pages per BUCKET does
/// depend on it, which is what #1973 measured and is a different question.
///
/// THE STORE PATH LENGTH is held constant and asserted. Counts are immune to it; allocation bytes
/// move at about six bytes a character, which is why no byte figure in this test is a claim.
#[test]
#[ignore = "seeds twelve stores up to 40,000 records each; run by name"]
fn the_pages_of_one_object_are_two_populations_at_both_routing_ranges() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut arms: Vec<(String, u32, PagesPerObject)> = Vec::new();

    for (records_label, records) in [("4,000", 4_000usize), ("40,000", 40_000usize)] {
        for (range_label, end) in [
            ("whole keyspace", WHOLE_KEYSPACE_END),
            ("operator 1023", OPERATOR_END),
        ] {
            for (shape, seed) in [
                ("routed keys", 0usize),
                ("containers of 100", 1),
                ("mixed half and half", 2),
            ] {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end);
                match seed {
                    0 => seed_routed_keys(&engine, records),
                    1 => seed_container_keys(&engine, records / 100, 100),
                    _ => seed_mixed(&engine, records),
                }
                let measured = pages_per_object(&engine);
                measured.report(&format!("{records_label} records, {range_label}, {shape}"));
                arms.push((format!("{shape} @ {records_label}"), end, measured));
            }
        }
    }

    assert_eq!(12, path_lengths.len(), "all twelve arms must have run");
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms; allocation bytes move with it at about six \
             bytes a character"
        );
    }
    println!("\n  store path length held at {} characters", path_lengths[0]);

    // --- Anti-vacuity: every arm must have censused something. ---
    for (label, _, measured) in &arms {
        assert!(
            measured.pages > 0 && measured.objects() > 0,
            "{label}: nothing censused, so every row it printed is free"
        );
    }

    // --- THE TWO POPULATIONS MUST BOTH BE REACHED, or the verdict is decided by the fixture. ---
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("routed")) {
        assert_eq!(
            1,
            PagesPerObject::max(&measured.histogram),
            "{label}: a routed-key object is supposed to hold exactly one page and the widest \
             holds {}",
            PagesPerObject::max(&measured.histogram)
        );
    }
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("containers")) {
        assert!(
            PagesPerObject::percentile(&measured.histogram, 0.50) >= 100,
            "{label}: the container arm exists to hold objects of a hundred pages and its p50 is \
             {}",
            PagesPerObject::percentile(&measured.histogram, 0.50)
        );
    }
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("mixed")) {
        assert_eq!(
            1,
            PagesPerObject::percentile(&measured.histogram, 0.50),
            "{label}: a mixed corpus is supposed to have a p50 of one -- routed keys outnumber \
             container objects -- and it is {}",
            PagesPerObject::percentile(&measured.histogram, 0.50)
        );
        assert!(
            PagesPerObject::max(&measured.histogram) >= 100,
            "{label}: and a MAX at the container shape, which is {}",
            PagesPerObject::max(&measured.histogram)
        );
    }

    // --- THE RANGE MUST NOT MOVE IT. Same shape, same corpus size, two ranges: identical. ---
    let mut compared = 0usize;
    for (label, end, measured) in &arms {
        if *end != WHOLE_KEYSPACE_END {
            continue;
        }
        let (_, _, other) = arms
            .iter()
            .find(|(l, e, _)| l == label && *e == OPERATOR_END)
            .expect("every arm is run at both ranges");
        compared += 1;
        assert_eq!(
            measured.histogram, other.histogram,
            "{label}: the pages-per-OBJECT histogram differs between the whole keyspace and \
             0..=1023. An object's pages route to its own key's bucket, so this distribution \
             cannot depend on how many buckets exist -- if it does, one of the two arms is not \
             measuring what it says"
        );
    }
    assert_eq!(
        6, compared,
        "six shape/size pairs are supposed to be compared across the two ranges, not {compared}"
    );
}

// =================================================================================================
// WHAT A TABLE WOULD COST. Built for real, over a real store, and charged to the chunk column.
// =================================================================================================

/// A candidate interning table: the handle-to-name direction a read needs, and the
/// name-to-handle direction a write needs. Both, because a table with only one of them cannot
/// serve the engine and would price the change at half.
#[derive(Default)]
struct CandidateTable {
    by_handle: Vec<Arc<str>>,
    by_name: std::collections::HashMap<Arc<str>, u32>,
}

impl CandidateTable {
    fn intern(&mut self, name: &Arc<str>) {
        if self.by_name.contains_key(name) {
            return;
        }
        let handle = self.by_handle.len() as u32;
        self.by_handle.push(Arc::clone(name));
        self.by_name.insert(Arc::clone(name), handle);
    }

    fn len(&self) -> usize {
        self.by_handle.len()
    }
}

/// What a table over `object_key` and a table over `component` would charge, measured.
///
/// TWO BYTE COLUMNS AND THEY ANSWER DIFFERENT QUESTIONS, which is a correction this measurement
/// forced rather than a distinction planned for it. `ALLOC_CHUNK_BYTES` is CUMULATIVE: a `Vec` and a
/// `HashMap` grown one entry at a time charge every intermediate buffer they outgrow, and a
/// geometric growth series sums to roughly twice the final buffer. So the chunk column is what
/// BUILDING the table costs the allocator, and refuting a table on it alone would be refuting it on
/// a figure about twice too large. `resident_bytes` is what the table then HOLDS -- its final
/// capacities -- and that is the steady-state cost a shipped table would carry, so that is the one
/// the verdicts below are taken on. Both are reported.
#[derive(Debug, Clone)]
struct TableCost {
    entries: usize,
    pages: usize,
    chunk_bytes: u64,
    request_bytes: u64,
    allocations: u64,
    resident_bytes: usize,
}

impl TableCost {
    /// Cumulative chunk bytes a page's share of BUILDING the table cost.
    fn chunk_per_page(&self) -> f64 {
        if self.pages == 0 {
            0.0
        } else {
            self.chunk_bytes as f64 / self.pages as f64
        }
    }

    /// What the built table HOLDS, per page. The figure a verdict is taken on.
    fn resident_per_page(&self) -> f64 {
        if self.pages == 0 {
            0.0
        } else {
            self.resident_bytes as f64 / self.pages as f64
        }
    }

    fn entries_per_page(&self) -> f64 {
        self.entries as f64 / self.pages.max(1) as f64
    }
}

/// What a built `CandidateTable` holds: the handle vector's capacity, and the map's.
///
/// The map's per-slot cost is its key and value plus hashbrown's one control byte a slot. The
/// `Arc<str>` payloads are NOT counted -- they are clones of allocations the store already holds,
/// which is the whole finding `page_entry_names` established, and charging them here would price
/// the table for bytes it does not add.
#[cfg(feature = "alloc-probe")]
fn resident_bytes(table: &CandidateTable) -> usize {
    let handle_side = table.by_handle.capacity() * std::mem::size_of::<Arc<str>>();
    let map_side = table.by_name.capacity()
        * (std::mem::size_of::<Arc<str>>() + std::mem::size_of::<u32>() + 1);
    handle_side + map_side
}

/// The bytes a handle SAVES in the entry, per page.
///
/// Not 12, and the difference is the whole reason this is a constant with a comment rather than a
/// subtraction at the call site. Swapping a 16-byte fat pointer for a 4-byte handle removes 12
/// bytes of FIELD, but `BlockIndex` is 8-aligned and carries three flag bytes in its slack, so
/// what the structure actually loses is a whole eight-byte step and then some of the slack. The
/// measured steps, from the layout probe below: 88 with three names, 72 with the model spelling as
/// one byte, 56 with the component as a two-byte ordinal as well, 48 with all three. Every one of
/// those is SIXTEEN, not twelve.
#[allow(dead_code)]
const ENTRY_BYTES_A_HANDLE_SAVES: usize = 16;

#[cfg(feature = "alloc-probe")]
fn measure_object_key_table(engine: &TemporalEngine) -> TableCost {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut pages = 0usize;
    // Collected first, so the probe charges the TABLE and not the walk that feeds it.
    let mut names: Vec<Arc<str>> = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            pages += 1;
            names.push(Arc::clone(&page.object_key));
        }
    }
    let probe = Probe::start();
    let mut table = CandidateTable::default();
    for name in &names {
        table.intern(name);
    }
    let counts = probe.stop();
    let entries = table.len();
    let resident = resident_bytes(&table);
    // Held past the probe so nothing is freed inside the window.
    std::hint::black_box(&table);
    TableCost {
        entries,
        pages,
        chunk_bytes: counts.chunk_bytes,
        request_bytes: counts.alloc_bytes,
        allocations: counts.allocs,
        resident_bytes: resident,
    }
}

#[cfg(feature = "alloc-probe")]
fn measure_component_table(engine: &TemporalEngine) -> TableCost {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut pages = 0usize;
    let mut names: Vec<Arc<str>> = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            pages += 1;
            if let Some(component) = page.component.as_ref() {
                names.push(Arc::clone(component));
            }
        }
    }
    let probe = Probe::start();
    let mut table = CandidateTable::default();
    for name in &names {
        table.intern(name);
    }
    let counts = probe.stop();
    let entries = table.len();
    let resident = resident_bytes(&table);
    std::hint::black_box(&table);
    TableCost {
        entries,
        pages,
        chunk_bytes: counts.chunk_bytes,
        request_bytes: counts.alloc_bytes,
        allocations: counts.allocs,
        resident_bytes: resident,
    }
}

/// A HANDLE FOR `object_key` PAYS ON ONE POPULATION AND LOSES ON THE OTHER, and the break-even is
/// measured rather than argued.
///
/// The saving is 16 bytes a PAGE. The cost is one table entry per distinct OBJECT KEY, and that
/// cost is an allocation, so it appears on the chunk column and nowhere else -- which is why this
/// arm is `alloc-probe` gated and the counting arm above is not.
///
/// BOTH DIRECTIONS OF THE TABLE ARE BUILT. A `Vec<Arc<str>>` alone would serve a read and price
/// the change at a third of what it costs; the engine also has to go name-to-handle on every
/// write, and that is a hash map.
#[test]
#[ignore = "alloc-probe only, seeds six stores; run by name with --features alloc-probe"]
#[cfg(feature = "alloc-probe")]
fn a_handle_for_the_object_key_pays_only_above_the_measured_break_even() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, TableCost, PagesPerObject)> = Vec::new();

    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        for (shape, seed) in [
            ("routed keys", 0usize),
            ("containers of 100", 1),
            ("mixed half and half", 2),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, end);
            match seed {
                0 => seed_routed_keys(&engine, 4_000),
                1 => seed_container_keys(&engine, 40, 100),
                _ => seed_mixed(&engine, 4_000),
            }
            let distribution = pages_per_object(&engine);
            let cost = measure_object_key_table(&engine);
            rows.push((format!("{shape}, {range_label}"), cost, distribution));
        }
    }

    assert_eq!(6, path_lengths.len());
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    println!(
        "\n=== A HANDLE FOR object_key: 16 B a page saved against one table entry per object ===\n\
           store path length {} characters; table = Vec<Arc<str>> + HashMap<Arc<str>, u32>",
        path_lengths[0]
    );
    println!(
        "  {:<34} {:>7} {:>7} {:>6} {:>10} {:>10} {:>8} {:>8}",
        "arm", "pages", "entries", "p/obj", "build/pg", "resident/pg", "saved/pg", "NET/pg"
    );
    for (label, cost, distribution) in &rows {
        let saved = ENTRY_BYTES_A_HANDLE_SAVES as f64;
        let net = saved - cost.resident_per_page();
        println!(
            "  {label:<34} {:>7} {:>7} {:>6.2} {:>10.2} {:>10.2} {:>8.2} {:>+8.2}",
            cost.pages,
            cost.entries,
            cost.pages as f64 / distribution.objects().max(1) as f64,
            cost.chunk_per_page(),
            cost.resident_per_page(),
            saved,
            net
        );
        // The chunk rule is a FLOOR, not an equality (#1969).
        assert!(
            cost.chunk_bytes >= cost.request_bytes,
            "{label}: the chunk column read {} BELOW the request column {}, which cannot happen \
             if it is reading `malloc_usable_size`",
            cost.chunk_bytes,
            cost.request_bytes
        );
        assert!(
            cost.allocations > 0,
            "{label}: the table was built without allocating, which reads as a probe that is not \
             installed rather than as a free table"
        );
        assert!(cost.pages > 0, "{label}: no pages, so every figure on this row is free");
    }

    // --- THE VERDICT, PER POPULATION. ---
    let routed = rows
        .iter()
        .filter(|(l, _, _)| l.starts_with("routed"))
        .collect::<Vec<_>>();
    let containers = rows
        .iter()
        .filter(|(l, _, _)| l.starts_with("containers"))
        .collect::<Vec<_>>();
    assert_eq!(2, routed.len());
    assert_eq!(2, containers.len());

    for (label, cost, _) in &routed {
        assert_eq!(
            cost.entries, cost.pages,
            "{label}: a routed key is one page per object, so the table is supposed to hold one \
             entry per page -- {} entries over {} pages",
            cost.entries, cost.pages
        );
        assert!(
            cost.resident_per_page() > ENTRY_BYTES_A_HANDLE_SAVES as f64,
            "{label}: the table holds {:.2} B a page against {} B saved, so a handle is supposed \
             to LOSE on this population and it does not",
            cost.resident_per_page(),
            ENTRY_BYTES_A_HANDLE_SAVES
        );
    }
    for (label, cost, _) in &containers {
        assert!(
            cost.entries * 50 < cost.pages,
            "{label}: a container object holds a hundred pages, so the table is supposed to hold \
             far fewer entries than pages -- {} entries over {} pages",
            cost.entries,
            cost.pages
        );
        assert!(
            cost.resident_per_page() < ENTRY_BYTES_A_HANDLE_SAVES as f64,
            "{label}: the table holds {:.2} B a page against {} B saved, so a handle is supposed \
             to PAY on this population and it does not",
            cost.resident_per_page(),
            ENTRY_BYTES_A_HANDLE_SAVES
        );
    }

    let break_even: f64 = {
        // Bytes a table entry HOLDS, from the routed arm where entries == pages exactly.
        let per_entry = routed[0].1.resident_per_page();
        per_entry / ENTRY_BYTES_A_HANDLE_SAVES as f64
    };
    println!(
        "\n  BREAK-EVEN: a table entry holds {:.2} B resident (and about {:.2} B to build, \n\
           cumulatively), so a handle pays above {:.2} pages per object and loses below it. \n\
           Measured p50 pages/object: routed 1, containers {}, mixed {}.",
        routed[0].1.resident_per_page(),
        routed[0].1.chunk_per_page(),
        break_even,
        PagesPerObject::percentile(&containers[0].2.histogram, 0.50),
        PagesPerObject::percentile(
            &rows
                .iter()
                .find(|(l, _, _)| l.starts_with("mixed"))
                .expect("a mixed arm ran")
                .2
                .histogram,
            0.50
        ),
    );
    assert!(
        break_even > 1.0,
        "the break-even came out at {break_even:.2} pages per object, at or below one. That would \
         mean a table entry is cheaper than the pointer it replaces even for a single-page object, \
         and this whole verdict would be the other way round"
    );
}

/// A HANDLE FOR `component` IS DECIDED BY HOW MANY DISTINCT NAMES THE STORE HOLDS, AND THAT IS TWO
/// POPULATIONS TOO.
///
/// THIS TEST WAS WRITTEN TO REFUTE THE HANDLE OUTRIGHT and its own anti-vacuity assertion refused to
/// let it. The reasoning was that a container key's fields are each their own page and the component
/// is the only thing telling them apart, so distinct component names must grow with the pages. That
/// is true WITHIN one object and false ACROSS a store: forty hashes with the same hundred field
/// names hold 400 distinct components over 4,000 pages, because a schema repeats. The assertion
/// fired, and the verdict is stated by population instead.
///
/// THE TWO POPULATIONS, and which one a real store is in depends on where the name comes from:
///
///   * A SCHEMA name -- a hash field -- repeats across every object of that shape, so distinct
///     names are bounded by the schema and a handle PAYS.
///   * A CONTENT-DERIVED name does not repeat at all. `zset_component` is
///     `format!("{biased:016x}{}", hex::encode(member))` and `timestamped_component` is
///     `format!("{stored_key:016x}{identity:016x}")`: the name IS the lookup key, computed from the
///     element. One entry per page, and a handle LOSES.
///
/// AND THE SECOND POPULATION CANNOT BE ORDINALISED AWAY, which is the part that matters for
/// sequencing. A content-derived name needs a per-object member-to-ordinal map to be replaced by an
/// ordinal at all -- the same table this test prices, plus a second one to get back. Which is why
/// the lever to reach for on that population is not the pointer: it is that those names are HEX
/// TEXT FOR NUMBERS, measured separately, needing no table at all.
#[test]
#[ignore = "alloc-probe only, seeds four stores; run by name with --features alloc-probe"]
#[cfg(feature = "alloc-probe")]
fn a_handle_for_the_component_costs_a_table_entry_per_page() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, TableCost, PagesPerObject)> = Vec::new();

    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        for (shape, keys, members) in [("containers of 100", 40usize, 100usize), ("one container of 2000", 1, 2_000)] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, end);
            seed_container_keys(&engine, keys, members);
            let distribution = pages_per_object(&engine);
            distribution.report(&format!("{shape}, {range_label}"));
            let cost = measure_component_table(&engine);
            rows.push((format!("{shape}, {range_label}"), cost, distribution));
        }
    }

    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    const SAVED: f64 = ENTRY_BYTES_A_HANDLE_SAVES as f64;
    println!(
        "\n=== A HANDLE FOR component: {SAVED} B a page saved against one table entry per \
           DISTINCT COMPONENT ===\n  store path length {} characters",
        path_lengths[0]
    );
    println!(
        "  {:<40} {:>7} {:>7} {:>9} {:>10} {:>11} {:>8}",
        "arm", "pages", "entries", "entr/page", "build/pg", "resident/pg", "NET/pg"
    );
    for (label, cost, _) in &rows {
        println!(
            "  {label:<40} {:>7} {:>7} {:>9.3} {:>10.2} {:>11.2} {:>+8.2}",
            cost.pages,
            cost.entries,
            cost.entries_per_page(),
            cost.chunk_per_page(),
            cost.resident_per_page(),
            SAVED - cost.resident_per_page()
        );
        assert!(
            cost.chunk_bytes >= cost.request_bytes,
            "{label}: chunk column below request column"
        );
        assert!(cost.pages > 0, "{label}: no pages measured");
    }

    // --- BOTH POPULATIONS MUST BE REACHED, or the verdict is decided by the fixture. ---
    let schema: Vec<&(String, TableCost, PagesPerObject)> = rows
        .iter()
        .filter(|(label, _, _)| label.starts_with("containers of 100"))
        .collect();
    let content: Vec<&(String, TableCost, PagesPerObject)> = rows
        .iter()
        .filter(|(label, _, _)| label.starts_with("one container of 2000"))
        .collect();
    assert_eq!(2, schema.len(), "the repeated-schema arm runs at both ranges");
    assert_eq!(2, content.len(), "the unique-name arm runs at both ranges");

    // --- THE SCHEMA POPULATION: names repeat, so the table is small and the handle PAYS. ---
    for (label, cost, _) in &schema {
        assert!(
            cost.entries_per_page() < 0.5,
            "{label}: {} distinct components over {} pages. This arm exists to hold a REPEATED \
             schema -- the same hundred field names across forty objects -- so distinct names are \
             supposed to be far fewer than pages",
            cost.entries,
            cost.pages
        );
        assert!(
            cost.resident_per_page() < SAVED,
            "{label}: the table holds {:.2} B a page against {SAVED} B saved, so a handle is \
             supposed to PAY where the names repeat",
            cost.resident_per_page()
        );
    }

    // --- THE CONTENT-DERIVED POPULATION: one name per page, and the handle LOSES. ---
    for (label, cost, _) in &content {
        assert!(
            cost.entries_per_page() > 0.9,
            "{label}: {} distinct components over {} pages. This arm exists to hold names that do \
             NOT repeat -- one container whose two thousand members are all distinct -- so the \
             table is supposed to hold about one entry per page",
            cost.entries,
            cost.pages
        );
        assert!(
            cost.resident_per_page() > SAVED,
            "{label}: the table holds {:.2} B a page against {SAVED} B saved, so a handle is \
             supposed to LOSE where every name is distinct",
            cost.resident_per_page()
        );
    }

    println!(
        "\n  VERDICT, BY POPULATION. Where a component name is a repeated SCHEMA name the table \n\
           holds {:.2} B a page and a handle nets {:+.2}. Where it is CONTENT-DERIVED -- a zset \n\
           member, a timestamped entry -- it holds {:.2} B a page and a handle nets {:+.2}. A \n\
           single blended figure would hide that, and the second population is the one a \n\
           container store is mostly made of.",
        schema[0].1.resident_per_page(),
        SAVED - schema[0].1.resident_per_page(),
        content[0].1.resident_per_page(),
        SAVED - content[0].1.resident_per_page(),
    );
}

/// THE COMPONENT NAME IS HEX TEXT FOR NUMBERS, AND HALF OF IT CARRIES NOTHING.
///
/// A separate and larger term than the pointer, measured separately because it is a separate
/// change: it needs no table, no ordinal and no map. `{biased:016x}` is sixteen characters for
/// eight bytes and `hex::encode(member)` is exactly twice its input, so a zset member of twenty
/// bytes becomes a fifty-six character name where twenty-eight bytes would do.
///
/// COUNTED OVER A REAL STORE and classified by a predicate that a hash FIELD fails -- a field is
/// the caller's own text and is not an encoding of anything, which is what stops this reporting a
/// saving on names that have none.
#[test]
#[ignore = "seeds two stores; run by name"]
fn the_component_name_is_hex_text_for_numbers_and_that_half_is_measured() {
    let mut rows: Vec<(String, PagesPerObject)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();
    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = probe_engine(dir.path());
        load_shard_over(&engine, end);
        // Sets and zsets produce hex component names; hash fields are the caller's own short
        // text. Both, so the classifier is exercised in both directions in the same store and a
        // predicate that matched everything would fail below.
        seed_wide_members(&engine, 4, 100);
        let measured = pages_per_object(&engine);
        measured.report(&format!("hex component census, {range_label}"));
        rows.push((range_label.to_string(), measured));
    }
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    for (label, measured) in &rows {
        assert!(
            measured.component_name_bytes > 0,
            "{label}: no component text at all, so the ratio below is free"
        );
        // BOTH directions of the predicate must be reached, or it is not discriminating.
        assert!(
            measured.hex_components > 0,
            "{label}: the predicate classified NO component as a hex spelling, and this fixture \
             seeds zsets and lists which produce them"
        );
        assert!(
            measured.hex_component_name_bytes < measured.component_name_bytes,
            "{label}: the predicate classified EVERY component byte as hex, and this fixture also \
             seeds hash fields, which are the caller's own text. A predicate that matches \
             everything is not measuring anything"
        );
        let hex_share =
            measured.hex_component_name_bytes as f64 / measured.component_name_bytes as f64;
        println!(
            "  {label}: {:.1}% of component text is a hex spelling of numbers; storing those as \
             bytes would halve {} B to {} B",
            100.0 * hex_share,
            measured.hex_component_name_bytes,
            measured.hex_component_name_bytes / 2
        );
    }
}

// =================================================================================================
// LAYOUT. What each name's handle does to the entry, and to the component level around it.
// =================================================================================================

/// Mirrors of `ComponentBlocks`/`ComponentList` with an ordinal in place of the name, so the
/// niche question is measured rather than reasoned about.
mod ordinal_layout {
    use super::*;
    use std::num::{NonZeroU16, NonZeroU32};

    #[derive(Debug, Clone)]
    pub(super) struct ComponentBlocksU16 {
        pub(super) component: Option<NonZeroU16>,
        pub(super) refs: BlockRefs,
    }
    #[derive(Debug, Clone)]
    pub(super) enum ComponentListU16 {
        Empty,
        One(ComponentBlocksU16),
        Many(Vec<ComponentBlocksU16>),
    }

    #[derive(Debug, Clone)]
    pub(super) struct ComponentBlocksU32 {
        pub(super) component: Option<NonZeroU32>,
        pub(super) refs: BlockRefs,
    }
    #[derive(Debug, Clone)]
    pub(super) enum ComponentListU32 {
        Empty,
        One(ComponentBlocksU32),
        Many(Vec<ComponentBlocksU32>),
    }

    /// A plain integer with a SENTINEL rather than a `NonZero`, which is the shape with no niche
    /// at all. Measured so the claim below is about what a niche is worth here, not about what a
    /// niche is worth in general.
    #[derive(Debug, Clone)]
    pub(super) struct ComponentBlocksPlain {
        pub(super) component: u16,
        pub(super) refs: BlockRefs,
    }
    #[derive(Debug, Clone)]
    pub(super) enum ComponentListPlain {
        Empty,
        One(ComponentBlocksPlain),
        Many(Vec<ComponentBlocksPlain>),
    }
}

/// AN ORDINAL DOES NOT COST `ComponentList` ITS TAG -- IT MAKES THE LEVEL SMALLER.
///
/// #1967 measured `ComponentList` at 40 bytes, the same as the `ComponentBlocks` its `One` arm
/// holds inline, and recorded that the tag rides a niche in the component NAME. The natural worry
/// is that replacing the name with an ordinal takes the niche away and the enum grows by eight.
///
/// MEASURED, IT GOES THE OTHER WAY, and the reason is instructive: with a 16-byte
/// `Option<Arc<str>>` beside a 24-byte `BlockRefs` the struct is 40 with NO PADDING, so a niche
/// really is the only place a tag could go. With a two- or four-byte ordinal the struct is 32 and
/// carries SIX BYTES OF PADDING before `refs`, and a tag fits there without needing a niche at
/// all. The plain-integer arm is the control on exactly that: it has no niche and is also 32.
///
/// So the niche was load-bearing only for the shape that no longer applies, and the component
/// level gets CHEAPER per entry rather than more expensive. What decides `component` is the table,
/// not the niche -- which is what the refutation above measures.
#[test]
fn an_ordinal_does_not_cost_the_component_list_its_tag() {
    use ordinal_layout::*;
    let rows = [
        ("ComponentBlocks (name, today)", std::mem::size_of::<ComponentBlocks>()),
        ("ComponentList   (name, today)", std::mem::size_of::<ComponentList>()),
        ("ComponentBlocks (NonZeroU16)", std::mem::size_of::<ComponentBlocksU16>()),
        ("ComponentList   (NonZeroU16)", std::mem::size_of::<ComponentListU16>()),
        ("ComponentBlocks (NonZeroU32)", std::mem::size_of::<ComponentBlocksU32>()),
        ("ComponentList   (NonZeroU32)", std::mem::size_of::<ComponentListU32>()),
        ("ComponentBlocks (u16 sentinel, NO niche)", std::mem::size_of::<ComponentBlocksPlain>()),
        ("ComponentList   (u16 sentinel, NO niche)", std::mem::size_of::<ComponentListPlain>()),
        ("BlockRefs", std::mem::size_of::<BlockRefs>()),
        ("ObjectBlockRefs", std::mem::size_of::<ObjectBlockRefs>()),
    ];
    println!("\n=== the component level, by what the name is ===");
    for (label, size) in rows {
        println!("  {label:<42} {size:>3} B");
    }

    // Today's shape, pinned so a change to `BlockRefs` cannot make this test vacuous.
    assert_eq!(40, std::mem::size_of::<ComponentBlocks>());
    assert_eq!(40, std::mem::size_of::<ComponentList>());

    // THE CLAIM: the wrapper stays free, and the entry shrinks.
    for (label, blocks, list) in [
        (
            "NonZeroU16",
            std::mem::size_of::<ComponentBlocksU16>(),
            std::mem::size_of::<ComponentListU16>(),
        ),
        (
            "NonZeroU32",
            std::mem::size_of::<ComponentBlocksU32>(),
            std::mem::size_of::<ComponentListU32>(),
        ),
    ] {
        assert_eq!(
            blocks, list,
            "{label}: the three-arm wrapper is supposed to stay free -- `ComponentList` the same \
             width as the `ComponentBlocks` its `One` arm holds -- and it is {list} against {blocks}"
        );
        assert!(
            list <= std::mem::size_of::<ComponentList>(),
            "{label}: `ComponentList` grew from {} to {list}; the niche the tag rode really was \
             load-bearing and this is the eight bytes that were feared",
            std::mem::size_of::<ComponentList>()
        );
        assert!(
            blocks < std::mem::size_of::<ComponentBlocks>(),
            "{label}: `ComponentBlocks` did not shrink at all ({blocks} against {})",
            std::mem::size_of::<ComponentBlocks>()
        );
    }

    // THE CONTROL on the explanation. If the tag needed a niche, the sentinel arm -- which has
    // none -- would be wider than the `NonZero` arm. It is not, so the tag is riding PADDING and
    // the niche is not what makes this work.
    assert_eq!(
        std::mem::size_of::<ComponentListPlain>(),
        std::mem::size_of::<ComponentListU16>(),
        "a plain `u16` has no niche and a `NonZeroU16` has one, so if the tag needed a niche these \
         two would differ. They do not, which is the evidence that the tag rides the padding an \
         ordinal introduces rather than a niche in the field"
    );
}

/// THE ENTRY IS 72 BYTES AND ITS RECONSTRUCTION IS ASSERTED, NOT ITS TOTAL.
///
/// A literal 72 goes stale silently the next time a field moves. What is asserted here is that the
/// fields ACCOUNT for the number: the eight-aligned group plus one rounding of the tail.
///
/// AND WHAT EACH OF THE THREE NAMES WOULD BE WORTH, measured on mirrors rather than projected, so
/// the remaining two verdicts are stated against the same arithmetic as the one that shipped.
#[test]
fn the_entry_is_fifty_six_bytes_and_every_one_is_accounted_for() {
    use std::mem::{align_of, offset_of, size_of};

    let address = size_of::<crate::block_store::BlockAddress>();
    let model = size_of::<StoredModelKind>();
    let object_key = size_of::<Arc<str>>();
    let component = size_of::<Option<Arc<str>>>();
    let flags = 3usize;

    println!("\n=== BlockIndex, field by field ===");
    println!("  offsets: address@{} object_key@{} component@{} model_id@{} dirty@{} deleted@{} log_backed@{}",
        offset_of!(BlockIndex, address),
        offset_of!(BlockIndex, object_key),
        offset_of!(BlockIndex, component),
        offset_of!(BlockIndex, model_id),
        offset_of!(BlockIndex, dirty),
        offset_of!(BlockIndex, deleted),
        offset_of!(BlockIndex, log_backed),
    );
    println!(
        "  address {address} + object_key {object_key} + component {component} + model_id {model} \
         + flags {flags} = {} bytes of field in {} B",
        address + object_key + component + model + flags,
        size_of::<BlockIndex>()
    );

    assert_eq!(1, model, "the model spelling is supposed to be one byte");
    assert_eq!(8, align_of::<BlockIndex>());

    // THE RECONSTRUCTION. The three 8-aligned fields form the group; the model byte and the three
    // flag bytes are the tail, and the tail is rounded once to the alignment.
    let eight_aligned = address + object_key + component;
    let tail = model + flags;
    let round_up = |value: usize, to: usize| (value + to - 1) / to * to;
    assert_eq!(
        eight_aligned + round_up(tail, align_of::<BlockIndex>()),
        size_of::<BlockIndex>(),
        "the fields do not account for the structure: group {eight_aligned} + tail {tail} rounded \
         to {} is not {}",
        align_of::<BlockIndex>(),
        size_of::<BlockIndex>()
    );
    // 56 AND NOT 64: the address inside the entry shed its object id, a whole eight-byte field in
    // the eight-aligned group. The accounting above is a RECONSTRUCTION and needed no change for it
    // -- which is the point of reconstructing rather than totalling -- but this literal did, and
    // NOTHING BUT RUNNING IT COULD HAVE SAID SO. That sentence was already here for the 72 -> 64
    // step and it earned itself again: a scan for `const _: () = assert!(size_of...)` does not see
    // an `assert_eq!` in a test body, so this pin compiled clean and failed at run time.
    assert_eq!(56, size_of::<BlockIndex>());

    // The slack, which is why every step here is sixteen bytes and not twelve.
    let slack = size_of::<BlockIndex>() - (eight_aligned + tail);
    println!(
        "  slack {slack} B -- the three flag bytes and the model byte share the rounding, which is \
         why swapping a 16-byte pointer for a small field takes the STRUCTURE down a whole \
         eight-byte step and then some"
    );
    assert!(slack < 8, "a slack of {slack} means a whole word is unaccounted for");

    // AND THE MAP ARM, WHICH NO LONGER CARRIES THE ENTRY INLINE.
    //
    // This used to assert `BlockIndexMap == 8 + size_of::<BlockIndex>()` -- a handle plus an inline
    // entry -- and that was the relation that made narrowing the entry worth a word off every
    // `BucketNode` in the `BucketMap`. The single-page arm now holds a POINTER, so the page index is
    // a bare container header and its width is INDEPENDENT of the entry's.
    //
    // WHICH MEANS THE NARROWING MEASURED ABOVE IS STILL WORTH SOMETHING, BUT SOMEWHERE ELSE: on the
    // HEAP rather than in the node. Every boxed single-page arm and every element of every page list
    // is sixteen bytes smaller for it. The node banked its share once, and once only, whichever of
    // the two changes landed second.
    //
    // The relation is asserted in the NEGATIVE so this cannot silently become true again: an entry
    // back inside the page index would put the width of the whole structure back on every bucket.
    let arm = size_of::<crate::engine::state::BlockIndexMap>();
    assert_ne!(
        8 + size_of::<BlockIndex>(),
        arm,
        "the page index is {arm} B, which is a handle plus a whole entry again -- the inline arm has \
         come back and every bucket in the map is paying the entry's width whether it holds one page \
         or fifty"
    );
    assert_eq!(
        size_of::<Vec<(u64, BlockIndex)>>(),
        arm,
        "the page index is {arm} B against its own page list's {} B; it is supposed to be exactly \
         that header, with both other arms riding pointer niches",
        size_of::<Vec<(u64, BlockIndex)>>()
    );
    assert!(
        arm < size_of::<BlockIndex>(),
        "the page index is {arm} B and a single entry is {} B; the index must be too narrow to hold \
         one inline, or the entry is back in the node",
        size_of::<BlockIndex>()
    );
    println!(
        "  BlockIndexMap {arm} B (a bare page-list header -- INDEPENDENT of the entry's width now), \
         BucketNode {} B. The sixteen bytes measured above are banked on the HEAP: in every boxed \
         arm and every list element, not in the node.",
        size_of::<crate::engine::state::BucketNode>()
    );
}

// =================================================================================================
// THE READ PATH, COUNTED. A handle adds a table lookup; a one-byte discriminant does not.
// =================================================================================================

/// READING A PAGE'S MODEL SPELLING EXAMINES NO MORE ENTRIES THAN IT DID.
///
/// A handle into a table adds a dependent load to every resolution of a name, and no footprint
/// measurement can see it. The spelling that SHIPPED here is not that shape: it is a discriminant
/// in the entry and the name is a `&'static str` off the registry, so resolving it is a match on
/// one byte and touches nothing outside the entry that was already loaded.
///
/// COUNTED, not timed -- a timing-ratio instrument in this tree once read 485x idle against 11x
/// busy off identical code. `PAGE_LOOKUP_ENTRIES_EXAMINED` is the existing counter inside
/// `find_page`, and the claim is that reading every page's spelling adds NOTHING to it.
#[test]
fn resolving_a_model_spelling_examines_no_page_index_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, OPERATOR_END);
    seed_container_keys(&engine, 8, 100);
    seed_routed_keys(&engine, 200);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    crate::engine::state::reset_page_lookup_entries_examined();
    let before = crate::engine::state::page_lookup_entries_examined();
    let mut resolved = 0usize;
    let mut folded = 0u64;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            // The resolution under test, folded so it cannot be optimised away.
            let spelling = page.model_id.as_str();
            folded = folded
                .wrapping_mul(31)
                .wrapping_add(spelling.len() as u64)
                .wrapping_add(spelling.as_bytes()[0] as u64);
            resolved += 1;
        }
    }
    let after = crate::engine::state::page_lookup_entries_examined();

    println!(
        "\n=== resolving a model spelling ===\n  {resolved} spellings resolved, \
         PAGE_LOOKUP_ENTRIES_EXAMINED {before} -> {after}, checksum {folded}"
    );
    assert!(resolved > 0, "no spelling was resolved, so the count below is free");
    assert_ne!(0, folded, "the fold read nothing, so the loads may have been dropped");
    assert_eq!(
        before, after,
        "resolving {resolved} model spellings moved the page-lookup counter from {before} to \
         {after}. A spelling is supposed to be a match on one byte already inside the entry; a \
         move here means it is going through an index"
    );
}

// =================================================================================================
// THE LIFETIME HAZARD. Release empties a bucket's page list; reload re-derives it.
// =================================================================================================

/// A RELEASED BUCKET RELOADS EVERY PAGE'S NAMES, ELEMENT BY ELEMENT.
///
/// `release_bucket_blocks` empties a bucket's page list and `reload_released_bucket` re-derives it
/// from the model maps. Anything the entry holds has to survive that or be re-derived IDENTICALLY,
/// and a count of pages cannot show it: a reload that produced the right NUMBER of pages with one
/// spelling wrong would pass a count and mis-attribute a page.
///
/// So the comparison is element by element, over the sorted triples, and the fixture is asserted
/// to have actually released something before the comparison is made.
#[test]
fn a_released_bucket_reloads_every_name_element_by_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, OPERATOR_END);
    // `released_model_kind_is_addressable` admits `string` and `context_node` only, so a release
    // is only reversible for those -- which is what this fixture seeds.
    seed_routed_keys(&engine, 400);

    // A RELEASE REFUSES A DIRTY BUCKET, and a freshly written one is dirty -- releasing it while
    // `eviction_dump_before_evict` is false would strand undumped writes with nothing to rebuild
    // them from. So the fixture dumps through the production lifecycle, which is what clears the
    // flag (`clear_dumped_bucket_dirty_state`).
    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!candidates.is_empty(), "the fixture produced no bucket to release");
    let lifecycle = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: candidates.clone(),
        max_dump_buckets_per_round: candidates.len(),
        ..Default::default()
    });
    println!("\n=== release/reload ===");
    println!("  fixture: {} candidate buckets, lifecycle ran", candidates.len());
    std::hint::black_box(&lifecycle);

    let before = page_triples(&engine);
    assert!(!before.is_empty(), "the fixture produced no pages to release");

    // DRIVEN ON THE FUNCTION ITSELF. `refused_buckets` is one number for eleven different answers,
    // so a fixture that fails a precondition has to say WHICH one -- otherwise it reads as a
    // release that simply had nothing to do, which is the shape that makes the comparison below
    // pass over an empty set.
    let outcome = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        crate::engine::storage_bucket_internals::release_bucket_blocks(shard, &candidates)
    };
    println!(
        "  released {} buckets holding {} pages, refused {}",
        outcome.released_buckets.len(),
        outcome.released_blocks,
        outcome.refused_buckets
    );
    println!("  refusals by term: {:?}", outcome.refusals);
    assert!(
        outcome.released_blocks > 0,
        "the release emptied no page list, so the reload below re-derives nothing and every \
         assertion after it is free. {} candidates were offered; the terms they failed on are \
         {:?}",
        candidates.len(),
        outcome.refusals
    );
    let released = outcome.released_buckets.clone();

    // The released buckets really did lose their pages.
    let during = page_triples(&engine);
    assert!(
        during.len() < before.len(),
        "the release left {} pages of {} resident, so it did not empty a page list",
        during.len(),
        before.len()
    );

    for routing_bucket in &released {
        let reloaded = {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard is loaded");
            crate::engine::storage_bucket_internals::reload_released_bucket(
                shard,
                1,
                *routing_bucket,
            )
        };
        assert!(reloaded, "bucket {routing_bucket} was released and refused to reload");
    }

    let after = page_triples(&engine);

    // ELEMENT BY ELEMENT, and the count is asserted too -- but only as well as, never instead of.
    assert_eq!(
        before.len(),
        after.len(),
        "the reload brought back {} pages against {} released",
        after.len(),
        before.len()
    );
    let mut compared = 0usize;
    for (index, (left, right)) in before.iter().zip(after.iter()).enumerate() {
        assert_eq!(
            left, right,
            "page {index} came back with different names: before {left:?}, after {right:?}. A \
             count would have passed this"
        );
        compared += 1;
    }
    assert_eq!(
        before.len(), compared,
        "the element-by-element comparison ran over {compared} of {} pages",
        before.len()
    );
    println!("  {compared} pages compared element by element, all three names identical");
}

/// Every page's three names and its address identity, sorted, so two walks are comparable.
fn page_triples(engine: &TemporalEngine) -> Vec<(String, String, Option<String>, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut triples: Vec<(String, String, Option<String>, u64, u64)> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.iter())
        .map(|(_, page)| {
            (
                page.model_id.as_str().to_string(),
                page.object_key.to_string(),
                page.component.as_deref().map(str::to_string),
                page.address.block_slab_id(),
                page.address.offset(),
            )
        })
        .collect();
    triples.sort();
    triples
}

// =================================================================================================
// LOUD OR SILENT. What a spelling the registry cannot place actually does.
// =================================================================================================

/// A STORED SPELLING THE REGISTRY CANNOT PLACE IS REFUSED, AND THE REFUSAL NAMES IT.
///
/// THE MUTANT THIS PLANTS is the dangerous one for a narrowed field: a stored index carrying a
/// spelling the engine does not know. Mapped onto some default variant it would file the page
/// under a kind it does not have, and a page filed under the wrong kind is still on its slab while
/// nothing looks for it -- silent corruption, not a red test.
///
/// The wire did NOT move for this change: the spelling is still written and read as a string. So
/// the failure mode a migration would have had -- hex parsed as a length, a handle parsed as a
/// string -- does not exist here, and what remains is exactly this one: a name with no row.
#[test]
#[should_panic(expected = "no stored model kind for model id \"not_a_model_kind\"")]
fn a_stored_spelling_with_no_row_is_refused_by_name_at_the_decode() {
    // PLANTED ON A REAL ENTRY. Encoding a hand-written map would make this test assert about an
    // address wire shape it has no business knowing; mutating one field of a genuine encoding
    // plants exactly the defect and nothing else.
    let mut json = serde_json::to_value(&sample_entry()).expect("encode");
    json["model_id"] = serde_json::Value::String("not_a_model_kind".to_string());
    let _: BlockIndex = serde_json::from_value(json).expect("the decode must be reached");
}

/// One page entry, for the tests that mutate a single field of a real encoding.
fn sample_entry() -> BlockIndex {
    BlockIndex {
        object_key: Arc::from("k"),
        model_id: StoredModelKind::String,
        component: Some(Arc::from("a")),
        address: crate::block_store::BlockAddress::from_parts(1, 2, 4, Some(5), Some(6)),
        dirty: true,
        deleted: false,
    }
}

/// AND THE SAME REFUSAL AT THE WRITE, which is the other direction a spelling arrives from.
#[test]
#[should_panic(expected = "no stored model kind for model id \"not_a_model_kind\"")]
fn a_written_spelling_with_no_row_is_refused_by_name() {
    crate::engine::storage_bucket_internals::stored_model_kind("not_a_model_kind");
}

/// THE TWO DERIVATIONS OF THE REPORT BYTE MUST AGREE, and they are independent.
///
/// `model_report_code` reads `StoredModelKind::report_code`; the registry's own guards derive their
/// expectations from `ModelKind::report_code`. Those are two match arms generated from the same
/// declaration, which is what makes the guards' comparison meaningful rather than a function
/// checked against itself -- and what makes DRIFT between them possible. This pins the agreement
/// over every live kind, and pins the retired half against the declared table.
///
/// It exists because a mutation run found the new accessor had no production caller at all: a
/// second derivation nothing reached, so mutating it killed nothing. Routing the report through it
/// fixed the reachability; this stops the two derivations diverging afterwards.
#[test]
fn the_two_derivations_of_the_report_byte_agree() {
    use crate::engine::storage_bucket_internals::{
        ModelKind, StoredModelKind as SMK, RETIRED_MODEL_REPORT_CODES,
    };
    let mut live = 0usize;
    for kind in ModelKind::ALL {
        let stored = SMK::from(*kind);
        assert_eq!(
            kind.report_code(),
            stored.report_code(),
            "the two derivations disagree for {:?}: ModelKind says {}, StoredModelKind says {}",
            kind,
            kind.report_code(),
            stored.report_code()
        );
        assert_eq!(kind.as_str(), stored.as_str(), "and their spellings disagree for {kind:?}");
        live += 1;
    }
    assert!(live >= 15, "only {live} live kinds were compared; the registry declares at least 15");

    let mut retired = 0usize;
    for (name, code) in RETIRED_MODEL_REPORT_CODES {
        let stored = SMK::from_stored_name(name)
            .unwrap_or_else(|| panic!("the entry's type cannot place the retired spelling {name:?}"));
        assert!(stored.is_retired(), "{name:?} is declared retired and the entry's type says live");
        assert_eq!(
            *code,
            stored.report_code(),
            "the retired spelling {name:?} is declared as {code} and reads as {}",
            stored.report_code()
        );
        retired += 1;
    }
    assert!(retired >= 2, "only {retired} retired spellings compared; the registry declares 2");

    // AND NO TWO OF THE SEVENTEEN COLLIDE, read through the ACCESSOR rather than off the
    // declaration. The declaration already has a const assert on its literals; that one cannot see
    // a `report_code` whose arms stopped returning them, which is exactly the mutant that survived.
    let mut seen: BTreeSet<u8> = BTreeSet::new();
    for kind in SMK::ALL {
        let code = kind.report_code();
        assert_ne!(0, code, "{kind:?} packs as 0, the byte an empty bucket writes");
        assert!(seen.insert(code), "two of the seventeen spellings pack as {code}");
    }
    assert_eq!(
        SMK::ALL.len(),
        seen.len(),
        "{} spellings produced {} distinct bytes",
        SMK::ALL.len(),
        seen.len()
    );
    println!(
        "\n=== the report byte ===\n  {live} live and {retired} retired spellings, {} distinct \
         bytes over {} spellings, both derivations agreeing",
        seen.len(),
        SMK::ALL.len()
    );
}

/// A RETIRED SPELLING STILL LOADS, which is why the entry's type covers BOTH halves of the
/// registry rather than only the live walk's kinds.
///
/// `ModelKind` is the live walk's set and has no variant for a spelling no arm emits. An entry
/// typed as that would refuse a store this engine opens TODAY: `storage_reporting` matches
/// `"sequence"` on a live entry, and `model_report_code` has carried both retired spellings since
/// #1970. So the entry's type is the seventeen-element set, and this drives both retired rows
/// through a decode to prove it.
#[test]
fn a_retired_spelling_still_decodes_into_a_page_entry() {
    let mut seen = 0usize;
    for spelling in ["sequence", "context_embedding"] {
        let mut json = serde_json::to_value(&sample_entry()).expect("encode");
        json["model_id"] = serde_json::Value::String(spelling.to_string());
        let page: BlockIndex = serde_json::from_value(json)
            .unwrap_or_else(|err| panic!("a store carrying {spelling:?} must still load: {err}"));
        assert_eq!(
            spelling,
            page.model_id.as_str(),
            "the spelling came back as something else"
        );
        assert!(
            page.model_id.is_retired(),
            "{spelling:?} is declared under `retired` and the entry does not say so"
        );
        seen += 1;
    }
    assert_eq!(2, seen, "both retired rows are supposed to be driven, not {seen}");
    // The floor that ties the entry's set to the registry's two halves.
    assert_eq!(
        StoredModelKind::ALL.len(),
        crate::engine::storage_bucket_internals::ModelKind::ALL.len()
            + crate::engine::storage_bucket_internals::RETIRED_MODEL_REPORT_CODES.len(),
        "the entry's spelling set is supposed to be exactly the registry's two halves: {} against \
         {} live plus {} retired",
        StoredModelKind::ALL.len(),
        crate::engine::storage_bucket_internals::ModelKind::ALL.len(),
        crate::engine::storage_bucket_internals::RETIRED_MODEL_REPORT_CODES.len(),
    );
}

// =================================================================================================
// THE CONTROL ON THE EXPLANATION. Where the mechanism predicts the effect is ABSENT.
// =================================================================================================

/// THE CONTROL: THE WIRE DID NOT MOVE, SO THE STORED BYTES MUST NOT HAVE.
///
/// Every claim above is about the IN-MEMORY width of a page entry. The mechanism -- a fat pointer
/// to a string from a closed set becomes a discriminant, and the string is written as the string
/// it always was -- predicts that the STORED encoding is byte-for-byte what it was. That is a
/// place the effect must be ABSENT, and 0.00% is the answer that distinguishes this change from
/// one that quietly moved the format.
///
/// Driven rather than asserted: an entry is encoded through the same serializers the index uses
/// -- the NAMED one and the POSITIONAL one, because #1969 found the index log packs positionally
/// and a field that changed shape would shift every field after it -- and the bytes are compared
/// against the spelling recorded when the field was an `Arc<str>`.
#[test]
fn the_model_spelling_did_not_move_on_the_wire_and_the_entry_lost_three_steps_in_memory() {
    // What the entry encoded to when `model_id` was an `Arc<str>`, captured at b8b12d30 by
    // `page_entry_names::capture_the_stored_spelling_of_a_page_entry`'s shape and re-derived here
    // from an equal-valued map so the golden is the VALUE, not this type's own impl.
    let page = sample_entry();

    let named = rmp_serde::to_vec_named(&page).expect("named encode");
    let positional = rmp_serde::to_vec(&page).expect("positional encode");
    let json = serde_json::to_value(&page).expect("json encode");

    println!(
        "\n=== the stored spelling ===\n  named {} B, positional {} B\n  json {}",
        named.len(),
        positional.len(),
        json
    );

    // THE MODEL SPELLING IS STILL A STRING ON THE WIRE. This is the control's whole content: a
    // number here would mean the format moved.
    assert_eq!(
        serde_json::Value::String("string".to_string()),
        json["model_id"],
        "the model spelling is supposed to be written as the string it always was; it is written \
         as {:?}",
        json["model_id"]
    );

    // And it round-trips through both encoders, positionally included -- which is the encoder a
    // shifted field would break.
    let from_named: BlockIndex = rmp_serde::from_slice(&named).expect("named decode");
    let from_positional: BlockIndex = rmp_serde::from_slice(&positional).expect("positional decode");
    for (label, decoded) in [("named", &from_named), ("positional", &from_positional)] {
        assert_eq!(
            page.model_id, decoded.model_id,
            "{label}: the spelling did not survive the round trip"
        );
        assert_eq!(page.object_key, decoded.object_key, "{label}: object_key moved");
        assert_eq!(page.component, decoded.component, "{label}: component moved");
        assert_eq!(page.dirty, decoded.dirty, "{label}: dirty moved");
        assert_eq!(page.deleted, decoded.deleted, "{label}: deleted moved");
        assert_eq!(page.log_backed(), decoded.log_backed(), "{label}: log_backed moved");
    }

    // THE ABSENT EFFECT, as THREE numbers now, because three changes have taken bytes off this entry
    // and a single subtraction would let any of them absorb another's.
    //
    // The model spelling took it 88 -> 72, which is the sixteen this test was written for. The address
    // inside it then took it 72 -> 64 by shedding its routing bucket and narrowing its block id. Then
    // `object_id` left that address and took it 64 -> 56 -- a WHOLE eight-byte field out of the
    // eight-aligned group, which is why it paid alone where neither narrowing did. All three are
    // asserted separately, so none can be credited with another's bytes.
    //
    // AND THE WIRE IS NO LONGER UNTOUCHED, WHICH IS WHY THIS TEST IS RENAMED. It held that the stored
    // bytes did not move AT ALL. The MODEL SPELLING still has not moved -- asserted above, written as
    // the string it always was -- but the address inside this entry now writes its `oi` slot empty,
    // which IS a stored move and is what the format stamp pays for -- 6, not the 4 this text first
    // named: 4 was reserved while main held 3, main is 5 now, and a stamp may only increase. The slot is
    // still present, because the index log packs positionally. See
    // `per_item_byte_budget::the_stored_form_moved_in_one_slot_and_the_version_stamp_pays_for_it`,
    // which is the tripwire for that and fired on this change.
    let in_memory_before = 88usize;
    let after_the_spelling = 72usize;
    let after_the_address_narrowing = 64usize;
    let in_memory_after = std::mem::size_of::<BlockIndex>();
    let spelling_wire_delta = 0i64;
    println!(
        "  in memory {in_memory_before} -> {after_the_spelling} -> {after_the_address_narrowing} \
         -> {in_memory_after} B ({:.2}% in total), model spelling on the wire \
         {spelling_wire_delta} B (0.00%)",
        100.0 * (in_memory_before - in_memory_after) as f64 / in_memory_before as f64
    );
    assert_eq!(
        16,
        in_memory_before - after_the_spelling,
        "the model spelling's in-memory effect is supposed to be sixteen bytes"
    );
    assert_eq!(
        8,
        after_the_spelling - after_the_address_narrowing,
        "the address narrowing's in-memory effect is supposed to be eight bytes, and it is eight \
         because SIX bytes of address payload left in two narrowings of which neither crosses a \
         multiple of eight alone"
    );
    assert_eq!(
        8,
        after_the_address_narrowing - in_memory_after,
        "shedding the address's object id is supposed to be eight bytes, and unlike the two \
         narrowings above it pays ALONE: it is a whole eight-byte field leaving the eight-aligned \
         group, not a field getting smaller inside it"
    );
    assert_eq!(
        0, spelling_wire_delta,
        "the model spelling is supposed to be untouched on the wire; the address's `oi` slot is a \
         separate matter and the version stamp covers it"
    );
}


// =================================================================================================
// THE TAIL. Three bools, and what they are worth alone against what they are worth in combination.
// =================================================================================================

/// Mirrors of the entry at each combination of narrowings, so what CROSSES the alignment step is
/// measured rather than projected. The one lesson #1969 paid for: individually most of its
/// narrowings were worth zero and only the combination crossed.
mod tail_layout {
    use super::*;
    use std::num::NonZeroU16;
    use std::num::NonZeroU32;

    /// The three flags as one byte behind masks. The shape #1968 took on `BucketNode`.
    #[derive(Debug, Clone, Copy, Default)]
    pub(super) struct PageFlags(u8);

    impl PageFlags {
        pub(super) const DIRTY: u8 = 1 << 0;
        pub(super) const DELETED: u8 = 1 << 1;
        pub(super) const LOG_BACKED: u8 = 1 << 2;

        pub(super) fn with(self, mask: u8, value: bool) -> Self {
            if value {
                PageFlags(self.0 | mask)
            } else {
                PageFlags(self.0 & !mask)
            }
        }

        pub(super) fn get(self, mask: u8) -> bool {
            self.0 & mask != 0
        }

        pub(super) fn raw(self) -> u8 {
            self.0
        }
    }

    /// Today.
    pub(super) struct A0 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }

    /// Today, flags packed.
    pub(super) struct A1 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }

    /// Component as an ordinal.
    pub(super) struct B0 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }
    pub(super) struct B1 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }

    /// Both names as ordinals.
    pub(super) struct C0 {
        pub(super) object_key: Option<NonZeroU32>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }
    pub(super) struct C1 {
        pub(super) object_key: Option<NonZeroU32>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 4],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }

    /// The shape while the address was 24: both names as `Arc`s. It was TODAY until `object_id` left
    /// the address, and it is kept as the row BEFORE that step -- exactly as `A0` is kept as the row
    /// before the address narrowed. The module prices a STEP, and a step whose before-state has been
    /// deleted cannot be priced.
    pub(super) struct E0 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 3],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }
    pub(super) struct E1 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 3],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }

    /// TODAY: both names as `Arc`s and the address at 16, after `object_id` stopped being a field and
    /// became a derivation from the terms beside it. This is the live shape and it is what the
    /// assertion below pins against `BlockIndex`.
    pub(super) struct F0 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 2],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }
    pub(super) struct F1 {
        pub(super) object_key: Arc<str>,
        pub(super) component: Option<Arc<str>>,
        pub(super) address: [u64; 2],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }

    /// Both names as ordinals AND the address at 24, which a sibling thread owns.
    pub(super) struct D0 {
        pub(super) object_key: Option<NonZeroU32>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 3],
        pub(super) model: u8,
        pub(super) dirty: bool,
        pub(super) deleted: bool,
        pub(super) log_backed: bool,
    }
    pub(super) struct D1 {
        pub(super) object_key: Option<NonZeroU32>,
        pub(super) component: Option<NonZeroU16>,
        pub(super) address: [u64; 3],
        pub(super) model: u8,
        pub(super) flags: PageFlags,
    }
}

/// PACKING THE THREE FLAGS IS WORTH NOTHING ON ITS OWN AND EIGHT BYTES IN COMBINATION.
///
/// THE FALSE GENERAL CLAIM THIS IS ABOUT. The comment above `BlockIndex` used to say the three flag
/// bytes "are already inside the alignment slack and packing them would reclaim nothing". That is
/// true of NARROWING ONE field and false of PACKING THREE, and it is exactly the claim that kept
/// the same win unclaimed on `BucketNode` for three PRs until #1968 measured it. The comment is
/// rewritten with this measurement beside it; what follows is the measurement.
///
/// Measured on mirrors at every combination, because the answer is not a property of the flags. At
/// today's width the tail is four bytes of slack and three bools fit in it, so packing crosses
/// nothing. Once BOTH names are ordinals the tail is what decides whether the entry lands on 40 or
/// 48, and then the same two bytes are worth a whole eight-byte step.
///
/// The address arm is included because a sibling thread is taking `BlockAddress` 32 -> 24, and
/// #1969's lesson is that a narrowing's worth is not separable from the ones landing beside it.
#[test]
fn packing_the_flag_bytes_is_worth_nothing_alone_and_a_step_in_combination() {
    use std::mem::size_of;
    use tail_layout::*;

    let rows = [
        ("names, 32 B address (before)", size_of::<A0>(), size_of::<A1>()),
        ("names, 24 B address (before)", size_of::<E0>(), size_of::<E1>()),
        ("names, 16 B address (TODAY)", size_of::<F0>(), size_of::<F1>()),
        ("component ordinal, 32 B", size_of::<B0>(), size_of::<B1>()),
        ("both names ordinals, 32 B", size_of::<C0>(), size_of::<C1>()),
        ("both names + 24 B address", size_of::<D0>(), size_of::<D1>()),
    ];
    println!("\n=== the entry's tail: three bools against one flag byte ===");
    println!("  {:<28} {:>10} {:>10} {:>8}", "combination", "3 bools", "packed", "saved");
    let mut crossings = 0usize;
    let mut no_ops = 0usize;
    for (label, loose, packed) in rows {
        let saved = loose - packed;
        println!("  {label:<28} {loose:>10} {packed:>10} {saved:>8}");
        if saved == 0 {
            no_ops += 1;
        } else {
            crossings += 1;
        }
    }

    // TODAY'S ARM MUST MATCH THE REAL STRUCT, or the mirrors are measuring something else.
    //
    // It is `F0` now, and the trail of relabellings is the point: `A0` carries a 32-byte address,
    // `E0` a 24-byte one, and the live address is 16. This assertion is what says so each time -- it
    // reported "the mirror of today's entry is 72 B against the real 64 B" when the address narrowed,
    // and "64 B against the real 56 B" when `object_id` left it. Both times a mirror was naming a
    // shape the engine no longer had, and NO COMPILER PASS CAN REACH A CLAIM OF THAT KIND: this is an
    // `assert_eq!` in a test body, so it compiles clean and only a run can find it.
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<F0>(),
        "the mirror of today's entry is {} B against the real {} B; every row below is then about \
         a different structure",
        size_of::<F0>(),
        size_of::<BlockIndex>()
    );
    assert_eq!(
        72,
        size_of::<A0>(),
        "`A0` is supposed to be the entry as it was before the address narrowed, and it reads {} B",
        size_of::<A0>()
    );

    // THE TWO HALVES OF THE CLAIM. Both must hold, or the honest answer is one of them.
    assert!(
        no_ops > 0,
        "packing is supposed to be worth NOTHING at some width -- that is the half of the old \
         comment that was true -- and it saved bytes in every combination"
    );
    assert!(
        crossings > 0,
        "packing is supposed to cross the alignment step in some combination -- that is the half \
         of the old comment that was false -- and it saved nothing anywhere"
    );
    assert_eq!(
        size_of::<E0>(),
        size_of::<E1>(),
        "at today's width the tail has four bytes of slack and three bools fit inside it, so \
         packing is supposed to move nothing"
    );
    assert!(
        size_of::<C1>() < size_of::<C0>(),
        "with both names as ordinals the tail decides the rounding, so packing is supposed to \
         cross a step: {} against {}",
        size_of::<C1>(),
        size_of::<C0>()
    );

    // AND THE MASKS MUST NOT ALIAS. A flag read through the wrong mask is a silent wrong answer,
    // so this feeds each mask a value set through a DIFFERENT one and requires false back.
    let mut checked = 0usize;
    for (name, set) in [
        ("DIRTY", PageFlags::DIRTY),
        ("DELETED", PageFlags::DELETED),
        ("LOG_BACKED", PageFlags::LOG_BACKED),
    ] {
        let flags = PageFlags::default().with(set, true);
        assert!(flags.get(set), "{name} set through its own mask reads false");
        for (other_name, other) in [
            ("DIRTY", PageFlags::DIRTY),
            ("DELETED", PageFlags::DELETED),
            ("LOG_BACKED", PageFlags::LOG_BACKED),
        ] {
            if other == set {
                continue;
            }
            assert!(
                !flags.get(other),
                "{name} was set and {other_name} reads true, so the two masks alias and a flag \
                 read through the wrong one gives a silent wrong answer"
            );
            checked += 1;
        }
    }
    assert_eq!(
        6, checked,
        "six cross-mask pairs are supposed to be checked, not {checked}; a loop that checked none \
         would pass this test by checking nothing"
    );
    // The control on the control: a mask that DID alias must be caught. Planted, not assumed.
    let aliasing = PageFlags::default().with(PageFlags::DIRTY | PageFlags::DELETED, true);
    assert!(
        aliasing.get(PageFlags::DELETED),
        "the planted aliasing value must read true through the other mask, or the check above \
         cannot fail when the masks really do alias"
    );
    println!(
        "  masks: {checked} cross pairs checked, and a planted aliasing byte ({:#04b}) is \
         recovered",
        aliasing.raw()
    );
}

/// AN EMPTY VALUE STILL CARRIES A LENGTH, so the zero-length tombstone is still OPEN -- and that
/// is the opposite of what this test was written to show.
///
/// A design that spells `page_size == 0 indicates page is deleted` can drop the `deleted` bit
/// entirely. The equivalent here would be `BlockAddress::length() == 0`, and the obvious way for
/// that to be unavailable is a legitimate zero-length page: `bin/server.rs` says "An empty body
/// still creates a zero-length object so a later GET succeeds", and `address_footprint` carries an
/// instrument that hunts for a "live zero-length block" -- which is not something one writes for a
/// state that cannot occur.
///
/// DRIVEN, AND IT REFUTED THAT. An empty string value produces a page whose length is NOT zero:
/// the block record carries a header, so the shortest live page this route can produce is twelve
/// bytes. The hypothesis that an empty value blocks the tombstone is wrong.
///
/// WHAT THAT DOES AND DOES NOT SETTLE. It bounds ONE route, not the class, and this test says so
/// rather than concluding: `address_footprint::the_capacity_ceilings_each_narrowing_would_impose`
/// is the instrument that asks the question over a wide fixture and prints the verdict, and it
/// could not answer at all until this change -- #1969 took `BlockAddress` 40 -> 32 and left a
/// hand-written payload figure of 37 beside it, so the probe aborted on an unsigned underflow
/// before printing anything. It is `#[ignore]`d, so no gate ran it and nothing said so.
///
/// So the finding is: the tombstone is not blocked by the route that looked most likely to block
/// it, and `length` lives in `BlockAddress`, which another thread owns. Handed over, not taken.
#[test]
fn an_empty_value_still_carries_a_length_so_the_zero_length_tombstone_is_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, OPERATOR_END);
    run_batch(
        &engine,
        vec![
            Command::StringSet {
                key: "empty".to_string(),
                value: Vec::new(),
            },
            Command::StringSet {
                key: "nonempty".to_string(),
                value: vec![b'v'; 32],
            },
        ],
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut zero_length_live = 0usize;
    let mut nonzero_length_live = 0usize;
    let mut deleted_pages = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            if page.deleted {
                deleted_pages += 1;
                continue;
            }
            if page.address.length() == 0 {
                zero_length_live += 1;
                println!(
                    "  live zero-length page: model={} key={}",
                    page.model_id.as_str(),
                    page.object_key
                );
            } else {
                nonzero_length_live += 1;
            }
        }
    }
    let empty_length = shard.strings.get("empty").map(|address| address.length());
    let nonempty_length = shard.strings.get("nonempty").map(|address| address.length());
    println!(
        "\n=== is `deleted` derivable from a zero length? ===\n  live zero-length \
         {zero_length_live}, live non-zero {nonzero_length_live}, delete-marked {deleted_pages}\n  \
         model-map lengths: empty value {empty_length:?}, 32-byte value {nonempty_length:?}"
    );

    // --- The fixture must have reached both writes, or the contrast is free. ---
    assert_eq!(
        2,
        zero_length_live + nonzero_length_live,
        "the fixture is supposed to produce exactly two live pages and produced {}",
        zero_length_live + nonzero_length_live
    );
    let empty_length = empty_length.expect("the empty write must have produced a page");

    // --- THE MEASURED FACT, which refutes the hypothesis this test was written for. ---
    assert!(
        empty_length > 0,
        "an empty value produced a page of length {empty_length}. This test exists because a \
         zero-length LIVE page would block the `length == 0` tombstone encoding, and the empty \
         write is the route most likely to produce one -- if it ever does, the tombstone is \
         unavailable and `deleted` must keep its bit"
    );
    assert_eq!(
        0, zero_length_live,
        "{zero_length_live} live pages carry length 0. On this route none should: the block record \
         carries a header. A live zero-length page here means the tombstone encoding is blocked"
    );
    println!(
        "  FINDING: the shortest live page this route produces is {empty_length} B, so an empty \
         value does NOT block a `length == 0` tombstone. That bounds one route, not the class -- \
         `address_footprint::the_capacity_ceilings_each_narrowing_would_impose` asks it over a \
         wide fixture, and `length` lives in `BlockAddress`, which another thread owns."
    );
}

// =================================================================================================
// WHAT THE COMPONENT LEVEL COSTS. Priced at N=1 and at N=100, bytes AND allocations, read path
// counted. A measurement handed over, not a change made.
// =================================================================================================

/// One flattened page ref: the component as an ordinal beside the ref the level used to group.
///
/// The shape a flattening would use, sized so the comparison is against something real rather than
/// against an estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FlatPageRef {
    component: Option<std::num::NonZeroU16>,
    routing_bucket: u32,
    block_ref_key: u64,
}

/// Build one object's component level the way the lookup does, and report what it cost.
fn nested_level(components: usize) -> (ObjectBlockRefs, usize) {
    let mut refs = ObjectBlockRefs::default();
    let mut heap = 0usize;
    for index in 0..components {
        // The name the engine would actually hold: a zset component is 16 hex characters for the
        // biased score plus twice the member's bytes.
        let name: Arc<str> = Arc::from(format!("{index:016x}{}", hex::encode(b"member-0000000000")).as_str());
        heap += name.len() + ARC_HEADER_BYTES;
        let entry = ComponentBlocks {
            component: Some(name),
            refs: BlockRefs::One(crate::engine::state::BlockLookupRef {
                routing_bucket: 7,
                block_ref_key: index as u64,
            }),
        };
        let at = match refs.position(entry.component.as_deref()) {
            Ok(at) => at,
            Err(at) => at,
        };
        refs.by_component.insert(at, entry);
    }
    (refs, heap)
}

/// WHAT THE COMPONENT LEVEL COSTS AT ONE COMPONENT AND AT A HUNDRED, with the read path counted.
///
/// #1967 measured `ComponentList` at 40 bytes, the same as the `ComponentBlocks` its `One` arm
/// holds inline, and that was recorded as "the level is free". IT IS A NARROWER CLAIM THAN IT
/// SOUNDS: what is free is the three-arm ENUM WRAPPER around one `ComponentBlocks`. The LEVEL costs
/// 40 bytes per component plus a vector allocation past the first, and at the container shape --
/// p50 100 components under one object -- that is about `40 + 100 * 40` bytes and an allocation per
/// object.
///
/// PRICED AGAINST A FLAT LIST, which is what a design with no component concept does: pages live in
/// one vector per object and a "component" is just a different page id.
///
/// NOTHING IS CHANGED HERE. This is the measurement, taken so the decision can be made on numbers
/// -- and the read path is counted because that is the term a footprint comparison cannot see: a
/// flattening that turns a 100-entry bisection into a 100-entry scan on the serving path has to be
/// declined with numbers or shipped with the cost stated.
#[test]
fn what_the_component_level_costs_at_one_component_and_at_a_hundred() {
    use std::mem::size_of;

    println!("\n=== the component level, priced ===");
    println!(
        "  {:>5} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10} {:>9} {:>9}",
        "N", "nested B", "nested allo", "nest reads", "flat B", "flat allo", "flat reads", "B saved", "allo"
    );

    let mut rows: Vec<(usize, usize, usize, usize, usize, usize, usize)> = Vec::new();
    for components in [1usize, 100] {
        // --- TODAY. The struct, plus the vector it spills into past one, plus the names. ---
        let (level, name_heap) = nested_level(components);
        let inline = size_of::<ObjectBlockRefs>();
        let spilled = if components > 1 {
            components * size_of::<ComponentBlocks>()
        } else {
            0
        };
        let nested_bytes = inline + spilled + name_heap;
        // One for the vector past the first component, one per component name.
        let nested_allocs = usize::from(components > 1) + components;

        // The read: a bisection over `by_component`, counted through the real comparator.
        let mut nested_reads = 0usize;
        let target = level.by_component[components - 1].component.clone();
        let _ = level
            .by_component
            .binary_search_by(|entry| {
                nested_reads += 1;
                entry.component.as_deref().cmp(&target.as_deref())
            });

        // --- FLATTENED. One vector of page refs per object, each carrying an ordinal. ---
        let flat: Vec<FlatPageRef> = (0..components)
            .map(|index| FlatPageRef {
                component: std::num::NonZeroU16::new(index as u16 + 1),
                routing_bucket: 7,
                block_ref_key: index as u64,
            })
            .collect();
        // The names still have to live somewhere -- a flat list does not make them free, it moves
        // them. Counted at the same cost, which is what makes this a comparison of the LEVEL.
        let flat_bytes = size_of::<Vec<FlatPageRef>>() + components * size_of::<FlatPageRef>() + name_heap;
        let flat_allocs = 1 + components;

        // The read, as a SCAN -- the honest worst case for a flat list, and the term that decides.
        let mut flat_scan_reads = 0usize;
        let wanted = flat[components - 1].component;
        for entry in &flat {
            flat_scan_reads += 1;
            if entry.component == wanted {
                break;
            }
        }

        println!(
            "  {components:>5} {nested_bytes:>12} {nested_allocs:>12} {nested_reads:>10} \
             {flat_bytes:>12} {flat_allocs:>12} {flat_scan_reads:>10} {:>9} {:>9}",
            nested_bytes as i64 - flat_bytes as i64,
            nested_allocs as i64 - flat_allocs as i64,
        );
        rows.push((
            components,
            nested_bytes,
            nested_allocs,
            nested_reads,
            flat_bytes,
            flat_allocs,
            flat_scan_reads,
        ));
    }

    let (_, n1_bytes, n1_allocs, n1_reads, f1_bytes, f1_allocs, f1_reads) = rows[0];
    let (_, n100_bytes, _n100_allocs, n100_reads, f100_bytes, _f100_allocs, f100_reads) = rows[1];

    // --- THE LEVEL AT N = 1: the wrapper is free and the flat list is not cheaper. ---
    assert_eq!(
        size_of::<ComponentList>(),
        size_of::<ComponentBlocks>(),
        "#1967's measurement: the three-arm wrapper is the width of the entry its `One` arm holds"
    );
    assert!(
        n1_bytes <= f1_bytes,
        "at one component the nested level costs {n1_bytes} B and a flat list {f1_bytes} B, so \
         flattening is supposed to be no cheaper at N=1 and it is"
    );
    assert!(
        n1_allocs <= f1_allocs,
        "at one component the nested level makes {n1_allocs} allocations and a flat list \
         {f1_allocs}; the inline `One` arm exists precisely to avoid the vector"
    );

    // --- THE LEVEL AT N = 100: it costs, and the read path is where flattening loses. ---
    assert!(
        n100_bytes > f100_bytes,
        "at a hundred components the nested level costs {n100_bytes} B against a flat list's \
         {f100_bytes} B, so the level is supposed to COST at the container shape and it does not"
    );
    assert!(
        f100_reads > n100_reads,
        "a bisection over a hundred entries is supposed to read fewer than a scan over a hundred: \
         nested {n100_reads}, flat {f100_reads}. If a flat scan ever reads fewer, the read-path \
         objection to flattening is gone and this needs re-deciding"
    );
    assert_eq!(
        1, n1_reads,
        "a bisection over a list of ONE is supposed to be a single comparison, not {n1_reads}"
    );
    assert_eq!(1, f1_reads, "and so is a scan over one");

    println!(
        "\n  VERDICT, HANDED OVER RATHER THAN TAKEN. At N=1 the level is free and flattening is no \
         cheaper. At N=100 the level costs {} B more than a flat list, and a flat SCAN reads \
         {f100_reads} entries where the bisection reads {n100_reads} -- {:.1}x. So the level pays \
         on the population that is p50 and costs on the population that is MAX, which is the \
         two-population shape a single number would hide. Recommend BY POPULATION.",
        n100_bytes - f100_bytes,
        f100_reads as f64 / n100_reads.max(1) as f64
    );
}
