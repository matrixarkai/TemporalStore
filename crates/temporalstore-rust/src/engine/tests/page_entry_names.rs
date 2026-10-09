// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THREE NAMES A PAGE ENTRY CARRIES, and whether they are per-page or per-bucket facts.
//!
//! #1959 ranked `BlockIndex` as the next dominant per-item term -- per PAGE rather than per
//! bucket, and so 52.0x the `BucketNode` total in a container store -- and opened up its 104
//! bytes: the address was 48, three flag bytes sat inside the alignment rounding, and **the three
//! names were 48**, the largest group. It called them "the three shared names" and priced holding
//! them as thin pointers at 80 bytes an entry.
//!
//! EVERY ONE OF THOSE FIGURES IS HISTORY NOW, and they are left in the past tense rather than
//! updated because they are what this module was written against. The entry is 64, the address 24
//! and the names 33 -- `model_id` stopped being a fat pointer and became a one-byte spelling -- and
//! a thin pointer would price at 48 rather than 80. `page_entry_name_pointer.rs` carries that
//! arithmetic, reads every width off the field rather than off a type named at the call site, and
//! declines the change.
//!
//! "SHARED" IS AN ASSUMPTION ABOUT WHERE THE NAMES COME FROM, and this module measures it instead.
//! The three are `object_key`, `model_id` and `component`. If all three were the same for every
//! page of a routing bucket, storing them once per page would be pure duplication and the entry
//! could carry a handle to the bucket's copy. That is the change this module was written to
//! justify, and it does not survive the measurement.
//!
//! WHAT THE COUNTS SAY, over real stores seeded through the engine:
//!
//!   * `component` is a PER-PAGE fact, by construction. A container key's fields, members and
//!     elements are each their own page and they all route to the container key's one bucket; the
//!     component is the only thing that tells them apart. A bucket of 100 pages holds 100 distinct
//!     components. Hoisting it would file a hundred hash fields as one page.
//!   * `object_key` and `model_id` are per-OBJECT facts, and a routing bucket is not an object.
//!     A bucket is `hash(key) % routing_bucket_count`, so whether one bucket holds one object or
//!     forty is a property of the SHARD'S CONFIGURED BUCKET RANGE and nothing else. At the range
//!     `load_shard` uses -- 0..u32::MAX, which is what #1958 and #1959 both measured on -- a
//!     bucket holds one object and the names look shared. At `0..=1023`, which is what
//!     `ingestion.rs` and `metaserver.rs` spell for a real shard, forty objects share a bucket and
//!     the names are not shared at all.
//!
//! SO THE PREMISE IS CONFIGURATION-DEPENDENT, WHICH IS THE WORST WAY FOR IT TO BE WRONG. A change
//! that hoisted the names to the node would pass every test on a store built by `load_shard` and
//! lose pages on a store built by the cluster path -- and it would lose them SILENTLY, because a
//! page filed under the wrong object name is still on its slab and nothing looks for it. Both
//! ranges are measured here, in the same module, so the two cannot be confused again.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::engine::state::BlockIndex;
use crate::control::LoadShardRequest;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe s full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// ---------------------------------------------------------------------------------------------
// THE INSTRUMENT.
// ---------------------------------------------------------------------------------------------

/// How many DISTINCT values of each of the three names a routing bucket's pages carry, as bucket
/// counts keyed by the distinct count.
///
/// A histogram and not a mean for the reason #1959 established: a mean of "1.02 object keys a
/// bucket" is consistent with a store where every bucket holds one object and with a store where
/// one bucket in fifty holds two, and only the second loses data when the name is hoisted.
#[derive(Debug, Default, Clone)]
struct NameSpread {
    object_key: BTreeMap<usize, usize>,
    model_id: BTreeMap<usize, usize>,
    component: BTreeMap<usize, usize>,
    /// Buckets keyed by pages held, so the container case can be asserted reached.
    pages_held: BTreeMap<usize, usize>,
    /// Buckets whose every page carries the SAME component, counted separately: a bucket holding
    /// one page trivially does, and a claim built on those would be vacuous.
    multi_page_buckets_with_one_component: usize,
    multi_page_buckets: usize,
}

impl NameSpread {
    fn buckets(&self) -> usize {
        self.pages_held.values().copied().sum()
    }

    fn pages(&self) -> usize {
        self.pages_held.iter().map(|(held, n)| held * n).sum()
    }

    fn buckets_with_more_than_one(map: &BTreeMap<usize, usize>) -> usize {
        map.iter().filter(|(d, _)| **d > 1).map(|(_, n)| *n).sum()
    }

    fn widest(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  buckets={} pages={} widest bucket={} page(s)",
            self.buckets(),
            self.pages(),
            Self::widest(&self.pages_held)
        );
        for (name, map) in [
            ("object_key", &self.object_key),
            ("model_id", &self.model_id),
            ("component", &self.component),
        ] {
            let buckets = self.buckets();
            let multi = Self::buckets_with_more_than_one(map);
            println!(
                "  {name:<10} : buckets holding >1 distinct value = {multi:>8}  ({:>7.3}%)  widest = {}",
                if buckets == 0 {
                    0.0
                } else {
                    100.0 * multi as f64 / buckets as f64
                },
                Self::widest(map)
            );
            for (distinct, count) in map.iter().take(6) {
                println!("      {distinct:>5} distinct : {count:>8} buckets");
            }
            if map.len() > 6 {
                println!("      ... {} further distinct-counts", map.len() - 6);
            }
        }
        println!(
            "  multi-page buckets = {}, of which all-one-component = {}",
            self.multi_page_buckets, self.multi_page_buckets_with_one_component
        );
    }
}

/// Walk every bucket of the loaded shard and count distinct names across its pages.
///
/// `component` is counted over the OPTION, so a bucket where some pages have a component and some
/// do not counts two -- which is the honest answer for "could one stored value serve them all".
fn name_spread(engine: &TemporalEngine) -> NameSpread {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut spread = NameSpread::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        let mut keys: BTreeSet<&str> = BTreeSet::new();
        let mut models: BTreeSet<&str> = BTreeSet::new();
        let mut components: BTreeSet<Option<&str>> = BTreeSet::new();
        let mut held = 0usize;
        for (_, page) in bucket.block_index.iter() {
            held += 1;
            keys.insert(page.object_key.as_ref());
            models.insert(page.model_id.as_str());
            components.insert(page.component.as_deref());
        }
        *spread.pages_held.entry(held).or_default() += 1;
        *spread.object_key.entry(keys.len()).or_default() += 1;
        *spread.model_id.entry(models.len()).or_default() += 1;
        *spread.component.entry(components.len()).or_default() += 1;
        if held > 1 {
            spread.multi_page_buckets += 1;
            if components.len() == 1 {
                spread.multi_page_buckets_with_one_component += 1;
            }
        }
    }
    spread
}

pub(super) fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ))
}

/// Load shard 1 over an EXPLICIT routing-bucket range.
///
/// `load_shard` hard-codes `0..u32::MAX`; `ingestion.rs` and `metaserver.rs` both spell
/// `0..=1023` for a shard of a real cluster. Which one a store was built under decides every
/// number below, so no arm here is allowed to take the default implicitly.
pub(super) fn load_shard_over(engine: &TemporalEngine, end_routing_bucket: u32) {
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

fn ack(response: &crate::types::BatchExecuteResponse) {
    assert!(response.status.ok, "seed must ack: {:?}", response.status);
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        ack(&response);
    }
}

/// Keys that route one to a bucket: plain strings.
pub(super) fn seed_routed_keys(engine: &TemporalEngine, strings_n: usize) {
    run_batch(
        engine,
        (0..strings_n)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Container keys: a hash, a set, a sorted set and a list, each of `members` elements. Every
/// element is its own page and they all route to the container key's one bucket.
pub(super) fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
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

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT.
// ---------------------------------------------------------------------------------------------

/// ARE THE THREE NAMES THE SAME FOR EVERY PAGE OF A BUCKET? Counted, at two corpus sizes and over
/// BOTH routing-bucket ranges this engine ships.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted. Counts are immune to it --
/// allocation bytes are not, at about six bytes a character -- which is why the counts carry the
/// claim and no byte figure appears in this test.
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn the_three_names_in_a_page_entry_are_per_object_and_a_bucket_is_not_an_object() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (label, wide-range spread, cluster-range spread)
    let mut wide_routed: Vec<NameSpread> = Vec::new();
    let mut wide_container: Vec<NameSpread> = Vec::new();
    let mut narrow_routed: Vec<NameSpread> = Vec::new();
    let mut narrow_container: Vec<NameSpread> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        for (range_label, end_routing_bucket) in
            [("0..u32::MAX (load_shard)", u32::MAX), ("0..=1023 (a cluster shard)", 1_023u32)]
        {
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_routed_keys(&engine, records);
                let spread = name_spread(&engine);
                spread.report(&format!("{label}, {range_label}: keys that route one to a bucket"));
                if end_routing_bucket == u32::MAX {
                    wide_routed.push(spread);
                } else {
                    narrow_routed.push(spread);
                }
            }
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_container_keys(&engine, records / 100, 100);
                let spread = name_spread(&engine);
                spread.report(&format!("{label}, {range_label}: container keys, 100 elements each"));
                if end_routing_bucket == u32::MAX {
                    wide_container.push(spread);
                } else {
                    narrow_container.push(spread);
                }
            }
        }
    }

    assert_eq!(
        8,
        path_lengths.len(),
        "all eight arms must have run, or a comparison below compares an arm with itself"
    );
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms ({} then {length})",
            path_lengths[0]
        );
    }

    // --- Denominators, read off the shard, before anything divides by them. ---
    for spread in wide_routed
        .iter()
        .chain(wide_container.iter())
        .chain(narrow_routed.iter())
        .chain(narrow_container.iter())
    {
        assert!(spread.buckets() > 0, "denominator: no routing buckets in this arm");
        assert!(spread.pages() > 0, "denominator: no page entries in this arm");
    }

    // --- 1. `component` is a PER-PAGE fact, in both ranges. ---
    //
    // The container arm is where this is visible at all, and the assertion is EQUALITY with the
    // pages held rather than ">1": a bucket of a hundred pages holding two distinct components
    // would still refute hoisting, but it would not be the structure this engine actually has.
    for (index, spread) in wide_container.iter().chain(narrow_container.iter()).enumerate() {
        assert_eq!(
            0, spread.multi_page_buckets_with_one_component,
            "container arm {index}: {} multi-page buckets hold ONE distinct component; every page \
             of a container is a distinct field, member or element, so a bucket whose pages shared \
             a component would mean the component had stopped telling pages apart",
            spread.multi_page_buckets_with_one_component
        );
        assert!(
            spread.multi_page_buckets > 0,
            "container arm {index}: no bucket holds more than one page, so this arm cannot say \
             anything about whether a name is shared ACROSS pages -- it would be vacuous"
        );
        assert!(
            NameSpread::widest(&spread.component) >= 100,
            "container arm {index}: the widest bucket holds {} distinct components; the fixture is \
             supposed to reach the container case where the per-page entry dominates at 52x",
            NameSpread::widest(&spread.component)
        );
    }

    // --- 2. Over 0..u32::MAX the OBJECT names look per-bucket. This is the trap. ---
    for (index, spread) in wide_routed.iter().chain(wide_container.iter()).enumerate() {
        assert_eq!(
            0,
            NameSpread::buckets_with_more_than_one(&spread.object_key),
            "wide arm {index}: a bucket already holds more than one object key at 0..u32::MAX"
        );
        assert_eq!(
            0,
            NameSpread::buckets_with_more_than_one(&spread.model_id),
            "wide arm {index}: a bucket already holds more than one model id at 0..u32::MAX"
        );
    }

    // --- 3. Over 0..=1023 they are NOT, and that is the configuration a cluster shard uses. ---
    for (index, spread) in narrow_routed.iter().enumerate() {
        let multi = NameSpread::buckets_with_more_than_one(&spread.object_key);
        assert!(
            multi > 0,
            "cluster-range routed arm {index}: NO bucket holds more than one object key. The \
             whole point of this arm is that a finite bucket space forces objects to share a \
             bucket; if it does not reach that, every claim below is untested"
        );
        // Not EVERY bucket: at 4,000 keys over 1,024 buckets the tail of the hash still leaves a
        // few holding one key, and at 40,000 it leaves none. The claim is that the OVERWHELMING
        // majority hold several -- a rate one unlucky bucket could not produce.
        assert!(
            10 * multi >= 9 * spread.buckets(),
            "cluster-range routed arm {index}: only {multi} of {} buckets hold more than one \
             object key; at 1,024 buckets and thousands of keys nearly all of them should, and a \
             rate this low would mean the arm is not the configuration it is named for",
            spread.buckets()
        );
        assert!(
            NameSpread::widest(&spread.object_key) > 1,
            "cluster-range routed arm {index}: widest bucket holds {} distinct object keys",
            NameSpread::widest(&spread.object_key)
        );
    }

    // --- 4. And a cluster-range CONTAINER store mixes model ids inside one bucket too. ---
    for (index, spread) in narrow_container.iter().enumerate() {
        assert!(
            NameSpread::buckets_with_more_than_one(&spread.model_id) > 0,
            "cluster-range container arm {index}: no bucket holds more than one model id, so the \
             claim that the KIND is not per-bucket either is untested here"
        );
    }

    // --- 5. The two ranges disagree about the same question, which is the finding. ---
    assert_eq!(
        0,
        NameSpread::buckets_with_more_than_one(&wide_routed[0].object_key),
        "control: the wide arm must be the one where the names look shared"
    );
    assert!(
        NameSpread::buckets_with_more_than_one(&narrow_routed[0].object_key) > 0,
        "control: the cluster arm must be the one where they are not. If both arms agreed, this \
         module would be measuring one configuration twice and reporting it as two"
    );
}

// ---------------------------------------------------------------------------------------------
// WHAT THE ENTRY CAN STOP DOING, given that it must keep all three names.
// ---------------------------------------------------------------------------------------------

/// Distinct ALLOCATIONS of each name behind the pages of one object, counted by pointer.
///
/// Contents cannot tell one shared allocation from two equal ones; pointers can. This is the same
/// instrument `part4::the_block_and_the_lookup_point_at_one_object_key` uses, applied across the
/// pages of ONE OBJECT rather than between one page and the lookup -- which is the axis that test
/// cannot see, because every object in its fixture has exactly one page.
#[derive(Debug, Default, Clone)]
struct KeyAllocationCensus {
    /// Objects keyed by how many distinct object-key allocations their pages hold.
    object_key: BTreeMap<usize, usize>,
    /// Objects keyed by how many distinct model-id allocations their pages hold.
    model_id: BTreeMap<usize, usize>,
    objects: usize,
    pages: usize,
    /// Sum over objects of (distinct allocations - 1), i.e. copies that carry no new information.
    surplus_object_key_allocations: usize,
    /// Bytes those surplus copies hold: the text plus an `Arc` header of two words.
    surplus_object_key_bytes: usize,
}

impl KeyAllocationCensus {
    fn objects_holding_more_than_one(map: &BTreeMap<usize, usize>) -> usize {
        map.iter().filter(|(d, _)| **d > 1).map(|(_, n)| *n).sum()
    }

    fn widest(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!("  objects={} pages={}", self.objects, self.pages);
        println!(
            "  object_key : objects holding >1 allocation = {} , widest = {}",
            Self::objects_holding_more_than_one(&self.object_key),
            Self::widest(&self.object_key)
        );
        println!(
            "  model_id   : objects holding >1 allocation = {} , widest = {}",
            Self::objects_holding_more_than_one(&self.model_id),
            Self::widest(&self.model_id)
        );
        println!(
            "  surplus object-key allocations = {} ({:.4} a page), {} B ({:.2} B a page)",
            self.surplus_object_key_allocations,
            if self.pages == 0 {
                0.0
            } else {
                self.surplus_object_key_allocations as f64 / self.pages as f64
            },
            self.surplus_object_key_bytes,
            if self.pages == 0 {
                0.0
            } else {
                self.surplus_object_key_bytes as f64 / self.pages as f64
            }
        );
    }
}

/// An `Arc<str>`'s own allocation: the text, behind two words of strong/weak count.
const ARC_HEADER_BYTES: usize = 16;

fn key_allocation_census(engine: &TemporalEngine) -> KeyAllocationCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // (model_id, object_key) -> the distinct allocation addresses its pages hold.
    let mut by_object: BTreeMap<(String, String), (BTreeSet<usize>, BTreeSet<usize>, usize)> =
        BTreeMap::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            let slot = by_object
                .entry((page.model_id.to_string(), page.object_key.to_string()))
                .or_default();
            slot.0.insert(page.object_key.as_ptr() as usize);
            // The spelling is a `&'static str` off the registry now, not an allocation: one
            // address per kind for the whole process, so this census reads 1 BY CONSTRUCTION
            // rather than because a pool happened to be consulted. Kept as the control it was.
            slot.1.insert(page.model_id.as_str().as_ptr() as usize);
            slot.2 += 1;
        }
    }
    let mut census = KeyAllocationCensus::default();
    for ((_, object_key), (key_ptrs, model_ptrs, pages)) in by_object.iter() {
        census.objects += 1;
        census.pages += pages;
        *census.object_key.entry(key_ptrs.len()).or_default() += 1;
        *census.model_id.entry(model_ptrs.len()).or_default() += 1;
        let surplus = key_ptrs.len().saturating_sub(1);
        census.surplus_object_key_allocations += surplus;
        census.surplus_object_key_bytes += surplus * (object_key.len() + ARC_HEADER_BYTES);
    }
    census
}

/// THE PAGES OF ONE OBJECT HOLD ONE ALLOCATION OF ITS KEY.
///
/// `part4::the_block_and_the_lookup_point_at_one_object_key` already asserts that a page and the
/// object lookup point at one copy. Its fixture is 64 STRING keys -- one page each -- so the
/// question it asks is only ever asked of a single page, and the axis where the answer differs is
/// the one it cannot reach: an object with MANY pages, filed one page at a time.
///
/// That is the container workload, and it is precisely the workload where #1959 measured the page
/// entry at 52.0x the node total. A hash of a hundred fields is a hundred pages of ONE object; if
/// each page's entry built its own `Arc::from(object_key)`, the store would hold a hundred
/// allocations of one short string and ninety-nine of them would carry nothing the first did not.
///
/// Counted by POINTER. Two equal strings are indistinguishable by contents and cost twice as much,
/// which is the whole reason this is not an equality assertion.
///
/// THE ANTI-VACUITY CHECK IS THE FIXTURE ITSELF: an object with one page trivially holds one
/// allocation, so the assertion is made only after the census is shown to contain objects with
/// many pages, and the widest is asserted to reach the hundred-page case.
#[test]
#[ignore = "seeds four stores; run by name"]
fn the_pages_of_one_object_hold_one_allocation_of_its_object_key() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut containers: Vec<KeyAllocationCensus> = Vec::new();
    let mut routed: Vec<KeyAllocationCensus> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, u32::MAX);
            seed_container_keys(&engine, records / 100, 100);
            let census = key_allocation_census(&engine);
            census.report(&format!("{label}: container keys, 100 elements each"));
            containers.push(census);
        }
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, u32::MAX);
            seed_routed_keys(&engine, records);
            let census = key_allocation_census(&engine);
            census.report(&format!("{label}: keys that route one to a bucket"));
            routed.push(census);
        }
    }

    assert_eq!(4, path_lengths.len(), "all four arms must have run");
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    // --- The fixture must reach the many-page object, or every claim below is free. ---
    for (index, census) in containers.iter().enumerate() {
        assert!(
            census.objects > 0 && census.pages > 0,
            "container arm {index}: nothing censused"
        );
        assert!(
            census.pages / census.objects >= 100,
            "container arm {index}: {} pages over {} objects; this arm exists to hold objects of \
             a hundred pages and it does not reach them",
            census.pages,
            census.objects
        );
    }

    // --- The claim. ---
    for (index, census) in containers.iter().chain(routed.iter()).enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.object_key),
            "arm {index}: an object's pages hold up to {} distinct allocations of one object key; \
             {} surplus copies across {} pages, {} B",
            KeyAllocationCensus::widest(&census.object_key),
            census.surplus_object_key_allocations,
            census.pages,
            census.surplus_object_key_bytes
        );
        assert_eq!(
            0,
            census.surplus_object_key_allocations,
            "arm {index}: {} allocations of an object key carry nothing the first copy did not",
            census.surplus_object_key_allocations
        );
    }

    // --- And the KIND is shared too, which is the control: a census that reported everything as
    // shared regardless would say the same thing here whether or not the pool existed.
    //
    // SINCE THE ENTRY STOPPED HOLDING A STRING FOR IT, this control is structural: the spelling
    // is a `&'static str` off `model_kind_registry`, so every page of a kind reads the same
    // address and no allocation exists to be duplicated. It is kept because it still discriminates
    // -- a census that reported two here would mean it is no longer reading the registry's
    // spelling -- but it is no longer evidence about a pool. ---
    for (index, census) in containers.iter().enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.model_id),
            "container arm {index}: the interned kind is held as {} allocations across one \
             object's pages; if this ever exceeds one the census is measuring something else",
            KeyAllocationCensus::widest(&census.model_id)
        );
    }
}

// ---------------------------------------------------------------------------------------------
// CAPTURE. Prints the bytes the goldens above are pinned against. Not a claim; an instrument.
// ---------------------------------------------------------------------------------------------

/// Print the stored spelling of a page entry in four shapes, and a whole small index.
///
/// Run at a named revision, its output IS the golden. It asserts nothing, so it cannot pass by
/// accident -- it exists to be read.
#[test]
#[ignore = "capture instrument; run by name and read its output"]
fn capture_the_stored_spelling_of_a_page_entry() {
    use crate::block_store::ElementEntry;
    use crate::engine::state::{BucketLayoutState, BucketNode, CoreIndex};

    fn page(
        key: &str,
        model: &str,
        component: Option<&str>,
        slab: u64,
        offset: u64,
        length: u64,
        flags: (bool, bool, bool),
    ) -> BlockIndex {
        BlockIndex {
            kind: crate::index_log::IndexItemKind::Page,
            routing_bucket: 7,
            object_key: Arc::from(key),
            model_id: crate::engine::storage_bucket_internals::stored_model_kind(model),
            component: component.map(Arc::from),
            address: ElementEntry::from_parts(
                slab,
                offset,
                length,
                Some(4),
                Some(42),
            ),
            dirty: flags.0,
            deleted: flags.1,
        }
    }

    println!(
        "PLAIN          = {}",
        serde_json::to_string(&page("k", "string", None, 1, 2, 3, (false, false, true))).unwrap()
    );
    println!(
        "WITH_COMPONENT = {}",
        serde_json::to_string(&page("k", "string", Some("f0"), 1, 2, 3, (false, false, true)))
            .unwrap()
    );
    println!(
        "ALL_FLAGS      = {}",
        serde_json::to_string(&page("k", "string", None, 1, 2, 3, (true, true, true))).unwrap()
    );
    println!(
        "OVER_WIDE      = {}",
        serde_json::to_string(&page("k", "string", None, 1, 2, u64::MAX, (false, false, true)))
            .unwrap()
    );

    // A whole index: one bucket, five pages of one object (one with a component) and a second
    // object in the SAME bucket -- the shape a cluster-range shard produces routinely.
    let mut node = BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::MultiObject,
        flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
        dirty_generation: 3,
        ..BucketNode::default()
    };
    node.object_index.insert(42);
    let mut live = crate::engine::state::BlockSlabLiveIndex::default();
    for offset in 0..4u64 {
        node.block_index
            .insert(page("k", "string", None, 1, offset, 3, (false, false, true)), &mut live);
    }
    node.block_index.insert(
        page("k", "hash", Some("f0"), 1, 9, 3, (true, false, false)),
        &mut live,
    );
    node.block_index.insert(
        page("other", "string", None, 2, 1, 5, (false, false, true)),
        &mut live,
    );

    let mut index = CoreIndex::default();
    index.bucket_map.insert(7, node);
    let text = serde_json::to_string(&index).unwrap();
    println!("INDEX_LEN      = {}", text.len());
    println!("INDEX          = {text}");
    println!("PAGES ----------------------------------------------------------");
    for bucket in index.bucket_map.values() {
        for entry in bucket.block_index.values() {
            println!(
                "        (\"{}\", \"{}\", {}, {}, {}, {}, {}, {}, {}),",
                entry.object_key,
                entry.model_id,
                match entry.component.as_deref() {
                    Some(name) => format!("Some(\"{name}\")"),
                    None => "None".to_string(),
                },
                entry.address.block_slab_id(),
                entry.address.offset(),
                entry.address.length(),
                entry.dirty,
                entry.deleted,
                entry.log_backed()
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE CAPTURED BYTES. Produced by `capture_the_stored_spelling_of_a_page_entry` run at
// `15583789e` and pasted here verbatim, then RE-CAPTURED when the address's slab id and offset
// merged into one word: `"ps":1,"o":2` became `"a":4294967298`, which is 1 in the high 32 bits
// and 2 in the low. The values are the ones the engine printed, not ones computed here -- the
// three guards below print what they built beside what is pinned, so a fixture edited to match a
// wrong output would have to match a wrong output that the engine actually produced.
//
// Every OTHER key in these fixtures is untouched, which is the point of keeping them: the slab id
// and the offset are the only part of the stored spelling this change moves.
// ---------------------------------------------------------------------------------------------

const PAGE_ENTRY_PLAIN: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

const PAGE_ENTRY_WITH_COMPONENT: &str = r#"{"object_key":"k","model_id":"string","component":"f0","address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

const PAGE_ENTRY_ALL_FLAGS: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":true,"deleted":true}"#;

const PAGE_ENTRY_OVER_WIDE: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":4294967295,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

/// A whole shard index written at `15583789e`: one bucket holding six pages -- five of one object
/// (four plain, one carrying a component and a different kind) and one of a SECOND object in the
/// same bucket, which is what a cluster-range shard produces routinely.
/// The routing bucket an index written before this change carries, and what this binary writes in
/// its place -- which is NOTHING, the slot having been retired from the wire struct rather than
/// held open and written nil. It was `"rs":null,` while the slot was still declared.
/// See `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed`.
const STORED_ROUTING_BUCKET: &str = r#""rs":7,"#;
/// The identity slot as an OLD row spells it, and as this binary writes it back.
///
/// The slot is not retired -- the index log packs the address POSITIONALLY, so dropping a field
/// shortens the array and refuses every existing row. It is written EMPTY instead, which is a
/// change to the stored bytes and is what `SHARD_INDEX_FORMAT_VERSION` 6 pays for.
const STORED_OBJECT_ID: &str = r#""oi":42,"#;
/// The log-resident flag AS THE OLD INDEX STORED IT, both polarities, each with its leading comma
/// so removing one leaves well-formed JSON. The fixture carries six of these across its entries.
const STORED_LOG_RESIDENT_TRUE: &str = r#","log_backed":true"#;
const STORED_LOG_RESIDENT_FALSE: &str = r#","log_backed":false"#;
const EMPTIED_OBJECT_ID: &str = r#""oi":null,"#;
const EMPTY_ROUTING_BUCKET: &str = r#""#;

const OLD_STORE_INDEX: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:4":{"object_key":"k","model_id":"hash","component":"f0","address":{"a":4294967305,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967296,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967297,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967299,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:4":{"object_key":"other","model_id":"string","address":{"a":8589934593,"l":5,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// THE SAME INDEX, CARRYING THE COMBINATION NO WRITER PRODUCES.
///
/// Byte for byte the text above as it stood before `generation` became derived: `"g":9` beside a
/// `"pi":4`, and block-ref keys ending `:4:9` to match. This engine has never written that --
/// every production constructor passed `block_id.or(object_id)` -- but an index that DID carry it
/// must not be loaded and silently re-keyed, because the generation is hashed into the page
/// handle and rendered into the key, and those handles are on disk inside the lookup refs.
const OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:9":{"object_key":"k","model_id":"hash","component":"f0","address":{"a":4294967305,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967296,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967297,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967299,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:9":{"object_key":"other","model_id":"string","address":{"a":8589934593,"l":5,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// What this binary must write back after loading `OLD_STORE_INDEX`: the same bytes, MINUS the one
/// key the node no longer holds.
///
/// Named rather than called "the text above", because there are now two index fixtures here and
/// only one of them is the one that loads.
///
/// `last_dump_sequence` is DELETED FROM THE STORED TEXT rather than a second fixture typed out
/// beside it, so every other byte of the comparison is still against what an older binary wrote.
/// The test asserts the deletion actually changed the string, or the expectation would be the
/// stored text unmodified and the round trip would be an equality with itself.
const REMOVED_KEY: &str = "\"last_dump_sequence\":11,";

/// Every page of that index, spelled out independently of the text it came from.
///
/// `(object_key, model_id, component, slab, offset, length, dirty, deleted, log_backed)`.
#[allow(clippy::type_complexity)]
const OLD_STORE_PAGES: &[(&str, &str, Option<&str>, u64, u64, u64, bool, bool, bool)] = &[
    ("k", "hash", Some("f0"), 1, 9, 3, true, false, false),
    ("k", "string", None, 1, 0, 3, false, false, true),
    ("k", "string", None, 1, 1, 3, false, false, true),
    ("k", "string", None, 1, 2, 3, false, false, true),
    ("k", "string", None, 1, 3, 3, false, false, true),
    ("other", "string", None, 2, 1, 5, false, false, true),
];

// ---------------------------------------------------------------------------------------------
// THE STORED FORMAT, pinned over several shapes.
// ---------------------------------------------------------------------------------------------

/// A page entry in a named shape, built from the declared types so the golden moves when the
/// declaration does rather than going quietly stale.
/// TWO FLAGS, NOT THREE, AND THE SIGNATURE IS WHAT ENFORCES IT.
///
/// This took a three-tuple while the entry carried three flags. When the third left, the body
/// stopped reading `flags.2` and every caller went on passing a bool that went nowhere -- no
/// warning, because a tuple field is not an unused variable. Narrowing the tuple makes the
/// compiler demand the change at each call site, which is the only way a caller finds out.
pub(super) fn page_fixture(
    component: Option<&str>,
    length: u64,
    flags: (bool, bool),
) -> BlockIndex {
    BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: Arc::from("k"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        component: component.map(Arc::from),
        address: crate::block_store::ElementEntry::from_parts(
            1,
            2,
            length,
            Some(4),
            Some(42),
        ),
        dirty: flags.0,
        deleted: flags.1,
    }
}

/// THE STORED SPELLING OF A PAGE ENTRY, character for character, over four shapes.
///
/// Pinned because this line of work is about the entry's REPRESENTATION, and the one way a
/// representation change becomes data loss is by moving a byte nobody was watching. Four shapes
/// rather than one, because the component is `skip_serializing_if = "Option::is_none"`: a golden
/// over a single shape would pin the branch that omits it and say nothing about the branch that
/// writes it.
///
/// WITH ITS OWN CONTROL. Two entries that both serialized to nothing would pass an equality test
/// against each other, so the shapes are asserted to differ from one another and each spelling is
/// asserted longer than 100 bytes. A comparison that cannot report a difference is vacuous.
/// AND IT DID MOVE, IN EXACTLY ONE SLOT. This test was called
/// `the_stored_spelling_of_a_page_entry_did_not_move` and it is renamed rather than re-goldened,
/// because a test whose NAME asserts the opposite of what it checks is worse than a stale golden:
/// the name is what a future reader greps for.
///
/// WHAT MOVED: `"oi":42` became `"oi":null`. `BlockAddressWire::object_id` carries `rename`,
/// `alias` and `default` but NO `skip_serializing_if`, so an address that no longer holds an id
/// writes the slot as a null instead of dropping it.
///
/// WHY IT IS WRITTEN RATHER THAN RETIRED: the index log packs this struct POSITIONALLY --
/// `encode_index_payload_into` uses a plain `rmp_serde::Serializer`, so a field is a position and
/// not a name -- so retiring the slot would shorten the array and refuse every row already on
/// disk. Keeping it empty is also what lets the decode go on cross-checking an old `g` against
/// `block_id.or(object_id)`, which `an_old_store_whose_generation_disagrees_is_refused_before_the_decode`
/// drives.
///
/// AND THE VALUE MOVING IS WHAT THE VERSION STAMP PAYS FOR. `SHARD_INDEX_FORMAT_VERSION` goes to 6
/// with this change; the four goldens below are the tripwire that makes someone come and check
/// that the stamp moved with them.
#[test]
fn the_stored_spelling_of_a_page_entry_moved_in_exactly_one_slot() {
    let plain = serde_json::to_string(&page_fixture(None, 3, (false, false)))
        .expect("a page entry serializes");
    let with_component = serde_json::to_string(&page_fixture(Some("f0"), 3, (false, false)))
        .expect("a page entry serializes");
    let all_flags = serde_json::to_string(&page_fixture(None, 3, (true, true)))
        .expect("a page entry serializes");
    let over_wide = serde_json::to_string(&page_fixture(None, u64::MAX, (false, false)))
        .expect("a page entry serializes");

    println!("PLAIN          = {plain}");
    println!("WITH_COMPONENT = {with_component}");
    println!("ALL_FLAGS      = {all_flags}");
    println!("OVER_WIDE      = {over_wide}");

    assert_eq!(
        PAGE_ENTRY_PLAIN, plain,
        "the stored spelling of a page entry moved AGAIN, beyond the one slot this change moved it \
         in; if that is intended, the version stamp has to move with it"
    );
    assert_eq!(
        PAGE_ENTRY_WITH_COMPONENT, with_component,
        "the stored spelling of a page entry WITH a component moved"
    );
    assert_eq!(
        PAGE_ENTRY_ALL_FLAGS, all_flags,
        "the stored spelling of a page entry with every flag set moved"
    );
    assert_eq!(
        PAGE_ENTRY_OVER_WIDE, over_wide,
        "the stored spelling of a page entry whose length saturated moved"
    );

    // --- Controls. Without these the four assertions above could all be comparing "" with "". ---
    for (name, spelling) in [
        ("plain", &plain),
        ("with component", &with_component),
        ("all flags", &all_flags),
        ("over wide", &over_wide),
    ] {
        assert!(
            spelling.len() > 100,
            "the {name} spelling is {} bytes; a golden over a near-empty document cannot fail",
            spelling.len()
        );
    }
    assert_ne!(
        plain, with_component,
        "a page carrying a component must not write the same bytes as one without it, or the \
         component has stopped being stored"
    );
    assert_ne!(
        plain, all_flags,
        "the three flags must reach the stored form, or a dirty page would load clean"
    );
    assert_ne!(
        plain, over_wide,
        "a saturated length must reach the stored form"
    );

    // --- SATURATES RATHER THAN WRAPS. An over-wide length must come back as a value no encoder
    // will accept, never as its own low bits -- `u64::MAX` truncated to `u32` is 4,294,967,295
    // either way, so the case that decides it is the one below. ---
    let wrapped_would_be = (u64::MAX as u32) as u64; // what `as u32` yields: the low bits.
    let saturated = page_fixture(None, u64::MAX, (false, false)).address.length();
    assert_eq!(
        u32::MAX as u64,
        saturated,
        "an over-wide length must saturate at u32::MAX, not wrap"
    );
    let low_bits = page_fixture(None, 0x1_0000_0003, (false, false))
        .address
        .length();
    assert_eq!(
        u32::MAX as u64,
        low_bits,
        "4 GiB + 3 must saturate to {} and not come back as the plausible small number {}",
        u32::MAX,
        3
    );
    assert_ne!(
        3, low_bits,
        "a length that wrapped would read as a shorter record and no error anywhere"
    );
    let _ = wrapped_would_be;
}

/// AN INDEX WRITTEN BEFORE THIS CHANGE, LOADED BY THIS BINARY, COMPARED ELEMENT BY ELEMENT.
///
/// The text below is the shard index a store built at `15583789e` writes for a bucket holding
/// five pages of one object -- four plain and one carrying a component -- plus a second object in
/// the same bucket. It is the bytes, not a paraphrase of them.
///
/// THE DIRECTION THAT MATTERS is the one nothing reports. A reader that cannot find a page does
/// not fail: the page is still on its slab and nothing looks for it. So the assertion is EQUALITY
/// of the whole page set, field by field, against what went in -- never that the set is non-empty
/// afterwards.
///
/// AND THE REVERSE IS NO LONGER THE SAME CLAIM, so it is no longer in this name. "An old index
/// still loads" and "it is written back byte for byte" were one test for as long as both were
/// true. The second stopped being true when the entry shed `log_backed`: this binary writes the
/// stored form one key lighter, by construction, and the format stamp is what pays for it. One
/// name over both would have had to be WEAKENED until it passed, and a weakened version would no
/// longer pin the half that is still exact -- so the write-back claim is restated, as what this
/// binary actually writes, in
/// `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed`.
///
/// THE GENERATION IN THESE BYTES MOVED, AND ONLY IT. When `generation` became derived from
/// `block_id.or(object_id)`, the fixture behind this capture stopped being able to express the
/// independent `9` it had chosen beside a `block_id` of `4` -- a combination no production
/// constructor in this engine has ever emitted. The bytes were regenerated with the capture
/// instrument in this module rather than hand-edited, and they came back the same 1,349 bytes
/// with `"g":4` and keys ending `:4:4`. An index that really did carry the old combination is
/// REFUSED rather than re-keyed; `an_old_store_whose_generation_disagrees_is_refused_before_the_decode`
/// drives it -- named, rather than called "the test directly below", which it stopped being the
/// moment this test split in two.
#[test]
fn an_index_written_before_this_change_loads_page_for_page() {
    let index: crate::engine::state::CoreIndex =
        serde_json::from_str(OLD_STORE_INDEX).expect("an index written at 15583789e must load");

    let bucket = index
        .bucket_map
        .get(&7)
        .expect("the stored bucket must load");

    // --- Element by element, in the order the stored map spells them. ---
    let mut loaded: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool)> = index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .map(|page| {
            (
                page.object_key.to_string(),
                page.model_id.to_string(),
                page.component.as_deref().map(str::to_string),
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
                page.dirty,
                page.deleted,
            )
        })
        .collect();
    loaded.sort();

    let mut expected: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool)> =
        OLD_STORE_PAGES
            .iter()
            // THE NINTH COLUMN IS READ NO LONGER, AND THE FIXTURE KEEPS IT ON PURPOSE. It records
            // what the old index STORED, which is the fixture's whole job -- and in some of these
            // rows what it stored was WRONG: a log-resident flag set on a slab-backed page. That
            // disagreement was a documented defect. The property is derived from the address now,
            // so there is no stored copy left to round-trip and nothing to compare it against.
            .map(|(key, model, component, slab, offset, length, dirty, deleted, _log_backed)| {
                (
                    (*key).to_string(),
                    (*model).to_string(),
                    component.map(str::to_string),
                    *slab,
                    *offset,
                    *length,
                    *dirty,
                    *deleted,
                )
            })
            .collect();
    expected.sort();

    assert_eq!(
        expected.len(),
        loaded.len(),
        "the page COUNT moved: {} written, {} loaded",
        expected.len(),
        loaded.len()
    );
    for (index_of, (want, got)) in expected.iter().zip(loaded.iter()).enumerate() {
        assert_eq!(want, got, "page {index_of} came back different");
    }

    // --- Anti-vacuity: the fixture has to hold the shapes the claim is about. ---
    assert!(
        loaded.len() >= 5,
        "the fixture holds {} pages; it is supposed to hold a multi-page object",
        loaded.len()
    );
    assert!(
        loaded.iter().any(|page| page.2.is_some()),
        "no loaded page carries a component, so the component's stored round trip is untested"
    );
    assert!(
        loaded.iter().filter(|page| page.0 == "k").count() > 1,
        "no object in the fixture holds more than one page, so the axis this change is about is \
         not covered by this golden"
    );
    assert!(
        bucket.block_index.len() > 1,
        "the stored bucket holds {} page(s); a single-page bucket would exercise only the inline \
         arm and say nothing about the map arm",
        bucket.block_index.len()
    );

    // --- The control: a DIFFERENT page set must not compare equal, or the comparison above is
    // reporting sameness it cannot actually detect. ---
    let mut injected = loaded.clone();
    injected[0].5 = injected[0].5.wrapping_add(1);
    assert_ne!(
        injected, loaded,
        "the element-by-element comparison cannot report a difference when one is injected"
    );
}

/// WHAT THIS BINARY WRITES BACK WHEN IT IS HANDED AN INDEX WRITTEN BEFORE THIS CHANGE.
///
/// THE OTHER HALF OF THE TEST ABOVE, SPLIT OFF RATHER THAN RELAXED -- see its note for why one
/// name could not keep both claims once the second stopped being true.
///
/// THE FIXTURE IS NOT RESTATED, and the direction this test runs in is the reason.
/// `OLD_STORE_INDEX` records YESTERDAY'S INPUT: the bytes a store at `15583789e` really wrote,
/// whose whole job is to prove that a pre-change index still decodes. Re-goldening it would hand
/// this test today's own output, and it would then assert nothing about the past. So the
/// expectation is DERIVED from those bytes instead, by naming each difference this binary makes to
/// a stored row and applying it -- with every substitution asserted to have FIRED, so none of them
/// can quietly match nothing and leave the comparison reporting a sameness it never tested.
///
/// FOUR DIFFERENCES ARE NAMED, and none of them is a number that moved: a key that is no longer
/// written, a slot retired from the wire struct, a slot written empty, and the flag the entry shed.
#[test]
fn an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed() {
    let index: crate::engine::state::CoreIndex =
        serde_json::from_str(OLD_STORE_INDEX).expect("an index written at 15583789e must load");

    // --- And back: the same bytes but for the keys the node no longer holds. ---
    let without_the_removed_key = OLD_STORE_INDEX.replace(REMOVED_KEY, "");
    assert_ne!(
        OLD_STORE_INDEX, without_the_removed_key.as_str(),
        "the stored fixture does not contain {REMOVED_KEY}, so the expectation below is the stored \
         text unmodified and the round trip proves nothing about the removal"
    );
    // AND THE ROUTING BUCKET IS NOT WRITTEN BACK AT ALL, which is the second content change this
    // round trip has to account for. The address does not hold a routing bucket, and the SLOT is
    // now retired from the wire struct rather than held open and written nil -- see
    // `block_store::shorter_struct_against_an_existing_row` for why a dead slot can leave. So an
    // index written before this loads with its `rs` read and ignored, and is written back with the
    // key ABSENT.
    //
    // That this fixture still LOADS, carrying six `"rs":7` keys no field claims, is the
    // compatibility half of this test and it is the half that matters: a named decoder tolerates a
    // key nothing declares.
    let stored_bucket_keys = without_the_removed_key.matches(STORED_ROUTING_BUCKET).count();
    assert_eq!(
        6, stored_bucket_keys,
        "the fixture must carry a stored routing bucket on each of its six page entries, or the \
         canonicalisation below is not reporting what happens to one"
    );
    let canonical = without_the_removed_key.replace(STORED_ROUTING_BUCKET, EMPTY_ROUTING_BUCKET);
    // AND THE RETIRED IDENTITY SLOT IS WRITTEN BACK EMPTY, which is the second content change this
    // binary makes to a stored row. The slot STAYS -- the log packs positionally -- so what the
    // writer emits is `"oi":null` where the old row carried the id. Canonicalised here for the same
    // reason the routing bucket is: the expectation has to be what THIS binary writes, and the
    // replacement is asserted to have fired so it cannot quietly match nothing.
    let stored_object_id_count = canonical.matches(STORED_OBJECT_ID).count();
    assert_eq!(
        6, stored_object_id_count,
        "the fixture must carry a stored object id on each of its six page entries, or the \
         canonicalisation below is not reporting what happens to one"
    );
    let canonical = canonical.replace(STORED_OBJECT_ID, EMPTIED_OBJECT_ID);
    assert_eq!(
        0,
        canonical.matches(STORED_OBJECT_ID).count(),
        "every stored object id must be emptied in the expectation, not left as it was"
    );
    assert_eq!(
        6,
        canonical.matches(EMPTIED_OBJECT_ID).count(),
        "and each one must be emptied to the null the writer actually emits"
    );
    assert_ne!(
        without_the_removed_key, canonical,
        "the stored fixture does not contain {STORED_ROUTING_BUCKET}, so the expectation below \
         cannot be reporting what happens to a stored routing bucket"
    );
    // COUNTED ON WHAT REMAINS, not on the replacement. `EMPTY_ROUTING_BUCKET` is the empty string
    // now that the slot is retired, and `str::matches("")` answers once per character boundary --
    // so counting occurrences OF it would report the length of the document and pass for any text
    // at all. What has to be true is that no routing-bucket key survives.
    assert_eq!(
        0,
        canonical.matches("\"rs\"").count(),
        "every stored routing bucket must be gone from the expectation, not rewritten to nil: \
         the slot is retired from the wire struct"
    );
    // AND THE LOG-RESIDENT KEY IS DROPPED FROM THE EXPECTATION, COUNTED BOTH WAYS.
    //
    // THE CLAIM THIS TEST MAKES HAS SPLIT IN TWO, and only the first half is unchanged. An index
    // written before this change still LOADS page for page -- asserted above, and that is the
    // tolerant decoder doing its job, since the wire struct declines no unknown key. What it no
    // longer does is write back the SAME BYTES: it writes back one key lighter, because the entry
    // shed the flag nothing maintained. That is precisely what the format stamp is spent on.
    //
    // Canonicalised rather than asserted around, in the same counted shape as the object id above:
    // the count is checked BEFORE so the replacement cannot be reporting on nothing, and checked
    // to zero after so it cannot have half-fired.
    let log_resident_keys = canonical.matches(STORED_LOG_RESIDENT_TRUE).count()
        + canonical.matches(STORED_LOG_RESIDENT_FALSE).count();
    assert_eq!(
        6, log_resident_keys,
        "the fixture must carry a stored log-resident flag on each of its six page entries, or \
         the canonicalisation below is not reporting what happens to one"
    );
    let canonical = canonical
        .replace(STORED_LOG_RESIDENT_TRUE, "")
        .replace(STORED_LOG_RESIDENT_FALSE, "");
    assert_eq!(
        0,
        canonical.matches("\"log_backed\"").count(),
        "every stored log-resident flag must be gone from the expectation: the key is retired \
         from the entry and this binary does not write it"
    );

    let rewritten = serde_json::to_string(&index).expect("the index re-serializes");
    assert_eq!(
        canonical, rewritten,
        "the index this binary writes back differs from the index it was given in some way other \
         than dropping last_dump_sequence, retiring the address's routing bucket, emptying its \
         identity slot, and dropping the log-resident flag the entry shed"
    );
    assert!(
        rewritten.len() > 500,
        "the re-serialized index is {} bytes; a comparison of two near-empty documents cannot \
         report a difference",
        rewritten.len()
    );
}

/// A MIS-VERSIONED STORE IS DRIVEN, AND IT FAILS LOUDLY -- BEFORE THE DECODE COMPLETES.
///
/// The index here is the one above as it stood before `generation` became derived: `"g":9` beside
/// a `"pi":4`, with block-ref keys ending `:4:9` to match. No production constructor in this
/// engine emits that combination, but if one ever reached disk, loading it under the derivation
/// would compute `4` where the writer stored `9` -- and the generation is hashed into the page
/// handle and rendered into the key, so every page in the bucket would be filed under a name the
/// refs already on disk do not use. That is a lost object, reported by nothing.
///
/// So the load must FAIL, and fail where it can still be understood: at the address, during
/// deserialization, naming the two values. The assertions below are on the MESSAGE as well as the
/// failure, because a load that fails for some unrelated reason would pass a bare `is_err`.
///
/// WITH ITS CONTROL, which is the test directly above: the same index with an agreeing generation
/// loads page for page and rewrites as the same 1,349 bytes. Without that, this test would pass
/// just as well if the loader had stopped accepting any index at all.
#[test]
fn an_old_store_whose_generation_disagrees_is_refused_before_the_decode() {
    let outcome: Result<crate::engine::state::CoreIndex, _> =
        serde_json::from_str(OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION);

    let error = outcome.err().expect(
        "an index carrying a generation this binary cannot reproduce must NOT load: deriving a \
         different one silently re-keys every page in the bucket",
    );
    let text = error.to_string();
    println!("REFUSED WITH: {text}");
    assert!(
        text.contains("disagrees with block_id.or(object_id)"),
        "the refusal must name what disagreed, got: {text}"
    );
    assert!(
        text.contains("recompute every page handle"),
        "and say why that matters, got: {text}"
    );

    // The two values themselves, so the message is actionable rather than merely alarming.
    assert!(text.contains('9'), "the stored generation must appear in the message: {text}");
    assert!(text.contains('4'), "and the derived one: {text}");

    // NON-VACUITY: the text this test drives must actually be the disagreeing shape, or the
    // refusal above could be about anything at all.
    assert!(
        OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION.contains(r#""g":9"#),
        "the fixture has stopped carrying the independent generation it exists to drive"
    );
    assert!(
        OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION.contains(r#":4:9""#),
        "the fixture has stopped carrying the keys that match it"
    );
    assert_ne!(
        OLD_STORE_INDEX, OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION,
        "the two fixtures are the same text, so one of them is not what it claims to be"
    );
}

// ---------------------------------------------------------------------------------------------
// THE READ PATH, and the footprint on the counting allocator.
// ---------------------------------------------------------------------------------------------

/// Give every page its OWN allocation of its object key, leaving the contents identical.
///
/// This is the shape the write path produced before this change, rebuilt inside one binary so the
/// two can be compared without a second build. It changes nothing a reader can observe except
/// which allocation it lands on, which is exactly the variable under test.
fn unshare_object_keys(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        for page in bucket.block_index.blocks_mut_unaccounted() {
            page.object_key = Arc::from(&*page.object_key);
        }
    }
}

/// Distinct object-key allocations behind the widest object in the store.
fn widest_object_key_allocations(engine: &TemporalEngine) -> usize {
    key_allocation_census(engine)
        .object_key
        .keys()
        .copied()
        .next_back()
        .unwrap_or_default()
}

/// Walk every page and fold its three names into a checksum.
///
/// Reads the NAMES, which is the only thing this change touches, and folds them so the compiler
/// cannot drop the loads.
fn fold_every_name(engine: &TemporalEngine) -> u64 {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut sum: u64 = 0;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            for byte in page.object_key.as_bytes() {
                sum = sum.wrapping_mul(31).wrapping_add(*byte as u64);
            }
            for byte in page.model_id.as_str().as_bytes() {
                sum = sum.wrapping_mul(31).wrapping_add(*byte as u64);
            }
            if let Some(component) = page.component.as_deref() {
                for byte in component.as_bytes() {
                    sum = sum.wrapping_mul(31).wrapping_add(*byte as u64);
                }
            }
        }
    }
    sum
}

/// READING A NAME IS NOT SLOWER WHEN THE PAGES OF AN OBJECT SHARE ONE ALLOCATION.
///
/// The shape this line of work first considered -- names hoisted out of the entry, the entry
/// holding a handle -- would add a dependent load to every read of a name, and that cost is not
/// visible in any footprint measurement. This change does not do that: the entry still holds its
/// own `Arc<str>` and reads it exactly as before. What moves is which allocation the pointer
/// lands on, and a hundred pages landing on ONE allocation is a hundred pages sharing a cache
/// line rather than touching a hundred.
///
/// Measured ABBA, because an A-then-B ordering charges the second arm with whatever the first
/// warmed -- and interleaving does not cancel an order effect, ABBA does. Both arms are asserted
/// to be IN THE SHAPE THEY ARE NAMED FOR before anything is timed, and their checksums are
/// asserted equal and non-zero: two arms that both read nothing would time identically.
#[test]
#[ignore = "timing; run by name"]
fn reading_a_name_is_not_slower_when_an_objects_pages_share_one_allocation() {
    let shared_dir = tempfile::tempdir().expect("tempdir");
    let unshared_dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(
        shared_dir.path().as_os_str().len(),
        unshared_dir.path().as_os_str().len(),
        "the store path length moved between arms"
    );

    let shared = probe_engine(shared_dir.path());
    load_shard_over(&shared, u32::MAX);
    seed_container_keys(&shared, 40, 100);

    let unshared = probe_engine(unshared_dir.path());
    load_shard_over(&unshared, u32::MAX);
    seed_container_keys(&unshared, 40, 100);
    unshare_object_keys(&unshared);

    // --- PROVE THE TREATMENT RAN. An arm that is not in the shape it is named for is a broken
    // arm, and a broken arm looks exactly like a winning one. ---
    let shared_widest = widest_object_key_allocations(&shared);
    let unshared_widest = widest_object_key_allocations(&unshared);
    println!("shared arm: widest object holds {shared_widest} object-key allocation(s)");
    println!("unshared arm: widest object holds {unshared_widest} object-key allocation(s)");
    assert_eq!(
        1, shared_widest,
        "the shared arm is not shared: its widest object holds {shared_widest} allocations"
    );
    assert!(
        unshared_widest >= 100,
        "the unshared arm is not unshared: its widest object holds {unshared_widest} allocations, \
         so this comparison has two identical arms and would report 1.00x whatever the truth is"
    );

    let want = fold_every_name(&shared);
    assert_ne!(0, want, "the walk folded nothing; both arms would time an empty loop");
    assert_eq!(
        want,
        fold_every_name(&unshared),
        "the two arms do not hold the same names, so any difference below is a difference of \
         contents and not of representation"
    );

    const ROUNDS: usize = 40;
    let mut shared_ns: u128 = 0;
    let mut unshared_ns: u128 = 0;
    let mut guard: u64 = 0;
    for _ in 0..ROUNDS {
        // A ... B ... B ... A, so whatever the first arm warms is paid by both equally.
        let t = std::time::Instant::now();
        guard = guard.wrapping_add(fold_every_name(&shared));
        shared_ns += t.elapsed().as_nanos();

        let t = std::time::Instant::now();
        guard = guard.wrapping_add(fold_every_name(&unshared));
        unshared_ns += t.elapsed().as_nanos();

        let t = std::time::Instant::now();
        guard = guard.wrapping_add(fold_every_name(&unshared));
        unshared_ns += t.elapsed().as_nanos();

        let t = std::time::Instant::now();
        guard = guard.wrapping_add(fold_every_name(&shared));
        shared_ns += t.elapsed().as_nanos();
    }
    assert_ne!(0, guard, "the timed work was folded away");

    let pages = key_allocation_census(&shared).pages;
    assert!(pages > 0, "denominator: no pages walked");
    let shared_per = shared_ns as f64 / (2 * ROUNDS * pages) as f64;
    let unshared_per = unshared_ns as f64 / (2 * ROUNDS * pages) as f64;
    println!(
        "\n  names read, {pages} pages x {} walks an arm:\n    pages of an object sharing one \
         key allocation : {shared_per:.2} ns a page\n    each page holding its own              \
              : {unshared_per:.2} ns a page  ({:.3}x)",
        2 * ROUNDS,
        unshared_per / shared_per
    );

    // The claim is NOT that sharing is faster -- that would be a claim about this box's caches.
    // It is that it is not SLOWER, which is what a representation change has to earn.
    assert!(
        shared_per <= unshared_per * 1.25,
        "reading a name costs {shared_per:.2} ns a page when the pages share one allocation \
         against {unshared_per:.2} ns when each holds its own; a representation that makes the \
         common path slower to make the store smaller is declined"
    );
}

/// BYTES AND ALLOCATIONS PER PAGE, on the counting allocator, at two corpus sizes.
///
/// ONE INSTRUMENT for both figures. #1959 found a published decline whose sign was wrong because
/// it set a `size_of` saving against an allocator cost; nothing here mixes the two.
///
/// The allocator's own rounding can swallow a byte win while the COUNT stays exact, so both are
/// reported and the count is what is claimed.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe"]
fn what_a_container_store_charges_the_allocator_a_page() {
    for (label, keys) in [("4,000 records", 40usize), ("40,000 records", 400usize)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_length = dir.path().as_os_str().len();
        let engine = probe_engine(dir.path());
        load_shard_over(&engine, u32::MAX);

        let probe = Probe::start();
        seed_container_keys(&engine, keys, 100);
        let counts = probe.stop();

        let census = key_allocation_census(&engine);
        assert!(census.pages > 0, "denominator: nothing seeded");
        assert!(
            census.pages / census.objects >= 100,
            "{label}: {} pages over {} objects; the container case is not reached",
            census.pages,
            census.objects
        );
        println!(
            "\n=== {label} (store path {path_length} chars) ===\n  pages={} objects={}\n  \
             alloc calls={} ({:.4} a page)\n  alloc bytes={} ({:.2} B a page)\n  widest object-key \
             allocation count={}",
            census.pages,
            census.objects,
            counts.allocs,
            counts.allocs as f64 / census.pages as f64,
            counts.alloc_bytes,
            counts.alloc_bytes as f64 / census.pages as f64,
            KeyAllocationCensus::widest(&census.object_key)
        );
        assert!(
            counts.allocs > 0,
            "the probe reported zero allocations for a seed of {} pages, which reads as a path \
             that does not allocate",
            census.pages
        );
    }
}

/// A COMPONENT CANNOT CROWD A KIND OUT OF THE POOL THEY SHARE.
///
/// `part4::blocks_of_one_kind_spend_one_byte_and_share_one_static_spelling` asserts the PAGE's
/// half of this: a page entry holds one byte for its kind and reads the registry's `&'static str`
/// through it, so a page cannot hold a pooled copy at all. Its fixture is 200 strings and 40
/// hashes whose field is always `"f"` -- ONE component name in the whole store -- so the pool it
/// would have checked holds three entries against a cap of sixty-four and can never be full. The
/// failure this test is about needs the pool FULL, and the only thing in this engine that fills it
/// is a container.
///
/// WHO STILL NEEDS THE POOL, now that the entry does not: the object lookup, whose `by_model` head
/// is keyed by the kind's SHARED name. That is the holder this test protects, and it is why a
/// component crowding a kind out still costs something -- one allocation per (kind, object) head
/// instead of one per kind.
///
/// Both counts are read, and the kind count is the claim. The component pool is asserted to be AT
/// ITS CAP in the same breath, so this cannot pass by the containers having quietly stopped
/// producing components.
#[test]
#[ignore = "seeds a container store; run by name"]
fn a_container_cannot_fill_the_pool_the_kinds_are_interned_in() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, u32::MAX);
    // Four container keys, one of each kind, a hundred elements each: 400 distinct component
    // names against a pool cap of 64.
    seed_container_keys(&engine, 4, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let pooled = shard.bucket_index.kind_pool.len();
    println!("kind_pool={pooled} entries");

    // The pool must be PAST the component ceiling, or the crowding-out this test is about cannot
    // happen and every assertion below is free.
    assert!(
        pooled > 64,
        "the pool holds {pooled} entries; this fixture is supposed to produce four hundred \
         distinct component names and so drive it past the component ceiling of 64. Below that, \
         a component could not have crowded a kind out even before this change"
    );
    // And still bounded: components stop at 64, kinds may use 16 more, nothing else may.
    assert!(
        pooled <= 64 + 16,
        "the pool holds {pooled} entries against a ceiling of 80; it is supposed to stay bounded \
         by the sum of the two ceilings"
    );

    // AND WHO STILL HOLDS THE POOLED NAME, which is the half of this test the entry no longer
    // decides. A page entry carries the one-byte spelling and no string, so it cannot hold a
    // pooled copy and cannot fail to. The remaining holder is the OBJECT LOOKUP, whose `by_model`
    // head is keyed by the shared name -- that is why the pool is still consulted at all, and it
    // is what the ceilings above are protecting.
    let mut lookup_heads = 0usize;
    let mut pooled_heads = 0usize;
    for (model_id, _object_key, _refs) in shard.bucket_index.object_block_lookup.iter() {
        lookup_heads += 1;
        if let Some(pooled) = shard.bucket_index.kind_pool.get(model_id) {
            if Arc::ptr_eq(pooled, model_id) {
                pooled_heads += 1;
            }
        }
    }
    assert!(
        lookup_heads > 0,
        "the lookup holds no (model, object) head, so there is nothing whose key could be pooled"
    );
    assert_eq!(
        lookup_heads, pooled_heads,
        "{pooled_heads} of {lookup_heads} lookup heads are keyed by the POOLED allocation of \
         their kind; the rest hold a copy, which is the cost the pool exists to remove"
    );

    // And the page's own side of it: one byte, no string, nothing to pool.
    let mut pages = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        pages += bucket.block_index.iter().count();
    }
    assert!(pages > 0, "no pages were recorded; nothing was measured");
    assert_eq!(
        1,
        std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>(),
        "the page entry is supposed to spend one byte on the spelling the pool holds for the lookup",
    );
}

// ---------------------------------------------------------------------------------------------
// LOUD OR SILENT: what a store stamped with the wrong struct version actually does.
// ---------------------------------------------------------------------------------------------

/// A MIS-VERSIONED STORE IS REFUSED BY NAME, AND THE REFUSAL COMES BEFORE THE DECODE.
///
/// This change moves no stored byte, so nothing here is a migration. It is the answer to the
/// question a format change would have had to ask FIRST, driven rather than asserted, and written
/// down so the next change to this shape has something to fail on.
///
/// There are two version stamps and they are not equivalent.
///
///   * `ShardState::index_format_version` lives INSIDE the payload. `decode_index_bytes` does not
///     consult it -- it cannot, it is not decoded yet -- so a stale JSON index decodes CLEANLY at
///     this layer. What refuses it is `load_index_inner` one layer up, which compares it against
///     `SHARD_INDEX_FORMAT_VERSION` and treats a stale index exactly like an absent one: the
///     caller replays the log. Slower, and correct.
///   * The binary container stamps the struct version big-endian right after the codec id, OUTSIDE
///     the payload. That one is checked before a byte is decompressed, because a name-free
///     positional encoding read against a different struct does not fail -- it MIS-READS, and
///     produces a plausible and wrong shard.
///
/// THE DIRECTION THAT MATTERS is the second one, and this drives it: a container whose version
/// stamp is wrong reports the VERSION, not a decompression failure, even when its body is garbage
/// that could not possibly decompress. A guard that ran after the decode would report the garbage.
#[test]
fn a_store_stamped_with_the_wrong_struct_version_is_refused_before_it_is_decoded() {
    use crate::engine::{
        decode_index_bytes, serialize_index_stamped, SHARD_INDEX_FORMAT_VERSION,
    };

    // --- A real index, through the production codec. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, u32::MAX);
    seed_container_keys(&engine, 2, 8);
    seed_routed_keys(&engine, 8);

    let mut shard = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards.get(&1).expect("shard is loaded").clone()
    };
    let bytes = serialize_index_stamped(&mut shard);
    assert!(
        bytes.len() > 200,
        "the serialized index is {} bytes; a version check over a near-empty document proves \
         nothing",
        bytes.len()
    );

    // --- CONTROL: it round-trips at the right version, page for page. ---
    let decoded = decode_index_bytes(&bytes).expect("an index this binary wrote must decode");
    let want = page_tuples(&shard);
    let got = page_tuples(&decoded);
    assert!(
        want.len() >= 16,
        "the fixture holds {} pages; too few to tell a correct decode from an empty one",
        want.len()
    );
    assert_eq!(want, got, "the index did not come back page for page");

    // --- And the control's own control: a DIFFERENT page set must not compare equal. ---
    let mut injected = got.clone();
    injected[0].3 = injected[0].3.wrapping_add(1);
    assert_ne!(
        injected, want,
        "the page comparison cannot report a difference when one is injected"
    );

    // --- Is the payload the binary container, which is the form the stamp guards? ---
    let magic_len = bytes.len() - {
        // The stamp sits after the magic and the codec id. Rather than re-spell either here, find
        // them by rewriting the four version bytes and watching the decode change its answer.
        0
    };
    let _ = magic_len;

    // The version stamp is the four bytes immediately before the compressed body, and the body is
    // what follows. Locate it by brute force over the whole prefix: exactly one position holds the
    // current version big-endian AND yields a version error when changed.
    let current = SHARD_INDEX_FORMAT_VERSION.to_be_bytes();
    let mut stamp_at: Option<usize> = None;
    for offset in 0..bytes.len().min(64).saturating_sub(4) {
        if bytes[offset..offset + 4] != current {
            continue;
        }
        let mut wrong = bytes.clone();
        wrong[offset..offset + 4].copy_from_slice(&(SHARD_INDEX_FORMAT_VERSION + 1).to_be_bytes());
        if let Err(message) = decode_index_bytes(&wrong) {
            if message.contains("struct version") {
                stamp_at = Some(offset);
                break;
            }
        }
    }
    let Some(stamp_at) = stamp_at else {
        panic!(
            "no version stamp found in the first 64 bytes of a {} byte index; this binary did not \
             write the binary container, so the stamp this test is about is not present and the \
             assertions below would be vacuous",
            bytes.len()
        );
    };
    println!("version stamp at byte {stamp_at}, value {SHARD_INDEX_FORMAT_VERSION}");

    // --- NEWER than this binary: refused, LOUDLY, naming both versions. ---
    for wrong_version in [SHARD_INDEX_FORMAT_VERSION + 1, SHARD_INDEX_FORMAT_VERSION + 7, 0] {
        let mut wrong = bytes.clone();
        wrong[stamp_at..stamp_at + 4].copy_from_slice(&wrong_version.to_be_bytes());
        let error = decode_index_bytes(&wrong)
            .err()
            .unwrap_or_else(|| panic!("version {wrong_version} decoded instead of being refused"));
        println!("  version {wrong_version} -> {error}");
        assert!(
            error.contains(&wrong_version.to_string())
                && error.contains(&SHARD_INDEX_FORMAT_VERSION.to_string()),
            "the refusal for version {wrong_version} does not name both versions: {error}"
        );
    }

    // --- AND THE REFUSAL IS FIRST. Garbage after the stamp must still report the VERSION. ---
    let mut wrong_and_garbled = bytes.clone();
    wrong_and_garbled[stamp_at..stamp_at + 4]
        .copy_from_slice(&(SHARD_INDEX_FORMAT_VERSION + 1).to_be_bytes());
    for byte in wrong_and_garbled[stamp_at + 4..].iter_mut() {
        *byte = 0xA5;
    }
    let error = decode_index_bytes(&wrong_and_garbled)
        .err()
        .expect("a garbled mis-versioned container must be refused");
    println!("  mis-versioned AND garbled -> {error}");
    assert!(
        error.contains("struct version"),
        "a mis-versioned container whose body is garbage reported {error:?} rather than the \
         version; the check runs AFTER the decode, which is the shape that mis-reads"
    );

    // --- The other stamp, stated rather than assumed: the in-payload field is NOT what refuses
    // at this layer. `load_index_inner` is. Saying so here keeps the two from being confused. ---
    assert_eq!(
        12, SHARD_INDEX_FORMAT_VERSION,
         "the struct version moved; the refusal messages pinned above quote it. Moved 2 -> 3 when the object id stopped folding the component in: the stored `oi` keeps its type, so an old index decodes cleanly and serves a recomputed id that disagrees with its own `object_index`. Moved 3 -> 5 when a container page gained the ability to state that one of its items was REMOVED: the payload is opaque to every index encoder, so an old index again decodes cleanly, and what disagrees is what a tombstone item MEANS -- the previous binary reads one as an empty live value and puts the element back. Moved 6 -> 7 when the object list began storing its SLOTS rather than a sorted set of its ids: the sequence is written in slot order and spells a placeholder `null`, so a bucket holding more than one object writes different bytes, and a previous binary reading them would take a slot position for an ascending rank -- which is the same class of silent misread as the three below, arriving on the load path. Moved 5 -> 6 when `object_id` left `ElementEntry`: `generation` is the block id alone now, so a WAL-resident page's generation went from Some(object_id) to None, every ref key it resolves through moves, and an OMITTED ref key restores to a different handle entirely. 4 was skipped and its reservation is now VOID -- it was held while this constant was 3, main moved to 5, and spending 4 would LOWER the constant, which `persistence.rs`'s one-sided `<` turns into a silent accept. A stamp may only ever increase. Moved 8 -> 9 when a zset's component stopped spelling its score: `block_index_written_key` renders `component` into the map key the served index is serialized under, so `\"0000000000000000<member>\"` became `\"<member>\"` under the same named map. The decode stays clean on both sides of this one too -- `component` keeps its `String` type -- and what disagrees is what the text MEANS on the recovery path that reads it back as a page's element, the same shape as 2->3, 3->5 and 6->7. All four bumps share one shape: the stored row decodes cleanly and the disagreement appears later, on a recovery path. Moved 9 -> 11 when a TOMBSTONE'S ELEMENT NAME LEFT THE ENTRY: a tombstone entry files no component and the element it records is a new key on the BUCKET NODE, so the entry's rendered key loses the element text AND the node grows a key. The named served index DROPS an undeclared key silently, so a binary before that change reading an index written after it loads every tombstone with no element at all -- a removal nothing can identify, which is the resurrection the change exists to prevent. Same shape as every bump above: the row decodes cleanly and the disagreement appears on a recovery path. TEN IS NOT A HOLE AND IS NOT SPENDABLE: it is claimed by an UNCOMMITTED edit in another working tree, which no ref holds and no sweep of refs can see -- a lane swept 2,567 refs, found nothing above 9, and was wrong for exactly that reason. Moved 11 -> 12 when HASH AND ZSET joined the page-named set: the same key `block_index_written_key` renders, moved for two more kinds, which is 8 -> 9 again with a wider scope. The pinned assertions above resolve the constant symbolically, so they followed it -- this literal is the tripwire that made someone come and check that they did"
    );
    // --- AND THE REFUSAL IS COUNTED APART FROM AN ABSENCE, which is the whole reason the counters
    //     exist: `load_index_inner` answers `Ok(None)` for stale, undecodable and absent alike, so
    //     without them a stamp bump is invisible from outside.
    //
    //     THE DESTRUCTURING ORDER BELOW IS (accepted, stale, absent, decode) AND IS NOT ARBITRARY.
    //     `persistence::index_load_path_counts` returns (accepted, refused_stale, absent,
    //     undecodable). An earlier version of this block read a counter of my own that returned the
    //     SAME four u64s in a different order, and swapping one accessor for the other compiles
    //     either way -- so getting it wrong would leave `stale_n` holding ACCEPTED, which is
    //     non-zero here for unrelated reasons, and every assertion would pass while measuring
    //     nothing. Bind by position against that signature, not by the order that reads nicely. ---
    {
        use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = crate::engine::TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );

        // ABSENT: no base index has ever been written for this shard.
        reset_index_load_path_counts();
        engine.load_shard(7);
        let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
        println!(
            "  absent index -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}"
        );
        assert!(
            absent_n > 0,
            "a shard with no base index must count as ABSENT; got stale {stale_n} decode              {decode_n} absent {absent_n} accepted {accepted_n}"
        );
        assert_eq!(
            0, stale_n,
            "an absent index must NOT be counted as a stale-stamp refusal, or the two cannot be              told apart and the counters say nothing"
        );

        // STALE: a base index written under an older stamp.
        engine.execute(crate::types::ExecuteRequest {
            shard_id: 7,
            command: crate::types::Command::StringSet {
                key: "stamped".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.flush_shard_index(7);
        // THROUGH THE FUNNEL, not as JSON. A base index is zstd inside a container with magic
        // bytes, so `serde_json::from_slice` on the file does not parse it -- which is how the first
        // version of this test failed.
        let path = dir.path().join("indexes").join("shard-7.index.json");
        let bytes = std::fs::read(&path).expect("a flushed base index exists");
        let mut restored = decode_index_bytes(&bytes).expect("the base index decodes");
        assert_eq!(
            restored.index_format_version, SHARD_INDEX_FORMAT_VERSION,
            "denominator: the index just written must carry the CURRENT stamp, or staling it below              changes nothing"
        );
        restored.index_format_version = SHARD_INDEX_FORMAT_VERSION - 1;
        std::fs::write(&path, crate::engine::encode_index_bytes(&restored)).expect("write");

        reset_index_load_path_counts();
        let reloaded = crate::engine::TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        reloaded.load_shard(7);
        let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
        println!(
            "  stale stamp  -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}"
        );
        assert!(
            stale_n > 0,
            "an index stamped {} against a current {SHARD_INDEX_FORMAT_VERSION} must be counted as              a STALE-STAMP refusal; got stale {stale_n} decode {decode_n} absent {absent_n}              accepted {accepted_n}",
            SHARD_INDEX_FORMAT_VERSION - 1
        );
        assert_eq!(
            0, absent_n,
            "a stale index is not an absent one; if both counters move, a bump is still invisible"
        );
    }

    let mut stale = shard.clone();
    stale.index_format_version = 0;
    let stale_json = serde_json::to_vec(&stale).expect("a shard serializes as JSON");
    assert!(
        decode_index_bytes(&stale_json).is_ok(),
        "a JSON index carrying a stale in-payload version decodes at this layer -- which is the \
         point: the field cannot guard the decode it lives inside, and `load_index_inner` is what \
         refuses it. If this ever starts failing, the two stamps have been merged and the comment \
         above is wrong."
    );
}

/// Every page of a shard as comparable tuples, in a stable order.
fn page_tuples(
    shard: &crate::engine::state::ShardState,
) -> Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> {
    let mut pages: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .map(|page| {
            (
                page.object_key.to_string(),
                page.model_id.to_string(),
                page.component.as_deref().map(str::to_string),
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
                page.dirty,
                page.deleted,
                page.log_backed(),
            )
        })
        .collect();
    pages.sort();
    pages
}


/// AN OMITTED HANDLE RESTORES WRONG, AND THIS IS WHY THE FORMAT STAMP IS NOT A FORMALITY.
///
/// `index_log::strip_block_ref_key_repeat` OMITS a page's composite handle from the delta whenever
/// the handle is derivable from the item's own parts, and `restore_block_ref_key_repeat` rebuilds it
/// on the way back in. Both derive it through `block_ref_key_from_parts`, and one of the eight parts
/// is `address.generation().unwrap_or_default()`.
///
/// `generation` was `block_id.or(object_id)` and is the block id alone. For a BLOCK-RESIDENT page
/// that is the value it always was. For a WAL-RESIDENT page -- no block id -- it was the object id
/// and is now `None`, so the eighth part goes from the object id to ZERO.
///
/// So a row ALREADY ON DISK whose handle was omitted because it was derivable under the old rule
/// now rebuilds to a DIFFERENT handle. The page is then filed and looked up under a key nothing
/// wrote, which resolves a durably acknowledged write to MISSING -- the exact hole `block_in_wal`
/// exists to close. The declined strip and its wasted allocation are the symptom; this is the
/// disease, and refusing the load is the only thing standing between the two.
#[test]
fn an_omitted_page_handle_restores_to_a_different_key_under_the_new_generation_rule() {

    const SLAB: u64 = 3;
    const OFFSET: u64 = 4_096;
    const LENGTH: u64 = 512;
    const OBJECT_ID: u64 = 0x0123_4567_89AB_CDEF;

    // A WAL-RESIDENT page: no block id, an object id. This is the shape every container member
    // takes -- `log_backed` is literally `address.block_id().is_none()`.
    let address = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, None, Some(OBJECT_ID));
    assert!(
        address.block_id().is_none(),
        "the fixture must be WAL-resident, or the generation term does not move and this test is \
         about nothing"
    );

    // WHAT THE OLD RULE DERIVED: generation = block_id.or(object_id) = the object id.
    let written_under_the_old_rule = crate::index_log::block_ref_key_from_parts(
        "set", "s", Some("6d30"), SLAB, OFFSET, LENGTH, 0, OBJECT_ID,
    );
    // WHAT THIS BINARY DERIVES, taken from the address itself rather than restated.
    let derived_now = crate::index_log::block_ref_key_from_parts(
        "set",
        "s",
        Some("6d30"),
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id().unwrap_or_default(),
        address.generation().unwrap_or_default(),
    );

    assert_eq!(
        address.generation(),
        None,
        "a WAL-resident address is supposed to carry no generation now; if it carries one the rule \
         has moved back and the hazard below is not the live one"
    );
    assert_ne!(
        written_under_the_old_rule, derived_now,
        "the two derivations must DIFFER, or there is no hazard to guard and the stamp is \
         unnecessary: old {written_under_the_old_rule}, now {derived_now}"
    );
    // And name the single term that moved, so a future change that alters a DIFFERENT part cannot
    // satisfy this test for the wrong reason.
    let same_parts_same_generation = crate::index_log::block_ref_key_from_parts(
        "set", "s", Some("6d30"), SLAB, OFFSET, LENGTH, 0, OBJECT_ID,
    );
    assert_eq!(
        written_under_the_old_rule, same_parts_same_generation,
        "control: the builder is deterministic over its parts, so the difference above is the \
         generation term and nothing else"
    );
    assert!(
        derived_now.ends_with(":0"),
        "the new derivation's generation term is supposed to be zero: {derived_now}"
    );
    assert!(
        written_under_the_old_rule.ends_with(&format!(":{OBJECT_ID}")),
        "the old derivation's generation term is supposed to be the object id: \
         {written_under_the_old_rule}"
    );
}


/// THE DERIVATION CHANGE IS NOT A MIGRATION, AND THIS IS THE DRIVEN PROOF RATHER THAN THE ARGUMENT.
///
/// `an_omitted_page_handle_restores_to_a_different_key_under_the_new_generation_rule` above proves
/// the two derivations DIFFER. That is necessary and it is not sufficient, and on its own it reads
/// like a migration hazard: a pre-shed writer omitted a page's composite handle because it matched
/// the old derivation, the stamp refuses the base snapshot and routes load into the replay path,
/// and the replay path rebuilds that handle from the NEW derivation. If the rebuilt handle were the
/// name the page is filed and looked up under, every WAL-resident page in such a record would be
/// filed under a name nothing ever wrote -- a durably acknowledged write reading MISSING, on
/// exactly the path the stamp sends load down.
///
/// IT IS NOT, AND THE REASON IS STRUCTURAL. `fold_delta_block_items` is the only thing that turns
/// index-log items into pages, and it never reads `block_ref_key`. It builds a `BlockIndex` from
/// the item's own fields and hands it to `BlockIndexMap::insert`, which ALLOCATES the handle; the
/// map is keyed by that allocated slot. The record's spelling is rebuilt on the way in and
/// recomputed again on the way out, and in between nothing consults it. So a changed derivation
/// cannot misfile a page through this path -- not because the rebuild agrees, but because the
/// rebuild is not used.
///
/// TWO FIXTURES, AND THE SECOND IS THE SHARPER ONE. The first has its handle OMITTED, which is
/// what a pre-shed writer left. The second carries a handle that is not any derivation of
/// anything. If a page whose handle is outright wrong is still filed and still named, then no
/// change to the derivation can break serving here, which is a stronger statement than "this
/// particular change happens to be safe".
///
/// AND THE FOLD IS DRIVEN EXPLICITLY, because the default load path does not reach it:
/// `index_log_replay_reach::the_default_load_path_does_not_fold_the_index_log_and_the_checked_one_does`
/// measures that `load_shard` calls `load_index_base_only` and never folds, and that the fold is
/// reached from the `TS_WAL_LEGACY_RECOVERY` arm and from `install_latest_manifest_if_newer_on_load`
/// through `load_index_checked`. That is a second, independent layer of why this is not a
/// migration -- but it is the weaker one, since an operator can flip that flag, so this test drives
/// the FOLDING arm and makes the claim there.
#[test]
fn a_record_whose_page_handle_was_omitted_still_names_its_page_after_the_fold() {
    use crate::index_log::{IndexItem, IndexItemKind};

    const SHARD: ShardId = 1;
    const SLAB: u64 = 3;
    const OFFSET: u64 = 4_096;
    const LENGTH: u64 = 512;
    const OBJECT_ID: u64 = 0x0123_4567_89AB_CDEF;
    const KEY: &str = "k";
    const KIND: &str = "string";
    const WRONG_KEY: &str = "wrongkey";
    const NOT_A_DERIVATION: &str = "this-is-not-any-derivation-of-anything";

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);

    // WAL-RESIDENT: no block id. The only shape whose generation term moves under this change, and
    // the shape every container member takes -- `log_backed` is `address.block_id().is_none()`.
    let address = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, None, Some(OBJECT_ID));
    assert!(
        address.block_id().is_none(),
        "the fixture must be WAL-resident or the generation term does not move"
    );
    assert_eq!(
        None,
        address.generation(),
        "a WAL-resident address carries no generation under this change; if it does, the hazard \
         under test is not the live one"
    );

    // DENOMINATOR ONE: the two derivations must differ, or there is nothing to be safe from.
    let old_rule = crate::index_log::block_ref_key_from_parts(
        KIND, KEY, None, SLAB, OFFSET, LENGTH, 0, OBJECT_ID,
    );
    let new_rule =
        crate::index_log::block_ref_key_from_parts(KIND, KEY, None, SLAB, OFFSET, LENGTH, 0, 0);
    assert_ne!(
        old_rule, new_rule,
        "the pre-shed and post-shed derivations agree, so this test is about nothing: {old_rule}"
    );

    let routing_bucket = crate::engine::hashing::block_routing_bucket(KEY, 0, u32::MAX);
    let wrong_routing_bucket =
        crate::engine::hashing::block_routing_bucket(WRONG_KEY, 0, u32::MAX);

    // FIXTURE ONE: the handle OMITTED, which is what a pre-shed writer left on disk.
    let omitted = IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket,
        block_ref_key: String::new(),
        object_key: KEY.into(),
        model_id: KIND.to_string(),
        component: None,
        object_id: OBJECT_ID,
        // The fixture asserts above that this address carries no block id, so the three slots
        // this used to state -- 0, LENGTH, true -- are exactly what the codec now derives.
        entry: Some(address.clone()),
        deleted: false,
    };
    // FIXTURE TWO: a handle that is not any derivation of anything.
    let nonsense = IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: wrong_routing_bucket,
        block_ref_key: NOT_A_DERIVATION.to_string(),
        object_key: WRONG_KEY.into(),
        model_id: KIND.to_string(),
        component: None,
        object_id: OBJECT_ID,
        // The fixture asserts above that this address carries no block id, so the three slots
        // this used to state -- 0, LENGTH, true -- are exactly what the codec now derives.
        entry: Some(address.clone()),
        deleted: false,
    };
    engine
        .index_log_store()
        .append_delta(SHARD, vec![omitted, nonsense], Vec::new(), Some(1), None, false, true)
        .expect("the delta record must append");

    // DENOMINATOR TWO: the omitted handle must really be ABSENT from the bytes on disk, and a
    // non-derivable handle must really be PRESENT -- otherwise the two fixtures are
    // indistinguishable and "omitted" is a word rather than a fact.
    let log_root = engine.index_dir.join("indexlogs");
    let mut raw = Vec::new();
    for entry in std::fs::read_dir(&log_root)
        .expect("the index-log directory exists")
        .flatten()
    {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(&format!("shard-{SHARD}.indexlog"))
        {
            raw.extend(std::fs::read(entry.path()).expect("an index-log piece reads"));
        }
    }
    assert!(
        raw.len() > 100,
        "the index log is {} bytes; a search over nothing cannot report an absence",
        raw.len()
    );
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.contains(NOT_A_DERIVATION),
        "a handle that is NOT the derivation must survive into the log, or this log does not carry \
         handles at all and the absence below says nothing"
    );
    assert!(
        !text.contains(&old_rule),
        "the log still carries the pre-shed handle {old_rule}, so nothing was omitted"
    );

    // AND NOW THE FOLD, on the arm that actually folds.
    let folded = engine
        .load_index_checked(SHARD, false)
        .expect("the checked load must not refuse this log")
        .expect("the checked load must return a shard state");

    let pages: Vec<&crate::engine::state::BlockIndex> = folded
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .collect();
    println!("=== after the fold: {} page(s) ===", pages.len());
    for page in &pages {
        println!(
            "  key={:?} kind={} slab={} off={} len={} log_backed={}",
            page.object_key,
            page.model_id.as_str(),
            page.address.block_slab_id(),
            page.address.offset(),
            page.address.length(),
            page.log_backed()
        );
    }

    let omitted_page = pages
        .iter()
        .find(|page| page.object_key.as_ref() == KEY)
        .unwrap_or_else(|| {
            panic!(
                "THE OMITTED-HANDLE PAGE IS MISSING AFTER THE FOLD. If this ever fires, the \
                 rebuilt composite handle HAS become the name the page is filed under, and this \
                 change needs a migration for records already on disk -- a stamp cannot cover it, \
                 because the stamp is what routes load down this path. {} page(s) came back.",
                pages.len()
            )
        });
    let nonsense_page = pages
        .iter()
        .find(|page| page.object_key.as_ref() == WRONG_KEY)
        .expect(
            "the page whose stored handle is not any derivation is missing, so the stored handle \
             IS consulted somewhere on this path",
        );

    // The page has to come back INTACT, not merely present: a page filed under the right name
    // carrying the wrong address would satisfy a presence check and still serve the wrong bytes.
    for (label, page) in [("omitted", omitted_page), ("nonsense", nonsense_page)] {
        assert_eq!(SLAB, page.address.block_slab_id(), "{label}: slab moved");
        assert_eq!(OFFSET, page.address.offset(), "{label}: offset moved");
        assert_eq!(LENGTH, page.address.length(), "{label}: length moved");
        assert!(
            page.log_backed(),
            "{label}: the page must come back WAL-resident, which is the shape the hazard is about"
        );
        assert!(!page.deleted, "{label}: the page must not come back deleted");
    }

    // And the object is named by the bucket that holds it, which is what a per-object read
    // resolves through -- present-but-unindexed would read as missing just the same.
    //
    // BY THE DERIVED ID, NOT THE ONE THE RECORD CARRIED. The first version of this assertion asked
    // for `OBJECT_ID`, the arbitrary value written into the fixture, and it failed -- correctly.
    // After the fold, `update_bucket_layout` rebuilds each bucket's live object set from the pages
    // themselves (`page.object_id(shard_id)`), so the derivation wins and a stale or invented id in
    // a stored record cannot survive the load. That is this change's whole premise arriving on the
    // recovery path, and it is worth asserting in both directions.
    let named = folded
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the routing bucket the omitted record named must exist after the fold");
    let derived_id = crate::engine::hashing::stable_block_object_id(SHARD, KIND, KEY);
    assert!(
        named.object_index.contains(&derived_id),
        "the bucket's object index does not name the page by its DERIVED id, so a per-object read \
         would not reach the page even though the page is filed"
    );
    assert_ne!(
        derived_id, OBJECT_ID,
        "control: the fixture's invented id must differ from the derivation, or the next assertion \
         cannot tell the two apart"
    );
    assert!(
        !named.object_index.contains(&OBJECT_ID),
        "the arbitrary id the record carried survived into the bucket's object index; the fold is \
         supposed to re-derive that set from each page's own terms, so a stored id cannot be \
         authoritative any more"
    );
}

// ---------------------------------------------------------------------------------------------
// AN UNGATED STORE MUST BE REFUSED, NOT MISREAD.
// ---------------------------------------------------------------------------------------------

/// CAPTURE. Prints a whole shard index written with the one-entry-a-page gate OFF, as plain JSON.
///
/// Run at a named revision, its output IS the golden below. It asserts nothing.
///
/// WHY A GOLDEN AND NOT A FIXTURE BUILT AT RUN TIME. The point of the guard below is what happens
/// to a store whose ENTRIES NAME THEIR ELEMENTS, and once `BlockIndex` has no field for an element
/// name there is no way left to build one -- not with the gate off, not by hand. A string constant
/// is the only form of that store that outlives the field, which is what makes it the
/// yesterday's-input kind of golden rather than the today's-output kind.
#[test]
#[ignore = "capture instrument; run by name and read its output"]
fn capture_an_ungated_store_index() {
    let restore = std::env::var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE).ok();
    std::env::set_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE, "0");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(7);
    for (field, value) in [("f0", b"v0".to_vec()), ("f1", b"v1".to_vec())] {
        engine.execute(crate::types::ExecuteRequest {
            shard_id: 7,
            command: crate::types::Command::HashSet {
                key: "ungated/hash".to_string(),
                field: field.to_string(),
                value,
            },
        });
    }
    engine.execute(crate::types::ExecuteRequest {
        shard_id: 7,
        command: crate::types::Command::ZSetAdd {
            key: "ungated/zset".to_string(),
            member: b"m0".to_vec(),
            score: 1.0,
        },
    });
    engine.flush_shard_index(7);
    let path = dir.path().join("indexes").join("shard-7.index.json");
    let bytes = std::fs::read(&path).expect("a flushed base index exists");
    let shard = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    let named: Vec<(String, String, Option<String>)> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .map(|page| {
            (
                page.object_key.to_string(),
                page.model_id.to_string(),
                page.component.as_deref().map(str::to_string),
            )
        })
        .collect();
    println!("\n=== entries, with the names the ungated layout files ===");
    for row in &named {
        println!("  {row:?}");
    }
    println!("\n=== the whole index as plain JSON ===");
    println!(
        "{}",
        String::from_utf8(crate::engine::encode_index_bytes_as_plain_json(&shard))
            .expect("the index is utf-8")
    );
    match restore {
        Some(previous) => {
            std::env::set_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE, previous)
        }
        None => std::env::remove_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE),
    }
}

/// A WHOLE SHARD INDEX WRITTEN WITH THE GATE OFF, AT THE STAMP A PRE-CHANGE BINARY CARRIES.
///
/// Captured by `capture_an_ungated_store_index` from this branch's own ungated writer, with
/// `index_format_version` set to 8 -- what `matrixark/main` holds. Three entries, each NAMING its
/// element: `component` `f0` and `f1` on a hash and `6d30` (hex `m0`) on a zset, and the written
/// key of each embedding that name.
///
/// A STRING CONSTANT, WHICH IS THE WHOLE POINT. Once `BlockIndex` has no field for an element
/// name there is no way left to BUILD this store -- not with a gate, not by hand -- so bytes are
/// the only form of it that outlives the field. This is the yesterday\x27s-input kind of golden: it
/// is not what this engine writes and must never be regenerated to match what it writes.
const UNGATED_STORE_INDEX_AT_STAMP_8: &str = r##"{"index_format_version":8,"wal_resident_blocks":{},"expires_at_ms":{},"strings":{},"hashes":{"ungated/hash":{"f1":{"a":27,"l":27,"pi":1,"oi":null,"g":1},"f0":{"a":0,"l":27,"pi":0,"oi":null,"g":0}}},"sets":{},"seen":{},"buckets":{},"zsets":{"ungated/zset":[[[109,48],[13830554455654793216,{"a":54,"l":34,"pi":0,"oi":null,"g":0}]]]},"lists":{},"features":{},"control_state":{},"control_state_blocks":{},"control_state_changes":{},"control_state_change_sketch":{},"control_state_selection":{},"control_state_uuid":{},"context_nodes":{},"context_event_timeline":{},"context_audits":{},"context_entities":{},"context_children":{},"context_summaries":{},"context_compressions":{},"slot_index":{"bucket_map":{"979707521":{"routing_slot":979707521,"layout":"SingleBlockObject","dirty":true,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":1,"object_index":[3660521199139779587],"deleted_object_index":[],"page_index":{"zset:ungated/zset:6d30:0:54:34:0:0":{"object_key":"ungated/zset","model_id":"zset","component":"6d30","address":{"a":54,"l":34,"pi":0,"oi":null,"g":0},"dirty":true,"deleted":false}}},"3709665044":{"routing_slot":3709665044,"layout":"MultiBlockObject","dirty":true,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":2,"object_index":[14105434925875933987],"deleted_object_index":[],"page_index":{"hash:ungated/hash:f0:0:0:27:0:0":{"object_key":"ungated/hash","model_id":"hash","component":"f0","address":{"a":0,"l":27,"pi":0,"oi":null,"g":0},"dirty":true,"deleted":false},"hash:ungated/hash:f1:0:27:27:1:1":{"object_key":"ungated/hash","model_id":"hash","component":"f1","address":{"a":27,"l":27,"pi":1,"oi":null,"g":1},"dirty":true,"deleted":false}}}}},"applied_wal_sequence":3}
"##;

/// AN UNGATED STORE IS REFUSED BEFORE IT IS SERVED, WHICH IS WHAT MAKES THE ELEMENT NAME REMOVABLE.
///
/// # THE QUESTION THIS SETTLES
///
/// Removing the element name from a page entry means a store whose entries DO name their elements
/// becomes uninterpretable: the named decoder drops a key it has no field for, SILENTLY, so such a
/// store would decode cleanly into an index whose container entries name nothing. If that index
/// were then served, every element it names would be lost or mis-served -- which is not a format
/// change, it is data loss.
///
/// The stamp is what has to stop it, and `persistence.rs` compares with `<`, so a value that is too
/// LOW falls through to Accepted. That asymmetry is exactly the lethal direction, which is why this
/// is DRIVEN rather than argued: the store is planted, the shard is loaded, and the load path is
/// read off the engine's own counters.
///
/// # THE ORDER IS THE LOAD-BEARING PART
///
/// The refusal happens AFTER the decode and BEFORE the index is used -- `decode_index_bytes`
/// succeeds, the dropped key is already gone by then, and `load_index_inner` refuses on the stamp
/// and answers `Ok(None)` so the caller replays the write-ahead log instead. So the silent drop is
/// harmless only because the refusal follows it. If that order ever inverted, this guard is what
/// would catch it: the first arm asserts the decode SUCCEEDS and the names are gone, and the second
/// asserts the shard nevertheless serves nothing from it.
///
/// # AND IT IS COUNTED BOTH WAYS
///
/// `stale > 0` alone is not enough: `index_load_path_counts` is four independent counters and a
/// load that was refused AND accepted would move both. So `accepted` is asserted at zero over the
/// same reset window. An earlier guard in this file moves `stale` and leaves `accepted` unchecked.
#[test]
fn an_ungated_store_is_refused_before_it_is_served() {
    use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};

    // --- ARM 1: THE DECODE SUCCEEDS, AND IT IS THE DECODE THAT LOSES THE NAMES. ---
    let decoded = crate::engine::decode_index_bytes(UNGATED_STORE_INDEX_AT_STAMP_8.as_bytes())
        .expect("an index written by a pre-change binary must still DECODE -- the stamp refuses it \
                 one layer up, and a decode that failed here would hide that");
    assert_eq!(
        8, decoded.index_format_version,
        "the golden's stamp is {} rather than 8, so it is not the pre-change store this guard is \
         about",
        decoded.index_format_version
    );
    let container_entries: Vec<(String, String, Option<String>)> = decoded
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| matches!(page.model_id.as_str(), "hash" | "zset" | "set" | "list"))
        .map(|page| {
            (
                page.object_key.to_string(),
                page.model_id.as_str().to_string(),
                element_name_of(page),
            )
        })
        .collect();
    println!("\n=== a pre-change store, decoded by this binary ===");
    for row in &container_entries {
        println!("  {row:?}");
    }
    // FLOOR: the golden really does hold container entries, or the naming claim below is about an
    // empty set and the refusal arm is about an empty store.
    assert_eq!(
        3,
        container_entries.len(),
        "the golden decoded to {} container entries rather than 3, so it is not the store this \
         guard was captured from",
        container_entries.len()
    );

    // THE NAMES ARE A FACT ABOUT THE STORED BYTES, asserted there rather than on the decoded
    // entries -- because what this binary can still READ off an entry is exactly what the change
    // under test takes away. The bytes cannot change; the field can. So the golden is asserted to
    // SPELL the three element names, and what the decode recovered is PRINTED beside it.
    for spelling in [
        "\"component\":\"f0\"",
        "\"component\":\"f1\"",
        "\"component\":\"6d30\"",
    ] {
        assert!(
            UNGATED_STORE_INDEX_AT_STAMP_8.contains(spelling),
            "the golden does not spell {spelling}, so it is not a store whose entries name their \
             elements and the refusal below is about nothing"
        );
    }
    let recovered = container_entries
        .iter()
        .filter(|(_, _, name)| name.is_some())
        .count();
    println!(
        "  of 3 stored element names, this binary recovered {recovered} off the entries -- a drop \
         to 0 is the silent key drop that the refusal below stands between a reader and"
    );

    // --- ARM 2: THE SHARD REFUSES IT, AND SERVES NOTHING FROM IT. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let indexes = dir.path().join("indexes");
    std::fs::create_dir_all(&indexes).expect("mkdir");
    std::fs::write(
        indexes.join("shard-7.index.json"),
        UNGATED_STORE_INDEX_AT_STAMP_8.as_bytes(),
    )
    .expect("plant the pre-change index");
    // AND THE ROUTING-RANGE STAMP BESIDE IT, WHICH A PRE-CHANGE STORE REALLY HAS.
    //
    // Without it this guard asserted nothing and did not say so: `decide_routing_range` refuses a
    // store that has ON-DISK STATE and NO range stamp, before the index is read at all, so all four
    // index-load counters stayed at ZERO -- not even `absent`. Measured, by driving it: a refusal
    // arm that cannot tell "refused for the reason under test" from "never asked" is the shape of a
    // control that passes for an unintended reason. A binary that wrote this index also wrote this
    // file, so planting both is what makes the FORMAT stamp the thing being tested.
    crate::engine::routing_range_stamp::write_routing_range_stamp(
        &indexes,
        7,
        crate::engine::routing_range_stamp::RoutingRangeStamp {
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
        },
    )
    .expect("plant the routing-range stamp the index was written under");

    reset_index_load_path_counts();
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        indexes,
    );
    // THE LOAD'S OWN STATUS, CHECKED. A first draft of this guard did not, and every counter read
    // ZERO -- not even `absent` -- which is what a load that never reached the index looks like.
    // A refusal arm that cannot tell "refused" from "never asked" asserts nothing.
    engine.load_shard(7);
    let (accepted, stale, absent, undecodable) = index_load_path_counts();
    println!(
        "  load path -> accepted {accepted}, stale {stale}, absent {absent}, undecodable \
         {undecodable}"
    );
    assert!(
        stale > 0,
        "a store stamped 8 against a current {} was NOT counted as a stale-stamp refusal: accepted \
         {accepted}, stale {stale}, absent {absent}, undecodable {undecodable}. \
         `persistence.rs` compares with `<`, so the direction that fails silently is a stamp that \
         is too LOW -- and this is that direction",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert_eq!(
        0, accepted,
        "the load counted {accepted} ACCEPTED beside {stale} refused. Four independent counters \
         mean a load can move both, and a refusal that is also an acceptance is an acceptance"
    );
    assert_eq!(
        0, undecodable,
        "the planted index was counted UNDECODABLE, so this guard is measuring a parse failure \
         rather than the stamp -- and a parse failure would hide the silent key drop arm 1 asserts"
    );

    // AND NOTHING IS SERVED FROM IT. There is no write-ahead log beside the planted index, so a
    // refusal leaves an empty shard; anything served here came out of the index this binary was
    // supposed to refuse.
    let served = engine.execute(crate::types::ExecuteRequest {
        shard_id: 7,
        command: crate::types::Command::HashGetAll {
            key: "ungated/hash".to_string(),
        },
    });
    let entries = match served.response {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("expected HashEntries, got {other:?}"),
    };
    println!("  served from the refused index: {} field(s)", entries.len());
    assert!(
        entries.is_empty(),
        "the refused index served {} field(s): {:?}. A refused store must be REPLAYED, never read \
         -- and with no log beside it the honest answer is nothing at all",
        entries.len(),
        entries
    );
}

/// The element name a page entry carries, read in the one place that has to change when the field
/// goes.
///
/// A HELPER AND NOT AN INLINE FIELD READ, so that removing `BlockIndex::component` leaves ONE
/// compile error here with a comment attached rather than silently turning the naming assertion
/// above into `None == None`. When the field goes this answers `None` for every entry, which is
/// precisely the silent drop arm 1 is about -- and the assertion above compares against the golden's
/// own recorded names rather than against whatever this returns.
fn element_name_of(page: &BlockIndex) -> Option<String> {
    page.component.as_deref().map(str::to_string)
}
