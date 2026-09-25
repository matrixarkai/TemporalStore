// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THREE NAMES A PAGE ENTRY CARRIES, and whether they are per-page or per-bucket facts.
//!
//! #1959 ranked `BlockIndex` as the next dominant per-item term -- per PAGE rather than per
//! bucket, and so 52.0x the `BucketNode` total in a container store -- and opened up its 104
//! bytes: the address is 48, three flag bytes sit inside the alignment rounding, and **the three
//! names are 48**, the largest group. It called them "the three shared names" and priced holding
//! them as thin pointers at 80 bytes an entry.
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
            models.insert(page.model_id.as_ref());
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

fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
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
fn seed_routed_keys(engine: &TemporalEngine, strings_n: usize) {
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
            slot.1.insert(page.model_id.as_ptr() as usize);
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
    // shared regardless would say the same thing here whether or not the pool existed. ---
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
    use crate::block_store::BlockAddress;
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
            object_key: Arc::from(key),
            model_id: Arc::from(model),
            component: component.map(Arc::from),
            address: BlockAddress::from_parts(
                slab,
                offset,
                length,
                Some(4),
                Some(42),
                Some(7),
            ),
            dirty: flags.0,
            deleted: flags.1,
            log_backed: flags.2,
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
        last_dump_sequence: 11,
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
                entry.address.block_slab_id,
                entry.address.offset,
                entry.address.length(),
                entry.dirty,
                entry.deleted,
                entry.log_backed
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE CAPTURED BYTES. Produced by `capture_the_stored_spelling_of_a_page_entry` run at
// `15583789e` -- the merge base of this change -- and pasted here verbatim.
// ---------------------------------------------------------------------------------------------

const PAGE_ENTRY_PLAIN: &str = r#"{"object_key":"k","model_id":"string","address":{"ps":1,"o":2,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true}"#;

const PAGE_ENTRY_WITH_COMPONENT: &str = r#"{"object_key":"k","model_id":"string","component":"f0","address":{"ps":1,"o":2,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true}"#;

const PAGE_ENTRY_ALL_FLAGS: &str = r#"{"object_key":"k","model_id":"string","address":{"ps":1,"o":2,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":true,"deleted":true,"log_backed":true}"#;

const PAGE_ENTRY_OVER_WIDE: &str = r#"{"object_key":"k","model_id":"string","address":{"ps":1,"o":2,"l":4294967295,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true}"#;

/// A whole shard index written at `15583789e`: one bucket holding six pages -- five of one object
/// (four plain, one carrying a component and a different kind) and one of a SECOND object in the
/// same bucket, which is what a cluster-range shard produces routinely.
const OLD_STORE_INDEX: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:4":{"object_key":"k","model_id":"hash","component":"f0","address":{"ps":1,"o":9,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:4":{"object_key":"k","model_id":"string","address":{"ps":1,"o":0,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:4":{"object_key":"k","model_id":"string","address":{"ps":1,"o":1,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:4":{"object_key":"k","model_id":"string","address":{"ps":1,"o":2,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:4":{"object_key":"k","model_id":"string","address":{"ps":1,"o":3,"l":3,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:4":{"object_key":"other","model_id":"string","address":{"ps":2,"o":1,"l":5,"pi":4,"oi":42,"rs":7,"g":4,"h":null},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// THE SAME INDEX, CARRYING THE COMBINATION NO WRITER PRODUCES.
///
/// Byte for byte the text above as it stood before `generation` became derived: `"g":9` beside a
/// `"pi":4`, and block-ref keys ending `:4:9` to match. This engine has never written that --
/// every production constructor passed `block_id.or(object_id)` -- but an index that DID carry it
/// must not be loaded and silently re-keyed, because the generation is hashed into the page
/// handle and rendered into the key, and those handles are on disk inside the lookup refs.
const OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:9":{"object_key":"k","model_id":"hash","component":"f0","address":{"ps":1,"o":9,"l":3,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:9":{"object_key":"k","model_id":"string","address":{"ps":1,"o":0,"l":3,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:9":{"object_key":"k","model_id":"string","address":{"ps":1,"o":1,"l":3,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:9":{"object_key":"k","model_id":"string","address":{"ps":1,"o":2,"l":3,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:9":{"object_key":"k","model_id":"string","address":{"ps":1,"o":3,"l":3,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:9":{"object_key":"other","model_id":"string","address":{"ps":2,"o":1,"l":5,"pi":4,"oi":42,"rs":7,"g":9,"h":null},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// What this binary must write back after loading `OLD_STORE_INDEX`: the same bytes.
///
/// Named rather than called "the text above", because there are now two index fixtures here and
/// only one of them is the one that loads.
const OLD_STORE_INDEX_CANONICAL: &str = OLD_STORE_INDEX;

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
fn page_fixture(component: Option<&str>, length: u64, flags: (bool, bool, bool)) -> BlockIndex {
    BlockIndex {
        object_key: Arc::from("k"),
        model_id: Arc::from("string"),
        component: component.map(Arc::from),
        address: crate::block_store::BlockAddress::from_parts(
            1,
            2,
            length,
            Some(4),
            Some(42),
            Some(7),
        ),
        dirty: flags.0,
        deleted: flags.1,
        log_backed: flags.2,
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
#[test]
fn the_stored_spelling_of_a_page_entry_did_not_move() {
    let plain = serde_json::to_string(&page_fixture(None, 3, (false, false, true)))
        .expect("a page entry serializes");
    let with_component = serde_json::to_string(&page_fixture(Some("f0"), 3, (false, false, true)))
        .expect("a page entry serializes");
    let all_flags = serde_json::to_string(&page_fixture(None, 3, (true, true, true)))
        .expect("a page entry serializes");
    let over_wide = serde_json::to_string(&page_fixture(None, u64::MAX, (false, false, true)))
        .expect("a page entry serializes");

    println!("PLAIN          = {plain}");
    println!("WITH_COMPONENT = {with_component}");
    println!("ALL_FLAGS      = {all_flags}");
    println!("OVER_WIDE      = {over_wide}");

    assert_eq!(PAGE_ENTRY_PLAIN, plain, "the stored spelling of a page entry moved");
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
    let saturated = page_fixture(None, u64::MAX, (false, false, true)).address.length();
    assert_eq!(
        u32::MAX as u64,
        saturated,
        "an over-wide length must saturate at u32::MAX, not wrap"
    );
    let low_bits = page_fixture(None, 0x1_0000_0003, (false, false, true))
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
/// AND THE REVERSE, where it is meaningful: the index this binary re-serializes from what it
/// loaded must be the SAME BYTES, so a store written by the new binary is one an old binary reads.
///
/// THE GENERATION IN THESE BYTES MOVED, AND ONLY IT. When `generation` became derived from
/// `block_id.or(object_id)`, the fixture behind this capture stopped being able to express the
/// independent `9` it had chosen beside a `block_id` of `4` -- a combination no production
/// constructor in this engine has ever emitted. The bytes were regenerated with the capture
/// instrument in this module rather than hand-edited, and they came back the same 1,349 bytes
/// with `"g":4` and keys ending `:4:4`. An index that really did carry the old combination is
/// REFUSED rather than re-keyed; the test directly below drives it.
#[test]
fn an_index_written_before_this_change_loads_page_for_page_and_writes_back_the_same_bytes() {
    let index: crate::engine::state::CoreIndex =
        serde_json::from_str(OLD_STORE_INDEX).expect("an index written at 15583789e must load");

    let bucket = index
        .bucket_map
        .get(&7)
        .expect("the stored bucket must load");

    // --- Element by element, in the order the stored map spells them. ---
    let mut loaded: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> = index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .map(|page| {
            (
                page.object_key.to_string(),
                page.model_id.to_string(),
                page.component.as_deref().map(str::to_string),
                page.address.block_slab_id,
                page.address.offset,
                page.address.length(),
                page.dirty,
                page.deleted,
                page.log_backed,
            )
        })
        .collect();
    loaded.sort();

    let mut expected: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> =
        OLD_STORE_PAGES
            .iter()
            .map(|(key, model, component, slab, offset, length, dirty, deleted, log_backed)| {
                (
                    (*key).to_string(),
                    (*model).to_string(),
                    component.map(str::to_string),
                    *slab,
                    *offset,
                    *length,
                    *dirty,
                    *deleted,
                    *log_backed,
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

    // --- And back: the same bytes, so an old binary reads what this one writes. ---
    let rewritten = serde_json::to_string(&index).expect("the index re-serializes");
    assert_eq!(
        OLD_STORE_INDEX_CANONICAL, rewritten,
        "the index this binary writes back is not the index it was given"
    );
    assert!(
        rewritten.len() > 500,
        "the re-serialized index is {} bytes; a comparison of two near-empty documents cannot \
         report a difference",
        rewritten.len()
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
            for byte in page.model_id.as_bytes() {
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
/// `part4::blocks_of_one_kind_share_a_single_kind_string` already asserts that pages of one kind
/// point at one allocation of it. Its fixture is 200 strings and 40 hashes whose field is always
/// `"f"` -- ONE component name in the whole store -- so the pool it checks holds three entries
/// against a cap of sixty-four and can never be full. The failure this test is about needs the
/// pool FULL, and the only thing in this engine that fills it is a container.
///
/// Both counts are read, and the kind count is the claim: a pool that took components would be
/// at its cap, and the four kinds of a container store would be allocating one copy per page of a
/// four-character string. The component pool is asserted to be AT ITS CAP in the same breath, so
/// this cannot pass by the containers having quietly stopped producing components.
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

    // And the consequence, which is the thing that actually costs: every page of a kind points at
    // one allocation of it.
    let mut first_of_kind: std::collections::HashMap<String, Arc<str>> =
        std::collections::HashMap::new();
    let mut pages = 0usize;
    let mut shared = 0usize;
    let mut compared = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            pages += 1;
            match first_of_kind.get(page.model_id.as_ref()) {
                None => {
                    first_of_kind.insert(page.model_id.to_string(), Arc::clone(&page.model_id));
                }
                Some(first) => {
                    compared += 1;
                    if Arc::ptr_eq(first, &page.model_id) {
                        shared += 1;
                    }
                }
            }
        }
    }
    assert!(pages > 0, "no pages were recorded; nothing was measured");
    assert!(
        compared > 0,
        "every page in this fixture is the first of its kind, so 'all shared' is true for free"
    );
    assert_eq!(
        compared, shared,
        "{shared} of {compared} pages share their kind string with the first page of that kind; \
         the rest hold their own copy of a name with four distinct values in the whole store"
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
        2, SHARD_INDEX_FORMAT_VERSION,
        "the struct version moved; the refusal messages pinned above quote it"
    );
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
                page.address.block_slab_id,
                page.address.offset,
                page.address.length(),
                page.dirty,
                page.deleted,
                page.log_backed,
            )
        })
        .collect();
    pages.sort();
    pages
}
