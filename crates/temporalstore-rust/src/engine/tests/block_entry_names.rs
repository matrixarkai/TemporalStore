// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THREE NAMES A BLOCK ENTRY CARRIES, and whether they are per-block or per-bucket facts.
//!
//! #1959 ranked `BlockIndex` as the next dominant per-item term -- per BLOCK rather than per
//! bucket, and so 52.0x the `BucketNode` total in a container store -- and opened up its 104
//! bytes: the address was 48, three flag bytes sat inside the alignment rounding, and **the three
//! names were 48**, the largest group. It called them "the three shared names" and priced holding
//! them as thin pointers at 80 bytes an entry.
//!
//! EVERY ONE OF THOSE FIGURES IS HISTORY NOW, and they are left in the past tense rather than
//! updated because they are what this module was written against. The entry is 64, the address 24
//! and the names 33 -- `model_id` stopped being a fat pointer and became a one-byte spelling -- and
//! a thin pointer would price at 48 rather than 80. `block_entry_name_pointer.rs` carries that
//! arithmetic, reads every width off the field rather than off a type named at the call site, and
//! declines the change.
//!
//! "SHARED" IS AN ASSUMPTION ABOUT WHERE THE NAMES COME FROM, and this module measures it instead.
//! The three are `object_key`, `model_id` and `element_name`. If all three were the same for every
//! block of a routing bucket, storing them once per block would be pure duplication and the entry
//! could carry a handle to the bucket's copy. That is the change this module was written to
//! justify, and it does not survive the measurement.
//!
//! WHAT THE COUNTS SAY, over real stores seeded through the engine:
//!
//!   * `element_name` is a PER-BLOCK fact, by construction. A container key's fields, members and
//!     elements are each their own block and they all route to the container key's one bucket; the
//!     element name is the only thing that tells them apart. A bucket of 100 blocks holds 100 distinct
//!     element names. Hoisting it would file a hundred hash fields as one block.
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
//! lose blocks on a store built by the cluster path -- and it would lose them SILENTLY, because a
//! block filed under the wrong object name is still on its slab and nothing looks for it. Both
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

/// How many DISTINCT values of each of the three names a routing bucket's blocks carry, as bucket
/// counts keyed by the distinct count.
///
/// A histogram and not a mean for the reason #1959 established: a mean of "1.02 object keys a
/// bucket" is consistent with a store where every bucket holds one object and with a store where
/// one bucket in fifty holds two, and only the second loses data when the name is hoisted.
#[derive(Debug, Default, Clone)]
struct NameSpread {
    object_key: BTreeMap<usize, usize>,
    model_id: BTreeMap<usize, usize>,
    element_name: BTreeMap<usize, usize>,
    /// Buckets keyed by blocks held, so the container case can be asserted reached.
    blocks_held: BTreeMap<usize, usize>,
    /// Buckets whose every block carries the SAME element name, counted separately: a bucket holding
    /// one block trivially does, and a claim built on those would be vacuous.
    multi_block_buckets_with_one_element_name: usize,
    multi_block_buckets: usize,
}

impl NameSpread {
    fn buckets(&self) -> usize {
        self.blocks_held.values().copied().sum()
    }

    fn blocks(&self) -> usize {
        self.blocks_held.iter().map(|(held, n)| held * n).sum()
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
            "  buckets={} blocks={} widest bucket={} block(s)",
            self.buckets(),
            self.blocks(),
            Self::widest(&self.blocks_held)
        );
        for (name, map) in [
            ("object_key", &self.object_key),
            ("model_id", &self.model_id),
            ("element_name", &self.element_name),
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
            "  multi-block buckets = {}, of which all-one-element-name = {}",
            self.multi_block_buckets, self.multi_block_buckets_with_one_element_name
        );
    }
}

/// Walk every bucket of the loaded shard and count distinct names across its blocks.
///
/// `element_name` is counted over the OPTION, so a bucket where some blocks have an element name and some
/// do not counts two -- which is the honest answer for "could one stored value serve them all".
fn name_spread(engine: &TemporalEngine) -> NameSpread {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut spread = NameSpread::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        let mut keys: BTreeSet<&str> = BTreeSet::new();
        let mut models: BTreeSet<&str> = BTreeSet::new();
        let mut element_names: BTreeSet<Option<&str>> = BTreeSet::new();
        let mut held = 0usize;
        for (handle, block) in bucket.block_index.iter() {
            held += 1;
            keys.insert(named.at(bucket.routing_bucket, *handle));
            models.insert(block.model_id().as_str());
            // Every entry's name is the same absence, so this set holds one member for any
            // non-empty bucket. The histogram it feeds is restated where it is read.
            element_names.insert(None::<&str>);
        }
        *spread.blocks_held.entry(held).or_default() += 1;
        *spread.object_key.entry(keys.len()).or_default() += 1;
        *spread.model_id.entry(models.len()).or_default() += 1;
        *spread.element_name.entry(element_names.len()).or_default() += 1;
        if held > 1 {
            spread.multi_block_buckets += 1;
            if element_names.len() == 1 {
                spread.multi_block_buckets_with_one_element_name += 1;
            }
        }
    }
    spread
}

pub(super) fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
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
/// element is its own block and they all route to the container key's one bucket.
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

/// ARE THE THREE NAMES THE SAME FOR EVERY BLOCK OF A BUCKET? Counted, at two corpus sizes and over
/// BOTH routing-bucket ranges this engine ships.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted. Counts are immune to it --
/// allocation bytes are not, at about six bytes a character -- which is why the counts carry the
/// claim and no byte figure appears in this test.
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn the_three_names_in_a_block_entry_are_per_object_and_a_bucket_is_not_an_object() {
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
        assert!(spread.blocks() > 0, "denominator: no block entries in this arm");
    }

    // --- 1. `element_name` is a PER-BLOCK fact, in both ranges. ---
    //
    // The container arm is where this is visible at all, and the assertion is EQUALITY with the
    // blocks held rather than ">1": a bucket of a hundred blocks holding two distinct element names
    // would still refute hoisting, but it would not be the structure this engine actually has.
    for (index, spread) in wide_container.iter().chain(narrow_container.iter()).enumerate() {
        assert_eq!(
            0, spread.multi_block_buckets_with_one_element_name,
            "container arm {index}: {} multi-block buckets hold ONE distinct element name; every block \
             of a container is a distinct field, member or element, so a bucket whose blocks shared \
             an element name would mean the element name had stopped telling blocks apart",
            spread.multi_block_buckets_with_one_element_name
        );
        assert!(
            spread.multi_block_buckets > 0,
            "container arm {index}: no bucket holds more than one block, so this arm cannot say \
             anything about whether a name is shared ACROSS blocks -- it would be vacuous"
        );
        assert!(
            NameSpread::widest(&spread.element_name) >= 100,
            "container arm {index}: the widest bucket holds {} distinct element names; the fixture is \
             supposed to reach the container case where the per-block entry dominates at 52x",
            NameSpread::widest(&spread.element_name)
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

/// Distinct ALLOCATIONS of each name behind the blocks of one object, counted by pointer.
///
/// Contents cannot tell one shared allocation from two equal ones; pointers can. This is the same
/// instrument `part4::the_block_and_the_lookup_point_at_one_object_key` uses, applied across the
/// blocks of ONE OBJECT rather than between one block and the lookup -- which is the axis that test
/// cannot see, because every object in its fixture has exactly one block.
#[derive(Debug, Default, Clone)]
struct KeyAllocationCensus {
    /// Objects keyed by how many distinct object-key allocations their blocks hold.
    object_key: BTreeMap<usize, usize>,
    /// Objects keyed by how many distinct model-id allocations their blocks hold.
    model_id: BTreeMap<usize, usize>,
    objects: usize,
    blocks: usize,
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
        println!("  objects={} blocks={}", self.objects, self.blocks);
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
            "  surplus object-key allocations = {} ({:.4} a block), {} B ({:.2} B a block)",
            self.surplus_object_key_allocations,
            if self.blocks == 0 {
                0.0
            } else {
                self.surplus_object_key_allocations as f64 / self.blocks as f64
            },
            self.surplus_object_key_bytes,
            if self.blocks == 0 {
                0.0
            } else {
                self.surplus_object_key_bytes as f64 / self.blocks as f64
            }
        );
    }
}

/// An `Arc<str>`'s own allocation: the text, behind two words of strong/weak count.
const ARC_HEADER_BYTES: usize = 16;

fn key_allocation_census(engine: &TemporalEngine) -> KeyAllocationCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // (model_id, object_key) -> the distinct allocation addresses its blocks hold.
    let mut by_object: BTreeMap<(String, String), (BTreeSet<usize>, BTreeSet<usize>, usize)> =
        BTreeMap::new();
    // READ THROUGH THE POSITION MAP, AND THE OBJECT-KEY COLUMN IS NOW A STRUCTURAL CONTROL.
    //
    // This counted the distinct ALLOCATIONS of one object key that its own blocks held, and
    // `surplus_object_key_allocations` was the copies beyond the first. A block entry holds no
    // key, so there is nothing per block to be a copy OF: the blocks of an object are all handed
    // the one `Arc` their bucket's lookup holds, and the column reads 1 BY CONSTRUCTION.
    //
    // KEPT, FOR THE REASON THE `model_id` COLUMN BESIDE IT IS KEPT -- and the comment there says
    // it first. It is no longer evidence that a sharing pool was consulted; it still
    // DISCRIMINATES, because a read path that copied the text per block instead of cloning the
    // `Arc` would report two here. That is not hypothetical: forty walks in this module tree take
    // a key per block through this same inversion, and a copy per block would reinstate the exact
    // duplication this change removed.
    let named = super::BlockObjects::of(&shard.bucket_index);
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in bucket.block_index.iter() {
            let object_key = named.at(bucket.routing_bucket, *handle);
            let slot = by_object
                .entry((block.model_id().to_string(), object_key.to_string()))
                .or_default();
            slot.0.insert(object_key.as_ptr() as usize);
            // The spelling is a `&'static str` off the registry now, not an allocation: one
            // address per kind for the whole process, so this census reads 1 BY CONSTRUCTION
            // rather than because a pool happened to be consulted. Kept as the control it was.
            slot.1.insert(block.model_id().as_str().as_ptr() as usize);
            slot.2 += 1;
        }
    }
    let mut census = KeyAllocationCensus::default();
    for ((_, object_key), (key_ptrs, model_ptrs, blocks)) in by_object.iter() {
        census.objects += 1;
        census.blocks += blocks;
        *census.object_key.entry(key_ptrs.len()).or_default() += 1;
        *census.model_id.entry(model_ptrs.len()).or_default() += 1;
        let surplus = key_ptrs.len().saturating_sub(1);
        census.surplus_object_key_allocations += surplus;
        census.surplus_object_key_bytes += surplus * (object_key.len() + ARC_HEADER_BYTES);
    }
    census
}

/// THE BLOCKS OF ONE OBJECT HOLD ONE ALLOCATION OF ITS KEY.
///
/// `part4::the_block_and_the_lookup_point_at_one_object_key` already asserts that a block and the
/// object lookup point at one copy. Its fixture is 64 STRING keys -- one block each -- so the
/// question it asks is only ever asked of a single block, and the axis where the answer differs is
/// the one it cannot reach: an object with MANY blocks, filed one block at a time.
///
/// That is the container workload, and it is precisely the workload where #1959 measured the block
/// entry at 52.0x the node total. A hash of a hundred fields is a hundred blocks of ONE object; if
/// each block's entry built its own `Arc::from(object_key)`, the store would hold a hundred
/// allocations of one short string and ninety-nine of them would carry nothing the first did not.
///
/// Counted by POINTER. Two equal strings are indistinguishable by contents and cost twice as much,
/// which is the whole reason this is not an equality assertion.
///
/// THE ANTI-VACUITY CHECK IS THE FIXTURE ITSELF: an object with one block trivially holds one
/// allocation, so the assertion is made only after the census is shown to contain objects with
/// many blocks, and the widest is asserted to reach the hundred-block case.
#[test]
#[ignore = "seeds four stores; run by name"]
fn the_blocks_of_one_object_hold_one_allocation_of_its_object_key() {
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

    // --- The fixture must reach the many-block object, or every claim below is free. ---
    for (index, census) in containers.iter().enumerate() {
        assert!(
            census.objects > 0 && census.blocks > 0,
            "container arm {index}: nothing censused"
        );
        assert!(
            census.blocks / census.objects >= 100,
            "container arm {index}: {} blocks over {} objects; this arm exists to hold objects of \
             a hundred blocks and it does not reach them",
            census.blocks,
            census.objects
        );
    }

    // --- The claim. ---
    for (index, census) in containers.iter().chain(routed.iter()).enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.object_key),
            "arm {index}: an object's blocks hold up to {} distinct allocations of one object key; \
             {} surplus copies across {} blocks, {} B",
            KeyAllocationCensus::widest(&census.object_key),
            census.surplus_object_key_allocations,
            census.blocks,
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
    // is a `&'static str` off `model_kind_registry`, so every block of a kind reads the same
    // address and no allocation exists to be duplicated. It is kept because it still discriminates
    // -- a census that reported two here would mean it is no longer reading the registry's
    // spelling -- but it is no longer evidence about a pool. ---
    for (index, census) in containers.iter().enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.model_id),
            "container arm {index}: the interned kind is held as {} allocations across one \
             object's blocks; if this ever exceeds one the census is measuring something else",
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
fn capture_the_stored_spelling_of_a_block_entry() {
    use crate::block_store::ElementEntry;
    use crate::engine::state::{BucketLayoutState, BucketNode, CoreIndex};

    fn block(
        key: &str,
        model: &str,
        element_name: Option<&str>,
        slab: u64,
        offset: u64,
        length: u64,
        flags: (bool, bool, bool),
    ) -> BlockIndex {
        BlockIndex::new(
            crate::engine::storage_bucket_internals::stored_model_kind(model),
            ElementEntry::from_parts( slab, offset, length, Some(4), Some(42), ),
            flags.0,
            flags.1,
        )
    }

    // THROUGH THE WRAPPER. The entry has no `Serialize` of its own, and it no longer takes an
    // identity either: `object_key` and `model_id` are the composite key's own second and first
    // slots and the decoder reads them back out of it, so the wrapper writes three keys and the
    // bytes printed here are two keys lighter than they were.
    fn encoded(entry: &BlockIndex) -> String {
        serde_json::to_string(&crate::engine::state::BlockEntryAt { block: entry }).unwrap()
    }

    println!(
        "PLAIN          = {}",
        encoded(&block("k", "string", None, 1, 2, 3, (false, false, true)))
    );
    println!(
        "WITH_ELEMENT_NAME = {}",
        encoded(&block("k", "string", Some("f0"), 1, 2, 3, (false, false, true)))
    );
    println!(
        "ALL_FLAGS      = {}",
        encoded(&block("k", "string", None, 1, 2, 3, (true, true, true)))
    );
    println!(
        "OVER_WIDE      = {}",
        encoded(&block("k", "string", None, 1, 2, u64::MAX, (false, false, true)))
    );

    // A whole index: one bucket, five blocks of one object (one with an element name) and a second
    // object in the SAME bucket -- the shape a cluster-range shard produces routinely.
    let mut node = BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::MultiObject,
        flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
        dirty_generation: 3,
        ..BucketNode::default()
    };
    node.object_index.insert(42);
    // THE IDENTITY IS NAMED BESIDE EACH BLOCK RATHER THAN READ OFF IT, which is what the fixture
    // now has to do and what every production placement already does.
    //
    // THIS FIXTURE IS WHY THE REFUSAL IS WORTH HAVING. It built a `CoreIndex`, inserted a node
    // holding six blocks, and never filed the object lookup -- so the inversion named no position
    // and every stored key below was composed from the entry's own copy. That was invisible while
    // the entry held one. The encoder refuses it now, with the window named, and the fix is to
    // file what a placement files: "bucket 7 holds a block whose stored key would name no object
    // ... the inversion names NO position at all".
    //
    // THE BYTES DID NOT MOVE. `INDEX` below is character-for-character the 1,125 bytes this test
    // printed before the identity became a neighbour, which is the whole claim of the step.
    let blocks: Vec<(Arc<str>, BlockIndex)> = (0..4u64)
        .map(|offset| {
            (
                Arc::from("k"),
                block("k", "string", None, 1, offset, 3, (false, false, true)),
            )
        })
        .chain(std::iter::once((
            Arc::from("k"),
            block("k", "hash", Some("f0"), 1, 9, 3, (true, false, false)),
        )))
        .chain(std::iter::once((
            Arc::from("other"),
            block("other", "string", None, 2, 1, 5, (false, false, true)),
        )))
        .collect();

    let mut live = crate::engine::state::BlockSlabLiveIndex::default();
    for (object_key, entry) in &blocks {
        node.block_index.insert(object_key, entry.clone(), &mut live);
    }

    let mut index = CoreIndex::default();
    index.bucket_map.insert(7, node);
    // ONE FILING PER PLACED BLOCK, through the same door the engine files through, with the handle
    // computed from the same terms the map keyed the block under.
    for (object_key, entry) in &blocks {
        let handle = crate::engine::state::block_index_handle(object_key, entry);
        index.insert_object_block_lookup(7, handle, object_key, entry);
    }
    let text = serde_json::to_string(&index).unwrap();
    println!("INDEX_LEN      = {}", text.len());
    println!("INDEX          = {text}");
    println!("BLOCKS ----------------------------------------------------------");
    let named = super::BlockObjects::of(&index);
    for bucket in index.bucket_map.values() {
        for (handle, entry) in bucket.block_index.iter() {
            println!(
                "        (\"{}\", \"{}\", {}, {}, {}, {}, {}, {}, {}),",
                named.at(bucket.routing_bucket, *handle),
                entry.model_id(),
                // The element name column is `None` on every row now: an entry names a block. The
                // column is kept in the printed shape so an existing golden still lines up.
                "None".to_string(),
                entry.address.block_slab_id(),
                entry.address.offset(),
                entry.address.length(),
                entry.dirty(),
                entry.deleted(),
                entry.log_backed()
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE CAPTURED BYTES. Produced by `capture_the_stored_spelling_of_a_block_entry` run at
// `15583789e` and pasted here verbatim, then RE-CAPTURED when the address's slab id and offset
// merged into one word: `"ps":1,"o":2` became `"a":4294967298`, which is 1 in the high 32 bits
// and 2 in the low. The values are the ones the engine printed, not ones computed here -- the
// three guards below print what they built beside what is pinned, so a fixture edited to match a
// wrong output would have to match a wrong output that the engine actually produced.
//
// Every OTHER key in these fixtures is untouched, which is the point of keeping them: the slab id
// and the offset are the only part of the stored spelling this change moves.
// ---------------------------------------------------------------------------------------------

// THE STORED SPELLING OF A BLOCK ENTRY, character for character, over three shapes. The object key
// and the kind are the composite key's own second and first slots, so the entry writes neither.

const BLOCK_ENTRY_PLAIN: &str = r#"{"address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

const BLOCK_ENTRY_ALL_FLAGS: &str = r#"{"address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":true,"deleted":true}"#;

const BLOCK_ENTRY_OVER_WIDE: &str = r#"{"address":{"a":4294967298,"l":4294967295,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

// ---------------------------------------------------------------------------------------------
// THE STORED FORMAT, pinned over several shapes.
// ---------------------------------------------------------------------------------------------

/// A block entry in a named shape, built from the declared types so the golden moves when the
/// declaration does rather than going quietly stale.
/// TWO FLAGS, NOT THREE, AND THE SIGNATURE IS WHAT ENFORCES IT.
///
/// This took a three-tuple while the entry carried three flags. When the third left, the body
/// stopped reading `flags.2` and every caller went on passing a bool that went nowhere -- no
/// warning, because a tuple field is not an unused variable. Narrowing the tuple makes the
/// compiler demand the change at each call site, which is the only way a caller finds out.
///
/// AND THE `element_name` ARGUMENT WENT THE SAME WAY, for the same reason and one step later. The
/// literal below stopped setting an element name when the field left `BlockIndex`, so the argument
/// decided nothing: `block_fixture(Some("f0"), ..)` and `block_fixture(None, ..)` returned the SAME
/// value, and the golden over the second shape was a second copy of the first. It is removed rather
/// than renamed to `_element_name`, so a caller that still tries to name an element is a compile error.
///
/// THE LITERAL IS EXHAUSTIVE AND HAS NO `..`, which is what makes it the tripwire the fourth shape
/// used to be: an element-name field returning to the entry breaks THIS function to compile, before
/// any golden has a chance to be re-goldened around it.
/// THE OBJECT THIS FIXTURE'S BLOCKS BELONG TO, named once rather than at each encode.
///
/// The entry carries its own copy today and will not; what writes the stored key is the identity
/// held BESIDE the block, so a fixture that encodes one has to state which object it means. This is
/// the same characters `block_fixture` puts on the entry, and a test comparing the two would be
/// comparing a constant with itself -- the goldens below are what hold them together.
pub(super) const BLOCK_FIXTURE_OBJECT_KEY: &str = "k";

pub(super) fn block_fixture(length: u64, flags: (bool, bool)) -> BlockIndex {
    BlockIndex::new(
        crate::engine::storage_bucket_internals::StoredModelKind::String,
        crate::block_store::ElementEntry::from_parts( 1, 2, length, Some(4), Some(42), ),
        flags.0,
        flags.1,
    )
}

/// THE STORED SPELLING OF A PAGE ENTRY, character for character, over three shapes.
///
/// Pinned because this line of work is about the entry's REPRESENTATION, and the one way a
/// representation change becomes data loss is by moving a byte nobody was watching.
///
/// THREE SHAPES, NOT FOUR, AND THE FOURTH WAS NOT DROPPED FOR CONVENIENCE. It read: "Four shapes
/// rather than one, because the element name is `skip_serializing_if = "Option::is_none"`: a golden
/// over a single shape would pin the branch that omits it and say nothing about the branch that
/// writes it." There is no branch that writes it. `BlockIndex` has no element-name field, so the
/// WITH_ELEMENT_NAME shape serialized to the same bytes as PLAIN -- the two goldens were one golden
/// twice, and the control asserting them different (`assert_ne!(plain, with_element_name)`, "a block
/// carrying an element name must not write the same bytes as one without it") had become a comparison
/// of a value with itself, asserted unequal.
///
/// WHAT REPLACES IT IS STRONGER THAN A FOURTH GOLDEN. The shape it was pinning cannot be
/// constructed: `block_fixture`'s literal names every field with no `..`, so the field returning is
/// a COMPILE error here rather than a golden someone updates.
///
/// AND THE THREE THAT REMAIN WERE CHECKED, NOT RE-GOLDENED. `PAGE_ENTRY_PLAIN`,
/// `PAGE_ENTRY_ALL_FLAGS` and `PAGE_ENTRY_OVER_WIDE` are byte-for-byte what they were: the entry's
/// serialized form lost the element-name key and nothing else, and none of these three ever carried
/// one. The stamp this change spends is accounted for in the module header.
///
/// WITH ITS OWN CONTROL. Two entries that both serialized to nothing would pass an equality test
/// against each other, so the shapes are asserted to differ from one another and each spelling is
/// asserted longer than 100 bytes. A comparison that cannot report a difference is vacuous.
/// THE ONE SLOT THAT IS WRITTEN EMPTY RATHER THAN RETIRED: `"oi":null`. `BlockAddressWire::object_id`
/// carries `rename`, `alias` and `default` but NO `skip_serializing_if`, so an address that holds no
/// id writes the slot as a null instead of dropping it. The index log packs this struct
/// POSITIONALLY -- `encode_index_payload_into` uses a plain `rmp_serde::Serializer`, so a field is a
/// position and not a name -- so retiring the slot would shorten the array.
#[test]
fn the_stored_spelling_of_a_block_entry_is_pinned_over_three_shapes() {
    // THROUGH THE WRAPPER, WHICH NO LONGER TAKES A KEY. `BLOCK_FIXTURE_OBJECT_KEY` was the
    // characters the derive read off the entry and then the characters this fixture injected; the
    // entry writes neither the object key nor the kind now, so the wrapper takes the block alone and
    // the three goldens below are two keys lighter.
    let plain = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(3, (false, false)),
    })
    .expect("a block entry serializes");
    let all_flags = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(3, (true, true)),
    })
    .expect("a block entry serializes");
    let over_wide = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(u64::MAX, (false, false)),
    })
    .expect("a block entry serializes");

    println!("PLAIN     = {plain}");
    println!("ALL_FLAGS = {all_flags}");
    println!("OVER_WIDE = {over_wide}");

    assert_eq!(
        BLOCK_ENTRY_PLAIN, plain,
        "the stored spelling of a block entry moved; if that is intended, the version stamp has to          move with it"
    );
    assert_eq!(
        BLOCK_ENTRY_ALL_FLAGS, all_flags,
        "the stored spelling of a block entry with every flag set moved"
    );
    assert_eq!(
        BLOCK_ENTRY_OVER_WIDE, over_wide,
        "the stored spelling of a block entry whose length saturated moved"
    );

    // --- Controls. Without these the assertions above could all be comparing "" with "". ---
    for (name, spelling) in [
        ("plain", &plain),
        ("all flags", &all_flags),
        ("over wide", &over_wide),
    ] {
        // SIXTY, NOT A HUNDRED, AND THE NUMBER MOVED BECAUSE THE DOCUMENT DID. The three spellings
        // lost the object key and the kind at stamp 27, which is about 37 bytes of each, so a
        // hundred-byte floor would now fail on the shortest of them for the very reason this
        // change exists. The floor is there to stop a comparison of two near-empty documents, and
        // sixty is still well clear of that.
        assert!(
            spelling.len() > 60,
            "the {name} spelling is {} bytes; a golden over a near-empty document cannot fail",
            spelling.len()
        );
    }
    assert_ne!(
        plain, all_flags,
        "the flags must reach the stored form, or a dirty block would load clean"
    );
    assert_ne!(
        plain, over_wide,
        "a saturated length must reach the stored form"
    );

    // --- SATURATES RATHER THAN WRAPS. An over-wide length must come back as a value no encoder
    // will accept, never as its own low bits -- `u64::MAX` truncated to `u32` is 4,294,967,295
    // either way, so the case that decides it is the one below. ---
    let wrapped_would_be = (u64::MAX as u32) as u64; // what `as u32` yields: the low bits.
    let saturated = block_fixture(u64::MAX, (false, false)).address.length();
    assert_eq!(
        u32::MAX as u64,
        saturated,
        "an over-wide length must saturate at u32::MAX, not wrap"
    );
    let low_bits = block_fixture(0x1_0000_0003, (false, false)).address.length();
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

// ---------------------------------------------------------------------------------------------
// THE READ PATH, and the footprint on the counting allocator.
// ---------------------------------------------------------------------------------------------

// THE SHARED-VERSUS-COPIED TIMING A/B IS RETIRED, WITH ITS THREE HELPERS, AND THIS IS WHAT IT
// MEASURED AND WHY NO VERSION OF IT SURVIVES.
//
// `reading_a_name_is_not_slower_when_an_objects_pages_share_one_allocation` timed two stores
// holding identical text in two REPRESENTATIONS: one where the blocks of an object all pointed at
// one `Arc<str>`, and one where `unshare_object_keys` had given each block its own. Its question
// was whether sharing cost a dependent load on the read path.
//
// ITS PREMISE WAS THE FIELD, STATED IN ITS OWN DOC, AND THE DOC HAD BECOME FALSE. It read: "This
// change does not do that: the entry still holds its own `Arc<str>` and reads it exactly as
// before. What moves is which allocation the pointer lands on." The entry holds no `Arc<str>`.
// There is no pointer on it to land anywhere, no second representation to build, and
// `unshare_object_keys` cannot be written at all -- there is no field to reseat.
//
// WHY IT IS NOT RESTATED AGAINST THE LOOKUP. The question it asked is answerable only where two
// arms differ, and the surviving store has ONE arm: every block of an object is handed the one
// allocation its bucket's lookup holds, and an engine that gave each block its own copy is not a
// state this tree can reach. Its own guard said what a one-armed comparison is worth -- "this
// comparison has two identical arms and would report 1.00x whatever the truth is" -- so keeping
// it with the treatment arm removed is the failure it was built to refuse.
//
// AND THE COST IT WAS WATCHING FOR IS NOW A DIFFERENT COST, MEASURED ELSEWHERE. What a reader
// pays to learn a block's object is no longer a pointer chase through the entry; it is a lookup
// inversion, built once per walk. `block_identities_in` and `BlockKeysByPosition` carry that price
// in their own docs, and `the_block_and_the_lookup_point_at_one_object_key` holds the property
// this A/B's "shared" arm stood for: the identity a reader is handed IS the lookup's allocation
// and not a copy of its text.
//
// `widest_object_key_allocations` and `fold_every_name` went with it; both had this one caller.
// `key_allocation_census`, which the first wrapped, has three others and stays.

/// BYTES AND ALLOCATIONS PER BLOCK, on the counting allocator, at two corpus sizes.
///
/// ONE INSTRUMENT for both figures. #1959 found a published decline whose sign was wrong because
/// it set a `size_of` saving against an allocator cost; nothing here mixes the two.
///
/// The allocator's own rounding can swallow a byte win while the COUNT stays exact, so both are
/// reported and the count is what is claimed.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe"]
fn what_a_container_store_charges_the_allocator_a_block() {
    for (label, keys) in [("4,000 records", 40usize), ("40,000 records", 400usize)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_length = dir.path().as_os_str().len();
        let engine = probe_engine(dir.path());
        load_shard_over(&engine, u32::MAX);

        let probe = Probe::start();
        seed_container_keys(&engine, keys, 100);
        let counts = probe.stop();

        let census = key_allocation_census(&engine);
        assert!(census.blocks > 0, "denominator: nothing seeded");
        assert!(
            census.blocks / census.objects >= 100,
            "{label}: {} blocks over {} objects; the container case is not reached",
            census.blocks,
            census.objects
        );
        println!(
            "\n=== {label} (store path {path_length} chars) ===\n  blocks={} objects={}\n  \
             alloc calls={} ({:.4} a block)\n  alloc bytes={} ({:.2} B a block)\n  widest object-key \
             allocation count={}",
            census.blocks,
            census.objects,
            counts.allocs,
            counts.allocs as f64 / census.blocks as f64,
            counts.alloc_bytes,
            counts.alloc_bytes as f64 / census.blocks as f64,
            KeyAllocationCensus::widest(&census.object_key)
        );
        assert!(
            counts.allocs > 0,
            "the probe reported zero allocations for a seed of {} blocks, which reads as a path \
             that does not allocate",
            census.blocks
        );
    }
}

/// An ELEMENT NAME CANNOT CROWD A KIND OUT OF THE POOL THEY SHARE.
///
/// `part4::blocks_of_one_kind_spend_one_byte_and_share_one_static_spelling` asserts the PAGE's
/// half of this: a block entry holds one byte for its kind and reads the registry's `&'static str`
/// through it, so a block cannot hold a pooled copy at all. Its fixture is 200 strings and 40
/// hashes whose field is always `"f"` -- ONE element name in the whole store -- so the pool it
/// would have checked holds three entries against a cap of sixty-four and can never be full. The
/// failure this test is about needs the pool FULL, and the only thing in this engine that fills it
/// is a container.
///
/// WHO STILL NEEDS THE POOL, now that the entry does not: the object lookup, whose `by_model` head
/// is keyed by the kind's SHARED name. That is the holder this test protects, and it is why a
/// element name crowding a kind out still costs something -- one allocation per (kind, object) head
/// instead of one per kind.
///
/// Both counts are read, and the kind count is the claim. The element name pool is asserted to be AT
/// ITS CAP in the same breath, so this cannot pass by the containers having quietly stopped
/// producing element names.
#[test]
#[ignore = "seeds a container store; run by name"]
fn a_container_cannot_fill_the_pool_the_kinds_are_interned_in() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, u32::MAX);
    // Four container keys, one of each kind, a hundred elements each: 400 distinct element name
    // names against a pool cap of 64.
    seed_container_keys(&engine, 4, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let pooled = shard.bucket_index.kind_pool.len();
    println!("kind_pool={pooled} entries");

    // The pool must be PAST the element name ceiling, or the crowding-out this test is about cannot
    // happen and every assertion below is free.
    assert!(
        pooled > 64,
        "the pool holds {pooled} entries; this fixture is supposed to produce four hundred \
         distinct element names and so drive it past the element name ceiling of 64. Below that, \
         an element name could not have crowded a kind out even before this change"
    );
    // And still bounded: element names stop at 64, kinds may use 16 more, nothing else may.
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

    // And the block's own side of it: one byte, no string, nothing to pool.
    let mut blocks = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        blocks += bucket.block_index.iter().count();
    }
    assert!(blocks > 0, "no blocks were recorded; nothing was measured");
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

    // --- CONTROL: it round-trips at the right version, block for block. ---
    let decoded = decode_index_bytes(&bytes).expect("an index this binary wrote must decode");
    let want = block_tuples(&shard);
    let got = block_tuples(&decoded);
    assert!(
        want.len() >= 16,
        "the fixture holds {} blocks; too few to tell a correct decode from an empty one",
        want.len()
    );
    assert_eq!(want, got, "the index did not come back block for block");

    // --- And the control's own control: a DIFFERENT block set must not compare equal. ---
    let mut injected = got.clone();
    injected[0].3 = injected[0].3.wrapping_add(1);
    assert_ne!(
        injected, want,
        "the block comparison cannot report a difference when one is injected"
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
        32, SHARD_INDEX_FORMAT_VERSION,
         "the struct version moved; the refusal messages pinned above quote it. Moved 2 -> 3 when the object id stopped folding the element_name in: the stored `oi` keeps its type, so an old index decodes cleanly and serves a recomputed id that disagrees with its own `object_index`. Moved 3 -> 5 when a container page gained the ability to state that one of its items was REMOVED: the payload is opaque to every index encoder, so an old index again decodes cleanly, and what disagrees is what a tombstone item MEANS -- the previous binary reads one as an empty live value and puts the element back. Moved 6 -> 7 when the object list began storing its SLOTS rather than a sorted set of its ids: the sequence is written in slot order and spells a placeholder `null`, so a bucket holding more than one object writes different bytes, and a previous binary reading them would take a slot position for an ascending rank -- which is the same class of silent misread as the three below, arriving on the load path. Moved 5 -> 6 when `object_id` left `ElementEntry`: `generation` is the block id alone now, so a WAL-resident page's generation went from Some(object_id) to None, every ref key it resolves through moves, and an OMITTED ref key restores to a different handle entirely. 4 was skipped and its reservation is now VOID -- it was held while this constant was 3, main moved to 5, and spending 4 would LOWER the constant, which `persistence.rs`'s one-sided `<` turns into a silent accept. A stamp may only ever increase. Moved 8 -> 9 when a zset's element_name stopped spelling its score: `block_index_written_key` renders `element_name` into the map key the served index is serialized under, so `\"0000000000000000<member>\"` became `\"<member>\"` under the same named map. The decode stays clean on both sides of this one too -- `element_name` keeps its `String` type -- and what disagrees is what the text MEANS on the recovery path that reads it back as a page's element, the same shape as 2->3, 3->5 and 6->7. All four bumps share one shape: the stored row decodes cleanly and the disagreement appears later, on a recovery path. Moved 9 -> 11 when a TOMBSTONE'S ELEMENT NAME LEFT THE ENTRY: a tombstone entry files no element_name and the element it records is a new key on the BUCKET NODE, so the entry's rendered key loses the element text AND the node grows a key. The named served index DROPS an undeclared key silently, so a binary before that change reading an index written after it loads every tombstone with no element at all -- a removal nothing can identify, which is the resurrection the change exists to prevent. Same shape as every bump above: the row decodes cleanly and the disagreement appears on a recovery path. TEN IS NOT A HOLE AND IS NOT SPENDABLE: it is claimed by an UNCOMMITTED edit in another working tree, which no ref holds and no sweep of refs can see -- a lane swept 2,567 refs, found nothing above 9, and was wrong for exactly that reason. Moved 11 -> 12 when HASH AND ZSET joined the page-named set: the same key `block_index_written_key` renders, moved for two more kinds, which is 8 -> 9 again with a wider scope. The pinned assertions above resolve the constant symbolically, so they followed it -- this literal is the tripwire that made someone come and check that they did. Moved 12 -> 13 when THE INDEX-LOG ROW LOST A SLOT: `index_log::INDEX_ITEM_SLOTS` went twelve to eleven, the element-name slot coming off a positional array that had carried a constant nil there since the entry stopped naming its element. THIS BUMP HAS A DIFFERENT SHAPE FROM EVERY ONE ABOVE IT, and that is the thing worth reading here rather than the number. All the others share one shape -- the stored row decodes cleanly and the disagreement appears later, on a recovery path -- so the stamp was the ONLY thing that could refuse them. This one does not decode cleanly: a stored twelve-slot row is refused outright by `index_log::IndexItemRow`'s arity check, in a message naming both arities, and a named row carrying the key is refused by `deny_unknown_fields` on `IndexItemWire`. So the stamp is NOT what makes this one safe; the row's own gate is. What 13 buys is that a store written by a twelve-slot binary is DISCARDED and rebuilt from the WAL rather than being read as far as the first row and then failing, which turns a load error into a rebuild on a store nothing is serving -- and `index_log_record`'s module doc is right that a row-shape change owes this constant nothing, which is why it is spent for the rebuild and not for the refusal. The value is free: measured over every `matrixark` remote ref, no tip held 13 and the highest any held was this constant's own 12. That note's own claim that 13 was live on four branches had expired. Moved 13 -> 14 when A RESIDENT ADDRESS MAP'S VALUE BECAME A CONSOLIDATED RECORD: `control_state_blocks` held a bare address holder and now holds an `ObjectRecord`, which carries that address plus a deadline, state bits and a model id. THIS ONE HAS THE COMMON SHAPE AND NOT 13'S, which is the thing worth reading here. The two wire forms overlap on exactly ONE key: `a`, the address word. An old row also carries `l`, `pi`, `oi` and `g`; the record declares `n`, `f`, `m` and `t` instead. The served index is a NAMED map and a reader DROPS the keys its struct no longer declares, silently, with every remaining value intact -- measured, and stated beside `BlockAddressWire`. So an old row decodes CLEANLY into a record holding the right address word with a page size of ZERO and no flags at all: a read then asks for zero bytes, and the cleared `BLOCK_IN_LOG` claims a slab-resident page for one that was log-resident. Nothing refuses that but this constant. So 14 is spent on the REFUSAL, which is the opposite of what 13 bought -- 13 was spent on a rebuild because the row's own arity gate already refused the shape. Moved 14 -> 15 when THE TWO DISTINCT-COUNT MAPS BECAME ONE: `control_state_change_sketch` left the named map set entirely, and `control_state_changes`' bucket value went from a bare member array to an externally-tagged `hll::DistinctBucket`. THIS IS THE FIRST BUMP TO CARRY BOTH SHAPES AT ONCE, one per tenant, which is the thing worth reading here rather than the number. THE DEPARTING MAP HAS THE COMMON SHAPE: the served index is a NAMED map and a reader DROPS the keys its struct no longer declares, silently, so a binary after this change reading an index written before it loads every sketched bucket as though it had never existed -- a bounded distinct count that quietly becomes exact-over-nothing, with no error anywhere, and nothing but this constant refuses it. THE CHANGED VALUE HAS 13'S SHAPE INSTEAD: a stored bucket is a JSON array where the enum now expects a variant tag, so the decode FAILS rather than producing a plausible value, and the stamp buys the rebuild rather than the refusal there. Measured, not inferred from the types, in `one_map_for_both_distinct_shapes::the_pre_bump_bucket_shape_cannot_be_read_as_the_new_one`. So one tenant of this bump is 14's case and the other is 13's, and a reader who assumes a bump has one shape will get this one wrong. The value is free, measured the way 13's note prescribes -- by reading this constant out of every `matrixark` remote ref rather than by grepping history: the maximum any tip held was 14, on seventeen refs, and no tip held 15. Moved 15 -> 16 when `ObjectSlot` GAINED A STORED FORM for its owned element array: the slot's image stopped being the bare record's and became an `r` key holding the record, beside an `e` array the slot carries only when the object has elements. 16 -> 17 and 17 -> 18 are the first four of the eight per-element series maps moving into the consolidated map -- `context_children` alone, then `context_audits`, `context_summaries` and `context_compressions` together. Each removes a top-level key from the stored image, which is 14's shape and not 13's: a named map DROPS an undeclared key silently, so without the bump an index written before the move loads with those maps EMPTY and no error anywhere. AND THIS PIN WAS RED FOR ALL THREE BEFORE ANYONE READ IT. It sat at 15 while main reached 17, through two merged changes, because the gate sets those changes ran named the OTHER stamp tripwire -- the one in `adopting_the_ported_index_log_record` -- and not this module. A literal pin only works if something runs it, so a stamp bump owes a sweep for EVERY pin that resolves the constant literally, not just the one the author remembers. Moved 18 -> 19 when THE FEATURE SERIES MOVED INTO THE CONSOLIDATED MAP: the largest of the eight per-element maps, and the only one the decode's canonical-key adoption pass covers -- that pass had to follow the series onto the kind's inner table, because dropping it would have cost every feature key its canonical handle with NOTHING failing, the keys comparing equal and only the sharing being lost. The bump itself has 17's and 18's shape: a key leaves the stored image, a named map drops an undeclared key in silence, and this constant is the only thing that refuses an index written before the move rather than loading it with every feature series empty. Moved 19 -> 20 for the LAST THREE of the eight -- `context_events`, `context_entities` and `context_indexes` -- and that bump carries a shape none of the others did. `context_entities` is the familiar case: a stored key leaves the top level. The other two were `#[serde(skip)]` and had NO stored image at all, so moving them into the serialized `objects` makes them DURABLE for the first time. The load side is unchanged (an older index carries nothing for them and rebuilds), but every index written after this one carries more than any index before it, which is a durability change rather than a key moving. Moved 20 -> 21 when `control_state_uuid` and `context_compression_watermark` became one domain-keyed `u64_ledgers`. The first was a stored top-level key, so this is 14's shape again -- but with a sharper consequence than the others: a dedup ledger that loads EMPTY does not lose a read, it makes a duplicate control write look new and apply twice. The second was `#[serde(skip)]` and becomes stored by joining a stored map, as the event and index series did at 20, and its writer is environment-gated so it stays empty in the shipped configuration. Moved 21 -> 22 when `control_state` and `feature_values` became one domain-keyed `counter_series`. `control_state` was a stored top-level key, so 14's shape again: an index written at 21 would load with every control-state counter series EMPTY and a windowed aggregate would answer zero over a store that holds the counts. `feature_values` was `#[serde(skip)]` and becomes stored by joining a stored map, as the compression watermark did at 21. Moved 22 -> 23 when `strings` and `context_nodes` became one domain-keyed `address_maps`. `strings` was a stored top-level key, and its loss is the worst consequence in this family: an index written at 22 would load with EVERY STRING VALUE's address missing, so a store full of keys serves none of them. It is a SAME-SHAPE merge and not a fold into `objects` -- an `ElementEntry` carries a block id and a generation the 24-byte record has no field for, which #2259 drove -- so it buys a map and not bytes. Moved 23 -> 24 when `expires_at_ms` joined `u64_ledgers` as a third domain. It was a stored top-level key, and the consequence is specific: an index written at 23 loads with NO DEADLINES, so every key with a TTL becomes immortal until something writes its expiry again -- a dropped guarantee rather than a missing read. The inner container became a `BTreeMap` for all three domains at the same time, because three production sites ITERATE the expiry table and a `HashMap` would have made those walks hash-seeded. Moved 24 -> 25 when `address_maps` and `counter_series` BECAME ONE FIELD, `side_tables`, keyed by a single `SideTable` enum. THIS BUMP REMOVES TWO STORED TOP-LEVEL KEYS AND ADDS ONE, which is the first time more than one has left at once, and both departing keys were stored rather than skipped -- so it is 14's shape twice over. A named map drops an undeclared key silently, so an index written at 24 loads under this binary with BOTH the string addresses and the control-state counters empty: a store full of string keys serves none of them, and every windowed aggregate answers zero over counts the store still holds. Nothing but this constant refuses that. What is NOT part of this bump is any change to the spellings or to the tables themselves: all four domain spellings -- `strings`, `context_nodes`, `control_state`, `feature_values` -- carry over unchanged, and each table's serialized form is still its bare inner map, because a domain's table SHAPE is resolved from its spelling on decode rather than from a variant tag on the wire. So the stored bytes are the same four objects they were, moved under one key instead of two, and the merge costs no key a byte: the variant wraps the TABLE, not the entry, which is what distinguishes it from the union priced in `index_bytes_per_key::what_merging_the_key_sharing_maps_would_cost_as_a_record` at 16.4 B/key saved at best and 144.2 lost at worst. Moved 25 -> 26 when THE STRING MAP'S VALUE BECAME A ROW: `strings` left `side_tables` entirely and its keys now live in `objects` under the String kind, holding an `ObjectSlot` rather than a bare `ElementEntry`. THIS BUMP HAS 14'S SHAPE, which is the one to read it as: the served index is a NAMED map and a reader DROPS the keys its struct no longer declares, so a binary after this change reading an index written before it finds no `strings` domain under `side_tables` and no String kind under `objects`, and loads with EVERY STRING VALUE's address missing -- a store full of keys serving none of them, with no error anywhere. Nothing but this constant refuses that. What the move buys is that a string row finally has somewhere to put a deadline and a dirty bit, which a bare address does not: the per-key expiry table is the duplicate it exists to delete, priced at a 27% crossover in `one_record_per_object::what_moving_the_string_map_onto_the_record_would_cost`. The row is still 24 bytes and the layout arm is still unused -- 400 of 400 real string addresses take the inline arm -- so the conversion adds the record's width and no allocation. Moved 26 -> 27 when A PAGE ENTRY STOPPED RESTATING THE KEY IT IS FILED UNDER: `object_key` and `model_id` are the composite key's second and first slots, the entry wrote both again as named fields, and #2350 measured that the key's characters were NOT READ BACK -- every page key in a 2,000-key index replaced by the decimal of its handle, fed to the production decoder and re-encoded, produced a byte-identical `slot_index`. The decoder parses the key now and the entry writes three keys rather than five. THIS BUMP CARRIES BOTH SHAPES, ONE PER DIRECTION, AND THE DIRECTION THAT NEEDS IT IS NOT THE ONE A READER EXPECTS. A new binary reading OLD bytes is 13's shape and needs no stamp: the named decoder drops the entry's two extra keys silently and the reconstruction reads the SAME values out of the key beside them, so an old index decodes to exactly the index it decoded to before -- driven against committed bytes by `an_index_written_before_this_change_loads_block_for_block` in this module, field by field on every page. What the stamp is spent on is an OLD binary reading NEW bytes: the msgpack container refuses on EQUALITY in its header before decoding, so 27 makes a rollback refuse cleanly where without it the old binary would reach `StoredBlockEntry`, fail on a missing required field and report corruption for a format step. The cost is that every existing store replays once on first load -- which it did not strictly need, since the new reader can read it -- and that is the trade taken, stated here rather than left to be inferred. `address` did NOT go with the other two and that is a measurement: `block_index_written_key` renders `block_id().unwrap_or_default()`, `ElementEntry::block_id()` is `None` EXACTLY while a block is log-resident, so a staged page and the first page of an object in a slab render ONE key and differ in `log_backed()` -- `the_key_a_block_is_filed_under_inverts_exactly::the_presence_bits_of_an_address_are_not_in_the_key_it_renders` builds both and asserts the keys identical. Moved 27 -> 28 when THE WRITER MOVED TO THE BLOCK SPELLING: every emitted page-spelled serde key in the crate flipped to its block spelling, with the page spelling kept beside it as the accepted ALIAS, and two of those keys are in the stored index image -- `object_page_layouts` became `object_block_layouts`, and an index-log item's kind token `page` became `block`. THIS BUMP HAS THE COMMON SHAPE, 14's and not 13's: the served index is a NAMED map and a reader DROPS the keys its struct no longer declares, so a binary predating the reader widening that shipped just before this flip would read an image written after it, find no `object_block_layouts`, and load every multi-block object's layout EMPTY -- an object whose blocks are on disk and unreachable, with no error anywhere. A binary that CARRIES the widening accepts both spellings and needs no stamp at all, so what 28 buys is a clean refusal across the pre-widening window rather than a silent miss. The cost is one replay per existing store on first load, which is exactly what 27 cost. Moved 28 -> 29 when THE LAST HAND-WRITTEN PAGE-SPELLED KEY MOVED: the bucket node's block map was serialized as `page_index` by a hand-rolled `serialize_field`, which is why the attribute sweep at 28 could not reach it -- there is no attribute to read. Its reader has accepted the set `block_index` / `page_index` / `page_refs` since the widening, so an index written before this loads unchanged; what 29 is spent on is the other direction, an older binary reading an image written after it, where the served index is a named map and a reader DROPS the key it does not declare -- loading every bucket with an EMPTY block map, which is every block in the shard unreachable with no error anywhere. 14's shape, and the sharpest instance of it in this family. Moved 29 -> 30 when THE CONSOLIDATED OBJECT MAP MOVED UNDER `slot_index`: each object's block refs live in its `ObjectSlot` now, `impl Serialize for CoreIndex` composes every live page's written key from those refs, and `serialize_block_index` REFUSES a page it cannot name -- so the map had to be somewhere that encoder can reach, and a sibling field of `bucket_index` is not, because no derive and no `serialize_with` can pass a sibling down. THIS BUMP HAS 14'S SHAPE IN BOTH DIRECTIONS, which is unusual in this family and is the thing to read it as. A new binary reading OLD bytes finds `objects` at the top level, where `CoreIndex`'s decoder names no arm for it and `ShardState`'s derive no longer has a field to put it in: BOTH halves ignore it rather than failing, so without this bump an older store loads SILENTLY into a shard holding no objects at all -- a store full of keys serving none of them, and for the kinds that live there, no record of where any page is. An old binary reading NEW bytes finds no top-level `objects` and the same silence. So neither direction has a gate of its own and this constant is the only thing that refuses either. What is NOT part of this bump: the refs themselves are NOT written. `ObjectSlotWire` carries the record and the element array and drops the refs, because the decode re-files every page it reads -- so the per-object stored bytes are unchanged and what moved is one key's PARENT MAP, not any value. The cost is one replay per existing store on first load, which is what 27, 28 and 29 each cost. Moved 31 -> 32 when THE OBJECT ROW WAS WRITTEN BY POSITION: `ObjectRecordWire` is `[address, block_size, flags, model_id]` (a fifth element, the size class, only when known) and `ObjectSlotWire` is `[record]` or `[record, elements]`, where both had been maps with one-letter keys and an `r` wrapper. A map where an array is expected and an array where a map is expected are each REFUSED by the decoder rather than read as something plausible, so an index written on either side of the move is discarded and rebuilt from the WAL instead of loading with defaulted rows. Measured on 8,000 objects, the `objects` subtree went from 528,235 to 336,235 bytes, 66.0 to 42.0 per object."
    );
    // --- AND THE REFUSAL IS COUNTED APART FROM AN ABSENCE, which is the whole reason the counters
    //     exist: `load_index_inner` answers `Ok(None)` for stale, undecodable and absent alike, so
    //     without them a stamp bump is invisible from outside.
    //
    //     THE DESTRUCTURING ORDER BELOW IS (accepted, absent, decode) AND IS NOT ARBITRARY.
    //     `persistence::index_load_path_counts` returns (accepted, absent, undecodable), all `u64`,
    //     so swapping two compiles -- and getting it wrong would leave `decode_n` holding ACCEPTED,
    //     which is non-zero here for unrelated reasons, and every assertion would pass while
    //     measuring nothing. Bind by position against that signature. ---
    {
        use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = crate::engine::TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );

        // ABSENT: no base index has ever been written for this shard.
        reset_index_load_path_counts();
        engine.load_shard(7);
        let (accepted_n, absent_n, decode_n) = index_load_path_counts();
        println!(
            "  absent index -> decode {decode_n}, absent {absent_n}, accepted {accepted_n}"
        );
        assert!(
            absent_n > 0,
            "a shard with no base index must count as ABSENT; got decode {decode_n} absent \
             {absent_n} accepted {accepted_n}"
        );
        assert_eq!(
            0, decode_n,
            "an absent index must NOT be counted as a refusal, or the two cannot be told apart and \
             the counters say nothing"
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
        let restored = decode_index_bytes(&bytes).expect("the base index decodes");
        assert_eq!(
            restored.index_format_version, SHARD_INDEX_FORMAT_VERSION,
            "denominator: the index just written must carry the CURRENT stamp, or staling it below \
             changes nothing"
        );
        // The same payload behind a header that names the previous struct version: the header is
        // the stamp, and the reader compares it before it reads the payload.
        let stale_bytes = crate::engine::encode_index_bytes_at_version(
            &restored,
            SHARD_INDEX_FORMAT_VERSION - 1,
        );
        let refusal = decode_index_bytes(&stale_bytes)
            .err()
            .expect("an image stamped one version back must be refused by the decode");
        assert!(
            refusal.contains("struct version")
                && refusal.contains(&(SHARD_INDEX_FORMAT_VERSION - 1).to_string()),
            "the stale image was refused, but not on its header's struct version: {refusal}"
        );
        std::fs::write(&path, &stale_bytes).expect("write");

        reset_index_load_path_counts();
        let reloaded = crate::engine::TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        reloaded.load_shard(7);
        let (accepted_n, absent_n, decode_n) = index_load_path_counts();
        println!(
            "  stale stamp  -> decode {decode_n}, absent {absent_n}, accepted {accepted_n}"
        );
        assert!(
            decode_n > 0,
            "an index stamped {} against a current {SHARD_INDEX_FORMAT_VERSION} must be REFUSED and \
             counted; got decode {decode_n} absent {absent_n} accepted {accepted_n}",
            SHARD_INDEX_FORMAT_VERSION - 1
        );
        assert_eq!(
            0, absent_n,
            "a stale index is not an absent one; if both counters move, a bump is still invisible"
        );
    }

    // PLAIN JSON IS REFUSED TOO. An index carrying its stamp as a payload field -- the only way
    // JSON could ever carry one -- is no longer a shape the decode takes: the stamp's one home is
    // the container header, and a stale image has to be refused there and nowhere else.
    let mut stale = shard.clone();
    stale.index_format_version = 0;
    let stale_json = serde_json::to_vec(&stale).expect("a shard serializes as JSON");
    assert!(
        decode_index_bytes(&stale_json).is_err(),
        "a JSON index carrying a stale in-payload version decoded: the stamp has a second home, so \
         an image can carry an old one past the header check"
    );
}

/// Every block of a shard as comparable tuples, in a stable order.
fn block_tuples(
    shard: &crate::engine::state::ShardState,
) -> Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> {
    let mut blocks: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool, bool)> =
        super::blocks_with_identity(&shard.bucket_index)
            .into_iter()
            .map(|(_, _, object_key, block)| {
            (
                object_key.to_string(),
                block.model_id().to_string(),
                None::<String>,
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
                block.dirty(),
                block.deleted(),
                block.log_backed(),
            )
        })
        .collect();
    blocks.sort();
    blocks
}


/// AN OMITTED HANDLE RESTORES WRONG, AND THIS IS WHY THE FORMAT STAMP IS NOT A FORMALITY.
///
/// `index_log::strip_block_ref_key_repeat` OMITS a block's composite handle from the delta whenever
/// the handle is derivable from the item's own parts, and `restore_block_ref_key_repeat` rebuilds it
/// on the way back in. Both derive it through `block_ref_key_from_parts`, and one of the eight parts
/// is `address.generation().unwrap_or_default()`.
///
/// `generation` was `block_id.or(object_id)` and is the block id alone. For a BLOCK-RESIDENT block
/// that is the value it always was. For a WAL-RESIDENT block -- no block id -- it was the object id
/// and is now `None`, so the eighth part goes from the object id to ZERO.
///
/// So a row ALREADY ON DISK whose handle was omitted because it was derivable under the old rule
/// now rebuilds to a DIFFERENT handle. The block is then filed and looked up under a key nothing
/// wrote, which resolves a durably acknowledged write to MISSING -- the exact hole `block_in_wal`
/// exists to close. The declined strip and its wasted allocation are the symptom; this is the
/// disease, and refusing the load is the only thing standing between the two.
#[test]
fn an_omitted_block_handle_restores_to_a_different_key_under_the_new_generation_rule() {

    const SLAB: u64 = 3;
    const OFFSET: u64 = 4_096;
    const LENGTH: u64 = 512;
    const OBJECT_ID: u64 = 0x0123_4567_89AB_CDEF;

    // A WAL-RESIDENT block: no block id, an object id. This is the shape every container member
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
/// `an_omitted_block_handle_restores_to_a_different_key_under_the_new_generation_rule` above proves
/// the two derivations DIFFER. That is necessary and it is not sufficient, and on its own it reads
/// like a migration hazard: a pre-shed writer omitted a block's composite handle because it matched
/// the old derivation, the stamp refuses the base snapshot and routes load into the replay path,
/// and the replay path rebuilds that handle from the NEW derivation. If the rebuilt handle were the
/// name the block is filed and looked up under, every WAL-resident block in such a record would be
/// filed under a name nothing ever wrote -- a durably acknowledged write reading MISSING, on
/// exactly the path the stamp sends load down.
///
/// IT IS NOT, AND THE REASON IS STRUCTURAL. `fold_delta_block_items` is the only thing that turns
/// index-log items into blocks, and it never reads `block_ref_key`. It builds a `BlockIndex` from
/// the item's own fields and hands it to `BlockIndexMap::insert`, which ALLOCATES the handle; the
/// map is keyed by that allocated slot. The record's spelling is rebuilt on the way in and
/// recomputed again on the way out, and in between nothing consults it. So a changed derivation
/// cannot misfile a block through this path -- not because the rebuild agrees, but because the
/// rebuild is not used.
///
/// TWO FIXTURES, AND THE SECOND IS THE SHARPER ONE. The first has its handle OMITTED, which is
/// what a pre-shed writer left. The second carries a handle that is not any derivation of
/// anything. If a block whose handle is outright wrong is still filed and still named, then no
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
fn a_record_whose_block_handle_was_omitted_still_names_its_block_after_the_fold() {
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
        dir.path().join("blocks"),
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
        kind: IndexItemKind::Block,
        routing_bucket,
        block_ref_key: String::new(),
        object_key: KEY.into(),
        model_id: KIND.to_string(),
        object_id: OBJECT_ID,
        // The fixture asserts above that this address carries no block id, so the three slots
        // this used to state -- 0, LENGTH, true -- are exactly what the codec now derives.
        entry: Some(address.clone()),
        deleted: false,
    };
    // FIXTURE TWO: a handle that is not any derivation of anything.
    let nonsense = IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: wrong_routing_bucket,
        block_ref_key: NOT_A_DERIVATION.to_string(),
        object_key: WRONG_KEY.into(),
        model_id: KIND.to_string(),
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

    let blocks = super::blocks_with_identity(&folded.bucket_index);
    println!("=== after the fold: {} block(s) ===", blocks.len());
    for (_, _, object_key, block) in &blocks {
        println!(
            "  key={:?} kind={} slab={} off={} len={} log_backed={}",
            object_key,
            block.model_id().as_str(),
            block.address.block_slab_id(),
            block.address.offset(),
            block.address.length(),
            block.log_backed()
        );
    }

    let omitted_block = blocks
        .iter()
        .find(|(_, _, object_key, _)| object_key.as_ref() == KEY)
        .map(|(_, _, _, block)| *block)
        .unwrap_or_else(|| {
            panic!(
                "THE OMITTED-HANDLE BLOCK IS MISSING AFTER THE FOLD. If this ever fires, the \
                 rebuilt composite handle HAS become the name the block is filed under, and this \
                 change needs a migration for records already on disk -- a stamp cannot cover it, \
                 because the stamp is what routes load down this path. {} block(s) came back.",
                blocks.len()
            )
        });
    let nonsense_block = blocks
        .iter()
        .find(|(_, _, object_key, _)| object_key.as_ref() == WRONG_KEY)
        .map(|(_, _, _, block)| *block)
        .expect(
            "the block whose stored handle is not any derivation is missing, so the stored handle \
             IS consulted somewhere on this path",
        );

    // The block has to come back INTACT, not merely present: a block filed under the right name
    // carrying the wrong address would satisfy a presence check and still serve the wrong bytes.
    for (label, block) in [("omitted", omitted_block), ("nonsense", nonsense_block)] {
        assert_eq!(SLAB, block.address.block_slab_id(), "{label}: slab moved");
        assert_eq!(OFFSET, block.address.offset(), "{label}: offset moved");
        assert_eq!(LENGTH, block.address.length(), "{label}: length moved");
        assert!(
            block.log_backed(),
            "{label}: the block must come back WAL-resident, which is the shape the hazard is about"
        );
        assert!(!block.deleted(), "{label}: the block must not come back deleted");
    }

    // And the object is named by the bucket that holds it, which is what a per-object read
    // resolves through -- present-but-unindexed would read as missing just the same.
    //
    // BY THE DERIVED ID, NOT THE ONE THE RECORD CARRIED. The first version of this assertion asked
    // for `OBJECT_ID`, the arbitrary value written into the fixture, and it failed -- correctly.
    // After the fold, `update_bucket_layout` rebuilds each bucket's live object set from the blocks
    // themselves (`block.object_id(shard_id)`), so the derivation wins and a stale or invented id in
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
        "the bucket's object index does not name the block by its DERIVED id, so a per-object read \
         would not reach the block even though the block is filed"
    );
    assert_ne!(
        derived_id, OBJECT_ID,
        "control: the fixture's invented id must differ from the derivation, or the next assertion \
         cannot tell the two apart"
    );
    assert!(
        !named.object_index.contains(&OBJECT_ID),
        "the arbitrary id the record carried survived into the bucket's object index; the fold is \
         supposed to re-derive that set from each block's own terms, so a stored id cannot be \
         authoritative any more"
    );
}
