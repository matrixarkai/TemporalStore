// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a live key costs in the shard index, broken down by structure, at two corpus sizes.
//!
//! WHY A CLONE IS THE MEASUREMENT. The sibling probes in `address_footprint` price containers by
//! RSS delta. RSS conflates live data with allocator retention and moves with the box, so a figure
//! read from it cannot be compared against the same figure taken an hour later. The counting
//! allocator answers a different and better question: `Clone` on a structure allocates exactly the
//! structure's own heap, node for node and byte for byte, and nothing else -- so the alloc-bytes
//! charged across one `clone()` IS that structure's deep heap footprint, as a count.
//!
//! It is exact for what it covers and it is honest about what it does not:
//!
//!   * `Arc<str>` clones bump a refcount and allocate nothing, so every shared string is invisible
//!     to the clone. `arc_payload_bytes` walks the distinct `Arc` pointers and adds them back
//!     ONCE each, which is what a shared string actually costs.
//!   * A `Vec` clones to its LENGTH, not its capacity, so spare capacity is not counted. Every
//!     `Vec` in these structures is built to a known small length (`BlockRefs::Many` and
//!     `ComponentList::Many` both start at capacity 2), so the omission is bounded and small.
//!   * `HashMap` and `BTreeMap` clones preserve bucket count and tree shape respectively, so their
//!     nodes ARE counted, including the half of a B-tree leaf that ascending inserts leave empty.
//!
//! `the_clone_probe_sees_a_container_whose_size_is_known` is the positive control: it fails if the
//! probe reports near zero for an allocation whose size is not in question, which is the failure
//! mode that would make every number below read as "this structure is free".
//!
//! NON-VACUITY. Every per-structure figure is printed beside the count of live addresses it holds,
//! and the scale probe asserts each map it reports is POPULATED before it divides by it. A walk
//! over an empty shard reports "0 bytes over 0 keys", which reads exactly like a structure that
//! costs nothing.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::block_store::BlockAddress;
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs, ComponentBlocks, ComponentList,
    ObjectIndex,
};

// Imported as a NAME rather than called through its full path on purpose: the counting-allocator
// gate in `alloc_probe.rs` scans every source line for the probe's fully qualified path and walks
// back to the nearest `#[test]`, so a HELPER spelling that path out would be reported as reading
// the probe outside any test. (This comment learned it the hard way -- written with the path
// quoted, it tripped the guard itself, which is the guard working.) Every test below carries the
// feature gate that guard is actually about.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;
#[cfg(feature = "alloc-probe")]
use std::collections::HashSet;

// ---------------------------------------------------------------------------------------------
// The shapes, drained. No allocator needed here: the discriminant IS the footprint.
// ---------------------------------------------------------------------------------------------

/// Which arm a shape is in, as a name, so a failure says what it collapsed to rather than `false`.
fn object_index_arm(index: &ObjectIndex) -> &'static str {
    match index {
        ObjectIndex::Empty => "Empty",
        ObjectIndex::One(_) => "One",
        ObjectIndex::Many(_) => "Many",
    }
}

fn block_index_map_arm(map: &BlockIndexMap) -> &'static str {
    match map {
        BlockIndexMap::Empty => "Empty",
        BlockIndexMap::One(..) => "One",
        BlockIndexMap::Many(_) => "Many",
    }
}

fn dirty_key_set_arm(set: &crate::engine::state::DirtyKeySet) -> &'static str {
    match set {
        crate::engine::state::DirtyKeySet::Empty => "Empty",
        crate::engine::state::DirtyKeySet::One(_) => "One",
        crate::engine::state::DirtyKeySet::Many(_) => "Many",
    }
}

fn component_list_arm(list: &ComponentList) -> &'static str {
    match list {
        ComponentList::Empty => "Empty",
        ComponentList::One(_) => "One",
        ComponentList::Many(_) => "Many",
    }
}

fn probe_page(object: &str, component: Option<&str>, slot: u64) -> BlockIndex {
    BlockIndex {
        object_key: Arc::from(object),
        model_id: Arc::from("string"),
        component: component.map(Arc::from),
        address: BlockAddress::from_parts(1, slot * 64, 64, Some(slot), Some(slot), Some(7), Some(slot)),
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

fn probe_component(name: Option<&str>, slot: u64) -> ComponentBlocks {
    ComponentBlocks {
        component: name.map(Arc::from),
        refs: BlockRefs::One(BlockLookupRef {
            routing_bucket: 7,
            block_ref_key: slot,
        }),
    }
}

/// THE SHRINK QUESTION, on every path that can shrink.
///
/// A container that grows to `Many` and never collapses is a leak that looks like usage: the memory
/// is genuinely reachable, the length is genuinely right, and nothing but the arm name says a node
/// is being kept to hold one value. Each shape is filled past its inline arm and drained back, and
/// the arm is asserted at EVERY step of the drain, not only at the end -- a collapse that fires
/// only at zero still keeps a node for the whole one-entry tail, which is the state the ordinary
/// bucket sits in for its entire life.
///
/// FAILURES ARE COLLECTED, NOT PANICKED ON. A mutant that breaks the first path must not stop the
/// later paths from running, or one mutation reports one survivor instead of the set.
#[test]
fn every_inline_shape_collapses_back_on_a_fill_and_drain() {
    const FILL: u64 = 8;
    let mut failures: Vec<String> = Vec::new();
    let mut steps = 0usize;

    // --- ObjectIndex: insert to Many, remove back to Empty. ---
    {
        let mut index = ObjectIndex::default();
        for id in 0..FILL {
            index.insert(id);
        }
        if index.len() != FILL as usize {
            failures.push(format!("ObjectIndex filled to {} entries, not {FILL}", index.len()));
        }
        // Drained from the TOP, so the last survivor is id 0 -- not the id a collapse would leave
        // if it simply kept whichever entry it reached first.
        let mut remaining = FILL;
        while remaining > 0 {
            remaining -= 1;
            index.remove(&remaining);
            steps += 1;
            let want = expected_arm(remaining as usize);
            let got = object_index_arm(&index);
            if got != want {
                failures.push(format!("ObjectIndex at {remaining} entries is {got}, expected {want}"));
            }
        }
        if !index.is_empty() {
            failures.push("ObjectIndex did not report empty after a full drain".to_string());
        }
    }

    // --- ObjectIndex: the clear() path. ---
    {
        let mut index = ObjectIndex::default();
        index.extend(0..FILL);
        index.clear();
        steps += 1;
        let got = object_index_arm(&index);
        if got != "Empty" {
            failures.push(format!("ObjectIndex after clear() is {got}, expected Empty"));
        }
    }

    // --- BlockIndexMap: insert to Many, remove back to Empty. ---
    {
        let mut live = crate::engine::state::BlockSlabLiveIndex::default();
        let mut map = BlockIndexMap::default();
        let mut handles: Vec<u64> = Vec::new();
        for slot in 0..FILL {
            let component = format!("c{slot}");
            handles.push(map.insert(probe_page("o", Some(&component), slot), &mut live));
        }
        if map.len() != FILL as usize {
            failures.push(format!("BlockIndexMap filled to {} pages, not {FILL}", map.len()));
        }
        let mut remaining = FILL as usize;
        while remaining > 0 {
            remaining -= 1;
            map.remove(&handles[remaining], &mut live);
            steps += 1;
            let want = expected_arm(remaining);
            let got = block_index_map_arm(&map);
            if got != want {
                failures.push(format!("BlockIndexMap at {remaining} pages is {got}, expected {want}"));
            }
        }
    }

    // --- BlockIndexMap: the retain() path, which is the OTHER way pages leave a bucket. ---
    // One page at a time, so the one-entry tail is visited exactly as the removal drain visits it.
    {
        let mut live = crate::engine::state::BlockSlabLiveIndex::default();
        let mut map = BlockIndexMap::default();
        let mut handles: Vec<u64> = Vec::new();
        for slot in 0..FILL {
            let component = format!("c{slot}");
            handles.push(map.insert(probe_page("o", Some(&component), slot), &mut live));
        }
        let mut remaining = FILL as usize;
        while remaining > 0 {
            remaining -= 1;
            let doomed = handles[remaining];
            map.retain(&mut live, |handle, _page| *handle != doomed);
            steps += 1;
            let want = expected_arm(remaining);
            let got = block_index_map_arm(&map);
            if got != want {
                failures.push(format!(
                    "BlockIndexMap after a retain leaving {remaining} pages is {got}, expected {want}"
                ));
            }
        }
    }

    // --- ComponentList: insert to Many, remove back to Empty from the tail. ---
    {
        let mut list = ComponentList::default();
        for slot in 0..FILL {
            let at = list.len();
            let name = format!("c{slot}");
            list.insert(at, probe_component(Some(&name), slot));
        }
        if list.len() != FILL as usize {
            failures.push(format!("ComponentList filled to {} entries, not {FILL}", list.len()));
        }
        let mut remaining = FILL as usize;
        while remaining > 0 {
            remaining -= 1;
            list.remove(remaining);
            steps += 1;
            let want = expected_arm(remaining);
            let got = component_list_arm(&list);
            if got != want {
                failures.push(format!("ComponentList at {remaining} entries is {got}, expected {want}"));
            }
        }
    }

    // --- ComponentList: removal from the FRONT, which a tail-only collapse gets wrong. ---
    {
        let mut list = ComponentList::default();
        for slot in 0..FILL {
            let at = list.len();
            let name = format!("c{slot}");
            list.insert(at, probe_component(Some(&name), slot));
        }
        let mut remaining = FILL as usize;
        while remaining > 0 {
            remaining -= 1;
            list.remove(0);
            steps += 1;
            let want = expected_arm(remaining);
            let got = component_list_arm(&list);
            if got != want {
                failures.push(format!(
                    "ComponentList draining from the front at {remaining} entries is {got}, expected {want}"
                ));
            }
            // The collapse must keep the RIGHT entry, not merely an entry: draining from the front
            // leaves the highest-numbered component behind.
            if remaining == 1 {
                let kept = list[0].component.as_deref().map(|s| s.to_string());
                let want_kept = Some(format!("c{}", FILL - 1));
                if kept != want_kept {
                    failures.push(format!("ComponentList collapsed to {kept:?}, expected {want_kept:?}"));
                }
            }
        }
    }

    // --- DirtyKeySet: the same shape, on the index that marks objects dirty. ---
    // Drained through `take`, which is the path `DirtyObjectIndex::insert` uses to MOVE a key
    // between buckets -- so the collapse has to happen there and not only on `remove`.
    {
        let mut set = crate::engine::state::DirtyKeySet::default();
        for slot in 0..FILL {
            set.insert(Arc::from(format!("k{slot}").as_str()));
        }
        if set.len() != FILL as usize {
            failures.push(format!("DirtyKeySet filled to {} keys, not {FILL}", set.len()));
        }
        let mut remaining = FILL as usize;
        while remaining > 0 {
            remaining -= 1;
            let key = format!("k{remaining}");
            let taken = set.take(&key);
            if taken.as_deref() != Some(key.as_str()) {
                failures.push(format!("DirtyKeySet::take({key}) handed back {taken:?}"));
            }
            steps += 1;
            let want = expected_arm(remaining);
            let got = dirty_key_set_arm(&set);
            if got != want {
                failures.push(format!("DirtyKeySet at {remaining} keys is {got}, expected {want}"));
            }
        }
    }

    // Denominator. Six paths drain `FILL` entries one at a time and the `clear()` path contributes
    // one state, so a complete run visits exactly 6 * FILL + 1 of them. A rewrite that stopped
    // filling would satisfy every assertion above by never making one.
    const DRAIN_STATES: usize = 6 * FILL as usize + 1;
    assert_eq!(
        DRAIN_STATES, steps,
        "the drain visited {steps} states across six paths of {FILL} entries, expected \
         {DRAIN_STATES}; it is not exercising what it claims to"
    );
    assert!(
        failures.is_empty(),
        "{} of the inline shapes failed to collapse on shrink ({steps} drain states visited):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The arm a shape holding `len` entries must be in.
fn expected_arm(len: usize) -> &'static str {
    match len {
        0 => "Empty",
        1 => "One",
        _ => "Many",
    }
}

/// `BlockRefs` is the one shape with NO shrink path, and that is a property of its surface rather
/// than an oversight -- so it is pinned here rather than left to be rediscovered.
///
/// `BlockRefs::insert` grows `One` into `Many`. Nothing anywhere removes a SINGLE ref: a ref leaves
/// only when its whole `ComponentBlocks` leaves, through `ComponentList::remove`, which drops the
/// `BlockRefs` entire. So a `Many` that could collapse never arises, and adding a per-ref removal
/// without adding the collapse beside it would create one.
///
/// This fails if a per-ref removal appears: it asserts the surface, and the surface is what makes
/// the missing collapse safe.
#[test]
fn a_ref_list_has_no_partial_removal_so_it_has_nothing_to_collapse_from() {
    let mut refs = BlockRefs::One(BlockLookupRef {
        routing_bucket: 7,
        block_ref_key: 2,
    });
    assert_eq!(1, refs.len(), "the inline arm holds one ref");
    let grew = refs.insert(BlockLookupRef {
        routing_bucket: 7,
        block_ref_key: 1,
    });
    assert!(grew, "a second distinct ref must be added");
    assert!(matches!(refs, BlockRefs::Many(_)), "a second ref spills the inline arm");
    assert_eq!(2, refs.len(), "and both refs are held");
    // Sorted, which is what would make a collapse well defined if one were ever added: the survivor
    // would have to be picked by position, and the position is meaningful.
    assert_eq!(
        vec![1u64, 2u64],
        refs.iter().map(|r| r.block_ref_key).collect::<Vec<_>>(),
        "refs stay sorted across the spill"
    );

    // Round-tripping through the wire is the ONLY path back to the inline arm, and it is the path a
    // reload takes: one ref comes back inline, so a reloaded index is not wider than the one that
    // wrote it.
    let single: BlockRefs = vec![BlockLookupRef {
        routing_bucket: 7,
        block_ref_key: 2,
    }]
    .into();
    assert!(
        matches!(single, BlockRefs::One(_)),
        "a one-element list must load back into the inline arm"
    );
    let pair: BlockRefs = vec![
        BlockLookupRef {
            routing_bucket: 7,
            block_ref_key: 1,
        },
        BlockLookupRef {
            routing_bucket: 7,
            block_ref_key: 2,
        },
    ]
    .into();
    assert!(
        matches!(pair, BlockRefs::Many(_)),
        "two refs must stay spilled -- a control for the arm above, which would otherwise pass by \
         always answering One"
    );
}

/// The dirty index, on a REAL shard rather than on a hand-built set.
///
/// The fill-and-drain above proves `DirtyKeySet` can collapse. This proves the live write path
/// actually leaves it collapsed, which is a different claim and the one that decides what the
/// index costs: a bucket holding one dirty key reports `len() == 1` from either arm, so length
/// cannot tell the 16-byte holding from the 192-byte one.
///
/// Cheap and NOT ignored -- it seeds a few hundred keys and reads the arms. The scale probe prices
/// the same property in bytes; this one is what runs in the ordinary gate.
#[test]
fn every_dirty_bucket_holds_its_one_key_inline() {
    const KEYS: usize = 400;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);
    let commands = (0..KEYS)
        .map(|i| Command::StringSet {
            key: format!("dirty{i}"),
            value: vec![b'v'; 16],
        })
        .collect::<Vec<_>>();
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "seed must ack: {:?}", response.status);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let dirty = shard.dirty_objects.len();
    let buckets = shard.dirty_objects.bucket_ids().count();
    let (empty, one, many) = shard.dirty_objects.bucket_arms();

    // Denominator first. A shard that left nothing dirty satisfies every assertion below by
    // holding no buckets at all, and reads exactly like a shard holding all of them inline.
    assert_eq!(
        KEYS, dirty,
        "denominator: the write path must have marked every key dirty"
    );
    assert!(
        buckets > 0,
        "denominator: {dirty} dirty keys must live in at least one bucket"
    );
    assert_eq!(
        buckets,
        empty + one + many,
        "the arm census must cover every dirty bucket"
    );
    println!(
        "{dirty} dirty keys over {buckets} buckets: Empty {empty}, One {one}, Many {many} \
         ({:.1}% inline)",
        100.0 * one as f64 / buckets as f64
    );
    assert_eq!(
        0, empty,
        "a dirty bucket whose set emptied must be dropped from the map, not kept as an Empty arm"
    );
    // Not "most": EVERY one. The write path routes one key to one bucket, so a bucket in `Many`
    // means either the routing changed or the collapse stopped firing, and both are worth a
    // failure rather than a percentage.
    assert_eq!(
        buckets, one,
        "{many} of {buckets} dirty buckets hold a B-tree node to carry a single key; each costs \
         192 live bytes where the inline arm costs 16"
    );
}

// ---------------------------------------------------------------------------------------------
// Occupancy: how many entries a container in each per-kind map actually holds.
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "alloc-probe")]
#[derive(Default, Debug)]
struct Occupancy {
    containers: usize,
    entries: usize,
    histogram: [usize; 9],
    largest: usize,
}

#[cfg(feature = "alloc-probe")]
const OCCUPANCY_BANDS: [&str; 9] = ["0", "1", "2", "3-4", "5-8", "9-16", "17-64", "65-256", "257+"];

#[cfg(feature = "alloc-probe")]
fn occupancy_band(len: usize) -> usize {
    match len {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5..=8 => 4,
        9..=16 => 5,
        17..=64 => 6,
        65..=256 => 7,
        _ => 8,
    }
}

#[cfg(feature = "alloc-probe")]
impl Occupancy {
    fn observe(&mut self, len: usize) {
        self.containers += 1;
        self.entries += len;
        self.histogram[occupancy_band(len)] += 1;
        self.largest = self.largest.max(len);
    }

    fn mean(&self) -> f64 {
        if self.containers == 0 {
            0.0
        } else {
            self.entries as f64 / self.containers as f64
        }
    }

    fn report(&self, label: &str) {
        println!(
            "    {label:<24} {:>7} containers, {:>8} entries, mean {:>8.2}, largest {:>6}",
            self.containers,
            self.entries,
            self.mean(),
            self.largest
        );
        if self.containers == 0 {
            println!("      (empty -- nothing to distribute, and nothing this shard pays for)");
            return;
        }
        let mut line = String::new();
        for (band, count) in OCCUPANCY_BANDS.iter().zip(self.histogram.iter()) {
            if *count == 0 {
                continue;
            }
            line.push_str(&format!(
                "  {band}: {count} ({:.1}%)",
                100.0 * *count as f64 / self.containers as f64
            ));
        }
        println!("     {line}");
    }
}

#[cfg(feature = "alloc-probe")]
fn series_occupancy(map: &std::collections::HashMap<String, BTreeMap<u64, BlockAddress>>) -> Occupancy {
    let mut occ = Occupancy::default();
    for series in map.values() {
        occ.observe(series.len());
    }
    occ
}

// ---------------------------------------------------------------------------------------------
// Deep heap footprint, by clone, under the counting allocator.
// ---------------------------------------------------------------------------------------------

/// Bytes the heap gives up to hold a deep copy of `value` -- which is what `value` itself holds.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}

/// An `Arc<str>`'s allocation: two counter words and the bytes, rounded to pointer alignment.
///
/// Identity is the DATA pointer, which is what makes the same figure comparable across two
/// different holders: an `Arc<str>` reached through the bucket index and the same text reached
/// through the dirty index are the same allocation if and only if this matches.
#[cfg(feature = "alloc-probe")]
fn arc_str_bytes(seen: &mut HashSet<usize>, value: &str) -> u64 {
    if !seen.insert(value.as_ptr() as usize) {
        return 0;
    }
    let raw = 16 + value.len();
    ((raw + 7) / 8 * 8) as u64
}

/// Every distinct shared string the bucket index and the object lookup hold, counted ONCE.
///
/// A clone cannot see these -- `Arc::clone` bumps a refcount. They are resident bytes all the same,
/// and the whole point of sharing is that there are fewer of them than there are holders, so
/// counting one per holder would be as wrong as counting none.
#[cfg(feature = "alloc-probe")]
fn arc_payload_bytes(bucket_index: &crate::engine::state::CoreIndex) -> (u64, usize) {
    let mut seen: HashSet<usize> = HashSet::new();
    let bytes = walk_bucket_index_strings(bucket_index, &mut seen);
    (bytes, seen.len())
}

#[cfg(feature = "alloc-probe")]
fn walk_bucket_index_strings(
    bucket_index: &crate::engine::state::CoreIndex,
    seen: &mut HashSet<usize>,
) -> u64 {
    let mut bytes = 0u64;
    for bucket in bucket_index.bucket_map.values() {
        for (_handle, page) in bucket.block_index.iter() {
            bytes += arc_str_bytes(seen, &page.object_key);
            bytes += arc_str_bytes(seen, &page.model_id);
            if let Some(component) = page.component.as_ref() {
                bytes += arc_str_bytes(seen, component);
            }
        }
    }
    for (model_id, object_key, refs) in bucket_index.object_block_lookup.iter() {
        bytes += arc_str_bytes(seen, model_id);
        bytes += arc_str_bytes(seen, object_key);
        for component in refs.by_component.iter() {
            if let Some(name) = component.component.as_ref() {
                bytes += arc_str_bytes(seen, name);
            }
        }
    }
    bytes
}

/// The dirty index's own key text, and HOW MUCH OF IT IS THE SAME ALLOCATION the bucket index
/// already holds.
///
/// `DirtyObjectIndex::insert` builds `Arc::from(object_key)` from the `&str` it is handed. Its two
/// inner maps share that one allocation with each other -- that sharing is documented and is why
/// the second index is cheap -- but nothing hands it the `Arc<str>` the page index already holds
/// for the same object. Whether that matters is a COUNT, not a reading: this returns the bytes, the
/// number of distinct allocations, and how many of them the bucket index turned out to be sharing.
#[cfg(feature = "alloc-probe")]
fn dirty_arc_payload_bytes(shard: &crate::engine::state::ShardState) -> (u64, usize, usize) {
    let mut bucket_strings: HashSet<usize> = HashSet::new();
    walk_bucket_index_strings(&shard.bucket_index, &mut bucket_strings);

    let mut seen: HashSet<usize> = HashSet::new();
    let mut bytes = 0u64;
    let mut shared = 0usize;
    for key in shard.dirty_objects.iter() {
        let ptr = key.as_ptr() as usize;
        if seen.contains(&ptr) {
            continue;
        }
        if bucket_strings.contains(&ptr) {
            shared += 1;
        }
        bytes += arc_str_bytes(&mut seen, key);
    }
    (bytes, seen.len(), shared)
}

/// The positive control for every figure below.
///
/// A probe reporting near zero for a megabyte reads as "this structure is free", which is
/// indistinguishable from the answer that would end this investigation. Prove it moves on something
/// whose size is not in question before trusting it on a shard index.
#[test]
#[cfg(feature = "alloc-probe")]
fn the_clone_probe_sees_a_container_whose_size_is_known() {
    const BYTES: usize = 1 << 20;
    let known: Vec<u8> = vec![7u8; BYTES];
    let measured = deep_heap_bytes(&known);
    println!(
        "clone probe on a {BYTES}-byte Vec<u8>: {measured} bytes charged ({:.3}x)",
        measured as f64 / BYTES as f64
    );
    assert!(
        measured >= BYTES as u64,
        "the probe saw {measured} bytes for a {BYTES}-byte Vec -- it is blind, and every \
         per-structure figure taken with it is noise"
    );
    assert!(
        measured < 2 * BYTES as u64,
        "the probe saw {measured} bytes for a {BYTES}-byte Vec -- it is counting more than the clone"
    );

    // And on the shape whose node overhead is the whole question: a one-entry B-tree must cost MORE
    // than the value it carries, or the container cost reported below is being absorbed somewhere.
    let mut one: BTreeMap<u64, BlockAddress> = BTreeMap::new();
    one.insert(1, BlockAddress::from_parts(1, 0, 64, Some(1), Some(1), Some(7), Some(1)));
    let one_bytes = deep_heap_bytes(&one);
    let value_width = std::mem::size_of::<BlockAddress>() as u64;
    println!(
        "clone probe on a one-entry BTreeMap<u64, BlockAddress>: {one_bytes} B to carry \
         {value_width} B of value ({:.1}x)",
        one_bytes as f64 / value_width as f64
    );
    assert!(
        one_bytes > value_width,
        "a one-entry B-tree node must cost more than the {value_width}-byte value it carries; the \
         probe charged {one_bytes}"
    );
}

/// THE MEASUREMENT. Bytes per live key, by structure, at two corpus sizes ten times apart.
///
/// A per-key cost that is FLAT across the two is a result: the index is linear in the corpus and
/// the figure multiplies out to any scale. One that GROWS is a finding, and the breakdown says
/// which structure grew.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 8,000 then 80,000 records under the counting allocator; run by name"]
fn what_a_live_key_costs_in_the_index_at_two_corpus_sizes() {
    let mut per_key: Vec<(&'static str, f64)> = Vec::new();
    let mut per_address: Vec<(&'static str, f64)> = Vec::new();
    let mut structure_rows: Vec<(&'static str, Vec<(String, f64)>)> = Vec::new();
    let mut allocs_per_write: Vec<(&'static str, f64, f64)> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = probe_engine(dir.path());
        probe_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");

        // --- Denominators FIRST, every one asserted non-empty. ---
        let string_keys = shard.strings.len();
        let series_map_keys = shard.features.len();
        let live_keys = string_keys + series_map_keys;
        let feature_points: usize = shard.features.values().map(|s| s.len()).sum();
        let bucket_pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|b| b.block_index.len())
            .sum();
        let live_addresses = string_keys + feature_points;

        assert_eq!(strings_n, string_keys, "denominator: the string map holds every key seeded");
        assert_eq!(series_keys, series_map_keys, "denominator: the feature map holds every series seeded");
        assert_eq!(
            series_keys * series_points,
            feature_points,
            "denominator: the feature series hold every point seeded"
        );
        assert!(
            bucket_pages > 0,
            "denominator: the bucket index must hold pages, or its footprint is a zero that means \
             nothing"
        );
        assert!(
            !shard.bucket_index.object_block_lookup.is_empty(),
            "denominator: the object lookup must be populated, or its footprint is a zero that \
             means nothing"
        );

        // --- Footprint, structure by structure. ---
        let strings_bytes = deep_heap_bytes(&shard.strings);
        let features_bytes = deep_heap_bytes(&shard.features);
        let bucket_map_bytes = deep_heap_bytes(&shard.bucket_index.bucket_map);
        let lookup_bytes = deep_heap_bytes(&shard.bucket_index.object_block_lookup);
        let expiry_bytes =
            deep_heap_bytes(&shard.expires_at_ms) + deep_heap_bytes(&shard.expiry_by_deadline);
        let core_rest_bytes = deep_heap_bytes(&shard.bucket_index)
            .saturating_sub(bucket_map_bytes)
            .saturating_sub(lookup_bytes);
        let dirty_bytes = deep_heap_bytes(&shard.dirty_objects);
        let recency_bytes = deep_heap_bytes(&shard.bucket_recency);
        let other_model_bytes = deep_heap_bytes(&shard.hashes)
            + deep_heap_bytes(&shard.sets)
            + deep_heap_bytes(&shard.zsets)
            + deep_heap_bytes(&shard.lists)
            + deep_heap_bytes(&shard.sequences)
            + deep_heap_bytes(&shard.seen)
            + deep_heap_bytes(&shard.buckets);
        let context_bytes = deep_heap_bytes(&shard.context_nodes)
            + deep_heap_bytes(&shard.context_events)
            + deep_heap_bytes(&shard.context_event_timeline)
            + deep_heap_bytes(&shard.context_indexes)
            + deep_heap_bytes(&shard.context_audits)
            + deep_heap_bytes(&shard.context_entities)
            + deep_heap_bytes(&shard.context_children)
            + deep_heap_bytes(&shard.context_summaries)
            + deep_heap_bytes(&shard.context_compressions);
        let whole_bytes = deep_heap_bytes(shard);
        let (arc_bytes, arc_count) = arc_payload_bytes(&shard.bucket_index);
        let (dirty_arc_bytes, dirty_arc_count, dirty_arc_shared) =
            dirty_arc_payload_bytes(shard);

        let named = strings_bytes
            + features_bytes
            + bucket_map_bytes
            + lookup_bytes
            + core_rest_bytes
            + expiry_bytes
            + dirty_bytes
            + recency_bytes
            + other_model_bytes
            + context_bytes;
        let other = whole_bytes.saturating_sub(named);
        let total = whole_bytes + arc_bytes + dirty_arc_bytes;

        println!("=== {label}: {live_keys} live keys, {live_addresses} live addresses ===");
        println!(
            "  strings {string_keys}, feature series {series_map_keys} x {series_points} points, \
             bucket-index pages {bucket_pages}, distinct shared strings {arc_count}"
        );
        println!(
            "  dirty index: {} keys, {} buckets, {dirty_arc_count} distinct Arc<str>, \
             {dirty_arc_shared} of them shared with the bucket index",
            shard.dirty_objects.len(),
            shard.dirty_objects.bucket_ids().count()
        );
        let rows: Vec<(String, u64)> = vec![
            ("strings HashMap<String, BlockAddress>".to_string(), strings_bytes),
            ("features HashMap<String, BTreeMap<..>>".to_string(), features_bytes),
            ("other model maps (hashes/sets/zsets/..)".to_string(), other_model_bytes),
            ("context maps (nine of them)".to_string(), context_bytes),
            ("bucket_index.bucket_map".to_string(), bucket_map_bytes),
            ("bucket_index.object_block_lookup".to_string(), lookup_bytes),
            ("bucket_index, the rest of CoreIndex".to_string(), core_rest_bytes),
            ("dirty_objects DirtyObjectIndex".to_string(), dirty_bytes),
            ("bucket_recency HashMap<u32, u64>".to_string(), recency_bytes),
            ("expiry indexes (both)".to_string(), expiry_bytes),
            ("everything else on ShardState".to_string(), other),
            ("bucket-index Arc<str> payloads (once each)".to_string(), arc_bytes),
            ("dirty-index Arc<str> payloads (once each)".to_string(), dirty_arc_bytes),
        ];
        let mut breakdown: Vec<(String, f64)> = Vec::new();
        for (name, bytes) in &rows {
            let bpk = *bytes as f64 / live_keys as f64;
            println!(
                "    {name:<42} {:>12} B   {:>9.1} B/key   {:>5.1}% of index",
                bytes,
                bpk,
                100.0 * *bytes as f64 / total as f64
            );
            breakdown.push((name.clone(), bpk));
        }
        println!(
            "    {:<42} {:>12} B   {:>9.1} B/key   {:>9.1} B/address",
            "TOTAL live heap the shard index holds",
            total,
            total as f64 / live_keys as f64,
            total as f64 / live_addresses as f64
        );

        // --- Occupancy, which is what a container's cost per byte STORED turns on. ---
        println!("  occupancy across the per-kind maps:");
        series_occupancy(&shard.features).report("features");
        series_occupancy(&shard.sequences).report("sequences");
        series_occupancy(&shard.context_events).report("context_events");
        series_occupancy(&shard.context_indexes).report("context_indexes");
        series_occupancy(&shard.context_audits).report("context_audits");
        series_occupancy(&shard.context_entities).report("context_entities");

        let mut pages_per_bucket = Occupancy::default();
        let mut objects_per_bucket = Occupancy::default();
        for bucket in shard.bucket_index.bucket_map.values() {
            pages_per_bucket.observe(bucket.block_index.len());
            objects_per_bucket.observe(bucket.object_index.len());
        }
        pages_per_bucket.report("pages per bucket");
        objects_per_bucket.report("objects per bucket");

        let mut components_per_object = Occupancy::default();
        let mut refs_per_component = Occupancy::default();
        for (_model, _key, refs) in shard.bucket_index.object_block_lookup.iter() {
            components_per_object.observe(refs.by_component.len());
            for component in refs.by_component.iter() {
                refs_per_component.observe(component.refs.len());
            }
        }
        components_per_object.report("components per object");
        refs_per_component.report("refs per component");

        // --- Which ARM each shape is actually in, across the whole shard. ---
        let mut page_arms = [0usize; 3];
        let mut object_arms = [0usize; 3];
        for bucket in shard.bucket_index.bucket_map.values() {
            page_arms[arm_slot(block_index_map_arm(&bucket.block_index))] += 1;
            object_arms[arm_slot(object_index_arm(&bucket.object_index))] += 1;
        }
        let mut component_arms = [0usize; 3];
        let mut ref_arms = [0usize; 2];
        for (_model, _key, refs) in shard.bucket_index.object_block_lookup.iter() {
            component_arms[arm_slot(component_list_arm(&refs.by_component))] += 1;
            for component in refs.by_component.iter() {
                ref_arms[match component.refs {
                    BlockRefs::One(_) => 0,
                    BlockRefs::Many(_) => 1,
                }] += 1;
            }
        }
        println!(
            "  arms Empty/One/Many: block_index {page_arms:?} over {} buckets; object_index \
             {object_arms:?}; by_component {component_arms:?}; refs One/Many {ref_arms:?}",
            shard.bucket_index.bucket_map.len()
        );

        // THE ONE FIGURE HERE THAT IS ASSERTED RATHER THAN REPORTED.
        //
        // Everything else in this probe is a measurement, deliberately: a per-key cost pinned to a
        // number would fail on the next unrelated field added to `ShardState`, and a guard that
        // fails for the wrong reason gets discounted. This one is different because it prices a
        // SHAPE. `by_bucket` held a `BTreeSet` per dirty object, which is a 192-byte node to carry
        // a 16-byte pointer; measured, that was 277.6 B/key at 80,000 records and 279.3 at 8,000,
        // making the dirty index 26% of the whole shard index. Inline it is 85.6 and 87.3. The
        // ceiling sits between the two, so a regression to the set fails and ordinary drift does
        // not.
        let dirty_per_key = dirty_bytes as f64 / live_keys as f64;
        assert!(
            dirty_per_key < 150.0,
            "the dirty index costs {dirty_per_key:.1} B/key at {label}; inline it measured 85.6 \
             and as a BTreeSet per bucket it measured 277.6, so this has regressed to the set"
        );

        // --- IS ANY OF THIS CLONED PER OPERATION? ---
        //
        // A structure copied whole on every write is the scale hazard that a footprint figure
        // cannot see: the index is the same size either way, and the cost only appears as
        // allocations that grow with the corpus. So it is measured the way a growth question has
        // to be -- the SAME operation on two shards ten times apart. Allocations per write that
        // are flat mean nothing shard-sized is being copied; allocations that track the corpus
        // mean something is.
        //
        // Counted, not timed, and on a WARM shard so the first-write reconcile is not in the span.
        drop(shards);
        const PROBE_WRITES: usize = 200;
        let warm = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "probe-warm".to_string(),
                value: vec![b'w'; 32],
            },
        });
        assert!(warm.status.ok, "the warm-up write must ack: {:?}", warm.status);
        let probe = Probe::start();
        let mut acked = 0usize;
        for i in 0..PROBE_WRITES {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("probe-{i}"),
                    value: vec![b'p'; 32],
                },
            });
            if response.status.ok {
                acked += 1;
            }
        }
        let write_counts = probe.stop();
        assert_eq!(
            PROBE_WRITES, acked,
            "denominator: every probe write must have been accepted, or the per-write figure is \
             divided by writes that did not happen"
        );
        let per_write = write_counts.allocs as f64 / PROBE_WRITES as f64;
        let bytes_per_write = write_counts.alloc_bytes as f64 / PROBE_WRITES as f64;
        println!(
            "  per write on a warm shard of {live_keys} keys: {per_write:.1} allocations, \
             {bytes_per_write:.0} B allocated, {:.0} B still outstanding",
            (write_counts.alloc_bytes as i64 - write_counts.free_bytes as i64) as f64
                / PROBE_WRITES as f64
        );
        allocs_per_write.push((label, per_write, bytes_per_write));

        per_key.push((label, total as f64 / live_keys as f64));
        per_address.push((label, total as f64 / live_addresses as f64));
        structure_rows.push((label, breakdown));
    }

    // --- THE RATIO, which is the whole reason to measure twice. ---
    assert_eq!(2, per_key.len(), "both corpus sizes must have been measured");
    let (small_label, small) = per_key[0];
    let (large_label, large) = per_key[1];
    let (_, small_addr) = per_address[0];
    let (_, large_addr) = per_address[1];
    println!("=== bytes per live key, {small_label} vs {large_label} ===");
    println!("  per key:     {small:.1} -> {large:.1} B, ratio {:.3}", large / small);
    println!(
        "  per address: {small_addr:.1} -> {large_addr:.1} B, ratio {:.3}",
        large_addr / small_addr
    );
    for (label, rows) in &structure_rows {
        println!("  {label}:");
        for (name, bpk) in rows {
            println!("    {name:<42} {bpk:>9.1} B/key");
        }
    }
    assert!(
        small > 0.0 && large > 0.0,
        "neither corpus may price a live key at nothing: {small:.1} and {large:.1} B/key"
    );

    // --- PER-OPERATION ALLOCATION, across the same ten-times step. ---
    assert_eq!(2, allocs_per_write.len(), "both corpus sizes must have been probed for writes");
    let (_, small_allocs, small_wbytes) = allocs_per_write[0];
    let (_, large_allocs, large_wbytes) = allocs_per_write[1];
    println!("=== allocations per write, {small_label} vs {large_label} ===");
    println!(
        "  allocations: {small_allocs:.1} -> {large_allocs:.1}, ratio {:.3}",
        large_allocs / small_allocs
    );
    println!(
        "  bytes:       {small_wbytes:.0} -> {large_wbytes:.0}, ratio {:.3}",
        large_wbytes / small_wbytes
    );
    assert!(
        small_allocs > 1.0,
        "a write that allocates {small_allocs:.1} times has not been measured -- the probe span \
         is empty and the flatness below would be the flatness of nothing"
    );
    // The corpus is ten times larger. A structure copied whole per write would show that here; a
    // write that touches only its own key will not. Generous, because this is a shape claim and
    // not a budget: anything under 2x rules out shard-sized copying, and the measured figure is
    // printed above for anyone who wants the number rather than the verdict.
    assert!(
        large_allocs < 2.0 * small_allocs,
        "allocations per write went {small_allocs:.1} -> {large_allocs:.1} across a ten-fold \
         corpus; something shard-sized is being copied on the write path"
    );
}

#[cfg(feature = "alloc-probe")]
fn arm_slot(arm: &'static str) -> usize {
    match arm {
        "Empty" => 0,
        "One" => 1,
        _ => 2,
    }
}

#[cfg(feature = "alloc-probe")]
fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

#[cfg(feature = "alloc-probe")]
fn probe_seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }
}
