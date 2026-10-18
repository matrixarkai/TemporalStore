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
//!     `Vec` in these structures is built to a known small length (`BlockRefs::Many`
//!     starts at capacity 2), so the omission is bounded and small.
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

use crate::block_store::ElementEntry;
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs,
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

/// THREE ARMS STILL, AND THE MIDDLE ONE IS NOW A POINTER RATHER THAN AN INLINE ENTRY. A bucket
/// holding one page still has a representation of its own, so the ladder this is checked against is
/// the shared [`expected_arm`]; what changed is that the single-page arm costs eight bytes in the
/// node instead of the width of a whole entry.
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

// The `component: Option<&str>` parameter this took was DEAD and is gone: it was accepted and
// never mentioned in the `BlockIndex` below, because the field it used to fill came off the entry.
// Two call sites were passing a value that decided nothing.
fn probe_page(object: &str, slot: u64) -> (Arc<str>, BlockIndex) {
    (
        Arc::from(object),
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
            address: ElementEntry::from_parts(1, slot * 64, 64, Some(slot), Some(slot)),
            dirty: false,
            deleted: false,
        },
    )
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
        if index.object_count() != FILL as usize {
            failures.push(format!("ObjectIndex filled to {} entries, not {FILL}", index.object_count()));
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
            let (object_key, entry) = probe_page("o", slot);
            handles.push(map.insert(&object_key, entry, &mut live));
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
    // BOTH of the page index's collapses matter and both are on this ladder: at one entry the list
    // becomes a box, and at zero the box goes too.
    {
        let mut live = crate::engine::state::BlockSlabLiveIndex::default();
        let mut map = BlockIndexMap::default();
        let mut handles: Vec<u64> = Vec::new();
        for slot in 0..FILL {
            let (object_key, entry) = probe_page("o", slot);
            handles.push(map.insert(&object_key, entry, &mut live));
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

    // TWO `ComponentList` DRAIN PATHS STOOD HERE AND THE TYPE IS GONE. They filled the list past
    // its inline arm and drained it one entry at a time -- once from the tail, once from the FRONT,
    // "which a tail-only collapse gets wrong" -- checking at every step that the arm matched
    // `expected_arm(len)`, and that the survivor of a front-drain was the right entry rather than
    // merely an entry.
    //
    // `Option<BlockRefs>` HAS NO IMAGE OF THAT LADDER, which is why neither is rewritten.
    // `expected_arm`'s three rungs need three states and an `Option` has two; a front-versus-tail
    // distinction needs positions and there is one slot; "keeps the right entry" needs entries to
    // choose between. The nearest surviving claim is that the object's entry goes when its last ref
    // does, and that is `BlockRefs`'s emptiness rather than a list's -- held in one statement, with
    // a comment saying it is the single holder, in `CoreIndex::remove_object_block_lookup_ref`.
    //
    // THE DENOMINATOR BELOW MOVED WITH THEM, from six drain paths to four. That constant is the
    // guard that stops this whole test passing by never filling anything, so it is edited rather
    // than dropped.

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

    // Denominator. Four paths drain `FILL` entries one at a time and the `clear()` path
    // contributes one state, so a complete run visits exactly 4 * FILL + 1 of them. It was SIX
    // until the two `ComponentList` paths went with their type. A rewrite that stopped filling
    // would satisfy every assertion above by never making one.
    const DRAIN_STATES: usize = 4 * FILL as usize + 1;
    assert_eq!(
        DRAIN_STATES, steps,
        "the drain visited {steps} states across four paths of {FILL} entries, expected \
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
/// `BlockRefs::insert` grows `One` into `Many`, and `BlockRefs::remove` can leave a `Many` holding
/// nothing -- it cannot turn back into `One`, and it has no collapse of its own.
///
/// # THE OLD SAFETY ARGUMENT WAS RETIRED BY THE COLLAPSE, AND SAYING SO IS THE POINT
///
/// This used to read: "Nothing anywhere removes a SINGLE ref: a ref leaves only when its whole
/// `ComponentBlocks` leaves, through `ComponentList::remove`, which drops the `BlockRefs` entire.
/// So a `Many` that could collapse never arises", and it promised "This fails if a per-ref removal
/// appears". A per-ref removal does appear -- `CoreIndex::remove_object_block_lookup_ref` calls
/// `BlockRefs::remove` -- and this test would not have caught it, because what it asserts is the
/// SURFACE and the surface did not change.
///
/// WHAT MAKES THE MISSING COLLAPSE SAFE NOW is not that the state cannot arise but that it cannot
/// SURVIVE: the empty `Many` is transient by contract, and its single holder drops the object from
/// the lookup in the same breath that sees it empty. The arms below are unchanged and still pin the
/// surface; the claim above them is now the true one.
#[test]
fn a_ref_lists_surface_is_pinned_and_its_empty_arm_is_transient() {
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
/// GENERIC OVER THE SERIES, so a map that changed representation can still be priced beside the
/// ones that have not. It reads a length and nothing else, which is what the series trait names.
// GENERIC OVER THE KEY, because the maps it reports on no longer agree on one key type: the
// event series map is keyed by `ModelKey` and the rest by `String`. The body only walks
// values, so the key needs no bound at all.
fn series_occupancy<K, S: crate::engine::timestamped_series::TimestampedSeries>(
    map: &std::collections::HashMap<K, S>,
) -> Occupancy {
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
        for (_handle, _page) in bucket.block_index.iter() {
            // THE ENTRY'S OWN KEY LINE IS GONE AND THE TOTAL DID NOT MOVE, which is the one case
            // in this change where that is true rather than argued. It read
            // `arc_str_bytes(seen, &page.object_key)`, and `seen` is shared with the lookup walk
            // twenty lines below -- the page's `Arc` and the lookup's were THE SAME ALLOCATION,
            // so whichever reached it first charged it and the other found it already seen. With
            // the entry holding no key, the lookup is simply the one that reaches it.
            //
            // The page's model spelling is one byte inline and a `&'static str`, so it holds no
            // shared allocation for this walk to find. The lookup's `by_model` head below still
            // does, and is still counted -- once, which is the point of the walk.
            // The ENTRY holds no element name either, and neither does the LOOKUP any more: the
            // level that held one is deleted, so there is no third shared allocation anywhere on
            // this path. The loop that used to sum component names was identically zero before it
            // was removed, because every name was already `None`.
        }
    }
    for (model_id, object_key, _refs) in bucket_index.object_block_lookup.iter() {
        bytes += arc_str_bytes(seen, model_id);
        bytes += arc_str_bytes(seen, object_key);
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
/// The key characters `strings` owns that the bucket index does not already hold.
///
/// WHY THIS ROW HAD TO EXIST. The per-key figures come from `deep_heap_bytes`, which measures by
/// CLONING and counting allocations -- and `Arc::clone` is a refcount bump, so a clone allocates
/// the table and NOTHING THE KEYS POINT AT. `strings` is keyed by `ModelKey`, which is `Arc<str>`,
/// so its key characters were never in its row and never in the total. They were not unattributed;
/// they were absent.
///
/// Proved with a control rather than argued: cloning an `Arc<str>`-keyed map of 8,000 entries is
/// ONE allocation, while a `String`-keyed map of the same size is 8,001. A bytes comparison cannot
/// show this -- the table alone is larger than the characters -- which is how the omission survived.
#[cfg(feature = "alloc-probe")]
fn strings_arc_payload_bytes(shard: &crate::engine::state::ShardState) -> (u64, usize, usize) {
    let mut bucket_strings: HashSet<usize> = HashSet::new();
    walk_bucket_index_strings(&shard.bucket_index, &mut bucket_strings);

    let mut seen: HashSet<usize> = HashSet::new();
    let mut bytes = 0u64;
    let mut shared = 0usize;
    for key in shard.strings.keys() {
        let ptr = key.as_ptr() as usize;
        if seen.contains(&ptr) {
            continue;
        }
        if bucket_strings.contains(&ptr) {
            // The bucket index already charged this allocation; billing it again would make the
            // rows add up to twice one buffer, which is the defect the dirty-index row had.
            shared += 1;
            seen.insert(ptr);
            continue;
        }
        bytes += arc_str_bytes(&mut seen, key);
    }
    (bytes, seen.len(), shared)
}

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
            // THE BUCKET INDEX ALREADY CHARGED THIS ALLOCATION. `total` sums this figure
            // with the bucket index's, so the bytes belong to exactly one of them or one
            // buffer is billed twice. Counted as shared and NOT billed. Before the dirty
            // index adopted the page index's handles this branch never ran and the two
            // rows WERE additive, which is what the flipped assertion below now pins.
            shared += 1;
            seen.insert(ptr);
            continue;
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
    let mut one: BTreeMap<u64, ElementEntry> = BTreeMap::new();
    one.insert(1, ElementEntry::from_parts(1, 0, 64, Some(1), Some(1)));
    let one_bytes = deep_heap_bytes(&one);
    let value_width = std::mem::size_of::<ElementEntry>() as u64;
    println!(
        "clone probe on a one-entry BTreeMap<u64, ElementEntry>: {one_bytes} B to carry \
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
        probe_seed(&engine, strings_n, series_keys, series_points, 0);

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
            + deep_heap_bytes(&shard.seen)
            + deep_heap_bytes(&shard.buckets);
        // THE COMPATIBILITY FIELD IS PRICED TOO, AND IT IS EXPECTED TO BE ZERO. The fold drains
        // it at decode and no mutator refills it, so after a load it holds nothing -- but it is
        // added here rather than assumed away, because `whole_bytes` below clones the whole shard
        // and would count it whether this line did or not, and a breakdown that silently omitted a
        // field would disagree with that total the first time the drain stopped working.
        let context_bytes = deep_heap_bytes(&shard.context_nodes)
            + deep_heap_bytes(shard.context_events())
            + deep_heap_bytes(&shard.clone_legacy_context_events_for_test())
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
            ("strings HashMap<ModelKey, ElementEntry>".to_string(), strings_bytes),
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
        series_occupancy(shard.context_events()).report("context_events");
        series_occupancy(&shard.context_indexes).report("context_indexes");
        series_occupancy(&shard.context_audits).report("context_audits");
        series_occupancy(&shard.context_entities).report("context_entities");

        let mut pages_per_bucket = Occupancy::default();
        let mut objects_per_bucket = Occupancy::default();
        for bucket in shard.bucket_index.bucket_map.values() {
            pages_per_bucket.observe(bucket.block_index.len());
            objects_per_bucket.observe(bucket.object_index.object_count());
        }
        pages_per_bucket.report("pages per bucket");
        objects_per_bucket.report("objects per bucket");


        // --- Which ARM each shape is actually in, across the whole shard. ---
        let mut page_arms = [0usize; 3];
        let mut object_arms = [0usize; 3];
        for bucket in shard.bucket_index.bucket_map.values() {
            page_arms[arm_slot(block_index_map_arm(&bucket.block_index))] += 1;
            object_arms[arm_slot(object_index_arm(&bucket.object_index))] += 1;
        }
        // THE `by_component` ARM CENSUS IS GONE AND ITS ONLY REACHABLE BUCKET SAYS WHY. It counted
        // `ComponentList`'s Empty/One/Many across the shard, and `Empty` was unreachable through
        // the producer -- every remover drops an object from the lookup the moment it holds
        // nothing -- while `Many` needed a second component. So it read `[0, n, 0]` by
        // construction for every corpus. The `ref_arms` census beside it is the one with content,
        // and it now reads the object's own slot.
        let mut ref_arms = [0usize; 2];
        for (_model, _key, refs) in shard.bucket_index.object_block_lookup.iter() {
            ref_arms[match refs {
                BlockRefs::One(_) => 0,
                BlockRefs::Many(_) => 1,
            }] += 1;
        }
        println!(
            "  arms Empty/One/Many: block_index {page_arms:?} over {} buckets; object_index \
             {object_arms:?}; lookup refs One/Many {ref_arms:?}",
            shard.bucket_index.bucket_map.len()
        );

        // --- WHAT AN INTEGER-ADDRESSED OBJECT LOOKUP WOULD MOVE, PRICED THROUGH THIS CORPUS.
        //
        // The three candidate entry widths are pinned beside `ObjectBlockLookup`. What a width pin
        // cannot say is what a byte of FIELD costs once it sits inside a B-tree node, and that is
        // the whole difference between the pinned eight bytes and the figure below. A leaf holds
        // eleven entries beside its parent pointer, so one entry's full-leaf cost is arithmetic;
        // the measured cost divided by it is this corpus' slack, and a field byte removed comes
        // back at that factor and no better.
        //
        // IT ALSO NAMES THE OUTER LEVEL'S DENOMINATOR, because "two nested trees" invites the
        // reading that both levels are per-object. `model_count` is one entry per stored kind for
        // the whole shard; the object count is the other figure; the two are printed together so
        // the ratio is not left to be inferred.
        let lookup_objects = shard.bucket_index.object_block_lookup.len();
        let lookup_models = shard.bucket_index.object_block_lookup.model_count();
        assert!(
            lookup_objects > 0,
            "denominator: the object lookup holds no object at {label}, so its per-object figure \
             is a zero that means nothing"
        );
        assert!(
            lookup_models > 0 && lookup_models < lookup_objects,
            "denominator: the lookup reports {lookup_models} model levels over {lookup_objects} \
             objects at {label} -- the outer level is one entry per stored kind, so these two \
             being equal would mean the per-object tree is the OUTER one and every figure below \
             is divided by the wrong count"
        );
        const BTREE_LEAF_CAPACITY: f64 = 11.0;
        const BTREE_LEAF_HEADER: f64 = 16.0;
        let full_leaf = |key: f64, value: f64| {
            (BTREE_LEAF_HEADER + BTREE_LEAF_CAPACITY * (key + value)) / BTREE_LEAF_CAPACITY
        };
        let lookup_today_full = full_leaf(16.0, 24.0);
        let lookup_id_full = full_leaf(8.0, 24.0);
        let lookup_id_keyed_full = full_leaf(8.0, 40.0);
        let lookup_per_object = lookup_bytes as f64 / lookup_objects as f64;
        let lookup_slack = lookup_per_object / lookup_today_full;
        let arc_per_key = arc_bytes as f64 / live_keys as f64;
        println!(
            "  object lookup, {lookup_objects} objects under {lookup_models} model levels: \
             {lookup_per_object:.1} B/OBJECT measured against {lookup_today_full:.1} B/object at a \
             full leaf -- slack {lookup_slack:.2}x, so {:.1} B/object of it is empty node slot",
            lookup_per_object - lookup_today_full
        );
        println!(
            "    integer-addressed, object key DROPPED: {:.1} B/object at this slack -- {:.1} \
             B/object back, plus the {arc_per_key:.1} B/KEY of key payload the index would stop \
             holding. This is the arm the encoder forbids: it writes no object key.",
            lookup_id_full * lookup_slack,
            lookup_per_object - lookup_id_full * lookup_slack,
        );
        println!(
            "    integer-addressed, object key KEPT:    {:.1} B/object at this slack -- {:.1} \
             B/object WORSE than today, and this is the only arm every reader of the inversion \
             can be served from",
            lookup_id_keyed_full * lookup_slack,
            lookup_id_keyed_full * lookup_slack - lookup_per_object,
        );

        // --- THE SHARED-ALLOCATION QUESTION, ASSERTED RATHER THAN PRINTED.
        //
        // The two key-payload lines above are the bucket index's and the dirty index's, each
        // counted ONCE. Whether they ADD turns on whether they are the same allocations: if they
        // were, dropping the key from one holder would free none of those bytes, because the other
        // would still own them. `DirtyObjectIndex::insert` takes a `&str` and builds its own
        // `Arc::from`, so they are distinct -- and that is asserted here rather than read off the
        // source, because the size of every saving claimed for dropping a key depends on it.
        assert_eq!(
            dirty_arc_count, dirty_arc_shared,
            "{dirty_arc_shared} of the dirty index's {dirty_arc_count} key allocations are the same allocation the bucket index holds at {label}, and EVERY ONE OF THEM SHOULD BE. This assertion read `0 == dirty_arc_shared` and pinned the opposite, because `mark_async_dirty_object` asks the lookup for a handle BEFORE the object's first page is filed, so the allocating arm always won and `insert_inner`'s early return never reconsidered. `DirtyObjectIndex::adopt_shared_key`, called once the page IS filed, closes that. A shortfall here means the adoption stopped running and a second copy of every key is back."
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

/// A TTL far enough out that nothing in the corpus expires while a probe is measuring it.
///
/// A deadline that elapsed mid-run would delete the very entries the fixture exists to hold, and
/// the probe would report a smaller corpus than it seeded without saying so.
#[cfg(feature = "alloc-probe")]
const PROBE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// Seed the corpus. `strings_with_deadlines` of the strings are written with a TTL.
///
/// WHY THE DEADLINE SHARE IS A PARAMETER AND NOT A CONSTANT. Five callers in this file price
/// unrelated things and their figures are only comparable to their own history, so they pass 0 and
/// their corpus is unchanged to the byte. One caller -- the footprint bracket -- passes a share,
/// because a corpus with no deadlines cannot see the trade the consolidation is making. Stating it
/// per call site means a reader of any one probe knows which corpus it measured.
///
/// EVERY OTHER KEY, not a contiguous prefix, so the deadline-carrying population is spread across
/// the routing range rather than concentrated in whichever buckets the first n keys hash to.
#[cfg(feature = "alloc-probe")]
fn probe_seed(
    engine: &TemporalEngine,
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
    strings_with_deadlines: usize,
) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| {
                if i % 2 == 0 && i / 2 < strings_with_deadlines {
                    Command::StringSetEx {
                        key: format!("s{i}"),
                        value: vec![b'v'; 32],
                        ttl_ms: PROBE_TTL_MS,
                    }
                } else {
                    Command::StringSet {
                        key: format!("s{i}"),
                        value: vec![b'v'; 32],
                    }
                }
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


// =================================================================================================
// THE SAME MEASUREMENT ON THE SHIPPED ROUTING RANGE
// =================================================================================================

/// A probe engine loaded on an EXPLICIT routing range, rather than on the bare default.
///
/// `probe_engine` calls `load_shard`, which inherits the whole-u32 keyspace -- and on that range
/// every key gets a bucket of its own BY CONSTRUCTION, so every per-bucket cost is charged to
/// exactly one key. That is the LEGACY range. #1973 made 0..1023 the shipped default, where keys
/// share nodes and the per-bucket costs amortise. The two ranges are different regimes, not
/// different fixtures, which is why this exists beside the other loader rather than replacing it.
#[cfg(feature = "alloc-probe")]
fn probe_engine_on(dir: &std::path::Path, end_routing_bucket: u32) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "index-bytes-per-key-shipped".to_string(),
        shard_uri: "local://index-bytes-per-key-shipped/1".to_string(),
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
    engine
}

/// WHAT A LIVE KEY COSTS ON THE SHIPPED ROUTING RANGE, AND WHETHER THE ENTRY IS THE DOMINANT TERM.
///
/// The sibling test above measures the LEGACY whole-u32 range. On it, 40,040 keys produced 40,040
/// buckets -- 40,000 of them in the single-block arm -- so every per-bucket cost divided by one key
/// and the derived bucket index came out at 53.2% of 614.1 B/key. That is not the range a
/// meta-server-managed shard runs on. This is the same measurement on 0..1023, so the two can be
/// read row for row.
///
/// WHY THE SHARES SHOULD MOVE, stated as the prediction this test can falsify: the per-bucket terms
/// -- the node, its one block list, the object index row, the recency row -- amortise across the
/// keys that share a bucket, while the per-BLOCK entry does not. So the entry's share should RISE
/// and everything per-bucket should fall. How far is the question; the ratio of keys to buckets is
/// not a constant anyone should quote from a mean, which is why the distribution is printed.
///
/// THE DISTRIBUTION, NOT THE MEAN. A routing hash is not uniform, and "~39 keys a bucket" is a
/// quotient, not an observation. `Occupancy` prints containers, entries, mean, largest and a nine
/// band histogram, so a skew shows up as a tail rather than hiding inside an average.
///
/// AND WHAT A RELEASE RECOVERS, measured on this range rather than bounded from the structure.
/// `release_bucket_blocks` empties a bucket's block list and keeps the node routable, so its
/// ceiling is the block-list share -- which on this range is shared across the keys in the bucket.
/// Reads must still answer afterwards, and that is asserted rather than assumed: the whole claim
/// about release is that it costs nothing a reader can see.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 8,000 then 80,000 records under the counting allocator; run by name"]
fn what_a_live_key_costs_on_the_shipped_routing_range() {
    /// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default
    /// since #1973.
    const SHIPPED_END: u32 = 1023;

    let mut totals: Vec<(&'static str, f64, f64)> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = probe_engine_on(dir.path(), SHIPPED_END);
        probe_seed(&engine, strings_n, series_keys, series_points, 0);

        let (total, per_key, per_address, live_keys, buckets) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");

            // --- Denominators FIRST, asserted, because a zero divides into anything. ---
            let string_keys = shard.strings.len();
            let series_map_keys = shard.features.len();
            let live_keys = string_keys + series_map_keys;
            let feature_points: usize = shard.features.values().map(|s| s.len()).sum();
            let live_addresses = string_keys + feature_points;
            let bucket_count = shard.bucket_index.bucket_map.values().count();
            let bucket_pages: usize = shard
                .bucket_index
                .bucket_map
                .values()
                .map(|b| b.block_index.len())
                .sum();
            assert_eq!(strings_n, string_keys, "denominator: the string map holds every key seeded");
            assert_eq!(
                series_keys, series_map_keys,
                "denominator: the feature map holds every series seeded"
            );
            assert!(bucket_pages > 0, "denominator: the bucket index must hold pages");
            assert!(
                bucket_count > 0 && bucket_count <= (SHIPPED_END as usize + 1),
                "denominator: {bucket_count} buckets is not a 0..{SHIPPED_END} population -- the \\
                 fixture did not load on the shipped range, and every share below is the other \\
                 regime's"
            );
            // THE WHOLE POINT OF THIS FIXTURE, asserted so it cannot silently become its sibling.
            assert!(
                bucket_count < live_keys,
                "{bucket_count} buckets for {live_keys} keys means one bucket per key, which is the \\
                 LEGACY range -- this test would be measuring exactly what the sibling measures"
            );

            // --- The same rows, computed by the same helpers, so the two ranges compare. ---
            let strings_bytes = deep_heap_bytes(&shard.strings);
            let features_bytes = deep_heap_bytes(&shard.features);
            let bucket_map_bytes = deep_heap_bytes(&shard.bucket_index.bucket_map);
            let lookup_bytes = deep_heap_bytes(&shard.bucket_index.object_block_lookup);
            let core_rest_bytes = deep_heap_bytes(&shard.bucket_index)
                .saturating_sub(bucket_map_bytes)
                .saturating_sub(lookup_bytes);
            let dirty_bytes = deep_heap_bytes(&shard.dirty_objects);
            let recency_bytes = deep_heap_bytes(&shard.bucket_recency);
            let expiry_bytes = deep_heap_bytes(&shard.expires_at_ms)
                + deep_heap_bytes(&shard.expiry_by_deadline);
            let whole_bytes = deep_heap_bytes(shard);
            let (arc_bytes, arc_count) = arc_payload_bytes(&shard.bucket_index);
            let (dirty_arc_bytes, dirty_arc_count, _shared) = dirty_arc_payload_bytes(shard);
            let total = whole_bytes + arc_bytes + dirty_arc_bytes;

            println!(
                "=== {label} on 0..{SHIPPED_END}: {live_keys} live keys, {live_addresses} live \\
                 addresses, {bucket_count} buckets, {bucket_pages} pages ==="
            );
            println!(
                "  distinct shared strings: bucket index {arc_count}, dirty index {dirty_arc_count}"
            );
            let rows: [(&str, u64); 11] = [
                ("strings HashMap<ModelKey, ElementEntry>", strings_bytes),
                ("features HashMap<String, BTreeMap<..>>", features_bytes),
                ("bucket_index.bucket_map", bucket_map_bytes),
                ("bucket_index.object_block_lookup", lookup_bytes),
                ("bucket_index, the rest of CoreIndex", core_rest_bytes),
                ("dirty_objects DirtyObjectIndex", dirty_bytes),
                ("bucket_recency HashMap<u32, u64>", recency_bytes),
                ("expiry indexes (both)", expiry_bytes),
                ("bucket-index Arc<str> payloads (once each)", arc_bytes),
                ("dirty-index Arc<str> payloads (once each)", dirty_arc_bytes),
                ("whole ShardState clone (superset of the above)", whole_bytes),
            ];
            for (name, bytes) in rows {
                println!(
                    "    {name:<46} {bytes:>12} B   {:>9.1} B/key   {:>5.1}% of total",
                    bytes as f64 / live_keys as f64,
                    100.0 * bytes as f64 / total as f64
                );
            }
            println!(
                "    {:<46} {total:>12} B   {:>9.1} B/key   {:>9.1} B/address",
                "TOTAL live heap the shard index holds",
                total as f64 / live_keys as f64,
                total as f64 / live_addresses as f64
            );

            // --- THE DISTRIBUTION, because the mean is a quotient and not an observation. ---
            let mut pages_per_bucket = Occupancy::default();
            let mut objects_per_bucket = Occupancy::default();
            for bucket in shard.bucket_index.bucket_map.values() {
                pages_per_bucket.observe(bucket.block_index.len());
                objects_per_bucket.observe(bucket.object_index.object_count());
            }
            println!("  what a bucket actually holds on this range:");
            pages_per_bucket.report("pages per bucket");
            objects_per_bucket.report("objects per bucket");

            (
                total,
                total as f64 / live_keys as f64,
                total as f64 / live_addresses as f64,
                live_keys,
                bucket_count,
            )
        };
        let _ = total;
        totals.push((label, per_key, per_address));

        // --- WHAT A RELEASE RECOVERS, on this range, with reads still answering. ---
        let probe_keys: Vec<String> = (0..live_keys.min(16)).map(|i| format!("s{i}")).collect();
        let readable_before = probe_keys
            .iter()
            .filter(|key| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key: (*key).clone() },
                        })
                        .response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count();
        assert_eq!(
            probe_keys.len(),
            readable_before,
            "the fixture cannot read its own keys BEFORE a release, so a read that works after one \\
             would prove nothing"
        );

        let bucket_map_before = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            deep_heap_bytes(&shards.get(&1).expect("shard").bucket_index.bucket_map)
        };
        let resident_before = engine.bucket_index_resident_bytes(1);
        let (released_buckets, released_pages, refused) =
            engine.release_all_releasable_bucket_index_blocks(1);
        let bucket_map_after = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            deep_heap_bytes(&shards.get(&1).expect("shard").bucket_index.bucket_map)
        };
        let resident_after = engine.bucket_index_resident_bytes(1);

        let readable_after = probe_keys
            .iter()
            .filter(|key| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key: (*key).clone() },
                        })
                        .response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count();

        println!(
            "  RELEASE on {label}: {released_buckets} of {buckets} buckets released, \\
             {released_pages} pages, {refused} refused"
        );
        println!(
            "    bucket_map heap  {bucket_map_before} -> {bucket_map_after} B  \
             (recovered {} B, {:.1} B/key, {:.1}% of it)",
            bucket_map_before.saturating_sub(bucket_map_after),
            bucket_map_before.saturating_sub(bucket_map_after) as f64 / live_keys as f64,
            if bucket_map_before == 0 {
                0.0
            } else {
                100.0 * bucket_map_before.saturating_sub(bucket_map_after) as f64
                    / bucket_map_before as f64
            }
        );
        println!(
            "    modelled resident {resident_before} -> {resident_after} B (this one is \
             pages * size_of::<BlockIndex>(), a MODEL, printed beside the measured figure on purpose)"
        );
        println!(
            "    reads after release: {readable_after} of {} still answer",
            probe_keys.len()
        );
        // THE CLAIM ABOUT RELEASE IS THAT A READER CANNOT SEE IT. Asserted, not assumed.
        assert_eq!(
            probe_keys.len(),
            readable_after,
            "a release made {} of {} keys unreadable -- releasing is only free if the next read \\
             loads the pages back from the model maps",
            probe_keys.len() - readable_after,
            probe_keys.len()
        );
    }

    println!("=== bytes per live key on 0..1023, two corpus sizes ===");
    for (label, per_key, per_address) in &totals {
        println!("  {label:<16} {per_key:>8.1} B/key   {per_address:>8.1} B/address");
    }
    if totals.len() == 2 {
        println!(
            "  ratio across a 10x corpus: {:.3} per key, {:.3} per address",
            totals[1].1 / totals[0].1,
            totals[1].2 / totals[0].2
        );
    }
}


/// WHAT A DUMP FOLLOWED BY A RELEASE ACTUALLY RECOVERS, ON THE SHIPPED ROUTING RANGE.
///
/// `what_a_live_key_costs_on_the_shipped_routing_range` measured a bare release and recovered
/// NOTHING: 0 of 1,024 buckets, 1,024 refused. That is not "releasing does not work" -- a release
/// requires every block clean and undeleted, and a freshly seeded store is entirely dirty, so every
/// candidate is refused on the dirty term alone. `apply_storage_eviction` carries `dump_before_evict`
/// for exactly this: it writes a dump manifest for the dirty victim buckets first, which is what lets
/// the release term pass.
///
/// So this drives the PRODUCTION sequence rather than a hand-rolled one -- threshold 0 so the
/// pressure gate cannot early-return, no batch limit, dump on, delete-drop OFF because that mode
/// shrinks the index by destroying data and would make any recovery figure meaningless.
///
/// WHY THE NUMBER MATTERS MORE THAN THE ENTRY WIDTH. The derived bucket index is 44% of per-key
/// resident cost on this range, against the index entry's 16%. If a dump-then-release recovers the
/// bulk of that, the lever is a TRIGGER rather than a width -- and the trigger ships off
/// (`enable_evict: false`). The three rows nobody could predict are `bucket_map`, which holds the
/// block lists a release empties, and `object_block_lookup` and `dirty_objects`, which are 19% and
/// 16% and may be structurally pinned.
///
/// READS MUST STILL ANSWER, and that is asserted rather than printed. A release that recovers memory
/// and breaks a read is not a win, and the whole claim about releasing is that the node stays
/// routable and the next read loads its pages back from the model maps.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 80,000 records then dumps and releases under the counting allocator; run by name"]
fn what_a_dump_then_release_recovers_on_the_shipped_routing_range() {
    const SHIPPED_END: u32 = 1023;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), SHIPPED_END);
    probe_seed(&engine, 40_000, 40, 1_000, 0);

    let probe_keys: Vec<String> = (0..16).map(|i| format!("s{i}")).collect();
    let readable = |engine: &TemporalEngine| -> usize {
        probe_keys
            .iter()
            .filter(|key| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key: (*key).clone() },
                        })
                        .response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count()
    };

    // A snapshot of every row the release could plausibly move, plus the denominators.
    let snapshot = |engine: &TemporalEngine| -> (u64, u64, u64, u64, usize, usize, usize, usize) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        (
            deep_heap_bytes(&shard.bucket_index.bucket_map),
            deep_heap_bytes(&shard.bucket_index.object_block_lookup),
            deep_heap_bytes(&shard.dirty_objects),
            deep_heap_bytes(shard),
            shard.strings.len() + shard.features.len(),
            shard.bucket_index.bucket_map.values().count(),
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|b| b.block_index.len())
                .sum(),
            shard.bucket_index.released_buckets.len(),
        )
    };

    let (bm0, lk0, dr0, whole0, keys, buckets0, pages0, released0) = snapshot(&engine);
    let resident0 = engine.bucket_index_resident_bytes(1);
    let read0 = readable(&engine);

    // --- DENOMINATORS, asserted, because every figure below divides by one of them. ---
    assert_eq!(40_040, keys, "denominator: the fixture did not seed the corpus it reports");
    assert!(
        buckets0 > 0 && buckets0 <= (SHIPPED_END as usize + 1),
        "denominator: {buckets0} buckets is not a 0..{SHIPPED_END} population"
    );
    assert!(buckets0 < keys, "denominator: one bucket per key means this is the LEGACY range");
    assert!(pages0 > 0, "denominator: no pages means no block lists to release");
    assert_eq!(0, released0, "denominator: the fixture starts with nothing released");
    assert_eq!(
        probe_keys.len(),
        read0,
        "the fixture cannot read its own keys BEFORE the sequence, so a read afterwards proves nothing"
    );
    println!(
        "=== before: {keys} keys, {buckets0} buckets, {pages0} pages, {released0} released ==="
    );
    println!(
        "  bucket_map {bm0} B ({:.1} B/key), object_block_lookup {lk0} B ({:.1} B/key), \
         dirty_objects {dr0} B ({:.1} B/key), whole {whole0} B ({:.1} B/key)",
        bm0 as f64 / keys as f64,
        lk0 as f64 / keys as f64,
        dr0 as f64 / keys as f64,
        whole0 as f64 / keys as f64
    );

    // --- THE PRODUCTION SEQUENCE: dump the dirty victims, then release them. ---
    let _report = engine.apply_storage_eviction(1, 0, 0, true, false);

    let (bm1, lk1, dr1, whole1, keys1, buckets1, pages1, released1) = snapshot(&engine);
    let resident1 = engine.bucket_index_resident_bytes(1);
    let read1 = readable(&engine);

    println!("=== after dump+release: {buckets1} buckets, {pages1} pages, {released1} released ===");
    let row = |name: &str, before: u64, after: u64| {
        println!(
            "  {name:<28} {before:>10} -> {after:>10} B   recovered {:>10} B  {:>8.1} B/key  {:>6.1}%",
            before.saturating_sub(after),
            before.saturating_sub(after) as f64 / keys as f64,
            if before == 0 { 0.0 } else { 100.0 * before.saturating_sub(after) as f64 / before as f64 }
        );
    };
    row("bucket_map", bm0, bm1);
    row("object_block_lookup", lk0, lk1);
    row("dirty_objects", dr0, dr1);
    row("whole ShardState", whole0, whole1);
    println!(
        "  modelled resident {resident0} -> {resident1} B (pages * size_of::<BlockIndex>(), a MODEL)"
    );
    println!(
        "  pages {pages0} -> {pages1}, buckets {buckets0} -> {buckets1}, keys {keys} -> {keys1}"
    );
    println!("  reads: {read0} of {} before, {read1} after", probe_keys.len());

    // --- THE CORRECTNESS GATE, WHICH IS NOT NEGOTIABLE. ---
    assert_eq!(
        probe_keys.len(),
        read1,
        "a dump-then-release made {} of {} keys unreadable. Releasing is only free if the node stays \
         routable and the next read loads its pages back from the model maps -- this is the failure \
         that would make any recovery figure above worthless",
        probe_keys.len() - read1,
        probe_keys.len()
    );
    // And no key may vanish: a release moves memory, never membership.
    assert_eq!(keys, keys1, "the key count moved across a release, which is data loss, not reclaim");

    // --- THE NON-VACUITY FLOOR, AND IT IS THE ASSERTION THIS TEST WAS MISSING. ---
    //
    // Every assertion above is satisfied by a release that did NOTHING. That is not a hypothetical:
    // it is exactly what `what_a_live_key_costs_on_the_shipped_routing_range` measures when it calls
    // a BARE release -- 0 of 1,024 buckets, 1,024 refused, 0 bytes recovered, and reads still 16 of
    // 16 because nothing moved. So without a floor this fixture passes while measuring the opposite
    // of what it claims, and it would keep passing if `dump_before_evict` silently stopped cleaning
    // the victims.
    //
    // Stated as FLOORS rather than equalities, because the refusal count depends on how many buckets
    // hold a kind `released_model_kind_is_addressable` excludes, and that moves with the corpus.
    assert!(
        released1 > 0,
        "nothing was released, so every figure above is the bare-release case this test exists to \
         distinguish from -- check that the dump actually cleaned the victims"
    );
    assert!(
        released1 * 100 >= buckets1 * 90,
        "only {released1} of {buckets1} buckets released ({:.1}%). The refusals should be the buckets \
         holding a kind the release excludes -- `feature` here -- which is a small minority of this \
         corpus. A large refusal share means a precondition other than the kind term is failing",
        100.0 * released1 as f64 / buckets1 as f64
    );
    assert!(
        pages1 < pages0,
        "the page count did not fall ({pages0} -> {pages1}), so no block list was emptied"
    );
    // The headline figure, floored well below what it measures so a regression is visible but noise
    // is not. Measured 55.7% of the whole ShardState clone at 80,000 records.
    let recovered = whole0.saturating_sub(whole1);
    assert!(
        recovered * 4 >= whole0,
        "a dump-then-release recovered {recovered} B of {whole0} ({:.1}%), which is below the floor \
         this measurement is quoted at. Either the release stopped emptying block lists or the dump \
         stopped cleaning the buckets it dumps",
        100.0 * recovered as f64 / whole0 as f64
    );
    // The buckets must all SURVIVE as nodes: a release empties a block list and keeps the node
    // routable, so losing a node is a different operation wearing this one's name.
    assert_eq!(
        buckets0, buckets1,
        "the bucket count moved across a release ({buckets0} -> {buckets1}); a release keeps every \
         node routable and only empties its block list"
    );
}

// =================================================================================================
// WHAT `bucket_map` IS MADE OF, TERM BY TERM, AND WHICH DENOMINATOR EACH TERM DIVIDES BY
// =================================================================================================

/// Both allocator columns for one deep copy, because this decomposition is the one place the two
/// genuinely disagree.
///
/// `alloc_bytes` is what the structure ASKED the allocator for. `chunk_bytes` is what the allocator
/// SET ASIDE -- header word and rounding included, read back from glibc rather than modelled. A
/// term made of SMALL BOXES is under-charged in the request column by a third: a `Box<BlockIndex>`
/// asks for 24 bytes and is served a 32-byte chunk. Since the single-block arm is exactly such a
/// box, and since the whole question here is which term is big, reporting one column would flatter
/// precisely the term under suspicion. Every row below is printed in both.
#[cfg(feature = "alloc-probe")]
fn deep_heap_columns<T: Clone>(value: &T) -> (u64, u64) {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    (counts.alloc_bytes, counts.chunk_bytes)
}

/// One named term of the `bucket_map` footprint, beside THE COUNT OF LIVE THINGS IT HOLDS.
///
/// `unit` is carried as text and printed, rather than left to the reader, because this campaign has
/// published three non-comparable per-key numbers already. A term divided by the wrong denominator
/// is not an approximation, it is a different measurement: the B-tree container divides by BUCKETS
/// and the block list divides by PAGES, and on one of the two routing ranges measured below those
/// are the same number -- which is exactly how a per-bucket cost gets quoted as a per-page cost.
#[cfg(feature = "alloc-probe")]
struct BucketMapTerm {
    name: &'static str,
    req: u64,
    chunk: u64,
    count: usize,
    unit: &'static str,
}

/// `bucket_map`, decomposed. Totals, terms, and every denominator the terms divide by.
#[cfg(feature = "alloc-probe")]
struct BucketMapDecomposition {
    buckets: usize,
    pages: usize,
    objects: usize,
    slots: usize,
    multi_object_buckets: usize,
    tombstone_buckets: usize,
    tombstone_rows: usize,
    single_page_buckets: usize,
    multi_page_buckets: usize,
    pages_in_list_arm: usize,
    /// Slots the page lists have RESERVED, which a clone cannot see.
    ///
    /// A `Vec` clones to its LENGTH, so every figure the clone probe reports omits spare capacity
    /// outright. That omission is not neutral here: the production resident-byte model charges the
    /// page list at `capacity * size_of::<(u64, BlockIndex)>()`, so the number an eviction gate
    /// reads and the number this probe reports are measuring different quantities whenever the two
    /// diverge. This is the term that says by how much.
    list_capacity: usize,
    total_req: u64,
    total_chunk: u64,
    terms: Vec<BucketMapTerm>,
}

/// THE DECOMPOSITION. Each heap-owning field of a `BucketNode` is cloned on its own, and the
/// container is measured on the SAME TREE with every value emptied.
///
/// WHY THE CONTAINER IS MEASURED THAT WAY rather than subtracted. A B-tree cost is its node arrays,
/// which are sized by the KEY and VALUE widths and by the tree shape -- not by anything the values
/// point at. Cloning the real map and then overwriting every value with `BucketNode::default()`
/// leaves the shape untouched (a value assignment moves no node) and leaves the values holding no
/// heap at all, so a clone of that map charges the node arrays and nothing else. That makes the
/// container an independently MEASURED term rather than a residual, which matters because a
/// residual absorbs every error in every other term and then reads as a finding. The residual is
/// still computed and printed, and it is the check on this: if the terms are right it is near zero,
/// and if it is not, one of them is wrong.
#[cfg(feature = "alloc-probe")]
fn decompose_bucket_map(map: &crate::engine::state::BucketMap) -> BucketMapDecomposition {
    let buckets = map.len();
    let (total_req, total_chunk) = deep_heap_columns(map);

    // --- The container, on the same tree with every value emptied. ---
    let mut shape = map.clone();
    for node in shape.values_mut() {
        *node = crate::engine::state::BucketNode::default();
    }
    assert_eq!(
        buckets,
        shape.len(),
        "the emptied tree must hold the same {buckets} entries as the real one, or it is not the \
         same shape and the container term is measured on a different container"
    );
    let (container_req, container_chunk) = deep_heap_columns(&shape);
    drop(shape);

    // --- Every heap-owning field of the node, bucket by bucket, split by ARM. ---
    let mut pages = 0usize;
    let mut objects = 0usize;
    let mut slots = 0usize;
    let mut multi_object_buckets = 0usize;
    let mut tombstone_buckets = 0usize;
    let mut tombstone_rows = 0usize;
    let mut single_page_buckets = 0usize;
    let mut multi_page_buckets = 0usize;
    let mut pages_in_list_arm = 0usize;
    let mut list_capacity = 0usize;

    let mut boxed_page_req = 0u64;
    let mut boxed_page_chunk = 0u64;
    let mut page_list_req = 0u64;
    let mut page_list_chunk = 0u64;
    let mut object_index_req = 0u64;
    let mut object_index_chunk = 0u64;
    let mut deleted_req = 0u64;
    let mut deleted_chunk = 0u64;
    let mut tombstone_req = 0u64;
    let mut tombstone_chunk = 0u64;

    for bucket in map.values() {
        pages += bucket.block_index.len();
        objects += bucket.object_index.object_count();
        slots += bucket.object_index.slot_count();
        if bucket.object_index.object_count() > 1 {
            multi_object_buckets += 1;
        }
        let rows = bucket.tombstone_elements.len();
        tombstone_rows += rows;
        if rows > 0 {
            tombstone_buckets += 1;
        }

        let (req, chunk) = deep_heap_columns(&bucket.block_index);
        match &bucket.block_index {
            BlockIndexMap::Empty => {}
            BlockIndexMap::One(..) => {
                single_page_buckets += 1;
                boxed_page_req += req;
                boxed_page_chunk += chunk;
            }
            BlockIndexMap::Many(list) => {
                multi_page_buckets += 1;
                pages_in_list_arm += list.len();
                list_capacity += list.capacity();
                page_list_req += req;
                page_list_chunk += chunk;
            }
        }

        let (req, chunk) = deep_heap_columns(&bucket.object_index);
        object_index_req += req;
        object_index_chunk += chunk;

        let (req, chunk) = deep_heap_columns(&bucket.deleted_object_index);
        deleted_req += req;
        deleted_chunk += chunk;

        let (req, chunk) = deep_heap_columns(&bucket.tombstone_elements);
        tombstone_req += req;
        tombstone_chunk += chunk;
    }

    let terms = vec![
        BucketMapTerm {
            name: "BTreeMap node arrays (BucketNode held INLINE)",
            req: container_req,
            chunk: container_chunk,
            count: buckets,
            unit: "buckets",
        },
        BucketMapTerm {
            name: "block_index One: Box<BlockIndex>",
            req: boxed_page_req,
            chunk: boxed_page_chunk,
            count: single_page_buckets,
            unit: "single-page buckets",
        },
        BucketMapTerm {
            name: "block_index Many: Vec<(u64, BlockIndex)>",
            req: page_list_req,
            chunk: page_list_chunk,
            count: pages_in_list_arm,
            unit: "pages in a list",
        },
        BucketMapTerm {
            name: "object_index Many: Box<ObjectSlots>",
            req: object_index_req,
            chunk: object_index_chunk,
            count: multi_object_buckets,
            unit: "multi-object buckets",
        },
        BucketMapTerm {
            name: "deleted_object_index: Option<Box<..>>",
            req: deleted_req,
            chunk: deleted_chunk,
            count: buckets,
            unit: "buckets",
        },
        BucketMapTerm {
            name: "tombstone_elements: Option<Box<Vec<..>>>",
            req: tombstone_req,
            chunk: tombstone_chunk,
            count: tombstone_rows,
            unit: "tombstone rows",
        },
    ];

    BucketMapDecomposition {
        buckets,
        pages,
        objects,
        slots,
        multi_object_buckets,
        tombstone_buckets,
        tombstone_rows,
        single_page_buckets,
        multi_page_buckets,
        pages_in_list_arm,
        list_capacity,
        total_req,
        total_chunk,
        terms,
    }
}

/// WHAT THE `bucket_map` FOOTPRINT IS ACTUALLY MADE OF, ON BOTH ROUTING RANGES.
///
/// # THE QUESTION THIS ANSWERS, AND THE DENOMINATOR TRAP IT WALKS INTO
///
/// `what_a_live_key_costs_in_the_index_at_two_corpus_sizes` prices `bucket_map` at 185.6 B/key over
/// 40,040 keys, and that fixture holds 40,080 pages -- so the same figure reads as 185 B per PAGE,
/// and a page entry is 24 bytes wide. That looks like 160 bytes of overhead nothing can account for.
///
/// It is not, and the reason is the third denominator nobody divided by. That probe loads on the
/// LEGACY whole-u32 routing range, where every key gets a bucket of its own by construction -- so
/// keys, pages and BUCKETS are all the same number, and a per-BUCKET cost is indistinguishable from
/// a per-page cost in every column. The container is a `BTreeMap<u32, BucketNode>` holding a
/// 96-byte node INLINE in its arrays; divided by buckets that is a bucket cost, and divided by
/// pages on that fixture it is the same arithmetic wearing a different name.
///
/// So this measures both ranges and prints every term against its OWN denominator, plus the same
/// term normalised per page and per bucket so the two can be read side by side.
///
/// # THE REGIME CONTROL
///
/// The two ranges must actually BE two regimes, or the comparison says nothing. That is asserted,
/// not assumed: the shipped range must hold materially fewer buckets than pages while the legacy
/// range must hold about one bucket per page. A fixture where both came out alike would make every
/// conclusion below an artefact of one population measured twice.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds four corpora under the counting allocator; run by name"]
fn what_the_bucket_map_footprint_is_made_of_on_both_routing_ranges() {
    /// The end bucket the shipped default routes on, and what an operator is told to set.
    const SHIPPED_END: u32 = 1023;

    let mut per_page_container: Vec<(&'static str, f64)> = Vec::new();
    let mut bucket_to_page: Vec<(&'static str, f64)> = Vec::new();

    for (label, shipped, strings_n, series_keys, series_points) in [
        ("legacy whole-u32, 8,000 records", false, 4_000usize, 4usize, 1_000usize),
        ("shipped 0..1023, 8,000 records", true, 4_000usize, 4usize, 1_000usize),
        ("legacy whole-u32, 80,000 records", false, 40_000usize, 40usize, 1_000usize),
        ("shipped 0..1023, 80,000 records", true, 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = if shipped {
            probe_engine_on(dir.path(), SHIPPED_END)
        } else {
            probe_engine(dir.path())
        };
        probe_seed(&engine, strings_n, series_keys, series_points, 0);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");

        let live_keys = shard.strings.len() + shard.features.len();
        assert_eq!(
            strings_n,
            shard.strings.len(),
            "denominator: the string map must hold every key seeded at {label}"
        );

        let d = decompose_bucket_map(&shard.bucket_index.bucket_map);

        // --- DENOMINATORS FIRST, each asserted populated. A zero here reads as "free". ---
        assert!(d.buckets > 0, "denominator: {label} produced no buckets");
        assert!(d.pages > 0, "denominator: {label} produced no pages");
        assert!(d.objects > 0, "denominator: {label} produced no objects");

        // --- The container floor: the tree holds every key and value INLINE, so it cannot be
        // --- cheaper than their widths. This is what fails if the emptied-tree trick stops
        // --- measuring the container.
        let inline_floor = d.buckets as u64
            * (std::mem::size_of::<crate::engine::state::BucketNode>()
                + std::mem::size_of::<u32>()) as u64;
        let container = d.terms[0].req;
        assert!(
            container >= inline_floor,
            "the B-tree container charged {container} B for {} buckets, below the {inline_floor} B \
             its own key and value widths occupy inline -- the container term is not measuring the \
             container",
            d.buckets
        );

        println!("=== {label} ===");
        println!(
            "  DENOMINATORS: {live_keys} live keys, {} pages, {} buckets, {} objects, {} object \
             slots, {} tombstone rows in {} buckets",
            d.pages, d.buckets, d.objects, d.slots, d.tombstone_rows, d.tombstone_buckets
        );
        println!(
            "  arms: {} single-page buckets, {} list buckets holding {} pages, {} multi-object \
             buckets",
            d.single_page_buckets, d.multi_page_buckets, d.pages_in_list_arm, d.multi_object_buckets
        );
        println!(
            "  pages per bucket {:.2}, keys per bucket {:.2}",
            d.pages as f64 / d.buckets as f64,
            live_keys as f64 / d.buckets as f64
        );
        println!(
            "  bucket_map TOTAL {} B requested, {} B in chunks   ({:.1} B/page, {:.1} B/bucket, \
             {:.1} B/key -- three different denominators, all printed)",
            d.total_req,
            d.total_chunk,
            d.total_req as f64 / d.pages as f64,
            d.total_req as f64 / d.buckets as f64,
            d.total_req as f64 / live_keys as f64
        );
        println!(
            "  {:<46} {:>11} {:>11}  {:>9} {:<22} {:>9} {:>9} | {:>9} {:>9}",
            "TERM",
            "B req",
            "B chunk",
            "over",
            "(its denominator)",
            "B/unit",
            "chunk",
            "B/page",
            "B/bucket"
        );
        let mut summed_req = 0u64;
        let mut summed_chunk = 0u64;
        for term in &d.terms {
            summed_req += term.req;
            summed_chunk += term.chunk;
            let per_unit = if term.count == 0 {
                f64::NAN
            } else {
                term.req as f64 / term.count as f64
            };
            let per_unit_chunk = if term.count == 0 {
                f64::NAN
            } else {
                term.chunk as f64 / term.count as f64
            };
            println!(
                "  {:<46} {:>11} {:>11}  {:>9} {:<22} {:>9.1} {:>9.1} | {:>9.1} {:>9.1}",
                term.name,
                term.req,
                term.chunk,
                term.count,
                term.unit,
                per_unit,
                per_unit_chunk,
                term.req as f64 / d.pages as f64,
                term.req as f64 / d.buckets as f64
            );
        }
        // --- WHAT THE CLONE CANNOT SEE, AND WHICH DIRECTION IT ERRS IN. ---
        //
        // Reported rather than folded into a term above, because it is a DIFFERENT measurement: the
        // rows above are what the structure holds, and this is what it has reserved and not filled.
        // The production model `bucket_index_resident_bytes` charges the page list at its CAPACITY,
        // so the two numbers answer different questions and the band between them belongs in the
        // open rather than inside either one.
        let pair = std::mem::size_of::<(u64, BlockIndex)>();
        let spare_slots = d.list_capacity.saturating_sub(d.pages_in_list_arm);
        println!(
            "  spare page-list capacity: {} slots for {} pages = {spare_slots} spare, {} B \
             the clone cannot see ({:.1} B/page, {:.2}x on the list term)",
            d.list_capacity,
            d.pages_in_list_arm,
            spare_slots * pair,
            (spare_slots * pair) as f64 / d.pages as f64,
            if d.pages_in_list_arm == 0 {
                f64::NAN
            } else {
                d.list_capacity as f64 / d.pages_in_list_arm as f64
            }
        );

        let residual_req = d.total_req as i64 - summed_req as i64;
        let residual_chunk = d.total_chunk as i64 - summed_chunk as i64;
        println!(
            "  {:<46} {:>11} {:>11}   <- total minus the named terms",
            "RESIDUAL (unnamed)", residual_req, residual_chunk
        );

        // --- DO THE TERMS SUM TO THE TOTAL? This is the check on the decomposition itself. ---
        //
        // A decomposition whose terms do not reconstruct the total is a list of numbers, not a
        // breakdown, and the term that is wrong cannot be identified from the list. Floored at 1%
        // rather than at zero only because the container is measured independently rather than
        // subtracted; a real omission is a whole structure and would be far larger than this.
        assert!(
            residual_req.unsigned_abs() * 100 <= d.total_req,
            "the named terms reconstruct {summed_req} of {} B at {label}, leaving {residual_req} B \
             unaccounted -- that is a structure nothing above names, not rounding",
            d.total_req
        );

        per_page_container.push((label, container as f64 / d.pages as f64));
        bucket_to_page.push((label, d.buckets as f64 / d.pages as f64));
        drop(shards);
    }

    // --- THE REGIME CONTROL. Both ranges must have been measured, and they must differ. ---
    assert_eq!(4, bucket_to_page.len(), "all four fixtures must have been measured");
    println!("=== buckets per page, by fixture (THE REGIME CONTROL) ===");
    for (label, ratio) in &bucket_to_page {
        println!("  {label:<34} {ratio:.4} buckets per page");
    }
    println!("=== the container term per PAGE, by fixture ===");
    for (label, bpp) in &per_page_container {
        println!("  {label:<34} {bpp:>8.1} B/page");
    }
    // Index 2 is the legacy 80,000-record fixture, index 3 the shipped one.
    let (legacy_label, legacy_ratio) = bucket_to_page[2];
    let (shipped_label, shipped_ratio) = bucket_to_page[3];
    assert!(
        legacy_ratio > 0.5,
        "{legacy_label} produced {legacy_ratio:.4} buckets per page. The legacy range is supposed \
         to give every key a bucket of its own, which is what makes a per-bucket cost read as a \
         per-page one -- without that this fixture is not the regime the 185 B/page figure came from"
    );
    assert!(
        shipped_ratio * 10.0 < legacy_ratio,
        "{shipped_label} produced {shipped_ratio:.4} buckets per page against {legacy_ratio:.4} on \
         the legacy range. The two must be different regimes or every row above is one population \
         measured twice"
    );
}

/// WHAT AN INTEGER-ADDRESSED OBJECT LOOKUP WOULD SERVE, AND WHAT IT TAKES TO CATCH IT.
///
/// `ObjectBlockLookup` is addressed by the object key, and the width step that would make it
/// integer-addressed is pinned beside the type: it pays in exactly one arm, the arm that stops
/// holding the key. So the question that DECIDES the route is not a width at all. It is whether an
/// address derived from the key can be trusted to name one object.
///
/// THE DERIVATION AVAILABLE IS `hashing::stable_object_hash` -- FNV-1a-64, the one that is
/// deterministic across runs and so the only candidate for an address anything durable leans on.
/// Three things about it, in the order that decides the outcome:
///
///   1. THE MERGE, which is a property of the step and not of the bytes chosen. The step is
///      `h <- (h ^ b) * P` with `P` odd, so two running states take the same value on the next
///      byte if and only if they differ in their low eight bits ALONE. The refusal half of that
///      assertion is here too: states differing one bit higher have NO mergeable byte pair, out of
///      all 65,536. So a chosen-key collision needs a 56-bit agreement rather than a 64-bit one --
///      about 2^28 candidate prefixes, not 2^32, which is a search and not a bound.
///   2. WHAT A COLLISION SERVES. Searched rather than written in, at a width where the search is
///      instant: a pair hard-coded here would go stale the moment the derivation moved, and a
///      stale pair reads exactly like a passing test. An index keyed by the derived address hands
///      the FIRST object's refs to a reader asking for the SECOND, with no error on any path.
///   3. WHAT CATCHES IT. Only a comparison against the object key the entry was filed under, which
///      is the allocation the route exists to remove. So integer addressing WITH verification pays
///      nothing -- it is eight bytes an entry wider than the key-addressed shape it replaces -- and
///      integer addressing without it can serve another object's pages.
#[test]
fn an_integer_addressed_lookup_serves_another_objects_refs() {
    use crate::engine::hashing::{stable_object_hash, stable_object_hash_update};

    // --- 1. THE MERGE, with the instrument's refusal beside it. ---
    let base = stable_object_hash("tenant/1/object/00000001");
    for delta in [0x01u64, 0x1f, 0xff] {
        let mut left = base;
        let mut right = base ^ delta;
        let byte = b'A';
        stable_object_hash_update(&mut left, &[byte]);
        stable_object_hash_update(&mut right, &[byte ^ delta as u8]);
        assert_eq!(
            left, right,
            "two states differing by {delta:#04x} in their low byte must merge on one byte each: \
             the step XORs the byte in before multiplying, so b and b ^ delta cancel the difference"
        );
    }
    // THE REFUSAL. One bit above the byte the step XORs, and nothing merges -- which is what makes
    // the three cases above a property of the step rather than of the deltas picked.
    let unmergeable = base ^ 0x100;
    let mut merging_pairs = 0usize;
    for left_byte in 0u16..256 {
        for right_byte in 0u16..256 {
            let mut left = base;
            let mut right = unmergeable;
            stable_object_hash_update(&mut left, &[left_byte as u8]);
            stable_object_hash_update(&mut right, &[right_byte as u8]);
            if left == right {
                merging_pairs += 1;
            }
        }
    }
    assert_eq!(
        0, merging_pairs,
        "{merging_pairs} of 65,536 byte pairs merged two states differing at bit 8; the merge \
         condition is then wider than the low byte alone, and the 2^28 figure in this doc is wrong"
    );

    // --- 2. WHAT A COLLISION SERVES. ---
    //
    // The search runs at a TRUNCATED address width so that it terminates in milliseconds inside
    // the ordinary suite. The truncation is the only thing scaled: the derivation, the key
    // spelling and the container are the ones an integer-addressed lookup would use. At 24 bits
    // the first collision arrives inside twenty thousand keys; at 64 it is the same construction at
    // a scale no test can run, which is why the bound above is stated rather than searched for.
    const ADDRESS_BITS: u32 = 24;
    const MASK: u64 = (1u64 << ADDRESS_BITS) - 1;
    const SEARCH_CEILING: usize = 40_000;
    let mut by_address: BTreeMap<u64, String> = BTreeMap::new();
    let mut collision: Option<(String, String, u64)> = None;
    let mut scanned = 0usize;
    for i in 0..SEARCH_CEILING {
        let key = format!("tenant/1/object/{i:08}");
        scanned += 1;
        let address = stable_object_hash(&key) & MASK;
        if let Some(first) = by_address.get(&address) {
            collision = Some((first.clone(), key, address));
            break;
        }
        by_address.insert(address, key);
    }
    let (first_key, second_key, address) = collision.expect(
        "no two of the keys scanned shared a truncated address, so this test asserted nothing \
         about what a derived address can serve",
    );
    println!(
        "a {ADDRESS_BITS}-bit derived address collides after {scanned} keys of \
         {SEARCH_CEILING}: {first_key:?} and {second_key:?} both address {address:#x}"
    );
    assert_ne!(
        first_key, second_key,
        "the search returned one key twice, which is a bug in the search and not a collision"
    );
    // AND THE INSTRUMENT CAN REFUSE. At FULL width these two keys do NOT collide, so the pair is
    // an artefact of the truncation and not of the two keys being equal in some other way.
    assert_ne!(
        stable_object_hash(&first_key),
        stable_object_hash(&second_key),
        "the two keys collide at full width too, so the truncation is not what produced this \
         collision and the scaling argument does not hold"
    );

    // The lookup, addressed the way an integer-addressed one would be, holding ONE object's refs.
    let mut id_addressed: BTreeMap<u64, BlockRefs> = BTreeMap::new();
    let filed = BlockRefs::One(BlockLookupRef {
        routing_bucket: 7,
        block_ref_key: 99,
    });
    id_addressed.insert(stable_object_hash(&first_key) & MASK, filed.clone());

    // AND THE SECOND OBJECT, WHICH HAS FILED NOTHING, IS SERVED THE FIRST ONE'S REFS.
    let served = id_addressed.get(&(stable_object_hash(&second_key) & MASK));
    assert_eq!(
        Some(&filed),
        served,
        "an index addressed by a derived id answered nothing for a key colliding with a filed one, \
         so the hazard this test exists to name is not reachable through it"
    );

    // --- 3. WHAT CATCHES IT, AND WHAT THAT COSTS. ---
    //
    // The only check that tells the two apart is a comparison against the object key the entry was
    // filed under -- so the entry has to HOLD that key, which is the allocation the route removes.
    let mut verified: BTreeMap<u64, (Arc<str>, BlockRefs)> = BTreeMap::new();
    verified.insert(
        stable_object_hash(&first_key) & MASK,
        (Arc::from(first_key.as_str()), filed.clone()),
    );
    let hit = verified.get(&(stable_object_hash(&second_key) & MASK));
    let resolved = match hit {
        Some((stored, refs)) if &**stored == second_key.as_str() => Some(refs),
        _ => None,
    };
    assert!(
        resolved.is_none(),
        "verify-on-hit did not reject a colliding key, so the only check available does not work \
         either"
    );
    assert!(
        hit.is_some(),
        "the verified map did not even see the colliding address, so the rejection above was \
         satisfied by a miss rather than by the verification"
    );
    // AND THE WIDTH THAT MAKES VERIFICATION A REGRESSION RATHER THAN A TRADE.
    assert!(
        std::mem::size_of::<(u64, (Arc<str>, BlockRefs))>()
            > std::mem::size_of::<(Arc<str>, BlockRefs)>(),
        "the verified id-addressed entry is no wider than the key-addressed entry it would \
         replace, so ground 1 of the refusal beside ObjectBlockLookup has to be restated"
    );
}

// =================================================================================================
// HOW MANY STRUCTURES HOLD AN ENTRY FOR ONE OBJECT, AND WHAT EACH OF THEM COSTS
// =================================================================================================
//
// THE QUESTION THE SIBLING PROBES DO NOT ANSWER. Every probe above divides a structure's heap by
// the shard's live-key count, which prices the index as a whole. It cannot say how many SEPARATE
// structures hold a row for one object, and that count -- not the width of any one row -- is what
// multiplies. A census by division also hides the kinds that are absent from a fixture: a map that
// holds nothing for a `string` reads as a small per-key number rather than as a zero.
//
// SO THIS MEASURES A MARGINAL COHORT. A population is seeded, every structure is weighed, a cohort
// of ONE KIND is added, and every structure is weighed again. A structure whose heap does not move
// holds no entry for that kind; one whose heap moves holds an entry costing the delta over the
// cohort. That is the count the census needs, and it is read off the allocator rather than off the
// type declarations.
//
// TWO FIGURES, BECAUSE A `HashMap` DELTA IS NOT A PER-ENTRY COST. A hash table clones to its bucket
// COUNT, which steps at a power of two: a cohort that straddles a rehash is charged the whole new
// table and the next cohort is charged nothing. So every structure is reported twice --
//
//   * MARGINAL, the cohort delta over the cohort size, which answers "does this structure hold an
//     entry for this kind at all" exactly, and answers "what does the entry cost" only on average
//     across a rehash;
//   * STEADY-STATE, the structure's whole heap over ITS OWN denominator -- `strings` over the
//     string count, `bucket_map` over the PAGE count, `bucket_recency` over the BUCKET count --
//     which is the resident figure including table slack, and is the one to quote.
//
// Mixing those two denominators is the error this campaign has already paid for once, so each row
// prints the denominator it used. A per-page or per-bucket row is meaningless without the bucket
// count, which is printed in the arm header and asserted to be a 0..1023 population.
#[cfg(feature = "alloc-probe")]
fn census_bytes(shard: &crate::engine::state::ShardState) -> Vec<(&'static str, u64)> {
    let bucket_map = deep_heap_bytes(&shard.bucket_index.bucket_map);
    let lookup = deep_heap_bytes(&shard.bucket_index.object_block_lookup);
    let core_rest = deep_heap_bytes(&shard.bucket_index)
        .saturating_sub(bucket_map)
        .saturating_sub(lookup);
    let (arc_bytes, _) = arc_payload_bytes(&shard.bucket_index);
    let (dirty_arc_bytes, _, _) = dirty_arc_payload_bytes(shard);
    vec![
        ("strings", deep_heap_bytes(&shard.strings)),
        ("features", deep_heap_bytes(&shard.features)),
        ("hashes", deep_heap_bytes(&shard.hashes)),
        ("bucket_index.bucket_map", bucket_map),
        ("bucket_index.object_block_lookup", lookup),
        ("bucket_index, the rest of CoreIndex", core_rest),
        ("dirty_objects", deep_heap_bytes(&shard.dirty_objects)),
        ("bucket_recency", deep_heap_bytes(&shard.bucket_recency)),
        ("expires_at_ms", deep_heap_bytes(&shard.expires_at_ms)),
        ("expiry_by_deadline", deep_heap_bytes(&shard.expiry_by_deadline)),
        ("wal_resident_blocks", deep_heap_bytes(&shard.wal_resident_blocks)),
        ("bucket-index Arc<str> payloads", arc_bytes),
        ("dirty-index Arc<str> payloads", dirty_arc_bytes),
        ("WHOLE ShardState clone", deep_heap_bytes(shard)),
    ]
}

/// The census rows, weighed through the shard lock and handed back as owned numbers.
#[cfg(feature = "alloc-probe")]
fn snapshot_census(engine: &TemporalEngine) -> Vec<(&'static str, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    census_bytes(shard)
}

/// Per-structure delta over a cohort of one kind, and the COUNT of structures that moved.
///
/// The count is the deliverable. `WHOLE ShardState clone` is a superset of every row above it and
/// is excluded from the count on purpose -- including it would report one structure too many for
/// every kind.
#[cfg(feature = "alloc-probe")]
fn report_cohort(
    case: &str,
    cohort: usize,
    before: &[(&'static str, u64)],
    after: &[(&'static str, u64)],
) -> usize {
    assert_eq!(before.len(), after.len(), "census rows must line up to be differenced");
    assert!(cohort > 0, "denominator: a cohort of zero divides into anything");
    println!("  --- {case}: cohort of {cohort} ---");
    let mut holders = 0usize;
    for (i, (name, b)) in before.iter().enumerate() {
        let (name_after, a) = after[i];
        assert_eq!(*name, name_after, "census rows drifted between the two snapshots");
        let delta = a as i64 - *b as i64;
        let whole = *name == "WHOLE ShardState clone";
        let note = if whole {
            "superset of the rows above, not counted"
        } else if delta == 0 {
            "HOLDS NO ENTRY for this kind"
        } else {
            ""
        };
        if delta != 0 && !whole {
            holders += 1;
        }
        println!(
            "      {name:<38} {delta:>12} B  {:>9.1} B/object  {note}",
            delta as f64 / cohort as f64
        );
    }
    println!("      => STRUCTURES HOLDING AN ENTRY for one {case}: {holders}");
    holders
}

#[cfg(feature = "alloc-probe")]
fn seed_strings_from(
    engine: &TemporalEngine,
    prefix: &str,
    start: usize,
    count: usize,
    ttl_ms: Option<u64>,
) {
    let end = start + count;
    for chunk_start in (start..end).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(end))
            .map(|i| match ttl_ms {
                None => Command::StringSet {
                    key: format!("{prefix}{i}"),
                    value: vec![b'v'; 32],
                },
                Some(ttl) => Command::StringSetEx {
                    key: format!("{prefix}{i}"),
                    value: vec![b'v'; 32],
                    ttl_ms: ttl,
                },
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
}

#[cfg(feature = "alloc-probe")]
fn seed_hashes_from(engine: &TemporalEngine, prefix: &str, count: usize, fields: usize) {
    for i in 0..count {
        let commands = (0..fields)
            .map(|f| Command::HashSet {
                key: format!("{prefix}{i}"),
                field: format!("f{f}"),
                value: vec![b'h'; 32],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "hash seed must ack: {:?}", response.status);
    }
}

#[cfg(feature = "alloc-probe")]
fn seed_features_from(engine: &TemporalEngine, prefix: &str, keys: usize, points: usize) {
    for k in 0..keys {
        for chunk_start in (0..points).step_by(500) {
            let batch = (chunk_start..(chunk_start + 500).min(points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if batch.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("{prefix}{k}"),
                    points: batch,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }
}

/// HOW MANY STRUCTURES HOLD AN ENTRY FOR ONE OBJECT, PER KIND, ON THE SHIPPED ROUTING RANGE.
///
/// Five cases, each measured as a marginal cohort against the same base population: a plain string
/// with no deadline, a string with one, a container, a timestamped series, and -- for the clean
/// case -- the same population after the production dump-and-release sequence, where a NEGATIVE
/// delta is memory the dump handed back.
///
/// WHY THE CLEAN CASE IS A DUMP RATHER THAN A SIXTH COHORT. Nothing writes a clean object: an
/// object is clean because a dump has been taken since it was written. So "clean" is not a kind to
/// seed, it is a state to reach, and the honest measurement of what being dirty costs is what a
/// dump gives back.
///
/// THE ARMS ARE 4x APART AT THE DENOMINATOR THEY ARE QUOTED AT, AND BOTH COMPLETE. The sibling
/// probes seed 8,000 then 80,000 records, and the 80,000 arm has never finished on this box -- it
/// was stopped mid-seed at thirteen minutes of CPU. A figure from an arm that does not complete is
/// not a figure.
///
/// AND 4x IN THE SEED IS NOT 4x IN THE TABLE. An earlier spelling scaled the base string count 4x
/// and left the container, series and cohort populations fixed; the live-key counts the
/// steady-state table divides by then came out 2.33x apart, and `hashes` fell from 48.9 to 20.9
/// B/key purely because the mix had changed underneath it. Every population here scales by ONE
/// factor, so the mix is identical between the arms and the ratio is 4.0 at the denominator the
/// figures are actually quoted against.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds two corpora under the counting allocator and weighs every structure; run by name"]
fn how_many_structures_hold_an_entry_for_one_object() {
    /// The shipped default since #1973, and the range `docs/runtime_tuning.md` tells an operator to
    /// set. On the LEGACY whole-u32 range every key gets a bucket of its own, so every per-bucket
    /// cost divides by one key and `bucket_recency` reads as a per-key structure. It is not one.
    const SHIPPED_END: u32 = 1023;

    // ONE SCALE FACTOR PER ARM, and EVERY population below derives from it, so the two arms
    // hold the same MIX and differ only in size. The previous spelling scaled the base strings 4x
    // and left the container and series populations fixed, which made the live-key ratio 2.33x at
    // the steady-state table rather than 4x -- and moved `hashes` from 48.9 to 20.9 B/key by
    // dilution alone. A ratio taken across two mixes is not a scaling result.
    for (arm, scale) in [("1x", 1usize), ("4x", 4usize)] {
        let base_strings = 2_000 * scale;
        let base_series = 2 * scale;
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = probe_engine_on(dir.path(), SHIPPED_END);
        probe_seed(&engine, base_strings, base_series, 1_000, 0);

        // --- DENOMINATORS FIRST, ASSERTED, because every figure below divides by one. ---
        let (base_keys, buckets, pages) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let keys = shard.strings.len() + shard.features.len();
            let buckets = shard.bucket_index.bucket_map.values().count();
            let pages: usize =
                shard.bucket_index.bucket_map.values().map(|b| b.block_index.len()).sum();
            assert_eq!(
                base_strings,
                shard.strings.len(),
                "denominator: the string map does not hold the base population"
            );
            assert_eq!(
                base_series,
                shard.features.len(),
                "denominator: the feature map does not hold the base population"
            );
            assert!(pages > 0, "denominator: the bucket index must hold pages");
            assert!(
                buckets > 0 && buckets <= (SHIPPED_END as usize + 1),
                "denominator: {buckets} buckets is not a 0..{SHIPPED_END} population -- the fixture \
                 did not load on the shipped range and every per-bucket row below is the other \
                 regime's"
            );
            assert!(
                buckets < keys,
                "denominator: {buckets} buckets for {keys} keys is one bucket per key, which is the \
                 LEGACY range -- a per-bucket structure would read as a per-key one"
            );
            (keys, buckets, pages)
        };
        println!(
            "=== {arm}-record base on 0..{SHIPPED_END}: {base_keys} live keys, {buckets} BUCKETS, \
             {pages} pages (scale {scale}x; every population scales together so the two arms hold \
             the same mix) ==="
        );

        // --- CASE 1: a plain string object, no deadline, dirty because nothing has dumped yet. ---
        let string_cohort: usize = 500 * scale;
        let before = snapshot_census(&engine);
        seed_strings_from(&engine, "p", base_strings + 1_000_000, string_cohort, None);
        let after = snapshot_census(&engine);
        report_cohort("plain string, NO deadline, DIRTY", string_cohort, &before, &after);

        // --- CASE 2: the same object WITH a deadline. ---
        let ttl_cohort: usize = 500 * scale;
        let ttl_before = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            shards.get(&1).expect("shard").expires_at_ms.len()
        };
        let before = snapshot_census(&engine);
        seed_strings_from(&engine, "t", base_strings + 2_000_000, ttl_cohort, Some(86_400_000));
        let after = snapshot_census(&engine);
        let ttl_after = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            shards.get(&1).expect("shard").expires_at_ms.len()
        };
        // NON-VACUITY: a deadline cohort that files no deadline would make the two expiry rows read
        // as free, which is the whole question this case exists to answer.
        assert_eq!(
            ttl_before + ttl_cohort,
            ttl_after,
            "the deadline cohort filed {} deadlines, not {ttl_cohort} -- the expiry rows below are \
             measuring nothing",
            ttl_after - ttl_before
        );
        report_cohort("string WITH a deadline, DIRTY", ttl_cohort, &before, &after);

        // --- CASE 3: a container object. N IS STATED: eight elements each. ---
        let hash_cohort: usize = 250 * scale;
        const HASH_FIELDS: usize = 8;
        let before = snapshot_census(&engine);
        seed_hashes_from(&engine, "h", hash_cohort, HASH_FIELDS);
        let after = snapshot_census(&engine);
        report_cohort("container object, 8 elements each, DIRTY", hash_cohort, &before, &after);

        // --- CASE 4: a timestamped object. N IS STATED: one thousand points each. ---
        let series_cohort: usize = 2 * scale;
        const SERIES_POINTS: usize = 1_000;
        let before = snapshot_census(&engine);
        seed_features_from(&engine, "g", series_cohort, SERIES_POINTS);
        let after = snapshot_census(&engine);
        report_cohort(
            "timestamped object, 1,000 points each, DIRTY",
            series_cohort,
            &before,
            &after,
        );

        // --- THE STEADY-STATE TABLE: every structure over ITS OWN denominator. ---
        {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let rows = census_bytes(shard);
            let strings_n = shard.strings.len();
            let features_n = shard.features.len();
            let hashes_n = shard.hashes.len();
            let lookup_n = shard.bucket_index.object_block_lookup.len();
            let dirty_n = shard.dirty_objects.len();
            let recency_n = shard.bucket_recency.len();
            let expiry_n = shard.expires_at_ms.len();
            let deadline_n = shard.expiry_by_deadline.len();
            let wal_n = shard.wal_resident_blocks.len();
            let buckets_now = shard.bucket_index.bucket_map.values().count();
            let pages_now: usize =
                shard.bucket_index.bucket_map.values().map(|b| b.block_index.len()).sum();
            let live_keys = strings_n + features_n + hashes_n;
            println!(
                "  --- STEADY STATE, each structure over ITS OWN denominator ({buckets_now} \
                 buckets, {pages_now} pages, {strings_n} strings, {features_n} series, {hashes_n} \
                 containers, {dirty_n} dirty) ---"
            );
            for (name, bytes) in &rows {
                let (denom, denom_name): (usize, &str) = match *name {
                    "strings" => (strings_n, "string object"),
                    "features" => (features_n, "series object"),
                    "hashes" => (hashes_n, "container object"),
                    "bucket_index.bucket_map" => (pages_now, "PAGE"),
                    "bucket_index.object_block_lookup" => (lookup_n, "lookup row"),
                    "bucket_index, the rest of CoreIndex" => (buckets_now, "BUCKET"),
                    "dirty_objects" => (dirty_n, "dirty object"),
                    "bucket_recency" => (recency_n, "BUCKET"),
                    "expires_at_ms" => (expiry_n, "deadline"),
                    "expiry_by_deadline" => (deadline_n, "deadline"),
                    "wal_resident_blocks" => (wal_n, "log-resident block"),
                    _ => (live_keys, "live key"),
                };
                println!(
                    "      {name:<38} {bytes:>12} B  {:>9.1} B per {denom_name:<18} (n = {denom})",
                    if denom == 0 { 0.0 } else { *bytes as f64 / denom as f64 }
                );
            }
        }

        // --- CASE 5: CLEAN. The production dump-and-release sequence; negative is handed back. ---
        let probe_keys: Vec<String> = (0..16).map(|i| format!("s{i}")).collect();
        let readable = |engine: &TemporalEngine| {
            probe_keys
                .iter()
                .filter(|key| {
                    matches!(
                        engine
                            .execute(ExecuteRequest {
                                shard_id: 1,
                                command: Command::StringGet { key: (*key).clone() },
                            })
                            .response,
                        crate::types::CommandResponse::Bytes { value: Some(_) }
                    )
                })
                .count()
        };
        let read_before = readable(&engine);
        assert_eq!(
            probe_keys.len(),
            read_before,
            "the fixture cannot read its own keys BEFORE the dump, so a read afterwards proves nothing"
        );
        let dirty_count = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            shards.get(&1).expect("shard").dirty_objects.len()
        };
        assert!(
            dirty_count > 0,
            "non-vacuity: nothing is dirty before the dump, so the clean case would recover nothing \
             and read as a structure that costs nothing"
        );
        let before = snapshot_census(&engine);
        let _report = engine.apply_storage_eviction(1, 0, 0, true, false);
        let after = snapshot_census(&engine);
        println!(
            "  --- CLEAN: dump+release over {dirty_count} dirty objects (negative = handed back) ---"
        );
        report_cohort("object made CLEAN by a dump", dirty_count, &before, &after);
        let read_after = readable(&engine);
        assert_eq!(
            probe_keys.len(),
            read_after,
            "the dump made keys unreadable -- a recovery figure taken beside broken reads is not a \
             saving"
        );
    }
}

// =================================================================================================
// THE FOOTPRINT BRACKET
// =================================================================================================

/// THE RATCHET: WHAT THE SHARD INDEX IS ALLOWED TO COST, BRACKETED ABOVE AND BELOW.
///
/// # WHY A BRACKET AND NOT A CEILING
///
/// A ceiling alone rots in one direction. It catches a regression and says nothing when the figure
/// IMPROVES, so a win goes unrecorded and the ceiling keeps quoting a cost nobody pays any more --
/// and the next reader takes the stale ceiling for the current footprint. The floor is what makes
/// this a ratchet rather than a cap: cross it and the test FAILS, demanding both bounds be moved
/// down and the new figure published. An improvement that does not tighten this test is an
/// improvement nobody can cite.
///
/// The floor earns its keep a second way. Every term here is measured by CLONING the structure and
/// counting what the clone allocates, so a structure that becomes empty, or a field that stops
/// being reached, measures as CHEAP rather than as broken. A figure that falls through the floor is
/// as likely to be a measurement that stopped measuring as a genuine saving, and the two are
/// indistinguishable from the number alone. The floor forces that question to be asked out loud.
///
/// # THE DENOMINATORS, AND WHY BOTH RATIOS ARE PUBLISHED
///
/// The same heap divides two ways and the two are NOT interchangeable:
///
///   * **per live key** -- one object, one index record. This is the denominator for anything
///     charged once per object, a per-record width above all.
///   * **per live address** -- one stored page. This is the denominator for anything charged once
///     per page.
///
/// On this fixture they differ by exactly 2x, because half the addresses are series points hanging
/// off forty keys. Quoting a per-record cost against the per-address figure halves it, and that is
/// a mistake this lane has already made and published. So both are asserted, each against its own
/// named denominator, and neither is derived from the other.
///
/// # WHAT IS PINNED, AND WHY EVERY ONE OF THEM HAS TO BE
///
/// A footprint figure is meaningless without the population it was taken over, so the fixture is
/// pinned and ASSERTED, not merely passed:
///
///   * **the routing range** -- 0..1023, the shipped default since #1973. On the legacy whole-u32
///     range every key gets a bucket of its own, every per-bucket cost divides by one key, and the
///     same measurement reads 48% higher. That is not a different fixture, it is a different
///     REGIME, and a bracket taken on one says nothing about the other.
///   * **the bucket count** -- 1,024, asserted exactly. It is the denominator of every per-bucket
///     term and it is what separates the two regimes.
///   * **the corpus** -- 40,000 single-page objects and 40 series of 1,000 points. Asserted at both
///     ends, because the per-key figure FALLS as the corpus grows (0.798x across a 10x corpus, so
///     per-bucket costs amortise), which makes the corpus size part of the number itself.
///
/// # WHAT THIS FIXTURE IS NOT
///
/// Its mix is CONSTRUCTED. Forty of 40,040 keys carry a series because this test passes forty, and
/// that quotient is this test's own arguments read back, not an observed property of any corpus.
/// Nothing here may be quoted as a population. The bracket is a regression gate on a fixed fixture
/// and is honest only as that.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 80,000 records under the counting allocator; run by name"]
fn the_index_footprint_stays_inside_its_measured_bracket() {
    /// The shipped routing default since #1973, and what an operator is told to set.
    const SHIPPED_END: u32 = 1023;
    /// Every bucket in 0..=1023 is occupied at this corpus size, and that is asserted rather than
    /// assumed: it is the denominator of every per-bucket term below.
    const PINNED_BUCKETS: usize = 1024;
    /// The corpus the bracket was taken over. Changing any of these four changes the number.
    const PINNED_STRINGS: usize = 40_000;
    const PINNED_SERIES: usize = 40;
    const PINNED_POINTS: usize = 1_000;
    /// HOW MANY STRINGS CARRY A DEADLINE, and the reason this corpus has any at all.
    ///
    /// WHAT THE FIXTURE COULD NOT SEE BEFORE. The seed issued `StringSet` and `FeatureAppend`
    /// only, and `StringSet` carries no TTL -- so `expires_at_ms` and `expiry_by_deadline` were
    /// EMPTY here. That matters because the consolidation's trade is exactly those two maps: a
    /// slot is sixteen bytes wider than the bare address it replaces, and those bytes are the
    /// deadline, flags and model id that a later change fills so a key's deadline entry can stop
    /// existing. On a corpus with no deadlines the COST of that widening was fully visible and the
    /// OFFSETTING WIN was structurally unmeasurable -- a bound that can see one half of a trade
    /// reports a regression for a change that is net neutral or better.
    ///
    /// That is the same vacuity as a duplication figure measured on an empty map, arriving from
    /// the other side. `strings` being seeded is what made its cost real; `expires_at_ms` being
    /// empty is what hid the win against it.
    ///
    /// HALF, NOT ALL. Both populations stay present, so a change that helps only the
    /// deadline-carrying keys shows a partial win rather than all-or-nothing -- which is more
    /// informative and does not imply every stored object has a TTL.
    const PINNED_DEADLINES: usize = PINNED_STRINGS / 2;

    // THE BRACKET, measured on 0..1023 at this corpus: 16,141,700 B over 40,040 live keys and
    // 80,000 live addresses, which is 403.1 B/key and 201.8 B/address.
    //
    // THESE ARE NOT COMPARABLE TO THE FIGURES THAT STOOD HERE BEFORE. The previous bracket read
    // 14,253,412 B -> 356.0 B/key and 178.2 B/address, and it was taken on a corpus with NO
    // DEADLINES. The corpus changed, so the measurement changed; the old pair and this pair are
    // measurements of different things and neither is a drift from the other.
    //
    // WHAT THE DELTA IS, STATED NO MORE STRONGLY THAN IT CAN BE. It is the delta OF THE CORPUS
    // CHANGE -- 1,888,288 B for 20,000 deadlines -- of which the two deadline maps' node space is
    // 1,883,344 B measured separately: 45.2 B per deadline in `expires_at_ms` and 48.9 B per
    // deadline in the ordered `expiry_by_deadline`. It is NOT "the cost of a deadline"; an earlier
    // version of this comment said that and it does not follow.
    //
    // AN OPEN QUESTION, LEFT OPEN HERE RATHER THAN LEFT OUT. Those two figures cannot BOTH be the
    // cost of the deadlines. A deadline's key characters live in one `Arc<str>` shared by both maps
    // (`set_expiry`: "ONE ALLOCATION OF THE CHARACTERS, HELD BY BOTH MAPS"), so ~480,000 B of
    // character buffers for 20,000 keys must appear in a WHOLE-SHARD delta -- and they appear in
    // neither node figure, which leaves the two within 0.26% of each other. So something in this
    // corpus change went DOWN by roughly the size of the characters, and it is not yet identified.
    // An unexplained term that size is exactly what makes a confident attribution unsafe, which is
    // why the sentence above claims only the corpus change.
    //
    // WHY THE NODE FIGURES EXCLUDE THE CHARACTERS, BY TWO ROUTES. The first reasons about what the
    // code does: `deep_heap_bytes` is `value.clone()` under the counting allocator, and
    // `Arc::clone` is a refcount bump rather than an allocation, so cloning either map allocates
    // node space and no characters. The second shows the alternative CANNOT BE TRUE, and is the
    // stronger of the two: std's `BTreeMap` holds at most 11 entries per node, and an
    // `Arc<str>`(16 B) key with a `u64`(8 B) value gives a 264 B full-node payload against this
    // crate's measured 280-byte root node -- so 45.2 B/entry implies about 56% node occupancy,
    // which is plausible, while 45.2 minus 24 B of characters would leave 21.2 B/entry and imply
    // 120% occupancy, which is impossible. One route argues from semantics; the other rules the
    // alternative out.
    //
    // THE MARGINS ARE CARRIED ACROSS UNCHANGED, which is what makes this a fixture change and not
    // a bound relaxation. Each bound keeps the exact distance it had from the figure it bracketed:
    //
    //     key     ceiling  +4.0   ->  403.1 + 4.0  = 407.1      (was 356.0 + 4.0  = 360.0)
    //     key     floor   -18.0   ->  403.1 - 18.0 = 385.1      (was 356.0 - 18.0 = 338.0)
    //     address ceiling  +1.8   ->  201.8 + 1.8  = 203.6      (was 178.2 + 1.8  = 180.0)
    //     address floor    -9.2   ->  201.8 - 9.2  = 192.6      (was 178.2 - 9.2  = 169.0)
    //
    // So the gate is exactly as tight as it was: it still refuses a change that moves the figure
    // by more than the same number of bytes it refused before. Headroom is deliberately thin --
    // the point is to catch a change that moves the figure, not to leave room for one, and a bound
    // nothing can breach gates nothing.
    //
    // THE WORKED EXAMPLE BELOW WAS MEASURED ON THE PRE-DEADLINE CORPUS, which is not the corpus
    // this file now seeds. Its figures -- 14,253,412 B, 382.2 B/key, and a breach of 22.2 B/key
    // against a 360.0 ceiling -- are measurements of the OLD fixture against the OLD bound, kept
    // because the rule they demonstrate does not depend on which corpus produced it. Read them as
    // the example's own numbers and not as this file's baseline: the baseline is the pair at the
    // top of this block. The same statement applies here as to the bounds themselves -- the two
    // corpora measure different things and neither is a drift from the other.
    //
    // AND THE CONVERSION'S COST IS NOT RE-DERIVED HERE. 26.2 B/key is what widening `strings` cost
    // on the old corpus; what it costs on this one is a measurement the conversion's own change
    // has to make. Asserting a new figure for it here would be the mirror of the error this block
    // exists to warn about.
    // ============================================================================================
    // BEFORE YOU ESTIMATE A VALUE-TYPE CHANGE AGAINST THIS BOUND: MULTIPLY BY THE TABLE'S
    // CAPACITY, NOT BY ITS LENGTH.
    // ============================================================================================
    //
    // A `HashMap` whose VALUE type grows by n bytes does not cost `len * n`. It costs
    // `capacity * n`, because hashbrown allocates one slot per capacity unit whether or not a key
    // occupies it. Capacity is a power of two at or above `len / 0.875`, so the multiplier between
    // the two estimates is 1.0 just after a doubling and approaches 2.0 just before the next one.
    // The SAME change therefore costs nearly twice as much in one store as in another with nothing
    // in the code differing.
    //
    // MEASURED HERE, on this fixture, by converting `strings` from a 16-byte `ElementEntry` value
    // to a 32-byte `ObjectSlot`:
    //
    //     predicted with len:       40,000 x 16 B  =   640,000 B  =  16.0 B/key
    //     measured:              15,301,988 B - 14,253,412 B
    //                                           = 1,048,576 B  =  26.2 B/key   (382.2 B/key)
    //     explained by capacity:   40,040 / 0.875 = 45,760 -> 65,536 slots
    //                              65,536 x 16 B  = 1,048,576 B   -- the measured delta, to the byte
    //
    // So the length-based estimate was low by 1.64x and the change breached this ceiling by 22.2
    // B/key when it was predicted to fit. The identity above is a very strong EXPLANATION of the
    // measured delta and is not a second independent measurement of it.
    //
    // AND IT CUTS BOTH WAYS, WHICH IS THE PART AN ESTIMATE USUALLY MISSES. Removing a whole map
    // recovers `capacity * entry_size`, also at capacity scale -- so a consolidation that widens a
    // value and deletes a table is paying and recovering at the SAME multiplier. Only the key
    // copies it saves scale with occupancy. That is why widening a value in place is a regression
    // while widening it as part of a merge need not be, and a net prediction that mixes the two
    // scales is wrong in a direction that flatters the change.
    const CEILING_B_PER_KEY: f64 = 407.1;
    const FLOOR_B_PER_KEY: f64 = 385.1;
    const CEILING_B_PER_ADDRESS: f64 = 203.6;
    const FLOOR_B_PER_ADDRESS: f64 = 192.6;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), SHIPPED_END);
    probe_seed(
        &engine,
        PINNED_STRINGS,
        PINNED_SERIES,
        PINNED_POINTS,
        PINNED_DEADLINES,
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // NON-VACUITY: the corpus really carries the deadlines it was asked for. Without this, a seed
    // that silently stopped setting them would quietly restore the old one-sided fixture and every
    // bound below would be re-derived on a corpus nobody checked.
    assert_eq!(
        PINNED_DEADLINES,
        shard.expires_at_ms.len(),
        "the fixture seeded {PINNED_DEADLINES} deadlines and the shard holds {}; the bracket's          whole reason for carrying them is that this map is not empty",
        shard.expires_at_ms.len()
    );

    // --- THE POPULATION, PINNED. Each of these is a denominator or a regime control, and every
    // --- one of them is asserted before a single ratio is written.
    let string_keys = shard.strings.len();
    let series_map_keys = shard.features.len();
    let feature_points: usize = shard.features.values().map(|s| s.len()).sum();
    let bucket_count = shard.bucket_index.bucket_map.values().count();
    let bucket_pages: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|b| b.block_index.len())
        .sum();

    assert_eq!(
        PINNED_STRINGS, string_keys,
        "the fixture seeded {PINNED_STRINGS} single-page objects and the string map holds \
         {string_keys} -- the corpus is part of the bracket, so a figure taken over a different \
         one is not comparable to it"
    );
    assert_eq!(
        PINNED_SERIES, series_map_keys,
        "the fixture seeded {PINNED_SERIES} series and the feature map holds {series_map_keys}"
    );
    assert_eq!(
        PINNED_SERIES * PINNED_POINTS,
        feature_points,
        "the fixture seeded {} series points and the feature maps hold {feature_points}",
        PINNED_SERIES * PINNED_POINTS
    );
    assert_eq!(
        PINNED_BUCKETS, bucket_count,
        "the bracket was taken over {PINNED_BUCKETS} buckets and this fixture produced \
         {bucket_count} -- every per-bucket term divides by this, so the bracket does not apply"
    );
    // THE REGIME CONTROL. On the legacy range buckets and keys are the same number and the whole
    // figure reads 48% higher. A fixture that drifted onto that range would sail past the ceiling
    // for a reason that has nothing to do with any structure's cost.
    assert!(
        bucket_count < string_keys + series_map_keys,
        "{bucket_count} buckets for {} keys is one bucket per key, which is the LEGACY range -- \
         this bracket is only valid on the shipped 0..{SHIPPED_END}",
        string_keys + series_map_keys
    );
    assert!(bucket_pages > 0, "denominator: the bucket index holds no pages");

    // --- THE NUMERATOR: the whole live heap the shard index holds. The clone counts shared
    // --- payloads ONCE, so the two `Arc` terms are added back explicitly rather than being
    // --- double-counted or dropped.
    let whole_bytes = deep_heap_bytes(shard);
    let (arc_bytes, _arc_count) = arc_payload_bytes(&shard.bucket_index);
    let (dirty_arc_bytes, _dirty_arc_count, _shared) = dirty_arc_payload_bytes(shard);
    let total_index_heap_bytes = whole_bytes + arc_bytes + dirty_arc_bytes;
    assert!(
        total_index_heap_bytes > 0,
        "numerator: the index measured as costing nothing, which is a broken measurement and not a \
         free index"
    );

    // --- THE TWO RATIOS. Numerator and denominator are named ON THE LINE that divides them,
    // --- because every wrong figure this campaign has published was a denominator question.
    //
    // numerator: total live index heap, bytes. denominator: LIVE KEYS -- one per object, which is
    // 40,000 single-page objects plus 40 series maps. This is the denominator for a per-record cost.
    let live_keys = string_keys + series_map_keys;
    let bytes_per_live_key = total_index_heap_bytes as f64 / live_keys as f64;
    //
    // numerator: the SAME total live index heap, bytes. denominator: LIVE ADDRESSES -- one per
    // stored page, which is 40,000 object pages plus 40,000 series points. This is the denominator
    // for a per-page cost, and it is 2x the key count on this fixture, so the two ratios are not
    // interchangeable and neither is derived from the other.
    let live_addresses = string_keys + feature_points;
    let bytes_per_live_address = total_index_heap_bytes as f64 / live_addresses as f64;

    println!(
        "=== footprint bracket on 0..{SHIPPED_END}: {total_index_heap_bytes} B over {live_keys} \
         live keys and {live_addresses} live addresses, {bucket_count} buckets, {bucket_pages} \
         pages ==="
    );
    println!("  {bytes_per_live_key:.1} B/key   (bracket {FLOOR_B_PER_KEY}..={CEILING_B_PER_KEY})");
    println!(
        "  {bytes_per_live_address:.1} B/address   (bracket {FLOOR_B_PER_ADDRESS}..=\
         {CEILING_B_PER_ADDRESS})"
    );

    // --- THE CEILING: a regression. Named with the denominator, so the figure in the message can
    // --- be compared to the right thing.
    assert!(
        bytes_per_live_key <= CEILING_B_PER_KEY,
        "the shard index now costs {bytes_per_live_key:.1} B per live key \
         ({total_index_heap_bytes} B over {live_keys} keys), above the {CEILING_B_PER_KEY} B/key \
         ceiling. Either the change pays for itself somewhere this does not measure -- say where \
         -- or the footprint regressed"
    );
    assert!(
        bytes_per_live_address <= CEILING_B_PER_ADDRESS,
        "the shard index now costs {bytes_per_live_address:.1} B per live address \
         ({total_index_heap_bytes} B over {live_addresses} addresses), above the \
         {CEILING_B_PER_ADDRESS} B/address ceiling"
    );

    // --- THE FLOOR: either a win that has to be recorded, or a measurement that stopped
    // --- measuring. The message has to say both, because the number cannot tell them apart.
    assert!(
        bytes_per_live_key >= FLOOR_B_PER_KEY,
        "the shard index now costs {bytes_per_live_key:.1} B per live key \
         ({total_index_heap_bytes} B over {live_keys} keys), BELOW the {FLOOR_B_PER_KEY} B/key \
         floor. Two things look like this and they are not the same: a real saving, which should \
         move both bounds down and publish the new figure; or a structure that went empty or \
         stopped being reached, which measures as cheap because this counts what a CLONE \
         allocates. Establish which before lowering anything"
    );
    assert!(
        bytes_per_live_address >= FLOOR_B_PER_ADDRESS,
        "the shard index now costs {bytes_per_live_address:.1} B per live address \
         ({total_index_heap_bytes} B over {live_addresses} addresses), BELOW the \
         {FLOOR_B_PER_ADDRESS} B/address floor -- a saving to be recorded, or a term that stopped \
         being measured"
    );
}

// =================================================================================================
// WHAT THE DEADLINE MERGE WOULD TRADE, PRICED BEFORE IT IS BUILT
// =================================================================================================

/// THE TWO HALVES OF A DEADLINE, WEIGHED SEPARATELY, BECAUSE ONLY ONE OF THEM CAN BE RECLAIMED.
///
/// # THE QUESTION THIS ANSWERS
///
/// An `ObjectSlot` is sixteen bytes wider than the bare `ElementEntry` it replaces, and those bytes
/// are a `deadline`, `flags` and a `model_id` that no production path reads yet. The case for
/// paying them is that a later change puts a key's deadline IN the slot, so the key's entry in the
/// deadline maps can stop existing.
///
/// A deadline is held in TWO maps, not one:
///
///   * `expires_at_ms: BTreeMap<Arc<str>, u64>` -- key to deadline. This is the half a slot can
///     absorb: one value per key is exactly the shape the record carries.
///   * `expiry_by_deadline: BTreeMap<(u64, Arc<str>), ()>` -- the DEADLINE-ORDERED index. This half
///     a slot cannot absorb, and the reason is control flow rather than width: the expiry round
///     walks it in order and BREAKS as soon as it passes `now`, so it visits only the due keys. A
///     `HashMap` of slots has no order, so the same question becomes a scan of every live key per
///     round. `expiry_scale` already holds a guard for why a bounded round-robin cursor should not
///     replace an ordered index here, so this is a settled question being priced, not reopened.
///
/// BOTH OF THESE ARE `BTreeMap`s, so NEITHER takes the capacity multiplier that governs the paid
/// side. There is no power-of-two slot array and no load factor on the recovered side: it is
/// node-allocated, and this crate has measured separately that a `BTreeMap` holding one element
/// allocates a 280-byte root node. The recovered side is therefore ONE cost model, not two.
///
/// So the merge pays the slot's width on EVERY key and recovers only `expires_at_ms`, and only on
/// keys that actually carry a deadline. That asymmetry is the whole trade, and it has a break-even
/// deadline share rather than a yes or no answer.
///
/// # WHAT `deep_heap_bytes` COUNTS HERE, AND WHY IT IS THE RECOVERABLE TERM
///
/// That helper is `let copy = value.clone()` under the counting allocator, so it reports what
/// CLONING allocates. `Arc::clone` is a refcount bump and not an allocation, so cloning either of
/// these maps allocates B-tree node space and NOTHING for the key characters. The figures below
/// are node space only.
///
/// THAT IS EXACTLY THE RECOVERABLE TERM, AND IT IS A PROPERTY OF THIS DATA RATHER THAN LUCK. The
/// characters are allocated ONCE and held by BOTH maps: `set_expiry` does a single
/// `Arc::from(key)` and clones the handle into each -- its own comment reads "ONE ALLOCATION OF THE
/// CHARACTERS, HELD BY BOTH MAPS". So deleting `expires_at_ms` leaves the characters alive under
/// the ordered map's refcount, and a figure that excluded them is the figure this trade needs.
///
/// DO NOT REUSE THE HELPER THIS WAY ON A MAP WITH OWNED KEYS. A `BTreeMap<String, _>` or a
/// `HashMap<Box<str>, _>` allocates its key buffers on clone, and there the same call reports the
/// characters too. Reused on a map with SHARED keys without knowing that, it silently omits the
/// largest term and nothing warns you.
///
/// AND THESE FIGURES ARE NOT COMPARABLE TO A WHOLE-SHARD DELTA. The bracket's 94.4 B per deadline
/// is a difference of total shard heap -- characters plus both maps' nodes. The numbers here are
/// node space for one map. Printing the two beside each other would read as a comparison because
/// it would be formatted as one, so this prints neither a total nor the bracket figure.
///
/// # WHY THE COST TERM IS A CAPACITY IDENTITY AND NOT A SECOND MEASUREMENT
///
/// The widening costs `capacity * 16 B`, not `len * 16 B`: hashbrown allocates a slot per capacity
/// unit whether a key occupies it or not. At 40,000 string keys the table holds 65,536 slots, so
/// the cost is 65,536 x 16 = 1,048,576 B. That figure is quoted here as the identity it is; the
/// footprint bracket is what measures it, and the two agree to the byte.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds the bracket corpus under the counting allocator; run by name"]
fn what_the_deadline_merge_would_trade() {
    const STRINGS: usize = 40_000;
    const SERIES: usize = 40;
    const POINTS: usize = 1_000;
    const DEADLINES: usize = STRINGS / 2;
    /// The string table's capacity at this key count: the next power of two at or above
    /// `40_000 / 0.875`. The widening is charged here and not at the key count.
    const STRING_TABLE_CAPACITY: usize = 65_536;
    /// An `ObjectSlot` over the `ElementEntry` it replaces.
    const SLOT_WIDENING_B: usize = 16;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), 1023);
    probe_seed(&engine, STRINGS, SERIES, POINTS, DEADLINES);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // NON-VACUITY, BOTH WAYS. A corpus with no deadlines prices the recoverable half at zero and
    // reads as "the merge recovers nothing"; a corpus where every key has one prices it at its
    // maximum and reads as "the merge always wins". Neither is this fixture, and the assertions say
    // which it is before any figure is taken.
    assert_eq!(
        DEADLINES,
        shard.expires_at_ms.len(),
        "the fixture seeded {DEADLINES} deadlines and the keyed map holds {}",
        shard.expires_at_ms.len()
    );
    assert_eq!(
        DEADLINES,
        shard.expiry_by_deadline.len(),
        "the two deadline maps disagree on their length, so one of them is not being maintained \
         and neither figure below is about the state this prices"
    );
    assert!(
        shard.strings.len() > DEADLINES,
        "every key carries a deadline in this fixture, so the asymmetry this measures -- paying on \
         all keys, recovering on some -- is not present and the break-even below is meaningless"
    );

    let keyed_b = deep_heap_bytes(&shard.expires_at_ms);
    let ordered_b = deep_heap_bytes(&shard.expiry_by_deadline);
    let keys = shard.strings.len();

    let recoverable_per_deadline = keyed_b as f64 / DEADLINES as f64;
    let cost_b = (STRING_TABLE_CAPACITY * SLOT_WIDENING_B) as f64;
    let recovered_b = keyed_b as f64;
    let net_b = recovered_b - cost_b;
    let break_even_deadlines = cost_b / recoverable_per_deadline;

    println!("\n=== what a deadline costs, and which half a slot can take ===");
    println!("  corpus: {keys} string keys, {DEADLINES} of them with a deadline");
    println!(
        "  expires_at_ms      (RECOVERABLE) {keyed_b:>10} B   {:>7.1} B per deadline",
        recoverable_per_deadline
    );
    println!(
        "  expiry_by_deadline (MUST STAY)   {ordered_b:>10} B   {:>7.1} B per deadline",
        ordered_b as f64 / DEADLINES as f64
    );
    println!("\n=== the trade, at this deadline share ===");
    println!(
        "  paid:      {cost_b:>10.0} B   = {STRING_TABLE_CAPACITY} slots x {SLOT_WIDENING_B} B, on EVERY key"
    );
    println!("  recovered: {recovered_b:>10.0} B   = the keyed half, on {DEADLINES} keys");
    println!(
        "  net:       {net_b:>+10.0} B   ({:+.1} B/key over {keys} keys)",
        net_b / keys as f64
    );
    println!(
        "\n  break-even: {break_even_deadlines:.0} deadlines, which is {:.1}% of {keys} keys",
        100.0 * break_even_deadlines / keys as f64
    );
    println!(
        "  so the merge pays for itself only where more than {:.1}% of objects carry a deadline.",
        100.0 * break_even_deadlines / keys as f64
    );

    // THE ONE SHAPE CLAIM: the ordered half is not a rounding error. If it ever became negligible
    // the trade would be worth revisiting, because then the unrecoverable term would not matter.
    assert!(
        ordered_b * 3 > keyed_b,
        "the ordered index is {ordered_b} B against the recoverable {keyed_b} B -- under a third of \
         it. The term this measurement treats as UNRECOVERABLE has stopped being material next to \
         the one it blocks, so the merge's arithmetic should be re-derived rather than read off \
         this probe"
    );
}

/// Does any OTHER resident map hold its own copy of the key characters?
///
/// The dirty index stopped holding one (#2183) and the probe above reports its payload row at zero.
/// But that probe walks the BUCKET INDEX's allocations and the dirty index's, and nothing else --
/// so a third holder would be invisible to it, and its bytes would be missing from the total rather
/// than merely unattributed. `strings` and `context_nodes` are both keyed by `ModelKey`, which is
/// `Arc<str>`, so they are the obvious candidates.
///
/// THIS IS A COUNT, NOT A READING. It compares ALLOCATION IDENTITY -- the pointer -- against the
/// set the bucket index holds, because the characters are equal either way and equality cannot
/// witness sharing.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib which_resident_maps_hold_their_own_key_characters -- --ignored --nocapture --test-threads=1
#[test]
#[ignore = "seeds a corpus under the counting allocator; run by name"]
#[cfg(feature = "alloc-probe")]
fn which_resident_maps_hold_their_own_key_characters() {
    const RECORDS: usize = 8_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = probe_engine_on(dir.path(), crate::DEFAULT_END_ROUTING_BUCKET);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("own-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    // Every allocation the bucket index holds, by pointer.
    let mut bucket: HashSet<usize> = HashSet::new();
    walk_bucket_index_strings(&shard.bucket_index, &mut bucket);

    // `strings` is keyed by ModelKey = Arc<str>. Are those the same allocations?
    let mut strings_total = 0usize;
    let mut strings_shared = 0usize;
    let mut seen: HashSet<usize> = HashSet::new();
    for key in shard.strings.keys() {
        let ptr = key.as_ptr() as usize;
        if !seen.insert(ptr) {
            continue;
        }
        strings_total += 1;
        if bucket.contains(&ptr) {
            strings_shared += 1;
        }
    }

    let dirty_shared = {
        let mut shared = 0usize;
        let mut seen: HashSet<usize> = HashSet::new();
        for key in shard.dirty_objects.iter() {
            let ptr = key.as_ptr() as usize;
            if seen.insert(ptr) && bucket.contains(&ptr) {
                shared += 1;
            }
        }
        shared
    };

    println!();
    println!("  {RECORDS} string writes on the shipped routing range");
    println!("  bucket index holds            {} distinct key allocations", bucket.len());
    println!("  strings                       {strings_total} distinct, {strings_shared} shared with the bucket index");
    println!("  dirty index                   {dirty_shared} shared with the bucket index");
    println!();
    if strings_shared == strings_total && strings_total > 0 {
        println!("  -> `strings` shares every key: there is ONE copy and the probe's total is right.");
    } else {
        let own = strings_total - strings_shared;
        println!("  -> `strings` holds {own} allocations of its OWN, so the characters exist");
        println!("     {} times and the per-key total above is MISSING those bytes.",
                 1 + usize::from(own > 0));
    }

    // The dirty index's sharing is already guarded elsewhere; assert it here too so this test
    // fails loudly if the adoption regresses, rather than quietly reporting a different subject.
    assert!(dirty_shared > 0, "the dirty index shares nothing -- the key adoption has regressed");
    assert_eq!(RECORDS, strings_total, "strings should hold one key per record");
    assert_eq!(
        strings_total, strings_shared,
        "{strings_shared} of `strings`' {strings_total} key allocations are the same allocation          the bucket index holds, and EVERY ONE SHOULD BE. Five of its six insert sites used to          spell `key.as_str().into()`, allocating a second copy of characters the page index          already held; `ShardState::adopt_strings_key`, called once the page IS filed, closes          that. A shortfall here means the adoption stopped running and the duplicate is back --          and it will NOT show up in the per-key total, because a clone-based probe cannot see an          `Arc` payload."
    );
}

/// How many times a CONTEXT key's characters exist, across the maps a context write touches.
///
/// The one-copy work so far has been about the maps keyed by `ModelKey`/`Arc<str>`, which CAN
/// share an allocation. The rest are keyed by `String`, which always owns its characters --
/// so a key landing in several of them has one copy per map, by construction, and no amount of
/// adoption can fix that without changing the key type.
///
/// This counts the copies rather than assuming them. For a string workload only one of those maps
/// is populated, which is why the per-key figures elsewhere do not show this at all.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib how_many_times_a_context_keys_characters_exist -- --ignored --nocapture --test-threads=1
#[test]
#[ignore = "seeds context events; run by name"]
#[cfg(feature = "alloc-probe")]
fn how_many_times_a_context_keys_characters_exist() {
    const EVENTS: u64 = 400;

    let dir = tempfile::tempdir().unwrap();
    let engine = probe_engine_on(dir.path(), crate::DEFAULT_END_ROUTING_BUCKET);
    for index in 0..EVENTS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 7,
                node_hash: 1_000 + index,
                event: Box::new(crate::types::ContextEvent {
                    event_id_hash: 0xDEAD_0000_0000_0000 | index,
                    event_time_ms: 1_787_270_070_000 + index,
                    ingestion_time_ms: 1_787_270_070_000 + index,
                    kind: 9,
                    event_type: 2,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.7,
                    text: format!("event payload {index}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                first_write_only: false,
                cold_storage: false,
            },
        });
        assert!(response.status.ok, "event {index}: {:?}", response.status);

        // AND MARK BOTH FLAVOURS OF DIRTY, so `context_dirty_index` is populated rather than
        // empty. Without this the probe reported it at 0 keys and said nothing about whether its
        // keys can be shared -- which is the question, since it is one of the last two maps on
        // this shard still keyed by `String`.
        for command in [
            Command::ContextMarkSummaryDirty {
                tenant_hash: 7,
                node_hash: 1_000 + index,
                event_time_ms: 1_787_270_070_000 + index,
                reason: 1,
                propagate_depth: 0,
            },
            Command::ContextMarkEmbeddingDirty {
                tenant_hash: 7,
                node_hash: 1_000 + index,
                event_time_ms: 1_787_270_070_000 + index,
                reason: 1,
                propagate_depth: 0,
                clear: false,
            },
        ] {
            let marked = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(marked.status.ok, "mark {index}: {:?}", marked.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    // Every String-keyed map a context write can touch, by name, with the key TEXTS it holds.
    let holders: Vec<(&str, Vec<&str>)> = vec![
        ("context_events (Arc-keyed)", shard.context_events().keys().map(|k| &**k).collect()),
        ("context_event_timeline (Arc-keyed)", shard.context_event_timeline.keys().map(|k| &**k).collect()),
        ("context_indexes", shard.context_indexes.keys().map(|k| &**k).collect()),
        ("context_audits", shard.context_audits.keys().map(|k| &**k).collect()),
        ("context_dirty_index", shard.context_dirty_index.keys().map(|k| k.as_str()).collect()),
        ("context_entities", shard.context_entities.keys().map(|k| &**k).collect()),
        ("context_children", shard.context_children.keys().map(|k| &**k).collect()),
        ("context_summaries", shard.context_summaries.keys().map(|k| &**k).collect()),
        ("context_compressions", shard.context_compressions.keys().map(|k| &**k).collect()),
        ("context_compression_watermark (Arc-keyed)", shard.context_compression_watermark.keys().map(|k| &**k).collect()),
        ("context_nodes (Arc-keyed)", shard.context_nodes.keys().map(|k| &**k).collect()),
    ];

    let mut copies: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let mut holders_of: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    let mut characters = 0usize;
    println!();
    println!("  {EVENTS} context events written");
    println!("  map                                keys   chars held");
    for (name, keys) in &holders {
        let chars: usize = keys.iter().map(|k| k.len()).sum();
        if !keys.is_empty() {
            characters += chars;
        }
        println!("  {name:34} {:>5}   {chars:>9}", keys.len());
        for k in keys {
            *copies.entry(k).or_insert(0) += 1;
            holders_of.entry(k).or_insert_with(Vec::new).push(*name);
        }
    }
    let distinct = copies.len();
    // KEYS THAT EXIST IN EXACTLY ONE MAP CANNOT BE SHARED WITH ANYTHING, and that is the end of
    // the one-copy road rather than a gap in it. A key is shareable only if some OTHER structure
    // holds the same characters; the dirty keys carry their own prefixes (`ctx:dirty:`,
    // `ctx:embdirty:`), no other map is keyed by them, and they are never written to the block
    // store so the page index does not hold them either. One holder, one allocation, nothing to
    // adopt.
    let single_holder = copies.values().filter(|n| **n == 1).count();
    let total_entries: usize = holders.iter().map(|(_, k)| k.len()).sum();
    let mut by_count: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
    for n in copies.values() {
        *by_count.entry(*n).or_insert(0) += 1;
    }
    println!();
    println!("  distinct key texts: {distinct}; total map entries: {total_entries}");
    println!("  key texts held by exactly ONE map: {single_holder} -- nothing to share with");
    for (n, how_many) in &by_count {
        println!("    {how_many} key texts exist in {n} map(s)");
    }
    println!("  characters held across all of them: {characters} B");
    if distinct > 0 {
        println!(
            "  so {:.2} copies per distinct key, {:.1} B/key of characters",
            total_entries as f64 / distinct as f64,
            characters as f64 / distinct as f64
        );
    }
    println!();
    println!("  The rows marked Arc-keyed CAN hold one allocation between them, which is what");
    println!("  the count below measures. A String-keyed row cannot: each of its entries owns");
    println!("  its own characters no matter what any other map holds.");

    // AND NOW BY ALLOCATION, WHICH IS A DIFFERENT QUESTION. The count above is a count of map
    // ENTRIES per distinct key TEXT, so it cannot tell two maps holding ONE allocation from two
    // maps holding two equal copies -- and that difference is the entire point of keying these two
    // by `ModelKey`. Contents cannot tell them apart; pointers can.
    let mut bucket: HashSet<usize> = HashSet::new();
    walk_bucket_index_strings(&shard.bucket_index, &mut bucket);
    let event_series_allocations: HashSet<usize> = shard
        .context_events()
        .keys()
        .map(|k| k.as_ptr() as usize)
        .collect();
    let mut timeline_total = 0usize;
    let mut shared_with_series = 0usize;
    let mut shared_with_bucket = 0usize;
    for key in shard.context_event_timeline.keys() {
        let ptr = key.as_ptr() as usize;
        timeline_total += 1;
        if event_series_allocations.contains(&ptr) {
            shared_with_series += 1;
        }
        if bucket.contains(&ptr) {
            shared_with_bucket += 1;
        }
    }
    println!();
    println!("  BY ALLOCATION, not by text:");
    println!("    time index keys                   {timeline_total:>5}");
    println!("    same allocation as the series map {shared_with_series:>5}");
    println!("    same allocation as bucket index   {shared_with_bucket:>5}");

    // IS THERE A THIRD COPY AT ALL? A zero on the line above is AMBIGUOUS, and the two readings
    // need different work: the page index might hold an equal-but-distinct copy of this key, or it
    // might not hold the key yet at all. An adoption route exists only in the first case. Asking
    // the lookup directly is the only way to tell, so ask it.
    let mut lookup_holds = 0usize;
    let mut lookup_distinct = 0usize;
    for key in shard.context_event_timeline.keys() {
        if let Some(canonical) = shard
            .bucket_index
            .object_block_lookup
            .any_shared_object_key(&**key)
        {
            lookup_holds += 1;
            if canonical.as_ptr() as usize != key.as_ptr() as usize {
                lookup_distinct += 1;
            }
        }
    }
    println!("    page index holds the key          {lookup_holds:>5}");
    println!("    ... as a DISTINCT allocation      {lookup_distinct:>5}");
    println!(
        "    adoption: {} calls, {} found no handle, {} moved",
        crate::engine::state::CONTEXT_EVENT_ADOPT_CALLS.load(std::sync::atomic::Ordering::Relaxed),
        crate::engine::state::CONTEXT_EVENT_ADOPT_NO_HANDLE
            .load(std::sync::atomic::Ordering::Relaxed),
        crate::engine::state::CONTEXT_EVENT_ADOPT_MOVED.load(std::sync::atomic::Ordering::Relaxed),
    );

    // ASSERTED IN BOTH DIRECTIONS, because one direction alone is not informative. "The pair
    // shares one allocation" was ALREADY true while a second, equal allocation sat in the page
    // index -- so it passing says nothing about the page index. The second assertion is the one
    // that catches a leftover copy: no key the lookup holds may be a DISTINCT allocation.
    //
    // The adoption counters are printed and deliberately NOT asserted: they are process-global,
    // so another test running in the same binary can move them, and a count that another test can
    // change cannot carry a verdict about this one.
    assert_eq!(
        timeline_total, shared_with_bucket,
        "{shared_with_bucket} of the time index's {timeline_total} key allocations are the same          allocation the PAGE INDEX holds, and every one should be. The pair files its maps during          execution, when the lookup does not yet hold the key, so the adoption after the bucket          index is filed is what closes it. A shortfall means that adoption stopped running or          moved to a point before the handle exists -- which is how it failed the first time, on          400 of 400 writes."
    );
    assert_eq!(
        0, lookup_distinct,
        "{lookup_distinct} of the keys the page index holds are a DISTINCT allocation from the          one the event maps hold, and none should be. This is the assertion that a passing          pair-sharing check cannot make: the pair shared one allocation while a second equal copy          sat in the lookup, and only comparing against the lookup finds it."
    );
    assert_eq!(
        timeline_total, shared_with_series,
        "{shared_with_series} of the time index's {timeline_total} key allocations are the same \
         allocation the event series map holds, and EVERY ONE SHOULD BE: both maps are filed at one \
         site from one handle. A shortfall means some caller went back to minting its own key, which \
         puts a second copy of every event key's characters back -- and it will NOT show up in the \
         per-key total, because a clone-based probe cannot see an `Arc` payload."
    );

    // THE GUARD THAT KEEPS ONE-COPY TRUE AS MAPS ARE ADDED.
    //
    // A key is shareable only when some OTHER structure holds the same characters, so the rule is
    // NOT "every map should be `Arc`-keyed". The 800 dirty keys above live in exactly one map and
    // have nothing to share with, which is why `context_dirty_index` keeping its `String` key
    // costs nothing at all. The rule is: if a key text is held by MORE THAN ONE map, every holder
    // must be able to share, or those characters exist twice -- and no clone-based byte probe will
    // say so, because cloning an `Arc` is a refcount bump.
    //
    // So this fails when a `String`-keyed map starts holding a key another map already holds,
    // which is exactly how a duplicate gets reintroduced after the adoption work.
    let mut unshareable_duplicates: Vec<(&str, Vec<&str>)> = holders_of
        .iter()
        .filter(|(_, holders)| holders.len() > 1)
        .filter(|(_, holders)| holders.iter().any(|name| !name.contains("Arc-keyed")))
        .map(|(key, holders)| (*key, holders.clone()))
        .collect();
    unshareable_duplicates.sort();
    assert!(
        unshareable_duplicates.is_empty(),
        "{} key texts are held by several maps where at least one CANNOT share, so those          characters exist more than once: {:?}. Either the new holder takes a `ModelKey` and joins          `adopt_object_key_allocations`, or it should not be keyed by the object key at all.",
        unshareable_duplicates.len(),
        &unshareable_duplicates[..unshareable_duplicates.len().min(4)]
    );

    assert!(
        distinct > 0,
        "no context keys landed -- the write path did not populate these maps"
    );
}

/// WHAT A PER-SHARD RESIDENT BOUND RECOVERS, AND THAT ITS SIGNAL IS THIS SHARD'S OWN BYTES.
///
/// `what_a_dump_then_release_recovers_on_the_shipped_routing_range` proves a dump-then-release
/// recovers the bulk of the derived index. It drives that sequence by hand, with
/// `memory_pressure_threshold = 0`, which is not a trigger an operator can deploy: the gate it
/// passes through sums four PROCESS-WIDE cache terms with one per-shard index term, so a constant
/// threshold fires on an idle shard whenever a busy one shares the process. That is measured in
/// `the_evict_gates_cache_terms_are_process_wide_not_shard` and is why `enable_evict` defaults off.
///
/// `apply_index_resident_bound` narrows the signal to `bucket_index_resident_bytes`, which is
/// per-shard and is the only term a release can reduce. This measures what that bound recovers AND
/// pins that it read the narrow signal -- `pressure_before` must equal this shard's own index
/// bytes, not a process-wide sum.
///
/// THE GUARD IS BIDIRECTIONAL, because "it fired" alone is satisfied by a gate that always fires:
/// a bound of 0 must report disabled, a bound ABOVE this shard's bytes must refuse, and only a
/// bound BELOW them may release anything.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_per_shard_resident_bound_recovers -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 40,000 records then dumps and releases under the counting allocator; run by name"]
fn what_a_per_shard_resident_bound_recovers() {
    const SHIPPED_END: u32 = 1023;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), SHIPPED_END);
    probe_seed(&engine, 40_000, 40, 1_000, 0);

    let probe_keys: Vec<String> = (0..16).map(|i| format!("s{i}")).collect();
    let readable = |engine: &TemporalEngine| -> usize {
        probe_keys
            .iter()
            .filter(|key| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key: (*key).clone() },
                        })
                        .response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count()
    };
    let snapshot = |engine: &TemporalEngine| -> (u64, u64, u64, u64, usize, usize, usize, usize) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        (
            deep_heap_bytes(&shard.bucket_index.bucket_map),
            deep_heap_bytes(&shard.bucket_index.object_block_lookup),
            deep_heap_bytes(&shard.dirty_objects),
            deep_heap_bytes(shard),
            shard.strings.len() + shard.features.len(),
            shard.bucket_index.bucket_map.values().count(),
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|b| b.block_index.len())
                .sum(),
            shard.bucket_index.released_buckets.len(),
        )
    };

    // THE WHOLE-INDEX TOTAL, COMPUTED THE WAY THE COMPOSITION PROBE COMPUTES IT, so the figure
    // after the bound is a MEASUREMENT of the same quantity rather than a subtraction of line
    // items from a number taken in another run. Same three terms, same helpers: the whole
    // `ShardState` plus each side's `Arc<str>` payloads counted once each.
    let index_total = |engine: &TemporalEngine| -> (u64, u64, u64, u64) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let whole = deep_heap_bytes(shard);
        let (arc, _n) = arc_payload_bytes(&shard.bucket_index);
        let (dirty_arc, _m, _s) = dirty_arc_payload_bytes(shard);
        (whole + arc + dirty_arc, whole, arc, dirty_arc)
    };
    // Strings hold one address each and each series holds its points. The model maps keep every
    // address across a release, so this denominator does not move with the bound.
    const LIVE_ADDRESSES: u64 = 40_000 + 40 * 1_000;

    // THE INDEX AT THE SAME SCOPE THE COMPARISON FIGURE USES, which is the only way the two
    // numbers mean anything beside each other. That figure is an INDEX NODE per key -- a tree node
    // wrapping a slot struct, reported by the allocator -- and it does NOT include the data the
    // records hold. Our `TOTAL live heap` row DOES: `strings` and `features` are the model maps,
    // the records themselves, and on this fixture they are most of it. Putting a data-inclusive
    // total beside a node-only figure is the scope error that gets caught every time it is made.
    //
    // So this sums only the per-key INDEX structures: the whole bucket index measured in one call
    // (its map, its object lookup and the rest of `CoreIndex`), the dirty index, the recency row,
    // and the key payloads each side holds once.
    let index_only = |engine: &TemporalEngine| -> (u64, u64, u64, u64, u64) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let bucket_index = deep_heap_bytes(&shard.bucket_index);
        let dirty = deep_heap_bytes(&shard.dirty_objects);
        let recency = deep_heap_bytes(&shard.bucket_recency);
        let (arc, _n) = arc_payload_bytes(&shard.bucket_index);
        let (dirty_arc, _m, _s) = dirty_arc_payload_bytes(shard);
        (
            bucket_index + dirty + recency + arc + dirty_arc,
            bucket_index,
            dirty,
            recency,
            arc + dirty_arc,
        )
    };

    let (bm0, lk0, dr0, whole0, keys, buckets0, pages0, released0) = snapshot(&engine);
    let resident0 = engine.bucket_index_resident_bytes(1);
    let read0 = readable(&engine);
    let (total0, w0, a0, d0) = index_total(&engine);
    let (idx0, bi0, di0, rc0, ap0) = index_only(&engine);

    assert_eq!(40_040, keys, "denominator: the fixture did not seed the corpus it reports");
    assert!(buckets0 < keys, "denominator: one bucket per key means this is the LEGACY range");
    assert_eq!(0, released0, "denominator: the fixture starts with nothing released");
    assert_eq!(probe_keys.len(), read0, "the fixture cannot read its own keys BEFORE the sequence");
    assert!(resident0 > 0, "denominator: a zero resident figure cannot be bounded");

    println!("=== before: {keys} keys, {buckets0} buckets, {pages0} pages ===");
    println!("  this shard's own index resident bytes: {resident0}");

    // --- CONTROL 1: a bound of zero is disabled and walks nothing. ---
    let disabled = engine.apply_index_resident_bound(1, 0, 0);
    assert_eq!(
        "index_resident_bound_disabled", disabled.skipped_reason,
        "a bound of 0 must mean disabled, so this is free to call on a deployment that set none"
    );
    assert_eq!(0, disabled.bucket_index_buckets_released, "a disabled bound released something");

    // --- CONTROL 2: a bound ABOVE this shard's own bytes must refuse. ---
    //
    // This is the assertion that separates a narrow gate from the wide one. The wide gate handed
    // this same number would OPEN, because its pressure is dominated by cache terms a release
    // cannot reduce; the narrow gate sees only the index and declines.
    let above = resident0.saturating_mul(2);
    let refused = engine.apply_index_resident_bound(1, above, 0);
    assert_eq!(
        "index_resident_below_bound", refused.skipped_reason,
        "a bound of {above} B is above this shard's {resident0} B of index and must refuse"
    );
    assert_eq!(0, refused.bucket_index_buckets_released, "a refused bound released something");
    assert_eq!(
        resident0, refused.pressure_before,
        "the gate reported {} B of pressure where this shard's index is {resident0} B -- it read a \
         signal wider than the shard, which is the contamination this entry point exists to avoid",
        refused.pressure_before
    );

    // --- AND THE BOUND THAT FIRES: below this shard's own bytes. ---
    let bound = resident0 / 2;
    let report = engine.apply_index_resident_bound(1, bound, 0);
    assert!(
        report.skipped_reason.is_empty(),
        "a bound of {bound} B under {resident0} B of index should have opened, not skipped on {:?}",
        report.skipped_reason
    );
    assert_eq!(
        resident0, report.pressure_before,
        "the firing gate read {} B where this shard's index is {resident0} B",
        report.pressure_before
    );

    let (bm1, lk1, dr1, whole1, keys1, buckets1, pages1, released1) = snapshot(&engine);
    let resident1 = engine.bucket_index_resident_bytes(1);
    let read1 = readable(&engine);

    println!("=== after the bound fired: {buckets1} buckets, {pages1} pages, {released1} released ===");
    let row = |name: &str, before: u64, after: u64| {
        println!(
            "  {name:<28} {before:>10} -> {after:>10} B   recovered {:>10} B  {:>8.1} B/key  {:>6.1}%",
            before.saturating_sub(after),
            before.saturating_sub(after) as f64 / keys as f64,
            if before == 0 { 0.0 } else { 100.0 * before.saturating_sub(after) as f64 / before as f64 }
        );
    };
    row("bucket_map", bm0, bm1);
    row("object_block_lookup", lk0, lk1);
    row("dirty_objects", dr0, dr1);
    row("whole ShardState", whole0, whole1);
    println!("  modelled resident {resident0} -> {resident1} B, bound was {bound} B");
    println!("  pages {pages0} -> {pages1}, buckets {buckets0} -> {buckets1}, keys {keys} -> {keys1}");
    println!("  reads: {read0} of {} before, {read1} after", probe_keys.len());

    // --- THE RE-MEASURED WHOLE-INDEX TOTAL, which is the figure that answers "how big is the
    // index now" rather than "how much did these three rows give back". ---
    let (total1, w1, a1v, d1) = index_total(&engine);
    println!();
    println!("  WHOLE-INDEX TOTAL, re-measured rather than derived:");
    println!(
        "    before    {total0:>10} B   {:>8.1} B/key   {:>8.1} B/address   (whole {w0}, arc {a0}, dirty-arc {d0})",
        total0 as f64 / keys as f64,
        total0 as f64 / LIVE_ADDRESSES as f64
    );
    println!(
        "    after     {total1:>10} B   {:>8.1} B/key   {:>8.1} B/address   (whole {w1}, arc {a1v}, dirty-arc {d1})",
        total1 as f64 / keys as f64,
        total1 as f64 / LIVE_ADDRESSES as f64
    );
    println!(
        "    recovered {:>10} B   {:>8.1} B/key   {:>6.1}% of the whole index",
        total0.saturating_sub(total1),
        total0.saturating_sub(total1) as f64 / keys as f64,
        100.0 * total0.saturating_sub(total1) as f64 / total0 as f64
    );
    // --- AND THE INDEX AT THE COMPARISON'S OWN SCOPE: no model data. ---
    let (idx1, bi1, di1, rc1, ap1) = index_only(&engine);
    println!();
    println!("  INDEX ONLY -- per-key index structures, excluding the model data maps:");
    println!(
        "    before    {idx0:>10} B   {:>8.1} B/key   (bucket index {bi0}, dirty {di0}, recency {rc0}, key payloads {ap0})",
        idx0 as f64 / keys as f64
    );
    println!(
        "    after     {idx1:>10} B   {:>8.1} B/key   (bucket index {bi1}, dirty {di1}, recency {rc1}, key payloads {ap1})",
        idx1 as f64 / keys as f64
    );
    println!(
        "    the model data this EXCLUDES, for scope: strings + features = {} B ({:.1} B/key)",
        total1.saturating_sub(idx1).saturating_sub(0),
        total1.saturating_sub(idx1) as f64 / keys as f64
    );
    assert!(
        idx1 < idx0,
        "the index-only subtotal did not fall ({idx0} -> {idx1})"
    );

    assert!(
        total1 < total0,
        "the whole-index total did not fall ({total0} -> {total1}), so the per-term recoveries          above did not add up to anything at the index level"
    );

    // --- CORRECTNESS, NOT NEGOTIABLE. ---
    assert_eq!(
        probe_keys.len(),
        read1,
        "the bound made {} of {} keys unreadable, which makes every figure above worthless",
        probe_keys.len() - read1,
        probe_keys.len()
    );
    assert_eq!(keys, keys1, "the key count moved, which is data loss rather than reclaim");

    // --- NON-VACUITY: every assertion above is satisfied by a bound that recovered nothing. ---
    assert!(
        released1 > 0,
        "the bound opened and released nothing, so this measured the bare-release case"
    );
    assert!(
        resident1 < resident0,
        "the bound opened, released {released1} buckets, and this shard's own resident figure did \
         not fall ({resident0} -> {resident1}) -- so the quantity the bound is written against is \
         not the quantity the release reduces, which would make the bound unable to ever satisfy \
         itself"
    );
}

/// WHAT THE FEATURE SERIES WOULD COST AS AN OWNED ARRAY INSTEAD OF A B-TREE.
///
/// `features` is the largest single row left in the index after the resident bound fires -- 49.1
/// B/key at a 40,040-key corpus -- and it holds 40 series of 1,000 addresses each. The row is
/// CONTAINER, not payload: a point is `(u64, ElementEntry)` and `ElementEntry` is 16 bytes, so the
/// payload is 24 bytes a point and the B-tree charges about twice that.
///
/// This prices the swap before anyone pays for it. The conversion is not a type swap --
/// `OwnedTimestampedSeries` deliberately has no serde impl, so an adopting map needs a
/// compatibility field and a decode fold -- so the saving has to be worth that before the slice is
/// worth writing. Measured here rather than computed from `size_of`, because the allocator's
/// rounding and the B-tree's fill are exactly what a `size_of` account gets wrong.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn what_the_feature_series_would_cost_as_an_owned_array() {
    const POINTS: u64 = 1_000;
    const SERIES: usize = 40;
    const LIVE_KEYS: f64 = 40_040.0;

    let point = |i: u64| -> (u64, ElementEntry) {
        (i, ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i)))
    };

    let mut trees: Vec<BTreeMap<u64, ElementEntry>> = Vec::with_capacity(SERIES);
    for _ in 0..SERIES {
        let mut tree = BTreeMap::new();
        for i in 0..POINTS {
            let (at, address) = point(i);
            tree.insert(at, address);
        }
        trees.push(tree);
    }
    let mut arrays: Vec<crate::engine::timestamped_series::OwnedTimestampedSeries> =
        Vec::with_capacity(SERIES);
    for _ in 0..SERIES {
        let mut array = crate::engine::timestamped_series::OwnedTimestampedSeries::default();
        for i in 0..POINTS {
            let (at, address) = point(i);
            array.insert(at, address);
        }
        arrays.push(array);
    }

    let tree_bytes = deep_heap_bytes(&trees);
    let array_bytes = deep_heap_bytes(&arrays);
    let points = SERIES as f64 * POINTS as f64;
    println!();
    println!("  {SERIES} series x {POINTS} points, payload is 24 B a point:");
    println!(
        "    BTreeMap      {tree_bytes:>10} B   {:>6.1} B/point   {:>6.1} B/key at 40,040 keys",
        tree_bytes as f64 / points,
        tree_bytes as f64 / LIVE_KEYS
    );
    println!(
        "    owned array   {array_bytes:>10} B   {:>6.1} B/point   {:>6.1} B/key at 40,040 keys",
        array_bytes as f64 / points,
        array_bytes as f64 / LIVE_KEYS
    );
    println!(
        "    the swap is worth {:>6.1} B/key, taking the measured features row from 49.1 to {:.1}",
        (tree_bytes.saturating_sub(array_bytes)) as f64 / LIVE_KEYS,
        array_bytes as f64 / LIVE_KEYS
    );

    assert_eq!(16, std::mem::size_of::<ElementEntry>(), "the address width moved");
    assert!(
        array_bytes < tree_bytes,
        "the owned array ({array_bytes} B) is not smaller than the B-tree ({tree_bytes} B), so the \
         conversion would be churn -- and the compatibility field and decode fold it needs are not \
         free"
    );
}

/// WHAT THE `strings` MAP COSTS, AND WHERE ITS 54.0 B/KEY ACTUALLY GOES.
///
/// After the resident bound fires, `strings` is the largest row left in the index -- 54.0 B/key at
/// a 40,040-key corpus -- and the entry it holds is 32 bytes: a 16-byte `Arc<str>` fat pointer and
/// a 16-byte `ElementEntry`. So the row is 1.69x its own payload, and the question this answers is
/// whether that is distributed slack a different container would avoid, or something intrinsic.
///
/// THE ANSWER IS CAPACITY ROUNDING, and it is worth pricing rather than guessing at: a hash table
/// rounds to a power of two, so 40,000 entries live in 65,536 slots at a 61% load and the unused
/// 39% is paid in full. A B-tree has no power-of-two step; it pays node headers instead. Which
/// wins is an empirical question about THESE widths, and a previous swap of this kind measured a
/// 1.93x REGRESSION, so the measurement comes before the change.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn where_the_string_maps_bytes_go_and_whether_a_btree_would_be_smaller() {
    const KEYS: usize = 40_000;
    const LIVE_KEYS: f64 = 40_040.0;

    let entry = |i: usize| -> (std::sync::Arc<str>, ElementEntry) {
        (
            std::sync::Arc::from(format!("own-{i:08}").as_str()),
            ElementEntry::from_parts(1, i as u64 * 64, 64, Some(0), Some(i as u64)),
        )
    };

    let mut hash: HashMap<std::sync::Arc<str>, ElementEntry> = HashMap::new();
    for i in 0..KEYS {
        let (key, address) = entry(i);
        hash.insert(key, address);
    }
    let mut presized: HashMap<std::sync::Arc<str>, ElementEntry> = HashMap::with_capacity(KEYS);
    for i in 0..KEYS {
        let (key, address) = entry(i);
        presized.insert(key, address);
    }
    let mut tree: BTreeMap<std::sync::Arc<str>, ElementEntry> = BTreeMap::new();
    for i in 0..KEYS {
        let (key, address) = entry(i);
        tree.insert(key, address);
    }

    // Measured WITHOUT the key payloads, which every candidate holds identically: a clone-based
    // probe cannot see an `Arc` payload anyway, so what comes back is the table or the nodes.
    let hash_bytes = deep_heap_bytes(&hash);
    let presized_bytes = deep_heap_bytes(&presized);
    let tree_bytes = deep_heap_bytes(&tree);
    let payload = (KEYS * (std::mem::size_of::<std::sync::Arc<str>>()
        + std::mem::size_of::<ElementEntry>())) as f64;

    println!();
    println!("  {KEYS} entries, payload is {} B each ({payload:.0} B total):",
        std::mem::size_of::<std::sync::Arc<str>>() + std::mem::size_of::<ElementEntry>());
    for (name, bytes, capacity) in [
        ("HashMap, grown", hash_bytes, Some(hash.capacity())),
        ("HashMap, pre-sized", presized_bytes, Some(presized.capacity())),
        ("BTreeMap", tree_bytes, None),
    ] {
        println!(
            "    {name:<20} {bytes:>10} B   {:>6.1} B/key at 40,040   {:>5.2}x payload{}",
            bytes as f64 / LIVE_KEYS,
            bytes as f64 / payload,
            match capacity {
                Some(c) => format!("   capacity {c}, load {:.0}%", 100.0 * KEYS as f64 / c as f64),
                None => String::new(),
            }
        );
    }
    let best = tree_bytes.min(presized_bytes);
    println!(
        "    the best alternative is worth {:>5.1} B/key against the grown table",
        (hash_bytes.saturating_sub(best)) as f64 / LIVE_KEYS
    );

    assert_eq!(16, std::mem::size_of::<ElementEntry>(), "the address width moved");
    assert!(hash_bytes > 0 && tree_bytes > 0, "a zero figure cannot be compared");
}

/// WHAT ONE KEY WITH ONE PAGE COSTS, which is the denominator a per-key index figure uses.
///
/// Every figure in this file so far is taken on a fixture that seeds 40,000 strings AND 40 series
/// of 1,000 points -- about TWO stored addresses per live key. A per-key index cost measured there
/// is not comparable to one measured on a corpus of single-page objects, because half the addresses
/// belong to forty keys. The honest matched measurement is one address per key, and this is it.
///
/// No feature series at all, so every live key owns exactly one page: `live_keys == pages`, which
/// is asserted rather than assumed because the whole figure divides by it.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_one_key_with_one_page_costs -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 40,000 single-page records then bounds the index; run by name"]
fn what_one_key_with_one_page_costs() {
    const SHIPPED_END: u32 = 1023;
    const KEYS: usize = 40_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), SHIPPED_END);
    probe_seed(&engine, KEYS, 0, 0, 0);

    let probe_keys: Vec<String> = (0..16).map(|i| format!("s{i}")).collect();
    let readable = |engine: &TemporalEngine| -> usize {
        probe_keys
            .iter()
            .filter(|key| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key: (*key).clone() },
                        })
                        .response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count()
    };
    let total = |engine: &TemporalEngine| -> (u64, usize, usize) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let whole = deep_heap_bytes(shard);
        let (arc, _n) = arc_payload_bytes(&shard.bucket_index);
        let (dirty_arc, _m, _s) = dirty_arc_payload_bytes(shard);
        let pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|b| b.block_index.len())
            .sum();
        (whole + arc + dirty_arc, shard.strings.len(), pages)
    };

    let (before, keys0, pages0) = total(&engine);
    let read0 = readable(&engine);
    assert_eq!(KEYS, keys0, "denominator: the fixture did not seed the corpus it reports");
    assert_eq!(
        keys0, pages0,
        "denominator: {pages0} pages over {keys0} keys is not one page per key, so this is not the \
         single-page regime it claims to measure"
    );
    assert_eq!(probe_keys.len(), read0, "the fixture cannot read its own keys before the bound");
    assert_eq!(
        0,
        engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&1)
            .expect("shard")
            .features
            .len(),
        "denominator: a feature series would put a second address on some key"
    );

    let resident = engine.bucket_index_resident_bytes(1);
    let report = engine.apply_index_resident_bound(1, resident / 2, 0);
    assert!(report.skipped_reason.is_empty(), "the bound did not open: {:?}", report.skipped_reason);

    let (after, keys1, pages1) = total(&engine);
    let read1 = readable(&engine);

    println!();
    println!("  ONE KEY, ONE PAGE -- {keys0} keys, {pages0} pages before, {pages1} after");
    println!(
        "    before   {before:>10} B   {:>6.1} B/key   (= B/page, one page a key)",
        before as f64 / keys0 as f64
    );
    println!(
        "    after    {after:>10} B   {:>6.1} B/key   recovered {:>6.1} B/key, {:>5.1}%",
        after as f64 / keys0 as f64,
        before.saturating_sub(after) as f64 / keys0 as f64,
        100.0 * before.saturating_sub(after) as f64 / before as f64
    );
    println!("    reads {read0} of {} before, {read1} after", probe_keys.len());

    assert_eq!(probe_keys.len(), read1, "the bound made keys unreadable");
    assert_eq!(keys0, keys1, "the key count moved, which is data loss rather than reclaim");
    assert!(after < before, "the total did not fall ({before} -> {after})");
}

/// WHAT THE DEFAULT CONFIGURATION ACTUALLY DOES TO A 40,000-KEY SHARD, which is not the same
/// question as what the bound delivers when it fires.
///
/// `what_one_key_with_one_page_costs` fires the bound DIRECTLY, at half the shard's own resident
/// index, and measures 76.8 B/key afterwards. That is what the mechanism is worth. It is NOT what
/// a deployment running defaults sees, and conflating the two would be the more flattering of the
/// two readings -- so this runs the default cycle, untouched, and records the verdict.
///
/// THE DEFAULT IS A CAP, NOT A SCHEDULE. It bounds the resident index at 64 MiB a shard. A
/// 40,000-key single-page shard holds about 1.4 MB of it, so the default DECLINES here, and that
/// is correct: a cap that fired at 2% of its limit would be a thrash, not a bound. The figure a
/// deployment at this size sees is therefore the unbounded one, and an operator wanting the
/// bounded figure sets the limit below that shard's own resident index.
///
/// Written down as a test rather than a comment because the alternative is a claim in a commit
/// message that nothing checks.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds 40,000 single-page records and runs the default cycle; run by name"]
fn what_the_default_configuration_does_to_a_forty_thousand_key_shard() {
    const SHIPPED_END: u32 = 1023;
    const KEYS: usize = 40_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on(dir.path(), SHIPPED_END);
    probe_seed(&engine, KEYS, 0, 0, 0);

    let total = |engine: &TemporalEngine| -> u64 {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let (arc, _n) = arc_payload_bytes(&shard.bucket_index);
        let (dirty_arc, _m, _s) = dirty_arc_payload_bytes(shard);
        deep_heap_bytes(shard) + arc + dirty_arc
    };

    let defaults = crate::engine::reports::StorageManagerCycleRequest::default();
    let limit = defaults.index_resident_limit_bytes;
    let resident = engine.bucket_index_resident_bytes(1);
    let before = total(&engine);

    println!();
    println!("  DEFAULT CONFIGURATION on a {KEYS}-key single-page shard:");
    println!("    default index_resident_limit_bytes : {limit} B ({} MiB)", limit / (1024 * 1024));
    println!("    this shard's own resident index    : {resident} B");
    println!(
        "    so the default {} here",
        if resident >= limit { "FIRES" } else { "DECLINES" }
    );

    let report = engine.apply_index_resident_bound(1, limit, defaults.eviction_batch_limit);
    let after = total(&engine);
    println!("    verdict                            : {:?}", report.skipped_reason);
    println!(
        "    index total                        : {before} -> {after} B   ({:.1} -> {:.1} B/key)",
        before as f64 / KEYS as f64,
        after as f64 / KEYS as f64
    );
    println!(
        "    the shard would have to hold about {:.0}x this many keys for the default to fire",
        limit as f64 / resident as f64
    );

    // THE HONEST PAIR, asserted so neither half can be quoted without the other.
    assert!(limit > 0, "the default bound is disabled, so nothing bounds a growing index");
    assert!(
        resident < limit,
        "this corpus is at or above the default bound, so the default DOES fire here and the \
         prose above is wrong"
    );
    assert_eq!(
        "index_resident_below_bound", report.skipped_reason,
        "the default declined for a reason other than being below the bound"
    );
    assert_eq!(before, after, "the default declined and yet the index moved");
}

/// WHY THERE ARE TWENTY-SIX PER-KEY MAPS AND NOT ONE, measured on one key written four ways.
///
/// The consolidation question is not "could these maps be merged" but "would merging them save
/// anything", and that turns entirely on whether one KEY TEXT is held by several of them. A record
/// folded into the consolidated map pays `ObjectSlot` at 32 bytes where a bare address map pays
/// `ElementEntry` at 16 -- so consolidating a key that lives in exactly ONE map makes it BIGGER by
/// 16 bytes, and only a key living in several maps gets smaller by losing the other entries.
///
/// So this writes the SAME raw key through four families and counts the holders of every key text
/// the shard ends up with. The answer is a property of how the keys are DERIVED, not of the
/// containers: each family prefixes the raw key with its own name, so the texts are disjoint.
///
/// A MAP SHOWING ZERO KEYS HERE IS NOT A MAP WITH NO WRITER, and reading it that way was an error
/// made against this very output. `control_state_uuid`, `control_state_rollups` and
/// `feature_rollups` report zero on this fixture and ALL THREE have production writers: the first
/// through a multi-line chain (`shard` / `.control_state_uuid` / `.insert(..)`) that a single-line
/// grep cannot match, the other two through `windowed_sum_series(&mut shard.., ..)`, which inserts
/// into whatever map it is handed. `control_state_uuid` is also `#[serde(default)]`, so a stored
/// image fills it with no code path involved at all.
///
/// So this probe answers "is this key duplicated", never "is this map dead". The second question
/// needs the three checks a grep misses: multi-line chains, `&mut` escapes into helpers, and
/// deserialization.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib why_there_are_many_per_key_maps -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "writes one key through four families; run by name"]
fn why_there_are_many_per_key_maps_and_not_one() {
    const KEYS: usize = 64;

    let dir = tempfile::tempdir().unwrap();
    let engine = probe_engine_on(dir.path(), crate::DEFAULT_END_ROUTING_BUCKET);

    for index in 0..KEYS {
        let raw = format!("k{index:04}");
        let commands = vec![
            Command::StringSet { key: raw.clone(), value: vec![b'v'; 32] },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index as u64,
                    value: vec![b'f'; 8],
                }],
            },
            Command::ControlStateSetAndGet {
                family: crate::types::ControlStateFamily::Counter,
                key: raw.clone(),
                timestamp_ms: 1_787_270_070_000 + index as u64,
                amount: 1,
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "sum".to_string(),
            },
            Command::ControlStateSelectionSet {
                key: raw.clone(),
                value: vec![b's'; 8],
                occur_time_ms: 1_787_270_070_000 + index as u64,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::Last,
            },
            // THE REMAINING MAPS WITH WRITERS, so this covers every per-key map a non-context
            // write can reach rather than the four the first version happened to drive. The last
            // one is a QUERY on purpose: `FeatureAggQuery` caches its decoded answer in
            // `feature_values` under the same key `features` is keyed by, so a read is what puts
            // a second holder on the key.
            Command::SeenCheck {
                key: raw.clone(),
                member: vec![b'm'; 8],
                window_ms: 60_000,
            },
            Command::BucketTake {
                key: raw.clone(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
            Command::FeatureAggQuery {
                key: raw.clone(),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "sum".to_string(),
                count: None,
            },
        ];
        for command in commands {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    // Every per-key map a non-context write can land in, with the key TEXTS it holds.
    // EACH KEY CARRIES ITS DATA POINTER, which is what makes this check representation-agnostic.
    // A `String` key and an `Arc<str>` key both expose the address of their characters, so "do
    // these two maps hold ONE allocation or two equal ones" is answerable without knowing, or
    // maintaining a list of, which maps are keyed by what. An allowlist of map names goes stale
    // the moment a map is converted; a pointer does not.
    let holders: Vec<(&str, Vec<(&str, usize)>)> = vec![
        ("strings", shard.strings.keys().map(|k| (&**k, k.as_ptr() as usize)).collect()),
        ("features", shard.features.keys().map(|k| (&**k, k.as_ptr() as usize)).collect()),
        (
            "control_state",
            shard.control_state.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
        ),
        (
            "control_state_selection",
            shard
                .control_state_selection
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_uuid",
            shard.control_state_uuid.keys().map(|k| (k.as_str(), k.as_ptr() as usize)).collect(),
        ),
        (
            "control_state_rollups",
            shard.control_state_rollups.keys().map(|k| (k.as_str(), k.as_ptr() as usize)).collect(),
        ),
        (
            "feature_values",
            shard.feature_values.keys().map(|k| (k.as_str(), k.as_ptr() as usize)).collect(),
        ),
        (
            "feature_rollups",
            shard.feature_rollups.keys().map(|k| (k.as_str(), k.as_ptr() as usize)).collect(),
        ),
        ("seen", shard.seen.keys().map(|k| (&**k, k.as_ptr() as usize)).collect()),
        ("buckets", shard.buckets.keys().map(|k| (&**k, k.as_ptr() as usize)).collect()),
        (
            "expires_at_ms",
            shard.expires_at_ms.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
        ),
    ];

    let mut holders_of: std::collections::BTreeMap<&str, Vec<(&str, usize)>> =
        std::collections::BTreeMap::new();
    println!();
    println!("  {KEYS} raw keys, each written through seven commands across every writable per-key map");
    println!("  map                            keys");
    for (name, keys) in &holders {
        println!("  {name:30} {:>5}", keys.len());
        for (text, ptr) in keys {
            holders_of.entry(text).or_default().push((*name, *ptr));
        }
    }

    let mut by_holders: std::collections::BTreeMap<usize, usize> =
        std::collections::BTreeMap::new();
    for maps in holders_of.values() {
        *by_holders.entry(maps.len()).or_insert(0) += 1;
    }
    println!();
    println!("  distinct key texts: {}", holders_of.len());
    for (n, how_many) in &by_holders {
        println!("    {how_many} texts are held by {n} map(s)");
    }
    let shareable: usize = holders_of.values().filter(|m| m.len() > 1).count();
    println!("  texts held by MORE THAN ONE map -- the only consolidation candidates: {shareable}");
    println!();
    println!("  a sample of the texts one raw key turns into:");
    for (text, maps) in holders_of.iter().filter(|(t, _)| t.contains("k0000")) {
        println!("    {text:38} <- {maps:?}");
    }
    println!();
    println!(
        "  widths: ElementEntry {} B, ObjectRecord {} B, ObjectSlot {} B -- so folding a key that",
        std::mem::size_of::<ElementEntry>(),
        std::mem::size_of::<crate::engine::object_record::ObjectRecord>(),
        std::mem::size_of::<crate::engine::object_record::ObjectSlot>()
    );
    println!("  lives in ONE map into the consolidated map makes it larger, not smaller.");

    // THE GUARD: one key text, one allocation, whatever holds it.
    //
    // Measured by POINTER, so it cannot be satisfied by two maps holding equal copies. Any text
    // whose holders report more than one distinct address has its characters stored once per
    // address, and a clone-based byte probe will never say so, because cloning an `Arc` is a
    // refcount bump.
    let mut duplicated: Vec<(&str, Vec<(&str, usize)>)> = holders_of
        .iter()
        .filter(|(_, held)| {
            held.iter().map(|(_, ptr)| *ptr).collect::<std::collections::HashSet<_>>().len() > 1
        })
        .map(|(text, held)| (*text, held.clone()))
        .collect();
    duplicated.sort();
    println!();
    println!(
        "  by ALLOCATION: {} of {} texts are stored more than once",
        duplicated.len(),
        holders_of.len()
    );
    assert!(
        duplicated.is_empty(),
        "{} key texts are stored more than once -- the holders report different addresses for the          same characters: {:?}. A map holding a key another map already holds must take a          `ModelKey` and join `adopt_object_key_allocations`.",
        duplicated.len(),
        &duplicated[..duplicated.len().min(3)]
    );

    assert!(!holders_of.is_empty(), "no keys landed -- the write path populated nothing");
    assert!(
        std::mem::size_of::<crate::engine::object_record::ObjectSlot>()
            > std::mem::size_of::<ElementEntry>(),
        "the consolidated slot is no longer wider than a bare address, so the arithmetic above \
         changed and the conclusion has to be re-derived"
    );
}

/// WHAT CONSOLIDATING THE THREE HOLDERS OF A RAW KEY WOULD COST, AND AT WHAT OVERLAP IT PAYS.
///
/// `why_there_are_many_per_key_maps_and_not_one` shows a raw key landing in `strings`, `features`
/// and `control_state_selection`. The obvious next move is one entry per key instead of three, and
/// the obvious place is the consolidated map -- except that `ObjectRecord` is 24 bytes carrying a
/// deadline word and a page-address word, and nothing else. It cannot hold an 8-byte series or a
/// 40-byte selection value: those maps are DOMAIN-valued, not address-valued, so they cannot fold
/// into `ObjectSlot` without widening it.
///
/// So the question is what a PURPOSE-BUILT consolidated entry would cost, and the answer has two
/// arms rather than one number:
///
///   * a key held by all three maps pays one key field instead of three, and one table instead of
///     three -- the win;
///   * a key held by ONE map pays the slots of the two values it does not have -- the loss.
///
/// Which dominates is a property of the WORKLOAD, not of the design, so this measures both arms and
/// prints the overlap fraction where they cross. A design chosen without that fraction is chosen on
/// the arm whoever argued for it happened to measure.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_consolidating_three_holders_would_cost -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds four map shapes at 20,000 keys; run by name"]
fn what_consolidating_three_holders_would_cost() {
    use crate::engine::timestamped_series::OwnedTimestampedSeries;
    const KEYS: usize = 20_000;

    // The union a consolidated entry would have to hold: every value, each optional, because a key
    // present in one map has nothing for the other two.
    // `Clone` because `deep_heap_bytes` measures by cloning -- the probe cannot weigh a shape
    // it cannot copy.
    #[allow(dead_code)]
    #[derive(Clone)]
    struct Consolidated {
        address: Option<ElementEntry>,
        series: Option<OwnedTimestampedSeries>,
        selection: Option<(u64, Vec<u8>, u8)>,
    }

    let key = |i: usize| -> std::sync::Arc<str> { std::sync::Arc::from(format!("k{i:08}").as_str()) };
    let address = |i: usize| ElementEntry::from_parts(1, i as u64 * 64, 64, Some(0), Some(i as u64));
    let series = |i: usize| {
        let mut s = OwnedTimestampedSeries::default();
        s.insert(i as u64, address(i));
        s
    };
    let selection = || (7u64, vec![b's'; 8], 1u8);

    // ARM A: three maps, one entry each, every key in all three. Keys SHARED, because that is the
    // state on this branch -- the adoption puts all three holders on one allocation, so the only
    // thing three maps still cost over one is the key FIELD and the table, not the characters.
    let shared: Vec<std::sync::Arc<str>> = (0..KEYS).map(key).collect();
    let mut m_strings: HashMap<std::sync::Arc<str>, ElementEntry> = HashMap::new();
    let mut m_series: HashMap<std::sync::Arc<str>, OwnedTimestampedSeries> = HashMap::new();
    let mut m_selection: HashMap<std::sync::Arc<str>, (u64, Vec<u8>, u8)> = HashMap::new();
    for (i, k) in shared.iter().enumerate() {
        m_strings.insert(std::sync::Arc::clone(k), address(i));
        m_series.insert(std::sync::Arc::clone(k), series(i));
        m_selection.insert(std::sync::Arc::clone(k), selection());
    }
    let three_maps =
        deep_heap_bytes(&m_strings) + deep_heap_bytes(&m_series) + deep_heap_bytes(&m_selection);

    // ARM B: one map, one entry per key, all three values present.
    let mut m_one: HashMap<std::sync::Arc<str>, Consolidated> = HashMap::new();
    for (i, k) in shared.iter().enumerate() {
        m_one.insert(
            std::sync::Arc::clone(k),
            Consolidated {
                address: Some(address(i)),
                series: Some(series(i)),
                selection: Some(selection()),
            },
        );
    }
    let one_map_full = deep_heap_bytes(&m_one);

    // ARM C: the LOSING arm -- every key in one map only, three maps.
    let mut c_strings: HashMap<std::sync::Arc<str>, ElementEntry> = HashMap::new();
    for (i, k) in shared.iter().enumerate() {
        c_strings.insert(std::sync::Arc::clone(k), address(i));
    }
    let three_maps_no_overlap = deep_heap_bytes(&c_strings);

    // ARM D: the same keys in the consolidated shape, two slots empty.
    let mut d_one: HashMap<std::sync::Arc<str>, Consolidated> = HashMap::new();
    for (i, k) in shared.iter().enumerate() {
        d_one.insert(
            std::sync::Arc::clone(k),
            Consolidated { address: Some(address(i)), series: None, selection: None },
        );
    }
    let one_map_sparse = deep_heap_bytes(&d_one);

    println!();
    println!("  {KEYS} keys, key allocations SHARED in every arm (so this prices fields and tables)");
    println!(
        "    widths: ElementEntry {} B, OwnedTimestampedSeries {} B, selection {} B, Consolidated {} B",
        std::mem::size_of::<ElementEntry>(),
        std::mem::size_of::<OwnedTimestampedSeries>(),
        std::mem::size_of::<(u64, Vec<u8>, u8)>(),
        std::mem::size_of::<Consolidated>()
    );
    println!();
    println!("  FULL OVERLAP -- every key in all three:");
    println!("    three maps      {three_maps:>10} B   {:>7.1} B/key", three_maps as f64 / KEYS as f64);
    println!("    one map         {one_map_full:>10} B   {:>7.1} B/key", one_map_full as f64 / KEYS as f64);
    let win = three_maps as f64 - one_map_full as f64;
    println!("    consolidation {:>+12.1} B   {:>+7.1} B/key", win, win / KEYS as f64);
    println!();
    println!("  NO OVERLAP -- every key in one map only:");
    println!(
        "    one bare map    {three_maps_no_overlap:>10} B   {:>7.1} B/key",
        three_maps_no_overlap as f64 / KEYS as f64
    );
    println!(
        "    one map, sparse {one_map_sparse:>10} B   {:>7.1} B/key",
        one_map_sparse as f64 / KEYS as f64
    );
    let loss = three_maps_no_overlap as f64 - one_map_sparse as f64;
    println!("    consolidation {:>+12.1} B   {:>+7.1} B/key", loss, loss / KEYS as f64);
    println!();
    let per_key_win = win / KEYS as f64;
    let per_key_loss = -loss / KEYS as f64;
    if per_key_win > 0.0 && per_key_loss > 0.0 {
        let breakeven = per_key_loss / (per_key_win + per_key_loss);
        println!(
            "  BREAK-EVEN: consolidation pays once more than {:.1}% of keys are held by all three.",
            100.0 * breakeven
        );
        println!("  Below that fraction it costs more than the three maps it replaces.");
    } else {
        println!("  One arm does not trade -- read the two figures above rather than a fraction.");
    }

    assert!(three_maps > 0 && one_map_full > 0, "a zero figure cannot be compared");
}

/// WHAT MAKING `control_state`s KEY A SHARED HANDLE COSTS AND SAVES, BOTH HALVES SEPARATELY.
///
/// `which_string_keyed_holders_duplicate_a_key_another_map_holds` shows the raw-keyed half of
/// `control_state` owning a second copy of characters three other maps already agreed on, 8 of 8
/// keys. Converting the key to `ModelKey` lets that half share. But one map cannot have two key
/// types, so the DECORATED half -- `control_state:<family>:<key>`, which no other map holds --
/// is converted too, and for that half sharing is impossible. It can only cost.
///
/// So the change has two arms, and a single average would hide one of them:
///
///   * raw half: the inline key field goes 24 -> 16 bytes AND the characters stop being a second
///     allocation, because the handle is the one the page index already owns;
///   * decorated half: the inline field still goes 24 -> 16, but the characters are STILL one
///     allocation, now with two counter words ahead of them.
///
/// # THE DECORATED HALF WAS PREDICTED TO COST AND IT DOES NOT
///
/// This test was written expecting the decorated half to be a LOSS -- the two counter words an
/// `Arc<str>` puts ahead of the bytes looked like they would cross an allocator rounding step
/// that the `String` did not. Measured at 20,000 keys, that is wrong: the decorated half improves
/// by a flat **10.2 B/key** at every key length tried, because the 8 bytes the inline field gives
/// up (24 -> 16, a fat pointer rather than ptr/len/capacity) shrinks the table as well, and at
/// these lengths `len + 16` lands in the same bin as `len` did.
///
/// So there is no overlap fraction to satisfy: the conversion pays on BOTH kinds of key, and the
/// `println!` below reports that rather than a break-even. The assert at the end pins the only
/// thing that must still hold -- that the raw-half saving GROWS with key length, because that is
/// the signature of a character allocation that stopped happening rather than of an inline field
/// that merely got smaller.
///
/// Measured, `chunk_bytes`, 20,000 keys:
///
///   key length    raw half            decorated half
///   8             401.0 -> 342.4      400.6 -> 390.4
///   24            400.6 -> 342.4      416.6 -> 406.4
///   48            432.6 -> 342.4      448.6 -> 438.4
///
/// Two things to read carefully there. The raw-half saving is a STEP, not a slope: 58.6 at length
/// 8 and 58.2 at 24 are the same figure, and it jumps to 90.2 at 48 -- so quoting one number as
/// "the saving" is quoting one cell. And the ~400 B/key absolute is almost all the one-point
/// `BTreeMap` value, not the key; only the differences between arms are about the key.
///
/// # WHY THIS MEASURES BY BUILDING AND NOT BY `deep_heap_bytes`
///
/// `deep_heap_bytes` measures by CLONING, and cloning an `Arc` is a refcount bump -- so it cannot
/// see an `Arc` payload at all. For the shared arm that happens to be right, because those
/// characters genuinely belong to the page index rather than to this map. For the DECORATED arm it
/// is wrong in the direction that flatters the change: that map owns its `Arc` outright, and a
/// clone probe would report its characters as free. So every arm here is built under the counting
/// allocator instead, with the key texts materialised OUTSIDE the probe, so each arm is charged
/// for exactly one key allocation of its own kind and nothing else.
///
/// The figure is `chunk_bytes`, what the allocator actually set aside, NOT the `alloc_bytes` that
/// `deep_heap_bytes` and therefore most published figures in this file report. That is deliberate
/// here -- an `Arc<str>` adds two counter words to the request, which is exactly the kind of thing
/// that crosses a rounding step and would be invisible in the requested figure -- but it means a
/// number from this test is NOT directly comparable with a B/key figure from the others.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_shared_control_state_key_costs -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds four map shapes at three key lengths; run by name, and ALONE -- the \n           allocation counters are process-global, so a test running beside this one is \n           counted into its arms"]
fn what_a_shared_control_state_key_costs_on_each_half() {
    const KEYS: usize = 20_000;

    println!();
    println!("  {KEYS} keys per arm, built under the counting allocator (not cloned)");

    let mut rows: Vec<(usize, f64, f64)> = Vec::new();

    // Key lengths that matter: the allocator rounds, so a figure at one length is one point on a
    // step function. 8 is short; 24 and 48 carry Arc<str>s two-refcount header into further bins,
    // and 48 is a realistic object key.
    for key_len in [8usize, 24, 48] {
        let pad = |mut s: String| {
            while s.len() < key_len {
                s.push('x');
            }
            s
        };
        // Materialised OUTSIDE every probe: these stand for text the caller already has.
        let raw_texts: Vec<String> = (0..KEYS).map(|i| pad(format!("k{i:07}"))).collect();
        let dec_texts: Vec<String> =
            raw_texts.iter().map(|k| format!("control_state:h:{k}")).collect();
        // The handles the page index owns. The shared arm borrows from here, which is what
        // adoption does -- so these characters are not charged to the map under test, exactly as
        // they are not charged in the engine.
        let handles: Vec<std::sync::Arc<str>> =
            raw_texts.iter().map(|k| std::sync::Arc::from(&k[..])).collect();

        let series = |i: usize| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(i as u64, i as i64);
            m
        };

        // Every arm allocates the same table and the same series, so the difference between two
        // arms is the key and only the key.
        let owned_keys = |texts: &Vec<String>| -> u64 {
            let probe = Probe::start();
            let mut map: HashMap<String, std::collections::BTreeMap<u64, i64>> = HashMap::new();
            for (i, text) in texts.iter().enumerate() {
                map.insert(text.clone(), series(i));
            }
            std::hint::black_box(&map);
            probe.stop().chunk_bytes
        };
        let minted_arc_keys = |texts: &Vec<String>| -> u64 {
            let probe = Probe::start();
            let mut map: HashMap<std::sync::Arc<str>, std::collections::BTreeMap<u64, i64>> =
                HashMap::new();
            for (i, text) in texts.iter().enumerate() {
                map.insert(std::sync::Arc::from(&text[..]), series(i));
            }
            std::hint::black_box(&map);
            probe.stop().chunk_bytes
        };

        let raw_owned = owned_keys(&raw_texts);
        let raw_shared = {
            let probe = Probe::start();
            let mut map: HashMap<std::sync::Arc<str>, std::collections::BTreeMap<u64, i64>> =
                HashMap::new();
            for (i, handle) in handles.iter().enumerate() {
                map.insert(std::sync::Arc::clone(handle), series(i));
            }
            std::hint::black_box(&map);
            probe.stop().chunk_bytes
        };
        let dec_owned = owned_keys(&dec_texts);
        let dec_minted = minted_arc_keys(&dec_texts);

        let per = |bytes: u64| bytes as f64 / KEYS as f64;
        let raw_win = per(raw_owned) - per(raw_shared);
        let dec_cost = per(dec_minted) - per(dec_owned);
        rows.push((key_len, raw_win, dec_cost));

        println!();
        println!("  key length {key_len}");
        println!(
            "    raw half        String {:>7.1}  ->  shared {:>7.1} B/key   {:+7.1}",
            per(raw_owned),
            per(raw_shared),
            -raw_win
        );
        println!(
            "    decorated half  String {:>7.1}  ->  minted {:>7.1} B/key   {:+7.1}",
            per(dec_owned),
            per(dec_minted),
            dec_cost
        );
        if raw_win > 0.0 && dec_cost > 0.0 {
            // net = f * raw_win - (1 - f) * dec_cost = 0
            println!(
                "    pays once more than {:.1}% of keys arrive through the counter commands",
                100.0 * dec_cost / (raw_win + dec_cost)
            );
        } else if dec_cost <= 0.0 {
            println!("    both halves improve at this length -- there is no ratio to satisfy");
        } else {
            println!("    the raw half does not improve here; read the two rows, not a ratio");
        }

        assert!(
            raw_owned > 0 && raw_shared > 0 && dec_owned > 0 && dec_minted > 0,
            "a zero figure cannot be compared at key length {key_len}"
        );
        // If the shared arm is not smaller, the handles were copied rather than borrowed and
        // every figure above is measuring something else.
        assert!(
            raw_shared < raw_owned,
            "the shared arm is not smaller at key length {key_len} ({raw_shared} vs {raw_owned}), \
             so the handle was not actually borrowed"
        );
    }

    // The saving must GROW with key length -- it is one character allocation that stops
    // happening. Flat would mean the figure is the inline field only and the sharing is not
    // working.
    assert!(
        rows[2].1 > rows[0].1,
        "the raw-half saving did not grow from key length {} to {} ({:.1} vs {:.1} B/key), so it \
         is not the characters that stopped being allocated",
        rows[0].0,
        rows[2].0,
        rows[0].1,
        rows[2].1
    );
}

/// WHAT THE FOUR COLLECTION MAPS SHARING ONE KEY IS WORTH, MEASURED AND NOT MULTIPLIED.
///
/// `which_string_keyed_holders_duplicate_a_key_another_map_holds` measured a raw key held by eight
/// holders at FIVE allocations: four adopted maps agreeing on one, and `hashes`, `sets`, `zsets`
/// and `lists` each owning a copy. All four share the handle now.
///
/// The arithmetic temptation is to take the one-map figure from
/// `what_a_shared_control_state_key_costs_on_each_half` and multiply it by four. That would be
/// wrong twice over: the four value types differ (`HashFieldMap`, two `BTreeMap<Vec<u8>, _>`
/// shapes and a `BTreeMap<i64, _>`), so the entry widths and therefore the table growth differ
/// per map; and the saving is a STEP in key length, so four steps need not land on the same one.
/// A footprint total is re-measured, never derived.
///
/// So this builds all four maps, with their real value types, in both key shapes.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_four_collection_maps_sharing -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds eight map shapes at three key lengths; run by name, and ALONE -- the allocation \
            counters are process-global"]
fn what_four_collection_maps_sharing_one_key_costs() {
    use std::collections::BTreeMap;
    const KEYS: usize = 20_000;

    println!();
    println!("  {KEYS} keys in EACH of four maps, built under the counting allocator");
    println!("  (chunk_bytes, so not comparable with the alloc_bytes figures elsewhere here)");

    let mut rows: Vec<(usize, f64)> = Vec::new();

    for key_len in [8usize, 24, 48] {
        let pad = |mut s: String| {
            while s.len() < key_len {
                s.push('x');
            }
            s
        };
        let texts: Vec<String> = (0..KEYS).map(|i| pad(format!("k{i:07}"))).collect();
        let handles: Vec<std::sync::Arc<str>> =
            texts.iter().map(|k| std::sync::Arc::from(&k[..])).collect();

        // The four real value shapes, one element each -- the element count is held equal across
        // both arms so the difference is the key.
        let set_v = |i: usize| {
            let mut m: BTreeMap<Vec<u8>, ElementEntry> = BTreeMap::new();
            m.insert(vec![b'm'; 8], ElementEntry::from_parts(1, i as u64, 8, None, None));
            m
        };
        let list_v = |i: usize| {
            let mut m: BTreeMap<i64, ElementEntry> = BTreeMap::new();
            m.insert(i as i64, ElementEntry::from_parts(1, i as u64, 8, None, None));
            m
        };
        let zset_v = |i: usize| {
            let mut m: BTreeMap<Vec<u8>, (u64, ElementEntry)> = BTreeMap::new();
            m.insert(
                vec![b'z'; 8],
                (1u64, ElementEntry::from_parts(1, i as u64, 8, None, None)),
            );
            m
        };

        let owned = {
            let probe = Probe::start();
            let mut sets: HashMap<String, BTreeMap<Vec<u8>, ElementEntry>> = HashMap::new();
            let mut lists: HashMap<String, BTreeMap<i64, ElementEntry>> = HashMap::new();
            let mut zsets: HashMap<String, BTreeMap<Vec<u8>, (u64, ElementEntry)>> = HashMap::new();
            let mut hashes: HashMap<String, BTreeMap<Vec<u8>, ElementEntry>> = HashMap::new();
            for (i, text) in texts.iter().enumerate() {
                sets.insert(text.clone(), set_v(i));
                lists.insert(text.clone(), list_v(i));
                zsets.insert(text.clone(), zset_v(i));
                hashes.insert(text.clone(), set_v(i));
            }
            std::hint::black_box((&sets, &lists, &zsets, &hashes));
            probe.stop().chunk_bytes
        };

        let shared = {
            let probe = Probe::start();
            let mut sets: HashMap<std::sync::Arc<str>, BTreeMap<Vec<u8>, ElementEntry>> =
                HashMap::new();
            let mut lists: HashMap<std::sync::Arc<str>, BTreeMap<i64, ElementEntry>> =
                HashMap::new();
            let mut zsets: HashMap<std::sync::Arc<str>, BTreeMap<Vec<u8>, (u64, ElementEntry)>> =
                HashMap::new();
            let mut hashes: HashMap<std::sync::Arc<str>, BTreeMap<Vec<u8>, ElementEntry>> =
                HashMap::new();
            for (i, handle) in handles.iter().enumerate() {
                sets.insert(std::sync::Arc::clone(handle), set_v(i));
                lists.insert(std::sync::Arc::clone(handle), list_v(i));
                zsets.insert(std::sync::Arc::clone(handle), zset_v(i));
                hashes.insert(std::sync::Arc::clone(handle), set_v(i));
            }
            std::hint::black_box((&sets, &lists, &zsets, &hashes));
            probe.stop().chunk_bytes
        };

        let per = |b: u64| b as f64 / KEYS as f64;
        let saving = per(owned) - per(shared);
        rows.push((key_len, saving));
        println!();
        println!("  key length {key_len}");
        println!("    four maps, own copies   {:>8.1} B/key", per(owned));
        println!("    four maps, one handle   {:>8.1} B/key", per(shared));
        println!("    saving                  {:>8.1} B/key", saving);

        assert!(
            shared < owned,
            "the shared arm is not smaller at key length {key_len} ({shared} vs {owned}), so the \
             handles were copied rather than borrowed"
        );
    }

    println!();
    println!("  The saving is a STEP in key length, not a slope -- read the row, not an average:");
    for (len, saving) in &rows {
        println!("    key length {len:>3}  ->  {saving:>7.1} B/key");
    }
    assert!(
        rows[2].1 > rows[0].1,
        "the saving did not grow with key length ({:.1} at {} vs {:.1} at {}), so it is not four \
         character allocations that stopped happening",
        rows[0].1,
        rows[0].0,
        rows[2].1,
        rows[2].0
    );
}

/// WHAT A WHOLE-SHARD LIVE-BLOCK WALK ALLOCATES PER ENTRY, AND WHAT THE KEY COSTS OF THAT.
///
/// `collect_model_live_block_entries` walks every live block on the shard and builds a
/// `LiveBlockEntry` for each. The key reaches it as a `&str`, because `visit_model_live_blocks`
/// hands its `emit` closure `(ModelKind, &str, Option<&str>, &ElementEntry)` -- so the handle the
/// model map holds is gone by the time the entry is built, and the entry is built with
///
///     live_block_entry(object_key.to_string(), ..)        // one allocation
///     -> LiveBlockEntry { object_key: Arc::from(object_key.into()), .. }   // and another
///
/// TWO allocations of the same characters per entry, where cloning the map's own handle would be
/// none. Same shape as the tombstone row -- a `&str` discarding a handle the caller already held
/// -- but at a CLOSURE boundary shared by fifteen arms, so the fix is wider than a signature.
///
/// Which is why this measures first. The walk is `WholeShardEntries`, so the cost is per live
/// block on the shard, and whether that is worth a fifteen-arm change is a number rather than an
/// opinion.
///
/// # MEASURED: TWO OF THE THREE ALLOCATIONS PER ENTRY ARE THE KEY
///
///     6000 live block entries from 2000 keys across three kinds
///     18,014 allocations = 3.00 per entry, 322.1 bytes per entry
///     the key's two allocations: at most 58.0 bytes per entry
///
/// So **67% of the allocation COUNT and 18% of the bytes**. That makes it a throughput finding for
/// the dump and rebuild paths that run this walk, more than a footprint one -- worth saying both
/// ways round, because a campaign measuring resident bytes would have called 18% marginal and
/// stopped.
///
/// # AND WHY THE FIX IS NOT JUST A SIGNATURE
///
/// Fourteen of the arms iterate a `ModelKey`-keyed map and could pass the handle they already
/// hold. One cannot: the `context_entities` arm COMPOSES its key from the collection key and the
/// entity hash and emits `composed.as_str()`, so there is no handle in existence to pass. A
/// uniform `&ModelKey` parameter is therefore impossible, and the shape that works is a small
///
///     enum KeyRef<'a> { Shared(&'a ModelKey), Borrowed(&'a str) }
///
/// with `live_block_entry` cloning the handle where there is one and minting where there is not.
/// That is a fifteen-arm change on the walk that feeds dumps and rebuilds, so it is left as a
/// separate reviewed change rather than folded in behind this measurement -- the number is here so
/// the decision can be made on it.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_whole_shard_live_block_walk -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds a shard and walks every live block; run by name, and ALONE -- the allocation \
            counters are process-global"]
fn what_a_whole_shard_live_block_walk_allocates_per_entry() {
    const KEYS: usize = 2_000;
    const T0: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);

    // A spread of kinds, so the walk crosses several arms rather than one.
    for index in 0..KEYS {
        let raw = format!("walked-{index:06}");
        for command in [
            crate::types::Command::StringSet { key: raw.clone(), value: vec![b'v'; 16] },
            crate::types::Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: T0 + index as u64,
                    value: vec![b'f'; 8],
                }],
            },
            crate::types::Command::HashSet {
                key: raw.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 8],
            },
        ] {
            let response = engine
                .execute(crate::types::ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    let probe = Probe::start();
    let entries = crate::engine::storage_bucket_internals::collect_model_live_block_entries(shard);
    let counts = probe.stop();
    let walked = entries.len();
    std::hint::black_box(&entries);

    println!();
    println!("  {KEYS} keys written across three kinds");
    println!("  live block entries the walk produced: {walked}");
    println!("  allocations: {}", counts.allocs);
    println!("  bytes (chunk): {}", counts.chunk_bytes);
    if walked > 0 {
        println!("    per entry: {:.2} allocations", counts.allocs as f64 / walked as f64);
        println!(
            "    per entry: {:.1} bytes",
            counts.chunk_bytes as f64 / walked as f64
        );
        // What the key alone would stop costing: the String and the Arc built from it. The figure
        // to compare a fifteen-arm change against.
        println!(
            "    of which the key's two allocations are at most {:.1} bytes per entry",
            (2 * (16 + "walked-000000".len())) as f64
        );
    }

    assert!(
        walked > 0,
        "the walk produced no entries, so this measures nothing about a whole-shard walk"
    );
    assert!(
        counts.allocs > 0,
        "the walk allocated nothing, which cannot be right for {walked} entries"
    );
}

/// WHAT THE COMPOSED ENTITY KEY COSTS THE WALK, AND WHETHER A HANDLE IS THE ONLY WAY TO FIX IT.
///
/// `what_a_whole_shard_live_block_walk_allocates_per_entry` reached 1.00 allocations per entry, and
/// the one arm left out of that is `context_entities`. It cannot lend a handle for a reason no
/// signature reaches: it COMPOSES its key inside the inner loop --
///
///     let composed = format!("{collection_key}:{entity_hash}");
///
/// -- so the key it emits exists nowhere to be borrowed. That was recorded as a floor.
///
/// IT IS NOT THE WHOLE FLOOR. A composed key costs TWO allocations per entry: the `format!` that
/// builds it, and the `Arc` the consumer mints from it because there is no handle. Interning the
/// composed key would remove the second and is a design question with its own footprint cost. The
/// FIRST is a reused buffer -- no design change, no interning, no new resident bytes.
///
/// So this measures the arm on its own, before any of that, so the two halves can be told apart.
/// The fixture writes `context_entities` directly rather than through a command, because the walk
/// takes a `&ShardState` and the question is about the walk.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_the_composed_entity_key_costs -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds a shard of entity series and walks it; run by name, and ALONE -- the allocation \
            counters are process-global"]
fn what_the_composed_entity_key_costs_the_walk() {
    use std::collections::BTreeMap;
    const COLLECTIONS: usize = 200;
    const PER_COLLECTION: usize = 10;

    let mut shard = crate::engine::state::ShardState::default();
    for collection in 0..COLLECTIONS {
        let collection_key: std::sync::Arc<str> =
            std::sync::Arc::from(format!("ctx:entity:tenant-a:node-{collection:05}").as_str());
        let mut series: BTreeMap<u64, ElementEntry> = BTreeMap::new();
        for entity in 0..PER_COLLECTION {
            series.insert(
                entity as u64,
                ElementEntry::from_parts(1, (collection * 64 + entity) as u64, 48, None, None),
            );
        }
        shard.context_entities.insert(collection_key, series);
    }

    let probe = Probe::start();
    let entries = crate::engine::storage_bucket_internals::collect_model_live_block_entries(&shard);
    let counts = probe.stop();
    let walked = entries.len();
    std::hint::black_box(&entries);

    println!();
    println!("  {COLLECTIONS} collections x {PER_COLLECTION} entities");
    println!("  entries the walk produced: {walked}");
    println!("  allocations: {}", counts.allocs);
    println!("  bytes (chunk): {}", counts.chunk_bytes);
    if walked > 0 {
        println!("    per entry: {:.2} allocations", counts.allocs as f64 / walked as f64);
        println!(
            "    per entry: {:.1} bytes",
            counts.chunk_bytes as f64 / walked as f64
        );
    }
    println!();
    println!("  For comparison, the arms that lend a handle reach 1.00 allocations per entry.");
    println!("  Two per entry here would be the `format!` plus the `Arc` minted from it: the");
    println!("  first is a reused buffer, the second needs the composed key to be interned.");

    assert_eq!(
        COLLECTIONS * PER_COLLECTION,
        walked,
        "the walk produced {walked} entries, not {}, so the entity arm is not the thing being \
         measured",
        COLLECTIONS * PER_COLLECTION
    );
    assert!(counts.allocs > 0, "the walk allocated nothing, which cannot be right");
}

/// WHICH ARM THE LAST ALLOCATION PER ENTRY BELONGS TO, MEASURED PER ARM INSTEAD OF GUESSED.
///
/// The whole-shard walk sits at 1.00 allocations per entry and I have now been wrong TWICE about
/// what that one is. I said it was the component's intermediate `String` -- it is not, because
/// every container kind converges on the page and no live arm emits a component at all. I ruled out
/// the `ElementEntry` clone by reading, which is right as far as it goes: that type is all inline
/// fields.
///
/// Two wrong decompositions is enough to stop deriving one. This walks ONE kind at a time, so the
/// per-entry figure is attributable by construction rather than by arithmetic over a mixed total:
/// a shard holding only strings, then only features, then only hashes.
///
/// What the arms do differently, and therefore what the comparison can separate:
///
///   strings    one entry per object, key lent, no component, no inner collection
///   features   goes through `visit_timestamped_series`, which builds a collection of unique
///              addresses per series before emitting
///   hashes     goes through `emit_one_entry_a_page`, which builds a `BTreeSet` of pages per key
///              to dedup them
///
/// So if the residual is per-SERIES rather than per-entry, the arms with an inner collection will
/// show it and the strings arm will not.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib which_arm_the_last_allocation -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "walks three single-kind shards; run by name, and ALONE -- the allocation counters are \
            process-global"]
fn which_arm_the_last_allocation_per_entry_belongs_to() {
    use std::collections::BTreeMap;
    const KEYS: usize = 1_000;
    const T0: u64 = 1_787_270_070_000;

    let address = |i: usize| ElementEntry::from_parts(1, i as u64 * 64, 48, None, None);

    // ---- strings: one entry per object, nothing nested -------------------------------
    let strings_only = {
        let mut shard = crate::engine::state::ShardState::default();
        for i in 0..KEYS {
            shard
                .strings
                .insert(std::sync::Arc::from(format!("s-{i:06}").as_str()), address(i));
        }
        let probe = Probe::start();
        let entries =
            crate::engine::storage_bucket_internals::collect_model_live_block_entries(&shard);
        let counts = probe.stop();
        let n = entries.len();
        std::hint::black_box(&entries);
        (n, counts.allocs, counts.chunk_bytes)
    };

    // ---- features: through `visit_timestamped_series`, one address each ---------------
    let features_only = {
        let mut shard = crate::engine::state::ShardState::default();
        for i in 0..KEYS {
            let mut series = crate::engine::timestamped_series::OwnedTimestampedSeries::default();
            series.insert(T0 + i as u64, address(i));
            shard
                .features
                .insert(std::sync::Arc::from(format!("f-{i:06}").as_str()), series);
        }
        let probe = Probe::start();
        let entries =
            crate::engine::storage_bucket_internals::collect_model_live_block_entries(&shard);
        let counts = probe.stop();
        let n = entries.len();
        std::hint::black_box(&entries);
        (n, counts.allocs, counts.chunk_bytes)
    };

    // ---- hashes: through `emit_one_entry_a_page`, one field each ----------------------
    let hashes_only = {
        let mut shard = crate::engine::state::ShardState::default();
        for i in 0..KEYS {
            shard.hashes.insert_element_for_test(
                &format!("h-{i:06}"),
                "field".to_string(),
                address(i),
            );
        }
        let probe = Probe::start();
        let entries =
            crate::engine::storage_bucket_internals::collect_model_live_block_entries(&shard);
        let counts = probe.stop();
        let n = entries.len();
        std::hint::black_box(&entries);
        (n, counts.allocs, counts.chunk_bytes)
    };

    println!();
    println!("  {KEYS} keys per arm, one entry each, walked on a single-kind shard");
    println!();
    println!("    arm        entries   allocations   per entry   bytes/entry");
    for (name, (n, allocs, bytes)) in [
        ("strings", strings_only),
        ("features", features_only),
        ("hashes", hashes_only),
    ] {
        println!(
            "    {name:<10} {n:>7}   {allocs:>11}   {:>9.2}   {:>11.1}",
            allocs as f64 / n.max(1) as f64,
            bytes as f64 / n.max(1) as f64
        );
    }
    println!();
    println!("  An arm at ~0 per entry has nothing left to charge. An arm at ~1 is paying for");
    println!("  something once per entry, and the shapes above say what is available to pay for:");
    println!("  an inner collection per series, or the entry itself.");
    println!();
    println!("  MEASURED: strings 0.01, hashes 1.01, features 2.01. The mixed walk's 1.00 is the");
    println!("  average of those three and nothing else -- (0.01 + 2.01 + 1.01) / 3 = 1.01.");
    println!("  strings pays NOTHING: its key is lent, it has no component and nothing nested.");
    println!("  hashes pays once, for `emit_one_entry_a_page`'s dedup set per key.");
    println!("  features pays TWICE, and `unique_timestamped_kv_block_addresses` is why:");
    println!("      .collect::<HashSet<_>>().into_iter().collect::<Vec<_>>()");
    println!("  a hash set to dedup, then a vector to sort.");
    println!();
    println!("  COLLAPSING THOSE TWO IS A DESIGN DECISION, NOT A REFACTOR. Sort-then-dedup needs a");
    println!("  TOTAL ORDER over every identity field of `ElementEntry`, which derives PartialEq,");
    println!("  Eq and Hash but deliberately NOT Ord. Sorting by the three documented keys leaves");
    println!("  entries differing in `block_id` comparing Equal, so a stable sort can interleave");
    println!("  them as A, B, A and `dedup()` -- which removes only CONSECUTIVE duplicates --");
    println!("  keeps the second A. A hand-written comparator that misses a field is worse still:");
    println!("  two distinct entries compare Equal and dedup drops one, which is data loss on a");
    println!("  walk that feeds rebuilds. The current shape is correct BY CONSTRUCTION because it");
    println!("  dedups on the derived Eq/Hash. Giving the wire type an Ord is the decision.");

    for (name, (n, _, _)) in [
        ("strings", strings_only),
        ("features", features_only),
        ("hashes", hashes_only),
    ] {
        assert_eq!(
            KEYS, n,
            "the {name} arm produced {n} entries, not {KEYS} -- that arm is not being measured"
        );
    }
}

/// THE SERIES DEDUP KEEPS ONE OF EACH DISTINCT ENTRY, INCLUDING THE CASE A NAIVE `dedup()` BREAKS.
///
/// `unique_timestamped_kv_block_addresses` used to dedup through a `HashSet` and then sort in a
/// `Vec` -- two allocations per series, and the whole of the features arm's walk cost. It now
/// collects once, sorts by (slab, offset, length), and dedups by the DERIVED `PartialEq` within
/// each run of equal sort keys.
///
/// That is safe for a reason worth pinning rather than asserting: two FULLY equal entries agree on
/// the three sort keys, so they land in the same contiguous group, and a group scan with the
/// derived comparison cannot miss a field. What is NOT safe, and what this test exists to catch,
/// is the obvious shortcut -- `sort_by(three keys)` then `dedup()`. `dedup()` removes only
/// CONSECUTIVE duplicates, and entries differing in `block_id` compare Equal under that sort, so a
/// stable sort can order them `A, B, A` and the second `A` survives.
///
/// So the fixture below is exactly that arrangement: two entries naming one page and differing in
/// their block id, with a repeat of the first. A correct dedup returns TWO entries; a `dedup()`
/// after that sort returns three.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib the_series_dedup_keeps_one_of_each -- --nocapture
#[test]
fn the_series_dedup_keeps_one_of_each_distinct_entry() {
    use crate::engine::storage_reporting::unique_timestamped_kv_block_addresses;
    use crate::engine::timestamped_series::OwnedTimestampedSeries;

    // One page -- same slab, offset and length -- carrying two different block ids, and the first
    // id seen again at a later timestamp. Under the sort these three compare Equal to each other,
    // which is the whole point.
    let page_a_id5 = ElementEntry::from_parts(7, 4_096, 64, Some(5), None);
    let page_a_id9 = ElementEntry::from_parts(7, 4_096, 64, Some(9), None);
    assert_ne!(
        page_a_id5, page_a_id9,
        "the fixture needs two DISTINCT entries on one page, or it tests nothing"
    );

    let mut series = OwnedTimestampedSeries::default();
    series.insert(1_787_270_070_001, page_a_id5.clone());
    series.insert(1_787_270_070_002, page_a_id9.clone());
    series.insert(1_787_270_070_003, page_a_id5.clone());

    let unique = unique_timestamped_kv_block_addresses(&series);

    println!();
    println!("  three addresses in, {} out", unique.len());
    for entry in &unique {
        println!(
            "    slab {} offset {} length {} block_id {:?}",
            entry.block_slab_id(),
            entry.offset(),
            entry.length(),
            entry.block_id()
        );
    }

    assert_eq!(
        2,
        unique.len(),
        "the dedup returned {} entries for two distinct addresses. Three means the repeat of the \
         first survived -- which is what `sort_by(three keys)` followed by `dedup()` does, because \
         the two block ids compare Equal under that sort and leave A, B, A with no consecutive \
         pair. One means two distinct entries were merged, which is data loss.",
        unique.len()
    );
    assert!(
        unique.iter().any(|entry| *entry == page_a_id5),
        "the entry with block id 5 is missing: {unique:?}"
    );
    assert!(
        unique.iter().any(|entry| *entry == page_a_id9),
        "the entry with block id 9 is missing: {unique:?}"
    );

    // AND THE ORDER THE CALLERS READ IS UNCHANGED: sorted by slab, then offset, then length.
    for pair in unique.windows(2) {
        let (left, right) = (&pair[0], &pair[1]);
        let ordered = (
            left.block_slab_id(),
            left.offset(),
            left.length(),
        ) <= (
            right.block_slab_id(),
            right.offset(),
            right.length(),
        );
        assert!(ordered, "the result is not sorted by (slab, offset, length): {unique:?}");
    }
}

/// WHAT INTERNING THE COMPOSED ENTITY KEY WOULD TRADE, PRICED ON BOTH SIDES.
///
/// The entity arm is the only one whose key cannot be lent: it COMPOSES the key from the collection
/// key and the entity hash, so the key it emits exists nowhere to be borrowed. Its last allocation
/// per entry is the `Arc` the consumer mints from those characters, and the standing suggestion for
/// removing it is to intern the composed key in a pool.
///
/// I recorded that as a decision "with a measurable cost on both sides" and then left it open.
/// This closes it, because the two sides are not symmetric and the asymmetry is the whole answer:
///
///   the SAVING is per walk        -- one allocation per entity entry, each time the walk runs,
///                                    and the minted `Arc` is dropped as soon as the entry is
///                                    consumed, so nothing accumulates today
///   the COST is permanent         -- one pooled `Arc<str>` per DISTINCT composed key, resident
///                                    for the life of the shard whether a walk ever runs again
///
/// So interning converts a transient, repeatable cost into a standing one. This prices the standing
/// side, which is the half that cannot be undone by running the walk less often.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_interning_the_composed_entity_key -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds an intern pool of composed keys; run by name, and ALONE -- the allocation \
            counters are process-global"]
fn what_interning_the_composed_entity_key_would_trade() {
    const COLLECTIONS: usize = 200;
    const PER_COLLECTION: usize = 10;
    let entities = COLLECTIONS * PER_COLLECTION;

    // The composed key as the arm builds it: `{collection_key}:{entity_hash}`.
    let composed: Vec<String> = (0..COLLECTIONS)
        .flat_map(|collection| {
            (0..PER_COLLECTION).map(move |entity| {
                format!("ctx:entity:tenant-a:node-{collection:05}:{entity}")
            })
        })
        .collect();
    let width = composed.first().map(|key| key.len()).unwrap_or(0);

    // THE STANDING COST: a pool holding one handle per distinct composed key, which is what
    // interning means. Measured as it is built, so the figure is the pool and not the fixture.
    let probe = Probe::start();
    let mut pool: std::collections::HashSet<std::sync::Arc<str>> =
        std::collections::HashSet::new();
    for key in &composed {
        if !pool.contains(&key[..]) {
            pool.insert(std::sync::Arc::from(&key[..]));
        }
    }
    let counts = probe.stop();
    std::hint::black_box(&pool);

    println!();
    println!("  {entities} distinct composed keys, {width} characters each");
    println!();
    println!("  THE STANDING COST of interning them:");
    println!("    allocations: {}", counts.allocs);
    println!("    bytes (chunk): {}", counts.chunk_bytes);
    println!(
        "    per entity: {:.1} bytes, resident for the life of the shard",
        counts.chunk_bytes as f64 / entities as f64
    );
    println!();
    println!("  THE SAVING, for comparison: 1 allocation per entity entry PER WALK, and the");
    println!("  entity arm measures 1.01 allocations per entry today");
    println!("  (`what_the_composed_entity_key_costs_the_walk`). The minted `Arc` is dropped as");
    println!("  soon as the entry is consumed, so today's cost accumulates nothing.");
    println!();
    println!("  SO THE TRADE IS: pay {:.1} resident bytes per entity, forever, to stop",
        counts.chunk_bytes as f64 / entities as f64);
    println!("  allocating one Arc per entity on each walk. That pays only if the walk runs often");
    println!("  enough to outweigh bytes that are never reclaimed -- and the walk is a dump,");
    println!("  rebuild and reporting path, not a serving one.");

    assert_eq!(
        entities,
        pool.len(),
        "the pool holds {} keys for {entities} distinct composed keys, so this is not pricing one \
         handle per key",
        pool.len()
    );
    assert!(
        counts.chunk_bytes > 0,
        "the pool allocated nothing, which cannot be right for {entities} keys"
    );
}

/// WHAT REPEATED FIELD NAMES COST THE MODEL MAPS, ON AN AXIS THIS CAMPAIGN NEVER MEASURED.
///
/// Every figure in this campaign has been about the object KEY. A hash object's FIELD names are a
/// second axis of the same invariant, and they are not shared at all: `HashFieldMap` holds
/// `Vec<(String, ElementEntry)>`, so each object owns a `String` per field. A `status` field across
/// ten thousand objects is ten thousand copies of one short name.
///
/// The index side already shares them. `intern_shared` draws component names from `kind_pool`, and
/// its own doc explains why the pool is CAPPED at 64: "a component name is not [bounded] -- it
/// comes from the caller -- and the cap is what makes sharing it safe anyway: past the cap a name
/// still works, it just allocates as it did before." So the mechanism and the safety argument both
/// exist; only the model side does not use them.
///
/// This prices the duplication before anything proposes removing it, on the shape that makes it
/// worst: many objects drawing field names from a small set, which is what a schema looks like.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_repeated_field_names_cost -- --ignored --nocapture
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds two field-map shapes at 4,000 objects; run by name, and ALONE -- the allocation \
            counters are process-global"]
fn what_repeated_field_names_cost_the_model_maps() {
    const OBJECTS: usize = 4_000;
    // A schema's worth of names, drawn from a small set -- which is the point.
    const NAMES: &[&str] = &[
        "status",
        "updated_at",
        "owner",
        "retention_class",
        "last_seen_region",
    ];

    let address = |i: usize| ElementEntry::from_parts(1, i as u64 * 64, 48, None, None);

    // AS IT IS: one owned `String` per field per object.
    let owned = {
        let probe = Probe::start();
        let mut maps: Vec<Vec<(String, ElementEntry)>> = Vec::with_capacity(OBJECTS);
        for object in 0..OBJECTS {
            let mut fields: Vec<(String, ElementEntry)> = Vec::with_capacity(NAMES.len());
            for (slot, name) in NAMES.iter().enumerate() {
                fields.push((name.to_string(), address(object * NAMES.len() + slot)));
            }
            maps.push(fields);
        }
        std::hint::black_box(&maps);
        probe.stop()
    };

    // SHARED, the way the index already shares a component: one handle per distinct name, taken
    // from a pool, and every object holding a clone of it.
    let shared = {
        let probe = Probe::start();
        let mut pool: std::collections::HashSet<std::sync::Arc<str>> =
            std::collections::HashSet::new();
        for name in NAMES {
            pool.insert(std::sync::Arc::from(*name));
        }
        let handles: Vec<std::sync::Arc<str>> = NAMES
            .iter()
            .map(|name| pool.get(&name[..]).expect("just inserted").clone())
            .collect();
        let mut maps: Vec<Vec<(std::sync::Arc<str>, ElementEntry)>> = Vec::with_capacity(OBJECTS);
        for object in 0..OBJECTS {
            let mut fields: Vec<(std::sync::Arc<str>, ElementEntry)> =
                Vec::with_capacity(NAMES.len());
            for (slot, handle) in handles.iter().enumerate() {
                fields.push((
                    std::sync::Arc::clone(handle),
                    address(object * NAMES.len() + slot),
                ));
            }
            maps.push(fields);
        }
        std::hint::black_box((&maps, &pool));
        probe.stop()
    };

    let fields = OBJECTS * NAMES.len();
    let per = |bytes: u64| bytes as f64 / fields as f64;

    println!();
    println!(
        "  {OBJECTS} objects x {} field names = {fields} fields, {} distinct names",
        NAMES.len(),
        NAMES.len()
    );
    println!();
    println!("    shape      allocations        bytes   per field");
    println!(
        "    owned      {:>11}   {:>10}   {:>9.1}",
        owned.allocs,
        owned.chunk_bytes,
        per(owned.chunk_bytes)
    );
    println!(
        "    shared     {:>11}   {:>10}   {:>9.1}",
        shared.allocs,
        shared.chunk_bytes,
        per(shared.chunk_bytes)
    );
    println!(
        "    saving     {:>11}   {:>10}   {:>9.1}",
        owned.allocs as i64 - shared.allocs as i64,
        owned.chunk_bytes as i64 - shared.chunk_bytes as i64,
        per(owned.chunk_bytes) - per(shared.chunk_bytes)
    );
    println!();
    println!("  A `String` field is 24 bytes of inline struct plus its characters; a shared handle");
    println!("  is 16 inline and no characters at all beyond the one pooled copy. Unlike interning");
    println!("  the composed entity key, THIS pool is bounded by the schema rather than by the");
    println!("  corpus -- which is the same argument `kind_pool`'s cap already makes.");
    println!();
    println!("  AND THE CONVERSION IS ARCHITECTURAL, NOT A SIGNATURE CHANGE -- checked, because");
    println!("  twice today I called something blocked and was wrong. `ElementMap::Element` is");
    println!("  `String` for `HashFieldMap`, and the three `insert_element` sites sit inside");
    println!("  `RecordedMap`'s generic operations, which hold `&mut` the map and NO access to the");
    println!("  shard's `kind_pool`. That pool is deliberately \"owned by the index rather than a");
    println!("  global, so the write path interns through the `&mut` it already holds and no lock");
    println!("  appears on it\". So every route is a decision:");
    println!();
    println!("    thread the pool into RecordedMap's ops   changes the declared operation set,");
    println!("                                             which is that module's stated invariant");
    println!("    intern before the proof is built         changes ElementMap::Element to Arc<str>");
    println!("                                             and the recorded-proof types with it");
    println!("    a static pool                            adds the lock the codebase rejected");
    println!();
    println!("  A per-map pool is the option that LOOKS cheap and defeats the purpose: the saving");
    println!("  above is cross-object sharing, and a pool per object shares nothing.");

    assert!(
        shared.chunk_bytes < owned.chunk_bytes,
        "the shared shape is not smaller ({} vs {}), so the comparison is measuring something \
         other than the field name",
        shared.chunk_bytes,
        owned.chunk_bytes
    );
    assert!(
        owned.allocs > 0 && shared.allocs > 0,
        "an arm allocated nothing, which cannot be right for {fields} fields"
    );
}
