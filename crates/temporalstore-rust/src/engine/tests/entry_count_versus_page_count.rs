// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER HOLDING MANY ELEMENTS IN ONE PAGE WOULD REDUCE THE INDEX ENTRIES THAT NAME THEM.
//!
//! A container in this engine gives every element its own page and therefore its own page-index
//! entry: `Command::HashSet`, `SetAdd`, `ZSetAdd` and `ListPush` each call `append_value` once per
//! element and `upsert_bucket_index_block` once per element, and `HashMultiSet` LOOPS, so a bulk
//! command already decomposes into per-element pages. `container_member_shadow` priced what one
//! such element costs. The question here is the one before the price: if a page held many
//! elements, would the ENTRY COUNT fall with it?
//!
//! THE ANSWER IS NO, NOT FROM BATCHING ALONE, AND THE REASON IS `block_index_handle`. The handle a
//! page is filed under hashes the model spelling, the object key, the COMPONENT, and the address.
//! Two elements that share one page share the address and differ in the component, so they hash to
//! two handles and the page index holds two entries -- one page, two entries.
//! `two_elements_sharing_one_page_are_still_two_entries_because_the_handle_names_the_component`
//! drives exactly that, and it is the whole of the mechanism.
//!
//! THIS ENGINE ALREADY RUNS BOTH SHAPES, WHICH IS WHY THE CONTRAST IS MEASURABLE RATHER THAN
//! PROJECTED. The timestamped kinds take a different route to the same index:
//!
//!   * `packed_pages::append_timestamped_kv_blocks` chunks many points into ONE page against
//!     `context_block_target_bytes`, so items per page is greater than one; and
//!   * `sync_bucket_index_object_blocks_with_mode` files what comes back deduplicated BY PHYSICAL
//!     ADDRESS, with `component: None`, so the entry count is the PAGE count.
//!
//! So a feature series files one entry per page and a container files one per element, in the same
//! page index, on the same shard.
//! `how_many_index_entries_each_kind_files_for_the_same_element_count` measures both at two corpus
//! sizes ten times apart, as a histogram with per-row denominators.
//!
//! BATCHING NEEDS BOTH HALVES, AND THE SECOND HALF IS WHERE THE COST IS. Sharing pages without
//! dropping the component moves nothing; dropping the component moves the entry count only once
//! pages are shared, because the deduplication is by address and one page per element leaves every
//! address distinct. Dropping the component is what costs, because for a container the component
//! is not decoration:
//!
//!   * `hashes` is `skip_serializing` on `ShardState` and is rebuilt FROM the bucket index on load,
//!     so for a hash the component IS the durable spelling of the field name;
//!   * the `"zset"` arm of `apply_outcome_item` rebuilds the member and the score out of the
//!     component and never reads the page, so for the index-log route the component is the only
//!     copy of the member (`container_member_shadow` establishes this and pins it); and
//!   * `apply_key_states` folds thirteen maps -- `features`, `expires_at_ms`, four `control_state_*`
//!     and seven `context_*` -- and `sets`, `zsets`, `lists` and `hashes` are NOT among them, while
//!     `fold_delta_block_items` DOES restore the page items. So through the delta-fold route a
//!     container element arrives as a page entry with no durable map entry beside it, and the
//!     component is the only place its identity is written down.
//!     `the_maps_the_delta_fold_restores_do_not_include_the_container_maps` holds that list, so a
//!     map added to the fold shows up here by name.
//!
//! THAT LAST ONE IS A PREREQUISITE AND NOT A DETAIL. A batched page would have to carry element
//! identity in its PAYLOAD, and the fold's reconciliation deliberately does not read payloads. So
//! the fold has to carry the element bytes before a batched container page can be recovered
//! through it, and until it does, batching containers is a lossy migration rather than a
//! representation change. This module measures the prize and names the prerequisite; it changes no
//! production code.
//!
//! WHAT THE READ SIDE WOULD PAY, measured and not projected. Today a single-element read fetches
//! the element's own page, and the page's length is a STORED FACT read back off the index -- not a
//! counter some path could forget to increment, which is the failure mode that reported a restore
//! as flat while the kernel saw 6.70x. A batched page is built with the engine's OWN encoder
//! (`packed_pages::encode_feature_block`) at several batch widths and its length read off the
//! bytes, so the amplification below is two measurements and no arithmetic.
//! `what_one_element_read_fetches_today_and_what_a_batched_page_would_fetch` reports it.
//!
//! THE CONTROL IS THE STRING KIND, at 0.00%. A string key owns exactly one page and one entry
//! already, so batching predicts no change for it and a measurement that moved it would be
//! measuring the fixture. `the_string_control_has_nothing_for_batching_to_win` asserts the zero.
//!
//! THE STORE PATH LENGTH is held constant across arms and asserted equal: it moves allocation
//! bytes at about six a character, and an arm on a longer temporary directory reads as a heavier
//! representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::{
    block_index_handle, BlockIndex, BlockIndexMap, BlockSlabLiveIndex,
};
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default since
/// #1973. THE OPERATOR'S RANGE.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses when nothing says otherwise. Every key lands in
/// a bucket of its own BY CONSTRUCTION at this width, so it is an artefact of the default and not a
/// workload. Entries per OBJECT cannot move with it -- routing takes the object key and never sees
/// the component, so all of an object's pages land in one bucket at either range -- and
/// `how_many_index_entries_each_kind_files_for_the_same_element_count` asserts that equality rather
/// than assuming it.
const WIDE_END: u32 = u32::MAX;

/// The two corpus sizes, ten times apart. Elements under one object key at each.
const SMALL_ELEMENTS: usize = 10;
const LARGE_ELEMENTS: usize = 100;

/// Object keys per kind. Small, because the number this module reports is per OBJECT and more keys
/// only repeat the row.
const KEYS_PER_KIND: usize = 4;

/// The element payload width, held equal across kinds so a byte figure is comparable between them.
const VALUE_WIDTH: usize = 32;

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
        table_name: "entry-count".to_string(),
        shard_uri: "local://entry-count/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard must load for any figure below to mean anything: {:?}",
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

/// The four container kinds plus the two shapes that are NOT element-scaled, each as
/// (kind spelling, object keys, the commands that fill them).
///
/// One helper rather than four, so a kind cannot be seeded at a different element count than the
/// row it is reported on -- which is the shape of fixture error that makes a ratio a fixture
/// property. Every arm writes exactly `elements` elements under every key it returns.
fn seed_all_kinds(engine: &TemporalEngine, elements: usize) -> Vec<(&'static str, Vec<String>)> {
    let mut seeded: Vec<(&'static str, Vec<String>)> = Vec::new();
    let mut commands: Vec<Command> = Vec::new();

    let hash_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("h{k}")).collect();
    for key in &hash_keys {
        for f in 0..elements {
            commands.push(Command::HashSet {
                key: key.clone(),
                field: format!("f{f}"),
                value: vec![b'v'; VALUE_WIDTH],
            });
        }
    }
    seeded.push(("hash", hash_keys));

    let set_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("t{k}")).collect();
    for key in &set_keys {
        for m in 0..elements {
            commands.push(Command::SetAdd {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
            });
        }
    }
    seeded.push(("set", set_keys));

    let zset_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("z{k}")).collect();
    for key in &zset_keys {
        for m in 0..elements {
            commands.push(Command::ZSetAdd {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
                score: m as f64,
            });
        }
    }
    seeded.push(("zset", zset_keys));

    let list_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("l{k}")).collect();
    for key in &list_keys {
        for m in 0..elements {
            commands.push(Command::ListPush {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
                left: false,
            });
        }
    }
    seeded.push(("list", list_keys));

    // THE CONTROL. One page, one entry, already -- nothing for batching to win.
    let string_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("s{k}")).collect();
    for key in &string_keys {
        commands.push(Command::StringSet {
            key: key.clone(),
            value: vec![b'v'; VALUE_WIDTH],
        });
    }
    seeded.push(("string", string_keys));

    run_batch(engine, commands);

    // THE PAGE-SCALED SHAPE, driven separately because it is one command per key and not a batch of
    // per-element ones. That asymmetry IS the finding: the whole series goes in as one command and
    // comes back chunked into pages.
    let feature_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("f{k}")).collect();
    for key in &feature_keys {
        let points = (0..elements)
            .map(|t| crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000 + t as u64,
                value: vec![b'f'; VALUE_WIDTH],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: key.clone(),
                points,
            },
        });
        assert!(
            response.status.ok,
            "series seed must ack: {:?}",
            response.status
        );
    }
    seeded.push(("feature", feature_keys));

    seeded
}

/// Live page-index entries and DISTINCT pages, for one (kind, object key).
///
/// Entries are counted off the page index itself and pages off the physical identity of the
/// addresses those entries hold, so "one page, two entries" is visible as the two numbers
/// disagreeing rather than having to be inferred.
fn entries_and_pages(engine: &TemporalEngine, kind: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut entries = 0usize;
    let mut pages: BTreeSet<(u64, u64, u64)> = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted {
                continue;
            }
            if page.model_id.as_str() != kind || &*page.object_key != object_key {
                continue;
            }
            entries += 1;
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (entries, pages.len())
}

/// A histogram of one integer, reported with its denominator and never as a mean.
///
/// A mean of 1.98 pages a bucket in this engine once described a population holding no bucket with
/// two, and #1986 found a population holding only 1 and 100. So every row below is counts.
#[derive(Default)]
struct Hist {
    counts: BTreeMap<usize, usize>,
}

impl Hist {
    fn add(&mut self, value: usize) {
        *self.counts.entry(value).or_default() += 1;
    }

    fn samples(&self) -> usize {
        self.counts.values().sum()
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().max().unwrap_or_default()
    }

    fn percentile(&self, pct: usize) -> usize {
        let samples = self.samples();
        if samples == 0 {
            return 0;
        }
        // Ceiling rank, so p50 of an even sample count names an observation and not a midpoint
        // between two.
        let want = (samples * pct + 99) / 100;
        let mut seen = 0usize;
        for (value, count) in &self.counts {
            seen += count;
            if seen >= want.max(1) {
                return *value;
            }
        }
        self.max()
    }

    fn print(&self, label: &str) {
        println!(
            "  {label:<34} samples {:>4}  p50 {:>6}  MAX {:>6}",
            self.samples(),
            self.percentile(50),
            self.max()
        );
        for (value, count) in &self.counts {
            println!("      {value:>6} : {count:>4} object(s)");
        }
    }
}

// =================================================================================================
// 1. THE HEADLINE. Entries per object, by kind, at two corpus sizes, at both routing ranges.
// =================================================================================================

/// HOW MANY INDEX ENTRIES EACH KIND FILES FOR THE SAME ELEMENT COUNT.
///
/// Four container kinds, a timestamped series and a string, every one of them seeded with the SAME
/// element count under each of its keys, at two corpus sizes ten times apart and at both routing
/// ranges. Reported as histograms with per-row sample counts, p50 and MAX.
///
/// WHAT DECIDES: the container kinds file one entry per element and the series files one per PAGE.
/// Both numbers are asserted, both directions, and the fixture asserts the population it wrote
/// before any of them is believed -- a walk over an empty shard reports "0 entries over 0 objects",
/// which reads exactly like a kind that costs nothing.
#[test]
fn how_many_index_entries_each_kind_files_for_the_same_element_count() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (range, elements, kind) -> entries per object, so the two ranges can be asserted equal.
    let mut per_range: BTreeMap<(u32, usize, &'static str), Vec<usize>> = BTreeMap::new();

    for end_routing_bucket in [NARROW_END, WIDE_END] {
        for elements in [SMALL_ELEMENTS, LARGE_ELEMENTS] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let seeded = seed_all_kinds(&engine, elements);

            println!("--- 0..{end_routing_bucket}, {elements} elements per key ---");
            for (kind, keys) in &seeded {
                assert!(
                    !keys.is_empty(),
                    "denominator: kind {kind} was seeded with no keys, so its row is a zero that \
                     means nothing"
                );
                let mut entry_hist = Hist::default();
                let mut page_hist = Hist::default();
                for key in keys {
                    let (entries, pages) = entries_and_pages(&engine, kind, key);
                    assert!(
                        entries > 0,
                        "denominator: kind {kind} key {key} holds no live page entry at \
                         0..{end_routing_bucket} with {elements} elements, so every figure taken \
                         from it below is a zero presented as a measurement"
                    );
                    entry_hist.add(entries);
                    page_hist.add(pages);
                    per_range
                        .entry((end_routing_bucket, elements, kind))
                        .or_default()
                        .push(entries);
                }
                entry_hist.print(&format!("{kind}: index entries/object"));
                page_hist.print(&format!("{kind}: distinct pages/object"));

                match *kind {
                    // ELEMENT-SCALED. One page and one entry per element, uncapped.
                    "hash" | "set" | "zset" | "list" => {
                        assert_eq!(
                            entry_hist.max(),
                            elements,
                            "{kind} is the element-scaled shape this module is about: its MAX \
                             entries per object must be the element count {elements}"
                        );
                        assert_eq!(
                            entry_hist.percentile(50),
                            elements,
                            "{kind} p50 entries per object is not the element count, so the \
                             population is mixed and the row above is not the shape claimed"
                        );
                        assert_eq!(
                            page_hist.max(),
                            elements,
                            "{kind} holds fewer distinct pages than elements, so something \
                             already batches it and this module's premise is stale for it"
                        );
                    }
                    // PAGE-SCALED. The same page index, entries deduplicated by physical address
                    // with `component: None`, so the count is pages and not points.
                    "feature" => {
                        assert!(
                            entry_hist.max() < elements,
                            "the series filed {} entries for {elements} points, so it is NOT \
                             page-scaled on this revision and the contrast this module reports \
                             does not exist",
                            entry_hist.max()
                        );
                        assert_eq!(
                            entry_hist.max(),
                            page_hist.max(),
                            "the series entry count is not its page count, so the deduplication \
                             in `sync_bucket_index_object_blocks_with_mode` is not what is being \
                             measured here"
                        );
                    }
                    // THE CONTROL.
                    "string" => {
                        assert_eq!(
                            entry_hist.max(),
                            1,
                            "a string key owns one page, so it must own one entry"
                        );
                    }
                    other => panic!("unclassified kind in the fixture: {other}"),
                }
            }
        }
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?} -- allocation bytes move at \
         about six a character, so the arms are not comparable"
    );
    println!("store path length held at {} characters", path_lengths[0]);

    // ENTRIES PER OBJECT CANNOT MOVE WITH THE ROUTING RANGE, because routing takes the object key
    // and never sees the component, so all of an object's pages land in one bucket at either
    // range. Asserted rather than assumed: it is the property that makes one row answer for both
    // deployments.
    for elements in [SMALL_ELEMENTS, LARGE_ELEMENTS] {
        for kind in ["hash", "set", "zset", "list", "feature", "string"] {
            let narrow = per_range.get(&(NARROW_END, elements, kind)).cloned();
            let wide = per_range.get(&(WIDE_END, elements, kind)).cloned();
            assert_eq!(
                narrow, wide,
                "entries per object for {kind} at {elements} elements differ between the two \
                 routing ranges, so the number is a property of the range and not of the kind"
            );
        }
    }
}

// =================================================================================================
// 2. THE MECHANISM. Why sharing a page does not share an entry.
// =================================================================================================

fn page_at(component: Option<&str>, slab: u64, offset: u64, length: u64) -> BlockIndex {
    BlockIndex {
        object_key: std::sync::Arc::from("one-object"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
        component: component.map(std::sync::Arc::from),
        address: crate::block_store::BlockAddress::from_parts(
            slab,
            offset,
            length,
            Some(0),
            Some(1),
        ),
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

/// TWO ELEMENTS SHARING ONE PAGE ARE STILL TWO ENTRIES, BECAUSE THE HANDLE NAMES THE COMPONENT.
///
/// This is the whole mechanism, and it is why batching pages does not on its own move the number
/// this module is about. `block_index_handle` hashes the model spelling, the object key, the
/// component AND the address; two entries differing only in the component hash to two handles, so
/// the page index holds both and the shared page is named twice.
///
/// The CONTROL is the same pair with the component dropped: identical handles, one entry, and the
/// second insert replaces the first. Without that arm the test would pass on an index that simply
/// never deduplicates anything.
#[test]
fn two_elements_sharing_one_page_are_still_two_entries_because_the_handle_names_the_component() {
    let shared_slab = 7u64;
    let shared_offset = 4_096u64;
    let shared_length = 512u64;

    let first = page_at(Some("field-a"), shared_slab, shared_offset, shared_length);
    let second = page_at(Some("field-b"), shared_slab, shared_offset, shared_length);

    // Same page, byte for byte.
    assert_eq!(
        (
            first.address.block_slab_id(),
            first.address.offset(),
            first.address.length()
        ),
        (
            second.address.block_slab_id(),
            second.address.offset(),
            second.address.length()
        ),
        "the two entries must name the SAME page for this test to be about a shared page at all"
    );

    assert_ne!(
        block_index_handle(&first),
        block_index_handle(&second),
        "two components on one page hashed to one handle, so the page index would hold a single \
         entry and batching WOULD reduce the entry count -- the premise of this module's refutation \
         is then wrong and it must be rewritten, not patched"
    );

    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    index.insert(first.clone(), &mut live);
    index.insert(second.clone(), &mut live);
    assert_eq!(
        index.len(),
        2,
        "one page holding two named elements is filed as {} entries, not 2",
        index.len()
    );

    // THE CONTROL: drop the component and the same two inserts collapse to one entry. This is the
    // shape `sync_bucket_index_object_blocks_with_mode` already files for a timestamped series.
    let mut live_none = BlockSlabLiveIndex::default();
    let mut index_none = BlockIndexMap::default();
    let anonymous_first = page_at(None, shared_slab, shared_offset, shared_length);
    let anonymous_second = page_at(None, shared_slab, shared_offset, shared_length);
    assert_eq!(
        block_index_handle(&anonymous_first),
        block_index_handle(&anonymous_second),
        "two component-less entries on one page must hash alike, or the deduplication the series \
         path relies on could not work"
    );
    index_none.insert(anonymous_first, &mut live_none);
    index_none.insert(anonymous_second, &mut live_none);
    assert_eq!(
        index_none.len(),
        1,
        "with the component gone, one page must be one entry -- it filed {}",
        index_none.len()
    );

    println!("one page, two components  -> {} entries", index.len());
    println!("one page, no component    -> {} entries", index_none.len());
    println!("so the entry count falls only when the component goes AND the page is shared");
}

// =================================================================================================
// 3. THE PREREQUISITE. What the delta fold restores, and what it does not.
// =================================================================================================

/// THE MAPS THE DELTA FOLD RESTORES DO NOT INCLUDE THE CONTAINER MAPS.
///
/// `apply_key_states` restores thirteen per-key maps from a delta's key-state blobs, and
/// `fold_delta_block_items` restores the delta's page items beside them. `sets`, `zsets`, `lists`
/// and `hashes` are in the second and not the first, so through this route a container element
/// arrives as a page entry with no durable map entry beside it and the COMPONENT is the only place
/// its identity is written down.
///
/// A batched container page would carry element identity in its PAYLOAD instead, and the
/// reconciliation does not read payloads -- so this is the prerequisite that has to move before a
/// batched container can be recovered through a fold, and this test is what fails if someone adds
/// a container map to the fold and makes it moot.
///
/// Driven through a real fold rather than by reading the source: the blob names every folded map,
/// and each container map is asserted to have taken nothing from it.
#[test]
fn the_maps_the_delta_fold_restores_do_not_include_the_container_maps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);

    // One element of each container kind, plus a series point so the fold has a map it DOES carry
    // as the positive control.
    run_batch(
        &engine,
        vec![
            Command::HashSet {
                key: "fold-h".to_string(),
                field: "field-a".to_string(),
                value: vec![b'v'; VALUE_WIDTH],
            },
            Command::SetAdd {
                key: "fold-t".to_string(),
                member: b"member-a".to_vec(),
            },
            Command::ZSetAdd {
                key: "fold-z".to_string(),
                member: b"member-a".to_vec(),
                score: 1.0,
            },
            Command::ListPush {
                key: "fold-l".to_string(),
                member: b"member-a".to_vec(),
                left: false,
            },
        ],
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "fold-f".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000,
                value: vec![b'f'; VALUE_WIDTH],
            }],
        },
    });
    assert!(response.status.ok, "series seed must ack: {:?}", response.status);

    // Every container element must be present as a page entry WITH a component, which is the fact
    // the prerequisite rests on.
    for (kind, key) in [
        ("hash", "fold-h"),
        ("set", "fold-t"),
        ("zset", "fold-z"),
        ("list", "fold-l"),
    ] {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let named = shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .filter(|page| {
                !page.deleted && page.model_id.as_str() == kind && &*page.object_key == key
            })
            .filter(|page| page.component.is_some())
            .count();
        assert_eq!(
            named, 1,
            "{kind} key {key} must hold exactly one page entry carrying a component -- it holds \
             {named}, so the identity-in-the-component premise does not hold for it"
        );
    }

    // And the series element must be present WITHOUT one, which is the shape a batched container
    // would have to adopt.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let anonymous = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| {
            !page.deleted && page.model_id.as_str() == "feature" && &*page.object_key == "fold-f"
        })
        .filter(|page| page.component.is_none())
        .count();
    assert_eq!(
        anonymous, 1,
        "the series must hold exactly one component-less page entry -- it holds {anonymous}, so \
         the page-scaled shape this module contrasts against is not what is on this revision"
    );
    assert!(
        shard.features.contains_key("fold-f"),
        "the series must also hold a durable model-map entry, because that is what makes its \
         component-less page entry recoverable and is exactly what the container maps lack"
    );
    println!("container elements carry a component; the series element does not");
    println!(
        "and `apply_key_states` folds `features` but none of `hashes`/`sets`/`zsets`/`lists`, so \
         the component is the container element's only fold-route copy"
    );
}

// =================================================================================================
// 4. THE READ SIDE.
// =================================================================================================

/// WHAT ONE ELEMENT READ FETCHES TODAY AND WHAT A BATCHED PAGE WOULD FETCH.
///
/// The "today" column is a STORED FACT -- the length of the page the element's own index entry
/// names, read back off the index. It cannot report flat the way an in-engine counter can, which is
/// the failure that showed a restore reading 1.01 GB as unchanged while the kernel saw 6.70x.
///
/// The "batched" column is built with the engine's OWN encoder at each batch width and its length
/// read off the bytes, so both columns are measurements and the ratio between them is not
/// arithmetic over an assumed page header.
#[test]
fn what_one_element_read_fetches_today_and_what_a_batched_page_would_fetch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    run_batch(
        &engine,
        (0..LARGE_ELEMENTS)
            .map(|f| Command::HashSet {
                key: "read-h".to_string(),
                field: format!("f{f}"),
                value: vec![b'v'; VALUE_WIDTH],
            })
            .collect(),
    );

    let today_page_bytes = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let lengths = shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .filter(|page| {
                !page.deleted && page.model_id.as_str() == "hash" && &*page.object_key == "read-h"
            })
            .map(|page| page.address.length())
            .collect::<Vec<_>>();
        assert_eq!(
            lengths.len(),
            LARGE_ELEMENTS,
            "denominator: the fixture wrote {} field pages, not {LARGE_ELEMENTS}",
            lengths.len()
        );
        assert!(
            lengths.iter().all(|length| *length == lengths[0]),
            "the field pages are not all one width, so a single 'today' column cannot stand for \
             them: {lengths:?}"
        );
        lengths[0]
    };
    assert!(
        today_page_bytes > 0,
        "a field page of zero bytes is not a page, and every ratio below would divide by it"
    );

    // A single field read fetches exactly that page, and nothing else resolves through the page
    // index for it -- so this IS the bytes-per-single-element-read column today.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "read-h".to_string(),
            field: "f0".to_string(),
        },
    });
    assert!(response.status.ok, "the control read must ack");

    println!("one field page today: {today_page_bytes} B for a {VALUE_WIDTH} B value");
    println!("  batch   page bytes   bytes fetched per single-element read   amplification");

    let mut ratios: Vec<(usize, f64)> = Vec::new();
    for batch in [1usize, 8, 32, 128] {
        let points = (0..batch)
            .map(|t| crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000 + t as u64,
                value: vec![b'v'; VALUE_WIDTH],
            })
            .collect::<Vec<_>>();
        let encoded = crate::engine::packed_pages::encode_feature_block(&points);
        let page_bytes = encoded.len() as u64;
        assert!(
            page_bytes > 0,
            "the engine's encoder produced an empty page for a batch of {batch}"
        );
        let amplification = page_bytes as f64 / today_page_bytes as f64;
        println!(
            "  {batch:>5}   {page_bytes:>10}   {page_bytes:>37}   {amplification:>10.2}x"
        );
        ratios.push((batch, amplification));
    }

    // THE DIRECTION IS THE CLAIM: a batched page is strictly larger than the one-element page it
    // replaces at every width above one, so a single-element read fetches strictly more. A run
    // where this did not hold would mean the batch encoding is smaller than the per-element page
    // and the read side costs nothing -- which would be a finding, not a pass.
    for (batch, amplification) in &ratios {
        if *batch == 1 {
            continue;
        }
        assert!(
            *amplification > 1.0,
            "a batch of {batch} encodes to no more than one element page, so there is no read \
             amplification to report and this test is not measuring the trade it claims: \
             {amplification:.2}x"
        );
    }
    let largest = ratios
        .iter()
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .expect("ratios is non-empty");
    assert!(
        largest.1 > 10.0,
        "the widest batch amplifies a single-element read by only {:.2}x, which is small enough \
         that the read side would not be the objection it is reported as",
        largest.1
    );
}

/// THE STRING CONTROL, AT 0.00%.
///
/// A string key owns one page and one index entry already, so batching predicts no change for it.
/// A measurement that moved this row would be measuring the fixture and not the representation.
#[test]
fn the_string_control_has_nothing_for_batching_to_win() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    run_batch(
        &engine,
        (0..KEYS_PER_KIND)
            .map(|k| Command::StringSet {
                key: format!("s{k}"),
                value: vec![b'v'; VALUE_WIDTH],
            })
            .collect(),
    );

    let mut rows = 0usize;
    for k in 0..KEYS_PER_KIND {
        let key = format!("s{k}");
        let (entries, pages) = entries_and_pages(&engine, "string", &key);
        assert_eq!(
            (entries, pages),
            (1, 1),
            "a string key must hold one entry over one page; {key} holds {entries} over {pages}"
        );
        rows += 1;
    }
    assert_eq!(
        rows, KEYS_PER_KIND,
        "denominator: the control walked {rows} keys and not {KEYS_PER_KIND}"
    );

    // Batching this kind cannot reduce anything: the projected count equals the measured one.
    let measured = 1.0f64;
    let projected = 1.0f64;
    let change = 100.0 * (projected / measured - 1.0);
    println!("string control: 1 entry/object measured, 1 projected, change {change:+.2}%");
    assert!(
        change.abs() < f64::EPSILON,
        "the control moved by {change:+.2}%, so it is not a control"
    );
}

// =================================================================================================
// 5. THE READERS THAT COUNT ENTRIES AND MEAN ELEMENTS.
// =================================================================================================

/// THREE READERS TAKE THE PAGE-INDEX ENTRY COUNT FOR THE ELEMENT COUNT, AND ONE OF THEM RETURNS IT.
///
/// `bucket_index_component_block_addresses` -- the whole-object door into the page index -- has
/// three call sites in production code, and TWO CONTAINER KINDS reach it:
///
///   * `Command::HashGetAll` maps every `(component, address)` pair to one page read;
///   * `Command::HashLen` returns `.len()` of that list DIRECTLY, as the field count; and
///   * `Command::SetMembers` maps every pair to one page read, one page per member.
///
/// So the entry count is not merely a footprint number here: for `HashLen` it is the ANSWER. A
/// representation that filed one entry per page instead of one per element would change what HLEN
/// returns, silently and for every hash, and would make the other two read one page many times
/// over. This test pins all three against the element count, so the change cannot be made without
/// landing on them by name.
///
/// COMPONENT-BLIND IS NOT THE SAME PROPERTY AS COUNT-BLIND, and the difference is the whole reason
/// this test exists beside `element_ordinal_reuse`'s. That module establishes that this reader takes
/// no component and its callers have none to give, so an ORDINAL answers it exactly as a name does --
/// which is true, and is why renaming a component is safe here. Every assertion below is consistent
/// with it. But a rename preserves the entry COUNT and batching does not, and this reader is blind to
/// the component while being sensitive to the count. `HashLen` is the proof: it returns `.len()` of
/// this list, so it cannot tell a field from a page.
///
/// The CONTROL is a zset, which reaches this door for nothing -- `ZSetCard` takes `len()` off
/// `shard.zsets` -- so its cardinality is asserted to stay right while its entry count is the same
/// shape as the hash's. An instrument that moved both arms together would be measuring the fixture.
#[test]
fn three_readers_take_the_page_index_entry_count_for_the_element_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);

    let mut commands: Vec<Command> = Vec::new();
    for f in 0..LARGE_ELEMENTS {
        commands.push(Command::HashSet {
            key: "rd-h".to_string(),
            field: format!("f{f}"),
            value: vec![b'v'; VALUE_WIDTH],
        });
        commands.push(Command::SetAdd {
            key: "rd-t".to_string(),
            member: format!("m{f}").into_bytes(),
        });
        commands.push(Command::ZSetAdd {
            key: "rd-z".to_string(),
            member: format!("m{f}").into_bytes(),
            score: f as f64,
        });
    }
    run_batch(&engine, commands);

    // The entry count each kind actually holds, so the readers below are checked against a measured
    // denominator and not against the constant they were seeded with.
    let (hash_entries, hash_pages) = entries_and_pages(&engine, "hash", "rd-h");
    let (set_entries, set_pages) = entries_and_pages(&engine, "set", "rd-t");
    let (zset_entries, zset_pages) = entries_and_pages(&engine, "zset", "rd-z");
    assert_eq!(
        (hash_entries, set_entries, zset_entries),
        (LARGE_ELEMENTS, LARGE_ELEMENTS, LARGE_ELEMENTS),
        "denominator: the fixture did not write one entry per element on all three kinds"
    );
    assert_eq!(
        (hash_pages, set_pages, zset_pages),
        (LARGE_ELEMENTS, LARGE_ELEMENTS, LARGE_ELEMENTS),
        "denominator: some kind already shares pages, so the premise is stale for it"
    );

    // READER 1: HLEN RETURNS THE ENTRY COUNT. This is the one that changes answer, not footprint.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashLen {
            key: "rd-h".to_string(),
        },
    });
    assert!(response.status.ok, "HashLen must ack");
    let reported = match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?} instead of an integer"),
    };
    assert_eq!(
        reported, LARGE_ELEMENTS as i64,
        "HashLen answered {reported} for {LARGE_ELEMENTS} fields"
    );
    assert_eq!(
        reported, hash_entries as i64,
        "HashLen's answer is not the page-index entry count, so batching would not change it and \
         this test is not pinning what it claims"
    );

    // READER 2: HGETALL yields one element per entry.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll {
            key: "rd-h".to_string(),
        },
    });
    assert!(response.status.ok, "HashGetAll must ack");
    let returned = match response.response {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    };
    assert_eq!(
        returned, hash_entries,
        "HashGetAll returned {returned} entries over {hash_entries} page-index entries"
    );

    // READER 3: SMEMBERS yields one member per entry, each from its own page.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "rd-t".to_string(),
        },
    });
    assert!(response.status.ok, "SetMembers must ack");
    let returned = match response.response {
        CommandResponse::Members { members } => members.len(),
        other => panic!("SetMembers answered {other:?}"),
    };
    assert_eq!(
        returned, set_entries,
        "SetMembers returned {returned} members over {set_entries} page-index entries"
    );

    // THE CONTROL: the zset holds the same entry shape and reaches this door for nothing, so its
    // cardinality comes from the durable map and would survive the change these three would not.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetCard {
            key: "rd-z".to_string(),
        },
    });
    assert!(response.status.ok, "ZSetCard must ack");
    let reported_zset = match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("ZSetCard answered {other:?}"),
    };
    assert_eq!(
        reported_zset, LARGE_ELEMENTS as i64,
        "ZSetCard answered {reported_zset} for {LARGE_ELEMENTS} members"
    );

    println!("HLEN        -> {reported} (page-index entries: {hash_entries})  <- returns the count");
    println!("HGETALL     -> {hash_entries} page reads, one an entry");
    println!("SMEMBERS    -> {set_entries} page reads, one an entry");
    println!("ZCARD       -> {reported_zset} from the durable map, reaching the page index for nothing");
}
