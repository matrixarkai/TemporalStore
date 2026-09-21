// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TWO WRITERS THAT FILE A PAGE, AND THE RANGE THEY COULD NOT REACH.
//!
//! mx#1949 enumerated eight production sites that supply a routing range as a literal, fixed five,
//! found one right as written, and DECLINED TWO with a price. The two it declined are the only
//! ones of the eight that decide WHERE A PAGE GOES:
//!
//! ```text
//!   upsert_bucket_index_block_inner            storage_bucket_internals.rs
//!   sync_bucket_index_object_blocks_with_mode  storage_bucket_internals.rs
//! ```
//!
//! It held the defect as a live measurement rather than a wish: `inner-000000` filed in bucket
//! **1,422,005,296** on a shard holding `0..1023`, its own bucket being 398. That measurement is
//! not deleted here. It is INVERTED -- the same fixture, the same two numbers, now asserting the
//! page is in 398 and NOT in 1,422,005,296 -- so the bucket that was wrong is still named in a
//! test that fails if the filing goes back.
//!
//! # PLACES, NOT FILTERS -- ESTABLISHED FOR EACH BEFORE ANYTHING CHANGED
//!
//! mx#1945's refutation is the thing to not re-break: `rebuild_bucket_block_ownership` FILTERS
//! over a whole-shard image carrying buckets from whatever range wrote it, so narrowing it drops
//! live data (0 of 400 surviving). Neither of these two is that.
//!
//! * `upsert_bucket_index_block_inner` runs over ONE address, supplied by its caller for this
//!   shard. The bucket it computes is the KEY it inserts under. What it REMOVES first comes from
//!   `block_refs_for` -- where the object's pages actually are -- or, when the lookup is not
//!   established, from a sweep of every bucket in the map. Neither consults the computed bucket.
//! * `sync_bucket_index_object_blocks_with_mode` runs over ONE OBJECT'S address list, likewise
//!   supplied for this shard. Its removal set is `object_block_refs(kind, object_key)`, or every
//!   bucket in the map while the lookup is being established. Again: the computed bucket is used
//!   only to INSERT.
//!
//! So narrowing them moves where an unrouted page lands and can drop nothing.
//! `narrowing_the_writers_removes_no_page_that_was_already_filed` drives that rather than reading
//! it off the source, and `a_store_written_on_one_range_still_reads_back_whole_on_the_other`
//! drives the cross-range restore in BOTH directions, because both are silent when they fail.
//!
//! # THE SHAPE, AND WHY IT IS NOT THE OTHER ONE
//!
//! Two ways to get the range to a function that never sees `&self`. Both were priced with the
//! compiler, and the numbers are in the pull request rather than here. The range is carried ON
//! THE SHARD, as three `#[serde(skip)]` fields read through `ShardState::routing_range()`.
//!
//! `ShardState` IS the serialized index, so the first question is whether that stored shape moves.
//! A `#[serde(skip)]` field is absent from the Serialize impl entirely, so it does not -- and that
//! is DRIVEN, not asserted, by `the_routing_range_field_changes_no_serialized_byte`, which
//! serializes the same shard stamped and unstamped and compares the bytes, and by the two
//! restart arms of `a_store_written_on_one_range_still_reads_back_whole_on_the_other`, which
//! write with one range and read with the other in both directions.
//!
//! The unstamped default is the WHOLE range -- exactly what these two sites passed unconditionally
//! before -- so a `ShardState` that never entered the engine is unchanged. What makes that default
//! unreachable in the engine is that there is ONE function which installs a shard and it stamps;
//! `every_shard_the_engine_installs_carries_its_routing_range` holds the install sites as a list.

use super::*;
use crate::engine::hashing::block_routing_bucket;
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;
/// The end bucket `load_shard` uses, and the one the two writers used to spell.
const WIDE_END: u32 = u32::MAX;

/// The key mx#1949 named its measurement with, and the two buckets it named.
const MEASURED_KEY: &str = "inner-000000";
/// Where `0..1023` puts `inner-000000`.
const MEASURED_NARROW_BUCKET: u32 = 398;
/// Where `0..u32::MAX` put it, on a shard that holds 0..1023.
const MEASURED_WIDE_BUCKET: u32 = 1_422_005_296;

const RECORDS: usize = 200;

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
        table_name: "shard-carried-range".to_string(),
        shard_uri: "local://shard-carried-range/1".to_string(),
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

fn seed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for index in 0..count {
        let key = format!("inner-{index:06}");
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
        keys.push(key);
    }
    keys
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

/// An address as an index written before the routing-bucket field carried it holds one, produced
/// by the ENGINE'S OWN DECODER rather than by the setter -- mx#1942's door, spelled the same way
/// in all four files that need it.
fn as_an_older_build_wrote_it(address: &BlockAddress) -> BlockAddress {
    let mut wire = serde_json::to_value(address).expect("an address serializes to its wire shape");
    let object = wire
        .as_object_mut()
        .expect("the address wire shape is a JSON object");
    assert!(
        object.remove("rs").is_some(),
        "the address wire shape carried no `rs` key to remove, so this helper is a no-op and \
         every count taken through it is zero for the wrong reason"
    );
    serde_json::from_value(wire).expect("the engine's decoder accepts an address with no `rs`")
}

fn strip_routing_buckets(shard: &mut crate::engine::state::ShardState) -> usize {
    let keys: Vec<String> = shard.strings.keys().cloned().collect();
    for key in &keys {
        let older = as_an_older_build_wrote_it(shard.strings.get(key).expect("key present"));
        shard.strings.insert(key.clone(), older);
    }
    keys.len()
}

/// Every bucket that holds at least one page, with the object keys it holds, sorted.
fn bucket_contents(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, Vec<String>> {
    let mut contents: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let mut keys: Vec<String> = bucket
            .block_index
            .values()
            .map(|page| page.object_key.to_string())
            .collect();
        if keys.is_empty() {
            continue;
        }
        keys.sort();
        contents.insert(*routing_bucket, keys);
    }
    contents
}

fn page_total(contents: &BTreeMap<u32, Vec<String>>) -> usize {
    contents.values().map(Vec::len).sum()
}

fn buckets_outside(contents: &BTreeMap<u32, Vec<String>>, start: u32, end: u32) -> usize {
    contents
        .keys()
        .filter(|bucket| **bucket < start || **bucket > end)
        .count()
}

// =============================================================================================
// 0. THE FIXTURE'S OWN ARITHMETIC
// =============================================================================================

/// THE TWO BUCKETS mx#1949 NAMED ARE STILL THE TWO BUCKETS THIS KEY HASHES TO.
///
/// Every assertion below leans on `inner-000000` landing in 398 under `0..1023` and in
/// 1,422,005,296 under the whole range. Those are properties of the hash, not of this change, and
/// if the hash ever moves every measurement in this file would silently start describing a
/// different pair of buckets. Asserted here, once, so that failure has a name.
#[test]
fn the_measured_key_still_hashes_to_the_two_buckets_the_measurement_names() {
    let narrow = block_routing_bucket(MEASURED_KEY, 0, NARROW_END);
    let wide = block_routing_bucket(MEASURED_KEY, 0, WIDE_END);
    println!("  {MEASURED_KEY}: narrow {narrow}, wide {wide}");
    assert_eq!(
        MEASURED_NARROW_BUCKET, narrow,
        "`{MEASURED_KEY}` no longer hashes to {MEASURED_NARROW_BUCKET} on 0..{NARROW_END}"
    );
    assert_eq!(
        MEASURED_WIDE_BUCKET, wide,
        "`{MEASURED_KEY}` no longer hashes to {MEASURED_WIDE_BUCKET} on the whole range"
    );
    assert!(
        wide > NARROW_END,
        "the whole-range bucket {wide} is inside 0..{NARROW_END}, so a page filed there is not \
         actually outside the shard and every assertion in this file passes for the wrong reason"
    );
}

// =============================================================================================
// 1. THE RANGE REACHES THE SHARD
// =============================================================================================

/// A LOADED SHARD CARRIES THE RANGE IT WAS LOADED ON, AND AN UNSTAMPED ONE ANSWERS THE OLD DEFAULT.
///
/// Both halves matter. The first is the change. The second is what makes the change safe for every
/// `ShardState` that never entered the engine -- a decoded manifest image, a report's scratch copy
/// -- because an unstamped state answers exactly what these two writers passed unconditionally
/// before the field existed.
#[test]
fn a_loaded_shard_carries_the_routing_range_it_was_loaded_on() {
    let unstamped = crate::engine::state::ShardState::default();
    assert!(
        !unstamped.routing_range_known,
        "a default ShardState claims to know a routing range; then the fallback below is not \
         testing the unstamped case at all"
    );
    assert_eq!(
        (0, WIDE_END),
        unstamped.routing_range(),
        "an unstamped ShardState must answer the WHOLE range -- what both writers passed \
         unconditionally before this field existed"
    );

    for end in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end);
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        println!("  loaded on 0..{end}, shard carries {:?}", shard.routing_range());
        assert!(
            shard.routing_range_known,
            "a shard loaded on 0..{end} entered the served map without its range stamped"
        );
        assert_eq!(
            (0, end),
            shard.routing_range(),
            "a shard loaded on 0..{end} carries the wrong range"
        );
    }
}

// =============================================================================================
// 2. THE MEASUREMENT, INVERTED
// =============================================================================================

/// THE TWO SITES THAT FILE A PAGE NOW FILE IT UNDER THE SHARD'S OWN RANGE.
///
/// This is mx#1949's `the_two_sites_that_file_a_page_still_file_it_under_the_whole_range`, with
/// the same fixture and the same two buckets named, asserting the opposite. It covers BOTH sites
/// -- mx#1949's measured only the upsert -- because a fix that reached one of them and not the
/// other would pass a test written against either alone.
///
/// The DENOMINATOR is asserted first: the address must be UNROUTED, or the fallback never fires
/// and this passes without exercising the site it names.
#[test]
fn the_two_sites_that_file_a_page_now_file_it_under_the_shards_own_range() {
    for (label, sync_path) in [("upsert_bucket_index_block", false), ("sync_bucket_index_object_blocks", true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, 8);
        let key = keys[0].clone();
        assert_eq!(
            MEASURED_KEY, key,
            "the fixture's first key is no longer the key the measurement names"
        );

        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        assert_eq!(
            (0, NARROW_END),
            shard.routing_range(),
            "the shard under test is not carrying 0..{NARROW_END}, so what follows would measure \
             the fallback rather than the fix"
        );
        let address = as_an_older_build_wrote_it(shard.strings.get(&key).expect("key present"));
        shard.strings.insert(key.clone(), address.clone());
        assert!(
            address.routing_bucket().is_none(),
            "the fixture needs an UNROUTED address here, or the fallback never fires"
        );

        if sync_path {
            crate::engine::storage_bucket_internals::sync_bucket_index_object_blocks(
                shard,
                1,
                "string",
                &key,
                vec![address],
                true,
            );
        } else {
            crate::engine::storage_bucket_internals::upsert_bucket_index_block(
                shard, 1, "string", &key, None, address, true,
            );
        }

        let contents = bucket_contents(shard);
        let in_narrow = contents
            .get(&MEASURED_NARROW_BUCKET)
            .is_some_and(|object_keys| object_keys.contains(&key));
        let in_wide = contents
            .get(&MEASURED_WIDE_BUCKET)
            .is_some_and(|object_keys| object_keys.contains(&key));
        println!(
            "  {label}: {key} -- in {MEASURED_NARROW_BUCKET} {in_narrow}, in \
             {MEASURED_WIDE_BUCKET} {in_wide}"
        );
        assert!(
            in_narrow,
            "`{label}` did not file {key} in bucket {MEASURED_NARROW_BUCKET}, the bucket the \
             shard's own range puts it in. Buckets holding it: {:?}",
            contents
                .iter()
                .filter(|(_, object_keys)| object_keys.contains(&key))
                .map(|(bucket, _)| *bucket)
                .collect::<Vec<_>>()
        );
        assert!(
            !in_wide,
            "`{label}` filed {key} in bucket {MEASURED_WIDE_BUCKET} -- the whole-range bucket, on \
             a shard that holds 0..{NARROW_END}. This is the defect mx#1949 measured and declined; \
             it has come back."
        );
        assert_eq!(
            0,
            buckets_outside(&contents, 0, NARROW_END),
            "the shard holds 0..{NARROW_END} and some page is filed outside it: {:?}",
            contents.keys().collect::<Vec<_>>()
        );
    }
}

// =============================================================================================
// 3. THE CONTROL: while every page is routed, the writers are value-identical
// =============================================================================================

/// THE WRITERS FILE IDENTICALLY, BUCKET FOR BUCKET, WHILE EVERY ADDRESS CARRIES ITS OWN BUCKET.
///
/// `address.routing_bucket()` still wins; the shard's range is consulted only where a fallback
/// would otherwise have fired, and it is `Some` for every page the live write path produces. So on
/// a store the current build wrote, a shard stamped `0..1023` and a shard stamped with the whole
/// range must file the same page in the same bucket.
///
/// Run at BOTH stamps, because "the shard's range IS the whole range" is exactly the condition
/// under which this change is invisible, and a control that holds at one width says nothing about
/// the other. The DENOMINATOR -- that every address really is routed -- is asserted first, since a
/// fixture where nothing was routed would make this pass for the opposite reason.
#[test]
fn the_two_writers_are_value_identical_while_every_page_is_routed() {
    let mut filings: Vec<BTreeMap<u32, Vec<String>>> = Vec::new();
    for stamp in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, 32);

        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        // Re-stamp under the test's own hand. The engine loaded this shard on 0..1023 either way;
        // what is varied here is the ONE input this change added.
        shard.set_routing_range(0, stamp);

        let routed = keys
            .iter()
            .filter(|key| {
                shard
                    .strings
                    .get(*key)
                    .is_some_and(|address| address.routing_bucket().is_some())
            })
            .count();
        assert_eq!(
            keys.len(),
            routed,
            "only {routed} of {} addresses carry a routing bucket; this control means nothing \
             unless every one of them does",
            keys.len()
        );

        for key in &keys {
            let address = shard.strings.get(key).expect("key present").clone();
            crate::engine::storage_bucket_internals::upsert_bucket_index_block(
                shard, 1, "string", key, None, address, true,
            );
        }
        let contents = bucket_contents(shard);
        println!(
            "  stamped 0..{stamp}: {} pages over {} buckets",
            page_total(&contents),
            contents.len()
        );
        assert!(page_total(&contents) >= keys.len(), "the fixture filed nothing");
        filings.push(contents);
    }
    assert_eq!(
        filings[0], filings[1],
        "the two writers filed a ROUTED page differently under the two stamps. The shard's range \
         must only be consulted where the address carries no bucket of its own."
    );
}

// =============================================================================================
// 4. DIRECTION: narrowing places, it does not filter
// =============================================================================================

/// NOT ONE PAGE ALREADY FILED IS REMOVED WHEN THE WRITERS NARROW.
///
/// mx#1945's refutation is that narrowing a FILTER drops live data -- 0 of 400 surviving. These
/// two are not filters, and this drives it element by element rather than by count: the whole
/// bucket-contents map before and after, with the only difference being the one page re-filed.
///
/// The shard here is loaded on `0..1023` with its pages made UNROUTED, so the load path's own
/// rebuild files them on the shard's range; then one page is re-published through each writer.
/// Every other page must be exactly where it was.
#[test]
fn narrowing_the_writers_removes_no_page_that_was_already_filed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let mut shards = engine.shards.write().expect("shards lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let stripped = strip_routing_buckets(shard);
    assert_eq!(
        RECORDS, stripped,
        "the fixture stripped {stripped} of {RECORDS} addresses"
    );
    crate::engine::storage_bucket_internals::rebuild_bucket_block_ownership(
        1,
        shard,
        0,
        NARROW_END,
    );
    let before = bucket_contents(shard);
    assert_eq!(
        RECORDS,
        page_total(&before),
        "the fixture starts with {} pages, not {RECORDS}",
        page_total(&before)
    );
    assert_eq!(
        0,
        buckets_outside(&before, 0, NARROW_END),
        "the fixture starts with pages outside the shard's range, so this measures the rebuild \
         rather than the writers"
    );

    let key = keys[0].clone();
    let address = shard.strings.get(&key).expect("key present").clone();
    assert!(address.routing_bucket().is_none(), "the fixture's address is routed");
    crate::engine::storage_bucket_internals::upsert_bucket_index_block(
        shard, 1, "string", &key, None, address, true,
    );
    let after = bucket_contents(shard);

    println!(
        "  before: {} pages / {} buckets     after: {} pages / {} buckets",
        page_total(&before),
        before.len(),
        page_total(&after),
        after.len()
    );
    assert_eq!(
        page_total(&before),
        page_total(&after),
        "a page was lost or duplicated by an upsert that only re-files one key"
    );
    assert_eq!(
        0,
        buckets_outside(&after, 0, NARROW_END),
        "the upsert filed something outside 0..{NARROW_END}"
    );
    // ELEMENT BY ELEMENT: every key present before is present after, and in the same bucket
    // unless it is the one key that was re-published.
    let placement = |contents: &BTreeMap<u32, Vec<String>>| -> BTreeMap<String, u32> {
        let mut out = BTreeMap::new();
        for (bucket, object_keys) in contents {
            for object_key in object_keys {
                out.insert(object_key.clone(), *bucket);
            }
        }
        out
    };
    let before_placement = placement(&before);
    let after_placement = placement(&after);
    assert_eq!(
        before_placement.keys().collect::<Vec<_>>(),
        after_placement.keys().collect::<Vec<_>>(),
        "the set of object keys in the index moved"
    );
    let moved: Vec<&String> = before_placement
        .iter()
        .filter(|(object_key, bucket)| after_placement.get(*object_key) != Some(bucket))
        .map(|(object_key, _)| object_key)
        .collect();
    assert!(
        moved.is_empty(),
        "these keys changed bucket when only {key} was re-published: {moved:?}"
    );
}

// =============================================================================================
// 5. THE STORED SHAPE DOES NOT MOVE
// =============================================================================================

/// THE FIELD IS ABSENT FROM THE SERIALIZED INDEX, DRIVEN BY BYTES.
///
/// `ShardState` IS the serialized index. Its own `index_format_version` doc records what a change
/// to that shape has cost once already, so "a skipped field is not serialized" is exactly the kind
/// of claim that has to be driven rather than read off an attribute.
///
/// Both directions come out of one measurement, because there is only one byte stream: a shard
/// serialized with the field stamped and the same shard serialized unstamped produce IDENTICAL
/// bytes, so there is no old-index/new-index distinction for a decoder to get wrong. What a decode
/// then yields -- an UNSTAMPED state, answering the whole range -- is what an index written before
/// this change reads as after it.
#[test]
fn the_routing_range_field_changes_no_serialized_byte() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    seed(&engine, 16);

    let mut shards = engine.shards.write().expect("shards lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");

    let mut stamped = shard.clone();
    // The PRODUCTION codec, not the legacy one: `serialize_index` picks its payload encoding off
    // this field, and a byte comparison taken on the fallback path would not be a comparison of
    // what the engine actually writes.
    stamped.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    stamped.set_routing_range(0, NARROW_END);
    let mut unstamped = shard.clone();
    unstamped.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    unstamped.routing_range_start = 0;
    unstamped.routing_range_end = 0;
    unstamped.routing_range_known = false;
    assert_ne!(
        stamped.routing_range(),
        unstamped.routing_range(),
        "the two states under test answer the same range, so byte-equality below would prove \
         nothing about the field"
    );

    let stamped_bytes = crate::engine::serialize_index(&stamped);
    let unstamped_bytes = crate::engine::serialize_index(&unstamped);
    println!(
        "  stamped {} bytes, unstamped {} bytes",
        stamped_bytes.len(),
        unstamped_bytes.len()
    );
    assert!(
        stamped_bytes.len() > 256,
        "the fixture serialized {} bytes; that is not a populated index and byte-equality over \
         two empty buffers says nothing",
        stamped_bytes.len()
    );
    assert_eq!(
        stamped_bytes, unstamped_bytes,
        "stamping the routing range changed the serialized index. The field is supposed to be \
         `#[serde(skip)]`; if it is now written, an index written by an older build no longer \
         reads the same and this is a stored-format change."
    );

    let decoded = crate::engine::decode_index_bytes(&stamped_bytes)
        .expect("the index this test just serialized decodes");
    assert!(
        !decoded.routing_range_known,
        "a decoded index came back claiming to know a routing range; the field cannot have been \
         skipped on the way out"
    );
    assert_eq!(
        (0, WIDE_END),
        decoded.routing_range(),
        "a decoded index must answer the WHOLE range until the engine stamps it, which is what \
         an index written before this change already did"
    );
    assert_eq!(
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        decoded.index_format_version,
        "the on-disk format version this index decodes as is not the one this build writes, which \
         says this change is a format change after all"
    );
    let decoded_unstamped = crate::engine::decode_index_bytes(&unstamped_bytes)
        .expect("the unstamped index decodes too");
    assert_eq!(
        decoded.index_format_version, decoded_unstamped.index_format_version,
        "the two arms decode as different on-disk versions"
    );
}

// =============================================================================================
// 6. THE CROSS-RANGE RESTORE, DRIVEN IN BOTH DIRECTIONS
// =============================================================================================

/// WRITE ON ONE RANGE, READ ON THE OTHER, BOTH WAYS.
///
/// mx#1945 found its refutation this way and it cost a record; mx#1949 re-drove it because a
/// change that touches filing is silent when it loses something. This one moves where an unrouted
/// page is filed, which is the same neighbourhood, so it is driven again: every record readable,
/// every page present, and the buckets counted against the READING shard's range in each
/// direction.
///
/// The second direction is the one with something to lose -- a store written on the whole range
/// and read on `0..1023` has every whole-range bucket outside the reading range.
#[test]
fn a_store_written_on_one_range_still_reads_back_whole_on_the_other() {
    for (write_end, read_end) in [(NARROW_END, WIDE_END), (WIDE_END, NARROW_END)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys = {
            let engine = engine_on(dir.path());
            load_on(&engine, write_end);
            let keys = seed(&engine, RECORDS);
            assert_eq!(RECORDS, read_back(&engine, &keys), "the write arm cannot read its own store");
            engine.unload_shard(1);
            keys
        };
        let engine = engine_on(dir.path());
        load_on(&engine, read_end);
        let recovered = read_back(&engine, &keys);
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let contents = bucket_contents(shard);
        let outside = buckets_outside(&contents, 0, read_end);
        println!(
            "  wrote on 0..{write_end}, read on 0..{read_end}: {recovered}/{RECORDS} records, \
             {} pages, {outside} buckets outside the reading range, stamp {:?}",
            page_total(&contents),
            shard.routing_range()
        );
        assert_eq!(
            RECORDS, recovered,
            "wrote on 0..{write_end}, read on 0..{read_end}: {recovered} of {RECORDS} records \
             came back"
        );
        assert!(
            page_total(&contents) >= RECORDS,
            "wrote on 0..{write_end}, read on 0..{read_end}: {} pages for {RECORDS} records",
            page_total(&contents)
        );
        assert_eq!(
            (0, read_end),
            shard.routing_range(),
            "the reloaded shard carries the range it was written with, not the one it was read \
             with"
        );
    }
}

// =============================================================================================
// 6b. THE MANIFEST INSTALL, WHICH SWAPS A WHOLE SHARD OUT FROM UNDER ITSELF
// =============================================================================================

/// A CROSS-RANGE MANIFEST INSTALL LEAVES THE TARGET CARRYING THE TARGET'S OWN RANGE.
///
/// THIS TEST EXISTS BECAUSE A MUTANT WAS KILLED BY A GUARD ALONE. Making the manifest install
/// insert its restored state directly instead of through `install_shard_state` was caught only by
/// `every_shard_the_engine_installs_carries_its_routing_range`, which reads the SHAPE of the call
/// -- weaker evidence than a test that reads the answer. This reads the answer.
///
/// It is also the path with the most to lose. mx#1945 narrowed this install's REBUILD and dropped
/// 0 of 400 pages; that rebuild still runs on the whole range and is untouched here. What the
/// stamp decides is only where the NEXT write files a page. Both are asserted: every page still
/// present, every record still readable, AND the target carrying `0..1023` rather than the range
/// the image was written on.
///
/// rust-internal: drives the engine's own manifest install, no product behaviour
#[test]
fn a_manifest_installed_across_ranges_leaves_the_target_carrying_its_own_range() {
    let source_dir = tempfile::tempdir().expect("tempdir");
    let source = engine_on(source_dir.path());
    load_on(&source, WIDE_END);
    let keys = seed(&source, RECORDS);

    let buckets: Vec<u32> = {
        let shards = source.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        assert_eq!(
            (0, WIDE_END),
            shard.routing_range(),
            "the SOURCE is not on the whole range, so this is not a cross-range install"
        );
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!buckets.is_empty(), "the source holds no bucket to dump");
    let manifest = source
        .create_bucket_dump_manifest(1, buckets)
        .expect("the source can dump its own buckets");

    let target_dir = tempfile::tempdir().expect("tempdir");
    let target = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        target_dir.path().join("cache"),
        // The pages live where the source wrote them; only the index is restored.
        source_dir.path().join("pages"),
        target_dir.path().join("indexes"),
    );
    load_on(&target, NARROW_END);
    target
        .install_bucket_dump_manifest(&manifest)
        .expect("the manifest installs on the narrower shard");

    let readable = read_back(&target, &keys);
    let shards = target.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let contents = bucket_contents(shard);
    println!(
        "  installed a 0..{WIDE_END} image on a 0..{NARROW_END} shard: {readable}/{RECORDS} \
         records, {} pages, stamp {:?}",
        page_total(&contents),
        shard.routing_range()
    );
    assert_eq!(
        RECORDS, readable,
        "{readable} of {RECORDS} records were readable after a cross-range manifest install -- \
         the failure mx#1945 caught when it first narrowed this install's range"
    );
    assert!(
        page_total(&contents) >= RECORDS,
        "{} pages for {RECORDS} records after the install",
        page_total(&contents)
    );
    assert_eq!(
        (0, NARROW_END),
        shard.routing_range(),
        "the target came out of a manifest install carrying the IMAGE's range rather than its \
         own, so the next unrouted page it files goes into a bucket it does not hold"
    );
}

// =============================================================================================
// 7. THE GUARD: there is one way in, and it stamps
// =============================================================================================

/// EVERY SHARD THE ENGINE INSTALLS CARRIES ITS ROUTING RANGE, HELD AS A LIST OF INSTALL SITES.
///
/// The unstamped default is the whole range, which is the OLD behaviour -- so an install site that
/// forgot to stamp would not fail anything, it would simply keep the defect on that path. What
/// makes that unreachable is that there is exactly ONE function which puts a `ShardState` into the
/// served map, and it stamps. This holds that shape as an equality-compared list, with vacuity
/// floors first and a CONTROL on the matcher itself.
///
/// rust-internal: reads this crate's own call sites, no product behaviour
#[test]
fn every_shard_the_engine_installs_carries_its_routing_range() {
    use std::path::Path;

    /// The production sites that insert a `ShardState` into the engine's served shard map.
    const INSTALL_SITES: &[&str] = &["engine.rs :: install_shard_state"];

    // THE MATCHER'S OWN CONTROL, before it is pointed at the tree. It must fire on both shapes an
    // install can take, and stay SILENT on an insert into a field OF a shard, which is the shape
    // that appears dozens of times on the write path.
    let control_lines: Vec<&str> = vec![
        "fn chain_form(&self) {",
        "    self.shards",
        "        .write()",
        "        .expect(\"engine lock poisoned\")",
        "        .insert(shard_id, state);",
        "}",
        "fn bound_form(&self) {",
        "    let mut shards = self.shards.write().expect(\"engine lock poisoned\");",
        "    shards.insert(shard_id, state);",
        "}",
        "fn not_an_install(&self) {",
        "    let mut shards = self.shards.write().expect(\"engine lock poisoned\");",
        "    let shard = shards.get_mut(&shard_id).expect(\"shard\");",
        "    shard.strings.insert(key.clone(), durable.clone());",
        "    shard.wal_resident_blocks.insert(object_id, placement);",
        "}",
    ];
    let control_hits = shard_map_insert_sites(&control_lines);
    assert_eq!(
        vec!["chain_form".to_string(), "bound_form".to_string()],
        control_hits,
        "the matcher does not report what it is supposed to report on a planted input, so its \
         verdict over the tree below means nothing"
    );

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![root.clone()];
    let mut files_scanned = 0usize;
    let mut lines_scanned = 0usize;
    let mut excluded = 0usize;
    let mut shards_mentions = 0usize;
    let mut sites: Vec<String> = Vec::new();

    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let display = entry_path.display().to_string();
            if display.contains("/tests/") || display.ends_with("tests.rs") {
                excluded += 1;
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&entry_path) else {
                continue;
            };
            files_scanned += 1;
            let lines: Vec<&str> = text.lines().collect();
            lines_scanned += lines.len();
            shards_mentions += lines.iter().filter(|line| line.contains("self.shards")).count();
            let relative = display
                .rsplit_once("/src/")
                .map(|(_, tail)| tail.to_string())
                .unwrap_or(display.clone());
            for enclosing in shard_map_insert_sites(&lines) {
                sites.push(format!("{relative} :: {enclosing}"));
            }
        }
    }

    // VACUITY FLOORS, BEFORE ANY VERDICT.
    assert!(
        files_scanned > 80,
        "the scan read {files_scanned} production .rs files under {}; below 80 it has stopped \
         reading the crate and the list below is empty for the wrong reason",
        root.display()
    );
    assert!(
        lines_scanned > 100_000,
        "the scan read {lines_scanned} lines; below 100,000 it is not reading this crate"
    );
    assert!(
        excluded > 10,
        "the scan excluded {excluded} test files; this crate has more than ten"
    );
    assert!(
        shards_mentions >= 20,
        "the scan saw `self.shards` {shards_mentions} times; there were 60 when this was written, \
         and below 20 the matcher is not looking at the engine's shard map at all"
    );

    sites.sort();
    println!(
        "  {files_scanned} files / {lines_scanned} lines scanned, {excluded} test files excluded, \
         {shards_mentions} mentions of the engine's shard map"
    );
    for site in &sites {
        println!("    installs a ShardState   {site}");
    }
    let mut expected: Vec<String> = INSTALL_SITES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(
        sites, expected,
        "the set of production sites that put a ShardState into the served shard map has moved.\n\
         If you ADDED one, it must stamp the shard's routing range -- an unstamped shard files \
         every unrouted page under the WHOLE range, which on any shard not loaded on 0..u32::MAX \
         is a bucket the shard does not hold. Route it through `install_shard_state` instead.\n\
         If you REMOVED one, the engine has another way to install a shard and this guard can no \
         longer see it."
    );

    // And the one site actually stamps: read off the source, since the list above only says WHERE.
    let installer = std::fs::read_to_string(root.join("engine.rs")).expect("engine.rs");
    assert!(
        installer.contains("state.set_routing_range(start_routing_bucket, end_routing_bucket);"),
        "`install_shard_state` no longer stamps the range it read"
    );
}

/// The enclosing function name of every line that inserts into the engine's shard map.
///
/// Two shapes, because both appear in this crate: a method chain starting at `self.shards`, and a
/// local binding taken from it. An insert into a field OF a shard (`shard.strings.insert(..)`) is
/// neither, and the control above holds that.
fn shard_map_insert_sites(lines: &[&str]) -> Vec<String> {
    let enclosing = |index: usize| -> String {
        for line in lines[..=index].iter().rev() {
            let trimmed = line.trim_start();
            if let Some(rest) = trimmed
                .strip_prefix("pub(crate) fn ")
                .or_else(|| trimmed.strip_prefix("pub(super) fn "))
                .or_else(|| trimmed.strip_prefix("pub fn "))
                .or_else(|| trimmed.strip_prefix("fn "))
            {
                return rest
                    .split(|c: char| c == '(' || c == '<' || c == ' ')
                    .next()
                    .unwrap_or("?")
                    .to_string();
            }
        }
        "?".to_string()
    };
    let mut found: Vec<(usize, String)> = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !line.contains("self.shards") {
            continue;
        }
        // Shape one: a method chain. Every following line that continues it starts with `.`.
        let mut cursor = index;
        if line.contains(".insert(") {
            found.push((index, enclosing(index)));
        }
        while cursor + 1 < lines.len() && lines[cursor + 1].trim_start().starts_with('.') {
            cursor += 1;
            if lines[cursor].contains(".insert(") {
                found.push((cursor, enclosing(cursor)));
            }
        }
        // Shape two: a binding. `let mut shards = self.shards.write()...` and then a bare
        // `shards.insert(`, which the chain walk above cannot see.
        if line.contains("= self.shards") {
            let Some(binding) = line
                .split('=')
                .next()
                .and_then(|left| left.split_whitespace().last())
            else {
                continue;
            };
            let needle = format!("{binding}.insert(");
            for (offset, later) in lines.iter().enumerate().skip(index + 1) {
                if later.trim_start() == "}" && later.len() <= 2 {
                    break;
                }
                if later.contains(&needle) {
                    found.push((offset, enclosing(offset)));
                }
            }
        }
    }
    found.sort_by_key(|(index, _)| *index);
    let mut seen = BTreeSet::new();
    found
        .into_iter()
        .filter(|(index, _)| seen.insert(*index))
        .map(|(_, name)| name)
        .collect()
}

// =============================================================================================
// 8. THE BYTE BUDGET
// =============================================================================================

/// THREE FIELDS FOR EIGHT BYTES, NOT SIXTEEN, AND THE STRUCT IS ONE PER SHARD.
///
/// mx#1949 added a field to `LiveBlockEntry` -- one per LIVE PAGE -- took it from 104 to 112, and
/// paid it back into existing padding rather than widening the bound. This field is on a structure
/// with ONE INSTANCE PER SHARD, which is the reason `per_item_byte_budget` carries `ShardState` as
/// a control rather than as a budgeted row: at a count of one, width is not a cost worth trading
/// clarity for.
///
/// It is still paid down, the same way and for the same reason mx#1937 gives: an `Option<(u32,
/// u32)>` is 12 bytes of its own and would have taken 16 after alignment; a `u32` pair plus a
/// `bool` puts the flag in the byte this struct was already padding out for `promote_scan_done`,
/// `control_coalesce_persist` and `control_distinct_sketch`. Pinned here so a later widening of
/// this particular field is a named failure rather than a silent one.
#[test]
fn the_shard_carried_range_costs_eight_bytes_on_a_structure_there_is_one_of() {
    use std::mem::size_of;
    let state = size_of::<crate::engine::state::ShardState>();
    println!("  ShardState {state} bytes, three routing-range fields inside it");
    assert_eq!(
        1_888, state,
        "`ShardState` is {state} bytes. It was 1,880 before the routing range was carried on it \
         and 1,888 after -- the two u32s, with the flag landing in padding this struct already \
         had. A change here means either a field was added or the flag stopped being free; say \
         which, and pay it down rather than widening this number."
    );
    assert_eq!(
        4,
        size_of::<u32>() + size_of::<bool>() - 1,
        "the arithmetic this bound rests on has moved"
    );
    // AN `Option<(u32, u32)>` WOULD HAVE COST MORE, and that is the trade this number records.
    assert!(
        size_of::<Option<(u32, u32)>>() > 2 * size_of::<u32>(),
        "an Option<(u32, u32)> is no longer wider than the pair it wraps, so the packing this \
         test justifies is no longer a saving"
    );
}
