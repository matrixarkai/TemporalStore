// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE A BLOCK IS FILED, AFTER THE ARGUMENT IS RECONCILED.
//!
//! mx#1942 established by construction that where a block is FILED is decided by an argument, and
//! held the nine production call sites that passed `0, u32::MAX` as a LIST -- deliberately
//! shipping no engine change. This file is the engine change, and what had to be true before it
//! could be made.
//!
//! # THE ASYMMETRY THAT SPLIT THE NINE, AND WHY IT IS GONE
//!
//! The three rebuilds used to react to the range differently, and the difference decided whether
//! passing the shard's range was safe:
//!
//! * `rebuild_bucket_first_index` used the range ONLY as the fallback placement for an address that
//!   carried no routing bucket of its own. Narrowing it moved unrouted blocks; it could drop nothing.
//! * `rebuild_bucket_block_ownership` did the same AND THEN FILTERED -- `if routing_bucket < start
//!   || routing_bucket > end { continue; }` -- so a block whose address carried an EXPLICIT bucket
//!   outside the range was dropped from the index entirely.
//! * `promote_model_maps_to_bucket_index_authority` delegates to ownership, so it inherited the
//!   filter.
//!
//! A `BlockAddress` NO LONGER CARRIES A ROUTING BUCKET. A block's bucket is
//! `start + FNV-1a-64(object_key) % (end - start + 1)` over the range the shard is stamped with, so
//! every rebuild derives it and every derived bucket is inside the range it was derived on
//! (`engine::hashing::derived_bucket_range` drives that, with a control). Both filters are therefore
//! gone, and the hazard they created is gone with them: there is no state in which a rebuild can
//! drop a block for being out of range, because there is no bucket that came from anywhere else.
//!
//! So the distinction this file was built on -- a ROUTED block, placed by its own address, against an
//! UNROUTED one, placed by the argument -- has collapsed into one case. Every block is placed by the
//! argument, and the tests below measure that rather than the difference between two populations
//! that no longer exist. `neither_rebuild_can_drop_a_page_because_every_bucket_is_derived_in_range`
//! is what used to be the asymmetry measurement, inverted.
//!
//! # MIGRATION, DRIVEN RATHER THAN ARGUED
//!
//! Blocks already filed under the whole range exist in any store written before this.
//! `a_store_filed_under_the_whole_range_reads_back_whole_after_the_change` writes such a store,
//! restarts on it, and asserts what comes back: every record readable, every block present, and --
//! the load-bearing observation -- every block filed INSIDE the shard's own range, because the
//! rebuild the load runs derives the placement rather than reading it off an address. No migration
//! step is needed, and that is a measurement here, not a hope.

use super::*;
use crate::engine::hashing::block_routing_bucket;
use crate::engine::storage_bucket_internals::{
    collect_live_block_entries, rebuild_bucket_block_ownership, rebuild_bucket_first_index,
    refresh_bucket_runtime_flags,
};
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;

/// The end bucket `load_shard` uses, and the one the nine call sites passed.
const WIDE_END: u32 = u32::MAX;

const RECORDS: usize = 400;

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
        table_name: "filing-range".to_string(),
        shard_uri: "local://filing-range/1".to_string(),
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
        let key = format!("filing-{index:06}");
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

/// An address with the `rs` key taken off the wire, produced by the ENGINE'S OWN DECODER rather
/// than by a setter -- mx#1942's door, kept because what it now demonstrates is worth more than what
/// it used to build.
///
/// IT USED TO PRODUCE AN UNROUTED ADDRESS. There is no such thing now: the address has nowhere to
/// put a bucket whether the key is there or not. So this is the IDENTITY, and
/// `an_older_index_decodes_to_exactly_the_same_address` asserts that it is -- which is the statement
/// that an index carrying a real `rs` loads to the same address as one carrying none, and therefore
/// that no migration is needed for the field's departure.
///
/// AND THE SLOT ITSELF IS GONE NOW, not written empty, which is what inverted this helper. While
/// `BlockAddressWire` still declared `rs`, an older index was the shape WITHOUT the key and this
/// removed it. The field is retired, so a current address has no `rs` and an older index is the
/// shape WITH it -- the key is added here instead, and the identity it demonstrates is the same one.
fn as_an_older_build_wrote_it(address: &BlockAddress) -> BlockAddress {
    let mut wire = serde_json::to_value(address).expect("an address serializes to its wire shape");
    let object = wire
        .as_object_mut()
        .expect("the address wire shape is a JSON object");
    // THE DIRECTION INVERTED WHEN THE SLOT LEFT THE STRUCT. This helper used to REMOVE `rs` from
    // what a current build writes, because a current build still wrote one. `BlockAddressWire` has
    // retired the field, so a current address carries no `rs` at all and removing it would be the
    // no-op the old assertion was written to catch. What an older build wrote is now the shape with
    // the key ADDED, and what this proves is that the retired key is still inert on the way in.
    assert!(
        object.remove("rs").is_none(),
        "a current address still carries an `rs` key, so the slot was not retired after all and \
         this helper is adding a key that is already there -- every count taken through it would \
         then be measuring the current shape, not an older one."
    );
    object.insert("rs".to_string(), serde_json::json!(513u32));
    serde_json::from_value(wire)
        .expect("the engine's decoder still accepts an index that carries the retired `rs` key")
}

/// Round-trip every string block through the wire with `rs` removed, asserting the identity per block.
///
/// It used to STRIP the routing bucket, which was the state the range-argument tests needed. There is
/// nothing to strip, so what is left is the per-block assertion that the key is inert -- and a fixture
/// that still covers every block, which is what the callers assert on.
fn round_trip_every_page_through_the_wire(
    shard: &mut crate::engine::state::ShardState,
) -> usize {
    let keys: Vec<String> = shard.strings.keys().map(|key| key.to_string()).collect();
    for key in &keys {
        let before = shard.strings.get(key.as_str()).expect("key present").clone();
        let older = as_an_older_build_wrote_it(&before);
        assert_eq!(
            older, before,
            "page {key} changed when its `rs` key was removed from the wire; the key is inert and \
             this round trip has to be the identity"
        );
        shard.strings.insert(key.as_str().into(), older);
    }
    keys.len()
}

/// THE `rs` KEY IS INERT, IN BOTH DIRECTIONS.
///
/// An index written before the routing bucket left carries a real bucket in `rs`; one written now
/// carries nil; one written before the field existed carries nothing at all. All three decode to the
/// SAME address, which is why the field's departure needs no migration step -- and it is asserted
/// here rather than argued, because a silently-ignored wire field is exactly the shape that lost this
/// tree an object once before.
///
/// rust-internal: reads the address wire shape, no product behaviour
#[test]
fn an_older_index_decodes_to_exactly_the_same_address() {
    let address = BlockAddress::from_parts(3, 4096, 128, Some(1), Some(2));

    // (1) THE KEY REMOVED ENTIRELY -- the oldest spelling.
    assert_eq!(
        as_an_older_build_wrote_it(&address),
        address,
        "an index carrying the retired `rs` key decoded to a different address than one without it"
    );

    // (2) A REAL BUCKET IN THE KEY -- what every index written before this change holds. The value
    // is one the address could not have produced, so a decoder that used it would be visible.
    let mut wire = serde_json::to_value(&address).expect("serializes");
    wire.as_object_mut()
        .expect("object")
        .insert("rs".to_string(), serde_json::json!(545_210_715_u32));
    let with_a_bucket: BlockAddress = serde_json::from_value(wire).expect("decodes");
    assert_eq!(
        with_a_bucket, address,
        "an index carrying a real routing bucket in `rs` must decode to the same address as one \
         carrying none: the bucket is the container's answer now, and an address that absorbed the \
         stored value would be carrying a second opinion about it"
    );

    // (3) AND THE SLOT IS NO LONGER WRITTEN AT ALL -- not as nil, not at all.
    //
    // This arm inverted. It used to require `Some(Null)`, because the slot was held open so the
    // index log's positional array would not shorten. The slot is retired now, and what makes
    // that safe is not that the array stayed the same length but that a shortened one is REFUSED
    // by length rather than reinterpreted -- `block_store`'s
    // `shorter_struct_against_an_existing_row` measures it, and
    // `a_positionally_packed_split_address_does_not_decode_as_a_merged_one` pins both counts.
    //
    // Asserted through `get` and not through indexing: `wire["rs"]` answers `Value::Null` for an
    // absent key exactly as it does for one written null, so the indexing form of this assertion
    // would keep passing after the slot left, for the opposite reason.
    assert_eq!(
        serde_json::to_value(&address)
            .expect("serializes")
            .get("rs")
            .cloned(),
        None,
        "the `rs` slot is retired: a current address must not write it, not even as nil"
    );
}

/// Every bucket that holds at least one block, with the object keys it holds, sorted. The element
/// by element picture a count cannot give.
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
    contents.values().map(|keys| keys.len()).sum()
}

// =============================================================================================
// 1. The asymmetry: which rebuild can DROP a block, and which cannot
// =============================================================================================

/// NEITHER REBUILD CAN DROP A BLOCK, AND THAT IS WHAT REMOVING THE DROP FILTERS REST ON.
///
/// This is the inversion of what stood here. `rebuild_bucket_block_ownership` used to FILTER on the
/// range -- `if routing_bucket < start || routing_bucket > end { continue; }` -- and a block whose
/// address carried an EXPLICIT bucket outside the range was dropped from the index entirely. That
/// was the losing direction of mx#1974 and the reason its argument change needed bounding.
///
/// An address carries no bucket. Both rebuilds DERIVE it as
/// `block_routing_bucket(object_key, start, end)`, which is inside `start..=end` by construction, so
/// a state in which either rebuild drops a block cannot be built -- and the fixture that used to
/// build it (`set_routing_bucket(Some(900_000))`) cannot be written.
///
/// So what is measured is the claim that replaced it: both rebuilds keep every block, at BOTH ranges,
/// and both file it where the shard's own range puts it. The element-by-element comparison against a
/// witness built outside the index is the control -- two rebuilds that both kept 400 blocks and put
/// them in different buckets would read as agreement on a count.
///
/// rust-internal: reads the engine's own rebuilds, no product behaviour
#[test]
fn neither_rebuild_can_drop_a_page_because_every_bucket_is_derived_in_range() {
    let mut observed: BTreeMap<(u32, &str), BTreeMap<u32, Vec<String>>> = BTreeMap::new();
    for rebuild_end in [NARROW_END, WIDE_END] {
        for (rebuild, is_ownership) in [("ownership", true), ("first_index", false)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, NARROW_END);
            let keys = seed(&engine, RECORDS);

            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");

            // DENOMINATOR: the fixture really wrote what the rebuild is about to re-file, read off
            // the MODEL MAP rather than the bucket index -- both rebuilds repopulate the index FROM
            // the model maps, so the model map is the input and the index is the output.
            assert_eq!(
                shard.strings.len(),
                keys.len(),
                "{rebuild}/0..{rebuild_end}: the model map holds {} of {} written keys, so the \
                 rebuild below is not being handed the fixture",
                shard.strings.len(),
                keys.len()
            );

            if is_ownership {
                rebuild_bucket_block_ownership(1, shard, 0, rebuild_end);
            } else {
                rebuild_bucket_first_index(1, shard, 0, rebuild_end);
            }
            refresh_bucket_runtime_flags(shard);
            observed.insert((rebuild_end, rebuild), bucket_contents(shard));
        }
    }

    for ((rebuild_end, rebuild), contents) in &observed {
        println!(
            "  0..{rebuild_end} {rebuild}: {} pages in {} buckets",
            page_total(contents),
            contents.len()
        );
    }

    for rebuild_end in [NARROW_END, WIDE_END] {
        // THE WITNESS, built outside the index from the fixture's own key list.
        let mut expected: BTreeMap<u32, Vec<String>> = BTreeMap::new();
        for index in 0..RECORDS {
            let key = format!("filing-{index:06}");
            expected
                .entry(block_routing_bucket(&key, 0, rebuild_end))
                .or_default()
                .push(key);
        }
        for bucket_keys in expected.values_mut() {
            bucket_keys.sort();
        }
        for rebuild in ["ownership", "first_index"] {
            let contents = observed
                .get(&(rebuild_end, rebuild))
                .expect("both arms ran");
            assert_eq!(
                page_total(contents),
                RECORDS,
                "0..{rebuild_end} {rebuild}: {} of {RECORDS} pages survived. Neither rebuild has a \
                 drop filter any more, and a derived bucket cannot be out of range, so nothing can \
                 legitimately be lost here.",
                page_total(contents)
            );
            assert_eq!(
                contents, &expected,
                "0..{rebuild_end} {rebuild}: the buckets the rebuild filed pages into are not the \
                 buckets the range puts those keys in"
            );
        }
        // And the two rebuilds agree with EACH OTHER, which is the statement the asymmetry used to
        // deny. Asserted separately from the witness: two arms that both disagreed with the witness
        // in the same way would still be an asymmetry this file has to report.
        assert_eq!(
            observed.get(&(rebuild_end, "ownership")),
            observed.get(&(rebuild_end, "first_index")),
            "0..{rebuild_end}: the two rebuilds no longer differ in what they keep or where they \
             put it, and this is the assertion that says so"
        );
    }

    // AND THE TWO RANGES REALLY DID PUT THE BLOCKS SOMEWHERE DIFFERENT. Without this the four
    // comparisons above could all hold on a fixture where the range decided nothing, and the test
    // would be asserting that two identical things are identical.
    assert_ne!(
        observed.get(&(NARROW_END, "ownership")),
        observed.get(&(WIDE_END, "ownership")),
        "the narrow and wide rebuilds filed the pages into the SAME buckets, so this test is not \
         measuring a placement the range decides"
    );
}

/// EVERY BLOCK THE LIVE WRITE PATH FILES LANDS INSIDE THE SHARD'S OWN RANGE, WITH A DENOMINATOR.
///
/// This used to be two halves, because a block could be placed in two ways: by an explicit bucket
/// stamped onto its address, or by the range as a fallback. One case remains -- the range -- so what
/// is asserted is the FILING, read off the keys of the bucket map, which is where a block now is.
///
/// Both widths, separately. On the wide shard every key has a bucket of its own by construction and
/// the assertion cannot fail; it is carried anyway as the control that says the narrow arm's
/// agreement is the change and not the fixture.
///
/// rust-internal: reads the engine's own write path, no product behaviour
#[test]
fn every_page_the_live_write_path_files_lands_inside_the_shards_range() {
    for end_routing_bucket in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed(&engine, RECORDS);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");

        let contents = bucket_contents(shard);
        assert_eq!(
            page_total(&contents),
            keys.len(),
            "0..{end_routing_bucket}: the fixture wrote {} keys and the index holds {} pages",
            keys.len(),
            page_total(&contents)
        );

        let outside: Vec<u32> = contents
            .keys()
            .copied()
            .filter(|routing_bucket| *routing_bucket > end_routing_bucket)
            .collect();
        println!(
            "  0..{end_routing_bucket}: {} live pages in {} buckets, {} buckets outside the range",
            page_total(&contents),
            contents.len(),
            outside.len()
        );
        assert!(
            outside.is_empty(),
            "0..{end_routing_bucket}: {} buckets hold pages but sit above the shard's end; the \
             first few are {:?}. `append_value` is handed \
             `block_routing_bucket(key, start, end)` and every filing site derives the same \
             expression, so a page there is invisible to every reader that scopes by bucket.",
            outside.len(),
            &outside[..outside.len().min(5)]
        );

        // AND THE FILING IS THE DERIVATION, key by key. "Inside the range" is satisfied by filing
        // everything into bucket 0, so the agreement with the witness is what makes the assertion
        // above worth having.
        let mut expected: BTreeMap<u32, Vec<String>> = BTreeMap::new();
        for key in &keys {
            expected
                .entry(block_routing_bucket(key, 0, end_routing_bucket))
                .or_default()
                .push(key.clone());
        }
        for bucket_keys in expected.values_mut() {
            bucket_keys.sort();
        }
        assert_eq!(
            contents, expected,
            "0..{end_routing_bucket}: the write path filed pages into buckets the shard's own \
             range does not put those keys in"
        );
    }
}

// =============================================================================================
// 2. The direction: a block is filed where the shard's own range puts it
// =============================================================================================

/// THE PRODUCTION FLUSH NOW FILES EVERY BLOCK INSIDE THE SHARD'S OWN RANGE, ELEMENT BY ELEMENT.
///
/// Driven through `flush_shard_index`, which is `persistence.rs`'s own entry point and carries two
/// of the nine call sites, rather than by calling the rebuild directly -- so what is measured is
/// the path, not the helper.
///
/// THE ASSERTION IS SET EQUALITY AGAINST A WITNESS COMPUTED OUTSIDE THE INDEX: the fixture's own
/// key list, hashed with the shard's own range. A count would pass for two sets wrong by the same
/// amount, and the failure that matters here is one block under a bucket nothing summarises.
///
/// Both widths, separately. On the wide shard the two placements are the same expression, so that
/// arm cannot fail -- it is carried anyway as the control that says the narrow arm's agreement is
/// the change and not the fixture.
///
/// rust-internal: reads the engine's own flush path, no product behaviour
#[test]
fn the_production_flush_files_every_page_where_the_shards_own_range_puts_it() {
    for end_routing_bucket in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed(&engine, RECORDS);

        // NO FIXTURE SURGERY. This used to strip the routing bucket off every address first,
        // because the argument only decided a placement for an address that carried none. The
        // argument decides EVERY placement now, so what runs below is the production state and the
        // production path -- which makes this test stronger than it was, not weaker.

        // THE PRODUCTION PATH.
        engine.flush_shard_index(1);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let contents = bucket_contents(shard);

        // DENOMINATOR: nothing was lost by the flush.
        assert_eq!(
            page_total(&contents),
            keys.len(),
            "0..{end_routing_bucket}: {} pages went in and {} came out of the flush",
            keys.len(),
            page_total(&contents)
        );

        // THE WITNESS, built outside the index from the fixture's own key list.
        let mut expected: BTreeMap<u32, Vec<String>> = BTreeMap::new();
        for key in &keys {
            expected
                .entry(block_routing_bucket(key, 0, end_routing_bucket))
                .or_default()
                .push(key.clone());
        }
        for bucket_keys in expected.values_mut() {
            bucket_keys.sort();
        }

        // And the witness must not be degenerate on the narrow arm: 400 keys over 1,024 buckets
        // collide, so a witness with one key per bucket would mean the hash is not reducing.
        if end_routing_bucket == NARROW_END {
            assert!(
                expected.len() < keys.len(),
                "the narrow witness put {} keys into {} buckets with no collision at all, which \
                 is not what a hash modulo 1,024 does to {} keys -- the witness is not being \
                 computed on the narrow range",
                keys.len(),
                expected.len(),
                keys.len()
            );
        }

        println!(
            "  0..{end_routing_bucket}: {} pages in {} buckets, witness names {} buckets",
            page_total(&contents),
            contents.len(),
            expected.len()
        );

        // ELEMENT BY ELEMENT.
        assert_eq!(
            contents, expected,
            "0..{end_routing_bucket}: the buckets the flush filed pages into are not the buckets \
             the shard's own range puts those keys in. Every rebuild takes the range as an \
             argument and it decides the placement, so a disagreement here is a call site still \
             passing `0, u32::MAX`."
        );

        // And no bucket outside the shard's own range, stated separately because a run that
        // disagreed with the witness but stayed inside the range is a different defect from one
        // that filed outside it.
        let outside: Vec<u32> = contents
            .keys()
            .copied()
            .filter(|routing_bucket| *routing_bucket > end_routing_bucket)
            .collect();
        assert!(
            outside.is_empty(),
            "0..{end_routing_bucket}: {} buckets hold pages but sit above the shard's end; the \
             first few are {:?}. A page there is invisible to every reader that scopes by bucket.",
            outside.len(),
            &outside[..outside.len().min(5)]
        );
    }
}

/// NO BLOCK CHANGES BUCKET EXCEPT THE ONES THIS CHANGE INTENDS TO MOVE.
///
/// The strong form mx#1942's measurement did not need and this change does: a full listing of what
/// each bucket holds, compared element by element against a control, so a block that quietly moved
/// somewhere neither arm intended is a failure rather than a matching count.
///
/// THE CONTROL IS THE SAME RANGE TWICE. There is no "routed" population left to compare against:
/// every block is placed by the argument. So the control that says the comparison can express
/// sameness is a rebuild at the SAME range run twice -- which must be identical -- and the subject is
/// two DIFFERENT ranges, which must differ on every block. Two directions, because an arm that moved
/// nothing and an arm that moved everything both read as "a number changed".
///
/// rust-internal: reads the engine's own rebuild, no product behaviour
#[test]
fn every_page_changes_bucket_with_the_range_because_the_range_decides_every_placement() {
    /// Build the fixture, rebuild on `rebuild_end`, and return what each bucket holds.
    fn contents_after(rebuild_end: u32) -> BTreeMap<u32, Vec<String>> {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, RECORDS);
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        rebuild_bucket_first_index(1, shard, 0, rebuild_end);
        refresh_bucket_runtime_flags(shard);
        let contents = bucket_contents(shard);
        assert_eq!(
            page_total(&contents),
            keys.len(),
            "end={rebuild_end}: {} pages went in and {} came out",
            keys.len(),
            page_total(&contents)
        );
        contents
    }

    /// Every (key -> bucket) pair, so two arms can be compared block by block rather than bucket by
    /// bucket.
    fn placement(contents: &BTreeMap<u32, Vec<String>>) -> BTreeMap<String, u32> {
        let mut placed = BTreeMap::new();
        for (routing_bucket, keys) in contents {
            for key in keys {
                placed.insert(key.clone(), *routing_bucket);
            }
        }
        placed
    }

    // THE CONTROL: the SAME range twice, over two independently built fixtures. It must come out
    // bucket-for-bucket identical, or the comparison below cannot express sameness and "everything
    // moved" would be the only answer it could ever give.
    let same_range_once = contents_after(NARROW_END);
    let same_range_again = contents_after(NARROW_END);
    assert_eq!(
        same_range_once, same_range_again,
        "two rebuilds at the SAME range filed the pages differently, so this comparison cannot \
         tell a placement the range decided from noise in the fixture"
    );

    // THE SUBJECT: two different ranges, which decide every placement.
    let wide = contents_after(WIDE_END);
    let narrow = contents_after(NARROW_END);

    let wide_placement = placement(&wide);
    let narrow_placement = placement(&narrow);
    assert_eq!(
        wide_placement.keys().collect::<BTreeSet<_>>(),
        narrow_placement.keys().collect::<BTreeSet<_>>(),
        "the two arms hold different KEYS, so comparing where they filed them compares two \
         different stores"
    );

    let moved: Vec<&String> = wide_placement
        .iter()
        .filter(|(key, routing_bucket)| {
            narrow_placement.get(*key) != Some(routing_bucket)
        })
        .map(|(key, _)| key)
        .collect();
    let outside_after: Vec<u32> = narrow
        .keys()
        .copied()
        .filter(|routing_bucket| *routing_bucket > NARROW_END)
        .collect();

    println!(
        "  control (same range twice): {} buckets, identical",
        same_range_once.len()
    );
    println!(
        "  subject: wide {} buckets -> narrow {} buckets, {} of {RECORDS} pages changed \
         bucket, {} buckets outside the shard's range after",
        wide.len(),
        narrow.len(),
        moved.len(),
        outside_after.len()
    );

    // EVERY block moves, and that is the intended set: the two placement functions differ on every
    // key whose hash exceeds the narrow bucket count, which is every key at a 64-bit hash.
    assert_eq!(
        moved.len(),
        RECORDS,
        "{} of {RECORDS} pages changed bucket between the two ranges. Every page is placed by the \
         argument now, so all of them must move; a smaller number means some page was placed by \
         something other than the argument.",
        moved.len()
    );
    // And they all land inside the shard, which is the point of moving them.
    assert!(
        outside_after.is_empty(),
        "after the narrow rebuild {} buckets still sit above the shard's end: {:?}",
        outside_after.len(),
        &outside_after[..outside_after.len().min(5)]
    );
}

/// THE OWNERSHIP REPORT NAMES A BLOCK FILED WHERE ITS KEY DOES NOT ROUTE.
///
/// `validate_bucket_ownership_index_from_entries` compares each live block's bucket against
/// `block_routing_bucket(object_key, start, end)`. It used to read the block's bucket off the ADDRESS,
/// which the filing site had just written with that same expression -- so the comparison was between
/// a value and a copy of itself, and only an address carrying NONE could make it fire. It now reads
/// the bucket the INDEX has the block under, which is an independent answer.
///
/// SO THE CHECK CAN FAIL, AND THIS IS IT FAILING. Without a guard the change would have replaced one
/// unfireable comparison with another and nothing would have said so.
///
/// WITH THE HONEST INDEX AS ITS DENOMINATOR: the same fixture before the move must report no
/// mismatch, or a report that flagged everything would pass the half below.
///
/// rust-internal: reads the engine's own validation, no product behaviour
#[test]
fn the_ownership_report_names_a_page_filed_where_its_key_does_not_route() {
    use crate::engine::storage_bucket_internals::validate_bucket_ownership_index_from_entries;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, 32);

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1");

    // DENOMINATOR: the honest index reports nothing.
    let entries = collect_live_block_entries(shard);
    assert_eq!(entries.len(), keys.len(), "the fixture must hold one page per key");
    let honest = validate_bucket_ownership_index_from_entries(1, shard, &entries, 0, NARROW_END);
    assert!(
        honest.mismatches.is_empty(),
        "the untouched index already reports {} mismatch(es), so the report below cannot be \
         attributed to the move: {:?}",
        honest.mismatches.len(),
        honest.mismatches
    );

    // THE MOVE: take one bucket's node and file it under a bucket its key does not route to.
    let moved_from = *shard
        .bucket_index
        .bucket_map
        .iter()
        .find(|(_, bucket)| !bucket.block_index.is_empty())
        .map(|(routing_bucket, _)| routing_bucket)
        .expect("some bucket holds a page");
    let node = shard.bucket_index.bucket_map.remove(&moved_from).expect("the node");
    let moved_to = (0..=NARROW_END)
        .find(|candidate| !shard.bucket_index.bucket_map.contains_key(candidate) && *candidate != moved_from)
        .expect("some bucket is free");
    let pages_moved = node.block_index.len();
    shard.bucket_index.bucket_map.insert(
        moved_to,
        crate::engine::state::BucketNode { routing_bucket: moved_to, ..node },
    );
    assert!(pages_moved > 0, "the move carried no page");

    let entries = collect_live_block_entries(shard);
    let after = validate_bucket_ownership_index_from_entries(1, shard, &entries, 0, NARROW_END);
    println!(
        "  moved {pages_moved} page(s) from bucket {moved_from} to {moved_to}: {} mismatch(es)",
        after.mismatches.len()
    );
    assert_eq!(
        pages_moved,
        after.mismatches.len(),
        "the report named {} of {pages_moved} moved page(s). A page filed where its key does not \
         route is what this check exists to find, and it is the only comparison on this path with \
         two independent sides.",
        after.mismatches.len()
    );
    let named = after.mismatches.first().expect("one mismatch");
    assert_eq!(
        Some(moved_to),
        named.actual_routing_bucket,
        "the report must name the bucket the page is FILED in"
    );
    assert_eq!(
        moved_from, named.expected_routing_bucket,
        "and the bucket its KEY routes to, which is where it was"
    );
}

// =============================================================================================
// 3. The refutation: a manifest is a WHOLE-SHARD image and must be installed whole
// =============================================================================================

/// INSTALLING A MANIFEST ON A NARROWER SHARD MUST NOT DROP THE BLOCKS THAT FALL OUTSIDE IT.
///
/// This is the site this change tried to reconcile and had to put back, so the reason is recorded
/// as a measurement rather than as a comment.
///
/// `install_bucket_dump_manifest` rebuilds ownership over `decode_index_bytes(&manifest
/// .index_bytes)` -- a WHOLE-SHARD image, written by whatever routing range the SOURCE shard ran
/// on, whose blocks already carry explicit routing buckets from that range.
/// `rebuild_bucket_block_ownership` used to FILTER on the range as well as place by it, so passing
/// the INSTALLING shard's range deleted every block whose source bucket fell outside it and the record
/// was simply gone. That filter is gone -- a derived bucket cannot be out of range -- so this test
/// now measures the property rather than the workaround: the install RE-DERIVES each block's bucket on
/// the target's range and keeps every block.
///
/// The denominator is asserted before the verdict, and it is the whole point: the fixture is only
/// meaningful if the source's blocks really do route outside the target's range. On a source loaded
/// `0..u32::MAX` and a target loaded `0..1023` they do, for essentially every key -- and that is
/// computed from the KEYS rather than read off an address, because an address no longer has an
/// opinion about it.
///
/// rust-internal: drives the engine's own manifest install, no product behaviour
#[test]
fn a_manifest_installed_on_a_narrower_shard_keeps_every_page() {
    const RECORDS: usize = 200;

    let source_dir = tempfile::tempdir().expect("tempdir");
    let source = engine_on(source_dir.path());
    // The SOURCE runs on the whole range, which is what `load_shard` gives and what the engine's
    // own default is.
    load_on(&source, WIDE_END);
    let keys = seed(&source, RECORDS);

    // DENOMINATOR ONE: the source's blocks carry explicit buckets, and they are outside the range
    // the target will be loaded with. Without this the install below cannot drop anything and a
    // pass says nothing.
    let outside_the_target: usize = {
        let shards = source.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        // Read off the bucket map -- where the blocks ARE on the source -- rather than off an
        // address, which carries no bucket to read.
        shard
            .bucket_index
            .bucket_map
            .iter()
            .filter(|(routing_bucket, _)| **routing_bucket > NARROW_END)
            .map(|(_, bucket)| bucket.block_index.len())
            .sum()
    };
    println!(
        "  source on 0..{WIDE_END}: {outside_the_target} of {RECORDS} pages sit in a bucket above \
         {NARROW_END}"
    );
    assert!(
        outside_the_target > RECORDS / 2,
        "only {outside_the_target} of {RECORDS} source pages sit in a bucket above {NARROW_END}, \
         so a narrowed install would have nothing to re-file and this test cannot see the defect \
         it exists for"
    );

    let buckets: Vec<u32> = {
        let shards = source.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!buckets.is_empty(), "the source holds no bucket to dump");
    let manifest = source
        .create_bucket_dump_manifest(1, buckets)
        .expect("the source can dump its own buckets");

    // The TARGET is loaded on the production narrow range -- a cross-range restore.
    let target_dir = tempfile::tempdir().expect("tempdir");
    let target = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        target_dir.path().join("cache"),
        // The blocks live where the source wrote them; only the index is restored.
        source_dir.path().join("pages"),
        target_dir.path().join("indexes"),
    );
    load_on(&target, NARROW_END);
    target
        .install_bucket_dump_manifest(&manifest)
        .expect("the manifest installs on the narrower shard");

    // DENOMINATOR TWO: the install put blocks in the index at all.
    let installed = {
        let shards = target.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        collect_live_block_entries(shard).len()
    };
    println!("  installed on 0..{NARROW_END}: {installed} of {RECORDS} pages present");
    assert_eq!(
        installed, RECORDS,
        "{installed} of {RECORDS} pages survived installing a whole-shard manifest onto a \
         narrower shard. `rebuild_bucket_block_ownership` FILTERS on the range it is given, so an \
         install that passes the INSTALLING shard's range drops every page whose source bucket \
         falls outside it. A whole-shard image is installed whole or it is truncated."
    );

    // And the records read back, which is the thing a user would notice.
    let readable = read_back(&target, &keys);
    assert_eq!(
        readable, RECORDS,
        "{readable} of {RECORDS} records were readable after a cross-range manifest install. This \
         is the failure `storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc` \
         caught when this change first narrowed the install's range: the record comes back None."
    );
}

// =============================================================================================
// 4. Migration: a store written before the change, read after it
// =============================================================================================

/// A STORE FILED UNDER THE WHOLE RANGE READS BACK WHOLE AFTER THE CHANGE, DRIVEN.
///
/// Changing the argument fixes new writes; it does not move blocks already filed. So: write a store
/// the way the engine wrote one BEFORE this change -- blocks in the older-build state, reconstructed
/// on `0, u32::MAX`, persisted -- then restart on it and assert what comes back, rather than
/// arguing about it.
///
/// THE ANSWER IS THAT NOTHING HAS TO BE MIGRATED, and the reason is specific: the whole-range
/// reconstruct files a block under a bucket the shard does not hold but does NOT write that bucket
/// onto the address. The addresses come back still unrouted, so the next rebuild -- which the load
/// runs anyway -- re-files them under the shard's own range on its own. Asserted in three parts,
/// because "the records are readable" would hold even if every block had been orphaned into a
/// bucket nothing will ever open.
///
/// rust-internal: drives the engine's own restart, no product behaviour
#[test]
fn a_store_filed_under_the_whole_range_reads_back_whole_after_the_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let keys: Vec<String>;

    // ARM ONE: write the store the way the engine wrote one before this change.
    {
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        keys = seed(&engine, RECORDS);
        let filed_outside;
        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");
            let round_tripped = round_trip_every_page_through_the_wire(shard);
            assert_eq!(
                round_tripped,
                keys.len(),
                "the door covered {round_tripped} keys"
            );
            // THE OLD ARGUMENT, which is what every one of the nine sites passed.
            rebuild_bucket_first_index(1, shard, 0, WIDE_END);
            refresh_bucket_runtime_flags(shard);
            let contents = bucket_contents(shard);
            filed_outside = contents
                .keys()
                .filter(|routing_bucket| **routing_bucket > NARROW_END)
                .count();
            println!(
                "  written with the OLD argument: {} pages in {} buckets, {filed_outside} of them \
                 outside the shard's own range",
                page_total(&contents),
                contents.len()
            );
        }
        // DENOMINATOR: the store really is in the broken state this test exists to migrate. A
        // run where the old argument had filed everything in range would pass every assertion
        // below for the wrong reason.
        assert!(
            filed_outside > 0,
            "the arm meant to write a store filed OUTSIDE the shard's range filed none there, so \
             there is nothing to migrate and the assertions below prove nothing"
        );
        engine.flush_shard_index(1);
    }

    // ARM TWO: restart on those same directories, with the change in place.
    {
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);

        // PART ONE: every record is still readable.
        let readable = read_back(&engine, &keys);
        assert_eq!(
            readable,
            keys.len(),
            "{readable} of {} records survived a restart on a store filed under the whole range. \
             A page filed under a bucket nothing summarises is supposed to be INVISIBLE TO A \
             SCOPED READER and still reachable by the full walk; if it is not readable at all, \
             this change is repairing active loss rather than latent misfiling.",
            keys.len()
        );

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let entries = collect_live_block_entries(shard);

        // PART TWO: no block was orphaned or duplicated by the reload.
        assert_eq!(
            entries.len(),
            keys.len(),
            "the store went in holding {} pages and came back holding {}. Anything other than \
             equality is a page orphaned or a page duplicated by the load.",
            keys.len(),
            entries.len()
        );

        // PART THREE -- THE LOAD-BEARING ONE. Every block came back filed INSIDE the shard's own
        // range, which is WHY no migration step is needed: the bucket the old argument chose was
        // never written onto the address, so the rebuild the load runs derives the placement from
        // the range the shard is stamped with. This used to be asserted as "the addresses are still
        // unrouted"; an address cannot be routed now, so the statement moves to where the blocks
        // actually are -- the keys of the bucket map.
        let outside: Vec<u32> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1");
            shard
                .bucket_index
                .bucket_map
                .iter()
                .filter(|(routing_bucket, bucket)| {
                    **routing_bucket > NARROW_END && !bucket.block_index.is_empty()
                })
                .map(|(routing_bucket, _)| *routing_bucket)
                .collect()
        };
        println!(
            "  after the restart: {readable} of {} records readable, {} pages, {} buckets above \
             the shard's end still holding pages",
            keys.len(),
            entries.len(),
            outside.len()
        );
        assert!(
            outside.is_empty(),
            "{} buckets above the shard's end came back holding pages; the first few are {:?}. A \
             page there is invisible to every reader that scopes by bucket, so if this ever holds, \
             a store written before this change needs a rebuild before it is loaded with it and \
             this test is the one that has to say so.",
            outside.len(),
            &outside[..outside.len().min(5)]
        );
    }
}
