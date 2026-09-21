// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE A PAGE IS FILED, AFTER THE ARGUMENT IS RECONCILED.
//!
//! mx#1942 established by construction that where a page is FILED is decided by an argument, and
//! held the nine production call sites that passed `0, u32::MAX` as a LIST -- deliberately
//! shipping no engine change. This file is the engine change, and what had to be true before it
//! could be made.
//!
//! # THE ASYMMETRY THAT SPLITS THE NINE
//!
//! The three rebuilds do NOT react to the range the same way, and the difference decides whether
//! passing the shard's range is safe:
//!
//! * `rebuild_bucket_first_index` uses the range ONLY as the fallback placement for an address
//!   that carries no routing bucket of its own. Narrowing it moves unrouted pages; it can drop
//!   nothing.
//! * `rebuild_bucket_block_ownership` does the same AND THEN FILTERS -- `if routing_bucket <
//!   start_routing_bucket || routing_bucket > end_routing_bucket { continue; }`. On `0..u32::MAX`
//!   that filter cannot fire. On the shard's own range it can, and a page whose address carries an
//!   EXPLICIT bucket outside the range is dropped from the index entirely.
//! * `promote_model_maps_to_bucket_index_authority` delegates to ownership, so it inherits the
//!   filter.
//!
//! `only_ownership_drops_a_page_whose_explicit_bucket_is_outside_the_shards_range` measures both
//! directions rather than reading the source: the same fixture, the same pages, one carrying an
//! in-range explicit bucket and one carrying an out-of-range one, through both functions.
//!
//! WHAT BOUNDS THAT HAZARD is that the state it needs does not arise. The live write path stamps
//! `block_routing_bucket(key, start, end)` -- the shard's own range -- at `append_value`, so an
//! explicit bucket is in range by construction; and the reconstruct stamps only the object id onto
//! an address, so a page filed under the whole range does NOT acquire that bucket explicitly. Both
//! halves are asserted with a denominator rather than assumed, because "it cannot happen" is the
//! claim this campaign has twice found to be a fixture artefact.
//!
//! And the filter is not new behaviour being introduced: ELEVEN production call sites already pass
//! the shard's own range to these same rebuilds, so the filter already fires on the narrow range
//! everywhere else in the engine. The change makes the outliers agree with the majority.
//!
//! # MIGRATION, DRIVEN RATHER THAN ARGUED
//!
//! Pages already filed under the whole range exist in any store written by the current code.
//! Changing the argument fixes new writes; it does not move old pages.
//! `a_store_filed_under_the_whole_range_reads_back_whole_after_the_change` writes a store with the
//! OLD argument, restarts on it, and asserts what comes back: every record readable, every page
//! present, and -- the load-bearing observation -- every address still UNROUTED, which is why the
//! next rebuild re-files them in range on its own. No migration step is needed, and that is a
//! measurement here, not a hope.

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

/// An address as an index written before the routing-bucket field carried it holds one, produced
/// by the ENGINE'S OWN DECODER rather than by the setter -- mx#1942's door, reused so the two
/// files construct the same state the same way.
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

/// Put every string page into the state an older index decodes into. Returns how many it changed,
/// so a caller can assert the fixture did something.
fn strip_routing_buckets(shard: &mut crate::engine::state::ShardState) -> usize {
    let keys: Vec<String> = shard.strings.keys().cloned().collect();
    for key in &keys {
        let older = as_an_older_build_wrote_it(shard.strings.get(key).expect("key present"));
        shard.strings.insert(key.clone(), older);
    }
    keys.len()
}

/// Every bucket that holds at least one page, with the object keys it holds, sorted. The element
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
// 1. The asymmetry: which rebuild can DROP a page, and which cannot
// =============================================================================================

/// ONLY `rebuild_bucket_block_ownership` HAS A DROP FILTER, AND IT IS WHY THE NINE ARE NOT ONE
/// CHANGE.
///
/// Passing the shard's own range to `rebuild_bucket_first_index` can only move an unrouted page.
/// Passing it to `rebuild_bucket_block_ownership` also starts ENFORCING the range against pages
/// that carry an explicit bucket, and a page outside it is dropped from the index rather than
/// moved. Measured both ways so the claim is a count rather than a reading of the source.
///
/// This is the hazard that had to be bounded before the argument could be changed at the two
/// ownership sites and the two promote sites, and the two tests after this one are what bound it.
///
/// rust-internal: reads the engine's own rebuilds, no product behaviour
#[test]
fn only_ownership_drops_a_page_whose_explicit_bucket_is_outside_the_shards_range() {
    // Well outside a 0..1023 shard, and a value no key on this fixture can hash to under the
    // narrow range, so a page found under it got there by the stamp and not by the fallback.
    const OUTSIDE: u32 = 900_000;
    const INSIDE: u32 = 7;

    let mut observed: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for (placement, explicit_bucket) in [("inside", INSIDE), ("outside", OUTSIDE)] {
        for (rebuild, is_ownership) in [("ownership", true), ("first_index", false)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, NARROW_END);
            let keys = seed(&engine, RECORDS);

            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");

            // Stamp the explicit bucket under test onto every page's address.
            let model_keys: Vec<String> = shard.strings.keys().cloned().collect();
            assert_eq!(
                model_keys.len(),
                keys.len(),
                "{placement}/{rebuild}: the model map holds {} of {} written keys, so the stamp \
                 below does not cover the fixture",
                model_keys.len(),
                keys.len()
            );
            for key in &model_keys {
                let mut address = shard.strings.get(key).expect("key present").clone();
                address.set_routing_bucket(Some(explicit_bucket));
                shard.strings.insert(key.clone(), address);
            }

            // DENOMINATOR: the stamp took, so the filter is what answers below.
            //
            // Read off the MODEL MAP, not off `collect_live_block_entries`. Both rebuilds
            // repopulate the bucket index FROM `collect_model_live_block_entries`, while
            // `collect_live_block_entries` answers from the bucket INDEX whenever `bucket_map` is
            // non-empty -- which it is here, the fixture having just written it. Asking the index
            // whether the model map was stamped reports 0 for every stamp, however well it took,
            // and the first draft of this test did exactly that: the denominator went red and the
            // verdict below would have been a clean pass on a fixture where nothing was stamped.
            let stamped = shard
                .strings
                .values()
                .filter(|address| address.routing_bucket() == Some(explicit_bucket))
                .count();
            assert_eq!(
                stamped,
                keys.len(),
                "{placement}/{rebuild}: meant to stamp {} pages with bucket {explicit_bucket} and \
                 stamped {stamped}; the branch under test is not the code answering",
                keys.len()
            );

            if is_ownership {
                rebuild_bucket_block_ownership(1, shard, 0, NARROW_END);
            } else {
                rebuild_bucket_first_index(1, shard, 0, NARROW_END);
            }
            refresh_bucket_runtime_flags(shard);
            observed.insert((placement, rebuild), page_total(&bucket_contents(shard)));
        }
    }

    for ((placement, rebuild), pages) in &observed {
        println!("  explicit bucket {placement}, {rebuild}: {pages} pages survived");
    }

    // An IN-RANGE explicit bucket survives both, which is the control: it says the drop below is
    // attributable to the RANGE and not to the rebuild having lost the pages for some other
    // reason.
    assert_eq!(
        observed.get(&("inside", "ownership")).copied(),
        Some(RECORDS),
        "an explicit bucket INSIDE the shard's range must survive the ownership rebuild"
    );
    assert_eq!(
        observed.get(&("inside", "first_index")).copied(),
        Some(RECORDS),
        "an explicit bucket INSIDE the shard's range must survive the first-index rebuild"
    );

    // `rebuild_bucket_first_index` has no filter, so an out-of-range explicit bucket survives it.
    assert_eq!(
        observed.get(&("outside", "first_index")).copied(),
        Some(RECORDS),
        "`rebuild_bucket_first_index` files a page under its address's own bucket and filters \
         nothing, so all {RECORDS} pages must survive even with an out-of-range explicit bucket. \
         If this ever stops holding, the four first-index call sites this change moved to the \
         shard's range acquire a drop they did not have."
    );

    // And ownership drops every one of them. This is the whole reason the four ownership/promote
    // sites needed the two tests below before their argument could be changed.
    assert_eq!(
        observed.get(&("outside", "ownership")).copied(),
        Some(0),
        "`rebuild_bucket_block_ownership` filters `routing_bucket < start || routing_bucket > end` \
         and drops the page rather than re-filing it, so on a 0..{NARROW_END} shard every page \
         carrying an explicit bucket of {OUTSIDE} must be dropped. This is the losing direction of \
         this whole change: it is safe ONLY because the state it needs does not arise, which is \
         what `the_live_write_path_never_stamps_a_bucket_outside_the_shards_range` and \
         `a_store_filed_under_the_whole_range_reads_back_whole_after_the_change` measure."
    );
}

/// THE STATE THE DROP NEEDS IS NOT ONE THE ENGINE PRODUCES, WITH A DENOMINATOR.
///
/// Two halves, asserted separately because they fail for different reasons:
///
///   1. the LIVE WRITE PATH stamps `block_routing_bucket(key, start, end)` at `append_value`, so
///      every explicit bucket it produces is in range by construction; and
///   2. the RECONSTRUCT does not stamp a bucket at all -- it calls `set_object_id` on the address
///      it files and nothing else -- so a page filed under the whole range does NOT come away
///      carrying that bucket explicitly.
///
/// Together they are what says the filter above cannot fire on a store this engine wrote. Both
/// widths, because on the wide shard the two placements are the same expression and a fixture that
/// only ran there could not tell the halves apart.
///
/// rust-internal: reads the engine's own write path, no product behaviour
#[test]
fn the_live_write_path_never_stamps_a_bucket_outside_the_shards_range() {
    for end_routing_bucket in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed(&engine, RECORDS);

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");

        // HALF ONE: the live write path.
        let entries = collect_live_block_entries(shard);
        assert_eq!(
            entries.len(),
            keys.len(),
            "0..{end_routing_bucket}: the fixture wrote {} keys and the index holds {} pages",
            keys.len(),
            entries.len()
        );
        let unrouted = entries
            .iter()
            .filter(|entry| entry.address.routing_bucket().is_none())
            .count();
        let outside: Vec<u32> = entries
            .iter()
            .filter_map(|entry| entry.address.routing_bucket())
            .filter(|routing_bucket| *routing_bucket > end_routing_bucket)
            .collect();
        println!(
            "  0..{end_routing_bucket}: {} live pages, {unrouted} unrouted, {} outside the range",
            entries.len(),
            outside.len()
        );
        assert!(
            outside.is_empty(),
            "0..{end_routing_bucket}: {} of {} pages the LIVE WRITE PATH produced carry an \
             explicit routing bucket above the shard's end; the first few are {:?}. \
             `append_value` stamps `block_routing_bucket(key, start, end)`, so this cannot \
             happen -- and if it ever does, the ownership rebuild drops exactly these pages.",
            outside.len(),
            entries.len(),
            &outside[..outside.len().min(5)]
        );
        assert_eq!(
            unrouted, 0,
            "0..{end_routing_bucket}: {unrouted} of {} pages arrived unrouted off the live write \
             path. That is not itself a defect, but it means the denominator for the half below \
             is not the fixture this test thinks it is.",
            entries.len()
        );

        // HALF TWO: the reconstruct does not stamp a bucket, so the whole-range filing does not
        // become explicit and does not stick.
        let stripped = strip_routing_buckets(shard);
        assert_eq!(
            stripped,
            keys.len(),
            "0..{end_routing_bucket}: the strip covered {stripped} of {} keys",
            keys.len()
        );
        rebuild_bucket_first_index(1, shard, 0, WIDE_END);
        refresh_bucket_runtime_flags(shard);
        let after = collect_live_block_entries(shard);
        let still_unrouted = after
            .iter()
            .filter(|entry| entry.address.routing_bucket().is_none())
            .count();
        assert_eq!(
            still_unrouted,
            after.len(),
            "0..{end_routing_bucket}: {still_unrouted} of {} pages came out of a WHOLE-RANGE \
             reconstruct unrouted, and all of them should have. If the reconstruct ever starts \
             stamping the bucket it chose, a whole-range filing becomes an EXPLICIT out-of-range \
             bucket that the ownership rebuild then drops -- and the migration this change relies \
             on stops being free.",
            after.len()
        );
    }
}

// =============================================================================================
// 2. The direction: a page is filed where the shard's own range puts it
// =============================================================================================

/// THE PRODUCTION FLUSH NOW FILES EVERY PAGE INSIDE THE SHARD'S OWN RANGE, ELEMENT BY ELEMENT.
///
/// Driven through `flush_shard_index`, which is `persistence.rs`'s own entry point and carries two
/// of the nine call sites, rather than by calling the rebuild directly -- so what is measured is
/// the path, not the helper.
///
/// THE ASSERTION IS SET EQUALITY AGAINST A WITNESS COMPUTED OUTSIDE THE INDEX: the fixture's own
/// key list, hashed with the shard's own range. A count would pass for two sets wrong by the same
/// amount, and the failure that matters here is one page under a bucket nothing summarises.
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

        // Put the model maps into the older-build state, which is the state on which the argument
        // decides anything at all. Without this the addresses answer and the fallback never runs.
        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");
            let stripped = strip_routing_buckets(shard);
            assert_eq!(
                stripped,
                keys.len(),
                "0..{end_routing_bucket}: the strip covered {stripped} of {} keys",
                keys.len()
            );
        }

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

/// NO PAGE CHANGES BUCKET EXCEPT THE ONES THIS CHANGE INTENDS TO MOVE.
///
/// The strong form mx#1942's measurement did not need and this change does: a full listing of what
/// each bucket holds, compared element by element against a control, so a page that quietly moved
/// somewhere neither arm intended is a failure rather than a matching count.
///
/// THE CONTROL IS THE SAME FIXTURE WITH ROUTED ADDRESSES. A page whose address carries its own
/// routing bucket is placed by that bucket under BOTH arguments -- the range is only consulted
/// when the address is silent. So the routed control must come out bucket-for-bucket IDENTICAL
/// under the two ranges, and the unrouted arm must differ on every page. Two directions, because
/// an arm that moved nothing and an arm that moved everything both read as "a number changed".
///
/// rust-internal: reads the engine's own rebuild, no product behaviour
#[test]
fn no_page_changes_bucket_except_the_unrouted_ones_the_range_decides() {
    /// Build the fixture, optionally strip the routing buckets, rebuild on `rebuild_end`, and
    /// return what each bucket holds.
    fn contents_after(strip: bool, rebuild_end: u32) -> BTreeMap<u32, Vec<String>> {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, RECORDS);
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        if strip {
            let stripped = strip_routing_buckets(shard);
            assert_eq!(stripped, keys.len(), "the strip covered {stripped} keys");
        }
        rebuild_bucket_first_index(1, shard, 0, rebuild_end);
        refresh_bucket_runtime_flags(shard);
        let contents = bucket_contents(shard);
        assert_eq!(
            page_total(&contents),
            keys.len(),
            "strip={strip} end={rebuild_end}: {} pages went in and {} came out",
            keys.len(),
            page_total(&contents)
        );
        contents
    }

    /// Every (key -> bucket) pair, so two arms can be compared page by page rather than bucket by
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

    // THE CONTROL: addresses left alone, so the range decides nothing.
    let routed_wide = contents_after(false, WIDE_END);
    let routed_narrow = contents_after(false, NARROW_END);
    assert_eq!(
        routed_wide, routed_narrow,
        "a page whose address carries its own routing bucket is filed under THAT bucket whatever \
         range the rebuild is given, so the control must be identical under both. It is not, which \
         means the range is reaching pages it does not decide and every count in this file is \
         measuring something else."
    );

    // THE SUBJECT: addresses in the older-build state, so the range decides every placement.
    let unrouted_wide = contents_after(true, WIDE_END);
    let unrouted_narrow = contents_after(true, NARROW_END);

    let wide_placement = placement(&unrouted_wide);
    let narrow_placement = placement(&unrouted_narrow);
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
    let outside_after: Vec<u32> = unrouted_narrow
        .keys()
        .copied()
        .filter(|routing_bucket| *routing_bucket > NARROW_END)
        .collect();

    println!(
        "  control (routed): {} buckets, identical under both ranges",
        routed_wide.len()
    );
    println!(
        "  subject (unrouted): wide {} buckets -> narrow {} buckets, {} of {RECORDS} pages changed \
         bucket, {} buckets outside the shard's range after",
        unrouted_wide.len(),
        unrouted_narrow.len(),
        moved.len(),
        outside_after.len()
    );

    // EVERY unrouted page moves, and that is the intended set: the two placement functions differ
    // on every key whose hash exceeds the narrow bucket count, which is every key at a 64-bit hash.
    assert_eq!(
        moved.len(),
        RECORDS,
        "{} of {RECORDS} unrouted pages changed bucket between the two ranges. These are exactly \
         the pages the range decides, so all of them must move; a smaller number means some page \
         was placed by something other than the argument.",
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

// =============================================================================================
// 3. The refutation: a manifest is a WHOLE-SHARD image and must be installed whole
// =============================================================================================

/// INSTALLING A MANIFEST ON A NARROWER SHARD MUST NOT DROP THE PAGES THAT FALL OUTSIDE IT.
///
/// This is the site this change tried to reconcile and had to put back, so the reason is recorded
/// as a measurement rather than as a comment.
///
/// `install_bucket_dump_manifest` rebuilds ownership over `decode_index_bytes(&manifest
/// .index_bytes)` -- a WHOLE-SHARD image, written by whatever routing range the SOURCE shard ran
/// on, whose pages already carry explicit routing buckets from that range.
/// `rebuild_bucket_block_ownership` does not merely PLACE an unrouted page by the range it is
/// handed; it also FILTERS on it. So passing the INSTALLING shard's range deletes every page whose
/// source bucket falls outside it, and the record is simply gone.
///
/// The denominator is asserted before the verdict, and it is the whole point: the fixture is only
/// meaningful if the source pages really do carry buckets the target's range excludes. On a source
/// loaded `0..u32::MAX` and a target loaded `0..1023` they do, for essentially every key.
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

    // DENOMINATOR ONE: the source's pages carry explicit buckets, and they are outside the range
    // the target will be loaded with. Without this the install below cannot drop anything and a
    // pass says nothing.
    let outside_the_target: usize = {
        let shards = source.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        collect_live_block_entries(shard)
            .iter()
            .filter_map(|entry| entry.address.routing_bucket())
            .filter(|routing_bucket| *routing_bucket > NARROW_END)
            .count()
    };
    println!(
        "  source on 0..{WIDE_END}: {outside_the_target} of {RECORDS} pages carry a bucket above \
         {NARROW_END}"
    );
    assert!(
        outside_the_target > RECORDS / 2,
        "only {outside_the_target} of {RECORDS} source pages carry a routing bucket above \
         {NARROW_END}, so a narrowed install would drop almost nothing and this test cannot see \
         the defect it exists for"
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
        // The pages live where the source wrote them; only the index is restored.
        source_dir.path().join("pages"),
        target_dir.path().join("indexes"),
    );
    load_on(&target, NARROW_END);
    target
        .install_bucket_dump_manifest(&manifest)
        .expect("the manifest installs on the narrower shard");

    // DENOMINATOR TWO: the install put pages in the index at all.
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
/// Changing the argument fixes new writes; it does not move pages already filed. So: write a store
/// the way the engine wrote one BEFORE this change -- pages in the older-build state, reconstructed
/// on `0, u32::MAX`, persisted -- then restart on it and assert what comes back, rather than
/// arguing about it.
///
/// THE ANSWER IS THAT NOTHING HAS TO BE MIGRATED, and the reason is specific: the whole-range
/// reconstruct files a page under a bucket the shard does not hold but does NOT write that bucket
/// onto the address. The addresses come back still unrouted, so the next rebuild -- which the load
/// runs anyway -- re-files them under the shard's own range on its own. Asserted in three parts,
/// because "the records are readable" would hold even if every page had been orphaned into a
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
            let stripped = strip_routing_buckets(shard);
            assert_eq!(stripped, keys.len(), "the strip covered {stripped} keys");
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

        // PART TWO: no page was orphaned or duplicated by the reload.
        assert_eq!(
            entries.len(),
            keys.len(),
            "the store went in holding {} pages and came back holding {}. Anything other than \
             equality is a page orphaned or a page duplicated by the load.",
            keys.len(),
            entries.len()
        );

        // PART THREE -- THE LOAD-BEARING ONE. The addresses are still unrouted, which is WHY no
        // migration step is needed: the bucket the old argument chose was never written onto the
        // address, so the rebuild the load runs re-derives the placement from the shard's own
        // range. Had that bucket stuck, it would be an EXPLICIT out-of-range bucket and the
        // ownership rebuild would DROP the page -- measured in
        // `only_ownership_drops_a_page_whose_explicit_bucket_is_outside_the_shards_range`.
        let carrying_a_bucket: Vec<u32> = entries
            .iter()
            .filter_map(|entry| entry.address.routing_bucket())
            .filter(|routing_bucket| *routing_bucket > NARROW_END)
            .collect();
        println!(
            "  after the restart: {readable} of {} records readable, {} pages, {} carrying an \
             explicit bucket outside the shard's range",
            keys.len(),
            entries.len(),
            carrying_a_bucket.len()
        );
        assert!(
            carrying_a_bucket.is_empty(),
            "{} pages came back carrying an EXPLICIT routing bucket above the shard's end; the \
             first few are {:?}. Those are precisely the pages `rebuild_bucket_block_ownership` \
             now drops, so if this ever holds, a store written before this change needs a rebuild \
             before it is loaded with it and this test is the one that has to say so.",
            carrying_a_bucket.len(),
            &carrying_a_bucket[..carrying_a_bucket.len().min(5)]
        );
    }
}
