// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE BUCKET A READER GUESSES, AGAINST THE BUCKET THE PAGE IS FILED UNDER.
//!
//! mx#1945 closed by naming a tenth site "no call site can correct": a routing range supplied as a
//! HARD-CODED LITERAL rather than taken as an argument, invisible to an argument-reading scan. It
//! named three. Enumerated with the compiler -- both doors renamed, iterated to a zero pass --
//! there are EIGHT, and they do not divide the way the three suggested.
//!
//! # THE SPLIT IS PLACES AGAINST ATTRIBUTES, AND ONLY ONE HALF NEEDS A RANGE
//!
//! Two of the eight decide WHERE A PAGE IS FILED (`upsert_bucket_index_block_inner`,
//! `sync_bucket_index_object_blocks_with_mode`). Those genuinely need the shard's range and could
//! not reach it: they take `&mut ShardState` and a `ShardId`, never `&self`, so
//! `shard_routing_range` -- which reads the engine's info rows -- is not available. They were NOT
//! changed here, and were held as a measured, named defect instead. mx#1953 fixed them by giving
//! the shard its own range to carry, and the measurement is inverted rather than deleted; see
//! `shard_carried_range` and section 5 below.
//!
//! FIVE do not decide placement at all. They ATTRIBUTE an already-filed page to a bucket, for a
//! report, for a dump-reuse comparison, or -- once -- to decide whether to DELETE the record. Each
//! asked "which bucket is this page in?" and answered it with `block_routing_bucket(key, 0,
//! u32::MAX)`: a hash over the WHOLE range, which is the bucket the page is filed under only when
//! the shard is loaded on the whole range too.
//!
//! **They never needed a range.** `collect_bucket_index_live_block_entries` walks `bucket_map`, so
//! the filed bucket is the KEY OF THE MAP IT IS WALKING -- and it was iterating `.values()` and
//! discarding it. Carrying it (`LiveBlockEntry::filed_routing_bucket`) removes the guess without
//! threading anything, and is correct under every range rather than under one.
//!
//! The eighth is a record field, not a placement, and is right as written. See
//! `the_delete_outcomes_routing_bucket_is_a_record_field_no_reader_consults`.
//!
//! # WHY THIS IS VALUE-IDENTICAL EXCEPT ON THE DEFECT
//!
//! The new field is consulted only where a fallback already fired -- `address.routing_bucket()`
//! still wins, unchanged, and it is `Some` for every page the live write path produces, because
//! `append_value` stamps the bucket and NO production call site passes it `None` (16 of 16 pass
//! `Some`). So on a store written by the current build these five sites compute exactly what they
//! computed before. `the_five_readers_are_value_identical_while_every_page_is_routed` asserts that
//! with its denominator stated, and it is the control that says the measurement below is the
//! change rather than the fixture.
//!
//! # WHAT mx#1945 LEFT SPLIT, MEASURED
//!
//! mx#1945 moved six REBUILD call sites to the shard's own range. The five readers here kept
//! guessing the whole range. On a store whose pages arrived unrouted -- mx#1942's door, an index
//! written before `BlockAddressWire::routing_bucket` existed -- that split is total:
//!
//! ```text
//!   after the load path's own rebuild on 0..1023:
//!     200 of 200 pages filed where the NARROW formula puts them
//!       0 of 200 pages filed where the WIDE   formula puts them
//!   the five readers agreed with the filing on   0 of 200
//!   an eviction round with delete_drop:  4 victims chosen, 0 objects dropped
//! ```
//!
//! So mx#1945's conclusion CHANGES. "The maintenance accounting is actively wrong" understates it:
//! `apply_storage_eviction` is not accounting, it is the actuator, and it dropped nothing. A round
//! that chooses victims and frees nothing does not look like a failure from outside -- it looks
//! like a store under memory pressure that will not come down.

use super::*;
use super::round_walk_scope::call_arguments;
use crate::engine::hashing::block_routing_bucket;
use crate::engine::storage_bucket_internals::{
    collect_live_block_entries, rebuild_bucket_first_index, refresh_bucket_runtime_flags,
    validate_bucket_ownership_index_from_entries,
};
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;
/// The end bucket `load_shard` uses, and the one all eight literal sites spell.
const WIDE_END: u32 = u32::MAX;

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
        table_name: "inner-range".to_string(),
        shard_uri: "local://inner-range/1".to_string(),
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
/// by the ENGINE'S OWN DECODER rather than by the setter -- mx#1942's door, reused so all three
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

/// What the five readers now compute for each page, and what they computed before: the bucket the
/// page is FILED under, against the whole-range hash of its key.
fn reader_attribution(
    shard: &crate::engine::state::ShardState,
) -> (usize, usize, usize) {
    let entries = collect_live_block_entries(shard);
    let total = entries.len();
    let mut agrees_with_filing = 0usize;
    let mut whole_range_agrees = 0usize;
    let filed: BTreeMap<(String, u64), u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .flat_map(|(bucket_id, bucket)| {
            bucket
                .block_index
                .values()
                .map(move |page| ((page.object_key.to_string(), page.address.offset), *bucket_id))
        })
        .collect();
    for entry in &entries {
        let Some(actual) = filed.get(&(entry.object_key.to_string(), entry.address.offset)) else {
            continue;
        };
        let now = entry
            .address
            .routing_bucket()
            .or(entry.filed_bucket())
            .unwrap_or_else(|| block_routing_bucket(&entry.object_key, 0, WIDE_END));
        let before = entry
            .address
            .routing_bucket()
            .unwrap_or_else(|| block_routing_bucket(&entry.object_key, 0, WIDE_END));
        if now == *actual {
            agrees_with_filing += 1;
        }
        if before == *actual {
            whole_range_agrees += 1;
        }
    }
    (total, agrees_with_filing, whole_range_agrees)
}

// =============================================================================================
// 1. THE CONTROL: while every page is routed, nothing moves
// =============================================================================================

/// THE FIVE READERS ARE VALUE-IDENTICAL ON A STORE THE CURRENT BUILD WROTE.
///
/// `address.routing_bucket()` still wins; the new field is consulted only where a fallback would
/// otherwise have fired. So this arm must come out bucket-for-bucket the same under the old
/// expression and the new one, and the DENOMINATOR is asserted first -- a fixture where nothing
/// was routed would make this pass for the opposite reason.
///
/// Run at BOTH widths, because "the shard's range is the whole range" is exactly the condition
/// under which the defect is invisible, and a control that only holds at one width says nothing
/// about the other.
#[test]
fn the_five_readers_are_value_identical_while_every_page_is_routed() {
    for end_routing_bucket in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed(&engine, RECORDS);

        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let entries = collect_live_block_entries(shard);

        let routed = entries
            .iter()
            .filter(|entry| entry.address.routing_bucket().is_some())
            .count();
        assert_eq!(
            routed,
            keys.len(),
            "0..{end_routing_bucket}: {routed} of {} pages the LIVE WRITE PATH produced carry an \
             explicit routing bucket. All of them must, or this control is vacuous: it only says \
             anything while the fallback cannot fire.",
            keys.len()
        );

        let (total, now, before) = reader_attribution(shard);
        assert_eq!(
            total,
            keys.len(),
            "0..{end_routing_bucket}: the fixture wrote {} keys and the index holds {total} pages",
            keys.len()
        );
        assert_eq!(
            now, before,
            "0..{end_routing_bucket}: the five readers agree with the filing on {now} pages under \
             the new expression and {before} under the old one. While every address is routed the \
             two MUST be identical -- `filed_routing_bucket` is consulted only after \
             `address.routing_bucket()` returns None."
        );
        assert_eq!(
            now, total,
            "0..{end_routing_bucket}: {now} of {total} routed pages are attributed to the bucket \
             they are filed under"
        );
        println!(
            "  0..{end_routing_bucket}: {total} pages, {routed} routed, attribution agrees \
             {now}/{total} both before and after"
        );
    }
}

// =============================================================================================
// 2. THE MEASUREMENT: with the door open, the five readers agreed with nothing
// =============================================================================================

/// ELEMENT BY ELEMENT, NOT BY COUNT, IN BOTH DIRECTIONS.
///
/// The subject arm is a store whose pages arrived UNROUTED, filed by the load path's own rebuild
/// on the shard's own range -- which is what mx#1945 made that rebuild pass. The five readers then
/// hashed each key over the WHOLE range and named a bucket the shard does not hold.
///
/// THE CONTROL IS THE SAME FIXTURE WITH ROUTED ADDRESSES, above: a page whose address carries its
/// own bucket is attributed by that bucket whichever expression is used, so the control must come
/// out identical and does. The subject arm must differ on every page, and does.
#[test]
fn a_reader_attributes_an_unrouted_page_to_the_bucket_it_is_actually_filed_under() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let (stripped, contents) = {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let stripped = strip_routing_buckets(shard);
        // The rebuild the load path runs, with the argument mx#1945 gave it.
        rebuild_bucket_first_index(1, shard, 0, NARROW_END);
        refresh_bucket_runtime_flags(shard);
        (stripped, bucket_contents(shard))
    };
    assert_eq!(
        stripped,
        keys.len(),
        "the door covered {stripped} of {} keys; a door that covered none would make every \
         number below zero for the wrong reason",
        keys.len()
    );

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let entries = collect_live_block_entries(shard);

    // DENOMINATOR FIRST.
    let unrouted = entries
        .iter()
        .filter(|entry| entry.address.routing_bucket().is_none())
        .count();
    assert_eq!(
        unrouted,
        keys.len(),
        "{unrouted} of {} pages came out of the reconstruct unrouted. All of them should have -- \
         `rebuild_bucket_first_index` stamps only the object id onto an address -- and if this \
         ever becomes zero the rest of this test measures nothing.",
        keys.len()
    );

    // Element by element: every page is filed where the SHARD'S range puts it, and none where the
    // whole range does. Both directions asserted, because only one of them loses a page.
    let mut filed_narrow = 0usize;
    let mut filed_wide = 0usize;
    for (bucket_id, object_keys) in &contents {
        for key in object_keys {
            if *bucket_id == block_routing_bucket(key, 0, NARROW_END) {
                filed_narrow += 1;
            }
            if *bucket_id == block_routing_bucket(key, 0, WIDE_END) {
                filed_wide += 1;
            }
            assert!(
                *bucket_id <= NARROW_END,
                "page {key} is filed in bucket {bucket_id}, which a shard loaded on 0..{NARROW_END} \
                 does not hold"
            );
        }
    }
    assert_eq!(
        filed_narrow,
        keys.len(),
        "{filed_narrow} of {} pages are filed where the shard's own range puts them",
        keys.len()
    );
    assert_eq!(
        filed_wide, 0,
        "{filed_wide} pages are filed where the WHOLE range puts them, on a shard that holds \
         0..{NARROW_END}"
    );

    let (total, now, before) = reader_attribution(shard);
    println!("  {total} unrouted pages filed on 0..{NARROW_END}");
    println!("    readers agree with the filing, NEW expression: {now} of {total}");
    println!("    readers agree with the filing, OLD expression: {before} of {total}");
    assert_eq!(
        now, total,
        "the five readers must attribute every page to the bucket it is filed under; they agree \
         on {now} of {total}"
    );
    assert_eq!(
        before, 0,
        "THE OLD EXPRESSION MUST AGREE WITH NOTHING HERE, or this fixture is not reproducing the \
         defect: it agreed on {before} of {total}. `block_routing_bucket(key, 0, u32::MAX)` names \
         a bucket above {NARROW_END} for every key, and the shard holds none of them."
    );
}

// =============================================================================================
// 3. THE ACTUATOR: an eviction round that chose victims and freed nothing
// =============================================================================================

/// `apply_storage_eviction` IS NOT ACCOUNTING. IT DELETES RECORDS.
///
/// Its filter asks which bucket each live page is in and keeps the page's object key when that
/// bucket is one of the round's victims. The victims come from `bucket_storage_summaries`, which
/// reads `bucket_map` -- so the victim buckets are where pages are FILED. Asking the whole-range
/// hash instead names a bucket no victim can be, so the filter matches nothing and the round frees
/// nothing.
///
/// Measured as a BAND, not a bound: a round that dropped everything would pass a floor, and a
/// round that dropped nothing passes a ceiling. Both ends are named.
#[test]
fn an_eviction_round_over_unrouted_pages_drops_the_objects_it_chose() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let unrouted = {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let stripped = strip_routing_buckets(shard);
        rebuild_bucket_first_index(1, shard, 0, NARROW_END);
        refresh_bucket_runtime_flags(shard);
        let entries = collect_live_block_entries(shard);
        assert_eq!(
            stripped,
            keys.len(),
            "the door covered {stripped} of {} keys",
            keys.len()
        );
        entries
            .iter()
            .filter(|entry| entry.address.routing_bucket().is_none())
            .count()
    };
    assert_eq!(
        unrouted,
        keys.len(),
        "{unrouted} of {} pages are unrouted; below that this round cannot exercise the fallback \
         at all and the numbers below say nothing",
        keys.len()
    );

    let report = engine.apply_storage_eviction(1, 1, 4, false, true);
    let chosen = report.selected_victims.len();
    let dropped = report.dropped_object_count;
    println!("  victims chosen {chosen}, objects dropped {dropped}, of {} records", keys.len());

    assert!(
        chosen > 0,
        "the round chose {chosen} victims, so it did no choosing and the drop count below \
         measures nothing"
    );
    // THE BAND. A floor alone passes if the round started dropping the whole store; a ceiling
    // alone passes if it dropped nothing, which is the defect.
    assert!(
        dropped > 0,
        "the round chose {chosen} victim buckets and dropped {dropped} objects. Zero is what the \
         whole-range guess produces: the filter names a bucket above {NARROW_END} for every page, \
         no victim bucket can equal it, and the round frees nothing while reporting that it ran."
    );
    assert!(
        dropped < keys.len(),
        "the round dropped {dropped} of {} objects. Dropping the whole store means the filter \
         stopped filtering, which passes a floor just as happily as the right answer does.",
        keys.len()
    );
}

// =============================================================================================
// 4. THE CROSS-RANGE RESTORE, DRIVEN IN BOTH DIRECTIONS
// =============================================================================================

/// WRITE ON ONE RANGE, READ ON THE OTHER, BOTH WAYS.
///
/// mx#1945's refutation was found this way and cost a record: a whole-shard image installed on a
/// narrower shard is installed whole or truncated. This change touches no install path and no
/// filter, so nothing here should move -- and that is exactly the claim that has to be DRIVEN
/// rather than asserted, in both directions, because both are silent when they fail.
#[test]
fn a_store_written_on_one_range_reads_back_whole_on_the_other() {
    for (wrote_on, read_on) in [(NARROW_END, WIDE_END), (WIDE_END, NARROW_END)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys = {
            let engine = engine_on(dir.path());
            load_on(&engine, wrote_on);
            let keys = seed(&engine, RECORDS);
            assert_eq!(
                read_back(&engine, &keys),
                keys.len(),
                "0..{wrote_on}: the store was not readable before the restart, so a loss after it \
                 would be attributed to the wrong thing"
            );
            let _ = engine.flush_shard_index(1);
            keys
        };

        let engine = engine_on(dir.path());
        load_on(&engine, read_on);
        let readable = read_back(&engine, &keys);

        let (pages, outside) = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            let contents = bucket_contents(shard);
            let pages: usize = contents.values().map(Vec::len).sum();
            let outside = contents
                .keys()
                .filter(|bucket_id| **bucket_id > read_on)
                .count();
            (pages, outside)
        };

        println!(
            "  wrote on 0..{wrote_on}, read on 0..{read_on}: {readable}/{} records, {pages} pages, \
             {outside} buckets outside the reading range",
            keys.len()
        );
        assert_eq!(
            readable,
            keys.len(),
            "wrote on 0..{wrote_on}, read on 0..{read_on}: {readable} of {} records came back",
            keys.len()
        );
        assert_eq!(
            pages,
            keys.len(),
            "wrote on 0..{wrote_on}, read on 0..{read_on}: {pages} of {} pages survived the \
             restore",
            keys.len()
        );
    }
}

// =============================================================================================
// 5. THE TWO SITES THAT FILED A PAGE UNDER THE WHOLE RANGE -- NOW IN shard_carried_range.rs
// =============================================================================================
//
// `the_two_sites_that_file_a_page_still_file_it_under_the_whole_range` lived here. It asserted
// that `upsert_bucket_index_block_inner` filed `inner-000000` in bucket 1,422,005,296 on a shard
// holding 0..1023, so that whoever threaded the range would get a failing test the moment they
// succeeded. mx#1953 succeeded -- by carrying the range on `ShardState` rather than through the
// writers' 32 production call sites -- and so this test failed, exactly as it was written to.
//
// IT IS NOT DELETED, IT IS INVERTED, and it moved to the change that inverted it:
// `shard_carried_range::the_two_sites_that_file_a_page_now_file_it_under_the_shards_own_range`
// keeps the same fixture and the same two bucket numbers, asserting the page IS in 398 and is
// NOT in 1,422,005,296 -- and it drives the SECOND site too, which the version here did not.

// =============================================================================================
// 6. THE EIGHTH SITE, RIGHT AS WRITTEN
// =============================================================================================

/// A DELETE OUTCOME'S `routing_bucket` IS A RECORD FIELD NO READER CONSULTS FOR PLACEMENT.
///
/// `mark_bucket_index_block_deleted_with` stages `WalOutcomeItem { routing_bucket:
/// block_routing_bucket(key, 0, u32::MAX), address: None, deleted: true, meta: true }`. It is the
/// one site of the eight that is not a fallback -- it fires on every typed removal.
///
/// It is still right as written, because NOTHING READS IT AS A PLACEMENT. `resolved_address()` is
/// the only path from an outcome item's bucket onto an address, and it returns `None` when
/// `address` is `None`, which it always is here -- a removal wrote no page, so there is none to
/// name. The replay arm for these items (`apply_outcome_item`, the `deleted` branch) re-derives
/// the buckets to clear from `object_block_lookup` and never looks at `item.routing_bucket`.
///
/// Asserted rather than read off the source, and the denominator -- that the removal was actually
/// recorded and replayed -- is stated first.
#[test]
fn the_delete_outcomes_routing_bucket_is_a_record_field_no_reader_consults() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);

    for field in ["f1", "f2", "f3"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "inner-hash".to_string(),
                field: field.to_string(),
                value: vec![b'x'; 8],
            },
        });
        assert!(response.status.ok, "hash write {field}: {:?}", response.status);
    }
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashDelete {
            key: "inner-hash".to_string(),
            field: "f2".to_string(),
        },
    });
    assert!(response.status.ok, "hash delete: {:?}", response.status);

    // The outcome this recorded names a bucket above the shard's range -- that is the literal.
    let recorded = block_routing_bucket("inner-hash", 0, WIDE_END);
    assert!(
        recorded > NARROW_END,
        "the whole-range bucket for this key is {recorded}, inside 0..{NARROW_END}, so this test \
         cannot tell a wrong bucket from a right one"
    );

    // What matters is that the removal SURVIVES a restart, which is the only thing that bucket
    // could have broken.
    let _ = engine.flush_shard_index(1);
    drop(engine);
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);

    let mut present = Vec::new();
    for field in ["f1", "f2", "f3"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet {
                key: "inner-hash".to_string(),
                field: field.to_string(),
            },
        });
        if matches!(
            &response.response,
            crate::types::CommandResponse::Bytes { value: Some(_) }
        ) {
            present.push(field);
        }
    }
    println!("  outcome recorded bucket {recorded} (shard holds 0..{NARROW_END})");
    println!("  fields present after the restart: {present:?}");
    assert_eq!(
        present,
        vec!["f1", "f3"],
        "the removal recorded with a whole-range bucket did not survive the restart intact. If \
         this fails, that bucket IS read somewhere and the site is not the inert record field \
         this test claims."
    );
}

/// THE MODEL-MAP WALK GENUINELY DOES NOT KNOW THE FILING, AND MUST SAY SO.
///
/// `collect_live_block_entries` has two arms. The bucket-index arm walks `bucket_map` and knows
/// the filing exactly. The model-map arm -- taken when `bucket_map` is empty -- reads the pages an
/// object owns and has no index to read a filing out of, so every entry it produces must answer
/// `None` and let the five readers fall back.
///
/// THIS TEST EXISTS BECAUSE A MUTANT SURVIVED. Dropping the `filing_is_known` check, so
/// `filed_bucket()` always answers `Some(self.filed_routing_bucket)`, passed every other test in
/// this file: nothing reached the model-map arm. An unconditional accessor would attribute every
/// page that arm produces to BUCKET 0 -- the field's zero value -- which is a bucket a shard very
/// often does hold, so it would not even look wrong.
///
/// The DENOMINATOR is asserted first: if the fixture ever stops taking the model-map arm, this
/// passes because it measured nothing.
#[test]
fn an_entry_from_the_model_map_walk_reports_no_filing_rather_than_bucket_zero() {
    use crate::engine::storage_bucket_internals::collect_model_live_block_entries;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, 32);

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    let from_model_maps = collect_model_live_block_entries(shard);
    assert_eq!(
        from_model_maps.len(),
        keys.len(),
        "the model-map walk produced {} entries for {} keys; if it produces none this test \
         measures nothing",
        from_model_maps.len(),
        keys.len()
    );

    let claiming_a_filing = from_model_maps
        .iter()
        .filter(|entry| entry.filed_bucket().is_some())
        .count();
    let claiming_bucket_zero = from_model_maps
        .iter()
        .filter(|entry| entry.filed_bucket() == Some(0))
        .count();
    println!(
        "  {} model-map entries, {claiming_a_filing} claiming a filing, {claiming_bucket_zero} \
         claiming bucket 0",
        from_model_maps.len()
    );
    assert_eq!(
        claiming_a_filing, 0,
        "{claiming_a_filing} of {} entries from the MODEL-MAP walk claim to know which bucket \
         they are filed in. That walk reads no index, so it cannot know -- and an entry that \
         claims a filing it does not have sends every reader in this file to bucket 0.",
        from_model_maps.len()
    );

    // The bucket-index arm is the control: the same shard, the other walk, where the filing IS
    // known -- so "0 claim a filing" above is a property of the walk and not of the fixture.
    let from_index = collect_live_block_entries(shard);
    let index_claiming = from_index
        .iter()
        .filter(|entry| entry.filed_bucket().is_some())
        .count();
    assert_eq!(
        index_claiming,
        from_index.len(),
        "{index_claiming} of {} entries from the BUCKET-INDEX walk report a filing; all of them \
         must, or the control says nothing about the arm above",
        from_index.len()
    );
}

// =============================================================================================
// 7. THE GUARD: the eight sites, held as a list
// =============================================================================================

/// THE EIGHT SITES THAT SUPPLY A ROUTING RANGE AS A LITERAL, AND WHICH OF THEM STILL GUESS.
///
/// mx#1942's guard reads the ARGUMENTS a call site passes, so it cannot see a site that takes no
/// range at all. This is that scan's blind spot, held the same way: as a LIST compared by
/// equality, not as a count, with vacuity floors first.
///
/// The list does NOT shrink when a site is fixed. The literal stays in the source as the last
/// resort for an entry that came from the model maps, where there is genuinely no filing to read;
/// what changes is whether the site consults `filed_routing_bucket` BEFORE reaching it. So the
/// scan holds two lists -- the sites, and the sites that still guess -- and both are non-empty, so
/// a matcher that broke and found nothing fails against either rather than passing as clean.
///
/// CLASSIFIED BY ITEM, NOT BY FILE. mx#1942's scan excluded test FILES, which is how a
/// `#[cfg(test)]` helper inside a production file came to be counted among nine production sites.
/// A `#[cfg(test)]` region inside a production file is excluded here by brace-tracking it.
///
/// rust-internal: reads this crate's own call sites, no product behaviour
#[test]
fn every_site_that_hard_codes_the_whole_routing_range_is_accounted_for() {
    use std::path::Path;

    /// Every production site that supplies `0, u32::MAX` to a routing-bucket door ITSELF, as
    /// `file :: door xN`. Enumerated with the compiler: both doors renamed at their definitions,
    /// rustc asked to name every caller, iterated to a ZERO PASS (69 sites, then 13 that pass one
    /// had suppressed, then none).
    /// A FIX REMOVES A SITE FROM THIS LIST ONLY WHEN IT PLACES. mx#1949 wrote that fixing a site
    /// does not shrink this list, and for the five it fixed that is right: they ATTRIBUTE an
    /// already-filed page, and the literal has to stay behind `filed_bucket()` as the last resort
    /// for a model-map entry that has no filing to read. The two that PLACE are different. A page
    /// being filed for the first time has no filing to read, so there is no fallback to put the
    /// literal behind; the range itself had to change. mx#1953 carries it on `ShardState`, and the
    /// two `block_routing_bucket` calls in `storage_bucket_internals.rs` now name
    /// `start_routing_bucket` and are no longer literal-range sites at all.
    const LITERAL_RANGE_SITES: &[&str] = &[
        "engine.rs :: block_routing_bucket x1",
        "engine/storage_bucket_internals.rs :: bucket_for_object x2",
        "engine/storage_lifecycle_methods.rs :: bucket_for_object x1",
        "engine/storage_reporting.rs :: bucket_for_object x2",
    ];

    /// Of those, the sites that do NOT consult the bucket the page is filed under first.
    ///
    /// ONE LEFT, and it is the record field no reader consults -- see
    /// `the_delete_outcomes_routing_bucket_is_a_record_field_no_reader_consults`. The two that
    /// FILE a page left this list by leaving the list above: they read the shard's own range now.
    /// `shard_carried_range::the_two_sites_that_file_a_page_now_file_it_under_the_shards_own_range`
    /// is what holds that, with both buckets named.
    const STILL_GUESSING: &[&str] = &["engine.rs :: block_routing_bucket x1"];

    const DOORS: [&str; 2] = ["block_routing_bucket(", "bucket_for_object("];

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![root.clone()];
    let mut files_scanned = 0usize;
    let mut lines_scanned = 0usize;
    let mut excluded = 0usize;
    let mut cfg_test_lines_skipped = 0usize;
    let mut door_calls = 0usize;
    let mut literal_sites: Vec<String> = Vec::new();
    let mut guessing_sites: Vec<String> = Vec::new();

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
            // BY ITEM, NOT BY FILE: the line ranges a `#[cfg(test)]` item covers, brace-tracked.
            let test_regions = cfg_test_regions(&lines);
            cfg_test_lines_skipped += test_regions
                .iter()
                .map(|(start, end)| end.saturating_sub(*start) + 1)
                .sum::<usize>();
            let relative = display
                .rsplit_once("/src/")
                .map(|(_, tail)| tail.to_string())
                .unwrap_or(display.clone());
            for (index, line) in lines.iter().enumerate() {
                if test_regions
                    .iter()
                    .any(|(start, end)| index >= *start && index <= *end)
                {
                    continue;
                }
                // A COMMENT NAMING A CALL IS NOT A CALL, and this cost a red run. The scan
                // counted its own module doc in storage_bucket_internals.rs, which spells the
                // whole-range call out in prose to say what these sites used to do, and a note
                // in storage_reporting.rs that does the same -- 10 sites where the compiler
                // enumeration found 8. The compiler is the authority here; a text scan that
                // disagrees with it is the text scan's bug, and the two must be reconciled
                // rather than the expectation moved to match the scan.
                let code = line.trim_start();
                if code.starts_with("//") {
                    continue;
                }
                for needle in DOORS {
                    if !line.contains(needle) {
                        continue;
                    }
                    if line.contains(&format!("fn {}", &needle[..needle.len() - 1])) {
                        continue;
                    }
                    let Some(arguments) = call_arguments(&lines, index, needle) else {
                        continue;
                    };
                    door_calls += 1;
                    // A LITERAL RANGE IS ONE THE SITE SPELLS ITSELF. A site reading the shard's
                    // range names `start_routing_bucket`; mx#1942 records why testing for
                    // `u32::MAX` first is too eager, and the same order is kept here.
                    if arguments.contains("start_routing_bucket")
                        || arguments.contains("routing_bucket_count(")
                    {
                        continue;
                    }
                    if !arguments.contains("u32::MAX") {
                        continue;
                    }
                    let site = format!("{relative} :: {}", &needle[..needle.len() - 1]);
                    literal_sites.push(site.clone());
                    // Does this site read where the page is actually filed before falling back?
                    // The check is over the call's own expression, which begins at most a few
                    // lines above the door.
                    let window_start = index.saturating_sub(4);
                    let consults_filing = lines[window_start..=index]
                        .iter()
                        .any(|text| text.contains("filed_bucket()"));
                    if !consults_filing {
                        guessing_sites.push(site);
                    }
                }
            }
        }
    }

    // VACUITY FLOORS, BEFORE ANY VERDICT.
    assert!(
        files_scanned > 80,
        "the scan read {files_scanned} production .rs files under {}; below 80 it has stopped \
         reading the crate and both lists below are empty for the wrong reason",
        root.display()
    );
    assert!(
        lines_scanned > 100_000,
        "the scan read {lines_scanned} lines; below 100,000 it is not reading this crate"
    );
    assert!(
        excluded > 10,
        "the scan excluded {excluded} test files; this crate has more than ten, so an exclusion \
         matching fewer means the matcher moved"
    );
    // THE EXCLUSION'S OWN DENOMINATOR. A `#[cfg(test)]` tracker that started matching everything
    // would empty both lists and read exactly like a clean tree, so what it removed is counted and
    // held BELOW the total -- an exclusion that swallowed the crate fails here.
    assert!(
        cfg_test_lines_skipped < lines_scanned / 2,
        "the #[cfg(test)] tracker excluded {cfg_test_lines_skipped} of {lines_scanned} lines. \
         Above half the crate it has stopped tracking braces and is swallowing production code, \
         which empties both lists below without failing anything else."
    );
    assert!(
        door_calls >= 30,
        "the scan found {door_calls} calls to the two routing-bucket doors in production items; \
         there were 50 when this was written, and below 30 the matcher is finding something other \
         than the calls"
    );

    literal_sites.sort();
    guessing_sites.sort();
    println!(
        "  {files_scanned} files / {lines_scanned} lines scanned, {excluded} test files excluded, \
         {cfg_test_lines_skipped} #[cfg(test)] lines skipped"
    );
    println!(
        "  {door_calls} production calls to the two doors; {} supply the range as a literal, {} \
         of those still guess",
        literal_sites.len(),
        guessing_sites.len()
    );

    let collapse = |sites: &[String]| -> Vec<String> {
        let mut counted: BTreeMap<String, usize> = BTreeMap::new();
        for site in sites {
            *counted.entry(site.clone()).or_default() += 1;
        }
        counted
            .iter()
            .map(|(site, count)| format!("{site} x{count}"))
            .collect()
    };

    let observed = collapse(&literal_sites);
    for site in &observed {
        println!("    literal range   {site}");
    }
    let observed_guessing = collapse(&guessing_sites);
    for site in &observed_guessing {
        println!("    STILL GUESSING  {site}");
    }

    let mut expected: Vec<String> = LITERAL_RANGE_SITES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(
        observed, expected,
        "the set of production sites that supply the routing range as a LITERAL has moved.\n\
         If you ADDED one, it is a site no caller can correct: it will name a bucket the shard \
         does not hold on every shard not loaded on the whole range.\n\
         If you REMOVED one, say which in this list. Note that FIXING one of these does not \
         remove it -- the literal stays as the last resort for a model-map entry that has no \
         filing to read -- so a removal here means the call itself went away."
    );

    let mut expected_guessing: Vec<String> = STILL_GUESSING.iter().map(|s| s.to_string()).collect();
    expected_guessing.sort();
    assert_eq!(
        observed_guessing, expected_guessing,
        "the set of literal-range sites that do NOT consult the bucket the page is filed under \
         has moved.\n\
         If you FIXED one -- made it read `filed_routing_bucket` before falling back -- delete it \
         from STILL_GUESSING, and check that \
         `the_two_sites_that_file_a_page_still_file_it_under_the_whole_range` still describes the \
         tree.\n\
         If you ADDED one, a reader has gone back to guessing a bucket out of an object key, and \
         on any shard not loaded on 0..u32::MAX it will name a bucket the shard does not hold."
    );
}

/// The line ranges a `#[cfg(test)]` item covers, by tracking braces from the item it decorates.
///
/// mx#1942 classified by FILE and so read a `#[cfg(test)]` helper inside a production file as
/// production. This is the item-level answer that scan was missing.
fn cfg_test_regions(lines: &[&str]) -> Vec<(usize, usize)> {
    let mut regions = Vec::new();
    let mut index = 0usize;
    while index < lines.len() {
        if !lines[index].trim_start().starts_with("#[cfg(test)]") {
            index += 1;
            continue;
        }
        let mut start = index + 1;
        while start < lines.len()
            && (lines[start].trim_start().starts_with('#') || lines[start].trim().is_empty())
        {
            start += 1;
        }
        if start >= lines.len() {
            break;
        }
        let mut depth = 0isize;
        let mut opened = false;
        let mut end = start;
        while end < lines.len() {
            // Strings and line comments can hold braces; both are stripped before counting, or a
            // single `"{"` in a message swallows the rest of the file into a test region.
            let stripped = strip_strings_and_comments(lines[end]);
            depth += stripped.matches('{').count() as isize;
            depth -= stripped.matches('}').count() as isize;
            if stripped.contains('{') {
                opened = true;
            }
            if opened && depth <= 0 {
                break;
            }
            if !opened && stripped.trim_end().ends_with(';') {
                break;
            }
            end += 1;
        }
        regions.push((start, end.min(lines.len() - 1)));
        index = end + 1;
    }
    regions
}

fn strip_strings_and_comments(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            continue;
        }
        if ch == '/' && chars.peek() == Some(&'/') {
            break;
        }
        out.push(ch);
    }
    out
}
