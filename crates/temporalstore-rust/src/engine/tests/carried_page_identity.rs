// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT TELLS TWO CARRIED PAGES OF ONE KEY APART, AND WHY THE COMPONENT CANNOT LEAVE THE OBJECT ID.
//!
//! #2007 measured that an object IS an element here: `stable_block_object_id` folds the component
//! into the identity, so pages per object are p50/p90/p99 = 1 and the same pages grouped on
//! `(kind, key)` reach MAX 100. The proposal that follows from it is to drop the component from the
//! identity, let one id name a KEY, and recover the per-page term from `BlockAddress::block_id` --
//! the ordinal #2008 threaded through `append_block_of_object`. The index arithmetic works: ~5 B a
//! page, landing on #2007's independently measured 6.409-6.496 B/page.
//!
//! THIS MODULE IS THE REFUTATION, AND IT IS NOT THE ONE THE MANDATE EXPECTED.
//!
//! The expected obstacle was the HASH FIELD NAME. For a hash the component IS the field name and
//! #2005 found it decoded by nothing, so the fear was that a component-free identity discards it.
//! IT DOES NOT, and the first test says so: `BlockIndex::component` is its own serialized field
//! beside the address, and `bucket_index_component_block_addresses` -- the whole-object door -- takes
//! `(shard, model_id, object_key)` and returns the component off the page entry. It never consults
//! the id. A hash page's field name survives an identity that has never heard of it.
//!
//! THIS IS THE COMPLEMENT OF #2009, NOT A RIVAL CLAIM, and the two are easy to read as one.
//! #2009 refuted removing `component` from the page ENTRY: `shard.hashes` is `skip_serializing`, so
//! for a hash the entry's component is the ONLY copy of the field name, and a component-less entry
//! answers `HashGetAll` with every field named "". This module refutes removing the component from
//! the ID. They point opposite ways on purpose:
//!
//!   * #2009  the component must stay ON THE ENTRY  -- or the field name is gone;
//!   * here   the component must stay IN THE ID     -- or the carried page cannot be found.
//!
//! The first test below depends on #2009's result rather than competing with it: the field name
//! survives a component-free identity PRECISELY BECAUSE the entry still holds it. Had #2009 gone the
//! other way and the component left the entry, that test would fail, and the two findings would be
//! one finding rather than two.
//!
//! WHAT DOES NOT SURVIVE IS THE CARRIED PAGE. `block_in_wal` serves a page whose only durable copy
//! is its WAL record, and it is reached on two read paths: a WAL-resident async write, and -- since
//! the single barrier acks on the log fsync and defers the block fsync -- ANY synchronous write
//! whose block a crash left unwritten. `read_block` resolves a record and then picks the page out of
//! it with
//!
//!     pages.iter().find(|page| page.object_id == object_id)
//!
//! `StagedBlock` is `{ object_id, bytes }` and nothing else. The object id is not merely the
//! registry's KEY -- it is the only discriminator BETWEEN PAGES INSIDE ONE RECORD, and one record
//! carries many pages ("an ingest reads several of the fields its own batch just wrote", per the
//! function's own comment). Collapse the identity onto the key and every field of one hash written
//! in one batch becomes the same page to that `find`, which returns the FIRST. The third test drives
//! exactly that through the product's own `read_block` and measures how many pages come back as
//! another page's bytes.
//!
//! AND THE PROPOSED FIX CANNOT REACH IT. Re-keying the registry to
//! `(store_id, shard, object_id, block_id)` fixes the HashMap and leaves the in-record `find`
//! untouched, because a `StagedBlock` carries no ordinal -- it is a WAL WIRE structure, so putting
//! one there is a second format break in a different log from the index the operator authorised.
//! The fourth test pins the struct's two fields for that reason.
//!
//! EVEN WITH THE ORDINAL THERE, IT IS THE WRONG TERM, and #2008 said so in the tree before this was
//! proposed: "`max` falls after a delete and the next insert is handed the ordinal that was just
//! freed. That is correct for a position and WOULD BE SILENT CORRUPTION FOR AN IDENTITY, which is
//! why the element's identity stays in the component." The same commit leaves the ordinal at 0 for
//! every page of an object past its 65,535th element -- a container this store serves today -- so
//! those pages would share `(object_id, 0)` as well. Neither property is re-derived here; both are
//! #2008's own measurements, cited.
//!
//! THE COLLAPSE HAS THREE SITES, NOT ONE, and the second test counts the pages each loses:
//!
//!   1. `block_in_wal`'s registry, `HashMap<(store, shard, object_id), _>` -- last write wins.
//!   2. `read_block`'s in-record `find` -- first match wins. WAL wire format.
//!   3. `ShardState::wal_resident_blocks`, `BTreeMap<u64, WalResidentBlock>` -- one placement per
//!      key survives a reload, and `rehydrate_wal_resident_blocks` re-registers only that one.
//!
//! Only the first is named in the proposal.

#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::engine::hashing::stable_block_object_id;
use crate::wal::StagedBlock;

const OPERATOR_END: u32 = 1023;
/// Eight keys and twenty-five fields: enough that a per-key group is unmistakably not a per-page
/// one, and small enough that every row below is exhaustive rather than sampled.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population. A string page's component is already `None`, so the transformation this
/// module measures is the IDENTITY on it and the mechanism predicts no effect whatever.
const ROUTED_KEYS: usize = 200;

// =================================================================================================
// HARNESS
// =================================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "carried-page-identity".to_string(),
        shard_uri: "local://carried-page-identity/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// The value a field is written with, distinct per (key, field) so that serving one field's page for
/// another is DETECTABLE rather than a coincidence of equal bytes.
fn hash_value(k: usize, f: usize) -> Vec<u8> {
    format!("h{k}-f{f}-value-{}", "x".repeat(16)).into_bytes()
}

fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: hash_value(k, f),
            });
        }
    }
    ack_batch(engine, commands);
}

fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: format!("s{i}-value").into_bytes(),
            })
            .collect(),
    );
}

/// Every live page of one kind, as `(object_key, component)`, read off the page entries themselves.
fn live_pages(engine: &TemporalEngine, kind: &str) -> Vec<(String, Option<String>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != kind {
                continue;
            }
            held.push((
                page.object_key.to_string(),
                page.component.as_deref().map(str::to_string),
            ));
        }
    }
    held.sort();
    held
}

/// Percentiles of a sorted sample, by nearest-rank. A mean over this distribution says nothing --
/// the whole question is the tail.
fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

// =================================================================================================
// 1. THE QUESTION THAT WAS EXPECTED TO SINK THIS: DOES A HASH PAGE'S FIELD NAME SURVIVE?
// =================================================================================================

/// A HASH PAGE'S FIELD NAME IS HELD BESIDE THE ID, NOT INSIDE IT, AND SURVIVES A RELOAD.
///
/// `BlockIndex::component: Option<Arc<str>>` is its own `#[serde]` field, written to the index and
/// read back from it. `bucket_index_component_block_addresses(shard, model_id, object_key)` -- the
/// whole-object door, and the reader #1986 found -- takes NO component and returns
/// `(page.component.clone(), page.address.clone())` off the entry. The id is not an input to it and
/// not an output of it.
///
/// Asserted across an unload/load cycle, so the names come back off the DISK copy rather than out of
/// a resident map, and asserted per key with the expectation derived from the loop bound.
///
/// THE SECOND ARM IS WHAT MAKES THIS A MEASUREMENT RATHER THAN A RESTATEMENT. For every key it also
/// computes the identity the proposed change would give each of that key's pages -- one number for
/// all twenty-five -- and asserts it IS one number. So the field names were recovered in full at the
/// same time as the ids became indistinguishable, which is the only way to show the recovery does
/// not depend on them.
///
/// rust-internal: drives the engine's own unload/load cycle, no external surface
#[test]
fn a_hash_pages_field_name_is_held_beside_the_id_and_survives_a_component_free_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);

    let written: BTreeSet<String> = (0..MEMBERS_PER_KEY).map(|f| format!("f{f}")).collect();
    assert_eq!(
        written.len(),
        MEMBERS_PER_KEY,
        "DENOMINATOR: the seed writes {} distinct field names, not {MEMBERS_PER_KEY}",
        written.len()
    );

    engine.unload_shard(1);
    load_on(&engine);

    println!("\n=== hash field names recovered after a reload, per key ===");
    println!("  key        pages  names recovered  distinct component-free ids");
    let mut total_recovered = 0usize;
    for k in 0..CONTAINER_KEYS {
        let key = format!("h{k}");
        let pairs = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            crate::engine::bucket_store::bucket_index_component_block_addresses(shard, "hash", &key)
        };
        let recovered: BTreeSet<String> = pairs
            .iter()
            .filter_map(|(component, _)| component.as_deref().map(str::to_string))
            .collect();
        // The identity the change would hand every page of this key: no component, so one number.
        let free_ids: BTreeSet<u64> = pairs
            .iter()
            .map(|_| stable_block_object_id(1, "hash", &key, None))
            .collect();
        println!(
            "  {key:<10} {:>5}  {:>15}  {:>27}",
            pairs.len(),
            recovered.len(),
            free_ids.len()
        );
        assert_eq!(
            recovered, written,
            "key {key} recovered {} of {MEMBERS_PER_KEY} field names after a reload -- the \
             component is NOT independently recoverable and the change is sunk here",
            recovered.len()
        );
        assert_eq!(
            free_ids.len(),
            1,
            "key {key} would carry {} component-free ids, not one -- the arm below is not \
             measuring what it claims",
            free_ids.len()
        );
        total_recovered += recovered.len();
    }
    assert_eq!(
        total_recovered,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "{total_recovered} field names recovered across {CONTAINER_KEYS} keys, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    println!(
        "  VERDICT: {total_recovered}/{} field names recovered while every key's pages share ONE \
         component-free id. The field name SURVIVES.",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
}

// =================================================================================================
// 2. WHAT THE COLLAPSE ACTUALLY IS, WITH ITS DENOMINATOR AND ITS CONTROL.
// =================================================================================================

/// HOW MANY PAGES SHARE AN IDENTITY ONCE THE COMPONENT LEAVES IT -- HISTOGRAM, NOT MEAN.
///
/// Every live page is grouped by the identity the change would give it. A group of size one is a
/// page the three collapse sites can still tell apart; a group of size N > 1 is N pages they cannot,
/// of which at most one is reachable.
///
/// THE CONTROL IS THE STRING ARM and it is the reason the hash arm means anything. A string page's
/// component is already `None`, so this transformation is the IDENTITY on it: the mechanism predicts
/// every group is size one and no page is lost. Its exercised bytes are asserted non-zero first,
/// because a control that read nothing reports 0.00% lost and "not exercised" wears the face of "did
/// not move".
///
/// rust-internal: reads a seeded store's own page entries, no external surface
#[test]
fn a_component_free_identity_collapses_every_page_of_one_key_onto_one_number() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    println!("\n=== pages per component-free identity ===");
    println!("  kind    pages  groups  p50  p90  p99  MAX  unreachable  share");
    let mut rows: Vec<(&str, usize, usize)> = Vec::new();
    for kind in ["hash", "string"] {
        let pages = live_pages(&engine, kind);
        assert!(
            !pages.is_empty(),
            "DENOMINATOR: no {kind} pages were exercised, so every share below is 0.00% for the \
             wrong reason"
        );
        let mut groups: BTreeMap<u64, usize> = BTreeMap::new();
        for (object_key, _component) in &pages {
            *groups
                .entry(stable_block_object_id(1, kind, object_key, None))
                .or_default() += 1;
        }
        let mut sizes: Vec<usize> = groups.values().copied().collect();
        sizes.sort();
        // A group of N holds N pages and serves at most one, so N - 1 are unreachable.
        let unreachable: usize = sizes.iter().map(|size| size.saturating_sub(1)).sum();
        let share = 100.0 * unreachable as f64 / pages.len() as f64;
        println!(
            "  {kind:<6} {:>6} {:>7} {:>4} {:>4} {:>4} {:>4} {:>12} {share:>6.2}%",
            pages.len(),
            sizes.len(),
            percentile(&sizes, 50.0),
            percentile(&sizes, 90.0),
            percentile(&sizes, 99.0),
            sizes.last().copied().unwrap_or(0),
            unreachable
        );
        rows.push((kind, pages.len(), unreachable));
    }

    let (_, hash_pages, hash_unreachable) = rows[0];
    let (_, string_pages, string_unreachable) = rows[1];

    assert_eq!(
        hash_pages,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the hash arm exercised {hash_pages} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        hash_unreachable,
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1),
        "the hash arm loses {hash_unreachable} pages, not {} -- one survivor per key",
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1)
    );

    // THE CONTROL. Exercised first, then asserted flat.
    assert_eq!(
        string_pages, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_pages} string pages, not {ROUTED_KEYS}"
    );
    assert_eq!(
        string_unreachable, 0,
        "the control lost {string_unreachable} of {string_pages} pages -- a kind whose component is \
         already None cannot be moved by removing the component, so the grouping above is wrong"
    );
    println!(
        "  CONTROL: string, {string_pages} pages exercised, {string_unreachable} unreachable, 0.00%"
    );
}

// =================================================================================================
// 3. THE REFUTATION, DRIVEN THROUGH THE PRODUCT'S OWN READ.
// =================================================================================================

/// TWO CARRIED PAGES SHARING AN OBJECT ID SERVE ONE ANOTHER'S BYTES.
///
/// This is the test that decides the change. It does not simulate `read_block`; it calls it.
///
/// A record is appended carrying two pages with DIFFERENT bytes -- the shape a batch writing two
/// fields of one hash produces -- and registered exactly as `register_record` registers a real
/// append. Under today's identity the two pages carry different ids and each reads back its own
/// bytes. Under the proposed identity they carry the SAME id, and `read_block`'s
/// `find(|page| page.object_id == object_id)` returns the first page for both: the second page's
/// bytes are not reachable through any argument `read_block` accepts.
///
/// THE TWO ARMS ARE THE SAME RECORD SHAPE AND DIFFER ONLY IN THE IDENTITY, which is what makes the
/// second arm attributable to the identity rather than to the fixture. The first arm is also the
/// control for the second: if it did not serve both pages correctly, the second arm's failure would
/// say nothing about the change.
///
/// rust-internal: drives the WAL append and the block-in-WAL read directly, no external surface
#[test]
fn two_carried_pages_sharing_an_object_id_serve_one_anothers_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let first_bytes = b"field-f0-bytes".to_vec();
    let second_bytes = b"field-f1-bytes-which-differ".to_vec();
    assert_ne!(
        first_bytes, second_bytes,
        "the two pages must differ or serving one for the other is undetectable"
    );

    println!("\n=== read_block over one record carrying two pages of one key ===");
    println!("  identity                      pages  served correctly  wrong bytes  missing");

    let mut results: Vec<(&str, usize, usize, usize)> = Vec::new();

    // --- ARM A: TODAY. The component is folded in, so the two pages carry different ids. ---
    // --- ARM B: THE CHANGE. The component is gone, so both carry the key's one id.       ---
    for (label, first_id, second_id) in [
        (
            "today (kind:key:component)",
            stable_block_object_id(1, "hash", "hk", Some("f0")),
            stable_block_object_id(1, "hash", "hk", Some("f1")),
        ),
        (
            "proposed (kind:key)",
            stable_block_object_id(1, "hash", "hk", None),
            stable_block_object_id(1, "hash", "hk", None),
        ),
    ] {
        let staged = vec![
            StagedBlock {
                object_id: first_id,
                bytes: first_bytes.clone(),
            },
            StagedBlock {
                object_id: second_id,
                bytes: second_bytes.clone(),
            },
        ];
        let (record, log_id) = engine
            .wal_store
            .append_with_sync_staged(
                1,
                Command::StringSet {
                    key: format!("carrier-{label}"),
                    value: b"carrier".to_vec(),
                },
                true,
                staged.clone(),
            )
            .expect("the carrier record must append");
        crate::engine::block_in_wal::register_record(
            &engine.block_store,
            1,
            &staged,
            log_id,
            record.sequence,
            &engine.wal_store,
        );

        // THREE OUTCOMES, NOT TWO, and the split is the whole severity of this finding. A page
        // that reads MISSING is a read fault: the caller falls through and the WAL replay still
        // holds the value. A page that reads ANOTHER PAGE'S BYTES is silent corruption -- the
        // caller is handed a plausible answer for a field it did not ask about, caches it, and
        // serves it. Folding the two into one "wrong" counter would let this test pass at 1/2
        // whichever of them the collapse produced, and they are not the same finding.
        let mut correct = 0usize;
        let mut other_pages_bytes = 0usize;
        let mut missing = 0usize;
        for (object_id, want) in [(first_id, &first_bytes), (second_id, &second_bytes)] {
            match crate::engine::block_in_wal::read_block(&engine.block_store, 1, object_id) {
                Some(got) if &got == want => correct += 1,
                Some(_) => other_pages_bytes += 1,
                None => missing += 1,
            }
        }
        println!(
            "  {label:<28} {:>6} {:>18} {other_pages_bytes:>12} {missing:>8}",
            staged.len(),
            correct
        );
        results.push((label, correct, other_pages_bytes, missing));
    }

    let (_, today_correct, today_other, today_missing) = results[0];
    let (_, proposed_correct, proposed_other, proposed_missing) = results[1];

    // The control arm. Today's identity tells the two pages apart and both read back.
    assert_eq!(
        (today_correct, today_other, today_missing),
        (2, 0, 0),
        "CONTROL FAILED: today's identity served {today_correct} of 2 pages correctly \
         ({today_other} wrong bytes, {today_missing} missing). Until this arm is 2/0/0 the arm \
         below attributes nothing to the change"
    );

    // The refutation. One id, one reachable page -- and the OTHER page is served the first page's
    // bytes rather than answering missing, which is the difference between a fault and corruption.
    assert_eq!(
        (proposed_correct, proposed_other, proposed_missing),
        (1, 1, 0),
        "the component-free identity served {proposed_correct} of 2 pages correctly, \
         {proposed_other} as another page's bytes and {proposed_missing} as missing; the mechanism \
         predicts exactly one survivor and one WRONG-BYTES serve, because `read_block` picks a \
         page out of its record with `find(|page| page.object_id == object_id)` and `find` returns \
         the first rather than failing on an ambiguity it cannot see"
    );
    println!(
        "  VERDICT: one record, two pages of one key. Today 2/2 correct; component-free 1/2, and \
         the lost page is served the FIRST page's BYTES -- not missing. Silent corruption on the \
         path that exists to stop a durably acked write reading as missing."
    );
}

// =================================================================================================
// 4. WHY RE-KEYING THE REGISTRY DOES NOT REACH IT.
// =================================================================================================

/// A CARRIED PAGE CARRIES NO ORDINAL, SO THE REGISTRY'S KEY IS NOT WHERE THE PAGES ARE TOLD APART.
///
/// The proposal's second term is `BlockAddress::block_id`, and the registry key
/// `(store_id, ShardId, object_id)` can indeed take it. But the registry resolves a RECORD, and the
/// page is then chosen INSIDE that record by object id alone -- `StagedBlock` has exactly two
/// fields. So a fourth key term fixes the map and leaves the `find` exactly as wrong as it was.
///
/// Putting an ordinal on `StagedBlock` is a WAL WIRE change: `StagedBlock` is in `wal.rs`, carried
/// in every record, decoded by `decode_wal_line`. That is a different log from the served/logged
/// INDEX whose format break was authorised, and it would add a per-page term to the one structure
/// whose entire cost is that it carries the page twice.
///
/// **THE INPUT SIZE IS ASSERTED BEFORE ANY COUNT IS BELIEVED** -- a zero-byte read scores every
/// "there is exactly one" below as a pass.
///
/// rust-internal: reads the crate's own source, no external surface
#[test]
fn a_carried_page_carries_no_ordinal_so_a_fourth_key_term_cannot_reach_the_record() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let wal = std::fs::read_to_string(root.join("wal.rs")).expect("wal.rs is readable");
    let carried = std::fs::read_to_string(root.join("engine/block_in_wal.rs"))
        .expect("block_in_wal.rs is readable");
    assert!(
        wal.len() > 100_000 && carried.len() > 5_000,
        "DENOMINATOR: read {} and {} bytes of source; a short read scores every assertion below as \
         a pass",
        wal.len(),
        carried.len()
    );

    // The carried page's two fields, in the order the wire writes them.
    assert!(
        wal.contains("pub struct StagedBlock {"),
        "StagedBlock is no longer declared in wal.rs, so it is no longer the WAL wire structure \
         this argument is about"
    );
    let declaration = wal
        .split("pub struct StagedBlock {")
        .nth(1)
        .expect("the declaration follows its header")
        .split("\n}")
        .next()
        .expect("the declaration is brace-terminated");
    let fields: Vec<&str> = declaration
        .lines()
        .filter_map(|line| line.trim().strip_suffix(','))
        .filter(|line| !line.starts_with("//") && !line.starts_with("#["))
        .collect();
    assert_eq!(
        fields,
        vec!["pub object_id: u64", "pub bytes: Vec<u8>"],
        "StagedBlock's fields are {fields:?}. If an ordinal has been added, the WAL wire format has \
         moved and the argument in this module needs re-measuring rather than re-reading"
    );

    // And the page is chosen inside the record by that id and nothing else. Two sites: the decoded
    // record LRU and the freshly parsed record.
    let finds = carried
        .matches("find(|page| page.object_id == object_id)")
        .count();
    assert_eq!(
        finds, 2,
        "`read_block` picks a page out of its record at {finds} sites, not two -- the count that \
         makes the object id the in-record discriminator has moved"
    );
    println!(
        "\n=== why a fourth registry key term does not reach the page ===\n  \
         StagedBlock fields: {fields:?}\n  \
         in-record `find` on object_id alone: {finds} sites in block_in_wal.rs\n  \
         source read: {} + {} bytes",
        wal.len(),
        carried.len()
    );
}
