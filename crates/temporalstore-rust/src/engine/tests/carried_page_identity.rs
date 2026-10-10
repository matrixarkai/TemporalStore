// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CARRIED PAGE NOW STATES WHICH ELEMENT IT IS, AND THAT IS WHAT FREES THE OBJECT ID.
//!
//! # THE HOLE THIS CLOSES, MEASURED BEFORE IT WAS CLOSED
//!
//! `block_in_wal::read_block` serves a page whose only durable copy is its WAL record. It resolves
//! a record and then picks one page out of it. Until this change the only thing it could pick by
//! was the object id:
//!
//! ```text
//!     pages.iter().find(|page| page.object_id == object_id)
//! ```
//!
//! `StagedBlock` was `{ object_id, bytes }` and nothing else, so the object id was not merely the
//! registry's KEY -- it was the record's only discriminator BETWEEN ITS OWN PAGES. And one record
//! routinely carries many: `append_batch_as_one_record` puts a whole batch in one record, so a
//! batch writing twenty-five fields of one hash writes one record holding twenty-five pages.
//!
//! Measured on `ddecb8e10`, driven through the product's own read, before any line of this change:
//!
//! ```text
//!     today (kind:key:component)      2 / 2 correct, 0 wrong bytes, 0 missing
//!     component-free id               1 / 2 correct, 1 WRONG BYTES, 0 missing
//!     at batch scale                  96.00% of hash pages unreachable (200 pages, 8 groups)
//!     string control                  0.00%, 200 pages exercised
//! ```
//!
//! The losing page was served the FIRST page's BYTES rather than answering missing. `find` returns
//! the first match and cannot see an ambiguity, so a caller asking for one field of a hash was
//! handed a plausible page of a different field, on the one path that exists to stop a durably
//! acknowledged write reading as missing.
//!
//! # WHAT CHANGED
//!
//! `StagedBlock` gained `component: Option<Arc<str>>` -- which element of the object this page
//! holds -- and both in-record `find` sites now test it. The registry key gained it too. The
//! numbers above are the BEFORE; the tests here are the AFTER, on the same read path.
//!
//! # WHY THE COMPONENT AND NOT `BlockAddress::block_id`
//!
//! The ordinal was the other candidate, and #2008 threaded a real one onto container pages. FOUR
//! of the five reasons it loses are already measured elsewhere in this tree and are CITED rather
//! than re-derived here:
//!
//!   1. `container_page_ordinal` takes `component: &str` and resolves the ordinal BY looking the
//!      component up. The ordinal is a projection of the component, so it cannot carry more.
//!   2. #2008: "`max` FALLS after a delete and the next insert is handed the ordinal just freed...
//!      a reader that treated the ordinal as naming a particular element would be silently corrupt
//!      the moment it happened." Driven there by `the_ordinal_names_a_position_and_a_delete_frees_it`.
//!   3. #2008 again: past an object's 65,535th element the ordinal is left at 0, so those pages
//!      share it. Driven by `past_the_ceiling_the_ordinal_is_left_unassigned_rather_than_panicking`.
//!   4. #1996: the delta fold delivers elements whose durable-map entry was never written, and for
//!      those the component is the ONLY copy of the member.
//!
//! THE FIFTH IS NEW AND IS DRIVEN BELOW, because it is the one that decides the question for THIS
//! path rather than in general. The mandate asked whether an ordinal is available at STAGE time.
//! It is -- `block_ordinal` is an argument to `append_value_of_object`, computed by
//! `container_page_ordinal` immediately before the call. The stage is not where it fails.
//! **It fails at READ time**: the asynchronous arm of `append_value_inner` builds its address with
//! `BlockAddress::try_from_parts(HOT_BLOCK_SLAB_ID, ticket, len, None, object_id)` -- `block_id`
//! is `None` -- and that synthetic address is the one `is_wal_resident` sends down this path. So
//! on the very path a carried page exists to serve, there is no ordinal on the address to read
//! back. `a_wal_resident_address_carries_no_ordinal_to_read_back` measures it.
//!
//! # WHAT THIS DOES NOT BUY
//!
//! On its own, NOTHING. It makes records LARGER. It is worth landing only because it removes the
//! single obstacle to making `stable_block_object_id` component-free, and that change's own worth
//! is measured elsewhere. The growth is measured here so the trade is stated in both directions.

#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::engine::hashing::stable_block_object_id;
use crate::wal::StagedBlock;

const OPERATOR_END: u32 = 1023;
/// Eight keys and twenty-five fields: the same fixture the BEFORE numbers were taken on, so the
/// rows below are comparable to them line for line rather than merely similar.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population. A string page's component is already `None`, so every transformation
/// this module measures is the IDENTITY on it and the mechanism predicts no effect whatever.
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

/// The value a field is written with, distinct per (key, field) so that serving one field's page
/// for another is DETECTABLE rather than a coincidence of equal bytes.
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
/// EVERY LIVE PAGE OF A KIND, WITH THE ELEMENT IT HOLDS -- READ FROM THE AUTHORITY FOR THAT.
///
/// RE-ATTRIBUTED, AND THE OLD BODY IS WHY THIS COMMENT IS LONG. It returned the entry's own
/// element name until the width step removed that field, and was then left handing back a
/// hardcoded `None::<String>` under a note saying "an entry names a page, not an element -- this
/// module's subject". That made the two groupings in
/// `the_element_beside_the_id_makes_every_page_of_a_batch_reachable` the SAME computation: "id
/// alone" and "id + element" both grouped on `(id, None)`, so the arm's deliverable compared a
/// value with itself and reported the hazard row twice.
///
/// THE ELEMENT NAME IS STILL CARRIED -- JUST NOT THERE. `wal::StagedBlock` keeps a per-element
/// discriminator and says in its own doc why it has to ("the record's only discriminator BETWEEN
/// ITS OWN PAGES"), which is this module's whole subject. What moved is where a READER recovers it
/// from: `shard.hashes` is durable and is the authority for which field a hash page holds.
///
/// JOINED BY ADDRESS, WHICH IS THE JOIN THE ENGINE ITSELF MAKES. The resident map holds an
/// `ElementEntry` per field and the index entry holds the same address, so the address is what
/// identifies a page in both. `RecordedMap::page_an_element_vacates` asks this exact question on
/// the write path, so this is not a fixture-only correspondence.
fn live_pages(engine: &TemporalEngine, kind: &str) -> Vec<(String, Option<String>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    // Address -> element name, for the kinds that have elements. `string` has none, which is what
    // makes it this module's control.
    let mut element_at: BTreeMap<(u64, u64, u64), String> = BTreeMap::new();
    if kind == "hash" {
        for (object_key, fields) in shard.hashes.iter() {
            let _ = object_key;
            for (field, address) in fields.iter() {
                element_at.insert(
                    (
                        address.block_slab_id(),
                        address.offset(),
                        address.length(),
                    ),
                    field.to_string(),
                );
            }
        }
    }
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != kind {
                continue;
            }
            let at = (
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            );
            held.push((page.object_key.to_string(), element_at.get(&at).cloned()));
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

fn carrier(label: &str) -> Command {
    Command::StringSet {
        key: format!("carrier-{label}"),
        value: b"carrier".to_vec(),
    }
}

// =================================================================================================
// 1. THE DELIVERABLE: THE SAME READ PATH, A COMPONENT-FREE ID, AND BOTH PAGES BACK.
// =================================================================================================

/// TWO CARRIED PAGES OF ONE KEY, SHARING AN OBJECT ID, EACH SERVE THEIR OWN BYTES.
///
/// This is the test the change exists to pass. It does not simulate `read_block`; it calls it.
///
/// A record is appended carrying two pages with different bytes -- the shape a batch writing two
/// fields of one hash produces -- and registered exactly as `register_record` registers a real
/// append. **Both pages carry the SAME object id**, the one a component-free
/// `stable_block_object_id` would hand them. Before this change that arm read 1 of 2 correctly and
/// served the first page's bytes for the second. It now reads 2 of 2, because the record says
/// which element each page is and `read_block` asks for one.
///
/// THREE ARMS, and the third is what makes the first mean anything:
///
///   * TODAY'S IDENTITY, the control. Two distinct ids, both pages back. If this arm ever fails,
///     nothing below is attributable to anything.
///   * THE COMPONENT-FREE IDENTITY, the deliverable. One id, two components, both pages back.
///   * ONE ID AND ONE COMPONENT, the NEGATIVE control. Two pages that really are indistinguishable
///     still collapse to 1 of 2. Without it, a `read_block` that had simply started returning
///     both pages for any question would pass the second arm exactly as a correct one does.
///
/// THREE OUTCOMES, NOT TWO, and the split is the whole severity of the finding this closes. A page
/// that reads MISSING is a read fault: the caller falls through and the WAL replay still holds the
/// value. A page that reads ANOTHER PAGE'S BYTES is silent corruption. Folding them into one
/// "wrong" counter would let this test pass at 1/2 whichever of them the collapse produced.
///
/// rust-internal: drives the WAL append and the block-in-WAL read directly, no external surface
#[test]
fn two_carried_pages_of_one_key_each_serve_their_own_bytes_under_a_component_free_id() {
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
    println!("  identity                          pages  correct  wrong bytes  missing");

    // THE PRODUCT'S OWN IDENTITY, since the component left `stable_block_object_id`. It was
    // reachable only as a simulation when this module was written; it is now simply the id.
    let free = stable_block_object_id(1, "hash", "hk");
    // THE CONTROL'S TWO IDS. The folded identity used to produce a distinct id per element, and
    // there is no longer a call that does. What the control is FOR is unchanged -- two pages
    // bearing DIFFERENT ids must both come back, or nothing below attributes anything to the
    // change -- so the two distinct numbers come from two distinct keys instead.
    let distinct_a = stable_block_object_id(1, "hash", "hk:f0");
    let distinct_b = stable_block_object_id(1, "hash", "hk:f1");
    assert_ne!(
        distinct_a, distinct_b,
        "the control needs two DIFFERENT ids and got one number twice"
    );
    assert!(
        free != distinct_a && free != distinct_b,
        "the control's ids collide with the object's own id, so its arm would not be a control"
    );
    let mut results: Vec<(&str, usize, usize, usize)> = Vec::new();

    for (label, first, second) in [
        (
            "CONTROL: two distinct ids",
            (distinct_a, Some("f0")),
            (distinct_b, Some("f1")),
        ),
        (
            "component-free id + element",
            (free, Some("f0")),
            (free, Some("f1")),
        ),
        (
            "NEGATIVE: one id, one element",
            (free, Some("f0")),
            (free, Some("f0")),
        ),
    ] {
        let staged = vec![
            StagedBlock {
                object_id: first.0,
                component: first.1.map(std::sync::Arc::from),
                bytes: first_bytes.clone(),
            },
            StagedBlock {
                object_id: second.0,
                component: second.1.map(std::sync::Arc::from),
                bytes: second_bytes.clone(),
            },
        ];
        let (record, log_id) = engine
            .wal_store
            .append_with_sync_staged(1, carrier(label), true, staged.clone())
            .expect("the carrier record must append");
        crate::engine::block_in_wal::register_record(
            &engine.block_store,
            1,
            &staged,
            log_id,
            record.sequence,
            &engine.wal_store,
        );

        let mut correct = 0usize;
        let mut other_pages_bytes = 0usize;
        let mut missing = 0usize;
        for ((object_id, component), want) in
            [(first, &first_bytes), (second, &second_bytes)]
        {
            match crate::engine::block_in_wal::read_block(
                &engine.block_store,
                1,
                object_id,
                component,
            ) {
                Some(got) if &got == want => correct += 1,
                Some(_) => other_pages_bytes += 1,
                None => missing += 1,
            }
        }
        println!(
            "  {label:<32} {:>5} {:>8} {other_pages_bytes:>12} {missing:>8}",
            staged.len(),
            correct
        );
        results.push((label, correct, other_pages_bytes, missing));
    }

    assert_eq!(
        (results[0].1, results[0].2, results[0].3),
        (2, 0, 0),
        "CONTROL FAILED: two distinct ids served {} of 2 pages correctly ({} wrong bytes, {} \
         missing). Until this arm is 2/0/0 the arms below attribute nothing to the change",
        results[0].1,
        results[0].2,
        results[0].3
    );
    assert_eq!(
        (results[1].1, results[1].2, results[1].3),
        (2, 0, 0),
        "THE DELIVERABLE FAILED: a component-free id served {} of 2 pages correctly ({} wrong \
         bytes, {} missing). The record carries `component` on each page and `read_block` takes \
         one, so both pages must be reachable",
        results[1].1,
        results[1].2,
        results[1].3
    );
    assert_eq!(
        (results[2].1, results[2].2, results[2].3),
        (1, 1, 0),
        "NEGATIVE CONTROL FAILED: two pages sharing BOTH terms served {}/{}/{} rather than \
         1/1/0. They are genuinely indistinguishable, so the discriminator must not separate \
         them -- if it does, the second arm above is passing for some other reason",
        results[2].1,
        results[2].2,
        results[2].3
    );
    println!(
        "  VERDICT: 2/2 under a component-free id (was 1/2 with the first page's bytes served for \
         the second), while two genuinely identical pages still collapse to 1/2."
    );
}

// =================================================================================================
// 2. THE SAME QUESTION AT BATCH SCALE, WITH ITS DENOMINATOR AND ITS CONTROL.
// =================================================================================================

/// AT BATCH SCALE: 96.00% UNREACHABLE ON THE ID ALONE, 0.00% ONCE THE ELEMENT IS BESIDE IT.
///
/// Every live page is grouped twice: by the identity a component-free `stable_block_object_id`
/// would give it, and by the PAGE KEY this change made the record and the registry use, which is
/// that identity plus the element. A group of size N > 1 is N pages the lookup cannot tell apart,
/// of which at most one is reachable.
///
/// THE FIRST GROUPING IS NOT A BUG BEING REPORTED -- it is the hazard the second grouping removes,
/// and it is here so the two are read off ONE population rather than two runs. This module's own
/// BEFORE numbers put it at 96.00%, and it reproduces here unchanged, because this change does not
/// touch the index: it changes what the RECORD can say about its own pages.
///
/// THE CONTROL IS THE STRING ARM and it is why the hash arm means anything. A string page's
/// component is already `None`, so adding the element to the key is the IDENTITY on it: the
/// mechanism predicts every group is size one under BOTH groupings and no page is ever lost. Its
/// exercised page count is asserted first, because a control that read nothing reports 0.00% lost
/// and "not exercised" wears the face of "did not move".
///
/// rust-internal: reads a seeded store's own page entries, no external surface
#[test]
fn the_element_beside_the_id_makes_every_page_of_a_batch_reachable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    println!("\n=== pages per identity, grouped two ways ===");
    println!("  kind    grouped by        pages  groups  p50  p90  p99  MAX  unreachable   share");
    let mut rows: Vec<(&str, &str, usize, usize)> = Vec::new();
    for kind in ["hash", "string"] {
        let pages = live_pages(&engine, kind);
        assert!(
            !pages.is_empty(),
            "DENOMINATOR: no {kind} pages were exercised, so every share below is 0.00% for the \
             wrong reason"
        );
        for grouping in ["id alone", "id + element"] {
            let mut groups: BTreeMap<(u64, Option<String>), usize> = BTreeMap::new();
            for (object_key, component) in &pages {
                let id = stable_block_object_id(1, kind, object_key);
                let term = if grouping == "id alone" {
                    None
                } else {
                    component.clone()
                };
                *groups.entry((id, term)).or_default() += 1;
            }
            let mut sizes: Vec<usize> = groups.values().copied().collect();
            sizes.sort();
            let unreachable: usize = sizes.iter().map(|size| size.saturating_sub(1)).sum();
            let share = 100.0 * unreachable as f64 / pages.len() as f64;
            println!(
                "  {kind:<6}  {grouping:<16} {:>5} {:>7} {:>4} {:>4} {:>4} {:>4} {:>12} {share:>6.2}%",
                pages.len(),
                sizes.len(),
                percentile(&sizes, 50.0),
                percentile(&sizes, 90.0),
                percentile(&sizes, 99.0),
                sizes.last().copied().unwrap_or(0),
                unreachable
            );
            rows.push((kind, grouping, pages.len(), unreachable));
        }
    }

    let find = |kind: &str, grouping: &str| -> (usize, usize) {
        let row = rows
            .iter()
            .find(|(k, g, _, _)| *k == kind && *g == grouping)
            .expect("both groupings ran for both kinds");
        (row.2, row.3)
    };

    // The hazard, unchanged by this PR and reproduced from the BEFORE run.
    let (hash_pages, hash_id_only) = find("hash", "id alone");
    assert_eq!(
        hash_pages,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the hash arm exercised {hash_pages} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        hash_id_only,
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1),
        "grouped on the id alone the hash arm loses {hash_id_only} pages, not {} -- one survivor \
         per key",
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1)
    );

    // The deliverable.
    let (_, hash_with_element) = find("hash", "id + element");
    assert_eq!(
        hash_with_element, 0,
        "with the element beside the id, {hash_with_element} of {hash_pages} hash pages are still \
         unreachable. Every page of this fixture is a distinct field of a hash, so every group \
         must hold exactly one page"
    );

    // THE CONTROL, exercised first and then asserted flat under both groupings.
    let (string_pages, string_id_only) = find("string", "id alone");
    let (_, string_with_element) = find("string", "id + element");
    assert_eq!(
        string_pages, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_pages} string pages, not {ROUTED_KEYS}"
    );
    assert_eq!(
        (string_id_only, string_with_element),
        (0, 0),
        "the control lost ({string_id_only}, {string_with_element}) of {string_pages} pages under \
         the two groupings. A kind whose component is already `None` cannot be moved by putting \
         the component into the key, so both must be zero"
    );
    println!(
        "  CONTROL: string, {string_pages} pages exercised, 0 unreachable under BOTH groupings, \
         0.00%.\n  VERDICT: 96.00% -> 0.00% unreachable for hash; the control does not move."
    );
}

// =================================================================================================
// 3. THE REASON THE ORDINAL LOSES THAT IS NOT ALREADY IN THE TREE.
// =================================================================================================

/// A WAL-RESIDENT ADDRESS CARRIES NO ORDINAL, SO THE READ COULD NOT HAVE USED ONE.
///
/// The mandate asked whether a page ordinal is available at STAGE time. IT IS, and saying
/// otherwise would have been wrong: `append_value_of_object` takes `block_ordinal` as an argument
/// and `container_page_ordinal` computes it immediately before the call. The stage is not where an
/// ordinal fails.
///
/// IT FAILS AT READ TIME. `read_block` is reached from `read_block_bytes`, whose handle is a
/// `BlockAddress`. For a page served out of its record that address is the SYNTHETIC one the
/// asynchronous arm minted, and that arm passes `None` for `block_id`. So the reader has no
/// ordinal to pass even when the writer had one.
///
/// Driven on real writes through the product, not on a hand-built address: the store is opened
/// with asynchronous storage so the pages stay log-resident, the addresses are read off the served
/// index, and every one is asserted to be WAL-resident first -- an arm that measured only durable
/// addresses would report "no ordinals" for the wrong reason.
///
/// rust-internal: reads the engine's own index entries, no external surface
#[test]
fn a_wal_resident_address_carries_no_ordinal_to_read_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    // Asynchronous storage is a per-REQUEST replication mode here, and it is what makes a
    // page stay in its record: the write never hands the page to the block store, so its
    // address is the synthetic one this test is about.
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            let response = engine.execute_replicated(
                ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: hash_value(k, f),
                    },
                }
                .with_async_storage(),
            );
            assert!(response.status.ok, "seed must ack: {:?}", response.status);
        }
    }

    let (resident, with_ordinal, durable) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let mut resident = 0usize;
        let mut with_ordinal = 0usize;
        let mut durable = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if page.deleted || page.model_id.as_str() != "hash" {
                    continue;
                }
                if crate::wal_record::is_wal_resident(page.address.block_slab_id()) {
                    resident += 1;
                    if page.address.block_id().is_some() {
                        with_ordinal += 1;
                    }
                } else {
                    durable += 1;
                }
            }
        }
        (resident, with_ordinal, durable)
    };

    println!("\n=== can the READ side see a page ordinal? ===");
    println!("  hash pages, WAL-resident            {resident}");
    println!("  ...of which carry an ordinal        {with_ordinal}");
    println!("  hash pages with a durable address   {durable}");

    assert!(
        resident > 0,
        "NOT EXERCISED: {resident} WAL-resident hash pages, so \"none carry an ordinal\" would be \
         true of an empty set. {durable} pages took the durable arm instead"
    );
    assert_eq!(
        with_ordinal, 0,
        "{with_ordinal} of {resident} WAL-resident addresses carry an ordinal. If that is no \
         longer zero, the asynchronous arm of `append_value_inner` has stopped passing `None` for \
         `block_id` and the argument in this module's header needs re-measuring rather than \
         re-reading"
    );
    println!(
        "  VERDICT: {with_ordinal}/{resident}. The writer HAS an ordinal at stage time; the reader \
         has none at read time, on exactly the path a carried page is served from."
    );
}

// =================================================================================================
// 4. WHAT THE RECORD GREW.
// =================================================================================================

/// BYTES PER STAGED PAGE, ON THE WIRE AND ON DISK, AT TWO CORPUS SIZES.
///
/// TWO FIGURES, BECAUSE THEY ARE NOT THE SAME NUMBER AND ONLY ONE OF THEM IS PER PAGE.
///
///   * THE WIRE COST is what the field occupies in the protobuf body of one staged page, read
///     from the writer's own length function rather than from a second copy of the rule -- the
///     same function that RESERVES the buffer the writer fills, so a figure here that disagreed
///     with the writer would be a record written into the wrong number of bytes.
///   * THE DISK COST is what the framed, zstd-compressed record actually grows by. It is
///     SUBLINEAR in the page count and gets cheaper the more pages a record carries, because the
///     element name repeats and that is exactly what a compressor removes. Reporting only the
///     wire cost would overstate what this change costs a real log; reporting only the disk cost
///     would hide that the field is per page.
///
/// `ALLOC_CHUNK_BYTES` is a FLOOR above the request rather than an equality (#1969 measured a
/// request of 104 landing in 128), so the chunk is reported beside the request and never instead
/// of it.
///
/// THE CONTROL IS THE COMPONENT-LESS PAGE. A whole-object page's component is `None`, the writer
/// skips the field entirely, and the record is byte-identical to one written before the field
/// existed. It must report +0.00% on BOTH figures -- and its exercised bytes are asserted non-zero
/// first, because a control that encoded nothing reports +0.00% for the wrong reason.
///
/// rust-internal: encodes records in memory, no store and no external surface
#[test]
fn what_an_element_named_page_adds_to_the_record() {
    use crate::wal::{encode_wal_line_for_test, WriteAheadLogRecord};

    fn page_of(i: usize, component: Option<&str>, payload: usize) -> StagedBlock {
        StagedBlock {
            object_id: stable_block_object_id(1, "hash", "hk"),
            component: component.map(std::sync::Arc::from),
            bytes: vec![b'v'; payload]
                .into_iter()
                .chain(format!("{i:04}").into_bytes())
                .collect(),
        }
    }

    fn record_of(pages: usize, component: Option<&str>, payload: usize) -> WriteAheadLogRecord {
        WriteAheadLogRecord {
            shard_id: 1,
            sequence: 7,
            command: Some(Command::StringGet {
                key: "carrier".to_string(),
            }),
            metadata: None,
            staged_blocks: (0..pages).map(|i| page_of(i, component, payload)).collect(),
            outcomes: Vec::new(),
        }
    }

    fn disk_len(record: &WriteAheadLogRecord) -> usize {
        encode_wal_line_for_test(record)
            .expect("the record encodes")
            .len()
    }

    // The writer's own measurement, for ONE page. `implied_object_id` is `None` here because a
    // record carrying many pages never elides an object id -- which is precisely the record shape
    // this whole module is about.
    fn wire_len(component: Option<&str>, payload: usize) -> usize {
        crate::raft::wal_proto::staged_block_body_len(&page_of(0, component, payload), None)
    }

    println!("\n=== what the element identity adds to a carried page ===");
    println!("  pages  element        wire B/page  chunk B   disk bare  disk named  disk B/page   share");
    let mut wire_rows: Vec<(&str, i64)> = Vec::new();
    let mut disk_rows: Vec<(usize, &str, f64)> = Vec::new();
    for pages in [25usize, 200usize] {
        for (label, component) in [("hash field f7", Some("f7")), ("CONTROL: none", None)] {
            let wire = wire_len(component, 32) as i64 - wire_len(None, 32) as i64;
            let bare = disk_len(&record_of(pages, None, 32));
            let named = disk_len(&record_of(pages, component, 32));
            let delta = named as i64 - bare as i64;
            let per_page = delta as f64 / pages as f64;
            let chunk = crate::alloc_probe::documented_glibc_chunk(wire.unsigned_abs() as usize);
            let share = 100.0 * delta as f64 / bare as f64;
            println!(
                "  {pages:>5}  {label:<13} {wire:>11} {chunk:>8} {bare:>11} {named:>11} \
                 {per_page:>11.2} {share:>6.2}%"
            );
            assert!(
                bare > 0 && named > 0,
                "NOT EXERCISED: encoded {bare} and {named} bytes at {pages} pages; a zero-byte \
                 encode reports every delta below as +0.00%"
            );
            wire_rows.push((label, wire));
            disk_rows.push((pages, label, per_page));
        }
    }

    // THE CONTROL, on both figures. A component-less page cannot be moved by adding a component
    // field to the page, because the writer never emits the field for it.
    for (label, wire) in &wire_rows {
        if label.starts_with("CONTROL") {
            assert_eq!(
                *wire, 0,
                "the control's WIRE cost moved by {wire} B/page. A page whose component is `None` \
                 skips the field entirely"
            );
        }
    }
    for (pages, label, per_page) in &disk_rows {
        if label.starts_with("CONTROL") {
            assert_eq!(
                *per_page, 0.0,
                "the control's DISK cost moved by {per_page:.2} B/page at {pages} pages. A record \
                 carrying no components must be byte-identical to one written before this field"
            );
        }
    }

    // The WIRE cost is per page, so it cannot depend on how many pages the record holds. The disk
    // cost deliberately can, and the two assertions below are what keep the two apart.
    let named_wire: Vec<i64> = wire_rows
        .iter()
        .filter(|(l, _)| l.starts_with("hash"))
        .map(|(_, w)| *w)
        .collect();
    assert_eq!(
        named_wire[0], named_wire[1],
        "the element field costs {} wire bytes per page at one corpus size and {} at the other. \
         It is a per-page field",
        named_wire[0], named_wire[1]
    );
    assert!(
        named_wire[0] > 0,
        "the element field reported {} wire bytes per page. This change makes records BIGGER; a \
         non-positive figure means the encoder is not writing the field at all",
        named_wire[0]
    );

    let small = disk_rows
        .iter()
        .find(|(p, l, _)| *p == 25 && l.starts_with("hash"))
        .expect("small row")
        .2;
    let large = disk_rows
        .iter()
        .find(|(p, l, _)| *p == 200 && l.starts_with("hash"))
        .expect("large row")
        .2;
    assert!(
        small > 0.0 && large > 0.0,
        "the disk cost came back non-positive ({small:.2}, {large:.2}); the field must cost \
         SOMETHING on disk or it is not being written"
    );
    assert!(
        large < small,
        "the disk cost was {small:.2} B/page at 25 pages and {large:.2} at 200. The element name \
         repeats across a record's pages and a compressor removes repetition, so the larger \
         record must pay LESS per page -- if it does not, the payload is no longer compressing \
         and this figure is measuring something else"
    );
    assert!(
        large < named_wire[0] as f64,
        "the disk cost per page ({large:.2}) is not below the wire cost ({}), so the record is \
         not being compressed and the two figures are measuring the same thing",
        named_wire[0]
    );
    println!(
        "  VERDICT: a two-character element name costs {} bytes on the WIRE per carried page, \
         identical at both corpus sizes, and {small:.2} -> {large:.2} B/page ON DISK as the record \
         grows from 25 pages to 200. The control is unchanged on both.",
        named_wire[0]
    );
}

/// THE ALLOCATIONS, which a byte count cannot see. The log is on the write path.
///
/// `stage` is charged to `CarriedPage`, and this change adds the component's own copy to that same
/// class deliberately -- it is a cost of carrying the page, and charging it elsewhere would let it
/// disappear from the class that exists to measure exactly this.
///
/// The control is the same staging with no component: it must add no allocation at all, because
/// `None.map(Arc::from)` allocates nothing.
///
/// rust-internal: stages pages into the thread-local buffer, no store and no external surface
#[cfg(feature = "alloc-probe")]
#[test]
fn what_an_element_named_page_allocates_when_it_is_staged() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    const PAGES: usize = 200;

    let measure = |component: Option<&str>| -> (u64, u64) {
        crate::engine::block_in_wal::begin_write();
        let span = ClassSpan::open();
        for _ in 0..PAGES {
            crate::engine::block_in_wal::stage(7, component, &[b'v'; 32]);
        }
        let counts = span
            .close()
            .expect("built with `alloc-probe`, so the ledger is a measurement");
        let row = counts.classes.row(AllocClass::CarriedPage);
        let _ = crate::engine::block_in_wal::take_staged();
        (row.allocs, row.alloc_bytes)
    };

    let (bare_allocs, bare_bytes) = measure(None);
    let (named_allocs, named_bytes) = measure(Some("f7"));

    println!("\n=== carried_page allocations for {PAGES} staged pages ===");
    println!("  element          allocs   bytes   allocs/page   B/page");
    for (label, allocs, bytes) in [
        ("CONTROL: none", bare_allocs, bare_bytes),
        ("hash field f7", named_allocs, named_bytes),
    ] {
        println!(
            "  {label:<15} {allocs:>7} {bytes:>7} {:>13.2} {:>8.2}",
            allocs as f64 / PAGES as f64,
            bytes as f64 / PAGES as f64
        );
    }

    assert!(
        bare_allocs >= PAGES as u64,
        "NOT EXERCISED: {bare_allocs} allocations for {PAGES} staged pages; the page copy alone is \
         one each, so a smaller figure means the staging did not run"
    );
    assert!(
        named_allocs > bare_allocs,
        "naming the element added {} allocations over {PAGES} pages. An `Arc<str>` from a `&str` \
         copies the characters, so it cannot be free -- a zero here means the field is not being \
         filled",
        named_allocs as i64 - bare_allocs as i64
    );
    println!(
        "  VERDICT: +{:.2} allocations and +{:.2} bytes per carried page for a two-character \
         element name.",
        (named_allocs - bare_allocs) as f64 / PAGES as f64,
        (named_bytes - bare_bytes) as f64 / PAGES as f64
    );
}

// =================================================================================================
// 5. PAGES PER RECORD -- THE REASON ANY OF THIS MATTERS.
// =================================================================================================

/// HOW MANY PAGES ONE RECORD CARRIES. HISTOGRAM, NEVER A MEAN.
///
/// A record holding one page has no ambiguity to resolve, so the whole finding rests on records
/// holding several. `append_batch_as_one_record` is the engine's default path for a batch that
/// produced outcomes, so the many-page record is the NORMAL shape for exactly the container writes
/// this concerns rather than a corner case.
///
/// Read off the written log rather than predicted from the batch size: a record is what the log
/// says it is.
///
/// rust-internal: scans the shard's own log, no external surface
#[test]
fn how_many_pages_one_record_carries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    let mut sizes: Vec<usize> = Vec::new();
    let mut carried_pages = 0usize;
    let (records, truncated) = engine
        .wal_store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the log scans");
    assert!(
        !truncated,
        "the scan stopped early, so the histogram below is of a PREFIX of the log rather \
         than of it"
    );
    for (_log_id, record) in &records {
        if !record.staged_blocks.is_empty() {
            carried_pages += record.staged_blocks.len();
            sizes.push(record.staged_blocks.len());
        }
    }
    sizes.sort();

    println!("\n=== pages per record, over the whole seeded log ===");
    println!("  records carrying pages  pages  p50  p90  p99  MAX  records with >1 page");
    let multi = sizes.iter().filter(|size| **size > 1).count();
    assert!(
        !sizes.is_empty(),
        "DENOMINATOR: no record in this log carries a page, so every figure below is vacuous"
    );
    println!(
        "  {:>22} {carried_pages:>6} {:>4} {:>4} {:>4} {:>4} {multi:>21}",
        sizes.len(),
        percentile(&sizes, 50.0),
        percentile(&sizes, 90.0),
        percentile(&sizes, 99.0),
        sizes.last().copied().unwrap_or(0),
    );
    assert!(
        multi > 0,
        "every one of {} records carries exactly one page, so no record in this fixture has two \
         pages to tell apart and the finding this module is about cannot arise in it",
        sizes.len()
    );
    assert!(
        sizes.last().copied().unwrap_or(0) > 1,
        "the largest record in this log carries {} page(s)",
        sizes.last().copied().unwrap_or(0)
    );
    println!(
        "  VERDICT: {multi} of {} page-carrying records hold more than one page, MAX {}.",
        sizes.len(),
        sizes.last().copied().unwrap_or(0)
    );
}

// =================================================================================================
// 6. THE GUARANTEE THE RECORD MAKES IS NOT WEAKENED.
// =================================================================================================

/// THE RECORD STILL CARRIES EVERY PAGE, AND EVERY PAGE STILL ROUND-TRIPS THROUGH THE WIRE.
///
/// This is the refusal condition the mandate named: if the record could not carry an element
/// identity without weakening what a carried record guarantees, the change does not ship. The
/// guarantee is that a record may state its results and drop the operation only when the blocks
/// those results name survive a crash -- and a carried block survives because it IS the record.
/// This change ADDS to what the record says; the test below is what says so rather than asserting
/// it in prose.
///
/// Driven through the real encoder, both arms of it: the binary payload the engine writes today,
/// and the JSON payload a record written before the binary encoder still uses. A field that
/// round-tripped through one and not the other would be a record that decodes differently
/// depending on when it was written, which is the same class of fault as not carrying it at all.
///
/// rust-internal: encodes and decodes records in memory, no external surface
#[test]
fn every_carried_page_keeps_its_bytes_and_gains_an_element_through_both_encoders() {
    use crate::wal::{decode_wal_line, encode_wal_line_for_test, WriteAheadLogRecord};

    // Deliberately mixed: named elements, a whole-object page, and an EMPTY element name. The
    // empty one is not decoration -- `put_len_delimited` skips an empty payload, so an encoder
    // that reused it here would turn `Some("")` into `None` and make an element page read as its
    // key's whole-object page.
    let pages: Vec<StagedBlock> = [
        (Some("f0"), b"zero".to_vec()),
        (Some("f1"), b"one".to_vec()),
        (None, b"whole object".to_vec()),
        (Some(""), b"empty element name".to_vec()),
    ]
    .into_iter()
    .map(|(component, bytes)| StagedBlock {
        object_id: stable_block_object_id(1, "hash", "hk"),
        component: component.map(std::sync::Arc::from),
        bytes,
    })
    .collect();

    let record = WriteAheadLogRecord {
        shard_id: 1,
        sequence: 11,
        command: Some(Command::StringGet {
            key: "carrier".to_string(),
        }),
        metadata: None,
        staged_blocks: pages.clone(),
        outcomes: Vec::new(),
    };

    let encoded = encode_wal_line_for_test(&record).expect("the record encodes");
    assert!(
        encoded.len() > 40,
        "NOT EXERCISED: the record encoded to {} bytes; a short encode would make every \
         round-trip assertion below trivially true",
        encoded.len()
    );
    let decoded = decode_wal_line(&encoded).expect("the record decodes");

    println!("\n=== every carried page round-trips, bytes AND element ===");
    println!("  page  element        bytes in  bytes out  element out");
    assert_eq!(
        decoded.staged_blocks.len(),
        pages.len(),
        "the record carried {} pages in and {} out. This change may only ADD to what a record \
         says; a page that does not come back is the refusal condition",
        pages.len(),
        decoded.staged_blocks.len()
    );
    for (i, (before, after)) in pages.iter().zip(decoded.staged_blocks.iter()).enumerate() {
        println!(
            "  {i:>4}  {:<13} {:>9} {:>10}  {:?}",
            format!("{:?}", before.component.as_deref()),
            before.bytes.len(),
            after.bytes.len(),
            after.component.as_deref()
        );
        assert_eq!(
            before.bytes, after.bytes,
            "page {i}'s BYTES did not survive the round trip. That is the guarantee this record \
             exists to make and nothing about naming its element may touch it"
        );
        assert_eq!(
            before.component, after.component,
            "page {i}'s element came back as {:?} rather than {:?}. `Some(\"\")` and `None` are \
             different pages, and an encoder that merges them re-creates the ambiguity this \
             change removes",
            after.component.as_deref(),
            before.component.as_deref()
        );
        assert_eq!(
            before.object_id, after.object_id,
            "page {i}'s object id did not survive the round trip"
        );
    }
    println!("  VERDICT: {} pages in, {} out, bytes and elements identical. Nothing the record guaranteed was traded away.", pages.len(), decoded.staged_blocks.len());
}

// =================================================================================================
// 7. CARRIED OVER FROM #2011, UNCHANGED, BECAUSE IT STILL HOLDS.
// =================================================================================================
//
// #2011 -- the refutation this PR answers -- established that a hash page's FIELD NAME is
// independently recoverable, which is what made a component-free identity worth pursuing at all.
// Nothing here changes that, so the measurement is carried over verbatim rather than rewritten: it
// asks a different question from every test above, and re-deriving a figure that still holds would
// only risk changing what it measures.
//
// Its three companions in that module are superseded above and are NOT carried over, each for a
// reason the module itself gave:
//
//   * `a_component_free_identity_collapses_every_page_of_one_key_onto_one_number` measured one
//     grouping; `the_element_beside_the_id_makes_every_page_of_a_batch_reachable` measures that
//     grouping AND the one this change introduces, off ONE population, and keeps its 96.00% row.
//   * `two_carried_pages_sharing_an_object_id_serve_one_anothers_bytes` asserted 1/2. That is now
//     2/2 for two pages of DIFFERENT elements, and its 1/2 survives as the NEGATIVE CONTROL arm of
//     the first test above, where two pages sharing BOTH terms still collapse.
//   * `a_carried_page_carries_no_ordinal_so_a_fourth_key_term_cannot_reach_the_record` asserted
//     `StagedBlock` has exactly two fields, and said in its own words: "If an ordinal has been
//     added, the WAL wire format has moved and the argument in this module needs RE-MEASURING
//     rather than re-reading." It has moved, and `a_wal_resident_address_carries_no_ordinal_to_read_back`
//     is that re-measurement -- on the read side, which is where the ordinal actually fails.

/// A HASH PAGE'S FIELD NAME IS HELD BESIDE THE ID, NOT INSIDE IT, AND SURVIVES A RELOAD.
///
/// THE SOURCE MOVED AND THE CLAIM DID NOT. This read the name off the page entry:
/// "`BlockIndex::component: Option<Arc<str>>` is its own `#[serde]` field, written to the index and
/// read back from it. `bucket_index_component_block_addresses(shard, model_id, object_key)` ... takes
/// NO component and returns `(page.component.clone(), page.address.clone())` off the entry." That
/// field is gone and the walk recovered 0 of 25 names.
///
/// The name is recovered from `shard.hashes` instead, and the claim is the same one: it is held
/// BESIDE the id rather than inside it, and it survives an unload/load. The map is durable --
/// `#[serde(default)]` on `ShardState`, with `sets`, `zsets` and `lists` beside it -- so this is
/// still a measurement across the DISK copy and not a reading of process state: the engine is
/// unloaded and loaded, and what comes back comes back from the index snapshot on disk.
///
/// THE SECOND ARM IS DELETED AS A TAUTOLOGY, AND THAT IS WORTH MORE THAN FIXING IT. It read:
///
///     let free_ids: BTreeSet<u64> =
///         pairs.iter().map(|_| stable_block_object_id(1, "hash", &key)).collect();
///     assert_eq!(free_ids.len(), 1, ...);
///
/// The closure IGNORES its item and returns the same value for every page, so the set has one
/// element for any non-empty input and the assertion could only ever fail on an EMPTY one. It was a
/// real measurement when the id was computed from the element name -- then two pages of one key
/// genuinely produced two ids, and collapsing them to one was the thing being shown.
/// `stable_block_object_id` takes no element at all now, so "every page of this key carries one id"
/// is a property of its SIGNATURE and not something a fixture can observe. The only content in that
/// assertion was its emptiness check, which is kept as an explicit page-count floor with its own
/// message.
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
    println!("  key        pages  names recovered");
    let mut total_recovered = 0usize;
    for k in 0..CONTAINER_KEYS {
        let key = format!("h{k}");
        let (pages, recovered) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            let pages = shard
                .bucket_index
                .bucket_map
                .values()
                .flat_map(|bucket| bucket.block_index.values())
                .filter(|page| {
                    !page.deleted && page.model_id.as_str() == "hash" && &*page.object_key == key
                })
                .count();
            let recovered: BTreeSet<String> = shard
                .hashes
                .get(&key)
                .map(|fields| fields.keys().map(|name| name.to_string()).collect())
                .unwrap_or_default();
            (pages, recovered)
        };
        println!("  {key:<10} {pages:>5}  {:>15}", recovered.len());
        assert!(
            pages > 0,
            "DENOMINATOR: key {key} holds no live page entries after the reload, so a full set of \
             recovered names below would be saying nothing about pages"
        );
        assert_eq!(
            recovered, written,
            "key {key} recovered {} of {MEMBERS_PER_KEY} field names after a reload -- the field \
             name is NOT independently recoverable and the change is sunk here",
            recovered.len()
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
        "  VERDICT: {total_recovered}/{} field names recovered across an unload/load cycle, from \
         the durable resident map rather than from a page entry. The field name SURVIVES.",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
}
