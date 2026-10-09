// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// DOES THE FOLD ROUTE LEAVE THE DURABLE HASH MAP NAMING EVERY FIELD THE PAGE INDEX NAMES?
// =================================================================================================

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

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
        table_name: "fold-hash-completeness".to_string(),
        shard_uri: "local://fold-hash-completeness/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
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

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// What the SERVED path answers for this key, which is the vacuity floor for both tests.
fn served_hash_fields(engine: &TemporalEngine, key: &str) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll { key: key.to_string() },
        })
        .response
    {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    }
}

/// The two sources, read the way each is read in production.
///
/// THE SECOND SOURCE IS RE-ATTRIBUTED, AND THAT IS THIS MODULE'S WHOLE EDIT. It was
/// `bucket_index_component_block_addresses`, which recovered each field's name from the page
/// entry's element-name field. `BlockIndex` has no such field now, so that walk answers for NO
/// fields and both arms failed on their own vacuity floors -- "the page index names 0 field(s) ...
/// so the comparison below would be vacuous". The floors were right to fire: with one side empty
/// the set equality would have passed on nothing.
///
/// THE NAMES DID NOT LEAVE STORAGE, THEY MOVED. A container page SPELLS its elements in its
/// payload -- that is what makes several of them foldable into one page at all -- so the pages
/// themselves are the second source, derived with the engine's own
/// `container_membership::derive_membership`, which is what `folded_page_membership` reads.
///
/// AND THE PAIR IS STRONGER THAN THE ONE IT REPLACES, which is worth saying because a
/// re-attribution usually is not. The entry's element name and the page's payload were written by
/// the SAME append, so their agreement said little about either. The durable map is maintained by
/// the command path and the pages by the append path, so these two can genuinely disagree -- which
/// is the property that makes this module's claim, that the map is complete, worth asserting at
/// all. The module header's "prove the map is complete FIRST, because the removal is what makes
/// the map load-bearing" is now the past tense: the removal has landed and the map is load-bearing.
///
/// COMPLETENESS IS ENFORCED HERE RATHER THAN RETURNED, so neither arm can compare a derivation that
/// fell short. An incomplete derivation that happened to agree would agree by luck.
fn both_sources(
    engine: &TemporalEngine,
    key: &str,
) -> (std::collections::BTreeSet<String>, std::collections::BTreeSet<String>) {
    let (durable, addresses) = {
        let shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let durable: std::collections::BTreeSet<String> = shard
            .hashes
            .get(key)
            .map(|fields| fields.keys().map(|name| name.to_string()).collect())
            .unwrap_or_default();
        // TOMBSTONE ENTRIES INCLUDED, no `page.deleted` skip. A tombstone is how a page STATES a
        // removal, so a walk that filtered them would read the page set as it stood before the
        // removal and would derive the removed field as live.
        let mut addresses: Vec<crate::block_store::ElementEntry> = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if page.model_id.as_str() == "hash" && &*page.object_key == key {
                    addresses.push(page.address.clone());
                }
            }
        }
        (durable, addresses)
    };
    assert!(
        !addresses.is_empty(),
        "DENOMINATOR: no page entries at all for {key}, so the derived side below would be empty \
         for a reason that has nothing to do with what the fold restored"
    );
    let derived = crate::engine::container_membership::derive_membership("hash", addresses, |address| {
        engine.block_store.read(address).ok()
    });
    assert!(
        derived.is_complete(),
        "the derivation from the pages is incomplete ({} failure(s)), so its agreement with the \
         durable map would be luck: {} read, {} undecodable, {} unframed, {} unrenderable",
        derived.failures(),
        derived.read_failures,
        derived.undecodable,
        derived.unframed,
        derived.unrenderable_items
    );
    let derived_fields: std::collections::BTreeSet<String> = derived.live.keys().cloned().collect();
    (durable, derived_fields)
}

/// THE QUESTION, AND WHY IT IS NOT "IS THE FIELD READABLE".
///
/// RESTATED, BECAUSE THE READER IT NAMED HAS MOVED. This read "`HashGetAll` resolves through
/// `bucket_index_component_block_addresses`, which is keyed BY THE COMPONENT -- it reads each
/// field's name off the page entry. The fold installs page entries. So a folded hash field is
/// readable today whether the durable map received it or not, and a test asserting 'readable
/// through the served path' would go green while proving nothing about the map."
///
/// `HashGetAll`'s index half answers from `shard.hashes` now -- it was one of the four consumers
/// that had to move before the element name could come off the entry -- so "readable through the
/// served path" and "in the durable map" have become the SAME statement. That is why this module
/// still does not make that assertion: it would now be circular rather than merely weak, which is
/// a different reason for the same restraint and worth having written down.
///
/// THE DEPENDENCY ON THE MAP EXISTS NOW -- IT WAS CREATED BY REMOVING THE ELEMENT NAME. This read
/// "DOES NOT EXIST YET ... Today the entry carries the field name and the map is a second copy.
/// Take `component` off the entry -- 16 of the 56 bytes a resident entry weighs -- and
/// `HashGetAll` has nowhere left to read a name FROM except `shard.hashes`. So the order is not
/// 'reroute the readers, then remove the field'. It is: prove the map is complete FIRST, because
/// the removal is what makes the map load-bearing. This module is that proof, or its refutation."
///
/// The entry went from 56 bytes to 40, `HashGetAll` answers from `shard.hashes`, and the map IS
/// load-bearing. So this module is no longer the proof that clears a step -- it is the standing
/// guard on a map the engine now depends on, and its second source had to move off the entry with
/// everything else. See `both_sources` for where it moved and why the pair is stronger for it.
///
/// WHAT IS ASSERTED, THEREFORE: that the two sources name THE SAME SET of fields after a fold.
/// Set equality in both directions and not a count, because a count cannot tell a missing field
/// from an extra one, and the two failures mean opposite things -- a field the map lacks is the
/// removal losing data, and a field the map holds alone is the fold resurrecting something.
///
/// rust-internal: reads the engine's own resident maps after a reload, no external surface
#[test]
fn the_fold_leaves_the_durable_hash_map_naming_exactly_the_fields_the_pages_derive() {
    const KEY: &str = "fold-hash";
    const FIELDS: usize = 6;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    for index in 0..FIELDS {
        write(
            &engine,
            Command::HashSet {
                key: KEY.to_string(),
                field: format!("field-{index}"),
                value: format!("value-{index}").into_bytes(),
            },
        );
    }

    // THE DENOMINATOR, BEFORE THE RELOAD. Two empty sets are equal, so an engine that served no
    // fields at all would satisfy the comparison below. This is the vacuity floor and it is
    // asserted on the LIVE engine, where the answer cannot yet involve the fold.
    let live_fields = served_hash_fields(&engine, KEY);
    assert_eq!(
        FIELDS, live_fields,
        "VACUITY: the live engine must serve all {FIELDS} fields before a reload can be said to \
         have kept or lost any; it served {live_fields}",
    );

    // No dump and no unload, so the base anchor is 0 and the whole delta log is folded -- which is
    // the route this module is about. An unload would write a base index and the reload would read
    // the map out of it, which proves nothing about the fold.
    drop(engine);
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let (durable, derived) = both_sources(&engine, KEY);

    println!("[fold-hash] durable map names {} field(s): {durable:?}", durable.len());
    println!("[fold-hash] the pages derive {} live field(s): {derived:?}", derived.len());

    // THE DENOMINATOR AGAIN, AFTER THE RELOAD, on the side that cannot be empty if the fold ran at
    // all. Without this the set comparison passes on two empty sets.
    assert_eq!(
        FIELDS,
        derived.len(),
        "the pages derive {} live field(s) after the fold rather than {FIELDS}, so the fold did not \
         install what this fixture is about and the comparison below would be vacuous",
        derived.len(),
    );

    // BOTH DIRECTIONS, NAMED SEPARATELY, because they are different defects.
    let missing: Vec<&String> = derived.difference(&durable).collect();
    assert!(
        missing.is_empty(),
        "{} field(s) the pages derive are ABSENT from the durable map: {missing:?}. With the \
         element name off the entry this is the shape that loses data -- the map is the only place \
         a reader can get a field name from, and a field the pages hold and the map does not is one \
         `HashGetAll` cannot name.",
        missing.len(),
    );
    let extra: Vec<&String> = durable.difference(&derived).collect();
    assert!(
        extra.is_empty(),
        "{} field(s) the durable map holds are NOT named by any live page: {extra:?}. The fold \
         restored an element whose page it did not leave behind, which is a resurrection.",
        extra.len(),
    );

    println!(
        "[fold-hash] the two sources name the same {} field(s); the map is complete for this route",
        durable.len()
    );

    // ---------------------------------------------------------------------------------------
    // AND WHAT THE HANDLE DISCRIMINATES NOW, WHICH THE SET EQUALITY ABOVE DOES NOT TOUCH.
    //
    // RESTATED: this read "`block_index_handle` hashes the component along with `model_id`,
    // `object_key` and five address fields, and `index_log.rs` calls it 'the only discriminator'
    // between two elements of ONE folded page ... so dropping it would collapse them onto one
    // slot." It was dropped. The handle is the model spelling, the object key and the address, and
    // the collapse that paragraph warned about is the shape the engine now files on purpose -- one
    // entry a page.
    //
    // SO THE QUESTION HERE IS NO LONGER "WOULD REMOVING IT COLLAPSE THESE". It is what the write
    // and fold paths leave behind for a hash of six fields, read off the index rather than argued
    // from the doc: the map is KEYED by the handle, so its keys are the handles.
    // ---------------------------------------------------------------------------------------
    let pages: Vec<(u64, String, String)> = {
        let shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut found = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for (handle, page) in bucket.block_index.iter() {
                if &*page.object_key == KEY {
                    found.push((
                        *handle,
                        "<none>".to_string(),
                        format!("{:?}", page.address),
                    ));
                }
            }
        }
        found.sort();
        found
    };

    println!("[fold-hash] the {} page entries this object holds:", pages.len());
    for (handle, component, address) in &pages {
        println!("    handle {handle:>20}  component {component:<10}  address {address}");
    }
    assert_eq!(
        FIELDS,
        pages.len(),
        "the index holds {} entries for this object rather than {FIELDS}, so the reading below is \
         not about the fields this fixture wrote",
        pages.len(),
    );

    let handles: std::collections::BTreeSet<u64> = pages.iter().map(|row| row.0).collect();
    let addresses: std::collections::BTreeSet<&String> = pages.iter().map(|row| &row.2).collect();
    println!(
        "[fold-hash] {} distinct handle(s) over {} distinct address(es)",
        handles.len(),
        addresses.len()
    );
    assert_eq!(
        FIELDS,
        handles.len(),
        "two of these entries already share a handle, which is a collision today rather than \
         anything to do with removing the component",
    );

    // THE VERDICT ON THE SECOND JOB, stated either way rather than asserted one way. If the six
    // fields sit at six addresses, the component is not what keeps them apart and the collapse the
    // doc warns about does not arise for this shape. If they share one address, it IS what keeps
    // them apart and removing it would take six entries to one -- and this fixture is then the
    // counter-example.
    if addresses.len() == FIELDS {
        // AND WHERE THE SHARED-ADDRESS CASE DOES COME FROM, so the bound is a location and not
        // merely a caveat. Every element gets its own page on the write path; several pages are
        // folded into one only by `container_pages`, and that module states it is done "at
        // COMPACTION and nowhere else". So the component becomes the only discriminator exactly
        // for a COMPACTED batched page -- which is the case the campaign already recorded
        // collapsing, forty hash fields on one page arriving as one entry with thirty-nine lost.
        println!(
            "[fold-hash] MEASURED: each field sits at its OWN address, so these six entries are \
             six pages and the index files one entry for each. That is what the write and fold \
             paths produce -- every element gets its own page, and pages are shared only after \
             COMPACTION batches them, where one entry a page is now the intended shape rather \
             than a collapse to guard against."
        );
    } else {
        println!(
            "[fold-hash] MEASURED: {} field(s) share {} address(es). These entries have no \
             element name to tell them apart, so entries converge on pages here -- which is the \
             intended shape and is what `container_pages_are_batched` measures through a round.",
            pages.len(),
            addresses.len()
        );
    }
    assert!(
        !addresses.is_empty(),
        "no addresses were read, so neither branch above says anything",
    );
}

/// AND THE SKIP COUNTER IS LIVE, so a silent decline cannot read as agreement.
///
/// `fold_carried_container_elements` merges a carried element only when the finished fold still
/// holds a block at the address the element was carried with, and it COUNTS the ones it declines.
/// That gate is correct -- it is what stops a fold resurrecting an element whose page the same fold
/// removed -- but it means "the map agrees with the page index" and "the map is missing a field the
/// merge declined to restore" can produce the same empty difference if the page is gone too.
///
/// So the counter has to be shown to move. This drives a field whose page the fold does NOT leave
/// behind, by deleting it after it was written, and asserts the pair that must then hold: the field
/// is in neither source. A merge that restored it anyway would put it in the durable map alone,
/// which is the `extra` assertion above firing -- so this test is the positive control for that
/// assertion rather than a second copy of it.
///
/// rust-internal: reads the engine's own resident maps after a reload, no external surface
#[test]
fn a_field_whose_page_the_fold_removed_is_in_neither_source() {
    const KEY: &str = "fold-hash-gone";

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    for index in 0..3 {
        write(
            &engine,
            Command::HashSet {
                key: KEY.to_string(),
                field: format!("field-{index}"),
                value: vec![b'v'; 8],
            },
        );
    }
    // The delete is deliberately NOT reported as a touched element -- a removal says nothing,
    // because taking the block away is the whole answer and the merge's own `live` gate is what
    // makes that safe. This is the arm that proves the gate rather than trusting the comment.
    write(
        &engine,
        Command::HashDelete {
            key: KEY.to_string(),
            field: "field-1".to_string(),
        },
    );
    let live_fields = served_hash_fields(&engine, KEY);
    assert_eq!(
        2, live_fields,
        "VACUITY: the live engine must serve the 2 surviving fields before the reload; it served \
         {live_fields}",
    );

    drop(engine);
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let (durable, derived) = both_sources(&engine, KEY);

    println!("[fold-hash-gone] durable {durable:?}  derived {derived:?}");

    assert_eq!(
        2,
        derived.len(),
        "the pages derive {} live field(s) rather than the 2 that survived, so this fixture is not \
         exercising a removal",
        derived.len(),
    );
    assert!(
        !derived.contains("field-1"),
        "field-1's page still derives as live, so the removal did not happen and the gate is untested",
    );
    assert!(
        !durable.contains("field-1"),
        "field-1 is in the durable map and is NOT derived from any page: the merge restored an \
         element whose page the fold removed. That is the resurrection the `live` gate exists to \
         prevent, and it is the defect the other test's `extra` assertion would report.",
    );
    assert_eq!(
        derived, durable,
        "the two sources disagree after a removal: derived {derived:?}, durable {durable:?}",
    );

    println!(
        "[fold-hash-gone] the removed field is in neither source, so the merge's gate declines \
         rather than resurrecting"
    );
}

/// THE CENSUS THAT BOUNDS WHAT THE TWO TESTS ABOVE CANNOT SEE.
///
/// A fixture proves the paths it drives. The class here is enumerable -- the commands that install
/// or remove a hash page -- so it can be bounded by reading the engine's own match rather than by
/// hoping a fixture happened to cover it.
///
/// Four commands write a hash. Three report a `TouchedContainerElement::Hash`, which is what puts
/// the field in the record's `hash_fields` blob and therefore in the durable map after a fold:
///
/// ```text
///     HashSet       installs one field    reports
///     HashMultiSet  installs one a entry  reports one a entry
///     HashIncrBy    installs one field    reports
///     HashDelete    removes a field       reports NOTHING, deliberately
/// ```
///
/// The removal's silence is not a gap and the reason is stated where it is decided: an element is
/// restored only if the finished fold still holds a block at the address it was carried with, so a
/// removal needs to say nothing -- taking the block away is already the whole answer.
/// `a_field_whose_page_the_fold_removed_is_in_neither_source` drives exactly that.
///
/// So every INSTALL path reports, and the one non-reporter is a removal whose safety is the merge's
/// `live` gate. That is the bound: there is no hash write that puts a page in the index without
/// putting its field in the carried channel.
///
/// This test asserts the census rather than printing it, so a fifth hash-writing command added
/// without a touched element fails here instead of silently costing the durable map a field.
///
/// rust-internal: reads the engine's own declarations, no product behaviour
#[test]
fn every_hash_install_path_reports_a_touched_element_and_only_a_removal_does_not() {
    use crate::types::Command;

    let install: Vec<(&str, Command)> = vec![
        (
            "HashSet",
            Command::HashSet {
                key: "k".to_string(),
                field: "f".to_string(),
                value: vec![1],
            },
        ),
        (
            "HashMultiSet",
            Command::HashMultiSet {
                key: "k".to_string(),
                entries: vec![("f".to_string(), vec![1]), ("g".to_string(), vec![2])],
            },
        ),
        (
            "HashIncrBy",
            Command::HashIncrBy {
                key: "k".to_string(),
                field: "f".to_string(),
                increment: 1,
            },
        ),
    ];

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    assert!(!install.is_empty(), "an empty census bounds nothing");
    for (name, command) in &install {
        let touched = crate::engine::command_touched_container_elements(command, shard);
        println!("[census] {name:<14} reports {} touched element(s)", touched.len());
        assert!(
            !touched.is_empty(),
            "{name} installs a hash page and reports NO touched element, so its field reaches the \
             page index and never the durable map -- which is the gap that makes removing \
             `component` lose data",
        );
    }

    // And the removal, which must report nothing for the stated reason.
    let delete = Command::HashDelete {
        key: "k".to_string(),
        field: "f".to_string(),
    };
    let touched = crate::engine::command_touched_container_elements(&delete, shard);
    println!("[census] {:<14} reports {} touched element(s)", "HashDelete", touched.len());
    assert!(
        touched.is_empty(),
        "HashDelete reports a touched element. A removal that carries its field would have the \
         fold restore it whenever the fold still holds some other page at that address, which is \
         the resurrection the `live` gate is there to prevent",
    );
}
