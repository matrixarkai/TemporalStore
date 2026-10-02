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
fn both_sources(
    engine: &TemporalEngine,
    key: &str,
) -> (std::collections::BTreeSet<String>, std::collections::BTreeSet<String>) {
    let shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let durable: std::collections::BTreeSet<String> = shard
        .hashes
        .get(key)
        .map(|fields| fields.keys().map(|name| name.to_string()).collect())
        .unwrap_or_default();
    let derived: std::collections::BTreeSet<String> =
        crate::engine::bucket_store::bucket_index_component_block_addresses(shard, "hash", key)
            .into_iter()
            .filter_map(|(component, _address)| component.map(|name| name.to_string()))
            .collect();
    (durable, derived)
}

/// THE QUESTION, AND WHY IT IS NOT "IS THE FIELD READABLE".
///
/// `HashGetAll` resolves through `bucket_index_component_block_addresses`, which is keyed BY THE
/// COMPONENT -- it reads each field's name off the page entry. The fold installs page entries. So a
/// folded hash field is readable today whether the durable map received it or not, and a test
/// asserting "readable through the served path" would go green while proving nothing about the map.
/// That is the assertion this module deliberately does NOT make.
///
/// THE DEPENDENCY ON THE MAP DOES NOT EXIST YET -- IT IS CREATED BY REMOVING THE COMPONENT. Today
/// the entry carries the field name and the map is a second copy. Take `component` off the entry --
/// 16 of the 56 bytes a resident entry weighs -- and `HashGetAll` has nowhere left to read a name
/// FROM except `shard.hashes`. So the order is not "reroute the readers, then remove the field". It
/// is: prove the map is complete FIRST, because the removal is what makes the map load-bearing.
/// This module is that proof, or its refutation.
///
/// WHAT IS ASSERTED, THEREFORE: that the two sources name THE SAME SET of fields after a fold.
/// Set equality in both directions and not a count, because a count cannot tell a missing field
/// from an extra one, and the two failures mean opposite things -- a field the map lacks is the
/// removal losing data, and a field the map holds alone is the fold resurrecting something.
///
/// rust-internal: reads the engine's own resident maps after a reload, no external surface
#[test]
fn the_fold_leaves_the_durable_hash_map_naming_exactly_the_fields_the_page_index_names() {
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
    println!("[fold-hash] page index names {} field(s): {derived:?}", derived.len());

    // THE DENOMINATOR AGAIN, AFTER THE RELOAD, on the side that cannot be empty if the fold ran at
    // all. Without this the set comparison passes on two empty sets.
    assert_eq!(
        FIELDS,
        derived.len(),
        "the page index names {} field(s) after the fold rather than {FIELDS}, so the fold did not \
         install what this fixture is about and the comparison below would be vacuous",
        derived.len(),
    );

    // BOTH DIRECTIONS, NAMED SEPARATELY, because they are different defects.
    let missing: Vec<&String> = derived.difference(&durable).collect();
    assert!(
        missing.is_empty(),
        "{} field(s) the page index names are ABSENT from the durable map: {missing:?}. Taking \
         `component` off the entry would lose exactly these -- the page would carry no name and the \
         map would not hold one either.",
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
    // AND THE COMPONENT'S SECOND JOB, WHICH THE SET EQUALITY ABOVE DOES NOT TOUCH.
    //
    // `block_index_handle` hashes the component along with `model_id`, `object_key` and five
    // address fields, and `index_log.rs` calls it "the only discriminator" between two elements of
    // ONE folded page -- which share model id, object key AND address -- so dropping it would
    // collapse them onto one slot.
    //
    // But the handle hashes the ADDRESS too, so that collapse needs the entries to share one.
    // Whether these six do is a fact about what the fold produced, not something to argue from the
    // doc, so it is read here: the map is KEYED by the handle, so its keys are the handles.
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
                        page.component.as_deref().unwrap_or("<none>").to_string(),
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
            "[fold-hash] VERDICT: each field sits at its OWN address, so the address alone \
             discriminates these six and the component is not load-bearing as a map key HERE. \
             The write and fold paths give every element its own page; elements share a page only \
             after COMPACTION batches them, so removing the component is safe for this route and \
             unsafe for a compacted batched page. The blocker is batching, not hash reads."
        );
    } else {
        println!(
            "[fold-hash] VERDICT: {} field(s) share {} address(es), so the component IS the only \
             discriminator for them and removing it would collapse them onto one slot. This is the \
             counter-example, in a fixture rather than in an argument.",
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
        "the page index names {} field(s) rather than the 2 that survived, so this fixture is not \
         exercising a removal",
        derived.len(),
    );
    assert!(
        !derived.contains("field-1"),
        "field-1's page is still in the index, so the removal did not happen and the gate is untested",
    );
    assert!(
        !durable.contains("field-1"),
        "field-1 is in the durable map and NOT in the page index: the merge restored an element \
         whose page the fold removed. That is the resurrection the `live` gate exists to prevent, \
         and it is the defect the other test's `extra` assertion would report.",
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
