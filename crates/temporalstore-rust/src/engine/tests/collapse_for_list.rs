// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE COLLAPSE EXTENDED TO `list`, AND THE MEASURED REASONS `zset` AND `hash` ARE NOT WITH IT.
//!
//! # WHAT WAS ATTEMPTED AND WHAT THE TREE SAID
//!
//! This module was first written to convert `hash`, `zset` and `list` together. Driving it
//! converted exactly one of the three, and the two refusals are the useful half -- both were
//! refused ON MEASUREMENT rather than on sequencing, and each for its own reason:
//!
//!   * `zset` REFUSED, 12 tests across 5 modules. A zset's component is not a name for the
//!     element, it IS THE SCORE: `{biased_score:016x}{hex(member)}`. Dropping it deletes the
//!     index's only copy of the score, so a member whose durable entry the fold has not delivered
//!     cannot be rebuilt from its page at all -- `durable_outranks_derived::an_empty_zset_member_
//!     is_a_whole_component_and_survives_the_fold_shape` came back `None` where `Some(3.0)` was
//!     written. And a RESCORE is an in-place rewrite from the index's side: the old element key is
//!     tombstoned while its live entry is not superseded, leaving TWO live entries for one member
//!     at one score, which is precisely the shape `index_entry_names_a_page` warns an
//!     address-keyed predicate mishandles.
//!   * `hash` REFUSED on its readers. Four of them resolve through the page index BY COMPONENT
//!     with no resident-map fallback, so a nameless hash entry makes a present field unreachable
//!     rather than merely unnamed.
//!
//! `list` is the one that converts, and it converts because its component carries no data the
//! engine reads back: the sequence that orders a list lives in `shard.lists`, which is keyed BY
//! that sequence, and every list reader (`ListLen`, `ListRange`) reads the resident map. `list`
//! also has no `LSET`, `LINSERT` or `LREM`, so it never rewrites an element in place and the
//! two-live-entries shape cannot arise for it.
//!
//! # TWO ARMS, BECAUSE A LOSS GUARD IS NOT A RESURRECTION GUARD
//!
//! Every arm of this series up to here asked whether a PRESENT element survives, and a change with
//! a source proof and a working negative control still resurrected a REMOVED member -- because
//! nothing removed anything. So the converted kind gets both halves: a present element is still
//! served, and a removed one is still gone. The removed half is asked a second time ACROSS A STORE
//! BOUNDARY, because re-derivation is where a resurrection actually happens, and a third time
//! through WAL REPLAY, where a refusal does not shorten an answer but fails the whole shard load.
//!
//! # THE FOLD IS NOT OPTIONAL AND ITS ABSENCE WOULD MAKE EVERY ARM VACUOUS
//!
//! One entry per PAGE is one entry per ELEMENT until elements SHARE a page. Without
//! `compact_shard_blocks` every element is its own page, the collapsed count equals the
//! per-element count, and every assertion below would pass over a gate that changed nothing. The
//! fold runs in every arm and the entry floor (`live < ELEMENTS`) is what proves it did.
//!
//! # BOTH GATE DIRECTIONS ARE SET EXPLICITLY, NEVER BY REMOVING THE VARIABLE
//!
//! The gate reads through `env_flag_default_on`, so it is `env_bool(name, true)` and REMOVING the
//! variable selects ON rather than off. An arm that cleared the variable to get the ungated path
//! would silently test the gated one.

#![allow(clippy::all)]
use super::*;

const ELEMENTS: usize = 40;
const LIST_KEY: &str = "collapse/list";
const HASH_KEY: &str = "collapse/hash";
const ZSET_KEY: &str = "collapse/zset";

/// Holds the gate at an explicit value and puts back whatever was there, panic or not.



fn engine_on(
    cache: &std::path::Path,
    pages: &std::path::Path,
    indexes: &std::path::Path,
) -> TemporalEngine {
    TemporalEngine::with_local_dirs(64 * 1024 * 1024, cache, pages, indexes)
}

fn load_request() -> crate::control::LoadShardRequest {
    crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "collapse-for-list".to_string(),
        shard_uri: "local://collapse-for-list/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    }
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(load_request());
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn respond(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

fn element_bytes(index: usize) -> Vec<u8> {
    format!("element-{index:03}").into_bytes()
}

fn field_name(index: usize) -> String {
    format!("field-{index:03}")
}

fn integer(engine: &TemporalEngine, command: Command) -> i64 {
    match respond(engine, command) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    }
}

fn members(engine: &TemporalEngine, command: Command) -> Vec<Vec<u8>> {
    match respond(engine, command) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    }
}

fn bytes(engine: &TemporalEngine, command: Command) -> Option<Vec<u8>> {
    match respond(engine, command) {
        crate::types::CommandResponse::Bytes { value } => value,
        other => panic!("expected Bytes, got {other:?}"),
    }
}

fn list_elements(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    members(
        engine,
        Command::ListRange {
            key: LIST_KEY.to_string(),
            start: 0,
            stop: -1,
        },
    )
}

fn list_len(engine: &TemporalEngine) -> i64 {
    integer(
        engine,
        Command::ListLen {
            key: LIST_KEY.to_string(),
        },
    )
}

/// Live index entries for one kind and object.
///
/// THE SECOND TERM IS GONE. It counted entries that NAME an element, and `BlockIndex` has no field
/// for a name -- so the count could only read zero and the assertions on it could only pass.
fn live_entries(engine: &TemporalEngine, model_id: &str, object_key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != model_id || &*page.object_key != object_key
            {
                continue;
            }
            live += 1;
        }
    }
    live
}

/// (live entries, tombstoned entries) for one kind and object.
fn live_and_tombstoned(
    engine: &TemporalEngine,
    model_id: &str,
    object_key: &str,
) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != model_id || &*page.object_key != object_key {
                continue;
            }
            if page.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

fn write_list(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
            engine,
            Command::ListPush {
                key: LIST_KEY.to_string(),
                member: element_bytes(index),
                left: false,
            },
        );
    }
}

fn write_hash(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
            engine,
            Command::HashSet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
                value: element_bytes(index),
            },
        );
    }
}

fn write_zset(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
            engine,
            Command::ZSetAdd {
                key: ZSET_KEY.to_string(),
                member: element_bytes(index),
                score: index as f64,
            },
        );
    }
}

fn fold(engine: &TemporalEngine) {
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
}

/// The entry floor for a collapsed kind: FEWER entries than elements. Printed before asserted.
///
/// THE SECOND HALF OF THIS FLOOR IS GONE. It also asserted that not one entry named an element,
/// and `BlockIndex` has no field for a name -- so that half could only pass. What is left is the
/// half that can still fail, and it is the sharper one: fewer live entries than elements means the
/// elements actually folded onto a shared page.
fn assert_collapsed(engine: &TemporalEngine, model_id: &str, object_key: &str) {
    let live = live_entries(engine, model_id, object_key);
    println!(
        "    {model_id:<5} entries: {live:>3} live (of {ELEMENTS} elements)"
    );
    assert!(
        live < ELEMENTS,
        "{model_id} filed {live} live entries for {ELEMENTS} elements, so nothing folded onto a \
         shared page and every assertion in this arm is about a gate that changed nothing"
    );
}
/// A PRESENT LIST ELEMENT IS STILL SERVED UNDER THE COLLAPSE, AND A POPPED ONE IS STILL GONE.
#[test]
fn a_list_keeps_its_present_elements_and_loses_its_popped_one_under_the_collapse() {
    println!("\n=== list, gate on, folded ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_list(&engine);
    fold(&engine);
    assert_collapsed(&engine, "list", LIST_KEY);

    // ---- PRESENT ----
    assert_eq!(
        ELEMENTS as i64,
        list_len(&engine),
        "the collapsed list does not report its own length"
    );
    let listed = list_elements(&engine);
    assert_eq!(
        ELEMENTS,
        listed.len(),
        "a collapsed list ranged {} of {ELEMENTS} elements",
        listed.len()
    );
    for index in 0..ELEMENTS {
        assert!(
            listed.contains(&element_bytes(index)),
            "element {index} is missing from a collapsed list range"
        );
    }
    // AND IN ORDER. A list's component is its sequence, which is the ORDER -- so this is the
    // property that would be lost if the order lived in the entry rather than in `shard.lists`.
    let expected: Vec<Vec<u8>> = (0..ELEMENTS).map(element_bytes).collect();
    assert_eq!(
        expected, listed,
        "a collapsed list served its elements OUT OF ORDER. The sequence that orders a list is \
         the component this change drops from the entry, so order is the property at risk"
    );

    // ---- REMOVED. `list` has no LSET/LINSERT/LREM, so a pop off the right end is its removal. ----
    let popped = bytes(
        &engine,
        Command::ListPop {
            key: LIST_KEY.to_string(),
            left: false,
        },
    )
    .expect("a non-empty collapsed list must pop something");
    assert_eq!(
        element_bytes(ELEMENTS - 1),
        popped,
        "the pop returned the wrong end of a collapsed list"
    );
    assert_eq!(
        (ELEMENTS - 1) as i64,
        list_len(&engine),
        "popping one element of a collapsed list did not change its length by one"
    );
    let after = list_elements(&engine);
    assert!(
        !after.contains(&element_bytes(ELEMENTS - 1)),
        "the popped element is still in a collapsed list range"
    );
    for index in 0..(ELEMENTS - 1) {
        assert!(
            after.contains(&element_bytes(index)),
            "popping the tail also took element {index}"
        );
    }
}

/// AND THE POP SURVIVES THE INDEX BEING THROWN AWAY AND REBUILT.
///
/// The arm that matters for a resurrection. The index is a derived projection: a removal held only
/// in the live index has not been tested until the index is re-derived from the model maps, which
/// is what opening the store again does.
#[test]
fn a_popped_list_element_does_not_come_back_when_the_index_is_re_derived() {
    println!("\n=== list, gate on, popped, then reopened ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    {
        let engine = engine_on(&dir.path().join("cache"), &pages, &indexes);
        load_on(&engine);
        write_list(&engine);
        fold(&engine);
        assert_collapsed(&engine, "list", LIST_KEY);
        let popped = bytes(
            &engine,
            Command::ListPop {
                key: LIST_KEY.to_string(),
                left: false,
            },
        )
        .expect("the pop must return an element");
        assert_eq!(element_bytes(ELEMENTS - 1), popped, "popped the wrong end");
        engine.unload_shard(1);
        drop(engine);
    }
    // ITS OWN CACHE, so every byte served comes off disk through the index just decoded rather
    // than out of pages the writer left warm.
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &pages, &indexes);
    load_on(&reloaded);

    let length = list_len(&reloaded);
    println!("    reloaded length: {length} (expected {})", ELEMENTS - 1);
    assert_eq!(
        (ELEMENTS - 1) as i64,
        length,
        "the reopened store holds {length} elements where {} were left; a short answer here is a \
         lost reload and a long one is a resurrection",
        ELEMENTS - 1
    );
    let listed = list_elements(&reloaded);
    assert!(
        !listed.contains(&element_bytes(ELEMENTS - 1)),
        "the popped element came back when the index was re-derived"
    );
    for index in 0..(ELEMENTS - 1) {
        assert!(
            listed.contains(&element_bytes(index)),
            "element {index} did not survive the reopen"
        );
    }
}

/// A GATED LIST COMES BACK THROUGH **WAL REPLAY**, WHICH HARD-REQUIRES THE COMPONENT.
///
/// # THE INVARIANT, WRITTEN DOWN AND DRIVEN
///
/// `lifecycle::apply_outcome_item`'s list arm destructures a `(Some(address), Some(component))`
/// and `return false` otherwise -- and a `false` there is not a wrong answer, it **REFUSES THE
/// WHOLE SHARD LOAD**. It then DECODES the component as `u64::from_str_radix(component, 16)` to
/// recover the sequence. So the component is not merely required to be present, it has to still be
/// the ELEMENT's sequence, spelled the way the write path spells it.
///
/// WHY COLLAPSING THE INDEX ENTRY DOES NOT REACH IT, which is the thing being pinned: that
/// component comes from the WAL outcome item, which `stage_outcome` fills from the
/// `ElementComponent` -- taken BEFORE the `FiledComponent` collapse, and a separate newtype
/// precisely so transposing the two fails to compile rather than silently filing the page's name
/// where the element's belongs. The index entry and the outcome record are different things that
/// share a field name. The existing module that drives this (`replay_under_the_gate`) is SET-ONLY,
/// so for `list` the reading was unpinned until here.
///
/// # WHY THERE IS NO UNLOAD
///
/// An `unload_shard` is what materializes the base index. This fixture deliberately does not
/// unload, so the index file is ABSENT and the reload must replay. The `absent` index-load-path
/// counter is asserted, because a reload that quietly found an index would answer every question
/// below correctly while testing nothing.
#[test]
fn a_gated_list_comes_back_through_wal_replay() {
    println!("\n=== a gated list through WAL replay, no index file ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    {
        let engine = engine_on(&dir.path().join("cache-writer"), &pages, &indexes);
        load_on(&engine);
        write_list(&engine);
        // THE FLOOR BEFORE THE RELOAD: the writer really holds them, so a short answer after the
        // replay is the replay's and not the fixture's.
        assert_eq!(
            ELEMENTS as i64,
            list_len(&engine),
            "the writer does not hold its list elements before the reload"
        );
        // Deliberately NO unload. The engine is dropped here.
    }

    crate::engine::persistence::reset_index_load_path_counts();
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &pages, &indexes);
    let response = reloaded.load_shard_with(load_request());
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "    load_ok={} accepted={accepted} refused_stale={refused_stale} absent={absent} \
         undecodable={undecodable}",
        response.status.ok
    );

    assert!(
        response.status.ok,
        "a gated list store REFUSED TO LOAD through replay: {:?}. The list replay arm answers \
         `false` when it cannot rebuild an element from the outcome's component, and the caller \
         turns that into a failed load -- so this is the shape where collapsing the index entry \
         would take the shard down rather than shorten an answer",
        response.status
    );
    assert!(
        absent > 0,
        "no index was reported ABSENT, so this reload did not have to replay and the arm proves \
         nothing about the replay path"
    );
    assert_eq!(
        ELEMENTS as i64,
        list_len(&reloaded),
        "the replayed list is short, so the list replay arm's sequence decode did not rebuild \
         every element"
    );
    let ranged = list_elements(&reloaded);
    let expected: Vec<Vec<u8>> = (0..ELEMENTS).map(element_bytes).collect();
    assert_eq!(
        expected, ranged,
        "WAL replay rebuilt the list OUT OF ORDER, so the sequence the component carries was not \
         recovered the way the write path spelled it"
    );
}

/// WHY THE PIN IN `element_ordinal_reuse` HAD TO BE RESTATED, MEASURED HERE.
///
/// # THE FACT THE OLD PIN RESTED ON
///
/// `element_ordinal_reuse::every_per_element_delete_leaves_one_tombstone_and_the_whole_object_
/// delete_leaves_none` branched on `kind == "set"`: the collapsed kind took an arm asserting a
/// TOMBSTONE APPEARS, and every other kind took an arm asserting the LIVE PAGE COUNT FALLS. With
/// `list` collapsed it falls into the second arm, and that arm is false for it.
///
/// This records the measurement that makes it so, rather than leaving it as a reading of the other
/// module's source: under the collapse a per-element removal does NOT reduce the live entry count,
/// because the one entry names a PAGE that still holds the object's other elements. What moves
/// instead is the tombstone count.
#[test]
fn a_gated_removal_keeps_the_live_entry_and_adds_a_tombstone_instead_of_reducing_the_count() {
    println!("\n=== what a gated per-element removal does to the entry census ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_list(&engine);
    fold(&engine);

    let before = live_and_tombstoned(&engine, "list", LIST_KEY);
    write(
        &engine,
        Command::ListPop {
            key: LIST_KEY.to_string(),
            left: false,
        },
    );
    let after = live_and_tombstoned(&engine, "list", LIST_KEY);
    println!("    list live/tombstoned before: {} / {}", before.0, before.1);
    println!("    list live/tombstoned after : {} / {}", after.0, after.1);

    // DENOMINATOR: the removal has to have been of a FOLDED object, or the live count would fall
    // for the ordinary reason and this would say nothing about the collapse.
    assert!(
        before.0 < ELEMENTS,
        "the object was not folded: {} live entries for {ELEMENTS} elements, so a falling live \
         count here would be unremarkable",
        before.0
    );
    assert_eq!(
        0, before.1,
        "the object already carried {} tombstone(s) before the removal, so the count after it \
         would not be attributable to the removal",
        before.1
    );

    assert_eq!(
        before.0, after.0,
        "the live entry count moved from {} to {} across a gated removal. If it now FALLS, the \
         `else` arm of `element_ordinal_reuse`'s split is true again for collapsed kinds and the \
         restatement there should be revisited",
        before.0, after.0
    );
    assert!(
        after.1 > before.1,
        "a gated removal filed no tombstone ({} -> {}), so it recorded the removal nowhere and a \
         membership derived from the pages would put the element back",
        before.1,
        after.1
    );
}

/// WHICH KINDS THE PROJECTION COLLAPSES, ASSERTED SIDE BY SIDE IN ONE STORE.
///
/// # THIS IS THE RESTATEMENT OF A PIN THAT HAD TO GO RED, FOR THE SECOND TIME
///
/// `index_entry_names_a_page` used to say "only its SET arm consumes the answer: hash, zset and
/// list emit one named entry per element either way", and
/// `gated_corpus_across_a_store_boundary` calls those three kinds THE CONTROL. This test replaced
/// that sentence with per-kind assertions against a real engine -- and then its own two control
/// arms went red in turn, because hash and zset joined the collapsed set once their refusals were
/// discharged. THE CONTROL IS NOW EMPTY: there is no container kind left outside the set, so the
/// thing this arm pins is no longer a boundary between kinds but the claim that ALL FOUR fold.
/// What replaces the control is `the_shared_predicate_names_every_container_kind_and_nothing_else`,
/// which walks the whole registry and asserts the non-container kinds are still out -- a boundary
/// that cannot be emptied by widening the set, because it is computed from the set rather than
/// listed beside it.
#[test]
fn the_projection_collapses_every_container_kind() {
    println!("\n=== all three kinds in one gated store, folded ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_hash(&engine);
    write_zset(&engine);
    write_list(&engine);
    fold(&engine);

    let hash = live_entries(&engine, "hash", HASH_KEY);
    let zset = live_entries(&engine, "zset", ZSET_KEY);
    let list = live_entries(&engine, "list", LIST_KEY);
    println!("    hash : {hash:>3} live");
    println!("    zset : {zset:>3} live");
    println!("    list : {list:>3} live");

    // ALL THREE, THROUGH ONE HELPER -- three independent measurements, not a restatement of one.
    //
    // AND THE PARAGRAPH THAT STOOD HERE IS REFUTED BY ITS OWN SUBJECT. It read: "THE SECOND HALF
    // IS WHAT KEEPS THIS FROM BECOMING VACUOUS. `entries_named` counts `component.is_some()`, and
    // with every container kind collapsed that count is zero for all of them, which could be read
    // as an assertion that can no longer fail. It can: a regression that files a container per
    // element makes it non-zero."
    //
    // IT CANNOT, ANY MORE. `BlockIndex` has no component field, so no regression in this crate can
    // make that count non-zero -- the only way back is to re-add the field, which fails
    // `state.rs`'s width pin at const-evaluation before any test runs. The half the paragraph
    // dismissed as the one that "would not notice" is now the only one with a subject, and the
    // fold floor is what stops it being vacuous: `live < ELEMENTS` is satisfied trivially when
    // elements share no page, which is why `assert_collapsed` is only ever called after `fold`.
    assert_collapsed(&engine, "list", LIST_KEY);
    assert_collapsed(&engine, "hash", HASH_KEY);
    assert_collapsed(&engine, "zset", ZSET_KEY);
}

/// THE MEMBERSHIP OF THE COLLAPSED SET, ASKED OF THE PREDICATE ITSELF, BOTH GATE DIRECTIONS.
///
/// The arm above measures what the projection EMITS; this one pins what the shared predicate every
/// filer reads ANSWERS, so a filer that stopped consulting it could not quietly disagree.
///
/// # RESTATED, NOT RE-GOLDENED
///
/// This was `the_shared_predicate_names_set_and_list_but_not_hash_or_zset` and its two `!` arms
/// carried the reasons hash and zset were held out. Both reasons were discharged -- hash's four
/// index-by-component readers now answer from `shard.hashes`, and a zset component is the member
/// in hex with the score on the outcome's `value` slot -- so the assertions are turned over rather
/// than deleted, and the kinds that are STILL out are asserted positively below so the set cannot
/// be widened past the containers by accident. The old name itself asserted the false claim, which
/// is why it could not stay.
#[test]
fn the_shared_predicate_names_every_container_kind_and_nothing_else() {
    use crate::engine::storage_bucket_internals::index_entry_names_a_page;
    {
        for kind in ["set", "list", "hash", "zset"] {
            assert!(
                index_entry_names_a_page(kind),
                "{kind} must be page-named when the gate is on: all four container kinds are in \
                 the collapsed set now. See `index_entry_names_a_page` for what discharged the \
                 hash and zset refusals"
            );
        }
        // AND THE OTHER SIDE, WHICH IS THE HALF THAT STAYS FALSE. These kinds file no element
        // name and hold exactly one page per object, so the convergence they need is the
        // object-wide one and NOT the page-keyed one: an address-keyed predicate here would leave
        // a stale live entry on every string write in the engine. Asserted per kind over the
        // whole registry rather than by naming two, so a kind added later is covered by this
        // test instead of by whoever remembers it.
        for kind in crate::engine::storage_bucket_internals::ModelKind::ALL {
            let name = kind.as_str();
            if matches!(name, "set" | "list" | "hash" | "zset") {
                continue;
            }
            assert!(
                !index_entry_names_a_page(name),
                "{name} answered page-named, but it is not a container kind: it files no element \
                 name and has one page per object, so the page-keyed term would leave its \
                 relocated pages behind as stale live entries"
            );
        }
    }
    // AND THERE IS NO SECOND POSITION TO CHECK, WHICH IS WHY THIS ARM IS INVERTED RATHER THAN
    // DELETED.
    //
    // This held the gate explicitly OFF and asserted that NO kind answered page-named -- the
    // escape hatch's own behaviour. The gate is retired, so that arm was left asserting something
    // FALSE the moment its holder was deleted: a line removed from a guard can leave the
    // assertion beside it reading the opposite of the truth, which is worse than deleting both.
    //
    // Restated as the claim that replaces it: the four container kinds answer page-named with NO
    // variable set anywhere, because there is no variable. Asserted over the same list, so the
    // non-container kinds are still checked in the same breath.
    for kind in ["set", "zset", "list", "hash"] {
        assert!(
            index_entry_names_a_page(kind),
            "{kind} is a container kind and did not answer page-named. There is no gate left to \
             turn off, so the only way this can be false is the predicate's own kind list"
        );
    }
    for kind in ["string", "feature"] {
        assert!(
            !index_entry_names_a_page(kind),
            "{kind} answered page-named, and it holds ONE page per object: the page-keyed term \
             would leave its relocated pages behind as stale live entries"
        );
    }
}

/// THE OTHER SIDE OF THE PREDICATE, DRIVEN RATHER THAN ASSERTED: a collapsed kind supersedes
/// exactly the page it replaces and leaves every sibling page of the object alone.
///
/// # WHY THIS IS NOT COVERED BY THE ARMS ABOVE
///
/// Those measure what the projection emits from the model maps -- a DERIVED index, rebuilt whole.
/// This one writes one more element into an object whose pages are already folded, which is the
/// path where a slot-keyed removal took every other page of the object with it: measured, on a
/// five-member set folded onto one page and written to once, as a listing serving ONE member.
/// Under one entry a page every entry of an object shares one slot keyed `None`, so the page key
/// derived from the address is the only thing telling the object's own entries apart -- and HASH
/// and ZSET reach this path for the first time with this change, which is what makes driving it
/// per kind worth the engine starts.
#[test]
fn a_write_after_a_fold_supersedes_one_page_and_not_the_objects_siblings() {
    for (model_id, object_key) in [("list", LIST_KEY), ("hash", HASH_KEY), ("zset", ZSET_KEY)] {
        println!("\n=== {model_id}: one more element written after the fold ===");
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(
            &dir.path().join("cache"),
            &dir.path().join("pages"),
            &dir.path().join("indexes"),
        );
        load_on(&engine);
        match model_id {
            "list" => write_list(&engine),
            "hash" => write_hash(&engine),
            _ => write_zset(&engine),
        }
        fold(&engine);
        let folded_live = live_entries(&engine, model_id, object_key);
        println!("    after the fold : {folded_live:>3} live");
        assert!(
            folded_live >= 1 && folded_live < ELEMENTS,
            "{model_id} holds {folded_live} live entries for {ELEMENTS} elements after the fold, \
             so nothing folded onto a shared page and the write below cannot exercise the \
             page-keyed term at all"
        );

        // ONE MORE ELEMENT, under a name the fold has never seen, so the entry it files is a NEW
        // page rather than a rewrite of one already folded.
        let fresh = ELEMENTS + 1;
        match model_id {
            "list" => write(
                &engine,
                Command::ListPush {
                    key: LIST_KEY.to_string(),
                    member: element_bytes(fresh),
                    left: false,
                },
            ),
            "hash" => write(
                &engine,
                Command::HashSet {
                    key: HASH_KEY.to_string(),
                    field: field_name(fresh),
                    value: element_bytes(fresh),
                },
            ),
            _ => write(
                &engine,
                Command::ZSetAdd {
                    key: ZSET_KEY.to_string(),
                    member: element_bytes(fresh),
                    score: fresh as f64,
                },
            ),
        }
        let after_live = live_entries(&engine, model_id, object_key);
        println!("    after a write  : {after_live:>3} live");

        // THE SIBLINGS SURVIVED, AS AN EXACT COUNT AND NOT A FLOOR.
        //
        // A FLOOR IS THE WRONG SHAPE HERE AND THE FIRST DRAFT OF THIS USED ONE. It asserted
        // `after_live >= folded_live`, and all forty elements fold onto ONE page -- so the broken
        // behaviour this arm is about, a slot-wide removal that takes the folded page with it,
        // leaves exactly one entry too, and `1 >= 1` passes. Measured: the mutation below was
        // green against that draft. The folded page's entry and the new page's entry are two
        // distinct entries, so the count after the write is the count before it PLUS ONE, and
        // that is the only reading the slot-wide behaviour cannot satisfy.
        assert_eq!(
            folded_live + 1,
            after_live,
            "{model_id} held {folded_live} live entries before one more element was written and \
             {after_live} after. One more page means one more entry; anything else means filing \
             the new page took the object's already-folded page with it, which is what the \
             page-keyed supersede term exists to prevent"
        );
        // THE `after_named == 0` ARM IS GONE: an entry has no element name to count, so it could
        // only pass. What it was watching for -- the write path filing through a different arm
        // than the projection -- is held by the count above, which is an exact `folded_live + 1`
        // and not a floor.

        // AND THE ELEMENTS ARE STILL SERVED, which is the half a count cannot stand in for: an
        // entry count is the index's own bookkeeping, and the question a client asks is whether
        // the members are there. This is the assertion that caught the recorded five-member set
        // serving ONE member after a single write.
        let served = match model_id {
            "list" => list_len(&engine),
            "hash" => integer(
                &engine,
                Command::HashLen {
                    key: HASH_KEY.to_string(),
                },
            ),
            _ => integer(
                &engine,
                Command::ZSetCard {
                    key: ZSET_KEY.to_string(),
                },
            ),
        };
        println!("    served         : {served:>3} element(s) (of {} written)", ELEMENTS + 1);
        assert_eq!(
            (ELEMENTS + 1) as i64,
            served,
            "{model_id} served {served} elements after {} were written, so writing one element \
             into a folded object orphaned the rest",
            ELEMENTS + 1
        );
    }
}

/// THE FOUR HASH READERS THAT USED TO HOLD HASH OUT, NOW ANSWERING OVER A COLLAPSED HASH.
///
/// # THE BASELINE THIS RECORDED IS NOW THE RESULT IT CHECKS
///
/// This was written while hash was held out, to record a measured "before" for the step that moves
/// those readers rather than a claim about one. The step has landed: they answer from
/// `shard.hashes` instead of resolving through the index by component, and hash is in the
/// collapsed set -- so the SAME assertions now say something stronger than they were written to
/// say. They are what shows a hash field is still reachable when its entry carries no element
/// name, which is the one thing the collapse could have broken for this kind.
///
/// `HashLen` is the sharpest of the four and the reason this arm is worth the engine start: it was
/// `bucket_index_component_block_addresses(..).len()`, so it counted ENTRIES, and over a collapsed
/// index it would report the PAGE count with no union to rescue it. It answers the full element
/// count here with the fields on folded pages, which is only possible because it stopped counting
/// entries.
#[test]
fn hashs_index_readers_still_answer_per_field_over_a_collapsed_hash() {
    println!("\n=== hash readers under the gate, hash held out of the collapse ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_hash(&engine);
    fold(&engine);

    let length = integer(
        &engine,
        Command::HashLen {
            key: HASH_KEY.to_string(),
        },
    );
    println!("    HashLen: {length} (of {ELEMENTS} fields)");
    assert_eq!(
        ELEMENTS as i64,
        length,
        "HashLen answered {length} for {ELEMENTS} fields. It counts index ENTRIES, so this is the \
         reader that reports a page count the moment hash is collapsed"
    );
    for index in 0..ELEMENTS {
        let value = bytes(
            &engine,
            Command::HashGet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
            },
        );
        assert_eq!(
            Some(element_bytes(index)),
            value,
            "HashGet lost field {index} over a COLLAPSED hash. It used to resolve through \
             `bucket_index_block_address`, which required `page.component == Some(field)` on \
             every branch; it answers from `shard.hashes` now, and this is what shows a present \
             field is still reachable when its entry carries no element name"
        );
    }
}

/// AND NO ZSET ENTRY SPELLS A SCORE ANY MORE -- THE ASSERTION TURNED OVER, NOT DELETED.
///
/// # WHAT THIS ARM USED TO SAY, AND WHY IT IS WORTH KEEPING INVERTED
///
/// It said a zset entry's name IS the score: a component was `{biased_score:016x}{hex(member)}`,
/// so the index's name for an element was the only copy of its score outside `shard.zsets`, which
/// made dropping it a data deletion rather than a renaming. That was the refusal that held zset
/// out of the collapsed set, and it was discharged by moving the score onto the WAL outcome's
/// `value` slot.
///
/// Inverted it is a TRIPWIRE ON A DATA LOSS, which is a stronger thing to own than a baseline. If
/// a zset entry ever carries sixteen leading hex characters again, either the score has crept back
/// onto the component -- in which case the collapse that drops the component is deleting it -- or
/// a member's own hex is being mistaken for one. Deleting this arm when its premise flipped would
/// have left nothing watching that, since every other assertion about zset components checks what
/// they DO hold.
///
/// Asserted over the entries in the index rather than over the predicate, so it reads the shape
/// that actually got filed.
#[test]
fn no_zset_entry_spells_a_score_now_that_the_collapse_drops_the_name() {
    println!("\n=== zset entries under the gate, zset in the collapse ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_zset(&engine);
    fold(&engine);

    // THE TRIPWIRE IS MOVED OFF THE ENTRIES AND ONTO THE RENDERER, because the entries can no
    // longer carry the shape it was watching for.
    //
    // It walked the live zset entries and asserted two counts were zero: how many named an element
    // at all, and how many of those spelled sixteen leading hex characters that decode as a biased
    // score. Its own comment explained why both were taken -- "no entry carries a score" is
    // satisfied vacuously by "no entry carries a name", and the named count is what tells those
    // apart. `BlockIndex` has no component field now, so BOTH counts are structurally zero and
    // neither can tell anything apart: the arm it was built to be, a tripwire on a data loss,
    // cannot be served by counting entries any more.
    //
    // WHAT CAN STILL REGRESS IS THE RENDERER, and that is what this now asserts.
    // `container_pages::component_from_element_key` is the function that turns a page's item key
    // back into a component string, and its `ScoreThenMember` arm is the one that used to spell
    // the score: it returns `hex::encode(key[8..])`, the member alone, deliberately skipping the
    // eight score bytes. A change that dropped the `8..` would put the score straight back into
    // every derived name -- which is the data-deletion risk the old arm was watching -- and it
    // would do it without any entry carrying a component at all. So the key is built WITH a known
    // score in its first eight bytes and the render is asserted not to contain it.
    //
    // AND THE ROUND TRIP IS ASSERTED IN BOTH DIRECTIONS, because a renderer that returned the
    // empty string would also "not contain the score" and would be a worse loss than the one
    // guarded against.
    let member = b"zs-member-01".to_vec();
    let score_bytes: [u8; 8] = 0x0123_4567_89ab_cdefu64.to_be_bytes();
    let mut page_key = score_bytes.to_vec();
    page_key.extend_from_slice(&member);
    let rendered = crate::engine::container_pages::component_from_element_key(
        crate::engine::container_pages::ElementKeySpelling::ScoreThenMember,
        &page_key,
    )
    .expect("a score-then-member key renders a component");
    let score_hex = hex::encode(score_bytes);
    println!("    key = {} -> component {rendered}", hex::encode(&page_key));
    assert!(
        !rendered.contains(&score_hex),
        "the zset component renderer spelled the score back into the name: {rendered} contains \
         {score_hex}. The score rides the WAL outcome's `value` slot and `shard.zsets` now, so a \
         score in a rendered name is the data the collapse deletes being put back where it will \
         be dropped"
    );
    assert_eq!(
        hex::encode(&member),
        rendered,
        "the zset component renderer must return the MEMBER alone. It returned {rendered}, which \
         is neither the member nor the member behind a score -- a renderer that lost the member \
         would satisfy the score assertion above while losing more than the score ever was"
    );
}
