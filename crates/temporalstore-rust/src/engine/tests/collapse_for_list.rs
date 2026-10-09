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
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;

const ELEMENTS: usize = 40;
const LIST_KEY: &str = "collapse/list";
const HASH_KEY: &str = "collapse/hash";
const ZSET_KEY: &str = "collapse/zset";

/// Holds the gate at an explicit value and puts back whatever was there, panic or not.
struct GateHeld {
    restore: Option<String>,
}

impl GateHeld {
    fn at(value: &str) -> Self {
        let restore = std::env::var(TS_CONTAINER_ONE_ENTRY_A_PAGE).ok();
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, value);
        Self { restore }
    }

    fn on() -> Self {
        Self::at("1")
    }

    fn off() -> Self {
        Self::at("0")
    }
}

impl Drop for GateHeld {
    fn drop(&mut self) {
        match self.restore.take() {
            Some(previous) => std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, previous),
            None => std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE),
        }
    }
}

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

/// Live index entries for one kind and object: how many there are, and how many NAME an element.
fn entries_named(engine: &TemporalEngine, model_id: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    let mut named = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != model_id || &*page.object_key != object_key
            {
                continue;
            }
            live += 1;
            if page.component.is_some() {
                named += 1;
            }
        }
    }
    (live, named)
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

/// The entry floor for a collapsed kind: FEWER entries than elements, and not one naming an
/// element. Printed before it is asserted.
fn assert_collapsed(engine: &TemporalEngine, model_id: &str, object_key: &str) {
    let (live, named) = entries_named(engine, model_id, object_key);
    println!(
        "    {model_id:<5} entries: {live:>3} live / {named:>3} naming an element (of {ELEMENTS} elements)"
    );
    assert!(
        live < ELEMENTS,
        "{model_id} filed {live} live entries for {ELEMENTS} elements, so nothing folded onto a \
         shared page and every assertion in this arm is about a gate that changed nothing"
    );
    assert_eq!(
        0, named,
        "{model_id} filed {named} entries naming an element, so the collapsed arm is not what \
         produced this index"
    );
}

/// A WRITE INTO AN ALREADY-FOLDED LIST SUPERSEDES ONE PAGE AND NOT THE OBJECT'S SIBLINGS.
///
/// # WHY THIS IS NOT COVERED BY THE ARMS AROUND IT
///
/// They measure what the projection emits from the model maps -- a DERIVED index, rebuilt whole.
/// This writes one more element into an object whose pages are already folded, which is the path
/// where a slot-wide removal took every other page of the object with it: measured, on a
/// five-member set folded onto one page and written to once, as a listing serving ONE member.
/// Under one entry a page every entry of an object shares one slot keyed `None`, so the page key
/// derived from the address is the only thing telling the object's own entries apart.
///
/// # THE FIRST DRAFT OF THIS ASSERTED A FLOOR AND THE FLOOR WAS VACUOUS
///
/// It asserted `after_live >= folded_live`. All forty elements fold onto ONE page, so the broken
/// behaviour this arm is about leaves exactly one entry too, and `1 >= 1` passes -- measured, by
/// forcing the collapsed kinds down the slot-wide arm and watching the draft stay GREEN. One more
/// page means one more entry, so the count after the write is the count before it PLUS ONE, and
/// that is the only reading the slot-wide behaviour cannot satisfy. The served length is asserted
/// beside it, because an entry count is the index's own bookkeeping and what a client asks is
/// whether the elements are there.
///
/// LIST ONLY, AND THAT IS THE POINT RATHER THAN A LIMITATION. This was written over all four
/// container kinds while the predicate named all four, and hash and zset are not in that set: see
/// `index_entry_names_a_page` for the measurement that put them back out. Driving them here would
/// assert the collapse for kinds that do not collapse.
#[test]
fn a_write_after_a_fold_supersedes_one_page_and_not_the_objects_siblings() {
    let _gate = GateHeld::on();
    println!("\n=== list: one more element written after the fold ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_list(&engine);
    fold(&engine);

    let (folded_live, folded_named) = entries_named(&engine, "list", LIST_KEY);
    println!("    after the fold : {folded_live:>3} live / {folded_named:>3} naming an element");
    assert!(
        folded_live >= 1 && folded_live < ELEMENTS,
        "list holds {folded_live} live entries for {ELEMENTS} elements after the fold, so nothing \
         folded onto a shared page and the write below cannot exercise the page-keyed term at all"
    );

    // ONE MORE ELEMENT, under a name the fold has never seen, so the entry it files names a NEW
    // page rather than rewriting one already folded.
    let fresh = ELEMENTS + 1;
    write(
        &engine,
        Command::ListPush {
            key: LIST_KEY.to_string(),
            member: element_bytes(fresh),
            left: false,
        },
    );

    let (after_live, after_named) = entries_named(&engine, "list", LIST_KEY);
    println!("    after a write  : {after_live:>3} live / {after_named:>3} naming an element");
    assert_eq!(
        folded_live + 1,
        after_live,
        "list held {folded_live} live entries before one more element was written and {after_live} \
         after. One more page means one more entry; anything else means filing the new page took \
         the object's already-folded page with it, which is what the page-keyed supersede term \
         exists to prevent"
    );
    assert_eq!(
        0, after_named,
        "list filed {after_named} entries naming an element after the write, so the write path did \
         not file through the collapsed arm the projection used"
    );

    let served = list_len(&engine);
    println!("    served         : {served:>3} element(s) (of {} written)", ELEMENTS + 1);
    assert_eq!(
        (ELEMENTS + 1) as i64,
        served,
        "list served {served} elements after {} were written, so writing one element into a folded \
         object orphaned the rest",
        ELEMENTS + 1
    );
}

/// A PRESENT LIST ELEMENT IS STILL SERVED UNDER THE COLLAPSE, AND A POPPED ONE IS STILL GONE.
#[test]
fn a_list_keeps_its_present_elements_and_loses_its_popped_one_under_the_collapse() {
    println!("\n=== list, gate on, folded ===");
    let _gate = GateHeld::on();
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
    let _gate = GateHeld::on();
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
        let _gate = GateHeld::on();
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

    let _gate = GateHeld::on();
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
    let _gate = GateHeld::on();
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
/// # THIS IS THE RESTATEMENT OF A PIN THAT HAD TO GO RED
///
/// `index_entry_names_a_page` used to say "only its SET arm consumes the answer: hash, zset and
/// list emit one named entry per element either way", and
/// `gated_corpus_across_a_store_boundary` calls those three kinds THE CONTROL. One of the three has
/// moved, so that sentence is false and the control is two kinds rather than three. This test is
/// what the sentence is replaced BY: the membership of the collapsed set, asserted per kind against
/// a real engine rather than described in a comment.
#[test]
fn the_projection_collapses_list_and_leaves_hash_and_zset_naming_every_element() {
    println!("\n=== all three kinds in one gated store, folded ===");
    let _gate = GateHeld::on();
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

    let hash = entries_named(&engine, "hash", HASH_KEY);
    let zset = entries_named(&engine, "zset", ZSET_KEY);
    let list = entries_named(&engine, "list", LIST_KEY);
    println!("    hash : {:>3} live / {:>3} naming an element", hash.0, hash.1);
    println!("    zset : {:>3} live / {:>3} naming an element", zset.0, zset.1);
    println!("    list : {:>3} live / {:>3} naming an element", list.0, list.1);

    assert_collapsed(&engine, "list", LIST_KEY);

    // THE CONTROLS, AND THE PIN ON THE COLLAPSED SET'S MEMBERSHIP.
    //
    // If either of these goes red because that kind collapsed too, the work named on
    // `index_entry_names_a_page` has to have been done first:
    //   * hash -- its four index-by-component readers moved onto `shard.hashes`;
    //   * zset -- the score taken out of the component, so dropping the component stops deleting
    //     the index's only copy of it, and the rescore supersede fixed so one member at one score
    //     cannot leave two live entries.
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        hash,
        "hash filed {} live entries of which {} name a field. Hash is deliberately NOT in the \
         page-named set: its reads resolve through the index BY COMPONENT with no resident-map \
         fallback, so a nameless hash entry makes a present field unreachable",
        hash.0,
        hash.1
    );
    assert_eq!(
        (ELEMENTS, ELEMENTS),
        zset,
        "zset filed {} live entries of which {} name an element. Zset is deliberately NOT in the \
         page-named set: its component IS THE SCORE, so dropping it deletes the index's only copy \
         of the score -- measured as 12 red tests across 5 modules when it was attempted",
        zset.0,
        zset.1
    );
}

/// THE MEMBERSHIP OF THE COLLAPSED SET, ASKED OF THE PREDICATE ITSELF, BOTH GATE DIRECTIONS.
///
/// The arm above measures what the projection EMITS; this one pins what the shared predicate every
/// filer reads ANSWERS, so a filer that stopped consulting it could not quietly disagree.
#[test]
fn the_shared_predicate_names_set_and_list_but_not_hash_or_zset() {
    use crate::engine::storage_bucket_internals::index_entry_names_a_page;
    {
        let _gate = GateHeld::on();
        assert!(
            index_entry_names_a_page("set"),
            "set must be page-named when the gate is on"
        );
        assert!(
            index_entry_names_a_page("list"),
            "list must be page-named when the gate is on"
        );
        assert!(
            !index_entry_names_a_page("hash"),
            "hash must NOT be page-named: four of its readers resolve through the index by \
             component with no resident-map fallback. See `index_entry_names_a_page`"
        );
        assert!(
            !index_entry_names_a_page("zset"),
            "zset must NOT be page-named: its component carries the SCORE, so dropping it deletes \
             data rather than a name. See `index_entry_names_a_page`"
        );
        // The component-less kinds keep component-keyed convergence, which is what supersedes a
        // relocated page for them. An address-keyed predicate here would leave a stale live entry
        // on every string write in the engine.
        assert!(
            !index_entry_names_a_page("string"),
            "string is not a container kind"
        );
        assert!(
            !index_entry_names_a_page("feature"),
            "feature is not a container kind"
        );
    }
    let _gate = GateHeld::off();
    for kind in ["set", "zset", "list", "hash", "string", "feature"] {
        assert!(
            !index_entry_names_a_page(kind),
            "{kind} answered page-named with the gate explicitly off"
        );
    }
}

/// THE FOUR HASH READERS THAT HOLD HASH OUT, EXERCISED SO THE BASELINE IS RECORDED.
///
/// Not a restatement of the assertion above. This drives the readers themselves under the gate, so
/// that the step which moves them has a measured "before" to preserve rather than a claim about
/// one. `HashLen` is the sharpest of the four: it is
/// `bucket_index_component_block_addresses(..).len()`, so it counts ENTRIES, and under a collapse
/// it would report the PAGE count with no union to rescue it.
#[test]
fn hashs_index_readers_still_answer_per_field_while_hash_is_held_out() {
    println!("\n=== hash readers under the gate, hash held out of the collapse ===");
    let _gate = GateHeld::on();
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
            "HashGet lost field {index}. It resolves through `bucket_index_block_address`, which \
             requires `page.component == Some(field)` on every branch"
        );
    }
}

/// AND THE ZSET READER THAT HOLDS ZSET OUT: ITS COMPONENT CARRIES THE SCORE.
///
/// The baseline the step that converts zset must preserve. A zset component is
/// `{biased_score:016x}{hex(member)}`, so the index's name for an element is the only copy of its
/// score outside `shard.zsets` -- which is why dropping it is a data deletion rather than a
/// renaming, and why `durable_outranks_derived` has three arms about the name being consulted and
/// being allowed to disagree with the durable map.
#[test]
fn a_zset_entry_still_names_its_score_while_zset_is_held_out() {
    println!("\n=== zset entries under the gate, zset held out of the collapse ===");
    let _gate = GateHeld::on();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_zset(&engine);
    fold(&engine);

    // Every live zset entry names an element, and the name is sixteen hex characters of biased
    // score followed by the hex member -- so the score is recoverable FROM THE ENTRY.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut named = 0usize;
    let mut score_bearing = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != "zset" || &*page.object_key != ZSET_KEY {
                continue;
            }
            if let Some(component) = page.component.as_deref() {
                named += 1;
                if component.len() >= 16 && u64::from_str_radix(&component[..16], 16).is_ok() {
                    score_bearing += 1;
                }
            }
        }
    }
    println!("    zset entries naming an element: {named}, of which {score_bearing} carry a decodable score");
    assert_eq!(
        ELEMENTS, named,
        "zset filed {named} entries naming an element for {ELEMENTS} members, so it is not \
         holding one named entry per element any more"
    );
    assert_eq!(
        named, score_bearing,
        "{} of {named} zset components did not decode a biased score out of their first sixteen \
         characters. That decode is what makes the component DATA rather than a name, and it is \
         the reason zset is held out of the collapse",
        named - score_bearing
    );
}
