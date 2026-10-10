// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER PAGE'S ORDINAL WAS A HARDCODED ZERO, AND THE FIELD TO HOLD IT WAS ALREADY STORED.
//!
//! # WHAT THIS IS, AND WHAT IT DELIBERATELY IS NOT
//!
//! `BlockAddress::block_id` is a stored `u16` -- a page's position INSIDE its object. Every
//! TIMESTAMPED kind already gets one: `next_block_index_for_object` has thirteen call sites covering
//! `feature` and the six `context_*` kinds, and `compaction` preserves the value across a rewrite
//! with a comment saying why ("taking a fresh one here would give every block of a multi-block
//! object the same index, and the index entries would collide"). No CONTAINER kind ever called it.
//! A container element reached the slab through `append_value` -> `append_with_block_metadata`,
//! and that function's whole body is `self.append_block_of_object(bytes, object_id, routing_bucket,
//! 0)`. The zero was an argument nobody supplied, not a missing field and not a format limit.
//!
//! THIS IS NOT THE COMPONENT'S REMOVAL. The component stays the element's identity: #1996 refuted
//! moving identity into an ordinal on two independent grounds, and the first of them -- the delta
//! fold delivers elements whose durable-map entry was never written -- is still true on this
//! revision. Nothing here reads the ordinal to find a row. Deletion still matches by component,
//! which is what `deletion_still_finds_its_element_by_component` holds. THE ENTRY no longer carries
//! that component: a container element's identity lives in its resident map and in its page's
//! payload, which is where every arm in this module now reads it from.
//!
//! # IT NAMES A POSITION, NOT AN ELEMENT
//!
//! That is forced by the tree rather than chosen: `max` FALLS after a delete and the next insert is
//! handed the ordinal just freed. `the_ordinal_names_a_position_and_a_delete_frees_it` drives exactly
//! that and asserts the reuse, because a reader that treated the ordinal as naming a particular
//! element would be silently corrupt the moment it happened.
//!
//! **THE REASON `max` FALLS HAS MOVED TWICE, AND THE CONCLUSION HAS NOT.** It used to fall because
//! the entry was GONE -- `mark_bucket_index_block_deleted_with` was named for a mark it did not
//! make, its body a `retain` returning false. A per-element removal then began KEEPING an entry,
//! and this paragraph said `max` fell because "`container_page_ordinal` FILTERS `deleted`". That
//! walk over entries is gone: the allocator asks the RESIDENT MAP, which is keyed by element and
//! holds exactly the object's live elements, so its high-water mark is over live elements by
//! construction and needs no filter. A removal drops its element from the map and `max` falls with
//! it.
//!
//! AND THE FILTER WOULD NOT HAVE BEEN ENOUGH, which is why this is worth more than a tidier
//! mechanism. A removal keeps the live entry over the page its element vacated -- not deleted, so
//! no `deleted` filter can see it -- so a walk over live ENTRIES does not fall after a removal at
//! all. The arms here assert both quantities wherever they part company: the POSITIONS from the
//! resident map, and the retained live entry as a footprint cost with its own message saying which
//! assertion to restate when the retirement lands.
//!
//! So the freed ordinal is held by two live entries and one tombstone -- the arrival's, the one the
//! removal left standing, and the tombstone that records the removal -- while exactly one live
//! ELEMENT occupies the position. That is asserted, not left implicit, because two
//! entries at one ordinal is also what corruption would look like and the whole difference is which is
//! live. The safety argument is unchanged and is the one this section opens with: identity lives in the
//! component, so a position may be refilled.
//!
//! # TWO DEPARTURES FROM THE SERIES DERIVATION, BOTH FORCED
//!
//!   * AN OVERWRITE KEEPS ITS ORDINAL. `HashSet`/`SetAdd` on an existing member REPLACES that
//!     member's page. `max + 1` there would climb once per WRITE rather than once per ELEMENT, and a
//!     single member rewritten 65,536 times would reach the ceiling on a set of one. Driven by
//!     `an_overwrite_keeps_the_members_ordinal_rather_than_climbing`, which fails at 4 instead of 0
//!     without the component lookup.
//!   * PAST THE CEILING NOTHING IS ASSIGNED. The stored width is sixteen bits, so the ceiling is
//!     65,535 -- and `narrow_block_id` PANICS above it rather than saturating, deliberately. An
//!     object's 65,536th element is not a caller's mistake, so the ordinal is simply left at the `0`
//!     it has on `main`: past the ceiling this change is a strict no-op. Driven directly on the
//!     helper by `past_the_ceiling_the_ordinal_is_left_unassigned_rather_than_panicking`.
//!
//! # THE ONE WAY THIS COULD HAVE BEEN SILENT DATA LOSS
//!
//! `decode_block_record` compares `address.block_id()` against the record header's own block id and
//! refuses the page with "page id mismatch" when they differ -- the one cross-check on a read that
//! can actually fire. It is inert today only because the address side is 0 on both sides. Stamping
//! the ordinal onto the INDEX's copy of the address after the append would have left the header
//! saying 0 and the address saying N, and every container page written would have become unreadable.
//! So the ordinal is threaded into `append_block_of_object`, which builds the header and the
//! returned address from the same value. `a_reloaded_container_still_reads_every_element` is the
//! guard: it unloads the shard, loads it again so nothing is served from a warm cache, and reads
//! every element back through the decode path.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = 1023;

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

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "container-page-ordinal".to_string(),
        shard_uri: "local://container-page-ordinal/1".to_string(),
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

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture read failed: {response:?}");
    response.response
}

/// The ordinals this object's LIVE page entries carry, in index order.
///
/// THE COMPONENT SLOT IS GONE FROM THIS TUPLE, BECAUSE IT HAD STOPPED CARRYING ONE. This returned
/// `Vec<(Option<String>, Option<u64>)>` and the first half was a hardcoded `None::<String>` once
/// the element-name field left `BlockIndex`. Three callers only ever took `.len()`, but two
/// FILTERED on that half -- and a filter over a column that is always `None` matches nothing, so
/// one of them counted zero stale pages and PASSED for that reason. Dropping the slot makes every
/// such filter a compile error instead. Element identity is `ordinal_for_component` below, which
/// asks the authority for it.
fn pages_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted {
                continue;
            }
            if page.model_id.as_str() == kind && &*page.object_key == key {
                held.push(page.address.block_id());
            }
        }
    }
    held.sort();
    held
}

/// This object's TOMBSTONE entries: the element each is about, and the ordinal its page carries.
///
/// THE ELEMENT NOW COMES FROM THE BUCKET'S TOMBSTONE ROWS, NOT FROM THE ENTRY. It read
/// `page.component`, and a tombstone entry files `None` there: the element a removal is about is a
/// per-ELEMENT fact and lives beside the entries, where only a removal pays for it. Restated rather
/// than re-goldened -- the two arms below are about WHICH element a removal is recorded against,
/// and that question still has an answer; what moved is where the answer is kept. Reading the
/// entry here instead would make both of them assert `None == None`.
fn tombstone_pages_of(
    engine: &TemporalEngine,
    kind: &str,
    key: &str,
) -> Vec<(Option<String>, Option<u64>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted && page.model_id.as_str() == kind && &*page.object_key == key {
                held.push((
                    bucket
                        .tombstone_element_at(&page.address)
                        .map(|row| row.component.to_string()),
                    page.address.block_id(),
                ));
            }
        }
    }
    held.sort();
    held
}

/// The ordinals this object's LIVE page entries carry, ascending, with absence spelled as `None`.
fn ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let mut ordinals = pages_of(engine, kind, key);
    ordinals.sort();
    ordinals
}

/// The positions this object's live ELEMENTS hold, ascending -- the allocator's own input.
///
/// NOT THE SAME QUANTITY AS [`ordinals_of`], AND THE DIFFERENCE IS THE POINT. A removal keeps the
/// live entry over the page its element vacated, so the live INDEX still carries that element's
/// ordinal while the element itself is gone. `container_page_ordinal` reads the RESIDENT MAP, so
/// this is the set a position claim is actually about, and the two are asserted separately wherever
/// they part company.
fn resident_ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held: Vec<Option<u64>> = match kind {
        "hash" => shard
            .hashes
            .get(key)
            .map(|fields| fields.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        "set" => shard
            .sets
            .get(key)
            .map(|members| members.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        "zset" => shard
            .zsets
            .get(key)
            .map(|members| {
                members
                    .values()
                    .map(|(_score, address)| address.block_id())
                    .collect()
            })
            .unwrap_or_default(),
        "list" => shard
            .lists
            .get(key)
            .map(|entries| entries.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        other => panic!("no resident container map is claimed for kind {other}"),
    };
    held.sort();
    held
}

/// The ordinal the element with exactly this component holds, if the object holds that element.
///
/// RE-ATTRIBUTED TO THE AUTHORITY, AND IT IS THE SAME DOOR THE ALLOCATOR USES. This searched
/// [`pages_of`] for an entry whose component matched, which answered `None` for every element once
/// an entry stopped naming one -- so three arms in this module read "no such element" for elements
/// that were plainly there. `state::resident_component_address` is what
/// `container_page_ordinal`'s overwrite arm reads, so an element's position and the number the
/// allocator would hand it back are one value read one way, and no component spelling is restated
/// here to drift from the engine's.
///
/// The two layers of `Option` are kept and mean what they did: the outer is whether the object
/// holds this element at all, the inner is whether its address carries an ordinal.
fn ordinal_for_component(
    engine: &TemporalEngine,
    kind: &str,
    key: &str,
    component: &str,
) -> Option<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    crate::engine::state::resident_component_address(shard, kind, key, component)
        .map(crate::block_store::ElementEntry::block_id)
}

const MEMBERS: usize = 8;

/// Seed one object of each container kind with `MEMBERS` distinct elements.
///
/// Returns the four `(kind, key)` pairs so no test hand-writes the list.
fn seed_one_of_each_kind(engine: &TemporalEngine) -> Vec<(&'static str, String)> {
    for m in 0..MEMBERS {
        write(
            engine,
            Command::SetAdd {
                key: "cpo-set".to_string(),
                member: format!("member-{m}").into_bytes(),
            },
        );
        write(
            engine,
            Command::ZSetAdd {
                key: "cpo-zset".to_string(),
                member: format!("member-{m}").into_bytes(),
                score: m as f64,
            },
        );
        write(
            engine,
            Command::ListPush {
                key: "cpo-list".to_string(),
                member: format!("member-{m}").into_bytes(),
                left: false,
            },
        );
        write(
            engine,
            Command::HashSet {
                key: "cpo-hash".to_string(),
                field: format!("field-{m}"),
                value: format!("value-{m}").into_bytes(),
            },
        );
    }
    vec![
        ("set", "cpo-set".to_string()),
        ("zset", "cpo-zset".to_string()),
        ("list", "cpo-list".to_string()),
        ("hash", "cpo-hash".to_string()),
    ]
}

// =================================================================================================
// 1. THE ASSIGNMENT, PER KIND, WITH THE DENOMINATOR PRINTED
// =================================================================================================

/// EVERY CONTAINER PAGE CARRIES ITS OWN ORDINAL, AND ON `main` EVERY ONE OF THEM CARRIES ZERO.
///
/// Four kinds, `MEMBERS` elements each, and the assertion is on the SET of ordinals rather than on
/// a count: `0..MEMBERS` exactly, no duplicate and none missing. A count alone would pass on a store that
/// handed every page the same number, which is precisely what `main` does.
///
/// The denominator is printed per kind because a kind that seeded nothing and a kind whose ordinals
/// are all correct both produce "no wrong ordinals".
///
/// rust-internal: drives four command arms through the engine, no external surface
#[test]
fn every_container_page_carries_its_own_ordinal_rather_than_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let kinds = seed_one_of_each_kind(&engine);

    let expected: Vec<Option<u64>> = (0..MEMBERS as u64).map(Some).collect();
    println!("\n=== container page ordinals, {MEMBERS} elements an object ===");
    let mut checked = 0usize;
    for (kind, key) in &kinds {
        let ordinals = ordinals_of(&engine, kind, key);
        println!(
            "  {kind:<5} pages={:<3} ordinals={:?}",
            ordinals.len(),
            ordinals
                .iter()
                .map(|o| o.map_or(-1i64, |v| v as i64))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ordinals.len(),
            MEMBERS,
            "{kind}/{key} holds {} pages, not the {MEMBERS} the seed wrote -- the arm under test \
             did not file what this asserts about",
            ordinals.len()
        );
        assert_eq!(
            ordinals, expected,
            "{kind}/{key} does not carry one distinct ordinal per element. On main every one of \
             these is Some(0), because `append_with_block_metadata` passes a hardcoded 0"
        );
        checked += ordinals.len();
    }
    assert_eq!(
        checked,
        kinds.len() * MEMBERS,
        "the sweep checked {checked} pages over {} kinds, not {}",
        kinds.len(),
        kinds.len() * MEMBERS
    );
    println!("  [denominator] {checked} pages over {} kinds", kinds.len());
}

// =================================================================================================
// 2. A CONTROL THAT MUST NOT MOVE
// =================================================================================================

/// THE CONTROL: A `string` PAGE IS UNTOUCHED, AT 0.00%.
///
/// `string` is the kind with no component at all, it is not a container, and its arm still calls
/// plain `append_value`. Every string page must still carry ordinal 0 -- if this moves, the change
/// reached an arm it was not supposed to reach, and the measurement above is measuring the harness.
///
/// rust-internal: drives StringSet, no external surface
#[test]
fn a_string_page_is_left_at_zero_which_is_the_control() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    const KEYS: usize = 6;
    for k in 0..KEYS {
        write(
            &engine,
            Command::StringSet {
                key: format!("cpo-string-{k}"),
                value: format!("value-{k}").into_bytes(),
            },
        );
    }

    let mut moved = 0usize;
    let mut seen = 0usize;
    for k in 0..KEYS {
        let ordinals = ordinals_of(&engine, "string", &format!("cpo-string-{k}"));
        assert_eq!(
            ordinals.len(),
            1,
            "a string key holds one page, not {}",
            ordinals.len()
        );
        seen += ordinals.len();
        if ordinals[0] != Some(0) {
            moved += 1;
        }
    }
    assert_eq!(seen, KEYS, "the control saw {seen} pages, not {KEYS}");
    let pct = 100.0 * moved as f64 / seen as f64;
    println!("\n=== control === string pages moved {moved}/{seen} = {pct:.2}%");
    assert_eq!(
        moved, 0,
        "{moved} of {seen} string pages changed ordinal. The control must sit at 0.00%: the \
         container arms are the only ones that were touched"
    );
}

// =================================================================================================
// 3. AN OVERWRITE KEEPS ITS ORDINAL
// =================================================================================================

/// AN OVERWRITE IS THE SAME ELEMENT IN THE SAME PLACE, SO IT KEEPS ITS ORDINAL.
///
/// This is the clause that bounds the ordinal by the object's live element count instead of by its
/// WRITE count. Without the component lookup, `max + 1` would hand the fifth write of one member
/// ordinal 4 on a set of one element -- and a member rewritten 65,536 times would reach a ceiling
/// that has nothing to do with how many elements the object holds.
///
/// Driven on both the kind whose component is content-derived (`set`) and the kind whose component
/// is the caller's own field name (`hash`).
///
/// rust-internal: drives repeated writes of one element, no external surface
#[test]
fn an_overwrite_keeps_the_members_ordinal_rather_than_climbing() {
    // ONE MEMBER REWRITTEN REPEATEDLY IS A ONE-PAGE SHAPE ONLY UNGATED -- gated it settles on
    // two pages and this test's own floor says so ("not measuring an overwrite"). The ordinal
    // under the collapsed projection is asserted by
    // `ordinal_under_the_gate::an_overwrite_keeps_the_members_ordinal_under_either_projection`,
    // which drives BOTH arms, so pinning this one loses nothing.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    const REWRITES: usize = 5;
    let member = b"the-one-member".to_vec();
    let component = hex::encode(&member);

    // One element, written REWRITES times.
    for _ in 0..REWRITES {
        write(
            &engine,
            Command::SetAdd {
                key: "cpo-ow-set".to_string(),
                member: member.clone(),
            },
        );
    }
    let set_pages = pages_of(&engine, "set", "cpo-ow-set");
    assert_eq!(
        set_pages.len(),
        1,
        "a set of one member rewritten {REWRITES} times holds {} pages, not 1 -- this test is not \
         measuring an overwrite",
        set_pages.len()
    );
    let set_ordinal = ordinal_for_component(&engine, "set", "cpo-ow-set", &component)
        .expect("the member's page is present");

    for i in 0..REWRITES {
        write(
            &engine,
            Command::HashSet {
                key: "cpo-ow-hash".to_string(),
                field: "the-one-field".to_string(),
                value: format!("v{i}").into_bytes(),
            },
        );
    }
    let hash_pages = pages_of(&engine, "hash", "cpo-ow-hash");
    assert_eq!(
        hash_pages.len(),
        1,
        "a hash of one field rewritten {REWRITES} times holds {} pages, not 1",
        hash_pages.len()
    );
    let hash_ordinal = ordinal_for_component(&engine, "hash", "cpo-ow-hash", "the-one-field")
        .expect("the field's page is present");

    println!(
        "\n=== overwrite === after {REWRITES} rewrites of ONE element: set ordinal {:?}, hash \
         ordinal {:?} (climbing would give Some({}))",
        set_ordinal,
        hash_ordinal,
        REWRITES - 1
    );
    assert_eq!(
        set_ordinal,
        Some(0),
        "the set member's ordinal climbed to {set_ordinal:?} over {REWRITES} rewrites. It must \
         stay 0: the ordinal counts elements, not writes"
    );
    assert_eq!(
        hash_ordinal,
        Some(0),
        "the hash field's ordinal climbed to {hash_ordinal:?} over {REWRITES} rewrites"
    );
}

// =================================================================================================
// 4. POSITION, NOT ELEMENT -- AND THE REUSE THAT PROVES IT
// =================================================================================================

/// THE ORDINAL NAMES A POSITION. A DELETE FREES IT AND THE NEXT INSERT IS HANDED IT BACK.
///
/// This is asserted rather than avoided, because it is what the tree does and the PR has to say
/// which of the two it is.
///
/// WHAT THE MECHANISM USED TO BE AND WHAT IT IS NOW. This said the page was REMOVED -- the body of
/// `mark_bucket_index_block_deleted_with` was a `retain` returning false -- and that was why `max`
/// fell. A removal now KEEPS an entry, pointing at the page that records it and carrying `deleted`,
/// so that a membership derived from the pages does not resurrect the element. The `max` still falls
/// for a different reason: `container_page_ordinal` filters `deleted`. So the CLAIM of this test is
/// unchanged and its MECHANISM moved, which is exactly the case worth spelling out -- a reader who
/// assumed the old mechanism would expect this test to be red.
///
/// AND THE SAFETY CONDITION IS ASSERTED IN THE SAME TEST: the page that inherits ordinal 2 carries
/// the NEW member's component, not the deleted one's. Identity stayed in the component, so the
/// reused ordinal names a place and never an element. If a future reader resolved an element by its
/// ordinal, this is the test that would have to change -- and it would be changing to describe
/// silent corruption.
///
/// rust-internal: drives ZSetAdd/ZSetRemove/ZSetAdd, no external surface
#[test]
fn the_ordinal_names_a_position_and_a_delete_frees_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let key = "cpo-pos-zset";
    for m in 0..3u64 {
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: format!("m{m}").into_bytes(),
                score: m as f64,
            },
        );
    }
    let before = resident_ordinals_of(&engine, "zset", key);
    assert_eq!(
        before,
        vec![Some(0), Some(1), Some(2)],
        "the seed did not produce three distinct ordinals: {before:?}"
    );

    // Remove the one holding the highest ordinal.
    write(
        &engine,
        Command::ZSetRemove {
            key: key.to_string(),
            member: b"m2".to_vec(),
        },
    );
    // THE POSITIONS, READ WHERE POSITIONS LIVE. This read the LIVE INDEX ENTRIES and expected them
    // to fall to [0, 1]; they stay at [0, 1, 2], because a removal keeps the live entry over the
    // page its element vacated. The message said the fall happens "because `container_page_ordinal`
    // filters `deleted`" -- that walk is gone. The allocator reads the RESIDENT MAP, which holds
    // exactly the object's live elements, so that is the set a position claim is about and the one
    // that falls. The retained index entry is asserted below, as a cost rather than a position.
    let after_delete = resident_ordinals_of(&engine, "zset", key);
    assert_eq!(
        after_delete,
        vec![Some(0), Some(1)],
        "the delete left the live ELEMENTS at {after_delete:?}, so the position the allocator would \
         hand out next has not fallen"
    );
    let live_entries_after_delete = ordinals_of(&engine, "zset", key);
    assert_eq!(
        live_entries_after_delete,
        vec![Some(0), Some(1), Some(2)],
        "the live index entries are {live_entries_after_delete:?}. Three is the expected number: \
         the removal keeps the entry over the vacated page and adds a tombstone beside it. If this \
         is two, the retirement has landed and this is the assertion to come and restate"
    );
    // AND THE TOMBSTONE KEPT THE NUMBER, which is the fact the old mechanism had no way to express.
    let tombstoned_after_delete = tombstone_pages_of(&engine, "zset", key);
    assert_eq!(
        1,
        tombstoned_after_delete.len(),
        "DENOMINATOR: {} tombstone entries after one removal, so the reuse below is not being \
         measured against one removed element",
        tombstoned_after_delete.len()
    );
    assert_eq!(
        Some(2),
        tombstoned_after_delete[0].1,
        "the tombstone holds ordinal {:?} and the element it replaced held 2",
        tombstoned_after_delete[0].1
    );

    // A DIFFERENT member now.
    write(
        &engine,
        Command::ZSetAdd {
            key: key.to_string(),
            member: b"m9".to_vec(),
            score: 9.0,
        },
    );
    let after_insert = resident_ordinals_of(&engine, "zset", key);
    let live_entries_after_insert = ordinals_of(&engine, "zset", key);
    println!(
        "\n=== position === seeded {before:?} -> removed the highest {after_delete:?} -> inserted a \
         DIFFERENT member {after_insert:?}\n  live index entries after the insert: \
         {live_entries_after_insert:?}"
    );
    assert_eq!(
        after_insert,
        vec![Some(0), Some(1), Some(2)],
        "ordinal 2 was not reused: {after_insert:?}. A position is reusable; this is the \
         behaviour, and it is why the ordinal must never be read as an element's identity"
    );

    // AND THE REUSED ORDINAL NAMES THE NEW MEMBER, because identity lives in the component.
    let m9_component = crate::engine::execute_on_shard::zset_component(b"m9");
    let reused = ordinal_for_component(&engine, "zset", key, &m9_component)
        .expect("the new member is held");
    assert_eq!(
        reused,
        Some(2),
        "the new member did not take the freed ordinal 2; it holds {reused:?}"
    );

    // THE REMOVED MEMBER IS GONE FROM THE AUTHORITY, which is what makes the reuse one position
    // refilled rather than two live elements on one number.
    //
    // THIS WAS A VACUOUS ASSERTION AND IT PASSED. It counted entries in [`pages_of`] whose
    // component ended in `6d32` -- the hex of `m2` -- and [`pages_of`]'s component column had
    // become a hardcoded `None`, so the filter matched nothing and the count was 0 for a reason
    // that had nothing to do with m2. Asked of `shard.zsets` it is a real question.
    let m2_component = crate::engine::execute_on_shard::zset_component(b"m2");
    assert!(
        ordinal_for_component(&engine, "zset", key, &m2_component).is_none(),
        "the DELETED member m2 is still held by the resident map, so the reuse above is two live \
         elements sharing one ordinal rather than one position being refilled"
    );

    // AND THE LIVE INDEX HOLDS TWO ENTRIES AT ORDINAL 2, WHICH IS THE RETAINED-ENTRY COST AND NOT
    // A COLLISION. This is the arm that reads it, so it is pinned here rather than left to look
    // like something nobody noticed.
    //
    // The invariant and the cost are different quantities and they are asserted separately:
    //
    //   * ONE LIVE ELEMENT occupies ordinal 2 -- m9. That is the position invariant, and it is
    //     asked of the resident map because that is where elements live and where the allocator
    //     reads.
    //   * TWO LIVE INDEX ENTRIES carry ordinal 2 -- m9's, and the one m2's removal left standing
    //     over the page it vacated. A removal reaches the index through a door that matches
    //     `component.is_none()`, which a page-named entry cannot satisfy, so the entry stays and a
    //     tombstone is added beside it. Membership is unaffected: `container_membership` folds by
    //     append position and the tombstone is the later page.
    //
    // This assertion read `1 == at_two_live` over the live INDEX and would now fail at 2. Changing
    // the number alone would have recorded the cost as if it were the invariant; the invariant has
    // moved to the line above instead, where it is still 1.
    let tombstoned = tombstone_pages_of(&engine, "zset", key);
    let resident_at_two = resident_ordinals_of(&engine, "zset", key)
        .into_iter()
        .filter(|ordinal| *ordinal == Some(2))
        .count();
    let at_two_live = pages_of(&engine, "zset", key)
        .into_iter()
        .filter(|ordinal| *ordinal == Some(2))
        .count();
    let at_two_tombstoned = tombstoned
        .iter()
        .filter(|(_, ordinal)| *ordinal == Some(2))
        .count();
    println!(
        "  ordinal 2 is held by {resident_at_two} live ELEMENT(s), {at_two_live} live index \
         entry(ies) and {at_two_tombstoned} tombstone(s); tombstones: {tombstoned:?}"
    );
    assert_eq!(
        1, resident_at_two,
        "{resident_at_two} live elements hold ordinal 2, and exactly one element can occupy a \
         position"
    );
    assert_eq!(
        2, at_two_live,
        "{at_two_live} live INDEX entries hold ordinal 2. Two is the expected number and the cost \
         described above: m9's and the one m2's removal left standing. If this is 1 the retirement \
         of a vacated page's entry has landed, and this assertion is the one to come and restate"
    );
    assert_eq!(
        1, at_two_tombstoned,
        "{at_two_tombstoned} tombstones hold ordinal 2; the removed member's tombstone should be the \
         one, and it is what keeps the removal readable from the pages"
    );
    // The tombstone and the arrival differ in COMPONENT, which is where identity lives. If they
    // agreed, the tombstone would be saying the new member is gone.
    let tombstoned_component = tombstoned
        .iter()
        .find(|(_, ordinal)| *ordinal == Some(2))
        .and_then(|(component, _)| component.clone())
        .expect("the tombstone at ordinal 2 names a component");
    assert_ne!(
        m9_component, tombstoned_component,
        "the tombstone at ordinal 2 names the SAME component as the live arrival, so the pages say \
         the arrival was removed"
    );
    assert_eq!(
        m2_component, tombstoned_component,
        "the tombstone at ordinal 2 names {tombstoned_component:?} and the member removed was \
         {m2_component:?}, so the removal is recorded against the wrong element"
    );
}

// =================================================================================================
// 5. DELETION STILL FINDS ITS ROW
// =================================================================================================

/// DELETION STILL MATCHES BY COMPONENT, AND THE ORDINAL HAD NOTHING TO DO WITH IT.
///
/// THE ROW IT MATCHES IS A RESIDENT ELEMENT NOW, NOT AN INDEX ENTRY, and the claim survives the
/// move intact. This read "`mark_bucket_index_block_deleted_with` matches on `component`. The
/// component did not move, so this must be unchanged." That predicate is
/// `component.is_none()` today -- the entry's own name was always `None` by the time it was
/// translated, and the other side is the function's ARGUMENT -- so it matches the kinds that
/// converge on the object key and matches NOTHING for a container. A container's removal finds its
/// element through `recorded_map::remove_element`, by component, in the resident map.
///
/// So "unchanged" is still the claim worth testing, and the failure mode is still a delete that
/// removes the wrong element or none: what changed is which artefact holds the row.
///
/// AND THE DENOMINATOR IS ASSERTED ON BOTH SIDES OF THE MOVE: three live ELEMENTS after removing
/// one of four, four live INDEX ENTRIES because the removal keeps the one over the vacated page,
/// exactly one tombstone naming exactly the removed component, and the survivor set equal to the
/// members that were not deleted.
///
/// rust-internal: drives SetAdd x4 then SetRemove, no external surface
#[test]
fn deletion_still_finds_its_element_by_component() {
    // ITS INSTRUMENT USED TO ASK WHICH ENTRY NAMES A COMPONENT, and no entry carries one, so it
    // reported every member as missing -- including ones that are present and served. It asks
    // `state::resident_component_address`, which is the door the allocator and the removal both
    // use.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let key = "cpo-del-set";
    let members: Vec<Vec<u8>> = (0..4).map(|m| format!("member-{m}").into_bytes()).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let before = pages_of(&engine, "set", key);
    assert_eq!(before.len(), 4, "the seed wrote {} pages, not 4", before.len());

    let doomed = &members[2];
    let doomed_component = hex::encode(doomed);
    assert!(
        ordinal_for_component(&engine, "set", key, &doomed_component).is_some(),
        "the member about to be deleted has no page, so the delete below would prove nothing"
    );

    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: doomed.clone(),
        },
    );

    let after = pages_of(&engine, "set", key);
    let resident_after = resident_ordinals_of(&engine, "set", key);
    println!(
        "\n=== deletion === {} live elements before, {} after; {} live index entries after; the \
         deleted component is {}",
        before.len(),
        resident_after.len(),
        after.len(),
        if ordinal_for_component(&engine, "set", key, &doomed_component).is_none() {
            "gone"
        } else {
            "STILL PRESENT"
        }
    );
    // THE DELETE FOUND ITS ELEMENT, AND THAT IS ASKED OF THE RESIDENT MAP.
    //
    // This asserted three LIVE PAGES and its own message explained the four it did not expect:
    // "a removal now retains a tombstone entry, so counting every entry would report 4 here and
    // that would be the retention rather than a mismatched delete". That accounted for the
    // TOMBSTONE and not for the RETAINED LIVE ENTRY, which is a second entry and is not deleted,
    // so the live count is four and `pages_of`'s `deleted` filter cannot bring it to three.
    //
    // Both quantities are asserted, because they answer different questions: three live ELEMENTS
    // is the delete finding its row, and four live INDEX ENTRIES is the retention it leaves behind.
    assert_eq!(
        resident_after.len(),
        3,
        "the delete left {} live elements, not 3 -- it matched the wrong number of rows",
        resident_after.len()
    );
    assert_eq!(
        after.len(),
        4,
        "the delete left {} live index entries. Four is the expected number and the retention \
         described above: three survivors and the entry over the page the removed member vacated. \
         If this is 3 the retirement of a vacated page's entry has landed, and this assertion is \
         the one to come and restate",
        after.len()
    );
    assert!(
        ordinal_for_component(&engine, "set", key, &doomed_component).is_none(),
        "the deleted member is still held; deletion no longer finds its element by component"
    );
    // AND EXACTLY ONE TOMBSTONE, NAMING EXACTLY THAT COMPONENT.
    //
    // The new half of "deletion still finds its row": it has to find the right row to REMOVE and the
    // right row to RECORD, and a delete that matched the wrong component would now be wrong twice --
    // once in the live set and once in the pages. Asserting only the live count would miss the second.
    let tombstoned = tombstone_pages_of(&engine, "set", key);
    println!("  tombstone entries after the delete: {tombstoned:?}");
    assert_eq!(
        1,
        tombstoned.len(),
        "the delete left {} tombstone entries, not one, so the pages do not state exactly this one \
         removal",
        tombstoned.len()
    );
    assert_eq!(
        Some(doomed_component.clone()),
        tombstoned[0].0,
        "the tombstone names component {:?} and the member deleted was {doomed_component:?}, so the \
         removal is recorded against the wrong element",
        tombstoned[0].0
    );
    for (i, member) in members.iter().enumerate() {
        if i == 2 {
            continue;
        }
        assert!(
            ordinal_for_component(&engine, "set", key, &hex::encode(member)).is_some(),
            "member {i} was removed by a delete that named a different member"
        );
    }
}

// =================================================================================================
// 6. HLEN READS THE ENTRY COUNT, AND THE ENTRY COUNT DID NOT MOVE
// =================================================================================================

/// `HashLen` ANSWERS THE ENTRY COUNT, WHICH THIS CHANGE DOES NOT TOUCH.
///
/// `HashLen` is `bucket_index_component_block_addresses(shard, "hash", &key).len()`. That reader is
/// component-BLIND by signature and it is NOT count-blind, so a change that added or dropped an
/// entry would move HLEN's answer. This change assigns a field on an existing entry and adds none,
/// so HLEN must equal the number of distinct fields written -- before any ordinal existed and after.
///
/// Asserted against the number of fields the fixture wrote AND against the page count, so a drift
/// in either direction fails rather than cancelling out.
///
/// rust-internal: drives HashSet and HashLen, no external surface
#[test]
fn hash_len_still_answers_the_entry_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let key = "cpo-hlen";
    const FIELDS: i64 = 7;
    for f in 0..FIELDS {
        write(
            &engine,
            Command::HashSet {
                key: key.to_string(),
                field: format!("field-{f}"),
                value: format!("value-{f}").into_bytes(),
            },
        );
    }
    // A rewrite of an existing field must not add an entry either.
    write(
        &engine,
        Command::HashSet {
            key: key.to_string(),
            field: "field-0".to_string(),
            value: b"rewritten".to_vec(),
        },
    );

    let answered = match read(
        &engine,
        Command::HashLen {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    };
    let pages = pages_of(&engine, "hash", key).len() as i64;
    println!("\n=== HLEN === answered {answered}, pages {pages}, fields written {FIELDS}");
    assert_eq!(
        answered, FIELDS,
        "HLEN answered {answered} for {FIELDS} distinct fields. The ordinal assignment must not \
         change the entry COUNT, which is what this reader returns"
    );
    assert_eq!(
        pages, FIELDS,
        "the page count is {pages} for {FIELDS} fields, so HLEN and the index disagree"
    );
}

// =================================================================================================
// 7. THE PAGE STILL READS -- THE ONE WAY THIS COULD HAVE BEEN DATA LOSS
// =================================================================================================

/// A RELOADED CONTAINER STILL READS EVERY ELEMENT, AND KEEPS THE ORDINALS IT WAS WRITTEN WITH.
///
/// THIS IS THE TEST THAT MATTERS. `decode_block_record` refuses a page whose address block id and
/// record header block id differ ("page id mismatch"), and that arm is inert today only because both
/// sides are 0. Had the ordinal been stamped onto the index's copy of the address after the append
/// -- the obvious way to write this change -- every container page would still have had a header
/// saying 0, and every one of these reads would come back empty.
///
/// The unload/load cycle is what makes the read go through the decode path rather than a warm cache
/// or a resident map. Ordinals are compared across the cycle too: the address is on the wire, and
/// `fold_delta_block_items` restores it without reassigning, so a reload must not renumber.
///
/// rust-internal: drives the engine's own unload/load cycle, no external surface
#[test]
fn a_reloaded_container_still_reads_every_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let kinds = seed_one_of_each_kind(&engine);

    let before: Vec<(String, Vec<Option<u64>>)> = kinds
        .iter()
        .map(|(kind, key)| (kind.to_string(), ordinals_of(&engine, kind, key)))
        .collect();

    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    // EVERY hash field must come back with its own bytes. An unreadable page answers None here.
    let mut read_back = 0usize;
    for m in 0..MEMBERS {
        let field = format!("field-{m}");
        let want = format!("value-{m}").into_bytes();
        match read(
            &engine,
            Command::HashGet {
                key: "cpo-hash".to_string(),
                field: field.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                assert_eq!(
                    bytes, want,
                    "field {field} read back the wrong bytes after a reload"
                );
                read_back += 1;
            }
            other => panic!(
                "field {field} did not read back after a reload: {other:?}. A \"page id mismatch\" \
                 from `decode_block_record` reads exactly like this -- the page is refused and the \
                 answer is empty"
            ),
        }
    }
    assert_eq!(
        read_back, MEMBERS,
        "{read_back} of {MEMBERS} hash fields read back, not all of them"
    );

    // And the set's members all come back.
    let members = match read(
        &engine,
        Command::SetMembers {
            key: "cpo-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    assert_eq!(
        members.len(),
        MEMBERS,
        "the set read back {} of {MEMBERS} members after a reload",
        members.len()
    );

    let after: Vec<(String, Vec<Option<u64>>)> = kinds
        .iter()
        .map(|(kind, key)| (kind.to_string(), ordinals_of(&engine, kind, key)))
        .collect();
    println!("\n=== reload === {read_back} hash fields and {} set members read back", members.len());
    for ((kind, was), (_, now)) in before.iter().zip(after.iter()) {
        println!(
            "  {kind:<5} before={:?} after={:?}",
            was.iter().map(|o| o.map_or(-1i64, |v| v as i64)).collect::<Vec<_>>(),
            now.iter().map(|o| o.map_or(-1i64, |v| v as i64)).collect::<Vec<_>>()
        );
    }
    assert_eq!(
        before, after,
        "a reload renumbered the ordinals. The address carries the id on the wire and the fold \
         restores it without reassigning, so this must be identical"
    );
}

// =================================================================================================
// 8. THE CEILING
// =================================================================================================

/// PAST THE CEILING THE ORDINAL IS LEFT UNASSIGNED, RATHER THAN PANICKING OR SATURATING.
///
/// Driven on the helper directly, because reaching the ceiling through the engine means 65,536
/// writes to one object. The index is built by hand with one page already AT the ceiling, which is
/// the state the next insert has to cope with.
///
/// Both wrong answers are refused by name. PANICKING is what `set_block_id` does above the ceiling
/// and it would turn a large container into a crash where `main` serves it. SATURATING would hand
/// out 65,535 twice, and this tree's own doctrine on that field is that a saturated page id is a
/// legal page id for a DIFFERENT page of the same object.
///
/// rust-internal: drives the assignment helper on a hand-built index, no engine
#[test]
fn past_the_ceiling_the_ordinal_is_left_unassigned_rather_than_panicking() {
    use crate::block_store::{ElementEntry, MAX_ADDRESSABLE_BLOCK_ID};
    use crate::engine::state::ShardState;

    let kind = "set";
    let key = "cpo-ceiling";

    // BUILT ON THE RESIDENT MAP AND NOT ON A HAND-MADE INDEX, which is the restatement this arm
    // needed rather than a re-goldening. `container_page_ordinal` reads the map that is keyed by
    // the element now: a hand-built `CoreIndex` is no longer an input it consults at all, so the
    // old fixture would have left this test asserting the empty-object answer twice.
    let mut shard = ShardState::default();

    // One member of this object, already holding the highest ordinal the field can store.
    let at_ceiling = ElementEntry::try_from_parts(1, 0, 16, Some(MAX_ADDRESSABLE_BLOCK_ID), None)
        .expect("an address at the ceiling is constructible");
    assert_eq!(
        at_ceiling.block_id(),
        Some(MAX_ADDRESSABLE_BLOCK_ID),
        "the fixture member is not actually at the ceiling, so nothing below is at the boundary"
    );
    let resident_member = b"already-at-the-ceiling".to_vec();
    let resident_component = hex::encode(&resident_member);
    shard
        .sets
        .insert_element_for_test(key, resident_member.clone(), at_ceiling.clone());

    // A NEW member of the same object: one past the ceiling.
    let fresh_component = hex::encode(b"a-brand-new-member");
    let assigned =
        crate::engine::state::container_page_ordinal(&shard, kind, key, &fresh_component);
    println!(
        "\n=== ceiling === a member at {MAX_ADDRESSABLE_BLOCK_ID} is held; the next new member is \
         assigned {assigned} (saturating would say {MAX_ADDRESSABLE_BLOCK_ID})"
    );
    assert_eq!(
        assigned, 0,
        "past the ceiling the ordinal must be left at 0 -- the value the page would carry before \
         any of this -- so that an object with more than {MAX_ADDRESSABLE_BLOCK_ID} elements is \
         served exactly as it is today. It was assigned {assigned}"
    );
    assert_ne!(
        u64::from(assigned),
        MAX_ADDRESSABLE_BLOCK_ID,
        "the ordinal saturated at the ceiling, handing a second page the id {MAX_ADDRESSABLE_BLOCK_ID}"
    );

    // AND THE EXISTING MEMBER STILL ANSWERS ITS OWN ORDINAL, at the ceiling, unharmed.
    let held =
        crate::engine::state::container_page_ordinal(&shard, kind, key, &resident_component);
    assert_eq!(
        u64::from(held),
        MAX_ADDRESSABLE_BLOCK_ID,
        "the element already at the ceiling was re-assigned {held} on an overwrite rather than \
         keeping the position it holds"
    );

    // AND A COMPONENT THAT WILL NOT PARSE ANSWERS THE FRESH-ELEMENT PATH, NOT THE OVERWRITE ONE.
    //
    // The spelling table answers `None` for a name it cannot read rather than defaulting to the
    // empty member -- which is a real member whose ordinal it would otherwise have handed back.
    // Driven here because this is the one arm with a hand-made fixture to drive it on.
    let unreadable = crate::engine::state::container_page_ordinal(&shard, kind, key, "zz-not-hex");
    assert_eq!(
        0, unreadable,
        "a component that is not a hex member resolved to ordinal {unreadable}; an unreadable name \
         names nothing, and past the ceiling the fresh-element answer is 0"
    );
}
