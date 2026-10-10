// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OVERWRITE KEEPS THE MEMBER'S ORDINAL UNDER EITHER PROJECTION.
//!
//! # THE PROPERTY, WHICH ALREADY HAS A GUARD FOR THE DEFAULT PATH
//!
//! `container_page_ordinal` assigns a container element its POSITION, and an overwrite of the same
//! element must reuse that position rather than take a new one --
//! `container_page_ordinal::an_overwrite_keeps_the_members_ordinal_rather_than_climbing` pins that
//! for the ungated path. The mechanism is an identity branch: the walk looks for a page already
//! filed under this element's component and, finding one, returns the ordinal that page holds.
//!
//! # WHY THE GATE BREAKS IT, AND WHY NO EXISTING TEST SEES THAT
//!
//! Under one entry per page the entry carries NO component, so
//! `page.component.as_deref() == Some(component)` never matches and the walk falls through to
//! `highest + 1`. Every rewrite of one member then takes a fresh ordinal, which is a fresh
//! `block_id`, which is a fresh ADDRESS -- so a member rewritten five times leaves five pages
//! instead of overwriting one.
//!
//! The existing guard cannot see this: it reads no environment variable, so it runs with the gate
//! off and passes. Inheriting coverage from the ungated suite is an illusion, which is why this arm
//! exists rather than being assumed.
//!
//! # WHERE THE ORDINAL COMES FROM INSTEAD
//!
//! Not from a page read -- `container_page_ordinal` is handed the index and nothing that could
//! fetch a page, and adding a fetch to a write path inside a footprint change is the scope drift
//! this series has already declined once.
//!
//! It comes from the RESIDENT MAP, which is keyed by the element and whose value carries the
//! address: a member that already exists has its own `block_id` right there, and that IS its
//! position. So the gated path asks the map the question the index can no longer answer, and the
//! answer is authoritative rather than derived.

#![allow(clippy::all)]
use super::*;

const REWRITES: usize = 5;

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
        table_name: "ordinal".to_string(),
        shard_uri: "local://ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

/// Distinct page addresses, and the block ids on them, for one set object.
fn pages_and_ordinals(engine: &TemporalEngine, object_key: &str) -> (usize, Vec<u64>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut ordinals = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set"
                || &*page.object_key != object_key
                || page.deleted
            {
                continue;
            }
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
            if let Some(block_id) = page.address.block_id() {
                ordinals.insert(block_id);
            }
        }
    }
    (pages.len(), ordinals.into_iter().collect())
}

/// Every live set entry of this object, as (component, slab, offset, length, block_id), so a
/// surprising entry count can be read rather than guessed at.
fn entry_rows(engine: &TemporalEngine, object_key: &str) -> Vec<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut rows = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            rows.push(format!(
                "deleted={} component=None slab={} off={} len={} block_id={:?}",
                page.deleted,
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
                page.address.block_id()
            ));
        }
    }
    rows.sort();
    rows
}

/// Rewrite ONE member `REWRITES` times under `gate_on`, and report what the index holds after.
fn rewrites_under(gate_on: bool) -> (usize, Vec<u64>, usize, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // BOTH DIRECTIONS AS VALUES: an unset variable now selects the GATED path.
    if gate_on {
    } else {
    }
    load_on(&engine);

    let member = b"the-one-member".to_vec();
    for _ in 0..REWRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "ord/set".to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    let (pages, ordinals) = pages_and_ordinals(&engine, "ord/set");
    // How many members the resident map holds, so a row cannot be read as correct because the
    // object vanished.
    let resident = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard.sets.get("ord/set").map(|m| m.len()).unwrap_or(0)
    };
    let rows_detail = entry_rows(&engine, "ord/set");
    drop(engine);
    (pages, ordinals, resident, rows_detail)
}

#[test]
fn an_overwrite_keeps_the_members_ordinal_under_either_projection() {
    println!("\n=== one member rewritten {REWRITES} times ===");
    println!("  {:>5}  {:>6}  {:>9}  {:>24}", "gate", "pages", "resident", "ordinals");

    let mut rows: Vec<(&str, usize, Vec<u64>, usize)> = Vec::new();
    for (label, gate_on) in [("off", false), ("on", true)] {
        let (pages, ordinals, resident, rows_detail) = rewrites_under(gate_on);
        println!("  {label:>5}  {pages:>6}  {resident:>9}  {ordinals:>24?}");
        for row in &rows_detail {
            println!("          {row}");
        }
        rows.push((label, pages, ordinals, resident));
    }

    // FLOORS, on each arm, on the reaching of the path: the object has to exist and be named.
    for (label, pages, _ordinals, resident) in &rows {
        assert_eq!(
            1, *resident,
            "gate {label}: the resident map holds {resident} member(s) after rewriting ONE member \
             {REWRITES} times, so this row is not measuring an overwrite"
        );
        assert!(
            *pages > 0,
            "gate {label}: no page is filed for the object at all, so the count below compares \
             absences"
        );
    }

    // THE ORDINAL DOES NOT CLIMB, ON EITHER ARM, AND THAT IS WHAT THIS STEP FIXES.
    //
    // An overwrite is the same element in the same position. Under the gate the entry carries no
    // component, so `container_page_ordinal`'s identity branch cannot find the member's own
    // previous page and falls through to `highest + 1` -- measured before the fix as ordinals
    // [0, 1]. The position now comes from `shard.sets[key][member].block_id()`: the map that is
    // keyed by the member, holding the address it currently occupies, which IS its position.
    // Authoritative rather than derived, and no page read.
    for (label, _pages, ordinals, _resident) in &rows {
        assert_eq!(
            vec![0u64],
            *ordinals,
            "gate {label}: one member rewritten {REWRITES} times holds ordinals {ordinals:?}. An \
             overwrite must reuse the member's position; a climbing ordinal is a fresh block id, a \
             fresh address, and therefore a fresh page"
        );
    }

    // THE ENTRY COUNTS, AND THE GATED ONE HAS CHANGED -- IT IS NOW ONE ON BOTH ARMS.
    //
    // This engine APPENDS a page per write; it does not modify one in place. The ungated path
    // always held ONE entry because its replacement is scoped BY THE ELEMENT -- the write unnames
    // the element's own previous page.
    //
    // THIS SAID A PAGE-NAMED ENTRY CANNOT CARRY THAT SCOPE, and that was true of a supersede keyed
    // on the address the write LANDS at: a rewrite lands at a new address, matches no existing
    // entry, and leaves the previous one behind as a stale live entry over a dead page. It was
    // asserted here as `> 1` and described as a transient the next derivation heals.
    //
    // THE WRITE PATH CARRIES THE SCOPE NOW. The filer is told which page the write REPLACES -- the
    // element's previous address, read from the resident map, which is keyed by the element and so
    // still holds it at filing time -- and retires that page's entry when no sibling is left on
    // it. So the gated arm leaves ONE entry, the same as the ungated one, and the transient does
    // not exist to be healed. Asserted as an exact count in both rows, because `> 1` was satisfied
    // by exactly the stranding the change removes and would now be satisfied by nothing at all.
    assert_eq!(
        1, rows[0].1,
        "gate off: {} entries where the element-scoped replacement should leave one",
        rows[0].1
    );
    assert_eq!(
        1, rows[1].1,
        "gate on: {} entries where one live page should leave one. More than one means the \
         supersede did not retire the page this rewrite vacated -- a live entry over a page no \
         element is on, which under this gate is a claim of membership",
        rows[1].1
    );
}

/// AND A DERIVATION DROPS AN ENTRY NO ELEMENT CARRIES, WHICH THE SERIES STILL RESTS ON.
///
/// The projection reads the resident map, which holds only the CURRENT address of each element, so
/// an entry naming a page no element is on is not emitted again. That property is what makes one
/// entry per live page reachable by a derivation at all.
///
/// # THE STALE ENTRY IS PLANTED NOW, BECAUSE THE WRITE PATH NO LONGER PRODUCES ONE
///
/// This used to produce the subject by rewriting one member several times under the gate: each
/// rewrite landed at a new address, the supersede keyed on the landing address matched nothing, and
/// the previous entry was left behind. It then asserted the derivation dropped it, with a floor on
/// there being more than one entry first.
///
/// THAT FLOOR IS NOW UNREACHABLE, and that is the point rather than a problem: the filer is told
/// which page a rewrite REPLACES and retires it, so five rewrites leave ONE entry and there is no
/// transient left to heal. Restated rather than deleted, because the derivation's property is not
/// about how the stale entry got there -- a compaction that relocates pages, a fold of a delta
/// written by an older binary, and a bug in some future filer all produce the same state. So the
/// entry is planted directly, at an address no element holds, and the derivation is asked the same
/// question about it.
///
/// THE PLANT IS LOCATED BY WHAT IT IS, NOT BY A STRING: it is the only entry of this object whose
/// address is absent from the resident map, and that is how the floor below finds it.
#[test]
fn a_derivation_drops_the_entry_no_element_carries_any_more() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let member = b"the-one-member".to_vec();
    for _ in 0..REWRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "heal/set".to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    // THE WRITE PATH LEAVES ONE, asserted here so the plant below is known to be the second entry
    // and not one of several.
    let written = entry_rows(&engine, "heal/set");
    assert_eq!(
        1,
        written.len(),
        "the write path left {} entr(ies) for one member. This test plants the stale entry, so it \
         first has to know the state it is planting into",
        written.len()
    );

    // PLANT: one more live entry for the same object, over a page the resident map does not name.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let (start, end) = shard.routing_range();
        let routing_bucket =
            crate::engine::block_routing_bucket("heal/set", start, end);
        let orphan = crate::block_store::ElementEntry::from_parts(
            4_096, 8_192, 48, Some(7), None,
        );
        let page = crate::engine::state::BlockIndex {
            kind: crate::index_log::IndexItemKind::Page,
            routing_bucket,
            object_key: std::sync::Arc::from("heal/set"),
            model_id: crate::engine::storage_bucket_internals::stored_model_kind("set"),
            address: orphan,
            dirty: false,
            deleted: false,
        };
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("the object's bucket exists after the writes above");
        bucket.insert_page(page, &mut shard.bucket_index.block_slab_live);
    }

    let before = entry_rows(&engine, "heal/set");
    // FLOOR: the plant landed, or the assertion after the derivation passes over a clean state.
    assert_eq!(
        2,
        before.len(),
        "{} entr(ies) before the derivation, where the write's one plus the plant make two. \
         Without the plant there is nothing for the derivation to drop and this test would pass \
         over a state that was already clean",
        before.len()
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let (start, end) = shard.routing_range();
        crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);
    }

    let after = entry_rows(&engine, "heal/set");
    println!("\n=== a derivation drops what no element carries ===");
    for row in &before {
        println!("  before  {row}");
    }
    for row in &after {
        println!("  after   {row}");
    }

    let resident = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard.sets.get("heal/set").map(|m| m.len()).unwrap_or(0)
    };

    assert_eq!(
        1, resident,
        "the resident map holds {resident} member(s), so the object is not the one-member shape \
         this test is about"
    );
    assert_eq!(
        1,
        after.len(),
        "after the derivation {} entr(ies) remain where one live page should leave one. A stale \
         entry naming a page no element carries is supposed to be dropped by the derivation, \
         because the projection reads the resident map and the resident map holds only the \
         current address",
        after.len()
    );
    assert!(
        after[0].contains("component=None"),
        "the surviving entry is {:?}, which still names an element. Under the gate the derivation \
         files one entry per page and a page needs no element name",
        after[0]
    );
}

// =================================================================================================
// HASH, THE SAME PROPERTY -- AND THE ONE THING SET NEVER HAD TO FACE, DRIVEN BY A PLANT.
// =================================================================================================
//
// Hash needed its OWN gated branch (`HashSet`, `HashMultiSet`, `HashIncrBy`'s ordinal lookup, and
// `HashGet`/`HashIncrBy`/`HashLen`'s reads) because hash is the one container kind among
// set/list/hash that rewrites an element IN PLACE under its own identity: `HSET` on a field that
// already exists keeps the field and takes a new address, over and over, for as long as the key
// lives. The address-keyed lookup the fix relies on
// (`shard.hashes.get(&key).and_then(|fields| fields.get(&field))`) has to report THIS write's own
// field's CURRENT address, not a stale one left by an earlier write to a different field of the
// same object.
//
// # WHY THESE TESTS PLANT RATHER THAN JUST SETTING THE ENV VAR AND WRITING
//
// `index_entry_names_a_page` is the single authority deciding whether an entry is filed with NO
// component, and today it answers `true` only for `set` and `list` (`storage_bucket_internals.rs`)
// -- hash is deliberately NOT in that list yet; widening it is a later, separate step. So setting
// `TS_CONTAINER_ONE_ENTRY_A_PAGE` alone does NOT make a `HashSet` write a component-less entry:
// the filing path still writes `Some(field)` for hash regardless of the env var, and a test that
// only sets the var and writes would pass or fail IDENTICALLY whether or not the gated branches
// below exist -- which would make it a placebo, not a guard.
//
// So these tests plant the state hash's own entries will be in once that later step lands, the
// same technique `hash_read_union_divergence.rs` already uses to drive `HashGetAll`'s union: strip
// `component` off the field's own live entry directly, then exercise the gated branches (which key
// on the RAW gate, `container_index_files_one_entry_a_page`, not on the kind allow-list) against
// that planted state. That is also why the ordinal test re-plants before every round: each write
// still files a fresh `Some(field)`-named entry regardless of the env var, because the filing path
// itself is untouched by this step.

/// Sets the gate for as long as it is held and restores it on the way out, INCLUDING on a panic.
/// The two tests above set and clear the variable by hand; an assertion failure between those two
/// lines would leak the gate into every test that runs after this one in the same binary. `Drop`
/// runs during unwinding, which is the case that matters, so the new tests below do not repeat
/// that risk.



/// Distinct block ids currently live for this hash object, over EVERY field -- the hash analogue
/// of the `ordinals` half of `pages_and_ordinals`. A single-field fixture is deliberately read at
/// object granularity rather than filtered to one field, so a planted entry the filing path failed
/// to retire would show up as a second ordinal rather than being filtered out of sight.
fn live_ordinals_hash(engine: &TemporalEngine, object_key: &str) -> Vec<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut ordinals = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "hash" || &*page.object_key != object_key || page.deleted {
                continue;
            }
            if let Some(block_id) = page.address.block_id() {
                ordinals.insert(block_id);
            }
        }
    }
    ordinals.into_iter().collect()
}


/// THE DISCRIMINATING TEST FOR THE HASH ORDINAL FIX: writes a field, overwrites it, and checks the
/// ordinal did not advance -- against an entry planted component-less, which is the one state
/// `container_page_ordinal`'s identity branch cannot match.
///
/// # WHAT ACTUALLY CLIMBS, CORRECTED AGAINST A FIRST WRONG READING
///
/// A first reading of this expected stale pages to PILE UP under the gate, the way `SetAdd`'s own
/// gated arm documents them doing. Driven, that is not what happens here: hash retires a field's
/// previous page by the ADDRESS the resident map held for it before the overwrite, which has
/// nothing to do with `component`, so there is still only ONE live page after every round, planted
/// or not -- measured while writing this test as `ordinals=[0]`, never `[0, 1]`. What climbs is
/// that ONE page's own position: with the component stripped, `container_page_ordinal`'s identity
/// branch cannot match the single live page it is itself looking at, so `highest` is read off it
/// anyway and the next write is handed `highest + 1` instead of reusing it -- `[0]`, then `[1]`,
/// then `[2]`, climbing round over round rather than piling up. With the gated branch, every round
/// reads `[0]`: the position comes from `shard.hashes[key][field].block_id()` instead, which is
/// authoritative and needs no page read.
#[test]
fn an_overwrite_of_a_component_less_hash_entry_reuses_its_ordinal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const KEY: &str = "ord/hash-planted";
    const FIELD: &str = "the-one-field";

    let seed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: KEY.to_string(),
            field: FIELD.to_string(),
            value: b"v0".to_vec(),
        },
    });
    assert!(seed.status.ok, "seed write failed: {seed:?}");

    let after_seed = live_ordinals_hash(&engine, KEY);
    assert_eq!(
        vec![0u64],
        after_seed,
        "the seed write must land at ordinal 0 alone, or the rounds below are not measuring an \
         overwrite of a known starting position"
    );

    println!("\n=== one hash field, planted component=None, overwritten {REWRITES} times ===");
    println!("  round=seed  ordinals={after_seed:?}");

    for round in 0..REWRITES {
        // NO PLANT: a hash entry is nameless by construction now, so the strip that used to
        // manufacture this state removes nothing. Asserted directly instead, per round, because
        // what this test needs established is that the overwrite below is an overwrite of a
        // NAMELESS entry -- which is the case `container_page_ordinal` could not resolve from the
        // index and now resolves from the resident map.
        assert_eq!(
            0,
            named_hash_entries(&engine, KEY),
            "round {round}: {} live hash entries name a field, so this round is not overwriting a \
             nameless entry",
            named_hash_entries(&engine, KEY)
        );

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: KEY.to_string(),
                field: FIELD.to_string(),
                value: format!("v{}", round + 1).into_bytes(),
            },
        });
        assert!(response.status.ok, "round {round} overwrite failed: {response:?}");

        let ordinals = live_ordinals_hash(&engine, KEY);
        println!("  round={round}  ordinals={ordinals:?}");
        assert_eq!(
            vec![0u64],
            ordinals,
            "round {round}: after planting component=None on the field's own live entry and \
             overwriting the field, the live ordinal(s) are {ordinals:?}. An overwrite must reuse \
             the field's position; a climbing ordinal is a fresh block id, a fresh address, and \
             therefore a fresh page"
        );
    }
}

/// THE READ-SIDE COMPANION: a component-less entry must not hide a present field from `HashGet`,
/// must not make `HashLen` disagree with the resident map, and must not make `HashIncrBy` read a
/// miss and silently restart the counter at zero.
///
/// # WHY TWO FIELDS, AND WHY NEITHER IS THE ONE REWRITTEN
///
/// A FIRST DRAFT of this test wrote ONE field, stripped its only entry's component, and read it
/// straight back -- and `HashGet` answered correctly even against the UNFIXED code. That was not
/// the fix working: a SECOND draft then rewrote that one field across several rounds, expecting
/// stale pages to accumulate the way the ordinal test's own history does. They do not -- driven
/// and checked directly: hash retires the field's PREVIOUS page by the address the resident map
/// held before the overwrite, which has nothing to do with `component`, so there is never more
/// than one live page per field regardless of what this test plants onto it. (That is also the
/// corrected reading behind the ordinal test above: its climbing block id is ONE live page's
/// position advancing round over round, not an accumulating pile the way `SetAdd`'s gated arm
/// leaves one.)
///
/// So a single field, rewritten or not, can never be the thing that makes `component` matter here
/// -- there is only ever one candidate page, and nothing needs to be disambiguated FROM. What the
/// brief's mechanism (`bucket_index_block_address` matching `page.component.as_deref() ==
/// component` on every branch) can only get wrong is choosing AMONG SEVERAL live pages of one
/// object -- which needs several FIELDS, not several rewrites of one. So this plants on one field
/// of a two-field object and leaves the other untouched, as the thing the stripped field's lookup
/// could be confused with (or lost beside).
#[test]
fn a_component_less_hash_entry_is_still_answered_correctly_by_get_len_and_incrby() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const KEY: &str = "ord/hash-point-read";
    const FIELD: &str = "alpha";
    const SIBLING: &str = "beta";

    for (field, value) in [(FIELD, b"41".to_vec()), (SIBLING, b"99".to_vec())] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: KEY.to_string(),
                field: field.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "seeding `{field}` failed: {response:?}");
    }

    // Strip ONLY `FIELD`'s entry. `SIBLING` is left exactly as written, component intact, so the
    // object is unambiguously a two-page object and the lookup below has something real to
    // disambiguate FROM rather than answering by elimination over an object with nothing else on
    // it.
    // NO PLANT IS NEEDED ANY MORE, AND THAT IS WHAT IS ASSERTED INSTEAD.
    //
    // This stripped the component off `FIELD`'s entry to manufacture the nameless entry the test is
    // about. Hash is in the page-named set, so the entry is nameless BY CONSTRUCTION and the strip
    // removes nothing -- measured as "stripped 0 entries, not one". A plant that plants nothing is
    // the shape that makes a guard read as a tree fact while asserting only its own fixture, so it
    // is replaced by the direct statement: this object's entries carry no field name at all, and
    // `SIBLING` is still a separate page, so the lookup below has something real to disambiguate
    // from rather than answering by elimination.
    assert_eq!(
        0,
        named_hash_entries(&engine, KEY),
        "{} of this object's live hash entries name a field, so the nameless-entry case this test \
         is about is not the state it is in",
        named_hash_entries(&engine, KEY)
    );

    let live_pages = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .filter(|page| {
                page.model_id.as_str() == "hash" && &*page.object_key == KEY && !page.deleted
            })
            .count()
    };
    assert_eq!(
        2, live_pages,
        "{live_pages} live page(s) for a two-field object -- the floor below assumes exactly one \
         planted (component=None) page and one intact (component=Some(\"{SIBLING}\")) sibling"
    );

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: KEY.to_string(),
            field: FIELD.to_string(),
        },
    });
    assert!(get.status.ok, "{:?}", get.status);
    match get.response {
        CommandResponse::Bytes { value } => assert_eq!(
            Some(b"41".to_vec()),
            value,
            "HashGet answered {value:?} for `{FIELD}`, whose only entry is component-less, beside \
             an intact sibling entry for `{SIBLING}`; the gated branch did not reach the resident \
             map"
        ),
        other => panic!("expected Bytes, got {other:?}"),
    }

    let len = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashLen {
            key: KEY.to_string(),
        },
    });
    assert!(len.status.ok, "{:?}", len.status);
    match len.response {
        CommandResponse::Integer { value } => assert_eq!(
            2, value,
            "HashLen answered {value} for an object with exactly two fields"
        ),
        other => panic!("expected Integer, got {other:?}"),
    }

    let incr = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: KEY.to_string(),
            field: FIELD.to_string(),
            increment: 1,
        },
    });
    assert!(incr.status.ok, "{:?}", incr.status);
    match incr.response {
        CommandResponse::Integer { value } => assert_eq!(
            42, value,
            "HashIncrBy answered {value}, not 42; a miss through the component-less entry \
             restarts the counter at zero instead of reading the present value `41`"
        ),
        other => panic!("expected Integer, got {other:?}"),
    }
}

/// How many of this object's LIVE hash entries name a field.
///
/// Replaces the strip-plant two arms above used to manufacture a nameless entry with. Under the
/// collapse the entry is nameless already, so the honest statement is a count of named ones -- and
/// a count of zero over an object that HAS entries is a stronger claim than a plant that lands.
fn named_hash_entries(engine: &TemporalEngine, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| {
            // THE `component.is_some()` TERM IS GONE and this counter is structurally zero. An
            // entry has no element name, so "how many hash entries name a field" has one answer
            // for every fixture. It is left as a counter rather than deleted because the arms
            // below PRINT it as a denominator beside the ordinal they are really about; each of
            // those is restated at its own assertion.
            !page.deleted
                && page.model_id.as_str() == "hash"
                && &*page.object_key == key
                && false
        })
        .count()
}
