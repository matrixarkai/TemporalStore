// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MAKES A PAGE-DERIVED MEMBERSHIP REFUSABLE, AND WHAT MAKES COLLECTING A TOMBSTONE SAFE.
//!
//! `container_tombstone_collection` settled the page FORMAT and the fold -- what a removal encodes,
//! and that the fold honours append order. It also wrote `may_drop_tombstones` and drove the
//! resurrection the naive rule causes. What it could NOT do is make either of those matter, because
//! nothing in production called the fold and nothing read `live_only_pages`. This module drives the
//! two things that changed.
//!
//! # 1. A PAGE OF THE FIRST SHAPE IS NOW REFUSABLE, AND IT WAS NOT
//!
//! `live_only_pages` had THREE occurrences in the tree -- the field, its increment, and a `println!`
//! in one test -- so no production decision read it. A derivation over a v1 page is a SUPERSET of the
//! membership: the shape cannot state a removal, so every element any page ever named live comes back,
//! including the ones a removal retired. And `is_complete` answered TRUE while that happened, because
//! nothing had FAILED -- the page read perfectly and walked perfectly.
//!
//! Driven in BOTH DIRECTIONS with the page counts printed beside the verdict, because "it refused" is
//! also what a derivation that refuses everything looks like.
//!
//! # 2. THE REFUSAL AND "THERE WERE NO PAGES" ARE DIFFERENT FACTS
//!
//! Both hand the caller `None`. `IndexLoadPath` exists because the same collision on the index load
//! path made a stamp refusal invisible -- "a `return Ok(None)` that the caller cannot tell from an
//! absent index" -- and the fix there was a counter per arm. Same fix, and the test is that the two
//! COUNTERS differ while the two return values do not.
//!
//! # 3. THE UNIT OF THE RULE'S TWO COUNTS IS A DISTINCT PAGE, AND AN ELEMENT COUNT RESURRECTS
//!
//! This is the trap found while wiring it, and it is the reason the compactor grew a set rather than
//! reusing a number it already had. `pages_folded` is `destinations.len()` summed -- ONE PER ELEMENT --
//! and #2027's entire purpose is that several elements share one page, so it exceeds the page count
//! exactly where folding worked. Fed to `may_drop_tombstones` as `pages_being_rewritten` it can
//! COINCIDE with the container's page count while a page survives, and that page can be one naming the
//! removed element live. Driven as a resurrection that happens, not as a rule that forbids it.
//!
//! # 4. THE COLLECTION GATE IS A CONJUNCTION, AND EACH TERM IS LOAD-BEARING
//!
//! `tombstones_collectable` ANDs the rule with the derivation being TRUSTED and with the pages
//! agreeing that each component being dropped is gone. Each term is driven alone against facts that
//! satisfy the other two, so a guard cannot pass against a gate that dropped one of them.

#![allow(clippy::all)]
use super::*;
use crate::block_store::BlockAddress;
use crate::engine::container_membership::{
    append_position, derive_membership, derive_trusted_membership, may_drop_tombstones,
    membership_derive_path_counts, reset_membership_derive_path_counts, tombstones_collectable,
    ContainerRoundFacts,
};
use crate::engine::container_pages::{
    encode_container_page_items, ContainerPageWrite, ElementKeySpelling, CONTAINER_PAGE_MAGIC,
    CONTAINER_PAGE_MAGIC_V2,
};

/// An address at a chosen append position, which is what orders pages.
///
/// The same fixture `container_tombstone_collection` uses, and for its stated reason: a real page
/// carries a block id and an object id, so a fixture without them exercises a shape the write path
/// does not produce. Neither participates in the ordering.
fn at(slab: u64, offset: u64, len: u32) -> BlockAddress {
    BlockAddress::from_parts(slab, offset, u64::from(len), Some(0), Some(7))
}

/// The same page bytes, relabelled as the FIRST shape.
///
/// By rewriting the magic rather than by a second encoder, which is the technique
/// `a_live_suffix_page_is_the_same_bytes_under_either_shape` established and the reason it works: a
/// live suffix item is byte-identical between the shapes, so this produces exactly the page a
/// pre-#2040 store holds rather than an approximation of one. There is no v1 ENCODER to call --
/// `encode_container_page` writes the second magic unconditionally -- which is itself the fact that
/// nothing migrates a v1 page forward.
fn as_first_shape(page: &[u8]) -> Vec<u8> {
    let mut relabelled = page.to_vec();
    relabelled[..CONTAINER_PAGE_MAGIC.len()].copy_from_slice(CONTAINER_PAGE_MAGIC);
    relabelled
}

/// Read a page out of a fixed set, by append position, the way the fold's closure is fed.
fn read_from(set: Vec<(BlockAddress, Vec<u8>)>) -> impl FnMut(&BlockAddress) -> Option<Vec<u8>> {
    move |address: &BlockAddress| -> Option<Vec<u8>> {
        set.iter()
            .find(|(candidate, _)| append_position(candidate) == append_position(address))
            .map(|(_, page)| page.clone())
    }
}

/// rust-internal: operates on the page codec and the fold directly
#[test]
fn a_live_only_page_makes_the_derivation_a_superset_and_the_caller_refuses_it() {
    reset_membership_derive_path_counts();
    let kept = b"member-kept".to_vec();
    let retired = b"member-retired".to_vec();
    let kept_component = hex::encode(&kept);
    let retired_component = hex::encode(&retired);

    // ONE PAGE NAMING TWO LIVE MEMBERS, which is what a #2022/#2027 store holds. One of the two has
    // since been removed -- and in a pre-#2040 store that removal reached the index and NOT the page,
    // so the page still names it. There is no tombstone page to find, which is the whole problem: the
    // page set is a superset of the membership and nothing in it says otherwise.
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[
            ContainerPageWrite::live(&kept, &kept),
            ContainerPageWrite::live(&retired, &retired),
        ],
    );
    assert!(
        page.starts_with(CONTAINER_PAGE_MAGIC_V2),
        "DENOMINATOR: the encoder did not produce a second-shape page, so the arms below compare \
         nothing"
    );
    let v1 = as_first_shape(&page);
    let address = at(1, 100, page.len() as u32);

    // ---- DIRECTION ONE: the FIRST shape. The fold completes and is a superset. ----
    let over_v1 = derive_membership("set", vec![address.clone()], read_from(vec![(address.clone(), v1.clone())]));
    println!(
        "first shape:  {} pages read, {} live-only, {} live, {} removed, complete={}, authoritative={}",
        over_v1.pages_read,
        over_v1.live_only_pages,
        over_v1.live.len(),
        over_v1.removed.len(),
        over_v1.is_complete(),
        over_v1.is_authoritative()
    );
    assert_eq!(1, over_v1.pages_read, "DENOMINATOR: the v1 page was not read");
    assert_eq!(1, over_v1.live_only_pages, "the v1 page was not counted as live-only");
    assert!(
        over_v1.is_complete(),
        "THE PREMISE OF THIS WHOLE GUARD IS THAT NOTHING FAILED. If `is_complete` is false here then \
         the v1 page is being read as a read failure, and the refusal below would be the incomplete \
         arm rather than the live-only one -- a different mechanism passing for this one."
    );
    assert!(
        over_v1.live.contains_key(&retired_component),
        "the fold did not name the retired member, so this fixture is not a superset and proves nothing"
    );
    assert!(
        !over_v1.is_authoritative(),
        "A COMPLETE DERIVATION OVER A V1 PAGE CALLED ITSELF AUTHORITATIVE. That is the silent \
         resurrection this stage exists to close: the page cannot state a removal, so its silence \
         about the retired member means nothing, and acting on this membership puts the member back."
    );
    assert!(
        derive_trusted_membership("set", vec![address.clone()], read_from(vec![(address.clone(), v1)]))
            .is_none(),
        "the production entry point handed back a membership derived over a v1 page"
    );

    // ---- DIRECTION TWO: the SECOND shape, same bytes, same items. Trusted. ----
    let trusted = derive_trusted_membership(
        "set",
        vec![address.clone()],
        read_from(vec![(address.clone(), page.clone())]),
    )
    .expect(
        "A V2-ONLY CONTAINER WAS REFUSED. Without this direction the guard above passes against a \
         `derive_trusted_membership` that refuses everything, which would make the wiring dead code.",
    );
    println!(
        "second shape: {} pages read, {} live-only, {} live, authoritative={}",
        trusted.pages_read,
        trusted.live_only_pages,
        trusted.live.len(),
        trusted.is_authoritative()
    );
    assert_eq!(0, trusted.live_only_pages);
    assert!(trusted.live.contains_key(&kept_component));

    // BOTH ARMS COUNTED, which is the requirement `IndexLoadPath` was created for.
    let (trusted_count, live_only, incomplete, no_pages) = membership_derive_path_counts();
    println!(
        "paths: trusted={trusted_count} refused-live-only={live_only} refused-incomplete={incomplete} no-pages={no_pages}"
    );
    assert_eq!(
        (1, 1, 0, 0),
        (trusted_count, live_only, incomplete, no_pages),
        "the two derivations did not land one in each arm"
    );
}

/// rust-internal: operates on the fold directly
#[test]
fn a_refusal_and_a_container_with_no_pages_are_told_apart_by_the_counter_not_the_return() {
    reset_membership_derive_path_counts();
    let member = b"m".to_vec();
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    let v1 = as_first_shape(&page);
    let address = at(2, 64, page.len() as u32);

    // A REFUSAL.
    let refused = derive_trusted_membership(
        "set",
        vec![address.clone()],
        read_from(vec![(address.clone(), v1)]),
    );
    // AND A CONTAINER WITH NOTHING IN IT.
    let empty = derive_trusted_membership("set", Vec::new(), read_from(Vec::new()));

    println!("refused={:?} empty={:?}", refused.is_none(), empty.is_none());
    assert!(
        refused.is_none() && empty.is_none(),
        "DENOMINATOR: the two must return the SAME thing, or the counters are not what distinguishes them"
    );

    let (trusted, live_only, incomplete, no_pages) = membership_derive_path_counts();
    println!("paths: trusted={trusted} live-only={live_only} incomplete={incomplete} no-pages={no_pages}");
    assert_eq!(
        (0, 1, 0, 1),
        (trusted, live_only, incomplete, no_pages),
        "A REFUSAL AND AN EMPTY CONTAINER LANDED IN THE SAME COUNTER. They return the same value on \
         purpose, so the counter is the only thing that tells a store whose tombstones can NEVER be \
         collected from one that has none to collect -- which is the exact collision \
         `index_load_path_counts` was added to fix on the index load path."
    );
}

/// rust-internal: operates on the fold directly
#[test]
fn a_derivation_that_is_both_incomplete_and_live_only_counts_once_as_incomplete() {
    reset_membership_derive_path_counts();
    let member = b"m".to_vec();
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    let v1 = as_first_shape(&page);
    let readable = at(3, 10, page.len() as u32);
    // A SECOND page the reader will not answer for, so the derivation is incomplete AS WELL AS
    // live-only. Exactly one arm may be counted, or the four counters stop summing to the number of
    // derivations and a guard cannot floor them against a denominator it drove itself.
    let unreadable = at(3, 20, 32);

    let derived = derive_trusted_membership(
        "set",
        vec![readable.clone(), unreadable],
        read_from(vec![(readable, v1)]),
    );
    assert!(derived.is_none());
    let (trusted, live_only, incomplete, no_pages) = membership_derive_path_counts();
    println!("paths: trusted={trusted} live-only={live_only} incomplete={incomplete} no-pages={no_pages}");
    assert_eq!(
        1,
        trusted + live_only + incomplete + no_pages,
        "ONE DERIVATION NOTED MORE THAN ONE ARM, so the counters no longer sum to the derivations"
    );
    assert_eq!(
        1, incomplete,
        "a derivation that was both incomplete and live-only was counted as live-only. The order is \
         part of the contract: incomplete is the stronger statement, because such a derivation is \
         unusable whatever the shapes were, while a complete v1 derivation is a precisely known \
         superset."
    );
}

/// rust-internal: operates on the fold and the collection rule directly
#[test]
fn an_element_count_where_the_rule_expects_a_page_count_permits_a_resurrection() {
    // THE TRAP, DRIVEN AS THE RESURRECTION IT CAUSES.
    //
    // Container pages: A holds TWO live members, B holds the removed member (still named live, because
    // the removal appended a tombstone and did not rewrite B) AND one more live member, T is the
    // tombstone. So the container has THREE pages and TWO live pages.
    //
    // The round relocates A's two elements and `should_relocate` declines B's one. That is ONE
    // distinct live page rewritten -- and TWO elements. The element count equals the LIVE PAGE COUNT
    // by coincidence, which is what makes the rule say the rewrite was total while B survives.
    let a_members: Vec<Vec<u8>> = vec![b"x".to_vec(), b"y".to_vec()];
    let retired = b"m".to_vec();
    let survivor = b"w".to_vec();
    let retired_component = hex::encode(&retired);

    let page_a = encode_container_page_items(
        ElementKeySpelling::Hex,
        &a_members
            .iter()
            .map(|member| ContainerPageWrite::live(member, member))
            .collect::<Vec<_>>(),
    );
    // B STILL NAMES THE REMOVED MEMBER AS LIVE, which is the state a removal leaves: the removal
    // appended a tombstone page and did not rewrite B, because B stays live for `w`.
    let page_b = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[
            ContainerPageWrite::live(&retired, &retired),
            ContainerPageWrite::live(&survivor, &survivor),
        ],
    );
    let tomb = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(&retired)],
    );

    let a_at = at(1, 100, page_a.len() as u32);
    let b_at = at(1, 200, page_b.len() as u32);
    let tomb_at = at(1, 300, tomb.len() as u32);
    let all = vec![
        (a_at.clone(), page_a.clone()),
        (b_at.clone(), page_b.clone()),
        (tomb_at.clone(), tomb.clone()),
    ];

    // WITH THE TOMBSTONE, the member is correctly gone -- the control, so the resurrection below is
    // the tombstone's absence and not the fixture simply never removing anything.
    let before = derive_membership(
        "set",
        vec![a_at.clone(), b_at.clone(), tomb_at.clone()],
        read_from(all.clone()),
    );
    println!(
        "with the tombstone:    {} pages, live={:?}",
        before.pages_read,
        before.live.keys().collect::<Vec<_>>()
    );
    assert_eq!(3, before.pages_read, "DENOMINATOR: three pages must be read");
    assert!(
        before.removed.contains(&retired_component) && !before.live.contains_key(&retired_component),
        "CONTROL: the tombstone did not remove the member, so this fixture cannot show a resurrection"
    );

    // DROP THE TOMBSTONE WHILE B SURVIVES, which is what the element count authorises. B is still in
    // the page set because `w`'s entry points at it.
    let after = derive_membership("set", vec![a_at, b_at, tomb_at], {
        let kept: Vec<(BlockAddress, Vec<u8>)> = all
            .iter()
            .filter(|(address, _)| append_position(address) != (1, 300))
            .cloned()
            .collect();
        move |address: &BlockAddress| -> Option<Vec<u8>> {
            kept.iter()
                .find(|(candidate, _)| append_position(candidate) == append_position(address))
                .map(|(_, page)| page.clone())
        }
    });
    println!(
        "tombstone dropped:     {} pages read, {} read failures, live={:?}",
        after.pages_read,
        after.read_failures,
        after.live.keys().collect::<Vec<_>>()
    );
    assert!(
        after.live.contains_key(&retired_component),
        "THE RESURRECTION DID NOT HAPPEN, so this guard is asserting nothing. Dropping the tombstone \
         while B survives naming the member live must bring it back -- if it does not, the fixture no \
         longer models the case the page count protects against."
    );

    // ---- AND NOW THE TWO COUNTS, THROUGH THE GATE'S OWN ARITHMETIC ----
    //
    // The gate asks `may_drop_tombstones(live + tombstone, rewritten + tombstone, batches)`. The only
    // term in dispute is `rewritten`: the honest one is DISTINCT PAGES (A alone, so 1) and the wrong
    // one is what `pages_folded` reports, one per ELEMENT (x and y, so 2).
    let live_pages = 2usize;
    let tombstone_pages = 1usize;
    let elements_relocated = a_members.len();
    let distinct_pages_relocated = 1usize;
    println!(
        "live_pages={live_pages} tombstone_pages={tombstone_pages} \
         elements_relocated={elements_relocated} distinct_pages_relocated={distinct_pages_relocated}"
    );
    assert_eq!(
        live_pages, elements_relocated,
        "DENOMINATOR: the element count must COINCIDE with the live page count, or this fixture is \
         not the trap -- the whole point is that two elements off one page read as two pages"
    );
    assert!(
        may_drop_tombstones(
            live_pages + tombstone_pages,
            elements_relocated + tombstone_pages,
            1
        ),
        "THE ELEMENT COUNT DOES NOT SATISFY THE RULE here, so this guard is not driving the trap and \
         the assertion below proves nothing about the unit"
    );
    assert!(
        !may_drop_tombstones(
            live_pages + tombstone_pages,
            distinct_pages_relocated + tombstone_pages,
            1
        ),
        "THE PAGE COUNT ALSO SATISFIED THE RULE. Then the rule permits the resurrection driven above, \
         and counting distinct source pages in the compactor buys nothing."
    );

    // AND THROUGH THE GATE ITSELF, with the honest facts, so the protection is asserted where
    // production actually asks for it rather than only on the rule underneath.
    let trusted = derive_trusted_membership(
        "set",
        vec![at(1, 300, tomb.len() as u32)],
        read_from(vec![(at(1, 300, tomb.len() as u32), tomb.clone())]),
    )
    .expect("DENOMINATOR: the tombstone page must derive, or the gate refuses for the wrong reason");
    assert!(
        !tombstones_collectable(
            &ContainerRoundFacts {
                live_pages,
                live_pages_rewritten: distinct_pages_relocated,
                tombstone_pages,
                batches: 1,
            },
            Some(&trusted),
            &[retired_component],
        ),
        "the production gate collected a tombstone while page B survived naming its element live"
    );
}

/// rust-internal: operates on the collection gate directly
#[test]
fn the_collection_gate_refuses_a_live_only_page_even_when_the_rewrite_was_total() {
    // FACTS THAT SATISFY THE RULE OUTRIGHT: one live page, it was rewritten, one tombstone page, one
    // batch. The rule says yes; the gate must still say no, because the derivation was refused.
    let facts = ContainerRoundFacts {
        live_pages: 1,
        live_pages_rewritten: 1,
        tombstone_pages: 1,
        batches: 1,
    };
    assert!(
        may_drop_tombstones(
            facts.live_pages + facts.tombstone_pages,
            facts.live_pages_rewritten + facts.tombstone_pages,
            facts.batches
        ),
        "DENOMINATOR: these facts do not satisfy the rule, so refusing them below says nothing about \
         the derivation term"
    );

    let component = hex::encode(b"m");
    assert!(
        !tombstones_collectable(&facts, None, &[component.clone()]),
        "THE GATE COLLECTED ON A REFUSED DERIVATION. `None` is what a `LiveOnly` page produces, and a \
         v1 page cannot state a removal -- so the pages still name the member live, dropping the \
         tombstone removes the only statement that it is gone, and the next derivation puts it back. \
         This is the term that makes the collection depend on Job 1's refusal rather than merely \
         coexist with it."
    );

    // AND THE POSITIVE DIRECTION, so the guard above is not passing against a gate that refuses
    // everything.
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(b"m")],
    );
    let address = at(5, 10, page.len() as u32);
    let trusted = derive_trusted_membership("set", vec![address.clone()], read_from(vec![(address, page)]))
        .expect("DENOMINATOR: a tombstone-only v2 page must derive");
    assert!(
        trusted.removed.contains(&component),
        "DENOMINATOR: the fold did not read the tombstone, so the agreement term below is vacuous"
    );
    assert!(
        tombstones_collectable(&facts, Some(&trusted), &[component.clone()]),
        "the gate refused facts that satisfy every term, so nothing can ever be collected"
    );

    // AND THE AGREEMENT TERM ALONE: a component the pages do NOT say is gone.
    assert!(
        !tombstones_collectable(&facts, Some(&trusted), &[hex::encode(b"not-mentioned")]),
        "THE GATE DROPPED A TOMBSTONE FOR A COMPONENT THE PAGES DO NOT AGREE IS GONE. A tombstone \
         entry whose component is not in `removed` means some page states that element LIVE later \
         than the tombstone -- the re-add sequence -- and collecting the container on the strength of \
         the round being total while treating the element as removed is not sound."
    );

    // AND THE EMPTY LIST, which must not read as a vacuous success.
    assert!(
        !tombstones_collectable(&facts, Some(&trusted), &[]),
        "THE GATE SAID YES WITH NOTHING TO DROP. `all` over an empty slice is TRUE, so without its own \
         check the gate reports a collection for every container that has no tombstone at all -- and \
         `a_vacuity_guard_belongs_on_the_scan` is the recorded shape of that mistake."
    );
}

/// rust-internal: operates on the collection gate directly
#[test]
fn the_collection_gate_refuses_a_round_that_left_a_live_page_behind_or_split_its_batches() {
    let component = hex::encode(b"m");
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(b"m")],
    );
    let address = at(6, 10, page.len() as u32);
    let trusted = derive_trusted_membership("set", vec![address.clone()], read_from(vec![(address, page)]))
        .expect("DENOMINATOR: a tombstone-only v2 page must derive");

    // A LIVE PAGE LEFT BEHIND. Two live pages, one rewritten -- the surviving one may be the one that
    // names the member live, which is the resurrection `may_drop_tombstones` exists for.
    let left_behind = ContainerRoundFacts {
        live_pages: 2,
        live_pages_rewritten: 1,
        tombstone_pages: 1,
        batches: 1,
    };
    assert!(
        !tombstones_collectable(&left_behind, Some(&trusted), &[component.clone()]),
        "the gate collected while a live page of the container was not rewritten"
    );

    // TWO BATCHES. Every page was rewritten, and into SEVERAL pages -- a tombstone dropped into batch
    // one is not seen by batch two.
    let split = ContainerRoundFacts {
        live_pages: 2,
        live_pages_rewritten: 2,
        tombstone_pages: 1,
        batches: 2,
    };
    assert!(
        !tombstones_collectable(&split, Some(&trusted), &[component.clone()]),
        "the gate collected on a round that split the container into several pages"
    );

    // AND THE CONTROL: the same container, one batch, every page rewritten.
    let total = ContainerRoundFacts {
        live_pages: 2,
        live_pages_rewritten: 2,
        tombstone_pages: 1,
        batches: 1,
    };
    assert!(
        tombstones_collectable(&total, Some(&trusted), &[component]),
        "CONTROL: a round that rewrote every page in one batch was refused, so the two refusals above \
         are not the gate declining everything"
    );
}
// ==================================================================================================
// THE WIRING ITSELF, THROUGH THE ENGINE'S OWN COMMAND SURFACE
// ==================================================================================================

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
        table_name: "tombstone-wiring".to_string(),
        shard_uri: "local://tombstone-wiring/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
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

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// (live entries, tombstone entries) this object holds in the page index.
fn entry_counts(engine: &TemporalEngine, kind: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != kind || &*page.object_key != object_key {
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

/// DISTINCT pages this object's LIVE entries point at.
///
/// Distinct, by append position, because that is the unit the tombstone rule counts in and the whole
/// point of #2027's folding is that this number comes apart from the entry count.
fn distinct_live_pages(engine: &TemporalEngine, kind: &str, object_key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut pages = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != kind || &*page.object_key != object_key {
                continue;
            }
            pages.insert((page.address.block_slab_id(), page.address.offset()));
        }
    }
    pages.len()
}

/// Members this set SERVES, by asking the engine rather than by reading the maps.
///
/// Through `SetMembers` because that is a reader, and the question this answers is whether a
/// collection is visible to one. A count off `shard.sets` would be the resident map agreeing with
/// itself.
fn served_members(engine: &TemporalEngine, key: &str) -> usize {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "a SetMembers read failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.len(),
        other => panic!("SetMembers answered {other:?}"),
    }
}

/// A ROUND THAT REWRITES THE WHOLE CONTAINER COLLECTS ITS TOMBSTONES, AND A SPLIT ROUND DOES NOT.
///
/// # WHY BOTH CONTAINERS IN ONE ROUND
///
/// One round, two containers, opposite verdicts. Driven together because the two failure modes are
/// each other's control: a gate that collected nothing would pass the declining half alone, and a gate
/// that collected everything would pass the collecting half alone. In one round the SAME code path
/// reaches both, so neither reading survives.
///
/// The small container fits one batch and every one of its pages is relocated, so the rewrite is
/// total. The large one is over `CONTAINER_BATCH_ELEMENT_CAP`, so the round seals two batches -- and a
/// tombstone dropped into batch one is not seen by batch two, which is the second term of
/// `may_drop_tombstones`.
///
/// rust-internal: drives SetAdd/SetRemove and a real compaction round
#[test]
fn a_total_round_collects_a_containers_tombstones_and_a_split_round_does_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const SMALL_MEMBERS: usize = 8;
    const SMALL_REMOVED: usize = 3;
    let small = "wiring-small";
    let large = "wiring-large";
    let large_members = crate::engine::CONTAINER_BATCH_ELEMENT_CAP + 30;
    const LARGE_REMOVED: usize = 2;

    let member = |index: usize| format!("member-{index:05}").into_bytes();

    for index in 0..SMALL_MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: small.to_string(),
                member: member(index),
            },
        );
    }
    for index in 0..large_members {
        write_to(
            &engine,
            Command::SetAdd {
                key: large.to_string(),
                member: member(index),
            },
        );
    }
    for index in 0..SMALL_REMOVED {
        write_to(
            &engine,
            Command::SetRemove {
                key: small.to_string(),
                member: member(index),
            },
        );
    }
    for index in 0..LARGE_REMOVED {
        write_to(
            &engine,
            Command::SetRemove {
                key: large.to_string(),
                member: member(index),
            },
        );
    }

    // THE DENOMINATOR, BEFORE THE ROUND. Both containers must actually be holding tombstone entries,
    // or "collected" and "declined" below are both zero for the same uninteresting reason.
    let (small_live_before, small_tombs_before) = entry_counts(&engine, "set", small);
    let (large_live_before, large_tombs_before) = entry_counts(&engine, "set", large);
    println!("=== tombstone entries, before and after one round ===");
    println!(
        "  small: {small_live_before} live + {small_tombs_before} tombstone   \
         large: {large_live_before} live + {large_tombs_before} tombstone"
    );
    assert_eq!(
        (SMALL_MEMBERS - SMALL_REMOVED, SMALL_REMOVED),
        (small_live_before, small_tombs_before),
        "the small fixture did not file one tombstone entry per removal"
    );
    assert_eq!(
        (large_members - LARGE_REMOVED, LARGE_REMOVED),
        (large_live_before, large_tombs_before),
        "the large fixture did not file one tombstone entry per removal"
    );

    crate::engine::reset_container_batch_counts();
    crate::engine::storage_bucket_internals::reset_container_tombstone_collection_counts();
    crate::engine::container_membership::reset_membership_derive_path_counts();
    let report = engine
        .compact_shard_blocks(1)
        .expect("a compaction round over this fixture must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let (collected, declined) =
        crate::engine::storage_bucket_internals::container_tombstone_collection_counts();
    let (trusted, refused_live_only, refused_incomplete, no_pages) =
        crate::engine::container_membership::membership_derive_path_counts();

    // FLOORS FIRST.
    assert!(
        batches > 0 && folded > 0,
        "DENOMINATOR: the round folded nothing ({batches} batches, {folded} pages), so it says \
         nothing about what a round does to a tombstone"
    );
    assert!(
        report.rewritten_block_refs > 0,
        "DENOMINATOR: the round relocated {} refs",
        report.rewritten_block_refs
    );
    assert!(
        trusted > 0,
        "NO DERIVATION WAS TRUSTED over this round, so the fold is not reached from production at all \
         and the collection below cannot be the thing being measured. trusted={trusted} \
         live-only={refused_live_only} incomplete={refused_incomplete} no-pages={no_pages}"
    );

    let (small_live_after, small_tombs_after) = entry_counts(&engine, "set", small);
    let (large_live_after, large_tombs_after) = entry_counts(&engine, "set", large);
    println!(
        "  round: {batches} batches, {folded} pages folded, {collected} entries collected, \
         {declined} container(s) declined"
    );
    println!(
        "  derive paths: trusted={trusted} refused-live-only={refused_live_only} \
         refused-incomplete={refused_incomplete} no-pages={no_pages}"
    );
    println!(
        "  small: {small_live_after} live + {small_tombs_after} tombstone   \
         large: {large_live_after} live + {large_tombs_after} tombstone"
    );

    // ---- THE COLLECTING HALF ----
    assert_eq!(
        0, small_tombs_after,
        "A TOTAL ROUND DID NOT COLLECT. The small container's every page was rewritten into one \
         batch, so `may_drop_tombstones` permits it and the derivation over its v2 pages is trusted -- \
         if {small_tombs_after} tombstone entries survive, the wiring is not reached and the fold is \
         still theoretical."
    );
    assert_eq!(
        SMALL_MEMBERS - SMALL_REMOVED,
        small_live_after,
        "the collecting round changed the LIVE entry count, which it must not"
    );
    assert!(
        collected >= SMALL_REMOVED as u64,
        "the counter says {collected} entries collected where at least {SMALL_REMOVED} went"
    );

    // ---- THE DECLINING HALF ----
    assert_eq!(
        LARGE_REMOVED, large_tombs_after,
        "A SPLIT ROUND COLLECTED. The large container is over the element cap, so the round seals \
         more than one batch and a tombstone dropped into the first is not seen by the second -- the \
         rule's second term. {large_tombs_after} of {LARGE_REMOVED} survive."
    );
    assert!(
        declined > 0,
        "NOTHING WAS DECLINED, so the surviving tombstones above are not the gate declining -- they \
         would be a container the census never looked at, which is a different fact with the same \
         appearance. That collision is exactly what this counter exists to break."
    );

    // ---- AND THE MEMBERSHIP IS UNCHANGED EITHER WAY, which is the only thing that matters ----
    assert_eq!(
        SMALL_MEMBERS - SMALL_REMOVED,
        served_members(&engine, small),
        "THE COLLECTION CHANGED THE MEMBERSHIP. Collecting a tombstone must be invisible to every \
         reader: it removes a record of an absence, not a member."
    );
    assert_eq!(
        large_members - LARGE_REMOVED,
        served_members(&engine, large),
        "the declining container's membership moved"
    );

    // ---- AND IT SURVIVES A RELOAD, which is where a resurrection would actually show up ----
    //
    // The whole point of collecting only on a total rewrite is that the pages left behind still state
    // the membership. A reload rebuilds the views from those pages, so this is the arm that would catch
    // a member coming back.
    engine.unload_shard(1);
    load_on(&engine);
    let small_reloaded = served_members(&engine, small);
    let large_reloaded = served_members(&engine, large);
    println!("  after a reload: small={small_reloaded} large={large_reloaded}");
    assert_eq!(
        SMALL_MEMBERS - SMALL_REMOVED,
        small_reloaded,
        "A REMOVED MEMBER CAME BACK AFTER A RELOAD once its tombstone was collected. That is the \
         resurrection the whole rule exists to prevent, arriving through the one path that reads the \
         pages rather than the resident map."
    );
    assert_eq!(
        large_members - LARGE_REMOVED,
        large_reloaded,
        "the declining container lost or gained a member across a reload"
    );
}

/// Every `deleted` entry in the SHARD, whatever object or kind it belongs to.
///
/// Deliberately unfiltered. `entry_counts` takes a kind and an object key, so it cannot see a
/// collection that dropped an entry belonging to something else -- and "the entries I asked about are
/// gone" and "exactly those entries are gone" are different statements.
fn deleted_entries_in_shard(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| page.deleted)
        .count()
}

/// THE SHARD'S DELETED-ENTRY COUNT FALLS BY EXACTLY WHAT THE ROUND SAYS IT COLLECTED.
///
/// # A SECOND TEST, NOT A SECOND ASSERTION
///
/// `a_total_round_collects_a_containers_tombstones_and_a_split_round_does_not` already checks two
/// things -- the object's tombstone count and the collected counter -- but both live in ONE test
/// function. Mutating the collecting `retain` to keep every entry was killed by that single test and
/// by nothing else, and a single-test kill is one `#[ignore]`, one rename or one refactor away from
/// being no kill at all. This is the independent one.
///
/// # AND ON AN OBSERVABLE NEITHER OF THOSE USES
///
/// Both existing checks filter by kind and object key. This counts every `deleted` entry in the
/// shard, so the two numbers can disagree in a way the filtered ones cannot show:
///
///   * a collection that removed NOTHING leaves the count where it was while the counter reports a
///     drop -- the no-op mutant;
///   * a collection that removed the right number of entries but the WRONG ones -- say an entry of
///     another object that happened to match a component -- keeps the object-filtered assertion happy
///     and moves this count by the same amount it moves the counter, so the EQUALITY below is what
///     catches it rather than the direction.
///
/// So it is asserted as an EQUALITY against the counter and not as "fewer than before".
///
/// # THE DENOMINATOR
///
/// One container, inside one batch, every page relocated, so the round is total and the collection is
/// permitted. The fixture asserts it actually filed one tombstone entry per removal before the round,
/// and asserts the round folded something, because a round that folded nothing says nothing about what
/// a round does to a tombstone.
///
/// rust-internal: drives SetAdd/SetRemove and a real compaction round
#[test]
fn the_whole_shards_deleted_entry_count_falls_by_exactly_what_the_round_collected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const MEMBERS: usize = 8;
    const REMOVED: usize = 3;
    let key = "shard-wide-count";
    let member = |index: usize| format!("member-{index:05}").into_bytes();

    for index in 0..MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member(index),
            },
        );
    }
    for index in 0..REMOVED {
        write_to(
            &engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member(index),
            },
        );
    }

    let (live_before, tombs_before) = entry_counts(&engine, "set", key);
    let deleted_before = deleted_entries_in_shard(&engine);
    println!("=== the shard's deleted-entry count across one total round ===");
    println!("  before: {live_before} live + {tombs_before} tombstone for this key, {deleted_before} deleted in the shard");
    assert_eq!(
        (MEMBERS - REMOVED, REMOVED),
        (live_before, tombs_before),
        "DENOMINATOR: the fixture did not file one tombstone entry per removal"
    );
    assert_eq!(
        REMOVED, deleted_before,
        "DENOMINATOR: the shard holds {deleted_before} deleted entries where this fixture made \
         {REMOVED} -- something else in the fixture is tombstoning, so the delta below would not be \
         this round's"
    );

    crate::engine::reset_container_batch_counts();
    crate::engine::storage_bucket_internals::reset_container_tombstone_collection_counts();
    let report = engine
        .compact_shard_blocks(1)
        .expect("a compaction round over this fixture must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let (collected, _declined) =
        crate::engine::storage_bucket_internals::container_tombstone_collection_counts();
    let deleted_after = deleted_entries_in_shard(&engine);
    let (live_after, tombs_after) = entry_counts(&engine, "set", key);
    println!(
        "  round: {batches} batches, {folded} pages folded, {collected} collected, \
         {} refs rewritten",
        report.rewritten_block_refs
    );
    println!("  after:  {live_after} live + {tombs_after} tombstone for this key, {deleted_after} deleted in the shard");

    assert!(
        batches > 0 && folded > 0,
        "DENOMINATOR: the round folded nothing ({batches} batches, {folded} pages)"
    );
    assert!(
        collected > 0,
        "the round collected nothing, so there is no delta to account for and this test is not \
         measuring the removal it claims"
    );

    // THE EQUALITY. Not "fewer than before": the counter and the count must agree exactly, which is
    // what fails both a collection that removed nothing and one that removed the wrong entries.
    assert_eq!(
        deleted_before - deleted_after,
        collected as usize,
        "THE SHARD'S DELETED-ENTRY COUNT AND THE COLLECTED COUNTER DISAGREE. The count fell by {} \
         while the round reports {collected} entries collected. Either the removal did not happen \
         (the count is unchanged and the counter is not), or it removed entries the counter did not \
         account for.",
        deleted_before - deleted_after
    );
    assert_eq!(
        0, deleted_after,
        "a total round over the only container in the shard left {deleted_after} deleted entries"
    );
    assert_eq!(
        MEMBERS - REMOVED,
        live_after,
        "the round changed the LIVE entry count, which collecting a tombstone must not"
    );
    assert_eq!(
        MEMBERS - REMOVED,
        served_members(&engine, key),
        "the collection changed the membership, which it must not: it removes a record of an \
         absence, not a member"
    );
}
