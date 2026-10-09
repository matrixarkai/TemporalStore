// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE GATED LISTING ANSWERS FROM THE PAGES, AND A REMOVED MEMBER STAYS GONE.
//!
//! # WHAT CHANGED AND WHY IT IS THE RISKY DIRECTION
//!
//! The ungated listing asks the ENTRIES which members exist and the page only what each one holds.
//! Its own `None` arm states why that cannot survive this gate: "an entry naming no component can
//! only be answered by a page that names no element either". Under the gate every entry names no
//! component and every page IS framed, so that arm drops every member and the listing answered
//! EMPTY -- measured at `listed=0` against 40 ungated, by two independent fixtures.
//!
//! So the gated listing takes identity out of the PAYLOAD. That is the direction that
//! **resurrected a removed member** when it was tried before, and the resurrection is the thing
//! this module exists to rule out rather than to hope about.
//!
//! # THE TWO THINGS THAT MAKE IT SAFE, BOTH DRIVEN RATHER THAN ARGUED
//!
//!   1. the tombstone page is REACHABLE from the index -- a gated removal files a page-named
//!      tombstone entry beside the live entry, which
//!      `a_gated_removal_leaves_a_tombstone_page_the_index_can_reach` drives;
//!   2. `container_membership::derive_membership` owns the ORDERING, folding every page of the
//!      object by `append_position` with a value and a tombstone as the same kind of statement, so
//!      the later page wins. A hand-rolled loop over the payload lacks exactly that precedence,
//!      and that is the loop that resurrected a member.
//!
//! # WHY THERE ARE COUNTERS AND NOT JUST A MEMBER LIST
//!
//! The gated branch declines an INCOMPLETE fold and answers from the durable map instead. That
//! fallback returns the right members, so a test that only checked the members would pass
//! identically whether the fold ran or never ran -- the exact shape that made an earlier
//! measurement in this series report a clean result over a branch nothing reached. So the branch
//! counts which path answered, and the floor here requires the FOLD to have served.

#![allow(clippy::all)]
use super::*;
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;
use std::collections::BTreeSet;

const MEMBERS: usize = 40;
const KEY: &str = "gated-listing-set";

struct GateHeldOn {
    restore: Option<String>,
}

impl GateHeldOn {
    fn on() -> Self {
        let restore = std::env::var(TS_CONTAINER_ONE_ENTRY_A_PAGE).ok();
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
        Self { restore }
    }
}

impl Drop for GateHeldOn {
    fn drop(&mut self) {
        match self.restore.take() {
            Some(previous) => std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, previous),
            None => std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE),
        }
    }
}

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
        table_name: "gated-listing".to_string(),
        shard_uri: "local://gated-listing/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:03}").into_bytes()
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn listed(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "SMEMBERS failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("SetMembers answered {other:?}"),
    }
}

/// Seed, fold, and remove one member. Returns the expected survivors and the victim.
fn seed_fold_remove(engine: &TemporalEngine) -> (BTreeSet<Vec<u8>>, Vec<u8>) {
    load_on(engine);
    for index in 0..MEMBERS {
        write(
            engine,
            Command::SetAdd {
                key: KEY.to_string(),
                member: member_bytes(index),
            },
        );
    }
    // FOLD FIRST. Before a fold each member owns its page, so removing one takes its page with it
    // and the pages never disagree -- the fixture would pass for the wrong reason.
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let victim = member_bytes(13);
    write(
        engine,
        Command::SetRemove {
            key: KEY.to_string(),
            member: victim.clone(),
        },
    );
    let expected: BTreeSet<Vec<u8>> = (0..MEMBERS)
        .map(member_bytes)
        .filter(|m| *m != victim)
        .collect();
    (expected, victim)
}

/// rust-internal: the gated listing folds the pages, and the removed member does not come back
#[test]
fn a_gated_listing_folds_the_pages_and_the_removed_member_stays_gone() {
    // SEPARATE STORES PER ARM. One shared store would let the second arm read the first's cached
    // answer, which has already produced a wrong figure once in this campaign.
    let gated_dir = tempfile::tempdir().expect("tempdir");
    let plain_dir = tempfile::tempdir().expect("tempdir");

    // ---- THE GATED ARM ----
    let expected = {
        let _held = GateHeldOn::on();
        let engine = engine_on(gated_dir.path());
        let (expected, victim) = seed_fold_remove(&engine);
        crate::engine::execute_on_shard::reset_gated_listing_counts();
        let seen = listed(&engine);
        let (derived, declined) = crate::engine::execute_on_shard::gated_listing_counts();

        println!("\n=== the gated listing ===");
        println!(
            "  members listed: {} of {} expected; victim present: {}",
            seen.len(),
            expected.len(),
            seen.contains(&victim)
        );
        println!("  answered by: derived={derived}, declined={declined}");

        // THE FLOOR, on the reaching of the path and not on the answer: the FOLD must have served.
        // The decline path answers from the durable map and would return the same members, so
        // without this the arm below passes whether or not the fold ran.
        assert_eq!(
            1, derived,
            "the gated listing was answered by the fold {derived} time(s) and declined \
             {declined} time(s). The decline path answers from the durable map and returns the \
             same members, so this assertion is what distinguishes a tested fold from an \
             untested one"
        );
        assert_eq!(
            0, declined,
            "the gated listing DECLINED the fold {declined} time(s) and fell back to the durable \
             map. A decline means a page could not be read, decoded, or walked -- the members \
             below would be right for a reason that has nothing to do with the pages"
        );

        assert_eq!(
            expected, seen,
            "the gated listing does not serve the membership the removal left"
        );
        assert!(
            !seen.contains(&victim),
            "THE GATED LISTING RESURRECTED THE REMOVED MEMBER. This is the failure that a \
             hand-rolled walk of the payload produced before `derive_membership` owned the \
             ordering: the folded page still states the member as live, and only a fold by \
             `append_position` lets the later tombstone page win"
        );
        assert_eq!(
            MEMBERS - 1,
            seen.len(),
            "the gated listing returned {} members, not {}",
            seen.len(),
            MEMBERS - 1
        );
        expected
    };

    // ---- THE UNGATED CONTROL, on its own store, so the gate is the only difference ----
    let engine = engine_on(plain_dir.path());
    let (plain_expected, plain_victim) = seed_fold_remove(&engine);
    let plain_seen = listed(&engine);
    println!("=== the ungated control ===");
    println!("  members listed: {}", plain_seen.len());
    assert_eq!(
        plain_expected, plain_seen,
        "the UNGATED listing is wrong on this fixture, so it is not a control for anything"
    );
    assert!(
        !plain_seen.contains(&plain_victim),
        "the ungated listing resurrected the member too, so the fixture rather than the gate is \
         the subject"
    );

    // ---- AND THE TWO AGREE, which is the property worth holding from here on ----
    assert_eq!(
        plain_seen, expected,
        "the gated listing and the ungated listing disagree about the same sequence of commands"
    );
}

/// rust-internal: operates on the fold directly, so the decline condition has no reachability doubt
///
/// AN UNFRAMED PAGE MAKES THE FOLD DECLINE RATHER THAN ANSWER SHORT.
///
/// # WHY THIS IS THE ONE CASE THE GATED LISTING CANNOT SIMPLY TRUST
///
/// `derive_membership` contributes **nothing** from an unframed page, and it is right not to: the
/// frame is what names the element, so a bare value cannot say WHICH member it belongs to. The
/// entry used to supply that name, and under this gate the entry no longer carries one.
///
/// Meanwhile today's ungated listing hands a bare page's payload back AS the member, resolving it
/// by the address the entry named. So the same page is a member to one reader and nothing at all to
/// the other — and a gated listing that folded without checking would answer SHORT by exactly the
/// number of unframed pages it met, silently.
///
/// That is why the gated branch keys its decline on `is_complete()` and falls back to the durable
/// map. This test establishes the condition that decline rests on, directly rather than through the
/// engine, so there is no question of whether the path was reached.
///
/// # WHEN A GATED STORE CAN ACTUALLY MEET ONE, WHICH IS NARROWER THAN IT SOUNDS
///
/// Not from its own writes. A set's component is `hex::encode(member)`, `ElementKeySpelling::Hex`
/// renders it back, and `element_key_from_component` cannot fail on it — so a gated binary writing
/// a set **always** frames, and `unframed_container_write_count` is floored at zero over all four
/// kinds elsewhere.
///
/// It can meet one only in a store written by a binary from before `container_pages` existed and
/// then read by a gated one. That is a CROSS-VERSION read, which is exactly the boundary step
/// nine's reload corpus exists to cover, and it is the reason the fallback is not dead code.
#[test]
fn an_unframed_page_makes_the_fold_decline_rather_than_answer_short() {
    use crate::block_store::ElementEntry;
    use crate::engine::container_membership::{append_position, derive_membership};

    fn at(slab: u64, offset: u64, len: u32) -> ElementEntry {
        ElementEntry::from_parts(slab, offset, u64::from(len), Some(0), Some(7))
    }

    let member = b"member-000".to_vec();
    let component = hex::encode(&member);

    // A real framed page for that member, through the codec rather than hand-assembled.
    let framed = crate::engine::container_pages::encode_container_page(
        crate::engine::container_pages::ElementKeySpelling::Hex,
        &[(member.as_slice(), member.as_slice())],
    );
    // A bare value, which is what a page written before `container_pages` looks like.
    let bare = b"a bare value from before the frame existed".to_vec();

    let framed_at = at(1, 0, framed.len() as u32);
    let bare_at = at(1, 4096, bare.len() as u32);
    let framed_position = append_position(&framed_at);

    // Keyed by POSITION, so the closure borrows nothing the two calls need to own.
    let read = || {
        let framed = framed.clone();
        let bare = bare.clone();
        move |address: &ElementEntry| -> Option<Vec<u8>> {
            if append_position(address) == framed_position {
                Some(framed.clone())
            } else {
                Some(bare.clone())
            }
        }
    };

    let only_framed = derive_membership("set", vec![framed_at.clone()], read());
    let with_bare = derive_membership("set", vec![framed_at.clone(), bare_at.clone()], read());

    println!("\n=== the fold, with and without an unframed page ===");
    println!(
        "  framed only : live={} unframed={} complete={}",
        only_framed.live.len(),
        only_framed.unframed,
        only_framed.is_complete()
    );
    println!(
        "  plus a bare : live={} unframed={} complete={}",
        with_bare.live.len(),
        with_bare.unframed,
        with_bare.is_complete()
    );

    // ---- THE CONTROL ARM: a fold of framed pages alone is complete, or the arm below says nothing
    assert_eq!(
        1,
        only_framed.live.len(),
        "the framed-only fold found {} live element(s), so this fixture's framed page is not \
         being walked and neither column below is attributable",
        only_framed.live.len()
    );
    assert!(
        only_framed.live.contains_key(&component),
        "the framed-only fold does not name the member, so the codec and the fold disagree about \
         the spelling and nothing here is about unframed pages"
    );
    assert_eq!(0, only_framed.unframed, "the framed page was counted unframed");
    assert!(
        only_framed.is_complete(),
        "a fold of framed pages alone reports itself incomplete, so `is_complete` is false for a \
         reason that has nothing to do with the bare page added below"
    );

    // ---- THE CASE UNDER TEST ----
    assert_eq!(
        1, with_bare.unframed,
        "the bare page was counted {} time(s) as unframed, not once",
        with_bare.unframed
    );
    assert!(
        !with_bare.is_complete(),
        "A FOLD THAT MET AN UNFRAMED PAGE REPORTS ITSELF COMPLETE. The gated listing keys its \
         decline on exactly this, so if `is_complete()` stops noticing an unframed page the \
         listing would serve a membership short by however many it met -- silently, because an \
         unframed page contributes no element and therefore no error either"
    );
    // AND THE SHORTNESS IS REAL, not merely flagged: the bare page added no member.
    assert_eq!(
        only_framed.live.len(),
        with_bare.live.len(),
        "the bare page contributed {} extra live element(s). It cannot name an element, so it must \
         contribute none -- and that is precisely why the fold has to decline rather than answer",
        with_bare.live.len() - only_framed.live.len()
    );
}
