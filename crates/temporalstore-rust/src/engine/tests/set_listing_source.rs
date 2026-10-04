// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SET LISTING DECODES EACH PAGE ONCE AND STILL TAKES ITS IDENTITY FROM THE ENTRIES.
//!
//! # WHAT CHANGED, AND WHAT DELIBERATELY DID NOT
//!
//! The listing read one page per entry. After a fold every entry of an object names the SAME page,
//! so a forty-member set read one page forty times and each read was a linear walk of that page's
//! packed keys. It now decodes each distinct page ONCE and answers each entry out of that.
//!
//! WHAT DID NOT MOVE IS WHERE IDENTITY COMES FROM. The entry walk still says which members exist.
//! That is not conservatism, it is the correctness argument, and it is here because the first
//! version of this change got it wrong.
//!
//! # THE REJECTED VERSION, RECORDED BECAUSE IT LOOKED RIGHT
//!
//! The first attempt enumerated members out of the payload and dropped the entry walk. It read one
//! page instead of forty. It had a source-proof -- drop an entry, the member survives -- and a
//! working negative control. It also **resurrected a removed member**, and `tombstone_reload_path`
//! caught it.
//!
//! The mechanism is the thing to remember: a fold followed by a removal leaves the folded page
//! still naming the member as LIVE, while the removal lives in a SEPARATE tombstone page and a
//! DELETED index entry. A reader enumerating the payload sees neither of those, so it serves an
//! element the store was told to forget.
//!
//! And the guard that missed it was not badly built -- it was built over the wrong property.
//! **Presence and absence are two properties.** A loss guard asks "is everything that should be
//! here, here?"; a resurrection guard asks "is everything that should be gone, gone?" The
//! source-proof and the negative control were both about presence. So this module asserts both,
//! and the absence arm is the one that would have failed.

#![allow(clippy::all)]
use super::*;

/// Forty on one compacted page is the measured, real shape.
const MEMBERS: usize = 40;

/// Member payload width, wide enough that a member is not confusable with framing.
const VALUE_WIDTH: usize = 24;

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
        table_name: "set-listing-source".to_string(),
        shard_uri: "local://set-listing-source/1".to_string(),
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

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("member-{index:04}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a fixture write failed: {response:?}");
}

/// A loaded shard holding one set of `MEMBERS` members, folded onto as few pages as the batcher
/// will use. The fold is asserted, because every arm here rests on members sharing a page.
fn folded_set(object_key: &str) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    for index in 0..MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: object_key.to_string(),
                member: member_bytes(index),
            },
        );
    }
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the compaction round failed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "the fixture folded nothing: {batches} batch(es), {folded} page(s). Every arm below rests \
         on the members sharing a page, so this is not a fixture detail."
    );
    (engine, dir)
}

/// Distinct page addresses and LIVE index entries this object resolves to, plus tombstoned ones.
fn page_and_entry_counts(
    engine: &TemporalEngine,
    object_key: &str,
) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            if page.deleted {
                tombstoned += 1;
                continue;
            }
            live += 1;
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (pages.len(), live, tombstoned)
}

/// Every element key a live page of this object still states as PRESENT, read off the bytes.
///
/// This is the other source. The point of the absence arm is that it disagrees with the listing.
fn components_the_pages_still_state(
    engine: &TemporalEngine,
    object_key: &str,
) -> std::collections::BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut stated = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&page.address) else {
                continue;
            };
            if let crate::engine::container_pages::ContainerPageDecode::Framed {
                spelling,
                items,
                ..
            } = crate::engine::container_pages::decode_container_page(&bytes)
            {
                for item in items {
                    if item.deleted {
                        continue;
                    }
                    if let Some(component) =
                        crate::engine::container_pages::component_from_element_key(
                            spelling, &item.key,
                        )
                    {
                        stated.insert(component);
                    }
                }
            }
        }
    }
    stated
}

/// Drop `how_many` of this object's live set entries, leaving their PAGE untouched.
fn drop_live_entries(engine: &TemporalEngine, object_key: &str, how_many: usize) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let mut dropped = 0usize;
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket
            .block_index
            .retain(&mut shard.bucket_index.block_slab_live, |_handle, page| {
                let mine = page.model_id.as_str() == "set"
                    && &*page.object_key == object_key
                    && !page.deleted;
                if mine && dropped < how_many {
                    dropped += 1;
                    return false;
                }
                true
            });
    }
    dropped
}

fn listed_members(engine: &TemporalEngine, object_key: &str) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: object_key.to_string(),
        },
    });
    assert!(response.status.ok, "the listing failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("the listing answered {other:?} rather than members"),
    }
}

fn all_members() -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = (0..MEMBERS).map(member_bytes).collect();
    all.sort();
    all
}

// =================================================================================================
// PRESENCE
// =================================================================================================

/// EVERY MEMBER IS STILL LISTED AFTER A FOLD, AND THE ORDER IS THE ONE CALLERS HAD.
#[test]
fn a_folded_set_still_lists_every_member_in_the_order_it_always_did() {
    let (engine, _dir) = folded_set("listing/whole");
    let (pages, live, tombstoned) = page_and_entry_counts(&engine, "listing/whole");
    println!(
        "[presence] {pages} distinct page(s), {live} live entr(ies), {tombstoned} tombstoned, \
         for {MEMBERS} members"
    );
    assert!(
        pages < live,
        "the members did not come to share a page: {pages} page(s) for {live} entries, so nothing \
         here is exercising a page read that used to happen several times"
    );
    let listed = listed_members(&engine, "listing/whole");
    // ORDER AND NOT JUST CONTENT. The walk is sorted by component, a set's component is its member
    // in hex, and hex preserves byte order -- so this is the order the listing always returned.
    assert_eq!(
        all_members(),
        listed,
        "the folded set did not list its members, in order"
    );
}

// =================================================================================================
// ABSENCE -- the property the rejected version failed
// =================================================================================================

/// A MEMBER REMOVED AFTER THE FOLD STAYS REMOVED, THOUGH ITS PAGE STILL SAYS OTHERWISE.
///
/// THIS IS THE ARM THAT WOULD HAVE CAUGHT THE REJECTED VERSION, and it is built so that it can.
/// The removal lands AFTER the fold, so the folded page still states the member as live while the
/// removal lives in a tombstone page and a deleted entry. The two sources therefore DISAGREE, and
/// that disagreement is asserted before the listing is consulted -- otherwise this would be an
/// agreement test and could not discriminate.
///
/// A reader enumerating the payload answers with the removed member. A reader taking identity from
/// the live entries cannot.
#[test]
fn a_member_removed_after_the_fold_is_not_listed_even_though_its_page_still_states_it() {
    let (engine, _dir) = folded_set("listing/removed");
    let victim = member_bytes(2);
    write_to(
        &engine,
        Command::SetRemove {
            key: "listing/removed".to_string(),
            member: victim.clone(),
        },
    );

    let (pages, live, tombstoned) = page_and_entry_counts(&engine, "listing/removed");
    println!(
        "[absence] after fold+remove: {pages} live page(s), {live} live entr(ies), \
         {tombstoned} tombstoned"
    );

    // THE TWO SOURCES MUST DISAGREE, OR THIS ARM PROVES NOTHING.
    let stated = components_the_pages_still_state(&engine, "listing/removed");
    let victim_component = hex::encode(&victim);
    assert!(
        stated.contains(&victim_component),
        "the pages no longer state the removed member as present, so the payload and the entries \
         AGREE and this arm cannot tell a payload-enumerating reader from an entry-driven one. \
         The removal has to land after the fold for the folded page to still carry it."
    );

    // AND THE LISTING MUST SIDE WITH THE ENTRIES.
    let listed = listed_members(&engine, "listing/removed");
    assert!(
        !listed.contains(&victim),
        "the listing returned a member that was removed. Its page still states it as live, so a \
         reader enumerating the payload answers exactly this way -- which is the defect this arm \
         exists for, and the reason identity stays with the entries"
    );
    let expected: Vec<Vec<u8>> = all_members()
        .into_iter()
        .filter(|member| *member != victim)
        .collect();
    assert_eq!(
        expected, listed,
        "the listing after one removal is not every other member, in order"
    );
}

// =================================================================================================
// WHICH SOURCE, AND THE CONTROL
// =================================================================================================

/// IDENTITY COMES FROM THE ENTRIES, PROVED BY REMOVING A NAME AND NOT ITS PAGE.
///
/// Post-fold the members share a page, so dropping ONE live entry removes a name while leaving the
/// page that holds the member. A listing driven by the entries loses exactly that member; one
/// enumerating the payload loses none. This is the same experiment the rejected version used --
/// with the assertion the other way round, because the answer it was looking for was the wrong one.
///
/// THE CONTROL IS DROPPING EVERY ENTRY: the listing must be empty. Without it the arm above could
/// pass on a listing that answers nothing at all.
#[test]
fn the_set_listing_takes_its_identity_from_the_entries_and_not_from_the_payload() {
    let (engine, _dir) = folded_set("listing/one-name-gone");
    let dropped = drop_live_entries(&engine, "listing/one-name-gone", 1);
    assert_eq!(1, dropped, "the fixture dropped {dropped} entries, not one");
    let (pages, live, _) = page_and_entry_counts(&engine, "listing/one-name-gone");
    assert!(
        pages >= 1 && live == MEMBERS - 1,
        "after dropping one entry: {pages} page(s), {live} live entr(ies). The page must survive \
         for this arm to distinguish the sources"
    );
    let listed = listed_members(&engine, "listing/one-name-gone");
    assert_eq!(
        MEMBERS - 1,
        listed.len(),
        "the listing returned {} member(s) after one NAME was dropped while its page survived. \
         Identity is supposed to come from the entries, so exactly one member should go; a \
         payload-enumerating reader would still return all {MEMBERS}",
        listed.len()
    );
    drop(engine);

    // THE CONTROL.
    let (engine, _dir) = folded_set("listing/all-names-gone");
    let dropped = drop_live_entries(&engine, "listing/all-names-gone", usize::MAX);
    assert!(
        dropped >= MEMBERS,
        "the control dropped only {dropped} of {MEMBERS} entries, so it proves nothing"
    );
    let empty = listed_members(&engine, "listing/all-names-gone");
    assert!(
        empty.is_empty(),
        "the listing returned {} member(s) with no live entry naming the page, so this test \
         cannot fail",
        empty.len()
    );
    println!("[source] one name dropped -> one member gone; every name dropped -> empty");
}

// =================================================================================================
// WHAT IT COSTS
// =================================================================================================

fn page_reads_of<T>(work: impl FnOnce() -> T) -> (T, u64) {
    crate::engine::reset_maintenance_block_read_counts();
    let value = work();
    let counts = crate::engine::maintenance_block_read_counts();
    (value, counts.block_reads_total)
}

/// A shard holding one set, optionally folded, REOPENED so the cache is cold.
///
/// The compaction round puts the page it writes into the cache, so measuring on the same engine
/// would count zero reads and measure nothing. Reopening is what makes the count a count.
fn reopened_set(object_key: &str, fold: bool) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = engine_on(dir.path());
        load_on(&engine);
        for index in 0..MEMBERS {
            write_to(
                &engine,
                Command::SetAdd {
                    key: object_key.to_string(),
                    member: member_bytes(index),
                },
            );
        }
        if fold {
            crate::engine::reset_container_batch_counts();
            engine
                .compact_shard_blocks(1)
                .expect("the compaction round failed");
            let (batches, folded) = crate::engine::container_batch_counts();
            assert!(
                batches > 0 && folded > 0,
                "the folded arm folded nothing: {batches} batch(es), {folded} page(s)"
            );
        }
    }
    let engine = engine_on(dir.path());
    load_on(&engine);
    (engine, dir)
}

/// WHAT THE FOLD IS WORTH TO A LISTING THAT DECODES EACH PAGE ONCE.
///
/// THE SAME CODE RUNS BOTH ARMS; only the data shape differs. Unfolded, each member owns its page
/// and its address, so a page is read per member exactly as before. Folded, every entry names one
/// page, which is now decoded once.
///
/// COUNTS AND NOT TIMES, for the reason the sibling module states: a timing ratio on this box has
/// read 485x idle against 11x busy off identical code, so the instrument is the counter inside the
/// one place a page read past the cache is noted -- and a count is unaffected by the load it was
/// taken under.
#[test]
fn what_the_fold_is_worth_to_a_listing_that_decodes_each_page_once() {
    println!(
        "\nload average: {}",
        std::fs::read_to_string("/proc/loadavg")
            .map(|text| text.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|_| "unavailable".to_string())
    );

    let mut reads_by_arm: Vec<(&str, u64)> = Vec::new();
    for (label, fold) in [("unfolded", false), ("folded", true)] {
        let key = format!("cost/{label}");
        let (engine, _dir) = reopened_set(&key, fold);
        let (pages, live, _) = page_and_entry_counts(&engine, &key);
        let (listed, reads) = page_reads_of(|| listed_members(&engine, &key));
        assert_eq!(
            all_members(),
            listed,
            "the {label} arm did not list its members, so its read count is a count over the \
             wrong answer"
        );
        println!(
            "  {label:<9} pages={pages:<3} entries={live:<4} reads={reads:<4} reads/member={:.3}",
            reads as f64 / MEMBERS as f64
        );
        reads_by_arm.push((label, reads));
        drop(engine);
    }

    let unfolded = reads_by_arm[0].1;
    let folded = reads_by_arm[1].1;
    assert!(
        unfolded > 0 && folded > 0,
        "an arm read no pages at all ({unfolded} unfolded, {folded} folded), so the comparison is \
         between two unexercised arms"
    );
    assert_eq!(
        MEMBERS as u64, unfolded,
        "the unfolded arm read {unfolded} pages for {MEMBERS} members; one per member is the shape \
         this listing has when every member owns its own page"
    );
    assert!(
        folded < unfolded,
        "the folded arm read {folded} pages against {unfolded} unfolded, so decoding each page \
         once bought nothing"
    );
    println!(
        "  => the fold takes the listing from {unfolded} page reads to {folded} for the same \
         {MEMBERS} members"
    );
}
