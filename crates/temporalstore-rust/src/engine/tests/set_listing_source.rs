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

/// The addresses of this object's LIVE pages, captured so they can still be read after the index
/// entry that names them is gone.
fn live_page_addresses(engine: &TemporalEngine, object_key: &str) -> Vec<ElementEntry> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut addresses = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key || page.deleted {
                continue;
            }
            addresses.push(page.address.clone());
        }
    }
    addresses
}

/// Every element key a given set of PAGE ADDRESSES still states as present, read straight off the
/// block store rather than through the index.
///
/// IT HAS TO BYPASS THE INDEX, which is the whole reason it exists beside
/// `components_the_pages_still_state`: `drop_live_entries` removes the index ENTRY and leaves the
/// block, so a page it un-names cannot be reached through `block_index` at all. Reading addresses
/// captured beforehand is what lets an arm say the bytes outlived the name.
fn components_at(
    engine: &TemporalEngine,
    addresses: &[ElementEntry],
) -> std::collections::BTreeSet<String> {
    let mut stated = std::collections::BTreeSet::new();
    for address in addresses {
        let Ok(bytes) = engine.block_store.read(address) else {
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
                    crate::engine::container_pages::component_from_element_key(spelling, &item.key)
                {
                    stated.insert(component);
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
    // RE-ATTRIBUTED, BECAUSE `pages < live` BECAME UNREACHABLE RATHER THAN FALSE. All four
    // container kinds now file ONE index entry a page, so every live entry of this object names a
    // distinct page and `pages == live` holds BY CONSTRUCTION -- the old form can only ever fail,
    // and its obvious repair, `pages == live`, is a tautology that would read like a guard while
    // asserting nothing, both sides being the same walk counted twice.
    //
    // THE SHARING THIS ARM NEEDS IS A PROPERTY OF THE PAGE PAYLOAD, which is where the members
    // went, so it is asserted there: strictly more members stated by the pages than there are
    // pages. That compares two independent artefacts -- the index's page count, and the bytes
    // inside those pages -- and the UNFOLDED shape FAILS it, at MEMBERS pages stating MEMBERS
    // members, which is what makes this a measurement of the fold rather than a restatement of it.
    let stated = components_the_pages_still_state(&engine, "listing/whole");
    assert_eq!(
        MEMBERS,
        stated.len(),
        "the live pages state {} member(s), not the {MEMBERS} written, so the payload side of the \
         comparison below is not this whole set",
        stated.len()
    );
    assert!(
        pages < stated.len(),
        "the members did not come to share a page: {pages} page(s) stating {} member(s), so \
         nothing here is exercising a page read that used to happen several times",
        stated.len()
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

/// IDENTITY COMES FROM THE ENTRIES, AND A PARTIAL UN-NAMING CAN NO LONGER BE BUILT.
///
/// RESTATED RATHER THAN ADJUSTED, because the experiment stopped being REPRESENTABLE. This arm
/// dropped ONE live entry of a folded object so that a name went while the page holding the member
/// stayed, then asked which way the listing went: an entry-driven listing loses exactly that
/// member, a payload-driven one loses none. All four container kinds now file ONE INDEX ENTRY A
/// PAGE, so a folded object has a single entry and the smallest un-naming the index can express is
/// the WHOLE PAGE. `live == MEMBERS - 1` is therefore not a stale number to re-golden; it names a
/// state the store can no longer reach, measured at 0 page(s) and 0 live entr(ies).
///
/// THE SAME DISCRIMINATION SURVIVES AT PAGE GRANULARITY, and that is what is asserted. Drop the one
/// entry and the member BYTES are untouched -- `drop_live_entries` removes index entries, not
/// blocks -- so the two sources disagree as sharply as they ever did: a reader enumerating the
/// payload answers with all MEMBERS members, and a reader taking identity from the entries answers
/// with none.
///
/// THE OLD CONTROL IS DELETED, NOT REPLACED IN KIND, and the reason is recorded here so that nobody
/// restores it. It dropped EVERY entry and required the listing to be empty, which guarded the arm
/// above against passing on a listing that answers nothing at all. Post-collapse, dropping the one
/// entry IS dropping every entry: the control had become the subject run a second time, and a
/// control that runs the subject cannot discriminate. The control that replaces it interrogates the
/// OTHER side -- the captured pages must still state all MEMBERS members after the drop -- so an
/// empty listing cannot be explained away by the bytes having gone with the name.
#[test]
fn the_set_listing_takes_its_identity_from_the_entries_and_not_from_the_payload() {
    let (engine, _dir) = folded_set("listing/one-name-gone");

    // ONE ENTRY A PAGE IS THE PREMISE, so it is asserted rather than assumed. If a set ever went
    // back to an entry per element this reads MEMBERS and the whole-page reasoning stops applying.
    let (pages_before, live_before, _) = page_and_entry_counts(&engine, "listing/one-name-gone");
    assert_eq!(
        1, live_before,
        "the folded object holds {live_before} live entr(ies) over {pages_before} page(s). One \
         entry a page is what makes a whole-page un-naming the only one available, and this arm \
         is built on it"
    );

    // Captured BEFORE the drop, because an un-named page is unreachable through the index: the
    // control below has to read these addresses straight off the block store.
    let addresses = live_page_addresses(&engine, "listing/one-name-gone");
    assert_eq!(
        pages_before,
        addresses.len(),
        "captured {} address(es) for {pages_before} live page(s)",
        addresses.len()
    );

    let dropped = drop_live_entries(&engine, "listing/one-name-gone", 1);
    assert_eq!(1, dropped, "the fixture dropped {dropped} entries, not one");
    let (pages, live, _) = page_and_entry_counts(&engine, "listing/one-name-gone");
    assert_eq!(
        (0usize, 0usize),
        (pages, live),
        "dropping this object's one entry left {pages} page(s) and {live} live entr(ies). The \
         un-naming is supposed to take a whole page's worth of names with it, because there is one \
         entry a page -- if this ever reads (1, 39) the partial un-naming is representable again \
         and the arm this one replaced should come back"
    );

    // THE CONTROL, AND IT IS ON THE SOURCE THE SUBJECT DOES NOT USE. The bytes outlived the name.
    let stated = components_at(&engine, &addresses);
    assert_eq!(
        MEMBERS,
        stated.len(),
        "the captured pages state {} member(s) after the entry was dropped, not the {MEMBERS} \
         written. The empty listing below has to be the entries' doing; if the payload went with \
         the name this arm cannot tell an entry-driven reader from a payload-driven one",
        stated.len()
    );

    // THE FINDING.
    let listed = listed_members(&engine, "listing/one-name-gone");
    assert!(
        listed.is_empty(),
        "the listing returned {} member(s) with no live entry naming their page, while those \
         pages still state all {MEMBERS}. A reader enumerating the payload answers exactly that \
         way, which is the defect this arm exists for and the reason identity stays with the \
         entries",
        listed.len()
    );
    println!(
        "[source] the object's one entry dropped -> {} member(s) listed, while the captured pages \
         still state {} -- identity is the entries'",
        listed.len(),
        stated.len()
    );
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
