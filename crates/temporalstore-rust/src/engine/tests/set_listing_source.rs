// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH SOURCE THE SET LISTING ANSWERS FROM, PROVED BY MAKING THE TWO SOURCES DISAGREE.
//!
//! # WHAT CHANGED
//!
//! The listing used to ask the page index for one entry per member and then read a page per entry.
//! After a compaction every one of those entries names the SAME page, so a forty-member set read
//! one page forty times and each read was a linear walk of that page's packed keys. It now reads
//! each distinct page ONCE and decodes every member out of it, which is what `container_pages`
//! wrote the per-item keys into the payload for.
//!
//! # WHY A LISTING TEST ALONE WOULD PROVE NOTHING
//!
//! Before and after the change the listing returns the same members, because both sources agree.
//! Two readers of sources that agree cannot disagree, so a test that only compares the listing
//! against the members it wrote is asserting agreement rather than source -- which is the defect
//! #2092's instrument exists to avoid. So the sources are made to DISAGREE: an entry is dropped
//! from the page index while the page itself is left intact.
//!
//! Post-fold the object's members all live on one page, so dropping one entry removes a NAME while
//! leaving the page that holds the member. A listing that enumerates members from entries loses
//! exactly that member; one that enumerates them from the payload loses nothing. That is the whole
//! experiment, and it can only come out one way per implementation.
//!
//! # AND THE NEGATIVE CONTROL IS WHAT MAKES IT FALSIFIABLE
//!
//! Dropping EVERY entry of the object must leave the listing empty. The index is still what makes
//! the page REACHABLE -- it holds the address -- so a listing that answered a full set with no
//! entries at all would mean this test could not fail, and would mean the reachability claim was
//! being smuggled in. The two arms together say exactly what moved: enumeration left the index,
//! reachability did not.

#![allow(clippy::all)]
use super::*;

/// Enough members that a fold is worth doing and a lost one is unmistakable. Forty on one compacted
/// page is the measured, real shape.
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

/// A loaded shard holding one set of `MEMBERS` members, FOLDED onto as few pages as the batcher
/// will use. Returns the engine and the directory that must outlive it.
fn folded_set(object_key: &str) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    for index in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: object_key.to_string(),
                member: member_bytes(index),
            },
        });
        assert!(response.status.ok, "a fixture write failed: {response:?}");
    }

    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the compaction round failed");
    let (batches, folded) = crate::engine::container_batch_counts();
    // PROOF THE TREATMENT RAN. Without a fold the members sit on separate pages, every entry names
    // a different address, and dropping one entry would drop its page too -- so the experiment
    // below would be measuring the wrong thing while still passing.
    assert!(
        batches > 0 && folded > 0,
        "the fixture folded nothing: {batches} batch(es), {folded} page(s) folded. Every arm \
         below rests on the members sharing a page, so this is not a fixture detail."
    );
    (engine, dir)
}

/// Distinct page addresses and live index entries this object resolves to.
fn addresses_and_entries(engine: &TemporalEngine, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut addresses = std::collections::BTreeSet::new();
    let mut entries = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted
                || page.model_id.as_str() != "set"
                || &*page.object_key != object_key
            {
                continue;
            }
            entries += 1;
            addresses.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (addresses.len(), entries)
}

/// Drop `how_many` of this object's set entries from the page index, leaving their PAGE untouched.
///
/// `usize::MAX` drops all of them. Returns how many were dropped.
fn drop_entries(engine: &TemporalEngine, object_key: &str, how_many: usize) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let mut dropped = 0usize;
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket
            .block_index
            .retain(&mut shard.bucket_index.block_slab_live, |_handle, page| {
                let mine = page.model_id.as_str() == "set" && &*page.object_key == object_key;
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

fn expected_members() -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = (0..MEMBERS).map(member_bytes).collect();
    all.sort();
    all
}

#[test]
fn the_set_listing_answers_from_the_page_and_not_from_the_entries_that_name_it() {
    // ---- ARM A: the fold happened, and the listing is whole. -------------------------------
    let (engine, _dir) = folded_set("listing/whole");
    let (addresses, entries) = addresses_and_entries(&engine, "listing/whole");
    println!(
        "[source] after the fold: {addresses} distinct page(s), {entries} index entr(ies) for \
         {MEMBERS} members"
    );
    assert!(
        addresses < entries,
        "the members did not come to share a page: {addresses} page(s) for {entries} entries. \
         The experiment below needs one page named by several entries."
    );
    let mut whole = listed_members(&engine, "listing/whole");
    whole.sort();
    assert_eq!(
        expected_members(),
        whole,
        "the listing did not return the members that were written"
    );
    drop(engine);

    // ---- ARM B: one NAME removed, the page left intact. -------------------------------------
    //
    // The sources now disagree. An index-enumerated listing is short by exactly one; a
    // payload-enumerated one is whole.
    let (engine, _dir) = folded_set("listing/one-name-gone");
    let dropped = drop_entries(&engine, "listing/one-name-gone", 1);
    assert_eq!(1, dropped, "the fixture dropped {dropped} entries, not one");
    let (addresses, entries) = addresses_and_entries(&engine, "listing/one-name-gone");
    assert!(
        addresses >= 1,
        "dropping one entry took the page with it, so this arm cannot distinguish the sources"
    );
    println!(
        "[source] one entry dropped: {addresses} page(s), {entries} entr(ies) still name it"
    );
    let mut after = listed_members(&engine, "listing/one-name-gone");
    after.sort();
    assert_eq!(
        expected_members(),
        after,
        "a member disappeared when its index ENTRY was dropped while its PAGE was left intact, \
         so the listing is still enumerating members from entries rather than from the payload. \
         This is the whole claim of the change."
    );
    drop(engine);

    // ---- ARM C: NEGATIVE CONTROL -- every name removed. -------------------------------------
    //
    // The index still owns reachability: it holds the address. With no entry at all there is no
    // page to read, and the listing must be empty. Without this arm the test could not fail.
    let (engine, _dir) = folded_set("listing/all-names-gone");
    let dropped = drop_entries(&engine, "listing/all-names-gone", usize::MAX);
    assert!(
        dropped >= MEMBERS,
        "the control dropped only {dropped} of {MEMBERS} entries, so the set is still reachable \
         and the control proves nothing"
    );
    let empty = listed_members(&engine, "listing/all-names-gone");
    assert!(
        empty.is_empty(),
        "the listing returned {} member(s) with NO index entry naming the page. The index is \
         what makes a page reachable, so answering here would mean this test cannot fail.",
        empty.len()
    );
    println!("[source] every entry dropped: listing is empty, so the test can fail");
}

/// Page reads the listing performs, with the cache cold.
fn page_reads_of<T>(work: impl FnOnce() -> T) -> (T, u64) {
    crate::engine::reset_maintenance_block_read_counts();
    let value = work();
    let counts = crate::engine::maintenance_block_read_counts();
    (value, counts.block_reads_total)
}

/// A shard holding one set of `MEMBERS` members, optionally folded, then REOPENED so the cache is
/// cold and a page read is a real one.
///
/// The compaction round puts the page it writes into the cache, so measuring on the same engine
/// would count zero reads and measure nothing. Reopening is what makes the count a count.
fn reopened_set(object_key: &str, fold: bool) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = engine_on(dir.path());
        load_on(&engine);
        for index in 0..MEMBERS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SetAdd {
                    key: object_key.to_string(),
                    member: member_bytes(index),
                },
            });
            assert!(response.status.ok, "a fixture write failed: {response:?}");
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

/// WHAT THE LISTING COSTS IN PAGE READS, FOLDED AGAINST UNFOLDED, ON ONE FIXTURE.
///
/// THE SAME CODE RUNS BOTH ARMS. What differs is the data shape: unfolded, each member has its own
/// page and its own address, so the listing reads a page per member exactly as it always did;
/// folded, every member's address is the one batched page, so the listing reads that page ONCE and
/// decodes every member out of it. So this measures what the fold is worth to a reader that takes
/// its members from the payload -- which is a saving the listing could not collect before, because
/// it asked for one element per entry and got one page read per ask.
///
/// COUNTS AND NOT TIMES, for the reason the sibling module states: a timing ratio on this box has
/// read 485x idle against 11x busy off identical code, so the instrument is the counter inside the
/// one place a page read past the cache is noted.
#[test]
fn what_the_fold_is_worth_to_a_listing_that_reads_the_payload() {
    println!(
        "\nload average: {}",
        std::fs::read_to_string("/proc/loadavg")
            .map(|text| text.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|_| "unavailable".to_string())
    );

    let mut rows: Vec<(&str, u64, usize)> = Vec::new();
    for (label, fold) in [("unfolded", false), ("folded", true)] {
        let key = format!("cost/{label}");
        let (engine, _dir) = reopened_set(&key, fold);
        let (addresses, entries) = addresses_and_entries(&engine, &key);
        let (members, reads) = page_reads_of(|| listed_members(&engine, &key));
        let mut got = members;
        got.sort();
        assert_eq!(
            expected_members(),
            got,
            "the {label} arm did not list the members that were written, so its read count is a \
             count over the wrong answer"
        );
        println!(
            "  {label:<9} pages={addresses:<3} entries={entries:<4} reads={reads:<4} \
             reads/member={:.3}",
            reads as f64 / MEMBERS as f64
        );
        rows.push((label, reads, addresses));
        drop(engine);
    }

    let unfolded = rows[0].1;
    let folded = rows[1].1;
    // PROOF BOTH ARMS DID WORK. A zero read count on both would satisfy any ratio.
    assert!(
        unfolded > 0 && folded > 0,
        "an arm read no pages at all ({unfolded} unfolded, {folded} folded), so the comparison is \
         between two unexercised arms"
    );
    assert_eq!(
        MEMBERS as u64, unfolded,
        "the unfolded arm read {unfolded} pages for {MEMBERS} members. One per member is the shape \
         this listing has always had when every member owns its own page"
    );
    assert!(
        folded < unfolded,
        "the folded arm read {folded} pages against the unfolded arm's {unfolded}. Folding the \
         members onto one page is supposed to let one read answer the whole listing"
    );
    println!(
        "  => the fold takes the listing from {unfolded} page reads to {folded} for the same \
         {MEMBERS} members"
    );
}
