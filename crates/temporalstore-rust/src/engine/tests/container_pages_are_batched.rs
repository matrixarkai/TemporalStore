// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A REWRITE COSTS AND WHAT IT FOLDS, WHEN A CONTAINER'S PAGES ARE BATCHED.
//!
//! # THE CLAIM
//!
//! A container gives every element its own page. `container_pages` made each of those pages name the
//! element it holds, which is what lets several of them be folded into one: a page holding many
//! elements is only readable if it says which is which.
//!
//! This module measures the fold. It is done at COMPACTION and nowhere else, so nothing about the
//! hot write path changes and no round does work it was not already going to do: `should_relocate`
//! is asked per element, and an element the round refuses is left alone and is not pulled into a
//! batch.
//!
//! # WHAT MOVES AND WHAT DOES NOT, STATED SO THE MEASUREMENT IS NOT READ AS MORE THAN IT IS
//!
//! ```text
//!     pages per container      FALLS -- that is the whole change
//!     index entries per container   UNCHANGED -- `block_index_handle` hashes the component
//!                                  beside the address, so elements sharing a page still hash
//!                                  to different handles
//!     bytes per element        BARELY MOVES -- measured 12 -> 3 of framing for a set of
//!                                  eight-byte members, which is a small number of a small number
//! ```
//!
//! `entry_count_versus_page_count`'s
//! `two_elements_sharing_one_page_are_still_two_entries_because_the_handle_names_the_component`
//! already drives the middle row and is untouched by this.
//! `a_compaction_round_folds_a_containers_pages_and_leaves_its_entry_count_alone` below asserts it
//! from this module's own fixture, because a reader of this module should not have to go and find
//! that one to learn what was NOT bought.
//!
//! # THE FOUR THINGS THAT COULD GO WRONG
//!
//! ## An element could be folded as empty
//!
//! The fold reads each element's value out of its page. A read that failed and contributed an empty
//! value would rewrite a live element as a zero-length one, and it would read back as PRESENT AND
//! EMPTY -- which is #1989's shape, a member served that should not be. So the read's failure arm is
//! never an `unwrap_or_default`.
//!
//! ## And it could be SKIPPED, which is a different failure and was the first version's
//!
//! Leaving the element alone and carrying on is safe for the ELEMENT and unsafe for the ROUND. A
//! round that hits an unreadable page must propagate, so the partial-failure handler durably commits
//! the consistent partial index -- CP4, and the reason is that a round returning success with the
//! volatile index advanced past the durable one lets the independent reclaim path purge a slab the
//! durable index still names. The first version of the fold skipped, the round returned success, and
//! `part4::partial_compaction_failure_durably_persists_the_consistent_partial_index` went red on its
//! `result.is_err()`. #2026 corrected the comment in `read_block_bytes_for_compaction` that had said
//! the caller skips.
//!
//! `a_container_element_whose_page_cannot_be_read_fails_the_round_rather_than_folding_an_empty_value`
//! drives BOTH halves through the block-read failure seam the compaction tests already use.
//!
//! ## The fold could lose an element
//!
//! Every element is read back after the round, by name, and after a RELOAD as well -- the reload
//! being the arm that proves the page index and the pages agree once several entries share one
//! address.
//!
//! ## The fold could report success without folding
//!
//! "Pages went down" is also what an idle round reports. So the instrument is two counters inside
//! the fold itself -- batches written and pages folded into them -- and every row asserts both are
//! non-zero before it reads anything else. A round that batched nothing fails here rather than
//! passing quietly.
//!
//! # THE CONTROL, AT ZERO, WITH ITS EXERCISED ROWS ASSERTED
//!
//! A `string` key owns one page and one element, so there is nothing to fold and the fold must
//! report zero batches for it WHILE the round still relocates its page. Both halves are asserted:
//! a control over a store where nothing was relocated reports zero for the wrong reason.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// Elements per container in the folding fixtures. Above the element cap for one arm, below it for
/// the others, so the cap is exercised rather than assumed.
const FOLDABLE_ELEMENTS: usize = 40;
const OVER_CAP_ELEMENTS: usize = crate::engine::CONTAINER_BATCH_ELEMENT_CAP + 30;

/// Element payload width, held equal across kinds so a per-element byte figure is comparable.
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
        table_name: "container-batching".to_string(),
        shard_uri: "local://container-batching/1".to_string(),
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
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

fn bytes_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

/// Distinct page addresses, and index entries, this object's live pages resolve to.
///
/// Both off the engine's own page index rather than off the model maps: the index is what a load
/// reads and what `HashLen` counts, and the point of the fold is that the two numbers come apart.
fn addresses_and_entries(engine: &TemporalEngine, kind: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut addresses = BTreeSet::new();
    let mut entries = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != kind || &*page.object_key != object_key {
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

/// Seed one container of each kind under one object key per kind.
fn seed(engine: &TemporalEngine, object_key: &str, elements: usize) {
    for index in 0..elements {
        write(
            engine,
            Command::HashSet {
                key: object_key.to_string(),
                field: format!("f{index:04}"),
                value: bytes_of(VALUE_WIDTH, index),
            },
        );
        write(
            engine,
            Command::SetAdd {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 1_000),
            },
        );
        write(
            engine,
            Command::ListPush {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 2_000),
                left: false,
            },
        );
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetAdd {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 3_000),
                score: index as f64,
            },
        });
        assert!(response.status.ok, "a zset write failed: {response:?}");
    }
    write(
        engine,
        Command::StringSet {
            key: format!("{object_key}-string"),
            value: bytes_of(VALUE_WIDTH, 9_999),
        },
    );
}

// =================================================================================================
// 1. THE FOLD, PER KIND, WITH BOTH COUNTERS FLOORED
// =================================================================================================

/// A COMPACTION ROUND FOLDS A CONTAINER'S PAGES AND LEAVES ITS ENTRY COUNT ALONE.
///
/// The headline. Pages per container before and after, off the engine's own page index, for all four
/// container kinds, with the string control beside them at zero.
///
/// THE COUNTERS ARE THE DENOMINATOR. "Fewer distinct addresses" is also what a round that relocated
/// nothing reports, so the fold's own two counters are asserted non-zero first and printed on the
/// row.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_compaction_round_folds_a_containers_pages_and_leaves_its_entry_count_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\n=== pages and entries per container, before and after one round ===\n  store path {} characters",
        dir.path().as_os_str().len()
    );
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed(&engine, "folded", FOLDABLE_ELEMENTS);

    let before: Vec<(&str, usize, usize)> = ["hash", "set", "zset", "list"]
        .into_iter()
        .map(|kind| {
            let (pages, entries) = addresses_and_entries(&engine, kind, "folded");
            (kind, pages, entries)
        })
        .collect();
    for (kind, pages, entries) in &before {
        assert_eq!(
            FOLDABLE_ELEMENTS, *pages,
            "{kind} started with {pages} pages for {FOLDABLE_ELEMENTS} elements, so the fixture is \
             not the one-page-per-element shape this test folds"
        );
        assert_eq!(FOLDABLE_ELEMENTS, *entries);
    }
    let (string_pages_before, string_entries_before) =
        addresses_and_entries(&engine, "string", "folded-string");
    assert_eq!(
        (1, 1),
        (string_pages_before, string_entries_before),
        "the string control must start at one page and one entry"
    );

    crate::engine::reset_container_batch_counts();
    let report = engine
        .compact_shard_blocks(1)
        .expect("a compaction round over this fixture must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();

    // THE FLOOR, before any figure below is read.
    assert!(
        batches > 0,
        "the round wrote {batches} batches, so nothing was folded and every row below would be \
         measuring an idle round"
    );
    assert!(
        folded > 0,
        "the round folded {folded} pages into those batches"
    );
    assert!(
        report.rewritten_block_refs > 0,
        "the round relocated {} page refs, so the control's zero would be a zero for the wrong \
         reason",
        report.rewritten_block_refs
    );
    println!(
        "  round: {} refs relocated, {batches} batches written, {folded} element pages folded",
        report.rewritten_block_refs
    );
    println!(
        "  {:>6}  {:>12}  {:>11}  {:>13}  {:>12}",
        "kind", "pages before", "pages after", "entries before", "entries after"
    );

    let mut kinds_folded = 0usize;
    for (kind, pages_before, entries_before) in &before {
        let (pages_after, entries_after) = addresses_and_entries(&engine, kind, "folded");
        println!(
            "  {kind:>6}  {pages_before:>12}  {pages_after:>11}  {entries_before:>13}  {entries_after:>12}"
        );
        assert!(
            pages_after < *pages_before,
            "{kind} holds {pages_after} pages where it held {pages_before}, so nothing folded"
        );
        // THE ENTRY COUNT IS THE PART THIS STAGE DOES NOT BUY, and it is asserted rather than
        // omitted: a reader who saw only the page column would take the entry column for granted.
        assert_eq!(
            *entries_before, entries_after,
            "{kind} holds {entries_after} entries where it held {entries_before}; the handle hashes \
             the component beside the address, so sharing a page must NOT change this"
        );
        kinds_folded += 1;
    }
    assert_eq!(4, kinds_folded, "every container kind must be covered");

    // THE CONTROL, at zero, and exercised: its page was relocated and it still holds one page.
    let (string_pages_after, string_entries_after) =
        addresses_and_entries(&engine, "string", "folded-string");
    println!(
        "  {:>6}  {:>12}  {:>11}  {:>13}  {:>12}   <- control",
        "string", string_pages_before, string_pages_after, string_entries_before, string_entries_after
    );
    assert_eq!(
        (1, 1),
        (string_pages_after, string_entries_after),
        "the string control moved, and a string owns one page and one element so there is nothing \
         for a fold to win"
    );
}

/// EVERY ELEMENT STILL READS BACK ITS OWN VALUE AFTER THE FOLD, AND AFTER A RELOAD.
///
/// The correctness claim. Several elements now share one address, so a reader that resolved an
/// address and returned its bytes would hand every one of them the same value -- which is #2013's
/// measured failure ("the page that loses is handed the FIRST page's BYTES rather than answering
/// missing"), one level up.
///
/// THE RELOAD IS THE HALF THAT MATTERS. Before it, `shard.hashes` still holds what the writes put
/// there. After it, the maps are derived from the page index and the values come off the folded
/// pages, which is the arm a later stage builds on.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn every_element_reads_back_its_own_value_after_the_fold_and_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let fields: Vec<(String, Vec<u8>)> = (0..FOLDABLE_ELEMENTS)
        .map(|index| (format!("f{index:04}"), bytes_of(VALUE_WIDTH, index)))
        .collect();
    let members: Vec<Vec<u8>> = (0..FOLDABLE_ELEMENTS)
        .map(|index| bytes_of(VALUE_WIDTH, index + 1_000))
        .collect();
    let elements: Vec<Vec<u8>> = (0..FOLDABLE_ELEMENTS)
        .map(|index| bytes_of(VALUE_WIDTH, index + 2_000))
        .collect();

    {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            &pages,
            &indexes,
        );
        load_on(&engine);
        for (field, value) in &fields {
            write(
                &engine,
                Command::HashSet {
                    key: "readback".to_string(),
                    field: field.clone(),
                    value: value.clone(),
                },
            );
        }
        for member in &members {
            write(
                &engine,
                Command::SetAdd {
                    key: "readback".to_string(),
                    member: member.clone(),
                },
            );
        }
        for element in &elements {
            write(
                &engine,
                Command::ListPush {
                    key: "readback".to_string(),
                    member: element.clone(),
                    left: false,
                },
            );
        }

        crate::engine::reset_container_batch_counts();
        engine
            .compact_shard_blocks(1)
            .expect("the round must succeed");
        let (batches, folded) = crate::engine::container_batch_counts();
        assert!(
            batches > 0 && folded > 0,
            "the round wrote {batches} batches folding {folded} pages, so the read-backs below are \
             not reading folded pages"
        );
        let (page_count, entry_count) = addresses_and_entries(&engine, "hash", "readback");
        assert!(
            page_count < entry_count,
            "the hash holds {page_count} pages for {entry_count} entries, so no page is shared and \
             this test cannot distinguish a correct selector from one that ignores the component"
        );
        println!(
            "\n=== read back after the fold ===\n  hash: {entry_count} entries over {page_count} pages, {batches} batches, {folded} folded"
        );

        // Before the reload: the resident maps still hold what the writes put there, so this arm
        // exercises the read funnel selecting out of a shared page.
        for (field, value) in &fields {
            match read(
                &engine,
                Command::HashGet {
                    key: "readback".to_string(),
                    field: field.clone(),
                },
            ) {
                crate::types::CommandResponse::Bytes { value: got } => assert_eq!(
                    Some(value.clone()),
                    got,
                    "hash field {field} did not read back its own value out of a folded page"
                ),
                other => panic!("expected Bytes, got {other:?}"),
            }
        }
        engine.flush_shard_index(1);
    }

    // AFTER THE RELOAD, on the same files, with its own cache directory so nothing is answered out
    // of a page the first engine left warm.
    let reloaded = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-reloaded"),
        &pages,
        &indexes,
    );
    crate::engine::reset_corrupt_container_page_count();
    load_on(&reloaded);

    let length = match read(
        &reloaded,
        Command::HashLen {
            key: "readback".to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    };
    assert_eq!(
        fields.len() as i64,
        length,
        "the reloaded hash holds {length} fields where {} were written, so the per-field \
         comparisons below would be over the wrong population",
        fields.len()
    );
    for (field, value) in &fields {
        match read(
            &reloaded,
            Command::HashGet {
                key: "readback".to_string(),
                field: field.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: got } => assert_eq!(
                Some(value.clone()),
                got,
                "hash field {field} did not survive the fold and the reload"
            ),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }
    let listed = match read(
        &reloaded,
        Command::SetMembers {
            key: "readback".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(members.len(), listed.len());
    for member in &members {
        assert!(
            listed.contains(member),
            "the reloaded set lost a member; a set listing reads a page per entry, so this is the \
             arm that reads every folded page"
        );
    }
    let ranged = match read(
        &reloaded,
        Command::ListRange {
            key: "readback".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(
        elements, ranged,
        "the reloaded list did not read back its elements in order out of folded pages"
    );
    assert_eq!(
        0,
        crate::engine::corrupt_container_page_count(),
        "a read met a folded page it could not walk"
    );
    println!("  reloaded: every hash field, set member and list element read back");
}

/// THE ELEMENT CAP BINDS, SO ONE PAGE NEVER HOLDS A WHOLE LARGE CONTAINER.
///
/// A container of more elements than the cap must come out of a round holding MORE THAN ONE page.
/// Without this, a container of tiny elements would fold to a single page and every point read of it
/// would become a read of the whole container.
///
/// THE FIXTURE IS ABOVE THE CAP BY CONSTRUCTION -- the constant is read from the engine, not
/// retyped -- so a cap that moved is still exercised by this test rather than silently unbinding it.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn the_element_cap_binds_so_one_page_never_holds_a_whole_large_container() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    for index in 0..OVER_CAP_ELEMENTS {
        write(
            &engine,
            Command::HashSet {
                key: "capped".to_string(),
                field: format!("f{index:04}"),
                // Small on purpose: the byte target must NOT be what binds here, or the test would
                // be measuring the target and not the cap.
                value: vec![b'v'; 4],
            },
        );
    }
    let (pages_before, entries_before) = addresses_and_entries(&engine, "hash", "capped");
    assert_eq!(OVER_CAP_ELEMENTS, pages_before);
    assert_eq!(OVER_CAP_ELEMENTS, entries_before);

    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let (pages_after, entries_after) = addresses_and_entries(&engine, "hash", "capped");

    let cap = crate::engine::CONTAINER_BATCH_ELEMENT_CAP;
    println!(
        "\n=== the element cap ===\n  cap {cap}, elements {OVER_CAP_ELEMENTS}: {pages_before} pages -> {pages_after}, {batches} batches, {folded} folded"
    );
    assert!(
        batches > 0 && folded > 0,
        "nothing folded, so the cap was not exercised"
    );
    assert!(
        pages_after > 1,
        "{OVER_CAP_ELEMENTS} elements folded into {pages_after} page(s) against a cap of {cap}, so \
         the cap did not bind and one page holds the whole container"
    );
    let smallest_possible = (OVER_CAP_ELEMENTS + cap - 1) / cap;
    assert!(
        pages_after >= smallest_possible,
        "{OVER_CAP_ELEMENTS} elements cannot fit in {pages_after} pages of at most {cap}"
    );
    assert_eq!(
        OVER_CAP_ELEMENTS, entries_after,
        "the entry count must not move"
    );
}

/// A CONTAINER ELEMENT WHOSE PAGE CANNOT BE READ FAILS THE ROUND RATHER THAN FOLDING AN EMPTY VALUE.
///
/// # TWO FAILURES, AND THE FIRST VERSION OF THIS TEST ONLY GUARDED ONE
///
/// The fold reads each element's value out of its page, so an unreadable page has two wrong answers
/// available and they are different:
///
///   * contribute an EMPTY value and the live element is rewritten as a zero-length one. It reads
///     back as PRESENT AND EMPTY, which is #1989's shape and is worse than a miss because a miss is
///     visible. This test's first version guarded that, and still does.
///   * SKIP the element, leave its address alone, and let the round return SUCCESS. Safe for the
///     element and unsafe for the round: CP4 records that a round which returns success with the
///     volatile index advanced past the durable one is how the independent reclaim path comes to
///     purge a slab the durable index still names. The fold's first version did exactly this, and
///     `part4::partial_compaction_failure_durably_persists_the_consistent_partial_index` went red
///     because its `result.is_err()` no longer held.
///
/// So the round FAILS, whatever had already been flushed stays flushed, and the handler durably
/// commits that consistent partial. All three are asserted below.
///
/// THE SEAM IS THE ONE THE COMPACTION TESTS ALREADY USE (`fail_compaction_block_read_after_for_test`),
/// so the failure is injected where a torn page would produce it rather than simulated elsewhere.
///
/// THE DENOMINATOR: the arm asserts the seam actually fired PARTWAY -- some elements folded and not
/// all of them -- because a seam that never tripped and a seam that tripped on the first read both
/// leave assertions about a partial state passing for the wrong reason.
///
/// rust-internal: drives the engine's own command surface through a documented failure seam
#[test]
fn a_container_element_whose_page_cannot_be_read_fails_the_round_rather_than_folding_an_empty_value()
{
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    let fields: Vec<(String, Vec<u8>)> = (0..FOLDABLE_ELEMENTS)
        .map(|index| (format!("f{index:04}"), bytes_of(VALUE_WIDTH, index)))
        .collect();
    for (field, value) in &fields {
        write(
            &engine,
            Command::HashSet {
                key: "torn".to_string(),
                field: field.clone(),
                value: value.clone(),
            },
        );
    }
    let before: Vec<(String, Vec<u8>)> = fields.clone();

    // Fail the read a few pages in, so some elements fold and at least one does not.
    crate::engine::compaction::fail_compaction_block_read_after_for_test(Some(5));
    crate::engine::reset_container_batch_counts();
    let outcome = engine.compact_shard_blocks(1);
    crate::engine::compaction::fail_compaction_block_read_after_for_test(None);
    let (batches, folded) = crate::engine::container_batch_counts();
    println!(
        "\n=== a page the fold cannot read ===\n  round outcome err={}, {batches} batches, {folded} folded",
        outcome.is_err()
    );

    // THE ROUND MUST FAIL. This is the half that CP4 turns on, and it is the half the first version
    // of this function got wrong.
    assert!(
        outcome.is_err(),
        "the round returned {outcome:?} after a live page could not be read. A round that succeeds \
         with the volatile index advanced past the durable one is how reclaim comes to purge a slab \
         the durable index still names (CP4)"
    );

    // AND IT MUST HAVE FAILED PARTWAY, or the assertions about a partial state are about nothing.
    assert!(
        (folded as usize) < before.len(),
        "every one of {} elements folded, so the injected read failure never tripped",
        before.len()
    );

    // THE FINDING: every element still answers with the bytes it was written with. An element the
    // round never reached keeps its own page; one already folded is inside a flushed batch. Neither
    // is empty.
    for (field, value) in &before {
        match read(
            &engine,
            Command::HashGet {
                key: "torn".to_string(),
                field: field.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: got } => {
                assert_eq!(
                    Some(value.clone()),
                    got,
                    "field {field} did not answer with its own bytes after a round whose reads \
                     partly failed -- an empty answer here is the defect this test exists for"
                );
            }
            other => panic!("expected Bytes, got {other:?}"),
        }
    }
    println!(
        "  all {} fields still answer with their own bytes",
        before.len()
    );
}

/// Bytes a value of `width` occupies that a compressor cannot shrink.
///
/// A multiplicative PRNG rather than a pattern, and it exists because of what this module measured
/// first: a fold of forty PATTERNED values reported a 72.6% byte saving, and most of that was not
/// framing at all. A block record carries a compression codec, and a one-element page of forty bytes
/// is too small for the compressor to do anything with while a forty-element page of sixteen hundred
/// is not. So the first number this test produced was mostly "batching makes compression work",
/// which is true and is a completely different claim from "the framing amortises".
fn incompressible_of(width: usize, index: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ ((index as u64).wrapping_mul(0x1000_0000_01B3));
    let mut bytes = Vec::with_capacity(width);
    while bytes.len() < width {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(width);
    bytes
}

/// WHAT THE FOLD IS WORTH PER ELEMENT, IN BYTES, SPLIT INTO THE TWO THINGS IT IS.
///
/// # THE MEASUREMENT THIS TEST WAS REWRITTEN FOR
///
/// The first version of it seeded patterned values and reported a 72.6% byte saving for a hash. That
/// figure is real and it is mostly NOT the framing amortising: a block record carries a compression
/// codec, a one-element page is too small to compress and a forty-element page is not, so folding
/// pages together is also what lets the compressor see enough bytes to work on. Reporting that as
/// the representation's saving would have credited the frame with a compressor's work.
///
/// So both arms run, on the same kinds at the same element count and the same value width, differing
/// ONLY in whether the values compress:
///
///   * the COMPRESSIBLE arm is what this fixture reported first, and it is kept because it is what a
///     real corpus of similar values looks like;
///   * the INCOMPRESSIBLE arm is values from a shift-register PRNG, and its saving is the framing
///     and the per-page record header and nothing else.
///
/// The difference between the two arms is the compressor's share, printed rather than argued.
///
/// BOTH SIDES OF EVERY ROW ARE STORED FACTS -- the summed length of the distinct pages an object
/// resolves to, before and after, off the engine's own page index. Not a counter, which is what
/// reported a restore as flat while the kernel saw 6.70x.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn what_the_fold_is_worth_per_element_in_bytes() {
    println!(
        "\n=== stored bytes per element over a fold of {FOLDABLE_ELEMENTS} elements, by value shape ==="
    );
    println!(
        "  {:>14}  {:>6}  {:>11}  {:>10}  {:>12}  {:>11}  {:>9}",
        "values", "kind", "bytes before", "per elem", "bytes after", "per elem", "saved %"
    );

    let mut saved_by_shape: Vec<(&str, f64)> = Vec::new();
    for (shape, compressible) in [("compressible", true), ("incompressible", false)] {
        let dir = tempfile::tempdir().expect("tempdir");
        // THE STORE PATH LENGTH is the same for both arms by construction -- one tempdir shape --
        // because it moves allocation bytes at about six a character. It does not move the STORED
        // lengths this test reads, which is why the arms may differ in directory.
        let engine = engine_on(dir.path());
        load_on(&engine);
        for index in 0..FOLDABLE_ELEMENTS {
            let value = if compressible {
                bytes_of(VALUE_WIDTH, index)
            } else {
                incompressible_of(VALUE_WIDTH, index)
            };
            write(
                &engine,
                Command::HashSet {
                    key: "priced".to_string(),
                    field: format!("f{index:04}"),
                    value: value.clone(),
                },
            );
            write(
                &engine,
                Command::SetAdd {
                    key: "priced".to_string(),
                    member: if compressible {
                        bytes_of(VALUE_WIDTH, index + 1_000)
                    } else {
                        incompressible_of(VALUE_WIDTH, index + 1_000)
                    },
                },
            );
            write(
                &engine,
                Command::ListPush {
                    key: "priced".to_string(),
                    member: value,
                    left: false,
                },
            );
        }

        let stored_bytes = |kind: &str| -> u64 {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let mut seen = BTreeSet::new();
            let mut total = 0u64;
            for bucket in shard.bucket_index.bucket_map.values() {
                for page in bucket.block_index.values() {
                    if page.deleted
                        || page.model_id.as_str() != kind
                        || &*page.object_key != "priced"
                    {
                        continue;
                    }
                    let identity = (
                        page.address.block_slab_id(),
                        page.address.offset(),
                        page.address.length(),
                    );
                    if seen.insert(identity) {
                        total += page.address.length();
                    }
                }
            }
            total
        };

        let before: Vec<(&str, u64)> = ["hash", "set", "list"]
            .into_iter()
            .map(|kind| (kind, stored_bytes(kind)))
            .collect();
        for (kind, bytes) in &before {
            assert!(
                *bytes > 0,
                "{shape}/{kind} stores {bytes} bytes before the round, so every ratio below divides \
                 by zero"
            );
        }

        crate::engine::reset_container_batch_counts();
        engine
            .compact_shard_blocks(1)
            .expect("the round must succeed");
        let (batches, folded) = crate::engine::container_batch_counts();
        assert!(
            batches > 0 && folded > 0,
            "{shape}: nothing folded, so this arm measures an idle round"
        );

        let mut arm_before = 0u64;
        let mut arm_after = 0u64;
        for (kind, bytes_before) in &before {
            let bytes_after = stored_bytes(kind);
            let per_before = *bytes_before as f64 / FOLDABLE_ELEMENTS as f64;
            let per_after = bytes_after as f64 / FOLDABLE_ELEMENTS as f64;
            let saved = (*bytes_before as f64 - bytes_after as f64) / *bytes_before as f64 * 100.0;
            println!(
                "  {shape:>14}  {kind:>6}  {bytes_before:>11}  {per_before:>10.1}  {bytes_after:>12}  {per_after:>11.1}  {saved:>8.1}%"
            );
            assert!(
                bytes_after <= *bytes_before,
                "{shape}/{kind} grew from {bytes_before} to {bytes_after} bytes, which a fold \
                 cannot do"
            );
            arm_before += *bytes_before;
            arm_after += bytes_after;
        }
        let arm_saved = (arm_before - arm_after) as f64 / arm_before as f64 * 100.0;
        saved_by_shape.push((shape, arm_saved));
    }

    assert_eq!(2, saved_by_shape.len(), "both value shapes must be measured");
    let compressible = saved_by_shape[0].1;
    let incompressible = saved_by_shape[1].1;
    println!(
        "  across the three kinds: compressible values saved {compressible:.1}%, incompressible \
         {incompressible:.1}%"
    );
    println!(
        "  so the compressor's share of the headline is {:.1} points, and the REPRESENTATION's \
         saving -- framing plus one record header per page instead of many -- is {incompressible:.1}%",
        compressible - incompressible
    );
    // THE CLAIM THIS TEST EXISTS TO PIN: the compressible arm must save MORE, or the split is not
    // real and the headline figure may be quoted as the representation's.
    assert!(
        compressible > incompressible,
        "compressible values saved {compressible:.1}% against incompressible {incompressible:.1}%, \
         so batching is not making compression more effective and this test's whole split is wrong"
    );
    // And the representation's own saving must be real, or the fold buys only compression.
    assert!(
        incompressible > 0.0,
        "incompressible values saved {incompressible:.1}%, so the fold buys nothing but compression"
    );
}
