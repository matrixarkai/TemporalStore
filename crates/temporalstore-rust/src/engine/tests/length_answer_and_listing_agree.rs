// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A LENGTH ANSWER AND ITS LISTING MUST AGREE, AND NOTHING SAID SO.
//!
//! # THE CONTRACT, ON ITS OWN TERMS
//!
//! For a container, two commands answer the same question in two shapes: one returns HOW MANY
//! elements the object holds, the other returns the elements. The number the first answers must be
//! the number of elements the second returns. That is not a nicety -- a client that sizes a buffer
//! from the count and then fills it from the listing is correct only if the two agree, and a count
//! that is right about a population the listing cannot produce is a wrong count, not a fast one.
//!
//! Nothing in this engine asserted it. This module does, per kind, and the assertion is the
//! deliverable: a future storage change that breaks the equality fails the build instead of
//! answering a number nobody can reconcile.
//!
//! # THE FOUR KINDS, AND WHICH PAIRS ACTUALLY EXIST
//!
//! Enumerated off `Command` and off the RESP dispatch, because the engine enum and the wire
//! surface do not hold the same set and a table built from either alone is wrong:
//!
//!   * HASH -- `Command::HashLen` and `Command::HashGetAll`. THE PAIR WITH TWO PATHS. The count is
//!     `bucket_index_component_block_addresses(shard, "hash", &key).len()`, which counts PAGE-INDEX
//!     ENTRIES and reads no page. The listing walks the same entries and then reads a page per
//!     entry through `read_block_bytes`, collecting through a `filter_map` -- so an entry whose
//!     page does not read is counted by the length and ABSENT from the listing. The two agree today
//!     because entries and elements are one-to-one and every page reads, which is a property of the
//!     storage and not a contract either side states.
//!   * LIST -- `Command::ListLen` and `Command::ListRange` over `0..=-1`. Both take the length from
//!     `shard.lists`, so the number itself has one source; the listing then reads a page per
//!     element through the same dropping `filter_map`. One shared source, one unshared read.
//!   * ZSET -- `Command::ZSetCard` and `Command::ZSetRange` over `0..=-1`. BOTH read `shard.zsets`
//!     and the listing reads no page at all: member and score come out of the map. Same structure
//!     for both answers, which is the shape that cannot diverge. Its listing is INTERLEAVED
//!     member/score, so the element count is half the returned length -- asserted even below,
//!     because an odd length would make the halving quietly wrong.
//!   * SET -- there is NO `Command` variant for a set's length. `SCARD` exists on the wire and its
//!     dispatch arm is `Command::SetMembers { .. }` followed by `members.len()`: the count IS the
//!     listing, counted. It cannot disagree with itself, and that is asserted here as the positive
//!     control for the property the other kinds only happen to have.
//!
//! One more length command has no listing at all: `Command::SeenCard` answers a set's size and no
//! command lists that set's members, so there is no pair to check. It is named here so a later
//! reader does not conclude it was missed.
//!
//! # WHY NOT SIMPLY MAKE THE COUNT READ THE MODEL MAP
//!
//! Because that breaks the same contract in the other direction, and this tree has the incident.
//! #1989 is the recorded case: a member was served but undeletable because the listing answered
//! from the page index while `shard.sets` had a single reader.
//!
//! ONE HALF OF THE ARGUMENT THIS PARAGRAPH USED TO MAKE HAS EXPIRED. It said `hashes` is
//! `skip_serializing` and is rebuilt FROM the bucket index on load. `hashes` carries
//! `#[serde(default)]` now and IS durable, so "it is not persisted" is no longer a reason for
//! anything. The conclusion below does not rest on it.
//!
//! THE HALF THAT HOLDS IS THE DELTA-FOLD ROUTE, and it is worth stating exactly, because it is
//! the mechanism and not the slogan. `fold_index_log_deltas` applies a record's page items
//! through `fold_delta_block_items` for EVERY record, but it adds container elements only from
//! the `key_states` blobs that carry one of `CARRIED_CONTAINER_FIELDS` -- and the fold's own
//! comment says a fold of records that predate the carry holds nothing. So a pre-carry delta
//! record leaves the page index an entry and the container nothing.
//!
//! AND WHAT RESCUES THAT IS ITSELF A READ OF THE PAGE INDEX, which is the part that decides
//! whether a reader may move. `fill_absent_elements` completes the durable map from the derived
//! view, and `an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name` in
//! `durable_outranks_derived` pins exactly that: an element the durable map lacks still comes
//! back from its name. So moving a count onto the container would not stop the bucket index
//! being the thing that makes the container complete -- it would move that dependency from read
//! time to load time. The page index is the right source for a hash today, and the fix for two
//! paths that agree by coincidence is to ASSERT the agreement, not to move one of them.
//!
//! # THE FIXTURE IS BUILT SO THE TWO COULD DIVERGE IF THE FILING CHANGED
//!
//! A run of appends would not exercise anything: every element would add one entry and one page
//! and any filing scheme at all would keep the two equal. So each kind is driven through the three
//! operations that file differently from each other:
//!
//!   * APPENDS, several, so the count is well above zero;
//!   * a DELETION, which removes an entry -- `mark_bucket_index_block_deleted_with` is a `retain`
//!     returning false, so the row goes away rather than being flagged, and a listing that filtered
//!     on a flag instead would diverge here; and
//!   * a REWRITE of an element that already exists, which adds NO entry. #2008 measured HLEN 7
//!     before and 7 after exactly this, and a scheme that filed a rewrite as a new row would show
//!     up as a count above the listing.
//!
//! A list has no in-place rewrite command -- `ListPush` always appends -- so its third operation is
//! a re-push after the pop, and the module says so rather than pretending the coverage is uniform.
//!
//! # THE EQUALITY IS ASSERTED NON-VACUOUSLY
//!
//! Two answers of zero satisfy "equal" perfectly, and a fixture that silently wrote nothing --
//! wrong key, unloaded shard, a kind that stopped accepting the command -- produces exactly that.
//! So every row asserts the count is EQUAL and that it is the population the fixture actually
//! wrote, which is a positive number stated per kind. The floor is printed with the row.

#![allow(clippy::all)]
use super::*;

/// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default since
/// #1973. THE OPERATOR'S RANGE -- not `u32::MAX`, under which every key lands in a bucket of its
/// own BY CONSTRUCTION and a per-object measurement becomes an artefact of the default.
const OPERATOR_END: u32 = 1023;

/// Elements appended per object before anything is removed or rewritten.
const APPENDED: usize = 6;

/// Elements deleted from each object. One is enough to move the population; the point is that the
/// count and the listing move together, not how far.
const DELETED: usize = 1;

/// What every kind must hold once the fixture has run: the appends less the deletion. A rewrite
/// adds nothing, and a list's re-push is counted below where it is done.
const EXPECTED: usize = APPENDED - DELETED;

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
        table_name: "length-answer-and-listing".to_string(),
        shard_uri: "local://length-answer-and-listing/1".to_string(),
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

/// The number a length command answered, refusing any other response shape by name.
fn length_answer(engine: &TemporalEngine, command: Command) -> i64 {
    match read(engine, command) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!(
            "a length command answered {other:?} rather than an integer, so there is no number to \
             compare against its listing"
        ),
    }
}

/// THE ONE ASSERTION THIS MODULE EXISTS FOR, in one place so every kind is held to the same words.
///
/// `counted` is what the length command answered; `listed` is how many ELEMENTS the listing
/// command returned. `floor` is the population the fixture wrote, so a pair that both answered
/// zero -- which satisfies equality and is this project's most repeated way of passing without
/// testing anything -- fails on the second assertion rather than reporting a clean row.
fn assert_count_matches_listing(kind: &str, counted: i64, listed: usize, floor: usize) {
    println!(
        "  {kind:<5} length answered {counted:>3}, listing returned {listed:>3} elements, \
         fixture wrote {floor:>3}"
    );
    assert_eq!(
        counted,
        listed as i64,
        "the {kind} length command answered {counted} and its listing returned {listed} elements. \
         These are two answers to one question and they must be the same number; a client that \
         sizes from the count and fills from the listing is wrong by {} elements",
        (counted - listed as i64).abs()
    );
    assert!(
        floor > 0,
        "the {kind} row declares a floor of zero, so the equality above could be satisfied by two \
         empty answers and this row would assert nothing"
    );
    assert_eq!(
        counted, floor as i64,
        "the {kind} length answered {counted} for the {floor} elements the fixture wrote. The \
         count and the listing agree with each other but not with the population, so the fixture \
         is not reaching the state this row claims to describe"
    );
}

// =================================================================================================
// 1. THE PAIRS THAT EXIST, EACH DRIVEN THROUGH APPEND / DELETE / REWRITE
// =================================================================================================

/// A HASH'S LENGTH ANSWER AND ITS LISTING RETURN THE SAME NUMBER.
///
/// The pair with two genuinely different paths: `HashLen` counts page-index entries and reads no
/// page; `HashGetAll` walks the same entries and reads a page for each, dropping any that does not
/// read. So this row is the one that would move first if the filing or the page layout changed
/// under either side.
///
/// rust-internal: drives HashSet, HashDelete, HashLen and HashGetAll, no external surface
#[test]
fn a_hash_length_answer_equals_its_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let key = "agree-hash";

    for f in 0..APPENDED {
        write(
            &engine,
            Command::HashSet {
                key: key.to_string(),
                field: format!("field-{f}"),
                value: format!("value-{f}").into_bytes(),
            },
        );
    }
    // A DELETION removes the entry outright rather than flagging it.
    write(
        &engine,
        Command::HashDelete {
            key: key.to_string(),
            field: "field-0".to_string(),
        },
    );
    // A REWRITE of a surviving field adds no entry -- #2008 measured HLEN 7 before and 7 after.
    write(
        &engine,
        Command::HashSet {
            key: key.to_string(),
            field: "field-1".to_string(),
            value: b"rewritten-and-longer-than-the-original".to_vec(),
        },
    );

    let counted = length_answer(
        &engine,
        Command::HashLen {
            key: key.to_string(),
        },
    );
    let listed = match read(
        &engine,
        Command::HashGetAll {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("HashGetAll answered {other:?}"),
    };
    println!("\n=== hash === HLEN against HGETALL");
    // The listing's own field names must be distinct, or a listing of N rows would not be a
    // listing of N elements and the comparison against a count would be meaningless.
    let distinct: std::collections::BTreeSet<&str> =
        listed.iter().map(|(field, _)| field.as_str()).collect();
    assert_eq!(
        distinct.len(),
        listed.len(),
        "the listing returned {} rows over {} distinct fields, so its length is not an element \
         count",
        listed.len(),
        distinct.len()
    );
    assert!(
        !distinct.contains("field-0"),
        "the deleted field is still in the listing, so the deletion did not take and this row is \
         not driving the state it claims"
    );
    assert_count_matches_listing("hash", counted, listed.len(), EXPECTED);
}

/// A LIST'S LENGTH ANSWER EQUALS THE NUMBER OF ELEMENTS ITS RANGE RETURNS.
///
/// `ListLen` and `ListRange` take the length from the same map, so the NUMBER has one source; the
/// listing then reads a page per element and drops what does not read, which the count never does.
/// A list has no in-place rewrite -- `ListPush` appends -- so the third operation here is a
/// re-push after the pop, and the expected population says so.
///
/// rust-internal: drives ListPush, ListPop, ListLen and ListRange, no external surface
#[test]
fn a_list_length_answer_equals_its_range_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let key = "agree-list";

    for m in 0..APPENDED {
        write(
            &engine,
            Command::ListPush {
                key: key.to_string(),
                member: format!("member-{m}").into_bytes(),
                left: false,
            },
        );
    }
    write(
        &engine,
        Command::ListPop {
            key: key.to_string(),
            left: true,
        },
    );
    // No rewrite command exists for a list, so the third operation is a re-push: it adds one back,
    // which the expected population below accounts for explicitly rather than by coincidence.
    write(
        &engine,
        Command::ListPush {
            key: key.to_string(),
            member: b"member-repushed".to_vec(),
            left: false,
        },
    );
    let expected = EXPECTED + 1;

    let counted = length_answer(
        &engine,
        Command::ListLen {
            key: key.to_string(),
        },
    );
    let listed = match read(
        &engine,
        Command::ListRange {
            key: key.to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("ListRange answered {other:?}"),
    };
    println!("\n=== list === LLEN against LRANGE 0 -1");
    assert_count_matches_listing("list", counted, listed.len(), expected);
}

/// A ZSET'S CARDINALITY EQUALS THE NUMBER OF MEMBERS ITS RANGE RETURNS.
///
/// Both answers come out of `shard.zsets` and the listing reads no page, so this pair has the
/// shape the others only happen to have. It is still asserted: the value of the assertion is that
/// a later change moving either side off that map fails here instead of shipping.
///
/// The listing is INTERLEAVED member/score, so an element count is half the returned length. The
/// evenness is asserted first -- an odd length would make the halving silently wrong, and a
/// truncating division would hide it.
///
/// rust-internal: drives ZSetAdd, ZSetRemove, ZSetCard and ZSetRange, no external surface
#[test]
fn a_zset_cardinality_equals_its_range_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let key = "agree-zset";

    for m in 0..APPENDED {
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: format!("member-{m}").into_bytes(),
                score: m as f64,
            },
        );
    }
    write(
        &engine,
        Command::ZSetRemove {
            key: key.to_string(),
            member: b"member-0".to_vec(),
        },
    );
    // A RE-SCORE of an existing member is this kind's rewrite: `ZSetAdd` answers 0 for it and must
    // not add a member.
    write(
        &engine,
        Command::ZSetAdd {
            key: key.to_string(),
            member: b"member-1".to_vec(),
            score: 99.0,
        },
    );

    let counted = length_answer(
        &engine,
        Command::ZSetCard {
            key: key.to_string(),
        },
    );
    let returned = match read(
        &engine,
        Command::ZSetRange {
            key: key.to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("ZSetRange answered {other:?}"),
    };
    println!("\n=== zset === ZCARD against ZRANGE 0 -1");
    assert_eq!(
        returned.len() % 2,
        0,
        "the listing returned {} interleaved member/score values, which is odd -- so it is not \
         whole pairs and halving it would report an element count that does not exist",
        returned.len()
    );
    assert_count_matches_listing("zset", counted, returned.len() / 2, EXPECTED);
}

/// A SET'S COUNT IS ITS LISTING, COUNTED -- THE POSITIVE CONTROL.
///
/// There is no `Command` variant for a set's length. On the wire `SCARD` exists, and its dispatch
/// arm executes `Command::SetMembers` and answers `members.len()`. So for this kind the count and
/// the listing are not two paths that agree, they are ONE path counted two ways, and the equality
/// holds by construction rather than by coincidence.
///
/// This row is the control for the whole module: it shows what the property looks like when the
/// structure guarantees it, against the hash row where it is a coincidence the hash row now pins.
/// The count is taken the way the dispatch takes it, off the listing, so this row asserts the
/// dispatch's arithmetic and the fixture's population rather than restating a tautology.
///
/// rust-internal: drives SetAdd, SetRemove and SetMembers, no external surface
#[test]
fn a_set_count_is_its_listing_counted_and_has_no_second_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let key = "agree-set";

    for m in 0..APPENDED {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: format!("member-{m}").into_bytes(),
            },
        );
    }
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: b"member-0".to_vec(),
        },
    );
    // Re-adding a member that is already present is this kind's rewrite and must add nothing.
    write(
        &engine,
        Command::SetAdd {
            key: key.to_string(),
            member: b"member-1".to_vec(),
        },
    );

    let listed = match read(
        &engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    // Exactly what `dispatch.rs` answers for SCARD.
    let counted = listed.len() as i64;
    println!("\n=== set === SCARD (which is SMEMBERS counted) against SMEMBERS");
    let distinct: std::collections::BTreeSet<&[u8]> =
        listed.iter().map(|member| member.as_slice()).collect();
    assert_eq!(
        distinct.len(),
        listed.len(),
        "the listing returned {} members over {} distinct values, so a set is holding a duplicate \
         and its length would count an element twice",
        listed.len(),
        distinct.len()
    );
    assert_count_matches_listing("set", counted, listed.len(), EXPECTED);
}

// =================================================================================================
// 2. THE SAME EQUALITY ACROSS A RELOAD
// =================================================================================================

/// THE EQUALITY SURVIVES AN UNLOAD AND A LOAD, WHICH IS WHERE THE TWO PATHS COULD PART.
///
/// The section above reads a warm shard, where the listing's page reads are served from resident
/// state. A reload is the one routine event that makes the listing go through `decode_block_record`
/// while the count still comes off the index -- and `decode_block_record` REFUSES a page whose
/// address block id and header block id disagree, answering nothing for it. A refusal there is
/// invisible to a count that never reads a page: the length would keep answering the entry count
/// while the listing came back short. That is the concrete way this contract breaks in this engine,
/// so it is driven rather than described.
///
/// Every kind that has a pair is checked, so a kind whose reload path changes cannot pass on the
/// strength of the others.
///
/// rust-internal: drives the engine's own unload/load cycle, no external surface
#[test]
fn every_length_answer_still_equals_its_listing_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    for m in 0..APPENDED {
        write(
            &engine,
            Command::HashSet {
                key: "reload-hash".to_string(),
                field: format!("field-{m}"),
                value: format!("value-{m}").into_bytes(),
            },
        );
        write(
            &engine,
            Command::ListPush {
                key: "reload-list".to_string(),
                member: format!("member-{m}").into_bytes(),
                left: false,
            },
        );
        write(
            &engine,
            Command::ZSetAdd {
                key: "reload-zset".to_string(),
                member: format!("member-{m}").into_bytes(),
                score: m as f64,
            },
        );
        write(
            &engine,
            Command::SetAdd {
                key: "reload-set".to_string(),
                member: format!("member-{m}").into_bytes(),
            },
        );
    }
    write(
        &engine,
        Command::HashDelete {
            key: "reload-hash".to_string(),
            field: "field-0".to_string(),
        },
    );
    write(
        &engine,
        Command::ListPop {
            key: "reload-list".to_string(),
            left: true,
        },
    );
    write(
        &engine,
        Command::ZSetRemove {
            key: "reload-zset".to_string(),
            member: b"member-0".to_vec(),
        },
    );
    write(
        &engine,
        Command::SetRemove {
            key: "reload-set".to_string(),
            member: b"member-0".to_vec(),
        },
    );

    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    println!("\n=== after a reload === every pair, cold");
    let hash_counted = length_answer(
        &engine,
        Command::HashLen {
            key: "reload-hash".to_string(),
        },
    );
    let hash_listed = match read(
        &engine,
        Command::HashGetAll {
            key: "reload-hash".to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    };
    assert_count_matches_listing("hash", hash_counted, hash_listed, EXPECTED);

    let list_counted = length_answer(
        &engine,
        Command::ListLen {
            key: "reload-list".to_string(),
        },
    );
    let list_listed = match read(
        &engine,
        Command::ListRange {
            key: "reload-list".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members.len(),
        other => panic!("ListRange answered {other:?}"),
    };
    assert_count_matches_listing("list", list_counted, list_listed, EXPECTED);

    let zset_counted = length_answer(
        &engine,
        Command::ZSetCard {
            key: "reload-zset".to_string(),
        },
    );
    let zset_returned = match read(
        &engine,
        Command::ZSetRange {
            key: "reload-zset".to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("ZSetRange answered {other:?}"),
    };
    assert_eq!(
        zset_returned.len() % 2,
        0,
        "the reloaded listing returned an odd {} interleaved values",
        zset_returned.len()
    );
    assert_count_matches_listing("zset", zset_counted, zset_returned.len() / 2, EXPECTED);

    let set_listed = match read(
        &engine,
        Command::SetMembers {
            key: "reload-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members.len(),
        other => panic!("SetMembers answered {other:?}"),
    };
    assert_count_matches_listing("set", set_listed as i64, set_listed, EXPECTED);
}

// =================================================================================================
// 3. THE GUARD CAN FAIL
// =================================================================================================

/// THE EQUALITY CHECK GOES RED WHEN THE TWO NUMBERS DIFFER BY ONE.
///
/// `assert_count_matches_listing` is the whole module, so a version of it that could not fail would
/// make every row above decoration. One element is dropped from the listing side -- exactly what a
/// single refused page would do to `HashGetAll`, which is the failure this module exists to catch --
/// and the assertion must fire. It is the same function the real rows call, not a copy.
///
/// rust-internal: drives the module's own assertion, no engine and no external surface
#[test]
#[should_panic(expected = "two answers to one question")]
fn the_equality_check_fails_when_a_listing_comes_back_short() {
    assert_count_matches_listing("hash", EXPECTED as i64, EXPECTED - 1, EXPECTED);
}

/// THE NON-VACUITY FLOOR GOES RED WHEN BOTH SIDES ANSWER ZERO.
///
/// Two empty answers satisfy the equality perfectly, and a fixture that quietly wrote nothing
/// produces them. This is the second half of the guard and it needs its own proof: with a floor of
/// zero declared, the check must refuse the row rather than report it clean.
///
/// rust-internal: drives the module's own assertion, no engine and no external surface
#[test]
#[should_panic(expected = "could be satisfied by two empty answers")]
fn the_equality_check_refuses_a_row_whose_floor_is_zero() {
    assert_count_matches_listing("hash", 0, 0, 0);
}
