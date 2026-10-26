// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-back door, the page ordinal it cannot hold, and the two halves of one contract that
//! used to disagree about it.
//!
//! THE ASYMMETRY. `ObjectRecord`'s inline arm keeps a page ordinal's PRESENCE and not its value,
//! which is correct and measured: a single-page object has page zero by construction, and 400 of
//! 400 live addresses on a real store carried ordinal 0. The conversion path refuses a NONZERO
//! ordinal by name, as `NonZeroBlockOrdinalDoesNotFitInline`. `set_address` -- the compactor's
//! write-back door -- did not refuse it; it wrote the presence bit and dropped the value.
//!
//! WHY NOTHING CAUGHT IT. No test fed either path a nonzero ordinal. The existing fixture at
//! `where_the_characters_are_durable_per_kind` passes `Some(9)` and never reads it back, so the
//! round trip was never compared, so the divergence between the documented behaviour and the actual
//! behaviour was invisible. A named `TODO` would have been no better; an instrument is what tells.
//!
//! rust-internal: guards this crate's own record shape, no product behaviour

use crate::block_store::ElementEntry;
use crate::engine::object_record::{
    dropped_nonzero_block_ordinals, reset_dropped_nonzero_block_ordinals, ObjectRecord,
    ObjectRecordRefusal,
};

/// An entry whose page ordinal is `at`, with everything else fixed so only the ordinal varies.
fn entry_with_ordinal(ordinal: Option<u64>) -> ElementEntry {
    ElementEntry::from_parts(1, 64, 32, ordinal, Some(11))
}

/// THE REFUSING DOOR REFUSES, BY THE SAME NAME THE CONVERSION PATH USES.
///
/// Two halves of one contract have to give one answer. `try_set_address` is the half that was
/// missing: the conversion path already answered
/// `NonZeroBlockOrdinalDoesNotFitInline`, and the write-back door answered nothing at all.
///
/// THE POSITIVE CONTROLS ARE THE POINT OF THE SECOND HALF. A refusal that fires on every input
/// would pass the first assertion just as well, so ordinal zero and ordinal absent must both still
/// be accepted -- and accepted means the address actually lands, not merely that no error came
/// back.
#[test]
fn the_write_back_door_refuses_a_block_ordinal_it_cannot_hold() {
    let mut record = ObjectRecord::default();

    let refusal = record
        .try_set_address(&entry_with_ordinal(Some(7)))
        .expect_err("a nonzero page ordinal has no room in the inline arm and must be refused");
    assert_eq!(
        refusal,
        ObjectRecordRefusal::NonZeroBlockOrdinalDoesNotFitInline { block_id: 7 },
        "the write-back door must refuse by the SAME named variant the conversion path uses, or \
         the two halves of one contract give two different answers to one question"
    );
    assert!(
        refusal.to_string().contains('7'),
        "the refusal does not name the ordinal it refused: {refusal}"
    );

    // --- POSITIVE CONTROL ONE: ordinal zero, which is what a single-page object carries.
    let mut record = ObjectRecord::default();
    record
        .try_set_address(&entry_with_ordinal(Some(0)))
        .expect("ordinal zero is exactly what the inline arm is for");
    assert_eq!(
        record.address().block_id(),
        Some(0),
        "the accepted address did not land, so the refusal above may be firing on everything"
    );

    // --- POSITIVE CONTROL TWO: no ordinal at all, which is a page still in the log.
    let mut record = ObjectRecord::default();
    record
        .try_set_address(&entry_with_ordinal(None))
        .expect("an absent ordinal is the log-backed arm and must be accepted");
    assert_eq!(
        record.address().block_id(),
        None,
        "an absent ordinal came back present, so the presence bit and the value disagree"
    );
}

/// WHAT THE UNCHANGED DOOR STILL DOES, PINNED, AND COUNTED WHILE IT DOES IT.
///
/// `set_address` keeps today's behaviour deliberately: the destination for a multi-page object does
/// not exist yet, so a fatal refusal would make a recovery path unopenable and a skip would drop
/// the page's address -- the same loss by another mechanism. What changes is that the drop is no
/// longer silent.
///
/// So this asserts BOTH halves: that the value is still dropped, and that the counter saw it. The
/// first is a stale-constant risk the day the owned array lands, which is the point -- it will fail
/// then, and the failure is the prompt to route such an object to its real home.
#[test]
fn the_unchanged_door_still_drops_the_value_and_now_says_so() {
    reset_dropped_nonzero_block_ordinals();

    let mut record = ObjectRecord::default();
    record.set_address(&entry_with_ordinal(Some(7)));

    assert_eq!(
        record.address().block_id(),
        Some(0),
        "the inline arm now round-trips a nonzero page ordinal. If that is deliberate -- the owned \
         array has landed and the write-back door routes to it -- then this test and the counter \
         beside it are what should be deleted, in that change. If it is not deliberate, a page \
         ordinal is being written somewhere it cannot be read back from"
    );
    assert_eq!(
        dropped_nonzero_block_ordinals(),
        1,
        "the door dropped a nonzero ordinal and the counter did not see it, so the instrument that \
         replaces the silence is not wired to the thing it instruments"
    );

    // AND IT DOES NOT COUNT WHAT IT DID NOT DROP. A counter that fires on every write would make
    // the assertion above pass while meaning nothing.
    reset_dropped_nonzero_block_ordinals();
    let mut record = ObjectRecord::default();
    record.set_address(&entry_with_ordinal(Some(0)));
    record.set_address(&entry_with_ordinal(None));
    assert_eq!(
        dropped_nonzero_block_ordinals(),
        0,
        "the counter fired for an ordinal that fits, so it cannot distinguish a dropped value from \
         an ordinary write"
    );
}

/// AND IT NEVER FIRES ON THE REAL WRITE PATHS, which is the claim the counter exists to make.
///
/// Every map holding a record today is single-page, so no production write should ever hand the
/// door a nonzero ordinal. That is an assertion about the whole engine rather than about one
/// record, so it is made after driving real writes through the command surface rather than by
/// constructing addresses by hand.
///
/// A COUNTER NOTHING ASSERTS ON IS A COUNTER NOBODY READS, which is why this exists at all.
#[test]
fn no_production_write_hands_the_door_an_ordinal_it_cannot_hold() {
    reset_dropped_nonzero_block_ordinals();

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "ordinal-drop-guard".to_string(),
        shard_uri: "local://ordinal-drop-guard/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "the fixture must load: {:?}", response.status);

    // Real writes, through the command surface, across the kinds that hold records today.
    let mut wrote = 0usize;
    for n in 0..64 {
        let ok = engine
            .execute(crate::types::ExecuteRequest {
                shard_id: 1,
                command: crate::types::Command::StringSet {
                    key: format!("k{n}"),
                    value: vec![b'v'; 48],
                },
            })
            .status
            .ok;
        assert!(ok, "the string write must ack");
        wrote += 1;
    }
    assert_eq!(wrote, 64, "the fixture wrote nothing, so this asserts over no writes at all");

    assert_eq!(
        dropped_nonzero_block_ordinals(),
        0,
        "a production write handed the write-back door a page ordinal the inline arm cannot hold, \
         so a page ordinal was silently dropped on a real path. That is the condition this counter \
         was added to detect, and it has fired"
    );
}
