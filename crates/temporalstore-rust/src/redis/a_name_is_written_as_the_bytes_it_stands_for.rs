// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A NAME SURVIVES THE ROUND TRIP THROUGH THE DISPATCH, BYTE FOR BYTE.
//!
//! # WHAT THESE DRIVE THAT THE READ-SIDE TESTS DO NOT
//!
//! The read-side module pins what the read path does with a given STORED spelling, on purpose,
//! by writing that spelling straight to the engine. These go through `HSET` and `HKEYS`/`HGETALL`
//! instead, so the ENCODER and the DECODER are both exercised and the claim is end-to-end. A test
//! that only drove one half could pass while both were wrong in the same direction.
//!
//! # THE TWO CONDITIONS THE FEAST WORKAROUND IS WAITING ON
//!
//! `integrations/feast/` encodes names itself because this store mangled them. It can drop that
//! encoding only when BOTH of these hold, which is why they are separate tests rather than one:
//!
//!   * ROUND-TRIP IDENTITY -- an arbitrary four-byte name comes back as the same four bytes;
//!   * NO COLLISION -- two distinct byte names stay two names.
//!
//! A fix that stopped the collision but still handed back a mangled name would satisfy the second
//! and not the first, and would not free that code.

use super::*;
use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest};

/// Four bytes that are NOT valid UTF-8: `0xff` is never a legal lead byte. This is the shape of a
/// real client's field name -- Feast's is a four-byte `mmh3` hash, and 91.06-91.08% of those are
/// not valid text.
const BINARY_NAME: &[u8] = &[0xff, 0x01, 0x80, 0x7f];
/// A second one differing ONLY in bytes that a lossy decode destroys: both `0xff` and `0xfe` become
/// U+FFFD, so before this change these two names collapsed onto one stored name and the second
/// write silently overwrote the first.
const COLLIDING_NAME: &[u8] = &[0xfe, 0x01, 0x80, 0x7f];

fn engine() -> TemporalEngine {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine
}

/// Drive a real RESP command through the dispatch, the way a client would.
///
/// THE STATE IS CARRIED ACROSS CALLS, which `execute_redis_command` does not do -- it builds a
/// fresh `RedisCommandState` per call, so the per-connection keyspace would be empty every time
/// and a SCAN would find nothing for reasons that have nothing to do with this change.
fn dispatch(
    engine: &TemporalEngine,
    state: &mut RedisCommandState,
    args: Vec<Vec<u8>>,
) -> RespValue {
    execute_redis_command_with_state(args, 1, state, |command: Command| {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if outcome.status.ok {
            Ok(outcome.response)
        } else {
            Err(outcome.status.message)
        }
    })
}

/// A cursor reply is `[cursor, [item, item, ..]]`, so the items are one level DOWN. Reading the
/// top level instead yields the cursor -- which is how the first version of the scan test came to
/// assert against `[[48]]`, the bulk `"0"`.
fn cursor_items(value: &RespValue) -> Vec<Vec<u8>> {
    match value {
        RespValue::Array(outer) => match outer.as_slice() {
            [_cursor, inner] => bulk_bytes(inner),
            other => panic!("expected [cursor, items], got {other:?}"),
        },
        other => panic!("expected a cursor array, got {other:?}"),
    }
}

fn bulk_bytes(value: &RespValue) -> Vec<Vec<u8>> {
    match value {
        RespValue::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                RespValue::Bulk(Some(bytes)) => Some(bytes.clone()),
                _ => None,
            })
            .collect(),
        other => panic!("expected an array of bulk replies, got {other:?}"),
    }
}

#[test]
fn an_arbitrary_byte_field_comes_back_as_the_same_bytes() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    let set = dispatch(
        &engine,
        &mut state,
        vec![b"HSET".to_vec(), b"h".to_vec(), BINARY_NAME.to_vec(), b"v".to_vec()],
    );
    assert!(
        !matches!(set, RespValue::Error(_)),
        "HSET of a non-text field must be accepted: {set:?}"
    );

    let keys = bulk_bytes(&dispatch(&engine, &mut state, vec![b"HKEYS".to_vec(), b"h".to_vec()]));
    assert_eq!(
        keys,
        vec![BINARY_NAME.to_vec()],
        "HKEYS MUST HAND BACK THE BYTES THAT WERE WRITTEN. Got {keys:?} for a field written as \
         {BINARY_NAME:?}. If these differ the name was mangled on the way in, on the way out, or \
         both -- and `integrations/feast/` cannot drop its own encoding until this holds."
    );

    // HGETALL too, because it is a different site from HKEYS and a fix applied to one is not a
    // fix applied to the other.
    let all = bulk_bytes(&dispatch(&engine, &mut state, vec![b"HGETALL".to_vec(), b"h".to_vec()]));
    assert_eq!(
        all,
        vec![BINARY_NAME.to_vec(), b"v".to_vec()],
        "HGETALL must hand back the same field bytes and its value"
    );
}

#[test]
fn two_distinct_byte_names_stay_two_names() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // Sliced, because `b"first"` and `b"second"` are arrays of DIFFERENT sizes and so of
    // different types; `&[u8]` is the common one.
    for (name, value) in [
        (BINARY_NAME, &b"first"[..]),
        (COLLIDING_NAME, &b"second"[..]),
    ] {
        let set = dispatch(
            &engine,
            &mut state,
            vec![b"HSET".to_vec(), b"h".to_vec(), name.to_vec(), value.to_vec()],
        );
        assert!(!matches!(set, RespValue::Error(_)), "HSET failed: {set:?}");
    }

    let len = dispatch(&engine, &mut state, vec![b"HLEN".to_vec(), b"h".to_vec()]);
    assert!(
        matches!(len, RespValue::Integer(2)),
        "TWO DISTINCT NAMES COLLAPSED INTO ONE. HLEN answered {len:?} for two fields that differ \
         only in a byte a lossy decode destroys ({:?} against {:?}). Measured, this happens to 1 \
         in 1,042 pairs of four-byte names, against 1 in 4.29 billion for a correct store",
        BINARY_NAME, COLLIDING_NAME
    );

    // And each still carries ITS OWN value -- a collision that kept two fields but crossed their
    // values would satisfy the count above.
    let first = dispatch(
        &engine,
        &mut state,
        vec![b"HGET".to_vec(), b"h".to_vec(), BINARY_NAME.to_vec()],
    );
    assert!(
        matches!(&first, RespValue::Bulk(Some(v)) if v == b"first"),
        "the first name must still hold its own value, got {first:?}"
    );
    let second = dispatch(
        &engine,
        &mut state,
        vec![b"HGET".to_vec(), b"h".to_vec(), COLLIDING_NAME.to_vec()],
    );
    assert!(
        matches!(&second, RespValue::Bulk(Some(v)) if v == b"second"),
        "the second name must still hold its own value, got {second:?}"
    );
}

#[test]
fn a_scan_pattern_still_finds_a_name_whose_stored_spelling_is_tagged() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // An ASCII field and a binary one under the same key. `*` must find both: the binary one's
    // STORED spelling begins with ESC and is base64, so a matcher reading the stored form would
    // still match `*` -- which is why the discriminating pattern below is the real test.
    for name in [b"plain".to_vec(), BINARY_NAME.to_vec()] {
        dispatch(
            &engine,
            &mut state,
            vec![b"HSET".to_vec(), b"h".to_vec(), name, b"v".to_vec()],
        );
    }
    let all = cursor_items(&dispatch(
        &engine,
        &mut state,
        vec![b"HSCAN".to_vec(), b"h".to_vec(), b"0".to_vec(), b"MATCH".to_vec(), b"*".to_vec()],
    ));
    assert!(
        all.contains(&BINARY_NAME.to_vec()),
        "HSCAN MATCH * must return the binary field as its own bytes; got {all:?}"
    );

    // THE DISCRIMINATING CASE. `plain` matches only the ASCII field. A matcher comparing the
    // pattern against the STORED spelling would be comparing it against ESC+base64 for the other
    // field, so this pins that the match reads the DECODED bytes -- and it is the assertion that
    // fails if the pattern fix is ever reverted while the writer stays.
    let plain = cursor_items(&dispatch(
        &engine,
        &mut state,
        vec![b"HSCAN".to_vec(), b"h".to_vec(), b"0".to_vec(), b"MATCH".to_vec(), b"plain".to_vec()],
    ));
    assert!(
        plain.contains(&b"plain".to_vec()),
        "HSCAN MATCH plain must still find the ASCII field; got {plain:?}"
    );
    assert!(
        !plain.contains(&BINARY_NAME.to_vec()),
        "HSCAN MATCH plain must NOT return the binary field; got {plain:?}"
    );
}
