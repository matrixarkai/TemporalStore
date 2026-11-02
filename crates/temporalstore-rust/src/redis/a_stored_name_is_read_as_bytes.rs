// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE READ HALF OF A BINARY-SAFE NAME, AND THE TWO CLAIMS IT HAS TO CARRY.
//!
//! A key and a hash field are binary-safe by specification. Our dispatch decodes both with
//! `String::from_utf8_lossy`, which is a wrong answer rather than a limitation: 91% of four-byte
//! names are not valid text, two distinct ones collide once in 1,042 against once in 4.29 billion
//! for a correct store, and `HKEYS` answers with the name AS STORED, so the original bytes are
//! gone rather than hidden.
//!
//! This file pins the READ step of the three-step sequence. The encoding module landed first and
//! inert; this wires `decode_name` into the seven sites that hand a stored name back; the WRITE
//! step, which starts emitting tagged names, is still separate and still unlanded. The order is
//! not tidiness -- a build must be able to READ the shape before any build emits it, or a rolling
//! restart puts a node in front of a name it cannot decode.
//!
//! So there are exactly two claims, and they pull in opposite directions:
//!
//!   * **A NO-OP ON EVERYTHING THAT EXISTS.** Every name stored today is untagged, and an untagged
//!     name must come back byte for byte. This is the whole safety argument for deploying this
//!     step on its own, so it is tested first and it is tested through the dispatch rather than
//!     against `decode_name` directly -- a unit test of the decoder cannot tell whether the
//!     dispatch actually calls it, and a mechanism that lands complete, tested and called from
//!     nowhere is a failure this tree has already had.
//!   * **ABLE TO READ WHAT THE NEXT STEP WILL WRITE.** A tagged name has to survive the read path,
//!     in BOTH its tagged branches, or the write step cannot be deployed after it.
//!
//! The fourth test covers the arm that neither claim reaches: a stored name our own encoder could
//! never have produced.

use super::*;
use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest};

/// Drive the RESP dispatch against a real engine, with arguments as RAW BYTES.
///
/// Taking `Vec<Vec<u8>>` rather than `&str` is the point: a test that could only send text could
/// not reach the defect at all.
fn resp(
    engine: &TemporalEngine,
    state: &mut RedisCommandState,
    args: Vec<Vec<u8>>,
) -> RespValue {
    execute_redis_command_with_state(args, 1, state, |command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message)
        }
    })
}

fn engine() -> TemporalEngine {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine
}

/// Store a field under a key by going STRAIGHT TO THE ENGINE, bypassing the dispatch's writes.
///
/// The write step has not landed, so `HSET` still spells a field with `string_arg` and would
/// mangle anything non-text on the way in. Writing the stored form directly is the only way to ask
/// what the READ path does with it, and it is also exactly the state a node will be in once the
/// write step is deployed ahead of it.
fn store_field(engine: &TemporalEngine, key: &str, stored_field: &str, value: &[u8]) {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: key.to_string(),
            field: stored_field.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(
        outcome.status.ok,
        "the fixture's own write failed, so nothing below is about the read path: {:?}",
        outcome.response
    );
}

/// Every `Bulk` payload of an array reply, so an assertion names bytes rather than a shape.
fn bulk_bytes(reply: &RespValue) -> Vec<Vec<u8>> {
    match reply {
        RespValue::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                RespValue::Bulk(Some(bytes)) => Some(bytes.clone()),
                _ => None,
            })
            .collect(),
        other => panic!("expected an array reply, got {other:?}"),
    }
}

#[test]
fn an_untagged_name_comes_back_byte_for_byte() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // Written through the DISPATCH, because this claim is about the names that already exist in
    // every store: whatever `HSET` spells today, `HKEYS` must hand back unchanged.
    let reply = resp(
        &engine,
        &mut state,
        vec![b"HSET".to_vec(), b"h".to_vec(), b"plain".to_vec(), b"v".to_vec()],
    );
    assert!(
        !matches!(reply, RespValue::Error(_)),
        "HSET failed: {reply:?}"
    );
    let fields = bulk_bytes(&resp(
        &engine,
        &mut state,
        vec![b"HKEYS".to_vec(), b"h".to_vec()],
    ));
    assert_eq!(
        fields,
        vec![b"plain".to_vec()],
        "THE READ WIRING IS NOT A NO-OP ON EXISTING DATA. An untagged name must decode to its own \
         bytes -- that is the entire safety argument for deploying this step before the write \
         step, and if it is red this change cannot ship at all"
    );
}

#[test]
fn a_tagged_binary_name_survives_the_read_path() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // The shape the WRITE step will produce for a field that is not valid text. 0xFF 0xFE is not
    // valid UTF-8, which is why `from_utf8_lossy` would have destroyed it.
    let raw = vec![0xffu8, 0xfe];
    let stored = crate::binary_safe_name::encode_name(&raw);
    store_field(&engine, "h", &stored, b"v");
    let fields = bulk_bytes(&resp(
        &engine,
        &mut state,
        vec![b"HKEYS".to_vec(), b"h".to_vec()],
    ));
    assert_eq!(
        fields,
        vec![raw],
        "A NODE RUNNING THIS CANNOT READ WHAT THE WRITE STEP WILL WRITE. The tagged form must come \
         back as the bytes it stands for, or the write step cannot be deployed after this one and \
         the sequence is stuck. Stored form was {stored:?}"
    );
}

#[test]
fn a_tagged_text_name_survives_the_read_path_too() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // THE SECOND TAGGED BRANCH, which exists only because a name may itself BEGIN with the tag
    // byte. Without this arm the sweep would exercise only the cheap tagged case, and a decoder
    // that read just the leading byte would pass.
    let raw = vec![0x1bu8, b'A', b'A', b'A', b'A'];
    let stored = crate::binary_safe_name::encode_name(&raw);
    assert_ne!(
        stored.as_bytes(),
        raw.as_slice(),
        "this arm is only meaningful if the encoder actually tagged the name"
    );
    store_field(&engine, "h", &stored, b"v");
    let fields = bulk_bytes(&resp(
        &engine,
        &mut state,
        vec![b"HKEYS".to_vec(), b"h".to_vec()],
    ));
    assert_eq!(
        fields,
        vec![raw],
        "the tagged-TEXT branch does not round-trip, so a name whose own first byte is the tag is \
         still corrupted. Stored form was {stored:?}"
    );
}

#[test]
fn a_name_this_encoder_could_not_have_written_does_not_fail_the_listing() {
    let engine = engine();
    let mut state = RedisCommandState::default();
    // Tag byte followed by a tail that is not base64. `encode_name` cannot produce this, so it
    // means corrupt or foreign data -- the one arm neither of the two claims above reaches.
    let mut stored = String::new();
    stored.push(char::from_u32(0x1b).expect("U+001B is a character"));
    stored.push_str("!!!!");
    store_field(&engine, "h", &stored, b"v");
    let fields = bulk_bytes(&resp(
        &engine,
        &mut state,
        vec![b"HKEYS".to_vec(), b"h".to_vec()],
    ));
    assert_eq!(
        fields,
        vec![stored.as_bytes().to_vec()],
        "AN UNDECODABLE NAME MUST NOT FAIL THE LISTING. Answering with the name as stored is what \
         every reader did before this encoding existed, so it is the old behaviour rather than a \
         new guess -- and propagating the error instead would let one bad name take out a listing \
         of a thousand good ones"
    );
}
