// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SERVED-INDEX WRITER HAS NO JSON ARM, NO SETTING THAT CHOOSES ONE, AND ITS STAMP COMES FROM
//! THE HEADER.
//!
//! `encode_index_bytes` used to fall through to a JSON write when the binary encode returned
//! `None`, a setting (`TS_INDEX_CODEC=zstd-json`) could select the compressed-JSON payload, and
//! `serialize_index` wrote compressed JSON, with the version field patched to the current constant,
//! for any shard whose own `index_format_version` was not yet current -- which is every shard that
//! was just loaded, until something stamped it. All three are gone: the payload is msgpack, for
//! every shard.
//!
//! The JSON READERS are still here (plain JSON and the zstd-json payload), kept deliberately for a
//! follow-up that retires them together with the door machinery that depends on them. This module
//! pins the WRITE side only.
//!
//! None of this can be driven by a failure: a `ShardState` that msgpack refuses and JSON accepts is
//! not a shape any type in it can produce, which is the argument for deleting the arm. So the first
//! test reads the code. It takes `encode_index_bytes` and `serialize_index` with comment lines
//! removed and asserts that they write msgpack unconditionally, name no JSON writer and read no
//! environment, that nothing in `engine.rs` reads the retired `TS_INDEX_CODEC` variable, and that
//! the helper which used to patch the stamp into a JSON value is not declared. Its CONTROL is the
//! old `serialize_index` text, reproduced verbatim: the scan is shown able to see the JSON write it
//! is looking for, so a clean result for the real functions is not the scan being blind.
//!
//! The other two tests are the half a source scan cannot reach. The stamp a reader sees has to be
//! the container HEADER's, because the field inside the payload is whatever the writing shard
//! carried. They write a shard whose own field is wrong in two different ways and ask what a
//! reader gets back.
#![allow(clippy::all)]

use crate::block_store::ElementEntry;
use crate::engine::state::ShardState;
use crate::engine::{
    decode_index_bytes, encode_index_bytes, serialize_index, SHARD_INDEX_FORMAT_VERSION,
};

fn uncommented(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn body(source: &str, from: &str, to: &str) -> String {
    let start = source
        .find(from)
        .unwrap_or_else(|| panic!("no declaration {from}"));
    let rest = &source[start..];
    let end = rest[from.len()..]
        .find(to)
        .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
        + from.len();
    uncommented(&rest[..end])
}

/// Does this text write JSON, write a different payload than msgpack, or read a setting?
fn writes_json_or_reads_a_setting(code: &str) -> bool {
    code.contains("serde_json") || code.contains("wrap_index_json") || code.contains("env::var")
}

#[test]
fn the_served_index_encoder_does_not_fall_through_to_json_or_read_a_setting() {
    let source = include_str!("../../engine.rs");
    let encoder = body(
        source,
        "pub(super) fn encode_index_bytes(",
        "fn wrap_index_json(",
    );
    assert!(encoder.len() > 80, "the scan found almost nothing");
    assert!(
        encoder.contains("encode_index_msgpack(shard).expect("),
        "the encoder no longer returns the msgpack encode unconditionally: {encoder}"
    );
    assert!(
        !writes_json_or_reads_a_setting(&encoder),
        "the encoder writes JSON or reads a setting: {encoder}"
    );

    // The entry point every production caller goes through has no arm of its own.
    let entry = body(source, "fn serialize_index(", "const INDEX_CONTAINER_MAGIC");
    assert!(entry.len() > 80, "the scan found almost nothing");
    assert!(
        entry.contains("encode_index_bytes(shard)"),
        "serialize_index no longer writes through the one encoder: {entry}"
    );
    assert!(
        !entry.contains("index_format_version") && !writes_json_or_reads_a_setting(&entry),
        "serialize_index branches on the shard's own stamp, or writes JSON, or reads a setting: {entry}"
    );

    // A retired name may not be read or declared anywhere in the file, in code.
    let code = uncommented(source);
    assert!(
        !code.contains("TS_INDEX_CODEC"),
        "engine.rs still names the retired TS_INDEX_CODEC setting in code"
    );
    assert!(
        !code.contains("fn stamp_index_format_version"),
        "the helper that patched the stamp into a JSON value is declared again"
    );

    // THE CONTROL: the stale-version arm as it stood, verbatim. The scan must see its JSON write.
    let old_entry = uncommented(
        "fn serialize_index(shard: &ShardState) -> Vec<u8> {\n    let bytes = if shard.index_format_version == SHARD_INDEX_FORMAT_VERSION {\n        encode_index_bytes(shard)\n    } else {\n        wrap_index_json(\n            serde_json::to_vec(&stamp_index_format_version(shard))\n                .expect(\"shard index should serialize\"),\n        )\n    };\n",
    );
    assert!(
        writes_json_or_reads_a_setting(&old_entry) && old_entry.contains("index_format_version"),
        "the control no longer sees the stale-version JSON arm, so this scan could pass over a write it cannot see"
    );
}

fn a_shard_with_something_in_it() -> ShardState {
    let mut shard = ShardState::default();
    shard.set_string_address(
        "stamp-probe".into(),
        &ElementEntry::from_parts(7, 11, 13, None, None),
    );
    shard
}

/// A shard nobody has stamped is the common case -- every shard is in it from load until its first
/// flush -- and it used to be the one written as JSON.
#[test]
fn a_shard_that_was_never_stamped_is_written_as_msgpack_and_reads_back_current() {
    let shard = a_shard_with_something_in_it();
    // DENOMINATOR: the input really is unstamped, or the assertions below hold for any shard.
    assert_eq!(shard.index_format_version, 0, "the fixture shard must be unstamped");
    assert_ne!(shard.index_format_version, SHARD_INDEX_FORMAT_VERSION);

    let bytes = serialize_index(&shard);
    assert!(bytes.starts_with(b"TSIDX\x01"), "served index must be the container");
    assert_eq!(bytes[6], 2, "an unstamped shard must be written as msgpack (codec 2), not JSON (codec 1)");

    let decoded = decode_index_bytes(&bytes).expect("the image decodes");
    assert!(decoded.strings().contains_key("stamp-probe"), "the content must survive");
    assert_eq!(
        decoded.index_format_version, SHARD_INDEX_FORMAT_VERSION,
        "a reader must see the current stamp for a shard that was written unstamped -- this is the \
         value every door compares, and a decode that returned the payload's own 0 would have all \
         of them refuse an image this binary just wrote"
    );
}

/// The payload's own copy of the field is not what a reader gets. A shard whose field disagrees
/// with the header, in a way only the header can resolve, is written and read back.
#[test]
fn the_stamp_a_reader_sees_is_the_header_and_not_the_field_inside_the_payload() {
    let mut shard = a_shard_with_something_in_it();
    shard.index_format_version = 7;
    let bytes = encode_index_bytes(&shard);
    let decoded = decode_index_bytes(&bytes).expect("the image decodes");
    assert_eq!(
        decoded.index_format_version, SHARD_INDEX_FORMAT_VERSION,
        "the decoded stamp followed the payload's field (7) instead of the container header"
    );
    assert_ne!(decoded.index_format_version, 7);

    // THE GATE IS UNCHANGED, and this says so: a header that is not this binary's version is still
    // refused at the decode, before a byte of the payload is read.
    let mut older = bytes.clone();
    older[7..11].copy_from_slice(&(SHARD_INDEX_FORMAT_VERSION - 1).to_be_bytes());
    let refused = decode_index_bytes(&older).expect_err("an older header is refused");
    assert!(refused.contains("struct version"), "unhelpful refusal: {refused}");
}
