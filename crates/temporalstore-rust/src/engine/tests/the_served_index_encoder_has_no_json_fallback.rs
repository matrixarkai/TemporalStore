// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SERVED-INDEX ENCODER HAS NO FALLBACK TO JSON AND NO SETTING THAT CHOOSES ONE.
//!
//! `encode_index_bytes` used to fall through to a JSON write when the binary encode returned
//! `None`, and a setting (`TS_INDEX_CODEC=zstd-json`) could select the compressed-JSON payload. Both
//! are gone: the payload is msgpack. The one JSON write that remains in `engine.rs` is a separate
//! decision (the stale-version arm of `serialize_index`); it is not fallen into by this function.
//!
//! This cannot be driven by a failure: a `ShardState` that msgpack refuses and JSON accepts is not a
//! shape any type in it can produce, which is the argument for deleting the arm. So the guard reads
//! the code. It takes `encode_index_bytes` with comment lines removed and asserts that it returns the
//! binary encode unconditionally, names no JSON writer and reads no environment, and that no
//! non-comment line in `engine.rs` reads the retired `TS_INDEX_CODEC` variable. The CONTROL is
//! `serialize_index`, which does still call `wrap_index_json`: the scan is shown able to see a JSON
//! write, so a clean result for the encoder is not the scan being blind.
#![allow(clippy::all)]

#[test]
fn the_served_index_encoder_does_not_fall_through_to_json_or_read_a_setting() {
    let source = include_str!("../../engine.rs");
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
        !encoder.contains("serde_json")
            && !encoder.contains("wrap_index_json")
            && !encoder.contains("env::var"),
        "the encoder writes JSON or reads a setting: {encoder}"
    );
    // A retired variable name may not be read anywhere in the file, in code.
    let code = uncommented(source);
    assert!(
        !code.contains("TS_INDEX_CODEC"),
        "engine.rs still names the retired TS_INDEX_CODEC setting in code"
    );
    // THE CONTROL: the stale-version arm is in this same file, and the scan sees its JSON write.
    let stale = body(source, "fn serialize_index(", "const INDEX_CONTAINER_MAGIC");
    assert!(
        stale.contains("serde_json::to_vec") && stale.contains("wrap_index_json"),
        "the control no longer sees the stale-version JSON arm, so this scan could pass over a write it cannot see"
    );
}
