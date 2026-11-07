// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SERVED-INDEX ENCODER HAS NO FALLBACK TO JSON.
//!
//! `encode_index_bytes` used to fall through to a JSON write when the binary encode returned
//! `None`. That arm is gone. The JSON writes that remain in `engine.rs` are ones somebody chooses
//! (the `TS_INDEX_CODEC=zstd-json` setting) or a separate decision (the stale-version arm of
//! `serialize_index`); none is fallen into.
//!
//! This cannot be driven by a failure: a `ShardState` that msgpack refuses and JSON accepts is not a
//! shape any type in it can produce, which is the argument for deleting the arm. So the guard reads
//! the code. It takes the msgpack branch of `encode_index_bytes` with comment lines removed and
//! asserts it returns the binary encode unconditionally, because the way this arm comes back is not
//! a `serde_json` call inside the branch but a branch that no longer returns and falls out into the
//! JSON arm below it.
#![allow(clippy::all)]

#[test]
fn the_served_index_encoder_does_not_fall_through_to_json() {
    let source = include_str!("../../engine.rs");
    fn body(source: &str, from: &str, to: &str) -> String {
        let start = source
            .find(from)
            .unwrap_or_else(|| panic!("no declaration {from}"));
        let rest = &source[start..];
        let end = rest[from.len()..]
            .find(to)
            .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
            + from.len();
        rest[..end]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    let whole = body(
        source,
        "pub(super) fn encode_index_bytes(",
        "fn wrap_index_json(",
    );
    let branch = body(
        &whole,
        "if index_container_codec() == INDEX_CODEC_ZSTD_MSGPACK {",
        "\n    }\n",
    );
    assert!(branch.len() > 40, "the scan found almost nothing");
    assert!(
        branch.contains("return encode_index_msgpack(shard).expect("),
        "the msgpack branch can fall out into the JSON arm: {branch}"
    );
    assert!(
        !branch.contains("serde_json") && !branch.contains("wrap_index_json"),
        "the msgpack branch writes JSON: {branch}"
    );
    // THE CONTROL: the chosen setting's arm is in this same function, outside the branch, so the
    // scan is shown able to see a JSON write there.
    assert!(
        whole.contains("serde_json::to_vec") && whole.contains("wrap_index_json"),
        "the control no longer sees the chosen JSON arm, so this scan could pass over a write it cannot see"
    );
}
