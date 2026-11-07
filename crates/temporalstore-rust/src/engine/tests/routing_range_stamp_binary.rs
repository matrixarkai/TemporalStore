// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOCAL ROUTING-RANGE STAMP IS WRITTEN BINARY, AND A JSON STAMP IS STILL READ.
//!
//! The published copy of this stamp was flipped earlier; this is the copy beside the base index.
//! The READER already went through the shared decoder (so it accepted both shapes before this
//! change), which is why this is a writer-only change: the hold rule was already satisfied.
//!
//! The JSON arm is driven on bytes PLANTED into the file, because the writer that used to produce
//! them is gone. Every value is chosen so only one arm can have produced it: the writer's stamp and
//! the planted one share no number, and a reader that failed on a file answers `None`, which a
//! "read back the planted value" assertion cannot be confused with.
#![allow(clippy::all)]
use super::*;
use crate::engine::routing_range_stamp::{
    read_routing_range_stamp, routing_range_stamp_path, write_routing_range_stamp,
    RoutingRangeStamp,
};
use crate::shared_store::SHARED_OBJECT_CONTAINER_PROTOBUF;

#[test]
fn the_local_stamp_is_written_binary_and_a_planted_json_stamp_is_still_read() {
    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");

    // ---- the real writer.
    let written = RoutingRangeStamp {
        start_routing_bucket: 3,
        end_routing_bucket: 7777,
    };
    write_routing_range_stamp(&index_dir, 4, written).unwrap();
    let path = routing_range_stamp_path(&index_dir, 4);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        bytes[0], SHARED_OBJECT_CONTAINER_PROTOBUF,
        "the local stamp writer did not write the binary shape"
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
        "the local stamp parses as JSON, so the container byte proves nothing"
    );
    assert_eq!(read_routing_range_stamp(&index_dir, 4), Some(written));
    assert!(
        path.to_string_lossy().ends_with(".routing-range.json"),
        "the stamp is opened by exact path and keeps its name: {path:?}"
    );

    // ---- the installed base: a JSON stamp, planted over it. Disjoint from the writer's values.
    let legacy = RoutingRangeStamp {
        start_routing_bucket: 11,
        end_routing_bucket: 9999,
    };
    let planted = serde_json::to_vec(&legacy).unwrap();
    assert_eq!(planted[0], b'{', "the planted stamp is not the JSON shape");
    std::fs::write(&path, &planted).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap()[0],
        b'{',
        "the plant did not take, so nothing below reads a JSON stamp"
    );
    assert_eq!(
        read_routing_range_stamp(&index_dir, 4),
        Some(legacy),
        "a JSON stamp was not read: every store written before this change would now read as \
         UNSTAMPED, which a load refuses by name"
    );

    // ---- CONTROL: a file neither shape can parse reads as None, so a Some above means a decode.
    std::fs::write(&path, [0x02u8, b'x', b'y']).unwrap();
    assert_eq!(read_routing_range_stamp(&index_dir, 4), None);
}

/// NO JSON ENCODE PATH IS LEFT FOR THE LOCAL STAMP.
///
/// The test above asserts the first byte of what the writer produced; this sees an encoder that
/// nobody calls YET. The scan reads the writer's own code with comment lines removed, anchored on its
/// declaration up to the next item, and its POSITIVE CONTROL is the shared decoder, which does call
/// `serde_json` (the permanent JSON arm): a scan that could not see that would pass over any writer.
#[test]
fn no_json_encode_path_is_left_for_the_local_routing_range_stamp() {
    fn body(source: &'static str, from: &str, to: &str) -> String {
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
    let stamp_source = include_str!("../routing_range_stamp.rs");
    let shared_source = include_str!("../../shared_store.rs");

    let writer = body(
        stamp_source,
        "pub(crate) fn write_routing_range_stamp(",
        "pub(super) fn store_has_on_disk_state(",
    );
    assert!(writer.len() > 100, "the scan found almost nothing");
    assert!(
        writer.contains("encode_routing_range_stamp("),
        "the local writer does not call the shared binary encoder"
    );
    assert!(
        !writer.contains("serde_json"),
        "the local stamp writer still reaches for serde_json: a JSON encode path survives. Delete \
         it; the reader keeps its JSON arm for the stamps already on disk."
    );

    let decoder = body(
        shared_source,
        "pub(crate) fn decode_routing_range_stamp(",
        "pub(crate) fn encode_routing_range_stamp(",
    );
    assert!(
        decoder.contains("serde_json::from_slice"),
        "the control no longer sees the permanent JSON arm, so this scan could pass over a writer \
         it cannot see"
    );
}
