// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOCAL ROUTING-RANGE STAMP IS WRITTEN BINARY, AND A JSON STAMP IS NOT READ.
//!
//! The published copy of this stamp and the copy beside the base index go through one shared
//! codec, and that codec reads protobuf only: the first release has no installed base, so there is
//! no JSON stamp on disk to keep loading.
//!
//! The refused shape is driven on bytes PLANTED into the file, because the writer that used to
//! produce them is gone. The planted stamp shares no number with the writer's, so a reader that
//! decoded it would answer `Some(planted)`, which cannot be confused with the `None` of a refusal.
#![allow(clippy::all)]
use super::*;
use crate::engine::routing_range_stamp::{
    read_routing_range_stamp, routing_range_stamp_path, write_routing_range_stamp,
    RoutingRangeStamp,
};
use crate::shared_store::SHARED_OBJECT_CONTAINER_PROTOBUF;

#[test]
fn the_local_stamp_is_written_binary_and_a_planted_json_stamp_is_refused() {
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

    // ---- a JSON stamp, planted over it. Disjoint from the writer's values.
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
        None,
        "a planted JSON stamp was read as {legacy:?}: a JSON decode path survives"
    );

    // ---- CONTROL: the real writer's file reads back after the refusal, so the `None` above is
    // the codec's answer to JSON and not a reader that fails on every file.
    write_routing_range_stamp(&index_dir, 4, written).unwrap();
    assert_eq!(read_routing_range_stamp(&index_dir, 4), Some(written));
}

/// NO JSON ENCODE PATH IS LEFT FOR THE LOCAL STAMP.
///
/// The test above asserts the first byte of what the writer produced; this sees an encoder that
/// nobody calls YET. The scan reads the writer's own code with comment lines removed, anchored on its
/// declaration up to the next item, and its POSITIVE CONTROL is a synthetic body that does call
/// `serde_json`: a scan that could not see that would pass over any writer.
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
    assert!(decoder.len() > 100, "the scan found almost nothing in the decoder");
    assert!(
        !decoder.contains("serde_json"),
        "the shared stamp decoder reaches for serde_json: a JSON decode path survives"
    );

    // POSITIVE CONTROL: the same scan, over a body that does call serde_json, must see it.
    let planted = body(
        "fn a() { serde_json::to_vec(&x) } fn b() {}",
        "fn a(",
        "fn b(",
    );
    assert!(
        planted.contains("serde_json"),
        "the control no longer sees serde_json, so this scan could pass over a writer it cannot see"
    );
}
