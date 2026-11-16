// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The bytes of the window reducer protocol did not move when its messages were renamed.
//!
//! Protobuf puts field numbers and wire types on the wire and never a name, so a rename is byte-neutral as long
//! as no number or type is touched. These tests prove that it was not touched, against two files written from the
//! proto as it stood before the rename:
//!
//! * `window_reduce_wire_golden.txt`: one fully populated value of every message, encoded in field number order,
//!   every field set to a distinct non-default value and every repeated field holding two entries. Names in it
//!   are the old ones.
//! * `window_reduce_wire_table.txt`: every (message, field number, wire type, label), every enum's value numbers
//!   and every reserved number, again under the old names.
//!
//! [`RENAMED`] is the one place the old and the new message names meet.

use super::wire;
use crate::wire_check::{field_numbers, parse_proto, read_golden, read_table};
use prost::Message;
use std::collections::BTreeSet;

const GOLDEN: &str = include_str!("../../tests/golden/window_reduce_wire_golden.txt");
const TABLE: &str = include_str!("../../tests/golden/window_reduce_wire_table.txt");
const PROTO: &str = include_str!("../../../../proto/temporalstore/windowreduce/v1/windowreduce.proto");

/// Messages and enums whose name changed: (old, new). A name not listed here is unchanged.
const RENAMED: &[(&str, &str)] = &[
    ("CellOperation", "WindowReduceOperation"),
    ("CellStatusCode", "WindowReduceStatusCode"),
    ("CellAddRequest", "WindowReduceAddRequest"),
    ("CellAddResponse", "WindowReduceAddResponse"),
    ("CellRestoreRequest", "WindowReduceRestoreRequest"),
    ("CellRestoreResponse", "WindowReduceRestoreResponse"),
    ("CellSelector", "WindowReduceSelector"),
    ("CellQuery", "WindowReduceQuery"),
    ("CellBatchQuery", "WindowReduceBatchQuery"),
    ("SnapshotInfo", "SampleInfo"),
    ("CellAnswer", "WindowReduceAnswer"),
    ("CellBatchAnswer", "WindowReduceBatchAnswer"),
    ("CellRemoveRequest", "WindowReduceRemoveRequest"),
    ("CellRemoveResponse", "WindowReduceRemoveResponse"),
    ("CellDeleteRequest", "WindowReduceDeleteRequest"),
    ("CellDeleteResponse", "WindowReduceDeleteResponse"),
    ("CellCall", "WindowReduceCall"),
    ("CellReply", "WindowReduceReply"),
];

fn new_name(old: &str) -> String {
    RENAMED.iter().find(|(o, _)| *o == old).map_or(old, |(_, n)| *n).to_string()
}

fn reencode(new_name: &str, bytes: &[u8]) -> Vec<u8> {
    macro_rules! through {
        ($($name:literal => $ty:ty),* $(,)?) => {
            match new_name {
                $($name => <$ty>::decode(bytes).expect($name).encode_to_vec(),)*
                other => panic!("no decoder for {other}"),
            }
        };
    }
    through! {
        "PairLanes" => wire::PairLanes,
        "LaneList" => wire::LaneList,
        "CellEntry" => wire::CellEntry,
        "Observation" => wire::Observation,
        "RestoredObservation" => wire::RestoredObservation,
        "WindowReduceAddRequest" => wire::WindowReduceAddRequest,
        "WindowReduceAddResponse" => wire::WindowReduceAddResponse,
        "WindowReduceRestoreRequest" => wire::WindowReduceRestoreRequest,
        "WindowReduceRestoreResponse" => wire::WindowReduceRestoreResponse,
        "WindowReduceSelector" => wire::WindowReduceSelector,
        "Window" => wire::Window,
        "WindowReduceQuery" => wire::WindowReduceQuery,
        "WindowReduceBatchQuery" => wire::WindowReduceBatchQuery,
        "SampleInfo" => wire::SampleInfo,
        "WindowReduceAnswer" => wire::WindowReduceAnswer,
        "WindowReduceBatchAnswer" => wire::WindowReduceBatchAnswer,
        "TimeSpan" => wire::TimeSpan,
        "KeyFilter" => wire::KeyFilter,
        "WindowReduceRemoveRequest" => wire::WindowReduceRemoveRequest,
        "WindowReduceRemoveResponse" => wire::WindowReduceRemoveResponse,
        "WindowReduceDeleteRequest" => wire::WindowReduceDeleteRequest,
        "WindowReduceDeleteResponse" => wire::WindowReduceDeleteResponse,
        "WindowReduceCall" => wire::WindowReduceCall,
        "CallStatus" => wire::CallStatus,
        "WindowReduceReply" => wire::WindowReduceReply,
    }
}

#[test]
fn the_golden_bytes_of_every_message_decode_and_re_encode_identically() {
    let values = read_golden(GOLDEN);
    assert_eq!(values.len(), 25, "the golden file holds one value per message");
    for g in &values {
        assert!(g.bytes.len() >= 3, "{}: a golden value that is nearly empty proves nothing", g.name);
        let again = reencode(&new_name(&g.name), &g.bytes);
        assert_eq!(again, g.bytes, "{}: the renamed type does not write the bytes the old one did", g.name);
    }
}

#[test]
fn every_golden_value_sets_every_field_its_message_has_so_no_field_escapes_the_comparison() {
    let proto = parse_proto(PROTO);
    let values = read_golden(GOLDEN);
    let covered: BTreeSet<String> = values.iter().map(|g| new_name(&g.name)).collect();
    let declared: BTreeSet<String> = proto.messages.keys().cloned().collect();
    assert_eq!(covered, declared, "the golden file and the proto name different messages");
    for g in &values {
        let shape = &proto.messages[&new_name(&g.name)];
        let want: BTreeSet<u32> = shape.fields.iter().map(|f| f.0).collect();
        assert!(!want.is_empty(), "{}", g.name);
        assert_eq!(field_numbers(&g.bytes), want, "{}: the golden value must set exactly the fields the message declares", g.name);
    }
}

#[test]
fn the_field_numbers_wire_types_labels_enum_values_and_reserved_numbers_are_those_the_proto_had_before() {
    let proto = parse_proto(PROTO);
    let before = read_table(TABLE, &new_name);
    assert_eq!(before.messages.len(), 25);
    assert_eq!(before.enums.len(), 6);
    assert_eq!(proto.messages.len(), before.messages.len(), "a message was added or dropped");
    for (name, old) in &before.messages {
        let now = proto.messages.get(name).unwrap_or_else(|| panic!("{name} is missing from the proto"));
        assert_eq!(now.fields, old.fields, "{name}: a field number, wire type or label changed");
        assert_eq!(now.reserved, old.reserved, "{name}: a reserved number changed");
    }
    assert_eq!(proto.enums, before.enums, "an enum value number changed");
}

#[test]
fn the_proto_names_no_snapshot_for_this_unit_and_the_stored_key_tag_is_still_cells() {
    assert!(!PROTO.to_ascii_lowercase().contains("snapshot"), "the proto still says snapshot for the unit now called a sample");
    assert!(!PROTO.contains("temporalstore.cells"), "the package must be temporalstore.windowreduce.v1");
    assert!(PROTO.contains("package temporalstore.windowreduce.v1;"));
    // The stored key tag is a storage tag: it is written into every key and is never renamed.
    assert_eq!(crate::engine::cell_set::VIEW_TAG, "cells");
}
