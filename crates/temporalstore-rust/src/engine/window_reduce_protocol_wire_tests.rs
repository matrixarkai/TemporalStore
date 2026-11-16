// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The bytes of the window reducer protocol did not move when its messages were renamed.
//!
//! Protobuf puts field numbers and wire types on the wire and never a name, so a rename is
//! byte-neutral as long as no number or type is touched. These tests prove that it was not touched,
//! against two files written from the proto as it stood before the rename:
//!
//! * `window_reduce_wire_golden.txt`: one fully populated value of every message, encoded in
//!   field number order, every field set to a distinct non-default value and every repeated field
//!   holding two entries. Names in it are the old ones.
//! * `window_reduce_wire_table.txt`: every (message, field number, wire type, label), every enum's
//!   value numbers and every reserved number, again under the old names.
//!
//! [`RENAMED`] is the one place the old and the new message names meet.

use super::wire;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};

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

/// One field: number, wire type, repeated.
type Field = (u32, u8, bool);

#[derive(Default)]
struct Shape {
    fields: BTreeSet<Field>,
    reserved: BTreeSet<u32>,
}

#[derive(Default)]
struct Parsed {
    messages: BTreeMap<String, Shape>,
    enums: BTreeMap<String, BTreeSet<i64>>,
}

fn wire_type(ty: &str, enums: &BTreeSet<String>) -> u8 {
    match ty {
        "int32" | "int64" | "uint32" | "uint64" | "bool" => 0,
        "double" => 1,
        "string" | "bytes" => 2,
        other if enums.contains(other) => 0,
        _ => 2,
    }
}

/// Reads the messages and enums of a proto file whose declarations are one per line.
fn parse_proto(text: &str) -> Parsed {
    let lines: Vec<&str> = text.lines().map(|l| l.split("//").next().unwrap().trim()).filter(|l| !l.is_empty()).collect();
    let enum_names: BTreeSet<String> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("enum ").map(|r| r.split_whitespace().next().unwrap().to_string()))
        .collect();
    let mut parsed = Parsed::default();
    let mut open: Option<(bool, String)> = None; // (is_message, name)
    for line in lines {
        if let Some(rest) = line.strip_prefix("message ") {
            let name = rest.split_whitespace().next().unwrap().to_string();
            parsed.messages.insert(name.clone(), Shape::default());
            open = Some((true, name));
        } else if let Some(rest) = line.strip_prefix("enum ") {
            let name = rest.split_whitespace().next().unwrap().to_string();
            parsed.enums.insert(name.clone(), BTreeSet::new());
            open = Some((false, name));
        } else if line == "}" {
            open = None;
        } else if let Some((is_message, name)) = &open {
            let body = line.trim_end_matches(';');
            if *is_message {
                if let Some(n) = body.strip_prefix("reserved ") {
                    parsed.messages.get_mut(name).unwrap().reserved.insert(n.trim().parse().unwrap());
                    continue;
                }
                let (repeated, body) = match body.strip_prefix("repeated ") {
                    Some(b) => (true, b),
                    None => (false, body),
                };
                let (decl, number) = body.split_once('=').unwrap();
                let ty = decl.split_whitespace().next().unwrap();
                let number: u32 = number.trim().parse().unwrap();
                parsed.messages.get_mut(name).unwrap().fields.insert((number, wire_type(ty, &enum_names), repeated));
            } else {
                let (_, number) = body.split_once('=').unwrap();
                parsed.enums.get_mut(name).unwrap().insert(number.trim().parse().unwrap());
            }
        }
    }
    parsed
}

/// The golden values, as (old message name, bytes).
fn golden() -> Vec<(String, Vec<u8>)> {
    GOLDEN
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
            let (name, hex) = l.split_once(' ').unwrap();
            let bytes = (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
            (name.to_string(), bytes)
        })
        .collect()
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

fn varint(bytes: &[u8], at: &mut usize) -> u64 {
    let (mut value, mut shift) = (0u64, 0);
    loop {
        let b = bytes[*at];
        *at += 1;
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

/// The field numbers present at the top level of an encoded message.
fn field_numbers(bytes: &[u8]) -> BTreeSet<u32> {
    let (mut at, mut seen) = (0, BTreeSet::new());
    while at < bytes.len() {
        let tag = varint(bytes, &mut at);
        seen.insert((tag >> 3) as u32);
        match tag & 7 {
            0 => {
                varint(bytes, &mut at);
            }
            1 => at += 8,
            2 => {
                let len = varint(bytes, &mut at) as usize;
                at += len;
            }
            5 => at += 4,
            other => panic!("wire type {other}"),
        }
    }
    assert_eq!(at, bytes.len(), "a field ran past the end of its message");
    seen
}

#[test]
fn the_golden_bytes_of_every_message_decode_and_re_encode_identically() {
    let values = golden();
    assert_eq!(values.len(), 25, "the golden file holds one value per message");
    for (old, bytes) in &values {
        assert!(bytes.len() >= 3, "{old}: a golden value that is nearly empty proves nothing");
        let again = reencode(&new_name(old), bytes);
        assert_eq!(&again, bytes, "{old}: the renamed type does not write the bytes the old one did");
    }
}

#[test]
fn every_golden_value_sets_every_field_its_message_has_so_no_field_escapes_the_comparison() {
    let proto = parse_proto(PROTO);
    let values = golden();
    let covered: BTreeSet<String> = values.iter().map(|(old, _)| new_name(old)).collect();
    let declared: BTreeSet<String> = proto.messages.keys().cloned().collect();
    assert_eq!(covered, declared, "the golden file and the proto name different messages");
    for (old, bytes) in &values {
        let shape = &proto.messages[&new_name(old)];
        let want: BTreeSet<u32> = shape.fields.iter().map(|f| f.0).collect();
        assert!(!want.is_empty(), "{old}");
        assert_eq!(field_numbers(bytes), want, "{old}: the golden value must set exactly the fields the message declares");
    }
}

#[test]
fn the_field_numbers_wire_types_labels_enum_values_and_reserved_numbers_are_those_the_proto_had_before() {
    let proto = parse_proto(PROTO);
    let mut old_messages: BTreeMap<String, Shape> = BTreeMap::new();
    let mut old_enums: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for line in TABLE.lines().filter(|l| !l.starts_with('#') && !l.trim().is_empty()) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0] {
            "message" => {
                old_messages.insert(new_name(parts[1]), Shape::default());
            }
            "field" => {
                let repeated = parts[4] == "repeated";
                old_messages
                    .get_mut(&new_name(parts[1]))
                    .unwrap()
                    .fields
                    .insert((parts[2].parse().unwrap(), parts[3].parse().unwrap(), repeated));
            }
            "reserved" => {
                old_messages.get_mut(&new_name(parts[1])).unwrap().reserved.insert(parts[2].parse().unwrap());
            }
            "enum" => {
                old_enums.insert(new_name(parts[1]), parts[2..].iter().map(|n| n.parse().unwrap()).collect());
            }
            other => panic!("unknown table row {other}"),
        }
    }
    assert_eq!(old_messages.len(), 25);
    assert_eq!(old_enums.len(), 6);
    assert_eq!(proto.messages.len(), old_messages.len(), "a message was added or dropped");
    for (name, old) in &old_messages {
        let now = proto.messages.get(name).unwrap_or_else(|| panic!("{name} is missing from the proto"));
        assert_eq!(now.fields, old.fields, "{name}: a field number, wire type or label changed");
        assert_eq!(now.reserved, old.reserved, "{name}: a reserved number changed");
    }
    assert_eq!(proto.enums, old_enums, "an enum value number changed");
}

#[test]
fn the_proto_names_no_snapshot_for_this_unit_and_the_stored_key_tag_is_still_cells() {
    assert!(!PROTO.to_ascii_lowercase().contains("snapshot"), "the proto still says snapshot for the unit now called a sample");
    assert!(!PROTO.contains("temporalstore.cells"), "the package must be temporalstore.windowreduce.v1");
    assert!(PROTO.contains("package temporalstore.windowreduce.v1;"));
    // The stored key tag is a storage tag: it is written into every key and is never renamed.
    assert_eq!(crate::engine::cell_set::VIEW_TAG, "cells");
}
