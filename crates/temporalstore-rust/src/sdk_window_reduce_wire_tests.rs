// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The bytes of the window reducer commands did not move when their messages were renamed.
//!
//! The commands that are logged and replicated (a rollup and a removal) travel as protobuf messages in
//! `temporalstore.proto`, inside the `Command` oneof at fields 69 and 75, and a response carries their receipt in
//! `CommandResponse`. A protobuf rename is byte-neutral as long as no number or type is touched; these tests prove
//! none was, against two files written from the proto as it stood before the rename (names in them are the old
//! ones): `window_reduce_command_golden.txt` (one populated value of every message the rename touches, one line per
//! alternative of a oneof) and `window_reduce_command_table.txt` (every field number, wire type and label, every
//! enum value and every reserved number of those messages). [`RENAMED`] is where the old and new names meet.

use super::v1;
use crate::wire_check::{field_numbers, parse_proto, read_golden, read_table, Parsed};
use prost::Message;
use std::collections::BTreeSet;

const GOLDEN: &str = include_str!("../tests/golden/window_reduce_command_golden.txt");
const TABLE: &str = include_str!("../tests/golden/window_reduce_command_table.txt");
const PROTO: &str = include_str!("../../../proto/temporalstore/v1/temporalstore.proto");

/// Messages and enums whose name changed: (old, new). A name not listed here is unchanged.
const RENAMED: &[(&str, &str)] = &[
    ("CellRollup", "WindowReduceRollup"),
    ("CellRollupTier", "WindowReduceRollupTier"),
    ("CellRollupTruncate", "WindowReduceRollupTruncate"),
    ("CellRollupCap", "WindowReduceRollupCap"),
    ("CellRollupScore", "WindowReduceRollupScore"),
    ("CellRollupRatio", "WindowReduceRollupRatio"),
    ("CellRollupWilson", "WindowReduceRollupWilson"),
    ("CellRollupWeighted", "WindowReduceRollupWeighted"),
    ("CellRollupAnchor", "WindowReduceRollupAnchor"),
    ("CellRemove", "WindowReduceRemove"),
];

fn new_name(old: &str) -> String {
    RENAMED.iter().find(|(o, _)| *o == old).map_or(old, |(_, n)| *n).to_string()
}

fn message_of(entry: &str) -> String {
    new_name(entry.split('#').next().unwrap())
}

fn reencode(entry: &str, bytes: &[u8]) -> Vec<u8> {
    macro_rules! through {
        ($($name:literal => $ty:ty),* $(,)?) => {
            match message_of(entry).as_str() {
                $($name => <$ty>::decode(bytes).expect(entry).encode_to_vec(),)*
                other => panic!("no decoder for {other}"),
            }
        };
    }
    through! {
        "WindowReduceRollup" => v1::WindowReduceRollup,
        "WindowReduceRollupTier" => v1::WindowReduceRollupTier,
        "WindowReduceRollupTruncate" => v1::WindowReduceRollupTruncate,
        "WindowReduceRollupCap" => v1::WindowReduceRollupCap,
        "WindowReduceRollupScore" => v1::WindowReduceRollupScore,
        "WindowReduceRollupRatio" => v1::WindowReduceRollupRatio,
        "WindowReduceRollupWilson" => v1::WindowReduceRollupWilson,
        "WindowReduceRollupWeighted" => v1::WindowReduceRollupWeighted,
        "WindowReduceRemove" => v1::WindowReduceRemove,
        "CellMemberSet" => v1::CellMemberSet,
        "CellMember" => v1::CellMember,
        "CellPredicate" => v1::CellPredicate,
        "Command" => v1::Command,
        "CommandResponse" => v1::CommandResponse,
    }
}

fn declared(proto: &Parsed, message: &str) -> BTreeSet<u32> {
    proto.messages[message].fields.iter().map(|f| f.0).collect()
}

#[test]
fn the_golden_bytes_of_every_touched_command_message_decode_and_re_encode_identically() {
    let values = read_golden(GOLDEN);
    assert_eq!(values.len(), 21, "the golden file holds a line per message and per oneof alternative");
    for g in &values {
        assert!(g.bytes.len() >= 2, "{}: a golden value that is nearly empty proves nothing", g.name);
        assert_eq!(reencode(&g.name, &g.bytes), g.bytes, "{}: the renamed type does not write the bytes the old one did", g.name);
    }
}

#[test]
fn the_oneof_field_numbers_of_the_two_commands_are_69_and_75_under_their_new_names() {
    let values = read_golden(GOLDEN);
    let bytes_of = |entry: &str| values.iter().find(|g| g.name == entry).unwrap().bytes.clone();
    match v1::Command::decode(bytes_of("Command#69").as_slice()).unwrap().kind {
        Some(v1::command::Kind::WindowReduceRollup(rollup)) => {
            assert_eq!(rollup.encode_to_vec(), v1::WindowReduceRollup::decode(bytes_of("CellRollup").as_slice()).unwrap().encode_to_vec());
        }
        other => panic!("field 69 is the window reduce rollup, not {other:?}"),
    }
    match v1::Command::decode(bytes_of("Command#75").as_slice()).unwrap().kind {
        Some(v1::command::Kind::WindowReduceRemove(remove)) => {
            assert_eq!(remove.encode_to_vec(), v1::WindowReduceRemove::decode(bytes_of("CellRemove").as_slice()).unwrap().encode_to_vec());
        }
        other => panic!("field 75 is the window reduce removal, not {other:?}"),
    }
    let receipt = v1::CommandResponse::decode(bytes_of("CommandResponse").as_slice()).unwrap();
    assert_eq!(
        (receipt.count, receipt.samples_before, receipt.cells_before, receipt.cells_after, receipt.cells_dropped, receipt.cells_removed),
        (1007, 1011, 1012, 1013, 1014, 1016),
        "the receipt fields keep their numbers (7, 11, 12, 13, 14, 16) under their new names"
    );
}

#[test]
fn every_golden_command_value_sets_the_fields_it_states_and_together_they_cover_every_declared_field() {
    let proto = parse_proto(PROTO);
    let values = read_golden(GOLDEN);
    let mut covered: std::collections::BTreeMap<String, BTreeSet<u32>> = Default::default();
    for g in &values {
        let message = message_of(&g.name);
        let stated = g.sets.clone().expect("every line of this file states its field numbers");
        assert_eq!(field_numbers(&g.bytes), stated, "{}: the value must set exactly the fields its line states", g.name);
        assert!(stated.is_subset(&declared(&proto, &message)), "{}: a stated field the message does not declare", g.name);
        covered.entry(message).or_default().extend(stated);
    }
    for (message, seen) in &covered {
        match message.as_str() {
            // 75 alternatives of the command oneof and the receipt fields of the response are not all exercised
            "Command" => assert_eq!(seen, &BTreeSet::from([69, 75])),
            "CommandResponse" => assert_eq!(seen, &(7..=16).collect::<BTreeSet<u32>>()),
            _ => assert_eq!(seen, &declared(&proto, message), "{message}: a declared field no golden value sets"),
        }
    }
}

#[test]
fn the_command_proto_field_numbers_wire_types_labels_and_enum_values_are_those_it_had_before() {
    let proto = parse_proto(PROTO);
    let before = read_table(TABLE, &new_name);
    assert_eq!(before.messages.len(), 14);
    assert_eq!(before.enums.len(), 2);
    for (name, old) in &before.messages {
        let now = proto.messages.get(name).unwrap_or_else(|| panic!("{name} is missing from the proto"));
        assert_eq!(now.fields, old.fields, "{name}: a field number, wire type or label changed");
        assert_eq!(now.reserved, old.reserved, "{name}: a reserved number changed");
    }
    for (name, values) in &before.enums {
        assert_eq!(proto.enums.get(name), Some(values), "{name}: an enum value number changed");
    }
}

#[test]
fn the_command_proto_names_no_cell_rollup_or_cell_remove_and_no_snapshot_count_field() {
    let word = |needle: &str| {
        PROTO.match_indices(needle).any(|(at, _)| {
            let before = PROTO[..at].chars().next_back().map_or(false, |c| c.is_alphanumeric() || c == '_');
            let after = PROTO[at + needle.len()..].chars().next().map_or(false, |c| c.is_alphanumeric() || c == '_');
            !before && !after
        })
    };
    for gone in ["CellRollup", "CellRemove", "CellRollupAnchor", "cell_rollup", "cell_remove", "snapshots_before"] {
        assert!(!word(gone), "temporalstore.proto still says {gone}");
    }
    assert!(PROTO.contains("WindowReduceRollup window_reduce_rollup = 69;"));
    assert!(PROTO.contains("WindowReduceRemove window_reduce_remove = 75;"));
    assert!(PROTO.contains("uint64 samples_before = 11;"));
}
