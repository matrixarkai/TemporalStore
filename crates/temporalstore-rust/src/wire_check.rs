// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the wire-bytes tests of the window reducer share: a reader for a `.proto` file whose declarations
//! are one per line, a reader for the golden and table files recorded before a rename, and a scanner that
//! lists the field numbers present in an encoded message. Test support only.

use std::collections::{BTreeMap, BTreeSet};

/// One field: number, wire type, repeated.
pub(crate) type Field = (u32, u8, bool);

#[derive(Default, Debug)]
pub(crate) struct Shape {
    pub(crate) fields: BTreeSet<Field>,
    pub(crate) reserved: BTreeSet<u32>,
}

#[derive(Default, Debug)]
pub(crate) struct Parsed {
    pub(crate) messages: BTreeMap<String, Shape>,
    pub(crate) enums: BTreeMap<String, BTreeSet<i64>>,
}

fn wire_type(ty: &str, enums: &BTreeSet<String>) -> u8 {
    match ty {
        "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64" | "bool" => 0,
        "double" => 1,
        "string" | "bytes" => 2,
        other if enums.contains(other) => 0,
        _ => 2,
    }
}

enum Open {
    Message(String),
    Enum(String),
    Other,
}

/// Reads the messages and enums of a proto file whose declarations are one per line. A `oneof` counts for
/// the message that holds it; a `service` is skipped.
pub(crate) fn parse_proto(text: &str) -> Parsed {
    let lines: Vec<&str> = text.lines().map(|l| l.split("//").next().unwrap().trim()).filter(|l| !l.is_empty()).collect();
    let enum_names: BTreeSet<String> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("enum ").map(|r| r.split_whitespace().next().unwrap().to_string()))
        .collect();
    let mut parsed = Parsed::default();
    let mut stack: Vec<Open> = Vec::new();
    for line in lines {
        if let Some(rest) = line.strip_prefix("message ") {
            let name = rest.split_whitespace().next().unwrap().to_string();
            parsed.messages.insert(name.clone(), Shape::default());
            // An empty message can close on the declaration line.
            if !rest.split_once('{').is_some_and(|(_, body)| body.trim() == "}") {
                stack.push(Open::Message(name));
            }
        } else if let Some(rest) = line.strip_prefix("enum ") {
            let name = rest.split_whitespace().next().unwrap().to_string();
            parsed.enums.insert(name.clone(), BTreeSet::new());
            stack.push(Open::Enum(name));
        } else if line.starts_with("service ") {
            stack.push(Open::Other);
        } else if line.starts_with("oneof ") {
            // fields of a oneof belong to the message that holds it
            let parent = match stack.last() {
                Some(Open::Message(name)) => Open::Message(name.clone()),
                _ => panic!("a oneof outside a message: {line}"),
            };
            stack.push(parent);
        } else if line == "}" {
            stack.pop();
        } else {
            let body = line.trim_end_matches(';');
            match stack.last() {
                Some(Open::Message(name)) => {
                    if let Some(n) = body.strip_prefix("reserved ") {
                        parsed.messages.get_mut(name).unwrap().reserved.insert(n.trim().parse().unwrap());
                        continue;
                    }
                    let (repeated, body) = match body.strip_prefix("repeated ") {
                        Some(b) => (true, b),
                        None => (false, body.strip_prefix("optional ").unwrap_or(body)),
                    };
                    let (decl, number) = body.split_once('=').unwrap();
                    let ty = decl.split_whitespace().next().unwrap();
                    let number: u32 = number.trim().parse().unwrap();
                    parsed.messages.get_mut(name).unwrap().fields.insert((number, wire_type(ty, &enum_names), repeated));
                }
                Some(Open::Enum(name)) => {
                    let (_, number) = body.split_once('=').unwrap();
                    parsed.enums.get_mut(name).unwrap().insert(number.trim().parse().unwrap());
                }
                _ => {}
            }
        }
    }
    assert!(stack.is_empty(), "unbalanced braces");
    parsed
}

/// The table recorded before a rename, with each name passed through `new_name`.
pub(crate) fn read_table(text: &str, new_name: &dyn Fn(&str) -> String) -> Parsed {
    let mut table = Parsed::default();
    for line in text.lines().filter(|l| !l.starts_with('#') && !l.trim().is_empty()) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0] {
            "message" => {
                table.messages.insert(new_name(parts[1]), Shape::default());
            }
            "field" => {
                let repeated = parts[4] == "repeated";
                table
                    .messages
                    .get_mut(&new_name(parts[1]))
                    .unwrap()
                    .fields
                    .insert((parts[2].parse().unwrap(), parts[3].parse().unwrap(), repeated));
            }
            "reserved" => {
                table.messages.get_mut(&new_name(parts[1])).unwrap().reserved.insert(parts[2].parse().unwrap());
            }
            "enum" => {
                table.enums.insert(new_name(parts[1]), parts[2..].iter().map(|n| n.parse().unwrap()).collect());
            }
            other => panic!("unknown table row {other}"),
        }
    }
    table
}

/// One golden value: the old message name (with `#field` for one alternative of a oneof), the field numbers it
/// sets when the file states them, and its bytes.
pub(crate) struct Golden {
    pub(crate) name: String,
    pub(crate) sets: Option<BTreeSet<u32>>,
    pub(crate) bytes: Vec<u8>,
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
}

/// Reads `<name> <hex>` lines, or `<name> <numbers> <hex>` lines that state which fields they set.
pub(crate) fn read_golden(text: &str) -> Vec<Golden> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
            let parts: Vec<&str> = l.split(' ').collect();
            match parts.as_slice() {
                [name, hex] => Golden { name: name.to_string(), sets: None, bytes: unhex(hex) },
                [name, sets, hex] => Golden {
                    name: name.to_string(),
                    sets: Some(sets.split(',').map(|n| n.parse().unwrap()).collect()),
                    bytes: unhex(hex),
                },
                _ => panic!("a golden line has two or three words: {l}"),
            }
        })
        .collect()
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
pub(crate) fn field_numbers(bytes: &[u8]) -> BTreeSet<u32> {
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
