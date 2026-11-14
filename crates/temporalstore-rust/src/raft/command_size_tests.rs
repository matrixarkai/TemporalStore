// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `command_size_bytes` is the JSON length of a command, counted while it is written.
//!
//! The function it replaced built the JSON in a `Vec` to read `.len()`. The count has to be the
//! same number for every command, not close to it: it is compared with the raft entry limit and
//! summed into the log-size figures, so a count that disagreed by a byte could split a command
//! that fit or admit one that did not. The old expression is therefore kept here, as the oracle.

use super::*;
use crate::types::{Command, SequenceFeatureRow};

/// What `command_size_bytes` computed before it counted: the length of the JSON it built.
fn the_length_of_the_json_it_used_to_build(command: &Command) -> u64 {
    serde_json::to_vec(command)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default()
}

fn rows(count: usize, wide: bool) -> Vec<SequenceFeatureRow> {
    (0..count)
        .map(|offset| {
            if wide {
                // the widest every field can be: more digits than any small value has
                SequenceFeatureRow {
                    timestamp_ms: u64::MAX - offset as u64,
                    gid: u64::MAX,
                    action_type: u32::MAX,
                    duration: u32::MAX,
                    author_id: u64::MAX - 1,
                }
            } else {
                SequenceFeatureRow {
                    timestamp_ms: offset as u64,
                    gid: 0,
                    action_type: (offset % 8) as u32,
                    duration: (offset % 600) as u32,
                    author_id: 7,
                }
            }
        })
        .collect()
}

#[test]
fn a_counted_command_size_equals_the_length_of_its_json_for_every_shape() {
    let commands: Vec<Command> = vec![
        // empty and near-empty
        Command::StringSet { key: String::new(), value: Vec::new() },
        Command::StringSet { key: "k".to_string(), value: vec![0] },
        // a value whose bytes have one, two and three digits as JSON numbers
        Command::StringSet { key: "digits".to_string(), value: vec![0, 9, 10, 99, 100, 255] },
        Command::StringSet { key: "bulk".to_string(), value: (0..=255u8).cycle().take(4096).collect() },
        // keys that JSON escapes or encodes as more than one byte
        Command::StringSet { key: "quote\"back\\slash\nnewline\ttab".to_string(), value: vec![1, 2, 3] },
        Command::StringSet { key: "unicode \u{e9}\u{4e2d}\u{1F600}".to_string(), value: vec![200] },
        Command::StringSet { key: "\u{0}\u{1f}control".to_string(), value: Vec::new() },
        // the command that carries rows, from none to many, narrow and widest
        Command::SequenceAdd { key: "s".to_string(), rows: Vec::new() },
        Command::SequenceAdd { key: "s".to_string(), rows: rows(1, false) },
        Command::SequenceAdd { key: "s".to_string(), rows: rows(1, true) },
        Command::SequenceAdd { key: "s".to_string(), rows: rows(7, true) },
        Command::SequenceAdd { key: "s".to_string(), rows: rows(770, false) },
        Command::SequenceAdd { key: "s".to_string(), rows: rows(770, true) },
    ];
    let mut checked = 0usize;
    for command in &commands {
        let expected = the_length_of_the_json_it_used_to_build(command);
        assert!(expected > 0, "the oracle measured nothing for {command:?}");
        assert_eq!(
            command_size_bytes(command),
            expected,
            "the counted size differs from the JSON length for {command:?}"
        );
        checked += 1;
    }
    // The corpus has to span the sizes the comparison is about, or equality proves little.
    let sizes: Vec<u64> = commands
        .iter()
        .map(the_length_of_the_json_it_used_to_build)
        .collect();
    assert!(
        sizes.iter().min().unwrap() < &100 && sizes.iter().max().unwrap() > &50_000,
        "the corpus does not span small to large commands: {sizes:?}"
    );
    assert_eq!(checked, commands.len());
}

/// A value that writes part of itself and then fails, the way a serialization error arrives.
struct FailsAfterWriting;

impl serde::Serialize for FailsAfterWriting {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&123_456_789u64)?;
        Err(serde::ser::Error::custom("this value cannot be serialized"))
    }
}

#[test]
fn a_value_that_fails_to_serialize_counts_as_zero_as_it_did() {
    // The old expression turned any error into zero, however much had been written before it. The
    // counter has already seen those bytes when the error arrives, so returning its total would be
    // a different answer.
    assert!(serde_json::to_vec(&FailsAfterWriting).is_err(), "the fixture must fail");
    assert_eq!(json_size_bytes(&FailsAfterWriting), 0);
    let old = serde_json::to_vec(&FailsAfterWriting)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default();
    assert_eq!(json_size_bytes(&FailsAfterWriting), old);
}

#[test]
fn the_byte_counter_counts_what_is_written_to_it_across_writes() {
    // `to_writer` may call `write` many times with pieces of any size, so the counter must add
    // each one, and must report all of it as written or `write_all` loops.
    use std::io::Write as _;
    let mut counter = ByteCounter(0);
    assert_eq!(counter.write(b"").unwrap(), 0);
    assert_eq!(counter.write(b"abc").unwrap(), 3);
    counter.write_all(&[0u8; 1000]).unwrap();
    counter.flush().unwrap();
    assert_eq!(counter.0, 1003);
    // and as the JSON of a value the standard way to measure it agrees: a bare string is its
    // text, escaped, plus two quotes.
    assert_eq!(json_size_bytes("a\"b"), serde_json::to_vec("a\"b").unwrap().len() as u64);
    assert_eq!(json_size_bytes(&Vec::<u8>::new()), 2);
}
