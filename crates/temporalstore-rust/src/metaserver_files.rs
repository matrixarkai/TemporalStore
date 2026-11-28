// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The metaserver's durable snapshot files: a protobuf message behind one container byte.
//!
//! The messages and their converters are generated from the Rust types by
//! `tools/gen_proto_converters.py` into [`crate::metaserver_files_proto`], with every struct
//! destructured without `..` so a field added to a type cannot be left out. This module is the part
//! that decides what a FILE is.
//!
//! ONE READER, ONE SHAPE. A file that does not open with the container byte is refused, never parsed
//! as anything else: the JSON these files used to hold, an empty file, a torn upload. An enum number
//! this build does not know, and a message that is required and absent, are refused by name.

use std::io;

use prost::Message;

use crate::meta::MetaSnapshot;
use crate::metaserver_files_proto as convert;
use crate::rebalance::TaskSchedulerSnapshot;
use crate::sdk::v1;

/// The container byte in front of a protobuf document.
///
/// Not `{`, and not JSON whitespace, so a reader handed the JSON these files used to hold says so
/// instead of misreading it.
pub const CONTAINER_PROTOBUF: u8 = 0x01;

/// A message as the bytes of a file.
pub fn frame<M: Message>(message: &M) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + message.encoded_len());
    out.push(CONTAINER_PROTOBUF);
    message
        .encode(&mut out)
        .expect("a Vec sink cannot run out of room");
    out
}

/// A message from the bytes of a file, refusing anything that does not open with the container byte.
pub fn unframe<M: Message + Default>(bytes: &[u8], what: &str) -> io::Result<M> {
    match bytes.split_first() {
        Some((&CONTAINER_PROTOBUF, body)) => M::decode(body)
            .map_err(|error| io::Error::other(format!("{what} is not a valid message: {error}"))),
        _ => Err(io::Error::other(format!(
            "{what} does not open with the protobuf container byte"
        ))),
    }
}

/// A meta snapshot as the bytes of its file.
pub fn encode_meta_snapshot(snapshot: &MetaSnapshot) -> Vec<u8> {
    frame(&convert::meta_snapshot_to_proto(snapshot))
}

/// A meta snapshot from the bytes of its file.
pub fn decode_meta_snapshot(bytes: &[u8]) -> io::Result<MetaSnapshot> {
    let message = unframe::<v1::MfMetaSnapshot>(bytes, "a meta snapshot file")?;
    convert::meta_snapshot_from_proto(message).map_err(io::Error::other)
}

/// A scheduler snapshot as the bytes of its file.
pub fn encode_scheduler_snapshot(snapshot: &TaskSchedulerSnapshot) -> Vec<u8> {
    frame(&convert::task_scheduler_snapshot_to_proto(snapshot))
}

/// A scheduler snapshot from the bytes of its file.
pub fn decode_scheduler_snapshot(bytes: &[u8]) -> io::Result<TaskSchedulerSnapshot> {
    let message = unframe::<v1::MfTaskSchedulerSnapshot>(bytes, "a scheduler snapshot file")?;
    convert::task_scheduler_snapshot_from_proto(message).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_meta_snapshot() -> MetaSnapshot {
        let mut n = 41;
        convert::generated_round_trips::sample_meta_snapshot(&mut n, 0)
    }

    fn a_scheduler_snapshot() -> TaskSchedulerSnapshot {
        let mut n = 97;
        convert::generated_round_trips::sample_task_scheduler_snapshot(&mut n, 0)
    }

    /// A snapshot comes back as written, through the file's bytes and not just the converter.
    #[test]
    fn a_meta_snapshot_file_round_trips() {
        let snapshot = a_meta_snapshot();
        let bytes = encode_meta_snapshot(&snapshot);
        assert_eq!(bytes[0], CONTAINER_PROTOBUF);
        assert_eq!(decode_meta_snapshot(&bytes).expect("decode"), snapshot);
        // DENOMINATORS: the fixture really holds what it claims, or the equality covers less.
        assert!(snapshot.servers.len() >= 2 && snapshot.tables.len() >= 2);
        assert!(!snapshot.shards.is_empty() && !snapshot.proxies.is_empty());
    }

    #[test]
    fn a_scheduler_snapshot_file_round_trips() {
        let snapshot = a_scheduler_snapshot();
        let bytes = encode_scheduler_snapshot(&snapshot);
        assert_eq!(bytes[0], CONTAINER_PROTOBUF);
        assert_eq!(decode_scheduler_snapshot(&bytes).expect("decode"), snapshot);
        assert!(snapshot.tasks.len() >= 2, "the fixture must hold tasks");
    }

    /// The file is not JSON, and the reader refuses JSON: the shape these files used to hold.
    #[test]
    fn the_json_shape_is_refused() {
        let snapshot = a_meta_snapshot();
        let bytes = encode_meta_snapshot(&snapshot);
        assert!(
            serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
            "the binary shape must not parse as JSON, or the container byte proves nothing"
        );
        let json = serde_json::to_vec(&snapshot).expect("json");
        assert_eq!(json[0], b'{', "the control must really be a JSON document");
        assert!(decode_meta_snapshot(&json).is_err());
        assert!(decode_meta_snapshot(&[]).is_err());
        let scheduler_json = serde_json::to_vec(&a_scheduler_snapshot()).expect("json");
        assert!(decode_scheduler_snapshot(&scheduler_json).is_err());
        assert_ne!(CONTAINER_PROTOBUF, b'{');
        for whitespace in [b' ', b'\t', b'\r', b'\n'] {
            assert_ne!(CONTAINER_PROTOBUF, whitespace);
        }
    }

    /// A cut of the file is never read as the whole snapshot.
    ///
    /// A protobuf message cut BETWEEN fields is a valid shorter message; that is the format's nature
    /// and the container byte cannot close it. What closes it is how the file is written: to a
    /// temporary path, synced, then renamed, so a torn file is never the one a reader opens. What
    /// this pins is the reader's side: no cut equals the whole, and a cut inside a field is refused.
    #[test]
    fn a_cut_file_is_never_read_as_the_whole() {
        let snapshot = TaskSchedulerSnapshot {
            tasks: a_scheduler_snapshot().tasks.into_iter().take(1).collect(),
            next_task_id: 9,
        };
        let bytes = encode_scheduler_snapshot(&snapshot);
        assert!(bytes.len() > 40, "too few cuts for the loop to mean anything");
        let mut refused = 0usize;
        for cut in 0..bytes.len() {
            match decode_scheduler_snapshot(&bytes[..cut]) {
                Err(_) => refused += 1,
                Ok(read) => assert_ne!(read, snapshot, "a file cut at {cut} was read as the whole"),
            }
        }
        assert!(refused > 0, "no cut was refused, so the reader accepts anything");
        assert!(decode_scheduler_snapshot(&bytes).is_ok(), "the untouched bytes are the control");
        let whole_meta = a_meta_snapshot();
        let whole = encode_meta_snapshot(&whole_meta);
        for cut in [0, 1, whole.len() / 2, whole.len() - 1] {
            if let Ok(read) = decode_meta_snapshot(&whole[..cut]) {
                assert_ne!(read, whole_meta, "a meta snapshot cut at {cut} was read as the whole");
            }
        }
    }

    /// THE SNAPSHOT FILES ARE WRITTEN AND READ WITHOUT JSON.
    ///
    /// Source text, so the denominators come first: a scan that read nothing would pass. Each site
    /// must be found, long enough to be the real function, and must go through this module.
    #[test]
    fn the_snapshot_files_are_written_and_read_without_json() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let read = |path: &str| {
            std::fs::read_to_string(root.join(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
        };
        let uncommented = |text: &str| -> String {
            text.lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let body_of = |text: &str, name: &str, end: &str| -> String {
            let after = text
                .split_once(name)
                .unwrap_or_else(|| panic!("{name} is no longer in the file"))
                .1;
            after[..after.find(end).unwrap_or_else(|| panic!("{name} does not close"))].to_string()
        };

        let mut sites = Vec::new();
        sites.push(("meta/snapshot_ops.rs (whole file)", read("src/meta/snapshot_ops.rs")));
        let server = read("src/bin/metaserver.rs");
        for name in [
            "fn decode_meta_scheduler_file",
            "fn save_scheduler_snapshot",
            "fn save_meta_snapshot_file",
        ] {
            sites.push((name, body_of(&server, name, "\n}\n")));
        }
        let rebalance = read("src/rebalance.rs");
        for name in ["pub fn encode_snapshot", "pub fn decode_snapshot"] {
            sites.push((name, body_of(&rebalance, name, "\n    }\n")));
        }
        assert_eq!(sites.len(), 6, "six sites, or the loop below asserts less than it says");
        for (name, code) in &sites {
            assert!(code.len() > 100, "{name}: the scan found almost nothing");
            assert!(
                code.contains("metaserver_files"),
                "{name} no longer goes through the protobuf file codec"
            );
            assert!(
                !uncommented(code).contains("serde_json"),
                "{name} reaches for serde_json: the snapshot file is JSON again"
            );
        }
        // THE CONTROL: the same scan over text that does call it must see it.
        assert!(
            uncommented("fn a() { serde_json::to_vec(&x) }").contains("serde_json"),
            "the control no longer sees serde_json, so this scan could pass over a JSON arm"
        );
    }
}
