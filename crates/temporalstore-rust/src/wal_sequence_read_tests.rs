// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::sdk::v1;
use crate::wal_proto::{
    BINARY_PAYLOAD_MARKER, COMPRESSED_ESCAPED_PAYLOAD_MARKER, COMPRESSED_RAW_PAYLOAD_MARKER,
    RAW_PAYLOAD_MARKER,
};
use prost::Message;

fn message(count: usize) -> v1::EngineWalRecord {
    v1::EngineWalRecord {
        shard_id: 7,
        sequence: u64::MAX - 1,
        metadata: Some(v1::EngineWalMetadata {
            batch_id: Some(9),
            batch_size: Some(2),
            batch_index: Some(2),
            ..Default::default()
        }),
        items: (0..count)
            .map(|index| v1::EngineWalItem {
                kind_name: Some("hash".into()),
                object_key: Some(format!("tenant/context/memory/{index}")),
                element_name: Some(format!("field-{index}")),
                value: Some(vec![b'x'; 64]),
                ..Default::default()
            })
            .collect(),
        staged_blocks: (0..count)
            .map(|index| v1::WalStagedBlock {
                object_id: Some(index as u64),
                element_name: Some(format!("field-{index}")),
                block: vec![b'x'; 64],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn escape(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .flat_map(|byte| match *byte {
            b'\n' => vec![0x1b, 1],
            0x1b => vec![0x1b, 2],
            byte => vec![byte],
        })
        .collect()
}

fn payloads(message: &v1::EngineWalRecord) -> Vec<Vec<u8>> {
    let raw = message.encode_to_vec();
    let bulk = zstd::bulk::compress(&raw, 1).unwrap();
    let streaming = zstd::stream::encode_all(raw.as_slice(), 1).unwrap();
    [
        (RAW_PAYLOAD_MARKER, raw.clone()),
        (BINARY_PAYLOAD_MARKER, escape(&raw)),
        (COMPRESSED_RAW_PAYLOAD_MARKER, bulk),
        (COMPRESSED_ESCAPED_PAYLOAD_MARKER, escape(&streaming)),
    ]
    .into_iter()
    .map(|(marker, body)| {
        let mut payload = vec![marker];
        payload.extend(body);
        payload
    })
    .collect()
}

fn assert_same(line: &[u8]) {
    assert_eq!(
        decode_wal_sequence(line).map_err(|error| error.to_string()),
        decode_wal_line(line)
            .map(|record| record.sequence)
            .map_err(|error| error.to_string()),
    );
}

#[test]
fn wal_sequence_reads_accept_every_payload_shape_and_control_field() {
    for mut message in [message(0), message(1), message(128)] {
        for command in [
            None,
            Some(v1::WalCommand { kind: None }),
            Some(v1::WalCommand {
                kind: Some(
                    crate::raft::wal_proto::command_to_proto(&Command::StringSet {
                        key: "key".into(),
                        value: vec![b'x'; 64],
                    })
                    .unwrap(),
                ),
            }),
        ] {
            message.command = command;
            for payload in payloads(&message) {
                let framed = crate::log_framing::encode_line(&payload);
                assert_same(&framed);
                assert_eq!(decode_wal_sequence(&framed).unwrap(), message.sequence);
            }
        }
    }
}

#[test]
fn wal_sequence_reads_keep_command_errors_and_full_protobuf_validation() {
    let mut invalid = message(1);
    invalid.command = Some(v1::WalCommand {
        kind: Some(v1::wal_command::Kind::SdkCommand(vec![0xff])),
    });
    for payload in payloads(&invalid) {
        let framed = crate::log_framing::encode_line(&payload);
        assert!(decode_wal_sequence(&framed).is_err());
        assert_same(&framed);
    }
    let raw = message(1).encode_to_vec();
    for end in 0..raw.len() {
        let mut payload = vec![RAW_PAYLOAD_MARKER];
        payload.extend_from_slice(&raw[..end]);
        assert_same(&crate::log_framing::encode_line(&payload));
    }
    // A valid sequence prefix cannot hide a malformed outcome at the end of the message.
    let mut payload = vec![RAW_PAYLOAD_MARKER];
    payload.extend_from_slice(&raw);
    payload.extend_from_slice(&[0x2a, 0xff]);
    let framed = crate::log_framing::encode_line(&payload);
    assert!(decode_wal_sequence(&framed).is_err());
    assert_same(&framed);
}

#[test]
fn wal_sequence_reads_keep_frame_integrity_and_encoding_errors() {
    for payload in payloads(&message(1)) {
        for framed in [
            crate::log_framing::encode_line(&payload),
            crate::log_framing::encode_record(&payload),
        ] {
            for end in 0..framed.len() {
                assert_same(&framed[..end]);
            }
            for index in 0..framed.len() {
                let mut corrupted = framed.clone();
                corrupted[index] ^= 0x80;
                assert_same(&corrupted);
            }
        }
    }
    for payload in [
        vec![0],
        vec![BINARY_PAYLOAD_MARKER, 0x1b],
        vec![COMPRESSED_RAW_PAYLOAD_MARKER, 0xff],
    ] {
        let framed = crate::log_framing::encode_line(&payload);
        assert!(decode_wal_sequence(&framed).is_err());
        assert_same(&framed);
    }
}

#[test]
fn wal_sequence_tail_scan_keeps_reservations_and_record_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tail.bin");
    let mut bytes = Vec::new();
    for sequence in [11, 12] {
        let mut message = message(128);
        message.sequence = sequence;
        bytes.extend(crate::log_framing::encode_record(&payloads(&message)[1]));
    }
    std::fs::write(&path, &bytes).unwrap();
    let expected = (12, bytes.len() as u64);
    assert_eq!(last_wal_sequence_in(&path).unwrap(), expected);
    bytes.resize(bytes.len() + 8192, 0);
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(last_wal_sequence_in(&path).unwrap(), expected);
}

#[test]
#[cfg(feature = "alloc-probe")]
fn wal_sequence_reads_do_not_allocate_replay_outcomes() {
    let key = "context/memory/catalog/".repeat(16);
    let mut message = message(1024);
    message.command = Some(v1::WalCommand {
        kind: Some(
            crate::raft::wal_proto::command_to_proto(&Command::HashMultiSet {
                key: key.clone(),
                entries: (0..1024)
                    .map(|index| (format!("field-{index}"), vec![b'x'; 64]))
                    .collect(),
            })
            .unwrap(),
        ),
    });
    for item in &mut message.items {
        item.object_key = None;
    }
    let framed = crate::log_framing::encode_record(&payloads(&message)[0]);
    let full_work = || std::hint::black_box(decode_wal_line(&framed).unwrap());
    let sequence_work = || std::hint::black_box(decode_wal_sequence(&framed).unwrap());
    drop(full_work());
    sequence_work();
    let probe = crate::alloc_probe::Probe::start();
    let full = full_work();
    let full_cost = probe.stop();
    let probe = crate::alloc_probe::Probe::start();
    let sequence = sequence_work();
    let sequence_cost = probe.stop();
    assert_eq!(sequence, full.sequence);
    assert!(full
        .outcomes
        .iter()
        .all(|outcome| outcome.object_key == key));
    println!("full={full_cost:?} sequence={sequence_cost:?}");
    // The full decoder restores the omitted command key for every outcome. A Vec collect
    // may reuse its input allocation, so its element size is not an allocation lower bound.
    assert!(
        full_cost.alloc_bytes >= sequence_cost.alloc_bytes + (1024 * key.len()) as u64,
        "full={full_cost:?} sequence={sequence_cost:?}"
    );
}
