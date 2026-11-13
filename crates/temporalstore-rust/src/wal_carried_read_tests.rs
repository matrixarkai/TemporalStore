// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn message(rows: usize, fields: usize) -> v1::EngineWalRecord {
    let mut message = v1::EngineWalRecord {
        shard_id: 7,
        sequence: 11,
        metadata: Some(v1::EngineWalMetadata::default()),
        ..Default::default()
    };
    for row in 0..rows {
        let key = format!("tenant/entity/{row}");
        let id = crate::engine::hashing::stable_block_object_id(7, "hash", &key);
        message.staged_blocks.push(v1::WalStagedBlock {
            object_id: Some(id),
            block: vec![row as u8; 1024],
            element_name: if row % 2 == 0 { None } else { Some(format!("field-{row}")) },
            ..Default::default()
        });
        for field in 0..fields {
            message.items.push(v1::EngineWalItem {
                kind_name: Some("hash".into()),
                object_key: Some(key.clone()),
                element_name: Some(format!("field-{field}")),
                ..Default::default()
            });
        }
    }
    message
}

fn payload(message: &v1::EngineWalRecord, marker: u8) -> Vec<u8> {
    let encoded = message.encode_to_vec();
    let body = match marker {
        RAW_PAYLOAD_MARKER => encoded,
        BINARY_PAYLOAD_MARKER => escape_newlines(&encoded),
        COMPRESSED_RAW_PAYLOAD_MARKER => zstd::bulk::compress(&encoded, 1).unwrap(),
        COMPRESSED_ESCAPED_PAYLOAD_MARKER =>
            escape_newlines(&zstd::bulk::compress(&encoded, 1).unwrap()),
        _ => unreachable!(),
    };
    let mut result = vec![marker];
    result.extend(body);
    result
}

#[test]
fn carried_projection_matches_full_restore_for_all_payload_encodings() {
    for (rows, fields) in [(0, 0), (1, 0), (8, 20), (128, 20)] {
        let mut message = message(rows, fields);
        if rows > 1 {
            message.staged_blocks[0].object_id = Some(0);
            message.staged_blocks[1].object_id = Some(u64::MAX);
            message.staged_blocks[1].element_name = Some(String::new());
            message.staged_blocks.last_mut().unwrap().element_name = Some("field-\u{96ea}".into());
        }
        for marker in [RAW_PAYLOAD_MARKER, BINARY_PAYLOAD_MARKER,
                       COMPRESSED_RAW_PAYLOAD_MARKER, COMPRESSED_ESCAPED_PAYLOAD_MARKER] {
            let payload = payload(&message, marker);
            let expected = decode(&payload).unwrap().staged_blocks;
            assert_eq!(decode_carried_blocks(&payload).unwrap(), expected);
            let framed = crate::log_framing::encode_frame(&payload);
            assert_eq!(crate::wal::decode_carried_blocks(&framed).unwrap(), expected);
            assert_eq!(crate::wal::decode_wal_line(&framed).unwrap().staged_blocks, expected);
        }
    }
}

#[test]
fn carried_projection_preserves_command_and_implied_identity_restoration() {
    let key = "tenant/command-key";
    let command = crate::types::Command::StringSet { key: key.into(), value: b"value".to_vec() };
    for with_command in [false, true] {
        let mut message = message(1, 1);
        message.staged_blocks[0].object_id = None;
        message.staged_blocks[0].element_name = None;
        message.items[0].kind_name = Some("string".into());
        message.items[0].object_key = if with_command { None } else { Some(key.into()) };
        message.command = with_command.then(|| v1::WalCommand {
            kind: Some(crate::raft::wal_proto::command_to_proto(&command).unwrap()),
        });
        let payload = payload(&message, RAW_PAYLOAD_MARKER);
        let blocks = decode_carried_blocks(&payload).unwrap();
        assert_eq!(blocks, decode(&payload).unwrap().staged_blocks);
        assert_eq!(blocks[0].object_id, crate::engine::hashing::stable_block_object_id(7, "string", key));
    }
    let mut message = message(8, 3);
    message.staged_blocks[3].object_id = None;
    let payload = payload(&message, RAW_PAYLOAD_MARKER);
    assert_eq!(decode_carried_blocks(&payload).unwrap(), decode(&payload).unwrap().staged_blocks);
}

#[test]
fn carried_projection_keeps_protobuf_and_frame_corruption_checks() {
    let encoded = payload(&message(2, 3), RAW_PAYLOAD_MARKER);
    for len in 1..encoded.len() {
        let full = decode(&encoded[..len]).map(|record| record.staged_blocks);
        assert_eq!(decode_carried_blocks(&encoded[..len]), full, "prefix {len}");
    }
    let mut framed = crate::log_framing::encode_frame(&encoded);
    *framed.last_mut().unwrap() ^= 1;
    assert!(crate::wal::decode_carried_blocks(&framed).is_err());
    for len in 0..framed.len() {
        let projected = crate::wal::decode_carried_blocks(&framed[..len]);
        let full = crate::wal::decode_wal_line(&framed[..len]);
        assert_eq!(projected.is_err(), full.is_err());
    }
    assert!(crate::wal::decode_carried_blocks(&crate::log_framing::encode_frame(b"unknown")).is_err());
    // A valid outer checksum must not conceal a malformed nested outcome or staged block.
    for body in [vec![0x2a, 1, 0xff], vec![0x32, 1, 0xff]] {
        let mut payload = vec![RAW_PAYLOAD_MARKER];
        payload.extend(body);
        assert!(decode_carried_blocks(&payload).is_err());
        assert!(decode(&payload).is_err());
    }
}

#[test]
#[cfg(feature = "alloc-probe")]
fn carried_projection_avoids_unused_outcome_restore_allocations() {
    for numeric_kind in [false, true] {
        let mut message = message(500, 20);
        if numeric_kind {
            for item in &mut message.items {
                item.kind_code = kind_code("hash");
                item.kind_name = None;
            }
        }
        let payload = payload(&message, RAW_PAYLOAD_MARKER);
        let probe = crate::alloc_probe::Probe::start();
        let full = decode(&payload).unwrap();
        let old = probe.stop();
        let probe = crate::alloc_probe::Probe::start();
        let blocks = decode_carried_blocks(&payload).unwrap();
        let new = probe.stop();
        assert_eq!(blocks, full.staged_blocks);
        assert!(new.allocs < old.allocs, "old={old:?}, new={new:?}");
        assert!(new.alloc_bytes < old.alloc_bytes, "old={old:?}, new={new:?}");
        if numeric_kind {
            assert!(old.allocs - new.allocs >= 10000, "old={old:?}, new={new:?}");
        }
        println!("CARRIED_RESTORE rows=500 fields=20 numeric_kind={numeric_kind} old_allocs={} new_allocs={} old_bytes={} new_bytes={}",
                 old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes);
    }
}

#[test]
#[ignore = "release timing fixture"]
fn carried_projection_cost() {
    for rows in [1, 128, 500] {
        let payload = payload(&message(rows, 20), COMPRESSED_RAW_PAYLOAD_MARKER);
        for sample in 0..4 {
            let measure = |projected: bool| {
                let started = std::time::Instant::now();
                for _ in 0..32 {
                    if projected {
                        std::hint::black_box(decode_carried_blocks(&payload).unwrap());
                    } else {
                        std::hint::black_box(decode(&payload).unwrap());
                    }
                }
                started.elapsed().as_nanos()
            };
            let (full_ns, projected_ns) = if sample % 2 == 0 {
                (measure(false), measure(true))
            } else {
                let projected = measure(true);
                (measure(false), projected)
            };
            println!("CARRIED_RESTORE_COST rows={rows} sample={sample} full_ns={full_ns} projected_ns={projected_ns}");
        }
    }
}
