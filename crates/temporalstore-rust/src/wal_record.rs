// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-ahead log record, ported from the design described below.
//!
//! This is a direct port: same record shape, same field numbers, same framing, and the same
//! addressing scheme. Only the names differ, following this crate's vocabulary — a routing
//! bucket rather than a slot, a block rather than a page, a shard rather than a partition.
//!
//! # Why this replaces a command log
//!
//! The existing WAL record carries a `Command`, so recovery re-executes the operation. That
//! only reproduces state if everything that influenced the original execution is reproduced
//! too, which is why config-driven eviction had to be made WAL-sequence-ordered and why expiry
//! needs a replay clock. This design stores the *result* instead: an item describes a
//! mutation at the storage layer, and a block-carrying item holds the block image itself.
//! Replay installs bytes rather than re-deriving them.
//!
//! # Addressing: the log id
//!
//! A block-carrying item does not name a slab. Its address is the **log id** — the byte offset
//! of the record that contains it — exactly as this design sets `address = log_id` when it
//! turns a log item into a block descriptor. Nothing needs to be looked up: a read seeks to the
//! offset and parses the record there. That is why the framing below must be length-prefixed
//! and offset-addressable rather than line-oriented.
//!
//! The address cannot be known while a command executes, because the record has not been
//! appended yet and therefore has no offset. This design resolves this by staging: items
//! accumulate during execution, and at commit the append returns the offset which is then
//! written back into every staged block descriptor. This module provides the pieces for that;
//! see [`block_address_from_item`].
//!
//! # Framing
//!
//! `varint32(payload_len) | little_endian_u32(crc32c) | payload`
//!
//! matching the record header exactly. The payload is protobuf, with the same field
//! numbers as the log item, so the encodings are wire-compatible.

use prost::Message;

use crate::block_store::BlockAddress;
pub use crate::record_framing::{decode_framed_at, encode_framed, RecordFramingError as WalFramingError};

/// Record format version, mirroring the log version.
pub const WAL_RECORD_VERSION: u32 = 1;

/// One write-ahead log record: the unit that is appended, and whose byte offset becomes the log
/// id for every block it carries. Mirrors the operation-log message.
#[derive(Clone, PartialEq, Message)]
pub struct WalRecord {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub items: Vec<WalItem>,
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
}

/// One mutation inside a record. Field numbers match the log item one-for-one, so
/// the two encodings are wire-compatible; only the names are this crate's.
#[derive(Clone, PartialEq, Message)]
pub struct WalItem {
    #[prost(uint64, tag = "1")]
    pub routing_bucket: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub object_key: Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub model_id: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub value: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub timestamp_ms: u64,
    #[prost(uint64, tag = "7")]
    pub cluster_id: u64,
    #[prost(bool, tag = "8")]
    pub deleted: bool,
    #[prost(bool, tag = "9")]
    pub object_deleted: bool,
    /// The item carries a block image.
    #[prost(bool, tag = "10")]
    pub block_log: bool,
    #[prost(uint32, tag = "11")]
    pub object_id: u32,
    #[prost(uint32, tag = "12")]
    pub block_id: u32,
    /// The block image.
    #[prost(bytes = "vec", tag = "13")]
    pub block: Vec<u8>,
    #[prost(uint64, tag = "14")]
    pub ttl: u64,
    #[prost(bool, tag = "15")]
    pub meta_log: bool,
}

/// Build the block address for a block-carrying item, from the log id of the record that
/// contains it.
///
/// This is the port of the log-item-to-block-descriptor conversion: the address is
/// the log id, and the length is the record's framed size. Nothing is looked up — the address
/// alone locates the bytes.
pub fn block_address_from_item(log_id: u64, log_size: u64, item: &WalItem) -> BlockAddress {
    BlockAddress {
        page_slab_id: WAL_LOG_SLAB_ID,
        offset: log_id,
        length: log_size,
        page_id: Some(u64::from(item.block_id)),
        object_id: Some(u64::from(item.object_id)),
        routing_bucket: u32::try_from(item.routing_bucket).ok(),
        generation: None,
        band_id: None,
        sha256: None,
    }
}

/// Sentinel slab id marking an address that resolves inside the WAL rather than a slab.
/// A reserved slab id carries this distinction through the existing address type without
/// widening it or adding a parallel flag.
pub const WAL_LOG_SLAB_ID: u64 = u64::MAX - 1;

/// Whether an address resolves inside the WAL.
pub fn is_wal_resident(page_slab_id: u64) -> bool {
    page_slab_id == WAL_LOG_SLAB_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_item(routing_bucket: u64, block: &[u8]) -> WalItem {
        WalItem {
            routing_bucket,
            block_log: true,
            block: block.to_vec(),
            block_id: 7,
            object_id: 3,
            ..Default::default()
        }
    }

    #[test]
    fn framed_record_round_trips() {
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 42,
            items: vec![block_item(5, b"block bytes with a \n newline inside")],
        };
        let framed = encode_framed(&record);
        let (decoded, next_offset): (WalRecord, usize) = decode_framed_at(&framed, 0).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(next_offset, framed.len());
        // The newline is the whole reason this framing is length-prefixed rather than
        // line-oriented: a line reader would have split this record in two.
        assert!(decoded.items[0].block.contains(&b'\n'));
    }

    #[test]
    fn the_log_id_is_the_byte_offset_of_the_record() {
        // Two records in one log; the second is addressed by where it starts.
        let first = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 1,
            items: vec![block_item(1, b"first")],
        };
        let second = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 2,
            items: vec![block_item(2, b"second")],
        };
        let mut log = encode_framed(&first);
        let second_log_id = log.len();
        log.extend_from_slice(&encode_framed(&second));

        let (decoded, _): (WalRecord, usize) = decode_framed_at(&log, second_log_id).unwrap();
        assert_eq!(decoded.sequence, 2);
        assert_eq!(decoded.items[0].block, b"second");
    }

    #[test]
    fn sequential_scan_walks_every_record() {
        let records: Vec<WalRecord> = (1..=5)
            .map(|sequence| WalRecord {
                version: WAL_RECORD_VERSION,
                sequence,
                items: vec![block_item(sequence, format!("block-{sequence}").as_bytes())],
            })
            .collect();
        let mut log = Vec::new();
        for record in &records {
            log.extend_from_slice(&encode_framed(record));
        }

        let mut offset = 0;
        let mut seen: Vec<WalRecord> = Vec::new();
        while offset < log.len() {
            let (record, next_offset): (WalRecord, usize) =
                decode_framed_at(&log, offset).unwrap();
            // A scan MUST make progress; without this the loop is unbounded and the growing
            // `seen` takes the process out via the OOM killer rather than failing an assert.
            assert!(next_offset > offset, "scan did not advance past {offset}");
            seen.push(record);
            offset = next_offset;
        }
        assert_eq!(seen, records);
    }

    #[test]
    fn a_corrupted_payload_is_rejected_not_skipped() {
        // A block-carrying record IS the durable copy, so corruption must surface as an error
        // rather than as an absent block.
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 9,
            items: vec![block_item(1, b"payload")],
        };
        let mut framed = encode_framed(&record);
        let last = framed.len() - 1;
        framed[last] ^= 0xff;
        assert!(decode_framed_at::<WalRecord>(&framed, 0).is_err());
    }

    #[test]
    fn a_truncated_tail_is_rejected() {
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 9,
            items: vec![block_item(1, b"payload that will be cut short")],
        };
        let framed = encode_framed(&record);
        let truncated = &framed[..framed.len() - 5];
        assert!(decode_framed_at::<WalRecord>(truncated, 0).is_err());
    }

    #[test]
    fn varint_lengths_round_trip_across_byte_boundaries() {
        // The header is varint-length-prefixed, so a record straddling 127/128 bytes (and each
        // later boundary) must still frame and re-parse exactly.
        for payload_len in [0_usize, 1, 126, 127, 128, 129, 16_383, 16_384] {
            let record = WalRecord {
                version: WAL_RECORD_VERSION,
                sequence: 1,
                items: vec![block_item(1, &vec![b'x'; payload_len])],
            };
            let framed = encode_framed(&record);
            let (decoded, next_offset): (WalRecord, usize) = decode_framed_at(&framed, 0).unwrap();
            assert_eq!(decoded.items[0].block.len(), payload_len);
            assert_eq!(next_offset, framed.len(), "payload_len {payload_len}");
        }
    }

    #[test]
    fn block_address_carries_the_log_id() {
        let item = block_item(11, b"bytes");
        let address = block_address_from_item(4096, 512, &item);
        assert!(is_wal_resident(address.page_slab_id));
        assert_eq!(address.offset, 4096, "the address IS the log id");
        assert_eq!(address.length, 512);
        assert_eq!(address.routing_bucket, Some(11));
        assert_eq!(address.page_id, Some(7));
        assert_eq!(address.object_id, Some(3));
    }

    #[test]
    fn field_numbers_match_the_on_disk_log_item_layout() {
        // Wire compatibility is the point of porting the field numbers, so assert on the
        // encoding rather than trusting the attributes. routing_bucket is field 1, varint:
        // key byte = (1 << 3) | 0 = 0x08.
        let item = WalItem {
            routing_bucket: 5,
            ..Default::default()
        };
        assert_eq!(item.encode_to_vec(), vec![0x08, 0x05]);

        // block is field 13, length-delimited: key byte = (13 << 3) | 2 = 0x6a.
        let with_block = WalItem {
            block: b"ab".to_vec(),
            ..Default::default()
        };
        assert_eq!(with_block.encode_to_vec(), vec![0x6a, 0x02, b'a', b'b']);
    }

    /// What a write costs on disk, in the shape the log uses today versus the binary one.
    ///
    /// The value of a write is stored as an array of decimal numbers when the record is JSON,
    /// so every byte of user data becomes three or four characters before framing, metadata and
    /// key names are counted at all. This measures both on identical content so the cost is a
    /// number rather than an impression.
    #[test]
    fn record_encoding_cost_per_byte_written() {
        use crate::types::Command;

        for value_len in [64usize, 128, 1024, 4096] {
            let value = vec![b'v'; value_len];
            let key = "scale-key-000000000";

            // As written today: a JSON document, then the integrity frame around it.
            let json_record = serde_json::json!({
                "shard_id": 1,
                "sequence": 1,
                "command": { "kind": "string_set", "key": key, "value": value },
                "metadata": {
                    "version": 1,
                    "timestamp_ms": 1_787_270_070_192u64,
                    "items": [{
                        "item_kind": "kv",
                        "model": "string",
                        "object_key": key,
                        "slot_id": 8539,
                        "deleted": false,
                        "meta_log": false,
                        "block_log": false
                    }]
                }
            });
            let json_bytes = serde_json::to_vec(&json_record).unwrap();
            let json_framed = crate::log_framing::encode_line(&json_bytes);

            // The binary shape: one record carrying the same write, length-prefixed and
            // checksummed by the shared framing.
            let binary_record = WalRecord {
                version: WAL_RECORD_VERSION,
                sequence: 1,
                items: vec![WalItem {
                    routing_bucket: 8539,
                    object_key: key.as_bytes().to_vec(),
                    model_id: 1,
                    key: key.as_bytes().to_vec(),
                    value: value.clone(),
                    timestamp_ms: 1_787_270_070_192,
                    ..Default::default()
                }],
            };
            let binary_framed = encode_framed(&binary_record);

            let json_ratio = json_framed.len() as f64 / value_len as f64;
            let binary_ratio = binary_framed.len() as f64 / value_len as f64;
            println!(
                "  value {value_len:>5}B -> json {:>7}B ({json_ratio:>5.2}x)   binary {:>6}B ({binary_ratio:>5.2}x)   {:>5.1}x smaller",
                json_framed.len(),
                binary_framed.len(),
                json_framed.len() as f64 / binary_framed.len() as f64
            );

            // The binary record must never be the larger of the two.
            assert!(
                binary_framed.len() < json_framed.len(),
                "binary encoding should be smaller at {value_len}B"
            );
        }

        // Guard the headline: a small write costs several times its own size as JSON.
        let value = vec![b'v'; 128];
        let json = serde_json::to_vec(&serde_json::json!({
            "shard_id": 1, "sequence": 1,
            "command": { "kind": "string_set", "key": "scale-key-000000000", "value": value },
        }))
        .unwrap();
        assert!(
            json.len() > value.len() * 3,
            "a 128B value should cost more than 3x as JSON, got {}B",
            json.len()
        );
    }

    /// What a write costs on disk, in the shape the log uses today versus the binary one.
    ///
    /// The value of a write is stored as an array of decimal numbers when the record is JSON,
    /// so every byte of user data becomes three or four characters before framing, metadata and
    /// key names are counted at all. This measures both on identical content so the cost is a
    /// number rather than an impression.
    #[test]
    fn record_encoding_cost_per_byte_written() {
        use crate::types::Command;

        for value_len in [64usize, 128, 1024, 4096] {
            let value = vec![b'v'; value_len];
            let key = "scale-key-000000000";

            // As written today: a JSON document, then the integrity frame around it.
            let json_record = serde_json::json!({
                "shard_id": 1,
                "sequence": 1,
                "command": { "kind": "string_set", "key": key, "value": value },
                "metadata": {
                    "version": 1,
                    "timestamp_ms": 1_787_270_070_192u64,
                    "items": [{
                        "item_kind": "kv",
                        "model": "string",
                        "object_key": key,
                        "slot_id": 8539,
                        "deleted": false,
                        "meta_log": false,
                        "block_log": false
                    }]
                }
            });
            let json_bytes = serde_json::to_vec(&json_record).unwrap();
            let json_framed = crate::log_framing::encode_line(&json_bytes);

            // The binary shape: one record carrying the same write, length-prefixed and
            // checksummed by the shared framing.
            let binary_record = WalRecord {
                version: WAL_RECORD_VERSION,
                sequence: 1,
                items: vec![WalItem {
                    routing_bucket: 8539,
                    object_key: key.as_bytes().to_vec(),
                    model_id: 1,
                    key: key.as_bytes().to_vec(),
                    value: value.clone(),
                    timestamp_ms: 1_787_270_070_192,
                    ..Default::default()
                }],
            };
            let binary_framed = encode_framed(&binary_record);

            let json_ratio = json_framed.len() as f64 / value_len as f64;
            let binary_ratio = binary_framed.len() as f64 / value_len as f64;
            println!(
                "  value {value_len:>5}B -> json {:>7}B ({json_ratio:>5.2}x)   binary {:>6}B ({binary_ratio:>5.2}x)   {:>5.1}x smaller",
                json_framed.len(),
                binary_framed.len(),
                json_framed.len() as f64 / binary_framed.len() as f64
            );

            // The binary record must never be the larger of the two.
            assert!(
                binary_framed.len() < json_framed.len(),
                "binary encoding should be smaller at {value_len}B"
            );
        }

        // Guard the headline: a small write costs several times its own size as JSON.
        let value = vec![b'v'; 128];
        let json = serde_json::to_vec(&serde_json::json!({
            "shard_id": 1, "sequence": 1,
            "command": { "kind": "string_set", "key": "scale-key-000000000", "value": value },
        }))
        .unwrap();
        assert!(
            json.len() > value.len() * 3,
            "a 128B value should cost more than 3x as JSON, got {}B",
            json.len()
        );
    }
}
