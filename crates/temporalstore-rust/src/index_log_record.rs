// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The served-index log record, ported from the design described below.
//!
//! Direct port: same record shape, same field numbers, same framing (see [`crate::record_framing`]).
//! Only the names follow this crate's vocabulary — routing bucket for slot, block for page, band
//! for zone, WAL for the operation log.
//!
//! The index log is the durable statement of *where blocks live* and *how far the WAL has been
//! dumped*. Three things make it load-bearing, and all three are why it is ported rather than
//! approximated:
//!
//!   * [`IndexMetaItem::start_wal_id`] is the dump watermark. Replay resumes from it, and WAL
//!     truncation must never pass it — a record below the watermark has had its blocks
//!     materialised into a band, one above it has not, and dropping the latter destroys the only
//!     durable copy.
//!   * [`IndexItem::in_wal`] records whether a block still lives in the WAL rather than a band.
//!     It is the flag that makes an address resolvable without a lookup table.
//!   * [`BandInfo`] carries the band lifecycle. This design pre-allocates a band in an INIT
//!     state, makes it durable, then creates the stream and moves it to CREATED — so a crash
//!     between the two leaves a band that is reused rather than an orphaned stream.

use prost::Message;

/// Record format version, mirroring the on-disk index-log version.
pub const INDEX_LOG_RECORD_VERSION: u32 = 1;

/// One served-index log record.
///
/// Field numbers match this design one-for-one, including the gap at 2 where a since-deprecated
/// item-type discriminator sat. The gap is preserved deliberately: reusing the tag would make the
/// two encodings disagree while still parsing, which is worse than a hole.
#[derive(Clone, PartialEq, Message)]
pub struct IndexLogRecord {
    /// On-disk version.
    #[prost(uint32, tag = "1")]
    pub version: u32,
    // Tag 2 is retired. Do not reuse.
    /// Fixed-width.
    #[prost(fixed64, tag = "3")]
    pub routing_bucket: u64,
    /// Block-location entries.
    #[prost(message, repeated, tag = "4")]
    pub items: Vec<IndexItem>,
    /// Present on a meta record.
    #[prost(message, optional, tag = "5")]
    pub meta_item: Option<IndexMetaItem>,
    /// Every object in this routing bucket, on a meta record.
    #[prost(message, repeated, tag = "6")]
    pub object_items: Vec<IndexObjectItem>,
    #[prost(uint64, tag = "7")]
    pub sequence: u64,
    /// The WAL sequence this index state reflects.
    #[prost(uint64, tag = "8")]
    pub wal_sequence: u64,
    #[prost(uint64, tag = "9")]
    pub timestamp_ms: u64,
}

/// Where one block lives.
#[derive(Clone, PartialEq, Message)]
pub struct IndexItem {
    #[prost(uint32, tag = "1")]
    pub object_id: u32,
    #[prost(uint32, tag = "2")]
    pub block_id: u32,
    /// The block's address. When [`Self::in_wal`] is set this is the log id — the byte offset of
    /// the WAL record carrying the block — otherwise it addresses a band.
    #[prost(uint64, tag = "3")]
    pub address: u64,
    #[prost(uint32, tag = "4")]
    pub size: u32,
    /// The block still lives in the WAL and has not been dumped into a band yet.
    #[prost(bool, tag = "5")]
    pub in_wal: bool,
    #[prost(bool, tag = "6")]
    pub deleted: bool,
    #[prost(uint32, tag = "7")]
    pub model_id: u32,
}

/// Band lifecycle state. This design calls a band a zone.
///
/// Numbering matches this design exactly, including that RECYCLED is 4 and 3 is unused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BandState {
    /// Reserved and durable, but its stream does not exist yet.
    Init = 0,
    /// Stream created and accepting writes.
    Created = 1,
    /// Sealed; no further writes.
    Frozen = 2,
    // 3 is unused in this design.
    /// Reclaimed and available for reuse.
    Recycled = 4,
}

/// One band's lifecycle record.
#[derive(Clone, PartialEq, Message)]
pub struct BandInfo {
    #[prost(uint32, tag = "1")]
    pub band_id: u32,
    #[prost(uint64, tag = "2")]
    pub total_bytes: u64,
    #[prost(enumeration = "BandState", tag = "3")]
    pub state: i32,
    #[prost(uint64, tag = "4")]
    pub init_time_ms: u64,
    #[prost(uint64, tag = "5")]
    pub created_time_ms: u64,
    #[prost(uint64, tag = "6")]
    pub frozen_time_ms: u64,
    #[prost(uint64, tag = "7")]
    pub recycled_time_ms: u64,
    /// Unique id for the band's lifetime, used to identify out-of-date blocks that point at a
    /// band slot which has since been recycled.
    #[prost(uint64, tag = "8")]
    pub version: u64,
}

/// The index meta record: the dump watermark and the band catalogue.
#[derive(Clone, PartialEq, Message)]
pub struct IndexMetaItem {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// The lowest WAL log id that has NOT been truncated — the dump watermark.
    ///
    /// Replay starts here, and truncation must never advance past it.
    #[prost(uint64, tag = "2")]
    pub start_wal_id: u64,
    #[prost(map = "uint32, message", tag = "3")]
    pub bands: std::collections::HashMap<u32, BandInfo>,
    #[prost(uint64, tag = "4")]
    pub timestamp_ms: u64,
    /// Version of the band catalogue.
    #[prost(uint64, tag = "5")]
    pub band_version: u64,
}

/// Per-object metadata carried on a meta record. Only the TTL is meaningful.
#[derive(Clone, PartialEq, Message)]
pub struct IndexObjectItem {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(uint32, tag = "2")]
    pub object_id: u32,
    #[prost(uint64, tag = "3")]
    pub ttl: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_framing::{decode_framed_at, encode_framed, FramedRecords};

    #[test]
    fn field_numbers_match_the_on_disk_index_log_layout() {
        // Wire compatibility is the point, so assert on bytes rather than trusting attributes.
        // routing_bucket is field 3, FIXED64 (wire type 1): key = (3 << 3) | 1 = 0x19.
        let record = IndexLogRecord {
            routing_bucket: 1,
            ..Default::default()
        };
        let encoded = record.encode_to_vec();
        assert_eq!(encoded[0], 0x19, "routing_bucket must be fixed64 at tag 3");
        assert_eq!(encoded.len(), 9, "fixed64 is 8 bytes plus the key");

        // in_wal is field 5, varint: key = (5 << 3) | 0 = 0x28.
        let item = IndexItem {
            in_wal: true,
            ..Default::default()
        };
        assert_eq!(item.encode_to_vec(), vec![0x28, 0x01]);
    }

    #[test]
    fn band_states_keep_their_on_disk_numbering() {
        // RECYCLED is 4, not 3 -- the gap is real and a renumber would silently reinterpret
        // existing records.
        assert_eq!(BandState::Init as i32, 0);
        assert_eq!(BandState::Created as i32, 1);
        assert_eq!(BandState::Frozen as i32, 2);
        assert_eq!(BandState::Recycled as i32, 4);
    }

    #[test]
    fn tag_two_stays_retired() {
        // This design retired tag 2. Encoding must never emit it, or the two disagree while
        // still parsing.
        let record = IndexLogRecord {
            version: INDEX_LOG_RECORD_VERSION,
            routing_bucket: 9,
            sequence: 3,
            ..Default::default()
        };
        let encoded = record.encode_to_vec();
        // Field 2 as varint would be key 0x10; as any wire type the key's tag bits are 2.
        assert!(
            !encoded.iter().any(|&byte| byte >> 3 == 2),
            "no field with tag 2 may be emitted"
        );
    }

    #[test]
    fn index_records_share_the_wal_framing() {
        // One framing for both streams, as in this design.
        let record = IndexLogRecord {
            version: INDEX_LOG_RECORD_VERSION,
            routing_bucket: 4,
            items: vec![IndexItem {
                object_id: 1,
                block_id: 2,
                address: 4096,
                size: 512,
                in_wal: true,
                ..Default::default()
            }],
            sequence: 8,
            wal_sequence: 77,
            ..Default::default()
        };
        let framed = encode_framed(&record);
        let (decoded, next_offset): (IndexLogRecord, usize) = decode_framed_at(&framed, 0).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(next_offset, framed.len());
    }

    #[test]
    fn the_dump_watermark_round_trips_with_the_band_catalogue() {
        // start_wal_id is what replay resumes from and what truncation must not pass, so it has
        // to survive a round trip alongside the bands it describes.
        let mut bands = std::collections::HashMap::new();
        bands.insert(
            1,
            BandInfo {
                band_id: 1,
                total_bytes: 1 << 20,
                state: BandState::Frozen as i32,
                version: 7,
                ..Default::default()
            },
        );
        let record = IndexLogRecord {
            version: INDEX_LOG_RECORD_VERSION,
            meta_item: Some(IndexMetaItem {
                version: 2,
                start_wal_id: 987_654,
                bands,
                timestamp_ms: 5,
                band_version: 7,
            }),
            ..Default::default()
        };
        let framed = encode_framed(&record);
        let (decoded, _): (IndexLogRecord, usize) = decode_framed_at(&framed, 0).unwrap();
        let meta = decoded.meta_item.expect("meta item");
        assert_eq!(meta.start_wal_id, 987_654);
        assert_eq!(meta.bands[&1].state, BandState::Frozen as i32);
        assert_eq!(meta.bands[&1].version, 7);
    }

    #[test]
    fn a_scan_yields_the_log_id_of_each_index_record() {
        let mut stream = Vec::new();
        let mut expected = Vec::new();
        for sequence in 1..=3 {
            expected.push(stream.len() as u64);
            stream.extend_from_slice(&encode_framed(&IndexLogRecord {
                version: INDEX_LOG_RECORD_VERSION,
                sequence,
                ..Default::default()
            }));
        }
        let seen: Vec<u64> = FramedRecords::<IndexLogRecord>::new(&stream, 0)
            .map(|item| item.unwrap().0)
            .collect();
        assert_eq!(seen, expected);
    }
}
