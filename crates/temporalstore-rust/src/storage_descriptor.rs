// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage-layer descriptors: the per-block header and the segmented-stream layer.
//!
//! Completes the on-disk type set alongside [`crate::wal_record`] (the write-ahead log) and
//! [`crate::index_log_record`] (the served-index log). Three groups live here:
//!
//!   * [`BlockHeader`] — the per-block descriptor. This is what the log-item-to-block
//!     conversion fills in, and it is where [`BlockHeader::block_in_wal`] records that a
//!     block's bytes are still in the WAL rather than in a band.
//!   * [`SlabInfo`] / [`SlabHeader`] / [`BlockFooter`] — the segmented-stream layer. A stream
//!     is a chain of slabs; each slab carries a header naming the whole chain, and the
//!     fixed-size blocks inside it end with a footer that makes a torn tail detectable on
//!     reopen.
//!   * [`StreamInfo`] / [`WalInfo`] — the runtime views a follower uses to adopt a leader's
//!     stream state without re-reading the stream itself.
//!
//! Field numbers are fixed by the on-disk layout and must not be renumbered: a stored stream
//! outlives the process that wrote it.

use prost::Message;

/// Block size a stream is written in: 128 KiB.
pub const STREAM_BLOCK_SIZE: u64 = 1 << 17;

/// Footer reserved at the end of every stream block: 128 B.
pub const STREAM_BLOCK_FOOTER_SIZE: u64 = 1 << 7;

/// Magic stamped into slab headers and block footers.
pub const STREAM_MAGIC: u64 = 0xBCBC_BCBC_6666_6666;

/// Stream format version.
pub const STREAM_VERSION: u32 = 1;

/// How a block's payload was compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockCompression {
    None = 0,
    Lz4 = 1,
}

/// Per-block descriptor.
///
/// `raw_data_size` is the size before compression and `data_size` after, so a reader can size
/// its buffer without decompressing first.
#[derive(Clone, PartialEq, Message)]
pub struct BlockHeader {
    /// Fixed-width, so the encoded size does not vary with the bucket value.
    #[prost(fixed64, tag = "1")]
    pub routing_bucket: u64,
    #[prost(uint32, tag = "2")]
    pub object_id: u32,
    #[prost(uint32, tag = "3")]
    pub block_id: u32,
    /// Size before compression.
    #[prost(uint64, tag = "4")]
    pub raw_data_size: u64,
    /// Size after compression.
    #[prost(uint64, tag = "5")]
    pub data_size: u64,
    #[prost(uint64, tag = "6")]
    pub timestamp_ms: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub key: Vec<u8>,
    #[prost(uint32, tag = "8")]
    pub model_id: u32,
    #[prost(enumeration = "BlockCompression", tag = "9")]
    pub compress: i32,
    /// The block's bytes live in the WAL record at [`Self::wal_sequence`], not in a band. Set
    /// for every block that has not been dumped yet.
    #[prost(bool, tag = "10")]
    pub block_in_wal: bool,
    /// Blocks for one object are ordered by this. It is derived from the log sequence, so a
    /// later write always wins.
    #[prost(uint64, tag = "11")]
    pub version: u64,
    /// Position in the WAL when the block was written.
    #[prost(uint64, tag = "12")]
    pub wal_sequence: u64,
}

/// One slab in a stream's chain.
///
/// `truncated_offset` is how a stream records that its head has been logically discarded
/// without rewriting the slab.
#[derive(Clone, PartialEq, Message)]
pub struct SlabInfo {
    #[prost(uint64, tag = "1")]
    pub slab_id: u64,
    #[prost(uint64, tag = "2")]
    pub start_offset: u64,
    #[prost(uint64, tag = "3")]
    pub slab_start_offset: u64,
    #[prost(uint64, tag = "4")]
    pub slab_end_offset: u64,
    #[prost(uint64, tag = "5")]
    pub end_record_sequence: u64,
    #[prost(uint64, tag = "6")]
    pub freeze_ms: u64,
    #[prost(uint64, tag = "7")]
    pub end_offset: u64,
    #[prost(uint64, tag = "8")]
    pub truncated_offset: u64,
}

/// The header written at the front of every slab.
///
/// It names the whole chain (`data_slabs`), which is what lets a reopen rebuild the stream
/// from the last slab alone rather than scanning all of them.
#[derive(Clone, PartialEq, Message)]
pub struct SlabHeader {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint64, tag = "2")]
    pub slab_id: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub slab_name: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub timestamp_ms: u64,
    #[prost(uint32, tag = "5")]
    pub version: u32,
    #[prost(uint64, tag = "6")]
    pub start_record_sequence: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub client_token: Vec<u8>,
    #[prost(message, repeated, tag = "8")]
    pub data_slabs: Vec<SlabInfo>,
    #[prost(uint64, tag = "9")]
    pub start_offset: u64,
    /// Present in the layout, never computed here. Nothing writes it and nothing reads it.
    ///
    /// It exists to chain one block to the one before it, so a block that went missing or arrived
    /// out of order is caught by the chain rather than by each record separately. This store does
    /// not do that: a record carries its own CRC32C, which is written and verified on every read,
    /// and a missing record shows up as a break in the sequence the loader already refuses.
    ///
    /// Left in place so the layout stays the shape it is read against. Do not add a check against
    /// it without computing it first -- it is always zero, so such a check would either pass
    /// always or fail always, and both look like a working validator.
    #[prost(uint32, tag = "10")]
    pub prev_block_crc32c: u32,
    #[prost(uint64, tag = "11")]
    pub truncated_offset: u64,
    #[prost(fixed32, tag = "12")]
    pub header_size: u32,
    #[prost(message, repeated, tag = "13")]
    pub obsolete_slabs: Vec<SlabInfo>,
}

/// The footer closing every fixed-size block within a slab.
///
/// A tail scan on reopen reads these to recover where the stream really ended: the last
/// complete record, its sequence, and the running CRC. Tag 11 is unused in the on-disk layout
/// and stays unused here.
#[derive(Clone, PartialEq, Message)]
pub struct BlockFooter {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint32, tag = "2")]
    pub version: u32,
    #[prost(uint64, tag = "3")]
    pub timestamp_ms: u64,
    /// Present in the layout, never computed here: the writer passes a literal zero and nothing
    /// reads it back. Integrity comes from the per-record CRC32C instead, which is verified on
    /// every read -- see `prev_block_crc32c` for the same note and the same warning about adding
    /// a check against a field that is always zero.
    #[prost(fixed32, tag = "4")]
    pub block_crc: u32,
    #[prost(uint64, tag = "5")]
    pub block_number: u64,
    #[prost(uint32, tag = "6")]
    pub block_end: u32,
    #[prost(uint64, tag = "7")]
    pub last_record_offset: u64,
    #[prost(uint32, tag = "8")]
    pub last_record_left_size: u32,
    #[prost(uint64, tag = "9")]
    pub last_record_sequence: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub client_token: Vec<u8>,
    // Tag 11 is unused on disk. Do not reuse.
    #[prost(uint64, tag = "12")]
    pub truncated_offset: u64,
}

/// Runtime view of a stream, used to restore a follower's stream state from a leader's.
#[derive(Clone, PartialEq, Message)]
pub struct StreamInfo {
    #[prost(message, repeated, tag = "1")]
    pub slab_infos: Vec<SlabInfo>,
    #[prost(uint64, tag = "2")]
    pub start_record_id: u64,
    #[prost(uint64, tag = "3")]
    pub length: u64,
    #[prost(uint64, tag = "4")]
    pub persistent_length: u64,
}

/// Runtime view of the WAL, carrying the sequence a follower may trust.
///
/// The reported sequence is clamped to the last durable slab's end sequence, so a follower
/// never adopts a sequence that exists only in the leader's memory.
#[derive(Clone, PartialEq, Message)]
pub struct WalInfo {
    #[prost(message, optional, tag = "1")]
    pub stream_info: Option<StreamInfo>,
    #[prost(uint64, tag = "2")]
    pub current_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_framing::{decode_framed_at, encode_framed};

    #[test]
    fn field_numbers_match_the_on_disk_block_header_layout() {
        // routing_bucket is field 1, FIXED64 (wire type 1): key = (1 << 3) | 1 = 0x09.
        let header = BlockHeader {
            routing_bucket: 1,
            ..Default::default()
        };
        let encoded = header.encode_to_vec();
        assert_eq!(encoded[0], 0x09, "routing_bucket must be fixed64 at tag 1");
        assert_eq!(encoded.len(), 9);

        // block_in_wal is field 10, varint: key = (10 << 3) | 0 = 0x50.
        let in_wal = BlockHeader {
            block_in_wal: true,
            ..Default::default()
        };
        assert_eq!(in_wal.encode_to_vec(), vec![0x50, 0x01]);
    }

    #[test]
    fn stream_constants_keep_their_on_disk_values() {
        assert_eq!(STREAM_BLOCK_SIZE, 131_072, "128 KiB blocks");
        assert_eq!(STREAM_BLOCK_FOOTER_SIZE, 128);
        assert_eq!(STREAM_MAGIC, 0xBCBC_BCBC_6666_6666);
        assert_eq!(STREAM_VERSION, 1);
        // A footer must fit inside a block with room left for payload, or the framing
        // degenerates into all-footer.
        assert!(STREAM_BLOCK_FOOTER_SIZE < STREAM_BLOCK_SIZE);
    }

    #[test]
    fn block_footer_leaves_tag_eleven_unused() {
        // Emitting tag 11 would make our encoding disagree with a stored one while still
        // parsing, which is the failure mode that does not announce itself.
        let footer = BlockFooter {
            magic: STREAM_MAGIC,
            version: STREAM_VERSION,
            block_number: 3,
            truncated_offset: 9,
            ..Default::default()
        };
        assert!(
            !footer.encode_to_vec().iter().any(|&byte| byte >> 3 == 11),
            "tag 11 must stay unused"
        );
    }

    #[test]
    fn a_block_in_wal_header_round_trips_through_the_shared_framing() {
        let header = BlockHeader {
            routing_bucket: 7,
            object_id: 2,
            block_id: 5,
            raw_data_size: 4096,
            data_size: 1024,
            compress: BlockCompression::Lz4 as i32,
            block_in_wal: true,
            version: 42,
            wal_sequence: 42,
            key: b"object-key".to_vec(),
            ..Default::default()
        };
        let framed = encode_framed(&header);
        let (decoded, next): (BlockHeader, usize) = decode_framed_at(&framed, 0).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(next, framed.len());
        assert!(decoded.block_in_wal);
        assert_eq!(decoded.compress, BlockCompression::Lz4 as i32);
    }

    #[test]
    fn slab_header_carries_the_chain_so_a_reopen_can_rebuild_from_the_last_slab() {
        let header = SlabHeader {
            magic: STREAM_MAGIC,
            slab_id: 3,
            version: STREAM_VERSION,
            data_slabs: vec![
                SlabInfo {
                    slab_id: 1,
                    end_record_sequence: 10,
                    ..Default::default()
                },
                SlabInfo {
                    slab_id: 2,
                    end_record_sequence: 20,
                    ..Default::default()
                },
            ],
            obsolete_slabs: vec![SlabInfo {
                slab_id: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        let framed = encode_framed(&header);
        let (decoded, _): (SlabHeader, usize) = decode_framed_at(&framed, 0).unwrap();
        assert_eq!(decoded.data_slabs.len(), 2);
        assert_eq!(decoded.data_slabs[1].end_record_sequence, 20);
        assert_eq!(decoded.obsolete_slabs.len(), 1);
    }
}
