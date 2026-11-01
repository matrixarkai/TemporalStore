// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOG-STREAM BYTE LAYOUT OF THE DESIGN BEING FOLLOWED: BLOBS OF 128 KiB BLOCKS, A FOOTER ON
//! EACH, RECORDS THAT SPAN THEM.
//!
//! Every constant, field and rule here was read from that design's stream writer and then CHECKED
//! AGAINST A FILE ITS OWN BINARY WROTE: a single-node cluster was brought up, the index stream it
//! produced (4,974 records across two full blocks and a partial third) was captured, and
//! `tests::the_captured_stream_is_read_exactly` decodes it while
//! `tests::our_writer_reproduces_the_captured_bytes` appends the same payloads and demands the same
//! 404,635 bytes. Nothing below is inferred from a schema alone.
//!
//! WHAT THE CAPTURE SETTLED THAT THE SOURCE DID NOT. The checksum is the Castagnoli polynomial with
//! an initial value of 0 and NO final inversion (neither the standard CRC-32C nor CRC-32/IEEE
//! matched either stored checksum, and this variant matched both). The crate's own
//! [`crate::checksum::crc32c_update`] is the standard, inverting form, so the raw one is
//! `!crc32c_update(!seed, bytes)` -- see [`stream_crc`].
//!
//! LAYOUT, in the order a reader meets it:
//!
//!   * a blob file opens with its HEADER BLOCK: `u32 proto_size, u32 crc` (little endian, a raw C
//!     struct) THEN the `BlobHeader` proto, zero-padded to a block boundary
//!     ([`header_block_size`]). Unlike the footer, the 8-byte struct comes FIRST here;
//!   * then data blocks of [`BLOCK_SIZE`] bytes. A block that has been filled ends in a
//!     [`FOOTER_SIZE`]-byte footer: zeros, the `BlockFooter` proto, then the 8-byte struct LAST;
//!   * a record is `varint32 length`, `little-endian u32 crc` of the payload, the payload. It may
//!     straddle blocks; `last_record_left_size` in the footer says how much of it is still to come;
//!   * a footer is written LAZILY, when the NEXT record does not fit -- so the last block of a blob
//!     that has not been sealed has none, and a reader must not look for one.
//!
//! WHAT THIS DOES NOT DO: it is not wired to anything. It reads and writes byte buffers. Nothing
//! on disk in this engine changes until a caller chooses it.

use prost::Message;

use crate::checksum::crc32c_update;

pub const BLOCK_SIZE: usize = 1 << 17;
pub const FOOTER_SIZE: usize = 1 << 7;
pub const MAGIC: u64 = 0xBCBC_BCBC_6666_6666;
pub const VERSION: u32 = 1;
const PROTO_HEADER_SIZE: usize = 8;

/// The stream's checksum: Castagnoli, initial value `seed`, no final inversion.
///
/// Chainable the way the writer chains it: `stream_crc(stream_crc(0, a), b)` is the checksum of
/// `a` followed by `b`.
pub fn stream_crc(seed: u32, bytes: &[u8]) -> u32 {
    !crc32c_update(!seed, bytes)
}

/// One blob's entry in a header's list of data blobs.
#[derive(Clone, PartialEq, Message)]
pub struct BlobInfo {
    #[prost(uint64, tag = "1")]
    pub blob_id: u64,
    #[prost(uint64, tag = "2")]
    pub start_offset: u64,
    #[prost(uint64, tag = "3")]
    pub blob_start_offset: u64,
    #[prost(uint64, tag = "4")]
    pub blob_end_offset: u64,
    #[prost(uint64, tag = "5")]
    pub end_record_sequence: u64,
    #[prost(uint64, tag = "6")]
    pub freeze_ms: u64,
    #[prost(uint64, tag = "7")]
    pub end_offset: u64,
    #[prost(uint64, tag = "8")]
    pub truncated_offset: u64,
}

/// The head of a blob file.
#[derive(Clone, PartialEq, Message)]
pub struct BlobHeader {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint64, tag = "2")]
    pub blob_id: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub blob_name: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub timestamp_ms: u64,
    #[prost(uint32, tag = "5")]
    pub version: u32,
    #[prost(uint64, tag = "6")]
    pub start_record_sequence: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub client_token: Vec<u8>,
    #[prost(message, repeated, tag = "8")]
    pub data_blobs: Vec<BlobInfo>,
    #[prost(uint64, tag = "9")]
    pub start_offset: u64,
    #[prost(uint32, tag = "10")]
    pub prev_block_crc32c: u32,
    #[prost(uint64, tag = "11")]
    pub truncated_offset: u64,
    /// The serialized length of this very message. A fixed-width field so that putting the length
    /// in does not change it; zero is not written, so a writer fills in a placeholder first.
    #[prost(fixed32, tag = "12")]
    pub header_size: u32,
    #[prost(message, repeated, tag = "13")]
    pub obsolete_blobs: Vec<BlobInfo>,
}

/// The tail of a filled block.
#[derive(Clone, PartialEq, Message)]
pub struct BlockFooter {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint32, tag = "2")]
    pub version: u32,
    #[prost(uint64, tag = "3")]
    pub timestamp_ms: u64,
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
    // Tag 11 is not used by the layout.
    #[prost(uint64, tag = "12")]
    pub truncated_offset: u64,
}

fn align_up(size: usize, unit: usize) -> usize {
    size.div_ceil(unit) * unit
}

/// How many bytes the header block takes, for a blob whose data begins at stream offset
/// `start_offset`.
pub fn header_block_size(proto_size: usize, start_offset: u64) -> usize {
    let start_in_block = (start_offset % BLOCK_SIZE as u64) as usize;
    let left = (BLOCK_SIZE - start_in_block) % BLOCK_SIZE;
    align_up(proto_size + PROTO_HEADER_SIZE + left, BLOCK_SIZE) - left
}

fn put_varint32(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn record_header(length: usize, crc: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    put_varint32(&mut out, length as u32);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// A footer slot: zeros, the proto, then `{size, crc}` last.
fn footer_bytes(footer: &BlockFooter) -> [u8; FOOTER_SIZE] {
    let proto = footer.encode_to_vec();
    assert!(proto.len() + PROTO_HEADER_SIZE <= FOOTER_SIZE, "a footer outgrew its slot");
    let mut slot = [0_u8; FOOTER_SIZE];
    let at = FOOTER_SIZE - PROTO_HEADER_SIZE - proto.len();
    slot[at..at + proto.len()].copy_from_slice(&proto);
    slot[FOOTER_SIZE - 8..FOOTER_SIZE - 4].copy_from_slice(&(proto.len() as u32).to_le_bytes());
    slot[FOOTER_SIZE - 4..].copy_from_slice(&stream_crc(0, &proto).to_le_bytes());
    slot
}

/// The header block for `header`, with `header_size` filled in.
pub fn header_block(mut header: BlobHeader) -> Vec<u8> {
    header.magic = MAGIC;
    header.version = VERSION;
    // A placeholder, so the field is written and the length is final.
    header.header_size = 1;
    let size = header.encode_to_vec().len();
    header.header_size = size as u32;
    let proto = header.encode_to_vec();
    debug_assert_eq!(proto.len(), size);
    let total = header_block_size(proto.len(), header.start_offset);
    let mut out = vec![0_u8; total];
    out[0..4].copy_from_slice(&(proto.len() as u32).to_le_bytes());
    out[4..8].copy_from_slice(&stream_crc(0, &proto).to_le_bytes());
    out[8..8 + proto.len()].copy_from_slice(&proto);
    out
}

/// What a footer is stamped with. Injected so a test can reproduce a file byte for byte.
///
/// `truncated_offset` is not a constant of the blob: the captured footers carry 130,302 and then
/// 260,725, because it is how far the stream had been truncated when the footer was written, and
/// the owner of the stream moves it as its log is collected.
pub struct FooterStamp {
    pub timestamp_ms: u64,
    pub truncated_offset: u64,
}

/// Writes one blob into memory, byte for byte as the design's writer lays it out.
///
/// Only a blob whose data begins on a block boundary is supported (`start_offset % BLOCK_SIZE ==
/// 0`), which is every first blob and every blob a writer rolls at a block boundary.
pub struct BlobWriter {
    out: Vec<u8>,
    /// Stream offset of the next byte, as the design counts it: from the start of the STREAM.
    length: u64,
    start_offset: u64,
    block_crc: u32,
    /// Records appended so far: the footer's `last_record_sequence`.
    sequence: u64,
    token: Vec<u8>,
    stamp: Box<dyn FnMut() -> FooterStamp>,
}

impl BlobWriter {
    pub fn new(
        header: BlobHeader,
        stamp: Box<dyn FnMut() -> FooterStamp>,
    ) -> Result<Self, String> {
        if header.start_offset % BLOCK_SIZE as u64 != 0 {
            return Err(format!(
                "a blob starting at stream offset {} is not on a block boundary",
                header.start_offset
            ));
        }
        let start_offset = header.start_offset;
        let token = header.client_token.clone();
        let block_crc = header.prev_block_crc32c;
        Ok(Self {
            out: header_block(header),
            length: start_offset,
            start_offset,
            block_crc,
            sequence: 0,
            token,
            stamp,
        })
    }

    /// The whole blob so far.
    pub fn bytes(&self) -> &[u8] {
        &self.out
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.out
    }

    fn push(&mut self, data: &[u8]) {
        self.out.extend_from_slice(data);
        self.length += data.len() as u64;
    }

    /// Append one record; returns its id (the stream offset its header starts at).
    pub fn append(&mut self, payload: &[u8]) -> u64 {
        let record_crc = stream_crc(0, payload);
        let header = record_header(payload.len(), record_crc);
        let block_offset = (self.length % BLOCK_SIZE as u64) as usize;
        let id;
        if header.len() + payload.len() + block_offset <= BLOCK_SIZE - FOOTER_SIZE {
            id = self.length;
            self.block_crc = stream_crc(self.block_crc, &header);
            self.block_crc = stream_crc(self.block_crc, payload);
            self.push(&header);
            self.push(payload);
        } else {
            id = self.append_across_blocks(&header, payload);
        }
        self.sequence += 1;
        id
    }

    fn append_across_blocks(&mut self, header: &[u8], payload: &[u8]) -> u64 {
        let pieces: [&[u8]; 2] = [header, payload];
        let size = header.len() + payload.len();
        let mut id = self.length;
        let mut block_offset = (self.length % BLOCK_SIZE as u64) as usize;
        // Data bytes this block holds: padding is not data, and a footer says where data ends.
        let mut block_end = block_offset;
        if block_offset + header.len() + FOOTER_SIZE > BLOCK_SIZE {
            let pad = vec![0_u8; BLOCK_SIZE - block_offset - FOOTER_SIZE];
            self.push(&pad);
            block_offset = BLOCK_SIZE - FOOTER_SIZE;
            id = align_up(self.length as usize, BLOCK_SIZE) as u64;
        }
        let mut piece_index = 0;
        let mut offset_in_piece = 0;
        let mut left_size = size;
        while left_size > 0 {
            if block_offset + FOOTER_SIZE == BLOCK_SIZE {
                let stamp = (self.stamp)();
                let footer = BlockFooter {
                    magic: MAGIC,
                    version: VERSION,
                    timestamp_ms: stamp.timestamp_ms,
                    block_crc: self.block_crc,
                    block_number: self.length / BLOCK_SIZE as u64,
                    block_end: block_end as u32,
                    last_record_offset: self.length,
                    last_record_left_size: if left_size < size { left_size as u32 } else { 0 },
                    last_record_sequence: self.sequence,
                    client_token: self.token.clone(),
                    truncated_offset: stamp.truncated_offset,
                };
                let slot = footer_bytes(&footer);
                self.push(&slot);
                debug_assert_eq!(self.length % BLOCK_SIZE as u64, 0);
                block_offset = 0;
                block_end = 0;
                self.block_crc = 0;
                continue;
            }
            let piece = pieces[piece_index];
            let take = (piece.len() - offset_in_piece).min(BLOCK_SIZE - FOOTER_SIZE - block_offset);
            let chunk = &piece[offset_in_piece..offset_in_piece + take];
            self.block_crc = stream_crc(self.block_crc, chunk);
            self.push(chunk);
            offset_in_piece += take;
            block_offset += take;
            block_end += take;
            left_size -= take;
            if offset_in_piece == piece.len() {
                piece_index += 1;
                offset_in_piece = 0;
            }
        }
        id
    }

    /// Where the blob's data begins, as a file offset.
    pub fn data_start(&self) -> usize {
        self.out.len() - (self.length - self.start_offset) as usize
    }
}

/// One record read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    /// The stream offset its header starts at.
    pub id: u64,
    pub payload: Vec<u8>,
}

/// What a blob holds.
#[derive(Debug, Clone, PartialEq)]
pub struct BlobContents {
    pub header: BlobHeader,
    pub records: Vec<StreamRecord>,
    /// Every footer found, in block order.
    pub footers: Vec<BlockFooter>,
}

fn read_varint32(bytes: &[u8], at: usize) -> Option<(u32, usize)> {
    let mut value = 0_u32;
    let mut shift = 0;
    let mut i = at;
    loop {
        let byte = *bytes.get(i)?;
        i += 1;
        value |= u32::from(byte & 0x7F).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some((value, i));
        }
        shift += 7;
        if shift > 28 {
            return None;
        }
    }
}

/// Read a whole blob, verifying every checksum it carries.
///
/// A record that fails its checksum, a footer that fails its own or its block's, a header that
/// disagrees with its stated size -- each is an error, not a skip: the design this follows treats
/// a bad checksum as corruption, and a reader that stepped over one would hand back a log with a
/// hole in it.
pub fn read_blob(bytes: &[u8]) -> Result<BlobContents, String> {
    if bytes.len() < PROTO_HEADER_SIZE {
        return Err("a blob shorter than its own header struct".to_string());
    }
    let proto_size = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let stored = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let proto = bytes
        .get(PROTO_HEADER_SIZE..PROTO_HEADER_SIZE + proto_size)
        .ok_or("the blob header runs past the end of the file")?;
    if stream_crc(0, proto) != stored {
        return Err("the blob header checksum does not match".to_string());
    }
    let header = BlobHeader::decode(proto).map_err(|error| error.to_string())?;
    if header.magic != MAGIC {
        return Err(format!("blob magic {:#x} is not {:#x}", header.magic, MAGIC));
    }
    if header.header_size as usize != proto_size {
        return Err(format!(
            "blob header says it is {} bytes and the struct says {proto_size}",
            header.header_size
        ));
    }
    if header.start_offset % BLOCK_SIZE as u64 != 0 {
        return Err(format!("blob data starting at {} is not on a block boundary", header.start_offset));
    }
    let data_start = header_block_size(proto_size, header.start_offset);
    let body = bytes.get(data_start..).ok_or("the blob has no data region")?;

    // Pass 1: strip the footers, checking each, into one logical stream; remember where each
    // block's logical bytes begin so a record's id can be turned back into a stream offset.
    let mut logical: Vec<u8> = Vec::with_capacity(body.len());
    let mut block_starts: Vec<(usize, u64)> = Vec::new();
    let mut footers = Vec::new();
    let full_blocks = body.len() / BLOCK_SIZE;
    let blocks = full_blocks + usize::from(body.len() % BLOCK_SIZE != 0);
    for block in 0..blocks {
        let slice = &body[block * BLOCK_SIZE..((block + 1) * BLOCK_SIZE).min(body.len())];
        block_starts.push((logical.len(), header.start_offset + (block * BLOCK_SIZE) as u64));
        if slice.len() < BLOCK_SIZE {
            // The tail: no footer yet.
            logical.extend_from_slice(slice);
            continue;
        }
        let proto_len =
            u32::from_le_bytes(slice[BLOCK_SIZE - 8..BLOCK_SIZE - 4].try_into().unwrap()) as usize;
        let proto_crc = u32::from_le_bytes(slice[BLOCK_SIZE - 4..].try_into().unwrap());
        if proto_len + PROTO_HEADER_SIZE > FOOTER_SIZE {
            return Err(format!("block {block}: footer claims {proto_len} bytes"));
        }
        let proto = &slice[BLOCK_SIZE - PROTO_HEADER_SIZE - proto_len..BLOCK_SIZE - PROTO_HEADER_SIZE];
        if stream_crc(0, proto) != proto_crc {
            return Err(format!("block {block}: the footer checksum does not match"));
        }
        let footer = BlockFooter::decode(proto).map_err(|error| error.to_string())?;
        if footer.magic != MAGIC {
            return Err(format!("block {block}: footer magic {:#x}", footer.magic));
        }
        let end = footer.block_end as usize;
        if end > BLOCK_SIZE - FOOTER_SIZE {
            return Err(format!("block {block}: block_end {end} is past the data area"));
        }
        if stream_crc(0, &slice[..end]) != footer.block_crc {
            return Err(format!("block {block}: the block checksum does not match its data"));
        }
        logical.extend_from_slice(&slice[..end]);
        footers.push(footer);
    }

    // Pass 2: records, across the joins.
    let mut records = Vec::new();
    let mut at = 0_usize;
    while at < logical.len() {
        let (length, after_length) = read_varint32(&logical, at)
            .ok_or_else(|| format!("a record header at logical offset {at} is truncated"))?;
        let crc_end = after_length + 4;
        let payload_end = crc_end + length as usize;
        if payload_end > logical.len() {
            // A record the tail has not finished: not an error in an unsealed blob.
            break;
        }
        let stored = u32::from_le_bytes(logical[after_length..crc_end].try_into().unwrap());
        let payload = &logical[crc_end..payload_end];
        if stream_crc(0, payload) != stored {
            return Err(format!("the record at logical offset {at} fails its checksum"));
        }
        let block_index = block_starts.partition_point(|(logical_start, _)| *logical_start <= at) - 1;
        let (logical_start, stream_start) = block_starts[block_index];
        records.push(StreamRecord {
            id: stream_start + (at - logical_start) as u64,
            payload: payload.to_vec(),
        });
        at = payload_end;
    }
    Ok(BlobContents { header, records, footers })
}

#[cfg(test)]
#[path = "stream_format_tests.rs"]
mod tests;
