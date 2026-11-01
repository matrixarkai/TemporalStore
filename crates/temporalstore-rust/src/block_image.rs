// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE BLOCK AS THE DESIGN BEING FOLLOWED STORES IT IN A DATA STREAM: A SIZE, A HEADER, THE BYTES.
//!
//! A block record is the payload of one stream record (see [`crate::stream_format`]):
//!
//!   `u16 header_size` (little endian, a raw C scalar) | `header_size` bytes of header proto | data
//!
//! where the data is `data_size` bytes, either the block itself or, when the header says `LZ4`, one
//! LZ4 BLOCK (the raw block format, no frame) that decompresses to exactly `raw_data_size` bytes.
//! The address of a block is its slab (their "zone") in the high 32 bits and the stream offset of
//! its record in the low 32; its stored size is the whole payload, `2 + header_size + data_size`.
//! A reader checks that last equality, because the index stores the size and the stream does not.
//!
//! The header message's field numbers and meanings are theirs; the names are this crate's.
//!
//! WRITING IS UNCOMPRESSED. The design's default is uncompressed (its compress flag is off) and this
//! engine compresses at a different layer; reading handles both, because a store written with the
//! flag on must still be readable. Nothing here is wired to a live path.

use prost::Message;

/// How a block's data is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockCompression {
    Uncompressed = 0,
    Lz4 = 1,
}

/// The header of one stored block.
#[derive(Clone, PartialEq, Message)]
pub struct BlockImageHeader {
    /// The routing bucket (their "slot"), 64 bits.
    #[prost(fixed64, tag = "1")]
    pub routing_bucket: u64,
    #[prost(uint32, tag = "2")]
    pub object_id: u32,
    #[prost(uint32, tag = "3")]
    pub block_id: u32,
    /// Bytes before compression.
    #[prost(uint64, tag = "4")]
    pub raw_data_size: u64,
    /// Bytes as stored.
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
    /// The block lives in the write-ahead log rather than a data stream. Never true in a data
    /// stream; the writer of the design refuses it there.
    #[prost(bool, tag = "10")]
    pub in_wal: bool,
    /// Blocks of one object are ordered by version.
    #[prost(uint64, tag = "11")]
    pub version: u64,
    /// The write-ahead log sequence this block was dumped at.
    #[prost(uint64, tag = "12")]
    pub wal_sequence: u64,
}

/// A block, with its data as the block's own bytes (decompressed if it was stored compressed).
#[derive(Clone, Debug, PartialEq)]
pub struct BlockImage {
    pub header: BlockImageHeader,
    pub data: Vec<u8>,
}

/// The address of a block: slab in the high half, stream offset in the low half.
pub fn block_address(slab_id: u32, stream_offset: u32) -> u64 {
    (u64::from(slab_id) << 32) | u64::from(stream_offset)
}

/// Encode a block uncompressed. `header.raw_data_size`, `data_size` and `compress` are set here,
/// not trusted from the caller. The result is the payload of one stream record.
pub fn encode_block_image(header: &BlockImageHeader, data: &[u8]) -> Result<Vec<u8>, String> {
    if header.in_wal {
        return Err("a block in the write-ahead log has no place in a data stream".to_string());
    }
    let mut header = header.clone();
    header.raw_data_size = data.len() as u64;
    header.data_size = data.len() as u64;
    header.compress = BlockCompression::Uncompressed as i32;
    let proto = header.encode_to_vec();
    let size = u16::try_from(proto.len())
        .map_err(|_| format!("a block header of {} bytes does not fit a u16 size", proto.len()))?;
    let mut out = Vec::with_capacity(2 + proto.len() + data.len());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&proto);
    out.extend_from_slice(data);
    Ok(out)
}

/// Decode one block record's payload, checking its stated sizes against what is there.
pub fn decode_block_image(payload: &[u8]) -> Result<BlockImage, String> {
    let size_bytes = payload.get(..2).ok_or("a block record shorter than its size prefix")?;
    let header_size = usize::from(u16::from_le_bytes([size_bytes[0], size_bytes[1]]));
    let proto = payload
        .get(2..2 + header_size)
        .ok_or("a block record shorter than its header")?;
    let header = BlockImageHeader::decode(proto).map_err(|error| error.to_string())?;
    let stored = &payload[2 + header_size..];
    if stored.len() as u64 != header.data_size {
        return Err(format!(
            "a block record holds {} data bytes and its header says {}",
            stored.len(),
            header.data_size
        ));
    }
    let data = match BlockCompression::try_from(header.compress) {
        Ok(BlockCompression::Uncompressed) => {
            if header.raw_data_size != header.data_size {
                return Err("an uncompressed block whose two sizes differ".to_string());
            }
            stored.to_vec()
        }
        Ok(BlockCompression::Lz4) => lz4_block_decompress(stored, header.raw_data_size as usize)?,
        Err(_) => return Err(format!("unknown block compression {}", header.compress)),
    };
    Ok(BlockImage { header, data })
}

/// Decompress one raw LZ4 block to exactly `raw_size` bytes.
///
/// The sequence loop of the format: a token (literal length in the high nibble, match length minus
/// four in the low), extended by 255-valued bytes; the literals; then, unless the input ends, a
/// little-endian two-byte offset back into the output and the match, which may overlap itself.
pub fn lz4_block_decompress(source: &[u8], raw_size: usize) -> Result<Vec<u8>, String> {
    let mut out: Vec<u8> = Vec::with_capacity(raw_size);
    let mut at = 0_usize;
    let mut extend = |at: &mut usize, mut length: usize| -> Result<usize, String> {
        loop {
            let byte = *source.get(*at).ok_or("lz4: a length runs past the input")?;
            *at += 1;
            length += usize::from(byte);
            if byte != 255 {
                return Ok(length);
            }
        }
    };
    while at < source.len() {
        let token = source[at];
        at += 1;
        let mut literals = usize::from(token >> 4);
        if literals == 15 {
            literals = extend(&mut at, literals)?;
        }
        let end = at.checked_add(literals).filter(|end| *end <= source.len());
        let end = end.ok_or("lz4: literals run past the input")?;
        out.extend_from_slice(&source[at..end]);
        at = end;
        if at >= source.len() {
            break; // the last sequence carries literals only
        }
        let offset_bytes = source.get(at..at + 2).ok_or("lz4: a match offset runs past the input")?;
        let offset = usize::from(u16::from_le_bytes([offset_bytes[0], offset_bytes[1]]));
        at += 2;
        if offset == 0 || offset > out.len() {
            return Err(format!("lz4: a match offset of {offset} with {} bytes out", out.len()));
        }
        let mut length = usize::from(token & 0x0F);
        if length == 15 {
            length = extend(&mut at, length)?;
        }
        length += 4;
        if out.len() + length > raw_size {
            return Err("lz4: the output outgrows its stated size".to_string());
        }
        // Byte by byte: a match is allowed to read what it is writing.
        let from = out.len() - offset;
        for index in 0..length {
            let byte = out[from + index];
            out.push(byte);
        }
    }
    if out.len() != raw_size {
        return Err(format!("lz4: {} bytes out where {raw_size} were stated", out.len()));
    }
    Ok(out)
}

#[cfg(test)]
#[path = "block_image_tests.rs"]
mod tests;
