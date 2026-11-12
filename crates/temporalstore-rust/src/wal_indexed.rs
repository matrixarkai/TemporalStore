// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Independently compressed carried blocks in an otherwise unchanged protobuf stream.
//! The directory is a Zstd skippable frame. Existing streaming readers skip it and
//! concatenate the following frames into exactly the original record bytes.

use super::*;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

const SKIP_MAGIC: u32 = 0x184d2a5e;
const INDEX_MAGIC: &[u8; 4] = b"TSIX";
const GROUP_BYTES: usize = 4096;
const MAX_DIRECTORY_BYTES: usize = 512 * 1024;
const CACHE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, PartialEq, Message)]
struct Frame {
    #[prost(uint32, tag = "1")]
    encoded: u32,
    #[prost(uint32, tag = "2")]
    decoded: u32,
    #[prost(fixed32, tag = "3")]
    checksum: u32,
}

#[derive(Clone, PartialEq, Message)]
struct Block {
    #[prost(uint32, tag = "1")]
    frame: u32,
    #[prost(uint32, tag = "2")]
    start: u32,
    #[prost(uint32, tag = "3")]
    length: u32,
}

#[derive(Clone, PartialEq, Message)]
struct Directory {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    shard: u64,
    #[prost(uint64, tag = "3")]
    sequence: u64,
    #[prost(message, repeated, tag = "4")]
    frames: Vec<Frame>,
    #[prost(message, repeated, tag = "5")]
    blocks: Vec<Block>,
}

fn enabled() -> bool {
    !matches!(
        std::env::var("TS_WAL_INDEXED_BLOCKS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

pub(super) fn eligible(record: &WriteAheadLogRecord, length: usize) -> bool {
    record.staged_blocks.len() >= 8
        && length >= 64 * 1024
        && length <= MAX_REUSED_INFLATE_BYTES
        && enabled()
}

fn compress_frame(
    bytes: &[u8],
    output: &mut Vec<u8>,
    frames: &mut Vec<Frame>,
) -> Result<(), String> {
    RECORD_COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot =
                Some(zstd::bulk::Compressor::new(COMPRESSION_LEVEL).map_err(|e| e.to_string())?);
        }
        let start = output.len();
        let bound = zstd::zstd_safe::compress_bound(bytes.len());
        output.resize(start + bound, 0);
        let length = slot
            .as_mut()
            .unwrap()
            .compress_to_buffer(bytes, &mut output[start..])
            .map_err(|e| e.to_string())?;
        output.truncate(start + length);
        frames.push(Frame {
            encoded: length as u32,
            decoded: bytes.len() as u32,
            checksum: crate::checksum::crc32c(&output[start..]),
        });
        Ok(())
    })
}

pub(super) fn encode(
    record: &WriteAheadLogRecord,
    parts: &RecordParts<'_>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    ENCODE_SCRATCH.with(|cell| {
        let mut scratch = cell.borrow_mut();
        scratch.clear();
        let result = (|| {
            let mut directory = Directory {
                version: 1,
                shard: record.shard_id,
                sequence: record.sequence,
                frames: Vec::new(),
                blocks: Vec::with_capacity(record.staged_blocks.len()),
            };
            parts.put_header(record, &mut scratch)?;
            compress_frame(&scratch, out, &mut directory.frames)?;
            scratch.clear();
            for block in &record.staged_blocks {
                let body = staged_block_body_len(block, None);
                let encoded = crate::raft::wal_proto::len_delimited_len(6, body);
                if !scratch.is_empty() && scratch.len() + encoded > GROUP_BYTES {
                    compress_frame(&scratch, out, &mut directory.frames)?;
                    scratch.clear();
                }
                let start = scratch.len() + encoded - body;
                directory.blocks.push(Block {
                    frame: directory.frames.len() as u32,
                    start: start as u32,
                    length: body as u32,
                });
                crate::raft::wal_proto::put_staged_block(6, block, None, &mut scratch);
            }
            if !scratch.is_empty() {
                compress_frame(&scratch, out, &mut directory.frames)?;
            }
            let size = directory.encoded_len() + INDEX_MAGIC.len();
            if size > MAX_DIRECTORY_BYTES {
                // Unusual records retain the existing writer rather than growing an unbounded directory.
                out.clear();
                scratch.clear();
                parts.put(record, &mut scratch)?;
                if !compress_payload_into(&scratch, out) {
                    // This path must stay Zstd under the caller's compressed marker.
                    out.clear();
                    compress_frame(&scratch, out, &mut Vec::new())?;
                }
                return Ok(());
            }
            let prefix = size + 8;
            let length = out.len();
            out.resize(length + prefix, 0);
            out.copy_within(0..length, prefix);
            out[..4].copy_from_slice(&SKIP_MAGIC.to_le_bytes());
            out[4..8].copy_from_slice(&(size as u32).to_le_bytes());
            out[8..12].copy_from_slice(INDEX_MAGIC);
            let mut directory_bytes = &mut out[12..prefix];
            directory
                .encode(&mut directory_bytes)
                .map_err(|e| e.to_string())?;
            Ok(())
        })();
        scratch.clear();
        if scratch.capacity() > MAX_ENCODE_SCRATCH_BYTES {
            *scratch = Vec::new();
        }
        result
    })
}

fn parse(bytes: &[u8]) -> Option<Directory> {
    if bytes.len() < 12 || bytes[..4] != SKIP_MAGIC.to_le_bytes() || &bytes[8..12] != INDEX_MAGIC {
        return None;
    }
    let size = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    if !(4..=MAX_DIRECTORY_BYTES).contains(&size) {
        return None;
    }
    let directory = Directory::decode(bytes.get(12..8 + size)?).ok()?;
    (directory.version == 1).then_some(directory)
}

pub(super) fn inflated_len(bytes: &[u8]) -> Option<u64> {
    let directory = parse(bytes)?;
    directory
        .frames
        .iter()
        .try_fold(0u64, |sum, frame| sum.checked_add(u64::from(frame.decoded)))
}

#[derive(Debug)]
struct Validated {
    directory: Directory,
    offsets: Vec<u64>,
    weight: usize,
}

#[derive(Debug)]
struct Cached {
    shard: u64,
    log_id: u64,
    payload_len: u64,
    checksum: u32,
    value: Arc<Validated>,
}

/// Directory metadata only; payload buffers are never retained here.
#[derive(Debug, Default)]
pub(crate) struct Cache {
    entries: VecDeque<Cached>,
    bytes: usize,
}

impl Cache {
    fn get(
        &mut self,
        shard: u64,
        log_id: u64,
        payload_len: u64,
        checksum: u32,
    ) -> Option<Arc<Validated>> {
        let index = self.entries.iter().position(|entry| {
            entry.shard == shard
                && entry.log_id == log_id
                && entry.payload_len == payload_len
                && entry.checksum == checksum
        })?;
        let entry = self.entries.remove(index)?;
        let value = Arc::clone(&entry.value);
        self.entries.push_back(entry);
        Some(value)
    }

    fn insert(
        &mut self,
        shard: u64,
        log_id: u64,
        payload_len: u64,
        checksum: u32,
        value: Arc<Validated>,
    ) {
        if value.weight > CACHE_BYTES {
            return;
        }
        if self.entries.len() >= 4096 {
            if let Some(old) = self.entries.pop_front() {
                self.bytes -= old.value.weight;
            }
        }
        self.entries.reserve(1);
        while self.bytes + value.weight + self.entries.capacity() * std::mem::size_of::<Cached>()
            > CACHE_BYTES
        {
            let Some(old) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= old.value.weight;
        }
        if self.bytes + value.weight + self.entries.capacity() * std::mem::size_of::<Cached>()
            > CACHE_BYTES
        {
            return;
        }
        self.bytes += value.weight;
        self.entries.push_back(Cached {
            shard,
            log_id,
            payload_len,
            checksum,
            value,
        });
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_exact_at(
    file: &mut File,
    offset: u64,
    bytes: &mut [u8],
    read_bytes: &mut u64,
) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)?;
    *read_bytes += bytes.len() as u64;
    Ok(())
}

/// Returns None only for an older encoding. A recognized corrupt index is an error.
pub(crate) fn read(
    file: &mut File,
    physical: u64,
    cache: &mut Cache,
    shard: u64,
    log_id: u64,
    sequence: u64,
    index: usize,
    object_id: u64,
    element_name: Option<&str>,
    read_bytes: &mut u64,
) -> io::Result<Option<Vec<u8>>> {
    let mut prefix = [0u8; 32];
    file.seek(SeekFrom::Start(physical))?;
    let mut count = 0;
    while count < prefix.len() {
        let length = file.read(&mut prefix[count..])?;
        if length == 0 {
            break;
        }
        count += length;
    }
    *read_bytes += count as u64;
    if count == 0 || prefix[0] != crate::log_framing::FRAME_MAGIC_V3 {
        return Ok(None);
    }
    let mut cursor = &prefix[1..count];
    let payload_len = prost::encoding::decode_varint(&mut cursor)
        .map_err(|_| invalid("invalid indexed WAL length"))?;
    let header_len = count - cursor.len() + 4;
    if cursor.len() < 4 {
        return Err(invalid("short indexed WAL header"));
    }
    let checksum = u32::from_le_bytes(cursor[..4].try_into().unwrap());
    if count < header_len + 9
        || prefix[header_len] != COMPRESSED_RAW_PAYLOAD_MARKER
        || prefix[header_len + 1..header_len + 5] != SKIP_MAGIC.to_le_bytes()
    {
        return Ok(None);
    }
    let value = match cache.get(shard, log_id, payload_len, checksum) {
        Some(value) => value,
        None => {
            let size =
                u32::from_le_bytes(prefix[header_len + 5..header_len + 9].try_into().unwrap())
                    as usize;
            if !(4..=MAX_DIRECTORY_BYTES).contains(&size) || (size as u64 + 9) > payload_len {
                return Err(invalid("invalid indexed WAL directory size"));
            }
            let mut encoded = vec![0u8; size + 9];
            encoded[..9].copy_from_slice(&prefix[header_len..header_len + 9]);
            read_exact_at(
                file,
                physical + header_len as u64 + 9,
                &mut encoded[9..],
                read_bytes,
            )?;
            let Some(directory) = parse(&encoded[1..]) else {
                // An unknown application/version remains a normal Zstd stream for older readers.
                if &encoded[9..13] != INDEX_MAGIC {
                    return Ok(None);
                }
                let decoded = Directory::decode(&encoded[13..])
                    .map_err(|_| invalid("invalid indexed WAL directory"))?;
                if decoded.version != 1 {
                    return Ok(None);
                }
                return Err(invalid("invalid indexed WAL directory"));
            };
            if directory.shard != shard
                || directory.sequence != sequence
                || directory.frames.len() < 2
                || directory.blocks.len() < 8
            {
                return Err(invalid("indexed WAL identity mismatch"));
            }
            let mut offsets = Vec::with_capacity(directory.frames.len());
            let mut end = encoded.len() as u64;
            let mut inflated = 0u64;
            let mut combined = crate::checksum::crc32c(&encoded);
            for frame in &directory.frames {
                if frame.encoded == 0 {
                    return Err(invalid("empty indexed WAL frame"));
                }
                offsets.push(end);
                end = end
                    .checked_add(u64::from(frame.encoded))
                    .ok_or_else(|| invalid("indexed WAL length overflow"))?;
                inflated += u64::from(frame.decoded);
                combined = crc32c::crc32c_combine(combined, frame.checksum, frame.encoded as usize);
            }
            if end != payload_len
                || inflated > MAX_REUSED_INFLATE_BYTES as u64
                || combined != checksum
            {
                return Err(invalid("indexed WAL checksum or length mismatch"));
            }
            for block in &directory.blocks {
                let frame = directory
                    .frames
                    .get(block.frame as usize)
                    .ok_or_else(|| invalid("invalid indexed WAL frame number"))?;
                if block.frame == 0
                    || u64::from(block.start) + u64::from(block.length) > u64::from(frame.decoded)
                {
                    return Err(invalid("invalid indexed WAL block bounds"));
                }
            }
            let weight = std::mem::size_of::<Validated>()
                + 2 * std::mem::size_of::<usize>()
                + directory.frames.capacity() * std::mem::size_of::<Frame>()
                + directory.blocks.capacity() * std::mem::size_of::<Block>()
                + offsets.capacity() * std::mem::size_of::<u64>();
            let value = Arc::new(Validated {
                directory,
                offsets,
                weight,
            });
            cache.insert(shard, log_id, payload_len, checksum, Arc::clone(&value));
            value
        }
    };
    if value.directory.shard != shard || value.directory.sequence != sequence {
        return Err(invalid("cached indexed WAL identity mismatch"));
    }
    let block = value
        .directory
        .blocks
        .get(index)
        .ok_or_else(|| invalid("invalid indexed WAL block number"))?;
    let frame = &value.directory.frames[block.frame as usize];
    let mut compressed = vec![0u8; frame.encoded as usize];
    let offset = physical
        .checked_add(header_len as u64)
        .and_then(|p| p.checked_add(value.offsets[block.frame as usize]))
        .ok_or_else(|| invalid("indexed WAL offset overflow"))?;
    read_exact_at(file, offset, &mut compressed, read_bytes)?;
    if crate::checksum::crc32c(&compressed) != frame.checksum {
        return Err(invalid("indexed WAL data checksum mismatch"));
    }
    let inflated = decompress_payload(&compressed).map_err(|e| invalid(&e))?;
    if inflated.len() != frame.decoded as usize {
        return Err(invalid("indexed WAL decoded length mismatch"));
    }
    let start = block.start as usize;
    let decoded = v1::WalStagedBlock::decode(&inflated[start..start + block.length as usize])
        .map_err(|_| invalid("invalid indexed WAL block"))?;
    if decoded.object_id != Some(object_id)
        || (decoded.element_name.is_some() && decoded.element_name.as_deref() != element_name)
    {
        return Err(invalid("indexed WAL block identity mismatch"));
    }
    Ok(Some(decoded.block))
}

#[cfg(test)]
#[path = "wal_indexed_tests.rs"]
mod tests;
