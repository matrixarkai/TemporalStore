// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::File;
use std::io::{self, BufRead, Read, Seek, SeekFrom};

use super::{
    block_of, decode_block_footer, encode_partial_block_footer, WAL_BLOCK_BYTES,
    WAL_BLOCK_FOOTER_BYTES,
};

// The transport wraps an unchanged integrity-framed record. Its authenticated first-chunk
// length makes the layout independent of a piece's header, including after prefix reclaim.
pub(super) const MAGIC: u8 = 0xB4;
pub(super) const HEADER_BYTES: usize = 1 + 8 + 4 + 4;
const DATA_BYTES: usize = (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES) as usize;

pub(super) struct ReadRecord {
    pub bytes: Vec<u8>,
    pub physical_bytes: u64,
}

pub(super) fn pack(frame: &[u8], offset: u64, header_len: u64, sequence: u64) -> Vec<u8> {
    let block_offset = ((offset - header_len) % WAL_BLOCK_BYTES) as usize;
    let first = DATA_BYTES - block_offset - HEADER_BYTES;
    debug_assert!(first > 0 && first < frame.len());
    let slots = (frame.len() + HEADER_BYTES) / DATA_BYTES + 1;
    let mut out =
        Vec::with_capacity(frame.len() + HEADER_BYTES + slots * WAL_BLOCK_FOOTER_BYTES as usize);
    out.push(MAGIC);
    out.extend_from_slice(&(frame.len() as u64).to_le_bytes());
    out.extend_from_slice(&(first as u32).to_le_bytes());
    let crc = crate::checksum::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    let mut used = 0;
    let mut chunk = first;
    loop {
        out.extend_from_slice(&frame[used..used + chunk]);
        used += chunk;
        if used == frame.len() {
            return out;
        }
        let footer_at = offset + out.len() as u64;
        out.extend_from_slice(&encode_partial_block_footer(
            block_of(footer_at - header_len),
            out.len() as u64,
            (frame.len() - used) as u32,
            sequence.saturating_sub(1),
        ));
        chunk = DATA_BYTES.min(frame.len() - used);
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn decode_header(header: &[u8; HEADER_BYTES]) -> io::Result<(u64, usize)> {
    let crc = u32::from_le_bytes(header[13..17].try_into().unwrap());
    if crate::checksum::crc32c(&header[..13]) != crc {
        return Err(invalid("packed WAL frame header checksum mismatch"));
    }
    let length = u64::from_le_bytes(header[1..9].try_into().unwrap());
    let first = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
    if length > u64::from(u32::MAX)
        || first == 0
        || first > DATA_BYTES - HEADER_BYTES
        || first as u64 >= length
    {
        return Err(invalid("packed WAL frame has invalid chunk lengths"));
    }
    Ok((length, first))
}

fn validate_footer(slot: &[u8], remaining: u64, physical_bytes: u64) -> io::Result<()> {
    let footer = decode_block_footer(slot)
        .ok_or_else(|| invalid("packed WAL frame footer checksum mismatch"))?;
    if footer.version != 2
        || u64::from(footer.last_record_left_size) != remaining
        || footer.last_record_offset != physical_bytes
        || footer.block_end as usize != DATA_BYTES
    {
        return Err(invalid("packed WAL frame footer disagrees with its record"));
    }
    Ok(())
}

fn read_at_offset(file: &mut File, offset: u64, bytes: &mut [u8]) -> io::Result<usize> {
    loop {
        #[cfg(unix)]
        let result = {
            use std::os::unix::fs::FileExt;
            file.read_at(bytes, offset)
        };
        #[cfg(not(unix))]
        let result = file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| file.read(bytes));
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn read_exact_at_offset(
    file: &mut File,
    offset: u64,
    bytes: &mut [u8],
    physical_bytes: &mut u64,
) -> io::Result<()> {
    let mut used = 0;
    while used < bytes.len() {
        let position = offset
            .checked_add(used as u64)
            .ok_or_else(|| invalid("packed WAL point offset overflow"))?;
        let read = read_at_offset(file, position, &mut bytes[used..])?;
        if read == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        used += read;
        *physical_bytes += read as u64;
    }
    Ok(())
}

/// A seekable view of the inner integrity frame, without reading its unrelated payloads.
/// The first chunk length is authenticated by the transport header. All footer slots are
/// validated up front; subsequent seeks use that fixed layout, including after prefix reclaim.
pub(super) struct PointReader<'a> {
    file: &'a mut File,
    start: u64,
    position: u64,
    packed: Option<(u64, u64)>,
    pub physical_bytes: u64,
}

impl<'a> PointReader<'a> {
    pub(super) fn new(file: &'a mut File, start: u64) -> io::Result<Option<Self>> {
        let mut header = [0u8; HEADER_BYTES];
        let read = read_at_offset(file, start, &mut header)?;
        if read == 0 {
            return Ok(None);
        }
        let mut view = Self {
            file,
            start,
            position: 0,
            packed: None,
            physical_bytes: read as u64,
        };
        if header[0] != MAGIC {
            return Ok(Some(view));
        }
        if read < HEADER_BYTES {
            read_exact_at_offset(
                view.file,
                start
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("packed WAL point offset overflow"))?,
                &mut header[read..],
                &mut view.physical_bytes,
            )?;
        }
        let (length, first) = decode_header(&header)?;
        // Indexed writers are bounded to 16 MiB of protobuf plus their directory. Larger
        // historical transports keep the streaming path rather than doing an unbounded probe.
        if length > 32 * 1024 * 1024 {
            return Ok(None);
        }
        let first = first as u64;
        view.packed = Some((length, first));
        let mut logical = first;
        let mut physical = HEADER_BYTES as u64 + first;
        while logical < length {
            let mut footer = [0u8; WAL_BLOCK_FOOTER_BYTES as usize];
            read_exact_at_offset(
                view.file,
                start
                    .checked_add(physical)
                    .ok_or_else(|| invalid("packed WAL point offset overflow"))?,
                &mut footer,
                &mut view.physical_bytes,
            )?;
            validate_footer(&footer, length - logical, physical)?;
            let chunk = (DATA_BYTES as u64).min(length - logical);
            logical += chunk;
            physical += WAL_BLOCK_FOOTER_BYTES + chunk;
        }
        Ok(Some(view))
    }
}

impl Read for PointReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let (offset, available) = match self.packed {
            Some((length, first)) => {
                if self.position >= length {
                    return Ok(0);
                }
                if self.position < first {
                    (HEADER_BYTES as u64 + self.position, first - self.position)
                } else {
                    let after = self.position - first;
                    let block = after / DATA_BYTES as u64;
                    let within = after % DATA_BYTES as u64;
                    (
                        HEADER_BYTES as u64
                            + first
                            + WAL_BLOCK_FOOTER_BYTES
                            + block * (DATA_BYTES as u64 + WAL_BLOCK_FOOTER_BYTES)
                            + within,
                        (DATA_BYTES as u64 - within).min(length - self.position),
                    )
                }
            }
            None => (self.position, bytes.len() as u64),
        };
        let length = bytes.len().min(available as usize);
        let read = read_at_offset(
            self.file,
            self.start
                .checked_add(offset)
                .ok_or_else(|| invalid("packed WAL point offset overflow"))?,
            &mut bytes[..length],
        )?;
        self.position += read as u64;
        self.physical_bytes += read as u64;
        Ok(read)
    }
}

impl Seek for PointReader<'_> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = match position {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => {
                let length = match self.packed {
                    Some((length, _)) => length,
                    None => self.file.metadata()?.len().saturating_sub(self.start),
                };
                i128::from(length) + i128::from(offset)
            }
        };
        self.position =
            u64::try_from(position).map_err(|_| invalid("invalid packed WAL point seek"))?;
        Ok(self.position)
    }
}

fn read_complete(reader: &mut impl Read, bytes: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(bytes) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

fn only_zero_tail(reader: &mut impl BufRead) -> io::Result<bool> {
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok(true);
        }
        if bytes.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        let used = bytes.len();
        reader.consume(used);
    }
}

pub(super) fn read(reader: &mut impl BufRead) -> io::Result<Option<ReadRecord>> {
    read_inner(reader, true)
}

pub(super) fn unpack(reader: &mut impl BufRead) -> io::Result<Option<ReadRecord>> {
    read_inner(reader, false)
}

fn read_inner(reader: &mut impl BufRead, keep_transport: bool) -> io::Result<Option<ReadRecord>> {
    if reader.fill_buf()?.first() != Some(&MAGIC) {
        return crate::log_framing::read_raw_record(reader).map(|record| {
            record.map(|bytes| ReadRecord {
                physical_bytes: bytes.len() as u64,
                bytes,
            })
        });
    }
    let mut header = [0u8; HEADER_BYTES];
    if !read_complete(reader, &mut header)? {
        return Ok(None);
    }
    let (length, first) = decode_header(&header)?;
    let mut bytes =
        Vec::with_capacity((length as usize).min(crate::log_framing::MAX_RESERVED_RECORD_BYTES));
    if keep_transport {
        bytes.extend_from_slice(&header);
    }
    let mut physical_bytes = HEADER_BYTES as u64;
    let mut logical_bytes = 0;
    let mut chunk = first;
    loop {
        let got = reader.by_ref().take(chunk as u64).read_to_end(&mut bytes)?;
        if got != chunk {
            return Ok(None);
        }
        physical_bytes += got as u64;
        logical_bytes += got as u64;
        let remaining = length - logical_bytes;
        if remaining == 0 {
            return Ok(Some(ReadRecord {
                bytes,
                physical_bytes,
            }));
        }
        let mut slot = [0u8; WAL_BLOCK_FOOTER_BYTES as usize];
        if !read_complete(reader, &mut slot)? {
            return Ok(None);
        }
        if slot.iter().all(|byte| *byte == 0) {
            return if only_zero_tail(reader)? {
                Ok(None)
            } else {
                Err(invalid("packed WAL frame has an interior missing footer"))
            };
        }
        validate_footer(&slot, remaining, physical_bytes)?;
        physical_bytes += WAL_BLOCK_FOOTER_BYTES;
        if keep_transport {
            bytes.extend_from_slice(&slot);
        }
        chunk = DATA_BYTES.min(remaining as usize);
    }
}
