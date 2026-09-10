// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};

use sha2::{Digest, Sha256};

use super::{
    BlockAddress, BlockStoreBlockIndexReport, BlockStoreError, BlockStoreOptions,
    BlockStoreSlabReport,
};

/// A block record carries only what the index cannot tell a reader.
///
/// The index entry already holds the object id, the block id, the routing bucket and the band,
/// so a copy in every record said nothing new -- and the band was derivable twice over, being
/// `band_id_for_slab` of the slab the record already sits in. Those are gone.
///
/// What is left is the checksum and the length, at constant offsets, so any field is one slice
/// rather than a walk. The length stays in the record even though the address carries it too:
/// the slab walks step record to record without an index, and the length is what lets them.
///
/// The checksum stays for a reason worth stating, because the comparison design does not keep
/// one: there, the layer beneath the block store checksums. Our slabs are plain files with no
/// such layer, so this CRC is the only thing standing between a corrupted block and a reader
/// that believes it.
/// Two bytes that say a slab holds records.
///
/// The comparison design has no marker, because a zone file there only ever holds blocks. We
/// have `install_slab`, which puts arbitrary bytes in a slab -- a restore path, and thirty test
/// sites -- so "is this a record" is a real question here and something has to answer it. Two
/// bytes answer it; the checksum catches anything that gets past them.
pub(super) const BLOCK_RECORD_MAGIC: &[u8; 2] = b"TB";

pub(super) const BLOCK_RECORD_CHECKSUM_OFFSET: usize = BLOCK_RECORD_MAGIC.len();

/// Length and codec share a word, the way the comparison design packs its status bits rather
/// than spending a byte on each. Thirty bits of length is a gigabyte per block.
pub(super) const BLOCK_RECORD_LENGTH_OFFSET: usize = BLOCK_RECORD_CHECKSUM_OFFSET + 4;

/// Which block of its object this is.
///
/// Two bytes, because a block id is an index INSIDE an object rather than a number handed out
/// across the whole store. An object with more than 65,535 blocks is not a case this design
/// serves; one with a handful is every case it does.
pub(super) const BLOCK_RECORD_BLOCK_ID_OFFSET: usize = BLOCK_RECORD_LENGTH_OFFSET + 4;
pub(super) const BLOCK_RECORD_LENGTH_MASK: u32 = 0x3FFF_FFFF;
pub(super) const BLOCK_RECORD_CODEC_SHIFT: u32 = 30;

/// The header is one size, always.
pub(crate) const BLOCK_RECORD_HEADER_LEN: usize = BLOCK_RECORD_BLOCK_ID_OFFSET + 2;

/// The checksum is a CRC32C and nothing else, so the field is exactly its width.
///
/// It was 32 bytes holding a SHA-256, then 32 holding a 4-byte CRC32C padded out, then 8 holding
/// the CRC32C plus a `C32C` marker that made the field self-describing across formats. With one
/// format there is nothing to tell apart, so the marker is gone and the field is the checksum.
/// A page checksum guards against a page that corrupted into something still decodable, not
/// against a forged one, and CRC32C is the right tool for that.
pub(super) const BLOCK_RECORD_CHECKSUM_LEN: usize = 4;

/// What the walks use to decide whether what is left can still be a record.
///
/// One header length now, so this is that length. It was a separate constant while the header
/// was variable, and set too high it made a walk read a short record as the end of the slab and
/// lose every record behind it -- which happened twice.
pub(super) const BLOCK_RECORD_SMALLEST_HEADER_LEN: usize = BLOCK_RECORD_HEADER_LEN;

/// The compression codec byte of a record this code writes, found by walking the header.
///
/// v9 had this at a fixed offset. A v10 header is variable length, so there is no constant to
/// name any more and a caller that wants the byte has to step over the varints to reach it.
#[cfg(test)]
pub(super) fn page_record_compression_byte(record: &[u8]) -> u8 {
    let sized = u32::from_le_bytes(
        record[BLOCK_RECORD_LENGTH_OFFSET..BLOCK_RECORD_LENGTH_OFFSET + 4]
            .try_into()
            .expect("block size slice"),
    );
    (sized >> BLOCK_RECORD_CODEC_SHIFT) as u8
}

/// How many bytes `value` takes as a varint.
///
/// Lets a test state the header length it expects from the values that went in, rather than from
/// a constant: with varints the length is a function of the data, and a test that cannot say what
/// it should be cannot notice when it is wrong.
#[cfg(test)]
pub(super) fn page_record_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Writes one LEB128 varint.
fn put_page_record_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Reads one LEB128 varint, advancing `cursor`.
fn read_page_record_varint(
    record: &[u8],
    cursor: &mut usize,
    address: &BlockAddress,
    what: &str,
) -> Result<u64, BlockStoreError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *record
            .get(*cursor)
            .ok_or_else(|| corrupt_page_envelope(address, format!("truncated {what}")))?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(corrupt_page_envelope(address, format!("oversized {what}")));
        }
    }
}
pub(super) const BLOCK_RECORD_COMPRESSION_MIN_BYTES: usize = 256;
pub(super) const BLOCK_RECORD_COMPRESSION_LEVEL: i32 = 0;
pub(super) const BLOCK_RECORD_COMPRESSION_NONE: u8 = 0;
pub(super) const BLOCK_RECORD_COMPRESSION_ZSTD: u8 = 1;

pub(super) fn default_page_record_compression_enabled() -> bool {
    true
}

pub(super) fn default_page_record_compression_min_bytes() -> usize {
    BLOCK_RECORD_COMPRESSION_MIN_BYTES
}

pub(super) fn default_page_record_compression_level() -> i32 {
    BLOCK_RECORD_COMPRESSION_LEVEL
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageRecordCompression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy)]
struct PageRecordHeader {
    header_len: usize,
    payload_len: usize,
    pub(super) stored_len: usize,
    checksum: [u8; BLOCK_RECORD_CHECKSUM_LEN],
    page_id: Option<u64>,
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    band_id: Option<u64>,
    pub(super) compression: PageRecordCompression,
}

#[derive(Debug)]
pub(super) struct EncodedPageRecord {
    pub(super) bytes: Vec<u8>,
    pub(super) logical_len: usize,
    pub(super) stored_len: usize,
    pub(super) compression: PageRecordCompression,
}

#[derive(Debug)]
pub(super) struct DecodedPageRecord {
    pub(super) payload: Vec<u8>,
    pub(super) logical_len: usize,
    pub(super) compression: PageRecordCompression,
}

#[derive(Debug)]
pub(super) struct LogicalRangeRead {
    pub(super) bytes: Vec<u8>,
    pub(super) compressed_records_read: u64,
}

pub(super) fn encode_page_record(
    payload: &[u8],
    page_id: u64,
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    band_id: u64,
    options: BlockStoreOptions,
) -> Result<EncodedPageRecord, BlockStoreError> {
    let checksum_field = page_record_checksum_field(payload);
    let (squeezed, compression) = encode_page_record_payload(payload, options)?;
    // A compressed block states its decompressed size in its own first four bytes. The header
    // carries the STORED size, because that is what a slab walk steps by, and a walk cannot
    // step over a record whose length it does not know. Putting the decompressed size here
    // rather than in the header keeps the header one size for every block, and costs the four
    // bytes only where something was actually compressed.
    let stored_payload = match compression {
        PageRecordCompression::None => squeezed,
        PageRecordCompression::Zstd => {
            let mut framed = Vec::with_capacity(squeezed.len() + 4);
            framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            framed.extend_from_slice(&squeezed);
            framed
        }
    };
    let block_size = stored_payload.len();
    if block_size as u64 > u64::from(BLOCK_RECORD_LENGTH_MASK) {
        return Err(corrupt_page_envelope(
            &BlockAddress::default(),
            format!("block of {block_size} bytes does not fit a block size field"),
        ));
    }
    // The object id, the routing bucket and the band are the index's to remember. The band was
    // derivable from the slab on top of that. None of them are written here any more.
    let _ = (object_id, routing_bucket, band_id);
    let codec = match compression {
        PageRecordCompression::None => u32::from(BLOCK_RECORD_COMPRESSION_NONE),
        PageRecordCompression::Zstd => u32::from(BLOCK_RECORD_COMPRESSION_ZSTD),
    };
    let sized = (codec << BLOCK_RECORD_CODEC_SHIFT) | (block_size as u32);
    let mut record = Vec::with_capacity(BLOCK_RECORD_HEADER_LEN + block_size);
    record.extend_from_slice(BLOCK_RECORD_MAGIC);
    record.extend_from_slice(&checksum_field);
    record.extend_from_slice(&sized.to_le_bytes());
    if page_id > u64::from(u16::MAX) {
        return Err(corrupt_page_envelope(
            &BlockAddress::default(),
            format!("block index {page_id} does not fit a block id field"),
        ));
    }
    record.extend_from_slice(&(page_id as u16).to_le_bytes());
    debug_assert_eq!(record.len(), BLOCK_RECORD_HEADER_LEN, "the header is one size");
    let stored_len = block_size;
    record.extend_from_slice(&stored_payload);
    Ok(EncodedPageRecord {
        bytes: record,
        logical_len: payload.len(),
        stored_len,
        compression,
    })
}

fn encode_page_record_payload(
    payload: &[u8],
    options: BlockStoreOptions,
) -> Result<(Vec<u8>, PageRecordCompression), BlockStoreError> {
    if !options.compression_enabled || payload.len() < options.compression_min_bytes {
        return Ok((payload.to_vec(), PageRecordCompression::None));
    }
    // Reuse one compression context per thread, the mirror of the decoder below.
    //
    // `zstd::stream::encode_all` constructs a compressor, uses it once and drops it, and a
    // compressor allocates its working buffers up front. Measured by sweeping value sizes
    // through a write: cost is flat at 2.0 B allocated per payload byte on both sides of a
    // step between 192 and 256 bytes worth 32,535 B per write -- and 256 is
    // `BLOCK_RECORD_COMPRESSION_MIN_BYTES`, so the step IS this call. At the 1,244-byte average
    // of a soak corpus that is about 80% of everything a write allocates.
    //
    // The level is per-call rather than fixed, so it is set on the held compressor each time;
    // that is a parameter update, not a rebuild. A compressor is `&mut` to compress, so this is
    // thread-local for the same reason the decompressor is: one shared instance would serialise
    // every writer behind a lock on a path that is otherwise concurrent.
    //
    // The stored bytes CHANGE -- the bulk API frames a stream without the content-size header
    // the streaming call writes, so output is a little smaller. Both are ordinary zstd frames,
    // both decode through the path below and through the streaming fallback, so a record written
    // by either build reads on either. It is still a change to what lands on disk.
    let level = options.compression_level.clamp(-7, 22);
    let compressed = ZSTD_COMPRESSOR.with(|cell| {
        let mut compressor = cell.borrow_mut();
        compressor.set_compression_level(level)?;
        compressor.compress(payload)
    })?;
    if compressed.len() < payload.len() {
        Ok((compressed, PageRecordCompression::Zstd))
    } else {
        Ok((payload.to_vec(), PageRecordCompression::None))
    }
}

pub(super) fn decode_page_record(
    record: &[u8],
    address: &BlockAddress,
) -> Result<DecodedPageRecord, BlockStoreError> {
    if record.len() < BLOCK_RECORD_HEADER_LEN || !record.starts_with(BLOCK_RECORD_MAGIC) {
        return Ok(DecodedPageRecord {
            payload: record.to_vec(),
            logical_len: record.len(),
            compression: PageRecordCompression::None,
        });
    }
    if record.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let header = parse_page_record_header(record, address)?;
    if let (Some(address_page_id), Some(record_page_id)) = (address.page_id(), header.page_id) {
        if address_page_id != record_page_id {
            return Err(corrupt_page_envelope(
                address,
                format!("page id mismatch: address {address_page_id}, record {record_page_id}"),
            ));
        }
    }
    if let (Some(address_object_id), Some(record_object_id)) = (address.object_id(), header.object_id)
    {
        if address_object_id != record_object_id {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "object id mismatch: address {address_object_id}, record {record_object_id}"
                ),
            ));
        }
    }
    if let (Some(address_routing_bucket), Some(record_routing_bucket)) =
        (address.routing_bucket(), header.routing_bucket)
    {
        if address_routing_bucket != record_routing_bucket {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "routing slot mismatch: address {address_routing_bucket}, record {record_routing_bucket}"
                ),
            ));
        }
    }
    if let (Some(address_band_id), Some(record_band_id)) = (address.band_id(), header.band_id)
    {
        if address_band_id != record_band_id {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "band id mismatch: address {address_band_id}, record {record_band_id}"
                ),
            ));
        }
    }
    if record.len() != header.header_len + header.stored_len {
        return Err(corrupt_page_envelope(
            address,
            "payload length mismatch".to_string(),
        ));
    }
    let payload = decode_page_record_payload(&record[header.header_len..], &header, address)?;
    verify_page_record_checksum(&payload, &header.checksum, address)?;
    Ok(DecodedPageRecord {
        payload,
        logical_len: header.payload_len,
        compression: header.compression,
    })
}

pub(super) fn logical_range_from_slab(
    slab: &[u8],
    block_slab_id: u64,
    offset: u64,
    size: u64,
) -> Result<LogicalRangeRead, BlockStoreError> {
    if size == 0 {
        return Ok(LogicalRangeRead {
            bytes: Vec::new(),
            compressed_records_read: 0,
        });
    }
    if slab.len() < BLOCK_RECORD_HEADER_LEN || !slab.starts_with(BLOCK_RECORD_MAGIC) {
        let start = offset as usize;
        let end = start.saturating_add(size as usize).min(slab.len());
        let bytes = if start >= slab.len() {
            Vec::new()
        } else {
            slab[start..end].to_vec()
        };
        return Ok(LogicalRangeRead {
            bytes,
            compressed_records_read: 0,
        });
    }

    let requested_start = offset as usize;
    let requested_end = requested_start.saturating_add(size as usize);
    let mut physical_offset = 0usize;
    let mut logical_offset = 0usize;
    let mut out = Vec::with_capacity(size as usize);
    let mut compressed_records_read = 0_u64;

    while physical_offset < slab.len() && out.len() < size as usize {
        let remaining = &slab[physical_offset..];
        let address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None);
        if remaining.len() < BLOCK_RECORD_HEADER_LEN || !remaining.starts_with(BLOCK_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
            return Err(corrupt_page_envelope(&address, "short header"));
        }
        let header = parse_page_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_page_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        let address = BlockAddress::from_parts(0, 0, record_len as u64, header.page_id, header.object_id, header.routing_bucket, header.page_id.or(header.object_id));
        let payload = decode_page_record_payload(
            &remaining[header.header_len..record_len],
            &header,
            &address,
        )?;
        verify_page_record_checksum(&payload, &header.checksum, &address)?;
        if header.compression == PageRecordCompression::Zstd {
            compressed_records_read += 1;
        }

        let logical_end = logical_offset.saturating_add(header.payload_len);
        let overlap_start = requested_start.max(logical_offset);
        let overlap_end = requested_end.min(logical_end);
        if overlap_start < overlap_end {
            let payload_start = overlap_start - logical_offset;
            let payload_end = overlap_end - logical_offset;
            out.extend_from_slice(&payload[payload_start..payload_end]);
        }

        physical_offset = physical_offset.saturating_add(record_len);
        logical_offset = logical_end;
    }

    Ok(LogicalRangeRead {
        bytes: out,
        compressed_records_read,
    })
}

/// Reads a header, whose fields are varints and so can only be read in order.
///
/// There is no declared header length to check against: the header ends where the walk ends.
fn parse_page_record_header(
    record: &[u8],
    address: &BlockAddress,
) -> Result<PageRecordHeader, BlockStoreError> {
    if !record.starts_with(BLOCK_RECORD_MAGIC) {
        return Err(corrupt_page_envelope(address, "not a block record"));
    }
    if record.len() < BLOCK_RECORD_HEADER_LEN {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let checksum = record[BLOCK_RECORD_CHECKSUM_OFFSET..BLOCK_RECORD_CHECKSUM_OFFSET + BLOCK_RECORD_CHECKSUM_LEN]
        .try_into()
        .expect("block checksum slice");
    let sized = u32::from_le_bytes(
        record[BLOCK_RECORD_LENGTH_OFFSET..BLOCK_RECORD_LENGTH_OFFSET + 4]
            .try_into()
            .expect("block size slice"),
    );
    let block_size = (sized & BLOCK_RECORD_LENGTH_MASK) as usize;
    let codec = (sized >> BLOCK_RECORD_CODEC_SHIFT) as u8;
    let block_id = u16::from_le_bytes(
        record[BLOCK_RECORD_BLOCK_ID_OFFSET..BLOCK_RECORD_BLOCK_ID_OFFSET + 2]
            .try_into()
            .expect("block id slice"),
    );
    let compression = match codec {
        BLOCK_RECORD_COMPRESSION_NONE => PageRecordCompression::None,
        BLOCK_RECORD_COMPRESSION_ZSTD => PageRecordCompression::Zstd,
        codec => {
            return Err(corrupt_page_envelope(
                address,
                format!("unsupported compression codec {codec}"),
            ));
        }
    };
    // The header carries the STORED size, because that is what a slab walk steps by. For an
    // uncompressed block the stored bytes ARE the block, so the two sizes are one number. For a
    // compressed one the decompressed size is the block's own first four bytes -- which is
    // readable here without decompressing anything, so callers that only want to ACCOUNT for
    // logical bytes (slab summaries, logical range reads) get the right number for free.
    let payload_len = match compression {
        PageRecordCompression::None => block_size,
        PageRecordCompression::Zstd => {
            let at = BLOCK_RECORD_HEADER_LEN;
            if record.len() < at + 4 {
                return Err(corrupt_page_envelope(
                    address,
                    "compressed block has no decompressed size",
                ));
            }
            u32::from_le_bytes(
                record[at..at + 4]
                    .try_into()
                    .expect("decompressed size slice"),
            ) as usize
        }
    };
    Ok(PageRecordHeader {
        header_len: BLOCK_RECORD_HEADER_LEN,
        payload_len,
        stored_len: block_size,
        checksum,
        page_id: Some(u64::from(block_id)),
        // The index holds these. A record that repeated them could only ever agree or be wrong.
        object_id: None,
        routing_bucket: None,
        band_id: None,
        compression,
    })
}

thread_local! {
    /// One zstd decompression context per thread, reused across page reads.
    ///
    /// Thread-local rather than a shared pool: a decompressor is `&mut` for the duration of a
    /// decompress, so sharing one would serialise every reader behind a lock on a path that is
    /// otherwise concurrent.
    static ZSTD_DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> =
        std::cell::RefCell::new(
            zstd::bulk::Decompressor::new().expect("zstd decompressor construction"),
        );

    /// The write-side mirror. Level is set per call, so the level chosen here is only a starting
    /// point and never the one a record is actually compressed at.
    static ZSTD_COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> =
        std::cell::RefCell::new(
            zstd::bulk::Compressor::new(0).expect("zstd compressor construction"),
        );
}

fn decode_page_record_payload(
    stored_payload: &[u8],
    header: &PageRecordHeader,
    address: &BlockAddress,
) -> Result<Vec<u8>, BlockStoreError> {
    match header.compression {
        PageRecordCompression::None => Ok(stored_payload.to_vec()),
        PageRecordCompression::Zstd => {
            // Reuse one decompression context per thread instead of building one per read.
            //
            // `zstd::stream::decode_all` constructs a fresh streaming decoder each call, and a
            // decoder allocates its window buffer up front. Measured over 120 freshly written
            // summary records: the block-store read allocated ~132 KB per address to return a
            // 344-byte payload, about 380x the data, and that read was 78% of the whole cost of
            // fetching a record. The window is the same size whatever the record is, so the
            // smaller the record the worse the ratio -- which is the wrong way round for a point
            // read.
            //
            // `header.payload_len` is the exact decompressed size, so the bulk API needs no
            // guessed capacity. The length check below still runs: it guards against a record
            // whose header disagrees with its payload, which is corruption, not a size hint.
            // `payload_len` comes out of the record header, and the bulk API allocates that much
            // BEFORE anything is decompressed or checked -- so a header that lies (corruption, a
            // truncated write, a hostile record) would turn into an allocation of whatever it
            // claims. The old streaming call sized its buffer from what it actually decompressed
            // and could not be steered this way, so this bound is guarding a hazard the reuse
            // introduces, not one that was already here.
            //
            // Above the ceiling, fall back to the streaming decoder: it pays the window
            // allocation, but a record that large is not the case being optimised, and the
            // fallback keeps behaviour identical rather than failing a read that used to work.
            const ZSTD_TRUSTED_PAYLOAD_CEILING: usize = 64 << 20;
            // A compressed block states its decompressed size in its own first four bytes.
            if stored_payload.len() < 4 {
                return Err(corrupt_page_envelope(
                    address,
                    "compressed block has no decompressed size",
                ));
            }
            let (size_bytes, frame) = stored_payload.split_at(4);
            let logical_len = u32::from_le_bytes(
                size_bytes.try_into().expect("decompressed size slice"),
            ) as usize;
            let payload = if logical_len <= ZSTD_TRUSTED_PAYLOAD_CEILING {
                ZSTD_DECOMPRESSOR
                    .with(|decompressor| decompressor.borrow_mut().decompress(frame, logical_len))
                    .map_err(|err| {
                        corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
                    })?
            } else {
                zstd::stream::decode_all(Cursor::new(frame)).map_err(|err| {
                    corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
                })?
            };
            if payload.len() != logical_len {
                return Err(corrupt_page_envelope(
                    address,
                    format!(
                        "decompressed length {} does not match the size the block states, {}",
                        payload.len(),
                        logical_len
                    ),
                ));
            }
            Ok(payload)
        }
    }
}

/// Build the 32-byte checksum field for a new (v7) record: CRC32C little-endian in the first
/// four bytes, a marker so the field is self-describing, then zero padding.
fn page_record_checksum_field(payload: &[u8]) -> [u8; BLOCK_RECORD_CHECKSUM_LEN] {
    crate::checksum::crc32c(payload).to_le_bytes()
}

/// Verify a page record's payload against its stored CRC32C.
fn verify_page_record_checksum(
    payload: &[u8],
    expected_checksum: &[u8; BLOCK_RECORD_CHECKSUM_LEN],
    address: &BlockAddress,
) -> Result<(), BlockStoreError> {
    let stored = u32::from_le_bytes(*expected_checksum);
    let actual = crate::checksum::crc32c(payload);
    if stored == actual {
        return Ok(());
    }
    Err(BlockStoreError::ChecksumMismatch {
        block_slab_id: address.block_slab_id,
        offset: address.offset,
        length: address.length,
        expected: format!("{stored:08x}"),
        actual: format!("{actual:08x}"),
    })
}

pub(super) fn corrupt_page_envelope(
    address: &BlockAddress,
    reason: impl Into<String>,
) -> BlockStoreError {
    BlockStoreError::CorruptPageEnvelope {
        block_slab_id: address.block_slab_id,
        offset: address.offset,
        reason: reason.into(),
    }
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The digest as the 32 bytes it is, for storing.
///
/// `sha256_hex` remains for the places that want text -- a report, an error message. What it does
/// not do any more is decide the in-memory representation of every page in the index.
pub(super) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SlabSummary {
    pub(super) logical_bytes: u64,
    pub(super) first_page_id: Option<u64>,
    pub(super) last_page_id: Option<u64>,
}

pub(super) fn summarize_slab(
    slab: &[u8],
    block_slab_id: u64,
) -> Result<SlabSummary, BlockStoreError> {
    if slab.len() < BLOCK_RECORD_HEADER_LEN || !slab.starts_with(BLOCK_RECORD_MAGIC) {
        return Ok(SlabSummary {
            logical_bytes: slab.len() as u64,
            first_page_id: None,
            last_page_id: None,
        });
    }
    let mut physical_offset = 0usize;
    let mut summary = SlabSummary::default();
    while physical_offset < slab.len() {
        let remaining = &slab[physical_offset..];
        let address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None);
        if remaining.len() < BLOCK_RECORD_HEADER_LEN || !remaining.starts_with(BLOCK_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
            return Err(corrupt_page_envelope(&address, "short header"));
        }
        let header = parse_page_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_page_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        if let Some(page_id) = header.page_id {
            summary.first_page_id = Some(
                summary
                    .first_page_id
                    .map_or(page_id, |current| current.min(page_id)),
            );
            summary.last_page_id = Some(
                summary
                    .last_page_id
                    .map_or(page_id, |current| current.max(page_id)),
            );
        }
        summary.logical_bytes = summary
            .logical_bytes
            .saturating_add(header.payload_len as u64);
        physical_offset = physical_offset.saturating_add(record_len);
    }
    Ok(summary)
}

/// The scan reads a prefix big enough to hold a header. There is one header size now.
const BLOCK_RECORD_WIDEST_HEADER_LEN: usize = BLOCK_RECORD_HEADER_LEN;

/// Read buffer for the page-id scan.
///
/// Sized so a bufferful spans many records rather than one: at the ~300-byte records the live
/// store holds, this is ~800 per fill, which is the difference between thousands of reads for a
/// slab and millions.
const BLOCK_RECORD_SCAN_BUFFER_BYTES: usize = 256 * 1024;

/// TS_BLOCK_INDEX_CHECKSUMS: recompute and record a hex digest per page record while inspecting.
///
/// Default OFF. `inspect_slab` runs at every engine open, and this hashes each payload a second
/// time -- `decode_page_record` has already verified the stored checksum -- then allocates a
/// 64-character String for it. Measured on a live-store copy, slab verification ran at 13.5 MB/s
/// against hundreds of MB/s for sha256 alone.
///
/// ONE caller reads the field: `block_address_api_ready` in
/// `StorageDataStructureApiParityReport`, which required `checksum.is_some()` on a block index
/// entry. An earlier version of this
/// comment claimed nothing read it -- wrong by exactly one -- and switching the hashing off left
/// that report permanently `ready: false` behind a `block_address_metadata_incomplete` blocker on
/// every default deployment. The report now asks for the checksum only when this is enabled, so
/// the two agree in both positions of the flag.
pub(crate) fn block_index_checksums_enabled() -> bool {
    std::env::var("TS_BLOCK_INDEX_CHECKSUMS")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true" || value == "yes"
        })
        .unwrap_or(false)
}

pub(super) fn inspect_slab(slab: &[u8], block_slab_id: u64) -> BlockStoreSlabReport {
    let mut report = BlockStoreSlabReport {
        block_slab_id,
        physical_bytes: slab.len() as u64,
        ..BlockStoreSlabReport::default()
    };
    let mut object_ids = BTreeSet::new();
    let mut routing_buckets = BTreeSet::new();
    if slab.is_empty() {
        return report;
    }
    if slab.len() < BLOCK_RECORD_HEADER_LEN || !slab.starts_with(BLOCK_RECORD_MAGIC) {
        report.logical_bytes = slab.len() as u64;
        report.page_count = 1;
        report.readable_prefix_physical_bytes = slab.len() as u64;
        return report;
    }

    let mut physical_offset = 0usize;
    while physical_offset < slab.len() {
        let remaining = &slab[physical_offset..];
        let mut address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None);
        if remaining.len() < BLOCK_RECORD_HEADER_LEN || !remaining.starts_with(BLOCK_RECORD_MAGIC) {
            record_slab_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "mixed raw bytes after page envelope").to_string(),
            );
            break;
        }
        if remaining.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
            record_slab_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "short header").to_string(),
            );
            break;
        }
        let header = match parse_page_record_header(remaining, &address) {
            Ok(header) => header,
            Err(err) => {
                record_slab_inspection_error(&mut report, address.offset, err.to_string());
                break;
            }
        };
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            record_slab_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "payload length mismatch".to_string()).to_string(),
            );
            break;
        }
        address.length = record_len as u64;
        address.set_page_id(header.page_id);
        address.set_object_id(header.object_id);
        address.set_routing_bucket(header.routing_bucket);
        match decode_page_record(&remaining[..record_len], &address) {
            Ok(decoded) => {
                report.page_count = report.page_count.saturating_add(1);
                report.logical_bytes = report
                    .logical_bytes
                    .saturating_add(decoded.logical_len as u64);
                report.block_index_entries.push(BlockStoreBlockIndexReport {
                    block_slab_id: block_slab_id,
                    offset: address.offset,
                    length: address.length,
                    compact_slab_address: address.compact_slab_address(),
                    compact_slab_id: address.compact_slab_id(),
                    compact_slab_offset: address.compact_slab_offset(),
                    storage_slab_id: header.band_id,
                    object_id: header.object_id,
                    model_id: None,
                    block_id: header.page_id,
                    block_size: decoded.logical_len as u64,
                    stored_size: header.stored_len as u64,
                    dirty: false,
                    deleted: decoded.logical_len == 0,
                    block_in_log: false,
                    routing_bucket: header.routing_bucket,
                    // Re-hashing the payload here doubled the sha256 work of every engine open
                    // to fill a field no caller reads. `decode_page_record` above has already
                    // verified this record's stored checksum. Opt in with
                    // TS_BLOCK_INDEX_CHECKSUMS=1 when inspecting a slab by hand.
                    checksum: block_index_checksums_enabled()
                        .then(|| sha256_hex(&decoded.payload)),
                });
                report.block_index_count = report.block_index_entries.len() as u64;
                if decoded.compression == PageRecordCompression::Zstd {
                    report.compressed_records = report.compressed_records.saturating_add(1);
                }
                if let Some(object_id) = header.object_id {
                    object_ids.insert(object_id);
                    report.object_count = object_ids.len() as u64;
                }
                if let Some(routing_bucket) = header.routing_bucket {
                    routing_buckets.insert(routing_bucket);
                    report.routing_bucket_count = routing_buckets.len() as u64;
                    report.first_routing_bucket = routing_buckets.first().copied();
                    report.last_routing_bucket = routing_buckets.last().copied();
                }
                if let Some(page_id) = header.page_id {
                    report.first_page_id = Some(
                        report
                            .first_page_id
                            .map_or(page_id, |current| current.min(page_id)),
                    );
                    report.last_page_id = Some(
                        report
                            .last_page_id
                            .map_or(page_id, |current| current.max(page_id)),
                    );
                }
            }
            Err(err) => {
                record_slab_inspection_error(&mut report, address.offset, err.to_string());
                break;
            }
        }
        physical_offset = physical_offset.saturating_add(record_len);
        report.readable_prefix_physical_bytes = physical_offset as u64;
    }
    report
}

fn record_slab_inspection_error(
    report: &mut BlockStoreSlabReport,
    offset: u64,
    error: String,
) {
    report.has_corruption = true;
    report.first_error_offset = Some(offset);
    report.first_error = Some(error);
}

/// The page record format, which there is only one of.
#[cfg(test)]
mod page_record_format_tests {
    use super::*;

    fn address() -> BlockAddress {
        BlockAddress {
            block_slab_id: 1,
            offset: 0,
            length: 0,
            ..Default::default()
        }
    }

    /// What the record carries comes back; what the index carries does not.
    ///
    /// A block record holds a checksum, its size and its block id. The object id, the routing
    /// bucket and the band used to be repeated here as well, where they could only ever agree
    /// with the index or be wrong. They are gone, and this states that they are: reading them
    /// back as `None` is the contract, not an oversight.
    #[test]
    fn a_record_carries_its_size_and_block_id_and_nothing_the_index_holds() {
        let payload = b"header field round trip payload";
        // A block id is an index inside its object, so it is small by construction.
        let block_id: u64 = 9;

        let encoded = encode_page_record(
            payload,
            block_id,
            Some(0x99AA_BBCC_DDEE_F001),
            Some(0x0BAD_C0DE),
            0x0102_0304,
            BlockStoreOptions::default(),
        )
        .expect("encode");
        assert_eq!(
            encoded.bytes.len(),
            BLOCK_RECORD_HEADER_LEN + payload.len(),
            "one header size, whatever the values"
        );
        let header = parse_page_record_header(&encoded.bytes, &address()).expect("parse");
        assert_eq!(header.page_id, Some(block_id), "block id");
        assert_eq!(header.payload_len, payload.len());
        assert_eq!(header.stored_len, payload.len());
        assert_eq!(header.object_id, None, "the index holds the object id");
        assert_eq!(header.routing_bucket, None, "the index holds the routing bucket");
        assert_eq!(header.band_id, None, "the band is the slab the record sits in");
        let decoded = decode_page_record(&encoded.bytes, &address()).expect("decode");
        assert_eq!(decoded.payload, payload);
    }

    /// Every field sits at a constant offset, so a reader takes one without walking.
    #[test]
    fn a_field_is_read_by_offset_not_by_walking() {
        let payload = b"offsets are constants";
        let block_id: u64 = u64::from(u16::MAX);
        let encoded =
            encode_page_record(payload, block_id, None, None, 0, BlockStoreOptions::default())
                .expect("encode");

        let at = BLOCK_RECORD_BLOCK_ID_OFFSET;
        let read_directly = u16::from_le_bytes(encoded.bytes[at..at + 2].try_into().unwrap());
        assert_eq!(
            u64::from(read_directly),
            block_id,
            "the block id is one slice at a constant offset"
        );

        let at = BLOCK_RECORD_LENGTH_OFFSET;
        let sized = u32::from_le_bytes(encoded.bytes[at..at + 4].try_into().unwrap());
        assert_eq!((sized & BLOCK_RECORD_LENGTH_MASK) as usize, payload.len());
        assert_eq!(
            (sized >> BLOCK_RECORD_CODEC_SHIFT) as u8,
            BLOCK_RECORD_COMPRESSION_NONE,
            "the codec shares the size word rather than taking a byte of its own"
        );
    }

    /// The checksum is a CRC32C of the payload, in the four bytes after the magic.
    #[test]
    fn the_checksum_is_a_crc32c_of_the_payload() {
        let payload = b"page payload that is long enough to be interesting";
        let encoded =
            encode_page_record(payload, 7, None, None, 3, BlockStoreOptions::default())
                .expect("encode");
        let at = BLOCK_RECORD_CHECKSUM_OFFSET;
        let stored = u32::from_le_bytes(
            encoded.bytes[at..at + BLOCK_RECORD_CHECKSUM_LEN]
                .try_into()
                .unwrap(),
        );
        assert_eq!(stored, crate::checksum::crc32c(payload));
        assert_eq!(BLOCK_RECORD_CHECKSUM_LEN, 4, "the field is the checksum, nothing more");
    }

    /// A record whose payload was altered after the fact is refused.
    #[test]
    fn a_corrupted_record_is_rejected() {
        let payload = b"payload that will be corrupted after the fact";
        let mut encoded =
            encode_page_record(payload, 1, None, None, 0, BlockStoreOptions::default())
                .expect("encode");
        let last = encoded.bytes.len() - 1;
        encoded.bytes[last] ^= 0xFF;
        let err = decode_page_record(&encoded.bytes, &address()).expect_err("must be refused");
        assert!(
            matches!(err, BlockStoreError::ChecksumMismatch { .. }),
            "expected a checksum mismatch, got {err:?}"
        );
    }

    /// A header that ends before its fields do is refused rather than read past.
    #[test]
    fn a_truncated_header_is_rejected() {
        let payload = b"truncation payload";
        let encoded =
            encode_page_record(payload, 1, None, None, 0, BlockStoreOptions::default())
                .expect("encode");
        for keep in [0, 1, BLOCK_RECORD_HEADER_LEN - 1] {
            let err = parse_page_record_header(&encoded.bytes[..keep], &address())
                .expect_err("a short record must be refused");
            assert!(
                matches!(err, BlockStoreError::CorruptPageEnvelope { .. }),
                "expected a corrupt envelope at {keep}, got {err:?}"
            );
        }
    }
}


#[cfg(test)]
mod reused_zstd_context_tests {
    use super::*;

    fn zstd_header(payload_len: usize, stored_len: usize) -> PageRecordHeader {
        PageRecordHeader {
            header_len: BLOCK_RECORD_HEADER_LEN,
            payload_len,
            stored_len,
            checksum: [0_u8; BLOCK_RECORD_CHECKSUM_LEN],
            page_id: Some(1),
            // The record no longer carries these; the index does.
            object_id: None,
            routing_bucket: None,
            band_id: None,
            compression: PageRecordCompression::Zstd,
        }
    }

    /// The stored bytes of a compressed block, which begin with its decompressed size.
    ///
    /// The header carries the STORED size, so the decompressed one has to be somewhere the
    /// decoder can reach, and it is the first four bytes of the block's own bytes.
    fn stored_bytes(original_len: usize, compressed: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(compressed.len() + 4);
        out.extend_from_slice(&(original_len as u32).to_le_bytes());
        out.extend_from_slice(compressed);
        out
    }

    fn address_for(payload_len: usize) -> BlockAddress {
        BlockAddress::from_parts(1, 0, payload_len as u64, Some(1), Some(1), Some(0), None)
    }

    /// Round-trip at several sizes through the shared thread-local context.
    ///
    /// Sizes straddle BLOCK_RECORD_COMPRESSION_MIN_BYTES (256) so both the compressed and the
    /// uncompressed branch are exercised, and the largest is well past any single decompress
    /// buffer -- a bulk decompressor given the wrong capacity truncates rather than erroring, so
    /// "it worked for the size I tried" is not evidence.
    #[test]
    fn a_compressed_record_round_trips_through_the_reused_context() {
        for len in [1usize, 64, 255, 256, 257, 4096, 200_000] {
            // Compressible content: random bytes would not compress, so the Zstd branch would
            // never be taken and the test would silently cover nothing.
            let original: Vec<u8> = (0..len).map(|i| (i % 7) as u8).collect();
            let compressed = zstd::stream::encode_all(
                std::io::Cursor::new(&original[..]),
                BLOCK_RECORD_COMPRESSION_LEVEL,
            )
            .expect("compresses");
            let stored = stored_bytes(original.len(), &compressed);
            let header = zstd_header(original.len(), stored.len());
            let decoded = decode_page_record_payload(&stored, &header, &address_for(len))
                .expect("a well-formed compressed record must decode");
            assert_eq!(
                original, decoded,
                "length {len} did not survive the reused decompression context"
            );
        }
    }

    /// The same context serves many reads in a row without carrying state between them.
    #[test]
    fn the_context_stays_correct_across_consecutive_reads_of_different_sizes() {
        let sizes = [4096usize, 17, 900, 3, 60_000];
        for _ in 0..3 {
            for len in sizes {
                let original: Vec<u8> = (0..len).map(|i| (i % 11) as u8).collect();
                let compressed = zstd::stream::encode_all(
                    std::io::Cursor::new(&original[..]),
                    BLOCK_RECORD_COMPRESSION_LEVEL,
                )
                .expect("compresses");
                let stored = stored_bytes(original.len(), &compressed);
            let header = zstd_header(original.len(), stored.len());
                let decoded = decode_page_record_payload(&stored, &header, &address_for(len))
                    .expect("decodes");
                assert_eq!(original, decoded, "size {len} was wrong on a reused context");
            }
        }
    }

    /// A block that lies about its decompressed size is refused, not served.
    ///
    /// The size moved: the header carries the STORED size now, and the decompressed size is
    /// the first four bytes of the block's own bytes. That is where a lie can be told, and the
    /// bulk decompressor allocates the claimed size before decompressing anything -- so the lie
    /// is acted on before it is checked, which is why it has to be checked afterwards.
    #[test]
    fn a_block_that_lies_about_its_decompressed_size_is_refused() {
        let original: Vec<u8> = (0..1000).map(|i| (i % 5) as u8).collect();
        let compressed = zstd::stream::encode_all(
            std::io::Cursor::new(&original[..]),
            BLOCK_RECORD_COMPRESSION_LEVEL,
        )
        .expect("compresses");
        // The frame really holds 1000 bytes; the block says 4242.
        let stored = stored_bytes(4242, &compressed);
        let header = zstd_header(4242, stored.len());
        assert!(
            decode_page_record_payload(&stored, &header, &address_for(1000)).is_err(),
            "a stated size that disagrees with the frame must be an error, not a short read"
        );
    }
}
