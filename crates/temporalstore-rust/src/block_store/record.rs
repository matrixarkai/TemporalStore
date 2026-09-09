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

pub(super) const PAGE_RECORD_MAGIC: &[u8; 8] = b"TSPAGE01";

/// The checksum is a CRC32C and nothing else, so the field is exactly its width.
///
/// It was 32 bytes holding a SHA-256, then 32 holding a 4-byte CRC32C padded out, then 8 holding
/// the CRC32C plus a `C32C` marker that made the field self-describing across formats. With one
/// format there is nothing to tell apart, so the marker is gone and the field is the checksum.
/// A page checksum guards against a page that corrupted into something still decodable, not
/// against a forged one, and CRC32C is the right tool for that.
pub(super) const PAGE_RECORD_CHECKSUM_LEN: usize = 4;

/// Where the checksum sits. Named because tests read the field by offset, and a literal there
/// passes silently when the layout moves under it.
pub(super) const PAGE_RECORD_CHECKSUM_OFFSET: usize = PAGE_RECORD_MAGIC.len();

/// Bytes before the varints: the magic, the checksum, then the object id.
///
/// There is one page record format, so no byte says which format this is and nothing here is
/// version numbered. The magic already ends in a number; a later format changes the magic.
pub(super) const PAGE_RECORD_FIXED_LEN: usize =
    PAGE_RECORD_MAGIC.len() + PAGE_RECORD_CHECKSUM_LEN + 8;

/// The shortest header: every varint one byte, plus the compression codec.
///
/// The slab walks use this to decide whether what is left can still be a record. Set too high, a
/// walk reads a short record as the end of the slab and loses every record behind it -- which has
/// happened twice, each time a new header came out shorter than the constant said was possible.
pub(super) const PAGE_RECORD_SMALLEST_HEADER_LEN: usize = PAGE_RECORD_FIXED_LEN + 4 + 1;

/// The compression codec byte of a record this code writes, found by walking the header.
///
/// v9 had this at a fixed offset. A v10 header is variable length, so there is no constant to
/// name any more and a caller that wants the byte has to step over the varints to reach it.
#[cfg(test)]
pub(super) fn page_record_compression_byte(record: &[u8]) -> u8 {
    let mut cursor = PAGE_RECORD_FIXED_LEN;
    for _ in 0..4 {
        while record[cursor] & 0x80 != 0 {
            cursor += 1;
        }
        cursor += 1;
    }
    record[cursor]
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
pub(super) const PAGE_RECORD_COMPRESSION_MIN_BYTES: usize = 256;
pub(super) const PAGE_RECORD_COMPRESSION_LEVEL: i32 = 0;
pub(super) const PAGE_RECORD_COMPRESSION_NONE: u8 = 0;
pub(super) const PAGE_RECORD_COMPRESSION_ZSTD: u8 = 1;

pub(super) fn default_page_record_compression_enabled() -> bool {
    true
}

pub(super) fn default_page_record_compression_min_bytes() -> usize {
    PAGE_RECORD_COMPRESSION_MIN_BYTES
}

pub(super) fn default_page_record_compression_level() -> i32 {
    PAGE_RECORD_COMPRESSION_LEVEL
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
    checksum: [u8; PAGE_RECORD_CHECKSUM_LEN],
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
    let (stored_payload, compression) = encode_page_record_payload(payload, options)?;
    let stored_len = stored_payload.len();
    let compressed = compression != PageRecordCompression::None;
    let mut record =
        Vec::with_capacity(PAGE_RECORD_SMALLEST_HEADER_LEN + 8 + stored_payload.len());
    record.extend_from_slice(PAGE_RECORD_MAGIC);
    record.extend_from_slice(&checksum_field);
    // Fixed width: an object id is a hash, so it fills its 64 bits and a varint costs more.
    record.extend_from_slice(&object_id.unwrap_or_default().to_le_bytes());
    put_page_record_varint(&mut record, payload.len() as u64);
    put_page_record_varint(&mut record, page_id);
    // 0 says there is no routing bucket, so an absent one costs one byte rather than five.
    put_page_record_varint(
        &mut record,
        routing_bucket.map_or(0, |bucket| u64::from(bucket) + 1),
    );
    put_page_record_varint(&mut record, band_id);
    record.push(match compression {
        PageRecordCompression::None => PAGE_RECORD_COMPRESSION_NONE,
        PageRecordCompression::Zstd => PAGE_RECORD_COMPRESSION_ZSTD,
    });
    if compressed {
        // Only a compressed record needs this: uncompressed, it is the payload length again.
        put_page_record_varint(&mut record, stored_len as u64);
    }
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
    // `PAGE_RECORD_COMPRESSION_MIN_BYTES`, so the step IS this call. At the 1,244-byte average
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
    if !record.starts_with(PAGE_RECORD_MAGIC) {
        return Ok(DecodedPageRecord {
            payload: record.to_vec(),
            logical_len: record.len(),
            compression: PageRecordCompression::None,
        });
    }
    if record.len() < PAGE_RECORD_SMALLEST_HEADER_LEN {
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
    if !slab.starts_with(PAGE_RECORD_MAGIC) {
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
        let address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_SMALLEST_HEADER_LEN {
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
        let address = BlockAddress::from_parts(0, 0, record_len as u64, header.page_id, header.object_id, header.routing_bucket, header.page_id.or(header.object_id), header.band_id);
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
    if !record.starts_with(PAGE_RECORD_MAGIC) {
        return Err(corrupt_page_envelope(address, "bad magic"));
    }
    if record.len() < PAGE_RECORD_SMALLEST_HEADER_LEN {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let checksum_at = PAGE_RECORD_CHECKSUM_OFFSET;
    let checksum = record[checksum_at..checksum_at + PAGE_RECORD_CHECKSUM_LEN]
        .try_into()
        .expect("page envelope checksum slice");
    let object_id = u64::from_le_bytes(
        record[checksum_at + PAGE_RECORD_CHECKSUM_LEN..PAGE_RECORD_FIXED_LEN]
            .try_into()
            .expect("page envelope object id slice"),
    );
    let mut cursor = PAGE_RECORD_FIXED_LEN;
    let payload_len =
        read_page_record_varint(record, &mut cursor, address, "payload length")? as usize;
    let page_id = read_page_record_varint(record, &mut cursor, address, "page id")?;
    let routing = read_page_record_varint(record, &mut cursor, address, "routing bucket")?;
    let band_id = read_page_record_varint(record, &mut cursor, address, "band id")?;
    let codec = *record
        .get(cursor)
        .ok_or_else(|| corrupt_page_envelope(address, "truncated compression codec"))?;
    cursor += 1;
    let compression = match codec {
        PAGE_RECORD_COMPRESSION_NONE => PageRecordCompression::None,
        PAGE_RECORD_COMPRESSION_ZSTD => PageRecordCompression::Zstd,
        codec => {
            return Err(corrupt_page_envelope(
                address,
                format!("unsupported compression codec {codec}"),
            ));
        }
    };
    let stored_len = if compression == PageRecordCompression::None {
        payload_len
    } else {
        read_page_record_varint(record, &mut cursor, address, "stored length")? as usize
    };
    let routing_bucket = match routing {
        0 => None,
        encoded => Some(u32::try_from(encoded - 1).map_err(|_| {
            corrupt_page_envelope(address, format!("routing bucket {encoded} out of range"))
        })?),
    };
    Ok(PageRecordHeader {
        header_len: cursor,
        payload_len,
        stored_len,
        checksum,
        page_id: Some(page_id),
        object_id: (object_id != 0).then_some(object_id),
        routing_bucket,
        band_id: Some(band_id),
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
            let payload = if header.payload_len <= ZSTD_TRUSTED_PAYLOAD_CEILING {
                ZSTD_DECOMPRESSOR
                    .with(|decompressor| {
                        decompressor
                            .borrow_mut()
                            .decompress(stored_payload, header.payload_len)
                    })
                    .map_err(|err| {
                        corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
                    })?
            } else {
                zstd::stream::decode_all(Cursor::new(stored_payload)).map_err(|err| {
                    corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
                })?
            };
            if payload.len() != header.payload_len {
                return Err(corrupt_page_envelope(
                    address,
                    format!(
                        "decompressed length {} does not match payload length {}",
                        payload.len(),
                        header.payload_len
                    ),
                ));
            }
            Ok(payload)
        }
    }
}

/// Build the 32-byte checksum field for a new (v7) record: CRC32C little-endian in the first
/// four bytes, a marker so the field is self-describing, then zero padding.
fn page_record_checksum_field(payload: &[u8]) -> [u8; PAGE_RECORD_CHECKSUM_LEN] {
    crate::checksum::crc32c(payload).to_le_bytes()
}

/// Verify a page record's payload against its stored CRC32C.
fn verify_page_record_checksum(
    payload: &[u8],
    expected_checksum: &[u8; PAGE_RECORD_CHECKSUM_LEN],
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
    if !slab.starts_with(PAGE_RECORD_MAGIC) {
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
        let address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_SMALLEST_HEADER_LEN {
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

/// The widest a header can be.
///
/// Every varint at its maximum, plus the compression codec and a stored length. Only the scan
/// uses it, to read a prefix big enough to hold any header before parsing one.
const PAGE_RECORD_WIDEST_HEADER_LEN: usize = PAGE_RECORD_FIXED_LEN + 5 * 10 + 1;

/// Read buffer for the page-id scan.
///
/// Sized so a bufferful spans many records rather than one: at the ~300-byte records the live
/// store holds, this is ~800 per fill, which is the difference between thousands of reads for a
/// slab and millions.
const PAGE_RECORD_SCAN_BUFFER_BYTES: usize = 256 * 1024;

/// The largest page id recorded in one slab, read from record headers alone.
///
/// `next_page_id_at` wants a single number per slab. It used to get it from `inspect_slab`, which
/// decodes every record to build a whole block-index report -- a zstd decompression, a checksum
/// verify, a second SHA-256 hex-encoded into a `String`, and one report struct per page -- and
/// then keeps only `last_page_id`. On a store whose pages are large and compressed that is tens
/// of gigabytes of decompress-and-hash to produce one integer, and it is paid on every open.
///
/// The page id and the record length both live in the header, so the walk never needs a payload.
/// Stepping the file by `header_len + stored_len` touches only headers, which is why this takes a
/// `File` rather than a `&[u8]`: the slab never has to be read into memory at all.
///
/// It reads through a `BufReader` rather than seeking per record. Records here are small -- the
/// live store holds ~3.4M of them averaging ~300 bytes -- so a seek and a read for each is
/// millions of syscalls, and measured SLOWER than reading the whole slab. `seek_relative`
/// drops buffered bytes in place when the destination is already in the buffer, so a run of
/// small records costs one read per bufferful while a large record still seeks.
///
/// Halting is deliberately `inspect_slab`'s: a record that does not parse ends the walk, and the
/// ids found before it stand. `LocalBlockStore::with_options` fences a torn tail on the active
/// slab precisely because this scan halts early. Returning an error instead would reach the
/// caller's `unwrap_or_default()` and reset the counter to 0 -- page-id reuse, and stale reads.
pub(super) fn max_page_id_in_slab_file(
    file: File,
    slab_len: u64,
    block_slab_id: u64,
) -> Result<Option<u64>, BlockStoreError> {
    let mut reader = BufReader::with_capacity(PAGE_RECORD_SCAN_BUFFER_BYTES, file);
    let mut max_page_id: Option<u64> = None;
    let mut offset: u64 = 0;
    let mut header = [0_u8; PAGE_RECORD_WIDEST_HEADER_LEN];
    while offset < slab_len {
        let remaining = slab_len - offset;
        let want = (PAGE_RECORD_WIDEST_HEADER_LEN as u64).min(remaining) as usize;
        if reader.read_exact(&mut header[..want]).is_err() {
            break;
        }
        let head = &header[..want];
        if !head.starts_with(PAGE_RECORD_MAGIC) {
            break;
        }
        let address =
            BlockAddress::from_parts(block_slab_id, offset, 0, None, None, None, None, None);
        let parsed = match parse_page_record_header(head, &address) {
            Ok(parsed) => parsed,
            Err(_) => break,
        };
        if let Some(page_id) = parsed.page_id {
            max_page_id = Some(max_page_id.map_or(page_id, |current: u64| current.max(page_id)));
        }
        let record_len = parsed.header_len.saturating_add(parsed.stored_len) as u64;
        if record_len == 0 || remaining < record_len {
            break;
        }
        // Step over the payload without reading it. The header read above may have run past
        // this record's end (a record shorter than the widest header) or stopped short of it,
        // so the skip is signed; `seek_relative` handles both inside the buffer.
        reader.seek_relative(record_len as i64 - want as i64)?;
        offset += record_len;
    }
    Ok(max_page_id)
}

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
    if !slab.starts_with(PAGE_RECORD_MAGIC) {
        report.logical_bytes = slab.len() as u64;
        report.page_count = 1;
        report.readable_prefix_physical_bytes = slab.len() as u64;
        return report;
    }

    let mut physical_offset = 0usize;
    while physical_offset < slab.len() {
        let remaining = &slab[physical_offset..];
        let mut address = BlockAddress::from_parts(block_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            record_slab_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "mixed raw bytes after page envelope").to_string(),
            );
            break;
        }
        if remaining.len() < PAGE_RECORD_SMALLEST_HEADER_LEN {
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
        address.set_band_id(header.band_id);
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

    /// Every header field comes back as it went in, each with a value that could not be mistaken
    /// for another field's.
    ///
    /// The payload is found by walking the header, so an offset error anywhere leaves the payload
    /// correct and reads page id, object id, routing bucket or band id out of the wrong bytes --
    /// silently. This is the test that notices.
    #[test]
    fn every_header_field_survives_a_round_trip() {
        let payload = b"header field round trip payload";
        let page_id: u64 = 0x1122_3344_5566_7788;
        let object_id: u64 = 0x99AA_BBCC_DDEE_F001;
        let routing_bucket: u32 = 0x0BAD_C0DE;
        let band_id: u64 = 0x0102_0304_0506_0708;

        let encoded = encode_page_record(
            payload,
            page_id,
            Some(object_id),
            Some(routing_bucket),
            band_id,
            BlockStoreOptions::default(),
        )
        .expect("encode");
        let header = parse_page_record_header(&encoded.bytes, &address()).expect("parse");
        assert_eq!(header.page_id, Some(page_id), "page id");
        assert_eq!(header.object_id, Some(object_id), "object id");
        assert_eq!(header.routing_bucket, Some(routing_bucket), "routing bucket");
        assert_eq!(header.band_id, Some(band_id), "band id");
        assert_eq!(header.payload_len, payload.len());
        assert_eq!(header.stored_len, payload.len());
        let decoded = decode_page_record(&encoded.bytes, &address()).expect("decode");
        assert_eq!(decoded.payload, payload);
    }

    /// An absent routing bucket comes back absent rather than as zero.
    #[test]
    fn an_absent_routing_bucket_stays_absent() {
        let payload = b"no routing bucket here";
        let encoded =
            encode_page_record(payload, 7, None, None, 0, BlockStoreOptions::default())
                .expect("encode");
        let header = parse_page_record_header(&encoded.bytes, &address()).expect("parse");
        assert_eq!(header.routing_bucket, None);
        assert_eq!(header.object_id, None);
        assert_eq!(header.band_id, Some(0));
    }

    /// The checksum is a CRC32C of the payload, in the four bytes after the magic.
    #[test]
    fn the_checksum_is_a_crc32c_of_the_payload() {
        let payload = b"page payload that is long enough to be interesting";
        let encoded =
            encode_page_record(payload, 7, None, None, 3, BlockStoreOptions::default())
                .expect("encode");
        let at = PAGE_RECORD_CHECKSUM_OFFSET;
        let stored = u32::from_le_bytes(
            encoded.bytes[at..at + PAGE_RECORD_CHECKSUM_LEN]
                .try_into()
                .unwrap(),
        );
        assert_eq!(stored, crate::checksum::crc32c(payload));
        assert_eq!(PAGE_RECORD_CHECKSUM_LEN, 4, "the field is the checksum, nothing more");
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
        for keep in [0, PAGE_RECORD_FIXED_LEN, PAGE_RECORD_SMALLEST_HEADER_LEN - 1] {
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
            header_len: PAGE_RECORD_SMALLEST_HEADER_LEN,
            payload_len,
            stored_len,
            checksum: [0_u8; PAGE_RECORD_CHECKSUM_LEN],
            page_id: Some(1),
            object_id: Some(1),
            routing_bucket: Some(0),
            band_id: None,
            compression: PageRecordCompression::Zstd,
        }
    }

    fn address_for(payload_len: usize) -> BlockAddress {
        BlockAddress::from_parts(1, 0, payload_len as u64, Some(1), Some(1), Some(0), None, None)
    }

    /// Round-trip at several sizes through the shared thread-local context.
    ///
    /// Sizes straddle PAGE_RECORD_COMPRESSION_MIN_BYTES (256) so both the compressed and the
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
                PAGE_RECORD_COMPRESSION_LEVEL,
            )
            .expect("compresses");
            let header = zstd_header(original.len(), compressed.len());
            let decoded = decode_page_record_payload(&compressed, &header, &address_for(len))
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
                    PAGE_RECORD_COMPRESSION_LEVEL,
                )
                .expect("compresses");
                let header = zstd_header(original.len(), compressed.len());
                let decoded = decode_page_record_payload(&compressed, &header, &address_for(len))
                    .expect("decodes");
                assert_eq!(original, decoded, "size {len} was wrong on a reused context");
            }
        }
    }

    /// A header that lies about its payload length is refused, not served.
    ///
    /// This matters more now than it did: the bulk API allocates the CLAIMED length before
    /// decompressing anything, so a lie is acted on before it is checked.
    #[test]
    fn a_header_that_lies_about_its_length_is_refused() {
        let original: Vec<u8> = (0..1000).map(|i| (i % 5) as u8).collect();
        let compressed = zstd::stream::encode_all(
            std::io::Cursor::new(&original[..]),
            PAGE_RECORD_COMPRESSION_LEVEL,
        )
        .expect("compresses");
        // The record really holds 1000 bytes; the header claims 4242.
        let header = zstd_header(4242, compressed.len());
        assert!(
            decode_page_record_payload(&compressed, &header, &address_for(1000)).is_err(),
            "a payload_len that disagrees with the record must be an error, not a short read"
        );
    }
}
