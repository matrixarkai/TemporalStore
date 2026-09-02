// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::io::Cursor;

use sha2::{Digest, Sha256};

use super::{
    BlockAddress, BlockStoreBlockIndexReport, BlockStoreError, BlockStoreOptions,
    BlockStoreSlabReport,
};

pub(super) const PAGE_RECORD_MAGIC: &[u8; 8] = b"TSPAGE01";
pub(super) const PAGE_RECORD_VERSION: u8 = 8;

/// Version at which the 32-byte checksum field switched from holding a full SHA-256 to
/// holding a CRC32C.
///
/// The digest is computed per page record on the synchronous write path, before the
/// durability barrier, and a cryptographic hash is the wrong tool for it: nothing here is
/// defending against a forged page, only against a page that corrupted into something still
/// decodable. CRC32C is what this design uses for exactly this. Records at
/// version 6 and below keep verifying as SHA-256, so existing slabs read back unchanged.
///
/// The field stays 32 bytes wide even though CRC32C needs 4, because every subsequent header
/// offset (page id, object id, routing bucket, band id, compression) is keyed off it. Keeping
/// the width means v7 differs from v6 only in how those bytes are interpreted, rather than
/// re-laying-out the header. The 28 unused bytes are written as zero and are worth reclaiming
/// in a later format change that renumbers offsets deliberately.
pub(super) const PAGE_RECORD_CHECKSUM_CRC32C_VERSION: u8 = 7;

/// Width of the checksum field, unchanged across the switch.
pub(super) const PAGE_RECORD_CHECKSUM_LEN: usize = 32;

/// Marker used in the checksum field's tail so a v7 record is self-describing on inspection.
const PAGE_RECORD_CHECKSUM_CRC32C: &[u8; 4] = b"C32C";
pub(super) const PAGE_RECORD_V1_HEADER_LEN: usize = 8 + 1 + 1 + 2 + 8 + 8 + 32;
pub(super) const PAGE_RECORD_V2_HEADER_LEN: usize = PAGE_RECORD_V1_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V3_HEADER_LEN: usize = PAGE_RECORD_V2_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V4_HEADER_LEN: usize = PAGE_RECORD_V3_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V5_HEADER_LEN: usize = PAGE_RECORD_V4_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V7_HEADER_LEN: usize = PAGE_RECORD_V5_HEADER_LEN + 16;
/// Version at which the checksum field shrank to the four bytes a CRC32C needs.
///
/// v7 kept it 32 bytes wide so that every later offset stayed where v6 had it, and wrote the
/// other 28 as zero. This reclaims them: the field is 4 bytes, and every field after it moves
/// down by 28. Records at v7 and below keep their old layout, so existing slabs read unchanged.
pub(super) const PAGE_RECORD_CHECKSUM_COMPACT_VERSION: u8 = 8;
/// Width of the checksum field at v8 and later: the CRC32C, and the marker that follows it.
///
/// Four bytes would hold the checksum, and the version byte already says which algorithm it is.
/// The marker is kept anyway because it is what makes the field self-describing to someone
/// reading a slab by hand, which the version byte does not do for a hexdump. Reclaiming 24 of the
/// 28 spare bytes and keeping that is the better trade.
pub(super) const PAGE_RECORD_CHECKSUM_COMPACT_LEN: usize = 8;
/// Bytes of interior padding v7 carried: three after the routing-bucket flag and seven after the
/// compression byte, both there to keep the 8-byte fields behind them aligned at offsets v6 had
/// fixed. v8 renumbers anyway, so it places those fields directly and keeps the ten.
pub(super) const PAGE_RECORD_V7_INTERIOR_PADDING: usize = 10;
pub(super) const PAGE_RECORD_HEADER_LEN: usize = PAGE_RECORD_V7_HEADER_LEN
    - (PAGE_RECORD_CHECKSUM_LEN - PAGE_RECORD_CHECKSUM_COMPACT_LEN)
    - PAGE_RECORD_V7_INTERIOR_PADDING;

/// Where the compression byte sits in a record this code writes.
///
/// Named rather than derived at each call site. Callers kept computing it -- first as the literal
/// 92, then as `PAGE_RECORD_HEADER_LEN - 16` -- and both encoded a layout: the literal encoded
/// v7's offsets, the subtraction encoded v7's trailing padding. Neither survived a renumbering.
pub(super) const fn page_record_compression_offset() -> usize {
    // Flag, routing bucket, band id, then the compression byte.
    28 + PAGE_RECORD_CHECKSUM_COMPACT_LEN + 17 + 4 + 8
}

/// Where the fields after the checksum begin, for a record at `version`.
///
/// Every offset past the checksum is this plus a fixed step, so the two layouts differ in one
/// number rather than in eight literals.
fn header_body_offset(version: u8) -> usize {
    let checksum_len = if version >= PAGE_RECORD_CHECKSUM_COMPACT_VERSION {
        PAGE_RECORD_CHECKSUM_COMPACT_LEN
    } else {
        PAGE_RECORD_CHECKSUM_LEN
    };
    28 + checksum_len
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
    /// Record format version, retained so the checksum is verified with the algorithm the
    /// record was written with.
    version: u8,
    header_len: usize,
    payload_len: usize,
    pub(super) stored_len: usize,
    expected_sha256: [u8; 32],
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
    let mut record = Vec::with_capacity(PAGE_RECORD_HEADER_LEN + stored_payload.len());
    record.extend_from_slice(PAGE_RECORD_MAGIC);
    record.push(PAGE_RECORD_VERSION);
    record.push(0);
    record.extend_from_slice(&(PAGE_RECORD_HEADER_LEN as u16).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&checksum_field[..PAGE_RECORD_CHECKSUM_COMPACT_LEN]);
    record.extend_from_slice(&page_id.to_le_bytes());
    record.extend_from_slice(&object_id.unwrap_or_default().to_le_bytes());
    record.push(u8::from(routing_bucket.is_some()));
    record.extend_from_slice(&routing_bucket.unwrap_or_default().to_le_bytes());
    record.extend_from_slice(&band_id.to_le_bytes());
    record.push(match compression {
        PageRecordCompression::None => PAGE_RECORD_COMPRESSION_NONE,
        PageRecordCompression::Zstd => PAGE_RECORD_COMPRESSION_ZSTD,
    });
    record.extend_from_slice(&(stored_len as u64).to_le_bytes());
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
    if record.len() < PAGE_RECORD_V1_HEADER_LEN {
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
    verify_page_record_checksum(&payload, &header.expected_sha256, header.version, address)?;
    Ok(DecodedPageRecord {
        payload,
        logical_len: header.payload_len,
        compression: header.compression,
    })
}

pub(super) fn logical_range_from_slab(
    slab: &[u8],
    page_slab_id: u64,
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
        let address = BlockAddress::from_parts(page_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
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
        verify_page_record_checksum(&payload, &header.expected_sha256, header.version, &address)?;
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

fn parse_page_record_header(
    record: &[u8],
    address: &BlockAddress,
) -> Result<PageRecordHeader, BlockStoreError> {
    let version = record[8];
    if !matches!(version, 1..=PAGE_RECORD_VERSION) {
        return Err(corrupt_page_envelope(
            address,
            format!("unsupported version {version}"),
        ));
    }
    let header_len = u16::from_le_bytes(
        record[10..12]
            .try_into()
            .expect("page envelope header length slice"),
    ) as usize;
    let expected_header_len = if version == 1 {
        PAGE_RECORD_V1_HEADER_LEN
    } else if version == 2 {
        PAGE_RECORD_V2_HEADER_LEN
    } else if version == 3 {
        PAGE_RECORD_V3_HEADER_LEN
    } else if version == 4 {
        PAGE_RECORD_V4_HEADER_LEN
    } else if version == 5 {
        PAGE_RECORD_V5_HEADER_LEN
    } else if version < PAGE_RECORD_CHECKSUM_COMPACT_VERSION {
        PAGE_RECORD_V7_HEADER_LEN
    } else {
        PAGE_RECORD_HEADER_LEN
    };
    if header_len != expected_header_len {
        return Err(corrupt_page_envelope(
            address,
            format!("unexpected header length {header_len}"),
        ));
    }
    if record.len() < expected_header_len {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let payload_len = u64::from_le_bytes(
        record[12..20]
            .try_into()
            .expect("page envelope payload length slice"),
    ) as usize;
    let raw_len = u64::from_le_bytes(
        record[20..28]
            .try_into()
            .expect("page envelope raw length slice"),
    ) as usize;
    if raw_len != payload_len {
        return Err(corrupt_page_envelope(
            address,
            format!("raw length {raw_len} does not match payload length {payload_len}"),
        ));
    }
    // Every offset below is stated as a step from where the checksum ends, so v8's narrower
    // field moves them all by one number instead of eight.
    let body = header_body_offset(version);
    let mut expected_sha256 = [0_u8; PAGE_RECORD_CHECKSUM_LEN];
    let checksum_len = body - 28;
    expected_sha256[..checksum_len].copy_from_slice(&record[28..body]);
    let expected_sha256 = expected_sha256;
    let page_id = if version >= 2 {
        Some(u64::from_le_bytes(
            record[body..body + 8]
                .try_into()
                .expect("page envelope page id slice"),
        ))
    } else {
        None
    };
    let object_id = if version >= 3 {
        let object_id = u64::from_le_bytes(
            record[body + 8..body + 16]
                .try_into()
                .expect("page envelope object id slice"),
        );
        (object_id != 0).then_some(object_id)
    } else {
        None
    };
    // v7 padded after the flag and after the compression byte; v8 does not.
    let flag_pad = if version >= PAGE_RECORD_CHECKSUM_COMPACT_VERSION { 0 } else { 3 };
    let compression_pad = if version >= PAGE_RECORD_CHECKSUM_COMPACT_VERSION { 0 } else { 7 };
    let routing_at = body + 17 + flag_pad;
    let band_at = routing_at + 4;
    let compression_at = band_at + 8;
    let stored_len_at = compression_at + 1 + compression_pad;
    let routing_bucket = if version >= 4 {
        if record[body + 16] == 1 {
            Some(u32::from_le_bytes(
                record[routing_at..routing_at + 4]
                    .try_into()
                    .expect("page envelope routing slot slice"),
            ))
        } else {
            None
        }
    } else {
        None
    };
    let band_id = if version >= 5 {
        Some(u64::from_le_bytes(
            record[band_at..band_at + 8]
                .try_into()
                .expect("page envelope band id slice"),
        ))
    } else {
        None
    };
    let (compression, stored_len) = if version >= 6 {
        let compression = match record[compression_at] {
            PAGE_RECORD_COMPRESSION_NONE => PageRecordCompression::None,
            PAGE_RECORD_COMPRESSION_ZSTD => PageRecordCompression::Zstd,
            codec => {
                return Err(corrupt_page_envelope(
                    address,
                    format!("unsupported compression codec {codec}"),
                ));
            }
        };
        let stored_len = u64::from_le_bytes(
            record[stored_len_at..stored_len_at + 8]
                .try_into()
                .expect("page envelope stored length slice"),
        ) as usize;
        (compression, stored_len)
    } else {
        (PageRecordCompression::None, payload_len)
    };
    if compression == PageRecordCompression::None && stored_len != payload_len {
        return Err(corrupt_page_envelope(
            address,
            format!("stored length {stored_len} does not match payload length {payload_len}"),
        ));
    }
    Ok(PageRecordHeader {
        version,
        header_len,
        payload_len,
        stored_len,
        expected_sha256,
        page_id,
        object_id,
        routing_bucket,
        band_id,
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
    let mut field = [0_u8; PAGE_RECORD_CHECKSUM_LEN];
    field[..4].copy_from_slice(&crate::checksum::crc32c(payload).to_le_bytes());
    field[4..8].copy_from_slice(PAGE_RECORD_CHECKSUM_CRC32C);
    field
}

/// Verify a page record's payload against its stored checksum field.
///
/// `version` selects the algorithm: v7 and later carry a CRC32C, v6 and earlier a full
/// SHA-256. Dispatching on the record's own version (rather than sniffing the field) means an
/// old slab always verifies the way it was written.
fn verify_page_record_checksum(
    payload: &[u8],
    expected_checksum: &[u8; PAGE_RECORD_CHECKSUM_LEN],
    version: u8,
    address: &BlockAddress,
) -> Result<(), BlockStoreError> {
    let (expected, actual) = if version >= PAGE_RECORD_CHECKSUM_CRC32C_VERSION {
        let stored = u32::from_le_bytes(
            expected_checksum[..4]
                .try_into()
                .expect("page envelope crc32c slice"),
        );
        let actual = crate::checksum::crc32c(payload);
        if stored == actual {
            return Ok(());
        }
        (format!("{stored:08x}"), format!("{actual:08x}"))
    } else {
        let actual_sha256 = Sha256::digest(payload);
        if &actual_sha256[..] == expected_checksum {
            return Ok(());
        }
        (
            hex::encode(expected_checksum),
            hex::encode(actual_sha256),
        )
    };
    Err(BlockStoreError::ChecksumMismatch {
        page_slab_id: address.page_slab_id,
        offset: address.offset,
        length: address.length,
        expected,
        actual,
    })
}

pub(super) fn corrupt_page_envelope(
    address: &BlockAddress,
    reason: impl Into<String>,
) -> BlockStoreError {
    BlockStoreError::CorruptPageEnvelope {
        page_slab_id: address.page_slab_id,
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
    page_slab_id: u64,
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
        let address = BlockAddress::from_parts(page_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
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

pub(super) fn inspect_slab(slab: &[u8], page_slab_id: u64) -> BlockStoreSlabReport {
    let mut report = BlockStoreSlabReport {
        page_slab_id,
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
        let mut address = BlockAddress::from_parts(page_slab_id, physical_offset as u64, 0, None, None, None, None, None);
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            record_slab_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "mixed raw bytes after page envelope").to_string(),
            );
            break;
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
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
                    block_slab_id: page_slab_id,
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
                    checksum: Some(sha256_hex(&decoded.payload)),
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

#[cfg(test)]
mod crc32c_switch_tests {
    use super::*;

    fn address() -> BlockAddress {
        BlockAddress::from_parts(1, 0, 0, None, None, None, None, None)
    }

    /// Rewrite a freshly encoded record as if it had been written by the previous format:
    /// version 6, with a full SHA-256 in the checksum field. The rest of the layout is
    /// identical, which is the whole reason the field kept its width.
    /// Build a record in the pre-v8 layout: 32-byte checksum field, 108-byte header.
    ///
    /// Written out by hand on purpose. Downgrading a record this code just encoded would test
    /// the new layout wearing an old version number; a slab written last year is these bytes.
    fn encode_v7_layout(
        payload: &[u8],
        version: u8,
        page_id: u64,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        band_id: u64,
    ) -> Vec<u8> {
        let mut record = Vec::new();
        record.extend_from_slice(PAGE_RECORD_MAGIC);
        record.push(version);
        record.push(0);
        record.extend_from_slice(&(PAGE_RECORD_V7_HEADER_LEN as u16).to_le_bytes());
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut checksum = [0_u8; PAGE_RECORD_CHECKSUM_LEN];
        if version >= PAGE_RECORD_CHECKSUM_CRC32C_VERSION {
            checksum[..4].copy_from_slice(&crate::checksum::crc32c(payload).to_le_bytes());
            checksum[4..8].copy_from_slice(PAGE_RECORD_CHECKSUM_CRC32C);
        } else {
            checksum.copy_from_slice(&Sha256::digest(payload));
        }
        record.extend_from_slice(&checksum);
        record.extend_from_slice(&page_id.to_le_bytes());
        record.extend_from_slice(&object_id.unwrap_or_default().to_le_bytes());
        record.push(u8::from(routing_bucket.is_some()));
        record.extend_from_slice(&[0, 0, 0]);
        record.extend_from_slice(&routing_bucket.unwrap_or_default().to_le_bytes());
        record.extend_from_slice(&band_id.to_le_bytes());
        record.push(PAGE_RECORD_COMPRESSION_NONE);
        record.extend_from_slice(&[0; 7]);
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(payload);
        assert_eq!(
            record.len(),
            PAGE_RECORD_V7_HEADER_LEN + payload.len(),
            "the fixture must be the old layout, not the new one"
        );
        record
    }

    /// A record written before the checksum field shrank still reads, fields and all.
    #[test]
    fn pre_v8_records_still_decode_every_field() {
        let payload = b"a page written before the checksum field shrank";
        let page_id: u64 = 0x1122_3344_5566_7788;
        let object_id: u64 = 0x99AA_BBCC_DDEE_F001;
        let routing_bucket: u32 = 0x0BAD_C0DE;
        let band_id: u64 = 0x0102_0304_0506_0708;

        for version in [6_u8, 7] {
            let record = encode_v7_layout(
                payload,
                version,
                page_id,
                Some(object_id),
                Some(routing_bucket),
                band_id,
            );
            let header =
                parse_page_record_header(&record, &address()).expect("old record parses");
            assert_eq!(header.version, version);
            assert_eq!(header.page_id, Some(page_id), "v{version} page id");
            assert_eq!(header.object_id, Some(object_id), "v{version} object id");
            assert_eq!(
                header.routing_bucket,
                Some(routing_bucket),
                "v{version} routing bucket"
            );
            assert_eq!(header.band_id, Some(band_id), "v{version} band id");
            let decoded = decode_page_record(&record, &address()).expect("old record decodes");
            assert_eq!(decoded.payload, payload, "v{version} payload");
        }
    }

    /// And a corrupted old record is still refused, at both checksum algorithms.
    #[test]
    fn a_corrupted_pre_v8_record_is_still_rejected() {
        let payload = b"payload that will be corrupted after the fact";
        for version in [6_u8, 7] {
            let mut record = encode_v7_layout(payload, version, 1, None, None, 0);
            let last = record.len() - 1;
            record[last] ^= 0xFF;
            assert!(
                decode_page_record(&record, &address()).is_err(),
                "v{version} corruption must be refused"
            );
        }
    }

    fn downgrade_to_v6_sha256(record: &mut [u8], payload: &[u8]) {
        record[8] = 6;
        let digest = Sha256::digest(payload);
        record[28..60].copy_from_slice(&digest);
    }

    #[test]
    fn new_records_are_written_at_the_crc32c_version() {
        let payload = b"page payload that is long enough to be interesting";
        let encoded = encode_page_record(payload, 7, None, None, 3, BlockStoreOptions::default())
            .expect("encode");
        assert_eq!(encoded.bytes[8], PAGE_RECORD_VERSION);
        // CRC32C in the four bytes the field now is. The marker that followed it existed to
        // make a 32-byte field self-describing; at v8 the field is exactly the checksum and the
        // version byte says which algorithm it is.
        let stored = u32::from_le_bytes(encoded.bytes[28..32].try_into().unwrap());
        assert_eq!(stored, crate::checksum::crc32c(payload));
    }

    /// Every header field comes back as it went in, each with a value that could not be
    /// mistaken for another field's.
    ///
    /// The existing round trips assert only the payload, and the payload is found through
    /// `header_len`, which the record carries. So an offset error anywhere after the checksum
    /// leaves the payload correct and reads page id, object id, routing bucket or band id out of
    /// the wrong bytes -- silently. This is the test that would notice, and it is the safety net
    /// any renumbering of those offsets needs.
    #[test]
    fn every_header_field_survives_a_round_trip() {
        let payload = b"header field round trip payload";
        // Distinct, non-overlapping, and none of them zero or equal to a length: a value read
        // from a neighbouring field cannot coincidentally match.
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

        let header = parse_page_record_header(&encoded.bytes, &address()).expect("parse header");
        assert_eq!(header.page_id, Some(page_id), "page id");
        assert_eq!(header.object_id, Some(object_id), "object id");
        assert_eq!(header.routing_bucket, Some(routing_bucket), "routing bucket");
        assert_eq!(header.band_id, Some(band_id), "band id");
        assert_eq!(header.payload_len as usize, payload.len(), "payload length");

        let decoded = decode_page_record(&encoded.bytes, &address()).expect("decode");
        assert_eq!(decoded.payload, payload, "payload");
    }

    #[test]
    fn crc32c_records_round_trip() {
        let payload = b"round trip payload";
        let encoded = encode_page_record(payload, 1, None, None, 0, BlockStoreOptions::default())
            .expect("encode");
        let decoded = decode_page_record(&encoded.bytes, &address()).expect("decode");
        assert_eq!(decoded.payload, payload);
    }

    /// The compatibility case that matters: slabs written before the switch must still read.


    #[test]
    fn a_corrupted_v7_record_is_rejected() {
        let payload = b"payload protected by crc32c";
        let mut encoded =
            encode_page_record(payload, 4, None, None, 0, BlockStoreOptions::default())
                .expect("encode");
        let last = encoded.bytes.len() - 1;
        encoded.bytes[last] ^= 0xff;
        assert!(matches!(
            decode_page_record(&encoded.bytes, &address()),
            Err(BlockStoreError::ChecksumMismatch { .. })
        ));
    }

    /// A record must NOT be accepted on the strength of a checksum the wrong algorithm would
    /// have produced -- the version alone selects the algorithm.
    ///
    /// Built in the pre-v8 layout so the header length agrees with the version it claims. Taking
    /// a record this code encodes today and stamping an old version byte on it no longer tests
    /// this: the header would be 84 bytes while v6 declares 108, so it is refused as a corrupt
    /// envelope before any checksum is looked at, and the assertion would pass without the
    /// algorithm ever being selected.
    #[test]
    fn the_version_selects_the_algorithm_not_the_field_contents() {
        let payload = b"version selects the algorithm";
        // A v7 record: its field holds a CRC32C.
        let mut record = encode_v7_layout(payload, 7, 5, None, None, 0);
        // Now claim it is v6, which verifies as SHA-256. The bytes in the field are a CRC32C,
        // so verification must fail -- and reach the checksum to do so.
        record[8] = 6;
        assert!(
            matches!(
                decode_page_record(&record, &address()),
                Err(BlockStoreError::ChecksumMismatch { .. })
            ),
            "a v6 claim over a crc32c field must fail SHA-256 verification"
        );

        // And the other direction: a v6 record's SHA-256 must not satisfy a v7 CRC32C read.
        let mut other = encode_v7_layout(payload, 6, 5, None, None, 0);
        other[8] = 7;
        assert!(
            matches!(
                decode_page_record(&other, &address()),
                Err(BlockStoreError::ChecksumMismatch { .. })
            ),
            "a v7 claim over a sha256 field must fail crc32c verification"
        );
    }
}


#[cfg(test)]
mod reused_zstd_context_tests {
    use super::*;

    fn zstd_header(payload_len: usize, stored_len: usize) -> PageRecordHeader {
        PageRecordHeader {
            version: PAGE_RECORD_VERSION,
            header_len: PAGE_RECORD_HEADER_LEN,
            payload_len,
            stored_len,
            expected_sha256: [0_u8; 32],
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
