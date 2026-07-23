use std::collections::BTreeSet;
use std::io::Cursor;

use sha2::{Digest, Sha256};

use super::{
    BlockAddress, BlockStoreBlockIndexReport, BlockStoreError, BlockStoreOptions,
    BlockStoreSegmentReport,
};

pub(super) const PAGE_RECORD_MAGIC: &[u8; 8] = b"TSPAGE01";
pub(super) const PAGE_RECORD_VERSION: u8 = 6;
pub(super) const PAGE_RECORD_V1_HEADER_LEN: usize = 8 + 1 + 1 + 2 + 8 + 8 + 32;
pub(super) const PAGE_RECORD_V2_HEADER_LEN: usize = PAGE_RECORD_V1_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V3_HEADER_LEN: usize = PAGE_RECORD_V2_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V4_HEADER_LEN: usize = PAGE_RECORD_V3_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_V5_HEADER_LEN: usize = PAGE_RECORD_V4_HEADER_LEN + 8;
pub(super) const PAGE_RECORD_HEADER_LEN: usize = PAGE_RECORD_V5_HEADER_LEN + 16;
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
    expected_sha256: [u8; 32],
    page_id: Option<u64>,
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    extent_id: Option<u64>,
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
    routing_slot: Option<u32>,
    extent_id: u64,
    options: BlockStoreOptions,
) -> Result<EncodedPageRecord, BlockStoreError> {
    let digest = Sha256::digest(payload);
    let (stored_payload, compression) = encode_page_record_payload(payload, options)?;
    let stored_len = stored_payload.len();
    let mut record = Vec::with_capacity(PAGE_RECORD_HEADER_LEN + stored_payload.len());
    record.extend_from_slice(PAGE_RECORD_MAGIC);
    record.push(PAGE_RECORD_VERSION);
    record.push(0);
    record.extend_from_slice(&(PAGE_RECORD_HEADER_LEN as u16).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&digest);
    record.extend_from_slice(&page_id.to_le_bytes());
    record.extend_from_slice(&object_id.unwrap_or_default().to_le_bytes());
    record.push(u8::from(routing_slot.is_some()));
    record.extend_from_slice(&[0, 0, 0]);
    record.extend_from_slice(&routing_slot.unwrap_or_default().to_le_bytes());
    record.extend_from_slice(&extent_id.to_le_bytes());
    record.push(match compression {
        PageRecordCompression::None => PAGE_RECORD_COMPRESSION_NONE,
        PageRecordCompression::Zstd => PAGE_RECORD_COMPRESSION_ZSTD,
    });
    record.extend_from_slice(&[0; 7]);
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
    let compressed = zstd::stream::encode_all(
        Cursor::new(payload),
        options.compression_level.clamp(-7, 22),
    )?;
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
    if let (Some(address_page_id), Some(record_page_id)) = (address.page_id, header.page_id) {
        if address_page_id != record_page_id {
            return Err(corrupt_page_envelope(
                address,
                format!("page id mismatch: address {address_page_id}, record {record_page_id}"),
            ));
        }
    }
    if let (Some(address_object_id), Some(record_object_id)) = (address.object_id, header.object_id)
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
    if let (Some(address_routing_slot), Some(record_routing_slot)) =
        (address.routing_slot, header.routing_slot)
    {
        if address_routing_slot != record_routing_slot {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "routing slot mismatch: address {address_routing_slot}, record {record_routing_slot}"
                ),
            ));
        }
    }
    if let (Some(address_extent_id), Some(record_extent_id)) = (address.extent_id, header.extent_id)
    {
        if address_extent_id != record_extent_id {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "extent id mismatch: address {address_extent_id}, record {record_extent_id}"
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
    verify_page_record_checksum(&payload, &header.expected_sha256, address)?;
    Ok(DecodedPageRecord {
        payload,
        logical_len: header.payload_len,
        compression: header.compression,
    })
}

pub(super) fn logical_range_from_segment(
    segment: &[u8],
    page_segment_id: u64,
    offset: u64,
    size: u64,
) -> Result<LogicalRangeRead, BlockStoreError> {
    if size == 0 {
        return Ok(LogicalRangeRead {
            bytes: Vec::new(),
            compressed_records_read: 0,
        });
    }
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        let start = offset as usize;
        let end = start.saturating_add(size as usize).min(segment.len());
        let bytes = if start >= segment.len() {
            Vec::new()
        } else {
            segment[start..end].to_vec()
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

    while physical_offset < segment.len() && out.len() < size as usize {
        let remaining = &segment[physical_offset..];
        let address = BlockAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            generation: None,
            extent_id: None,
            sha256: None,
        };
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
        let address = BlockAddress {
            length: record_len as u64,
            page_id: header.page_id,
            object_id: header.object_id,
            routing_slot: header.routing_slot,
            generation: header.page_id.or(header.object_id),
            extent_id: header.extent_id,
            ..address
        };
        let payload = decode_page_record_payload(
            &remaining[header.header_len..record_len],
            &header,
            &address,
        )?;
        verify_page_record_checksum(&payload, &header.expected_sha256, &address)?;
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
    let expected_sha256 = record[28..60]
        .try_into()
        .expect("page envelope sha256 slice");
    let page_id = if version >= 2 {
        Some(u64::from_le_bytes(
            record[60..68]
                .try_into()
                .expect("page envelope page id slice"),
        ))
    } else {
        None
    };
    let object_id = if version >= 3 {
        let object_id = u64::from_le_bytes(
            record[68..76]
                .try_into()
                .expect("page envelope object id slice"),
        );
        (object_id != 0).then_some(object_id)
    } else {
        None
    };
    let routing_slot = if version >= 4 {
        if record[76] == 1 {
            Some(u32::from_le_bytes(
                record[80..84]
                    .try_into()
                    .expect("page envelope routing slot slice"),
            ))
        } else {
            None
        }
    } else {
        None
    };
    let extent_id = if version >= 5 {
        Some(u64::from_le_bytes(
            record[84..92]
                .try_into()
                .expect("page envelope extent id slice"),
        ))
    } else {
        None
    };
    let (compression, stored_len) = if version >= 6 {
        let compression = match record[92] {
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
            record[100..108]
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
        header_len,
        payload_len,
        stored_len,
        expected_sha256,
        page_id,
        object_id,
        routing_slot,
        extent_id,
        compression,
    })
}

fn decode_page_record_payload(
    stored_payload: &[u8],
    header: &PageRecordHeader,
    address: &BlockAddress,
) -> Result<Vec<u8>, BlockStoreError> {
    match header.compression {
        PageRecordCompression::None => Ok(stored_payload.to_vec()),
        PageRecordCompression::Zstd => {
            let payload = zstd::stream::decode_all(Cursor::new(stored_payload)).map_err(|err| {
                corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
            })?;
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

fn verify_page_record_checksum(
    payload: &[u8],
    expected_sha256: &[u8; 32],
    address: &BlockAddress,
) -> Result<(), BlockStoreError> {
    let actual_sha256 = Sha256::digest(payload);
    if &actual_sha256[..] != expected_sha256 {
        return Err(BlockStoreError::ChecksumMismatch {
            page_segment_id: address.page_segment_id,
            offset: address.offset,
            length: address.length,
            expected: hex::encode(expected_sha256),
            actual: hex::encode(actual_sha256),
        });
    }
    Ok(())
}

pub(super) fn corrupt_page_envelope(
    address: &BlockAddress,
    reason: impl Into<String>,
) -> BlockStoreError {
    BlockStoreError::CorruptPageEnvelope {
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        reason: reason.into(),
    }
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SegmentSummary {
    pub(super) logical_bytes: u64,
    pub(super) first_page_id: Option<u64>,
    pub(super) last_page_id: Option<u64>,
}

pub(super) fn summarize_segment(
    segment: &[u8],
    page_segment_id: u64,
) -> Result<SegmentSummary, BlockStoreError> {
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        return Ok(SegmentSummary {
            logical_bytes: segment.len() as u64,
            first_page_id: None,
            last_page_id: None,
        });
    }
    let mut physical_offset = 0usize;
    let mut summary = SegmentSummary::default();
    while physical_offset < segment.len() {
        let remaining = &segment[physical_offset..];
        let address = BlockAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            generation: None,
            extent_id: None,
            sha256: None,
        };
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

pub(super) fn inspect_segment(segment: &[u8], page_segment_id: u64) -> BlockStoreSegmentReport {
    let mut report = BlockStoreSegmentReport {
        page_segment_id,
        physical_bytes: segment.len() as u64,
        ..BlockStoreSegmentReport::default()
    };
    let mut object_ids = BTreeSet::new();
    let mut routing_slots = BTreeSet::new();
    if segment.is_empty() {
        return report;
    }
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        report.logical_bytes = segment.len() as u64;
        report.page_count = 1;
        report.readable_prefix_physical_bytes = segment.len() as u64;
        return report;
    }

    let mut physical_offset = 0usize;
    while physical_offset < segment.len() {
        let remaining = &segment[physical_offset..];
        let mut address = BlockAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            generation: None,
            extent_id: None,
            sha256: None,
        };
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            record_segment_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "mixed raw bytes after page envelope").to_string(),
            );
            break;
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
            record_segment_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "short header").to_string(),
            );
            break;
        }
        let header = match parse_page_record_header(remaining, &address) {
            Ok(header) => header,
            Err(err) => {
                record_segment_inspection_error(&mut report, address.offset, err.to_string());
                break;
            }
        };
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            record_segment_inspection_error(
                &mut report,
                address.offset,
                corrupt_page_envelope(&address, "payload length mismatch".to_string()).to_string(),
            );
            break;
        }
        address.length = record_len as u64;
        address.page_id = header.page_id;
        address.object_id = header.object_id;
        address.routing_slot = header.routing_slot;
        address.extent_id = header.extent_id;
        match decode_page_record(&remaining[..record_len], &address) {
            Ok(decoded) => {
                report.page_count = report.page_count.saturating_add(1);
                report.logical_bytes = report
                    .logical_bytes
                    .saturating_add(decoded.logical_len as u64);
                report.block_index_entries.push(BlockStoreBlockIndexReport {
                    block_segment_id: page_segment_id,
                    offset: address.offset,
                    length: address.length,
                    compact_segment_address: address.compact_segment_address(),
                    compact_segment_id: address.compact_segment_id(),
                    compact_segment_offset: address.compact_segment_offset(),
                    storage_segment_id: header.extent_id,
                    object_id: header.object_id,
                    model_id: None,
                    block_id: header.page_id,
                    block_size: decoded.logical_len as u64,
                    stored_size: header.stored_len as u64,
                    dirty: false,
                    deleted: decoded.logical_len == 0,
                    block_in_log: false,
                    routing_slot: header.routing_slot,
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
                if let Some(routing_slot) = header.routing_slot {
                    routing_slots.insert(routing_slot);
                    report.routing_slot_count = routing_slots.len() as u64;
                    report.first_routing_slot = routing_slots.first().copied();
                    report.last_routing_slot = routing_slots.last().copied();
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
                record_segment_inspection_error(&mut report, address.offset, err.to_string());
                break;
            }
        }
        physical_offset = physical_offset.saturating_add(record_len);
        report.readable_prefix_physical_bytes = physical_offset as u64;
    }
    report
}

fn record_segment_inspection_error(
    report: &mut BlockStoreSegmentReport,
    offset: u64,
    error: String,
) {
    report.has_corruption = true;
    report.first_error_offset = Some(offset);
    report.first_error = Some(error);
}
