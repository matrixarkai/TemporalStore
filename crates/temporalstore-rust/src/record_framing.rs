// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Record framing shared by the write-ahead log and the served-index log.
//!
//! Both are append-only streams in the reference implementation, and both are framed by the
//! same stream layer:
//!
//! ```text
//! varint32(payload_len) | little_endian_u32(crc32c) | payload
//! ```
//!
//! This module is that layer. It is shared rather than duplicated for the same reason the
//! reference shares it: the two streams differ in what they carry, not in how a record is
//! delimited or verified, and a second framing would be a second set of corruption and
//! truncation semantics to keep correct.
//!
//! Length-prefixing is what makes a record addressable by its byte offset — the log id that a
//! block address carries — and it is what allows a payload to contain any byte, including the
//! newline that a line-oriented format cannot survive.

use prost::Message;

use crate::checksum::crc32c;

/// A framing or integrity failure.
///
/// A record that fails to decode is corruption of committed data, not an absent record. In the
/// WAL a block-carrying record IS the durable copy of that block; in the index log a record is
/// the durable statement of where blocks live. Callers must surface this as data loss rather
/// than skipping past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFramingError(pub String);

impl std::fmt::Display for RecordFramingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "record framing error: {}", self.0)
    }
}

impl std::error::Error for RecordFramingError {}

/// Frame one record: `varint32(payload_len) | little_endian_u32(crc32c) | payload`.
pub fn encode_framed<M: Message>(record: &M) -> Vec<u8> {
    let payload = record.encode_to_vec();
    let checksum = crc32c(&payload);
    let mut out = Vec::with_capacity(payload.len() + 9);
    encode_varint32(payload.len() as u32, &mut out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Decode the record starting at `offset`, returning it together with the **absolute offset of
/// the next record**.
///
/// `offset` is the log id — the value a block address carries.
///
/// The second element is deliberately an absolute offset rather than a relative length. Every
/// caller either scans forward (`offset = next_offset`) or stops; returning a length invites
/// `offset = consumed`, which silently walks backwards after the first record and loops
/// forever, and the growing results then take the process out through the OOM killer rather
/// than failing an assertion. That is not hypothetical — it is what the first consumer of this
/// function did — so the signature is shaped to make it unrepresentable.
pub fn decode_framed_at<M: Message + Default>(
    bytes: &[u8],
    offset: usize,
) -> Result<(M, usize), RecordFramingError> {
    let rest = bytes.get(offset..).ok_or_else(|| {
        RecordFramingError(format!("offset {offset} is past the end of the stream"))
    })?;
    let (payload_len, varint_len) = decode_varint32(rest)?;
    let header_len = varint_len + 4;
    let payload_len = payload_len as usize;
    let end = header_len
        .checked_add(payload_len)
        .ok_or_else(|| RecordFramingError("record length overflows".to_string()))?;
    if rest.len() < end {
        return Err(RecordFramingError(format!(
            "record at {offset} claims {payload_len} payload bytes but only {} remain",
            rest.len().saturating_sub(header_len)
        )));
    }
    let declared = u32::from_le_bytes(
        rest[varint_len..header_len]
            .try_into()
            .expect("checksum slice is 4 bytes"),
    );
    let payload = &rest[header_len..end];
    let actual = crc32c(payload);
    if declared != actual {
        return Err(RecordFramingError(format!(
            "record at {offset} checksum mismatch: declared {declared:08x}, actual {actual:08x}"
        )));
    }
    let record = M::decode(payload).map_err(|error| {
        RecordFramingError(format!("record at {offset} failed to decode: {error}"))
    })?;
    Ok((record, offset + end))
}

/// Iterate every record in a framed stream, owning the cursor.
///
/// Callers get records rather than offsets, so they cannot do the offset arithmetic that the
/// signature above exists to prevent. This mirrors the reference, whose log iterators advance
/// internally for the same reason. Stops at the first malformed record, yielding the error.
pub struct FramedRecords<'a, M> {
    bytes: &'a [u8],
    offset: usize,
    finished: bool,
    _record: std::marker::PhantomData<M>,
}

impl<'a, M> FramedRecords<'a, M> {
    /// Iterate from `start_offset`, which is a log id.
    pub fn new(bytes: &'a [u8], start_offset: usize) -> Self {
        Self {
            bytes,
            offset: start_offset,
            finished: false,
            _record: std::marker::PhantomData,
        }
    }

    /// The log id of the record the next call will decode.
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl<M: Message + Default> Iterator for FramedRecords<'_, M> {
    /// `(log_id, framed_size, record)` — the log id and size are what a block address is built
    /// from, so a replay can address a block it just read without recomputing anything.
    type Item = Result<(u64, u64, M), RecordFramingError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.offset >= self.bytes.len() {
            return None;
        }
        let log_id = self.offset as u64;
        match decode_framed_at::<M>(self.bytes, self.offset) {
            Ok((record, next_offset)) => {
                debug_assert!(next_offset > self.offset, "framed scan must advance");
                let framed_size = (next_offset - self.offset) as u64;
                self.offset = next_offset;
                Some(Ok((log_id, framed_size, record)))
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn encode_varint32(mut value: u32, out: &mut Vec<u8>) {
    loop {
        if value < 0x80 {
            out.push(value as u8);
            return;
        }
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
}

fn decode_varint32(bytes: &[u8]) -> Result<(u32, usize), RecordFramingError> {
    let mut value = 0_u32;
    for (index, &byte) in bytes.iter().take(5).enumerate() {
        let part = u32::from(byte & 0x7f);
        value |= part
            .checked_shl(7 * index as u32)
            .ok_or_else(|| RecordFramingError("varint overflows 32 bits".to_string()))?;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(RecordFramingError(
        "varint is truncated or overlong".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_record::{WalItem, WalRecord, WAL_RECORD_VERSION};

    fn record(sequence: u64, block: &[u8]) -> WalRecord {
        WalRecord {
            version: WAL_RECORD_VERSION,
            sequence,
            items: vec![WalItem {
                block_log: true,
                block: block.to_vec(),
                ..Default::default()
            }],
        }
    }

    #[test]
    fn iterator_yields_log_id_and_size_for_every_record() {
        let records: Vec<WalRecord> = (1..=4)
            .map(|sequence| record(sequence, format!("block-{sequence}").as_bytes()))
            .collect();
        let mut stream = Vec::new();
        let mut expected_log_ids = Vec::new();
        for value in &records {
            expected_log_ids.push(stream.len() as u64);
            stream.extend_from_slice(&encode_framed(value));
        }

        let seen: Vec<(u64, u64, WalRecord)> = FramedRecords::new(&stream, 0)
            .map(|item| item.unwrap())
            .collect();
        assert_eq!(seen.len(), records.len());
        for (index, (log_id, framed_size, value)) in seen.iter().enumerate() {
            assert_eq!(*log_id, expected_log_ids[index], "log id is the byte offset");
            assert_eq!(value, &records[index]);
            assert!(*framed_size > 0);
        }
    }

    #[test]
    fn iterator_can_start_from_a_watermark() {
        // Replay resumes from the dumped-log-id watermark rather than from zero.
        let first = encode_framed(&record(1, b"first"));
        let second_log_id = first.len();
        let mut stream = first;
        stream.extend_from_slice(&encode_framed(&record(2, b"second")));

        let seen: Vec<WalRecord> = FramedRecords::new(&stream, second_log_id)
            .map(|item| item.unwrap().2)
            .collect();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].sequence, 2);
    }

    #[test]
    fn iterator_stops_at_corruption_rather_than_skipping_it() {
        let mut stream = encode_framed(&record(1, b"good"));
        let corrupt_at = stream.len();
        stream.extend_from_slice(&encode_framed(&record(2, b"bad")));
        let last = stream.len() - 1;
        stream[last] ^= 0xff;

        let results: Vec<_> = FramedRecords::<WalRecord>::new(&stream, 0).collect();
        assert_eq!(results.len(), 2, "one good record, then the error");
        assert!(results[0].is_ok());
        let error = results[1].as_ref().unwrap_err();
        assert!(
            error.0.contains(&corrupt_at.to_string()),
            "error names the offending offset: {error}"
        );
    }

    #[test]
    fn a_truncated_tail_surfaces_as_an_error_not_a_silent_stop() {
        // A crash mid-append leaves a partial record. Replay must see an error, not treat the
        // stream as having ended cleanly.
        let mut stream = encode_framed(&record(1, b"complete"));
        stream.extend_from_slice(&encode_framed(&record(2, b"incomplete")));
        stream.truncate(stream.len() - 4);

        let results: Vec<_> = FramedRecords::<WalRecord>::new(&stream, 0).collect();
        assert!(results[0].is_ok());
        assert!(results.last().unwrap().is_err());
    }
}
