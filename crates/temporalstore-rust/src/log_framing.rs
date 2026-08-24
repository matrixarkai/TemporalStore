// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Per-record integrity framing for the append-only JSON logs (WAL + served-index log).
//!
//! Historically each log record was a bare single-line JSON document terminated by `\n`.
//! A value-preserving bit-flip -- a flipped digit in a sequence/ttl, a flipped byte inside
//! a base64 value -- parses cleanly and would replay as truth on the default recovery path.
//! Framing wraps each line with a magic prefix, the payload length, and a truncated SHA-256
//! digest of the payload, so a silent corruption of an otherwise-complete committed record
//! is detected on read and surfaced as data loss rather than applied.
//!
//! Backward compatibility: a legacy unframed record is a serialized JSON object, so it
//! always begins with `{`. A framed record begins with [`FRAME_MAGIC`]. [`decode_line`]
//! detects the framing by that prefix and, for a legacy record, treats the whole line as the
//! payload -- so a WAL / index-log written before this change still loads unchanged. New
//! writes are framed; the two coexist in one file across an upgrade (and across a GC rewrite,
//! which re-emits every retained record framed).
//!
//! Two framed formats exist. New writes use `#tsf2`, which carries a CRC32C:
//! `#tsf2 <payload_len> <crc32c_hex> <payload_json>\n`
//! `#tsf1` records carry a truncated SHA-256 instead and are still read:
//! `#tsf1 <payload_len> <sha256_prefix_hex> <payload_json>\n`
//!
//! The switch to CRC32C is a write-path cost change, not a weakening. This checksum defends
//! against accidental corruption of a committed record -- a flipped bit that still parses --
//! and never against a forged one; nothing in the recovery path treats it as a signature. A
//! cryptographic digest was simply the wrong tool here: it is computed per record,
//! synchronously, before the durability barrier. CRC32C is what this design
//! uses for the same job (a per-record CRC in the record header plus a running per-block CRC
//! in the block footer).
//! The payload JSON is compact serde output, so it contains neither a literal `\n` (line
//! boundary) nor is its boundary ambiguous: the first two space-delimited fields after the
//! magic are the length and digest, and everything after the third space is the payload
//! (which may itself contain spaces inside JSON string values).

use sha2::{Digest, Sha256};

/// ASCII framing prefix for the current (CRC32C) format, including the trailing space that
/// delimits the fields after it. Chosen so it can never collide with a legacy record: a
/// serialized JSON object always starts with `{`, never `#`.
pub(crate) const FRAME_MAGIC_V2: &[u8] = b"#tsf2 ";

/// Framing prefix for the previous (truncated SHA-256) format. Still decoded so logs written
/// before the switch load unchanged; never written.
pub(crate) const FRAME_MAGIC_V1: &[u8] = b"#tsf1 ";

/// The prefix new writes use.
pub(crate) const FRAME_MAGIC: &[u8] = FRAME_MAGIC_V2;

/// Prefix of the reclaim-base header, the optional first line of a log file.
///
/// A reclaim drops a prefix of the file, shifting every surviving record down by the number of
/// bytes removed. This header records the running total of bytes reclaimed so far, which is
/// what lets a stored byte offset keep meaning the same record afterwards. Distinct from the
/// record magics so a reader can tell a header from a record, and absent from files that have
/// never been reclaimed -- which read as a base of zero.
pub(crate) const BASE_HEADER_MAGIC: &[u8] = b"#tsb1 ";

/// Encode the reclaim-base header line, newline-terminated.
///
/// Checksummed like a record: the base is load-bearing for address resolution, so a corrupted
/// one would silently resolve every offset to the wrong record.
pub(crate) fn encode_base_header(base_offset: u64) -> Vec<u8> {
    let value = base_offset.to_string();
    let mut line = Vec::with_capacity(BASE_HEADER_MAGIC.len() + 32);
    line.extend_from_slice(BASE_HEADER_MAGIC);
    line.extend_from_slice(crc_digest_hex(value.as_bytes()).as_bytes());
    line.push(b' ');
    line.extend_from_slice(value.as_bytes());
    line.push(b'\n');
    line
}

/// Decode a reclaim-base header line, verifying its checksum.
///
/// Returns `Ok(None)` when the line is not a header at all, which is how a file that predates
/// reclaim -- or has simply never been reclaimed -- reads as a base of zero.
pub(crate) fn decode_base_header(line: &[u8]) -> Result<Option<u64>, FramingError> {
    if !line.starts_with(BASE_HEADER_MAGIC) {
        return Ok(None);
    }
    let rest = &line[BASE_HEADER_MAGIC.len()..];
    let split = rest
        .iter()
        .position(|byte| *byte == b' ')
        .ok_or_else(|| FramingError("reclaim-base header has no checksum separator".to_string()))?;
    let (digest, value) = rest.split_at(split);
    let value = &value[1..];
    let value_text = std::str::from_utf8(value)
        .map_err(|_| FramingError("reclaim-base header is not utf-8".to_string()))?
        .trim_end();
    let expected = crc_digest_hex(value_text.as_bytes());
    if expected.as_bytes() != digest {
        return Err(FramingError(format!(
            "reclaim-base header checksum mismatch: expected {expected}, found {}",
            String::from_utf8_lossy(digest)
        )));
    }
    value_text
        .parse::<u64>()
        .map(Some)
        .map_err(|_| FramingError(format!("reclaim-base header is not a number: {value_text}")))
}

/// Number of leading SHA-256 bytes retained in a v1 framed digest.
const DIGEST_BYTES: usize = 8;

/// Integrity failure discovered while decoding a framed log line. Carried into the WAL /
/// index-log error types (as their `Corruption` variant) so the recovery path surfaces it as
/// data loss and refuses the load rather than silently applying or skipping the record.
#[derive(Debug, Clone)]
pub(crate) struct FramingError(pub String);

impl std::fmt::Display for FramingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "log record integrity error: {}", self.0)
    }
}

impl std::error::Error for FramingError {}

/// v1 checksum: the leading bytes of a SHA-256, hex encoded. Read-only.
fn sha256_digest_hex(payload: &[u8]) -> String {
    let full = Sha256::digest(payload);
    hex::encode(&full[..DIGEST_BYTES])
}

/// v2 checksum: CRC32C, hex encoded, fixed width.
fn crc_digest_hex(payload: &[u8]) -> String {
    crate::checksum::crc32c_hex(payload)
}

/// Encode `payload` (a single-line JSON document with NO trailing newline) as a framed,
/// newline-terminated log line.
pub(crate) fn encode_line(payload: &[u8]) -> Vec<u8> {
    let digest = crc_digest_hex(payload);
    let header = format!("#tsf2 {} {} ", payload.len(), digest);
    let mut out = Vec::with_capacity(header.len() + payload.len() + 1);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(payload);
    out.push(b'\n');
    out
}

/// Decode one raw log line (as read by a `\n`-delimited reader; a trailing newline is
/// optional and stripped) into its JSON payload bytes, verifying the integrity envelope when
/// present. A legacy unframed record is returned as-is (minus any trailing newline). A framed
/// record whose declared length or digest does not match its payload is COMMITTED corruption
/// and returns `Err`.
pub(crate) fn decode_line(line: &[u8]) -> Result<&[u8], FramingError> {
    let line = strip_trailing_newline(line);
    // Pick the checksum by prefix so both framed formats round-trip out of one file: an
    // upgrade, or a GC rewrite spanning one, leaves v1 and v2 records interleaved.
    let (checksum_of, rest): (fn(&[u8]) -> String, &[u8]) = if line.starts_with(FRAME_MAGIC_V2) {
        (crc_digest_hex, &line[FRAME_MAGIC_V2.len()..])
    } else if line.starts_with(FRAME_MAGIC_V1) {
        (sha256_digest_hex, &line[FRAME_MAGIC_V1.len()..])
    } else {
        // Legacy unframed record (or a blank line): the whole line is the payload.
        return Ok(line);
    };
    let (len_field, rest) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, payload) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    if declared_len != payload.len() {
        return Err(FramingError(format!(
            "framed record length mismatch: declared {declared_len}, actual {}",
            payload.len()
        )));
    }
    let actual = checksum_of(payload);
    if actual.as_bytes() != digest_field {
        return Err(FramingError(format!(
            "framed record checksum mismatch: declared {}, actual {actual}",
            String::from_utf8_lossy(digest_field)
        )));
    }
    Ok(payload)
}

/// Introduces a record whose payload is binary rather than text.
///
/// A binary payload may contain a newline, so a record carrying one cannot be found by splitting
/// on newlines. Readers use [`next_frame`] or [`read_frame`] instead, which take the length the
/// frame already declares rather than looking for a delimiter.
pub(crate) const BINARY_PAYLOAD_MAGIC: u8 = 0xA7;

/// Read the next record from `bytes`, returning how many bytes it occupied and its payload.
///
/// `Ok(None)` means there is nothing further to read, or that what remains is shorter than the
/// record it declares -- a torn tail from a crash mid-append, which the caller truncates. A
/// complete record whose digest does not match is committed corruption and returns `Err`. The
/// checksum is picked by prefix, exactly as [`decode_line`] picks it.
pub(crate) fn next_frame(bytes: &[u8]) -> Result<Option<(usize, &[u8])>, FramingError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let (checksum_of, header_prefix_len): (fn(&[u8]) -> String, usize) =
        if bytes.starts_with(FRAME_MAGIC_V2) {
            (crc_digest_hex, FRAME_MAGIC_V2.len())
        } else if bytes.starts_with(FRAME_MAGIC_V1) {
            (sha256_digest_hex, FRAME_MAGIC_V1.len())
        } else {
            // Unframed: the record ends at the newline, and the whole line is the payload.
            // Without a newline the record was never finished being written -- a torn tail.
            let Some(position) = bytes.iter().position(|byte| *byte == b'\n') else {
                return Ok(None);
            };
            let line_len = position + 1;
            return Ok(Some((line_len, strip_trailing_newline(&bytes[..line_len]))));
        };
    let rest = &bytes[header_prefix_len..];
    let (len_field, after_len) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, after_digest) = split_once_space(after_len)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    if after_digest.len() < declared_len {
        // Fewer bytes present than the record declares: the append was interrupted. That is a
        // torn tail to be truncated, not damage to a record that was completely written.
        return Ok(None);
    }
    let payload = &after_digest[..declared_len];
    let actual = checksum_of(payload);
    if actual.as_bytes() != digest_field {
        return Err(FramingError(
            "framed record digest does not match its payload".to_string(),
        ));
    }
    let header_len = bytes.len() - after_digest.len();
    let mut consumed = header_len + declared_len;
    if bytes.get(consumed) == Some(&b'\n') {
        consumed += 1; // the newline the encoder appends
    }
    Ok(Some((consumed, payload)))
}

/// Read one record from a stream, returning how many bytes it occupied and its payload.
///
/// The streaming twin of [`next_frame`], for the paths that face the largest logs there are:
/// memory stays bounded by the biggest single record rather than by the file.
pub(crate) fn read_frame<R: std::io::BufRead>(
    reader: &mut R,
) -> Result<Option<(usize, Vec<u8>)>, FramingError> {
    let longest_magic = FRAME_MAGIC_V1.len().max(FRAME_MAGIC_V2.len());
    let mut header = Vec::with_capacity(longest_magic + 32);
    let mut spaces = 0usize;
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Nothing at all is a clean end; a partial header is a torn tail.
                return Ok(None);
            }
            Err(err) => return Err(FramingError(err.to_string())),
        }
        header.push(byte[0]);
        if header.len() <= longest_magic {
            let still_v1 = FRAME_MAGIC_V1.starts_with(&header[..header.len().min(FRAME_MAGIC_V1.len())])
                && header.len() <= FRAME_MAGIC_V1.len();
            let still_v2 = FRAME_MAGIC_V2.starts_with(&header[..header.len().min(FRAME_MAGIC_V2.len())])
                && header.len() <= FRAME_MAGIC_V2.len();
            if header.as_slice() == FRAME_MAGIC_V1 || header.as_slice() == FRAME_MAGIC_V2 {
                spaces = 1; // the magic ends with the space that follows it
                continue;
            }
            if still_v1 || still_v2 {
                continue;
            }
            // Not a framed record: an unframed one runs to the next newline.
            return read_unframed(reader, header);
        }
        if byte[0] == b' ' {
            spaces += 1;
            if spaces == 3 {
                break;
            }
        }
    }
    let checksum_of: fn(&[u8]) -> String = if header.starts_with(FRAME_MAGIC_V2) {
        crc_digest_hex
    } else {
        sha256_digest_hex
    };
    let magic_len = if header.starts_with(FRAME_MAGIC_V2) {
        FRAME_MAGIC_V2.len()
    } else {
        FRAME_MAGIC_V1.len()
    };
    let rest = &header[magic_len..];
    let (len_field, after_len) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, _) = split_once_space(after_len)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    let digest_expected = digest_field.to_vec();
    let mut payload = vec![0u8; declared_len];
    if reader.read_exact(&mut payload).is_err() {
        // Fewer bytes than the record declares: the append was interrupted.
        return Ok(None);
    }
    let actual = checksum_of(&payload);
    if actual.as_bytes() != digest_expected.as_slice() {
        return Err(FramingError(
            "framed record digest does not match its payload".to_string(),
        ));
    }
    let mut trailing = [0u8; 1];
    let consumed_newline =
        matches!(reader.read_exact(&mut trailing), Ok(())) && trailing[0] == b'\n';
    let consumed = header.len() + declared_len + usize::from(consumed_newline);
    Ok(Some((consumed, payload)))
}

/// Finish reading a record that is not framed: it runs to the next newline, and without one it
/// was never finished being written.
fn read_unframed<R: std::io::BufRead>(
    reader: &mut R,
    mut started: Vec<u8>,
) -> Result<Option<(usize, Vec<u8>)>, FramingError> {
    if started.last() == Some(&b'\n') {
        let consumed = started.len();
        let payload = strip_trailing_newline(&started).to_vec();
        return Ok(Some((consumed, payload)));
    }
    let mut rest = Vec::new();
    let read = reader
        .read_until(b'\n', &mut rest)
        .map_err(|err| FramingError(err.to_string()))?;
    if read == 0 || !rest.ends_with(b"\n") {
        return Ok(None);
    }
    let consumed = started.len() + rest.len();
    started.extend_from_slice(&rest);
    let payload = strip_trailing_newline(&started).to_vec();
    Ok(Some((consumed, payload)))
}

fn strip_trailing_newline(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn split_once_space(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    bytes
        .iter()
        .position(|&byte| byte == b' ')
        .map(|index| (&bytes[..index], &bytes[index + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_round_trip_recovers_exact_payload() {
        let payload = br#"{"shard_id":5,"sequence":42,"value":"a b c"}"#;
        let framed = encode_line(payload);
        assert!(framed.ends_with(b"\n"));
        assert!(framed.starts_with(FRAME_MAGIC));
        assert_eq!(decode_line(&framed).unwrap(), payload);
    }

    #[test]
    fn legacy_unframed_line_is_returned_as_payload() {
        let legacy = b"{\"shard_id\":5,\"sequence\":1}\n";
        assert_eq!(decode_line(legacy).unwrap(), &legacy[..legacy.len() - 1]);
        // Without a trailing newline too.
        let legacy_no_nl = b"{\"shard_id\":5}";
        assert_eq!(decode_line(legacy_no_nl).unwrap(), legacy_no_nl);
    }

    #[test]
    fn value_preserving_bitflip_in_framed_payload_is_detected() {
        let payload = br#"{"shard_id":5,"sequence":42}"#;
        let mut framed = encode_line(payload);
        // Flip a digit in the payload's sequence so it STILL parses as valid JSON but no
        // longer matches the framed digest.
        let position = framed
            .windows(3)
            .position(|window| window == b":42")
            .expect("payload contains :42")
            + 2;
        framed[position] = b'9'; // 42 -> 49, still valid JSON
        let decoded = decode_line(&framed);
        assert!(
            decoded.is_err(),
            "a value-preserving bit-flip must fail the framed checksum"
        );
    }

    #[test]
    fn new_writes_use_the_crc_format() {
        let framed = encode_line(br#"{"a":1}"#);
        assert!(framed.starts_with(FRAME_MAGIC_V2));
    }

    #[test]
    fn v1_sha256_records_still_decode() {
        // A record written before the switch: same layout, SHA-256 prefix as the checksum.
        let payload = br#"{"shard_id":5,"sequence":42}"#;
        let digest = sha256_digest_hex(payload);
        let mut legacy = format!("#tsf1 {} {} ", payload.len(), digest).into_bytes();
        legacy.extend_from_slice(payload);
        legacy.push(b'\n');
        assert_eq!(decode_line(&legacy).unwrap(), payload);
    }

    #[test]
    fn a_corrupted_v1_record_is_still_rejected_after_the_switch() {
        let payload = br#"{"sequence":42}"#;
        let digest = sha256_digest_hex(payload);
        let mut legacy = format!("#tsf1 {} {} ", payload.len(), digest).into_bytes();
        legacy.extend_from_slice(br#"{"sequence":49}"#); // same length, different value
        legacy.push(b'\n');
        assert!(decode_line(&legacy).is_err());
    }

    #[test]
    fn both_formats_interleave_in_one_file() {
        // A GC rewrite spanning the upgrade produces exactly this.
        let payload = br#"{"k":"v"}"#;
        let v2 = encode_line(payload);
        let digest = sha256_digest_hex(payload);
        let mut v1 = format!("#tsf1 {} {} ", payload.len(), digest).into_bytes();
        v1.extend_from_slice(payload);
        v1.push(b'\n');
        for line in [v1, v2] {
            assert_eq!(decode_line(&line).unwrap(), payload);
        }
    }

    #[test]
    fn truncated_length_field_is_rejected() {
        let payload = br#"{"a":1}"#;
        let mut framed = encode_line(payload);
        // Corrupt the declared length so it no longer matches the payload length.
        let magic_len = FRAME_MAGIC.len();
        framed[magic_len] = b'9';
        assert!(decode_line(&framed).is_err());
    }

    #[test]
    fn a_reclaim_base_header_round_trips() {
        for base in [0u64, 1, 4096, u64::MAX] {
            let line = encode_base_header(base);
            assert!(line.starts_with(BASE_HEADER_MAGIC));
            assert!(line.ends_with(b"\n"));
            assert_eq!(decode_base_header(&line).unwrap(), Some(base));
        }
    }

    #[test]
    fn a_record_line_is_not_mistaken_for_a_base_header() {
        // The two must be distinguishable, or a reader would take a record for a base and
        // resolve every offset against garbage.
        let record = encode_line(br#"{"k":1}"#);
        assert_eq!(decode_base_header(&record).unwrap(), None);
        // A file that has never been reclaimed has no header at all, which reads as base zero.
        assert_eq!(decode_base_header(br#"{"k":1}"#).unwrap(), None);
    }

    #[test]
    fn a_corrupted_base_header_is_rejected_not_silently_zeroed() {
        // Falling back to zero on a corrupt base would resolve every stored offset to the
        // wrong record while looking like a clean read.
        let mut line = encode_base_header(8192);
        let position = line.len() - 2;
        line[position] ^= 0x01;
        assert!(decode_base_header(&line).is_err());
    }
}
