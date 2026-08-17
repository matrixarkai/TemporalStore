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
//! Framed line layout (single line, `\n`-terminated on disk):
//! `#tsf1 <payload_len> <digest_hex> <payload_json>\n`
//! The payload JSON is compact serde output, so it contains neither a literal `\n` (line
//! boundary) nor is its boundary ambiguous: the first two space-delimited fields after the
//! magic are the length and digest, and everything after the third space is the payload
//! (which may itself contain spaces inside JSON string values).

use sha2::{Digest, Sha256};

/// ASCII framing prefix, including the trailing space that delimits the fields after it.
/// Chosen so it can never collide with a legacy record: a serialized JSON object always
/// starts with `{`, never `#`.
pub(crate) const FRAME_MAGIC: &[u8] = b"#tsf1 ";

/// Number of leading SHA-256 bytes retained in the framed digest. 8 bytes (a 64-bit digest,
/// 16 hex chars) is ample to catch accidental corruption of a committed record.
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

fn digest_hex(payload: &[u8]) -> String {
    let full = Sha256::digest(payload);
    hex::encode(&full[..DIGEST_BYTES])
}

/// Encode `payload` (a single-line JSON document with NO trailing newline) as a framed,
/// newline-terminated log line.
pub(crate) fn encode_line(payload: &[u8]) -> Vec<u8> {
    let digest = digest_hex(payload);
    let header = format!("#tsf1 {} {} ", payload.len(), digest);
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
    if !line.starts_with(FRAME_MAGIC) {
        // Legacy unframed record (or a blank line): the whole line is the payload.
        return Ok(line);
    }
    let rest = &line[FRAME_MAGIC.len()..];
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
    let actual = digest_hex(payload);
    if actual.as_bytes() != digest_field {
        return Err(FramingError(format!(
            "framed record checksum mismatch: declared {}, actual {actual}",
            String::from_utf8_lossy(digest_field)
        )));
    }
    Ok(payload)
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
    fn truncated_length_field_is_rejected() {
        let payload = br#"{"a":1}"#;
        let mut framed = encode_line(payload);
        // Corrupt the declared length so it no longer matches the payload length.
        let magic_len = FRAME_MAGIC.len();
        framed[magic_len] = b'9';
        assert!(decode_line(&framed).is_err());
    }
}
