// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encoding for byte payloads inside a JSON document.
//!
//! A JSON document has no byte type, so a `Vec<u8>` serialized as a sequence becomes an array of
//! decimal numbers -- three or four characters for every byte. Base64 is a flat 1.33x instead,
//! whatever the bytes are.
//!
//! Both shapes decode. Anything written before this still loads, and a client that sends the array
//! shape is still understood.


use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::Serializer;

thread_local! {
}

thread_local! {
    /// Payloads pulled out of the document being written, in the order they were met.
    ///
    /// Only the log sets this, and only while it is encoding one record. Everywhere else -- a
    /// request on the wire, a response to a client -- the payload stays in the document, so nothing
    /// outside the log sees a different shape.
    static CARRIED: std::cell::RefCell<Option<Vec<Vec<u8>>>> = const { std::cell::RefCell::new(None) };
    /// Payloads read out of a record, for the document to refer back to.
    static AVAILABLE: std::cell::RefCell<Option<Vec<Vec<u8>>>> = const { std::cell::RefCell::new(None) };
}

/// Marks a payload that lives beside the document. Base64 never produces a leading `~`, so a
/// marker can never be mistaken for an encoded payload, in either direction.
const CARRIED_PREFIX: char = '~';

/// Collect the payloads of whatever `write` serializes, instead of encoding them into the document.
///
/// Returns them in the order they were met, which is the order a reader meets the markers.
pub(crate) fn carrying_payloads<T>(write: impl FnOnce() -> T) -> (T, Vec<Vec<u8>>) {
    CARRIED.with(|carried| *carried.borrow_mut() = Some(Vec::new()));
    let outcome = write();
    let payloads = CARRIED
        .with(|carried| carried.borrow_mut().take())
        .unwrap_or_default();
    (outcome, payloads)
}

/// Resolve payload markers while `read` deserializes.
pub(crate) fn with_payloads<T>(payloads: Vec<Vec<u8>>, read: impl FnOnce() -> T) -> T {
    AVAILABLE.with(|available| *available.borrow_mut() = Some(payloads));
    let outcome = read();
    AVAILABLE.with(|available| *available.borrow_mut() = None);
    outcome
}

/// What escaping this payload would cost, so the cheaper of the two can be chosen on the bytes
/// rather than on a guess. Newline and the escape byte each become two.
fn escaped_len(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .map(|byte| usize::from(*byte == b'\n' || *byte == ESCAPE) + 1)
        .sum()
}

pub(crate) const ESCAPE: u8 = 0x1b;

/// Remove newlines from a payload so it can sit in a newline-delimited log.
pub(crate) fn escape_payload(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(escaped_len(bytes));
    for byte in bytes {
        match *byte {
            b'\n' => out.extend_from_slice(&[ESCAPE, b'n']),
            ESCAPE => out.extend_from_slice(&[ESCAPE, ESCAPE]),
            other => out.push(other),
        }
    }
    out
}

/// Put back what [`escape_payload`] took out.
pub(crate) fn unescape_payload(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut following_escape = false;
    for byte in bytes {
        if following_escape {
            match *byte {
                b'n' => out.push(b'\n'),
                ESCAPE => out.push(ESCAPE),
                other => return Err(format!("payload has an unknown escape: {other:#04x}")),
            }
            following_escape = false;
        } else if *byte == ESCAPE {
            following_escape = true;
        } else {
            out.push(*byte);
        }
    }
    if following_escape {
        return Err("payload ends inside an escape".to_string());
    }
    Ok(out)
}

/// Write a byte payload in the encoded shape.
///
/// There used to be a second shape -- an array of JSON numbers, three to four characters per
/// byte -- behind TS_LEGACY_ARRAY_BYTES, for a consumer reading records directly that had not
/// moved across. It was measured against this one at 200 records per size, both shapes in one
/// process, alternating so a busy machine landed on both:
///
/// | value | array | encoded | smaller | faster |
/// |---|---|---|---|---|
/// | 64 B | 72,892 B (5.69x user) | 35,892 B (2.80x user) | 2.03x | 1.03x |
/// | 256 B | 227,377 B (4.44x user) | 74,492 B (1.45x user) | 3.05x | 1.43x |
/// | 1024 B | 845,367 B (4.13x user) | 229,284 B (1.12x user) | 3.69x | 2.95x |
/// | 4096 B | 3,705,054 B (4.52x user) | 845,493 B (1.03x user) | 4.38x | 3.83x |
///
/// The flag is retired and only this shape is written. Decoding never consulted it and accepts
/// either shape by inspection, so a store written with the array shape still reads back -- what
/// is gone is the ability to produce more of it.
pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    // Carried beside the document when that is smaller. Escaping costs whatever newlines weigh in
    // these particular bytes -- about 1% for most payloads -- against base64's flat third. A
    // payload that is mostly newlines would grow, so it stays encoded.
    let marker = CARRIED.with(|carried| {
        let mut carried = carried.borrow_mut();
        let Some(payloads) = carried.as_mut() else {
            return None;
        };
        if escaped_len(bytes) >= bytes.len().div_ceil(3) * 4 {
            return None;
        }
        payloads.push(bytes.to_vec());
        Some(format!("{CARRIED_PREFIX}{}", payloads.len() - 1))
    });
    match marker {
        Some(marker) => serializer.serialize_str(&marker),
        None => serializer.serialize_str(&STANDARD.encode(bytes)),
    }
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    deserializer.deserialize_any(EitherShape)
}

/// Accepts the encoded string, or the array of numbers written before it.
pub(crate) struct EitherShape;

impl<'de> Visitor<'de> for EitherShape {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("encoded bytes, or the array of bytes written before")
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
        // A marker naming a payload carried beside the document. Base64 never starts with this,
        // so there is no shape a payload could take that would be read as the wrong one.
        if let Some(index) = value.strip_prefix(CARRIED_PREFIX) {
            let index: usize = index
                .parse()
                .map_err(|_| E::custom(format!("payload marker is not a number: {value}")))?;
            return AVAILABLE.with(|available| {
                let available = available.borrow();
                let payloads = available.as_ref().ok_or_else(|| {
                    E::custom(format!(
                        "record refers to a payload carried beside it, but none were read: {value}"
                    ))
                })?;
                payloads.get(index).cloned().ok_or_else(|| {
                    E::custom(format!(
                        "record refers to payload {index}, and only {} were read",
                        payloads.len()
                    ))
                })
            });
        }
        STANDARD
            .decode(value)
            .map_err(|err| E::custom(format!("bytes are not valid base64: {err}")))
    }

    fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(value.to_vec())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(bytes)
    }
}

/// The same encoding for a list of (name, bytes) pairs.
pub mod pairs {
    use super::*;

    struct Bytes(Vec<u8>);

    impl<'de> serde::Deserialize<'de> for Bytes {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_any(EitherShape).map(Bytes)
        }
    }

    pub fn serialize<S: Serializer>(
        pairs: &[(String, Vec<u8>)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(pairs.len()))?;
        for (name, bytes) in pairs {
            seq.serialize_element(&(name, STANDARD.encode(bytes)))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<(String, Vec<u8>)>, D::Error> {
        let pairs = <Vec<(String, Bytes)> as serde::Deserialize>::deserialize(deserializer)?;
        Ok(pairs
            .into_iter()
            .map(|(name, bytes)| (name, bytes.0))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Command;

    /// A record written before this encoding existed still loads.
    ///
    /// This is the whole migration story: nothing rewrites logs, so the reader has to accept both
    /// shapes forever. The array shape below is exactly what the previous code emitted.
    #[test]
    fn the_array_shape_written_before_still_loads() {
        let legacy = r#"{"kind":"string_set","key":"k","value":[104,105]}"#;
        let decoded: Command = serde_json::from_str(legacy).expect("the array shape must load");
        assert_eq!(
            decoded,
            Command::StringSet {
                key: "k".to_string(),
                value: b"hi".to_vec(),
            }
        );
    }

    /// ...including inside a list of pairs, which encodes separately.
    #[test]
    fn the_array_shape_still_loads_inside_pairs() {
        let legacy = r#"{"kind":"hash_multi_set","key":"k","entries":[["f",[104,105]]]}"#;
        let decoded: Command = serde_json::from_str(legacy).expect("the array shape must load");
        assert_eq!(
            decoded,
            Command::HashMultiSet {
                key: "k".to_string(),
                entries: vec![("f".to_string(), b"hi".to_vec())],
            }
        );
    }

    /// Every payload survives the round trip byte for byte, including bytes that are not text.
    #[test]
    fn every_byte_survives_the_round_trip() {
        let all_bytes: Vec<u8> = (0..=255u8).collect();
        for command in [
            Command::StringSet {
                key: "k".to_string(),
                value: all_bytes.clone(),
            },
            Command::HashMultiSet {
                key: "k".to_string(),
                entries: vec![("f".to_string(), all_bytes.clone())],
            },
            Command::SetAdd {
                key: "k".to_string(),
                member: all_bytes.clone(),
            },
        ] {
            let encoded = serde_json::to_vec(&command).unwrap();
            let decoded: Command = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, command, "payload changed across the round trip");
        }
    }
}
