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

/// The byte a binary-safe name is tagged with (see `binary_safe_name`).
pub(crate) const ESCAPE: u8 = 0x1b;

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
    serializer.serialize_str(&STANDARD.encode(bytes))
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    deserializer.deserialize_any(EitherShape)
}

/// Accepts the encoded string, or the array of numbers written before it.
/// The same payload, for a BINARY format rather than a JSON document.
///
/// The parent module exists because a JSON document has no byte type, so it base64s. A binary format
/// has one, and base64 into it costs a flat third for nothing: a 32-byte digest becomes 44
/// characters where a msgpack `bin` string is 34 bytes. Measured by
/// `what_the_durable_byte_encodings_cost`.
///
/// BOTH CODECS LIVE HERE SO THE CHOICE IS VISIBLE. The index-log anchor reached for the base64 one
/// on a msgpack path because it was the only one in sight, and paid twelve bytes an anchor for it.
///
/// # IT READS EVERY SPELLING THIS REPO HAS EVER WRITTEN
///
/// `deserialize` delegates to [`EitherShape`], the parent module's visitor, which already accepts
/// `visit_bytes` (this spelling), `visit_str` (base64) and `visit_seq` (the oldest array-of-integers
/// shape). So a payload already on disk still loads, and -- the direction that usually bites -- an
/// OLDER build reads the new spelling too, through the same `visit_bytes` it has had since the
/// parent module was introduced. A format change whose old reader cannot read the new bytes needs a
/// version gate; this one does not.
pub(crate) mod binary {
    pub fn serialize<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        // REUSED RATHER THAN REWRITTEN. The parent's visitor already takes all three spellings, so
        // a second one here would be a second place for them to drift apart.
        deserializer.deserialize_any(super::EitherShape)
    }
}

pub(crate) mod borrowed_binary {
    use super::*;
    use std::borrow::Cow;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Cow<'de, [u8]>, D::Error> {
        deserializer.deserialize_any(BorrowedOrOwned)
    }

    struct BorrowedOrOwned;

    impl<'de> Visitor<'de> for BorrowedOrOwned {
        type Value = Cow<'de, [u8]>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            EitherShape.expecting(formatter)
        }

        fn visit_borrowed_bytes<E: Error>(self, value: &'de [u8]) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(value))
        }

        fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
            Ok(Cow::Owned(value.to_vec()))
        }

        fn visit_byte_buf<E: Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
            Ok(Cow::Owned(value))
        }

        fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
            EitherShape.visit_str(value).map(Cow::Owned)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            EitherShape.visit_seq(seq).map(Cow::Owned)
        }
    }
}

pub(crate) struct EitherShape;

impl<'de> Visitor<'de> for EitherShape {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("encoded bytes, or the array of bytes written before")
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
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

    /// One value's base64, written straight into the output.
    ///
    /// `STANDARD.encode` allocates a fresh `String` for every entry only to hand it to the
    /// serializer and drop it -- one allocation per pair, sized 1.33x the value. `collect_str`
    /// lets the format write the encoding out as it is produced. The bytes on the wire are
    /// identical, same alphabet and same padding, which
    /// `the_pairs_encoding_is_what_encode_produced` holds for every remainder class.
    struct Base64Value<'a>(&'a [u8]);

    impl serde::Serialize for Base64Value<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&base64::display::Base64Display::new(self.0, &STANDARD))
        }
    }

    pub fn serialize<S: Serializer>(
        pairs: &[(String, Vec<u8>)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(pairs.len()))?;
        for (name, bytes) in pairs {
            seq.serialize_element(&(name, Base64Value(bytes)))?;
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

#[cfg(test)]
mod pairs_encoding_tests {
    use super::pairs;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    /// The encoding is byte for byte what `STANDARD.encode` produced.
    ///
    /// Values stream through `collect_str` now instead of being encoded into a `String` first.
    /// Base64 pads by remainder, so every remainder class is covered -- 0, 1 and 2 bytes over a
    /// multiple of three -- plus the empty value and the full byte range.
    #[test]
    fn the_pairs_encoding_is_what_encode_produced() {
        let cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"a".to_vec(),
            b"ab".to_vec(),
            b"abc".to_vec(),
            b"abcd".to_vec(),
            (0u8..=255).collect(),
            br#"{"body":"a message","record_type":"raw_event"}"#.to_vec(),
        ];
        for value in cases {
            let input = vec![("field".to_string(), value.clone())];
            let mut out = Vec::new();
            {
                let mut ser = serde_json::Serializer::new(&mut out);
                pairs::serialize(&input, &mut ser).expect("the pairs serialise");
            }
            let expected = serde_json::to_vec(&vec![("field", STANDARD.encode(&value))])
                .expect("the reference form serialises");
            assert_eq!(
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&expected),
                "the encoding moved for a {}-byte value",
                value.len()
            );
        }
    }

    /// And a value still survives the round trip.
    #[test]
    fn the_pairs_encoding_round_trips() {
        let input: Vec<(String, Vec<u8>)> = vec![
            ("a".to_string(), Vec::new()),
            ("b".to_string(), (0u8..=255).collect()),
            ("c".to_string(), b"abcd".to_vec()),
        ];
        let mut out = Vec::new();
        {
            let mut ser = serde_json::Serializer::new(&mut out);
            pairs::serialize(&input, &mut ser).expect("serialises");
        }
        let mut de = serde_json::Deserializer::from_slice(&out);
        let back = pairs::deserialize(&mut de).expect("deserialises");
        assert_eq!(back, input, "a value did not survive the round trip");
    }

}
