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

use std::cell::Cell;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::Serializer;

thread_local! {
    /// 0 = not yet resolved, 1 = encoded, 2 = the array shape.
    ///
    /// Per thread, not per process. Serialization happens on the thread doing the work, so this is
    /// the right scope -- and it means a test that drives the shape cannot change what every other
    /// test in the process writes while it runs.
    static SHAPE: Cell<u8> = const { Cell::new(0) };
}

/// TS_LEGACY_ARRAY_BYTES: write byte payloads as an array of numbers, as they were written before.
///
/// Default off. The array shape costs three to four characters per byte; the escape hatch exists
/// for a consumer that reads records directly and has not moved to the encoded shape.
fn writes_the_array_shape() -> bool {
    SHAPE.with(|shape| match shape.get() {
        1 => false,
        2 => true,
        _ => {
            let legacy = matches!(
                std::env::var("TS_LEGACY_ARRAY_BYTES")
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "true" | "yes" | "on"
            );
            shape.set(if legacy { 2 } else { 1 });
            legacy
        }
    })
}

/// Choose the shape for THIS THREAD. Exists so the two can be compared in one run; the
/// environment variable is the supported way to set it.
pub fn set_array_shape_for_measurement(array_shape: bool) {
    SHAPE.with(|shape| shape.set(if array_shape { 2 } else { 1 }));
}

pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    if writes_the_array_shape() {
        let mut seq = serializer.serialize_seq(Some(bytes.len()))?;
        for byte in bytes {
            seq.serialize_element(byte)?;
        }
        return seq.end();
    }
    serializer.serialize_str(&STANDARD.encode(bytes))
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
            if super::writes_the_array_shape() {
                seq.serialize_element(&(name, bytes))?;
            } else {
                seq.serialize_element(&(name, STANDARD.encode(bytes)))?;
            }
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

    /// The escape hatch really does restore the old shape, and it still reads back.
    #[test]
    fn the_escape_hatch_restores_the_array_shape() {
        let command = Command::StringSet {
            key: "k".to_string(),
            value: b"hi".to_vec(),
        };
        super::set_array_shape_for_measurement(true);
        let as_array = serde_json::to_string(&command).unwrap();
        super::set_array_shape_for_measurement(false);
        let as_encoded = serde_json::to_string(&command).unwrap();

        assert!(
            as_array.contains("[104,105]"),
            "the escape hatch should write the array shape, got {as_array}"
        );
        assert!(
            !as_encoded.contains("[104,105]"),
            "the default should not write the array shape, got {as_encoded}"
        );
        assert!(
            as_encoded.len() < as_array.len(),
            "the default should be the smaller of the two"
        );
        for shape in [as_array, as_encoded] {
            let decoded: Command = serde_json::from_str(&shape).unwrap();
            assert_eq!(decoded, command);
        }
    }
}
