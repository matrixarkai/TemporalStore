// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::state::WalResidentBlock;
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
enum Field {
    Key,
    Log,
    Sequence,
}

struct Column<'a> {
    entries: &'a [(&'a u64, &'a WalResidentBlock)],
    field: Field,
}

impl Serialize for Column<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut output = serializer.serialize_seq(Some(self.entries.len()))?;
        let mut previous = 0u64;
        for (&key, placement) in self.entries.iter().copied() {
            let value = match self.field {
                Field::Key => key,
                Field::Log => placement.log_id,
                Field::Sequence => placement.sequence,
            };
            let delta = match self.field {
                Field::Log => value - previous,
                _ => value.wrapping_sub(previous),
            };
            output.serialize_element(&delta)?;
            previous = value;
        }
        output.end()
    }
}

pub(super) fn serialize<S: Serializer>(
    values: &BTreeMap<u64, WalResidentBlock>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut ordered: Vec<_> = values.iter().collect();
    ordered.sort_unstable_by_key(|(key, placement)| (placement.log_id, placement.sequence, **key));
    // Stream three passes over one sorted handle array rather than allocate three value arrays.
    (
        Column {
            entries: &ordered,
            field: Field::Key,
        },
        Column {
            entries: &ordered,
            field: Field::Log,
        },
        Column {
            entries: &ordered,
            field: Field::Sequence,
        },
    )
        .serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<u64, WalResidentBlock>, D::Error> {
    struct Read;
    impl<'de> Visitor<'de> for Read {
        type Value = BTreeMap<u64, WalResidentBlock>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("three locator delta columns or a legacy locator map")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, placement)) = map.next_entry()? {
                if result.insert(key, placement).is_some() {
                    return Err(A::Error::custom("duplicate WAL locator key"));
                }
            }
            Ok(result)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let keys: Vec<u64> = seq
                .next_element()?
                .ok_or_else(|| A::Error::custom("missing locator keys"))?;
            let logs: Vec<u64> = seq
                .next_element()?
                .ok_or_else(|| A::Error::custom("missing locator logs"))?;
            let sequences: Vec<u64> = seq
                .next_element()?
                .ok_or_else(|| A::Error::custom("missing locator sequences"))?;
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(A::Error::custom("extra locator column"));
            }
            if keys.len() != logs.len() || keys.len() != sequences.len() {
                return Err(A::Error::custom("locator column lengths differ"));
            }
            let mut result = BTreeMap::new();
            let (mut key, mut log, mut sequence) = (0u64, 0u64, 0u64);
            for ((key_delta, log_delta), sequence_delta) in
                keys.into_iter().zip(logs).zip(sequences)
            {
                key = key.wrapping_add(key_delta);
                log = log
                    .checked_add(log_delta)
                    .ok_or_else(|| A::Error::custom("locator log overflow"))?;
                sequence = sequence.wrapping_add(sequence_delta);
                if result
                    .insert(
                        key,
                        WalResidentBlock {
                            log_id: log,
                            sequence,
                        },
                    )
                    .is_some()
                {
                    return Err(A::Error::custom("duplicate WAL locator key"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_any(Read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Image(#[serde(with = "super")] BTreeMap<u64, WalResidentBlock>);

    #[test]
    fn locator_columns_stream_the_exact_three_column_wire_shape() {
        let image = Image(BTreeMap::from([
            (
                7,
                WalResidentBlock {
                    log_id: 90,
                    sequence: 3,
                },
            ),
            (
                9,
                WalResidentBlock {
                    log_id: 90,
                    sequence: 3,
                },
            ),
        ]));
        let expected = (vec![7u64, 2], vec![90u64, 0], vec![3u64, 0]);
        assert_eq!(
            rmp_serde::to_vec_named(&image).unwrap(),
            rmp_serde::to_vec_named(&expected).unwrap()
        );
        assert_eq!(
            serde_json::to_vec(&image).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
    }

    #[test]
    fn locator_columns_round_trip_extremes_shared_records_and_sequence_resets() {
        let values = Image(BTreeMap::from([
            (
                0,
                WalResidentBlock {
                    log_id: 0,
                    sequence: u64::MAX,
                },
            ),
            (
                u64::MAX,
                WalResidentBlock {
                    log_id: 0,
                    sequence: 0,
                },
            ),
            (
                1,
                WalResidentBlock {
                    log_id: 8,
                    sequence: 3,
                },
            ),
            (
                9,
                WalResidentBlock {
                    log_id: 8,
                    sequence: 3,
                },
            ),
            (
                2,
                WalResidentBlock {
                    log_id: u64::MAX,
                    sequence: 1,
                },
            ),
        ]));
        let bytes = rmp_serde::to_vec_named(&values).unwrap();
        assert_eq!(rmp_serde::from_slice::<Image>(&bytes).unwrap(), values);
        let json = serde_json::to_vec(&values).unwrap();
        assert_eq!(serde_json::from_slice::<Image>(&json).unwrap(), values);
        let empty = Image(BTreeMap::new());
        assert_eq!(serde_json::from_str::<Image>("[[],[],[]]").unwrap(), empty);
        assert_eq!(
            rmp_serde::from_slice::<Image>(&rmp_serde::to_vec_named(&empty).unwrap()).unwrap(),
            empty
        );
    }

    #[test]
    fn locator_columns_read_legacy_maps_and_refuse_malformed_columns() {
        let legacy = r#"{"7":{"log_id":90,"sequence":3},"8":{"log_id":90,"sequence":3}}"#;
        let decoded: Image = serde_json::from_str(legacy).unwrap();
        assert_eq!(decoded.0.len(), 2);
        assert_eq!(
            decoded.0[&7],
            WalResidentBlock {
                log_id: 90,
                sequence: 3
            }
        );
        let legacy_binary = rmp_serde::to_vec_named(&decoded.0).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<Image>(&legacy_binary).unwrap(),
            decoded
        );
        for invalid in [
            "[]",
            "[[]]",
            "[[],[]]",
            "[[],[],[],[]]",
            "[[1],[],[1]]",
            "[[1,0],[1,0],[1,0]]",
            "[[1,2],[18446744073709551615,1],[1,1]]",
            r#"{"7":{"log_id":1,"sequence":1},"7":{"log_id":2,"sequence":2}}"#,
        ] {
            assert!(
                serde_json::from_str::<Image>(invalid).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn locator_columns_round_trip_many_distinct_records_and_aliases() {
        let mut placements = BTreeMap::new();
        let mut key = 7u64;
        for i in 0..16_384u64 {
            key ^= key << 13;
            key ^= key >> 7;
            key ^= key << 17;
            assert!(placements
                .insert(
                    key,
                    WalResidentBlock {
                        log_id: (i / 8) * 513,
                        sequence: if i % 3 == 0 { u64::MAX - i } else { i / 8 },
                    }
                )
                .is_none());
        }
        let image = Image(placements);
        let bytes = rmp_serde::to_vec_named(&image).unwrap();
        assert_eq!(rmp_serde::from_slice::<Image>(&bytes).unwrap(), image);
    }

    #[test]
    fn locator_columns_read_previous_container_without_replaying_its_log() {
        use crate::engine::{
            decode_index_bytes, INDEX_BINARY_VERSION_BYTES, INDEX_CODEC_ZSTD_MSGPACK_LEGACY,
            INDEX_CONTAINER_MAGIC, SHARD_INDEX_FORMAT_VERSION,
        };
        struct Legacy {
            fields: serde_json::Map<String, serde_json::Value>,
            placements: BTreeMap<u64, WalResidentBlock>,
        }
        impl Serialize for Legacy {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(self.fields.len()))?;
                for (key, value) in &self.fields {
                    if key == "wal_resident_blocks" {
                        map.serialize_entry(key, &self.placements)?;
                    } else {
                        map.serialize_entry(key, value)?;
                    }
                }
                map.end()
            }
        }
        let fields = serde_json::to_value(crate::engine::state::ShardState::default())
            .unwrap()
            .as_object()
            .unwrap()
            .clone();
        let placements = BTreeMap::from([(
            7,
            WalResidentBlock {
                log_id: 90,
                sequence: 3,
            },
        )]);
        let fixture = Legacy {
            fields,
            placements: placements.clone(),
        };
        let raw = rmp_serde::to_vec_named(&fixture).unwrap();
        let mut image = INDEX_CONTAINER_MAGIC.to_vec();
        image.push(INDEX_CODEC_ZSTD_MSGPACK_LEGACY);
        assert_eq!(INDEX_BINARY_VERSION_BYTES, 4);
        image.extend_from_slice(&SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
        image.extend(zstd::stream::encode_all(raw.as_slice(), 3).unwrap());
        let restored = decode_index_bytes(&image).unwrap();
        assert_eq!(restored.wal_resident_blocks, placements);
        let current = crate::engine::encode_index_bytes(&restored);
        assert_ne!(
            current[INDEX_CONTAINER_MAGIC.len()],
            INDEX_CODEC_ZSTD_MSGPACK_LEGACY
        );
        assert_eq!(
            decode_index_bytes(&current).unwrap().wal_resident_blocks,
            placements
        );
    }
}
