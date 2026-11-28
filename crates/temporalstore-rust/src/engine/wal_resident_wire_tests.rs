// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::{ShardState, WalResidentBlock};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct LegacyCoordinates {
    log_id: u64,
    sequence: u64,
}

#[test]
fn writes_ordered_pairs_readable_by_the_existing_struct_decoder() {
    for (log_id, sequence) in [(0, 0), (0, u64::MAX), (u64::MAX, 0), (u64::MAX, u64::MAX)] {
        let value = WalResidentBlock { log_id, sequence };
        let json = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&json).unwrap(),
            serde_json::json!([log_id, sequence])
        );
        let expected = LegacyCoordinates { log_id, sequence };
        assert_eq!(
            serde_json::from_slice::<LegacyCoordinates>(&json).unwrap(),
            expected
        );
        let packed = rmp_serde::to_vec_named(&value).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<LegacyCoordinates>(&packed).unwrap(),
            expected
        );
    }
}

#[test]
fn reads_both_named_legacy_and_compact_coordinates() {
    let legacy = LegacyCoordinates {
        log_id: 321,
        sequence: 777,
    };
    let expected = WalResidentBlock {
        log_id: 321,
        sequence: 777,
    };
    assert_eq!(
        serde_json::from_slice::<WalResidentBlock>(&serde_json::to_vec(&legacy).unwrap()).unwrap(),
        expected
    );
    assert_eq!(
        rmp_serde::from_slice::<WalResidentBlock>(&rmp_serde::to_vec_named(&legacy).unwrap())
            .unwrap(),
        expected
    );
    assert_eq!(
        serde_json::from_slice::<WalResidentBlock>(b"[321,777]").unwrap(),
        expected
    );
}

#[test]
fn refuses_missing_extra_or_invalid_coordinates() {
    for bytes in [
        b"[]".as_slice(),
        b"[1]",
        b"[1,2,3]",
        b"[1,-1]",
        b"[null,2]",
        br#"{"log_id":1}"#,
        br#"{"log_id":1,"sequence":null}"#,
        br#"{"log_id":1,"log_id":2,"sequence":3}"#,
    ] {
        assert!(
            serde_json::from_slice::<WalResidentBlock>(bytes).is_err(),
            "{bytes:?}"
        );
    }
    assert!(
        rmp_serde::from_slice::<WalResidentBlock>(&rmp_serde::to_vec(&[1_u64]).unwrap()).is_err()
    );
    assert!(
        rmp_serde::from_slice::<WalResidentBlock>(&rmp_serde::to_vec(&[1_u64, 2, 3]).unwrap())
            .is_err()
    );
}

#[test]
fn registry_saves_repeated_names_without_losing_any_coordinates() {
    const COUNT: usize = 8_000;
    let current: BTreeMap<_, _> = (0..COUNT)
        .map(|i| {
            let key = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            (
                key,
                WalResidentBlock {
                    log_id: i as u64 * 271,
                    sequence: i as u64 + 1,
                },
            )
        })
        .collect();
    let legacy: BTreeMap<_, _> = current
        .iter()
        .map(|(key, value)| {
            (
                *key,
                LegacyCoordinates {
                    log_id: value.log_id,
                    sequence: value.sequence,
                },
            )
        })
        .collect();
    let old_json = serde_json::to_vec(&legacy).unwrap();
    let json = serde_json::to_vec(&current).unwrap();
    let old_packed = rmp_serde::to_vec_named(&legacy).unwrap();
    let packed = rmp_serde::to_vec_named(&current).unwrap();
    assert_eq!(old_json.len() - json.len(), 20 * COUNT);
    assert_eq!(old_packed.len() - packed.len(), 16 * COUNT);
    assert_eq!(
        serde_json::from_slice::<BTreeMap<u64, LegacyCoordinates>>(&json).unwrap(),
        legacy
    );
    assert_eq!(
        rmp_serde::from_slice::<BTreeMap<u64, LegacyCoordinates>>(&packed).unwrap(),
        legacy
    );
    assert_eq!(
        rmp_serde::from_slice::<BTreeMap<u64, WalResidentBlock>>(&packed).unwrap(),
        current
    );
    println!(
        "rows={COUNT} json_bytes={}=>{} packed_bytes={}=>{}",
        old_json.len(),
        json.len(),
        old_packed.len(),
        packed.len()
    );
}

#[test]
fn full_index_codec_keeps_registry_coordinates() {
    let mut state = ShardState::default();
    state.wal_resident_blocks.insert(
        41,
        WalResidentBlock {
            log_id: 321,
            sequence: 777,
        },
    );
    state.wal_resident_blocks.insert(
        42,
        WalResidentBlock {
            log_id: 654,
            sequence: 999,
        },
    );
    let bytes = crate::engine::encode_index_bytes(&state);
    let restored = crate::engine::decode_index_bytes(&bytes).unwrap();
    assert_eq!(restored.wal_resident_blocks, state.wal_resident_blocks);
}
