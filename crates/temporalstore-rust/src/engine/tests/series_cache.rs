// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{FeaturePoint, SequenceFeatureRow};

fn point(timestamp_ms: u64, value: Vec<u8>) -> FeaturePoint {
    FeaturePoint { timestamp_ms, value }
}

fn row(value: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms: value,
        gid: value,
        action_type: u32::MAX,
        duration: 0,
        author_id: u64::MAX,
    }
}

fn check(response: CommandResponse) {
    let encoded = encode_cached_response(&response).unwrap();
    assert_eq!(encoded, rmp_serde::to_vec_named(&response).unwrap());
    assert_eq!(decode_cached_response(&encoded), Some(response.clone()));
    assert_eq!(rmp_serde::from_slice::<CommandResponse>(&encoded).unwrap(), response);
}

#[test]
fn cached_feature_points_preserve_empty_binary_and_extreme_timestamps() {
    for size in [0, 1, 48, 4096] {
        check(CommandResponse::FeaturePoints {
            points: vec![point(0, vec![0xff; size]), point(u64::MAX, (0..=255).collect())],
        });
    }
    check(CommandResponse::FeaturePoints { points: Vec::new() });
}

#[test]
fn cached_feature_groups_preserve_order_empty_groups_and_repeated_keys() {
    check(CommandResponse::FeaturePointGroups {
        groups: vec![
            ("same".into(), vec![point(9, vec![0, 128, 255]), point(2, vec![])]),
            ("empty".into(), vec![]),
            ("same".into(), vec![point(9, vec![7])]),
        ],
    });
    check(CommandResponse::FeaturePointGroups { groups: Vec::new() });
}

#[test]
fn cached_sequence_shapes_preserve_all_columns() {
    check(CommandResponse::SequenceRows { rows: vec![row(0), row(u64::MAX)] });
    check(CommandResponse::SequenceRows { rows: Vec::new() });
    check(CommandResponse::SequenceRowGroups {
        groups: vec![("one".into(), vec![row(1), row(1)]), ("empty".into(), vec![])],
    });
    check(CommandResponse::SequenceRowGroups { groups: Vec::new() });
}

#[test]
fn cached_series_accept_reordered_fields_and_legacy_byte_arrays() {
    #[derive(serde::Serialize)]
    struct Reordered<'a> {
        points: Vec<FeaturePoint>,
        unused: u64,
        kind: &'a str,
    }
    let points = vec![point(1, vec![0, 128, 255])];
    let encoded = rmp_serde::to_vec_named(&Reordered {
        points: points.clone(), unused: 99, kind: "feature_points",
    }).unwrap();
    assert_eq!(decode_cached_response(&encoded), Some(CommandResponse::FeaturePoints { points: points.clone() }));
    let legacy = serde_json::json!({"kind":"feature_points", "points":[{"timestamp_ms":1,"value":[0,128,255]}]});
    assert_eq!(decode_cached_response(&rmp_serde::to_vec_named(&legacy).unwrap()),
        Some(CommandResponse::FeaturePoints { points }));
}

#[test]
fn cached_series_refuse_truncation_missing_payload_and_duplicate_fields() {
    let encoded = encode_cached_response(&CommandResponse::FeaturePoints {
        points: vec![point(1, vec![0, 128, 255])],
    }).unwrap();
    for end in 0..encoded.len() {
        assert!(decode_cached_response(&encoded[..end]).is_none(), "truncation at {end}");
    }
    for invalid in [
        serde_json::json!({"kind":"feature_points"}),
        serde_json::json!({"kind":"feature_points","points":false}),
        serde_json::json!({"kind":"feature_point_groups","groups":[["x",false]]}),
        serde_json::json!({"kind":"sequence_rows","rows":[{"timestamp_ms":1}]}),
        serde_json::json!({"kind":"sequence_row_groups","groups":false}),
        serde_json::json!({"kind":"unknown","points":[]}),
    ] {
        assert!(decode_cached_response(&rmp_serde::to_vec_named(&invalid).unwrap()).is_none());
    }
    let mut duplicate = vec![0x83];
    duplicate.extend(rmp_serde::to_vec(&"kind").unwrap());
    duplicate.extend(rmp_serde::to_vec(&"feature_points").unwrap());
    for _ in 0..2 {
        duplicate.extend(rmp_serde::to_vec(&"points").unwrap());
        duplicate.push(0x90);
    }
    assert!(decode_cached_response(&duplicate).is_none());
}

#[test]
fn cached_series_hits_reuse_the_existing_memory_entry_and_bad_entries_refill() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let key = CacheKey::string(1, "series-cache-test");
    let expected = CommandResponse::FeaturePoints { points: vec![point(7, vec![0xff; 48])] };
    cache.put_memory_only(key.clone(), encode_cached_response(&expected).unwrap());
    assert_eq!(cached_response(&cache, key.clone(), || panic!("hit must not read the source")), expected);
    cache.put_memory_only(key.clone(), vec![0xc1]);
    assert_eq!(cached_response(&cache, key.clone(), || expected.clone()), expected);
    assert_eq!(cached_response(&cache, key, || panic!("refill must be cached")), expected);
}

#[test]
fn cached_series_keep_existing_scalar_and_byte_response_decoders() {
    for response in [
        CommandResponse::Integer { value: i64::MIN },
        CommandResponse::Bytes { value: Some(vec![0, 128, 255]) },
        CommandResponse::Values { values: vec![None, Some(vec![]), Some(vec![255])] },
        CommandResponse::Members { members: vec![vec![], vec![0, 255]] },
        CommandResponse::Aggregate { value: 42 },
    ] { check(response); }
}

#[test]
fn read_block_keys_keep_the_canonical_bytes_at_address_and_routing_boundaries() {
    for shard in [1, 7] {
        for slab in [0, crate::engine::HOT_BLOCK_SLAB_ID, crate::wal_record::WAL_LOG_SLAB_ID, u64::from(u32::MAX) - 2] {
            for offset in [0, 1, 10, u64::from(u32::MAX)] {
                for length in [0, 1, 4096, u64::from(u32::MAX)] {
                    let address = crate::block_store::ElementEntry::from_parts(slab, offset, length, None, None);
                    for bucket in [None, Some(0), Some(u32::MAX)] {
                        assert_eq!(crate::engine::read_block_key(shard, &address, bucket),
                            CacheKey::page_with_slot(shard, address.block_slab_id(), address.offset(), address.length(), bucket));
                    }
                }
            }
        }
    }
}

#[test]
fn read_block_keys_share_one_prefix_without_reusing_selectors() {
    let first = crate::block_store::ElementEntry::from_parts(44, 10, 48, None, None);
    let second = crate::block_store::ElementEntry::from_parts(44, 11, 49, None, None);
    let a = crate::engine::read_block_key(1, &first, Some(3));
    let b = crate::engine::read_block_key(1, &second, Some(4));
    assert!(std::sync::Arc::ptr_eq(&a.record_key, &b.record_key));
    assert_ne!(a.selector, b.selector);
    assert_eq!(a, CacheKey::page_with_slot(1, 44, 10, 48, Some(3)));
    assert_eq!(b, CacheKey::page_with_slot(1, 44, 11, 49, Some(4)));
}

#[test]
fn read_block_keys_keep_shards_separate_and_only_retain_the_last_prefix() {
    let address = crate::block_store::ElementEntry::from_parts(19, 20, 21, None, None);
    let first = crate::engine::read_block_key(1, &address, None);
    for shard in [7, 1, 99] {
        assert_eq!(crate::engine::read_block_key(shard, &address, None),
            CacheKey::page_with_slot(shard, 19, 20, 21, None));
    }
    assert_eq!(first, CacheKey::page_with_slot(1, 19, 20, 21, None));
    crate::engine::READ_BLOCK_KEYS.with(|keys| {
        assert_eq!(keys.borrow().slab.as_ref().unwrap().0, (99, 19));
    });
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "isolated allocation measurement"]
fn measure_read_block_key_allocations() {
    let slab = crate::engine::HOT_BLOCK_SLAB_ID;
    let address = crate::block_store::ElementEntry::from_parts(slab, 12, 48, None, None);
    let _ = crate::engine::read_block_key(1, &address, Some(3));
    let mut measured = Vec::new();
    for reuse in [false, true] {
        let span = crate::alloc_probe::Probe::start();
        for _ in 0..256 {
            let key = if reuse { crate::engine::read_block_key(1, &address, Some(3)) }
                else { CacheKey::page_with_slot(1, slab, 12, 48, Some(3)) };
            std::hint::black_box(key);
        }
        let result = span.stop();
        measured.push(result);
    }
    assert!(measured[1].allocs < measured[0].allocs);
    assert!(measured[1].alloc_bytes < measured[0].alloc_bytes);
    for (reuse, result) in [false, true].into_iter().zip(measured) {
        println!("BLOCK_READ_KEY_COST reuse={reuse} calls=256 allocs={} bytes={}", result.allocs, result.alloc_bytes);
    }
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "isolated allocation measurement"]
fn measure_cached_series_decode_allocations() {
    for count in [1, 32, 256] {
        for width in [0, 48, 4096] {
            let response = CommandResponse::FeaturePoints {
                points: (0..count).map(|i| point(i, vec![255; width])).collect(),
            };
            let bytes = encode_cached_response(&response).unwrap();
            let mut measured = Vec::new();
            for direct in [false, true] {
                let span = crate::alloc_probe::Probe::start();
                for _ in 0..8 {
                    let answer = if direct { decode_cached_response(&bytes).unwrap() } else {
                        assert_eq!(rmp_serde::from_slice::<CachedKind<'_>>(&bytes).unwrap().kind, "feature_points");
                        rmp_serde::from_slice::<CommandResponse>(&bytes).unwrap()
                    };
                    std::hint::black_box(answer);
                }
                let measured_span = span.stop();
                measured.push(measured_span.alloc_bytes);
                println!("SERIES_CACHE_COST points={count} width={width} direct={direct} allocs={} bytes={}",
                    measured_span.allocs, measured_span.alloc_bytes);
            }
            assert!(measured[1] < measured[0]);
        }
    }
}
