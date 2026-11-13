// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn borrowed_cache_payload_points_into_the_encoded_response() {
    for size in [1, 32, 4096, 65536] {
        let value = vec![0xff; size];
        let encoded = encode_cached_response(&CommandResponse::Bytes { value: Some(value.clone()) }).unwrap();
        let borrowed = decode_borrowed_cached_bytes(&encoded).unwrap().unwrap();
        assert_eq!(borrowed, value);
        let start = encoded.as_ptr() as usize;
        assert!((start..start + encoded.len()).contains(&(borrowed.as_ptr() as usize)));
    }
}

#[test]
fn borrowed_cache_preserves_absent_and_empty_values() {
    for value in [None, Some(Vec::new())] {
        let encoded = encode_cached_response(&CommandResponse::Bytes { value: value.clone() }).unwrap();
        assert_eq!(decode_borrowed_cached_bytes(&encoded), Some(value.as_deref()));
    }
}

#[test]
fn borrowed_cache_hits_consume_the_shared_buffer_without_a_source_read() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let key = CacheKey::hash(1, "row", "timestamp");
    cache.put_memory_only(key.clone(), encode_cached_response(
        &CommandResponse::Bytes { value: Some(vec![9; 32]) }).unwrap());
    let encoded = cache.get_shared(&key).unwrap().unwrap();
    let expected = decode_borrowed_cached_bytes(&encoded).unwrap().unwrap();
    let count = cached_bytes_with(&cache, key, || panic!("hot bytes must not read storage"), |value| {
        let value = value.unwrap();
        assert_eq!(value.as_ptr(), expected.as_ptr());
        value.len()
    });
    assert_eq!(count, 32);
}

#[test]
fn borrowed_cache_misses_fill_the_existing_response_format() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let key = CacheKey::hash(1, "row", "timestamp");
    assert_eq!(cached_bytes_with(&cache, key.clone(), || Some(vec![7; 8]), |value| value.unwrap().len()), 8);
    let bytes = cache.get_shared(&key).unwrap().unwrap();
    assert!(matches!(decode_cached_response(&bytes), Some(CommandResponse::Bytes { value: Some(value) }) if value == vec![7; 8]));
}

#[test]
fn borrowed_cache_retains_the_legacy_integer_array_decoder() {
    #[derive(serde::Serialize)]
    struct OldBytes { kind: &'static str, value: Option<Vec<u8>> }
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    for value in [None, Some(Vec::new()), Some(vec![0, 128, 255])] {
        let key = CacheKey::hash(1, "row", "timestamp");
        let mut encoded = Vec::new();
        serde::Serialize::serialize(&OldBytes { kind: "bytes", value: value.clone() },
            &mut rmp_serde::Serializer::new(&mut encoded).with_struct_map()).unwrap();
        cache.put_memory_only(key.clone(), encoded);
        let decoded = cached_bytes_with(&cache, key, || panic!("legacy cache value must decode"),
            |value| value.map(<[u8]>::to_vec));
        assert_eq!(decoded, value);
    }
}

#[test]
fn borrowed_cache_corrupt_or_wrong_kind_entries_refill_from_storage() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    for encoded in [vec![0xc1], encode_cached_response(&CommandResponse::Integer { value: 8 }).unwrap()] {
        let key = CacheKey::hash(1, "row", "timestamp");
        cache.put_memory_only(key.clone(), encoded);
        assert_eq!(cached_bytes_with(&cache, key.clone(), || Some(vec![7; 8]),
            |value| value.unwrap().len()), 8);
        assert_eq!(cached_bytes_with(&cache, key, || panic!("refilled value must be cached"),
            |value| value.unwrap()[0]), 7);
    }
}
