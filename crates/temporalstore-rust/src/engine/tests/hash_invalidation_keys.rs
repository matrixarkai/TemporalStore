// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::BatchHashMultiSetValue;

#[test]
fn maintenance_record_lookup_preserves_mixed_models_and_unrelated_pages() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        16 * 1024 * 1024, dir.path().join("cache"), dir.path().join("blocks"),
        dir.path().join("indexes"), BlockStoreOptions::default(), false,
    );
    let cache = &engine.cache;
    let mut wanted = Vec::new();
    let mut survivors = Vec::new();
    for shard_id in [1, 2] {
        for record in ["due", "live"] {
            for namespace in SWEPT_RECORD_NAMESPACES {
                for selector in ["first", "second"] {
                    let key = CacheKey { shard_id, namespace: (*namespace).into(),
                        record_key: record.into(), selector: selector.into() };
                    cache.put(key.clone(), vec![1; 16]).unwrap();
                    if shard_id == 1 && record == "due" { wanted.push(key); }
                    else { survivors.push(key); }
                }
            }
            for key in named_record_keys(shard_id, record) {
                cache.put(key.clone(), vec![2; 16]).unwrap();
                if shard_id == 1 && record == "due" { wanted.push(key); }
                else { survivors.push(key); }
            }
        }
    }
    for index in 0..4096 {
        let page = CacheKey { shard_id: 1, namespace: "page".into(),
            record_key: format!("page-{index}").into(), selector: "data".into() };
        cache.put(page.clone(), vec![3; 16]).unwrap();
        survivors.push(page);
    }
    for key in wanted.iter().chain(&survivors) { assert!(cache.peek(key)); }
    let counts = CacheSweepCounts::zeroed();
    counts.set_armed(true);
    invalidate_records_all_batched(cache, 1, &["due", "due"], &counts);
    assert_eq!(counts.read_batched(), (1, 2, 1, 4, 4));
    assert_eq!(counts.read().3, 4);
    for key in &wanted { assert!(!cache.peek(key), "not invalidated: {key:?}"); }
    for key in &survivors { assert!(cache.peek(key), "over-invalidated: {key:?}"); }
    counts.reset();
    invalidate_records_all_batched(cache, 1, &["due"], &counts);
    assert_eq!(counts.read_batched(), (1, 1, 1, 0, 0));
}

#[test]
fn scratch_invalidation_keeps_record_shard_namespace_and_field_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let long = "long-field-".repeat(128);
    let unicode = "\u{96ea}\u{2603}";
    let fields = ["", "a", long.as_str(), unicode, "untouched"];
    for shard in [1, 2] {
        for record in ["row", "neighbor"] {
            for field in fields {
                cache.put_memory_only(CacheKey::hash(shard, record, field), vec![1]);
            }
        }
    }
    cache.put_memory_only(CacheKey::string(1, "row"), vec![2]);
    let mut invalidator = HashFieldCacheInvalidator::new(&cache, 1, "row");
    for field in [long.as_str(), "a", "", unicode, "a"] {
        invalidator.invalidate(field);
    }
    for shard in [1, 2] {
        for record in ["row", "neighbor"] {
            for field in fields {
                let expected = !(shard == 1 && record == "row" && field != "untouched");
                assert_eq!(cache.peek(&CacheKey::hash(shard, record, field)), expected,
                           "shard={shard} record={record} field={field}");
            }
        }
    }
    assert!(cache.peek(&CacheKey::string(1, "row")));
}

#[test]
fn scratch_key_preserves_owned_copies_after_selector_changes() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let mut invalidator = HashFieldCacheInvalidator::new(&cache, 1, "row");
    invalidator.invalidate("first");
    let held = invalidator.key.clone();
    let record = std::sync::Arc::clone(&invalidator.key.record_key);
    let selector = invalidator.key.selector.as_ptr();
    invalidator.invalidate("next");
    assert_eq!(held, CacheKey::hash(1, "row", "first"));
    assert_eq!(invalidator.key, CacheKey::hash(1, "row", "next"));
    assert!(std::sync::Arc::ptr_eq(&record, &invalidator.key.record_key));
    assert_eq!(selector, invalidator.key.selector.as_ptr());
}

#[test]
fn packed_and_scalar_updates_preserve_cached_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let run = |command| {
        let reply = engine.execute(ExecuteRequest { shard_id: 1, command });
        assert!(reply.status.ok, "{:?}", reply.status);
        reply.response
    };
    for field in ["a", "b", "untouched"] {
        run(Command::HashSet { key: "row".into(), field: field.into(), value: vec![1] });
        assert_eq!(run(Command::HashGet { key: "row".into(), field: field.into() }),
                   CommandResponse::Bytes { value: Some(vec![1]) });
    }
    run(Command::HashMultiSet {
        key: "row".into(), entries: vec![("a".into(), vec![2]), ("b".into(), vec![3])],
    });
    for (field, value) in [("a", vec![2]), ("b", vec![3]), ("untouched", vec![1])] {
        assert_eq!(run(Command::HashGet { key: "row".into(), field: field.into() }),
                   CommandResponse::Bytes { value: Some(value) });
    }
    let response = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1, ttl_ms: None, fields: vec!["a".into(), "b".into()],
        rows: vec![BatchHashMultiSetRow("row".into(), vec![
            BatchHashMultiSetValue(vec![4]), BatchHashMultiSetValue(vec![5]),
        ])],
    }).0;
    assert!(response.status.ok && response.errors.is_empty());
    for (field, value) in [("a", vec![4]), ("b", vec![5]), ("untouched", vec![1])] {
        assert_eq!(run(Command::HashGet { key: "row".into(), field: field.into() }),
                   CommandResponse::Bytes { value: Some(value) });
    }
}

#[test]
#[cfg(feature = "alloc-probe")]
fn scratch_invalidation_allocations_do_not_scale_with_field_count() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024 * 1024, dir.path());
    let keys: Vec<_> = (0..256).map(|index| format!("row-{index:06}-feature-serving-identifier")).collect();
    for field_count in [1usize, 3, 21, 65] {
        let fields: Vec<_> = (0..field_count).map(|index| format!("stats:feature-{index:03}")).collect();
        for _ in 0..2 {
            let mut warm = HashFieldCacheInvalidator::new(&cache, 1, &keys[0]);
            for field in &fields { warm.invalidate(field); }
        }
        let old_probe = crate::alloc_probe::Probe::start();
        for key in &keys {
            for field in &fields {
                invalidate_if_cached(&cache, CacheKey::hash(1, key, field));
            }
        }
        let old = old_probe.stop();
        let new_probe = crate::alloc_probe::Probe::start();
        for key in &keys {
            let mut invalidator = HashFieldCacheInvalidator::new(&cache, 1, key);
            for field in &fields { invalidator.invalidate(field); }
        }
        let new = new_probe.stop();
        eprintln!("INVALIDATION_ALLOC rows={} fields={} old_allocs={} new_allocs={} old_bytes={} new_bytes={}",
                  keys.len(), field_count, old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes);
        assert_eq!(new.allocs, (keys.len() * 2) as u64);
        assert_eq!(old.allocs, (keys.len() * field_count * 2) as u64);
        if field_count > 1 {
            assert!(old.alloc_bytes > new.alloc_bytes * 2);
        } else {
            assert_eq!(old.alloc_bytes, new.alloc_bytes);
        }
    }
}
