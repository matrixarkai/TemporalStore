// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

#[test]
fn reclaim_restores_a_slab_owned_by_an_unloaded_shards_dump() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        engine
            .block_store
            .install_slab(0, b"retained payload")
            .unwrap();
        engine.block_store.install_slab(1, b"active").unwrap();
        engine
            .block_store
            .gc_slabs_before_with_live_refs_delayed_destroy(1, [1])
            .unwrap();
        assert_eq!(
            engine.block_store.delayed_destroy_slab_ids().unwrap(),
            vec![0]
        );
        engine
            .persist_bucket_dump_manifest(&BucketDumpManifest {
                shard_id: 2,
                manifest_id: "retained".into(),
                block_slab_ids: vec![0],
                ..Default::default()
            })
            .unwrap();
    }
    let engine = open(dir.path());
    assert!(!engine.shards.with_state(2, |owner| owner.is_some()));
    let report = engine.purge_storage_delayed_destroy(&StorageLifecycleRequest {
        shard_id: 1,
        purge_delayed_destroy: true,
        ..Default::default()
    });
    assert_eq!(report.restored_block_slab_ids, vec![0]);
    assert!(report.purged_block_slab_ids.is_empty());
    assert!(engine.block_store.slab_ids().unwrap().contains(&0));
    let plan = engine.storage_block_gc_dependency_plan(1, [0], [], [], None, None, 0);
    assert_eq!(plan.blocked_block_slab_ids, vec![0]);
}

#[test]
fn fallible_page_purge_keeps_mature_slab_when_manifest_listing_fails() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.block_store.install_slab(0, b"stale").unwrap();
    engine.block_store.install_slab(1, b"active").unwrap();
    engine.block_store.gc_slabs_before_with_live_refs_delayed_destroy(1, [1]).unwrap();
    engine.block_store.age_quarantine_for_reclaim_test(1);
    engine.persist_bucket_dump_manifest(&BucketDumpManifest {
        shard_id: 2, manifest_id: "other-shard".into(), ..Default::default()
    }).unwrap();
    let broken = bucket_dump_manifest_dir(&engine.index_dir, 2).join("damaged.manifest");
    fs::write(&broken, b"broken").unwrap();
    let request = StorageLifecycleRequest {
        shard_id: 1, purge_delayed_destroy: true, ..Default::default()
    };
    assert!(engine.try_purge_storage_delayed_destroy(&request).is_err());
    assert_eq!(engine.block_store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    fs::remove_file(broken).unwrap();
    let recovered = engine.try_purge_storage_delayed_destroy(&request).unwrap();
    assert_eq!(recovered.purged_block_slab_ids, vec![0]);
    assert!(engine.block_store.delayed_destroy_slab_ids().unwrap().is_empty());
}

#[test]
fn reclaim_refuses_a_damaged_manifest_without_discarding_recovery_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .persist_bucket_dump_manifest(&BucketDumpManifest {
            shard_id: 2,
            manifest_id: "valid".into(),
            block_slab_ids: vec![3],
            ..Default::default()
        })
        .unwrap();
    let path = bucket_dump_manifest_dir(&engine.index_dir, 2).join("damaged.manifest");
    fs::write(&path, b"not a manifest").unwrap();
    assert_eq!(
        list_bucket_dump_manifests_shared_at(&engine.index_dir, 2)
            .unwrap()
            .len(),
        1
    );
    assert!(list_reclaim_manifests_at(&engine.index_dir).is_err());
    let plan = engine.storage_block_gc_dependency_plan(1, [3, 4], [], [], None, None, 0);
    assert!(!plan.safe_to_reclaim);
    assert!(plan.reclaimable_block_slab_ids.is_empty());
    assert!(plan
        .blocker_reasons
        .iter()
        .any(|reason| reason == "dump_manifest_unreadable"));
    fs::remove_file(path).unwrap();
    let repaired = engine.storage_block_gc_dependency_plan(1, [3, 4], [], [], None, None, 0);
    assert_eq!(repaired.blocked_block_slab_ids, vec![3]);
    assert_eq!(repaired.reclaimable_block_slab_ids, vec![4]);
}

#[test]
fn reclaim_refuses_a_manifest_with_an_invalid_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .persist_bucket_dump_manifest(&BucketDumpManifest {
            shard_id: 2,
            manifest_id: "bad-checksum".into(),
            checksum: "incorrect".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(list_reclaim_manifests_at(&engine.index_dir).is_err());
    assert!(list_bucket_dump_manifests_shared_at(&engine.index_dir, 2)
        .unwrap()
        .is_empty());
}

#[test]
fn direct_reclaim_honors_retention_owners_and_extended_grace() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.block_store.install_slab(0, b"retained").unwrap();
    engine.block_store.install_slab(1, b"active").unwrap();
    engine
        .block_store
        .gc_slabs_before_with_live_refs_delayed_destroy(1, [1])
        .unwrap();
    engine.block_store.age_quarantine_for_reclaim_test(0);
    let request = StorageLifecycleRequest {
        shard_id: 1,
        purge_delayed_destroy: true,
        block_gc_checkpoint_floor_slab_id: Some(0),
        ..Default::default()
    };
    let held = engine.purge_storage_delayed_destroy(&request);
    assert!(held.purged_block_slab_ids.is_empty());
    assert_eq!(held.restored_block_slab_ids, vec![0]);
    assert_eq!(engine.block_store.read_slab(0).unwrap(), b"retained");
    assert!(engine.block_store.delayed_destroy_slab_ids().unwrap().is_empty());
    // Release the owner and quarantine again before testing the independent grace gate.
    engine.block_store.gc_slabs_before_with_live_refs_delayed_destroy(1, [1]).unwrap();
    engine.block_store.age_quarantine_for_reclaim_test(0);
    assert_eq!(
        engine.block_store.delayed_destroy_slab_ids().unwrap(),
        vec![0]
    );
    let held = engine.purge_storage_delayed_destroy(&StorageLifecycleRequest {
        block_gc_checkpoint_floor_slab_id: None,
        block_gc_delayed_destroy_grace_ms: u64::MAX,
        ..request.clone()
    });
    assert!(held.purged_block_slab_ids.is_empty());
    assert!(held.restored_block_slab_ids.is_empty());
    assert_eq!(engine.block_store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    let released = engine.purge_storage_delayed_destroy(&StorageLifecycleRequest {
        block_gc_checkpoint_floor_slab_id: None,
        ..request
    });
    assert_eq!(released.purged_block_slab_ids, vec![0]);
    assert!(released.purged_physical_bytes > 0);
    assert!(engine.block_store.read_slab(0).is_err());
}

fn owned_inventory_wal_floor(snapshot: &ShardState) -> Option<u64> {
    let oldest = snapshot.wal_resident_blocks.values().map(|block| block.sequence).min();
    let indexed = snapshot.bucket_index.bucket_map.values()
        .flat_map(|bucket| bucket.block_index.values())
        .any(|block| crate::wal_record::is_wal_resident(block.address.block_slab_id()));
    let models = storage_bucket_internals::collect_model_live_block_entries(snapshot).iter()
        .any(|entry| crate::wal_record::is_wal_resident(entry.address.block_slab_id()));
    (indexed || models).then_some(oldest.unwrap_or(0))
}

#[test]
fn borrowed_wal_floor_preserves_index_model_and_missing_placement_cases() {
    let _on = crate::engine::value_once_in_test(true);
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    for command in [
        Command::StringSet { key: "string".into(), value: b"value".to_vec() },
        Command::HashSet { key: "hash".into(), field: "field".into(), value: b"value".to_vec() },
    ] {
        assert!(engine.execute(ExecuteRequest { shard_id: 1, command }).status.ok);
    }
    let mut snapshot = engine.shards.with_state(1, |owner| owner.unwrap().clone());
    assert!(wal_floor_of_index_image(&snapshot).is_some());
    assert_eq!(wal_floor_of_index_image(&snapshot), owned_inventory_wal_floor(&snapshot));
    for mut bucket in snapshot.bucket_index.bucket_map.values_mut() {
        bucket.block_index = crate::engine::state::BlockIndexMap::Empty;
    }
    assert!(wal_floor_of_index_image(&snapshot).is_some(), "model-only placements still retain logs");
    assert_eq!(wal_floor_of_index_image(&snapshot), owned_inventory_wal_floor(&snapshot));
    snapshot.wal_resident_blocks.clear();
    assert_eq!(wal_floor_of_index_image(&snapshot), Some(0));
    assert_eq!(wal_floor_of_index_image(&snapshot), owned_inventory_wal_floor(&snapshot));

    let mut stale = ShardState::default();
    stale.wal_resident_blocks = engine.shards.with_state(1, |owner| owner.unwrap().wal_resident_blocks.clone());
    assert!(!stale.wal_resident_blocks.is_empty());
    assert_eq!(wal_floor_of_index_image(&stale), None, "stale locators alone retain nothing");
    assert_eq!(wal_floor_of_index_image(&stale), owned_inventory_wal_floor(&stale));

    let mut indexed_only = ShardState::default();
    indexed_only.bucket_index.bucket_map = engine.shards.with_state(1, |owner| owner.unwrap().bucket_index.bucket_map.clone());
    for mut bucket in indexed_only.bucket_index.bucket_map.values_mut() {
        let handles = bucket.block_index.iter().map(|(handle, _)| *handle).collect::<Vec<_>>();
        for handle in handles { bucket.block_index.get_mut(&handle).unwrap().set_deleted(true); }
    }
    assert_eq!(wal_floor_of_index_image(&indexed_only), Some(0), "indexed tombstones keep their unknown floor");
    assert_eq!(wal_floor_of_index_image(&indexed_only), owned_inventory_wal_floor(&indexed_only));
}

/// WHERE NO BLOCK IS LOG-RESIDENT THE FLOOR IS NONE, AND ASKING FOR IT LISTS NO BLOCK.
///
/// The floor only needs to know whether ANY model block is log-resident, so its model walk accepts
/// exactly those. An image written with the two-copy placement has none, and the walk must emit
/// nothing for it; it used to accept every block and be charged a second full listing. The answer
/// is compared with the inventory oracle on the images that matter: a populated one, a released one
/// and an empty one.
#[test]
fn the_floor_agrees_with_the_inventory_oracle_where_nothing_is_log_resident_and_lists_no_block() {
    let _off = crate::engine::value_once_in_test(false);
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    const KEYS: usize = 64;
    for index in 0..KEYS {
        assert!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringSet { key: format!("key-{index}"), value: vec![7; 32] },
        }).status.ok);
    }
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::HashSet { key: "hash".into(), field: "field".into(), value: b"value".to_vec() },
    }).status.ok);
    let populated = engine.shards.with_state(1, |owner| owner.unwrap().clone());
    // The counter is global: assert on the walk of THIS call, not on a ratio across tests.
    storage_bucket_internals::reset_live_block_scan_entries();
    let floor = wal_floor_of_index_image(&populated);
    let listed = storage_bucket_internals::live_block_scan_entries();
    assert_eq!(floor, None, "no block is log-resident, so no log is retained");
    assert_eq!(floor, owned_inventory_wal_floor(&populated));
    assert!(listed < KEYS as u64, "the floor listed {listed} blocks of {KEYS}; it should list none");

    // A released image answers the same way as the oracle does.
    let mut released = populated.clone();
    let ids = released.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>();
    for id in ids { released.bucket_index.released_buckets.insert(id); }
    assert_eq!(wal_floor_of_index_image(&released), owned_inventory_wal_floor(&released));

    // So does an empty one.
    let empty = ShardState::default();
    assert_eq!(wal_floor_of_index_image(&empty), None);
    assert_eq!(wal_floor_of_index_image(&empty), owned_inventory_wal_floor(&empty));
}

/// A STRING WHOSE VALUE LIVES ONLY IN THE LOG IS FOUND BY THE FLOOR'S FILTERED WALK.
///
/// The model walk accepts only log-resident blocks, so it is safe only if the residency test is true
/// for every model block whose value exists nowhere else. A string written with the single-copy
/// placement is such a block: with the index entry present and with it removed (a model-only image
/// with no placement), the floor must retain the log, and must equal the inventory oracle.
#[test]
fn the_floor_retains_the_log_for_a_string_whose_value_lives_only_in_it() {
    let _on = crate::engine::value_once_in_test(true);
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::StringSet { key: "only-in-the-log".into(), value: b"value".to_vec() },
    }).status.ok);
    let mut snapshot = engine.shards.with_state(1, |owner| owner.unwrap().clone());
    assert!(
        snapshot.strings().iter().any(|(_, slot)| crate::wal_record::is_wal_resident(slot.address().block_slab_id())),
        "the fixture must hold a string whose only copy is its log record"
    );
    assert!(wal_floor_of_index_image(&snapshot).is_some(), "an indexed log-resident string retains the log");
    assert_eq!(wal_floor_of_index_image(&snapshot), owned_inventory_wal_floor(&snapshot));
    // Model-only: no index entry names it, and no registration places it.
    for mut bucket in snapshot.bucket_index.bucket_map.values_mut() {
        bucket.block_index = crate::engine::state::BlockIndexMap::Empty;
    }
    snapshot.wal_resident_blocks.clear();
    assert_eq!(wal_floor_of_index_image(&snapshot), Some(0), "a log-resident string with no placement reads as floor 0");
    assert_eq!(wal_floor_of_index_image(&snapshot), owned_inventory_wal_floor(&snapshot));
}

#[test]
fn generation_validation_populates_the_same_images_retention_floor() {
    let _on = crate::engine::value_once_in_test(true);
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path());
    for index in 0..8 {
        assert!(engine.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringSet { key: format!("key-{index}"), value: vec![7; 32] },
        }).status.ok);
    }
    engine.wal_store.flush(1).unwrap();
    let mut snapshot = engine.shards.with_state(1, |owner| owner.unwrap().clone());
    let summaries = engine.bucket_storage_summaries(1);
    let mut manifest = BucketDumpManifest {
        version: 1, shard_id: 1, manifest_id: "image-retention".into(),
        bucket_ids: summaries.iter().map(|summary| summary.routing_bucket).collect(),
        bucket_summaries: summaries,
        wal_sequence: engine.wal_store.stats_without_rescan(1).last_sequence,
        index_log_sequence: engine.index_log_store.stats_without_rescan(1).last_sequence,
        ..Default::default()
    };
    let expected = owned_inventory_wal_floor(&snapshot);
    assert!(expected.is_some_and(|floor| floor > 0));
    for expected_floor in [expected, Some(0)] {
        if expected_floor == Some(0) { snapshot.wal_resident_blocks.clear(); }
        manifest.index_bytes = serialize_index(&snapshot);
        manifest.index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
        manifest.checksum.clear();
        manifest.checksum = bucket_dump_io::bucket_dump_manifest_checksum(&manifest).unwrap();
        engine.persist_bucket_dump_manifest(&manifest).unwrap();
        let plan = engine.storage_wal_reclaim_plan(1, [], []);
        assert!(plan.safe_to_reclaim, "{:?}", plan.blocker_reasons);
        let listed = list_bucket_dump_manifests_shared_at(&engine.index_dir, 1).unwrap();
        let held = listed.iter().find(|item| item.manifest_id == manifest.manifest_id).unwrap();
        assert_eq!(bucket_dump_io::manifest_wal_floor_memoised(held, || {
            panic!("a validated image must not be decoded again for its retention floor")
        }), Ok(expected_floor));
    }
}

#[test]
fn wal_floor_does_not_build_an_owned_block_inventory() {
    let source = include_str!("storage_lifecycle_methods.rs");
    let function = source.split("fn wal_floor_of_index_image(").nth(1).unwrap()
        .split("\n}\n").next().unwrap();
    assert!(!function.contains("collect_model_live_block_entries("));
    assert!(function.contains("visit_model_live_blocks("));
}
