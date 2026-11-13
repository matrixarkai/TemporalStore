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
    assert!(!engine.shards.read().unwrap().contains_key(&2));
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
    let released = engine.purge_storage_delayed_destroy(&StorageLifecycleRequest {
        block_gc_checkpoint_floor_slab_id: None,
        ..request
    });
    assert_eq!(released.purged_block_slab_ids, vec![0]);
    assert!(released.purged_physical_bytes > 0);
}
