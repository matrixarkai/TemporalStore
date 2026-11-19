// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(16 * 1024 * 1024,
        root.join("cache"), root.join("blocks"), root.join("indexes"));
    engine.load_shard(1);
    engine
}

fn physical(engine: &TemporalEngine) -> Vec<u64> {
    assert!(engine.execute(ExecuteRequest { shard_id: 1,
        command: Command::SetAdd { key: "physical".into(), member: vec![7; 64] },
    }).status.ok);
    let ids = engine.block_store.slab_ids().unwrap();
    assert!(!ids.is_empty());
    assert!(engine.block_slab_live_tallies(1).is_some());
    ids
}

#[test]
fn operational_wal_only_plan_does_not_walk_retained_fields() {
    for count in [32, 512] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        let (result, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
            shard_id: 1,
            fields: (0..20).map(|i| format!("field-{i:02}")).collect(),
            rows: (0..count).map(|row| BatchHashMultiSetRow(format!("key-{row}"),
                (0..20).map(|_| BatchHashMultiSetValue(vec![row as u8; 48])).collect())).collect(),
            ttl_ms: None,
        });
        assert!(result.status.ok);
        assert!(engine.block_store.slab_ids().unwrap().is_empty());
        let request = StorageLifecycleRequest { shard_id: 1,
            min_undumped_wal_records: u64::MAX, min_undumped_wal_bytes: u64::MAX,
            ..Default::default() };
        crate::snapshot_probe::reset();
        let operational = engine.storage_lifecycle_plan_with_report_details(request.clone(), false);
        assert_eq!(crate::snapshot_probe::counts().live_slab_scan_addresses, 0);
        assert!(operational.stale_block_slab_ids.is_empty());
        crate::snapshot_probe::reset();
        let detailed = engine.storage_lifecycle_plan(request);
        let visited = crate::snapshot_probe::counts().live_slab_scan_addresses;
        assert!(visited >= count as u64 * 20);
        assert_eq!(operational.live_block_slab_ids, detailed.live_block_slab_ids);
        assert_eq!(operational.dirty_buckets, detailed.dirty_buckets);
        assert_eq!(operational.selected_dump_buckets, detailed.selected_dump_buckets);
        assert!(operational.reclaim_candidates.is_empty());
        assert!(detailed.reclaim_candidates.is_empty());
        println!("rows={count} fields=20 operational_live_addresses=0 detailed_live_addresses={visited}");
    }
}

#[test]
fn a_missing_live_tally_entry_requires_the_authoritative_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let ids = physical(&engine);
    let expected = engine.live_block_slab_ids(1);
    crate::snapshot_probe::reset();
    assert_eq!(engine.planning_live_block_slab_ids(1, Some(&ids), false), expected);
    assert_eq!(crate::snapshot_probe::counts().live_slab_scan_addresses, 0);
    engine.forget_tally_slab_for_test(1, ids[0]);
    crate::snapshot_probe::reset();
    let actual = engine.planning_live_block_slab_ids(1, Some(&ids), false);
    assert_eq!(actual, expected);
    assert!(crate::snapshot_probe::counts().live_slab_scan_addresses > 0);
    let plan = engine.storage_lifecycle_plan_with_report_details(StorageLifecycleRequest {
        shard_id: 1, ..Default::default()
    }, false);
    assert!(!plan.stale_block_slab_ids.contains(&ids[0]));
}

#[test]
fn unknown_inventory_unready_tallies_and_detailed_requests_always_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let ids = physical(&engine);
    let expected = engine.live_block_slab_ids(1);
    for (inventory, detailed) in [(None, false), (Some(ids.as_slice()), true)] {
        crate::snapshot_probe::reset();
        assert_eq!(engine.planning_live_block_slab_ids(1, inventory, detailed), expected);
        assert!(crate::snapshot_probe::counts().live_slab_scan_addresses > 0);
    }
    engine.shards.write().unwrap().get_mut(&1).unwrap()
        .bucket_index.block_slab_live.forget_for_test();
    crate::snapshot_probe::reset();
    assert_eq!(engine.planning_live_block_slab_ids(1, Some(&ids), false), expected);
    assert!(crate::snapshot_probe::counts().live_slab_scan_addresses > 0);
}

#[test]
fn unpublished_compaction_slabs_remain_retained_and_retirement_rechecks_liveness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    physical(&engine);
    engine.block_store.roll_slab().unwrap();
    let pending = engine.block_store.append(&[9; 64]).unwrap().block_slab_id();
    engine.unpublished_compaction_slabs.write().unwrap().insert(1, BTreeSet::from([pending]));
    let ids = engine.block_store.slab_ids().unwrap();
    crate::snapshot_probe::reset();
    assert!(engine.planning_live_block_slab_ids(1, Some(&ids), false).contains(&pending));
    assert_eq!(crate::snapshot_probe::counts().live_slab_scan_addresses, 0);
    engine.unpublished_compaction_slabs.write().unwrap().remove(&1);
    crate::snapshot_probe::reset();
    assert!(!engine.planning_live_block_slab_ids(1, Some(&ids), false).contains(&pending));
    assert!(crate::snapshot_probe::counts().live_slab_scan_addresses > 0);
}

#[test]
fn an_overreporting_tally_only_defers_collection_until_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    physical(&engine);
    engine.block_store.roll_slab().unwrap();
    let orphan = engine.block_store.append(&[9; 64]).unwrap();
    let orphan_id = orphan.block_slab_id();
    engine.shards.write().unwrap().get_mut(&1).unwrap()
        .bucket_index.block_slab_live.add_address(&orphan);
    let request = StorageLifecycleRequest { shard_id: 1, ..Default::default() };
    let conservative = engine.storage_lifecycle_plan_with_report_details(request.clone(), false);
    assert!(!conservative.stale_block_slab_ids.contains(&orphan_id));
    let detailed = engine.storage_lifecycle_plan(request.clone());
    assert!(detailed.stale_block_slab_ids.contains(&orphan_id));
    engine.block_slab_live_drift_check(1);
    crate::snapshot_probe::reset();
    let reconciled = engine.storage_lifecycle_plan_with_report_details(request, false);
    assert!(reconciled.stale_block_slab_ids.contains(&orphan_id));
    assert!(crate::snapshot_probe::counts().live_slab_scan_addresses > 0);
}
