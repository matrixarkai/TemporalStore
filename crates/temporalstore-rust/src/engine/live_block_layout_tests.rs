// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::state::{BlockSlabLiveIndex, TombstoneElement};

fn fixture(count: usize) -> (BucketNode, BlockSlabLiveIndex, u64) {
    let mut bucket = BucketNode { routing_bucket: 23, ..BucketNode::default() };
    let mut live = BlockSlabLiveIndex::default();
    let mut rows = Vec::new();
    for index in 0..count {
        let address = ElementEntry::from_parts(7, index as u64 * 64, 64, None, None);
        rows.push(TombstoneElement {
            object_key: Arc::from("order"),
            model_id: StoredModelKind::Set,
            element_name: Arc::from(format!("removed-{index}")),
            block: live_block_key(&address),
        });
        bucket.block_index.insert("order", BlockIndex::new(
            StoredModelKind::Set, address, false, true,
        ), &mut live);
    }
    bucket.rebuild_tombstone_elements(rows);
    bucket.object_index.insert(42);
    let address = ElementEntry::from_parts(7, count as u64 * 64, 64, None, None);
    let handle = bucket.block_index.insert("order", BlockIndex::new(
        StoredModelKind::Set, address, false, false,
    ), &mut live);
    (bucket, live, handle)
}

fn reset_passes() { TOMBSTONE_LAYOUT_PASSES.with(|count| count.set(0)); }
fn passes() -> usize { TOMBSTONE_LAYOUT_PASSES.with(std::cell::Cell::get) }

#[test]
fn live_retirement_preserves_deleted_rows_without_a_reconcile_pass() {
    for count in [0, 1, 128, 2048] {
        let (mut bucket, mut live, handle) = fixture(count);
        let before = bucket.tombstone_elements.clone();
        let removed = bucket.block_index.remove(
            &handle, &mut live, crate::engine::state::StaleCause::Removed,
        ).unwrap();
        reset_passes();
        classify_bucket_layout_after_block_removal(&mut bucket, &removed);
        assert_eq!(passes(), 0);
        assert_eq!(bucket.tombstone_elements, before);
        let mut fully_reconciled = bucket.clone();
        classify_bucket_layout_in_place(&mut fully_reconciled);
        assert_eq!(bucket.layout, fully_reconciled.layout);
        assert_eq!(bucket.tombstone_elements, fully_reconciled.tombstone_elements);
    }
}

#[test]
fn deleted_retirement_still_reconciles_its_markers() {
    let (mut bucket, mut live, _) = fixture(128);
    let handle = bucket.block_index.iter().find_map(|(handle, block)| {
        (block.deleted() && block.address.offset() == 0).then_some(*handle)
    }).unwrap();
    let removed = bucket.block_index.remove(
        &handle, &mut live, crate::engine::state::StaleCause::Swept,
    ).unwrap();
    reset_passes();
    classify_bucket_layout_after_block_removal(&mut bucket, &removed);
    assert_eq!(passes(), 1);
    assert_eq!(bucket.tombstone_elements.len(), 127);
    assert!(bucket.tombstone_elements.iter().all(|row| row.block != live_block_key(&removed.address)));
}

#[test]
fn an_unexpected_displacement_keeps_full_reconciliation_even_with_an_append_hint() {
    let (bucket, live, live_handle) = fixture(1);
    let mut shard = ShardState::default();
    shard.bucket_index.bucket_map.insert(23, bucket);
    shard.bucket_index.block_slab_live = live;
    // Remove the unrelated live entry so the colliding address is the only stored block.
    shard.bucket_index.bucket_map.get_mut(&23).unwrap().block_index.remove(
        &live_handle, &mut shard.bucket_index.block_slab_live,
        crate::engine::state::StaleCause::Removed,
    );
    reset_passes();
    upsert_bucket_index_block_with_value_at_bucket(
        &mut shard, 1, 23, "set", "order", Some("different-member".into()),
        ElementEntry::from_parts(7, 0, 64, None, None), false, false, None,
        ReplacedBlock::Appended,
    );
    assert_eq!(passes(), 1);
    let bucket = shard.bucket_index.bucket_map.get(&23).unwrap();
    assert_eq!(bucket.block_index.len(), 1);
    assert!(bucket.tombstone_elements.is_empty());
}

fn open(dir: &std::path::Path, async_storage: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024, dir.join("cache"), dir.join("blocks"), dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(crate::control::SetConfigRequest {
        shard_id: 1,
        config: Config { version: 2, async_storage, ..Config::default() },
    }).ok);
    engine
}

fn write(engine: &TemporalEngine, commands: Vec<Command>) {
    let response = engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands });
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.responses.iter().all(|row| row.status.ok));
}

fn members(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::SetMembers { key: "order".into() },
    });
    assert!(response.status.ok);
    let CommandResponse::Members { members } = response.response else { panic!("missing members") };
    members
}

#[test]
fn fresh_append_skips_reconcile_but_readd_still_sweeps_its_marker() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage);
        write(&engine, (0..128).map(|index| Command::SetAdd {
            key: "order".into(), member: format!("member-{index}").into_bytes(),
        }).collect());
        write(&engine, (0..64).map(|index| Command::SetRemove {
            key: "order".into(), member: format!("member-{index}").into_bytes(),
        }).collect());
        reset_passes();
        write(&engine, vec![Command::SetAdd { key: "order".into(), member: b"fresh".to_vec() }]);
        assert_eq!(passes(), 1, "only the final batch flag refresh needs reconciliation");
        reset_passes();
        write(&engine, vec![Command::SetAdd { key: "order".into(), member: b"member-0".to_vec() }]);
        assert_eq!(passes(), 2, "re-add also reconciles its swept tombstone before refiling");
        let expected = members(&engine);
        assert_eq!(expected.len(), 66);
        assert!(expected.contains(&b"member-0".to_vec()));
        assert!(!expected.contains(&b"member-1".to_vec()));
        drop(engine);
        assert_eq!(members(&open(dir.path(), async_storage)), expected);
    }
}

#[test]
fn metadata_shaped_churn_preserves_rows_and_history_after_reopen() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage);
        write(&engine, vec![Command::HashMultiSet {
            key: "catalog".into(), entries: (0..128).map(|i| (format!("id-{i}"), vec![0; 64])).collect(),
        }]);
        write(&engine, (0..128).map(|i| Command::SetAdd {
            key: "order".into(), member: format!("0:id-{i}").into_bytes(),
        }).collect());
        for round in 1..=16 {
            let mut commands = vec![Command::HashMultiSet {
                key: "catalog".into(),
                entries: (0..8).map(|i| (format!("id-{i}"), vec![round as u8; 64])).collect(),
            }];
            for i in 0..8 {
                commands.extend([
                    Command::HashSet { key: format!("history-{i}"), field: round.to_string(), value: vec![round as u8; 64] },
                    Command::SetRemove { key: "order".into(), member: format!("{}:id-{i}", round - 1).into_bytes() },
                    Command::SetAdd { key: "order".into(), member: format!("{round}:id-{i}").into_bytes() },
                ]);
            }
            write(&engine, commands);
        }
        let expected = members(&engine);
        assert_eq!(expected.len(), 128);
        for i in 0..8 { assert!(expected.contains(&format!("16:id-{i}").into_bytes())); }
        drop(engine);
        let recovered = open(dir.path(), async_storage);
        assert_eq!(members(&recovered), expected);
        for i in 0..8 {
            let response = recovered.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashLen { key: format!("history-{i}") },
            });
            assert!(response.status.ok);
            assert!(matches!(response.response, CommandResponse::Integer { value: 16 }));
            let response = recovered.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashGet { key: "catalog".into(), field: format!("id-{i}") },
            });
            assert!(response.status.ok);
            assert!(matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == vec![16; 64]));
        }
    }
}
