// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

const SHARD: ShardId = 1;

fn open(dir: &std::path::Path) -> TemporalEngine {
    open_with_range(dir, u32::MAX)
}

fn open_with_range(dir: &std::path::Path, end_routing_bucket: u32) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
    assert!(loaded.status.ok, "{:?}", loaded.status);
    engine
}

fn execute(engine: &TemporalEngine, command: Command) {
    let result = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command,
    });
    assert!(result.status.ok, "{:?}", result.status);
}

fn seed(engine: &TemporalEngine, count: usize) {
    for item in 0..count {
        execute(
            engine,
            Command::StringSet {
                key: format!("key-{item}"),
                value: vec![item as u8; 96],
            },
        );
    }
}

fn scan_control(engine: &TemporalEngine, covered: &BTreeSet<u32>) -> Vec<u32> {
    let dirty = engine
        .shards
        .read()
        .unwrap()
        .get(&SHARD)
        .map(super::super::storage_reporting::dirty_bucket_nodes)
        .unwrap_or_default();
    engine
        .bucket_storage_summaries(SHARD)
        .into_iter()
        .filter(|summary| dirty.contains(&summary.routing_bucket))
        .map(|summary| summary.routing_bucket)
        .filter(|bucket| !covered.contains(bucket))
        .collect()
}

fn measured(engine: &TemporalEngine, covered: &BTreeSet<u32>) -> Vec<u32> {
    crate::engine::reset_live_block_scan_entries();
    let result = engine.recovery_missing_dump_bucket_ids(SHARD, covered);
    let visits = crate::engine::live_block_scan_entries();
    assert_eq!(
        visits, 0,
        "dirty bucket selection must not list live blocks"
    );
    result
}

#[test]
fn missing_dirty_bucket_selection_does_not_list_live_blocks() {
    for count in [32, 512] {
        for end_routing_bucket in [0, 1023, u32::MAX] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_with_range(dir.path(), end_routing_bucket);
            seed(&engine, count);
            let dirty = scan_control(&engine, &BTreeSet::new());
            assert!(!dirty.is_empty());
            for covered in [
                BTreeSet::new(),
                dirty.iter().step_by(2).copied().collect(),
                dirty.iter().copied().collect(),
            ] {
                crate::engine::reset_live_block_scan_entries();
                let expected = scan_control(&engine, &covered);
                assert!(crate::engine::live_block_scan_entries() >= count as u64);
                assert_eq!(
                    engine.recovery_missing_dump_bucket_ids(SHARD, &covered),
                    expected
                );
                assert_eq!(measured(&engine, &covered), expected);
            }
        }
    }
}

#[test]
fn empty_dirty_nodes_remain_visible_without_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&SHARD).unwrap();
        super::super::storage_bucket_internals::mark_object_dirty_at_bucket(
            shard,
            "string:metadata-only",
            700,
        );
    }
    let covered = BTreeSet::new();
    assert_eq!(scan_control(&engine, &covered), vec![700]);
    assert_eq!(measured(&engine, &covered), vec![700]);
    assert_eq!(
        engine
            .storage_recovery_boundary_report_sampled(SHARD, 1)
            .missing_dump_bucket_ids,
        vec![700]
    );
    assert_eq!(measured(&engine, &BTreeSet::from([701])), vec![700]);
    assert!(measured(&engine, &BTreeSet::from([700])).is_empty());
}

#[test]
fn metadata_and_container_models_use_the_same_dirty_selection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for command in [
        Command::HashSet {
            key: "hash".into(),
            field: "f".into(),
            value: vec![1; 32],
        },
        Command::SetAdd {
            key: "set".into(),
            member: b"member".to_vec(),
        },
        Command::ListPush {
            key: "list".into(),
            member: b"member".to_vec(),
            left: false,
        },
        Command::ZSetAdd {
            key: "sorted".into(),
            member: b"member".to_vec(),
            score: 1.0,
        },
        Command::FeatureAppend {
            key: "sequence".into(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 10,
                value: vec![2; 32],
            }],
        },
        Command::ControlStateIncrement {
            key: "control".into(),
            timestamp_ms: 10,
            amount: 1,
        },
    ] {
        execute(&engine, command);
    }
    let expected = scan_control(&engine, &BTreeSet::new());
    assert!(!expected.is_empty());
    assert_eq!(measured(&engine, &BTreeSet::new()), expected);
    assert!(measured(&engine, &expected.into_iter().collect()).is_empty());
}

#[test]
fn a_dumped_bucket_does_not_hide_dirty_neighbours() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 32);
    let dirty = scan_control(&engine, &BTreeSet::new());
    assert!(dirty.len() > 1);
    let covered = BTreeSet::from([dirty[0]]);
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, covered.iter().copied())
        .unwrap();
    assert_eq!(
        manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>(),
        covered
    );
    let expected = scan_control(&engine, &covered);
    assert!(!expected.is_empty());
    assert_eq!(measured(&engine, &covered), expected);
    assert_eq!(
        engine
            .storage_recovery_boundary_report_sampled(SHARD, 1)
            .missing_dump_bucket_ids,
        expected
    );
}

#[test]
fn node_flags_not_key_ledger_counts_decide_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    seed(&engine, 32);
    let before = scan_control(&engine, &BTreeSet::new());
    assert!(!before.is_empty());
    engine
        .shards
        .write()
        .unwrap()
        .get_mut(&SHARD)
        .unwrap()
        .dirty_objects
        .clear();
    assert_eq!(scan_control(&engine, &BTreeSet::new()), before);
    assert_eq!(measured(&engine, &BTreeSet::new()), before);
    let clean_bucket = before[0];
    engine
        .shards
        .write()
        .unwrap()
        .get_mut(&SHARD)
        .unwrap()
        .bucket_index
        .bucket_map
        .get_mut(&clean_bucket)
        .unwrap()
        .set_dirty(false);
    let expected = scan_control(&engine, &BTreeSet::new());
    assert_eq!(expected.len() + 1, before.len());
    assert!(!expected.contains(&clean_bucket));
    assert_eq!(measured(&engine, &BTreeSet::new()), expected);
}

#[test]
fn unloaded_shard_has_no_missing_dirty_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    assert!(engine
        .recovery_missing_dump_bucket_ids(99, &BTreeSet::new())
        .is_empty());
}
