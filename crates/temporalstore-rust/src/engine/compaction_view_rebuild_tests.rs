// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn fixture(count: usize, remove: bool, readd: bool) -> ShardState {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("index"),
    );
    engine.load_shard(1);
    let apply = |commands: Vec<Command>| {
        if commands.is_empty() {
            return;
        }
        let expected = commands.len();
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "{:?}", response.status);
        assert_eq!(response.responses.len(), expected);
        assert!(response.responses.iter().all(|row| row.status.ok));
    };
    let mut commands = Vec::new();
    for index in 0..count {
        commands.extend([
            Command::StringSet {
                key: format!("string-{index}"),
                value: vec![index as u8; 64],
            },
            Command::HashSet {
                key: "hash".into(),
                field: format!("field-{index}"),
                value: vec![index as u8; 32],
            },
            Command::SetAdd {
                key: "set".into(),
                member: format!("member-{index}").into_bytes(),
            },
            Command::ZSetAdd {
                key: "scores".into(),
                member: format!("member-{index}").into_bytes(),
                score: index as f64,
            },
            Command::ListPush {
                key: "list".into(),
                member: format!("item-{index}").into_bytes(),
                left: false,
            },
            Command::ContextWriteEvent {
                tenant_hash: 7,
                node_hash: 9,
                event: Box::new(ContextEvent {
                    event_id_hash: index as u64 + 1,
                    event_time_ms: index as u64 + 1,
                    ingestion_time_ms: index as u64 + 1,
                    kind: 0,
                    event_type: 1,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 0.5,
                    text: format!("message-{index}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: vec![0.5; 8],
                    embedding_model_hash: 7,
                }),
                first_write_only: false,
                cold_storage: false,
            },
        ]);
    }
    apply(commands);
    let mut commands = Vec::new();
    if remove {
        for index in (0..count).step_by(3) {
            commands.extend([
                Command::CommonDelete {
                    key: format!("string-{index}"),
                },
                Command::HashDelete {
                    key: "hash".into(),
                    field: format!("field-{index}"),
                },
                Command::SetRemove {
                    key: "set".into(),
                    member: format!("member-{index}").into_bytes(),
                },
                Command::ZSetRemove {
                    key: "scores".into(),
                    member: format!("member-{index}").into_bytes(),
                },
                Command::ListPop {
                    key: "list".into(),
                    left: true,
                },
            ]);
        }
    }
    apply(commands);
    if readd && count > 0 {
        apply(vec![
            Command::StringSet {
                key: "string-0".into(),
                value: b"returned".to_vec(),
            },
            Command::HashSet {
                key: "hash".into(),
                field: "field-0".into(),
                value: b"returned".to_vec(),
            },
            Command::SetAdd {
                key: "set".into(),
                member: b"member-0".to_vec(),
            },
            Command::ZSetAdd {
                key: "scores".into(),
                member: b"member-0".to_vec(),
                score: 7.0,
            },
        ]);
    }
    let shard = engine.shards.read().unwrap().get(&1).unwrap().clone();
    shard
}

fn separate_rebuilds(shard: &mut ShardState) {
    rebuild_bucket_first_index(1, shard, 0, u32::MAX);
    refresh_bucket_runtime_flags(shard);
    rebuild_bucket_block_ownership(1, shard, 0, u32::MAX);
}

fn assert_same_state(expected: &ShardState, actual: &ShardState) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(
        format!("{:?}", actual.dirty_objects),
        format!("{:?}", expected.dirty_objects)
    );
    assert_eq!(
        format!("{:?}", actual.context_events()),
        format!("{:?}", expected.context_events())
    );
    let expected = validate_bucket_ownership_index(1, expected, 0, u32::MAX);
    let actual = validate_bucket_ownership_index(1, actual, 0, u32::MAX);
    assert_eq!(
        actual.missing_owner_block_refs,
        expected.missing_owner_block_refs
    );
    assert_eq!(actual.mismatches, expected.mismatches);
}

#[test]
fn compaction_views_share_one_model_walk_without_changing_the_result() {
    for count in [4, 32, 256] {
        let original = fixture(count, true, false);
        let denominator = collect_model_live_block_entries(&original).len() as u64;
        assert!(denominator > 0);
        let mut expected = original.clone();
        let mut actual = original;
        reset_live_block_scan_entries();
        #[cfg(feature = "alloc-probe")]
        let probe = crate::alloc_probe::Probe::start();
        separate_rebuilds(&mut expected);
        #[cfg(feature = "alloc-probe")]
        let old_alloc = probe.stop();
        let old = live_block_scan_entries();
        reset_live_block_scan_entries();
        #[cfg(feature = "alloc-probe")]
        let probe = crate::alloc_probe::Probe::start();
        rebuild_compacted_bucket_views(1, &mut actual, 0, u32::MAX);
        #[cfg(feature = "alloc-probe")]
        let new_alloc = probe.stop();
        let new = live_block_scan_entries();
        assert_eq!(old, denominator * 2);
        assert_eq!(new, denominator);
        assert_same_state(&expected, &actual);
        println!("objects={count} live_blocks={denominator} separate={old} shared={new}");
        #[cfg(feature = "alloc-probe")]
        {
            assert!(old_alloc.allocs > 0 && old_alloc.alloc_bytes > 0);
            println!(
                "allocations separate={} shared={} requested_bytes separate={} shared={}",
                old_alloc.allocs, new_alloc.allocs, old_alloc.alloc_bytes, new_alloc.alloc_bytes
            );
            assert!(new_alloc.allocs < old_alloc.allocs);
            assert!(new_alloc.alloc_bytes < old_alloc.alloc_bytes);
        }
    }
}

#[test]
fn compaction_views_preserve_empty_live_deleted_and_readded_shapes() {
    for count in [0, 1, 64] {
        for (remove, readd) in [(false, false), (true, false), (true, true)] {
            let original = fixture(count, remove, readd);
            let mut expected = original.clone();
            let mut actual = original;
            separate_rebuilds(&mut expected);
            rebuild_compacted_bucket_views(1, &mut actual, 0, u32::MAX);
            assert_same_state(&expected, &actual);
        }
    }
}

#[test]
fn compaction_views_do_not_change_authoritative_rows_between_phases() {
    for (remove, readd) in [(false, false), (true, false), (true, true)] {
        let mut shard = fixture(64, remove, readd);
        let before = collect_model_live_block_entries(&shard);
        rebuild_bucket_first_index_from_entries_with_layout::<true>(
            1,
            &mut shard,
            0,
            u32::MAX,
            before.iter().cloned(),
            update_bucket_layout_with_live_objects,
        );
        refresh_bucket_runtime_flags(&mut shard);
        let after = collect_model_live_block_entries(&shard);
        assert!(!before.is_empty());
        assert_eq!(format!("{before:?}"), format!("{after:?}"));
    }
}

#[test]
fn compaction_views_preserve_serialized_reload_and_repeat_rebuild() {
    let original = fixture(64, true, true);
    let mut expected = original.clone();
    let mut actual = original;
    separate_rebuilds(&mut expected);
    rebuild_compacted_bucket_views(1, &mut actual, 0, u32::MAX);
    let encoded = serde_json::to_vec(&actual).unwrap();
    let mut reloaded: ShardState = serde_json::from_slice(&encoded).unwrap();
    let mut expected_reload: ShardState =
        serde_json::from_slice(&serde_json::to_vec(&expected).unwrap()).unwrap();
    separate_rebuilds(&mut expected_reload);
    rebuild_compacted_bucket_views(1, &mut reloaded, 0, u32::MAX);
    assert_same_state(&expected_reload, &reloaded);
    separate_rebuilds(&mut expected);
    rebuild_compacted_bucket_views(1, &mut actual, 0, u32::MAX);
    assert_same_state(&expected, &actual);
}
