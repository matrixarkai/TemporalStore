// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(dir: &std::path::Path, async_storage: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn seed(engine: &TemporalEngine, count: usize) {
    let mut commands = Vec::new();
    for i in 0..count {
        commands.extend([
            Command::StringSet {
                key: format!("key-{i}"),
                value: vec![i as u8; 96],
            },
            Command::HashSet {
                key: "catalog".into(),
                field: format!("id-{i}"),
                value: vec![i as u8; 64],
            },
            Command::SetAdd {
                key: "order".into(),
                member: format!("id-{i}").into_bytes(),
            },
            Command::ContextWriteEvent {
                tenant_hash: 7,
                node_hash: 9,
                event: Box::new(ContextEvent {
                    event_id_hash: i as u64 + 1,
                    event_time_ms: i as u64 + 1,
                    ingestion_time_ms: i as u64 + 1,
                    kind: 0,
                    event_type: 1,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 0.5,
                    text: format!("message-{i}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                }),
                first_write_only: false,
                cold_storage: false,
            },
        ]);
    }
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.responses.iter().all(|row| row.status.ok));
}

fn check_values(engine: &TemporalEngine, count: usize) {
    for i in 0..count {
        for (command, expected) in [
            (
                Command::StringGet {
                    key: format!("key-{i}"),
                },
                vec![i as u8; 96],
            ),
            (
                Command::HashGet {
                    key: "catalog".into(),
                    field: format!("id-{i}"),
                },
                vec![i as u8; 64],
            ),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "{:?}", response.status);
            assert!(
                matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == expected)
            );
        }
    }
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "order".into(),
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Members { members } = response.response else {
        panic!("missing set")
    };
    assert_eq!(members.len(), count);
    for i in 0..count {
        assert!(members.contains(&format!("id-{i}").into_bytes()));
    }
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: 7,
            node_hash: 9,
            start_time_ms: 0,
            end_time_ms: count as u64 + 1,
            limit: Some(count),
            max_scan: None,
            current_valid_only: true,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    let CommandResponse::ContextEvents { events, .. } = response.response else {
        panic!("missing events")
    };
    assert_eq!(events.len(), count);
    for (i, event) in events.iter().rev().enumerate() {
        assert_eq!(event.text, format!("message-{i}"));
    }
}

#[test]
fn background_compaction_skips_surveys_not_durable_actions() {
    for async_storage in [false, true] {
        for detailed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), async_storage);
            seed(&engine, 32);
            let drain = engine.live_block_slab_ids(1);
            assert!(!drain.is_empty());
            crate::snapshot_probe::reset();
            reset_compaction_flush_publish_observation_for_test();
            let report = engine
                .compact_shard_blocks_relocating_inner(
                    1,
                    u64::MAX,
                    usize::MAX,
                    Some(drain.into_iter().collect()),
                    detailed,
                )
                .unwrap();
            let counts = crate::snapshot_probe::counts();
            assert_eq!(
                compaction_flush_published_under_guard_for_test(),
                Some(true)
            );
            if detailed {
                let report = report.expect("explicit report");
                assert!(report.rewritten_block_refs > 0);
                assert!(!report.model_layouts.is_empty());
                assert!(report.before.live_block_refs > 0 && report.after.live_block_refs > 0);
                assert_eq!(counts.live_slab_scans, 2);
            } else {
                assert!(report.is_none(), "no incomplete report may escape");
                assert_eq!(counts.live_slab_scans, 0);
            }
            check_values(&engine, 32);
            drop(engine);
            check_values(&open(dir.path(), async_storage), 32);
        }
    }
}

#[test]
fn background_compaction_keeps_budget_resume_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 8);
    let mut target = None;
    let mut rounds = 0;
    loop {
        assert!(
            engine
                .compact_shard_blocks_relocating_inner(1, u64::MAX, 1, None, false,)
                .unwrap()
                .is_none()
        );
        rounds += 1;
        let anchor = engine.compaction_rounds.read().unwrap().get(&1).copied();
        let Some((_, slab)) = anchor else { break };
        if let Some(target) = target {
            assert_eq!(slab, target);
        } else {
            target = Some(slab);
        }
        assert!(rounds < 64, "bounded compaction must finish");
    }
    assert!(rounds > 1);
    check_values(&engine, 8);
    drop(engine);
    check_values(&open(dir.path(), false), 8);
}

#[test]
fn background_compaction_partial_failure_is_durable_and_resumable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    crate::engine::compaction::fail_compaction_block_read_after_for_test(Some(4));
    let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    crate::engine::compaction::fail_compaction_block_read_after_for_test(None);
    assert_eq!(
        failed.expect_err("injected read failure").code,
        "block_compaction_failed"
    );
    let anchor = engine.compaction_rounds.read().unwrap().get(&1).copied();
    assert!(anchor.is_some());
    {
        let shards = engine.shards_write_marked();
        assert_the_index_names_what_the_maps_hold(shards.get(&1).unwrap());
    }
    check_values(&engine, 16);
    // Reopen the partially published image before another round can repair anything.
    drop(engine);
    let engine = open(dir.path(), false);
    check_values(&engine, 16);
    assert!(
        engine
            .compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false,)
            .unwrap()
            .is_none()
    );
    assert!(engine.compaction_rounds.read().unwrap().get(&1).is_none());
    drop(engine);
    check_values(&open(dir.path(), false), 16);
}

#[test]
fn background_compaction_still_rejects_misfiled_ownership_before_rolling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 8);
    {
        let mut shards = engine.shards_write_marked();
        let shard = shards.get_mut(&1).unwrap();
        let original = *shard.bucket_index.bucket_map.keys().next().unwrap();
        let mut wrong = original.wrapping_add(1);
        while shard.bucket_index.bucket_map.contains_key(&wrong) { wrong = wrong.wrapping_add(1); }
        let mut bucket = shard.bucket_index.bucket_map.remove(&original).unwrap();
        bucket.routing_bucket = wrong;
        shard.bucket_index.bucket_map.insert(wrong, bucket);
    }
    let slabs_before = engine.block_store().slab_ids().unwrap();
    let failure = engine.compact_shard_blocks_relocating_inner(
        1, u64::MAX, usize::MAX, None, false,
    ).expect_err("wrong bucket must be rejected without report collection");
    assert_eq!(failure.code, "block_compaction_owner_mismatch");
    assert_eq!(engine.block_store().slab_ids().unwrap(), slabs_before);
}

/// A round whose index publish FAILS has already moved the volatile index. The durable index
/// still names the pre-round slabs and no log can replay the relocation, so reclaim (which
/// derives stale slabs from the volatile live set) must keep those slabs until a round publishes.
#[test]
fn a_compaction_whose_index_publish_failed_keeps_the_slabs_the_disk_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    let before = engine.live_block_slab_ids(1);
    assert!(!before.is_empty());
    // Make the index file unwritable: a non-empty directory cannot be renamed over.
    let index_path = engine.index_path(1);
    let _ = std::fs::remove_file(&index_path);
    std::fs::create_dir_all(index_path.join("blocker")).unwrap();
    let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    assert_eq!(
        failed.expect_err("the publish must fail").code,
        "block_compaction_failed"
    );
    let after_failure = engine.live_block_slab_ids(1);
    let all_shards = engine.live_block_slab_ids_all_shards();
    for slab in &before {
        assert!(after_failure.contains(slab), "slab {slab} the on-disk index names was released");
        assert!(all_shards.contains(slab), "slab {slab} released for all-shard reclaim");
    }
    // Once a round publishes, the pin goes and the vacated slabs become reclaimable.
    std::fs::remove_dir_all(&index_path).unwrap();
    assert!(
        engine
            .compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false)
            .unwrap()
            .is_none()
    );
    let after_publish = engine.live_block_slab_ids(1);
    assert!(
        before.iter().any(|slab| !after_publish.contains(slab)),
        "a published round must release the pin"
    );
    check_values(&engine, 16);
}

fn assert_pre_round_slabs_still_live(engine: &TemporalEngine, before: &[u64]) {
    let after = engine.live_block_slab_ids(1);
    let all_shards = engine.live_block_slab_ids_all_shards();
    for slab in before {
        assert!(after.contains(slab), "slab {slab} the on-disk index names was released");
        assert!(all_shards.contains(slab), "slab {slab} released for all-shard reclaim");
    }
}

/// The barrier arm of a normal round, which the publish test above does not reach.
#[test]
fn a_compaction_whose_barrier_failed_keeps_the_slabs_the_disk_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    let before = engine.live_block_slab_ids(1);
    fail_next_compaction_barrier_for_test(true);
    let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    fail_next_compaction_barrier_for_test(false);
    let status = failed.expect_err("the barrier must fail");
    assert!(status.message.contains("before publishing"), "wrong arm: {}", status.message);
    assert_pre_round_slabs_still_live(&engine, &before);
}

/// The barrier arm of the partial-failure handler.
#[test]
fn a_partial_compaction_whose_barrier_failed_keeps_the_slabs_the_disk_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    let before = engine.live_block_slab_ids(1);
    crate::engine::compaction::fail_compaction_block_read_after_for_test(Some(4));
    fail_next_compaction_barrier_for_test(true);
    let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    crate::engine::compaction::fail_compaction_block_read_after_for_test(None);
    fail_next_compaction_barrier_for_test(false);
    let status = failed.expect_err("the barrier must fail");
    assert!(status.message.contains("partial compaction"), "wrong arm: {}", status.message);
    assert_pre_round_slabs_still_live(&engine, &before);
    // Only some blocks moved, so the old slabs stay live by their remaining blocks alone: the hold
    // itself is what must name them.
    let held = engine.unpublished_compaction_slabs.read().unwrap().get(&1).cloned().unwrap_or_default();
    assert!(before.iter().all(|slab| held.contains(slab)), "the hold lost a slab: {held:?} vs {before:?}");
}

/// An unloaded shard holds nothing: the index was flushed and a reload reads it.
#[test]
fn unloading_a_shard_drops_its_unpublished_compaction_hold() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 4);
    fail_next_compaction_barrier_for_test(true);
    let _ = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    fail_next_compaction_barrier_for_test(false);
    assert!(engine.unpublished_compaction_slabs.read().unwrap().contains_key(&1));
    engine.unload_shard(1);
    assert!(!engine.unpublished_compaction_slabs.read().unwrap().contains_key(&1));
}

/// THE LOSS ITSELF, not a proxy for it: a failed barrier leaves the on-disk index naming the old
/// slabs; a reclaim that treats them as stale quarantines and purges them; a restart over that
/// index then cannot read its keys.
#[test]
fn a_failed_compaction_barrier_followed_by_reclaim_and_a_restart_loses_no_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    // Publish the index first: it now carries the applied sequence, so a restart reads the keys
    // through the slabs it names instead of replaying the log.
    engine.flush_shard_index(1);
    fail_next_compaction_barrier_for_test(true);
    let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
    fail_next_compaction_barrier_for_test(false);
    assert!(failed.is_err(), "the barrier must fail");
    // Reclaim as the lifecycle does it: slabs before the newest that the live set does not name.
    let live = engine.live_block_slab_ids_all_shards();
    let newest = *engine.block_store.slab_ids().unwrap().iter().max().unwrap();
    engine
        .block_store
        .gc_slabs_before_with_live_refs_delayed_destroy(newest, live)
        .unwrap();
    engine.block_store.purge_delayed_destroy_slabs_older_than(0).unwrap();
    drop(engine);
    check_values(&open(dir.path(), false), 16);
}

/// Pins accumulate. The second failing round starts from the index the first one already moved,
/// which no longer names the first round's old slabs, so replacing the pin would drop them.
#[test]
fn two_failed_compaction_rounds_keep_the_slabs_of_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 16);
    let before = engine.live_block_slab_ids(1);
    for _ in 0..2 {
        fail_next_compaction_barrier_for_test(true);
        let failed = engine.compact_shard_blocks_relocating_inner(1, u64::MAX, usize::MAX, None, false);
        fail_next_compaction_barrier_for_test(false);
        assert!(failed.is_err(), "the barrier must fail");
    }
    assert_pre_round_slabs_still_live(&engine, &before);
}

/// A drain round is bounded by its drain set: keys with no block on a drain slab are not visited.
#[test]
fn a_drain_round_asks_only_about_keys_that_hold_a_block_on_the_drain_slab() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let mut commands = Vec::new();
    for i in 0..64 {
        commands.push(Command::StringSet { key: format!("key-{i}"), value: vec![i as u8; 96] });
        commands.push(Command::HashSet {
            key: "catalog".into(), field: format!("id-{i}"), value: vec![i as u8; 64],
        });
        commands.push(Command::SetAdd { key: "order".into(), member: format!("id-{i}").into_bytes() });
    }
    let response = engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands });
    assert!(response.status.ok, "{:?}", response.status);
    // Roll the same shard writer used by the seed and the three late writes.
    let drain_slab = engine.block_store.roll_slab_for_shard(1).unwrap().new_block_slab_id;
    for i in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("late-{i}"), value: vec![9u8; 96] },
        });
        assert!(response.status.ok);
    }
    {
        let shards = engine.shards.read().unwrap();
        let mut drain_keys: Vec<_> = collect_live_block_entries(shards.get(&1).unwrap())
            .into_iter()
            .filter(|entry| entry.address.block_slab_id() == drain_slab)
            .map(|entry| entry.object_key.to_string())
            .collect();
        drain_keys.sort_unstable();
        assert_eq!(drain_keys, ["late-0", "late-1", "late-2"]);
    }
    let report = engine
        .compact_shard_blocks_relocating_inner(
            1, u64::MAX, usize::MAX, Some(std::collections::BTreeSet::from([drain_slab])), true,
        )
        .unwrap()
        .expect("detailed report");
    assert_eq!(
        last_compaction_round_decisions_for_test(),
        3,
        "only the three keys on the drain slab may be asked about",
    );
    assert_eq!(report.rewritten_block_refs, 3);
    // 64 strings + 64 hash fields + 64 set members stay where they are, and are still reported.
    assert_eq!(report.blocks_left_off_drain_set, 192);
    for i in 0..64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: format!("key-{i}") },
        });
        assert!(matches!(response.response, CommandResponse::Bytes { value: Some(v) } if v == vec![i as u8; 96]));
    }
    for i in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: format!("late-{i}") },
        });
        assert!(matches!(response.response, CommandResponse::Bytes { value: Some(v) } if v == vec![9u8; 96]));
    }
}


/// A round rebuilds the bucket views from the model maps, and a rebuild must not move the
/// generation a dump manifest is compared against. Each bucket's dirty generation is read before
/// the round and after it.
#[test]
fn a_compaction_round_keeps_every_buckets_dirty_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 8);
    for member in ["id-1", "id-4"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetRemove { key: "order".into(), member: member.as_bytes().to_vec() },
        });
        assert!(response.status.ok);
    }
    let generations = |engine: &TemporalEngine| {
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&1).unwrap();
        shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(bucket, node)| (*bucket, node.dirty_generation))
            .collect::<BTreeMap<u32, u64>>()
    };
    let before = generations(&engine);
    assert!(
        before.values().any(|generation| *generation > 1),
        "the fixture must hold a bucket whose generation a reset would change: {before:?}"
    );
    engine.compact_shard_blocks(1).unwrap();
    assert_eq!(
        generations(&engine),
        before,
        "a compaction round changed a bucket's dirty generation"
    );
}

/// The fields of every bucket a block move cannot change, one line per bucket in bucket order: the
/// watermarks, the flags, the object and deleted-object ids and the tombstone rows. The blocks
/// themselves are left out -- moving one changes its address and handle by design, and a folded
/// container leaves fewer of them -- and are held to account by the model-map agreement, the tally
/// and the lookup total.
fn bucket_what_a_move_leaves(shard: &ShardState) -> Vec<String> {
    let mut rows: Vec<(u32, String)> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(bucket, node)| {
            let text = format!("{node:?}");
            let kept: Vec<&str> = text
                .split(", ")
                .filter(|part| {
                    [
                        "dirty_generation",
                        "first_dirty",
                        "flags",
                        "deleted_object_index",
                        "object_index",
                        "tombstone_elements",
                    ]
                    .iter()
                    .any(|field| part.contains(field))
                })
                .collect();
            (*bucket, kept.join("|"))
        })
        .collect();
    rows.sort();
    rows.into_iter().map(|(bucket, text)| format!("bucket {bucket}: {text}")).collect()
}

/// What a rebuild would derive from the model maps is what the bucket index holds.
fn assert_the_index_names_what_the_maps_hold(shard: &ShardState) {
    // THE INDEX AND THE MODEL MAPS NAME THE SAME BLOCKS: what a rebuild would derive from the maps is
    // what the index holds, so a block the round moved is filed at the address the map now reads.
    // The flag bits an index entry carries beside its address (model, dirty) are left out of the
    // comparison: they are the index's, and the map's entry has none.
    {
        let held: BTreeSet<(String, String)> = collect_live_block_entries(shard)
            .into_iter()
            // A removal's tombstone is the index's own record; the maps hold live elements only.
            .filter(|entry| !entry.deleted)
            .map(|entry| (entry.object_key.to_string(), format!(
                    "slab {} offset {} length {} block {:?}",
                    entry.address.block_slab_id(),
                    entry.address.offset(),
                    entry.address.length(),
                    entry.address.block_id()
                )))
            .collect();
        let derived: BTreeSet<(String, String)> = collect_model_live_block_entries(shard)
            .into_iter()
            .map(|entry| (entry.object_key.to_string(), format!(
                    "slab {} offset {} length {} block {:?}",
                    entry.address.block_slab_id(),
                    entry.address.offset(),
                    entry.address.length(),
                    entry.address.block_id()
                )))
            .collect();
        let only_index: Vec<_> = held.difference(&derived).take(5).collect();
        let only_maps: Vec<_> = derived.difference(&held).take(5).collect();
        assert!(
            only_index.is_empty() && only_maps.is_empty(),
            "the index and the model maps name different blocks after a round: \
             {} only in the index {only_index:?}, {} only in the maps {only_maps:?}",
            held.difference(&derived).count(),
            derived.difference(&held).count()
        );
    }
}

/// A round that moves blocks updates the bucket index for just those blocks, and must leave
/// everything else in it as it was: every bucket keeps its watermarks, flags, deleted-object
/// ids and tombstone rows, the per-slab live tally agrees with a walk of the index, and the lookup
/// total is unchanged. A whole rebuild is NOT the oracle: it re-derives from the model maps and so
/// forgets what only the index holds (a deleted object's marker, a bucket's first-dirty sequences).
#[test]
fn a_round_that_updates_the_bucket_index_in_place_changes_only_the_blocks_it_moved() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 8);
    for member in ["id-1", "id-4"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetRemove { key: "order".into(), member: member.as_bytes().to_vec() },
        });
        assert!(response.status.ok);
    }
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringDelete { key: "key-3".into() },
    });
    assert!(response.status.ok);

    let before = {
        let shards = engine.shards_write_marked();
        bucket_what_a_move_leaves(shards.get(&1).unwrap())
    };
    engine.compact_shard_blocks(1).unwrap();
    assert_eq!(
        last_round_index_update_for_test(),
        1,
        "the round fell back to a rebuild, so this would test the rebuild against itself"
    );
    let mut shards = engine.shards_write_marked();
    let shard = shards.get_mut(&1).unwrap();
    assert_eq!(
        bucket_what_a_move_leaves(shard),
        before,
        "the round changed more of the bucket index than the blocks it moved"
    );
    assert_the_index_names_what_the_maps_hold(shard);
    // THE COUNTS A REBUILD WOULD EARN, for the two figures the in-place update maintains by hand:
    // the lookup total and the per-slab tally. The blocks agree with the maps (above), so a rebuild
    // re-earns exactly these, and the ones the round left must be the same.
    let lookup_after_round = shard.bucket_index.object_element_name_block_refs;
    let drift = reconcile_block_slab_live(shard);
    assert!(drift.slabs_compared > 0, "no slab was compared: {drift:?}");
    assert_eq!(drift.drifted_slabs, 0, "the live tally disagrees with a walk of the index: {drift:?}");
    let (start, end) = shard.routing_range();
    rebuild_bucket_block_ownership(1, shard, start, end);
    assert_eq!(
        shard.bucket_index.object_element_name_block_refs, lookup_after_round,
        "the round left a different number of block lookup refs than a rebuild earns"
    );
}
