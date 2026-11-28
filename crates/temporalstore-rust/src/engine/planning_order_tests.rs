// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn previous_order(
    summaries: &[BucketStorageSummary],
    claims: &BTreeMap<u32, (u64, u64)>,
) -> (Vec<u32>, usize) {
    let lookups = std::cell::Cell::new(0);
    let mut rows = summaries
        .iter()
        .filter(|row| {
            lookups.set(lookups.get() + 1);
            claims.contains_key(&row.routing_bucket)
        })
        .collect::<Vec<_>>();
    let rank = |bucket| {
        lookups.set(lookups.get() + 1);
        match claims.get(&bucket).map(|claim| claim.0) {
            Some(0) | None => u64::MAX,
            Some(sequence) => sequence,
        }
    };
    rows.sort_by(|left, right| {
        rank(left.routing_bucket)
            .cmp(&rank(right.routing_bucket))
            .then_with(|| left.last_dump_sequence.cmp(&right.last_dump_sequence))
            .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
    });
    (rows.iter().map(|row| row.routing_bucket).collect(), lookups.get())
}

fn fixture(count: u32, seed: u64) -> (Vec<BucketStorageSummary>, BTreeMap<u32, (u64, u64)>) {
    let mut summaries = Vec::new();
    let mut claims = BTreeMap::new();
    let mut random = seed;
    for routing_bucket in 0..count {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        summaries.push(BucketStorageSummary {
            routing_bucket,
            last_dump_sequence: random % 19,
            ..Default::default()
        });
        let wal = match routing_bucket % 11 {
            0 => continue,
            1 => 0,
            2 => u64::MAX,
            3 => u64::MAX - 1,
            _ => random % 97 + 1,
        };
        claims.insert(routing_bucket, (wal, random));
    }
    for index in (1..summaries.len()).rev() {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        summaries.swap(index, random as usize % (index + 1));
    }
    (summaries, claims)
}

#[test]
fn ranked_dump_order_preserves_all_ties_and_unknown_claims() {
    for count in [0, 1, 2, 31, 257, 4096] {
        for seed in [1, 7, 0x9e3779b97f4a7c15] {
            let (summaries, claims) = fixture(count, seed);
            let expected = previous_order(&summaries, &claims).0;
            let actual = oldest_dirty_buckets(&summaries, &claims);
            assert_eq!(actual, expected, "count={count} seed={seed}");
            for cap in [0, 1, 2, 64, actual.len(), actual.len() + 1] {
                assert_eq!(
                    actual.iter().take(cap).collect::<Vec<_>>(),
                    expected.iter().take(cap).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn dump_rank_lookups_are_linear_in_candidate_count() {
    for count in [4096, 32768] {
        let (summaries, claims) = fixture(count, 0x123456789abcdef);
        let (expected, previous_lookups) = previous_order(&summaries, &claims);
        DUMP_RANK_LOOKUPS.with(|lookups| lookups.set(0));
        let actual = oldest_dirty_buckets(&summaries, &claims);
        let lookups = DUMP_RANK_LOOKUPS.with(std::cell::Cell::get);
        assert_eq!(actual, expected);
        assert_eq!(lookups, count as usize);
        assert!(previous_lookups > lookups * 4, "the control did not expose repeated lookups");
        assert!(actual.capacity() < count as usize * 2, "rank tuples leaked into output capacity");
        eprintln!("DUMP_ORDER candidates={count} selected={} lookups={lookups} previous_lookups={previous_lookups}", actual.len());
    }
}

#[test]
fn claim_ranks_match_manifest_augmented_summaries() {
    for count in [0, 1, 31, 257, 4096] {
        for seed in [1, 7, 0x9e3779b97f4a7c15] {
            let (mut summaries, claims) = fixture(count, seed);
            for summary in &mut summaries {
                summary.last_dump_sequence = 0;
            }
            assert_eq!(oldest_dirty_bucket_claims(&claims, None), oldest_dirty_buckets(&summaries, &claims));
            let manifest = BucketDumpManifest {
                index_log_sequence: 19,
                bucket_ids: summaries.iter().filter(|summary| summary.routing_bucket % 3 == 0)
                    .map(|summary| summary.routing_bucket).collect(),
                ..Default::default()
            };
            let summaries = storage_reporting::merge_last_dump_sequence(summaries, &manifest);
            assert_eq!(oldest_dirty_bucket_claims(&claims, Some(&manifest)), oldest_dirty_buckets(&summaries, &claims));
        }
    }
}

fn pressure_engine(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..128 {
        assert!(engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("pressure-{index:04}"), value: vec![b'v'; 96],
            },
        }).status.ok);
    }
    engine
}

#[test]
fn pressure_plan_reuses_one_claim_snapshot_without_changing_dump_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = pressure_engine(dir.path());
    for after_capped_dump in [false, true] {
        if after_capped_dump {
            let before = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1, max_dump_buckets_per_round: 2, ..Default::default()
            });
            assert!(before.dirty_buckets.len() > 2);
            assert_eq!(before.selected_dump_buckets.len(), 2);
            let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
                shard_id: 1, max_dump_buckets_per_round: 2, ..Default::default()
            });
            let mut expected_manifest_buckets = before.selected_dump_buckets.clone();
            expected_manifest_buckets.sort_unstable();
            assert_eq!(report.dump_manifest.unwrap().bucket_ids, expected_manifest_buckets);
            let after = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1, max_dump_buckets_per_round: 2, ..Default::default()
            });
            assert_eq!(after.dirty_buckets.len() + 2, before.dirty_buckets.len());
        }
        for unknown_claim in [false, true] {
            let restore_claim = if unknown_claim {
                engine.shards.with_state_mut(1, |state| {
                    let shard = state.unwrap();
                    let bucket = *shard.bucket_index.bucket_map.dirty_buckets().first().unwrap();
                    let mut node = shard.bucket_index.bucket_map.get_mut(&bucket).unwrap();
                    let claim = node.first_dirty_wal_sequence;
                    node.first_dirty_wal_sequence = 0;
                    Some((bucket, claim))
                })
            } else {
                None
            };
            let expected_bytes = engine.undumped_wal_bytes_above_oldest_dirty(1);
            assert!(expected_bytes > 0);
            for detailed in [false, true] {
                for minimum in [0, 1, expected_bytes, expected_bytes + 1] {
                    let request = StorageLifecycleRequest {
                        shard_id: 1, max_dump_buckets_per_round: 2,
                        min_undumped_wal_records: u64::MAX, min_undumped_wal_bytes: minimum,
                        ..Default::default()
                    };
                    let expected = engine.storage_lifecycle_plan_with_report_details(request.clone(), detailed);
                    storage_reporting::DIRTY_CLAIM_BUILDS_FOR_TEST.with(|count| count.set(0));
                    let (actual, bytes) = engine.storage_lifecycle_pressure_plan(request, detailed);
                    assert_eq!(storage_reporting::DIRTY_CLAIM_BUILDS_FOR_TEST.with(|count| count.get()), 1);
                    assert_eq!(bytes, expected_bytes);
                    assert_eq!(actual, expected);
                    assert_eq!(actual.dump_delayed, minimum == 0 || expected_bytes < minimum);
                    assert_eq!(actual.selected_dump_buckets.is_empty(), actual.dump_delayed);
                }
            }
            if let Some((bucket, claim)) = restore_claim {
                engine.shards.with_state_mut(1, |state| {
                    state.unwrap()
                        .bucket_index.bucket_map.get_mut(&bucket).unwrap().first_dirty_wal_sequence = claim;
                });
            }
        }
    }
}

#[test]
fn runtime_pressure_snapshot_collects_dirty_claims_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = pressure_engine(dir.path());
    let expected_bytes = engine.undumped_wal_bytes_above_oldest_dirty(1);
    let runtime = crate::data_node::DataNodeRuntime::new_without_workers_with_options(
        engine.clone(), crate::data_node::DataNodeRuntimeOptions {
            worker_threads: 0, max_queue_depth: 64, max_background_queue_depth: 8,
        },
    );
    storage_reporting::DIRTY_CLAIM_BUILDS_FOR_TEST.with(|count| count.set(0));
    let (pressure, plan) = runtime.storage_manager_pressure_snapshot(
        1, &crate::data_node::StorageManagerOptions::default(),
    );
    assert!(pressure.dirty_bucket_count > 0);
    assert_eq!(pressure.dirty_bucket_count, plan.dirty_buckets.len());
    assert_eq!(pressure.wal_bytes, expected_bytes);
    assert_eq!(storage_reporting::DIRTY_CLAIM_BUILDS_FOR_TEST.with(|count| count.get()), 1);
    engine.undumped_wal_bytes_above_oldest_dirty(1);
    assert_eq!(storage_reporting::DIRTY_CLAIM_BUILDS_FOR_TEST.with(|count| count.get()), 2);
}

#[test]
fn captured_claim_summary_matches_independent_bucket_validation() {
    for clean_stride in [0, 2, 1] {
        let dir = tempfile::tempdir().unwrap();
        let engine = pressure_engine(dir.path());
        engine.shards.with_state_mut(1, |state| {
            let shard = state.unwrap();
            let ids: Vec<_> = shard.bucket_index.bucket_map.keys().copied().collect();
            assert!(!ids.is_empty());
            assert_eq!(shard.bucket_index.bucket_map.dirty_buckets().len(), ids.len());
            if clean_stride > 0 {
                for (index, key) in ids.iter().enumerate() {
                    if index % clean_stride == 0 {
                        shard.bucket_index.bucket_map.get_mut(key).unwrap().set_dirty(false);
                    }
                }
            }
            let buckets = &shard.bucket_index.bucket_map;
            let expected: BTreeMap<_, _> = buckets.dirty_buckets().iter().filter_map(|key| {
                buckets.get(key).map(|node| (*key, (node.first_dirty_wal_sequence, node.first_dirty_index_log_sequence)))
            }).collect();
            let expected_marked = expected.keys().all(|key| buckets.get(key).unwrap().dirty());
            let (actual, marked) = storage_reporting::dirty_bucket_claims_with_summary_state(shard);
            assert_eq!(actual, expected);
            assert_eq!(marked, expected_marked);
            assert_eq!(storage_reporting::dirty_bucket_claims(shard), expected);
        });
    }
}

#[test]
fn inconsistent_dirty_membership_still_requires_authoritative_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = pressure_engine(dir.path());
    let changed_bucket = engine.shards.with_state_mut(1, |state| {
        let shard = state.unwrap();
        let buckets = &mut shard.bucket_index.bucket_map;
        assert_eq!(buckets.dirty_buckets().len(), buckets.len());
        let changed_bucket = *buckets.dirty_buckets().first().unwrap();
        let mut node = buckets.get_mut(&changed_bucket).unwrap();
        node.set_dirty(false);
        // Simulate stale membership without running the mutation guard.
        std::mem::forget(node);
        assert!(buckets.dirty_buckets().contains(&changed_bucket));
        let (claims, all_marked) = storage_reporting::dirty_bucket_claims_with_summary_state(shard);
        assert!(claims.contains_key(&changed_bucket));
        assert!(!all_marked);
        changed_bucket
    });
    let request = StorageLifecycleRequest {
        shard_id: 1, max_dump_buckets_per_round: 2, ..Default::default()
    };
    let conservative = engine.storage_lifecycle_plan_with_report_details(request.clone(), false);
    let detailed = engine.storage_lifecycle_plan_with_report_details(request, true);
    assert!(conservative.bucket_summaries.is_some());
    assert_eq!(conservative.dirty_buckets, detailed.dirty_buckets);
    assert_eq!(conservative.selected_dump_buckets, detailed.selected_dump_buckets);
    engine.shards.with_state_mut(1, |state| {
        state.unwrap().bucket_index.bucket_map
            .get_mut(&changed_bucket).unwrap().set_dirty(true);
    });
}

#[test]
fn uncovered_dirty_log_pins_agree_with_full_reclaim_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = pressure_engine(dir.path());
    let pin = engine.shards.with_state_mut(1, |state| {
        let shard = state.unwrap();
        for node in shard.bucket_index.bucket_map.values_mut() {
            node.first_dirty_wal_sequence = 2;
            node.first_dirty_index_log_sequence = 2;
        }
        *shard.bucket_index.bucket_map.keys().next().unwrap()
    });
    for wal in [0, 1, 2, u64::MAX] {
        for index in [0, 1, 2, u64::MAX] {
            engine.shards.with_state_mut(1, |state| {
                let mut node = state.unwrap().bucket_index.bucket_map.get_mut(&pin).unwrap();
                node.first_dirty_wal_sequence = wal;
                node.first_dirty_index_log_sequence = index;
            });
            crate::engine::reset_live_block_scan_entries();
            let pinned = engine.periodic_wal_reclaim_is_pinned(1);
            assert_eq!(pinned, wal <= 1 || index == 0, "wal={wal} index={index}");
            assert_eq!(crate::engine::live_block_scan_entries(), 0);
            if pinned {
                let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
                assert!(!plan.safe_to_reclaim, "wal={wal} index={index}: {plan:?}");
                let before = engine.write_ahead_log_store().stats_without_rescan(1);
                let report = engine.apply_storage_wal_reclaim(plan);
                assert!(!report.applied);
                assert_eq!(report.wal_records_removed, 0);
                assert_eq!(engine.write_ahead_log_store().stats_without_rescan(1).persistent_bytes, before.persistent_bytes);
            }
        }
    }
}

#[test]
fn covered_clean_and_stale_dirty_membership_do_not_supply_a_log_pin() {
    let dir = tempfile::tempdir().unwrap();
    let engine = pressure_engine(dir.path());
    let pin = engine.shards.with_state_mut(1, |state| {
        let shard = state.unwrap();
        for node in shard.bucket_index.bucket_map.values_mut() {
            node.first_dirty_wal_sequence = 2;
            node.first_dirty_index_log_sequence = 2;
        }
        *shard.bucket_index.bucket_map.keys().next().unwrap()
    });
    engine.shards.with_state_mut(1, |state| {
        state.unwrap().bucket_index.bucket_map
            .get_mut(&pin).unwrap().first_dirty_wal_sequence = 1;
    });
    assert!(engine.periodic_wal_reclaim_is_pinned(1));
    engine.shards.with_state_mut(1, |state| {
        let buckets = &mut state.unwrap().bucket_index.bucket_map;
        assert!(buckets.dirty_buckets().contains(&pin));
        let mut node = buckets.get_mut(&pin).unwrap();
        node.set_dirty(false);
        // A stale cached membership must not invent a dirty generation.
        std::mem::forget(node);
        assert!(buckets.dirty_buckets().contains(&pin));
    });
    assert!(!engine.periodic_wal_reclaim_is_pinned(1));
    engine.shards.with_state_mut(1, |state| {
        state.unwrap().bucket_index.bucket_map
            .get_mut(&pin).unwrap().set_dirty(true);
    });
    let manifest = engine.create_bucket_dump_manifest(1, vec![pin]).unwrap();
    assert!(manifest.bucket_summaries.iter().any(|summary| summary.routing_bucket == pin));
    assert!(!engine.periodic_wal_reclaim_is_pinned(1));
    engine.clear_dumped_bucket_dirty_state(1, &manifest);
    assert!(!engine.periodic_wal_reclaim_is_pinned(1));
    assert!(!engine.periodic_wal_reclaim_is_pinned(999));
}

#[test]
fn periodic_page_log_declines_a_proven_pin_and_retries_after_dump() {
    for hash in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..128 {
            let key = format!("page-pin-{index:04}");
            let command = if hash {
                Command::HashSet { key, field: "f".into(), value: vec![b'v'; 64] }
            } else {
                Command::StringSet { key, value: vec![b'v'; 64] }
            };
            assert!(engine.execute(ExecuteRequest { shard_id: 1, command }).status.ok);
        }
        let (pin, other) = engine.shards.with_state(1, |state| {
            let nodes = &state.unwrap().bucket_index.bucket_map;
            let pin = *nodes.iter().find(|(_, node)| node.first_dirty_wal_sequence == 1).unwrap().0;
            let other = *nodes.keys().find(|bucket| **bucket != pin).unwrap();
            (pin, other)
        });
        let manifest = engine.create_bucket_dump_manifest(1, vec![other]).unwrap();
        engine.clear_dumped_bucket_dirty_state(1, &manifest);
        assert!(engine.storage_dump_wal_sequence(1).unwrap() > 0);
        assert!(engine.periodic_wal_reclaim_is_pinned(1));
        assert!(!engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new()).safe_to_reclaim);
        let runtime = crate::data_node::DataNodeRuntime::new_without_workers_with_options(
            engine.clone(), crate::data_node::DataNodeRuntimeOptions {
                worker_threads: 0, max_queue_depth: 64, max_background_queue_depth: 8,
            },
        );
        let options = crate::data_node::StorageManagerOptions {
            enable_prepare: false, enable_wal_reclaim: false, enable_memory_reclaim: false,
            enable_expire: false, enable_evict: false, enable_block_gc: true,
            enable_block_compaction: false, enable_index_gc: false, enable_metrics_reap: false,
            ..Default::default()
        };
        let before = storage_plan_build_counts().1;
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert!(report.status.ok, "{report:?}");
        assert_eq!(storage_plan_build_counts().1, before, "blocked page check built a full plan");
        let manifest = engine.create_bucket_dump_manifest(1, vec![pin]).unwrap();
        engine.clear_dumped_bucket_dirty_state(1, &manifest);
        // Fully publish the rest so no other unknown claim can stand in for the first pin.
        let buckets = engine.shards.with_state(1, |state| {
            state.unwrap().bucket_index.bucket_map.dirty_buckets().iter().copied().collect::<Vec<_>>()
        });
        assert!(!buckets.is_empty());
        let manifest = engine.create_bucket_dump_manifest(1, buckets).unwrap();
        engine.clear_dumped_bucket_dirty_state(1, &manifest);
        assert!(!engine.periodic_wal_reclaim_is_pinned(1));
        let before = storage_plan_build_counts().1;
        let report = runtime.run_storage_manager_once(1, options);
        assert!(report.status.ok, "{report:?}");
        assert_eq!(storage_plan_build_counts().1, before + 1, "possible reclaim never reached full validation");
        for index in 0..128 {
            let key = format!("page-pin-{index:04}");
            let command = if hash { Command::HashGet { key, field: "f".into() } }
                else { Command::StringGet { key } };
            assert_eq!(engine.execute(ExecuteRequest { shard_id: 1, command }).response,
                CommandResponse::Bytes { value: Some(vec![b'v'; 64]) });
        }
    }
}

#[test]
fn wal_reclaim_refuses_a_plan_from_before_a_durable_base_replacement() {
    let root = tempfile::tempdir().unwrap();
    let open = || TemporalEngine::with_local_dirs(
        16 << 20, root.path().join("cache"), root.path().join("blocks"), root.path().join("indexes"),
    );
    let engine = open();
    engine.load_shard(1);
    let write = |key: &str, value: u8| {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: key.into(), value: vec![value; 4096] },
        });
        assert!(response.status.ok, "{:?}", response.status);
    };
    write("base", 1);
    engine.materialize_synthetic_blocks(1);
    engine.flush_shard_index(1);
    let index_path = engine.index_path(1);
    let old_bytes = std::fs::read(&index_path).unwrap();
    let old_anchor = base_checkpoint_anchor::read(&index_path);
    assert!(old_anchor > 0);
    for (key, value) in [("tail-a", 2), ("tail-b", 3), ("tail-c", 4)] {
        write(key, value);
    }
    engine.materialize_synthetic_blocks(1);
    engine.flush_shard_index(1);
    let newer_anchor = base_checkpoint_anchor::read(&index_path);
    assert!(newer_anchor >= old_anchor + 3);
    assert_eq!(engine.synthetic_address_count_for_test(1), 0);

    let current = engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(current.wal_sequence, newer_anchor);
    engine.clear_dumped_bucket_dirty_state(1, &current);
    assert!(engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1, ..Default::default()
    }).dirty_buckets.is_empty());
    // Remove the redundant manifest from this fixture while the durable base
    // still covers the same records, leaving that base as the sole replay image.
    std::fs::remove_file(bucket_dump_manifest_path(&engine.index_dir, 1, &current.manifest_id)).unwrap();
    assert!(engine.list_bucket_dump_manifests(1).is_empty());
    let stale = engine.storage_wal_reclaim_plan(1, [], []);
    assert!(stale.safe_to_reclaim, "{stale:?}");
    assert_eq!(stale.current_wal_sequence, newer_anchor);
    assert_eq!(stale.durable_bucket_generation_frontier_wal_sequence, newer_anchor);
    assert_eq!(stale.retain_from_wal_sequence, newer_anchor + 1);
    engine.install_index_bytes(1, &old_bytes,
        crate::engine::routing_range_stamp::RoutingRangeStamp {
            start_routing_bucket: 0, end_routing_bucket: u32::MAX,
        },
    ).unwrap();
    assert_eq!(std::fs::read(&index_path).unwrap(), old_bytes);
    assert_eq!(base_checkpoint_anchor::read(&index_path), old_anchor);

    let collected = engine.apply_storage_wal_reclaim(stale);
    assert!(!collected.applied, "{collected:?}");
    assert!(collected.plan.blocker_reasons.iter().any(|reason| reason == "reclaim_replay_anchor_changed"));
    assert_eq!(collected.wal_records_removed, 0);
    drop(engine);
    let reopened = open();
    reopened.load_shard(1);
    for (key, value) in [("base", 1), ("tail-a", 2), ("tail-b", 3), ("tail-c", 4)] {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: key.into() },
        });
        assert!(response.status.ok, "{key}: {:?}", response.status);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![value; 4096]) });
    }
}
thread_local! {
    static BEFORE_WAL_RECLAIM_TRUNCATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>>
        = const { std::cell::RefCell::new(None) };
}

pub(super) fn before_wal_reclaim_truncate() {
    let hook = BEFORE_WAL_RECLAIM_TRUNCATE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[test]
fn manifest_install_drains_admitted_wal_reclaim_without_blocking_peer_publication() {
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    let timeout = Duration::from_secs(10);
    let root = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.path().join("cache"), root.path().join("blocks"), root.path().join("indexes"),
    );
    for id in [1, 2] {
        engine.load_shard(id);
    }
    let write = |id, key: &str| engine.execute_durable(ExecuteRequest {
        shard_id: id,
        command: Command::StringSet { key: key.into(), value: vec![7; 4096] },
    });
    assert!(write(1, "base").status.ok);
    engine.materialize_synthetic_blocks(1);
    engine.flush_shard_index(1);
    let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &manifest);
    assert!(engine.bucket_dump_install_preflight_report(&manifest).install_safe);
    let plan = engine.storage_wal_reclaim_plan(1, [], []);
    assert!(plan.safe_to_reclaim && plan.retain_from_wal_sequence > 1, "{plan:?}");

    let before = engine.checkpoint_publications.capture(1);
    assert_eq!(engine.checkpoint_publications.publish(&before, || Ok(())).unwrap(), Some(()));
    let old_base = std::fs::read(engine.index_path(1)).unwrap();
    std::thread::scope(|scope| {
        let (proven_tx, proven_rx) = channel();
        let (release_tx, release_rx) = channel();
        let collecting = &engine;
        let collector = scope.spawn(move || {
            BEFORE_WAL_RECLAIM_TRUNCATE.with(|slot| *slot.borrow_mut() = Some(Box::new(move || {
                proven_tx.send(()).unwrap();
                release_rx.recv_timeout(timeout).unwrap();
            })));
            collecting.apply_storage_wal_reclaim(plan)
        });
        proven_rx.recv_timeout(timeout).unwrap();
        let install = scope.spawn(|| engine.install_bucket_dump_manifest(&manifest));
        let deadline = Instant::now() + timeout;
        while engine.checkpoint_publications.is_current_after_publish(&before) {
            assert!(Instant::now() < deadline, "install never marked the publication closing");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(engine.pass_slot(1).try_lock().is_err());
        let markers = engine.interrupted_bucket_dump_installs(1);
        assert!(markers.iter().any(|marker| marker.phase == "prepare"));
        assert!(!markers.iter().any(|marker| marker.phase == "install"));
        assert_eq!(std::fs::read(engine.index_path(1)).unwrap(), old_base);

        let (peer_tx, peer_rx) = channel();
        let peer_engine = &engine;
        let peer_write = &write;
        let peer = scope.spawn(move || {
            let result = peer_write(2, "peer");
            peer_engine.flush_shard_index(2);
            peer_tx.send((result, base_checkpoint_anchor::read(&peer_engine.index_path(2)))).unwrap();
        });
        let progress = peer_rx.recv_timeout(timeout);
        release_tx.send(()).unwrap();
        peer.join().unwrap();
        let (response, anchor) = progress.expect("peer append or publication waited for shard 1");
        assert!(response.status.ok);
        assert!(anchor > 0);
        assert!(collector.join().unwrap().applied);
        install.join().unwrap().unwrap();
    });
    for (id, key) in [(1, "base"), (2, "peer")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: id, command: Command::StringGet { key: key.into() },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![7; 4096]) });
    }
}
