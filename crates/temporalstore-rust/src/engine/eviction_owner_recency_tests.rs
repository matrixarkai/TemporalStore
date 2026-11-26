// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);

fn fixture() -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    for shard_id in [1, 2] {
        engine.load_shard(shard_id);
        command(&engine, shard_id, Command::StringSet {
            key: "resident".into(), value: vec![shard_id as u8; 64],
        });
    }
    (dir, engine)
}

fn command(engine: &TemporalEngine, shard_id: u64, command: Command) -> CommandResponse {
    let result = engine.execute(ExecuteRequest { shard_id, command });
    assert!(result.status.ok, "{:?}", result.status);
    result.response
}

fn bucket(engine: &TemporalEngine, shard_id: u64) -> u32 {
    engine.shards.with_state(shard_id, |state| {
        *state.unwrap().bucket_index.bucket_map.keys().next().unwrap()
    })
}

fn assert_value(engine: &TemporalEngine, shard_id: u64, key: &str, value: Vec<u8>) {
    assert_eq!(command(engine, shard_id, Command::StringGet { key: key.into() }),
        CommandResponse::Bytes { value: Some(value) });
}

fn pending_reads(engine: &TemporalEngine) -> HashMap<(u64, u32), u64> {
    assert_value(engine, 1, "resident", vec![1; 64]);
    assert_value(engine, 2, "resident", vec![2; 64]);
    let pending = pending_stamps(engine);
    for shard_id in [1, 2] {
        assert!(pending.get(&(shard_id, bucket(engine, shard_id))).copied().unwrap_or(0) > 0,
            "the real read must leave a pending owner stamp");
    }
    pending
}

fn pending_stamps(engine: &TemporalEngine) -> HashMap<(u64, u32), u64> {
    let mut pending = HashMap::new();
    for id in engine.shards.shard_ids() {
        if let Some(owner) = engine.shards.handle(id) {
            owner.with_fast_read_recency_for_test(|stamps| {
                pending.extend(stamps.iter().map(|(&bucket, &stamp)| ((id, bucket), stamp)));
            });
        }
    }
    pending
}

// Release the peer before joining the eviction worker even when the progress deadline fails.
// The old all-shard fold then terminates and fails the assertion instead of hanging the suite.
fn with_parked_peer(
    engine: &TemporalEngine,
    run: impl FnOnce() -> StorageEvictionReport + Send,
) -> (StorageEvictionReport, HashMap<(u64, u32), u64>) {
    std::thread::scope(|scope| {
        let (held_tx, held_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let peer = scope.spawn(move || {
            engine.shards.with_state_mut(2, |state| {
                assert!(state.is_some());
                held_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(15)).unwrap();
            });
        });
        held_rx.recv_timeout(WAIT).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = scope.spawn(move || { done_tx.send(run()).unwrap(); });
        let completed = done_rx.recv_timeout(WAIT);
        let pending = pending_stamps(engine);
        release_tx.send(()).unwrap();
        peer.join().unwrap();
        worker.join().unwrap();
        (completed.expect("target eviction waited for a parked peer owner"), pending)
    })
}

#[test]
fn pressured_eviction_and_resident_bound_leave_parked_peer_stamps_pending() {
    for bound_route in [false, true] {
        let (_dir, engine) = fixture();
        let a = bucket(&engine, 1);
        let b = bucket(&engine, 2);
        let resident = engine.bucket_index_resident_bytes(1);
        assert!(resident > 1);
        let before = pending_reads(&engine);
        let (report, pending) = with_parked_peer(&engine, || {
            if bound_route {
                engine.apply_index_resident_bound(1, resident - 1, 1)
            } else {
                engine.apply_storage_eviction(1, resident - 1, 1, false, false)
            }
        });
        assert!(report.pressure_gate_open);
        assert_eq!(report.selected_victims.len(), 1);
        assert_eq!(report.dropped_object_count, 0);
        assert!(!pending.contains_key(&(1, a)));
        assert_eq!(pending.get(&(2, b)), before.get(&(2, b)));
        assert_value(&engine, 1, "resident", vec![1; 64]);
        assert_value(&engine, 2, "resident", vec![2; 64]);
    }
}

#[test]
fn declined_pressure_and_bound_gates_do_not_fold_either_owner() {
    for route in 0..3 {
        let (_dir, engine) = fixture();
        let resident = engine.bucket_index_resident_bytes(1);
        assert!(resident > 0);
        let stamps = pending_reads(&engine);
        let (report, pending) = with_parked_peer(&engine, || match route {
            0 => engine.apply_storage_eviction(1, resident, 1, false, false),
            1 => engine.apply_index_resident_bound(1, resident, 1),
            _ => engine.apply_index_resident_bound(1, 0, 1),
        });
        assert!(!report.pressure_gate_open);
        assert!(report.selected_victims.is_empty());
        assert_eq!(pending, stamps);
        assert_value(&engine, 1, "resident", vec![1; 64]);
        assert_value(&engine, 2, "resident", vec![2; 64]);
    }
}

#[test]
fn a_fast_hot_read_is_folded_before_sampled_victim_ranking() {
    let (_dir, engine) = fixture();
    let hot = bucket(&engine, 1);
    let (cold_key, cold) = (0..1024).find_map(|i| {
        let key = format!("cold-{i}");
        command(&engine, 1, Command::StringSet { key: key.clone(), value: vec![3; 64] });
        engine.shards.with_state(1, |state| {
            state.unwrap().bucket_index.bucket_map.keys().copied()
                .find(|id| *id != hot).map(|id| (key, id))
        })
    }).expect("fixture must contain distinct hot and cold routing buckets");
    drop(engine.shards.handle(1).unwrap().take_fast_read_recency());
    engine.shards.with_state_mut(1, |state| {
        let state = state.unwrap();
        state.bucket_recency.insert(hot, 0);
        state.bucket_recency.insert(cold, 1);
    });
    assert_value(&engine, 1, "resident", vec![1; 64]);
    let stamp = *pending_stamps(&engine).get(&(1, hot))
        .expect("the real hot read must take the deferred-recency path");
    assert!(stamp > 1);
    let report = engine.apply_storage_eviction(1, 0, 1, false, false);
    assert_eq!(report.selected_victims.iter().map(|v| v.routing_bucket).collect::<Vec<_>>(), vec![cold]);
    engine.shards.with_state(1, |state| {
        assert_eq!(state.unwrap().bucket_recency.get(&hot), Some(&stamp));
    });
    assert_value(&engine, 1, "resident", vec![1; 64]);
    assert_value(&engine, 1, &cold_key, vec![3; 64]);
}

#[test]
fn repeated_same_key_stamps_preserve_the_later_owner_recency() {
    let (_dir, engine) = fixture();
    let a = bucket(&engine, 1);
    for (owner_stamp, pending_stamp, expected) in [(20, 10, 20), (20, 30, 30)] {
        engine.shards.with_state_mut(1, |state| {
            state.unwrap().bucket_recency.insert(a, owner_stamp);
        });
        engine.shards.handle(1).unwrap().note_fast_read_recency(a, pending_stamp);
        engine.fold_fast_read_recency_for_shard(1);
        engine.shards.with_state(1, |state| {
            assert_eq!(state.unwrap().bucket_recency.get(&a), Some(&expected));
        });
        assert!(!pending_stamps(&engine).contains_key(&(1, a)));
    }
}

#[test]
fn a_captured_recency_owner_cannot_mutate_its_replacement() {
    let (_dir, engine) = fixture();
    let old = engine.shards.handle(1).unwrap();
    let replacement = engine.shards.with_state(1, |state| state.unwrap().clone());
    let retired = engine.shards.install(1, replacement);
    assert!(retired.is_some());
    let current = engine.shards.handle(1).unwrap();
    assert!(!old.same_incarnation(&current));
    let a = bucket(&engine, 1);
    engine.shards.with_state_mut(1, |state| { state.unwrap().bucket_recency.insert(a, 40); });
    // This is the captured-handle application primitive used by the existing fold helper.
    engine.shards.with_handle_state_mut(&old, |state| {
        assert!(state.is_none(), "retired capture must not resolve by shard ID again");
    });
    old.note_fast_read_recency(a, 50);
    engine.fold_fast_read_recency_for_shard(1);
    current.with_read(|state| { assert_eq!(state.unwrap().bucket_recency.get(&a), Some(&40)); });
    assert_eq!(old.take_fast_read_recency().get(&a), Some(&50));
    drop(retired);
    assert_value(&engine, 1, "resident", vec![1; 64]);
}

#[test]
fn a_parked_peer_stamp_mutex_does_not_block_target_read_or_eviction() {
    for bound_route in [false, true] {
        let (_dir, engine) = fixture();
        let before = pending_reads(&engine);
        let peer_owner = engine.shards.handle(2).unwrap();
        let b = bucket(&engine, 2);
        let resident = engine.bucket_index_resident_bytes(1);
        let observed = std::thread::scope(|scope| {
            let (held, ready) = mpsc::channel();
            let (release, resume) = mpsc::channel();
            let parked = scope.spawn(move || peer_owner.with_fast_read_recency_for_test(|_| {
                held.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(15)).unwrap();
            }));
            ready.recv_timeout(WAIT).unwrap();
            let (done, completed) = mpsc::channel();
            let engine = &engine;
            let worker = scope.spawn(move || {
                assert_value(engine, 1, "resident", vec![1; 64]);
                let report = if bound_route {
                    engine.apply_index_resident_bound(1, resident - 1, 1)
                } else {
                    engine.apply_storage_eviction(1, resident - 1, 1, false, false)
                };
                done.send(report).unwrap();
            });
            let result = completed.recv_timeout(WAIT);
            release.send(()).unwrap();
            parked.join().unwrap();
            worker.join().unwrap();
            result
        }).expect("unrelated owner stamp mutex blocked target work");
        assert!(observed.pressure_gate_open);
        assert_eq!(observed.selected_victims.len(), 1);
        assert_eq!(pending_stamps(&engine).get(&(2, b)), before.get(&(2, b)));
    }
}

#[test]
fn late_fast_plan_stamps_only_its_captured_incarnation() {
    let (_dir, engine) = fixture();
    let a = bucket(&engine, 1);
    let old = engine.shards.handle(1).unwrap();
    drop(old.take_fast_read_recency());
    let replacement = engine.shards.with_state(1, |state| state.unwrap().clone());
    let peer = engine.clone();
    AFTER_FAST_READ_PLAN.with(|slot| *slot.borrow_mut() = Some(Box::new(move || {
        let retired = peer.shards.install(1, replacement);
        assert!(retired.is_some());
        peer.shards.with_state_mut(1, |state| {
            state.unwrap().bucket_recency.insert(a, 17);
        });
        drop(retired);
    })));
    // The real plan pins addresses before replacement and stamps after the hook.
    assert_value(&engine, 1, "resident", vec![1; 64]);
    let current = engine.shards.handle(1).unwrap();
    assert!(!old.same_incarnation(&current));
    current.with_fast_read_recency_for_test(|stamps| assert!(stamps.is_empty()));
    old.with_fast_read_recency_for_test(|stamps| assert!(stamps[&a] > 17));
    engine.fold_fast_read_recency_for_shard(1);
    current.with_read(|state| assert_eq!(state.unwrap().bucket_recency.get(&a), Some(&17)));
    // A new read must still heat the replacement; the old stamp was not a shortcut.
    assert_value(&engine, 1, "resident", vec![1; 64]);
    assert!(pending_stamps(&engine)[&(1, a)] > 17);
}

#[test]
fn pending_repeated_stamps_keep_the_maximum_before_and_after_fold() {
    let (_dir, engine) = fixture();
    let owner = engine.shards.handle(1).unwrap();
    let a = bucket(&engine, 1);
    drop(owner.take_fast_read_recency());
    engine.shards.with_state_mut(1, |state| { state.unwrap().bucket_recency.insert(a, 0); });
    for stamp in [30, 10, 30, 20] {
        owner.note_fast_read_recency(a, stamp);
    }
    owner.with_fast_read_recency_for_test(|stamps| {
        assert_eq!(stamps.len(), 1);
        assert_eq!(stamps[&a], 30);
    });
    engine.fold_fast_read_recency_for_shard(1);
    owner.note_fast_read_recency(a, 5);
    engine.fold_fast_read_recency_for_shard(1);
    owner.with_read(|state| assert_eq!(state.unwrap().bucket_recency.get(&a), Some(&30)));
}

#[test]
fn unload_reload_discards_old_owner_stamps_without_losing_new_reads() {
    let (_dir, engine) = fixture();
    let old = engine.shards.handle(1).unwrap();
    let a = bucket(&engine, 1);
    old.note_fast_read_recency(a, u64::MAX);
    engine.unload_shard(1);
    assert!(engine.shards.handle(1).is_none());
    engine.fold_fast_read_recency_for_shard(1);
    engine.load_shard(1);
    command(&engine, 1, Command::StringSet { key: "resident".into(), value: vec![4; 64] });
    let current = engine.shards.handle(1).unwrap();
    assert!(!old.same_incarnation(&current));
    current.with_fast_read_recency_for_test(|stamps| assert!(stamps.is_empty()));
    // An outstanding old plan remains isolated even after the new ID is loaded.
    old.note_fast_read_recency(a, u64::MAX);
    assert_value(&engine, 1, "resident", vec![4; 64]);
    let stamp = pending_stamps(&engine)[&(1, bucket(&engine, 1))];
    assert!(stamp > 0 && stamp < u64::MAX);
    engine.fold_fast_read_recency();
    let current_bucket = bucket(&engine, 1);
    current.with_read(|state| {
        assert_eq!(state.unwrap().bucket_recency.get(&current_bucket), Some(&stamp));
    });
    old.with_read(|state| assert!(state.is_none()));
}
