// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The sliced pass: what holds while the shard write guard is free between slices.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

fn open(dir: &std::path::Path) -> TemporalEngine {
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
                config: Config { version: 2, async_storage: false, ..Config::default() },
            })
            .ok
    );
    engine
}

fn value_of(i: usize, generation: u8) -> Vec<u8> {
    vec![(i as u8).wrapping_add(generation); 96]
}

/// Strings, a hash and a set, so the pass moves a keyed model, a folded container and a flat one.
/// Returns the slabs the pass drains: every slab but the newest, which holds the rewrites made after
/// the roll.
fn seed(engine: &TemporalEngine, keys: usize) -> (BTreeMap<String, Vec<u8>>, BTreeSet<u64>) {
    let mut expected = BTreeMap::new();
    let mut commands = Vec::new();
    for i in 0..keys {
        commands.push(Command::StringSet { key: format!("key-{i}"), value: value_of(i, 0) });
        expected.insert(format!("key-{i}"), value_of(i, 0));
    }
    for i in 0..12 {
        commands.push(Command::HashSet {
            key: "catalog".into(),
            field: format!("f-{i}"),
            value: vec![i as u8; 64],
        });
        commands.push(Command::SetAdd { key: "order".into(), member: format!("m-{i}").into_bytes() });
    }
    assert!(engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands }).status.ok);
    engine.block_store.roll_slab().unwrap();
    for i in 0..keys / 20 {
        write(engine, i, 1, &mut expected);
    }
    let newest = *engine.block_store.slab_ids().unwrap().iter().max().unwrap();
    let drain = engine
        .block_store
        .slab_ids()
        .unwrap()
        .into_iter()
        .filter(|slab| *slab != newest)
        .collect();
    (expected, drain)
}

fn write(engine: &TemporalEngine, i: usize, generation: u8, expected: &mut BTreeMap<String, Vec<u8>>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: format!("key-{i}"), value: value_of(i, generation) },
    });
    assert!(response.status.ok);
    expected.insert(format!("key-{i}"), value_of(i, generation));
}

fn read(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.to_string() },
    });
    assert!(response.status.ok);
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response {other:?}"),
    }
}

fn assert_every_key_reads(engine: &TemporalEngine, expected: &BTreeMap<String, Vec<u8>>, when: &str) {
    for (key, value) in expected {
        assert_eq!(read(engine, key).as_ref(), Some(value), "{when}: {key} did not read back");
    }
    let field = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet { key: "catalog".into(), field: "f-3".into() },
    });
    assert!(
        matches!(field.response, CommandResponse::Bytes { value: Some(ref value) } if *value == vec![3u8; 64]),
        "{when}: a hash field did not read back"
    );
}

/// What the invariant says holds whenever the guard is free: the bucket index names the blocks the
/// model maps hold, and the per-slab live tally agrees with a walk of the index.
fn assert_index_agrees(engine: &TemporalEngine, when: &str) {
    let mut shards = engine.shards_write_marked();
    let shard = shards.get_mut(&1).unwrap();
    let name = |entries: Vec<LiveBlockEntry>| -> BTreeSet<(String, u64, u64, u64)> {
        entries
            .into_iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| {
                (
                    entry.object_key.to_string(),
                    entry.address.block_slab_id(),
                    entry.address.offset(),
                    entry.address.length(),
                )
            })
            .collect()
    };
    let held = name(collect_live_block_entries(shard));
    let derived = name(collect_model_live_block_entries(shard));
    assert_eq!(
        held.difference(&derived).count() + derived.difference(&held).count(),
        0,
        "{when}: the bucket index and the model maps name different blocks: {:?} / {:?}",
        held.difference(&derived).take(3).collect::<Vec<_>>(),
        derived.difference(&held).take(3).collect::<Vec<_>>()
    );
    let drift = reconcile_block_slab_live(shard);
    assert_eq!(drift.drifted_slabs, 0, "{when}: the live tally disagrees with the index: {drift:?}");
}

/// A pass in slices holds the invariant at EVERY boundary, with writes, a delete and a whole-round
/// attempt landing between slices, and what it publishes survives a restart.
#[test]
fn a_sliced_pass_keeps_every_key_readable_and_the_index_consistent_between_slices() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (expected, drain) = seed(&engine, 400);
    let expected = Arc::new(Mutex::new(expected));
    let pre_round = engine.live_block_slab_ids(1);
    let boundaries = Arc::new(Mutex::new(0usize));
    {
        let expected = Arc::clone(&expected);
        let boundaries = Arc::clone(&boundaries);
        let pre_round = pre_round.clone();
        super::compaction_slices::set_slice_boundary_for_test(Some(Box::new(move |engine| {
            let at = {
                let mut count = boundaries.lock().unwrap();
                *count += 1;
                *count
            };
            let when = format!("slice boundary {at}");
            let mut expected = expected.lock().unwrap();
            assert_every_key_reads(engine, &expected, &when);
            assert_index_agrees(engine, &when);
            // The slabs the durable index still names must stay out of reclaim's reach.
            let live = engine.live_block_slab_ids(1);
            let all_shards = engine.live_block_slab_ids_all_shards();
            for slab in &pre_round {
                assert!(live.contains(slab), "{when}: slab {slab} was released");
                assert!(all_shards.contains(slab), "{when}: slab {slab} was released for reclaim");
            }
            // Writes and a delete between slices: some of the keys the pass has not reached, some it
            // has.
            write(engine, at * 7 % 400, 2, &mut expected);
            write(engine, 399 - at, 3, &mut expected);
            if at == 2 {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringDelete { key: "key-250".into() },
                });
                assert!(response.status.ok);
                expected.remove("key-250");
                assert!(read(engine, "key-250").is_none());
                // A whole round must not start under a pass in flight.
                let refused = engine.compact_shard_blocks(1).expect_err("a pass owns the shard");
                assert_eq!(refused.code, "compaction_pass_in_progress");
            }
        })));
    }
    engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 32).unwrap();
    super::compaction_slices::set_slice_boundary_for_test(None);
    let slices = *boundaries.lock().unwrap();
    assert!(slices >= 8, "the fixture produced {slices} boundaries, so the guard was released too rarely to prove anything");

    let expected = expected.lock().unwrap().clone();
    assert_every_key_reads(&engine, &expected, "after the pass");
    assert_index_agrees(&engine, "after the pass");
    // The pass moved what it was asked to: no live block remains on a drain slab.
    let still_on_drain = {
        let shards = engine.shards_write_marked();
        collect_live_block_entries(shards.get(&1).unwrap())
            .into_iter()
            .filter(|entry| drain.contains(&entry.address.block_slab_id()))
            .count()
    };
    assert_eq!(still_on_drain, 0, "blocks were left on the drained slabs");
    // And the published index is complete: a restart serves the same values.
    drop(engine);
    let restarted = open(dir.path());
    assert_every_key_reads(&restarted, &expected, "after a restart");
}

/// A second pass over slabs nothing is left on does no work: no roll, no publish.
#[test]
fn a_pass_with_nothing_on_the_drain_slabs_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_, drain) = seed(&engine, 60);
    engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 32).unwrap();
    let slabs = engine.block_store.slab_ids().unwrap();
    engine.compact_shard_blocks_draining_sliced(1, drain, 32).unwrap();
    assert_eq!(engine.block_store.slab_ids().unwrap(), slabs, "a pass with nothing to move rolled a slab");
}

/// The point of slicing: a writer waits for a slice, not for the pass.
#[test]
fn a_writer_waits_less_for_a_sliced_pass_than_for_a_whole_round() {
    let measure = |sliced: bool| {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(open(dir.path()));
        let (_, drain) = seed(&engine, 1500);
        let stall = super::super::writer_stall_probe::measure_writer_stall(&engine, 1, || {
            if sliced {
                engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 32).unwrap();
            } else {
                engine.compact_shard_blocks_draining_background(1, drain.clone()).unwrap();
            }
        });
        eprintln!("STALL sliced={sliced} {stall:?}");
        stall
    };
    let whole = measure(false);
    let sliced = measure(true);
    assert!(whole.writes > 0 && sliced.writes > 0);
    assert!(
        sliced.max_wait_us * 2 < whole.max_wait_us,
        "a sliced pass made a writer wait {} us against {} us for a whole round",
        sliced.max_wait_us,
        whole.max_wait_us
    );
}
