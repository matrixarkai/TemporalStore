// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The sliced pass: what holds while the shard write guard is free between slices.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

fn open(dir: &std::path::Path) -> TemporalEngine {
    open_over(dir, u32::MAX)
}

fn open_over(dir: &std::path::Path, end_routing_bucket: u32) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    assert!(engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1, load_version: 0, local_node_id: None, shard_uri: String::new(),
        start_routing_bucket: 0, end_routing_bucket, readonly: false, table_name: String::new(),
    }).status.ok);
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
    // The keys in the middle of the sort order live on a slab of their own. A pass walks keys in
    // sorted order, so that slab runs out of live blocks at a boundary in the middle of the pass,
    // while the durable index still names it -- which is what the pin exists for.
    let middle = |i: usize| (200..300).contains(&i);
    for i in (0..keys).filter(|i| !middle(*i)) {
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
    let mut commands = Vec::new();
    for i in (0..keys).filter(|i| middle(*i)) {
        commands.push(Command::StringSet { key: format!("key-{i}"), value: value_of(i, 0) });
        expected.insert(format!("key-{i}"), value_of(i, 0));
    }
    if !commands.is_empty() {
        assert!(engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands }).status.ok);
        engine.block_store.roll_slab().unwrap();
    }
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

#[test]
fn selected_compaction_visits_one_owner_across_every_live_model() {
    use crate::engine::storage_bucket_internals::model_kind_registry_guards;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let fixture = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    let entries = collect_model_live_block_entries(&fixture);
    assert_eq!(entries.len(), 15);
    for entry in entries {
        let mut shard = fixture.clone();
        let keys = std::collections::HashSet::from([entry.object_key.clone()]);
        let mut stats = CompactionRewriteStats::for_round(99, 0, 0);
        engine.relocate_keyed_blocks(1, &mut shard, 0, u32::MAX, Some(&keys), &mut stats).unwrap();
        engine.relocate_unkeyed_blocks(1, &mut shard, 0, u32::MAX, Some(&keys), &mut stats).unwrap();
        assert_eq!(stats.decisions, 1, "selected {}:{}", entry.kind, entry.object_key);
        assert!(stats.moved.is_empty(), "zero budget must not move a block");
    }
}

#[test]
fn selected_series_compaction_does_not_visit_unrelated_owners() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut shard = ShardState::default();
    let at = ElementEntry::from_parts(3, 128, 64, Some(0), None);
    for index in 0..100_000 {
        shard.features_mut().insert(format!("feature-{index}").into(),
            BTreeMap::from([(1, at.clone())]).into());
        shard.context_nodes_mut().insert(format!("node-{index}").into(), at.clone());
    }
    let keys = std::collections::HashSet::from([Arc::from("feature-7")]);
    let mut stats = CompactionRewriteStats::for_round(99, 0, 0);
    engine.relocate_unkeyed_blocks(1, &mut shard, 0, u32::MAX, Some(&keys), &mut stats).unwrap();
    assert_eq!(stats.decisions, 1);
}

#[test]
fn scoped_compaction_identity_still_names_a_misfiled_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let commands = (0..16).map(|index| Command::StringSet {
        key: format!("key-{index}"), value: value_of(index, 0),
    }).collect();
    assert!(engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands }).status.ok);
    let mut shards = engine.shards_write_marked();
    let shard = shards.get_mut(&1).unwrap();
    let original = *shard.bucket_index.bucket_map.keys().next().unwrap();
    let mut wrong = original.wrapping_add(1);
    while shard.bucket_index.bucket_map.contains_key(&wrong) { wrong = wrong.wrapping_add(1); }
    let node = shard.bucket_index.bucket_map.remove(&original).unwrap();
    let blocks = node.block_index.len();
    shard.bucket_index.bucket_map.insert(wrong, node);
    shard.bucket_index.ensure_bucket_owners();
    let mut entries = Vec::new();
    storage_bucket_internals::visit_owned_bucket_live_block_entries(
        shard, &wrong, shard.bucket_index.bucket_map.get(&wrong).unwrap(),
        |entry| entries.push(entry),
    );
    assert!(blocks > 0);
    assert_eq!(entries.len(), blocks, "a scoped inversion must not hide damaged blocks");
    let report = validate_bucket_ownership_index_from_entries(1, shard, &entries, 0, u32::MAX);
    assert!(!report.mismatches.is_empty());
}

#[test]
fn a_selected_entity_key_must_keep_its_canonical_numeric_suffix() {
    use crate::engine::storage_bucket_internals::model_kind_registry_guards;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut shard = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    for key in ["ctx:entity:t:n:01", "ctx:entity:t:n:+1", "ctx:entity:t:n:other"] {
        let keys = std::collections::HashSet::from([Arc::from(key)]);
        let mut stats = CompactionRewriteStats::for_round(99, 0, 0);
        engine.relocate_unkeyed_blocks(1, &mut shard, 0, u32::MAX, Some(&keys), &mut stats).unwrap();
        assert_eq!(stats.decisions, 0, "{key} is not the owner key of entity 1");
    }
}

#[test]
fn selected_entity_compaction_moves_only_the_named_point_and_keeps_its_bytes() {
    use crate::engine::timestamped_series::TimestampedSeries;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut shard = ShardState::default();
    let mut points = BTreeMap::new();
    for entity in 1..=3 {
        let key = format!("collection:with:colons:{entity}");
        let bucket = block_routing_bucket(&key, 0, u32::MAX);
        let address = append_value(&engine.cache, &engine.block_store, 1, &value_of(entity, 0),
            Some(stable_block_object_id(1, "context_entity", &key)), None, Some(bucket), false).unwrap();
        points.insert(entity as u64, address);
    }
    let before = points.clone();
    shard.context_entities_mut().insert(Arc::from("collection:with:colons"), points.into());
    let target = engine.block_store.roll_slab().unwrap().new_block_slab_id;
    let mut stats = CompactionRewriteStats::for_round(target, u64::MAX, usize::MAX);
    let keys = std::collections::HashSet::from([Arc::from("collection:with:colons:2")]);
    engine.relocate_unkeyed_blocks(1, &mut shard, 0, u32::MAX, Some(&keys), &mut stats).unwrap();
    assert_eq!(stats.decisions, 1);
    assert_eq!(stats.moved.len(), 1);
    assert_eq!(stats.moved[0].object_key, "collection:with:colons:2");
    let series = shard.context_entities().get("collection:with:colons").unwrap();
    for entity in [1, 3] {
        assert_eq!(TimestampedSeries::get(series, entity), before.get(&entity));
    }
    let address = TimestampedSeries::get(series, 2).unwrap();
    assert_eq!(address.block_slab_id(), target);
    let key = "collection:with:colons:2";
    engine.cache.invalidate_shard(1).unwrap();
    assert_eq!(read_block_bytes(&engine.cache, &engine.block_store, 1, address,
        BlockIdentity::of(1, "context_entity", key, None),
        Some(block_routing_bucket(key, 0, u32::MAX))).unwrap(), value_of(2, 0));
}

fn feature_slice_case(count: usize, cursor: bool, routing_end: u32) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_over(dir.path(), routing_end);
    let commands = (0..count).map(|index| Command::FeatureAppend {
        key: format!("feature-{index}"),
        points: vec![FeaturePoint { timestamp_ms: 1, value: value_of(index, 0) }],
    }).collect();
    assert!(engine.batch_execute(BatchExecuteRequest { shard_id: 1, commands }).status.ok);
    let drain = engine.block_store.slab_ids().unwrap().into_iter().collect::<BTreeSet<_>>();
    engine.block_store.roll_slab().unwrap();
    if !cursor {
        let mut shards = engine.shards_write_marked();
        shards.get_mut(&1).unwrap().bucket_index.block_slab_live.forget_for_test();
    }
    let started = std::time::Instant::now();
    let (progress, identity_visits) = crate::engine::state::measure_block_identity_visits_for_test(|| {
        engine.compact_shard_blocks_slice(1, &drain, 16).unwrap()
    });
    assert_eq!(progress, super::SliceProgress::More);
    let first_slice_us = started.elapsed().as_micros();
    let remaining = {
        let shards = engine.shards.read().unwrap();
        collect_model_live_block_entries(shards.get(&1).unwrap()).iter()
            .filter(|entry| drain.contains(&entry.address.block_slab_id())).count()
    };
    assert!(remaining < count && remaining >= count - 16,
        "first slice moved {} of {count} owners, cursor={cursor}", count - remaining);
    if cursor && routing_end >= 1023 {
        assert!(identity_visits < count / 2,
            "first slice visited {identity_visits} identity owners out of {count}");
    }
    assert_index_agrees(&engine, "after the first feature slice");
    let tail_started = std::time::Instant::now();
    engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 16).unwrap();
    eprintln!("FEATURE_COMPACTION keys={count} cursor={cursor} routing_end={routing_end} first_slice_us={first_slice_us} first_slice_owners={} tail_and_publication_us={}",
        count - remaining, tail_started.elapsed().as_micros());
    assert_index_agrees(&engine, "after all feature slices");
    engine.flush_shard_index(1);
    drop(engine);
    let reopened = open_over(dir.path(), routing_end);
    reopened.cache.invalidate_shard(1).unwrap();
    let shards = reopened.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let entries = collect_model_live_block_entries(shard);
    assert_eq!(entries.len(), count);
    assert!(entries.iter().all(|entry| !drain.contains(&entry.address.block_slab_id())));
    for entry in entries {
        let index = entry.object_key.strip_prefix("feature-").unwrap().parse::<usize>().unwrap();
        let bytes = read_block_bytes(&reopened.cache, &reopened.block_store, 1, &entry.address,
            BlockIdentity::of(1, "feature", &entry.object_key, None),
            Some(entry.filed_routing_bucket)).unwrap();
        let points = super::super::packed_blocks::decode_feature_block(&bytes).unwrap();
        assert_eq!(points, vec![FeaturePoint { timestamp_ms: 1, value: value_of(index, 0) }]);
    }
}

#[test]
fn feature_only_compaction_moves_a_slice_instead_of_the_whole_shard() {
    for (cursor, end) in [(false, u32::MAX), (true, u32::MAX), (true, 1023), (true, 0)] {
        feature_slice_case(256, cursor, end);
    }
}

#[test]
#[ignore = "large feature compaction and reload measurement"]
fn feature_compaction_slices_at_scale() {
    feature_slice_case(100_000, true, 1023);
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
            if at == 1 {
                // A container element rewritten between slices: its fresh registration must
                // survive the finish.
                assert!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::HashSet {
                                key: "catalog".into(),
                                field: "f-5".into(),
                                value: vec![77u8; 64],
                            },
                        })
                        .status
                        .ok
                );
            }
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
    let field = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet { key: "catalog".into(), field: "f-5".into() },
    });
    assert!(
        matches!(field.response, CommandResponse::Bytes { value: Some(ref v) } if *v == vec![77u8; 64]),
        "a hash field rewritten between slices did not survive a restart"
    );
}

/// A ROUND NAMES THE BLOCKS THE LOG WAS HOLDING THAT IT TOOK OUT, each with the placement it had.
/// A string or series block is found by its object alone, a container element by its object AND its
/// element name, and a block with no registration is not named. Two elements of one object with one
/// registered must name exactly that one: looking an element up by its object alone would name both
/// or neither. Dropping the element name from the lookup turns this red.
#[test]
fn a_round_names_the_log_placements_of_the_blocks_it_moved() {
    use super::super::block_in_wal::wal_resident_key;
    use super::super::compaction::{CompactionRewriteStats, MovedBlock, RelocatedWalBlock};
    use super::super::hashing::stable_block_object_id;
    use super::super::state::WalResidentBlock;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let entry = crate::block_store::ElementEntry::default;
    let moved = |model: &str, key: &str, element: Option<&str>| MovedBlock {
        model_id: model.to_string(),
        object_key: key.to_string(),
        element_name: element.map(str::to_string),
        old: entry(),
        new: entry(),
    };
    let string_object = stable_block_object_id(1, "string", "held");
    let hash_object = stable_block_object_id(1, "hash", "catalog");
    {
        let mut shards = engine.shards_write_marked();
        let shard = shards.get_mut(&1).unwrap();
        shard.wal_resident_blocks.insert(
            wal_resident_key(string_object, None),
            WalResidentBlock { log_id: 3, sequence: 41 },
        );
        shard.wal_resident_blocks.insert(
            wal_resident_key(hash_object, Some("f-1")),
            WalResidentBlock { log_id: 3, sequence: 42 },
        );
    }
    let mut stats = CompactionRewriteStats::for_drain_round(1, u64::MAX, usize::MAX, BTreeSet::new());
    stats.moved = vec![
        moved("string", "held", None),
        moved("string", "never-in-the-log", None),
        moved("hash", "catalog", Some("f-1")),
        moved("hash", "catalog", Some("f-2")),
    ];
    {
        let shards = engine.shards_write_marked();
        stats.note_relocated_wal_blocks(1, shards.get(&1).unwrap());
    }
    assert_eq!(
        stats.relocated_wal_blocks,
        vec![
            RelocatedWalBlock { object_id: string_object, element_name: None, from_log_id: 3, from_sequence: 41 },
            RelocatedWalBlock {
                object_id: hash_object,
                element_name: Some("f-1".to_string()),
                from_log_id: 3,
                from_sequence: 42,
            },
        ],
        "a round must name exactly the moved blocks the log held, one per registration"
    );
}

/// A second pass over slabs nothing is left on does no work: no roll, no publish.
#[test]
fn a_pass_with_nothing_on_the_drain_slabs_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_, drain) = seed(&engine, 60);
    engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 32).unwrap();
    let slabs = engine.block_store.slab_ids().unwrap();
    let log_sequence = engine.index_log_store.stats(1).last_sequence;
    reset_live_block_scan_entries();
    engine.compact_shard_blocks_draining_sliced(1, drain, 32).unwrap();
    assert_eq!(engine.block_store.slab_ids().unwrap(), slabs, "a pass with nothing to move rolled a slab");
    // AN IDLE PASS WALKS NOTHING AND WRITES NOTHING: the per-slab tally says no block is left on the
    // drained slabs, so the gate answers without a walk, and no step ran, so no record was appended.
    assert_eq!(
        live_block_scan_entries(),
        0,
        "a pass with nothing on the drain slabs walked the shard to find that out"
    );
    assert_eq!(
        engine.index_log_store.stats(1).last_sequence,
        log_sequence,
        "a pass with nothing to move appended an index-log record"
    );
}

/// A PASS MAKES THE RELOCATION OF EVERY BLOCK IT MOVED DURABLE. Each slice's moves become relocation
/// records (old item, new item, element name, one of each per block), appended after the blocks they
/// name are synced, so recovery can repoint a block from the base image plus the records. Not
/// appending them (dropping `flush_relocation_records`, or its call at the finish) leaves the log with
/// none, and this turns red.
#[test]
fn a_pass_writes_a_durable_relocation_record_for_every_block_it_moved() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_, drain) = seed(&engine, 60);
    let expected: BTreeSet<_> = {
        let shards = engine.shards.read().unwrap();
        collect_live_block_entries(shards.get(&1).unwrap())
            .into_iter()
            .filter(|entry| !entry.deleted && drain.contains(&entry.address.block_slab_id()))
            .map(|entry| (entry.address.block_slab_id(), entry.address.offset(), entry.address.length()))
            .collect()
    };
    assert!(!expected.is_empty(), "the fixture must leave live blocks on the drain slabs");
    engine.compact_shard_blocks_draining_sliced(1, drain, 32).unwrap();
    let mut recorded = BTreeSet::new();
    let mut ordinary = 0usize;
    engine.index_log_store.for_each_delta_record_above_anchor(1, 0, 0, |record| {
        if record.relocated_from.is_empty() {
            ordinary += 1;
            return;
        }
        assert_eq!(record.relocated_from.len(), record.items.len(), "unpaired relocation items");
        assert_eq!(record.relocated_elements.len(), record.items.len(), "unpaired element names");
        for from in &record.relocated_from {
            let address = from.entry.as_ref().expect("a moved block must have its old address");
            assert!(recorded.insert((address.block_slab_id(), address.offset(), address.length())),
                "the pass recorded one source block twice");
        }
    }).expect("the durable relocation log must decode completely");
    assert!(ordinary > 0, "the guard must distinguish relocation records from ordinary deltas");
    assert_eq!(recorded, expected, "the pass must record exactly every block it moved");
}

/// A KEYED STEP NAMES THE BLOCKS OF ITS OWN BUCKETS, NOT THE SHARD'S. Naming a block's object needs an
/// inversion of the object tables; built with the whole-shard walk once per bucket a 200-key step
/// visited every object of every kind about 200 times, which measured 4.1 s a step at 100k keys and
/// 18 s at 300k. Built from the buckets' own recorded owners it visits only those buckets' objects. The
/// count of objects visited while naming blocks is the instrument: it must stay a small multiple of the
/// keys of the step, far below the shard's objects.
#[test]
fn a_keyed_step_names_the_blocks_of_its_buckets_and_not_the_shards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_expected, drain) = seed(&engine, 900);
    // The setup step, then one keyed step that records the bucket owners the later steps reuse.
    assert_eq!(engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(), super::SliceProgress::More);
    assert_eq!(engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(), super::SliceProgress::More);
    let (progress, visits) = crate::engine::state::measure_block_identity_visits_for_test(|| {
        engine.compact_shard_blocks_slice(1, &drain, 16).unwrap()
    });
    assert_eq!(progress, super::SliceProgress::More);
    eprintln!("COMPACTION_STEP_IDENTITY_VISITS visits={visits} keys_per_step=16 shard_objects>=900");
    assert!(
        visits <= 16 * 8,
        "a 16-key step visited {visits} objects while naming blocks: it inverted the shard"
    );
}

/// THE SETUP AND THE FINISH OF A PASS DO NOT WALK THE SHARD TO CONFIRM THE INDEX WHEN THE WRITE PATH
/// ALREADY HAS. Each owed a promote check (a walk of every live block against the bucket index); the
/// write path keeps the index in step once one scan has confirmed it and says so with
/// `promote_scan_done`, which the per-command scan already consults. A shard that has taken writes
/// is confirmed, so a whole pass runs no promote check at all. Dropping the test at either site turns
/// this red (two checks, one per site). A shard that has NOT been confirmed still walks, which the
/// second half asserts so the skip cannot become unconditional.
#[test]
fn a_pass_over_a_confirmed_index_runs_no_promote_scan() {
    use super::super::storage_bucket_internals::{
        promote_model_map_check_counts, reset_promote_model_map_check_counts,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (expected, drain) = seed(&engine, 300);
    // The seed's writes confirmed the index; a read-back command proves the latch holds.
    assert_every_key_reads(&engine, &expected, "before the pass");
    let confirmed = {
        let shards = engine.shards_write_marked();
        shards.get(&1).unwrap().promote_scan_done
    };
    assert!(confirmed, "the fixture's writes did not confirm the index, so this test would prove nothing");
    reset_promote_model_map_check_counts();
    engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 32).unwrap();
    assert_eq!(
        promote_model_map_check_counts().0,
        0,
        "a pass over a confirmed index still walked the shard to promote it"
    );
    assert_every_key_reads(&engine, &expected, "after the pass");

    // The control: an unconfirmed shard still pays the check, once at setup and once at the finish.
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_, drain) = seed(&engine, 300);
    {
        let mut shards = engine.shards_write_marked();
        shards.get_mut(&1).unwrap().promote_scan_done = false;
    }
    reset_promote_model_map_check_counts();
    engine.compact_shard_blocks_draining_sliced(1, drain, 32).unwrap();
    assert!(
        promote_model_map_check_counts().0 >= 2,
        "an unconfirmed index was not promoted at setup and at the finish"
    );
}

/// THE TALLY ONLY SKIPS WORK; IT NEVER DECIDES WHAT IS SAFE. The pin that keeps reclaim away from the
/// slabs the durable index names is the tally's slabs UNION every slab the block store holds, so a
/// tally that forgot a slab (a counting bug, a door that did not charge) cannot leave a slab out of the
/// pin. Taking the pin from the tally alone turns this red.
#[test]
fn the_pin_covers_a_slab_the_tally_forgot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (_expected, drain) = seed(&engine, 300);
    let forgotten = *drain.iter().next().unwrap();
    {
        let mut shards = engine.shards_write_marked();
        shards
            .get_mut(&1)
            .unwrap()
            .bucket_index
            .block_slab_live
            .drop_slab_for_test(forgotten);
    }
    // The slabs that exist BEFORE the pass: the slab the pass rolls for itself is new, and no durable
    // index names it, so it is not part of the pin.
    let before = engine.block_store.slab_ids().unwrap();
    // A drain slab other than the forgotten one still reads live, so the pass starts.
    assert_eq!(engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(), super::SliceProgress::More);
    let pinned = engine
        .unpublished_compaction_slabs
        .read()
        .unwrap()
        .get(&1)
        .cloned()
        .unwrap_or_default();
    assert!(
        pinned.contains(&forgotten),
        "slab {forgotten} holds blocks the tally forgot and was left out of the pin {pinned:?}"
    );
    for slab in before {
        assert!(pinned.contains(&slab), "slab {slab} the store held before the pass is not pinned: {pinned:?}");
    }
}

#[test]
fn a_keyed_slice_does_not_rebuild_every_buckets_object_index() {
    use super::super::storage_bucket_internals::{bucket_visit_sites, refresh_bucket_runtime_flags};

    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (expected, drain) = seed(&engine, 900);
    assert_eq!(
        engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(),
        super::SliceProgress::More,
    );
    bucket_visit_sites::reset();
    {
        let mut shards = engine.shards_write_marked();
        refresh_bucket_runtime_flags(shards.get_mut(&1).unwrap());
    }
    let complete_layout_visits = bucket_visit_sites::snapshot().0;
    assert!(complete_layout_visits >= expected.len() as u64);
    bucket_visit_sites::reset();
    assert_eq!(
        engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(),
        super::SliceProgress::More,
    );
    assert_eq!(bucket_visit_sites::snapshot().0, 0);
    assert!(bucket_visit_sites::flag_folds() < complete_layout_visits / 2);
    eprintln!(
        "COMPACTION_SLICE_INDEX complete_layout={complete_layout_visits} selected_layout={} selected_flags={}",
        bucket_visit_sites::snapshot().0,
        bucket_visit_sites::flag_folds(),
    );
    assert_every_key_reads(&engine, &expected, "after targeted refresh");
    assert_index_agrees(&engine, "after targeted refresh");
    engine.compact_shard_blocks_draining_sliced(1, drain, 16).unwrap();
    assert_index_agrees(&engine, "after publication");
    drop(engine);
    let reopened = open(dir.path());
    assert_every_key_reads(&reopened, &expected, "after restart");
    assert_index_agrees(&reopened, "after restart");
}

/// SETUP AND EACH SLICE READ THE BUCKETS THEY REACH, NOT THE SHARD. The gate and the pin come from
/// the per-slab tally and the slices walk the buckets after a cursor, so the number of live-block
/// entries the first two steps materialize is a small part of the shard, and the whole pass reads
/// each bucket about once. A setup that walks every block again makes the first step cost the
/// shard and turns this red.
#[test]
fn a_pass_reads_the_buckets_it_reaches_and_not_the_shard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (expected, drain) = seed(&engine, 900);
    // THE REGISTRATIONS ARE CLEARED FIRST, AND THAT IS A STATED LIMIT. Every block a write made is
    // registered against the log it was written through, and the finish of a pass retires those
    // registrations by walking every registered object once (`retained_objects_outside_slab`). That
    // walk is the one item this guard does not claim to bound; the pass over a shard whose
    // registrations are already retired is what it measures.
    super::super::block_in_wal::clear_shard(&engine.block_store, 1);
    let total = {
        let shards = engine.shards_write_marked();
        collect_live_block_entries(shards.get(&1).unwrap()).len() as u64
    };
    assert!(total >= 900, "the fixture holds {total} live blocks");

    // Both steps move only the owners selected by their bucket cursors.
    reset_live_block_scan_entries();
    assert_eq!(engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(), super::SliceProgress::More);
    let setup = live_block_scan_entries();
    assert_eq!(engine.compact_shard_blocks_slice(1, &drain, 16).unwrap(), super::SliceProgress::More);
    let two_steps = live_block_scan_entries();
    eprintln!("COMPACTION_PASS_SCAN total_blocks={total} setup={setup} two_steps={two_steps}");
    assert!(
        setup * 4 < total,
        "setup materialized {setup} live-block entries of {total}: it walked the shard"
    );
    assert!(
        two_steps * 4 < total,
        "two steps materialized {two_steps} live-block entries of {total}: a slice walked the shard"
    );

    // The rest of the pass reads each bucket about once, not the shard once per slice.
    let mut steps = 2u64;
    while engine.compact_shard_blocks_slice(1, &drain, 16).unwrap() == super::SliceProgress::More {
        steps += 1;
    }
    let whole_pass = live_block_scan_entries();
    eprintln!(
        "COMPACTION_PASS_SCAN_SITES whole_pass={whole_pass} steps={steps} sites={:?}",
        super::super::storage_bucket_internals::live_block_scan_sites_snapshot()
    );
    assert!(steps >= 10, "the pass took {steps} steps, too few to prove a per-step bound");
    assert!(
        whole_pass <= total + total / 2,
        "the pass materialized {whole_pass} live-block entries over {steps} steps for a shard of {total}"
    );
    assert_every_key_reads(&engine, &expected, "after a cursor pass");
    assert_index_agrees(&engine, "after a cursor pass");
    let still_on_drain = {
        let shards = engine.shards_write_marked();
        collect_live_block_entries(shards.get(&1).unwrap())
            .into_iter()
            .filter(|entry| drain.contains(&entry.address.block_slab_id()))
            .count()
    };
    assert_eq!(still_on_drain, 0, "blocks were left on the drained slabs");
}

/// Run by hand at scale: `SCALE_KEYS=300000 TS_COMPACTION_PHASE_TIMING=1 cargo test --release --lib
/// repro_cursor_pass_at_scale -- --ignored --nocapture`. Runs the SAME fixture twice in one process, once
/// through the whole-round function the stage used to call and once through the sliced pass it calls
/// now, and prints for each the pass time and the longest write wait of a concurrent writer; with the
/// timing switch it also prints the duration of the setup, every slice and the finish.
#[test]
#[ignore]
fn repro_cursor_pass_at_scale() {
    let keys: usize = std::env::var("SCALE_KEYS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    for sliced in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(open(dir.path()));
        let (_, drain) = seed(&engine, keys);
        let stall = super::super::writer_stall_probe::measure_writer_stall(&engine, 1, || {
            if sliced {
                engine.compact_shard_blocks_draining_sliced(1, drain.clone(), 200).unwrap();
            } else {
                engine.compact_shard_blocks_draining_background(1, drain.clone()).unwrap();
            }
        });
        eprintln!(
            "SCALE keys={keys} arm={} pass {} ms, longest write wait {} ms, writes {}",
            if sliced { "sliced" } else { "whole_round" },
            stall.round_us / 1000,
            stall.max_wait_us / 1000,
            stall.writes
        );
    }
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
    // A scheduler stall on a loaded machine lands on either arm at random, so the sliced arm is
    // taken as the best of three: a pass that really holds the guard for long cannot do well in any.
    let sliced = (0..3)
        .map(|_| measure(true))
        .min_by_key(|stall| stall.max_wait_us)
        .unwrap();
    assert!(whole.writes > 0 && sliced.writes > 0);
    assert!(
        sliced.max_wait_us * 2 < whole.max_wait_us,
        "a sliced pass made a writer wait {} us against {} us for a whole round",
        sliced.max_wait_us,
        whole.max_wait_us
    );
}
