// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::collections::{BTreeMap, BTreeSet};

const SHARD: ShardId = 1;
const LIVE: &str = "owner-live";
const DELETED: &str = "owner-deleted";
const ELEMENT: &str = "owner-element";

fn open(root: &std::path::Path, end: u32) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD, table_name: "owner-rebuild".into(),
        shard_uri: "local://owner-rebuild/1".into(), start_routing_bucket: 0,
        end_routing_bucket: end, readonly: false, load_version: 1, local_node_id: Some(1),
    });
    assert!(loaded.status.ok, "{:?}", loaded.status);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let result = engine.execute(ExecuteRequest { shard_id: SHARD, command });
    assert!(result.status.ok, "{:?}", result.status);
    result.response
}

fn string(engine: &TemporalEngine, key: &str, value: &[u8]) {
    run(engine, Command::StringSet { key: key.into(), value: value.to_vec() });
}

fn expect_string(engine: &TemporalEngine, key: &str, value: Option<&[u8]>) {
    assert_eq!(run(engine, Command::StringGet { key: key.into() }),
        crate::types::CommandResponse::Bytes { value: value.map(|bytes| bytes.to_vec()) });
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Owner {
    dirty: bool,
    deleted: bool,
    generation: u64,
    wal: u64,
    index: u64,
    blocks: usize,
    block_flags: BTreeMap<(String, bool, bool), usize>,
    object_ids: BTreeSet<u64>,
    deleted_ids: BTreeSet<u64>,
    element_tombstones: BTreeSet<(String, String, String)>,
}

#[derive(Debug)]
struct State {
    owners: BTreeMap<u32, Owner>,
    logical: BTreeSet<String>,
    flags: BTreeSet<u32>,
    cached: BTreeSet<u32>,
    claims: BTreeMap<u32, (u64, u64)>,
}

fn state(engine: &TemporalEngine) -> State {
    engine.shards.with_state(SHARD, |shard| {
        let shard = shard.unwrap();
        let nodes = &shard.bucket_index.bucket_map;
        State {
            owners: nodes.iter().map(|(&id, node)| (id, Owner {
                dirty: node.dirty(), deleted: node.deleted(), generation: node.dirty_generation,
                wal: node.first_dirty_wal_sequence, index: node.first_dirty_index_log_sequence,
                blocks: node.block_index.len(), object_ids: node.object_index.iter().copied().collect(),
                block_flags: node.block_index.values().fold(BTreeMap::new(), |mut flags, block| {
                    *flags.entry((block.model_id().as_str().to_owned(), block.dirty(), block.deleted()))
                        .or_insert(0) += 1;
                    flags
                }),
                deleted_ids: node.deleted_object_index.iter().copied().collect(),
                element_tombstones: node.tombstone_elements.iter().map(|row| (
                    row.model_id.as_str().to_owned(), row.object_key.to_string(), row.element_name.to_string(),
                )).collect(),
            })).collect(),
            logical: shard.dirty_objects.iter().map(|key| key.to_string()).collect(),
            flags: nodes.iter().filter_map(|(&id, node)| node.dirty().then_some(id)).collect(),
            cached: nodes.dirty_buckets().clone(),
            claims: crate::engine::storage_reporting::dirty_bucket_claims(shard),
        }
    })
}

fn bucket(engine: &TemporalEngine, key: &str) -> u32 {
    let (start, end) = engine.shard_routing_range(SHARD);
    block_routing_bucket(key, start, end)
}

fn release_live(engine: &TemporalEngine) {
    release_string(engine, LIVE);
}

fn string_block_flags(engine: &TemporalEngine, key: &str) -> (bool, bool) {
    engine.shards.with_state(SHARD, |shard| {
        let shard = shard.unwrap();
        let refs = shard.bucket_index.object_block_refs("string", key).expect("real String ref");
        let refs = refs.iter().collect::<Vec<_>>();
        assert_eq!(refs.len(), 1);
        let block_ref = refs[0];
        let node = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket).unwrap();
        let block = node.block_index.get(&block_ref.block_ref_key).unwrap();
        assert_eq!(block.model_id().as_str(), "string");
        assert_eq!(block.address, shard.strings().get(key).unwrap().address(),
            "the reconstructed block must use the current model address");
        (block.dirty(), block.deleted())
    })
}

fn release_string(engine: &TemporalEngine, key: &str) {
    let id = bucket(engine, key);
    let flags = string_block_flags(engine, key);
    engine.shards.with_state_mut(SHARD, |shard| {
        let shard = shard.unwrap();
        let outcome = storage_bucket_internals::release_bucket_blocks_preserving_inline_pages(shard, &[id]);
        assert_eq!(outcome.released_buckets, vec![id], "a real inline release is required");
        assert!(outcome.released_blocks > 0);
        assert!(shard.bucket_index.bucket_map.get(&id).unwrap().block_index.is_empty());
        assert!(shard.bucket_index.released_buckets.contains(&id));
        assert!(shard.bucket_index.object_block_refs("string", key)
            .is_none_or(|refs| refs.iter().next().is_none()),
            "the moved String must actually lack its old lookup ref");
        assert_eq!(shard.strings().get(key).unwrap().record.retained_page_state(), flags,
            "release must capture the exact page bits, not owner-wide dirtiness");
    });
}

fn compact(engine: &TemporalEngine, expected_path: u8) {
    let old_live_address = engine.shards.with_state(SHARD, |shard| {
        shard.unwrap().strings().get(LIVE).unwrap().address()
    });
    LAST_ROUND_INDEX_UPDATE.with(|cell| cell.set(0));
    let report = engine.compact_shard_blocks(SHARD).expect("real compaction");
    assert!(report.rewritten_block_refs > 0, "the round must move actual blocks");
    assert!(report.relocated_bytes > 0);
    assert_eq!(last_round_index_update_for_test(), expected_path);
    let new_live_address = engine.shards.with_state(SHARD, |shard| {
        shard.unwrap().strings().get(LIVE).unwrap().address()
    });
    assert_ne!(new_live_address, old_live_address, "the String itself must actually move");
}

fn preserve(before: &State, after: &State) {
    assert!(!before.claims.is_empty());
    assert_eq!(after.logical, before.logical, "reconstruction does not retire debt");
    assert_eq!(after.flags, before.flags);
    assert_eq!(after.cached, after.flags);
    assert_eq!(after.claims, before.claims, "preserve both real log domains exactly");
    for (&id, old) in &before.owners {
        assert!(old.wal > 0 && old.index > 0, "real positive claims are mandatory: {old:?}");
        let new = after.owners.get(&id).expect("actual prior owner survives");
        assert_eq!(new.generation, old.generation);
        assert_eq!(new.dirty, old.dirty);
        assert_eq!(new.deleted, old.deleted);
        assert_eq!(new.deleted_ids, old.deleted_ids);
        assert_eq!(new.element_tombstones, old.element_tombstones);
        assert_eq!(new.block_flags, old.block_flags,
            "reconstructed block flags are independent of preserved owner flags");
    }
}

fn retention_is_claim_bounded(engine: &TemporalEngine, captured: &State) {
    let first_wal = captured.claims.values().map(|claims| claims.0).min().unwrap();
    let first_index = captured.claims.values().map(|claims| claims.1).min().unwrap();
    assert!(first_wal > 0 && first_index > 0);
    let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
    assert!(plan.missing_bucket_generations.is_empty(), "{:?}", plan.blocker_reasons);
    // No bucket dump exists in this fixture. Positive claims, not clean-node omission or
    // unknown-zero fallback, must establish both retain boundaries.
    assert_eq!(plan.retain_from_wal_sequence, first_wal);
    assert_eq!(plan.retain_from_index_log_sequence, first_index);
}

fn deletion_fallback(expire: bool) {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path(), u32::MAX);
    let ids = [LIVE, DELETED, ELEMENT].map(|key| bucket(&engine, key));
    assert_eq!(ids.into_iter().collect::<BTreeSet<_>>().len(), 3);
    string(&engine, LIVE, b"exact live payload before fallback");
    if expire {
        run(&engine, Command::StringSetEx {
            key: DELETED.into(), value: b"expires".to_vec(), ttl_ms: 1,
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        let swept = engine.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: SHARD, load_cold_buckets: true, ..ShardExpirySweepRequest::default()
        }).unwrap();
        assert_eq!(swept.expired_records_removed, 1);
    } else {
        string(&engine, DELETED, b"deleted");
        run(&engine, Command::CommonDelete { key: DELETED.into() });
    }
    run(&engine, Command::SetAdd { key: ELEMENT.into(), member: b"member".to_vec() });
    run(&engine, Command::SetRemove { key: ELEMENT.into(), member: b"member".to_vec() });
    let before = state(&engine);
    assert_eq!(before.logical, [LIVE, DELETED, ELEMENT].into_iter().map(str::to_owned).collect());
    let dead = &before.owners[&ids[1]];
    assert!(dead.dirty && dead.deleted && !dead.deleted_ids.is_empty());
    assert_eq!(dead.blocks, 0, "whole-object deletion is not an element tombstone block");
    assert_eq!(dead.object_ids, dead.deleted_ids);
    let element = &before.owners[&ids[2]];
    assert!(element.blocks > 0 && !element.element_tombstones.is_empty());
    retention_is_claim_bounded(&engine, &before);
    assert_eq!(string_block_flags(&engine, LIVE), (true, false));
    release_live(&engine);
    // No read may reload the missing ref before this real moved-block fallback.
    compact(&engine, 2);
    let after = state(&engine);
    preserve(&before, &after);
    assert_eq!(string_block_flags(&engine, LIVE), (true, false));
    assert_eq!(after.owners[&ids[1]].object_ids, dead.deleted_ids);
    assert_eq!(after.owners[&ids[1]].blocks, 0);
    retention_is_claim_bounded(&engine, &after);
    expect_string(&engine, LIVE, Some(b"exact live payload before fallback"));
    expect_string(&engine, DELETED, None);
    assert_eq!(run(&engine, Command::SetMembers { key: ELEMENT.into() }),
        crate::types::CommandResponse::Members { members: Vec::new() });

    let manifest = engine.create_bucket_dump_manifest(SHARD, ids.to_vec()).unwrap();
    string(&engine, LIVE, b"newer than the captured dump");
    engine.clear_dumped_bucket_dirty_state(SHARD, &manifest);
    let newer = state(&engine);
    assert_eq!(newer.logical, [LIVE.to_owned()].into_iter().collect());
    assert!(newer.owners[&ids[0]].generation > after.owners[&ids[0]].generation);
    assert_eq!(newer.claims.get(&ids[0]), after.claims.get(&ids[0]));
    for id in &ids[1..] {
        assert!(!newer.flags.contains(id));
        let owner = &newer.owners[id];
        assert_eq!((owner.wal, owner.index), (0, 0), "only covered generations retire claims");
    }
    expect_string(&engine, LIVE, Some(b"newer than the captured dump"));
    expect_string(&engine, DELETED, None);
    string(&engine, DELETED, b"re-added after deletion");
    let readded = state(&engine);
    assert!(!readded.owners[&ids[1]].deleted);
    assert!(readded.owners[&ids[1]].deleted_ids.is_empty());
    expect_string(&engine, DELETED, Some(b"re-added after deletion"));
}

#[test]
fn released_string_fallback_preserves_both_claims_and_both_deletion_authorities() {
    deletion_fallback(false);
}

#[test]
fn expiry_only_owner_survives_the_real_released_string_fallback() {
    deletion_fallback(true);
}

#[test]
fn released_live_wal_owner_with_deleted_ids_is_not_a_deleted_owner_and_readd_prunes() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path(), 0);
    string(&engine, LIVE, b"survives in a mixed owner");
    string(&engine, DELETED, b"removed from the same owner");
    run(&engine, Command::CommonDelete { key: DELETED.into() });
    let before = state(&engine);
    let owner = &before.owners[&0];
    assert!(!owner.deleted && !owner.deleted_ids.is_empty() && owner.blocks > 0);
    assert!(owner.object_ids.is_disjoint(&owner.deleted_ids));
    assert_eq!(string_block_flags(&engine, LIVE), (true, false));
    engine.shards.with_state(SHARD, |shard| {
        assert!(crate::wal_record::is_wal_resident(
            shard.unwrap().strings().get(LIVE).unwrap().address().block_slab_id(),
        ), "real WAL-resident String is the denominator");
    });
    release_live(&engine);
    engine.shards.with_state_mut(SHARD, |shard| {
        let shard = shard.unwrap();
        refresh_bucket_runtime_flags(shard);
        let node = shard.bucket_index.bucket_map.get(&0).unwrap();
        assert!(node.block_index.is_empty() && !node.deleted());
        assert!(!node.deleted_object_index.is_empty());
    });
    compact(&engine, 2);
    let after = state(&engine);
    preserve(&before, &after);
    assert_eq!(string_block_flags(&engine, LIVE), (true, false));
    assert_eq!(after.owners[&0].object_ids, owner.object_ids, "do not carry deleted IDs as old live IDs");
    expect_string(&engine, LIVE, Some(b"survives in a mixed owner"));
    expect_string(&engine, DELETED, None);
    string(&engine, DELETED, b"mixed owner re-add");
    assert!(state(&engine).owners[&0].deleted_ids.is_empty());
    expect_string(&engine, DELETED, Some(b"mixed owner re-add"));
}

#[test]
fn in_place_compaction_retains_the_same_owner_claim_contract() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path(), u32::MAX);
    string(&engine, LIVE, b"in-place exact payload");
    let before = state(&engine);
    compact(&engine, 1);
    let after = state(&engine);
    preserve(&before, &after);
    retention_is_claim_bounded(&engine, &after);
    expect_string(&engine, LIVE, Some(b"in-place exact payload"));
}

#[test]
fn fallback_keeps_published_pages_clean_and_ignores_nonreleased_retained_bits() {
    const CLEAN: &str = "owner-clean";
    for release_clean in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), u32::MAX);
        let clean_id = bucket(&engine, CLEAN);
        assert_ne!(clean_id, bucket(&engine, LIVE));
        string(&engine, CLEAN, b"published clean payload");
        assert_eq!(string_block_flags(&engine, CLEAN), (true, false));
        if !release_clean {
            release_string(&engine, CLEAN);
            // Use the real reload producer: a fast String read alone need not restore refs.
            engine.shards.with_state_mut(SHARD, |shard| {
                assert!(storage_bucket_internals::reload_released_bucket(shard.unwrap(), SHARD, clean_id));
            });
            expect_string(&engine, CLEAN, Some(b"published clean payload"));
            engine.shards.with_state(SHARD, |shard| {
                let shard = shard.unwrap();
                assert!(!shard.bucket_index.released_buckets.contains(&clean_id));
                assert_eq!(shard.strings().get(CLEAN).unwrap().record.retained_page_state(), (true, false));
            });
        }
        let manifest = engine.create_bucket_dump_manifest(SHARD, vec![clean_id]).unwrap();
        engine.clear_dumped_bucket_dirty_state(SHARD, &manifest);
        assert_eq!(string_block_flags(&engine, CLEAN), (false, false));
        let clean_before = state(&engine).owners.remove(&clean_id).unwrap();
        assert!(!clean_before.dirty);
        assert_eq!((clean_before.wal, clean_before.index), (0, 0));
        if release_clean {
            release_string(&engine, CLEAN);
        } else {
            engine.shards.with_state(SHARD, |shard| {
                assert_eq!(shard.unwrap().strings().get(CLEAN).unwrap().record.retained_page_state(), (true, false));
            });
        }
        string(&engine, LIVE, b"pending beside a clean page");
        let dirty_before = state(&engine).owners.remove(&bucket(&engine, LIVE)).unwrap();
        assert!(dirty_before.dirty && dirty_before.wal > 0 && dirty_before.index > 0);
        release_live(&engine);
        compact(&engine, 2);
        let after = state(&engine);
        assert_eq!(after.owners[&clean_id], clean_before);
        assert_eq!(after.owners[&bucket(&engine, LIVE)], dirty_before);
        assert!(!after.claims.contains_key(&clean_id));
        assert_eq!(string_block_flags(&engine, CLEAN), (false, false));
        assert_eq!(string_block_flags(&engine, LIVE), (true, false));
        expect_string(&engine, CLEAN, Some(b"published clean payload"));
        expect_string(&engine, LIVE, Some(b"pending beside a clean page"));
    }
}

const RESIDENT: &str = "owner-dirty-resident";

#[derive(Clone)]
struct MoveOrder {
    resident_first: bool,
    observed: bool,
}

thread_local! {
    static MOVE_ORDER: std::cell::RefCell<Option<MoveOrder>> = const { std::cell::RefCell::new(None) };
}

struct MoveOrderGuard(Option<MoveOrder>);

impl MoveOrderGuard {
    fn install(resident_first: bool) -> Self {
        Self(MOVE_ORDER.with(|slot| slot.replace(Some(MoveOrder { resident_first, observed: false }))))
    }

    fn assert_observed(&self) {
        MOVE_ORDER.with(|slot| assert!(slot.borrow().as_ref().unwrap().observed));
    }
}

impl Drop for MoveOrderGuard {
    fn drop(&mut self) {
        MOVE_ORDER.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(super) fn order_moves_for_test(
    moved: &[super::super::compaction::MovedBlock],
) -> Option<Vec<super::super::compaction::MovedBlock>> {
    MOVE_ORDER.with(|slot| {
        let control = slot.borrow();
        let control = control.as_ref()?;
        assert_eq!(moved.len(), 2, "both real String relocations must be present");
        assert_eq!(moved.iter().filter(|row| row.model_id == "string" && row.object_key == LIVE).count(), 1);
        assert_eq!(moved.iter().filter(|row| row.model_id == "string" && row.object_key == RESIDENT).count(), 1);
        let mut ordered = moved.to_vec();
        // Reorder actual rewrite output, never fabricate a moved address or page flag.
        ordered.sort_by_key(|row| if row.object_key == RESIDENT { !control.resident_first } else { control.resident_first });
        Some(ordered)
    })
}

pub(super) fn observe_partial_move_for_test(
    shard: &ShardState,
    moved: &[super::super::compaction::MovedBlock],
    in_place: bool,
) {
    MOVE_ORDER.with(|slot| {
        let mut control = slot.borrow_mut();
        let Some(control) = control.as_mut() else { return; };
        assert!(!in_place, "released A must cause the real in-place updater to decline");
        let movement = moved.iter().find(|row| row.object_key == RESIDENT).unwrap();
        let released = moved.iter().find(|row| row.object_key == LIVE).unwrap();
        assert_ne!(movement.old, movement.new);
        assert_ne!(released.old, released.new);
        let refs = shard.bucket_index.object_block_refs("string", RESIDENT).unwrap();
        assert_eq!(refs.iter().count(), 1);
        let block_ref = refs.iter().next().unwrap();
        let node = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket).unwrap();
        let block = node.block_index.get(&block_ref.block_ref_key).unwrap();
        assert_eq!((block.dirty(), block.deleted()), (true, false));
        assert_eq!(block.address, if control.resident_first { movement.new.clone() } else { movement.old.clone() });
        assert!(shard.bucket_index.object_block_refs("string", LIVE)
            .is_none_or(|refs| refs.iter().next().is_none()));
        let entry = storage_bucket_internals::collect_model_live_block_entries(shard)
            .into_iter().find(|entry| entry.object_key.as_ref() == RESIDENT).unwrap();
        assert_eq!(entry.address, movement.new);
        let lineages = storage_bucket_internals::StringRelocations::new(moved);
        let flags = storage_bucket_internals::indexed_string_page_flags;
        assert_eq!(flags(shard, &entry, lineages.get(RESIDENT)), Some((true, false)));
        assert_eq!(flags(shard, &entry, None), control.resident_first.then_some((true, false)));

        let mut wrong_address = entry.clone();
        wrong_address.address = movement.old.clone();
        assert_eq!(flags(shard, &wrong_address, Some(movement)), None,
            "an old address cannot masquerade as the current model address");
        let mut wrong_key = entry.clone();
        wrong_key.object_key = std::sync::Arc::from(LIVE);
        assert_eq!(flags(shard, &wrong_key, Some(movement)), None);
        if !control.resident_first {
            let mut wrong = movement.clone();
            wrong.model_id = "set".into();
            assert_eq!(flags(shard, &entry, Some(&wrong)), None);
            wrong = movement.clone();
            wrong.object_key = LIVE.into();
            assert_eq!(flags(shard, &entry, Some(&wrong)), None);
            wrong = movement.clone();
            wrong.element_name = Some("not-a-String-page".into());
            assert_eq!(flags(shard, &entry, Some(&wrong)), None);
            wrong = movement.clone();
            wrong.new = released.new.clone();
            assert_eq!(flags(shard, &entry, Some(&wrong)), None);
            wrong = movement.clone();
            wrong.old = movement.new.clone();
            assert_eq!(flags(shard, &entry, Some(&wrong)), None);
            let ambiguous = vec![movement.clone(), wrong];
            let lineages = storage_bucket_internals::StringRelocations::new(&ambiguous);
            assert!(lineages.get(RESIDENT).is_none());
            assert_eq!(flags(shard, &entry, lineages.get(RESIDENT)), None);
            let duplicate = vec![movement.clone(), movement.clone()];
            let lineages = storage_bucket_internals::StringRelocations::new(&duplicate);
            assert_eq!(flags(shard, &entry, lineages.get(RESIDENT)), Some((true, false)));
        }
        control.observed = true;
    });
}

#[test]
fn dirty_resident_string_beside_released_string_survives_both_partial_move_orders() {
    for resident_first in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), u32::MAX);
        assert_ne!(bucket(&engine, LIVE), bucket(&engine, RESIDENT));
        string(&engine, LIVE, b"released A exact payload");
        string(&engine, RESIDENT, b"dirty resident B exact payload");
        assert_eq!(string_block_flags(&engine, LIVE), (true, false));
        assert_eq!(string_block_flags(&engine, RESIDENT), (true, false));
        let before = state(&engine);
        retention_is_claim_bounded(&engine, &before);
        release_live(&engine);
        assert_eq!(string_block_flags(&engine, RESIDENT), (true, false));
        let resident_id = bucket(&engine, RESIDENT);
        engine.shards.with_state(SHARD, |shard| {
            assert!(!shard.unwrap().bucket_index.released_buckets.contains(&resident_id));
        });
        let old_resident_address = engine.shards.with_state(SHARD, |shard| {
            shard.unwrap().strings().get(RESIDENT).unwrap().address()
        });
        let order = MoveOrderGuard::install(resident_first);
        compact(&engine, 2);
        order.assert_observed();
        drop(order);
        let after = state(&engine);
        preserve(&before, &after);
        retention_is_claim_bounded(&engine, &after);
        assert_eq!(string_block_flags(&engine, LIVE), (true, false));
        assert_eq!(string_block_flags(&engine, RESIDENT), (true, false));
        engine.shards.with_state(SHARD, |shard| {
            assert_ne!(shard.unwrap().strings().get(RESIDENT).unwrap().address(), old_resident_address);
        });
        // A later real release must capture B's preserved bits, not perpetuate false-clean state.
        release_string(&engine, RESIDENT);
        engine.shards.with_state_mut(SHARD, |shard| {
            assert!(storage_bucket_internals::reload_released_bucket(shard.unwrap(), SHARD, resident_id));
        });
        assert_eq!(string_block_flags(&engine, RESIDENT), (true, false));
        expect_string(&engine, LIVE, Some(b"released A exact payload"));
        expect_string(&engine, RESIDENT, Some(b"dirty resident B exact payload"));
    }
}

#[test]
fn no_lineage_rebuild_preserves_only_exact_current_string_page_authority() {
    let root = tempfile::tempdir().unwrap();
    let engine = open(root.path(), u32::MAX);
    string(&engine, LIVE, b"old real address");
    let old_address = engine.shards.with_state(SHARD, |shard| shard.unwrap().strings().get(LIVE).unwrap().address());
    string(&engine, LIVE, b"new real address");
    let before = state(&engine);
    engine.shards.with_state_mut(SHARD, |shard| {
        let shard = shard.unwrap();
        let mut entry = storage_bucket_internals::collect_model_live_block_entries(shard).pop().unwrap();
        assert_ne!(entry.address, old_address);
        assert_eq!(storage_bucket_internals::indexed_string_page_flags(shard, &entry, None), Some((true, false)));
        entry.address = old_address;
        assert_eq!(storage_bucket_internals::indexed_string_page_flags(shard, &entry, None), None);
        storage_bucket_internals::rebuild_bucket_block_ownership(SHARD, shard, 0, u32::MAX);
        refresh_bucket_runtime_flags(shard);
    });
    preserve(&before, &state(&engine));
    assert_eq!(string_block_flags(&engine, LIVE), (true, false));
    expect_string(&engine, LIVE, Some(b"new real address"));
}
