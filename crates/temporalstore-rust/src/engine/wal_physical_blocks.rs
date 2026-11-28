// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) type PhysicalKey = (u64, crate::engine::LiveBlockKey);
type Addresses = HashMap<crate::engine::LiveBlockKey, Registration, ahash::RandomState>;
type Objects = HashMap<u64, Addresses, ahash::RandomState>;
#[derive(Default)]
struct PhysicalShard {
    owner: usize,
    shard: ShardId,
    objects: Objects,
    len: usize,
    folded_counts: Option<HashMap<u64, usize, ahash::RandomState>>,
}

impl Drop for PhysicalShard {
    fn drop(&mut self) {
        for location in self.objects.values().flat_map(|pages| pages.values()) {
            untrack_location(self.owner, self.shard, location);
        }
    }
}
type Stores = HashMap<usize, HashMap<ShardId, PhysicalShard>>;

impl PhysicalShard {
    fn new(owner: usize, shard: ShardId) -> Self {
        Self { owner, shard, objects: HashMap::default(), len: 0, folded_counts: None }
    }
}

fn table() -> &'static Mutex<Stores> {
    static TABLE: OnceLock<Mutex<Stores>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn enable_fold_tracking(blocks: &crate::block_store::BlockStore, shard: ShardId) {
    // Keep lock order aligned with collision checks and hash-frame retirement.
    let mut logical = registry().lock().expect("logical WAL locator lock poisoned");
    let mut physical = table().lock().expect("physical WAL locator lock poisoned");
    logical.shard_mut(blocks.store_id(), shard).enable_fold_tracking();
    let state = physical.entry(blocks.store_id()).or_default().entry(shard).or_insert_with(|| {
        PhysicalShard::new(blocks.store_id(), shard)
    });
    if state.folded_counts.is_none() {
        let mut counts = HashMap::<_, _, ahash::RandomState>::default();
        for (object, pages) in &state.objects {
            for address in pages.keys() {
                *counts.entry(folded((*object, *address))).or_default() += 1;
            }
        }
        state.folded_counts = Some(counts);
    }
}

fn series(kind: &str) -> bool {
    matches!(
        kind,
        "feature"
            | "context_event"
            | "context_index"
            | "context_audit"
            | "context_child"
            | "context_summary"
            | "context_compression"
    )
}

pub(super) fn bindings(
    outcomes: &[crate::wal::WalOutcomeItem],
    blocks: &[StagedBlock],
    shared_hash_frames: bool,
) -> Vec<(PhysicalKey, usize)> {
    if shared_hash_frames && outcomes.len() == blocks.len() {
        let mut seen = std::collections::HashSet::<_, ahash::RandomState>::default();
        let mut paired = Vec::with_capacity(blocks.len());
        let valid = outcomes
            .iter()
            .zip(blocks)
            .enumerate()
            .all(|(index, (item, block))| {
                let Some(address) = item.resolved_address() else {
                    return false;
                };
                if item.kind != "hash"
                    || item.object_id != block.object_id
                    || (crate::wal_record::is_wal_resident(address.block_slab_id())
                        && address.length() != block.bytes.len() as u64)
                {
                    return false;
                }
                let key = (item.object_id, crate::engine::live_block_key(&address));
                if !seen.insert(key) {
                    return false;
                }
                paired.push((key, index));
                true
            });
        if valid {
            return paired;
        }
    }
    let objects: std::collections::HashSet<_> = outcomes
        .iter()
        .filter(|item| series(&item.kind) || (shared_hash_frames && item.kind == "hash"))
        .map(|item| item.object_id)
        .collect();
    if objects.is_empty() {
        return Vec::new();
    }
    outcome_block_bindings(outcomes, blocks)
        .into_iter()
        .filter(|((object, _), _)| objects.contains(object))
        .collect()
}

pub(super) fn folded(key: PhysicalKey) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in b"physical-frame-v1"
        .iter()
        .copied()
        .chain(key.0.to_le_bytes())
        .chain(key.1 .0.to_le_bytes())
        .chain(key.1 .1.to_le_bytes())
        .chain(key.1 .2.to_le_bytes())
    {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
pub(super) fn install(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    key: PhysicalKey,
    location: Registration,
) -> u64 {
    install_many(blocks, shard, std::iter::once((key, location)))[0]
}

pub(super) fn install_many(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    locations: impl IntoIterator<Item = (PhysicalKey, Registration)>,
) -> Vec<u64> {
    let mut locations = locations.into_iter().peekable();
    let mut folded_keys = Vec::with_capacity(locations.size_hint().0);
    if locations.peek().is_none() {
        return folded_keys;
    }
    // A carrying record publishes all its locations under the shard's write guard.
    // Hold the locator lock once rather than once per carried frame.
    let mut table = table().lock().expect("physical WAL locator lock poisoned");
    let state = table
        .entry(blocks.store_id())
        .or_default()
        .entry(shard)
        .or_insert_with(|| PhysicalShard::new(blocks.store_id(), shard));
    for (key, location) in locations {
        track_location(blocks.store_id(), shard, &location,
            state.objects.get(&key.0).and_then(|pages| pages.get(&key.1)));
        let folded_key = folded(key);
        if state
            .objects
            .entry(key.0)
            .or_default()
            .insert(key.1, location)
            .is_none()
        {
            state.len += 1;
            if let Some(counts) = state.folded_counts.as_mut() {
                *counts.entry(folded_key).or_default() += 1;
            }
        }
        folded_keys.push(folded_key);
    }
    folded_keys
}

pub(super) fn lookup(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    object: u64,
    address: crate::engine::LiveBlockKey,
) -> Option<Option<Registration>> {
    lookup_retaining(blocks, shard, object, address, |_| {})
}

pub(super) fn lookup_retaining(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    object: u64,
    address: crate::engine::LiveBlockKey,
    mut retain: impl FnMut(&Registration),
) -> Option<Option<Registration>> {
    let table = table().lock().ok()?;
    let pages = table
        .get(&blocks.store_id())?
        .get(&shard)?
        .objects
        .get(&object)?;
    let location = pages.get(&address).cloned();
    if let Some(location) = &location { retain(location); }
    Some(location)
}

pub(super) fn locations(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
) -> Vec<Registration> {
    let Ok(table) = table().lock() else {
        return Vec::new();
    };
    table
        .get(&blocks.store_id())
        .and_then(|shards| shards.get(&shard))
        .into_iter()
        .flat_map(|state| state.objects.values())
        .flat_map(|pages| pages.values())
        .cloned()
        .collect()
}

pub(super) fn count(blocks: &crate::block_store::BlockStore, shard: ShardId) -> usize {
    let Ok(table) = table().lock() else {
        return 0;
    };
    table
        .get(&blocks.store_id())
        .and_then(|shards| shards.get(&shard))
        .map_or(0, |state| state.len)
}

pub(super) fn folded_keys(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
) -> std::collections::HashSet<u64> {
    let Ok(table) = table().lock() else {
        return std::collections::HashSet::new();
    };
    let mut keys = std::collections::HashSet::new();
    if let Some(state) = table
        .get(&blocks.store_id())
        .and_then(|shards| shards.get(&shard))
    {
        keys.reserve(state.len);
        for (object, pages) in &state.objects {
            keys.extend(pages.keys().map(|address| folded((*object, *address))));
        }
    }
    keys
}

pub(super) fn recover(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    store: &LocalWriteAheadLogStore,
    log_id: u64,
    record: &crate::wal::WriteAheadLogRecord,
    wanted: &std::collections::BTreeSet<u64>,
) -> Vec<u64> {
    let candidates = bindings(&record.outcomes, &record.staged_blocks, true);
    let hash_objects: std::collections::HashSet<_> = record
        .outcomes
        .iter()
        .filter(|item| item.kind == "hash")
        .map(|item| item.object_id)
        .collect();
    let shared_hash_frames = candidates
        .iter()
        .any(|(key, _)| hash_objects.contains(&key.0) && wanted.contains(&folded(*key)));
    if shared_hash_frames { enable_fold_tracking(blocks, shard); }
    let bindings = if shared_hash_frames {
        candidates
    } else {
        bindings(&record.outcomes, &record.staged_blocks, false)
    };
    let packed = packed_objects(&record.outcomes);
    let mut pool = std::collections::HashSet::new();
    let physical_indices: std::collections::HashSet<_> =
        bindings.iter().map(|(_, index)| *index).collect();
    let expected = bindings.len()
        + record
            .staged_blocks
            .iter()
            .enumerate()
            .filter(|(index, _)| !physical_indices.contains(index))
            .map(|(_, block)| registration_names(block, &packed, &mut pool).len())
            .sum::<usize>();
    install_many(
        blocks,
        shard,
        bindings.into_iter().filter_map(|(key, index)| {
            wanted.contains(&folded(key)).then(|| {
                (
                    key,
                    Arc::new((
                        store.clone(),
                        log_id,
                        record.sequence,
                        expected,
                        Some(index),
                    )),
                )
            })
        }),
    )
}

#[track_caller]
fn retained_frame_folds(
    state: &crate::engine::ShardState,
    shard: ShardId,
    retain_live_real_frames: bool,
) -> std::collections::HashSet<u64> {
    use crate::engine::storage_bucket_internals::{visit_model_live_blocks, ModelWalkTally};

    let mut retained = std::collections::HashSet::new();
    visit_model_live_blocks(
        state,
        ModelWalkTally::WholeShardEntries,
        |_, _| true,
        |kind, key, _, address| {
            if retain_live_real_frames
                || crate::wal_record::is_wal_resident(address.block_slab_id())
            {
                retained.insert(folded((
                    crate::engine::stable_block_object_id(shard, kind.as_str(), key.as_str()),
                    crate::engine::live_block_key(address),
                )));
            }
        },
    );
    retained
}

/// Whether this shard has any physical registration. The same test `retirements` opens with: when
/// this is false it returns nothing, so a caller may skip the walk it would otherwise run first.
/// A poisoned lock reads as "yes", which keeps the walk.
pub(super) fn has_entries(blocks: &crate::block_store::BlockStore, shard: ShardId) -> bool {
    let Ok(table) = table().lock() else {
        return true;
    };
    table
        .get(&blocks.store_id())
        .and_then(|shards| shards.get(&shard))
        .is_some_and(|state| state.len != 0)
}

pub(super) fn retirements(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    store: &LocalWriteAheadLogStore,
    state: &crate::engine::ShardState,
    retain_live_real_frames: bool,
) -> Vec<RegistrationRetirement> {
    {
        let Ok(table) = table().lock() else {
            return Vec::new();
        };
        if table
            .get(&blocks.store_id())
            .and_then(|shards| shards.get(&shard))
            .is_none_or(|state| state.len == 0)
        {
            return Vec::new();
        }
    }
    let retained_folded = retained_frame_folds(state, shard, retain_live_real_frames);
    // The persisted locator map folds both key domains into u64. Keep a collision
    // rather than retiring the other domain's still-needed location.
    let logical_folded: std::collections::HashSet<_> = match registry().lock() {
        Ok(map) => map
            .entries(blocks.store_id(), shard)
            .map(|((object, name), _)| wal_resident_key(*object, name.as_deref()))
            .collect(),
        Err(_) => return Vec::new(),
    };
    let Ok(table) = table().lock() else {
        return Vec::new();
    };
    let mut retirements = Vec::new();
    if let Some(state) = table
        .get(&blocks.store_id())
        .and_then(|shards| shards.get(&shard))
    {
        for (object, pages) in &state.objects {
            for (address, location) in pages {
                let key = (*object, *address);
                let folded_key = folded(key);
                if !retained_folded.contains(&folded_key)
                    && !logical_folded.contains(&folded_key)
                    && location.0.same_log(store)
                {
                    retirements.push(RegistrationRetirement {
                        folded_key,
                        log_id: location.1,
                        sequence: location.2,
                        block: RetirementKey::Physical(key),
                        location: location.clone(),
                    });
                }
            }
        }
    }
    retirements
}

pub(super) fn retire_logical_hints(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    state: &mut crate::engine::ShardState,
    retired: &[(u64, Registration)],
) {
    if retired.is_empty() {
        return;
    }
    let Ok(logical) = registry().lock() else {
        return;
    };
    let Ok(table) = table().lock() else {
        return;
    };
    for (key, location) in retired {
        let logical_collision = logical
            .shard(blocks.store_id(), shard)
            .is_some_and(|entries| entries.has_fold(key));
        let physical_collision = table
            .get(&blocks.store_id())
            .and_then(|shards| shards.get(&shard))
            .is_some_and(|entries| entries.folded_counts.as_ref().is_some_and(|counts| counts.contains_key(key)));
        if !logical_collision
            && !physical_collision
            && state
                .wal_resident_blocks
                .get(key)
                .is_some_and(|held| held.log_id == location.1 && held.sequence == location.2)
        {
            state.wal_resident_blocks.remove(key);
        }
    }
}

pub(super) fn prune_hash_frames(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    store: &LocalWriteAheadLogStore,
    state: &mut crate::engine::ShardState,
    outcomes: &[crate::wal::WalOutcomeItem],
) -> std::collections::HashSet<u64> {
    let mut live = HashMap::<
        u64,
        std::collections::HashSet<crate::engine::LiveBlockKey, ahash::RandomState>,
        ahash::RandomState,
    >::default();
    for item in outcomes.iter().filter(|item| item.kind == "hash") {
        if live.contains_key(&item.object_id) {
            continue;
        }
        let mut addresses = std::collections::HashSet::default();
        let mut previous = None;
        for address in state
            .bucket_index
            .hashes
            .get(&item.object_key)
            .into_iter()
            .flat_map(|fields| fields.values())
        {
            let address = crate::engine::live_block_key(address);
            // Packed columns commonly share one frame; do not hash it once per column.
            if previous != Some(address) {
                addresses.insert(address);
                previous = Some(address);
            }
        }
        live.insert(item.object_id, addresses);
    }
    let mut retired = std::collections::HashSet::new();
    let Ok(logical) = registry().lock() else {
        return retired;
    };
    let Ok(mut table) = table().lock() else {
        return retired;
    };
    let Some(physical) = table
        .get_mut(&blocks.store_id())
        .and_then(|shards| shards.get_mut(&shard))
    else {
        return retired;
    };
    for (object, live) in live {
        let Some(pages) = physical.objects.get_mut(&object) else {
            continue;
        };
        let dead: Vec<_> = pages
            .iter()
            .filter(|(address, location)| !live.contains(address) && location.0.same_log(store))
            .map(|(address, location)| (*address, location.clone()))
            .collect();
        for (address, location) in dead {
            pages.remove(&address);
            untrack_location(blocks.store_id(), shard, &location);
            physical.len -= 1;
            let key = folded((object, address));
            let count = physical
                .folded_counts
                .as_mut().expect("hash frame folds enabled")
                .get_mut(&key)
                .expect("physical fold exists");
            *count -= 1;
            if *count == 0 {
                physical.folded_counts.as_mut().unwrap().remove(&key);
            }
            if !physical.folded_counts.as_ref().unwrap().contains_key(&key) {
                retired.insert(key);
                let logical_collision = logical
                    .shard(blocks.store_id(), shard)
                    .is_some_and(|entries| entries.has_fold(&key));
                if !logical_collision
                    && state.wal_resident_blocks.get(&key).is_some_and(|held| {
                        held.log_id == location.1 && held.sequence == location.2
                    })
                {
                    state.wal_resident_blocks.remove(&key);
                }
            }
        }
        if pages.is_empty() {
            physical.objects.remove(&object);
        }
    }
    if physical.objects.is_empty() {
        physical.objects.shrink_to_fit();
        if let Some(counts) = physical.folded_counts.as_mut() { counts.shrink_to_fit(); }
    }
    retired
}

pub(super) fn retire(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    retirement: &RegistrationRetirement,
) {
    let RetirementKey::Physical((object, address)) = &retirement.block else {
        return;
    };
    let Ok(mut table) = table().lock() else {
        return;
    };
    let Some(state) = table
        .get_mut(&blocks.store_id())
        .and_then(|shards| shards.get_mut(&shard))
    else {
        return;
    };
    let objects = &mut state.objects;
    let Some(pages) = objects.get_mut(object) else {
        return;
    };
    if pages
        .get(address)
        .is_some_and(|held| Arc::ptr_eq(held, &retirement.location))
    {
        let removed = pages.remove(address).expect("retired physical location exists");
        untrack_location(blocks.store_id(), shard, &removed);
        state.len -= 1;
        if let Some(counts) = state.folded_counts.as_mut() {
            let folded = folded((*object, *address));
            let count = counts.get_mut(&folded).expect("physical fold exists");
            *count -= 1;
            if *count == 0 { counts.remove(&folded); }
        }
    }
    if pages.is_empty() {
        objects.remove(object);
    }
    if objects.is_empty() && state.folded_counts.is_none() {
        if let Some(shards) = table.get_mut(&blocks.store_id()) {
            shards.remove(&shard);
        }
    } else if objects.is_empty() {
        objects.shrink_to_fit();
        state.folded_counts.as_mut().unwrap().shrink_to_fit();
    }
}

pub(super) fn clear_shard(owner: usize, shard: ShardId) {
    if let Ok(mut table) = table().lock() {
        if let Some(shards) = table.get_mut(&owner) {
            shards.remove(&shard);
            if shards.is_empty() {
                table.remove(&owner);
            }
        }
    }
}

pub(super) fn clear_store(owner: usize) {
    if let Ok(mut table) = table().lock() {
        table.remove(&owner);
    }
}

#[cfg(test)]
#[path = "wal_retention_work_tests.rs"]
mod retention_work_tests;

#[cfg(test)]
#[path = "wal_location_batch_tests.rs"]
mod location_batch_tests;

#[cfg(test)]
#[path = "wal_registration_summary_tests.rs"]
mod registration_summary_tests;

#[cfg(test)]
#[path = "wal_hash_frame_tests.rs"]
mod hash_frame_locator_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::types::{Command, CommandResponse, ExecuteRequest, FeaturePoint};
    use crate::{Config, SetConfigRequest};

    fn engine(root: &std::path::Path, index: &str) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            4096,
            root.join(format!("{index}-cache")),
            root.join("blocks"),
            root.join(index),
        )
    }

    fn append(engine: &TemporalEngine, at: u64, value: u64) {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppend {
                        key: "series".into(),
                        points: vec![FeaturePoint {
                            timestamp_ms: at,
                            value: value.to_le_bytes().to_vec()
                        }],
                    }
                })
                .status
                .ok
        );
    }

    fn check(engine: &TemporalEngine, rewritten: bool) {
        engine.cache().invalidate_shard(1).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "series".into(),
                start_ms: 0,
                end_ms: 128,
                count: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(
            response.response,
            CommandResponse::FeaturePoints {
                points: (0..128u64)
                    .map(|at| FeaturePoint {
                        timestamp_ms: at,
                        value: if rewritten && at == 0 { 999u64 } else { at }
                            .to_le_bytes()
                            .to_vec(),
                    })
                    .collect()
            }
        );
    }

    #[test]
    fn physical_wal_series_frames_survive_cold_reads_checkpoint_and_replay() {
        for checkpoint in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = engine(dir.path(), "indexes");
            source.load_shard(1);
            assert!(
                source
                    .set_config(SetConfigRequest {
                        shard_id: 1,
                        config: Config {
                            version: 2,
                            async_storage: true,
                            ..Config::default()
                        }
                    })
                    .ok
            );
            for at in 0..128 {
                append(&source, at, at);
            }
            check(&source, false);
            assert_eq!(source.registration_count_for_test(1), 128);
            assert_eq!(
                min_registered_sequence(&source.block_store, 1, &source.wal_store),
                Some(1)
            );
            append(&source, 0, 999);
            check(&source, true);
            if checkpoint {
                source.create_bucket_dump_manifest(1, []).unwrap();
                assert_eq!(
                    source.registration_count_for_test(1),
                    128,
                    "a completed dump must retire the overwritten frame, not its replacement"
                );
                source.flush_shard_index(1);
            }
            source.wal_store.flush(1).unwrap();
            drop(source);
            let mut restored = engine(
                dir.path(),
                if checkpoint {
                    "indexes"
                } else {
                    "fresh-indexes"
                },
            );
            if !checkpoint {
                restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
            }
            restored.load_shard(1);
            check(&restored, true);
        }
    }

    #[test]
    fn physical_wal_locators_leave_with_the_final_block_store_owner() {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "indexes");
        source.load_shard(1);
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    }
                })
                .ok
        );
        append(&source, 1, 1);
        let blocks = source.block_store();
        let owner = blocks.store_id();
        drop(source);
        assert!(table().lock().unwrap().contains_key(&owner));
        drop(blocks);
        assert!(!table().lock().unwrap().contains_key(&owner));
    }

    #[test]
    fn physical_wal_series_batch_frames_keep_distinct_positions() {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "indexes");
        source.load_shard(1);
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    }
                })
                .ok
        );
        assert!(
            source
                .batch_execute(crate::types::BatchExecuteRequest {
                    shard_id: 1,
                    commands: (0..128u64)
                        .map(|at| Command::FeatureAppend {
                            key: "series".into(),
                            points: vec![FeaturePoint {
                                timestamp_ms: at,
                                value: at.to_le_bytes().to_vec()
                            }]
                        })
                        .collect(),
                })
                .status
                .ok
        );
        assert_eq!(source.registration_count_for_test(1), 128);
        check(&source, false);
        source.wal_store.flush(1).unwrap();
        drop(source);
        let mut restored = engine(dir.path(), "fresh-indexes");
        restored.wal_store = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
        restored.load_shard(1);
        check(&restored, false);
    }

    #[test]
    fn physical_wal_dump_preserves_cross_domain_folded_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "indexes");
        source.load_shard(1);
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    }
                })
                .ok
        );
        append(&source, 1, 1);
        let key = {
            let table = table().lock().unwrap();
            let objects = &table[&source.block_store.store_id()][&1].objects;
            let (object, pages) = objects.iter().next().unwrap();
            (*object, *pages.keys().next().unwrap())
        };
        let location = lookup(&source.block_store, 1, key.0, key.1)
            .unwrap()
            .unwrap();
        let folded_key = folded(key);
        register_at(
            &source.block_store,
            1,
            folded_key,
            None,
            location.1,
            location.2,
            &source.wal_store,
        );
        assert!(retirements(
            &source.block_store,
            1,
            &source.wal_store,
            &crate::engine::ShardState::default(),
            false
        )
        .is_empty());
        source.create_bucket_dump_manifest(1, []).unwrap();
        assert!(source.shards.with_state(1, |state| {
            state.unwrap().wal_resident_blocks.contains_key(&folded_key)
        }));
        source.flush_shard_index(1);
        source.wal_store.flush(1).unwrap();
        drop(source);
        let restored = engine(dir.path(), "indexes");
        restored.load_shard(1);
        restored.cache().invalidate_shard(1).unwrap();
        let read = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "series".into(),
                start_ms: 0,
                end_ms: 2,
                count: None,
            },
        });
        assert!(read.status.ok);
        assert_eq!(
            read.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: 1u64.to_le_bytes().to_vec(),
                }]
            }
        );
    }

    #[test]
    fn physical_wal_series_mixed_placement_retires_only_durable_frames() {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "indexes");
        source.load_shard(1);
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    }
                })
                .ok
        );
        for at in 0..64 {
            append(&source, at, at);
        }
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 3,
                        async_storage: false,
                        ..Config::default()
                    }
                })
                .ok
        );
        for at in 64..128 {
            append(&source, at, at);
        }
        assert_eq!(source.registration_count_for_test(1), 128);
        check(&source, false);
        source.create_bucket_dump_manifest(1, []).unwrap();
        source.flush_shard_index(1);
        assert_eq!(
            source.registration_count_for_test(1),
            64,
            "only log-only frames must keep pinning records after the durability barrier"
        );
        drop(source);
        let restored = engine(dir.path(), "indexes");
        restored.load_shard(1);
        check(&restored, false);
    }

    #[test]
    #[ignore = "large frame-locator recovery measurement"]
    fn physical_wal_series_recover_a_hundred_thousand_points() {
        const KEYS: usize = 10_000;
        const POINTS: u64 = 10;
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "indexes");
        source.load_shard(1);
        assert!(
            source
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    }
                })
                .ok
        );
        let started = std::time::Instant::now();
        for key in 0..KEYS {
            for at in 0..POINTS {
                let mut value = (key as u64).to_le_bytes().to_vec();
                value.extend_from_slice(&at.to_le_bytes());
                assert!(
                    source
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::FeatureAppend {
                                key: format!("series-{key:05}"),
                                points: vec![FeaturePoint {
                                    timestamp_ms: at,
                                    value
                                }],
                            }
                        })
                        .status
                        .ok
                );
            }
        }
        assert_eq!(
            source.registration_count_for_test(1),
            KEYS * POINTS as usize
        );
        assert_eq!(
            min_registered_sequence(&source.block_store, 1, &source.wal_store),
            Some(1)
        );
        source.create_bucket_dump_manifest(1, []).unwrap();
        source.flush_shard_index(1);
        source.wal_store.flush(1).unwrap();
        drop(source);
        let restored = engine(dir.path(), "indexes");
        restored.load_shard(1);
        restored.cache().invalidate_shard(1).unwrap();
        for key in 0..KEYS {
            let response = restored.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: format!("series-{key:05}"),
                    start_ms: 0,
                    end_ms: POINTS,
                    count: None,
                },
            });
            assert!(response.status.ok, "{response:?}");
            assert_eq!(
                response.response,
                CommandResponse::FeaturePoints {
                    points: (0..POINTS)
                        .map(|at| {
                            let mut value = (key as u64).to_le_bytes().to_vec();
                            value.extend_from_slice(&at.to_le_bytes());
                            FeaturePoint {
                                timestamp_ms: at,
                                value,
                            }
                        })
                        .collect()
                }
            );
        }
        println!(
            "PHYSICAL_WAL_RECOVERY keys={KEYS} points={} elapsed_ms={}",
            KEYS * POINTS as usize,
            started.elapsed().as_millis()
        );
    }
}
