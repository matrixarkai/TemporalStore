// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::ElementEntry;
use crate::engine::storage_bucket_internals::{
    collect_model_live_block_entries, live_block_scan_entries, model_kind_registry_guards,
    stored_model_kind, ModelKind,
};
use crate::engine::ShardState;
use std::collections::{BTreeMap, HashSet};

fn materialized_folds(state: &ShardState, keep_real: bool) -> HashSet<u64> {
    let mut keys = HashSet::new();
    for entry in collect_model_live_block_entries(state) {
        if keep_real || crate::wal_record::is_wal_resident(entry.address.block_slab_id()) {
            keys.insert((
                crate::engine::stable_block_object_id(1, entry.kind.as_str(), &entry.object_key),
                crate::engine::live_block_key(&entry.address),
            ));
        }
    }
    keys.into_iter().map(folded).collect()
}

fn fixture() -> ShardState {
    let mut state = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    for (key, slab) in [
        ("wal-fields", crate::wal_record::WAL_LOG_SLAB_ID),
        ("hot-fields", crate::engine::HOT_BLOCK_SLAB_ID),
    ] {
        let address = ElementEntry::from_parts(slab, 256, 64, Some(0), None);
        state.bucket_index.hashes.insert_elements_for_test(
            key,
            [("field".to_string(), address.clone())]
                .into_iter()
                .collect(),
        );
        state.features_mut().insert(
            key.into(),
            (0..64)
                .map(|i| (i, address.clone()))
                .collect::<BTreeMap<_, _>>()
                .into(),
        );
    }
    state
}

#[test]
fn borrowed_retention_matches_every_kind_and_both_wal_sentinels() {
    assert!(ModelKind::ALL.len() >= 15);
    for kind in ModelKind::ALL {
        assert_eq!(kind.as_str(), stored_model_kind(kind.as_str()).as_str());
    }
    let state = fixture();
    for keep_real in [false, true] {
        let before = live_block_scan_entries();
        let expected = materialized_folds(&state, keep_real);
        let materialized_visits = live_block_scan_entries() - before;
        let before = live_block_scan_entries();
        let current = retained_frame_folds(&state, 1, keep_real);
        let borrowed_visits = live_block_scan_entries() - before;
        assert_eq!(current, expected);
        assert!(!current.is_empty());
        assert_eq!(materialized_visits, borrowed_visits);
        assert!(borrowed_visits >= ModelKind::ALL.len() as u64);
        assert_eq!(
            current.len(),
            if keep_real {
                ModelKind::ALL.len() + 4
            } else {
                4
            }
        );
    }
}

fn engine(root: &std::path::Path) -> crate::engine::TemporalEngine {
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        4096,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

#[test]
fn retirement_proposals_keep_live_non_series_and_foreign_log_frames() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let foreign = LocalWriteAheadLogStore::new(dir.path().join("foreign-wal"));
    let state = fixture();
    let entries = collect_model_live_block_entries(&state);
    let mut registered = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        let key = (
            crate::engine::stable_block_object_id(1, entry.kind.as_str(), &entry.object_key),
            crate::engine::live_block_key(&entry.address),
        );
        let location = Arc::new((engine.wal_store.clone(), 1, i as u64 + 1, 64, None));
        install(&engine.block_store, 1, key, location.clone());
        registered.push((key, location));
    }
    for (object, store) in [(123, engine.wal_store.clone()), (456, foreign)] {
        let key = (object, (3, 1024, 64));
        let location = Arc::new((store, 1, 999, 64, None));
        install(&engine.block_store, 1, key, location.clone());
        registered.push((key, location));
    }
    for keep_real in [false, true] {
        let retained = materialized_folds(&state, keep_real);
        let mut expected: Vec<_> = registered
            .iter()
            .filter(|(key, location)| {
                !retained.contains(&folded(*key)) && location.0.same_log(&engine.wal_store)
            })
            .map(|(key, location)| (folded(*key), location.1, location.2))
            .collect();
        let mut actual: Vec<_> =
            retirements(&engine.block_store, 1, &engine.wal_store, &state, keep_real)
                .into_iter()
                .map(|row| (row.folded_key, row.log_id, row.sequence))
                .collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert!(!expected.is_empty());
        assert_eq!(actual, expected);
    }
}

#[test]
fn stale_retirement_cannot_remove_a_replacement_location() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let key = (123, (3, 1024, 64));
    let old = Arc::new((engine.wal_store.clone(), 1, 1, 64, None));
    install(&engine.block_store, 1, key, old);
    let proposals = retirements(
        &engine.block_store,
        1,
        &engine.wal_store,
        &ShardState::default(),
        false,
    );
    assert_eq!(proposals.len(), 1);
    let replacement = Arc::new((engine.wal_store.clone(), 1, 2, 64, None));
    install(&engine.block_store, 1, key, replacement.clone());
    retire(&engine.block_store, 1, &proposals[0]);
    assert!(Arc::ptr_eq(
        &lookup(&engine.block_store, 1, key.0, key.1)
            .unwrap()
            .unwrap(),
        &replacement
    ));
    let current = retirements(
        &engine.block_store,
        1,
        &engine.wal_store,
        &ShardState::default(),
        false,
    );
    assert_eq!(current.len(), 1);
    retire(&engine.block_store, 1, &current[0]);
    assert!(lookup(&engine.block_store, 1, key.0, key.1).is_none());
}

#[cfg(feature = "alloc-probe")]
#[test]
fn borrowed_retention_avoids_the_owned_entry_list_and_full_key_set() {
    let mut state = ShardState::default();
    let count = 8192;
    for i in 0..count {
        state.set_string_address(
            format!("key-{i}").into(),
            &ElementEntry::from_parts(3, i as u64 * 64, 64, Some(0), None),
        );
    }
    drop(materialized_folds(&state, true));
    drop(retained_frame_folds(&state, 1, true));
    let probe = crate::alloc_probe::Probe::start();
    let old = materialized_folds(&state, true);
    let old_counts = probe.stop();
    let probe = crate::alloc_probe::Probe::start();
    let current = retained_frame_folds(&state, 1, true);
    let new_counts = probe.stop();
    assert_eq!(current, old);
    assert_eq!(current.len(), count);
    println!("blocks={count} materialized_rust_alloc_bytes={} borrowed_rust_alloc_bytes={} materialized_calls={} borrowed_calls={}",
        old_counts.alloc_bytes, new_counts.alloc_bytes, old_counts.allocs, new_counts.allocs);
    assert!(
        old_counts.alloc_bytes
            >= new_counts.alloc_bytes
                + (count
                    * std::mem::size_of::<crate::engine::storage_bucket_internals::LiveBlockEntry>(
                    )) as u64
    );
}
