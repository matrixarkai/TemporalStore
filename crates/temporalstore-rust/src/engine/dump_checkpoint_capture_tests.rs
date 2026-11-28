// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_at(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    engine
}

fn execute(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
}

fn seed_catalog_wal_models(engine: &TemporalEngine) -> Vec<(Command, CommandResponse)> {
    execute(engine, Command::StringSet { key: "catalog-string".into(), value: b"scalar".to_vec() });
    execute(engine, Command::HashSet {
        key: "catalog-hash".into(), field: "field".into(), value: b"member".to_vec(),
    });
    let point = crate::types::FeaturePoint { timestamp_ms: 7, value: b"point".to_vec() };
    execute(engine, Command::FeatureAppend { key: "catalog-feature".into(), points: vec![point.clone()] });
    let queries = vec![
        (Command::StringGet { key: "catalog-string".into() },
         CommandResponse::Bytes { value: Some(b"scalar".to_vec()) }),
        (Command::HashGet { key: "catalog-hash".into(), field: "field".into() },
         CommandResponse::Bytes { value: Some(b"member".to_vec()) }),
        (Command::FeatureQuery { key: "catalog-feature".into(), start_ms: 0, end_ms: 100, count: None },
         CommandResponse::FeaturePoints { points: vec![point] }),
    ];
    check_catalog_wal_models(engine, &queries);
    queries
}

fn check_catalog_wal_models(engine: &TemporalEngine, queries: &[(Command, CommandResponse)]) {
    for (command, expected) in queries {
        let response = engine.execute(ExecuteRequest { shard_id: 1, command: command.clone() });
        assert!(response.status.ok, "{command:?}: {:?}", response.status);
        assert_eq!(&response.response, expected, "{command:?}");
    }
}

#[test]
fn published_capture_reuses_exact_owner_digests_and_retention_floor() {
    for selected_key in [None, Some("catalog-string"), Some("catalog-hash"), Some("catalog-feature")] {
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        let queries = seed_catalog_wal_models(&engine);
        for index in 0..128 {
            execute(&engine, Command::StringSet {
                key: format!("unrelated-{index:04}"), value: vec![index as u8; 48],
            });
        }
        let selected = selected_key.map(|key| vec![engine.routing_bucket_for_key(1, key)])
            .unwrap_or_default();
        let before = bucket_dump_io::bucket_dump_manifest_io_counts();
        let manifest = engine.create_bucket_dump_manifest(1, selected).unwrap();
        let after_write = bucket_dump_io::bucket_dump_manifest_io_counts();
        assert_eq!(after_write.owned_copies - before.owned_copies, 0);
        assert_eq!(after_write.owned_copy_bytes - before.owned_copy_bytes, 0);
        let decoded = decode_index_bytes(&manifest.index_bytes).unwrap();
        let mut expected = crate::engine::storage_reporting::bucket_generation_digests_by_bucket(1, &decoded);
        assert!(expected.len() > 100, "unrelated owners must be present in the image");
        expected.retain(|bucket, _| manifest.bucket_ids.contains(bucket));
        if selected_key.is_some() { assert_eq!(expected.len(), 1); }
        let floor = super::super::storage_lifecycle_methods::wal_floor_of_index_image(&decoded);
        assert!(floor.is_some_and(|sequence| sequence > 0));
        let decode_count = CHECKPOINT_DECODES.with(|count| count.get());
        let listed = bucket_dump_io::list_bucket_dump_manifests_shared_at(&engine.index_dir, 1).unwrap();
        assert_eq!(listed.len(), 1);
        let digests = bucket_dump_io::manifest_digests_memoised(1, &listed[0], || {
            panic!("published owner facts must not require another image decode")
        }).unwrap();
        assert_eq!(&*digests, &expected);
        assert_eq!(bucket_dump_io::manifest_wal_floor_memoised(&listed[0], || {
            panic!("published retention floor must not require another image decode")
        }), Ok(floor));
        assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()), decode_count);
        let after_read = bucket_dump_io::bucket_dump_manifest_io_counts();
        assert_eq!(after_read.file_reads - after_write.file_reads, 1);
        assert_eq!(after_read.checksum_serializes - after_write.checksum_serializes, 1);
        let warm_plan = engine.storage_wal_reclaim_plan(1, [], []);
        assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()), decode_count,
            "the production reclaim plan must use the published owner facts");
        let off = bucket_dump_io::ManifestParseMemoOff::new();
        let cold_plan = engine.storage_wal_reclaim_plan(1, [], []);
        assert!(CHECKPOINT_DECODES.with(|count| count.get()) > decode_count,
            "the cold control must actually decode the durable image");
        drop(off);
        assert_eq!(warm_plan, cold_plan, "capture reuse must not change a reclaim decision");
        check_catalog_wal_models(&engine, &queries);
    }
}

#[test]
fn a_replacement_cannot_inherit_a_published_captures_file_identity() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    seed_catalog_wal_models(&engine);
    let manifest = engine.create_bucket_dump_manifest(1, vec![]).unwrap();
    let listed = bucket_dump_io::list_bucket_dump_manifests_shared_at(&engine.index_dir, 1).unwrap();
    let before = bucket_dump_io::bucket_dump_manifest_io_counts();
    let mut replacement = manifest.clone();
    replacement.wal_sequence += 1000;
    replacement.checksum = bucket_dump_io::bucket_dump_manifest_checksum_in_place(&mut replacement).unwrap();
    engine.persist_bucket_dump_manifest(&replacement).unwrap();
    let updated = bucket_dump_io::list_bucket_dump_manifests_shared_at(&engine.index_dir, 1).unwrap();
    assert_eq!(updated[0].wal_sequence, replacement.wal_sequence);
    assert!(!Arc::ptr_eq(&listed[0], &updated[0]));
    assert_eq!(bucket_dump_io::bucket_dump_manifest_io_counts().file_reads - before.file_reads, 1);
    let mut recomputed = false;
    bucket_dump_io::manifest_digests_memoised(1, &updated[0], || {
        recomputed = true;
        Some(BTreeMap::new())
    });
    assert!(recomputed, "the old allocation's digest seed must not survive a file replacement");
}

#[test]
fn published_owner_facts_require_the_verified_documents_checksum() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    seed_catalog_wal_models(&engine);
    let manifest = engine.create_bucket_dump_manifest(1, vec![]).unwrap();
    let mut replacement = manifest.clone();
    replacement.wal_sequence += 1000;
    replacement.checksum = bucket_dump_io::bucket_dump_manifest_checksum_in_place(&mut replacement).unwrap();
    assert_ne!(manifest.checksum, replacement.checksum);
    engine.persist_bucket_dump_manifest(&replacement).unwrap();
    let path = bucket_dump_manifest_path(&engine.index_dir, 1, &manifest.manifest_id);
    let replacement_metadata = fs::metadata(path).unwrap();
    // Deliberately pair the old facts with the new inode, isolating checksum binding
    // from the file-identity guard. Neither facts value is valid for the new document.
    bucket_dump_io::remember_published_capture(&engine.index_dir, &manifest,
        Some(&replacement_metadata), BTreeMap::new(), Some(u64::MAX));
    let listed = bucket_dump_io::list_bucket_dump_manifests_shared_at(&engine.index_dir, 1).unwrap();
    assert_eq!(listed[0].checksum, replacement.checksum);
    let mut recomputed = false;
    assert_eq!(bucket_dump_io::manifest_wal_floor_memoised(&listed[0], || {
        recomputed = true;
        Ok(None)
    }), Ok(None));
    assert!(recomputed, "a matching stat must not attach facts from another document");
    let mut digests_computed = false;
    bucket_dump_io::manifest_digests_memoised(1, &listed[0], || {
        digests_computed = true;
        Some(BTreeMap::new())
    });
    assert!(digests_computed);
}

#[test]
fn publication_identity_is_taken_from_the_open_file_before_rename() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("identity");
    let original = atomic_write_bytes_synced_metadata(&path, b"first", true, true).unwrap().unwrap();
    atomic_write_bytes(&path, b"other").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(original.ino(), fs::metadata(&path).unwrap().ino());
    }
    assert_eq!(original.len(), 5);
    assert_eq!(fs::read(path).unwrap(), b"other");
}

#[test]
fn catalog_checkpoint_keeps_locators_for_live_wal_backed_models() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    let queries = seed_catalog_wal_models(&engine);
    let expected = engine.capture_shard_checkpoint(1).unwrap().0.wal_resident_blocks;
    assert!(expected.len() >= 3, "all three models must depend on WAL records");
    for _ in 0..2 {
        let report = engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0).unwrap();
        assert!(!report.wal_sweep_failed && !report.index_log_sweep_failed, "{report:?}");
        let current = engine.capture_shard_checkpoint(1).unwrap().0;
        assert_eq!(current.wal_resident_blocks, expected,
            "an index checkpoint does not relocate the values it still names in WAL");
        let persisted = engine.load_index_base_only(1, false).unwrap();
        assert_eq!(persisted.wal_resident_blocks, expected);
        check_catalog_wal_models(&engine, &queries);
    }
}

#[test]
fn catalog_checkpoint_repeated_dumps_restore_wal_backed_models() {
    for materialize_string in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_at(dir.path());
        let queries = seed_catalog_wal_models(&engine);
        if materialize_string {
            assert_eq!(engine.materialize_synthetic_blocks(1), 1);
        }
        assert!(engine.synthetic_address_count_for_test(1) > 0);
        let anchor = engine.wal_store.stats_without_rescan(1).last_sequence;
        for _ in 0..2 {
            let report = engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0).unwrap();
            assert!(!report.wal_sweep_failed && !report.index_log_sweep_failed, "{report:?}");
            assert_eq!(report.wal_anchor, anchor);
        }
        assert_eq!(engine.load_index_base_only(1, false).unwrap().applied_wal_sequence, Some(anchor),
            "the checkpoint must cover every write, not recover missing locators by tail replay");
        drop(engine);
        let reopened = TemporalEngine::with_local_dirs(
            16 << 20, dir.path().join("fresh-cache"), dir.path().join("blocks"), dir.path().join("indexes"),
        );
        reopened.load_shard(1);
        check_catalog_wal_models(&reopened, &queries);
    }
}

fn dump_without_round_trip(engine: &TemporalEngine, buckets: Vec<u32>) -> BucketDumpManifest {
    let control = CHECKPOINT_DECODES.with(|count| count.get());
    decode_index_bytes(&serialize_index(&ShardState::default())).unwrap();
    assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()) - control, 1,
        "the decode counter must observe a real decode before its zero is trusted");
    let before = CHECKPOINT_DECODES.with(|count| count.get());
    reset_index_encode_counts();
    let manifest = engine.create_bucket_dump_manifest(1, buckets).unwrap();
    assert_eq!(CHECKPOINT_DECODES.with(|count| count.get()) - before, 0,
        "an owned checkpoint must not be decoded to create its manifest");
    assert_eq!(index_encode_counts().encodes_total, 1,
        "the final checkpoint must be encoded once, including retirement");
    manifest
}

#[test]
fn owned_dump_capture_preserves_model_state_and_reports_during_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..64 {
        execute(&engine, Command::StringSet {
            key: format!("capture-{index:04}"), value: vec![index as u8; 128],
        });
    }
    for index in 0..16 {
        for command in [
            Command::HashSet { key: "hash".into(), field: format!("field-{index}"), value: vec![index; 8] },
            Command::SetAdd { key: "set".into(), member: vec![index] },
            Command::ZSetAdd { key: "zset".into(), member: vec![index], score: f64::from(index) },
            Command::ListPush { key: "list".into(), member: vec![index], left: false },
            Command::FeatureAppend { key: "feature".into(), points: vec![crate::types::FeaturePoint {
                timestamp_ms: u64::from(index) + 1, value: vec![index; 8],
            }] },
        ] {
            execute(&engine, command);
        }
    }
    execute(&engine, Command::CommonExpire { key: "hash".into(), ttl_ms: 3_600_000 });
    let (snapshot, _) = engine.capture_shard_checkpoint(1).unwrap();
    let expected_bytes = serialize_index(&snapshot);
    let normalized = decode_index_bytes(&expected_bytes).unwrap();
    let expected_lifecycle = object_lifecycle_report_from_entries(
        1, &normalized, collect_live_block_entries(&normalized), &BTreeSet::new(),
        |key| engine.routing_bucket_for_key(1, key),
    );
    let manifest = dump_without_round_trip(&engine, vec![]);
    assert_eq!(manifest.object_lifecycle, expected_lifecycle);
    let state = decode_index_bytes(&manifest.index_bytes).unwrap();
    // Retirement may remove obsolete placements, but no other persisted field
    // may change and every retained placement must still name the same record.
    let mut expected_state = serde_json::to_value(&normalized).unwrap();
    let mut actual_state = serde_json::to_value(&state).unwrap();
    expected_state.as_object_mut().unwrap().remove("wal_resident_blocks");
    actual_state.as_object_mut().unwrap().remove("wal_resident_blocks");
    assert_eq!(actual_state, expected_state);
    assert!(state.wal_resident_blocks.len() <= normalized.wal_resident_blocks.len());
    for (key, placement) in &state.wal_resident_blocks {
        let expected = normalized.wal_resident_blocks.get(key).unwrap();
        assert_eq!(placement.log_id, expected.log_id);
        assert_eq!(placement.sequence, expected.sequence);
    }
    assert_eq!(manifest.index_sha256, sha256_hex_bytes(&manifest.index_bytes));
    assert_eq!(state.strings().len(), 64);
    assert_eq!(state.bucket_index.hashes.get("hash").unwrap().len(), 16);
    assert_eq!(state.bucket_index.sets.get("set").unwrap().len(), 16);
    assert_eq!(state.bucket_index.zsets.get("zset").unwrap().len(), 16);
    assert_eq!(state.bucket_index.lists.get("list").unwrap().len(), 16);
    assert_eq!(state.features().get("feature").unwrap().len(), 16);
    assert!(state.expires_at_ms().get("hash").is_some());
}

#[test]
fn bucket_dump_capture_omits_deadline_tree_but_keeps_the_ttl_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for key in ["selected-ttl", "other-ttl"] {
        execute(&engine, Command::StringSet { key: key.into(), value: vec![7; 64] });
        execute(&engine, Command::CommonExpire { key: key.into(), ttl_ms: 3_600_000 });
    }
    assert_eq!(engine.materialize_synthetic_blocks(1), 2);
    let (full, _) = engine.capture_shard_checkpoint(1).unwrap();
    let (dump, _) = engine.capture_bucket_dump_checkpoint(1).unwrap();
    assert_eq!(full.expiry_by_deadline.len(), 2);
    assert!(dump.expiry_by_deadline.is_empty());
    assert_eq!(serialize_index(&dump), serialize_index(&full));
    let selected = engine.routing_bucket_for_key(1, "selected-ttl");
    assert_ne!(selected, engine.routing_bucket_for_key(1, "other-ttl"));
    let manifest = engine.create_bucket_dump_manifest(1, [selected]).unwrap();
    assert_eq!(manifest.bucket_ids, [selected]);
    assert_eq!(manifest.wal_sequence, full.applied_wal_sequence.unwrap());
    let (after, _) = engine.capture_shard_checkpoint(1).unwrap();
    assert_eq!(after.expiry_by_deadline, full.expiry_by_deadline,
        "dump capture must not consume the live expiry scheduler");
    let image = decode_index_bytes(&manifest.index_bytes).unwrap();
    for key in ["selected-ttl", "other-ttl"] {
        let deadline = full.expires_at_ms().get(key).unwrap();
        assert_eq!(image.expires_at_ms().get(key), Some(deadline));
        assert!(image.string_address(key).is_some());
    }
    drop(engine);
    let restored = TemporalEngine::with_local_dirs(
        16 << 20, dir.path().join("manifest-cache"), dir.path().join("blocks"),
        dir.path().join("manifest-indexes"),
    );
    restored.load_shard(1);
    assert_eq!(restored.wal_store.stats_without_rescan(1).last_sequence, 0);
    restored.install_bucket_dump_manifest(&manifest).unwrap();
    restored.sweep_expired_records(1).unwrap();
    assert_eq!(restored.wal_store.stats_without_rescan(1).last_sequence, 0,
        "TTL recovery must not depend on the source WAL");
    restored.shards.with_state(1, |state| {
        assert_eq!(state.unwrap().expiry_by_deadline, full.expiry_by_deadline);
    });
    for key in ["selected-ttl", "other-ttl"] {
        check_catalog_wal_models(&restored, &[(
            Command::StringGet { key: key.into() },
            CommandResponse::Bytes { value: Some(vec![7; 64]) },
        )]);
    }
    drop(restored);
    let reopened = engine_at(dir.path());
    for key in ["selected-ttl", "other-ttl"] {
        check_catalog_wal_models(&reopened, &[(
            Command::StringGet { key: key.into() },
            CommandResponse::Bytes { value: Some(vec![7; 64]) },
        )]);
    }
    reopened.shards.with_state(1, |state| {
        let shard = state.unwrap();
        for key in ["selected-ttl", "other-ttl"] {
            assert_eq!(shard.expires_at_ms().get(key), full.expires_at_ms().get(key));
        }
    });
}

#[test]
fn owned_dump_capture_encodes_once_after_registration_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for key in ["live", "gone"] {
        execute(&engine, Command::StringSet { key: key.into(), value: vec![7; 256] });
    }
    let before = engine.registration_count_for_test(1);
    assert_eq!(before, 2);
    execute(&engine, Command::StringDelete { key: "gone".into() });
    let manifest = dump_without_round_trip(&engine, vec![]);
    assert_eq!(engine.registration_count_for_test(1), 1);
    assert_eq!(engine.wal_resident_block_count(1), 1);
    let state = decode_index_bytes(&manifest.index_bytes).unwrap();
    assert_eq!(state.index_format_version, SHARD_INDEX_FORMAT_VERSION);
    assert_eq!(state.wal_resident_blocks.len(), 1);
    assert!(state.string_address("live").is_some());
    assert!(state.string_address("gone").is_none());
    assert_eq!(manifest.index_sha256, sha256_hex_bytes(&manifest.index_bytes));
    assert_eq!(manifest.version, 4);
    assert_eq!(manifest.wal_frames.len(), 1);
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    let unrelated_root = tempfile::tempdir().unwrap();
    let unrelated = engine_at(unrelated_root.path());
    let before_index = std::fs::read(unrelated.index_path(1)).ok();
    let before_manifests = unrelated.list_bucket_dump_manifests(1);
    let (before, _) = unrelated.capture_shard_checkpoint(1).unwrap();
    let status = unrelated.validate_bucket_dump_manifest(&manifest).unwrap_err();
    assert_eq!(status.code, "slot_dump_invalid_wal_binding");
    let status = unrelated.install_bucket_dump_manifest(&manifest).unwrap_err();
    assert_eq!(status.code, "slot_dump_invalid_wal_binding");
    assert_eq!(std::fs::read(unrelated.index_path(1)).ok(), before_index);
    assert_eq!(unrelated.list_bucket_dump_manifests(1), before_manifests);
    let (after, _) = unrelated.capture_shard_checkpoint(1).unwrap();
    assert_eq!(serialize_index(&before), serialize_index(&after));
    drop(engine);
    let restored = engine_at(dir.path());
    for (key, expected) in [("live", Some(vec![7; 256])), ("gone", None)] {
        let response = restored.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: key.into() },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: expected });
    }
}

#[test]
fn owned_dump_capture_keeps_released_buckets_and_installs_without_source_logs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..32 {
        execute(&engine, Command::StringSet {
            key: format!("released-{index:04}"), value: vec![index as u8; 128],
        });
    }
    assert_eq!(engine.materialize_synthetic_blocks(1), 32);
    let first = engine.create_bucket_dump_manifest(1, []).unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &first);
    let released = engine.shards.with_state_mut(1, |state| {
        let shard = state.unwrap();
        let buckets = shard.bucket_index.bucket_map.keys().copied().take(8).collect::<Vec<_>>();
        let outcome = storage_bucket_internals::release_bucket_blocks(shard, &buckets);
        assert_eq!(outcome.released_buckets, buckets);
        buckets
    });
    let selected = engine.routing_bucket_for_key(1, "released-0031");
    let (snapshot, _) = engine.capture_shard_checkpoint(1).unwrap();
    let expected = serialize_index(&snapshot);
    let manifest = dump_without_round_trip(&engine, vec![selected]);
    assert_eq!(manifest.index_bytes, expected);
    assert_eq!(manifest.bucket_ids, vec![selected]);
    engine.shards.with_state(1, |state| {
        assert_eq!(state.unwrap().bucket_index.released_buckets.len(), released.len());
    });
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    drop(engine);
    let restored = TemporalEngine::with_local_dirs(
        16 << 20, dir.path().join("fresh-cache"), dir.path().join("blocks"),
        dir.path().join("fresh-indexes"),
    );
    restored.load_shard(1);
    assert_eq!(restored.wal_store.stats_without_rescan(1).last_sequence, 0);
    restored.install_bucket_dump_manifest(&manifest).unwrap();
    for index in 0..32 {
        let result = restored.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: format!("released-{index:04}") },
        });
        assert!(result.status.ok);
        assert_eq!(result.response, CommandResponse::Bytes { value: Some(vec![index as u8; 128]) });
    }
}

#[test]
fn live_entry_stream_does_not_hold_one_owned_row_per_hash_field() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..128 {
        execute(&engine, Command::HashSet {
            key: "wide-hash".into(), field: format!("field-{index:04}"), value: vec![7; 32],
        });
    }
    engine.shards.with_state(1, |state| {
        let shard = state.unwrap();
        let entries = collect_live_block_entries(shard);
        assert_eq!(entries.len(), 128);
        let key = Arc::clone(&entries[0].object_key);
        drop(entries);
        let base_handles = Arc::strong_count(&key);
        let control = collect_live_block_entries(shard);
        assert_eq!(Arc::strong_count(&key), base_handles + 128,
            "the collected control must retain one key handle per block");
        let expected = control.iter().map(|entry| (
            entry.kind, entry.object_key.to_string(), entry.address.clone(), entry.filed_bucket(),
            entry.dirty, entry.deleted, entry.log_backed,
        )).collect::<Vec<_>>();
        drop(control);
        let mut index = 0;
        visit_live_block_entries(shard, |entry| {
            assert_eq!(Arc::strong_count(&key), base_handles + 1);
            assert_eq!((entry.kind, entry.object_key.to_string(), entry.address.clone(), entry.filed_bucket(),
                        entry.dirty, entry.deleted, entry.log_backed), expected[index]);
            index += 1;
        });
        assert_eq!(index, 128);
        assert_eq!(Arc::strong_count(&key), base_handles);
    });
}

#[test]
fn streamed_bucket_facts_match_collected_facts_with_deletes_and_released_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..96 {
        execute(&engine, Command::HashSet {
            key: format!("stream-{index:04}"), field: "value".into(), value: vec![7; 128],
        });
        execute(&engine, Command::StringSet {
            key: format!("released-stream-{index:04}"), value: vec![9; 128],
        });
    }
    for index in 0..8 {
        execute(&engine, Command::HashDelete {
            key: format!("stream-{index:04}"), field: "value".into(),
        });
    }
    engine.materialize_synthetic_blocks(1);
    for release in [false, true] {
        if release {
            let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
            engine.clear_dumped_bucket_dirty_state(1, &manifest);
            engine.shards.with_state_mut(1, |state| {
                let shard = state.unwrap();
                // Release supports addressable scalar blocks, not hash fields.
                let buckets = shard.bucket_index.bucket_map.iter()
                    .filter(|(_, bucket)| !bucket.block_index.is_empty()
                        && bucket.block_index.values().all(|block| block.model_id().as_str() == "string"))
                    .map(|(id, _)| *id).take(8).collect::<Vec<_>>();
                assert!(!buckets.is_empty());
                let released = storage_bucket_internals::release_bucket_blocks(shard, &buckets);
                assert!(!released.released_buckets.is_empty(), "{released:?}");
            });
        }
        let resolve_bucket = engine.routing_bucket_resolver(1);
        engine.shards.with_state(1, |state| {
            let shard = state.unwrap();
            let (start, end) = shard.routing_range();
            let entries = collect_live_block_entries(shard);
            assert_eq!(bucket_storage_summaries(shard, start, end),
                bucket_storage_summaries_from_entries(shard, start, end, &entries));
            let selected = entries.iter().take(8).map(|entry| entry.filed_bucket().unwrap()).collect::<BTreeSet<_>>();
            let released = shard.bucket_index.released_buckets.clone();
            for selected in [BTreeSet::new(), released, selected] {
                assert_selected_summaries(shard, start, end, &entries, &selected);
                let report = storage_object_lifecycle_report_for_buckets(1, shard, &selected,
                    &resolve_bucket);
                let mut owners = BTreeMap::<u64, BTreeSet<u64>>::new();
                let mut refs = 0u64;
                for entry in &entries {
                    if selected.is_empty() || selected.contains(&entry.filed_bucket().unwrap()) {
                        refs += 1;
                        owners.entry(expected_live_block_object_id(1, entry)).or_default()
                            .insert(stable_object_hash(&format!("{}:{}", entry.kind.as_str(), entry.object_key)));
                    }
                }
                assert_eq!(report.live_object_ids, owners.len() as u64);
                assert_eq!(report.live_block_refs, refs);
                assert_eq!(report.reused_object_ids, owners.into_iter()
                    .filter_map(|(id, terms)| (terms.len() > 1).then_some(id)).collect::<Vec<_>>());
            }
        });
    }
}

#[test]
fn loaded_manifest_install_failure_after_rename_keeps_tail_recoverable() {
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    let timeout = Duration::from_secs(10);
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    engine.load_shard(2);
    let set = |shard_id, key: &str, bytes: &[u8]| ExecuteRequest {
        shard_id,
        command: Command::StringSet { key: key.into(), value: bytes.to_vec() },
    };
    let get = |shard_id, key: &str| ExecuteRequest {
        shard_id, command: Command::StringGet { key: key.into() },
    };
    let exact = |engine: &TemporalEngine, request, bytes: &[u8]| {
        let response = engine.execute(request);
        assert!(response.status.ok, "{:?}", response.status);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(bytes.to_vec()) });
    };
    assert!(engine.execute_durable(set(1, "base", b"manifest")).status.ok);
    engine.materialize_synthetic_blocks(1);
    engine.flush_shard_index(1);
    let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
    assert!(manifest.wal_sequence > 0);
    assert!(!manifest.block_slab_ids.is_empty());
    assert!(manifest.block_slab_ids.iter().all(|id| !crate::wal_record::is_wal_resident(*id)));
    assert!(engine.bucket_dump_install_preflight_report(&manifest).install_safe);
    engine.clear_dumped_bucket_dirty_state(1, &manifest);

    let previous_owner = engine.shards.handle(1).unwrap();
    let before = engine.checkpoint_publications.capture(1);
    std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let publisher = scope.spawn(|| {
            engine.checkpoint_publications.publish(&before, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(timeout).unwrap();
                Ok(())
            })
        });
        entered_rx.recv_timeout(timeout).unwrap();
        assert!(engine.checkpoint_publications.is_current_after_publish(&before));

        let installer = scope.spawn(|| {
            super::super::persistence::fail_closed_install_after_rename_for_test();
            engine.install_bucket_dump_manifest(&manifest)
        });
        // While this publisher remains admitted, only closing invalidates its revision.
        let limit = Instant::now() + timeout;
        while engine.checkpoint_publications.is_current_after_publish(&before) {
            assert!(Instant::now() < limit, "install never reached publication drain");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(engine.pass_slot(1).try_lock().is_err());

        let (wrote_tx, wrote_rx) = channel();
        let writer_engine = &engine;
        let writer = scope.spawn(move || {
            let peer = writer_engine.execute_durable(set(2, "peer", b"peer committed"));
            let tail = writer_engine.execute_durable(set(1, "tail", b"drain committed"));
            wrote_tx.send((peer, tail)).unwrap();
        });
        let writes = wrote_rx.recv_timeout(timeout);
        release_tx.send(()).unwrap();
        assert_eq!(publisher.join().unwrap().unwrap(), Some(()));
        writer.join().unwrap();
        let failure = installer.join().unwrap().unwrap_err();
        assert_eq!(failure.code, "slot_dump_install_failed");
        assert!(failure.message.contains("injected failure after served index rename"), "{failure:?}");
        let (peer, tail) = writes.expect("writes waited for the publication drain");
        assert!(peer.status.ok && tail.status.ok, "{peer:?}, {tail:?}");
    });

    assert!(previous_owner.same_incarnation(&engine.shards.handle(1).unwrap()));
    assert!(!engine.get_info(1).info.unwrap().recovering);
    assert!(engine.pass_slot(1).try_lock().is_ok());
    exact(&engine, get(1, "base"), b"manifest");
    exact(&engine, get(1, "tail"), b"drain committed");
    exact(&engine, get(2, "peer"), b"peer committed");
    let on_disk = std::fs::read(engine.index_path(1)).unwrap();
    assert_eq!(decode_index_bytes(&on_disk).unwrap().applied_wal_sequence, Some(manifest.wal_sequence));
    assert!(engine.shards.with_state(1, |state| {
        state.unwrap().applied_wal_sequence.is_some_and(|sequence| sequence > manifest.wal_sequence)
    }));
    assert!(engine.interrupted_bucket_dump_installs(1).iter().any(|marker| {
        marker.manifest_id == manifest.manifest_id && marker.phase == "prepare"
    }));

    // Use live collection entry points. The reopened engine must replay from the
    // installed base even though the old, still-serving owner has a higher anchor.
    let plan = engine.storage_wal_reclaim_plan(1, [], []);
    assert!(plan.retain_from_wal_sequence <= manifest.wal_sequence + 1, "{plan:?}");
    engine.apply_storage_wal_reclaim(plan);
    engine.apply_periodic_index_gc(StorageLifecycleRequest { shard_id: 1, ..Default::default() }, None, 1024);
    assert_eq!(std::fs::read(engine.index_path(1)).unwrap(), on_disk);
    // Drop without an unload or checkpoint, so the old live state cannot repair the base.
    drop((previous_owner, before, engine));

    let reopened = TemporalEngine::with_local_dirs(
        16 << 20, root.path().join("reopen-cache"), root.path().join("blocks"), root.path().join("indexes"),
    );
    reopened.load_shard(1);
    reopened.load_shard(2);
    exact(&reopened, get(1, "base"), b"manifest");
    exact(&reopened, get(1, "tail"), b"drain committed");
    exact(&reopened, get(2, "peer"), b"peer committed");
}


fn assert_selected_summaries(
    shard: &ShardState,
    start: u32,
    end: u32,
    entries: &[LiveBlockEntry],
    selected: &BTreeSet<u32>,
) {
    use super::super::storage_reporting::{
        selected_bucket_storage_summaries_from_visit, take_selected_summary_work,
    };
    let full = bucket_storage_summaries_from_entries(shard, start, end, entries);
    let expected = full.into_iter().filter(|summary| {
        selected.is_empty() || selected.contains(&summary.routing_bucket)
    }).collect::<Vec<_>>();
    let _ = take_selected_summary_work();
    let mut global_visits = 0;
    let actual = selected_bucket_storage_summaries_from_visit(
        shard, start, end, selected, |consume| {
            for entry in entries {
                consume(entry);
                global_visits += 1;
            }
        },
    );
    assert_eq!(actual, expected);
    assert_eq!(global_visits, entries.len(), "the whole-image observer still sees all entries");
    let expected_work = if selected.is_empty() {
        (0, 0, 0)
    } else {
        let refs = expected.iter().map(|summary| summary.block_ref_count as usize).sum();
        (refs, selected.len(), selected.len())
    };
    assert_eq!(take_selected_summary_work(), expected_work);
}

#[test]
fn selected_summaries_use_filing_and_bound_work_as_clean_state_grows() {
    use super::super::hashing::block_routing_bucket;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    let key = "misfiled-dump-owner";
    let original = engine.routing_bucket_for_key(1, key);
    let filed = original.wrapping_add(1);
    let dirty_only = original.wrapping_add(2);
    let missing = original.wrapping_add(3);
    let tombstone_only = original.wrapping_add(4);
    execute(&engine, Command::StringSet { key: key.into(), value: vec![7; 128] });
    for (start_index, stop_index) in [(0, 8), (8, 256)] {
        for index in start_index..stop_index {
            let mut candidate = format!("clean-dump-{index}");
            while [original, filed, dirty_only, missing, tombstone_only]
                .contains(&engine.routing_bucket_for_key(1, &candidate))
            {
                candidate.push('x');
            }
            execute(&engine, Command::StringSet { key: candidate, value: vec![8; 32] });
        }
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        engine.clear_dumped_bucket_dirty_state(1, &manifest);
        let (mut snapshot, _) = engine.capture_shard_checkpoint(1).unwrap();
        assert_eq!(snapshot.dirty_objects.bucket_count(original), 0);
        // Preserve the original owner lookup and move the indexed handle to a different bucket.
        // The full walk must find its key by handle, while reporting the actual filed bucket.
        let mut node = snapshot.bucket_index.bucket_map.remove(&original).unwrap();
        assert_eq!(node.block_index.len(), 1);
        node.routing_bucket = filed;
        assert!(snapshot.bucket_index.bucket_map.insert(filed, node).is_none());
        snapshot.dirty_objects.insert("dirty-only-dump-owner", dirty_only);
        assert!(!snapshot.bucket_index.bucket_map.contains_key(&dirty_only));
        let mut tombstone = super::super::state::BucketNode::default();
        tombstone.routing_bucket = tombstone_only;
        tombstone.dirty_generation = 41;
        tombstone.deleted_object_index.insert(1);
        tombstone.set_dirty(true);
        assert!(snapshot.bucket_index.bucket_map.insert(tombstone_only, tombstone).is_none());
        let mut entries = collect_live_block_entries(&snapshot);
        assert_eq!(entries.len(), stop_index + 1);
        let entry = entries.iter().find(|entry| entry.object_key.as_ref() == key).unwrap();
        assert_eq!(entry.filed_bucket(), Some(filed));
        // Add a model-map style entry with no filing so the supplied range is observable.
        let mut unfenced = entry.clone();
        unfenced.object_key = Arc::from("unfiled-dump-owner");
        unfenced.filing_is_known = false;
        entries.push(unfenced);
        for (start, end) in [(0, u32::MAX), (17, 47)] {
            let derived = block_routing_bucket("unfiled-dump-owner", start, end);
            assert_ne!(derived, missing);
            for selected in [
                BTreeSet::from([filed]),
                BTreeSet::from([derived, dirty_only]),
                BTreeSet::from([missing]),
                BTreeSet::from([tombstone_only]),
                BTreeSet::new(),
            ] {
                assert_selected_summaries(&snapshot, start, end, &entries, &selected);
            }
        }
    }
}
