// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(root: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(1 << 20, root.join(cache), root.join("blocks"), root.join("indexes"))
}

fn put(engine: &TemporalEngine, key: &str, value: &[u8]) {
    assert!(engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringSet { key: key.into(), value: value.to_vec() },
    }).status.ok);
}

fn seed(root: &std::path::Path) -> TemporalEngine {
    let engine = open(root, "seed-cache");
    engine.load_shard(1);
    assert!(engine.set_config(crate::control::SetConfigRequest {
        shard_id: 1, config: Config { version: 2, async_storage: false, ..Default::default() },
    }).ok);
    put(&engine, "base", b"base");
    engine.flush_shard_index(1);
    engine
}

fn append_catalog(engine: &TemporalEngine) -> crate::index_log::MetaItem {
    let anchor = decode_index_bytes(&fs::read(engine.index_path(1)).unwrap()).unwrap()
        .applied_wal_sequence.unwrap();
    let meta = crate::index_log::MetaItem {
        version: 1, start_wal_sequence: anchor, timestamp_ms: now_ms(),
        slab_version: anchor, slabs: engine.block_store.slab_catalog(anchor),
    };
    assert!(!meta.slabs.is_empty());
    engine.index_log_store.append_delta(1, vec![], vec![], Some(anchor), Some(meta.clone()), false, true).unwrap();
    meta
}

fn frames(engine: &TemporalEngine) -> u64 {
    crate::index_log::probe::reset();
    engine.index_log_store.read_delta_records(1, 0).unwrap();
    let count = crate::index_log::probe::fold_frames_read();
    assert!(count > 0);
    count
}

#[test]
fn durable_restore_reads_the_base_and_catalog_in_one_pass() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    let expected = append_catalog(&engine);
    let count = frames(&engine);
    let reader = open(dir.path(), "cold-reader-cache");
    crate::index_log::probe::reset();
    let (state, anchor, catalog, _proof) = reader.load_durable_index_with_catalog(1, false).unwrap();
    assert_eq!(crate::index_log::probe::fold_frames_read(), count);
    assert_eq!(catalog, Some(expected));
    assert_eq!(state.unwrap().applied_wal_sequence, Some(anchor));
}

#[test]
fn durable_restore_selects_the_newer_manifest_before_its_only_pass() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    let old_base = fs::read(engine.index_path(1)).unwrap();
    put(&engine, "newer", b"in the manifest");
    let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
    fs::write(engine.index_path(1), old_base).unwrap();
    let count = frames(&engine);
    crate::index_log::probe::reset();
    let (state, anchor, _, _proof) = engine.load_durable_index_with_catalog(1, false).unwrap();
    assert_eq!(crate::index_log::probe::fold_frames_read(), count);
    assert_eq!(anchor, manifest.wal_sequence);
    assert!(state.unwrap().string_address("newer").is_some());
}

#[test]
fn default_recovery_copies_no_manifest_and_repacks_only_the_selected_image() {
    let _placement = crate::engine::value_once_in_test(false);
    assert!(crate::engine::wal_single_barrier());

    // The old and new images are both real checkpoints. Restore one as the served
    // file after the writer exits to exercise each relationship to the manifest.
    for case in ["tie", "older manifest", "newer manifest", "rejected manifest"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = seed(dir.path());
        let old_bytes = fs::read(engine.index_path(1)).unwrap();
        let old_anchor = decode_index_bytes(&old_bytes).unwrap().applied_wal_sequence.unwrap();

        let manifest = if case == "older manifest" {
            engine.create_bucket_dump_manifest(1, []).unwrap()
        } else {
            put(&engine, "base", b"new value");
            engine.create_bucket_dump_manifest(1, []).unwrap()
        };
        if case == "older manifest" {
            put(&engine, "base", b"new value");
        }
        engine.flush_shard_index(1);
        let new_bytes = fs::read(engine.index_path(1)).unwrap();
        let new_anchor = decode_index_bytes(&new_bytes).unwrap().applied_wal_sequence.unwrap();
        assert!(new_anchor > old_anchor, "{case}");
        assert_eq!(
            manifest.wal_sequence,
            if case == "older manifest" { old_anchor } else { new_anchor },
            "{case}"
        );

        if case == "rejected manifest" {
            // The envelope and its digests validate, but the codec rejects the image.
            let mut rejected = manifest.clone();
            rejected.index_bytes[crate::engine::INDEX_CONTAINER_MAGIC.len()] = u8::MAX;
            rejected.index_sha256 = crate::engine::sha256_hex_bytes(&rejected.index_bytes);
            rejected.dump_generation_id = crate::engine::bucket_dump_generation_id(&rejected);
            rejected.checksum = crate::engine::bucket_dump_codec::bucket_dump_protobuf_digest(&rejected).0;
            let path = crate::engine::bucket_dump_manifest_path(
                &engine.index_dir, 1, &rejected.manifest_id,
            );
            fs::write(path, crate::engine::bucket_dump_codec::encode_bucket_dump_manifest(&rejected))
                .unwrap();
        }

        drop(engine);
        let reader = open(dir.path(), "fresh-reader-cache");
        let served_bytes = if matches!(case, "newer manifest" | "rejected manifest") {
            &old_bytes
        } else {
            &new_bytes
        };
        fs::write(reader.index_path(1), served_bytes).unwrap();

        reset_bucket_dump_manifest_io_counts();
        let picked = durable_recovery_bucket_dump_manifest_shared_at(&reader.index_dir, 1).unwrap();
        assert_eq!(picked.manifest_id, manifest.manifest_id, "{case}");
        let counts_before = bucket_dump_manifest_io_counts();
        let repacks_before = state::thread_repack_calls();
        let (selected, anchor, _, _proof) =
            reader.load_durable_index_with_catalog(1, false).unwrap();
        let counts_after = bucket_dump_manifest_io_counts();
        assert!(counts_after.parse_hits > counts_before.parse_hits, "{case}");
        assert_eq!(counts_after.owned_copies, counts_before.owned_copies, "{case}");
        assert_eq!(counts_after.owned_copy_bytes, counts_before.owned_copy_bytes, "{case}");
        assert_eq!(state::thread_repack_calls() - repacks_before, 1, "{case}");
        let expected_anchor = if case == "rejected manifest" { old_anchor } else { new_anchor };
        assert_eq!(anchor, expected_anchor, "{case}");
        assert_eq!(selected.unwrap().applied_wal_sequence, Some(expected_anchor), "{case}");

        reader.load_shard(1);
        let result = reader.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: "base".into() },
        });
        assert!(result.status.ok, "{case}");
        assert_eq!(
            result.response,
            crate::types::CommandResponse::Bytes { value: Some(b"new value".to_vec()) },
            "{case}"
        );
    }
}

#[test]
fn durable_restore_leaves_an_append_for_exactly_one_wal_replay() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    let old_base = fs::read(engine.index_path(1)).unwrap();
    let base = decode_index_bytes(&old_base).unwrap();
    assert!(engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringAppend { key: "base".into(), value: b"-tail".to_vec() },
    }).status.ok);
    fs::write(engine.index_path(1), &old_base).unwrap();
    let (selected, anchor, _, _proof) = engine.load_durable_index_with_catalog(1, false).unwrap();
    assert_eq!(anchor, base.applied_wal_sequence.unwrap());
    assert_eq!(selected.unwrap().string_address("base"), base.string_address("base"));
    drop(engine);
    let reader = open(dir.path(), "reader-cache");
    fs::write(reader.index_path(1), &old_base).unwrap();
    reader.load_shard(1);
    assert_eq!(reader.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringGet { key: "base".into() },
    }).response, crate::types::CommandResponse::Bytes { value: Some(b"base-tail".to_vec()) });
}

#[test]
fn catalog_recovery_survives_an_absent_or_rejected_base() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    let expected = append_catalog(&engine);
    for rejected in [false, true] {
        if rejected { fs::write(engine.index_path(1), b"invalid base").unwrap(); }
        else { fs::remove_file(engine.index_path(1)).unwrap(); }
        let count = frames(&engine);
        crate::index_log::probe::reset();
        let (state, anchor, catalog, _proof) = engine.load_durable_index_with_catalog(1, false).unwrap();
        assert_eq!(crate::index_log::probe::fold_frames_read(), count);
        assert!(state.is_none());
        assert_eq!(anchor, 0);
        assert_eq!(catalog, Some(expected.clone()));
    }
}

#[test]
fn checked_restore_reads_the_catalog_in_its_delta_pass() {
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    let expected = append_catalog(&engine);
    let count = frames(&engine);
    crate::index_log::probe::reset();
    let (state, catalog) = engine.load_index_checked_with_catalog(1, false).unwrap();
    assert!(state.is_some());
    assert_eq!(catalog, Some(expected));
    assert_eq!(crate::index_log::probe::fold_frames_read(), count);
}

#[test]
fn a_corrupt_tail_never_installs_a_partial_catalog() {
    use std::io::Write;
    let _placement = crate::engine::value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = seed(dir.path());
    append_catalog(&engine);
    let logs: Vec<_> = fs::read_dir(engine.index_dir.join("indexlogs")).unwrap()
        .map(|entry| entry.unwrap().path()).filter(|path| path.is_file()).collect();
    assert_eq!(logs.len(), 1);
    let mut log = fs::OpenOptions::new().append(true).open(&logs[0]).unwrap();
    log.write_all(&crate::log_framing::encode_record(b"invalid record")).unwrap();
    log.sync_all().unwrap();
    assert!(engine.load_index_checked_with_catalog(1, false).is_err());
    let (state, _, catalog, _proof) = engine.load_durable_index_with_catalog(1, false).unwrap();
    assert!(state.is_some(), "the durable base must remain available for WAL recovery");
    assert!(catalog.is_none(), "a valid prefix is not a complete catalog scan");
}
