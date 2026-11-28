// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(20);
const KEY: &str = "completion-owner";

fn engine_at(root: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    let predecessor = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: "completion-predecessor".into(), value: b"anchor".to_vec() },
    });
    assert!(predecessor.status.ok, "{:?}", predecessor.status);
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    assert!(manifest.wal_sequence > 0 && manifest.index_log_sequence > 0);
    engine.clear_dumped_bucket_dirty_state(1, &manifest);
    write(&engine, b"before");
    engine
}

fn write(engine: &TemporalEngine, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: KEY.into(), value: value.to_vec() },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn exact_value(engine: &TemporalEngine, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringGet { key: KEY.into() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(response.response, CommandResponse::Bytes { value: Some(value.to_vec()) });
}

#[derive(Debug, PartialEq, Eq)]
struct Pending {
    keys: Vec<String>,
    nodes: BTreeMap<u32, (bool, u64, u64, u64, Vec<(bool, bool)>)>,
    durable_items: u64,
    live_items: u64,
}

fn pending(engine: &TemporalEngine) -> Pending {
    engine.shards.with_state(1, |state| {
        let state = state.expect("loaded owner");
        Pending {
            keys: state.dirty_objects.iter().map(str::to_owned).collect(),
            durable_items: state.durable_items(),
            live_items: state.live_item_count(),
            nodes: state.bucket_index.bucket_map.iter().map(|(id, node)| {
                (*id, (node.dirty(), node.dirty_generation,
                    node.first_dirty_wal_sequence, node.first_dirty_index_log_sequence,
                    node.block_index.values().map(|block| (block.dirty(), block.deleted())).collect()))
            }).collect(),
        }
    })
}

fn positive_claims(state: &Pending) {
    assert_eq!(state.keys, vec![KEY.to_owned()]);
    assert!(state.nodes.values().any(|node| node.0 && node.2 > 0 && node.3 > 0),
        "the test must carry real positive pending claims: {state:?}");
}

fn request() -> StorageLifecycleRequest {
    StorageLifecycleRequest {
        item_amplification_full_dump_ppm: 0,
        shard_id: 1, selected_dump_buckets: Vec::new(), max_dump_buckets_per_round: 1,
        min_undumped_wal_records: 0, min_undumped_wal_bytes: 0,
        purge_delayed_destroy: false, purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false, roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(), block_gc_shared_store_cursors: Vec::new(),
        block_gc_raft_snapshot_refs: Vec::new(), block_gc_checkpoint_floor_slab_id: None,
        block_gc_raft_install_floor_slab_id: None,
        block_gc_delayed_destroy_grace_ms: 0, invalidate_cache: false, warm_cache: false,
    }
}

struct ResetHook;
impl Drop for ResetHook {
    fn drop(&mut self) {
        BEFORE_DUMP_DIRTY_COMPLETION.with(|cell| { cell.borrow_mut().take(); });
    }
}

fn parked_completion(engine: &TemporalEngine) -> (
    Receiver<()>, Sender<()>, std::thread::JoinHandle<StorageLifecycleReport>,
) {
    let engine = engine.clone();
    let (published_tx, published_rx) = channel();
    let (resume_tx, resume_rx) = channel();
    let worker = std::thread::spawn(move || {
        let _reset = ResetHook;
        BEFORE_DUMP_DIRTY_COMPLETION.with(|cell| {
            assert!(cell.borrow().is_none());
            *cell.borrow_mut() = Some(Box::new(move || {
                published_tx.send(()).unwrap();
                resume_rx.recv_timeout(WAIT).expect("release parked completion");
            }));
        });
        engine.apply_storage_lifecycle(request())
    });
    (published_rx, resume_tx, worker)
}

#[test]
fn create_only_keeps_claims_and_current_normal_completion_clears_them() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    let before = pending(&engine);
    positive_claims(&before);
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    assert_eq!(pending(&engine), before, "public create must remain create-only");
    let report = engine.apply_storage_lifecycle(request());
    let manifest = report.dump_manifest.expect("normal path must durably publish");
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    let after = pending(&engine);
    assert!(after.keys.is_empty());
    assert!(!after.nodes.is_empty(), "this patch must not prune owner nodes");
    for node in after.nodes.values() {
        assert!(!node.0);
        assert_eq!((node.2, node.3), (0, 0));
        assert!(node.4.iter().all(|flags| !flags.0));
    }
    exact_value(&engine, b"before");
}

#[test]
fn unrelated_write_and_newer_publication_do_not_stall_selected_owner_completion() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    let before = pending(&engine);
    positive_claims(&before);
    let owner = engine.shards.handle(1).unwrap();
    let (start, end) = owner.with_read(|state| state.unwrap().routing_range());
    let key_bucket = super::super::hashing::block_routing_bucket(KEY, start, end);
    let other = (0..10_000).map(|index| format!("completion-other-{index}"))
        .find(|key| super::super::hashing::block_routing_bucket(key, start, end) != key_bucket).unwrap();
    let other_bucket = super::super::hashing::block_routing_bucket(&other, start, end);
    assert_ne!(key_bucket, other_bucket);
    let (published, resume, worker) = parked_completion(&engine);
    published.recv_timeout(WAIT).unwrap();
    assert!(engine.pass_slot(1).try_lock().is_ok(),
        "normal completion must not secretly hold the lifecycle reservation");
    for value in [b"other-before".as_slice(), b"other-after".as_slice()] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringSet { key: other.clone(), value: value.to_vec() },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    let accrued = pending(&engine);
    assert_eq!(accrued.keys.iter().cloned().collect::<BTreeSet<_>>(), BTreeSet::from([KEY.to_owned(), other.clone()]));
    assert_eq!(accrued.nodes[&key_bucket], before.nodes[&key_bucket], "unrelated writes must leave the selected owner unchanged");
    let other_claims = &accrued.nodes[&other_bucket];
    assert!(other_claims.0 && other_claims.2 > 0 && other_claims.3 > 0);
    assert!(accrued.durable_items > accrued.live_items, "real overwritten records must make an item-reset observable");
    let newer = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.validate_bucket_dump_manifest(&newer).unwrap();
    assert_eq!(pending(&engine), accrued, "newer create-only publication must not clear either owner");
    assert!(owner.same_incarnation(&engine.shards.handle(1).unwrap()));
    resume.send(()).unwrap();
    let manifest = worker.join().unwrap().dump_manifest.unwrap();
    assert_eq!(manifest.bucket_ids, vec![key_bucket]);
    let after = pending(&engine);
    assert_eq!(after.keys, vec![other.clone()], "only the captured selected owner's claims may drain");
    let selected = &after.nodes[&key_bucket];
    assert!(!selected.0);
    assert_eq!((selected.2, selected.3), (0, 0));
    assert!(selected.4.iter().all(|flags| !flags.0));
    assert_eq!(after.nodes[&other_bucket], accrued.nodes[&other_bucket], "retain every unrelated claim/flag exactly");
    assert_eq!(after.durable_items, accrued.durable_items, "new item accrual must not be reset by the old dump");
    assert_eq!(after.live_items, accrued.live_items);
    exact_value(&engine, b"before");
    let response = engine.execute(ExecuteRequest { shard_id: 1, command: Command::StringGet { key: other } });
    assert!(response.status.ok);
    assert_eq!(response.response, CommandResponse::Bytes { value: Some(b"other-after".to_vec()) });
}

#[test]
fn public_unload_reload_and_write_cannot_inherit_a_parked_normal_completion() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    positive_claims(&pending(&engine));
    let previous = engine.shards.handle(1).unwrap();
    let (published, resume, worker) = parked_completion(&engine);
    published.recv_timeout(WAIT).unwrap();
    let response = engine.unload_shard_with(UnloadShardRequest { shard_id: 1 });
    assert!(response.status.ok, "{:?}", response.status);
    engine.load_shard(1);
    let replacement = engine.shards.handle(1).unwrap();
    assert!(!previous.same_incarnation(&replacement));
    write(&engine, b"replacement");
    let before = pending(&engine);
    positive_claims(&before);
    resume.send(()).unwrap();
    assert!(worker.join().unwrap().dump_manifest.is_some());
    assert_eq!(pending(&engine), before, "old completion must not touch the replacement");
    exact_value(&engine, b"replacement");
}

#[test]
fn a_newer_write_keeps_its_real_claims_after_the_older_normal_dump() {
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    let (published, resume, worker) = parked_completion(&engine);
    published.recv_timeout(WAIT).unwrap();
    write(&engine, b"newer");
    let before = pending(&engine);
    positive_claims(&before);
    resume.send(()).unwrap();
    assert!(worker.join().unwrap().dump_manifest.is_some());
    assert_eq!(pending(&engine), before);
    exact_value(&engine, b"newer");
}

#[test]
fn replacement_between_entry_handle_and_checkpoint_capture_keeps_new_owner_pending() {
    struct ResetCapture;
    impl Drop for ResetCapture {
        fn drop(&mut self) {
            BEFORE_DUMP_CHECKPOINT_CAPTURE.with(|cell| { cell.borrow_mut().take(); });
        }
    }
    let _reset = ResetCapture;
    let root = tempfile::tempdir().unwrap();
    let engine = engine_at(root.path());
    let previous = engine.shards.handle(1).unwrap();
    positive_claims(&pending(&engine));
    let expected = std::sync::Arc::new(std::sync::Mutex::new(None));
    let captured_expected = expected.clone();
    let concurrent = engine.clone();
    let old = previous.clone();
    BEFORE_DUMP_CHECKPOINT_CAPTURE.with(|cell| {
        assert!(cell.borrow().is_none());
        *cell.borrow_mut() = Some(Box::new(move || {
            // No lifecycle reservation or shard guard is held at this exact seam.
            let unloaded = concurrent.unload_shard_with(UnloadShardRequest { shard_id: 1 });
            assert!(unloaded.status.ok, "{:?}", unloaded.status);
            concurrent.load_shard(1);
            let replacement = concurrent.shards.handle(1).unwrap();
            assert!(!old.same_incarnation(&replacement));
            assert!(old.with_read(|state| state.is_none()), "replacement must empty the saved handle");
            write(&concurrent, b"replacement-before-capture");
            let before = pending(&concurrent);
            positive_claims(&before);
            *captured_expected.lock().unwrap() = Some(before);
        }));
    });
    let report = engine.apply_storage_lifecycle(request());
    let manifest = report.dump_manifest.expect("replacement capture must actually publish");
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    let before = expected.lock().unwrap().take().expect("the pre-capture seam must run");
    assert!(previous.with_read(|state| state.is_none()));
    assert_eq!(pending(&engine), before,
        "a replacement's successful publication cannot authorize the saved old handle to clear it");
    exact_value(&engine, b"replacement-before-capture");
}

const BULK_STAGE: &str = "TS_DUMP_COMPLETION_BULK_STAGE";
const BULK_ROOT: &str = "TS_DUMP_COMPLETION_BULK_ROOT";
const BULK_CASE: &str = "TS_DUMP_COMPLETION_BULK_CASE";
const BULK_TEST: &str = "engine::bucket_dump_manifest_methods::completion_identity_tests::bulk_frozen_image_retains_uncovered_work_and_reopens_acknowledged_tail";

fn bulk_index_path(root: &Path) -> std::path::PathBuf {
    root.join("indexes").join("shard-1.index.json")
}

fn bulk_engine_at(root: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn bulk_new_key(snapshot: &ShardState) -> String {
    let (start, end) = snapshot.routing_range();
    (0..10_000).map(|index| format!("bulk-live-only-{index}"))
        .find(|key| {
            let bucket = super::super::hashing::block_routing_bucket(key, start, end);
            !snapshot.bucket_index.bucket_map.contains_key(&bucket)
                && !snapshot.strings().contains_key(key.as_str())
        }).unwrap()
}

fn bulk_named_value(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1, command: Command::StringGet { key: key.to_owned() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(response.response, CommandResponse::Bytes { value: Some(value.to_vec()) });
}

fn run_bulk_child(root: &Path, case: &str, stage: &str) {
    let marker = root.join(format!("bulk-{stage}.ok"));
    assert!(!marker.exists());
    let log_path = root.join(format!("bulk-{stage}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", BULK_TEST, "--test-threads=1", "--color", "never"])
        .env(BULK_STAGE, stage).env(BULK_ROOT, root).env(BULK_CASE, case)
        .env_remove("MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE")
        .env_remove("MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS")
        .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(log));
    if stage == "bulk" { command.env("MATRIXARK_BULK_INGEST", "1"); }
    else { command.env_remove("MATRIXARK_BULK_INGEST"); }
    let mut child = command.spawn().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("bulk child {stage} did not finish: {result:?}");
            }
        }
    };
    let mut output = String::new();
    let mut bounded = std::io::Read::take(std::fs::File::open(&log_path).unwrap(), 64 << 10);
    std::io::Read::read_to_string(&mut bounded, &mut output).unwrap();
    assert!(status.success(), "bulk child {case}/{stage}: {status}; {output}");
    assert_eq!(std::fs::read_to_string(marker).unwrap(), stage,
        "an ignored or unmatched child test cannot provide stage evidence");
}

#[test]
fn bulk_frozen_image_retains_uncovered_work_and_reopens_acknowledged_tail() {
    if let Ok(stage) = std::env::var(BULK_STAGE) {
        let root = std::path::PathBuf::from(std::env::var_os(BULK_ROOT).unwrap());
        let case = std::env::var(BULK_CASE).unwrap();
        assert!(case == "overwrite" || case == "new-key");
        if stage == "prepare" {
            assert!(!bulk_ingest_mode());
            let engine = engine_at(&root);
            let prefix = engine.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringSet {
                    key: "bulk-counter-prefix".to_owned(), value: b"prefix".to_vec(),
                },
            });
            assert!(prefix.status.ok, "{:?}", prefix.status);
            let covered = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
            engine.validate_bucket_dump_manifest(&covered).unwrap();
            engine.clear_dumped_bucket_dirty_state(1, &covered);
            assert!(pending(&engine).keys.is_empty());
            engine.flush_shard_index(1);
            let n = engine.wal_store.stats(1).last_sequence;
            assert!(n > 0);
            exact_value(&engine, b"before");
            let response = engine.unload_shard_with(UnloadShardRequest { shard_id: 1 });
            assert!(response.status.ok, "{:?}", response.status);
            let base = std::fs::read(bulk_index_path(&root)).unwrap();
            assert_eq!(decode_index_bytes(&base).unwrap().applied_wal_sequence, Some(n));
        } else {
            let frozen = std::fs::read(bulk_index_path(&root)).unwrap();
            let base = decode_index_bytes(&frozen).unwrap();
            let n = base.applied_wal_sequence.expect("actual durable base anchor");
            assert!(n > 0);
            let new_key = bulk_new_key(&base);
            let key = if case == "overwrite" { KEY } else { new_key.as_str() };
            let value = b"bulk-after-with-new-length";
            if stage == "bulk" {
                assert!(bulk_ingest_mode());
                let engine = bulk_engine_at(&root);
                assert_eq!(engine.wal_store.stats(1).last_sequence, n);
                exact_value(&engine, b"before");
                let initial = pending(&engine);
                bulk_named_value(&engine, "completion-predecessor", b"anchor");
                bulk_named_value(&engine, "bulk-counter-prefix", b"prefix");
                for (index, payload) in [b"bulk-intermediate".as_slice(), value.as_slice()].into_iter().enumerate() {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1, command: Command::StringSet { key: key.to_owned(), value: payload.to_vec() },
                    });
                    assert!(response.status.ok, "{:?}", response.status);
                    assert_eq!(engine.wal_store.stats_without_rescan(1).last_sequence,
                        n.checked_add(index as u64 + 1).unwrap());
                }
                engine.wal_store.flush(1).unwrap();
                assert_eq!(engine.wal_store.stats(1).last_sequence, n.checked_add(2).unwrap());
                bulk_named_value(&engine, key, value);
                assert_eq!(std::fs::read(bulk_index_path(&root)).unwrap(), frozen);
                let before = pending(&engine);
                engine.shards.with_state(1, |state| {
                    assert!(state.unwrap().bucket_index.released_buckets.is_empty());
                });
                assert_eq!(before.keys, vec![key.to_owned()]);
                let (start, end) = base.routing_range();
                let bucket = super::super::hashing::block_routing_bucket(key, start, end);
                let claim = &before.nodes[&bucket];
                assert!(claim.0 && claim.1 > 0 && claim.2 > 0,
                    "real dirty work must retain a positive WAL claim; async IndexLog debt may be zero");
                assert_eq!(claim.2, n.checked_add(1).unwrap(), "the second write must not move the earliest uncovered claim");
                assert_eq!(before.durable_items, initial.durable_items.checked_add(2).unwrap());
                assert_ne!(before.durable_items, before.live_items, "an incorrect item reset must be observable");
                let selected = before.nodes.keys().copied().collect::<Vec<_>>();
                let manifests = || engine.list_bucket_dump_manifests(1).into_iter()
                    .map(|m| (m.manifest_id, m.checksum)).collect::<Vec<_>>();
                let published_before = manifests();
                let refusal = engine.create_bucket_dump_manifest_and_clear(1, selected.clone(), true).unwrap_err();
                assert_eq!(refusal.code, "bulk_dump_uncovered_image");
                let failures = engine.dump_failures.load(std::sync::atomic::Ordering::Relaxed);
                let mut req = request();
                req.selected_dump_buckets = selected;
                req.max_dump_buckets_per_round = before.nodes.len();
                assert!(engine.apply_storage_lifecycle(req).dump_manifest.is_none());
                assert_eq!(engine.dump_failures.load(std::sync::atomic::Ordering::Relaxed), failures + 1);
                assert_eq!(manifests(), published_before, "normal bulk refusal must not publish a mixed manifest");
                assert_eq!(pending(&engine), before, "refusal must retain every actual claim, flag and item count");
                let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
                engine.validate_bucket_dump_manifest(&manifest).unwrap();
                assert_eq!(manifest.wal_sequence, n);
                let image = decode_index_bytes(installable_image(&manifest).unwrap()).unwrap();
                assert_eq!(image.applied_wal_sequence, Some(n));
                assert!(!image.strings().contains_key(new_key.as_str()));
                if case == "new-key" {
                    assert!(!manifest.bucket_ids.contains(&bucket), "live-only coverage must not enter the frozen image manifest");
                }
                assert_eq!(pending(&engine), before, "create-only must retain uncovered work too");
                engine.shards.with_state(1, |state| {
                    assert!(state.unwrap().bucket_index.released_buckets.is_empty());
                });
                drop(engine);
                assert_eq!(std::fs::read(bulk_index_path(&root)).unwrap(), frozen,
                    "no unload or later full checkpoint may refresh the base before physical reopen");
            } else {
                assert_eq!(stage, "reopen");
                assert!(!bulk_ingest_mode());
                let engine = bulk_engine_at(&root);
                assert_eq!(engine.wal_store.stats(1).last_sequence, n.checked_add(2).unwrap());
                bulk_named_value(&engine, key, value);
                if case == "new-key" { exact_value(&engine, b"before"); }
                bulk_named_value(&engine, "completion-predecessor", b"anchor");
                bulk_named_value(&engine, "bulk-counter-prefix", b"prefix");
            }
        }
        std::fs::write(root.join(format!("bulk-{stage}.ok")), stage.as_bytes()).unwrap();
        return;
    }
    let root = tempfile::tempdir().unwrap();
    for case in ["overwrite", "new-key"] {
        let path = root.path().join(case);
        std::fs::create_dir(&path).unwrap();
        for stage in ["prepare", "bulk", "reopen"] { run_bulk_child(&path, case, stage); }
    }
}
