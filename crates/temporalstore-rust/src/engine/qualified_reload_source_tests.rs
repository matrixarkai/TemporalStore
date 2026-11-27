// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc::channel;
use std::time::Duration;

const A: (u32, u32) = (0, 63);
const B: (u32, u32) = (64, 127);
const WAIT: Duration = Duration::from_secs(20);

fn request(range: (u32, u32), version: u64) -> crate::control::LoadShardRequest {
    crate::control::LoadShardRequest {
        shard_id: 1, table_name: "reload-authority".into(), shard_uri: "local://reload/1".into(),
        start_routing_bucket: range.0, end_routing_bucket: range.1,
        readonly: false, load_version: version, local_node_id: Some(1),
    }
}

fn open(root: &Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    )
}

fn fresh(root: &Path) -> TemporalEngine {
    let engine = open(root);
    assert!(engine.load_shard_with(request(A, 5)).status.ok);
    assert_eq!(source(&engine), Some(A));
    engine
}

fn source(engine: &TemporalEngine) -> Option<(u32, u32)> {
    engine.shards.with_state(1, |state| state.unwrap().source_owner_range)
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let result = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(result.status.ok, "{:?}", result.status);
    result.response
}

fn exact(engine: &TemporalEngine, key: &str, value: &[u8]) {
    assert_eq!(run(engine, Command::StringGet { key: key.into() }),
        CommandResponse::Bytes { value: Some(value.to_vec()) });
}

#[test]
fn range_reload_invalidates_before_actual_new_range_writes_and_stays_unknown_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = fresh(root.path());
    run(&engine, Command::StringSet { key: "reload-before".into(), value: b"before".to_vec() });
    let old = engine.shards.handle(1).unwrap();
    assert!(engine.reload_shard_with(request(B, 6)).status.ok);
    assert!(old.same_incarnation(&engine.shards.handle(1).unwrap()));
    assert_eq!(source(&engine), None);
    assert_eq!(engine.shard_routing_range(1), B);
    // Metadata reload does not certify that old physical filing moved to B.
    old.with_read(|state| assert_eq!(state.unwrap().routing_range(), A));
    let key = "reload-after";
    run(&engine, Command::StringSet { key: key.into(), value: b"after".to_vec() });
    let bucket = engine.routing_bucket_for_key(1, key);
    assert!((B.0..=B.1).contains(&bucket));
    old.with_read(|state| {
        let state = state.unwrap();
        assert_eq!(state.source_owner_range, None);
        assert!(state.dirty_objects.is_marked_at(key, bucket));
        assert!(state.bucket_index.bucket_map.get(&bucket).unwrap().dirty());
    });
    exact(&engine, "reload-before", b"before");
    exact(&engine, key, b"after");
    assert!(engine.unload_shard_with(UnloadShardRequest { shard_id: 1 }).status.ok);
    let bytes = fs::read(engine.index_path(1)).unwrap();
    assert_eq!(decode_index_bytes(&bytes).unwrap().source_owner_range, None);
    drop(engine);
    let reopened = open(root.path());
    // Existing durable range policy is unchanged: a metadata reload did not rewrite its stamp.
    let refused = reopened.load_shard_with(request(B, 7));
    assert!(!refused.status.ok);
    assert_eq!(refused.status.code, super::super::routing_range_stamp::ROUTING_RANGE_MISMATCH);
    assert!(reopened.shards.handle(1).is_none());
    assert!(reopened.load_shard_with(request(A, 7)).status.ok);
    assert_eq!(source(&reopened), None, "a real old image cannot be minted as a fresh source");
    exact(&reopened, "reload-before", b"before");
    exact(&reopened, key, b"after");
}

#[test]
fn unchanged_range_and_rejected_reload_do_not_wait_for_owner_or_revoke_authority() {
    let root = tempfile::tempdir().unwrap();
    let engine = fresh(root.path());
    run(&engine, Command::StringSet { key: "reload-kept".into(), value: b"kept".to_vec() });
    let owner = engine.shards.handle(1).unwrap();
    for (req, expected_ok) in [(request(A, 6), true), (request(B, 5), false)] {
        let worker_engine = engine.clone();
        let (sender, receiver) = channel();
        let (received, worker) = owner.with_write(|state| {
            let state = state.unwrap();
            assert_eq!(state.source_owner_range, Some(A));
            let worker = std::thread::spawn(move || {
                let result = worker_engine.reload_shard_with(req);
                sender.send(result).unwrap();
            });
            (receiver.recv_timeout(WAIT), worker)
        });
        worker.join().unwrap();
        let result = received.expect("unchanged or already-rejected reload waited for an owner");
        assert_eq!(result.status.ok, expected_ok);
        if !expected_ok { assert_eq!(result.status.code, "stale_load_version"); }
        assert!(owner.same_incarnation(&engine.shards.handle(1).unwrap()));
        assert_eq!(source(&engine), Some(A));
        assert_eq!(engine.shard_routing_range(1), A);
        let infos = engine.infos.read().unwrap();
        let info = infos.get(&1).unwrap();
        assert_eq!(info.load_version, 6);
        assert!(!info.readonly);
    }
    exact(&engine, "reload-kept", b"kept");
}

#[derive(Debug, PartialEq, Eq)]
struct Pending {
    generation: u64, wal: u64, index: u64, dirty: bool, deleted: bool,
    debt: u64, deleted_ids: BTreeSet<u64>, object_ids: BTreeSet<u64>,
}

fn pending(engine: &TemporalEngine, bucket: u32) -> Pending {
    engine.shards.with_state(1, |state| {
        let state = state.unwrap();
        let node = state.bucket_index.bucket_map.get(&bucket).unwrap();
        Pending {
            generation: node.dirty_generation, wal: node.first_dirty_wal_sequence,
            index: node.first_dirty_index_log_sequence, dirty: node.dirty(), deleted: node.deleted(),
            debt: state.dirty_objects.bucket_count(bucket),
            deleted_ids: node.deleted_object_index.iter().copied().collect(),
            object_ids: node.object_index.iter().copied().collect(),
        }
    })
}

#[test]
fn range_reload_while_publication_is_parked_keeps_the_prepared_empty_owner_pending() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) { BEFORE_DUMP_DIRTY_COMPLETION.with(|slot| { slot.borrow_mut().take(); }); }
    }
    let root = tempfile::tempdir().unwrap();
    let engine = fresh(root.path());
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    run(&engine, Command::StringSet { key: "reload-predecessor".into(), value: b"anchor".to_vec() });
    let prime = engine.create_bucket_dump_manifest_and_clear(
        1, vec![engine.routing_bucket_for_key(1, "reload-predecessor")], true,
    ).unwrap();
    engine.validate_bucket_dump_manifest(&prime).unwrap();
    assert!(prime.wal_sequence > 0); // Async IndexLog debt is observed, not fabricated.
    let predecessor_bucket = engine.routing_bucket_for_key(1, "reload-predecessor");
    let expired = (0..128).map(|n| format!("reload-expired-{n}"))
        .find(|key| engine.routing_bucket_for_key(1, key) != predecessor_bucket).unwrap();
    let key = expired.as_str();
    {
        let _clock = ReplayClockGuard::enter(Some(1000));
        run(&engine, Command::StringSetEx { key: key.into(), value: b"expired".to_vec(), ttl_ms: 10 });
    }
    {
        let _clock = ReplayClockGuard::enter(Some(1011));
        assert_eq!(run(&engine, Command::CommonTtl { key: key.into() }), CommandResponse::Integer { value: -2 });
    }
    let bucket = engine.routing_bucket_for_key(1, key);
    let before = pending(&engine, bucket);
    assert!(before.dirty && before.deleted && before.wal > 0 && !before.deleted_ids.is_empty());
    engine.shards.with_state(1, |state| {
        let state = state.unwrap();let node = state.bucket_index.bucket_map.get(&bucket).unwrap();
        assert!(node.block_index.is_empty() && node.tombstone_elements.is_empty());
        assert!(!state.strings().contains_key(key) && !state.expires_at_ms().contains_key(key));
    });
    let worker_engine = engine.clone();
    let (ready_tx, ready_rx) = channel();let (resume_tx, resume_rx) = channel();
    let worker = std::thread::spawn(move || {
        let _reset = Reset;
        BEFORE_DUMP_DIRTY_COMPLETION.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Box::new(move || {
                ready_tx.send(()).unwrap();resume_rx.recv_timeout(WAIT).unwrap();
            }));
        });
        worker_engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true)
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(pending(&engine, bucket), before);
    assert!(engine.reload_shard_with(request(B, 6)).status.ok);
    assert_eq!(source(&engine), None);
    assert_eq!(pending(&engine, bucket), before, "reload must invalidate only source authority");
    resume_tx.send(()).unwrap();
    let manifest = worker.join().unwrap().unwrap();
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    assert!(!decode_index_bytes(&manifest.index_bytes).unwrap().bucket_index.bucket_map.contains_key(&bucket),
        "the parked capture must actually have prepared normalization");
    assert_eq!(pending(&engine, bucket), before, "refused proposal must not fall through to ordinary clear");
    exact(&engine, "reload-predecessor", b"anchor");
}
