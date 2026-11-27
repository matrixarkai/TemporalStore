// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{CommandResponse, FeaturePoint, SequenceFeatureRow};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

type Cases = Vec<(Command, CommandResponse)>;

fn engine(root: &std::path::Path, index: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        1 << 20, root.join(format!("{index}-cache")), root.join("blocks"),
        root.join(index), BlockStoreOptions::default(), false,
    )
}

fn exec(engine: &TemporalEngine, shard: u64, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: shard, command });
    assert!(response.status.ok, "{response:?}");
    response.response
}

fn async_shard(engine: &TemporalEngine, id: u64) {
    engine.load_shard(id);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: id, config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
}

fn seed(engine: &TemporalEngine) -> (Cases, std::collections::BTreeSet<u64>) {
    let bytes = (0..1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let point = FeaturePoint { timestamp_ms: 17, value: bytes.clone() };
    let row = SequenceFeatureRow { timestamp_ms: 19, gid: 7, action_type: 2, duration: 11, author_id: 13 };
    let writes = vec![
        Command::StringSet { key: "old-string".into(), value: bytes.clone() },
        Command::HashSet { key: "old-hash".into(), field: "field".into(), value: bytes.clone() },
        Command::SetAdd { key: "old-set".into(), member: bytes.clone() },
        Command::ListPush { key: "old-list".into(), member: bytes.clone(), left: false },
        Command::ZSetAdd { key: "old-zset".into(), member: bytes.clone(), score: 3.0 },
        Command::FeatureAppend { key: "old-feature".into(), points: vec![point.clone()] },
        Command::SequenceAdd { key: "old-sequence".into(), rows: vec![row.clone()] },
    ];
    let mut sequences = std::collections::BTreeSet::new();
    for command in writes {
        exec(engine, 1, command);
        sequences.insert(engine.wal_store.stats_without_rescan(1).last_sequence);
    }
    assert_eq!(sequences.len(), 7);
    engine.wal_store.flush(1).unwrap();
    let cases = vec![
        (Command::StringGet { key: "old-string".into() }, CommandResponse::Bytes { value: Some(bytes.clone()) }),
        (Command::HashGet { key: "old-hash".into(), field: "field".into() }, CommandResponse::Bytes { value: Some(bytes.clone()) }),
        (Command::SetMembers { key: "old-set".into() }, CommandResponse::Members { members: vec![bytes.clone()] }),
        (Command::ListRange { key: "old-list".into(), start: 0, stop: -1 }, CommandResponse::Members { members: vec![bytes.clone()] }),
        (Command::ZSetScore { key: "old-zset".into(), member: bytes }, CommandResponse::Bytes { value: Some(b"3".to_vec()) }),
        (Command::FeatureQuery { key: "old-feature".into(), start_ms: 0, end_ms: 100, count: None }, CommandResponse::FeaturePoints { points: vec![point] }),
        (Command::SequenceQuery { key: "old-sequence".into(), start_ms: 0, end_ms: 100, count: 8, filters: Vec::new() }, CommandResponse::SequenceRows { rows: vec![row] }),
    ];
    check(engine, &cases);
    // Require a genuine persisted-placement record using the complete-record decoder,
    // not only the indexed-directory representation or a successful unrelated fallback.
    let (records, truncated) = engine.wal_store.scan_decoded(1, 0, u64::MAX, 1 << 20).unwrap();
    assert!(!truncated);
    let placements = engine.shards.with_state(1, |state| state.unwrap().wal_resident_blocks
        .values().map(|p| p.sequence).collect::<std::collections::BTreeSet<_>>());
    assert!(sequences.is_subset(&placements), "all seven producer families must carry actual live placements");
    let mut legacy = false;
    for (id, record) in records {
        if !placements.contains(&record.sequence) { continue; }
        if let Some(block) = record.staged_blocks.first() {
            if let Some(crate::wal::CarriedBlockRead::Record(_)) = engine.wal_store.read_carried_block_at_log_id(
                1, id, record.sequence, 0, block.object_id, block.element_name.as_deref(),
            ).unwrap() { legacy = true; }
        }
    }
    assert!(legacy, "must produce a real non-indexed carried record");
    (cases, sequences)
}

fn check(engine: &TemporalEngine, cases: &Cases) {
    engine.cache().invalidate_shard(1).unwrap();
    for (command, expected) in cases {
        assert_eq!(&exec(engine, 1, command.clone()), expected, "{command:?}");
    }
}

fn suffix(engine: &TemporalEngine) {
    exec(engine, 1, Command::StringSet { key: "suffix".into(), value: b"after-image".to_vec() });
    engine.wal_store.flush(1).unwrap();
}

fn check_tail(engine: &TemporalEngine) {
    for (key, bytes) in [("suffix", b"after-image".as_slice()), ("during", b"during-read".as_slice())] {
        assert_eq!(exec(engine, 1, Command::StringGet { key: key.into() }),
            CommandResponse::Bytes { value: Some(bytes.to_vec()) });
    }
}

fn paused(
    engine: Arc<TemporalEngine>,
    sequences: std::collections::BTreeSet<u64>,
    run: impl FnOnce(&TemporalEngine) + Send + 'static,
) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let events = Arc::new(Mutex::new(Vec::<(bool, Vec<(u64, u64)>)>::new()));
    let observed = events.clone();
    let loading = engine.clone();
    let worker = std::thread::spawn(move || {
        let mut first = Some(resume_rx);
        crate::wal::with_restore_batch_hook_for_test(move |shard, prepared, requests| {
            assert_eq!(shard, 1);
            observed.lock().unwrap().push((prepared, requests.to_vec()));
            if prepared {
                if let Some(resume) = first.take() {
                    ready_tx.send(()).unwrap();
                    resume.recv_timeout(Duration::from_secs(20)).unwrap();
                }
            }
        }, || run(&loading));
    });
    let ready = ready_rx.recv_timeout(Duration::from_secs(20));
    if ready.is_err() {
        let _ = resume_tx.send(());
        worker.join().unwrap();
        panic!("engine restore never entered the admitted productive read");
    }
    let (progress_tx, progress_rx) = mpsc::channel();
    let writing = engine.clone();
    let progress = std::thread::spawn(move || {
        let peer = writing.execute(ExecuteRequest { shard_id: 2,
            command: Command::StringSet { key: "peer".into(), value: b"peer-progress".to_vec() } });
        let fenced = writing.execute(ExecuteRequest { shard_id: 1,
            command: Command::StringSet { key: "must-refuse".into(), value: b"not-serving".to_vec() } });
        let raw = writing.wal_store.append_with_sync(1,
            Command::StringSet { key: "during".into(), value: b"during-read".to_vec() }, true);
        progress_tx.send((peer.status.ok, fenced.status.ok, raw.map(|record| record.sequence))).unwrap();
    });
    let before_resume = progress_rx.recv_timeout(Duration::from_secs(20));
    let _ = resume_tx.send(());
    progress.join().unwrap();
    worker.join().unwrap();
    let (peer, improperly_served, appended) = before_resume.expect("appends must progress before restore resumes");
    assert!(peer);
    assert!(!improperly_served, "same-shard serving must remain fenced during recovery");
    assert!(appended.unwrap() > *sequences.iter().next_back().unwrap());
    let events = events.lock().unwrap();
    let completed = events.iter().filter(|(prepared, _)| !prepared).flat_map(|(_, requests)| requests.iter().map(|(_, seq)| *seq)).collect::<std::collections::BTreeSet<_>>();
    assert!(sequences.is_subset(&completed), "every seeded model must traverse successful admitted I/O: {events:?}");
    assert_eq!(events.iter().filter(|(prepared, _)| *prepared).count(), events.iter().filter(|(prepared, _)| !prepared).count());
    assert_eq!(exec(&engine, 2, Command::StringGet { key: "peer".into() }), CommandResponse::Bytes { value: Some(b"peer-progress".to_vec()) });
    assert_eq!(exec(&engine, 1, Command::StringGet { key: "must-refuse".into() }), CommandResponse::Bytes { value: None });
}

#[test]
fn cold_engine_rehydration_admits_every_model_and_replays_the_real_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "indexes");
    async_shard(&source, 1);
    let (cases, sequences) = seed(&source);
    source.flush_shard_index(1);
    let image = decode_index_bytes(&std::fs::read(source.index_path(1)).unwrap()).unwrap();
    let persisted = image.wal_resident_blocks.values().map(|p| p.sequence).collect::<std::collections::BTreeSet<_>>();
    assert!(sequences.is_subset(&persisted));
    suffix(&source);
    drop(source);
    let restored = Arc::new(engine(dir.path(), "indexes"));
    async_shard(&restored, 2);
    paused(restored.clone(), sequences, |engine| engine.load_shard(1));
    check(&restored, &cases);
    check_tail(&restored);
    drop(restored);
    let reopened = engine(dir.path(), "indexes");
    reopened.load_shard(1);
    check(&reopened, &cases);
    check_tail(&reopened);
}

#[test]
fn loaded_manifest_rehydration_admits_every_model_and_replays_the_real_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "source");
    async_shard(&source, 1);
    let (cases, sequences) = seed(&source);
    let manifest = source.create_bucket_dump_manifest(1, []).unwrap();
    assert!(source.wal_resident_block_count(1) > 0);
    // Keep this real loaded owner's registrations for preflight. Append a genuine
    // suffix without advancing its index, rather than inventing an install frontier.
    let suffix = source.wal_store.append_with_sync(1, Command::StringSet {
        key: "suffix".into(), value: b"after-image".to_vec(),
    }, true).unwrap();
    assert!(suffix.sequence > manifest.wal_sequence);
    async_shard(&source, 2);
    let target = Arc::new(source);
    paused(target.clone(), sequences, move |engine| engine.install_bucket_dump_manifest(&manifest).unwrap());
    check(&target, &cases);
    check_tail(&target);
    target.flush_shard_index(1);
    drop(target);
    let reopened = engine(dir.path(), "source");
    reopened.load_shard(1);
    check(&reopened, &cases);
    check_tail(&reopened);
}

fn readonly_load(engine: &TemporalEngine) {
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1, load_version: 0, local_node_id: None, shard_uri: String::new(),
        start_routing_bucket: 0, end_routing_bucket: u32::MAX,
        readonly: true, table_name: String::new(),
    });
    assert!(response.status.ok, "{response:?}");
}

#[test]
fn readonly_cold_load_does_not_adopt_the_intended_writers_lifetime_flock() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "indexes");
    async_shard(&source, 1);
    let (cases, _) = seed(&source);
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    let anchor = source.wal_store.stats_without_rescan(1).last_sequence;
    let image = decode_index_bytes(&std::fs::read(source.index_path(1)).unwrap()).unwrap();
    assert_eq!(image.applied_wal_sequence, Some(anchor), "exercise no-tail readonly recovery, not an adoption path");
    assert!(!image.wal_resident_blocks.is_empty());
    drop(source);
    let observer = engine(dir.path(), "indexes");
    let admitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = admitted.clone();
    crate::wal::with_restore_batch_hook_for_test(move |_, _, _| {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }, || readonly_load(&observer));
    assert_eq!(admitted.load(std::sync::atomic::Ordering::SeqCst), 0);
    check(&observer, &cases);
    let rejected = observer.execute(ExecuteRequest { shard_id: 1,
        command: Command::StringSet { key: "forbidden".into(), value: b"readonly".to_vec() } });
    assert!(!rejected.status.ok);
    let intended = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
    let written = intended.append_with_sync(1, Command::StringSet {
        key: "writer-after-observer".into(), value: b"real-writer-tail".to_vec(),
    }, true).expect("readonly-first restore must leave lifetime append ownership available");
    assert!(written.sequence > anchor);
    check(&observer, &cases);
    let competing = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
    let refusal = competing.append_with_sync(1, Command::StringSet {
        key: "must-not-append".into(), value: Vec::new(),
    }, true).unwrap_err();
    assert!(matches!(refusal, crate::wal::WriteAheadLogError::Io(ref error)
        if error.kind() == std::io::ErrorKind::WouldBlock));
    drop(competing);
    drop(intended);
    drop(observer);
    // Drain the actual writer, then adopt/replay its real command in a writable
    // engine and publish the resulting base through the normal checkpoint API.
    let adopting = engine(dir.path(), "indexes");
    adopting.load_shard(1);
    check(&adopting, &cases);
    assert_eq!(exec(&adopting, 1, Command::StringGet { key: "writer-after-observer".into() }),
        CommandResponse::Bytes { value: Some(b"real-writer-tail".to_vec()) });
    adopting.flush_shard_index(1);
    adopting.wal_store.flush(1).unwrap();
    drop(adopting);
    let reopened = engine(dir.path(), "indexes");
    readonly_load(&reopened);
    check(&reopened, &cases);
    assert_eq!(exec(&reopened, 1, Command::StringGet { key: "writer-after-observer".into() }),
        CommandResponse::Bytes { value: Some(b"real-writer-tail".to_vec()) });
    let next_writer = LocalWriteAheadLogStore::new(dir.path().join("indexes/wals"));
    let next = next_writer.append_with_sync(1, Command::StringSet {
        key: "after-drain-and-readonly-reload".into(), value: b"still-available".to_vec(),
    }, true).unwrap();
    assert!(next.sequence > written.sequence);
}
