// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The shared WAL is a run of SEGMENTS, each a log object and its offset index, and a writer starts
//! the next segment after a quota of appends. These tests count what the object store is asked
//! for, because the claim being made is about requests and extents, not about what a replay
//! returns: a replay over a segmented log and over an unsegmented one return the same thing.

use super::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const SHARD: ShardId = 7;
const CLUSTER: &str = "cluster-seg";

/// What the store was asked, per key.
#[derive(Debug, Default)]
struct Ledger {
    appends: BTreeMap<String, usize>,
    reads: BTreeMap<String, usize>,
    deletes: Vec<String>,
}

/// A file store that records every append, read and delete by key, and can be told to refuse
/// appends. Every `ObjectStore` method is forwarded, including the defaulted ones, because a
/// wrapper that falls back to a default turns an append into an "unsupported" refusal.
struct CountingStore {
    inner: FileObjectStore,
    ledger: Mutex<Ledger>,
    refuse_appends: AtomicUsize,
    refuse_key: Mutex<Option<String>>,
}

impl CountingStore {
    fn new(root: &Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileObjectStore::new(root.join("objects")),
            ledger: Mutex::new(Ledger::default()),
            refuse_appends: AtomicUsize::new(0),
            refuse_key: Mutex::new(None),
        })
    }
    fn read(&self, key: &str) {
        *self
            .ledger
            .lock()
            .unwrap()
            .reads
            .entry(key.to_string())
            .or_default() += 1;
    }
    fn forget(&self) {
        *self.ledger.lock().unwrap() = Ledger::default();
    }
    fn appends(&self) -> BTreeMap<String, usize> {
        self.ledger.lock().unwrap().appends.clone()
    }
    fn reads(&self) -> BTreeMap<String, usize> {
        self.ledger.lock().unwrap().reads.clone()
    }
    fn deletes(&self) -> Vec<String> {
        self.ledger.lock().unwrap().deletes.clone()
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, bytes).await
    }
    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        if self.refuse_key.lock().unwrap().as_deref() == Some(key) {
            return Err(ObjectStoreError::Io(std::io::Error::other(
                "append refused for this key",
            )));
        }
        if self
            .refuse_appends
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ObjectStoreError::Io(std::io::Error::other(
                "append refused by the test",
            )));
        }
        let receipt = self.inner.append_blob(key, bytes).await?;
        *self
            .ledger
            .lock()
            .unwrap()
            .appends
            .entry(key.to_string())
            .or_default() += 1;
        Ok(receipt)
    }
    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        self.inner.put_many(objects).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        self.inner.compare_and_swap(key, expected, new).await
    }
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.read(key);
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        self.read(key);
        self.inner.get_range(key, offset, length).await
    }
    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.read(key);
        self.inner.get_range_from(key, offset).await
    }
    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        for key in keys {
            self.read(key);
        }
        self.inner.get_many(keys).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list(prefix).await
    }
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_after(prefix, after).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.ledger.lock().unwrap().deletes.push(key.to_string());
        self.inner.delete(key).await
    }
    fn uri(&self, key: &str) -> String {
        self.inner.uri(key)
    }
}

fn entry(index: u64) -> SharedStoreWalEntry {
    SharedStoreWalEntry {
        shard_id: SHARD,
        wal_index: index,
        command: Some(Command::StringSet {
            key: format!("key-{index:04}"),
            value: format!("value-{index:04}").into_bytes(),
        }),
        staged_blocks: Vec::new(),
        outcomes: Vec::new(),
    }
}

fn replicator(store: &Arc<CountingStore>, quota: usize) -> SharedStoreReplicator<CountingStore> {
    SharedStoreReplicator::new(CLUSTER, store.clone()).with_wal_segment_appends(quota)
}

fn engine(root: &Path, role: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        4096,
        root.join(format!("{role}-cache")),
        root.join(format!("{role}-blocks")),
        root.join(format!("{role}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

/// One entry per batch, so a batch is one append to each object.
async fn write(replicator: &SharedStoreReplicator<CountingStore>, from: u64, to: u64) {
    for index in from..=to {
        replicator
            .publish_wal_entries_batch(&[entry(index)])
            .await
            .unwrap();
    }
}

fn value_of(engine: &TemporalEngine, index: u64) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: format!("key-{index:04}"),
            },
        })
        .response
    {
        crate::types::CommandResponse::Bytes { value } => value,
        other => panic!("unexpected answer {other:?}"),
    }
}

fn firsts(segments: &[SharedStoreWalSegment]) -> Vec<u64> {
    segments.iter().map(|segment| segment.first_wal_index).collect()
}

fn log_key(r: &SharedStoreReplicator<CountingStore>, first: u64) -> String {
    r.wal_segment_log_key(SHARD, first)
}

fn index_key(r: &SharedStoreReplicator<CountingStore>, first: u64) -> String {
    r.wal_segment_index_key(SHARD, first)
}

#[tokio::test]
async fn the_segment_names_the_quota_and_the_builder_floor_are_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let r = replicator(&store, 4);
    assert_eq!(SHARED_STORE_WAL_SEGMENT_APPENDS_DEFAULT, 256);
    assert_eq!(SHARED_STORE_WAL_SEGMENT_APPENDS_COMPACT, 2048);
    assert_eq!(
        r.wal_segment_log_key(7, 42),
        "cluster-seg/shards/7/shared/wal/seg_00000000000000000042.wal.protobuf.blob"
    );
    assert_eq!(
        r.wal_segment_index_key(7, 42),
        "cluster-seg/shards/7/shared/wal/seg_00000000000000000042.index.protobuf.blob"
    );
    assert_eq!(parse_wal_segment_key(&r.wal_segment_log_key(7, 42)), Some((42, false)));
    assert_eq!(parse_wal_segment_key(&r.wal_segment_index_key(7, 42)), Some((42, true)));
    assert_eq!(parse_wal_segment_key("x/wal/wal_00000000000000000042.protobuf.blob"), None);
    // A quota of zero is one, not "never roll".
    let floor = replicator(&store, 0);
    write(&floor, 1, 3).await;
    assert_eq!(firsts(&floor.wal_segments(SHARD).await.unwrap()), vec![1, 2, 3]);
}

#[tokio::test]
async fn rolls_log_objects_before_an_encoded_append_crosses_the_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let first = encode_wal_proto_frame(&entry(1), WAL_FRAME_FORMAT_DEFAULT).unwrap();
    let second = encode_wal_proto_frame(&entry(2), WAL_FRAME_FORMAT_DEFAULT).unwrap();
    let limit = (first.len() + second.len()) as u64;
    let r = replicator(&store, 256).with_wal_segment_max_bytes(limit);

    let writer = r.clone();
    for i in 1..=3 {
        writer.publish_wal_entry(entry(i)).await.unwrap();
    }
    let segments = r.wal_segments(SHARD).await.unwrap();
    assert_eq!(firsts(&segments), vec![1, 3]);
    assert_eq!(store.appends()[&log_key(&r, 1)], 2);
    assert_eq!(store.appends()[&log_key(&r, 3)], 1);
    assert_eq!(store.get(&log_key(&r, 1)).await.unwrap().len() as u64, limit);
    let follower = engine(dir.path(), "rollover");
    assert_eq!(r.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 3);
    for i in 1..=3 {
        assert_eq!(value_of(&follower, i), Some(format!("value-{i:04}").into_bytes()));
    }
}

#[tokio::test]
async fn divides_oversized_wal_batches_before_they_reach_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let frame_size = encode_wal_proto_frame(&entry(1), WAL_FRAME_FORMAT_DEFAULT).unwrap().len();
    let r = replicator(&store, 256).with_wal_segment_max_bytes(frame_size as u64);
    let entries = [entry(1), entry(2), entry(3), entry(4)];
    let reports = r.publish_wal_entries_batch(&entries).await.unwrap();
    assert_eq!(reports.iter().map(|report| report.wal_index).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    for segment in r.wal_segments(SHARD).await.unwrap() {
        assert!(store.get(&log_key(&r, segment.first_wal_index)).await.unwrap().len() <= frame_size);
    }
    let follower = engine(dir.path(), "split");
    assert_eq!(r.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 4);
}

#[tokio::test]
async fn split_group_commit_reports_only_the_suffix_as_unpublished() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let limit = encode_wal_proto_frame(&entry(1), WAL_FRAME_FORMAT_DEFAULT).unwrap().len() as u64;
    let r = Arc::new(replicator(&store, 256)
        .with_wal_segment_max_bytes(limit)
        .with_publish_retry(1, Duration::ZERO));
    *store.refuse_key.lock().unwrap() = Some(log_key(&r, 2));
    let writer = r.storage_writer(SharedStoreStorageMode::Sync, 1)
        .with_group_commit(true, Duration::from_millis(5));
    let (first, second, third) = tokio::join!(
        writer.write(SHARD, entry(1).command.unwrap()),
        writer.write(SHARD, entry(2).command.unwrap()),
        writer.write(SHARD, entry(3).command.unwrap()),
    );
    assert_eq!(first.unwrap().wal_index, 1);
    assert!(second.is_err() && third.is_err());
    assert_eq!(writer.unresolved_hole(), Some((2, 3)));
    assert_eq!(r.latest_persisted_wal_index(SHARD).await.unwrap(), 1);
}

#[tokio::test]
async fn split_async_flush_retries_only_the_unpublished_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let limit = encode_wal_proto_frame(&entry(1), WAL_FRAME_FORMAT_DEFAULT).unwrap().len() as u64;
    let r = Arc::new(replicator(&store, 256)
        .with_wal_segment_max_bytes(limit)
        .with_publish_retry(1, Duration::ZERO));
    let writer = r.storage_writer(SharedStoreStorageMode::Async, 1);
    for index in 1..=3 {
        assert!(writer.write(SHARD, entry(index).command.unwrap()).await.unwrap().queued);
    }
    *store.refuse_key.lock().unwrap() = Some(log_key(&r, 2));
    assert!(writer.flush_pending(3).await.is_err());
    assert_eq!(writer.queued_len(), 2);
    *store.refuse_key.lock().unwrap() = None;
    assert_eq!(writer.flush_pending(3).await.unwrap().flushed, 2);
    assert_eq!(store.appends()[&log_key(&r, 1)], 1);
    let follower = engine(dir.path(), "partial-retry");
    assert_eq!(r.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 3);
}

#[tokio::test]
async fn sync_request_batch_uses_one_log_and_index_append() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let r = replicator(&store, 256);
    let writer = r.storage_writer(SharedStoreStorageMode::Sync, 1);
    let commands = (1..=3).map(|index| entry(index).command.unwrap()).collect();
    writer.write_sync_batch(SHARD, commands).await.unwrap();

    assert_eq!(store.appends()[&log_key(&r, 1)], 1);
    assert_eq!(store.appends()[&index_key(&r, 1)], 1);
    assert_eq!(writer.unresolved_hole(), None);
    let follower = engine(dir.path(), "request-batch");
    assert_eq!(r.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 3);
    for index in 1..=3 {
        assert_eq!(value_of(&follower, index), Some(format!("value-{index:04}").into_bytes()));
    }
}

/// Every interruption point of a rollover, each asserting the index the writer resumes at and what
/// replay then returns. Only the segmented layout has an interruption point between two objects
/// that are both part of one log.
#[tokio::test]
async fn a_crash_between_two_segments_loses_nothing_acknowledged_and_resumes_at_the_next_index() {
    // (1) Nothing of the new segment exists: the old one filled and the writer died.
    {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        write(&replicator(&store, 4), 1, 4).await;
        let restarted = replicator(&store, 4);
        assert_eq!(restarted.latest_persisted_wal_index(SHARD).await.unwrap(), 4);
        write(&restarted, 5, 5).await;
        let segments = restarted.wal_segments(SHARD).await.unwrap();
        assert_eq!(firsts(&segments), vec![1, 5]);
        let follower = engine(dir.path(), "one");
        let report = replicator(&store, 4).replay_wal(SHARD, 0, &follower).await.unwrap();
        assert_eq!((report.applied, report.last_wal_index), (5, 5));
    }
    // (2) The new segment's log took its first append and its index did not: an orphan log that
    // no index names. It was never acknowledged. The restart resumes from the index, appends the
    // real entry AFTER the orphan bytes, and replay returns the real entry, not the orphan.
    {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let r = replicator(&store, 4);
        write(&r, 1, 4).await;
        let orphan = encode_wal_proto_frame(
            &SharedStoreWalEntry {
                command: Some(Command::StringSet {
                    key: "key-0005".into(),
                    value: b"ORPHAN-NEVER-ACKNOWLEDGED".to_vec(),
                }),
                ..entry(5)
            },
            WAL_FRAME_FORMAT_DEFAULT,
        )
        .unwrap();
        let landed = store
            .append_blob(&log_key(&r, 5), Bytes::from(orphan))
            .await
            .unwrap();
        let restarted = replicator(&store, 4);
        assert_eq!(restarted.latest_persisted_wal_index(SHARD).await.unwrap(), 4);
        write(&restarted, 5, 5).await;
        let real = restarted
            .lookup_wal_offset_metadata(SHARD, 5)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            real.wal_blob_start_offset, landed.end_offset,
            "the real entry lands after the orphan bytes, where the receipt says"
        );
        let follower = engine(dir.path(), "two");
        replicator(&store, 4).replay_wal(SHARD, 0, &follower).await.unwrap();
        assert_eq!(value_of(&follower, 5), Some(b"value-0005".to_vec()));
    }
    // (3) The first batch of the new segment is complete in both objects and the crash came before
    // the acknowledgement: it is present, and replayed as committed.
    {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        write(&replicator(&store, 4), 1, 5).await;
        let restarted = replicator(&store, 4);
        assert_eq!(restarted.latest_persisted_wal_index(SHARD).await.unwrap(), 5);
        let follower = engine(dir.path(), "three");
        assert_eq!(restarted.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 5);
    }
    // (4) The first append of the new segment is refused: nothing is created, nothing is counted,
    // and the retry opens the same segment. The publish itself does not retry here, so the refusal
    // is seen by the caller.
    {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let r = replicator(&store, 4).with_publish_retry(1, Duration::ZERO);
        write(&r, 1, 4).await;
        store.refuse_appends.store(1, AtomicOrdering::SeqCst);
        assert!(r.publish_wal_entries_batch(&[entry(5)]).await.is_err());
        assert_eq!(firsts(&r.wal_segments(SHARD).await.unwrap()), vec![1]);
        write(&r, 5, 5).await;
        assert_eq!(firsts(&r.wal_segments(SHARD).await.unwrap()), vec![1, 5]);
        let follower = engine(dir.path(), "four");
        assert_eq!(r.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 5);
    }
    // A writer that starts always opens a NEW segment, even where the last one has room.
    {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        write(&replicator(&store, 1024), 1, 3).await;
        let restarted = replicator(&store, 1024);
        write(&restarted, 4, 4).await;
        assert_eq!(firsts(&restarted.wal_segments(SHARD).await.unwrap()), vec![1, 4]);
    }
}

#[tokio::test]
async fn replay_crosses_every_segment_boundary_in_order_and_skips_the_segments_below_a_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    write(&w, 1, 17).await;
    assert_eq!(firsts(&w.wal_segments(SHARD).await.unwrap()), vec![1, 5, 9, 13, 17]);

    // From the start: every entry, in order, across four boundaries.
    store.forget();
    let from_zero = engine(dir.path(), "zero");
    let report = replicator(&store, 4).replay_wal(SHARD, 0, &from_zero).await.unwrap();
    assert_eq!((report.applied, report.last_wal_index), (17, 17));
    for index in 1..=17 {
        assert_eq!(value_of(&from_zero, index), Some(format!("value-{index:04}").into_bytes()));
    }
    // A run read never spans two log objects: every read names exactly one key, and all five
    // segments were read.
    let reads = store.reads();
    for first in [1, 5, 9, 13, 17] {
        assert!(reads.contains_key(&index_key(&w, first)), "segment {first} index not read");
        assert!(reads.contains_key(&log_key(&w, first)), "segment {first} log not read");
    }

    // From a checkpoint at 6, inside the second segment: the first segment is never touched.
    store.forget();
    let from_six = engine(dir.path(), "six");
    let report = replicator(&store, 4).replay_wal(SHARD, 6, &from_six).await.unwrap();
    assert_eq!((report.applied, report.last_wal_index), (11, 17));
    assert_eq!(value_of(&from_six, 7), Some(b"value-0007".to_vec()));
    assert_eq!(value_of(&from_six, 6), None, "entry 6 is the checkpoint's, not replay's");
    let reads = store.reads();
    assert!(!reads.contains_key(&index_key(&w, 1)), "a segment below the checkpoint was read");
    assert!(!reads.contains_key(&log_key(&w, 1)), "a segment below the checkpoint was read");
    assert!(reads.contains_key(&index_key(&w, 5)));
}

#[tokio::test]
async fn a_hole_between_segments_is_refused_and_never_replayed_as_a_shorter_log() {
    async fn built() -> (tempfile::TempDir, Arc<CountingStore>, SharedStoreReplicator<CountingStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let w = replicator(&store, 4);
        write(&w, 1, 13).await;
        (dir, store, w)
    }
    async fn replay_from(
        dir: &tempfile::TempDir,
        store: &Arc<CountingStore>,
        after: u64,
        role: &str,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let follower = engine(dir.path(), role);
        replicator(store, 4).replay_wal(SHARD, after, &follower).await
    }

    // A whole middle segment gone: entries 5..=8.
    let (dir, store, w) = built().await;
    store.inner.delete(&index_key(&w, 5)).await.unwrap();
    store.inner.delete(&log_key(&w, 5)).await.unwrap();
    assert!(matches!(
        replay_from(&dir, &store, 0, "a").await,
        Err(SharedStoreReplicationError::ReplayGap { expected: 5, actual: 9 })
    ));
    // The same hole is not an obstacle to a replay that starts above it.
    let report = replay_from(&dir, &store, 8, "b").await.unwrap();
    assert_eq!((report.applied, report.last_wal_index), (5, 13));

    // Only its index gone, the log left behind.
    let (dir, store, w) = built().await;
    store.inner.delete(&index_key(&w, 5)).await.unwrap();
    assert!(matches!(
        replay_from(&dir, &store, 0, "c").await,
        Err(SharedStoreReplicationError::ReplayGap { expected: 5, actual: 9 })
    ));

    // A sealed index that lost its last frames: entries 7 and 8 are missing.
    let (dir, store, w) = built().await;
    let full = store.inner.get(&index_key(&w, 5)).await.unwrap();
    let mut shorter = BTreeMap::new();
    let last = decode_wal_offset_metadata_frames(&full, &log_key(&w, 5), &mut shorter).unwrap();
    store
        .inner
        .put(&index_key(&w, 5), full.slice(..full.len() - last))
        .await
        .unwrap();
    let err = replay_from(&dir, &store, 0, "d").await.unwrap_err();
    assert!(
        matches!(err, SharedStoreReplicationError::ReplayGap { actual: 9, .. }),
        "got {err:?}"
    );

    // Only its log gone: the index names bytes that are not there. That is a read error, not a hole in the numbering
    // and never a success.
    let (dir, store, w) = built().await;
    store.inner.delete(&log_key(&w, 5)).await.unwrap();
    assert!(replay_from(&dir, &store, 0, "e").await.is_err());
}

#[tokio::test]
async fn the_collector_never_deletes_the_live_segment_and_deletes_index_before_log() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    write(&w, 1, 13).await;
    assert_eq!(firsts(&w.wal_segments(SHARD).await.unwrap()), vec![1, 5, 9, 13]);

    // Retain from 9: the first two segments are wholly below it.
    store.forget();
    let report = w.gc_wal_before(SHARD, 9).await.unwrap();
    assert_eq!(report.deleted_wal_objects, 4);
    assert_eq!(
        store.deletes(),
        vec![
            index_key(&w, 1),
            log_key(&w, 1),
            index_key(&w, 5),
            log_key(&w, 5)
        ],
        "each segment's index goes before its log"
    );
    assert_eq!(firsts(&w.wal_segments(SHARD).await.unwrap()), vec![9, 13]);

    // Retain from far past the end: everything is covered, and the highest segment still stays,
    // because a restart finds the last index in it.
    let report = w.gc_wal_before(SHARD, 10_000).await.unwrap();
    assert_eq!(report.deleted_wal_objects, 2);
    assert_eq!(firsts(&w.wal_segments(SHARD).await.unwrap()), vec![13]);
    let restarted = replicator(&store, 4);
    assert_eq!(
        restarted.latest_persisted_wal_index(SHARD).await.unwrap(),
        13,
        "the writer would resume at 1 if the live segment were collected"
    );
    // And it resumes by writing the next index, not by clobbering.
    write(&restarted, 14, 14).await;
    let follower = engine(dir.path(), "live");
    let report = replicator(&store, 4).replay_wal(SHARD, 12, &follower).await.unwrap();
    assert_eq!((report.applied, report.last_wal_index), (2, 14));
}

#[tokio::test]
async fn the_collector_sweeps_orphan_logs_and_keeps_the_one_the_next_append_goes_to() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    write(&w, 1, 9).await;
    // An orphan log below the live segment, as an interrupted delete leaves it: its index is gone.
    store.inner.delete(&index_key(&w, 5)).await.unwrap();
    // And an unindexed log ABOVE the live segment, the one a restart appends to next.
    store
        .inner
        .append_blob(&log_key(&w, 12), Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    let report = w.gc_wal_before(SHARD, 1).await.unwrap();
    assert_eq!(report.deleted_wal_objects, 1, "only the orphan below the live segment");
    let segments = w.wal_segments(SHARD).await.unwrap();
    assert_eq!(firsts(&segments), vec![1, 9, 12]);
    assert!(segments[2].has_log && !segments[2].has_index);
    // A pass that is blocked by a consumer cursor deletes nothing at all.
    w.save_replay_cursor(&SharedStoreReplayCursor {
        shard_id: SHARD,
        last_wal_index: 2,
        last_replay_time_ms: 1,
    })
    .await
    .unwrap();
    store.forget();
    assert!(matches!(
        w.gc_wal_before_cursor_safe(SHARD, 9).await,
        Err(SharedStoreReplicationError::GcBlockedByReplayCursor { .. })
    ));
    assert!(store.deletes().is_empty());
}

/// The claim the whole change exists for, asserted on the requests and not on a replay: no object
/// holds more appends than a segment's quota, and a batch is never split across two segments.
#[tokio::test]
async fn no_object_takes_more_appends_than_the_quota_and_a_batch_never_straddles_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    const QUOTA: usize = 8;
    let w = replicator(&store, QUOTA);
    let mut next = 1u64;
    for _ in 0..100 {
        let batch: Vec<_> = (next..next + 3).map(entry).collect();
        w.publish_wal_entries_batch(&batch).await.unwrap();
        next += 3;
    }
    let appends = store.appends();
    assert!(appends.len() >= 2 * 12, "100 batches at 8 per segment is 13 segments of two objects");
    for (key, count) in &appends {
        assert!(*count <= QUOTA, "{key} took {count} appends, more than the quota {QUOTA}");
    }
    assert_eq!(appends.values().sum::<usize>(), 200, "two appends per batch");
    // Every segment holds whole batches and the same number of them, so its entries are a
    // multiple of three and its first index is where a batch starts.
    let mut per_log: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (index, metadata) in w.load_wal_offset_metadata(SHARD).await.unwrap() {
        per_log.entry(metadata.wal_blob_key).or_default().push(index);
    }
    for (key, indices) in &per_log {
        assert_eq!(indices.len() % 3, 0, "{key} holds a part of a batch");
        assert_eq!((indices[0] - 1) % 3, 0, "{key} does not start where a batch starts");
    }
    let follower = engine(dir.path(), "straddle");
    let report = replicator(&store, QUOTA).replay_wal(SHARD, 0, &follower).await.unwrap();
    assert_eq!((report.applied, report.last_wal_index), (300, 300));
}

/// A start must not read the history. The history here is ten segments; the restart's reads are of
/// the highest index segment alone.
#[tokio::test]
async fn a_restart_reads_the_highest_index_segment_and_not_the_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    write(&w, 1, 40).await;
    assert_eq!(w.wal_segments(SHARD).await.unwrap().len(), 10);
    store.forget();
    let restarted = replicator(&store, 4);
    assert_eq!(restarted.latest_persisted_wal_index(SHARD).await.unwrap(), 40);
    let reads = store.reads();
    assert_eq!(
        reads.keys().cloned().collect::<Vec<_>>(),
        vec![index_key(&w, 37)],
        "a start read something other than the newest index segment"
    );
    // A second call is a tail read of that one segment and nothing more.
    assert_eq!(restarted.latest_persisted_wal_index(SHARD).await.unwrap(), 40);
    assert_eq!(store.reads().keys().count(), 1);
}

#[tokio::test]
async fn a_shard_in_the_pre_segment_layout_is_refused_by_name_and_per_key_objects_are_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    let old = format!("{CLUSTER}/shards/{SHARD}/shared/wal/wal.protobuf.blob");
    store.inner.put(&old, Bytes::from_static(b"x")).await.unwrap();
    let follower = engine(dir.path(), "old");
    for result in [
        w.latest_persisted_wal_index(SHARD).await.map(|_| ()),
        w.replay_wal(SHARD, 0, &follower).await.map(|_| ()),
        w.gc_wal_before(SHARD, 5).await.map(|_| ()),
    ] {
        match result {
            Err(SharedStoreReplicationError::WalLayoutUnsupported { shard_id, key }) => {
                assert_eq!(shard_id, SHARD);
                assert_eq!(key, old);
            }
            other => panic!("the old layout was not refused by name: {other:?}"),
        }
    }
    let message = w.latest_persisted_wal_index(SHARD).await.unwrap_err().to_string();
    assert!(message.contains("pre-segment layout") && message.contains(&old), "{message}");

    // A per-key object beside a segmented log is not part of that log and is not read.
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let w = replicator(&store, 4);
    write(&w, 1, 3).await;
    store
        .inner
        .put(
            &format!("{CLUSTER}/shards/{SHARD}/shared/wal/wal_{:020}.protobuf.blob", 99),
            Bytes::from_static(b"not a log"),
        )
        .await
        .unwrap();
    assert_eq!(w.latest_persisted_wal_index(SHARD).await.unwrap(), 3);
    let follower = engine(dir.path(), "perkey");
    assert_eq!(w.replay_wal(SHARD, 0, &follower).await.unwrap().applied, 3);
}

/// A stale owner is refused before it creates anything, including at a rollover.
#[tokio::test]
async fn a_fenced_stale_owner_cannot_open_a_new_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let a = SharedStoreReplicator::new(CLUSTER, store.clone())
        .with_fence(1, "A")
        .with_wal_segment_appends(1);
    a.acquire_shard_lease(SHARD, 1, "A").await.unwrap();
    a.publish_wal_entries_batch(&[entry(1)]).await.unwrap();
    let b = SharedStoreReplicator::new(CLUSTER, store.clone()).with_fence(2, "B");
    b.acquire_shard_lease(SHARD, 2, "B").await.unwrap();
    // A's quota is spent, so its next batch would open segment 2.
    let refused = a.publish_wal_entries_batch(&[entry(2)]).await;
    assert!(refused.is_err());
    assert_eq!(firsts(&a.wal_segments(SHARD).await.unwrap()), vec![1]);
}
