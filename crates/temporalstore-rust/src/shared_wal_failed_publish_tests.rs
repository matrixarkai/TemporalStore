// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a failed publish does to the shared WAL. A publish that fails must neither leave a hole
//! that a later publish seals in (replay refuses a log with a hole before its first index), nor
//! write a record twice because an append that errored had in fact landed.

use super::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const SHARD: ShardId = 9;
const CLUSTER: &str = "cluster-hole";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// The append fails before anything is stored.
    Refuse,
    /// The append is stored, and then the caller is told it failed.
    CommitThenFail,
}

struct Fault {
    key_contains: &'static str,
    kind: Kind,
    remaining: usize,
}

/// A file store that fails appends on demand, and counts the appends that really landed per key.
struct FlakyStore {
    inner: FileObjectStore,
    fault: Mutex<Option<Fault>>,
    landed: Mutex<BTreeMap<String, usize>>,
    fail_reads: AtomicUsize,
}

impl FlakyStore {
    fn new(root: &Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileObjectStore::new(root.join("objects")),
            fault: Mutex::new(None),
            landed: Mutex::new(BTreeMap::new()),
            fail_reads: AtomicUsize::new(0),
        })
    }
    fn inject(&self, key_contains: &'static str, kind: Kind, remaining: usize) {
        *self.fault.lock().unwrap() = Some(Fault {
            key_contains,
            kind,
            remaining,
        });
    }
    fn landed_on(&self, needle: &str) -> usize {
        self.landed
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.contains(needle))
            .map(|(_, count)| *count)
            .sum()
    }
    fn take_fault(&self, key: &str) -> Option<Kind> {
        let mut guard = self.fault.lock().unwrap();
        let fault = guard.as_mut()?;
        if fault.remaining == 0 || !key.contains(fault.key_contains) {
            return None;
        }
        fault.remaining -= 1;
        Some(fault.kind)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, bytes).await
    }
    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        let fault = self.take_fault(key);
        if fault == Some(Kind::Refuse) {
            return Err(ObjectStoreError::Http(format!("{key}: the store answered 503")));
        }
        let receipt = self.inner.append_blob(key, bytes).await?;
        *self.landed.lock().unwrap().entry(key.to_string()).or_default() += 1;
        if fault == Some(Kind::CommitThenFail) {
            return Err(ObjectStoreError::Http(format!(
                "{key}: the store answered 400 -- consensus deadline, write outcome may be committed"
            )));
        }
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
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Bytes, ObjectStoreError> {
        self.inner.get_range(key, offset, length).await
    }
    async fn get_range_from(&self, key: &str, offset: u64) -> Result<Option<Bytes>, ObjectStoreError> {
        if self
            .fail_reads
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |left| left.checked_sub(1))
            .is_ok()
        {
            return Err(ObjectStoreError::Http(format!("{key}: the store answered 500")));
        }
        self.inner.get_range_from(key, offset).await
    }
    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        self.inner.get_many(keys).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list(prefix).await
    }
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_after(prefix, after).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }
    fn uri(&self, key: &str) -> String {
        self.inner.uri(key)
    }
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

fn set(key: &str) -> Command {
    Command::StringSet {
        key: key.to_string(),
        value: format!("value-of-{key}").into_bytes(),
    }
}

async fn replay(
    store: &Arc<FlakyStore>,
    root: &Path,
    role: &str,
    after: u64,
) -> (Result<ReplayReport, SharedStoreReplicationError>, TemporalEngine) {
    let engine = engine(root, role);
    let reader = SharedStoreReplicator::new(CLUSTER, store.clone());
    (reader.replay_wal(SHARD, after, &engine).await, engine)
}

fn has(engine: &TemporalEngine, key: &str) -> bool {
    matches!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringGet { key: key.to_string() },
            })
            .response,
        crate::types::CommandResponse::Bytes { value: Some(_) }
    )
}

/// The behaviour the writer must have, stated with nothing but the writer's public calls: a
/// publish that fails once, between two that succeed, must not turn the log into one that cannot
/// be replayed from its start.
#[tokio::test]
async fn a_failed_publish_does_not_leave_a_hole_in_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let store = FlakyStore::new(dir.path());
    let writer = SharedStoreReplicator::new(CLUSTER, store.clone())
        .storage_writer(SharedStoreStorageMode::Sync, 1);
    writer.write(SHARD, set("k1")).await.unwrap();
    writer.write(SHARD, set("k2")).await.unwrap();
    store.inject(".wal.protobuf.blob", Kind::Refuse, 1);
    let third = writer.write(SHARD, set("k3")).await;
    let fourth = writer.write(SHARD, set("k4")).await;
    let (result, follower) = replay(&store, dir.path(), "follower", 0).await;
    let report = result.unwrap_or_else(|error| {
        panic!(
            "replay from the start was refused: {error} (third write {:?}, fourth write {:?})",
            third.as_ref().map(|r| r.wal_index).map_err(|e| e.to_string()),
            fourth.as_ref().map(|r| r.wal_index).map_err(|e| e.to_string())
        )
    });
    // Whatever the writer told its callers, the log holds exactly the writes it acknowledged.
    for (key, acknowledged) in [("k3", third.is_ok()), ("k4", fourth.is_ok())] {
        assert_eq!(has(&follower, key), acknowledged, "{key}: acknowledged {acknowledged}");
    }
    assert_eq!(report.applied, 2 + usize::from(third.is_ok()) + usize::from(fourth.is_ok()));
}
