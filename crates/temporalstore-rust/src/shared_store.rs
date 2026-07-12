use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalstore_snapshot::object_store::{ObjectStore, ObjectStoreError};
use thiserror::Error;
use tokio::task::JoinSet;

use crate::block_store::{BlockStoreError, LocalBlockStore};
use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest, ShardId, Status};

#[derive(Debug, Error)]
pub enum SharedStoreReplicationError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("no shared-store checkpoint found for shard {0}")]
    CheckpointNotFound(ShardId),
    #[error("replicated command failed at WAL index {oplog_index}: {status:?}")]
    ApplyFailed { oplog_index: u64, status: Status },
    #[error("WAL replay gap: expected index {expected}, got {actual}")]
    ReplayGap { expected: u64, actual: u64 },
    #[error(
        "shared-store GC would remove WAL entry needed by replay cursor {cursor_oplog_index} before retain {retain_from_oplog_index}"
    )]
    GcBlockedByReplayCursor {
        cursor_oplog_index: u64,
        retain_from_oplog_index: u64,
    },
    #[error(
        "shared-store checkpoint GC would remove checkpoint {checkpoint_id} at WAL index {checkpoint_oplog_index} needed by replay cursor {cursor_oplog_index}"
    )]
    CheckpointGcBlockedByReplayCursor {
        cursor_oplog_index: u64,
        checkpoint_oplog_index: u64,
        checkpoint_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreOplogEntry {
    pub shard_id: ShardId,
    pub oplog_index: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreOplogObject {
    pub entry: SharedStoreOplogEntry,
    pub entry_byte_size: u64,
    pub entry_sha256: String,
}

pub type SharedStoreWalEntry = SharedStoreOplogEntry;
pub type SharedStoreWalObject = SharedStoreOplogObject;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStorePageSegment {
    pub page_segment_id: u64,
    pub key: String,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreCheckpointManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub checkpoint_id: String,
    pub checkpoint_oplog_index: u64,
    pub created_at_ms: u64,
    pub index_key: String,
    pub index_byte_size: u64,
    pub index_sha256: String,
    pub page_segments: Vec<SharedStorePageSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreReplayCursor {
    pub shard_id: ShardId,
    pub last_oplog_index: u64,
    pub last_replay_time_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SharedStoreStorageMode {
    Sync,
    Async,
}

impl Default for SharedStoreStorageMode {
    fn default() -> Self {
        Self::Async
    }
}

impl SharedStoreStorageMode {
    pub fn from_sync_flag(sync: bool) -> Self {
        if sync {
            Self::Sync
        } else {
            Self::Async
        }
    }

    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWriteReport {
    pub oplog_index: u64,
    pub published: bool,
    pub queued: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreFlushReport {
    pub flushed: usize,
    pub remaining: usize,
    pub last_oplog_index: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreGcReport {
    pub shard_id: ShardId,
    pub deleted_oplog_objects: usize,
    pub deleted_checkpoints: usize,
    pub deleted_checkpoint_objects: usize,
    pub retained_checkpoint_ids: Vec<String>,
    #[serde(default)]
    pub retained_for_cursor_oplog_index: Option<u64>,
    #[serde(default)]
    pub retained_for_cursor_checkpoint_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreRetryPolicy {
    pub max_attempts: usize,
    pub backoff_ms: u64,
}

impl Default for SharedStoreRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_ms: 0,
        }
    }
}

#[derive(Debug)]
pub struct SharedStoreReplicator<O> {
    cluster_id: String,
    object_store: Arc<O>,
    retry_policy: SharedStoreRetryPolicy,
    transfer_concurrency: usize,
}

impl<O> Clone for SharedStoreReplicator<O> {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            object_store: Arc::clone(&self.object_store),
            retry_policy: self.retry_policy,
            transfer_concurrency: self.transfer_concurrency,
        }
    }
}

#[derive(Debug)]
pub struct SharedStoreStorageWriter<O> {
    replicator: SharedStoreReplicator<O>,
    mode: SharedStoreStorageMode,
    next_oplog_index: AtomicU64,
    pending: Mutex<VecDeque<SharedStoreOplogEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayReport {
    pub applied: usize,
    pub last_oplog_index: u64,
}

impl<O> SharedStoreReplicator<O>
where
    O: ObjectStore + 'static,
{
    pub fn new(cluster_id: impl Into<String>, object_store: Arc<O>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy::default(),
            transfer_concurrency: default_transfer_concurrency(),
        }
    }

    pub fn with_retry_policy(
        cluster_id: impl Into<String>,
        object_store: Arc<O>,
        retry_policy: SharedStoreRetryPolicy,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy {
                max_attempts: retry_policy.max_attempts.max(1),
                backoff_ms: retry_policy.backoff_ms,
            },
            transfer_concurrency: default_transfer_concurrency(),
        }
    }

    pub fn with_transfer_concurrency(mut self, transfer_concurrency: usize) -> Self {
        self.transfer_concurrency = transfer_concurrency.max(1);
        self
    }

    pub async fn publish_oplog_entry(
        &self,
        entry: SharedStoreOplogEntry,
    ) -> Result<(), SharedStoreReplicationError> {
        self.publish_wal_entry(entry).await
    }

    pub async fn publish_wal_entry(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.oplog_key(entry.shard_id, entry.oplog_index);
        let entry_bytes = serde_json::to_vec(&entry)?;
        let object = SharedStoreWalObject {
            entry,
            entry_byte_size: entry_bytes.len() as u64,
            entry_sha256: sha256_hex(&entry_bytes),
        };
        self.put_with_retry(&key, Bytes::from(serde_json::to_vec(&object)?))
            .await?;
        Ok(())
    }

    pub fn storage_writer(
        &self,
        mode: SharedStoreStorageMode,
        next_oplog_index: u64,
    ) -> SharedStoreStorageWriter<O> {
        SharedStoreStorageWriter {
            replicator: self.clone(),
            mode,
            next_oplog_index: AtomicU64::new(next_oplog_index.max(1)),
            pending: Mutex::default(),
        }
    }

    pub fn default_storage_writer(&self, next_oplog_index: u64) -> SharedStoreStorageWriter<O> {
        self.storage_writer(SharedStoreStorageMode::default(), next_oplog_index)
    }

    pub async fn publish_index(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<(), SharedStoreReplicationError> {
        self.object_store
            .put(
                &self.index_key(shard_id),
                Bytes::from(engine.export_index_bytes(shard_id)?),
            )
            .await?;
        Ok(())
    }

    pub async fn publish_page_segments(
        &self,
        shard_id: ShardId,
        block_store: &LocalBlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let mut objects = Vec::new();
        let mut published = Vec::new();
        for page_segment_id in block_store.segment_ids()? {
            let key = self.page_segment_key(shard_id, page_segment_id);
            objects.push((key, Bytes::from(block_store.read_segment(page_segment_id)?)));
            published.push(page_segment_id);
        }
        self.put_objects_concurrent(objects).await?;
        Ok(published)
    }

    pub async fn publish_checkpoint(
        &self,
        shard_id: ShardId,
        checkpoint_oplog_index: u64,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let prefix = self.checkpoint_prefix(shard_id, &checkpoint_id);
        let index_key = format!("{prefix}index/shard.index.json");
        let index = engine.export_index_bytes(shard_id)?;
        self.object_store
            .put(&index_key, Bytes::from(index.clone()))
            .await?;

        let mut page_segments = Vec::new();
        let mut objects = Vec::new();
        for page_segment_id in block_store.segment_ids()? {
            let bytes = block_store.read_segment(page_segment_id)?;
            let key = format!("{prefix}page_segments/page_segment_{page_segment_id:020}.seg");
            page_segments.push(SharedStorePageSegment {
                page_segment_id,
                key: key.clone(),
                byte_size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
            });
            objects.push((key, Bytes::from(bytes)));
        }
        self.put_objects_concurrent(objects).await?;

        let manifest = SharedStoreCheckpointManifest {
            cluster_id: self.cluster_id.clone(),
            shard_id,
            checkpoint_id,
            checkpoint_oplog_index,
            created_at_ms: now_ms(),
            index_key,
            index_byte_size: index.len() as u64,
            index_sha256: sha256_hex(&index),
            page_segments,
        };
        self.object_store
            .put(
                &self.checkpoint_manifest_key(shard_id, &manifest.checkpoint_id),
                Bytes::from(serde_json::to_vec_pretty(&manifest)?),
            )
            .await?;
        Ok(manifest)
    }

    pub async fn restore_index_and_pages(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let index = self.object_store.get(&self.index_key(shard_id)).await?;
        engine.install_index_bytes(shard_id, &index)?;

        let prefix = self.page_segment_prefix(shard_id);
        let mut page_keys = self
            .object_store
            .list(&prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                parse_page_segment_id(&key).map(|page_segment_id| (page_segment_id, key))
            })
            .collect::<Vec<_>>();
        page_keys.sort_by_key(|(page_segment_id, _)| *page_segment_id);
        let page_segment_ids = page_keys
            .iter()
            .map(|(page_segment_id, _)| *page_segment_id)
            .collect::<Vec<_>>();
        let page_bytes = self
            .get_objects_concurrent(page_keys.into_iter().map(|(_, key)| key).collect())
            .await?;
        let mut restored = Vec::new();
        for (page_segment_id, (_, bytes)) in page_segment_ids.into_iter().zip(page_bytes) {
            block_store.install_segment(page_segment_id, &bytes)?;
            restored.push(page_segment_id);
        }
        Ok(restored)
    }

    pub async fn list_checkpoints(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SharedStoreCheckpointManifest>, SharedStoreReplicationError> {
        let manifest_keys = self
            .object_store
            .list(&self.checkpoints_prefix(shard_id))
            .await?
            .into_iter()
            .filter(|key| key.ends_with("/manifest.json"))
            .collect::<Vec<_>>();
        let mut manifests = Vec::new();
        for (_, bytes) in self.get_objects_concurrent(manifest_keys).await? {
            manifests.push(serde_json::from_slice(&bytes)?);
        }
        manifests.sort_by_key(|manifest: &SharedStoreCheckpointManifest| {
            (manifest.checkpoint_oplog_index, manifest.created_at_ms)
        });
        Ok(manifests)
    }

    pub async fn restore_checkpoint(
        &self,
        manifest: &SharedStoreCheckpointManifest,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<(), SharedStoreReplicationError> {
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        engine.install_index_bytes(manifest.shard_id, &index)?;

        let mut segments = manifest.page_segments.clone();
        segments.sort_by_key(|segment| segment.page_segment_id);
        let segment_bytes = self
            .get_objects_concurrent(segments.iter().map(|segment| segment.key.clone()).collect())
            .await?;
        for (segment, (_, bytes)) in segments.iter().zip(segment_bytes) {
            verify_checksum(&segment.key, &bytes, segment.byte_size, &segment.sha256)?;
            block_store.install_segment(segment.page_segment_id, &bytes)?;
        }
        Ok(())
    }

    pub async fn restore_latest_checkpoint(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        self.restore_checkpoint(&manifest, engine, block_store)
            .await?;
        Ok(manifest)
    }

    pub async fn replay_oplog(
        &self,
        shard_id: ShardId,
        after_oplog_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        self.replay_wal(shard_id, after_oplog_index, engine).await
    }

    pub async fn replay_wal(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut keys = self.object_store.list(&self.oplog_prefix(shard_id)).await?;
        keys.sort();

        let mut report = ReplayReport {
            applied: 0,
            last_oplog_index: after_wal_index,
        };
        for key in keys {
            let Some(oplog_index) = parse_oplog_index(&key) else {
                continue;
            };
            if oplog_index <= after_wal_index {
                continue;
            }
            let entry = self.read_oplog_entry(&key).await?;
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: entry.command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    oplog_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_oplog_index = oplog_index;
        }
        Ok(report)
    }

    pub async fn replay_oplog_strict(
        &self,
        shard_id: ShardId,
        after_oplog_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        self.replay_wal_strict(shard_id, after_oplog_index, engine)
            .await
    }

    pub async fn replay_wal_strict(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut keys = self.object_store.list(&self.oplog_prefix(shard_id)).await?;
        keys.sort();

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_oplog_index: after_wal_index,
        };
        for key in keys {
            let Some(oplog_index) = parse_oplog_index(&key) else {
                continue;
            };
            if oplog_index <= after_wal_index {
                continue;
            }
            if oplog_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: oplog_index,
                });
            }
            let entry = self.read_oplog_entry(&key).await?;
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: entry.command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    oplog_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_oplog_index = oplog_index;
            expected += 1;
        }
        Ok(report)
    }

    pub async fn load_replay_cursor(
        &self,
        shard_id: ShardId,
    ) -> Result<SharedStoreReplayCursor, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.replay_cursor_key(shard_id))
            .await
        {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(ObjectStoreError::NotFound(_)) => Ok(SharedStoreReplayCursor {
                shard_id,
                last_oplog_index: 0,
                last_replay_time_ms: 0,
            }),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn save_replay_cursor(
        &self,
        cursor: &SharedStoreReplayCursor,
    ) -> Result<(), SharedStoreReplicationError> {
        self.object_store
            .put(
                &self.replay_cursor_key(cursor.shard_id),
                Bytes::from(serde_json::to_vec_pretty(cursor)?),
            )
            .await?;
        Ok(())
    }

    pub async fn replay_oplog_strict_with_cursor(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        self.replay_wal_strict_with_cursor(shard_id, engine).await
    }

    pub async fn replay_wal_strict_with_cursor(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut cursor = self.load_replay_cursor(shard_id).await?;
        let report = self
            .replay_wal_strict(shard_id, cursor.last_oplog_index, engine)
            .await?;
        if report.last_oplog_index > cursor.last_oplog_index {
            cursor.last_oplog_index = report.last_oplog_index;
            cursor.last_replay_time_ms = now_ms();
            self.save_replay_cursor(&cursor).await?;
        }
        Ok(report)
    }

    pub async fn gc_oplog_before(
        &self,
        shard_id: ShardId,
        retain_from_oplog_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        self.gc_wal_before(shard_id, retain_from_oplog_index).await
    }

    pub async fn gc_wal_before(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let mut deleted_oplog_objects = 0usize;
        for key in self.object_store.list(&self.oplog_prefix(shard_id)).await? {
            let Some(oplog_index) = parse_oplog_index(&key) else {
                continue;
            };
            if oplog_index < retain_from_wal_index {
                self.object_store.delete(&key).await?;
                deleted_oplog_objects += 1;
            }
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_oplog_objects,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_oplog_before_cursor_safe(
        &self,
        shard_id: ShardId,
        retain_from_oplog_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        self.gc_wal_before_cursor_safe(shard_id, retain_from_oplog_index)
            .await
    }

    pub async fn gc_wal_before_cursor_safe(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let cursor = self.load_replay_cursor(shard_id).await?;
        if cursor.last_oplog_index > 0
            && retain_from_wal_index > cursor.last_oplog_index.saturating_add(1)
        {
            return Err(SharedStoreReplicationError::GcBlockedByReplayCursor {
                cursor_oplog_index: cursor.last_oplog_index,
                retain_from_oplog_index: retain_from_wal_index,
            });
        }
        let mut report = self.gc_wal_before(shard_id, retain_from_wal_index).await?;
        if cursor.last_oplog_index > 0 {
            report.retained_for_cursor_oplog_index = Some(cursor.last_oplog_index);
        }
        Ok(report)
    }

    pub async fn gc_checkpoints(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        let delete_count = manifests.len().saturating_sub(keep_last);
        let retained_checkpoint_ids = manifests[delete_count..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoint_objects = 0usize;
        for manifest in manifests.iter().take(delete_count) {
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints: delete_count,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_checkpoints_cursor_safe(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        let cursor = self.load_replay_cursor(shard_id).await?;
        let retain_start = manifests.len().saturating_sub(keep_last);
        let cursor_anchor = if cursor.last_oplog_index > 0 {
            manifests
                .iter()
                .enumerate()
                .rev()
                .find(|(_, manifest)| manifest.checkpoint_oplog_index <= cursor.last_oplog_index)
                .map(|(index, _)| index)
        } else {
            None
        };
        let mut retained_checkpoint_ids = manifests[retain_start..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoints = 0usize;
        let mut deleted_checkpoint_objects = 0usize;
        for (index, manifest) in manifests.iter().enumerate() {
            let retained_by_keep_last = index >= retain_start;
            let retained_by_cursor = cursor_anchor == Some(index);
            if retained_by_keep_last || retained_by_cursor {
                if retained_by_cursor
                    && !retained_checkpoint_ids
                        .iter()
                        .any(|id| id == &manifest.checkpoint_id)
                {
                    retained_checkpoint_ids.push(manifest.checkpoint_id.clone());
                }
                continue;
            }
            if cursor.last_oplog_index > 0
                && manifest.checkpoint_oplog_index <= cursor.last_oplog_index
                && cursor_anchor.is_none()
            {
                return Err(
                    SharedStoreReplicationError::CheckpointGcBlockedByReplayCursor {
                        cursor_oplog_index: cursor.last_oplog_index,
                        checkpoint_oplog_index: manifest.checkpoint_oplog_index,
                        checkpoint_id: manifest.checkpoint_id.clone(),
                    },
                );
            }
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
            deleted_checkpoints += 1;
        }
        retained_checkpoint_ids.sort();
        retained_checkpoint_ids.dedup();
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            retained_for_cursor_oplog_index: (cursor.last_oplog_index > 0)
                .then_some(cursor.last_oplog_index),
            retained_for_cursor_checkpoint_id: cursor_anchor
                .map(|index| manifests[index].checkpoint_id.clone()),
            ..SharedStoreGcReport::default()
        })
    }

    fn shard_prefix(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}/shared/", self.cluster_id, shard_id)
    }

    fn index_key(&self, shard_id: ShardId) -> String {
        format!("{}index/shard.index.json", self.shard_prefix(shard_id))
    }

    fn page_segment_prefix(&self, shard_id: ShardId) -> String {
        format!("{}page_segments/", self.shard_prefix(shard_id))
    }

    fn page_segment_key(&self, shard_id: ShardId, page_segment_id: u64) -> String {
        format!(
            "{}page_segment_{page_segment_id:020}.seg",
            self.page_segment_prefix(shard_id)
        )
    }

    fn oplog_prefix(&self, shard_id: ShardId) -> String {
        format!("{}oplog/", self.shard_prefix(shard_id))
    }

    fn oplog_key(&self, shard_id: ShardId, oplog_index: u64) -> String {
        format!(
            "{}oplog_{oplog_index:020}.json",
            self.oplog_prefix(shard_id)
        )
    }

    fn replay_cursor_key(&self, shard_id: ShardId) -> String {
        format!("{}replay_cursor.json", self.shard_prefix(shard_id))
    }

    fn checkpoints_prefix(&self, shard_id: ShardId) -> String {
        format!("{}checkpoints/", self.shard_prefix(shard_id))
    }

    fn checkpoint_prefix(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!("{}{checkpoint_id}/", self.checkpoints_prefix(shard_id))
    }

    fn checkpoint_manifest_key(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!(
            "{}manifest.json",
            self.checkpoint_prefix(shard_id, checkpoint_id)
        )
    }

    async fn read_oplog_entry(
        &self,
        key: &str,
    ) -> Result<SharedStoreOplogEntry, SharedStoreReplicationError> {
        let bytes = self.object_store.get(key).await?;
        if let Ok(object) = serde_json::from_slice::<SharedStoreOplogObject>(&bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn put_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.put(key, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.retry_policy.backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.retry_policy.backoff_ms))
                            .await;
                    }
                }
            }
        }
        Err(last_error
            .expect("retry loop must record failed object-store error")
            .into())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize, SharedStoreReplicationError> {
        let keys = self.object_store.list(prefix).await?;
        let deleted = keys.len();
        self.delete_keys_concurrent(keys).await?;
        Ok(deleted)
    }

    async fn put_objects_concurrent(
        &self,
        objects: Vec<(String, Bytes)>,
    ) -> Result<(), SharedStoreReplicationError> {
        if self.transfer_concurrency <= 1 {
            for (key, bytes) in objects {
                self.put_with_retry(&key, bytes).await?;
            }
            return Ok(());
        }

        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0usize;
        while next_to_submit < objects.len() || !join_set.is_empty() {
            while next_to_submit < objects.len() && join_set.len() < self.transfer_concurrency {
                let (key, bytes) = objects[next_to_submit].clone();
                let replicator = self.clone();
                join_set.spawn(async move { replicator.put_with_retry(&key, bytes).await });
                next_to_submit += 1;
            }
            let Some(joined) = join_set.join_next().await else {
                continue;
            };
            joined.map_err(join_error)?.map_err(|err| {
                join_set.abort_all();
                err
            })?;
        }
        Ok(())
    }

    async fn get_objects_concurrent(
        &self,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Bytes)>, SharedStoreReplicationError> {
        if self.transfer_concurrency <= 1 {
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                let bytes = self.object_store.get(&key).await?;
                out.push((key, bytes));
            }
            return Ok(out);
        }

        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0usize;
        let mut out = Vec::with_capacity(keys.len());
        while next_to_submit < keys.len() || !join_set.is_empty() {
            while next_to_submit < keys.len() && join_set.len() < self.transfer_concurrency {
                let key = keys[next_to_submit].clone();
                let store = Arc::clone(&self.object_store);
                let index = next_to_submit;
                join_set.spawn(async move {
                    let bytes = store.get(&key).await?;
                    Ok::<_, SharedStoreReplicationError>((index, key, bytes))
                });
                next_to_submit += 1;
            }
            let Some(joined) = join_set.join_next().await else {
                continue;
            };
            match joined.map_err(join_error)? {
                Ok(item) => out.push(item),
                Err(err) => {
                    join_set.abort_all();
                    return Err(err);
                }
            }
        }
        out.sort_by_key(|(index, _, _)| *index);
        Ok(out
            .into_iter()
            .map(|(_, key, bytes)| (key, bytes))
            .collect())
    }

    async fn delete_keys_concurrent(
        &self,
        keys: Vec<String>,
    ) -> Result<(), SharedStoreReplicationError> {
        if self.transfer_concurrency <= 1 {
            for key in keys {
                self.object_store.delete(&key).await?;
            }
            return Ok(());
        }

        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0usize;
        while next_to_submit < keys.len() || !join_set.is_empty() {
            while next_to_submit < keys.len() && join_set.len() < self.transfer_concurrency {
                let key = keys[next_to_submit].clone();
                let store = Arc::clone(&self.object_store);
                join_set.spawn(async move { store.delete(&key).await.map_err(Into::into) });
                next_to_submit += 1;
            }
            let Some(joined) = join_set.join_next().await else {
                continue;
            };
            match joined.map_err(join_error)? {
                Ok(()) => {}
                Err(err) => {
                    join_set.abort_all();
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

fn default_transfer_concurrency() -> usize {
    std::env::var("TS_SHARED_STORE_TRANSFER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
}

fn join_error(err: tokio::task::JoinError) -> SharedStoreReplicationError {
    SharedStoreReplicationError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("shared-store transfer task failed: {err}"),
    ))
}

impl<O> SharedStoreStorageWriter<O>
where
    O: ObjectStore + 'static,
{
    pub async fn write(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        let oplog_index = self.next_oplog_index.fetch_add(1, Ordering::Relaxed);
        let entry = SharedStoreOplogEntry {
            shard_id,
            oplog_index,
            command,
        };
        match self.mode {
            SharedStoreStorageMode::Sync => {
                self.replicator.publish_oplog_entry(entry).await?;
                Ok(SharedStoreWriteReport {
                    oplog_index,
                    published: true,
                    queued: false,
                })
            }
            SharedStoreStorageMode::Async => {
                self.pending
                    .lock()
                    .expect("shared-store async queue lock poisoned")
                    .push_back(entry);
                Ok(SharedStoreWriteReport {
                    oplog_index,
                    published: false,
                    queued: true,
                })
            }
        }
    }

    pub fn queued_len(&self) -> usize {
        self.pending
            .lock()
            .expect("shared-store async queue lock poisoned")
            .len()
    }

    pub async fn flush_pending(
        &self,
        max_entries: usize,
    ) -> Result<SharedStoreFlushReport, SharedStoreReplicationError> {
        let limit = max_entries.max(1);
        let mut drained = std::collections::VecDeque::new();
        {
            let mut pending = self
                .pending
                .lock()
                .expect("shared-store async queue lock poisoned");
            for _ in 0..limit {
                let Some(entry) = pending.pop_front() else {
                    break;
                };
                drained.push_back(entry);
            }
        }

        let mut last_oplog_index = 0;
        let mut flushed = 0usize;
        while let Some(entry) = drained.pop_front() {
            last_oplog_index = entry.oplog_index;
            if let Err(err) = self.replicator.publish_oplog_entry(entry.clone()).await {
                let mut to_requeue = Vec::with_capacity(drained.len() + 1);
                let mut pending = self
                    .pending
                    .lock()
                    .expect("shared-store async queue lock poisoned");
                to_requeue.push(entry);
                while let Some(entry) = drained.pop_front() {
                    to_requeue.push(entry);
                }
                while let Some(entry) = to_requeue.pop() {
                    pending.push_front(entry);
                }
                return Err(err);
            }
            flushed += 1;
        }
        let remaining = {
            let pending = self
                .pending
                .lock()
                .expect("shared-store async queue lock poisoned");
            pending.len()
        };
        Ok(SharedStoreFlushReport {
            flushed,
            remaining,
            last_oplog_index,
        })
    }

    pub async fn flush_pending_concurrent(
        &self,
        max_entries: usize,
        max_in_flight: usize,
    ) -> Result<SharedStoreFlushReport, SharedStoreReplicationError> {
        let limit = max_entries.max(1);
        let max_in_flight = max_in_flight.max(1);
        if max_in_flight == 1 {
            return self.flush_pending(limit).await;
        }

        let mut drained = Vec::new();
        {
            let mut pending = self
                .pending
                .lock()
                .expect("shared-store async queue lock poisoned");
            for _ in 0..limit {
                let Some(entry) = pending.pop_front() else {
                    break;
                };
                drained.push(entry);
            }
        }

        if drained.is_empty() {
            let remaining = self.queued_len();
            return Ok(SharedStoreFlushReport {
                flushed: 0,
                remaining,
                last_oplog_index: 0,
            });
        }

        let mut next_to_submit = 0usize;
        let mut flushed = 0usize;
        let mut last_oplog_index = 0u64;
        let mut join_set = JoinSet::new();
        while next_to_submit < drained.len() || !join_set.is_empty() {
            while next_to_submit < drained.len() && join_set.len() < max_in_flight {
                let entry = drained[next_to_submit].clone();
                let replicator = self.replicator.clone();
                let entry_index = next_to_submit;
                join_set.spawn(async move {
                    let oplog_index = entry.oplog_index;
                    let result = replicator.publish_oplog_entry(entry).await;
                    (entry_index, oplog_index, result)
                });
                next_to_submit += 1;
            }

            let Some(joined) = join_set.join_next().await else {
                continue;
            };
            let (entry_index, oplog_index, result) = joined.map_err(|err| {
                SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("shared-store flush task failed: {err}"),
                ))
            })?;
            if let Err(err) = result {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                self.requeue_unflushed(&drained, entry_index);
                return Err(err);
            }
            flushed += 1;
            last_oplog_index = last_oplog_index.max(oplog_index);
        }

        let remaining = self.queued_len();
        Ok(SharedStoreFlushReport {
            flushed,
            remaining,
            last_oplog_index,
        })
    }

    fn requeue_unflushed(&self, drained: &[SharedStoreOplogEntry], failed_index: usize) {
        let mut pending = self
            .pending
            .lock()
            .expect("shared-store async queue lock poisoned");
        for entry in drained[failed_index..].iter().rev() {
            pending.push_front(entry.clone());
        }
    }
}

fn parse_page_segment_id(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("page_segment_")?
        .strip_suffix(".seg")?
        .parse()
        .ok()
}

fn parse_oplog_index(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("oplog_")?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

fn verify_checksum(
    key: &str,
    bytes: &[u8],
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), SharedStoreReplicationError> {
    let actual = sha256_hex(bytes);
    if bytes.len() as u64 != expected_size || actual != expected_sha256 {
        return Err(SharedStoreReplicationError::ChecksumMismatch {
            path: key.to_string(),
            expected: expected_sha256.to_string(),
            actual,
        });
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

    use super::*;
    use crate::types::CommandResponse;

    const TEST_CLUSTER_ID: &str = "cluster-a";
    const TEST_CACHE_BYTES: usize = 1024;

    fn test_engine(root: &Path, role: &str) -> TemporalEngine {
        test_engine_with_cache(root, role, TEST_CACHE_BYTES)
    }

    fn test_engine_with_cache(root: &Path, role: &str, cache_bytes: usize) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            cache_bytes,
            root.join(format!("{role}-cache")),
            root.join(format!("{role}-pages")),
            root.join(format!("{role}-index")),
        )
    }

    fn test_shared_store(
        root: &Path,
    ) -> (Arc<FileObjectStore>, SharedStoreReplicator<FileObjectStore>) {
        let store = Arc::new(FileObjectStore::new(root.join("objects")));
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone());
        (store, replicator)
    }

    #[tokio::test]
    async fn shared_store_restores_index_pages_and_replays_later_oplog() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_page_segments(1, &primary.block_store())
            .await
            .unwrap();
        replicator
            .publish_oplog_entry(SharedStoreOplogEntry {
                shard_id: 1,
                oplog_index: 2,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                },
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_pages(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored, vec![0]);
        follower.load_shard(1);

        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );

        let report = replicator.replay_oplog(1, 1, &follower).await.unwrap();
        assert_eq!(
            report,
            ReplayReport {
                applied: 1,
                last_oplog_index: 2,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn shared_store_checkpoint_rejects_corrupt_page_segment() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });

        let (store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        store
            .put(
                &manifest.page_segments[0].key,
                Bytes::from_static(b"corrupt"),
            )
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        assert!(matches!(
            replicator
                .restore_checkpoint(&manifest, &follower, &follower.block_store())
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn shared_store_strict_replay_rejects_wal_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                oplog_index: 2,
                command: Command::StringSet {
                    key: "gap".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert!(matches!(
            replicator
                .replay_wal_strict(1, 0, &follower)
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ReplayGap {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[tokio::test]
    async fn shared_store_sync_storage_publishes_and_cursor_replay_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 1);

        let report = writer
            .write(
                1,
                Command::StringSet {
                    key: "sync".to_string(),
                    value: b"published".to_vec(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            report,
            SharedStoreWriteReport {
                oplog_index: 1,
                published: true,
                queued: false,
            }
        );

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let replay = replicator
            .replay_oplog_strict_with_cursor(1, &follower)
            .await
            .unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 1,
                last_oplog_index: 1,
            }
        );
        assert_eq!(
            replicator
                .load_replay_cursor(1)
                .await
                .unwrap()
                .last_oplog_index,
            1
        );
        assert_eq!(
            replicator
                .replay_oplog_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_oplog_index: 1,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "sync".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"published".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn shared_store_async_storage_flushes_in_order_with_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);

        for (key, value) in [("a", b"1".to_vec()), ("b", b"2".to_vec())] {
            let report = writer
                .write(
                    1,
                    Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                )
                .await
                .unwrap();
            assert!(report.queued);
            assert!(!report.published);
        }
        assert_eq!(writer.queued_len(), 2);

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert_eq!(
            replicator
                .replay_oplog_strict(1, 0, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_oplog_index: 0,
            }
        );

        assert_eq!(
            writer.flush_pending(1).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 1,
                last_oplog_index: 1,
            }
        );
        assert_eq!(
            replicator
                .replay_oplog_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_oplog_index: 1,
            }
        );

        assert_eq!(
            writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_oplog_index: 2,
            }
        );
        assert_eq!(
            replicator
                .replay_oplog_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_oplog_index: 2,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "b".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"2".to_vec())
            }
        );
    }

    // shared-corpus: storage_disk_shared_store_persistence_parity
    #[tokio::test]
    async fn disk_and_shared_store_persistence_recover_through_restart_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        assert!(
            primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "disk-key".to_string(),
                        value: b"disk-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        drop(primary);

        let restarted_primary = test_engine(dir.path(), "primary");
        restarted_primary.load_shard(1);
        assert_eq!(
            restarted_primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "disk-key".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"disk-value".to_vec())
            }
        );

        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &restarted_primary, &restarted_primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_oplog_index, 1);
        assert!(!manifest.page_segments.is_empty());

        let sync_writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 2);
        assert_eq!(
            sync_writer
                .write(
                    1,
                    Command::StringSet {
                        key: "shared-sync".to_string(),
                        value: b"sync-value".to_vec(),
                    },
                )
                .await
                .unwrap(),
            SharedStoreWriteReport {
                oplog_index: 2,
                published: true,
                queued: false,
            }
        );

        let async_writer = replicator.storage_writer(SharedStoreStorageMode::Async, 3);
        assert_eq!(
            async_writer
                .write(
                    1,
                    Command::StringSet {
                        key: "shared-async".to_string(),
                        value: b"async-value".to_vec(),
                    },
                )
                .await
                .unwrap(),
            SharedStoreWriteReport {
                oplog_index: 3,
                published: false,
                queued: true,
            }
        );
        assert_eq!(
            async_writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_oplog_index: 3,
            }
        );

        let follower = test_engine_with_cache(dir.path(), "follower", 32);
        replicator
            .restore_checkpoint(&manifest, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        let reads_before = follower.block_store().stats().reads;
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "disk-key".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"disk-value".to_vec())
            }
        );
        assert!(
            follower.block_store().stats().reads > reads_before,
            "restored follower should read checkpointed bytes from disk-backed block segments"
        );

        let replay = replicator.replay_wal_strict(1, 1, &follower).await.unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 2,
                last_oplog_index: 3,
            }
        );
        for (key, value) in [
            ("shared-sync", b"sync-value".to_vec()),
            ("shared-async", b"async-value".to_vec()),
        ] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(value) }
            );
        }

        replicator
            .save_replay_cursor(&SharedStoreReplayCursor {
                shard_id: 1,
                last_oplog_index: replay.last_oplog_index,
                last_replay_time_ms: now_ms(),
            })
            .await
            .unwrap();
        let gc = replicator
            .gc_wal_before_cursor_safe(1, 5)
            .await
            .unwrap_err();
        assert!(matches!(
            gc,
            SharedStoreReplicationError::GcBlockedByReplayCursor {
                cursor_oplog_index: 3,
                retain_from_oplog_index: 5,
            }
        ));
    }

    #[tokio::test]
    async fn shared_store_rejects_corrupt_oplog_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_oplog_entry(SharedStoreOplogEntry {
                shard_id: 1,
                oplog_index: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .await
            .unwrap();

        let key = "cluster-a/shards/1/shared/oplog/oplog_00000000000000000001.json";
        let mut object: SharedStoreOplogObject =
            serde_json::from_slice(&store.get(key).await.unwrap()).unwrap();
        object.entry_sha256 = "bad".to_string();
        store
            .put(
                key,
                Bytes::from(serde_json::to_vec_pretty(&object).unwrap()),
            )
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert!(matches!(
            replicator
                .replay_oplog_strict(1, 0, &follower)
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn shared_store_retry_policy_retries_transient_put_failures() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FlakyObjectStore {
            inner: FileObjectStore::new(dir.path().join("objects")),
            fail_puts: Mutex::new(1),
        });
        let replicator = SharedStoreReplicator::with_retry_policy(
            "cluster-a",
            store,
            SharedStoreRetryPolicy {
                max_attempts: 2,
                backoff_ms: 0,
            },
        );
        replicator
            .publish_oplog_entry(SharedStoreOplogEntry {
                shard_id: 1,
                oplog_index: 1,
                command: Command::StringSet {
                    key: "retry".to_string(),
                    value: b"ok".to_vec(),
                },
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shared_store_async_flush_requeues_after_publish_failure() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FlakyObjectStore {
            inner: FileObjectStore::new(dir.path().join("objects")),
            fail_puts: Mutex::new(1),
        });
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store);
        let writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);
        writer
            .write(
                1,
                Command::StringSet {
                    key: "retry-queue".to_string(),
                    value: b"ok".to_vec(),
                },
            )
            .await
            .unwrap();

        assert!(writer.flush_pending(1).await.is_err());
        assert_eq!(writer.queued_len(), 1);
        assert_eq!(
            writer.flush_pending(1).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_oplog_index: 1,
            }
        );
    }

    #[tokio::test]
    async fn shared_store_gc_removes_old_oplog_and_checkpoint_generations() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        let (store, replicator) = test_shared_store(dir.path());

        for oplog_index in 1..=3 {
            replicator
                .publish_oplog_entry(SharedStoreOplogEntry {
                    shard_id: 1,
                    oplog_index,
                    command: Command::StringSet {
                        key: format!("k{oplog_index}"),
                        value: vec![oplog_index as u8],
                    },
                })
                .await
                .unwrap();
        }
        let oplog_gc = replicator.gc_oplog_before(1, 3).await.unwrap();
        assert_eq!(oplog_gc.deleted_oplog_objects, 2);
        let oplog_keys = store
            .list("cluster-a/shards/1/shared/oplog/")
            .await
            .unwrap();
        assert_eq!(oplog_keys.len(), 1);
        assert!(oplog_keys[0].ends_with("oplog_00000000000000000003.json"));

        for checkpoint_oplog_index in 1..=3 {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("checkpoint-{checkpoint_oplog_index}"),
                    value: vec![checkpoint_oplog_index as u8],
                },
            });
            replicator
                .publish_checkpoint(1, checkpoint_oplog_index, &primary, &primary.block_store())
                .await
                .unwrap();
        }
        let checkpoint_gc = replicator.gc_checkpoints(1, 1).await.unwrap();
        assert_eq!(checkpoint_gc.deleted_checkpoints, 2);
        assert_eq!(checkpoint_gc.retained_checkpoint_ids.len(), 1);
        assert_eq!(replicator.list_checkpoints(1).await.unwrap().len(), 1);
    }

    #[derive(Debug)]
    struct FlakyObjectStore {
        inner: FileObjectStore,
        fail_puts: Mutex<usize>,
    }

    #[async_trait]
    impl ObjectStore for FlakyObjectStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
            {
                let mut fail_puts = self.fail_puts.lock().expect("flaky store lock poisoned");
                if *fail_puts > 0 {
                    *fail_puts -= 1;
                    return Err(ObjectStoreError::InvalidKey("injected failure".to_string()));
                }
            }
            self.inner.put(key, bytes).await
        }

        async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
            self.inner.get(key).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn uri(&self, key: &str) -> String {
            self.inner.uri(key)
        }
    }
}
