// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalstore_snapshot::object_store::{
    AppendBlobReceipt, FileObjectStore, ObjectStore, ObjectStoreError,
};
use thiserror::Error;

use crate::block_store::{BlockStoreError, LocalBlockStore, SharedSlabSource};
use crate::engine::TemporalEngine;
use crate::sdk::{self, v1};
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
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("no shared-store checkpoint found for shard {0}")]
    CheckpointNotFound(ShardId),
    #[error("replicated command failed at WAL index {wal_index}: {status:?}")]
    ApplyFailed { wal_index: u64, status: Status },
    #[error("WAL replay gap: expected index {expected}, got {actual}")]
    ReplayGap { expected: u64, actual: u64 },
    #[error(
        "shared-store GC would remove WAL entry needed by replay cursor {cursor_wal_index} before retain {retain_from_wal_index}"
    )]
    GcBlockedByReplayCursor {
        cursor_wal_index: u64,
        retain_from_wal_index: u64,
    },
    #[error(
        "shared-store checkpoint GC would remove checkpoint {checkpoint_id} at WAL index {checkpoint_wal_index} needed by replay cursor {cursor_wal_index}"
    )]
    CheckpointGcBlockedByReplayCursor {
        cursor_wal_index: u64,
        checkpoint_wal_index: u64,
        checkpoint_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalEntry {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalObject {
    pub entry: SharedStoreWalEntry,
    pub entry_byte_size: u64,
    pub entry_sha256: String,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWalOffsetMetadata {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub wal_blob_key: String,
    pub wal_blob_start_offset: u64,
    pub wal_blob_end_offset: u64,
    pub wal_blob_bytes_written: u64,
    pub wal_blob_object_length: u64,
    #[serde(default)]
    pub wal_blob_physical_band_count: u64,
    pub wal_blob_first_physical_offset: Option<u64>,
    pub command_byte_size: u64,
    pub command_sha256: String,
    pub command_encoding: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalIndexedRead {
    pub metadata: SharedStoreWalOffsetMetadata,
    pub entry: SharedStoreWalEntry,
    pub range_bytes_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStorePageSlab {
    #[serde(alias = "page_segment_id")]
    pub page_slab_id: u64,
    pub key: String,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreCheckpointManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub checkpoint_id: String,
    #[serde(rename = "checkpoint_wal_index")]
    pub checkpoint_wal_index: u64,
    pub created_at_ms: u64,
    pub index_key: String,
    pub index_byte_size: u64,
    pub index_sha256: String,
    #[serde(alias = "page_segments")]
    pub page_slabs: Vec<SharedStorePageSlab>,
    /// Next free block page id at checkpoint time. A lazy restore advances the fresh
    /// owner's page-id counter past this floor so replayed/new writes never reuse a
    /// page id that a lazily-fetched checkpoint slab still carries. Defaults to 0 for
    /// manifests written before this field existed (backward compatible).
    #[serde(default)]
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreReplayCursor {
    pub shard_id: ShardId,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
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
pub enum SharedStoreWalAppendMode {
    JsonPerKey,
    ProtobufAppendBlob,
}

impl Default for SharedStoreWalAppendMode {
    fn default() -> Self {
        Self::JsonPerKey
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWriteReport {
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub published: bool,
    pub queued: bool,
    #[serde(default)]
    pub wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_bytes_written: Option<u64>,
    #[serde(default)]
    pub wal_blob_object_length: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreFlushReport {
    pub flushed: usize,
    pub remaining: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub last_wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_object_length: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreGcReport {
    pub shard_id: ShardId,
    #[serde(rename = "deleted_wal_objects")]
    pub deleted_wal_objects: usize,
    pub deleted_checkpoints: usize,
    pub deleted_checkpoint_objects: usize,
    pub retained_checkpoint_ids: Vec<String>,
    #[serde(rename = "retained_for_cursor_wal_index", default)]
    pub retained_for_cursor_wal_index: Option<u64>,
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
    wal_append_mode: SharedStoreWalAppendMode,
}

impl<O> Clone for SharedStoreReplicator<O> {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            object_store: Arc::clone(&self.object_store),
            retry_policy: self.retry_policy,
            wal_append_mode: self.wal_append_mode,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalFrameProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    command_byte_size: u64,
    #[prost(string, tag = "5")]
    command_sha256: String,
    #[prost(uint32, tag = "6")]
    command_encoding: u32,
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalOffsetMetadataProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(string, tag = "3")]
    wal_blob_key: String,
    #[prost(uint64, tag = "4")]
    wal_blob_start_offset: u64,
    #[prost(uint64, tag = "5")]
    wal_blob_end_offset: u64,
    #[prost(uint64, tag = "6")]
    wal_blob_bytes_written: u64,
    #[prost(uint64, tag = "7")]
    wal_blob_object_length: u64,
    #[prost(uint64, tag = "8")]
    command_byte_size: u64,
    #[prost(string, tag = "9")]
    command_sha256: String,
    #[prost(uint32, tag = "10")]
    command_encoding: u32,
    #[prost(uint64, tag = "11")]
    wal_blob_physical_band_count: u64,
    #[prost(uint64, optional, tag = "12")]
    wal_blob_first_physical_offset: Option<u64>,
}

#[derive(Debug)]
pub struct SharedStoreStorageWriter<O> {
    replicator: SharedStoreReplicator<O>,
    mode: SharedStoreStorageMode,
    next_wal_index: AtomicU64,
    pending: Mutex<VecDeque<SharedStoreWalEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub applied: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub offset_index_reads: usize,
    #[serde(default)]
    pub range_bytes_read: u64,
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
            wal_append_mode: SharedStoreWalAppendMode::default(),
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
            wal_append_mode: SharedStoreWalAppendMode::default(),
        }
    }

    pub fn with_wal_append_mode(mut self, mode: SharedStoreWalAppendMode) -> Self {
        self.wal_append_mode = mode;
        self
    }

    pub async fn publish_wal_entry(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            return self.publish_wal_entry_protobuf_blob(entry).await;
        }
        let key = self.wal_key(entry.shard_id, entry.wal_index);
        let entry_bytes = serde_json::to_vec(&entry)?;
        let object = SharedStoreWalObject {
            entry,
            entry_byte_size: entry_bytes.len() as u64,
            entry_sha256: sha256_hex(&entry_bytes),
        };
        self.put_with_retry(&key, Bytes::from(serde_json::to_vec(&object)?))
            .await?;
        Ok(None)
    }

    async fn publish_wal_entry_protobuf_blob(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let key = self.wal_blob_key(entry.shard_id);
        let command_metadata = wal_command_metadata(&entry.command)?;
        let frame = encode_wal_proto_frame(&entry)?;
        let receipt = self
            .append_blob_with_retry(&key, Bytes::from(frame))
            .await?
            .expect("protobuf append blob must return a receipt");
        self.publish_wal_offset_metadata(&entry, &key, &receipt, command_metadata)
            .await?;
        Ok(Some(receipt))
    }

    async fn publish_wal_offset_metadata(
        &self,
        entry: &SharedStoreWalEntry,
        wal_blob_key: &str,
        receipt: &AppendBlobReceipt,
        command_metadata: WalCommandMetadata,
    ) -> Result<(), SharedStoreReplicationError> {
        let metadata = SharedStoreWalOffsetMetadata {
            shard_id: entry.shard_id,
            wal_index: entry.wal_index,
            wal_blob_key: wal_blob_key.to_string(),
            wal_blob_start_offset: receipt.start_offset,
            wal_blob_end_offset: receipt.end_offset,
            wal_blob_bytes_written: receipt.bytes_written,
            wal_blob_object_length: receipt.object_length,
            wal_blob_physical_band_count: receipt.physical_band_count as u64,
            wal_blob_first_physical_offset: receipt.first_physical_offset,
            command_byte_size: command_metadata.byte_size,
            command_sha256: command_metadata.sha256,
            command_encoding: command_metadata.encoding,
        };
        let frame = encode_wal_offset_metadata_frame(&metadata);
        self.append_blob_with_retry(
            &self.wal_offset_index_blob_key(entry.shard_id),
            Bytes::from(frame),
        )
        .await?;
        Ok(())
    }

    pub async fn lookup_wal_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        Ok(self
            .load_wal_offset_metadata(shard_id)
            .await?
            .remove(&wal_index))
    }

    pub async fn read_wal_entry_by_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalIndexedRead>, SharedStoreReplicationError> {
        let Some(metadata) = self
            .lookup_wal_offset_metadata(shard_id, wal_index)
            .await?
        else {
            return Ok(None);
        };
        let length = metadata
            .wal_blob_end_offset
            .checked_sub(metadata.wal_blob_start_offset)
            .ok_or_else(|| SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid WAL offset metadata range for shard {shard_id} index {wal_index}"),
            )))?;
        if length != metadata.wal_blob_bytes_written {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "WAL offset metadata bytes mismatch for shard {shard_id} index {wal_index}"
                ),
            )));
        }
        let bytes = self
            .object_store
            .get_range(
                &metadata.wal_blob_key,
                metadata.wal_blob_start_offset,
                length,
            )
            .await?;
        let entry = decode_wal_proto_frame_exact(&bytes, &metadata.wal_blob_key)?;
        if entry.shard_id != shard_id || entry.wal_index != wal_index {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("WAL offset metadata points to shard {} index {}, expected shard {shard_id} index {wal_index}", entry.shard_id, entry.wal_index),
            )));
        }
        Ok(Some(SharedStoreWalIndexedRead {
            metadata,
            entry,
            range_bytes_read: length,
        }))
    }

    pub fn storage_writer(
        &self,
        mode: SharedStoreStorageMode,
        next_wal_index: u64,
    ) -> SharedStoreStorageWriter<O> {
        SharedStoreStorageWriter {
            replicator: self.clone(),
            mode,
            next_wal_index: AtomicU64::new(next_wal_index.max(1)),
            pending: Mutex::default(),
        }
    }

    pub fn default_storage_writer(&self, next_wal_index: u64) -> SharedStoreStorageWriter<O> {
        self.storage_writer(SharedStoreStorageMode::default(), next_wal_index)
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

    pub async fn publish_page_slabs(
        &self,
        shard_id: ShardId,
        block_store: &LocalBlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let mut published = Vec::new();
        for page_slab_id in block_store.slab_ids()? {
            self.object_store
                .put(
                    &self.page_slab_key(shard_id, page_slab_id),
                    Bytes::from(block_store.read_slab(page_slab_id)?),
                )
                .await?;
            published.push(page_slab_id);
        }
        Ok(published)
    }

    pub async fn publish_checkpoint(
        &self,
        shard_id: ShardId,
        checkpoint_wal_index: u64,
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

        let mut page_slabs = Vec::new();
        for page_slab_id in block_store.slab_ids()? {
            let bytes = block_store.read_slab(page_slab_id)?;
            let key = format!("{prefix}page_segments/page_segment_{page_slab_id:020}.seg");
            self.object_store
                .put(&key, Bytes::from(bytes.clone()))
                .await?;
            page_slabs.push(SharedStorePageSlab {
                page_slab_id,
                key,
                byte_size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
            });
        }

        let manifest = SharedStoreCheckpointManifest {
            cluster_id: self.cluster_id.clone(),
            shard_id,
            checkpoint_id,
            checkpoint_wal_index,
            created_at_ms: now_ms(),
            index_key,
            index_byte_size: index.len() as u64,
            index_sha256: sha256_hex(&index),
            page_slabs,
            next_page_id: block_store.next_page_id(),
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

        let prefix = self.page_slab_prefix(shard_id);
        let mut restored = Vec::new();
        for key in self.object_store.list(&prefix).await? {
            let Some(page_slab_id) = parse_page_slab_id(&key) else {
                continue;
            };
            let bytes = self.object_store.get(&key).await?;
            block_store.install_slab(page_slab_id, &bytes)?;
            restored.push(page_slab_id);
        }
        restored.sort_unstable();
        Ok(restored)
    }

    pub async fn list_checkpoints(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SharedStoreCheckpointManifest>, SharedStoreReplicationError> {
        let mut manifests = Vec::new();
        for key in self
            .object_store
            .list(&self.checkpoints_prefix(shard_id))
            .await?
        {
            if !key.ends_with("/manifest.json") {
                continue;
            }
            manifests.push(serde_json::from_slice(&self.object_store.get(&key).await?)?);
        }
        manifests.sort_by_key(|manifest: &SharedStoreCheckpointManifest| {
            (manifest.checkpoint_wal_index, manifest.created_at_ms)
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

        for slab in &manifest.page_slabs {
            let bytes = self.object_store.get(&slab.key).await?;
            verify_checksum(&slab.key, &bytes, slab.byte_size, &slab.sha256)?;
            block_store.install_slab(slab.page_slab_id, &bytes)?;
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

    pub async fn replay_wal(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            if let Some(report) = self
                .replay_wal_from_offset_metadata(shard_id, after_wal_index, engine, false)
                .await?
            {
                return Ok(report);
            }
        }
        let wal_entries = self.load_wal_entries(shard_id).await?;

        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, entry) in wal_entries {
            if wal_index <= after_wal_index {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: entry.command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = wal_index;
        }
        Ok(report)
    }

    pub async fn replay_wal_strict(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            if let Some(report) = self
                .replay_wal_from_offset_metadata(shard_id, after_wal_index, engine, true)
                .await?
            {
                return Ok(report);
            }
        }
        let wal_entries = self.load_wal_entries(shard_id).await?;

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, entry) in wal_entries {
            if wal_index <= after_wal_index {
                continue;
            }
            if wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: wal_index,
                });
            }
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: entry.command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = wal_index;
            expected += 1;
        }
        Ok(report)
    }

    async fn replay_wal_from_offset_metadata(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
        strict: bool,
    ) -> Result<Option<ReplayReport>, SharedStoreReplicationError> {
        let offset_metadata = self.load_wal_offset_metadata(shard_id).await?;
        if offset_metadata.is_empty() {
            return Ok(None);
        }

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, _) in offset_metadata.range((after_wal_index + 1)..) {
            if strict && *wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: *wal_index,
                });
            }
            let read = self
                .read_wal_entry_by_offset_metadata(shard_id, *wal_index)
                .await?
                .ok_or_else(|| {
                    SharedStoreReplicationError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "missing WAL offset metadata for shard {shard_id} index {wal_index}"
                        ),
                    ))
                })?;
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: read.entry.command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index: *wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = *wal_index;
            report.offset_index_reads += 1;
            report.range_bytes_read += read.range_bytes_read;
            expected += 1;
        }
        Ok(Some(report))
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
                last_wal_index: 0,
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

    pub async fn replay_wal_strict_with_cursor(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut cursor = self.load_replay_cursor(shard_id).await?;
        let report = self
            .replay_wal_strict(shard_id, cursor.last_wal_index, engine)
            .await?;
        if report.last_wal_index > cursor.last_wal_index {
            cursor.last_wal_index = report.last_wal_index;
            cursor.last_replay_time_ms = now_ms();
            self.save_replay_cursor(&cursor).await?;
        }
        Ok(report)
    }

    pub async fn gc_wal_before(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let mut deleted_wal_objects = 0usize;
        for key in self.object_store.list(&self.wal_prefix(shard_id)).await? {
            let Some(wal_index) = parse_wal_index(&key) else {
                continue;
            };
            if wal_index < retain_from_wal_index {
                self.object_store.delete(&key).await?;
                deleted_wal_objects += 1;
            }
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_wal_objects,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_wal_before_cursor_safe(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let cursor = self.load_replay_cursor(shard_id).await?;
        if cursor.last_wal_index > 0
            && retain_from_wal_index > cursor.last_wal_index.saturating_add(1)
        {
            return Err(SharedStoreReplicationError::GcBlockedByReplayCursor {
                cursor_wal_index: cursor.last_wal_index,
                retain_from_wal_index: retain_from_wal_index,
            });
        }
        let mut report = self.gc_wal_before(shard_id, retain_from_wal_index).await?;
        if cursor.last_wal_index > 0 {
            report.retained_for_cursor_wal_index = Some(cursor.last_wal_index);
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
        let cursor_anchor = if cursor.last_wal_index > 0 {
            manifests
                .iter()
                .enumerate()
                .rev()
                .find(|(_, manifest)| manifest.checkpoint_wal_index <= cursor.last_wal_index)
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
            if cursor.last_wal_index > 0
                && manifest.checkpoint_wal_index <= cursor.last_wal_index
                && cursor_anchor.is_none()
            {
                return Err(
                    SharedStoreReplicationError::CheckpointGcBlockedByReplayCursor {
                        cursor_wal_index: cursor.last_wal_index,
                        checkpoint_wal_index: manifest.checkpoint_wal_index,
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
            retained_for_cursor_wal_index: (cursor.last_wal_index > 0)
                .then_some(cursor.last_wal_index),
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

    fn page_slab_prefix(&self, shard_id: ShardId) -> String {
        format!("{}page_segments/", self.shard_prefix(shard_id))
    }

    fn page_slab_key(&self, shard_id: ShardId, page_slab_id: u64) -> String {
        format!(
            "{}page_segment_{page_slab_id:020}.seg",
            self.page_slab_prefix(shard_id)
        )
    }

    fn wal_prefix(&self, shard_id: ShardId) -> String {
        format!("{}wal/", self.shard_prefix(shard_id))
    }

    fn wal_key(&self, shard_id: ShardId, wal_index: u64) -> String {
        format!(
            "{}wal_{wal_index:020}.json",
            self.wal_prefix(shard_id)
        )
    }

    fn wal_blob_key(&self, shard_id: ShardId) -> String {
        format!("{}wal.protobuf.blob", self.wal_prefix(shard_id))
    }

    fn wal_offset_index_blob_key(&self, shard_id: ShardId) -> String {
        format!(
            "{}wal.offset_index.protobuf.blob",
            self.wal_prefix(shard_id)
        )
    }

    pub async fn load_wal_offset_metadata(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.wal_offset_index_blob_key(shard_id))
            .await
        {
            Ok(bytes) => decode_wal_offset_metadata_blob(&bytes),
            Err(ObjectStoreError::NotFound(_)) => Ok(BTreeMap::new()),
            Err(err) => Err(err.into()),
        }
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

    async fn read_wal_entry(
        &self,
        key: &str,
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        let bytes = self.object_store.get(key).await?;
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(&bytes) {
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

    fn parse_wal_entry_object(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    async fn load_wal_entries(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalEntry>, SharedStoreReplicationError> {
        let mut entries = BTreeMap::new();
        let mut keys = self.object_store.list(&self.wal_prefix(shard_id)).await?;
        keys.sort();
        let indexed_keys = keys
            .into_iter()
            .filter_map(|key| parse_wal_index(&key).map(|wal_index| (wal_index, key)))
            .collect::<Vec<_>>();
        if !indexed_keys.is_empty() {
            let object_keys = indexed_keys
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let objects = self.object_store.get_many(&object_keys).await?;
            if objects.len() == indexed_keys.len()
                && objects
                    .iter()
                    .zip(&indexed_keys)
                    .all(|((actual, _), (_, expected))| actual == expected)
            {
                for ((_, bytes), (wal_index, key)) in objects.iter().zip(&indexed_keys) {
                    entries.insert(*wal_index, self.parse_wal_entry_object(key, bytes)?);
                }
            } else {
                for (wal_index, key) in &indexed_keys {
                    let entry = self.read_wal_entry(key).await?;
                    entries.insert(*wal_index, entry);
                }
            }
        }

        match self.object_store.get(&self.wal_blob_key(shard_id)).await {
            Ok(bytes) => {
                for entry in decode_wal_proto_blob(&bytes)? {
                    entries.insert(entry.wal_index, entry);
                }
            }
            Err(ObjectStoreError::NotFound(_)) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(entries)
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

    async fn append_blob_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.append_blob(key, bytes.clone()).await {
                Ok(receipt) => return Ok(Some(receipt)),
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
        for key in keys {
            self.object_store.delete(&key).await?;
        }
        Ok(deleted)
    }
}

/// One slab's location + integrity in a shared-storage checkpoint: the object key to
/// read it from and the checksum to verify it against before caching it locally.
#[derive(Debug, Clone)]
struct SharedSlabAddress {
    key: String,
    byte_size: u64,
    sha256: String,
}

/// Lazy read-through source backing reference-parity recovery on the shared-filesystem
/// (`FileObjectStore`) backend. Holds the checkpoint's slab address map (slab id ->
/// shared object key) with no slab bytes, and resolves each slab on demand to a
/// synchronous filesystem read of the shared store, verifying the checkpoint
/// checksum before returning the bytes. The block store installs (caches) the
/// returned bytes locally, so each shared slab is fetched at most once and only when
/// a read actually needs it — never eagerly at recovery time.
#[derive(Debug)]
pub struct SharedPathSlabSource {
    object_store: Arc<FileObjectStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

impl SharedPathSlabSource {
    fn new(object_store: Arc<FileObjectStore>, slabs: BTreeMap<u64, SharedSlabAddress>) -> Self {
        Self {
            object_store,
            slabs,
        }
    }

    /// Number of slabs this source can serve lazily (i.e. the checkpoint's slab
    /// count). Useful for asserting that recovery installed an address map, not bytes.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }
}

impl SharedSlabSource for SharedPathSlabSource {
    fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&page_slab_id) else {
            return Ok(None);
        };
        let path = self.object_store.object_path(&address.key).map_err(|err| {
            BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                err.to_string(),
            ))
        })?;
        let bytes = std::fs::read(&path)?;
        // Parity with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                page_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

impl SharedStoreReplicator<FileObjectStore> {
    /// Reference-parity LAZY restore for the shared-filesystem backend: install the served
    /// INDEX and a per-slab shared address map WITHOUT downloading any slab bytes.
    /// Old (pre-checkpoint) pages are then read lazily through [`SharedPathSlabSource`]
    /// on the first read that needs them. The caller replays only the WAL tail after
    /// `manifest.checkpoint_wal_index`, so recovery cost is O(index + recent WAL),
    /// not O(full history + all slabs). Returns `CheckpointNotFound` when no
    /// checkpoint exists so the caller can fall back to a full WAL replay.
    pub async fn restore_index_and_page_addresses(
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
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        engine.install_index_bytes(manifest.shard_id, &index)?;

        let mut slabs = BTreeMap::new();
        let mut max_slab_id = 0u64;
        for slab in &manifest.page_slabs {
            max_slab_id = max_slab_id.max(slab.page_slab_id);
            slabs.insert(
                slab.page_slab_id,
                SharedSlabAddress {
                    key: slab.key.clone(),
                    byte_size: slab.byte_size,
                    sha256: slab.sha256.clone(),
                },
            );
        }
        let source = Arc::new(SharedPathSlabSource::new(
            Arc::clone(&self.object_store),
            slabs,
        ));
        block_store.attach_shared_slab_source(source);
        // Roll local appends past the checkpoint's slab/page-id range so replayed WAL-tail
        // and new writes never overwrite a slab still served lazily from shared storage.
        if !manifest.page_slabs.is_empty() {
            block_store.reserve_lazy_checkpoint_range(max_slab_id, manifest.next_page_id)?;
        }
        Ok(manifest)
    }
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
        let wal_index = self.next_wal_index.fetch_add(1, Ordering::Relaxed);
        let entry = SharedStoreWalEntry {
            shard_id,
            wal_index,
            command,
        };
        match self.mode {
            SharedStoreStorageMode::Sync => {
                let receipt = self.replicator.publish_wal_entry(entry).await?;
                Ok(SharedStoreWriteReport {
                    wal_index,
                    published: true,
                    queued: false,
                    wal_blob_start_offset: receipt.as_ref().map(|receipt| receipt.start_offset),
                    wal_blob_end_offset: receipt.as_ref().map(|receipt| receipt.end_offset),
                    wal_blob_bytes_written: receipt.as_ref().map(|receipt| receipt.bytes_written),
                    wal_blob_object_length: receipt.as_ref().map(|receipt| receipt.object_length),
                })
            }
            SharedStoreStorageMode::Async => {
                self.pending
                    .lock()
                    .expect("shared-store async queue lock poisoned")
                    .push_back(entry);
                Ok(SharedStoreWriteReport {
                    wal_index,
                    published: false,
                    queued: true,
                    wal_blob_start_offset: None,
                    wal_blob_end_offset: None,
                    wal_blob_bytes_written: None,
                    wal_blob_object_length: None,
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

        let mut last_wal_index = 0;
        let mut last_receipt = None;
        for (index, entry) in drained.iter().cloned().enumerate() {
            last_wal_index = entry.wal_index;
            match self.replicator.publish_wal_entry(entry).await {
                Ok(receipt) => last_receipt = receipt,
                Err(err) => {
                    let mut pending = self
                        .pending
                        .lock()
                        .expect("shared-store async queue lock poisoned");
                    for entry in drained[index..].iter().rev().cloned() {
                        pending.push_front(entry);
                    }
                    return Err(err);
                }
            }
        }
        let remaining = self.queued_len();
        Ok(SharedStoreFlushReport {
            flushed: drained.len(),
            remaining,
            last_wal_index,
            last_wal_blob_start_offset: last_receipt.as_ref().map(|receipt| receipt.start_offset),
            last_wal_blob_end_offset: last_receipt.as_ref().map(|receipt| receipt.end_offset),
            last_wal_blob_object_length: last_receipt.as_ref().map(|receipt| receipt.object_length),
        })
    }
}

fn parse_page_slab_id(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("page_segment_")?
        .strip_suffix(".seg")?
        .parse()
        .ok()
}

fn parse_wal_index(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("wal_")?
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

const WAL_COMMAND_ENCODING_JSON_SERDE: u32 = 0;
const WAL_COMMAND_ENCODING_SDK_PROTO: u32 = 1;

struct WalCommandMetadata {
    byte_size: u64,
    sha256: String,
    encoding: u32,
}

fn wal_command_metadata(
    command: &Command,
) -> Result<WalCommandMetadata, SharedStoreReplicationError> {
    let (command_payload, command_encoding) = match command_to_sdk_proto(command) {
        Some(command) => (command.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
        None => (
            serde_json::to_vec(command)?,
            WAL_COMMAND_ENCODING_JSON_SERDE,
        ),
    };
    Ok(WalCommandMetadata {
        byte_size: command_payload.len() as u64,
        sha256: sha256_hex(&command_payload),
        encoding: command_encoding,
    })
}

fn encode_wal_proto_frame(
    entry: &SharedStoreWalEntry,
) -> Result<Vec<u8>, SharedStoreReplicationError> {
    let (command_payload, command_encoding) = match command_to_sdk_proto(&entry.command) {
        Some(command) => (command.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
        None => (
            serde_json::to_vec(&entry.command)?,
            WAL_COMMAND_ENCODING_JSON_SERDE,
        ),
    };
    let frame = SharedStoreWalFrameProto {
        shard_id: entry.shard_id,
        wal_index: entry.wal_index,
        command_byte_size: command_payload.len() as u64,
        command_sha256: sha256_hex(&command_payload),
        command_payload,
        command_encoding,
    };
    let mut encoded = frame.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    Ok(out)
}

fn encode_wal_offset_metadata_frame(metadata: &SharedStoreWalOffsetMetadata) -> Vec<u8> {
    let proto = SharedStoreWalOffsetMetadataProto {
        shard_id: metadata.shard_id,
        wal_index: metadata.wal_index,
        wal_blob_key: metadata.wal_blob_key.clone(),
        wal_blob_start_offset: metadata.wal_blob_start_offset,
        wal_blob_end_offset: metadata.wal_blob_end_offset,
        wal_blob_bytes_written: metadata.wal_blob_bytes_written,
        wal_blob_object_length: metadata.wal_blob_object_length,
        command_byte_size: metadata.command_byte_size,
        command_sha256: metadata.command_sha256.clone(),
        command_encoding: metadata.command_encoding,
        wal_blob_physical_band_count: metadata.wal_blob_physical_band_count,
        wal_blob_first_physical_offset: metadata.wal_blob_first_physical_offset,
    };
    let mut encoded = proto.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    out
}

fn decode_wal_proto_frame_exact(
    bytes: &[u8],
    checksum_source: &str,
) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
    if bytes.len() < 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated protobuf WAL frame length",
        )));
    }
    let len = u32::from_be_bytes(
        bytes[0..4]
            .try_into()
            .expect("length slice is exactly 4 bytes"),
    ) as usize;
    if bytes.len() != len + 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "protobuf WAL frame range does not match frame length",
        )));
    }
    let frame = SharedStoreWalFrameProto::decode(&bytes[4..])?;
    verify_checksum(
        checksum_source,
        &frame.command_payload,
        frame.command_byte_size,
        &frame.command_sha256,
    )?;
    let command = match frame.command_encoding {
        WAL_COMMAND_ENCODING_SDK_PROTO => {
            let command = v1::Command::decode(frame.command_payload.as_slice())?;
            sdk::sdk_command_to_types(command).map_err(|status| {
                SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    status.to_string(),
                ))
            })?
        }
        WAL_COMMAND_ENCODING_JSON_SERDE => serde_json::from_slice(&frame.command_payload)?,
        other => {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported protobuf WAL command encoding {other}"),
            )));
        }
    };
    Ok(SharedStoreWalEntry {
        shard_id: frame.shard_id,
        wal_index: frame.wal_index,
        command,
    })
}

fn decode_wal_proto_blob(
    bytes: &[u8],
) -> Result<Vec<SharedStoreWalEntry>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = Vec::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame payload",
            )));
        }
        let entry =
            decode_wal_proto_frame_exact(&bytes[cursor - 4..cursor + len], "protobuf-wal-blob")?;
        cursor += len;
        entries.push(entry);
    }
    Ok(entries)
}

fn decode_wal_offset_metadata_blob(
    bytes: &[u8],
) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = BTreeMap::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame payload",
            )));
        }
        let frame = SharedStoreWalOffsetMetadataProto::decode(&bytes[cursor..cursor + len])?;
        cursor += len;
        entries.insert(
            frame.wal_index,
            SharedStoreWalOffsetMetadata {
                shard_id: frame.shard_id,
                wal_index: frame.wal_index,
                wal_blob_key: frame.wal_blob_key,
                wal_blob_start_offset: frame.wal_blob_start_offset,
                wal_blob_end_offset: frame.wal_blob_end_offset,
                wal_blob_bytes_written: frame.wal_blob_bytes_written,
                wal_blob_object_length: frame.wal_blob_object_length,
                wal_blob_physical_band_count: frame.wal_blob_physical_band_count,
                wal_blob_first_physical_offset: frame.wal_blob_first_physical_offset,
                command_byte_size: frame.command_byte_size,
                command_sha256: frame.command_sha256,
                command_encoding: frame.command_encoding,
            },
        );
    }
    Ok(entries)
}

fn command_to_sdk_proto(command: &Command) -> Option<v1::Command> {
    let kind = match command {
        Command::CommonExpire { key, ttl_ms } => {
            v1::command::Kind::CommonExpire(v1::CommonExpire {
                key: key.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::CommonExists { key } => {
            v1::command::Kind::CommonExists(v1::CommonExists { key: key.clone() })
        }
        Command::StringSet { key, value } => v1::command::Kind::StringSet(v1::StringSet {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: 0,
        }),
        Command::StringSetEx { key, value, ttl_ms } => {
            v1::command::Kind::StringSet(v1::StringSet {
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::StringGet { key } => {
            v1::command::Kind::StringGet(v1::StringGet { key: key.clone() })
        }
        Command::StringDelete { key } => {
            v1::command::Kind::StringDelete(v1::StringDelete { key: key.clone() })
        }
        Command::HashSet { key, field, value } => v1::command::Kind::HashSet(v1::HashSet {
            key: key.clone(),
            field: field.clone(),
            value: value.clone(),
        }),
        Command::HashGet { key, field } => v1::command::Kind::HashGet(v1::HashGet {
            key: key.clone(),
            field: field.clone(),
        }),
        Command::HashMultiSet { key, entries } => {
            v1::command::Kind::HashMultiSet(v1::HashMultiSet {
                key: key.clone(),
                entries: entries
                    .iter()
                    .map(|(field, value)| v1::HashEntry {
                        field: field.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
        }
        Command::HashMultiGet { key, fields } => {
            v1::command::Kind::HashMultiGet(v1::HashMultiGet {
                key: key.clone(),
                fields: fields.clone(),
            })
        }
        Command::SetAdd { key, member } => v1::command::Kind::SetAdd(v1::SetAdd {
            key: key.clone(),
            member: member.clone(),
        }),
        Command::SetMembers { key } => {
            v1::command::Kind::SetMembers(v1::SetMembers { key: key.clone() })
        }
        Command::FeatureAppend { key, points } => {
            v1::command::Kind::FeatureAppend(v1::FeatureAppend {
                key: key.clone(),
                points: points
                    .iter()
                    .map(|point| v1::FeaturePoint {
                        timestamp_ms: point.timestamp_ms,
                        value: point.value.clone(),
                    })
                    .collect(),
            })
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => v1::command::Kind::FeatureQuery(v1::FeatureQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: count.unwrap_or(0).min(u32::MAX as usize) as u32,
        }),
        Command::SequenceAdd { key, rows } => {
            v1::command::Kind::SequenceAppend(v1::SequenceAppend {
                key: key.clone(),
                rows: rows
                    .iter()
                    .map(|row| v1::SequenceFeatureRow {
                        timestamp_ms: row.timestamp_ms,
                        gid: row.gid,
                        action_type: row.action_type,
                        duration: row.duration,
                        author_id: row.author_id,
                    })
                    .collect(),
            })
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } if filters.is_empty() => v1::command::Kind::SequenceQuery(v1::SequenceQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: (*count).min(u32::MAX as usize) as u32,
        }),
        Command::ControlStateIncrement {
            key,
            timestamp_ms,
            amount,
        } => v1::command::Kind::ControlStateIncrement(v1::ControlStateIncrement {
            key: key.clone(),
            family: String::new(),
            delta: *amount,
            timestamp_ms: *timestamp_ms,
        }),
        Command::ControlStateQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => v1::command::Kind::ControlStateQuery(v1::ControlStateQuery {
            key: key.clone(),
            family: aggregator.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
        }),
        _ => return None,
    };
    Some(v1::Command { kind: Some(kind) })
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
    async fn shared_store_restores_index_pages_and_replays_later_wal() {
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
            .publish_page_slabs(1, &primary.block_store())
            .await
            .unwrap();
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
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

        let report = replicator.replay_wal(1, 1, &follower).await.unwrap();
        assert_eq!(
            report,
            ReplayReport {
                applied: 1,
                last_wal_index: 2,
                offset_index_reads: 0,
                range_bytes_read: 0,
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
    async fn shared_store_lazy_restore_reads_old_page_on_demand() {
        // Reference-parity lazy recovery: a fresh node with ONLY shared storage restores the
        // served index + a slab ADDRESS map (no slab bytes), replays the WAL tail, and
        // fetches an old (pre-checkpoint) slab ON DEMAND the first time a read needs it.
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
        // Publish a real metadata+slab checkpoint at WAL index 1, then a WAL tail entry.
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert!(
            !manifest.page_slabs.is_empty(),
            "checkpoint must upload the slab bytes so a lazy owner can fetch them"
        );
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                },
            })
            .await
            .unwrap();

        // Fresh owner: no local slabs at all.
        let follower = test_engine(dir.path(), "follower");
        assert!(follower.block_store().slab_ids().unwrap().is_empty());

        // Lazy restore: index + address map only, NO slab bytes installed up front.
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored.checkpoint_wal_index, 1);
        assert!(
            follower.block_store().has_shared_slab_source(),
            "restore must attach a shared read-through source"
        );
        // Proof of laziness: the only local slab is the reserved (empty) append slab;
        // the checkpoint's slab 0 was NOT installed, and nothing was fetched yet.
        assert!(
            !follower.block_store().slab_ids().unwrap().contains(&0),
            "checkpoint slab 0 must not be installed eagerly"
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        follower.load_shard(1);
        // Replay only the WAL tail after the checkpoint index: applies exactly "after".
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(report.applied, 1);
        assert_eq!(report.last_wal_index, 2);
        // The tail write did not touch any pre-checkpoint slab, so still no lazy fetch.
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        // Reading the OLD key triggers exactly ONE on-demand slab fetch and is correct.
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
        assert_eq!(
            follower.block_store().stats().shared_slab_fetches,
            1,
            "reading one old key must fetch exactly one slab on demand (not all slabs up front)"
        );

        // The fetched slab is now cached locally; a second read does NOT re-fetch.
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
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 1);

        // The WAL-tail value is served from the reserved local slab.
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
    async fn shared_store_lazy_restore_replays_only_wal_tail() {
        // WAL-tail replay is O(recent): applied count == number of post-checkpoint
        // records, not the full history.
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
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_wal_index, 1);

        // Three post-checkpoint tail records.
        for (wal_index, key) in [(2u64, "k2"), (3, "k3"), (4, "k4")] {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(
            report.applied, 3,
            "replay applies only the 3 post-checkpoint records, not the full history"
        );
        assert_eq!(report.last_wal_index, 4);
        for key in ["k2", "k3", "k4"] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes {
                    value: Some(key.as_bytes().to_vec())
                }
            );
        }
    }

    #[tokio::test]
    async fn shared_store_lazy_restore_no_checkpoint_falls_back_to_full_replay() {
        // Backward compatible: with no checkpoint published, the lazy restore reports
        // CheckpointNotFound and the caller replays the full shared WAL from 0.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        for (wal_index, key, value) in [
            (1u64, "a", b"1".to_vec()),
            (2, "b", b"2".to_vec()),
        ] {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let after = match replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
        {
            Err(SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            other => panic!("expected CheckpointNotFound, got {other:?}"),
        };
        assert!(!follower.block_store().has_shared_slab_source());
        let report = replicator.replay_wal(1, after, &follower).await.unwrap();
        assert_eq!(report.applied, 2);
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "a".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"1".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn shared_store_checkpoint_rejects_corrupt_page_slab() {
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
                &manifest.page_slabs[0].key,
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
                wal_index: 2,
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
                wal_index: 1,
                published: true,
                queued: false,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
            }
        );

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let replay = replicator
            .replay_wal_strict_with_cursor(1, &follower)
            .await
            .unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 1,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        assert_eq!(
            replicator
                .load_replay_cursor(1)
                .await
                .unwrap()
                .last_wal_index,
            1
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
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
                .replay_wal_strict(1, 0, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_wal_index: 0,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );

        assert_eq!(
            writer.flush_pending(1).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 1,
                last_wal_index: 1,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );

        assert_eq!(
            writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_wal_index: 2,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_wal_index: 2,
                offset_index_reads: 0,
                range_bytes_read: 0,
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
        assert_eq!(manifest.checkpoint_wal_index, 1);
        assert!(!manifest.page_slabs.is_empty());

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
                wal_index: 2,
                published: true,
                queued: false,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
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
                wal_index: 3,
                published: false,
                queued: true,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
            }
        );
        assert_eq!(
            async_writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_wal_index: 3,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );

        let follower = test_engine_with_cache(dir.path(), "follower", 32);
        replicator
            .restore_checkpoint(&manifest, &follower, &follower.block_store())
            .await
            .unwrap();
        // Capture the baseline BEFORE load_shard: eager cache warm-on-load (default ON,
        // MATRIXARK_EAGER_CACHE_WARM_ON_LOAD) reads the checkpointed disk-backed segments into
        // cache during load, so the subsequent StringGet may be served from that warm cache.
        // Counting from before load makes the assertion robust to warm-on-load either way.
        let reads_before = follower.block_store().stats().reads;
        follower.load_shard(1);
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
            "restored follower should read checkpointed bytes from disk-backed block segments \
             (via eager cache warm on load and/or the read)"
        );

        let replay = replicator.replay_wal_strict(1, 1, &follower).await.unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 2,
                last_wal_index: 3,
                offset_index_reads: 0,
                range_bytes_read: 0,
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
                last_wal_index: replay.last_wal_index,
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
                cursor_wal_index: 3,
                retain_from_wal_index: 5,
            }
        ));
    }

    #[tokio::test]
    async fn shared_store_rejects_corrupt_wal_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .await
            .unwrap();

        let key = "cluster-a/shards/1/shared/wal/wal_00000000000000000001.json";
        let mut object: SharedStoreWalObject =
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
                .replay_wal_strict(1, 0, &follower)
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
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 1,
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
                last_wal_index: 1,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
    }

    #[tokio::test]
    async fn shared_store_gc_removes_old_wal_and_checkpoint_generations() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        let (store, replicator) = test_shared_store(dir.path());

        for wal_index in 1..=3 {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Command::StringSet {
                        key: format!("k{wal_index}"),
                        value: vec![wal_index as u8],
                    },
                })
                .await
                .unwrap();
        }
        let wal_gc = replicator.gc_wal_before(1, 3).await.unwrap();
        assert_eq!(wal_gc.deleted_wal_objects, 2);
        let wal_keys = store
            .list("cluster-a/shards/1/shared/wal/")
            .await
            .unwrap();
        assert_eq!(wal_keys.len(), 1);
        assert!(wal_keys[0].ends_with("wal_00000000000000000003.json"));

        for checkpoint_wal_index in 1..=3 {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("checkpoint-{checkpoint_wal_index}"),
                    value: vec![checkpoint_wal_index as u8],
                },
            });
            replicator
                .publish_checkpoint(1, checkpoint_wal_index, &primary, &primary.block_store())
                .await
                .unwrap();
        }
        let checkpoint_gc = replicator.gc_checkpoints(1, 1).await.unwrap();
        assert_eq!(checkpoint_gc.deleted_checkpoints, 2);
        assert_eq!(checkpoint_gc.retained_checkpoint_ids.len(), 1);
        assert_eq!(replicator.list_checkpoints(1).await.unwrap().len(), 1);
    }

    #[cfg(feature = "matrixobject")]
    #[tokio::test]
    async fn shared_store_protobuf_append_blob_replays_matrixobject_blob() {
        use crate::matrixobject_store::MatrixObjectObjectStore;
        use matrixobjectstore_rs::StoreOptions;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            MatrixObjectObjectStore::new(
                "temporalstore-shared",
                StoreOptions {
                    segment_size: 16,
                    max_extent_bytes: 4,
                    chunk_size: 4,
                    ..StoreOptions::default()
                },
            )
            .unwrap(),
        );
        let replicator = SharedStoreReplicator::new("cluster-a", store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

        let mut append_receipts = Vec::new();
        for (wal_index, key, value) in [
            (1, "proto-a", b"one".to_vec()),
            (2, "proto-b", b"two".to_vec()),
        ] {
            let receipt = replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .await
                .unwrap();
            append_receipts.push(receipt.expect("protobuf append blob should return offsets"));
        }
        assert_eq!(append_receipts[0].start_offset, 0);
        assert_eq!(
            append_receipts[0].end_offset,
            append_receipts[1].start_offset
        );
        assert_eq!(
            append_receipts[1].end_offset,
            append_receipts[1].object_length
        );
        assert!(append_receipts[1].physical_band_count > 1);

        let blob_key = "cluster-a/shards/1/shared/wal/wal.protobuf.blob";
        let offset_index_key = "cluster-a/shards/1/shared/wal/wal.offset_index.protobuf.blob";
        assert_eq!(
            store
                .list("cluster-a/shards/1/shared/wal/")
                .await
                .unwrap(),
            vec![offset_index_key.to_string(), blob_key.to_string()]
        );
        assert!(!store.get(blob_key).await.unwrap().is_empty());
        assert!(!store.get(offset_index_key).await.unwrap().is_empty());
        let offset_metadata = replicator.load_wal_offset_metadata(1).await.unwrap();
        assert_eq!(offset_metadata.len(), append_receipts.len());
        for (index, receipt) in append_receipts.iter().enumerate() {
            let metadata = offset_metadata
                .get(&(index as u64 + 1))
                .expect("offset metadata by wal index");
            assert_eq!(metadata.wal_blob_key, blob_key);
            assert_eq!(metadata.wal_blob_start_offset, receipt.start_offset);
            assert_eq!(metadata.wal_blob_end_offset, receipt.end_offset);
            assert_eq!(metadata.wal_blob_bytes_written, receipt.bytes_written);
        }
        let matrixobject_blob = store
            .inner()
            .lock()
            .expect("matrixobject lock poisoned")
            .get_object("temporalstore-shared", blob_key)
            .unwrap();
        assert!(
            matrixobject_blob.metadata.extents.len() > 1,
            "protobuf WAL frames should be appended into one MatrixObject blob"
        );

        let restarted = SharedStoreReplicator::new("cluster-a", store)
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert_eq!(
            restarted.replay_wal_strict(1, 0, &follower).await.unwrap(),
            ReplayReport {
                applied: 2,
                last_wal_index: 2,
                offset_index_reads: 2,
                range_bytes_read: append_receipts
                    .iter()
                    .map(|receipt| receipt.bytes_written)
                    .sum(),
            }
        );
        for (key, value) in [("proto-a", b"one".to_vec()), ("proto-b", b"two".to_vec())] {
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
