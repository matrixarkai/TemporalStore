// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

use crate::metrics::SnapshotMetrics;
use crate::object_store::{ObjectStore, ObjectStoreError};
use crate::types::{
    ChecksumEntry, CompressionFormat, LocalSnapshot, BlockSlabManifest, ShardId, SnapshotManifest,
    SnapshotRef, SnapshotRetention, SnapshotStatus,
};

/// THE FIRST BYTE OF A BINARY MANIFEST.
///
/// A JSON manifest starts with `{` (0x7B), optionally behind JSON whitespace -- and this writer
/// uses `to_vec_pretty`, so that whitespace is really in the durable bytes. 0x01 is not a brace,
/// not whitespace and not valid JSON in any position, so one byte decides the shape.
pub(crate) const MANIFEST_CONTAINER_PROTOBUF: u8 = 0x01;

/// The binary body of a manifest, or `None` when the bytes are the JSON shape.
///
/// THE ONLY PLACE THAT DECIDES, so the four readers cannot tell the shapes apart four different
/// ways.
fn manifest_protobuf_body(bytes: &[u8]) -> Option<&[u8]> {
    match bytes.split_first() {
        Some((&MANIFEST_CONTAINER_PROTOBUF, body)) => Some(body),
        _ => None,
    }
}

// THE MANIFEST WRITER IS UNCONDITIONAL AS OF THIS CHANGE, AND NO JSON ENCODE PATH IS LEFT.
//
// `TS_SNAPSHOT_MANIFEST_PROTOBUF_ENABLED` and the per-thread override that went with it are
// gone, along with `encode_manifest`'s `serde_json::to_vec_pretty` arm. The gate existed so the
// binary writer could land while the reader rolled out; the reader has shipped and has never
// consulted the gate, so there is nothing left for it to protect.
//
// THE ENCODER IS DELETED AND THE DECODER IS KEPT, FOR OPPOSITE REASONS. An encoder that no
// caller reaches is how a retired format comes back -- some future branch finds the function and
// calls it. A decoder with no caller in THIS tree is still reached by every snapshot already on
// disk: the hold is on the installed base, not on which writer runs today. See the note above
// `decode_manifest`, which decides per document from the first byte and never from a setting.

fn compression_to_proto(format: CompressionFormat) -> crate::manifest_proto::CompressionFormat {
    match format {
        CompressionFormat::None => crate::manifest_proto::CompressionFormat::None,
        CompressionFormat::Lz4 => crate::manifest_proto::CompressionFormat::Lz4,
        CompressionFormat::Zstd => crate::manifest_proto::CompressionFormat::Zstd,
    }
}

/// `None` is a real choice here, so UNSPECIFIED is refused rather than read as "no compression".
/// Defaulting it would hand a reader uncompressed bytes for a snapshot a newer build compressed.
fn compression_from_proto(value: i32) -> Result<CompressionFormat, SnapshotStoreError> {
    match crate::manifest_proto::CompressionFormat::try_from(value) {
        Ok(crate::manifest_proto::CompressionFormat::None) => Ok(CompressionFormat::None),
        Ok(crate::manifest_proto::CompressionFormat::Lz4) => Ok(CompressionFormat::Lz4),
        Ok(crate::manifest_proto::CompressionFormat::Zstd) => Ok(CompressionFormat::Zstd),
        Ok(crate::manifest_proto::CompressionFormat::Unspecified) | Err(_) => {
            Err(SnapshotStoreError::Io(std::io::Error::other(format!(
                "snapshot manifest names compression {value}, which this build cannot read"
            ))))
        }
    }
}

/// Decode a manifest in EITHER shape.
///
/// Every reader goes through this. The JSON arm is what every snapshot written before the writer
/// flip carries, and removing it would make those snapshots unreadable -- which is why it stays
/// PERMANENTLY, now that no writer in this tree can produce that shape.
///
/// THE HOLD RULE: a reader stays if ANY build that ever shipped could have written data a live
/// store still holds. "Which writer runs today" is the wrong question -- this writer no longer
/// writes JSON and that is exactly why the arm below can never be deleted from evidence found in
/// this tree. The shape is decided per document from the first byte, never from a setting, so a
/// node that has never written a JSON manifest still reads one correctly.
fn decode_manifest(bytes: &[u8]) -> Result<SnapshotManifest, SnapshotStoreError> {
    let Some(body) = manifest_protobuf_body(bytes) else {
        return Ok(serde_json::from_slice(bytes)?);
    };
    let proto = <crate::manifest_proto::SnapshotManifest as prost::Message>::decode(body)
        .map_err(|err| SnapshotStoreError::Io(std::io::Error::other(err)))?;
    let created_at = proto
        .created_at
        .parse::<chrono::DateTime<Utc>>()
        .map_err(|err| {
            SnapshotStoreError::Io(std::io::Error::other(format!(
                "snapshot manifest carries an unreadable created_at {:?}: {err}",
                proto.created_at
            )))
        })?;
    Ok(SnapshotManifest {
        cluster_id: proto.cluster_id,
        shard_id: proto.shard_id,
        snapshot_id: proto.snapshot_id,
        raft_term: proto.raft_term,
        last_log_index: proto.last_log_index,
        last_applied_log_id: proto.last_applied_log_id,
        created_at,
        engine_version: proto.engine_version,
        // ORDER PRESERVED, deliberately: the published digest folds these in iteration order.
        block_slabs: proto
            .block_slabs
            .into_iter()
            .map(|slab| BlockSlabManifest {
                block_slab_id: slab.block_slab_id,
                relative_path: slab.relative_path,
                byte_size: slab.byte_size,
                sha256: slab.sha256,
            })
            .collect(),
        object_count: proto.object_count,
        record_count: proto.record_count,
        checksums: proto
            .checksums
            .into_iter()
            .map(|entry| ChecksumEntry {
                relative_path: entry.relative_path,
                sha256: entry.sha256,
                byte_size: entry.byte_size,
            })
            .collect(),
        compression: compression_from_proto(proto.compression)?,
    })
}

/// Encode a manifest. ONE SHAPE: the binary one, with no gate to pick another.
///
/// `SnapshotManifest` still derives `Serialize`, so building JSON from one remains POSSIBLE --
/// the tests below do exactly that to plant a legacy document. What is gone is any path a
/// running node can take to write it. See `decode_manifest` for why the read side keeps its JSON
/// arm permanently.
fn encode_manifest(manifest: &SnapshotManifest) -> Result<Bytes, SnapshotStoreError> {
    let proto = crate::manifest_proto::SnapshotManifest {
        cluster_id: manifest.cluster_id.clone(),
        shard_id: manifest.shard_id,
        snapshot_id: manifest.snapshot_id.clone(),
        raft_term: manifest.raft_term,
        last_log_index: manifest.last_log_index,
        last_applied_log_id: manifest.last_applied_log_id.clone(),
        // The same text the JSON form carries, so the value round-trips exactly.
        created_at: manifest.created_at.to_rfc3339(),
        engine_version: manifest.engine_version.clone(),
        block_slabs: manifest
            .block_slabs
            .iter()
            .map(|slab| crate::manifest_proto::BlockSlabManifest {
                block_slab_id: slab.block_slab_id.clone(),
                relative_path: slab.relative_path.clone(),
                byte_size: slab.byte_size,
                sha256: slab.sha256.clone(),
            })
            .collect(),
        object_count: manifest.object_count,
        record_count: manifest.record_count,
        checksums: manifest
            .checksums
            .iter()
            .map(|entry| crate::manifest_proto::ChecksumEntry {
                relative_path: entry.relative_path.clone(),
                sha256: entry.sha256.clone(),
                byte_size: entry.byte_size,
            })
            .collect(),
        compression: compression_to_proto(manifest.compression) as i32,
    };
    let mut out = Vec::with_capacity(1 + prost::Message::encoded_len(&proto));
    out.push(MANIFEST_CONTAINER_PROTOBUF);
    prost::Message::encode(&proto, &mut out)
        .map_err(|err| SnapshotStoreError::Io(std::io::Error::other(err)))?;
    Ok(Bytes::from(out))
}

const MANIFEST: &str = "manifest.json";
/// The name a manifest takes once its body is binary.
///
/// Same convention as the shared store's binary objects (`wal.protobuf.blob`), rather than a
/// second spelling for the same idea.
const MANIFEST_BINARY: &str = "manifest.protobuf.blob";
const INDEX: &str = "index.bin";
const CHECKSUMS: &str = "checksums.json";

#[derive(Debug, Error)]
pub enum SnapshotStoreError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("snapshot is older than local state: snapshot={snapshot_index}, local={local_index}")]
    StaleSnapshot {
        snapshot_index: u64,
        local_index: u64,
    },
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn create_local_snapshot(
        &self,
        shard_id: ShardId,
        last_log_id: String,
    ) -> Result<LocalSnapshot, SnapshotStoreError>;
    async fn upload_snapshot(
        &self,
        snapshot: LocalSnapshot,
    ) -> Result<SnapshotRef, SnapshotStoreError>;
    async fn download_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError>;
    async fn list_snapshots(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError>;
    async fn delete_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError>;
    async fn verify_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError>;
}

pub struct S3SnapshotStore<O> {
    cluster_id: String,
    engine_version: String,
    local_root: PathBuf,
    object_store: Arc<O>,
    metrics: Option<SnapshotMetrics>,
    retention: SnapshotRetention,
}

impl<O> S3SnapshotStore<O>
where
    O: ObjectStore + 'static,
{
    pub fn new(
        cluster_id: impl Into<String>,
        engine_version: impl Into<String>,
        local_root: impl Into<PathBuf>,
        object_store: Arc<O>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            engine_version: engine_version.into(),
            local_root: local_root.into(),
            object_store,
            metrics: None,
            retention: SnapshotRetention::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: SnapshotMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_retention(mut self, retention: SnapshotRetention) -> Self {
        self.retention = retention;
        self
    }

    pub async fn garbage_collect(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError> {
        let mut snapshots = self.list_snapshots(shard_id).await?;
        snapshots.sort_by_key(|s| s.created_at);
        snapshots.reverse();

        let cutoff = Utc::now() - Duration::seconds(self.retention.keep_newer_than_secs as i64);
        let mut deleted = Vec::new();
        for (idx, snapshot) in snapshots.iter().enumerate() {
            if idx < self.retention.keep_last {
                continue;
            }
            if snapshot.created_at >= cutoff {
                continue;
            }
            self.delete_snapshot(snapshot).await?;
            deleted.push(snapshot.clone());
        }
        Ok(deleted)
    }

    pub async fn install_snapshot_guard(
        &self,
        snapshot_ref: &SnapshotRef,
        local_last_log_index: u64,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        if snapshot_ref.last_log_index < local_last_log_index {
            if let Some(metrics) = &self.metrics {
                metrics.observe_restore(snapshot_ref.shard_id, SnapshotStatus::Failure);
            }
            return Err(SnapshotStoreError::StaleSnapshot {
                snapshot_index: snapshot_ref.last_log_index,
                local_index: local_last_log_index,
            });
        }
        let snapshot = self.download_snapshot(snapshot_ref, destination).await;
        if let Some(metrics) = &self.metrics {
            metrics.observe_restore(
                snapshot_ref.shard_id,
                if snapshot.is_ok() {
                    SnapshotStatus::Success
                } else {
                    SnapshotStatus::Failure
                },
            );
        }
        snapshot
    }

    pub async fn download_snapshot_by_uri(
        &self,
        manifest_uri: &str,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let manifest_key = key_from_uri(manifest_uri)
            .trim_end_matches(MANIFEST)
            .to_string();
        let manifest_bytes = self
            .object_store
            .get(&format!("{manifest_key}{MANIFEST}"))
            .await?;
        let manifest: SnapshotManifest = decode_manifest(&manifest_bytes)?;
        let snapshot_ref =
            snapshot_ref_from_manifest(self.object_store.as_ref(), &manifest).await?;
        self.download_snapshot(&snapshot_ref, destination).await
    }

    pub async fn create_local_snapshot_with_index_bytes(
        &self,
        shard_id: ShardId,
        last_log_id: String,
        index_bytes: Bytes,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let root = self.local_root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(root.join("page_segments")).await?;
        let index_path = root.join(INDEX);
        tokio::fs::write(&index_path, index_bytes).await?;
        let checksums_path = root.join(CHECKSUMS);
        if !checksums_path.exists() {
            tokio::fs::write(&checksums_path, b"[]").await?;
        }

        let block_slabs = list_local_block_slabs(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            block_slabs,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.block_slabs = block_slab_manifests(&snapshot).await?;
        snapshot.manifest.checksums = checksum_entries(&snapshot).await?;
        snapshot.manifest.record_count = snapshot.manifest.checksums.len() as u64;
        snapshot.manifest.object_count = snapshot.manifest.block_slabs.len() as u64;

        if let Some(metrics) = &self.metrics {
            metrics.observe_create(shard_id, SnapshotStatus::Success);
        }
        Ok(snapshot)
    }

    fn snapshot_prefix(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}/snapshots/", self.cluster_id, shard_id)
    }
}

#[async_trait]
impl<O> SnapshotStore for S3SnapshotStore<O>
where
    O: ObjectStore + 'static,
{
    async fn create_local_snapshot(
        &self,
        shard_id: ShardId,
        last_log_id: String,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let root = self.local_root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(root.join("page_segments")).await?;
        let index_path = root.join(INDEX);
        if !index_path.exists() {
            tokio::fs::write(&index_path, []).await?;
        }
        let checksums_path = root.join(CHECKSUMS);
        if !checksums_path.exists() {
            tokio::fs::write(&checksums_path, b"[]").await?;
        }

        let block_slabs = list_local_block_slabs(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            block_slabs,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.block_slabs = block_slab_manifests(&snapshot).await?;
        snapshot.manifest.checksums = checksum_entries(&snapshot).await?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_create(shard_id, SnapshotStatus::Success);
        }
        Ok(snapshot)
    }

    async fn upload_snapshot(
        &self,
        snapshot: LocalSnapshot,
    ) -> Result<SnapshotRef, SnapshotStoreError> {
        let started = Instant::now();
        let stable_prefix = snapshot.manifest.stable_prefix();
        let temp_prefix = format!("{stable_prefix}.tmp-{}/", uuid::Uuid::new_v4());
        let result = upload_snapshot_inner(
            self.object_store.as_ref(),
            &snapshot,
            &stable_prefix,
            &temp_prefix,
        )
        .await;

        match result {
            Ok(snapshot_ref) => {
                if let Some(metrics) = &self.metrics {
                    metrics.observe_upload(
                        snapshot.manifest.shard_id,
                        SnapshotStatus::Success,
                        snapshot_ref.byte_size,
                        started.elapsed().as_secs(),
                    );
                    metrics.set_last_success_log_index(
                        snapshot.manifest.shard_id,
                        snapshot.manifest.last_log_index,
                    );
                }
                Ok(snapshot_ref)
            }
            Err(err) => {
                let _ = delete_prefix(self.object_store.as_ref(), &temp_prefix).await;
                if let Some(metrics) = &self.metrics {
                    metrics.observe_upload(
                        snapshot.manifest.shard_id,
                        SnapshotStatus::Failure,
                        0,
                        started.elapsed().as_secs(),
                    );
                }
                Err(err)
            }
        }
    }

    async fn download_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let started = Instant::now();
        let result =
            download_snapshot_inner(self.object_store.as_ref(), snapshot_ref, destination).await;
        if let Some(metrics) = &self.metrics {
            metrics.observe_download(
                snapshot_ref.shard_id,
                if result.is_ok() {
                    SnapshotStatus::Success
                } else {
                    SnapshotStatus::Failure
                },
                started.elapsed().as_secs(),
            );
        }
        result
    }

    async fn list_snapshots(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError> {
        let prefix = self.snapshot_prefix(shard_id);
        let keys = self.object_store.list(&prefix).await?;
        let mut snapshots = Vec::new();
        for key in keys {
            if !is_snapshot_manifest_key(&key) {
                continue;
            }
            let manifest_bytes = self.object_store.get(&key).await?;
            let manifest: SnapshotManifest = decode_manifest(&manifest_bytes)?;
            snapshots
                .push(snapshot_ref_from_manifest(self.object_store.as_ref(), &manifest).await?);
        }
        snapshots.sort_by_key(|s| (s.last_log_index, s.created_at));
        Ok(snapshots)
    }

    async fn delete_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let prefix = prefix_from_ref(snapshot_ref);
        delete_prefix(self.object_store.as_ref(), &prefix).await?;
        Ok(())
    }

    async fn verify_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let manifest_key = format!("{}{}", prefix_from_ref(snapshot_ref), MANIFEST);
        let manifest_bytes = self.object_store.get(&manifest_key).await?;
        let manifest: SnapshotManifest = decode_manifest(&manifest_bytes)?;
        for entry in &manifest.checksums {
            let key = format!("{}{}", manifest.stable_prefix(), entry.relative_path);
            let bytes = self.object_store.get(&key).await?;
            let actual = sha256_hex(&bytes);
            if actual != entry.sha256 {
                return Err(SnapshotStoreError::ChecksumMismatch {
                    path: entry.relative_path.clone(),
                    expected: entry.sha256.clone(),
                    actual,
                });
            }
        }
        Ok(())
    }
}

async fn upload_snapshot_inner<O: ObjectStore>(
    object_store: &O,
    snapshot: &LocalSnapshot,
    stable_prefix: &str,
    temp_prefix: &str,
) -> Result<SnapshotRef, SnapshotStoreError> {
    put_file(
        object_store,
        &format!("{temp_prefix}{INDEX}"),
        &snapshot.index_path,
    )
    .await?;
    put_file(
        object_store,
        &format!("{temp_prefix}{CHECKSUMS}"),
        &snapshot.checksums_path,
    )
    .await?;
    for block_slab in &snapshot.block_slabs {
        let name = block_slab.file_name().unwrap().to_string_lossy();
        put_file(
            object_store,
            &format!("{temp_prefix}block_segments/{name}"),
            block_slab,
        )
        .await?;
    }

    let temp_keys = object_store.list(temp_prefix).await?;
    for temp_key in temp_keys {
        let suffix = temp_key.trim_start_matches(temp_prefix);
        let bytes = object_store.get(&temp_key).await?;
        object_store
            .put(&format!("{stable_prefix}{suffix}"), bytes)
            .await?;
    }

    let manifest_bytes = encode_manifest(&snapshot.manifest)?;
    object_store
        .put(&format!("{stable_prefix}{MANIFEST}"), manifest_bytes)
        .await?;
    delete_prefix(object_store, temp_prefix).await?;
    snapshot_ref_from_manifest(object_store, &snapshot.manifest).await
}

async fn download_snapshot_inner<O: ObjectStore>(
    object_store: &O,
    snapshot_ref: &SnapshotRef,
    destination: PathBuf,
) -> Result<LocalSnapshot, SnapshotStoreError> {
    let prefix = prefix_from_ref(snapshot_ref);
    let manifest_bytes = object_store.get(&format!("{prefix}{MANIFEST}")).await?;
    let manifest: SnapshotManifest = decode_manifest(&manifest_bytes)?;
    tokio::fs::create_dir_all(destination.join("page_segments")).await?;

    let index_path = destination.join(INDEX);
    write_file(
        &index_path,
        object_store.get(&format!("{prefix}{INDEX}")).await?,
    )
    .await?;
    let checksums_path = destination.join(CHECKSUMS);
    write_file(
        &checksums_path,
        object_store.get(&format!("{prefix}{CHECKSUMS}")).await?,
    )
    .await?;

    let mut block_slabs = Vec::new();
    for slab in &manifest.block_slabs {
        let path = destination.join(&slab.relative_path);
        write_file(
            &path,
            object_store
                .get(&format!("{prefix}{}", slab.relative_path))
                .await?,
        )
        .await?;
        block_slabs.push(path);
    }

    let local = LocalSnapshot {
        manifest,
        root_dir: destination,
        index_path,
        checksums_path,
        block_slabs,
    };
    verify_local_snapshot(&local).await?;
    Ok(local)
}

async fn snapshot_ref_from_manifest<O: ObjectStore>(
    object_store: &O,
    manifest: &SnapshotManifest,
) -> Result<SnapshotRef, SnapshotStoreError> {
    let mut total = 0;
    let mut hasher = Sha256::new();
    for entry in &manifest.checksums {
        total += entry.byte_size;
        hasher.update(entry.sha256.as_bytes());
    }
    Ok(SnapshotRef {
        cluster_id: manifest.cluster_id.clone(),
        shard_id: manifest.shard_id,
        snapshot_id: manifest.snapshot_id.clone(),
        raft_term: manifest.raft_term,
        last_log_index: manifest.last_log_index,
        uri: object_store.uri(&manifest.stable_prefix()),
        byte_size: total,
        checksum: hex::encode(hasher.finalize()),
        created_at: manifest.created_at,
    })
}

async fn put_file<O: ObjectStore>(
    object_store: &O,
    key: &str,
    path: &Path,
) -> Result<(), SnapshotStoreError> {
    object_store
        .put(key, Bytes::from(tokio::fs::read(path).await?))
        .await?;
    Ok(())
}

async fn write_file(path: &Path, bytes: Bytes) -> Result<(), SnapshotStoreError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    file.write_all(&bytes).await?;
    file.flush().await?;
    Ok(())
}

async fn delete_prefix<O: ObjectStore>(
    object_store: &O,
    prefix: &str,
) -> Result<(), SnapshotStoreError> {
    for key in object_store.list(prefix).await? {
        object_store.delete(&key).await?;
    }
    Ok(())
}

async fn list_local_block_slabs(dir: &Path) -> Result<Vec<PathBuf>, SnapshotStoreError> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_file() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

async fn block_slab_manifests(
    snapshot: &LocalSnapshot,
) -> Result<Vec<BlockSlabManifest>, SnapshotStoreError> {
    let mut out = Vec::new();
    for path in &snapshot.block_slabs {
        let bytes = tokio::fs::read(path).await?;
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        out.push(BlockSlabManifest {
            block_slab_id: file_name.trim_end_matches(".seg").to_string(),
            relative_path: format!("block_segments/{file_name}"),
            byte_size: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        });
    }
    Ok(out)
}

async fn checksum_entries(
    snapshot: &LocalSnapshot,
) -> Result<Vec<ChecksumEntry>, SnapshotStoreError> {
    let mut entries = Vec::new();
    for (relative, path) in [
        (INDEX.to_string(), snapshot.index_path.clone()),
        (CHECKSUMS.to_string(), snapshot.checksums_path.clone()),
    ] {
        let bytes = tokio::fs::read(path).await?;
        entries.push(ChecksumEntry {
            relative_path: relative,
            sha256: sha256_hex(&bytes),
            byte_size: bytes.len() as u64,
        });
    }
    for slab in &snapshot.manifest.block_slabs {
        entries.push(ChecksumEntry {
            relative_path: slab.relative_path.clone(),
            sha256: slab.sha256.clone(),
            byte_size: slab.byte_size,
        });
    }
    Ok(entries)
}

async fn verify_local_snapshot(snapshot: &LocalSnapshot) -> Result<(), SnapshotStoreError> {
    for entry in &snapshot.manifest.checksums {
        let path = snapshot.root_dir.join(&entry.relative_path);
        let bytes = tokio::fs::read(&path).await?;
        let actual = sha256_hex(&bytes);
        if actual != entry.sha256 {
            return Err(SnapshotStoreError::ChecksumMismatch {
                path: entry.relative_path.clone(),
                expected: entry.sha256.clone(),
                actual,
            });
        }
    }
    Ok(())
}

/// WHETHER THIS KEY NAMES A SNAPSHOT MANIFEST. THE NAME SAYS WHAT IT IS, NOT HOW IT IS ENCODED.
///
/// A KEY FILTER THAT NAMES A CODEC IS A SILENT SKIP WAITING TO HAPPEN, and `list_snapshots` is
/// the shape where that costs most: it returns a LIST, so a manifest passed over does not read as
/// a missing file -- the shard simply appears to have fewer snapshots, or none. There is no error
/// and no log line.
///
/// What that breaks here specifically: `garbage_collect` is built on this listing, so a snapshot
/// it cannot see is a snapshot it can never collect. The leak grows silently, and `keep_last` is
/// counted against a population that is missing members. `list_snapshots` is also a public trait
/// method, so a consumer outside this crate sees the same short list.
///
/// THE `.tmp-` EXCLUSION IS PART OF THE PREDICATE, NOT A SEPARATE CONCERN. An upload stages under
/// a temp prefix and publishes the manifest last; a temp manifest appearing in the list would be a
/// half-written snapshot presented as a complete one. Widening the suffix without carrying the
/// exclusion across would trade one defect for a worse one.
fn is_snapshot_manifest_key(key: &str) -> bool {
    (key.ends_with(MANIFEST) || key.ends_with(MANIFEST_BINARY)) && !key.contains("/.tmp-")
}

fn prefix_from_ref(snapshot_ref: &SnapshotRef) -> String {
    format!(
        "{}/shards/{}/snapshots/{}-{}-{}/",
        snapshot_ref.cluster_id,
        snapshot_ref.shard_id,
        snapshot_ref.raft_term,
        snapshot_ref.last_log_index,
        snapshot_ref.snapshot_id
    )
}

fn key_from_uri(uri: &str) -> String {
    uri.split_once("://")
        .map(|(_, key)| key)
        .unwrap_or(uri)
        .to_string()
}

fn parse_log_index(last_log_id: &str) -> u64 {
    last_log_id
        .rsplit([':', '-'])
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_default()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;
    use tempfile::TempDir;

    use super::*;
    use crate::object_store::FileObjectStore;

    async fn sample_snapshot(root: &Path, shard_id: ShardId, log_index: u64) -> LocalSnapshot {
        let shard_root = root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(shard_root.join("page_segments"))
            .await
            .unwrap();
        let index = shard_root.join(INDEX);
        let checksums = shard_root.join(CHECKSUMS);
        let slab = shard_root.join("page_segments").join("0001.seg");
        tokio::fs::write(&index, b"index-bytes").await.unwrap();
        tokio::fs::write(&checksums, b"[]").await.unwrap();
        tokio::fs::write(&slab, b"block-segment-bytes")
            .await
            .unwrap();

        let mut local = LocalSnapshot::new(
            "cluster-a",
            shard_id,
            7,
            log_index,
            format!("term:7:index:{log_index}"),
            shard_root,
            index,
            checksums,
            vec![slab],
        );
        local.manifest.block_slabs = block_slab_manifests(&local).await.unwrap();
        local.manifest.checksums = checksum_entries(&local).await.unwrap();
        local
    }

    #[tokio::test]
    async fn upload_writes_manifest_last_and_lists_visible_snapshot() {
        let tmp = TempDir::new().unwrap();
        let object_root = tmp.path().join("objects");
        let store = Arc::new(FileObjectStore::with_uri_scheme(&object_root, "s3"));
        let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = sample_snapshot(&tmp.path().join("source"), 42, 100).await;

        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        assert_eq!(uploaded.shard_id, 42);
        assert_eq!(uploaded.last_log_index, 100);

        let listed = snapshots.list_snapshots(42).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].snapshot_id, uploaded.snapshot_id);
    }

    /// A MANIFEST IS FOUND BY WHAT IT IS, NOT BY WHAT IT IS CALLED.
    ///
    /// The filter read `manifest.json`, so a manifest under any other suffix was skipped -- and a
    /// skip here produces a SHORT LIST, which is a successful call. `garbage_collect` is built on
    /// this listing, so an invisible snapshot is one it can never collect.
    ///
    /// This re-files a REAL uploaded manifest with ONLY the name changed and asserts the COUNT,
    /// because `list_snapshots(..).is_ok()` and an empty `Vec` are both "success".
    /// A manifest with deliberately UNSORTED, NON-UNIQUE-PREFIX entries, so an encoder that
    /// reordered, deduped or re-grouped either list could not produce an equal value.
    fn manifest_with_ordered_entries() -> SnapshotManifest {
        SnapshotManifest {
            cluster_id: "cluster-a".to_string(),
            shard_id: 42,
            snapshot_id: "snap-1".to_string(),
            raft_term: 7,
            last_log_index: 100,
            last_applied_log_id: "term:7:index:100".to_string(),
            created_at: Utc::now(),
            engine_version: "0.1.0".to_string(),
            block_slabs: vec![
                BlockSlabManifest {
                    block_slab_id: "0002".to_string(),
                    relative_path: "page_segments/0002.seg".to_string(),
                    byte_size: 2,
                    sha256: "bb".to_string(),
                },
                BlockSlabManifest {
                    block_slab_id: "0001".to_string(),
                    relative_path: "page_segments/0001.seg".to_string(),
                    byte_size: 1,
                    sha256: "aa".to_string(),
                },
            ],
            object_count: 3,
            record_count: 4,
            checksums: vec![
                ChecksumEntry {
                    relative_path: "zzz".to_string(),
                    sha256: "c3".to_string(),
                    byte_size: 3,
                },
                ChecksumEntry {
                    relative_path: "aaa".to_string(),
                    sha256: "c1".to_string(),
                    byte_size: 1,
                },
                ChecksumEntry {
                    relative_path: "mmm".to_string(),
                    sha256: "c2".to_string(),
                    byte_size: 2,
                },
            ],
            compression: CompressionFormat::Zstd,
        }
    }

    /// BOTH SHAPES DECODE, AND THE ENCODING DOES NOT CHANGE THE VALUE.
    ///
    /// The second half is the one that matters. The published snapshot digest folds the checksum
    /// entries in ITERATION ORDER, so order is part of the value: modelling the list as a proto
    /// `map` -- which is the tidier-looking choice -- would leave every file verifying
    /// individually while the snapshot's own digest changed for every snapshot already written.
    /// Asserting the decoded manifests are EQUAL is stronger than comparing one digest, because
    /// any digest computed from equal manifests is equal.
    #[test]
    fn a_manifest_decodes_from_both_shapes_and_the_encoding_does_not_change_it() {
        let original = manifest_with_ordered_entries();

        // The shape every snapshot on disk today carries.
        let json = serde_json::to_vec_pretty(&original).unwrap();
        assert_eq!(json[0], b'{', "the JSON shape must still start with a brace");
        let from_json = decode_manifest(&json).expect("the JSON shape must still decode");
        assert_eq!(from_json, original, "JSON round trip changed the manifest");

        // The shape THIS build writes, with nothing to turn on.
        let binary = encode_manifest(&original).unwrap();
        assert_eq!(
            binary[0], MANIFEST_CONTAINER_PROTOBUF,
            "the writer is unconditional as of the flip, so this is the only shape it can produce"
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&binary).is_err(),
            "the binary shape must not parse as JSON, or the container byte proves nothing"
        );
        let from_binary = decode_manifest(&binary).expect("the binary shape must decode");
        assert_eq!(from_binary, original, "binary round trip changed the manifest");

        // THE DIGEST-NEUTRALITY ASSERTION: the two shapes decode to the same value, so anything
        // derived from them -- including the published digest -- is the same.
        assert_eq!(from_json, from_binary, "the two shapes decode to different manifests");

        // ORDER, SPELLED OUT. Equality above would catch a reorder, but naming it here is what
        // tells the next person the order is load-bearing rather than incidental.
        assert_eq!(
            from_binary
                .checksums
                .iter()
                .map(|e| e.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["zzz", "aaa", "mmm"],
            "the checksum order is folded into the published digest and must not be sorted"
        );
        assert_eq!(
            from_binary
                .block_slabs
                .iter()
                .map(|s| s.block_slab_id.as_str())
                .collect::<Vec<_>>(),
            vec!["0002", "0001"],
            "the slab order must not be sorted either"
        );
        // And the timestamp survives exactly, which is why it travels as text rather than millis.
        assert_eq!(from_binary.created_at, original.created_at);
    }

    /// AN UNREADABLE COMPRESSION TAG IS REFUSED, NOT DEFAULTED.
    ///
    /// `None` is a real choice, so reading UNSPECIFIED as "no compression" would hand a caller
    /// raw bytes for a snapshot a newer build had compressed.
    #[test]
    fn an_unknown_compression_tag_is_refused_rather_than_defaulted() {
        for (value, expected) in [
            (1, CompressionFormat::None),
            (2, CompressionFormat::Lz4),
            (3, CompressionFormat::Zstd),
        ] {
            assert_eq!(
                compression_from_proto(value).expect("a known tag must decode"),
                expected
            );
        }
        for value in [0, 99, -1] {
            assert!(
                compression_from_proto(value).is_err(),
                "compression tag {value} was accepted, so an unknown tag is being guessed at"
            );
        }
    }

    /// THE WRITER IS BINARY WITH NOTHING TO TURN ON, AND A JSON MANIFEST IS STILL READ.
    ///
    /// Driven through the real upload, list, verify and download paths, because the four readers
    /// are what a running node uses -- a codec that round-trips in isolation proves nothing about
    /// them.
    ///
    /// THE JSON ARM IS DRIVEN ON BYTES PLANTED DIRECTLY INTO THE OBJECT STORE, because the writer
    /// that used to produce them no longer exists. A test that round-tripped through the writer
    /// would have had nothing left to drive after the flip, and would have quietly become a test
    /// of the binary arm twice over. The plant is reconstructed from a REAL uploaded manifest, so
    /// the document is a valid snapshot in every respect except its encoding.
    ///
    /// `list_snapshots` asserts a COUNT rather than an error, because a manifest the listing
    /// cannot see produces a SHORT LIST, which is a successful call.
    ///
    /// NOTE ON THE KEY: the manifest keeps the name `manifest.json` even when its body is binary.
    /// That is deliberate -- it is fetched by an exact key, and deployed binaries open it by that
    /// name, so renaming it is the one-way door. The listing widening landed separately is what
    /// makes a future rename possible, not something this change needs.
    #[tokio::test]
    async fn a_manifest_is_written_binary_and_a_planted_json_one_is_still_read() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots =
            S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store.clone());

        // ---- THE SHIPPED DEFAULT IS THE BINARY SHAPE, with nothing to turn on.
        let local = sample_snapshot(&tmp.path().join("source-default"), 42, 100).await;
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        let key = format!("{}{MANIFEST}", prefix_from_ref(&uploaded));
        let first = store.get(&key).await.unwrap();
        assert_eq!(
            first[0], MANIFEST_CONTAINER_PROTOBUF,
            "the shipped default must write the binary shape as of the flip"
        );
        assert_ne!(
            first[0], b'{',
            "the default still writes a JSON manifest, so the encode arm this change deleted is \
             reachable after all"
        );

        // ---- AND THE INSTALLED BASE IS STILL READ. The JSON bytes are built from a real
        // ---- uploaded manifest and written STRAIGHT INTO THE OBJECT STORE, because the writer
        // ---- that used to produce them has been deleted -- which is the whole point of deleting
        // ---- it. `Serialize` survives on the type, so a test can still build the shape even
        // ---- though no production path can.
        let legacy = sample_snapshot(&tmp.path().join("source-legacy"), 42, 150).await;
        let legacy_ref = snapshots.upload_snapshot(legacy).await.unwrap();
        let legacy_key = format!("{}{MANIFEST}", prefix_from_ref(&legacy_ref));
        let as_binary = store.get(&legacy_key).await.unwrap();
        let as_json = {
            let manifest = decode_manifest(&as_binary).expect("the binary manifest must decode");
            Bytes::from(serde_json::to_vec_pretty(&manifest).expect("the JSON shape must build"))
        };
        assert_eq!(
            as_json[0], b'{',
            "the planted legacy manifest is not the JSON shape, so the reader arm under test is \
             being driven on the wrong bytes"
        );
        store
            .put(&legacy_key, as_json)
            .await
            .expect("planting the legacy manifest must succeed");
        assert_eq!(
            store.get(&legacy_key).await.unwrap()[0],
            b'{',
            "the plant did not take, so nothing below reads a JSON manifest"
        );

        // ---- a second binary snapshot, so the listing walks a MIXTURE of both shapes.
        let local = sample_snapshot(&tmp.path().join("source-proto"), 42, 200).await;
        let binary_ref = snapshots.upload_snapshot(local).await.unwrap();

        let binary_key = format!("{}{MANIFEST}", prefix_from_ref(&binary_ref));
        let on_disk = store.get(&binary_key).await.unwrap();
        assert_eq!(
            on_disk[0], MANIFEST_CONTAINER_PROTOBUF,
            "with the gate on, the manifest must be written in the binary shape"
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&on_disk).is_err(),
            "the binary manifest must not parse as JSON"
        );

        // ---- THE LOAD-BEARING PART: every reader, over a MIXTURE of both shapes on disk.
        let listed = snapshots.list_snapshots(42).await.unwrap();
        assert_eq!(
            listed.len(),
            3,
            "all three snapshots must be listed -- two binary and one planted as JSON: {listed:?}"
        );
        assert!(
            listed.iter().any(|s| s.snapshot_id == binary_ref.snapshot_id),
            "the binary-bodied snapshot is missing from the listing"
        );
        assert!(
            listed
                .iter()
                .any(|s| s.snapshot_id == legacy_ref.snapshot_id),
            "the snapshot whose manifest is JSON ON DISK was not listed, so the decode arm this \
             change keeps has stopped working and every snapshot written before the flip is \
             invisible: {listed:?}"
        );

        snapshots
            .verify_snapshot(&binary_ref)
            .await
            .expect("verify must read the binary manifest");
        snapshots
            .download_snapshot(&binary_ref, tmp.path().join("restore"))
            .await
            .expect("download must read the binary manifest");

        // The same two readers over the PLANTED JSON document. These are separate calls because
        // each fetches the manifest by its own key and decodes it itself; the listing passing
        // says nothing about either.
        snapshots
            .verify_snapshot(&legacy_ref)
            .await
            .expect("verify must still read a JSON manifest");
        snapshots
            .download_snapshot(&legacy_ref, tmp.path().join("restore-legacy"))
            .await
            .expect("download must still read a JSON manifest");
    }

    #[tokio::test]
    async fn list_snapshots_finds_a_manifest_filed_under_the_binary_suffix() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots =
            S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store.clone());
        let local = sample_snapshot(&tmp.path().join("source"), 42, 100).await;
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();

        // CONTROL: it is found under the old name first, so a failure after the rename is
        // attributable to the NAME rather than to a fixture that never worked.
        assert_eq!(snapshots.list_snapshots(42).await.unwrap().len(), 1);

        // DENOMINATOR FIRST: locate the one manifest before renaming it. A prefix that listed
        // nothing would make the rename a no-op and every assertion below vacuous.
        let prefix = "cluster-a/shards/42/snapshots/";
        let manifests: Vec<String> = store
            .list(prefix)
            .await
            .unwrap()
            .into_iter()
            .filter(|key| key.ends_with(MANIFEST))
            .collect();
        assert_eq!(manifests.len(), 1, "expected one manifest under {prefix}");
        let old_key = manifests[0].clone();

        // Only the NAME changes.
        let bytes = store.get(&old_key).await.unwrap();
        let new_key = format!(
            "{}{MANIFEST_BINARY}",
            old_key.strip_suffix(MANIFEST).unwrap()
        );
        store.put(&new_key, bytes).await.unwrap();
        store.delete(&old_key).await.unwrap();

        let listed = snapshots.list_snapshots(42).await.unwrap();
        assert_eq!(
            listed.len(),
            1,
            "the manifest was passed over because of its NAME, and a short list is what a shard \
             with fewer snapshots returns"
        );
        assert_eq!(listed[0].snapshot_id, uploaded.snapshot_id);
    }

    /// THE `.tmp-` EXCLUSION SURVIVES THE WIDENING.
    ///
    /// An upload stages under a temp prefix and publishes the manifest last, so a temp manifest in
    /// the list would be a half-written snapshot presented as a complete one. Widening the suffix
    /// is exactly the change that could drop this, and the old filter spelled the exclusion inline
    /// where it was easy to lose.
    #[tokio::test]
    async fn a_staged_manifest_is_still_excluded_under_either_suffix() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots =
            S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store.clone());
        let local = sample_snapshot(&tmp.path().join("source"), 42, 100).await;
        snapshots.upload_snapshot(local).await.unwrap();
        assert_eq!(snapshots.list_snapshots(42).await.unwrap().len(), 1);

        // Stage a copy of the real manifest under a temp prefix, in BOTH spellings.
        let prefix = "cluster-a/shards/42/snapshots/";
        let real = store
            .list(prefix)
            .await
            .unwrap()
            .into_iter()
            .find(|key| key.ends_with(MANIFEST))
            .expect("one real manifest");
        let bytes = store.get(&real).await.unwrap();
        for name in [MANIFEST, MANIFEST_BINARY] {
            store
                .put(&format!("{prefix}.tmp-staging/{name}"), bytes.clone())
                .await
                .unwrap();
        }

        // DENOMINATOR: the staged objects really are under the prefix the listing walks, or this
        // test would be asserting that nothing is excluded because nothing is there.
        let staged = store
            .list(prefix)
            .await
            .unwrap()
            .into_iter()
            .filter(|k| k.contains("/.tmp-"))
            .count();
        assert_eq!(staged, 2, "both staged manifests must be present to be excluded");

        assert_eq!(
            snapshots.list_snapshots(42).await.unwrap().len(),
            1,
            "a staged manifest reached the list, so a half-written snapshot is being presented \
             as a complete one"
        );
    }

    #[tokio::test]
    async fn download_verifies_and_restores_snapshot_files() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = sample_snapshot(&tmp.path().join("source"), 7, 123).await;
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();

        let restored = snapshots
            .download_snapshot(&uploaded, tmp.path().join("restore"))
            .await
            .unwrap();

        assert_eq!(restored.manifest.shard_id, 7);
        assert_eq!(
            tokio::fs::read(restored.root_dir.join("block_segments/0001.seg"))
                .await
                .unwrap(),
            b"block-segment-bytes"
        );
    }

    #[tokio::test]
    async fn create_with_index_bytes_and_download_by_uri_restore_payload() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::with_uri_scheme(
            tmp.path().join("objects"),
            "s3",
        ));
        let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = snapshots
            .create_local_snapshot_with_index_bytes(
                9,
                "term:4:index:44".to_string(),
                Bytes::from_static(b"raft-snapshot-payload"),
            )
            .await
            .unwrap();
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        let restored = snapshots
            .download_snapshot_by_uri(&uploaded.uri, tmp.path().join("restore-by-uri"))
            .await
            .unwrap();

        assert_eq!(restored.manifest.last_log_index, 44);
        assert_eq!(
            tokio::fs::read(restored.index_path).await.unwrap(),
            b"raft-snapshot-payload"
        );
    }

    #[tokio::test]
    async fn corrupt_snapshot_fails_verification() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots =
            S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store.clone());
        let local = sample_snapshot(&tmp.path().join("source"), 5, 55).await;
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        store
            .put(
                &format!(
                    "cluster-a/shards/5/snapshots/{}-{}-{}/index.bin",
                    uploaded.raft_term, uploaded.last_log_index, uploaded.snapshot_id
                ),
                Bytes::from_static(b"corrupt"),
            )
            .await
            .unwrap();

        let err = snapshots.verify_snapshot(&uploaded).await.unwrap_err();
        assert!(matches!(err, SnapshotStoreError::ChecksumMismatch { .. }));
    }

    #[tokio::test]
    async fn stale_snapshot_install_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(FileObjectStore::new(tmp.path().join("objects")));
        let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = sample_snapshot(&tmp.path().join("source"), 1, 10).await;
        let uploaded = snapshots.upload_snapshot(local).await.unwrap();

        let err = snapshots
            .install_snapshot_guard(&uploaded, 11, tmp.path().join("restore"))
            .await
            .unwrap_err();
        assert!(matches!(err, SnapshotStoreError::StaleSnapshot { .. }));
    }

    #[tokio::test]
    async fn metrics_are_exported_with_prometheus_names() {
        let mut registry = Registry::default();
        let metrics = SnapshotMetrics::register(&mut registry);
        metrics.observe_upload(3, SnapshotStatus::Success, 2048, 2);
        metrics.set_last_success_log_index(3, 99);

        let mut output = String::new();
        encode(&mut output, &registry).unwrap();
        assert!(output.contains("temporalstore_snapshot_upload_total"));
        assert!(output.contains("temporalstore_snapshot_bytes"));
        assert!(output.contains("temporalstore_snapshot_last_success_log_index"));
    }
}
