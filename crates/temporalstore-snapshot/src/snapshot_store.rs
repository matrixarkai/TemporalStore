use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use crate::metrics::SnapshotMetrics;
use crate::object_store::{ObjectStore, ObjectStoreError};
use crate::types::{
    ChecksumEntry, LocalSnapshot, PageSegmentManifest, ShardId, SnapshotManifest, SnapshotRef,
    SnapshotRetention, SnapshotStatus,
};

const MANIFEST: &str = "manifest.json";
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
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
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

        let page_segments = list_local_page_segments(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            page_segments,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.page_segments = page_segment_manifests(&snapshot).await?;
        snapshot.manifest.checksums = checksum_entries(&snapshot).await?;
        snapshot.manifest.record_count = snapshot.manifest.checksums.len() as u64;
        snapshot.manifest.object_count = snapshot.manifest.page_segments.len() as u64;

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

        let page_segments = list_local_page_segments(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            page_segments,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.page_segments = page_segment_manifests(&snapshot).await?;
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
        let result =
            upload_snapshot_inner(Arc::clone(&self.object_store), &snapshot, &stable_prefix).await;

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
                let _ =
                    delete_prefix_concurrent(Arc::clone(&self.object_store), &stable_prefix).await;
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
            download_snapshot_inner(Arc::clone(&self.object_store), snapshot_ref, destination)
                .await;
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
        let mut snapshots =
            list_snapshot_refs_concurrent(Arc::clone(&self.object_store), keys).await?;
        snapshots.sort_by_key(|s| (s.last_log_index, s.created_at));
        Ok(snapshots)
    }

    async fn delete_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let prefix = prefix_from_ref(snapshot_ref);
        delete_prefix_concurrent(Arc::clone(&self.object_store), &prefix).await?;
        Ok(())
    }

    async fn verify_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let manifest_key = format!("{}{}", prefix_from_ref(snapshot_ref), MANIFEST);
        let manifest_bytes = self.object_store.get(&manifest_key).await?;
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        verify_remote_checksum_entries(Arc::clone(&self.object_store), &manifest).await?;
        Ok(())
    }
}

async fn upload_snapshot_inner<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    snapshot: &LocalSnapshot,
    stable_prefix: &str,
) -> Result<SnapshotRef, SnapshotStoreError> {
    let mut upload_files = vec![
        SnapshotUploadFile {
            key: format!("{stable_prefix}{INDEX}"),
            path: snapshot.index_path.clone(),
        },
        SnapshotUploadFile {
            key: format!("{stable_prefix}{CHECKSUMS}"),
            path: snapshot.checksums_path.clone(),
        },
    ];
    for page_segment in &snapshot.page_segments {
        let name = page_segment.file_name().unwrap().to_string_lossy();
        upload_files.push(SnapshotUploadFile {
            key: format!("{stable_prefix}page_segments/{name}"),
            path: page_segment.clone(),
        });
    }
    put_files_unique_concurrent(Arc::clone(&object_store), upload_files).await?;

    let manifest_bytes = Bytes::from(serde_json::to_vec_pretty(&snapshot.manifest)?);
    object_store
        .put_unique(&format!("{stable_prefix}{MANIFEST}"), manifest_bytes)
        .await?;
    snapshot_ref_from_manifest(object_store.as_ref(), &snapshot.manifest).await
}

async fn download_snapshot_inner<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    snapshot_ref: &SnapshotRef,
    destination: PathBuf,
) -> Result<LocalSnapshot, SnapshotStoreError> {
    let prefix = prefix_from_ref(snapshot_ref);
    let manifest_bytes = object_store.get(&format!("{prefix}{MANIFEST}")).await?;
    let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
    tokio::fs::create_dir_all(destination.join("page_segments")).await?;

    let index_path = destination.join(INDEX);
    let checksums_path = destination.join(CHECKSUMS);
    let mut page_segments = Vec::new();
    let mut download_files = vec![
        SnapshotDownloadFile {
            key: format!("{prefix}{INDEX}"),
            path: index_path.clone(),
        },
        SnapshotDownloadFile {
            key: format!("{prefix}{CHECKSUMS}"),
            path: checksums_path.clone(),
        },
    ];
    for segment in &manifest.page_segments {
        let path = destination.join(&segment.relative_path);
        download_files.push(SnapshotDownloadFile {
            key: format!("{prefix}{}", segment.relative_path),
            path: path.clone(),
        });
        page_segments.push(path);
    }
    get_files_concurrent(object_store, download_files).await?;

    let local = LocalSnapshot {
        manifest,
        root_dir: destination,
        index_path,
        checksums_path,
        page_segments,
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

#[derive(Debug, Clone)]
struct SnapshotUploadFile {
    key: String,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct SnapshotDownloadFile {
    key: String,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct SnapshotDeleteObject {
    key: String,
}

async fn put_files_unique_concurrent<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    files: Vec<SnapshotUploadFile>,
) -> Result<(), SnapshotStoreError> {
    let concurrency = snapshot_upload_concurrency();
    let mut join_set = JoinSet::new();
    let mut next_to_submit = 0usize;
    while next_to_submit < files.len() || !join_set.is_empty() {
        while next_to_submit < files.len() && join_set.len() < concurrency {
            let file = files[next_to_submit].clone();
            let store = Arc::clone(&object_store);
            join_set.spawn(async move {
                let bytes = Bytes::from(tokio::fs::read(&file.path).await?);
                store.put_unique(&file.key, bytes).await?;
                Ok::<_, SnapshotStoreError>(())
            });
            next_to_submit += 1;
        }
        join_set
            .join_next()
            .await
            .expect("snapshot upload task missing")
            .map_err(std::io::Error::other)??;
    }
    Ok(())
}

async fn list_snapshot_refs_concurrent<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    keys: Vec<String>,
) -> Result<Vec<SnapshotRef>, SnapshotStoreError> {
    let manifest_keys: Vec<_> = keys
        .into_iter()
        .filter(|key| key.ends_with(MANIFEST) && !key.contains("/.tmp-"))
        .collect();
    let concurrency = snapshot_transfer_concurrency();
    let mut join_set = JoinSet::new();
    let mut next_to_submit = 0usize;
    let mut snapshots = Vec::with_capacity(manifest_keys.len());
    while next_to_submit < manifest_keys.len() || !join_set.is_empty() {
        while next_to_submit < manifest_keys.len() && join_set.len() < concurrency {
            let key = manifest_keys[next_to_submit].clone();
            let store = Arc::clone(&object_store);
            join_set.spawn(async move {
                let manifest_bytes = store.get(&key).await?;
                let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
                snapshot_ref_from_manifest(store.as_ref(), &manifest).await
            });
            next_to_submit += 1;
        }
        let snapshot_ref = join_set
            .join_next()
            .await
            .expect("snapshot list task missing")
            .map_err(std::io::Error::other)??;
        snapshots.push(snapshot_ref);
    }
    Ok(snapshots)
}

async fn get_files_concurrent<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    files: Vec<SnapshotDownloadFile>,
) -> Result<(), SnapshotStoreError> {
    let concurrency = snapshot_transfer_concurrency();
    let mut join_set = JoinSet::new();
    let mut next_to_submit = 0usize;
    while next_to_submit < files.len() || !join_set.is_empty() {
        while next_to_submit < files.len() && join_set.len() < concurrency {
            let file = files[next_to_submit].clone();
            let store = Arc::clone(&object_store);
            join_set.spawn(async move {
                let bytes = store.get(&file.key).await?;
                write_file(&file.path, bytes).await?;
                Ok::<_, SnapshotStoreError>(())
            });
            next_to_submit += 1;
        }
        join_set
            .join_next()
            .await
            .expect("snapshot download task missing")
            .map_err(std::io::Error::other)??;
    }
    Ok(())
}

async fn delete_prefix_concurrent<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    prefix: &str,
) -> Result<(), SnapshotStoreError> {
    let objects: Vec<_> = object_store
        .list(prefix)
        .await?
        .into_iter()
        .map(|key| SnapshotDeleteObject { key })
        .collect();
    let concurrency = snapshot_transfer_concurrency();
    let mut join_set = JoinSet::new();
    let mut next_to_submit = 0usize;
    while next_to_submit < objects.len() || !join_set.is_empty() {
        while next_to_submit < objects.len() && join_set.len() < concurrency {
            let object = objects[next_to_submit].clone();
            let store = Arc::clone(&object_store);
            join_set.spawn(async move {
                store.delete(&object.key).await?;
                Ok::<_, SnapshotStoreError>(())
            });
            next_to_submit += 1;
        }
        join_set
            .join_next()
            .await
            .expect("snapshot delete task missing")
            .map_err(std::io::Error::other)??;
    }
    Ok(())
}

async fn verify_remote_checksum_entries<O: ObjectStore + 'static>(
    object_store: Arc<O>,
    manifest: &SnapshotManifest,
) -> Result<(), SnapshotStoreError> {
    let concurrency = snapshot_transfer_concurrency();
    let prefix = manifest.stable_prefix();
    let entries = manifest.checksums.clone();
    let mut join_set = JoinSet::new();
    let mut next_to_submit = 0usize;
    while next_to_submit < entries.len() || !join_set.is_empty() {
        while next_to_submit < entries.len() && join_set.len() < concurrency {
            let entry = entries[next_to_submit].clone();
            let key = format!("{prefix}{}", entry.relative_path);
            let store = Arc::clone(&object_store);
            join_set.spawn(async move {
                let bytes = store.get(&key).await?;
                let actual = sha256_hex(&bytes);
                if actual != entry.sha256 {
                    return Err(SnapshotStoreError::ChecksumMismatch {
                        path: entry.relative_path,
                        expected: entry.sha256,
                        actual,
                    });
                }
                Ok::<_, SnapshotStoreError>(())
            });
            next_to_submit += 1;
        }
        join_set
            .join_next()
            .await
            .expect("snapshot verify task missing")
            .map_err(std::io::Error::other)??;
    }
    Ok(())
}

fn snapshot_upload_concurrency() -> usize {
    snapshot_transfer_concurrency_from_env("TS_SNAPSHOT_UPLOAD_CONCURRENCY")
}

fn snapshot_transfer_concurrency() -> usize {
    std::env::var("TS_SNAPSHOT_TRANSFER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| snapshot_transfer_concurrency_from_env("TS_SNAPSHOT_UPLOAD_CONCURRENCY"))
}

fn snapshot_transfer_concurrency_from_env(name: &str) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
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

async fn list_local_page_segments(dir: &Path) -> Result<Vec<PathBuf>, SnapshotStoreError> {
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

async fn page_segment_manifests(
    snapshot: &LocalSnapshot,
) -> Result<Vec<PageSegmentManifest>, SnapshotStoreError> {
    let mut out = Vec::new();
    for path in &snapshot.page_segments {
        let bytes = tokio::fs::read(path).await?;
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        out.push(PageSegmentManifest {
            page_segment_id: file_name.trim_end_matches(".seg").to_string(),
            relative_path: format!("page_segments/{file_name}"),
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
    for segment in &snapshot.manifest.page_segments {
        entries.push(ChecksumEntry {
            relative_path: segment.relative_path.clone(),
            sha256: segment.sha256.clone(),
            byte_size: segment.byte_size,
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
    use crate::object_store::{FileObjectStore, MatrixObjectStore, MatrixObjectStoreConfig};

    async fn sample_snapshot(root: &Path, shard_id: ShardId, log_index: u64) -> LocalSnapshot {
        let shard_root = root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(shard_root.join("page_segments"))
            .await
            .unwrap();
        let index = shard_root.join(INDEX);
        let checksums = shard_root.join(CHECKSUMS);
        let segment = shard_root.join("page_segments").join("0001.seg");
        tokio::fs::write(&index, b"index-bytes").await.unwrap();
        tokio::fs::write(&checksums, b"[]").await.unwrap();
        tokio::fs::write(&segment, b"page-segment-bytes")
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
            vec![segment],
        );
        local.manifest.page_segments = page_segment_manifests(&local).await.unwrap();
        local.manifest.checksums = checksum_entries(&local).await.unwrap();
        local
    }

    #[tokio::test]
    async fn upload_writes_manifest_last_and_lists_visible_snapshot() {
        let tmp = TempDir::new().unwrap();
        let object_root = tmp.path().join("objects");
        let store = Arc::new(FileObjectStore::with_uri_scheme(&object_root, "s3"));
        let snapshots =
            S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = sample_snapshot(&tmp.path().join("source"), 42, 100).await;

        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        assert_eq!(uploaded.shard_id, 42);
        assert_eq!(uploaded.last_log_index, 100);

        let listed = snapshots.list_snapshots(42).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].snapshot_id, uploaded.snapshot_id);
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
            tokio::fs::read(restored.root_dir.join("page_segments/0001.seg"))
                .await
                .unwrap(),
            b"page-segment-bytes"
        );
    }

    #[tokio::test]
    async fn matrix_object_store_snapshot_upload_uses_direct_unique_objects() {
        let tmp = TempDir::new().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(tmp.path().join("objects"))
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = Arc::new(MatrixObjectStore::from_config(config));
        let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
        let local = sample_snapshot(&tmp.path().join("source"), 11, 321).await;

        let uploaded = snapshots.upload_snapshot(local).await.unwrap();
        snapshots.verify_snapshot(&uploaded).await.unwrap();
        let listed = snapshots.list_snapshots(11).await.unwrap();
        assert_eq!(listed.len(), 1);
        let restored = snapshots
            .download_snapshot(&uploaded, tmp.path().join("restore-matrixobjectstore"))
            .await
            .unwrap();

        assert_eq!(restored.manifest.last_log_index, 321);
        assert_eq!(
            tokio::fs::read(restored.root_dir.join("page_segments/0001.seg"))
                .await
                .unwrap(),
            b"page-segment-bytes"
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
