use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalstore_snapshot::object_store::{ObjectStore, ObjectStoreError};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::page_store::{LocalPageStore, PageStoreError};
use crate::types::{Command, ExecuteRequest, ShardId, Status};

#[derive(Debug, Error)]
pub enum SharedStoreReplicationError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    PageStore(#[from] PageStoreError),
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
    #[error("replicated command failed at oplog index {oplog_index}: {status:?}")]
    ApplyFailed { oplog_index: u64, status: Status },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreOplogEntry {
    pub shard_id: ShardId,
    pub oplog_index: u64,
    pub command: Command,
}

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

#[derive(Debug, Clone)]
pub struct SharedStoreReplicator<O> {
    cluster_id: String,
    object_store: Arc<O>,
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
        }
    }

    pub async fn publish_oplog_entry(
        &self,
        entry: SharedStoreOplogEntry,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.oplog_key(entry.shard_id, entry.oplog_index);
        self.object_store
            .put(&key, Bytes::from(serde_json::to_vec_pretty(&entry)?))
            .await?;
        Ok(())
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
        page_store: &LocalPageStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let mut published = Vec::new();
        for page_segment_id in page_store.segment_ids()? {
            self.object_store
                .put(
                    &self.page_segment_key(shard_id, page_segment_id),
                    Bytes::from(page_store.read_segment(page_segment_id)?),
                )
                .await?;
            published.push(page_segment_id);
        }
        Ok(published)
    }

    pub async fn publish_checkpoint(
        &self,
        shard_id: ShardId,
        checkpoint_oplog_index: u64,
        engine: &TemporalEngine,
        page_store: &LocalPageStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let prefix = self.checkpoint_prefix(shard_id, &checkpoint_id);
        let index_key = format!("{prefix}index/shard.index.json");
        let index = engine.export_index_bytes(shard_id)?;
        self.object_store
            .put(&index_key, Bytes::from(index.clone()))
            .await?;

        let mut page_segments = Vec::new();
        for page_segment_id in page_store.segment_ids()? {
            let bytes = page_store.read_segment(page_segment_id)?;
            let key = format!("{prefix}page_segments/page_segment_{page_segment_id:020}.seg");
            self.object_store
                .put(&key, Bytes::from(bytes.clone()))
                .await?;
            page_segments.push(SharedStorePageSegment {
                page_segment_id,
                key,
                byte_size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
            });
        }

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
        page_store: &LocalPageStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let index = self.object_store.get(&self.index_key(shard_id)).await?;
        engine.install_index_bytes(shard_id, &index)?;

        let prefix = self.page_segment_prefix(shard_id);
        let mut restored = Vec::new();
        for key in self.object_store.list(&prefix).await? {
            let Some(page_segment_id) = parse_page_segment_id(&key) else {
                continue;
            };
            let bytes = self.object_store.get(&key).await?;
            page_store.install_segment(page_segment_id, &bytes)?;
            restored.push(page_segment_id);
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
            (manifest.checkpoint_oplog_index, manifest.created_at_ms)
        });
        Ok(manifests)
    }

    pub async fn restore_checkpoint(
        &self,
        manifest: &SharedStoreCheckpointManifest,
        engine: &TemporalEngine,
        page_store: &LocalPageStore,
    ) -> Result<(), SharedStoreReplicationError> {
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        engine.install_index_bytes(manifest.shard_id, &index)?;

        for segment in &manifest.page_segments {
            let bytes = self.object_store.get(&segment.key).await?;
            verify_checksum(&segment.key, &bytes, segment.byte_size, &segment.sha256)?;
            page_store.install_segment(segment.page_segment_id, &bytes)?;
        }
        Ok(())
    }

    pub async fn restore_latest_checkpoint(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        page_store: &LocalPageStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        self.restore_checkpoint(&manifest, engine, page_store)
            .await?;
        Ok(manifest)
    }

    pub async fn replay_oplog(
        &self,
        shard_id: ShardId,
        after_oplog_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut keys = self.object_store.list(&self.oplog_prefix(shard_id)).await?;
        keys.sort();

        let mut report = ReplayReport {
            applied: 0,
            last_oplog_index: after_oplog_index,
        };
        for key in keys {
            let Some(oplog_index) = parse_oplog_index(&key) else {
                continue;
            };
            if oplog_index <= after_oplog_index {
                continue;
            }
            let entry: SharedStoreOplogEntry =
                serde_json::from_slice(&self.object_store.get(&key).await?)?;
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
    use std::sync::Arc;

    use bytes::Bytes;
    use temporalstore_snapshot::object_store::FileObjectStore;
    use temporalstore_snapshot::object_store::ObjectStore;

    use super::*;
    use crate::types::CommandResponse;

    #[tokio::test]
    async fn shared_store_restores_index_pages_and_replays_later_oplog() {
        let dir = tempfile::tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("primary-cache"),
            dir.path().join("primary-pages"),
            dir.path().join("primary-index"),
        );
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
        let replicator = SharedStoreReplicator::new("cluster-a", store);
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_page_segments(1, &primary.page_store())
            .await
            .unwrap();
        replicator
            .publish_oplog_entry(SharedStoreOplogEntry {
                shard_id: 1,
                oplog_index: 2,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"oplog-value".to_vec(),
                },
            })
            .await
            .unwrap();

        let follower = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("follower-cache"),
            dir.path().join("follower-pages"),
            dir.path().join("follower-index"),
        );
        let restored = replicator
            .restore_index_and_pages(1, &follower, &follower.page_store())
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
                value: Some(b"oplog-value".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn shared_store_checkpoint_rejects_corrupt_page_segment() {
        let dir = tempfile::tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("primary-cache"),
            dir.path().join("primary-pages"),
            dir.path().join("primary-index"),
        );
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });

        let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
        let replicator = SharedStoreReplicator::new("cluster-a", store.clone());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.page_store())
            .await
            .unwrap();
        store
            .put(
                &manifest.page_segments[0].key,
                Bytes::from_static(b"corrupt"),
            )
            .await
            .unwrap();

        let follower = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("follower-cache"),
            dir.path().join("follower-pages"),
            dir.path().join("follower-index"),
        );
        assert!(matches!(
            replicator
                .restore_checkpoint(&manifest, &follower, &follower.page_store())
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ChecksumMismatch { .. }
        ));
    }
}
