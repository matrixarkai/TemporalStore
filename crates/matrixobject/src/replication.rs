use crate::local::LocalMatrixObjectStore;
use crate::types::*;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Primary,
    Secondary,
}

#[derive(Debug, Clone)]
pub struct ReplicaNode {
    pub node_id: u64,
    pub role: ReplicaRole,
    pub lag_versions: u64,
    pub store: Arc<LocalMatrixObjectStore>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicationPolicy {
    pub sync_secondary_count: usize,
    pub max_read_lag_versions: u64,
    pub allow_stale_replica_reads: bool,
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            sync_secondary_count: 0,
            max_read_lag_versions: 0,
            allow_stale_replica_reads: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentReplicationReport {
    pub segment_id: SegmentId,
    pub primary_version: u64,
    pub replica_versions: Vec<(u64, u64)>,
    pub max_lag_versions: u64,
}

#[derive(Clone)]
pub struct ReplicaSet {
    primary: ReplicaNode,
    secondaries: Vec<ReplicaNode>,
    policy: ReplicationPolicy,
}

impl ReplicaSet {
    pub fn new(
        primary: ReplicaNode,
        secondaries: Vec<ReplicaNode>,
        policy: ReplicationPolicy,
    ) -> Self {
        Self {
            primary,
            secondaries,
            policy,
        }
    }

    pub fn primary(&self) -> &ReplicaNode {
        &self.primary
    }

    pub fn secondaries(&self) -> &[ReplicaNode] {
        &self.secondaries
    }

    pub async fn replicate_segment(
        &self,
        segment_id: &SegmentId,
    ) -> Result<SegmentReplicationReport> {
        let export = self.primary.store.export_segment(segment_id).await?;
        let primary_version = export.manifest.open_version;
        let mut replica_versions = Vec::with_capacity(self.secondaries.len());

        for secondary in self
            .secondaries
            .iter()
            .take(self.policy.sync_secondary_count)
        {
            let resp = secondary.store.import_segment(export.clone()).await?;
            replica_versions.push((secondary.node_id, resp.open_version));
        }

        let max_lag_versions = replica_versions
            .iter()
            .map(|(_, version)| primary_version.saturating_sub(*version))
            .max()
            .unwrap_or(primary_version);

        Ok(SegmentReplicationReport {
            segment_id: segment_id.clone(),
            primary_version,
            replica_versions,
            max_lag_versions,
        })
    }

    pub async fn choose_read_store(
        &self,
        segment_id: &SegmentId,
    ) -> Result<Arc<LocalMatrixObjectStore>> {
        if !self.policy.allow_stale_replica_reads {
            return Ok(self.primary.store.clone());
        }

        let primary_space = self.primary.store.stat_segment(segment_id).await?;
        for secondary in &self.secondaries {
            if let Ok(space) = secondary.store.stat_segment(segment_id).await {
                let lag = primary_space
                    .open_version
                    .saturating_sub(space.open_version);
                if lag <= self.policy.max_read_lag_versions {
                    return Ok(secondary.store.clone());
                }
            }
        }
        Ok(self.primary.store.clone())
    }
}
