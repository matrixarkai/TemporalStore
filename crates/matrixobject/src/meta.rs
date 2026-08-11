// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PlacementPolicy {
    pub replica_count: usize,
    pub spread_across_zones: bool,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            replica_count: 3,
            spread_across_zones: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MatrixObjectMetaService {
    inner: Arc<RwLock<MetaState>>,
    catalog_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
struct MetaState {
    nodes: BTreeMap<u64, NodeDescriptor>,
    node_load: BTreeMap<u64, NodeLoadReport>,
    namespaces: BTreeMap<NamespaceId, NamespaceDescriptor>,
    volumes: BTreeMap<VolumeId, VolumeDescriptor>,
    segments: BTreeMap<SegmentId, SegmentDescriptor>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MetaCatalogSnapshot {
    version: u32,
    updated_at_micros: u64,
    #[serde(default)]
    nodes: Vec<NodeDescriptor>,
    #[serde(default)]
    node_load: Vec<NodeLoadReport>,
    #[serde(default)]
    namespaces: Vec<NamespaceDescriptor>,
    #[serde(default)]
    volumes: Vec<VolumeDescriptor>,
    #[serde(default)]
    segments: Vec<SegmentDescriptor>,
}

impl MatrixObjectMetaService {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub async fn open_persistent(catalog_path: impl Into<PathBuf>) -> Result<Self> {
        let catalog_path = catalog_path.into();
        let state = if catalog_path.exists() {
            read_meta_catalog(&catalog_path).await?
        } else {
            MetaState::default()
        };
        let service = Self {
            inner: Arc::new(RwLock::new(state)),
            catalog_path: Some(catalog_path),
        };
        service.persist_current().await?;
        Ok(service)
    }

    pub async fn register_node(&self, node: NodeDescriptor) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.nodes.insert(node.node_id, node);
        self.persist_locked(&inner).await
    }

    pub async fn update_node_serviceable(&self, node_id: u64, serviceable: bool) -> Result<()> {
        let mut inner = self.inner.write().await;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(MatrixObjectError::NodeNotFound(node_id))?;
        node.serviceable = serviceable;
        for disk in &mut node.disk_status {
            disk.serviceable = serviceable;
        }
        self.persist_locked(&inner).await
    }

    pub async fn report_node_load(&self, req: ReportNodeLoadRequest) -> Result<NodeDescriptor> {
        let mut inner = self.inner.write().await;
        let node_id = req.report.node_id;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(MatrixObjectError::NodeNotFound(node_id))?;
        node.serviceable = req.report.serviceable;
        for disk in &mut node.disk_status {
            disk.serviceable = req.report.serviceable;
            disk.approximate_used_bytes = req.report.used_bytes;
        }
        let node = node.clone();
        inner.node_load.insert(node_id, req.report);
        self.persist_locked(&inner).await?;
        Ok(node)
    }

    pub async fn get_node_load(&self, node_id: u64) -> Result<NodeLoadReport> {
        self.inner
            .read()
            .await
            .node_load
            .get(&node_id)
            .cloned()
            .ok_or(MatrixObjectError::NodeNotFound(node_id))
    }

    pub async fn list_node_load(&self) -> Vec<NodeLoadReport> {
        self.inner
            .read()
            .await
            .node_load
            .values()
            .cloned()
            .collect()
    }

    pub async fn create_namespace(
        &self,
        namespace_id: NamespaceId,
        max_volumes: Option<usize>,
        max_logical_bytes: Option<u64>,
    ) -> Result<NamespaceDescriptor> {
        let mut inner = self.inner.write().await;
        let now = now_micros();
        let descriptor = inner
            .namespaces
            .entry(namespace_id.clone())
            .or_insert_with(|| NamespaceDescriptor {
                namespace_id,
                created_at_micros: now,
                updated_at_micros: now,
                serviceable: true,
                max_volumes,
                max_logical_bytes,
                volume_count: 0,
                segment_count: 0,
                logical_bytes: 0,
            });
        descriptor.updated_at_micros = now;
        descriptor.serviceable = true;
        descriptor.max_volumes = max_volumes;
        descriptor.max_logical_bytes = max_logical_bytes;
        let descriptor = descriptor.clone();
        self.persist_locked(&inner).await?;
        Ok(descriptor)
    }

    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceDescriptor> {
        self.inner
            .read()
            .await
            .namespaces
            .get(namespace_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::NamespaceNotFound(namespace_id.clone()))
    }

    pub async fn list_namespaces(&self) -> ListNamespacesResponse {
        let inner = self.inner.read().await;
        let namespaces = inner.namespaces.values().cloned().collect::<Vec<_>>();
        ListNamespacesResponse {
            total_volumes: namespaces
                .iter()
                .map(|namespace| namespace.volume_count)
                .sum(),
            total_segments: namespaces
                .iter()
                .map(|namespace| namespace.segment_count)
                .sum(),
            total_logical_bytes: namespaces
                .iter()
                .map(|namespace| namespace.logical_bytes)
                .sum(),
            namespaces,
        }
    }

    pub async fn update_namespace_serviceable(
        &self,
        namespace_id: &NamespaceId,
        serviceable: bool,
    ) -> Result<NamespaceDescriptor> {
        let mut inner = self.inner.write().await;
        let namespace = inner
            .namespaces
            .get_mut(namespace_id)
            .ok_or_else(|| MatrixObjectError::NamespaceNotFound(namespace_id.clone()))?;
        namespace.serviceable = serviceable;
        namespace.updated_at_micros = now_micros();
        let namespace = namespace.clone();
        self.persist_locked(&inner).await?;
        Ok(namespace)
    }

    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceDescriptor> {
        let mut inner = self.inner.write().await;
        let namespace = inner
            .namespaces
            .get(namespace_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::NamespaceNotFound(namespace_id.clone()))?;
        if namespace.volume_count > 0 || namespace.segment_count > 0 {
            return Err(MatrixObjectError::AdmissionControl(format!(
                "namespace {} is not empty",
                namespace_id
            )));
        }
        inner.namespaces.remove(namespace_id);
        self.persist_locked(&inner).await?;
        Ok(namespace)
    }

    pub async fn create_volume(
        &self,
        volume_id: VolumeId,
        max_segments: Option<usize>,
        max_logical_bytes: Option<u64>,
    ) -> Result<VolumeDescriptor> {
        let mut inner = self.inner.write().await;
        ensure_namespace_exists_locked(&mut inner, volume_id.namespace_id())?;
        let now = now_micros();
        let descriptor =
            inner
                .volumes
                .entry(volume_id.clone())
                .or_insert_with(|| VolumeDescriptor {
                    volume_id: volume_id.clone(),
                    created_at_micros: now,
                    updated_at_micros: now,
                    serviceable: true,
                    max_segments,
                    max_logical_bytes,
                    segment_count: 0,
                    logical_bytes: 0,
                });
        descriptor.updated_at_micros = now;
        descriptor.serviceable = true;
        descriptor.max_segments = max_segments;
        descriptor.max_logical_bytes = max_logical_bytes;
        refresh_hierarchy_stats_locked(&mut inner);
        let descriptor = inner
            .volumes
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::VolumeNotFound(volume_id.clone()))?;
        self.persist_locked(&inner).await?;
        Ok(descriptor)
    }

    pub async fn get_volume(&self, volume_id: &VolumeId) -> Result<VolumeDescriptor> {
        self.inner
            .read()
            .await
            .volumes
            .get(volume_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::VolumeNotFound(volume_id.clone()))
    }

    pub async fn list_volumes(&self, namespace_id: Option<&NamespaceId>) -> ListVolumesResponse {
        let inner = self.inner.read().await;
        let volumes = inner
            .volumes
            .values()
            .filter(|volume| {
                namespace_id
                    .map(|namespace_id| &volume.volume_id.namespace_id() == namespace_id)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        ListVolumesResponse {
            total_segments: volumes.iter().map(|volume| volume.segment_count).sum(),
            total_logical_bytes: volumes.iter().map(|volume| volume.logical_bytes).sum(),
            volumes,
        }
    }

    pub async fn update_volume_serviceable(
        &self,
        volume_id: &VolumeId,
        serviceable: bool,
    ) -> Result<VolumeDescriptor> {
        let mut inner = self.inner.write().await;
        let volume = inner
            .volumes
            .get_mut(volume_id)
            .ok_or_else(|| MatrixObjectError::VolumeNotFound(volume_id.clone()))?;
        volume.serviceable = serviceable;
        volume.updated_at_micros = now_micros();
        let volume = volume.clone();
        self.persist_locked(&inner).await?;
        Ok(volume)
    }

    pub async fn delete_volume(&self, volume_id: &VolumeId) -> Result<VolumeDescriptor> {
        let mut inner = self.inner.write().await;
        let volume = inner
            .volumes
            .get(volume_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::VolumeNotFound(volume_id.clone()))?;
        if volume.segment_count > 0 {
            return Err(MatrixObjectError::AdmissionControl(format!(
                "volume {} is not empty",
                volume_id
            )));
        }
        inner.volumes.remove(volume_id);
        refresh_hierarchy_stats_locked(&mut inner);
        self.persist_locked(&inner).await?;
        Ok(volume)
    }

    pub async fn sweep_node_failures(
        &self,
        policy: NodeFailureDetectorPolicy,
        placement: PlacementPolicy,
    ) -> Result<NodeFailureDetectorReport> {
        let now = now_micros();
        let mut stale_nodes = Vec::new();
        {
            let mut inner = self.inner.write().await;
            let stale_candidates = inner
                .nodes
                .iter()
                .filter_map(|(node_id, node)| {
                    let stale = inner
                        .node_load
                        .get(node_id)
                        .map(|load| {
                            !load.serviceable
                                || now.saturating_sub(load.observed_at_micros)
                                    > policy.heartbeat_timeout_micros
                        })
                        .unwrap_or(true);
                    (stale && node.serviceable).then_some(*node_id)
                })
                .collect::<Vec<_>>();
            for node_id in stale_candidates {
                let Some(node) = inner.nodes.get_mut(&node_id) else {
                    continue;
                };
                node.serviceable = false;
                for disk in &mut node.disk_status {
                    disk.serviceable = false;
                }
                stale_nodes.push(node_id);
            }

            if !stale_nodes.is_empty() {
                for segment in inner.segments.values_mut() {
                    for replica in &mut segment.replicas {
                        if stale_nodes.contains(&replica.node_id) {
                            replica.serviceable = false;
                            replica.lag_versions = replica.lag_versions.saturating_add(1);
                        }
                    }
                    ensure_segment_primary(segment);
                }
                self.persist_locked(&inner).await?;
            }
        }

        let mut rebalance_plan = self.plan_rebalance(placement).await;
        if policy.rebalance_on_failure && !rebalance_plan.actions.is_empty() {
            rebalance_plan = self.apply_rebalance(rebalance_plan).await?;
        }
        let affected_segments = rebalance_plan.affected_segments;
        Ok(NodeFailureDetectorReport {
            checked_nodes: self.inner.read().await.nodes.len(),
            stale_nodes,
            affected_segments,
            rebalance_plan,
        })
    }

    pub async fn open_segment(
        &self,
        segment_id: SegmentId,
        policy: PlacementPolicy,
        create_if_missing: bool,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        if let Some(segment) = inner.segments.get(&segment_id) {
            return Ok(segment.clone());
        }
        if !create_if_missing {
            return Err(MatrixObjectError::SegmentNotFound(segment_id));
        }
        ensure_volume_exists_locked(&mut inner, segment_id.volume_key())?;
        validate_segment_create_locked(&inner, &segment_id)?;

        let replicas = choose_replicas(&inner.nodes, &inner.node_load, policy)
            .into_iter()
            .enumerate()
            .map(|(index, node_id)| SegmentReplicaDescriptor {
                node_id,
                role: if index == 0 {
                    SegmentReplicaRole::Primary
                } else {
                    SegmentReplicaRole::Secondary
                },
                lag_versions: 0,
                serviceable: true,
            })
            .collect();

        let segment = SegmentDescriptor {
            segment_id: segment_id.clone(),
            status: SegmentStatus::Open,
            open_version: 1,
            logical_size: 0,
            replicas,
            snapshots: Vec::new(),
        };
        inner.segments.insert(segment_id, segment.clone());
        refresh_hierarchy_stats_locked(&mut inner);
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn close_segment(&self, segment_id: &SegmentId) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        segment.status = SegmentStatus::Frozen;
        segment.open_version += 1;
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn delete_segment(&self, segment_id: &SegmentId) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        segment.status = SegmentStatus::Deleted;
        segment.open_version += 1;
        let segment = segment.clone();
        refresh_hierarchy_stats_locked(&mut inner);
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn switch_primary(
        &self,
        segment_id: &SegmentId,
        node_id: u64,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        if !inner.nodes.contains_key(&node_id) {
            return Err(MatrixObjectError::NodeNotFound(node_id));
        }
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        let mut found = false;
        for replica in &mut segment.replicas {
            if replica.node_id == node_id {
                replica.role = SegmentReplicaRole::Primary;
                replica.lag_versions = 0;
                found = true;
            } else if replica.role == SegmentReplicaRole::Primary {
                replica.role = SegmentReplicaRole::Secondary;
            }
        }
        if !found {
            segment.replicas.push(SegmentReplicaDescriptor {
                node_id,
                role: SegmentReplicaRole::Primary,
                lag_versions: 0,
                serviceable: true,
            });
        }
        segment.open_version += 1;
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn add_replica(
        &self,
        segment_id: &SegmentId,
        node_id: u64,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        if !inner.nodes.contains_key(&node_id) {
            return Err(MatrixObjectError::NodeNotFound(node_id));
        }
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        if !segment
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id)
        {
            segment.replicas.push(SegmentReplicaDescriptor {
                node_id,
                role: SegmentReplicaRole::Secondary,
                lag_versions: segment.open_version,
                serviceable: true,
            });
            segment.open_version += 1;
        }
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn remove_replica(
        &self,
        segment_id: &SegmentId,
        node_id: u64,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        segment
            .replicas
            .retain(|replica| replica.node_id != node_id);
        if !segment
            .replicas
            .iter()
            .any(|replica| replica.role == SegmentReplicaRole::Primary)
        {
            if let Some(replica) = segment.replicas.first_mut() {
                replica.role = SegmentReplicaRole::Primary;
                replica.lag_versions = 0;
            }
        }
        segment.open_version += 1;
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn record_replica_version(
        &self,
        segment_id: &SegmentId,
        node_id: u64,
        replica_open_version: u64,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        let primary_version = segment.open_version.max(replica_open_version);
        segment.open_version = primary_version;
        let mut found = false;
        for replica in &mut segment.replicas {
            if replica.node_id == node_id {
                replica.lag_versions = primary_version.saturating_sub(replica_open_version);
                replica.serviceable = true;
                found = true;
            } else if replica.role == SegmentReplicaRole::Primary {
                replica.lag_versions = 0;
            }
        }
        if !found {
            segment.replicas.push(SegmentReplicaDescriptor {
                node_id,
                role: SegmentReplicaRole::Secondary,
                lag_versions: primary_version.saturating_sub(replica_open_version),
                serviceable: true,
            });
        }
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn record_replica_failure(
        &self,
        segment_id: &SegmentId,
        node_id: u64,
    ) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        for replica in &mut segment.replicas {
            if replica.node_id == node_id {
                replica.serviceable = false;
                replica.lag_versions = replica.lag_versions.saturating_add(1);
            }
        }
        ensure_segment_primary(segment);
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn plan_rebalance(&self, policy: PlacementPolicy) -> RebalancePlan {
        let inner = self.inner.read().await;
        plan_rebalance_locked(&inner, policy)
    }

    pub async fn apply_rebalance(&self, plan: RebalancePlan) -> Result<RebalancePlan> {
        let mut inner = self.inner.write().await;
        let mut applied = Vec::new();
        for action in plan.actions {
            match &action {
                RebalanceAction::AddReplica {
                    segment_id,
                    node_id,
                    ..
                } => {
                    if !node_serviceable(&inner, *node_id) {
                        continue;
                    }
                    let Some(segment) = inner.segments.get_mut(segment_id) else {
                        continue;
                    };
                    if !segment
                        .replicas
                        .iter()
                        .any(|replica| replica.node_id == *node_id)
                    {
                        segment.replicas.push(SegmentReplicaDescriptor {
                            node_id: *node_id,
                            role: SegmentReplicaRole::Secondary,
                            lag_versions: segment.open_version,
                            serviceable: true,
                        });
                        segment.open_version += 1;
                        applied.push(action);
                    }
                }
                RebalanceAction::RemoveReplica {
                    segment_id,
                    node_id,
                    ..
                } => {
                    let Some(segment) = inner.segments.get_mut(segment_id) else {
                        continue;
                    };
                    let before = segment.replicas.len();
                    segment
                        .replicas
                        .retain(|replica| replica.node_id != *node_id);
                    if segment.replicas.len() != before {
                        ensure_segment_primary(segment);
                        segment.open_version += 1;
                        applied.push(action);
                    }
                }
                RebalanceAction::PromoteReplica {
                    segment_id,
                    node_id,
                    ..
                } => {
                    if !node_serviceable(&inner, *node_id) {
                        continue;
                    }
                    let Some(segment) = inner.segments.get_mut(segment_id) else {
                        continue;
                    };
                    let mut changed = false;
                    for replica in &mut segment.replicas {
                        if replica.node_id == *node_id {
                            if replica.role != SegmentReplicaRole::Primary {
                                replica.role = SegmentReplicaRole::Primary;
                                replica.lag_versions = 0;
                                changed = true;
                            }
                        } else if replica.role == SegmentReplicaRole::Primary {
                            replica.role = SegmentReplicaRole::Secondary;
                            changed = true;
                        }
                    }
                    if changed {
                        segment.open_version += 1;
                        applied.push(action);
                    }
                }
                RebalanceAction::MarkReplicaUnserviceable {
                    segment_id,
                    node_id,
                    ..
                } => {
                    let Some(segment) = inner.segments.get_mut(segment_id) else {
                        continue;
                    };
                    let mut changed = false;
                    for replica in &mut segment.replicas {
                        if replica.node_id == *node_id && replica.serviceable {
                            replica.serviceable = false;
                            changed = true;
                        }
                    }
                    if changed {
                        ensure_segment_primary(segment);
                        segment.open_version += 1;
                        applied.push(action);
                    }
                }
            }
        }
        let affected_segments = applied
            .iter()
            .map(rebalance_action_segment_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        self.persist_locked(&inner).await?;
        Ok(RebalancePlan {
            actions: applied,
            affected_segments,
        })
    }

    pub async fn rebalance(&self, policy: PlacementPolicy) -> Result<RebalancePlan> {
        let plan = self.plan_rebalance(policy).await;
        self.apply_rebalance(plan).await
    }

    pub async fn drain_node(&self, node_id: u64, policy: PlacementPolicy) -> Result<RebalancePlan> {
        self.update_node_serviceable(node_id, false).await?;
        let mut plan = self.plan_rebalance(policy).await;
        for segment in self.sniff_segments(None).await.segments {
            if segment
                .replicas
                .iter()
                .any(|replica| replica.node_id == node_id)
            {
                plan.actions.push(RebalanceAction::RemoveReplica {
                    segment_id: segment.segment_id.clone(),
                    node_id,
                    reason: RebalanceReason::ScaleDown,
                });
            }
        }
        dedupe_actions(&mut plan.actions);
        plan.affected_segments = affected_segment_count(&plan.actions);
        self.apply_rebalance(plan).await
    }

    pub async fn fail_node(&self, node_id: u64, policy: PlacementPolicy) -> Result<RebalancePlan> {
        self.update_node_serviceable(node_id, false).await?;
        let mut plan = RebalancePlan::default();
        {
            let inner = self.inner.read().await;
            for segment in inner.segments.values() {
                for replica in &segment.replicas {
                    if replica.node_id == node_id {
                        plan.actions
                            .push(RebalanceAction::MarkReplicaUnserviceable {
                                segment_id: segment.segment_id.clone(),
                                node_id,
                                reason: RebalanceReason::NodeFailure,
                            });
                    }
                }
            }
            plan.actions
                .extend(plan_rebalance_locked(&inner, policy).actions);
        }
        dedupe_actions(&mut plan.actions);
        plan.affected_segments = affected_segment_count(&plan.actions);
        self.apply_rebalance(plan).await
    }

    pub async fn record_snapshot(&self, snapshot: SnapshotRef) -> Result<SegmentDescriptor> {
        let mut inner = self.inner.write().await;
        let segment = inner
            .segments
            .get_mut(&snapshot.segment_id)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(snapshot.segment_id.clone()))?;
        segment.snapshots.push(snapshot);
        segment
            .snapshots
            .sort_by_key(|snapshot| snapshot.open_version);
        let segment = segment.clone();
        self.persist_locked(&inner).await?;
        Ok(segment)
    }

    pub async fn get_segment(&self, segment_id: &SegmentId) -> Result<SegmentDescriptor> {
        self.inner
            .read()
            .await
            .segments
            .get(segment_id)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))
    }

    pub async fn sniff_segments(&self, tenant_prefix: Option<&str>) -> SniffSegmentsResponse {
        let inner = self.inner.read().await;
        let mut segments = Vec::new();
        for segment in inner.segments.values() {
            if tenant_prefix
                .map(|prefix| segment.segment_id.tenant_id.starts_with(prefix))
                .unwrap_or(true)
            {
                segments.push(segment.clone());
            }
        }
        let total_logical_size = segments.iter().map(|segment| segment.logical_size).sum();
        let total_replicas = segments.iter().map(|segment| segment.replicas.len()).sum();
        SniffSegmentsResponse {
            segments,
            total_logical_size,
            total_replicas,
        }
    }

    pub async fn persist_current(&self) -> Result<()> {
        let inner = self.inner.read().await;
        self.persist_locked(&inner).await
    }

    async fn persist_locked(&self, inner: &MetaState) -> Result<()> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };
        write_meta_catalog(path, inner).await
    }
}

fn ensure_namespace_exists_locked(inner: &mut MetaState, namespace_id: NamespaceId) -> Result<()> {
    let now = now_micros();
    inner
        .namespaces
        .entry(namespace_id.clone())
        .or_insert_with(|| NamespaceDescriptor {
            namespace_id,
            created_at_micros: now,
            updated_at_micros: now,
            serviceable: true,
            max_volumes: None,
            max_logical_bytes: None,
            volume_count: 0,
            segment_count: 0,
            logical_bytes: 0,
        });
    Ok(())
}

fn ensure_volume_exists_locked(inner: &mut MetaState, volume_id: VolumeId) -> Result<()> {
    ensure_namespace_exists_locked(inner, volume_id.namespace_id())?;
    let namespace_id = volume_id.namespace_id();
    let namespace = inner
        .namespaces
        .get(&namespace_id)
        .ok_or_else(|| MatrixObjectError::NamespaceNotFound(namespace_id.clone()))?;
    if !namespace.serviceable {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "namespace {} is not serviceable",
            namespace_id
        )));
    }
    if !inner.volumes.contains_key(&volume_id) {
        if let Some(limit) = namespace.max_volumes {
            let volume_count = inner
                .volumes
                .keys()
                .filter(|existing| existing.namespace_id() == namespace_id)
                .count();
            if volume_count >= limit {
                return Err(MatrixObjectError::AdmissionControl(format!(
                    "namespace {} volume quota exceeded",
                    namespace_id
                )));
            }
        }
        let now = now_micros();
        inner
            .volumes
            .entry(volume_id.clone())
            .or_insert_with(|| VolumeDescriptor {
                volume_id,
                created_at_micros: now,
                updated_at_micros: now,
                serviceable: true,
                max_segments: None,
                max_logical_bytes: None,
                segment_count: 0,
                logical_bytes: 0,
            });
    }
    Ok(())
}

fn validate_segment_create_locked(inner: &MetaState, segment_id: &SegmentId) -> Result<()> {
    let namespace_id = segment_id.namespace_id();
    let volume_id = segment_id.volume_key();
    let namespace = inner
        .namespaces
        .get(&namespace_id)
        .ok_or_else(|| MatrixObjectError::NamespaceNotFound(namespace_id.clone()))?;
    if !namespace.serviceable {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "namespace {} is not serviceable",
            namespace_id
        )));
    }
    if namespace
        .max_logical_bytes
        .is_some_and(|limit| namespace.logical_bytes >= limit)
    {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "namespace {} logical quota exceeded",
            namespace_id
        )));
    }
    let volume = inner
        .volumes
        .get(&volume_id)
        .ok_or_else(|| MatrixObjectError::VolumeNotFound(volume_id.clone()))?;
    if !volume.serviceable {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "volume {} is not serviceable",
            volume_id
        )));
    }
    if volume
        .max_segments
        .is_some_and(|limit| volume.segment_count >= limit)
    {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "volume {} segment quota exceeded",
            volume_id
        )));
    }
    if volume
        .max_logical_bytes
        .is_some_and(|limit| volume.logical_bytes >= limit)
    {
        return Err(MatrixObjectError::AdmissionControl(format!(
            "volume {} logical quota exceeded",
            volume_id
        )));
    }
    Ok(())
}

fn refresh_hierarchy_stats_locked(inner: &mut MetaState) {
    for namespace in inner.namespaces.values_mut() {
        namespace.volume_count = 0;
        namespace.segment_count = 0;
        namespace.logical_bytes = 0;
        namespace.updated_at_micros = now_micros();
    }
    for volume in inner.volumes.values_mut() {
        volume.segment_count = 0;
        volume.logical_bytes = 0;
        volume.updated_at_micros = now_micros();
    }
    for segment in inner
        .segments
        .values()
        .filter(|segment| segment.status != SegmentStatus::Deleted)
    {
        let namespace_id = segment.segment_id.namespace_id();
        let volume_id = segment.segment_id.volume_key();
        if let Some(volume) = inner.volumes.get_mut(&volume_id) {
            volume.segment_count += 1;
            volume.logical_bytes = volume.logical_bytes.saturating_add(segment.logical_size);
        }
        if let Some(namespace) = inner.namespaces.get_mut(&namespace_id) {
            namespace.segment_count += 1;
            namespace.logical_bytes = namespace.logical_bytes.saturating_add(segment.logical_size);
        }
    }
    let volume_keys = inner.volumes.keys().cloned().collect::<Vec<_>>();
    for volume_id in volume_keys {
        if let Some(namespace) = inner.namespaces.get_mut(&volume_id.namespace_id()) {
            namespace.volume_count += 1;
        }
    }
}

fn rebuild_hierarchy_from_segments_locked(inner: &mut MetaState) {
    let segment_ids = inner.segments.keys().cloned().collect::<Vec<_>>();
    for segment_id in segment_ids {
        let _ = ensure_volume_exists_locked(inner, segment_id.volume_key());
    }
    refresh_hierarchy_stats_locked(inner);
}

fn choose_replicas(
    nodes: &BTreeMap<u64, NodeDescriptor>,
    node_load: &BTreeMap<u64, NodeLoadReport>,
    policy: PlacementPolicy,
) -> Vec<u64> {
    let mut chosen = Vec::new();
    let mut zones = Vec::<String>::new();
    let mut candidates = nodes
        .values()
        .filter(|node| node.serviceable)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|node| {
        node_load
            .get(&node.node_id)
            .map(|load| {
                (
                    load.in_flight,
                    load.write_qps.saturating_add(load.read_qps),
                    load.used_bytes,
                    node.node_id,
                )
            })
            .unwrap_or((0, 0, 0, node.node_id))
    });
    for node in candidates {
        if chosen.len() >= policy.replica_count {
            break;
        }
        if policy.spread_across_zones && zones.iter().any(|zone| zone == &node.zone) {
            continue;
        }
        zones.push(node.zone.clone());
        chosen.push(node.node_id);
    }

    if chosen.len() < policy.replica_count {
        for node in nodes.values().filter(|node| node.serviceable) {
            if chosen.len() >= policy.replica_count {
                break;
            }
            if !chosen.contains(&node.node_id) {
                chosen.push(node.node_id);
            }
        }
    }
    chosen
}

fn plan_rebalance_locked(inner: &MetaState, policy: PlacementPolicy) -> RebalancePlan {
    let mut actions = Vec::new();
    for segment in inner
        .segments
        .values()
        .filter(|segment| segment.status != SegmentStatus::Deleted)
    {
        let live_replicas = segment
            .replicas
            .iter()
            .filter(|replica| replica.serviceable && node_serviceable(inner, replica.node_id))
            .collect::<Vec<_>>();
        if !live_replicas
            .iter()
            .any(|replica| replica.role == SegmentReplicaRole::Primary)
        {
            if let Some(replica) = live_replicas
                .iter()
                .min_by_key(|replica| (replica.lag_versions, replica.node_id))
            {
                actions.push(RebalanceAction::PromoteReplica {
                    segment_id: segment.segment_id.clone(),
                    node_id: replica.node_id,
                    reason: RebalanceReason::NodeFailure,
                });
            }
        }

        if live_replicas.len() < policy.replica_count {
            for node_id in choose_replicas(&inner.nodes, &inner.node_load, policy.clone()) {
                if live_replicas
                    .iter()
                    .any(|replica| replica.node_id == node_id)
                    || segment
                        .replicas
                        .iter()
                        .any(|replica| replica.node_id == node_id)
                {
                    continue;
                }
                actions.push(RebalanceAction::AddReplica {
                    segment_id: segment.segment_id.clone(),
                    node_id,
                    reason: RebalanceReason::UnderReplicated,
                });
                if live_replicas.len()
                    + actions
                        .iter()
                        .filter(|action| {
                            matches!(
                                action,
                                RebalanceAction::AddReplica { segment_id, .. }
                                    if segment_id == &segment.segment_id
                            )
                        })
                        .count()
                    >= policy.replica_count
                {
                    break;
                }
            }
        }

        for replica in &segment.replicas {
            if !node_serviceable(inner, replica.node_id) && replica.serviceable {
                actions.push(RebalanceAction::MarkReplicaUnserviceable {
                    segment_id: segment.segment_id.clone(),
                    node_id: replica.node_id,
                    reason: RebalanceReason::NodeFailure,
                });
            }
        }
    }
    dedupe_actions(&mut actions);
    RebalancePlan {
        affected_segments: affected_segment_count(&actions),
        actions,
    }
}

fn node_serviceable(inner: &MetaState, node_id: u64) -> bool {
    inner
        .nodes
        .get(&node_id)
        .map(|node| node.serviceable)
        .unwrap_or(false)
        && inner
            .node_load
            .get(&node_id)
            .map(|load| load.serviceable)
            .unwrap_or(true)
}

fn ensure_segment_primary(segment: &mut SegmentDescriptor) {
    if segment
        .replicas
        .iter()
        .any(|replica| replica.role == SegmentReplicaRole::Primary && replica.serviceable)
    {
        return;
    }
    if let Some(replica) = segment
        .replicas
        .iter_mut()
        .filter(|replica| replica.serviceable)
        .min_by_key(|replica| (replica.lag_versions, replica.node_id))
    {
        replica.role = SegmentReplicaRole::Primary;
        replica.lag_versions = 0;
    }
}

fn affected_segment_count(actions: &[RebalanceAction]) -> usize {
    actions
        .iter()
        .map(rebalance_action_segment_id)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn rebalance_action_segment_id(action: &RebalanceAction) -> SegmentId {
    match action {
        RebalanceAction::AddReplica { segment_id, .. }
        | RebalanceAction::RemoveReplica { segment_id, .. }
        | RebalanceAction::PromoteReplica { segment_id, .. }
        | RebalanceAction::MarkReplicaUnserviceable { segment_id, .. } => segment_id.clone(),
    }
}

fn dedupe_actions(actions: &mut Vec<RebalanceAction>) {
    let mut seen = std::collections::BTreeSet::new();
    actions.retain(|action| {
        let key = match action {
            RebalanceAction::AddReplica {
                segment_id,
                node_id,
                reason,
            } => (0u8, segment_id.clone(), *node_id, format!("{:?}", reason)),
            RebalanceAction::RemoveReplica {
                segment_id,
                node_id,
                reason,
            } => (1u8, segment_id.clone(), *node_id, format!("{:?}", reason)),
            RebalanceAction::PromoteReplica {
                segment_id,
                node_id,
                reason,
            } => (2u8, segment_id.clone(), *node_id, format!("{:?}", reason)),
            RebalanceAction::MarkReplicaUnserviceable {
                segment_id,
                node_id,
                reason,
            } => (3u8, segment_id.clone(), *node_id, format!("{:?}", reason)),
        };
        seen.insert(key)
    });
}

async fn read_meta_catalog(path: &Path) -> Result<MetaState> {
    let bytes = tokio::fs::read(path).await?;
    let snapshot: MetaCatalogSnapshot = serde_json::from_slice(&bytes)?;
    let nodes = snapshot
        .nodes
        .into_iter()
        .map(|node| (node.node_id, node))
        .collect();
    let node_load = snapshot
        .node_load
        .into_iter()
        .map(|load| (load.node_id, load))
        .collect();
    let namespaces = snapshot
        .namespaces
        .into_iter()
        .map(|namespace| (namespace.namespace_id.clone(), namespace))
        .collect();
    let volumes = snapshot
        .volumes
        .into_iter()
        .map(|volume| (volume.volume_id.clone(), volume))
        .collect();
    let segments = snapshot
        .segments
        .into_iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect();
    let mut state = MetaState {
        nodes,
        node_load,
        namespaces,
        volumes,
        segments,
    };
    rebuild_hierarchy_from_segments_locked(&mut state);
    Ok(state)
}

async fn write_meta_catalog(path: &Path, state: &MetaState) -> Result<()> {
    let snapshot = MetaCatalogSnapshot {
        version: 1,
        updated_at_micros: now_micros(),
        nodes: state.nodes.values().cloned().collect(),
        node_load: state.node_load.values().cloned().collect(),
        namespaces: state.namespaces.values().cloned().collect(),
        volumes: state.volumes.values().cloned().collect(),
        segments: state.segments.values().cloned().collect(),
    };
    let mut bytes = serde_json::to_vec_pretty(&snapshot)?;
    bytes.push(b'\n');
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(tmp, path)?;
        Ok(())
    })
    .await
    .expect("blocking meta catalog write panicked")
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
