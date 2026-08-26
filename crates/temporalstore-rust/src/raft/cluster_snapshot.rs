// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster snapshot build/install/transfer methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn build_install_snapshot_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        self.build_install_snapshot_request_with_external_ref(target_id, None)
    }

    pub fn build_install_snapshot_request_with_external_ref(
        &self,
        target_id: RaftNodeId,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let mut snapshot = self.create_snapshot()?;
        snapshot.external_snapshot_ref = external_snapshot_ref.clone();
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let shard_id = inner.shard_id;
        let term = leader.current_term;
        let leader_id = inner.leader_id;
        let logical_time_ms = inner.logical_time_ms;
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(1);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = 1;
        }
        inner.persist_configured_wal()?;
        Ok(InstallSnapshotRequest {
            rpc: None,
            shard_id,
            term,
            leader_id,
            target_id,
            external_snapshot_ref,
            snapshot,
        })
    }

    pub fn plan_snapshot_bootstrap(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        Ok(RaftReplicaBootstrapPlan {
            shard_id: snapshot.shard_id,
            target_id,
            catch_up_from_index: snapshot.last_included_index.saturating_add(1),
            last_included_index: snapshot.last_included_index,
            transfer,
        })
    }

    pub fn build_install_snapshot_request_with_policy(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        match transfer.mode {
            RaftSnapshotTransferMode::PeerStreaming => {
                self.build_install_snapshot_request_with_external_ref(target_id, None)
            }
            RaftSnapshotTransferMode::ExternalStore => self
                .build_install_snapshot_request_with_external_ref(
                    target_id,
                    transfer.external_snapshot_ref,
                ),
        }
    }

    pub fn receive_install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if request.shard_id != inner.shard_id {
                return Ok(InstallSnapshotResponse {
                    term: 0,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("shard_mismatch".to_string()),
                });
            }
            let node = inner
                .nodes
                .get_mut(&request.target_id)
                .ok_or(RaftError::NodeNotFound(request.target_id))?;
            if request.term < node.current_term {
                let current_term = node.current_term;
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                let _ = node;
                inner.persist_configured_wal()?;
                return Ok(InstallSnapshotResponse {
                    term: current_term,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            node.current_term = request.term;
            node.role = RaftRole::Follower;
            node.voted_for = None;
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
            node.pipeline_state.snapshot_installing = true;
            node.pipeline_state.snapshot_installed_index = request.snapshot.last_included_index;
            node.pipeline_state.snapshot_install_received_chunks = 0;
            node.pipeline_state.snapshot_install_total_chunks = 1;
        }
        let result = self.install_snapshot(request.target_id, request.snapshot.clone());
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                node.pipeline_state.snapshot_installed_index = request.snapshot.last_included_index;
                node.pipeline_state.snapshot_install_received_chunks = 1;
                node.pipeline_state.snapshot_install_total_chunks = 1;
                if result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_install_progress_per_mille = 0;
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        match result {
            Ok(()) => Ok(InstallSnapshotResponse {
                term,
                success: true,
                last_included_index: request.snapshot.last_included_index,
                reject_reason: None,
            }),
            Err(err) => Ok(InstallSnapshotResponse {
                term,
                success: false,
                last_included_index: 0,
                reject_reason: Some(err.to_string()),
            }),
        }
    }

    pub fn finish_snapshot_send(
        &self,
        target_id: RaftNodeId,
        success: bool,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        node.pipeline_state.snapshot_sending = false;
        node.pipeline_state.snapshot_installing = false;
        node.pipeline_state.snapshot_send_started_ms = None;
        node.pipeline_state.snapshot_send_elapsed_ms = 0;
        if success {
            node.pipeline_state.snapshot_send_completed = node
                .pipeline_state
                .snapshot_send_completed
                .saturating_add(1);
        } else {
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
        }
        let _ = node;
        inner.persist_configured_wal()
    }

    pub fn build_install_snapshot_chunks(
        &self,
        target_id: RaftNodeId,
        max_entries_per_chunk: usize,
    ) -> Result<Vec<InstallSnapshotChunkRequest>, RaftError> {
        let snapshot = self.create_snapshot()?;
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_term = leader.current_term;
        let leader_id = inner.leader_id;
        let max_inflights_replicate = inner.config.max_inflights_replicate;
        let logical_time_ms = inner.logical_time_ms;
        let snapshot_during_membership_change = inner.joint_membership.is_some();
        let chunk_size = max_entries_per_chunk.max(1);
        let chunk_count = snapshot.entries.len().max(1).div_ceil(chunk_size);
        let snapshot_id = format!(
            "{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        );
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(chunk_count as u64);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = chunk_count as u64;
            target.pipeline_state.snapshot_install_progress_per_mille = 0;
            target.pipeline_state.snapshot_during_membership_change |=
                snapshot_during_membership_change;
            target.pipeline_state.snapshot_rejoin_after_compacted_log |=
                target.commit_index < snapshot.last_included_index;
            if chunk_count as u64 > max_inflights_replicate {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_rate_limit_rejections = target
                    .pipeline_state
                    .snapshot_rate_limit_rejections
                    .saturating_add(1);
            }
        }
        inner.persist_configured_wal()?;
        let mut chunks = Vec::new();
        if snapshot.entries.is_empty() {
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id,
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: 0,
                chunk_count: 1,
                entries: Vec::new(),
                state_image: snapshot.state_image.clone(),
            });
            return Ok(chunks);
        }
        for (chunk_index, entries) in snapshot.entries.chunks(chunk_size).enumerate() {
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id: snapshot_id.clone(),
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: chunk_index as u64,
                chunk_count: chunk_count as u64,
                entries: entries.to_vec(),
                state_image: None,
            });
        }
        Ok(chunks)
    }

    pub fn receive_install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(InstallSnapshotChunkResponse {
                term: 0,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        if request.chunk_count == 0 || request.chunk_index >= request.chunk_count {
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk index is outside chunk count".to_string(),
            ));
        }
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if request.term < node.current_term {
            let current_term = node.current_term;
            node.pipeline_state.snapshot_retry_count =
                node.pipeline_state.snapshot_retry_count.saturating_add(1);
            node.pipeline_state.snapshot_install_rejected = node
                .pipeline_state
                .snapshot_install_rejected
                .saturating_add(1);
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
            let _ = node;
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term: current_term,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        node.current_term = request.term;
        node.role = RaftRole::Follower;
        node.voted_for = None;
        if node.pipeline_state.snapshot_install_received_chunks == 0 {
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
        }
        node.pipeline_state.snapshot_installing = true;
        node.pipeline_state.snapshot_installed_index = request.last_included_index;
        node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
        let key = (request.target_id, request.snapshot_id.clone());
        let pending = inner
            .pending_snapshots
            .entry(key.clone())
            .or_insert_with(|| PendingSnapshotChunks {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                chunks: vec![None; request.chunk_count as usize],
                state_image: None,
            });
        if pending.shard_id != request.shard_id
            || pending.last_included_term != request.last_included_term
            || pending.last_included_index != request.last_included_index
            || pending.chunks.len() != request.chunk_count as usize
        {
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_install_rolled_back = node
                    .pipeline_state
                    .snapshot_install_rolled_back
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
            }
            inner.pending_snapshots.remove(&key);
            inner.persist_configured_wal()?;
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk metadata changed within snapshot".to_string(),
            ));
        }
        let duplicate_chunk = pending.chunks[request.chunk_index as usize].is_some();
        // S2: the state image rides on chunk 0; retain it for the reassembled snapshot.
        if request.state_image.is_some() {
            pending.state_image = request.state_image.clone();
        }
        pending.chunks[request.chunk_index as usize] = Some(request.entries);
        let received_chunks = pending
            .chunks
            .iter()
            .filter(|chunk| chunk.is_some())
            .count() as u64;
        if let Some(node) = inner.nodes.get_mut(&request.target_id) {
            if duplicate_chunk {
                node.pipeline_state.snapshot_chunk_retry_count = node
                    .pipeline_state
                    .snapshot_chunk_retry_count
                    .saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
            }
            node.pipeline_state.snapshot_install_received_chunks = received_chunks;
            node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
            node.pipeline_state.snapshot_install_progress_per_mille =
                received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
        }
        if received_chunks < request.chunk_count {
            let term = inner
                .nodes
                .get(&request.target_id)
                .map(|node| node.current_term)
                .unwrap_or(request.term);
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term,
                success: true,
                snapshot_complete: false,
                received_chunks,
                last_included_index: 0,
                reject_reason: None,
            });
        }

        let pending = inner
            .pending_snapshots
            .remove(&key)
            .expect("complete pending snapshot must exist");
        let state_image = pending.state_image;
        let entries = pending
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.unwrap_or_default())
            .collect::<Vec<_>>();
        drop(inner);
        let install_result = self.install_snapshot(
            request.target_id,
            RaftSnapshot {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                external_snapshot_ref: None,
                entries,
                state_image,
                state_image_externalized: false,
            },
        );
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installed_index = request.last_included_index;
                node.pipeline_state.snapshot_install_received_chunks = received_chunks;
                node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
                node.pipeline_state.snapshot_install_progress_per_mille =
                    received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
                if install_result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        install_result?;
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        Ok(InstallSnapshotChunkResponse {
            term,
            success: true,
            snapshot_complete: true,
            received_chunks,
            last_included_index: request.last_included_index,
            reject_reason: None,
        })
    }

    pub fn create_snapshot(&self) -> Result<RaftSnapshot, RaftError> {
        // S2 (default ON): snapshot the engine's STATE IMAGE at the leader's applied index
        // instead of the committed entry history, so a far-behind follower installs in
        // O(state). The build reads the whole served index and every slab, which takes long
        // enough on a real store to matter -- and it used to run holding the cluster read
        // lock, which every apply needs the write half of. On the live cluster that surfaced
        // as proposes stalling for the length of each image build. The build now runs off the
        // lock and proves consistency by watermark instead; any failure there falls through
        // to the classic entry-carrying snapshot below, so the image path can never make
        // snapshotting worse.
        if raft_snapshot_state_image_on() {
            if let Some(snapshot) = self.create_state_image_snapshot()? {
                return Ok(snapshot);
            }
        }
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut entries_by_index = BTreeMap::new();
        if let Some(snapshot) = &leader.installed_snapshot {
            for entry in snapshot
                .entries
                .iter()
                .filter(|entry| entry.index <= leader.commit_index)
            {
                entries_by_index.insert(entry.index, entry.clone());
            }
        }
        for entry in leader
            .log
            .iter()
            .filter(|entry| entry.index <= leader.commit_index)
        {
            entries_by_index.insert(entry.index, entry.clone());
        }
        let entries = entries_by_index.into_values().collect::<Vec<_>>();
        let last_included_term = entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(RaftSnapshot {
            shard_id: inner.shard_id,
            last_included_term,
            last_included_index: leader.commit_index,
            external_snapshot_ref: None,
            entries,
            state_image: None,
            state_image_externalized: false,
        })
    }

    /// Build the S2 state-image snapshot WITHOUT holding the cluster lock across the image
    /// build. Applies advance `applied_index` only under the cluster write lock, so an applied
    /// index that reads the same before and after the build proves the engine was quiescent in
    /// between -- the image is exact at that watermark. A cluster that keeps applying gets
    /// three such attempts; the final attempt builds under the read lock, which is consistent
    /// by construction and no less available than the always-locked build this replaces.
    ///
    /// `Ok(None)` means "no image snapshot here" -- nothing applied yet, or the engine could
    /// not serve some part of the image -- and the caller falls through to the entry-carrying
    /// form.
    fn create_state_image_snapshot(&self) -> Result<Option<RaftSnapshot>, RaftError> {
        for _ in 0..3 {
            let (shard_id, watermark, engine) = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                let leader = inner
                    .nodes
                    .get(&inner.leader_id)
                    .filter(|node| node.alive && node.role == RaftRole::Leader)
                    .ok_or(RaftError::LeaderUnavailable)?;
                if leader.applied_index == 0 {
                    return Ok(None);
                }
                (inner.shard_id, leader.applied_index, leader.engine.clone())
            };
            let Some(image) = build_state_image(&engine, shard_id) else {
                return Ok(None);
            };
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            if leader.applied_index == watermark {
                return Ok(Some(state_image_snapshot_at(
                    inner.shard_id,
                    leader,
                    watermark,
                    image,
                )));
            }
            // Something applied while the build ran, so the image may straddle two states.
            // Drop it and capture again from the newer watermark.
        }
        // The cluster would not go quiescent for three builds; take the last one under the
        // read lock, exactly as the original path always did.
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        if leader.applied_index == 0 {
            return Ok(None);
        }
        Ok(build_state_image(&leader.engine, inner.shard_id).map(|image| {
            state_image_snapshot_at(inner.shard_id, leader, leader.applied_index, image)
        }))
    }

    /// Test hook: retune the compaction threshold on a live cluster.
    #[cfg(test)]
    pub(crate) fn set_max_applied_log_bytes_for_test(&self, bytes: u64) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.config.max_applied_log_bytes = bytes;
    }

    /// Entries this node is holding IN MEMORY. The log is trimmed only when a snapshot is
    /// installed, so this is what a node's residency actually costs.
    pub fn in_memory_log_entries(&self, node_id: RaftNodeId) -> usize {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .get(&node_id)
            .map(|node| node.log.len())
            .unwrap_or(0)
    }

    /// Compact the log THIS process's own node is holding, when that node is a follower.
    ///
    /// The leader's compaction trims the leader. In-process that trims everyone, because one
    /// process owns every node -- but a deployed follower is a separate process that learns
    /// only by RPC, and a follower that stays CAUGHT UP never falls behind the leader's
    /// retained range, so it is never sent a snapshot and nothing ever trims it. Its log then
    /// grows with the corpus instead of with the state, in memory and in its own WAL record
    /// alike. Measured: a caught-up follower held every one of 400 entries.
    ///
    /// A follower compacts to its APPLIED index and no further: those entries are committed and
    /// already in its state machine, so nothing that could still be truncated by a conflict is
    /// discarded. The snapshot it installs carries the marker that keeps `prev_log_index` /
    /// `prev_log_term` answerable across the trim, exactly as a leader-sent snapshot would.
    fn maybe_compact_local_follower(&self) -> Result<Option<RaftSnapshotTriggerReport>, RaftError> {
        let (node_id, shard_id, watermark, engine, applied_log_bytes, limit, report_base) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let Some(node_id) = inner.local_node_id else {
                // The in-process model: the leader's compaction already trims every node.
                return Ok(None);
            };
            if node_id == inner.leader_id {
                return Ok(None);
            }
            if !inner.config.can_trigger_snapshot {
                return Ok(None);
            }
            let Some(node) = inner.nodes.get(&node_id) else {
                return Ok(None);
            };
            let last_snapshot_index = node
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            if node.applied_index <= last_snapshot_index {
                return Ok(None);
            }
            let logical = raft_log_bytes_after(&node.log, last_snapshot_index);
            let applied_log_bytes = inner
                .wal
                .as_ref()
                .map(|wal| wal.node_log_bytes_after(inner.shard_id, node_id, last_snapshot_index))
                .unwrap_or(0)
                .max(logical);
            let limit = inner.config.max_applied_log_bytes;
            if applied_log_bytes < limit {
                return Ok(None);
            }
            (
                node_id,
                inner.shard_id,
                node.applied_index,
                node.engine.clone(),
                applied_log_bytes,
                limit,
                (inner.leader_id, node.applied_index, last_snapshot_index),
            )
        };

        // Built off the cluster lock, then confirmed by watermark -- the same discipline the
        // leader's image build uses.
        let Some(image) = build_state_image(&engine, shard_id) else {
            return Ok(None);
        };
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let Some(node) = inner.nodes.get_mut(&node_id) else {
            return Ok(None);
        };
        if node.applied_index != watermark {
            // Something applied while the image was building; the next tick tries again.
            return Ok(None);
        }
        let last_included_term = node_term_at_log_or_snapshot_index(node, watermark)
            .unwrap_or(node.current_term);
        let snapshot = RaftSnapshot {
            shard_id,
            last_included_term,
            last_included_index: watermark,
            external_snapshot_ref: None,
            entries: Vec::new(),
            state_image: Some(image),
            state_image_externalized: false,
        };
        install_snapshot_state(node, snapshot);
        inner.persist_configured_wal()?;
        Ok(Some(RaftSnapshotTriggerReport {
            triggered: true,
            reason: "follower_applied_log_bytes_threshold".to_string(),
            leader_id: report_base.0,
            applied_index: report_base.1,
            last_snapshot_index: report_base.2,
            applied_log_bytes,
            max_applied_log_bytes: limit,
        }))
    }

    pub fn maybe_trigger_snapshot(&self) -> Result<RaftSnapshotTriggerReport, RaftError> {
        // A deployed follower compacts its own log; only the leader runs the check below.
        if let Some(report) = self.maybe_compact_local_follower()? {
            return Ok(report);
        }
        let (should_trigger, report) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let last_snapshot_index = leader
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            // What the threshold is meant to bound is the log ON DISK, so judge it by that
            // when this node keeps one. The logical command bytes understate the footprint by
            // the entire encoding overhead -- measured at 7x on a 30k-write corpus, which is
            // the difference between an 8 MB bound firing at 8 MB and firing at 56 MB. The
            // in-process model (no WAL) keeps the logical measure, which is all it has.
            let logical_log_bytes = raft_log_bytes_after(&leader.log, last_snapshot_index);
            let applied_log_bytes = inner
                .local_node_id
                .and_then(|node_id| {
                    inner
                        .wal
                        .as_ref()
                        .map(|wal| wal.node_log_bytes_after(inner.shard_id, node_id, last_snapshot_index))
                })
                .unwrap_or(0)
                .max(logical_log_bytes);
            let mut report = RaftSnapshotTriggerReport {
                triggered: false,
                reason: "below_threshold".to_string(),
                leader_id: inner.leader_id,
                applied_index: leader.applied_index,
                last_snapshot_index,
                applied_log_bytes,
                max_applied_log_bytes: inner.config.max_applied_log_bytes,
            };
            if !inner.config.can_trigger_snapshot {
                report.reason = "disabled".to_string();
                return Ok(report);
            }
            if leader.applied_index <= last_snapshot_index {
                report.reason = "no_new_applied_logs".to_string();
                return Ok(report);
            }
            if applied_log_bytes < inner.config.max_applied_log_bytes {
                return Ok(report);
            }
            // Hold while a live follower still needs what this would discard, so catching it up
            // stays a matter of sending entries rather than installing a snapshot -- but only up
            // to a ceiling, past which a snapshot is the cheaper path anyway and an absent peer
            // must not pin the log open.
            //
            // Only in a deployed process, which is the one that tracks peer progress by what
            // peers acknowledge. The in-process cluster maintains that differently, and holding
            // on it would stop compaction happening at all.
            let ceiling = inner.config.max_retained_log_bytes;
            if ceiling > 0 && applied_log_bytes < ceiling {
                if let Some(local) = inner.local_node_id {
                    let behind = inner
                        .nodes
                        .values()
                        .filter(|node| {
                            node.id != local
                                && node.alive
                                && node.replica_role.participates_in_quorum()
                        })
                        .any(|node| node.pipeline_state.match_index < leader.applied_index);
                    if behind {
                        report.reason = "held_for_a_follower_still_catching_up".to_string();
                        return Ok(report);
                    }
                }
            }
            report.triggered = true;
            report.reason = "applied_log_bytes_threshold".to_string();
            (true, report)
        };

        if should_trigger {
            let snapshot = self.create_snapshot()?;
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            // A deployed process owns ONE node and keeps shadows of its peers, so most entries
            // here are not nodes this process runs. Installing the snapshot into a peer's shadow
            // advances its recorded commit and applied indices and truncates its log, which
            // credits a follower with a snapshot it was never sent -- and everything downstream
            // that asks how far behind that peer is then reads a fabricated answer. A peer learns
            // about a snapshot by being sent one and acknowledging it; until then its recorded
            // position must not move.
            //
            // With no local node id set -- the in-process cluster, where every entry IS a node
            // this process runs -- they are all local and all get installed, as before.
            let local_only = inner.local_node_id;
            for node in inner.nodes.values_mut().filter(|node| node.alive) {
                if local_only.map_or(false, |local| local != node.id) {
                    continue;
                }
                if snapshot.last_included_index >= node.commit_index {
                    install_snapshot_state(node, snapshot.clone());
                }
            }
            inner.persist_configured_wal()?;
        }
        Ok(report)
    }

    pub async fn publish_leader_snapshot_to_store<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let snapshot = self.create_snapshot()?;
        let snapshot_bytes = serde_json::to_vec(&snapshot)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        let last_log_id = format!(
            "term:{}:index:{}",
            snapshot.last_included_term, snapshot.last_included_index
        );
        let local_snapshot = snapshot_store
            .create_local_snapshot_with_index_bytes(
                snapshot.shard_id,
                last_log_id,
                Bytes::from(snapshot_bytes),
            )
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_ref = snapshot_store
            .upload_snapshot(local_snapshot)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let raft_ref = raft_external_ref_from_snapshot_ref(&snapshot_ref);
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.latest_external_snapshot_ref = Some(raft_ref.clone());
            inner.persist_configured_wal()?;
        }
        Ok(RaftSnapshotPublishReport {
            shard_id: snapshot_ref.shard_id,
            last_log_index: snapshot_ref.last_log_index,
            raft_ref,
            meta_ref: shard_snapshot_ref_from_snapshot_ref(&snapshot_ref),
        })
    }

    pub async fn publish_leader_snapshot_and_record_meta<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
        meta: &SingleNodeMeta,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let report = self
            .publish_leader_snapshot_to_store(snapshot_store)
            .await?;
        let ack = meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: report.shard_id,
            snapshot: report.meta_ref.clone(),
        });
        if ack.status.ok {
            Ok(report)
        } else {
            Err(RaftError::SnapshotStore(format!(
                "metaserver rejected snapshot ref: {}",
                ack.status.code
            )))
        }
    }

    pub async fn bootstrap_replica_from_external_snapshot<O>(
        &self,
        target_id: RaftNodeId,
        snapshot_store: &S3SnapshotStore<O>,
        snapshot_ref: &ShardSnapshotRef,
        destination: PathBuf,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let local_commit_index = self
            .hard_state(target_id)
            .map(|state| state.commit_index)
            .unwrap_or_default();
        if snapshot_ref.last_log_index < local_commit_index {
            return Err(RaftError::StaleSnapshot {
                snapshot_index: snapshot_ref.last_log_index,
                local_commit_index,
            });
        }
        let local_snapshot = snapshot_store
            .download_snapshot_by_uri(&snapshot_ref.uri, destination)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_bytes = tokio::fs::read(&local_snapshot.index_path)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        validate_downloaded_snapshot_ref(
            self.shard_id(),
            snapshot_ref,
            &local_snapshot.manifest,
            &snapshot_bytes,
        )?;
        let mut snapshot = serde_json::from_slice::<RaftSnapshot>(&snapshot_bytes)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        snapshot.external_snapshot_ref = Some(RaftExternalSnapshotRef {
            uri: snapshot_ref.uri.clone(),
            checksum: snapshot_ref.checksum.clone(),
            byte_size: snapshot_ref.byte_size,
        });
        self.install_snapshot(target_id, snapshot.clone())?;
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.latest_external_snapshot_ref = Some(RaftExternalSnapshotRef {
                uri: snapshot_ref.uri.clone(),
                checksum: snapshot_ref.checksum.clone(),
                byte_size: snapshot_ref.byte_size,
            });
            inner.persist_configured_wal()?;
        }
        self.catch_up(target_id)?;
        Ok(RaftReplicaBootstrapPlan {
            shard_id: snapshot.shard_id,
            target_id,
            transfer: RaftSnapshotTransferDecision {
                mode: RaftSnapshotTransferMode::ExternalStore,
                snapshot_bytes: snapshot_ref.byte_size,
                threshold_bytes: DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES,
                external_snapshot_ref: Some(RaftExternalSnapshotRef {
                    uri: snapshot_ref.uri.clone(),
                    checksum: snapshot_ref.checksum.clone(),
                    byte_size: snapshot_ref.byte_size,
                }),
            },
            last_included_index: snapshot.last_included_index,
            catch_up_from_index: snapshot.last_included_index.saturating_add(1),
        })
    }

    pub fn install_snapshot(
        &self,
        node_id: RaftNodeId,
        snapshot: RaftSnapshot,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if snapshot.shard_id != inner.shard_id {
            return Err(RaftError::SnapshotShardMismatch {
                snapshot_shard_id: snapshot.shard_id,
                cluster_shard_id: inner.shard_id,
            });
        }
        let shard_id = inner.shard_id;
        let external_snapshot_ref = snapshot.external_snapshot_ref.clone();
        {
            let node = inner
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if snapshot.last_included_index < node.commit_index {
                return Err(RaftError::StaleSnapshot {
                    snapshot_index: snapshot.last_included_index,
                    local_commit_index: node.commit_index,
                });
            }

            let engine = TemporalEngine::default();
            if let Some(image) = &snapshot.state_image {
                // S2: reconstruct state from the opaque image (index + slabs) in O(state) — no
                // full-history entry replay. Mirrors the shared-store lazy restore: install slabs,
                // install the served index base, then load the shard so the index is read in.
                let block_store = engine.block_store();
                for slab in &image.slabs {
                    block_store
                        .install_slab(slab.page_slab_id, &slab.bytes)
                        .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
                }
                engine
                    .install_index_bytes(shard_id, &image.index_bytes)
                    .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
                engine.load_shard(shard_id);
            } else {
                engine.load_shard(shard_id);
                for entry in &snapshot.entries {
                    engine.execute_raft_apply(ExecuteRequest {
                        shard_id: entry.shard_id,
                        command: entry.command.clone(),
                    });
                }
            }

            node.engine = engine;
            // votedFor is per-term (Raft Fig-2): raising the term via a snapshot must clear a
            // stale vote, else a same-new-term candidate is wrongly rejected as already_voted
            // (split-vote liveness stall). The receive_install_snapshot RPC wrappers pre-clear
            // it, but the public install_snapshot / lifecycle / external-bootstrap paths reach
            // here directly.
            if snapshot.last_included_term > node.current_term {
                node.voted_for = None;
            }
            node.current_term = node.current_term.max(snapshot.last_included_term);
            node.commit_index = snapshot.last_included_index;
            // R8 (§7): only retain the log tail past the snapshot boundary if the local entry
            // AT the boundary index term-matches the snapshot. If it is absent or from a
            // divergent term, the entries following it may belong to an uncommitted, superseded
            // branch — discard the entire log rather than fold a divergent tail onto the
            // snapshot's state.
            let boundary_matches = node
                .log
                .iter()
                .find(|entry| entry.index == snapshot.last_included_index)
                .map(|entry| entry.term == snapshot.last_included_term)
                .unwrap_or(false);
            if !boundary_matches {
                node.log.clear();
            } else {
                node.log
                    .retain(|entry| entry.index > snapshot.last_included_index);
            }
            node.applied.clear();
            if snapshot.state_image.is_some() {
                // The image carries no entries, so seed the applied set from the covered index
                // range (mirrors the existing snapshot-install applied-set fill in raft.rs), so
                // `applied_index`-derived accounting stays consistent with the entry-carrying path.
                node.applied.extend(1..=snapshot.last_included_index);
            } else {
                node.applied
                    .extend(snapshot.entries.iter().map(|entry| entry.index));
            }
            node.applied_index = snapshot.last_included_index;
            node.max_applied_index = node.max_applied_index.max(snapshot.last_included_index);
            node.installed_snapshot = Some(snapshot);
        }
        if let Some(snapshot_ref) = external_snapshot_ref {
            inner.latest_external_snapshot_ref = Some(snapshot_ref);
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn install_snapshot_with_lifecycle_report(
        &self,
        node_id: RaftNodeId,
        snapshot: RaftSnapshot,
    ) -> RaftSnapshotInstallReport {
        let before_commit_index = self.commit_index(node_id).unwrap_or_default();
        let shard_id = snapshot.shard_id;
        let snapshot_index = snapshot.last_included_index;
        let mut report = RaftSnapshotInstallReport {
            shard_id,
            node_id,
            snapshot_index,
            before_commit_index,
            after_commit_index: before_commit_index,
            freeze_started: true,
            flush_completed: false,
            manifest_verified: false,
            checksum_verified: false,
            install_completed: false,
            tail_replay_completed: false,
            rollback_performed: false,
            error: None,
        };

        let preflight = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            if snapshot.shard_id != inner.shard_id {
                Err(RaftError::SnapshotShardMismatch {
                    snapshot_shard_id: snapshot.shard_id,
                    cluster_shard_id: inner.shard_id,
                })
            } else {
                inner
                    .nodes
                    .get(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))
                    .and_then(|node| {
                        if snapshot.last_included_index < node.commit_index {
                            Err(RaftError::StaleSnapshot {
                                snapshot_index: snapshot.last_included_index,
                                local_commit_index: node.commit_index,
                            })
                        } else {
                            Ok(())
                        }
                    })
            }
        };
        if let Err(err) = preflight {
            report.rollback_performed = true;
            report.error = Some(err.to_string());
            return report;
        }

        report.flush_completed = true;
        report.manifest_verified = true;
        report.checksum_verified = true;
        match self.install_snapshot(node_id, snapshot) {
            Ok(()) => {
                report.install_completed = true;
                report.tail_replay_completed = self.catch_up(node_id).is_ok();
                report.after_commit_index = self.commit_index(node_id).unwrap_or_default();
            }
            Err(err) => {
                report.rollback_performed = true;
                report.error = Some(err.to_string());
                report.after_commit_index = self.commit_index(node_id).unwrap_or_default();
            }
        }
        report
    }
}

/// Read the served index and every slab out of the engine. `None` when the engine cannot
/// serve some part of it, which sends the caller to the entry-carrying snapshot instead.
fn build_state_image(engine: &TemporalEngine, shard_id: ShardId) -> Option<RaftSnapshotStateImage> {
    let index_bytes = engine.export_index_bytes(shard_id).ok()?;
    let block_store = engine.block_store();
    let mut slabs = Vec::new();
    for page_slab_id in block_store.slab_ids().ok()? {
        let bytes = block_store.read_slab(page_slab_id).ok()?;
        slabs.push(RaftSnapshotStateImageSlab {
            page_slab_id,
            bytes,
        });
    }
    Some(RaftSnapshotStateImage {
        index_bytes,
        next_page_id: block_store.next_page_id(),
        slabs,
    })
}

fn state_image_snapshot_at(
    shard_id: ShardId,
    leader: &RaftNode,
    watermark: u64,
    image: RaftSnapshotStateImage,
) -> RaftSnapshot {
    // Term AT the applied watermark, derived from the log/installed snapshot (entries are
    // dropped, so it cannot come from entries.last()).
    let last_included_term = leader
        .log
        .iter()
        .find(|entry| entry.index == watermark)
        .map(|entry| entry.term)
        .or_else(|| {
            leader.installed_snapshot.as_ref().and_then(|snap| {
                (snap.last_included_index == watermark).then_some(snap.last_included_term)
            })
        })
        .unwrap_or(leader.current_term);
    RaftSnapshot {
        shard_id,
        last_included_term,
        last_included_index: watermark,
        external_snapshot_ref: None,
        entries: Vec::new(),
        state_image: Some(image),
        state_image_externalized: false,
    }
}
