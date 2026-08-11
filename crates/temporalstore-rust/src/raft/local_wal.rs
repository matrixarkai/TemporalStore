// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalRaftWal WAL persistence/recovery methods, split from raft.rs.
use super::*;

impl LocalRaftWal {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn persist_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)
    }

    pub fn append_node_record(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        let path = self.node_path(shard_id, node_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let recovery = self.recover_node(shard_id, node_id)?;
        let envelope = RaftWalEnvelope {
            sequence: recovery.valid_records as u64 + 1,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        serde_json::to_writer(&mut file, &envelope).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    pub fn persist_node_with_retention(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        keep_last: usize,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)?;
        self.compact_node_records(shard_id, node_id, keep_last)
    }

    pub fn persist_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
    ) -> io::Result<RaftWalSegmentReport> {
        self.persist_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            u64::MAX,
        )
    }

    pub fn persist_node_segmented_with_fsync_threshold(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
        slow_fsync_threshold_ms: u64,
    ) -> io::Result<RaftWalSegmentReport> {
        let max_segment_bytes = max_segment_bytes.max(1);
        let min_keep_segments = min_keep_segments.max(1);
        let segment_dir = self.node_segment_dir(shard_id, node_id);
        fs::create_dir_all(&segment_dir)?;
        let recovery = self.recover_node_segmented(shard_id, node_id)?;
        let sequence = recovery.valid_records as u64 + 1;
        let envelope = RaftWalEnvelope {
            sequence,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
        };
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &envelope).map_err(io::Error::other)?;
        encoded.push(b'\n');

        let mut active_segment_id = self
            .node_segments(shard_id, node_id)?
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(1);
        let mut active_path = self.node_segment_path(shard_id, node_id, active_segment_id);
        let active_len = fs::metadata(&active_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        if active_len > 0 && active_len + encoded.len() as u64 > max_segment_bytes {
            active_segment_id += 1;
            active_path = self.node_segment_path(shard_id, node_id, active_segment_id);
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        file.write_all(&encoded)?;
        let fsync_started = Instant::now();
        file.sync_data()?;
        let last_fsync_elapsed_ms = fsync_started.elapsed().as_millis() as u64;
        let before_prune_segments = self.node_segments(shard_id, node_id)?.len();
        self.prune_node_segments(shard_id, node_id, min_keep_segments)?;
        let after_prune_segments = self.node_segments(shard_id, node_id)?.len();
        let mut report = self.segment_report(shard_id, node_id)?;
        report.released_segment_count =
            before_prune_segments.saturating_sub(after_prune_segments) as u64;
        report.last_fsync_elapsed_ms = last_fsync_elapsed_ms;
        report.slow_fsync_backpressure_observed = last_fsync_elapsed_ms >= slow_fsync_threshold_ms;
        self.persist_segment_runtime_state(shard_id, node_id, &report)?;
        Ok(report)
    }

    pub fn recover_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        let segments = self.node_segments(shard_id, node_id)?;
        if segments.is_empty() {
            return self.recover_node_legacy(shard_id, node_id);
        }

        let mut last_record = None;
        let mut valid_records = 0usize;
        let mut truncated_bytes = 0u64;
        let mut corrupt_tail = false;
        for segment in segments {
            let bytes = fs::read(&segment.path)?;
            let mut offset = 0usize;
            let mut valid_until = 0usize;
            while offset < bytes.len() {
                let remaining = &bytes[offset..];
                let line_len = remaining
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map(|pos| pos + 1)
                    .unwrap_or(remaining.len());
                let raw_line = &remaining[..line_len];
                let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
                if line.is_empty() {
                    valid_until = offset + line_len;
                    offset += line_len;
                    continue;
                }
                let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                    break;
                };
                if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                    break;
                }
                valid_records += 1;
                last_record = Some(envelope.record);
                valid_until = offset + line_len;
                offset += line_len;
            }
            let segment_truncated = bytes.len().saturating_sub(valid_until) as u64;
            if segment_truncated > 0 {
                OpenOptions::new()
                    .write(true)
                    .open(&segment.path)?
                    .set_len(valid_until as u64)?;
                truncated_bytes += segment_truncated;
                corrupt_tail = true;
                break;
            }
        }

        Ok(RaftWalRecovery {
            record: last_record,
            valid_records,
            truncated_bytes,
            corrupt_tail,
        })
    }

    pub fn compact_node_records(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        keep_last: usize,
    ) -> io::Result<()> {
        let keep_last = keep_last.max(1);
        let path = self.node_path(shard_id, node_id);
        if !path.exists() {
            return Ok(());
        }
        let bytes = fs::read(&path)?;
        let mut envelopes = Vec::new();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let line_len = remaining
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|pos| pos + 1)
                .unwrap_or(remaining.len());
            let raw_line = &remaining[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            if line.is_empty() {
                offset += line_len;
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                break;
            };
            if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                break;
            }
            envelopes.push(envelope);
            offset += line_len;
        }
        if envelopes.len() <= keep_last {
            return Ok(());
        }
        let retained = envelopes.split_off(envelopes.len() - keep_last);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        for envelope in retained {
            serde_json::to_writer(&mut file, &envelope).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
        }
        file.sync_data()?;
        Ok(())
    }

    pub fn load_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<Option<RaftWalRecord>> {
        self.recover_node_segmented(shard_id, node_id)
            .map(|recovery| recovery.record)
    }

    pub fn recover_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        self.recover_node_segmented(shard_id, node_id)
    }

    pub(super) fn recover_node_legacy(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        let path = self.node_path(shard_id, node_id);
        if !path.exists() {
            return Ok(RaftWalRecovery {
                record: None,
                valid_records: 0,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }
        let bytes = fs::read(&path)?;
        if bytes.is_empty() {
            return Ok(RaftWalRecovery {
                record: None,
                valid_records: 0,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }
        if let Ok(record) = serde_json::from_slice::<RaftWalRecord>(&bytes) {
            return Ok(RaftWalRecovery {
                record: Some(record),
                valid_records: 1,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }

        let mut offset = 0usize;
        let mut valid_until = 0usize;
        let mut last_record = None;
        let mut valid_records = 0usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let line_len = remaining
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|pos| pos + 1)
                .unwrap_or(remaining.len());
            let raw_line = &remaining[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            if line.is_empty() {
                valid_until = offset + line_len;
                offset += line_len;
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                break;
            };
            if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                break;
            }
            valid_records += 1;
            last_record = Some(envelope.record);
            valid_until = offset + line_len;
            offset += line_len;
        }

        let truncated_bytes = bytes.len().saturating_sub(valid_until) as u64;
        if truncated_bytes > 0 {
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(valid_until as u64)?;
        }
        Ok(RaftWalRecovery {
            record: last_record,
            valid_records,
            truncated_bytes,
            corrupt_tail: truncated_bytes > 0,
        })
    }

    pub(super) fn node_path(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.root
            .join(format!("shard-{shard_id}"))
            .join(format!("node-{node_id}.json"))
    }

    pub(super) fn node_segment_dir(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.root
            .join(format!("shard-{shard_id}"))
            .join(format!("node-{node_id}.segments"))
    }

    pub(super) fn node_segment_path(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        segment_id: u64,
    ) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join(format!("{segment_id:020}.wal"))
    }

    pub(super) fn node_segment_runtime_state_path(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join("segment-runtime-state.json")
    }

    pub(super) fn persist_segment_runtime_state(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        report: &RaftWalSegmentReport,
    ) -> io::Result<()> {
        let state = RaftWalSegmentRuntimeState {
            released_segment_count: report.released_segment_count,
            last_fsync_elapsed_ms: report.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: report.slow_fsync_backpressure_observed,
        };
        let encoded = serde_json::to_vec_pretty(&state).map_err(io::Error::other)?;
        fs::write(
            self.node_segment_runtime_state_path(shard_id, node_id),
            encoded,
        )
    }

    pub(super) fn read_segment_runtime_state(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> RaftWalSegmentRuntimeState {
        fs::read(self.node_segment_runtime_state_path(shard_id, node_id))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<RaftWalSegmentRuntimeState>(&bytes).ok())
            .unwrap_or_default()
    }

    pub(super) fn node_segments(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<Vec<RaftWalSegmentInfo>> {
        let dir = self.node_segment_dir(shard_id, node_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut segments = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(segment_id) = stem.parse::<u64>() else {
                continue;
            };
            let (record_count, first_sequence, last_sequence, first_log_index, last_log_index) =
                Self::inspect_segment_sequences(&path)?;
            segments.push(RaftWalSegmentInfo {
                segment_id,
                bytes: entry.metadata()?.len(),
                record_count,
                first_sequence,
                last_sequence,
                first_log_index,
                last_log_index,
                path: path.to_string_lossy().into_owned(),
            });
        }
        segments.sort_by_key(|segment| segment.segment_id);
        Ok(segments)
    }

    pub(super) fn inspect_segment_sequences(path: &Path) -> io::Result<(u64, u64, u64, u64, u64)> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut record_count = 0u64;
        let mut first_sequence = 0u64;
        let mut last_sequence = 0u64;
        let mut first_log_index = 0u64;
        let mut last_log_index = 0u64;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let Ok(envelope) = serde_json::from_str::<RaftWalEnvelope>(&line) else {
                continue;
            };
            record_count = record_count.saturating_add(1);
            if first_sequence == 0 {
                first_sequence = envelope.sequence;
            }
            last_sequence = envelope.sequence;
            let record_first_log_index = envelope
                .record
                .entries
                .first()
                .map(|entry| entry.index)
                .unwrap_or(envelope.record.hard_state.commit_index);
            let record_last_log_index = envelope
                .record
                .entries
                .last()
                .map(|entry| entry.index)
                .unwrap_or(envelope.record.hard_state.commit_index);
            if first_log_index == 0
                || (record_first_log_index > 0 && record_first_log_index < first_log_index)
            {
                first_log_index = record_first_log_index;
            }
            last_log_index = last_log_index.max(record_last_log_index);
        }
        Ok((
            record_count,
            first_sequence,
            last_sequence,
            first_log_index,
            last_log_index,
        ))
    }

    pub(super) fn prune_node_segments(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        min_keep_segments: usize,
    ) -> io::Result<()> {
        let segments = self.node_segments(shard_id, node_id)?;
        if segments.len() <= min_keep_segments {
            return Ok(());
        }
        for segment in segments
            .iter()
            .take(segments.len().saturating_sub(min_keep_segments))
        {
            fs::remove_file(&segment.path)?;
        }
        Ok(())
    }

    pub fn segment_report(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalSegmentReport> {
        let segments = self.node_segments(shard_id, node_id)?;
        let runtime_state = self.read_segment_runtime_state(shard_id, node_id);
        let first_retained_log_index = segments
            .iter()
            .find_map(|segment| (segment.first_log_index > 0).then_some(segment.first_log_index))
            .unwrap_or_default();
        let last_retained_log_index = segments
            .iter()
            .rev()
            .find_map(|segment| (segment.last_log_index > 0).then_some(segment.last_log_index))
            .unwrap_or_default();
        Ok(RaftWalSegmentReport {
            active_segment_id: segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            segments,
            released_segment_count: runtime_state.released_segment_count,
            first_retained_log_index,
            last_retained_log_index,
            last_fsync_elapsed_ms: runtime_state.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: runtime_state.slow_fsync_backpressure_observed,
        })
    }
}
