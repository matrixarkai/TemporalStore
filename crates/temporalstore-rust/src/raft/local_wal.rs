// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalRaftWal WAL persistence/recovery methods, split from raft.rs.
use super::*;

impl LocalRaftWal {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursors: Arc::new(Mutex::new(BTreeMap::new())),
        }
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

        // O(1) append: use an in-memory cursor for the next sequence number and
        // active-segment offset instead of re-parsing the whole node log on every
        // append. The cursor is seeded ONCE from a disk scan (which also truncates a
        // corrupt tail exactly like the legacy first-append path) and then updated
        // incrementally. On-disk format, filenames, and the sequence values written
        // are byte-for-byte identical to the previous full-scan implementation.
        let mut cursors = self.cursors.lock().expect("raft wal cursor lock poisoned");
        if !cursors.contains_key(&(shard_id, node_id)) {
            let seeded = self.seed_node_cursor(shard_id, node_id)?;
            cursors.insert((shard_id, node_id), seeded);
        }
        let cursor = cursors
            .get_mut(&(shard_id, node_id))
            .expect("cursor seeded above");

        let sequence = cursor.next_sequence;
        let envelope = RaftWalEnvelope {
            sequence,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
        };
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &envelope).map_err(io::Error::other)?;
        encoded.push(b'\n');

        let mut active_segment_id = cursor
            .segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(1);
        let active_len = cursor
            .segments
            .last()
            .map(|segment| segment.bytes)
            .unwrap_or_default();
        let rotate = active_len > 0 && active_len + encoded.len() as u64 > max_segment_bytes;
        if rotate {
            active_segment_id += 1;
        }
        let active_path = self.node_segment_path(shard_id, node_id, active_segment_id);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        file.write_all(&encoded)?;
        let fsync_started = Instant::now();
        file.sync_data()?;
        let last_fsync_elapsed_ms = fsync_started.elapsed().as_millis() as u64;

        // Update the in-memory segment metadata to mirror what a fresh on-disk scan
        // (`node_segments` -> `inspect_segment_sequences`) would report.
        if rotate || cursor.segments.is_empty() {
            cursor.segments.push(RaftWalSegmentInfo {
                segment_id: active_segment_id,
                path: active_path.to_string_lossy().into_owned(),
                bytes: 0,
                record_count: 0,
                first_sequence: 0,
                last_sequence: 0,
                first_log_index: 0,
                last_log_index: 0,
            });
        }
        let (record_first_log_index, record_last_log_index) =
            Self::record_log_index_bounds(record);
        {
            let active = cursor
                .segments
                .last_mut()
                .expect("active segment present after push");
            active.bytes += encoded.len() as u64;
            active.record_count = active.record_count.saturating_add(1);
            if active.first_sequence == 0 {
                active.first_sequence = sequence;
            }
            active.last_sequence = sequence;
            if active.first_log_index == 0
                || (record_first_log_index > 0 && record_first_log_index < active.first_log_index)
            {
                active.first_log_index = record_first_log_index;
            }
            active.last_log_index = active.last_log_index.max(record_last_log_index);
        }
        // One more record is now durable on disk.
        cursor.next_sequence = cursor.next_sequence.saturating_add(1);

        // Prune the oldest segments, keeping at least `min_keep_segments`. Removing a
        // segment drops its records from the retained window, so the next sequence
        // number decreases by exactly those records -- matching the legacy behavior
        // where the sequence was `recover().valid_records + 1` over the surviving
        // segments only.
        let mut released_segment_count = 0u64;
        if cursor.segments.len() > min_keep_segments {
            let remove = cursor.segments.len() - min_keep_segments;
            let pruned: Vec<RaftWalSegmentInfo> = cursor.segments.drain(0..remove).collect();
            for segment in &pruned {
                fs::remove_file(&segment.path)?;
                cursor.next_sequence = cursor
                    .next_sequence
                    .saturating_sub(segment.record_count);
            }
            released_segment_count = pruned.len() as u64;
        }

        let slow_fsync_backpressure_observed = last_fsync_elapsed_ms >= slow_fsync_threshold_ms;
        cursor.released_segment_count = released_segment_count;
        cursor.last_fsync_elapsed_ms = last_fsync_elapsed_ms;
        cursor.slow_fsync_backpressure_observed = slow_fsync_backpressure_observed;

        let mut report = Self::segment_report_from_segments(&cursor.segments);
        report.released_segment_count = released_segment_count;
        report.last_fsync_elapsed_ms = last_fsync_elapsed_ms;
        report.slow_fsync_backpressure_observed = slow_fsync_backpressure_observed;
        self.persist_segment_runtime_state(shard_id, node_id, &report)?;
        Ok(report)
    }

    /// Seed a fresh in-memory cursor from disk. Runs the full recovery scan exactly
    /// once (truncating a corrupt tail like the legacy path did on the first append),
    /// then snapshots the segment metadata and persisted runtime state.
    fn seed_node_cursor(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<NodeWalCursor> {
        let recovery = self.recover_node_segmented_scan(shard_id, node_id)?;
        let segments = self.node_segments(shard_id, node_id)?;
        let runtime_state = self.read_segment_runtime_state(shard_id, node_id);
        Ok(NodeWalCursor {
            next_sequence: recovery.valid_records as u64 + 1,
            segments,
            released_segment_count: runtime_state.released_segment_count,
            last_fsync_elapsed_ms: runtime_state.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: runtime_state.slow_fsync_backpressure_observed,
        })
    }

    fn record_log_index_bounds(record: &RaftWalRecord) -> (u64, u64) {
        let first = record
            .entries
            .first()
            .map(|entry| entry.index)
            .unwrap_or(record.hard_state.commit_index);
        let last = record
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(record.hard_state.commit_index);
        (first, last)
    }

    fn segment_report_from_segments(segments: &[RaftWalSegmentInfo]) -> RaftWalSegmentReport {
        let first_retained_log_index = segments
            .iter()
            .find_map(|segment| (segment.first_log_index > 0).then_some(segment.first_log_index))
            .unwrap_or_default();
        let last_retained_log_index = segments
            .iter()
            .rev()
            .find_map(|segment| (segment.last_log_index > 0).then_some(segment.last_log_index))
            .unwrap_or_default();
        RaftWalSegmentReport {
            active_segment_id: segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            segments: segments.to_vec(),
            released_segment_count: 0,
            first_retained_log_index,
            last_retained_log_index,
            last_fsync_elapsed_ms: 0,
            slow_fsync_backpressure_observed: false,
        }
    }

    pub fn recover_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        // Recovery may truncate a corrupt tail on disk, so any cached cursor must be
        // rebuilt from disk on the next append.
        if let Ok(mut cursors) = self.cursors.lock() {
            cursors.remove(&(shard_id, node_id));
        }
        self.recover_node_segmented_scan(shard_id, node_id)
    }

    fn recover_node_segmented_scan(
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

    // Retained for the legacy on-disk prune path; the segmented append hot path now
    // prunes in-memory via the cursor to stay O(1).
    #[allow(dead_code)]
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
