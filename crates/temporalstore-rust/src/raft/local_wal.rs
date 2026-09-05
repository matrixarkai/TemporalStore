// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalRaftWal WAL persistence/recovery methods, split from raft.rs.
use super::*;

/// An append whose bytes are in the file but whose durability barrier has not been taken.
///
/// Writing and flushing are separated so a caller can hold a lock across the write -- record
/// order matters, since each record carries the log and an older one landing after a newer one
/// would regress it on recovery -- and then release that lock before the barrier, which is both
/// the expensive part and the part that concurrent writers can share.
#[must_use = "a staged append is not durable until finish_staged takes its barrier"]
pub struct StagedWalAppend {
    gate: Arc<crate::flush_gate::FlushGate>,
    ticket: crate::flush_gate::FlushTicket,
    file: fs::File,
    shard_id: ShardId,
    node_id: RaftNodeId,
    min_keep_segments: usize,
    slow_fsync_threshold_ms: u64,
    /// Whether this append wrote a base whose snapshot boundary ADVANCED -- the one kind of
    /// record that supersedes on-disk history. Its barrier makes earlier segments prunable.
    compacting_base: bool,
}

impl LocalRaftWal {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursors: Arc::new(Mutex::new(BTreeMap::new())),
            flush_gates: Arc::new(crate::flush_gate::FlushRegistry::default()),
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
            // The single-file (non-segmented) path always writes full records.
            delta: None,
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(&wal_proto::encode_record_line(
            &envelope,
            wal_proto::binary_records_enabled(),
        )?)?;
        crate::durability_metrics::record_barrier("raft_wal_append_unsegmented");
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

    /// The file a snapshot's state image lives in, named by the snapshot's boundary index.
    fn image_path(&self, shard_id: ShardId, node_id: RaftNodeId, index: u64) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join(format!("image-{index:020}.bin"))
    }

    /// Move a record's in-memory image into its own durable file, returning a record that
    /// references it. The file is fsynced BEFORE any record naming it can be written, so a
    /// record that says "my image is in a file" is never durable ahead of the file itself.
    /// Older image files are pruned: recovery folds records forward, so only the newest
    /// snapshot's image is ever read back.
    fn externalize_state_image(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<Option<RaftWalRecord>> {
        let Some(snapshot) = &record.installed_snapshot else {
            return Ok(None);
        };
        let Some(image) = &snapshot.state_image else {
            return Ok(None);
        };
        let index = snapshot.last_included_index;
        let path = self.image_path(shard_id, node_id, index);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let message = crate::sdk::v1::WalStateImage {
                index: image.index_bytes.clone(),
                next_page_id: image.next_page_id,
                slabs: image
                    .slabs
                    .iter()
                    .map(|slab| crate::sdk::v1::WalStateImageSlab {
                        page_slab_id: slab.page_slab_id,
                        slab: slab.bytes.clone(),
                    })
                    .collect(),
            };
            let bytes = compress_state_image(prost::Message::encode_to_vec(&message));
            let tmp = path.with_extension("tmp");
            {
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)?;
                // Binary frame: the payload here is compressed bytes, and a text frame
                // would let one ending in 0x0A be mistaken for its own terminator.
                file.write_all(&crate::log_framing::encode_frame(&bytes))?;
                file.sync_data()?;
            }
            fs::rename(&tmp, &path)?;
            // Prune superseded images: recovery only ever reattaches the newest.
            if let Ok(entries) = fs::read_dir(self.node_segment_dir(shard_id, node_id)) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if let Some(stem) = name.strip_prefix("image-").and_then(|rest| {
                        rest.strip_suffix(".bin")
                    }) {
                        if stem.parse::<u64>().map(|old| old < index).unwrap_or(false) {
                            let _ = fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
        let mut externalized = record.clone();
        if let Some(snapshot) = externalized.installed_snapshot.as_mut() {
            snapshot.state_image = None;
            snapshot.state_image_externalized = true;
        }
        Ok(Some(externalized))
    }

    /// Reattach an externalized image to a recovered record. A record that references a file
    /// which does not exist is COMMITTED corruption -- the file is made durable before any
    /// record naming it, so a torn tail cannot produce this.
    fn reattach_state_image(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &mut RaftWalRecord,
    ) -> io::Result<()> {
        let Some(snapshot) = record.installed_snapshot.as_mut() else {
            return Ok(());
        };
        if !snapshot.state_image_externalized || snapshot.state_image.is_some() {
            return Ok(());
        }
        let path = self.image_path(shard_id, node_id, snapshot.last_included_index);
        let raw = fs::read(&path).map_err(|err| {
            io::Error::other(format!(
                "snapshot image file missing for index {}: {err}",
                snapshot.last_included_index
            ))
        })?;
        // No newline stripping here: `decode_line` strips one itself for a text record, and
        // doing it first would take a real byte off a binary payload that ends in 0x0A.
        let payload = crate::log_framing::decode_line(&raw).map_err(io::Error::other)?;
        let payload = decompress_state_image(payload)?;
        let message = <crate::sdk::v1::WalStateImage as prost::Message>::decode(payload.as_ref())
            .map_err(io::Error::other)?;
        snapshot.state_image = Some(RaftSnapshotStateImage {
            index_bytes: message.index,
            next_page_id: message.next_page_id,
            slabs: message
                .slabs
                .into_iter()
                .map(|slab| RaftSnapshotStateImageSlab {
                    page_slab_id: slab.page_slab_id,
                    bytes: slab.slab,
                })
                .collect(),
        });
        snapshot.state_image_externalized = false;
        Ok(())
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

    /// Write a record's bytes without taking its barrier. See `finish_staged`.
    pub fn stage_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
    ) -> io::Result<StagedWalAppend> {
        self.stage_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            u64::MAX,
        )
    }

    /// Append a record and make it durable before returning.
    pub fn persist_node_segmented_with_fsync_threshold(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
        slow_fsync_threshold_ms: u64,
    ) -> io::Result<RaftWalSegmentReport> {
        let staged = self.stage_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            slow_fsync_threshold_ms,
        )?;
        self.finish_staged(staged)
    }

    /// Write a record's bytes WITHOUT taking its durability barrier.
    ///
    /// The returned handle owes a barrier: nothing here is durable until `finish_staged` takes
    /// it. Callers that must order their writes against each other hold their lock across this
    /// call and release it before finishing, so the barriers coalesce while the writes stay in
    /// order.
    pub fn stage_node_segmented_with_fsync_threshold(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
        slow_fsync_threshold_ms: u64,
    ) -> io::Result<StagedWalAppend> {
        // A state image is written once, to its own file, before any record that references it;
        // the record then carries the reference. Embedding it re-serialized the whole image into
        // every subsequent record.
        let externalized = self.externalize_state_image(shard_id, node_id, record)?;
        let record = externalized.as_ref().unwrap_or(record);
        let marker_index = record
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index);
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

        // The record Raft hands us always carries the node's WHOLE log. Writing that on
        // every append makes each append cost O(log length) and the WAL O(n^2) overall,
        // which shows up as write latency that climbs with the log. Write the entries
        // that are actually new instead, and re-base with a full record only when a
        // delta could not be folded back (start of a segment, or the log moved under us).
        let log_first_index = record
            .entries
            .first()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let log_last_index = record
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let log_last_term = record
            .entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default();
        // A delta is only sound while the log still contains the exact entry we last
        // wrote, at the same term. A conflict overwrite (raft truncating a divergent
        // suffix) or a snapshot compaction (dropping a prefix) breaks that.
        let delta_safe = cursor.has_base
            && cursor.persisted_last_index > 0
            && log_last_index >= cursor.persisted_last_index
            && log_first_index > 0
            && log_first_index <= cursor.persisted_last_index
            && record.entries.iter().any(|entry| {
                entry.index == cursor.persisted_last_index && entry.term == cursor.persisted_last_term
            });

        let encode = |as_delta: bool| -> io::Result<Vec<u8>> {
            let envelope = if as_delta {
                let from_index = cursor.persisted_last_index;
                // Only the entries above `from_index`, taken as a slice. Cloning the record
                // and filtering afterwards allocated the whole log on every append, which is
                // the cost incremental records exist to remove.
                let tail_start = record
                    .entries
                    .partition_point(|entry| entry.index <= from_index);
                let delta_record = RaftWalRecord {
                    hard_state: record.hard_state.clone(),
                    membership: record.membership.clone(),
                    replica_role: record.replica_role,
                    joint_membership: record.joint_membership.clone(),
                    latest_external_snapshot_ref: record.latest_external_snapshot_ref.clone(),
                    // An unchanged marker folds forward from the base, exactly as entries do.
                    installed_snapshot: if record
                        .installed_snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.last_included_index)
                        == cursor.persisted_marker_index
                        && cursor.persisted_marker_index.is_some()
                    {
                        None
                    } else {
                        record.installed_snapshot.clone()
                    },
                    apply_snapshot_fence: record.apply_snapshot_fence.clone(),
                    storage_apply_fence: record.storage_apply_fence.clone(),
                    // Telemetry counters are not durability-relevant (the fingerprint zeroes
                    // them), and they were more than half the bytes of every record. Recovery
                    // keeps whatever the base record carried.
                    pipeline_state: RaftPeerPipelineRuntimeState::default(),
                    read_safety_state: RaftReadSafetyRuntimeState::default(),
                    membership_evidence: record.membership_evidence.clone(),
                    entries: record.entries[tail_start..].to_vec(),
                };
                RaftWalEnvelope {
                    sequence,
                    checksum: raft_wal_checksum(&delta_record)?,
                    record: delta_record,
                    delta: Some(RaftWalEntryDelta {
                        from_index,
                        log_first_index,
                        log_last_index,
                    }),
                }
            } else {
                RaftWalEnvelope {
                    sequence,
                    checksum: raft_wal_checksum(record)?,
                    record: record.clone(),
                    delta: None,
                }
            };
            wal_proto::encode_record_line(&envelope, wal_proto::binary_records_enabled())
        };

        let mut wrote_base = !delta_safe;
        let mut encoded = encode(delta_safe)?;

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
        // Every segment opens with a full base record, so pruning whole segments can
        // never orphan a delta. Rotation still honours `max_segment_bytes`; it is only
        // clamped up to one base record so a segment can always hold the record that
        // opens it.
        let rotate_threshold = max_segment_bytes.max(cursor.base_bytes);
        // Size-based rotation as before -- plus: a COMPACTING base opens a segment. When the
        // snapshot boundary advanced, the base supersedes the history in the current segment,
        // and only at a segment boundary can pruning reclaim it. Ordinary re-bases (an empty
        // log, a conflict) stay in-segment: rotating on every one turned each read-index
        // persist into file churn and erased the record trail the legacy contract promises.
        let marker_advanced =
            marker_index.is_some() && marker_index != cursor.persisted_marker_index;
        let rotate = (active_len > 0 && active_len + encoded.len() as u64 > rotate_threshold)
            || (wrote_base && marker_advanced && active_len > 0);
        if rotate {
            active_segment_id += 1;
            if !wrote_base {
                // Opening a new segment: it must start self-sufficient.
                encoded = encode(false)?;
                wrote_base = true;
            }
        }
        let active_path = self.node_segment_path(shard_id, node_id, active_segment_id);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        file.write_all(&encoded)?;
        // The bytes are in the file now, so claim a barrier -- but do not take it here. Taking it
        // under the cursor lock is what kept barriers-per-write pinned at 1.0 however many
        // writers there were: each queued for the lock and then paid for an fsync that would have
        // covered all of them. The barrier is taken below, once the lock is released.
        let gate = self.flush_gates.gate((shard_id, node_id));
        let ticket = gate.register_write();

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
        if wrote_base {
            cursor.base_bytes = encoded.len() as u64;
            cursor.has_base = true;
        }
        cursor.persisted_last_index = log_last_index;
        cursor.persisted_last_term = log_last_term;
        cursor.persisted_marker_index = record
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .or(cursor.persisted_marker_index);

        // Everything the cursor must record about this append is in place, so the lock can go
        // before the expensive part. Writers blocked behind it now proceed and register against
        // the same gate, and one barrier covers all of them.
        drop(cursors);
        Ok(StagedWalAppend {
            gate,
            ticket,
            file,
            shard_id,
            node_id,
            min_keep_segments,
            slow_fsync_threshold_ms,
            compacting_base: wrote_base && marker_advanced,
        })
    }

    /// Take (or ride) the barrier a staged append owes, then prune and report.
    ///
    /// Pruning happens strictly after the barrier: dropping an old segment before the new record
    /// is durable would trade a retention rule for a hole in the log.
    pub fn finish_staged(&self, staged: StagedWalAppend) -> io::Result<RaftWalSegmentReport> {
        let StagedWalAppend {
            gate,
            ticket,
            file,
            shard_id,
            node_id,
            min_keep_segments,
            slow_fsync_threshold_ms,
            compacting_base,
        } = staged;
        let last_fsync_elapsed_ms = gate.await_durable(ticket, || {
            crate::durability_metrics::record_barrier("raft_wal_append");
            file.sync_data()
        })?;
        let mut cursors = self.cursors.lock().expect("raft wal cursor lock poisoned");
        let Some(cursor) = cursors.get_mut(&(shard_id, node_id)) else {
            // A concurrent `recover_node_segmented` dropped the cached cursor so the next append
            // rebuilds it from disk. This append is already durable -- the barrier above saw to
            // that -- and only the in-memory bookkeeping is gone, so read the report off disk
            // rather than insist on state that was deliberately discarded.
            drop(cursors);
            return self.segment_report(shard_id, node_id);
        };

        // Prune the oldest segments, keeping at least `min_keep_segments`. Removing a
        // segment drops its records from the retained window, so the next sequence
        // number decreases by exactly those records -- matching the legacy behavior
        // where the sequence was `recover().valid_records + 1` over the surviving
        // segments only.
        let mut released_segment_count = 0u64;
        // A durable COMPACTING base supersedes every earlier segment; one predecessor is kept
        // so a torn tail on the active segment still has somewhere to recover from. Ordinary
        // appends keep the configured count.
        let effective_keep = if compacting_base {
            min_keep_segments.min(2)
        } else {
            min_keep_segments
        };
        if cursor.segments.len() > effective_keep {
            let remove = cursor.segments.len() - effective_keep;
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
        let recovered_marker = recovery
            .record
            .as_ref()
            .and_then(|record| record.installed_snapshot.as_ref())
            .map(|snapshot| snapshot.last_included_index);
        let segments = self.node_segments(shard_id, node_id)?;
        let runtime_state = self.read_segment_runtime_state(shard_id, node_id);
        Ok(NodeWalCursor {
            persisted_marker_index: recovered_marker,
            next_sequence: recovery.valid_records as u64 + 1,
            segments,
            released_segment_count: runtime_state.released_segment_count,
            last_fsync_elapsed_ms: runtime_state.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: runtime_state.slow_fsync_backpressure_observed,
            // A freshly seeded cursor has written nothing in this process, so the first
            // append re-bases. That keeps recovery independent of whatever the previous
            // process had in memory.
            base_bytes: 0,
            persisted_last_index: 0,
            persisted_last_term: 0,
            has_base: false,
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
        let mut recovery = self.recover_node_segmented_scan(shard_id, node_id)?;
        if let Some(record) = recovery.record.as_mut() {
            self.reattach_state_image(shard_id, node_id, record)?;
        }
        Ok(recovery)
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
                // Blank padding is not a record, but it is not damage either.
                if remaining.first() == Some(&b'\n') {
                    valid_until = offset + 1;
                    offset += 1;
                    continue;
                }
                let Some((line_len, envelope)) = wal_proto::next_envelope(remaining) else {
                    break;
                };
                if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                    break;
                }
                valid_records += 1;
                last_record = match envelope.delta {
                    // A base record replaces everything before it outright.
                    None => Some(envelope.record),
                    Some(delta) => {
                        // Fold the appended entries onto the running record. Entries above
                        // `from_index` were superseded (conflict overwrite), and anything
                        // below `log_first_index` was compacted away.
                        let mut merged = envelope.record;
                        // A marker the record omitted is unchanged: inherit the base's, the
                        // same way unchanged entries fold forward.
                        if merged.installed_snapshot.is_none() {
                            if let Some(previous) = last_record.as_ref() {
                                merged.installed_snapshot =
                                    previous.installed_snapshot.clone();
                            }
                        }
                        let appended = std::mem::take(&mut merged.entries);
                        let base = last_record;
                        // An incremental record omits the volatile telemetry blocks; carry
                        // the base's forward rather than resetting them to zero.
                        if let Some(base) = base.as_ref() {
                            if merged.pipeline_state == RaftPeerPipelineRuntimeState::default() {
                                merged.pipeline_state = base.pipeline_state.clone();
                            }
                            if merged.read_safety_state == RaftReadSafetyRuntimeState::default() {
                                merged.read_safety_state = base.read_safety_state.clone();
                            }
                        }
                        let mut entries = base
                            .map(|base| base.entries)
                            .unwrap_or_default();
                        // A raft log is in ascending index order, and both trims below depend on
                        // that: each is a prefix or a suffix, never a scattered subset.
                        debug_assert!(
                            entries.windows(2).all(|pair| pair[0].index <= pair[1].index),
                            "replay assumes the accumulated log is ordered by index"
                        );
                        // Drop what a conflict superseded. Testing the LAST entry first makes the
                        // ordinary case -- an append that supersedes nothing -- O(1) instead of a
                        // scan of everything accumulated so far. Scanning every time is what made
                        // replay quadratic in record count, so a long-lived node paid for its
                        // whole history on every restart.
                        if entries
                            .last()
                            .map_or(false, |entry| entry.index > delta.from_index)
                        {
                            let keep =
                                entries.partition_point(|entry| entry.index <= delta.from_index);
                            crate::durability_metrics::record_scan(
                                "replay_entries_scanned",
                                (entries.len() - keep) as u64,
                            );
                            entries.truncate(keep);
                        }
                        entries.extend(appended);
                        // Same again for what compaction removed, from the front.
                        if delta.log_first_index > 0
                            && entries
                                .first()
                                .map_or(false, |entry| entry.index < delta.log_first_index)
                        {
                            let drop_upto = entries
                                .partition_point(|entry| entry.index < delta.log_first_index);
                            crate::durability_metrics::record_scan(
                                "replay_entries_scanned",
                                drop_upto as u64,
                            );
                            entries.drain(..drop_upto);
                        }
                        merged.entries = entries;
                        Some(merged)
                    }
                };
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
            if remaining.first() == Some(&b'\n') {
                offset += 1;
                continue;
            }
            let Some((line_len, envelope)) = wal_proto::next_envelope(remaining) else {
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
        // Rewriting the retained records adopts whichever encoding is current; a reader
        // handles either, but a file written whole in one encoding is easier to reason about.
        let binary = wal_proto::binary_records_enabled();
        for envelope in retained {
            file.write_all(&wal_proto::encode_record_line(&envelope, binary)?)?;
        }
        crate::durability_metrics::record_barrier("raft_wal_compact");
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
            if remaining.first() == Some(&b'\n') {
                valid_until = offset + 1;
                offset += 1;
                continue;
            }
            let Some((line_len, envelope)) = wal_proto::next_envelope(remaining) else {
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
        let bytes = fs::read(path)?;
        let mut record_count = 0u64;
        let mut first_sequence = 0u64;
        let mut last_sequence = 0u64;
        let mut first_log_index = 0u64;
        let mut last_log_index = 0u64;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            // Blank padding is not a record.
            if remaining.first() == Some(&b'\n') {
                offset += 1;
                continue;
            }
            // A record that will not decode leaves no way to find where the next one begins, so
            // stop here -- the same place the recovery path stops on the same file.
            let Some((consumed, envelope)) = wal_proto::next_envelope(remaining) else {
                break;
            };
            offset += consumed;
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
    /// Bytes of LOG this node holds on disk -- the segments, and nothing else.
    ///
    /// This is what the compaction threshold bounds, and it is deliberately not the directory's
    /// total size. An externalized state image lives beside the segments, and compaction cannot
    /// reclaim it: compaction is what WRITES it. Counting it made the threshold permanently
    /// satisfied on any shard whose state outgrew the bound, so every check with a single new
    /// entry rebuilt the entire image to reclaim a few kilobytes of log -- measured at 223 KB
    /// weighed against a 4 KB bound, one small write after a compaction.
    ///
    /// Judging it by the logical size of the commands instead is the opposite error, and
    /// understates it by the log's whole encoding overhead: on a 30k-write corpus the commands
    /// were 5 MB while the segments held 36 MB. The segments are the honest measure.
    ///
    /// Metadata only -- no segment is opened or scanned, so the periodic check can consult it
    /// on every tick.
    /// Bytes of log a compaction at `snapshot_index` could still be carrying -- the segments
    /// holding entries ABOVE that marker.
    ///
    /// This is what the compaction threshold should weigh, and neither of the simpler measures
    /// is right. Total directory size counts the externalized state image, which compaction
    /// writes rather than reclaims, so a shard whose state outgrew the bound compacted on every
    /// check forever. Total SEGMENT size is closer but still counts the predecessor segment that
    /// rotation deliberately keeps so a torn tail has somewhere to recover from -- a segment
    /// entirely below the marker, which the next compaction supersedes rather than shrinks.
    /// Weighing either one makes the trigger fire for bytes it cannot do anything about.
    ///
    /// Served from the cursor, which already carries each segment's index bounds, so this stays
    /// a lock and some arithmetic. Without a cursor it falls back to the whole-segment total,
    /// which is the conservative direction: it can only compact sooner, never later.
    pub fn node_log_bytes_after(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        snapshot_index: u64,
    ) -> u64 {
        {
            let cursors = self.cursors.lock().expect("raft wal cursor lock poisoned");
            if let Some(cursor) = cursors.get(&(shard_id, node_id)) {
                return cursor
                    .segments
                    .iter()
                    .filter(|segment| {
                        // `last_log_index == 0` means the segment carries no entry bounds
                        // (an empty or read-index-only segment); count it, since nothing
                        // proves it superseded.
                        segment.last_log_index == 0 || segment.last_log_index > snapshot_index
                    })
                    .map(|segment| segment.bytes)
                    .sum();
            }
        }
        self.node_log_bytes(shard_id, node_id)
    }

    pub fn node_log_bytes(&self, shard_id: ShardId, node_id: RaftNodeId) -> u64 {
        let dir = self.node_segment_dir(shard_id, node_id);
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                // Segments only. An `image-*.bin` in this directory is state, not log.
                let is_segment =
                    path.extension().and_then(|ext| ext.to_str()) == Some("wal");
                if !is_segment {
                    continue;
                }
                if let Ok(metadata) = entry.metadata() {
                    total = total.saturating_add(metadata.len());
                }
            }
        }
        // The pre-segment single-file layout, for a node that has not rolled yet.
        if let Ok(metadata) = fs::metadata(self.node_path(shard_id, node_id)) {
            total = total.saturating_add(metadata.len());
        }
        total
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

    /// Bytes and index bounds this node's segments hold.
    ///
    /// Served from the in-memory cursor when one is live. Deriving it from disk means READING
    /// AND PARSING EVERY SEGMENT IN FULL -- `node_segments` calls `inspect_segment_sequences`,
    /// which reads each file end to end to find its sequence bounds. That is fine as a cold
    /// rebuild, and it is not fine on the admin status endpoint, which is a plain HTTP GET:
    /// anything polling it made the node re-read its whole log every time, and one cheap
    /// request forced megabytes of disk read and record parsing.
    ///
    /// The cursor already carries the per-segment info this report returns, kept current by
    /// every append, so the live path costs a lock and a clone. The disk scan stays as the
    /// fallback for a node whose cursor has not been seeded yet.
    pub fn segment_report(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalSegmentReport> {
        {
            let cursors = self.cursors.lock().expect("raft wal cursor lock poisoned");
            if let Some(cursor) = cursors.get(&(shard_id, node_id)) {
                let mut report = Self::segment_report_from_segments(&cursor.segments);
                report.released_segment_count = cursor.released_segment_count;
                report.last_fsync_elapsed_ms = cursor.last_fsync_elapsed_ms;
                report.slow_fsync_backpressure_observed = cursor.slow_fsync_backpressure_observed;
                return Ok(report);
            }
        }
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

/// Marker for a compressed state image. An image written before compression existed carries a
/// protobuf field tag here instead, so the reader can tell the two apart and keep reading old
/// files -- this is a durable on-disk format and a node must not need a flag day to restart.
const STATE_IMAGE_ZSTD_MAGIC: &[u8] = b"TSZ1";

/// Compress an encoded state image. The image is dominated by the served index, which is JSON:
/// on a scaled corpus 37% of the image's bytes were digits, commas and structural characters,
/// and it opens with dozens of empty index families. That compresses away almost entirely, and
/// the image is written once per compaction and read once per restore, so the cost lands
/// nowhere near a write path. A payload that does not shrink is stored as-is.
fn compress_state_image(bytes: Vec<u8>) -> Vec<u8> {
    match zstd::stream::encode_all(std::io::Cursor::new(&bytes), 3) {
        Ok(compressed) if compressed.len() + STATE_IMAGE_ZSTD_MAGIC.len() < bytes.len() => {
            let mut out = Vec::with_capacity(STATE_IMAGE_ZSTD_MAGIC.len() + compressed.len());
            out.extend_from_slice(STATE_IMAGE_ZSTD_MAGIC);
            out.extend_from_slice(&compressed);
            out
        }
        _ => bytes,
    }
}

/// Undo [`compress_state_image`], passing an uncompressed (older) image through untouched.
fn decompress_state_image(payload: &[u8]) -> io::Result<std::borrow::Cow<'_, [u8]>> {
    match payload.strip_prefix(STATE_IMAGE_ZSTD_MAGIC) {
        Some(compressed) => zstd::stream::decode_all(std::io::Cursor::new(compressed))
            .map(std::borrow::Cow::Owned)
            .map_err(|err| io::Error::other(format!("state image decompress failed: {err}"))),
        None => Ok(std::borrow::Cow::Borrowed(payload)),
    }
}
