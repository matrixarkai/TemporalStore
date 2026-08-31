// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Stream read/scan + batch execute + index snapshot publish methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Block | StreamKind::Page => self
                .page_store
                .read_logical_range(request.page_slab_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::Index => {
                // Serve the complete current index through the funnel (live in-memory on
                // the delta path, base file otherwise), then slice the requested window.
                // Preserves the original missing-file -> error semantics.
                self.load_served_index_bytes(request.shard_id)
                    .map_err(|err| err.to_string())
                    .map(|bytes| {
                        let start = request.offset as usize;
                        let end = start.saturating_add(request.size as usize).min(bytes.len());
                        if start >= bytes.len() {
                            Vec::new()
                        } else {
                            bytes[start..end].to_vec()
                        }
                    })
            }
            StreamKind::Wal => self
                .wal_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::IndexLog => self
                .index_log_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
        };
        match data {
            Ok(data) => StreamReadResponse {
                status: Status::ok(),
                data,
            },
            Err(err) => StreamReadResponse {
                status: Status::error("stream_read_failed", err.to_string()),
                data: Vec::new(),
            },
        }
    }

    pub fn scan_stream(&self, request: ScanStreamRequest) -> ScanStreamResponse {
        if request.start_offset > request.end_offset {
            return ScanStreamResponse {
                status: Status::error("invalid_stream_range", "start_offset is after end_offset"),
                records: Vec::new(),
                end_of_stream: true,
            };
        }
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(request.max_bytes);
        if request.stream_kind == StreamKind::Wal || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Wal => self
                    .wal_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Block | StreamKind::Page => unreachable!(),
            };
            return match records {
                // `truncated` is the whole point of asking: the walk stops both when the window
                // ends and when max_bytes runs out, and this used to answer "end of stream" for
                // either. A caller reading a range larger than its budget was handed a prefix
                // and told it had the lot.
                Ok((records, truncated)) => ScanStreamResponse {
                    status: Status::ok(),
                    records: records
                        .into_iter()
                        .map(|(offset, data)| StreamRecord { offset, data })
                        .collect(),
                    end_of_stream: !truncated,
                },
                Err(err) => ScanStreamResponse {
                    status: Status::error("stream_scan_failed", err.to_string()),
                    records: Vec::new(),
                    end_of_stream: true,
                },
            };
        }
        let read = self.read_stream(StreamReadRequest {
            shard_id: request.shard_id,
            stream_kind: request.stream_kind,
            page_slab_id: request.page_slab_id,
            offset: request.start_offset,
            size,
        });
        // A byte-addressed read is capped the same way -- `size` above is the window clamped to
        // max_bytes -- so it ends the stream only when it reached the offset that was asked for.
        let end_of_stream = !read.status.ok
            || request
                .start_offset
                .saturating_add(read.data.len() as u64)
                >= request.end_offset;
        ScanStreamResponse {
            status: read.status.clone(),
            records: if read.status.ok && !read.data.is_empty() {
                vec![StreamRecord {
                    offset: request.start_offset,
                    data: read.data,
                }]
            } else {
                Vec::new()
            },
            end_of_stream,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        // Every command here executes through `execute_on_shard`, which STAGES an outcome item,
        // and the records this path appends are built from the commands -- so nothing ever took
        // what was staged. Those items stayed in the thread's buffer, and the next write on that
        // thread appended them as its own doing. Threads are reused across requests, so "the next
        // write" is routinely a different request entirely.
        //
        // A guard rather than a drain at the end, because this function returns early on a
        // missing shard and on a failed durable commit, and those exits leaked too. Discarding is
        // correct while batch records carry their commands: replay re-executes them, so the items
        // describe work that is already accounted for.
        struct DrainStagedOnExit;
        impl Drop for DrainStagedOnExit {
            fn drop(&mut self) {
                let _ = super::block_in_wal::take_outcomes();
            }
        }
        let _drain_staged = DrainStagedOnExit;
        let command_count = request.commands.len();
        let mut responses = Vec::with_capacity(command_count);
        if command_count == 0 {
            return BatchExecuteResponse {
                status: Status::ok(),
                responses,
            };
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            // returns a BATCH-LEVEL topology error with ZERO CmdResponse entries when the
            // partition is missing/not-primary (a topology error: the client should
            // refresh its shard map), not an OK batch full of per-command errors. The Rust client
            // treats a batch-level shard_not_loaded status as topology-retryable
            // (client/retry.rs status_is_topology_retryable) and refreshes + retries -- but it
            // keys on the batch-level status, which the old ok+N-errors shape left ok, so the
            // client never refreshed and kept hitting the wrong node. Return it batch-level.
            return BatchExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                responses: Vec::new(),
            };
        };
        // Mirror execute_with_storage_override: a shard replaying its WAL on load is present
        // but not yet serving. Reject the whole batch with a retryable status so batch writes
        // cannot interleave with replay (anchor regression / double-apply) -- the batch path
        // must not be a hole around the single-execute recovery gate. Replay itself never
        // routes through batch_execute, but replaying_wal() is checked for symmetry.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            // Batch-level topology-retryable error with empty responses, as above (returns a
            // partition-level error here, not a per-command one), so the client refreshes topology
            // and retries rather than seeing an ok batch of failed commands.
            return BatchExecuteResponse {
                status: Status::error(
                    "shard_not_loaded",
                    "shard is recovering (WAL replay in progress)",
                ),
                responses: Vec::new(),
            };
        }
        let readonly = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false);
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let start_routing_bucket = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end_routing_bucket = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        // Phase-1 flat-append fast-skip, the same one `execute_with_storage_override` applies to
        // single commands. Without it the batch path pays the reconcile scan on EVERY batch, and
        // that scan walks and CLONES every live model-map entry in the shard just to re-confirm
        // that `bucket_index` -- which each mutating command already upserts before returning --
        // is in sync. Measured on a 290 MB store, that made an ingest 1.65 s of pure proxy CPU
        // against 0.15 s on a 26 MB one, and it was the single hottest frame in a stack profile
        // of the write path. Every mem0 write arrives here, so the guarded path was the one that
        // mattered and the unguarded one was the one being used.
        //
        // `promote_scan_done` is `#[serde(skip)]`, so a freshly loaded shard still pays exactly
        // one full reconcile before the skip engages, and with the gate off the scan runs on
        // every batch precisely as before.
        if !(phase1_flat_enabled() && shard.promote_scan_done) {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                request.shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.page_store, shard, None);
            }
            // Latch only once the shard actually holds model-map state: `promote` returns false
            // without establishing anything on an empty shard, so guarding on non-emptiness
            // avoids latching before the first real write.
            if phase1_flat_enabled() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let mut mutated_any = false;
        // Set if any command in this batch rebuilt the bucket index, which invalidates the
        // per-batch record of touched buckets and forces the full sweep below.
        let mut rebuilt_bucket_index = false;
        let mut wal_commands = Vec::new();
        // Accumulate the object keys every mutating command in the batch touched, for the
        // single O(delta) index-log append below (delta path). Empty when the flag is off.
        let mut delta_command_keys: Vec<String> = Vec::new();
        // Some(components) while every mutating command in the batch is a pure page-upsert;
        // one non-upsert write downgrades the whole record to snapshot semantics.
        let mut batch_upsert_components: Option<Vec<(&'static str, String, Option<String>)>> =
            Some(Vec::new());
        for command in request.commands {
            let write_command = is_write_command(&command);
            if readonly && write_command {
                responses.push(ExecuteResponse {
                    status: Status::error("readonly_shard", "readonly shard rejects write command"),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if let Err(status) =
                self.check_admission(request.shard_id, write_command, &config, &info)
            {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if write_command
                && config
                    .maxmemory_bytes
                    // Current on-disk footprint (GC-decremented), not cumulative-ever
                    // bytes_written -- see the single-command execute path.
                    .map(|limit| self.page_store.zone_summary().total_known_physical_bytes >= limit)
                    .unwrap_or(false)
            {
                responses.push(ExecuteResponse {
                    status: Status::error(
                        "storage_quota_exceeded",
                        "shard maxmemory_bytes limit has been reached",
                    ),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.page_store,
                request.shard_id,
                shard,
                &command,
            ) {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                continue;
            }
            let command_for_post_write = command.clone();
            let outcome = execute_on_shard(
                &self.cache,
                &self.page_store,
                config.feature_max_size,
                config.async_storage,
                config.control_rollup_enabled(),
                config.control_coalesce_persist_enabled(),
                config.control_distinct_sketch_enabled(),
                request.shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                command,
            );
            // LRU recency: stamp the bucket(s) this command
            // touched, read or write, so eviction can prefer least-recently-used buckets.
            {
                let now = now_ms();
                for key in command_touched_keys(&command_for_post_write) {
                    let recency_bucket =
                        page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                    shard.bucket_recency.insert(recency_bucket, now);
                }
            }
            if outcome.mutated {
                mutated_any = true;
                let object_keys = command_object_keys(&command_for_post_write);
                delta_command_keys.extend(object_keys.iter().cloned());
                match (
                    &mut batch_upsert_components,
                    command_upsert_components(&command_for_post_write),
                ) {
                    (Some(collected), Some(components)) => collected.extend(components),
                    (state, _) => *state = None,
                }
                if object_keys.is_empty() {
                    rebuild_bucket_page_ownership(
                        request.shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                } else {
                    for object_key in object_keys {
                        shard.dirty_objects.insert(object_key.clone());
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        );
                    }
                }
                if !defer_bucket_index_reconstruct()
                    && (!command_updates_bucket_index_directly(&command_for_post_write)
                        || shard.bucket_index.bucket_map.is_empty())
                {
                    rebuild_bucket_first_index(
                        request.shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                    // The rebuild replaced bucket_map wholesale, so the record of which buckets
                    // this batch touched no longer describes it.
                    rebuilt_bucket_index = true;
                }
                if write_command {
                    wal_commands.push(command_for_post_write);
                }
            }
            responses.push(ExecuteResponse {
                status: Status::ok(),
                response: outcome.response,
            });
        }
        if mutated_any {
            if rebuilt_bucket_index {
                // The reconstruct on the line above already recomputed every bucket's object
                // index from its page index, so the sweep's own rebuild would redo that identical
                // scan. Flags still refresh; only the duplicate scan is dropped.
                refresh_bucket_runtime_flags_after_reconstruct(shard);
            } else {
                // Refresh only the buckets this batch disturbed. The full sweep is
                // O(total pages), so running it per batch left bulk ingest quadratic in the
                // corpus -- with a smaller constant than the per-write sweep, but the same shape.
                refresh_pending_bucket_runtime_flags(shard);
            }
            // Every write records a WAL entry before any page is written. async_storage only
            // changes whether the commit BLOCKS: sync -> fsync, async (or bulk backfill) ->
            // buffered, no fsync (a fire-and-forget commit).
            //
            // The batch is logged as ONE crash-atomic group: all commands are appended under a
            // single batch id and made durable by a SINGLE barrier (not a per-command fsync
            // loop). A crash mid-batch therefore leaves an incomplete trailing batch that replay
            // drops wholesale -- so a retry never double-applies a durable PREFIX of the batch,
            // which for a non-idempotent / time-unspecified command (FeatureAppend occur_time=0 ->
            // a fresh now on each apply) would otherwise duplicate points.
            let sync = !config.async_storage && !bulk_ingest_mode();
            // Use what the batch staged rather than discarding it. Every command here recorded
            // what it DID; writing the commands instead left those items in the thread's buffer
            // and put this whole path outside data-only, which is why a batch measured larger
            // than the same writes made separately.
            //
            // one record carrying every item, when the items can be found again after a crash --
            // the same rule a single write follows. Otherwise the commands go, as before.
            let staged_outcomes = super::block_in_wal::take_outcomes();
            let staged_blocks = super::block_in_wal::take_staged();
            let recoverable = sync || !staged_blocks.is_empty();
            let batch_result = if crate::wal::wal_data_only_enabled()
                && !staged_outcomes.is_empty()
                && recoverable
            {
                self.wal_store
                    .append_batch_as_one_record(
                        request.shard_id,
                        staged_outcomes,
                        staged_blocks,
                        sync,
                    )
                    .map(|_| ())
            } else {
                self.wal_store
                    .append_batch_atomic(request.shard_id, wal_commands, sync)
                    .map(|_| ())
            };
            if let Err(err) = batch_result {
                if sync {
                    // A durable batch commit that failed is not durable; surface it rather than
                    // acking undurable writes (mirrors the single-command execute path).
                    return BatchExecuteResponse {
                        status: Status::error(
                            "wal_commit_failed",
                            format!("durable WAL batch commit failed: {err}"),
                        ),
                        responses,
                    };
                } else {
                    tracing::error!(
                        shard_id = request.shard_id,
                        error = %err,
                        "async WAL batch append failed: acked writes are NOT durable and will be \
                         lost on a crash before the next flush"
                    );
                }
            }
            // Page/index materialization stays deferred to the background dump in
            // async and bulk modes (index_log/persist already no-op under bulk).
            if !config.async_storage && !bulk_ingest_mode() {
                // Anchor the served index to the WAL sequence it reflects (see the
                // single-command path) so shard load replays only records after it. Under
                // TS_PHASE1_FLAT read the O(1) cached last sequence instead of `stats()` (which
                // rescans the whole WAL file); gate OFF keeps the exact `stats()` value.
                shard.applied_wal_sequence = Some(if phase1_flat_enabled() {
                    self.wal_store.cached_last_sequence(request.shard_id)
                } else {
                    self.wal_store.stats(request.shard_id).last_sequence
                });
                // Append the pages this batch changed (O(delta)) to the index-log (advances
                // the sequence + populates the delta stream). The whole-index base rewrite is
                // deferred to the next compaction point (see the single-command execute path).
                let (items, upsert_record) = match batch_upsert_components.as_ref() {
                    Some(components) => (
                        collect_upsert_index_items(
                            shard,
                            request.shard_id,
                            components,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                        true,
                    ),
                    None => (
                        collect_command_index_items(
                            shard,
                            &delta_command_keys,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                        false,
                    ),
                };
                let key_states = capture_key_states(shard, &delta_command_keys);
                let _ = self.index_log_store.append_delta(
                    request.shard_id,
                    items,
                    key_states,
                    shard.applied_wal_sequence,
                    None,
                    upsert_record,
                    // Non-blocking on the raft apply path (raft log is the durability source).
                    !raft_applying(),
                );
            }
        }
        BatchExecuteResponse {
            status: Status::ok(),
            responses,
        }
    }

    pub fn batch_execute_replicated(
        &self,
        request: ReplicatedBatchExecuteRequest,
    ) -> ReplicatedBatchExecuteResponse {
        let mut responses = Vec::with_capacity(request.commands.len());
        let mut replication = Vec::with_capacity(request.commands.len());
        for command in request.commands {
            replication.push(
                self.replication_selection_report(&command.command, command.replication_mode),
            );
            responses.push(self.execute_replicated(ReplicatedExecuteRequest {
                shard_id: request.shard_id,
                command: command.command,
                replication_mode: command.replication_mode,
            }));
        }
        ReplicatedBatchExecuteResponse {
            status: Status::ok(),
            responses,
            replication,
        }
    }

    pub fn batch_execute_checked(
        &self,
        request: CheckedBatchExecuteRequest,
    ) -> CheckedBatchExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedBatchExecuteResponse {
                status: status.clone(),
                response: BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                },
            };
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: request.shard_id,
            commands: request.commands,
        });
        CheckedBatchExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    pub fn export_index_bytes(&self, shard_id: ShardId) -> Result<Vec<u8>, std::io::Error> {
        // Route through the served-index funnel: on the delta path (and outside bulk) the
        // authoritative index is the live in-memory shard; otherwise fall back to the base
        // file, preserving the original missing-file -> Err contract that callers map to a
        // dump/publish failure status.
        self.load_served_index_bytes(shard_id)
    }

    pub fn publish_shard_index_snapshot(&self, shard_id: ShardId) -> Result<usize, Status> {
        self.publish_shard_index_snapshot_for_keys(shard_id, Vec::<String>::new())
    }

    pub fn publish_shard_index_snapshot_for_keys(
        &self,
        shard_id: ShardId,
        selected_keys: impl IntoIterator<Item = String>,
    ) -> Result<usize, Status> {
        enum PublishTarget {
            String { key: String },
            Hash { key: String, field: String },
        }

        let selected_keys = selected_keys
            .into_iter()
            .filter(|key| !key.trim().is_empty())
            .collect::<BTreeSet<_>>();
        let publish_all = selected_keys.is_empty();
        let publish_targets = {
            let shards = self.shards.read().expect("engine lock poisoned");
            let Some(shard) = shards.get(&shard_id) else {
                return Err(Status::error(
                    "shard_not_loaded",
                    "shard is not loaded on this server",
                ));
            };
            if publish_all {
                let mut publish_targets = shard
                    .strings
                    .iter()
                    .filter(|(_, address)| crate::wal_record::is_wal_resident(address.page_slab_id))
                    .map(|(key, address)| {
                        (PublishTarget::String { key: key.clone() }, address.clone())
                    })
                    .collect::<Vec<_>>();
                publish_targets.extend(
                    shard
                        .hashes
                        .iter()
                        .flat_map(|(key, fields)| {
                            fields.iter().filter_map(move |(field, address)| {
                                crate::wal_record::is_wal_resident(address.page_slab_id).then(|| {
                                    (
                                        PublishTarget::Hash {
                                            key: key.clone(),
                                            field: field.clone(),
                                        },
                                        address.clone(),
                                    )
                                })
                            })
                        })
                        .collect::<Vec<_>>(),
                );
                publish_targets
            } else {
                let mut publish_targets = Vec::new();
                for key in &selected_keys {
                    if let Some(address) = shard.strings.get(key) {
                        if crate::wal_record::is_wal_resident(address.page_slab_id) {
                            publish_targets.push((
                                PublishTarget::String { key: key.clone() },
                                address.clone(),
                            ));
                        }
                    }
                    if let Some(fields) = shard.hashes.get(key) {
                        publish_targets.extend(fields.iter().filter_map(|(field, address)| {
                            crate::wal_record::is_wal_resident(address.page_slab_id).then(|| {
                                (
                                    PublishTarget::Hash {
                                        key: key.clone(),
                                        field: field.clone(),
                                    },
                                    address.clone(),
                                )
                            })
                        }));
                    }
                }
                publish_targets
            }
        };
        let mut publish_records = Vec::with_capacity(publish_targets.len());
        for (target, address) in publish_targets {
            if let Some(bytes) = read_page_bytes(&self.cache, &self.page_store, shard_id, &address)
            {
                publish_records.push((
                    target,
                    address.clone(),
                    bytes,
                    address.object_id,
                    address.routing_bucket,
                ));
            }
        }
        if publish_records.is_empty() {
            return Ok(0);
        }
        let append_records = publish_records
            .iter()
            .map(|(_, _, bytes, object_id, routing_bucket)| {
                (bytes.clone(), *object_id, *routing_bucket)
            })
            .collect::<Vec<BlockAppendRecord>>();
        let published_addresses = self
            .page_store
            .append_batch_with_page_metadata(append_records)
            .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
        let index_bytes = {
            let mut shards = self.shards.write().expect("engine lock poisoned");
            let Some(shard) = shards.get_mut(&shard_id) else {
                return Err(Status::error(
                    "shard_not_loaded",
                    "shard is not loaded on this server",
                ));
            };
            let mut published_object_keys = BTreeSet::new();
            for ((target, original, bytes, _, _), published) in
                publish_records.into_iter().zip(published_addresses)
            {
                match target {
                    PublishTarget::String { key } => {
                        if shard.strings.get(&key) != Some(&original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot(
                                shard_id,
                                published.page_slab_id,
                                published.offset,
                                published.length,
                                published.routing_bucket,
                            ),
                            bytes,
                        );
                        upsert_bucket_index_page(
                            shard,
                            shard_id,
                            "string",
                            &key,
                            None,
                            published.clone(),
                            false,
                        );
                        published_object_keys.insert(key.clone());
                        shard.strings.insert(key, published);
                    }
                    PublishTarget::Hash { key, field } => {
                        let current = shard.hashes.get(&key).and_then(|fields| fields.get(&field));
                        if current != Some(&original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot(
                                shard_id,
                                published.page_slab_id,
                                published.offset,
                                published.length,
                                published.routing_bucket,
                            ),
                            bytes,
                        );
                        upsert_bucket_index_page(
                            shard,
                            shard_id,
                            "hash",
                            &key,
                            Some(field.clone()),
                            published.clone(),
                            false,
                        );
                        published_object_keys.insert(key.clone());
                        if let Some(fields) = shard.hashes.get_mut(&key) {
                            fields.insert(field, published);
                        }
                    }
                }
            }
            for object_key in published_object_keys {
                clear_published_object_dirty_state(shard, &object_key);
            }
            refresh_bucket_runtime_flags(shard);
            serialize_index(shard)
        };
        if !bulk_ingest_mode() {
            // Bulk backfill defers per-record index persistence to an explicit
            // flush_shard_index() call; skip the O(n^2) rewrite + indexlog append here.
            self.index_log_store
                .append_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
            self.persist_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
        }
        Ok(index_bytes.len())
    }
}
