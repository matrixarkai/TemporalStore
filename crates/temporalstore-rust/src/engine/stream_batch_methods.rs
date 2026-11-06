// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Stream read/scan + batch execute + index snapshot publish methods for TemporalEngine, split from engine.rs.
use super::*;

enum BatchCommandSource {
    Commands(std::vec::IntoIter<Command>),
    SharedTimestampRows {
        fields: Vec<String>,
        timestamp_field: Option<String>,
        rows: std::vec::IntoIter<BatchHashTimestampSetRow>,
    },
}

impl BatchCommandSource {
    fn command_count(&self) -> usize {
        match self {
            Self::Commands(commands) => commands.len(),
            Self::SharedTimestampRows { rows, .. } => rows.len(),
        }
    }

    fn next_command(&mut self) -> Option<Command> {
        match self {
            Self::Commands(commands) => commands.next(),
            Self::SharedTimestampRows {
                fields,
                timestamp_field,
                rows,
            } => {
                let BatchHashTimestampSetRow(
                    key,
                    timestamp_seconds,
                    timestamp_nanos,
                    values,
                ) = rows.next()?;
                debug_assert_eq!(fields.len(), values.len());
                Some(Command::HashMultiSetIfTimestampOlder {
                    key,
                    timestamp_field: timestamp_field
                        .take()
                        .expect("shared timestamp field returned after every row"),
                    timestamp_seconds,
                    timestamp_nanos,
                    entries: std::mem::take(fields)
                        .into_iter()
                        .zip(values.into_iter().map(|value| value.0))
                        .collect(),
                })
            }
        }
    }

    fn recycle(&mut self, command: Command) {
        let Self::SharedTimestampRows {
            fields,
            timestamp_field,
            ..
        } = self
        else {
            return;
        };
        let Command::HashMultiSetIfTimestampOlder {
            timestamp_field: returned_timestamp_field,
            entries,
            ..
        } = command
        else {
            panic!("shared timestamp batch returned a different command kind");
        };
        debug_assert!(fields.is_empty());
        fields.extend(entries.into_iter().map(|(field, _)| field));
        *timestamp_field = Some(returned_timestamp_field);
    }

    fn recycles_commands(&self) -> bool {
        matches!(self, Self::SharedTimestampRows { .. })
    }
}

#[cfg(test)]
mod shared_timestamp_source_tests {
    use super::*;
    use crate::types::BatchHashMultiSetValue;

    #[test]
    fn reuses_shared_field_name_allocations_across_rows() {
        let mut source = BatchCommandSource::SharedTimestampRows {
            fields: vec!["event_time".to_owned(), "score".to_owned()],
            timestamp_field: Some("event_time".to_owned()),
            rows: vec![
                BatchHashTimestampSetRow(
                    "feature:1".to_owned(),
                    10,
                    0,
                    vec![
                        BatchHashMultiSetValue(vec![1]),
                        BatchHashMultiSetValue(vec![2]),
                    ],
                ),
                BatchHashTimestampSetRow(
                    "feature:2".to_owned(),
                    11,
                    0,
                    vec![
                        BatchHashMultiSetValue(vec![3]),
                        BatchHashMultiSetValue(vec![4]),
                    ],
                ),
            ]
            .into_iter(),
        };

        let first = source.next_command().expect("first row");
        let Command::HashMultiSetIfTimestampOlder {
            timestamp_field,
            entries,
            ..
        } = &first
        else {
            panic!("unexpected command kind");
        };
        let timestamp_ptr = timestamp_field.as_ptr();
        let field_ptrs = entries
            .iter()
            .map(|(field, _)| field.as_ptr())
            .collect::<Vec<_>>();
        source.recycle(first);

        let second = source.next_command().expect("second row");
        let Command::HashMultiSetIfTimestampOlder {
            timestamp_field,
            entries,
            ..
        } = second
        else {
            panic!("unexpected command kind");
        };
        assert_eq!(timestamp_field.as_ptr(), timestamp_ptr);
        assert_eq!(
            entries
                .iter()
                .map(|(field, _)| field.as_ptr())
                .collect::<Vec<_>>(),
            field_ptrs
        );
    }
}

impl TemporalEngine {
    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Block => self
                .block_store
                .read_logical_range(request.block_slab_id, request.offset, request.size)
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
        // A zero budget means "no budget named", not "a budget of zero bytes".
        //
        // The byte-addressed kinds below clamp the window to `max_bytes`, so zero clamped the
        // read to nothing: no records, and `end_of_stream` FALSE, because the end of the window
        // had not been reached. A caller walking `while !end_of_stream` then asked again at the
        // same offset, for ever -- a walk that cannot advance.
        //
        // The record-framed kinds never had this. `scan_collect` keeps its first record whatever
        // the budget says (`total > 0 &&`), for exactly this reason, and its comment says so. That
        // rule belongs to the whole surface rather than one half of it: the two halves answered
        // one and zero records to the same zero budget. The rest of this tree already reads an
        // unset budget as no limit rather than as a limit of nothing.
        let max_bytes = if request.max_bytes == 0 {
            u64::MAX
        } else {
            request.max_bytes
        };
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(max_bytes);
        if request.stream_kind == StreamKind::Wal || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Wal => self
                    .wal_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Block => unreachable!(),
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
            block_slab_id: request.block_slab_id,
            offset: request.start_offset,
            size,
        });
        // A byte-addressed read is capped the same way -- `size` above is the window clamped to
        // max_bytes -- so it ends the stream when it reached the offset that was asked for.
        //
        // It also ends when the read came back with NOTHING. That case used to fall through to
        // the arithmetic below, where zero bytes can never reach `end_offset`, so a scan that
        // found nothing answered end_of_stream FALSE. A caller walking `while !end_of_stream`
        // advances by the bytes it was handed, is handed none, asks again at the same offset and
        // is told the same thing -- a walk that cannot terminate.
        //
        // All three byte-addressed kinds can land here. `Index` returns an empty window for a
        // start at or past the end of the served index, and `logical_range_from_slab` does the
        // same for `Block` and `Page` on both its raw and its framed path.
        //
        // An empty answer means the stream ended inside the window rather than that nothing was
        // asked for. A window of zero width (`start_offset == end_offset`, which the check above
        // permits) is the one case where nothing WAS asked for, and it already answered true by
        // the arithmetic below -- `start + 0 >= end` holds when the two are equal -- so the new
        // clause agrees with it rather than changing it. Everywhere else `size` is at least one
        // byte, because a zero budget has already been read as no budget.
        //
        // This is the rule the record-framed half already follows: `scan_bounded` reports
        // `truncated` only when the BUDGET cut the walk short, so a walk that simply ran out of
        // records reports the end of the stream. The two halves now answer alike.
        let end_of_stream = !read.status.ok
            || read.data.is_empty()
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

    fn compact_hash_multi_get_response(
        field_count: usize,
        response: BatchExecuteResponse,
    ) -> BatchHashMultiGetResponse {
        if !response.status.ok {
            return BatchHashMultiGetResponse {
                status: response.status,
                rows: Vec::new(),
                errors: Vec::new(),
            };
        }
        let mut rows = Vec::with_capacity(response.responses.len());
        let mut errors = Vec::new();
        for (index, item) in response.responses.into_iter().enumerate() {
            if !item.status.ok {
                errors.push(BatchHashMultiGetError {
                    index,
                    status: item.status,
                });
                rows.push(BatchHashMultiGetRow(vec![None; field_count]));
                continue;
            }
            match item.response {
                CommandResponse::Values { values } => rows.push(BatchHashMultiGetRow(values)),
                _ => {
                    errors.push(BatchHashMultiGetError {
                        index,
                        status: Status::error(
                            "unexpected_read_response",
                            "hash multi-get returned a non-values response",
                        ),
                    });
                    rows.push(BatchHashMultiGetRow(vec![None; field_count]));
                }
            }
        }
        BatchHashMultiGetResponse {
            status: Status::ok(),
            rows,
            errors,
        }
    }

    /// Serve one shared field projection without expanding it into owned commands per row.
    pub fn batch_hash_multi_get_compact(
        &self,
        request: BatchHashMultiGetRequest,
    ) -> BatchHashMultiGetResponse {
        let BatchHashMultiGetRequest {
            shard_id,
            keys,
            fields,
        } = request;
        let field_count = fields.len();
        if keys.is_empty() {
            return BatchHashMultiGetResponse {
                status: Status::ok(),
                rows: Vec::new(),
                errors: Vec::new(),
            };
        }

        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return BatchHashMultiGetResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                rows: Vec::new(),
                errors: Vec::new(),
            };
        };
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            return BatchHashMultiGetResponse {
                status: Status::error(
                    "shard_not_loaded",
                    "shard is recovering (WAL replay in progress)",
                ),
                rows: Vec::new(),
                errors: Vec::new(),
            };
        }

        // Lazy expiry is a mutating read and must retain the generic batch's WAL/index handling.
        // Feature-serving rows do not use TTLs, so the no-command-expansion path remains the
        // common case while any shard that does carry expiry state keeps the established behavior.
        if !shard.expires_at_ms().is_empty() {
            drop(shards);
            let response = self.batch_execute(BatchExecuteRequest {
                shard_id,
                commands: keys
                    .into_iter()
                    .map(|key| Command::HashMultiGet {
                        key,
                        fields: fields.clone(),
                    })
                    .collect(),
            });
            return Self::compact_hash_multi_get_response(field_count, response);
        }

        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start_routing_bucket = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end_routing_bucket = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        if !(self.wal_store.flat_append() && shard.promote_scan_done) {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.block_store, shard, None);
            }
            if self.wal_store.flat_append() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }

        let projection_plan = container_blocks::Utf8ProjectionPlan::new(&fields);
        let mut rows = Vec::with_capacity(keys.len());
        let mut errors = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let rejected = if !raft_applying() && !replaying_wal() {
                if !self.charge_quota(shard_id, quota::QuotaKind::Read) {
                    Some(Status::error(
                        "quota_exhausted",
                        format!("shard {shard_id} is over its read rate limit"),
                    ))
                } else {
                    self.check_admission(shard_id, false, &config, &info).err()
                }
            } else {
                None
            };
            if let Some(status) = rejected {
                errors.push(BatchHashMultiGetError { index, status });
                rows.push(BatchHashMultiGetRow(vec![None; field_count]));
                continue;
            }

            let outcome = execute_on_shard::execute_hash_multi_get(
                &self.cache,
                &self.block_store,
                shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                key,
                &fields,
                &projection_plan,
            );
            debug_assert!(!outcome.mutated, "TTL state must use the generic batch path");
            let now = now_ms();
            crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::RecencyStamp, || {
                let routing_bucket =
                    block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                shard.bucket_recency.insert(routing_bucket, now);
            });
            match outcome.response {
                CommandResponse::Values { values } => rows.push(BatchHashMultiGetRow(values)),
                _ => {
                    errors.push(BatchHashMultiGetError {
                        index,
                        status: Status::error(
                            "unexpected_read_response",
                            "hash multi-get returned a non-values response",
                        ),
                    });
                    rows.push(BatchHashMultiGetRow(vec![None; field_count]));
                }
            }
        }
        BatchHashMultiGetResponse {
            status: Status::ok(),
            rows,
            errors,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        self.batch_execute_inner(
            request.shard_id,
            BatchCommandSource::Commands(request.commands.into_iter()),
            false,
        )
    }

    /// Execute timestamp-conditioned packed hash rows without retaining a second copy of every
    /// value solely for the command-WAL fallback. Applied rows stage a complete packed block, so
    /// the default data-only WAL can recover them from outcomes; rejected rows never enter the
    /// WAL. The caller keeps this mode limited to that homogeneous request shape.
    pub(crate) fn batch_execute_packed_hash_rows(
        &self,
        request: BatchHashTimestampSetRequest,
    ) -> BatchExecuteResponse {
        if !crate::wal::wal_data_only_enabled() {
            return BatchExecuteResponse {
                status: Status::error(
                    "data_only_wal_required",
                    "shared timestamp rows require the data-only WAL",
                ),
                responses: Vec::new(),
            };
        }
        self.batch_execute_inner(
            request.shard_id,
            BatchCommandSource::SharedTimestampRows {
                fields: request.fields,
                timestamp_field: Some(request.timestamp_field),
                rows: request.rows.into_iter(),
            },
            true,
        )
    }

    fn batch_execute_inner(
        &self,
        shard_id: ShardId,
        mut command_source: BatchCommandSource,
        elide_packed_hash_payloads: bool,
    ) -> BatchExecuteResponse {
        let elide_packed_hash_payloads =
            elide_packed_hash_payloads && crate::wal::wal_data_only_enabled();
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
        let command_count = command_source.command_count();
        let mut responses = Vec::with_capacity(command_count);
        if command_count == 0 {
            return BatchExecuteResponse {
                status: Status::ok(),
                responses,
            };
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            // returns a BATCH-LEVEL topology error with ZERO CmdResponse entries when the
            // partition is missing/not-primary (a topology error: the client should
            // refresh its shard map), not an OK batch full of per-command errors. The Rust client
            // treats a batch-level shard_not_loaded status as topology-retryable
            // (client/client_retry.rs status_is_topology_retryable) and refreshes + retries -- but it
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
                .get(&shard_id)
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
            .get(&shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false);
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
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
        if !(self.wal_store.flat_append() && shard.promote_scan_done) {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.block_store, shard, None);
            }
            // Latch only once the shard actually holds model-map state: `promote` returns false
            // without establishing anything on an empty shard, so guarding on non-emptiness
            // avoids latching before the first real write.
            if self.wal_store.flat_append() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let mut mutated_any = false;
        // Set if any command in this batch rebuilt the bucket index, which invalidates the
        // per-batch record of touched buckets and forces the full sweep below.
        let mut rebuilt_bucket_index = false;
        let mut wal_commands = Vec::new();
        let mut wal_payloads_elided = false;
        let mut context_embedding_commands = Vec::new();
        // Accumulate the object keys every mutating command in the batch touched, for the
        // single O(delta) index-log append below (delta path). Empty when the flag is off.
        let mut delta_command_keys: Vec<String> = Vec::new();
        // Did ANY command in this batch remove something? Only then does the per-key membership
        // capture below have anything to preserve -- see the single-command execute path, whose
        // guard this mirrors.
        let mut batch_membership_shrank = false;
        // Some(components) while every mutating command in the batch is a pure block-upsert;
        // one non-upsert write downgrades the whole record to snapshot semantics.
        let mut batch_upsert_components: Option<Vec<(&'static str, String, Option<String>)>> =
            Some(Vec::new());
        // Every container element any command in this batch added or replaced. Unlike the
        // components above this is never downgraded to `None`: it is a list of elements, not a
        // claim that the whole record is a pure upsert, so one non-upsert command in the batch
        // does not make the others' identities uncarryable.
        let mut batch_touched_container_elements: Vec<TouchedContainerElement> = Vec::new();
        while let Some(command) = command_source.next_command() {
            let write_command = is_write_command(&command);
            if readonly && write_command {
                responses.push(ExecuteResponse {
                    status: Status::error("readonly_shard", "readonly shard rejects write command"),
                    response: CommandResponse::Empty,
                });
                command_source.recycle(command);
                continue;
            }
            // Exempt under raft apply and under replay, as the single-command path is: a
            // follower that refuses what its leader committed diverges from the leader, and a
            // replay that refuses a record already in the log costs the whole shard load.
            //
            // Both are SYMMETRY here, deliberately and stated as such. Today neither reaches
            // this function -- execute_raft_apply_batch and batch_execute_replicated both loop
            // over the single-command path, and replay never batches -- exactly as the
            // recovering-shard gate above says of replaying_wal(). A future batched apply that
            // does route here would otherwise arrive at an ungated limit, and the failure it
            // produces (a follower quietly skipping a committed entry) is not one that shows up
            // as an error anywhere.
            if !raft_applying() && !replaying_wal() {
                // Charged per COMMAND, not per batch. The single-command path spends one token a
                // command, and a batch is not one unit of work -- a limit that counted it as one
                // would be escaped by batching, which is how most traffic arrives here.
                let kind = if write_command {
                    quota::QuotaKind::Write
                } else {
                    quota::QuotaKind::Read
                };
                if !self.charge_quota(shard_id, kind) {
                    responses.push(ExecuteResponse {
                        status: Status::error(
                            "quota_exhausted",
                            format!(
                                "shard {} is over its {} rate limit",
                                shard_id,
                                match kind {
                                    quota::QuotaKind::Write => "write",
                                    quota::QuotaKind::Read => "read",
                                }
                            ),
                        ),
                        response: CommandResponse::Empty,
                    });
                    command_source.recycle(command);
                    continue;
                }
                if let Err(status) =
                    self.check_admission(shard_id, write_command, &config, &info)
                {
                    responses.push(ExecuteResponse {
                        status,
                        response: CommandResponse::Empty,
                    });
                    command_source.recycle(command);
                    continue;
                }
            }
            // Exempt under raft apply and under replay, as the single-command path is and for
            // the same reason. Symmetry here, on the same footing as the admission guard above:
            // neither reaches this function today.
            if !raft_applying()
                && !replaying_wal()
                && write_command
                && config
                    .maxmemory_bytes
                    // Current on-disk footprint (GC-decremented), not cumulative-ever
                    // bytes_written -- see the single-command execute path.
                    .map(|limit| self.block_store.slab_summary().total_known_physical_bytes >= limit)
                    .unwrap_or(false)
            {
                responses.push(ExecuteResponse {
                    status: Status::error(
                        "storage_quota_exceeded",
                        "shard maxmemory_bytes limit has been reached",
                    ),
                    response: CommandResponse::Empty,
                });
                command_source.recycle(command);
                continue;
            }
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.block_store,
                shard_id,
                shard,
                &command,
            ) {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                command_source.recycle(command);
                continue;
            }
            let object_keys = command_object_keys(&command);
            // What the touched keys held before this command, so the capture below can be
            // skipped when nothing was removed. Sizes only -- no allocation.
            let membership_before: Vec<(String, usize, Option<u64>)> = object_keys
                .iter()
                .map(|key| {
                    let size = key_membership_size(shard, &key);
                    let deadline = shard.expires_at_ms().get(key.as_str()).copied();
                    (key.clone(), size, deadline)
                })
                .collect();
            let feature_trimmed_membership = command_will_trim_feature_membership(
                &command,
                shard,
                config.feature_max_size,
            );
            let reuse_consumed_metadata = elide_packed_hash_payloads
                && matches!(&command, Command::HashMultiSetIfTimestampOlder { .. });
            let cloned_post_write = (!reuse_consumed_metadata).then(|| command.clone());
            let (outcome, returned_post_write) = match command {
                Command::HashMultiSetIfTimestampOlder {
                    key,
                    timestamp_field,
                    timestamp_seconds,
                    timestamp_nanos,
                    entries,
                } if reuse_consumed_metadata => {
                    let (outcome, metadata) =
                        execute_on_shard::execute_packed_conditional_hash_write(
                            &self.cache,
                            &self.block_store,
                            shard_id,
                            start_routing_bucket,
                            end_routing_bucket,
                            shard,
                            key,
                            timestamp_field,
                            timestamp_seconds,
                            timestamp_nanos,
                            entries,
                        );
                    (outcome, Some(metadata))
                }
                command => (
                    execute_on_shard(
                        &self.cache,
                        &self.block_store,
                        config.feature_max_size,
                        config.async_storage,
                        config.control_rollup_enabled(),
                        config.control_coalesce_persist_enabled(),
                        config.control_state_max_size(),
                        config.control_distinct_sketch_enabled(),
                        config.trim_boundary_record_enabled(),
                        shard_id,
                        start_routing_bucket,
                        end_routing_bucket,
                        shard,
                        command,
                    ),
                    None,
                ),
            };
            let mut command_for_post_write = Some(
                returned_post_write
                    .or(cloned_post_write)
                    .expect("every batch command keeps post-write metadata"),
            );
            let command_for_post_write_ref = command_for_post_write
                .as_ref()
                .expect("post-write command remains available until WAL or recycle");
            let payload_elided = reuse_consumed_metadata;
            if Self::command_updates_context_embedding(command_for_post_write_ref) {
                context_embedding_commands.push(command_for_post_write_ref.clone());
            }
            // LRU recency: stamp the bucket(s) this command
            // touched, read or write, so eviction can prefer least-recently-used buckets.
            {
                let now = now_ms();
                crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::RecencyStamp, || {
                    for_each_touched_key(command_for_post_write_ref, |key| {
                        let recency_bucket =
                            block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                        shard.bucket_recency.insert(recency_bucket, now);
                    });
                });
            }
            if outcome.mutated {
                mutated_any = true;
                delta_command_keys.extend(object_keys.iter().cloned());
                // Same question as the single-command path, through the SAME function: a
                // membership shrink OR a deadline change. Two spellings of this test is how
                // one of them keeps a bug the other fixed, so there is only one.
                if !batch_membership_shrank {
                    batch_membership_shrank = feature_trimmed_membership
                        || delta_key_state_change(shard, &membership_before);
                }
                let written_components =
                    command_upsert_components(command_for_post_write_ref, shard);
                batch_touched_container_elements.extend(command_touched_container_elements(
                    command_for_post_write_ref,
                    shard,
                ));
                // The keys this write touched, for the maintenance below. `object_keys` is
                // consumed by the loop, so they are kept as they go past.
                let mut object_keys_for_maintenance: Vec<String> = Vec::new();
                if object_keys.is_empty() {
                    rebuild_bucket_block_ownership(
                        shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                } else {
                    for object_key in object_keys {
                        object_keys_for_maintenance.push(object_key.clone());
                        // `mark_object_dirty` records the key together with the
                        // routing bucket it computes on the next line. Inserting it here as well
                        // was redundant when the set was flat and is wrong now: this site does
                        // not know the bucket, and a key filed without one belongs to no bucket.
                        mark_object_dirty(
                            shard,
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        );
                    }
                }
                // The same maintenance the single-command path does. An ingest's writes are
                // split across both paths, which is why covering only the other one removed
                // exactly half the rebuild work and left the rest: 2404 block visits per add
                // became 1205, and stayed there until this site was covered too.
                //
                // Same two guards: every touched key must report that it synced, or the rebuild
                // still runs; and an empty bucket_map still rebuilds, because maintenance updates
                // an index rather than constructing one.
                let maintained_bucket_index = if shard.bucket_index.bucket_map.is_empty() {
                    false
                } else {
                    // Prefer the components this command actually wrote. The whole-object sync
                    // below re-upserts every field the object holds, which is quadratic in that
                    // count; this is one upsert per written component. It reports false when it
                    // cannot model the write, and the whole-object sync then runs.
                    let narrow = match written_components.as_deref() {
                        Some(components) => {
                            crate::engine::storage_bucket_internals::sync_blocks_for_written_components(
                                shard,
                                shard_id,
                                components,
                            )
                        }
                        None => false,
                    };
                    narrow
                        || (!object_keys_for_maintenance.is_empty()
                            && object_keys_for_maintenance.iter().all(|object_key| {
                                crate::engine::storage_bucket_internals::sync_context_blocks_for_object(
                                    shard,
                                    shard_id,
                                    object_key,
                                )
                            }))
                };
                match (&mut batch_upsert_components, written_components) {
                    (Some(collected), Some(components)) => collected.extend(components),
                    (state, _) => *state = None,
                }
                if !maintained_bucket_index
                    && !defer_bucket_index_reconstruct()
                    // A command that writes no block cannot have changed the block index, so a
                    // rebuild after one recomputes what it already held. The single-command path
                    // has always skipped it here; this path did not, so the same command walked
                    // the whole store when it arrived in a batch and nothing when it arrived
                    // alone -- measured at one visit per block in the shard, per command.
                    && !command_writes_no_block(command_for_post_write_ref)
                    && (!command_updates_bucket_index_directly(command_for_post_write_ref)
                        || shard.bucket_index.bucket_map.is_empty())
                {
                    rebuild_bucket_first_index(
                        shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                    // The rebuild replaced bucket_map wholesale, so the record of which buckets
                    // this batch touched no longer describes it.
                    rebuilt_bucket_index = true;
                }
                if write_command {
                    wal_payloads_elided |= payload_elided;
                    if !command_source.recycles_commands() {
                        wal_commands.push(
                            command_for_post_write
                                .take()
                                .expect("post-write command is available for the WAL"),
                        );
                    }
                }
            }
            if let Some(command) = command_for_post_write.take() {
                command_source.recycle(command);
            }
            responses.push(ExecuteResponse {
                status: Status::ok(),
                response: outcome.response,
            });
        }
        if mutated_any {
            if rebuilt_bucket_index {
                // The reconstruct on the line above already recomputed every bucket's object
                // index from its block index, so the sweep's own rebuild would redo that identical
                // scan. Flags still refresh; only the duplicate scan is dropped.
                refresh_bucket_runtime_flags_after_reconstruct(shard);
            } else {
                // Refresh only the buckets this batch disturbed. The full sweep is
                // O(total blocks), so running it per batch left bulk ingest quadratic in the
                // corpus -- with a smaller constant than the per-write sweep, but the same shape.
                refresh_pending_bucket_runtime_flags(shard);
            }
            // Every write records a WAL entry before any block is written. async_storage only
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
            // ASKED PER RESULT, not per batch -- see `wal::every_result_can_be_found_again`. This
            // was `sync || !staged_blocks.is_empty()`, which asked a per-result question at BATCH
            // granularity: one carried block anywhere in the batch answered it for every result in
            // the batch, and a measured batch left 12 of 20 acked results naming a block-store
            // address and nothing else while the rule called the batch recoverable.
            let recoverable =
                crate::wal::every_result_can_be_found_again(&staged_outcomes, &staged_blocks);
            let use_data_only =
                crate::wal::wal_data_only_enabled() && !staged_outcomes.is_empty() && recoverable;
            if wal_payloads_elided && !use_data_only {
                return BatchExecuteResponse {
                    status: Status::error(
                        "wal_commit_failed",
                        "packed hash outcomes were not recoverable from the data-only WAL",
                    ),
                    responses,
                };
            }
            let batch_result = if use_data_only {
                self.wal_store
                    .append_batch_as_one_record(
                        shard_id,
                        staged_outcomes,
                        staged_blocks,
                        sync,
                    )
                    .map(|_| ())
            } else {
                self.wal_store
                    .append_batch_atomic(shard_id, wal_commands, sync)
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
                        shard_id,
                        error = %err,
                        "async WAL batch append failed: acked writes are NOT durable and will be \
                         lost on a crash before the next flush"
                    );
                }
            }
            // Block/index materialization stays deferred to the background dump in
            // async and bulk modes (index_log/persist already no-op under bulk).
            if !config.async_storage && !bulk_ingest_mode() {
                // Anchor the served index to the WAL sequence it reflects (see the
                // single-command path) so shard load replays only records after it. Under
                // flat append read the O(1) cached last sequence instead of `stats()` (which
                // rescans the whole WAL file); without it the exact `stats()` value is kept.
                // Where this batch STARTS in the log, taken before the anchor moves.
                //
                // The claim stamped below has to be the bucket's OLDEST undumped write, and for a
                // batch that is the FIRST record it appended, not the last. The anchor still
                // holding the previous write's position is one below that first record, so
                // `+ 1` names it -- and when some other path wrote without anchoring, the value
                // is only lower, which retains more rather than less. A claim ABOVE a bucket's
                // oldest undumped write is the direction that frees records it still needs.
                let batch_first_wal_sequence = shard
                    .applied_wal_sequence
                    .unwrap_or_default()
                    .saturating_add(1);
                shard.applied_wal_sequence = Some(if self.wal_store.flat_append() {
                    self.wal_store.cached_last_sequence(shard_id)
                } else {
                    self.wal_store.stats(shard_id).last_sequence
                });
                // The WAL half of what each bucket this batch dirtied holds over the log, stamped
                // the way the single-command path stamps it (engine.rs, after its own append).
                //
                // Without it a batch-dirtied bucket reported NO CLAIM RECORDED, which the reclaim
                // plan reads as unknown rather than as "needs nothing" -- so every such bucket
                // landed in `missing_bucket_generations` and refused the whole plan until a dump
                // happened to cover it. A dump is capped per round, so on a shard written through
                // this path the two logs simply grew.
                //
                // Only when unset, for the same reason as the sibling: the field is the OLDEST
                // undumped write, so a later batch into an already-dirty bucket must not move it.
                for key in &delta_command_keys {
                    let routing_bucket =
                        block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                    if let Some(mut bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
                        if bucket.first_dirty_wal_sequence == 0 {
                            bucket.first_dirty_wal_sequence = batch_first_wal_sequence;
                        }
                    }
                }
                // Append the blocks this batch changed (O(delta)) to the index-log (advances
                // the sequence + populates the delta stream). The whole-index base rewrite is
                // deferred to the next compaction point (see the single-command execute path).
                let (items, upsert_record) = match batch_upsert_components.as_ref() {
                    Some(components) => (
                        collect_upsert_index_items(
                            shard,
                            shard_id,
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
                // Capturing serializes every entry the per-key maps hold for each key, so
                // appending to a posting of length N serialized all N -- 99.5% of the index log
                // on a real corpus (3.53 GB against 16 MB for the same 20,001 messages). A write
                // that only ADDED leaves nothing to resurrect, so it is skipped there, exactly as
                // the single-command execute path already does.
                let mut key_states = if batch_membership_shrank {
                    capture_key_states(shard, &delta_command_keys)
                } else {
                    Vec::new()
                };
                // Carried whether or not the shrink capture ran, for the reason the
                // single-command path states: the gate above asks about a shrink or a deadline,
                // and a pure add -- the write whose element the fold could not restore -- is
                // neither.
                key_states.extend(capture_container_element_states(
                    shard,
                    &batch_touched_container_elements,
                ));
                let appended_index_log_sequence = self
                    .index_log_store
                    .append_delta(
                        shard_id,
                        items,
                        key_states,
                        shard.applied_wal_sequence,
                        None,
                        upsert_record,
                        // Non-blocking on the raft apply path (raft log is the durability
                        // source).
                        !raft_applying(),
                    )
                    .unwrap_or(0);
                // The index-log half, and it is a SEPARATE half: the plan keeps two frontiers
                // counted in two different sequences and requires both, so a bucket that can
                // place itself in the WAL and not in the index log has still said nothing.
                //
                // `append_delta` returns 0 when it wrote nothing, which is exactly "no claim" --
                // the same guard the single-command path uses.
                if appended_index_log_sequence > 0 {
                    for key in &delta_command_keys {
                        let routing_bucket =
                            block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                        if let Some(mut bucket) =
                            shard.bucket_index.bucket_map.get_mut(&routing_bucket)
                        {
                            if bucket.first_dirty_index_log_sequence == 0 {
                                bucket.first_dirty_index_log_sequence =
                                    appended_index_log_sequence;
                            }
                        }
                    }
                }
            }
        }
        drop(shards);
        self.publish_context_embedding_commands(&context_embedding_commands);
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

    /// Republish the named keys' log-resident blocks into the block store, and checkpoint the
    /// served index when one is due.
    ///
    /// ANSWERS WITH THE SERVED-INDEX BYTES THIS CALL WROTE, which is what it always answered --
    /// and that is now sometimes ZERO, because the checkpoint runs per published key rather than
    /// per call. See [`crate::engine::VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT`] for the
    /// interval and for why deferring the checkpoint loses nothing. Zero therefore means "this
    /// call wrote no index", which covers both the publish that found nothing log-resident and
    /// the publish whose checkpoint was deferred; it has never meant "nothing was published", and
    /// the blocks are republished either way before this returns.
    pub fn publish_shard_index_snapshot_for_keys(
        &self,
        shard_id: ShardId,
        selected_keys: impl IntoIterator<Item = String>,
    ) -> Result<usize, Status> {
        enum PublishTarget {
            String { key: String },
            Hash { key: String, field: String },
        }

        impl PublishTarget {
            /// The OBJECT key of the block, which is the key its routing bucket is derived from.
            ///
            /// A hash field's block is filed under the hash's key, not under the field -- the field
            /// is the block's COMPONENT. Reading the bucket from the field would key the cache under
            /// a bucket nothing wrote.
            fn object_key(&self) -> &str {
                match self {
                    PublishTarget::String { key } => key,
                    PublishTarget::Hash { key, .. } => key,
                }
            }

            /// The ELEMENT of that object, which the comment above already calls the block's
            /// component. It was named there and then thrown away; a read that resolves
            /// through a log record needs it to pick the right block out of the record.
            fn component(&self) -> Option<&str> {
                match self {
                    PublishTarget::String { .. } => None,
                    PublishTarget::Hash { field, .. } => Some(field),
                }
            }

            /// The MODEL KIND, which is the third term of the object's identity.
            ///
            /// Named here rather than at the read because the two variants ARE the two kinds, so a
            /// `match` at the call site would be this function written out again. The kind is not
            /// recoverable from the key or the component: a `string` and a `hash` may share a key,
            /// and it is the kind that tells their blocks apart.
            fn kind(&self) -> &'static str {
                match self {
                    PublishTarget::String { .. } => "string",
                    PublishTarget::Hash { .. } => "hash",
                }
            }
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
                    .strings()
                    .iter()
                    .filter(|(_, slot)| crate::wal_record::is_wal_resident(slot.block_slab_id()))
                    .map(|(key, slot)| {
                        (PublishTarget::String { key: key.to_string() }, slot.address())
                    })
                    .collect::<Vec<_>>();
                publish_targets.extend(
                    shard
                        .hashes
                        .iter()
                        .flat_map(|(key, fields)| {
                            fields.iter().filter_map(move |(field, address)| {
                                crate::wal_record::is_wal_resident(address.block_slab_id()).then(|| {
                                    (
                                        PublishTarget::Hash {
                                            key: key.to_string(),
                                            field: field.to_string(),
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
                    if let Some(address) = shard.string_address(key.as_str()) {
                        if crate::wal_record::is_wal_resident(address.block_slab_id()) {
                            publish_targets.push((
                                PublishTarget::String { key: key.clone() },
                                address,
                            ));
                        }
                    }
                    if let Some(fields) = shard.hashes.get(key) {
                        publish_targets.extend(fields.iter().filter_map(|(field, address)| {
                            crate::wal_record::is_wal_resident(address.block_slab_id()).then(|| {
                                (
                                    PublishTarget::Hash {
                                        key: key.clone(),
                                        field: field.to_string(),
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
        let (start_routing_bucket, end_routing_bucket) = self.shard_routing_range(shard_id);
        let mut publish_records = Vec::with_capacity(publish_targets.len());
        for (target, address) in publish_targets {
            let routing_bucket = crate::engine::hashing::block_routing_bucket(
                target.object_key(),
                start_routing_bucket,
                end_routing_bucket,
            );
            if let Some(bytes) = read_block_bytes(
                &self.cache,
                &self.block_store,
                shard_id,
                &address,
                BlockIdentity::of(shard_id, target.kind(), target.object_key(), target.component()),
                Some(routing_bucket),
            ) {
                // Derived BEFORE `target` moves into the tuple: the terms are borrowed from it.
                let object_id =
                    stable_block_object_id(shard_id, target.kind(), target.object_key());
                publish_records.push((
                    target,
                    address.clone(),
                    bytes,
                    Some(object_id),
                    Some(routing_bucket),
                ));
            }
        }
        if publish_records.is_empty() {
            return Ok(0);
        }
        let append_records = publish_records
            .iter()
            // `publish_records` owns these bytes and outlives the append, so the store can
            // borrow them rather than take a copy of every block being published.
            // This republishes a block that already exists, so it keeps the id it already
            // has. A block id is an index inside its object; handing out a fresh one here
            // would give the object two blocks claiming the same position.
            .map(|(_, address, bytes, object_id, routing_bucket)| {
                (
                    bytes.as_slice(),
                    *object_id,
                    *routing_bucket,
                    address.block_id().unwrap_or_default() as u32,
                )
            })
            .collect::<Vec<BlockAppendRecord>>();
        let published_addresses = self
            .block_store
            .append_batch_with_block_metadata(append_records)
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
                        // COMPARED AS AN ADDRESS, which is what `original` is. The row holds
                        // more than the address now, so comparing rows would make an unrelated
                        // field's change look like a concurrent write and skip the publish.
                        if shard.string_address(key.as_str()) != Some(original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot(
                                shard_id,
                                published.block_slab_id(),
                                published.offset(),
                                published.length(),
                                Some(crate::engine::hashing::block_routing_bucket(
                                    &key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            ),
                            bytes,
                        );
                        upsert_bucket_index_block(
                            shard,
                            shard_id,
                            "string",
                            &key,
                            None,
                            published.clone(),
                            false,
                        );
                        published_object_keys.insert(key.clone());
                        // ONE COPY OF THE CHARACTERS. `into_boxed_str` was a free move of the
                        // caller's buffer, so converting it to a bare `Arc::from` would have
                        // ADDED an allocation rather than removed one. Take the handle the block
                        // index already holds, and allocate only when it has none.
                        let shared = shard
                            .bucket_index
                            .any_shared_object_key(&key);
                        let stored = shared.unwrap_or_else(|| std::sync::Arc::from(key));
                        shard.set_string_address(stored, &published);
                    }
                    PublishTarget::Hash { key, field } => {
                        let current = shard.hashes.get(&key).and_then(|fields| fields.get(&field));
                        if current != Some(&original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot(
                                shard_id,
                                published.block_slab_id(),
                                published.offset(),
                                published.length(),
                                Some(crate::engine::hashing::block_routing_bucket(
                                    &key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            ),
                            bytes,
                        );
                        super::recorded_map::install_element::<super::recorded_map::HashKind>(
                            shard,
                            shard_id,
                            &key,
                            Some(field.clone()),
                            std::sync::Arc::from(&field[..]),
                            published.clone(),
                            false,
                            published,
                        );
                        published_object_keys.insert(key.clone());
                    }
                }
            }
            // COUNTED BEFORE THE SET IS CONSUMED BY THE LOOP BELOW. These are the keys whose
            // blocks this call actually relocated -- the ones that passed the concurrent-write
            // check above -- which is the unit the interval is counted in.
            let published_key_count = published_object_keys.len() as u64;
            for object_key in published_object_keys {
                clear_published_object_dirty_state(shard, &object_key);
            }
            refresh_bucket_runtime_flags(shard);
            // THE CHECKPOINT IS DUE PER PUBLISHED KEY, NOT PER PUBLISH.
            //
            // Re-encoding the whole served index costs about 47 bytes per RESIDENT key -- 376,012
            // encoded bytes to publish ONE key at 8,000 keys, measured by
            // `what_one_visibility_publish_writes_grows_with_the_store_not_the_publish`, against a
            // figure that does not move when the named set widens 256-fold on a fixed store -- so
            // a checkpoint per call charges the whole store to whatever the call happened to
            // name. `publish_index_flush_countdown` spends this call's published keys against the
            // engine's interval and only encodes when they run it out, which bounds the index
            // bytes ONE published key can be charged instead of bounding nothing.
            //
            // SAFE BECAUSE THE RELOCATED ROWS ARE ALREADY DURABLE BY REPLAY, which is the same
            // ground the write path's deferred index sync stands on. This publish does not retire
            // the relocated blocks' registrations and does not clear any bucket's
            // `first_dirty_wal_sequence` -- that is written to zero at exactly one site, and only
            // by a durable dump -- so both WAL retention floors still sit below the records it
            // rewrote, and a checkpoint that lands late, lands stale or never lands is re-derived
            // by replaying them.
            // `a_deferred_publish_checkpoint_still_reloads_with_the_published_values` drives that
            // rather than asserting it.
            //
            // THE COUNTDOWN IS SPENT WHETHER OR NOT THE CALL IS DUE, and reloaded only on a
            // checkpoint, so the calls between two checkpoints sum to the interval rather than
            // resetting whenever a call happens to name one key.
            shard.publish_index_flush_countdown = shard
                .publish_index_flush_countdown
                .saturating_sub(published_key_count);
            if shard.publish_index_flush_countdown == 0 {
                shard.publish_index_flush_countdown =
                    self.visibility_publish_index_flush_interval();
                Some(serialize_index(shard))
            } else {
                None
            }
        };
        // `None` when this call's published keys did not run the countdown out -- see the
        // countdown above. The index-log anchor goes with the index and not without it: an anchor
        // record carries the digest and length of the index file it names, so there is nothing for
        // it to name on a call that wrote no index.
        let Some(index_bytes) = index_bytes else {
            return Ok(0);
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
