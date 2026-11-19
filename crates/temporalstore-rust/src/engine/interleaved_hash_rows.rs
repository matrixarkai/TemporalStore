// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

const MAX_SLICE_ROWS: usize = 64;
const MAX_SLICE_BYTES: usize = 256 * 1024;

#[cfg(test)]
thread_local! {
    static AFTER_SLICE: std::cell::RefCell<Option<Box<dyn FnMut(&TemporalEngine, ShardId, usize)>>> =
        const { std::cell::RefCell::new(None) };
}

fn slice_len(source: &BatchCommandSource<'_>) -> usize {
    let BatchCommandSource::BorrowedTimestampRows { entries, rows, .. } = source else {
        unreachable!("interleaved writes own borrowed timestamp rows")
    };
    let schema_bytes = entries.iter().fold(64usize, |bytes, (name, _)| {
        bytes.saturating_add(name.len()).saturating_add(16)
    });
    let mut bytes = 0usize;
    let mut count = 0usize;
    for row in rows.as_slice().iter().take(MAX_SLICE_ROWS) {
        let row_bytes = row
            .3
            .iter()
            .fold(schema_bytes.saturating_add(row.0.len()), |bytes, value| {
                bytes.saturating_add(value.0.len())
            });
        if count > 0 && bytes.saturating_add(row_bytes) > MAX_SLICE_BYTES {
            break;
        }
        bytes = bytes.saturating_add(row_bytes);
        count += 1;
    }
    count
}

impl TemporalEngine {
    /// Rows remain atomic, while different slices may be observed independently.
    pub(crate) fn batch_execute_interleaved_hash_rows(
        &self,
        request: BorrowedHashTimestampRequest<'_>,
        mut authorize_slice: impl FnMut() -> Result<(), Status>,
    ) -> (BatchConditionalWriteResponse, Vec<String>) {
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if config.maxmemory_bytes.is_some()
            || !crate::wal::wal_data_only_enabled()
            || wal_legacy_recovery()
            || bulk_ingest_mode()
            || raft_applying()
            || replaying_wal()
        {
            if let Err(status) = authorize_slice() {
                return (
                    BatchResultCollector::new(0, Some(true))
                        .finish(status)
                        .into_compact(),
                    Vec::new(),
                );
            }
            return self.batch_execute_borrowed_hash_rows(request);
        }
        let shared_fields =
            container_blocks::SharedHashFields::new(request.fields.iter().map(String::as_str));
        let valid_rows = request
            .fields
            .iter()
            .any(|field| field == &request.timestamp_field)
            && request
                .rows
                .iter()
                .all(|row| row.3.len() == request.fields.len());
        let Some(shared_fields) = shared_fields.filter(|_| valid_rows) else {
            return (
                BatchResultCollector::new(0, Some(true))
                    .finish(Status::error(
                        "bad_request",
                        "conditional rows require a unique timestamp column and matching values",
                    ))
                    .into_compact(),
                Vec::new(),
            );
        };
        let shard_id = request.shard_id;
        let total_rows = request.rows.len();
        let mut completed_keys = Vec::with_capacity(total_rows);
        let mut source = BatchCommandSource::BorrowedTimestampRows {
            entries: request
                .fields
                .into_iter()
                .map(|field| (field, Vec::new()))
                .collect(),
            timestamp_field: Some(request.timestamp_field),
            ttl_ms: request.ttl_ms,
            rows: request.rows.into_iter(),
            current_values: Vec::new(),
            completed_keys: &mut completed_keys,
        };
        let mut result = BatchResultCollector::new(0, Some(true)).finish(Status::ok());
        while source.command_count() > 0 {
            if let Err(status) = authorize_slice() {
                result.status = status;
                break;
            }
            let first = total_rows - source.command_count();
            let count = slice_len(&source);
            let partial = self.batch_execute_inner(
                shard_id,
                &mut source,
                true,
                Some(config.async_storage),
                Some(true),
                BatchExecutionOptions {
                    max_commands: Some(count),
                    defer_barrier: true,
                    shared_hash_fields: Some(&shared_fields),
                },
            );
            if let Some(sequence) = partial.pending_barrier_sequence {
                result.pending_barrier_sequence =
                    Some(result.pending_barrier_sequence.unwrap_or(0).max(sequence));
            }
            if !partial.status.ok {
                result.status = partial.status;
                break;
            }
            debug_assert_eq!(source.command_count(), total_rows - first - count);
            result.applied += partial.applied;
            result.skipped += partial.skipped;
            result
                .errors
                .extend(partial.errors.into_iter().map(|mut error| {
                    error.index += first;
                    error
                }));
            #[cfg(test)]
            AFTER_SLICE.with(|slot| {
                let mut observer = slot.borrow_mut().take();
                if let Some(callback) = observer.as_mut() {
                    callback(self, shard_id, first + count);
                }
                *slot.borrow_mut() = observer;
            });
        }
        // Previously appended slices need their barrier even when a later slice is rejected.
        if let Some(sequence) = result.pending_barrier_sequence {
            if let Err(status) = self.commit_shared_row_barrier(shard_id, sequence) {
                result.status = status;
            }
        }
        if result.status.code == "wal_commit_failed" {
            if let Some(info) = self
                .infos
                .write()
                .expect("info lock poisoned")
                .get_mut(&shard_id)
            {
                info.recovering = true;
            }
        }
        drop(source);
        (result.into_compact(), completed_keys)
    }
}

#[cfg(test)]
#[path = "tests/interleaved_hash_rows.rs"]
mod tests;
