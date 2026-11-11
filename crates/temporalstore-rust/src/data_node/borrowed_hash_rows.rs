// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::BorrowedHashTimestampRequest;

impl DataNodeRuntime {
    /// Retain binary payload slices through admission and timestamp comparison.
    pub fn batch_hash_timestamp_set_borrowed(
        &self,
        request: BorrowedHashTimestampRequest<'_>,
    ) -> BatchConditionalWriteResponse {
        // The existing path owns malformed, duplicate-schema, and mirrored requests.
        if !request.fields.iter().any(|field| field == &request.timestamp_field)
            || request.rows.iter().any(|row| row.3.len() != request.fields.len())
            || request.fields.iter().collect::<BTreeSet<_>>().len() != request.fields.len()
            || !crate::wal::wal_data_only_enabled()
        {
            return self.batch_hash_timestamp_set(request.into_owned());
        }
        let marker = Command::HashMultiSetIfTimestampOlder {
            key: String::new(), timestamp_field: String::new(), timestamp_seconds: 0,
            timestamp_nanos: 0, ttl_ms: None, entries: Vec::new(),
        };
        if let Err(status) = validate_foreground_write_allowed_inner(
            &self.inner, request.shard_id, std::slice::from_ref(&marker))
        {
            return BatchConditionalWriteResponse { status, applied: 0, skipped: 0, errors: Vec::new() };
        }
        let shared_wal_sink = self.inner.shared_wal_sink.read().expect("shared wal sink lock poisoned");
        if shared_wal_sink.is_some() {
            drop(shared_wal_sink);
            return self.batch_hash_timestamp_set(request.into_owned());
        }
        let shard = request.shard_id;
        let (response, keys) = self.inner.engine.batch_execute_borrowed_hash_rows(request);
        mark_dirty_keys(&self.inner.dirty, shard, keys);
        response
    }
}

#[cfg(test)]
#[path = "tests/borrowed_timestamp_payloads.rs"]
mod tests;
