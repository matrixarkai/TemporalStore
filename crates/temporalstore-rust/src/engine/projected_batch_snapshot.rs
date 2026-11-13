// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::super::execute_on_shard::ProjectedHashValues;
use crate::Status;

type Row = (usize, Result<ProjectedHashValues, Status>);

const SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;
const SNAPSHOT_ROWS: usize = 4096;

pub(super) struct ProjectedBatchSnapshot {
    first: Option<Row>,
    remaining: Vec<Row>,
    retained_bytes: usize,
    budget: usize,
    streaming: bool,
}

impl ProjectedBatchSnapshot {
    pub(super) fn new(rows: usize) -> Self {
        let remaining = Vec::with_capacity(rows.min(SNAPSHOT_ROWS).saturating_sub(1));
        let retained_bytes = remaining
            .capacity()
            .saturating_mul(std::mem::size_of::<Row>());
        Self {
            first: None,
            remaining,
            retained_bytes,
            budget: SNAPSHOT_BYTES,
            streaming: false,
        }
    }

    pub(super) fn push(
        &mut self,
        index: usize,
        result: Result<ProjectedHashValues, Status>,
        visit: &mut impl FnMut(usize, Result<ProjectedHashValues, Status>),
    ) {
        let bytes = match &result {
            Ok(values) => values.retained_bytes(),
            Err(status) => status
                .code
                .capacity()
                .saturating_add(status.message.capacity()),
        };
        let next_bytes = self.retained_bytes.saturating_add(bytes);
        if !self.streaming
            && next_bytes <= self.budget
            && (self.first.is_none() || self.remaining.len() < self.remaining.capacity())
        {
            self.retained_bytes = next_bytes;
            if self.first.is_none() {
                self.first = Some((index, result));
            } else {
                self.remaining.push((index, result));
            }
            return;
        }
        // Preserve one request-wide snapshot. Overflow keeps the original under-lock
        // streaming path instead of unlocking and observing several different versions.
        self.flush(visit);
        self.streaming = true;
        visit(index, result);
    }

    pub(super) fn flush(
        &mut self,
        visit: &mut impl FnMut(usize, Result<ProjectedHashValues, Status>),
    ) {
        if let Some((index, result)) = self.first.take() {
            visit(index, result);
        }
        for (index, result) in self.remaining.drain(..) {
            visit(index, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn point_snapshot_does_not_allocate_a_row_vector() {
        let mut snapshot = ProjectedBatchSnapshot::new(1);
        assert_eq!(snapshot.remaining.capacity(), 0);
        let mut observed = Vec::new();
        let mut visit = |index, result: Result<ProjectedHashValues, Status>| {
            observed.push((index, result.unwrap().len()));
        };
        snapshot.push(0, Ok(ProjectedHashValues::Missing(3)), &mut visit);
        assert!(snapshot.first.is_some());
        snapshot.flush(&mut visit);
        assert_eq!(observed, [(0, 3)]);
    }

    #[test]
    fn snapshots_count_complete_frames_and_owned_capacity() {
        let frame: Arc<[u8]> = vec![7; 8192].into();
        let values = ProjectedHashValues::Shared {
            frame: frame.clone(),
            ranges: vec![Some(0..1)],
        };
        assert!(values.retained_bytes() >= frame.len());
        let mut value = Vec::with_capacity(4096);
        value.push(1);
        let owned = ProjectedHashValues::Owned(vec![Some(value)]);
        assert!(owned.retained_bytes() >= 4096);
        let mut snapshot = ProjectedBatchSnapshot::new(2);
        snapshot.budget = 4096;
        let mut indexes = Vec::new();
        let mut visit = |index, _: Result<ProjectedHashValues, Status>| indexes.push(index);
        snapshot.push(0, Ok(values), &mut visit);
        assert!(snapshot.streaming);
        assert!(snapshot.first.is_none());
        snapshot.push(1, Ok(owned), &mut visit);
        snapshot.flush(&mut visit);
        assert_eq!(indexes, [0, 1]);
        assert_eq!(Arc::strong_count(&frame), 1);
    }

    #[test]
    fn overflow_flushes_successes_and_errors_in_order() {
        let mut snapshot = ProjectedBatchSnapshot::new(3);
        let error = Status::error("limited", "read rejected");
        snapshot.budget =
            snapshot.retained_bytes + error.code.capacity() + error.message.capacity();
        let mut indexes = Vec::new();
        let mut visit = |index, result: Result<ProjectedHashValues, Status>| {
            indexes.push((index, result.is_ok()));
        };
        snapshot.push(0, Ok(ProjectedHashValues::Missing(1)), &mut visit);
        snapshot.push(1, Err(error), &mut visit);
        assert!(!snapshot.streaming);
        snapshot.push(
            2,
            Ok(ProjectedHashValues::Owned(vec![Some(vec![5; 1024])])),
            &mut visit,
        );
        assert!(snapshot.streaming);
        snapshot.flush(&mut visit);
        assert_eq!(indexes, [(0, true), (1, false), (2, true)]);
    }

    #[test]
    fn direct_calls_cannot_grow_snapshot_row_storage() {
        let mut snapshot = ProjectedBatchSnapshot::new(SNAPSHOT_ROWS + 50);
        let capacity = snapshot.remaining.capacity();
        let mut seen = 0;
        let mut visit = |index, _: Result<ProjectedHashValues, Status>| {
            assert_eq!(index, seen);
            seen += 1;
        };
        for index in 0..SNAPSHOT_ROWS + 50 {
            snapshot.push(index, Ok(ProjectedHashValues::Missing(1)), &mut visit);
        }
        assert!(snapshot.streaming);
        assert_eq!(snapshot.remaining.capacity(), capacity);
        snapshot.flush(&mut visit);
        assert_eq!(seen, SNAPSHOT_ROWS + 50);
    }
}
