// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Leased, bounded record payload reads outside the WAL writer mutex.

use super::*;

const MAX_IN_FLIGHT: usize = 8;

#[derive(Debug, Default)]
pub(super) struct State {
    active: Mutex<usize>,
    available: std::sync::Condvar,
    indexed: crate::wal_proto::indexed::Cache,
    layouts: packed_frames::LayoutCache,
}

impl State {
    pub(super) fn enter(self: Arc<Self>) -> Permit {
        let mut active = self.active.lock().expect("point read admission poisoned");
        while *active >= MAX_IN_FLIGHT {
            active = self
                .available
                .wait(active)
                .expect("point read admission poisoned");
        }
        *active += 1;
        drop(active);
        Permit { state: self }
    }
}

#[derive(Debug, Default)]
pub(super) struct Counts {
    completed: Mutex<(u64, u64)>,
}

impl Counts {
    pub(super) fn complete(&self, bytes: u64) {
        let mut counts = self.completed.lock().expect("point read counts poisoned");
        counts.0 = counts.0.saturating_add(1);
        counts.1 = counts.1.saturating_add(bytes);
    }

    pub(super) fn stats(&self, mut stats: WriteAheadLogStats) -> WriteAheadLogStats {
        let counts = self.completed.lock().expect("point read counts poisoned");
        stats.reads = stats.reads.saturating_add(counts.0);
        stats.bytes_read = stats.bytes_read.saturating_add(counts.1);
        stats
    }
}

pub(super) struct Permit {
    state: Arc<State>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .state
            .active
            .lock()
            .expect("point read admission poisoned");
        *active -= 1;
        self.state.available.notify_one();
    }
}

struct PreparedRead {
    source: point_readers::Reader,
    permit: Permit,
    _lease: WriteAheadLogReadLease,
    shard: ShardId,
    log_id: u64,
    sequence: u64,
    counts: Arc<Counts>,
    writer: std::sync::Weak<ShardWriter>,
}

fn prepare(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    log_id: u64,
    sequence: u64,
) -> Result<Option<PreparedRead>, WriteAheadLogError> {
    let writer = store.shard_writer(shard);
    let (state, counts) = {
        let inner = writer.lock_inner_raw();
        (inner.point_reads.clone(), inner.point_read_counts.clone())
    };
    // Admission waits outside both the writer mutex and the retention registry.
    let permit = state.enter();
    let lease = {
        let coordinator = &writer.coordinator;
        let mut readers = coordinator
            .read_retention
            .lock()
            .expect("read retention lock poisoned");
        store.register_read_lease(coordinator, &mut readers, sequence)
    };
    for attempt in 0..2 {
        let result = {
            let mut inner = writer.lock_inner();
            let WriteAheadLogInner {
                root,
                point_readers,
                ..
            } = &mut *inner;
            point_readers.take(root, shard, log_id)
        };
        match result {
            Ok(Some(source)) => {
                return Ok(Some(PreparedRead {
                    source,
                    permit,
                    _lease: lease,
                    shard,
                    log_id,
                    sequence,
                    counts,
                    writer: Arc::downgrade(&writer),
                }))
            }
            Ok(None) => return Ok(None),
            Err(error)
                if attempt == 0
                    && point_readers::descriptor_pressure(&error)
                    && store.evict_idle_reader_for_open(shard) =>
            {
                ()
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded descriptor retry returns on its final attempt")
}

impl PreparedRead {
    fn load(
        &mut self,
        index: usize,
        object_id: u64,
        element_name: Option<&str>,
    ) -> (Result<Option<CarriedBlockRead>, WriteAheadLogError>, u64) {
        let mut bytes_read = 0;
        let result = (|| {
            let selected = match packed_frames::PointReader::with_layout_cache(
                &mut self.source.file,
                self.source.physical,
                self.source.sealed.then_some(self.source.stamp).flatten(),
                &self.permit.state.layouts,
            )? {
                Some(mut reader) => {
                    let result = crate::wal_proto::indexed::read(
                        &mut reader,
                        0,
                        &self.permit.state.indexed,
                        self.shard,
                        self.log_id,
                        self.sequence,
                        index,
                        object_id,
                        element_name,
                        &mut 0,
                    );
                    bytes_read = reader.physical_bytes;
                    result
                }
                None => Ok(None),
            };
            if let Some(block) = selected? {
                return Ok(Some(CarriedBlockRead::Block(block)));
            }
            self.source
                .file
                .seek(SeekFrom::Start(self.source.physical))?;
            let mut reader = BufReader::with_capacity(1024, &mut self.source.file);
            let raw = read_raw_record(&mut reader)?;
            if let Some(record) = raw.as_ref() {
                bytes_read += record.physical_bytes;
            }
            Ok(raw.map(|record| CarriedBlockRead::Record(record.bytes)))
        })();
        (result, bytes_read)
    }
}

fn finish(
    _store: &LocalWriteAheadLogStore,
    prepared: PreparedRead,
    result: Result<Option<CarriedBlockRead>, WriteAheadLogError>,
    bytes_read: u64,
) -> Result<Option<CarriedBlockRead>, WriteAheadLogError> {
    prepared.counts.complete(bytes_read);
    if result.is_ok() {
        // Reuse is optional. A completed read must not queue behind an append or GC
        // merely to return a descriptor; the existing cache still enforces its bounds.
        if let Some(writer) = prepared.writer.upgrade() {
            match writer.inner.try_lock() {
                Ok(mut inner) => inner.point_readers.put(prepared.source),
                Err(std::sync::TryLockError::WouldBlock) => (),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    panic!("write-ahead log lock poisoned")
                }
            };
        }
    }
    result
}

pub(super) fn read(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    log_id: u64,
    sequence: u64,
    index: usize,
    object_id: u64,
    element_name: Option<&str>,
) -> Result<Option<CarriedBlockRead>, WriteAheadLogError> {
    let Some(mut prepared) = prepare(store, shard, log_id, sequence)? else {
        return Ok(None);
    };
    let (result, bytes_read) = prepared.load(index, object_id, element_name);
    finish(store, prepared, result, bytes_read)
}

#[cfg(test)]
#[path = "point_read_concurrency_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "read_budget_tests.rs"]
mod budget_tests;
