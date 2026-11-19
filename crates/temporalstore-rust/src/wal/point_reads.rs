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
    completed: Mutex<(u64, u64)>,
}

impl State {
    fn enter(self: Arc<Self>) -> Permit {
        let mut active = self.active.lock().expect("point read admission poisoned");
        while *active >= MAX_IN_FLIGHT {
            active = self.available.wait(active).expect("point read admission poisoned");
        }
        *active += 1;
        drop(active);
        Permit { state: self }
    }

    fn complete(&self, bytes: u64) {
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

struct Permit {
    state: Arc<State>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self.state.active.lock().expect("point read admission poisoned");
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
}

fn prepare(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    log_id: u64,
    sequence: u64,
) -> Result<Option<PreparedRead>, WriteAheadLogError> {
    let state = Arc::clone(&store.lock_inner_raw().point_reads);
    // Admission waits outside both the writer mutex and the retention registry.
    let permit = state.enter();
    let lease = store.retain_read_sequence(shard, sequence);
    let mut inner = store.lock_inner();
    let WriteAheadLogInner { root, point_readers, .. } = &mut *inner;
    let Some(source) = point_readers.take(root, shard, log_id)? else {
        return Ok(None);
    };
    Ok(Some(PreparedRead { source, permit, _lease: lease, shard, log_id, sequence }))
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
            let selected = match packed_frames::PointReader::new(&mut self.source.file, self.source.physical)? {
                Some(mut reader) => {
                    let result = crate::wal_proto::indexed::read(
                        &mut reader, 0, &self.permit.state.indexed, self.shard, self.log_id,
                        self.sequence, index, object_id, element_name, &mut 0,
                    );
                    bytes_read = reader.physical_bytes;
                    result
                }
                None => Ok(None),
            };
            if let Some(block) = selected? {
                return Ok(Some(CarriedBlockRead::Block(block)));
            }
            self.source.file.seek(SeekFrom::Start(self.source.physical))?;
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
    store: &LocalWriteAheadLogStore,
    prepared: PreparedRead,
    result: Result<Option<CarriedBlockRead>, WriteAheadLogError>,
    bytes_read: u64,
) -> Result<Option<CarriedBlockRead>, WriteAheadLogError> {
    prepared.permit.state.complete(bytes_read);
    if result.is_ok() {
        // Reuse is optional. A completed read must not queue behind an append or GC
        // merely to return a descriptor; the existing cache still enforces its bounds.
        match store.inner.try_lock() {
            Ok(mut inner) => inner.point_readers.put(prepared.source),
            Err(std::sync::TryLockError::WouldBlock) => (),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("write-ahead log lock poisoned"),
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
