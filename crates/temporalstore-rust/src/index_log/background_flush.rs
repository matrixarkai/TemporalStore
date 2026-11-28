// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::Condvar;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const TICK: Duration = Duration::from_millis(INDEX_LOG_FLUSH_INTERVAL_MS);
// Keep bursts on one worker and retire quiet streams, including registered
// shards that remain in the catalog after their last background write.
const IDLE_RETIRE: Duration = Duration::from_millis(
    INDEX_LOG_FLUSH_INTERVAL_MS * INDEX_LOG_FLUSHER_IDLE_TICKS as u64,
);

/// Store clones share the shutdown owner. Workers hold the registry only to
/// keep the final stream drain after every accepted operation.
#[derive(Debug)]
pub(super) struct BackgroundFlush {
    streams: Arc<IndexStreams>,
}

#[derive(Debug, Default)]
pub(super) struct StreamFlush {
    slot: Mutex<Slot>,
    changed: Condvar,
    #[cfg(test)]
    before_write: Mutex<Option<TestPause>>,
}

#[derive(Debug, Default)]
struct Slot {
    due: Option<Instant>,
    worker: Option<JoinHandle<()>>,
    closing: bool,
}

struct WorkerStream {
    // The writer must be released before the registry, even on unwinding.
    writer: Arc<Mutex<IndexLogInner>>,
    _registry: Arc<IndexStreams>,
}

impl BackgroundFlush {
    pub(super) fn new(streams: Arc<IndexStreams>) -> Self {
        Self { streams }
    }

    pub(super) fn request(
        &self,
        shard: ShardId,
        writer: &Arc<Mutex<IndexLogInner>>,
        control: &Arc<StreamFlush>,
    ) {
        if let Err(error) = control.request(shard, writer, &self.streams) {
            tracing::error!(shard, %error, "could not start Index flush worker");
        }
    }
}

impl Drop for BackgroundFlush {
    fn drop(&mut self) {
        let workers: Vec<_> = {
            let streams = self.streams.handles.read().expect("index stream registry poisoned");
            streams.values().filter_map(|handle| handle.flush.get()?.close()).collect()
        };
        if workers.iter().any(|worker| worker.thread().id() == thread::current().id()) {
            // A tracing subscriber can release the last store on a worker.
            // All worker captures keep the registry alive through retirement.
            return;
        }
        for worker in workers {
            let _ = worker.join();
        }
    }
}

impl StreamFlush {
    fn request(
        self: &Arc<Self>, shard: ShardId, writer: &Arc<Mutex<IndexLogInner>>,
        streams: &Arc<IndexStreams>,
    ) -> std::io::Result<()> {
        let mut slot = self.slot.lock().expect("Index flush slot poisoned");
        if slot.closing {
            return Ok(());
        }
        slot.due.get_or_insert_with(|| Instant::now() + TICK);
        if slot.worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            self.changed.notify_one();
            return Ok(());
        }
        // On spawn failure the due bit stays set. A subsequent request can
        // retry. Orderly shutdown still drains queued ordinary records.
        let control = Arc::clone(self);
        let stream = WorkerStream { writer: Arc::clone(writer), _registry: Arc::clone(streams) };
        slot.worker = Some(thread::Builder::new().name(format!("index-log-flush-{shard}"))
            .spawn(move || control.run(shard, stream))?);
        Ok(())
    }

    fn close(&self) -> Option<JoinHandle<()>> {
        let mut slot = self.slot.lock().expect("Index flush slot poisoned");
        slot.closing = true;
        slot.due = None;
        let worker = slot.worker.take();
        self.changed.notify_all();
        worker
    }

    fn run(&self, shard: ShardId, stream: WorkerStream) {
        let mut idle_deadline = Instant::now() + IDLE_RETIRE;
        let mut slot = self.slot.lock().expect("Index flush slot poisoned");
        loop {
            if slot.closing {
                return;
            }
            let remaining = slot.due.unwrap_or(idle_deadline).saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                slot = self.changed.wait_timeout(slot, remaining).expect("Index flush slot poisoned").0;
                continue;
            }
            if slot.due.take().is_none() {
                // The shutdown owner must acquire this slot before releasing
                // its registry. With closing=false, these two releases cannot
                // run a final writer drain. Expose the vacant slot only after
                // relinquishing both captures so a replacement cannot race it.
                drop(stream);
                let _ = slot.worker.take();
                return;
            }
            drop(slot);
            let retry = self.flush(shard, &stream.writer);
            idle_deadline = Instant::now() + IDLE_RETIRE;
            slot = self.slot.lock().expect("Index flush slot poisoned");
            if retry && !slot.closing {
                slot.due.get_or_insert_with(|| Instant::now() + TICK);
            }
        }
    }

    fn flush(&self, shard: ShardId, writer: &Mutex<IndexLogInner>) -> bool {
        let mut inner = match writer.try_lock() {
            Ok(inner) => inner,
            Err(std::sync::TryLockError::WouldBlock) => return true,
            Err(std::sync::TryLockError::Poisoned(_)) => return false,
        };
        #[cfg(test)]
        if let Some(pause) = self.before_write.lock().unwrap().take() {
            let _ = pause.arrived.send(());
            let _ = pause.resume.recv_timeout(Duration::from_secs(10));
        }
        let result = flush_queued_locked(&mut inner);
        drop(inner);
        if let Err(error) = result {
            tracing::error!(shard, %error, "index log batch write failed; it will be retried");
            return true;
        }
        false
    }
}

#[cfg(test)]
#[derive(Debug)]
struct TestPause {
    arrived: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn append(store: &LocalIndexLogStore, shard: ShardId, wal: u64) -> u64 {
        store.append_delta(shard, Vec::new(), Vec::new(), Some(wal), None, true, false).unwrap()
    }

    #[test]
    fn six_parked_index_workers_do_not_delay_peer_writes() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(root.path());
        let _queued = group_write_in_test(true);
        let _started = flusher_in_test(true);
        let mut blocked = Vec::new();
        for shard in 1..=6 {
            let writer = store.stream(shard);
            let (arrived, parked) = mpsc::channel();
            let (resume, released) = mpsc::channel();
            {
                let inner = writer.lock().unwrap();
                let control = inner.flush.get_or_init(Arc::default);
                *control.before_write.lock().unwrap() = Some(TestPause { arrived, resume: released });
            }
            assert_eq!(append(&store, shard, 1), 1);
            parked.recv_timeout(TIMEOUT).expect("peer never claimed its queued write");
            blocked.push((writer, resume));
        }
        let peer = store.clone();
        let (completed, receipt) = mpsc::channel();
        let foreground = thread::spawn(move || {
            let _queued = group_write_in_test(true);
            let _started = flusher_in_test(true);
            assert_eq!(append(&peer, 7, 1), 1);
            let deadline = Instant::now() + TIMEOUT;
            let path = index_log_path(&peer.root.path, 7);
            while !path.is_file() {
                assert!(Instant::now() < deadline, "peer worker did not write its record");
                thread::sleep(TICK);
            }
            assert_eq!(peer.read_delta_records(7, 0).unwrap()[0].applied_wal_sequence, Some(1));
            completed.send(()).unwrap();
        });
        let completed = receipt.recv_timeout(TIMEOUT);
        let all_parked = blocked.iter().all(|(writer, _)| {
            matches!(writer.try_lock(), Err(std::sync::TryLockError::WouldBlock))
        });
        for (_, resume) in blocked {
            let _ = resume.send(());
        }
        foreground.join().unwrap();
        completed.expect("an independent stream waited for a parked writer");
        assert!(all_parked, "a parked writer exited before the peer completed");
        drop(store);
        let reopened = LocalIndexLogStore::new(root.path());
        for shard in 1..=7 {
            let records = reopened.read_delta_records(shard, 0).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!((records[0].sequence, records[0].applied_wal_sequence), (1, Some(1)));
        }
    }

    #[test]
    fn checkpoint_capture_does_not_drain_a_failing_peer() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(root.path());
        let _queued = group_write_in_test(true);
        let _inactive = flusher_in_test(false);
        assert_eq!(store.append_delta(7, Vec::new(), Vec::new(), Some(9), None, true, true).unwrap(), 1);
        assert_eq!(append(&store, 8, 3), 1);
        let blocked_path = index_log_path(&store.root.path, 8);
        fs::create_dir(&blocked_path).unwrap();
        let capture = store.capture_checkpoint_relocations(7, 0, |_| {}).unwrap();
        assert_eq!(capture.through_sequence(), 1);
        {
            let writer = store.stream(8);
            let inner = writer.lock().unwrap();
            assert_eq!(inner.queued.len(), 1);
            assert!(inner.stuck.is_empty(), "target capture attempted a peer write");
        }
        fs::remove_dir(blocked_path).unwrap();
        assert_eq!(store.read_delta_records(8, 0).unwrap()[0].applied_wal_sequence, Some(3));
        drop(store);
        let reopened = LocalIndexLogStore::new(root.path());
        let target = reopened.read_delta_records(7, 0).unwrap();
        let peer = reopened.read_delta_records(8, 0).unwrap();
        assert_eq!(target.len(), 1);
        assert_eq!(peer.len(), 1);
        assert_eq!((target[0].sequence, target[0].applied_wal_sequence), (1, Some(9)));
        assert_eq!((peer[0].sequence, peer[0].applied_wal_sequence), (1, Some(3)));
    }
}
