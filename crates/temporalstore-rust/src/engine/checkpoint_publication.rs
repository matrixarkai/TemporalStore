// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::types::ShardId;

#[derive(Debug)]
struct PublicationState {
    revision: AtomicU64,
    last_dump_time_ms: AtomicU64,
    closing: AtomicBool,
    active: Mutex<bool>,
}

impl PublicationState {
    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    fn advance(&self) -> io::Result<()> {
        let revision = self
            .revision
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "checkpoint revision exhausted"))?;
        self.revision.store(revision, Ordering::Release);
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CheckpointPublication {
    shard_id: ShardId,
    state: Arc<PublicationState>,
    revision: u64,
}

impl CheckpointPublication {
    pub(super) fn allocate_dump_time_ms(&self, now_ms: u64, persisted_ms: u64) -> io::Result<u64> {
        let exhausted = || io::Error::new(io::ErrorKind::Other, "checkpoint timestamp exhausted");
        let minimum = persisted_ms.checked_add(1).ok_or_else(exhausted)?.max(now_ms);
        let previous = self.state.last_dump_time_ms.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |previous| previous.checked_add(1).map(|next| next.max(minimum)),
        ).map_err(|_| exhausted())?;
        Ok((previous + 1).max(minimum))
    }
}

#[derive(Debug, Default)]
pub(super) struct CheckpointPublications {
    states: Mutex<HashMap<ShardId, Arc<PublicationState>>>,
}

pub(super) struct ClosingPublication {
    shard_id: ShardId,
    state: Arc<PublicationState>,
}

pub(super) struct DrainedPublication(ClosingPublication);

impl DrainedPublication {
    pub(super) fn shard_id(&self) -> ShardId {
        self.0.shard_id
    }
}

impl ClosingPublication {
    /// Wait outside the engine shard guards. The returned token holds no mutex.
    pub(super) fn drain(self) -> DrainedPublication {
        {
            let mut active = self.state.active.lock().expect("checkpoint publication lock poisoned");
            *active = false;
        }
        DrainedPublication(self)
    }
}

impl CheckpointPublications {
    fn state(&self, shard_id: ShardId) -> Arc<PublicationState> {
        self.states
            .lock()
            .expect("checkpoint map lock poisoned")
            .entry(shard_id)
            .or_insert_with(|| {
                Arc::new(PublicationState {
                    revision: AtomicU64::new(0),
                    last_dump_time_ms: AtomicU64::new(0),
                    closing: AtomicBool::new(false),
                    active: Mutex::new(true),
                })
            })
            .clone()
    }

    pub(super) fn capture(&self, shard_id: ShardId) -> CheckpointPublication {
        let state = self.state(shard_id);
        // Captures run under the shard guard; never wait there for publication I/O.
        let revision = state.revision.load(Ordering::Acquire);
        CheckpointPublication {
            shard_id,
            state,
            revision,
        }
    }

    pub(super) fn publish<T>(
        &self,
        captured: &CheckpointPublication,
        write: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        let current = self
            .states
            .lock()
            .expect("checkpoint map lock poisoned")
            .get(&captured.shard_id)
            .cloned();
        if !current
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &captured.state))
        {
            return Ok(None);
        }
        let active = captured
            .state
            .active
            .lock()
            .expect("checkpoint publication lock poisoned");
        if !*active || captured.state.is_closing() || captured.state.revision.load(Ordering::Acquire) != captured.revision {
            return Ok(None);
        }
        // An I/O error may follow a successful rename. Invalidate older captures before
        // attempting publication, including when its final durability step fails.
        captured.state.advance()?;
        write().map(Some)
    }

    pub(super) fn publish_current<T>(
        &self,
        shard_id: ShardId,
        write: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let publication = self.state(shard_id);
        let active = publication
            .active
            .lock()
            .expect("checkpoint publication lock poisoned");
        if !*active || publication.is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "checkpoint generation is closed",
            ));
        }
        publication.advance()?;
        write()
    }

    /// Check a completed publication without waiting for another publisher's I/O.
    pub(super) fn is_current_after_publish(&self, captured: &CheckpointPublication) -> bool {
        let Some(revision) = captured.revision.checked_add(1) else { return false; };
        self.states.lock().expect("checkpoint map lock poisoned")
            .get(&captured.shard_id)
            .is_some_and(|state| Arc::ptr_eq(state, &captured.state) && !state.is_closing()
                && state.revision.load(Ordering::Acquire) == revision)
    }

    /// Reserve close while the caller owns the shard's lifecycle reservation and state guard.
    /// Keep the entry discoverable so ID-based lookups cannot allocate another generation.
    pub(super) fn begin_close(&self, shard_id: ShardId) -> ClosingPublication {
        let state = self.state(shard_id);
        assert!(!state.closing.swap(true, Ordering::AcqRel), "publication already closing");
        ClosingPublication { shard_id, state }
    }

    /// Check identity before detaching live state and clearing ID-based registrations.
    pub(super) fn is_current_close(&self, drained: &DrainedPublication) -> bool {
        let closing = &drained.0;
        self.states.lock().expect("checkpoint map lock poisoned")
            .get(&closing.shard_id)
            .is_some_and(|state| Arc::ptr_eq(state, &closing.state) && state.is_closing())
    }

    /// Remove the closed entry only after the lifecycle reservation's side cleanup is complete.
    pub(super) fn finish_close(&self, drained: DrainedPublication) -> bool {
        let closing = drained.0;
        let mut states = self.states.lock().expect("checkpoint map lock poisoned");
        if !states.get(&closing.shard_id)
            .is_some_and(|state| Arc::ptr_eq(state, &closing.state) && state.is_closing())
        {
            return false;
        }
        states.remove(&closing.shard_id);
        true
    }

    pub(super) fn forget(&self, shard_id: ShardId) {
        let state = self.states.lock().expect("checkpoint map lock poisoned")
            .get(&shard_id).cloned();
        if let Some(state) = state {
            let mut active = state.active.lock().expect("checkpoint publication lock poisoned");
            let mut states = self.states.lock().expect("checkpoint map lock poisoned");
            if states.get(&shard_id).is_some_and(|current| Arc::ptr_eq(current, &state) && !current.is_closing()) {
                *active = false;
                states.remove(&shard_id);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn tracked_shards_for_test(&self) -> usize {
        self.states.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_names_advance_when_the_wall_clock_stops_or_moves_back() {
        let publications = CheckpointPublications::default();
        let capture = publications.capture(1);
        assert_eq!(capture.allocate_dump_time_ms(1000, 0).unwrap(), 1000);
        assert_eq!(capture.allocate_dump_time_ms(1000, 0).unwrap(), 1001);
        assert_eq!(capture.allocate_dump_time_ms(900, 0).unwrap(), 1002);
        assert_eq!(publications.capture(2).allocate_dump_time_ms(900, 0).unwrap(), 900);
    }

    #[test]
    fn a_reopened_dump_clock_starts_after_the_persisted_parent() {
        let publications = CheckpointPublications::default();
        assert_eq!(publications.capture(1).allocate_dump_time_ms(8000, 9000).unwrap(), 9001);
        publications.forget(1);
        assert_eq!(publications.capture(1).allocate_dump_time_ms(8000, 9001).unwrap(), 9002);
    }

    #[test]
    fn concurrent_dump_captures_reserve_distinct_times() {
        let publications = Arc::new(CheckpointPublications::default());
        let workers: Vec<_> = (0..16).map(|_| {
            let publications = Arc::clone(&publications);
            std::thread::spawn(move || {
                let capture = publications.capture(1);
                (0..64).map(|_| capture.allocate_dump_time_ms(1000, 0).unwrap()).collect::<Vec<_>>()
            })
        }).collect();
        let stamps: std::collections::BTreeSet<_> = workers.into_iter()
            .flat_map(|worker| worker.join().unwrap()).collect();
        assert_eq!(stamps.len(), 1024);
        assert_eq!(stamps.first(), Some(&1000));
        assert_eq!(stamps.last(), Some(&2023));
    }

    #[test]
    fn an_exhausted_dump_clock_refuses_to_wrap() {
        let publications = CheckpointPublications::default();
        let capture = publications.capture(1);
        assert!(capture.allocate_dump_time_ms(1000, u64::MAX).is_err());
        assert_eq!(capture.allocate_dump_time_ms(1000, 0).unwrap(), 1000);
        assert_eq!(capture.allocate_dump_time_ms(u64::MAX, 0).unwrap(), u64::MAX);
        assert!(capture.allocate_dump_time_ms(1000, 0).is_err());
    }

    #[test]
    fn a_completed_publication_is_current_only_until_replaced_or_forgotten() {
        let publications = CheckpointPublications::default();
        let first = publications.capture(1);
        assert!(!publications.is_current_after_publish(&first));
        assert!(publications.publish(&first, || Ok(())).unwrap().is_some());
        assert!(publications.is_current_after_publish(&first));
        let next = publications.capture(1);
        assert!(publications.publish(&next, || Ok(())).unwrap().is_some());
        assert!(!publications.is_current_after_publish(&first));
        assert!(publications.is_current_after_publish(&next));
        publications.forget(1);
        assert!(!publications.is_current_after_publish(&next));
        let _ = publications.capture(1);
        assert!(!publications.is_current_after_publish(&next));
    }

    #[test]
    fn captures_do_not_wait_for_checkpoint_io() {
        let publications = Arc::new(CheckpointPublications::default());
        let writer = Arc::clone(&publications);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let publishing = std::thread::spawn(move || {
            writer
                .publish_current(1, || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();
        let reader = Arc::clone(&publications);
        let (captured_tx, captured_rx) = std::sync::mpsc::channel();
        let capturing = std::thread::spawn(move || {
            captured_tx.send(reader.capture(1)).unwrap();
        });
        let captured = captured_rx.recv_timeout(std::time::Duration::from_secs(10));
        release_tx.send(()).unwrap();
        publishing.join().unwrap();
        capturing.join().unwrap();
        let captured = captured.expect("capture waited for publication I/O");
        assert!(publications
            .publish(&captured, || Ok(()))
            .unwrap()
            .is_some());
    }

    #[test]
    fn a_newer_publication_rejects_an_older_capture() {
        let publications = CheckpointPublications::default();
        let older = publications.capture(1);
        publications.publish_current(1, || Ok(())).unwrap();
        let wrote = std::cell::Cell::new(false);
        assert!(publications
            .publish(&older, || {
                wrote.set(true);
                Ok(())
            })
            .unwrap()
            .is_none());
        assert!(!wrote.get());
    }

    #[test]
    fn a_failed_publication_still_invalidates_older_captures() {
        let publications = CheckpointPublications::default();
        let older = publications.capture(1);
        assert!(publications
            .publish_current::<()>(1, || {
                Err(io::Error::new(io::ErrorKind::Other, "publication failed"))
            })
            .is_err());
        assert!(publications.publish(&older, || Ok(())).unwrap().is_none());
    }

    #[test]
    fn unrelated_shards_keep_their_captured_revisions() {
        let publications = CheckpointPublications::default();
        let captured = publications.capture(1);
        publications.publish_current(2, || Ok(())).unwrap();
        assert!(publications
            .publish(&captured, || Ok(()))
            .unwrap()
            .is_some());
    }

    #[test]
    fn closing_publication_keeps_its_identity_until_cleanup_finishes() {
        let publications = CheckpointPublications::default();
        let before = publications.capture(1);
        let closing = publications.begin_close(1);
        let during = publications.capture(1);
        assert!(Arc::ptr_eq(&before.state, &during.state));
        assert!(during.state.is_closing());
        assert!(publications.publish(&during, || -> io::Result<()> {
            panic!("closed capture published");
        }).unwrap().is_none());
        assert!(publications.publish_current(1, || -> io::Result<()> {
            panic!("ID-only publisher entered a closing generation");
        }).is_err());
        let drained = closing.drain();
        assert!(publications.is_current_close(&drained));
        assert!(publications.finish_close(drained));
        let replacement = publications.capture(1);
        assert!(!Arc::ptr_eq(&during.state, &replacement.state));
        assert!(publications.publish(&before, || Ok(())).unwrap().is_none());
        assert!(publications.publish(&replacement, || Ok(())).unwrap().is_some());
    }

    #[test]
    fn close_reservation_does_not_wait_for_a_failing_publisher() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let publications = Arc::new(CheckpointPublications::default());
        let writer = Arc::clone(&publications);
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel();
        let publishing = std::thread::spawn(move || {
            writer.publish_current(1, || -> io::Result<()> {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                Err(io::Error::other("publication failed"))
            })
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        let closer = Arc::clone(&publications);
        let (reserved_tx, reserved_rx) = channel();
        let closing = std::thread::spawn(move || {
            let reservation = closer.begin_close(1);
            reserved_tx.send(()).unwrap();
            reservation.drain()
        });
        let reserved = reserved_rx.recv_timeout(Duration::from_secs(5));
        let peer = publications.publish_current(2, || Ok(()));
        release_tx.send(()).unwrap();
        assert!(publishing.join().unwrap().is_err());
        let drained = closing.join().unwrap();
        reserved.expect("close reservation waited for the publication");
        peer.unwrap();
        assert!(publications.is_current_close(&drained));
        assert!(publications.finish_close(drained));
    }

    #[test]
    fn forgotten_shards_release_state_and_refuse_old_captures() {
        let publications = CheckpointPublications::default();
        let older = publications.capture(1);
        publications.forget(1);
        assert!(publications.states.lock().unwrap().is_empty());
        let newer = publications.capture(1);
        assert!(publications.publish(&older, || Ok(())).unwrap().is_none());
        assert!(publications.publish(&newer, || Ok(())).unwrap().is_some());
        publications.forget(1);
        assert!(publications.states.lock().unwrap().is_empty());
    }
}
