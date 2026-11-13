// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::types::ShardId;

#[derive(Debug)]
struct PublicationState {
    revision: AtomicU64,
    active: Mutex<bool>,
}

impl PublicationState {
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

#[derive(Debug, Default)]
pub(super) struct CheckpointPublications {
    states: Mutex<HashMap<ShardId, Arc<PublicationState>>>,
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
        if !*active || captured.state.revision.load(Ordering::Acquire) != captured.revision {
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
        if !*active {
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
            .is_some_and(|state| Arc::ptr_eq(state, &captured.state)
                && state.revision.load(Ordering::Acquire) == revision)
    }

    pub(super) fn forget(&self, shard_id: ShardId) {
        let state = self
            .states
            .lock()
            .expect("checkpoint map lock poisoned")
            .remove(&shard_id);
        if let Some(state) = state {
            *state
                .active
                .lock()
                .expect("checkpoint publication lock poisoned") = false;
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
