// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, TryLockError};

pub(super) struct ShardStateRegistry<T> {
    entries: RwLock<BTreeMap<u64, Arc<RwLock<Option<T>>>>>,
}

pub(super) struct ShardStateHandle<T> {
    shard_id: u64,
    state: Arc<RwLock<Option<T>>>,
}

impl<T> Clone for ShardStateHandle<T> {
    fn clone(&self) -> Self {
        Self { shard_id: self.shard_id, state: Arc::clone(&self.state) }
    }
}

impl<T> ShardStateHandle<T> {
    pub(super) fn shard_id(&self) -> u64 {
        self.shard_id
    }

    pub(super) fn same_incarnation(&self, other: &Self) -> bool {
        self.shard_id == other.shard_id && Arc::ptr_eq(&self.state, &other.state)
    }

    #[cfg(test)]
    pub(super) fn try_read_loaded_for_test(&self) -> Result<bool, std::sync::TryLockError<()>> {
        match self.state.try_read() {
            Ok(state) => Ok(state.is_some()),
            Err(std::sync::TryLockError::WouldBlock) => Err(std::sync::TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
        }
    }

    #[cfg(test)]
    pub(super) fn try_write_loaded_for_test(&self) -> Result<bool, std::sync::TryLockError<()>> {
        match self.state.try_write() {
            Ok(state) => Ok(state.is_some()),
            Err(std::sync::TryLockError::WouldBlock) => Err(std::sync::TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
        }
    }

    pub(super) fn with_read<R>(&self, read: impl FnOnce(Option<&T>) -> R) -> R {
        let state = self.state.read().expect("shard state lock poisoned");
        read(state.as_ref())
    }

    pub(super) fn with_write<R>(&self, write: impl FnOnce(Option<&mut T>) -> R) -> R {
        let mut state = self.state.write().expect("shard state lock poisoned");
        write(state.as_mut())
    }
}

impl<T> Default for ShardStateRegistry<T> {
    fn default() -> Self {
        Self { entries: RwLock::new(BTreeMap::new()) }
    }
}

impl<T> ShardStateRegistry<T> {
    pub(super) fn handle(&self, shard_id: u64) -> Option<ShardStateHandle<T>> {
        let entries = self.entries.read().expect("shard registry lock poisoned");
        entries.get(&shard_id).map(|state| ShardStateHandle {
            shard_id,
            state: Arc::clone(state),
        })
    }

    pub(super) fn shard_ids(&self) -> Vec<u64> {
        self.entries.read().expect("shard registry lock poisoned").keys().copied().collect()
    }

    pub(super) fn with_shard<R>(&self, shard_id: u64, read: impl FnOnce(Option<&T>) -> R) -> R {
        match self.handle(shard_id) {
            Some(handle) => handle.with_read(read),
            None => read(None),
        }
    }

    pub(super) fn with_shard_mut<R>(
        &self,
        shard_id: u64,
        write: impl FnOnce(Option<&mut T>) -> R,
    ) -> R {
        match self.handle(shard_id) {
            Some(handle) => handle.with_write(write),
            None => write(None),
        }
    }

    // Lifecycle callers must reserve the shard ID through side-resource cleanup.
    // The old state is returned so its destruction occurs outside registry guards.
    pub(super) fn replace(&self, shard_id: u64, state: T) -> Option<T> {
        let replacement = Arc::new(RwLock::new(Some(state)));
        loop {
            let Some(previous) = self.handle(shard_id) else {
                let mut entries = self.entries.write().expect("shard registry lock poisoned");
                if entries.contains_key(&shard_id) {
                    continue;
                }
                entries.insert(shard_id, replacement);
                return None;
            };
            let mut old = previous.state.write().expect("shard state lock poisoned");
            let mut entries = self.entries.write().expect("shard registry lock poisoned");
            if !entries.get(&shard_id).is_some_and(|current| Arc::ptr_eq(current, &previous.state)) {
                continue;
            }
            let retired = old.take();
            entries.insert(shard_id, replacement);
            return retired;
        }
    }

    pub(super) fn retire_if_current(&self, expected: &ShardStateHandle<T>) -> Option<T> {
        let mut state = expected.state.write().expect("shard state lock poisoned");
        let mut entries = self.entries.write().expect("shard registry lock poisoned");
        if !entries.get(&expected.shard_id).is_some_and(|current| Arc::ptr_eq(current, &expected.state)) {
            return None;
        }
        let retired = state.take();
        entries.remove(&expected.shard_id);
        retired
    }

    fn snapshot(&self) -> Vec<ShardStateHandle<T>> {
        self.entries.read().expect("shard registry lock poisoned").iter()
            .map(|(&shard_id, state)| ShardStateHandle { shard_id, state: Arc::clone(state) })
            .collect()
    }

    fn same_membership(
        entries: &BTreeMap<u64, Arc<RwLock<Option<T>>>>,
        handles: &[ShardStateHandle<T>],
    ) -> bool {
        entries.len() == handles.len() && handles.iter().all(|handle| {
            entries.get(&handle.shard_id).is_some_and(|state| Arc::ptr_eq(state, &handle.state))
        })
    }

    // Coherent cross-shard operations lock owners in ascending order. No registry
    // guard or earlier owner guard is held while waiting for a busy owner.
    // Retry with fresh membership after the wait. The final read guard freezes
    // membership while the callback sees a coherent view of all owners.
    pub(super) fn with_all_shards_read<R>(
        &self,
        read: impl FnOnce(&BTreeMap<u64, &T>) -> R,
    ) -> R {
        'capture: loop {
            let handles = self.snapshot();
            let mut states = Vec::with_capacity(handles.len());
            for handle in &handles {
                match handle.state.try_read() {
                    Ok(state) => states.push(state),
                    Err(TryLockError::WouldBlock) => {
                        drop(states);
                        #[cfg(test)]
                        tests::notify_capture_wait(handle.shard_id);
                        drop(handle.state.read().expect("shard state lock poisoned"));
                        continue 'capture;
                    }
                    Err(TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
                }
            }
            let entries = self.entries.read().expect("shard registry lock poisoned");
            if !Self::same_membership(&entries, &handles) {
                continue;
            }
            let view = handles.iter().zip(&states).map(|(handle, state)| {
                (handle.shard_id, state.as_ref().expect("live shard has no state"))
            }).collect();
            return read(&view);
        }
    }

    pub(super) fn with_all_shards_mut<R>(
        &self,
        write: impl FnOnce(&mut BTreeMap<u64, &mut T>) -> R,
    ) -> R {
        'capture: loop {
            let handles = self.snapshot();
            let mut states = Vec::with_capacity(handles.len());
            for handle in &handles {
                match handle.state.try_write() {
                    Ok(state) => states.push(state),
                    Err(TryLockError::WouldBlock) => {
                        drop(states);
                        #[cfg(test)]
                        tests::notify_capture_wait(handle.shard_id);
                        drop(handle.state.write().expect("shard state lock poisoned"));
                        continue 'capture;
                    }
                    Err(TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
                }
            }
            let entries = self.entries.read().expect("shard registry lock poisoned");
            if !Self::same_membership(&entries, &handles) {
                continue;
            }
            let mut view = handles.iter().zip(&mut states).map(|(handle, state)| {
                (handle.shard_id, state.as_mut().expect("live shard has no state"))
            }).collect();
            return write(&mut view);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    thread_local! {
        static CAPTURE_WAIT: std::cell::RefCell<Option<mpsc::Sender<u64>>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn notify_capture_wait(shard_id: u64) {
        CAPTURE_WAIT.with(|slot| {
            if let Some(notify) = slot.borrow_mut().take() {
                let _ = notify.send(shard_id);
            }
        });
    }

    fn coherent_capture_yields_prefix_while_waiting(write: bool) {
        const DEADLINE: Duration = Duration::from_secs(5);
        let states = Arc::new(ShardStateRegistry::default());
        states.replace(1, 10_u64);
        states.replace(2, 20);
        let busy = states.handle(2).unwrap();
        let mut busy_state = busy.state.write().unwrap();

        let (waiting, wait_observed) = mpsc::channel();
        let (finished, finish_observed) = mpsc::channel();
        let capture = Arc::clone(&states);
        let capture_thread = std::thread::spawn(move || {
            CAPTURE_WAIT.with(|slot| *slot.borrow_mut() = Some(waiting));
            let mut calls = 0;
            let view = if write {
                capture.with_all_shards_mut(|all| {
                    calls += 1;
                    let view = all.iter().map(|(&id, value)| (id, **value)).collect::<Vec<_>>();
                    **all.get_mut(&1).unwrap() += 100;
                    view
                })
            } else {
                capture.with_all_shards_read(|all| {
                    calls += 1;
                    all.iter().map(|(&id, value)| (id, **value)).collect::<Vec<_>>()
                })
            };
            let _ = finished.send((calls, view));
        });

        // The event comes from WouldBlock after the prefix is dropped. Keep
        // owner 2 locked until the peer proves both mutation and registry progress.
        let waited_on = wait_observed.recv_timeout(DEADLINE);
        let (updated, update_observed) = mpsc::channel();
        let peer = Arc::clone(&states);
        let peer_thread = std::thread::spawn(move || {
            peer.with_shard_mut(1, |state| *state.unwrap() += 1);
            peer.replace(3, 30);
            let _ = updated.send(());
        });
        let progressed = update_observed.recv_timeout(DEADLINE);
        *busy_state = Some(22);
        drop(busy_state);

        // Release before asserting so a failing check cannot strand the workers
        // behind the injected contention. Wait for completion with a deadline.
        let completed = finish_observed.recv_timeout(DEADLINE);
        let peer_done = progressed.is_ok() || update_observed.recv_timeout(DEADLINE).is_ok();
        if peer_done || peer_thread.is_finished() {
            peer_thread.join().unwrap();
        }
        if completed.is_ok() || capture_thread.is_finished() {
            capture_thread.join().unwrap();
        }
        assert_eq!(waited_on.expect("capture never reached a busy shard"), 2);
        progressed.expect("peer mutation or registry insertion stalled behind the busy shard");
        assert_eq!(completed.expect("capture did not finish"), (1, vec![(1, 11), (2, 22), (3, 30)]));
        states.with_shard(1, |state| assert_eq!(state.copied(), Some(if write { 111 } else { 11 })));
    }

    #[test]
    fn coherent_read_yields_prefix_before_waiting_for_busy_shard() {
        coherent_capture_yields_prefix_while_waiting(false);
    }

    #[test]
    fn coherent_mutation_yields_prefix_before_waiting_for_busy_shard() {
        coherent_capture_yields_prefix_while_waiting(true);
    }

    #[test]
    fn held_owner_does_not_block_peer_lookup_or_mutation() {
        let states = Arc::new(ShardStateRegistry::default());
        states.replace(1, 10_u64);
        states.replace(2, 20);
        let (held, ready) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let owner = Arc::clone(&states);
        let first = std::thread::spawn(move || owner.with_shard_mut(1, |state| {
            held.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(5)).unwrap();
            *state.unwrap() += 1;
        }));
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done, completed) = mpsc::channel();
        let peer = Arc::clone(&states);
        let second = std::thread::spawn(move || {
            peer.with_shard_mut(2, |state| *state.unwrap() += 1);
            done.send(()).unwrap();
        });
        let progressed = completed.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        progressed.expect("peer blocked behind a different owner");
        states.with_all_shards_read(|all| assert_eq!(**all.get(&2).unwrap(), 21));
    }

    #[test]
    fn retired_waiter_and_cleanup_cannot_rebind_to_a_replacement() {
        let states = ShardStateRegistry::default();
        states.replace(1, 10_u64);
        let old = states.handle(1).unwrap();
        assert_eq!(states.replace(1, 20), Some(10));
        let current = states.handle(1).unwrap();
        assert!(!old.same_incarnation(&current));
        old.with_write(|state| assert!(state.is_none()));
        assert_eq!(states.retire_if_current(&old), None);
        current.with_read(|state| assert_eq!(state, Some(&20)));
        assert_eq!(states.retire_if_current(&current), Some(20));
        assert!(states.shard_ids().is_empty());
    }

    #[test]
    fn coherent_mutation_visits_owners_in_key_order() {
        let states = ShardStateRegistry::default();
        states.replace(2, 20_u64);
        states.replace(1, 10);
        states.with_all_shards_mut(|all| {
            assert_eq!(all.keys().copied().collect::<Vec<_>>(), [1, 2]);
            for value in all.values_mut() {
                **value += 1;
            }
        });
        states.with_all_shards_read(|all| assert_eq!(all.values().map(|value| **value).sum::<u64>(), 32));
    }
}
