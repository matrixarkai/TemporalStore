// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use super::super::point_readers::Stamp;

const MAX_ENTRIES: usize = 4096;
const MAX_BYTES: usize = 1024 * 1024;
type Key = (Stamp, u64);
type Layout = (u64, u64);

/// Validated transport geometry only, scoped to one WAL store. File versions include
/// device, inode, size and nanosecond modification/change times. Unstamped files bypass it.
#[derive(Debug, Default)]
pub(crate) struct Cache {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<Key, Layout, ahash::RandomState>,
    oldest: VecDeque<Key>,
}

impl State {
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.entries.capacity() * (std::mem::size_of::<(Key, Layout)>() + 16)
            + self.oldest.capacity() * std::mem::size_of::<Key>()
    }
}

impl Cache {
    pub(super) fn contains(&self, version: Stamp, offset: u64, length: u64, first: u64) -> bool {
        self.state.lock().expect("WAL transport layout cache poisoned")
            .entries.get(&(version, offset)) == Some(&(length, first))
    }

    pub(super) fn insert(&self, version: Stamp, offset: u64, length: u64, first: u64) {
        let key = (version, offset);
        let mut state = self.state.lock().expect("WAL transport layout cache poisoned");
        if state.entries.contains_key(&key) {
            return;
        }
        if state.entries.len() == MAX_ENTRIES {
            if let Some(oldest) = state.oldest.pop_front() {
                state.entries.remove(&oldest);
            }
        }
        state.entries.reserve(1);
        state.oldest.reserve(1);
        // Capacity, including conservative hash-table control overhead, is bounded too.
        if state.weight() > MAX_BYTES {
            *state = State::default();
            return;
        }
        state.entries.insert(key, (length, first));
        state.oldest.push_back(key);
    }
}

#[cfg(test)]
#[path = "point_layout_tests.rs"]
mod tests;
