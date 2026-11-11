// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

impl ShardState {
    pub(super) fn note_wal_application(&mut self, first: u64, last: u64) {
        let next = self.applied_wal_sequence.unwrap_or(0).checked_add(1);
        if next != Some(first) || last < first {
            self.wal_apply_discontinuous = true;
        }
        self.applied_wal_sequence = Some(last);
    }

    pub(super) fn checkpoint_wal_sequence(&self) -> Option<u64> {
        (!self.wal_apply_discontinuous).then_some(self.applied_wal_sequence.unwrap_or(0))
    }
}

#[cfg(test)]
#[path = "wal_application_tests.rs"]
mod tests;
