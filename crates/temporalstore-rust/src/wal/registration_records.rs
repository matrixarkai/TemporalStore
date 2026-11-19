// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
struct Record {
    live: usize,
    expected: usize,
    matching: usize,
    others: Option<Box<BTreeMap<usize, usize>>>,
}

impl Record {
    fn incomplete(&self) -> bool {
        self.live != 0
            && (self.others.is_some() || self.expected == 0 || self.expected != self.live)
    }

    fn add_expected(&mut self, expected: usize) {
        if self.matching == 0 {
            self.expected = expected;
            self.matching = 1;
        } else if self.expected == expected {
            self.matching = self
                .matching
                .checked_add(1)
                .expect("registration expectation count exhausted");
        } else {
            let count = self
                .others
                .get_or_insert_with(Box::default)
                .entry(expected)
                .or_default();
            *count = count
                .checked_add(1)
                .expect("registration expectation count exhausted");
        }
    }

    fn remove_expected(&mut self, expected: usize) {
        if expected == self.expected {
            self.matching -= 1;
            if self.matching == 0 {
                if let Some(others) = self.others.as_mut() {
                    let (expected, count) =
                        others.pop_first().expect("conflicting expectation exists");
                    self.expected = expected;
                    self.matching = count;
                }
            }
        } else {
            let others = self
                .others
                .as_mut()
                .expect("conflicting expectation exists");
            let count = others
                .get_mut(&expected)
                .expect("registered expectation exists");
            *count -= 1;
            if *count == 0 {
                others.remove(&expected);
            }
        }
        if self.others.as_ref().is_some_and(|others| others.is_empty()) {
            self.others = None;
        }
    }
}

#[derive(Debug, Default)]
struct Scope {
    records: BTreeMap<u64, Record>,
    incomplete: usize,
}

impl Scope {
    fn add(&mut self, sequence: u64, expected: usize) {
        let record = self.records.entry(sequence).or_default();
        self.incomplete -= usize::from(record.incomplete());
        record.live = record
            .live
            .checked_add(1)
            .expect("registration count exhausted");
        record.add_expected(expected);
        self.incomplete += usize::from(record.incomplete());
    }

    fn remove(&mut self, sequence: u64, expected: usize) {
        let record = self
            .records
            .get_mut(&sequence)
            .expect("registered sequence exists");
        self.incomplete -= usize::from(record.incomplete());
        record.remove_expected(expected);
        record.live -= 1;
        self.incomplete += usize::from(record.incomplete());
        if record.live == 0 {
            self.records.remove(&sequence);
        }
    }

    fn has_holes(&self) -> bool {
        if self.incomplete != 0 {
            return true;
        }
        let (Some((first, _)), Some((last, _))) = (
            self.records.first_key_value(),
            self.records.last_key_value(),
        ) else {
            return false;
        };
        (self.records.len() as u64) < last.saturating_sub(*first).saturating_add(1)
    }
}

/// Counts registered slots, not temporary Arc clones or in-flight readers.
#[derive(Debug, Default)]
pub(super) struct RegistrationRecords {
    scopes: HashMap<(usize, ShardId), Scope>,
}

impl RegistrationRecords {
    fn add(&mut self, owner: usize, shard: ShardId, sequence: u64, expected: usize) {
        self.scopes
            .entry((owner, shard))
            .or_default()
            .add(sequence, expected);
    }

    fn remove(&mut self, owner: usize, shard: ShardId, sequence: u64, expected: usize) {
        let scope = self
            .scopes
            .get_mut(&(owner, shard))
            .expect("registration scope exists");
        scope.remove(sequence, expected);
        if scope.records.is_empty() {
            self.scopes.remove(&(owner, shard));
        }
    }
}

impl LocalWriteAheadLogStore {
    pub(crate) fn replace_block_location(
        &self,
        owner: usize,
        shard: ShardId,
        sequence: u64,
        expected: usize,
        previous: Option<(&LocalWriteAheadLogStore, u64, usize)>,
    ) {
        let same = previous.is_some_and(|(store, _, _)| self.same_log(store));
        {
            let mut retention = self
                .read_retention
                .lock()
                .expect("registration records lock poisoned");
            let records = &mut retention.registrations;
            records.add(owner, shard, sequence, expected);
            if let Some((_, sequence, expected)) = previous.filter(|_| same) {
                records.remove(owner, shard, sequence, expected);
            }
        }
        // Publish the new owner's floor before retiring the old owner's claim.
        if let Some((store, sequence, expected)) = previous.filter(|_| !same) {
            store.remove_block_location(owner, shard, sequence, expected);
        }
    }

    pub(crate) fn remove_block_location(
        &self,
        owner: usize,
        shard: ShardId,
        sequence: u64,
        expected: usize,
    ) {
        self.read_retention
            .lock()
            .expect("registration records lock poisoned")
            .registrations
            .remove(owner, shard, sequence, expected);
    }

    pub(crate) fn registered_block_floor(&self, owner: usize, shard: ShardId) -> Option<u64> {
        self.read_retention
            .lock()
            .expect("registration records lock poisoned")
            .registrations
            .scopes
            .get(&(owner, shard))?
            .records
            .first_key_value()
            .map(|(sequence, _)| *sequence)
    }

    pub(crate) fn registered_block_span_has_holes(&self, owner: usize, shard: ShardId) -> bool {
        self.read_retention
            .lock()
            .expect("registration records lock poisoned")
            .registrations
            .scopes
            .get(&(owner, shard))
            .is_some_and(Scope::has_holes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_match_an_independent_slot_model() {
        let mut index = RegistrationRecords::default();
        let mut slots = vec![None; 128];
        let mut random = 173u64;
        for _ in 0..20_000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let slot = (random >> 16) as usize % slots.len();
            if let Some((owner, shard, sequence, expected)) = slots[slot].take() {
                index.remove(owner, shard, sequence, expected);
            }
            if random & 7 != 0 {
                let value = (
                    1 + (random >> 10) as usize % 2,
                    1 + (random >> 12) % 2,
                    (random >> 20) % 32,
                    (random >> 30) as usize % 8,
                );
                index.add(value.0, value.1, value.2, value.3);
                slots[slot] = Some(value);
            }
            for owner in 1..=2 {
                for shard in 1..=2 {
                    let mut records = BTreeMap::<u64, Vec<usize>>::new();
                    for &(held, partition, sequence, expected) in slots.iter().flatten() {
                        if held == owner && partition == shard {
                            records.entry(sequence).or_default().push(expected);
                        }
                    }
                    let wanted_floor = records.first_key_value().map(|(sequence, _)| *sequence);
                    let wanted_holes = records.values().any(|expected| {
                        expected[0] == 0
                            || expected.len() != expected[0]
                            || expected.iter().any(|value| *value != expected[0])
                    }) || records
                        .first_key_value()
                        .zip(records.last_key_value())
                        .is_some_and(|((first, _), (last, _))| {
                            (records.len() as u64) < last.saturating_sub(*first).saturating_add(1)
                        });
                    let scope = index.scopes.get(&(owner, shard));
                    assert_eq!(
                        scope.and_then(|scope| scope
                            .records
                            .first_key_value()
                            .map(|(sequence, _)| *sequence)),
                        wanted_floor
                    );
                    assert_eq!(scope.is_some_and(Scope::has_holes), wanted_holes);
                }
            }
        }
        for (owner, shard, sequence, expected) in slots.into_iter().flatten() {
            index.remove(owner, shard, sequence, expected);
        }
        assert!(index.scopes.is_empty());
    }

    #[test]
    fn uniform_expectations_stay_inline_and_conflicts_release_their_storage() {
        assert!(std::mem::size_of::<Record>() <= 4 * std::mem::size_of::<usize>());
        let mut scope = Scope::default();
        for _ in 0..500 {
            scope.add(7, 500);
        }
        assert!(!scope.has_holes());
        assert!(scope.records[&7].others.is_none());
        scope.add(7, 0);
        assert!(scope.has_holes());
        scope.remove(7, 0);
        assert!(!scope.has_holes());
        assert!(scope.records[&7].others.is_none());
        scope.add(9, 1);
        assert!(scope.has_holes());
        scope.add(8, 1);
        assert!(!scope.has_holes());
    }
}
