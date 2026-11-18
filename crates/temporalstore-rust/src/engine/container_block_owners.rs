// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::ModelKey;
use std::collections::HashMap;
use std::sync::Mutex;

type PhysicalBlock = (u64, u64, u64);
const MAX_OBJECTS: usize = 4;
const MAX_BLOCKS: usize = 8192;
const MAX_CAPACITY: usize = MAX_BLOCKS * 2;

#[derive(Debug)]
pub(super) struct BlockOwners {
    counts: Option<HashMap<PhysicalBlock, usize>>,
}

impl Default for BlockOwners {
    fn default() -> Self {
        Self {
            counts: Some(HashMap::new()),
        }
    }
}

impl BlockOwners {
    pub(super) fn observe(&mut self, block: PhysicalBlock) {
        let Some(counts) = &mut self.counts else {
            return;
        };
        if counts.len() == MAX_BLOCKS && !counts.contains_key(&block) {
            self.counts = None;
            return;
        }
        let count = counts.entry(block).or_default();
        if let Some(next) = count.checked_add(1) {
            *count = next;
        } else {
            self.counts = None;
        }
    }

    fn len(&self) -> usize {
        self.counts.as_ref().map_or(0, HashMap::len)
    }

    fn capacity(&self) -> usize {
        self.counts.as_ref().map_or(0, HashMap::capacity)
    }

    fn shared(&self, block: PhysicalBlock) -> Option<bool> {
        // The caller has already located the member at this physical address.
        self.counts
            .as_ref()?
            .get(&block)
            .filter(|count| **count > 0)
            .map(|count| *count > 1)
    }

    fn replace(&mut self, previous: Option<PhysicalBlock>, next: Option<PhysicalBlock>) {
        if previous == next {
            return;
        }
        let Some(counts) = &mut self.counts else {
            return;
        };
        if let Some(previous) = previous {
            let Some(count) = counts.get_mut(&previous).filter(|count| **count > 0) else {
                self.counts = None;
                return;
            };
            *count -= 1;
            if *count == 0 {
                counts.remove(&previous);
            }
        }
        if let Some(next) = next {
            self.observe(next);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct OwnershipCache(Mutex<HashMap<ModelKey, BlockOwners>>);

impl OwnershipCache {
    pub(super) fn shared(
        &self,
        key: &ModelKey,
        block: PhysicalBlock,
        scan: impl FnOnce() -> BlockOwners,
    ) -> Option<bool> {
        let mut entries = self.0.lock().expect("block owner cache poisoned");
        if let Some(owners) = entries.get(key) {
            return owners.shared(block);
        }
        let mut owners = scan();
        if owners.capacity() > MAX_CAPACITY {
            owners.counts = None;
        }
        // Limit both distinct physical blocks and object handles, including negative entries.
        while entries.len() >= MAX_OBJECTS
            || entries.values().map(BlockOwners::len).sum::<usize>() + owners.len() > MAX_BLOCKS
            || entries.values().map(BlockOwners::capacity).sum::<usize>() + owners.capacity()
                > MAX_CAPACITY
        {
            let victim = entries.keys().next().cloned().expect("cache at capacity");
            entries.remove(&victim);
        }
        let shared = owners.shared(block);
        entries.insert(key.clone(), owners);
        shared
    }

    pub(super) fn contains(&mut self, key: &str) -> bool {
        self.0
            .get_mut()
            .expect("block owner cache poisoned")
            .contains_key(key)
    }

    pub(super) fn replace(
        &mut self,
        key: &str,
        previous: Option<PhysicalBlock>,
        next: Option<PhysicalBlock>,
    ) {
        let entries = self.0.get_mut().expect("block owner cache poisoned");
        if let Some(owners) = entries.get_mut(key) {
            owners.replace(previous, next);
        }
        if entries.values().map(BlockOwners::len).sum::<usize>() > MAX_BLOCKS
            || entries.values().map(BlockOwners::capacity).sum::<usize>() > MAX_CAPACITY
        {
            if let Some(owners) = entries.get_mut(key) {
                owners.counts = None;
            }
        }
    }

    pub(super) fn invalidate(&mut self, key: &str) {
        self.0
            .get_mut()
            .expect("block owner cache poisoned")
            .remove(key);
    }

    pub(super) fn clear(&mut self) {
        self.0
            .get_mut()
            .expect("block owner cache poisoned")
            .clear();
    }

    pub(super) fn adopt_key(&mut self, key: &ModelKey) {
        let entries = self.0.get_mut().expect("block owner cache poisoned");
        if let Some(owners) = entries.remove(key.as_ref()) {
            entries.insert(key.clone(), owners);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summarize(blocks: impl IntoIterator<Item = PhysicalBlock>) -> BlockOwners {
        let mut owners = BlockOwners::default();
        for block in blocks {
            owners.observe(block);
        }
        owners
    }

    #[test]
    fn shared_frames_are_counted_by_slab_offset_and_length() {
        let target = (3, 128, 64);
        let mut owners = summarize([target, target, (4, 128, 64), (3, 256, 64), (3, 128, 128)]);
        assert_eq!(owners.shared(target), Some(true));
        owners.replace(Some(target), Some((9, 0, 64)));
        assert_eq!(owners.shared(target), Some(false));
        owners.replace(Some(target), None);
        assert_eq!(owners.shared(target), None);
        assert_eq!(owners.shared((3, 128, 128)), Some(false));
    }

    #[test]
    fn growing_batched_container_scans_once_before_incremental_removals() {
        let mut cache = OwnershipCache::default();
        let key = ModelKey::from("global-order");
        let scans = std::cell::Cell::new(0);
        let slots = std::cell::Cell::new(0);
        for member in 0..65_536u64 {
            let block = (1, member / 64 * 1024, 1024);
            assert_eq!(
                cache.shared(&key, block, || {
                    scans.set(scans.get() + 1);
                    let owners = summarize((0..65_536u64).map(|i| (1, i / 64 * 1024, 1024)));
                    slots.set(owners.capacity());
                    owners
                }),
                Some(member % 64 != 63)
            );
            cache.replace(&key, Some(block), None);
        }
        assert_eq!(scans.get(), 1);
        println!("members=65536 physical_blocks=1024 initial_scans={} reported_capacity={} capacity_payload_bytes={}", scans.get(), slots.get(), slots.get() * std::mem::size_of::<(PhysicalBlock, usize)>());
        assert_eq!(cache.0.get_mut().unwrap()[&key].len(), 0);
    }

    #[test]
    fn replacements_match_live_counts_through_member_churn() {
        let mut cache = OwnershipCache::default();
        let key = ModelKey::from("churn");
        let mut live = HashMap::<u64, PhysicalBlock>::new();
        let mut seed = 0x13e2_552bu64;
        for round in 0..20_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let member = (seed >> 32) % 128;
            let next = (round % 3 != 0).then_some(((seed >> 16) % 4, seed % 32, 64));
            let previous = match next {
                Some(next) => live.insert(member, next),
                None => live.remove(&member),
            };
            cache.replace(&key, previous, next);
            for block in live.values().take(3) {
                assert_eq!(
                    cache.shared(&key, *block, || summarize(live.values().copied())),
                    Some(live.values().filter(|other| *other == block).count() > 1)
                );
            }
            if round % 37 == 0 {
                cache.invalidate(&key);
            }
            if round % 193 == 0 {
                cache.clear();
            }
            cache.adopt_key(&ModelKey::from("churn"));
        }
    }

    #[test]
    fn capacity_is_bounded_and_oversized_objects_keep_scan_fallback() {
        let mut cache = OwnershipCache::default();
        for index in 0..16u64 {
            let key = ModelKey::from(format!("object-{index}"));
            assert_eq!(
                cache.shared(&key, (index, 0, 1), || summarize(
                    (0..4096).map(|offset| (index, offset, 1))
                )),
                Some(false)
            );
            let entries = cache.0.lock().unwrap();
            assert!(entries.len() <= MAX_OBJECTS);
            assert!(entries.values().map(BlockOwners::len).sum::<usize>() <= MAX_BLOCKS);
            assert!(entries.values().map(BlockOwners::capacity).sum::<usize>() <= MAX_CAPACITY);
        }
        let key = ModelKey::from("oversized");
        assert_eq!(
            cache.shared(&key, (0, 0, 1), || summarize(
                (0..=MAX_BLOCKS as u64).map(|offset| (0, offset, 1))
            )),
            None
        );
        assert_eq!(
            cache.shared(&key, (0, 0, 1), || panic!(
                "negative entry must not rebuild"
            )),
            None
        );
        assert!(cache.contains(&key));
        cache.invalidate(&key);
        assert_eq!(
            cache.shared(&key, (0, 0, 1), || summarize([(0, 0, 1)])),
            Some(false)
        );
        cache.replace(&key, None, Some((0, 0, 1)));
        assert_eq!(cache.shared(&key, (0, 0, 1), || unreachable!()), Some(true));
    }

    #[test]
    fn missing_predecessor_discards_cached_proof() {
        let mut cache = OwnershipCache::default();
        let key = ModelKey::from("key");
        assert_eq!(
            cache.shared(&key, (1, 0, 1), || summarize([(1, 0, 1)])),
            Some(false)
        );
        cache.replace(&key, Some((2, 0, 1)), None);
        assert_eq!(cache.shared(&key, (1, 0, 1), || unreachable!()), None);
    }

    #[test]
    fn replacement_budget_and_count_overflow_discard_proof_without_affecting_other_objects() {
        let mut cache = OwnershipCache::default();
        for key in ["a", "b"] {
            let key = ModelKey::from(key);
            assert_eq!(
                cache.shared(&key, (1, 0, 1), || summarize(
                    (0..4096).map(|offset| (1, offset, 1))
                )),
                Some(false)
            );
        }
        cache.replace("a", None, Some((2, 0, 1)));
        assert_eq!(
            cache.shared(&ModelKey::from("a"), (1, 0, 1), || unreachable!()),
            None
        );
        assert_eq!(
            cache.shared(&ModelKey::from("b"), (1, 0, 1), || unreachable!()),
            Some(false)
        );
        let mut owners = BlockOwners {
            counts: Some(HashMap::from([((1, 0, 1), usize::MAX)])),
        };
        owners.observe((1, 0, 1));
        assert_eq!(owners.shared((1, 0, 1)), None);
    }
}
