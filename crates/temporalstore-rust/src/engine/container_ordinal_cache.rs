// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::ModelKey;
use std::collections::HashMap;
use std::sync::Mutex;

const MAX_CACHED_OBJECTS: usize = 64;

#[derive(Clone, Copy, Debug)]
pub(super) struct OrdinalSummary {
    highest: Option<(u64, usize)>,
    second: Option<(u64, usize)>,
    second_known: bool,
}

impl Default for OrdinalSummary {
    fn default() -> Self {
        Self {
            highest: None,
            second: None,
            second_known: true,
        }
    }
}

impl OrdinalSummary {
    pub(super) fn highest(&self) -> Option<u64> {
        self.highest.map(|(id, _)| id)
    }

    pub(super) fn insert(&mut self, id: u64) {
        match &mut self.highest {
            None => self.highest = Some((id, 1)),
            Some((highest, count)) if id == *highest => *count += 1,
            Some((highest, _)) if id > *highest => {
                self.second = self.highest;
                self.highest = Some((id, 1));
                self.second_known = true;
            }
            _ if self.second_known => match &mut self.second {
                Some((second, count)) if id == *second => *count += 1,
                Some((second, _)) if id < *second => {}
                _ => self.second = Some((id, 1)),
            },
            _ => {}
        }
    }

    // A missing next maximum invalidates the summary rather than guessing a freed position.
    fn remove(&mut self, id: u64) -> bool {
        if let Some((highest, count)) = &mut self.highest {
            if id == *highest {
                if *count > 1 {
                    *count -= 1;
                } else if self.second_known {
                    self.highest = self.second.take();
                    self.second_known = self.highest.is_none();
                } else {
                    return false;
                }
                return true;
            }
        }
        if let Some((second, count)) = &mut self.second {
            if id == *second {
                if *count > 1 {
                    *count -= 1;
                } else {
                    self.second = None;
                    self.second_known = false;
                }
            }
        }
        true
    }
}

#[derive(Debug, Default)]
pub(super) struct OrdinalCache(Mutex<HashMap<ModelKey, OrdinalSummary>>);

impl OrdinalCache {
    pub(super) fn highest(
        &self,
        key: &ModelKey,
        scan: impl FnOnce() -> OrdinalSummary,
    ) -> Option<u64> {
        if let Some(summary) = self
            .0
            .lock()
            .expect("ordinal cache poisoned")
            .get(key)
            .copied()
        {
            return summary.highest();
        }
        let summary = scan();
        let mut entries = self.0.lock().expect("ordinal cache poisoned");
        if entries.len() >= MAX_CACHED_OBJECTS && !entries.contains_key(key) {
            let victim = entries.keys().next().cloned().expect("cache at capacity");
            entries.remove(&victim);
        }
        entries.insert(key.clone(), summary);
        summary.highest()
    }

    pub(super) fn contains(&mut self, key: &str) -> bool {
        self.0
            .get_mut()
            .expect("ordinal cache poisoned")
            .contains_key(key)
    }

    pub(super) fn replace(&mut self, key: &str, previous: Option<u64>, next: Option<u64>) {
        if previous == next {
            return;
        }
        let entries = self.0.get_mut().expect("ordinal cache poisoned");
        if let Some(summary) = entries.get_mut(key) {
            if previous.is_some_and(|id| !summary.remove(id)) {
                entries.remove(key);
                return;
            }
            if let Some(id) = next {
                summary.insert(id);
            }
        }
    }

    pub(super) fn invalidate(&mut self, key: &str) {
        self.0
            .get_mut()
            .expect("ordinal cache poisoned")
            .remove(key);
    }

    pub(super) fn clear(&mut self) {
        self.0.get_mut().expect("ordinal cache poisoned").clear();
    }

    pub(super) fn adopt_key(&mut self, key: &ModelKey) {
        let entries = self.0.get_mut().expect("ordinal cache poisoned");
        if let Some(summary) = entries.remove(key.as_ref()) {
            entries.insert(key.clone(), summary);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summarize(ids: &[u64]) -> OrdinalSummary {
        let mut summary = OrdinalSummary::default();
        for id in ids {
            summary.insert(*id);
        }
        summary
    }

    #[test]
    fn summaries_match_live_maxima_through_duplicate_and_top_removals() {
        let mut live = vec![0, 3, 3, 7];
        let mut summary = summarize(&live);
        let mut seed = 0x1234_5678u64;
        for step in 0..50_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            if !live.is_empty() && (seed & 1) == 0 {
                let index = seed as usize % live.len();
                let removed = live.swap_remove(index);
                if !summary.remove(removed) {
                    summary = summarize(&live);
                }
            } else {
                let id = (seed >> 8) % 128;
                live.push(id);
                summary.insert(id);
            }
            assert_eq!(summary.highest(), live.iter().copied().max(), "step={step}");
        }
    }

    #[test]
    fn highest_replacement_reuses_runner_up_but_repeated_deletes_rescan() {
        let mut summary = summarize(&[1, 2, 2, 3]);
        for _ in 0..1000 {
            assert!(summary.remove(3));
            assert_eq!(summary.highest(), Some(2));
            summary.insert(3);
        }
        assert!(summary.remove(3));
        assert!(summary.remove(2));
        assert!(!summary.remove(2));
    }

    #[test]
    fn cache_is_bounded_and_cold_evictions_recompute_exactly() {
        let mut cache = OrdinalCache::default();
        for round in 0..3 {
            for index in 0..256 {
                let key: ModelKey = std::sync::Arc::from(format!("object-{index}"));
                assert_eq!(cache.highest(&key, || summarize(&[index])), Some(index));
                assert!(cache.0.lock().unwrap().len() <= MAX_CACHED_OBJECTS);
            }
            cache.invalidate("object-255");
            assert_eq!(
                cache.highest(&std::sync::Arc::from("object-255"), || summarize(&[round])),
                Some(round)
            );
        }
        cache.clear();
        assert!(cache.0.lock().unwrap().is_empty());
    }
}
