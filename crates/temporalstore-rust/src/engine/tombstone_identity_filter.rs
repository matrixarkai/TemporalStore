// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::TombstoneElement;
use crate::engine::storage_bucket_internals::StoredModelKind;
use std::hash::{Hash, Hasher};

const MIN_FILTER_ROWS: usize = 64;
const FILTER_SEED: &[u8; 32] = b"TemporalStoreTombstoneFilterSeed";

#[derive(Clone)]
pub(super) struct TombstoneRows {
    rows: Vec<TombstoneElement>,
    filter: Option<Box<IdentityFilter>>,
}

#[derive(Clone)]
struct IdentityFilter {
    bloom: bloomfilter::Bloom<u64>,
    capacity: usize,
}

impl std::fmt::Debug for TombstoneRows {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.rows.fmt(formatter)
    }
}

impl PartialEq for TombstoneRows {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}
impl Eq for TombstoneRows {}

fn identity_hash(kind: StoredModelKind, object: &str, element: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (kind, object, element).hash(&mut hasher);
    hasher.finish()
}

impl TombstoneRows {
    pub(super) fn new(rows: Vec<TombstoneElement>) -> Self {
        let mut result = Self { rows, filter: None };
        result.rebuild_filter();
        result
    }

    fn rebuild_filter(&mut self) {
        self.filter = None;
        if self.rows.len() < MIN_FILTER_ROWS {
            return;
        }
        let capacity = self
            .rows
            .len()
            .checked_next_power_of_two()
            .unwrap_or(self.rows.len());
        let Ok(mut bloom) = bloomfilter::Bloom::new_for_fp_rate_with_seed(capacity, 0.01, FILTER_SEED)
        else {
            return;
        };
        for row in &self.rows {
            bloom.set(&identity_hash(
                row.model_id,
                &row.object_key,
                &row.element_name,
            ));
        }
        self.filter = Some(Box::new(IdentityFilter { bloom, capacity }));
    }

    pub(super) fn may_contain(&self, kind: StoredModelKind, object: &str, element: &str) -> bool {
        self.filter
            .as_ref()
            .is_none_or(|filter| filter.bloom.check(&identity_hash(kind, object, element)))
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &TombstoneElement> {
        self.rows.iter()
    }

    fn find_index(&self, kind: StoredModelKind, object: &str, element: &str) -> Option<usize> {
        if !self.may_contain(kind, object, element) {
            return None;
        }
        self.rows.iter().position(|row| {
            #[cfg(test)]
            VISITED.with(|count| count.set(count.get() + 1));
            row.model_id == kind
                && row.object_key.as_ref() == object
                && row.element_name.as_ref() == element
        })
    }

    pub(super) fn find(
        &self,
        kind: StoredModelKind,
        object: &str,
        element: &str,
    ) -> Option<&TombstoneElement> {
        self.find_index(kind, object, element)
            .map(|index| &self.rows[index])
    }

    pub(super) fn update_block(
        &mut self,
        kind: StoredModelKind,
        object: &str,
        element: &str,
        block: (u64, u64, u64),
    ) -> bool {
        let Some(index) = self.find_index(kind, object, element) else {
            return false;
        };
        self.rows[index].block = block;
        true
    }

    pub(super) fn push(&mut self, row: TombstoneElement) {
        if let Some(filter) = &mut self.filter {
            filter.bloom.set(&identity_hash(
                row.model_id,
                &row.object_key,
                &row.element_name,
            ));
        }
        self.rows.push(row);
        if self.rows.len() >= MIN_FILTER_ROWS
            && self
                .filter
                .as_ref()
                .is_none_or(|filter| self.rows.len() > filter.capacity)
        {
            self.rebuild_filter();
        }
    }

    pub(super) fn retain(&mut self, keep: impl FnMut(&TombstoneElement) -> bool) {
        let mut keep = keep;
        self.rows.retain(|row| {
            #[cfg(test)]
            VISITED.with(|count| count.set(count.get() + 1));
            keep(row)
        });
        if self.rows.len() < MIN_FILTER_ROWS {
            self.filter = None;
        } else if self
            .filter
            .as_ref()
            .is_some_and(|filter| self.rows.len() < filter.capacity / 4)
        {
            self.rebuild_filter();
        }
    }
}

#[cfg(test)]
thread_local! {
    static VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn row(index: usize) -> TombstoneElement {
        TombstoneElement {
            object_key: Arc::from("memory-order"),
            model_id: StoredModelKind::Set,
            element_name: Arc::from(format!("member-{index:08}")),
            block: (1, index as u64, 16),
        }
    }

    #[test]
    fn filtered_and_scalar_identity_lookups_agree_and_wire_bytes_do_not_change() {
        for count in [0, 1, 63, 64, 65, 128, 4096] {
            let mut source: Vec<_> = (0..count).map(row).collect();
            if count > 0 {
                let mut duplicate = source[0].clone();
                duplicate.block = (2, 17, 32);
                source.push(duplicate);
            }
            let filtered = TombstoneRows::new(source.clone());
            for candidate in source.iter().chain([row(count + 1)].iter()) {
                let scalar = source.iter().find(|row| {
                    row.model_id == candidate.model_id
                        && row.object_key == candidate.object_key
                        && row.element_name == candidate.element_name
                });
                assert_eq!(
                    filtered.find(
                        candidate.model_id,
                        &candidate.object_key,
                        &candidate.element_name
                    ),
                    scalar
                );
            }
            assert!(filtered
                .find(StoredModelKind::Hash, "memory-order", "member-00000000")
                .is_none());
            assert!(filtered
                .find(StoredModelKind::Set, "other-object", "member-00000000")
                .is_none());
            let wrapper = super::super::TombstoneElements::from_rows(source.clone());
            let bytes = serde_json::to_vec(&wrapper).unwrap();
            assert_eq!(bytes, serde_json::to_vec(&source).unwrap());
            let decoded: super::super::TombstoneElements = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wrapper, decoded);
            assert_eq!(wrapper, wrapper.clone());
        }
    }

    #[test]
    fn growth_retain_and_block_updates_never_hide_a_retained_identity() {
        let mut filtered = TombstoneRows::new(Vec::new());
        for index in 0..1024 {
            filtered.push(row(index));
            assert!(filtered
                .find(
                    StoredModelKind::Set,
                    "memory-order",
                    &row(index).element_name
                )
                .is_some());
        }
        filtered.retain(|row| row.block.1 % 3 == 0);
        for candidate in &filtered.rows {
            assert_eq!(
                filtered.find(
                    candidate.model_id,
                    &candidate.object_key,
                    &candidate.element_name
                ),
                Some(candidate)
            );
        }
        assert!(filtered.update_block(
            StoredModelKind::Set,
            "memory-order",
            "member-00000000",
            (3, 99, 16)
        ));
        assert_eq!(
            filtered
                .find(StoredModelKind::Set, "memory-order", "member-00000000")
                .unwrap()
                .block,
            (3, 99, 16)
        );
        filtered.retain(|row| row.element_name.as_ref() == "member-00000000");
        assert!(filtered.filter.is_none());
        assert!(filtered
            .find(StoredModelKind::Set, "memory-order", "member-00000000")
            .is_some());
        filtered.retain(|_| false);
        assert!(filtered.is_empty());
        assert!(filtered.filter.is_none());
    }

    #[test]
    fn positive_filter_results_still_require_exact_identity_matching() {
        let mut filtered = TombstoneRows::new((0..256).map(row).collect());
        let original = filtered.rows.clone();
        filtered.filter.as_mut().unwrap().bloom.set(&identity_hash(
            StoredModelKind::Set,
            "memory-order",
            "absent",
        ));
        assert!(filtered.may_contain(StoredModelKind::Set, "memory-order", "absent"));
        VISITED.with(|count| count.set(0));
        assert!(filtered
            .find(StoredModelKind::Set, "memory-order", "absent")
            .is_none());
        assert_eq!(VISITED.with(|count| count.get()), 256);
        assert!(!filtered.update_block(StoredModelKind::Set, "memory-order", "absent", (3, 1, 1)));
        assert_eq!(filtered.rows, original);
    }

    #[test]
    fn absent_bucket_operations_avoid_full_history_walks_with_compact_filters() {
        assert_eq!(
            std::mem::size_of::<super::super::TombstoneElements>(),
            std::mem::size_of::<usize>()
        );
        for count in [128, 4096, 32768] {
            let mut bucket = super::super::BucketNode::default();
            bucket.tombstone_elements =
                super::super::TombstoneElements::from_rows((0..count).map(row).collect());
            let filter_bytes = bucket
                .tombstone_elements
                .0
                .as_ref()
                .unwrap()
                .filter
                .as_ref()
                .unwrap()
                .bloom
                .len()
                / 8;
            assert!(filter_bytes <= (count * 4) as u64);
            VISITED.with(|count| count.set(0));
            for index in 0..256 {
                let name = format!("absent-{index:08}");
                assert!(bucket
                    .tombstone_element_block("memory-order", StoredModelKind::Set, &name)
                    .is_none());
                bucket.forget_tombstone_element("memory-order", StoredModelKind::Set, &name);
            }
            let visited = VISITED.with(|count| count.get());
            let scalar = count * 256 * 2;
            assert!(
                visited < scalar / 20,
                "rows={count} visited={visited} scalar={scalar}"
            );
            assert_eq!(bucket.tombstone_elements.len(), count);
            println!("rows={count} identity_visits={visited} scalar_visits={scalar} filter_bytes={filter_bytes}");
        }
    }
}
