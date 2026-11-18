// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};
use std::collections::{btree_set, BTreeSet};

const TREE_THRESHOLD: usize = 256;
const TREE_DEMOTE: usize = TREE_THRESHOLD / 2;

#[derive(Debug, Clone, Eq, Deserialize)]
#[serde(from = "Vec<BlockLookupRef>")]
pub(crate) enum BlockRefs {
    One(BlockLookupRef),
    Many(Vec<BlockLookupRef>),
    Large(Box<BTreeSet<BlockLookupRef>>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct BlockLookupRef {
    #[serde(rename = "routing_slot")]
    pub(crate) routing_bucket: u32,
    pub(crate) block_ref_key: u64,
}

#[derive(Clone)]
pub(crate) enum BlockRefsIter<'a> {
    Slice(std::slice::Iter<'a, BlockLookupRef>),
    Tree(btree_set::Iter<'a, BlockLookupRef>),
}

impl<'a> Iterator for BlockRefsIter<'a> {
    type Item = &'a BlockLookupRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Slice(iter) => iter.next(),
            Self::Tree(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for BlockRefsIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Slice(iter) => iter.next_back(),
            Self::Tree(iter) => iter.next_back(),
        }
    }
}

impl ExactSizeIterator for BlockRefsIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Slice(iter) => iter.len(),
            Self::Tree(iter) => iter.len(),
        }
    }
}

impl std::iter::FusedIterator for BlockRefsIter<'_> {}

impl<'a> IntoIterator for &'a BlockRefs {
    type Item = &'a BlockLookupRef;
    type IntoIter = BlockRefsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
thread_local! {
    static BLOCK_REF_VECTOR_SHIFTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn measure_block_ref_vector_shifts_for_test<T>(work: impl FnOnce() -> T) -> (T, usize) {
    BLOCK_REF_VECTOR_SHIFTS.with(|count| count.set(0));
    let result = work();
    (result, BLOCK_REF_VECTOR_SHIFTS.with(std::cell::Cell::get))
}

impl BlockRefs {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(values) => values.len(),
            Self::Large(values) => values.len(),
        }
    }

    pub(crate) fn iter(&self) -> BlockRefsIter<'_> {
        match self {
            Self::One(value) => BlockRefsIter::Slice(std::slice::from_ref(value).iter()),
            Self::Many(values) => BlockRefsIter::Slice(values.iter()),
            Self::Large(values) => BlockRefsIter::Tree(values.iter()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains(&self, value: &BlockLookupRef) -> bool {
        match self {
            Self::One(existing) => existing == value,
            Self::Many(values) => values.binary_search(value).is_ok(),
            Self::Large(values) => values.contains(value),
        }
    }

    pub(crate) fn remove(&mut self, value: &BlockLookupRef) -> bool {
        match self {
            Self::One(existing) => {
                if existing != value {
                    return false;
                }
                *self = Self::Many(Vec::new());
                true
            }
            Self::Many(values) => match values.binary_search(value) {
                Ok(at) => {
                    #[cfg(test)]
                    BLOCK_REF_VECTOR_SHIFTS
                        .with(|count| count.set(count.get() + values.len() - at - 1));
                    values.remove(at);
                    true
                }
                Err(_) => false,
            },
            Self::Large(values) => {
                let removed = values.remove(value);
                if removed && values.len() <= TREE_DEMOTE {
                    *self = Self::Many(values.iter().cloned().collect());
                }
                removed
            }
        }
    }

    pub(crate) fn insert(&mut self, value: BlockLookupRef) -> bool {
        match self {
            Self::One(existing) => match value.cmp(existing) {
                std::cmp::Ordering::Equal => false,
                std::cmp::Ordering::Less => {
                    *self = Self::Many(vec![value, existing.clone()]);
                    true
                }
                std::cmp::Ordering::Greater => {
                    *self = Self::Many(vec![existing.clone(), value]);
                    true
                }
            },
            Self::Many(values) => match values.binary_search(&value) {
                Ok(_) => false,
                Err(at) => {
                    #[cfg(test)]
                    BLOCK_REF_VECTOR_SHIFTS
                        .with(|count| count.set(count.get() + values.len() - at));
                    values.insert(at, value);
                    if values.len() == TREE_THRESHOLD + 1 && strictly_sorted(values) {
                        *self = Self::Large(Box::new(std::mem::take(values).into_iter().collect()));
                    }
                    true
                }
            },
            Self::Large(values) => values.insert(value),
        }
    }
}

fn strictly_sorted(values: &[BlockLookupRef]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

impl Default for BlockRefs {
    fn default() -> Self {
        Self::Many(Vec::new())
    }
}

impl Serialize for BlockRefs {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;
        for value in self {
            sequence.serialize_element(value)?;
        }
        sequence.end()
    }
}

impl PartialEq for BlockRefs {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl From<Vec<BlockLookupRef>> for BlockRefs {
    fn from(mut values: Vec<BlockLookupRef>) -> Self {
        if values.len() == 1 {
            Self::One(values.pop().expect("length just checked"))
        } else if values.len() > TREE_THRESHOLD && strictly_sorted(&values) {
            Self::Large(Box::new(values.into_iter().collect()))
        } else {
            // Preserve old noncanonical sequences rather than silently sort or discard duplicates.
            Self::Many(values)
        }
    }
}

impl From<BlockRefs> for Vec<BlockLookupRef> {
    fn from(values: BlockRefs) -> Self {
        match values {
            BlockRefs::One(value) => vec![value],
            BlockRefs::Many(values) => values,
            BlockRefs::Large(values) => (*values).into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: u64) -> BlockLookupRef {
        BlockLookupRef {
            routing_bucket: (key % 7) as u32,
            block_ref_key: key,
        }
    }

    #[test]
    fn inline_width_and_small_spill_stay_unchanged() {
        assert_eq!(std::mem::size_of::<BlockRefs>(), 24);
        assert_eq!(std::mem::align_of::<BlockRefs>(), 8);
        let mut refs = BlockRefs::One(row(1));
        assert!(refs.insert(row(2)));
        assert!(matches!(refs, BlockRefs::Many(_)));
        assert!(!refs.insert(row(2)));
        assert!(refs.remove(&row(1)));
        assert_eq!(refs, BlockRefs::One(row(2)));
        assert!(refs.remove(&row(2)));
        assert!(refs.is_empty());
    }

    #[test]
    fn large_insert_and_remove_do_not_shift_the_reference_vector() {
        let mut refs = BlockRefs::default();
        for key in 0..1024 {
            refs.insert(row(key));
        }
        assert!(matches!(refs, BlockRefs::Large(_)));
        let (_, shifts) = measure_block_ref_vector_shifts_for_test(|| {
            for key in 1024..65_536 {
                assert!(refs.insert(row(key)));
            }
            for key in 0..32_768 {
                assert!(refs.remove(&row(key)));
            }
        });
        assert_eq!(shifts, 0);
        assert_eq!(refs.len(), 32_768);
        assert!(refs.iter().zip(refs.iter().skip(1)).all(|(a, b)| a < b));
    }

    #[test]
    fn transitions_match_an_ordered_set_through_churn_and_demotion() {
        let mut refs = BlockRefs::default();
        let mut expected = BTreeSet::new();
        let mut seed = 0x134d_7fabu64;
        for _ in 0..20_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = row((seed >> 16) % 1024);
            if seed & 4 == 0 {
                assert_eq!(refs.remove(&value), expected.remove(&value));
            } else {
                assert_eq!(refs.insert(value.clone()), expected.insert(value));
            }
            assert_eq!(refs.len(), expected.len());
            assert_eq!(refs.contains(&row(7)), expected.contains(&row(7)));
            assert!(refs.iter().eq(expected.iter()));
        }
        for value in expected.clone() {
            assert!(refs.remove(&value));
        }
        assert!(matches!(refs, BlockRefs::Many(_)));
        assert!(refs.is_empty());
    }

    #[test]
    fn borrowed_iterators_keep_order_length_and_double_ended_progress() {
        for size in [0, 1, 2, 256, 257, 8192] {
            let expected: Vec<_> = (0..size)
                .map(row)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let refs = BlockRefs::from(expected.clone());
            let mut iter = refs.iter();
            assert_eq!(iter.size_hint(), (size as usize, Some(size as usize)));
            if size > 1 {
                assert_eq!(iter.next(), expected.first());
                assert_eq!(iter.next_back(), expected.last());
                assert_eq!(iter.len(), size as usize - 2);
            }
            assert!(refs.iter().eq(expected.iter()));
            assert!(refs.iter().rev().eq(expected.iter().rev()));
        }
    }

    #[test]
    fn json_and_binary_sequences_survive_promotion_clone_and_reload() {
        for size in [0, 1, 2, 256, 257, 4096] {
            let expected: Vec<_> = (0..size)
                .map(row)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let refs = BlockRefs::from(expected.clone());
            let json = serde_json::to_vec(&expected).unwrap();
            assert_eq!(serde_json::to_vec(&refs).unwrap(), json);
            let binary = rmp_serde::to_vec_named(&expected).unwrap();
            assert_eq!(rmp_serde::to_vec_named(&refs).unwrap(), binary);
            let loaded: BlockRefs = rmp_serde::from_slice(&binary).unwrap();
            assert_eq!(loaded, refs);
            assert_eq!(Vec::from(loaded.clone()), expected);
            assert_eq!(serde_json::from_slice::<BlockRefs>(&json).unwrap(), loaded);
        }
    }

    #[test]
    fn legacy_noncanonical_sequences_are_not_silently_sorted_or_deduplicated() {
        let values: Vec<_> = (0..1024).rev().map(|key| row(key % 512)).collect();
        let encoded = rmp_serde::to_vec_named(&values).unwrap();
        let refs: BlockRefs = rmp_serde::from_slice(&encoded).unwrap();
        assert!(matches!(refs, BlockRefs::Many(_)));
        assert_eq!(rmp_serde::to_vec_named(&refs).unwrap(), encoded);
        assert_eq!(Vec::from(refs), values);
    }
}
