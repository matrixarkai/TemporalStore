// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Which objects hold a block in each bucket.
//!
//! A block sits in a bucket's block index; the object it belongs to is named by the refs the
//! OBJECT holds. Going from a bucket to the owners of its blocks therefore had no index: every
//! caller that asked (a delete settling its bucket, an eviction releasing one) visited every object
//! of every kind in the shard and kept the ones with a ref in the bucket. That made the cost of one
//! delete proportional to the shard, not to the bucket.
//!
//! This holds the other direction: per bucket, the objects (kind and key) that have a ref filed
//! in it, so naming a bucket's blocks visits the bucket's own objects.
//!
//! DERIVED AND NOT PERSISTED. It is rebuilt from the objects' refs when a shard is installed and kept
//! current by the one door that files a ref. It is OPTIONAL: while it is absent every caller falls
//! back to the whole-shard walk, so a state that never had it built stays correct.
//!
//! A STALE EXTRA ENTRY IS HARMLESS AND A MISSING ONE IS NOT. An entry whose object no longer holds a
//! ref in the bucket names nothing when the bucket is inverted; an object that holds a ref in the
//! bucket and is absent here would have its block go unnamed. So adding is exact, and removing is
//! done where an object leaves a bucket but is never relied on for correctness.
//!
//! TWO PARALLEL VECTORS PER BUCKET, NOT A SET OF PAIRS. An owner is a one-byte kind and a shared
//! key pointer. A pair is 24 bytes with its padding and a set adds a control byte and load-factor
//! slack on top; two vectors are 17 bytes an entry. A bucket holds a few hundred objects at most, so
//! the scan that removes a key is short.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::storage_bucket_internals::StoredModelKind;

/// The owners of one bucket: `kinds[i]` and `keys[i]` are one object.
#[derive(Debug, Default)]
pub(super) struct BucketList {
    kinds: Vec<StoredModelKind>,
    keys: Vec<Arc<str>>,
}

impl BucketList {
    pub(super) fn iter(&self) -> impl Iterator<Item = (StoredModelKind, &Arc<str>)> {
        self.kinds.iter().copied().zip(self.keys.iter())
    }

    #[cfg(test)]
    pub(super) fn contains(&self, kind: StoredModelKind, key: &str) -> bool {
        self.iter().any(|(k, owner)| k == kind && &**owner == key)
    }

    fn shrink(&mut self) {
        self.kinds.shrink_to_fit();
        self.keys.shrink_to_fit();
    }
}

#[derive(Debug, Default)]
pub(super) struct BucketOwners {
    by_bucket: BTreeMap<u32, BucketList>,
}

impl BucketOwners {
    /// Record that `key` of `kind` holds a ref in `routing_bucket`. The caller adds an object once
    /// per bucket, when its FIRST ref there is filed.
    pub(super) fn add(&mut self, routing_bucket: u32, kind: StoredModelKind, key: &Arc<str>) {
        let list = self.by_bucket.entry(routing_bucket).or_default();
        list.kinds.push(kind);
        list.keys.push(Arc::clone(key));
    }

    /// Forget every kind of `key` in one bucket, for an object that has left it.
    pub(super) fn forget_key(&mut self, routing_bucket: u32, key: &str) {
        if let Some(list) = self.by_bucket.get_mut(&routing_bucket) {
            let mut index = 0;
            while index < list.keys.len() {
                if &*list.keys[index] == key {
                    list.kinds.swap_remove(index);
                    list.keys.swap_remove(index);
                } else {
                    index += 1;
                }
            }
            if list.keys.is_empty() {
                self.by_bucket.remove(&routing_bucket);
            }
        }
    }

    /// The objects recorded as holding a ref in `routing_bucket`.
    pub(super) fn owners_of(&self, routing_bucket: u32) -> Option<&BucketList> {
        self.by_bucket.get(&routing_bucket)
    }

    pub(super) fn forget_bucket(&mut self, routing_bucket: u32) {
        self.by_bucket.remove(&routing_bucket);
    }

    pub(super) fn reset_bucket(&mut self, routing_bucket: u32) {
        if let Some(list) = self.by_bucket.get_mut(&routing_bucket) {
            list.kinds.clear();
            list.keys.clear();
        }
    }

    /// Release the growth slack, for a set that was just built.
    pub(super) fn shrink(&mut self) {
        for list in self.by_bucket.values_mut() {
            list.shrink();
        }
    }

    /// Every bucket and its owners, for the test-only validator.
    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = (&u32, &BucketList)> {
        self.by_bucket.iter()
    }

    /// How many (bucket, object) entries are held.
    #[cfg(test)]
    pub(super) fn entries(&self) -> usize {
        self.by_bucket.values().map(|list| list.keys.len()).sum()
    }

    #[cfg(test)]
    /// The bytes the vectors have reserved, for the footprint figure: capacity and not length, so
    /// growth slack is counted.
    pub(super) fn reserved_bytes(&self) -> usize {
        self.by_bucket
            .values()
            .map(|list| {
                list.kinds.capacity() * std::mem::size_of::<StoredModelKind>()
                    + list.keys.capacity() * std::mem::size_of::<Arc<str>>()
            })
            .sum()
    }
}

/// The optional, boxed owners a `CoreIndex` carries. One pointer wide while absent.
///
/// A CLONE STARTS ABSENT. The clones of an index are taken to encode it, which never asks for the
/// owners, and copying the lists per clone would add to every dump a cost that nothing reads. A
/// clone that is installed as live state is built by `install_shard_state`.
#[derive(Debug, Default)]
pub(super) struct OwnersCell(Option<Box<BucketOwners>>);

impl Clone for OwnersCell {
    fn clone(&self) -> Self {
        Self(None)
    }
}

impl OwnersCell {
    pub(super) fn get(&self) -> Option<&BucketOwners> {
        self.0.as_deref()
    }

    pub(super) fn get_mut(&mut self) -> Option<&mut BucketOwners> {
        self.0.as_deref_mut()
    }

    pub(super) fn set(&mut self, owners: BucketOwners) {
        self.0 = Some(Box::new(owners));
    }
}

#[cfg(test)]
mod ordered_directory_tests {
    use super::*;

    #[test]
    fn growth_retains_every_bucket_and_shared_key() {
        let mut owners = BucketOwners::default();
        let keys: Vec<Arc<str>> = (0..4096)
            .map(|id| Arc::from(format!("key-{id}")))
            .collect();
        for id in (0..4096u32).rev() {
            owners.add(id, StoredModelKind::ALL[0], &keys[id as usize]);
        }
        assert_eq!(owners.entries(), keys.len());
        assert_eq!(owners.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                   (0..4096u32).collect::<Vec<_>>());
        for (id, key) in keys.iter().enumerate() {
            let list = owners.owners_of(id as u32).unwrap();
            let entries: Vec<_> = list.iter().collect();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, StoredModelKind::ALL[0]);
            assert!(Arc::ptr_eq(entries[0].1, key));
        }
        owners.shrink();
        assert_eq!(owners.entries(), keys.len());
    }

    #[test]
    fn forgetting_a_key_removes_all_kinds_but_keeps_other_keys_and_buckets() {
        assert!(!StoredModelKind::ALL.is_empty());
        let mut owners = BucketOwners::default();
        let removed: Arc<str> = Arc::from("removed");
        let retained: Arc<str> = Arc::from("retained");
        for &kind in StoredModelKind::ALL {
            owners.add(3, kind, &removed);
        }
        owners.add(3, StoredModelKind::ALL[0], &retained);
        owners.add(9, StoredModelKind::ALL[0], &removed);
        owners.forget_key(3, "removed");
        let list = owners.owners_of(3).unwrap();
        for &kind in StoredModelKind::ALL {
            assert!(!list.contains(kind, "removed"));
        }
        assert!(list.contains(StoredModelKind::ALL[0], "retained"));
        assert!(owners.owners_of(9).unwrap().contains(StoredModelKind::ALL[0], "removed"));
        owners.forget_key(3, "retained");
        assert!(owners.owners_of(3).is_none());
        owners.forget_key(44, "absent");
        assert!(owners.owners_of(44).is_none());
        assert_eq!(owners.entries(), 1);
    }

    #[test]
    fn reset_and_bucket_removal_allow_exact_refiling() {
        let mut owners = BucketOwners::default();
        let old: Arc<str> = Arc::from("old");
        let new: Arc<str> = Arc::from("new");
        owners.add(7, StoredModelKind::ALL[0], &old);
        owners.reset_bucket(7);
        assert_eq!(owners.owners_of(7).unwrap().iter().count(), 0);
        owners.add(7, StoredModelKind::ALL[0], &new);
        assert_eq!(owners.entries(), 1);
        assert!(!owners.owners_of(7).unwrap().contains(StoredModelKind::ALL[0], "old"));
        assert!(owners.owners_of(7).unwrap().contains(StoredModelKind::ALL[0], "new"));
        owners.forget_bucket(7);
        assert!(owners.owners_of(7).is_none());
        owners.add(7, StoredModelKind::ALL[0], &old);
        assert_eq!(owners.entries(), 1);
        assert!(owners.owners_of(7).unwrap().contains(StoredModelKind::ALL[0], "old"));
    }

    #[test]
    fn a_capture_clone_stays_absent_and_does_not_change_live_ownership() {
        let mut owners = BucketOwners::default();
        let key: Arc<str> = Arc::from("live");
        owners.add(2, StoredModelKind::ALL[0], &key);
        let mut cell = OwnersCell::default();
        cell.set(owners);
        let captured = cell.clone();
        assert!(captured.get().is_none());
        assert!(cell.get().unwrap().owners_of(2).unwrap()
            .contains(StoredModelKind::ALL[0], "live"));
        cell.get_mut().unwrap().forget_bucket(2);
        assert!(cell.get().unwrap().owners_of(2).is_none());
    }
}
