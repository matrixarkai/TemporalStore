// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Canonical String slots, ordered by filed owner and then by key.
//!
//! Routing is supplied by the caller, never inferred from the slot. A key's route
//! must stay stable between insertion and explicit `remap`. Route changes update
//! the exceptional locators before the caller publishes its new routing bounds.

use std::collections::{btree_map, BTreeMap};
use std::ops::Bound::{Excluded, Unbounded};

use super::object_record::{ObjectRecord, ObjectSlot};
use super::state::ModelKey;

#[derive(Clone, Debug, Default)]
pub(super) struct FiledStringRows {
    owners: BTreeMap<u32, FiledStringOwner>,
    // Only a misfiled row pays for a second key handle; never a second slot.
    exceptions: BTreeMap<ModelKey, u32>,
    len: usize,
}

#[derive(Clone, Debug)]
pub(super) enum FiledStringOwner {
    One(ModelKey, ObjectSlot),
    Many(BTreeMap<ModelKey, ObjectSlot>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RemapError {
    MissingSource,
    DestinationExists,
}

enum OwnerRowsIter<'a> {
    One(std::option::IntoIter<(&'a ModelKey, &'a ObjectSlot)>),
    Many(btree_map::Iter<'a, ModelKey, ObjectSlot>),
}

impl<'a> Iterator for OwnerRowsIter<'a> {
    type Item = (&'a ModelKey, &'a ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::One(rows) => rows.next(), Self::Many(rows) => rows.next() }
    }
}

enum OwnerRowsIterMut<'a> {
    One(std::option::IntoIter<(&'a ModelKey, &'a mut ObjectSlot)>),
    Many(btree_map::IterMut<'a, ModelKey, ObjectSlot>),
}

impl<'a> Iterator for OwnerRowsIterMut<'a> {
    type Item = (&'a ModelKey, &'a mut ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::One(rows) => rows.next(), Self::Many(rows) => rows.next() }
    }
}

pub(super) struct FiledRowsIter<'a> {
    owners: btree_map::Values<'a, u32, FiledStringOwner>,
    current: Option<OwnerRowsIter<'a>>,
    remaining: usize,
}

impl<'a> Iterator for FiledRowsIter<'a> {
    type Item = (&'a ModelKey, &'a ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.as_mut().and_then(Iterator::next) {
                self.remaining -= 1;
                return Some(row);
            }
            self.current = Some(self.owners.next()?.iter());
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}

impl ExactSizeIterator for FiledRowsIter<'_> {}
impl std::iter::FusedIterator for FiledRowsIter<'_> {}

pub(super) struct FiledRowsIterMut<'a> {
    owners: btree_map::ValuesMut<'a, u32, FiledStringOwner>,
    current: Option<OwnerRowsIterMut<'a>>,
    remaining: usize,
}

impl<'a> Iterator for FiledRowsIterMut<'a> {
    type Item = (&'a ModelKey, &'a mut ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.as_mut().and_then(Iterator::next) {
                self.remaining -= 1;
                return Some(row);
            }
            self.current = Some(self.owners.next()?.iter_mut());
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}

impl ExactSizeIterator for FiledRowsIterMut<'_> {}
impl std::iter::FusedIterator for FiledRowsIterMut<'_> {}

impl<'a> IntoIterator for &'a FiledStringRows {
    type Item = (&'a ModelKey, &'a ObjectSlot);
    type IntoIter = FiledRowsIter<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

impl<'a> IntoIterator for &'a mut FiledStringRows {
    type Item = (&'a ModelKey, &'a mut ObjectSlot);
    type IntoIter = FiledRowsIterMut<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter_mut() }
}

impl FiledStringOwner {
    fn iter(&self) -> OwnerRowsIter<'_> {
        match self {
            Self::One(key, slot) => OwnerRowsIter::One(Some((key, slot)).into_iter()),
            Self::Many(rows) => OwnerRowsIter::Many(rows.iter()),
        }
    }

    fn iter_mut(&mut self) -> OwnerRowsIterMut<'_> {
        match self {
            Self::One(key, slot) => OwnerRowsIterMut::One(Some((&*key, slot)).into_iter()),
            Self::Many(rows) => OwnerRowsIterMut::Many(rows.iter_mut()),
        }
    }

    pub(super) fn into_rows(self) -> impl Iterator<Item = (ModelKey, ObjectSlot)> {
        let (one, many) = match self {
            Self::One(key, slot) => (Some((key, slot)), None),
            Self::Many(rows) => (None, Some(rows.into_iter())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}

impl FiledStringRows {
    pub(super) fn len(&self) -> usize { self.len }

    pub(super) fn is_empty(&self) -> bool { self.len == 0 }

    pub(super) fn reroute_all(&mut self, mut route: impl FnMut(&str) -> u32) {
        let Self { owners, exceptions, .. } = self;
        for (&filed, owner) in owners.iter() {
            for (key, _) in owner.iter() {
                if route(key.as_ref()) == filed {
                    exceptions.remove(key.as_ref());
                } else {
                    exceptions.insert(key.clone(), filed);
                }
            }
        }
    }

    /// Resolve a point lookup using the route valid for this installed key.
    pub(super) fn filed_owner(&self, key: &str, routed: u32) -> u32 {
        self.exceptions.get(key).copied().unwrap_or(routed)
    }

    pub(super) fn get(&self, key: &str, routed: u32) -> Option<&ObjectSlot> {
        self.get_key_value(key, routed).map(|(_, slot)| slot)
    }

    pub(super) fn get_key_value(&self, key: &str, routed: u32) -> Option<(&ModelKey, &ObjectSlot)> {
        match self.owners.get(&self.filed_owner(key, routed))? {
            FiledStringOwner::One(stored, slot) => (stored.as_ref() == key).then_some((stored, slot)),
            FiledStringOwner::Many(rows) => rows.get_key_value(key),
        }
    }

    pub(super) fn get_mut(&mut self, key: &str, routed: u32) -> Option<&mut ObjectSlot> {
        let filed = self.filed_owner(key, routed);
        match self.owners.get_mut(&filed)? {
            FiledStringOwner::One(stored, slot) => (stored.as_ref() == key).then_some(slot),
            FiledStringOwner::Many(rows) => rows.get_mut(key),
        }
    }

    /// Replace just metadata, preserving the slot's owned elements and block refs.
    pub(super) fn replace_record(
        &mut self,
        key: &str,
        routed: u32,
        record: ObjectRecord,
    ) -> Option<ObjectRecord> {
        Some(std::mem::replace(&mut self.get_mut(key, routed)?.record, record))
    }

    /// Exchange an entire slot, returning the previous owned slot, if any.
    /// `routed` must be the existing route on replacement. Use `remap` to change it.
    /// Unlike `replace_record`, this does not merge old payload or block refs.
    pub(super) fn insert(
        &mut self,
        key: ModelKey,
        routed: u32,
        filed: u32,
        slot: ObjectSlot,
    ) -> Option<ObjectSlot> {
        if self.filed_owner(key.as_ref(), routed) == filed {
            if let Some(current) = self.get_mut(key.as_ref(), routed) {
                return Some(std::mem::replace(current, slot));
            }
        }
        let old = self.remove(key.as_ref(), routed);
        self.install(key, routed, filed, slot);
        old
    }

    pub(super) fn remove(&mut self, key: &str, routed: u32) -> Option<ObjectSlot> {
        let filed = self.filed_owner(key, routed);
        let btree_map::Entry::Occupied(mut owner) = self.owners.entry(filed) else {
            return None;
        };
        let removed = match owner.get_mut() {
            FiledStringOwner::One(stored, _) if stored.as_ref() == key => {
                let FiledStringOwner::One(_, slot) = owner.remove() else { unreachable!() };
                slot
            }
            FiledStringOwner::One(..) => return None,
            FiledStringOwner::Many(rows) => {
                let slot = rows.remove(key)?;
                if rows.len() == 1 {
                    let (key, remaining) = rows.pop_first().expect("one remaining row");
                    owner.insert(FiledStringOwner::One(key, remaining));
                }
                slot
            }
        };
        self.exceptions.remove(key);
        self.len -= 1;
        Some(removed)
    }

    /// Visit each canonical slot once, retaining matching rows in place. Keys
    /// and filing identities cannot be changed by the predicate.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&ModelKey, &mut ObjectSlot) -> bool) {
        let exceptions = &mut self.exceptions;
        let len = &mut self.len;
        self.owners.retain(|_, owner| match owner {
            FiledStringOwner::One(key, slot) => {
                if keep(key, slot) { return true; }
                exceptions.remove(key.as_ref());
                *len -= 1;
                false
            }
            FiledStringOwner::Many(rows) => {
                rows.retain(|key, slot| {
                    if keep(key, slot) { return true; }
                    exceptions.remove(key.as_ref());
                    *len -= 1;
                    false
                });
                if rows.len() == 1 {
                    let (key, slot) = rows.pop_first().expect("one retained row");
                    *owner = FiledStringOwner::One(key, slot);
                    true
                } else {
                    !rows.is_empty()
                }
            }
        });
    }

    /// Move/rekey a slot without rebuilding or cloning its payload.
    /// Both routes must come from their respective identities. A same-key route
    /// change still requires this call, even when the filed owner stays unchanged.
    /// Errors leave both rows and locators untouched. External indexes must be
    /// updated under the same CoreIndex guard as this operation.
    pub(super) fn remap(
        &mut self,
        old_key: &str,
        old_routed: u32,
        new_key: ModelKey,
        new_routed: u32,
        new_filed: u32,
    ) -> Result<(), RemapError> {
        if self.get(old_key, old_routed).is_none() {
            return Err(RemapError::MissingSource);
        }
        if old_key != new_key.as_ref() && self.get(new_key.as_ref(), new_routed).is_some() {
            return Err(RemapError::DestinationExists);
        }
        if old_key == new_key.as_ref() && self.filed_owner(old_key, old_routed) == new_filed {
            return self.reroute(old_key, old_routed, new_routed);
        }
        let slot = self.remove(old_key, old_routed).expect("validated source");
        self.install(new_key, new_routed, new_filed, slot);
        Ok(())
    }

    /// Change only the exceptional locator. The canonical owner, key, and slot
    /// stay in place, including when routing changes into or out of misfiling.
    pub(super) fn reroute(
        &mut self,
        key: &str,
        old_routed: u32,
        new_routed: u32,
    ) -> Result<(), RemapError> {
        let filed = self.filed_owner(key, old_routed);
        let (canonical, _) = self.get_key_value(key, old_routed).ok_or(RemapError::MissingSource)?;
        if filed != new_routed {
            let canonical = canonical.clone();
            self.exceptions.insert(canonical, filed);
        } else {
            self.exceptions.remove(key);
        }
        Ok(())
    }

    /// Decode/migrate owned rows without cloning slots. `route` must return the
    /// stable route for each key. Duplicate keys reject the entire import;
    /// partial state, the rejected slot, and the input iterator are dropped on error.
    pub(super) fn try_from_rows(
        rows: impl IntoIterator<Item = (u32, ModelKey, ObjectSlot)>,
        mut route: impl FnMut(&str) -> u32,
    ) -> Result<Self, ModelKey> {
        let mut result = Self::default();
        for (filed, key, slot) in rows {
            let routed = route(key.as_ref());
            if result.get(key.as_ref(), routed).is_some() { return Err(key); }
            result.install(key, routed, filed, slot);
        }
        Ok(result)
    }

    // Caller has removed any existing row. This touches only one owner and one
    // exceptional locator, not a flat mirror or a whole-table derived cache.
    fn install(&mut self, key: ModelKey, routed: u32, filed: u32, slot: ObjectSlot) {
        if routed != filed {
            self.exceptions.insert(key.clone(), filed);
        }
        match self.owners.entry(filed) {
            btree_map::Entry::Vacant(owner) => {
                owner.insert(FiledStringOwner::One(key, slot));
            }
            btree_map::Entry::Occupied(mut owner) => match owner.get_mut() {
                FiledStringOwner::Many(rows) => {
                    assert!(!rows.contains_key(key.as_ref()), "inconsistent routed identity");
                    rows.insert(key, slot);
                }
                FiledStringOwner::One(stored, _) => {
                    assert!(stored.as_ref() != key.as_ref(), "inconsistent routed identity");
                    // An empty BTreeMap allocates no nodes; move the singleton out.
                    let first = std::mem::replace(owner.get_mut(), FiledStringOwner::Many(BTreeMap::new()));
                    let FiledStringOwner::One(first_key, first_slot) = first else { unreachable!() };
                    owner.insert(FiledStringOwner::Many(BTreeMap::from([
                        (first_key, first_slot), (key, slot),
                    ])));
                }
            },
        }
        self.len += 1;
    }

    pub(super) fn capture(&self, filed: u32) -> impl Iterator<Item = (&ModelKey, &ObjectSlot)> {
        self.rows_after(filed, None, usize::MAX)
    }

    /// Exclusive lexical seek within one filed owner. No cloned prefix or scan.
    /// A resumed page requires caller-managed owner revision validation.
    pub(super) fn rows_after<'a>(
        &'a self,
        filed: u32,
        after: Option<&str>,
        limit: usize,
    ) -> impl Iterator<Item = (&'a ModelKey, &'a ObjectSlot)> + 'a {
        let (one, many) = match self.owners.get(&filed) {
            Some(FiledStringOwner::One(key, slot)) => (
                after.map_or(true, |after| key.as_ref() > after).then_some((key, slot)), None,
            ),
            Some(FiledStringOwner::Many(rows)) => (
                None, Some(rows.range::<str, _>((after.map_or(Unbounded, Excluded), Unbounded))),
            ),
            None => (None, None),
        };
        one.into_iter().chain(many.into_iter().flatten()).take(limit)
    }

    /// Deterministic filed-owner order, then lexical key order within each owner.
    pub(super) fn iter(&self) -> FiledRowsIter<'_> {
        FiledRowsIter { owners: self.owners.values(), current: None, remaining: self.len }
    }

    pub(super) fn iter_mut(&mut self) -> FiledRowsIterMut<'_> {
        FiledRowsIterMut { owners: self.owners.values_mut(), current: None, remaining: self.len }
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &ModelKey> {
        self.iter().map(|(key, _)| key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &ObjectSlot> {
        self.iter().map(|(_, slot)| slot)
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut ObjectSlot> {
        self.iter_mut().map(|(_, slot)| slot)
    }

    pub(super) fn iter_filed(&self) -> impl Iterator<Item = (u32, &ModelKey, &ObjectSlot)> {
        self.owners.iter().flat_map(|(&filed, owner)| {
            owner.iter().map(move |(key, slot)| (filed, key, slot))
        })
    }

    pub(super) fn iter_filed_mut(&mut self) -> impl Iterator<Item = (u32, &ModelKey, &mut ObjectSlot)> {
        self.owners.iter_mut().flat_map(|(&filed, owner)| {
            owner.iter_mut().map(move |(key, slot)| (filed, key, slot))
        })
    }

    pub(super) fn into_rows(self) -> impl Iterator<Item = (u32, ModelKey, ObjectSlot)> {
        self.into_owners().flat_map(|(filed, owner)| {
            owner.into_rows().map(move |(key, slot)| (filed, key, slot))
        })
    }

    /// The exception locator is derived and is intentionally not exported.
    pub(super) fn into_owners(self) -> btree_map::IntoIter<u32, FiledStringOwner> {
        self.owners.into_iter()
    }
}

#[cfg(test)]
#[path = "filed_string_rows_tests.rs"]
mod tests;
