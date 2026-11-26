// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{btree_map, hash_map, BTreeSet, HashMap};

use serde::ser::{SerializeSeq, SerializeTuple};
use serde::{Deserialize, Serialize};

use super::filed_string_rows::{FiledRowsIter, FiledRowsIterMut, FiledStringOwner, FiledStringRows};
use super::hashing::block_routing_bucket;
use super::object_record::ObjectSlot;
use super::state::ModelKey;

type Bounds = (u32, u32);
const OWNER_ROWS_VERSION: &str = "string_owner_rows_v1";

#[derive(Clone, Debug)]
pub(super) enum ObjectRows {
    Flat(HashMap<ModelKey, ObjectSlot>),
    String {
        bounds: Bounds,
        rows: FiledStringRows,
        unverified: BTreeSet<ModelKey>,
    },
}

impl Default for ObjectRows {
    fn default() -> Self { Self::Flat(HashMap::new()) }
}

impl<Q: AsRef<str> + ?Sized> std::ops::Index<&Q> for ObjectRows {
    type Output = ObjectSlot;
    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key.as_ref()).expect("object row key")
    }
}

pub(super) enum ObjectRowsIter<'a> {
    Flat(hash_map::Iter<'a, ModelKey, ObjectSlot>),
    String(FiledRowsIter<'a>),
}

impl<'a> Iterator for ObjectRowsIter<'a> {
    type Item = (&'a ModelKey, &'a ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::Flat(rows) => rows.next(), Self::String(rows) => rows.next() }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self { Self::Flat(rows) => rows.size_hint(), Self::String(rows) => rows.size_hint() }
    }
}

pub(super) enum ObjectRowsIterMut<'a> {
    Flat(hash_map::IterMut<'a, ModelKey, ObjectSlot>),
    String(FiledRowsIterMut<'a>),
}

impl<'a> Iterator for ObjectRowsIterMut<'a> {
    type Item = (&'a ModelKey, &'a mut ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::Flat(rows) => rows.next(), Self::String(rows) => rows.next() }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self { Self::Flat(rows) => rows.size_hint(), Self::String(rows) => rows.size_hint() }
    }
}

impl<'a> IntoIterator for &'a ObjectRows {
    type Item = (&'a ModelKey, &'a ObjectSlot);
    type IntoIter = ObjectRowsIter<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

impl<'a> IntoIterator for &'a mut ObjectRows {
    type Item = (&'a ModelKey, &'a mut ObjectSlot);
    type IntoIter = ObjectRowsIterMut<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter_mut() }
}

pub(super) enum OwnerRowsIntoIter {
    One(std::option::IntoIter<(ModelKey, ObjectSlot)>),
    Many(btree_map::IntoIter<ModelKey, ObjectSlot>),
}

impl Iterator for OwnerRowsIntoIter {
    type Item = (ModelKey, ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::One(rows) => rows.next(), Self::Many(rows) => rows.next() }
    }
}

pub(super) enum ObjectRowsIntoIter {
    Flat(hash_map::IntoIter<ModelKey, ObjectSlot>),
    String {
        owners: btree_map::IntoIter<u32, FiledStringOwner>,
        current: Option<OwnerRowsIntoIter>,
        remaining: usize,
    },
}

impl Iterator for ObjectRowsIntoIter {
    type Item = (ModelKey, ObjectSlot);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Flat(rows) => rows.next(),
            Self::String { owners, current, remaining } => loop {
                if let Some(row) = current.as_mut().and_then(Iterator::next) {
                    *remaining -= 1;
                    return Some(row);
                }
                let (_, owner) = owners.next()?;
                *current = Some(match owner {
                    FiledStringOwner::One(key, slot) => OwnerRowsIntoIter::One(Some((key, slot)).into_iter()),
                    FiledStringOwner::Many(rows) => OwnerRowsIntoIter::Many(rows.into_iter()),
                });
            },
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Flat(rows) => rows.size_hint(),
            Self::String { remaining, .. } => (*remaining, Some(*remaining)),
        }
    }
}

impl ExactSizeIterator for ObjectRowsIntoIter {}
impl std::iter::FusedIterator for ObjectRowsIntoIter {}

impl IntoIterator for ObjectRows {
    type Item = (ModelKey, ObjectSlot);
    type IntoIter = ObjectRowsIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Flat(rows) => ObjectRowsIntoIter::Flat(rows.into_iter()),
            Self::String { rows, .. } => {
                let remaining = rows.len();
                ObjectRowsIntoIter::String { owners: rows.into_owners(), current: None, remaining }
            }
        }
    }
}

impl FromIterator<(ModelKey, ObjectSlot)> for ObjectRows {
    fn from_iter<T: IntoIterator<Item = (ModelKey, ObjectSlot)>>(rows: T) -> Self {
        Self::Flat(HashMap::from_iter(rows))
    }
}

impl Extend<(ModelKey, ObjectSlot)> for ObjectRows {
    fn extend<T: IntoIterator<Item = (ModelKey, ObjectSlot)>>(&mut self, rows: T) {
        match self {
            Self::Flat(flat) => flat.extend(rows),
            Self::String { .. } => {
                for (key, slot) in rows { self.insert(key, slot); }
            }
        }
    }
}

impl ObjectRows {
    pub(super) fn string(bounds: Bounds) -> Self {
        Self::String { bounds, rows: FiledStringRows::default(), unverified: BTreeSet::new() }
    }

    pub(super) fn flat(&self) -> Option<&HashMap<ModelKey, ObjectSlot>> {
        match self { Self::Flat(rows) => Some(rows), Self::String { .. } => None }
    }

    pub(super) fn flat_mut(&mut self) -> Option<&mut HashMap<ModelKey, ObjectSlot>> {
        match self { Self::Flat(rows) => Some(rows), Self::String { .. } => None }
    }

    /// One-time legacy conversion and explicit route changes preserve filed ownership.
    /// A legacy row without a unique block owner cannot authorize selected capture.
    pub(super) fn enable_string_owners(&mut self, new_bounds: Bounds) {
        assert!(new_bounds.0 <= new_bounds.1);
        match self {
            Self::String { bounds, rows, .. } => {
                if *bounds == new_bounds { return; }
                rows.reroute_all(|key| block_routing_bucket(key, new_bounds.0, new_bounds.1));
                *bounds = new_bounds;
            }
            Self::Flat(_) => {
                let Self::Flat(legacy) = std::mem::take(self) else { unreachable!() };
                let mut rows = FiledStringRows::default();
                let mut unverified = BTreeSet::new();
                for (key, slot) in legacy {
                    let routed = block_routing_bucket(&key, new_bounds.0, new_bounds.1);
                    let unique_owner = slot.refs().and_then(|refs| {
                        let mut refs = refs.iter();
                        let owner = refs.next()?.routing_bucket;
                        refs.all(|entry| entry.routing_bucket == owner).then_some(owner)
                    });
                    if unique_owner.is_none() { unverified.insert(key.clone()); }
                    rows.insert(key, routed, unique_owner.unwrap_or(routed), slot);
                }
                *self = Self::String { bounds: new_bounds, rows, unverified };
            }
        }
    }

    /// Called with accepted filing evidence, never just a new routing result.
    pub(super) fn file_string(&mut self, key: &str, filed: u32) -> bool {
        let Self::String { bounds, rows, unverified } = self else { return false; };
        let routed = block_routing_bucket(key, bounds.0, bounds.1);
        let Some((canonical, _)) = rows.get_key_value(key, routed) else { return false; };
        if rows.filed_owner(key, routed) != filed {
            let canonical = canonical.clone();
            rows.remap(key, routed, canonical, routed, filed).expect("validated String row");
        }
        unverified.remove(key);
        true
    }

    pub(super) fn unverify_string(&mut self, key: &str) {
        let Self::String { bounds, rows, unverified } = self else { return; };
        let routed = block_routing_bucket(key, bounds.0, bounds.1);
        if let Some((key, _)) = rows.get_key_value(key, routed) {
            unverified.insert(key.clone());
        }
    }

    pub(super) fn adopt_key(&mut self, key: &str, canonical: &ModelKey) -> bool {
        let Some((old, _)) = self.get_key_value(key) else { return false; };
        if std::sync::Arc::ptr_eq(old, canonical) { return false; }
        match self {
            Self::Flat(rows) => {
                let (_, slot) = rows.remove_entry(key).expect("validated object row");
                rows.insert(canonical.clone(), slot);
            }
            Self::String { bounds, rows, unverified } => {
                let routed = block_routing_bucket(key, bounds.0, bounds.1);
                let filed = rows.filed_owner(key, routed);
                let was_unverified = unverified.remove(key);
                let slot = rows.remove(key, routed).expect("validated String row");
                rows.insert(canonical.clone(), routed, filed, slot);
                if was_unverified { unverified.insert(canonical.clone()); }
            }
        }
        true
    }

    pub(super) fn selected_strings(&self, filed: u32)
        -> Option<impl Iterator<Item = (&ModelKey, &ObjectSlot)>>
    {
        let Self::String { rows, unverified, .. } = self else { return None; };
        if !unverified.is_empty() { return None; }
        Some(rows.capture(filed))
    }

    pub(super) fn get_key_value(&self, key: &str) -> Option<(&ModelKey, &ObjectSlot)> {
        match self {
            Self::Flat(rows) => rows.get_key_value(key),
            Self::String { bounds, rows, .. } => rows.get_key_value(key,
                block_routing_bucket(key, bounds.0, bounds.1)),
        }
    }

    pub(super) fn get(&self, key: &str) -> Option<&ObjectSlot> {
        self.get_key_value(key).map(|(_, slot)| slot)
    }

    pub(super) fn get_mut(&mut self, key: &str) -> Option<&mut ObjectSlot> {
        match self {
            Self::Flat(rows) => rows.get_mut(key),
            Self::String { bounds, rows, .. } => rows.get_mut(key,
                block_routing_bucket(key, bounds.0, bounds.1)),
        }
    }

    pub(super) fn contains_key(&self, key: &str) -> bool { self.get(key).is_some() }
    pub(super) fn len(&self) -> usize {
        match self { Self::Flat(rows) => rows.len(), Self::String { rows, .. } => rows.len() }
    }
    pub(super) fn is_empty(&self) -> bool { self.len() == 0 }

    pub(super) fn insert(&mut self, key: ModelKey, slot: ObjectSlot) -> Option<ObjectSlot> {
        match self {
            Self::Flat(rows) => rows.insert(key, slot),
            Self::String { bounds, rows, unverified } => {
                let routed = block_routing_bucket(&key, bounds.0, bounds.1);
                let filed = rows.filed_owner(&key, routed);
                unverified.insert(key.clone());
                rows.insert(key, routed, filed, slot)
            }
        }
    }

    pub(super) fn remove(&mut self, key: &str) -> Option<ObjectSlot> {
        match self {
            Self::Flat(rows) => rows.remove(key),
            Self::String { bounds, rows, unverified } => {
                unverified.remove(key);
                rows.remove(key, block_routing_bucket(key, bounds.0, bounds.1))
            }
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Flat(rows) => rows.clear(),
            Self::String { rows, unverified, .. } => {
                *rows = FiledStringRows::default();
                unverified.clear();
            }
        }
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&ModelKey, &mut ObjectSlot) -> bool) {
        match self {
            Self::Flat(rows) => rows.retain(|key, slot| keep(key, slot)),
            Self::String { rows, unverified, .. } => rows.retain(|key, slot| {
                if keep(key, slot) { return true; }
                unverified.remove(key);
                false
            }),
        }
    }

    pub(super) fn iter(&self) -> ObjectRowsIter<'_> {
        match self {
            Self::Flat(rows) => ObjectRowsIter::Flat(rows.iter()),
            Self::String { rows, .. } => ObjectRowsIter::String(rows.iter()),
        }
    }
    pub(super) fn iter_mut(&mut self) -> ObjectRowsIterMut<'_> {
        match self {
            Self::Flat(rows) => ObjectRowsIterMut::Flat(rows.iter_mut()),
            Self::String { rows, .. } => ObjectRowsIterMut::String(rows.iter_mut()),
        }
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &ModelKey> { self.iter().map(|(key, _)| key) }
    pub(super) fn values(&self) -> impl Iterator<Item = &ObjectSlot> { self.iter().map(|(_, slot)| slot) }
    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut ObjectSlot> {
        self.iter_mut().map(|(_, slot)| slot)
    }

    pub(super) fn entry(&mut self, key: ModelKey) -> ObjectRowsEntry<'_> {
        match self {
            Self::Flat(rows) => ObjectRowsEntry::Flat(rows.entry(key)),
            Self::String { .. } => ObjectRowsEntry::String { rows: self, key },
        }
    }
}

pub(super) enum ObjectRowsEntry<'a> {
    Flat(hash_map::Entry<'a, ModelKey, ObjectSlot>),
    String { rows: &'a mut ObjectRows, key: ModelKey },
}

impl<'a> ObjectRowsEntry<'a> {
    pub(super) fn or_insert_with(self, make: impl FnOnce() -> ObjectSlot) -> &'a mut ObjectSlot {
        match self {
            Self::Flat(entry) => entry.or_insert_with(make),
            Self::String { rows, key } => {
                if !rows.contains_key(&key) { rows.insert(key.clone(), make()); }
                rows.get_mut(&key).expect("inserted String row")
            }
        }
    }
    pub(super) fn or_default(self) -> &'a mut ObjectSlot { self.or_insert_with(ObjectSlot::default) }
    pub(super) fn or_insert(self, slot: ObjectSlot) -> &'a mut ObjectSlot {
        self.or_insert_with(|| slot)
    }
}

struct StringRowsWire<'a>(&'a FiledStringRows, &'a BTreeSet<ModelKey>);

impl Serialize for StringRowsWire<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut rows = serializer.serialize_seq(Some(self.0.len()))?;
        for (filed, key, slot) in self.0.iter_filed() {
            rows.serialize_element(&(filed, !self.1.contains(key), key, slot))?;
        }
        rows.end()
    }
}

impl Serialize for ObjectRows {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Flat(rows) => rows.serialize(serializer),
            Self::String { bounds, rows, unverified } => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(OWNER_ROWS_VERSION)?;
                tuple.serialize_element(bounds)?;
                tuple.serialize_element(&StringRowsWire(rows, unverified))?;
                tuple.end()
            }
        }
    }
}

struct StringRowsSeed(Bounds);

impl<'de> serde::de::DeserializeSeed<'de> for StringRowsSeed {
    type Value = (FiledStringRows, BTreeSet<ModelKey>);
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        struct Visitor(Bounds);
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = (FiledStringRows, BTreeSet<ModelKey>);
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("filed String rows")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let mut rows = FiledStringRows::default();
                let mut unverified = BTreeSet::new();
                while let Some((filed, verified, key, slot)) = seq.next_element::<(u32, bool, ModelKey, ObjectSlot)>()? {
                    let routed = block_routing_bucket(&key, self.0.0, self.0.1);
                    if rows.get(&key, routed).is_some() { return Err(A::Error::custom("duplicate String key")); }
                    if !verified { unverified.insert(key.clone()); }
                    rows.insert(key, routed, filed, slot);
                }
                Ok((rows, unverified))
            }
        }
        deserializer.deserialize_seq(Visitor(self.0))
    }
}

impl<'de> Deserialize<'de> for ObjectRows {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowsVisitor;
        impl<'de> serde::de::Visitor<'de> for RowsVisitor {
            type Value = ObjectRows;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an object map or versioned String owner rows")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                HashMap::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(ObjectRows::Flat)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let version: String = seq.next_element()?.ok_or_else(|| A::Error::custom("missing owner row version"))?;
                if version != OWNER_ROWS_VERSION { return Err(A::Error::custom("unsupported owner row version")); }
                let bounds: Bounds = seq.next_element()?.ok_or_else(|| A::Error::custom("missing String routing bounds"))?;
                if bounds.0 > bounds.1 { return Err(A::Error::custom("invalid String routing bounds")); }
                let (rows, unverified) = seq.next_element_seed(StringRowsSeed(bounds))?
                    .ok_or_else(|| A::Error::custom("missing String owner rows"))?;
                if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("extra String owner row field"));
                }
                Ok(ObjectRows::String { bounds, rows, unverified })
            }
        }
        deserializer.deserialize_any(RowsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn legacy_rows_move_once_and_require_filing_before_selected_capture() {
        let key: ModelKey = Arc::from("legacy-row");
        let mut rows = ObjectRows::Flat(HashMap::from([(key.clone(), ObjectSlot::default())]));
        rows.enable_string_owners((0, u32::MAX));
        assert!(rows.flat().is_none());
        assert_eq!(rows.len(), 1);
        assert!(Arc::ptr_eq(rows.get_key_value(&key).unwrap().0, &key));
        assert!(rows.selected_strings(17).is_none());
        assert!(rows.file_string(&key, 17));
        assert_eq!(rows.selected_strings(17).unwrap().count(), 1);
        rows.enable_string_owners((30, 70));
        assert!(rows.contains_key(&key));
        assert_eq!(rows.selected_strings(17).unwrap().count(), 1);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn owner_codec_preserves_filing_and_refuses_unknown_or_duplicate_rows() {
        let mut rows = ObjectRows::string((0, u32::MAX));
        rows.insert(Arc::from("first"), ObjectSlot::default());
        rows.insert(Arc::from("second"), ObjectSlot::default());
        assert!(rows.file_string("first", 7));
        assert!(rows.file_string("second", 11));
        let bytes = rmp_serde::to_vec_named(&rows).unwrap();
        let mut reopened: ObjectRows = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(reopened.selected_strings(7).unwrap().count(), 1);
        assert_eq!(reopened.selected_strings(11).unwrap().count(), 1);
        reopened.enable_string_owners((50, 100));
        assert!(reopened.get("first").is_some());
        assert!(reopened.get("second").is_some());
        assert_eq!(reopened.selected_strings(7).unwrap().count(), 1);

        let mut encoded = serde_json::to_value(&rows).unwrap();
        encoded[0] = serde_json::json!("string_owner_rows_v99");
        assert!(serde_json::from_value::<ObjectRows>(encoded).is_err());
        let mut encoded = serde_json::to_value(&rows).unwrap();
        let duplicate = encoded[2][0].clone();
        encoded[2].as_array_mut().unwrap().push(duplicate);
        assert!(serde_json::from_value::<ObjectRows>(encoded).is_err());
    }

    #[test]
    fn mutating_borrow_and_retain_reach_only_the_canonical_slots() {
        let mut rows = ObjectRows::string((0, u32::MAX));
        for index in 0..128 {
            let key: ModelKey = Arc::from(format!("row-{index}"));
            rows.insert(key.clone(), ObjectSlot::default());
            assert!(rows.file_string(&key, 4));
        }
        let mut visited = 0;
        for (_, slot) in &mut rows {
            visited += 1;
            slot.set_logical_size(91);
        }
        assert_eq!(visited, 128);
        assert!(rows.values().all(|slot| slot.logical_size() == Some(91)));
        rows.retain(|key, _| key.as_ref() == "row-1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.selected_strings(4).unwrap().count(), 1);
        assert_eq!(rows.get("row-1").unwrap().logical_size(), Some(91));
        rows.remove("row-1");
        assert!(rows.is_empty());
    }

    #[test]
    fn reconstructed_string_rows_keep_metadata_and_persist_the_accepted_owner() {
        use super::super::state::{CoreIndex, ReconstructedStringRow, ShardState};
        use super::super::storage_bucket_internals::StoredModelKind;
        let mut shard = ShardState::default();
        shard.set_routing_range(0, 100);
        let key: ModelKey = Arc::from("surviving-key");
        let mut slot = ObjectSlot::default();
        slot.set_logical_size(91);
        let address = slot.address();
        shard.strings_mut().insert(key.clone(), slot);
        shard.strings_mut().insert(Arc::from("removed-key"), ObjectSlot::default());
        assert!(shard.strings_mut().file_string(&key, 7));
        let unresolved = shard.install_reconstructed_string_addresses(HashMap::from([
            (key.clone(), ReconstructedStringRow { filed_bucket: Some(19), address }),
        ]));
        assert_eq!(unresolved, 0);
        assert_eq!(shard.strings().len(), 1);
        assert_eq!(shard.string_row(&key).unwrap().logical_size(), Some(91));
        assert_eq!(shard.strings().selected_strings(19).unwrap().count(), 1);
        assert_eq!(shard.strings().selected_strings(7).unwrap().count(), 0);
        let encoded = rmp_serde::to_vec_named(&shard.bucket_index).unwrap();
        let decoded: CoreIndex = rmp_serde::from_slice(&encoded).unwrap();
        let restored = decoded.objects.get(&StoredModelKind::String).unwrap();
        assert_eq!(restored.get(&key).unwrap().logical_size(), Some(91));
        assert_eq!(restored.selected_strings(19).unwrap().count(), 1);
    }

    #[test]
    fn reconstruction_without_filing_never_invents_selected_owner_authority() {
        use super::super::state::{ReconstructedStringRow, ShardState};
        let mut shard = ShardState::default();
        shard.set_routing_range(0, 100);
        let key: ModelKey = Arc::from("unresolved-row");
        let address = ObjectSlot::default().address();
        assert_eq!(shard.install_reconstructed_string_addresses(HashMap::from([
            (key.clone(), ReconstructedStringRow { filed_bucket: None, address }),
        ])), 1);
        assert!(shard.string_address(&key).is_some());
        for owner in [0, 19, 100] {
            assert!(shard.strings().selected_strings(owner).is_none());
        }
    }
}
