// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::ElementEntry;

/// TAKES AN ITERATOR OF `(key, members)` RATHER THAN THE MAP ITSELF.
///
/// The resident map's value is a per-object ROW now, not the members directly, so a caller cannot
/// hand over a `&HashMap<_, BTreeMap<_, _>>` without building one. An iterator costs nothing
/// extra: this function already collects its own intermediate below, and the caller's `iter()`
/// projects the members out of the row by reference.
///
/// THE BYTES DO NOT MOVE. The encoding is whatever the intermediate `BTreeMap` serializes to, so it
/// stays sorted by key and shaped as a sequence of `(member, address)` pairs per object. The same
/// pairs go in, so the same bytes come out -- checked, not assumed, by
/// `recorded_map_invariant::the_four_kind_encodings_are_byte_for_byte_what_they_were`.
pub fn serialize<'a, S, I>(value: I, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    I: IntoIterator<Item = (&'a super::state::ModelKey, &'a BTreeMap<Vec<u8>, ElementEntry>)>,
{
    let encoded = value
        .into_iter()
        .map(|(key, members)| {
            (
                key,
                members
                    .iter()
                    .map(|(member, address)| (member, address))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<HashMap<super::state::ModelKey, BTreeMap<Vec<u8>, ElementEntry>>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = HashMap::<super::state::ModelKey, Vec<(Vec<u8>, ElementEntry)>>::deserialize(
        deserializer,
    )?;
    Ok(encoded
        .into_iter()
        .map(|(key, members)| (key, members.into_iter().collect()))
        .collect())
}
