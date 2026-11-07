// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::ElementEntry;

/// TAKES AN ITERATOR OF `(key, members)` RATHER THAN THE MAP ITSELF, for the reason
/// `set_index_serde::serialize` gives: the resident map's value is a per-object ROW now, the
/// intermediate below is collected either way, and the stored bytes are unchanged because the same
/// pairs go in. A zset's member carries its score beside the address, which is why this codec is
/// stated per kind rather than shared with the set one.
pub fn serialize<'a, S, I>(value: I, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    I: IntoIterator<
        Item = (
            &'a super::state::ModelKey,
            &'a BTreeMap<Vec<u8>, (u64, ElementEntry)>,
        ),
    >,
{
    let encoded = value
        .into_iter()
        .map(|(key, members)| {
            (
                key,
                members
                    .iter()
                    .map(|(member, entry)| (member, entry))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<HashMap<super::state::ModelKey, BTreeMap<Vec<u8>, (u64, ElementEntry)>>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded =
        HashMap::<super::state::ModelKey, Vec<(Vec<u8>, (u64, ElementEntry))>>::deserialize(
            deserializer,
        )?;
    Ok(encoded
        .into_iter()
        .map(|(key, members)| (key, members.into_iter().collect()))
        .collect())
}
