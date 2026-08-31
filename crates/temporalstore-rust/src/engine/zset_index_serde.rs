// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::BlockAddress;

pub fn serialize<S>(
    value: &HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = value
        .iter()
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
) -> Result<HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded =
        HashMap::<String, Vec<(Vec<u8>, (u64, BlockAddress))>>::deserialize(deserializer)?;
    Ok(encoded
        .into_iter()
        .map(|(key, members)| (key, members.into_iter().collect()))
        .collect())
}
