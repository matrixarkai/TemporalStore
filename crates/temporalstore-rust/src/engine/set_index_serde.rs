use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::page_store::PageAddress;

pub fn serialize<S>(
    value: &HashMap<String, BTreeMap<Vec<u8>, PageAddress>>,
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
                    .map(|(member, address)| (member, address))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, BTreeMap<Vec<u8>, PageAddress>>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = HashMap::<String, Vec<(Vec<u8>, PageAddress)>>::deserialize(deserializer)?;
    Ok(encoded
        .into_iter()
        .map(|(key, members)| (key, members.into_iter().collect()))
        .collect())
}
