// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Deserializer, Serialize};

/// Legacy records use a boolean; mixed batches name only the keys to replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexDeltaApplyMode {
    Legacy(bool),
    KeyedSnapshots(KeyedSnapshots),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyedSnapshots {
    #[serde(deserialize_with = "nonempty_keys")]
    replace_keys: Vec<String>,
}

fn nonempty_keys<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    let keys = Vec::<String>::deserialize(deserializer)?;
    if keys.is_empty() {
        return Err(serde::de::Error::custom(
            "keyed snapshot requires replacement keys",
        ));
    }
    Ok(keys)
}

impl Default for IndexDeltaApplyMode {
    fn default() -> Self {
        Self::Legacy(false)
    }
}

impl From<bool> for IndexDeltaApplyMode {
    fn from(upsert: bool) -> Self {
        Self::Legacy(upsert)
    }
}

impl IndexDeltaApplyMode {
    pub fn keyed_snapshots(mut keys: Vec<String>) -> Option<Self> {
        if keys.is_empty() {
            return None;
        }
        keys.sort_unstable();
        keys.dedup();
        Some(Self::KeyedSnapshots(KeyedSnapshots { replace_keys: keys }))
    }

    pub fn legacy_upsert(&self) -> Option<bool> {
        match self {
            Self::Legacy(upsert) => Some(*upsert),
            Self::KeyedSnapshots(_) => None,
        }
    }

    pub fn replacement_keys(&self) -> Option<&[String]> {
        match self {
            Self::Legacy(_) => None,
            Self::KeyedSnapshots(plan) => Some(&plan.replace_keys),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_boolean_modes_keep_their_wire_shape() {
        for upsert in [false, true] {
            let mode = IndexDeltaApplyMode::from(upsert);
            assert_eq!(
                serde_json::to_value(&mode).unwrap(),
                serde_json::json!(upsert)
            );
            assert_eq!(
                rmp_serde::to_vec(&mode).unwrap(),
                rmp_serde::to_vec(&upsert).unwrap()
            );
            assert_eq!(
                serde_json::from_value::<IndexDeltaApplyMode>(serde_json::json!(upsert)).unwrap(),
                mode
            );
        }
    }

    #[test]
    fn keyed_mode_roundtrips_and_legacy_boolean_readers_refuse_it() {
        let mode =
            IndexDeltaApplyMode::keyed_snapshots(vec!["b".into(), "a".into(), "b".into()]).unwrap();
        assert_eq!(
            mode.replacement_keys().unwrap(),
            &["a".to_string(), "b".to_string()]
        );
        assert_eq!(mode.legacy_upsert(), None);
        let json = serde_json::to_vec(&mode).unwrap();
        let packed = rmp_serde::to_vec(&mode).unwrap();
        assert_eq!(
            serde_json::from_slice::<IndexDeltaApplyMode>(&json).unwrap(),
            mode
        );
        assert_eq!(
            rmp_serde::from_slice::<IndexDeltaApplyMode>(&packed).unwrap(),
            mode
        );
        assert!(serde_json::from_slice::<bool>(&json).is_err());
        assert!(rmp_serde::from_slice::<bool>(&packed).is_err());
    }

    #[test]
    fn malformed_modes_are_not_treated_as_plain_upserts() {
        for json in [
            r#"{}"#,
            r#"{"replace_keys":[]}"#,
            r#"{"replace_keys":[1]}"#,
            r#"{"replace_keys":["a"],"unknown":true}"#,
            "1",
            "null",
            r#""upsert""#,
        ] {
            assert!(
                serde_json::from_str::<IndexDeltaApplyMode>(json).is_err(),
                "{json}"
            );
        }
        assert!(IndexDeltaApplyMode::keyed_snapshots(Vec::new()).is_none());
        assert!(IndexDeltaApplyMode::keyed_snapshots(vec![String::new()]).is_some());
    }
}
