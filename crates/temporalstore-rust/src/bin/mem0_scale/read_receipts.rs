// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_READ_PROBES: usize = 4;

#[derive(Debug)]
pub(super) struct ReadProbe {
    pub(super) id: String,
    text_digest: [u8; 32],
}

impl ReadProbe {
    pub(super) fn check_get(&self, value: &Value) -> Result<(), String> {
        if value["id"].as_str() == Some(self.id.as_str())
            && value["memory"].as_str().is_some_and(|text| {
                <[u8; 32]>::from(Sha256::digest(text.as_bytes())) == self.text_digest
            })
        {
            Ok(())
        } else {
            Err("read differs from the acknowledged memory".into())
        }
    }

    pub(super) fn check_history(&self, value: &Value) -> Result<(), String> {
        if value["results"].as_array().is_some_and(|rows| {
            rows.iter()
                .all(|row| row["id"].as_str() == Some(self.id.as_str()))
                && rows.iter().any(|row| row["event"] == "ADD")
        }) {
            Ok(())
        } else {
            Err("acknowledged memory creation history is missing or mixed".into())
        }
    }
}

/// Keep two oldest and two latest receipts, never the source bodies or whole catalog.
pub(super) fn remember(probes: &mut Vec<ReadProbe>, response: &Value) {
    let Some(rows) = response["results"].as_array() else {
        return;
    };
    for (position, row) in rows.iter().enumerate() {
        let (Some(id), Some(text)) = (row["id"].as_str(), row["memory"].as_str()) else {
            continue;
        };
        if let Some(existing) = probes.iter_mut().find(|probe| probe.id == id) {
            existing.text_digest = Sha256::digest(text.as_bytes()).into();
            continue;
        }
        if probes.len() >= 2 && position < rows.len().saturating_sub(2) {
            continue;
        }
        if probes.len() == MAX_READ_PROBES {
            probes.remove(2);
        }
        probes.push(ReadProbe {
            id: id.to_owned(),
            text_digest: Sha256::digest(text.as_bytes()).into(),
        });
    }
}

pub(super) fn baseline_state(ingestion_acknowledged: bool, probes: &[ReadProbe]) -> &'static str {
    if !ingestion_acknowledged {
        "unknown_ingestion_outcome"
    } else if probes.is_empty() {
        "no_acknowledged_memory"
    } else {
        "acknowledged_ingestion"
    }
}

pub(super) fn valid_reset_receipt(value: &Value) -> bool {
    value["deleted"].as_u64().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn receipts_keep_the_oldest_and_latest_ids_with_fixed_storage() {
        let mut probes = Vec::new();
        for start in [0, 100] {
            remember(
                &mut probes,
                &json!({"results": (start..start + 100)
                .map(|id| json!({"id":id.to_string(),"memory":format!("source {id}")}))
                .collect::<Vec<_>>()}),
            );
            assert_eq!(probes.len(), MAX_READ_PROBES);
        }
        assert_eq!(
            probes
                .iter()
                .map(|probe| probe.id.as_str())
                .collect::<Vec<_>>(),
            vec!["0", "1", "198", "199"]
        );
    }

    #[test]
    fn reads_must_preserve_the_id_and_complete_acknowledged_text() {
        let mut probes = Vec::new();
        remember(
            &mut probes,
            &json!({"results":[{"id":"a","memory":"full source"}]}),
        );
        assert!(probes[0]
            .check_get(&json!({"id":"a","memory":"full source"}))
            .is_ok());
        for value in [
            json!({"id":"b","memory":"full source"}),
            json!({"id":"a","memory":"full"}),
            json!({"id":"a"}),
        ] {
            assert!(probes[0].check_get(&value).is_err());
        }
        remember(
            &mut probes,
            &json!({"results":[{"id":"a","memory":"new source"}]}),
        );
        assert_eq!(probes.len(), 1);
        assert!(probes[0]
            .check_get(&json!({"id":"a","memory":"new source"}))
            .is_ok());
    }

    #[test]
    fn histories_require_the_same_memory_and_its_creation() {
        let mut probes = Vec::new();
        remember(
            &mut probes,
            &json!({"results":[{"id":"a","memory":"source"}]}),
        );
        assert!(probes[0]
            .check_history(&json!({"results":[{"id":"a","event":"ADD"}]}))
            .is_ok());
        for value in [
            json!({"results":[]}),
            json!({"results":[{"id":"a","event":"UPDATE"}]}),
            json!({"results":[{"id":"b","event":"ADD"}]}),
            json!({"results":[{"id":"a","event":"ADD"},{"id":"b","event":"ADD"}]}),
        ] {
            assert!(probes[0].check_history(&value).is_err());
        }
    }

    #[test]
    fn unknown_ingestion_is_not_labeled_as_a_clean_read_baseline() {
        assert!(valid_reset_receipt(&json!({"deleted": 0})));
        for value in [
            json!({}),
            json!({"status":{"ok":true}}),
            json!({"deleted":true}),
            json!({"deleted":-1}),
        ] {
            assert!(!valid_reset_receipt(&value));
        }
        let mut probes = Vec::new();
        assert_eq!(baseline_state(false, &probes), "unknown_ingestion_outcome");
        assert_eq!(baseline_state(true, &probes), "no_acknowledged_memory");
        remember(
            &mut probes,
            &json!({"results":[{"id":"a","memory":"source"}]}),
        );
        assert_eq!(baseline_state(false, &probes), "unknown_ingestion_outcome");
        assert_eq!(baseline_state(true, &probes), "acknowledged_ingestion");
    }
}
