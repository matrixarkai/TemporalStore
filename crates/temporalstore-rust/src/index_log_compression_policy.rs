// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Keep compression off the small-record path while bounding large index snapshots on disk.

#[derive(Clone, Copy)]
pub(super) struct CompressionPolicy {
    enabled: bool,
    min_bytes: usize,
    level: i32,
}

impl CompressionPolicy {
    pub(super) fn from_env() -> Self {
        #[cfg(test)]
        if let Some(policy) = TEST_POLICY.with(|slot| slot.get()) {
            return policy;
        }
        let enabled = std::env::var("TS_INDEX_LOG_COMPRESSION_ENABLED").ok();
        let policy = Self::from_values(enabled.as_deref(), None, None);
        if !policy.enabled {
            return policy;
        }
        Self::from_values(
            enabled.as_deref(),
            std::env::var("TS_INDEX_LOG_COMPRESSION_MIN_BYTES")
                .ok()
                .as_deref(),
            std::env::var("TS_INDEX_LOG_COMPRESSION_LEVEL")
                .ok()
                .as_deref(),
        )
    }

    fn from_values(enabled: Option<&str>, min_bytes: Option<&str>, level: Option<&str>) -> Self {
        let enabled = enabled.is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
        Self {
            enabled,
            min_bytes: min_bytes
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(256),
            level: level
                .and_then(|value| value.trim().parse().ok())
                .filter(|level| (1..=22).contains(level))
                .unwrap_or(3),
        }
    }

    pub(super) fn should_compress(&self, bytes: usize) -> bool {
        self.enabled && bytes >= self.min_bytes
    }

    pub(super) fn level(&self) -> i32 {
        self.level
    }
}

#[cfg(test)]
thread_local! {
    static TEST_POLICY: std::cell::Cell<Option<CompressionPolicy>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn with_policy<T>(policy: CompressionPolicy, run: impl FnOnce() -> T) -> T {
    struct Restore(Option<CompressionPolicy>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TEST_POLICY.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(TEST_POLICY.with(|slot| slot.replace(Some(policy))));
    run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_remains_opt_in() {
        let policy = CompressionPolicy::from_values(None, None, None);
        assert!(!policy.should_compress(usize::MAX));
        let enabled = CompressionPolicy::from_values(Some("1"), None, None);
        assert!(!enabled.should_compress(255));
        assert!(enabled.should_compress(256));
        assert_eq!(enabled.level(), 3);
    }

    #[test]
    fn explicit_disable_overrides_any_record_size() {
        for value in ["0", "false", " off ", "no", "invalid", ""] {
            let policy = CompressionPolicy::from_values(Some(value), Some("0"), None);
            assert!(!policy.should_compress(usize::MAX));
        }
    }

    #[test]
    fn explicit_threshold_and_enable_are_preserved() {
        for value in ["1", "true", " YES ", "on"] {
            let policy = CompressionPolicy::from_values(Some(value), Some(" 256 "), Some("3"));
            assert!(!policy.should_compress(255));
            assert!(policy.should_compress(256));
            assert_eq!(policy.level(), 3);
        }
        let invalid = CompressionPolicy::from_values(Some("1"), Some("invalid"), Some("23"));
        assert!(!invalid.should_compress(255));
        assert!(invalid.should_compress(256));
        assert_eq!(invalid.level(), 3);
        for level in ["0", "-1", "bad", "2147483648"] {
            assert_eq!(
                CompressionPolicy::from_values(Some("1"), None, Some(level)).level(),
                3
            );
        }
    }

    #[test]
    fn large_payload_reuses_the_existing_codec_and_both_encoders_agree() {
        use super::super::*;
        for level in ["1", "3", "22"] {
            with_policy(
                CompressionPolicy::from_values(Some("1"), Some("16384"), Some(level)),
                || {
                    let value =
                        serde_json::json!({"payload":"durable memory index ".repeat(8_192)});
                    let payload = encode_index_payload(&value, INDEX_LOG_SHAPE_DELTA).unwrap();
                    let mut reusable = Vec::new();
                    encode_index_payload_into(&value, INDEX_LOG_SHAPE_DELTA, &mut reusable)
                        .unwrap();
                    assert_eq!(payload, reusable);
                    assert_eq!(
                        index_container_parts(payload[0]),
                        (INDEX_LOG_CODEC_MSGPACK_ZSTD, INDEX_LOG_SHAPE_DELTA)
                    );
                    assert!(payload.len() < 4_096);
                    let decoded: serde_json::Value = decode_index_payload(&payload).unwrap();
                    assert_eq!(decoded, value);
                    let mut truncated = payload;
                    truncated.truncate(truncated.len() / 2);
                    assert!(decode_index_payload::<serde_json::Value>(&truncated).is_err());
                },
            );
        }
    }

    #[test]
    fn small_and_large_durable_records_reopen_with_the_same_contents() {
        use super::super::*;
        with_policy(
            CompressionPolicy::from_values(Some("1"), Some("16384"), None),
            || {
                let dir = tempfile::tempdir().unwrap();
                let small = serde_json::json!({"key":"small"});
                let large = serde_json::json!({"key":"large","payload":"retained memory history ".repeat(8_192)});
                {
                    let store = LocalIndexLogStore::new(dir.path());
                    store
                        .append_delta(
                            1,
                            Vec::new(),
                            vec![small.clone()],
                            Some(1),
                            None,
                            false,
                            true,
                        )
                        .unwrap();
                    store
                        .append_delta(
                            1,
                            Vec::new(),
                            vec![large.clone()],
                            Some(2),
                            None,
                            false,
                            true,
                        )
                        .unwrap();
                    assert!(store.log_len_bytes(1) < 4_096);
                }
                let reopened = LocalIndexLogStore::new(dir.path());
                let records = reopened.read_delta_records(1, 0).unwrap();
                assert_eq!(records.len(), 2);
                assert_eq!(records[0].key_states, vec![small]);
                assert_eq!(records[1].key_states, vec![large]);
                assert_eq!(records[1].applied_wal_sequence, Some(2));
                assert_eq!(reopened.stats(1).last_sequence, 2);
                assert_eq!(
                    reopened
                        .append_delta(
                            1,
                            Vec::new(),
                            vec![serde_json::json!({"key":"next"})],
                            Some(3),
                            None,
                            false,
                            true
                        )
                        .unwrap(),
                    3
                );
            },
        );
    }
}
