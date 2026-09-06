// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

pub const TS_CONTEXT_PAGE_TARGET_BYTES: &str = "TS_CONTEXT_PAGE_TARGET_BYTES";
pub const TS_BLOCK_SLAB_TARGET_BYTES: &str = "TS_BLOCK_SLAB_TARGET_BYTES";
pub const TS_STORAGE_ZONE_SIZE: &str = "TS_STORAGE_ZONE_SIZE";
pub const TS_STREAM_MAX_BLOB_SIZE: &str = "TS_STREAM_MAX_BLOB_SIZE";
pub const TS_COMPACTION_WATERMARK_BYTES: &str = "TS_COMPACTION_WATERMARK_BYTES";
pub const TS_COLD_SCAN_NO_CACHE_FILL: &str = "TS_COLD_SCAN_NO_CACHE_FILL";
pub const TS_PAGE_INDEX_CACHE_BYTES: &str = "TS_PAGE_INDEX_CACHE_BYTES";
pub const TS_BLOCK_INDEX_CACHE_BYTES: &str = "TS_BLOCK_INDEX_CACHE_BYTES";
pub const TS_INDEX_DUMP_WAL_GAP_BYTES: &str = "TS_INDEX_DUMP_WAL_GAP_BYTES";
/// Previous name for [`TS_INDEX_DUMP_WAL_GAP_BYTES`], still honoured so a deployment that
/// sets it keeps working. Read only when the current name is unset.
pub const TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME: &str = "TS_INDEX_DUMP_OPLOG_GAP_BYTES";

/// Previous name for [`TS_BLOCK_SLAB_TARGET_BYTES`], still honoured so a deployment that sets it
/// keeps working. Read only when the current name is unset.
///
/// The constant was renamed to `SLAB` some time ago and kept the SEGMENT spelling as its value, so
/// the name in the source and the name an operator sets had drifted apart: every doc, config and
/// conformance list said one thing and the code said another. The variable now spells itself the
/// way the rest of this store spells it -- the data structure is a slab, the accessor is
/// `effective_block_slab_target_bytes`, and the field is `block_slab_target_bytes`.
pub const TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_BLOCK_SEGMENT_TARGET_BYTES";

pub const DEFAULT_CONTEXT_PAGE_TARGET_BYTES: usize = 64 * 1024;
pub const DEFAULT_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 30;
// Match the data-slab seal size (block_slab_target) so one slab maps to one band
// (band_id_for_slab stays 1:1), Mirroring where group_size == zone_size
// (specification) and a zone seals at the device zone_size (~1GiB large mode).
pub const DEFAULT_STORAGE_ZONE_SIZE: u64 = 1 << 30;
pub const DEFAULT_STREAM_MAX_BLOB_SIZE: u64 = 10 * 1024 * 1024;
pub const DEFAULT_COMPACTION_WATERMARK_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_COLD_SCAN_NO_CACHE_FILL: bool = true;
pub const DEFAULT_PAGE_INDEX_CACHE_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BLOCK_INDEX_CACHE_BYTES: u64 = 64 * 1024 * 1024;
// Undumped index-log-gap threshold that triggers a background catalog/index dump under the
// MANIFEST-CONFORMANCE FOLD (TS_INDEX_CATALOG_FOLD). Matches the
// default of 1 MiB.
pub const DEFAULT_INDEX_DUMP_WAL_GAP_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTuningConfig {
    pub context_page_target_bytes: usize,
    #[serde(alias = "block_segment_target_bytes")]
    pub block_slab_target_bytes: u64,
    pub storage_zone_size: u64,
    pub stream_max_blob_size: u64,
    pub compaction_watermark_bytes: u64,
    pub cold_scan_no_cache_fill: bool,
    pub page_index_cache_bytes: u64,
    pub block_index_cache_bytes: u64,
    pub index_dump_wal_gap_bytes: u64,
}

impl Default for StorageTuningConfig {
    fn default() -> Self {
        Self {
            context_page_target_bytes: DEFAULT_CONTEXT_PAGE_TARGET_BYTES,
            block_slab_target_bytes: DEFAULT_BLOCK_SLAB_TARGET_BYTES,
            storage_zone_size: DEFAULT_STORAGE_ZONE_SIZE,
            stream_max_blob_size: DEFAULT_STREAM_MAX_BLOB_SIZE,
            compaction_watermark_bytes: DEFAULT_COMPACTION_WATERMARK_BYTES,
            cold_scan_no_cache_fill: DEFAULT_COLD_SCAN_NO_CACHE_FILL,
            page_index_cache_bytes: DEFAULT_PAGE_INDEX_CACHE_BYTES,
            block_index_cache_bytes: DEFAULT_BLOCK_INDEX_CACHE_BYTES,
            index_dump_wal_gap_bytes: DEFAULT_INDEX_DUMP_WAL_GAP_BYTES,
        }
    }
}

impl StorageTuningConfig {
    pub fn from_env() -> Self {
        Self::from_getter(|name| std::env::var(name).ok())
    }

    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        let defaults = Self::default();
        Self {
            context_page_target_bytes: parse_usize(
                get(TS_CONTEXT_PAGE_TARGET_BYTES),
                defaults.context_page_target_bytes,
            )
            .max(1024),
            block_slab_target_bytes: parse_u64(
                get(TS_BLOCK_SLAB_TARGET_BYTES)
                    .or_else(|| get(TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME)),
                defaults.block_slab_target_bytes,
            )
            .max(1024),
            storage_zone_size: parse_u64(get(TS_STORAGE_ZONE_SIZE), defaults.storage_zone_size)
                .max(1024),
            stream_max_blob_size: parse_u64(
                get(TS_STREAM_MAX_BLOB_SIZE),
                defaults.stream_max_blob_size,
            )
            .max(1024),
            compaction_watermark_bytes: parse_u64(
                get(TS_COMPACTION_WATERMARK_BYTES),
                defaults.compaction_watermark_bytes,
            ),
            cold_scan_no_cache_fill: parse_bool(
                get(TS_COLD_SCAN_NO_CACHE_FILL),
                defaults.cold_scan_no_cache_fill,
            ),
            page_index_cache_bytes: parse_u64(
                get(TS_PAGE_INDEX_CACHE_BYTES),
                defaults.page_index_cache_bytes,
            ),
            block_index_cache_bytes: parse_u64(
                get(TS_BLOCK_INDEX_CACHE_BYTES),
                defaults.block_index_cache_bytes,
            ),
            index_dump_wal_gap_bytes: parse_u64(
                get(TS_INDEX_DUMP_WAL_GAP_BYTES)
                    .or_else(|| get(TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME)),
                defaults.index_dump_wal_gap_bytes,
            ),
        }
    }

    pub fn effective_slab_target_bytes(self) -> u64 {
        // A slab must be able to hold the largest blob it stores, so the max blob
        // size is a FLOOR, not a ceiling. asserts a record fits within a zone
        // (specification), i.e. seal >= max_blob. The previous
        // `.min` inverted this, clamping the 1GiB seal target down to the 10MiB blob
        // size and sealing slabs 100x too small.
        self.block_slab_target_bytes
            .max(self.stream_max_blob_size)
    }

    pub fn env_names() -> [&'static str; 11] {
        [
            TS_CONTEXT_PAGE_TARGET_BYTES,
            TS_BLOCK_SLAB_TARGET_BYTES,
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME,
            TS_STORAGE_ZONE_SIZE,
            TS_STREAM_MAX_BLOB_SIZE,
            TS_COMPACTION_WATERMARK_BYTES,
            TS_COLD_SCAN_NO_CACHE_FILL,
            TS_PAGE_INDEX_CACHE_BYTES,
            TS_BLOCK_INDEX_CACHE_BYTES,
            TS_INDEX_DUMP_WAL_GAP_BYTES,
            TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME,
        ]
    }
}

pub fn context_page_target_bytes() -> usize {
    StorageTuningConfig::from_env().context_page_target_bytes
}

pub fn effective_block_slab_target_bytes() -> u64 {
    StorageTuningConfig::from_env().effective_slab_target_bytes()
}

pub fn storage_zone_size_bytes() -> u64 {
    StorageTuningConfig::from_env().storage_zone_size
}

/// Undumped index-log-gap threshold (bytes) that triggers a background catalog/index dump under
/// the MANIFEST-CONFORMANCE FOLD. Reads the `TS_INDEX_DUMP_WAL_GAP_BYTES` env override -- or the
/// previous name for it, so existing deployments keep working -- falling back to the 1 MiB
/// default. Only consulted when `index_catalog_fold_enabled()`.
pub fn index_dump_wal_gap_bytes() -> u64 {
    StorageTuningConfig::from_env().index_dump_wal_gap_bytes
}

fn parse_u64(value: Option<String>, default: u64) -> u64 {
    value
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_usize(value: Option<String>, default: usize) -> usize {
    value
        .as_deref()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_bool(value: Option<String>, default: bool) -> bool {
    match value.as_deref().map(str::trim).map(str::to_ascii_lowercase) {
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on") => true,
        Some(value) if matches!(value.as_str(), "0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    // shared-corpus: storage_config_like_public_knobs
    fn defaults_match_like_public_surface() {
        let config = StorageTuningConfig::default();
        assert_eq!(
            config.context_page_target_bytes,
            DEFAULT_CONTEXT_PAGE_TARGET_BYTES
        );
        assert_eq!(
            config.block_slab_target_bytes,
            DEFAULT_BLOCK_SLAB_TARGET_BYTES
        );
        assert_eq!(config.storage_zone_size, DEFAULT_STORAGE_ZONE_SIZE);
        assert_eq!(config.stream_max_blob_size, DEFAULT_STREAM_MAX_BLOB_SIZE);
        assert_eq!(
            config.compaction_watermark_bytes,
            DEFAULT_COMPACTION_WATERMARK_BYTES
        );
        assert!(config.cold_scan_no_cache_fill);
        assert_eq!(
            config.page_index_cache_bytes,
            DEFAULT_PAGE_INDEX_CACHE_BYTES
        );
        assert_eq!(
            config.block_index_cache_bytes,
            DEFAULT_BLOCK_INDEX_CACHE_BYTES
        );
        // 1 MiB default.
        assert_eq!(
            config.index_dump_wal_gap_bytes,
            DEFAULT_INDEX_DUMP_WAL_GAP_BYTES
        );
        assert_eq!(config.index_dump_wal_gap_bytes, 1024 * 1024);
    }

    #[test]
    // shared-corpus: storage_config_like_public_knobs
    fn env_names_are_stable_for_rust_parity() {
        assert_eq!(
            StorageTuningConfig::env_names(),
            [
                "TS_CONTEXT_PAGE_TARGET_BYTES",
                "TS_BLOCK_SLAB_TARGET_BYTES",
                "TS_BLOCK_SEGMENT_TARGET_BYTES",
                "TS_STORAGE_ZONE_SIZE",
                "TS_STREAM_MAX_BLOB_SIZE",
                "TS_COMPACTION_WATERMARK_BYTES",
                "TS_COLD_SCAN_NO_CACHE_FILL",
                "TS_PAGE_INDEX_CACHE_BYTES",
                "TS_BLOCK_INDEX_CACHE_BYTES",
                "TS_INDEX_DUMP_WAL_GAP_BYTES",
                "TS_INDEX_DUMP_OPLOG_GAP_BYTES",
            ]
        );
    }

    #[test]
    fn the_previous_slab_env_name_still_configures_the_knob() {
        // A deployment that sets the old name must be unaffected by the rename.
        let env = HashMap::from([(TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME, "2147483648")]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.block_slab_target_bytes, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn the_current_slab_env_name_wins_when_both_are_set() {
        // Precedence has to be defined, or a deployment mid-migration gets whichever the lookup
        // happened to try first.
        let env = HashMap::from([
            (TS_BLOCK_SLAB_TARGET_BYTES, "4294967296"),
            (TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME, "2147483648"),
        ]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.block_slab_target_bytes, 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn neither_slab_env_name_set_falls_back_to_the_default() {
        let env: HashMap<&str, &str> = HashMap::new();
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.block_slab_target_bytes, DEFAULT_BLOCK_SLAB_TARGET_BYTES);
    }

    #[test]
    fn the_previous_gap_env_name_still_configures_the_knob() {
        // A deployment that sets the old name must be unaffected by the rename.
        let env = HashMap::from([(TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME, "4194304")]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.index_dump_wal_gap_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn the_current_gap_env_name_wins_when_both_are_set() {
        // Precedence has to be defined, or a deployment mid-migration gets whichever the
        // lookup happened to try first.
        let env = HashMap::from([
            (TS_INDEX_DUMP_WAL_GAP_BYTES, "8388608"),
            (TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME, "4194304"),
        ]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.index_dump_wal_gap_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn neither_gap_env_name_set_falls_back_to_the_default() {
        let env: HashMap<&str, &str> = HashMap::new();
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(
            config.index_dump_wal_gap_bytes,
            DEFAULT_INDEX_DUMP_WAL_GAP_BYTES
        );
    }

    #[test]
    // shared-corpus: storage_config_like_public_knobs
    fn parses_public_knobs_from_getter() {
        let env = HashMap::from([
            (TS_CONTEXT_PAGE_TARGET_BYTES, "32768"),
            (TS_BLOCK_SLAB_TARGET_BYTES, "10485760"),
            (TS_STORAGE_ZONE_SIZE, "10485760"),
            (TS_STREAM_MAX_BLOB_SIZE, "8388608"),
            (TS_COMPACTION_WATERMARK_BYTES, "4096"),
            (TS_COLD_SCAN_NO_CACHE_FILL, "false"),
            (TS_PAGE_INDEX_CACHE_BYTES, "2097152"),
            (TS_BLOCK_INDEX_CACHE_BYTES, "4194304"),
            (TS_INDEX_DUMP_WAL_GAP_BYTES, "2097152"),
        ]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.context_page_target_bytes, 32 * 1024);
        assert_eq!(config.block_slab_target_bytes, 10 * 1024 * 1024);
        assert_eq!(config.storage_zone_size, 10 * 1024 * 1024);
        assert_eq!(config.stream_max_blob_size, 8 * 1024 * 1024);
        // Seal = max(block_slab_target 10MiB, max_blob 8MiB) = 10MiB (blob is a floor).
        assert_eq!(config.effective_slab_target_bytes(), 10 * 1024 * 1024);
        assert_eq!(config.compaction_watermark_bytes, 4096);
        assert!(!config.cold_scan_no_cache_fill);
        assert_eq!(config.page_index_cache_bytes, 2 * 1024 * 1024);
        assert_eq!(config.block_index_cache_bytes, 4 * 1024 * 1024);
        assert_eq!(config.index_dump_wal_gap_bytes, 2 * 1024 * 1024);
    }
}
