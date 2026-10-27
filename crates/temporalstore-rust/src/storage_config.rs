// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

pub const TS_CONTEXT_BLOCK_TARGET_BYTES: &str = "TS_CONTEXT_BLOCK_TARGET_BYTES";
/// Previous name for [`TS_CONTEXT_BLOCK_TARGET_BYTES`], still honoured so a deployment that
/// sets it keeps working. Read only when the current name is unset.
///
/// The constant was renamed to `BLOCK` and kept the `PAGE` spelling as its VALUE, so the name in
/// the source and the name an operator sets had drifted apart -- the same drift
/// [`TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME`] below was written to close.
pub const TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_CONTEXT_PAGE_TARGET_BYTES";
pub const TS_BLOCK_SLAB_TARGET_BYTES: &str = "TS_BLOCK_SLAB_TARGET_BYTES";
pub const TS_STREAM_MAX_BLOB_SIZE: &str = "TS_STREAM_MAX_BLOB_SIZE";
pub const TS_COMPACTION_WATERMARK_BYTES: &str = "TS_COMPACTION_WATERMARK_BYTES";
pub const TS_COLD_SCAN_NO_CACHE_FILL: &str = "TS_COLD_SCAN_NO_CACHE_FILL";
// RETIRED: TS_PAGE_INDEX_CACHE_BYTES and TS_BLOCK_INDEX_CACHE_BYTES.
//
// Both named a budget for an index cache that does not exist: an address lookup is answered
// from the DRAM-resident index with no cache call, and the record cache is bounded by
// TS_CACHE_MEMORY_BYTES. Each parsed into a setting whose only production reader put it in the
// tuning report, so setting either moved one reported number and nothing else -- measured two
// ways before removal, with both set to 1 byte and with a panic planted on the reads.
//
// Nothing reads either name now, so a deployment that still sets one is accepted and ignored
// rather than refused: there is no read to fail. The shipped config loader behaves the same way
// and was measured doing it -- an unmapped key is reported `unmapped config keys (ignored)` and
// the process exits 0. Held true by `the_retired_index_cache_budgets_are_read_by_nothing`.
pub const TS_INDEX_DUMP_WAL_GAP_BYTES: &str = "TS_INDEX_DUMP_WAL_GAP_BYTES";
/// Previous name for [`TS_INDEX_DUMP_WAL_GAP_BYTES`], still honoured so a deployment that
/// sets it keeps working. Read only when the current name is unset.
pub const TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME: &str = "TS_INDEX_DUMP_OPLOG_GAP_BYTES";
/// Shortest time between two catalog dumps of the same shard. The byte gap says a dump is
/// WORTH doing; this says it is not worth doing AGAIN yet.
pub const TS_INDEX_DUMP_MIN_INTERVAL_MS: &str = "TS_INDEX_DUMP_MIN_INTERVAL_MS";
/// How many reclaimable bytes the index log must be carrying before the post-dump sweep is worth
/// rewriting it for.
pub const TS_INDEX_GC_MIN_RECLAIMABLE_BYTES: &str = "TS_INDEX_GC_MIN_RECLAIMABLE_BYTES";

/// Previous name for [`TS_BLOCK_SLAB_TARGET_BYTES`], still honoured so a deployment that sets it
/// keeps working. Read only when the current name is unset.
///
/// The constant was renamed to `SLAB` some time ago and kept the SEGMENT spelling as its value, so
/// the name in the source and the name an operator sets had drifted apart: every doc, config and
/// conformance list said one thing and the code said another. The variable now spells itself the
/// way the rest of this store spells it -- the data structure is a slab, the accessor is
/// `effective_block_slab_target_bytes`, and the field is `block_slab_target_bytes`.
pub const TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_BLOCK_SEGMENT_TARGET_BYTES";

pub const DEFAULT_CONTEXT_BLOCK_TARGET_BYTES: usize = 64 * 1024;
pub const DEFAULT_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 30;

/// THE CEILING THAT MAKES AN OFFSET FIT IN 32 BITS, AND WHY IT IS EXACTLY THIS NUMBER.
///
/// A block address carries its slab and its offset inside that slab in ONE 64-bit word, the
/// slab in the high half and the offset in the low half -- see
/// [`crate::block_store::make_block_address_word`]. The low half holds `0 ..= u32::MAX`, so the
/// engine needs a bound on how large a recorded offset can get, and the only thing that bounds
/// it is this knob.
///
/// The bound is not "the target", it is "the target minus one", and that is a fact about the
/// roll predicate rather than about arithmetic. `block_store::should_roll_before_append` is
/// `write_offset > 0 && write_offset.saturating_add(record_len) > slab_target_bytes`, and every
/// append on both write paths goes through it. So a record is recorded at an offset `W` only
/// when `W == 0` or `W + record_len <= target`; `encode_block_record` always emits at least
/// `BLOCK_RECORD_HEADER_LEN` bytes, so `record_len >= 1` and therefore `W <= target - 1`. An
/// offset can never EQUAL the target.
///
/// That makes `1 << 32` the largest target that still fits: the highest offset a store with
/// that target can record is `2^32 - 1`, which is exactly `u32::MAX`. One byte more of target
/// and the low half overflows.
///
/// This is a REAL REDUCTION IN WHAT AN OPERATOR MAY SET. The knob was a `u64` and accepted any
/// value; a store configured above this now REFUSES TO START rather than recording an address
/// that points somewhere else. It is checked, never clamped: a clamp would turn the
/// misconfiguration into a wrong address later, which is the failure mode this exists to
/// prevent.
pub const MAX_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 32;
pub const DEFAULT_STREAM_MAX_BLOB_SIZE: u64 = 10 * 1024 * 1024;
pub const DEFAULT_COMPACTION_WATERMARK_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_COLD_SCAN_NO_CACHE_FILL: bool = true;
// Undumped index-log-gap threshold that triggers a background catalog/index dump under the
// MANIFEST-CONFORMANCE FOLD (TS_INDEX_CATALOG_FOLD). Matches the
// default of 1 MiB.
pub const DEFAULT_INDEX_DUMP_WAL_GAP_BYTES: u64 = 1024 * 1024;
/// Shortest time between two catalog dumps of the same shard, in milliseconds.
///
/// The byte gap alone leaves the cadence in the hands of whoever calls the threshold check. One
/// caller has a cadence of its own -- the proxy's reclaim thread polls on a timer -- and the
/// other, the background storage cycle, has none: it checks on every round it runs. So under a
/// write burst the gap can be crossed again immediately and the dump fires back to back, and a
/// dump is not cheap: it serializes the whole served index, writes it durably, appends a folded
/// anchor and then rewrites both logs.
///
/// A floor in TIME belongs with the threshold rather than with its callers, so that a caller
/// that polls quickly cannot dump quickly. Waiting also makes each dump worth MORE: the deltas
/// that arrive during the wait fold into the same base rather than into the next one -- the
/// same reason the design being followed delays a dump until its log has accumulated.
///
/// 1.5 s, against the 1 s that design allows between two index-meta updates. Half a second more
/// because what we do on a dump is the heavier of the two -- a whole-index serialize where
/// theirs writes one meta record -- and because 1.5 s still bounds the extra log growth this
/// costs to one and a half seconds of writes.
pub const DEFAULT_INDEX_DUMP_MIN_INTERVAL_MS: u64 = 1_500;
/// Reclaimable bytes the index log must hold before the post-dump sweep rewrites it.
///
/// The sweep reads the whole log, decides which records the durable base already reflects, and
/// writes the survivors to a temp file which it fsyncs and renames over the original. The READ
/// is what decides; the write, the barrier and the rename are what it costs. When little is
/// reclaimable that cost buys almost nothing, and it is paid on every dump.
///
/// 768 KiB, against the 1 MiB the design being followed requires before its index GC runs. Below
/// its own threshold that design skips the round entirely, for the same reason.
///
/// A zero disables the threshold, so any reclaimable byte at all triggers a rewrite. Nothing
/// reclaimable NEVER triggers one regardless: rewriting a log while retaining every record
/// produces the same bytes it started with, so that is arithmetic rather than policy.
pub const DEFAULT_INDEX_GC_MIN_RECLAIMABLE_BYTES: u64 = 768 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTuningConfig {
    pub context_block_target_bytes: usize,
    #[serde(alias = "block_segment_target_bytes")]
    pub block_slab_target_bytes: u64,
    pub stream_max_blob_size: u64,
    pub compaction_watermark_bytes: u64,
    pub cold_scan_no_cache_fill: bool,
    pub index_dump_wal_gap_bytes: u64,
    pub index_dump_min_interval_ms: u64,
    pub index_gc_min_reclaimable_bytes: u64,
}

impl Default for StorageTuningConfig {
    fn default() -> Self {
        Self {
            context_block_target_bytes: DEFAULT_CONTEXT_BLOCK_TARGET_BYTES,
            block_slab_target_bytes: DEFAULT_BLOCK_SLAB_TARGET_BYTES,
            stream_max_blob_size: DEFAULT_STREAM_MAX_BLOB_SIZE,
            compaction_watermark_bytes: DEFAULT_COMPACTION_WATERMARK_BYTES,
            cold_scan_no_cache_fill: DEFAULT_COLD_SCAN_NO_CACHE_FILL,
            index_dump_wal_gap_bytes: DEFAULT_INDEX_DUMP_WAL_GAP_BYTES,
            index_dump_min_interval_ms: DEFAULT_INDEX_DUMP_MIN_INTERVAL_MS,
            index_gc_min_reclaimable_bytes: DEFAULT_INDEX_GC_MIN_RECLAIMABLE_BYTES,
        }
    }
}

impl StorageTuningConfig {
    pub fn from_env() -> Self {
        Self::from_getter(|name| std::env::var(name).ok())
    }

    /// Read the configuration and REFUSE one that cannot be addressed.
    ///
    /// [`Self::from_getter`] panics on the same condition; this is the same check with the
    /// failure returned rather than raised, so a test can feed an out-of-range value and read
    /// the message instead of unwinding.
    pub fn try_from_getter(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let config = Self::parse(get);
        config.validate()?;
        Ok(config)
    }

    /// Whether this configuration can be addressed at all.
    ///
    /// The effective target -- not the raw knob -- is what the roll predicate compares against,
    /// and it is `block_slab_target_bytes.max(stream_max_blob_size)`. So BOTH knobs can push it
    /// over the ceiling and both are named in the failure, because an operator who set only the
    /// blob size within bounds would otherwise be told about a slab target they never touched.
    pub fn validate(&self) -> Result<(), String> {
        let effective = self.effective_slab_target_bytes();
        if effective > MAX_BLOCK_SLAB_TARGET_BYTES {
            return Err(format!(
                "{TS_BLOCK_SLAB_TARGET_BYTES}={} with {TS_STREAM_MAX_BLOB_SIZE}={} gives an \
                 effective slab target of {effective} bytes, above the {MAX_BLOCK_SLAB_TARGET_BYTES} \
                 byte ceiling a block address can name. A block address holds its offset in 32 \
                 bits, so a store with this target could record an offset that does not fit and \
                 would resolve to the wrong block. Lower the target to {MAX_BLOCK_SLAB_TARGET_BYTES} \
                 bytes or less; this is refused at startup rather than clamped, because a clamp \
                 would surface as a mis-addressed read long after the configuration was set",
                self.block_slab_target_bytes, self.stream_max_blob_size
            ));
        }
        Ok(())
    }

    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        let config = Self::parse(get);
        if let Err(message) = config.validate() {
            panic!("{message}");
        }
        config
    }

    fn parse(get: impl Fn(&str) -> Option<String>) -> Self {
        // A variable that is PRESENT AND BLANK is not a value. Without this, `get(NEW)` answers
        // `Some("")`, `or_else` never reaches the previous name below, and a deployment that set
        // the older spelling correctly gets the built-in default instead of what it asked for.
        let get = |name: &str| get(name).filter(|value| !value.trim().is_empty());
        let defaults = Self::default();
        Self {
            context_block_target_bytes: parse_usize(
                get(TS_CONTEXT_BLOCK_TARGET_BYTES)
                    .or_else(|| get(TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME)),
                defaults.context_block_target_bytes,
            )
            .max(1024),
            block_slab_target_bytes: parse_u64(
                get(TS_BLOCK_SLAB_TARGET_BYTES)
                    .or_else(|| get(TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME)),
                defaults.block_slab_target_bytes,
            )
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
            index_dump_wal_gap_bytes: parse_u64(
                get(TS_INDEX_DUMP_WAL_GAP_BYTES)
                    .or_else(|| get(TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME)),
                defaults.index_dump_wal_gap_bytes,
            ),
            index_dump_min_interval_ms: parse_u64(
                get(TS_INDEX_DUMP_MIN_INTERVAL_MS),
                defaults.index_dump_min_interval_ms,
            ),
            index_gc_min_reclaimable_bytes: parse_u64(
                get(TS_INDEX_GC_MIN_RECLAIMABLE_BYTES),
                defaults.index_gc_min_reclaimable_bytes,
            ),
        }
    }

    pub fn effective_slab_target_bytes(self) -> u64 {
        // A slab must be able to hold the largest blob it stores, so the max blob
        // size is a FLOOR, not a ceiling. asserts a record fits within a slab
        // (specification), i.e. seal >= max_blob. The previous
        // `.min` inverted this, clamping the 1GiB seal target down to the 10MiB blob
        // size and sealing slabs 100x too small.
        self.block_slab_target_bytes
            .max(self.stream_max_blob_size)
    }

    pub fn env_names() -> [&'static str; 11] {
        [
            TS_CONTEXT_BLOCK_TARGET_BYTES,
            TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME,
            TS_BLOCK_SLAB_TARGET_BYTES,
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME,
            TS_STREAM_MAX_BLOB_SIZE,
            TS_COMPACTION_WATERMARK_BYTES,
            TS_COLD_SCAN_NO_CACHE_FILL,
            TS_INDEX_DUMP_WAL_GAP_BYTES,
            TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME,
            TS_INDEX_DUMP_MIN_INTERVAL_MS,
            TS_INDEX_GC_MIN_RECLAIMABLE_BYTES,
        ]
    }
}

pub fn context_block_target_bytes() -> usize {
    StorageTuningConfig::from_env().context_block_target_bytes
}

pub fn effective_block_slab_target_bytes() -> u64 {
    StorageTuningConfig::from_env().effective_slab_target_bytes()
}

/// Undumped index-log threshold (bytes) that triggers a background catalog/index dump under
/// the MANIFEST-CONFORMANCE FOLD. Reads the `TS_INDEX_DUMP_WAL_GAP_BYTES` env override -- or the
/// previous name for it, so existing deployments keep working -- falling back to the 1 MiB
/// default. Only consulted by the catalog fold's threshold dump.
///
/// This value is a FLOOR, not the threshold itself. A dump writes the whole served index, so its
/// cost is the store while what it releases is only what accrued since the previous dump; against
/// a constant value the two are unrelated and bytes written per byte released grows with the
/// store. `index_log::effective_index_dump_threshold_bytes` raises this floor to
/// `base_index / INDEX_DUMP_BASE_FRACTION_DIVISOR` once the base index is large enough for that to
/// bind, which is what keeps the two in proportion. A store whose base index is smaller than
/// `divisor` times this value keeps exactly the cadence it has today, and a zero here still
/// disables threshold dumping entirely.
pub fn index_dump_wal_gap_bytes() -> u64 {
    StorageTuningConfig::from_env().index_dump_wal_gap_bytes
}

/// Shortest time (ms) between two catalog dumps of the same shard. A zero disables the floor,
/// which is what every test that is not exercising the timer passes.
pub fn index_dump_min_interval_ms() -> u64 {
    StorageTuningConfig::from_env().index_dump_min_interval_ms
}

/// Reclaimable bytes the index log must hold before the post-dump sweep rewrites it. A zero
/// disables the threshold, which is what a test that is not exercising it passes.
pub fn index_gc_min_reclaimable_bytes() -> u64 {
    StorageTuningConfig::from_env().index_gc_min_reclaimable_bytes
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
    value
        .as_deref()
        .and_then(crate::env_flag::parse_bool)
        .unwrap_or(default)
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
            config.context_block_target_bytes,
            DEFAULT_CONTEXT_BLOCK_TARGET_BYTES
        );
        assert_eq!(
            config.block_slab_target_bytes,
            DEFAULT_BLOCK_SLAB_TARGET_BYTES
        );
        assert_eq!(config.stream_max_blob_size, DEFAULT_STREAM_MAX_BLOB_SIZE);
        assert_eq!(
            config.compaction_watermark_bytes,
            DEFAULT_COMPACTION_WATERMARK_BYTES
        );
        assert!(config.cold_scan_no_cache_fill);
        // 1 MiB default.
        assert_eq!(
            config.index_dump_wal_gap_bytes,
            DEFAULT_INDEX_DUMP_WAL_GAP_BYTES
        );
        assert_eq!(config.index_dump_wal_gap_bytes, 1024 * 1024);
        assert_eq!(config.index_dump_min_interval_ms, 1_500);
        assert_eq!(config.index_gc_min_reclaimable_bytes, 768 * 1024);
    }

    #[test]
    // shared-corpus: storage_config_like_public_knobs
    fn env_names_are_stable_for_rust_parity() {
        assert_eq!(
            StorageTuningConfig::env_names(),
            [
                "TS_CONTEXT_BLOCK_TARGET_BYTES",
                "TS_CONTEXT_PAGE_TARGET_BYTES",
                "TS_BLOCK_SLAB_TARGET_BYTES",
                "TS_BLOCK_SEGMENT_TARGET_BYTES",
                "TS_STREAM_MAX_BLOB_SIZE",
                "TS_COMPACTION_WATERMARK_BYTES",
                "TS_COLD_SCAN_NO_CACHE_FILL",
                "TS_INDEX_DUMP_WAL_GAP_BYTES",
                "TS_INDEX_DUMP_OPLOG_GAP_BYTES",
                "TS_INDEX_DUMP_MIN_INTERVAL_MS",
                "TS_INDEX_GC_MIN_RECLAIMABLE_BYTES",
            ]
        );
    }

    /// A SLAB TARGET ABOVE THE CEILING IS REFUSED AT LOAD, NOT CLAMPED.
    ///
    /// The distinction is the whole point of this check. A clamp would accept the configuration,
    /// serve happily at a target the operator did not set, and turn into a mis-addressed read
    /// months later; a refusal is visible at the moment the configuration is wrong. So this
    /// asserts BOTH that the load fails and that nothing came back holding a clamped value.
    #[test]
    fn a_slab_target_above_the_addressable_ceiling_is_refused() {
        let over = HashMap::from([(
            TS_BLOCK_SLAB_TARGET_BYTES,
            (MAX_BLOCK_SLAB_TARGET_BYTES + 1).to_string(),
        )]);
        let refused =
            StorageTuningConfig::try_from_getter(|name| over.get(name).map(|v| v.to_string()))
                .expect_err("a slab target above the ceiling must be refused");
        assert!(
            refused.contains("effective slab target") && refused.contains("32 bits"),
            "the refusal does not say why: {refused}"
        );

        // THE BOUNDARY, not just a large number: the ceiling itself loads.
        let at = HashMap::from([(
            TS_BLOCK_SLAB_TARGET_BYTES,
            MAX_BLOCK_SLAB_TARGET_BYTES.to_string(),
        )]);
        let accepted =
            StorageTuningConfig::try_from_getter(|name| at.get(name).map(|v| v.to_string()))
                .expect("the ceiling itself must load");
        assert_eq!(accepted.block_slab_target_bytes, MAX_BLOCK_SLAB_TARGET_BYTES);

        // THE OTHER KNOB THAT REACHES THE SAME TARGET. `effective_slab_target_bytes` takes the
        // MAXIMUM of the slab target and the blob size, so a blob size alone can push the
        // effective target over the ceiling with the slab target untouched at its default. A
        // check that read only the slab target would miss this entirely.
        let by_blob = HashMap::from([(
            TS_STREAM_MAX_BLOB_SIZE,
            (MAX_BLOCK_SLAB_TARGET_BYTES + 1).to_string(),
        )]);
        let refused_by_blob =
            StorageTuningConfig::try_from_getter(|name| by_blob.get(name).map(|v| v.to_string()))
                .expect_err("a blob size above the ceiling must be refused too");
        assert!(
            refused_by_blob.contains(TS_STREAM_MAX_BLOB_SIZE),
            "the refusal does not name the knob that was actually set: {refused_by_blob}"
        );

        // And the shipped default is nowhere near it, so no deployment that left this alone is
        // affected.
        let default = StorageTuningConfig::default();
        assert!(
            default.validate().is_ok(),
            "the shipped default does not pass its own check"
        );
        assert!(default.effective_slab_target_bytes() <= MAX_BLOCK_SLAB_TARGET_BYTES / 4);
    }

    /// The panicking spelling raises what the checked one returns.
    #[test]
    #[should_panic(expected = "effective slab target")]
    fn from_getter_panics_on_an_unaddressable_slab_target() {
        let over = HashMap::from([(
            TS_BLOCK_SLAB_TARGET_BYTES,
            (MAX_BLOCK_SLAB_TARGET_BYTES * 2).to_string(),
        )]);
        let _ = StorageTuningConfig::from_getter(|name| over.get(name).map(|v| v.to_string()));
    }

    #[test]
    fn the_previous_context_target_env_name_still_configures_the_knob() {
        // The rename this milestone makes, and the reason it is not a clean break. Nothing
        // errors when a variable stops being read: the knob turns OFF, the default applies, and
        // a deployment sized for 32 KiB blocks silently starts cutting 64 KiB ones.
        let previous = HashMap::from([(TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME, "32768")]);
        let config =
            StorageTuningConfig::from_getter(|name| previous.get(name).map(|v| v.to_string()));
        assert_eq!(
            config.context_block_target_bytes, 32768,
            "the previous spelling must still configure the knob, not fall back to the default"
        );
        assert_ne!(
            config.context_block_target_bytes, DEFAULT_CONTEXT_BLOCK_TARGET_BYTES,
            "a value read from the previous name must differ from the default, or this proves \
             nothing"
        );

        // And the current spelling wins over it.
        let both = HashMap::from([
            (TS_CONTEXT_BLOCK_TARGET_BYTES, "16384"),
            (TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME, "32768"),
        ]);
        let config = StorageTuningConfig::from_getter(|name| both.get(name).map(|v| v.to_string()));
        assert_eq!(config.context_block_target_bytes, 16384);
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
    fn a_slab_target_under_the_blob_ceiling_does_not_reach_the_write_path() {
        // The case `parses_public_knobs_from_getter` cannot see. It sets slab 10 MiB against blob
        // 8 MiB -- the slab wins, so the floor below never fires and that assertion holds whether
        // or not this clamp exists at all.
        //
        // `block_store/append.rs` and `block_store.rs` seal at
        // `effective_block_slab_target_bytes()`, so this, not the field, is what an operator's
        // value has to survive.
        for written in ["1024", "65536", "1048576", "10485759"] {
            let config = StorageTuningConfig::from_getter(|name| {
                if name == TS_BLOCK_SLAB_TARGET_BYTES {
                    Some(written.to_string())
                } else {
                    None
                }
            });
            assert_eq!(
                written.parse::<u64>().unwrap(),
                config.block_slab_target_bytes,
                "the field should hold what was written"
            );
            assert_eq!(
                DEFAULT_STREAM_MAX_BLOB_SIZE,
                config.effective_slab_target_bytes(),
                "{written} is under the blob ceiling, so the write path must use the ceiling"
            );
        }
    }

    #[test]
    fn the_blob_ceiling_moves_the_slab_target_with_it() {
        // The other direction, and the reason neither page entry could be written without naming
        // the other: TS_STREAM_MAX_BLOB_SIZE calls itself a ceiling on a payload, and is also a
        // floor on the reclaim unit.
        let config = StorageTuningConfig::from_getter(|name| {
            if name == TS_BLOCK_SLAB_TARGET_BYTES {
                Some("1073741824".to_string())
            } else if name == TS_STREAM_MAX_BLOB_SIZE {
                Some("2147483648".to_string())
            } else {
                None
            }
        });
        assert_eq!(1_073_741_824, config.block_slab_target_bytes);
        assert_eq!(2_147_483_648, config.effective_slab_target_bytes());
    }

    #[test]
    // shared-corpus: storage_config_like_public_knobs
    fn parses_public_knobs_from_getter() {
        let env = HashMap::from([
            (TS_CONTEXT_BLOCK_TARGET_BYTES, "32768"),
            (TS_BLOCK_SLAB_TARGET_BYTES, "10485760"),
            (TS_STREAM_MAX_BLOB_SIZE, "8388608"),
            (TS_COMPACTION_WATERMARK_BYTES, "4096"),
            (TS_COLD_SCAN_NO_CACHE_FILL, "false"),
            (TS_INDEX_DUMP_WAL_GAP_BYTES, "2097152"),
        ]);
        let config = StorageTuningConfig::from_getter(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(config.context_block_target_bytes, 32 * 1024);
        assert_eq!(config.block_slab_target_bytes, 10 * 1024 * 1024);
        assert_eq!(config.stream_max_blob_size, 8 * 1024 * 1024);
        // Seal = max(block_slab_target 10MiB, max_blob 8MiB) = 10MiB (blob is a floor).
        assert_eq!(config.effective_slab_target_bytes(), 10 * 1024 * 1024);
        assert_eq!(config.compaction_watermark_bytes, 4096);
        assert!(!config.cold_scan_no_cache_fill);
        assert_eq!(config.index_dump_wal_gap_bytes, 2 * 1024 * 1024);
    }

    /// Every use of a name, by SHAPE rather than by needle. Continuation lines are joined onto
    /// their receiver first, so a field access sitting on its own line is seen -- a
    /// line-at-a-time matcher reports those as absent, which is a clean pass over a scan that
    /// cannot see half its subject. Prose is skipped. The match is whole-word, so a longer
    /// identifier that merely starts with the name is a different identifier.
    ///
    /// Any use counts, read or write: after the retirement below there is no legitimate one.
    ///
    /// Returns `(line, name)` per use.
    fn uses_of(text: &str, names: &[&str]) -> Vec<(usize, String)> {
        let mut joined: Vec<(usize, String)> = Vec::new();
        for (index, raw) in text.lines().enumerate() {
            let trimmed = raw.trim();
            if trimmed.starts_with('.') && !trimmed.starts_with("..") {
                if let Some((_, previous)) = joined.last_mut() {
                    previous.push_str(trimmed);
                    continue;
                }
            }
            joined.push((index + 1, trimmed.to_string()));
        }

        let ident = |c: char| c.is_alphanumeric() || c == '_';
        let mut found = Vec::new();
        for (number, line) in joined {
            if line.starts_with("//") || line.starts_with("/*") || line.starts_with('*') {
                continue;
            }
            for name in names {
                let mut from = 0usize;
                while let Some(offset) = line[from..].find(name) {
                    let at = from + offset;
                    let before = line[..at].chars().next_back();
                    let after = line[at + name.len()..].chars().next();
                    let bounded = !matches!(before, Some(c) if ident(c))
                        && !matches!(after, Some(c) if ident(c));
                    if bounded {
                        found.push((number, (*name).to_string()));
                    }
                    from = at + name.len();
                }
            }
        }
        found
    }

    /// The six names the index-cache budgets were spelled with. All retired.
    const RETIRED_INDEX_CACHE_NAMES: [&str; 6] = [
        "page_index_cache_bytes",
        "block_index_cache_bytes",
        "TS_PAGE_INDEX_CACHE_BYTES",
        "TS_BLOCK_INDEX_CACHE_BYTES",
        "DEFAULT_PAGE_INDEX_CACHE_BYTES",
        "DEFAULT_BLOCK_INDEX_CACHE_BYTES",
    ];

    /// A knob of the same shape that IS live, used as the positive control below.
    const LIVE_SIBLING_NAMES: [&str; 3] = [
        "compaction_watermark_bytes",
        "TS_COMPACTION_WATERMARK_BYTES",
        "DEFAULT_COMPACTION_WATERMARK_BYTES",
    ];

    /// THE RETIRED INDEX-CACHE BUDGETS ARE READ BY NOTHING.
    ///
    /// `TS_PAGE_INDEX_CACHE_BYTES` and `TS_BLOCK_INDEX_CACHE_BYTES` named a budget for a cache
    /// that does not exist. Before the retirement this guard asserted a reader set of FOUR --
    /// two fallback reads here and two `values.insert` calls in the tuning report -- and the
    /// claim it now has to hold is that the set is EMPTY. A guard whose population empties has
    /// to say so rather than stop checking, so the assertion inverted instead of going away.
    ///
    /// AN EMPTY SET IS THE ASSERTION A BROKEN SCAN PASSES BY DEFAULT, which is why it is not the
    /// only one here. Three things carry it:
    ///
    /// - the denominators: 400+ files, 200,000+ production lines, 50+ files cut at
    ///   `#[cfg(test)]`, each asserted, so a walk that read nothing fails rather than passes;
    /// - a POSITIVE CONTROL over the same corpus with the same matcher, on
    ///   `compaction_watermark_bytes` -- a knob of identical shape that IS live. It must be
    ///   found, several times. If the matcher ever stops seeing its subject, the control goes
    ///   empty and this fails, instead of the retired set reading clean for the wrong reason;
    /// - `a_reinstated_index_cache_budget_would_fail_this_guard`, the negative control, which
    ///   feeds the matcher a re-added read, a re-added write and a re-added constant and
    ///   requires it to see all three.
    ///
    /// Why the names and not just the fields: the settings, the two `TS_*` variables and the two
    /// `DEFAULT_*` constants were all removed, so any of the six reappearing in production code
    /// is the retirement being undone. A deployment that still exports either variable is
    /// accepted and ignored -- nothing reads the name, so there is no read to fail -- and the
    /// shipped config loader was measured doing the same with an unmapped key.
    #[test]
    fn the_retired_index_cache_budgets_are_read_by_nothing() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = 0usize;
        let mut lines = 0usize;
        let mut truncated = 0usize;
        let mut retired: Vec<(String, usize, String)> = Vec::new();
        let mut control: Vec<(String, usize, String)> = Vec::new();

        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let Ok(display) = path.strip_prefix(&root) else {
                    continue;
                };
                let display = display.to_string_lossy().replace('\\', "/");
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                // A test item in a production FILE is still a test. Each file is cut at its
                // first `#[cfg(test)]`, which is also what keeps this module's own mentions of
                // the retired names out of the production count. The cuts are counted.
                let production = match text.find("#[cfg(test)]") {
                    Some(at) => {
                        truncated += 1;
                        &text[..at]
                    }
                    None => &text[..],
                };
                files += 1;
                lines += production.lines().count();
                for (number, name) in uses_of(production, &RETIRED_INDEX_CACHE_NAMES) {
                    retired.push((display.clone(), number, name));
                }
                for (number, name) in uses_of(production, &LIVE_SIBLING_NAMES) {
                    control.push((display.clone(), number, name));
                }
            }
        }

        retired.sort();
        control.sort();
        for (file, number, name) in &retired {
            println!("  RETIRED-NAME STILL USED  {file}:{number}  {name}");
        }
        println!(
            "  files {files}, production lines {lines}, cut at #[cfg(test)] {truncated}; \
             retired uses {}, live-sibling uses {}",
            retired.len(),
            control.len()
        );

        // Denominators before verdicts.
        assert!(
            files >= 400,
            "the walk read {files} source files; this guard would be passing over nothing"
        );
        assert!(
            lines >= 200_000,
            "the walk read {lines} production lines, which is not this crate"
        );
        assert!(
            truncated >= 50,
            "only {truncated} files were cut at #[cfg(test)]; the cut has stopped matching and \
             this scan is now counting test items as production"
        );

        // The positive control: the same matcher, the same corpus, a knob that IS live. Without
        // this, a matcher that had stopped seeing anything would report the retirement clean.
        assert!(
            control.len() >= 4,
            "the positive control found only {} uses of a knob that is live, so the matcher is \
             no longer seeing its subject and the empty result below means nothing",
            control.len()
        );

        let offenders: Vec<String> = retired
            .iter()
            .map(|(file, number, name)| format!("{file}:{number}  {name}"))
            .collect();
        assert!(
            offenders.is_empty(),
            "the index-cache budgets were retired because neither bounded a cache, and these \
             production sites use one of their names again. If a real cache is being bounded, \
             that is a fine change -- reinstate the setting deliberately and update this guard, \
             README.md, crates/temporalstore-rust/README.md, \
             docs/temporalstore_page_block_address_contract.md and config/temporalstore.toml, \
             which all now say no such budget exists:\n{}",
            offenders.join("\n")
        );
    }

    /// The negative control for the guard above: the matcher must see a reinstated name in each
    /// of the three shapes it could come back in. Without this, a matcher that had stopped
    /// matching anything would report the same clean result as a tree with nothing wrong in it.
    #[test]
    fn a_reinstated_index_cache_budget_would_fail_this_guard() {
        // A read split across lines, which is the shape a line-at-a-time matcher misses.
        let read = "fn size_the_index_cache(tuning: StorageTuningConfig) -> u64 {\n\
                    \x20   tuning\n\
                    \x20       .page_index_cache_bytes\n\
                    }\n";
        assert_eq!(
            uses_of(read, &RETIRED_INDEX_CACHE_NAMES),
            vec![(2usize, "page_index_cache_bytes".to_string())],
            "a reinstated read must be seen, and on the receiver's line"
        );

        // A write, which after the retirement is just as much a reinstatement as a read.
        let write = "        Self {\n            block_index_cache_bytes: 0,\n        }\n";
        assert_eq!(
            uses_of(write, &RETIRED_INDEX_CACHE_NAMES),
            vec![(2usize, "block_index_cache_bytes".to_string())],
            "a reinstated struct-literal field must be seen"
        );

        // The variable name coming back.
        let constant = "        pub const TS_PAGE_INDEX_CACHE_BYTES: &str = \"x\";\n";
        assert_eq!(
            uses_of(constant, &RETIRED_INDEX_CACHE_NAMES),
            vec![(1usize, "TS_PAGE_INDEX_CACHE_BYTES".to_string())],
            "a reinstated variable constant must be seen"
        );

        // And what must NOT be seen: prose, and a longer identifier that starts with the name.
        assert!(
            uses_of(
                "    // page_index_cache_bytes was retired; nothing reads it.\n",
                &RETIRED_INDEX_CACHE_NAMES
            )
            .is_empty(),
            "prose naming a retired knob is not a use of it"
        );
        assert!(
            uses_of(
                "    let used = tuning.page_index_cache_bytes_observed;\n",
                &RETIRED_INDEX_CACHE_NAMES
            )
            .is_empty(),
            "a longer identifier that starts with the name is a different identifier"
        );

        // The positive control's matcher is the same one, so prove it on the live sibling too.
        assert_eq!(
            uses_of(
                "            compaction_watermark_bytes: DEFAULT_COMPACTION_WATERMARK_BYTES,\n",
                &LIVE_SIBLING_NAMES
            )
            .len(),
            2,
            "the live-sibling matcher must see both the field and the constant"
        );
    }
}
