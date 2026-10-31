// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Product data-model read + aggregation helpers.
//!
//! Backs the engine's higher-level model queries: decoding sequence rows from the
//! shared feature timestamped-KV storage and applying `FeatureFilter` predicates,
//! and computing control-state family aggregates (Counter sum/count/min/max/
//! first/last, Distinct cardinality, Selection first/last). Write paths live in
//! the engine's execute path; this module is the read/aggregation side.

use super::timestamped_series::TimestampedSeries as _;
use std::collections::BTreeMap;

use crate::block_store::{ElementEntry, BlockStore};
use crate::types::{
    FeatureFilter, FeatureFilterOp, ControlStateFamily,
    ControlStateSelectionType, SequenceFeatureRow, ShardId,
};
use matrixcache::MultiLayerCache;

use super::packed_blocks::decode_feature_block_strict;
use super::state::PackedFeatureBlockDecode;
use super::hashing::BlockIdentity;
use super::{parse_i64, read_block_bytes, ShardState};
pub(super) fn read_sequence_row(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &ElementEntry,
    // WHICH PAGE, stated by the caller. A packed series page holds many points and is not an
    // element of its object, so the element half is `None` -- but the OBJECT half cannot be
    // recovered here: this function is handed an address and a timestamp, and neither names a key.
    // See [`BlockIdentity`] for why it is the terms rather than an id that cross.
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
) -> Option<SequenceFeatureRow> {
    let bytes = read_block_bytes(cache, block_store, shard_id, address, identity, routing_bucket)?;
    match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .and_then(|point| serde_json::from_slice(&point.value).ok()),
        PackedFeatureBlockDecode::Legacy => serde_json::from_slice(&bytes).ok(),
        PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

pub(super) fn sequence_filter_matches(row: &SequenceFeatureRow, filter: &FeatureFilter) -> bool {
    let lhs = match filter.field.as_str() {
        "gid" => row.gid,
        "action_type" => row.action_type as u64,
        "duration" => row.duration as u64,
        "author_id" => row.author_id,
        _ => return false,
    };
    match filter.op {
        FeatureFilterOp::Equal => lhs == filter.value,
        FeatureFilterOp::NotEqual => lhs != filter.value,
        FeatureFilterOp::GreaterThan => lhs > filter.value,
        FeatureFilterOp::GreaterOrEqual => lhs >= filter.value,
        FeatureFilterOp::LessThan => lhs < filter.value,
        FeatureFilterOp::LessOrEqual => lhs <= filter.value,
    }
}

pub(super) fn sequence_rows_in_range(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: &[FeatureFilter],
) -> Vec<SequenceFeatureRow> {
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket =
        crate::engine::hashing::block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    shard
        .features()
        .get(key)
        .map(|series| {
            series
                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row(
                        cache,
                        block_store,
                        shard_id,
                        timestamp_ms,
                        address,
                        BlockIdentity::of(shard_id, "feature", key, None),
                        Some(routing_bucket),
                    )
                })
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn aggregate_feature_values(values: &[Vec<u8>], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" => values.iter().filter_map(parse_i64).sum(),
        "avg" | "average" => {
            let numeric = values.iter().filter_map(parse_i64).collect::<Vec<_>>();
            if numeric.is_empty() {
                0
            } else {
                numeric.iter().sum::<i64>() / numeric.len() as i64
            }
        }
        "min" => values
            .iter()
            .filter_map(parse_i64)
            .min()
            .unwrap_or_default(),
        "max" => values
            .iter()
            .filter_map(parse_i64)
            .max()
            .unwrap_or_default(),
        "first" => values.first().and_then(parse_i64).unwrap_or_default(),
        "last" => values.last().and_then(parse_i64).unwrap_or_default(),
        "count" | "events" | "" => values.len() as i64,
        _ => values.len() as i64,
    }
}

pub(super) fn aggregate_control_state_values(values: &[i64], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" | "count" | "" => values.iter().sum(),
        "events" | "len" => values.len() as i64,
        "min" => values.iter().copied().min().unwrap_or_default(),
        "max" => values.iter().copied().max().unwrap_or_default(),
        "first" => values.first().copied().unwrap_or_default(),
        "last" => values.last().copied().unwrap_or_default(),
        _ => values.iter().sum(),
    }
}

pub(super) fn is_control_state_change_aggregator(aggregator: &str) -> bool {
    aggregator.eq_ignore_ascii_case("change")
}

/// UUID idempotency dedup window for control-state writes (300s), matching the
/// control_state `kUUIDExpiredTime` production value.
pub(super) const CONTROL_STATE_UUID_DEDUP_MS: u64 = 300_000;

/// Soft cap on the in-memory UUID dedup ledger before a reclaiming sweep runs.
/// Expired entries are already ignored by the dedup check (an entry past its
/// expiry is treated as absent), so this sweep is purely for memory reclamation
/// and only fires when the ledger grows large — keeping per-write cost O(1)
/// amortized instead of O(n) on every write.
const CONTROL_STATE_UUID_LEDGER_SOFT_CAP: usize = 2_000_000;

/// Reclaim expired UUID dedup entries, but only once the ledger exceeds the soft
/// cap so the common path stays allocation-free and O(1).
pub(super) fn gc_control_state_uuid(shard: &mut ShardState, now_ms: u64) {
    if shard.control_state_uuid().len() > CONTROL_STATE_UUID_LEDGER_SOFT_CAP {
        shard.control_state_uuid_mut().retain(|_, expiry| *expiry > now_ms);
    }
}

pub(super) fn count_control_state_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    // Exact when no bucket in the window is sketched; HLL estimate once any bucket has been
    // converted to a bounded sketch (high-cardinality distinct).
    super::hll::count_changes(shard, key, start_ms, end_ms)
}

/// Distinct count for a caller that names a precision, covered with that precision's ladder.
pub(super) fn count_control_state_changes_at_precision(
    shard: &ShardState,
    key: &str,
    precision_ms: u64,
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    super::hll::count_changes_at_precision(shard, key, precision_ms, start_ms, end_ms)
}

pub(super) fn control_state_family_key(family: ControlStateFamily, key: &str) -> String {
    format!("control_state:{}:{key}", control_state_family_name(family))
}

/// A Control State bucket is identified by the PRECISION IT WAS WRITTEN AT as well as by its
/// start instant, and this is where that pair is spelled.
///
/// # WHY THE PRECISION HAS TO BE PART OF THE IDENTITY
///
/// A counter bucket used to be identified by its start instant alone: a write truncated its
/// timestamp down to a multiple of the precision it named and added its amount there. Two writes
/// that named DIFFERENT precisions then landed in the SAME bucket whenever the coarser truncation
/// happened to agree with the finer one -- a per-minute write and a per-hour write at the top of
/// an hour both reduce to the top of that hour -- and their amounts were summed into one cell with
/// nothing left to say which precision either belonged to.
///
/// That is not an exotic shape. Serving a per-minute rate and a per-day cap off one counter is the
/// ordinary way to use this model, and it is reachable from a single command:
/// `ControlStateSetAndGetWithOptions` names a precision, writes at it, and then reads a window.
/// The read summed every bucket in the window at EVERY precision, so the per-minute answer
/// included the per-day write, and a cap built on that answer admitted traffic it should have
/// refused.
///
/// The format this model implements never had that problem, because the precision is part of the
/// stored field's NAME: a field is the precision's code followed by the bucket's start, so two
/// precisions cannot occupy one field, and a query names the precision it wants and reads only
/// that precision's fields.
///
/// # HOW THE PAIR FITS WHERE ONE NUMBER USED TO
///
/// The codes below are the format's own, and they are two digits each for exactly the reason that
/// matters here: a code is SMALL. Seven bits hold the largest of them, an epoch millisecond needs
/// forty-one until the year 10889, and both fit in the `u64` a bucket key already is. So the
/// identity gains the precision WITHOUT the series changing type -- which is what keeps this
/// change out of the durable paths. `control_state` rides the index snapshot and the WAL; had the
/// key become a pair, every store holding one and every reader of those stores would have needed a
/// format migration. The key stays a `u64`, so they do not.
///
/// Data written before this change carries code `0`, which is the format's own `DISABLE` -- the
/// value it uses for a write that names no precision. Old buckets therefore read back as exactly
/// what they are, untagged, and a read that names no precision still finds them.
pub(super) const CONTROL_STATE_PRECISION_SHIFT: u32 = 48;

/// Low bits of a bucket key: the bucket's start, in epoch milliseconds.
pub(super) const CONTROL_STATE_BUCKET_MASK: u64 = (1u64 << CONTROL_STATE_PRECISION_SHIFT) - 1;

/// Every precision code a bucket key can carry, coarsest last, with `0` -- the format's `DISABLE`,
/// meaning no precision was named -- first.
///
/// A read that does not name a precision has to answer across all of them, and it does that by
/// scanning this list rather than by walking the series: nine range reads of a `BTreeMap` cost
/// `O(codes * (log n + hits))`, where iterating the series and filtering on each key's low bits
/// would cost `O(n)` and would get slower the longer a key had been counting.
pub(super) const CONTROL_STATE_PRECISION_CODES: [u64; 9] = [0, 10, 14, 19, 30, 34, 39, 50, 60];

/// The code the format files a write under, for the precisions it defines.
///
/// `None` means this precision is not one of them, and such a write keeps the untagged bucketing it
/// has always had (code `0`) rather than being given a code of our own invention -- a tag only this
/// implementation understands would be a divergence dressed up as conformance.
///
/// `OneMonth` (code 80) is deliberately absent: a month is not a fixed span of milliseconds, so it
/// cannot be expressed by the truncation this function's callers perform, and admitting it here
/// would file calendar-month writes under a code whose buckets are not months.
pub(super) fn control_state_precision_code(precision_ms: u64) -> Option<u64> {
    Some(match precision_ms {
        1_000 => 10,
        5_000 => 14,
        10_000 => 19,
        60_000 => 30,
        300_000 => 34,
        600_000 => 39,
        3_600_000 => 50,
        86_400_000 => 60,
        _ => return None,
    })
}

/// Compose a bucket key from a precision code and a bucket start.
pub(super) fn control_state_pack_bucket(code: u64, bucket_start_ms: u64) -> u64 {
    (code << CONTROL_STATE_PRECISION_SHIFT) | (bucket_start_ms & CONTROL_STATE_BUCKET_MASK)
}

/// The bucket key a write at `timestamp_ms` naming `precision_ms` belongs in.
///
/// With no precision, or one the format does not define, this is the timestamp itself under code
/// `0` -- byte-for-byte the key such a write has always produced.
pub(super) fn control_state_bucket_key(timestamp_ms: u64, precision_ms: Option<u64>) -> u64 {
    let Some(precision_ms) = precision_ms.filter(|precision_ms| *precision_ms > 0) else {
        return control_state_pack_bucket(0, timestamp_ms);
    };
    let start_ms = timestamp_ms - timestamp_ms % precision_ms;
    control_state_pack_bucket(
        control_state_precision_code(precision_ms).unwrap_or(0),
        start_ms,
    )
}

/// The bucket START a key names, with its precision code removed.
///
/// Every path that hands a bucket key back to a caller as a timestamp, or does time arithmetic on
/// one, has to go through this: a packed key read as a timestamp is an instant in the year 10889.
pub(super) fn control_state_bucket_start_ms(bucket_key: u64) -> u64 {
    bucket_key & CONTROL_STATE_BUCKET_MASK
}

/// The precisions a DISTINCT write is stored at: its own, and one coarser.
///
/// # WHY A DISTINCT WRITE FANS OUT AND A COUNTER WRITE DOES NOT
///
/// A counter over a wide window is the SUM of the fine buckets inside it, so one stored level is
/// enough -- adding them up is cheap and exact, and `control_rollup` keeps a derived ladder in
/// memory to make it cheaper still.
///
/// A distinct count is not a sum. The number of distinct members in an hour is not the sum of the
/// distinct members of its sixty minutes, because a member appearing in two minutes must be
/// counted once. So a long window has to MERGE every fine bucket it covers: a day at minute
/// precision is 1,440 sketch merges, and a week is 10,080. Storing the member at a coarser level
/// as it arrives turns that into 7 reads of a day-level sketch.
///
/// # WHY STORING IT TWICE IS SAFE HERE AND WOULD BE WRONG FOR A COUNTER
///
/// Set union is IDEMPOTENT. A member recorded into both its minute bucket and its hour bucket is
/// still one distinct member in each, so no read can double-count it however it combines the
/// levels -- including a read that covers both. The same fan-out applied to a counter would
/// double every amount, which is exactly why the counter path writes one field and this one does
/// not.
///
/// The pairs below are the format's own ladder. Each step is a factor of sixty or twenty-four, and
/// the coarsest precision has no coarser partner to fan out to.
pub(super) fn control_state_precision_levels(precision_ms: u64) -> &'static [u64] {
    match precision_ms {
        1_000 => &[1_000, 60_000],
        5_000 => &[5_000, 300_000],
        10_000 => &[10_000, 600_000],
        60_000 => &[60_000, 3_600_000],
        300_000 => &[300_000, 3_600_000],
        600_000 => &[600_000, 3_600_000],
        3_600_000 => &[3_600_000, 86_400_000],
        86_400_000 => &[86_400_000],
        // Not a precision the format defines, so it has no ladder and keeps the single untagged
        // bucket it has always had. Returning an empty slice here would stop the write entirely.
        _ => &[],
    }
}

/// The bucket keys a window covers at ONE precision -- the format's own read shape, where a query
/// names a precision and scans the fields that carry its code.
///
/// Inverted windows are EMPTY rather than a panic, which is the contract `timestamp_range_bounds`
/// already keeps for the untagged reads: `BTreeMap::range` panics when its start exceeds its end,
/// and a caller is free to pass a window either way round.
pub(super) fn control_state_precision_bounds(
    code: u64,
    start_ms: u64,
    end_ms: u64,
) -> (std::ops::Bound<u64>, std::ops::Bound<u64>) {
    use std::ops::Bound;
    if start_ms > end_ms {
        return (Bound::Included(1), Bound::Excluded(1));
    }
    (
        Bound::Included(control_state_pack_bucket(code, start_ms)),
        Bound::Included(control_state_pack_bucket(code, end_ms)),
    )
}

/// Every bucket a window covers at EVERY precision: what a read that names no precision answers.
///
/// Yielded precision-major, so a key counting at one precision -- which is every key written
/// before this existed, and every key whose caller names one precision -- comes back in exactly
/// the time order it always did.
pub(super) fn control_state_window_entries<'a>(
    series: &'a std::collections::BTreeMap<u64, i64>,
    start_ms: u64,
    end_ms: u64,
) -> impl Iterator<Item = (&'a u64, &'a i64)> + 'a {
    CONTROL_STATE_PRECISION_CODES
        .iter()
        .flat_map(move |code| series.range(control_state_precision_bounds(*code, start_ms, end_ms)))
}

pub(super) fn control_state_family_name(family: ControlStateFamily) -> &'static str {
    match family {
        ControlStateFamily::Counter => "h",
        ControlStateFamily::Distinct => "cpc",
        ControlStateFamily::Selection => "fol",
    }
}

/// `MANAGER` op-code conformance. Accepts the numeric code or its symbolic name.
/// `None` / unknown -> the family summary (the historical default).
pub(super) fn control_state_manager_op_code(op_type: Option<&str>) -> Option<i64> {
    let value = op_type?.trim();
    value.parse::<i64>().ok().or_else(|| match value {
        "QUERY" | "query" => Some(2),
        "FIELD_LIST" | "field_list" => Some(5),
        "FIELD_COUNT" | "field_count" => Some(6),
        "ALL_DATA_VALUE" | "all_data_value" => Some(7),
        _ => None,
    })
}

fn control_state_manager_series<'a>(
    shard: &'a ShardState,
    key: &str,
    is_distinct: bool,
) -> Option<&'a BTreeMap<u64, i64>> {
    let family = if is_distinct {
        ControlStateFamily::Distinct
    } else {
        ControlStateFamily::Counter
    };
    shard.control_state().get(&control_state_family_key(family, key)[..])
}

/// Dispatch a Control State MANAGER request. Mirrors the `Manager`/`CPCManager`
/// read op-codes QUERY(2), FIELD_LIST(5), FIELD_COUNT(6), ALL_DATA_VALUE(7); any
/// other op (or `None`) returns the cross-family summary.
pub(super) fn control_state_manager_entries(
    shard: &ShardState,
    key: &str,
    op_type: Option<&str>,
    field_list: &[(String, String)],
    start_offset: &str,
    end_offset: &str,
    is_distinct: bool,
) -> Vec<(String, Vec<u8>)> {
    match control_state_manager_op_code(op_type) {
        Some(2) => {
            let Some(series) = control_state_manager_series(shard, key, is_distinct) else {
                return Vec::new();
            };
            field_list
                .iter()
                .filter_map(|(field, _)| {
                    field
                        .parse::<u64>()
                        .ok()
                        .and_then(|timestamp_ms| series.get(&timestamp_ms))
                        .map(|value| (field.clone(), value.to_string().into_bytes()))
                })
                .collect()
        }
        Some(5) => {
            let Some(series) = control_state_manager_series(shard, key, is_distinct) else {
                return vec![("key_list".to_string(), Vec::new())];
            };
            let start = start_offset.parse::<u64>().unwrap_or(0);
            let end = end_offset.parse::<u64>().unwrap_or(u64::MAX);
            let value = series
                .range(crate::engine::timestamp_range_bounds(start, end))
                .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                .collect::<Vec<_>>()
                .join(",");
            vec![("key_list".to_string(), value.into_bytes())]
        }
        Some(6) => {
            let size = control_state_manager_series(shard, key, is_distinct)
                .map(BTreeMap::len)
                .unwrap_or_default();
            vec![("size".to_string(), size.to_string().into_bytes())]
        }
        Some(7) => control_state_manager_series(shard, key, is_distinct)
            .map(|series| {
                series
                    .iter()
                    .map(|(timestamp_ms, value)| {
                        (timestamp_ms.to_string(), value.to_string().into_bytes())
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => control_state_manager_summary_entries(shard, key),
    }
}

fn control_state_manager_summary_entries(shard: &ShardState, key: &str) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    for family in [
        ControlStateFamily::Counter,
        ControlStateFamily::Distinct,
        ControlStateFamily::Selection,
    ] {
        let family_key = control_state_family_key(family, key);
        let values = shard
            .control_state()
            .get(&family_key[..])
            .map(|series| series.values().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        entries.push((
            format!("{}_events", control_state_family_name(family)),
            values.len().to_string().into_bytes(),
        ));
        entries.push((
            format!("{}_sum", control_state_family_name(family)),
            values.iter().sum::<i64>().to_string().into_bytes(),
        ));
    }
    if let Some(fol) = shard.control_state_selection.get(key) {
        entries.push(("fol_value".to_string(), fol.value.clone()));
        entries.push((
            "fol_occur_time_ms".to_string(),
            fol.occur_time_ms.to_string().into_bytes(),
        ));
        entries.push((
            "fol_type".to_string(),
            match fol.selection_type {
                ControlStateSelectionType::First => b"first".to_vec(),
                ControlStateSelectionType::Last => b"last".to_vec(),
            },
        ));
    }
    entries
}
