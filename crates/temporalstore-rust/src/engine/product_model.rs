use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::block_store::{BlockAddress, LocalBlockStore};
use crate::types::{
    FeatureFilter, FeatureFilterOp, FeaturePoint, IpsSnapshotReport, IpsStats, ControlStateFamily,
    ControlStateFolType, SequenceFeatureRow, ShardId,
};
use matrixcache::MultiLayerCache;

use super::packed_pages::{decode_feature_page_strict, read_feature_point};
use super::state::PackedFeaturePageDecode;
use super::{parse_i64, read_page_bytes, ShardState};
pub(super) fn read_sequence_row(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
) -> Option<SequenceFeatureRow> {
    let bytes = read_page_bytes(cache, block_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .and_then(|point| serde_json::from_slice(&point.value).ok()),
        PackedFeaturePageDecode::Legacy => serde_json::from_slice(&bytes).ok(),
        PackedFeaturePageDecode::Corrupt(_) => None,
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
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: &[FeatureFilter],
) -> Vec<SequenceFeatureRow> {
    shard
        .sequences
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row(cache, block_store, shard_id, *timestamp_ms, address)
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
/// C++ control_state `kUUIDExpiredTime` production value.
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
    if shard.control_state_uuid.len() > CONTROL_STATE_UUID_LEDGER_SOFT_CAP {
        shard.control_state_uuid.retain(|_, expiry| *expiry > now_ms);
    }
}

pub(super) fn count_control_state_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let mut unique = BTreeSet::new();
    if let Some(series) = shard.control_state_changes.get(key) {
        for (_, values) in series.range(start_ms..=end_ms) {
            unique.extend(values.iter().cloned());
        }
    }
    unique.len() as i64
}

pub(super) fn control_state_family_key(family: ControlStateFamily, key: &str) -> String {
    format!("control_state:{}:{key}", control_state_family_name(family))
}

pub(super) fn control_state_family_name(family: ControlStateFamily) -> &'static str {
    match family {
        ControlStateFamily::H => "h",
        ControlStateFamily::Cpc => "cpc",
        ControlStateFamily::Fol => "fol",
    }
}

/// C++ `MANAGER` op-code parity. Accepts the numeric code or its symbolic name.
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
    is_cpc: bool,
) -> Option<&'a BTreeMap<u64, i64>> {
    let family = if is_cpc {
        ControlStateFamily::Cpc
    } else {
        ControlStateFamily::H
    };
    shard.control_state.get(&control_state_family_key(family, key))
}

/// Dispatch a Control State MANAGER request. Mirrors the C++ `Manager`/`CPCManager`
/// read op-codes QUERY(2), FIELD_LIST(5), FIELD_COUNT(6), ALL_DATA_VALUE(7); any
/// other op (or `None`) returns the cross-family summary.
pub(super) fn control_state_manager_entries(
    shard: &ShardState,
    key: &str,
    op_type: Option<&str>,
    field_list: &[(String, String)],
    start_offset: &str,
    end_offset: &str,
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    match control_state_manager_op_code(op_type) {
        Some(2) => {
            let Some(series) = control_state_manager_series(shard, key, is_cpc) else {
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
            let Some(series) = control_state_manager_series(shard, key, is_cpc) else {
                return vec![("key_list".to_string(), Vec::new())];
            };
            let start = start_offset.parse::<u64>().unwrap_or(0);
            let end = end_offset.parse::<u64>().unwrap_or(u64::MAX);
            let value = series
                .range(start..=end)
                .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                .collect::<Vec<_>>()
                .join(",");
            vec![("key_list".to_string(), value.into_bytes())]
        }
        Some(6) => {
            let size = control_state_manager_series(shard, key, is_cpc)
                .map(BTreeMap::len)
                .unwrap_or_default();
            vec![("size".to_string(), size.to_string().into_bytes())]
        }
        Some(7) => control_state_manager_series(shard, key, is_cpc)
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
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        let values = shard
            .control_state
            .get(&family_key)
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
    if let Some(fol) = shard.control_state_fol.get(key) {
        entries.push(("fol_value".to_string(), fol.value.clone()));
        entries.push((
            "fol_occur_time_ms".to_string(),
            fol.occur_time_ms.to_string().into_bytes(),
        ));
        entries.push((
            "fol_type".to_string(),
            match fol.fol_type {
                ControlStateFolType::First => b"first".to_vec(),
                ControlStateFolType::Last => b"last".to_vec(),
            },
        ));
    }
    entries
}

pub(super) fn ips_points_in_range(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
) -> Vec<FeaturePoint> {
    shard
        .ips
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count.unwrap_or(usize::MAX))
                .filter_map(|(timestamp_ms, address)| {
                    read_feature_point(cache, block_store, shard_id, *timestamp_ms, address)
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn ips_points_in_range_with_options(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
    action_type: Option<u32>,
    table_id: Option<u64>,
) -> Vec<FeaturePoint> {
    let Some(series) = shard.ips_meta.get(key) else {
        return ips_points_in_range(
            cache,
            block_store,
            shard_id,
            shard,
            key,
            start_ms,
            end_ms,
            count,
        );
    };
    series
        .range(start_ms..=end_ms)
        .filter(|(_, meta)| {
            action_type
                .map(|expected| meta.action_type == Some(expected))
                .unwrap_or(true)
                && table_id
                    .map(|expected| meta.table_id == Some(expected))
                    .unwrap_or(true)
        })
        .take(count.unwrap_or(usize::MAX))
        .filter_map(|(timestamp_ms, meta)| {
            read_feature_point(cache, block_store, shard_id, *timestamp_ms, &meta.address)
        })
        .collect()
}

pub(super) fn empty_ips_snapshot_report(
    key: String,
    start_ms: u64,
    end_ms: u64,
    requested_count: Option<usize>,
) -> IpsSnapshotReport {
    IpsSnapshotReport {
        key,
        start_ms,
        end_ms,
        requested_count,
        returned_count: 0,
        total_in_range: 0,
        first_timestamp_ms: None,
        last_timestamp_ms: None,
        action_type_counts: Vec::new(),
        table_id_counts: Vec::new(),
        unique_page_ref_count: 0,
        packed_timestamped_page_count: 0,
        page_slab_ids: Vec::new(),
    }
}

pub(super) fn ips_snapshot_report_in_range(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: String,
    start_ms: u64,
    end_ms: u64,
    requested_count: Option<usize>,
) -> IpsSnapshotReport {
    let points = ips_points_in_range(
        cache,
        block_store,
        shard_id,
        shard,
        &key,
        start_ms,
        end_ms,
        requested_count,
    );
    let stats = ips_stats_in_range(shard, &key, start_ms, end_ms);
    let mut page_refs = HashSet::<BlockAddress>::new();
    let mut page_slab_ids = BTreeSet::<u64>::new();
    let mut packed_timestamped_page_count = 0usize;
    if let Some(series) = shard.ips.get(&key) {
        for (_, address) in series.range(start_ms..=end_ms) {
            if page_refs.insert(address.clone()) {
                page_slab_ids.insert(address.page_slab_id);
                if read_page_bytes(cache, block_store, shard_id, address)
                    .map(|bytes| {
                        matches!(
                            decode_feature_page_strict(&bytes),
                            PackedFeaturePageDecode::Packed(_)
                        )
                    })
                    .unwrap_or(false)
                {
                    packed_timestamped_page_count += 1;
                }
            }
        }
    }
    IpsSnapshotReport {
        key,
        start_ms,
        end_ms,
        requested_count,
        returned_count: points.len(),
        total_in_range: stats.total,
        first_timestamp_ms: stats.first_timestamp_ms,
        last_timestamp_ms: stats.last_timestamp_ms,
        action_type_counts: stats.action_type_counts,
        table_id_counts: stats.table_id_counts,
        unique_page_ref_count: page_refs.len(),
        packed_timestamped_page_count,
        page_slab_ids: page_slab_ids.into_iter().collect(),
    }
}

pub(super) fn ips_stats_in_range(
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
) -> IpsStats {
    let mut total = 0u64;
    let mut first_timestamp_ms = None;
    let mut last_timestamp_ms = None;
    let mut action_type_counts = BTreeMap::<u32, u64>::new();
    let mut table_id_counts = BTreeMap::<u64, u64>::new();

    if let Some(series) = shard.ips.get(key) {
        for (timestamp_ms, _) in series.range(start_ms..=end_ms) {
            total += 1;
            first_timestamp_ms.get_or_insert(*timestamp_ms);
            last_timestamp_ms = Some(*timestamp_ms);
        }
    }
    if let Some(series) = shard.ips_meta.get(key) {
        for (_, meta) in series.range(start_ms..=end_ms) {
            if let Some(action_type) = meta.action_type {
                *action_type_counts.entry(action_type).or_default() += 1;
            }
            if let Some(table_id) = meta.table_id {
                *table_id_counts.entry(table_id).or_default() += 1;
            }
        }
    }

    IpsStats {
        total,
        first_timestamp_ms,
        last_timestamp_ms,
        action_type_counts: action_type_counts.into_iter().collect(),
        table_id_counts: table_id_counts.into_iter().collect(),
    }
}
