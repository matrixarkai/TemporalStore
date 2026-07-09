use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::block_store::{BlockAddress, LocalBlockStore};
use crate::types::{
    FeatureFilter, FeatureFilterOp, FeaturePoint, IpsSnapshotReport, IpsStats, RiskFamily,
    SequenceFeatureRow, ShardId,
};
use rustmtcache::MultiLayerCache;

use super::packed_pages::{decode_feature_page_strict, read_feature_point_cached};
use super::state::PackedFeaturePageDecode;
use super::{parse_i64, read_page_bytes_cold, ShardState};

fn decode_sequence_row_value(bytes: &[u8]) -> Option<SequenceFeatureRow> {
    serde_json::from_slice(bytes).ok()
}

fn read_sequence_row_cached(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
    page_cache: &mut HashMap<BlockAddress, Option<Vec<FeaturePoint>>>,
) -> Option<SequenceFeatureRow> {
    let point = read_feature_point_cached(
        cache,
        block_store,
        shard_id,
        timestamp_ms,
        address,
        page_cache,
    )?;
    decode_sequence_row_value(&point.value)
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
            let mut page_cache = HashMap::new();
            series
                .range(start_ms..=end_ms)
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row_cached(
                        cache,
                        block_store,
                        shard_id,
                        *timestamp_ms,
                        address,
                        &mut page_cache,
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

pub(super) fn aggregate_risk_values(values: &[i64], aggregator: &str) -> i64 {
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

pub(super) fn is_risk_change_aggregator(aggregator: &str) -> bool {
    aggregator.eq_ignore_ascii_case("change")
}

pub(super) fn count_risk_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let mut unique = BTreeSet::new();
    if let Some(series) = shard.risk_changes.get(key) {
        for (_, values) in series.range(start_ms..=end_ms) {
            unique.extend(values.iter().cloned());
        }
    }
    unique.len() as i64
}

pub(super) fn risk_family_key(family: RiskFamily, key: &str) -> String {
    format!("risk:{}:{key}", risk_family_name(family))
}

pub(super) fn risk_family_name(family: RiskFamily) -> &'static str {
    match family {
        RiskFamily::H => "h",
        RiskFamily::Cpc => "cpc",
        RiskFamily::Fol => "fol",
    }
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
            let mut page_cache = HashMap::new();
            series
                .range(start_ms..=end_ms)
                .take(count.unwrap_or(usize::MAX))
                .filter_map(|(timestamp_ms, address)| {
                    read_feature_point_cached(
                        cache,
                        block_store,
                        shard_id,
                        *timestamp_ms,
                        address,
                        &mut page_cache,
                    )
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
        .scan(HashMap::new(), |page_cache, (timestamp_ms, meta)| {
            Some(read_feature_point_cached(
                cache,
                block_store,
                shard_id,
                *timestamp_ms,
                &meta.address,
                page_cache,
            ))
        })
        .flatten()
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
        page_segment_ids: Vec::new(),
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
    let mut page_segment_ids = BTreeSet::<u64>::new();
    let mut packed_timestamped_page_count = 0usize;
    if let Some(series) = shard.ips.get(&key) {
        for (_, address) in series.range(start_ms..=end_ms) {
            if page_refs.insert(address.clone()) {
                page_segment_ids.insert(address.page_segment_id);
                if read_page_bytes_cold(block_store, address)
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
        page_segment_ids: page_segment_ids.into_iter().collect(),
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
