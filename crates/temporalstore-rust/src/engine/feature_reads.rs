use std::collections::BTreeMap;

use matrixcache::MultiLayerCache;

use crate::block_store::{LocalBlockStore, BlockAddress};
use crate::types::{FeatureFilter, FeaturePoint, SequenceFeatureRow, ShardId};

use super::packed_pages::{
    read_feature_points_cached_batch, read_feature_points_from_pages_in_range,
    read_feature_points_from_pages_last,
};
use super::product_model::{
    aggregate_feature_values, is_supported_feature_aggregate, sequence_filter_matches,
};
use super::slot_index_object_page_addresses;
use super::state::ShardState;

fn timestamp_page_refs_in_range(
    series: &BTreeMap<u64, BlockAddress>,
    start_ms: u64,
    end_ms: u64,
    limit: usize,
) -> Vec<(u64, BlockAddress)> {
    let mut refs = Vec::with_capacity(limit.min(series.len()));
    refs.extend(
        series
            .range(start_ms..=end_ms)
            .take(limit)
            .map(|(timestamp_ms, address)| (*timestamp_ms, address.clone())),
    );
    refs
}

fn timestamp_page_refs_last(
    series: &BTreeMap<u64, BlockAddress>,
    limit: usize,
) -> Vec<(u64, BlockAddress)> {
    let mut refs = Vec::with_capacity(limit.min(series.len()));
    refs.extend(
        series
            .iter()
            .rev()
            .take(limit)
            .map(|(timestamp_ms, address)| (*timestamp_ms, address.clone())),
    );
    refs
}

fn is_feature_count_aggregator(aggregator: &str) -> bool {
    let aggregator = aggregator.trim();
    aggregator.is_empty()
        || aggregator.eq_ignore_ascii_case("count")
        || aggregator.eq_ignore_ascii_case("events")
}

pub(super) fn read_feature_points_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    limit: usize,
) -> Vec<FeaturePoint> {
    let series = match model_id {
        "feature" => shard.features.get(key),
        "sequence" => shard.sequences.get(key),
        "ips" => shard.ips.get(key),
        _ => None,
    };
    series
        .map(|series| {
            let refs = timestamp_page_refs_in_range(series, start_ms, end_ms, limit);
            read_feature_points_cached_batch(cache, page_store, shard_id, &refs)
        })
        .unwrap_or_else(|| {
            let addresses = slot_index_object_page_addresses(shard, model_id, key);
            read_feature_points_from_pages_in_range(
                cache, page_store, shard_id, &addresses, start_ms, end_ms, limit,
            )
        })
}

pub(super) fn read_filtered_feature_points(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    limit: usize,
    filters: &[FeatureFilter],
) -> Vec<FeaturePoint> {
    read_feature_points_in_range(
        cache, page_store, shard_id, shard, "feature", key, start_ms, end_ms, limit,
    )
    .into_iter()
    .filter(|point| {
        let Some(row) =
            SequenceFeatureRow::decode_cpp_feature_value(point.timestamp_ms, &point.value)
        else {
            return false;
        };
        filters
            .iter()
            .all(|filter| sequence_filter_matches(&row, filter))
    })
    .collect()
}

pub(super) fn read_feature_aggregate(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    aggregator: &str,
    count: Option<usize>,
) -> i64 {
    let limit = count.unwrap_or(5000);
    if !is_supported_feature_aggregate(aggregator) {
        return 0;
    }
    if is_feature_count_aggregator(aggregator) {
        return read_feature_points_in_range(
            cache, page_store, shard_id, shard, "feature", key, start_ms, end_ms, limit,
        )
        .len() as i64;
    }
    let values = read_feature_points_in_range(
        cache, page_store, shard_id, shard, "feature", key, start_ms, end_ms, limit,
    )
    .into_iter()
    .map(|point| point.value)
    .collect::<Vec<_>>();
    aggregate_feature_values(&values, aggregator)
}

pub(super) fn read_ips_count_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    shard
        .ips
        .get(key)
        .map(|series| series.range(start_ms..=end_ms).count() as i64)
        .unwrap_or_else(|| {
            let addresses = slot_index_object_page_addresses(shard, "ips", key);
            read_feature_points_from_pages_in_range(
                cache,
                page_store,
                shard_id,
                &addresses,
                start_ms,
                end_ms,
                usize::MAX,
            )
            .len() as i64
        })
}

pub(super) fn read_ips_points_last(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    count: usize,
) -> Vec<FeaturePoint> {
    shard
        .ips
        .get(key)
        .map(|series| {
            let refs = timestamp_page_refs_last(series, count);
            read_feature_points_cached_batch(cache, page_store, shard_id, &refs)
        })
        .unwrap_or_else(|| {
            let addresses = slot_index_object_page_addresses(shard, "ips", key);
            read_feature_points_from_pages_last(cache, page_store, shard_id, &addresses, count)
        })
}
