// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use super::cache::{page_physical_identity_key, PagePhysicalIdentityKey};
use super::state::ShardState;
use crate::block_store::BlockAddress;

pub(super) struct TimestampedPageBatchWrite {
    pub(super) kind: &'static str,
    pub(super) object_key: String,
    pub(super) timestamp_ms: u64,
    pub(super) value: Vec<u8>,
    pub(super) routing_bucket: u32,
}

pub(super) fn unique_timestamped_kv_page_addresses(
    series: &BTreeMap<u64, BlockAddress>,
) -> Vec<BlockAddress> {
    let mut addresses = BTreeMap::<PagePhysicalIdentityKey, BlockAddress>::new();
    for address in series.values() {
        addresses.insert(page_physical_identity_key(address), address.clone());
    }
    addresses.into_values().collect()
}

pub(super) fn unique_feature_page_addresses(
    series: &BTreeMap<u64, BlockAddress>,
) -> Vec<BlockAddress> {
    unique_timestamped_kv_page_addresses(series)
}

pub(super) fn timestamped_kv_series<'a>(
    shard: &'a ShardState,
) -> Vec<(&'static str, &'a str, &'a BTreeMap<u64, BlockAddress>)> {
    let mut series = Vec::new();
    for (key, timeline) in &shard.features {
        series.push(("feature", key.as_str(), timeline));
    }
    }
    for (key, timeline) in &shard.context_events {
        series.push(("context_event", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_indexes {
        series.push(("context_index", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_audits {
        series.push(("context_audit", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_children {
        series.push(("context_child", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_summaries {
        series.push(("context_summary", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_compressions {
        series.push(("context_compression", key.as_str(), timeline));
    }
    series
}
