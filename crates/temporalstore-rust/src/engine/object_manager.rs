// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

use super::state::ShardState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectRuntimeState {
    pub object_id: u64,
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub object_keys: Vec<String>,
    pub model_ids: Vec<String>,
    pub page_ref_count: usize,
    pub hot_page_ref_count: usize,
    pub cold_page_ref_count: usize,
    pub deleted_page_ref_count: usize,
    pub residency: String,
    pub dirty: bool,
    pub deleted: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectManagerRuntimeReport {
    pub object_manager_runtime_module: bool,
    #[serde(rename = "slot_index_authority")]
    pub bucket_index_authority: bool,
    pub live_object_count: usize,
    pub live_page_ref_count: usize,
    pub missing_object_owner_refs: usize,
    pub reused_object_ids: usize,
    pub dirty_object_count: usize,
    pub deleted_object_count: usize,
    pub hot_object_count: usize,
    pub cold_object_count: usize,
    pub mixed_residency_object_count: usize,
    pub object_page_transition_count: usize,
    pub loading_object_count: usize,
    pub in_memory_object_count: usize,
    pub ttl_object_count: usize,
    pub objects: Vec<ObjectRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> ObjectManagerRuntimeReport {
    let mut object_ids = std::collections::BTreeSet::new();
    let mut missing_object_owner_refs = 0usize;
    let mut object_ref_counts = std::collections::BTreeMap::<u64, usize>::new();
    let mut objects = std::collections::BTreeMap::<u64, ObjectRuntimeState>::new();
    let mut live_page_ref_count = 0usize;

    for bucket in shard.bucket_index.bucket_map.values() {
        for object_id in &bucket.object_index {
            object_ids.insert(*object_id);
            let object_deleted = bucket.deleted || bucket.deleted_object_index.contains(object_id);
            let object = objects
                .entry(*object_id)
                .or_insert_with(|| ObjectRuntimeState {
                    object_id: *object_id,
                    routing_bucket: bucket.routing_bucket,
                    object_keys: Vec::new(),
                    model_ids: Vec::new(),
                    page_ref_count: 0,
                    hot_page_ref_count: 0,
                    cold_page_ref_count: 0,
                    deleted_page_ref_count: 0,
                    residency: "cold".to_string(),
                    dirty: bucket.dirty,
                    deleted: object_deleted,
                    loading: bucket.loading,
                    in_memory: bucket.in_memory,
                    ttl_ms: bucket.ttl_ms,
                });
            object.dirty |= bucket.dirty;
            object.deleted |= object_deleted;
            object.loading |= bucket.loading;
            object.in_memory |= bucket.in_memory;
            object.ttl_ms = match (object.ttl_ms, bucket.ttl_ms) {
                (Some(existing), Some(next)) => Some(existing.min(next)),
                (None, Some(next)) => Some(next),
                (existing, None) => existing,
            };
        }
        for page in bucket.page_index.values() {
            live_page_ref_count = live_page_ref_count.saturating_add(1);
            object_ids.insert(page.object_id);
            *object_ref_counts.entry(page.object_id).or_default() += 1;
            if page.address.object_id() != Some(page.object_id) {
                missing_object_owner_refs = missing_object_owner_refs.saturating_add(1);
            }
            let object = objects
                .entry(page.object_id)
                .or_insert_with(|| ObjectRuntimeState {
                    object_id: page.object_id,
                    routing_bucket: bucket.routing_bucket,
                    object_keys: Vec::new(),
                    model_ids: Vec::new(),
                    page_ref_count: 0,
                    hot_page_ref_count: 0,
                    cold_page_ref_count: 0,
                    deleted_page_ref_count: 0,
                    residency: "cold".to_string(),
                    dirty: false,
                    deleted: false,
                    loading: false,
                    in_memory: false,
                    ttl_ms: None,
                });
            object.page_ref_count = object.page_ref_count.saturating_add(1);
            object.dirty |= page.dirty || bucket.dirty;
            let object_deleted =
                page.deleted || bucket.deleted || bucket.deleted_object_index.contains(&page.object_id);
            object.deleted |= object_deleted;
            if object_deleted {
                object.deleted_page_ref_count = object.deleted_page_ref_count.saturating_add(1);
            } else if bucket.in_memory && !page.log_backed {
                object.hot_page_ref_count = object.hot_page_ref_count.saturating_add(1);
            } else {
                object.cold_page_ref_count = object.cold_page_ref_count.saturating_add(1);
            }
            object.loading |= bucket.loading;
            object.in_memory |= bucket.in_memory;
            object.ttl_ms = match (object.ttl_ms, bucket.ttl_ms) {
                (Some(existing), Some(next)) => Some(existing.min(next)),
                (None, Some(next)) => Some(next),
                (existing, None) => existing,
            };
            if !object.object_keys.contains(&page.object_key) {
                object.object_keys.push(page.object_key.clone());
            }
            if !object.model_ids.contains(&page.model_id) {
                object.model_ids.push(page.model_id.clone());
            }
        }
    }
    let objects = objects
        .into_values()
        .map(|mut object| {
            object.residency = if object.deleted
                && (object.page_ref_count == 0
                    || object.deleted_page_ref_count >= object.page_ref_count)
            {
                "deleted".to_string()
            } else if object.hot_page_ref_count > 0 && object.cold_page_ref_count > 0 {
                "mixed".to_string()
            } else if object.hot_page_ref_count > 0
                || (object.page_ref_count == 0 && object.in_memory)
            {
                "hot".to_string()
            } else {
                "cold".to_string()
            };
            object
        })
        .collect::<Vec<_>>();

    ObjectManagerRuntimeReport {
        object_manager_runtime_module: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        live_object_count: object_ids.len(),
        live_page_ref_count,
        missing_object_owner_refs,
        reused_object_ids: object_ref_counts.values().filter(|refs| **refs > 1).count(),
        dirty_object_count: objects.iter().filter(|object| object.dirty).count(),
        deleted_object_count: objects.iter().filter(|object| object.deleted).count(),
        hot_object_count: objects
            .iter()
            .filter(|object| object.residency == "hot")
            .count(),
        cold_object_count: objects
            .iter()
            .filter(|object| object.residency == "cold")
            .count(),
        mixed_residency_object_count: objects
            .iter()
            .filter(|object| object.residency == "mixed")
            .count(),
        object_page_transition_count: objects
            .iter()
            .filter(|object| {
                object.page_ref_count > 1
                    || object.deleted_page_ref_count > 0
                    || object.residency == "mixed"
            })
            .count(),
        loading_object_count: objects.iter().filter(|object| object.loading).count(),
        in_memory_object_count: objects.iter().filter(|object| object.in_memory).count(),
        ttl_object_count: objects
            .iter()
            .filter(|object| object.ttl_ms.is_some())
            .count(),
        objects,
    }
}
