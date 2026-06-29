use serde::{Deserialize, Serialize};

use super::state::ShardState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectRuntimeState {
    pub object_id: u64,
    pub routing_slot: u32,
    pub object_keys: Vec<String>,
    pub model_ids: Vec<String>,
    pub page_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectManagerRuntimeReport {
    pub object_manager_runtime_module: bool,
    pub slot_index_authority: bool,
    pub live_object_count: usize,
    pub live_page_ref_count: usize,
    pub missing_object_owner_refs: usize,
    pub reused_object_ids: usize,
    pub dirty_object_count: usize,
    pub deleted_object_count: usize,
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

    for slot in shard.slot_index.slot_map.values() {
        for object_id in &slot.object_index {
            object_ids.insert(*object_id);
            let object = objects
                .entry(*object_id)
                .or_insert_with(|| ObjectRuntimeState {
                    object_id: *object_id,
                    routing_slot: slot.routing_slot,
                    object_keys: Vec::new(),
                    model_ids: Vec::new(),
                    page_ref_count: 0,
                    dirty: slot.dirty,
                    deleted: false,
                    loading: slot.loading,
                    in_memory: slot.in_memory,
                    ttl_ms: slot.ttl_ms,
                });
            object.dirty |= slot.dirty;
            object.loading |= slot.loading;
            object.in_memory |= slot.in_memory;
            object.ttl_ms = match (object.ttl_ms, slot.ttl_ms) {
                (Some(existing), Some(next)) => Some(existing.min(next)),
                (None, Some(next)) => Some(next),
                (existing, None) => existing,
            };
        }
        for page in slot.page_index.values() {
            live_page_ref_count = live_page_ref_count.saturating_add(1);
            object_ids.insert(page.object_id);
            *object_ref_counts.entry(page.object_id).or_default() += 1;
            if page.address.object_id != Some(page.object_id) {
                missing_object_owner_refs = missing_object_owner_refs.saturating_add(1);
            }
            let object = objects
                .entry(page.object_id)
                .or_insert_with(|| ObjectRuntimeState {
                    object_id: page.object_id,
                    routing_slot: slot.routing_slot,
                    object_keys: Vec::new(),
                    model_ids: Vec::new(),
                    page_ref_count: 0,
                    dirty: false,
                    deleted: false,
                    loading: false,
                    in_memory: false,
                    ttl_ms: None,
                });
            object.page_ref_count = object.page_ref_count.saturating_add(1);
            object.dirty |= page.dirty || slot.dirty;
            object.deleted |= page.deleted;
            object.loading |= slot.loading;
            object.in_memory |= slot.in_memory;
            object.ttl_ms = match (object.ttl_ms, slot.ttl_ms) {
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
    let objects = objects.into_values().collect::<Vec<_>>();

    ObjectManagerRuntimeReport {
        object_manager_runtime_module: true,
        slot_index_authority: !shard.slot_index.slot_map.is_empty(),
        live_object_count: object_ids.len(),
        live_page_ref_count,
        missing_object_owner_refs,
        reused_object_ids: object_ref_counts.values().filter(|refs| **refs > 1).count(),
        dirty_object_count: objects.iter().filter(|object| object.dirty).count(),
        deleted_object_count: objects.iter().filter(|object| object.deleted).count(),
        loading_object_count: objects.iter().filter(|object| object.loading).count(),
        in_memory_object_count: objects.iter().filter(|object| object.in_memory).count(),
        ttl_object_count: objects
            .iter()
            .filter(|object| object.ttl_ms.is_some())
            .count(),
        objects,
    }
}
