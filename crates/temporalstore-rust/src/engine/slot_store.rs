use serde::{Deserialize, Serialize};

use crate::page_store::{LocalPageStore, PageAddress};
use crate::types::ShardId;
use rustmtcache::MultiLayerCache;

use super::state::{
    object_component_lookup_key, object_page_lookup_key, ShardState, SlotLayoutState,
};
use super::{read_page_bytes, read_page_bytes_batch};

type SlotPageIdentityKey = (
    u64,
    u64,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SlotRuntimeState {
    pub routing_slot: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub page_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub deleted_page_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SlotStoreRuntimeReport {
    pub slot_store_runtime_module: bool,
    pub slot_index_authority: bool,
    pub slot_count: usize,
    pub page_ref_count: usize,
    pub dirty_slot_count: usize,
    pub deleted_slot_count: usize,
    pub empty_slots: usize,
    pub single_object_slots: usize,
    pub single_page_object_slots: usize,
    pub multi_page_object_slots: usize,
    pub multi_object_slots: usize,
    pub deleted_page_ref_count: usize,
    pub loading_slot_count: usize,
    pub in_memory_slot_count: usize,
    pub ttl_slot_count: usize,
    pub max_dirty_generation: u64,
    pub slots: Vec<SlotRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> SlotStoreRuntimeReport {
    let mut report = SlotStoreRuntimeReport {
        slot_store_runtime_module: true,
        slot_index_authority: !shard.slot_index.slot_map.is_empty(),
        slot_count: shard.slot_index.slot_map.len(),
        page_ref_count: 0,
        dirty_slot_count: 0,
        deleted_slot_count: 0,
        empty_slots: 0,
        single_object_slots: 0,
        single_page_object_slots: 0,
        multi_page_object_slots: 0,
        multi_object_slots: 0,
        deleted_page_ref_count: 0,
        loading_slot_count: 0,
        in_memory_slot_count: 0,
        ttl_slot_count: 0,
        max_dirty_generation: 0,
        slots: Vec::new(),
    };

    for slot in shard.slot_index.slot_map.values() {
        report.page_ref_count = report.page_ref_count.saturating_add(slot.page_index.len());
        if slot.dirty {
            report.dirty_slot_count = report.dirty_slot_count.saturating_add(1);
        }
        if slot.deleted {
            report.deleted_slot_count = report.deleted_slot_count.saturating_add(1);
        }
        if slot.loading {
            report.loading_slot_count = report.loading_slot_count.saturating_add(1);
        }
        if slot.in_memory {
            report.in_memory_slot_count = report.in_memory_slot_count.saturating_add(1);
        }
        if slot.ttl_ms.is_some() {
            report.ttl_slot_count = report.ttl_slot_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(slot.dirty_generation);
        let deleted_page_ref_count = slot.page_index.values().filter(|page| page.deleted).count();
        report.deleted_page_ref_count = report
            .deleted_page_ref_count
            .saturating_add(deleted_page_ref_count);
        match slot.layout {
            SlotLayoutState::Empty => report.empty_slots = report.empty_slots.saturating_add(1),
            SlotLayoutState::SingleObject => {
                report.single_object_slots = report.single_object_slots.saturating_add(1)
            }
            SlotLayoutState::SinglePageObject => {
                report.single_page_object_slots = report.single_page_object_slots.saturating_add(1)
            }
            SlotLayoutState::MultiPageObject => {
                report.multi_page_object_slots = report.multi_page_object_slots.saturating_add(1)
            }
            SlotLayoutState::MultiObject => {
                report.multi_object_slots = report.multi_object_slots.saturating_add(1)
            }
        }
        report.slots.push(SlotRuntimeState {
            routing_slot: slot.routing_slot,
            layout: slot_layout_name(slot.layout).to_string(),
            object_ids: slot.object_index.iter().copied().collect(),
            page_ref_count: slot.page_index.len(),
            dirty: slot.dirty,
            deleted: slot.deleted,
            meta_loaded: slot.meta_loaded,
            loading: slot.loading,
            in_memory: slot.in_memory,
            ttl_ms: slot.ttl_ms,
            dirty_generation: slot.dirty_generation,
            last_dump_sequence: slot.last_dump_sequence,
            deleted_page_ref_count,
        });
    }

    report
}

#[allow(dead_code)]
fn slot_layout_name(layout: SlotLayoutState) -> &'static str {
    match layout {
        SlotLayoutState::Empty => "empty",
        SlotLayoutState::SingleObject => "single_object",
        SlotLayoutState::SinglePageObject => "single_page_object",
        SlotLayoutState::MultiPageObject => "multi_page_object",
        SlotLayoutState::MultiObject => "multi_object",
    }
}

fn slot_page_identity_key(address: &PageAddress) -> SlotPageIdentityKey {
    (
        address.page_segment_id,
        address.offset,
        address.length,
        address.page_id,
        address.object_id,
        address.routing_slot,
        address.generation,
    )
}

fn select_earliest_page_address(
    current: &mut Option<(SlotPageIdentityKey, PageAddress)>,
    address: &PageAddress,
) {
    let identity = slot_page_identity_key(address);
    if current
        .as_ref()
        .map(|(current_identity, _)| identity < *current_identity)
        .unwrap_or(true)
    {
        *current = Some((identity, address.clone()));
    }
}

pub(super) fn slot_index_page_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<PageAddress> {
    let lookup_key = object_page_lookup_key(model_id, object_key, component);
    if let Some(page_refs) = shard.slot_index.object_page_lookup.get(&lookup_key) {
        let mut selected = None;
        for page_ref in page_refs {
            let Some(slot) = shard.slot_index.slot_map.get(&page_ref.routing_slot) else {
                continue;
            };
            let Some(page) = slot.page_index.get(&page_ref.page_ref_key) else {
                continue;
            };
            if !page.deleted
                && page.model_id == model_id
                && page.object_key == object_key
                && page.component.as_deref() == component
            {
                select_earliest_page_address(&mut selected, &page.address);
            }
        }
        return selected.map(|(_, address)| address);
    }

    if !shard.slot_index.object_page_lookup.is_empty() {
        return None;
    }

    if let Some(page_refs) = shard.slot_index.live_page_refs_for_object_key(object_key) {
        let mut selected = None;
        for page_ref in page_refs {
            if page_ref.model_id != model_id || page_ref.component.as_deref() != component {
                continue;
            }
            let Some(slot) = shard.slot_index.slot_map.get(&page_ref.routing_slot) else {
                continue;
            };
            let Some(page) = slot.page_index.get(&page_ref.page_ref_key) else {
                continue;
            };
            if !page.deleted
                && page.model_id == model_id
                && page.object_key == object_key
                && page.component.as_deref() == component
            {
                select_earliest_page_address(&mut selected, &page.address);
            }
        }
        return selected.map(|(_, address)| address);
    }

    if let Some(routing_slots) = shard
        .slot_index
        .routing_slots_for_object_key(object_key)
        .filter(|slots| !slots.is_empty())
    {
        let mut selected = None;
        for page in routing_slots
            .iter()
            .filter_map(|routing_slot| shard.slot_index.slot_map.get(routing_slot))
            .flat_map(|slot| slot.page_index.values())
            .filter(|page| {
                !page.deleted
                    && page.model_id == model_id
                    && page.object_key == object_key
                    && page.component.as_deref() == component
            })
        {
            select_earliest_page_address(&mut selected, &page.address);
        }
        return selected.map(|(_, address)| address);
    }

    let mut selected = None;
    for page in shard
        .slot_index
        .slot_map
        .values()
        .flat_map(|slot| slot.page_index.values())
        .filter(|page| {
            !page.deleted
                && page.model_id == model_id
                && page.object_key == object_key
                && page.component.as_deref() == component
        })
    {
        select_earliest_page_address(&mut selected, &page.address);
    }
    selected.map(|(_, address)| address)
}

pub(super) fn slot_index_component_page_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<String>, PageAddress)> {
    let lookup_key = object_component_lookup_key(model_id, object_key);
    if let Some(page_refs) = shard.slot_index.object_component_lookup.get(&lookup_key) {
        let mut refs = Vec::with_capacity(page_refs.len());
        for page_ref in page_refs {
            let Some(slot) = shard.slot_index.slot_map.get(&page_ref.routing_slot) else {
                continue;
            };
            let Some(page) = slot.page_index.get(&page_ref.page_ref_key) else {
                continue;
            };
            if !page.deleted && page.model_id == model_id && page.object_key == object_key {
                refs.push((
                    page.component.clone(),
                    slot_page_identity_key(&page.address),
                    page.address.clone(),
                ));
            }
        }
        if !refs.is_empty() {
            return sorted_slot_component_refs(refs);
        }
        return Vec::new();
    }

    if !shard.slot_index.object_component_lookup.is_empty() {
        return Vec::new();
    }

    if let Some(page_refs) = shard.slot_index.live_page_refs_for_object_key(object_key) {
        let mut refs = Vec::with_capacity(page_refs.len());
        for page_ref in page_refs {
            if page_ref.model_id != model_id {
                continue;
            }
            let Some(slot) = shard.slot_index.slot_map.get(&page_ref.routing_slot) else {
                continue;
            };
            let Some(page) = slot.page_index.get(&page_ref.page_ref_key) else {
                continue;
            };
            if !page.deleted && page.model_id == model_id && page.object_key == object_key {
                refs.push((
                    page.component.clone(),
                    slot_page_identity_key(&page.address),
                    page.address.clone(),
                ));
            }
        }
        return sorted_slot_component_refs(refs);
    }

    let mut refs = Vec::new();
    if let Some(routing_slots) = shard
        .slot_index
        .routing_slots_for_object_key(object_key)
        .filter(|slots| !slots.is_empty())
    {
        let capacity = routing_slots
            .iter()
            .filter_map(|routing_slot| shard.slot_index.slot_map.get(routing_slot))
            .map(|slot| slot.page_index.len())
            .sum();
        refs.reserve(capacity);
        for routing_slot in routing_slots {
            let Some(slot) = shard.slot_index.slot_map.get(&routing_slot) else {
                continue;
            };
            for page in slot.page_index.values() {
                if !page.deleted && page.model_id == model_id && page.object_key == object_key {
                    refs.push((
                        page.component.clone(),
                        slot_page_identity_key(&page.address),
                        page.address.clone(),
                    ));
                }
            }
        }
    } else {
        let capacity = shard
            .slot_index
            .slot_map
            .values()
            .map(|slot| slot.page_index.len())
            .sum();
        refs.reserve(capacity);
        for slot in shard.slot_index.slot_map.values() {
            for page in slot.page_index.values() {
                if !page.deleted && page.model_id == model_id && page.object_key == object_key {
                    refs.push((
                        page.component.clone(),
                        slot_page_identity_key(&page.address),
                        page.address.clone(),
                    ));
                }
            }
        }
    };
    sorted_slot_component_refs(refs)
}

fn sorted_slot_component_refs(
    mut refs: Vec<(Option<String>, SlotPageIdentityKey, PageAddress)>,
) -> Vec<(Option<String>, PageAddress)> {
    refs.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    refs.into_iter()
        .map(|(component, _, address)| (component, address))
        .collect()
}

pub(super) fn read_slot_index_value(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<Vec<u8>> {
    slot_index_page_address(shard, model_id, object_key, component)
        .and_then(|address| read_page_bytes(cache, page_store, shard_id, &address))
}

pub(super) fn read_slot_index_component_values(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<String>, Vec<u8>)> {
    let refs = slot_index_component_page_addresses(shard, model_id, object_key);
    if refs.is_empty() {
        return Vec::new();
    }
    if refs.len() == 1 {
        let (component, address) = refs
            .into_iter()
            .next()
            .expect("single component ref is present");
        return read_page_bytes(cache, page_store, shard_id, &address)
            .map(|value| vec![(component, value)])
            .unwrap_or_default();
    }

    let mut addresses = Vec::with_capacity(refs.len());
    addresses.extend(refs.iter().map(|(_, address)| Some(address.clone())));
    let values = read_page_bytes_batch(cache, page_store, shard_id, &addresses);

    let mut entries = Vec::with_capacity(refs.len());
    for (value, (component, _)) in values.into_iter().zip(refs) {
        if let Some(value) = value {
            entries.push((component, value));
        }
    }
    entries
}
