use crate::types::ControlStateFamily;

use super::product_model::control_state_family_key;
use super::state::{object_component_lookup_key, ShardState};

pub(super) fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("control_state:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        keys.push(control_state_family_key(family, key));
    }
    keys
}

pub(super) fn visit_associated_record_keys(key: &str, mut visit: impl FnMut(&str)) {
    visit(key);
    if key.starts_with("control_state:") {
        return;
    }
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        visit(&family_key);
    }
}

pub(super) fn any_associated_record_key(
    key: &str,
    mut predicate: impl FnMut(&str) -> bool,
) -> bool {
    if predicate(key) {
        return true;
    }
    if key.starts_with("control_state:") {
        return false;
    }
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        if predicate(&family_key) {
            return true;
        }
    }
    false
}

pub(super) fn record_exists(shard: &ShardState, key: &str) -> bool {
    any_associated_record_key(key, |record_key| record_exists_exact(shard, record_key))
}

pub(super) fn record_exists_exact(shard: &ShardState, key: &str) -> bool {
    if shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.features.contains_key(key)
        || shard.sequences.contains_key(key)
        || shard.ips.contains_key(key)
        || shard.control_state.contains_key(key)
        || shard.control_state_pages.contains_key(key)
        || shard.control_state_changes.contains_key(key)
        || shard.control_state_fol.contains_key(key)
        || shard.context_nodes.contains_key(key)
        || shard.context_events.contains_key(key)
        || shard.context_indexes.contains_key(key)
        || shard.context_audits.contains_key(key)
        || shard.context_dirty.contains_key(key)
        || shard.context_entities.contains_key(key)
        || shard.context_children.contains_key(key)
        || shard.context_embeddings.contains_key(key)
        || shard.context_summaries.contains_key(key)
        || shard.context_compressions.contains_key(key)
    {
        return true;
    }

    if !shard.slot_index.object_key_lookup.is_empty() {
        shard.slot_index.contains_object_key(key)
    } else if !shard.slot_index.object_component_lookup.is_empty() {
        storage_model_kinds().iter().any(|kind| {
            shard
                .slot_index
                .object_component_lookup
                .get(&object_component_lookup_key(kind, key))
                .map(|page_refs| {
                    page_refs.iter().any(|page_ref| {
                        shard
                            .slot_index
                            .slot_map
                            .get(&page_ref.routing_slot)
                            .and_then(|slot| slot.page_index.get(&page_ref.page_ref_key))
                            .map(|page| {
                                !page.deleted && page.model_id == *kind && page.object_key == key
                            })
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        })
    } else {
        shard.slot_index.contains_object_key(key)
    }
}

pub(super) fn storage_model_kinds() -> &'static [&'static str] {
    &[
        "string",
        "hash",
        "set",
        "feature",
        "sequence",
        "ips",
        "control_state",
        "context_node",
        "context_event",
        "context_index",
        "context_audit",
        "context_dirty",
        "context_entity",
        "context_child",
        "context_embedding",
        "context_summary",
        "context_compression",
    ]
}
