use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::page_store::PageAddress;
use crate::types::{CommandResponse, FeaturePoint, RiskFolType, ShardId};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(super) struct ShardState {
    pub(super) expires_at_ms: HashMap<String, u64>,
    pub(super) strings: HashMap<String, PageAddress>,
    pub(super) hashes: HashMap<String, HashMap<String, PageAddress>>,
    #[serde(default, with = "super::set_index_serde")]
    pub(super) sets: HashMap<String, BTreeMap<Vec<u8>, PageAddress>>,
    pub(super) features: HashMap<String, BTreeMap<u64, PageAddress>>,
    pub(super) sequences: HashMap<String, BTreeMap<u64, PageAddress>>,
    pub(super) ips: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) ips_meta: HashMap<String, BTreeMap<u64, IpsPointMeta>>,
    #[serde(default)]
    pub(super) ips_request_ids: HashMap<String, BTreeSet<String>>,
    pub(super) risk: HashMap<String, BTreeMap<u64, i64>>,
    #[serde(default)]
    pub(super) risk_pages: HashMap<String, PageAddress>,
    #[serde(default)]
    pub(super) risk_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>,
    #[serde(default)]
    pub(super) risk_fol: HashMap<String, RiskFolValue>,
    #[serde(default)]
    pub(super) context_nodes: HashMap<String, PageAddress>,
    #[serde(default)]
    pub(super) context_events: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_indexes: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_audits: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_dirty: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_entities: HashMap<String, PageAddress>,
    #[serde(default)]
    pub(super) context_children: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_embeddings: HashMap<String, PageAddress>,
    #[serde(default)]
    pub(super) context_summaries: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) context_compressions: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    pub(super) slot_index: CoreIndex,
    #[serde(skip)]
    pub(super) dirty_objects: BTreeSet<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct CoreIndex {
    #[serde(default, alias = "slots")]
    pub(super) slot_map: SlotMap,
    #[serde(default)]
    pub(super) object_page_lookup: ObjectPageLookup,
    #[serde(default)]
    pub(super) object_component_lookup: ObjectComponentLookup,
    #[serde(default)]
    pub(super) object_key_lookup: ObjectKeyLookup,
}

pub(super) type SlotMap = BTreeMap<u32, SlotNode>;
pub(super) type ObjectIndex = BTreeSet<u64>;
pub(super) type PageIndexMap = BTreeMap<String, PageIndex>;
pub(super) type ObjectPageLookup = BTreeMap<String, BTreeSet<PageLookupRef>>;
pub(super) type ObjectComponentLookup = BTreeMap<String, BTreeSet<ComponentPageLookupRef>>;
pub(super) type ObjectKeyLookup = BTreeMap<String, BTreeSet<ObjectKeyPageLookupRef>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct PageLookupRef {
    pub(super) routing_slot: u32,
    pub(super) page_ref_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct ComponentPageLookupRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<String>,
    pub(super) routing_slot: u32,
    pub(super) page_ref_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct ObjectKeyPageLookupRef {
    pub(super) model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<String>,
    pub(super) routing_slot: u32,
    pub(super) page_ref_key: String,
}

/// Rust-native core index mirroring the C++ shape:
/// Index -> SlotMap -> SlotNode -> PageIndex/ObjectIndex.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct SlotNode {
    pub(super) routing_slot: u32,
    #[serde(default)]
    pub(super) layout: SlotLayoutState,
    pub(super) dirty: bool,
    #[serde(default)]
    pub(super) deleted: bool,
    pub(super) meta_loaded: bool,
    pub(super) loading: bool,
    pub(super) in_memory: bool,
    pub(super) ttl_ms: Option<u64>,
    pub(super) dirty_generation: u64,
    pub(super) last_dump_sequence: u64,
    #[serde(default, alias = "object_ids")]
    pub(super) object_index: ObjectIndex,
    #[serde(default, alias = "deleted_object_ids")]
    pub(super) deleted_object_index: ObjectIndex,
    #[serde(default, alias = "page_refs")]
    pub(super) page_index: PageIndexMap,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum SlotLayoutState {
    #[default]
    Empty,
    SingleObject,
    SinglePageObject,
    MultiPageObject,
    MultiObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PageIndex {
    pub(super) object_key: String,
    pub(super) model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<String>,
    pub(super) object_id: u64,
    pub(super) address: PageAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
}

impl CoreIndex {
    pub(super) fn rebuild_object_page_lookup(&mut self) {
        self.object_page_lookup.clear();
        self.object_component_lookup.clear();
        self.object_key_lookup.clear();
        let refs = self
            .slot_map
            .iter()
            .flat_map(|(routing_slot, slot)| {
                slot.page_index.iter().map(move |(page_ref_key, page)| {
                    (*routing_slot, page_ref_key.clone(), page.clone())
                })
            })
            .collect::<Vec<_>>();
        for (routing_slot, page_ref_key, page) in refs {
            self.insert_object_page_lookup(routing_slot, page_ref_key, &page);
        }
    }

    pub(super) fn insert_object_page_lookup(
        &mut self,
        routing_slot: u32,
        page_ref_key: String,
        page: &PageIndex,
    ) {
        if page.deleted {
            return;
        }
        self.object_page_lookup
            .entry(object_page_lookup_key(
                &page.model_id,
                &page.object_key,
                page.component.as_deref(),
            ))
            .or_default()
            .insert(PageLookupRef {
                routing_slot,
                page_ref_key: page_ref_key.clone(),
            });
        self.object_component_lookup
            .entry(object_component_lookup_key(
                &page.model_id,
                &page.object_key,
            ))
            .or_default()
            .insert(ComponentPageLookupRef {
                component: page.component.clone(),
                routing_slot,
                page_ref_key: page_ref_key.clone(),
            });
        self.object_key_lookup
            .entry(page.object_key.clone())
            .or_default()
            .insert(ObjectKeyPageLookupRef {
                model_id: page.model_id.clone(),
                component: page.component.clone(),
                routing_slot,
                page_ref_key: page_ref_key.clone(),
            });
    }

    pub(super) fn remove_object_page_lookup_entry(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) {
        self.object_page_lookup
            .remove(&object_page_lookup_key(model_id, object_key, component));
        let component_lookup_key = object_component_lookup_key(model_id, object_key);
        if let Some(component_refs) = self.object_component_lookup.get_mut(&component_lookup_key) {
            component_refs.retain(|page_ref| page_ref.component.as_deref() != component);
            if component_refs.is_empty() {
                self.object_component_lookup.remove(&component_lookup_key);
            }
        }
        self.remove_object_key_lookup_entry(model_id, object_key, component);
    }

    pub(super) fn remove_all_object_page_lookup_entries(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) {
        let component_lookup_key = object_component_lookup_key(model_id, object_key);
        let components = self
            .object_component_lookup
            .remove(&component_lookup_key)
            .map(|component_refs| {
                component_refs
                    .into_iter()
                    .map(|page_ref| page_ref.component)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        if components.is_empty() {
            self.object_page_lookup
                .remove(&object_page_lookup_key(model_id, object_key, None));
            self.remove_object_key_lookup_model_entries(model_id, object_key);
            return;
        }
        for component in components {
            self.object_page_lookup.remove(&object_page_lookup_key(
                model_id,
                object_key,
                component.as_deref(),
            ));
        }
        self.remove_object_key_lookup_model_entries(model_id, object_key);
    }

    pub(super) fn remove_object_lookup_entries(&mut self, object_key: &str, model_ids: &[&str]) {
        for model_id in model_ids {
            let component_lookup_key = object_component_lookup_key(model_id, object_key);
            let components = self
                .object_component_lookup
                .remove(&component_lookup_key)
                .map(|component_refs| {
                    component_refs
                        .into_iter()
                        .map(|page_ref| page_ref.component)
                        .collect::<BTreeSet<_>>()
                })
                .unwrap_or_default();
            for component in components {
                self.object_page_lookup.remove(&object_page_lookup_key(
                    model_id,
                    object_key,
                    component.as_deref(),
                ));
            }
            self.remove_object_key_lookup_model_entries(model_id, object_key);
        }
    }

    fn remove_object_key_lookup_entry(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) {
        if let Some(page_refs) = self.object_key_lookup.get_mut(object_key) {
            page_refs.retain(|page_ref| {
                !(page_ref.model_id == model_id && page_ref.component.as_deref() == component)
            });
            if page_refs.is_empty() {
                self.object_key_lookup.remove(object_key);
            }
        }
    }

    fn remove_object_key_lookup_model_entries(&mut self, model_id: &str, object_key: &str) {
        if let Some(page_refs) = self.object_key_lookup.get_mut(object_key) {
            page_refs.retain(|page_ref| page_ref.model_id != model_id);
            if page_refs.is_empty() {
                self.object_key_lookup.remove(object_key);
            }
        }
    }

    pub(super) fn contains_object_key(&self, object_key: &str) -> bool {
        if let Some(page_refs) = self.object_key_lookup.get(object_key) {
            return page_refs
                .iter()
                .any(|page_ref| self.object_key_ref_is_live(object_key, page_ref));
        }

        if !self.object_key_lookup.is_empty() {
            return false;
        }

        self.slot_map.values().any(|slot| {
            slot.page_index
                .values()
                .any(|page| page.object_key == object_key && !page.deleted)
        })
    }

    pub(super) fn routing_slots_for_object_key(&self, object_key: &str) -> Option<BTreeSet<u32>> {
        self.object_key_lookup.get(object_key).map(|page_refs| {
            page_refs
                .iter()
                .filter(|page_ref| self.object_key_ref_is_live(object_key, page_ref))
                .map(|page_ref| page_ref.routing_slot)
                .collect()
        })
    }

    fn object_key_ref_is_live(&self, object_key: &str, page_ref: &ObjectKeyPageLookupRef) -> bool {
        self.slot_map
            .get(&page_ref.routing_slot)
            .and_then(|slot| slot.page_index.get(&page_ref.page_ref_key))
            .map(|page| !page.deleted && page.object_key == object_key)
            .unwrap_or(false)
    }

    pub(super) fn contains_object_page_address(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
        address: &PageAddress,
    ) -> bool {
        let lookup_key = object_page_lookup_key(model_id, object_key, component);
        if let Some(page_refs) = self.object_page_lookup.get(&lookup_key) {
            return page_refs.iter().any(|page_ref| {
                self.slot_map
                    .get(&page_ref.routing_slot)
                    .and_then(|slot| slot.page_index.get(&page_ref.page_ref_key))
                    .map(|page| {
                        !page.deleted
                            && page.model_id == model_id
                            && page.object_key == object_key
                            && page.component.as_deref() == component
                            && same_page_address(&page.address, address)
                    })
                    .unwrap_or(false)
            });
        }

        if !self.object_page_lookup.is_empty() {
            return false;
        }

        self.slot_map.values().any(|slot| {
            slot.page_index.values().any(|page| {
                !page.deleted
                    && page.model_id == model_id
                    && page.object_key == object_key
                    && page.component.as_deref() == component
                    && same_page_address(&page.address, address)
            })
        })
    }
}

pub(super) fn object_component_lookup_key(model_id: &str, object_key: &str) -> String {
    let mut key = String::new();
    push_lookup_part(&mut key, model_id);
    push_lookup_part(&mut key, object_key);
    key
}

pub(super) fn object_page_lookup_key(
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> String {
    let mut key = String::new();
    push_lookup_part(&mut key, model_id);
    push_lookup_part(&mut key, object_key);
    match component {
        Some(component) => {
            key.push_str("1|");
            push_lookup_part(&mut key, component);
        }
        None => key.push_str("0|"),
    }
    key
}

fn push_lookup_part(buffer: &mut String, value: &str) {
    buffer.push_str(&value.len().to_string());
    buffer.push(':');
    buffer.push_str(value);
    buffer.push('|');
}

fn same_page_address(left: &PageAddress, right: &PageAddress) -> bool {
    left.page_segment_id == right.page_segment_id
        && left.offset == right.offset
        && left.length == right.length
        && left.page_id == right.page_id
        && left.object_id == right.object_id
        && left.routing_slot == right.routing_slot
        && left.generation == right.generation
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RiskFolValue {
    pub(super) occur_time_ms: u64,
    pub(super) value: Vec<u8>,
    pub(super) fol_type: RiskFolType,
}

#[derive(Debug, Default, Clone)]
pub(super) struct AdmissionState {
    pub(super) window_epoch_sec: u64,
    pub(super) read_count: u64,
    pub(super) write_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum AdmissionScope {
    Shard(ShardId),
    Table(String),
    Tenant(String),
}

pub(super) struct AdmissionLimit {
    pub(super) scope: AdmissionScope,
    pub(super) limit: u64,
    pub(super) label: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct IpsPointMeta {
    pub(super) address: PageAddress,
    pub(super) action_type: Option<u32>,
    pub(super) table_id: Option<u64>,
    pub(super) request_id: Option<String>,
}

pub(super) struct ExecuteOutcome {
    pub(super) response: CommandResponse,
    pub(super) mutated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PackedFeaturePage {
    pub(super) version: u8,
    pub(super) points: Vec<FeaturePoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PackedFeaturePageDecode {
    Legacy,
    Packed(Vec<FeaturePoint>),
    Corrupt(String),
}
