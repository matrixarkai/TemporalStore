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
    #[serde(skip)]
    pub(super) dirty_objects: BTreeSet<String>,
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
